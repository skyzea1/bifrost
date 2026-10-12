package warp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/framework/sidekiq"
)

// Discovery and upkeep.
//
// A run that clusters a window finds the topics of that window. Traffic moves
// on: a subject nobody asked about then is asked about now, and its requests
// match nothing, so they are pooled with their vectors. Discovery clusters
// that pool, and what clusters large enough becomes a topic - a new one, or
// an existing one it turns out to belong to. The rest stay pooled for next
// time.
//
// Upkeep is the other direction. A topic whose traffic stopped is retired so
// it takes nothing new and the live comparison stays small, and two topics
// that have come to mean the same thing are merged, the smaller into the
// larger, with the requests filed under the smaller still counting through
// merged_into.
//
// Both run on a timer, discovery through Sidekiq because it spends model
// calls on names and should be visible and cancellable like any other run,
// upkeep in-process because it is a few row updates.

const (
	// TopicDiscoveryJobKind is the Sidekiq job that clusters the pool of
	// requests that matched no topic.
	TopicDiscoveryJobKind = "warp_topic_discovery"
	// TopicsJobPartition serialises every job that writes topics: a run and a
	// discovery must not interleave their writes.
	TopicsJobPartition = "warp_log_topics"

	// topicsDiscoveryInterval is how often upkeep runs and discovery is
	// considered.
	topicsDiscoveryInterval = time.Hour
	// topicsDiscoveryMinPool is how many pooled requests it takes before
	// discovery is worth a run and its model calls.
	topicsDiscoveryMinPool = 100
	// topicsDiscoveryMinSize is how many requests a discovered cluster needs
	// to be a topic. Below it the requests stay pooled: a cluster of three is
	// three requests, not a pattern, and naming it would spend a model call
	// on a topic nobody will see.
	topicsDiscoveryMinSize = 10
	// topicsRetireAfter is how long a topic may go without a request before it
	// is retired. A topic younger than this is never retired for silence.
	topicsRetireAfter   = 30 * 24 * time.Hour
	topicsUpkeepTimeout = 5 * time.Minute
)

// TopicJobQueue is what discovery is started through. *sidekiq.Runner is one.
type TopicJobQueue interface {
	EnqueuePartitioned(ctx context.Context, id, kind, partitioningKey, metadata, createdBy string) error
}

// BuildDiscoveryJobMeta freezes the embedding space for one discovery. There
// is no window: discovery reads the pool, whatever it holds.
func (s *Service) BuildDiscoveryJobMeta(ctx context.Context) (string, error) {
	if s.logs == nil || s.store == nil {
		return "", ErrUnavailable
	}
	if s.topicStore() == nil {
		return "", fmt.Errorf("%w: the log store cannot keep topics", ErrUnavailable)
	}
	config, err := s.Config(ctx)
	if err != nil {
		return "", err
	}
	if !config.IsConfigured() || config.EmbeddingProvider == "" || config.EmbeddingModel == "" {
		return "", fmt.Errorf("%w: configure an embedding provider and model before discovering topics", ErrInvalidConfig)
	}
	return marshalBackfillMeta(BackfillJobMeta{
		ConfigSignature: embeddingConfigSignature(config),
		Namespace:       config.EffectiveLogVectorStoreNamespace(),
	})
}

// RunDiscoveryJob is the Sidekiq handler: read the pool, cluster it, give each
// cluster large enough to an existing topic it belongs to or name it as a new
// one, and file its requests.
func (s *Service) RunDiscoveryJob(ctx context.Context, job tables.TableSidekiqJob, progress sidekiq.ProgressFunc) (string, error) {
	var meta BackfillJobMeta
	if err := sonic.Unmarshal([]byte(job.Metadata), &meta); err != nil {
		return job.Metadata, fmt.Errorf("parse Warp discovery metadata: %w", err)
	}
	snapshot := func() string {
		encoded, err := marshalBackfillMeta(meta)
		if err != nil {
			return job.Metadata
		}
		return encoded
	}
	checkpoint := func(message string) error {
		meta.Message = message
		return progress(snapshot())
	}

	config, err := s.Config(ctx)
	if err != nil {
		return snapshot(), err
	}
	if embeddingConfigSignature(config) != meta.ConfigSignature {
		return snapshot(), fmt.Errorf("Warp embedding configuration changed while topics were being discovered")
	}
	topicStore := s.topicStore()
	if topicStore == nil {
		return snapshot(), fmt.Errorf("%w: the log store cannot keep topics", ErrUnavailable)
	}

	// 1. Read the pool.
	pool, err := topicStore.ListWarpTopicUnmatched(ctx, meta.ConfigSignature, topicsMaxVectors)
	if err != nil {
		meta.LastError = err.Error()
		return snapshot(), fmt.Errorf("read pooled requests: %w", err)
	}
	meta.Scanned, meta.Total = len(pool), int64(len(pool))
	if len(pool) < topicsDiscoveryMinPool {
		meta.Message = fmt.Sprintf("%d request(s) are waiting for a topic; discovery runs once there are %d.", len(pool), topicsDiscoveryMinPool)
		return snapshot(), progress(snapshot())
	}
	if err := checkpoint(fmt.Sprintf("Clustering %d request(s) that matched no topic.", len(pool))); err != nil {
		return snapshot(), err
	}

	// 2. Cluster it, as a run clusters a window.
	limits := s.topicLimits
	if limits.batch == 0 {
		limits = defaultTopicLimits()
	}
	run := newTopicRun(limits, topicsSeed(meta))
	for _, entry := range pool {
		vector, err := logstore.DecodeWarpVector(entry.Vector)
		if err != nil || len(vector) == 0 {
			continue
		}
		if err := run.add(ctx, topicVector{logID: entry.LogID, vector: unitVector(vector)}); err != nil {
			return snapshot(), err
		}
	}
	if err := run.finish(ctx); err != nil {
		if ctx.Err() != nil {
			meta.Message = "Stopped while clustering."
			_ = progress(snapshot())
			return snapshot(), ctx.Err()
		}
		meta.LastError = err.Error()
		return snapshot(), err
	}
	meta.Clustered = run.clustered

	// 3. Sort the clusters: large enough to be topics, or back to the pool.
	existing, err := topicStore.ListWarpTopics(ctx, logstore.WarpTopicFilter{
		Kinds: []string{logstore.WarpTopicKindTopic}, Statuses: []string{logstore.WarpTopicStatusActive}, EmbeddingSignature: meta.ConfigSignature,
	})
	if err != nil {
		meta.LastError = err.Error()
		return snapshot(), fmt.Errorf("read topics: %w", err)
	}
	var candidates []topicCluster
	for _, cluster := range run.clusters {
		if cluster.size < topicsDiscoveryMinSize {
			meta.Unassigned += cluster.size
			continue
		}
		candidates = append(candidates, cluster)
	}
	meta.Unassigned += run.dropped + run.unmatched
	if len(candidates) == 0 {
		meta.Message = fmt.Sprintf("Clustered %d pooled request(s); no cluster was large enough to be a topic.", len(pool))
		return snapshot(), progress(snapshot())
	}
	if err := checkpoint(fmt.Sprintf("Found %d cluster(s) large enough to be topics; labelling.", len(candidates))); err != nil {
		return snapshot(), err
	}

	// 4. Name them, after joining what is the same subject.
	labelCtx := withBackgroundWork(ctx)
	failed, lastError := labelTopics(labelCtx, s.chatFuncFor(labelCtx, config, ""), config, candidates, s.topicSampleTexts(ctx, candidates))
	if ctx.Err() != nil {
		meta.Message = "Stopped while labelling."
		_ = progress(snapshot())
		return snapshot(), ctx.Err()
	}
	if failed > 0 {
		meta.Failed += failed
		meta.LastError = lastError
	}
	candidates, meta.MergedByName = joinTopicsByName(candidates)

	// 5. Give each cluster to the existing topic it belongs to, by centre or
	// by name, and make a topic of the rest.
	placed := placeDiscoveredClusters(candidates, existing, run.threshold)
	now := time.Now().UTC()
	var created []logstore.WarpTopic
	for index := range placed {
		if placed[index].topic != "" {
			meta.Merged++
			continue
		}
		cluster := placed[index].cluster
		topic := logstore.WarpTopic{
			ID: uuid.NewString(), Kind: logstore.WarpTopicKindTopic, Label: cluster.label,
			Centroid: logstore.EncodeWarpVector(cluster.centroid()), AssignFloor: float64(run.floor), Cohesion: cluster.cohesion(),
			EmbeddingSignature: meta.ConfigSignature, Status: logstore.WarpTopicStatusActive, CreatedAt: now,
		}
		if err := topic.SetExampleLogIDs(cluster.sampleIDs); err != nil {
			return snapshot(), fmt.Errorf("encode topic examples: %w", err)
		}
		created = append(created, topic)
		placed[index].topic = topic.ID
		meta.Topics = append(meta.Topics, TopicSummary{Label: cluster.label, Size: cluster.size, Cohesion: roundTo(cluster.cohesion(), 3)})
	}
	if err := topicStore.UpsertWarpTopics(ctx, created); err != nil {
		meta.LastError = err.Error()
		return snapshot(), fmt.Errorf("write topics: %w", err)
	}
	meta.Indexed = len(created)
	filed := 0
	for _, placement := range placed {
		n, err := fileTopicMembers(ctx, topicStore, placement.topic, placement.cluster.members, now)
		filed += n
		if err != nil {
			meta.LastError = err.Error()
			return snapshot(), fmt.Errorf("file requests: %w", err)
		}
	}
	meta.Assigned = filed
	if s.indexer != nil {
		s.indexer.InvalidateTopics()
	}
	meta.Message = fmt.Sprintf("Clustered %d pooled request(s): %d new topic(s), %d cluster(s) folded into existing topics, %d request(s) filed.", len(pool), meta.Indexed, meta.Merged, filed)
	if meta.Unassigned > 0 {
		meta.Message += fmt.Sprintf(" %d stay pooled for next time.", meta.Unassigned)
	}
	return snapshot(), progress(snapshot())
}

// topicPlacement is one discovered cluster and the topic it goes to: an
// existing one, or "" for a topic of its own.
type topicPlacement struct {
	cluster topicCluster
	topic   string
}

// placeDiscoveredClusters decides which existing topic, if any, each cluster
// belongs to. By centre first: a cluster at least threshold like an existing
// topic is that topic, whatever it was named, the same test the run applies
// between its own clusters. By name second, for the reason joinTopicsByName
// gives. threshold is nil when the run derived none, and then only names
// join.
func placeDiscoveredClusters(clusters []topicCluster, existing []logstore.WarpTopic, threshold *float64) []topicPlacement {
	type centre struct {
		id     string
		vector []float32
	}
	centres := make([]centre, 0, len(existing))
	byName := make(map[string]string, len(existing))
	for _, topic := range existing {
		byName[topicNameKey(topic.Label)] = topic.ID
		if vector, err := logstore.DecodeWarpVector(topic.Centroid); err == nil && len(vector) > 0 {
			centres = append(centres, centre{id: topic.ID, vector: vector})
		}
	}
	placed := make([]topicPlacement, 0, len(clusters))
	for _, cluster := range clusters {
		placement := topicPlacement{cluster: cluster}
		if threshold != nil {
			own := cluster.centroid()
			best, bestScore := "", float32(-2)
			for _, candidate := range centres {
				if len(candidate.vector) != len(own) {
					continue
				}
				if score := dot(own, candidate.vector); score > bestScore {
					best, bestScore = candidate.id, score
				}
			}
			if best != "" && float64(bestScore) >= *threshold {
				placement.topic = best
			}
		}
		if placement.topic == "" {
			if id, ok := byName[topicNameKey(cluster.label)]; ok && topicNameKey(cluster.label) != "" {
				placement.topic = id
			}
		}
		placed = append(placed, placement)
	}
	return placed
}

// topicNameKey is a label as names are compared: case and spacing aside.
func topicNameKey(label string) string {
	return strings.ToLower(strings.Join(strings.Fields(label), " "))
}

// fileTopicMembers files requests under a topic and takes them out of the
// pool. Their rows are read back first so the columns a topic question is
// asked by are kept; a request with no row yet is projected from its log.
func fileTopicMembers(ctx context.Context, store logstore.WarpTopicStore, topic string, members []topicMember, now time.Time) (int, error) {
	filed := 0
	for start := 0; start < len(members); start += topicsWriteBatch {
		batch := members[start:min(start+topicsWriteBatch, len(members))]
		ids := make([]string, 0, len(batch))
		similarity := make(map[string]float32, len(batch))
		for _, member := range batch {
			ids = append(ids, member.logID)
			similarity[member.logID] = member.similarity
		}
		rows, err := store.ListWarpTopicAssignments(ctx, logstore.WarpTopicAssignmentFilter{LogIDs: ids}, len(ids))
		if err != nil {
			return filed, err
		}
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			seen[row.LogID] = struct{}{}
		}
		var missing []string
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			projected, err := store.ListLogsForWarpTopics(ctx, missing, time.Time{}, time.Time{})
			if err != nil {
				return filed, err
			}
			rows = append(rows, projected...)
		}
		for index := range rows {
			rows[index].TopicID = topic
			rows[index].Similarity = float64(similarity[rows[index].LogID])
			rows[index].AssignedAt = now
		}
		if err := store.UpsertWarpTopicAssignments(ctx, rows); err != nil {
			return filed, err
		}
		if err := store.DeleteWarpTopicUnmatched(ctx, ids); err != nil {
			return filed, err
		}
		filed += len(rows)
	}
	return filed, nil
}

// TopicUpkeep is what one pass of MaintainTopics did.
type TopicUpkeep struct {
	Retired int
	Merged  int
}

// MaintainTopics retires the topics whose traffic has stopped and merges the
// ones that have come to mean the same thing.
//
// Retirement is by silence: a topic old enough to have had its chance with
// no request in topicsRetireAfter takes nothing new. Topics built with an
// embedding model other than the configured one are retired outright, since
// their centres mean nothing against today's vectors.
//
// Merging reuses the run's own test. Each topic is rebuilt as a cluster from
// what its row keeps - the centre is the direction of the members' sum and
// the cohesion is that sum's length over their number, so the sum is exactly
// recoverable - and the clusters are merged at the threshold those clusters
// derive, then joined by name. The smaller topic is marked merged into the
// larger, whose centre and cohesion are worked out again from both. Nothing
// filed under the smaller is rewritten: the reader follows merged_into.
func (s *Service) MaintainTopics(ctx context.Context) (TopicUpkeep, error) {
	var done TopicUpkeep
	store := s.topicStore()
	if store == nil {
		return done, ErrUnavailable
	}
	config, err := s.Config(ctx)
	if err != nil {
		return done, err
	}
	signature := embeddingConfigSignature(config)
	active, err := store.ListWarpTopics(ctx, logstore.WarpTopicFilter{Kinds: []string{logstore.WarpTopicKindTopic}, Statuses: []string{logstore.WarpTopicStatusActive}})
	if err != nil {
		return done, fmt.Errorf("read topics: %w", err)
	}
	if len(active) == 0 {
		return done, nil
	}
	ids := make([]string, 0, len(active))
	for _, topic := range active {
		ids = append(ids, topic.ID)
	}
	now := time.Now().UTC()
	since := now.Add(-topicsRetireAfter)
	recent, err := store.SummarizeWarpTopicAssignments(ctx, logstore.WarpTopicAssignmentFilter{TopicIDs: ids, StartTime: &since})
	if err != nil {
		return done, fmt.Errorf("count recent requests: %w", err)
	}
	total, err := store.SummarizeWarpTopicAssignments(ctx, logstore.WarpTopicAssignmentFilter{TopicIDs: ids})
	if err != nil {
		return done, fmt.Errorf("count requests: %w", err)
	}
	recentCount := make(map[string]int64, len(recent))
	for _, entry := range recent {
		recentCount[entry.TopicID] = entry.Requests
	}
	totalCount := make(map[string]int64, len(total))
	for _, entry := range total {
		totalCount[entry.TopicID] = entry.Requests
	}

	var changed []logstore.WarpTopic
	var current []logstore.WarpTopic
	for _, topic := range active {
		silent := topic.CreatedAt.Before(since) && recentCount[topic.ID] == 0
		if topic.EmbeddingSignature != signature || silent {
			topic.Status = logstore.WarpTopicStatusRetired
			changed = append(changed, topic)
			done.Retired++
			continue
		}
		current = append(current, topic)
	}

	merged, survivors := mergeStoredTopics(current, totalCount)
	changed = append(changed, merged...)
	changed = append(changed, survivors...)
	done.Merged = len(merged)
	if len(merged) > 0 {
		repointed, err := repointMergedTopics(ctx, store, merged)
		if err != nil {
			return done, err
		}
		changed = append(changed, repointed...)
	}
	if len(changed) == 0 {
		return done, nil
	}
	if err := store.UpsertWarpTopics(ctx, changed); err != nil {
		return done, fmt.Errorf("write topics: %w", err)
	}
	if s.indexer != nil && (done.Retired > 0 || done.Merged > 0) {
		s.indexer.InvalidateTopics()
	}
	return done, nil
}

// repointMergedTopics returns the topics already merged into one of the
// topics now being merged, pointed at that topic's survivor instead.
//
// Every merged topic then points straight at a live topic, so "the requests
// of topic X" is X and the topics merged into X, one level deep: the logs
// filter (applyTopicFilter in logstore) follows exactly one, and the reader's
// chain walk (resolveMergedTopics) finds the same answer either way.
func repointMergedTopics(ctx context.Context, store logstore.WarpTopicStore, merged []logstore.WarpTopic) ([]logstore.WarpTopic, error) {
	survivorOf := make(map[string]string, len(merged))
	for _, topic := range merged {
		survivorOf[topic.ID] = topic.MergedInto
	}
	earlier, err := store.ListWarpTopics(ctx, logstore.WarpTopicFilter{Kinds: []string{logstore.WarpTopicKindTopic}, Statuses: []string{logstore.WarpTopicStatusMerged}, OmitCentroids: true})
	if err != nil {
		return nil, fmt.Errorf("read merged topics: %w", err)
	}
	var repointed []logstore.WarpTopic
	for _, topic := range earlier {
		survivor, ok := survivorOf[topic.MergedInto]
		if !ok {
			continue
		}
		// Omitted on read; written back whole, so it has to be read in full.
		whole, err := store.ListWarpTopics(ctx, logstore.WarpTopicFilter{IDs: []string{topic.ID}})
		if err != nil || len(whole) != 1 {
			return nil, fmt.Errorf("read merged topic %s: %w", topic.ID, err)
		}
		whole[0].MergedInto = survivor
		repointed = append(repointed, whole[0])
	}
	return repointed, nil
}

// mergeStoredTopics finds the topics that have come to mean the same thing
// and returns the ones to mark merged, with MergedInto set, and the survivors
// whose centre and cohesion changed.
func mergeStoredTopics(topics []logstore.WarpTopic, sizes map[string]int64) (merged, survivors []logstore.WarpTopic) {
	if len(topics) < 2 {
		return nil, nil
	}
	byID := make(map[string]logstore.WarpTopic, len(topics))
	clusters := make([]topicCluster, 0, len(topics))
	for _, topic := range topics {
		centre, err := logstore.DecodeWarpVector(topic.Centroid)
		if err != nil || len(centre) == 0 {
			continue
		}
		byID[topic.ID] = topic
		size := int(max(sizes[topic.ID], 1))
		// sum = centre * |sum|, and |sum| = cohesion * size.
		scale := float32(topic.Cohesion * float64(size))
		if topic.Cohesion <= 0 {
			scale = float32(size)
		}
		sum := make([]float32, len(centre))
		for d, value := range centre {
			sum[d] = value * scale
		}
		clusters = append(clusters, topicCluster{
			sum: sum, size: size, label: topic.Label,
			// The cluster stands for its topic; absorb carries the ids along.
			members: []topicMember{{logID: topic.ID}},
		})
	}
	sortTopicClusters(clusters)
	if threshold, ok := topicMergeThreshold(clusters); ok {
		clusters, _ = mergeTopicClusters(clusters, threshold)
	}
	clusters, _ = joinTopicsByName(clusters)
	for _, cluster := range clusters {
		if len(cluster.members) < 2 {
			continue
		}
		// The largest topic is the survivor: absorb keeps the first of a pair
		// and the clusters were sorted by size.
		survivor := byID[cluster.members[0].logID]
		survivor.Centroid = logstore.EncodeWarpVector(cluster.centroid())
		survivor.Cohesion = cluster.cohesion()
		survivors = append(survivors, survivor)
		for _, member := range cluster.members[1:] {
			topic := byID[member.logID]
			topic.Status = logstore.WarpTopicStatusMerged
			topic.MergedInto = survivor.ID
			merged = append(merged, topic)
		}
	}
	return merged, survivors
}

// resolveMergedTopics maps every merged topic to the active topic its chain
// ends in, for a reader counting requests filed under topics since merged. A
// chain that ends nowhere active is left out.
func resolveMergedTopics(active map[string]struct{}, merged []logstore.WarpTopic) map[string]string {
	into := make(map[string]string, len(merged))
	for _, topic := range merged {
		into[topic.ID] = topic.MergedInto
	}
	resolved := make(map[string]string, len(merged))
	for id := range into {
		target := id
		for hops := 0; hops < len(into)+1; hops++ {
			next, ok := into[target]
			if !ok {
				break
			}
			target = next
		}
		if _, ok := active[target]; ok && target != id {
			resolved[id] = target
		}
	}
	return resolved
}

// StartTopicUpkeep runs upkeep on a timer until Shutdown, and starts a
// discovery whenever enough requests are pooled and no topics job is in
// flight. Calling it twice is a no-op.
func (s *Service) StartTopicUpkeep() {
	if s.store == nil {
		return
	}
	s.upkeepOnce.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		stop := make(chan struct{})
		s.stopUpkeep = stop
		s.mu.Unlock()
		go func() {
			ticker := time.NewTicker(topicsDiscoveryInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(context.Background(), topicsUpkeepTimeout)
					s.topicUpkeepTick(ctx)
					cancel()
				case <-stop:
					return
				}
			}
		}()
	})
}

// stopTopicUpkeep ends the upkeep loop. Safe without a matching start, and
// safe twice.
func (s *Service) stopTopicUpkeep() {
	s.upkeepStopOnce.Do(func() {
		if s.stopUpkeep != nil {
			close(s.stopUpkeep)
		}
	})
}

// topicUpkeepTick is one pass: upkeep, then discovery if it is due.
func (s *Service) topicUpkeepTick(ctx context.Context) {
	if s.topicStore() == nil {
		return
	}
	done, err := s.MaintainTopics(ctx)
	switch {
	case errors.Is(err, ErrUnavailable):
		return
	case err != nil:
		s.warnf("failed to maintain Warp topics: %v", err)
	case done.Retired > 0 || done.Merged > 0:
		s.infof("warp topics: retired %d, merged %d", done.Retired, done.Merged)
	}
	started, err := s.StartDiscoveryIfDue(ctx)
	if err != nil {
		s.warnf("failed to start Warp topic discovery: %v", err)
	} else if started {
		s.infof("warp topics: started discovery over the pooled requests")
	}
}

// StartDiscoveryIfDue enqueues a discovery when the pool holds enough
// requests and no run or discovery is in flight, and reports whether it did.
func (s *Service) StartDiscoveryIfDue(ctx context.Context) (bool, error) {
	s.mu.RLock()
	queue, jobs := s.topicJobs, s.backfillJobs
	s.mu.RUnlock()
	store := s.topicStore()
	if queue == nil || jobs == nil || store == nil {
		return false, nil
	}
	config, err := s.Config(ctx)
	if err != nil || !config.IsConfigured() || config.EmbeddingProvider == "" || config.EmbeddingModel == "" {
		return false, nil
	}
	pooled, err := store.CountWarpTopicUnmatched(ctx, embeddingConfigSignature(config))
	if err != nil {
		return false, fmt.Errorf("count pooled requests: %w", err)
	}
	if pooled < topicsDiscoveryMinPool {
		return false, nil
	}
	for _, kind := range []string{TopicsJobKind, TopicDiscoveryJobKind} {
		inFlight, err := jobs.GetInFlightSidekiqJobByKind(ctx, kind)
		if err != nil {
			return false, fmt.Errorf("check for a running %s job: %w", kind, err)
		}
		if inFlight != nil {
			return false, nil
		}
	}
	metadata, err := s.BuildDiscoveryJobMeta(ctx)
	if err != nil {
		return false, err
	}
	if err := queue.EnqueuePartitioned(ctx, uuid.NewString(), TopicDiscoveryJobKind, TopicsJobPartition, metadata, ""); err != nil {
		return false, err
	}
	return true, nil
}
