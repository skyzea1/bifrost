package warp

import (
	"context"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/stretchr/testify/require"
)

// retentionTestService indexes two requests, one a year old, and returns the
// service with the vector store they landed in.
func retentionTestService(t *testing.T) (*Service, *fakeWarpVectorStore) {
	t.Helper()
	vectors := newFakeWarpVectorStore()
	service := NewService(nil,
		WithConfigStore(&recordingStore{row: assignTestRow()}), WithLogReader(topicTestReader(t, map[string]logstore.Log{})),
		WithVectorStore(vectors), WithEmbeddingExecutor(assignTestEmbedding),
	)
	t.Cleanup(service.Shutdown)
	now := time.Now().UTC()
	for id, at := range map[string]time.Time{"fresh": now.Add(-time.Hour), "stale": now.Add(-365 * 24 * time.Hour)} {
		text := "how do I get a refund"
		outcome, err := service.indexer.Index(context.Background(), &logstore.Log{
			ID: id, Timestamp: at, Object: string(schemas.ChatCompletionRequest), Status: "success",
			InputHistoryParsed: []schemas.ChatMessage{chatTurn(schemas.ChatMessageRoleUser, text)},
		})
		require.NoError(t, err)
		require.Equal(t, IndexOutcomeIndexed, outcome)
	}
	require.Len(t, vectors.adds, 2)
	return service, vectors
}

// A log a person deleted leaves no vector behind: semantic search and topic
// runs would otherwise keep finding an id with nothing behind it. An id with
// no vector is not an error.
func TestWarpForgetLogsDropsTheirVectors(t *testing.T) {
	service, vectors := retentionTestService(t)
	service.ForgetLogs(context.Background(), []string{"stale", "never-indexed"})
	require.NotContains(t, vectors.adds, "stale")
	require.Contains(t, vectors.adds, "fresh")
}

// Retention is age: the hourly upkeep sweeps the vectors of logs older than
// the logs' retention, reading the retention each time, and leaves the rest.
// Without a retention there is no sweep.
func TestWarpSweepsVectorsOfExpiredLogs(t *testing.T) {
	service, vectors := retentionTestService(t)
	now := time.Now().UTC()

	swept, err := service.sweepExpiredVectors(context.Background(), now)
	require.NoError(t, err)
	require.Zero(t, swept, "no retention, no sweep")
	require.Len(t, vectors.adds, 2)

	days := 0
	service.SetLogRetention(func() int { return days })
	swept, err = service.sweepExpiredVectors(context.Background(), now)
	require.NoError(t, err)
	require.Zero(t, swept, "a retention of zero keeps everything")

	days = 30
	swept, err = service.sweepExpiredVectors(context.Background(), now)
	require.NoError(t, err)
	require.Equal(t, 1, swept)
	require.NotContains(t, vectors.adds, "stale")
	require.Contains(t, vectors.adds, "fresh")

	// Only Warp's own vectors: another owner's entry in the namespace stays.
	require.NoError(t, vectors.Add(context.Background(), assignTestRow().LogVectorStoreNamespace, "other", []float32{1, 0, 0}, map[string]interface{}{"timestamp": int64(0)}))
	swept, err = service.sweepExpiredVectors(context.Background(), now)
	require.NoError(t, err)
	require.Zero(t, swept)
	require.Contains(t, vectors.adds, "other")
}
