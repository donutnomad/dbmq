package dbmqapi

import (
	"testing"

	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

func TestBuildTopicResponsesWithPartitionStatsDefaults(t *testing.T) {
	topics := []query.TopicMetrics{
		{
			TopicName:      "orders",
			PartitionCount: 3,
			MessageCount:   11,
			SizeBytes:      512,
		},
	}
	statsByTopic := map[string][]query.PartitionStats{
		"orders": {
			{
				Partition:      1,
				FirstMessageID: 10,
				LastMessageID:  20,
				MessageCount:   11,
				SizeBytes:      512,
			},
		},
	}

	results := buildTopicResponses(topics, statsByTopic, true)
	require.Len(t, results, 1)
	require.Len(t, results[0].PartitionStats, 3)
	require.Equal(t, uint(0), results[0].PartitionStats[0].Partition)
	require.Equal(t, int64(-1), results[0].PartitionStats[0].FirstMessageID)
	require.Equal(t, int64(-1), results[0].PartitionStats[0].LastMessageID)
	require.Equal(t, uint(1), results[0].PartitionStats[1].Partition)
	require.Equal(t, int64(10), results[0].PartitionStats[1].FirstMessageID)
	require.Equal(t, int64(20), results[0].PartitionStats[1].LastMessageID)
	require.Equal(t, uint(2), results[0].PartitionStats[2].Partition)
	require.Equal(t, int64(-1), results[0].PartitionStats[2].FirstMessageID)
	require.Equal(t, int64(-1), results[0].PartitionStats[2].LastMessageID)
}
