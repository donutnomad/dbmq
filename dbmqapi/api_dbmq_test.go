package dbmqapi

import (
	"testing"

	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

func TestBuildStatsFromSummary(t *testing.T) {
	resp := buildStatsFromSummary(
		&query.TopicSummaryStats{TopicCount: 3, PartitionCount: 8},
		5,
		&query.TableStats{EstimatedRows: 123, TotalBytes: 2048},
		60,
	)

	require.Equal(t, 3, resp.Cluster.TopicCount)
	require.Equal(t, 8, resp.Cluster.PartitionCount)
	require.Equal(t, 5, resp.Cluster.ConsumerGroupCount)
	require.Equal(t, int64(123), resp.Cluster.TotalMessages)
	require.Equal(t, int64(2048), resp.Cluster.TotalSizeBytes)
	require.Equal(t, "60s", resp.Broker.Uptime)
}
