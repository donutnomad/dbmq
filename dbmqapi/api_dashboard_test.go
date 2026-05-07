package dbmqapi

import (
	"testing"

	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

func TestBuildDashboardDataIncludesStatsAndManualAssignments(t *testing.T) {
	resp := buildDashboardData(
		[]query.TopicMetrics{{TopicName: "orders", PartitionCount: 2, MessageCount: 7, SizeBytes: 128}},
		[]query.ConsumerGroupMetrics{{GroupID: "billing", State: "Active", Lag: 3}},
		[]*manualassignment.Assignment{{ID: 1, GroupID: "billing", ConsumerIDPattern: "host:*", Topic: "orders", Partition: 1}},
		&query.TableStats{EstimatedRows: 99, TotalBytes: 4096},
		100,
	)

	require.Len(t, resp.Topics, 1)
	require.Len(t, resp.ConsumerGroups, 1)
	require.Len(t, resp.ManualAssignments, 1)
	require.Equal(t, 1, resp.Stats.Cluster.TopicCount)
	require.Equal(t, 2, resp.Stats.Cluster.PartitionCount)
	require.Equal(t, 1, resp.Stats.Cluster.ConsumerGroupCount)
	require.Equal(t, int64(99), resp.Stats.Cluster.TotalMessages)
	require.Equal(t, int64(4096), resp.Stats.Cluster.TotalSizeBytes)
	require.Equal(t, "100s", resp.Stats.Broker.Uptime)
}
