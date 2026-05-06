package query

import (
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/stretchr/testify/require"
)

func TestBuildPartitionLagMetrics(t *testing.T) {
	now := time.Unix(1710000000, 0)
	progress := &consumerprogressrepo.ProgressPO{
		LastConsumedMessageID:      42,
		SubscriptionStartWatermark: 10,
		UpdatedAt:                  now,
	}

	metrics := buildPartitionLagMetrics(
		"orders",
		3,
		progress,
		77,
		123,
		partitionLagCounts{ConsumedCount: 12, LagCount: 5},
	)

	require.Equal(t, "orders", metrics.Topic)
	require.Equal(t, 3, metrics.Partition)
	require.Equal(t, int64(42), metrics.CurrentOffset)
	require.Equal(t, int64(77), metrics.LatestOffset)
	require.Equal(t, int64(5), metrics.Lag)
	require.Equal(t, int64(10), metrics.SubscriptionStartWatermark)
	require.Equal(t, int64(123), metrics.TotalMessageCount)
	require.Equal(t, int64(12), metrics.ConsumedMessages)
	require.Equal(t, int64(5), metrics.RemainingMessages)
	require.Equal(t, now.UnixMilli(), metrics.UpdatedAt)
	require.InDelta(t, 70.588235, metrics.ConsumedPercentage, 0.0001)
}

func TestBuildPartitionLagMetricsWithoutProgress(t *testing.T) {
	metrics := buildPartitionLagMetrics(
		"orders",
		1,
		nil,
		99,
		120,
		partitionLagCounts{},
	)

	require.Equal(t, "orders", metrics.Topic)
	require.Equal(t, 1, metrics.Partition)
	require.Equal(t, int64(-1), metrics.CurrentOffset)
	require.Equal(t, int64(99), metrics.LatestOffset)
	require.Equal(t, int64(0), metrics.Lag)
	require.Equal(t, int64(0), metrics.ConsumedMessages)
	require.Equal(t, int64(0), metrics.RemainingMessages)
	require.Equal(t, float64(0), metrics.ConsumedPercentage)
	require.Equal(t, int64(0), metrics.UpdatedAt)
}
