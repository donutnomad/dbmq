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

func TestStaleProgressSQLContainsCoreClauses(t *testing.T) {
	// 必须 JOIN mq_messages 取已消费消息时间
	require.Contains(t, staleProgressSQL, "JOIN mq_messages consumed")
	require.Contains(t, staleProgressSQL, "consumed.id = cgp.last_consumed_message_id")
	// 必须按天计算落后时间
	require.Contains(t, staleProgressSQL, "TIMESTAMPDIFF(DAY")
	require.Contains(t, staleProgressSQL, ">= ?")
	// 必须排除哨兵值 -1
	require.Contains(t, staleProgressSQL, "cgp.last_consumed_message_id >= 0")
	// 排序保证最严重的在前
	require.Contains(t, staleProgressSQL, "ORDER BY stale_days DESC, lag_count DESC")
}

func TestDetachedProgressSQLContainsCoreClauses(t *testing.T) {
	// 必须 LEFT JOIN generations 表
	require.Contains(t, detachedProgressSQL, "LEFT JOIN mq_consumer_group_generations")
	// 通过 group_id IS NULL 识别 detached
	require.Contains(t, detachedProgressSQL, "g.group_id IS NULL")
}
