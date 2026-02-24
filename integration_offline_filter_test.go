//go:build integration

package dbmq

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
)

// TestTC_OfflineFilterExtended 验证 GetConsumerGroupExtended 过滤离线超过1小时的成员
func TestTC_OfflineFilterExtended(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()
	db := globalEnv.DB

	groupID := "offline-filter-test-group"
	now := time.Now()

	// 插入 generation 记录
	db.Exec("INSERT INTO mq_consumer_group_generations (group_id, generation_id, protocol_type) VALUES (?, 1, 'consumer')", groupID)

	// 准备4种消费者：
	// 1. 在线消费者
	// 2. 离线不到1小时的消费者（应该返回）
	// 3. 刚好离线1小时的消费者（应该返回，边界）
	// 4. 离线超过1小时的消费者（不应该返回）
	offlineAt30m := now.Add(-30 * time.Minute)
	offlineAt60m := now.Add(-60 * time.Minute)
	offlineAt2h := now.Add(-2 * time.Hour)

	heartbeats := []heartbeatrepo.HeartbeatPO{
		{
			GroupID:            groupID,
			ConsumerID:         "consumer-online",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            false,
			LastHeartbeat:      now,
			OfflineAt:          nil,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "consumer-offline-30m",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt30m,
			OfflineAt:          &offlineAt30m,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "consumer-offline-60m",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt60m,
			OfflineAt:          &offlineAt60m,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "consumer-offline-2h",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt2h,
			OfflineAt:          &offlineAt2h,
		},
	}

	for _, hb := range heartbeats {
		require.NoError(t, db.Create(&hb).Error)
	}

	// 执行查询
	consumerQuery := query.NewConsumerQuery(db)
	extended, err := consumerQuery.GetConsumerGroupExtended(ctx, groupID)
	require.NoError(t, err)

	// 验证：应该返回3个成员（在线 + 离线30m + 离线60m），不返回离线2h的
	memberIDs := make([]string, len(extended.Members))
	for i, m := range extended.Members {
		memberIDs[i] = m.ConsumerID
	}

	assert.Len(t, extended.Members, 3, "应该返回3个成员，过滤掉离线超过1小时的")
	assert.Contains(t, memberIDs, "consumer-online", "在线消费者应该被返回")
	assert.Contains(t, memberIDs, "consumer-offline-30m", "离线30分钟的消费者应该被返回")
	assert.Contains(t, memberIDs, "consumer-offline-60m", "离线刚好60分钟的消费者应该被返回（边界）")
	assert.NotContains(t, memberIDs, "consumer-offline-2h", "离线超过1小时的消费者不应该被返回")
}

// TestTC_OfflineFilterMetrics 验证 GetConsumerGroupMetrics 过滤离线超过1小时的成员
func TestTC_OfflineFilterMetrics(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()
	db := globalEnv.DB

	groupID := "offline-filter-metrics-group"
	topicName := "metrics-filter-topic"
	now := time.Now()

	// 创建 Topic
	admin := NewAdminClient(db)
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	// 插入 generation 记录
	db.Exec("INSERT INTO mq_consumer_group_generations (group_id, generation_id, protocol_type) VALUES (?, 1, 'consumer')", groupID)

	// 准备3种消费者
	offlineAt30m := now.Add(-30 * time.Minute)
	offlineAt3h := now.Add(-3 * time.Hour)

	heartbeats := []heartbeatrepo.HeartbeatPO{
		{
			GroupID:            groupID,
			ConsumerID:         "metrics-consumer-online",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{topicName},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{{Topic: topicName, Partition: 0}},
			Offline:            false,
			LastHeartbeat:      now,
			OfflineAt:          nil,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "metrics-consumer-offline-30m",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{topicName},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt30m,
			OfflineAt:          &offlineAt30m,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "metrics-consumer-offline-3h",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{topicName},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt3h,
			OfflineAt:          &offlineAt3h,
		},
	}

	for _, hb := range heartbeats {
		require.NoError(t, db.Create(&hb).Error)
	}

	// 执行查询
	consumerQuery := query.NewConsumerQuery(db)
	metrics, err := consumerQuery.GetConsumerGroupMetrics(ctx, groupID)
	require.NoError(t, err)

	// 验证：应该返回2个成员（在线 + 离线30m），不返回离线3h的
	memberIDs := make([]string, len(metrics.Members))
	for i, m := range metrics.Members {
		memberIDs[i] = m.ConsumerID
	}

	assert.Len(t, metrics.Members, 2, "应该返回2个成员，过滤掉离线超过1小时的")
	assert.Contains(t, memberIDs, "metrics-consumer-online", "在线消费者应该被返回")
	assert.Contains(t, memberIDs, "metrics-consumer-offline-30m", "离线30分钟的消费者应该被返回")
	assert.NotContains(t, memberIDs, "metrics-consumer-offline-3h", "离线超过1小时的消费者不应该被返回")
}

// TestTC_OfflineFilterNotAffectOnlineWithNilOfflineAt 验证 offline=false 且 offline_at=nil 的消费者不受影响
func TestTC_OfflineFilterNotAffectOnlineWithNilOfflineAt(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()
	db := globalEnv.DB

	groupID := "offline-nil-test-group"
	now := time.Now()

	// 插入 generation 记录
	db.Exec("INSERT INTO mq_consumer_group_generations (group_id, generation_id, protocol_type) VALUES (?, 1, 'consumer')", groupID)

	// 在线消费者，offline_at = nil
	heartbeats := []heartbeatrepo.HeartbeatPO{
		{
			GroupID:            groupID,
			ConsumerID:         "online-nil-offlineat",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            false,
			LastHeartbeat:      now,
			OfflineAt:          nil, // offline_at 为 NULL
		},
	}

	for _, hb := range heartbeats {
		require.NoError(t, db.Create(&hb).Error)
	}

	consumerQuery := query.NewConsumerQuery(db)
	extended, err := consumerQuery.GetConsumerGroupExtended(ctx, groupID)
	require.NoError(t, err)

	assert.Len(t, extended.Members, 1, "offline=false 的消费者不应该被过滤")
	assert.Equal(t, "online-nil-offlineat", extended.Members[0].ConsumerID)
}

// TestTC_OfflineFilterAllOfflineOver1Hour 验证所有成员都离线超过1小时时返回空列表
func TestTC_OfflineFilterAllOfflineOver1Hour(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()
	db := globalEnv.DB

	groupID := "all-offline-test-group"

	// 插入 generation 记录
	db.Exec("INSERT INTO mq_consumer_group_generations (group_id, generation_id, protocol_type) VALUES (?, 1, 'consumer')", groupID)

	offlineAt2h := time.Now().Add(-2 * time.Hour)
	offlineAt5h := time.Now().Add(-5 * time.Hour)

	heartbeats := []heartbeatrepo.HeartbeatPO{
		{
			GroupID:            groupID,
			ConsumerID:         "all-offline-1",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt2h,
			OfflineAt:          &offlineAt2h,
		},
		{
			GroupID:            groupID,
			ConsumerID:         "all-offline-2",
			GenerationID:       1,
			SubscribedTopics:   datatypes.JSONSlice[string]{"test-topic"},
			AssignedPartitions: datatypes.JSONSlice[heartbeatrepo.PartitionInfo]{},
			Offline:            true,
			LastHeartbeat:      offlineAt5h,
			OfflineAt:          &offlineAt5h,
		},
	}

	for _, hb := range heartbeats {
		require.NoError(t, db.Create(&hb).Error)
	}

	consumerQuery := query.NewConsumerQuery(db)

	// 验证 Extended
	extended, err := consumerQuery.GetConsumerGroupExtended(ctx, groupID)
	require.NoError(t, err)
	assert.Empty(t, extended.Members, "所有成员离线超过1小时时应该返回空列表")

	// 验证 Metrics
	metrics, err := consumerQuery.GetConsumerGroupMetrics(ctx, groupID)
	require.NoError(t, err)
	assert.Empty(t, metrics.Members, "所有成员离线超过1小时时 Members 应该为空")
	assert.Equal(t, "Dead", metrics.State, "所有成员都被过滤后状态应为 Dead")
}
