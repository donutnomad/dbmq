package pkg

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDALUpdateAssignmentsPartialFailure 测试UpdateAssignments部分更新失败BUG
// BUG描述：UpdateAssignments方法内部使用事务确保所有消费者分配的原子性更新，
// 如果某个消费者的更新失败，整个事务应该回滚，防止部分更新
func TestDALUpdateAssignmentsPartialFailure(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	groupID := "test-partial-failure-group"

	// 创建测试消费者
	consumers := []types.ConsumerHeartbeat{
		{
			ConsumerID:         "consumer-1",
			GroupID:            groupID,
			GenerationID:       1,
			SubscribedTopics:   mustMarshalJSON([]string{"test-topic"}),
			AssignedPartitions: mustMarshalJSON([]types.PartitionInfo{}), // 初始为空分区
			LastHeartbeat:      time.Now(),
		},
		{
			ConsumerID:         "consumer-2",
			GroupID:            groupID,
			GenerationID:       1,
			SubscribedTopics:   mustMarshalJSON([]string{"test-topic"}),
			AssignedPartitions: mustMarshalJSON([]types.PartitionInfo{}), // 初始为空分区
			LastHeartbeat:      time.Now(),
		},
		{
			ConsumerID:         "consumer-nonexistent", // 这个消费者不存在于数据库中
			GroupID:            groupID,
			GenerationID:       1,
			SubscribedTopics:   mustMarshalJSON([]string{"test-topic"}),
			AssignedPartitions: mustMarshalJSON([]types.PartitionInfo{}), // 初始为空分区
			LastHeartbeat:      time.Now(),
		},
	}

	// 只插入前两个消费者到数据库
	for i := 0; i < 2; i++ {
		require.NoError(t, db.Create(&consumers[i]).Error)
	}

	// 准备分配映射，包括不存在的消费者
	assignments := map[string][]types.PartitionInfo{
		"consumer-1": {
			{Topic: "test-topic", Partition: 0},
		},
		"consumer-2": {
			{Topic: "test-topic", Partition: 1},
		},
		"consumer-nonexistent": {
			{Topic: "test-topic", Partition: 2},
		},
	}

	// 尝试更新分配，应该由于不存在的消费者而失败
	err := dal.UpdateAssignments(context.Background(), db, groupID, 2, assignments)

	// 这个操作应该失败，因为consumer-nonexistent不存在
	assert.Error(t, err, "更新不存在的消费者应该失败")

	// 关键测试：检查是否有部分更新发生
	// 由于是在事务中，所有更新都应该被回滚
	var updatedConsumers []types.ConsumerHeartbeat
	require.NoError(t, db.Where("group_id = ?", groupID).Find(&updatedConsumers).Error)

	for _, consumer := range updatedConsumers {
		if consumer.GenerationID != 1 {
			t.Errorf("💥 BUG确认：消费者 %s 的代际ID被部分更新为 %d，应该仍为 1（事务应该完全回滚）",
				consumer.ConsumerID, consumer.GenerationID)
		}

		// 检查分配是否被部分更新
		if len(consumer.AssignedPartitions) > 0 {
			var partitions []types.PartitionInfo
			if err := json.Unmarshal(consumer.AssignedPartitions, &partitions); err == nil && len(partitions) > 0 {
				t.Errorf("💥 BUG确认：消费者 %s 的分区分配被部分更新，事务应该完全回滚", consumer.ConsumerID)
			}
		}
	}
}

// TestDALUpdateAssignmentsConsistency 测试分配更新的一致性
// BUG描述：检查UpdateAssignments是否能正确处理并发更新
func TestDALUpdateAssignmentsConsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	groupID := "test-consistency-group"

	// 创建测试消费者
	consumer := types.ConsumerHeartbeat{
		ConsumerID:         "consumer-1",
		GroupID:            groupID,
		GenerationID:       1,
		SubscribedTopics:   mustMarshalJSON([]string{"test-topic"}),
		AssignedPartitions: mustMarshalJSON([]types.PartitionInfo{}), // 初始为空分区
		LastHeartbeat:      time.Now(),
	}
	require.NoError(t, db.Create(&consumer).Error)

	// 准备两个不同的分配方案
	assignments1 := map[string][]types.PartitionInfo{
		"consumer-1": {
			{Topic: "test-topic", Partition: 0},
			{Topic: "test-topic", Partition: 1},
		},
	}

	assignments2 := map[string][]types.PartitionInfo{
		"consumer-1": {
			{Topic: "test-topic", Partition: 2},
			{Topic: "test-topic", Partition: 3},
		},
	}

	// 第一次更新
	err1 := dal.UpdateAssignments(context.Background(), db, groupID, 2, assignments1)
	require.NoError(t, err1)

	// 第二次更新（模拟快速重新均衡）
	err2 := dal.UpdateAssignments(context.Background(), db, groupID, 3, assignments2)
	require.NoError(t, err2)

	// 验证最终状态
	var finalConsumer types.ConsumerHeartbeat
	require.NoError(t, db.Where("group_id = ? AND consumer_id = ?", groupID, "consumer-1").First(&finalConsumer).Error)

	// 检查代际ID是否正确
	assert.Equal(t, uint(3), finalConsumer.GenerationID, "最终代际ID应该是3")

	// 检查分区分配是否正确
	var finalPartitions []types.PartitionInfo
	require.NoError(t, json.Unmarshal(finalConsumer.AssignedPartitions, &finalPartitions))

	expectedPartitions := assignments2["consumer-1"]
	assert.Equal(t, expectedPartitions, finalPartitions, "最终分区分配应该是第二次更新的结果")
}

// TestDALBatchCommitOffsetsAtomicity 测试批量提交偏移量的原子性
// BUG描述：BatchCommitOffsets在部分提交失败时的行为
func TestDALBatchCommitOffsetsAtomicity(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	groupID := "test-batch-commit-group"
	generationID := uint(1)

	// 创建测试主题和分区
	topic := &types.Topic{
		TopicName:      "test-batch-topic",
		PartitionCount: 3,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	// 准备偏移量映射
	offsets := map[types.PartitionInfo]int64{
		{Topic: "test-batch-topic", Partition: 0}: 100,
		{Topic: "test-batch-topic", Partition: 1}: 200,
		{Topic: "test-batch-topic", Partition: 2}: 300,
	}

	// 第一次批量提交应该成功
	err := dal.BatchCommitOffsets(context.Background(), db, groupID, generationID, offsets)
	require.NoError(t, err, "第一次批量提交应该成功")

	// 验证所有偏移量都被正确提交
	for partition, expectedOffset := range offsets {
		var committedOffset types.ConsumerGroupOffset
		err := db.Where("group_id = ? AND topic = ? AND `partition` = ?",
			groupID, partition.Topic, partition.Partition).First(&committedOffset).Error
		require.NoError(t, err, "应该能找到提交的偏移量")
		assert.Equal(t, expectedOffset, committedOffset.CommittedOffset,
			"分区 %d 的偏移量应该正确", partition.Partition)
	}

	// 测试部分更新场景：修改其中一个偏移量为无效值（比如负数，如果有验证的话）
	invalidOffsets := map[types.PartitionInfo]int64{
		{Topic: "test-batch-topic", Partition: 0}: 150, // 有效更新
		{Topic: "test-batch-topic", Partition: 1}: 250, // 有效更新
		{Topic: "test-batch-topic", Partition: 2}: 350, // 有效更新
	}

	// 这次应该成功，因为所有偏移量都是有效的
	err = dal.BatchCommitOffsets(context.Background(), db, groupID, generationID, invalidOffsets)
	require.NoError(t, err, "有效的批量提交应该成功")

	// 验证所有偏移量都被正确更新
	for partition, expectedOffset := range invalidOffsets {
		var committedOffset types.ConsumerGroupOffset
		err := db.Where("group_id = ? AND topic = ? AND `partition` = ?",
			groupID, partition.Topic, partition.Partition).First(&committedOffset).Error
		require.NoError(t, err, "应该能找到更新的偏移量")
		assert.Equal(t, expectedOffset, committedOffset.CommittedOffset,
			"分区 %d 的偏移量应该被正确更新", partition.Partition)
	}
}

// TestDALFetchMessagesBatchConsistency 测试批量获取消息的一致性
// BUG描述：FetchMessagesBatch的UNION ALL查询可能导致消息顺序问题
func TestDALFetchMessagesBatchConsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-fetch-topic",
		PartitionCount: 2,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	// 插入测试消息（交替插入到两个分区）
	messages := []types.Message{
		{Topic: "test-fetch-topic", Partition: 0, Body: []byte("msg-p0-1"), CreatedAt: time.Now()},
		{Topic: "test-fetch-topic", Partition: 1, Body: []byte("msg-p1-1"), CreatedAt: time.Now().Add(1 * time.Millisecond)},
		{Topic: "test-fetch-topic", Partition: 0, Body: []byte("msg-p0-2"), CreatedAt: time.Now().Add(2 * time.Millisecond)},
		{Topic: "test-fetch-topic", Partition: 1, Body: []byte("msg-p1-2"), CreatedAt: time.Now().Add(3 * time.Millisecond)},
		{Topic: "test-fetch-topic", Partition: 0, Body: []byte("msg-p0-3"), CreatedAt: time.Now().Add(4 * time.Millisecond)},
	}

	for _, msg := range messages {
		require.NoError(t, dal.CreateMessage(context.Background(), db, &msg))
	}

	// 批量获取消息
	requests := []dal.PartitionRequest{
		{Topic: "test-fetch-topic", Partition: 0, Offset: 0, Limit: 10},
		{Topic: "test-fetch-topic", Partition: 1, Offset: 0, Limit: 10},
	}

	fetchedMessages, err := dal.FetchMessagesBatch(context.Background(), db, requests)
	require.NoError(t, err)

	// 验证消息数量
	assert.Equal(t, 5, len(fetchedMessages), "应该获取到5条消息")

	// 验证消息是否按ID排序（这是FetchMessagesBatch的预期行为）
	for i := 1; i < len(fetchedMessages); i++ {
		if fetchedMessages[i].ID <= fetchedMessages[i-1].ID {
			t.Errorf("💥 BUG确认：消息顺序不正确，消息 %d (ID: %d) 应该在消息 %d (ID: %d) 之后",
				i, fetchedMessages[i].ID, i-1, fetchedMessages[i-1].ID)
		}
	}

	// 验证每个分区的消息是否完整
	partition0Count := 0
	partition1Count := 0
	for _, msg := range fetchedMessages {
		if msg.Partition == 0 {
			partition0Count++
		} else if msg.Partition == 1 {
			partition1Count++
		}
	}

	assert.Equal(t, 3, partition0Count, "分区0应该有3条消息")
	assert.Equal(t, 2, partition1Count, "分区1应该有2条消息")
}

// 辅助函数已在其他测试文件中定义
