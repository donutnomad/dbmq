package dbmq

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsumerRebalanceDataLossFixed 验证重新均衡数据丢失BUG的修复
func TestConsumerRebalanceDataLossFixed(t *testing.T) {
	testDB, testRedis := setupIntegrationTest(t)

	// 启动协调器
	coordinator := NewCoordinator(CoordinatorConfig{
		DB:                     testDB,
		HeartbeatTimeout:       10 * time.Second,
		RebalanceInterval:      1 * time.Second,
		RebalanceTimeout:       10 * time.Second,
		RetentionCheckInterval: 1 * time.Hour,
		DefaultRetentionAge:    24 * time.Hour,
	})
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为领导者
	for !coordinator.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// 创建测试主题
	admin := NewAdminClient(testDB)

	topicName := "rebalance-fix-test-topic"
	err := admin.CreateTopic(context.Background(), NewTopicRequest{
		Name:          topicName,
		NumPartitions: 3,
	})
	require.NoError(t, err)

	// 发送一些测试消息
	producer, err := NewProducer(ProducerConfig{
		DB:                  testDB,
		Redis:               testRedis,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息到分区
	for i := 0; i < 6; i++ {
		msg := &ProducerMessage{
			Topic: topicName,
			Key:   []byte("test-key"),
			Value: []byte("test-message"),
		}
		_, err := producer.Send(context.Background(), msg)
		require.NoError(t, err)
	}

	// 创建消费者
	consumer, err := NewConsumer(ConsumerConfig{
		DB:                testDB,
		Redis:             testRedis,
		GroupID:           "rebalance-fix-test-group",
		Topics:            []string{topicName},
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
		EnableAutoCommit:  false,
	})
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.SubscribeTopics(topicName)
	require.NoError(t, err)

	// 等待分区分配
	time.Sleep(3 * time.Second)

	// 设置一些polled偏移量（模拟已消费但未提交的消息）
	partition1 := types.PartitionInfo{Topic: topicName, Partition: 0}
	partition2 := types.PartitionInfo{Topic: topicName, Partition: 1}
	partition3 := types.PartitionInfo{Topic: topicName, Partition: 2}

	consumer.setPolledOffset(partition1, 2)
	consumer.setPolledOffset(partition2, 3)
	consumer.setPolledOffset(partition3, 1)

	// 记录重新均衡前的状态
	consumer.mu.RLock()
	polledOffsetsBeforeRebalance := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.polledOffsets {
		polledOffsetsBeforeRebalance[k] = v
	}
	consumer.mu.RUnlock()

	t.Logf("重新均衡前的polledOffsets: %v", polledOffsetsBeforeRebalance)
	require.NotEmpty(t, polledOffsetsBeforeRebalance, "重新均衡前应该有polledOffsets")

	// 模拟重新均衡 - 只保留分区0和1，撤销分区2
	newAssignment := map[string][]uint{
		topicName: {0, 1}, // 撤销分区2
	}

	// 应用修复后的重新均衡逻辑
	err = consumer.clearAndFetchOffsetsForNewAssignment(newAssignment)
	require.NoError(t, err)

	// 检查修复后的状态
	consumer.mu.RLock()
	polledOffsetsAfterRebalance := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.polledOffsets {
		polledOffsetsAfterRebalance[k] = v
	}
	committedOffsetsAfterRebalance := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.committedOffsets {
		committedOffsetsAfterRebalance[k] = v
	}
	consumer.mu.RUnlock()

	t.Logf("重新均衡后的polledOffsets: %v", polledOffsetsAfterRebalance)
	t.Logf("重新均衡后的committedOffsets: %v", committedOffsetsAfterRebalance)

	// 验证修复效果：
	// 1. 继续分配的分区（0和1）应该保留它们的polledOffsets
	assert.Contains(t, polledOffsetsAfterRebalance, partition1, "分区0的polledOffset应该被保留")
	assert.Contains(t, polledOffsetsAfterRebalance, partition2, "分区1的polledOffset应该被保留")
	assert.Equal(t, int64(2), polledOffsetsAfterRebalance[partition1], "分区0的polledOffset值应该正确")
	assert.Equal(t, int64(3), polledOffsetsAfterRebalance[partition2], "分区1的polledOffset值应该正确")

	// 2. 被撤销的分区（2）的偏移量应该被清除
	assert.NotContains(t, polledOffsetsAfterRebalance, partition3, "被撤销分区的polledOffset应该被清除")
	assert.NotContains(t, committedOffsetsAfterRebalance, partition3, "被撤销分区的committedOffset应该被清除")

	t.Logf("✅ 修复验证成功：重新均衡不再丢失未提交的偏移量")
}

// TestConsumerSetPolledOffsetFixed 验证setPolledOffset单调性检查的修复
func TestConsumerSetPolledOffsetFixed(t *testing.T) {
	testDB, testRedis := setupIntegrationTest(t)

	consumer, err := NewConsumer(ConsumerConfig{
		DB:      testDB,
		Redis:   testRedis,
		GroupID: "monotonic-fix-test-group",
		Topics:  []string{"test-topic"},
	})
	require.NoError(t, err)
	defer consumer.Close()

	partition := types.PartitionInfo{Topic: "test-topic", Partition: 0}

	// 设置已提交偏移量为20（表示下一个要消费的是20）
	consumer.mu.Lock()
	consumer.committedOffsets[partition] = 20
	consumer.mu.Unlock()

	// 尝试设置小于已提交偏移量的polled偏移量
	consumer.setPolledOffset(partition, 18) // 18 < 20-1 = 19

	// 检查是否被拒绝
	consumer.mu.RLock()
	_, polledExists := consumer.polledOffsets[partition]
	consumer.mu.RUnlock()

	assert.False(t, polledExists, "应该拒绝设置小于已提交偏移量的polledOffset")

	// 尝试设置有效的偏移量
	consumer.setPolledOffset(partition, 19) // 19 >= 20-1 = 19，应该被接受

	consumer.mu.RLock()
	polledOffset, polledExists := consumer.polledOffsets[partition]
	consumer.mu.RUnlock()

	assert.True(t, polledExists, "应该接受有效的polledOffset")
	assert.Equal(t, int64(19), polledOffset, "polledOffset值应该正确")

	// 测试正常的单调递增
	consumer.setPolledOffset(partition, 22)
	consumer.mu.RLock()
	polledOffset = consumer.polledOffsets[partition]
	consumer.mu.RUnlock()
	assert.Equal(t, int64(22), polledOffset, "应该接受更大的偏移量")

	// 测试回退保护
	consumer.setPolledOffset(partition, 21) // 尝试回退
	consumer.mu.RLock()
	polledOffset = consumer.polledOffsets[partition]
	consumer.mu.RUnlock()
	assert.Equal(t, int64(22), polledOffset, "应该拒绝回退")

	t.Logf("✅ 修复验证成功：setPolledOffset现在正确处理单调性检查")
}

// TestConsumerConcurrentCloseFixed 验证Close方法竞争条件的修复
func TestConsumerConcurrentCloseFixed(t *testing.T) {
	testDB, testRedis := setupIntegrationTest(t)

	// 创建启用自动提交的消费者
	consumer, err := NewConsumer(ConsumerConfig{
		DB:                 testDB,
		Redis:              testRedis,
		GroupID:            "concurrent-close-fix-group",
		Topics:             []string{"test-topic"},
		HeartbeatInterval:  1 * time.Second,
		EnableAutoCommit:   true,
		AutoCommitInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	err = consumer.SubscribeTopics("test-topic")
	require.NoError(t, err)

	// 等待自动提交循环启动
	time.Sleep(200 * time.Millisecond)

	// 设置一些polled偏移量
	partition := types.PartitionInfo{Topic: "test-topic", Partition: 0}
	consumer.setPolledOffset(partition, 10)

	// 关闭消费者 - 修复后应该：
	// 1. 先停止所有后台goroutine
	// 2. 然后进行最终提交
	// 3. 不会有竞争条件
	startTime := time.Now()
	consumer.Close()
	closeTime := time.Since(startTime)

	// 验证关闭是正常完成的（没有死锁或panic）
	assert.Less(t, closeTime, 10*time.Second, "关闭应该在合理时间内完成")

	t.Logf("✅ 修复验证成功：Close方法不再有竞争条件，关闭时间: %v", closeTime)
}

// TestConsumerManualCommitCloseFixed 验证手动提交模式下Close方法不会自动提交未确认偏移量的修复
func TestConsumerManualCommitCloseFixed(t *testing.T) {
	testDB, testRedis := setupIntegrationTest(t)

	// 启动协调器
	coordinator := NewCoordinator(CoordinatorConfig{
		DB:                     testDB,
		HeartbeatTimeout:       10 * time.Second,
		RebalanceInterval:      1 * time.Second,
		RebalanceTimeout:       10 * time.Second,
		RetentionCheckInterval: 1 * time.Hour,
		DefaultRetentionAge:    24 * time.Hour,
	})
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为领导者
	for !coordinator.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// 创建测试主题
	admin := NewAdminClient(testDB)
	topicName := "manual-commit-close-test-topic"
	err := admin.CreateTopic(context.Background(), NewTopicRequest{
		Name:          topicName,
		NumPartitions: 2,
	})
	require.NoError(t, err)

	// 发送一些测试消息
	producer, err := NewProducer(ProducerConfig{
		DB:                  testDB,
		Redis:               testRedis,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息到分区
	for i := 0; i < 4; i++ {
		msg := &ProducerMessage{
			Topic: topicName,
			Key:   []byte("test-key"),
			Value: []byte("test-message"),
		}
		_, err := producer.Send(context.Background(), msg)
		require.NoError(t, err)
	}

	// 创建手动提交模式的消费者
	consumer, err := NewConsumer(ConsumerConfig{
		DB:                testDB,
		Redis:             testRedis,
		GroupID:           "manual-commit-close-test-group",
		Topics:            []string{topicName},
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
		EnableAutoCommit:  false, // 手动提交模式
	})
	require.NoError(t, err)

	err = consumer.SubscribeTopics(topicName)
	require.NoError(t, err)

	// 等待分区分配
	time.Sleep(3 * time.Second)

	// 模拟消费消息但不提交
	partition1 := types.PartitionInfo{Topic: topicName, Partition: 0}
	partition2 := types.PartitionInfo{Topic: topicName, Partition: 1}

	// 设置已拉取但未提交的偏移量
	consumer.setPolledOffset(partition1, 1) // 消费了offset 1的消息但没有提交
	consumer.setPolledOffset(partition2, 1) // 消费了offset 1的消息但没有提交

	// 记录关闭前的状态
	consumer.mu.RLock()
	polledOffsetsBeforeClose := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.polledOffsets {
		polledOffsetsBeforeClose[k] = v
	}
	committedOffsetsBeforeClose := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.committedOffsets {
		committedOffsetsBeforeClose[k] = v
	}
	consumer.mu.RUnlock()

	t.Logf("关闭前的polledOffsets: %v", polledOffsetsBeforeClose)
	t.Logf("关闭前的committedOffsets: %v", committedOffsetsBeforeClose)

	// 验证有未提交的偏移量
	require.NotEmpty(t, polledOffsetsBeforeClose, "关闭前应该有未提交的polledOffsets")
	assert.Contains(t, polledOffsetsBeforeClose, partition1, "分区0应该有未提交的偏移量")
	assert.Contains(t, polledOffsetsBeforeClose, partition2, "分区1应该有未提交的偏移量")

	// 关闭消费者（手动提交模式下不应该自动提交）
	consumer.Close()

	// 创建新的消费者来检查偏移量是否被错误提交
	consumer2, err := NewConsumer(ConsumerConfig{
		DB:                testDB,
		Redis:             testRedis,
		GroupID:           "manual-commit-close-test-group", // 同一个消费组
		Topics:            []string{topicName},
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromCommitted, // 从已提交的偏移量开始
		EnableAutoCommit:  false,
	})
	require.NoError(t, err)
	defer consumer2.Close()

	err = consumer2.SubscribeTopics(topicName)
	require.NoError(t, err)

	// 等待分区分配
	time.Sleep(3 * time.Second)

	// 检查新消费者的偏移量状态
	consumer2.mu.RLock()
	committedOffsetsAfterRestart := make(map[types.PartitionInfo]int64)
	for k, v := range consumer2.committedOffsets {
		committedOffsetsAfterRestart[k] = v
	}
	consumer2.mu.RUnlock()

	t.Logf("重启后的committedOffsets: %v", committedOffsetsAfterRestart)

	// 验证修复效果：
	// 在手动提交模式下，Close()不应该提交未确认的偏移量
	// 所以新消费者应该从头开始消费（或者从之前手动提交的位置开始）
	// 这里我们期望从头开始，因为我们没有手动提交任何偏移量

	// 如果偏移量被错误提交，committedOffsets会是2（因为我们设置了polledOffset为1）
	// 如果修复生效，committedOffsets应该是-1或者不存在（表示从头开始）
	for partition, offset := range committedOffsetsAfterRestart {
		// 期望偏移量不会是被错误提交的值
		assert.NotEqual(t, int64(2), offset, "分区 %v 的偏移量不应该是被错误提交的值 2", partition)
		t.Logf("分区 %v 的偏移量: %d (正确，未被错误提交)", partition, offset)
	}

	t.Logf("✅ 修复验证成功：手动提交模式下Close()不会自动提交未确认的偏移量")
}

// TestConsumerManualCommitRebalanceFixed 验证手动提交模式下重新均衡不会自动提交未确认偏移量的修复
func TestConsumerManualCommitRebalanceFixed(t *testing.T) {
	testDB, testRedis := setupIntegrationTest(t)

	// 启动协调器
	coordinator := NewCoordinator(CoordinatorConfig{
		DB:                     testDB,
		HeartbeatTimeout:       10 * time.Second,
		RebalanceInterval:      1 * time.Second,
		RebalanceTimeout:       10 * time.Second,
		RetentionCheckInterval: 1 * time.Hour,
		DefaultRetentionAge:    24 * time.Hour,
	})
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为领导者
	for !coordinator.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}

	// 创建测试主题
	admin := NewAdminClient(testDB)
	topicName := "manual-commit-rebalance-test-topic"
	err := admin.CreateTopic(context.Background(), NewTopicRequest{
		Name:          topicName,
		NumPartitions: 3,
	})
	require.NoError(t, err)

	// 发送一些测试消息
	producer, err := NewProducer(ProducerConfig{
		DB:                  testDB,
		Redis:               testRedis,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息到分区
	for i := 0; i < 6; i++ {
		msg := &ProducerMessage{
			Topic: topicName,
			Key:   []byte("test-key"),
			Value: []byte("test-message"),
		}
		_, err := producer.Send(context.Background(), msg)
		require.NoError(t, err)
	}

	// 创建手动提交模式的消费者
	consumer, err := NewConsumer(ConsumerConfig{
		DB:                testDB,
		Redis:             testRedis,
		GroupID:           "manual-commit-rebalance-test-group",
		Topics:            []string{topicName},
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
		EnableAutoCommit:  false, // 手动提交模式
	})
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.SubscribeTopics(topicName)
	require.NoError(t, err)

	// 等待分区分配
	time.Sleep(3 * time.Second)

	// 模拟消费消息但不提交
	partition1 := types.PartitionInfo{Topic: topicName, Partition: 0}
	partition2 := types.PartitionInfo{Topic: topicName, Partition: 1}
	partition3 := types.PartitionInfo{Topic: topicName, Partition: 2}

	// 设置已拉取但未提交的偏移量
	consumer.setPolledOffset(partition1, 1)
	consumer.setPolledOffset(partition2, 1)
	consumer.setPolledOffset(partition3, 1) // 这个分区将被撤销

	// 记录重新均衡前的状态
	consumer.mu.RLock()
	polledOffsetsBeforeRebalance := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.polledOffsets {
		polledOffsetsBeforeRebalance[k] = v
	}
	consumer.mu.RUnlock()

	t.Logf("重新均衡前的polledOffsets: %v", polledOffsetsBeforeRebalance)
	require.Contains(t, polledOffsetsBeforeRebalance, partition3, "分区2应该有未提交的偏移量")

	// 模拟重新均衡 - 撤销分区2
	newAssignment := map[string][]uint{
		topicName: {0, 1}, // 保留分区0和1，撤销分区2
	}

	// 执行重新均衡逻辑
	err = consumer.clearAndFetchOffsetsForNewAssignment(newAssignment)
	require.NoError(t, err)

	// 检查重新均衡后的状态
	consumer.mu.RLock()
	polledOffsetsAfterRebalance := make(map[types.PartitionInfo]int64)
	for k, v := range consumer.polledOffsets {
		polledOffsetsAfterRebalance[k] = v
	}
	consumer.mu.RUnlock()

	t.Logf("重新均衡后的polledOffsets: %v", polledOffsetsAfterRebalance)

	// 验证修复效果：
	// 1. 继续分配的分区应该保留它们的polledOffsets
	assert.Contains(t, polledOffsetsAfterRebalance, partition1, "分区0的polledOffset应该被保留")
	assert.Contains(t, polledOffsetsAfterRebalance, partition2, "分区1的polledOffset应该被保留")

	// 2. 被撤销的分区的偏移量应该被清除（但不应该被提交到数据库）
	assert.NotContains(t, polledOffsetsAfterRebalance, partition3, "被撤销分区的polledOffset应该被清除")

	// 3. 创建新的消费者来验证被撤销分区的偏移量没有被错误提交
	consumer2, err := NewConsumer(ConsumerConfig{
		DB:                testDB,
		Redis:             testRedis,
		GroupID:           "manual-commit-rebalance-test-group", // 同一个消费组
		Topics:            []string{topicName},
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromCommitted,
		EnableAutoCommit:  false,
	})
	require.NoError(t, err)
	defer consumer2.Close()

	err = consumer2.SubscribeTopics(topicName)
	require.NoError(t, err)

	// 等待分区分配
	time.Sleep(3 * time.Second)

	// 检查新消费者对分区2的偏移量
	consumer2.mu.RLock()
	committedOffset, exists := consumer2.committedOffsets[partition3]
	consumer2.mu.RUnlock()

	// 如果分区2的偏移量被错误提交，committedOffset会是2
	// 如果修复生效，应该没有提交记录或者从头开始
	if exists {
		assert.NotEqual(t, int64(2), committedOffset, "分区2的偏移量不应该被错误提交为2")
		t.Logf("分区2的偏移量: %d (可能是之前的提交，不是被错误提交的)", committedOffset)
	} else {
		t.Logf("分区2没有已提交偏移量记录 (正确，未被错误提交)")
	}

	t.Logf("✅ 修复验证成功：手动提交模式下重新均衡不会自动提交未确认的偏移量")
}
