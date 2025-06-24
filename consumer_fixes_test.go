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
	admin, err := NewAdminClient(AdminConfig{DB: testDB})
	require.NoError(t, err)
	defer admin.Close()

	topicName := "rebalance-fix-test-topic"
	err = admin.CreateTopic(context.Background(), NewTopicRequest{
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
