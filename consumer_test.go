package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/types"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to sort PartitionInfo slices for consistent comparison

// Helper function to sort PartitionInfo slices for consistent comparison
func sortPartitions(partitions []types.PartitionInfo) {
	SortPartitionsByTopicAndPartition(partitions)
}

func TestFindRevokedPartitions(t *testing.T) {
	testCases := []struct {
		name           string
		current        map[string][]uint
		next           map[string][]uint
		expectedRevoke []types.PartitionInfo
	}{
		{
			name: "No change",
			current: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {2},
			},
			next: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {2},
			},
			expectedRevoke: []types.PartitionInfo{},
		},
		{
			name: "Partition removed from a topic",
			current: map[string][]uint{
				"topic-a": {0, 1, 2},
			},
			next: map[string][]uint{
				"topic-a": {0, 2},
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 1},
			},
		},
		{
			name: "Entire topic removed",
			current: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {0},
			},
			next: map[string][]uint{
				"topic-a": {0, 1},
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-b", Partition: 0},
			},
		},
		{
			name: "All partitions revoked",
			current: map[string][]uint{
				"topic-a": {0, 1},
			},
			next: map[string][]uint{},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 0},
				{Topic: "topic-a", Partition: 1},
			},
		},
		{
			name:    "Start with empty assignment",
			current: map[string][]uint{},
			next: map[string][]uint{
				"topic-a": {0},
			},
			expectedRevoke: []types.PartitionInfo{},
		},
		{
			name: "Complex change",
			current: map[string][]uint{
				"topic-a": {0, 1, 2},
				"topic-b": {0, 1},
				"topic-c": {5},
			},
			next: map[string][]uint{
				"topic-a": {2, 3}, // 0, 1 revoked
				"topic-c": {5},    // no change
				"topic-d": {0},    // new topic
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 0},
				{Topic: "topic-a", Partition: 1},
				{Topic: "topic-b", Partition: 0},
				{Topic: "topic-b", Partition: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := &Consumer{
				mu:         sync.RWMutex{},
				assignment: tc.current,
			}

			revoked := consumer.findRevokedPartitions(tc.next)

			// Sort both slices for consistent comparison
			sortPartitions(revoked)
			sortPartitions(tc.expectedRevoke)

			// Handle nil vs empty slice issue for reflect.DeepEqual
			if len(revoked) == 0 && len(tc.expectedRevoke) == 0 {
				// If both are empty (one might be nil, one might be an empty slice),
				// we consider them equal for the purpose of this test.
				return
			}

			if !reflect.DeepEqual(revoked, tc.expectedRevoke) {
				t.Errorf("findRevokedPartitions() failed\nGot: %v\nWant: %v", revoked, tc.expectedRevoke)
			}
		})
	}
}

func TestConsumerHeartbeat(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 创建测试Topic
	testTopic := &types.Topic{
		TopicName:      "heartbeat-test-topic",
		PartitionCount: 2,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为Leader
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 创建消费者
	consumerConf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "heartbeat-test-group",
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
	}
	consumer, err := NewConsumer(consumerConf)
	require.NoError(t, err)
	defer consumer.Close()

	// 订阅Topic
	err = consumer.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待消费者准备就绪
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 验证心跳记录存在
	var heartbeat types.ConsumerHeartbeat
	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumerConf.GroupID, consumer.id).First(&heartbeat).Error
	require.NoError(t, err)

	// 验证心跳时间戳在合理范围内
	assert.True(t, time.Since(heartbeat.LastHeartbeat) < 2*time.Second)

	// 等待几个心跳周期
	time.Sleep(2 * time.Second)

	// 再次检查心跳是否更新
	var newHeartbeat types.ConsumerHeartbeat
	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumerConf.GroupID, consumer.id).First(&newHeartbeat).Error
	require.NoError(t, err)

	assert.True(t, newHeartbeat.LastHeartbeat.After(heartbeat.LastHeartbeat))
}

func TestConsumerRebalanceProcess(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 创建测试Topic
	testTopic := &types.Topic{
		TopicName:      "rebalance-test-topic",
		PartitionCount: 4,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为Leader
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 创建第一个消费者
	consumer1Conf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "rebalance-test-group",
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
	}
	consumer1, err := NewConsumer(consumer1Conf)
	require.NoError(t, err)
	defer consumer1.Close()

	err = consumer1.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待第一个消费者准备就绪
	require.Eventually(t, consumer1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 验证第一个消费者获得了所有分区
	var heartbeat1 types.ConsumerHeartbeat
	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumer1Conf.GroupID, consumer1.id).First(&heartbeat1).Error
	require.NoError(t, err)

	var assignedPartitions1List []types.PartitionInfo = heartbeat1.AssignedPartitions

	assignedPartitions1 := make(map[string][]uint)
	for _, p := range assignedPartitions1List {
		if _, ok := assignedPartitions1[p.Topic]; !ok {
			assignedPartitions1[p.Topic] = make([]uint, 0)
		}
		assignedPartitions1[p.Topic] = append(assignedPartitions1[p.Topic], p.Partition)
	}
	assert.Len(t, assignedPartitions1[testTopic.TopicName], 4)

	// 创建第二个消费者
	consumer2Conf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "rebalance-test-group",
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
	}
	consumer2, err := NewConsumer(consumer2Conf)
	require.NoError(t, err)
	defer consumer2.Close()

	err = consumer2.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待第二个消费者准备就绪和再均衡完成
	require.Eventually(t, consumer2.IsReady, 5*time.Second, 100*time.Millisecond)
	time.Sleep(2 * time.Second) // 等待再均衡完成

	// 验证分区重新分配
	var heartbeat1After, heartbeat2After types.ConsumerHeartbeat
	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumer1Conf.GroupID, consumer1.id).First(&heartbeat1After).Error
	require.NoError(t, err)

	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumer2Conf.GroupID, consumer2.id).First(&heartbeat2After).Error
	require.NoError(t, err)

	var assignedPartitions1AfterList []types.PartitionInfo = heartbeat1After.AssignedPartitions

	assignedPartitions1After := make(map[string][]uint)
	for _, p := range assignedPartitions1AfterList {
		if _, ok := assignedPartitions1After[p.Topic]; !ok {
			assignedPartitions1After[p.Topic] = make([]uint, 0)
		}
		assignedPartitions1After[p.Topic] = append(assignedPartitions1After[p.Topic], p.Partition)
	}

	var assignedPartitions2AfterList []types.PartitionInfo = heartbeat2After.AssignedPartitions

	assignedPartitions2After := make(map[string][]uint)
	for _, p := range assignedPartitions2AfterList {
		if _, ok := assignedPartitions2After[p.Topic]; !ok {
			assignedPartitions2After[p.Topic] = make([]uint, 0)
		}
		assignedPartitions2After[p.Topic] = append(assignedPartitions2After[p.Topic], p.Partition)
	}

	// 验证分区被平均分配
	assert.Len(t, assignedPartitions1After[testTopic.TopicName], 2)
	assert.Len(t, assignedPartitions2After[testTopic.TopicName], 2)

	// 验证分区没有重复分配
	allPartitions := make(map[uint]bool)
	for _, p := range assignedPartitions1After[testTopic.TopicName] {
		allPartitions[p] = true
	}
	for _, p := range assignedPartitions2After[testTopic.TopicName] {
		assert.False(t, allPartitions[p], "Partition %d was assigned to multiple consumers", p)
		allPartitions[p] = true
	}
	assert.Len(t, allPartitions, 4)

	// 关闭第二个消费者，测试分区回收
	consumer2.Close()
	time.Sleep(3 * time.Second) // 等待心跳超时和再均衡

	// 验证所有分区重新分配给第一个消费者
	var heartbeat1Final types.ConsumerHeartbeat
	err = dbClient.Where("group_id = ? AND consumer_id = ?",
		consumer1Conf.GroupID, consumer1.id).First(&heartbeat1Final).Error
	require.NoError(t, err)

	var assignedPartitions1FinalList []types.PartitionInfo = heartbeat1Final.AssignedPartitions

	assignedPartitions1Final := make(map[string][]uint)
	for _, p := range assignedPartitions1FinalList {
		if _, ok := assignedPartitions1Final[p.Topic]; !ok {
			assignedPartitions1Final[p.Topic] = make([]uint, 0)
		}
		assignedPartitions1Final[p.Topic] = append(assignedPartitions1Final[p.Topic], p.Partition)
	}
	assert.Len(t, assignedPartitions1Final[testTopic.TopicName], 4)
}

func TestConsumerOffsetCommitAndRestore(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 创建测试Topic
	testTopic := &types.Topic{
		TopicName:      "offset-test-topic",
		PartitionCount: 2,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为Leader
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 创建生产者并发送一些消息
	producerConf := ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: true,
	}
	producer, err := NewProducer(producerConf)
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息到不同分区
	messages := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key-1"), []byte("value-1")},
		{[]byte("key-2"), []byte("value-2")},
		{[]byte("key-3"), []byte("value-3")},
		{[]byte("key-4"), []byte("value-4")},
	}

	var sentOffsets = make(map[uint]int64)
	for _, msg := range messages {
		result, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   msg.key,
			Value: msg.value,
		})
		require.NoError(t, err)
		sentOffsets[result.Partition] = result.Offset
	}

	// 创建第一个消费者实例并消费消息
	consumer1Conf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "offset-test-group",
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
		ConsumeStrategy:     ConsumeFromEarliest, // 从最早的消息开始消费
	}
	consumer1, err := NewConsumer(consumer1Conf)
	require.NoError(t, err)
	err = consumer1.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待消费者准备就绪
	require.Eventually(t, consumer1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 消费消息
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	receivedMsgs, err := consumer1.Poll(ctx, 1*time.Second)
	cancel()
	require.NoError(t, err)
	require.Len(t, receivedMsgs, len(messages))

	// 提交偏移量
	err = consumer1.CommitSync()
	require.NoError(t, err)

	// 验证偏移量已提交到数据库
	for partition, offset := range sentOffsets {
		var committedOffset types.ConsumerGroupOffset
		err = dbClient.Where(&types.ConsumerGroupOffset{
			GroupID:   consumer1Conf.GroupID,
			Topic:     testTopic.TopicName,
			Partition: partition,
		}).First(&committedOffset).Error
		require.NoError(t, err)
		assert.Equal(t, offset, committedOffset.CommittedOffset)
	}

	// 关闭第一个消费者
	consumer1.Close()

	// 创建第二个消费者实例（同一个消费组）
	consumer2Conf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "offset-test-group", // 使用相同的消费组
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
		ConsumeStrategy:     ConsumeFromCommitted, // 从已提交的偏移量开始消费
	}
	consumer2, err := NewConsumer(consumer2Conf)
	require.NoError(t, err)
	defer consumer2.Close()

	err = consumer2.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待消费者准备就绪
	require.Eventually(t, consumer2.IsReady, 5*time.Second, 100*time.Millisecond)

	// 发送新消息
	newMessages := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key-5"), []byte("value-5")},
		{[]byte("key-6"), []byte("value-6")},
	}

	for _, msg := range newMessages {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   msg.key,
			Value: msg.value,
		})
		require.NoError(t, err)
	}

	// 新消费者应该只收到新消息
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	receivedMsgs, err = consumer2.Poll(ctx, 1*time.Second)
	cancel()
	require.NoError(t, err)
	assert.Len(t, receivedMsgs, len(newMessages))

	// 验证收到的是新消息而不是旧消息
	for _, msg := range receivedMsgs {
		assert.True(t, string(msg.Key) == "key-5" || string(msg.Key) == "key-6",
			"Received unexpected message: %s", string(msg.Key))
	}
}

func TestConsumerOffsetReset(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 创建测试Topic
	testTopic := &types.Topic{
		TopicName:      "offset-reset-topic",
		PartitionCount: 1,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为Leader
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 创建生产者并发送一些消息
	producerConf := ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: true,
	}
	producer, err := NewProducer(producerConf)
	require.NoError(t, err)
	defer producer.Close()

	// 发送5条消息
	for i := 0; i < 5; i++ {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, err)
	}

	// 创建消费者并消费消息
	consumerConf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "offset-reset-group",
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   500 * time.Millisecond,
		ConsumeStrategy:     ConsumeFromEarliest, // 从最早的消息开始消费
	}
	consumer, err := NewConsumer(consumerConf)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待消费者准备就绪
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 消费消息
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	receivedMsgs, err := consumer.Poll(ctx, 1*time.Second)
	cancel()
	require.NoError(t, err)
	require.Len(t, receivedMsgs, 5)

	// 提交偏移量
	err = consumer.CommitSync()
	require.NoError(t, err)

	// 手动重置偏移量到开始位置
	err = dbClient.Delete(&types.ConsumerGroupOffset{
		GroupID: consumerConf.GroupID,
		Topic:   testTopic.TopicName,
	}).Error
	require.NoError(t, err)

	// 重新创建消费者
	consumer.Close()
	consumer, err = NewConsumer(consumerConf)
	require.NoError(t, err)
	defer consumer.Close()

	err = consumer.SubscribeTopics(testTopic.TopicName)
	require.NoError(t, err)

	// 等待消费者准备就绪
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 应该能再次收到所有消息
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	receivedMsgs, err = consumer.Poll(ctx, 1*time.Second)
	cancel()
	require.NoError(t, err)
	require.Len(t, receivedMsgs, 5)

	// 验证消息顺序
	for i, msg := range receivedMsgs {
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(msg.Key))
		assert.Equal(t, fmt.Sprintf("value-%d", i), string(msg.Value))
	}
}
