//go:build integration

package dbmq

import (
	"context"
	"database/sql"
	"dbmq/pkg/dbmq/types"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_FullFlow(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. Create a Topic for the test
	testTopic := &types.Topic{
		TopicName:      "integration-topic",
		PartitionCount: 1,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. Start Coordinator
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// Wait for the coordinator to become leader
	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond, "Coordinator did not become leader")

	// 3. Start Consumer
	consumerGroup := "test-group-1"
	consumerConf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             consumerGroup,
		NotificationEnabled: false, // 禁用通知以避免超时
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	}
	consumer, err := NewConsumer(consumerConf)
	require.NoError(t, err)
	err = consumer.Subscribe(testTopic.TopicName)
	require.NoError(t, err)
	defer consumer.Close()

	// Wait for the rebalance to happen and partitions to be assigned
	// We need to ensure the consumer has been assigned partitions and is not in rebalancing state
	log.Printf("Waiting for consumer to be ready...")
	require.Eventually(t, func() bool {
		ready := consumer.IsReady()
		log.Printf("Consumer ready: %v", ready)
		return ready
	}, 10*time.Second, 500*time.Millisecond, "Consumer should be ready (assigned partitions and not rebalancing)")
	log.Printf("Consumer is ready! Proceeding to create producer...")

	// 4. Start Producer and Send a Message
	producerConf := ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: false, // 禁用通知以避免超时
	}
	producer, err := NewProducer(producerConf)
	require.NoError(t, err)
	defer producer.Close()

	testKey := []byte("test-key")
	testValue := []byte("hello world")
	sentMsg := &ProducerMessage{
		Topic: testTopic.TopicName,
		Key:   testKey,
		Value: testValue,
	}
	log.Printf("Sending message: key=%s, value=%s", string(testKey), string(testValue))
	sendResult, err := producer.Send(context.Background(), sentMsg)
	require.NoError(t, err)
	assert.Equal(t, uint(0), sendResult.Partition)
	log.Printf("Message sent successfully: partition=%d, offset=%d", sendResult.Partition, sendResult.Offset)

	// 5. Consumer Polls and Receives the Message
	log.Printf("Starting to poll for messages...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receivedMsgs, err := consumer.Poll(ctx, 5*time.Second)
	log.Printf("Poll completed, received %d messages", len(receivedMsgs))
	require.NoError(t, err)
	require.Len(t, receivedMsgs, 1, "Consumer should have received exactly one message")

	receivedMsg := receivedMsgs[0]
	assert.Equal(t, testTopic.TopicName, receivedMsg.Topic)
	assert.Equal(t, testKey, receivedMsg.Key)
	assert.Equal(t, testValue, receivedMsg.Value)
	assert.Equal(t, sendResult.Offset, receivedMsg.Offset)

	// 6. Commit the offset
	err = consumer.CommitSync()
	require.NoError(t, err)

	// 7. Verify the commit in the database
	var committedOffset types.ConsumerGroupOffset
	err = dbClient.Where(&types.ConsumerGroupOffset{
		GroupID:   consumerGroup,
		Topic:     testTopic.TopicName,
		Partition: 0,
	}).First(&committedOffset).Error
	require.NoError(t, err, "Failed to find the committed offset in the database")
	assert.Equal(t, sendResult.Offset, committedOffset.CommittedOffset, "Committed offset in DB does not match sent message offset")
	log.Printf("Successfully verified committed offset in DB: %d", committedOffset.CommittedOffset)
}

func TestIntegration_MultiConsumerGroups(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. 创建测试主题（2个分区）
	testTopic := &types.Topic{
		TopicName:      "multi-group-topic",
		PartitionCount: 2,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为leader
	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond)

	// 3. 启动两个消费者组
	group1 := "test-group-1"
	group2 := "test-group-2"

	// 创建消费者组1的两个消费者
	consumer1_1, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             group1,
		NotificationEnabled: false, // 禁用通知以避免超时
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	defer consumer1_1.Close()

	consumer1_2, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             group1,
		NotificationEnabled: false, // 禁用通知以避免超时
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	defer consumer1_2.Close()

	// 创建消费者组2的消费者
	consumer2, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             group2,
		NotificationEnabled: false, // 禁用通知以避免超时
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	defer consumer2.Close()

	// 订阅主题
	require.NoError(t, consumer1_1.Subscribe(testTopic.TopicName))
	require.NoError(t, consumer1_2.Subscribe(testTopic.TopicName))
	require.NoError(t, consumer2.Subscribe(testTopic.TopicName))

	// 等待所有消费者准备就绪
	consumers := []*Consumer{consumer1_1, consumer1_2, consumer2}
	for i, c := range consumers {
		require.Eventually(t, func() bool {
			return c.IsReady()
		}, 10*time.Second, 500*time.Millisecond, "Consumer %d should be ready", i)
	}

	// 4. 创建生产者并发送消息
	producer, err := NewProducer(ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: false, // 禁用通知以避免超时
	})
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息到两个分区
	messages := []struct {
		key   string
		value string
	}{
		{"key1", "message1"},
		{"key2", "message2"},
		{"key3", "message3"},
		{"key4", "message4"},
	}

	for _, msg := range messages {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   []byte(msg.key),
			Value: []byte(msg.value),
		})
		require.NoError(t, err)
	}

	// 5. 消费者轮询消息（不依赖Redis通知）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var allMessages []ConsumerMessage
	for _, consumer := range consumers {
		receivedMsgs, err := consumer.Poll(ctx, 2*time.Second)
		require.NoError(t, err)
		allMessages = append(allMessages, receivedMsgs...)

		if len(receivedMsgs) > 0 {
			err = consumer.CommitSync()
			require.NoError(t, err)
		}
	}

	// 6. 验证消息分布
	assert.True(t, len(allMessages) > 0, "至少应该收到一些消息")

	// 验证每个消费者组都有提交的偏移量
	for _, groupID := range []string{group1, group2} {
		var offsets []types.ConsumerGroupOffset
		err = dbClient.Where("group_id = ?", groupID).Find(&offsets).Error
		require.NoError(t, err)
		assert.True(t, len(offsets) > 0, "消费者组 %s 应该有提交的偏移量", groupID)
	}
}

func TestIntegration_ConsumerFailover(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. 创建测试主题
	testTopic := &types.Topic{
		TopicName:      "failover-topic",
		PartitionCount: 2,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  3 * time.Second, // 较短的超时时间以便快速故障检测
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond)

	// 3. 启动第一个消费者
	consumer1, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "failover-group",
		NotificationEnabled: false, // 禁用通知
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, consumer1.Subscribe(testTopic.TopicName))

	// 等待消费者准备就绪
	require.Eventually(t, consumer1.IsReady, 10*time.Second, 500*time.Millisecond)

	// 4. 发送一些消息
	producer, err := NewProducer(ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	for i := 0; i < 4; i++ {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, err)
	}

	// 5. 消费者1消费消息
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msgs1, err := consumer1.Poll(ctx, 2*time.Second)
	cancel()
	require.NoError(t, err)

	if len(msgs1) > 0 {
		require.NoError(t, consumer1.CommitSync())
	}

	// 6. 启动第二个消费者
	consumer2, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "failover-group",
		NotificationEnabled: false,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	defer consumer2.Close()
	require.NoError(t, consumer2.Subscribe(testTopic.TopicName))

	// 等待重新平衡
	time.Sleep(3 * time.Second)

	// 7. 关闭第一个消费者（模拟故障）
	consumer1.Close()

	// 等待故障检测和重新平衡
	time.Sleep(5 * time.Second)

	// 8. 验证第二个消费者接管了所有分区
	require.Eventually(t, consumer2.IsReady, 10*time.Second, 500*time.Millisecond)

	// 9. 验证消费者2可以消费剩余消息
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	msgs2, err := consumer2.Poll(ctx2, 2*time.Second)
	cancel2()
	require.NoError(t, err)

	// 验证至少收到了一些消息
	totalMsgs := len(msgs1) + len(msgs2)
	assert.True(t, totalMsgs > 0, "应该总共收到一些消息")

	if len(msgs2) > 0 {
		require.NoError(t, consumer2.CommitSync())
	}

	// 10. 验证偏移量记录
	var offsets []types.ConsumerGroupOffset
	err = dbClient.Where("group_id = ?", "failover-group").Find(&offsets).Error
	require.NoError(t, err)
	assert.True(t, len(offsets) > 0, "应该有提交的偏移量记录")
}

func TestIntegration_MessageCleanup(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. 创建测试主题
	testTopic := &types.Topic{
		TopicName:      "cleanup-topic",
		PartitionCount: 1,
		Configs: sql.NullString{
			String: `{"retention_hours": 1}`,
			Valid:  true,
		},
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond)

	// 3. 创建生产者
	producer, err := NewProducer(ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 4. 发送一些消息
	now := time.Now()
	oldTime := now.Add(-2 * time.Hour) // 2小时前

	messages := []struct {
		key       string
		value     string
		timestamp time.Time
	}{
		{"old-1", "value-1", oldTime},
		{"old-2", "value-2", oldTime},
		{"new-1", "value-3", now},
		{"new-2", "value-4", now},
	}

	var oldMsgIDs []int64
	var newMsgIDs []int64

	for _, msg := range messages {
		result, err := producer.Send(context.Background(), &ProducerMessage{
			Topic: testTopic.TopicName,
			Key:   []byte(msg.key),
			Value: []byte(msg.value),
		})
		require.NoError(t, err)

		// 获取消息ID并更新时间戳
		var message types.Message
		err = dbClient.Where("topic = ? AND `partition` = ?", result.Topic, result.Partition).
			Order("id DESC").First(&message).Error
		require.NoError(t, err)

		// 更新消息的创建时间
		err = dbClient.Model(&message).Update("created_at", msg.timestamp).Error
		require.NoError(t, err)

		if msg.timestamp.Before(now) {
			oldMsgIDs = append(oldMsgIDs, message.ID)
		} else {
			newMsgIDs = append(newMsgIDs, message.ID)
		}
	}

	// 5. 执行消息清理
	err = coordinator.CleanupExpiredMessages(context.Background())
	require.NoError(t, err)

	// 6. 验证旧消息已被删除，新消息仍然存在
	for _, msgID := range oldMsgIDs {
		var count int64
		err = dbClient.Table("mq_messages").Where("id = ?", msgID).Count(&count).Error
		require.NoError(t, err)
		assert.Equal(t, int64(0), count, "过期消息应该被删除: %d", msgID)
	}

	for _, msgID := range newMsgIDs {
		var count int64
		err = dbClient.Table("mq_messages").Where("id = ?", msgID).Count(&count).Error
		require.NoError(t, err)
		assert.Equal(t, int64(1), count, "未过期消息应该保留: %d", msgID)
	}
}

func TestIntegration_RedisNotification(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. 创建测试主题
	testTopic := &types.Topic{
		TopicName:      "notification-topic",
		PartitionCount: 1,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. 启动协调器
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond)

	// 3. 启动消费者（启用通知）
	consumer, err := NewConsumer(ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "notification-group",
		NotificationEnabled: true, // 启用通知进行测试
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	})
	require.NoError(t, err)
	defer consumer.Close()

	require.NoError(t, consumer.Subscribe(testTopic.TopicName))
	require.Eventually(t, consumer.IsReady, 10*time.Second, 500*time.Millisecond)

	// 4. 创建生产者（启用通知）
	producer, err := NewProducer(ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: true, // 启用通知进行测试
	})
	require.NoError(t, err)
	defer producer.Close()

	// 5. 发送消息并验证基本功能
	_, err = producer.Send(context.Background(), &ProducerMessage{
		Topic: testTopic.TopicName,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	})
	require.NoError(t, err)

	// 6. 验证消费者可以接收消息（使用较短的超时）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	receivedMsgs, err := consumer.Poll(ctx, 3*time.Second)
	require.NoError(t, err)

	// 验证收到了消息
	assert.True(t, len(receivedMsgs) > 0, "应该收到至少一条消息")

	if len(receivedMsgs) > 0 {
		assert.Equal(t, "test-key", string(receivedMsgs[0].Key))
		assert.Equal(t, "test-value", string(receivedMsgs[0].Value))

		// 提交偏移量
		err = consumer.CommitSync()
		require.NoError(t, err)
	}

	// 7. 验证偏移量已提交
	var offset types.ConsumerGroupOffset
	err = dbClient.Where(&types.ConsumerGroupOffset{
		GroupID:   "notification-group",
		Topic:     testTopic.TopicName,
		Partition: 0,
	}).First(&offset).Error
	require.NoError(t, err)
	assert.True(t, offset.CommittedOffset > 0, "偏移量应该已提交")
}
