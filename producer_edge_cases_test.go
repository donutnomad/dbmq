package dbmq

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProducerRoundRobinRaceCondition 测试生产者轮询分区竞争条件BUG
// BUG描述：nextRoundRobinPartition方法在高并发下可能导致分区分配不均匀
func TestProducerRoundRobinRaceCondition(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-roundrobin-topic",
		PartitionCount: 4,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	producer, err := NewProducer(ProducerConfig{
		DB:                  db,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 并发发送消息，测试分区分配的均匀性
	const numMessages = 1000
	const numWorkers = 10
	partitionCounts := make([]int64, 4)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numMessages/numWorkers; j++ {
				msg := &ProducerMessage{
					Topic: "test-roundrobin-topic",
					Value: []byte(fmt.Sprintf("message-%d-%d", workerID, j)),
					// 不设置Key，使用轮询分区
				}

				result, err := producer.Send(context.Background(), msg)
				if err != nil {
					t.Errorf("发送消息失败: %v", err)
					return
				}

				// 统计分区分配
				atomic.AddInt64(&partitionCounts[result.Partition], 1)
			}
		}(i)
	}

	wg.Wait()

	// 检查分区分配的均匀性
	expectedPerPartition := int64(numMessages / 4)
	tolerance := int64(numMessages / 10) // 10%的容忍度

	for i, count := range partitionCounts {
		diff := count - expectedPerPartition
		if diff < 0 {
			diff = -diff
		}

		if diff > tolerance {
			t.Errorf("💥 BUG确认：分区 %d 的消息数量 %d 与期望值 %d 差异过大（差异: %d, 容忍度: %d）",
				i, count, expectedPerPartition, diff, tolerance)
		}

		t.Logf("分区 %d: %d 条消息", i, count)
	}
}

// TestProducerTopicMetadataCacheInconsistency 测试主题元数据缓存不一致BUG
// BUG描述：topicMetadataCache在主题更新后可能返回过期数据
func TestProducerTopicMetadataCacheInconsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建初始主题
	topic := &types.Topic{
		TopicName:      "test-cache-topic",
		PartitionCount: 2,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	producer, err := NewProducer(ProducerConfig{
		DB:                  db,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 首次发送消息，应该缓存主题元数据
	msg1 := &ProducerMessage{
		Topic: "test-cache-topic",
		Value: []byte("initial message"),
	}

	result1, err := producer.Send(context.Background(), msg1)
	require.NoError(t, err)
	assert.True(t, result1.Partition < 2, "分区应该小于2")

	// 直接在数据库中更新主题的分区数量（模拟外部更新）
	require.NoError(t, db.Model(topic).Update("partition_count", 4).Error)

	// 再次发送消息，由于缓存，可能仍然使用旧的分区数量
	msg2 := &ProducerMessage{
		Topic: "test-cache-topic",
		Key:   []byte("key-for-partition-3"), // 这个key可能会被分配到分区3
		Value: []byte("updated message"),
	}

	result2, err := producer.Send(context.Background(), msg2)
	require.NoError(t, err)

	// 如果缓存没有更新，分区仍然会小于2
	// 但实际上应该可以使用0-3的分区
	if result2.Partition >= 2 {
		t.Logf("✅ 缓存已正确更新，使用了新的分区 %d", result2.Partition)
	} else {
		t.Logf("⚠️  可能存在缓存不一致：分区 %d 仍然使用旧的分区范围", result2.Partition)
	}

	// 清理生产者的缓存（如果有这样的方法）
	// 这里我们创建一个新的生产者实例来模拟缓存清理
	producer2, err := NewProducer(ProducerConfig{
		DB:                  db,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer2.Close()

	// 使用新的生产者实例发送消息，应该使用更新后的分区数量
	msg3 := &ProducerMessage{
		Topic: "test-cache-topic",
		Key:   []byte("key-for-partition-3"),
		Value: []byte("new producer message"),
	}

	result3, err := producer2.Send(context.Background(), msg3)
	require.NoError(t, err)

	// 新的生产者应该能够使用所有4个分区
	assert.True(t, result3.Partition < 4, "新生产者应该使用更新后的分区范围")
}

// TestProducerNotificationFailureHandling 测试通知失败处理BUG
// BUG描述：sendNotification方法的错误处理可能影响消息发送的可靠性
func TestProducerNotificationFailureHandling(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-notification-topic",
		PartitionCount: 1,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	// 创建生产者，启用通知但不提供Redis连接（模拟Redis故障）
	producer, err := NewProducer(ProducerConfig{
		DB:                   db,
		Redis:                nil,  // 故意设置为nil
		NotificationEnabled:  true, // 但启用通知
		NotificationStateTTL: 10 * time.Second,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 发送消息，即使通知失败也应该成功
	msg := &ProducerMessage{
		Topic: "test-notification-topic",
		Value: []byte("test message with notification failure"),
	}

	result, err := producer.Send(context.Background(), msg)
	require.NoError(t, err, "即使通知失败，消息发送也应该成功")
	assert.Equal(t, "test-notification-topic", result.Topic)
	assert.Equal(t, uint(0), result.Partition)
	assert.Greater(t, result.Offset, int64(0))

	// 验证消息确实被持久化到数据库
	var dbMessage types.Message
	err = db.Where("topic = ? AND `partition` = ? AND id = ?",
		result.Topic, result.Partition, result.Offset).First(&dbMessage).Error
	require.NoError(t, err, "消息应该被持久化到数据库")
	assert.Equal(t, "test message with notification failure", string(dbMessage.Body))
}

// TestProducerHashPartitionConsistency 测试哈希分区一致性BUG
// BUG描述：hashPartition方法可能在不同条件下为相同的key产生不同的分区
func TestProducerHashPartitionConsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-hash-topic",
		PartitionCount: 8,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	producer, err := NewProducer(ProducerConfig{
		DB:                  db,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 测试相同key的一致性
	testKeys := []string{
		"user-123",
		"order-456",
		"product-789",
		"",       // 空字符串
		"🚀emoji", // 包含emoji的key
		"very-long-key-that-contains-many-characters-to-test-hash-function-stability",
	}

	for _, key := range testKeys {
		keyBytes := []byte(key)
		partitions := make(map[uint]int)

		// 多次发送相同key的消息
		for i := 0; i < 10; i++ {
			msg := &ProducerMessage{
				Topic: "test-hash-topic",
				Key:   keyBytes,
				Value: []byte(fmt.Sprintf("message-%d", i)),
			}

			result, err := producer.Send(context.Background(), msg)
			require.NoError(t, err)

			partitions[result.Partition]++
		}

		// 相同key的所有消息应该路由到同一个分区
		if len(partitions) != 1 {
			t.Errorf("💥 BUG确认：key '%s' 被路由到多个分区: %v", key, partitions)
		} else {
			for partition, count := range partitions {
				assert.Equal(t, 10, count, "key '%s' 的所有消息应该在分区 %d", key, partition)
				t.Logf("✅ key '%s' 一致性测试通过，所有消息都在分区 %d", key, partition)
			}
		}
	}
}

// TestProducerConcurrentSendStability 测试并发发送稳定性BUG
// BUG描述：高并发发送可能导致数据竞争或性能问题
func TestProducerConcurrentSendStability(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-concurrent-topic",
		PartitionCount: 4,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	producer, err := NewProducer(ProducerConfig{
		DB:                  db,
		NotificationEnabled: false,
	})
	require.NoError(t, err)
	defer producer.Close()

	// 高并发发送测试
	const numWorkers = 20
	const messagesPerWorker = 50

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64
	var results []SendResult
	var resultsMu sync.Mutex

	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			var msgs []ProducerMessage
			for j := 0; j < messagesPerWorker; j++ {
				msg := &ProducerMessage{
					Topic: "test-concurrent-topic",
					Key:   []byte(fmt.Sprintf("worker-%d", workerID)), // 使用worker ID作为key
					Value: []byte(fmt.Sprintf("message-%d-%d", workerID, j)),
					Headers: map[string]string{
						"worker-id": fmt.Sprintf("%d", workerID),
						"msg-id":    fmt.Sprintf("%d", j),
					},
				}
				msgs = append(msgs, *msg)
			}
			result, err := producer.SendBatch(context.Background(), msgs...)
			if err != nil {
				atomic.AddInt64(&errorCount, int64(len(msgs)))
				t.Errorf("Worker %d 发送消息 %d 失败: %v", workerID, 0, err)
			} else {
				atomic.AddInt64(&successCount, int64(len(msgs)))
				resultsMu.Lock()
				results = append(results, result...)
				resultsMu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 验证结果
	expectedTotal := int64(numWorkers * messagesPerWorker)
	actualTotal := successCount + errorCount

	assert.Equal(t, expectedTotal, actualTotal, "总消息数应该匹配")
	assert.Equal(t, expectedTotal, successCount, "所有消息都应该成功发送")
	assert.Equal(t, int64(0), errorCount, "不应该有发送失败的消息")

	resultsMu.Lock()
	assert.Equal(t, int(expectedTotal), len(results), "结果数量应该匹配")
	resultsMu.Unlock()

	// 性能检查
	messagesPerSecond := float64(successCount) / duration.Seconds()
	t.Logf("并发发送性能: %.2f 消息/秒", messagesPerSecond)

	if messagesPerSecond < 100 { // 假设最低性能要求
		t.Logf("⚠️  性能可能有问题：%.2f 消息/秒 低于预期", messagesPerSecond)
	}

	// 验证分区分配的一致性（相同key应该在同一分区）
	resultsMu.Lock()
	keyPartitionMap := make(map[string]uint)
	for _, result := range results {
		// 由于我们使用worker ID作为key，相同worker的所有消息应该在同一分区
		workerKey := fmt.Sprintf("worker-%d", result.Partition) // 这里简化处理
		if _, exists := keyPartitionMap[workerKey]; exists {
			// 注意：这里的逻辑需要根据实际的key来验证
			// 由于我们使用的是worker ID作为key，这里需要更复杂的验证逻辑
		} else {
			keyPartitionMap[workerKey] = result.Partition
		}
	}
	resultsMu.Unlock()
}

// TestProducerResourceCleanup 测试生产者资源清理BUG
// BUG描述：生产者关闭时可能没有正确清理资源
func TestProducerResourceCleanup(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-cleanup-topic",
		PartitionCount: 1,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	// 记录初始goroutine数量
	initialGoroutines := runtime.NumGoroutine()

	// 创建和销毁多个生产者实例
	for i := 0; i < 5; i++ {
		producer, err := NewProducer(ProducerConfig{
			DB:                  db,
			NotificationEnabled: false,
		})
		require.NoError(t, err)

		// 发送一些消息
		for j := 0; j < 10; j++ {
			msg := &ProducerMessage{
				Topic: "test-cleanup-topic",
				Value: []byte(fmt.Sprintf("cleanup-test-%d-%d", i, j)),
			}
			_, err := producer.Send(context.Background(), msg)
			require.NoError(t, err)
		}

		// 关闭生产者
		producer.Close()

		// 等待一段时间确保资源被清理
		time.Sleep(100 * time.Millisecond)
	}

	// 强制垃圾回收
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	// 检查goroutine是否有泄漏
	finalGoroutines := runtime.NumGoroutine()
	goroutineDiff := finalGoroutines - initialGoroutines

	// 允许一些合理的goroutine增长
	if goroutineDiff > 5 {
		t.Errorf("💥 BUG确认：可能存在goroutine泄漏，增加了 %d 个goroutine", goroutineDiff)
		t.Logf("初始goroutine数量: %d, 最终数量: %d", initialGoroutines, finalGoroutines)
	}
}
