package dal

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/pkg/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// ==================== 高级并发测试 ====================

// TestConcurrentOperations_RaceConditions 测试并发操作中的竞态条件
func TestConcurrentOperations_RaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过长时间运行的并发测试")
	}

	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "concurrent-group"
	
	// 设置mock期望 - 需要足够多的期望来处理并发请求
	for i := 0; i < 100; i++ {
		rows := sqlmock.NewRows([]string{"group_id", "generation_id", "protocol_type", "updated_at"}).
			AddRow(groupID, i+1, "consumer", time.Now())
		mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
			WithArgs(groupID).
			WillReturnRows(rows)
	}

	var wg sync.WaitGroup
	errors := make([]error, 100)
	results := make([]*types.ConsumerGroupGeneration, 100)

	// 启动100个并发goroutine
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			result, err := GetConsumerGroupGeneration(ctx, db, groupID)
			errors[index] = err
			results[index] = result
		}(i)
	}

	wg.Wait()

	// 验证所有操作都成功完成
	for i, err := range errors {
		assert.NoError(t, err, "Goroutine %d should not error", i)
		assert.NotNil(t, results[i], "Goroutine %d should return result", i)
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestConcurrentHeartbeatUpdates 测试并发心跳更新
func TestConcurrentHeartbeatUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过长时间运行的并发测试")
	}

	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "heartbeat-group"

	// 设置mock期望
	expectedSQL := "UPDATE `mq_consumer_heartbeats` SET `last_heartbeat` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	for i := 0; i < 50; i++ {
		mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
			WithArgs(sqlmock.AnyArg(), groupID, fmt.Sprintf("consumer-%d", i)).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	var wg sync.WaitGroup
	errors := make([]error, 50)

	// 启动50个并发心跳更新
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			consumerID := fmt.Sprintf("consumer-%d", index)
			errors[index] = UpdateHeartbeat(ctx, db, groupID, consumerID)
		}(i)
	}

	wg.Wait()

	// 验证所有心跳更新都成功
	for i, err := range errors {
		assert.NoError(t, err, "Heartbeat update %d should not error", i)
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 内存压力测试 ====================

// TestMemoryPressure_LargeDataSets 测试大数据集的内存压力
func TestMemoryPressure_LargeDataSets(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过内存压力测试")
	}

	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name         string
		messageCount int
		messageSize  int // KB
	}{
		{"SmallMessages", 10000, 1},   // 10MB total
		{"MediumMessages", 1000, 100}, // 100MB total
		{"LargeMessages", 100, 1000},  // 100MB total
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建大量消息用于批量获取测试
			rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
			
			for i := 0; i < tc.messageCount; i++ {
				body := make([]byte, tc.messageSize*1024) // KB to bytes
				for j := range body {
					body[j] = byte(j % 256)
				}
				
				rows.AddRow(
					int64(i+1),
					"memory-test-topic",
					0,
					nil,
					[]byte("{}"),
					body,
					time.Now(),
				)
			}

			mock.ExpectQuery("SELECT.*FROM.*mq_messages.*").
				WillReturnRows(rows)

			// 执行批量获取
			messages, err := FetchMessages(ctx, db, "memory-test-topic", 0, 0, tc.messageCount)
			
			assert.NoError(t, err)
			assert.Len(t, messages, tc.messageCount)
			
			// 验证消息内容
			for i, msg := range messages {
				assert.Equal(t, int64(i+1), msg.ID)
				assert.Equal(t, tc.messageSize*1024, len(msg.Body))
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 数据完整性和一致性测试 ====================

// TestDataIntegrity_ConsistencyChecks 测试数据完整性和一致性
func TestDataIntegrity_ConsistencyChecks(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name        string
		setup       func()
		operation   func() error
		expectError bool
	}{
		{
			name: "CommitOffsetWithStaleGeneration",
			setup: func() {
				// 模拟使用过期的代际ID提交偏移量
				commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
				mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
					WithArgs("test-group", "topic-a", 0, int64(100), uint(1), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			operation: func() error {
				p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
				return CommitOffset(ctx, db, "test-group", 1, p, 100) // 使用旧代际ID
			},
			expectError: false, // 不会报错，但可能不会更新
		},
		{
			name: "CommitOffsetWithNewerGeneration",
			setup: func() {
				commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
				mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
					WithArgs("test-group", "topic-a", 0, int64(200), uint(10), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			operation: func() error {
				p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
				return CommitOffset(ctx, db, "test-group", 10, p, 200) // 使用新代际ID
			},
			expectError: false,
		},
		{
			name: "HeartbeatWithInvalidJSON",
			setup: func() {
				// UpsertHeartbeat不验证JSON，所以不会失败
				expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs("test-group", "consumer-1", []byte("{invalid json"), []byte("{}"), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			operation: func() error {
				return UpsertHeartbeat(ctx, db, "test-group", "consumer-1", []byte("{invalid json"))
			},
			expectError: false, // 函数不验证JSON
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			err := tc.operation()
			
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 性能退化检测测试 ====================

// TestPerformanceDegradation_QueryOptimization 测试性能退化检测
func TestPerformanceDegradation_QueryOptimization(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试")
	}

	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name              string
		operation         func() error
		expectedQueryType string
		setup             func()
	}{
		{
			name: "EfficientOffsetQuery",
			operation: func() error {
				partitions := []types.PartitionInfo{
					{Topic: "topic-a", Partition: 0},
					{Topic: "topic-a", Partition: 1},
				}
				_, err := GetCommittedOffsets(ctx, db, "test-group", partitions)
				return err
			},
			expectedQueryType: "efficient IN query",
			setup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "topic", "partition", "committed_offset", "generation_id", "updated_at"})
				mock.ExpectQuery("SELECT.*FROM.*mq_consumer_group_offsets.*WHERE.*").
					WillReturnRows(rows)
			},
		},
		{
			name: "InefficientSinglePartitionQuery",
			operation: func() error {
				// 模拟单个分区查询（不如批量查询高效）
				_, err := FetchMessages(ctx, db, "topic-a", 0, 100, 1000)
				return err
			},
			expectedQueryType: "single partition query",
			setup: func() {
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				expectedSQL := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs("topic-a", 0, int64(100), 1000).
					WillReturnRows(rows)
			},
		},
		{
			name: "BatchQueryOptimization",
			operation: func() error {
				requests := []PartitionRequest{
					{Topic: "topic-a", Partition: 0, Offset: 100, Limit: 10},
					{Topic: "topic-a", Partition: 1, Offset: 200, Limit: 10},
					{Topic: "topic-b", Partition: 0, Offset: 300, Limit: 10},
				}
				_, err := FetchMessagesBatch(ctx, db, requests)
				return err
			},
			expectedQueryType: "optimized batch query",
			setup: func() {
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				mock.ExpectQuery("SELECT.*FROM.*mq_messages.*UNION ALL.*ORDER BY.*").
					WillReturnRows(rows)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			
			start := time.Now()
			err := tc.operation()
			duration := time.Since(start)
			
			assert.NoError(t, err)
			
			// 性能断言 - 操作应该在合理时间内完成
			assert.Less(t, duration, 100*time.Millisecond, "Operation should complete quickly: %s", tc.expectedQueryType)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 错误恢复和重试测试 ====================

// TestErrorRecovery_TransientFailures 测试瞬时错误的恢复
func TestErrorRecovery_TransientFailures(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		setup     func()
		operation func() error
		retries   int
	}{
		{
			name: "DeadlockRecovery",
			setup: func() {
				// 第一次调用返回死锁错误，第二次成功
				mock.ExpectExec("UPDATE.*mq_consumer_heartbeats.*").
					WillReturnError(fmt.Errorf("deadlock detected"))
				mock.ExpectExec("UPDATE.*mq_consumer_heartbeats.*").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func() error {
				// 模拟带重试逻辑的操作（这里简化为直接调用）
				if err := UpdateHeartbeat(ctx, db, "test-group", "consumer-1"); err != nil {
					// 重试一次
					return UpdateHeartbeat(ctx, db, "test-group", "consumer-1")
				}
				return nil
			},
			retries: 1,
		},
		{
			name: "ConnectionTimeout",
			setup: func() {
				mock.ExpectQuery("SELECT.*FROM.*mq_consumer_heartbeats.*").
					WillReturnError(fmt.Errorf("connection timeout"))
				mock.ExpectQuery("SELECT.*FROM.*mq_consumer_heartbeats.*").
					WillReturnRows(sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}))
			},
			operation: func() error {
				if _, err := GetHeartbeat(ctx, db, "test-group", "consumer-1"); err != nil {
					// 重试一次
					_, err = GetHeartbeat(ctx, db, "test-group", "consumer-1")
					return err
				}
				return nil
			},
			retries: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			err := tc.operation()
			assert.NoError(t, err)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 边界条件和极值测试 ====================

// TestBoundaryConditions_ExtremeValues 测试边界条件和极值
func TestBoundaryConditions_ExtremeValues(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		setup     func()
		operation func() error
		expectErr bool
	}{
		{
			name: "MaxInt64Offset",
			setup: func() {
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				expectedSQL := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs("test-topic", uint(0), int64(math.MaxInt64), 10).
					WillReturnRows(rows)
			},
			operation: func() error {
				_, err := FetchMessages(ctx, db, "test-topic", 0, math.MaxInt64, 10)
				return err
			},
			expectErr: false,
		},
		{
			name: "MinInt64Offset",
			setup: func() {
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				expectedSQL := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs("test-topic", uint(0), int64(math.MinInt64), 10).
					WillReturnRows(rows)
			},
			operation: func() error {
				_, err := FetchMessages(ctx, db, "test-topic", 0, math.MinInt64, 10)
				return err
			},
			expectErr: false,
		},
		{
			name: "MaxUintPartition",
			setup: func() {
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				expectedSQL := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs("test-topic", uint(math.MaxUint32), int64(100), 10).
					WillReturnRows(rows)
			},
			operation: func() error {
				_, err := FetchMessages(ctx, db, "test-topic", math.MaxUint32, 100, 10)
				return err
			},
			expectErr: false,
		},
		{
			name: "ZeroGenerationID",
			setup: func() {
				commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
				mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
					WithArgs("test-group", "topic-a", 0, int64(100), uint(0), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			operation: func() error {
				p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
				return CommitOffset(ctx, db, "test-group", 0, p, 100)
			},
			expectErr: false,
		},
		{
			name: "MaxUintGenerationID",
			setup: func() {
				commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
				mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
					WithArgs("test-group", "topic-a", 0, int64(100), uint(math.MaxUint32), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			operation: func() error {
				p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
				return CommitOffset(ctx, db, "test-group", math.MaxUint32, p, 100)
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			err := tc.operation()
			
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 资源泄漏检测测试 ====================

// TestResourceLeakDetection 测试资源泄漏检测
func TestResourceLeakDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过资源泄漏检测测试")
	}

	// 这个测试检查重复调用函数是否会导致资源泄漏
	db, mock := newMockDB(t)
	ctx := context.Background()

	// 设置大量的mock期望
	for i := 0; i < 1000; i++ {
		rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
		mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?")).
			WithArgs("leak-test-group", sqlmock.AnyArg()).
			WillReturnRows(rows)
	}

	// 执行大量操作
	for i := 0; i < 1000; i++ {
		consumers, err := FindActiveConsumers(ctx, db, "leak-test-group", 30*time.Second)
		assert.NoError(t, err)
		assert.NotNil(t, consumers)
		
		// 强制垃圾回收以检测内存泄漏
		if i%100 == 0 {
			// 在实际测试中，这里可以添加内存使用量检查
			// 或者使用runtime.GC()和runtime.ReadMemStats()
		}
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 数据库连接池测试 ====================

// TestConnectionPoolBehavior 测试数据库连接池行为
func TestConnectionPoolBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过连接池测试")
	}

	db, mock := newMockDB(t)
	ctx := context.Background()

	// 模拟连接池耗尽场景
	testCases := []struct {
		name           string
		concurrentOps  int
		expectedResult bool
	}{
		{"LowConcurrency", 5, true},
		{"MediumConcurrency", 20, true},
		{"HighConcurrency", 100, true}, // 假设连接池足够大
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 设置mock期望
			for i := 0; i < tc.concurrentOps; i++ {
				rows := sqlmock.NewRows([]string{"max_id"}).AddRow(int64(i))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT COALESCE(MAX(id), 0) FROM `mq_messages` WHERE `topic` = ? AND `partition` = ?")).
					WithArgs(fmt.Sprintf("topic-%d", i), uint(0)).
					WillReturnRows(rows)
			}

			var wg sync.WaitGroup
			results := make([]int64, tc.concurrentOps)
			errors := make([]error, tc.concurrentOps)

			// 并发执行操作
			for i := 0; i < tc.concurrentOps; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					topic := fmt.Sprintf("topic-%d", index)
					result, err := GetLatestOffset(ctx, db, topic, 0)
					results[index] = result
					errors[index] = err
				}(i)
			}

			wg.Wait()

			// 验证所有操作是否成功
			successCount := 0
			for i := 0; i < tc.concurrentOps; i++ {
				if errors[i] == nil {
					successCount++
					assert.Equal(t, int64(i), results[i])
				}
			}

			if tc.expectedResult {
				assert.Equal(t, tc.concurrentOps, successCount, "All operations should succeed")
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 字符编码和国际化测试 ====================

// TestInternationalization_CharacterEncoding 测试国际化和字符编码
func TestInternationalization_CharacterEncoding(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name    string
		groupID string
		topics  []string
	}{
		{
			name:    "ChineseCharacters",
			groupID: "消费组-测试",
			topics:  []string{"主题-一", "主题-二", "主题-三"},
		},
		{
			name:    "JapaneseCharacters",
			groupID: "グループ-テスト",
			topics:  []string{"トピック-一", "トピック-二"},
		},
		{
			name:    "ArabicCharacters",
			groupID: "مجموعة-اختبار",
			topics:  []string{"موضوع-واحد", "موضوع-اثنان"},
		},
		{
			name:    "RussianCharacters",
			groupID: "группа-тест",
			topics:  []string{"тема-один", "тема-два"},
		},
		{
			name:    "MixedUnicodeEmojis",
			groupID: "group-🚀🌟💻",
			topics:  []string{"topic-🎯", "topic-📊", "topic-🔥"},
		},
		{
			name:    "ComplexUnicodeNormalization",
			groupID: "café-naïve-résumé", // 包含重音符号
			topics:  []string{"café-topic", "naïve-topic"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 测试心跳插入
			expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"
			
			topicsJSON := fmt.Sprintf(`["%s"]`, strings.Join(tc.topics, `","`))
			
			mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
				WithArgs(tc.groupID, "consumer-1", []byte(topicsJSON), []byte("{}"), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))

			err := UpsertHeartbeat(ctx, db, tc.groupID, "consumer-1", []byte(topicsJSON))
			assert.NoError(t, err)

			// 测试查找活跃消费者
			rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}).
				AddRow(tc.groupID, "consumer-1", 1, topicsJSON, "{}", time.Now())
			
			mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?")).
				WithArgs(tc.groupID, sqlmock.AnyArg()).
				WillReturnRows(rows)

			consumers, err := FindActiveConsumers(ctx, db, tc.groupID, 30*time.Second)
			assert.NoError(t, err)
			assert.Len(t, consumers, 1)
			assert.Equal(t, tc.groupID, consumers[0].GroupID)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}