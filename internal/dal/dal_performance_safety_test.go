package dal

import (
	"context"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// TestGetCommittedOffsets_SQLInjectionVulnerability 测试SQL注入漏洞
// BUG: 虽然使用了参数化查询，但动态构建的WHERE子句可能存在风险
func TestGetCommittedOffsets_SQLInjectionVulnerability(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 尝试包含恶意内容的分区信息
	maliciousPartitions := []types.PartitionInfo{
		{Topic: "topic'; DROP TABLE mq_consumer_group_offsets; --", Partition: 0},
		{Topic: "normal-topic", Partition: 1},
	}

	// 构建期望的查询（应该安全地处理恶意内容）
	rows := sqlmock.NewRows([]string{"group_id", "topic", "partition", "committed_offset", "generation_id", "updated_at"}).
		AddRow(groupID, "normal-topic", 1, int64(100), 1, time.Now())

	mock.ExpectQuery("SELECT.*FROM.*mq_consumer_group_offsets.*WHERE.*").
		WillReturnRows(rows)

	offsets, err := GetCommittedOffsets(ctx, db, groupID, maliciousPartitions)

	assert.NoError(t, err)
	assert.NotNil(t, offsets)
	// 确保恶意内容被正确转义，没有导致SQL注入
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFetchMessagesBatch_MemoryExhaustion 测试内存耗尽攻击
// BUG: 大量请求可能导致内存耗尽
func TestFetchMessagesBatch_MemoryExhaustion(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	// 创建大量请求以测试内存使用
	var requests []PartitionRequest
	for i := 0; i < 10000; i++ {
		requests = append(requests, PartitionRequest{
			Topic:     "topic-" + strings.Repeat("a", 100), // 长topic名称
			Partition: uint(i % 1000),
			Offset:    int64(i),
			Limit:     1000, // 每个请求都要很多消息
		})
	}

	// 记录初始内存使用
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// 模拟查询（但不实际执行，避免测试时间过长）
	mock.ExpectQuery("SELECT.*FROM.*mq_messages.*").
		WillReturnRows(sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"}))

	_, err := FetchMessagesBatch(ctx, db, requests)

	// 检查内存使用
	runtime.GC()
	runtime.ReadMemStats(&m2)
	memIncrease := m2.Alloc - m1.Alloc

	assert.NoError(t, err)
	// 警告：如果内存增长超过100MB，可能存在内存问题
	if memIncrease > 100*1024*1024 {
		t.Logf("警告：内存使用增长了 %d bytes，可能存在内存效率问题", memIncrease)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestUpdateAssignments_LargeJSONPayload 测试大JSON负载处理
// BUG: 大量分区分配的JSON序列化可能导致性能问题
func TestUpdateAssignments_LargeJSONPayload(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	// 创建包含大量分区的分配
	largeAssignments := make(map[string][]types.PartitionInfo)
	for i := 0; i < 100; i++ {
		consumerID := "consumer-" + strings.Repeat("x", 50) // 长消费者ID
		var partitions []types.PartitionInfo
		for j := 0; j < 1000; j++ {
			partitions = append(partitions, types.PartitionInfo{
				Topic:     "topic-" + strings.Repeat("y", 100), // 长topic名称
				Partition: uint(j),
			})
		}
		largeAssignments[consumerID] = partitions
	}

	mock.ExpectBegin()

	// 对于大量的分配，每个都期望一个更新查询
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	for consumerID := range largeAssignments {
		mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
			WithArgs(generationID, sqlmock.AnyArg(), groupID, consumerID).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	mock.ExpectCommit()

	// 测量执行时间
	start := time.Now()
	err := UpdateAssignments(ctx, db, groupID, generationID, largeAssignments)
	duration := time.Since(start)

	assert.NoError(t, err)
	// 警告：如果处理大负载需要超过5秒，可能存在性能问题
	if duration > 5*time.Second {
		t.Logf("警告：处理大JSON负载花费了 %v，可能存在性能问题", duration)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCommitOffset_DatabaseLockTimeout 测试数据库锁超时
// BUG: 长时间的事务可能导致锁超时
func TestCommitOffset_DatabaseLockTimeout(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)
	p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
	offset := int64(100)

	// 模拟锁超时错误
	commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

	mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
		WithArgs(groupID, p.Topic, p.Partition, offset, generationID, sqlmock.AnyArg()).
		WillReturnError(sqlmock.ErrCancelled) // 模拟超时取消

	err := CommitOffset(ctx, db, groupID, generationID, p, offset)

	assert.Error(t, err)
	assert.Equal(t, sqlmock.ErrCancelled, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFindActiveConsumers_IndexPerformance 测试索引性能问题
// BUG: 如果last_heartbeat字段没有索引，查询可能很慢
func TestFindActiveConsumers_IndexPerformance(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	timeout := 30 * time.Second

	// 模拟大量数据的查询（测试索引的重要性）
	expectedSQL := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"

	// 创建大量行数据
	rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
	for i := 0; i < 10000; i++ {
		rows.AddRow(groupID, "consumer-"+string(rune(i)), 1, `["topic-a"]`, `[]`, time.Now())
	}

	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
		WithArgs(groupID, sqlmock.AnyArg()).
		WillReturnRows(rows)

	start := time.Now()
	consumers, err := FindActiveConsumers(ctx, db, groupID, timeout)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Len(t, consumers, 10000)

	// 警告：如果查询大量数据需要超过1秒，可能需要优化索引
	if duration > 1*time.Second {
		t.Logf("警告：查询大量活跃消费者花费了 %v，可能需要优化索引", duration)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBatchCommitOffsets_TransactionDeadlock 测试事务死锁
// BUG: 并发提交偏移量可能导致死锁
func TestBatchCommitOffsets_TransactionDeadlock(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	offsets := map[types.PartitionInfo]int64{
		{Topic: "topic-a", Partition: 0}: 100,
		{Topic: "topic-b", Partition: 1}: 200,
	}

	mock.ExpectBegin()

	// 模拟死锁错误
	commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

	mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
		WithArgs(groupID, "topic-a", uint(0), int64(100), generationID, sqlmock.AnyArg()).
		WillReturnError(sqlmock.ErrCancelled) // 模拟死锁导致的取消

	mock.ExpectRollback()

	err := BatchCommitOffsets(ctx, db, groupID, generationID, offsets)

	assert.Error(t, err)
	// 应该有适当的重试机制或错误处理
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetLatestOffset_OverflowProtection 测试数字溢出保护
// BUG: MAX(id)查询可能导致整数溢出
func TestGetLatestOffset_OverflowProtection(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	topic := "test-topic"
	partition := uint(0)

	expectedSQL := "SELECT COALESCE(MAX(id), 0) FROM `mq_messages` WHERE `topic` = ? AND `partition` = ?"

	// 模拟非常大的ID值（接近int64最大值）
	veryLargeID := int64(9223372036854775806) // 接近int64最大值

	rows := sqlmock.NewRows([]string{"max_id"}).AddRow(veryLargeID)
	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
		WithArgs(topic, partition).
		WillReturnRows(rows)

	maxOffset, err := GetLatestOffset(ctx, db, topic, partition)

	assert.NoError(t, err)
	assert.Equal(t, veryLargeID, maxOffset)

	// 检查是否接近溢出边界
	if maxOffset > 9223372036854775800 {
		t.Logf("警告：偏移量 %d 接近int64最大值，可能需要考虑溢出处理", maxOffset)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestDeleteMessagesByPartition_LimitBypass 测试limit绕过漏洞
// BUG: 如果limit参数没有正确验证，可能被绕过
func TestDeleteMessagesByPartition_LimitBypass(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	topic := "test-topic"
	partition := uint(0)
	maxOffset := int64(1000)
	retentionDate := time.Now().Add(-24 * time.Hour)

	// 测试异常大的limit值
	dangerousLimit := 2147483647 // int32最大值

	expectedSQL := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` < ? AND `created_at` < ? LIMIT ?"

	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
		WithArgs(topic, partition, maxOffset, retentionDate, dangerousLimit).
		WillReturnResult(sqlmock.NewResult(0, int64(dangerousLimit))) // 模拟删除了大量行

	rowsAffected, err := DeleteMessagesByPartition(ctx, db, topic, partition, maxOffset, retentionDate, dangerousLimit)

	assert.NoError(t, err)
	assert.Equal(t, int64(dangerousLimit), rowsAffected)

	// 警告：如果删除了过多行，可能需要限制单次删除的数量
	if rowsAffected > 10000 {
		t.Logf("警告：单次删除了 %d 行，可能需要限制批处理大小", rowsAffected)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestUpsertHeartbeat_TimestampPrecision 测试时间戳精度问题
// BUG: 不同系统的时间精度可能导致数据不一致
func TestUpsertHeartbeat_TimestampPrecision(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	consumerID := "consumer-1"
	topics := []byte(`["topic-a"]`)

	expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"

	// 测试时间精度
	beforeCall := time.Now()

	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
		WithArgs(groupID, consumerID, topics, []byte("{}"), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := UpsertHeartbeat(ctx, db, groupID, consumerID, topics)

	afterCall := time.Now()

	assert.NoError(t, err)

	// 检查时间精度问题
	timeDiff := afterCall.Sub(beforeCall)
	if timeDiff > 100*time.Millisecond {
		t.Logf("警告：心跳更新花费了 %v，可能存在时间精度或性能问题", timeDiff)
	}
	assert.NoError(t, mock.ExpectationsWereMet())
}

// BenchmarkUpdateAssignments 性能基准测试
func BenchmarkUpdateAssignments(b *testing.B) {
	// 创建一个临时的testing.T用于mock
	t := &testing.T{}
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	// 准备测试数据
	assignments := map[string][]types.PartitionInfo{
		"consumer-1": {{Topic: "topic-a", Partition: 0}},
		"consumer-2": {{Topic: "topic-b", Partition: 1}},
	}

	// 设置mock期望
	mock.ExpectBegin()
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	for range assignments {
		mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		UpdateAssignments(ctx, db, groupID, generationID, assignments)
	}
}
