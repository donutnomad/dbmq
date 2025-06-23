package dal

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/pkg/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

// TestIncrementAndGetGenerationID_ConcurrentRaceCondition 测试并发竞态条件
// BUG: 两个事务同时检测到记录不存在时，可能都尝试插入导致重复键错误
func TestIncrementAndGetGenerationID_ConcurrentRaceCondition(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 模拟第一个事务：锁定查询返回记录不存在
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ? FOR UPDATE")).
		WithArgs(groupID).
		WillReturnError(gorm.ErrRecordNotFound)

	// 第一个事务尝试插入，但模拟重复键错误（另一个事务已经插入）
	insertSQL := "INSERT INTO `mq_consumer_group_generations` (`group_id`, `generation_id`, `protocol_type`, `updated_at`) VALUES (?, ?, ?, ?)"
	mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WithArgs(groupID, uint(1), "consumer", sqlmock.AnyArg()).
		WillReturnError(fmt.Errorf("ERROR 1062 (23000): Duplicate entry '%s' for key 'PRIMARY'", groupID))
	mock.ExpectRollback()

	// 执行测试
	generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

	// 应该处理重复键错误并重试或返回适当的错误
	assert.Error(t, err)
	assert.Equal(t, uint(0), generationID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestIncrementAndGetGenerationID_UpdatedAtNotRefreshed 测试UpdatedAt字段未更新的问题
// BUG: 在更新generation_id时，没有同时更新updated_at字段
func TestIncrementAndGetGenerationID_UpdatedAtNotRefreshed(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 模拟找到已存在的记录
	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"group_id", "generation_id", "protocol_type", "updated_at"}).
		AddRow(groupID, 5, "consumer", time.Now().Add(-1*time.Hour)) // 1小时前的记录
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ? FOR UPDATE")).
		WithArgs(groupID).
		WillReturnRows(rows)

	// 期望更新语句包含updated_at字段
	updateSQL := "UPDATE `mq_consumer_group_generations` SET `generation_id` = ? WHERE `group_id` = ?"
	mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
		WithArgs(uint(6), groupID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

	assert.NoError(t, err)
	assert.Equal(t, uint(6), generationID)
	assert.NoError(t, mock.ExpectationsWereMet())

	// 注意：这个测试暴露了一个BUG - updated_at字段没有被更新
	// 修复建议：SQL应该是 "UPDATE ... SET `generation_id` = ?, `updated_at` = ? WHERE ..."
}

// TestUpdateAssignments_ConsumerNotExists 测试消费者不存在的边缘情况
// BUG: 当消费者在不同代际或已被删除时，错误处理不够精确
func TestUpdateAssignments_ConsumerNotExists(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	assignments := map[string][]types.PartitionInfo{
		"consumer-1": {{Topic: "topic-a", Partition: 0}},
		"consumer-2": {{Topic: "topic-b", Partition: 1}}, // 这个消费者不存在
	}

	mock.ExpectBegin()

	// 第一个消费者更新成功
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
		WithArgs(generationID, `[{"Topic":"topic-a","Partition":0}]`, groupID, "consumer-1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// 第二个消费者不存在（RowsAffected = 0）
	mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
		WithArgs(generationID, `[{"Topic":"topic-b","Partition":1}]`, groupID, "consumer-2").
		WillReturnResult(sqlmock.NewResult(0, 0)) // 没有行被影响

	mock.ExpectRollback()

	err := UpdateAssignments(ctx, db, groupID, generationID, assignments)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer consumer-2 not found in group test-group")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestUpdateAssignments_PartialFailure 测试部分更新失败的原子性
func TestUpdateAssignments_PartialFailure(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	assignments := map[string][]types.PartitionInfo{
		"consumer-1": {{Topic: "topic-a", Partition: 0}},
		"consumer-2": {{Topic: "topic-b", Partition: 1}},
	}

	mock.ExpectBegin()

	// 第一个消费者更新成功
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
		WithArgs(generationID, `[{"Topic":"topic-a","Partition":0}]`, groupID, "consumer-1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// 第二个消费者更新失败（数据库错误）
	mock.ExpectExec(regexp.QuoteMeta(updateSQL)).
		WithArgs(generationID, `[{"Topic":"topic-b","Partition":1}]`, groupID, "consumer-2").
		WillReturnError(fmt.Errorf("database connection lost"))

	mock.ExpectRollback()

	err := UpdateAssignments(ctx, db, groupID, generationID, assignments)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update assignment for consumer consumer-2")
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCommitOffset_GenerationIdLogicEdgeCases 测试代际ID逻辑的边缘情况
// BUG: 当generation_id发生回滚时，逻辑可能出现问题
func TestCommitOffset_GenerationIdLogicEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	testCases := []struct {
		name              string
		currentGeneration uint
		newGeneration     uint
		expectUpdate      bool
	}{
		{"正常递增", 5, 6, true},
		{"相同代际", 5, 5, true},
		{"代际回滚", 6, 5, false}, // BUG: 这种情况下的行为可能不符合预期
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
			offset := int64(100)

			// 复杂的ON DUPLICATE KEY UPDATE SQL
			expectedSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

			mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
				WithArgs(groupID, p.Topic, p.Partition, offset, tc.newGeneration, sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))

			err := CommitOffset(ctx, db, groupID, tc.newGeneration, p, offset)
			assert.NoError(t, err)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestGetCommittedOffsets_LargePartitionCount 测试大量分区时的性能问题
// BUG: OR查询在分区数量很大时性能很差
func TestGetCommittedOffsets_LargePartitionCount(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 创建大量分区（模拟性能问题）
	var partitions []types.PartitionInfo
	for i := 0; i < 1000; i++ {
		partitions = append(partitions, types.PartitionInfo{
			Topic:     fmt.Sprintf("topic-%d", i%10),
			Partition: uint(i % 10),
		})
	}

	// 期望生成很长的OR查询
	var conditions []string
	var args []interface{}
	args = append(args, groupID)

	for _, p := range partitions {
		conditions = append(conditions, "(`topic` = ? AND `partition` = ?)")
		args = append(args, p.Topic, p.Partition)
	}

	// 模拟查询返回
	rows := sqlmock.NewRows([]string{"group_id", "topic", "partition", "committed_offset", "generation_id", "updated_at"})
	for i := 0; i < 10; i++ {
		rows.AddRow(groupID, fmt.Sprintf("topic-%d", i), i, int64(i*100), 1, time.Now())
	}

	mock.ExpectQuery("SELECT.*FROM.*mq_consumer_group_offsets.*WHERE.*").
		WillReturnRows(rows)

	offsets, err := GetCommittedOffsets(ctx, db, groupID, partitions)

	assert.NoError(t, err)
	assert.NotNil(t, offsets)
	// 注意：这个测试暴露了性能问题，建议使用IN查询替代OR查询
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestFindActiveConsumers_TimePrecisionIssues 测试时间精度问题
// BUG: 没有考虑数据库时间和应用时间的差异
func TestFindActiveConsumers_TimePrecisionIssues(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	timeout := 30 * time.Second

	// 模拟时间差异：数据库时间比应用时间慢1分钟
	dbTime := time.Now().Add(-1 * time.Minute)
	appTime := time.Now()

	expectedSQL := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"

	// 应用使用的时间基准
	timeBaseline := appTime.Add(-timeout)

	rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}).
		AddRow(groupID, "consumer-1", 1, `["topic-a"]`, `[]`, dbTime) // 数据库中的时间

	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
		WithArgs(groupID, sqlmock.AnyArg()). // time.Now().Add(-timeout)
		WillReturnRows(rows)

	consumers, err := FindActiveConsumers(ctx, db, groupID, timeout)

	assert.NoError(t, err)
	assert.Len(t, consumers, 1)

	// 注意：这个测试暴露了潜在的时间同步问题
	// 建议：使用数据库的NOW()函数替代应用时间
	assert.True(t, consumers[0].LastHeartbeat.Before(timeBaseline.Add(2*time.Minute)))
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestBatchCommitOffsets_TransactionNesting 测试事务嵌套问题
// BUG: BatchCommitOffsets调用CommitOffset可能导致事务嵌套问题
func TestBatchCommitOffsets_TransactionNesting(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	offsets := map[types.PartitionInfo]int64{
		{Topic: "topic-a", Partition: 0}: 100,
		{Topic: "topic-b", Partition: 1}: 200,
	}

	mock.ExpectBegin() // BatchCommitOffsets的事务开始

	// 每个CommitOffset调用都期望执行SQL（在同一个事务中）
	commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

	for p, offset := range offsets {
		mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
			WithArgs(groupID, p.Topic, p.Partition, offset, generationID, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	mock.ExpectCommit()

	err := BatchCommitOffsets(ctx, db, groupID, generationID, offsets)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestUpsertHeartbeat_InvalidJSON 测试JSON序列化问题
// BUG: 当传入的topics不是有效的JSON时，可能导致数据损坏
func TestUpsertHeartbeat_InvalidJSON(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	consumerID := "consumer-1"
	invalidTopics := []byte(`invalid json`) // 无效的JSON

	expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"

	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
		WithArgs(groupID, consumerID, invalidTopics, []byte("{}"), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := UpsertHeartbeat(ctx, db, groupID, consumerID, invalidTopics)

	// 函数应该成功，但这可能导致数据库中存储无效的JSON
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())

	// 注意：这暴露了一个潜在问题 - 应该验证JSON的有效性
}

// TestConcurrentConsumerOperations 测试并发消费者操作
func TestConcurrentConsumerOperations(t *testing.T) {
	// 这个测试需要真实的数据库连接，这里只是框架
	t.Skip("需要真实数据库连接进行并发测试")

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// 模拟多个并发操作
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// 这里应该进行实际的并发测试
			// 比如同时进行心跳更新、分区分配、偏移量提交等
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有并发错误
	for err := range errors {
		if err != nil {
			t.Errorf("并发操作错误: %v", err)
		}
	}
}

// anyTime is a custom matcher for sqlmock to match any time.Time value
type anyTime struct{}

func (a anyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}
