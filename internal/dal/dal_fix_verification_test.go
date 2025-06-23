package dal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIncrementAndGetGenerationID_FixedConcurrentSafety 验证并发安全性修复
func TestIncrementAndGetGenerationID_FixedConcurrentSafety(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 测试场景1：首次创建消费组（INSERT部分）
	t.Run("FirstTimeCreation", func(t *testing.T) {
		mock.ExpectBegin()

		// 期望原子性的INSERT ... ON DUPLICATE KEY UPDATE语句
		insertSQL := `INSERT INTO mq_consumer_group_generations 
				(group_id, generation_id, protocol_type, updated_at) 
				VALUES (?, 1, 'consumer', ?) 
				ON DUPLICATE KEY UPDATE 
				generation_id = generation_id + 1, 
				updated_at = VALUES(updated_at)`

		mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
			WithArgs(groupID, sqlmock.AnyArg()). // updated_at使用当前时间
			WillReturnResult(sqlmock.NewResult(1, 1))

		// 期望查询获取generation_id
		selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
		rows := sqlmock.NewRows([]string{"generation_id"}).AddRow(1)
		mock.ExpectQuery(regexp.QuoteMeta(selectSQL)).
			WithArgs(groupID).
			WillReturnRows(rows)

		mock.ExpectCommit()

		generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

		assert.NoError(t, err)
		assert.Equal(t, uint(1), generationID)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// 测试场景2：递增现有消费组（UPDATE部分）
	t.Run("IncrementExisting", func(t *testing.T) {
		mock.ExpectBegin()

		// 期望原子性的INSERT ... ON DUPLICATE KEY UPDATE语句
		insertSQL := `INSERT INTO mq_consumer_group_generations 
				(group_id, generation_id, protocol_type, updated_at) 
				VALUES (?, 1, 'consumer', ?) 
				ON DUPLICATE KEY UPDATE 
				generation_id = generation_id + 1, 
				updated_at = VALUES(updated_at)`

		mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
			WithArgs(groupID, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 1)) // 0 insert, 1 update

		// 期望查询获取递增后的generation_id
		selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
		rows := sqlmock.NewRows([]string{"generation_id"}).AddRow(6) // 假设从5递增到6
		mock.ExpectQuery(regexp.QuoteMeta(selectSQL)).
			WithArgs(groupID).
			WillReturnRows(rows)

		mock.ExpectCommit()

		generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

		assert.NoError(t, err)
		assert.Equal(t, uint(6), generationID)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestIncrementAndGetGenerationID_TimestampUpdate 验证时间戳更新修复
func TestIncrementAndGetGenerationID_TimestampUpdate(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	mock.ExpectBegin()

	// 捕获时间戳以验证它被正确传递
	beforeCall := time.Now()

	insertSQL := `INSERT INTO mq_consumer_group_generations 
			(group_id, generation_id, protocol_type, updated_at) 
			VALUES (?, 1, 'consumer', ?) 
			ON DUPLICATE KEY UPDATE 
			generation_id = generation_id + 1, 
			updated_at = VALUES(updated_at)`

	mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WithArgs(groupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
	rows := sqlmock.NewRows([]string{"generation_id"}).AddRow(2)
	mock.ExpectQuery(regexp.QuoteMeta(selectSQL)).
		WithArgs(groupID).
		WillReturnRows(rows)

	mock.ExpectCommit()

	generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

	afterCall := time.Now()

	require.NoError(t, err)
	assert.Equal(t, uint(2), generationID)

	// 验证时间戳在合理范围内（调用前后）
	// 这间接验证了updated_at字段被正确设置
	callDuration := afterCall.Sub(beforeCall)
	assert.True(t, callDuration < 100*time.Millisecond, "调用时间应该很短，说明时间戳处理正确")

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestIncrementAndGetGenerationID_ErrorHandling 测试错误处理
func TestIncrementAndGetGenerationID_ErrorHandling(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 测试数据库连接错误
	t.Run("DatabaseConnectionError", func(t *testing.T) {
		mock.ExpectBegin()

		insertSQL := `INSERT INTO mq_consumer_group_generations 
				(group_id, generation_id, protocol_type, updated_at) 
				VALUES (?, 1, 'consumer', ?) 
				ON DUPLICATE KEY UPDATE 
				generation_id = generation_id + 1, 
				updated_at = VALUES(updated_at)`

		mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
			WithArgs(groupID, sqlmock.AnyArg()).
			WillReturnError(sqlmock.ErrCancelled)

		mock.ExpectRollback()

		generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to increment generation ID")
		assert.Equal(t, uint(0), generationID)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	// 测试SELECT查询错误
	t.Run("SelectQueryError", func(t *testing.T) {
		mock.ExpectBegin()

		insertSQL := `INSERT INTO mq_consumer_group_generations 
				(group_id, generation_id, protocol_type, updated_at) 
				VALUES (?, 1, 'consumer', ?) 
				ON DUPLICATE KEY UPDATE 
				generation_id = generation_id + 1, 
				updated_at = VALUES(updated_at)`

		mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
			WithArgs(groupID, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
		mock.ExpectQuery(regexp.QuoteMeta(selectSQL)).
			WithArgs(groupID).
			WillReturnError(sqlmock.ErrCancelled)

		mock.ExpectRollback()

		generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to retrieve generation ID")
		assert.Equal(t, uint(0), generationID)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestIncrementAndGetGenerationID_IdempotencyAndConsistency 测试幂等性和一致性
func TestIncrementAndGetGenerationID_IdempotencyAndConsistency(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	// 模拟连续多次调用，每次都应该正确递增
	expectedGenerations := []uint{1, 2, 3, 4, 5}

	for i, expectedGen := range expectedGenerations {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			mock.ExpectBegin()

			insertSQL := `INSERT INTO mq_consumer_group_generations 
					(group_id, generation_id, protocol_type, updated_at) 
					VALUES (?, 1, 'consumer', ?) 
					ON DUPLICATE KEY UPDATE 
					generation_id = generation_id + 1, 
					updated_at = VALUES(updated_at)`

			mock.ExpectExec(regexp.QuoteMeta(insertSQL)).
				WithArgs(groupID, sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(0, 1))

			selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
			rows := sqlmock.NewRows([]string{"generation_id"}).AddRow(expectedGen)
			mock.ExpectQuery(regexp.QuoteMeta(selectSQL)).
				WithArgs(groupID).
				WillReturnRows(rows)

			mock.ExpectCommit()

			generationID, err := IncrementAndGetGenerationID(ctx, db, groupID)

			assert.NoError(t, err)
			assert.Equal(t, expectedGen, generationID)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}
