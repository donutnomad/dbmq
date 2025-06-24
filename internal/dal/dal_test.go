package dal

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// newMockDB creates a new mock database connection for testing.
func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	require.NoError(t, err)

	return gormDB, mock
}

func TestUpsertHeartbeat(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	consumerID := "consumer-1"
	topics := []byte(`["topic-a"]`)

	expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"

	mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
		WithArgs(groupID, consumerID, topics, []byte("{}"), sqlmock.AnyArg()). // sqlmock.AnyArg for time.Now()
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := UpsertHeartbeat(ctx, db, groupID, consumerID, topics)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetConsumerGroupLowWatermarks(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	expectedSQL := "SELECT `topic`, `partition`, MIN(`committed_offset`) as low_watermark FROM `mq_consumer_group_offsets` GROUP BY `topic`, `partition`"

	rows := sqlmock.NewRows([]string{"topic", "partition", "low_watermark"}).
		AddRow("topic-a", 0, 123).
		AddRow("topic-b", 1, 456)

	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WillReturnRows(rows)

	watermarks, err := GetConsumerGroupLowWatermarks(ctx, db)
	require.NoError(t, err)
	require.NotNil(t, watermarks)

	expectedWatermarks := map[types.PartitionInfo]int64{
		{Topic: "topic-a", Partition: 0}: 123,
		{Topic: "topic-b", Partition: 1}: 456,
	}
	assert.Equal(t, expectedWatermarks, watermarks)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// Example of a failing test if the SQL syntax were wrong
func TestGetConsumerGroupLowWatermarks_Fail(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	// This is the correct, quoted SQL
	expectedSQL := "SELECT `topic`, `partition`, MIN(`committed_offset`) as low_watermark FROM `mq_consumer_group_offsets` GROUP BY `topic`, `partition`"

	// We simulate a DB error
	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).WillReturnError(sql.ErrConnDone)

	_, err := GetConsumerGroupLowWatermarks(ctx, db)
	assert.Error(t, err)
	assert.Equal(t, sql.ErrConnDone, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchMessagesBatch(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	// 准备测试数据
	requests := []PartitionRequest{
		{Topic: "topic-a", Partition: 0, Offset: 100, Limit: 10},
		{Topic: "topic-b", Partition: 1, Offset: 200, Limit: 5},
	}

	// 构建期望的SQL查询
	expectedSQL := "(SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?) UNION ALL (SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?) ORDER BY `id` ASC"

	// 模拟返回的数据
	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"}).
		AddRow(101, "topic-a", 0, nil, []byte("{}"), []byte("message1"), now).
		AddRow(102, "topic-a", 0, nil, []byte("{}"), []byte("message2"), now).
		AddRow(201, "topic-b", 1, nil, []byte("{}"), []byte("message3"), now)

	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
		WithArgs("topic-a", 0, int64(100), 10, "topic-b", 1, int64(200), 5).
		WillReturnRows(rows)

	// 执行测试
	messages, err := FetchMessagesBatch(ctx, db, requests)
	require.NoError(t, err)
	require.Len(t, messages, 3)

	// 验证结果
	assert.Equal(t, int64(101), messages[0].ID)
	assert.Equal(t, "topic-a", messages[0].Topic)
	assert.Equal(t, uint(0), messages[0].Partition)
	assert.Equal(t, []byte("message1"), messages[0].Body)

	assert.Equal(t, int64(102), messages[1].ID)
	assert.Equal(t, "topic-a", messages[1].Topic)

	assert.Equal(t, int64(201), messages[2].ID)
	assert.Equal(t, "topic-b", messages[2].Topic)
	assert.Equal(t, uint(1), messages[2].Partition)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchMessagesBatch_EmptyRequests(t *testing.T) {
	db, _ := newMockDB(t)
	ctx := context.Background()

	// 测试空请求列表
	messages, err := FetchMessagesBatch(ctx, db, []PartitionRequest{})
	require.NoError(t, err)
	assert.Len(t, messages, 0)
}

func TestFetchMessagesBatch_SingleRequest(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	// 准备单个请求
	requests := []PartitionRequest{
		{Topic: "topic-a", Partition: 0, Offset: 100, Limit: 10},
	}

	// 构建期望的SQL查询（单个请求）
	expectedSQL := "(SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?) ORDER BY `id` ASC"

	// 模拟返回的数据
	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"}).
		AddRow(101, "topic-a", 0, nil, []byte("{}"), []byte("message1"), now)

	mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
		WithArgs("topic-a", 0, int64(100), 10).
		WillReturnRows(rows)

	// 执行测试
	messages, err := FetchMessagesBatch(ctx, db, requests)
	require.NoError(t, err)
	require.Len(t, messages, 1)

	// 验证结果
	assert.Equal(t, int64(101), messages[0].ID)
	assert.Equal(t, "topic-a", messages[0].Topic)
	assert.Equal(t, uint(0), messages[0].Partition)

	assert.NoError(t, mock.ExpectationsWereMet())
}
