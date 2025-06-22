package dal

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"dbmq/pkg/dbmq/types"

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

	expectedSQL := "INSERT INTO mq_consumer_heartbeats (group_id, consumer_id, generation_id, subscribed_topics, assigned_partitions, last_heartbeat) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE subscribed_topics = VALUES(subscribed_topics), last_heartbeat = VALUES(last_heartbeat)"

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
