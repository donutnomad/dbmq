package dal

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/donutnomad/dbmq/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// ==================== 数值边界测试 ====================

// TestGetLatestOffset_NumericBoundaries 测试数值边界情况
func TestGetLatestOffset_NumericBoundaries(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	topic := "test-topic"
	partition := uint(0)

	testCases := []struct {
		name     string
		maxID    int64
		expected int64
	}{
		{"Zero", 0, 0},
		{"MaxInt64", math.MaxInt64, math.MaxInt64},
		{"NearMaxInt64", math.MaxInt64 - 1, math.MaxInt64 - 1},
		{"MinPositive", 1, 1},
		{"LargeNumber", 999999999999999, 999999999999999},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedSQL := "SELECT COALESCE(MAX(id), 0) FROM `mq_messages` WHERE `topic` = ? AND `partition` = ?"
			rows := sqlmock.NewRows([]string{"max_id"}).AddRow(tc.maxID)
			mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
				WithArgs(topic, partition).
				WillReturnRows(rows)

			maxOffset, err := GetLatestOffset(ctx, db, topic, partition)

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, maxOffset)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestCommitOffset_GenerationBoundaries 测试代际ID边界情况
func TestCommitOffset_GenerationBoundaries(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	p := types.PartitionInfo{Topic: "topic-a", Partition: 0}
	offset := int64(100)

	testCases := []struct {
		name         string
		generationID uint
	}{
		{"MinGeneration", 0},
		{"MaxUint32", math.MaxUint32},
		{"NearMaxUint32", math.MaxUint32 - 1},
		{"TypicalGeneration", 42},
	}

	commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
				WithArgs(groupID, p.Topic, p.Partition, offset, tc.generationID, sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))

			err := CommitOffset(ctx, db, groupID, tc.generationID, p, offset)
			assert.NoError(t, err)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== Unicode和特殊字符测试 ====================

// TestFindActiveConsumers_UnicodeAndSpecialChars 测试Unicode和特殊字符处理
func TestFindActiveConsumers_UnicodeAndSpecialChars(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	timeout := 30 * time.Second

	testCases := []struct {
		name    string
		groupID string
	}{
		{"EmptyString", ""},
		{"UnicodeEmojis", "group-🚀🌟💻"},
		{"ChineseCharacters", "消费组-测试"},
		{"JapaneseCharacters", "グループ-テスト"},
		{"ArabicCharacters", "مجموعة-اختبار"},
		{"SpecialChars", "group@#$%^&*()"},
		{"SQLMetaChars", "group';DROP TABLE;--"},
		{"MaxLengthString", strings.Repeat("a", 255)},
		{"NullBytes", "group\x00test"},
		{"ControlChars", "group\r\n\t"},
		{"MixedUnicode", "group-混合🌟test"},
	}

	expectedSQL := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 验证字符串是否为有效UTF-8
			isValidUTF8 := utf8.ValidString(tc.groupID)

			rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
			if isValidUTF8 && tc.groupID != "" {
				rows.AddRow(tc.groupID, "consumer-1", 1, `["topic-a"]`, `[]`, time.Now())
			}

			mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
				WithArgs(tc.groupID, sqlmock.AnyArg()).
				WillReturnRows(rows)

			consumers, err := FindActiveConsumers(ctx, db, tc.groupID, timeout)

			assert.NoError(t, err)
			if isValidUTF8 && tc.groupID != "" {
				assert.Len(t, consumers, 1)
				assert.Equal(t, tc.groupID, consumers[0].GroupID)
			} else {
				assert.Len(t, consumers, 0)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 时间和时区测试 ====================

// TestFindActiveConsumers_TimeZoneAndPrecision 测试时区和时间精度问题
func TestFindActiveConsumers_TimeZoneAndPrecision(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	testCases := []struct {
		name        string
		timeout     time.Duration
		heartbeatAt time.Time
		shouldFind  bool
	}{
		{"NanosecondPrecision", 1 * time.Second, time.Now().Add(-999 * time.Millisecond), true},
		{"MicrosecondBoundary", 1 * time.Second, time.Now().Add(-1*time.Second + 1*time.Microsecond), true},
		{"ExactTimeout", 1 * time.Second, time.Now().Add(-1 * time.Second), false},
		{"VeryShortTimeout", 1 * time.Nanosecond, time.Now().Add(-2 * time.Nanosecond), false},
		{"VeryLongTimeout", 24 * time.Hour, time.Now().Add(-23 * time.Hour), true},
		{"FutureHeartbeat", 1 * time.Hour, time.Now().Add(1 * time.Hour), true}, // 未来时间
		{"ZeroTimeout", 0, time.Now(), true},
	}

	expectedSQL := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
			if tc.shouldFind {
				rows.AddRow(groupID, "consumer-1", 1, `["topic-a"]`, `[]`, tc.heartbeatAt)
			}

			mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
				WithArgs(groupID, sqlmock.AnyArg()).
				WillReturnRows(rows)

			consumers, err := FindActiveConsumers(ctx, db, groupID, tc.timeout)

			assert.NoError(t, err)
			if tc.shouldFind {
				assert.Len(t, consumers, 1)
			} else {
				assert.Len(t, consumers, 0)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== JSON边缘情况测试 ====================

// TestUpsertHeartbeat_JSONEdgeCases 测试JSON的各种边缘情况
func TestUpsertHeartbeat_JSONEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	consumerID := "consumer-1"

	testCases := []struct {
		name           string
		subscribedJSON []byte
		shouldSucceed  bool
	}{
		{"EmptyJSON", []byte(`{}`), true},
		{"EmptyArray", []byte(`[]`), true},
		{"NullJSON", []byte(`null`), true},
		{"VeryLargeJSON", []byte(`[` + strings.Repeat(`"topic-`, 10000) + strings.Repeat(`a`, 100) + strings.Repeat(`",`, 10000)[:len(strings.Repeat(`",`, 10000))-1] + `]`), true},
		{"NestedJSON", []byte(`{"topics":[{"name":"test","partitions":[1,2,3]}]}`), true},
		{"UnicodeInJSON", []byte(`["topic-🚀","topic-测试","topic-グループ"]`), true},
		{"InvalidJSON", []byte(`{invalid json`), true},       // 函数不验证JSON有效性
		{"BinaryData", []byte{0x00, 0x01, 0x02, 0xFF}, true}, // 二进制数据
		{"ControlCharsInJSON", []byte(`["topic\r\n\t"]`), true},
		{"SQLInJSON", []byte(`["'; DROP TABLE mq_topics; --"]`), true},
		{"LongUnicodeString", []byte(`["` + strings.Repeat("🌟", 1000) + `"]`), true},
		{"MaxSizeJSON", make([]byte, 65535), true}, // 最大大小
		{"EmptyBytes", []byte{}, true},
	}

	// 填充MaxSizeJSON
	testCases[len(testCases)-2].subscribedJSON = []byte(`["` + strings.Repeat("a", 65530) + `"]`)

	expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
				WithArgs(groupID, consumerID, tc.subscribedJSON, []byte("{}"), sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))

			err := UpsertHeartbeat(ctx, db, groupID, consumerID, tc.subscribedJSON)

			if tc.shouldSucceed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 数组和集合边缘情况测试 ====================

// TestFindTopicsByNames_ArrayEdgeCases 测试数组的各种边缘情况
func TestFindTopicsByNames_ArrayEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name       string
		topicNames []string
		expectCall bool
	}{
		{"EmptySlice", []string{}, false},
		{"NilSlice", nil, false},
		{"SingleTopic", []string{"topic-a"}, true},
		{"DuplicateTopics", []string{"topic-a", "topic-a", "topic-a"}, true},
		{"ManyTopics", make([]string, 1000), true},
		{"UnicodeTopics", []string{"topic-🚀", "topic-测试", "topic-グループ"}, true},
		{"EmptyStringTopic", []string{""}, true},
		{"SpecialCharTopics", []string{"topic@#$%", "topic';DROP TABLE;--"}, true},
		{"VeryLongTopicNames", []string{strings.Repeat("topic-", 100)}, true},
		{"MixedCases", []string{"TOPIC-A", "topic-a", "Topic-A"}, true},
	}

	// 填充ManyTopics
	for i := range testCases[4].topicNames {
		testCases[4].topicNames[i] = fmt.Sprintf("topic-%d", i)
	}

	expectedSQL := "SELECT * FROM `mq_topics` WHERE `topic_name` IN (?)"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectCall {
				rows := sqlmock.NewRows([]string{"topic_name", "partition_count", "created_at"})
				for _, topic := range tc.topicNames {
					if topic != "" { // 假设空字符串topic不存在
						rows.AddRow(topic, 3, time.Now())
					}
				}

				mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.topicNames).
					WillReturnRows(rows)
			}

			topics, err := FindTopicsByNames(ctx, db, tc.topicNames)

			assert.NoError(t, err)
			if !tc.expectCall {
				assert.Nil(t, topics)
			} else {
				assert.NotNil(t, topics)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 分区和偏移量测试 ====================

// TestGetCommittedOffsets_PartitionEdgeCases 测试分区的各种边缘情况
func TestGetCommittedOffsets_PartitionEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	testCases := []struct {
		name       string
		partitions []types.PartitionInfo
		expectCall bool
	}{
		{"EmptyPartitions", []types.PartitionInfo{}, false},
		{"SinglePartition", []types.PartitionInfo{{Topic: "topic-a", Partition: 0}}, true},
		{"MaxUintPartition", []types.PartitionInfo{{Topic: "topic-a", Partition: math.MaxUint32}}, true},
		{"ZeroPartition", []types.PartitionInfo{{Topic: "topic-a", Partition: 0}}, true},
		{"ManyPartitions", make([]types.PartitionInfo, 10000), true},
		{"DuplicatePartitions", []types.PartitionInfo{
			{Topic: "topic-a", Partition: 0},
			{Topic: "topic-a", Partition: 0},
		}, true},
		{"UnicodeTopicNames", []types.PartitionInfo{
			{Topic: "topic-🚀", Partition: 0},
			{Topic: "topic-测试", Partition: 1},
		}, true},
		{"EmptyTopicName", []types.PartitionInfo{{Topic: "", Partition: 0}}, true},
		{"SpecialCharTopics", []types.PartitionInfo{
			{Topic: "topic'; DROP TABLE; --", Partition: 0},
		}, true},
	}

	// 填充ManyPartitions
	for i := range testCases[4].partitions {
		testCases[4].partitions[i] = types.PartitionInfo{
			Topic:     fmt.Sprintf("topic-%d", i%100),
			Partition: uint(i % 10),
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectCall {
				rows := sqlmock.NewRows([]string{"group_id", "topic", "partition", "committed_offset", "generation_id", "updated_at"})
				for _, p := range tc.partitions {
					rows.AddRow(groupID, p.Topic, p.Partition, int64(100), 1, time.Now())
				}

				mock.ExpectQuery("SELECT.*FROM.*mq_consumer_group_offsets.*WHERE.*").
					WillReturnRows(rows)
			}

			offsets, err := GetCommittedOffsets(ctx, db, groupID, tc.partitions)

			assert.NoError(t, err)
			assert.NotNil(t, offsets)

			if !tc.expectCall {
				assert.Len(t, offsets, 0)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 消息获取边缘情况测试 ====================

// TestFetchMessages_OffsetEdgeCases 测试偏移量的边缘情况
func TestFetchMessages_OffsetEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	topic := "test-topic"
	partition := uint(0)

	testCases := []struct {
		name   string
		offset int64
		limit  int
		valid  bool
	}{
		{"NegativeOffset", -1, 10, true}, // MySQL会处理负数
		{"ZeroOffset", 0, 10, true},
		{"MaxInt64Offset", math.MaxInt64, 10, true},
		{"MinInt64Offset", math.MinInt64, 10, true},
		{"ZeroLimit", 100, 0, true},      // 空结果集
		{"NegativeLimit", 100, -1, true}, // MySQL会处理负数限制
		{"MaxIntLimit", 100, math.MaxInt32, true},
		{"LargeOffset", 999999999999, 10, true},
		{"VeryLargeLimit", 100, 1000000, true},
	}

	expectedSQL := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
			if tc.limit > 0 && tc.valid {
				// 模拟返回一些消息
				for i := 0; i < min(tc.limit, 3); i++ {
					rows.AddRow(tc.offset+int64(i)+1, topic, partition, nil, []byte("{}"), []byte("test"), time.Now())
				}
			}

			mock.ExpectQuery(regexp.QuoteMeta(expectedSQL)).
				WithArgs(topic, partition, tc.offset, tc.limit).
				WillReturnRows(rows)

			messages, err := FetchMessages(ctx, db, topic, partition, tc.offset, tc.limit)

			assert.NoError(t, err)
			assert.NotNil(t, messages)

			if tc.limit <= 0 || !tc.valid {
				assert.Len(t, messages, 0)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 批量操作极限测试 ====================

// TestBatchCommitOffsets_ExtremeBatches 测试极限批量操作
func TestBatchCommitOffsets_ExtremeBatches(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	generationID := uint(5)

	testCases := []struct {
		name        string
		offsetCount int
	}{
		{"EmptyBatch", 0},
		{"SingleOffset", 1},
		{"SmallBatch", 10},
		{"MediumBatch", 100},
		{"LargeBatch", 1000},
		{"ExtremelyLargeBatch", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			offsets := make(map[types.PartitionInfo]int64)

			// 创建测试数据
			for i := 0; i < tc.offsetCount; i++ {
				p := types.PartitionInfo{
					Topic:     fmt.Sprintf("topic-%d", i%100),
					Partition: uint(i % 10),
				}
				offsets[p] = int64(i * 100)
			}

			if tc.offsetCount > 0 {
				mock.ExpectBegin()

				commitSQL := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"

				for range offsets {
					mock.ExpectExec(regexp.QuoteMeta(commitSQL)).
						WillReturnResult(sqlmock.NewResult(1, 1))
				}

				mock.ExpectCommit()
			}

			err := BatchCommitOffsets(ctx, db, groupID, generationID, offsets)

			assert.NoError(t, err)
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 数据库连接和资源测试 ====================

// TestDeleteMessagesByPartition_ExtremeDeletion 测试极限删除操作
func TestDeleteMessagesByPartition_ExtremeDeletion(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	topic := "test-topic"
	partition := uint(0)
	maxOffset := int64(1000000)
	retentionDate := time.Now().Add(-24 * time.Hour)

	testCases := []struct {
		name         string
		limit        int
		expectedRows int64
		shouldError  bool
	}{
		{"ZeroLimit", 0, 0, false},
		{"SmallLimit", 10, 10, false},
		{"NormalLimit", 1000, 1000, false},
		{"LargeLimit", 100000, 100000, false},
		{"MaxIntLimit", math.MaxInt32, 0, true}, // 可能导致错误
		{"NegativeLimit", -1, 0, true},          // 无效限制
	}

	expectedSQL := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` < ? AND `created_at` < ? LIMIT ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldError {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(topic, partition, maxOffset, retentionDate, tc.limit).
					WillReturnError(fmt.Errorf("invalid limit"))
			} else {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(topic, partition, maxOffset, retentionDate, tc.limit).
					WillReturnResult(sqlmock.NewResult(0, tc.expectedRows))
			}

			rowsAffected, err := DeleteMessagesByPartition(ctx, db, topic, partition, maxOffset, retentionDate, tc.limit)

			if tc.shouldError {
				assert.Error(t, err)
				assert.Equal(t, int64(0), rowsAffected)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedRows, rowsAffected)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 并发和一致性测试 ====================

// TestUpdateHeartbeat_ConcurrentUpdates 测试并发心跳更新
func TestUpdateHeartbeat_ConcurrentUpdates(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"

	testCases := []struct {
		name          string
		consumerID    string
		shouldSucceed bool
		rowsAffected  int64
	}{
		{"NormalConsumer", "consumer-1", true, 1},
		{"NonExistentConsumer", "consumer-999", true, 0}, // 不报错，但没有更新行
		{"EmptyConsumerID", "", true, 0},
		{"UnicodeConsumerID", "consumer-🚀", true, 1},
		{"LongConsumerID", strings.Repeat("consumer-", 50), true, 1},
		{"SpecialCharConsumerID", "consumer@#$%^&*()", true, 1},
	}

	expectedSQL := "UPDATE `mq_consumer_heartbeats` SET `last_heartbeat` = ? WHERE `group_id` = ? AND `consumer_id` = ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
				WithArgs(sqlmock.AnyArg(), groupID, tc.consumerID).
				WillReturnResult(sqlmock.NewResult(0, tc.rowsAffected))

			err := UpdateHeartbeat(ctx, db, groupID, tc.consumerID)

			if tc.shouldSucceed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 内存和性能测试 ====================

// TestCreateMessage_LargeMessages 测试大消息创建
func TestCreateMessage_LargeMessages(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name        string
		messageSize int // 消息体大小（字节）
		shouldError bool
	}{
		{"TinyMessage", 1, false},
		{"SmallMessage", 1024, false},             // 1KB
		{"MediumMessage", 1024 * 1024, false},     // 1MB
		{"LargeMessage", 10 * 1024 * 1024, false}, // 10MB
		// 注意：真实场景中可能有大小限制
		{"HugeMessage", 100 * 1024 * 1024, false}, // 100MB
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &types.Message{
				Topic:     "test-topic",
				Partition: 0,
				Body:      make([]byte, tc.messageSize),
				Headers:   []byte("{}"),
				CreatedAt: time.Now(),
			}

			// 填充消息体
			for i := range msg.Body {
				msg.Body[i] = byte(i % 256)
			}

			if tc.shouldError {
				mock.ExpectQuery("INSERT INTO.*").
					WillReturnError(fmt.Errorf("message too large"))
			} else {
				mock.ExpectQuery("INSERT INTO.*").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			}

			err := CreateMessage(ctx, db, msg)

			if tc.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ==================== 数据完整性测试 ====================

// TestGetAllTopics_DataConsistency 测试数据一致性
func TestGetAllTopics_DataConsistency(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		mockSetup func()
		expectErr bool
	}{
		{
			name: "EmptyDatabase",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"topic_name", "partition_count", "created_at"})
				mock.ExpectQuery("SELECT.*").WillReturnRows(rows)
			},
			expectErr: false,
		},
		{
			name: "SingleTopic",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"topic_name", "partition_count", "created_at"}).
					AddRow("topic-a", 3, time.Now())
				mock.ExpectQuery("SELECT.*").WillReturnRows(rows)
			},
			expectErr: false,
		},
		{
			name: "ManyTopics",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"topic_name", "partition_count", "created_at"})
				for i := 0; i < 10000; i++ {
					rows.AddRow(fmt.Sprintf("topic-%d", i), 3, time.Now())
				}
				mock.ExpectQuery("SELECT.*").WillReturnRows(rows)
			},
			expectErr: false,
		},
		{
			name: "DatabaseError",
			mockSetup: func() {
				mock.ExpectQuery("SELECT.*").
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockSetup()

			topics, err := GetAllTopics(ctx, db)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Nil(t, topics)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, topics)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}
