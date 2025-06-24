package dal

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// ==================== GetConsumerGroupGeneration 函数测试 ====================

// TestGetConsumerGroupGeneration_AllScenarios 测试获取消费组代际的所有场景
func TestGetConsumerGroupGeneration_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name         string
		groupID      string
		mockSetup    func()
		expectResult bool
		expectError  bool
	}{
		{
			name:    "ExistingGroup",
			groupID: "existing-group",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "generation_id", "protocol_type", "updated_at"}).
					AddRow("existing-group", 5, "consumer", time.Now())
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
					WithArgs("existing-group").
					WillReturnRows(rows)
			},
			expectResult: true,
			expectError:  false,
		},
		{
			name:    "NonExistentGroup",
			groupID: "non-existent-group",
			mockSetup: func() {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
					WithArgs("non-existent-group").
					WillReturnError(sqlmock.ErrCancelled) // 模拟记录不存在
			},
			expectResult: false,
			expectError:  true,
		},
		{
			name:    "EmptyGroupID",
			groupID: "",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "generation_id", "protocol_type", "updated_at"})
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
					WithArgs("").
					WillReturnRows(rows)
			},
			expectResult: false,
			expectError:  false,
		},
		{
			name:    "UnicodeGroupID",
			groupID: "group-🚀测试",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "generation_id", "protocol_type", "updated_at"}).
					AddRow("group-🚀测试", 1, "consumer", time.Now())
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
					WithArgs("group-🚀测试").
					WillReturnRows(rows)
			},
			expectResult: true,
			expectError:  false,
		},
		{
			name:    "DatabaseError",
			groupID: "error-group",
			mockSetup: func() {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?")).
					WithArgs("error-group").
					WillReturnError(fmt.Errorf("database connection lost"))
			},
			expectResult: false,
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockSetup()

			gen, err := GetConsumerGroupGeneration(ctx, db, tc.groupID)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, gen)
			} else {
				assert.NoError(t, err)
				if tc.expectResult {
					assert.NotNil(t, gen)
					assert.Equal(t, tc.groupID, gen.GroupID)
				} else {
					assert.Nil(t, gen)
				}
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== GetHeartbeat 函数测试 ====================

// TestGetHeartbeat_AllScenarios 测试获取心跳的所有场景
func TestGetHeartbeat_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name        string
		groupID     string
		consumerID  string
		mockSetup   func()
		expectFound bool
		expectError bool
	}{
		{
			name:       "ExistingConsumer",
			groupID:    "test-group",
			consumerID: "consumer-1",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}).
					AddRow("test-group", "consumer-1", 1, `["topic-a"]`, `[{"Topic":"topic-a","Partition":0}]`, time.Now())
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
					WithArgs("test-group", "consumer-1").
					WillReturnRows(rows)
			},
			expectFound: true,
			expectError: false,
		},
		{
			name:       "NonExistentConsumer",
			groupID:    "test-group",
			consumerID: "non-existent",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
					WithArgs("test-group", "non-existent").
					WillReturnRows(rows)
			},
			expectFound: false,
			expectError: true, // GORM会返回ErrRecordNotFound
		},
		{
			name:       "EmptyIDs",
			groupID:    "",
			consumerID: "",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
					WithArgs("", "").
					WillReturnRows(rows)
			},
			expectFound: false,
			expectError: true,
		},
		{
			name:       "UnicodeIDs",
			groupID:    "group-🚀",
			consumerID: "consumer-测试",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}).
					AddRow("group-🚀", "consumer-测试", 1, `["topic-a"]`, `[]`, time.Now())
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
					WithArgs("group-🚀", "consumer-测试").
					WillReturnRows(rows)
			},
			expectFound: true,
			expectError: false,
		},
		{
			name:       "DatabaseError",
			groupID:    "test-group",
			consumerID: "consumer-1",
			mockSetup: func() {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
					WithArgs("test-group", "consumer-1").
					WillReturnError(fmt.Errorf("database timeout"))
			},
			expectFound: false,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockSetup()

			hb, err := GetHeartbeat(ctx, db, tc.groupID, tc.consumerID)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, hb)
			} else {
				assert.NoError(t, err)
				if tc.expectFound {
					assert.NotNil(t, hb)
					assert.Equal(t, tc.groupID, hb.GroupID)
					assert.Equal(t, tc.consumerID, hb.ConsumerID)
				} else {
					assert.Nil(t, hb)
				}
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== GetConsumerAssignment 函数测试 ====================

// TestGetConsumerAssignment_AliasFunction 测试GetConsumerAssignment别名函数
func TestGetConsumerAssignment_AliasFunction(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()
	groupID := "test-group"
	consumerID := "consumer-1"

	// GetConsumerAssignment是GetHeartbeat的别名，应该行为一致
	rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"}).
		AddRow(groupID, consumerID, 1, `["topic-a"]`, `[{"Topic":"topic-a","Partition":0}]`, time.Now())

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?")).
		WithArgs(groupID, consumerID).
		WillReturnRows(rows)

	assignment, err := GetConsumerAssignment(ctx, db, groupID, consumerID)

	assert.NoError(t, err)
	assert.NotNil(t, assignment)
	assert.Equal(t, groupID, assignment.GroupID)
	assert.Equal(t, consumerID, assignment.ConsumerID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== RegisterConsumer 函数测试 ====================

// TestRegisterConsumer_AllScenarios 测试注册消费者的所有场景
func TestRegisterConsumer_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		heartbeat *types.ConsumerHeartbeat
		expectErr bool
	}{
		{
			name: "NewConsumer",
			heartbeat: &types.ConsumerHeartbeat{
				GroupID:            "test-group",
				ConsumerID:         "consumer-1",
				GenerationID:       0,
				SubscribedTopics:   []byte(`["topic-a"]`),
				AssignedPartitions: []byte(`[]`),
				LastHeartbeat:      time.Now(),
			},
			expectErr: false,
		},
		{
			name: "ExistingConsumer",
			heartbeat: &types.ConsumerHeartbeat{
				GroupID:            "test-group",
				ConsumerID:         "consumer-1",
				GenerationID:       1,
				SubscribedTopics:   []byte(`["topic-a","topic-b"]`),
				AssignedPartitions: []byte(`[{"Topic":"topic-a","Partition":0}]`),
				LastHeartbeat:      time.Now(),
			},
			expectErr: false,
		},
		{
			name: "UnicodeConsumer",
			heartbeat: &types.ConsumerHeartbeat{
				GroupID:            "group-🚀",
				ConsumerID:         "consumer-测试",
				GenerationID:       1,
				SubscribedTopics:   []byte(`["topic-🌟"]`),
				AssignedPartitions: []byte(`[]`),
				LastHeartbeat:      time.Now(),
			},
			expectErr: false,
		},
		{
			name: "EmptyTopics",
			heartbeat: &types.ConsumerHeartbeat{
				GroupID:            "test-group",
				ConsumerID:         "consumer-empty",
				GenerationID:       1,
				SubscribedTopics:   []byte(`[]`),
				AssignedPartitions: []byte(`[]`),
				LastHeartbeat:      time.Now(),
			},
			expectErr: false,
		},
		{
			name: "LargeJSON",
			heartbeat: &types.ConsumerHeartbeat{
				GroupID:            "test-group",
				ConsumerID:         "consumer-large",
				GenerationID:       1,
				SubscribedTopics:   []byte(`[` + strings.Repeat(`"topic-very-long-name-with-many-characters",`, 1000)[:len(strings.Repeat(`"topic-very-long-name-with-many-characters",`, 1000))-1] + `]`),
				AssignedPartitions: []byte(`[]`),
				LastHeartbeat:      time.Now(),
			},
			expectErr: false,
		},
	}

	expectedSQL := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `subscribed_topics` = VALUES(`subscribed_topics`), `generation_id` = VALUES(`generation_id`), `last_heartbeat` = VALUES(`last_heartbeat`)"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectErr {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WillReturnError(fmt.Errorf("database error"))
			} else {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.heartbeat.GroupID, tc.heartbeat.ConsumerID, tc.heartbeat.GenerationID, tc.heartbeat.SubscribedTopics, tc.heartbeat.AssignedPartitions, tc.heartbeat.LastHeartbeat).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := RegisterConsumer(ctx, db, tc.heartbeat)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== DeleteHeartbeat 函数测试 ====================

// TestDeleteHeartbeat_AllScenarios 测试删除心跳的所有场景
func TestDeleteHeartbeat_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name         string
		groupID      string
		consumerID   string
		rowsAffected int64
		expectErr    bool
	}{
		{
			name:         "ExistingConsumer",
			groupID:      "test-group",
			consumerID:   "consumer-1",
			rowsAffected: 1,
			expectErr:    false,
		},
		{
			name:         "NonExistentConsumer",
			groupID:      "test-group",
			consumerID:   "non-existent",
			rowsAffected: 0,
			expectErr:    false, // 不报错，但没有删除任何行
		},
		{
			name:         "EmptyIDs",
			groupID:      "",
			consumerID:   "",
			rowsAffected: 0,
			expectErr:    false,
		},
		{
			name:         "UnicodeIDs",
			groupID:      "group-🚀",
			consumerID:   "consumer-测试",
			rowsAffected: 1,
			expectErr:    false,
		},
		{
			name:         "DatabaseError",
			groupID:      "test-group",
			consumerID:   "consumer-1",
			rowsAffected: 0,
			expectErr:    true,
		},
	}

	expectedSQL := "DELETE FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectErr {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.groupID, tc.consumerID).
					WillReturnError(fmt.Errorf("database error"))
			} else {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.groupID, tc.consumerID).
					WillReturnResult(sqlmock.NewResult(0, tc.rowsAffected))
			}

			err := DeleteHeartbeat(ctx, db, tc.groupID, tc.consumerID)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== FindAllActiveGroups 函数测试 ====================

// TestFindAllActiveGroups_AllScenarios 测试查找所有活跃组的场景
func TestFindAllActiveGroups_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name           string
		timeout        time.Duration
		mockSetup      func()
		expectedGroups []string
		expectErr      bool
	}{
		{
			name:    "NoActiveGroups",
			timeout: 30 * time.Second,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id"})
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			expectedGroups: []string{},
			expectErr:      false,
		},
		{
			name:    "SingleActiveGroup",
			timeout: 30 * time.Second,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id"}).
					AddRow("group-1")
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			expectedGroups: []string{"group-1"},
			expectErr:      false,
		},
		{
			name:    "MultipleActiveGroups",
			timeout: 30 * time.Second,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id"}).
					AddRow("group-1").
					AddRow("group-2").
					AddRow("group-🚀").
					AddRow("group-测试")
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			expectedGroups: []string{"group-1", "group-2", "group-🚀", "group-测试"},
			expectErr:      false,
		},
		{
			name:    "ZeroTimeout",
			timeout: 0,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id"}).
					AddRow("group-1").
					AddRow("group-2")
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			expectedGroups: []string{"group-1", "group-2"},
			expectErr:      false,
		},
		{
			name:    "VeryLargeTimeout",
			timeout: 365 * 24 * time.Hour, // 1年
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"group_id"}).
					AddRow("ancient-group")
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			expectedGroups: []string{"ancient-group"},
			expectErr:      false,
		},
		{
			name:    "DatabaseError",
			timeout: 30 * time.Second,
			mockSetup: func() {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?")).
					WithArgs(sqlmock.AnyArg()).
					WillReturnError(fmt.Errorf("database connection failed"))
			},
			expectedGroups: nil,
			expectErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockSetup()

			groups, err := FindAllActiveGroups(ctx, db, tc.timeout)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Nil(t, groups)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedGroups, groups)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== DeleteMessagesByPartitionUnconsumed 函数测试 ====================

// TestDeleteMessagesByPartitionUnconsumed_AllScenarios 测试删除未消费消息的场景
func TestDeleteMessagesByPartitionUnconsumed_AllScenarios(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name          string
		topic         string
		partition     uint
		retentionDate time.Time
		limit         int
		rowsAffected  int64
		expectErr     bool
	}{
		{
			name:          "NormalDeletion",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000,
			rowsAffected:  100,
			expectErr:     false,
		},
		{
			name:          "NoDeletion",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000,
			rowsAffected:  0,
			expectErr:     false,
		},
		{
			name:          "ZeroLimit",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         0,
			rowsAffected:  0,
			expectErr:     false,
		},
		{
			name:          "LargeLimit",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000000,
			rowsAffected:  500000,
			expectErr:     false,
		},
		{
			name:          "UnicodeTopic",
			topic:         "topic-🚀测试",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000,
			rowsAffected:  50,
			expectErr:     false,
		},
		{
			name:          "MaxUintPartition",
			topic:         "test-topic",
			partition:     4294967295, // MaxUint32
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000,
			rowsAffected:  0,
			expectErr:     false,
		},
		{
			name:          "FutureRetentionDate",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(24 * time.Hour), // 未来时间
			limit:         1000,
			rowsAffected:  1000, // 可能删除所有消息
			expectErr:     false,
		},
		{
			name:          "DatabaseError",
			topic:         "test-topic",
			partition:     0,
			retentionDate: time.Now().Add(-24 * time.Hour),
			limit:         1000,
			rowsAffected:  0,
			expectErr:     true,
		},
	}

	expectedSQL := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `created_at` < ? LIMIT ?"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectErr {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.topic, tc.partition, tc.retentionDate, tc.limit).
					WillReturnError(fmt.Errorf("database error"))
			} else {
				mock.ExpectExec(regexp.QuoteMeta(expectedSQL)).
					WithArgs(tc.topic, tc.partition, tc.retentionDate, tc.limit).
					WillReturnResult(sqlmock.NewResult(0, tc.rowsAffected))
			}

			rowsAffected, err := DeleteMessagesByPartitionUnconsumed(ctx, db, tc.topic, tc.partition, tc.retentionDate, tc.limit)

			if tc.expectErr {
				assert.Error(t, err)
				assert.Equal(t, int64(0), rowsAffected)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.rowsAffected, rowsAffected)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== FetchMessagesBatch 极端边缘情况 ====================

// TestFetchMessagesBatch_ExtremeEdgeCases 测试批量获取消息的极端边缘情况
func TestFetchMessagesBatch_ExtremeEdgeCases(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		requests  []PartitionRequest
		expectErr bool
	}{
		{
			name:      "NilRequests",
			requests:  nil,
			expectErr: false,
		},
		{
			name: "SingleRequestWithExtremeValues",
			requests: []PartitionRequest{
				{
					Topic:     strings.Repeat("topic-", 100), // 很长的topic名
					Partition: 4294967295,                    // MaxUint32
					Offset:    9223372036854775807,           // MaxInt64
					Limit:     2147483647,                    // MaxInt32
				},
			},
			expectErr: false,
		},
		{
			name: "MultipleRequestsWithUnicode",
			requests: []PartitionRequest{
				{Topic: "topic-🚀", Partition: 0, Offset: 100, Limit: 10},
				{Topic: "topic-测试", Partition: 1, Offset: 200, Limit: 20},
				{Topic: "topic-グループ", Partition: 2, Offset: 300, Limit: 30},
			},
			expectErr: false,
		},
		{
			name: "RequestsWithZeroAndNegativeValues",
			requests: []PartitionRequest{
				{Topic: "topic-a", Partition: 0, Offset: 0, Limit: 0},
				{Topic: "topic-b", Partition: 0, Offset: -1, Limit: -1},
				{Topic: "topic-c", Partition: 0, Offset: -9223372036854775808, Limit: 10}, // MinInt64
			},
			expectErr: false,
		},
		{
			name: "VeryManyRequests",
			requests: func() []PartitionRequest {
				requests := make([]PartitionRequest, 10000)
				for i := range requests {
					requests[i] = PartitionRequest{
						Topic:     fmt.Sprintf("topic-%d", i%100),
						Partition: uint(i % 10),
						Offset:    int64(i),
						Limit:     10,
					}
				}
				return requests
			}(),
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.requests) == 0 {
				// 空请求，不期望任何数据库调用
			} else {
				// 模拟返回空结果
				rows := sqlmock.NewRows([]string{"id", "topic", "partition", "message_key", "headers", "body", "created_at"})
				mock.ExpectQuery("SELECT.*FROM.*mq_messages.*UNION ALL.*ORDER BY.*").
					WillReturnRows(rows)
			}

			messages, err := FetchMessagesBatch(ctx, db, tc.requests)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, messages)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 数据库连接池和资源管理测试 ====================

// TestCreateMessage_ResourceManagement 测试创建消息的资源管理
func TestCreateMessage_ResourceManagement(t *testing.T) {
	db, mock := newMockDB(t)
	ctx := context.Background()

	testCases := []struct {
		name      string
		message   *types.Message
		expectErr bool
	}{
		{
			name: "NormalMessage",
			message: &types.Message{
				Topic:     "test-topic",
				Partition: 0,
				Body:      []byte("test message"),
				Headers:   []byte(`{"key":"value"}`),
				CreatedAt: time.Now(),
			},
			expectErr: false,
		},
		{
			name: "MessageWithNullBody",
			message: &types.Message{
				Topic:     "test-topic",
				Partition: 0,
				Body:      nil,
				Headers:   []byte(`{}`),
				CreatedAt: time.Now(),
			},
			expectErr: false,
		},
		{
			name: "MessageWithEmptyBody",
			message: &types.Message{
				Topic:     "test-topic",
				Partition: 0,
				Body:      []byte{},
				Headers:   []byte(`{}`),
				CreatedAt: time.Now(),
			},
			expectErr: false,
		},
		{
			name: "MessageWithUnicodeContent",
			message: &types.Message{
				Topic:     "topic-🚀",
				Partition: 0,
				Body:      []byte("测试消息 🌟 グループテスト"),
				Headers:   []byte(`{"author":"用户🚀","type":"测试"}`),
				CreatedAt: time.Now(),
			},
			expectErr: false,
		},
		{
			name: "MessageWithBinaryContent",
			message: &types.Message{
				Topic:     "binary-topic",
				Partition: 0,
				Body:      []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
				Headers:   []byte(`{}`),
				CreatedAt: time.Now(),
			},
			expectErr: false,
		},
		{
			name:      "NilMessage",
			message:   nil,
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectErr {
				if tc.message == nil {
					// 对于nil消息，直接测试
					err := CreateMessage(ctx, db, tc.message)
					assert.Error(t, err)
					return
				}
				mock.ExpectQuery("INSERT INTO.*").
					WillReturnError(fmt.Errorf("database error"))
			} else {
				mock.ExpectQuery("INSERT INTO.*").
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			}

			err := CreateMessage(ctx, db, tc.message)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}

// ==================== 上下文取消和超时测试 ====================

// TestContextCancellationAndTimeout 测试上下文取消和超时
func TestContextCancellationAndTimeout(t *testing.T) {
	db, mock := newMockDB(t)

	testCases := []struct {
		name        string
		setupCtx    func() (context.Context, context.CancelFunc)
		operation   func(ctx context.Context) error
		expectError bool
	}{
		{
			name: "ContextWithTimeout",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Millisecond)
			},
			operation: func(ctx context.Context) error {
				// 模拟长时间运行的操作
				time.Sleep(10 * time.Millisecond)
				_, err := FindActiveConsumers(ctx, db, "test-group", 30*time.Second)
				return err
			},
			expectError: true,
		},
		{
			name: "ContextCancellation",
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			operation: func(ctx context.Context) error {
				// 这个测试需要实际的取消操作，这里只是示例
				_, err := FindActiveConsumers(ctx, db, "test-group", 30*time.Second)
				return err
			},
			expectError: false, // 在mock环境中不会真正超时
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.setupCtx()
			defer cancel()

			// 设置mock期望
			if tc.name == "ContextCancellation" {
				rows := sqlmock.NewRows([]string{"group_id", "consumer_id", "generation_id", "subscribed_topics", "assigned_partitions", "last_heartbeat"})
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?")).
					WillReturnRows(rows)
			}

			// 对于超时测试，立即取消上下文
			if tc.name == "ContextWithTimeout" {
				cancel()
			}

			err := tc.operation(ctx)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	assert.NoError(t, mock.ExpectationsWereMet())
}
