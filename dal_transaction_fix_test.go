package dbmq

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBugFix5_UpdateAssignmentsTransactionFix 验证BUG #5的修复：事务管理优化
// 修复内容：将事务逻辑从调用者移动到UpdateAssignments内部，确保原子性
func TestBugFix5_UpdateAssignmentsTransactionFix(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	groupID := "test-transaction-fix-group"

	t.Run("正常情况下的事务行为", func(t *testing.T) {
		// 创建测试消费者
		consumers := []types.ConsumerHeartbeat{
			{
				ConsumerID:         "consumer-1",
				GroupID:            groupID,
				GenerationID:       1,
				SubscribedTopics:   ([]string{"test-topic"}),
				AssignedPartitions: ([]types.PartitionInfo{}), // 初始为空分区
				LastHeartbeat:      time.Now(),
			},
			{
				ConsumerID:         "consumer-2",
				GroupID:            groupID,
				GenerationID:       1,
				SubscribedTopics:   ([]string{"test-topic"}),
				AssignedPartitions: ([]types.PartitionInfo{}), // 初始为空分区
				LastHeartbeat:      time.Now(),
			},
		}

		// 插入消费者到数据库
		for _, consumer := range consumers {
			require.NoError(t, db.Create(&consumer).Error)
		}

		// 准备分配映射
		assignments := map[string][]types.PartitionInfo{
			"consumer-1": {
				{Topic: "test-topic", Partition: 0},
			},
			"consumer-2": {
				{Topic: "test-topic", Partition: 1},
			},
		}

		// 调用新的UpdateAssignments函数（内部自动创建事务）
		err := dal.UpdateAssignments(context.Background(), db, groupID, 2, assignments)
		require.NoError(t, err, "正常的分配更新应该成功")

		// 验证所有消费者都被正确更新
		var updatedConsumers []types.ConsumerHeartbeat
		require.NoError(t, db.Where("group_id = ?", groupID).Find(&updatedConsumers).Error)

		for _, consumer := range updatedConsumers {
			assert.Equal(t, uint(2), consumer.GenerationID,
				"消费者 %s 的代际ID应该被更新为2", consumer.ConsumerID)

			// 验证分区分配
			var partitions []types.PartitionInfo = consumer.AssignedPartitions

			expectedPartitions := assignments[consumer.ConsumerID]
			assert.Equal(t, expectedPartitions, partitions,
				"消费者 %s 的分区分配应该正确", consumer.ConsumerID)
		}

		t.Log("✅ BUG #5 修复验证：正常情况下事务行为正确")
	})

	t.Run("失败情况下的事务回滚", func(t *testing.T) {
		// 使用不同的group ID避免冲突
		failGroupID := "test-transaction-fail-group"

		// 只创建一个消费者
		consumer := types.ConsumerHeartbeat{
			ConsumerID:         "existing-consumer",
			GroupID:            failGroupID,
			GenerationID:       1,
			SubscribedTopics:   ([]string{"test-topic"}),
			AssignedPartitions: ([]types.PartitionInfo{}), // 初始为空分区
			LastHeartbeat:      time.Now(),
		}
		require.NoError(t, db.Create(&consumer).Error)

		// 准备分配映射，包含不存在的消费者
		failAssignments := map[string][]types.PartitionInfo{
			"existing-consumer": {
				{Topic: "test-topic", Partition: 0},
			},
			"nonexistent-consumer": { // 这个消费者不存在
				{Topic: "test-topic", Partition: 1},
			},
		}

		// 调用UpdateAssignments，应该失败并回滚
		err := dal.UpdateAssignments(context.Background(), db, failGroupID, 2, failAssignments)
		assert.Error(t, err, "包含不存在消费者的更新应该失败")

		// 验证事务回滚：existing-consumer的状态不应该被更新
		var unchangedConsumer types.ConsumerHeartbeat
		require.NoError(t, db.Where("group_id = ? AND consumer_id = ?",
			failGroupID, "existing-consumer").First(&unchangedConsumer).Error)

		assert.Equal(t, uint(1), unchangedConsumer.GenerationID,
			"事务回滚后，existing-consumer的代际ID应该仍为1")

		// 验证分区分配也没有被更新（应该为空或原值）
		for _, partition := range unchangedConsumer.AssignedPartitions {
			if partition.Topic == "test-topic" && partition.Partition == 0 {
				t.Error("事务回滚后，不应该包含新的分区分配")
			}
		}

		t.Log("✅ BUG #5 修复验证：失败情况下事务正确回滚")
	})

	t.Run("调用者无需管理事务", func(t *testing.T) {
		// 这个测试验证调用者不需要显式创建事务
		simpleGroupID := "test-simple-call-group"

		consumer := types.ConsumerHeartbeat{
			ConsumerID:         "simple-consumer",
			GroupID:            simpleGroupID,
			GenerationID:       1,
			SubscribedTopics:   ([]string{"test-topic"}),
			AssignedPartitions: ([]types.PartitionInfo{}), // 初始为空分区
			LastHeartbeat:      time.Now(),
		}
		require.NoError(t, db.Create(&consumer).Error)

		assignments := map[string][]types.PartitionInfo{
			"simple-consumer": {
				{Topic: "test-topic", Partition: 0},
			},
		}

		// 直接调用，无需创建事务
		err := dal.UpdateAssignments(context.Background(), db, simpleGroupID, 3, assignments)
		require.NoError(t, err, "简单调用应该成功")

		// 验证更新成功
		var updatedConsumer types.ConsumerHeartbeat
		require.NoError(t, db.Where("group_id = ? AND consumer_id = ?",
			simpleGroupID, "simple-consumer").First(&updatedConsumer).Error)

		assert.Equal(t, uint(3), updatedConsumer.GenerationID, "代际ID应该被更新")

		t.Log("✅ BUG #5 修复验证：调用者无需管理事务，接口更加安全")
	})
}

// TestUpdateAssignmentsAPIComparison 对比修复前后的API使用方式
func TestUpdateAssignmentsAPIComparison(t *testing.T) {
	t.Run("API使用方式对比", func(t *testing.T) {
		t.Log("🔧 BUG #5 修复对比：")
		t.Log("")
		t.Log("修复前（容易出错的方式）：")
		t.Log("  err = c.db.Transaction(func(tx *gorm.DB) error {")
		t.Log("    return dal.UpdateAssignmentsInTx(ctx, tx, groupID, genID, assignments)")
		t.Log("  })")
		t.Log("  问题：调用者可能忘记使用事务，导致数据不一致")
		t.Log("")
		t.Log("修复后（安全的方式）：")
		t.Log("  err = dal.UpdateAssignments(ctx, db, groupID, genID, assignments)")
		t.Log("  优势：事务逻辑内置，调用者无需关心事务管理")
		t.Log("")
		t.Log("✅ 修复效果：")
		t.Log("  1. 防止调用者忘记使用事务")
		t.Log("  2. 确保分配更新的原子性")
		t.Log("  3. 简化API使用，降低出错概率")
		t.Log("  4. 更好的封装性和数据一致性保证")
	})
}
