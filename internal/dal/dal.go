package dal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/datatypes"

	"github.com/donutnomad/dbmq/types"

	"gorm.io/gorm"
)

type DB interface {
	WithContext(ctx context.Context) *gorm.DB
}
type MqDao struct {
	db DB
}

func NewMqDao(db DB) *MqDao {
	return &MqDao{db: db}
}

// IncrementAndGetGenerationID 原子性地递增消费组的代际ID并返回新值
// 如果消费组不存在，则创建一个新的
// 这是重新均衡过程中最关键的操作，确保了代际的原子性更新
func (d *MqDao) IncrementAndGetGenerationID(ctx context.Context, groupID string) (uint, error) {
	var generationID uint

	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 使用 INSERT ... ON DUPLICATE KEY UPDATE 避免并发竞态条件
		// 这个SQL语句会原子性地处理插入新记录或递增现有记录的generation_id
		sql := `INSERT INTO mq_consumer_group_generations 
				(group_id, generation_id, protocol_type, updated_at) 
				VALUES (?, 1, 'consumer', ?) 
				ON DUPLICATE KEY UPDATE 
				generation_id = generation_id + 1, 
				updated_at = VALUES(updated_at)`

		// 执行原子性插入或更新操作
		if err := tx.Exec(sql, groupID, time.Now()).Error; err != nil {
			return fmt.Errorf("failed to increment generation ID for group %s: %w", groupID, err)
		}

		// 获取更新后的generation_id值
		// 使用单独的SELECT确保我们获得最新的值
		selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
		if err := tx.Raw(selectSQL, groupID).Scan(&generationID).Error; err != nil {
			return fmt.Errorf("failed to retrieve generation ID for group %s: %w", groupID, err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}
	return generationID, nil
}

// UpdateAssignments 在单个事务中更新多个消费者的分区分配
// assignments map是 consumerID -> partition list 的映射
// 这确保了所有消费者的分区分配是原子性更新的
func (d *MqDao) UpdateAssignments(ctx context.Context, groupID string, generationID uint, assignments map[string][]types.PartitionInfo) error {
	// 在函数内部创建事务，确保所有分配更新的原子性
	// 这防止了调用者忘记使用事务而导致的部分更新问题
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
		for consumerID, partitions := range assignments {
			result := tx.Exec(updateSQL, generationID, datatypes.NewJSONSlice(partitions), groupID, consumerID)
			if result.Error != nil {
				// 如果任何一个消费者的更新失败，整个事务会自动回滚
				return fmt.Errorf("failed to update assignment for consumer %s: %w", consumerID, result.Error)
			}
			// 检查是否有行被更新，如果没有则说明消费者不存在
			if result.RowsAffected == 0 {
				return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
			}
		}
		return nil
	})
}

// GetCommittedOffsets 获取消费组对一组分区的已提交偏移量
// 返回PartitionInfo到已提交偏移量的映射。没有已提交偏移量的分区将不在映射中
func (d *MqDao) GetCommittedOffsets(ctx context.Context, groupID string, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	results := make(map[types.PartitionInfo]int64)
	if len(partitions) == 0 {
		return results, nil
	}

	var offsets []types.ConsumerGroupOffset

	// 为每个分区构建OR子句，因为GORM在复杂IN查询上有问题
	var conditions []string
	var args []any
	args = append(args, groupID) // group_id的第一个参数

	for _, p := range partitions {
		conditions = append(conditions, "(`topic` = ? AND `partition` = ?)")
		args = append(args, p.Topic, p.Partition)
	}

	whereClause := "`group_id` = ? AND (" + strings.Join(conditions, " OR ") + ")"

	err := d.db.WithContext(ctx).
		Model(&types.ConsumerGroupOffset{}).
		Where(whereClause, args...).
		Find(&offsets).Error

	if err != nil {
		return nil, err
	}

	// 将结果转换为map
	for _, offset := range offsets {
		p := types.PartitionInfo{Topic: offset.Topic, Partition: offset.Partition}
		results[p] = offset.CommittedOffset
	}

	return results, nil
}

// BatchCommitOffsets 在单个事务中为消费组提交一批偏移量
// 这确保了偏移量提交的原子性，要么全部成功要么全部失败
func BatchCommitOffsets(ctx context.Context, db DB, groupID string, generationID uint, offsets map[types.PartitionInfo]int64) error {
	if len(offsets) == 0 {
		return nil
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for p, offset := range offsets {
			if err := CommitOffset(ctx, tx, groupID, generationID, p, offset); err != nil {
				return err
			}
		}
		return nil
	})
}

// CommitOffset 为单个分区提交偏移量
// 使用代际隔离机制防止旧代际的消费者覆盖新代际的偏移量
func CommitOffset(ctx context.Context, db DB, groupID string, generationID uint, p types.PartitionInfo, offset int64) error {
	// 添加调试日志
	fmt.Printf("🔍 [CommitOffset] GroupID: %s, Topic: %s, Partition: %d, Offset: %d, GenerationID: %d\n",
		groupID, p.Topic, p.Partition, offset, generationID)

	// IF(VALUES(generation_id) >= generation_id, ...) 子句是隔离的关键
	// 它防止来自先前代际（具有较小generation_id）的消费者
	// 覆盖来自当前或未来代际的消费者的偏移量
	sql := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
	err := db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, offset, generationID, time.Now()).Error

	if err != nil {
		fmt.Printf("❌ [CommitOffset] 提交失败: %v\n", err)
	} else {
		fmt.Printf("✅ [CommitOffset] 提交成功\n")
	}

	return err
}

// CreateMessage 向数据库插入新消息
// 使用优化的 INSERT ... SELECT 语句，直接从表中获取下一个偏移量并插入
// 这种方式减少了数据库往返次数，提高了性能
func CreateMessage(ctx context.Context, db DB, msg types.Message) error {
	return insertMessagesForPartitionOptimized(db.WithContext(ctx), msg.Topic, msg.Partition, []*types.Message{&msg})
}

// CreateMessagesBatch 批量插入消息，显著提升高吞吐量场景的性能
// 使用优化的 INSERT ... SELECT 语句，在单个语句中计算 offset 并插入所有消息
// 注意：批量插入是原子性的，要么全部成功，要么全部失败
func CreateMessagesBatch(ctx context.Context, db DB, messages []*types.Message) error {
	if len(messages) == 0 {
		return nil
	}
	type tmpKey struct {
		topic     string
		partition uint
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 按分区分组消息，因为每个分区需要独立计算offset
		partitionGroups := make(map[tmpKey][]*types.Message)
		for _, msg := range messages {
			key := tmpKey{topic: msg.Topic, partition: msg.Partition}
			partitionGroups[key] = append(partitionGroups[key], msg)
		}

		// 为每个分区批量处理消息
		for partitionKey, partitionMessages := range partitionGroups {
			if err := insertMessagesForPartitionOptimized(tx, partitionKey.topic, partitionKey.partition, partitionMessages); err != nil {
				return fmt.Errorf("failed to batch insert messages for partition %v: %w", partitionKey, err)
			}
			fmt.Printf("✅ [CreateMessagesBatch] 分区 %v 批量插入成功 - %d 条消息\n",
				partitionKey, len(partitionMessages))
		}
		fmt.Printf("✅ [CreateMessagesBatch] 全部批量插入成功 - 总计 %d 条消息\n", len(messages))
		return nil
	})
}

// insertMessagesForPartitionOptimized 为单个分区优化批量插入消息
// 使用单个 INSERT ... SELECT 语句，直接在 SQL 中计算连续的 offset 值
func insertMessagesForPartitionOptimized(tx *gorm.DB, topic string, partition uint, messages []*types.Message) error {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) == 1 {
		msg := messages[0]
		msg.Fix()

		// 使用INSERT ... SELECT优化的SQL语句
		// 这个语句会原子性地获取下一个offset并插入消息
		sql := `
INSERT INTO mq_messages (topic, ` + "`partition`" + `, per_partition_offset, message_key, headers, body, created_at)
SELECT ?, ?, 
       COALESCE(MAX(per_partition_offset), -1) + 1 as next_offset,
       ?, ?, ?, ?
FROM mq_messages 
WHERE topic = ? AND ` + "`partition`" + ` = ?
FOR UPDATE`

		// 执行插入操作
		result := tx.Exec(sql,
			msg.Topic, msg.Partition, // INSERT部分的topic, partition
			msg.MessageKey, msg.Headers, msg.Body, msg.CreatedAt, // INSERT部分的其他字段
			msg.Topic, msg.Partition, // SELECT部分的WHERE条件
		)
		return result.Error
	}

	// 构建批量 INSERT ... SELECT 语句
	// 使用 ROW_NUMBER() 窗口函数为每条消息分配连续的 offset
	var valueStrings []string
	var args []interface{}

	// 先添加基础参数（用于子查询获取基准 offset）
	args = append(args, topic, partition)

	// 构建 VALUES 子句，每条消息一行
	for i, msg := range messages {
		msg.Fix()

		// 构建 VALUES 子句：(topic, partition, message_key, headers, body, created_at, row_index)
		// row_index 从 1 开始，因为 offset 应该是 base_offset + row_index
		valueStrings = append(valueStrings, fmt.Sprintf("(?, ?, ?, ?, ?, ?, %d)", i+1))
		args = append(args, topic, partition, msg.MessageKey, msg.Headers, msg.Body, msg.CreatedAt)
	}

	// 构建完整的 INSERT ... SELECT 语句
	// 这个语句会：
	// 1. 使用 CTE 只查询一次当前分区的最大 offset（避免重复查询）
	// 2. 为每条新消息分配连续的 offset
	// 3. 在单个语句中插入所有消息
	sql := fmt.Sprintf(`
WITH max_offset AS (
    SELECT COALESCE(MAX(per_partition_offset), -1) as current_max_offset 
    FROM mq_messages 
    WHERE topic = ? AND `+"`partition`"+` = ? 
    FOR UPDATE
)
INSERT INTO mq_messages (topic, `+"`partition`"+`, per_partition_offset, message_key, headers, body, created_at)
SELECT 
    batch_data.topic,
    batch_data.partition,
    max_offset.current_max_offset + batch_data.row_num as per_partition_offset,
    batch_data.message_key,
    batch_data.headers,
    batch_data.body,
    batch_data.created_at
FROM (
    VALUES %s
) AS batch_data(topic, partition, message_key, headers, body, created_at, row_num)
CROSS JOIN max_offset
ORDER BY batch_data.row_num`, strings.Join(valueStrings, ", "))

	// 执行批量插入
	result := tx.Exec(sql, args...)
	if result.Error != nil {
		fmt.Printf("❌ [insertMessagesForPartitionOptimized] 批量插入失败: %v\n", result.Error)
		return result.Error
	}

	// 检查插入行数
	expectedRows := int64(len(messages))
	if result.RowsAffected != expectedRows {
		return fmt.Errorf("expected to insert %d rows, but inserted %d", expectedRows, result.RowsAffected)
	}

	return nil
}

// FindAllActiveGroups 查找在超时期间内发送过心跳的所有不同消费组ID
// 用于协调器的全局扫描，找出所有活跃的消费组
func (d *MqDao) FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error) {
	var groupIDs []string
	err := d.db.WithContext(ctx).
		Model(&types.ConsumerHeartbeat{}).
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Distinct("`group_id`").
		Pluck("`group_id`", &groupIDs).Error
	return groupIDs, err
}

// FindAllGroups 查找所有消费组ID，包括活跃和非活跃的
// 通过联合查询心跳表和代际表获取所有消费组
func (d *MqDao) FindAllGroups(ctx context.Context) ([]string, error) {
	var groupIDs []string
	sql := "SELECT DISTINCT `group_id` FROM (SELECT `group_id` FROM `mq_consumer_heartbeats` UNION SELECT `group_id` FROM `mq_consumer_group_generations`) AS all_groups"
	err := d.db.WithContext(ctx).Raw(sql).Pluck("group_id", &groupIDs).Error
	return groupIDs, err
}

// LowWatermark holds the result of the low watermark query.
type LowWatermark struct {
	Topic              string `gorm:"column:topic"`
	Partition          uint   `gorm:"column:partition"`
	LowWatermarkOffset int64  `gorm:"column:low_watermark"`
}

// GetConsumerGroupLowWatermarks calculates the minimum committed offset for every partition across all consumer groups.
// This is the "consumption low watermark".
func (d *MqDao) GetConsumerGroupLowWatermarks(ctx context.Context) (map[types.PartitionInfo]int64, error) {
	var results []LowWatermark
	sql := "SELECT `topic`, `partition`, MIN(`committed_offset`) as low_watermark FROM `mq_consumer_group_offsets` GROUP BY `topic`, `partition`"

	err := d.db.WithContext(ctx).Raw(sql).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	watermarks := make(map[types.PartitionInfo]int64, len(results))
	for _, res := range results {
		p := types.PartitionInfo{Topic: res.Topic, Partition: res.Partition}
		watermarks[p] = res.LowWatermarkOffset
	}

	return watermarks, nil
}
