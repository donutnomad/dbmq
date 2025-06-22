package dal

import (
	"context"
	"dbmq/pkg/types"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// FindActiveConsumers 查找在超时期间内发送过心跳的消费组中的所有活跃消费者
// 这是协调器判断消费组成员变化的核心函数
func FindActiveConsumers(ctx context.Context, db *gorm.DB, groupID string, timeout time.Duration) ([]types.ConsumerHeartbeat, error) {
	var activeConsumers []types.ConsumerHeartbeat
	sql := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"
	err := db.WithContext(ctx).
		Raw(sql, groupID, time.Now().Add(-timeout)).
		Scan(&activeConsumers).Error
	return activeConsumers, err
}

// GetConsumerGroupGeneration 获取消费组的当前代际元数据
// 代际是重新均衡机制的核心，每次重新均衡时递增
func GetConsumerGroupGeneration(ctx context.Context, db *gorm.DB, groupID string) (*types.ConsumerGroupGeneration, error) {
	var gen types.ConsumerGroupGeneration
	sql := "SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?"
	err := db.WithContext(ctx).Raw(sql, groupID).Scan(&gen).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // 不是错误，消费组可能是新的
		}
		return nil, err
	}
	return &gen, nil
}

// IncrementAndGetGenerationID 原子性地递增消费组的代际ID并返回新值
// 如果消费组不存在，则创建一个新的
// 这是重新均衡过程中最关键的操作，确保了代际的原子性更新
func IncrementAndGetGenerationID(ctx context.Context, db *gorm.DB, groupID string) (uint, error) {
	var gen types.ConsumerGroupGeneration

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 使用FOR UPDATE锁定行，确保并发安全
		err := tx.Raw("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ? FOR UPDATE", groupID).Scan(&gen).Error
		if err != nil {
			// 如果记录不存在，我们创建它
			if errors.Is(err, gorm.ErrRecordNotFound) {
				gen = types.ConsumerGroupGeneration{
					GroupID:      groupID,
					GenerationID: 1, // 从代际1开始
					ProtocolType: "consumer",
					UpdatedAt:    time.Now(),
				}
				insertSQL := "INSERT INTO `mq_consumer_group_generations` (`group_id`, `generation_id`, `protocol_type`, `updated_at`) VALUES (?, ?, ?, ?)"
				if err := tx.Exec(insertSQL, gen.GroupID, gen.GenerationID, gen.ProtocolType, gen.UpdatedAt).Error; err != nil {
					return err
				}
				return nil // 成功结束事务
			}
			return err // 其他数据库错误
		}

		// 如果找到，递增代际ID
		gen.GenerationID++
		updateSQL := "UPDATE `mq_consumer_group_generations` SET `generation_id` = ? WHERE `group_id` = ?"
		return tx.Exec(updateSQL, gen.GenerationID, gen.GroupID).Error
	})

	if err != nil {
		return 0, err
	}
	return gen.GenerationID, nil
}

// UpdateAssignmentsInTx 在单个事务中更新多个消费者的分区分配
// assignments map是 consumerID -> partition list 的映射
// 这确保了所有消费者的分区分配是原子性更新的
func UpdateAssignmentsInTx(ctx context.Context, tx *gorm.DB, groupID string, generationID uint, assignments map[string][]types.PartitionInfo) error {
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	for consumerID, partitions := range assignments {
		// 将分区列表序列化为JSON格式存储
		partitionsJSON, err := json.Marshal(partitions)
		if err != nil {
			return fmt.Errorf("failed to marshal assignment for consumer %s: %w", consumerID, err)
		}
		err = tx.WithContext(ctx).Exec(updateSQL, generationID, partitionsJSON, groupID, consumerID).Error
		if err != nil {
			return err
		}
	}
	return nil
}

// FindTopicsByNames 查找所有匹配给定名称的Topic
// 主要用于验证Topic是否存在和获取分区数量
func FindTopicsByNames(ctx context.Context, db *gorm.DB, topicNames []string) ([]types.Topic, error) {
	if len(topicNames) == 0 {
		return nil, nil
	}
	var topics []types.Topic
	sql := "SELECT * FROM `mq_topics` WHERE `topic_name` IN (?)"
	err := db.WithContext(ctx).Raw(sql, topicNames).Scan(&topics).Error
	return topics, err
}

// GetHeartbeat 获取单个消费者的心跳记录
// 包含了消费者的分区分配、订阅信息和最后心跳时间
func GetHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) (*types.ConsumerHeartbeat, error) {
	var hb types.ConsumerHeartbeat
	sql := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?"
	err := db.WithContext(ctx).Raw(sql, groupID, consumerID).Scan(&hb).Error
	if err != nil {
		return nil, err
	}
	return &hb, nil
}

// UpsertHeartbeat 原子性地创建或更新消费者的心跳
// 更新最后心跳时间并确保消费者的订阅Topic是最新的
// 这是消费者心跳循环使用的主要函数
func UpsertHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string, subscribedTopics []byte) error {
	sql := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"
	return db.WithContext(ctx).Exec(sql,
		groupID,
		consumerID,
		subscribedTopics,
		[]byte("{}"), // 默认为空JSON对象
		time.Now(),
	).Error
}

// GetConsumerAssignment 获取单个消费者的分区分配
// 这是GetHeartbeat的别名，因为分配存储在心跳记录中
func GetConsumerAssignment(ctx context.Context, db *gorm.DB, groupID, consumerID string) (*types.ConsumerHeartbeat, error) {
	return GetHeartbeat(ctx, db, groupID, consumerID)
}

// FetchMessages 从特定分区在给定偏移量之后获取消息
// 这是消费者Poll操作的核心数据库查询
func FetchMessages(ctx context.Context, db *gorm.DB, topic string, partition uint, offset int64, limit int) ([]types.Message, error) {
	var messages []types.Message
	sql := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
	err := db.WithContext(ctx).
		Raw(sql, topic, partition, offset, limit).
		Scan(&messages).Error
	return messages, err
}

// PartitionRequest 表示单个分区的获取请求
type PartitionRequest struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
	Offset    int64  // 起始偏移量
	Limit     int    // 获取限制
}

// FetchMessagesBatch 批量从多个分区获取消息
// 使用UNION ALL查询一次性获取多个分区的消息，提高数据库查询效率
func FetchMessagesBatch(ctx context.Context, db *gorm.DB, requests []PartitionRequest) ([]types.Message, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// 构建UNION ALL查询
	var unionParts []string
	var args []any

	for _, req := range requests {
		unionParts = append(unionParts, "(SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?)")
		args = append(args, req.Topic, req.Partition, req.Offset, req.Limit)
	}

	// 组合所有UNION查询，最后按ID排序以保证消息的全局顺序
	sql := strings.Join(unionParts, " UNION ALL ") + " ORDER BY `id` ASC"

	var messages []types.Message
	err := db.WithContext(ctx).
		Raw(sql, args...).
		Scan(&messages).Error

	return messages, err
}

// GetCommittedOffsets 获取消费组对一组分区的已提交偏移量
// 返回PartitionInfo到已提交偏移量的映射。没有已提交偏移量的分区将不在映射中
func GetCommittedOffsets(ctx context.Context, db *gorm.DB, groupID string, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
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

	err := db.WithContext(ctx).
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
func BatchCommitOffsets(ctx context.Context, db *gorm.DB, groupID string, generationID uint, offsets map[types.PartitionInfo]int64) error {
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
func CommitOffset(ctx context.Context, db *gorm.DB, groupID string, generationID uint, p types.PartitionInfo, offset int64) error {
	// IF(VALUES(generation_id) >= generation_id, ...) 子句是隔离的关键
	// 它防止来自先前代际（具有较小generation_id）的消费者
	// 覆盖来自当前或未来代际的消费者的偏移量
	sql := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
	return db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, offset, generationID, time.Now()).Error
}

// CreateMessage 向数据库插入新消息
// 使用GORM的Create方法确保消息的ID在插入后被填充
func CreateMessage(ctx context.Context, db *gorm.DB, msg *types.Message) error {
	return db.WithContext(ctx).Create(msg).Error
}

// RegisterConsumer 创建或更新消费者的注册，包括其Topic订阅
// 应该在消费者启动或更改其订阅时调用
func RegisterConsumer(ctx context.Context, db *gorm.DB, heartbeat *types.ConsumerHeartbeat) error {
	// 此操作确保消费者的记录存在且其订阅的Topic是最新的
	sql := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `subscribed_topics` = VALUES(`subscribed_topics`), `generation_id` = VALUES(`generation_id`), `last_heartbeat` = VALUES(`last_heartbeat`)"
	return db.WithContext(ctx).Exec(sql,
		heartbeat.GroupID,
		heartbeat.ConsumerID,
		heartbeat.GenerationID,
		heartbeat.SubscribedTopics,
		heartbeat.AssignedPartitions, // 初始为空
		heartbeat.LastHeartbeat,
	).Error
}

// UpdateHeartbeat 仅更新消费者的last_heartbeat时间戳
// 这是应该定期调用的轻量级操作
func UpdateHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) error {
	sql := "UPDATE `mq_consumer_heartbeats` SET `last_heartbeat` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	return db.WithContext(ctx).Exec(sql, time.Now(), groupID, consumerID).Error
}

// DeleteHeartbeat 完全删除消费者的心跳记录
// 用于优雅关闭，表示立即离开消费组
func DeleteHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) error {
	sql := "DELETE FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?"
	return db.WithContext(ctx).Exec(sql, groupID, consumerID).Error
}

// FindAllActiveGroups 查找在超时期间内发送过心跳的所有不同消费组ID
// 用于协调器的全局扫描，找出所有活跃的消费组
func FindAllActiveGroups(ctx context.Context, db *gorm.DB, timeout time.Duration) ([]string, error) {
	var groupIDs []string
	sql := "SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?"
	err := db.WithContext(ctx).
		Raw(sql, time.Now().Add(-timeout)).
		Pluck("group_id", &groupIDs).Error
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
func GetConsumerGroupLowWatermarks(ctx context.Context, db *gorm.DB) (map[types.PartitionInfo]int64, error) {
	var results []LowWatermark
	sql := "SELECT `topic`, `partition`, MIN(`committed_offset`) as low_watermark FROM `mq_consumer_group_offsets` GROUP BY `topic`, `partition`"

	err := db.WithContext(ctx).Raw(sql).Scan(&results).Error
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

// GetAllTopics retrieves all topics from the database.
func GetAllTopics(ctx context.Context, db *gorm.DB) ([]types.Topic, error) {
	var topics []types.Topic
	err := db.WithContext(ctx).Find(&topics).Error
	return topics, err
}

// GetLatestOffset 获取指定分区的最新偏移量（最大消息ID）
// 如果分区没有消息，返回0
func GetLatestOffset(ctx context.Context, db *gorm.DB, topic string, partition uint) (int64, error) {
	var maxID int64
	sql := "SELECT COALESCE(MAX(id), 0) FROM `mq_messages` WHERE `topic` = ? AND `partition` = ?"
	err := db.WithContext(ctx).Raw(sql, topic, partition).Scan(&maxID).Error
	return maxID, err
}

// DeleteMessagesByPartition deletes messages from a partition that are older than a certain offset AND a certain time.
func DeleteMessagesByPartition(ctx context.Context, db *gorm.DB, topic string, partition uint, maxOffset int64, retentionDate time.Time, limit int) (int64, error) {
	// We must use a raw query because GORM does not support DELETE with table alias and JOIN.
	sql := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` < ? AND `created_at` < ? LIMIT ?"
	res := db.WithContext(ctx).Exec(sql, topic, partition, maxOffset, retentionDate, limit)
	return res.RowsAffected, res.Error
}

// DeleteMessagesByPartitionUnconsumed deletes messages from a partition that are older than a certain time.
// This is used for partitions that have no active consumers.
func DeleteMessagesByPartitionUnconsumed(ctx context.Context, db *gorm.DB, topic string, partition uint, retentionDate time.Time, limit int) (int64, error) {
	sql := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `created_at` < ? LIMIT ?"
	res := db.WithContext(ctx).Exec(sql, topic, partition, retentionDate, limit)
	return res.RowsAffected, res.Error
}
