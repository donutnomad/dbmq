package repo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/interfaces"

	"gorm.io/datatypes"

	"gorm.io/gorm"
)

type DB = interfaces.DB

type MqRepo struct {
	db DB
}

func NewMqRepo(db DB) *MqRepo {
	return &MqRepo{db: db}
}

func (d *MqRepo) DB() DB {
	return d.db
}

// IncrementAndGetGenerationID 原子性地递增消费组的代际ID并返回新值
// 如果消费组不存在，则创建一个新的
// 这是重新均衡过程中最关键的操作，确保了代际的原子性更新
func (d *MqRepo) IncrementAndGetGenerationID(ctx context.Context, groupID string) (uint, error) {
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
func (d *MqRepo) UpdateAssignments(ctx context.Context, groupID string, generationID uint, assignments map[string][]db.PartitionInfo) error {
	// 在函数内部创建事务，确保所有分配更新的原子性
	// 这防止了调用者忘记使用事务而导致的部分更新问题
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		activeIDs := make([]string, 0, len(assignments))
		for consumerID, partitions := range assignments {
			activeIDs = append(activeIDs, consumerID)
			updates := map[string]any{
				"generation_id":       generationID,
				"assigned_partitions": datatypes.NewJSONSlice(partitions),
				"offline":             false,
				"offline_at":          gorm.Expr("NULL"),
			}
			result := tx.Model(&db.ConsumerHeartbeat{}).
				Where("`group_id` = ?", groupID).
				Where("`consumer_id` = ?", consumerID).
				Updates(updates)
			if result.Error != nil {
				return fmt.Errorf("failed to update assignment for consumer %s: %w", consumerID, result.Error)
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
			}
		}

		inactiveQuery := tx.Model(&db.ConsumerHeartbeat{}).
			Where("`group_id` = ?", groupID)
		if len(activeIDs) > 0 {
			inactiveQuery = inactiveQuery.Where("`consumer_id` NOT IN ?", activeIDs)
		}
		inactiveUpdates := map[string]any{
			"generation_id":       generationID,
			"assigned_partitions": datatypes.NewJSONSlice([]db.PartitionInfo{}),
		}
		if err := inactiveQuery.Updates(inactiveUpdates).Error; err != nil {
			return fmt.Errorf("failed to clear assignments for inactive consumers: %w", err)
		}
		return nil
	})
}

// GetCommittedOffsets 获取消费组对一组分区的消费进度
// 返回PartitionInfo到下一个要消费的消息ID的映射
// 如果返回的消息ID为N，是最后一次消费的消息ID
func (d *MqRepo) GetCommittedOffsets(ctx context.Context, groupID string, partitions []db.PartitionInfo) (db.ConsumerGroupConsumptionProgressSlice, error) {
	if len(partitions) == 0 {
		return nil, nil
	}

	var progressRecords db.ConsumerGroupConsumptionProgressSlice

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
		Model(&db.ConsumerGroupConsumptionProgress{}).
		Where(whereClause, args...).
		Find(&progressRecords).Error

	if err != nil {
		return nil, err
	}
	return progressRecords, nil
}

// BatchCommitOffsetsWithInitialWatermark 在单个事务中为消费组提交一批消费进度，同时设置初始水位线
// 这个方法用于首次消费分区时，记录初始水位线以区分消费策略，解决手动提交模式下的注册问题
func (d *MqRepo) BatchCommitOffsetsWithInitialWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[db.PartitionInfo]ConsumptionProgressWithWatermark) error {
	if len(progressWithWatermarks) == 0 {
		return nil
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		dao := NewMqRepo(tx)
		for p, progressData := range progressWithWatermarks {
			if err := dao.CommitConsumptionProgressWithSubscriptionRegistration(ctx, groupID, generationID, p, progressData.LastConsumedMessageID, progressData.SubscriptionStartWatermark); err != nil {
				return err
			}
		}
		return nil
	})
}

// ConsumptionProgressWithWatermark 包含消费进度和初始水位线的结构
type ConsumptionProgressWithWatermark struct {
	LastConsumedMessageID      int64 // 最后成功消费的消息ID
	SubscriptionStartWatermark int64 // 订阅时的水位线，可为nil表示不设置
}

// CommitConsumptionProgressWithSubscriptionRegistration 为单个分区提交消费进度，同时设置订阅注册信息
// 使用代际隔离机制防止旧代际的消费者覆盖新代际的进度
// 这个函数专门用于首次订阅分区时，同时设置消费进度和订阅水位线
func (d *MqRepo) CommitConsumptionProgressWithSubscriptionRegistration(ctx context.Context, groupID string, generationID uint, p db.PartitionInfo, lastConsumedMessageID, subscriptionStartWatermark int64) error {
	var sql string
	var args []any

	_ = db.ConsumerGroupConsumptionProgress{}
	now := time.Now()
	// 包含订阅水位线的SQL - 用于首次订阅分区
	sql = `INSERT INTO mq_consumer_group_consumption_progress (
	group_id,
	topic,
	` + "`partition`" + `,
	last_consumed_message_id,
	subscription_registered_at,
	subscription_start_watermark,
	generation_id,
	updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE 
 	last_consumed_message_id = IF(VALUES(generation_id) >= generation_id, VALUES(last_consumed_message_id), last_consumed_message_id),
	subscription_start_watermark = IF(subscription_start_watermark IS NULL AND VALUES(generation_id) >= generation_id, VALUES(subscription_start_watermark), subscription_start_watermark),
	generation_id = IF(VALUES(generation_id) >= generation_id, VALUES(generation_id), generation_id),
	updated_at = IF(VALUES(generation_id) >= generation_id, VALUES(updated_at), updated_at)
`

	args = []any{groupID, p.Topic, p.Partition, lastConsumedMessageID, now, subscriptionStartWatermark, generationID, now}

	return d.db.WithContext(ctx).Exec(sql, args...).Error
}

// CreateMessagesBatch 批量插入消息，显著提升高吞吐量场景的性能
func (d *MqRepo) CreateMessagesBatch(ctx context.Context, messages []*db.Message) error {
	if len(messages) == 0 {
		return nil
	}
	for _, msg := range messages {
		msg.Fix()
	}
	result := d.db.WithContext(ctx).CreateInBatches(messages, 100) // 每批100条
	if result.Error != nil {
		return fmt.Errorf("batch create message: %w", result.Error)
	}
	return nil
}

// FindAllActiveGroups 查找在超时期间内发送过心跳的所有不同消费组ID
// 用于协调器的全局扫描，找出所有活跃的消费组
func (d *MqRepo) FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error) {
	var groupIDs []string
	err := d.db.WithContext(ctx).
		Model(&db.ConsumerHeartbeat{}).
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Distinct("`group_id`").
		Pluck("`group_id`", &groupIDs).Error
	return groupIDs, err
}

// FindAllGroups 查找所有消费组ID，包括活跃和非活跃的
// 通过联合查询心跳表和代际表获取所有消费组
func (d *MqRepo) FindAllGroups(ctx context.Context) ([]string, error) {
	var groupIDs []string
	sql := `SELECT DISTINCT group_id FROM (SELECT group_id FROM mq_consumer_heartbeats UNION SELECT group_id FROM mq_consumer_group_generations) AS all_groups`
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
func (d *MqRepo) GetConsumerGroupLowWatermarks(ctx context.Context) (map[db.PartitionInfo]int64, error) {
	var results []LowWatermark
	err := d.db.WithContext(ctx).Model(&db.ConsumerGroupConsumptionProgress{}).
		Select("topic, `partition`, MIN(last_consumed_message_id) as low_watermark").
		Group("topic, `partition`").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	watermarks := make(map[db.PartitionInfo]int64, len(results))
	for _, res := range results {
		p := db.PartitionInfo{Topic: res.Topic, Partition: res.Partition}
		watermarks[p] = res.LowWatermarkOffset
	}

	return watermarks, nil
}
