package dao

import (
	"context"
	"errors"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/samber/lo"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// GetConsumerHeartbeat 获取单个消费者的心跳记录
// 包含了消费者的分区分配、订阅信息和最后心跳时间
func (d *MqDao) GetConsumerHeartbeat(ctx context.Context, groupID, consumerID string) (*db.ConsumerHeartbeat, error) {
	var hb db.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&db.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		First(&hb).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = nil
		}
		return nil, err
	}
	return &hb, nil
}

// UpsertConsumerHeartbeat 原子性地创建或更新消费者的心跳
// 更新最后心跳时间并确保消费者的订阅Topic是最新的
// 这是消费者心跳循环使用的主要函数
func (d *MqDao) UpsertConsumerHeartbeat(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error {
	sql := "INSERT INTO " + db.ConsumerHeartbeat{}.TableName() + ` (group_id, consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at) 
	VALUES (?, ?, 0, ?, ?, FALSE, ?, NULL) 
	ON DUPLICATE KEY UPDATE 
		last_heartbeat = VALUES(last_heartbeat), 
		offline = FALSE, 
		offline_at = NULL
`
	return d.db.WithContext(ctx).Exec(sql,
		groupID,
		consumerID,
		datatypes.NewJSONSlice(subscribedTopics),
		datatypes.NewJSONSlice([]db.PartitionInfo{}), // 默认为空JSON对象
		time.Now(),
	).Error
}

// MarkConsumerOffline 标记消费者为离线状态
// 用于优雅关闭，保留历史记录但标记为已下线
func (d *MqDao) MarkConsumerOffline(ctx context.Context, groupID, consumerID string) error {
	now := time.Now()
	return d.db.WithContext(ctx).
		Model(&db.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		Updates(map[string]any{
			"offline":    true,
			"offline_at": now,
		}).Error
}

// DeleteConsumerHeartbeat 完全删除消费者的心跳记录
// 保留此方法以支持老的删除逻辑（如果需要）
func (d *MqDao) DeleteConsumerHeartbeat(ctx context.Context, groupID, consumerID string) error {
	return d.db.WithContext(ctx).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		Delete(&db.ConsumerHeartbeat{}).Error
}

// FindActiveConsumers 查找在超时期间内发送过心跳的消费组中的所有活跃消费者
// 这是协调器判断消费组成员变化的核心函数
// 只查找未标记为离线且在超时期间内发送过心跳的消费者
func (d *MqDao) FindActiveConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]db.ConsumerHeartbeat, error) {
	var activeConsumers []db.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&db.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Where("`offline` = FALSE").
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Find(&activeConsumers).Error
	return activeConsumers, err
}

// FindAllConsumers 查找消费组中的所有消费者（包括在线和离线的）
// 用于UI显示，可以看到消费者的完整历史记录
func (d *MqDao) FindAllConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]db.ConsumerHeartbeat, error) {
	var allConsumers []db.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&db.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Order("`last_heartbeat` DESC").
		Find(&allConsumers).Error

	// 为每个消费者添加状态判断逻辑（在应用层判断是否超时）
	cutoffTime := time.Now().Add(-timeout)
	for i, consumer := range allConsumers {
		// 如果没有标记为离线，但心跳超时，认为是超时状态
		if !consumer.Offline && consumer.LastHeartbeat.Before(cutoffTime) {
			allConsumers[i].Offline = true
			allConsumers[i].OfflineAt = lo.ToPtr(time.Now())
		}
	}

	return allConsumers, err
}

// GetConsumerGroupGeneration 获取消费组的当前代际元数据
// 代际是重新均衡机制的核心，每次重新均衡时递增
func (d *MqDao) GetConsumerGroupGeneration(ctx context.Context, groupID string) (*db.ConsumerGroupGeneration, error) {
	var gen db.ConsumerGroupGeneration
	err := d.db.WithContext(ctx).
		Model(&db.ConsumerGroupGeneration{}).
		Where("`group_id` = ?", groupID).
		First(&gen).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = nil
		}
		return nil, err
	}
	return &gen, nil
}

// CommitOffset 为单个分区提交消费进度
// 使用代际隔离机制防止旧代际的消费者覆盖新代际的进度
// offset参数表示最后成功消费的消息ID
func (d *MqDao) CommitOffset(ctx context.Context, groupID string, generationID uint, p db.PartitionInfo, lastConsumedMessageID int64) error {
	// IF(VALUES(generation_id) >= generation_id, ...) 子句是隔离的关键
	// 它防止来自先前代际（具有较小generation_id）的消费者
	// 覆盖来自当前或未来代际的消费者的进度
	sql := "INSERT INTO " + db.ConsumerGroupConsumptionProgress{}.TableName() + " (group_id, topic, " + "`partition`" + `, last_consumed_message_id, generation_id, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE 
			last_consumed_message_id = IF(VALUES(generation_id) >= generation_id, VALUES(last_consumed_message_id), last_consumed_message_id), 
			generation_id = IF(VALUES(generation_id) >= generation_id, VALUES(generation_id), generation_id), 
			updated_at = IF(VALUES(generation_id) >= generation_id, VALUES(updated_at), updated_at)
`
	return d.db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, lastConsumedMessageID, generationID, time.Now()).Error
}

// BatchCommitLastConsumeMessageID 在单个事务中为消费组提交一批消息消费进度
// 这确保了消费进度提交的原子性，要么全部成功要么全部失败
// offsets参数中的值表示最后成功消费的消息ID
func (d *MqDao) BatchCommitLastConsumeMessageID(ctx context.Context, groupID string, generationID uint, consumedIds map[db.PartitionInfo]int64) error {
	if len(consumedIds) == 0 {
		return nil
	}
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		dao := NewMqDao(tx)
		for p, offset := range consumedIds {
			if err := dao.CommitOffset(ctx, groupID, generationID, p, offset); err != nil {
				return err
			}
		}
		return nil
	})
}
