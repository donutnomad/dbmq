package dal

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/dbmq/types"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// GetConsumerHeartbeat 获取单个消费者的心跳记录
// 包含了消费者的分区分配、订阅信息和最后心跳时间
func (d *MqDao) GetConsumerHeartbeat(ctx context.Context, groupID, consumerID string) (*types.ConsumerHeartbeat, error) {
	var hb types.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&types.ConsumerHeartbeat{}).
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
	sql := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `offline`, `last_heartbeat`, `offline_at`) VALUES (?, ?, 0, ?, ?, FALSE, ?, NULL) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`), `offline` = FALSE, `offline_at` = NULL"
	return d.db.WithContext(ctx).Exec(sql,
		groupID,
		consumerID,
		datatypes.NewJSONSlice(subscribedTopics),
		datatypes.NewJSONSlice([]types.PartitionInfo{}), // 默认为空JSON对象
		time.Now(),
	).Error
}

// MarkConsumerOffline 标记消费者为离线状态
// 用于优雅关闭，保留历史记录但标记为已下线
func (d *MqDao) MarkConsumerOffline(ctx context.Context, groupID, consumerID string) error {
	now := time.Now()
	return d.db.WithContext(ctx).
		Model(&types.ConsumerHeartbeat{}).
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
		Delete(&types.ConsumerHeartbeat{}).Error
}

// FindActiveConsumers 查找在超时期间内发送过心跳的消费组中的所有活跃消费者
// 这是协调器判断消费组成员变化的核心函数
// 只查找未标记为离线且在超时期间内发送过心跳的消费者
func (d *MqDao) FindActiveConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]types.ConsumerHeartbeat, error) {
	var activeConsumers []types.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&types.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Where("`offline` = FALSE").
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Find(&activeConsumers).Error
	return activeConsumers, err
}

// FindAllConsumers 查找消费组中的所有消费者（包括在线和离线的）
// 用于UI显示，可以看到消费者的完整历史记录
func (d *MqDao) FindAllConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]types.ConsumerHeartbeat, error) {
	var allConsumers []types.ConsumerHeartbeat
	err := d.db.WithContext(ctx).
		Model(&types.ConsumerHeartbeat{}).
		Where("`group_id` = ?", groupID).
		Order("`last_heartbeat` DESC").
		Find(&allConsumers).Error

	// 为每个消费者添加状态判断逻辑（在应用层判断是否超时）
	cutoffTime := time.Now().Add(-timeout)
	for i := range allConsumers {
		consumer := &allConsumers[i]
		// 如果没有标记为离线，但心跳超时，认为是超时状态
		if !consumer.Offline && consumer.LastHeartbeat.Before(cutoffTime) {
			// 可以在这里添加状态字段，或者在业务层处理
		}
	}

	return allConsumers, err
}

// GetConsumerGroupGeneration 获取消费组的当前代际元数据
// 代际是重新均衡机制的核心，每次重新均衡时递增
func (d *MqDao) GetConsumerGroupGeneration(ctx context.Context, groupID string) (*types.ConsumerGroupGeneration, error) {
	var gen types.ConsumerGroupGeneration
	err := d.db.WithContext(ctx).
		Model(&types.ConsumerGroupGeneration{}).
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
