package dao

import (
	"context"
	"github.com/donutnomad/dbmq/internal/db"
	"strings"
	"time"
)

// FetchMessages 从特定分区在给定偏移量之后获取消息
// 这是消费者Poll操作的核心数据库查询
// offset现在是全局ID，而不是分区内偏移量
func (d *MqDao) FetchMessages(ctx context.Context, topic string, partition uint, offset int64, limit int) ([]db.Message, error) {
	var messages []db.Message
	err := d.db.WithContext(ctx).
		Model(&db.Message{}).
		Where("topic = ?", topic).
		Where("`partition` = ?", partition).
		Where("id > ?", offset).
		Order("id ASC").
		Limit(limit).
		Scan(&messages).Error
	return messages, err
}

// PartitionRequest 表示单个分区的获取请求
type PartitionRequest struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
	ID        int64  // 已消费的最新ID
	Limit     int    // 获取限制
}

// FetchMessagesBatch 批量从多个分区获取消息
// 每个分区有一个ID，会查询返回大于这个ID的消息，所以这个ID是已消费的最新ID
// 如果从未消费，那么值是0，而数据库的ID都是从1开始的，所以也是满足要求的
func (d *MqDao) FetchMessagesBatch(ctx context.Context, requests []PartitionRequest) ([]db.Message, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// 构建UNION ALL查询
	var unionParts []string
	var args []any

	for _, req := range requests {
		unionParts = append(unionParts, "(SELECT * FROM "+db.Message{}.TableName()+" WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?)")
		args = append(args, req.Topic, req.Partition, req.ID, req.Limit)
	}

	// 组合所有UNION查询，最后按ID排序以保证消息的顺序
	sql := strings.Join(unionParts, " UNION ALL ") + " ORDER BY `id` ASC"

	var messages []db.Message
	err := d.db.WithContext(ctx).
		Raw(sql, args...).
		Scan(&messages).Error

	return messages, err
}

type TopicPartitionOffset struct {
	Topic     string `gorm:"column:topic"`
	Partition uint   `gorm:"column:partition"`
	MaxID     int64  `gorm:"column:max_id"` // 这里用 Offset 对应 MAX(id)
}

func (d *MqDao) GetTopicsLatestIDsByPartitions(ctx context.Context, topicPartitions []db.PartitionInfo) (map[db.PartitionInfo]int64, error) {
	if len(topicPartitions) == 0 {
		return map[db.PartitionInfo]int64{}, nil // 没有要查询的组合，返回空 map
	}

	var inArgs []any
	var placeholders = make([]string, len(topicPartitions))
	for i, tp := range topicPartitions {
		placeholders[i] = "(?, ?)"
		inArgs = append(inArgs, tp.Topic, tp.Partition)
	}

	var results []TopicPartitionOffset
	query := d.db.WithContext(ctx).
		Model(&db.Message{}).
		Select("topic, `partition`, COALESCE(MAX(`id`), 0) AS max_id").
		Where("(topic, `partition`) IN ("+strings.Join(placeholders, ", ")+")", inArgs...). // 核心：使用行构造器
		Group("topic, `partition`").
		Find(&results)

	if query.Error != nil {
		return nil, query.Error
	}

	latestIDs := make(map[db.PartitionInfo]int64)
	for _, res := range results {
		latestIDs[db.PartitionInfo{Topic: res.Topic, Partition: res.Partition}] = res.MaxID
	}

	return latestIDs, nil
}

// GetTopicLatestIDByPartition 获取指定分区的最后一个消息的ID
func (d *MqDao) GetTopicLatestIDByPartition(ctx context.Context, topic string, partition uint) (int64, error) {
	var offset int64
	err := d.db.WithContext(ctx).
		Model(&db.Message{}).
		Select("COALESCE(MAX(`id`), 0)").
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Scan(&offset).Error
	if err != nil {
		return 0, err
	}
	return offset, err
}

// DeleteMessagesByPartition 删除分区中比某个偏移量和某个时间都更早的消息。
// maxOffset现在是全局ID，而不是分区内偏移量
func (d *MqDao) DeleteMessagesByPartition(ctx context.Context, topic string, partition uint, maxOffset int64, retentionDate time.Time, limit int) (int64, error) {
	result := d.db.WithContext(ctx).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`id` < ?", maxOffset).
		Where("`created_at` < ?", retentionDate).
		Limit(limit).
		Delete(&db.Message{})
	return result.RowsAffected, result.Error
}

// DeleteMessagesByPartitionUnconsumed 会删除某个分区中超过特定时间的未消费消息。
// 用于分区没有活跃消费者的情况
func (d *MqDao) DeleteMessagesByPartitionUnconsumed(ctx context.Context, topic string, partition uint, retentionDate time.Time, limit int) (int64, error) {
	result := d.db.WithContext(ctx).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`created_at` < ?", retentionDate).
		Limit(limit).
		Delete(&db.Message{})
	return result.RowsAffected, result.Error
}
