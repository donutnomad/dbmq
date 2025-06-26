package dal

import (
	"context"
	"github.com/donutnomad/dbmq/types"
	"strings"
	"time"
)

// FetchMessages 从特定分区在给定偏移量之后获取消息
// 这是消费者Poll操作的核心数据库查询
func (d *MqDao) FetchMessages(ctx context.Context, topic string, partition uint, offset int64, limit int) ([]types.Message, error) {
	var messages []types.Message
	err := d.db.WithContext(ctx).
		Model(&types.Message{}).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`per_partition_offset` > ?", offset).
		Order("`per_partition_offset` ASC").
		Limit(limit).
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
func (d *MqDao) FetchMessagesBatch(ctx context.Context, requests []PartitionRequest) ([]types.Message, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	// 构建UNION ALL查询
	var unionParts []string
	var args []any

	for _, req := range requests {
		unionParts = append(unionParts, "(SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `per_partition_offset` > ? ORDER BY `per_partition_offset` ASC LIMIT ?)")
		args = append(args, req.Topic, req.Partition, req.Offset, req.Limit)
	}

	// 组合所有UNION查询，最后按创建时间排序以保证消息的时间顺序
	sql := strings.Join(unionParts, " UNION ALL ") + " ORDER BY `created_at` ASC, `per_partition_offset` ASC"

	var messages []types.Message
	err := d.db.WithContext(ctx).
		Raw(sql, args...).
		Scan(&messages).Error

	return messages, err
}

// GetTopicLatestOffsetByPartition 获取指定分区的最新偏移量（最大per_partition_offset）
// 如果分区没有消息，返回-1（表示下一条消息从0开始）
func (d *MqDao) GetTopicLatestOffsetByPartition(ctx context.Context, topic string, partition uint) (int64, error) {
	var offset int64
	err := d.db.WithContext(ctx).
		Model(&types.Message{}).
		Select("COALESCE(MAX(`per_partition_offset`), -1)").
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Scan(&offset).Error
	if err != nil {
		return 0, err
	}
	return offset, err
}

// DeleteMessagesByPartition 删除分区中比某个偏移量和某个时间都更早的消息。
func (d *MqDao) DeleteMessagesByPartition(ctx context.Context, topic string, partition uint, maxOffset int64, retentionDate time.Time, limit int) (int64, error) {
	result := d.db.WithContext(ctx).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`per_partition_offset` < ?", maxOffset).
		Where("`created_at` < ?", retentionDate).
		Limit(limit).
		Delete(&types.Message{})
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
		Delete(&types.Message{})
	return result.RowsAffected, result.Error
}
