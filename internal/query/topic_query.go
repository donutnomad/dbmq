package query

import (
	"context"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
)

// TopicQuery Topic 查询接口
type TopicQuery interface {
	// GetPartitionStats 获取指定分区的统计信息
	GetPartitionStats(ctx context.Context, topicName string, partition uint) (*PartitionStats, error)
	// GetTopicStats 获取 Topic 的统计信息 (包含所有分区)
	GetTopicStats(ctx context.Context, topicName string, partitionCount uint) (*TopicStats, error)
	// GetPartitionMessageCount 获取分区消息数量
	GetPartitionMessageCount(ctx context.Context, topicName string, partition uint) (int64, error)
	// GetPartitionSizeBytes 获取分区存储大小
	GetPartitionSizeBytes(ctx context.Context, topicName string, partition uint) (int64, error)
}

// topicQueryMySQL Topic 查询 MySQL 实现
type topicQueryMySQL struct {
	db interfaces.DB
}

// NewTopicQuery 创建 Topic 查询实例
func NewTopicQuery(db interfaces.DB) TopicQuery {
	return &topicQueryMySQL{db: db}
}

// GetPartitionStats 获取指定分区的统计信息
func (q *topicQueryMySQL) GetPartitionStats(ctx context.Context, topicName string, partition uint) (*PartitionStats, error) {
	var result struct {
		FirstMessageID int64  `gorm:"column:first_message_id"`
		LastMessageID  int64  `gorm:"column:last_message_id"`
		MessageCount   int64  `gorm:"column:message_count"`
		SizeBytes      int64  `gorm:"column:size_bytes"`
		CreatedAt      string `gorm:"column:created_at"`
		UpdatedAt      string `gorm:"column:updated_at"`
	}

	sql := `
		SELECT
			COALESCE(MIN(id), -1) AS first_message_id,
			COALESCE(MAX(id), -1) AS last_message_id,
			COUNT(*) AS message_count,
			COALESCE(SUM(LENGTH(body)), 0) AS size_bytes,
			COALESCE(MIN(created_at), '') AS created_at,
			COALESCE(MAX(created_at), '') AS updated_at
		FROM mq_messages
		WHERE topic = ? AND ` + "`partition`" + ` = ?`

	if err := q.db.WithContext(ctx).Raw(sql, topicName, partition).Scan(&result).Error; err != nil {
		return nil, err
	}

	return &PartitionStats{
		Partition:      partition,
		FirstMessageID: result.FirstMessageID,
		LastMessageID:  result.LastMessageID,
		MessageCount:   result.MessageCount,
		SizeBytes:      result.SizeBytes,
		CreatedAt:      result.CreatedAt,
		UpdatedAt:      result.UpdatedAt,
	}, nil
}

// GetTopicStats 获取 Topic 的统计信息 (包含所有分区)
func (q *topicQueryMySQL) GetTopicStats(ctx context.Context, topicName string, partitionCount uint) (*TopicStats, error) {
	stats := &TopicStats{
		Partitions: make([]PartitionStats, partitionCount),
	}

	for i := range partitionCount {
		partStats, err := q.GetPartitionStats(ctx, topicName, i)
		if err != nil {
			return nil, err
		}
		stats.Partitions[i] = *partStats
		stats.TotalMessages += partStats.MessageCount
		stats.TotalSize += partStats.SizeBytes
		if partStats.LastMessageID > stats.LatestOffset {
			stats.LatestOffset = partStats.LastMessageID
		}
	}

	return stats, nil
}

// GetPartitionMessageCount 获取分区消息数量
func (q *topicQueryMySQL) GetPartitionMessageCount(ctx context.Context, topicName string, partition uint) (int64, error) {
	var count int64
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Where("topic = ? AND `partition` = ?", topicName, partition).
		Count(&count).Error
	return count, err
}

// GetPartitionSizeBytes 获取分区存储大小
func (q *topicQueryMySQL) GetPartitionSizeBytes(ctx context.Context, topicName string, partition uint) (int64, error) {
	var sizeBytes int64
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select("COALESCE(SUM(LENGTH(body)), 0)").
		Where("topic = ? AND `partition` = ?", topicName, partition).
		Scan(&sizeBytes).Error
	return sizeBytes, err
}
