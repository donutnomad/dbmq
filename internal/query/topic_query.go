package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
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
	// GetTopicMetrics 获取 Topic 完整监控指标
	GetTopicMetrics(ctx context.Context, topicName string) (*TopicMetrics, error)
	// GetAllTopicsMetrics 获取所有 Topic 监控指标
	GetAllTopicsMetrics(ctx context.Context) ([]TopicMetrics, error)
	// GetAllTopicPartitionStats 获取所有 Topic 的分区统计
	GetAllTopicPartitionStats(ctx context.Context) (map[string][]PartitionStats, error)
}

// topicQueryMySQL Topic 查询 MySQL 实现
type topicQueryMySQL struct {
	db interfaces.DB
}

const allTopicPartitionStatsSelectSQL = `
	topic,
	` + "`partition`" + `,
	COALESCE(MIN(id), -1) AS first_message_id,
	COALESCE(MAX(id), -1) AS last_message_id,
	COUNT(*) AS message_count,
	COALESCE(SUM(LENGTH(body)), 0) AS size_bytes,
	COALESCE(MIN(created_at), '') AS created_at,
	COALESCE(MAX(created_at), '') AS updated_at
`

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
		stats.Partitions[i] = PartitionStats{
			Partition:      i,
			FirstMessageID: -1,
			LastMessageID:  -1,
		}
	}

	type partitionStatRow struct {
		Topic          string `gorm:"column:topic"`
		Partition      uint   `gorm:"column:partition"`
		FirstMessageID int64  `gorm:"column:first_message_id"`
		LastMessageID  int64  `gorm:"column:last_message_id"`
		MessageCount   int64  `gorm:"column:message_count"`
		SizeBytes      int64  `gorm:"column:size_bytes"`
		CreatedAt      string `gorm:"column:created_at"`
		UpdatedAt      string `gorm:"column:updated_at"`
	}

	var rows []partitionStatRow
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select(allTopicPartitionStatsSelectSQL).
		Where("topic = ?", topicName).
		Group("topic, `partition`").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get topic stats: %w", err)
	}

	for _, row := range rows {
		if row.Partition >= partitionCount {
			continue
		}
		partStats := PartitionStats{
			Partition:      row.Partition,
			FirstMessageID: row.FirstMessageID,
			LastMessageID:  row.LastMessageID,
			MessageCount:   row.MessageCount,
			SizeBytes:      row.SizeBytes,
			CreatedAt:      row.CreatedAt,
			UpdatedAt:      row.UpdatedAt,
		}
		stats.Partitions[row.Partition] = partStats
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

// GetTopicMetrics 获取 Topic 完整监控指标
func (q *topicQueryMySQL) GetTopicMetrics(ctx context.Context, topicName string) (*TopicMetrics, error) {
	// 获取 Topic 基本信息
	var topicPO topicrepo.TopicPO
	if err := q.db.WithContext(ctx).Where("topic_name = ?", topicName).First(&topicPO).Error; err != nil {
		return nil, fmt.Errorf("failed to get topic %s: %w", topicName, err)
	}

	metrics := &TopicMetrics{
		TopicName:      topicPO.TopicName,
		PartitionCount: int(topicPO.PartitionCount),
		CreatedAt:      topicPO.CreatedAt,
		Config:         make(map[string]string),
	}

	// 解析 Topic 配置
	configMap := topicPO.Configs
	if len(configMap) > 0 {
		var config map[string]any
		if err := json.Unmarshal(configMap, &config); err == nil {
			for k, v := range config {
				metrics.Config[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	// 获取 Topic 统计信息
	topicStats, err := q.GetTopicStats(ctx, topicName, topicPO.PartitionCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic stats: %w", err)
	}

	// 转换分区信息
	partitions := make([]PartitionMetricsDTO, len(topicStats.Partitions))
	for i, p := range topicStats.Partitions {
		partitions[i] = PartitionMetricsDTO{
			Partition:      int(p.Partition),
			FirstMessageID: p.FirstMessageID,
			LatestOffset:   p.LastMessageID,
			MessageCount:   p.MessageCount,
			SizeBytes:      p.SizeBytes,
		}
	}

	metrics.Partitions = partitions
	metrics.MessageCount = topicStats.TotalMessages
	metrics.SizeBytes = topicStats.TotalSize
	metrics.LatestOffset = topicStats.LatestOffset

	return metrics, nil
}

// GetAllTopicsMetrics 获取所有 Topic 监控指标
func (q *topicQueryMySQL) GetAllTopicsMetrics(ctx context.Context) ([]TopicMetrics, error) {
	var topics []topicrepo.TopicPO
	if err := q.db.WithContext(ctx).Find(&topics).Error; err != nil {
		return nil, fmt.Errorf("failed to get all topics: %w", err)
	}
	if len(topics) == 0 {
		return nil, nil
	}

	metricsSlice := make([]TopicMetrics, 0, len(topics))
	for _, t := range topics {
		metrics := TopicMetrics{
			TopicName:      t.TopicName,
			PartitionCount: int(t.PartitionCount),
			CreatedAt:      t.CreatedAt,
			Config:         make(map[string]string),
		}

		// 解析 Topic 配置
		if len(t.Configs) > 0 {
			var config map[string]any
			if err := json.Unmarshal(t.Configs, &config); err == nil {
				for k, v := range config {
					metrics.Config[k] = fmt.Sprintf("%v", v)
				}
			}
		}

		partitions := make([]PartitionMetricsDTO, t.PartitionCount)
		for i := range t.PartitionCount {
			partitions[i] = PartitionMetricsDTO{Partition: int(i), FirstMessageID: -1, LatestOffset: -1}
		}
		metrics.Partitions = partitions
		metricsSlice = append(metricsSlice, metrics)
	}

	return metricsSlice, nil
}

func (q *topicQueryMySQL) GetAllTopicPartitionStats(ctx context.Context) (map[string][]PartitionStats, error) {
	type partitionStatRow struct {
		Topic          string `gorm:"column:topic"`
		Partition      uint   `gorm:"column:partition"`
		FirstMessageID int64  `gorm:"column:first_message_id"`
		LastMessageID  int64  `gorm:"column:last_message_id"`
		MessageCount   int64  `gorm:"column:message_count"`
		SizeBytes      int64  `gorm:"column:size_bytes"`
		CreatedAt      string `gorm:"column:created_at"`
		UpdatedAt      string `gorm:"column:updated_at"`
	}

	var rows []partitionStatRow
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select(allTopicPartitionStatsSelectSQL).
		Group("topic, `partition`").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get partition stats: %w", err)
	}

	statsByTopic := make(map[string][]PartitionStats)
	for _, row := range rows {
		statsByTopic[row.Topic] = append(statsByTopic[row.Topic], PartitionStats{
			Partition:      row.Partition,
			FirstMessageID: row.FirstMessageID,
			LastMessageID:  row.LastMessageID,
			MessageCount:   row.MessageCount,
			SizeBytes:      row.SizeBytes,
			CreatedAt:      row.CreatedAt,
			UpdatedAt:      row.UpdatedAt,
		})
	}

	return statsByTopic, nil
}
