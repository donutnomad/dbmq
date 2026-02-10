package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

// ConsumerQuery 消费组查询接口
type ConsumerQuery interface {
	// GetSubscribedTopics 获取消费组订阅的 Topic 列表
	GetSubscribedTopics(ctx context.Context, groupID string) ([]string, error)
	// GetProgressRecords 获取消费组的消费进度记录 (返回 PO - 保留向后兼容)
	GetProgressRecords(ctx context.Context, groupID string) ([]consumerprogressrepo.ProgressPO, error)
	// GetProgressRecordsDTO 获取消费进度记录 (返回 DTO)
	GetProgressRecordsDTO(ctx context.Context, groupID string) ([]ProgressRecord, error)
	// GetProgressRecord 获取消费组指定分区的消费进度
	GetProgressRecord(ctx context.Context, groupID string, topic string, partition uint) (*consumerprogressrepo.ProgressPO, error)
	// GetPartitionLagStats 获取分区延迟统计
	GetPartitionLagStats(ctx context.Context, topic string, partition uint, currentOffset int64, watermark int64) (*PartitionLag, error)
	// GetConsumerGroupExtended 获取消费组扩展信息
	GetConsumerGroupExtended(ctx context.Context, groupID string) (*ConsumerGroupExtended, error)
	// GetConsumerGroupMetrics 获取消费组完整监控指标
	GetConsumerGroupMetrics(ctx context.Context, groupID string) (*ConsumerGroupMetrics, error)
	// GetAllConsumerGroupsMetrics 获取所有消费组监控指标
	GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error)
}

// consumerQueryMySQL 消费组查询 MySQL 实现
type consumerQueryMySQL struct {
	db interfaces.DB
}

// NewConsumerQuery 创建消费组查询实例
func NewConsumerQuery(db interfaces.DB) ConsumerQuery {
	return &consumerQueryMySQL{db: db}
}

// GetSubscribedTopics 获取消费组订阅的 Topic 列表
func (q *consumerQueryMySQL) GetSubscribedTopics(ctx context.Context, groupID string) ([]string, error) {
	var records []consumerprogressrepo.ProgressPO
	if err := q.db.WithContext(ctx).Model(&consumerprogressrepo.ProgressPO{}).
		Where("group_id = ?", groupID).
		Find(&records).Error; err != nil {
		return nil, err
	}

	// 去重获取 Topic 列表
	topicMap := make(map[string]struct{})
	for _, r := range records {
		topicMap[r.Topic] = struct{}{}
	}
	topics := make([]string, 0, len(topicMap))
	for t := range topicMap {
		topics = append(topics, t)
	}
	return topics, nil
}

// GetProgressRecords 获取消费组的消费进度记录
func (q *consumerQueryMySQL) GetProgressRecords(ctx context.Context, groupID string) ([]consumerprogressrepo.ProgressPO, error) {
	var records []consumerprogressrepo.ProgressPO
	err := q.db.WithContext(ctx).Model(&consumerprogressrepo.ProgressPO{}).
		Where("group_id = ?", groupID).
		Find(&records).Error
	return records, err
}

// GetProgressRecord 获取消费组指定分区的消费进度
func (q *consumerQueryMySQL) GetProgressRecord(ctx context.Context, groupID string, topic string, partition uint) (*consumerprogressrepo.ProgressPO, error) {
	var record consumerprogressrepo.ProgressPO
	err := q.db.WithContext(ctx).
		Where("group_id = ? AND topic = ? AND `partition` = ?", groupID, topic, partition).
		First(&record).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetPartitionLagStats 获取分区延迟统计
// 使用单个聚合查询获取所有统计数据，减少数据库往返
func (q *consumerQueryMySQL) GetPartitionLagStats(ctx context.Context, topic string, partition uint, currentOffset int64, watermark int64) (*PartitionLag, error) {
	result := &PartitionLag{
		Topic:         topic,
		Partition:     partition,
		CurrentOffset: currentOffset,
	}

	// 使用单个聚合查询获取所有统计
	var stats struct {
		TotalCount    int64 `gorm:"column:total_count"`
		ConsumedCount int64 `gorm:"column:consumed_count"`
		LagCount      int64 `gorm:"column:lag_count"`
	}
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select(`
			COUNT(*) as total_count,
			SUM(CASE WHEN id >= ? AND id < ? THEN 1 ELSE 0 END) as consumed_count,
			SUM(CASE WHEN id > ? THEN 1 ELSE 0 END) as lag_count
		`, watermark, currentOffset, currentOffset).
		Where("topic = ? AND `partition` = ?", topic, partition).
		Scan(&stats).Error
	if err != nil {
		return nil, err
	}

	result.TotalMessageCount = stats.TotalCount
	result.ConsumedMessages = stats.ConsumedCount
	result.Lag = stats.LagCount
	result.RemainingMessages = stats.LagCount // lag 和 remaining 相同

	// 计算消费进度百分比
	if stats.ConsumedCount+stats.LagCount > 0 {
		result.ConsumedPercentage = float64(stats.ConsumedCount) / float64(stats.ConsumedCount+stats.LagCount) * 100
	}

	return result, nil
}

// GetConsumerGroupExtended 获取消费组扩展信息
func (q *consumerQueryMySQL) GetConsumerGroupExtended(ctx context.Context, groupID string) (*ConsumerGroupExtended, error) {
	result := &ConsumerGroupExtended{}

	// 获取代际信息
	var generationInfo struct {
		GenerationID int    `gorm:"column:generation_id"`
		LeaderID     string `gorm:"column:leader_id"`
		UpdatedAt    string `gorm:"column:updated_at"`
	}
	err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("generation_id, leader_id, updated_at").
		Where("group_id = ?", groupID).
		First(&generationInfo).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	result.GenerationID = generationInfo.GenerationID
	result.LeaderID = generationInfo.LeaderID

	// 获取成员信息
	var members []ConsumerMemberExtended
	err = q.db.WithContext(ctx).Table("mq_consumer_heartbeats").
		Select("consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at").
		Where("group_id = ?", groupID).
		Order("offline ASC, last_heartbeat DESC").
		Find(&members).Error
	if err != nil {
		return nil, err
	}
	result.Members = members

	// 获取消费进度
	var progress []struct {
		Topic                      string        `gorm:"column:topic"`
		Partition                  int           `gorm:"column:partition"`
		CommittedOffset            int64         `gorm:"column:committed_offset"`
		GenerationID               int           `gorm:"column:generation_id"`
		Metadata                   string        `gorm:"column:metadata"`
		UpdatedAt                  string        `gorm:"column:updated_at"`
		SubscriptionStartWatermark sql.NullInt64 `gorm:"column:subscription_start_watermark"`
	}
	err = q.db.WithContext(ctx).Table("mq_consumer_group_consumption_progress").
		Select("topic, `partition`, last_consumed_message_id as committed_offset, generation_id, metadata, updated_at, subscription_start_watermark").
		Where("group_id = ?", groupID).
		Find(&progress).Error
	if err != nil {
		return nil, err
	}

	result.SubscribedProgress = make([]SubscribedProgress, len(progress))
	for i, p := range progress {
		sp := SubscribedProgress{
			Topic:           p.Topic,
			Partition:       p.Partition,
			CommittedOffset: p.CommittedOffset,
			GenerationID:    p.GenerationID,
			Metadata:        p.Metadata,
		}
		if p.SubscriptionStartWatermark.Valid {
			w := p.SubscriptionStartWatermark.Int64
			sp.SubscriptionStartWatermark = &w
		}
		result.SubscribedProgress[i] = sp
	}

	return result, nil
}

// GetProgressRecordsDTO 获取消费进度记录 (返回 DTO)
func (q *consumerQueryMySQL) GetProgressRecordsDTO(ctx context.Context, groupID string) ([]ProgressRecord, error) {
	records, err := q.GetProgressRecords(ctx, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]ProgressRecord, len(records))
	for i, r := range records {
		result[i] = ProgressRecord{
			Topic:                      r.Topic,
			Partition:                  r.Partition,
			CommittedOffset:            r.LastConsumedMessageID,
			GenerationID:               int(r.GenerationID),
			Metadata:                   r.Metadata,
			SubscriptionStartWatermark: r.SubscriptionStartWatermark,
			UpdatedAt:                  r.UpdatedAt,
		}
	}
	return result, nil
}

// GetConsumerGroupMetrics 获取消费组完整监控指标
// 从 metrics.go 迁移的核心业务逻辑
func (q *consumerQueryMySQL) GetConsumerGroupMetrics(ctx context.Context, groupID string) (*ConsumerGroupMetrics, error) {
	metrics := &ConsumerGroupMetrics{
		GroupID:        groupID,
		ProtocolType:   "consumer",
		AssignedTopics: []string{},
		Members:        []ConsumerMemberMetrics{},
		PartitionLags:  []PartitionLagMetrics{},
	}

	// 获取消费组代际信息
	var generation struct {
		GenerationID int `gorm:"column:generation_id"`
	}
	err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("generation_id").
		Where("group_id = ?", groupID).
		First(&generation).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("failed to get consumer group generation: %w", err)
	}
	metrics.GenerationID = int64(generation.GenerationID)

	// 获取活跃消费者
	var heartbeats []heartbeatrepo.HeartbeatPO
	heartbeatTimeout := 30 * time.Second
	cutoff := time.Now().Add(-heartbeatTimeout)
	err = q.db.WithContext(ctx).Where("group_id = ?", groupID).Find(&heartbeats).Error
	if err != nil {
		return nil, err
	}

	var onlineCount int
	for _, item := range heartbeats {
		// 解析分配的分区 - 直接访问 AssignedPartitions 字段
		var assignment []PartitionInfo
		for _, p := range item.AssignedPartitions {
			assignment = append(assignment, PartitionInfo{Topic: p.Topic, Partition: p.Partition})
		}

		member := ConsumerMemberMetrics{
			ConsumerID:    item.ConsumerID,
			ClientID:      item.ConsumerID,
			Host:          "localhost",
			LastHeartbeat: item.LastHeartbeat,
			Assignment:    assignment,
		}
		metrics.Members = append(metrics.Members, member)

		if item.Offline || item.LastHeartbeat.Before(cutoff) {
			continue
		}
		onlineCount++
		metrics.LastHeartbeat = item.LastHeartbeat
	}

	if onlineCount == 0 {
		metrics.State = "Dead"
	} else {
		metrics.State = "Active"
	}

	// 获取消费进度记录
	progressRecords, err := q.GetProgressRecords(ctx, groupID)
	if err != nil {
		return nil, err
	}

	metrics.AssignedTopics = lo.Uniq(lo.Map(progressRecords, func(item consumerprogressrepo.ProgressPO, _ int) string {
		return item.Topic
	}))

	// 计算消费延迟
	var totalLag int64
	for _, topicName := range metrics.AssignedTopics {
		// 获取 Topic 信息
		var topicPO topicrepo.TopicPO
		if err := q.db.WithContext(ctx).Where("topic_name = ?", topicName).First(&topicPO).Error; err != nil {
			continue
		}

		for i := range topicPO.PartitionCount {
			// 获取已提交的 offset
			var progress consumerprogressrepo.ProgressPO
			err := q.db.WithContext(ctx).
				Where("group_id = ? AND topic = ? AND `partition` = ?", groupID, topicName, i).
				First(&progress).Error
			if err != nil && err != gorm.ErrRecordNotFound {
				continue
			}

			currentID := progress.LastConsumedMessageID
			updateAt := progress.UpdatedAt.UnixMilli()
			watermark := progress.SubscriptionStartWatermark

			// 获取该 Topic+分区的最新消息 ID
			var latestID int64
			err = q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
				Select("COALESCE(MAX(id), -1)").
				Where("topic = ? AND `partition` = ?", topicName, i).
				Scan(&latestID).Error
			if err != nil {
				continue
			}

			// 获取分区延迟统计
			lagStats, err := q.GetPartitionLagStats(ctx, topicName, i, currentID, watermark)
			if err != nil {
				continue
			}

			partitionLag := PartitionLagMetrics{
				Topic:                      topicName,
				Partition:                  int(i),
				CurrentOffset:              currentID,
				LatestOffset:               latestID,
				Lag:                        lagStats.Lag,
				SubscriptionStartWatermark: watermark,
				TotalMessageCount:          lagStats.TotalMessageCount,
				LastMessageId:              latestID,
				ConsumedMessages:           lagStats.ConsumedMessages,
				RemainingMessages:          lagStats.RemainingMessages,
				ConsumedPercentage:         lagStats.ConsumedPercentage,
				UpdatedAt:                  updateAt,
			}
			metrics.PartitionLags = append(metrics.PartitionLags, partitionLag)
			totalLag += lagStats.Lag
		}
	}
	metrics.Lag = totalLag

	return metrics, nil
}

// GetAllConsumerGroupsMetrics 获取所有消费组监控指标
func (q *consumerQueryMySQL) GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error) {
	// 获取所有消费组
	var generations []struct {
		GroupID string `gorm:"column:group_id"`
	}
	err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("DISTINCT group_id").
		Find(&generations).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get all groups: %w", err)
	}

	var metricsSlice []ConsumerGroupMetrics
	for _, g := range generations {
		groupMetrics, err := q.GetConsumerGroupMetrics(ctx, g.GroupID)
		if err != nil {
			// 记录错误但继续处理其他消费组
			fmt.Printf("Warning: failed to get metrics for consumer group %s: %v\n", g.GroupID, err)
			continue
		}
		metricsSlice = append(metricsSlice, *groupMetrics)
	}

	return metricsSlice, nil
}
