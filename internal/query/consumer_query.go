package query

import (
	"context"
	"database/sql"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"gorm.io/gorm"
)

// ConsumerQuery 消费组查询接口
type ConsumerQuery interface {
	// GetSubscribedTopics 获取消费组订阅的 Topic 列表
	GetSubscribedTopics(ctx context.Context, groupID string) ([]string, error)
	// GetProgressRecords 获取消费组的消费进度记录
	GetProgressRecords(ctx context.Context, groupID string) ([]consumerprogressrepo.ProgressPO, error)
	// GetProgressRecord 获取消费组指定分区的消费进度
	GetProgressRecord(ctx context.Context, groupID string, topic string, partition uint) (*consumerprogressrepo.ProgressPO, error)
	// GetPartitionLagStats 获取分区延迟统计
	GetPartitionLagStats(ctx context.Context, topic string, partition uint, currentOffset int64, watermark int64) (*PartitionLag, error)
	// GetConsumerGroupExtended 获取消费组扩展信息
	GetConsumerGroupExtended(ctx context.Context, groupID string) (*ConsumerGroupExtended, error)
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
