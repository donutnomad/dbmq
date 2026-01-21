package consumerprogress

import (
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// Progress 消费进度领域实体
type Progress struct {
	GroupID                    string    // 消费组ID
	Topic                      string    // Topic名称
	Partition                  uint      // 分区号
	LastConsumedMessageID      int64     // 最后成功消费的消息ID
	SubscriptionRegisteredAt   time.Time // 消费组首次订阅此分区的时间
	SubscriptionStartWatermark int64     // 订阅时topic的最新消息ID
	GenerationID               uint      // 最后更新此记录时的代际ID
	Metadata                   string    // 可选的元数据信息
	UpdatedAt                  time.Time // 更新时间
}

// PartitionInfo 返回分区信息
func (p *Progress) PartitionInfo() db.PartitionInfo {
	return db.PartitionInfo{
		Topic:     p.Topic,
		Partition: p.Partition,
	}
}

// FromDB 从数据库模型转换
func FromDB(progress *db.ConsumerGroupConsumptionProgress) *Progress {
	if progress == nil {
		return nil
	}
	return &Progress{
		GroupID:                    progress.GroupID,
		Topic:                      progress.Topic,
		Partition:                  progress.Partition,
		LastConsumedMessageID:      progress.LastConsumedMessageID,
		SubscriptionRegisteredAt:   progress.SubscriptionRegisteredAt,
		SubscriptionStartWatermark: progress.SubscriptionStartWatermark,
		GenerationID:               progress.GenerationID,
		Metadata:                   progress.Metadata,
		UpdatedAt:                  progress.UpdatedAt,
	}
}

// FromDBSlice 从数据库模型切片转换
func FromDBSlice(progressList []db.ConsumerGroupConsumptionProgress) []*Progress {
	result := make([]*Progress, len(progressList))
	for i := range progressList {
		result[i] = FromDB(&progressList[i])
	}
	return result
}

// ProgressWithWatermark 包含消费进度和初始水位线的结构
type ProgressWithWatermark struct {
	LastConsumedMessageID      int64 // 最后成功消费的消息ID
	SubscriptionStartWatermark int64 // 订阅时的水位线
}
