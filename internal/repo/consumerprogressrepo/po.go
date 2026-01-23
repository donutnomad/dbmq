package consumerprogressrepo

import (
	"time"

	"github.com/donutnomad/dbmq/internal/types"
)

// ProgressPO 消费组消费进度跟踪表
// 存储消费组对每个分区的消费进度和状态
// 实现"至少一次"消费语义的关键表
// 使用代际隔离防止旧代际消费者覆盖新代际的进度
// 新设计解决了offset命名混乱和手动提交模式下的注册问题
type ProgressPO struct {
	GroupID                    string    `gorm:"primaryKey;type:varchar(255);column:group_id;not null"`
	Topic                      string    `gorm:"primaryKey;type:varchar(255);column:topic;not null"`
	Partition                  uint      `gorm:"primaryKey;type:int unsigned;column:partition;not null"`
	LastConsumedMessageID      int64     `gorm:"column:last_consumed_message_id;not null;default:-1;"`                                       // 最后成功消费的消息ID，-1表示还未消费任何消息
	SubscriptionRegisteredAt   time.Time `gorm:"type:timestamp(3);column:subscription_registered_at;not null;default:CURRENT_TIMESTAMP(3);"` // 消费组首次订阅此分区的时间
	SubscriptionStartWatermark int64     `gorm:"column:subscription_start_watermark"`                                                        // 订阅时topic的最新消息ID，用于区分消费策略(从头开始/从最新开始)
	GenerationID               uint      `gorm:"type:int unsigned;column:generation_id;not null;"`                                           // 最后更新此记录时的代际ID，用于并发控制
	Metadata                   string    `gorm:"type:varchar(255);column:metadata;default:''"`                                               // 可选的元数据信息
	UpdatedAt                  time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"`
}

func (p ProgressPO) PartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		Topic:     p.Topic,
		Partition: p.Partition,
	}
}

func (ProgressPO) TableName() string {
	return "mq_consumer_group_consumption_progress"
}

type ProgressPOSlice []ProgressPO

func (s ProgressPOSlice) ToMap() map[types.PartitionInfo]int64 {
	results := make(map[types.PartitionInfo]int64)
	for _, progress := range s {
		results[types.PartitionInfo{Topic: progress.Topic, Partition: progress.Partition}] = progress.LastConsumedMessageID
	}
	return results
}
