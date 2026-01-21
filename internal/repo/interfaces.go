package repo

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// TopicRepo Topic 管理接口
type TopicRepo interface {
	// GetTopic 获取单个 Topic
	GetTopic(ctx context.Context, topicName string) (*db.Topic, error)
	// GetAllTopics 获取所有 Topic
	GetAllTopics(ctx context.Context) ([]db.Topic, error)
	// FindTopicsByNames 批量查找 Topic
	FindTopicsByNames(ctx context.Context, topicNames []string) ([]db.Topic, error)
}

// MessageRepo 消息读写接口
type MessageRepo interface {
	// CreateMessagesBatch 批量创建消息
	CreateMessagesBatch(ctx context.Context, messages []*db.Message) error
	// FetchMessages 获取单分区消息
	FetchMessages(ctx context.Context, topic string, partition uint, offset int64, limit int) ([]db.Message, error)
	// FetchMessagesBatch 批量获取多分区消息
	FetchMessagesBatch(ctx context.Context, requests []PartitionRequest) ([]db.Message, error)
	// GetTopicLatestIDByPartition 获取单分区最新 ID
	GetTopicLatestIDByPartition(ctx context.Context, topic string, partition uint) (int64, error)
	// GetTopicsLatestIDsByPartitions 批量获取多分区最新 ID
	GetTopicsLatestIDsByPartitions(ctx context.Context, topicPartitions []db.PartitionInfo) (map[db.PartitionInfo]int64, error)
	// DeleteMessagesByPartition 删除已消费的消息
	DeleteMessagesByPartition(ctx context.Context, topic string, partition uint, maxOffset int64, retentionDate time.Time, limit int) (int64, error)
	// DeleteMessagesByPartitionUnconsumed 删除未消费的过期消息
	DeleteMessagesByPartitionUnconsumed(ctx context.Context, topic string, partition uint, retentionDate time.Time, limit int) (int64, error)
}

// ConsumerHeartbeatRepo 消费者心跳管理接口
type ConsumerHeartbeatRepo interface {
	// GetConsumerHeartbeat 获取消费者心跳
	GetConsumerHeartbeat(ctx context.Context, groupID, consumerID string) (*db.ConsumerHeartbeat, error)
	// UpsertConsumerHeartbeat 创建或更新消费者心跳
	UpsertConsumerHeartbeat(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error
	// MarkConsumerOffline 标记消费者离线
	MarkConsumerOffline(ctx context.Context, groupID, consumerID string) error
	// DeleteConsumerHeartbeat 删除消费者心跳记录
	DeleteConsumerHeartbeat(ctx context.Context, groupID, consumerID string) error
	// FindActiveConsumers 查找活跃消费者
	FindActiveConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]db.ConsumerHeartbeat, error)
	// FindAllConsumers 查找所有消费者（包括离线）
	FindAllConsumers(ctx context.Context, groupID string, timeout time.Duration) ([]db.ConsumerHeartbeat, error)
}

// ConsumerGroupRepo 消费组管理接口
type ConsumerGroupRepo interface {
	// GetConsumerGroupGeneration 获取消费组代际
	GetConsumerGroupGeneration(ctx context.Context, groupID string) (*db.ConsumerGroupGeneration, error)
	// IncrementAndGetGenerationID 递增并获取代际 ID
	IncrementAndGetGenerationID(ctx context.Context, groupID string) (uint, error)
	// UpdateAssignments 更新分区分配
	UpdateAssignments(ctx context.Context, groupID string, generationID uint, assignments map[string][]db.PartitionInfo) error
	// FindAllActiveGroups 查找活跃消费组
	FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error)
	// FindAllGroups 查找所有消费组
	FindAllGroups(ctx context.Context) ([]string, error)
}

// ConsumerOffsetRepo 消费进度管理接口
type ConsumerOffsetRepo interface {
	// GetCommittedOffsets 获取已提交的消费进度
	GetCommittedOffsets(ctx context.Context, groupID string, partitions []db.PartitionInfo) (db.ConsumerGroupConsumptionProgressSlice, error)
	// CommitOffset 提交单分区消费进度
	CommitOffset(ctx context.Context, groupID string, generationID uint, p db.PartitionInfo, lastConsumedMessageID int64) error
	// BatchCommitLastConsumeMessageID 批量提交消费进度
	BatchCommitLastConsumeMessageID(ctx context.Context, groupID string, generationID uint, consumedIds map[db.PartitionInfo]int64) error
	// BatchCommitOffsetsWithInitialWatermark 批量提交带水位线的消费进度
	BatchCommitOffsetsWithInitialWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[db.PartitionInfo]ConsumptionProgressWithWatermark) error
	// CommitConsumptionProgressWithSubscriptionRegistration 提交消费进度并注册订阅
	CommitConsumptionProgressWithSubscriptionRegistration(ctx context.Context, groupID string, generationID uint, p db.PartitionInfo, lastConsumedMessageID, subscriptionStartWatermark int64) error
	// GetConsumerGroupLowWatermarks 获取消费组低水位线
	GetConsumerGroupLowWatermarks(ctx context.Context) (map[db.PartitionInfo]int64, error)
}

// ManualAssignmentRepo 手动分区分配配置管理接口
type ManualAssignmentRepo interface {
	// CreateManualAssignment 创建手动分区分配配置
	CreateManualAssignment(ctx context.Context, assignment *db.ManualPartitionAssignment) error
	// GetManualAssignmentsByGroup 获取消费组的所有手动分配配置
	GetManualAssignmentsByGroup(ctx context.Context, groupID string) ([]db.ManualPartitionAssignment, error)
	// DeleteManualAssignment 删除手动分区分配配置
	DeleteManualAssignment(ctx context.Context, id int64) error
	// GetManualAssignments 获取消费组的手动分配配置
	// 根据 pattern 匹配 consumerIDs，返回 map[consumerID][]PartitionInfo
	GetManualAssignments(ctx context.Context, groupID string, consumerIDs []string) (map[string][]db.PartitionInfo, error)
}

// Repo 聚合所有子接口的完整仓储接口
type Repo interface {
	// DB 获取底层数据库连接
	DB() DB

	TopicRepo
	MessageRepo
	ConsumerHeartbeatRepo
	ConsumerGroupRepo
	ConsumerOffsetRepo
	ManualAssignmentRepo
}

// 编译时接口实现检查
var _ Repo = (*MqRepo)(nil)
