package dbmq

import (
	"context"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/repo"
)

type ConsumerRepo interface {
	// UpsertConsumerHeartbeat 原子性地创建或更新消费者的心跳
	// 更新最后心跳时间并确保消费者的订阅Topic是最新的
	UpsertConsumerHeartbeat(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error

	// GetConsumerHeartbeat 获取单个消费者的心跳记录
	// 包含了消费者的分区分配、订阅信息和最后心跳时间
	GetConsumerHeartbeat(ctx context.Context, groupID, consumerID string) (*db.ConsumerHeartbeat, error)

	// MarkConsumerOffline 标记消费者为离线状态
	// 用于优雅关闭，保留历史记录但标记为已下线
	MarkConsumerOffline(ctx context.Context, groupID, consumerID string) error

	// GetCommittedOffsets 获取消费组对一组分区的消费进度
	// 返回PartitionInfo到最后消费的消息ID的映射
	GetCommittedOffsets(ctx context.Context, groupID string, partitions []db.PartitionInfo) (db.ConsumerGroupConsumptionProgressSlice, error)

	// BatchCommitOffsetsWithInitialWatermark 在单个事务中为消费组提交一批消费进度，同时设置初始水位线
	// 用于首次消费分区时，记录初始水位线以区分消费策略
	BatchCommitOffsetsWithInitialWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[db.PartitionInfo]repo.ConsumptionProgressWithWatermark) error

	// BatchCommitLastConsumeMessageID 在单个事务中为消费组提交一批消息消费进度
	// 这确保了消费进度提交的原子性，要么全部成功要么全部失败
	BatchCommitLastConsumeMessageID(ctx context.Context, groupID string, generationID uint, consumedIds map[db.PartitionInfo]int64) error

	// GetTopicsLatestIDsByPartitions 获取指定分区的最新消息ID
	// 用于实现 ConsumeFromLatest 策略
	GetTopicsLatestIDsByPartitions(ctx context.Context, topicPartitions []db.PartitionInfo) (map[db.PartitionInfo]int64, error)

	// FetchMessagesBatch 批量从多个分区获取消息
	// 每个分区有一个ID，会查询返回大于这个ID的消息
	FetchMessagesBatch(ctx context.Context, requests []repo.PartitionRequest) ([]db.Message, error)
}

var _ ConsumerRepo = (*repo.MqRepo)(nil)
