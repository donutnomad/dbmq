package consumerprogress

import (
	"context"

	"github.com/donutnomad/dbmq/internal/types"
)

// Repo 消费进度仓储接口
type Repo interface {
	// GetCommittedOffsets 获取已提交的消费进度
	GetCommittedOffsets(ctx context.Context, groupID string, partitions []types.PartitionInfo) ([]*Progress, error)
	// CommitOffset 提交单分区消费进度
	CommitOffset(ctx context.Context, groupID string, generationID uint, partition types.PartitionInfo, lastConsumedMessageID int64) error
	// BatchCommitOffsets 批量提交消费进度
	BatchCommitOffsets(ctx context.Context, groupID string, generationID uint, consumedIDs map[types.PartitionInfo]int64) error
	// BatchCommitOffsetsWithWatermark 批量提交带水位线的消费进度
	BatchCommitOffsetsWithWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[types.PartitionInfo]ProgressWithWatermark) error
	// CommitWithSubscriptionRegistration 提交消费进度并注册订阅
	CommitWithSubscriptionRegistration(ctx context.Context, groupID string, generationID uint, partition types.PartitionInfo, lastConsumedMessageID, subscriptionStartWatermark int64) error
	// GetLowWatermarks 获取消费组低水位线
	GetLowWatermarks(ctx context.Context) (map[types.PartitionInfo]int64, error)
}
