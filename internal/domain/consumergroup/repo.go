package consumergroup

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/types"
)

// Repo 消费组仓储接口
type Repo interface {
	// GetGeneration 获取消费组代际
	GetGeneration(ctx context.Context, groupID string) (*Generation, error)
	// IncrementGenerationID 递增并获取代际 ID
	IncrementGenerationID(ctx context.Context, groupID string) (uint, error)
	// IncrementAndUpdateAssignments 原子地递增代际 ID 并更新分区分配。
	// 将 IncrementGenerationID 和 UpdateAssignments 合并在同一事务中，
	// 消除两步操作之间的竞态窗口（消费者心跳可能读到已增加的 generation 但尚未更新的分区）。
	IncrementAndUpdateAssignments(ctx context.Context, groupID string, assignments map[string][]types.PartitionInfo) (newGenerationID uint, err error)
	// FindAllActiveGroups 查找活跃消费组
	FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error)
	// FindAllGroups 查找所有消费组
	FindAllGroups(ctx context.Context) ([]string, error)
}
