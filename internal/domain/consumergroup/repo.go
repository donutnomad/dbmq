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
	// UpdateAssignments 更新分区分配
	UpdateAssignments(ctx context.Context, groupID string, generationID uint, assignments map[string][]types.PartitionInfo) error
	// FindAllActiveGroups 查找活跃消费组
	FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error)
	// FindAllGroups 查找所有消费组
	FindAllGroups(ctx context.Context) ([]string, error)
}
