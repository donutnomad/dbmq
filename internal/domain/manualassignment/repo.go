package manualassignment

import (
	"context"

	"github.com/donutnomad/dbmq/internal/types"
)

// Repo 手动分区分配仓储接口
type Repo interface {
	// Create 创建手动分区分配配置
	Create(ctx context.Context, assignment *Assignment) error
	// GetByGroup 获取消费组的所有手动分配配置
	GetByGroup(ctx context.Context, groupID string) ([]*Assignment, error)
	// Delete 删除手动分区分配配置
	Delete(ctx context.Context, id int64) error
	// GetMatching 获取消费组的手动分配配置
	// 根据 pattern 匹配 consumerIDs，返回 map[consumerID][]PartitionInfo
	GetMatching(ctx context.Context, groupID string, consumerIDs []string) (map[string][]types.PartitionInfo, error)
}
