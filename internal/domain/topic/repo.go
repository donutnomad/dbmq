package topic

import (
	"context"
)

// Repo Topic 仓储接口
type Repo interface {
	// Get 获取单个 Topic
	Get(ctx context.Context, topicName string) (*Topic, error)
	// GetAll 获取所有 Topic
	GetAll(ctx context.Context) ([]*Topic, error)
	// FindByNames 批量查找 Topic
	FindByNames(ctx context.Context, topicNames []string) ([]*Topic, error)
}
