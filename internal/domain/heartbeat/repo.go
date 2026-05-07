package heartbeat

import (
	"context"
	"time"
)

// Repo 消费者心跳仓储接口
type Repo interface {
	// Get 获取单个消费者的心跳记录
	Get(ctx context.Context, groupID, consumerID string) (*Heartbeat, error)
	// Upsert 创建或更新消费者心跳
	Upsert(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error
	// MarkOffline 标记消费者为离线状态
	MarkOffline(ctx context.Context, groupID, consumerID string) error
	// Delete 删除消费者心跳记录
	Delete(ctx context.Context, groupID, consumerID string) error
	// DeleteByGroup 删除消费组的所有消费者心跳记录
	DeleteByGroup(ctx context.Context, groupID string) error
	// DeleteExpired 删除 last_heartbeat 早于 before 的心跳记录,limit 限制单次最大删除数量,避免长时间锁表
	DeleteExpired(ctx context.Context, before time.Time, limit int) (int64, error)
	// FindActive 查找活跃消费者
	FindActive(ctx context.Context, groupID string, timeout time.Duration) ([]*Heartbeat, error)
	// FindAll 查找所有消费者（包括离线）
	FindAll(ctx context.Context, groupID string, timeout time.Duration) ([]*Heartbeat, error)
}
