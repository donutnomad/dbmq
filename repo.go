package dbmq

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// Waiter 等待接口，封装等待通知或超时的逻辑
type Waiter interface {
	// Wait 等待通知到达或超时，返回的 channel 在完成时收到信号
	Wait(ctx context.Context, timeout time.Duration) <-chan struct{}
}

// Notifier 抽象消息通知接口
// 用于将 Redis PubSub 等通知机制与消费者解耦，便于测试
type Notifier interface {
	// Subscribe 订阅分区通知
	Subscribe(partitions []db.PartitionInfo) error
	// Unsubscribe 取消订阅
	Unsubscribe(partitions []db.PartitionInfo) error
	// Wait 等待通知到达或超时
	// 返回的 channel 在有通知、超时或 ctx 取消时会收到信号
	Wait(ctx context.Context, timeout time.Duration) <-chan struct{}
	// Close 关闭 notifier
	Close() error
	// IsHealthy 返回连接是否健康
	IsHealthy() bool
}
