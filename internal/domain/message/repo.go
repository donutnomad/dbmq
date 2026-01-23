package message

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/types"
)

// Repo 消息仓储接口
type Repo interface {
	// CreateBatch 批量创建消息
	CreateBatch(ctx context.Context, messages []*Message) error
	// Fetch 获取单分区消息
	Fetch(ctx context.Context, topic string, partition uint, afterID int64, limit int) ([]*Message, error)
	// FetchBatch 批量获取多分区消息
	FetchBatch(ctx context.Context, requests []FetchRequest) ([]*Message, error)
	// GetLatestID 获取单分区最新 ID
	GetLatestID(ctx context.Context, topic string, partition uint) (int64, error)
	// GetLatestIDs 批量获取多分区最新 ID
	GetLatestIDs(ctx context.Context, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error)
	// DeleteConsumed 删除已消费的消息
	DeleteConsumed(ctx context.Context, topic string, partition uint, maxID int64, retentionDate time.Time, limit int) (int64, error)
	// DeleteExpired 删除过期的未消费消息
	DeleteExpired(ctx context.Context, topic string, partition uint, retentionDate time.Time, limit int) (int64, error)
}
