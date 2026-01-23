package heartbeat

import (
	"time"

	"github.com/donutnomad/dbmq/internal/types"
)

// Heartbeat 消费者心跳领域实体
type Heartbeat struct {
	GroupID            string                // 消费组ID
	ConsumerID         string                // 消费者唯一ID
	GenerationID       uint                  // 消费者当前所属的代际ID
	SubscribedTopics   []string              // 订阅的Topic列表
	AssignedPartitions []types.PartitionInfo // 被分配的分区
	Offline            bool                  // 是否已下线
	LastHeartbeat      time.Time             // 最后心跳时间
	OfflineAt          *time.Time            // 下线时间
}

// IsActive 判断消费者是否活跃
func (h *Heartbeat) IsActive(timeout time.Duration) bool {
	if h.Offline {
		return false
	}
	return time.Since(h.LastHeartbeat) <= timeout
}
