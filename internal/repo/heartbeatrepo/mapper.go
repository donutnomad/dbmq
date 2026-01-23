package heartbeatrepo

import (
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/types"
	"gorm.io/datatypes"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(hb *HeartbeatPO) *heartbeat.Heartbeat {
	if hb == nil {
		return nil
	}
	// 将 heartbeatrepo.PartitionInfo 转换为 types.PartitionInfo
	assignedPartitions := make([]types.PartitionInfo, len(hb.AssignedPartitions))
	for i, p := range hb.AssignedPartitions {
		assignedPartitions[i] = types.PartitionInfo{
			Topic:     p.Topic,
			Partition: p.Partition,
		}
	}
	return &heartbeat.Heartbeat{
		GroupID:            hb.GroupID,
		ConsumerID:         hb.ConsumerID,
		GenerationID:       hb.GenerationID,
		SubscribedTopics:   hb.SubscribedTopics,
		AssignedPartitions: assignedPartitions,
		Offline:            hb.Offline,
		LastHeartbeat:      hb.LastHeartbeat,
		OfflineAt:          hb.OfflineAt,
	}
}

// ToDomainSlice 从数据库模型切片转换为领域实体切片
func ToDomainSlice(hbs []HeartbeatPO) []*heartbeat.Heartbeat {
	result := make([]*heartbeat.Heartbeat, len(hbs))
	for i := range hbs {
		result[i] = ToDomain(&hbs[i])
	}
	return result
}

// ToPO 从领域实体转换为数据库模型
func ToPO(h *heartbeat.Heartbeat) *HeartbeatPO {
	if h == nil {
		return nil
	}
	// 将 types.PartitionInfo 转换为 heartbeatrepo.PartitionInfo
	assignedPartitions := make([]PartitionInfo, len(h.AssignedPartitions))
	for i, p := range h.AssignedPartitions {
		assignedPartitions[i] = PartitionInfo{
			Topic:     p.Topic,
			Partition: p.Partition,
		}
	}
	return &HeartbeatPO{
		GroupID:            h.GroupID,
		ConsumerID:         h.ConsumerID,
		GenerationID:       h.GenerationID,
		SubscribedTopics:   h.SubscribedTopics,
		AssignedPartitions: datatypes.JSONSlice[PartitionInfo](assignedPartitions),
		Offline:            h.Offline,
		LastHeartbeat:      h.LastHeartbeat,
		OfflineAt:          h.OfflineAt,
	}
}
