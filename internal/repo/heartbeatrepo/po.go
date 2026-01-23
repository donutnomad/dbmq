package heartbeatrepo

import (
	"time"

	"gorm.io/datatypes"
)

// PartitionInfo 唯一标识一个Topic-分区对
type PartitionInfo struct {
	Topic     string `json:"Topic"`     // Topic名称
	Partition uint   `json:"Partition"` // 分区号
}

// HeartbeatPO 消费者心跳与分区分配表
// 存储消费者心跳、分区分配和订阅信息
// 协调器通过此表判断消费者存活状态和进行分区分配
// last_heartbeat索引是性能关键，用于快速找到超时的消费者
// offline字段用于标识消费者是否已主动下线，避免删除历史记录
type HeartbeatPO struct {
	GroupID            string                             `gorm:"primaryKey;type:varchar(255);column:group_id;not null"`
	ConsumerID         string                             `gorm:"primaryKey;type:varchar(255);column:consumer_id;not null;"`          // 消费者唯一ID (e.g., UUID)
	GenerationID       uint                               `gorm:"type:int unsigned;column:generation_id;not null;"`                   // 消费者当前所属的代际ID
	SubscribedTopics   datatypes.JSONSlice[string]        `gorm:"type:json;column:subscribed_topics;not null;"`                       // 订阅的Topic列表
	AssignedPartitions datatypes.JSONSlice[PartitionInfo] `gorm:"type:json;column:assigned_partitions;not null;"`                     // 被分配的分区
	Offline            bool                               `gorm:"column:;not null;default:false;index:idx_offline_status,priority:1"` // 是否已下线：true=主动下线，false=在线或超时
	LastHeartbeat      time.Time                          `gorm:"column:last_heartbeat;type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index:idx_last_heartbeat;index:idx_offline_status,priority:2"`
	OfflineAt          *time.Time                         `gorm:"column:offline_at;type:timestamp(3);"` // 下线时间，仅当offline=true时有效
}

func (HeartbeatPO) TableName() string {
	return "mq_consumer_heartbeats"
}
