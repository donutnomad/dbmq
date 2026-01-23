package consumergrouprepo

import (
	"time"
)

// GenerationPO 消费组代际与元数据表
// 存储消费组的代际信息，是重新均衡机制的核心
// 每次重新均衡时代际ID递增，用于隔离不同代际的消费者
type GenerationPO struct {
	GroupID      string    `gorm:"primaryKey;type:varchar(255);column:group_id;not null;"` // 消费组ID
	GenerationID uint      `gorm:"type:int unsigned;column:generation_id;not null;"`       // 代际ID, 每次再均衡时加一
	ProtocolType string    `gorm:"type:varchar(50);column:protocol_type;not null;default:'consumer'"`
	LeaderID     string    `gorm:"type:varchar(255);column:leader_id;not null;default:''"`
	UpdatedAt    time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"`
}

func (GenerationPO) TableName() string {
	return "mq_consumer_group_generations"
}
