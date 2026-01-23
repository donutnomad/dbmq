package manualassignmentrepo

import (
	"time"
)

// AssignmentPO 手动分区分配配置
// 用于覆盖默认的自动分区分配策略，支持将特定分区固定分配给特定消费者
type AssignmentPO struct {
	ID                int64     `gorm:"primaryKey;column:id;autoIncrement"`                                                                      // 自增主键
	GroupID           string    `gorm:"type:varchar(255);column:group_id;not null;uniqueIndex:idx_unique_assignment,priority:1"`                 // 消费组ID
	ConsumerIDPattern string    `gorm:"type:varchar(255);column:consumer_id_pattern;not null;uniqueIndex:idx_unique_assignment,priority:2"`      // 消费者ID匹配模式
	Topic             string    `gorm:"type:varchar(255);column:topic;not null;uniqueIndex:idx_unique_assignment,priority:3"`                    // Topic名称
	Partition         uint      `gorm:"type:int unsigned;column:partition;not null;uniqueIndex:idx_unique_assignment,priority:4"`                // 分区号
	CreatedAt         time.Time `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3)"`                               // 创建时间
	UpdatedAt         time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"` // 更新时间
}

func (AssignmentPO) TableName() string {
	return "mq_manual_partition_assignments"
}
