package manualassignment

import (
	"time"

	"github.com/donutnomad/dbmq/internal/types"
)

// Assignment 手动分区分配领域实体
type Assignment struct {
	ID                int64     // 自增主键
	GroupID           string    // 消费组ID
	ConsumerIDPattern string    // 消费者ID匹配模式
	Topic             string    // Topic名称
	Partition         uint      // 分区号
	CreatedAt         time.Time // 创建时间
	UpdatedAt         time.Time // 更新时间
}

// PartitionInfo 返回分区信息
func (a *Assignment) PartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		Topic:     a.Topic,
		Partition: a.Partition,
	}
}
