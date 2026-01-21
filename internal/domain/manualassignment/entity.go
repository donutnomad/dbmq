package manualassignment

import (
	"time"

	"github.com/donutnomad/dbmq/internal/db"
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
func (a *Assignment) PartitionInfo() db.PartitionInfo {
	return db.PartitionInfo{
		Topic:     a.Topic,
		Partition: a.Partition,
	}
}

// FromDB 从数据库模型转换
func FromDB(assignment *db.ManualPartitionAssignment) *Assignment {
	if assignment == nil {
		return nil
	}
	return &Assignment{
		ID:                assignment.ID,
		GroupID:           assignment.GroupID,
		ConsumerIDPattern: assignment.ConsumerIDPattern,
		Topic:             assignment.Topic,
		Partition:         assignment.Partition,
		CreatedAt:         assignment.CreatedAt,
		UpdatedAt:         assignment.UpdatedAt,
	}
}

// FromDBSlice 从数据库模型切片转换
func FromDBSlice(assignments []db.ManualPartitionAssignment) []*Assignment {
	result := make([]*Assignment, len(assignments))
	for i := range assignments {
		result[i] = FromDB(&assignments[i])
	}
	return result
}
