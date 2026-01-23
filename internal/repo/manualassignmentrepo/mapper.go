package manualassignmentrepo

import (
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(a *AssignmentPO) *manualassignment.Assignment {
	if a == nil {
		return nil
	}
	return &manualassignment.Assignment{
		ID:                a.ID,
		GroupID:           a.GroupID,
		ConsumerIDPattern: a.ConsumerIDPattern,
		Topic:             a.Topic,
		Partition:         a.Partition,
		CreatedAt:         a.CreatedAt,
		UpdatedAt:         a.UpdatedAt,
	}
}

// ToDomainSlice 从数据库模型切片转换为领域实体切片
func ToDomainSlice(assignments []AssignmentPO) []*manualassignment.Assignment {
	result := make([]*manualassignment.Assignment, len(assignments))
	for i := range assignments {
		result[i] = ToDomain(&assignments[i])
	}
	return result
}

// ToPO 从领域实体转换为数据库模型
func ToPO(a *manualassignment.Assignment) *AssignmentPO {
	if a == nil {
		return nil
	}
	return &AssignmentPO{
		ID:                a.ID,
		GroupID:           a.GroupID,
		ConsumerIDPattern: a.ConsumerIDPattern,
		Topic:             a.Topic,
		Partition:         a.Partition,
		CreatedAt:         a.CreatedAt,
		UpdatedAt:         a.UpdatedAt,
	}
}
