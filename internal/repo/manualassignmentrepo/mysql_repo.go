package manualassignmentrepo

import (
	"context"
	"errors"
	"strings"

	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/types"
)

// ErrNotFound 记录未找到错误
var ErrNotFound = errors.New("record not found")

type mysqlRepo struct {
	db interfaces.DB
}

// New 创建 MySQL 实现的手动分配仓储
func New(db interfaces.DB) manualassignment.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) Create(ctx context.Context, assignment *manualassignment.Assignment) error {
	dbAssignment := &AssignmentPO{
		GroupID:           assignment.GroupID,
		ConsumerIDPattern: assignment.ConsumerIDPattern,
		Topic:             assignment.Topic,
		Partition:         assignment.Partition,
	}
	err := r.db.WithContext(ctx).Create(dbAssignment).Error
	if err != nil {
		return err
	}
	assignment.ID = dbAssignment.ID
	assignment.CreatedAt = dbAssignment.CreatedAt
	assignment.UpdatedAt = dbAssignment.UpdatedAt
	return nil
}

func (r *mysqlRepo) GetByGroup(ctx context.Context, groupID string) ([]*manualassignment.Assignment, error) {
	var assignments []AssignmentPO
	err := r.db.WithContext(ctx).
		Model(&AssignmentPO{}).
		Where("`group_id` = ?", groupID).
		Order("id ASC").
		Find(&assignments).Error
	if err != nil {
		return nil, err
	}
	return ToDomainSlice(assignments), nil
}

func (r *mysqlRepo) Delete(ctx context.Context, id int64) error {
	result := r.db.WithContext(ctx).Delete(&AssignmentPO{}, id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *mysqlRepo) GetMatching(ctx context.Context, groupID string, consumerIDs []string) (map[string][]types.PartitionInfo, error) {
	if len(consumerIDs) == 0 {
		return nil, nil
	}

	var assignments []AssignmentPO
	err := r.db.WithContext(ctx).
		Model(&AssignmentPO{}).
		Where("`group_id` = ?", groupID).
		Find(&assignments).Error
	if err != nil {
		return nil, err
	}

	if len(assignments) == 0 {
		return nil, nil
	}

	result := make(map[string][]types.PartitionInfo)

	for _, assignment := range assignments {
		pattern := assignment.ConsumerIDPattern
		partition := types.PartitionInfo{
			Topic:     assignment.Topic,
			Partition: assignment.Partition,
		}

		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			for _, consumerID := range consumerIDs {
				if strings.HasPrefix(consumerID, prefix) {
					result[consumerID] = append(result[consumerID], partition)
				}
			}
		} else {
			for _, consumerID := range consumerIDs {
				if consumerID == pattern {
					result[consumerID] = append(result[consumerID], partition)
				}
			}
		}
	}

	if len(result) == 0 {
		return nil, nil
	}

	return result, nil
}

// 编译时接口实现检查
var _ manualassignment.Repo = (*mysqlRepo)(nil)
