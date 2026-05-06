package consumergrouprepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"
	"github.com/donutnomad/dbmq/internal/types"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type mysqlRepo struct {
	db interfaces.DB
}

func New(db interfaces.DB) consumergroup.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) GetGeneration(ctx context.Context, groupID string) (*consumergroup.Generation, error) {
	var gen GenerationPO
	err := r.db.WithContext(ctx).
		Model(&GenerationPO{}).
		Where("`group_id` = ?", groupID).
		First(&gen).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return ToDomain(&gen), nil
}

func (r *mysqlRepo) IncrementGenerationID(ctx context.Context, groupID string) (uint, error) {
	var generationID uint

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sql := `INSERT INTO mq_consumer_group_generations
				(group_id, generation_id, protocol_type, updated_at)
				VALUES (?, 1, 'consumer', ?)
				ON DUPLICATE KEY UPDATE
				generation_id = generation_id + 1,
				updated_at = VALUES(updated_at)`

		if err := tx.Exec(sql, groupID, time.Now()).Error; err != nil {
			return fmt.Errorf("failed to increment generation ID for group %s: %w", groupID, err)
		}

		selectSQL := "SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?"
		if err := tx.Raw(selectSQL, groupID).Scan(&generationID).Error; err != nil {
			return fmt.Errorf("failed to retrieve generation ID for group %s: %w", groupID, err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}
	return generationID, nil
}

func (r *mysqlRepo) IncrementAndUpdateAssignments(ctx context.Context, groupID string, assignments map[string][]types.PartitionInfo) (uint, error) {
	var newGenerationID uint
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Step 1: 原子递增 generation_id 并读取新值
		var err error
		newGenerationID, err = r.IncrementGenerationID(ctx, groupID)
		if err != nil {
			return err
		}

		// Step 2: 在同一事务内更新所有消费者的分区分配
		activeIDs := make([]string, 0, len(assignments))
		for consumerID, partitions := range assignments {
			activeIDs = append(activeIDs, consumerID)
			hbPartitions := make([]heartbeatrepo.PartitionInfo, len(partitions))
			for i, p := range partitions {
				hbPartitions[i] = heartbeatrepo.PartitionInfo{
					Topic:     p.Topic,
					Partition: p.Partition,
				}
			}
			updates := map[string]any{
				"generation_id":       newGenerationID,
				"assigned_partitions": datatypes.NewJSONSlice(hbPartitions),
				"offline":             false,
				"offline_at":          gorm.Expr("NULL"),
			}
			result := tx.Model(&heartbeatrepo.HeartbeatPO{}).
				Where("`group_id` = ?", groupID).
				Where("`consumer_id` = ?", consumerID).
				Updates(updates)
			if result.Error != nil {
				return fmt.Errorf("failed to update assignment for consumer %s: %w", consumerID, result.Error)
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
			}
		}

		// Step 3: 清空非活跃消费者的分区分配
		inactiveQuery := tx.Model(&heartbeatrepo.HeartbeatPO{}).
			Where("`group_id` = ?", groupID)
		if len(activeIDs) > 0 {
			inactiveQuery = inactiveQuery.Where("`consumer_id` NOT IN ?", activeIDs)
		}
		inactiveUpdates := map[string]any{
			"generation_id":       newGenerationID,
			"assigned_partitions": datatypes.NewJSONSlice([]heartbeatrepo.PartitionInfo{}),
		}
		if err := inactiveQuery.Updates(inactiveUpdates).Error; err != nil {
			return fmt.Errorf("failed to clear assignments for inactive consumers: %w", err)
		}

		return nil
	})
	return newGenerationID, err
}

func (r *mysqlRepo) FindAllActiveGroups(ctx context.Context, timeout time.Duration) ([]string, error) {
	var groupIDs []string
	err := r.db.WithContext(ctx).
		Model(&heartbeatrepo.HeartbeatPO{}).
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Distinct("`group_id`").
		Pluck("`group_id`", &groupIDs).Error
	return groupIDs, err
}

func (r *mysqlRepo) FindAllGroups(ctx context.Context) ([]string, error) {
	var groupIDs []string
	sql := `SELECT DISTINCT group_id FROM (SELECT group_id FROM mq_consumer_heartbeats UNION SELECT group_id FROM mq_consumer_group_generations) AS all_groups`
	err := r.db.WithContext(ctx).Raw(sql).Pluck("group_id", &groupIDs).Error
	return groupIDs, err
}

func (r *mysqlRepo) Delete(ctx context.Context, groupID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := heartbeatrepo.New(tx).DeleteByGroup(ctx, groupID); err != nil {
			return err
		}
		if err := consumerprogressrepo.New(tx).DeleteByGroup(ctx, groupID); err != nil {
			return err
		}
		if err := manualassignmentrepo.New(tx).DeleteByGroup(ctx, groupID); err != nil {
			return err
		}
		return tx.Where("`group_id` = ?", groupID).Delete(&GenerationPO{}).Error
	})
}

// 编译时接口实现检查
var _ consumergroup.Repo = (*mysqlRepo)(nil)
