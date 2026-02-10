package heartbeatrepo

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/interfaces"
	pkgerrors "github.com/pkg/errors"
	"github.com/samber/lo"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type mysqlRepo struct {
	db interfaces.DB
}

// New 创建 MySQL 实现的心跳仓储
func New(db interfaces.DB) heartbeat.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) Get(ctx context.Context, groupID, consumerID string) (*heartbeat.Heartbeat, error) {
	var hb HeartbeatPO
	err := r.db.WithContext(ctx).
		Model(&HeartbeatPO{}).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		First(&hb).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return ToDomain(&hb), nil
}

func (r *mysqlRepo) Upsert(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error {
	sql := "INSERT INTO " + HeartbeatPO{}.TableName() + ` (group_id, consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at)
	VALUES (?, ?, 0, ?, ?, FALSE, ?, NULL)
	ON DUPLICATE KEY UPDATE
		subscribed_topics = VALUES(subscribed_topics),
		last_heartbeat = VALUES(last_heartbeat),
		offline = FALSE,
		offline_at = NULL,
		generation_id = generation_id
`
	err := r.db.WithContext(ctx).Exec(sql,
		groupID,
		consumerID,
		datatypes.NewJSONSlice(subscribedTopics),
		datatypes.NewJSONSlice([]PartitionInfo{}),
		time.Now(),
	).Error

	if err != nil {
		// 添加错误日志
		return pkgerrors.Wrapf(err, "upsert heartbeat failed for group=%s consumer=%s", groupID, consumerID)
	}

	return nil
}

func (r *mysqlRepo) MarkOffline(ctx context.Context, groupID, consumerID string) error {
	now := time.Now()
	return r.db.WithContext(ctx).
		Model(&HeartbeatPO{}).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		Updates(map[string]any{
			"offline":    true,
			"offline_at": now,
		}).Error
}

func (r *mysqlRepo) Delete(ctx context.Context, groupID, consumerID string) error {
	return r.db.WithContext(ctx).
		Where("`group_id` = ?", groupID).
		Where("`consumer_id` = ?", consumerID).
		Delete(&HeartbeatPO{}).Error
}

func (r *mysqlRepo) FindActive(ctx context.Context, groupID string, timeout time.Duration) ([]*heartbeat.Heartbeat, error) {
	var activeConsumers []HeartbeatPO
	err := r.db.WithContext(ctx).
		Model(&HeartbeatPO{}).
		Where("`group_id` = ?", groupID).
		Where("`offline` = FALSE").
		Where("`last_heartbeat` > ?", time.Now().Add(-timeout)).
		Find(&activeConsumers).Error
	if err != nil {
		return nil, err
	}
	return ToDomainSlice(activeConsumers), nil
}

func (r *mysqlRepo) FindAll(ctx context.Context, groupID string, timeout time.Duration) ([]*heartbeat.Heartbeat, error) {
	var allConsumers []HeartbeatPO
	err := r.db.WithContext(ctx).
		Model(&HeartbeatPO{}).
		Where("`group_id` = ?", groupID).
		Order("`last_heartbeat` DESC").
		Find(&allConsumers).Error
	if err != nil {
		return nil, err
	}

	cutoffTime := time.Now().Add(-timeout)
	for i, consumer := range allConsumers {
		if !consumer.Offline && consumer.LastHeartbeat.Before(cutoffTime) {
			allConsumers[i].Offline = true
			allConsumers[i].OfflineAt = lo.ToPtr(time.Now())
		}
	}

	return ToDomainSlice(allConsumers), nil
}

// 编译时接口实现检查
var _ heartbeat.Repo = (*mysqlRepo)(nil)
