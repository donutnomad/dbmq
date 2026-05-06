package consumerprogressrepo

import (
	"context"
	"strings"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/types"

	"gorm.io/gorm"
)

type mysqlRepo struct {
	db interfaces.DB
}

// New 创建 MySQL 实现的消费进度仓储
func New(db interfaces.DB) consumerprogress.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) GetCommittedOffsets(ctx context.Context, groupID string, partitions []types.PartitionInfo) ([]*consumerprogress.Progress, error) {
	if len(partitions) == 0 {
		return nil, nil
	}

	var progressRecords []ProgressPO

	var conditions []string
	var args []any
	args = append(args, groupID)

	for _, p := range partitions {
		conditions = append(conditions, "(`topic` = ? AND `partition` = ?)")
		args = append(args, p.Topic, p.Partition)
	}

	whereClause := "`group_id` = ? AND (" + strings.Join(conditions, " OR ") + ")"

	err := r.db.WithContext(ctx).
		Model(&ProgressPO{}).
		Where(whereClause, args...).
		Find(&progressRecords).Error

	if err != nil {
		return nil, err
	}
	return ToDomainSlice(progressRecords), nil
}

func (r *mysqlRepo) CommitOffset(ctx context.Context, groupID string, generationID uint, p types.PartitionInfo, lastConsumedMessageID int64) error {
	sql := "INSERT INTO " + ProgressPO{}.TableName() + " (group_id, topic, " + "`partition`" + `, last_consumed_message_id, generation_id, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			last_consumed_message_id = IF(VALUES(generation_id) >= generation_id, VALUES(last_consumed_message_id), last_consumed_message_id),
			generation_id = IF(VALUES(generation_id) >= generation_id, VALUES(generation_id), generation_id),
			updated_at = IF(VALUES(generation_id) >= generation_id, VALUES(updated_at), updated_at)
`
	return r.db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, lastConsumedMessageID, generationID, time.Now()).Error
}

func (r *mysqlRepo) BatchCommitOffsets(ctx context.Context, groupID string, generationID uint, consumedIDs map[types.PartitionInfo]int64) error {
	if len(consumedIDs) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		repo := &mysqlRepo{db: tx}
		for p, offset := range consumedIDs {
			if err := repo.CommitOffset(ctx, groupID, generationID, p, offset); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *mysqlRepo) BatchCommitOffsetsWithWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[types.PartitionInfo]consumerprogress.ProgressWithWatermark) error {
	if len(progressWithWatermarks) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		repo := &mysqlRepo{db: tx}
		for p, progressData := range progressWithWatermarks {
			if err := repo.CommitWithSubscriptionRegistration(ctx, groupID, generationID, p, progressData.LastConsumedMessageID, progressData.SubscriptionStartWatermark); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *mysqlRepo) CommitWithSubscriptionRegistration(ctx context.Context, groupID string, generationID uint, p types.PartitionInfo, lastConsumedMessageID, subscriptionStartWatermark int64) error {
	now := time.Now()
	sql := `INSERT INTO mq_consumer_group_consumption_progress (
	group_id,
	topic,
	` + "`partition`" + `,
	last_consumed_message_id,
	subscription_registered_at,
	subscription_start_watermark,
	generation_id,
	updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
 	last_consumed_message_id = IF(VALUES(generation_id) >= generation_id, VALUES(last_consumed_message_id), last_consumed_message_id),
	subscription_start_watermark = IF(subscription_start_watermark IS NULL AND VALUES(generation_id) >= generation_id, VALUES(subscription_start_watermark), subscription_start_watermark),
	generation_id = IF(VALUES(generation_id) >= generation_id, VALUES(generation_id), generation_id),
	updated_at = IF(VALUES(generation_id) >= generation_id, VALUES(updated_at), updated_at)
`
	return r.db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, lastConsumedMessageID, now, subscriptionStartWatermark, generationID, now).Error
}

type lowWatermark struct {
	Topic              string `gorm:"column:topic"`
	Partition          uint   `gorm:"column:partition"`
	LowWatermarkOffset int64  `gorm:"column:low_watermark"`
}

func (r *mysqlRepo) GetLowWatermarks(ctx context.Context) (map[types.PartitionInfo]int64, error) {
	var results []lowWatermark
	err := r.db.WithContext(ctx).Model(&ProgressPO{}).
		Select("topic, `partition`, MIN(last_consumed_message_id) as low_watermark").
		Group("topic, `partition`").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	watermarks := make(map[types.PartitionInfo]int64, len(results))
	for _, res := range results {
		p := types.PartitionInfo{Topic: res.Topic, Partition: res.Partition}
		watermarks[p] = res.LowWatermarkOffset
	}

	return watermarks, nil
}

func (r *mysqlRepo) DeleteByGroup(ctx context.Context, groupID string) error {
	return r.db.WithContext(ctx).
		Where("`group_id` = ?", groupID).
		Delete(&ProgressPO{}).Error
}

// 编译时接口实现检查
var _ consumerprogress.Repo = (*mysqlRepo)(nil)
