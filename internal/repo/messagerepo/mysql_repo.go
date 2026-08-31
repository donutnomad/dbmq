package messagerepo

import (
	"context"
	"strings"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/types"
)

type mysqlRepo struct {
	db interfaces.DB
}

const consumePullIndexHint = " FORCE INDEX (`idx_consume_pull`)"

func consumePullQuery() string {
	return "SELECT * FROM " + MessagePO{}.TableName() + consumePullIndexHint + " WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
}

// New 创建 MySQL 实现的消息仓储
func New(db interfaces.DB) message.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) CreateBatch(ctx context.Context, messages []*message.Message) error {
	if len(messages) == 0 {
		return nil
	}
	pos := ToPOSlice(messages)
	for _, po := range pos {
		po.Fix()
	}
	if err := r.db.WithContext(ctx).CreateInBatches(pos, 100).Error; err != nil {
		return err
	}
	for i, po := range pos {
		messages[i].ID = po.ID
	}
	return nil
}

func (r *mysqlRepo) Fetch(ctx context.Context, topic string, partition uint, afterID int64, limit int) ([]*message.Message, error) {
	var messages []MessagePO
	err := r.db.WithContext(ctx).
		Raw(consumePullQuery(), topic, partition, afterID, limit).
		Scan(&messages).Error
	if err != nil {
		return nil, err
	}
	return ToDomainSlice(messages), nil
}

func (r *mysqlRepo) FetchBatch(ctx context.Context, requests []message.FetchRequest) ([]*message.Message, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	query := consumePullQuery()

	var ret []*message.Message
	for _, req := range requests {
		var messages []MessagePO
		err := r.db.WithContext(ctx).
			Raw(query, req.Topic, req.Partition, req.AfterID, req.Limit).
			Scan(&messages).Error
		if err != nil {
			return nil, err
		}
		ret = append(ret, ToDomainSlice(messages)...)
	}

	return ret, nil
}

func (r *mysqlRepo) GetLatestID(ctx context.Context, topic string, partition uint) (int64, error) {
	var id int64
	err := r.db.WithContext(ctx).
		Model(&MessagePO{}).
		Select("COALESCE(MAX(`id`), 0)").
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Scan(&id).Error
	return id, err
}

type topicPartitionOffset struct {
	Topic     string `gorm:"column:topic"`
	Partition uint   `gorm:"column:partition"`
	MaxID     int64  `gorm:"column:max_id"`
}

func (r *mysqlRepo) GetLatestIDs(ctx context.Context, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	if len(partitions) == 0 {
		return map[types.PartitionInfo]int64{}, nil
	}

	var inArgs []any
	placeholders := make([]string, len(partitions))
	for i, tp := range partitions {
		placeholders[i] = "(?, ?)"
		inArgs = append(inArgs, tp.Topic, tp.Partition)
	}

	var results []topicPartitionOffset
	query := r.db.WithContext(ctx).
		Model(&MessagePO{}).
		Select("topic, `partition`, COALESCE(MAX(`id`), 0) AS max_id").
		Where("(topic, `partition`) IN ("+strings.Join(placeholders, ", ")+")", inArgs...).
		Group("topic, `partition`").
		Find(&results)

	if query.Error != nil {
		return nil, query.Error
	}

	latestIDs := make(map[types.PartitionInfo]int64)
	for _, res := range results {
		latestIDs[types.PartitionInfo{Topic: res.Topic, Partition: res.Partition}] = res.MaxID
	}

	return latestIDs, nil
}

func (r *mysqlRepo) DeleteConsumed(ctx context.Context, topic string, partition uint, maxID int64, retentionDate time.Time, limit int) (int64, error) {
	result := r.db.WithContext(ctx).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`id` < ?", maxID).
		Where("`created_at` < ?", retentionDate).
		Limit(limit).
		Delete(&MessagePO{})
	return result.RowsAffected, result.Error
}

func (r *mysqlRepo) DeleteExpired(ctx context.Context, topic string, partition uint, retentionDate time.Time, limit int) (int64, error) {
	result := r.db.WithContext(ctx).
		Where("`topic` = ?", topic).
		Where("`partition` = ?", partition).
		Where("`created_at` < ?", retentionDate).
		Limit(limit).
		Delete(&MessagePO{})
	return result.RowsAffected, result.Error
}

// 编译时接口实现检查
var _ message.Repo = (*mysqlRepo)(nil)
