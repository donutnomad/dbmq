package topicrepo

import (
	"context"
	"errors"

	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/interfaces"

	"gorm.io/gorm"
)

type mysqlRepo struct {
	db interfaces.DB
}

// New 创建 MySQL 实现的 Topic 仓储
func New(db interfaces.DB) topic.Repo {
	return &mysqlRepo{db: db}
}

func (r *mysqlRepo) Get(ctx context.Context, topicName string) (*topic.Topic, error) {
	var t TopicPO
	err := r.db.WithContext(ctx).
		Where("topic_name = ?", topicName).
		First(&t).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return ToDomain(&t), nil
}

func (r *mysqlRepo) GetAll(ctx context.Context) ([]*topic.Topic, error) {
	var topics []TopicPO
	err := r.db.WithContext(ctx).Find(&topics).Error
	if err != nil {
		return nil, err
	}
	return ToDomainSlice(topics), nil
}

func (r *mysqlRepo) FindByNames(ctx context.Context, topicNames []string) ([]*topic.Topic, error) {
	if len(topicNames) == 0 {
		return nil, nil
	}
	var topics []TopicPO
	err := r.db.WithContext(ctx).
		Model(&TopicPO{}).
		Where("`topic_name` IN (?)", topicNames).
		Find(&topics).Error
	if err != nil {
		return nil, err
	}
	return ToDomainSlice(topics), nil
}

// 编译时接口实现检查
var _ topic.Repo = (*mysqlRepo)(nil)
