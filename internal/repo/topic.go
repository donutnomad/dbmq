package repo

import (
	"context"
	"errors"
	"github.com/donutnomad/dbmq/internal/db"

	"gorm.io/gorm"
)

// FindTopicsByNames 查找所有匹配给定名称的Topic
func (d *MqRepo) FindTopicsByNames(ctx context.Context, topicNames []string) ([]db.Topic, error) {
	if len(topicNames) == 0 {
		return nil, nil
	}
	var topics []db.Topic
	err := d.db.WithContext(ctx).
		Model(&db.Topic{}).
		Where("`topic_name` IN (?)", topicNames).
		Find(&topics).Error
	return topics, err
}

// GetAllTopics retrieves all topics from the database.
func (d *MqRepo) GetAllTopics(ctx context.Context) ([]db.Topic, error) {
	var topics []db.Topic
	err := d.db.WithContext(ctx).Find(&topics).Error
	return topics, err
}

func (d *MqRepo) GetTopic(ctx context.Context, topicName string) (*db.Topic, error) {
	var topic db.Topic
	err := d.db.WithContext(ctx).
		Where("topic_name = ?", topicName).
		First(&topic).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = nil
		}
		return nil, err
	}
	return &topic, nil
}
