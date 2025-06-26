package dal

import (
	"context"
	"github.com/donutnomad/dbmq/types"
)

// FindTopicsByNames 查找所有匹配给定名称的Topic
func (d *MqDao) FindTopicsByNames(ctx context.Context, topicNames []string) ([]types.Topic, error) {
	if len(topicNames) == 0 {
		return nil, nil
	}
	var topics []types.Topic
	err := d.db.WithContext(ctx).
		Model(&types.Topic{}).
		Where("`topic_name` IN (?)", topicNames).
		Find(&topics).Error
	return topics, err
}

// GetAllTopics retrieves all topics from the database.
func (d *MqDao) GetAllTopics(ctx context.Context) ([]types.Topic, error) {
	var topics []types.Topic
	err := d.db.WithContext(ctx).Find(&topics).Error
	return topics, err
}
