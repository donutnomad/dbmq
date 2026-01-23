package topicrepo

import (
	"encoding/json"

	"github.com/donutnomad/dbmq/internal/domain/topic"
	"gorm.io/datatypes"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(t *TopicPO) *topic.Topic {
	if t == nil {
		return nil
	}
	var configs map[string]any
	_ = json.Unmarshal(t.Configs, &configs)
	return &topic.Topic{
		Name:           t.TopicName,
		PartitionCount: t.PartitionCount,
		Configs:        configs,
		CreatedAt:      t.CreatedAt,
	}
}

// ToDomainSlice 从数据库模型切片转换为领域实体切片
func ToDomainSlice(topics []TopicPO) []*topic.Topic {
	result := make([]*topic.Topic, len(topics))
	for i := range topics {
		result[i] = ToDomain(&topics[i])
	}
	return result
}

// ToPO 从领域实体转换为数据库模型
func ToPO(t *topic.Topic) *TopicPO {
	if t == nil {
		return nil
	}
	var configs datatypes.JSON
	if t.Configs != nil {
		configs, _ = json.Marshal(t.Configs)
	}
	return &TopicPO{
		TopicName:      t.Name,
		PartitionCount: t.PartitionCount,
		Configs:        configs,
		CreatedAt:      t.CreatedAt,
	}
}
