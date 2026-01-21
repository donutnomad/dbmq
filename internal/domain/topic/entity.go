package topic

import (
	"encoding/json"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// Topic 领域实体
type Topic struct {
	Name           string         // Topic名称
	PartitionCount uint           // 分区数量
	Configs        map[string]any // Topic配置
	CreatedAt      time.Time      // 创建时间
}

// GetConfig 获取指定配置项的值
func (t *Topic) GetConfig(key string) (float64, bool) {
	if t.Configs == nil {
		return 0, false
	}
	val, ok := t.Configs[key]
	if !ok {
		return 0, false
	}
	if num, ok := val.(float64); ok {
		return num, true
	}
	return 0, false
}

// FromDB 从数据库模型转换
func FromDB(topic *db.Topic) *Topic {
	if topic == nil {
		return nil
	}
	var configs map[string]any
	_ = json.Unmarshal(topic.Configs, &configs)
	return &Topic{
		Name:           topic.TopicName,
		PartitionCount: topic.PartitionCount,
		Configs:        configs,
		CreatedAt:      topic.CreatedAt,
	}
}

// FromDBSlice 从数据库模型切片转换
func FromDBSlice(topics []db.Topic) []*Topic {
	result := make([]*Topic, len(topics))
	for i := range topics {
		result[i] = FromDB(&topics[i])
	}
	return result
}
