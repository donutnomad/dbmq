package topicrepo

import (
	"encoding/json"
	"time"

	"gorm.io/datatypes"
)

// TopicPO Topic 元数据表
// 系统中最重要的表，存储所有消息数据
// 使用复合索引优化消费查询性能
type TopicPO struct {
	TopicName      string         `gorm:"primaryKey;type:varchar(255);column:topic_name;not null;"` // Topic名称
	PartitionCount uint           `gorm:"type:int unsigned;column:partition_count;not null;"`       // 分区数量，创建后不可修改
	Configs        datatypes.JSON `gorm:"type:json;column:configs;not null;"`                       // comment:'Topic级别配置, e.g. {"retention_ms": 604800000}'
	CreatedAt      time.Time      `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3)"`
}

// GetConfig 从Topic的JSON配置中获取指定配置项的值
// 返回值：配置值（float64类型）和是否找到该配置项的布尔值
func (t TopicPO) GetConfig(key string) (float64, bool) {
	var config map[string]any
	if err := json.Unmarshal(t.Configs, &config); err != nil {
		return 0, false // JSON格式无效
	}
	// 查找指定的配置项
	val, ok := config[key]
	if !ok {
		return 0, false
	}
	// JSON中的数字会被解析为float64类型
	if num, ok := val.(float64); ok {
		return num, true
	}
	return 0, false
}

func (TopicPO) TableName() string {
	return "mq_topics"
}
