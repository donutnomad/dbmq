package topic

import (
	"time"
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
