package message

import (
	"time"

	"github.com/donutnomad/dbmq/internal/types"
	"gorm.io/datatypes"
)

// Message 消息领域实体
type Message struct {
	ID         int64             // 全局唯一ID
	Topic      string            // Topic名称
	Partition  uint              // 分区号
	MessageKey string            // 消息的业务Key
	Headers    map[string]string // 消息头
	Body       datatypes.JSON    // 消息体
	CreatedAt  time.Time         // 创建时间
}

// PartitionInfo 返回分区信息
func (m *Message) PartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		Topic:     m.Topic,
		Partition: m.Partition,
	}
}

// FetchRequest 表示单个分区的获取请求
type FetchRequest struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
	AfterID   int64  // 获取此ID之后的消息
	Limit     int    // 获取限制
}
