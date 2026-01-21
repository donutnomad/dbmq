package message

import (
	"time"

	"github.com/donutnomad/dbmq/internal/db"
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
func (m *Message) PartitionInfo() db.PartitionInfo {
	return db.PartitionInfo{
		Topic:     m.Topic,
		Partition: m.Partition,
	}
}

// FromDB 从数据库模型转换
func FromDB(msg *db.Message) *Message {
	if msg == nil {
		return nil
	}
	return &Message{
		ID:         msg.ID,
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		MessageKey: msg.MessageKey,
		Headers:    msg.Headers.Data(),
		Body:       msg.Body,
		CreatedAt:  msg.CreatedAt,
	}
}

// FromDBSlice 从数据库模型切片转换
func FromDBSlice(msgs []db.Message) []*Message {
	result := make([]*Message, len(msgs))
	for i := range msgs {
		result[i] = FromDB(&msgs[i])
	}
	return result
}

// FetchRequest 表示单个分区的获取请求
type FetchRequest struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
	AfterID   int64  // 获取此ID之后的消息
	Limit     int    // 获取限制
}
