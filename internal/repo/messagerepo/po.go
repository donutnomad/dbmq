package messagerepo

import (
	"time"

	"gorm.io/datatypes"
)

// MessagePO 消息持久化日志表
type MessagePO struct {
	CreatedAt  time.Time                             `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3);index:idx_created_at;index:idx_topic_created_id,priority:2,sort:desc;index:idx_topic_partition_created_id,priority:3,sort:desc"`
	ID         int64                                 `gorm:"primaryKey;column:id;autoIncrement;index:idx_consume_pull,priority:3;index:idx_topic_created_id,priority:3,sort:desc;index:idx_topic_partition_created_id,priority:4,sort:desc"` // 全局唯一ID，用于消息排序和消费
	Topic      string                                `gorm:"type:varchar(255);column:topic;not null;index:idx_consume_pull,priority:1;index:idx_topic_created_id,priority:1;index:idx_topic_partition_created_id,priority:1"`
	Partition  uint                                  `gorm:"type:int unsigned;column:partition;not null;index:idx_consume_pull,priority:2;index:idx_topic_partition_created_id,priority:2"`
	MessageKey string                                `gorm:"type:varchar(255);column:message_key;not null"` // 消息的业务Key, 用于分区策略
	Headers    datatypes.JSONType[map[string]string] `gorm:"type:json;column:headers;not null"`
	Body       datatypes.JSON                        `gorm:"type:json;column:body;not null"`
}

func NewMessagePO(topic string, partition uint, messageKey string, headers map[string]string, body datatypes.JSON, createdAt time.Time) MessagePO {
	return MessagePO{
		CreatedAt:  createdAt,
		Topic:      topic,
		Partition:  partition,
		MessageKey: messageKey,
		Headers:    datatypes.NewJSONType(headers),
		Body:       body,
	}
}

func (m *MessagePO) Fix() {
	if m.Body == nil {
		m.Body = []byte{} // 确保不为nil
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
}

func (MessagePO) TableName() string {
	return "mq_messages"
}
