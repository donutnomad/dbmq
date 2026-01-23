package messagerepo

import (
	"github.com/donutnomad/dbmq/internal/domain/message"
	"gorm.io/datatypes"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(msg *MessagePO) *message.Message {
	if msg == nil {
		return nil
	}
	return &message.Message{
		ID:         msg.ID,
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		MessageKey: msg.MessageKey,
		Headers:    msg.Headers.Data(),
		Body:       msg.Body,
		CreatedAt:  msg.CreatedAt,
	}
}

// ToDomainSlice 从数据库模型切片转换为领域实体切片
func ToDomainSlice(msgs []MessagePO) []*message.Message {
	result := make([]*message.Message, len(msgs))
	for i := range msgs {
		result[i] = ToDomain(&msgs[i])
	}
	return result
}

// ToPO 从领域实体转换为数据库模型
func ToPO(m *message.Message) *MessagePO {
	if m == nil {
		return nil
	}
	return &MessagePO{
		ID:         m.ID,
		Topic:      m.Topic,
		Partition:  m.Partition,
		MessageKey: m.MessageKey,
		Headers:    datatypes.NewJSONType(m.Headers),
		Body:       m.Body,
		CreatedAt:  m.CreatedAt,
	}
}

// ToPOSlice 从领域实体切片转换为数据库模型切片
func ToPOSlice(msgs []*message.Message) []*MessagePO {
	result := make([]*MessagePO, len(msgs))
	for i, m := range msgs {
		result[i] = ToPO(m)
	}
	return result
}
