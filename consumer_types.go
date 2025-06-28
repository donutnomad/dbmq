package dbmq

import (
	"encoding/json"
	"github.com/donutnomad/dbmq/types"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"gorm.io/gorm"
	"time"
)

// ConsumeStrategy 消费策略枚举
type ConsumeStrategy int

const (
	// ConsumeFromCommitted 从已提交的偏移量开始消费，如果没有则使用Latest策略（默认）
	ConsumeFromCommitted ConsumeStrategy = iota
	// ConsumeFromEarliest 从最早的消息开始消费（偏移量0）
	ConsumeFromEarliest
	// ConsumeFromLatest 从最新的消息开始消费（跳过历史消息）
	ConsumeFromLatest
)

// String 返回消费策略的字符串表示
func (s ConsumeStrategy) String() string {
	switch s {
	case ConsumeFromCommitted:
		return "committed"
	case ConsumeFromEarliest:
		return "earliest"
	case ConsumeFromLatest:
		return "latest"
	default:
		return "unknown"
	}
}

// ConsumerConfig 消费者配置结构
// 包含数据库连接、Redis连接、消费组设置和性能参数
type ConsumerConfig struct {
	DB                  *gorm.DB        // 数据库连接，用于消息拉取和偏移量提交
	Redis               *redis.Client   // Redis连接，用于实时通知（可选）
	GroupID             string          // 消费组ID，同一消费组内的消费者共同消费Topic
	NotificationEnabled bool            // 是否启用Redis实时通知优化
	HeartbeatInterval   time.Duration   // 心跳间隔，用于向协调器报告存活状态
	Topics              []string        // 要订阅的Topic列表
	PollFetchLimit      int             // 每次Poll操作从单个分区最多拉取的消息数
	PollFetchTimeout    time.Duration   // Poll操作中数据库查询的超时时间
	ConsumeStrategy     ConsumeStrategy // 消费策略，决定消费者首次注册时从哪里开始消费

	// 自动提交相关配置
	EnableAutoCommit   bool          // 是否启用自动提交偏移量
	AutoCommitInterval time.Duration // 自动提交间隔，仅在EnableAutoCommit为true时有效
}

func (c ConsumerConfig) GetPollFetchTimeout() time.Duration {
	if c.PollFetchTimeout == 0 {
		return 5 * time.Second
	}
	return c.PollFetchTimeout
}

func (c ConsumerConfig) GetPollFetchLimit() int {
	if c.PollFetchLimit == 0 {
		return 100
	}
	return c.PollFetchLimit
}

// ConsumerMessage 消费者接收到的消息（重复定义，为了保持兼容性）
type ConsumerMessage struct {
	Topic     string            // 消息所属的Topic
	Partition uint              // 消息所属的分区
	ID        int64             // 消息的ID
	Key       []byte            // 消息Key
	Value     []byte            // 消息内容
	Headers   map[string]string // 消息头
	Timestamp time.Time         // 消息时间戳
}

func (c ConsumerMessage) PartitionInfo() types.PartitionInfo {
	return types.PartitionInfo{
		Topic:     c.Topic,
		Partition: c.Partition,
	}
}

type ConsumerMessages []ConsumerMessage

func (*ConsumerMessages) FromMessages(messages []types.Message) ConsumerMessages {
	return lo.Map(messages, func(m types.Message, index int) ConsumerMessage {
		msg := ConsumerMessage{
			Topic:     m.Topic,
			Partition: m.Partition,
			ID:        m.ID,
			Value:     m.Body,
			Timestamp: m.CreatedAt,
		}
		if len(m.Headers) > 0 {
			var headers map[string]string
			_ = json.Unmarshal(m.Headers, &headers)
			msg.Headers = headers
		}
		if m.MessageKey.Valid {
			msg.Key = []byte(m.MessageKey.String)
		}
		return msg
	})
}
