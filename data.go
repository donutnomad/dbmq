package dbmq

import (
	"encoding/json"
	"github.com/donutnomad/dbmq/internal/dao"
	"github.com/donutnomad/dbmq/types"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"time"
)

// TopicConfig Topic配置结构，模仿Kafka的TopicConfig
type TopicConfig struct {
	RetentionMs    *int64 `json:"retention_ms,omitempty"`    // 消息保留时间（毫秒）
	RetentionHours *int   `json:"retention_hours,omitempty"` // 消息保留时间（小时）
	CleanupPolicy  string `json:"cleanup_policy,omitempty"`  // 清理策略：delete或compact
}

// NewTopicRequest 创建Topic的请求结构
type NewTopicRequest struct {
	Name          string       // Topic名称
	NumPartitions int          // 分区数量
	Config        *TopicConfig // Topic配置（可选）
	ValidateOnly  bool         // 是否仅验证而不实际创建
}

// TopicResult Topic操作的结果
type TopicResult struct {
	Name  string // Topic名称
	Error error  // 操作错误（如果有）
}

// CreateTopicsResult 批量创建Topic的结果
type CreateTopicsResult struct {
	Results []TopicResult // 每个Topic的创建结果
}

// ProducerConfig 生产者配置结构
// 包含数据库连接、Redis连接和通知相关配置
type ProducerConfig struct {
	NotificationEnabled  bool          // 启用Redis实时通知优化. 这是一个"即发即忘"的操作，失败不影响消息发送
	NotificationStateTTL time.Duration // 设置Redis中通知状态键的过期时间, 默认60秒
	DB                   dao.DB        // 数据库连接，用于消息持久化
	Redis                *redis.Client // Redis连接，用于实时通知（可选）
}

func (c ProducerConfig) GetNotificationStateTTL() time.Duration {
	if c.NotificationStateTTL == 0 {
		return defaultNotificationStateTTL
	}
	return c.NotificationStateTTL
}

// ProducerMessage 生产者发送的消息结构（重复定义，为了保持兼容性）
type ProducerMessage struct {
	Topic   string            // 目标Topic名称
	Key     []byte            // 消息Key，用于分区路由
	Value   []byte            // 消息内容
	Headers map[string]string // 消息头，键值对格式
}

// SendResult 消息发送后返回的结果（重复定义，为了保持兼容性）
type SendResult struct {
	Topic     string // 消息所在的Topic
	Partition uint   // 消息所在的分区
	Offset    int64  // 消息的全局ID（用作偏移量）
}

// BatchSendResult 批量发送的结果
type BatchSendResult []SendResult

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
	DB                  dao.DB          // 数据库连接，用于消息拉取和偏移量提交
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
