package dbmq

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/repo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
	"go.opentelemetry.io/otel/trace"
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
	NotificationEnabled  bool                  // 启用Redis实时通知优化. 这是一个"即发即忘"的操作，失败不影响消息发送
	NotificationStateTTL time.Duration         // 设置Redis中通知状态键的过期时间, 默认60秒
	TopicMetadataTTL     time.Duration         // Topic 元数据缓存存活时间，默认30秒
	DB                   repo.DB               // 数据库连接，用于消息持久化
	Redis                redis.UniversalClient // Redis连接，用于实时通知（可选）
}

func (c ProducerConfig) GetNotificationStateTTL() time.Duration {
	if c.NotificationStateTTL == 0 {
		return defaultNotificationStateTTL
	}
	return c.NotificationStateTTL
}

func (c ProducerConfig) GetTopicMetadataTTL() time.Duration {
	if c.TopicMetadataTTL <= 0 {
		return 30 * time.Second
	}
	return c.TopicMetadataTTL
}

// ProducerMessage 生产者发送的消息结构（重复定义，为了保持兼容性）
type ProducerMessage struct {
	Topic   string            // 目标Topic名称
	Key     string            // 消息Key，用于分区路由
	Value   []byte            // 消息内容
	Headers map[string]string // 消息头，键值对格式
}

// SendResult 消息发送后返回的结果（重复定义，为了保持兼容性）
type SendResult struct {
	Topic     string // 消息所在的Topic
	Partition uint   // 消息所在的分区
	Offset    int64  // 消息的全局ID（用作偏移量）
}

// ConsumeStrategy 消费策略枚举
type ConsumeStrategy int

const (
	// ConsumeFromEarliest 从最早的消息开始消费（偏移量0）
	ConsumeFromEarliest ConsumeStrategy = iota
	// ConsumeFromLatest 从最新的消息开始消费（跳过历史消息）
	ConsumeFromLatest
)

// String 返回消费策略的字符串表示
func (s ConsumeStrategy) String() string {
	switch s {
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
	DB                        repo.DB               // 数据库连接，用于消息拉取和偏移量提交
	Redis                     redis.UniversalClient // Redis连接，用于实时通知（可选）
	GroupID                   string                // 消费组ID，同一消费组内的消费者共同消费Topic
	ClientID                  string                // 用户自定义的稳定标识，为空则自动生成 {hostname}:{mac地址}
	NotificationEnabled       bool                  // 是否启用Redis实时通知优化
	HeartbeatInterval         time.Duration         // 心跳间隔，用于向协调器报告存活状态
	HeartbeatOperationTimeout time.Duration         // 单次心跳数据库操作的超时时间
	Topics                    []string              // 要订阅的Topic列表
	PollFetchLimit            int                   // 每次Poll操作从单个分区最多拉取的消息数
	PollFetchTimeout          time.Duration         // Poll操作中数据库查询的超时时间
	ConsumeStrategy           ConsumeStrategy       // 消费策略，决定消费者首次注册时从哪里开始消费

	// 自动提交相关配置
	EnableAutoCommit   bool          // 是否启用自动提交偏移量
	AutoCommitInterval time.Duration // 自动提交间隔，仅在EnableAutoCommit为true时有效
}

func (c ConsumerConfig) GetHeartbeatInterval() time.Duration {
	if c.HeartbeatInterval == 0 {
		return 3 * time.Second
	}
	return c.HeartbeatInterval
}

func (c ConsumerConfig) GetHeartbeatOperationTimeout() time.Duration {
	if c.HeartbeatOperationTimeout <= 0 {
		return 5 * time.Second
	}
	return c.HeartbeatOperationTimeout
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
	Key       string            // 消息Key
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

// ExtractTracingContext 从消息 Headers 中提取追踪上下文并创建新的 context
// parentCtx: 父级 context（通常是当前请求的 context）
// 返回: 包含从消息中提取的追踪信息的新 context
func (c ConsumerMessage) ExtractTracingContext(parentCtx context.Context) context.Context {
	propagator := propagation.TraceContext{}
	return propagator.Extract(parentCtx, propagation.MapCarrier(c.Headers))
}

// StartConsumerSpan 创建一个消费者 span，并从消息 Headers 中提取追踪上下文作为链接
// 这是推荐的方式，符合 OpenTelemetry 消息消费的语义约定
// parentCtx: 父级 context
// spanName: span 名称，如果为空则使用 "process"
// 返回: 新的 context 和 span（调用者负责调用 span.End()）
func (c ConsumerMessage) StartConsumerSpan(parentCtx context.Context, spanName string, attributes ...attribute.KeyValue) (context.Context, trace.Span) {
	if spanName == "" {
		spanName = "process"
	}

	tracer := otel.Tracer("dbmq.consumer")
	propagator := propagation.TraceContext{}

	// 从消息头提取生产者的 span context，作为 consumer span 的父级。
	// 以 parentCtx 为基底进行 Extract：若消息携带有效 traceparent，则 carrierCtx
	// 会带上生产者的 SpanContext，从而让 consumer span 与 producer span 处于同一条
	// trace（SigNoz 等 UI 默认按 trace-id 聚合展示父子树，这样才能看到串联）。
	carrierCtx := propagator.Extract(parentCtx, propagation.MapCarrier(c.Headers))
	parentSpanContext := trace.SpanContextFromContext(carrierCtx)

	// 同时保留一条 Span Link 指向生产者，符合 OpenTelemetry 消息消费语义约定，
	// 也便于多消费组 / 重复消费场景下追溯到同一条原始消息。
	var links []trace.Link
	if parentSpanContext.IsValid() {
		links = append(links, trace.Link{
			SpanContext: parentSpanContext,
		})
	}

	attributes = append(attributes,
		semconv.MessagingSystemKey.String("dbmq"),
		semconv.MessagingOperationName(spanName),
		semconv.MessagingDestinationName(c.Topic),
		attribute.Int64("messaging.message.id", c.ID),
		attribute.Int("messaging.destination.partition.id", int(c.Partition)),
	)

	// 以 carrierCtx 作为父级：consumer span 继承 producer 的 trace-id，
	// 在 SigNoz 中表现为 producer→consumer 的父子串联。
	ctx, span := tracer.Start(carrierCtx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(attributes...),
	)

	if c.Key != "" {
		span.SetAttributes(attribute.String("messaging.message.conversation_id", c.Key))
	}

	return ctx, span
}

type ConsumerMessages []ConsumerMessage

// StartBatchConsumerSpan 为批量消息创建一个消费者 span，使用 Span Links 链接所有消息的追踪上下文。
//
// 与单条消息的 StartConsumerSpan 不同，批量 span 刻意使用 Span Link 而非父子关系：
// 一个批次可能聚合来自多个生产者 / 多条不同 trace 的消息，无法同时成为多个 producer 的子 span。
// 因此这里 span 挂在调用方的 parentCtx 上（消费者自己的 trace），并对批内每条消息各建一条
// Link 指回其原始 producer span。这符合 OpenTelemetry 对批量消费的语义约定。
//
// 注意：SigNoz 等 UI 默认按 trace-id 展示父子树，批量 span 与各 producer 的关联通过 Link 体现，
// 需在支持 Span Link 的视图中查看。若需要严格的父子串联，请改用单条消息的 StartConsumerSpan。
//
// parentCtx: 父级 context（消费者侧）
// spanName: span 名称，如果为空则使用 "process_batch"
// 返回: 新的 context 和 span（调用者负责调用 span.End()）
func (messages ConsumerMessages) StartBatchConsumerSpan(parentCtx context.Context, spanName string) (context.Context, trace.Span) {
	if spanName == "" {
		spanName = "process_batch"
	}

	tracer := otel.Tracer("dbmq.consumer")
	propagator := propagation.TraceContext{}

	// 从所有消息头提取 span context 并创建 links
	var links []trace.Link
	topicSet := make(map[string]bool)

	for _, msg := range messages {
		carrierCtx := propagator.Extract(context.Background(), propagation.MapCarrier(msg.Headers))
		parentSpanContext := trace.SpanContextFromContext(carrierCtx)

		if parentSpanContext.IsValid() {
			links = append(links, trace.Link{
				SpanContext: parentSpanContext,
			})
		}

		topicSet[msg.Topic] = true
	}

	// 创建 span 属性
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("dbmq"),
		semconv.MessagingOperationName("process"),
		semconv.MessagingBatchMessageCount(len(messages)),
	}

	// 如果只有一个 topic，添加 destination name
	if len(topicSet) == 1 {
		for topic := range topicSet {
			attrs = append(attrs, semconv.MessagingDestinationName(topic))
		}
	}

	ctx, span := tracer.Start(parentCtx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(attrs...),
	)

	return ctx, span
}

func (*ConsumerMessages) FromMessages(messages []messagerepo.MessagePO) ConsumerMessages {
	return lo.Map(messages, func(m messagerepo.MessagePO, index int) ConsumerMessage {
		return ConsumerMessage{
			Topic:     m.Topic,
			Partition: m.Partition,
			ID:        m.ID,
			Value:     m.Body,
			Timestamp: m.CreatedAt,
			Headers:   m.Headers.Data(),
			Key:       m.MessageKey,
		}
	})
}

func (*ConsumerMessages) FromDomainMessages(messages []*message.Message) ConsumerMessages {
	return lo.Map(messages, func(m *message.Message, index int) ConsumerMessage {
		return ConsumerMessage{
			Topic:     m.Topic,
			Partition: m.Partition,
			ID:        m.ID,
			Value:     m.Body,
			Timestamp: m.CreatedAt,
			Headers:   m.Headers,
			Key:       m.MessageKey,
		}
	})
}
