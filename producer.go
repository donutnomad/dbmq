package dbmq

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/donutnomad/dbmq/logger"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/datatypes"
)

type contextKey string

const (
	// contextKeyLogEnabled 用于在 context 中标记是否启用日志
	contextKeyLogEnabled contextKey = "dbmq.log_enabled"

	// producerNotifyScript 实现"智能通知合并"逻辑的Lua脚本
	// 只有当分区的通知状态键不存在时才返回需要发送通知，避免惊群效应
	//
	// 注意: 为兼容 Redis Cluster, 脚本内不再执行 PUBLISH（脚本内 PUBLISH 只会在
	// 脚本所在节点广播，集群下订阅端可能连在其他节点而收不到通知）。脚本只负责
	// 原子的"去重"判断，PUBLISH 由 Go 侧在脚本返回 1 后调用，交由客户端正确路由。
	// 同时脚本只访问单个 key（KEYS[1]），彻底规避 CROSSSLOT 限制。
	//
	// KEYS[1]: 状态键 (例如: "mq_notify_state:{topic:partition}")
	// ARGV[1]: 状态键的过期时间（秒）
	//
	// 返回值: 1表示获得通知权（需要 PUBLISH），0表示通知被合并（已有其他通知在处理中）
	producerNotifyScript = `
if redis.call("SET", KEYS[1], "notified", "NX", "EX", ARGV[1]) then
  return 1
else
  return 0
end
`
	defaultNotificationStateTTL = 60 * time.Second // 默认通知状态TTL
)

var (
	notifyScript = redis.NewScript(producerNotifyScript) // 预编译的Lua脚本
)

// WithLogEnabled 返回一个启用日志记录的 context
// 当使用此 context 调用 Send/SendBatch 或消费消息时，会记录详细的调试日志
func WithLogEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyLogEnabled, true)
}

// IsLogEnabled 检查 context 中是否启用了日志
func IsLogEnabled(ctx context.Context) bool {
	if val := ctx.Value(contextKeyLogEnabled); val != nil {
		if enabled, ok := val.(bool); ok {
			return enabled
		}
	}
	return false
}

// Producer 消息生产者，线程安全，可以并发使用
// 负责将消息发送到指定的Topic和分区
type Producer struct {
	config             ProducerConfig        // 生产者配置
	redis              redis.UniversalClient // Redis连接（可选）
	topicMetadataCache sync.Map              // Topic元数据缓存，带TTL自动失效
	roundRobinCounters sync.Map              // 轮询分区计数器，map[string]*atomic.Uint32，用于线程安全的分区轮询
	topicRepo          topic.Repo            // Topic 仓储
	messageRepo        message.Repo          // 消息仓储
}

type cachedTopicMetadata struct {
	topic     *topic.Topic
	expiresAt time.Time
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	// 自动包装 Redis 客户端, 支持 Redis 7.0+ 集群下的 sharded pub/sub (对调用方透明)
	config.Redis = wrapSharded(config.Redis)
	return &Producer{
		config:             config,
		redis:              config.Redis,
		topicMetadataCache: sync.Map{},
		roundRobinCounters: sync.Map{},
		topicRepo:          topicrepo.New(config.DB),
		messageRepo:        messagerepo.New(config.DB),
	}, nil
}

func MustNewProducer(config ProducerConfig) *Producer {
	// 自动包装 Redis 客户端, 支持 Redis 7.0+ 集群下的 sharded pub/sub (对调用方透明)
	config.Redis = wrapSharded(config.Redis)
	return &Producer{
		config:             config,
		redis:              config.Redis,
		topicMetadataCache: sync.Map{},
		roundRobinCounters: sync.Map{},
		topicRepo:          topicrepo.New(config.DB),
		messageRepo:        messagerepo.New(config.DB),
	}
}

func (p *Producer) Send(ctx context.Context, msg ProducerMessage) (*SendResult, error) {
	results, err := p.SendBatch(ctx, msg)
	if err != nil {
		return nil, err
	}
	return &results[0], nil
}

func (p *Producer) SendBatch(ctx context.Context, messages ...ProducerMessage) ([]SendResult, error) {
	// 创建 OTEL span，使用标准语义约定
	tracer := otel.Tracer("dbmq.producer")

	// 收集所有涉及的 topic，用于 span 属性
	topics := lo.Uniq(lo.Map(messages, func(m ProducerMessage, _ int) string {
		return m.Topic
	}))

	ctx, span := tracer.Start(ctx, "send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("dbmq"),
			semconv.MessagingOperationName("publish"),
			semconv.MessagingBatchMessageCount(len(messages)),
		),
	)
	defer span.End()

	// 如果只有一个 topic，添加到 span 属性
	if len(topics) == 1 {
		span.SetAttributes(semconv.MessagingDestinationName(topics[0]))
	}

	if len(messages) == 0 {
		return nil, nil
	}

	for i, msg := range messages {
		if msg.Topic == "" {
			err := fmt.Errorf("message at index %d: producer message and topic cannot be empty", i)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
	}

	topicMetadataMap := make(map[string]*topic.Topic)
	for topicName := range lo.GroupBy(messages, func(item ProducerMessage) string {
		return item.Topic
	}) {
		topicMeta, err := p.getTopicMetadata(ctx, topicName)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("failed to get metadata for topic %s: %w", topicName, err)
		}
		topicMetadataMap[topicName] = topicMeta
	}

	var domainMessages []*message.Message
	var notificationPartitions []types.PartitionInfo
	var currentTime = time.Now()

	// 创建 TraceContext propagator 用于注入追踪上下文
	propagator := propagation.TraceContext{}

	for _, msg := range messages {
		partitionCount := topicMetadataMap[msg.Topic].PartitionCount
		var partition uint
		if len(msg.Key) > 0 {
			partition = hashPartition(msg.Key, partitionCount)
		} else {
			partition = p.nextRoundRobinPartition(msg.Topic, partitionCount)
		}

		// 将追踪上下文注入到消息 Headers 中
		if msg.Headers == nil {
			msg.Headers = make(map[string]string)
		}
		propagator.Inject(ctx, propagation.MapCarrier(msg.Headers))

		domainMsg := &message.Message{
			Topic:      msg.Topic,
			Partition:  partition,
			MessageKey: msg.Key,
			Headers:    msg.Headers,
			Body:       datatypes.JSON(msg.Value),
			CreatedAt:  currentTime,
		}
		domainMessages = append(domainMessages, domainMsg)
		notificationPartitions = append(notificationPartitions, types.PartitionInfo{Topic: msg.Topic, Partition: partition})
	}

	// 批量插入消息到数据库
	if err := p.messageRepo.CreateBatch(ctx, domainMessages); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to create messages batch in db: %w", err)
	}

	// 如果启用了日志记录，打印消息详情
	if IsLogEnabled(ctx) {
		for _, domainMsg := range domainMessages {
			p.logger().DebugContext(ctx, "producer.send",
				"topic", domainMsg.Topic,
				"partition", domainMsg.Partition,
				"key", domainMsg.MessageKey,
				"message_id", domainMsg.ID,
				"headers", domainMsg.Headers,
			)
		}
	}

	p.sendBatchNotifications(context.Background(), notificationPartitions)

	span.SetStatus(codes.Ok, "")

	return lo.Map(domainMessages, func(domainMsg *message.Message, index int) SendResult {
		return SendResult{
			Topic:     domainMsg.Topic,
			Partition: domainMsg.Partition,
			Offset:    domainMsg.ID, // 使用数据库自动生成的ID作为offset
		}
	}), nil
}

// sendBatchNotifications 批量发送通知，去重相同的topic-partition组合
func (p *Producer) sendBatchNotifications(ctx context.Context, partitions []types.PartitionInfo) {
	if p.redis == nil || !p.config.NotificationEnabled {
		return
	}
	for _, partition := range lo.Uniq(partitions) {
		go p.sendNotification(ctx, partition.Topic, partition.Partition)
	}
}

// sendNotification 发送智能通知到Redis
// 使用Lua脚本确保原子性，实现"智能通知合并"逻辑
func (p *Producer) sendNotification(ctx context.Context, topic string, partition uint) {
	// 构造Redis键名。状态键使用 hash tag {topic:partition}，
	// 与通知频道 hash 到同一 slot，保证 Redis Cluster 下行为可控。
	stateKey := notifyStateKey(topic, partition)  // 状态键，用于防止重复通知
	channelKey := notifyChannel(topic, partition) // 通知频道

	// 执行Lua脚本：仅做原子去重判断，只访问单个 key，规避 CROSSSLOT 限制
	res, err := notifyScript.Run(ctx, p.redis, []string{stateKey}, p.config.GetNotificationStateTTL().Seconds()).Result()
	if err != nil {
		// 记录错误但不影响消息发送操作
		// 通知失败不应该影响消息的可靠性
		p.logger().Error("❌ [生产者通知] Redis通知脚本执行失败", "channel", channelKey, "error", err)
		return
	}

	// 脚本返回 1 表示获得通知权，由客户端正确路由 PUBLISH（集群下也能广播到订阅节点）
	if val, ok := res.(int64); ok && val == 1 {
		if err := p.redis.Publish(ctx, channelKey, "new_message").Err(); err != nil {
			// PUBLISH 失败时主动删除状态键，避免 TTL 内后续生产者被"合并"掉，
			// 导致该分区实时通知在 TTL 窗口内静默丢失（正确性仍由 MySQL 轮询兜底）。
			if delErr := p.redis.Del(ctx, stateKey).Err(); delErr != nil {
				p.logger().Error("❌ [生产者通知] 回滚通知状态键失败", "stateKey", stateKey, "error", delErr)
			}
			p.logger().Error("❌ [生产者通知] Redis发布通知失败", "channel", channelKey, "error", err)
			return
		}
		p.logger().Debug("📢 [生产者通知] 成功发送通知到", "channel", channelKey)
	} else {
		p.logger().Debug("🔄 [生产者通知] 通知被合并（已有通知在处理中）", "channel", channelKey)
	}
}

// getTopicMetadata 获取Topic元数据，带内存缓存优化
// 缓存可以显著减少数据库查询，提高性能
func (p *Producer) getTopicMetadata(ctx context.Context, topicName string) (*topic.Topic, error) {
	// 首先检查缓存
	if entry, ok := p.topicMetadataCache.Load(topicName); ok {
		cached := entry.(*cachedTopicMetadata)
		if time.Now().Before(cached.expiresAt) {
			return cached.topic, nil
		}
		// 条目已过期，删除后重建
		p.topicMetadataCache.Delete(topicName)
	}

	// 缓存未命中，从数据库查询
	topics, err := p.topicRepo.FindByNames(ctx, []string{topicName})
	if err != nil {
		return nil, fmt.Errorf("failed to find topic '%s': %w", topicName, err)
	}
	if len(topics) == 0 {
		return nil, &ErrUnknownTopicOrPartition{Topic: topicName}
	}

	// 缓存查询结果并设置TTL
	t := topics[0]
	p.topicMetadataCache.Store(topicName, &cachedTopicMetadata{
		topic:     t,
		expiresAt: time.Now().Add(p.config.GetTopicMetadataTTL()),
	})
	return t, nil
}

// nextRoundRobinPartition 使用轮询策略选择下一个分区
// 使用原子操作确保线程安全，每个Topic独立维护计数器
func (p *Producer) nextRoundRobinPartition(topic string, partitionCount uint) uint {
	if partitionCount <= 1 {
		return 0
	}
	// 获取或创建该Topic的计数器
	counter, _ := p.roundRobinCounters.LoadOrStore(topic, &atomic.Uint32{})
	// 原子性递增并取模，实现轮询
	newVal := counter.(*atomic.Uint32).Add(1)
	return uint(newVal-1) % partitionCount
}

// hashPartition 使用哈希算法选择分区
// 使用FNV-1a哈希算法，确保相同Key总是路由到同一分区，保证消息顺序
func hashPartition(key string, partitionCount uint) uint {
	if partitionCount == 0 {
		return 0
	}
	hasher := fnv.New64a() // FNV-1a哈希算法，速度快且分布均匀
	_, _ = hasher.Write([]byte(key))
	return uint(hasher.Sum64() % uint64(partitionCount))
}

func (p *Producer) logger() *slog.Logger {
	return logger.GetLogger().With("component", "producer")
}
