package dbmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/types"

	"github.com/donutnomad/dbmq/internal/dal"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProducerConfig 生产者配置结构
// 包含数据库连接、Redis连接和通知相关配置
type ProducerConfig struct {
	// NotificationEnabled 启用Redis实时通知优化
	// 这是一个"即发即忘"的操作，失败不影响消息发送
	NotificationEnabled bool
	// NotificationStateTTL 设置Redis中通知状态键的过期时间
	// 防止消费者崩溃导致状态锁定，默认60秒
	NotificationStateTTL time.Duration
	DB                   *gorm.DB      // 数据库连接，用于消息持久化
	Redis                *redis.Client // Redis连接，用于实时通知（可选）
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
	Offset    int64  // 消息在分区中的偏移量
}

const (
	// producerNotifyScript 实现"智能通知合并"逻辑的Lua脚本
	// 只有当分区的通知状态键不存在时才发送通知，避免惊群效应
	//
	// KEYS[1]: 状态键 (例如: "mq_notify_state:{topic}:{partition}")
	// KEYS[2]: 通知频道 (例如: "mq_notify:{topic}:{partition}")
	// ARGV[1]: 通知消息内容 (例如: "new_message")
	// ARGV[2]: 状态键的过期时间（秒）
	//
	// 返回值: 1表示发送了通知，0表示通知被合并（即已有其他通知在处理中）
	producerNotifyScript = `
if redis.call("SET", KEYS[1], "notified", "NX", "EX", ARGV[2]) then
  redis.call("PUBLISH", KEYS[2], ARGV[1])
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

// Producer 消息生产者，线程安全，可以并发使用
// 负责将消息发送到指定的Topic和分区
type Producer struct {
	config             ProducerConfig // 生产者配置
	db                 *gorm.DB       // 数据库连接
	redis              *redis.Client  // Redis连接（可选）
	topicMetadataCache sync.Map       // Topic元数据缓存，map[string]*types.Topic
	roundRobinCounters sync.Map       // 轮询分区计数器，map[string]*atomic.Uint32，用于线程安全的分区轮询
}

// NewProducer 创建新的生产者实例
// 如果NotificationStateTTL为0，则使用默认值60秒
func NewProducer(config ProducerConfig) (*Producer, error) {
	if config.NotificationStateTTL == 0 {
		config.NotificationStateTTL = defaultNotificationStateTTL
	}
	return &Producer{
		config:             config,
		db:                 config.DB,
		redis:              config.Redis,
		topicMetadataCache: sync.Map{},
		roundRobinCounters: sync.Map{},
	}, nil
}

// Send 发送消息到指定Topic，这是一个阻塞操作
// 核心流程：验证 -> 获取Topic元数据 -> 选择分区 -> 持久化 -> 可选通知
func (p *Producer) Send(ctx context.Context, msg *ProducerMessage) (*SendResult, error) {
	// 1. 验证消息和Topic名称
	if msg == nil || msg.Topic == "" {
		return nil, errors.New("producer message and topic cannot be empty")
	}

	// 2. 获取Topic元数据（带缓存优化）
	topicMeta, err := p.getTopicMetadata(ctx, msg.Topic)
	if err != nil {
		return nil, err
	}
	partitionCount := topicMeta.PartitionCount

	// 3. 选择目标分区
	var partition uint
	if msg.Key != nil {
		// 如果消息设置了Key（包括空key），使用哈希分区确保相同Key的消息总是路由到同一分区
		// 这确保了即使是空key也会有一致的分区分配
		partition = p.hashPartition(msg.Key, partitionCount)
	} else {
		// 如果消息没有设置Key（Key为nil），使用轮询分区实现负载均衡
		partition = p.nextRoundRobinPartition(msg.Topic, partitionCount)
	}

	// 4. 构造数据库消息对象并持久化
	dbMsg := &types.Message{
		Topic:     msg.Topic,
		Partition: partition,
		Body:      msg.Value,
		CreatedAt: time.Now(),
	}

	// 设置消息Key（如果有）
	if msg.Key != nil {
		dbMsg.MessageKey.String = string(msg.Key)
		dbMsg.MessageKey.Valid = true
	}
	// 序列化消息头（如果有）
	if msg.Headers != nil {
		headersJSON, err := json.Marshal(msg.Headers)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal headers to json: %w", err)
		}
		dbMsg.Headers = headersJSON
	}

	// 5. 持久化消息到数据库
	if err := dal.CreateMessage(ctx, p.db, dbMsg); err != nil {
		return nil, fmt.Errorf("failed to create message in db: %w", err)
	}

	// 添加调试日志显示发送结果
	fmt.Printf("📤 [Producer] 消息发送成功 - Topic: %s, Partition: %d, ID: %d, PerPartitionOffset: %d\n",
		msg.Topic, partition, dbMsg.ID, dbMsg.PerPartitionOffset)

	// 6. 可选的智能通知机制
	// 在后台goroutine中执行，不影响消息发送的性能和可靠性
	if p.config.NotificationEnabled && p.redis != nil {
		go p.sendNotification(context.Background(), msg.Topic, partition)
	}

	// 7. 返回发送结果，包含消息的精确位置信息
	return &SendResult{
		Topic:     msg.Topic,
		Partition: partition,
		Offset:    dbMsg.PerPartitionOffset, // 分区内的偏移量
	}, nil
}

// sendNotification 发送智能通知到Redis
// 使用Lua脚本确保原子性，实现"智能通知合并"逻辑
func (p *Producer) sendNotification(ctx context.Context, topic string, partition uint) {
	// 构造Redis键名
	stateKey := fmt.Sprintf("mq_notify_state:%s:%d", topic, partition) // 状态键，用于防止重复通知
	channelKey := fmt.Sprintf("mq_notify:%s:%d", topic, partition)     // 通知频道
	keys := []string{stateKey, channelKey}
	args := []any{"new_message", p.config.NotificationStateTTL.Seconds()}

	// 执行Lua脚本
	res, err := notifyScript.Run(ctx, p.redis, keys, args...).Result()
	if err != nil {
		// 记录错误但不影响消息发送操作
		// 通知失败不应该影响消息的可靠性
		fmt.Printf("❌ [生产者通知] Redis通知脚本执行失败 %s: %v\n", channelKey, err)
		return
	}

	// 调试信息：记录通知是否被发送或合并
	if val, ok := res.(int64); ok && val == 1 {
		fmt.Printf("📢 [生产者通知] 成功发送通知到 %s\n", channelKey)
	} else {
		fmt.Printf("🔄 [生产者通知] 通知被合并（已有通知在处理中）%s\n", channelKey)
	}
}

// getTopicMetadata 获取Topic元数据，带内存缓存优化
// 缓存可以显著减少数据库查询，提高性能
func (p *Producer) getTopicMetadata(ctx context.Context, topicName string) (*types.Topic, error) {
	// 首先检查缓存
	if metadata, ok := p.topicMetadataCache.Load(topicName); ok {
		return metadata.(*types.Topic), nil
	}

	// 缓存未命中，从数据库查询
	topics, err := dal.FindTopicsByNames(ctx, p.db, []string{topicName})
	if err != nil {
		return nil, fmt.Errorf("failed to find topic '%s': %w", topicName, err)
	}
	if len(topics) == 0 {
		return nil, &ErrUnknownTopicOrPartition{Topic: topicName}
	}

	// 缓存查询结果
	topic := &topics[0]
	p.topicMetadataCache.Store(topicName, topic)
	return topic, nil
}

// nextRoundRobinPartition 使用轮询策略选择下一个分区
// 使用原子操作确保线程安全，每个Topic独立维护计数器
func (p *Producer) nextRoundRobinPartition(topic string, partitionCount uint) uint {
	if partitionCount == 0 {
		return 0
	}
	if partitionCount == 1 {
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
func (p *Producer) hashPartition(key []byte, partitionCount uint) uint {
	if partitionCount == 0 {
		return 0
	}
	hasher := fnv.New64a() // FNV-1a哈希算法，速度快且分布均匀
	hasher.Write(key)
	return uint(hasher.Sum64() % uint64(partitionCount))
}

// Close 关闭生产者，释放资源
// 目前是占位符，因为数据库和Redis连接由外部管理
func (p *Producer) Close() {
	// 当前无需特殊清理逻辑，数据库和Redis连接由外部管理
}
