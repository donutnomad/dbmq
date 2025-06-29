package dbmq

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/donutnomad/dbmq/logger"
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
	Offset    int64  // 消息的全局ID（用作偏移量）
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
	topicMetadataCache sync.Map       // Topic元数据缓存，map[string]*types.Topic， // TODO: 未来如果支持增加分区数量，那么需要清理这个缓存
	roundRobinCounters sync.Map       // 轮询分区计数器，map[string]*atomic.Uint32，用于线程安全的分区轮询
	dao                *dal.MqDao
}

// NewProducer 创建新的生产者实例
// 如果NotificationStateTTL为0，则使用默认值60秒
// 如果OffsetCacheTTL为0，则使用默认值300秒
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
		dao:                dal.NewMqDao(config.DB),
	}, nil
}

// Send 发送消息到指定Topic，这是一个阻塞操作
// 内部调用SendBatch方法以复用代码逻辑
func (p *Producer) Send(ctx context.Context, msg *ProducerMessage) (*SendResult, error) {
	// 验证消息和Topic名称
	if msg == nil || msg.Topic == "" {
		return nil, errors.New("producer message and topic cannot be empty")
	}

	// 调用批量发送方法处理单条消息
	results, err := p.SendBatch(ctx, *msg)
	if err != nil {
		return nil, err
	}

	// 返回第一条消息的发送结果
	return &results[0], nil
}

// BatchSendResult 批量发送的结果
type BatchSendResult []SendResult

// SendBatch 批量发送消息，显著提升高吞吐量场景的性能
// 通过减少数据库事务数量和网络往返次数来优化性能
// 注意：批量发送是原子性的，要么全部成功，要么全部失败
func (p *Producer) SendBatch(ctx context.Context, messages ...ProducerMessage) (BatchSendResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// 验证所有消息
	for i, msg := range messages {
		if msg.Topic == "" {
			return nil, fmt.Errorf("message at index %d: producer message and topic cannot be empty", i)
		}
	}

	// 按Topic分组消息，以便批量获取元数据
	topicGroups := lo.GroupBy(messages, func(item ProducerMessage) string {
		return item.Topic
	})

	// 预先获取所有Topic的元数据
	topicMetadataMap := make(map[string]*types.Topic)
	for topicName := range topicGroups {
		topicMeta, err := p.getTopicMetadata(ctx, topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata for topic %s: %w", topicName, err)
		}
		topicMetadataMap[topicName] = topicMeta
	}

	// 准备批量插入的数据库消息
	var dbMessages []*types.Message
	var notificationPartitions []struct {
		topic     string
		partition uint
	}

	currentTime := time.Now()

	// 为每条消息分配分区并构造数据库对象
	for _, msg := range messages {
		partitionCount := topicMetadataMap[msg.Topic].PartitionCount
		// 选择目标分区
		var partition uint
		if msg.Key != nil {
			partition = hashPartition(msg.Key, partitionCount)
		} else {
			partition = p.nextRoundRobinPartition(msg.Topic, partitionCount)
		}
		// 构造数据库消息对象
		dbMsg := types.NewMessage(msg.Topic, partition, msg.Key, msg.Headers, msg.Value, currentTime)
		dbMessages = append(dbMessages, &dbMsg)

		// 记录需要发送通知的分区（去重）
		if p.config.NotificationEnabled && p.redis != nil {
			notificationPartitions = append(notificationPartitions, struct {
				topic     string
				partition uint
			}{msg.Topic, partition})
		}
	}

	// 批量插入消息到数据库
	if err := dal.CreateMessagesBatch(ctx, p.db, dbMessages); err != nil {
		return nil, fmt.Errorf("failed to create messages batch in db: %w", err)
	}

	// 构造返回结果
	results := lo.Map(dbMessages, func(dbMsg *types.Message, index int) SendResult {
		return SendResult{
			Topic:     dbMsg.Topic,
			Partition: dbMsg.Partition,
			Offset:    dbMsg.ID, // 使用数据库自动生成的ID作为offset
		}
	})

	// 添加调试日志
	p.logger().Debug(fmt.Sprintf("📤 [Producer] 批量发送成功 - %d 条消息", len(messages)))

	p.sendBatchNotifications(context.Background(), notificationPartitions)

	return results, nil
}

// sendBatchNotifications 批量发送通知，去重相同的topic-partition组合
func (p *Producer) sendBatchNotifications(ctx context.Context, partitions []struct {
	topic     string
	partition uint
}) {
	if p.config.NotificationEnabled && p.redis != nil {
		return
	}
	// 去重分区
	uniquePartitions := make(map[string]struct {
		topic     string
		partition uint
	})

	for _, partition := range partitions {
		key := fmt.Sprintf("%s:%d", partition.topic, partition.partition)
		uniquePartitions[key] = partition
	}

	// 为每个唯一分区发送通知
	for _, partition := range uniquePartitions {
		go p.sendNotification(ctx, partition.topic, partition.partition)
	}
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
		p.logger().Error("❌ [生产者通知] Redis通知脚本执行失败", "channel", channelKey, "error", err)
		return
	}

	// 调试信息：记录通知是否被发送或合并
	if val, ok := res.(int64); ok && val == 1 {
		p.logger().Debug("📢 [生产者通知] 成功发送通知到", "channel", channelKey)
	} else {
		p.logger().Debug("🔄 [生产者通知] 通知被合并（已有通知在处理中）", "channel", channelKey)
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
	topics, err := p.dao.FindTopicsByNames(ctx, []string{topicName})
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
func hashPartition(key []byte, partitionCount uint) uint {
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

func (p *Producer) logger() *slog.Logger {
	return logger.GetLogger().With("component", "producer")
}
