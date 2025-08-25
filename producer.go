package dbmq

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/dao"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/logger"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
)

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
	redis              redis.Cmdable  // Redis连接（可选）
	topicMetadataCache sync.Map       // Topic元数据缓存，map[string]*db.Topic， // TODO: 未来如果支持增加分区数量，那么需要清理这个缓存
	roundRobinCounters sync.Map       // 轮询分区计数器，map[string]*atomic.Uint32，用于线程安全的分区轮询
	dao                *dao.MqDao
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	return &Producer{
		config:             config,
		redis:              config.Redis,
		topicMetadataCache: sync.Map{},
		roundRobinCounters: sync.Map{},
		dao:                dao.NewMqDao(config.DB),
	}, nil
}

func (p *Producer) Send(ctx context.Context, msg ProducerMessage) (*SendResult, error) {
	results, err := p.SendBatch(ctx, msg)
	if err != nil {
		return nil, err
	}
	return &results[0], nil
}

func (p *Producer) SendBatch(ctx context.Context, messages ...ProducerMessage) (BatchSendResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	for i, msg := range messages {
		if msg.Topic == "" {
			return nil, fmt.Errorf("message at index %d: producer message and topic cannot be empty", i)
		}
	}

	topicMetadataMap := make(map[string]*db.Topic)
	for topicName := range lo.GroupBy(messages, func(item ProducerMessage) string {
		return item.Topic
	}) {
		topicMeta, err := p.getTopicMetadata(ctx, topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata for topic %s: %w", topicName, err)
		}
		topicMetadataMap[topicName] = topicMeta
	}

	var dbMessages []*db.Message
	var notificationPartitions []db.PartitionInfo
	var currentTime = time.Now()

	for _, msg := range messages {
		partitionCount := topicMetadataMap[msg.Topic].PartitionCount
		var partition uint
		if len(msg.Key) > 0 {
			partition = hashPartition(msg.Key, partitionCount)
		} else {
			partition = p.nextRoundRobinPartition(msg.Topic, partitionCount)
		}
		dbMsg := db.NewMessage(msg.Topic, partition, msg.Key, msg.Headers, msg.Value, currentTime)
		dbMessages = append(dbMessages, &dbMsg)
		notificationPartitions = append(notificationPartitions, db.PartitionInfo{Topic: msg.Topic, Partition: partition})
	}

	// 批量插入消息到数据库
	if err := p.dao.CreateMessagesBatch(ctx, dbMessages); err != nil {
		return nil, fmt.Errorf("failed to create messages batch in db: %w", err)
	}

	p.sendBatchNotifications(context.Background(), notificationPartitions)

	return lo.Map(dbMessages, func(dbMsg *db.Message, index int) SendResult {
		return SendResult{
			Topic:     dbMsg.Topic,
			Partition: dbMsg.Partition,
			Offset:    dbMsg.ID, // 使用数据库自动生成的ID作为offset
		}
	}), nil
}

// sendBatchNotifications 批量发送通知，去重相同的topic-partition组合
func (p *Producer) sendBatchNotifications(ctx context.Context, partitions []db.PartitionInfo) {
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
	// 构造Redis键名
	stateKey := fmt.Sprintf("mq_notify_state:%s:%d", topic, partition) // 状态键，用于防止重复通知
	channelKey := fmt.Sprintf("mq_notify:%s:%d", topic, partition)     // 通知频道
	keys := []string{stateKey, channelKey}
	args := []any{"new_message", p.config.GetNotificationStateTTL().Seconds()}

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
func (p *Producer) getTopicMetadata(ctx context.Context, topicName string) (*db.Topic, error) {
	// 首先检查缓存
	if metadata, ok := p.topicMetadataCache.Load(topicName); ok {
		return metadata.(*db.Topic), nil
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
