package dbmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/logger"
	"github.com/donutnomad/dbmq/types"
	"github.com/samber/lo"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
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

// Consumer 代表一个消费者实例，属于某个消费组
// 消费者负责从分配的分区中拉取消息、处理消息、提交偏移量
// 支持自动重新均衡和故障恢复
type Consumer struct {
	config ConsumerConfig // 消费者配置
	id     string         // 消费者唯一ID（UUID），用于在消费组内标识
	db     *gorm.DB       // 数据库连接
	redis  *redis.Client  // Redis连接（可选）
	topics []string       // 订阅的Topic列表

	// Redis发布/订阅，用于实时通知
	pubsub         *redis.PubSub         // Redis订阅对象
	notifyCh       <-chan *redis.Message // 通知消息频道
	muSub          sync.Mutex            // 保护pubsub对象的互斥锁
	pubsubHealthy  atomic.Bool           // PubSub连接健康状态
	lastPubsubTime atomic.Int64          // 最后一次PubSub活动时间戳

	// 重新均衡和轮询状态管理
	mu                       sync.RWMutex                  // 保护内部状态的读写锁
	rebalancing              atomic.Bool                   // 标记是否正在进行重新均衡
	generationID             uint                          // 当前代际ID，用于版本控制
	assignment               map[string][]uint             // 分区分配，topic -> partitions
	alreadyConsumeMessageIDs map[types.PartitionInfo]int64 // 已经消费的消息ID
	lastPolledMessageIDs     map[types.PartitionInfo]int64 // 拉取到的消息的最后一个ID
	heartbeatStarted         atomic.Bool                   // 标记心跳循环是否已启动

	// 自动提交相关
	autoCommitStarted atomic.Bool  // 标记自动提交循环是否已启动
	lastAutoCommit    atomic.Int64 // 最后一次自动提交的时间戳

	stopCh chan struct{}  // 停止信号频道
	wg     sync.WaitGroup // 等待组，用于优雅关闭
	dao    *dal.MqDao
}

// NewConsumer 创建新的消费者实例
// 如果HeartbeatInterval为0，则默认使用3秒
// 如果ConsumeStrategy未设置，则默认使用ConsumeFromCommitted策略
// 如果启用自动提交但未设置间隔，则默认使用5秒
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}
	// 如果没有设置消费策略，默认使用ConsumeFromCommitted
	// 这与Kafka的默认行为一致：从已提交的偏移量开始，如果没有则从最新开始
	if config.ConsumeStrategy == 0 {
		config.ConsumeStrategy = ConsumeFromCommitted
	}
	// 如果启用了自动提交但没有设置间隔，使用默认值5秒
	if config.EnableAutoCommit && config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}
	consumer := &Consumer{
		config:                   config,
		id:                       uuid.NewString(), // 生成唯一的消费者ID
		db:                       config.DB,
		redis:                    config.Redis,
		topics:                   config.Topics,
		stopCh:                   make(chan struct{}),
		assignment:               make(map[string][]uint),
		alreadyConsumeMessageIDs: make(map[types.PartitionInfo]int64),
		lastPolledMessageIDs:     make(map[types.PartitionInfo]int64),
		dao:                      dal.NewMqDao(config.DB),
	}

	// 初始化原子变量
	consumer.pubsubHealthy.Store(false)
	consumer.lastPubsubTime.Store(time.Now().Unix())
	consumer.lastAutoCommit.Store(time.Now().Unix())

	return consumer, nil
}

func (c *Consumer) Id() string {
	return c.id
}

// SubscribeTopics 注册消费者要监听的Topic列表
// 必须在第一次调用Poll之前调用
// 同时触发消费者加入消费组并开始心跳
func (c *Consumer) SubscribeTopics(topics ...string) error {
	c.mu.Lock()
	c.topics = topics
	c.mu.Unlock()

	// 在订阅时启动心跳循环，循环本身会处理注册和所有后续的状态协调
	if c.heartbeatStarted.CompareAndSwap(false, true) {
		c.wg.Add(1)
		go c.heartbeatLoop()
	}

	// 如果启用了自动提交，启动自动提交循环
	if c.config.EnableAutoCommit && c.autoCommitStarted.CompareAndSwap(false, true) {
		c.wg.Add(1)
		go c.autoCommitLoop()
	}

	return nil
}

// Close 优雅关闭消费者，停止所有循环并最后提交一次偏移量
func (c *Consumer) Close() {
	c.logger().Debug("Closing", "consumer-id", c.id)

	// 停止所有后台循环
	select {
	case <-c.stopCh:
		// 已经关闭了，不需要再次关闭
	default:
		close(c.stopCh)
	}

	// 等待所有goroutine停止
	c.wg.Wait()

	// 关闭pubsub连接
	c.muSub.Lock()
	if c.pubsub != nil {
		_ = c.pubsub.Close()
	}
	c.muSub.Unlock()

	// 在所有后台进程停止后，只有在自动提交模式下才进行最后一次提交
	// 手动提交模式下，用户应该负责提交所有需要确认的消息
	// 这样可以避免与自动提交循环的竞争条件，也避免在手动模式下误提交未确认的消息
	if c.config.EnableAutoCommit {
		if err := c.CommitSync(); err != nil {
			c.logger().Error(fmt.Sprintf("ERROR: final auto-commit failed for consumer %s: %v", c.id, err))
		}
	} else {
		c.logger().Debug(fmt.Sprintf("Consumer %s: 手动提交模式，跳过Close时的自动提交。用户应确保所有消息都已手动提交。", c.id))
	}

	// 通过标记消费者为离线状态优雅离开消费组，保留历史记录
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.dao.MarkConsumerOffline(ctx, c.config.GroupID, c.id); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to mark consumer offline gracefully for consumer %s: %v", c.id, err))
	}

	c.logger().Debug("Shutdown", "consumer-id", c.id)
}

func (c *Consumer) getTopics() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.topics)
}

// IsReady 如果消费者没有在重新均衡且有分配的分区，返回true
func (c *Consumer) IsReady() bool {
	if c.rebalancing.Load() {
		return false
	}
	return isNotEmpty(c.getAssignedPartitions())
}

func (c *Consumer) GetGenerationID() uint {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getGenerationIDLocked()
}

func (c *Consumer) getGenerationIDLocked() uint {
	return c.generationID
}

// IsAutoCommitEnabled 返回是否启用了自动提交
func (c *Consumer) IsAutoCommitEnabled() bool {
	return c.config.EnableAutoCommit
}

// GetLastAutoCommitTime 返回最后一次自动提交的时间
func (c *Consumer) GetLastAutoCommitTime() time.Time {
	return time.Unix(c.lastAutoCommit.Load(), 0)
}

func (c *Consumer) getAssignedPartitions() []types.PartitionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return mapToPartition(c.assignment)
}

// clearAndFetchOffsetsForNewAssignment
// 清除旧状态并获取新分配的已提交偏移量。
// 必须在消费者设置新分配后调用。
func (c *Consumer) clearAndFetchOffsetsForNewAssignment(newAssignment map[string][]uint) error {
	// 构建新分配的分区列表
	var partitionsToFetch []types.PartitionInfo
	newPartitionSet := make(map[types.PartitionInfo]bool)
	for topic, parts := range newAssignment {
		for _, pNum := range parts {
			partition := types.PartitionInfo{Topic: topic, Partition: pNum}
			partitionsToFetch = append(partitionsToFetch, partition)
			newPartitionSet[partition] = true
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 找出被撤销的分区（在当前分配中但不在新分配中）
	var revokedPartitions []types.PartitionInfo
	for topic, parts := range c.assignment {
		for _, pNum := range parts {
			partition := types.PartitionInfo{Topic: topic, Partition: pNum}
			if !newPartitionSet[partition] {
				revokedPartitions = append(revokedPartitions, partition)
			}
		}
	}

	// 提交被撤销分区的
	if len(revokedPartitions) > 0 {
		if c.config.EnableAutoCommit {
			c.logger().Debug(fmt.Sprintf("Consumer %s: auto-commit mode, committing offsets for revoked partitions: %v", c.id, revokedPartitions))

			// 为撤销的分区准备消息ID提交
			messageIDsToCommit := make(map[types.PartitionInfo]int64)
			for _, p := range revokedPartitions {
				if polledMessageID, exists := c.lastPolledMessageIDs[p]; exists {
					messageIDsToCommit[p] = polledMessageID
				} else if messageID, exists := c.alreadyConsumeMessageIDs[p]; exists {
					messageIDsToCommit[p] = messageID
				}
			}

			// 执行提交（暂时释放锁）
			if len(messageIDsToCommit) > 0 {
				c.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, c.config.GroupID, c.generationID, messageIDsToCommit)
				cancel()
				c.mu.Lock()

				if err != nil {
					c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: failed to commit revoked partitions: %v", c.id, err))
					// 继续执行，但记录错误
				} else {
					c.logger().Debug(fmt.Sprintf("Consumer %s: successfully committed revoked partitions", c.id))
				}
			}
		} else {
			// 手动提交模式下，不自动提交被撤销分区的消息ID
			// 记录警告信息，提醒用户可能丢失未提交的消息
			var uncommittedPartitions []types.PartitionInfo
			for _, p := range revokedPartitions {
				if _, exists := c.lastPolledMessageIDs[p]; exists {
					uncommittedPartitions = append(uncommittedPartitions, p)
				}
			}
			if len(uncommittedPartitions) > 0 {
				c.logger().Warn(fmt.Sprintf("WARNING: Consumer %s: 手动提交模式下，重新均衡导致分区 %v 被撤销，但这些分区有未提交的消息。这些消息将被重新消费。", c.id, uncommittedPartitions))
			}
			c.logger().Debug(fmt.Sprintf("Consumer %s: manual commit mode, not auto-committing revoked partitions: %v", c.id, revokedPartitions))
		}
	}

	// 只清空被撤销的分区的消息ID记录，保留继续分配的分区
	for _, p := range revokedPartitions {
		delete(c.lastPolledMessageIDs, p)
		delete(c.alreadyConsumeMessageIDs, p)
	}

	// 更新分配
	c.assignment = newAssignment

	// 如果没有新分区需要获取，直接返回
	if len(partitionsToFetch) == 0 {
		return nil
	}

	// 获取新分区的已提交偏移量（暂时释放锁）
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	////////////////////// 为新增的分区应用策略 //////////////////////
	c.logger().Debug(fmt.Sprintf("Consumer %s: fetching offsets for partitions: %v", c.id, partitionsToFetch))
	fetchedOffsets, err := c.dao.GetCommittedOffsets(ctx, c.config.GroupID, partitionsToFetch)
	if err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: dal.GetCommittedOffsets failed: %v", c.id, err))
		c.mu.Lock()
		return fmt.Errorf("dal.GetCommittedOffsets failed: %w", err)
	}
	// 筛选出新增的分区
	addedPartitions := lo.Filter(partitionsToFetch, func(p types.PartitionInfo, index int) bool {
		_, exists := fetchedOffsets[p]
		return !exists
	})
	initialProgressWithWatermarks := make(map[types.PartitionInfo]dal.ConsumptionProgressWithWatermark)
	// 为没有已提交偏移量的分区应用消费策略并立即记录到数据库
	for _, partition := range addedPartitions {
		startID := c.determineStartMessageID(ctx, partition)

		c.logger().Debug(fmt.Sprintf("Consumer %s: 正在为新分区注册订阅信息", c.id))

		c.mu.Lock()
		c.alreadyConsumeMessageIDs[partition] = startID - 1
		c.mu.Unlock()
		initialProgressWithWatermarks[partition] = dal.ConsumptionProgressWithWatermark{
			LastConsumedMessageID:      startID - 1,
			SubscriptionStartWatermark: startID,
		}
	}
	if err := dal.BatchCommitOffsetsWithInitialWatermark(ctx, c.db, c.config.GroupID, c.generationID, initialProgressWithWatermarks); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: 订阅信息注册失败: %v", c.id, err))
		// 继续执行，但记录错误。这不是致命错误，因为重新注册时会重新应用策略
	} else {
		c.logger().Debug(fmt.Sprintf("Consumer %s: 订阅信息注册成功", c.id))
	}
	////////////////////// 为新增的分区应用策略 //////////////////////

	c.mu.Lock() // 有defer解锁，这是必须的

	return nil
}

var firstMessageId = int64(1) // 数据库的消息ID主键是从1开始的

// determineStartMessageID 首次，根据消费策略，确定从哪个ID开始消费，比如2，那么第一个将会消费2这个ID
func (c *Consumer) determineStartMessageID(ctx context.Context, partition types.PartitionInfo) int64 {
	switch c.config.ConsumeStrategy {
	case ConsumeFromEarliest:
		return firstMessageId
	case ConsumeFromLatest, ConsumeFromCommitted:
		latestOffset, err := c.dao.GetTopicLatestIDByPartition(ctx, partition.Topic, partition.Partition)
		if err != nil {
			return firstMessageId
		}
		return latestOffset + 1
	default:
		// unreachable
		panic(fmt.Errorf("unknown consume strategy: %v", c.config.ConsumeStrategy))
	}
}

func (c *Consumer) logger() *slog.Logger {
	return logger.GetLogger().With("component", "consumer")
}
