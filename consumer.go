package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/logger"
	"github.com/donutnomad/dbmq/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

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

// IsReady 如果消费者没有在重新均衡且有分配的分区，返回true
func (c *Consumer) IsReady() bool {
	if c.rebalancing.Load() {
		return false
	}
	return isNotEmpty(c.getAssignedPartitions())
}

func (c *Consumer) ID() string {
	return c.id
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

func (c *Consumer) getTopics() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topics
}

func (c *Consumer) getAssignedPartitions() []types.PartitionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return mapToPartition(c.assignment)
}

func (c *Consumer) getAlreadyConsumeMessageIDByPartition(p types.PartitionInfo) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.alreadyConsumeMessageIDs[p]
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
