package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	dberrors "github.com/donutnomad/dbmq/pkg/errors"
	"github.com/donutnomad/dbmq/pkg/types"

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

// ConsumerMessage 消费者接收到的消息（重复定义，为了保持兼容性）
type ConsumerMessage struct {
	Topic     string            // 消息所属的Topic
	Partition uint              // 消息所属的分区
	Offset    int64             // 消息在分区中的偏移量
	Key       []byte            // 消息Key
	Value     []byte            // 消息内容
	Headers   map[string]string // 消息头
	Timestamp time.Time         // 消息时间戳
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
	mu               sync.RWMutex                  // 保护内部状态的读写锁
	rebalancing      atomic.Bool                   // 标记是否正在进行重新均衡
	generationID     uint                          // 当前代际ID，用于版本控制
	assignment       map[string][]uint             // 分区分配，topic -> partitions
	committedOffsets map[types.PartitionInfo]int64 // 已提交的偏移量
	polledOffsets    map[types.PartitionInfo]int64 // 最后一次拉取的偏移量
	heartbeatStarted atomic.Bool                   // 标记心跳循环是否已启动

	// 自动提交相关
	autoCommitStarted atomic.Bool  // 标记自动提交循环是否已启动
	lastAutoCommit    atomic.Int64 // 最后一次自动提交的时间戳

	stopCh chan struct{}  // 停止信号频道
	wg     sync.WaitGroup // 等待组，用于优雅关闭
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
		config:           config,
		id:               uuid.NewString(), // 生成唯一的消费者ID
		db:               config.DB,
		redis:            config.Redis,
		topics:           config.Topics,
		stopCh:           make(chan struct{}),
		assignment:       make(map[string][]uint),
		committedOffsets: make(map[types.PartitionInfo]int64),
		polledOffsets:    make(map[types.PartitionInfo]int64),
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
	log.Printf("Closing consumer %s...", c.id)

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
		c.pubsub.Close()
	}
	c.muSub.Unlock()

	// 在所有后台进程停止后，进行最后一次提交
	// 这样可以避免与自动提交循环的竞争条件
	if err := c.CommitSync(); err != nil {
		log.Printf("ERROR: final commit failed for consumer %s: %v", c.id, err)
	}

	// 通过删除心跳记录优雅离开消费组
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dal.DeleteHeartbeat(ctx, c.db, c.config.GroupID, c.id); err != nil {
		log.Printf("ERROR: failed to leave group gracefully for consumer %s: %v", c.id, err)
	}

	log.Printf("Consumer %s shut down.", c.id)
}

// Poll 从订阅的Topic和分区中拉取消息
// 这是消费者逻辑的核心，实现了复杂的拉取和通知机制
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	// 如果正在进行重新均衡，立即返回并提示用户
	// 心跳循环负责处理重新均衡过程
	if c.rebalancing.Load() {
		return nil, &dberrors.ErrRebalanceInProgress{GroupID: c.config.GroupID}
	}

	// 获取当前分配的分区
	assignedPartitions := c.getAssignedPartitions()
	if len(assignedPartitions) == 0 {
		// 没有分配的分区，等待超时后返回空列表
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil // 超时，返回空列表
		}
	}

	// 如果启用了通知优化，使用Redis Pub/Sub等待通知
	if c.config.NotificationEnabled && c.redis != nil {
		// 检查并确保PubSub连接健康
		c.ensurePubSubConnection()

		c.muSub.Lock()
		if c.pubsub != nil && c.pubsubHealthy.Load() {
			// 先排空任何在我们开始等待之前到达的消息
			select {
			case <-c.notifyCh:
				// 有消息在等待，我们将立即进行拉取
				log.Printf("Drained pending notification for consumer %s", c.id)
				c.lastPubsubTime.Store(time.Now().Unix())
			default:
				// 没有消息在等待，使用非阻塞方式等待通知
				waitTimeout := timeout
				if waitTimeout > 3*time.Second {
					waitTimeout = 3 * time.Second // 最多等待3秒，避免长时间阻塞
				}

				// 使用带超时的上下文
				waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)

				// 使用channel接收通知，而不是ReceiveTimeout
				select {
				case msg := <-c.notifyCh:
					if msg != nil {
						log.Printf("Received notification for consumer %s: %s", c.id, msg.Channel)
						c.lastPubsubTime.Store(time.Now().Unix())
					}
					cancel()
				case <-waitCtx.Done():
					// 超时或上下文取消，这是正常的
					cancel()
				}
			}
		} else {
			// PubSub连接不健康，标记为不健康并回退到轮询模式
			log.Printf("PubSub connection unhealthy for consumer %s, falling back to polling", c.id)
			c.pubsubHealthy.Store(false)
		}
		c.muSub.Unlock()

		// 如果PubSub不可用，回退到简单的睡眠
		if !c.pubsubHealthy.Load() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(min(timeout, 1*time.Second)): // 缩短轮询间隔
			}
		}
	} else {
		// 如果禁用了通知，回退到简单的睡眠
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
		}
	}

	// 使用批量查询从所有分配的分区拉取消息
	// 这个拉取操作在通知或超时后执行
	// 为拉取本身使用较短的上下文，因为主要的等待已经发生了
	fetchTimeout := 5 * time.Second
	if c.config.PollFetchTimeout > 0 {
		fetchTimeout = c.config.PollFetchTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	// 构建批量请求
	var batchRequests []dal.PartitionRequest
	for _, p := range assignedPartitions {
		offset := c.getOffset(p)
		limit := 100
		if c.config.PollFetchLimit > 0 {
			limit = c.config.PollFetchLimit
		}
		batchRequests = append(batchRequests, dal.PartitionRequest{
			Topic:     p.Topic,
			Partition: p.Partition,
			Offset:    offset,
			Limit:     limit,
		})
	}

	// 批量获取消息
	allMessages, err := dal.FetchMessagesBatch(fetchCtx, c.db, batchRequests)
	if err != nil {
		// 如果批量查询失败，记录错误并返回
		log.Printf("ERROR: failed to batch fetch messages for consumer %s: %v", c.id, err)
		return nil, fmt.Errorf("batch fetch failed: %w", err)
	}

	// 按分区分组消息，用于更新偏移量和通知状态
	messagesByPartition := make(map[types.PartitionInfo][]types.Message)
	for _, msg := range allMessages {
		partition := types.PartitionInfo{Topic: msg.Topic, Partition: msg.Partition}
		messagesByPartition[partition] = append(messagesByPartition[partition], msg)
	}

	// 更新每个分区的已拉取偏移量和通知状态
	for partition, messages := range messagesByPartition {
		if len(messages) > 0 {
			// 更新该分区的已拉取偏移量
			lastMessage := messages[len(messages)-1]
			c.setPolledOffset(partition, lastMessage.ID)

			// "重新装填"该分区的通知触发器，因为我们刚刚拉取了数据
			if c.config.NotificationEnabled && c.redis != nil {
				go c.resetNotificationState(context.Background(), partition)
			}
		}
	}

	return toConsumerMessages(allMessages), nil
}

// heartbeatLoop 消费者的核心后台进程，负责：
// 1. 定期向协调器发送心跳
// 2. 获取最新的分区分配和代际ID
// 3. 检测到变化时触发并执行重新均衡协议
// 这将状态管理的网络I/O与主Poll()循环解耦
func (c *Consumer) heartbeatLoop() {
	defer c.wg.Done()

	// 在启动定时器之前执行初始状态协调
	c.reconcileState(context.Background())

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 这里的上下文应该对这个特定任务是短暂的
			ctx, cancel := context.WithTimeout(context.Background(), c.config.HeartbeatInterval)
			c.reconcileState(ctx)
			cancel()
		case <-c.stopCh:
			return
		}
	}
}

// autoCommitLoop 自动提交偏移量的后台进程
// 定期提交已拉取的消息偏移量，无需手动调用CommitSync
// autoCommitLoop 自动提交偏移量的后台进程
// 定期提交已拉取的消息偏移量，无需手动调用CommitSync
func (c *Consumer) autoCommitLoop() {
	defer c.wg.Done()

	log.Printf("Consumer %s: 启动自动提交循环，间隔: %v", c.id, c.config.AutoCommitInterval)

	ticker := time.NewTicker(c.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 执行自动提交，只提交有新拉取偏移量的分区
			if err := c.autoCommitPolledOffsets(); err != nil {
				log.Printf("ERROR: Consumer %s 自动提交失败: %v", c.id, err)
			} else {
				c.lastAutoCommit.Store(time.Now().Unix())
			}
		case <-c.stopCh:
			// 在停止前执行最后一次提交
			log.Printf("Consumer %s: 自动提交循环收到停止信号，执行最后一次提交", c.id)
			if err := c.autoCommitPolledOffsets(); err != nil {
				log.Printf("ERROR: Consumer %s 最终自动提交失败: %v", c.id, err)
			}
			return
		}
	}
}

// autoCommitPolledOffsets 自动提交已拉取但未提交的偏移量
// 这是一个内部方法，只在自动提交模式下使用
// autoCommitPolledOffsets 自动提交已拉取但未提交的偏移量
// 这是一个内部方法，只在自动提交模式下使用
func (c *Consumer) autoCommitPolledOffsets() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否有需要提交的已拉取偏移量
	if len(c.polledOffsets) == 0 {
		return nil // 没有新的偏移量需要提交
	}

	// 创建需要提交的偏移量副本
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	for partition, offset := range c.polledOffsets {
		offsetsToCommit[partition] = offset
	}

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, c.generationID, offsetsToCommit)
	if err != nil {
		return fmt.Errorf("failed to auto-commit polled offsets: %w", err)
	}

	// 提交成功后，更新内存状态
	for partition, offset := range offsetsToCommit {
		// committedOffsets 存储下一个要消费的偏移量（已提交偏移量 + 1）
		c.committedOffsets[partition] = offset + 1
		delete(c.polledOffsets, partition)
	}

	log.Printf("Consumer %s: 自动提交成功，提交了 %d 个分区的偏移量", c.id, len(offsetsToCommit))
	return nil
}

// reconcileState 执行单次发送心跳、获取消费者当前状态和处理重新均衡（如有必要）的循环
func (c *Consumer) reconcileState(ctx context.Context) {
	// 首先注册/更新心跳
	if err := c.register(ctx); err != nil {
		log.Printf("ERROR: failed to send heartbeat for consumer %s: %v", c.id, err)
		return // 如果连心跳都无法发送，就不继续处理
	}

	// 从数据库获取我们自己的状态
	hb, err := dal.GetHeartbeat(ctx, c.db, c.config.GroupID, c.id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 这在罕见的竞态条件下可能发生，我们在心跳和获取之间被踢出
			// 下一次心跳会重新注册
			log.Printf("WARN: could not find our own heartbeat for consumer %s, will retry.", c.id)
		} else {
			log.Printf("ERROR: failed to fetch consumer state for %s: %v", c.id, err)
		}
		return
	}

	c.mu.RLock()
	currentGenID := c.generationID
	c.mu.RUnlock()

	// 检查是否需要重新均衡
	if hb.GenerationID == currentGenID {
		return // 没有变化，无需操作
	}

	log.Printf("Consumer %s: Generation ID changed from %d to %d, starting rebalance", c.id, currentGenID, hb.GenerationID)

	// ---- 需要重新均衡 ----
	log.Printf("Rebalance detected for consumer %s. New Generation ID: %d", c.id, hb.GenerationID)
	c.rebalancing.Store(true)
	defer c.rebalancing.Store(false)

	log.Printf("Consumer %s: received assignment data: %s", c.id, string(hb.AssignedPartitions))
	newPartitions, err := c.parsePartitions(hb.AssignedPartitions)
	if err != nil {
		log.Printf("ERROR: failed to parse new partition assignment for consumer %s: %v", c.id, err)
		return
	}
	log.Printf("Consumer %s: parsed new partitions: %v", c.id, newPartitions)

	// 1. 找出被撤销的分区
	revokedPartitions := c.findRevokedPartitions(newPartitions)

	// 2. 为被撤销的分区提交偏移量，确保工作不会丢失
	// 使用新的代际ID进行提交，防止过时的提交
	if len(revokedPartitions) > 0 {
		log.Printf("Consumer %s revoking partitions: %v", c.id, revokedPartitions)
		if err := c.commitOffsets(ctx, revokedPartitions, hb.GenerationID); err != nil {
			log.Printf("ERROR: failed to commit offsets for revoked partitions on consumer %s: %v", c.id, err)
			// 即使提交失败也继续重新均衡
		}
	}

	// 3. 清除内部状态并为新分配获取偏移量
	log.Printf("Consumer %s: clearing and fetching offsets for new assignment", c.id)
	if err := c.clearAndFetchOffsetsForNewAssignment(newPartitions); err != nil {
		log.Printf("ERROR: failed to fetch offsets for new assignment on consumer %s: %v", c.id, err)
		return // 这对重新均衡来说是致命错误
	}
	log.Printf("Consumer %s: successfully fetched offsets for new assignment", c.id)

	// 4. 如果启用了Redis订阅，更新Redis订阅
	if c.config.NotificationEnabled && c.redis != nil {
		// 这可以并发进行，但为了简单起见，我们内联执行
		// 更高级的实现可以更平滑地管理这个过程
		var oldPartitionsList []types.PartitionInfo
		c.mu.RLock()
		for topic, partitions := range c.assignment {
			for _, pID := range partitions {
				oldPartitionsList = append(oldPartitionsList, types.PartitionInfo{Topic: topic, Partition: pID})
			}
		}
		c.mu.RUnlock()

		var newPartitionsList []types.PartitionInfo
		for topic, parts := range newPartitions {
			for _, p := range parts {
				newPartitionsList = append(newPartitionsList, types.PartitionInfo{Topic: topic, Partition: p})
			}
		}

		c.unsubscribeFromChannels(oldPartitionsList)
		c.subscribeToChannels(newPartitionsList)
	}

	// 5. 原子性地更新消费者状态
	c.mu.Lock()
	c.generationID = hb.GenerationID
	c.assignment = newPartitions
	c.mu.Unlock()

	log.Printf("Rebalance completed for consumer %s. New assignment: %v", c.id, newPartitions)
}

// register 向协调器发送心跳，有效地注册或更新消费者的存活状态和Topic订阅信息
func (c *Consumer) register(ctx context.Context) error {
	c.mu.RLock()
	topicsData, err := json.Marshal(c.topics)
	c.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal subscribed topics: %w", err)
	}

	// 心跳同时作为注册，这是一个upsert操作
	return dal.UpsertHeartbeat(ctx, c.db, c.config.GroupID, c.id, topicsData)
}

// IsReady 如果消费者没有在重新均衡且有分配的分区，返回true
func (c *Consumer) IsReady() bool {
	if c.rebalancing.Load() {
		return false
	}
	assignedPartitions := c.getAssignedPartitions()
	return len(assignedPartitions) > 0
}

// IsAutoCommitEnabled 返回是否启用了自动提交
func (c *Consumer) IsAutoCommitEnabled() bool {
	return c.config.EnableAutoCommit
}

// GetLastAutoCommitTime 返回最后一次自动提交的时间
func (c *Consumer) GetLastAutoCommitTime() time.Time {
	return time.Unix(c.lastAutoCommit.Load(), 0)
}

// CommitSync 同步提交所有当前分配分区的偏移量
// 这是一个阻塞操作
// CommitSync 同步提交所有当前分配分区的偏移量
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息偏移量
// CommitSync 同步提交所有当前分配分区的偏移量
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息偏移量
func (c *Consumer) CommitSync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 获取当前分配的分区
	partitions := c.getAssignedPartitionsLocked()
	if len(partitions) == 0 {
		return nil // 没有分配的分区，无需提交
	}

	// 构建需要提交的偏移量映射
	// 对于每个分区，提交已拉取的最大偏移量（如果存在）
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// 只提交有新拉取偏移量的分区
		if polledOffset, exists := c.polledOffsets[p]; exists {
			offsetsToCommit[p] = polledOffset
		}
	}

	if len(offsetsToCommit) == 0 {
		return nil // 没有需要提交的偏移量
	}

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, c.generationID, offsetsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit offsets: %w", err)
	}

	// 提交成功后，更新内存状态
	// 将成功提交的偏移量从polledOffsets移动到committedOffsets
	for partition, offset := range offsetsToCommit {
		// committedOffsets 存储下一个要消费的偏移量（已提交偏移量 + 1）
		c.committedOffsets[partition] = offset + 1
		// 删除已提交的polledOffsets条目
		delete(c.polledOffsets, partition)
	}

	return nil
}

// CommitOffsets 提交指定的偏移量到指定的分区
// 这允许精确控制提交到哪个消息ID
// CommitOffsets 提交指定的偏移量到指定的分区
// 这允许精确控制提交到哪个消息ID，用于手动提交模式
// CommitOffsets 提交指定的偏移量到指定的分区
// 这允许精确控制提交到哪个消息ID，用于手动提交模式
func (c *Consumer) CommitOffsets(offsets map[types.PartitionInfo]int64) error {
	if len(offsets) == 0 {
		return nil
	}

	c.mu.Lock()
	generationID := c.generationID
	c.mu.Unlock()

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsets)
	if err != nil {
		return fmt.Errorf("failed to commit specified offsets: %w", err)
	}

	// 只有在数据库提交成功后才更新内存状态
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新已提交的偏移量
	for partition, offset := range offsets {
		// committedOffsets 存储下一个要消费的偏移量（已提交偏移量 + 1）
		c.committedOffsets[partition] = offset + 1
		// 如果指定的偏移量已经在已拉取偏移量中，且小于等于指定偏移量，则从中移除
		if polledOffset, exists := c.polledOffsets[partition]; exists && polledOffset <= offset {
			delete(c.polledOffsets, partition)
		}
	}

	return nil
}

// CommitMessage 提交单个消息的偏移量
// 这是CommitOffsets的便利方法，用于精确的手动提交
func (c *Consumer) CommitMessage(msg ConsumerMessage) error {
	partition := types.PartitionInfo{
		Topic:     msg.Topic,
		Partition: msg.Partition,
	}
	offsets := map[types.PartitionInfo]int64{
		partition: msg.Offset,
	}
	return c.CommitOffsets(offsets)
}

// commitOffsets handles the database logic for committing a batch of offsets.
// This is the legacy method that acquires locks internally.
// commitOffsets 处理重新均衡时撤销分区的偏移量提交
// 这是一个内部方法，用于重新均衡过程中的偏移量提交
// commitOffsets 处理重新均衡时撤销分区的偏移量提交
// 这是一个内部方法，用于重新均衡过程中的偏移量提交
func (c *Consumer) commitOffsets(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	if len(partitions) == 0 {
		return nil
	}

	c.mu.RLock()
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// 优先提交已拉取的偏移量，如果没有则提交已提交的偏移量-1（转换为最后消费的偏移量）
		if polledOffset, exists := c.polledOffsets[p]; exists {
			offsetsToCommit[p] = polledOffset
		} else if committedOffset, exists := c.committedOffsets[p]; exists && committedOffset > 0 {
			// committedOffsets存储的是下一个要消费的偏移量，所以减1得到最后消费的偏移量
			offsetsToCommit[p] = committedOffset - 1
		}
	}
	c.mu.RUnlock()

	if len(offsetsToCommit) == 0 {
		return nil
	}

	err := dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsetsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit offsets for revoked partitions: %w", err)
	}

	// 成功提交后更新内存状态
	c.mu.Lock()
	for partition, offset := range offsetsToCommit {
		// committedOffsets 存储下一个要消费的偏移量（已提交偏移量 + 1）
		c.committedOffsets[partition] = offset + 1
		delete(c.polledOffsets, partition)
	}
	c.mu.Unlock()

	return nil
}

// getAssignedPartitionsLocked returns assigned partitions without acquiring locks.
// This should only be called when the caller already holds the appropriate lock.
func (c *Consumer) getAssignedPartitionsLocked() []types.PartitionInfo {
	partitions := make([]types.PartitionInfo, 0)
	for topic, parts := range c.assignment {
		for _, pNum := range parts {
			partitions = append(partitions, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}
	return partitions
}

// --- Redis Notification Helpers ---

func toChannelNames(partitions []types.PartitionInfo) []string {
	channels := make([]string, 0, len(partitions))
	for _, p := range partitions {
		channels = append(channels, fmt.Sprintf("mq_notify:%s:%d", p.Topic, p.Partition))
	}
	return channels
}

func (c *Consumer) subscribeToChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	// 确保PubSub连接健康
	c.ensurePubSubConnection()

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub != nil && c.pubsubHealthy.Load() {
		channels := toChannelNames(partitions)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
			log.Printf("ERROR: failed to subscribe to redis channels %v: %v", channels, err)
			c.pubsubHealthy.Store(false)
		} else {
			log.Printf("Consumer %s subscribed to channels: %v", c.id, channels)
			c.lastPubsubTime.Store(time.Now().Unix())
		}
	} else {
		log.Printf("WARN: PubSub connection not available for consumer %s", c.id)
	}
}

func (c *Consumer) unsubscribeFromChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub == nil || !c.pubsubHealthy.Load() {
		return // Nothing to unsubscribe from or connection not healthy
	}

	channels := toChannelNames(partitions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.pubsub.Unsubscribe(ctx, channels...); err != nil {
		log.Printf("ERROR: failed to unsubscribe from redis channels %v: %v", channels, err)
		c.pubsubHealthy.Store(false)
	} else {
		log.Printf("Consumer %s unsubscribed from channels: %v", c.id, channels)
		c.lastPubsubTime.Store(time.Now().Unix())
	}

	// 不要关闭pubsub连接，保持连接以便重用
	// pubsub连接只在消费者关闭时才关闭
}

// --- Helper methods ---

func (c *Consumer) getAssignedPartitions() []types.PartitionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	partitions := make([]types.PartitionInfo, 0)
	for topic, parts := range c.assignment {
		for _, pNum := range parts {
			partitions = append(partitions, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}
	return partitions
}

// clearAndFetchOffsetsForNewAssignment clears old state and fetches committed offsets for a new assignment.
// It must be called after the new assignment has been set on the consumer.
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

	// 如果有被撤销的分区，先提交它们的偏移量
	if len(revokedPartitions) > 0 {
		log.Printf("Consumer %s: committing offsets for revoked partitions: %v", c.id, revokedPartitions)

		// 为撤销的分区准备偏移量提交
		offsetsToCommit := make(map[types.PartitionInfo]int64)
		for _, p := range revokedPartitions {
			// 优先提交polledOffset，如果没有则提交committedOffset-1
			if polledOffset, exists := c.polledOffsets[p]; exists {
				offsetsToCommit[p] = polledOffset
			} else if committedOffset, exists := c.committedOffsets[p]; exists && committedOffset > 0 {
				offsetsToCommit[p] = committedOffset - 1
			}
		}

		// 执行提交（暂时释放锁）
		if len(offsetsToCommit) > 0 {
			c.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, c.generationID, offsetsToCommit)
			cancel()
			c.mu.Lock()

			if err != nil {
				log.Printf("ERROR: Consumer %s: failed to commit revoked partitions: %v", c.id, err)
				// 继续执行，但记录错误
			} else {
				log.Printf("Consumer %s: successfully committed revoked partitions", c.id)
			}
		}
	}

	// 只清空被撤销的分区的偏移量，保留继续分配的分区
	for _, p := range revokedPartitions {
		delete(c.polledOffsets, p)
		delete(c.committedOffsets, p)
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

	log.Printf("Consumer %s: fetching offsets for partitions: %v", c.id, partitionsToFetch)
	fetchedOffsets, err := dal.GetCommittedOffsets(ctx, c.db, c.config.GroupID, partitionsToFetch)
	if err != nil {
		log.Printf("ERROR: Consumer %s: dal.GetCommittedOffsets failed: %v", c.id, err)
		c.mu.Lock()
		return fmt.Errorf("dal.GetCommittedOffsets failed: %w", err)
	}

	// 为没有已提交偏移量的分区应用消费策略
	for _, p := range partitionsToFetch {
		if _, exists := fetchedOffsets[p]; !exists {
			startOffset, err := c.determineStartOffset(ctx, p)
			if err != nil {
				log.Printf("ERROR: Consumer %s: failed to determine start offset for %v: %v", c.id, p, err)
				fetchedOffsets[p] = 0 // 回退到0
				log.Printf("Consumer %s: fallback to offset 0 for partition %v due to error", c.id, p)
			} else {
				fetchedOffsets[p] = startOffset
				log.Printf("Consumer %s: 🆕 first time consuming partition %v, using %s strategy, starting from offset %d",
					c.id, p, c.config.ConsumeStrategy.String(), startOffset)
			}
		} else {
			log.Printf("Consumer %s: 🔄 continuing partition %v from committed offset %d",
				c.id, p, fetchedOffsets[p])
		}
	}

	log.Printf("Consumer %s: fetched offsets: %v", c.id, fetchedOffsets)

	// 更新偏移量（重新获取锁）
	c.mu.Lock()
	for partition, offset := range fetchedOffsets {
		c.committedOffsets[partition] = offset
	}

	return nil
}

// findRevokedPartitions calculates which partitions are present in the old assignment
// but not in the new one.
func (c *Consumer) findRevokedPartitions(newPartitions map[string][]uint) []types.PartitionInfo {
	oldSet := make(map[types.PartitionInfo]struct{})
	c.mu.RLock()
	for topic, partitions := range c.assignment {
		for _, pID := range partitions {
			oldSet[types.PartitionInfo{Topic: topic, Partition: pID}] = struct{}{}
		}
	}
	c.mu.RUnlock()

	newSet := make(map[types.PartitionInfo]struct{})
	for topic, partitions := range newPartitions {
		for _, pID := range partitions {
			newSet[types.PartitionInfo{Topic: topic, Partition: pID}] = struct{}{}
		}
	}

	var revoked []types.PartitionInfo
	for p := range oldSet {
		if _, ok := newSet[p]; !ok {
			revoked = append(revoked, p)
		}
	}
	return revoked
}

// parsePartitions decodes the JSON partition assignment data.
func (c *Consumer) parsePartitions(data []byte) (map[string][]uint, error) {
	var partitions []types.PartitionInfo
	if err := json.Unmarshal(data, &partitions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition assignment: %w", err)
	}
	var m = make(map[string][]uint)
	for _, partition := range partitions {
		if _, ok := m[partition.Topic]; !ok {
			m[partition.Topic] = []uint{partition.Partition}
		} else {
			m[partition.Topic] = append(m[partition.Topic], partition.Partition)
		}
	}
	return m, nil
}

// getOffset 获取分区的下一个消费偏移量
// 返回下一个应该消费的消息偏移量（已提交偏移量 + 1）
// getOffset 获取分区的下一个消费偏移量
// 返回下一个应该消费的消息偏移量
func (c *Consumer) getOffset(p types.PartitionInfo) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// committedOffsets 存储的是下一个要消费的偏移量
	// 直接返回，不需要 +1
	if committedOffset, exists := c.committedOffsets[p]; exists {
		return committedOffset
	}

	// 如果没有已提交的偏移量，从0开始
	return 0
}

// setPolledOffset 设置分区的已拉取偏移量
// 这个方法确保偏移量是单调递增的，避免回退
func (c *Consumer) setPolledOffset(p types.PartitionInfo, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查新偏移量是否小于已提交的偏移量
	if committedOffset, exists := c.committedOffsets[p]; exists {
		// committedOffsets存储的是下一个要消费的偏移量
		// 所以polledOffset不应该小于committedOffset-1
		if offset < committedOffset-1 {
			log.Printf("WARNING: Consumer %s: attempted to set polledOffset %d which is less than committed offset %d for partition %v",
				c.id, offset, committedOffset-1, p)
			return // 拒绝设置无效的偏移量
		}
	}

	// 确保偏移量是单调递增的
	if currentOffset, exists := c.polledOffsets[p]; exists {
		if offset > currentOffset {
			c.polledOffsets[p] = offset
		}
		// 如果新偏移量小于等于当前偏移量，则忽略（避免回退）
	} else {
		// 第一次设置此分区的偏移量
		c.polledOffsets[p] = offset
	}
}

func toConsumerMessages(msgs []types.Message) []ConsumerMessage {
	res := make([]ConsumerMessage, len(msgs))
	for i, m := range msgs {
		var headers map[string]string
		if len(m.Headers) > 0 {
			_ = json.Unmarshal(m.Headers, &headers)
		}

		var key []byte
		if m.MessageKey.Valid {
			key = []byte(m.MessageKey.String)
		}

		res[i] = ConsumerMessage{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.ID,
			Key:       key,
			Value:     m.Body,
			Headers:   headers,
			Timestamp: m.CreatedAt,
		}
	}
	return res
}

// resetNotificationState deletes the notification state key in Redis, allowing a subsequent
// producer to trigger a new notification. This is part of the "Intelligent Notification Coalescing" pattern.
func (c *Consumer) resetNotificationState(ctx context.Context, p types.PartitionInfo) {
	key := fmt.Sprintf("mq_notify_state:%s:%d", p.Topic, p.Partition)
	if err := c.redis.Del(ctx, key).Err(); err != nil {
		log.Printf("WARN: failed to reset notification state for %v: %v", p, err)
	}
}

// ensurePubSubConnection 确保PubSub连接健康，如果不健康则尝试重新连接
func (c *Consumer) ensurePubSubConnection() {
	c.muSub.Lock()
	defer c.muSub.Unlock()

	// 检查连接是否健康
	now := time.Now().Unix()
	lastActivity := c.lastPubsubTime.Load()

	// 如果超过30秒没有活动，或者连接标记为不健康，尝试重新连接
	if c.pubsub == nil || !c.pubsubHealthy.Load() || (now-lastActivity > 30) {
		log.Printf("PubSub connection needs refresh for consumer %s", c.id)

		// 关闭旧连接
		if c.pubsub != nil {
			c.pubsub.Close()
			c.pubsub = nil
			c.notifyCh = nil
		}

		// 创建新连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		c.pubsub = c.redis.Subscribe(ctx)
		if c.pubsub != nil {
			c.notifyCh = c.pubsub.Channel()
			c.pubsubHealthy.Store(true)
			c.lastPubsubTime.Store(now)

			// 重新订阅当前分配的分区
			assignedPartitions := c.getAssignedPartitions()
			if len(assignedPartitions) > 0 {
				channels := toChannelNames(assignedPartitions)
				if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
					log.Printf("ERROR: failed to resubscribe to channels %v: %v", channels, err)
					c.pubsubHealthy.Store(false)
				} else {
					log.Printf("Consumer %s resubscribed to channels: %v", c.id, channels)
				}
			}
		} else {
			log.Printf("ERROR: failed to create PubSub connection for consumer %s", c.id)
			c.pubsubHealthy.Store(false)
		}
	}
}

// min 返回两个time.Duration中的较小值
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// determineStartOffset 根据消费策略确定分区的起始偏移量
// 注意：此方法只在消费组第一次消费某个分区时调用（即没有已提交偏移量时）
// 如果分区已有提交的偏移量，将直接使用该偏移量，不会调用此方法
func (c *Consumer) determineStartOffset(ctx context.Context, partition types.PartitionInfo) (int64, error) {
	switch c.config.ConsumeStrategy {
	case ConsumeFromEarliest:
		// 从分区的第一条消息开始消费（偏移量0）
		log.Printf("Consumer %s: applying EARLIEST strategy for partition %v", c.id, partition)
		return 0, nil

	case ConsumeFromLatest:
		// 从最新的消息之后开始消费（跳过所有历史消息）
		log.Printf("Consumer %s: applying LATEST strategy for partition %v", c.id, partition)
		latestOffset, err := dal.GetLatestOffset(ctx, c.db, partition.Topic, partition.Partition)
		if err != nil {
			return 0, fmt.Errorf("failed to get latest offset: %w", err)
		}
		// 从最新偏移量的下一条消息开始消费
		return latestOffset + 1, nil

	case ConsumeFromCommitted:
		// 从已提交的偏移量开始消费，如果没有已提交偏移量则从最新开始
		// 由于此方法只在没有已提交偏移量时被调用，所以使用Latest作为后备策略
		log.Printf("Consumer %s: applying COMMITTED strategy (fallback to LATEST) for partition %v", c.id, partition)
		latestOffset, err := dal.GetLatestOffset(ctx, c.db, partition.Topic, partition.Partition)
		if err != nil {
			return 0, fmt.Errorf("failed to get latest offset for committed strategy fallback: %w", err)
		}
		// 从最新偏移量的下一条消息开始消费
		return latestOffset + 1, nil

	default:
		return 0, fmt.Errorf("unknown consume strategy: %v", c.config.ConsumeStrategy)
	}
}
