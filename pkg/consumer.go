package pkg

import (
	"context"
	"dbmq/internal/dal"
	dberrors "dbmq/pkg/errors"
	"dbmq/pkg/types"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ConsumerConfig 消费者配置结构
// 包含数据库连接、Redis连接、消费组设置和性能参数
type ConsumerConfig struct {
	DB                  *gorm.DB      // 数据库连接，用于消息拉取和偏移量提交
	Redis               *redis.Client // Redis连接，用于实时通知（可选）
	GroupID             string        // 消费组ID，同一消费组内的消费者共同消费Topic
	NotificationEnabled bool          // 是否启用Redis实时通知优化
	HeartbeatInterval   time.Duration // 心跳间隔，用于向协调器报告存活状态
	Topics              []string      // 要订阅的Topic列表
	PollFetchLimit      int           // 每次Poll操作从单个分区最多拉取的消息数
	PollFetchTimeout    time.Duration // Poll操作中数据库查询的超时时间
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
	pubsub   *redis.PubSub         // Redis订阅对象
	notifyCh <-chan *redis.Message // 通知消息频道
	muSub    sync.Mutex            // 保护pubsub对象的互斥锁

	// 重新均衡和轮询状态管理
	mu               sync.RWMutex                  // 保护内部状态的读写锁
	rebalancing      atomic.Bool                   // 标记是否正在进行重新均衡
	generationID     uint                          // 当前代际ID，用于版本控制
	assignment       map[string][]uint             // 分区分配，topic -> partitions
	committedOffsets map[types.PartitionInfo]int64 // 已提交的偏移量
	polledOffsets    map[types.PartitionInfo]int64 // 最后一次拉取的偏移量
	heartbeatStarted atomic.Bool                   // 标记心跳循环是否已启动

	stopCh chan struct{}  // 停止信号频道
	wg     sync.WaitGroup // 等待组，用于优雅关闭
}

// NewConsumer 创建新的消费者实例
// 如果HeartbeatInterval为0，则默认使用3秒
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}
	return &Consumer{
		config:           config,
		id:               uuid.NewString(), // 生成唯一的消费者ID
		db:               config.DB,
		redis:            config.Redis,
		topics:           config.Topics,
		stopCh:           make(chan struct{}),
		assignment:       make(map[string][]uint),
		committedOffsets: make(map[types.PartitionInfo]int64),
		polledOffsets:    make(map[types.PartitionInfo]int64),
	}, nil
}

// Subscribe 注册消费者要监听的Topic列表
// 必须在第一次调用Poll之前调用
// 同时触发消费者加入消费组并开始心跳
func (c *Consumer) Subscribe(topics ...string) error {
	c.mu.Lock()
	c.topics = topics
	c.mu.Unlock()

	// 在订阅时启动心跳循环，循环本身会处理注册和所有后续的状态协调
	if c.heartbeatStarted.CompareAndSwap(false, true) {
		c.wg.Add(1)
		go c.heartbeatLoop()
	}

	return nil
}

// Close 优雅关闭消费者，停止所有循环并最后提交一次偏移量
func (c *Consumer) Close() {
	log.Printf("Closing consumer %s...", c.id)
	// 停止所有后台循环（例如心跳循环）
	close(c.stopCh)
	c.wg.Wait()

	// 关闭pubsub连接
	c.muSub.Lock()
	if c.pubsub != nil {
		c.pubsub.Close()
	}
	c.muSub.Unlock()

	// 最后一次提交待处理的偏移量
	if err := c.CommitSync(); err != nil {
		log.Printf("ERROR: final commit failed for consumer %s: %v", c.id, err)
	}

	// 通过删除心跳记录优雅离开消费组
	// 这让协调器能够立即触发重新均衡，而不需要等待心跳超时
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
		c.muSub.Lock()
		if c.pubsub != nil {
			// 先排空任何在我们开始等待之前到达的消息
			select {
			case <-c.notifyCh:
				// 有消息在等待，我们将立即进行拉取
				log.Printf("Drained pending notification for consumer %s", c.id)
			default:
				// 没有消息在等待，继续等待超时
				_, err := c.pubsub.ReceiveTimeout(ctx, timeout)
				if err != nil {
					// 这可能是超时错误，这是预期的
					// 无论如何我们都会继续进行拉取阶段
					if !errors.Is(err, redis.ErrClosed) {
						log.Printf("Notification wait ended for consumer %s (may be a timeout): %v", c.id, err)
					}
				}
			}
		}
		c.muSub.Unlock()
	} else {
		// 如果禁用了通知，回退到简单的睡眠
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
		}
	}

	// 并发地从所有分配的分区拉取消息
	// 这个拉取操作在通知或超时后执行
	type fetchResult struct {
		messages  []types.Message
		partition types.PartitionInfo
		err       error
	}
	resultsCh := make(chan fetchResult, len(assignedPartitions))

	// 为拉取本身使用较短的上下文，因为主要的等待已经发生了
	fetchTimeout := 5 * time.Second
	if c.config.PollFetchTimeout > 0 {
		fetchTimeout = c.config.PollFetchTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	var fetchWg sync.WaitGroup
	for _, p := range assignedPartitions {
		fetchWg.Add(1)
		go func(partition types.PartitionInfo) {
			defer fetchWg.Done()
			// 获取该分区的当前偏移量
			offset := c.getOffset(partition)
			limit := 100
			if c.config.PollFetchLimit > 0 {
				limit = c.config.PollFetchLimit
			}
			// 从数据库拉取消息
			messages, err := dal.FetchMessages(fetchCtx, c.db, partition.Topic, partition.Partition, offset, limit)
			resultsCh <- fetchResult{messages: messages, partition: partition, err: err}
		}(p)
	}

	fetchWg.Wait()
	close(resultsCh)

	// 收集所有拉取结果
	var allMessages []types.Message
	var lastErr error
	for res := range resultsCh {
		if res.err != nil {
			// 收集最后一个错误，更健壮的策略可能需要更复杂的错误处理
			if !errors.Is(res.err, context.Canceled) && !errors.Is(res.err, context.DeadlineExceeded) {
				lastErr = res.err
				log.Printf("ERROR: failed to fetch from partition %v: %v", res.partition, res.err)
			}
			continue
		}
		if len(res.messages) > 0 {
			allMessages = append(allMessages, res.messages...)
			// 更新该分区的已拉取偏移量
			lastMessage := res.messages[len(res.messages)-1]
			c.setPolledOffset(res.partition, lastMessage.ID)

			// "重新装填"该分区的通知触发器，因为我们刚刚拉取了数据
			if c.config.NotificationEnabled && c.redis != nil {
				go c.resetNotificationState(context.Background(), res.partition)
			}
		}
	}

	if lastErr != nil && len(allMessages) == 0 {
		return nil, fmt.Errorf("all fetch attempts failed, last error: %w", lastErr)
	}

	return toConsumerMessages(allMessages), lastErr
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

// CommitSync 同步提交所有当前分配分区的偏移量
// 这是一个阻塞操作
func (c *Consumer) CommitSync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 在提交前将已拉取的偏移量合并到已提交的偏移量中
	for p, offset := range c.polledOffsets {
		c.committedOffsets[p] = offset
	}
	// 合并后清除已拉取的偏移量
	c.polledOffsets = make(map[types.PartitionInfo]int64)

	// 获取当前分配和代际ID
	partitions := c.getAssignedPartitionsLocked()
	generationID := c.generationID

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 在不持有锁的情况下调用commitOffsetsWithoutLock，因为我们已经有了需要的数据
	err := c.commitOffsetsWithoutLock(ctx, partitions, generationID)

	return err
}

// commitOffsetsWithoutLock 处理批量提交偏移量的数据库逻辑
// 这个版本不获取任何锁，供内部使用
func (c *Consumer) commitOffsetsWithoutLock(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// 提交已提交的偏移量（包含合并的已拉取偏移量）
		if offset, ok := c.committedOffsets[p]; ok {
			offsetsToCommit[p] = offset
		}
	}

	if len(offsetsToCommit) == 0 {
		return nil
	}
	return dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsetsToCommit)
}

// commitOffsets handles the database logic for committing a batch of offsets.
// This is the legacy method that acquires locks internally.
func (c *Consumer) commitOffsets(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	c.mu.RLock()
	for _, p := range partitions {
		// Commit the committed offset (which includes merged polled offsets)
		if offset, ok := c.committedOffsets[p]; ok {
			offsetsToCommit[p] = offset
		}
	}
	c.mu.RUnlock()

	if len(offsetsToCommit) == 0 {
		return nil
	}
	return dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsetsToCommit)
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

	c.muSub.Lock()
	defer c.muSub.Unlock()

	// If we don't have a pubsub connection yet, create one.
	if c.pubsub == nil {
		c.pubsub = c.redis.Subscribe(context.Background())
		c.notifyCh = c.pubsub.Channel()
	}

	channels := toChannelNames(partitions)
	if err := c.pubsub.Subscribe(context.Background(), channels...); err != nil {
		log.Printf("ERROR: failed to subscribe to redis channels %v: %v", channels, err)
	} else {
		log.Printf("Consumer %s subscribed to channels: %v", c.id, channels)
	}
}

func (c *Consumer) unsubscribeFromChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub == nil {
		return // Nothing to unsubscribe from
	}

	channels := toChannelNames(partitions)
	if err := c.pubsub.Unsubscribe(context.Background(), channels...); err != nil {
		log.Printf("ERROR: failed to unsubscribe from redis channels %v: %v", channels, err)
	} else {
		log.Printf("Consumer %s unsubscribed from channels: %v", c.id, channels)
	}

	if c.pubsub != nil {
		c.pubsub.Close()
		c.pubsub = nil
	}
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
	// Build the list of partitions to fetch before acquiring any locks
	var partitionsToFetch []types.PartitionInfo
	for topic, parts := range newAssignment {
		for _, pNum := range parts {
			partitionsToFetch = append(partitionsToFetch, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}

	c.mu.Lock()
	c.polledOffsets = make(map[types.PartitionInfo]int64)
	c.committedOffsets = make(map[types.PartitionInfo]int64)
	c.assignment = newAssignment
	c.mu.Unlock()

	if len(partitionsToFetch) == 0 {
		return nil
	}

	// This now happens in the background, so use a background context.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Consumer %s: fetching offsets for partitions: %v", c.id, partitionsToFetch)
	fetchedOffsets, err := dal.GetCommittedOffsets(ctx, c.db, c.config.GroupID, partitionsToFetch)
	if err != nil {
		log.Printf("ERROR: Consumer %s: dal.GetCommittedOffsets failed: %v", c.id, err)
		return fmt.Errorf("dal.GetCommittedOffsets failed: %w", err)
	}
	log.Printf("Consumer %s: fetched offsets: %v", c.id, fetchedOffsets)

	c.mu.Lock()
	c.committedOffsets = fetchedOffsets
	c.mu.Unlock()

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

func (c *Consumer) getOffset(p types.PartitionInfo) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Always fetch from the last known committed offset.
	return c.committedOffsets[p]
}

func (c *Consumer) setPolledOffset(p types.PartitionInfo, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.polledOffsets[p] = offset
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
