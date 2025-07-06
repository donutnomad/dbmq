package dbmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/dao"
	"github.com/donutnomad/dbmq/logger"
	"github.com/donutnomad/dbmq/types"
)

const (
	leaderLockName      = "mq_coordinator_leader_lock" // 全局领导者锁名称
	lockRefreshInterval = 10 * time.Second             // 锁刷新间隔，10秒
	cleanupBatchSize    = 1000                         // 每批删除的消息数量
	cleanupBatchSleep   = 100 * time.Millisecond       // 批处理间的睡眠时间
)

// CoordinatorConfig 协调器配置结构
type CoordinatorConfig struct {
	DB                     dao.DB        // 数据库连接
	HeartbeatTimeout       time.Duration // 消费者心跳超时时间，超过此时间认为消费者已死亡
	RebalanceInterval      time.Duration // 重新均衡检查间隔
	RebalanceTimeout       time.Duration // 重新均衡操作的上下文超时时间
	RetentionCheckInterval time.Duration // 消息保留清理检查间隔
	DefaultRetentionAge    time.Duration // 没有特定保留策略的Topic的默认保留时间
}

// Coordinator 管理单个消费组及其重新均衡的协调器
// 当它是领导者时，还承担消息保留清理的全局责任
type Coordinator struct {
	config   CoordinatorConfig // 协调器配置
	dao      *dao.MqDao
	isLeader atomic.Bool // 原子布尔值，标记是否为领导者

	ctx     context.Context    // 根上下文，控制整个协调器生命周期
	cancel  context.CancelFunc // 取消函数，用于停止所有goroutine
	stopped atomic.Bool        // 原子布尔值，标记是否已停止

	wg sync.WaitGroup // 等待组，用于优雅关闭

	mu      sync.Mutex                     // 保护members map的互斥锁
	members map[string]map[string]struct{} // groupID -> set of consumer IDs，缓存消费组成员信息

	logger           *slog.Logger
	rebalancingLocks *groupLocks // 分消费组的重新均衡锁
}

// NewCoordinator 创建一个新的协调器
// 拥有全局领导者锁的协调器还将执行系统级任务，如消息清理
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	// 设置默认值
	if config.RebalanceInterval == 0 {
		config.RebalanceInterval = 10 * time.Second
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 30 * time.Second
	}
	if config.DefaultRetentionAge == 0 {
		// 默认7天保留期
		config.DefaultRetentionAge = 7 * 24 * time.Hour
	}
	if config.RetentionCheckInterval == 0 {
		// 默认每小时检查一次
		config.RetentionCheckInterval = 1 * time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		config:           config,
		ctx:              ctx,
		cancel:           cancel,
		members:          make(map[string]map[string]struct{}),
		rebalancingLocks: newGroupLocks(),
		dao:              dao.NewMqDao(config.DB),
		logger:           logger.GetLogger().With("component", "coordinator"),
	}
}

// Start 开始协调器的工作，包括领导者选举
func (c *Coordinator) Start() {
	// 检查是否已经停止，防止重复启动
	if c.stopped.Load() {
		c.logger.Debug("Coordinator has already been stopped, cannot start again")
		return
	}

	c.wg.Add(1)
	go c.leaderElectionLoop()
}

// Stop 优雅关闭协调器，支持多次安全调用
func (c *Coordinator) Stop() {
	// 使用原子操作确保只执行一次停止逻辑
	if !c.stopped.CompareAndSwap(false, true) {
		// 已经停止过了，直接返回
		return
	}
	c.logger.Debug("Coordinator stopping...")

	// 取消所有goroutine的context
	c.cancel()

	// 等待所有goroutine完成
	c.wg.Wait()

	c.logger.Debug("Coordinator stopped successfully")
}

// IsLeader 返回此协调器实例是否为当前领导者
func (c *Coordinator) IsLeader() bool {
	return c.isLeader.Load()
}

// IsStopped 返回此协调器是否已经停止
func (c *Coordinator) IsStopped() bool {
	return c.stopped.Load()
}

// setLeader 设置领导者状态，并在成为领导者时启动主工作循环
func (c *Coordinator) setLeader(isLeader bool) {
	wasLeader := c.isLeader.Swap(isLeader)
	if isLeader && !wasLeader {
		c.logger.Debug("Coordinator became the global leader.")
		// 当我们成为领导者时，启动主工作循环
		c.wg.Add(1)
		go c.leaderLoop()
	}
	if !isLeader && wasLeader {
		c.logger.Debug("Coordinator lost global leadership.")
	}
}

// leaderElectionLoop 领导者选举循环
func (c *Coordinator) leaderElectionLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(lockRefreshInterval)
	defer ticker.Stop()

	// 初始尝试获取领导权
	c.attemptToBecomeLeader()

	for {
		select {
		case <-c.ctx.Done():
			if c.IsLeader() {
				c.releaseLock()
			}
			return
		case <-ticker.C:
			c.attemptToBecomeLeader()
		}
	}
}

// attemptToBecomeLeader 尝试成为领导者
func (c *Coordinator) attemptToBecomeLeader() {
	// 如果协调器已停止，不再尝试获取领导权
	if c.IsStopped() {
		return
	}

	var ch = make(chan int)

	go func() {
		var result int
		// GET_LOCK是会话特定的。结果为1表示我们获得了锁
		// 0表示另一个会话持有锁。NULL表示发生了错误
		// 使用30秒超时，如果当前持有者死亡，允许接管
		err := c.dao.DB().Raw("SELECT GET_LOCK(?, ?)", leaderLockName, lockRefreshInterval/time.Second/2).Scan(&result).Error
		if err != nil {
			c.logger.Error("Error in leader election", "error", err)
			ch <- -1
		} else {
			ch <- result
		}
	}()

	select {
	case <-c.ctx.Done():
		if c.IsLeader() {
			c.setLeader(false)
		}
		return
	case result := <-ch:
		switch result {
		case -1:
			if c.IsLeader() {
				c.setLeader(false)
			}
		case 0:
			// 无法获得锁，可能有其他协调器持有锁，或者锁获取超时
			if c.IsLeader() {
				c.logger.Debug("Coordinator lost leadership (unable to acquire lock).")
				c.setLeader(false)
			}
			// 如果我们不是领导者，这是正常情况
		case 1:
			// 我们获得了锁，成为或保持领导者
			if !c.IsLeader() {
				c.logger.Debug("Coordinator acquired leadership.")
				c.setLeader(true)
			}
			// 如果我们已经是领导者，这只是刷新锁
		}
		// 如果result == 0且!c.IsLeader()，我们是追随者，无需操作
	}
}

// releaseLock 释放全局领导者锁
// 在优雅关闭时调用这个函数是好的实践
func (c *Coordinator) releaseLock() {
	if err := c.dao.DB().Exec("SELECT RELEASE_LOCK(?)", leaderLockName).Error; err != nil {
		c.logger.Error("Error releasing global leader lock", "error", err)
	} else {
		c.logger.Debug("Coordinator released global leader lock.")
	}
}

// leaderLoop 领导者主工作循环
// 负责全局重新均衡扫描和消息保留清理
func (c *Coordinator) leaderLoop() {
	defer c.wg.Done()
	c.logger.Debug("Coordinator leader loop started.")
	rebalanceTicker := time.NewTicker(c.config.RebalanceInterval)
	defer rebalanceTicker.Stop()
	cleanupTicker := time.NewTicker(c.config.RetentionCheckInterval)
	defer cleanupTicker.Stop()

	// 启动时立即运行一次
	c.scanAndRebalanceAllGroups()
	c.runRetentionCleanup(c.ctx)

	for {
		// 如果协调器已停止或我们不再是领导者，循环应该停止
		if c.IsStopped() || !c.IsLeader() {
			if c.IsStopped() {
				c.logger.Debug("Coordinator stopped, stopping leader loop.")
			} else {
				c.logger.Debug("No longer leader, stopping leader loop.")
			}
			return
		}

		select {
		case <-c.ctx.Done():
			c.logger.Debug("Coordinator stopping leader loop.")
			return
		case <-rebalanceTicker.C:
			c.logger.Debug("Starting global rebalance scan...")
			c.scanAndRebalanceAllGroups()
		case <-cleanupTicker.C:
			c.logger.Debug("Starting message retention cleanup...")
			c.runRetentionCleanup(c.ctx)
		}
	}
}

// runRetentionCleanup 运行消息保留清理
// 根据Topic配置删除过期的消息
func (c *Coordinator) runRetentionCleanup(ctx context.Context) {
	// 使用协调器的context作为父context，确保在协调器停止时能够快速退出
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute) // 清理的慷慨超时时间
	defer cancel()

	// 如果协调器已停止，不执行清理操作
	if c.IsStopped() {
		c.logger.Debug("Coordinator stopped, skipping message retention cleanup.")
		return
	}

	c.logger.Debug("Starting message retention cleanup cycle.")
	startTime := time.Now()

	// 1. 获取所有Topic以了解它们的保留策略
	allTopics, err := c.dao.GetAllTopics(ctx)
	if err != nil {
		c.logger.Error("Cleanup failed to get topics", "error", err)
		return
	}
	topicConfigMap := make(map[string]time.Duration)
	for _, topic := range allTopics {
		retentionMs, _ := topic.GetConfig("retention_ms")
		if retentionMs > 0 {
			topicConfigMap[topic.TopicName] = time.Duration(retentionMs) * time.Millisecond
		}
	}

	// 2. Calculate global low watermark for all consumed partitions
	watermarks, err := c.dao.GetConsumerGroupLowWatermarks(ctx)
	if err != nil {
		c.logger.Error("Cleanup failed to get low watermarks", "error", err)
		return
	}
	c.logger.Debug(fmt.Sprintf("Found %d consumed partitions with a low watermark.", len(watermarks)))

	var totalDeletedCount int64

	// 3. Iterate through all partitions of all topics and apply deletion logic
	for _, topic := range allTopics {
		retentionAge := c.config.DefaultRetentionAge
		if configuredAge, ok := topicConfigMap[topic.TopicName]; ok {
			retentionAge = configuredAge
		}
		retentionDate := time.Now().Add(-retentionAge)

		for i := uint(0); i < topic.PartitionCount; i++ {
			p := types.PartitionInfo{Topic: topic.TopicName, Partition: i}
			partitionTotalDeleted := int64(0)

			// Loop to delete in batches until no more rows are affected
			for {
				var deletedCount int64
				var err error

				if lowWatermark, ok := watermarks[p]; ok {
					// This partition is consumed, so use the low watermark
					deletedCount, err = c.dao.DeleteMessagesByPartition(ctx, p.Topic, p.Partition, lowWatermark, retentionDate, cleanupBatchSize)
				} else {
					// This partition is not in the watermark map, meaning no group has ever committed an offset for it.
					// We can only clean it up based on time.
					deletedCount, err = c.dao.DeleteMessagesByPartitionUnconsumed(ctx, p.Topic, p.Partition, retentionDate, cleanupBatchSize)
				}

				if err != nil {
					c.logger.Error("Failed to clean partition", "partition", p, "error", err)
					break // Break from batch loop on error
				}

				if deletedCount > 0 {
					partitionTotalDeleted += deletedCount
				}

				// If we deleted fewer rows than the batch size, we are done with this partition.
				if deletedCount < cleanupBatchSize {
					break
				}

				// Sleep briefly to avoid overwhelming the DB, but check for context cancellation
				select {
				case <-ctx.Done():
					c.logger.Debug("Cleanup cancelled during batch processing for partition", "partition", p)
					return
				case <-time.After(cleanupBatchSleep):
					// Continue to next batch
				}
			}

			if partitionTotalDeleted > 0 {
				totalDeletedCount += partitionTotalDeleted
				c.logger.Debug(fmt.Sprintf("Cleaned up %d messages from partition %v", partitionTotalDeleted, p))
			}
		}
	}

	c.logger.Debug(fmt.Sprintf("Finished message retention cleanup cycle in %v. Total messages deleted: %d", time.Since(startTime), totalDeletedCount))
}

// scanAndRebalanceAllGroups is the new top-level function for the global leader.
// It finds all active groups and triggers a rebalance check for each one.
func (c *Coordinator) scanAndRebalanceAllGroups() {
	// 使用协调器的context作为父context，确保在协调器停止时能够快速退出
	ctx, cancel := context.WithTimeout(c.ctx, 1*time.Minute)
	defer cancel()

	// 找到活跃的消费组IDs
	activeGroupIds, err := c.dao.FindAllActiveGroups(ctx, c.config.HeartbeatTimeout)
	if err != nil {
		c.logger.Error("Failed to scan for active groups", "error", err)
		return
	}
	if len(activeGroupIds) == 0 {
		return
	}

	c.logger.Debug(fmt.Sprintf("[scanAndRebalanceAllGroups] Found active consumer groups: %v", activeGroupIds))
	for _, groupID := range activeGroupIds {
		if err := c.rebalanceIfNeeded(groupID); err != nil {
			c.logger.Error("Rebalance failed for group", "group", groupID, "error", err)
		}
	}
}

// rebalanceIfNeeded 包含特定消费组的主要重新均衡逻辑。
// 这是DBMQ系统中最核心的协调机制，负责在消费者成员变化时重新分配分区，
// 确保每个分区只被一个活跃消费者处理，实现高可用和负载均衡。
//
// 重新均衡流程严格遵循以下步骤：
// 1. 获取分组锁，防止并发重新均衡
// 2. 发现当前活跃的消费者成员
// 3. 检查是否需要重新均衡（成员变化检测）
// 4. 递增代际ID，隔离旧消费者
// 5. 收集订阅的主题和分区信息
// 6. 计算新的分区分配方案
// 7. 在事务中持久化新分配
// 8. 更新内存状态
func (c *Coordinator) rebalanceIfNeeded(groupID string) error {
	// ========== 第一步：获取分组锁，确保重新均衡的串行执行 ==========
	// 尝试获取重新均衡锁。如果已被占用，说明另一个重新均衡循环正在运行。
	// 这是一个关键的并发控制机制，避免同一消费组的多个重新均衡操作并发执行，
	// 防止数据竞争和状态不一致。
	if !c.rebalancingLocks.TryLock(groupID) {
		c.logger.Debug("Rebalance check for group skipped", "group", groupID, "reason", "another rebalance is already in progress")
		return nil
	}
	defer c.rebalancingLocks.Unlock(groupID)

	// 设置重新均衡操作的超时时间，防止操作无限期阻塞
	// 默认15秒，可通过配置调整。超时机制确保系统的响应性。
	// 使用协调器的context作为父context，确保在协调器停止时能够快速退出
	timeout := 15 * time.Second
	if c.config.RebalanceTimeout > 0 {
		timeout = c.config.RebalanceTimeout
	}
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	// ========== 第二步：发现活跃消费者 ==========
	// 查找该消费组中所有活跃的消费者。活跃性通过心跳超时判断：
	// 如果消费者在HeartbeatTimeout时间内没有发送心跳，则认为已死亡。
	// 这是重新均衡决策的基础数据。
	activeConsumers, err := c.dao.FindActiveConsumers(ctx, groupID, c.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("failed to find active consumers: %w", err)
	}

	// 将活跃消费者列表转换为ID集合，便于后续比较和处理
	activeConsumerIDs := make(map[string]struct{}, len(activeConsumers))
	for _, consumer := range activeConsumers {
		activeConsumerIDs[consumer.ConsumerID] = struct{}{}
	}

	// ========== 第三步：检查是否需要重新均衡 ==========
	// 通过比较当前活跃消费者集合与上次成功重新均衡时的消费者集合，
	// 判断是否发生了成员变化。只有在成员变化时才需要重新均衡，
	// 这避免了不必要的重新均衡操作，提高系统效率。
	if !c.isRebalanceNeeded(groupID, activeConsumerIDs) {
		return nil // 没有变化，无需重新均衡
	}

	c.logger.Info(fmt.Sprintf("Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, c.getMemberIDs(groupID), activeConsumerIDs))

	// ========== 第四步：开始重新均衡协议 - 代际隔离 ==========
	// 递增代际ID。这是DBMQ的核心隔离机制：
	// - 新代际ID会使所有旧消费者的请求失效（代际不匹配）
	// - 防止旧消费者继续处理消息，避免重复消费
	// - 实现"围栏"效应，确保只有新分配的消费者能工作
	newGenerationID, err := c.dao.IncrementAndGetGenerationID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to increment generation id: %w", err)
	}

	// 特殊情况：如果没有活跃消费者，只需递增代际并结束
	// 这种情况通常发生在所有消费者都退出时，
	// 递增代际确保后续加入的消费者使用新的代际ID
	if len(activeConsumers) == 0 {
		c.logger.Debug(fmt.Sprintf("No active consumers for group '%s'. Rebalance to generation %d complete.", groupID, newGenerationID))
		c.updateMembers(groupID, activeConsumerIDs)
		return nil
	}

	// 收集所有活跃消费者订阅的主题及其分区信息
	allPartitions, err := c.getAllPartitionsForConsumers(ctx, activeConsumers)
	if err != nil {
		return fmt.Errorf("failed to get partitions for consumers: %w", err)
	}

	// 分配分区
	newAssignments := c.calculateAssignments(activeConsumers, allPartitions)

	// 持久化到数据库中
	err = c.dao.UpdateAssignments(ctx, groupID, newGenerationID, newAssignments)
	if err != nil {
		return fmt.Errorf("failed to update assignments: %w", err)
	}

	// 记录新的消费组成员
	c.updateMembers(groupID, activeConsumerIDs)
	c.logger.Info(fmt.Sprintf("Rebalance for group '%s' to generation %d completed successfully.", groupID, newGenerationID))
	return nil
}

// isRebalanceNeeded 检查是否需要对指定消费组进行重新均衡。
// 触发重新均衡的条件：
// 1. 消费组首次出现活跃成员（从空组变为有成员）
// 2. 消费者数量发生变化（新增或减少）
// 3. 消费者成员发生变化（不同的消费者ID）
func (c *Coordinator) isRebalanceNeeded(groupID string, activeConsumerIDs map[string]struct{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 确保消费组的成员映射存在，如果不存在说明这是新的消费组
	if _, ok := c.members[groupID]; !ok {
		if len(activeConsumerIDs) > 0 {
			return true // 消费组之前为空或是新建的，但现在有成员了
		}
		return false // 消费组之前为空，现在仍然为空
	}

	// 检查消费者数量是否发生变化
	if len(activeConsumerIDs) != len(c.members[groupID]) {
		return true // 成员数量变化，需要重新均衡
	}

	// 检查消费者成员是否发生变化（即使数量相同，成员也可能不同）
	for id := range activeConsumerIDs {
		if _, exists := c.members[groupID][id]; !exists {
			return true // 发现新成员，需要重新均衡
		}
	}

	return false // 成员集合完全相同，无需重新均衡
}

func (c *Coordinator) updateMembers(groupID string, newMembers map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.members[groupID] = newMembers
}

// getMemberIDs 获取指定消费组当前缓存的成员ID列表。
func (c *Coordinator) getMemberIDs(groupID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]string, 0, len(c.members[groupID]))
	for id := range c.members[groupID] {
		ids = append(ids, id)
	}
	return ids
}

// getAllPartitionsForConsumers 收集活跃消费者订阅的所有唯一主题，
// 并返回这些主题的所有分区列表。这是重新均衡过程中的关键步骤，
// 确定需要在消费者之间分配的所有分区。
//
// 处理流程：
// 1. 解析每个消费者的订阅主题列表（JSON格式）
// 2. 合并所有消费者的订阅主题，去重
// 3. 从数据库查询主题的元数据（分区数量等）
// 4. 为每个主题生成完整的分区列表
func (c *Coordinator) getAllPartitionsForConsumers(ctx context.Context, consumers []types.ConsumerHeartbeat) ([]types.PartitionInfo, error) {
	// 使用map去重，收集所有唯一的订阅主题
	subscribedTopics := make(map[string]struct{})
	for _, consumer := range consumers {
		// 将该消费者的所有订阅主题加入到全局集合中
		for _, topic := range consumer.SubscribedTopics {
			subscribedTopics[topic] = struct{}{}
		}
	}

	// 将主题集合转换为切片，便于数据库查询
	topicNames := make([]string, 0, len(subscribedTopics))
	for topic := range subscribedTopics {
		topicNames = append(topicNames, topic)
	}

	// 从数据库查询主题的元数据信息
	dbTopics, err := c.dao.FindTopicsByNames(ctx, topicNames)
	if err != nil {
		return nil, fmt.Errorf("failed to find topics by name: %w", err)
	}

	// 为每个主题生成所有分区的完整列表
	var allPartitions []types.PartitionInfo
	for _, topic := range dbTopics {
		// 根据主题的分区数量，生成从0到PartitionCount-1的所有分区
		for i := uint(0); i < topic.PartitionCount; i++ {
			allPartitions = append(allPartitions, types.PartitionInfo{Topic: topic.TopicName, Partition: i})
		}
	}
	return allPartitions, nil
}

// calculateAssignments 使用稳定的轮询策略在消费者之间分配分区。
// 通过对消费者和分区进行排序，确保分配结果是确定性的，并在消费者增减时最小化分区迁移的开销
func (c *Coordinator) calculateAssignments(consumers []types.ConsumerHeartbeat, partitions []types.PartitionInfo) map[string][]types.PartitionInfo {
	assignments := make(map[string][]types.PartitionInfo)
	if len(consumers) == 0 {
		return assignments
	}

	// 对消费者按ID排序，确保分配顺序的一致性。这是实现稳定分配的核心，避免相同条件下产生不同的分配结果
	SortConsumersByID(consumers)

	// 先按主题排序，再按分区号排序，确保分配的逻辑顺序
	SortPartitionsByTopicAndPartition(partitions)

	// 提取消费者ID列表，并初始化每个消费者的分区分配为空
	consumerIDs := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		consumerIDs = append(consumerIDs, consumer.ConsumerID)
		assignments[consumer.ConsumerID] = []types.PartitionInfo{}
	}

	// 使用轮询算法分配分区
	// 第i个分区分配给第(i % 消费者数量)个消费者
	// 这确保了分区的均匀分布，负载均衡效果最优
	for i, p := range partitions {
		consumerID := consumerIDs[i%len(consumerIDs)]
		assignments[consumerID] = append(assignments[consumerID], p)
	}

	return assignments
}

// CleanupExpiredMessages 执行消息清理，删除过期的消息。
// 它会根据每个主题的保留策略来删除消息。
func (c *Coordinator) CleanupExpiredMessages(ctx context.Context) error {
	c.runRetentionCleanup(ctx)
	return nil
}

// groupLocks 提供基于消费组ID的锁机制
// 确保同一个消费组在同一时间只能进行一次重新均衡操作
type groupLocks struct {
	mu    sync.Mutex
	locks map[string]struct{} // 存储已锁定的消费组ID
}

func newGroupLocks() *groupLocks {
	return &groupLocks{
		locks: make(map[string]struct{}),
	}
}

// TryLock 尝试为给定的消费组ID获取锁
// 返回true表示获取成功，false表示锁已被持有
func (gl *groupLocks) TryLock(groupID string) bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if _, ok := gl.locks[groupID]; ok {
		return false // 锁已被持有
	}
	gl.locks[groupID] = struct{}{}
	return true
}

// Unlock 释放指定消费组ID的锁
func (gl *groupLocks) Unlock(groupID string) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	delete(gl.locks, groupID)
}
