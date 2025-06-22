package pkg

import (
	"context"
	"dbmq/internal/dal"
	"dbmq/pkg/types"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

const (
	leaderLockName      = "mq_coordinator_leader_lock" // 全局领导者锁名称
	lockRefreshInterval = 5 * time.Second              // 锁刷新间隔，应小于数据库会话超时时间
	cleanupBatchSize    = 1000                         // 每批删除的消息数量
	cleanupBatchSleep   = 100 * time.Millisecond       // 批处理间的睡眠时间
)

// groupLocks 提供基于消费组ID的锁机制
// 确保同一个消费组在同一时间只能进行一次重新均衡操作
type groupLocks struct {
	mu    sync.Mutex          // 保护locks map的互斥锁
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

// CoordinatorConfig 协调器配置结构
type CoordinatorConfig struct {
	DB                     *gorm.DB      // 数据库连接
	HeartbeatTimeout       time.Duration // 消费者心跳超时时间，超过此时间认为消费者已死亡
	RebalanceInterval      time.Duration // 重新均衡检查间隔
	RebalanceTimeout       time.Duration // 重新均衡操作的上下文超时时间
	RetentionCheckInterval time.Duration // 消息保留清理检查间隔
	DefaultRetentionAge    time.Duration // 没有特定保留策略的Topic的默认保留时间
}

// Coordinator 管理单个消费组及其重新均衡的协调器
// 当它是领导者时，还承担消息保留清理的全局责任
type Coordinator struct {
	config           CoordinatorConfig              // 协调器配置
	db               *gorm.DB                       // 数据库连接
	isLeader         atomic.Bool                    // 原子布尔值，标记是否为领导者
	rebalancingLocks *groupLocks                    // 分消费组的重新均衡锁
	stopCh           chan struct{}                  // 停止信号频道
	wg               sync.WaitGroup                 // 等待组，用于优雅关闭
	mu               sync.Mutex                     // 保护members map的互斥锁
	members          map[string]map[string]struct{} // groupID -> set of consumer IDs，缓存消费组成员信息
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
	return &Coordinator{
		config:           config,
		db:               config.DB,
		stopCh:           make(chan struct{}),
		members:          make(map[string]map[string]struct{}),
		rebalancingLocks: newGroupLocks(),
	}
}

// Start 开始协调器的工作，包括领导者选举
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.leaderElectionLoop()
}

// Stop 优雅关闭协调器
func (c *Coordinator) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// IsLeader 返回此协调器实例是否为当前领导者
func (c *Coordinator) IsLeader() bool {
	return c.isLeader.Load()
}

// setLeader 设置领导者状态，并在成为领导者时启动主工作循环
func (c *Coordinator) setLeader(isLeader bool) {
	wasLeader := c.isLeader.Swap(isLeader)
	if isLeader && !wasLeader {
		log.Printf("Coordinator became the global leader.")
		// 当我们成为领导者时，启动主工作循环
		c.wg.Add(1)
		go c.leaderLoop()
	}
	if !isLeader && wasLeader {
		log.Printf("Coordinator lost global leadership.")
	}
}

// leaderElectionLoop 领导者选举循环
// 使用MySQL的GET_LOCK函数实现分布式锁机制
func (c *Coordinator) leaderElectionLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(lockRefreshInterval)
	defer ticker.Stop()

	// 初始尝试获取领导权
	c.attemptToBecomeLeader()

	for {
		select {
		case <-c.stopCh:
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
// 使用MySQL的GET_LOCK函数进行原子性的锁获取
func (c *Coordinator) attemptToBecomeLeader() {
	var result int
	// GET_LOCK是会话特定的。结果为1表示我们获得了锁
	// 0表示另一个会话持有锁。NULL表示发生了错误
	// 超时为0表示我们不等待锁
	err := c.db.Raw("SELECT GET_LOCK(?, 0)", leaderLockName).Scan(&result).Error
	if err != nil {
		log.Printf("Error in leader election: %v", err)
		if c.IsLeader() {
			c.setLeader(false)
		}
		return
	}
	if result == 0 {
		// 我们是领导者但失去了锁（例如，数据库连接断开并重新建立）
		// 另一个协调器可能已经接管
		log.Printf("Coordinator failed to renew lock.")
		c.setLeader(false)
	} else if result == 1 {
		// 我们是领导者
		if !c.IsLeader() {
			c.setLeader(true)
		}
		// 如果我们已经是领导者，这只是刷新会话活动
	}
	// 如果result == 0且!c.IsLeader()，我们是追随者，无需操作
}

// releaseLock 释放全局领导者锁
// 在优雅关闭时调用这个函数是好的实践
func (c *Coordinator) releaseLock() {
	if err := c.db.Exec("SELECT RELEASE_LOCK(?)", leaderLockName).Error; err != nil {
		log.Printf("Error releasing global leader lock: %v", err)
	} else {
		log.Printf("Coordinator released global leader lock.")
	}
}

// leaderLoop 领导者主工作循环
// 负责全局重新均衡扫描和消息保留清理
func (c *Coordinator) leaderLoop() {
	defer c.wg.Done()
	log.Printf("Coordinator leader loop started.")
	rebalanceTicker := time.NewTicker(c.config.RebalanceInterval)
	defer rebalanceTicker.Stop()
	cleanupTicker := time.NewTicker(c.config.RetentionCheckInterval)
	defer cleanupTicker.Stop()

	// 启动时立即运行一次
	c.scanAndRebalanceAllGroups()
	c.runRetentionCleanup()

	for {
		// 如果我们不再是领导者，循环应该停止
		if !c.IsLeader() {
			log.Printf("No longer leader, stopping leader loop.")
			return
		}

		select {
		case <-c.stopCh:
			log.Printf("Coordinator stopping leader loop.")
			return
		case <-rebalanceTicker.C:
			log.Printf("Leader coordinator starting global rebalance scan...")
			c.scanAndRebalanceAllGroups()
		case <-cleanupTicker.C:
			log.Printf("Leader coordinator starting message retention cleanup...")
			c.runRetentionCleanup()
		}
	}
}

// lockName 返回全局锁名称
// 注意：此实现已更改为单个全局锁，以符合设计文档中央协调器领导者的目标
func (c *Coordinator) lockName() string {
	return leaderLockName
}

// runRetentionCleanup 运行消息保留清理
// 根据Topic配置删除过期的消息
func (c *Coordinator) runRetentionCleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // 清理的慷慨超时时间
	defer cancel()

	log.Println("Starting message retention cleanup cycle.")
	startTime := time.Now()

	// 1. 获取所有Topic以了解它们的保留策略
	allTopics, err := dal.GetAllTopics(ctx, c.db)
	if err != nil {
		log.Printf("ERROR: Cleanup failed to get topics: %v", err)
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
	watermarks, err := dal.GetConsumerGroupLowWatermarks(ctx, c.db)
	if err != nil {
		log.Printf("ERROR: Cleanup failed to get low watermarks: %v", err)
		return
	}
	log.Printf("Found %d consumed partitions with a low watermark.", len(watermarks))

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
					deletedCount, err = dal.DeleteMessagesByPartition(ctx, c.db, p.Topic, p.Partition, lowWatermark, retentionDate, cleanupBatchSize)
				} else {
					// This partition is not in the watermark map, meaning no group has ever committed an offset for it.
					// We can only clean it up based on time.
					deletedCount, err = dal.DeleteMessagesByPartitionUnconsumed(ctx, c.db, p.Topic, p.Partition, retentionDate, cleanupBatchSize)
				}

				if err != nil {
					log.Printf("ERROR: Failed to clean partition %v: %v", p, err)
					break // Break from batch loop on error
				}

				if deletedCount > 0 {
					partitionTotalDeleted += deletedCount
				}

				// If we deleted fewer rows than the batch size, we are done with this partition.
				if deletedCount < cleanupBatchSize {
					break
				}

				// Sleep briefly to avoid overwhelming the DB
				time.Sleep(cleanupBatchSleep)
			}

			if partitionTotalDeleted > 0 {
				totalDeletedCount += partitionTotalDeleted
				log.Printf("Cleaned up %d messages from partition %v", partitionTotalDeleted, p)
			}
		}
	}

	log.Printf("Finished message retention cleanup cycle in %v. Total messages deleted: %d", time.Since(startTime), totalDeletedCount)
}

// scanAndRebalanceAllGroups is the new top-level function for the global leader.
// It finds all active groups and triggers a rebalance check for each one.
func (c *Coordinator) scanAndRebalanceAllGroups() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	activeGroups, err := dal.FindAllActiveGroups(ctx, c.db, c.config.HeartbeatTimeout)
	if err != nil {
		log.Printf("ERROR: Failed to scan for active groups: %v", err)
		return
	}

	if len(activeGroups) > 0 {
		log.Printf("Found active consumer groups: %v", activeGroups)
		for _, groupID := range activeGroups {
			if err := c.rebalanceIfNeeded(groupID); err != nil {
				log.Printf("ERROR: Rebalance failed for group '%s': %v", groupID, err)
			}
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
		log.Printf("Rebalance check for group '%s' skipped: another rebalance is already in progress.", groupID)
		return nil
	}
	defer c.rebalancingLocks.Unlock(groupID)

	// 设置重新均衡操作的超时时间，防止操作无限期阻塞
	// 默认15秒，可通过配置调整。超时机制确保系统的响应性。
	timeout := 15 * time.Second
	if c.config.RebalanceTimeout > 0 {
		timeout = c.config.RebalanceTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// ========== 第二步：发现活跃消费者 ==========
	// 查找该消费组中所有活跃的消费者。活跃性通过心跳超时判断：
	// 如果消费者在HeartbeatTimeout时间内没有发送心跳，则认为已死亡。
	// 这是重新均衡决策的基础数据。
	activeConsumers, err := dal.FindActiveConsumers(ctx, c.db, groupID, c.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("failed to find active consumers: %w", err)
	}

	// 将活跃消费者列表转换为ID集合，便于后续比较和处理
	// 使用map[string]struct{}作为集合类型，内存效率高且查找快速
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

	log.Printf("Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, c.getMemberIDs(groupID), activeConsumerIDs)

	// ========== 第四步：开始重新均衡协议 - 代际隔离 ==========
	// 递增代际ID。这是DBMQ的核心隔离机制：
	// - 新代际ID会使所有旧消费者的请求失效（代际不匹配）
	// - 防止旧消费者继续处理消息，避免重复消费
	// - 实现"围栏"效应，确保只有新分配的消费者能工作
	newGenerationID, err := dal.IncrementAndGetGenerationID(ctx, c.db, groupID)
	if err != nil {
		return fmt.Errorf("failed to increment generation id: %w", err)
	}

	// 特殊情况：如果没有活跃消费者，只需递增代际并结束
	// 这种情况通常发生在所有消费者都退出时，
	// 递增代际确保后续加入的消费者使用新的代际ID
	if len(activeConsumers) == 0 {
		log.Printf("No active consumers for group '%s'. Rebalance to generation %d complete.",
			groupID, newGenerationID)
		c.updateMembers(groupID, activeConsumerIDs)
		return nil
	}

	// ========== 第五步：收集订阅信息 ==========
	// 收集所有活跃消费者订阅的主题及其分区信息。
	// 这一步会：
	// 1. 解析每个消费者的订阅主题列表（JSON格式）
	// 2. 合并所有唯一主题
	// 3. 查询数据库获取主题的分区数量
	// 4. 生成完整的分区列表
	allPartitions, err := c.getAllPartitionsForConsumers(ctx, activeConsumers)
	if err != nil {
		return fmt.Errorf("failed to get partitions for consumers: %w", err)
	}

	// ========== 第六步：计算新的分区分配 ==========
	// 使用稳定的轮询分配算法分配分区：
	// 1. 对消费者和分区进行排序，确保分配的确定性
	// 2. 使用轮询方式分配分区，确保负载均衡
	// 3. 最小化消费者变化时的分区迁移
	newAssignments := c.calculateAssignments(activeConsumers, allPartitions)

	// ========== 第七步：持久化新分配 ==========
	// 将新的分区分配持久化到数据库中。这一步必须在事务中完成，
	// 确保所有消费者的分配要么全部成功，要么全部失败，
	// 维护分配状态的一致性。
	assignmentsForDAL := make(map[string][]byte)
	for consumerID, parts := range newAssignments {
		// 将分区列表序列化为JSON格式存储
		jsonBytes, err := json.Marshal(parts)
		if err != nil {
			return fmt.Errorf("failed to marshal assignment for consumer %s: %w", consumerID, err)
		}
		assignmentsForDAL[consumerID] = jsonBytes
	}

	// 在数据库事务中更新分配信息，确保原子性
	// 事务包括：更新代际ID、更新每个消费者的分区分配
	err = c.db.Transaction(func(tx *gorm.DB) error {
		return dal.UpdateAssignmentsInTx(ctx, tx, groupID, newGenerationID, assignmentsForDAL)
	})
	if err != nil {
		return fmt.Errorf("failed to update assignments: %w", err)
	}

	// ========== 第八步：更新内存状态 ==========
	// 更新协调器的内存缓存，记录新的消费组成员。
	// 这个缓存用于下次重新均衡时的成员变化检测，
	// 避免每次都需要查询数据库。
	c.updateMembers(groupID, activeConsumerIDs)
	log.Printf("Rebalance for group '%s' to generation %d completed successfully.",
		groupID, newGenerationID)
	return nil
}

// isRebalanceNeeded 检查是否需要对指定消费组进行重新均衡。
// 通过比较当前活跃消费者集合与上次重新均衡后缓存的消费者集合，
// 判断消费组成员是否发生了变化。
//
// 触发重新均衡的条件：
// 1. 消费组首次出现活跃成员（从空组变为有成员）
// 2. 消费者数量发生变化（新增或减少）
// 3. 消费者成员发生变化（不同的消费者ID）
//
// 参数：
//   - groupID: 消费组ID
//   - activeConsumerIDs: 当前检测到的活跃消费者ID集合
//
// 返回值：
//   - true: 需要重新均衡
//   - false: 不需要重新均衡，成员集合未发生变化
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

// updateMembers 更新指定消费组的成员缓存。
// 在重新均衡完成后调用，将新的成员集合保存到内存中，
// 用于下次重新均衡时的成员变化检测。
//
// 参数：
//   - groupID: 消费组ID
//   - newMembers: 新的消费者成员ID集合
func (c *Coordinator) updateMembers(groupID string, newMembers map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.members[groupID] = newMembers
}

// getMemberIDs 获取指定消费组当前缓存的成员ID列表。
// 用于日志记录和调试，显示消费组的当前成员状态。
//
// 参数：
//   - groupID: 消费组ID
//
// 返回值：
//   - []string: 消费者ID列表
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
//
// 参数：
//   - ctx: 上下文，用于超时控制
//   - consumers: 活跃消费者列表
//
// 返回值：
//   - []types.PartitionInfo: 所有需要分配的分区信息
//   - error: 错误信息，如果解析订阅或查询主题失败
func (c *Coordinator) getAllPartitionsForConsumers(ctx context.Context, consumers []types.ConsumerHeartbeat) ([]types.PartitionInfo, error) {
	// 使用map去重，收集所有唯一的订阅主题
	subscribedTopics := make(map[string]struct{})
	for _, consumer := range consumers {
		var topics []string
		// 解析消费者的订阅主题列表（存储为JSON格式）
		if err := json.Unmarshal(consumer.SubscribedTopics, &topics); err != nil {
			// 这是一个关键错误。如果无法解析消费者的订阅信息，
			// 就无法执行安全的重新均衡。需要中止当前周期。
			return nil, fmt.Errorf("could not unmarshal subscribed topics for consumer %s: %w", consumer.ConsumerID, err)
		}
		// 将该消费者的所有订阅主题加入到全局集合中
		for _, topic := range topics {
			subscribedTopics[topic] = struct{}{}
		}
	}

	// 将主题集合转换为切片，便于数据库查询
	topicNames := make([]string, 0, len(subscribedTopics))
	for topic := range subscribedTopics {
		topicNames = append(topicNames, topic)
	}

	// 从数据库查询主题的元数据信息
	dbTopics, err := dal.FindTopicsByNames(ctx, c.db, topicNames)
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
// 通过对消费者和分区进行排序，确保分配结果是确定性的，
// 并在消费者增减时最小化分区迁移的开销。
//
// 分配算法特点：
// 1. 确定性：相同的输入总是产生相同的分配结果
// 2. 负载均衡：使用轮询确保分区尽可能均匀分配
// 3. 稳定性：消费者变化时尽量减少不必要的分区迁移
// 4. 有序性：通过排序确保分配的一致性和可预测性
//
// 参数：
//   - consumers: 活跃消费者列表
//   - partitions: 需要分配的分区列表
//
// 返回值：
//   - map[string][]types.PartitionInfo: 消费者ID到分区列表的映射
func (c *Coordinator) calculateAssignments(consumers []types.ConsumerHeartbeat, partitions []types.PartitionInfo) map[string][]types.PartitionInfo {
	assignments := make(map[string][]types.PartitionInfo)
	if len(consumers) == 0 {
		return assignments
	}

	// ========== 关键步骤：排序以确保确定性分配 ==========
	// 对消费者按ID排序，确保分配顺序的一致性
	// 这是实现稳定分配的核心，避免相同条件下产生不同的分配结果
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})

	// 对分区按主题名称和分区号排序
	// 先按主题排序，再按分区号排序，确保分配的逻辑顺序
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
	// ========== 排序完成 ==========

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
	// 获取所有主题
	var topics []types.Topic
	if err := c.db.Find(&topics).Error; err != nil {
		return fmt.Errorf("failed to fetch topics: %v", err)
	}

	for _, topic := range topics {
		// 获取主题的保留时间配置
		retentionHours, ok := topic.GetConfig("retention_hours")
		if !ok {
			// 使用默认保留时间
			retentionHours = float64(c.config.DefaultRetentionAge.Hours())
		}

		// 计算截止时间
		cutoffTime := time.Now().Add(-time.Duration(retentionHours) * time.Hour)

		// 分批删除过期消息
		for {
			// 获取一批要删除的消息ID
			var messageIDs []int64
			err := c.db.Table("mq_messages").
				Where("topic = ? AND created_at < ?", topic.TopicName, cutoffTime).
				Limit(cleanupBatchSize).
				Pluck("id", &messageIDs).Error
			if err != nil {
				return fmt.Errorf("failed to fetch expired message IDs: %v", err)
			}

			if len(messageIDs) == 0 {
				break // 没有更多过期消息
			}

			// 删除这批消息
			err = c.db.Delete(&types.Message{}, messageIDs).Error
			if err != nil {
				return fmt.Errorf("failed to delete expired messages: %v", err)
			}

			// 检查是否需要停止
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cleanupBatchSleep):
				// 继续下一批
			}
		}
	}

	return nil
}
