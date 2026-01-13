package dbmq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/repo"
	"github.com/donutnomad/dbmq/logger"
)

const (
	leaderLockName      = "mq_coordinator_leader_lock" // 全局领导者锁名称
	lockRefreshInterval = 10 * time.Second             // 锁刷新间隔，10秒
	cleanupBatchSize    = 1000                         // 每批删除的消息数量
	cleanupBatchSleep   = 100 * time.Millisecond       // 批处理间的睡眠时间
)

// CoordinatorConfig 协调器配置结构
type CoordinatorConfig struct {
	DB                     repo.DB       // 数据库连接
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
	dao      *repo.MqRepo
	isLeader atomic.Bool // 原子布尔值，标记是否为领导者

	ctx     context.Context    // 根上下文，控制整个协调器生命周期
	cancel  context.CancelFunc // 取消函数，用于停止所有goroutine
	stopped atomic.Bool        // 原子布尔值，标记是否已停止

	wg sync.WaitGroup // 等待组，用于优雅关闭

	mu             sync.Mutex
	groupSnapshots map[string]*groupSnapshot // 缓存消费组成员和订阅/分区快照

	logger           *slog.Logger
	rebalancingLocks *groupLocks // 分消费组的重新均衡锁

	leaderSessionMu sync.Mutex
	leaderSession   *leaderSession
	leaderSessionID uint64
}

// leaderSession 表示一次完整的领导者任期（包含持有锁的连接以及派生的上下文）
type leaderSession struct {
	id     uint64
	conn   *sql.Conn
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // 当领导者主循环结束时关闭
}

// groupSnapshot 缓存每次成功重新均衡后的成员订阅和分区元数据
type groupSnapshot struct {
	memberTopics  map[string]string // consumerID -> 订阅Topic哈希，用于检测订阅变更
	partitionHash string            // 相关Topic及分区数量的哈希，用于检测Topic/分区变化
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
		groupSnapshots:   make(map[string]*groupSnapshot),
		rebalancingLocks: newGroupLocks(),
		dao:              repo.NewMqRepo(config.DB),
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
	}
	if !isLeader && wasLeader {
		c.logger.Debug("Coordinator lost global leadership.")
	}
}

// releaseConn 使用给定的连接释放MySQL锁，并确保连接被关闭
func (c *Coordinator) releaseConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", leaderLockName).Scan(&released); err != nil {
		c.logger.Error("Error releasing global leader lock", "error", err)
	} else if !released.Valid {
		c.logger.Warn("Release lock returned NULL (lock may not exist)")
	} else if released.Int64 == 0 {
		c.logger.Warn("Release lock returned 0 (lock not owned by this session)")
	} else {
		c.logger.Debug("Coordinator released global leader lock.")
	}
	if err := conn.Close(); err != nil {
		c.logger.Warn("Error closing leader lock connection", "error", err)
	}
}

// ensureLeaderLockHealth 确保当前领导者连接仍然可用（用于刷新期间的健康检查）
func (c *Coordinator) ensureLeaderLockHealth() {
	session := c.getLeaderSession()
	if session == nil {
		c.logger.Warn("Leader flag is set but no leader session is tracked, forcing step-down.")
		c.relinquishLeadership("missing leader session state")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.conn.PingContext(ctx); err != nil {
		c.logger.Warn("Leader lock connection ping failed, relinquishing leadership.", "error", err)
		c.relinquishLeadership("leader lock connection unhealthy")
	}
}

// getLeaderSession 读取当前的领导者任期信息（只读快照）
func (c *Coordinator) getLeaderSession() *leaderSession {
	c.leaderSessionMu.Lock()
	session := c.leaderSession
	c.leaderSessionMu.Unlock()
	return session
}

// startLeaderSession 基于新的 MySQL 连接创建一个领导者任期，并启动主循环
func (c *Coordinator) startLeaderSession(conn *sql.Conn) {
	if conn == nil {
		c.logger.Error("Cannot start leader session with nil connection")
		return
	}
	if c.getLeaderSession() != nil {
		c.relinquishLeadership("replacing existing leader session")
	}
	leaderCtx, cancel := context.WithCancel(c.ctx)
	sessionID := atomic.AddUint64(&c.leaderSessionID, 1)
	session := &leaderSession{
		id:     sessionID,
		conn:   conn,
		ctx:    leaderCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	c.leaderSessionMu.Lock()
	c.leaderSession = session
	c.leaderSessionMu.Unlock()

	if !c.IsLeader() {
		c.setLeader(true)
	}
	c.logger.Debug("Leader session started", "session_id", sessionID)

	c.wg.Add(1)
	go c.leaderLoop(session)
}

// relinquishLeadership 终止当前领导者任期，释放锁并更新状态
func (c *Coordinator) relinquishLeadership(reason string) {
	c.leaderSessionMu.Lock()
	session := c.leaderSession
	if session != nil {
		c.leaderSession = nil
	}
	c.leaderSessionMu.Unlock()

	if session == nil {
		if c.IsLeader() {
			c.setLeader(false)
		}
		return
	}

	if reason != "" {
		c.logger.Debug("Relinquishing leadership", "reason", reason, "session_id", session.id)
	}
	session.cancel()
	<-session.done
	c.releaseConn(session.conn)
	if c.IsLeader() {
		c.setLeader(false)
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
			c.relinquishLeadership("leader election loop context cancelled")
			return
		case <-ticker.C:
			c.attemptToBecomeLeader()
		}
	}
}

// attemptToBecomeLeader 尝试成为领导者
func (c *Coordinator) attemptToBecomeLeader() {
	if c.IsStopped() {
		c.logger.Debug("Coordinator stopped, not attempting to become leader")
		return
	}

	if c.IsLeader() {
		c.ensureLeaderLockHealth()
		return
	}

	sqlDb, err := c.dao.DB().DB()
	if err != nil {
		c.logger.Error("Error getting database connection for leader election", "error", err)
		return
	}

	conn, err := sqlDb.Conn(c.ctx)
	if err != nil {
		c.logger.Error("Error getting dedicated connection for leader election", "error", err)
		return
	}
	// releaseConn 标记用于 defer：只有在确认为领导者时才保留连接，其他场景都立即关闭
	releaseConn := true
	defer func() {
		if releaseConn {
			if err := conn.Close(); err != nil {
				c.logger.Warn("Error closing leader lock connection", "error", err)
			}
		}
	}()

	var timeout = int(lockRefreshInterval / time.Second / 2)
	c.logger.Debug("Attempting to acquire leader lock", "timeout_seconds", timeout)
	lockCtx, cancel := context.WithTimeout(c.ctx, time.Duration(timeout+2)*time.Second)
	defer cancel()

	var result int
	if err := conn.QueryRowContext(lockCtx, "SELECT GET_LOCK(?, ?)", leaderLockName, timeout).Scan(&result); err != nil {
		c.logger.Error("Error executing GET_LOCK", "error", err)
		return
	}

	c.logger.Debug("GET_LOCK result", "result", result)
	switch result {
	case 0:
		// GET_LOCK 返回0表示锁被其他会话持有；连接会在 defer 中关闭
		if c.IsLeader() {
			c.logger.Debug("Coordinator lost leadership (unable to acquire lock).")
			c.relinquishLeadership("GET_LOCK returned 0")
		}
	case 1:
		if c.ctx.Err() != nil {
			// 如果在获取期间协调器已经退出，立即释放锁，避免遗留
			c.logger.Warn("Acquired leader lock while coordinator is stopping, releasing immediately")
			releaseConn = false
			c.releaseConn(conn)
			return
		}
		releaseConn = false // 交给 leaderSession 管理连接生命周期
		c.logger.Debug("Coordinator acquired leadership.")
		c.startLeaderSession(conn)
	default:
		// MySQL 理论上只返回0/1/NULL；这里兜底并关闭连接（defer 会处理）
		c.logger.Warn("Unexpected GET_LOCK result", "result", result)
	}
}

// leaderLoop 领导者主工作循环（绑定在特定的领导者任期上）
// 只要 session.ctx 被取消，循环就会立刻退出，确保不会与新的领导者并行执行
func (c *Coordinator) leaderLoop(session *leaderSession) {
	defer c.wg.Done()
	defer close(session.done)

	c.logger.Debug("[LEADER] Coordinator leader loop started.", "session_id", session.id)
	rebalanceTicker := time.NewTicker(c.config.RebalanceInterval)
	defer rebalanceTicker.Stop()
	cleanupTicker := time.NewTicker(c.config.RetentionCheckInterval)
	defer cleanupTicker.Stop()

	// 启动时立即运行一次，使用领导者上下文，确保中途撤权时可以立刻取消
	c.scanAndRebalanceAllGroups(session.ctx)
	c.runRetentionCleanup(session.ctx)

	for {
		select {
		case <-session.ctx.Done():
			c.logger.Debug("[LEADER] Leader session context cancelled, exiting loop.", "session_id", session.id)
			return
		case <-rebalanceTicker.C:
			c.logger.Debug("[LEADER] Starting global rebalance scan...", "session_id", session.id)
			c.scanAndRebalanceAllGroups(session.ctx)
		case <-cleanupTicker.C:
			c.logger.Debug("[LEADER] Starting message retention cleanup...", "session_id", session.id)
			c.runRetentionCleanup(session.ctx)
		}
	}
}

// runRetentionCleanup 运行消息保留清理
// 根据Topic配置删除过期的消息
func (c *Coordinator) runRetentionCleanup(ctx context.Context) {
	if ctx == nil {
		ctx = c.ctx
	}
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
			p := db.PartitionInfo{Topic: topic.TopicName, Partition: i}
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

// scanAndRebalanceAllGroups 由领导者循环驱动，扫描并触发所有消费组的重新均衡
// parentCtx 为领导者任期派生出来的上下文，可在撤权时立刻取消
func (c *Coordinator) scanAndRebalanceAllGroups(parentCtx context.Context) {
	if parentCtx == nil {
		parentCtx = c.ctx
	}
	ctx, cancel := context.WithTimeout(parentCtx, 1*time.Minute)
	defer cancel()

	// 找到活跃的消费组IDs
	activeGroupIds, err := c.dao.FindAllActiveGroups(ctx, c.config.HeartbeatTimeout)
	if err != nil {
		c.logger.Error("[LEADER] Failed to scan for active groups", "error", err)
		return
	}
	if len(activeGroupIds) == 0 {
		return
	}

	c.logger.Debug(fmt.Sprintf("[LEADER] [scanAndRebalanceAllGroups] Found active consumer groups: %v", activeGroupIds))
	for _, groupID := range activeGroupIds {
		if err := c.rebalanceIfNeeded(ctx, groupID); err != nil {
			c.logger.Error("[LEADER] Rebalance failed for group", "group", groupID, "error", err)
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
func (c *Coordinator) rebalanceIfNeeded(parentCtx context.Context, groupID string) error {
	if parentCtx == nil {
		parentCtx = c.ctx
	}
	// ========== 第一步：获取分组锁，确保重新均衡的串行执行 ==========
	// 尝试获取重新均衡锁。如果已被占用，说明另一个重新均衡循环正在运行。
	// 这是一个关键的并发控制机制，避免同一消费组的多个重新均衡操作并发执行，
	// 防止数据竞争和状态不一致。
	if !c.rebalancingLocks.TryLock(groupID) {
		c.logger.Debug("[LEADER] Rebalance check for group skipped", "group", groupID, "reason", "another rebalance is already in progress")
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
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	// ========== 第二步：发现活跃消费者 ==========
	// 查找该消费组中所有活跃的消费者。活跃性通过心跳超时判断：
	// 如果消费者在HeartbeatTimeout时间内没有发送心跳，则认为已死亡。
	// 这是重新均衡决策的基础数据。
	activeConsumers, err := c.dao.FindActiveConsumers(ctx, groupID, c.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to find active consumers: %w", err)
	}

	// 记录本轮活跃的消费者ID集合，仅用于日志输出
	activeConsumerIDs := make(map[string]struct{}, len(activeConsumers))
	for _, consumer := range activeConsumers {
		activeConsumerIDs[consumer.ConsumerID] = struct{}{}
	}

	allPartitions, partitionHash, err := c.getAllPartitionsForConsumers(ctx, activeConsumers)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get partitions for consumers: %w", err)
	}

	if !c.isRebalanceNeeded(groupID, activeConsumers, partitionHash) {
		return nil // 没有变化，无需重新均衡
	}

	c.logger.Info(fmt.Sprintf("[LEADER] Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, c.getMemberIDs(groupID), mapKeys(activeConsumerIDs)))

	// ========== 第四步：开始重新均衡协议 - 代际隔离 ==========
	// 递增代际ID。这是DBMQ的核心隔离机制：
	// - 新代际ID会使所有旧消费者的请求失效（代际不匹配）
	// - 防止旧消费者继续处理消息，避免重复消费
	// - 实现"围栏"效应，确保只有新分配的消费者能工作
	newGenerationID, err := c.dao.IncrementAndGetGenerationID(ctx, groupID)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to increment generation id: %w", err)
	}

	// 特殊情况：如果没有活跃消费者，只需递增代际并结束
	if len(activeConsumers) == 0 {
		if err := c.dao.UpdateAssignments(ctx, groupID, newGenerationID, map[string][]db.PartitionInfo{}); err != nil {
			return fmt.Errorf("[LEADER] failed to clear assignments for empty group: %w", err)
		}
		c.updateGroupSnapshot(groupID, nil, "")
		c.logger.Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' completed with no active consumers. Generation: %d", groupID, newGenerationID))
		return nil
	}

	// 分配分区
	newAssignments := c.calculateAssignments(activeConsumers, allPartitions)

	// 持久化到数据库中
	err = c.dao.UpdateAssignments(ctx, groupID, newGenerationID, newAssignments)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to update assignments: %w", err)
	}

	// 记录新的消费组快照
	c.updateGroupSnapshot(groupID, activeConsumers, partitionHash)
	c.logger.Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' to generation %d completed successfully.", groupID, newGenerationID))
	return nil
}

// isRebalanceNeeded 检查是否需要对指定消费组进行重新均衡。
// 除了成员集合外，还会比较每个成员的订阅主题和Topic/Partition元数据哈希。
func (c *Coordinator) isRebalanceNeeded(groupID string, consumers []db.ConsumerHeartbeat, partitionHash string) bool {
	c.mu.Lock()
	snapshot := c.groupSnapshots[groupID]
	c.mu.Unlock()

	if snapshot == nil {
		return len(consumers) > 0 || partitionHash != ""
	}

	if len(consumers) != len(snapshot.memberTopics) {
		return true
	}

	for _, consumer := range consumers {
		topicHash := hashSubscribedTopics(consumer.SubscribedTopics)
		if cachedHash, ok := snapshot.memberTopics[consumer.ConsumerID]; !ok || cachedHash != topicHash {
			return true
		}
	}

	return snapshot.partitionHash != partitionHash
}

func (c *Coordinator) updateGroupSnapshot(groupID string, consumers []db.ConsumerHeartbeat, partitionHash string) {
	snapshot := &groupSnapshot{
		memberTopics:  make(map[string]string, len(consumers)),
		partitionHash: partitionHash,
	}
	for _, consumer := range consumers {
		snapshot.memberTopics[consumer.ConsumerID] = hashSubscribedTopics(consumer.SubscribedTopics)
	}
	c.mu.Lock()
	if len(snapshot.memberTopics) == 0 && partitionHash == "" {
		delete(c.groupSnapshots, groupID)
	} else {
		c.groupSnapshots[groupID] = snapshot
	}
	c.mu.Unlock()
}

func hashSubscribedTopics(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	sorted := slices.Clone(topics)
	sort.Strings(sorted)
	return strings.Join(sorted, "|")
}

func mapKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// getMemberIDs 获取指定消费组当前缓存的成员ID列表。
func (c *Coordinator) getMemberIDs(groupID string) []string {
	c.mu.Lock()
	snapshot := c.groupSnapshots[groupID]
	c.mu.Unlock()

	if snapshot == nil {
		return nil
	}
	ids := make([]string, 0, len(snapshot.memberTopics))
	for id := range snapshot.memberTopics {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// getAllPartitionsForConsumers 收集活跃消费者订阅的所有唯一主题，
// 并返回这些主题的所有分区列表及一个Topic/Partition哈希。
// 该哈希用于检测Topic扩容或缩容等元数据变化。
func (c *Coordinator) getAllPartitionsForConsumers(ctx context.Context, consumers []db.ConsumerHeartbeat) ([]db.PartitionInfo, string, error) {
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
		return nil, "", fmt.Errorf("failed to find topics by name: %w", err)
	}

	// 为每个主题生成所有分区的完整列表
	var allPartitions []db.PartitionInfo
	sort.Slice(dbTopics, func(i, j int) bool {
		return dbTopics[i].TopicName < dbTopics[j].TopicName
	})
	var partitionHashBuilder strings.Builder
	for _, topic := range dbTopics {
		if partitionHashBuilder.Len() > 0 {
			partitionHashBuilder.WriteString("|")
		}
		fmt.Fprintf(&partitionHashBuilder, "%s:%d", topic.TopicName, topic.PartitionCount)
		// 根据主题的分区数量，生成从0到PartitionCount-1的所有分区
		for i := uint(0); i < topic.PartitionCount; i++ {
			allPartitions = append(allPartitions, db.PartitionInfo{Topic: topic.TopicName, Partition: i})
		}
	}
	return allPartitions, partitionHashBuilder.String(), nil
}

// calculateAssignments 使用稳定的轮询策略在消费者之间分配分区。
// 通过对消费者和分区进行排序，确保分配结果是确定性的，并在消费者增减时最小化分区迁移的开销
func (c *Coordinator) calculateAssignments(consumers []db.ConsumerHeartbeat, partitions []db.PartitionInfo) map[string][]db.PartitionInfo {
	assignments := make(map[string][]db.PartitionInfo)
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
		assignments[consumer.ConsumerID] = []db.PartitionInfo{}
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
