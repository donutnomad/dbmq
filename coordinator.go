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

	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/donutnomad/dbmq/logger"
)

const (
	leaderLockPrefix    = "mq_coordinator_leader_lock_" // 全局领导者锁名称前缀
	lockRefreshInterval = 10 * time.Second              // 锁刷新间隔，10秒
	cleanupBatchSize    = 1000                          // 每批删除的消息数量
	cleanupBatchSleep   = 100 * time.Millisecond        // 批处理间的睡眠时间
)

// CoordinatorConfig 协调器配置结构
type CoordinatorConfig struct {
	LockSuffix             string        // 锁后缀，用于区分不同应用的协调器（必填）
	DB                     interfaces.DB // 数据库连接
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
	lockName string            // 完整的锁名称（前缀+后缀）

	// 依赖（domain 层接口）
	topicRepo            topic.Repo
	messageRepo          message.Repo
	heartbeatRepo        heartbeat.Repo
	groupRepo            consumergroup.Repo
	progressRepo         consumerprogress.Repo
	manualAssignmentRepo manualassignment.Repo
	db                   interfaces.DB

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
	generationID         uint              // 最后一次成功 rebalance 后的 generation_id
	memberTopics         map[string]string // consumerID -> 订阅Topic哈希，用于检测订阅变更
	partitionHash        string            // 相关Topic及分区数量的哈希，用于检测Topic/分区变化
	manualAssignmentHash string            // 手动分配规则的哈希，用于检测手动分配变化
}

// NewCoordinator 创建一个新的协调器
// 拥有全局领导者锁的协调器还将执行系统级任务，如消息清理
// LockSuffix 是必填参数，用于区分不同应用的协调器
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	// 验证必填参数
	if config.LockSuffix == "" {
		panic("CoordinatorConfig.LockSuffix is required to distinguish different applications")
	}

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
		config:               config,
		lockName:             leaderLockPrefix + config.LockSuffix,
		ctx:                  ctx,
		cancel:               cancel,
		groupSnapshots:       make(map[string]*groupSnapshot),
		rebalancingLocks:     newGroupLocks(),
		topicRepo:            topicrepo.New(config.DB),
		messageRepo:          messagerepo.New(config.DB),
		heartbeatRepo:        heartbeatrepo.New(config.DB),
		groupRepo:            consumergrouprepo.New(config.DB),
		progressRepo:         consumerprogressrepo.New(config.DB),
		manualAssignmentRepo: manualassignmentrepo.New(config.DB),
		db:                   config.DB,
	}
}

func (c *Coordinator) getLogger() *slog.Logger {
	return logger.GetLogger().With("component", "coordinator", "lock_suffix", c.config.LockSuffix)
}

// Start 开始协调器的工作，包括领导者选举
func (c *Coordinator) Start() {
	// 检查是否已经停止，防止重复启动
	if c.stopped.Load() {
		c.getLogger().Debug("Coordinator has already been stopped, cannot start again")
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
	c.getLogger().Debug("Coordinator stopping...")

	// 取消所有goroutine的context
	c.cancel()

	// 等待所有goroutine完成
	c.wg.Wait()

	c.getLogger().Debug("Coordinator stopped successfully")
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
		c.getLogger().Debug("Coordinator became the global leader.")
	}
	if !isLeader && wasLeader {
		c.getLogger().Debug("Coordinator lost global leadership.")
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
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", c.lockName).Scan(&released); err != nil {
		c.getLogger().Error("Error releasing global leader lock", "error", err)
	} else if !released.Valid {
		c.getLogger().Warn("Release lock returned NULL (lock may not exist)")
	} else if released.Int64 == 0 {
		c.getLogger().Warn("Release lock returned 0 (lock not owned by this session)")
	} else {
		c.getLogger().Debug("Coordinator released global leader lock.")
	}
	if err := conn.Close(); err != nil {
		c.getLogger().Warn("Error closing leader lock connection", "error", err)
	}
}

// ensureLeaderLockHealth 确保当前领导者连接仍然可用（用于刷新期间的健康检查）
func (c *Coordinator) ensureLeaderLockHealth() {
	session := c.getLeaderSession()
	if session == nil {
		c.getLogger().Warn("Leader flag is set but no leader session is tracked, forcing step-down.")
		c.relinquishLeadership("missing leader session state")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.conn.PingContext(ctx); err != nil {
		c.getLogger().Warn("Leader lock connection ping failed, relinquishing leadership.", "error", err)
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
		c.getLogger().Error("Cannot start leader session with nil connection")
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
	c.getLogger().Debug("Leader session started", "session_id", sessionID)

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
		c.getLogger().Debug("Relinquishing leadership", "reason", reason, "session_id", session.id)
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
		c.getLogger().Debug("Coordinator stopped, not attempting to become leader")
		return
	}

	if c.IsLeader() {
		c.ensureLeaderLockHealth()
		return
	}

	sqlDb, err := c.db.DB()
	if err != nil {
		c.getLogger().Error("Error getting database connection for leader election", "error", err)
		return
	}

	conn, err := sqlDb.Conn(c.ctx)
	if err != nil {
		c.getLogger().Error("Error getting dedicated connection for leader election", "error", err)
		return
	}
	// releaseConn 标记用于 defer：只有在确认为领导者时才保留连接，其他场景都立即关闭
	releaseConn := true
	defer func() {
		if releaseConn {
			if err := conn.Close(); err != nil {
				c.getLogger().Warn("Error closing leader lock connection", "error", err)
			}
		}
	}()

	var timeout = int(lockRefreshInterval / time.Second / 2)
	c.getLogger().Debug("Attempting to acquire leader lock", "timeout_seconds", timeout)
	lockCtx, cancel := context.WithTimeout(c.ctx, time.Duration(timeout+2)*time.Second)
	defer cancel()

	var result int
	if err := conn.QueryRowContext(lockCtx, "SELECT GET_LOCK(?, ?)", c.lockName, timeout).Scan(&result); err != nil {
		c.getLogger().Error("Error executing GET_LOCK", "error", err)
		return
	}

	c.getLogger().Debug("GET_LOCK result", "result", result)
	switch result {
	case 0:
		// GET_LOCK 返回0表示锁被其他会话持有；连接会在 defer 中关闭
		if c.IsLeader() {
			c.getLogger().Debug("Coordinator lost leadership (unable to acquire lock).")
			c.relinquishLeadership("GET_LOCK returned 0")
		}
	case 1:
		if c.ctx.Err() != nil {
			// 如果在获取期间协调器已经退出，立即释放锁，避免遗留
			c.getLogger().Warn("Acquired leader lock while coordinator is stopping, releasing immediately")
			releaseConn = false
			c.releaseConn(conn)
			return
		}
		releaseConn = false // 交给 leaderSession 管理连接生命周期
		c.getLogger().Debug("Coordinator acquired leadership.")
		c.startLeaderSession(conn)
	default:
		// MySQL 理论上只返回0/1/NULL；这里兜底并关闭连接（defer 会处理）
		c.getLogger().Warn("Unexpected GET_LOCK result", "result", result)
	}
}

// leaderLoop 领导者主工作循环（绑定在特定的领导者任期上）
// 只要 session.ctx 被取消，循环就会立刻退出，确保不会与新的领导者并行执行
func (c *Coordinator) leaderLoop(session *leaderSession) {
	defer c.wg.Done()
	defer close(session.done)

	c.getLogger().Debug("[LEADER] Coordinator leader loop started.", "session_id", session.id)
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
			c.getLogger().Debug("[LEADER] Leader session context cancelled, exiting loop.", "session_id", session.id)
			return
		case <-rebalanceTicker.C:
			c.getLogger().Debug("[LEADER] Starting global rebalance scan...", "session_id", session.id)
			c.scanAndRebalanceAllGroups(session.ctx)
		case <-cleanupTicker.C:
			c.getLogger().Debug("[LEADER] Starting message retention cleanup...", "session_id", session.id)
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
		c.getLogger().Debug("Coordinator stopped, skipping message retention cleanup.")
		return
	}

	c.getLogger().Debug("Starting message retention cleanup cycle.")
	startTime := time.Now()

	// 1. 获取所有Topic以了解它们的保留策略
	allTopics, err := c.topicRepo.GetAll(ctx)
	if err != nil {
		c.getLogger().Error("Cleanup failed to get topics", "error", err)
		return
	}
	topicConfigMap := make(map[string]time.Duration)
	for _, t := range allTopics {
		retentionMs, _ := t.GetConfig("retention_ms")
		if retentionMs > 0 {
			topicConfigMap[t.Name] = time.Duration(retentionMs) * time.Millisecond
		}
	}

	// 2. Calculate global low watermark for all consumed partitions
	watermarks, err := c.progressRepo.GetLowWatermarks(ctx)
	if err != nil {
		c.getLogger().Error("Cleanup failed to get low watermarks", "error", err)
		return
	}
	c.getLogger().Debug(fmt.Sprintf("Found %d consumed partitions with a low watermark.", len(watermarks)))

	var totalDeletedCount int64

	// 3. Iterate through all partitions of all topics and apply deletion logic
	for _, t := range allTopics {
		retentionAge := c.config.DefaultRetentionAge
		if configuredAge, ok := topicConfigMap[t.Name]; ok {
			retentionAge = configuredAge
		}
		retentionDate := time.Now().Add(-retentionAge)

		for i := uint(0); i < t.PartitionCount; i++ {
			p := types.PartitionInfo{Topic: t.Name, Partition: i}
			partitionTotalDeleted := int64(0)

			// Loop to delete in batches until no more rows are affected
			for {
				var deletedCount int64
				var err error

				if lowWatermark, ok := watermarks[p]; ok {
					// This partition is consumed, so use the low watermark
					deletedCount, err = c.messageRepo.DeleteConsumed(ctx, p.Topic, p.Partition, lowWatermark, retentionDate, cleanupBatchSize)
				} else {
					// This partition is not in the watermark map, meaning no group has ever committed an offset for it.
					// We can only clean it up based on time.
					deletedCount, err = c.messageRepo.DeleteExpired(ctx, p.Topic, p.Partition, retentionDate, cleanupBatchSize)
				}

				if err != nil {
					c.getLogger().Error("Failed to clean partition", "partition", p, "error", err)
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
					c.getLogger().Debug("Cleanup cancelled during batch processing for partition", "partition", p)
					return
				case <-time.After(cleanupBatchSleep):
					// Continue to next batch
				}
			}

			if partitionTotalDeleted > 0 {
				totalDeletedCount += partitionTotalDeleted
				c.getLogger().Debug(fmt.Sprintf("Cleaned up %d messages from partition %v", partitionTotalDeleted, p))
			}
		}
	}

	c.getLogger().Debug(fmt.Sprintf("Finished message retention cleanup cycle in %v. Total messages deleted: %d", time.Since(startTime), totalDeletedCount))
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
	activeGroupIds, err := c.groupRepo.FindAllActiveGroups(ctx, c.config.HeartbeatTimeout)
	if err != nil {
		c.getLogger().Error("[LEADER] Failed to scan for active groups", "error", err)
		return
	}
	if len(activeGroupIds) == 0 {
		return
	}

	c.getLogger().Debug(fmt.Sprintf("[LEADER] [scanAndRebalanceAllGroups] Found active consumer groups: %v", activeGroupIds))
	for _, groupID := range activeGroupIds {
		if err := c.rebalanceIfNeeded(ctx, groupID); err != nil {
			c.getLogger().Error("[LEADER] Rebalance failed for group", "group", groupID, "error", err)
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
		c.getLogger().Debug("[LEADER] Rebalance check for group skipped", "group", groupID, "reason", "another rebalance is already in progress")
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
	activeConsumers, err := c.heartbeatRepo.FindActive(ctx, groupID, c.config.HeartbeatTimeout)
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

	// 获取当前 generation_id，用于检测外部触发的重新均衡（如 TriggerRebalance API）
	var currentGenerationID uint
	if gen, err := c.groupRepo.GetGeneration(ctx, groupID); err != nil {
		return fmt.Errorf("[LEADER] failed to get generation: %w", err)
	} else if gen != nil {
		currentGenerationID = gen.GenerationID
	}

	if !c.isRebalanceNeeded(groupID, activeConsumers, partitionHash, currentGenerationID) {
		return nil // 没有变化，无需重新均衡
	}

	c.getLogger().Info(fmt.Sprintf("[LEADER] Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, c.getMemberIDs(groupID), mapKeys(activeConsumerIDs)))

	// ========== 第四步：开始重新均衡协议 - 代际隔离 ==========
	// 特殊情况：如果没有活跃消费者，只需原子递增代际并清空所有分配
	if len(activeConsumers) == 0 {
		newGenerationID, err := c.groupRepo.IncrementAndUpdateAssignments(ctx, groupID, map[string][]types.PartitionInfo{})
		if err != nil {
			return fmt.Errorf("[LEADER] failed to increment generation for empty group: %w", err)
		}
		c.updateGroupSnapshot(groupID, newGenerationID, nil, "", nil)
		c.getLogger().Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' completed with no active consumers. Generation: %d", groupID, newGenerationID))
		return nil
	}

	// 获取所有消费者的 ID 列表
	consumerIDs := make([]string, 0, len(activeConsumers))
	for _, c := range activeConsumers {
		consumerIDs = append(consumerIDs, c.ConsumerID)
	}

	// 查询手动分配配置
	manualAssignments, err := c.manualAssignmentRepo.GetMatching(ctx, groupID, consumerIDs)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get manual assignments: %w", err)
	}

	// 分配分区（传入手动配置）
	newAssignments := c.calculateAssignments(activeConsumers, allPartitions, manualAssignments)

	// 原子地递增代际 ID 并持久化分区分配，消除两阶段提交竞态窗口
	newGenerationID, err := c.groupRepo.IncrementAndUpdateAssignments(ctx, groupID, newAssignments)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to increment generation and update assignments: %w", err)
	}

	// 记录新的消费组快照
	c.updateGroupSnapshot(groupID, newGenerationID, activeConsumers, partitionHash, manualAssignments)
	c.getLogger().Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' to generation %d completed successfully.", groupID, newGenerationID))
	return nil
}

// isRebalanceNeeded 检查是否需要对指定消费组进行重新均衡。
// 除了成员集合外，还会比较每个成员的订阅主题、Topic/Partition元数据哈希，
// 以及 generation_id（检测外部触发的重新均衡，如 TriggerRebalance API）。
func (c *Coordinator) isRebalanceNeeded(groupID string, consumers []*heartbeat.Heartbeat, partitionHash string, currentGenerationID uint) bool {
	c.mu.Lock()
	snapshot := c.groupSnapshots[groupID]
	c.mu.Unlock()

	if snapshot == nil {
		return len(consumers) > 0 || partitionHash != ""
	}

	// 检查 generation_id 是否被外部修改（如 TriggerRebalance API 递增了 generations 表）
	if currentGenerationID != snapshot.generationID {
		c.getLogger().Info("[LEADER] 检测到 generation_id 被外部修改，强制触发 rebalance",
			"group_id", groupID,
			"snapshot_generation", snapshot.generationID,
			"current_generation", currentGenerationID,
		)
		return true
	}

	// 检查消费者数量变化
	if len(consumers) != len(snapshot.memberTopics) {
		return true
	}

	// 检查订阅 Topic 变化
	for _, consumer := range consumers {
		topicHash := hashSubscribedTopics(consumer.SubscribedTopics)
		if cachedHash, ok := snapshot.memberTopics[consumer.ConsumerID]; !ok || cachedHash != topicHash {
			return true
		}
	}

	// 检查分区变化
	if snapshot.partitionHash != partitionHash {
		return true
	}

	// 检查手动分配规则是否变化
	ctx := context.Background()
	consumerIDs := make([]string, 0, len(consumers))
	for _, c := range consumers {
		consumerIDs = append(consumerIDs, c.ConsumerID)
	}

	manualAssignments, err := c.manualAssignmentRepo.GetMatching(ctx, groupID, consumerIDs)
	if err != nil {
		// 查询失败时保守触发 rebalance
		c.getLogger().Warn("[LEADER] Failed to check manual assignments, triggering rebalance", "error", err)
		return true
	}

	currentManualHash := hashManualAssignments(manualAssignments)
	if snapshot.manualAssignmentHash != currentManualHash {
		c.getLogger().Info("[LEADER] 🎯 检测到手动分配规则变化，触发 rebalance",
			"group_id", groupID,
			"old_hash", snapshot.manualAssignmentHash,
			"new_hash", currentManualHash,
		)
		return true
	}

	return false
}

func (c *Coordinator) updateGroupSnapshot(groupID string, generationID uint, consumers []*heartbeat.Heartbeat, partitionHash string, manualAssignments map[string][]types.PartitionInfo) {
	snapshot := &groupSnapshot{
		generationID:         generationID,
		memberTopics:         make(map[string]string, len(consumers)),
		partitionHash:        partitionHash,
		manualAssignmentHash: hashManualAssignments(manualAssignments),
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

// hashManualAssignments 计算手动分配规则的哈希值，用于检测规则变化
func hashManualAssignments(assignments map[string][]types.PartitionInfo) string {
	if len(assignments) == 0 {
		return ""
	}

	// 收集所有条目并排序，确保结果可重现
	type entry struct {
		consumerID string
		partitions []types.PartitionInfo
	}

	entries := make([]entry, 0, len(assignments))
	for consumerID, partitions := range assignments {
		// 克隆并排序分区列表
		sortedParts := slices.Clone(partitions)
		utils.SortPartitionsByTopicAndPartition(sortedParts)
		entries = append(entries, entry{
			consumerID: consumerID,
			partitions: sortedParts,
		})
	}

	// 按 consumerID 排序
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].consumerID < entries[j].consumerID
	})

	// 构建哈希字符串
	var parts []string
	for _, e := range entries {
		for _, p := range e.partitions {
			parts = append(parts, fmt.Sprintf("%s:%s:%d", e.consumerID, p.Topic, p.Partition))
		}
	}

	return strings.Join(parts, "|")
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
func (c *Coordinator) getAllPartitionsForConsumers(ctx context.Context, consumers []*heartbeat.Heartbeat) ([]types.PartitionInfo, string, error) {
	// 使用map去重，收集所有唯一的订阅主题
	subscribedTopics := make(map[string]struct{})
	for _, consumer := range consumers {
		// 将该消费者的所有订阅主题加入到全局集合中
		for _, topicName := range consumer.SubscribedTopics {
			subscribedTopics[topicName] = struct{}{}
		}
	}

	// 将主题集合转换为切片，便于数据库查询
	topicNames := make([]string, 0, len(subscribedTopics))
	for topicName := range subscribedTopics {
		topicNames = append(topicNames, topicName)
	}

	// 从数据库查询主题的元数据信息
	dbTopics, err := c.topicRepo.FindByNames(ctx, topicNames)
	if err != nil {
		return nil, "", fmt.Errorf("failed to find topics by name: %w", err)
	}

	// 为每个主题生成所有分区的完整列表
	var allPartitions []types.PartitionInfo
	sort.Slice(dbTopics, func(i, j int) bool {
		return dbTopics[i].Name < dbTopics[j].Name
	})
	var partitionHashBuilder strings.Builder
	for _, t := range dbTopics {
		if partitionHashBuilder.Len() > 0 {
			partitionHashBuilder.WriteString("|")
		}
		fmt.Fprintf(&partitionHashBuilder, "%s:%d", t.Name, t.PartitionCount)
		// 根据主题的分区数量，生成从0到PartitionCount-1的所有分区
		for i := uint(0); i < t.PartitionCount; i++ {
			allPartitions = append(allPartitions, types.PartitionInfo{Topic: t.Name, Partition: i})
		}
	}
	return allPartitions, partitionHashBuilder.String(), nil
}

// calculateAssignments 使用稳定的轮询策略在消费者之间分配分区，支持手动分配覆盖。
// 通过对消费者和分区进行排序，确保分配结果是确定性的，并在消费者增减时最小化分区迁移的开销。
// manualAssignments 参数允许为特定消费者指定固定的分区分配，这些分区不会参与自动轮询分配。
func (c *Coordinator) calculateAssignments(
	consumers []*heartbeat.Heartbeat,
	partitions []types.PartitionInfo,
	manualAssignments map[string][]types.PartitionInfo,
) map[string][]types.PartitionInfo {
	assignments := make(map[string][]types.PartitionInfo)
	if len(consumers) == 0 {
		return assignments
	}

	// 对消费者按ID排序，确保分配顺序的一致性。这是实现稳定分配的核心，避免相同条件下产生不同的分配结果
	utils.SortHeartbeatsByID(consumers)

	// 先按主题排序，再按分区号排序，确保分配的逻辑顺序
	utils.SortPartitionsByTopicAndPartition(partitions)

	// Step 1: 处理手动分配的消费者
	// 记录已被手动分配的分区，避免重复分配
	manualAssignedPartitions := make(map[types.PartitionInfo]struct{})
	var autoConsumers []*heartbeat.Heartbeat

	for _, consumer := range consumers {
		if manualParts, hasManual := manualAssignments[consumer.ConsumerID]; hasManual && len(manualParts) > 0 {
			// 手动分配：直接分配配置的分区
			assignments[consumer.ConsumerID] = manualParts
			for _, p := range manualParts {
				manualAssignedPartitions[p] = struct{}{}
			}
		} else {
			// 自动模式：加入待分配列表
			autoConsumers = append(autoConsumers, consumer)
			assignments[consumer.ConsumerID] = []types.PartitionInfo{}
		}
	}

	// Step 2: 从分区池中移除已手动分配的分区
	var remainingPartitions []types.PartitionInfo
	for _, p := range partitions {
		if _, isManual := manualAssignedPartitions[p]; !isManual {
			remainingPartitions = append(remainingPartitions, p)
		}
	}

	// Step 3: 剩余分区按轮询策略分配给自动模式的消费者
	if len(autoConsumers) > 0 && len(remainingPartitions) > 0 {
		// 提取自动模式消费者的ID列表
		autoConsumerIDs := make([]string, 0, len(autoConsumers))
		for _, consumer := range autoConsumers {
			autoConsumerIDs = append(autoConsumerIDs, consumer.ConsumerID)
		}

		// 使用轮询算法分配剩余分区
		// 第i个分区分配给第(i % 消费者数量)个消费者
		// 这确保了分区的均匀分布，负载均衡效果最优
		for i, p := range remainingPartitions {
			consumerID := autoConsumerIDs[i%len(autoConsumerIDs)]
			assignments[consumerID] = append(assignments[consumerID], p)
		}
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
