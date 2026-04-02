package dbmq

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	leader "github.com/donutnomad/dbleader"
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
	"github.com/samber/lo"
)

const (
	leaderLockPrefix  = "mq_coordinator_leader_lock_" // 全局领导者锁名称前缀
	leaderLockTable   = "mq_coordinator_leader_lock"  // leader 选举锁表名
	cleanupBatchSize  = 1000                          // 每批删除的消息数量
	cleanupBatchSleep = 100 * time.Millisecond        // 批处理间的睡眠时间
)

// CoordinatorConfig 协调器配置结构
type CoordinatorConfig struct {
	LockSuffix             string        // 锁后缀，用于区分不同应用的协调器（必填）
	NodeAddr               string        // 节点地址标识，用于 leader 选举（必填）
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
	config CoordinatorConfig // 协调器配置

	// 依赖（domain 层接口）
	topicRepo            topic.Repo
	messageRepo          message.Repo
	heartbeatRepo        heartbeat.Repo
	groupRepo            consumergroup.Repo
	progressRepo         consumerprogress.Repo
	manualAssignmentRepo manualassignment.Repo
	db                   interfaces.DB

	manager *leader.Manager // dbleader 管理器，负责 leader 选举和续约

	mu             sync.Mutex
	groupSnapshots map[string]*groupSnapshot // 缓存消费组成员和订阅/分区快照

	logger           *slog.Logger
	rebalancingLocks *groupLocks // 分消费组的重新均衡锁
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
// LockSuffix 和 NodeAddr 是必填参数
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	// 验证必填参数
	if config.LockSuffix == "" {
		panic("CoordinatorConfig.LockSuffix is required to distinguish different applications")
	}
	if config.NodeAddr == "" {
		panic("CoordinatorConfig.NodeAddr is required for leader election")
	}

	// 设置默认值
	if config.RebalanceInterval == 0 { // 重平衡间隔
		config.RebalanceInterval = 10 * time.Second
	}
	if config.HeartbeatTimeout == 0 { // 消费心跳时间
		config.HeartbeatTimeout = 30 * time.Second
	}
	if config.DefaultRetentionAge == 0 { // 消息保留: 默认7天保留期
		config.DefaultRetentionAge = 7 * 24 * time.Hour
	}
	if config.RetentionCheckInterval == 0 { // 消息保留: 默认每小时检查一次
		config.RetentionCheckInterval = 1 * time.Hour
	}

	lockName := leaderLockPrefix + config.LockSuffix
	c := &Coordinator{
		config:               config,
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

	store := leader.NewMysqlLockStore(config.DB, leaderLockTable)
	c.manager = leader.NewManager(store, lockName, config.NodeAddr, []leader.LeaderTask{&coordinatorTask{c}})
	return c
}

func (c *Coordinator) getLogger() *slog.Logger {
	return logger.GetLogger().With("component", "coordinator", "lock_suffix", c.config.LockSuffix)
}

// coordinatorTask 包装 Coordinator 实现 leader.LeaderTask 接口
// 避免与 Coordinator.Start() 方法签名冲突
type coordinatorTask struct {
	c *Coordinator
}

func (t *coordinatorTask) Name() string { return "coordinator" }

// Start 实现 leader.LeaderTask 接口，被 dbleader.Manager 在获得锁后调用
// 阻塞运行，ctx 取消时优雅退出
func (t *coordinatorTask) Start(ctx context.Context) error {
	c := t.c
	c.getLogger().Debug("[LEADER] Coordinator leader loop started.")

	var wg sync.WaitGroup
	wg.Go(func() {
		_ = cronRun(ctx, c.config.RebalanceInterval, 0, func(ctx context.Context) {
			c.getLogger().Debug("[LEADER] Starting global rebalance scan...")
			c.scanAndRebalanceAllGroups(ctx)
		})
	})
	wg.Go(func() {
		_ = cronRun(ctx, c.config.RetentionCheckInterval, 0, func(ctx context.Context) {
			c.getLogger().Debug("[LEADER] Starting message retention cleanup...")
			c.runRetentionCleanup(ctx)
		})
	})
	wg.Wait()
	return ctx.Err()
}

// Start 开始协调器的工作，包括领导者选举
func (c *Coordinator) Start() {
	c.manager.Start()
}

// Stop 优雅关闭协调器
func (c *Coordinator) Stop() {
	c.getLogger().Debug("Coordinator stopping...")
	c.manager.Stop()
	c.getLogger().Debug("Coordinator stopped successfully")
}

// IsLeader 返回此协调器实例是否为当前领导者
func (c *Coordinator) IsLeader() bool {
	return c.manager.IsLeader()
}

// runRetentionCleanup 根据Topic配置删除过期的消息
func (c *Coordinator) runRetentionCleanup(ctx context.Context) {
	// 使用传入的context作为父context，确保在领导权丢失时能够快速退出
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute) // 清理的慷慨超时时间
	defer cancel()

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

// scanAndRebalanceAllGroups 扫描并触发所有消费组的重新均衡
func (c *Coordinator) scanAndRebalanceAllGroups(parentCtx context.Context) {
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

	// 获取消费者的分区数量
	allPartitions, partitionHash, err := c.getAllPartitionsForConsumers(ctx, activeConsumers)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get partitions for consumers: %w", err)
	}

	// 获取当前 generation_id，用于检测外部触发的重新均衡（如 TriggerRebalance API）
	gen, err := c.groupRepo.GetGeneration(ctx, groupID)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get generation: %w", err)
	}

	var currentGenerationID uint = 0
	if gen != nil {
		currentGenerationID = gen.GenerationID
	}

	if !c.isRebalanceNeeded(ctx, groupID, activeConsumers, partitionHash, currentGenerationID) {
		return nil // 没有变化，无需重新均衡
	}

	c.getLogger().Info(fmt.Sprintf("[LEADER] Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, c.getMemberIDs(groupID), getHeartbeatConsumerIDs(activeConsumers)))

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

	// 查询手动分配配置
	manualAssignments, err := c.manualAssignmentRepo.GetMatching(ctx, groupID, getHeartbeatConsumerIDs(activeConsumers))
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get manual assignments: %w", err)
	}
	// 分配分区（传入手动配置）
	newAssignments := calculateAssignments(activeConsumers, allPartitions, manualAssignments)
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
func (c *Coordinator) isRebalanceNeeded(ctx context.Context, groupID string, consumers []*heartbeat.Heartbeat, partitionHash string, currentGenerationID uint) bool {
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
	consumerIDs := getHeartbeatConsumerIDs(consumers)

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
	// 收集Topic
	var topicNames []string
	for _, consumer := range consumers {
		for _, topicName := range consumer.SubscribedTopics {
			topicNames = append(topicNames, topicName)
		}
	}
	topicNames = lo.Uniq(topicNames)

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
		_, _ = fmt.Fprintf(&partitionHashBuilder, "%s:%d", t.Name, t.PartitionCount)
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
func calculateAssignments(
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

func getHeartbeatConsumerIDs(hs []*heartbeat.Heartbeat) []string {
	return lo.Map(hs, func(item *heartbeat.Heartbeat, index int) string {
		return item.ConsumerID
	})
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
