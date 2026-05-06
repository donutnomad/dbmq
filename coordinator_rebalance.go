package dbmq

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/donutnomad/dbmq/logger"
	"github.com/donutnomad/gt"
)

type coordinatorTask struct {
	cfg              *CoordinatorConfig
	rebalancingLocks *groupLocks // 分消费组的重新均衡锁
	snapshotInfo     *snapshotInfo
	repos
}

func newCoordinatorTask(cfg *CoordinatorConfig, repos repos) *coordinatorTask {
	return &coordinatorTask{
		cfg:              cfg,
		rebalancingLocks: newGroupLocks(),
		snapshotInfo:     newSnapshotInfo(),
		repos:            repos,
	}
}

func (t *coordinatorTask) Name() string { return "coordinator" }

func (t *coordinatorTask) logger() *slog.Logger {
	return logger.GetLogger().With("component", "coordinator")
}

func (t *coordinatorTask) Start(ctx context.Context) error {
	log := t.logger()
	log.Debug("[LEADER] Coordinator leader loop started.")

	return gt.TickRun(ctx, gt.CRON, t.cfg.RebalanceInterval, 0, 0, func(ctx context.Context) *gt.TickOptions {
		log.Debug("[LEADER] Starting global rebalance scan...")
		t.tryRebalanceGroups(ctx)
		return nil
	})
}

// tryRebalanceGroups 扫描并触发所有消费组的重新均衡
func (t *coordinatorTask) tryRebalanceGroups(parentCtx context.Context) {
	log := t.logger()

	ctx, cancel := context.WithTimeout(parentCtx, 1*time.Minute)
	defer cancel()

	// 找到活跃的消费组IDs
	activeGroupIds, err := t.groupRepo.FindAllActiveGroups(ctx, t.cfg.HeartbeatTimeout)
	if err != nil {
		log.Error("[LEADER] Failed to scan for active groups", "error", err)
		return
	}
	if len(activeGroupIds) == 0 {
		return
	}
	log.Debug(fmt.Sprintf("[LEADER] [scanAndRebalanceAllGroups] Found active consumer groups: %v", activeGroupIds))

	for _, groupID := range activeGroupIds {
		if err := t.tryRebalance(ctx, groupID); err != nil {
			log.Error("[LEADER] Rebalance failed for group", "group", groupID, "error", err)
		}
	}
}

// tryRebalance 负责在消费者成员变化时重新分配分区，确保每个分区只被一个活跃消费者处理，实现高可用和负载均衡。
func (t *coordinatorTask) tryRebalance(parentCtx context.Context, groupID string) error {
	log := t.logger()

	// ========== 1. 按消费组加锁 ==========
	if !t.rebalancingLocks.TryLock(groupID) {
		log.Debug("[LEADER] Rebalance check for group skipped", "group", groupID, "reason", "another rebalance is already in progress")
		return nil
	}
	defer t.rebalancingLocks.Unlock(groupID)

	ctx, cancel := context.WithTimeout(parentCtx, t.cfg.RebalanceTimeout)
	defer cancel()

	// ========== 2. 活跃消费者 ==========
	// 获取消费组中所有活跃的消费者
	activeConsumers, err := t.heartbeatRepo.FindActive(ctx, groupID, t.cfg.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to find active consumers: %w", err)
	}
	// 获取消费者的分区数量
	partitionInfos, partitionHash, err := getAllPartitionsForConsumers(ctx, t.repos.topicRepo, activeConsumers)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get partitions for consumers: %w", err)
	}
	// 获取当前 generation_id，用于检测外部触发的重新均衡（如 TriggerRebalance API）
	generation, err := t.groupRepo.GetGeneration(ctx, groupID)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to get generation: %w", err)
	}

	var currentGenerationID uint = 0
	if generation != nil {
		currentGenerationID = generation.GenerationID
	}

	snapshot := t.snapshotInfo.get(groupID)
	if !canRebalance(ctx, snapshot, t.repos.manualAssignmentRepo, groupID, activeConsumers, partitionHash, currentGenerationID) {
		return nil // 没有变化，无需重新均衡
	}

	log.Info(fmt.Sprintf("[LEADER] Rebalance needed for group '%s'. Old members: %v, New members: %v",
		groupID, t.snapshotInfo.getMemberIDs(groupID), getHeartbeatConsumerIDs(activeConsumers)))

	// ========== 4. 重新均衡 - 代际隔离 ==========
	var manualAssignments, newAssignments = map[string][]types.PartitionInfo{}, map[string][]types.PartitionInfo{}

	if len(activeConsumers) > 0 {
		// 查询手动分配配置
		manualAssignments, err = t.manualAssignmentRepo.GetMatching(ctx, groupID, getHeartbeatConsumerIDs(activeConsumers))
		if err != nil {
			return fmt.Errorf("[LEADER] failed to get manual assignments: %w", err)
		}
		// 分配分区
		newAssignments = calculateAssignments(activeConsumers, partitionInfos, manualAssignments)
	}

	// 原子地递增代际 ID 并持久化分区分配，消除两阶段提交竞态窗口
	newGenerationID, err := t.groupRepo.IncrementAndUpdateAssignments(ctx, groupID, newAssignments)
	if err != nil {
		return fmt.Errorf("[LEADER] failed to increment generation and update assignments: %w", err)
	}

	// 记录新的消费组快照
	t.snapshotInfo.updateGroupSnapshot(groupID, newGenerationID, activeConsumers, partitionHash, manualAssignments)

	if len(activeConsumers) > 0 {
		log.Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' to generation %d completed successfully.", groupID, newGenerationID))
	} else {
		log.Info(fmt.Sprintf("[LEADER] Rebalance for group '%s' completed with no active consumers. Generation: %d", groupID, newGenerationID))
	}

	return nil
}

type snapshotInfo struct {
	mu             sync.Mutex
	groupSnapshots map[string]*groupSnapshot // 缓存消费组成员和订阅/分区快照
}

func newSnapshotInfo() *snapshotInfo {
	return &snapshotInfo{
		mu:             sync.Mutex{},
		groupSnapshots: make(map[string]*groupSnapshot),
	}
}

func (c *snapshotInfo) get(groupID string) *groupSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groupSnapshots[groupID]
}

// getMemberIDs 获取指定消费组当前缓存的成员ID列表。
func (c *snapshotInfo) getMemberIDs(groupID string) []string {
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

func (c *snapshotInfo) updateGroupSnapshot(groupID string, generationID uint, consumers []*heartbeat.Heartbeat, partitionHash string, manualAssignments map[string][]types.PartitionInfo) {
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
