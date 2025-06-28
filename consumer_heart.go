package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
	"github.com/samber/lo"
	"slices"
	"time"
)

// heartbeatLoop 消费者的核心后台进程，负责：
// 1. 定期向协调器发送心跳
// 2. 获取最新的分区分配和代际ID
// 3. 检测到变化时触发并执行重新均衡协议
// 这将状态管理的网络I/O与主Poll()循环解耦
func (c *Consumer) heartbeatLoop() {
	defer c.wg.Done()

	c.reconcileState(context.Background())

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.reconcileState(context.Background())
		case <-c.stopCh:
			return
		}
	}
}

// reconcileState 执行单次发送心跳、获取消费者当前状态和处理重新均衡（如有必要）的循环
func (c *Consumer) reconcileState(ctx context.Context) {
	// 首先注册/更新心跳
	if err := c.dao.UpsertConsumerHeartbeat(ctx, c.config.GroupID, c.id, c.getTopics()); err != nil {
		c.logger().Debug(fmt.Sprintf("ERROR: failed to send heartbeat for consumer %s: %v", c.id, err))
		return // 如果连心跳都无法发送，就不继续处理
	}

	// 从数据库获取我们自己的状态
	hb, err := c.dao.GetConsumerHeartbeat(ctx, c.config.GroupID, c.id)
	if err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to fetch consumer state for %s: %v", c.id, err))
		return
	}
	if hb == nil {
		// 这在罕见的竞态条件下可能发生，我们在心跳和获取之间被踢出
		// 下一次心跳会重新注册
		c.logger().Warn(fmt.Sprintf("WARN: could not find our own heartbeat for consumer %s, will retry.", c.id))
		return
	}
	currentGenID := c.GetGenerationID()
	// 检查是否需要重新均衡
	if hb.GenerationID == currentGenID {
		return // 没有变化，无需操作
	}

	c.logger().Debug(fmt.Sprintf("Consumer %s: Generation ID changed from %d to %d, starting rebalance", c.id, currentGenID, hb.GenerationID))

	// ---- 需要重新均衡 ----
	c.logger().Debug(fmt.Sprintf("Rebalance detected for consumer %s. New Generation ID: %d", c.id, hb.GenerationID))

	c.rebalancing.Store(true)
	defer c.rebalancing.Store(false)

	newPartitions := slices.Clone(hb.AssignedPartitions)
	c.logger().Debug(fmt.Sprintf("Consumer %s: parsed new partitions: %v", c.id, newPartitions))

	// 3. 清除内部状态并为新分配获取偏移量
	if err := c.clearAndFetchOffsetsForNewAssignment(ctx, newPartitions); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to fetch offsets for new assignment on consumer %s: %v", c.id, err))
		return // 这对重新均衡来说是致命错误
	}
	c.logger().Debug(fmt.Sprintf("Consumer %s: successfully fetched offsets for new assignment", c.id))

	// 这可以并发进行，但为了简单起见，我们内联执行
	// 取消旧的Redis，订阅新的
	c.unsubscribeFromChannels(c.getAssignedPartitions())
	c.subscribeToChannels(newPartitions)

	// 5. 原子性地更新消费者状态
	c.mu.Lock()
	c.generationID = hb.GenerationID
	c.assignment = partitionToMap(newPartitions)
	c.mu.Unlock()

	c.logger().Info(fmt.Sprintf("Rebalance completed for consumer %s. New assignment: %v", c.id, newPartitions))
}

// clearAndFetchOffsetsForNewAssignment
// 清除旧状态并获取新分配的已提交偏移量。
// 必须在消费者设置新分配后调用。
func (c *Consumer) clearAndFetchOffsetsForNewAssignment(ctx context.Context, newPartitions []types.PartitionInfo) error {
	// 1. 找出被撤销的分区
	revokedPartitions := c.findRevokedPartitions(newPartitions)

	// 提交被撤销分区的
	if len(revokedPartitions) > 0 {
		if c.config.EnableAutoCommit {
			c.logger().Debug(fmt.Sprintf("Consumer %s: auto-commit mode, committing offsets for revoked partitions: %v", c.id, revokedPartitions))
			// 为撤销的分区准备消息ID提交
			err := c.commitSync(ctx, c.getGenerationIDLocked(), revokedPartitions)
			if err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: failed to commit revoked partitions: %v", c.id, err))
				// 继续执行，但记录错误
			} else {
				c.logger().Debug(fmt.Sprintf("Consumer %s: successfully committed revoked partitions", c.id))
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
	{
		c.mu.Lock()
		for _, p := range revokedPartitions {
			delete(c.lastPolledMessageIDs, p)
			delete(c.alreadyConsumeMessageIDs, p)
		}
		c.mu.Unlock()
	}

	// 构建新分配的分区列表
	// 如果没有新分区需要获取，直接返回
	if len(newPartitions) == 0 {
		return nil
	}

	////////////////////// 为新增的分区应用策略 //////////////////////
	c.logger().Debug(fmt.Sprintf("Consumer %s: fetching offsets for partitions: %v", c.id, newPartitions))
	fetchedOffsets_, err := c.dao.GetCommittedOffsets(ctx, c.config.GroupID, newPartitions)
	if err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: dal.GetCommittedOffsets failed: %v", c.id, err))
		return fmt.Errorf("dal.GetCommittedOffsets failed: %w", err)
	}
	fetchedOffsets := fetchedOffsets_.ToMap()
	// 筛选出新增的分区
	addedPartitions := lo.Filter(newPartitions, func(p types.PartitionInfo, index int) bool {
		_, exists := fetchedOffsets[p]
		return !exists
	})
	initialProgressWithWatermarks := make(map[types.PartitionInfo]dal.ConsumptionProgressWithWatermark)
	// 为没有已提交偏移量的分区应用消费策略并立即记录到数据库
	for _, partition := range addedPartitions {
		startID := c.determineStartMessageID(ctx, partition)

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

	return nil
}

// findRevokedPartitions 计算出在旧分配方案中存在而在新分配方案中不存在的分区
func (c *Consumer) findRevokedPartitions(newPartitions []types.PartitionInfo) []types.PartitionInfo {
	oldSet := lo.SliceToMap(c.getAssignedPartitions(), func(item types.PartitionInfo) (types.PartitionInfo, struct{}) {
		return item, struct{}{}
	})
	newSet := lo.SliceToMap(newPartitions, func(item types.PartitionInfo) (types.PartitionInfo, struct{}) {
		return item, struct{}{}
	})
	var revoked []types.PartitionInfo
	for p := range oldSet {
		if _, ok := newSet[p]; !ok {
			revoked = append(revoked, p)
		}
	}
	return revoked
}
