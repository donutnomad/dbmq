package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/types"
	"github.com/samber/lo"
	"time"
)

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

	c.mu.RLock()
	currentGenID := c.generationID
	c.mu.RUnlock()

	// 检查是否需要重新均衡
	if hb.GenerationID == currentGenID {
		return // 没有变化，无需操作
	}

	c.logger().Debug(fmt.Sprintf("Consumer %s: Generation ID changed from %d to %d, starting rebalance", c.id, currentGenID, hb.GenerationID))

	// ---- 需要重新均衡 ----
	c.logger().Debug(fmt.Sprintf("Rebalance detected for consumer %s. New Generation ID: %d", c.id, hb.GenerationID))
	c.rebalancing.Store(true)
	defer c.rebalancing.Store(false)

	newPartitions := lo.GroupByMap(hb.AssignedPartitions, func(item types.PartitionInfo) (string, uint) {
		return item.Topic, item.Partition
	})
	c.logger().Debug(fmt.Sprintf("Consumer %s: parsed new partitions: %v", c.id, newPartitions))

	// 1. 找出被撤销的分区
	revokedPartitions := c.findRevokedPartitions(newPartitions)

	// 2. 为被撤销的分区提交偏移量，确保工作不会丢失（仅在自动提交模式下）
	// 使用新的代际ID进行提交，防止过时的提交
	if len(revokedPartitions) > 0 {
		if c.config.EnableAutoCommit {
			c.logger().Debug(fmt.Sprintf("Consumer %s auto-commit mode, committing offsets for revoked partitions: %v", c.id, revokedPartitions))
			if err := c.commitOffsets(ctx, revokedPartitions, hb.GenerationID); err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: failed to commit offsets for revoked partitions on consumer %s: %v", c.id, err))
				// 即使提交失败也继续重新均衡
			}
		} else {
			// 手动提交模式下，记录警告但不自动提交
			c.mu.RLock()
			var uncommittedPartitions []types.PartitionInfo
			for _, p := range revokedPartitions {
				if _, exists := c.lastPolledMessageIDs[p]; exists {
					uncommittedPartitions = append(uncommittedPartitions, p)
				}
			}
			c.mu.RUnlock()

			if len(uncommittedPartitions) > 0 {
				c.logger().Warn(fmt.Sprintf("WARNING: Consumer %s: 手动提交模式下，重新均衡导致分区 %v 被撤销，但这些分区有未提交的消息。这些消息将被重新消费。", c.id, uncommittedPartitions))
			}
			c.logger().Debug(fmt.Sprintf("Consumer %s manual commit mode, not auto-committing revoked partitions: %v", c.id, revokedPartitions))
		}
	}

	// 3. 清除内部状态并为新分配获取偏移量
	c.logger().Debug(fmt.Sprintf("Consumer %s: clearing and fetching offsets for new assignment", c.id))
	if err := c.clearAndFetchOffsetsForNewAssignment(newPartitions); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to fetch offsets for new assignment on consumer %s: %v", c.id, err))
		return // 这对重新均衡来说是致命错误
	}
	c.logger().Debug(fmt.Sprintf("Consumer %s: successfully fetched offsets for new assignment", c.id))

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

	c.logger().Info(fmt.Sprintf("Rebalance completed for consumer %s. New assignment: %v", c.id, newPartitions))
}

// findRevokedPartitions 计算出在旧分配方案中存在而在新分配方案中不存在的分区
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
