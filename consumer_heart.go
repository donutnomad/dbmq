package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/dao"
	"github.com/donutnomad/dbmq/internal/db"
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

	ticker := time.NewTicker(c.config.GetHeartbeatInterval())
	defer ticker.Stop()

	for {
		c.reconcileState(context.Background())
		select {
		case <-ticker.C:
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
		// 这在罕见的竞态条件下可能发生，我们在心跳和获取之间被踢出/ 下一次心跳会重新注册
		c.logger().Warn(fmt.Sprintf("WARN: could not find our own heartbeat for consumer %s, will retry.", c.id))
		return
	}
	currentGenID := c.GetGenerationID()
	// 检查是否需要重新均衡
	if hb.GenerationID == currentGenID {
		return // 没有变化，无需操作
	}

	c.logger().Debug(fmt.Sprintf("Consumer %s: Generation ID changed from %d to %d, starting rebalance", c.id, currentGenID, hb.GenerationID))
	c.logger().Debug(fmt.Sprintf("Rebalance detected for consumer %s. New Generation ID: %d", c.id, hb.GenerationID))

	c.rebalancing.Store(true)
	defer c.rebalancing.Store(false)

	oldPartitions := c.getAssignedPartitions()
	newPartitions := slices.Clone(hb.AssignedPartitions)
	c.logger().Debug(fmt.Sprintf("Consumer %s: parsed new partitions: %v", c.id, newPartitions))

	if err := c.clearAndFetchOffsetsForNewAssignment(ctx, hb.GenerationID, newPartitions); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to fetch offsets for new assignment on consumer %s: %v", c.id, err))
		return // 这对重新均衡来说是致命错误
	}

	// 这可以并发进行，但为了简单起见，我们内联执行.  取消旧的Redis，订阅新的
	c.unsubscribeFromChannels(oldPartitions)
	c.subscribeToChannels(newPartitions)
}

// clearAndFetchOffsetsForNewAssignment
// 清除旧状态并获取新分配的已提交偏移量。
// 必须在消费者设置新分配后调用。
func (c *Consumer) clearAndFetchOffsetsForNewAssignment(ctx context.Context, newGenerationID uint, newPartitions []db.PartitionInfo) error {
	// 找出被撤销的分区
	revokedPartitions := c.findRevokedPartitions(newPartitions)

	c.logger().Debug(fmt.Sprintf("Consumer %s: fetching offsets for partitions: %v", c.id, newPartitions))
	fetchedOffsets, err := c.dao.GetCommittedOffsets(ctx, c.config.GroupID, newPartitions)
	if err != nil {
		return fmt.Errorf("dao.GetCommittedOffsets failed: %w", err)
	}
	fetchedOffsetsMap := fetchedOffsets.ToMap()

	// 筛选出新增的分区
	addedPartitions := lo.Filter(newPartitions, func(p db.PartitionInfo, index int) bool {
		_, exists := fetchedOffsetsMap[p]
		return !exists
	})

	// 为没有已提交偏移量的分区应用消费策略并立即记录到数据库
	partitionMaxIDMap := c.determineStartMessageID(ctx, addedPartitions)
	initialProgressWithWatermarks := lo.MapValues(partitionMaxIDMap, func(startID int64, key db.PartitionInfo) dao.ConsumptionProgressWithWatermark {
		return dao.ConsumptionProgressWithWatermark{LastConsumedMessageID: startID - 1, SubscriptionStartWatermark: startID}
	})

	////////////////////// 为新增的分区应用策略-START //////////////////////
	if err := c.dao.BatchCommitOffsetsWithInitialWatermark(ctx, c.config.GroupID, c.generationID, initialProgressWithWatermarks); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: Consumer %s: Failed to register subscription information: %v", c.id, err))
		// 继续执行，但记录错误。这不是致命错误，因为重新注册时会重新应用策略
	} else {
		c.logger().Debug(fmt.Sprintf("Consumer %s: Subscription information registered successfully", c.id))
	}
	////////////////////// 为新增的分区应用策略-END //////////////////////

	// 只清空被撤销的分区的消息ID记录，保留继续分配的分区
	{
		c.mu.Lock()
		for _, p := range revokedPartitions {
			delete(c.offsetsToCommit, p)
			delete(c.alreadyConsumeMessageIDs, p)
		}

		for k, v := range initialProgressWithWatermarks {
			c.alreadyConsumeMessageIDs[k] = v.LastConsumedMessageID
		}

		for _, item := range fetchedOffsets {
			c.alreadyConsumeMessageIDs[db.PartitionInfo{
				Topic:     item.Topic,
				Partition: item.Partition,
			}] = item.LastConsumedMessageID
		}

		c.generationID = newGenerationID
		c.assignment = partitionToMap(newPartitions)
		c.mu.Unlock()
	}

	return nil
}

// findRevokedPartitions 计算出在旧分配方案中存在而在新分配方案中不存在的分区
func (c *Consumer) findRevokedPartitions(newPartitions []db.PartitionInfo) []db.PartitionInfo {
	oldSet := lo.SliceToMap(c.getAssignedPartitions(), func(item db.PartitionInfo) (db.PartitionInfo, struct{}) {
		return item, struct{}{}
	})
	newSet := lo.SliceToMap(newPartitions, func(item db.PartitionInfo) (db.PartitionInfo, struct{}) {
		return item, struct{}{}
	})
	var revoked []db.PartitionInfo
	for p := range oldSet {
		if _, ok := newSet[p]; !ok {
			revoked = append(revoked, p)
		}
	}
	return revoked
}

var firstMessageId = int64(1) // 数据库的消息ID主键是从1开始的

// determineStartMessageID 首次，根据消费策略，确定从哪个ID开始消费，比如2，那么第一个将会消费2这个ID
func (c *Consumer) determineStartMessageID(ctx context.Context, partitions []db.PartitionInfo) map[db.PartitionInfo]int64 {
	var ret = make(map[db.PartitionInfo]int64)

	var needFetchFromDB []db.PartitionInfo
	for _, partition := range partitions {
		ret[partition] = firstMessageId
		switch c.config.ConsumeStrategy {
		case ConsumeFromEarliest:
			continue
		case ConsumeFromLatest:
			needFetchFromDB = append(needFetchFromDB, partition)
		default:
			// unreachable
			panic(fmt.Errorf("unknown consume strategy: %v", c.config.ConsumeStrategy))
		}
	}

	byPartitions, err := c.dao.GetTopicsLatestIDsByPartitions(ctx, needFetchFromDB)
	if err != nil {
		return ret
	}
	for k, v := range byPartitions {
		ret[k] = v
	}

	return ret
}
