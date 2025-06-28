package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
	"time"
)

// autoCommitPolledOffsets 自动提交已拉取但未提交的偏移量
// 这是一个内部方法，只在自动提交模式下使用
func (c *Consumer) autoCommitPolledOffsets() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否有需要提交的已拉取消息ID
	if len(c.lastPolledMessageIDs) == 0 {
		return nil // 没有新的消息ID需要提交
	}

	// 创建需要提交的消息ID副本
	messageIDsToCommit := CloneMap(c.lastPolledMessageIDs)

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, c.config.GroupID, c.generationID, messageIDsToCommit)
	if err != nil {
		return fmt.Errorf("failed to auto-commit polled message IDs: %w", err)
	}

	// 提交成功后，更新内存状态
	for partition, messageID := range messageIDsToCommit {
		c.alreadyConsumeMessageIDs[partition] = messageID
		delete(c.lastPolledMessageIDs, partition)
	}

	c.logger().Debug(fmt.Sprintf("Consumer %s: 自动提交成功，提交了 %d 个分区的消息ID", c.id, len(messageIDsToCommit)))
	return nil
}

// autoCommitLoop 自动提交偏移量的后台进程
// 定期提交已拉取的消息偏移量，无需手动调用CommitSync
func (c *Consumer) autoCommitLoop() {
	defer c.wg.Done()

	c.logger().Debug(fmt.Sprintf("Consumer %s: 启动自动提交循环，间隔: %v", c.id, c.config.AutoCommitInterval))

	ticker := time.NewTicker(c.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 执行自动提交，只提交有新拉取偏移量的分区
			if err := c.autoCommitPolledOffsets(); err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: Consumer %s 自动提交失败: %v", c.id, err))
			} else {
				c.lastAutoCommit.Store(time.Now().Unix())
			}
		case <-c.stopCh:
			// 在停止前执行最后一次提交
			c.logger().Debug(fmt.Sprintf("Consumer %s: 自动提交循环收到停止信号，执行最后一次提交", c.id))
			if err := c.autoCommitPolledOffsets(); err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: Consumer %s 最终自动提交失败: %v", c.id, err))
			}
			return
		}
	}
}

// CommitSync 同步提交所有当前分配分区的消费进度
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息ID
func (c *Consumer) CommitSync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 获取当前分配的分区
	partitions := c.getAssignedPartitionsLocked()
	if len(partitions) == 0 {
		return nil // 没有分配的分区，无需提交
	}

	// 构建需要提交的消息ID映射
	// 对于每个分区，提交已拉取的最大消息ID（如果存在）
	messageIDsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// 只提交有新拉取消息ID的分区
		if polledMessageID, exists := c.lastPolledMessageIDs[p]; exists {
			messageIDsToCommit[p] = polledMessageID
		}
	}

	if len(messageIDsToCommit) == 0 {
		return nil // 没有需要提交的消息ID
	}

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, c.config.GroupID, c.generationID, messageIDsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit consumption progress: %w", err)
	}

	// 提交成功后，更新内存状态
	for partition, messageID := range messageIDsToCommit {
		c.alreadyConsumeMessageIDs[partition] = messageID
		delete(c.lastPolledMessageIDs, partition)
	}

	return nil
}

// CommitMessage 提交单个消息的偏移量
// 这是CommitOffsets的便利方法，用于精确的手动提交
func (c *Consumer) CommitMessage(msg ConsumerMessage) error {
	// 添加调试日志
	c.logger().Debug(fmt.Sprintf("🔍 [CommitMessage] Topic: %s, Partition: %d, ID: %d",
		msg.Topic, msg.Partition, msg.Offset))

	return c.commitIDToPartition(map[types.PartitionInfo]int64{
		types.PartitionInfo{
			Topic:     msg.Topic,
			Partition: msg.Partition,
		}: msg.Offset,
	})
}

// commitIDToPartition 提交特定消息的ID，表示该ID已经被消费
func (c *Consumer) commitIDToPartition(offsets map[types.PartitionInfo]int64) error {
	if len(offsets) == 0 {
		return nil
	}

	c.mu.Lock()
	generationID := c.generationID
	c.mu.Unlock()

	// 执行数据库提交
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, c.config.GroupID, generationID, offsets)
	if err != nil {
		return fmt.Errorf("failed to commit specified offsets: %w", err)
	}

	// 只有在数据库提交成功后才更新内存状态
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新已提交的消息ID
	for partition, messageID := range offsets {
		// alreadyConsumeMessageIDs 存储最新的已消费的消息ID（已提交消息ID）
		c.alreadyConsumeMessageIDs[partition] = messageID
		// 如果指定的消息ID已经在已拉取消息ID中，且小于等于指定消息ID，则从中移除
		if polledMessageID, exists := c.lastPolledMessageIDs[partition]; exists && polledMessageID <= messageID {
			delete(c.lastPolledMessageIDs, partition)
		}
	}

	return nil
}

// commitOffsets 处理重新均衡时撤销分区的偏移量提交
// 这是一个内部方法，用于重新均衡过程中的偏移量提交
func (c *Consumer) commitOffsets(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	if len(partitions) == 0 {
		return nil
	}

	c.mu.RLock()
	messageIDsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// 优先提交已拉取的消息ID，如果没有则提交下一个要消费的消息ID-1（转换为最后消费的消息ID）
		if polledMessageID, exists := c.lastPolledMessageIDs[p]; exists {
			messageIDsToCommit[p] = polledMessageID
		} else if messageID, exists := c.alreadyConsumeMessageIDs[p]; exists {
			// alreadyConsumeMessageIDs存储的是最后已经消费的消息ID
			messageIDsToCommit[p] = messageID
		}
	}
	c.mu.RUnlock()

	if len(messageIDsToCommit) == 0 {
		return nil
	}

	err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, c.config.GroupID, generationID, messageIDsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit message IDs for revoked partitions: %w", err)
	}

	// 成功提交后更新内存状态
	c.mu.Lock()
	for partition, messageID := range messageIDsToCommit {
		// alreadyConsumeMessageIDs 存储最新的已经消费的消息ID（已提交消息ID）
		c.alreadyConsumeMessageIDs[partition] = messageID
		delete(c.lastPolledMessageIDs, partition)
	}
	c.mu.Unlock()

	return nil
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
