package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/samber/lo"
	"time"
)

// Acknowledge 确认一批消息已经成功处理
// 这会将这批消息中最大的偏移量标记为准备提交
// 在自动提交模式下，后台循环会提交这些偏移量
// 在手动提交模式下，您仍需调用 CommitSync 来实际提交
func (c *Consumer) Acknowledge(messages ...ConsumerMessage) {
	if len(messages) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 按分区对消息进行分组
	groupedMessages := lo.GroupBy(messages, func(msg ConsumerMessage) db.PartitionInfo {
		return msg.PartitionInfo()
	})

	// 为每个分区找到最大的消息ID并标记为待提交
	for partition, msgs := range groupedMessages {
		if len(msgs) == 0 {
			continue
		}
		// 找到这批消息中的最大ID
		maxID := lo.MaxBy(msgs, func(a, b ConsumerMessage) bool {
			return a.ID > b.ID
		}).ID
		c.offsetsToCommit[partition] = max(maxID, c.offsetsToCommit[partition])
	}
}

// CommitSync 同步提交所有当前分配分区的消费进度
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息ID
func (c *Consumer) CommitSync() error {
	partitions := c.getAssignedPartitions()
	generationID := c.GetGenerationID()

	c.mu.RLock()
	// 我们只提交那些已经被Acknowledge的偏移量
	messageIDsToCommit := make(map[db.PartitionInfo]int64)
	for _, p := range partitions {
		if ackedOffset, exists := c.offsetsToCommit[p]; exists {
			messageIDsToCommit[p] = ackedOffset
		}
	}
	c.mu.RUnlock()

	return c.commitMessageIDs(context.Background(), c.config.GroupID, generationID, messageIDsToCommit)
}

// CommitMessage 提交单个消息ACK
func (c *Consumer) CommitMessage(msg ConsumerMessage) error {
	// 添加调试日志
	c.logger().Debug(fmt.Sprintf("🔍 [CommitMessage] Topic: %s, Partition: %d, ID: %d",
		msg.Topic, msg.Partition, msg.ID))

	messageIDsToCommit := map[db.PartitionInfo]int64{
		msg.PartitionInfo(): msg.ID,
	}

	return c.commitMessageIDs(context.Background(), c.config.GroupID, c.GetGenerationID(), messageIDsToCommit)
}

// commitMessageIDs 提交消息ID的核心逻辑
func (c *Consumer) commitMessageIDs(parent context.Context, groupID string, generationID uint, messageIDsToCommit map[db.PartitionInfo]int64) error {
	if len(messageIDsToCommit) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	err := c.repo.BatchCommitLastConsumeMessageID(ctx, groupID, generationID, messageIDsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit message IDs: %w", err)
	}

	// 提交成功后，更新内存状态
	c.mu.Lock()
	for partition, messageID := range messageIDsToCommit {
		c.alreadyConsumeMessageIDs[partition] = max(messageID, c.alreadyConsumeMessageIDs[partition])
		delete(c.offsetsToCommit, partition)
	}
	c.mu.Unlock()

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
			if err := c.CommitSync(); err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: Consumer %s 自动提交失败: %v", c.id, err))
			} else {
				c.lastAutoCommit.Store(time.Now().Unix())
			}
		case <-c.stopCh:
			// 在停止前执行最后一次提交
			c.logger().Debug(fmt.Sprintf("Consumer %s: 自动提交循环收到停止信号，执行最后一次提交", c.id))
			if err := c.CommitSync(); err != nil {
				c.logger().Error(fmt.Sprintf("ERROR: Consumer %s 最终自动提交失败: %v", c.id, err))
			}
			return
		}
	}
}
