package dbmq

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
)

// CommitSync 同步提交所有当前分配分区的消费进度
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息ID
func (c *Consumer) CommitSync() error {
	return c.commitSync(nil, c.GetGenerationID(), c.getAssignedPartitions())
}

// CommitMessage 提交单个消息ACK
func (c *Consumer) CommitMessage(msg ConsumerMessage) error {
	// 添加调试日志
	c.logger().Debug(fmt.Sprintf("🔍 [CommitMessage] Topic: %s, Partition: %d, ID: %d",
		msg.Topic, msg.Partition, msg.ID))

	messageIDsToCommit := map[types.PartitionInfo]int64{
		msg.PartitionInfo(): msg.ID,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitMessageIDsLocked(nil, c.config.GroupID, c.getGenerationIDLocked(), messageIDsToCommit)
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

func (c *Consumer) commitSync(parent context.Context, generationID uint, partitions []types.PartitionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	messageIDsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		if polledMessageID, exists := c.lastPolledMessageIDs[p]; exists {
			messageIDsToCommit[p] = polledMessageID
		} else if messageID, exists := c.alreadyConsumeMessageIDs[p]; exists {
			messageIDsToCommit[p] = messageID
		}
	}

	return c.commitMessageIDsLocked(parent, c.config.GroupID, generationID, messageIDsToCommit)
}

// commitMessageIDsLocked 提交消息ID的核心逻辑
func (c *Consumer) commitMessageIDsLocked(parent context.Context, groupID string, generationID uint, messageIDsToCommit map[types.PartitionInfo]int64) error {
	if len(messageIDsToCommit) == 0 {
		return nil
	}
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	err := dal.BatchCommitLastConsumeMessageID(ctx, c.db, groupID, generationID, messageIDsToCommit)
	if err != nil {
		return fmt.Errorf("failed to commit message IDs: %w", err)
	}

	// 提交成功后，更新内存状态
	for partition, messageID := range messageIDsToCommit {
		c.alreadyConsumeMessageIDs[partition] = messageID
		delete(c.lastPolledMessageIDs, partition)
	}

	return nil
}
