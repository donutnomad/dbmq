package dbmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
	"github.com/samber/lo"
	"time"
)

type ErrFailedFetchMessage struct {
	err error
}

func (e *ErrFailedFetchMessage) Error() string {
	return e.err.Error()
}

// Poll 从订阅的Topic和分区中拉取消息
// 这是消费者逻辑的核心，实现了复杂的拉取和通知机制
// 会返回的错误:
// ErrFailedFetchMessage
// ErrRebalanceInProgress
// context.DeadlineExceeded
// context.Canceled
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	// 如果正在进行重新均衡，立即返回并提示用户
	// 心跳循环负责处理重新均衡过程
	if c.rebalancing.Load() {
		return nil, &ErrRebalanceInProgress{GroupID: c.config.GroupID}
	}

	// 获取当前分配的分区
	assignedPartitions := c.getAssignedPartitions()

	// 等待通知或超时
	if err := c.waitPoll(ctx, !isEmpty(assignedPartitions), timeout); err != nil {
		return nil, err
	}
	if isEmpty(assignedPartitions) {
		return nil, nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, c.config.GetPollFetchTimeout())
	defer cancel()

	// 批量获取消息, 获取id > ?的记录
	allMessages, err := c.dao.FetchMessagesBatch(fetchCtx, lo.Map(assignedPartitions, func(p types.PartitionInfo, _ int) dal.PartitionRequest {
		return dal.PartitionRequest{
			Topic:     p.Topic,
			Partition: p.Partition,
			ID:        c.getAlreadyConsumeMessageIDByPartition(p),
			Limit:     c.config.GetPollFetchLimit(),
		}
	}))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		// 如果批量查询失败，记录错误并返回
		c.logger().Error(fmt.Sprintf("ERROR: failed to batch fetch messages for consumer %s: %v", c.id, err))
		return nil, &ErrFailedFetchMessage{err}
	}

	// 按照(Topic+分区) 分组
	// 在数据库中消息都是按照Id递增的，所以分组后，每组的Message顺序是确定的
	for partition, messages := range lo.GroupBy(allMessages, func(msg types.Message) types.PartitionInfo {
		return types.PartitionInfo{Topic: msg.Topic, Partition: msg.Partition}
	}) {
		// 更新该分区的已拉取偏移量
		lastMessage, ok := lo.Last(messages)
		if !ok {
			continue
		}
		// 设置已经拉取的最后一个消息的ID
		c.setAlreadyPolledMessageID(partition, lastMessage.ID)

		// 添加调试日志
		c.logger().Debug(fmt.Sprintf("🔍 [Poll] 更新分区 %v 的polledOffset为 %d (消息ID: %d)",
			partition, lastMessage.ID, lastMessage.ID))

		// "重新装填"该分区的通知触发器
		c.tryResetNotificationState(context.Background(), partition)
	}
	return new(ConsumerMessages).FromMessages(allMessages), nil
}

// setAlreadyPolledMessageID 设置分区的已拉取消息ID
// 这个方法确保消息ID是单调递增的，避免回退
// 参数messageID是从数据库拉取到的最新消息ID
func (c *Consumer) setAlreadyPolledMessageID(p types.PartitionInfo, messageID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.logger().Debug(fmt.Sprintf("🔍 [setAlreadyPolledMessageID] 分区 %v 设置已拉取消息ID为 %d", p, messageID))

	// 检查新消息ID是否小于已提交的消息ID
	if alreadyMessageID, exists := c.alreadyConsumeMessageIDs[p]; exists {
		// alreadyConsumeMessageIDs存储的是最新的已消费的消息ID
		// 所以已拉取的消息ID不应该小于alreadyMessageID
		if messageID < alreadyMessageID {
			c.logger().Warn(fmt.Sprintf("WARNING: Consumer %s: 尝试设置已拉取消息ID %d，但小于下一个要消费的消息ID %d，分区 %v",
				c.id, messageID, alreadyMessageID, p))
			return // 拒绝设置无效的消息ID
		}
	}

	// 确保消息ID是单调递增的
	if currentMessageID, exists := c.lastPolledMessageIDs[p]; exists {
		if messageID > currentMessageID {
			c.lastPolledMessageIDs[p] = messageID
			c.logger().Debug(fmt.Sprintf("✅ [setAlreadyPolledMessageID] 分区 %v 已拉取消息ID更新为 %d (之前: %d)", p, messageID, currentMessageID))
		} else {
			c.logger().Debug(fmt.Sprintf("⚠️ [setAlreadyPolledMessageID] 分区 %v 忽略非递增消息ID %d (当前: %d)", p, messageID, currentMessageID))
		}
	} else {
		// 第一次设置此分区的消息ID
		c.lastPolledMessageIDs[p] = messageID
		c.logger().Debug(fmt.Sprintf("🆕 [setAlreadyPolledMessageID] 分区 %v 首次设置已拉取消息ID为 %d", p, messageID))
	}
}

// 返回错误
// context.DeadlineExceeded
// context.Canceled
func (c *Consumer) waitPoll(ctx context.Context, useRedis bool, timeout time.Duration) error {
	// 如果启用了通知优化，使用Redis Pub/Sub等待通知
	if useRedis && c.config.NotificationEnabled && c.redis != nil {
		// 检查并确保PubSub连接健康
		c.ensurePubSubConnection()

		c.muSub.Lock()
		if c.pubsub != nil && c.pubsubHealthy.Load() {
			// 先排空任何在我们开始等待之前到达的消息
			select {
			case <-c.notifyCh:
				// 有消息在等待，我们将立即进行拉取
				c.logger().Debug(fmt.Sprintf("Drained pending notification for consumer %s", c.id))
				c.lastPubsubTime.Store(time.Now().Unix())
			default:
				// 没有消息在等待，使用非阻塞方式等待通知
				waitTimeout := timeout
				if waitTimeout > 3*time.Second {
					waitTimeout = 3 * time.Second // 最多等待3秒，避免长时间阻塞
				}

				// 使用带超时的上下文
				waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)

				// 使用channel接收通知，而不是ReceiveTimeout
				select {
				case msg := <-c.notifyCh:
					if msg != nil {
						c.logger().Debug(fmt.Sprintf("Received notification for consumer %s: %s", c.id, msg.Channel))
						c.lastPubsubTime.Store(time.Now().Unix())
					}
					cancel()
				case <-waitCtx.Done():
					// 超时或上下文取消，这是正常的
					cancel()
				}
			}
		} else {
			// PubSub连接不健康，标记为不健康并回退到轮询模式
			c.logger().Debug(fmt.Sprintf("PubSub connection unhealthy for consumer %s, falling back to polling", c.id))
			c.pubsubHealthy.Store(false)
		}
		c.muSub.Unlock()

		// 如果PubSub不可用，回退到简单的睡眠
		if !c.pubsubHealthy.Load() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(min(timeout, 1*time.Second)): // 缩短轮询间隔
			}
		}
	} else {
		// 如果禁用了通知，回退到简单的睡眠
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeout):
		}
	}
	return nil
}
