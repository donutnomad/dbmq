package dbmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/repo"
	"github.com/samber/lo"
)

type ErrFailedFetchMessage struct {
	err error
}

func (e *ErrFailedFetchMessage) Error() string {
	return e.err.Error()
}

func (c *Consumer) PollLoopTimeout(ctx context.Context, fn func(c *Consumer, lastMessageCount int64) time.Duration, onMessage func(messages []ConsumerMessage)) error {
	var timeA = time.NewTimer(1 * time.Second)
	var lastMessageCount int64 = 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeA.C:
		}
		if !c.IsReady() {
			timeA.Reset(1 * time.Second)
			continue
		}
		messages, err := c.Poll(ctx, fn(c, lastMessageCount))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			var rebalanceErr *ErrRebalanceInProgress
			if errors.As(err, &rebalanceErr) { // 正在重平衡
				timeA.Reset(1 * time.Second)
				continue
			}
			var fetchErr *ErrFailedFetchMessage
			if errors.As(err, &fetchErr) {
				c.logger().Error(fmt.Sprintf("ERROR: failed to batch fetch messages for consumer %s: %v", c.id, err))
			}
		} else {
			lastMessageCount = int64(len(messages))
			if messages == nil {
				timeA.Reset(500 * time.Millisecond)
				continue
			}
			if len(messages) > 0 {
				onMessage(messages)
			}
		}
		timeA.Reset(0)
	}
}

func (c *Consumer) PollLoop(ctx context.Context, timeout time.Duration, onMessage func(messages []ConsumerMessage)) error {
	return c.PollLoopTimeout(ctx, func(c *Consumer, lastMessageCount int64) time.Duration {
		return timeout
	}, onMessage)
}

// Poll 从订阅的Topic和分区中拉取消息
// 会返回的错误:
// ErrFailedFetchMessage
// ErrRebalanceInProgress
// context.DeadlineExceeded
// context.Canceled
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	// 如果正在进行重新均衡，立即返回并提示用户. 心跳循环负责处理重新均衡过程
	if c.rebalancing.Load() {
		return nil, &ErrRebalanceInProgress{GroupID: c.config.GroupID}
	}

	// 获取当前分配的分区和代际快照
	// 用于在 waitPoll 后检测是否发生了重平衡
	assignedPartitions := c.getAssignedPartitions()
	snapshotGeneration := c.GetGenerationID()

	if isEmpty(assignedPartitions) {
		return nil, nil
	}
	if err := c.waitPoll(ctx, timeout, assignedPartitions); err != nil {
		return nil, err
	}

	// waitPoll 返回后，检查是否发生了重平衡
	// 这是为了防止在 waitPoll 等待期间发生重平衡，导致 assignedPartitions 和 alreadyConsumeMessageIDs 不一致
	if c.rebalancing.Load() || c.GetGenerationID() != snapshotGeneration {
		return nil, &ErrRebalanceInProgress{GroupID: c.config.GroupID}
	}

	fetchCtx, cancel := context.WithTimeout(ctx, c.config.GetPollFetchTimeout())
	defer cancel()

	// 批量获取消息, 获取id > ?的记录
	allMessages, err := c.repo.FetchMessagesBatch(fetchCtx, lo.Map(assignedPartitions, func(p db.PartitionInfo, _ int) repo.PartitionRequest {
		return repo.PartitionRequest{
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
		return nil, &ErrFailedFetchMessage{err}
	}

	partitionsToReset := slices.Collect(maps.Keys(lo.GroupBy(allMessages, func(msg db.Message) db.PartitionInfo {
		return msg.ToPartitionInfo()
	})))
	// 批量重置通知状态
	c.tryResetNotificationStateBatch(context.Background(), partitionsToReset)

	messages := new(ConsumerMessages).FromMessages(allMessages)

	// 如果启用了日志记录，打印消息详情
	if IsLogEnabled(ctx) {
		for _, msg := range messages {
			c.logger().DebugContext(ctx, "consumer.poll",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"key", msg.Key,
				"message_id", msg.ID,
				"headers", msg.Headers,
			)
		}
	}

	return messages, nil
}

// 返回错误
// context.DeadlineExceeded
// context.Canceled
func (c *Consumer) waitPoll(ctx context.Context, timeout time.Duration, partitions []db.PartitionInfo) error {
	// 如果启用了通知优化，使用Redis Pub/Sub等待通知
	if c.config.NotificationEnabled && c.redis != nil {
		// 检查并确保PubSub连接健康
		c.ensurePubSubConnection()
		c.muSub.Lock()
		c.subscribe(partitions)
		var pubSub = c.pubsub
		c.muSub.Unlock()

		if pubSub != nil && c.pubsubHealthy.Load() {
			select {
			case msg, ok := <-c.notifyCh:
				if !ok {
					// channel 已关闭，标记为不健康并回退到轮询模式
					c.logger().Debug(fmt.Sprintf("notifyCh closed for consumer %s, marking pubsub unhealthy", c.id))
					c.pubsubHealthy.Store(false)
					// 继续执行到下面的轮询逻辑
					break
				}
				c.logger().Debug(fmt.Sprintf("Received notification for consumer %s: %s", c.id, msg.Channel))
				c.lastPubsubTime.Store(time.Now().Unix())
				return nil
			case <-time.After(timeout):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// PubSub连接不健康，标记为不健康并回退到轮询模式
		c.logger().Debug(fmt.Sprintf("PubSub connection unhealthy for consumer %s, falling back to polling", c.id))
		c.pubsubHealthy.Store(false)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return nil
	}
}
