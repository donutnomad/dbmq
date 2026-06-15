package dbmq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrFailedFetchMessage 表示消息拉取失败
type ErrFailedFetchMessage struct {
	err error
}

func (e *ErrFailedFetchMessage) Error() string {
	return e.err.Error()
}

// PollLoopTimeout 使用动态超时时间进行消息轮询循环
// fn 函数可以根据上次消息数量动态调整下次轮询的超时时间
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
			c.logger().Debug(fmt.Sprintf("[CONSUMER-%s] PollLoop: not ready", c.config.GroupID))
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
				c.logger().Error(fmt.Sprintf("ERROR: failed to batch fetch messages for consumer %s: %v", c.ID(), err))
				timeA.Reset(1 * time.Second)
				continue
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

// PollLoop 使用固定超时时间进行消息轮询循环
func (c *Consumer) PollLoop(ctx context.Context, timeout time.Duration, onMessage func(messages []ConsumerMessage)) error {
	return c.PollLoopTimeout(ctx, func(c *Consumer, lastMessageCount int64) time.Duration {
		return timeout
	}, onMessage)
}

// Poll 从订阅的Topic和分区中拉取消息
// 会返回的错误:
// - ErrFailedFetchMessage
// - ErrRebalanceInProgress
// - context.DeadlineExceeded
// - context.Canceled
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	return c.actor.Poll(ctx, timeout)
}
