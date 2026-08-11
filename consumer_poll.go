package dbmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	fetchRetryBaseDelay = time.Second
	fetchRetryMaxDelay  = 30 * time.Second
)

func defaultFetchRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	exponent := min(attempt-1, 5)
	delay := fetchRetryBaseDelay << exponent
	if delay > fetchRetryMaxDelay {
		delay = fetchRetryMaxDelay
	}

	// 使用 80%-100% 向下 jitter，保证实际延迟不超过上限。
	jitterWindow := delay / 5
	return delay - time.Duration(rand.Int64N(int64(jitterWindow)+1))
}

func (c *Consumer) nextFetchRetryDelay(attempt int) time.Duration {
	if c.fetchRetryDelay != nil {
		return c.fetchRetryDelay(attempt)
	}
	return defaultFetchRetryDelay(attempt)
}

// ErrFailedFetchMessage 表示消息拉取失败。
// 底层错误可能是内部 Fetch context 超时；调用方应先用 errors.As
// 匹配 ErrFailedFetchMessage，再判断调用方 context 的 Canceled/DeadlineExceeded。
type ErrFailedFetchMessage struct {
	err error
}

func (e *ErrFailedFetchMessage) Error() string {
	return e.err.Error()
}

func (e *ErrFailedFetchMessage) Unwrap() error {
	return e.err
}

// PollLoopTimeout 使用动态超时时间进行消息轮询循环
// fn 函数可以根据上次消息数量动态调整下次轮询的超时时间
// 每个 Consumer 实例同时执行一个 Poll；其他 Poll 会等待当前轮询完成或 context 取消。
// onMessage 内应通过取消 context 停止循环，Close 由 PollLoop 返回后的外层执行。
func (c *Consumer) PollLoopTimeout(ctx context.Context, fn func(c *Consumer, lastMessageCount int64) time.Duration, onMessage func(messages []ConsumerMessage)) error {
	var timeA = time.NewTimer(1 * time.Second)
	defer timeA.Stop()

	var lastMessageCount int64 = 0
	var pollImmediately bool
	var consecutiveFetchFailures int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.actor.lifecycleDone():
			return context.Canceled
		case <-timeA.C:
		}
		if !c.IsReady() {
			c.logger().Debug(fmt.Sprintf("[CONSUMER-%s] PollLoop: not ready", c.config.GroupID))
			timeA.Reset(1 * time.Second)
			continue
		}

		pollTimeout := fn(c, lastMessageCount)
		if pollImmediately {
			pollTimeout = 0
			pollImmediately = false
		}

		messages, err := c.Poll(ctx, pollTimeout)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			var rebalanceErr *ErrRebalanceInProgress
			if errors.As(err, &rebalanceErr) { // 正在重平衡
				pollImmediately = true
				timeA.Reset(0)
				continue
			}

			var fetchErr *ErrFailedFetchMessage
			if errors.As(err, &fetchErr) {
				consecutiveFetchFailures++
				retryDelay := c.nextFetchRetryDelay(consecutiveFetchFailures)
				c.logger().Error("failed to batch fetch messages; retrying",
					"consumer-id", c.ID(),
					"error", fetchErr,
					"retry-after", retryDelay,
					"consecutive-failures", consecutiveFetchFailures,
				)
				pollImmediately = true
				timeA.Reset(retryDelay)
				continue
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			c.logger().Error("unexpected poll error; retrying", "consumer-id", c.ID(), "error", err)
			pollImmediately = true
			timeA.Reset(1 * time.Second)
			continue
		} else {
			consecutiveFetchFailures = 0
			select {
			case <-c.actor.lifecycleDone():
				return context.Canceled
			default:
			}
			lastMessageCount = int64(len(messages))
			if messages == nil {
				timeA.Reset(500 * time.Millisecond)
				continue
			}
			if len(messages) > 0 {
				if !c.actor.deliver(messages, onMessage) {
					return context.Canceled
				}
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
// - ErrFailedFetchMessage（包括内部数据库查询超时；应优先使用 errors.As 分类）
// - ErrRebalanceInProgress
// - context.DeadlineExceeded（调用方 context 到期，且未匹配 ErrFailedFetchMessage）
// - context.Canceled（调用方取消或 Consumer 关闭，且未匹配 ErrFailedFetchMessage）
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	return c.actor.Poll(ctx, timeout)
}
