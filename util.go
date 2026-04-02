package dbmq

import (
	"context"
	"time"
)

// cronRun 按固定时间点对齐触发，类似 cron 的行为。
// 例如 interval=10s 时，在每分钟的 0s、10s、20s... 时刻触发，不受执行耗时影响。
// delay > 0 时在触发前额外等待 delay，可用于错峰。
func cronRun(ctx context.Context, interval, delay time.Duration, f func(ctx context.Context)) error {
	f(ctx)
	for {
		now := time.Now()
		next := now.Truncate(interval).Add(interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(next.Sub(now)):
			if delay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
			f(ctx)
		}
	}
}
