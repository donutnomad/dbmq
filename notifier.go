package dbmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/dbmq/internal/types"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
)

// NotifyMessage 表示从 Notifier 接收到的通知消息
type NotifyMessage struct {
	Partition types.PartitionInfo
}

// NoopWaiter 空实现，仅基于超时等待
type NoopWaiter struct{}

func (NoopWaiter) Wait(ctx context.Context, timeout time.Duration) <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		select {
		case ch <- struct{}{}:
		default:
		}
		close(ch)
	}()
	return ch
}

// RedisNotifier 基于 Redis PubSub 的 Notifier 实现
type RedisNotifier struct {
	redis   redis.UniversalClient
	pubsub  *redis.PubSub
	healthy atomic.Bool

	mu       sync.Mutex
	notifyCh chan NotifyMessage
	waitCh   chan struct{} // 用于 Wait() 的 channel
	done     chan struct{}
	closed   atomic.Bool

	// 用于跟踪最后活动时间
	lastActivity atomic.Int64
}

// NewRedisNotifier 创建一个新的 RedisNotifier
func NewRedisNotifier(client redis.UniversalClient) *RedisNotifier {
	n := &RedisNotifier{
		redis:    client,
		notifyCh: make(chan NotifyMessage, 100),
		waitCh:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	n.healthy.Store(false)
	n.lastActivity.Store(time.Now().Unix())
	return n
}

// Subscribe 订阅指定分区的通知
func (n *RedisNotifier) Subscribe(partitions []types.PartitionInfo) error {
	if len(partitions) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed.Load() {
		return fmt.Errorf("notifier is closed")
	}

	// 如果 pubsub 未初始化，先初始化
	if n.pubsub == nil {
		n.pubsub = n.redis.Subscribe(context.Background())
		n.healthy.Store(true)
		n.lastActivity.Store(time.Now().Unix())

		// 启动消息转发 goroutine
		go n.forwardMessages()
	}

	channels := partitionToChannels(partitions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := n.pubsub.Subscribe(ctx, channels...); err != nil {
		n.healthy.Store(false)
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	n.lastActivity.Store(time.Now().Unix())
	return nil
}

// Unsubscribe 取消订阅指定分区的通知
func (n *RedisNotifier) Unsubscribe(partitions []types.PartitionInfo) error {
	if len(partitions) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.pubsub == nil || !n.healthy.Load() {
		return nil
	}

	channels := partitionToChannels(partitions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := n.pubsub.Unsubscribe(ctx, channels...); err != nil {
		n.healthy.Store(false)
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	n.lastActivity.Store(time.Now().Unix())
	return nil
}

// NotifyCh 返回通知 channel
func (n *RedisNotifier) NotifyCh() <-chan NotifyMessage {
	return n.notifyCh
}

// Close 关闭 notifier
func (n *RedisNotifier) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil // 已经关闭
	}

	close(n.done)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.healthy.Store(false)

	if n.pubsub != nil {
		if err := n.pubsub.Close(); err != nil {
			return err
		}
		n.pubsub = nil
	}

	close(n.notifyCh)
	return nil
}

// IsHealthy 返回连接是否健康
func (n *RedisNotifier) IsHealthy() bool {
	return n.healthy.Load() && !n.closed.Load()
}

// Wait 等待通知到达或超时
// 返回的 channel 在有通知、超时或 ctx 取消时会收到信号
func (n *RedisNotifier) Wait(ctx context.Context, timeout time.Duration) <-chan struct{} {
	ch := make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
		case <-n.done:
		case <-timer.C:
		case <-n.waitCh:
		}

		select {
		case ch <- struct{}{}:
		default:
		}
		close(ch)
	}()

	return ch
}

// forwardMessages 将 Redis 消息转发到 notifyCh 和 waitCh
func (n *RedisNotifier) forwardMessages() {
	n.mu.Lock()
	pubsub := n.pubsub
	n.mu.Unlock()

	if pubsub == nil {
		return
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-n.done:
			return
		case msg, ok := <-ch:
			if !ok {
				n.healthy.Store(false)
				return
			}

			partition, err := channelToPartition(msg.Channel)
			if err != nil {
				continue // 忽略无法解析的消息
			}

			n.lastActivity.Store(time.Now().Unix())

			// 发送到 notifyCh
			select {
			case n.notifyCh <- NotifyMessage{Partition: partition}:
			default:
				// channel 已满，丢弃消息
			}

			// 发送到 waitCh
			select {
			case n.waitCh <- struct{}{}:
			default:
				// channel 已满
			}
		}
	}
}

// partitionToChannels 将分区列表转换为 Redis channel 名称列表
func partitionToChannels(partitions []types.PartitionInfo) []string {
	return lo.Map(partitions, func(p types.PartitionInfo, _ int) string {
		return fmt.Sprintf("mq_notify:%s:%d", p.Topic, p.Partition)
	})
}

// channelToPartition 将 Redis channel 名称解析为分区信息
func channelToPartition(channel string) (types.PartitionInfo, error) {
	var topic string
	var partition uint
	_, err := fmt.Sscanf(channel, "mq_notify:%s:%d", &topic, &partition)
	if err != nil {
		// 尝试更宽松的解析方式
		n, err := fmt.Sscanf(channel, "mq_notify:%255s", &topic)
		if err != nil || n == 0 {
			return types.PartitionInfo{}, fmt.Errorf("invalid channel format: %s", channel)
		}
		// 查找最后一个冒号来分离 topic 和 partition
		for i := len(channel) - 1; i >= 0; i-- {
			if channel[i] == ':' {
				topic = channel[len("mq_notify:"):i]
				_, err = fmt.Sscanf(channel[i+1:], "%d", &partition)
				if err != nil {
					return types.PartitionInfo{}, fmt.Errorf("invalid partition in channel: %s", channel)
				}
				break
			}
		}
	}
	return types.PartitionInfo{Topic: topic, Partition: partition}, nil
}

// FakeNotifier 用于测试的 Notifier 实现
type FakeNotifier struct {
	mu           sync.Mutex
	notifyCh     chan NotifyMessage
	waitCh       chan struct{} // 用于 Wait() 的 channel
	healthy      atomic.Bool
	closed       atomic.Bool
	subscribed   map[string]struct{}     // 已订阅的 channel 集合
	subscribes   [][]types.PartitionInfo // 记录所有订阅调用
	unsubscribes [][]types.PartitionInfo // 记录所有取消订阅调用
}

// NewFakeNotifier 创建一个新的 FakeNotifier
func NewFakeNotifier() *FakeNotifier {
	n := &FakeNotifier{
		notifyCh:   make(chan NotifyMessage, 100),
		waitCh:     make(chan struct{}, 1),
		subscribed: make(map[string]struct{}),
	}
	n.healthy.Store(true)
	return n
}

// Subscribe 记录订阅请求
func (n *FakeNotifier) Subscribe(partitions []types.PartitionInfo) error {
	if len(partitions) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed.Load() {
		return fmt.Errorf("notifier is closed")
	}

	n.subscribes = append(n.subscribes, partitions)

	for _, p := range partitions {
		key := fmt.Sprintf("%s:%d", p.Topic, p.Partition)
		n.subscribed[key] = struct{}{}
	}

	return nil
}

// Unsubscribe 记录取消订阅请求
func (n *FakeNotifier) Unsubscribe(partitions []types.PartitionInfo) error {
	if len(partitions) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed.Load() {
		return nil
	}

	n.unsubscribes = append(n.unsubscribes, partitions)

	for _, p := range partitions {
		key := fmt.Sprintf("%s:%d", p.Topic, p.Partition)
		delete(n.subscribed, key)
	}

	return nil
}

// NotifyCh 返回通知 channel
func (n *FakeNotifier) NotifyCh() <-chan NotifyMessage {
	return n.notifyCh
}

// Close 关闭 notifier
func (n *FakeNotifier) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	close(n.notifyCh)
	return nil
}

// IsHealthy 返回连接是否健康
func (n *FakeNotifier) IsHealthy() bool {
	return n.healthy.Load() && !n.closed.Load()
}

// Wait 等待通知到达或超时
// 返回的 channel 在有通知、超时或 ctx 取消时会收到信号
func (n *FakeNotifier) Wait(ctx context.Context, timeout time.Duration) <-chan struct{} {
	ch := make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-n.waitCh:
		}

		select {
		case ch <- struct{}{}:
		default:
		}
		close(ch)
	}()

	return ch
}

// Notify 手动触发通知（用于测试）
func (n *FakeNotifier) Notify(partition types.PartitionInfo) bool {
	if n.closed.Load() {
		return false
	}

	// 发送到 waitCh（非阻塞）
	select {
	case n.waitCh <- struct{}{}:
	default:
		// channel 已满，忽略
	}

	// 发送到 notifyCh
	select {
	case n.notifyCh <- NotifyMessage{Partition: partition}:
		return true
	default:
		return false // channel 已满
	}
}

// SetHealthy 设置健康状态（用于测试）
func (n *FakeNotifier) SetHealthy(healthy bool) {
	n.healthy.Store(healthy)
}

// GetSubscribed 返回当前已订阅的分区列表（用于测试验证）
func (n *FakeNotifier) GetSubscribed() []types.PartitionInfo {
	n.mu.Lock()
	defer n.mu.Unlock()

	var result []types.PartitionInfo
	for key := range n.subscribed {
		var topic string
		var partition uint
		_, _ = fmt.Sscanf(key, "%255[^:]:%d", &topic, &partition)
		// 更可靠的解析方式
		for i := len(key) - 1; i >= 0; i-- {
			if key[i] == ':' {
				topic = key[:i]
				_, _ = fmt.Sscanf(key[i+1:], "%d", &partition)
				break
			}
		}
		result = append(result, types.PartitionInfo{Topic: topic, Partition: partition})
	}
	return result
}

// GetSubscribeCalls 返回所有订阅调用记录（用于测试验证）
func (n *FakeNotifier) GetSubscribeCalls() [][]types.PartitionInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.subscribes
}

// GetUnsubscribeCalls 返回所有取消订阅调用记录（用于测试验证）
func (n *FakeNotifier) GetUnsubscribeCalls() [][]types.PartitionInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.unsubscribes
}

// Reset 重置 FakeNotifier 状态（用于测试）
func (n *FakeNotifier) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.subscribed = make(map[string]struct{})
	n.subscribes = nil
	n.unsubscribes = nil
	n.healthy.Store(true)
}
