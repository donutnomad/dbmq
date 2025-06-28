package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/types"
	"strings"
	"time"
)

// --- Redis Notification Helpers ---

func toChannelNames(partitions []types.PartitionInfo) []string {
	channels := make([]string, 0, len(partitions))
	for _, p := range partitions {
		channels = append(channels, fmt.Sprintf("mq_notify:%s:%d", p.Topic, p.Partition))
	}
	return channels
}

func (c *Consumer) subscribeToChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	// 确保PubSub连接健康
	c.ensurePubSubConnection()

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub != nil && c.pubsubHealthy.Load() {
		channels := toChannelNames(partitions)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
			c.logger().Error(fmt.Sprintf("ERROR: failed to subscribe to redis channels %v: %v", strings.Join(channels, ","), err))
			c.pubsubHealthy.Store(false)
		} else {
			c.logger().Debug("Subscribed to channels", "consumer-id", c.id, "channels", strings.Join(channels, ","))
			c.lastPubsubTime.Store(time.Now().Unix())
		}
	} else {
		c.logger().Warn(fmt.Sprintf("WARN: PubSub connection not available for consumer %s", c.id))
	}
}

func (c *Consumer) unsubscribeFromChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub == nil || !c.pubsubHealthy.Load() {
		return // Nothing to unsubscribe from or connection not healthy
	}

	channels := toChannelNames(partitions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.pubsub.Unsubscribe(ctx, channels...); err != nil {
		c.logger().Error(fmt.Sprintf("ERROR: failed to unsubscribe from redis channels %v: %v", channels, err))
		c.pubsubHealthy.Store(false)
	} else {
		c.logger().Debug(fmt.Sprintf("Consumer %s unsubscribed from channels: %v", c.id, channels))
		c.lastPubsubTime.Store(time.Now().Unix())
	}

	// 不要关闭pubsub连接，保持连接以便重用
	// pubsub连接只在消费者关闭时才关闭
}

// --- Helper methods ---

// resetNotificationState deletes the notification state key in Redis, allowing a subsequent
// producer to trigger a new notification. This is part of the "Intelligent Notification Coalescing" pattern.
func (c *Consumer) resetNotificationState(ctx context.Context, p types.PartitionInfo) {
	key := fmt.Sprintf("mq_notify_state:%s:%d", p.Topic, p.Partition)
	if err := c.redis.Del(ctx, key).Err(); err != nil {
		c.logger().Warn(fmt.Sprintf("WARN: failed to reset notification state for %v: %v", p, err))
	}
}

// 删除该分区的通知状态
func (c *Consumer) tryResetNotificationState(ctx context.Context, partition types.PartitionInfo) {
	if c.config.NotificationEnabled && c.redis != nil {
		go c.resetNotificationState(ctx, partition)
	}
}

// ensurePubSubConnection 确保PubSub连接健康，如果不健康则尝试重新连接
func (c *Consumer) ensurePubSubConnection() {
	c.muSub.Lock()
	defer c.muSub.Unlock()

	// 检查连接是否健康
	now := time.Now().Unix()
	lastActivity := c.lastPubsubTime.Load()

	// 如果超过30秒没有活动，或者连接标记为不健康，尝试重新连接
	if c.pubsub == nil || !c.pubsubHealthy.Load() || (now-lastActivity > 30) {
		c.logger().Debug(fmt.Sprintf("PubSub connection needs refresh for consumer %s", c.id))

		// 关闭旧连接
		if c.pubsub != nil {
			c.pubsub.Close()
			c.pubsub = nil
			c.notifyCh = nil
		}

		// 创建新连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		c.pubsub = c.redis.Subscribe(ctx)
		if c.pubsub != nil {
			c.notifyCh = c.pubsub.Channel()
			c.pubsubHealthy.Store(true)
			c.lastPubsubTime.Store(now)

			// 重新订阅当前分配的分区
			assignedPartitions := c.getAssignedPartitions()
			if len(assignedPartitions) > 0 {
				channels := toChannelNames(assignedPartitions)
				if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
					c.logger().Error(fmt.Sprintf("ERROR: failed to resubscribe to channels %v: %v", channels, err))
					c.pubsubHealthy.Store(false)
				} else {
					c.logger().Debug("Resubscribed to channels", "consumer-id", c.id, "channels", strings.Join(channels, ","))
				}
			}
		} else {
			c.logger().Error(fmt.Sprintf("ERROR: failed to create PubSub connection for consumer %s", c.id))
			c.pubsubHealthy.Store(false)
		}
	}
}
