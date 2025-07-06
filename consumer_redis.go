package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/types"
	"github.com/samber/lo"
	"time"
)

// --- Redis Notification Helpers ---

// toChannelNames converts a slice of PartitionInfo into a slice of Redis channel names.
func toChannelNames(partitions []types.PartitionInfo) []string {
	if len(partitions) == 0 {
		return nil
	}
	return lo.Map(partitions, func(p types.PartitionInfo, _ int) string {
		return fmt.Sprintf("mq_notify:%s:%d", p.Topic, p.Partition)
	})
}

// subscribeToChannels subscribes the consumer to Redis channels for new message notifications.
func (c *Consumer) subscribeToChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	// Ensure the PubSub connection is healthy before proceeding.
	c.ensurePubSubConnection()

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub != nil && c.pubsubHealthy.Load() {
		channels := toChannelNames(partitions)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
			c.logger().Error("Failed to subscribe to redis channels", "channels", channels, "error", err)
			c.pubsubHealthy.Store(false)
		} else {
			c.logger().Debug("Subscribed to channels", "channels", channels)
			c.lastPubsubTime.Store(time.Now().Unix())
		}
	} else {
		c.logger().Warn("PubSub connection not available, cannot subscribe to channels")
	}
}

// unsubscribeFromChannels unsubscribes the consumer from Redis channels.
func (c *Consumer) unsubscribeFromChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub == nil || !c.pubsubHealthy.Load() {
		return // Nothing to unsubscribe from or connection not healthy.
	}

	channels := toChannelNames(partitions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.pubsub.Unsubscribe(ctx, channels...); err != nil {
		c.logger().Error("Failed to unsubscribe from redis channels", "channels", channels, "error", err)
		c.pubsubHealthy.Store(false)
	} else {
		c.logger().Debug("Unsubscribed from channels", "channels", channels)
		c.lastPubsubTime.Store(time.Now().Unix())
	}
}

// --- Helper methods ---

// resetNotificationStateBatch deletes multiple notification state keys in Redis using a single command.
func (c *Consumer) resetNotificationStateBatch(ctx context.Context, partitions []types.PartitionInfo) {
	if len(partitions) == 0 || c.redis == nil {
		return
	}
	keys := lo.Map(partitions, func(p types.PartitionInfo, _ int) string {
		return fmt.Sprintf("mq_notify_state:%s:%d", p.Topic, p.Partition)
	})
	if err := c.redis.Del(ctx, keys...).Err(); err != nil {
		c.logger().Warn("Failed to batch reset notification state", "count", len(partitions), "error", err)
	}
}

// tryResetNotificationStateBatch asynchronously deletes multiple notification state keys.
func (c *Consumer) tryResetNotificationStateBatch(ctx context.Context, partitions []types.PartitionInfo) {
	if c.config.NotificationEnabled && c.redis != nil {
		go c.resetNotificationStateBatch(ctx, partitions)
	}
}

// ensurePubSubConnection ensures the PubSub connection is healthy, attempting to reconnect if not.
// This version is optimized to avoid nested locks and improve logging.
func (c *Consumer) ensurePubSubConnection() {
	// 1. Quick, lock-free check for health.
	now := time.Now().Unix()
	lastActivity := c.lastPubsubTime.Load()
	if c.pubsub != nil && c.pubsubHealthy.Load() && (now-lastActivity <= 30) {
		return // Connection is healthy and active.
	}

	// 2. Get current assignments *before* locking, to avoid nested locks.
	assignedPartitions := c.getAssignedPartitions()

	// 3. Acquire lock to perform detailed check and potential refresh.
	c.muSub.Lock()
	defer c.muSub.Unlock()

	// 4. Double-check lock: another goroutine might have fixed the connection while we waited for the lock.
	now = time.Now().Unix()
	lastActivity = c.lastPubsubTime.Load()
	if c.pubsub != nil && c.pubsubHealthy.Load() && (now-lastActivity <= 30) {
		return
	}

	c.logger().Debug("PubSub connection needs refresh.")

	// 5. Close the old connection if it exists.
	if c.pubsub != nil {
		if err := c.pubsub.Close(); err != nil {
			c.logger().Warn("Error closing old pubsub connection", "error", err)
		}
		c.pubsub = nil
		c.notifyCh = nil
	}

	// 6. Create a new connection.
	// The context here is for subsequent operations, not the initial object creation.
	c.pubsub = c.redis.Subscribe(context.Background())
	c.notifyCh = c.pubsub.Channel()
	c.pubsubHealthy.Store(true)
	c.lastPubsubTime.Store(now)
	c.logger().Debug("Successfully refreshed PubSub connection.")

	c.subscribe(assignedPartitions)
}

func (c *Consumer) subscribe(topics []types.PartitionInfo) {
	if len(topics) == 0 {
		return
	}
	channels := toChannelNames(topics)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.pubsub.Subscribe(ctx, channels...); err != nil {
		c.logger().Error("Failed to resubscribe to channels after connection refresh", "channels", channels, "error", err)
		c.pubsubHealthy.Store(false)
	} else {
		c.logger().Debug("Resubscribed to channels", "channels", channels)
	}
}
