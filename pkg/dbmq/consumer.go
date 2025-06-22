package dbmq

import (
	"context"
	"dbmq/internal/dal"
	dberrors "dbmq/pkg/dbmq/errors"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ConsumerConfig holds configuration for the consumer.
type ConsumerConfig struct {
	DB                  *gorm.DB
	Redis               *redis.Client
	GroupID             string
	NotificationEnabled bool
	HeartbeatInterval   time.Duration
	Topics              []string
	PollFetchLimit      int           // Max messages to fetch per partition per poll
	PollFetchTimeout    time.Duration // Timeout for the DB fetch part of a poll
}

// ConsumerMessage is a message received by the consumer.
type ConsumerMessage struct {
	Topic     string
	Partition uint
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// Consumer represents a consumer instance that is part of a consumer group.
type Consumer struct {
	config ConsumerConfig
	id     string
	db     *gorm.DB
	redis  *redis.Client
	topics []string

	// Redis Pub/Sub for notifications
	pubsub   *redis.PubSub
	notifyCh <-chan *redis.Message
	muSub    sync.Mutex // Protects pubsub object

	// State for rebalancing and polling
	mu               sync.RWMutex
	generationID     uint
	assignment       map[string][]uint // topic -> partitions
	committedOffsets map[types.PartitionInfo]int64
	polledOffsets    map[types.PartitionInfo]int64 // Offsets from the last successful poll
	heartbeatStarted atomic.Bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewConsumer creates a new consumer instance.
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 3 * time.Second
	}
	return &Consumer{
		config:           config,
		id:               uuid.NewString(),
		db:               config.DB,
		redis:            config.Redis,
		topics:           config.Topics,
		stopCh:           make(chan struct{}),
		assignment:       make(map[string][]uint),
		committedOffsets: make(map[types.PartitionInfo]int64),
		polledOffsets:    make(map[types.PartitionInfo]int64),
	}, nil
}

// Subscribe registers the topics this consumer will listen to.
// This must be called before the first call to Poll.
// It also triggers the consumer to join the group and start heartbeating.
func (c *Consumer) Subscribe(topics ...string) error {
	c.mu.Lock()
	c.topics = topics
	c.mu.Unlock()

	// Start the heartbeat loop on subscribe, which allows the consumer to join the group
	// even if Poll() is not called immediately.
	if c.heartbeatStarted.CompareAndSwap(false, true) {
		// Register the consumer immediately.
		if err := c.register(); err != nil {
			// If we can't even register, stop everything.
			c.heartbeatStarted.Store(false)
			return fmt.Errorf("initial registration failed: %w", err)
		}
		c.wg.Add(1)
		go c.heartbeatLoop()
	}

	return nil
}

// Close gracefully shuts down the consumer, its loops, and commits one final offset.
func (c *Consumer) Close() {
	log.Printf("Closing consumer %s...", c.id)
	// Stop all background loops (e.g., heartbeat)
	close(c.stopCh)
	c.wg.Wait()

	// Close the pubsub connection
	c.muSub.Lock()
	if c.pubsub != nil {
		c.pubsub.Close()
	}
	c.muSub.Unlock()

	// Commit any pending offsets for the final time.
	if err := c.CommitSync(); err != nil {
		log.Printf("ERROR: final commit failed for consumer %s: %v", c.id, err)
	}

	// Gracefully leave the group by deleting the heartbeat.
	// This allows the coordinator to trigger a rebalance immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dal.DeleteHeartbeat(ctx, c.db, c.config.GroupID, c.id); err != nil {
		log.Printf("ERROR: failed to leave group gracefully for consumer %s: %v", c.id, err)
	}

	log.Printf("Consumer %s shut down.", c.id)
}

// Poll fetches messages for the subscribed topics and partitions.
// It is the core of the consumer's logic.
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	// Check for rebalance and update assignments if necessary.
	rebalanced, err := c.ensureAssignment()
	if err != nil {
		return nil, fmt.Errorf("failed to ensure partition assignment: %w", err)
	}
	if rebalanced {
		// Signal to the user that a rebalance occurred and their partition set may have changed.
		// They should not process messages from this Poll call.
		return nil, &dberrors.ErrRebalanceInProgress{GroupID: c.config.GroupID}
	}

	assignedPartitions := c.getAssignedPartitions()
	if len(assignedPartitions) == 0 {
		// No partitions assigned, wait for the timeout.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil // Timed out, return empty list.
		}
	}

	// Use Redis Pub/Sub to wait for notifications if enabled.
	if c.config.NotificationEnabled && c.redis != nil {
		c.muSub.Lock()
		if c.pubsub != nil {
			// Drain any pre-existing message that arrived before we started waiting.
			select {
			case <-c.notifyCh:
				// Message was waiting, we'll fetch immediately.
				log.Printf("Drained pending notification for consumer %s", c.id)
			default:
				// No message was waiting, proceed to wait with timeout.
				_, err := c.pubsub.ReceiveTimeout(ctx, timeout)
				if err != nil {
					// This is likely a timeout error, which is expected.
					// We'll proceed to the fetch stage regardless.
					if !errors.Is(err, redis.ErrClosed) {
						log.Printf("Notification wait ended for consumer %s (may be a timeout): %v", c.id, err)
					}
				}
			}
		}
		c.muSub.Unlock()
	} else {
		// Fallback to simple sleep if notifications are disabled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
		}
	}

	// Concurrently fetch from all assigned partitions.
	// This fetch is performed after a notification or a timeout.
	type fetchResult struct {
		messages  []types.Message
		partition types.PartitionInfo
		err       error
	}
	resultsCh := make(chan fetchResult, len(assignedPartitions))
	// Use a shorter context for the fetch itself, as the primary wait has already occurred.
	fetchTimeout := 5 * time.Second
	if c.config.PollFetchTimeout > 0 {
		fetchTimeout = c.config.PollFetchTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	var fetchWg sync.WaitGroup
	for _, p := range assignedPartitions {
		fetchWg.Add(1)
		go func(partition types.PartitionInfo) {
			defer fetchWg.Done()
			offset := c.getOffset(partition)
			limit := 100
			if c.config.PollFetchLimit > 0 {
				limit = c.config.PollFetchLimit
			}
			messages, err := dal.FetchMessages(fetchCtx, c.db, partition.Topic, partition.Partition, offset, limit)
			resultsCh <- fetchResult{messages: messages, partition: partition, err: err}
		}(p)
	}

	fetchWg.Wait()
	close(resultsCh)

	var allMessages []types.Message
	var lastErr error
	for res := range resultsCh {
		if res.err != nil {
			// Collect last error, a more robust strategy might be needed
			if !errors.Is(res.err, context.Canceled) && !errors.Is(res.err, context.DeadlineExceeded) {
				lastErr = res.err
				log.Printf("ERROR: failed to fetch from partition %v: %v", res.partition, res.err)
			}
			continue
		}
		if len(res.messages) > 0 {
			allMessages = append(allMessages, res.messages...)
			lastMessage := res.messages[len(res.messages)-1]
			c.setPolledOffset(res.partition, lastMessage.ID)

			// "Re-arm" the notification trigger for this partition since we just fetched data.
			if c.config.NotificationEnabled && c.redis != nil {
				go c.resetNotificationState(context.Background(), res.partition)
			}
		}
	}

	if lastErr != nil && len(allMessages) == 0 {
		return nil, fmt.Errorf("all fetch attempts failed, last error: %w", lastErr)
	}

	return toConsumerMessages(allMessages), nil
}

// heartbeatLoop is the background goroutine that periodically sends heartbeats.
func (c *Consumer) heartbeatLoop() {
	defer c.wg.Done()
	log.Printf("Heartbeat loop started for consumer %s", c.id)

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	// The initial registration is done in Subscribe. This loop just sends lightweight heartbeats.
	for {
		select {
		case <-c.stopCh:
			log.Printf("Heartbeat loop stopped for consumer %s.", c.id)
			return
		case <-ticker.C:
			if err := dal.UpdateHeartbeat(context.Background(), c.db, c.config.GroupID, c.id); err != nil {
				log.Printf("ERROR: heartbeat failed for consumer %s: %v", c.id, err)
			}
		}
	}
}

// register sends the consumer's initial registration, including subscriptions.
func (c *Consumer) register() error {
	c.mu.RLock()
	subsJSON, err := json.Marshal(c.topics)
	c.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal subscribed topics: %w", err)
	}

	heartbeat := &types.ConsumerHeartbeat{
		GroupID:            c.config.GroupID,
		ConsumerID:         c.id,
		SubscribedTopics:   subsJSON,
		AssignedPartitions: []byte(`{}`), // Start with empty assignment
		LastHeartbeat:      time.Now(),
	}
	return dal.RegisterConsumer(context.Background(), c.db, heartbeat)
}

// CommitSync commits the offsets for all currently assigned partitions.
// This is a blocking operation.
func (c *Consumer) CommitSync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Merge polled offsets into committed offsets before committing.
	for p, offset := range c.polledOffsets {
		c.committedOffsets[p] = offset
	}
	// Clear polled offsets after merging.
	c.polledOffsets = make(map[types.PartitionInfo]int64)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for p, offset := range c.committedOffsets {
		if err := dal.CommitOffset(ctx, c.db, c.config.GroupID, c.generationID, p, offset); err != nil {
			// In a real scenario, we might retry or collect errors.
			return fmt.Errorf("failed to commit offset for partition %v: %w", p, err)
		}
	}
	return nil
}

// ensureAssignment checks if the consumer's partition assignment is up to date.
// It returns true if a rebalance just happened.
func (c *Consumer) ensureAssignment() (bool, error) {
	hb, err := dal.GetHeartbeat(context.Background(), c.db, c.config.GroupID, c.id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// This can happen if the coordinator hasn't processed the first heartbeat yet.
			// Not an error, but no assignment is available yet.
			return false, nil
		}
		return false, fmt.Errorf("could not get own heartbeat: %w", err)
	}

	isRebalance := hb.GenerationID != c.generationID
	if !isRebalance {
		return false, nil // No rebalance needed.
	}

	// Rebalance is needed. All information (new generation, new assignment) is in the heartbeat record.
	log.Printf("Rebalance detected for consumer %s. Old gen: %d, New gen: %d", c.id, c.generationID, hb.GenerationID)

	newPartitionsMap, err := c.parsePartitions(hb.AssignedPartitions)
	if err != nil {
		// If we can't parse our new assignment, it's a critical failure.
		return false, fmt.Errorf("failed to parse new assignment: %w", err)
	}

	// Commit offsets for revoked partitions in a transaction
	err = c.db.Transaction(func(tx *gorm.DB) error {
		revokedPartitions := c.findRevokedPartitions(newPartitionsMap)
		if len(revokedPartitions) > 0 {
			// Unsubscribe from Redis channels for revoked partitions
			c.unsubscribeFromChannels(revokedPartitions)

			// CRITICAL FIX: Use the NEW generation ID for committing offsets of revoked partitions.
			// This proves to the coordinator that this consumer is aware of the rebalance.
			if err := c.commitOffsets(tx.Statement.Context, revokedPartitions, hb.GenerationID); err != nil {
				return err // Rollback transaction
			}
		}
		return nil // Commit transaction
	})
	if err != nil {
		// Log but continue, joining the new generation is more critical.
		log.Printf("ERROR: could not commit offsets for revoked partitions: %v", err)
	}

	// Update internal state
	c.mu.Lock()
	c.generationID = hb.GenerationID
	c.assignment = newPartitionsMap
	if err := c.clearAndFetchOffsetsForNewAssignment(); err != nil {
		// This is a critical failure, as we cannot determine the correct starting point.
		c.mu.Unlock()
		return false, fmt.Errorf("failed to refresh offsets for new assignment: %w", err)
	}
	c.mu.Unlock()

	// Subscribe to new channels
	c.subscribeToChannels(c.getAssignedPartitions())

	return true, nil
}

// commitOffsets commits the offsets for revoked partitions.
func (c *Consumer) commitOffsets(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	for _, p := range partitions {
		if offset, ok := c.committedOffsets[p]; ok {
			if err := dal.CommitOffset(ctx, c.db, c.config.GroupID, generationID, p, offset); err != nil {
				return fmt.Errorf("failed to commit offset for revoked partition %v: %w", p, err)
			}
		}
	}
	return nil
}

// --- Redis Notification Helpers ---

func toChannelNames(partitions []types.PartitionInfo) []string {
	channels := make([]string, len(partitions))
	for i, p := range partitions {
		channels[i] = fmt.Sprintf("mq_notify:%s:%d", p.Topic, p.Partition)
	}
	return channels
}

func (c *Consumer) subscribeToChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	// If we don't have a pubsub connection yet, create one.
	if c.pubsub == nil {
		c.pubsub = c.redis.Subscribe(context.Background())
		c.notifyCh = c.pubsub.Channel()
	}

	channels := toChannelNames(partitions)
	if err := c.pubsub.Subscribe(context.Background(), channels...); err != nil {
		log.Printf("ERROR: failed to subscribe to redis channels %v: %v", channels, err)
	} else {
		log.Printf("Consumer %s subscribed to channels: %v", c.id, channels)
	}
}

func (c *Consumer) unsubscribeFromChannels(partitions []types.PartitionInfo) {
	if !c.config.NotificationEnabled || c.redis == nil || len(partitions) == 0 {
		return
	}

	c.muSub.Lock()
	defer c.muSub.Unlock()

	if c.pubsub == nil {
		return // Nothing to unsubscribe from
	}

	channels := toChannelNames(partitions)
	if err := c.pubsub.Unsubscribe(context.Background(), channels...); err != nil {
		log.Printf("ERROR: failed to unsubscribe from redis channels %v: %v", channels, err)
	} else {
		log.Printf("Consumer %s unsubscribed from channels: %v", c.id, channels)
	}
}

// --- Helper methods ---

func (c *Consumer) getAssignedPartitions() []types.PartitionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	partitions := make([]types.PartitionInfo, 0)
	for topic, parts := range c.assignment {
		for _, pNum := range parts {
			partitions = append(partitions, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}
	return partitions
}

func (c *Consumer) clearAndFetchOffsetsForNewAssignment() error {
	newPartitionsList := c.getAssignedPartitions()
	c.committedOffsets = make(map[types.PartitionInfo]int64) // Clear old offsets
	c.polledOffsets = make(map[types.PartitionInfo]int64)    // Also clear polled offsets

	// Fetch committed offsets for the new assignment.
	if len(newPartitionsList) > 0 {
		fetchedOffsets, err := dal.GetCommittedOffsets(context.Background(), c.db, c.config.GroupID, newPartitionsList)
		if err != nil {
			log.Printf("ERROR: failed to fetch committed offsets for new assignment: %v", err)
			return fmt.Errorf("failed to fetch committed offsets for new assignment: %w", err)
		}

		for p, offset := range fetchedOffsets {
			c.committedOffsets[p] = offset
		}
	}
	return nil
}

func (c *Consumer) findRevokedPartitions(newPartitions map[string][]uint) []types.PartitionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var revoked []types.PartitionInfo

	for topic, currentPartitions := range c.assignment {
		newParts, ok := newPartitions[topic]
		if !ok { // Topic is no longer assigned
			for _, p := range currentPartitions {
				revoked = append(revoked, types.PartitionInfo{Topic: topic, Partition: p})
			}
			continue
		}

		newPartsSet := make(map[uint]struct{})
		for _, p := range newParts {
			newPartsSet[p] = struct{}{}
		}

		for _, p := range currentPartitions {
			if _, exists := newPartsSet[p]; !exists {
				revoked = append(revoked, types.PartitionInfo{Topic: topic, Partition: p})
			}
		}
	}
	return revoked
}

func (c *Consumer) parsePartitions(data []byte) (map[string][]uint, error) {
	var parsed map[string][]uint
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partitions json: %w", err)
	}
	return parsed, nil
}

func (c *Consumer) getOffset(p types.PartitionInfo) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Always fetch from the last known committed offset.
	return c.committedOffsets[p]
}

func (c *Consumer) setPolledOffset(p types.PartitionInfo, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.polledOffsets[p] = offset
}

func toConsumerMessages(msgs []types.Message) []ConsumerMessage {
	res := make([]ConsumerMessage, len(msgs))
	for i, m := range msgs {
		var headers map[string]string
		if len(m.Headers) > 0 {
			_ = json.Unmarshal(m.Headers, &headers)
		}

		var key []byte
		if m.MessageKey.Valid {
			key = []byte(m.MessageKey.String)
		}

		res[i] = ConsumerMessage{
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.ID,
			Key:       key,
			Value:     m.Body,
			Headers:   headers,
			Timestamp: m.CreatedAt,
		}
	}
	return res
}

// resetNotificationState deletes the notification state key in Redis, allowing a subsequent
// producer to trigger a new notification. This is part of the "Intelligent Notification Coalescing" pattern.
func (c *Consumer) resetNotificationState(ctx context.Context, p types.PartitionInfo) {
	stateKey := fmt.Sprintf("mq_notify_state:%s:%d", p.Topic, p.Partition)
	if err := c.redis.Del(ctx, stateKey).Err(); err != nil {
		log.Printf("ERROR: failed to reset notification state for partition %v: %v", p, err)
	} else {
		// log.Printf("Notification state reset for partition %v", p)
	}
}
