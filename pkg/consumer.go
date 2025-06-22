package pkg

import (
	"context"
	"dbmq/internal/dal"
	dberrors "dbmq/pkg/errors"
	"dbmq/pkg/types"
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
	rebalancing      atomic.Bool // True if a rebalance is currently in progress
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

	// Start the heartbeat loop on subscribe. The loop itself will handle
	// registration and all subsequent state reconciliation.
	if c.heartbeatStarted.CompareAndSwap(false, true) {
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
	// If a rebalance is in progress, signal to the user and return immediately.
	// The heartbeat loop is responsible for handling the rebalance process.
	if c.rebalancing.Load() {
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

	return toConsumerMessages(allMessages), lastErr
}

// heartbeatLoop is the core background process for the consumer. It is responsible for:
// 1. Periodically sending heartbeats to the coordinator.
// 2. Fetching its latest assignment and generation ID.
// 3. Triggering and executing the rebalance protocol if a change is detected.
// This decouples the network I/O of state management from the main Poll() loop.
func (c *Consumer) heartbeatLoop() {
	defer c.wg.Done()

	// Perform an initial state reconciliation before starting the ticker.
	c.reconcileState(context.Background())

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// The context here should be short-lived for this specific task.
			ctx, cancel := context.WithTimeout(context.Background(), c.config.HeartbeatInterval)
			c.reconcileState(ctx)
			cancel()
		case <-c.stopCh:
			return
		}
	}
}

// reconcileState performs a single cycle of sending a heartbeat, fetching the
// consumer's current state, and handling a rebalance if necessary.
func (c *Consumer) reconcileState(ctx context.Context) {
	// Register/update heartbeat first.
	if err := c.register(ctx); err != nil {
		log.Printf("ERROR: failed to send heartbeat for consumer %s: %v", c.id, err)
		return // Don't proceed if we can't even heartbeat.
	}

	// Fetch our own state back from the database.
	hb, err := dal.GetHeartbeat(ctx, c.db, c.config.GroupID, c.id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// This can happen in rare race conditions where we are kicked out
			// between our heartbeat and fetch. The next heartbeat will re-register.
			log.Printf("WARN: could not find our own heartbeat for consumer %s, will retry.", c.id)
		} else {
			log.Printf("ERROR: failed to fetch consumer state for %s: %v", c.id, err)
		}
		return
	}

	c.mu.RLock()
	currentGenID := c.generationID
	c.mu.RUnlock()

	// Check if a rebalance is needed.
	if hb.GenerationID == currentGenID {
		return // No change, nothing to do.
	}

	// ---- REBALANCE REQUIRED ----
	log.Printf("Rebalance detected for consumer %s. New Generation ID: %d", c.id, hb.GenerationID)
	c.rebalancing.Store(true)
	defer c.rebalancing.Store(false)

	log.Printf("Consumer %s: received assignment data: %s", c.id, string(hb.AssignedPartitions))
	newPartitions, err := c.parsePartitions(hb.AssignedPartitions)
	if err != nil {
		log.Printf("ERROR: failed to parse new partition assignment for consumer %s: %v", c.id, err)
		return
	}
	log.Printf("Consumer %s: parsed new partitions: %v", c.id, newPartitions)

	// 1. Find which partitions were revoked.
	revokedPartitions := c.findRevokedPartitions(newPartitions)

	// 2. Commit offsets for revoked partitions to ensure no work is lost.
	// Use the new generation ID for the commit to prevent stale commits.
	if len(revokedPartitions) > 0 {
		log.Printf("Consumer %s revoking partitions: %v", c.id, revokedPartitions)
		if err := c.commitOffsets(ctx, revokedPartitions, hb.GenerationID); err != nil {
			log.Printf("ERROR: failed to commit offsets for revoked partitions on consumer %s: %v", c.id, err)
			// Continue with rebalance even if commit fails.
		}
	}

	// 3. Clear internal state and fetch offsets for the new assignment.
	log.Printf("Consumer %s: clearing and fetching offsets for new assignment", c.id)
	if err := c.clearAndFetchOffsetsForNewAssignment(newPartitions); err != nil {
		log.Printf("ERROR: failed to fetch offsets for new assignment on consumer %s: %v", c.id, err)
		return // This is a fatal error for the rebalance.
	}
	log.Printf("Consumer %s: successfully fetched offsets for new assignment", c.id)

	// 4. Update Redis subscriptions if enabled.
	if c.config.NotificationEnabled && c.redis != nil {
		// This can be done concurrently, but for simplicity, we do it inline.
		// A more advanced implementation could manage this more smoothly.
		var oldPartitionsList []types.PartitionInfo
		c.mu.RLock()
		for topic, partitions := range c.assignment {
			for _, pID := range partitions {
				oldPartitionsList = append(oldPartitionsList, types.PartitionInfo{Topic: topic, Partition: pID})
			}
		}
		c.mu.RUnlock()

		var newPartitionsList []types.PartitionInfo
		for topic, parts := range newPartitions {
			for _, p := range parts {
				newPartitionsList = append(newPartitionsList, types.PartitionInfo{Topic: topic, Partition: p})
			}
		}

		c.unsubscribeFromChannels(oldPartitionsList)
		c.subscribeToChannels(newPartitionsList)
	}

	// 5. Atomically update the consumer's state.
	c.mu.Lock()
	c.generationID = hb.GenerationID
	c.assignment = newPartitions
	c.mu.Unlock()

	log.Printf("Rebalance completed for consumer %s. New assignment: %v", c.id, newPartitions)
}

// register sends a heartbeat to the coordinator, effectively registering or
// updating the consumer's liveness and topic subscription information.
func (c *Consumer) register(ctx context.Context) error {
	c.mu.RLock()
	topicsData, err := json.Marshal(c.topics)
	c.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal subscribed topics: %w", err)
	}

	// Heartbeat also serves as registration. It's an upsert operation.
	return dal.UpsertHeartbeat(ctx, c.db, c.config.GroupID, c.id, topicsData)
}

// IsReady returns true if the consumer is not rebalancing and has assigned partitions.
func (c *Consumer) IsReady() bool {
	if c.rebalancing.Load() {
		return false
	}
	assignedPartitions := c.getAssignedPartitions()
	return len(assignedPartitions) > 0
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

	// Get the current assignment and generation ID
	partitions := c.getAssignedPartitionsLocked()
	generationID := c.generationID

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Call commitOffsetsWithoutLock without the lock since we already have the data we need
	err := c.commitOffsetsWithoutLock(ctx, partitions, generationID)

	return err
}

// commitOffsetsWithoutLock handles the database logic for committing a batch of offsets.
// This version doesn't acquire any locks and is used internally.
func (c *Consumer) commitOffsetsWithoutLock(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	for _, p := range partitions {
		// Commit the committed offset (which includes merged polled offsets)
		if offset, ok := c.committedOffsets[p]; ok {
			offsetsToCommit[p] = offset
		}
	}

	if len(offsetsToCommit) == 0 {
		return nil
	}
	return dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsetsToCommit)
}

// commitOffsets handles the database logic for committing a batch of offsets.
// This is the legacy method that acquires locks internally.
func (c *Consumer) commitOffsets(ctx context.Context, partitions []types.PartitionInfo, generationID uint) error {
	offsetsToCommit := make(map[types.PartitionInfo]int64)
	c.mu.RLock()
	for _, p := range partitions {
		// Commit the committed offset (which includes merged polled offsets)
		if offset, ok := c.committedOffsets[p]; ok {
			offsetsToCommit[p] = offset
		}
	}
	c.mu.RUnlock()

	if len(offsetsToCommit) == 0 {
		return nil
	}
	return dal.BatchCommitOffsets(ctx, c.db, c.config.GroupID, generationID, offsetsToCommit)
}

// getAssignedPartitionsLocked returns assigned partitions without acquiring locks.
// This should only be called when the caller already holds the appropriate lock.
func (c *Consumer) getAssignedPartitionsLocked() []types.PartitionInfo {
	partitions := make([]types.PartitionInfo, 0)
	for topic, parts := range c.assignment {
		for _, pNum := range parts {
			partitions = append(partitions, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}
	return partitions
}

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

	if c.pubsub != nil {
		c.pubsub.Close()
		c.pubsub = nil
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

// clearAndFetchOffsetsForNewAssignment clears old state and fetches committed offsets for a new assignment.
// It must be called after the new assignment has been set on the consumer.
func (c *Consumer) clearAndFetchOffsetsForNewAssignment(newAssignment map[string][]uint) error {
	// Build the list of partitions to fetch before acquiring any locks
	var partitionsToFetch []types.PartitionInfo
	for topic, parts := range newAssignment {
		for _, pNum := range parts {
			partitionsToFetch = append(partitionsToFetch, types.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}

	c.mu.Lock()
	c.polledOffsets = make(map[types.PartitionInfo]int64)
	c.committedOffsets = make(map[types.PartitionInfo]int64)
	c.assignment = newAssignment
	c.mu.Unlock()

	if len(partitionsToFetch) == 0 {
		return nil
	}

	// This now happens in the background, so use a background context.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("Consumer %s: fetching offsets for partitions: %v", c.id, partitionsToFetch)
	fetchedOffsets, err := dal.GetCommittedOffsets(ctx, c.db, c.config.GroupID, partitionsToFetch)
	if err != nil {
		log.Printf("ERROR: Consumer %s: dal.GetCommittedOffsets failed: %v", c.id, err)
		return fmt.Errorf("dal.GetCommittedOffsets failed: %w", err)
	}
	log.Printf("Consumer %s: fetched offsets: %v", c.id, fetchedOffsets)

	c.mu.Lock()
	c.committedOffsets = fetchedOffsets
	c.mu.Unlock()

	return nil
}

// findRevokedPartitions calculates which partitions are present in the old assignment
// but not in the new one.
func (c *Consumer) findRevokedPartitions(newPartitions map[string][]uint) []types.PartitionInfo {
	oldSet := make(map[types.PartitionInfo]struct{})
	c.mu.RLock()
	for topic, partitions := range c.assignment {
		for _, pID := range partitions {
			oldSet[types.PartitionInfo{Topic: topic, Partition: pID}] = struct{}{}
		}
	}
	c.mu.RUnlock()

	newSet := make(map[types.PartitionInfo]struct{})
	for topic, partitions := range newPartitions {
		for _, pID := range partitions {
			newSet[types.PartitionInfo{Topic: topic, Partition: pID}] = struct{}{}
		}
	}

	var revoked []types.PartitionInfo
	for p := range oldSet {
		if _, ok := newSet[p]; !ok {
			revoked = append(revoked, p)
		}
	}
	return revoked
}

// parsePartitions decodes the JSON partition assignment data.
func (c *Consumer) parsePartitions(data []byte) (map[string][]uint, error) {
	var partitions []types.PartitionInfo
	if err := json.Unmarshal(data, &partitions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition assignment: %w", err)
	}
	var m = make(map[string][]uint)
	for _, partition := range partitions {
		if _, ok := m[partition.Topic]; !ok {
			m[partition.Topic] = []uint{partition.Partition}
		} else {
			m[partition.Topic] = append(m[partition.Topic], partition.Partition)
		}
	}
	return m, nil
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
	key := fmt.Sprintf("mq_notify_state:%s:%d", p.Topic, p.Partition)
	if err := c.redis.Del(ctx, key).Err(); err != nil {
		log.Printf("WARN: failed to reset notification state for %v: %v", p, err)
	}
}
