package dbmq

import (
	"context"
	"dbmq/internal/dal"
	"dbmq/pkg/dbmq/types"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

const (
	leaderLockName      = "mq_coordinator_leader_lock"
	lockRefreshInterval = 5 * time.Second // Should be less than DB session timeout
	cleanupBatchSize    = 1000            // Number of messages to delete in one batch
	cleanupBatchSleep   = 100 * time.Millisecond
)

// groupLocks provides a mechanism to lock and unlock based on a group ID.
type groupLocks struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

func newGroupLocks() *groupLocks {
	return &groupLocks{
		locks: make(map[string]struct{}),
	}
}

// TryLock attempts to acquire a lock for the given group ID.
// It returns true if the lock was acquired, and false otherwise.
func (gl *groupLocks) TryLock(groupID string) bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if _, ok := gl.locks[groupID]; ok {
		return false // Lock is already held
	}
	gl.locks[groupID] = struct{}{}
	return true
}

// Unlock releases the lock for the given group ID.
func (gl *groupLocks) Unlock(groupID string) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	delete(gl.locks, groupID)
}

// CoordinatorConfig holds the configuration for the coordinator.
type CoordinatorConfig struct {
	DB                     *gorm.DB
	HeartbeatTimeout       time.Duration // How long until a consumer is considered dead
	RebalanceInterval      time.Duration // How often to check for rebalances
	RebalanceTimeout       time.Duration // Context timeout for a rebalance operation
	RetentionCheckInterval time.Duration // How often to run the message retention cleanup.
	DefaultRetentionAge    time.Duration // Default retention for topics without a specific policy.
}

// Coordinator manages a single consumer group and its rebalancing.
// It also takes on the global responsibility of message retention cleanup when it is the leader.
type Coordinator struct {
	config           CoordinatorConfig
	db               *gorm.DB
	isLeader         atomic.Bool
	rebalancingLocks *groupLocks // Per-group rebalance lock
	stopCh           chan struct{}
	wg               sync.WaitGroup
	mu               sync.Mutex                     // Protects members map
	members          map[string]map[string]struct{} // groupID -> set of consumer IDs
}

// NewCoordinator creates a new coordinator for a specific consumer group.
// The coordinator with the global leader lock will also perform system-wide tasks like message cleanup.
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	if config.RebalanceInterval == 0 {
		config.RebalanceInterval = 10 * time.Second
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = 30 * time.Second
	}
	if config.DefaultRetentionAge == 0 {
		// Default to 7 days
		config.DefaultRetentionAge = 7 * 24 * time.Hour
	}
	if config.RetentionCheckInterval == 0 {
		// Default to once per hour
		config.RetentionCheckInterval = 1 * time.Hour
	}
	return &Coordinator{
		config:           config,
		db:               config.DB,
		stopCh:           make(chan struct{}),
		members:          make(map[string]map[string]struct{}),
		rebalancingLocks: newGroupLocks(),
	}
}

// Start begins the coordinator's work, including leader election.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.leaderElectionLoop()
}

// Stop gracefully shuts down the coordinator.
func (c *Coordinator) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// IsLeader returns true if this coordinator instance is the current leader.
func (c *Coordinator) IsLeader() bool {
	return c.isLeader.Load()
}

func (c *Coordinator) setLeader(isLeader bool) {
	wasLeader := c.isLeader.Swap(isLeader)
	if isLeader && !wasLeader {
		log.Printf("Coordinator became the global leader.")
		// When we become leader, start the main work loop
		c.wg.Add(1)
		go c.leaderLoop()
	}
	if !isLeader && wasLeader {
		log.Printf("Coordinator lost global leadership.")
	}
}

func (c *Coordinator) leaderElectionLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(lockRefreshInterval)
	defer ticker.Stop()

	// Initial attempt
	c.attemptToBecomeLeader()

	for {
		select {
		case <-c.stopCh:
			if c.IsLeader() {
				c.releaseLock()
			}
			return
		case <-ticker.C:
			c.attemptToBecomeLeader()
		}
	}
}

func (c *Coordinator) attemptToBecomeLeader() {
	var result int
	// GET_LOCK is session-specific. A result of 1 means we got the lock.
	// 0 means another session holds it. NULL means an error occurred.
	// The timeout of 0 means we don't wait for the lock.
	err := c.db.Raw("SELECT GET_LOCK(?, 0)", leaderLockName).Scan(&result).Error
	if err != nil {
		log.Printf("Error in leader election: %v", err)
		if c.IsLeader() {
			c.setLeader(false)
		}
		return
	}
	if result == 0 {
		// We were leader but lost the lock (e.g., DB connection dropped and re-established).
		// Another coordinator has likely taken over.
		log.Printf("Coordinator failed to renew lock.")
		c.setLeader(false)
	} else if result == 1 {
		// We are the leader.
		if !c.IsLeader() {
			c.setLeader(true)
		}
		// If we were already leader, this just refreshes the session activity.
	}
	// If result == 0 and !c.IsLeader(), we are a follower, nothing to do.
}

func (c *Coordinator) releaseLock() {
	// RELEASE_LOCK releases the lock. It's good practice to do this on graceful shutdown.
	if err := c.db.Exec("SELECT RELEASE_LOCK(?)", leaderLockName).Error; err != nil {
		log.Printf("Error releasing global leader lock: %v", err)
	} else {
		log.Printf("Coordinator released global leader lock.")
	}
}

func (c *Coordinator) leaderLoop() {
	defer c.wg.Done()
	log.Printf("Coordinator leader loop started.")
	rebalanceTicker := time.NewTicker(c.config.RebalanceInterval)
	defer rebalanceTicker.Stop()
	cleanupTicker := time.NewTicker(c.config.RetentionCheckInterval)
	defer cleanupTicker.Stop()

	// Run once immediately on startup
	c.scanAndRebalanceAllGroups()
	c.runRetentionCleanup()

	for {
		// If we are no longer the leader, the loop should stop.
		if !c.IsLeader() {
			log.Printf("No longer leader, stopping leader loop.")
			return
		}

		select {
		case <-c.stopCh:
			log.Printf("Coordinator stopping leader loop.")
			return
		case <-rebalanceTicker.C:
			log.Printf("Leader coordinator starting global rebalance scan...")
			c.scanAndRebalanceAllGroups()
		case <-cleanupTicker.C:
			log.Printf("Leader coordinator starting message retention cleanup...")
			c.runRetentionCleanup()
		}
	}
}

// lockName returns the global lock name.
// NOTE: This implementation has been changed to a single global lock
// to align with the design document's goal of a central coordinator leader.
func (c *Coordinator) lockName() string {
	return leaderLockName
}

func (c *Coordinator) runRetentionCleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // Generous timeout for cleanup
	defer cancel()

	log.Println("Starting message retention cleanup cycle.")
	startTime := time.Now()

	// 1. Get all topics to understand their retention policies
	allTopics, err := dal.GetAllTopics(ctx, c.db)
	if err != nil {
		log.Printf("ERROR: Cleanup failed to get topics: %v", err)
		return
	}
	topicConfigMap := make(map[string]time.Duration)
	for _, topic := range allTopics {
		retentionMs, _ := topic.GetConfig("retention_ms")
		if retentionMs > 0 {
			topicConfigMap[topic.TopicName] = time.Duration(retentionMs) * time.Millisecond
		}
	}

	// 2. Calculate global low watermark for all consumed partitions
	watermarks, err := dal.GetConsumerGroupLowWatermarks(ctx, c.db)
	if err != nil {
		log.Printf("ERROR: Cleanup failed to get low watermarks: %v", err)
		return
	}
	log.Printf("Found %d consumed partitions with a low watermark.", len(watermarks))

	var totalDeletedCount int64

	// 3. Iterate through all partitions of all topics and apply deletion logic
	for _, topic := range allTopics {
		retentionAge := c.config.DefaultRetentionAge
		if configuredAge, ok := topicConfigMap[topic.TopicName]; ok {
			retentionAge = configuredAge
		}
		retentionDate := time.Now().Add(-retentionAge)

		for i := uint(0); i < topic.PartitionCount; i++ {
			p := types.PartitionInfo{Topic: topic.TopicName, Partition: i}
			partitionTotalDeleted := int64(0)

			// Loop to delete in batches until no more rows are affected
			for {
				var deletedCount int64
				var err error

				if lowWatermark, ok := watermarks[p]; ok {
					// This partition is consumed, so use the low watermark
					deletedCount, err = dal.DeleteMessagesByPartition(ctx, c.db, p.Topic, p.Partition, lowWatermark, retentionDate, cleanupBatchSize)
				} else {
					// This partition is not in the watermark map, meaning no group has ever committed an offset for it.
					// We can only clean it up based on time.
					deletedCount, err = dal.DeleteMessagesByPartitionUnconsumed(ctx, c.db, p.Topic, p.Partition, retentionDate, cleanupBatchSize)
				}

				if err != nil {
					log.Printf("ERROR: Failed to clean partition %v: %v", p, err)
					break // Break from batch loop on error
				}

				if deletedCount > 0 {
					partitionTotalDeleted += deletedCount
				}

				// If we deleted fewer rows than the batch size, we are done with this partition.
				if deletedCount < cleanupBatchSize {
					break
				}

				// Sleep briefly to avoid overwhelming the DB
				time.Sleep(cleanupBatchSleep)
			}

			if partitionTotalDeleted > 0 {
				totalDeletedCount += partitionTotalDeleted
				log.Printf("Cleaned up %d messages from partition %v", partitionTotalDeleted, p)
			}
		}
	}

	log.Printf("Finished message retention cleanup cycle in %v. Total messages deleted: %d", time.Since(startTime), totalDeletedCount)
}

// scanAndRebalanceAllGroups is the new top-level function for the global leader.
// It finds all active groups and triggers a rebalance check for each one.
func (c *Coordinator) scanAndRebalanceAllGroups() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	activeGroups, err := dal.FindAllActiveGroups(ctx, c.db, c.config.HeartbeatTimeout)
	if err != nil {
		log.Printf("ERROR: Failed to scan for active groups: %v", err)
		return
	}

	if len(activeGroups) > 0 {
		log.Printf("Found active consumer groups: %v", activeGroups)
		for _, groupID := range activeGroups {
			if err := c.rebalanceIfNeeded(groupID); err != nil {
				log.Printf("ERROR: Rebalance failed for group '%s': %v", groupID, err)
			}
		}
	}
}

// rebalanceIfNeeded contains the main rebalance logic for a specific group.
func (c *Coordinator) rebalanceIfNeeded(groupID string) error {
	// Attempt to set the rebalancing flag. If it's already set, another loop is running.
	if !c.rebalancingLocks.TryLock(groupID) {
		log.Printf("Rebalance check for group '%s' skipped: another rebalance is already in progress.", groupID)
		return nil
	}
	defer c.rebalancingLocks.Unlock(groupID)

	timeout := 15 * time.Second
	if c.config.RebalanceTimeout > 0 {
		timeout = c.config.RebalanceTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 1. Find all active consumers for this group
	activeConsumers, err := dal.FindActiveConsumers(ctx, c.db, groupID, c.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("failed to find active consumers: %w", err)
	}

	activeConsumerIDs := make(map[string]struct{}, len(activeConsumers))
	for _, consumer := range activeConsumers {
		activeConsumerIDs[consumer.ConsumerID] = struct{}{}
	}

	// 2. Check if a rebalance is needed by comparing the current active set of
	// consumers with the set from the last successful rebalance.
	if !c.isRebalanceNeeded(groupID, activeConsumerIDs) {
		return nil // No changes, no rebalance needed
	}

	log.Printf("Rebalance needed for group '%s'. Old members: %v, New members: %v", groupID, c.getMemberIDs(groupID), activeConsumerIDs)

	// 3. --- START REBALANCE PROTOCOL ---
	// Increment generation ID. This fences off old consumers.
	newGenerationID, err := dal.IncrementAndGetGenerationID(ctx, c.db, groupID)
	if err != nil {
		return fmt.Errorf("failed to increment generation id: %w", err)
	}

	// If there are no active consumers, we just incremented the generation and can stop.
	if len(activeConsumers) == 0 {
		log.Printf("No active consumers for group '%s'. Rebalance to generation %d complete.", groupID, newGenerationID)
		c.updateMembers(groupID, activeConsumerIDs)
		return nil
	}

	// 4. Gather all subscribed topics and their partitions
	allPartitions, err := c.getAllPartitionsForConsumers(ctx, activeConsumers)
	if err != nil {
		return fmt.Errorf("failed to get partitions for consumers: %w", err)
	}

	// 5. Calculate new assignments
	newAssignments := c.calculateAssignments(activeConsumers, allPartitions)

	// 6. Persist new assignments in a transaction
	assignmentsForDAL := make(map[string][]byte)
	for consumerID, parts := range newAssignments {
		jsonBytes, err := json.Marshal(parts)
		if err != nil {
			return fmt.Errorf("failed to marshal assignment for consumer %s: %w", consumerID, err)
		}
		assignmentsForDAL[consumerID] = jsonBytes
	}

	err = c.db.Transaction(func(tx *gorm.DB) error {
		return dal.UpdateAssignmentsInTx(ctx, tx, groupID, newGenerationID, assignmentsForDAL)
	})
	if err != nil {
		return fmt.Errorf("failed to update assignments: %w", err)
	}

	// 7. Update internal state
	c.updateMembers(groupID, activeConsumerIDs)
	log.Printf("Rebalance for group '%s' to generation %d completed successfully.", groupID, newGenerationID)
	return nil
}

func (c *Coordinator) isRebalanceNeeded(groupID string, activeConsumerIDs map[string]struct{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure the map for the group exists before accessing it.
	if _, ok := c.members[groupID]; !ok {
		if len(activeConsumerIDs) > 0 {
			return true // Group was empty or new, but now has members.
		}
		return false // Group was and is empty.
	}

	if len(activeConsumerIDs) != len(c.members[groupID]) {
		return true // Number of members changed
	}

	for id := range activeConsumerIDs {
		if _, exists := c.members[groupID][id]; !exists {
			return true // New member joined
		}
	}

	return false // Sets are identical
}

func (c *Coordinator) updateMembers(groupID string, newMembers map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.members[groupID] = newMembers
}

func (c *Coordinator) getMemberIDs(groupID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.members[groupID]))
	for id := range c.members[groupID] {
		ids = append(ids, id)
	}
	return ids
}

// getAllPartitionsForConsumers collects all unique topics subscribed to by the active consumers
// and returns a list of all partitions for those topics.
func (c *Coordinator) getAllPartitionsForConsumers(ctx context.Context, consumers []types.ConsumerHeartbeat) ([]types.PartitionInfo, error) {
	subscribedTopics := make(map[string]struct{})
	for _, consumer := range consumers {
		var topics []string
		if err := json.Unmarshal(consumer.SubscribedTopics, &topics); err != nil {
			// This is a critical error. If we cannot parse a consumer's subscriptions,
			// we cannot perform a safe rebalance. Abort this cycle.
			return nil, fmt.Errorf("could not unmarshal subscribed topics for consumer %s: %w", consumer.ConsumerID, err)
		}
		for _, topic := range topics {
			subscribedTopics[topic] = struct{}{}
		}
	}

	topicNames := make([]string, 0, len(subscribedTopics))
	for topic := range subscribedTopics {
		topicNames = append(topicNames, topic)
	}

	dbTopics, err := dal.FindTopicsByNames(ctx, c.db, topicNames)
	if err != nil {
		return nil, fmt.Errorf("failed to find topics by name: %w", err)
	}

	var allPartitions []types.PartitionInfo
	for _, topic := range dbTopics {
		for i := uint(0); i < topic.PartitionCount; i++ {
			allPartitions = append(allPartitions, types.PartitionInfo{Topic: topic.TopicName, Partition: i})
		}
	}
	return allPartitions, nil
}

// calculateAssignments distributes partitions among consumers using a stable round-robin strategy.
// It sorts consumers and partitions to ensure that the assignment is deterministic and minimizes
// churn when consumers are added or removed.
func (c *Coordinator) calculateAssignments(consumers []types.ConsumerHeartbeat, partitions []types.PartitionInfo) map[string][]types.PartitionInfo {
	assignments := make(map[string][]types.PartitionInfo)
	if len(consumers) == 0 {
		return assignments
	}

	// --- CRITICAL: Sort for deterministic assignment ---
	// Sort consumers by their ID
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})

	// Sort partitions by topic then by partition number
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
	// --- End of sorting ---

	consumerIDs := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		consumerIDs = append(consumerIDs, consumer.ConsumerID)
		assignments[consumer.ConsumerID] = []types.PartitionInfo{}
	}

	// Round-robin assignment
	for i, p := range partitions {
		consumerID := consumerIDs[i%len(consumerIDs)]
		assignments[consumerID] = append(assignments[consumerID], p)
	}

	return assignments
}
