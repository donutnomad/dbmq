package dbmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/repo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========== Mock ConsumerRepo Implementation ==========

// heartbeatCall records a call to UpsertConsumerHeartbeat
type heartbeatCall struct {
	groupID    string
	consumerID string
	topics     []string
}

// getHeartbeatCall records a call to GetConsumerHeartbeat
type getHeartbeatCall struct {
	groupID    string
	consumerID string
}

// markOfflineCall records a call to MarkConsumerOffline
type markOfflineCall struct {
	groupID    string
	consumerID string
}

// getCommittedOffsetsCall records a call to GetCommittedOffsets
type getCommittedOffsetsCall struct {
	groupID    string
	partitions []db.PartitionInfo
}

// batchCommitCall records a call to BatchCommitLastConsumeMessageID
type batchCommitCall struct {
	groupID      string
	generationID uint
	consumedIds  map[db.PartitionInfo]int64
}

// fetchMessagesCall records a call to FetchMessagesBatch
type fetchMessagesCall struct {
	requests []repo.PartitionRequest
}

// mockConsumerRepo is a mock implementation of ConsumerRepo for testing
type mockConsumerRepo struct {
	mu sync.Mutex

	// Call records
	heartbeatCalls           []heartbeatCall
	getHeartbeatCalls        []getHeartbeatCall
	markOfflineCalls         []markOfflineCall
	getCommittedOffsetsCalls []getCommittedOffsetsCall
	batchCommitCalls         []batchCommitCall
	fetchMessagesCalls       []fetchMessagesCall

	// Return value controls
	upsertHeartbeatErr error
	heartbeat          *db.ConsumerHeartbeat
	getHeartbeatErr    error
	markOfflineErr     error

	committedOffsets    db.ConsumerGroupConsumptionProgressSlice
	getCommittedErr     error
	batchCommitErr      error
	batchCommitWaterErr error

	latestIDs        map[db.PartitionInfo]int64
	latestIDsErr     error
	fetchedMessages  []db.Message
	fetchMessagesErr error
}

func newMockConsumerRepo() *mockConsumerRepo {
	return &mockConsumerRepo{
		latestIDs: make(map[db.PartitionInfo]int64),
	}
}

func (m *mockConsumerRepo) UpsertConsumerHeartbeat(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCalls = append(m.heartbeatCalls, heartbeatCall{
		groupID:    groupID,
		consumerID: consumerID,
		topics:     subscribedTopics,
	})
	return m.upsertHeartbeatErr
}

func (m *mockConsumerRepo) GetConsumerHeartbeat(ctx context.Context, groupID, consumerID string) (*db.ConsumerHeartbeat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getHeartbeatCalls = append(m.getHeartbeatCalls, getHeartbeatCall{
		groupID:    groupID,
		consumerID: consumerID,
	})
	return m.heartbeat, m.getHeartbeatErr
}

func (m *mockConsumerRepo) MarkConsumerOffline(ctx context.Context, groupID, consumerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markOfflineCalls = append(m.markOfflineCalls, markOfflineCall{
		groupID:    groupID,
		consumerID: consumerID,
	})
	return m.markOfflineErr
}

func (m *mockConsumerRepo) GetCommittedOffsets(ctx context.Context, groupID string, partitions []db.PartitionInfo) (db.ConsumerGroupConsumptionProgressSlice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCommittedOffsetsCalls = append(m.getCommittedOffsetsCalls, getCommittedOffsetsCall{
		groupID:    groupID,
		partitions: partitions,
	})
	return m.committedOffsets, m.getCommittedErr
}

func (m *mockConsumerRepo) BatchCommitOffsetsWithInitialWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[db.PartitionInfo]repo.ConsumptionProgressWithWatermark) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batchCommitWaterErr
}

func (m *mockConsumerRepo) BatchCommitLastConsumeMessageID(ctx context.Context, groupID string, generationID uint, consumedIds map[db.PartitionInfo]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchCommitCalls = append(m.batchCommitCalls, batchCommitCall{
		groupID:      groupID,
		generationID: generationID,
		consumedIds:  consumedIds,
	})
	return m.batchCommitErr
}

func (m *mockConsumerRepo) GetTopicsLatestIDsByPartitions(ctx context.Context, topicPartitions []db.PartitionInfo) (map[db.PartitionInfo]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestIDs, m.latestIDsErr
}

func (m *mockConsumerRepo) FetchMessagesBatch(ctx context.Context, requests []repo.PartitionRequest) ([]db.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchMessagesCalls = append(m.fetchMessagesCalls, fetchMessagesCall{
		requests: requests,
	})
	return m.fetchedMessages, m.fetchMessagesErr
}

// Helper methods for testing
func (m *mockConsumerRepo) getHeartbeatCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.heartbeatCalls)
}

func (m *mockConsumerRepo) getMarkOfflineCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.markOfflineCalls)
}

func (m *mockConsumerRepo) setHeartbeat(hb *db.ConsumerHeartbeat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeat = hb
}

// Compile-time check that mockConsumerRepo implements ConsumerRepo
var _ ConsumerRepo = (*mockConsumerRepo)(nil)

// ========== Test Cases ==========

func TestConsumerActor_NewConsumerActor(t *testing.T) {
	t.Run("creates actor with default values", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo))

		require.NotNil(t, actor)
		assert.NotEmpty(t, actor.ID())
		// Check internal state directly since State() requires actor to be started
		assert.Equal(t, StateUninitialized, actor.stateMachine.Get())
	})

	t.Run("creates actor with custom clock and notifier", func(t *testing.T) {
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()
		mockRepo := newMockConsumerRepo()

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock), WithNotifier(fakeNotifier))

		require.NotNil(t, actor)
		// Check internal state directly since State() requires actor to be started
		assert.Equal(t, StateUninitialized, actor.stateMachine.Get())
	})

	t.Run("sets default auto commit interval when enabled", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID:          "test-group",
			EnableAutoCommit: true,
		}, WithRepo(mockRepo))

		require.NotNil(t, actor)
		assert.Equal(t, 5*time.Second, actor.config.AutoCommitInterval)
	})

	t.Run("preserves custom auto commit interval", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 10 * time.Second,
		}, WithRepo(mockRepo))

		require.NotNil(t, actor)
		assert.Equal(t, 10*time.Second, actor.config.AutoCommitInterval)
	})
}

func TestConsumerActor_Start_Stop(t *testing.T) {
	t.Run("start and stop without subscription", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		assert.Equal(t, StateUninitialized, actor.State())

		actor.Stop()
		// After stop, actor should no longer respond
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()

		// Multiple stops should not panic
		actor.Stop()
		actor.Stop()
		actor.Stop()
	})
}

func TestConsumerActor_SubscribeTopics(t *testing.T) {
	t.Run("subscribing transitions state to Joining", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock to return nil heartbeat initially
		mockRepo.heartbeat = nil

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.Equal(t, StateUninitialized, actor.State())

		actor.SubscribeTopics("test-topic")

		// State should be Joining after subscription
		assert.Equal(t, StateJoining, actor.State())
	})

	t.Run("subscribing multiple times does not change state", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.SubscribeTopics("test-topic-1")
		assert.Equal(t, StateJoining, actor.State())

		// Second subscription should not restart heartbeat loop
		actor.SubscribeTopics("test-topic-2")
		assert.Equal(t, StateJoining, actor.State())
	})
}

func TestConsumerActor_State(t *testing.T) {
	t.Run("returns current state", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.Equal(t, StateUninitialized, actor.State())

		actor.SubscribeTopics("test-topic")
		assert.Equal(t, StateJoining, actor.State())
	})

	t.Run("returns Stopped after stop", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		actor.Stop()

		// After stop, State() should return StateStopped
		assert.Equal(t, StateStopped, actor.State())
	})
}

func TestConsumerActor_Heartbeat_StateTransition(t *testing.T) {
	t.Run("heartbeat triggers state transition to Ready", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()

		// Setup mock to return heartbeat with assignment
		mockRepo.heartbeat = &db.ConsumerHeartbeat{
			GenerationID: 1,
			AssignedPartitions: []db.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{
			{
				Topic:                 "test-topic",
				Partition:             0,
				LastConsumedMessageID: 100,
			},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock), WithNotifier(fakeNotifier))

		actor.Start()
		defer actor.Stop()

		// Subscribe to start heartbeat loop
		actor.SubscribeTopics("test-topic")

		// After subscription, state should be Joining or Ready (if heartbeat already processed)
		initialState := actor.State()
		assert.True(t, initialState == StateJoining || initialState == StateReady,
			"Initial state should be Joining or Ready, got %s", initialState)

		// Advance clock to trigger heartbeat
		fakeClock.Advance(2 * time.Second)

		// Wait a bit for the heartbeat to process
		time.Sleep(50 * time.Millisecond)

		// State should transition based on heartbeat response
		// Since GenerationID changed from 0 to 1, rebalance should occur
		state := actor.State()
		assert.True(t, state == StateReady || state == StateJoining,
			"State should be Ready or Joining, got %s", state)
	})

	t.Run("heartbeat error does not crash actor", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock to return error on heartbeat
		mockRepo.upsertHeartbeatErr = errors.New("database connection lost")

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.SubscribeTopics("test-topic")

		// Advance clock to trigger heartbeat
		fakeClock.Advance(2 * time.Second)
		time.Sleep(50 * time.Millisecond)

		// Actor should still be running, state should still be Joining
		assert.Equal(t, StateJoining, actor.State())
	})
}

func TestConsumerActor_Poll_NotReady(t *testing.T) {
	t.Run("Poll returns error when state is Uninitialized", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		messages, err := actor.Poll(ctx, 50*time.Millisecond)
		require.Error(t, err)
		assert.Nil(t, messages)

		var rebalanceErr *ErrRebalanceInProgress
		assert.True(t, errors.As(err, &rebalanceErr))
	})

	t.Run("Poll returns error when state is Joining", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.SubscribeTopics("test-topic")
		assert.Equal(t, StateJoining, actor.State())

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		messages, err := actor.Poll(ctx, 50*time.Millisecond)
		require.Error(t, err)
		assert.Nil(t, messages)

		var rebalanceErr *ErrRebalanceInProgress
		assert.True(t, errors.As(err, &rebalanceErr))
	})

	t.Run("Poll returns error when state is Rebalancing", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Manually set state to Rebalancing for testing
		actor.stateMachine.forceSetState(StateRebalancing)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		messages, err := actor.Poll(ctx, 50*time.Millisecond)
		require.Error(t, err)
		assert.Nil(t, messages)

		var rebalanceErr *ErrRebalanceInProgress
		assert.True(t, errors.As(err, &rebalanceErr))
	})

	t.Run("Poll returns error when context is cancelled", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		messages, err := actor.Poll(ctx, 50*time.Millisecond)
		require.Error(t, err)
		assert.Nil(t, messages)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

func TestConsumerActor_Poll_Ready(t *testing.T) {
	t.Run("Poll returns empty when no messages", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()
		fakeNotifier.SetHealthy(false) // Disable notifier to avoid blocking

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock), WithNotifier(fakeNotifier))

		actor.Start()
		defer actor.Stop()

		// Manually set up Ready state with assignment
		actor.stateMachine.forceSetState(StateReady)

		actor.assignment = []db.PartitionInfo{
			{Topic: "test-topic", Partition: 0},
		}
		actor.generationID = 1

		// Setup mock to return empty messages
		mockRepo.fetchedMessages = []db.Message{}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// Advance clock to trigger timeout in waitForNotification
		go func() {
			time.Sleep(10 * time.Millisecond)
			fakeClock.Advance(100 * time.Millisecond)
		}()

		messages, err := actor.Poll(ctx, 50*time.Millisecond)
		require.NoError(t, err)
		assert.Empty(t, messages)
	})
}

func TestConsumerActor_Close(t *testing.T) {
	t.Run("close transitions through Stopping to Stopped", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()

		actor.Close()

		assert.Equal(t, StateStopped, actor.State())
		assert.Equal(t, 1, mockRepo.getMarkOfflineCallCount())
	})

	t.Run("close with auto commit performs final commit", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 10 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()

		// Manually set up Ready state with some offsets to commit
		actor.stateMachine.forceSetState(StateReady)

		actor.offsetsToCommit = map[db.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}
		actor.generationID = 1

		actor.Close()

		// Check that commit was called
		mockRepo.mu.Lock()
		commitCalls := len(mockRepo.batchCommitCalls)
		mockRepo.mu.Unlock()

		assert.GreaterOrEqual(t, commitCalls, 1, "should have called commit at least once")
	})

	t.Run("close is idempotent", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()

		// First close
		actor.Close()

		// Wait a bit to ensure stopCh is fully closed
		time.Sleep(10 * time.Millisecond)

		// Subsequent Close() calls should return immediately (stopCh is closed)
		// Use goroutines with timeout to ensure we don't block forever
		done := make(chan struct{})
		go func() {
			actor.Close()
			close(done)
		}()

		select {
		case <-done:
			// OK, Close returned
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Second Close() call blocked")
		}

		// Use internal state machine since State() would return StateStopped via stopCh select
		assert.Equal(t, StateStopped, actor.stateMachine.Get())
		// MarkConsumerOffline should only be called once
		assert.Equal(t, 1, mockRepo.getMarkOfflineCallCount())
	})

	t.Run("close from Joining state", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()

		actor.SubscribeTopics("test-topic")
		assert.Equal(t, StateJoining, actor.State())

		actor.Close()

		assert.Equal(t, StateStopped, actor.State())
	})
}

func TestConsumerActor_Acknowledge(t *testing.T) {
	t.Run("acknowledge updates offsets to commit", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		messages := []ConsumerMessage{
			{Topic: "test-topic", Partition: 0, ID: 10},
			{Topic: "test-topic", Partition: 0, ID: 20},
			{Topic: "test-topic", Partition: 1, ID: 5},
		}

		actor.Acknowledge(messages...)

		// Allow time for the command to be processed
		time.Sleep(50 * time.Millisecond)

		// Verify offsets are tracked (max per partition)
		assert.Equal(t, int64(20), actor.offsetsToCommit[db.PartitionInfo{Topic: "test-topic", Partition: 0}])
		assert.Equal(t, int64(5), actor.offsetsToCommit[db.PartitionInfo{Topic: "test-topic", Partition: 1}])
	})

	t.Run("acknowledge with empty messages does nothing", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Should not panic or block
		actor.Acknowledge()

		time.Sleep(50 * time.Millisecond)
		assert.Empty(t, actor.offsetsToCommit)
	})
}

func TestConsumerActor_CommitSync(t *testing.T) {
	t.Run("commit sync commits pending offsets", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Setup offsets to commit
		actor.offsetsToCommit = map[db.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}
		actor.generationID = 1

		err := actor.CommitSync(context.Background())
		require.NoError(t, err)

		// Verify commit was called
		mockRepo.mu.Lock()
		require.Len(t, mockRepo.batchCommitCalls, 1)
		assert.Equal(t, "test-group", mockRepo.batchCommitCalls[0].groupID)
		assert.Equal(t, uint(1), mockRepo.batchCommitCalls[0].generationID)
		mockRepo.mu.Unlock()
	})

	t.Run("commit sync with no pending offsets succeeds", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		err := actor.CommitSync(context.Background())
		require.NoError(t, err)

		// No commit should be called
		mockRepo.mu.Lock()
		assert.Empty(t, mockRepo.batchCommitCalls)
		mockRepo.mu.Unlock()
	})

	t.Run("commit sync returns error on failure", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		mockRepo.batchCommitErr = errors.New("commit failed")
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.offsetsToCommit = map[db.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}

		err := actor.CommitSync(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit failed")
	})
}

func TestConsumerActor_IsReady(t *testing.T) {
	t.Run("returns false when not in Ready state", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.False(t, actor.IsReady())
	})

	t.Run("returns false when Ready but no partitions assigned", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Manually set state to Ready without assignments
		actor.stateMachine.forceSetState(StateReady)

		assert.False(t, actor.IsReady())
	})

	t.Run("returns true when Ready with partitions assigned", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Manually set state to Ready with assignments
		actor.stateMachine.forceSetState(StateReady)

		actor.assignment = []db.PartitionInfo{
			{Topic: "test-topic", Partition: 0},
			{Topic: "test-topic", Partition: 1},
		}

		assert.True(t, actor.IsReady())
	})
}

func TestConsumerActor_Rebalance(t *testing.T) {
	t.Run("rebalance updates generation and assignment", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()

		// Setup mock for rebalance - start with generation 1
		mockRepo.heartbeat = &db.ConsumerHeartbeat{
			GenerationID: 1,
			AssignedPartitions: []db.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock), WithNotifier(fakeNotifier))

		actor.Start()
		defer actor.Stop()

		// Subscribe to trigger first heartbeat
		actor.SubscribeTopics("test-topic")
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// Verify state is Ready after first heartbeat with assignment
		state := actor.State()
		assert.Equal(t, StateReady, state)

		// Now update mock to return generation 2
		mockRepo.setHeartbeat(&db.ConsumerHeartbeat{
			GenerationID: 2,
			AssignedPartitions: []db.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
				{Topic: "test-topic", Partition: 1},
			},
		})
		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
			{Topic: "test-topic", Partition: 1, LastConsumedMessageID: 200},
		}

		// Trigger another heartbeat to detect generation change
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// Verify state is still Ready after rebalance
		state = actor.State()
		assert.Equal(t, StateReady, state)

		// Verify notifier received unsubscribe and subscribe calls during rebalance
		// This indirectly verifies that rebalance occurred
		assert.GreaterOrEqual(t, len(fakeNotifier.GetSubscribeCalls()), 2)
	})

	t.Run("rebalance failure restores state and allows retry", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock - 第一次心跳成功，获得分配
		mockRepo.heartbeat = &db.ConsumerHeartbeat{
			GenerationID: 1,
			AssignedPartitions: []db.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Subscribe 并等待第一次心跳完成，进入 Ready 状态
		actor.SubscribeTopics("test-topic")
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		require.Equal(t, StateReady, actor.State())
		require.Equal(t, uint(1), actor.generationID)

		// 设置 generation 2，但让 GetCommittedOffsets 失败
		mockRepo.setHeartbeat(&db.ConsumerHeartbeat{
			GenerationID: 2,
			AssignedPartitions: []db.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
				{Topic: "test-topic", Partition: 1},
			},
		})
		mockRepo.mu.Lock()
		mockRepo.getCommittedErr = errors.New("database connection lost")
		mockRepo.mu.Unlock()

		// 触发心跳，重平衡应该失败
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// 关键断言：状态应该恢复到 Ready，而不是卡在 Rebalancing
		assert.Equal(t, StateReady, actor.State(), "状态应该恢复到 Ready，以便下次心跳可以重试")
		// generation 应该保持不变，因为重平衡失败了
		assert.Equal(t, uint(1), actor.generationID, "generation 应该保持不变")

		// 现在修复错误，让重平衡成功
		mockRepo.mu.Lock()
		mockRepo.getCommittedErr = nil
		mockRepo.committedOffsets = db.ConsumerGroupConsumptionProgressSlice{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
			{Topic: "test-topic", Partition: 1, LastConsumedMessageID: 200},
		}
		mockRepo.mu.Unlock()

		// 再次触发心跳，这次重平衡应该成功
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// 验证重平衡成功
		assert.Equal(t, StateReady, actor.State())
		assert.Equal(t, uint(2), actor.generationID, "generation 应该更新为 2")
		assert.Len(t, actor.assignment, 2, "应该有 2 个分区分配")
	})
}

func TestConsumerActor_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent state reads are safe", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.SubscribeTopics("test-topic")

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = actor.State()
					_ = actor.IsReady()
					_ = actor.ID()
				}
			}()
		}
		wg.Wait()
	})

	t.Run("concurrent acknowledge calls are safe", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					actor.Acknowledge(ConsumerMessage{
						Topic:     "test-topic",
						Partition: uint(id % 3),
						ID:        int64(j),
					})
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestConsumerActor_ID(t *testing.T) {
	t.Run("returns unique ID", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()

		actor1 := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo))

		actor2 := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithRepo(mockRepo))

		assert.NotEmpty(t, actor1.ID())
		assert.NotEmpty(t, actor2.ID())
		assert.NotEqual(t, actor1.ID(), actor2.ID())
	})
}

func TestConsumerActor_AutoCommitLoop(t *testing.T) {
	t.Run("auto commit loop commits periodically", func(t *testing.T) {
		mockRepo := newMockConsumerRepo()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 1 * time.Second,
			HeartbeatInterval:  10 * time.Second,
		}, WithRepo(mockRepo), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Subscribe to start auto commit loop
		actor.SubscribeTopics("test-topic")

		// Use Acknowledge to add offsets (this goes through the command channel)
		actor.Acknowledge(ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			ID:        100,
		})

		// Wait for the acknowledge command to be processed
		time.Sleep(50 * time.Millisecond)

		// Set generationID (needed for commit to work)
		// This is a race but acceptable for test purposes
		actor.generationID = 1

		// Advance clock past auto commit interval
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// Verify commit was called
		mockRepo.mu.Lock()
		commitCount := len(mockRepo.batchCommitCalls)
		mockRepo.mu.Unlock()

		assert.GreaterOrEqual(t, commitCount, 1)
	})
}
