package dbmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ========== Mock Heartbeat Repo Implementation ==========

// mockHeartbeatRepo implements heartbeat.Repo for testing
type mockHeartbeatRepo struct {
	mu sync.Mutex

	// Call records
	upsertCalls      []heartbeatUpsertCall
	getCalls         []heartbeatGetCall
	markOfflineCalls []markOfflineCall

	// Return value controls
	upsertErr      error
	heartbeat      *heartbeat.Heartbeat
	getErr         error
	markOfflineErr error
}

type heartbeatUpsertCall struct {
	groupID    string
	consumerID string
	topics     []string
}

type heartbeatGetCall struct {
	groupID    string
	consumerID string
}

type markOfflineCall struct {
	groupID    string
	consumerID string
}

func newMockHeartbeatRepo() *mockHeartbeatRepo {
	return &mockHeartbeatRepo{}
}

func (m *mockHeartbeatRepo) Get(ctx context.Context, groupID, consumerID string) (*heartbeat.Heartbeat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls = append(m.getCalls, heartbeatGetCall{groupID: groupID, consumerID: consumerID})
	return m.heartbeat, m.getErr
}

func (m *mockHeartbeatRepo) Upsert(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsertCalls = append(m.upsertCalls, heartbeatUpsertCall{groupID: groupID, consumerID: consumerID, topics: subscribedTopics})
	return m.upsertErr
}

func (m *mockHeartbeatRepo) MarkOffline(ctx context.Context, groupID, consumerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markOfflineCalls = append(m.markOfflineCalls, markOfflineCall{groupID: groupID, consumerID: consumerID})
	return m.markOfflineErr
}

func (m *mockHeartbeatRepo) Delete(ctx context.Context, groupID, consumerID string) error {
	return nil
}

func (m *mockHeartbeatRepo) FindActive(ctx context.Context, groupID string, timeout time.Duration) ([]*heartbeat.Heartbeat, error) {
	return nil, nil
}

func (m *mockHeartbeatRepo) FindAll(ctx context.Context, groupID string, timeout time.Duration) ([]*heartbeat.Heartbeat, error) {
	return nil, nil
}

func (m *mockHeartbeatRepo) setHeartbeat(hb *heartbeat.Heartbeat) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeat = hb
}

func (m *mockHeartbeatRepo) getMarkOfflineCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.markOfflineCalls)
}

// ========== Mock Progress Repo Implementation ==========

// mockProgressRepo implements consumerprogress.Repo for testing
type mockProgressRepo struct {
	mu sync.Mutex

	// Call records
	getCommittedOffsetsCalls []getCommittedOffsetsCall
	batchCommitCalls         []batchCommitCall

	// Return value controls
	committedOffsets    []*consumerprogress.Progress
	getCommittedErr     error
	batchCommitErr      error
	batchCommitWaterErr error
}

type getCommittedOffsetsCall struct {
	groupID    string
	partitions []types.PartitionInfo
}

type batchCommitCall struct {
	groupID      string
	generationID uint
	consumedIds  map[types.PartitionInfo]int64
}

func newMockProgressRepo() *mockProgressRepo {
	return &mockProgressRepo{}
}

func (m *mockProgressRepo) GetCommittedOffsets(ctx context.Context, groupID string, partitions []types.PartitionInfo) ([]*consumerprogress.Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCommittedOffsetsCalls = append(m.getCommittedOffsetsCalls, getCommittedOffsetsCall{groupID: groupID, partitions: partitions})
	return m.committedOffsets, m.getCommittedErr
}

func (m *mockProgressRepo) CommitOffset(ctx context.Context, groupID string, generationID uint, partition types.PartitionInfo, lastConsumedMessageID int64) error {
	return nil
}

func (m *mockProgressRepo) BatchCommitOffsets(ctx context.Context, groupID string, generationID uint, consumedIDs map[types.PartitionInfo]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchCommitCalls = append(m.batchCommitCalls, batchCommitCall{groupID: groupID, generationID: generationID, consumedIds: consumedIDs})
	return m.batchCommitErr
}

func (m *mockProgressRepo) BatchCommitOffsetsWithWatermark(ctx context.Context, groupID string, generationID uint, progressWithWatermarks map[types.PartitionInfo]consumerprogress.ProgressWithWatermark) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batchCommitWaterErr
}

func (m *mockProgressRepo) CommitWithSubscriptionRegistration(ctx context.Context, groupID string, generationID uint, partition types.PartitionInfo, lastConsumedMessageID, subscriptionStartWatermark int64) error {
	return nil
}

func (m *mockProgressRepo) GetLowWatermarks(ctx context.Context) (map[types.PartitionInfo]int64, error) {
	return nil, nil
}

// ========== Mock Message Repo Implementation ==========

// mockMessageRepo implements message.Repo for testing
type mockMessageRepo struct {
	mu sync.Mutex

	// Call records
	fetchBatchCalls []fetchBatchCall

	// Return value controls
	latestIDs        map[types.PartitionInfo]int64
	latestIDsErr     error
	fetchedMessages  []*message.Message
	fetchMessagesErr error
}

type fetchBatchCall struct {
	requests []message.FetchRequest
}

func newMockMessageRepo() *mockMessageRepo {
	return &mockMessageRepo{
		latestIDs: make(map[types.PartitionInfo]int64),
	}
}

func (m *mockMessageRepo) CreateBatch(ctx context.Context, messages []*message.Message) error {
	return nil
}

func (m *mockMessageRepo) Fetch(ctx context.Context, topic string, partition uint, afterID int64, limit int) ([]*message.Message, error) {
	return nil, nil
}

func (m *mockMessageRepo) FetchBatch(ctx context.Context, requests []message.FetchRequest) ([]*message.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchBatchCalls = append(m.fetchBatchCalls, fetchBatchCall{requests: requests})
	return m.fetchedMessages, m.fetchMessagesErr
}

func (m *mockMessageRepo) GetLatestID(ctx context.Context, topic string, partition uint) (int64, error) {
	return 0, nil
}

func (m *mockMessageRepo) GetLatestIDs(ctx context.Context, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestIDs, m.latestIDsErr
}

func (m *mockMessageRepo) DeleteConsumed(ctx context.Context, topic string, partition uint, maxID int64, retentionDate time.Time, limit int) (int64, error) {
	return 0, nil
}

func (m *mockMessageRepo) DeleteExpired(ctx context.Context, topic string, partition uint, retentionDate time.Time, limit int) (int64, error) {
	return 0, nil
}

// ========== Combined Mock for backward compatibility ==========

type mockConsumerRepos struct {
	heartbeat *mockHeartbeatRepo
	progress  *mockProgressRepo
	message   *mockMessageRepo
}

func newMockConsumerRepos() *mockConsumerRepos {
	return &mockConsumerRepos{
		heartbeat: newMockHeartbeatRepo(),
		progress:  newMockProgressRepo(),
		message:   newMockMessageRepo(),
	}
}

// Compile-time check that mocks implement interfaces
var (
	_ heartbeat.Repo        = (*mockHeartbeatRepo)(nil)
	_ consumerprogress.Repo = (*mockProgressRepo)(nil)
	_ message.Repo          = (*mockMessageRepo)(nil)
)

// ========== Test Cases ==========

func TestConsumerActor_NewConsumerActor(t *testing.T) {
	t.Run("creates actor with default values", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		require.NotNil(t, actor)
		assert.NotEmpty(t, actor.ID())
		// Check internal state directly since State() requires actor to be started
		assert.Equal(t, StateUninitialized, actor.stateMachine.Get())
	})

	t.Run("creates actor with custom clock and notifier", func(t *testing.T) {
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()
		mocks := newMockConsumerRepos()

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock), WithNotifier(fakeNotifier))

		require.NotNil(t, actor)
		// Check internal state directly since State() requires actor to be started
		assert.Equal(t, StateUninitialized, actor.stateMachine.Get())
	})

	t.Run("sets default auto commit interval when enabled", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID:          "test-group",
			EnableAutoCommit: true,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		require.NotNil(t, actor)
		assert.Equal(t, 5*time.Second, actor.config.AutoCommitInterval)
	})

	t.Run("preserves custom auto commit interval", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		require.NotNil(t, actor)
		assert.Equal(t, 10*time.Second, actor.config.AutoCommitInterval)
	})
}

func TestConsumerActor_Start_Stop(t *testing.T) {
	t.Run("start and stop without subscription", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		assert.Equal(t, StateUninitialized, actor.State())

		actor.Stop()
		// After stop, actor should no longer respond
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()

		// Multiple stops should not panic
		actor.Stop()
		actor.Stop()
		actor.Stop()
	})
}

func TestConsumerActor_SubscribeTopics(t *testing.T) {
	t.Run("subscribing transitions state to Joining", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock to return nil heartbeat initially
		mocks.heartbeat.heartbeat = nil

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.Equal(t, StateUninitialized, actor.State())

		actor.SubscribeTopics("test-topic")

		// State should be Joining after subscription
		assert.Equal(t, StateJoining, actor.State())
	})

	t.Run("subscribing multiple times does not change state", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.Equal(t, StateUninitialized, actor.State())

		actor.SubscribeTopics("test-topic")
		assert.Equal(t, StateJoining, actor.State())
	})

	t.Run("returns Stopped after stop", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		actor.Stop()

		// After stop, State() should return StateStopped
		assert.Equal(t, StateStopped, actor.State())
	})
}

func TestConsumerActor_Heartbeat_StateTransition(t *testing.T) {
	t.Run("heartbeat triggers state transition to Ready", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()

		// Setup mock to return heartbeat with assignment
		mocks.heartbeat.heartbeat = &heartbeat.Heartbeat{
			GenerationID: 1,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mocks.progress.committedOffsets = []*consumerprogress.Progress{
			{
				Topic:                 "test-topic",
				Partition:             0,
				LastConsumedMessageID: 100,
			},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock), WithNotifier(fakeNotifier))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock to return error on heartbeat
		mocks.heartbeat.upsertErr = errors.New("database connection lost")

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()
		fakeNotifier.SetHealthy(false) // Disable notifier to avoid blocking

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock), WithNotifier(fakeNotifier))

		actor.Start()
		defer actor.Stop()

		// Manually set up Ready state with assignment
		actor.stateMachine.forceSetState(StateReady)

		actor.assignment = []types.PartitionInfo{
			{Topic: "test-topic", Partition: 0},
		}
		actor.generationID = 1

		// Setup mock to return empty messages
		mocks.message.fetchedMessages = []*message.Message{}

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()

		actor.Close()

		assert.Equal(t, StateStopped, actor.State())
		assert.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
	})

	t.Run("close with auto commit performs final commit", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		mocks.progress.committedOffsets = []*consumerprogress.Progress{}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()

		// Manually set up Ready state with some offsets to commit
		actor.stateMachine.forceSetState(StateReady)

		actor.offsetsToCommit = map[types.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}
		actor.generationID = 1

		actor.Close()

		// Check that commit was called
		mocks.progress.mu.Lock()
		commitCalls := len(mocks.progress.batchCommitCalls)
		mocks.progress.mu.Unlock()

		assert.GreaterOrEqual(t, commitCalls, 1, "should have called commit at least once")
	})

	t.Run("close is idempotent", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		assert.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
	})

	t.Run("close from Joining state", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()

		actor.SubscribeTopics("test-topic")
		assert.Equal(t, StateJoining, actor.State())

		actor.Close()

		assert.Equal(t, StateStopped, actor.State())
	})
}

func TestConsumerActor_Acknowledge(t *testing.T) {
	t.Run("acknowledge updates offsets to commit", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		assert.Equal(t, int64(20), actor.offsetsToCommit[types.PartitionInfo{Topic: "test-topic", Partition: 0}])
		assert.Equal(t, int64(5), actor.offsetsToCommit[types.PartitionInfo{Topic: "test-topic", Partition: 1}])
	})

	t.Run("acknowledge with empty messages does nothing", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Setup offsets to commit
		actor.offsetsToCommit = map[types.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}
		actor.generationID = 1

		err := actor.CommitSync(context.Background())
		require.NoError(t, err)

		// Verify commit was called
		mocks.progress.mu.Lock()
		require.Len(t, mocks.progress.batchCommitCalls, 1)
		assert.Equal(t, "test-group", mocks.progress.batchCommitCalls[0].groupID)
		assert.Equal(t, uint(1), mocks.progress.batchCommitCalls[0].generationID)
		mocks.progress.mu.Unlock()
	})

	t.Run("commit sync with no pending offsets succeeds", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		err := actor.CommitSync(context.Background())
		require.NoError(t, err)

		// No commit should be called
		mocks.progress.mu.Lock()
		assert.Empty(t, mocks.progress.batchCommitCalls)
		mocks.progress.mu.Unlock()
	})

	t.Run("commit sync returns error on failure", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		mocks.progress.batchCommitErr = errors.New("commit failed")
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		actor.offsetsToCommit = map[types.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
		}

		err := actor.CommitSync(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit failed")
	})
}

func TestConsumerActor_IsReady(t *testing.T) {
	t.Run("returns false when not in Ready state", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		assert.False(t, actor.IsReady())
	})

	t.Run("returns false when Ready but no partitions assigned", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Manually set state to Ready without assignments
		actor.stateMachine.forceSetState(StateReady)

		assert.False(t, actor.IsReady())
	})

	t.Run("returns true when Ready with partitions assigned", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Manually set state to Ready with assignments
		actor.stateMachine.forceSetState(StateReady)

		actor.assignment = []types.PartitionInfo{
			{Topic: "test-topic", Partition: 0},
			{Topic: "test-topic", Partition: 1},
		}

		assert.True(t, actor.IsReady())
	})
}

func TestConsumerActor_Rebalance(t *testing.T) {
	t.Run("rebalance updates generation and assignment", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())
		fakeNotifier := NewFakeNotifier()

		// Setup mock for rebalance - start with generation 1
		mocks.heartbeat.heartbeat = &heartbeat.Heartbeat{
			GenerationID: 1,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mocks.progress.committedOffsets = []*consumerprogress.Progress{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock), WithNotifier(fakeNotifier))

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
		mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
			GenerationID: 2,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
				{Topic: "test-topic", Partition: 1},
			},
		})
		mocks.progress.committedOffsets = []*consumerprogress.Progress{
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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		// Setup mock - 第一次心跳成功，获得分配
		mocks.heartbeat.heartbeat = &heartbeat.Heartbeat{
			GenerationID: 1,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		}
		mocks.progress.committedOffsets = []*consumerprogress.Progress{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()

		// Subscribe 并等待第一次心跳完成，进入 Ready 状态
		actor.SubscribeTopics("test-topic")
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		require.Equal(t, StateReady, actor.State())
		require.Equal(t, uint(1), actor.generationID)

		// 设置 generation 2，但让 GetCommittedOffsets 失败
		mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
			GenerationID: 2,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
				{Topic: "test-topic", Partition: 1},
			},
		})
		mocks.progress.mu.Lock()
		mocks.progress.getCommittedErr = errors.New("database connection lost")
		mocks.progress.mu.Unlock()

		// 触发心跳，重平衡应该失败
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// 关键断言：状态应该恢复到 Ready，而不是卡在 Rebalancing
		assert.Equal(t, StateReady, actor.State(), "状态应该恢复到 Ready，以便下次心跳可以重试")
		// generation 应该保持不变，因为重平衡失败了
		assert.Equal(t, uint(1), actor.generationID, "generation 应该保持不变")

		// 现在修复错误，让重平衡成功
		mocks.progress.mu.Lock()
		mocks.progress.getCommittedErr = nil
		mocks.progress.committedOffsets = []*consumerprogress.Progress{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
			{Topic: "test-topic", Partition: 1, LastConsumedMessageID: 200},
		}
		mocks.progress.mu.Unlock()

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks := newMockConsumerRepos()

		actor1 := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		actor2 := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		assert.NotEmpty(t, actor1.ID())
		assert.NotEmpty(t, actor2.ID())
		assert.NotEqual(t, actor1.ID(), actor2.ID())
	})
}

func TestConsumerActor_AutoCommitLoop(t *testing.T) {
	t.Run("auto commit loop commits periodically", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:            "test-group",
			EnableAutoCommit:   true,
			AutoCommitInterval: 1 * time.Second,
			HeartbeatInterval:  10 * time.Second,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

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
		mocks.progress.mu.Lock()
		commitCount := len(mocks.progress.batchCommitCalls)
		mocks.progress.mu.Unlock()

		assert.GreaterOrEqual(t, commitCount, 1)
	})
}
