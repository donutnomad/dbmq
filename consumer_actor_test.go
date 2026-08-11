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
	upsertFn       func(context.Context, string, string, []string) error
	getFn          func(context.Context, string, string) (*heartbeat.Heartbeat, error)
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
	m.getCalls = append(m.getCalls, heartbeatGetCall{groupID: groupID, consumerID: consumerID})
	fn := m.getFn
	hb := m.heartbeat
	err := m.getErr
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, groupID, consumerID)
	}
	return hb, err
}

func (m *mockHeartbeatRepo) Upsert(ctx context.Context, groupID, consumerID string, subscribedTopics []string) error {
	m.mu.Lock()
	m.upsertCalls = append(m.upsertCalls, heartbeatUpsertCall{groupID: groupID, consumerID: consumerID, topics: subscribedTopics})
	fn := m.upsertFn
	err := m.upsertErr
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, groupID, consumerID, subscribedTopics)
	}
	return err
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

func (m *mockHeartbeatRepo) DeleteExpired(ctx context.Context, before time.Time, limit int) (int64, error) {
	return 0, nil
}

func (m *mockHeartbeatRepo) DeleteByGroup(ctx context.Context, groupID string) error {
	return nil
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

func (m *mockHeartbeatRepo) getUpsertCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.upsertCalls)
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

func (m *mockProgressRepo) DeleteByGroup(ctx context.Context, groupID string) error {
	return nil
}

func (m *mockProgressRepo) setCommittedOffsets(offsets []*consumerprogress.Progress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedOffsets = offsets
}

func (m *mockProgressRepo) DeleteByGroupTopicPartition(ctx context.Context, groupID string, topic string, partition uint) error {
	return nil
}

// ========== Mock Message Repo Implementation ==========

// mockMessageRepo implements message.Repo for testing
type mockMessageRepo struct {
	mu sync.Mutex

	// Call records
	fetchBatchCalls    []fetchBatchCall
	latestIDsCallCount int

	// Return value controls
	latestIDs        map[types.PartitionInfo]int64
	latestIDsErr     error
	fetchedMessages  []*message.Message
	fetchMessagesErr error
	fetchBatchFn     func(context.Context, []message.FetchRequest) ([]*message.Message, error)
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
	m.fetchBatchCalls = append(m.fetchBatchCalls, fetchBatchCall{requests: requests})
	fn := m.fetchBatchFn
	fetchedMessages := m.fetchedMessages
	fetchMessagesErr := m.fetchMessagesErr
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, requests)
	}
	return fetchedMessages, fetchMessagesErr
}

func (m *mockMessageRepo) GetLatestID(ctx context.Context, topic string, partition uint) (int64, error) {
	return 0, nil
}

func (m *mockMessageRepo) getFetchBatchCalls() []fetchBatchCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]fetchBatchCall(nil), m.fetchBatchCalls...)
}

func (m *mockMessageRepo) GetLatestIDs(ctx context.Context, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestIDsCallCount++
	return m.latestIDs, m.latestIDsErr
}

func (m *mockMessageRepo) getLatestIDsCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestIDsCallCount
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

	t.Run("stop before start is safe", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		actor.Stop()
		actor.Start()

		require.Equal(t, StateStopped, actor.State())
		require.Equal(t, 0, mocks.heartbeat.getMarkOfflineCallCount())
	})

	t.Run("concurrent start and stop is safe", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		actor := NewConsumerActor(ConsumerConfig{
			GroupID: "test-group",
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))

		const callers = 32
		start := make(chan struct{})
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(callers)
		for i := range callers {
			go func(callStart bool) {
				defer wg.Done()
				<-start
				if callStart {
					actor.Start()
					return
				}
				actor.Stop()
			}(i%2 == 0)
		}
		close(start)
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("并发 Start/Stop 未在期限内完成")
		}
		require.Equal(t, StateStopped, actor.State())
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

func TestConsumerActor_HeartbeatOperationTimeoutKeepsActorResponsive(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	upsertStarted := make(chan struct{})
	var startedOnce sync.Once
	mocks.heartbeat.upsertFn = func(ctx context.Context, _, _ string, _ []string) error {
		startedOnce.Do(func() { close(upsertStarted) })
		<-ctx.Done()
		return ctx.Err()
	}

	actor := NewConsumerActor(ConsumerConfig{
		GroupID:                   "test-group",
		HeartbeatInterval:         time.Second,
		HeartbeatOperationTimeout: 20 * time.Millisecond,
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))
	actor.Start()
	t.Cleanup(actor.Close)
	actor.SubscribeTopics("test-topic")

	select {
	case <-upsertStarted:
	case <-time.After(time.Second):
		t.Fatal("心跳 Upsert 未开始")
	}
	require.Eventually(t, func() bool {
		return actor.State() == StateJoining
	}, time.Second, 10*time.Millisecond, "心跳超时后 actor 应继续响应命令")
}

func TestConsumerActor_CloseCancelsBlockedHeartbeat(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	upsertStarted := make(chan struct{})
	mocks.heartbeat.upsertFn = func(ctx context.Context, _, _ string, _ []string) error {
		close(upsertStarted)
		<-ctx.Done()
		return ctx.Err()
	}

	actor := NewConsumerActor(ConsumerConfig{
		GroupID:                   "test-group",
		HeartbeatInterval:         time.Second,
		HeartbeatOperationTimeout: time.Hour,
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))
	actor.Start()
	actor.SubscribeTopics("test-topic")

	select {
	case <-upsertStarted:
	case <-time.After(time.Second):
		t.Fatal("心跳 Upsert 未开始")
	}

	closeDone := make(chan struct{})
	go func() {
		actor.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close 未取消阻塞的心跳")
	}
	require.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
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

func TestConsumerActor_Close_ConcurrentCalls(t *testing.T) {
	mocks := newMockConsumerRepos()
	actor := NewConsumerActor(ConsumerConfig{
		GroupID: "test-group",
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))
	actor.Start()

	const callers = 32
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			actor.Close()
		}()
	}
	close(start)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("并发 Close 未在期限内完成")
	}
	require.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
}

func TestConsumerActor_Stop_ConcurrentCalls(t *testing.T) {
	mocks := newMockConsumerRepos()
	actor := NewConsumerActor(ConsumerConfig{
		GroupID: "test-group",
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))
	actor.Start()

	const callers = 32
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			actor.Stop()
		}()
	}
	close(start)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("并发 Stop 未在期限内完成")
	}
	require.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
}

func TestConsumerActor_CloseAndStopShareShutdown(t *testing.T) {
	mocks := newMockConsumerRepos()
	actor := NewConsumerActor(ConsumerConfig{
		GroupID: "test-group",
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message))
	actor.Start()

	const callers = 32
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func(useClose bool) {
			defer wg.Done()
			<-start
			if useClose {
				actor.Close()
				return
			}
			actor.Stop()
		}(i%2 == 0)
	}
	close(start)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("混合 Close/Stop 未在期限内完成")
	}
	require.Equal(t, 1, mocks.heartbeat.getMarkOfflineCallCount())
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
		mocks.progress.setCommittedOffsets([]*consumerprogress.Progress{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 100},
			{Topic: "test-topic", Partition: 1, LastConsumedMessageID: 200},
		})

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

		// 重平衡失败后进入 Joining，暂停旧分区消费并等待心跳重试。
		assert.Equal(t, StateJoining, actor.State())
		// generation 应该保持不变，因为重平衡失败了
		assert.Equal(t, uint(1), actor.generationID, "generation 应该保持不变")
		pollCtx, cancelPoll := context.WithTimeout(context.Background(), time.Second)
		_, pollErr := actor.Poll(pollCtx, 0)
		cancelPoll()
		var rebalanceErr *ErrRebalanceInProgress
		require.ErrorAs(t, pollErr, &rebalanceErr)

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

func TestConsumerActor_ConsumeFromLatestPausesWhenLatestIDLookupFails(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	partition := types.PartitionInfo{Topic: "test-topic", Partition: 0}
	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       1,
		AssignedPartitions: []types.PartitionInfo{partition},
	})
	mocks.message.latestIDsErr = errors.New("latest ID query failed")

	actor := NewConsumerActor(ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: time.Second,
		ConsumeStrategy:   ConsumeFromLatest,
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))
	actor.Start()
	defer actor.Close()
	actor.SubscribeTopics("test-topic")

	require.Eventually(t, func() bool {
		return mocks.message.getLatestIDsCallCount() > 0 && actor.State() == StateJoining
	}, time.Second, 10*time.Millisecond)
	pausedState := getActorState(t, actor)
	require.Equal(t, uint(0), pausedState.GenerationID)
	require.Empty(t, pausedState.Assignment)

	mocks.message.mu.Lock()
	mocks.message.latestIDsErr = nil
	mocks.message.latestIDs[partition] = 42
	mocks.message.mu.Unlock()

	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.State == StateReady && state.GenerationID == 1
	}, 3*time.Second, 50*time.Millisecond)

	_, err := actor.Poll(context.Background(), 0)
	require.NoError(t, err)
	requests := mocks.message.getFetchBatchCalls()
	require.NotEmpty(t, requests)
	require.Equal(t, int64(41), requests[len(requests)-1].requests[0].AfterID)
}

// TestBug_CoordinatorRaceWindow 复现 coordinator 两阶段提交竞态导致消费者永久卡死的 bug。
//
// Bug 根因：coordinator 的 rebalance 分两个独立事务执行：
//
//	事务1: IncrementGenerationID (generation 508 → 509)
//	事务2: UpdateAssignments    (写入分区分配)
//
// 消费者心跳恰好落在事务1和事务2之间的窗口时：
//   - Get() 读到 generation_id=509（事务1已提交）
//   - 但 assigned_partitions=[]（事务2还没提交）
//   - doRebalance(509, []) 成功执行：generationID=509, assignment=[], state=Ready
//   - 事务2之后提交，数据库里有分区，但消费者内存里分区为空
//   - 此后每次心跳: dbGen(509) == currentGen(509) → 直接 return，永远不再触发 rebalance
//   - IsReady() 永远返回 false（state=Ready 但 assignment 为空）
func TestBug_CoordinatorRaceWindow(t *testing.T) {
	t.Run("heartbeat with empty partitions during coordinator two-phase commit self-heals", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())

		// 初始状态：消费者刚订阅，数据库里 generation=0，分区为空
		mocks.heartbeat.heartbeat = &heartbeat.Heartbeat{
			GenerationID:       0,
			AssignedPartitions: []types.PartitionInfo{},
		}

		actor := NewConsumerActor(ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: 1 * time.Second,
			ConsumeStrategy:   ConsumeFromEarliest,
		}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

		actor.Start()
		defer actor.Stop()
		actor.SubscribeTopics("test-topic")

		// 首次心跳：dbGen=0 == currentGen=0，不触发 rebalance，消费者保持 Joining
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, StateJoining, getActorState(t, actor).State, "首次心跳后应仍在 Joining 状态")

		// ====== 模拟 coordinator 竞态窗口 ======
		// 事务1完成：IncrementGenerationID，generation 0→509
		// 事务2未完成：UpdateAssignments 还没执行
		// 此时消费者心跳读到：generation_id=509 但 assigned_partitions=[]
		mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
			GenerationID:       509,
			AssignedPartitions: []types.PartitionInfo{}, // 竞态窗口：分区还未写入
		})

		// 触发心跳：dbGen=509 != currentGen=0 → 触发 doRebalance(509, [])
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// doRebalance 使用空分区成功完成：state=Ready，但 assignment=[]
		midState := getActorState(t, actor)
		require.Equal(t, StateReady, midState.State, "doRebalance(509,[]) 后状态变为 Ready")
		require.Equal(t, uint(509), midState.GenerationID, "generationID 已更新为 509")
		require.Empty(t, midState.Assignment, "分区为空（竞态窗口中拿到的是空分区）")

		// ====== 事务2完成：coordinator 写入了实际的分区分配 ======
		// 数据库：generation_id=509, assigned_partitions=[p0]
		mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
			GenerationID: 509, // generation 没有再次变化
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		})

		// ====== 验证自愈：generation 未变但 assignment 与数据库不一致时，心跳触发自愈同步 ======
		// （历史 bug：旧实现只比较 generation，此场景会永久卡死在空 assignment，需重启恢复）
		require.Eventually(t, func() bool {
			fakeClock.Advance(2 * time.Second)
			state := getActorState(t, actor)
			return state.State == StateReady && state.GenerationID == 509 && len(state.Assignment) == 1
		}, 3*time.Second, 50*time.Millisecond, "同代际 assignment 偏差应通过心跳自愈，恢复 [p0]")
		assert.True(t, actor.IsReady(), "自愈后 IsReady() 应返回 true")

		// ====== 验证修复：当 coordinator 再次触发 rebalance（gen 变化），消费者能正常恢复 ======
		// 修复后，coordinator 端合并了两个事务，不再出现 gen 增加但 partition 未更新的中间态。
		// 这里模拟正常的下一次 rebalance（gen 510，有分区）
		mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
			GenerationID: 510,
			AssignedPartitions: []types.PartitionInfo{
				{Topic: "test-topic", Partition: 0},
			},
		})
		mocks.progress.setCommittedOffsets([]*consumerprogress.Progress{
			{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 0},
		})

		// 触发心跳：gen=510 != currentGen=509，触发 doRebalance(510, [p0])
		fakeClock.Advance(2 * time.Second)
		time.Sleep(100 * time.Millisecond)

		// 修复后验证：消费者能通过下一次 rebalance 自愈
		finalState := getActorState(t, actor)
		assert.Equal(t, StateReady, finalState.State, "下一次 rebalance 后应恢复 Ready")
		assert.Equal(t, uint(510), finalState.GenerationID, "generation 更新为 510")
		assert.Len(t, finalState.Assignment, 1, "分区分配已恢复")
		assert.True(t, actor.IsReady(), "IsReady() 应返回 true")
	})
}

// blockingWaiter 测试用 Waiter：Wait() 被调用时向 waitCalled 发信号（用于确认
// Poll 已进入等待，消除 sleep 竞态），返回的 channel 由测试通过 release 控制
type blockingWaiter struct {
	waitCalled chan struct{}
	release    chan struct{}
}

func newBlockingWaiter() *blockingWaiter {
	return &blockingWaiter{
		waitCalled: make(chan struct{}, 10),
		release:    make(chan struct{}),
	}
}

func (w *blockingWaiter) Wait(ctx context.Context, timeout time.Duration) <-chan struct{} {
	select {
	case w.waitCalled <- struct{}{}:
	default:
	}
	ch := make(chan struct{})
	if timeout <= 0 {
		close(ch)
		return ch
	}
	go func() {
		select {
		case <-w.release:
		case <-ctx.Done():
		}
		close(ch)
	}()
	return ch
}

// startReadyActorWithConfig 创建并驱动一个消费者到 Ready(gen=1, [test-topic:0]) 状态。
func startReadyActorWithConfig(t *testing.T, mocks *mockConsumerRepos, fakeClock *FakeClock, waiter Waiter, config ConsumerConfig) *ConsumerActor {
	t.Helper()

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID: 1,
		AssignedPartitions: []types.PartitionInfo{
			{Topic: "test-topic", Partition: 0},
		},
	})

	actor := NewConsumerActor(config, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock), WithWaiter(waiter))

	actor.Start()
	actor.SubscribeTopics("test-topic")

	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.State == StateReady && len(state.Assignment) == 1
	}, 3*time.Second, 50*time.Millisecond, "消费者应进入 Ready 且持有分区")

	return actor
}

// startReadyActorWithWaiter 使用默认测试配置创建 Ready actor。
func startReadyActorWithWaiter(t *testing.T, mocks *mockConsumerRepos, fakeClock *FakeClock, waiter Waiter) *ConsumerActor {
	t.Helper()
	return startReadyActorWithConfig(t, mocks, fakeClock, waiter, ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
	})
}

// TestConsumerActor_HeartbeatNotBlockedByPoll Poll 等待期间心跳必须照常执行。
// 历史 bug：handlePoll 在 actor 单线程内阻塞等待长达整个 Poll timeout，
// 心跳命令排队等待，实际心跳间隔 = HeartbeatInterval + 阻塞时间，
// 超过 HeartbeatTimeout 后消费者被协调器误判死亡并清空分区。
func TestConsumerActor_HeartbeatNotBlockedByPoll(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()

	// 启动一个长时间等待的 Poll
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		_, _ = actor.Poll(context.Background(), 1*time.Hour)
	}()

	// 等待 Poll 进入等待状态
	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未进入等待状态")
	}

	baseline := mocks.heartbeat.getUpsertCallCount()

	// Poll 等待期间推进时钟触发心跳，Upsert 必须照常发生
	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		return mocks.heartbeat.getUpsertCallCount() > baseline
	}, 3*time.Second, 50*time.Millisecond, "Poll 等待期间心跳 Upsert 应照常执行")

	// 清理：释放 waiter 让 Poll 结束
	close(waiter.release)
	select {
	case <-pollDone:
	case <-time.After(2 * time.Second):
		t.Fatal("释放 waiter 后 Poll 未返回")
	}
}

// TestConsumerActor_Close_NotBlockedByPoll Poll 等待期间 Close 必须能及时完成。
// 历史 bug：CloseCmd 排在阻塞的 PollCmd 之后，若 waiter 长时间不触发，
// Close 会阻塞整个 Poll timeout（waiter 永不触发时死锁）。
func TestConsumerActor_Close_NotBlockedByPoll(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)

	pollResult := make(chan error, 1)
	go func() {
		_, err := actor.Poll(context.Background(), 1*time.Hour)
		pollResult <- err
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未进入等待状态")
	}

	// waiter 永不释放，Close 必须仍能在限定时间内完成
	closeDone := make(chan struct{})
	go func() {
		actor.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 等待期间 Close 超时（被阻塞的 Poll 卡住）")
	}

	select {
	case err := <-pollResult:
		assert.ErrorIs(t, err, context.Canceled, "Close 后等待中的 Poll 应返回 context.Canceled")
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后 Poll 未返回")
	}
}

// TestConsumerActor_Poll_RebalanceDuringWait 等待期间 generation 变化应返回 ErrRebalanceInProgress
func TestConsumerActor_Poll_RebalanceDuringWait(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()
	defer close(waiter.release)

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()

	pollResult := make(chan error, 1)
	go func() {
		_, err := actor.Poll(context.Background(), 1*time.Hour)
		pollResult <- err
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未进入等待状态")
	}

	// 等待期间发生 generation 变化的重平衡
	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       2,
		AssignedPartitions: []types.PartitionInfo{{Topic: "test-topic", Partition: 0}},
	})
	cmd := NewRebalanceCmd(context.Background(), 2, []types.PartitionInfo{{Topic: "test-topic", Partition: 0}})
	_, err := sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)

	select {
	case err := <-pollResult:
		var rebalanceErr *ErrRebalanceInProgress
		assert.True(t, errors.As(err, &rebalanceErr), "等待期间 generation 变化应返回 ErrRebalanceInProgress，实际: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未返回")
	}
}

// TestConsumerActor_WaitForNotification_AllWaitersWakeOnRebalance 验证
// rebalance channel 的广播语义。Poll 入口会在此层之上串行化。
func TestConsumerActor_WaitForNotification_AllWaitersWakeOnRebalance(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()
	defer close(waiter.release)

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()
	rebalanceCh := actor.currentRebalanceCh()

	waitResults := make(chan error, 2)
	for range 2 {
		go func() {
			waitResults <- actor.waitForNotification(context.Background(), time.Hour, rebalanceCh)
		}()
	}

	for range 2 {
		select {
		case <-waiter.waitCalled:
		case <-time.After(2 * time.Second):
			t.Fatal("Poll 未进入等待状态")
		}
	}

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       2,
		AssignedPartitions: []types.PartitionInfo{{Topic: "test-topic", Partition: 0}},
	})
	cmd := NewRebalanceCmd(context.Background(), 2, []types.PartitionInfo{{Topic: "test-topic", Partition: 0}})
	_, err := sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)

	for range 2 {
		select {
		case err := <-waitResults:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("rebalance 未唤醒全部 waiter")
		}
	}
}

func TestConsumerActor_Poll_SerializesConcurrentCalls(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()
	defer close(waiter.release)

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()

	firstResult := make(chan error, 1)
	go func() {
		_, err := actor.Poll(context.Background(), time.Hour)
		firstResult <- err
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个 Poll 未进入等待状态")
	}

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSecond()
	_, err := actor.Poll(secondCtx, time.Hour)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-waiter.waitCalled:
		t.Fatal("并发 Poll 进入了第二个等待流程")
	default:
	}

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       2,
		AssignedPartitions: []types.PartitionInfo{{Topic: "test-topic", Partition: 0}},
	})
	cmd := NewRebalanceCmd(context.Background(), 2, []types.PartitionInfo{{Topic: "test-topic", Partition: 0}})
	_, err = sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)

	select {
	case err := <-firstResult:
		var rebalanceErr *ErrRebalanceInProgress
		require.ErrorAs(t, err, &rebalanceErr)
	case <-time.After(2 * time.Second):
		t.Fatal("第一个 Poll 未在 rebalance 后返回")
	}
}

func TestConsumerActor_Poll_CloseCancelsActiveAndQueuedCalls(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()
	defer close(waiter.release)

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)

	results := make(chan error, 2)
	go func() {
		_, err := actor.Poll(context.Background(), time.Hour)
		results <- err
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("第一个 Poll 未进入等待状态")
	}

	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		_, err := actor.Poll(context.Background(), time.Hour)
		results <- err
	}()
	<-secondStarted

	actor.Close()

	for range 2 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("Consumer Close 未取消全部 Poll")
		}
	}
}

// TestConsumer_PollLoop_RetriesInternalFetchTimeout 复现生产问题：一次内部
// FetchBatch 超时后 PollLoop 仍应保活，并在下一次尝试中消费积压消息。
func TestConsumer_PollLoop_RetriesInternalFetchTimeout(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithConfig(t, mocks, fakeClock, NoopWaiter{}, ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: time.Second,
		PollFetchTimeout:  20 * time.Millisecond,
		ConsumeStrategy:   ConsumeFromEarliest,
	})
	defer actor.Stop()

	var attempt int
	mocks.message.fetchBatchFn = func(ctx context.Context, _ []message.FetchRequest) ([]*message.Message, error) {
		attempt++
		if attempt == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []*message.Message{{ID: 42, Topic: "test-topic", Partition: 0}}, nil
	}

	consumer := &Consumer{
		config: actor.config,
		actor:  actor,
		fetchRetryDelay: func(int) time.Duration {
			return 0
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	received := make(chan []ConsumerMessage, 1)
	go func() {
		errCh <- consumer.PollLoop(ctx, 0, func(messages []ConsumerMessage) {
			received <- messages
			cancel()
		})
	}()

	select {
	case messages := <-received:
		require.Len(t, messages, 1)
		assert.Equal(t, int64(42), messages[0].ID)
	case err := <-errCh:
		t.Fatalf("PollLoop 在恢复前退出: %v", err)
	case <-time.After(4 * time.Second):
		t.Fatal("PollLoop 未从内部拉取超时中恢复")
	}

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop 未在消费到消息后退出")
	}
	require.Len(t, mocks.message.getFetchBatchCalls(), 2)
	require.True(t, consumer.IsReady())
}

func TestDefaultFetchRetryDelayBounds(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		nominal := fetchRetryBaseDelay << min(attempt-1, 5)
		if nominal > fetchRetryMaxDelay {
			nominal = fetchRetryMaxDelay
		}
		for range 100 {
			delay := defaultFetchRetryDelay(attempt)
			require.GreaterOrEqual(t, delay, nominal-nominal/5)
			require.LessOrEqual(t, delay, nominal)
		}
	}
}

func TestConsumer_PollLoop_FetchBackoffResetsAfterSuccess(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithWaiter(t, mocks, fakeClock, NoopWaiter{})
	defer actor.Close()

	var fetchAttempt int
	mocks.message.fetchBatchFn = func(context.Context, []message.FetchRequest) ([]*message.Message, error) {
		fetchAttempt++
		switch fetchAttempt {
		case 1, 2, 4:
			return nil, errors.New("temporary database failure")
		case 3:
			return []*message.Message{}, nil
		default:
			return []*message.Message{{ID: 46, Topic: "test-topic", Partition: 0}}, nil
		}
	}

	var retryAttempts []int
	consumer := &Consumer{
		config: actor.config,
		actor:  actor,
		fetchRetryDelay: func(attempt int) time.Duration {
			retryAttempts = append(retryAttempts, attempt)
			return 0
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.PollLoop(ctx, 0, func([]ConsumerMessage) {
			cancel()
		})
	}()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("PollLoop 未完成退避复位场景")
	}
	require.Equal(t, []int{1, 2, 1}, retryAttempts)
}

func TestConsumerActor_Poll_InternalFetchTimeoutPreservesCause(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithConfig(t, mocks, fakeClock, NoopWaiter{}, ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: time.Second,
		PollFetchTimeout:  20 * time.Millisecond,
		ConsumeStrategy:   ConsumeFromEarliest,
	})
	defer actor.Stop()

	mocks.message.fetchBatchFn = func(ctx context.Context, _ []message.FetchRequest) ([]*message.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := actor.Poll(context.Background(), 0)
	var fetchErr *ErrFailedFetchMessage
	require.ErrorAs(t, err, &fetchErr)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestConsumer_PollLoop_StopsOnParentCancellation(t *testing.T) {
	t.Run("before poll", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())
		actor := startReadyActorWithWaiter(t, mocks, fakeClock, NoopWaiter{})
		defer actor.Stop()

		consumer := &Consumer{config: actor.config, actor: actor}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := consumer.PollLoop(ctx, time.Hour, func([]ConsumerMessage) {})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, mocks.message.getFetchBatchCalls())
	})

	t.Run("during fetch", func(t *testing.T) {
		mocks := newMockConsumerRepos()
		fakeClock := NewFakeClock(time.Now())
		actor := startReadyActorWithConfig(t, mocks, fakeClock, NoopWaiter{}, ConsumerConfig{
			GroupID:           "test-group",
			HeartbeatInterval: time.Second,
			PollFetchTimeout:  time.Hour,
			ConsumeStrategy:   ConsumeFromEarliest,
		})
		defer actor.Stop()

		fetchStarted := make(chan struct{})
		mocks.message.fetchBatchFn = func(ctx context.Context, _ []message.FetchRequest) ([]*message.Message, error) {
			close(fetchStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		consumer := &Consumer{config: actor.config, actor: actor}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- consumer.PollLoop(ctx, 0, func([]ConsumerMessage) {})
		}()

		select {
		case <-fetchStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("FetchBatch 未开始")
		}
		cancel()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("PollLoop 未在父 context 取消后退出")
		}
		require.Len(t, mocks.message.getFetchBatchCalls(), 1)
	})
}

// TestConsumer_PollLoop_RebalanceFetchesWithoutLongWait 覆盖 actor 收到
// rebalance 后的恢复链：唤醒长等待 Poll，随后立即查询数据库积压。
func TestConsumer_PollLoop_RebalanceFetchesWithoutLongWait(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()
	defer close(waiter.release)

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()
	mocks.message.fetchedMessages = []*message.Message{{ID: 43, Topic: "test-topic", Partition: 0}}

	consumer := &Consumer{config: actor.config, actor: actor}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	received := make(chan []ConsumerMessage, 1)
	go func() {
		errCh <- consumer.PollLoop(ctx, time.Hour, func(messages []ConsumerMessage) {
			received <- messages
			cancel()
		})
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop 未进入长等待")
	}

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       2,
		AssignedPartitions: []types.PartitionInfo{{Topic: "test-topic", Partition: 0}},
	})
	cmd := NewRebalanceCmd(context.Background(), 2, []types.PartitionInfo{{Topic: "test-topic", Partition: 0}})
	_, err := sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)

	select {
	case messages := <-received:
		require.Len(t, messages, 1)
		assert.Equal(t, int64(43), messages[0].ID)
	case err := <-errCh:
		t.Fatalf("PollLoop 在 rebalance 恢复前退出: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("PollLoop 未在 rebalance 后及时拉取积压消息")
	}

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("PollLoop 未在 rebalance 恢复后退出")
	}
}

func TestConsumer_PollLoop_StopsWhenConsumerClosesDuringTimer(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithWaiter(t, mocks, fakeClock, NoopWaiter{})
	consumer := &Consumer{config: actor.config, actor: actor}
	t.Cleanup(consumer.Close)

	errCh := make(chan error, 1)
	delivered := make(chan struct{}, 1)
	go func() {
		errCh <- consumer.PollLoop(context.Background(), time.Hour, func([]ConsumerMessage) {
			delivered <- struct{}{}
		})
	}()

	consumer.Close()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer Close 后 PollLoop 仍在 timer 中等待")
	}
	select {
	case <-delivered:
		t.Fatal("Consumer Close 后仍投递了消息")
	default:
	}
}

func TestConsumer_PollLoop_CloseDuringFetchPreventsDelivery(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithConfig(t, mocks, fakeClock, NoopWaiter{}, ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: time.Second,
		PollFetchTimeout:  time.Hour,
		ConsumeStrategy:   ConsumeFromEarliest,
	})
	consumer := &Consumer{config: actor.config, actor: actor}
	t.Cleanup(consumer.Close)

	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})
	mocks.message.fetchBatchFn = func(context.Context, []message.FetchRequest) ([]*message.Message, error) {
		close(fetchStarted)
		<-releaseFetch
		return []*message.Message{{ID: 44, Topic: "test-topic", Partition: 0}}, nil
	}

	errCh := make(chan error, 1)
	delivered := make(chan []ConsumerMessage, 1)
	go func() {
		errCh <- consumer.PollLoop(context.Background(), 0, func(messages []ConsumerMessage) {
			delivered <- messages
		})
	}()

	select {
	case <-fetchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("FetchBatch 未开始")
	}

	consumer.Close()
	close(releaseFetch)

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer Close 后 PollLoop 未退出")
	}
	select {
	case messages := <-delivered:
		t.Fatalf("Consumer Close 后投递了 %d 条消息", len(messages))
	default:
	}
}

func TestConsumer_CloseWaitsForInFlightCallback(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	actor := startReadyActorWithWaiter(t, mocks, fakeClock, NoopWaiter{})
	mocks.message.fetchedMessages = []*message.Message{{ID: 45, Topic: "test-topic", Partition: 0}}
	consumer := &Consumer{config: actor.config, actor: actor}

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	defer release()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.PollLoop(context.Background(), 0, func([]ConsumerMessage) {
			close(callbackStarted)
			<-releaseCallback
		})
	}()

	select {
	case <-callbackStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("onMessage 未开始")
	}

	closeDone := make(chan struct{})
	go func() {
		consumer.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close 在已开始的 onMessage 完成前返回")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("onMessage 完成后 Close 未返回")
	}
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后 PollLoop 未退出")
	}
}

// TestConsumerActor_Poll_SameGenAssignmentChange_UsesNewAssignment 等待期间发生
// 同代际 assignment 自愈（见 handleHeartbeat 的漂移自愈路径）时，
// fetch 必须使用等待后的新 assignment，不能使用等待前的快照
func TestConsumerActor_Poll_SameGenAssignmentChange_UsesNewAssignment(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()

	p1 := types.PartitionInfo{Topic: "test-topic", Partition: 1}

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter) // Ready: gen=1, [p0]
	defer actor.Stop()

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		_, _ = actor.Poll(context.Background(), 1*time.Hour)
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未进入等待状态")
	}

	// 等待期间发生同代际 assignment 变化（generation 仍为 1，分区 p0 → p1）
	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       1,
		AssignedPartitions: []types.PartitionInfo{p1},
	})
	cmd := NewRebalanceCmd(context.Background(), 1, []types.PartitionInfo{p1})
	_, err := sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)

	close(waiter.release)

	select {
	case <-pollDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未返回")
	}

	calls := mocks.message.getFetchBatchCalls()
	require.NotEmpty(t, calls, "Poll 应执行了 FetchBatch")
	lastCall := calls[len(calls)-1]
	require.Len(t, lastCall.requests, 1)
	assert.Equal(t, "test-topic", lastCall.requests[0].Topic)
	assert.Equal(t, uint(1), lastCall.requests[0].Partition, "fetch 应使用等待后的新分区 p1")
}

// TestConsumerActor_Poll_OffsetsTakenAfterWait offsets 必须在等待之后取：
// 等待期间 commit 会推进消费位置，使用等待前的快照会重复拉取已确认的消息
func TestConsumerActor_Poll_OffsetsTakenAfterWait(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())
	waiter := newBlockingWaiter()

	// 初始已提交位置为 10
	mocks.progress.committedOffsets = []*consumerprogress.Progress{
		{Topic: "test-topic", Partition: 0, LastConsumedMessageID: 10},
	}

	actor := startReadyActorWithWaiter(t, mocks, fakeClock, waiter)
	defer actor.Stop()

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		_, _ = actor.Poll(context.Background(), 1*time.Hour)
	}()

	select {
	case <-waiter.waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未进入等待状态")
	}

	// 等待期间确认并提交到 42
	actor.Acknowledge(ConsumerMessage{Topic: "test-topic", Partition: 0, ID: 42})
	require.NoError(t, actor.CommitSync(context.Background()))

	close(waiter.release)

	select {
	case <-pollDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Poll 未返回")
	}

	calls := mocks.message.getFetchBatchCalls()
	require.NotEmpty(t, calls, "Poll 应执行了 FetchBatch")
	lastCall := calls[len(calls)-1]
	require.Len(t, lastCall.requests, 1)
	assert.Equal(t, int64(42), lastCall.requests[0].AfterID, "fetch 应使用等待后（commit 推进过）的 offset")
}

// getActorState 通过命令通道线程安全地读取 actor 状态
func getActorState(t *testing.T, actor *ConsumerActor) GetStateResult {
	t.Helper()
	cmd := NewGetStateCmd()
	result, err := sendCmd(actor, cmd, cmd.ResultChan())
	require.NoError(t, err)
	return result
}

// TestConsumerActor_Heartbeat_SameGenAssignmentDrift_SelfHeals 同 generation 但
// 数据库 assignment 与内存不一致时（手动修数据或未知异常导致的等代际偏差），
// 心跳必须触发自愈同步，而不是因 generation 相等而跳过
func TestConsumerActor_Heartbeat_SameGenAssignmentDrift_SelfHeals(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())

	p0 := types.PartitionInfo{Topic: "test-topic", Partition: 0}
	p1 := types.PartitionInfo{Topic: "test-topic", Partition: 1}

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       1,
		AssignedPartitions: []types.PartitionInfo{p0},
	})

	actor := NewConsumerActor(ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

	actor.Start()
	defer actor.Stop()
	actor.SubscribeTopics("test-topic")

	// 驱动到 Ready: gen=1, assignment=[p0]
	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.State == StateReady && len(state.Assignment) == 1
	}, 3*time.Second, 50*time.Millisecond, "消费者应进入 Ready 且持有 [p0]")

	// 数据库 assignment 变为 [p0, p1]，generation 保持 1 不变
	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       1,
		AssignedPartitions: []types.PartitionInfo{p0, p1},
	})

	// 心跳后必须自愈：assignment 同步为 2 个分区，generation 仍为 1
	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.State == StateReady && state.GenerationID == 1 && len(state.Assignment) == 2
	}, 3*time.Second, 50*time.Millisecond, "同代际 assignment 偏差应通过心跳自愈")
}

// TestConsumerActor_Heartbeat_GenerationRegression_Adopts generation 回退必须采纳数据库状态
// （组删除重建后 generation 从低值重新开始是合法回退；协调器的 generation 落后检测
// 会很快推进行 generation）。固化该语义，防止未来改成拒绝回退导致永久卡死。
func TestConsumerActor_Heartbeat_GenerationRegression_Adopts(t *testing.T) {
	mocks := newMockConsumerRepos()
	fakeClock := NewFakeClock(time.Now())

	p0 := types.PartitionInfo{Topic: "test-topic", Partition: 0}
	p1 := types.PartitionInfo{Topic: "test-topic", Partition: 1}

	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       5,
		AssignedPartitions: []types.PartitionInfo{p0},
	})

	actor := NewConsumerActor(ConsumerConfig{
		GroupID:           "test-group",
		HeartbeatInterval: 1 * time.Second,
		ConsumeStrategy:   ConsumeFromEarliest,
	}, WithHeartbeatRepo(mocks.heartbeat), WithProgressRepo(mocks.progress), WithMessageRepo(mocks.message), WithClock(fakeClock))

	actor.Start()
	defer actor.Stop()
	actor.SubscribeTopics("test-topic")

	// 驱动到 Ready: gen=5, assignment=[p0]
	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.State == StateReady && state.GenerationID == 5
	}, 3*time.Second, 50*time.Millisecond, "消费者应进入 Ready 且 gen=5")

	// 数据库 generation 回退到 2（如组删除重建）
	mocks.heartbeat.setHeartbeat(&heartbeat.Heartbeat{
		GenerationID:       2,
		AssignedPartitions: []types.PartitionInfo{p1},
	})

	require.Eventually(t, func() bool {
		fakeClock.Advance(2 * time.Second)
		state := getActorState(t, actor)
		return state.GenerationID == 2 && len(state.Assignment) == 1 &&
			state.Assignment[0] == p1
	}, 3*time.Second, 50*time.Millisecond, "generation 回退时应采纳数据库状态")
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
