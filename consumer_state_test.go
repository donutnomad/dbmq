package dbmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerState_String(t *testing.T) {
	tests := []struct {
		state    ConsumerState
		expected string
	}{
		{StateUninitialized, "Uninitialized"},
		{StateJoining, "Joining"},
		{StateReady, "Ready"},
		{StateRebalancing, "Rebalancing"},
		{StateStopping, "Stopping"},
		{StateStopped, "Stopped"},
		{ConsumerState(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestConsumerStateMachine_InitialState(t *testing.T) {
	sm := newConsumerStateMachine()
	assert.Equal(t, StateUninitialized, sm.Get())
}

func TestConsumerStateMachine_ValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from ConsumerState
		to   ConsumerState
		ok   bool
	}{
		// From Uninitialized
		{"Uninitialized -> Joining", StateUninitialized, StateJoining, true},
		{"Uninitialized -> Stopping", StateUninitialized, StateStopping, true},
		{"Uninitialized -> Ready (invalid)", StateUninitialized, StateReady, false},
		{"Uninitialized -> Rebalancing (invalid)", StateUninitialized, StateRebalancing, false},

		// From Joining
		{"Joining -> Ready", StateJoining, StateReady, true},
		{"Joining -> Rebalancing", StateJoining, StateRebalancing, true},
		{"Joining -> Stopping", StateJoining, StateStopping, true},
		{"Joining -> Uninitialized (invalid)", StateJoining, StateUninitialized, false},

		// From Ready
		{"Ready -> Rebalancing", StateReady, StateRebalancing, true},
		{"Ready -> Stopping", StateReady, StateStopping, true},
		{"Ready -> Joining (invalid)", StateReady, StateJoining, false},
		{"Ready -> Uninitialized (invalid)", StateReady, StateUninitialized, false},

		// From Rebalancing
		{"Rebalancing -> Ready", StateRebalancing, StateReady, true},
		{"Rebalancing -> Joining (recovery)", StateRebalancing, StateJoining, true}, // 允许恢复到 Joining
		{"Rebalancing -> Stopping", StateRebalancing, StateStopping, true},
		{"Rebalancing -> Uninitialized (invalid)", StateRebalancing, StateUninitialized, false},

		// From Stopping
		{"Stopping -> Stopped", StateStopping, StateStopped, true},
		{"Stopping -> Ready (invalid)", StateStopping, StateReady, false},
		{"Stopping -> Rebalancing (invalid)", StateStopping, StateRebalancing, false},

		// From Stopped (terminal state)
		{"Stopped -> Stopping (invalid)", StateStopped, StateStopping, false},
		{"Stopped -> Ready (invalid)", StateStopped, StateReady, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newConsumerStateMachine()
			// Set initial state by forcing it (for testing purposes)
			sm.forceSetState(tt.from)

			err := sm.Transition(tt.to)
			if tt.ok {
				assert.NoError(t, err, "Expected transition to succeed")
				assert.Equal(t, tt.to, sm.Get())
			} else {
				assert.Error(t, err, "Expected transition to fail")
				var invalidErr *ErrInvalidStateTransition
				assert.ErrorAs(t, err, &invalidErr)
				assert.Equal(t, tt.from, sm.Get(), "State should not change on invalid transition")
			}
		})
	}
}

func TestConsumerStateMachine_TransitionIf(t *testing.T) {
	sm := newConsumerStateMachine()

	// Should succeed when current state matches
	ok := sm.TransitionIf(StateUninitialized, StateJoining)
	assert.True(t, ok)
	assert.Equal(t, StateJoining, sm.Get())

	// Should fail when current state doesn't match
	ok = sm.TransitionIf(StateUninitialized, StateReady)
	assert.False(t, ok)
	assert.Equal(t, StateJoining, sm.Get()) // State unchanged

	// Should fail when transition is invalid even if current state matches
	ok = sm.TransitionIf(StateJoining, StateUninitialized)
	assert.False(t, ok)
	assert.Equal(t, StateJoining, sm.Get()) // State unchanged
}

func TestConsumerStateMachine_OnTransitionCallback(t *testing.T) {
	sm := newConsumerStateMachine()

	var transitions []struct{ from, to ConsumerState }
	sm.SetOnTransition(func(from, to ConsumerState) {
		transitions = append(transitions, struct{ from, to ConsumerState }{from, to})
	})

	// Perform some transitions
	require.NoError(t, sm.Transition(StateJoining))
	require.NoError(t, sm.Transition(StateReady))
	require.NoError(t, sm.Transition(StateRebalancing))
	require.NoError(t, sm.Transition(StateReady))

	// Verify callbacks were called
	require.Len(t, transitions, 4)
	assert.Equal(t, StateUninitialized, transitions[0].from)
	assert.Equal(t, StateJoining, transitions[0].to)
	assert.Equal(t, StateJoining, transitions[1].from)
	assert.Equal(t, StateReady, transitions[1].to)
	assert.Equal(t, StateReady, transitions[2].from)
	assert.Equal(t, StateRebalancing, transitions[2].to)
	assert.Equal(t, StateRebalancing, transitions[3].from)
	assert.Equal(t, StateReady, transitions[3].to)
}

func TestConsumerStateMachine_IsAny(t *testing.T) {
	sm := newConsumerStateMachine()

	assert.True(t, sm.IsAny(StateUninitialized, StateJoining))
	assert.True(t, sm.IsAny(StateUninitialized))
	assert.False(t, sm.IsAny(StateJoining, StateReady))

	require.NoError(t, sm.Transition(StateJoining))
	assert.True(t, sm.IsAny(StateJoining, StateReady))
	assert.False(t, sm.IsAny(StateUninitialized, StateReady))
}

func TestConsumerStateMachine_HelperMethods(t *testing.T) {
	sm := newConsumerStateMachine()

	// Initial state
	assert.False(t, sm.IsReady())
	assert.False(t, sm.IsRebalancing())
	assert.False(t, sm.IsStopped())
	assert.False(t, sm.CanPoll())

	// After joining
	require.NoError(t, sm.Transition(StateJoining))
	assert.False(t, sm.IsReady())
	assert.False(t, sm.CanPoll())

	// After ready
	require.NoError(t, sm.Transition(StateReady))
	assert.True(t, sm.IsReady())
	assert.True(t, sm.CanPoll())
	assert.True(t, sm.CanConsume())

	// During rebalancing
	require.NoError(t, sm.Transition(StateRebalancing))
	assert.False(t, sm.IsReady())
	assert.True(t, sm.IsRebalancing())
	assert.False(t, sm.CanPoll())
	assert.True(t, sm.CanConsume()) // Can still consume during rebalancing

	// After stopping
	require.NoError(t, sm.Transition(StateStopping))
	assert.False(t, sm.CanPoll())
	assert.False(t, sm.CanConsume())

	// After stopped
	require.NoError(t, sm.Transition(StateStopped))
	assert.True(t, sm.IsStopped())
}

func TestConsumerStateMachine_MustTransition(t *testing.T) {
	sm := newConsumerStateMachine()

	// Valid transition should not panic
	assert.NotPanics(t, func() {
		sm.MustTransition(StateJoining)
	})

	// Invalid transition should panic
	assert.Panics(t, func() {
		sm.MustTransition(StateUninitialized)
	})
}

// 注意：移除了 TestConsumerStateMachine_ConcurrentAccess 测试
// 因为 Actor 模型下状态机仅在单一 goroutine 中访问，不需要并发安全

func TestConsumerStateMachine_FullLifecycle(t *testing.T) {
	sm := newConsumerStateMachine()

	// Simulate full consumer lifecycle
	assert.Equal(t, StateUninitialized, sm.Get())

	// Subscribe -> Joining
	require.NoError(t, sm.Transition(StateJoining))
	assert.Equal(t, StateJoining, sm.Get())

	// First assignment -> Ready
	require.NoError(t, sm.Transition(StateReady))
	assert.Equal(t, StateReady, sm.Get())

	// Rebalance detected
	require.NoError(t, sm.Transition(StateRebalancing))
	assert.Equal(t, StateRebalancing, sm.Get())

	// Rebalance complete
	require.NoError(t, sm.Transition(StateReady))
	assert.Equal(t, StateReady, sm.Get())

	// Another rebalance
	require.NoError(t, sm.Transition(StateRebalancing))
	require.NoError(t, sm.Transition(StateReady))

	// Close
	require.NoError(t, sm.Transition(StateStopping))
	assert.Equal(t, StateStopping, sm.Get())

	require.NoError(t, sm.Transition(StateStopped))
	assert.Equal(t, StateStopped, sm.Get())

	// Cannot transition from Stopped
	err := sm.Transition(StateReady)
	assert.Error(t, err)
}

func TestErrInvalidStateTransition_Error(t *testing.T) {
	err := &ErrInvalidStateTransition{
		From: StateReady,
		To:   StateUninitialized,
	}
	assert.Equal(t, "invalid state transition: Ready -> Uninitialized", err.Error())
}
