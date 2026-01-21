package dbmq

import (
	"fmt"
	"slices"
)

// ConsumerState 表示消费者的生命周期状态
type ConsumerState int

const (
	// StateUninitialized 消费者已创建但尚未订阅任何主题
	StateUninitialized ConsumerState = iota

	// StateJoining 消费者正在加入消费组，等待分区分配
	StateJoining

	// StateReady 消费者已就绪，可以正常消费消息
	StateReady

	// StateRebalancing 消费者正在进行重平衡，暂停消费
	StateRebalancing

	// StateStopping 消费者正在关闭
	StateStopping

	// StateStopped 消费者已完全关闭
	StateStopped
)

// String 返回状态的字符串表示
func (s ConsumerState) String() string {
	switch s {
	case StateUninitialized:
		return "Uninitialized"
	case StateJoining:
		return "Joining"
	case StateReady:
		return "Ready"
	case StateRebalancing:
		return "Rebalancing"
	case StateStopping:
		return "Stopping"
	case StateStopped:
		return "Stopped"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// consumerStateMachine 管理消费者状态转换的状态机
// 注意：此状态机设计为仅在 Actor 主循环中访问，无需锁保护
// 所有状态修改都通过命令通道串行执行，保证线程安全
type consumerStateMachine struct {
	state ConsumerState

	// onTransition 状态转换时的回调函数（可选）
	onTransition func(from, to ConsumerState)
}

// newConsumerStateMachine 创建一个新的状态机，初始状态为 Uninitialized
func newConsumerStateMachine() *consumerStateMachine {
	return &consumerStateMachine{
		state: StateUninitialized,
	}
}

// validTransitions 定义合法的状态转换
// key: 当前状态, value: 允许转换到的状态列表
var validTransitions = map[ConsumerState][]ConsumerState{
	StateUninitialized: {StateJoining, StateStopping},
	StateJoining:       {StateReady, StateRebalancing, StateStopping},
	StateReady:         {StateRebalancing, StateStopping},
	StateRebalancing:   {StateReady, StateJoining, StateStopping}, // 允许恢复到 Joining（重平衡失败时）
	StateStopping:      {StateStopped},
	StateStopped:       {}, // 终态，不能转换到任何状态
}

// Get 获取当前状态
func (sm *consumerStateMachine) Get() ConsumerState {
	return sm.state
}

// Is 检查当前状态是否为指定状态
func (sm *consumerStateMachine) Is(s ConsumerState) bool {
	return sm.Get() == s
}

// IsAny 检查当前状态是否为指定状态之一
func (sm *consumerStateMachine) IsAny(states ...ConsumerState) bool {
	current := sm.Get()
	return slices.Contains(states, current)
}

// CanTransitionTo 检查是否可以转换到目标状态
func (sm *consumerStateMachine) CanTransitionTo(to ConsumerState) bool {
	return sm.canTransitionToLocked(to)
}

func (sm *consumerStateMachine) canTransitionToLocked(to ConsumerState) bool {
	allowed, ok := validTransitions[sm.state]
	if !ok {
		return false
	}
	return slices.Contains(allowed, to)
}

// ErrInvalidStateTransition 表示非法的状态转换
type ErrInvalidStateTransition struct {
	From ConsumerState
	To   ConsumerState
}

func (e *ErrInvalidStateTransition) Error() string {
	return fmt.Sprintf("invalid state transition: %s -> %s", e.From, e.To)
}

// Transition 尝试转换到目标状态
// 如果转换不合法，返回 ErrInvalidStateTransition
func (sm *consumerStateMachine) Transition(to ConsumerState) error {
	if !sm.canTransitionToLocked(to) {
		return &ErrInvalidStateTransition{From: sm.state, To: to}
	}

	from := sm.state
	sm.state = to

	if sm.onTransition != nil {
		sm.onTransition(from, to)
	}

	return nil
}

// TransitionIf 仅当当前状态为 expectedFrom 时才转换
// 返回是否成功转换
func (sm *consumerStateMachine) TransitionIf(expectedFrom, to ConsumerState) bool {
	if sm.state != expectedFrom {
		return false
	}

	if !sm.canTransitionToLocked(to) {
		return false
	}

	from := sm.state
	sm.state = to

	if sm.onTransition != nil {
		sm.onTransition(from, to)
	}

	return true
}

// MustTransition 强制转换到目标状态，如果不合法则 panic
// 仅用于确定状态转换一定合法的场景
func (sm *consumerStateMachine) MustTransition(to ConsumerState) {
	if err := sm.Transition(to); err != nil {
		panic(err)
	}
}

// SetOnTransition 设置状态转换时的回调函数
func (sm *consumerStateMachine) SetOnTransition(fn func(from, to ConsumerState)) {
	sm.onTransition = fn
}

// IsReady 检查消费者是否处于就绪状态
func (sm *consumerStateMachine) IsReady() bool {
	return sm.Is(StateReady)
}

// IsRebalancing 检查消费者是否正在重平衡
func (sm *consumerStateMachine) IsRebalancing() bool {
	return sm.Is(StateRebalancing)
}

// IsStopped 检查消费者是否已停止
func (sm *consumerStateMachine) IsStopped() bool {
	return sm.Is(StateStopped)
}

// CanPoll 检查消费者是否可以进行 Poll 操作
// 只有在 Ready 状态才能 Poll
func (sm *consumerStateMachine) CanPoll() bool {
	return sm.Is(StateReady)
}

// CanConsume 检查消费者是否可以消费消息
// Ready 和 Rebalancing 状态都可能需要处理未完成的消息
func (sm *consumerStateMachine) CanConsume() bool {
	return sm.IsAny(StateReady, StateRebalancing)
}

// forceSetState 强制设置状态（仅用于测试）
// 警告：此方法绕过状态转换验证，仅应在测试中使用
func (sm *consumerStateMachine) forceSetState(s ConsumerState) {
	sm.state = s
}
