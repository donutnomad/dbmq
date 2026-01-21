package dbmq

import (
	"sync"
	"time"
)

// Clock 抽象时间操作的接口
// 用于在测试中注入 FakeClock，控制时间推进
type Clock interface {
	// Now 返回当前时间
	Now() time.Time
	// After 返回一个 channel，在指定时间后发送当前时间
	After(d time.Duration) <-chan time.Time
	// NewTicker 创建一个定时触发的 Ticker
	NewTicker(d time.Duration) Ticker
	// NewTimer 创建一个单次触发的 Timer
	NewTimer(d time.Duration) Timer
}

// Ticker 抽象定时器接口
type Ticker interface {
	// C 返回 tick channel
	C() <-chan time.Time
	// Stop 停止 ticker
	Stop()
	// Reset 重置 ticker 的间隔
	Reset(d time.Duration)
}

// Timer 抽象单次计时器接口
type Timer interface {
	// C 返回触发 channel
	C() <-chan time.Time
	// Stop 停止 timer，返回是否成功停止（false 表示已经触发或已停止）
	Stop() bool
	// Reset 重置 timer
	Reset(d time.Duration) bool
}

// ============== RealClock 实现 ==============

// RealClock 使用标准库 time 的真实时钟实现
type RealClock struct{}

// NewRealClock 创建一个真实时钟
func NewRealClock() *RealClock {
	return &RealClock{}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (c *RealClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{ticker: time.NewTicker(d)}
}

func (c *RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(d)}
}

// realTicker 包装 time.Ticker
type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *realTicker) Stop() {
	t.ticker.Stop()
}

func (t *realTicker) Reset(d time.Duration) {
	t.ticker.Reset(d)
}

// realTimer 包装 time.Timer
type realTimer struct {
	timer *time.Timer
}

func (t *realTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *realTimer) Stop() bool {
	return t.timer.Stop()
}

func (t *realTimer) Reset(d time.Duration) bool {
	return t.timer.Reset(d)
}

// ============== FakeClock 实现 ==============

// FakeClock 用于测试的可控时钟
// 支持手动推进时间，触发所有到期的 timer 和 ticker
type FakeClock struct {
	mu      sync.Mutex
	current time.Time
	timers  []*fakeTimer
	tickers []*fakeTicker
	afters  []*fakeAfter
}

// NewFakeClock 创建一个测试用的假时钟
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{
		current: start,
	}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	fa := &fakeAfter{
		deadline: c.current.Add(d),
		ch:       ch,
	}
	c.afters = append(c.afters, fa)
	return ch
}

func (c *FakeClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()

	ft := &fakeTicker{
		clock:    c,
		interval: d,
		nextTick: c.current.Add(d),
		ch:       make(chan time.Time, 1),
		stopped:  false,
	}
	c.tickers = append(c.tickers, ft)
	return ft
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	ft := &fakeTimer{
		clock:    c,
		deadline: c.current.Add(d),
		ch:       make(chan time.Time, 1),
		stopped:  false,
		fired:    false,
	}
	c.timers = append(c.timers, ft)
	return ft
}

// Advance 推进时间，触发所有到期的 timer、ticker 和 after
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newTime := c.current.Add(d)
	c.current = newTime

	// 触发到期的 afters
	var remainingAfters []*fakeAfter
	for _, fa := range c.afters {
		if !newTime.Before(fa.deadline) {
			select {
			case fa.ch <- newTime:
			default:
			}
		} else {
			remainingAfters = append(remainingAfters, fa)
		}
	}
	c.afters = remainingAfters

	// 触发到期的 timers
	for _, ft := range c.timers {
		if !ft.stopped && !ft.fired && !newTime.Before(ft.deadline) {
			ft.fired = true
			select {
			case ft.ch <- newTime:
			default:
			}
		}
	}

	// 触发到期的 tickers
	for _, ft := range c.tickers {
		if !ft.stopped {
			for !newTime.Before(ft.nextTick) {
				select {
				case ft.ch <- ft.nextTick:
				default:
				}
				ft.nextTick = ft.nextTick.Add(ft.interval)
			}
		}
	}
}

// Set 直接设置当前时间（不触发 timers）
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = t
}

// fakeAfter 用于 After() 的假实现
type fakeAfter struct {
	deadline time.Time
	ch       chan time.Time
}

// fakeTicker 用于测试的假 Ticker
type fakeTicker struct {
	clock    *FakeClock
	interval time.Duration
	nextTick time.Time
	ch       chan time.Time
	stopped  bool
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.stopped = true
}

func (t *fakeTicker) Reset(d time.Duration) {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.interval = d
	t.nextTick = t.clock.current.Add(d)
	t.stopped = false
}

// fakeTimer 用于测试的假 Timer
type fakeTimer struct {
	clock    *FakeClock
	deadline time.Time
	ch       chan time.Time
	stopped  bool
	fired    bool
}

func (t *fakeTimer) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.deadline = t.clock.current.Add(d)
	t.stopped = false
	t.fired = false
	return wasActive
}

// 编译时检查接口实现
var (
	_ Clock  = (*RealClock)(nil)
	_ Clock  = (*FakeClock)(nil)
	_ Ticker = (*realTicker)(nil)
	_ Ticker = (*fakeTicker)(nil)
	_ Timer  = (*realTimer)(nil)
	_ Timer  = (*fakeTimer)(nil)
)
