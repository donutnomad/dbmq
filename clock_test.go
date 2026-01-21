package dbmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClock_Now(t *testing.T) {
	clock := NewRealClock()
	before := time.Now()
	now := clock.Now()
	after := time.Now()

	assert.True(t, !now.Before(before), "clock.Now() should be >= before")
	assert.True(t, !now.After(after), "clock.Now() should be <= after")
}

func TestRealClock_After(t *testing.T) {
	clock := NewRealClock()
	start := time.Now()

	ch := clock.After(50 * time.Millisecond)
	<-ch

	elapsed := time.Since(start)
	assert.True(t, elapsed >= 50*time.Millisecond, "should wait at least 50ms")
}

func TestRealClock_NewTicker(t *testing.T) {
	clock := NewRealClock()
	ticker := clock.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// 等待第一次 tick
	select {
	case <-ticker.C():
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ticker did not tick within timeout")
	}
}

func TestRealClock_NewTimer(t *testing.T) {
	clock := NewRealClock()
	timer := clock.NewTimer(50 * time.Millisecond)

	select {
	case <-timer.C():
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within timeout")
	}
}

func TestFakeClock_Now(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	assert.Equal(t, start, clock.Now())

	clock.Advance(1 * time.Hour)
	assert.Equal(t, start.Add(1*time.Hour), clock.Now())
}

func TestFakeClock_After(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	ch := clock.After(1 * time.Hour)

	// 还没到时间，不应该触发
	select {
	case <-ch:
		t.Fatal("should not receive before advance")
	default:
		// OK
	}

	// 推进时间
	clock.Advance(30 * time.Minute)
	select {
	case <-ch:
		t.Fatal("should not receive before deadline")
	default:
		// OK
	}

	// 推进到刚好到期
	clock.Advance(30 * time.Minute)
	select {
	case received := <-ch:
		assert.Equal(t, start.Add(1*time.Hour), received)
	default:
		t.Fatal("should receive after advance past deadline")
	}
}

func TestFakeClock_NewTimer(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	timer := clock.NewTimer(1 * time.Hour)

	// 还没到时间
	select {
	case <-timer.C():
		t.Fatal("should not fire before advance")
	default:
		// OK
	}

	// 推进时间触发
	clock.Advance(1 * time.Hour)
	select {
	case received := <-timer.C():
		assert.Equal(t, start.Add(1*time.Hour), received)
	default:
		t.Fatal("timer should fire after advance")
	}

	// 重复推进不应该再次触发
	clock.Advance(1 * time.Hour)
	select {
	case <-timer.C():
		t.Fatal("timer should not fire twice")
	default:
		// OK
	}
}

func TestFakeClock_Timer_Stop(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	timer := clock.NewTimer(1 * time.Hour)

	// 停止应该返回 true（成功停止）
	stopped := timer.Stop()
	assert.True(t, stopped)

	// 再次停止应该返回 false
	stopped = timer.Stop()
	assert.False(t, stopped)

	// 推进时间后不应该触发
	clock.Advance(2 * time.Hour)
	select {
	case <-timer.C():
		t.Fatal("stopped timer should not fire")
	default:
		// OK
	}
}

func TestFakeClock_Timer_Reset(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	timer := clock.NewTimer(1 * time.Hour)

	// 重置为更长的时间
	wasActive := timer.Reset(2 * time.Hour)
	assert.True(t, wasActive)

	// 推进1小时不应该触发
	clock.Advance(1 * time.Hour)
	select {
	case <-timer.C():
		t.Fatal("timer should not fire before new deadline")
	default:
		// OK
	}

	// 推进到新的deadline
	clock.Advance(1 * time.Hour)
	select {
	case <-timer.C():
		// OK
	default:
		t.Fatal("timer should fire after new deadline")
	}
}

func TestFakeClock_NewTicker(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	ticker := clock.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// 还没到时间
	select {
	case <-ticker.C():
		t.Fatal("should not tick before advance")
	default:
		// OK
	}

	// 推进1小时，应该触发1次
	clock.Advance(1 * time.Hour)
	select {
	case <-ticker.C():
		// OK
	default:
		t.Fatal("ticker should tick after 1 hour")
	}

	// 不应该有第二次
	select {
	case <-ticker.C():
		t.Fatal("ticker should only tick once per interval")
	default:
		// OK
	}

	// 再推进1小时，应该再触发1次
	clock.Advance(1 * time.Hour)
	select {
	case <-ticker.C():
		// OK
	default:
		t.Fatal("ticker should tick again after another hour")
	}
}

func TestFakeClock_Ticker_Stop(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	ticker := clock.NewTicker(1 * time.Hour)
	ticker.Stop()

	// 停止后推进时间不应该触发
	clock.Advance(2 * time.Hour)
	select {
	case <-ticker.C():
		t.Fatal("stopped ticker should not tick")
	default:
		// OK
	}
}

func TestFakeClock_Ticker_Reset(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	ticker := clock.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	// 重置为30分钟
	ticker.Reset(30 * time.Minute)

	// 推进30分钟应该触发
	clock.Advance(30 * time.Minute)
	select {
	case <-ticker.C():
		// OK
	default:
		t.Fatal("ticker should tick after reset interval")
	}
}

func TestFakeClock_MultipleTimers(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	timer1 := clock.NewTimer(1 * time.Hour)
	timer2 := clock.NewTimer(2 * time.Hour)
	timer3 := clock.NewTimer(30 * time.Minute)

	// 推进45分钟，只有 timer3 应该触发
	clock.Advance(45 * time.Minute)

	var fired []int
	select {
	case <-timer1.C():
		fired = append(fired, 1)
	default:
	}
	select {
	case <-timer2.C():
		fired = append(fired, 2)
	default:
	}
	select {
	case <-timer3.C():
		fired = append(fired, 3)
	default:
	}

	require.Equal(t, []int{3}, fired, "only timer3 should have fired")

	// 推进到1小时15分钟，timer1 应该触发
	clock.Advance(30 * time.Minute)

	select {
	case <-timer1.C():
		// OK
	default:
		t.Fatal("timer1 should have fired")
	}

	select {
	case <-timer2.C():
		t.Fatal("timer2 should not have fired yet")
	default:
		// OK
	}
}

func TestFakeClock_Set(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)

	newTime := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC)
	clock.Set(newTime)

	assert.Equal(t, newTime, clock.Now())
}
