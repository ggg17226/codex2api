package auth

import (
	"sync"
	"time"
)

// Timer 是可注入的定时器抽象，语义对齐 time.Timer 的常用子集。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Clock 是可注入的时钟抽象，生产路径使用 RealClock，测试路径使用 FakeClock。
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
	NewTimer(d time.Duration) Timer
}

// RealClock 是基于 time 包的真实时钟实现。
type RealClock struct{}

func (RealClock) Now() time.Time                  { return time.Now() }
func (RealClock) Sleep(d time.Duration)           { time.Sleep(d) }
func (RealClock) Since(t time.Time) time.Duration { return time.Since(t) }
func (RealClock) Until(t time.Time) time.Duration { return time.Until(t) }
func (RealClock) NewTimer(d time.Duration) Timer  { return &realTimer{Timer: time.NewTimer(d)} }

type realTimer struct {
	*time.Timer
}

func (r *realTimer) C() <-chan time.Time {
	return r.Timer.C
}

// DefaultClock 是全局默认时钟，测试中可替换为 FakeClock。
var DefaultClock Clock = RealClock{}

// fakeTimer 是 FakeClock 内部的定时器实现，由 FakeClock.mu 保护。
type fakeTimer struct {
	clk      *FakeClock
	ch       chan time.Time
	deadline time.Time
	active   bool
}

func (ft *fakeTimer) C() <-chan time.Time {
	return ft.ch
}

func (ft *fakeTimer) Stop() bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.active = false
	return wasActive
}

func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.clk.mu.Lock()
	defer ft.clk.mu.Unlock()
	wasActive := ft.active
	ft.deadline = ft.clk.now.Add(d)
	ft.active = true
	return wasActive
}

// FakeClock 是确定性测试时钟：时间仅由 Advance 显式推进。
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock 创建以 initial 为起点的假时钟。
func NewFakeClock(initial time.Time) *FakeClock {
	return &FakeClock{now: initial}
}

func (fc *FakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.now
}

func (fc *FakeClock) Sleep(d time.Duration) {
	fc.Advance(d)
}

func (fc *FakeClock) Since(t time.Time) time.Duration {
	return fc.Now().Sub(t)
}

func (fc *FakeClock) Until(t time.Time) time.Duration {
	return t.Sub(fc.Now())
}

func (fc *FakeClock) NewTimer(d time.Duration) Timer {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	ft := &fakeTimer{
		clk:      fc,
		ch:       make(chan time.Time, 1),
		deadline: fc.now.Add(d),
		active:   true,
	}
	fc.timers = append(fc.timers, ft)
	return ft
}

// Advance 将假时钟时间向前推进 d，并唤醒所有到达 deadline 的活跃定时器。
func (fc *FakeClock) Advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.now = fc.now.Add(d)
	for _, ft := range fc.timers {
		if ft.active && !fc.now.Before(ft.deadline) {
			ft.active = false
			select {
			case ft.ch <- ft.deadline:
			default:
			}
		}
	}
}
