package auth

import (
	"testing"
	"time"
)

func TestFakeClock_AdvanceAndTimer(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	if !clk.Now().Equal(start) {
		t.Fatalf("expected now %v, got %v", start, clk.Now())
	}

	timer := clk.NewTimer(10 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("timer triggered prematurely")
	default:
	}

	clk.Advance(5 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("timer triggered at 5m")
	default:
	}

	clk.Advance(5 * time.Minute)
	select {
	case fireTime := <-timer.C():
		expected := start.Add(10 * time.Minute)
		if !fireTime.Equal(expected) {
			t.Fatalf("expected fire time %v, got %v", expected, fireTime)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timer failed to fire after reaching deadline")
	}
}

func TestFakeClock_TimerResetAndStop(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)

	timer := clk.NewTimer(10 * time.Minute)
	clk.Advance(5 * time.Minute)
	stopped := timer.Stop()
	if !stopped {
		t.Fatal("expected timer.Stop() to return true")
	}

	clk.Advance(10 * time.Minute)
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}

	timer.Reset(2 * time.Minute)
	clk.Advance(2 * time.Minute)
	select {
	case <-timer.C():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("reset timer failed to fire")
	}
}
