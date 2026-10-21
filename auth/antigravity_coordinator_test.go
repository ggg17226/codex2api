package auth

import (
	"context"
	"testing"
	"time"
)

func TestAntigravityCoordinator_DualChannelAndFlightMutualExclusion(t *testing.T) {
	coord := NewAntigravityCoordinator(4)
	ctx := context.Background()

	// 1. 同一账号 flight 互斥
	lease1, err := coord.AcquireFlight(ctx, 101, "background_summary")
	if err != nil {
		t.Fatalf("acquire lease1 failed: %v", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = coord.AcquireFlight(timeoutCtx, 101, "manual_refresh")
	if err == nil {
		t.Fatal("expected second acquire on same account to block and timeout")
	}
	lease1.Release()

	// 2. 交互通道不占后台 4 并发许可
	// 占满 4 个后台许可
	var releases []func()
	for i := 0; i < 4; i++ {
		rel, err := coord.AcquirePermit(ctx, "background_summary")
		if err != nil {
			t.Fatalf("acquire background permit %d failed: %v", i, err)
		}
		releases = append(releases, rel)
	}

	// 第 5 个后台许可应阻塞（用新鲜的短超时 context，验证许可饱和阻塞而非 context 取消）
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer blockedCancel()
	_, err = coord.AcquirePermit(blockedCtx, "background_summary")
	if err == nil {
		t.Fatal("expected 5th background permit to block until timeout")
	}

	// 交互通道不受后台 4 许可限制，可立即获取
	relInteractive, err := coord.AcquirePermit(ctx, "http_401_recovery")
	if err != nil {
		t.Fatalf("interactive permit should not be blocked: %v", err)
	}
	relInteractive()

	for _, rel := range releases {
		rel()
	}
}

func TestAntigravityCoordinator_LeaseReleaseIsIdempotent(t *testing.T) {
	coord := NewAntigravityCoordinator(4)
	ctx := context.Background()

	lease, err := coord.AcquireFlight(ctx, 301, "background_summary")
	if err != nil {
		t.Fatalf("acquire lease failed: %v", err)
	}

	// 同一 lease 重复 Release 必须幂等：不得 panic，也不得产生副作用。
	lease.Release()
	lease.Release()
	lease.Release()

	// 释放后账号状态干净：新的获取者应立即成功。
	freshCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	next, err := coord.AcquireFlight(freshCtx, 301, "background_summary")
	if err != nil {
		t.Fatalf("acquire after release should succeed immediately: %v", err)
	}

	// 已释放的旧 lease 再次 Release 不得影响 next 的持有权。
	lease.Release()

	blockedCtx, blockedCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer blockedCancel()
	if _, err := coord.AcquireFlight(blockedCtx, 301, "manual_refresh"); err == nil {
		t.Fatal("stale release of an already released lease must not free the account")
	}

	next.Release()
}

func TestAntigravityCoordinator_StaleReleaseDoesNotBreakNewLease(t *testing.T) {
	coord := NewAntigravityCoordinator(4)
	ctx := context.Background()

	stale, err := coord.AcquireFlight(ctx, 401, "background_summary")
	if err != nil {
		t.Fatalf("acquire stale lease failed: %v", err)
	}
	stale.Release()

	fresh, err := coord.AcquireFlight(ctx, 401, "background_summary")
	if err != nil {
		t.Fatalf("acquire fresh lease after release failed: %v", err)
	}

	// 越权释放：旧 lease（对应已被回收的 channel）不得解开新 lease。
	stale.Release()

	blockedCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := coord.AcquireFlight(blockedCtx, 401, "manual_refresh"); err == nil {
		t.Fatal("stale lease release must not break mutual exclusion of the current holder")
	}

	// 真正持有者释放后，后续获取者应能立即成功。
	fresh.Release()

	acquireCtx, acquireCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer acquireCancel()
	next, err := coord.AcquireFlight(acquireCtx, 401, "background_summary")
	if err != nil {
		t.Fatalf("acquire after real release should succeed: %v", err)
	}
	next.Release()
}

func TestAntigravityCoordinator_ReleaseWakesBlockedWaiter(t *testing.T) {
	coord := NewAntigravityCoordinator(4)
	ctx := context.Background()

	holder, err := coord.AcquireFlight(ctx, 501, "background_summary")
	if err != nil {
		t.Fatalf("acquire holder lease failed: %v", err)
	}

	waiterDone := make(chan *FlightLease, 1)
	go func() {
		lease, err := coord.AcquireFlight(context.Background(), 501, "background_summary")
		if err != nil {
			t.Errorf("waiter acquire failed: %v", err)
			waiterDone <- nil
			return
		}
		waiterDone <- lease
	}()

	select {
	case <-waiterDone:
		t.Fatal("waiter must stay blocked while the account flight is held")
	case <-time.After(50 * time.Millisecond):
	}

	holder.Release()

	select {
	case lease := <-waiterDone:
		if lease == nil {
			t.Fatal("waiter acquired a nil lease")
		}
		lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("blocked waiter was not woken up after the lease was released")
	}
}
