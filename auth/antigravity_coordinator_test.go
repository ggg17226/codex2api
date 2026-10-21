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
