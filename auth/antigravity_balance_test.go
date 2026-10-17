package auth

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/codex2api/database"
)

// newAntigravityBalanceStore 构造一个 round_robin + 加权避险开启的号池，账号均为
// 可调度的 Antigravity OAuth 账号。测试结束恢复全局开关默认值。
func newAntigravityBalanceStore(t *testing.T, accounts int) *Store {
	t.Helper()
	store := NewStore(nil, nil, &database.SystemSettings{MaxConcurrency: 4, TestConcurrency: 1, TestModel: "gemini-3.8-flash-high"})
	store.SetSchedulerMode("round_robin")
	for i := 1; i <= accounts; i++ {
		store.AddAccount(&Account{
			DBID:                 int64(i),
			UpstreamType:         UpstreamAntigravity,
			AccessToken:          fmt.Sprintf("google-at-%d", i),
			AntigravityProjectID: fmt.Sprintf("project-%d", i),
		})
	}
	t.Cleanup(func() { SetAntigravityBalancedAffinity(true) })
	SetAntigravityBalancedAffinity(true)
	return store
}

func antigravitySelectionOpts() SelectionOptions {
	return SelectionOptions{
		Provider: database.UpstreamChannelAntigravity,
		Model:    "gemini-3.8-flash-high",
		Policy:   DispatchPolicyStandard,
	}
}

// 加权避险：4 个健康账号，账号 1 记录一次 429 惩罚后，多次不同 key 的首选必须显著
// 避开账号 1，而其余健康账号仍被正常选中。
func TestAntigravityBalancedSelectionAvoidsPenalizedAccount(t *testing.T) {
	store := newAntigravityBalanceStore(t, 4)
	defer store.Stop()

	opts := antigravitySelectionOpts()
	const trials = 400

	// 控制组：未记惩罚时四个健康账号应大致摊开，证明选号本身没有把账号 1 天然压到队尾。
	control := map[int64]int{}
	for i := 0; i < trials; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch(fmt.Sprintf("control-%d", i), 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("control selection returned no account")
		}
		control[acc.DBID]++
		store.Release(acc)
	}
	if len(control) < 2 {
		t.Fatalf("control selections did not spread across accounts: %v", control)
	}
	if control[1] == 0 {
		t.Fatalf("control run already starves account 1 before any penalty: %v", control)
	}

	penalized := store.FindByID(1)
	if penalized == nil {
		t.Fatal("penalized account 1 not found")
	}
	RecordAntigravityRateLimitEvent(penalized, AntigravityRateLimitEvent{
		StatusCode: 429,
		Kind:       "rate_limit",
		Scope:      AGScopeModelRateLimit,
		Weight:     AntigravityPenaltyWeightExplicitRateLimit,
		ObservedAt: time.Now(),
	})

	treatment := map[int64]int{}
	for i := 0; i < trials; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch(fmt.Sprintf("treatment-%d", i), 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("treatment selection returned no account")
		}
		treatment[acc.DBID]++
		store.Release(acc)
	}

	minHealthy := -1
	for id, count := range treatment {
		if id == 1 {
			continue
		}
		if count == 0 {
			t.Fatalf("healthy account %d was never selected after penalizing account 1: %v", id, treatment)
		}
		if minHealthy < 0 || count < minHealthy {
			minHealthy = count
		}
	}
	if minHealthy <= 0 {
		t.Fatalf("no healthy account selected in treatment run: %v", treatment)
	}
	// 账号 1 的选中次数必须显著低于最少的健康账号（阈值：不足其 1/4）。
	if treatment[1]*4 >= minHealthy {
		t.Fatalf("penalized account 1 not avoided enough: %v (want count[1] well below %d)", treatment, minHealthy)
	}
}

// 同分（全部空闲、无惩罚）时用 (key, DBID) 哈希做确定性打散：同一 key 恒命中同一
// 账号，不同 key 摊到多个账号。
func TestAntigravityBalancedSelectionDeterministicAndSpread(t *testing.T) {
	store := newAntigravityBalanceStore(t, 4)
	defer store.Stop()
	opts := antigravitySelectionOpts()

	const key = "affinity-stable"
	var first int64
	for i := 0; i < 6; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch(key, 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("no account selected")
		}
		if first == 0 {
			first = acc.DBID
		} else if acc.DBID != first {
			t.Fatalf("same key drifted across selections: got %d want %d", acc.DBID, first)
		}
		store.Release(acc)
	}

	seen := map[int64]bool{}
	for i := 0; i < 60; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch(fmt.Sprintf("spread-%d", i), 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("no account selected")
		}
		seen[acc.DBID] = true
		store.Release(acc)
	}
	if len(seen) < 2 {
		t.Fatalf("distinct keys did not spread across accounts: %v", seen)
	}
}

// 单轮求值保护：加权选号对每个候选账号的 filter(acc) 至多求值一次。用「同一账号第二次
// 求值即 panic」的 filter 直接验证——若发生重复求值，本测试会以 panic 失败。
func TestAntigravityBalancedSelectionEvaluatesFilterOnce(t *testing.T) {
	store := newAntigravityBalanceStore(t, 4)
	defer store.Stop()
	opts := antigravitySelectionOpts()

	var seen sync.Map
	filter := func(acc *Account) bool {
		if acc == nil {
			return false
		}
		if _, loaded := seen.LoadOrStore(acc.DBID, struct{}{}); loaded {
			panic(fmt.Sprintf("filter evaluated more than once for account %d", acc.DBID))
		}
		return true
	}
	acc := store.nextAccountForFreshAffinityWithDispatch("single-eval", 0, nil, filter, opts)
	if acc == nil {
		t.Fatal("no account selected")
	}
	store.Release(acc)
}

// 触发门控：仅 provider=antigravity + 开关开启 + round_robin 三者同时满足才走加权路径。
// 其余组合必须返回 handled=false，让调用方继续走既有 spread / HRW / round-robin 逻辑。
func TestAntigravityBalancedSelectionGating(t *testing.T) {
	store := newAntigravityBalanceStore(t, 2)
	defer store.Stop()

	if acc, handled := store.selectAntigravityBalancedAccount("k", 0, nil, nil, SelectionOptions{Provider: database.UpstreamChannelCodex, Policy: DispatchPolicyStandard}); acc != nil || handled {
		t.Fatal("non-antigravity provider entered weighted selection")
	}
	if acc, handled := store.selectAntigravityBalancedAccount("k", 0, nil, nil, SelectionOptions{Policy: DispatchPolicyStandard}); acc != nil || handled {
		t.Fatal("empty provider entered weighted selection")
	}

	acc, handled := store.selectAntigravityBalancedAccount("k", 0, nil, nil, antigravitySelectionOpts())
	if !handled {
		t.Fatal("antigravity provider did not take over the selection")
	}
	if acc == nil {
		t.Fatal("antigravity provider did not select an account")
	}
	store.Release(acc)

	SetAntigravityBalancedAffinity(false)
	if acc, handled := store.selectAntigravityBalancedAccount("k", 0, nil, nil, antigravitySelectionOpts()); acc != nil || handled {
		t.Fatal("weighted selection stayed active after disabling the switch")
	}
	SetAntigravityBalancedAffinity(true)

	store.SetSchedulerMode("fill_first")
	if acc, handled := store.selectAntigravityBalancedAccount("k", 0, nil, nil, antigravitySelectionOpts()); acc != nil || handled {
		t.Fatal("weighted selection stayed active outside round_robin")
	}
	store.SetSchedulerMode("round_robin")
}

// 加权路径必须尊重 exclude：被排除的账号即使分数最低也不能被选中。
func TestAntigravityBalancedSelectionRespectsExclude(t *testing.T) {
	store := newAntigravityBalanceStore(t, 3)
	defer store.Stop()
	opts := antigravitySelectionOpts()

	for i := 0; i < 40; i++ {
		acc, handled := store.selectAntigravityBalancedAccount(fmt.Sprintf("excl-%d", i), 0, map[int64]bool{1: true, 2: true}, nil, opts)
		if !handled {
			t.Fatal("weighted selection did not take over")
		}
		if acc == nil {
			t.Fatal("no account selected")
		}
		if acc.DBID == 1 || acc.DBID == 2 {
			t.Fatalf("excluded account %d was selected", acc.DBID)
		}
		store.Release(acc)
	}
}

// saturateAntigravityAccount 把账号的占用/活跃并发顶到任何档位上限之上，模拟号池已满。
func saturateAntigravityAccount(acc *Account) {
	if acc == nil {
		return
	}
	acc.ActiveRequests.Store(64)
	acc.OccupiedRequests.Store(64)
}

// 饱和池：饱和号池下加权路径必须自己接管本次选号——返回 nil 且不得让调用方回退重扫。
// 用「同一账号第二次求值即 panic」的 filter 断言 filter 每个账号至多一次；spread 开与关
// 两条回退路径都要覆盖（回退分别落在 NextExcludingWithDispatch 与 HRW 候选扫描）。
func TestAntigravityBalancedSelectionSaturatedPoolDoesNotRefilter(t *testing.T) {
	for _, spread := range []bool{false, true} {
		name := "spread-off"
		if spread {
			name = "spread-on"
		}
		t.Run(name, func(t *testing.T) {
			store := newAntigravityBalanceStore(t, 4)
			defer store.Stop()
			store.SetSessionAffinitySpread(spread)
			for _, acc := range store.Accounts() {
				saturateAntigravityAccount(acc)
			}

			var mu sync.Mutex
			evaluated := map[int64]int{}
			filter := func(acc *Account) bool {
				if acc == nil {
					return false
				}
				mu.Lock()
				defer mu.Unlock()
				evaluated[acc.DBID]++
				if evaluated[acc.DBID] > 1 {
					panic(fmt.Sprintf("filter evaluated %d times for account %d", evaluated[acc.DBID], acc.DBID))
				}
				return true
			}

			if acc := store.nextAccountForFreshAffinityWithDispatch("saturated", 0, nil, filter, antigravitySelectionOpts()); acc != nil {
				t.Fatalf("a saturated pool must not hand out account %d", acc.DBID)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(evaluated) == 0 {
				t.Fatal("the weighted path never evaluated the filter")
			}
			for dbID, count := range evaluated {
				if count > 1 {
					t.Fatalf("account %d evaluated the filter %d times, want at most 1", dbID, count)
				}
			}
		})
	}
}

// 饱和池：handled=true 但整层拿不下时返回 nil；加权路径内部按 (调度优先级, 健康档位)
// 逐层下沉，最高层全满必须继续尝试次高层，而不是直接返回 nil。
func TestAntigravityBalancedSelectionFallsBackToLowerPriorityLayer(t *testing.T) {
	store := newAntigravityBalanceStore(t, 3)
	defer store.Stop()
	opts := antigravitySelectionOpts()

	high := int64(10)
	if !store.ApplyAccountSchedulerPriority(1, &high) {
		t.Fatal("failed to raise the priority of account 1")
	}
	t.Cleanup(func() { store.ApplyAccountSchedulerPriority(1, nil) })

	// 未饱和时最高层独占：同一批 key 只能命中账号 1。
	for i := 0; i < 20; i++ {
		acc, handled := store.selectAntigravityBalancedAccount(fmt.Sprintf("prio-%d", i), 0, nil, nil, opts)
		if !handled {
			t.Fatal("weighted selection did not take over")
		}
		if acc == nil {
			t.Fatal("no account selected")
		}
		if acc.DBID != 1 {
			t.Fatalf("lower priority account %d won while the top layer was free", acc.DBID)
		}
		store.Release(acc)
	}

	// 最高层全满：必须下沉到次高层，而不是返回 nil。
	saturateAntigravityAccount(store.FindByID(1))
	seen := map[int64]bool{}
	for i := 0; i < 20; i++ {
		acc, handled := store.selectAntigravityBalancedAccount(fmt.Sprintf("sunk-%d", i), 0, nil, nil, opts)
		if !handled {
			t.Fatal("weighted selection did not take over")
		}
		if acc == nil {
			t.Fatal("a free lower layer must still be selected")
		}
		if acc.DBID == 1 {
			t.Fatal("the saturated top layer handed out an account")
		}
		seen[acc.DBID] = true
		store.Release(acc)
	}
	if len(seen) == 0 {
		t.Fatal("no lower-layer account was reachable")
	}
}

// 饱和池：handled=false（非 Antigravity 渠道）时旧路径行为不变——散列绑定仍然生效，
// 同一亲和键幂等命中同一账号，不同键摊到多个账号。
func TestAntigravityFreshAffinityLegacyPathForNonAntigravity(t *testing.T) {
	store := newAntigravityBalanceStore(t, 4)
	defer store.Stop()
	store.SetSessionAffinitySpread(true)
	t.Cleanup(func() { store.SetSessionAffinitySpread(false) })

	opts := SelectionOptions{Provider: database.UpstreamChannelCodex, Model: "gpt-5.4", Policy: DispatchPolicyStandard}

	var first int64
	for i := 0; i < 6; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch("legacy-stable", 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("legacy path selected no account")
		}
		if first == 0 {
			first = acc.DBID
		} else if acc.DBID != first {
			t.Fatalf("legacy hash binding drifted: got %d want %d", acc.DBID, first)
		}
		store.Release(acc)
	}

	seen := map[int64]bool{}
	for i := 0; i < 60; i++ {
		acc := store.nextAccountForFreshAffinityWithDispatch(fmt.Sprintf("legacy-spread-%d", i), 0, nil, nil, opts)
		if acc == nil {
			t.Fatal("legacy path selected no account")
		}
		seen[acc.DBID] = true
		store.Release(acc)
	}
	if len(seen) < 2 {
		t.Fatalf("legacy path stopped spreading across accounts: %v", seen)
	}
}
