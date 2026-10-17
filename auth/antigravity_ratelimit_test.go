package auth

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/codex2api/cache"
	"github.com/codex2api/database"
)

// antigravityGeminiFamilyKey 与 proxy.antigravityGeminiFamilyCooldownKey 同一个
// 合成键；auth 包不能反向依赖 proxy，这里按字面量断言。
const antigravityGeminiFamilyKey = "antigravity:gemini"

func antigravityPenaltyClose(t *testing.T, got, want float64, label string) {
	t.Helper()
	tolerance := 1e-6
	if want == 0 {
		tolerance = 1e-9
	}
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func TestAntigravityPenaltyDecaysWithHalfLife(t *testing.T) {
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	acc := &Account{DBID: 8801, AccessToken: "token"}
	if got := acc.AntigravityPenalty(base); got != 0 {
		t.Fatalf("fresh account penalty = %v, want 0", got)
	}

	RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{
		StatusCode:  429,
		Kind:        "rate_limit",
		Scope:       AGScopeModelRateLimit,
		PublicModel: "gemini-3.8-flash-high",
		WireModel:   "gemini-3.8-flash-tiered",
		Family:      antigravityGeminiFamilyKey,
		Weight:      AntigravityPenaltyWeightExplicitRateLimit,
		ObservedAt:  base,
	})

	antigravityPenaltyClose(t, acc.AntigravityPenalty(base), 1.0, "penalty at t0")
	antigravityPenaltyClose(t, acc.AntigravityPenalty(base.Add(antigravityThrottleHalfLife)), 0.5, "penalty at one half-life")
	antigravityPenaltyClose(t, acc.AntigravityPenalty(base.Add(2*antigravityThrottleHalfLife)), 0.25, "penalty at two half-lives")
	// 窗口之外直接归零，不做「无限尾巴」的长尾惩罚。
	if got := acc.AntigravityPenalty(base.Add(antigravityThrottleWindow)); got != 0 {
		t.Fatalf("penalty outside the window = %v, want 0", got)
	}
	if got := acc.AntigravityPenalty(base.Add(time.Hour)); got != 0 {
		t.Fatalf("penalty one hour later = %v, want 0", got)
	}
	// 记录时刻之后的时间戳按 0 处理，不能让时钟回拨放大惩罚。
	if got := acc.AntigravityPenalty(base.Add(-time.Minute)); got != 1 {
		t.Fatalf("penalty before the record = %v, want the un-decayed 1.0", got)
	}

	acc.ResetAntigravityPenalty()
	if got := acc.AntigravityPenalty(base); got != 0 {
		t.Fatalf("penalty after reset = %v, want 0", got)
	}
}

func TestAntigravityPenaltyAccumulatesAndCaps(t *testing.T) {
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	acc := &Account{DBID: 8802, AccessToken: "token"}

	RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Weight: 1.0, ObservedAt: base})
	RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Weight: 1.0, ObservedAt: base.Add(10 * time.Second)})

	want := 1*antigravityThrottleDecay(10*time.Second) + 1
	antigravityPenaltyClose(t, acc.AntigravityPenalty(base.Add(10*time.Second)), want, "accumulated penalty")
	if want <= 1 || want >= 2 {
		t.Fatalf("accumulated penalty %v did not decay the previous record", want)
	}

	// 距上次记录超过整个窗口后，旧累加值完全衰减，只留下本次事件。
	RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Weight: 0.5, ObservedAt: base.Add(10 * time.Second)})
	RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Weight: 1.0, ObservedAt: base.Add(10*time.Second + antigravityThrottleWindow)})
	antigravityPenaltyClose(t, acc.AntigravityPenalty(base.Add(10*time.Second+antigravityThrottleWindow)), 1.0, "penalty after the window expired")

	// 累加封顶 4.0：再多的 429 也不会把惩罚推成无界分数。
	for i := 0; i < 20; i++ {
		RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Weight: 1.0, ObservedAt: base.Add(10*time.Second + antigravityThrottleWindow + time.Duration(i)*time.Millisecond)})
	}
	if got := acc.AntigravityPenalty(base.Add(10*time.Second + antigravityThrottleWindow + 19*time.Millisecond)); got > antigravityThrottleMaxPenalty+1e-9 || got < 3.5 {
		t.Fatalf("penalty = %v, want it to saturate at %v", got, antigravityThrottleMaxPenalty)
	}
}

func TestRecordAntigravityRateLimitEventWeights(t *testing.T) {
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	// 共享容量与账号配额不记短期惩罚。
	for _, scope := range []AntigravityRateLimitScope{AGScopeSharedCapacity, AGScopeAccountQuota} {
		acc := &Account{DBID: 8810, AccessToken: "token"}
		RecordAntigravityRateLimitEvent(acc, AntigravityRateLimitEvent{Scope: scope, Weight: AntigravityPenaltyWeightNone, ObservedAt: base})
		if got := acc.AntigravityPenalty(base); got != 0 {
			t.Fatalf("scope %s penalty = %v, want 0", scope, got)
		}
	}

	// 裸 RESOURCE_EXHAUSTED 记半个惩罚，明确限流记一个。
	bare := &Account{DBID: 8811, AccessToken: "token"}
	RecordAntigravityRateLimitEvent(bare, AntigravityRateLimitEvent{Scope: AGScopeModelRateLimit, Weight: AntigravityPenaltyWeightUnclassifiedRateLimit, ObservedAt: base})
	antigravityPenaltyClose(t, bare.AntigravityPenalty(base), 0.5, "bare RESOURCE_EXHAUSTED penalty")

	explicit := &Account{DBID: 8812, AccessToken: "token"}
	RecordAntigravityRateLimitEvent(explicit, AntigravityRateLimitEvent{Scope: AGScopeModelRateLimit, Weight: AntigravityPenaltyWeightExplicitRateLimit, ObservedAt: base})
	antigravityPenaltyClose(t, explicit.AntigravityPenalty(base), 1.0, "explicit rate limit penalty")

	// 权重越界被夹到 [0, 1]，NaN 与负值直接丢弃。
	clamped := &Account{DBID: 8813, AccessToken: "token"}
	RecordAntigravityRateLimitEvent(clamped, AntigravityRateLimitEvent{Weight: 9, ObservedAt: base})
	antigravityPenaltyClose(t, clamped.AntigravityPenalty(base), 1.0, "clamped weight")

	garbage := &Account{DBID: 8814, AccessToken: "token"}
	RecordAntigravityRateLimitEvent(garbage, AntigravityRateLimitEvent{Weight: math.NaN(), ObservedAt: base})
	RecordAntigravityRateLimitEvent(garbage, AntigravityRateLimitEvent{Weight: -1, ObservedAt: base})
	if got := garbage.AntigravityPenalty(base); got != 0 {
		t.Fatalf("invalid weights produced penalty %v, want 0", got)
	}

	// 缺省 ObservedAt 落到当前时刻；nil 账号不 panic。
	fallback := &Account{DBID: 8815, AccessToken: "token"}
	RecordAntigravityRateLimitEvent(fallback, AntigravityRateLimitEvent{Weight: 1.0})
	antigravityPenaltyClose(t, fallback.AntigravityPenalty(time.Now()), 1.0, "penalty with a default observation time")
	RecordAntigravityRateLimitEvent(nil, AntigravityRateLimitEvent{Weight: 1.0})
	var nilAccount *Account
	if got := nilAccount.AntigravityPenalty(time.Now()); got != 0 {
		t.Fatalf("nil account penalty = %v, want 0", got)
	}
}

func TestAntigravityBalancedAffinitySwitch(t *testing.T) {
	previous := GetAntigravityBalancedAffinity()
	t.Cleanup(func() { SetAntigravityBalancedAffinity(previous) })

	SetAntigravityBalancedAffinity(false)
	if GetAntigravityBalancedAffinity() {
		t.Fatal("switch stayed on after SetAntigravityBalancedAffinity(false)")
	}
	SetAntigravityBalancedAffinity(true)
	if !GetAntigravityBalancedAffinity() {
		t.Fatal("switch stayed off after SetAntigravityBalancedAffinity(true)")
	}

	for value, want := range map[string]bool{
		"":      true,
		"1":     true,
		"true":  true,
		"on":    true,
		"weird": true,
		"0":     false,
		"false": false,
		"off":   false,
		"none":  false,
		"no":    false,
		"OFF":   false,
	} {
		t.Setenv(antigravityBalancedAffinityEnv, value)
		ReloadAntigravityBalancedAffinityFromEnv()
		if got := GetAntigravityBalancedAffinity(); got != want {
			t.Fatalf("ANTIGRAVITY_BALANCED_AFFINITY=%q -> %v, want %v", value, got, want)
		}
	}
}

func newAntigravityRateLimitTestStore(t *testing.T, tokenCache cache.TokenCache, accounts ...*Account) *Store {
	t.Helper()
	store := NewStore(nil, tokenCache, &database.SystemSettings{MaxConcurrency: 4, TestConcurrency: 1, TestModel: "gpt-5.4"})
	t.Cleanup(store.Stop)
	for _, acc := range accounts {
		store.AddAccount(acc)
	}
	return store
}

// TestWithAntigravityCooldownFilterContextSyncsSharedCooldowns 覆盖多实例部署：
// 实例 A 写入的 wireModel / Gemini family 冷却，实例 B 只按 publicModel 过滤时看
// 不见，必须通过三键过滤器读到并同步进本地账号。
func TestWithAntigravityCooldownFilterContextSyncsSharedCooldowns(t *testing.T) {
	tokenCache := cache.NewMemory(8)
	defer tokenCache.Close()

	account := &Account{DBID: 8820, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady}
	writing := newAntigravityRateLimitTestStore(t, tokenCache, account)
	reading := newAntigravityRateLimitTestStore(t, tokenCache)

	writing.MarkModelCooldownUntil(account, "gemini-3.8-flash-tiered", "rate_limited_model", time.Now().Add(time.Minute))

	// 抹掉本地内存态，模拟该账号在实例 B 上还没有任何冷却记录。
	account.mu.Lock()
	account.ModelCooldowns = nil
	account.mu.Unlock()
	if account.IsModelRateLimited("gemini-3.8-flash-tiered") {
		t.Fatal("local cooldown was not cleared")
	}

	// 只按 publicModel 过滤：另一实例写入的 wire 冷却不可见，账号仍会被选中。
	plain := reading.WithAntigravityCooldownFilterContext(context.Background(), "gemini-3.8-flash-low", "", "", nil)
	if !plain(account) {
		t.Fatal("a public-model-only filter must pass the candidate")
	}

	// 带上 wireModel 的三键过滤：命中另一实例的冷却，账号被过滤掉。
	filter := reading.WithAntigravityCooldownFilterContext(context.Background(), "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", antigravityGeminiFamilyKey, nil)
	if filter(account) {
		t.Fatal("the wire-model cooldown written by the other instance must filter the candidate out")
	}
	if !account.IsModelRateLimited("gemini-3.8-flash-tiered") {
		t.Fatal("the remote cooldown was not synchronized into the local account")
	}

	// 下游 filter 仍然生效；nil 候选被拒绝。
	downstream := &Account{DBID: 8821, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady}
	rejecting := reading.WithAntigravityCooldownFilterContext(context.Background(), "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", "", func(*Account) bool { return false })
	if rejecting(downstream) {
		t.Fatal("the downstream filter must still be able to reject a candidate")
	}
	if filter(nil) {
		t.Fatal("a nil candidate must be rejected")
	}

	// 取消的上下文按 fail-closed 处理，不能在读缓存失败时放行。
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledFilter := reading.WithAntigravityCooldownFilterContext(canceled, "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", "", nil)
	if canceledFilter(&Account{DBID: 8822, AccessToken: "token"}) {
		t.Fatal("a canceled context must reject the candidate")
	}

	// 三个键都为空时等价于透传下游 filter。
	empty := reading.WithAntigravityCooldownFilterContext(context.Background(), "", "", "", func(*Account) bool { return true })
	if !empty(&Account{DBID: 8823, AccessToken: "token"}) {
		t.Fatal("an empty key set must pass the candidate through")
	}
}

// TestWithAntigravityAwareCooldownFilterContextSplitsByAccountKind 覆盖 auto 渠道
// （渠道未限定）的候选感知：Antigravity 候选走 public/wire/family 三键检查并 hydrate
// 远端冷却，非 Antigravity 候选只看 publicModel，与 WithModelCooldownFilterContext 的
// 单键语义一致；下游 filter 对每个候选只调用一次；ctx 取消 fail-closed。
func TestWithAntigravityAwareCooldownFilterContextSplitsByAccountKind(t *testing.T) {
	tokenCache := cache.NewMemory(8)
	defer tokenCache.Close()

	antigravity := &Account{DBID: 8830, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady}
	other := &Account{DBID: 8831, AccessToken: "token", Status: StatusReady}
	writing := newAntigravityRateLimitTestStore(t, tokenCache, antigravity, other)
	reading := newAntigravityRateLimitTestStore(t, tokenCache)

	// 另一实例只在共享 wire 模型上写了冷却：两个账号本地都没有任何冷却记录。
	writing.MarkModelCooldownUntil(antigravity, "gemini-3.8-flash-tiered", "rate_limited_model", time.Now().Add(time.Minute))
	writing.MarkModelCooldownUntil(other, "gemini-3.8-flash-tiered", "rate_limited_model", time.Now().Add(time.Minute))
	for _, acc := range []*Account{antigravity, other} {
		acc.mu.Lock()
		acc.ModelCooldowns = nil
		acc.mu.Unlock()
	}

	aware := reading.WithAntigravityAwareCooldownFilterContext(context.Background(), "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", antigravityGeminiFamilyKey, nil)

	// Antigravity 候选：wire 冷却可见并拦截，且同步进本地内存。
	if aware(antigravity) {
		t.Fatal("the aware filter must apply the three-key check to an Antigravity candidate")
	}
	if !antigravity.IsModelRateLimited("gemini-3.8-flash-tiered") {
		t.Fatal("the aware filter did not hydrate the remote wire cooldown into the Antigravity account")
	}

	// 非 Antigravity 候选：只看 publicModel，wire 冷却不可见 → 放行。
	if !aware(other) {
		t.Fatal("a non-Antigravity candidate must keep the single public-model semantics")
	}

	// 家族冷却同样只作用于 Antigravity 候选。
	familyOnly := &Account{DBID: 8832, AccessToken: "token", Status: StatusReady}
	writing.MarkModelCooldownUntil(familyOnly, antigravityGeminiFamilyKey, "rate_limited_model", time.Now().Add(time.Minute))
	familyOnly.mu.Lock()
	familyOnly.ModelCooldowns = nil
	familyOnly.mu.Unlock()
	if !aware(familyOnly) {
		t.Fatal("a non-Antigravity candidate must not see the Antigravity family cooldown")
	}
	antigravityFamilyOnly := &Account{DBID: 8833, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady}
	writing.MarkModelCooldownUntil(antigravityFamilyOnly, antigravityGeminiFamilyKey, "rate_limited_model", time.Now().Add(time.Minute))
	antigravityFamilyOnly.mu.Lock()
	antigravityFamilyOnly.ModelCooldowns = nil
	antigravityFamilyOnly.mu.Unlock()
	if aware(antigravityFamilyOnly) {
		t.Fatal("the aware filter must apply the Gemini family cooldown to an Antigravity candidate")
	}

	// 下游 filter 对每个候选只调用一次（两个分支共用同一次求值）。
	calls := map[int64]int{}
	counting := reading.WithAntigravityAwareCooldownFilterContext(context.Background(), "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", antigravityGeminiFamilyKey, func(acc *Account) bool {
		calls[acc.DBID]++
		return true
	})
	mixed := []*Account{
		{DBID: 8834, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady},
		{DBID: 8835, AccessToken: "token", Status: StatusReady},
	}
	for _, acc := range mixed {
		if !counting(acc) {
			t.Fatalf("the downstream filter rejected candidate %d unexpectedly", acc.DBID)
		}
		if calls[acc.DBID] != 1 {
			t.Fatalf("the downstream filter ran %d times for candidate %d, want 1", calls[acc.DBID], acc.DBID)
		}
	}
	if counting(nil) {
		t.Fatal("a nil candidate must be rejected")
	}

	// ctx 取消 fail-closed。
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledFilter := reading.WithAntigravityAwareCooldownFilterContext(canceled, "gemini-3.8-flash-low", "gemini-3.8-flash-tiered", "", nil)
	if canceledFilter(&Account{DBID: 8836, AccessToken: "token"}) {
		t.Fatal("a canceled context must reject the candidate")
	}

	// publicModel 为空时：Antigravity 候选仍按 wire 检查，非 Antigravity 候选无键可查 → 放行。
	agNoPublic := &Account{DBID: 8837, UpstreamType: UpstreamAntigravity, AccessToken: "token", Status: StatusReady}
	writing.MarkModelCooldownUntil(agNoPublic, "gemini-3.8-flash-tiered", "rate_limited_model", time.Now().Add(time.Minute))
	agNoPublic.mu.Lock()
	agNoPublic.ModelCooldowns = nil
	agNoPublic.mu.Unlock()
	publicless := reading.WithAntigravityAwareCooldownFilterContext(context.Background(), "", "gemini-3.8-flash-tiered", "", nil)
	if publicless(agNoPublic) {
		t.Fatal("an Antigravity candidate must still be checked against the wire key without a public model")
	}
	if !publicless(&Account{DBID: 8838, AccessToken: "token", Status: StatusReady}) {
		t.Fatal("a non-Antigravity candidate must pass when there is no public model to check")
	}
}
