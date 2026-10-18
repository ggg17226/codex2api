package auth

import (
	"fmt"
	"hash/fnv"
	"math/bits"
	"strconv"
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

// antigravityTieBreakRaw 是修复前的打散键：FNV-1a 终值不做雪崩混淆。测试用它证明
// 下面的分布断言确实能抓到「去掉 fmix64」这一回归。
func antigravityTieBreakRaw(key string, dbID int64) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	_, _ = hasher.Write([]byte{':'})
	_, _ = hasher.Write([]byte(strconv.FormatInt(dbID, 10)))
	return hasher.Sum64()
}

// antigravityTieBreakWinner 统计「同分时取哈希最小值」在 accounts 个连续 DBID 账号上的
// 实际胜率分布：这才是 tie-break 真正被使用的方式，也是十进制 ASCII 偏斜唯一会显形的地方。
func antigravityTieBreakWinner(tie func(string, int64) uint64, accounts, trials int) []int {
	wins := make([]int, accounts)
	for i := 1; i <= trials; i++ {
		key := fmt.Sprintf("user-session-%d", i)
		best, bestHash := 0, tie(key, 1)
		for account := int64(2); account <= int64(accounts); account++ {
			if got := tie(key, account); got < bestHash {
				best, bestHash = int(account-1), got
			}
		}
		wins[best]++
	}
	return wins
}

func antigravityChiSquared(wins []int) float64 {
	total, expected := 0, 0.0
	for _, win := range wins {
		total += win
	}
	expected = float64(total) / float64(len(wins))
	chi := 0.0
	for _, win := range wins {
		diff := float64(win) - expected
		chi += diff * diff / expected
	}
	return chi
}

// TestAntigravityTieBreakGoldenVectors 钉住跨平台确定性：纯算术 fmix64 在任何 GOARCH
// 上都必须给出同一批常量，否则同分顺序会随构建产物漂移。
func TestAntigravityTieBreakGoldenVectors(t *testing.T) {
	cases := []struct {
		key  string
		dbID int64
		want uint64
	}{
		{"", 0, 0x43751afc069a4a45},
		{"user-session-1", 1, 0xf155dbb4071460da},
		{"gemini-key", 42, 0xcf28f3d8add60b65},
		{"中文亲和键", 1000003, 0x8dcf2c161bc0b644},
	}
	for _, tc := range cases {
		if got := antigravityTieBreak(tc.key, tc.dbID); got != tc.want {
			t.Fatalf("antigravityTieBreak(%q, %d) = 0x%016x, want 0x%016x", tc.key, tc.dbID, got, tc.want)
		}
	}
	if got := fmix64(0); got != 0 {
		t.Fatalf("fmix64(0) = 0x%016x, want 0", got)
	}
	if got := fmix64(1); got != 0xb456bcfc34c2cb2c {
		t.Fatalf("fmix64(1) = 0x%016x, want 0xb456bcfc34c2cb2c", got)
	}
	if got := fmix64(0xffffffffffffffff); got != 0x64b5720b4b825f21 {
		t.Fatalf("fmix64(^0) = 0x%016x, want 0x64b5720b4b825f21", got)
	}
}

// TestAntigravityTieBreakDeterministicAndSensitive 覆盖打散键的基本性质：同输入恒等、
// 键或账号变化时输出变化、大批量样本内无碰撞。
func TestAntigravityTieBreakDeterministicAndSensitive(t *testing.T) {
	if antigravityTieBreak("stable-key", 7) != antigravityTieBreak("stable-key", 7) {
		t.Fatal("antigravityTieBreak is not deterministic")
	}
	seen := make(map[uint64]string, 20000)
	for i := 1; i <= 10000; i++ {
		key := fmt.Sprintf("affinity-%d", i)
		for _, dbID := range []int64{int64(i), int64(i) + 1000000} {
			hash := antigravityTieBreak(key, dbID)
			sample := fmt.Sprintf("%s/%d", key, dbID)
			if previous, ok := seen[hash]; ok {
				t.Fatalf("hash collision between %s and %s", previous, sample)
			}
			seen[hash] = sample
		}
	}
	// 相邻账号 ID（十进制 ASCII 只差最后一位）必须给出完全不同的哈希。
	similar := map[uint64]int{}
	for dbID := int64(1); dbID <= 1000; dbID++ {
		similar[antigravityTieBreak("same-key", dbID)]++
	}
	if len(similar) != 1000 {
		t.Fatalf("1000 consecutive account IDs produced %d distinct hashes", len(similar))
	}
}

// TestAntigravityTieBreakAvalanche 覆盖雪崩性：输入单比特翻转后，输出平均应有约一半
// 比特翻转。FNV-1a 单独使用时该值明显低于 32（尾部字节只影响高位区间），终值混淆把它
// 拉回理论值。
func TestAntigravityTieBreakAvalanche(t *testing.T) {
	total, samples := 0.0, 0
	for dbID := int64(0); dbID < 2000; dbID++ {
		base := antigravityTieBreak("avalanche-key", dbID)
		for bit := uint64(0); bit < 64; bit++ {
			flipped := antigravityTieBreak("avalanche-key", int64(uint64(dbID)^(1<<bit)))
			total += float64(bits.OnesCount64(base ^ flipped))
			samples++
		}
	}
	mean := total / float64(samples)
	if mean < 31 || mean > 33 {
		t.Fatalf("mean avalanche distance = %.2f bits, want ~32 of 64", mean)
	}
	// 对照：未混淆的 FNV-1a 明显偏离，说明本断言对终值混淆敏感。
	rawTotal, rawSamples := 0.0, 0
	for dbID := int64(0); dbID < 2000; dbID++ {
		base := antigravityTieBreakRaw("avalanche-key", dbID)
		for bit := uint64(0); bit < 64; bit++ {
			flipped := antigravityTieBreakRaw("avalanche-key", int64(uint64(dbID)^(1<<bit)))
			rawTotal += float64(bits.OnesCount64(base ^ flipped))
			rawSamples++
		}
	}
	if rawMean := rawTotal / float64(rawSamples); rawMean >= mean-0.5 {
		t.Fatalf("raw FNV-1a avalanche mean = %.2f, fmix64 mean = %.2f: the finalizer stopped contributing", rawMean, mean)
	}
}

// TestAntigravityTieBreakSpreadAcrossAccountIDs 是 P2-1 的核心断言：同分账号按哈希最小值
// 打散时，每个账号的胜率必须接近 1/N。十进制 ASCII 后缀在纯 FNV-1a 下会把最小值系统性
// 地推向某个账号（实测 8 个账号里有一个吃到 50% 的样本），fmix64 之后应回到均匀分布。
func TestAntigravityTieBreakSpreadAcrossAccountIDs(t *testing.T) {
	const accounts, trials = 8, 20000

	wins := antigravityTieBreakWinner(antigravityTieBreak, accounts, trials)
	expected := float64(trials) / float64(accounts)
	for index, win := range wins {
		share := float64(win) / expected
		if share < 0.6 || share > 1.4 {
			t.Fatalf("account %d won %.1f%% of ties, expected %.0f of %d, wins=%v", index+1, share*100, expected, trials, wins)
		}
	}
	if chi := antigravityChiSquared(wins); chi > 60 {
		t.Fatalf("tie-break winner chi-squared = %.1f (df=%d), wins=%v, want a uniform spread", chi, accounts-1, wins)
	}

	// 对照：去掉终值混淆后同一批输入必须显著偏斜，否则本测试抓不到回归。
	rawWins := antigravityTieBreakWinner(antigravityTieBreakRaw, accounts, trials)
	rawMax := 0
	for _, win := range rawWins {
		if win > rawMax {
			rawMax = win
		}
	}
	if float64(rawMax) <= expected*1.5 {
		t.Fatalf("raw FNV-1a winner distribution is unexpectedly flat (max %d of %d): this test no longer proves the need for fmix64", rawMax, trials)
	}

	// 大 ID / 稀疏 ID 同样要摊开：真实部署里账号 ID 是数据库自增主键，可能已经很大。
	for _, ids := range [][]int64{{1000003, 1000004, 1000005, 1000006}, {1, 7, 999999, 1000000, 4294967296}} {
		counts := make([]int, len(ids))
		for i := 1; i <= trials; i++ {
			key := fmt.Sprintf("spread-%d", i)
			best, bestHash := 0, antigravityTieBreak(key, ids[0])
			for index := 1; index < len(ids); index++ {
				if got := antigravityTieBreak(key, ids[index]); got < bestHash {
					best, bestHash = index, got
				}
			}
			counts[best]++
		}
		exp := float64(trials) / float64(len(ids))
		for index, count := range counts {
			if share := float64(count) / exp; share < 0.55 || share > 1.45 {
				t.Fatalf("ids %v: account %d won %.1f%% of ties, counts=%v", ids, index, share*100, counts)
			}
		}
	}
}
