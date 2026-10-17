package auth

import (
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Antigravity 限流事件与首绑/Fresh 选号避险惩罚。
//
// 上游 Cloud Code 在 429/503 上给出的信号粒度差别很大：RATE_LIMIT_EXCEEDED 带
// RetryInfo 是「这个凭据此刻确实被限流」，MODEL_CAPACITY_EXHAUSTED 是 Google 全
// 局容量不足（换号无用），QUOTA_EXHAUSTED 已经有长冷却 + Gemini family 冷却兜
// 底。首绑加权避险只应该为前一类信号付出代价，所以这里把事件规范化成带权重的
// 记录，并把权重压进一个进程内、随时间指数衰减的惩罚累加器（不落库、不进
// Redis）：惩罚只影响「下一个新会话选谁」，不改变任何既有冷却与绑定语义。

// AntigravityRateLimitScope 是限流事件的归因范围。
type AntigravityRateLimitScope string

const (
	// AGScopeModelRateLimit：账号在该模型上被明确限流（含裸 RESOURCE_EXHAUSTED）。
	AGScopeModelRateLimit AntigravityRateLimitScope = "model_rate_limit"
	// AGScopeSharedCapacity：MODEL_CAPACITY_EXHAUSTED，全模型共享容量短缺。
	AGScopeSharedCapacity AntigravityRateLimitScope = "shared_capacity"
	// AGScopeAccountQuota：QUOTA_EXHAUSTED，已有长冷却与 family 冷却兜底。
	AGScopeAccountQuota AntigravityRateLimitScope = "account_quota"
	// AGScopeUnknown：无法解析出 google.rpc 结构的 429。
	AGScopeUnknown AntigravityRateLimitScope = "unknown"
)

// AntigravityRateLimitEvent 是一次归一化后的 429/503 观测。
type AntigravityRateLimitEvent struct {
	StatusCode  int
	Kind        string
	Scope       AntigravityRateLimitScope
	PublicModel string
	WireModel   string
	Family      string
	Weight      float64
	ResetAt     time.Time
	ObservedAt  time.Time
}

// 惩罚累加器的时间常数：半衰期 60s、窗口 180s、上限 4.0。
// 权重常量供打分侧（首绑/Fresh 选号）与记录侧共用，避免两处各写一份魔数。
const (
	antigravityThrottleWindow     = 180 * time.Second
	antigravityThrottleHalfLife   = 60 * time.Second
	antigravityThrottleMaxPenalty = 4.0
	antigravityThrottleWeight     = 0.35
	antigravityUsageWeight        = 0.50
)

// 事件权重（仅用于首绑/Fresh 选号避险打分，不参与冷却时长计算）。
const (
	// AntigravityPenaltyWeightExplicitRateLimit：有明确 reason 或 RetryInfo 的限流。
	AntigravityPenaltyWeightExplicitRateLimit = 1.0
	// AntigravityPenaltyWeightUnclassifiedRateLimit：裸 RESOURCE_EXHAUSTED 或解析
	// 失败的非结构化 429——没有任何信息指向这个凭据，只记半个惩罚。
	AntigravityPenaltyWeightUnclassifiedRateLimit = 0.5
	// AntigravityPenaltyWeightNone：共享容量与账号配额不记短期惩罚。
	AntigravityPenaltyWeightNone = 0.0
)

// AntigravityThrottleWeights 返回首绑打分使用的惩罚/用量权重。
func AntigravityThrottleWeights() (penaltyWeight, usageWeight float64) {
	return antigravityThrottleWeight, antigravityUsageWeight
}

// AntigravityThrottleWindow 返回惩罚累加器的有效窗口。
func AntigravityThrottleWindow() time.Duration {
	return antigravityThrottleWindow
}

// antigravityThrottleDecay 是窗口内的指数衰减因子：elapsed=0 → 1，
// elapsed=半衰期 → 0.5。
func antigravityThrottleDecay(elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 1
	}
	if elapsed >= antigravityThrottleWindow {
		return 0
	}
	halfLives := float64(elapsed) / float64(antigravityThrottleHalfLife)
	return math.Pow(0.5, halfLives)
}

// RecordAntigravityRateLimitEvent 把一次限流观测累加进账号的进程内惩罚。
// 权重 <= 0 的事件（共享容量、账号配额）不记账；累加值封顶
// antigravityThrottleMaxPenalty，超过窗口后自然归零。
func RecordAntigravityRateLimitEvent(acc *Account, event AntigravityRateLimitEvent) {
	if acc == nil {
		return
	}
	weight := event.Weight
	if math.IsNaN(weight) || weight <= 0 {
		return
	}
	if weight > AntigravityPenaltyWeightExplicitRateLimit {
		weight = AntigravityPenaltyWeightExplicitRateLimit
	}
	now := event.ObservedAt
	if now.IsZero() {
		now = time.Now()
	}
	acc.mu.Lock()
	defer acc.mu.Unlock()
	penalty := acc.antigravityPenalty
	if !acc.antigravityPenaltyAt.IsZero() {
		elapsed := now.Sub(acc.antigravityPenaltyAt)
		if elapsed < 0 {
			elapsed = 0
		}
		penalty *= antigravityThrottleDecay(elapsed)
	}
	penalty += weight
	if math.IsNaN(penalty) || penalty < 0 {
		penalty = 0
	}
	if penalty > antigravityThrottleMaxPenalty {
		penalty = antigravityThrottleMaxPenalty
	}
	acc.antigravityPenalty = penalty
	acc.antigravityPenaltyAt = now
}

// AntigravityPenalty 返回账号在当前时刻的衰减后惩罚（0 表示无近期限流）。
func (a *Account) AntigravityPenalty(now time.Time) float64 {
	if a == nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now()
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.antigravityPenalty <= 0 || a.antigravityPenaltyAt.IsZero() {
		return 0
	}
	elapsed := now.Sub(a.antigravityPenaltyAt)
	if elapsed < 0 {
		elapsed = 0
	}
	penalty := a.antigravityPenalty * antigravityThrottleDecay(elapsed)
	if math.IsNaN(penalty) || penalty <= 0 {
		return 0
	}
	return penalty
}

// ResetAntigravityPenalty 清空账号的限流惩罚（人工恢复、冷却清理时使用）。
func (a *Account) ResetAntigravityPenalty() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.antigravityPenalty = 0
	a.antigravityPenaltyAt = time.Time{}
}

// antigravityBalancedAffinityEnabled 是首绑/Fresh 选号加权避险的进程级开关。
// 默认开启；ANTIGRAVITY_BALANCED_AFFINITY 取 0/false/off/none/no 时关闭，回退为
// 原 round-robin。冷却正确性修复不受该开关影响，始终生效。
var antigravityBalancedAffinityEnabled atomic.Bool

func init() {
	antigravityBalancedAffinityEnabled.Store(antigravityBalancedAffinityFromEnv())
}

func antigravityBalancedAffinityFromEnv() bool {
	raw, ok := os.LookupEnv(antigravityBalancedAffinityEnv)
	if !ok {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "off", "none", "no":
		return false
	default:
		return true
	}
}

// antigravityBalancedAffinityEnv 是开关的环境变量名。
const antigravityBalancedAffinityEnv = "ANTIGRAVITY_BALANCED_AFFINITY"

// SetAntigravityBalancedAffinity 覆盖加权避险开关（启动装配与测试使用）。
func SetAntigravityBalancedAffinity(enabled bool) {
	antigravityBalancedAffinityEnabled.Store(enabled)
}

// GetAntigravityBalancedAffinity 报告首绑/Fresh 选号是否启用加权避险打分。
func GetAntigravityBalancedAffinity() bool {
	return antigravityBalancedAffinityEnabled.Load()
}

// ReloadAntigravityBalancedAffinityFromEnv 重新读取环境变量（关闭需重启生效）。
func ReloadAntigravityBalancedAffinityFromEnv() {
	antigravityBalancedAffinityEnabled.Store(antigravityBalancedAffinityFromEnv())
}
