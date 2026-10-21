package auth

import (
	"time"
)

// IsAntigravityQuotaExhaustedLocked 是纯内存只读的硬门禁谓词：基于已投影到
// Account.AntigravityQuota 的 V2 快照，判断该账号此刻是否应因配额耗尽被阻断调度。
//
// 严格约束（Spec §3.3）：本方法不得进行任何 I/O，也不得派生 goroutine；调用方需
// 自行保证在持有账号锁的临界区内调用（Locked 语义）。
//
// 返回：
//   - exhausted：是否阻断；
//   - reason：阻断原因（known_zero / stale_zero），未阻断时为空串；
//   - resetAt：阻断的到期时刻（可信 resetAt 或 TTL 兜底），未阻断时为零值。
func (a *Account) IsAntigravityQuotaExhaustedLocked(now time.Time) (bool, string, time.Time) {
	if a == nil || a.UpstreamType != UpstreamAntigravity {
		return false, "", time.Time{}
	}
	// 止血开关：若系统设置关闭门禁，立即放行（Spec §8.1，不影响已落库快照）
	settings := ConfiguredAntigravitySettings()
	if !settings.GetQuotaGateEnabled() {
		return false, "", time.Time{}
	}
	// legacy 快照（无 schema_version=2）：旧数据显式 0 不可信，绝不硬封（Spec §3.1）
	if a.AntigravityQuota.SchemaVersion != 2 {
		return false, "", time.Time{}
	}
	// 元数据可信条件（Spec §3.1）：applied_seq > 0、observed_at 非零且不在未来；
	// 缺失即 unknown，不构成阻断依据。
	meta, ok := a.AntigravityQuota.SourceMetadata["groups"]
	if !ok || meta.AppliedSeq <= 0 || meta.ObservedAt.IsZero() || meta.ObservedAt.After(now) {
		return false, "", time.Time{}
	}

	for _, g := range a.AntigravityQuota.Groups {
		for _, b := range g.Buckets {
			w, ok := NormalizeAntigravityWindow(b.Window)
			if !ok || !b.FractionPresent || b.RemainingFraction == nil {
				continue // 白名单外 / 无 presence / 无数值 → unknown，不阻断
			}
			if *b.RemainingFraction != 0.0 {
				continue // known_positive（含极小正数）与解析层未收敛的非法值一律不阻断
			}
			resetAt, hasReset := ValidateResetTime(b.ResetTime, b.ObservedAt, w)
			ttlDeadline := b.ObservedAt.Add(time.Duration(b.TTLSeconds) * time.Second)
			if hasReset {
				if now.Before(resetAt) {
					if now.After(ttlDeadline) {
						// 超龄已知零（stale）：观测年龄超 TTL 但 resetAt 未到，
						// 保持阻断并标记 stale，进入优先补查，绝不静默放行（Spec §3.3）
						return true, "stale_zero", resetAt
					}
					return true, "known_zero", resetAt // 阻断持续到可信 resetAt
				}
				// resetAt 已过：窗口滚动转 unknown，仅解除本桶，不清其他有效零桶
				continue
			}
			// 无可信 resetAt：TTL 兜底阻断，避免永久封禁
			if now.Before(ttlDeadline) {
				return true, "known_zero", ttlDeadline
			}
		}
	}
	return false, "", time.Time{}
}
