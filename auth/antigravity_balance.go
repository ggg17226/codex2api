package auth

import (
	"context"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/codex2api/database"
)

// SelectionOptions 描述一次选号的调度上下文。
//
// Provider 是下游 Key 限定的上游渠道（database.UpstreamChannel*，空表示不指定/通用），
// Model 是请求的 public model，Policy 是用量闸门（Standard/Spark）。首绑与请求重试的
// Fresh 选号据此识别「这是一次 Antigravity 请求」，从而在 round_robin 下启用加权避险；
// 其余渠道与模式完全走既有选号逻辑，行为不变。
type SelectionOptions struct {
	Provider string // 例如 database.UpstreamChannelAntigravity；空表示不指定/通用
	Model    string // public model
	Policy   DispatchPolicy
}

// schedulerModeRoundRobin 是加权避险唯一生效的调度模式；remaining_quota / fill_first
// 保持既有用量优先语义，只继承冷却正确性修复。
const schedulerModeRoundRobin = "round_robin"

// isAntigravityProvider 报告 provider 是否指向 Antigravity 渠道。auth 侧的
// UpstreamAntigravity 与 database.UpstreamChannelAntigravity 同为 "antigravity"，
// 这里两者都接受，大小写不敏感。
func isAntigravityProvider(provider string) bool {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return false
	}
	return strings.EqualFold(provider, database.UpstreamChannelAntigravity) ||
		strings.EqualFold(provider, UpstreamAntigravity)
}

// antigravityBalancedSelectionEnabled 报告本次选号是否应走 Antigravity 加权避险：
// provider=antigravity、进程开关开启、且调度模式为 round_robin 时成立。
func (s *Store) antigravityBalancedSelectionEnabled(opts SelectionOptions) bool {
	if s == nil {
		return false
	}
	if !isAntigravityProvider(opts.Provider) {
		return false
	}
	if !GetAntigravityBalancedAffinity() {
		return false
	}
	return s.GetSchedulerMode() == schedulerModeRoundRobin
}

// fmix64 是 MurmurHash3 finalizer（也见于 SplitMix64）的 64 位雪崩混淆：三次
// 「右移 33 位亦或 + 大素数乘法」把输入每一位的差异扩散到全部 64 位输出。
//
// 纯算术实现，不依赖平台、字节序与编译器内联，任何 GOARCH 上恒得同一结果。
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// antigravityTieBreak 是加权选号同分时的确定性打散键：对 (亲和键, 账号ID) 做
// FNV-1a，再用 fmix64 做终值雪崩混淆。
//
// 同一亲和键在号池不变时恒得同一顺序（幂等），不同键把并列账号摊开。
//
// 终值混淆不是可选项：FNV-1a 只有「乘 1099511628211 后亦或」一步扩散，输入尾部
// 字节只影响输出的高位区间，低位比特几乎不混合。而这里的输入尾部是十进制 ASCII
// 的账号 ID——同批账号的 ID 是连续整数，后缀高度相似（"…1"、"…2"、"…10"、"…11"），
// 直接取 FNV-1a 最小值会把并列账号系统性地偏向某几个 ID 前缀/后缀，同分打散退化成
// 固定偏好。fmix64 之后每个输出比特都依赖全部输入比特，最小值选择才真正与账号身份
// 无关。
func antigravityTieBreak(key string, dbID int64) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	_, _ = hasher.Write([]byte{':'})
	_, _ = hasher.Write([]byte(strconv.FormatInt(dbID, 10)))
	return fmix64(hasher.Sum64())
}

// selectAntigravityBalancedAccount 是首绑 / Fresh 选号的 Antigravity 加权避险实现。
//
// 候选按 (调度优先级, 健康档位) 从高到低分层，层内按
//
//	score = load + usage*0.50 + penalty*0.35   （load=occupied/limit，usage=已用配额/100）
//
// 升序打分，同分用 (key, DBID) 哈希做确定性打散；从最高层开始逐个尝试取号，命中即返回，
// 整层都拿不下（并发被抢占 / 命中 cached cooldown）时继续下一层。它只影响「下一个新会话
// 优先选谁」，不改变任何冷却、绑定或并发语义。
//
// 第二个返回值 handled 表示加权路径是否已接管本次选号：provider=antigravity、进程开关
// 开启且调度模式为 round_robin 时成立。handled=false 表示本次不适用（调用方继续走既有
// spread / HRW / round-robin 逻辑）；handled=true 时调用方必须直接采用本函数的结果，
// 包括返回 nil 的情况——号池确实没有可用容量，不得再回退重扫。
//
// 单轮求值保护：本函数对每个候选账号的 filter(acc) 至多求值一次（快照循环内），取号循环
// 只做 cached cooldown 与 tryAcquire，不再触碰 filter，杜绝闭包重复触发与 Redis 读放大。
// 这也是 handled 存在的原因：一旦本函数已经消费过 filter，任何回退重扫都会二次求值。
func (s *Store) selectAntigravityBalancedAccount(key string, apiKeyID int64, exclude map[int64]bool, filter AccountFilter, opts SelectionOptions) (*Account, bool) {
	if !s.antigravityBalancedSelectionEnabled(opts) {
		return nil, false
	}
	filter = s.withUsableEgressFilter(filter)

	type candidate struct {
		acc               *Account
		schedulerPriority int64
		tierPriority      int
		limit             int64
		score             float64
		tie               uint64
	}

	maxConcurrency := s.maxConcurrency.Load()
	now := time.Now()
	penaltyWeight, usageWeight := AntigravityThrottleWeights()

	accounts := s.accountSnapshotAccounts()
	candidates := make([]candidate, 0, len(accounts))
	for _, acc := range accounts {
		if exclude != nil && exclude[acc.DBID] {
			continue
		}
		if !acc.dispatchableForPolicy(opts.Policy) {
			continue
		}
		if !s.accountAllowedForAPIKey(acc, apiKeyID) {
			continue
		}
		// filter 单次求值：整个选号只在这里对 acc 调一次 filter。
		if filter != nil && !filter(acc) {
			continue
		}
		occupied := accountOccupiedRequests(acc)
		tier, _, _, limit := acc.schedulerSnapshotForPolicy(maxConcurrency, opts.Policy)
		if limit <= 0 || occupied >= limit {
			continue
		}
		load := float64(occupied) / float64(limit)
		usage := acc.usagePercentForScheduling() / 100.0
		penalty := acc.AntigravityPenalty(now)
		score := load + usage*usageWeight + penalty*penaltyWeight
		candidates = append(candidates, candidate{
			acc:               acc,
			schedulerPriority: acc.schedulerPriority(),
			tierPriority:      tierPriority(tier),
			limit:             limit,
			score:             score,
			tie:               antigravityTieBreak(key, acc.DBID),
		})
	}
	if len(candidates) == 0 {
		// 加权路径已接管但号池没有可用容量：交给调用方直接返回 nil 进入等待，
		// 不能再回退到会对同一批账号重扫 filter 的常规选号。
		return nil, true
	}

	// 一次排序同时确定层序与层内序：层按 (调度优先级, 健康档位) 降序，层内按 score 升序，
	// 同分用哈希 tie 升序做确定性打散。
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.schedulerPriority != right.schedulerPriority {
			return left.schedulerPriority > right.schedulerPriority
		}
		if left.tierPriority != right.tierPriority {
			return left.tierPriority > right.tierPriority
		}
		if left.score != right.score {
			return left.score < right.score
		}
		return left.tie < right.tie
	})

	// 逐层尝试：运营者设置的调度优先级与健康档位仍然高于低层账号，只有整层都拿不下才
	// 下沉到下一层，而不是直接放弃。
	for start := 0; start < len(candidates); {
		end := start + 1
		for end < len(candidates) &&
			candidates[end].schedulerPriority == candidates[start].schedulerPriority &&
			candidates[end].tierPriority == candidates[start].tierPriority {
			end++
		}
		// cached cooldown 只在取号循环里对「即将获取」的账号做一次验证，不做前置 N 次 Redis 读。
		for _, c := range candidates[start:end] {
			if s.accountHasBlockingCachedCooldown(c.acc, opts.Policy) {
				continue
			}
			if s.tryAcquireAccount(c.acc, c.limit, true) {
				return c.acc, true
			}
		}
		start = end
	}
	return nil, true
}

// NextForSessionWithDispatchGuardForProvider 是携带 SelectionOptions 的会话选号入口，
// 供 proxy 在已知请求渠道 / public model 时调用，使 Antigravity 首绑与 Fresh 重选走加权
// 避险。非 Antigravity 渠道或未开启时与 NextForSessionWithDispatchGuard 行为完全一致。
func (s *Store) NextForSessionWithDispatchGuardForProvider(opts SelectionOptions, key string, apiKeyID int64, exclude map[int64]bool, filter AccountFilter) (*Account, string, SessionAffinityGuard) {
	return s.nextForSessionWithFilter(key, apiKeyID, exclude, filter, false, opts)
}

// NextForContinuationWithDispatchForProvider 是续链请求的 SelectionOptions 变体。续链
// 只认绑定账号，加权避险不改变其绑定复用；opts 把 Policy 与渠道一起透传到底层选号——
// 绑定不存在（首次绑定）时续链会落到 Fresh 选号，Provider 缺失会让 Antigravity 请求
// 在重试链上丢掉加权避险上下文。
func (s *Store) NextForContinuationWithDispatchForProvider(opts SelectionOptions, key string, apiKeyID int64, exclude map[int64]bool, filter AccountFilter) (*Account, string) {
	account, proxyURL, _ := s.nextForSessionWithFilter(key, apiKeyID, exclude, filter, true, opts)
	return account, proxyURL
}

// WaitForDispatchAvailableForProvider 是 WaitForDispatchAvailable 的 SelectionOptions 变体，
// 让排队等待路径也能识别 Antigravity 请求并在恢复可用时走加权避险选号。
func (s *Store) WaitForDispatchAvailableForProvider(ctx context.Context, key string, timeout time.Duration, apiKeyID int64, exclude map[int64]bool, filter AccountFilter, opts SelectionOptions, preserveBinding bool, heartbeat ...SchedulerWaitHeartbeat) (*Account, string, SessionAffinityGuard, error) {
	return s.waitForSessionAvailableWithFilter(ctx, key, timeout, apiKeyID, exclude, filter, preserveBinding, opts, heartbeat...)
}
