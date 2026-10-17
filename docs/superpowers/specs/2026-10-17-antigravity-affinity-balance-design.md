# Antigravity 调度优化与冷却正确性规格（收窄定稿版）

> 状态：定稿（Phase 1/2/3 完成，复审通过）。
> 实施记录：Phase 1 冷却正确性（canonical 双写 / countTokens / 事件累加器）；Phase 2 加权选号与跨实例过滤；
> 修复 Phase 2 审查 P1（auto Key -aware 过滤、handled 单次 filter）后复审通过；Phase 3 全量回归通过。
> 收窄策略：仅做冷却正确性与首绑/Fresh 加权避险，不做运行时主动迁移状态机。
> 核心策略：修复 canonical 冷却漏写/漏读真实漏洞 + 补齐 countTokens 429 链路 + 首绑与重试 Fresh 选号加权避险。
> 架构取舍：**彻底移除运行时主动会话迁移状态机**（不做版本化 CAS、不做 pending ledger、不做 typed rejection 借用），完全保留既有 Bounded/Strict 语义与生命周期，杜绝跨实例写竞争与状态污染。
> 代码行号基于 `main@5ee6c292`，实施时以实际代码为准。

## 1. 背景与根因收敛

### 1.1 现状与已确认事实
- 线上配置已确认（均为默认值）：`affinity_mode=bounded`、`scheduler_mode=round_robin`、`skip_warm_tier=false`、`relay_model_cooldown_mode=off`、`session_affinity_spread=false`。
- 请求内 HTTP 429 处理链：[Responses](proxy/handler.go#L4442-L4455)、[native Gemini](proxy/handler_gemini.go#L568-L577)、[Anthropic](proxy/handler_anthropic.go#L1167-L1177) 均在收到 429 时立即执行 `Release + UnbindSessionAffinity + retryExclusions.MarkHTTPFailure`，并触发下一轮重选。因此，**遇到 429 的请求在下一次尝试时本来就是 Fresh selection**。
- 截图现象：错误摘要为 `rate_limited_model / 429 · Resource has been exhausted (e.g. check quota)`，持续约 122s，无首字。
- 缓存现状：成功的 200 请求缓存命中率约 98%（cached≈176k / prompt≈178k），会话亲和正在有效命中 Gemini 上游 implicit cache，不能随意破坏活跃会话的粘性。

### 1.2 确认存在的真实代码漏洞
1. **模型别名导致冷却漏写（P1-4）**：
   - Antigravity 渠道中，`gemini-3.7/3.8-flash-low/medium/high` 共享相同的 upstream wire 模型 `gemini-3.7/3.8-flash-tiered`（[antigravity_models.go:50-60](proxy/antigravity_models.go#L50-L60)）。
   - 当前 `applyAntigravityCooldown`（[antigravity_quota.go:319-373](proxy/antigravity_quota.go#L319-L373)）仅针对调用方传入的 public model 写入冷却，**从未写入 wireModel 冷却**。
   - 过滤侧 `antigravityAccountModelRateLimited`（[antigravity_quota.go:282-293](proxy/antigravity_quota.go#L282-L293)）虽然检查了 wireModel，但因冷却表中没有 wireModel 记录，导致 high 被限流后，low/medium 的请求仍能选中该账号并继续触发 429。
2. **跨实例共享缓存漏读**：
   - `WithModelCooldownFilterContext`（[store.go:4170-4189](auth/store.go#L4170-L4189)）仅针对请求的 public model 查询与 hydrate 跨实例缓存；另一实例写入的 wireModel / Gemini family 冷却在当前实例不可见。
3. **native Gemini countTokens 漏路径**：
   - `handleGeminiCountTokens` 的非 200 分支（[handler_gemini.go:271-307](proxy/handler_gemini.go#L271-L307)）仅执行 `ReportFailure/Release/Unbind/exclude`，从未调用 `applyCooldownForModel`，完全漏掉了冷却写入与事件观测。
4. **首绑与重试 Fresh 选号无负载与限流避险权重**：
   - 默认配置下 `session_affinity_spread=false`，首绑走 round-robin；
   - 请求内遇到 429 解绑后，或新会话到来时，选号器在同优先级层内不比对在途负载、配额用量以及近期限流历史，导致已被限流的账号在冷却短暂到期（或被跨 alias 请求）后极易再次被选中。

---

## 2. 目标与非目标

### 2.1 目标
1. **修复冷却正确性**：
   - 统一写入 public + wireModel（及配额耗尽时的 Gemini family）冷却；
   - 跨实例共享过滤器增加对 wireModel 和 family 的查询与本地同步；
   - 补齐 native countTokens 的 429/503 冷却写入。
2. **首绑与重选加权避险**：
   - 建立轻量级进程内限流惩罚累加器（`antigravityPenalty`）；
   - 在新会话首绑及重选（Fresh selection）时，显式识别 `provider=antigravity`，按 `load + 0.50*usage + 0.35*penalty` 加权打分，大幅降低刚被限流账号的再次选中率。
3. **安全与性能保障**：
   - 单次选号中对候选账号的 filter 至多求值一次，杜绝测试闭包 panic 与 Redis 读放大；
   - cached cooldown 检查保留在取号循环中，不执行前置 N 次 Redis 读。

### 2.2 非目标
- **不做会话主动迁移状态机**：不修改 `nextForSessionWithFilter` 的绑定复用逻辑，不引入版本化 CAS、不引入复杂的分布式锁与 pending admission ledger。
- **不覆盖 strict 模式**：完全尊重运营者的 strict 配置。
- **不破坏现有调度模式**：`remaining_quota` 与 `fill_first` 模式保持原有用量优先语义，仅继承冷却正确性修复。
- **不修改计费与错误码**：上游返回的错误状态码与计费逻辑保持完全一致。

---

## 3. 详细设计

### 3.1 统一限流事件与分类权重

在 `proxy/antigravity_quota.go` 与 `auth/antigravity_ratelimit.go` 中规范化 429/503 事件：

```go
type AntigravityRateLimitScope string

const (
    AGScopeModelRateLimit AntigravityRateLimitScope = "model_rate_limit"
    AGScopeSharedCapacity AntigravityRateLimitScope = "shared_capacity"
    AGScopeAccountQuota   AntigravityRateLimitScope = "account_quota"
    AGScopeUnknown        AntigravityRateLimitScope = "unknown"
)

type AntigravityRateLimitEvent struct {
    StatusCode    int
    Kind          string
    Scope         AntigravityRateLimitScope
    PublicModel   string
    WireModel     string
    Family        string
    Weight        float64 // 0.0, 0.5, 1.0
    ResetAt       time.Time
    ObservedAt    time.Time
}
```

**权重规则（仅用于首绑/Fresh 选号避险打分）**：
- `RATE_LIMIT_EXCEEDED`（有明确 reason 或有 RetryInfo）：`Weight = 1.0`，`Scope = AGScopeModelRateLimit`
- 裸 `RESOURCE_EXHAUSTED`（无 reason 且无 RetryInfo，截图即此类）：`Weight = 0.5`，`Scope = AGScopeModelRateLimit`
- 非结构化 429（解析失败）：`Weight = 0.5`，`Scope = AGScopeUnknown`
- `MODEL_CAPACITY_EXHAUSTED`（共享模型容量）：`Weight = 0.0`，`Scope = AGScopeSharedCapacity`（不记惩罚）
- `QUOTA_EXHAUSTED`：`Weight = 0.0`，`Scope = AGScopeAccountQuota`（已有长冷却与 family 冷却，不记短期惩罚）
- 非结构化 503 / 未知错误：`Weight = 0.0`

### 3.2 惩罚累加器（仅内存态）

在 `auth.Account` 上增加轻量字段（受 `a.mu` 保护，不落库）：
```go
antigravityPenalty   float64   // 衰减累加值，最大 4.0
antigravityPenaltyAt time.Time // 最近一次记录时刻
```

常量定义：
```go
const (
    antigravityThrottleWindow     = 180 * time.Second
    antigravityThrottleHalfLife   = 60 * time.Second
    antigravityThrottleMaxPenalty = 4.0
    antigravityThrottleWeight     = 0.35
    antigravityUsageWeight        = 0.50
)
```

- 记录：`p = min(maxPenalty, oldPenalty*decay(now-at) + event.Weight)`，更新 `at = now`。
- 读取：`p * decay(now-at)`，超过 180s 窗口直接返回 0。

### 3.3 Canonical 冷却与跨实例过滤修复

1. **写入侧（`proxy/antigravity_quota.go`）**：
   - 在 `applyAntigravityCooldown` 中：
     ```go
     wireModel, _ := antigravityResolvePublicModelForAccount(account, model)
     // 1. 写入 public model 冷却（保持既有「可被覆盖」语义）
     store.MarkModelCooldownUntil(account, model, decision.Reason, decision.ResetAt)
     // 2. 若 wireModel 存在且与 public model 不同，同步写入 wireModel 冷却
     //    （共享 backing：只延长不缩短，避免兄弟 tier 的短事件把长冷却压短）
     if wireModel != "" && wireModel != model {
         store.MarkModelCooldownUntilAtLeast(account, wireModel, decision.Reason, decision.ResetAt)
     }
     // 3. 若为 Gemini 配额耗尽，写入 family 冷却（同样只延长不缩短）
     if quota.Kind == antigravityQuotaKindQuotaExhausted && antigravityIsGeminiModel(model) {
         store.MarkModelCooldownUntilAtLeast(account, antigravityGeminiFamilyCooldownKey, decision.Reason, decision.ResetAt)
     }
     ```
2. **补齐 countTokens 路径（`proxy/handler_gemini.go`）**：
   - 在 `handleGeminiCountTokens` 的非 200 分支中调用 `h.applyCooldownForModel(account, resp.StatusCode, errBody, resp, model)`，确保错误与事件被记录。
3. **读取侧与跨实例同步（`auth/store.go`）**：
   - 新增 `WithAntigravityCooldownFilterContext(ctx, publicModel, wireModel, familyKey, filter)`：
     对当前候选账号，依次检查 publicModel、wireModel 以及 familyKey（仅 Gemini）在共享缓存中的冷却状态并进行本地同步，任一命中冷却即过滤。
   - 新增 `WithAntigravityAwareCooldownFilterContext(...)`：供**渠道未限定（auto Key）**请求使用。
     `acc.IsAntigravityAPI()` 的候选走 public/wire/family 三键检查；其余候选只查 publicModel
     （与既有 `WithModelCooldownFilterContext` 等价）。下游 filter 对每个候选只求值一次，ctx 取消 fail-closed。
   - `proxy/handler.go` 的 `withRequestModelCooldownFilter` 按渠道三档选择：antigravity→三键、
     auto（空串）→aware 版本、其他显式渠道→单键。这样 auto Key 选中 AG 账号时也能看到跨实例
     wire/family 冷却（P1-1），非 AG 候选行为不变。

### 3.4 首绑与重试 Fresh 选号加权均衡

1. **选号入口与上下文传递**：
   ```go
   type SelectionOptions struct {
       Provider string // 显式传入，例如 database.UpstreamChannelAntigravity
       Model    string // public model
       Policy   DispatchPolicy
   }
   ```
   在 proxy 的选号入口处（`nextAccountForSessionWithDispatchGuard` 等），通过 `requestUpstreamChannel(c)` 显式传入 `Provider` 与 `Model`。

2. **打分与排序逻辑（`auth/antigravity_balance.go`）**：
   - 当 `opts.Provider == database.UpstreamChannelAntigravity` 且 `scheduler_mode == "round_robin"` 时：
     - 候选按 `(schedulerPriority, tierPriority)` 从高到低分层，层内筛选符合条件的 Antigravity 候选；
     - 候选打分公式（分数越小越优）：
       ```go
       load := float64(occupied) / float64(limit) // 0..1+
       usage := acc.usagePercentForScheduling() / 100.0 // 0..1
       penalty := acc.AntigravityPenalty(now) // 0..4
       score := load + usage*antigravityUsageWeight + penalty*antigravityThrottleWeight
       ```
     - 排序：层内按 `score` 升序，同分时使用 FNV 哈希 tie-break；
     - 取号：从最高层开始按顺序执行 `tryAcquireAccount(acc, limit, true)`，整层拿不下时下沉下一层，命中即返回。
   - **返回 `(*Account, handled bool)`**：`handled=true` 表示加权路径已接管本次选号（provider+开关+round_robin 成立）；
     此时即使返回 nil（号池全满/无可用候选），调用方也**不得**再回退到 spread/HRW/round-robin 重扫，
     否则会二次求值同一批账号的 filter。`handled=false` 时才平滑回退既有逻辑。
   - **native Gemini（generateContent/stream 与 countTokens）强制 `Provider=antigravity`**：这两个入口的候选池
     本就被 `antigravityOAuthChannelAccountFilter` 硬限定为 AG OAuth，auto Key 下也必须启用加权避险；
     通用入口（Responses/Chat/Messages）在 auto Key 下 Provider 为空，不启用加权，只享受冷却过滤修复（规格边界）。

3. **单轮 Filter 求值保护**：
   - 选号前对候选账号进行快照遍历，保证单次选号中每个账号的 `filter(acc)` 至多调用一次，避免测试中的闭包 channel panic 及生产环境中的 Redis 读重复。
   - `accountHasBlockingCachedCooldown` 的检查依然保留在遍历取号循环中，仅对即将获取的账号进行单次验证。
   - 加权路径接管后（handled=true）不回退重扫；号池饱和时返回 nil 交给上层等待/排队。**这是一次有意的行为变化**：
     原逻辑「宁可暂时聚集也不拒绝」；新逻辑优先保证 filter 单次求值，饱和时由上层 wait/queue 处理。

---

## 4. 行为变化与兼容性

| 场景 | 行为说明 |
| :--- | :--- |
| **已绑定且正常的会话** | 完全沿用原有绑定，100% 保护上游 prompt cache，零变更 |
| **请求内遇到 429** | 既有逻辑已解绑；随后的重试与下一轮请求走加权 Fresh 选号，自动避开刚被限流的账号 |
| **同 backing 跨 tier 请求** | high 触发冷却后，wireModel 冷却生效，low/medium 不会再误选同一受限账号 |
| **多实例部署** | 实例 A 写入的 wireModel/family 冷却，实例 B 在过滤时能即时读取并同步 |
| **auto Key（未限定渠道）** | 冷却过滤按候选账号类型分流：AG 候选查 public/wire/family 三键，非 AG 只查 public；native Gemini/countTokens 仍启用加权避险；通用入口不启用加权（规格边界） |
| **号池饱和（加权路径生效）** | 返回 nil 交给上层等待/排队，不再回退常规选号（换取 filter 单次求值；行为变化已记录） |
| **Strict 模式** | 完全尊重原语义，不执行强制解绑；无绑定时的首选享受加权避险 |
| **Remaining Quota / Fill First** | 保持原有用量优先排序，仅继承冷却与过滤修复 |
| **其他渠道（Codex / Claude / Grok）**| `opts.Provider != antigravity`，完全走原逻辑，零影响 |

---

## 5. 开关与回滚

- 环境变量开关：`ANTIGRAVITY_BALANCED_AFFINITY`
  - 默认值：`true`（开启加权避险）；
  - 设置为 `0`, `false`, `off`, `none`, `no` 时关闭加权避险，回退为原 round-robin；
  - 冷却修复（Canonical 双写与跨实例读）属于正确性修复，始终生效。
- 关闭需重启生效。

---

## 6. 代码修改落点

| 文件 | 修改职责 |
| :--- | :--- |
| `auth/antigravity_balance.go`（新建） | `SelectionOptions`、加权打分与分层选号（`(*Account, handled)`）、FNV tie-break、filter 单次求值保护 |
| `auth/antigravity_ratelimit.go`（新建） | 限流事件数据结构、分类记录方法、惩罚累加器、开关管理 |
| `auth/store.go` | 字段挂载、`SelectionOptions` 贯穿选号/等待入口、`WithAntigravityCooldownFilterContext` 与 `WithAntigravityAwareCooldownFilterContext` 跨实例三键/分流过滤、handled 直接返回 |
| `proxy/antigravity_quota.go` | `applyAntigravityCooldown` 补齐 wireModel 冷却写入与单点事件记录；非结构化 429 补写 wire；共享 wire/family 冷却走 `MarkModelCooldownUntilAtLeast`（只延长不缩短）；admin 连接测试使用不记账模式 |
| `proxy/handler_gemini.go` | `handleGeminiCountTokens` 429/503 补齐调用 `applyCooldownForModel`；native 入口强制 `Provider=antigravity` |
| `proxy/handler.go` 及相关 handler | `withRequestModelCooldownFilter` 三档渠道分流；`SelectionOptions` 贯穿 Responses/Chat/Messages/WS/重试/等待/续链；候选池排除 AG 的 compact 入口改用 policy-only `SelectionOptions` |
| `auth/antigravity_balance_test.go`（新建） | 加权避险排序、分层 fallback、饱和池 filter 单次、handled 语义 |
| `proxy/antigravity_balance_test.go` / `proxy/antigravity_auto_channel_test.go`（新建） | 跨实例三键过滤、auto 渠道分流、native 强制 provider、端到端 countTokens 加权 |
| `proxy/antigravity_quota_test.go` | wireModel 同步写入、非结构化 429 wire、countTokens 冷却、admin 隔离 |

---

## 7. 验收测试清单

1. **别名冷却联动测试**：请求 `gemini-3.8-flash-high` 触发 429 冷却后，同一账号立即请求 `gemini-3.8-flash-low`，断言被模型冷却拦截。
2. **跨实例冷却同步测试**：实例 A 写入 `gemini-3.8-flash-tiered` 冷却，实例 B 使用带有共享缓存的 filter，断言成功过滤该账号。
3. **countTokens 冷却写入测试**：`countTokens` 收到 429，断言该账号模型冷却被正确写入。
4. **加权避险与首绑打分散列测试**：
   - 4 个健康账号，其中账号 A 记录一次 429 惩罚；发起多次不同 key 的首选，断言账号 A 选中概率显著低于其他账号；
   - 断言候选账号的 filter 闭包只被求值一次；
   - 饱和池（所有候选并发满）+ 二次求值即 panic 的 filter：断言不 panic、每账号至多一次、返回 nil（handled=true 不回退）；
   - 层间 fallback：最高层全满时能下沉到次高层取号。
5. **auto Key 与非结构化 429**：
   - auto 渠道 + AG 账号：跨实例 wire/family 冷却能拦截；auto 渠道 + 非 AG 账号仍只查 public；
   - native Gemini/countTokens 在 auto Key 下仍强制 `Provider=antigravity`，端到端验证受罚账号不被选中；
   - 非结构化 429 回退分支补写 wire 冷却（relay 冷却策略开启时）。
6. **续链与等待路径**：preserveBinding/turnContinuationPinned 分支透传 `SelectionOptions`，不丢失加权上下文。
7. **共享冷却 deadline 单调性**：同账号先写较长的 wire/family 冷却（如 290s）、再触发更短事件（如 5s）时，共享 deadline 保持不变（断言剩余 > 60s）；先短后长则延长。public 冷却保持既有覆盖语义不变。
8. **compact 入口不套用加权**：compact 执行器与 native remote compaction 的候选池排除 AG 账号，Antigravity 渠道 Key 下这两个入口的 `SelectionOptions.Provider` 为空、只保留 `Policy`，不绕过 spread/HRW。
9. **既有调度回归测试**：
   - `go test ./auth/ -run 'Antigravity|Affinity|LockOrder' -count=1`
   - `go test ./proxy/ -run 'Antigravity|GeminiNative|WithRequestModelCooldownFilter' -count=1`
   - `go test -race ./auth/ ./proxy/ -count=1`

---

## 8. 已知边界与后续优化（P2，不阻塞本次定稿）

以下由 Phase 2 修复复审（deepseek-flash）确认，记录为已知行为/后续项：

1. **auto Key 的通用入口不启用加权**：Responses/Chat/Messages 在 auto Key 下 Provider 为空，只享受三键冷却过滤修复，不启用加权避险；native Gemini/countTokens 因候选池只含 AG 而强制启用。若后续希望 auto Key 通用入口也加权，可按模型归属（`antigravityPublicModel`）推导 provider，属于后续增强。
2. **capacityFull 绑定溢出路径重复 filter**：`takeByIDModeWithCapacity` 先 filter，容量满回退 fresh 选号时会再对同一账号求值一次 filter。这是改动前就存在的路径，不在本次「加权路径单次求值」范围内；后续可记录 filter 结果或前移容量检查。
3. **加权路径未接入调度指标/索引**：未调用 `recordSchedulerSelection`，`selection_total` 会低估 AG fresh 选号；indexed 引擎下改为 O(N) 全量扫描 + 排序，大号池有性能/可观测性回归。后续可补指标与候选窗口。
4. **非结构化 429 的 wire 冷却受 relay 策略约束**：默认 `relay_model_cooldown_mode=off` 时 `Apply429Cooldown` 不写任何冷却，因此非结构化 429 只记 0.5 惩罚、不写 public/wire；开启 relay 冷却后才会补写 wire。这尊重运营者配置。
5. **测试缺口**：续链 Provider 透传入口、层间 fallback 的「整层 tryAcquire 失败」路径、load/usage 权重敏感性、capacityFull 重复 filter 尚未直接覆盖。
6. **spread=true（非默认）行为变化**：AG 加权路径在 spread 检查之前接管，HRW 一致哈希被 FNV tie-break 取代；规格按 round_robin 触发，属于非默认配置的已接受变化。
7. **`ResetAntigravityPenalty` 暂无生产调用点**：人工清冷却/恢复不会清惩罚，依赖自然衰减（180s 窗口）。
8. **compact/WS 的冷却过滤仍按渠道三键**：这两个入口候选池不含 AG（且没有 `applyUpstreamChannelFilter`），但 `withRequestModelCooldownFilter` 的 antigravity 分支仍按三键检查；对 Codex 模型 `AntigravityWireModelID` 为空无影响，对 Gemini public model 理论上可能误过滤一个在 AG wire key 上持有冷却的 relay 账号。后续可改用 `WithAntigravityAwareCooldownFilterContext` 按账号类型分流。
9. **`affinity_mode=off` 绕过加权（非默认）**：`nextForSessionWithFilter` 在 off 模式直接走 `NextExcludingWithDispatch`，丢弃 `SelectionOptions.Provider`，加权避险不生效；默认 bounded 不受影响。
10. **`lazy_mode=true` 绕过 lazy 预热（非默认）**：加权路径使用 `dispatchableForPolicy`/实时快照，不调用 `accountLazySelectable`/`ensureLazyDispatchReady`，需要刷新凭据的账号可能被选中并多一次 401/刷新；默认 lazy=false 不受影响。
11. **`SelectionOptions.Model` 目前 reserved**：auth 侧只读取 `Provider`/`Policy`，`Model` 由 handler 传入并保留给后续按模型族推导 scope/门控。
12. **cache/DB 冷却写入为 last-writer-wins**：本机内存的 AtLeast 单调性成立；`afterModelCooldownMarked` 的 cache `SetRuntime` 与 DB upsert 在锁外，同账号兄弟 tier 并发时短事件可能在长事件之后把跨实例 cache/DB 改回短 deadline（窄窗口、跨实例 best-effort）。若要求严格跨实例单调，需要 max-aware 的 cache/DB 写入。
13. **`TestForwardImagesCatchAllRetriesBeyondOrdinaryAttemptCap` 为既有 race flake**：5 次全量 `-race ./proxy/` 中 1 次失败（upstream calls=7/want 6），单独 `-race -count=20` 全过；未触碰 images/runtime settings，判断为既有 image 重试路径在 race 调度下的时序 flake。CI 建议隔离/记录该测试，不以全量 race 全绿作为本变更验收前提。
