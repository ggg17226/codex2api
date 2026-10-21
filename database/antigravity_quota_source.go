package database

// Antigravity 配额来源（source）的短事务 CAS 协议（Spec §4.1、验收 #28/#36/#39）。
//
// 本文件与 auth 包解耦：配额事实以 credentials JSON 的 `antigravity_quota`
// （字符串形式的 JSON 快照）与 `antigravity_quota_epoch`（失效纪元）两个键持久化，
// 所有写入都在**短事务 + 行锁**内完成，事务中不联网：
//   - 预约：逐 source 递增 `source_metadata.<source>.current_attempt_seq`；
//   - 提交：Latest-Success-Wins，仅要求 `mySeq > applied_seq` 且 gen/scope/epoch 匹配；
//   - 清除：同事务删除 source 事实与定时器并递增 epoch，构成在途观测的失效屏障；
//   - 轮换：UpdateCAS 的受控 carryforward 让同身份例行换票不洗新已落库的配额事实。
//
// 序号写入不推进 `credential_generation`（预约不是凭据轮换），因此预约与提交
// 可以在同一代际上前后发生。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	// antigravityQuotaCredentialKey 是配额快照在 credentials JSON 中的键（明文存储，
	// 触发器可直接 json_extract 比较）。
	antigravityQuotaCredentialKey = "antigravity_quota"
	// antigravityQuotaEpochCredentialKey 是配额事实失效纪元（与 generation 独立）。
	antigravityQuotaEpochCredentialKey = "antigravity_quota_epoch"
	// antigravityQuotaPermanentRefreshErrorKey 是 RT 谱系断裂的永久失败标记。
	antigravityQuotaPermanentRefreshErrorKey = "antigravity_permanent_refresh_error"

	antigravityUpstreamType = "antigravity"

	antigravityQuotaKeySchemaVersion  = "schema_version"
	antigravityQuotaKeySourceMetadata = "source_metadata"
	antigravityQuotaKeyQuotaGroups    = "quota_groups"
	antigravityQuotaKeyAICredits      = "ai_credits"

	antigravitySourceMetaAppliedSeq        = "applied_seq"
	antigravitySourceMetaCurrentAttemptSeq = "current_attempt_seq"
	antigravitySourceMetaLastAttemptAt     = "last_attempt_at"
	antigravitySourceMetaLastSuccessAt     = "last_success_at"

	// antigravityQuotaTimerKindPrefix 是配额流定时器在 maintenance_jobs 中的 job_kind 前缀。
	antigravityQuotaTimerKindPrefix = "antigravity_quota"
)

// RTConflictOutcome 是 RT 轮换冲突按 reload 结果分流后的处置分支（Spec §4.1 #5）。
type RTConflictOutcome int

const (
	// RTConflictTrustWinner durable 行已含更新的 access token：信任 winner，丢弃本次
	// token 与全部 source 观测，绝不回写。
	RTConflictTrustWinner RTConflictOutcome = iota + 1
	// RTConflictReplayCredentialOnly durable 行仍等于预轮换凭据且身份未变：在调用方
	// 仍持 family lease 的前提下重放一次仅含凭据字段的 UpdateCAS，不携带任何 source 观测。
	RTConflictReplayCredentialOnly
	// RTConflictIsolateIdentity 身份谱系无法证明连续：禁止把本次 RT 写入当前身份，
	// 落永久失败标记并写审计。
	RTConflictIsolateIdentity
)

func (o RTConflictOutcome) String() string {
	switch o {
	case RTConflictTrustWinner:
		return "trust_winner"
	case RTConflictReplayCredentialOnly:
		return "replay_credential_only"
	case RTConflictIsolateIdentity:
		return "isolate_identity"
	default:
		return "unknown"
	}
}

// antigravityQuotaRowTx 是短事务内锁定的账号行。
type antigravityQuotaRowTx struct {
	tx         *sql.Tx
	doc        map[string]any
	generation int64
	familyID   string
}

// mutateAntigravityQuotaCredentials 在单个短事务内锁定账号行（PG 用 FOR UPDATE，
// SQLite 依赖 withSQLiteWriteLock 串行化），把 mutator 对 credentials 文档的修改写回，
// **不**推进 credential_generation。mutator 返回 proceed=false 时不写入（fence 拒绝）。
func (db *DB) mutateAntigravityQuotaCredentials(ctx context.Context, accountID int64, mutator func(*antigravityQuotaRowTx) (bool, error)) error {
	if db == nil || db.conn == nil || accountID <= 0 {
		return errors.New("antigravity quota mutation requires a live account id")
	}
	return db.withSQLiteWriteLock(ctx, func() error {
		tx, beginErr := db.conn.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		defer tx.Rollback()
		query := `SELECT credentials, credential_generation, COALESCE(credential_family_id,'') FROM accounts WHERE id=$1 AND status <> 'deleted'`
		if !db.isSQLite() {
			query += ` FOR UPDATE`
		}
		var raw any
		var generation int64
		var familyID string
		if scanErr := tx.QueryRowContext(ctx, query, accountID).Scan(&raw, &generation, &familyID); scanErr != nil {
			return scanErr
		}
		doc := decodeCredentials(raw)
		if doc == nil {
			doc = map[string]any{}
		}
		row := &antigravityQuotaRowTx{tx: tx, doc: doc, generation: generation, familyID: strings.TrimSpace(familyID)}
		proceed, mutateErr := mutator(row)
		if mutateErr != nil || !proceed {
			return mutateErr
		}
		encoded, marshalErr := json.Marshal(encryptSensitiveCredentials(row.doc))
		if marshalErr != nil {
			return marshalErr
		}
		updateQuery := `UPDATE accounts SET credentials=$1, updated_at=CURRENT_TIMESTAMP WHERE id=$2 AND credential_generation=$3`
		if !db.isSQLite() {
			updateQuery = `UPDATE accounts SET credentials=$1::jsonb, updated_at=NOW() WHERE id=$2 AND credential_generation=$3`
		}
		if _, execErr := tx.ExecContext(ctx, updateQuery, encoded, accountID, generation); execErr != nil {
			return execErr
		}
		return tx.Commit()
	})
}

// ReserveAntigravityQuotaSource 在该 source 自己的子请求联网前预约一枚序号：短事务内
// 锁定账号行，校验未删除、expectedGen 与 family/scope 匹配，仅递增该 source 的
// `current_attempt_seq` 并记录真实尝试时间，返回序号与其绑定的 quota_epoch。
// 未取得执行资格的调用方不得调用本函数空耗序号（Spec §4.1 #1）。
func (db *DB) ReserveAntigravityQuotaSource(ctx context.Context, accountID int64, source string, expectedGen int64, expectedScope string) (int64, int64, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return 0, 0, errors.New("antigravity quota reservation requires a source name")
	}
	expectedScope = strings.TrimSpace(expectedScope)
	var seq, epoch int64
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if row.generation != expectedGen {
			return false, fmt.Errorf("antigravity quota reservation rejected: generation fence account=%d expected=%d current=%d", accountID, expectedGen, row.generation)
		}
		if expectedScope != "" && row.familyID != "" && row.familyID != expectedScope {
			return false, fmt.Errorf("antigravity quota reservation rejected: identity scope fence account=%d expected=%s current=%s", accountID, expectedScope, row.familyID)
		}
		meta := antigravitySourceMetadataForWrite(row.doc, source)
		next, _ := int64FromJSONValue(meta[antigravitySourceMetaCurrentAttemptSeq])
		next++
		meta[antigravitySourceMetaCurrentAttemptSeq] = next
		meta[antigravitySourceMetaLastAttemptAt] = time.Now().UTC().Format(time.RFC3339)
		seq = next
		epoch = antigravityQuotaEpochFromDocument(row.doc)
		return true, nil
	})
	if err != nil {
		return 0, 0, err
	}
	return seq, epoch, nil
}

// CommitAntigravityQuotaSource 以 Latest-Success-Wins 契约提交一个 source 的成功观测：
// 短事务内复验 epoch/gen/scope，并要求 `mySeq > applied_seq`；**不得**要求
// `current_attempt_seq == mySeq`（后发失败不得否决先发成功，验收 #28）。任一 fence
// 不匹配只返回 applied=false 且不报错——它是写入栅栏，不是网络去重证明。
func (db *DB) CommitAntigravityQuotaSource(ctx context.Context, accountID int64, source string, mySeq int64, epoch int64, expectedGen int64, expectedScope string, snapshotJSON string) (bool, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return false, errors.New("antigravity quota commit requires a source name")
	}
	incoming := map[string]any{}
	if trimmed := strings.TrimSpace(snapshotJSON); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &incoming); err != nil {
			return false, fmt.Errorf("decode antigravity quota snapshot for source %s: %w", source, err)
		}
		if incoming == nil {
			incoming = map[string]any{}
		}
	}
	expectedScope = strings.TrimSpace(expectedScope)
	applied := false
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if row.generation != expectedGen {
			return false, nil
		}
		if expectedScope != "" && row.familyID != "" && row.familyID != expectedScope {
			return false, nil
		}
		if epoch != antigravityQuotaEpochFromDocument(row.doc) {
			return false, nil
		}
		appliedSeq, _ := int64FromJSONValue(antigravitySourceMetadataEntryForRead(row.doc, source)[antigravitySourceMetaAppliedSeq])
		if mySeq <= appliedSeq {
			return false, nil
		}
		successAt := time.Now().UTC().Format(time.RFC3339)
		facts := antigravityQuotaFactsForWrite(row.doc)
		antigravityMergeQuotaFacts(facts, incoming, source, mySeq, successAt)
		encoded, marshalErr := json.Marshal(facts)
		if marshalErr != nil {
			return false, marshalErr
		}
		row.doc[antigravityQuotaCredentialKey] = string(encoded)
		applied = true
		return true, nil
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// ClearAntigravityQuotaSourcesCAS 清除该账号全部配额 source 事实与定时器，并在
// **同一短事务内**递增 `antigravity_quota_epoch`（Spec §4.1 失效纪元、验收 #36）。
// 凭据、models 等目录分区与 credential generation 均保持不变，因此清除后仍在途的
// 观测（无论其 mySeq 多大）提交时都会因 epoch 不匹配被拒。
func (db *DB) ClearAntigravityQuotaSourcesCAS(ctx context.Context, accountID int64) (int64, error) {
	newEpoch := int64(0)
	err := db.mutateAntigravityQuotaCredentials(ctx, accountID, func(row *antigravityQuotaRowTx) (bool, error) {
		if _, err := row.tx.ExecContext(ctx,
			`DELETE FROM maintenance_jobs WHERE entity_id=$1 AND job_kind LIKE $2`,
			accountID, antigravityQuotaTimerKindPrefix+"%"); err != nil {
			return false, err
		}
		if raw, ok := row.doc[antigravityQuotaCredentialKey].(string); ok && strings.TrimSpace(raw) != "" {
			var facts map[string]any
			if json.Unmarshal([]byte(raw), &facts) == nil {
				// 只删除配额 source 事实；models / 目录分区由目录流自行维护。
				delete(facts, antigravityQuotaKeyQuotaGroups)
				delete(facts, antigravityQuotaKeySourceMetadata)
				delete(facts, antigravityQuotaKeyAICredits)
				if antigravityQuotaFactsHasContent(facts) {
					encoded, marshalErr := json.Marshal(facts)
					if marshalErr != nil {
						return false, marshalErr
					}
					row.doc[antigravityQuotaCredentialKey] = string(encoded)
				} else {
					row.doc[antigravityQuotaCredentialKey] = ""
				}
			}
		}
		newEpoch = antigravityQuotaEpochFromDocument(row.doc) + 1
		row.doc[antigravityQuotaEpochCredentialKey] = newEpoch
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	return newEpoch, nil
}

// GetAntigravityQuotaEpoch 只读解析 credentials JSON 内嵌的失效纪元，缺省 0。
func (db *DB) GetAntigravityQuotaEpoch(ctx context.Context, accountID int64) (int64, error) {
	if db == nil || db.conn == nil || accountID <= 0 {
		return 0, nil
	}
	var raw any
	if err := db.conn.QueryRowContext(ctx, `SELECT credentials FROM accounts WHERE id=$1`, accountID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return antigravityQuotaEpochFromDocument(decodeCredentials(raw)), nil
}

// ApplyAntigravityCarryforwardOnUpdateCAS 是 UpdateAccountCredentialsCAS 的 Antigravity
// 受控 carryforward（Spec §4.1 代际变迁规则 #1）：例行同身份换票不得把已成功落库的配额
// 事实洗新，也不得递增失效纪元——严格保留原 observed_at/reset/ttl/applied_seq 与
// known_zero 状态，因此已确认的零额度门禁不会因为一次 token 轮换而解除。
//
// 该逻辑按 `upstream_type == "antigravity"` 条件化，Grok/Codex 走原路径完全不受影响。
// 调用方显式管理 `antigravity_quota`（写入新观测，或身份断裂时按既有策略清空）时，
// 本函数一律不覆盖其值；身份/family 变化时同样不搬运。
func ApplyAntigravityCarryforwardOnUpdateCAS(merged map[string]any, prevRaw string) {
	if merged == nil {
		return
	}
	if upstream, _ := merged["upstream_type"].(string); !strings.EqualFold(strings.TrimSpace(upstream), antigravityUpstreamType) {
		return
	}
	trimmed := strings.TrimSpace(prevRaw)
	if trimmed == "" {
		return
	}
	var prev map[string]any
	if err := json.Unmarshal([]byte(trimmed), &prev); err != nil || prev == nil {
		return
	}
	prevFamily := strings.TrimSpace(credentialStringFromMap(prev, "credential_family_id"))
	nextFamily := strings.TrimSpace(credentialStringFromMap(merged, "credential_family_id"))
	if prevFamily != "" && nextFamily != "" && prevFamily != nextFamily {
		return
	}
	if _, ok := merged[antigravityQuotaEpochCredentialKey]; !ok {
		if prevEpoch, found := int64FromJSONValue(prev[antigravityQuotaEpochCredentialKey]); found {
			merged[antigravityQuotaEpochCredentialKey] = prevEpoch
		}
	}
	if _, ok := merged[antigravityQuotaCredentialKey]; ok {
		return
	}
	if prevFacts := strings.TrimSpace(credentialStringFromMap(prev, antigravityQuotaCredentialKey)); prevFacts != "" {
		merged[antigravityQuotaCredentialKey] = prevFacts
	}
}

// HandleAntigravityRTConflict 在凭据 CAS 失败后按 **reload 结果** 三分支互斥分流
// （Spec §4.1 #5、验收 #39）。调用方约定：仍持有同一 family lease。
// 分支之外不得自行发明第四种覆盖写入，任何分支都绝不顺带接纳来源不匹配的配额观测。
func (db *DB) HandleAntigravityRTConflict(ctx context.Context, accountID int64, expectedGen int64, preRotationCredentials string, rotated map[string]any) (RTConflictOutcome, error) {
	if db == nil || db.conn == nil || accountID <= 0 {
		return 0, errors.New("antigravity RT conflict handling requires a live account id")
	}
	var pre map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(preRotationCredentials)), &pre); err != nil || pre == nil {
		return 0, fmt.Errorf("decode pre-rotation credentials: %w", err)
	}
	// reload 持久化行：不复用本方内存态。
	var raw any
	var durableGen int64
	var familyColumn string
	var status string
	err := db.conn.QueryRowContext(ctx,
		`SELECT credentials, credential_generation, COALESCE(credential_family_id,''), status FROM accounts WHERE id=$1`,
		accountID).Scan(&raw, &durableGen, &familyColumn, &status)
	if errors.Is(err, sql.ErrNoRows) {
		log.Printf("antigravity rt-conflict audit: account=%d outcome=isolate_identity reason=account_missing rotated_refresh_token_not_persisted=true", accountID)
		return RTConflictIsolateIdentity, nil
	}
	if err != nil {
		return 0, err
	}
	durable := decodeCredentials(raw)
	durableFamily := strings.TrimSpace(familyColumn)
	if durableFamily == "" {
		durableFamily = strings.TrimSpace(credentialStringFromMap(durable, "credential_family_id"))
	}
	if reason := antigravityIdentityBreakReason(pre, durable, durableFamily, status); reason != "" {
		return db.isolateAntigravityIdentity(ctx, accountID, durableGen, reason)
	}
	// 分支 (a)：durable winner 已完成同身份换票 → 信任 winner，绝不回写。
	if strings.TrimSpace(credentialStringFromMap(durable, "access_token")) != strings.TrimSpace(credentialStringFromMap(pre, "access_token")) {
		return RTConflictTrustWinner, nil
	}
	// 分支 (b) 的前提：expected generation 未变（本方仍是唯一持有已被消费 RT 的一方）。
	if expectedGen > 0 && durableGen != expectedGen {
		return RTConflictTrustWinner, nil
	}
	credentialUpdates := antigravityCredentialOnlyUpdates(rotated)
	if len(credentialUpdates) == 0 {
		// 本次没有可落库的凭据字段，等价于无可写事实：信任 durable 状态。
		return RTConflictTrustWinner, nil
	}
	_, applied, replayErr := db.UpdateAccountCredentialsCAS(ctx, accountID, durableGen, credentialUpdates)
	if replayErr == nil && applied {
		return RTConflictReplayCredentialOnly, nil
	}
	reason := "credential-only replay rejected by generation fence"
	if replayErr != nil {
		reason = "credential-only replay failed: " + replayErr.Error()
	}
	return db.isolateAntigravityIdentity(ctx, accountID, durableGen, reason)
}

// antigravityIdentityBreakReason 返回身份谱系断裂的原因；可验证连续时返回空串。
func antigravityIdentityBreakReason(pre, durable map[string]any, durableFamily, status string) string {
	if strings.EqualFold(strings.TrimSpace(status), "deleted") {
		return "account deleted"
	}
	if strings.TrimSpace(credentialStringFromMap(durable, antigravityQuotaPermanentRefreshErrorKey)) != "" {
		return "account already carries a permanent refresh error (isolated)"
	}
	preFamily := strings.TrimSpace(credentialStringFromMap(pre, "credential_family_id"))
	if preFamily != "" && durableFamily != "" && preFamily != durableFamily {
		return fmt.Sprintf("credential family changed from %s to %s", preFamily, durableFamily)
	}
	for _, key := range []string{"account_id", "email", "project_id"} {
		prevValue := strings.TrimSpace(credentialStringFromMap(pre, key))
		durableValue := strings.TrimSpace(credentialStringFromMap(durable, key))
		if prevValue == "" || durableValue == "" || prevValue == durableValue {
			continue
		}
		return fmt.Sprintf("identity scope field %s changed", key)
	}
	return ""
}

// isolateAntigravityIdentity 落永久失败标记并写审计（分支 (c)）：禁止把本次 RT 写入
// 当前身份，需管理员重新建立身份。
func (db *DB) isolateAntigravityIdentity(ctx context.Context, accountID, durableGen int64, reason string) (RTConflictOutcome, error) {
	message := "Antigravity refresh token consumed upstream could not be persisted: " + reason + "; administrative identity sync required"
	_, applied, err := db.UpdateAccountCredentialsCAS(ctx, accountID, durableGen, map[string]any{
		antigravityQuotaPermanentRefreshErrorKey: message,
	})
	log.Printf("antigravity rt-conflict audit: account=%d outcome=isolate_identity reason=%q rotated_refresh_token_not_persisted=true marker_applied=%v", accountID, reason, applied)
	if err != nil {
		return RTConflictIsolateIdentity, err
	}
	return RTConflictIsolateIdentity, nil
}

// antigravityCredentialOnlyUpdates 过滤出仅含凭据字段的 updates（分支 (b) 的重放不得
// 携带任何 source 观测）。
func antigravityCredentialOnlyUpdates(rotated map[string]any) map[string]any {
	allowed := []string{
		"upstream_type", "access_token", "refresh_token", "id_token", "expires_at",
		"oauth_client_key", "antigravity_client_id", "antigravity_client_secret", "oauth_scope",
	}
	filtered := make(map[string]any, len(allowed))
	if rotated == nil {
		return filtered
	}
	for _, key := range allowed {
		if value, ok := rotated[key]; ok {
			filtered[key] = value
		}
	}
	return filtered
}

// antigravityQuotaEpochFromDocument 解析 credentials 文档内嵌的失效纪元，缺省 0。
func antigravityQuotaEpochFromDocument(doc map[string]any) int64 {
	if doc == nil {
		return 0
	}
	value, _ := int64FromJSONValue(doc[antigravityQuotaEpochCredentialKey])
	return value
}

// antigravityQuotaFactsHasContent 判断配额事实是否还承载实质内容（只剩 schema_version
// 之类的骨架时视为已清空，与既有 `antigravity_quota = ""` 的清空写法一致）。
func antigravityQuotaFactsHasContent(facts map[string]any) bool {
	if facts == nil {
		return false
	}
	for key := range facts {
		if key != antigravityQuotaKeySchemaVersion {
			return true
		}
	}
	return false
}

// antigravityQuotaFactsForWrite 返回 credentials 文档内的配额事实对象，缺失时按 V2
// 契约创建骨架。返回的 map 是文档内的活引用，调用方就地修改即可。
func antigravityQuotaFactsForWrite(doc map[string]any) map[string]any {
	switch typed := doc[antigravityQuotaCredentialKey].(type) {
	case map[string]any:
		if typed != nil {
			return typed
		}
	case string:
		if trimmed := strings.TrimSpace(typed); trimmed != "" {
			var facts map[string]any
			if json.Unmarshal([]byte(trimmed), &facts) == nil && facts != nil {
				doc[antigravityQuotaCredentialKey] = facts
				return facts
			}
		}
	}
	facts := map[string]any{antigravityQuotaKeySchemaVersion: 2}
	doc[antigravityQuotaCredentialKey] = facts
	return facts
}

// antigravitySourceMetadataEntryForRead 只读地定位 credentials 文档内本 source 的元数据，
// 不创建任何对象（用于提交栅栏判定，拒绝路径不得改写文档）。
func antigravitySourceMetadataEntryForRead(doc map[string]any, source string) map[string]any {
	var facts map[string]any
	switch typed := doc[antigravityQuotaCredentialKey].(type) {
	case map[string]any:
		facts = typed
	case string:
		if trimmed := strings.TrimSpace(typed); trimmed != "" {
			var parsed map[string]any
			if json.Unmarshal([]byte(trimmed), &parsed) == nil {
				facts = parsed
			}
		}
	}
	if facts == nil {
		return map[string]any{}
	}
	metadata, _ := facts[antigravityQuotaKeySourceMetadata].(map[string]any)
	entry, _ := metadata[source].(map[string]any)
	if entry == nil {
		return map[string]any{}
	}
	return entry
}

// antigravitySourceMetadataForWrite 返回 credentials 文档内 facts.source_metadata.<source>
// 的活引用。
func antigravitySourceMetadataForWrite(doc map[string]any, source string) map[string]any {
	return antigravitySourceMetadataEntryForWrite(antigravityQuotaFactsForWrite(doc), source)
}

// antigravitySourceMetadataEntryForWrite 在配额事实对象内取（或建）本 source 的元数据。
func antigravitySourceMetadataEntryForWrite(facts map[string]any, source string) map[string]any {
	metadata, _ := facts[antigravityQuotaKeySourceMetadata].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		facts[antigravityQuotaKeySourceMetadata] = metadata
	}
	entry, _ := metadata[source].(map[string]any)
	if entry == nil {
		entry = map[string]any{}
		metadata[source] = entry
	}
	return entry
}

// antigravityMergeQuotaFacts 把本 source 的成功观测合并进既有事实：只覆盖本次返回的
// 分组/桶与自己的 source_metadata，未返回的旧桶保留原时标，其他来源保持不变；随后把
// applied_seq 推进到 mySeq 并写入成功时标。
func antigravityMergeQuotaFacts(facts map[string]any, incoming map[string]any, source string, mySeq int64, successAt string) {
	if facts == nil || incoming == nil {
		return
	}
	for key, value := range incoming {
		switch key {
		case antigravityQuotaKeyQuotaGroups:
			facts[key] = antigravityMergeQuotaGroups(facts[key], value)
		case antigravityQuotaKeySourceMetadata:
			antigravityMergeSourceMetadata(facts, value, source)
		default:
			facts[key] = value
		}
	}
	entry := antigravitySourceMetadataEntryForWrite(facts, source)
	appliedSeq, _ := int64FromJSONValue(entry[antigravitySourceMetaAppliedSeq])
	if mySeq > appliedSeq {
		entry[antigravitySourceMetaAppliedSeq] = mySeq
	}
	entry[antigravitySourceMetaLastSuccessAt] = successAt
}

// antigravityMergeQuotaGroups 按 display_name 合并分组、按 window 合并桶：incoming
// 未返回的旧桶原样保留（含其原 observed_at），incoming 返回的桶整体覆盖。
func antigravityMergeQuotaGroups(prevValue, incomingValue any) any {
	incoming, ok := incomingValue.([]any)
	if !ok || len(incoming) == 0 {
		// 未返回任何分组：既有事实原样保留。
		return prevValue
	}
	prevGroups, _ := prevValue.([]any)
	merged := make([]any, 0, len(prevGroups)+len(incoming))
	index := make(map[string]int, len(prevGroups)+len(incoming))
	position := 0
	for _, item := range prevGroups {
		group, _ := item.(map[string]any)
		if group == nil {
			continue
		}
		key := antigravityGroupIdentity(group, position)
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(group))
		position++
	}
	for _, item := range incoming {
		group, _ := item.(map[string]any)
		if group == nil {
			continue
		}
		key := antigravityGroupIdentity(group, position)
		position++
		if at, exists := index[key]; exists {
			target, _ := merged[at].(map[string]any)
			if target == nil {
				continue
			}
			for groupKey, groupValue := range group {
				if groupKey == "buckets" {
					continue
				}
				target[groupKey] = groupValue
			}
			target["buckets"] = antigravityMergeBuckets(target["buckets"], group["buckets"])
			continue
		}
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(group))
	}
	return merged
}

// antigravityMergeBuckets 按 window 合并桶；incoming 未返回的旧桶保留原时标。
func antigravityMergeBuckets(prevValue, incomingValue any) any {
	incoming, ok := incomingValue.([]any)
	if !ok || len(incoming) == 0 {
		return prevValue
	}
	prevBuckets, _ := prevValue.([]any)
	merged := make([]any, 0, len(prevBuckets)+len(incoming))
	index := make(map[string]int, len(prevBuckets)+len(incoming))
	position := 0
	for _, item := range prevBuckets {
		bucket, _ := item.(map[string]any)
		if bucket == nil {
			continue
		}
		key := antigravityBucketIdentity(bucket, position)
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(bucket))
		position++
	}
	for _, item := range incoming {
		bucket, _ := item.(map[string]any)
		if bucket == nil {
			continue
		}
		key := antigravityBucketIdentity(bucket, position)
		position++
		if at, exists := index[key]; exists {
			merged[at] = cloneJSONObject(bucket)
			continue
		}
		index[key] = len(merged)
		merged = append(merged, cloneJSONObject(bucket))
	}
	return merged
}

// antigravityMergeSourceMetadata 只合并本 source 的元数据，其他来源保持不变（Spec §4.1 #2）。
func antigravityMergeSourceMetadata(facts map[string]any, incomingValue any, source string) {
	incoming, _ := incomingValue.(map[string]any)
	if incoming == nil {
		return
	}
	incomingEntry, _ := incoming[source].(map[string]any)
	if incomingEntry == nil {
		return
	}
	entry := antigravitySourceMetadataEntryForWrite(facts, source)
	for key, value := range incomingEntry {
		if key == antigravitySourceMetaAppliedSeq || key == antigravitySourceMetaCurrentAttemptSeq {
			// 序号由预约与提交栅栏拥有，不接受快照文本回退。
			if existing, ok := int64FromJSONValue(entry[key]); ok {
				if next, nextOK := int64FromJSONValue(value); !nextOK || next < existing {
					continue
				}
			}
		}
		entry[key] = value
	}
}

func antigravityGroupIdentity(group map[string]any, position int) string {
	for _, key := range []string{"display_name", "group_id", "name"} {
		if value := strings.TrimSpace(credentialStringFromMap(group, key)); value != "" {
			return "g:" + strings.ToLower(value)
		}
	}
	return fmt.Sprintf("#g%d", position)
}

func antigravityBucketIdentity(bucket map[string]any, position int) string {
	for _, key := range []string{"window", "bucket_id"} {
		if value := strings.TrimSpace(credentialStringFromMap(bucket, key)); value != "" {
			return "b:" + strings.ToLower(value)
		}
	}
	return fmt.Sprintf("#b%d", position)
}

func cloneJSONObject(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
