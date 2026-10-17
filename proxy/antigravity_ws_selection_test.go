package proxy

import (
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
)

// TestResponsesWSAntigravityChannelSelectionSkipsWeighting 覆盖 Responses-over-WebSocket
// 入口（forwardResponsesWebSocketTurn）的选号上下文：该入口的候选池
// accountFilterForResponsesWebSocket 恒不含 Antigravity 账号
// （OpenAIResponsesUsesUpstreamWebsocket 对 AG 恒为 false，accountFilterForModel
// 直接拒绝 AG），因此 Antigravity 渠道 Key 打到该入口时 SelectionOptions.Provider
// 必须为空，否则 Store 侧会对 Codex relay 候选套用 Antigravity 加权打分，并绕过
// spread / HRW 一致哈希。
//
// 覆盖边界：驱动完整 WS handler 需要 websocket 升级与上游账号凭据，现有测试基础
// 设施成本过高，这里对入口实际调用的 helper 组装做单元测试——入口处
// selectionOpts 只来自 selectionOptionsForCandidatePool(..., true)，并原样传给
// nextRetryAccountForContinuationWithSelection /
// nextRetryAccountForSessionWithDispatchGuard
// 两条选号路径，helper 断言即等价于入口传出的 SelectionOptions.Provider 为空。
func TestResponsesWSAntigravityChannelSelectionSkipsWeighting(t *testing.T) {
	c := newChannelTestGin(database.UpstreamChannelAntigravity)

	// 前置事实：Responses WS 候选池确实不含 Antigravity 账号，Provider 为空才是正确语义。
	antigravityAccount := &auth.Account{
		DBID: 9140, UpstreamType: auth.UpstreamAntigravity, AccessToken: "google-token",
		AntigravityProjectID: "google-project", Status: auth.StatusReady,
		Models: []string{"gemini-3.8-flash-tiered"},
	}
	if antigravityAccount.OpenAIResponsesUsesUpstreamWebsocket() {
		t.Fatal("Antigravity accounts must never dial the upstream Responses WebSocket")
	}
	if accountFilterForResponsesWebSocket("gpt-5.5")(antigravityAccount) {
		t.Fatal("the Responses WS candidate pool must keep excluding Antigravity accounts")
	}

	// 入口组装：Antigravity 渠道 Key + antigravityExcluded=true → Provider 为空、Policy 保留。
	cases := []struct {
		name   string
		policy auth.DispatchPolicy
	}{
		{name: "standard", policy: auth.DispatchPolicyStandard},
		{name: "spark", policy: auth.DispatchPolicySpark},
	}
	for _, tc := range cases {
		opts := selectionOptionsForCandidatePool(c, "gpt-5.5", tc.policy, true)
		if opts.Provider != "" {
			t.Fatalf("%s: Provider = %q, want empty so the Antigravity weighting stays off", tc.name, opts.Provider)
		}
		if opts.Policy != tc.policy {
			t.Fatalf("%s: Policy = %v, want it preserved", tc.name, opts.Policy)
		}
	}

	// 对照：候选池可能含 Antigravity 账号的普通入口仍带渠道，加权避险照常启用。
	ordinary := selectionOptionsForCandidatePool(c, "gemini-3.8-flash-high", auth.DispatchPolicyStandard, false)
	if ordinary.Provider != database.UpstreamChannelAntigravity {
		t.Fatalf("ordinary Responses options Provider = %q, want the request channel", ordinary.Provider)
	}
}
