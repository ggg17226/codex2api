package proxy

import (
	"testing"

	"github.com/codex2api/auth"
	"github.com/codex2api/database"
)

// TestCompactSelectionOptionsSkipAntigravityWeighting 覆盖 compact 入口的选号上下文：
// compact 执行器与 native remote compaction 的候选池显式排除 Antigravity 账号，因此
// Antigravity 渠道 Key 打到这两个入口时 SelectionOptions.Provider 必须为空，否则
// Store 侧会对 Codex relay 候选套用 Antigravity 加权打分，并绕过 spread / HRW。
func TestCompactSelectionOptionsSkipAntigravityWeighting(t *testing.T) {
	c := newChannelTestGin(database.UpstreamChannelAntigravity)

	// 前置事实：compact 候选池确实不含 Antigravity 账号，Provider 为空才是正确语义。
	antigravityAccount := &auth.Account{
		DBID: 9130, UpstreamType: auth.UpstreamAntigravity, AccessToken: "google-token",
		AntigravityProjectID: "google-project", Status: auth.StatusReady,
		Models: []string{"gemini-3.8-flash-tiered"},
	}
	if accountFilterForCompactResponsesModelWithOriginal("gemini-3.8-flash-high", "gemini-3.8-flash-high", false)(antigravityAccount) {
		t.Fatal("the compact executor pool must keep excluding Antigravity accounts")
	}
	if accountFilterForInlineCompactionModelWithOriginal("gemini-3.8-flash-high", "gemini-3.8-flash-high", false)(antigravityAccount) {
		t.Fatal("the native remote compaction pool must keep excluding Antigravity accounts")
	}

	// 对照：普通 Responses 请求（候选池可能含 Antigravity 账号）仍带渠道，加权避险照常启用。
	ordinary := selectionOptionsForCandidatePool(c, "gemini-3.8-flash-high", auth.DispatchPolicyStandard, false)
	if ordinary.Provider != database.UpstreamChannelAntigravity {
		t.Fatalf("ordinary Responses options Provider = %q, want the request channel", ordinary.Provider)
	}
	if ordinary.Model != "gemini-3.8-flash-high" || ordinary.Policy != auth.DispatchPolicyStandard {
		t.Fatalf("ordinary Responses options = %+v, want the model and policy preserved", ordinary)
	}

	// compact 执行器与 native remote compaction：只保留 Policy，不启用加权。
	cases := []struct {
		name   string
		policy auth.DispatchPolicy
	}{
		{name: "compact executor", policy: auth.DispatchPolicyStandard},
		{name: "native remote compaction", policy: auth.DispatchPolicySpark},
	}
	for _, tc := range cases {
		opts := selectionOptionsForCandidatePool(c, "gpt-5.6-sol", tc.policy, true)
		if opts.Provider != "" {
			t.Fatalf("%s: Provider = %q, want empty so the Antigravity weighting stays off", tc.name, opts.Provider)
		}
		if opts.Policy != tc.policy {
			t.Fatalf("%s: Policy = %v, want it preserved", tc.name, opts.Policy)
		}
	}
}
