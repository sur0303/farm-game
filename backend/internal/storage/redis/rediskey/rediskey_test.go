package rediskey

import (
	"testing"
)

// TestKeyFormats 验证 key 构造符合规范(纯离线,无需真库)。
func TestKeyFormats(t *testing.T) {
	opDigest := "0d7bf0e9baeed450c8feae9f953b2d8c6b84da480fc6436460470f2952d8ae60"
	cases := map[string]string{
		Session(1001):                      "session:current:{1001}:hash",
		RouteHint(1001):                    "route:owner.node:{1001}:string",
		FarmOnline(2002):                   "farm:online:{2002}:set",
		OperationResultCache(1001, "op-x"): "opresult:cache:{1001}:" + opDigest + ":string",
		SessionAuthorityEpoch():            "session:authority.epoch:global:string",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("key = %q, want %q", got, want)
		}
	}
}

// TestLegacyOwnerScopedKeyCompatibility only protects existing access/runtime
// key formats. It must not be treated as the Actor authority slot contract.
func TestLegacyOwnerScopedKeyCompatibility(t *testing.T) {
	owner := uint64(123456789)
	keys := []string{Session(owner), RouteHint(owner), ConnBinding(owner), LockOwner(owner)}
	tag := extractHashTag(keys[0])
	if tag == "" {
		t.Fatalf("first key %q has no hash tag", keys[0])
	}
	for _, k := range keys {
		if extractHashTag(k) != tag {
			t.Errorf("key %q hash tag = %q, want %q", k, extractHashTag(k), tag)
		}
	}
}

// extractHashTag 提取 {..} 内的 hash tag(Redis Cluster 规则:第一个 { 到之后第一个 } 之间)。
func extractHashTag(key string) string {
	start := -1
	for i, c := range key {
		if c == '{' {
			start = i + 1
			continue
		}
		if c == '}' && start >= 0 {
			return key[start:i]
		}
	}
	return ""
}
