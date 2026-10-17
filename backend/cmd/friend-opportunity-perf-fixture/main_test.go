package main

import (
	"strings"
	"testing"

	"farmgame/internal/friend"
)

func TestResolveIsolatedNamespaceRequiresRunID(t *testing.T) {
	if _, _, err := resolveIsolatedNamespace("", ""); err == nil {
		t.Fatal("empty run-id must fail closed")
	}
	if _, _, err := resolveIsolatedNamespace("   ", "dev"); err == nil {
		t.Fatal("blank run-id must fail closed")
	}
}

func TestResolveIsolatedNamespaceDerivesFoperfEnv(t *testing.T) {
	env, run, err := resolveIsolatedNamespace("c0smoke1", "")
	if err != nil {
		t.Fatal(err)
	}
	if env != "foperf-c0smoke1" || run != "c0smoke1" {
		t.Fatalf("env=%q run=%q", env, run)
	}
	env, run, err = resolveIsolatedNamespace("foperf-c0smoke2", "foperf-c0smoke2")
	if err != nil {
		t.Fatal(err)
	}
	if env != "foperf-c0smoke2" || run != "c0smoke2" {
		t.Fatalf("env=%q run=%q", env, run)
	}
}

func TestResolveIsolatedNamespaceAllowsCellScopedRunUnderSharedNamespace(t *testing.T) {
	// Matrix cells share one Redis namespace but use distinct operation run ids.
	env, run, err := resolveIsolatedNamespace("c0m0801f20", "foperf-c0m0801")
	if err != nil {
		t.Fatal(err)
	}
	if env != "foperf-c0m0801" || run != "c0m0801f20" {
		t.Fatalf("env=%q run=%q", env, run)
	}
}

func TestResolveIsolatedNamespaceRejectsSharedOrOversized(t *testing.T) {
	if _, _, err := resolveIsolatedNamespace("c0smoke1", "dev"); err == nil {
		t.Fatal("dev namespace must fail closed")
	}
	if _, _, err := resolveIsolatedNamespace("c0smoke1", "prod"); err == nil {
		t.Fatal("prod namespace must fail closed")
	}
	long := strings.Repeat("a", 40)
	if _, _, err := resolveIsolatedNamespace(long, ""); err == nil {
		t.Fatal("oversized derived namespace must fail closed")
	}
}

func TestResolveFixtureNamespaceAllowsOnlyExactQQFarmTuple(t *testing.T) {
	cfg := config{viewerCount: 2, runID: "friends50", namespaceEnv: "uctestscaleplanqqfarm20260816", zone: "z1",
		qqFarmConfirm: qqFarmLoadtestConfirmation, routePath: "redis-current"}
	env, run, err := resolveFixtureNamespace(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if env != "uctestscaleplanqqfarm20260816" || run != "friends50" {
		t.Fatalf("env=%q run=%q", env, run)
	}
	for name, mutate := range map[string]func(*config){
		"missing confirm": func(c *config) { c.qqFarmConfirm = "" },
		"wrong env":       func(c *config) { c.namespaceEnv = "prod" },
		"wrong zone":      func(c *config) { c.zone = "z2" },
		"wrong confirm":   func(c *config) { c.qqFarmConfirm = "yes" },
		"static route":    func(c *config) { c.routePath = "stale.json" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cfg
			mutate(&candidate)
			if _, _, err := resolveFixtureNamespace(candidate); err == nil {
				t.Fatal("unsafe qqfarm tuple must fail closed")
			}
		})
	}
}

func TestResolveFixtureNamespaceAllowsOneViewerQQFarmSmoke(t *testing.T) {
	cfg := config{viewerCount: 1, runID: "friends50-smoke", namespaceEnv: "uctestscaleplanqqfarm20260816", zone: "z1",
		qqFarmConfirm: qqFarmLoadtestConfirmation, routePath: "redis-current"}
	if _, _, err := resolveFixtureNamespace(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedRanges(t *testing.T) {
	end, ok := boundedRangeEnd(100, 50)
	if !ok || end != 149 {
		t.Fatalf("end=%d ok=%v", end, ok)
	}
	if _, ok := boundedRangeEnd(^uint64(0), 2); ok {
		t.Fatal("overflow must fail")
	}
	if !rangesOverlap(100, 149, 149, 200) || rangesOverlap(100, 149, 150, 200) {
		t.Fatal("range overlap boundary mismatch")
	}
}

func TestEncodeVerifyCursorRoundTrip(t *testing.T) {
	cursor := encodeVerifyCursor(1001, 7, friend.FriendIndexEntry{
		FriendPlayerID: 2002, RelationID: "r", RelationEpoch: 1, ActivatedAtMs: 42,
	})
	decoded, err := friend.DecodeListCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ViewerPlayerID != 1001 || decoded.RosterRevision != 7 ||
		decoded.LastFriendPlayerID != 2002 || decoded.LastActivatedAtMs != 42 {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestSanitizeRunID(t *testing.T) {
	if got := sanitizeRunID(" C0M-F20! "); got != "c0m-f20" {
		t.Fatalf("sanitize=%q", got)
	}
}
