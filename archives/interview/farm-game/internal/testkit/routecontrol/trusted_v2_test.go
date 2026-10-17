//go:build linux

package routecontrol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrustedV2DerivesPassAndOfflineRehashesEveryArtifact(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run")
	writer, err := NewTrustedRunWriter(TrustedRunOptions{
		Root: root, TaskID: "RT2Q-FINAL", RunID: "rt2q-final-test", Challenge: strings.Repeat("1", 32),
		Level: EvidenceE3, Runtime: "process", RedisMode: "isolated", Scenarios: []string{"S0"},
		Git:       GitBinding{HeadCommit: strings.Repeat("a", 40), TreeHash: strings.Repeat("b", 40), Detached: true, Clean: true},
		Collector: "rt2q-route-control-harness", CollectorSHA256: strings.Repeat("c", 64),
		Keyspace: "redis-keyspace-tuple-v1:" + strings.Repeat("d", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteJSON("binaries.json", map[string]any{"count": 1}); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteJSON("configs.json", map[string]any{"hash": strings.Repeat("e", 64)}); err != nil {
		t.Fatal(err)
	}
	observer := ProcessIdentity{Name: "harness", Role: "observer", PID: os.Getpid(), BinarySHA256: strings.Repeat("c", 64), StartIdentity: "unit-start"}
	for index := 0; index < 7; index++ {
		if err := writer.Observe("snapshots.ndjson", "S0", "snapshot_installed", observer, map[string]any{"consumer": index}); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 100; index++ {
		if err := writer.Observe("business.ndjson", "S0", "business_resolve", observer, map[string]any{"ok": true, "index": index}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Observe("snapshots.ndjson", "S0", "hot_path_counters", observer, map[string]any{"rpc_delta": 0, "stream_delta": 0}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Observe("cleanup.ndjson", "S0", "cleanup_key", observer, map[string]any{"deleted": true, "exists": 0}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Observe("cleanup.ndjson", "S0", "cleanup_processes", observer, map[string]any{"residue": false}); err != nil {
		t.Fatal(err)
	}
	bindings := TrustedBindings{
		ConfigSetSHA256: strings.Repeat("1", 64), ProcessSetSHA256: strings.Repeat("2", 64),
		TLSPeerSetSHA256: strings.Repeat("3", 64), GatePeerSetSHA256: strings.Repeat("4", 64),
		RedisKeyLedgerSHA256: strings.Repeat("5", 64), OracleSHA256: strings.Repeat("6", 64),
	}
	oracle, err := writer.Finalize(bindings, nil)
	if err != nil {
		t.Fatal(err)
	}
	if oracle.Status != StatusPass {
		t.Fatalf("oracle=%+v", oracle)
	}
	manifest, verified, err := VerifyTrustedRun(root, strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Status != StatusPass || verified.Status != StatusPass || len(manifest.Artifacts) != len(TrustedArtifactNames()) {
		t.Fatalf("manifest=%+v oracle=%+v", manifest, verified)
	}

	file, err := os.OpenFile(filepath.Join(root, "business.ndjson"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString("{}\n")
	_ = file.Close()
	if _, _, err := VerifyTrustedRun(root, strings.Repeat("a", 40)); err == nil {
		t.Fatal("offline verifier accepted artifact drift")
	}
}

func TestTrustedV2E4PassBecomesCandidateWithoutSignature(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run")
	writer, err := NewTrustedRunWriter(TrustedRunOptions{
		Root: root, TaskID: "RT2Q-FINAL", RunID: "rt2q-final-e4-test", Challenge: strings.Repeat("2", 32),
		Level: EvidenceE4, Runtime: "container", RedisMode: "isolated", Scenarios: []string{"S0"},
		Git:       GitBinding{HeadCommit: strings.Repeat("a", 40), TreeHash: strings.Repeat("b", 40), Detached: true, Clean: true},
		Collector: "rt2q-route-control-harness", CollectorSHA256: strings.Repeat("c", 64),
		Keyspace: "redis-keyspace-tuple-v1:" + strings.Repeat("d", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.WriteJSON("binaries.json", map[string]any{"count": 1})
	_ = writer.WriteJSON("configs.json", map[string]any{"count": 1})
	observer := ProcessIdentity{Name: "harness", Role: "observer", ContainerID: strings.Repeat("a", 64), BinarySHA256: strings.Repeat("c", 64), StartIdentity: "unit-container"}
	for index := 0; index < 7; index++ {
		_ = writer.Observe("snapshots.ndjson", "S0", "snapshot_installed", observer, map[string]any{"consumer": index})
	}
	for index := 0; index < 100; index++ {
		_ = writer.Observe("business.ndjson", "S0", "business_resolve", observer, map[string]any{"ok": true})
	}
	_ = writer.Observe("snapshots.ndjson", "S0", "hot_path_counters", observer, map[string]any{"rpc_delta": 0, "stream_delta": 0})
	_ = writer.Observe("cleanup.ndjson", "S0", "cleanup_key", observer, map[string]any{"deleted": true, "exists": 0})
	_ = writer.Observe("cleanup.ndjson", "S0", "cleanup_processes", observer, map[string]any{"residue": false})
	bindings := TrustedBindings{ConfigSetSHA256: strings.Repeat("1", 64), ProcessSetSHA256: strings.Repeat("2", 64), TLSPeerSetSHA256: strings.Repeat("3", 64), GatePeerSetSHA256: strings.Repeat("4", 64), RedisKeyLedgerSHA256: strings.Repeat("5", 64), OracleSHA256: strings.Repeat("6", 64)}
	if _, err := writer.Finalize(bindings, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "run-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest TrustedManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Status != StatusInconclusive || !strings.Contains(manifest.Reason, "signature") {
		t.Fatalf("unsigned E4 manifest=%+v", manifest)
	}
	if _, derived, err := VerifyTrustedRun(root, strings.Repeat("a", 40)); err != nil || derived.Status != StatusPass {
		t.Fatalf("candidate verify derived=%+v err=%v", derived, err)
	}
}

func TestAggregateStatusIsFailDominant(t *testing.T) {
	if got := AggregateStatus(StatusPass, StatusInconclusive); got != StatusInconclusive {
		t.Fatalf("PASS + INCONCLUSIVE = %s", got)
	}
	if got := AggregateStatus(StatusInconclusive, StatusFail); got != StatusFail {
		t.Fatalf("INCONCLUSIVE + FAIL = %s", got)
	}
}

func TestS6OracleRequiresReceiptsReplayRejoinAndFourTruth(t *testing.T) {
	index := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S6": {
			"business_receipt": {
				mustRecord(t, map[string]any{"operation": "T1", "count": 1}),
				mustRecord(t, map[string]any{"operation": "T2", "count": 1}),
				mustRecord(t, map[string]any{"operation": "T3", "count": 1}),
			},
			"business_response_lost": {mustRecord(t, map[string]any{"operation": "T2", "committed": true})},
			"business_replay":        {mustRecord(t, map[string]any{"operation": "T2", "same_receipt": true, "duplicate_reward": false})},
			"subscription_rejoin": {mustRecord(t, map[string]any{
				"reason": "SUBSCRIPTION_LEASE_LOST", "fresh_attempt": true, "fresh_fence": true,
			})},
			"business_reconcile": {mustRecord(t, map[string]any{
				"authority": true, "snapshot": true, "holder": true, "plan": true,
			})},
		},
	}}
	for _, clause := range scenarioClauses("S6", index) {
		if clause.Status != StatusPass {
			t.Fatalf("clause %s status=%s reason=%s", clause.ID, clause.Status, clause.Reason)
		}
	}
	failIndex := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S6": {
			"business_receipt": {
				mustRecord(t, map[string]any{"operation": "T1", "count": 1}),
				mustRecord(t, map[string]any{"operation": "T2", "count": 1}),
			},
			"business_response_lost": {mustRecord(t, map[string]any{"operation": "T2", "committed": true})},
			"business_replay":        {mustRecord(t, map[string]any{"operation": "T2", "same_receipt": true, "duplicate_reward": false})},
			"subscription_rejoin": {mustRecord(t, map[string]any{
				"reason": "SUBSCRIPTION_LEASE_LOST", "fresh_attempt": true, "fresh_fence": true,
			})},
			"business_reconcile": {mustRecord(t, map[string]any{
				"authority": true, "snapshot": true, "holder": true, "plan": true,
			})},
		},
	}}
	for _, clause := range scenarioClauses("S6", failIndex) {
		if clause.ID == "s6.three_operations" && clause.Status == StatusPass {
			t.Fatal("incomplete T1-T3 set must not pass")
		}
	}
}

func TestDeadlineRefreshOracleRequiresProjectionSevenConsumersOldHorizonBusinessAndLocalHotPath(t *testing.T) {
	if !validScenario("deadline-refresh") {
		t.Fatal("deadline-refresh evidence scenario is not admitted")
	}
	business := make([]TrustedRecord, 100)
	for index := range business {
		business[index] = mustRecord(t, map[string]any{"ok": true, "sequence": index + 1})
	}
	facts := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"deadline-refresh": {
			"deadline_refresh_projected": {mustRecord(t, map[string]any{"changed_shards": 4, "one_complete_snapshot_per_revision": true, "generation_unchanged": true})},
			"consumer_metrics_applied":   {mustRecord(t, map[string]any{"consumer_count": 7})},
			"business_resolve":           business,
			"long_lived_route": {mustRecord(t, map[string]any{
				"old_deadline_crossed": true, "business_resolves": 100, "consumer_count": 7,
				"hot_path_rpc_delta": 0, "holder_callback_delta": 0, "authority_transfer_delta": 0,
			})},
		},
	}}
	for _, clause := range scenarioClauses("deadline-refresh", facts) {
		if clause.Status != StatusPass {
			t.Fatalf("clause %s status=%s reason=%s", clause.ID, clause.Status, clause.Reason)
		}
	}
	facts.byScenario["deadline-refresh"]["business_resolve"] = business[:99]
	for _, clause := range scenarioClauses("deadline-refresh", facts) {
		if clause.ID == "deadline.business_after_old_horizon" && clause.Status == StatusPass {
			t.Fatal("99 post-horizon business resolves passed")
		}
	}
}

func TestS5OracleRequiresLiveRejectExpiryAndFence(t *testing.T) {
	index := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S5": {
			"takeover_live_rejected": {mustRecord(t, map[string]any{"rejected": true})},
			"redis_expiry_proof":     {mustRecord(t, map[string]any{"expired": true})},
			"authority_transfer":     {{Sequence: 3}},
			"old_generation_request": {mustRecord(t, map[string]any{"fenced": true, "durable_unchanged": true})},
		},
	}}
	for _, clause := range scenarioClauses("S5", index) {
		if clause.Status != StatusPass {
			t.Fatalf("clause %s status=%s reason=%s", clause.ID, clause.Status, clause.Reason)
		}
	}
}

func TestS4OracleRequiresExactNegativeSetAndFinalZero(t *testing.T) {
	index := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S4": {
			"verify_negative": {
				mustRecord(t, map[string]any{"case": "pending", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "stale", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "unavailable", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "unknown", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "missing-replay", "nonzero": true}),
			},
			"snapshot_release": {mustRecord(t, map[string]any{"identical": true})},
			"verify_final":     {mustRecord(t, map[string]any{"zero": true, "checks": 11})},
		},
	}}
	for _, clause := range scenarioClauses("S4", index) {
		if clause.Status != StatusPass {
			t.Fatalf("clause %s status=%s reason=%s", clause.ID, clause.Status, clause.Reason)
		}
	}
	failIndex := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S4": {
			"verify_negative": {
				mustRecord(t, map[string]any{"case": "pending", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "stale", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "unavailable", "nonzero": true}),
				mustRecord(t, map[string]any{"case": "unknown", "nonzero": true}),
			},
			"snapshot_release": {mustRecord(t, map[string]any{"identical": true})},
			"verify_final":     {mustRecord(t, map[string]any{"zero": true, "checks": 11})},
		},
	}}
	for _, clause := range scenarioClauses("S4", failIndex) {
		if clause.ID == "s4.negative_verify" && clause.Status == StatusPass {
			t.Fatalf("incomplete negative set must not pass")
		}
	}
}

func TestS3OracleRequiresCommitUnknownBarrierAndSucceeded(t *testing.T) {
	index := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S3": {
			"transfer_evalsha_forwarded": {{Sequence: 1}},
			"transfer_reply_dropped":     {{Sequence: 2}},
			"pending_projection": {mustRecord(t, map[string]any{
				"authority_target": true, "snapshot_source": true, "plan_assigning": true, "authority_attempted": true,
			})},
			"holder_callback_stop":    {{Sequence: 4}},
			"holder_callback_wait":    {{Sequence: 5}},
			"holder_callback_facts":   {{Sequence: 6}},
			"holder_callback_prepare": {{Sequence: 7}},
			"authority_transfer":      {{Sequence: 8}},
			"plan_phase":              {mustRecord(t, map[string]any{"phase": "SUCCEEDED", "plan_id": "p"})},
		},
	}}
	clauses := scenarioClauses("S3", index)
	for _, clause := range clauses {
		if clause.Status != StatusPass {
			t.Fatalf("clause %s status=%s reason=%s", clause.ID, clause.Status, clause.Reason)
		}
	}
	failIndex := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S3": {
			"transfer_evalsha_forwarded": {{Sequence: 1}},
			"transfer_reply_dropped":     {{Sequence: 2}, {Sequence: 3}},
			"pending_projection": {mustRecord(t, map[string]any{
				"authority_target": true, "snapshot_source": true, "plan_assigning": true, "authority_attempted": true,
			})},
			"holder_callback_stop":    {{Sequence: 4}},
			"holder_callback_wait":    {{Sequence: 5}},
			"holder_callback_facts":   {{Sequence: 6}},
			"holder_callback_prepare": {{Sequence: 7}},
			"authority_transfer":      {{Sequence: 8}},
			"plan_phase":              {mustRecord(t, map[string]any{"phase": "SUCCEEDED"})},
		},
	}}
	for _, clause := range scenarioClauses("S3", failIndex) {
		if clause.ID == "s3.reply_dropped_once" && clause.Status != StatusFail {
			t.Fatalf("duplicate reply drop must fail, got %s", clause.Status)
		}
	}
}

func TestPhaseClauseAllowsActiveBetweenPreparingAndSucceeded(t *testing.T) {
	phase := func(seq uint64, name string, revision int) TrustedRecord {
		raw, err := json.Marshal(map[string]any{"phase": name, "revision": revision})
		if err != nil {
			t.Fatal(err)
		}
		return TrustedRecord{Sequence: seq, Kind: "plan_phase", Payload: raw}
	}
	withActive := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S2": {"plan_phase": {
			phase(1, "DRAINING", 3), phase(2, "ASSIGNING", 4), phase(3, "PREPARING", 5),
			phase(4, "ACTIVE", 6), phase(5, "SUCCEEDED", 7),
		}},
	}}
	clause := phaseClause(withActive, "S2", "s2.plan_monotonic", "plan_phase",
		[]string{"DRAINING", "ASSIGNING", "PREPARING", "SUCCEEDED"})
	if clause.Status != StatusPass {
		t.Fatalf("ACTIVE intermediate must pass: status=%s reason=%s", clause.Status, clause.Reason)
	}
	sparse := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S2": {"plan_phase": {
			phase(1, "DRAINING", 3), phase(2, "ASSIGNING", 4), phase(3, "PREPARING", 5), phase(4, "SUCCEEDED", 7),
		}},
	}}
	clause = phaseClause(sparse, "S2", "s2.plan_monotonic", "plan_phase",
		[]string{"DRAINING", "ASSIGNING", "PREPARING", "SUCCEEDED"})
	if clause.Status != StatusPass {
		t.Fatalf("sparse poll without ACTIVE must still pass: status=%s reason=%s", clause.Status, clause.Reason)
	}
	regress := factIndex{byScenario: map[string]map[string][]TrustedRecord{
		"S2": {"plan_phase": {
			phase(1, "DRAINING", 3), phase(2, "ASSIGNING", 4), phase(3, "PREPARING", 5), phase(4, "ASSIGNING", 6),
		}},
	}}
	clause = phaseClause(regress, "S2", "s2.plan_monotonic", "plan_phase",
		[]string{"DRAINING", "ASSIGNING", "PREPARING", "SUCCEEDED"})
	if clause.Status != StatusFail {
		t.Fatalf("phase regression must fail, got %s", clause.Status)
	}
}

func mustRecord(t *testing.T, facts map[string]any) TrustedRecord {
	t.Helper()
	raw, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	return TrustedRecord{Sequence: 1, Kind: "unit", Payload: raw}
}
