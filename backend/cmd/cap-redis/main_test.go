package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

func TestPayloadDistributionHasFrozenQuantiles(t *testing.T) {
	values := make([]int, 600)
	for i := range values {
		values[i] = len(payloadFor(i))
	}
	got := ints(values)
	if got.P50 != 256 || got.P95 != 1024 || got.P99 != 4096 || got.Max != 16*1024 {
		t.Fatalf("payload quantiles=%+v", got)
	}
}

func TestMetricDiffAndSortedCommands(t *testing.T) {
	before := metricSnapshot{ReadBytes: 10, WriteBytes: 20, Dials: 2, Commands: 3, Pipelines: 1, Items: 2,
		ByCommand: map[string]uint64{"GET": 2, "EVAL": 1}}
	after := metricSnapshot{ReadBytes: 110, WriteBytes: 220, Dials: 5, Commands: 9, Pipelines: 3, Items: 8,
		ByCommand: map[string]uint64{"GET": 5, "EVAL": 3, "SET": 1}}
	got := diffMetrics(after, before)
	if got.ReadBytes != 100 || got.WriteBytes != 200 || got.Dials != 3 || got.Commands != 6 || got.Pipelines != 2 || got.Items != 6 {
		t.Fatalf("diff=%+v", got)
	}
	wantCommands := []commandCount{{Name: "EVAL", Count: 2}, {Name: "GET", Count: 3}, {Name: "SET", Count: 1}}
	if commands := sortedCommands(got.ByCommand); !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands=%+v want=%+v", commands, wantCommands)
	}
}

func TestFriendReconcileIsCorrectnessStop(t *testing.T) {
	if !isCorrectnessError(fmt.Errorf("wrapped: %w", friend.ErrFriendReconcile)) {
		t.Fatal("ErrFriendReconcile must stop pressure as correctness")
	}
	if isCorrectnessError(errors.New("redis timeout")) {
		t.Fatal("generic transport error must not be mislabeled as Friend reconciliation")
	}
	gates := hardGates{FriendAuthorityReconcile: 1}
	if gates.total() != 1 {
		t.Fatalf("hard gate total=%d", gates.total())
	}
}

func TestManifestRequiresExactIdentityAndPoolForC300(t *testing.T) {
	valid := manifestFile{SchemaVersion: schemaVersion, Frozen: frozenIdentity{TaskID: "CAP-REDIS-L1-R1",
		BackendBusinessCommit: "b", BackendEvidenceHead: "e", H5Commit: "h", BackendImageDigest: "bi", H5ImageDigest: "hi"},
		RedisConfigHash: "r", MachineSpecHash: "m", ModelLayoutVersion: "l", RouteSnapshotHash: "rs",
		LeaseProfileHash: "lp", ProfileHash: "p", RedisPoolSize: 400}
	if err := validateManifest(valid); err != nil {
		t.Fatalf("valid manifest: %v", err)
	}
	r2 := valid
	r2.SchemaVersion = schemaVersionR2
	r2.Frozen.TaskID = "CAP-REDIS-L1-R2"
	if err := validateManifest(r2); err != nil {
		t.Fatalf("valid R2 manifest: %v", err)
	}
	mismatched := r2
	mismatched.SchemaVersion = schemaVersion
	if err := validateManifest(mismatched); err == nil {
		t.Fatal("R2 task accepted R1 schema")
	}
	tooSmall := valid
	tooSmall.RedisPoolSize = 299
	if err := validateManifest(tooSmall); err == nil {
		t.Fatal("pool below c300 accepted")
	}
	missing := valid
	missing.Frozen.BackendBusinessCommit = ""
	if err := validateManifest(missing); err == nil {
		t.Fatal("missing business commit accepted")
	}
}

func TestParticipantDiagnosticDigestMatchesFrozenFriendLocatorVector(t *testing.T) {
	digest := participantDiagnosticDigest("invocation-fixture-1")
	if got, want := hex.EncodeToString(digest[:]), "39a87b120c5bb3a375f2f7b8dd5f04ef7b456ffa3be7546bbe5c39f57d6e4b86"; got != want {
		t.Fatalf("participant diagnostic digest=%s want=%s", got, want)
	}
}

func TestPublicDiagnosticDigestMatchesFrozenFriendLocatorVector(t *testing.T) {
	identity := friend.CommitIdentity{SubjectPlayerID: 10, CommandType: "RESERVE_FRIEND_SLOT", OperationID: "operation-1"}
	digest := publicDiagnosticDigest(identity, friend.FriendQuotaAuthority, "10")
	if got, want := hex.EncodeToString(digest[:]), "3c90f20630042a1d3bc7fa666645d9a57e10111b9ba72185710a8fec1c24df64"; got != want {
		t.Fatalf("public diagnostic digest=%s want=%s", got, want)
	}
}

func TestFailureAPIErrorUsesAllowlistRedaction(t *testing.T) {
	raw := "secret-pair-and-operation"
	got := sanitizedAPIError(fmt.Errorf("%w: %s", friend.ErrFriendReconcile, raw))
	if strings.Contains(got, raw) || got != "ERR_FRIEND_RECONCILE:REDACTED" {
		t.Fatalf("sanitized reconcile error=%q", got)
	}
	got = sanitizedAPIError(fmt.Errorf("transport failed for %s", raw))
	if strings.Contains(got, raw) || got != "ERR_TECHNICAL:REDACTED" {
		t.Fatalf("sanitized technical error=%q", got)
	}
}

func TestFailureDiagnosticReadGapForcesUnknownAndCollectionError(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "diagnosticgap")
	if err != nil {
		t.Fatal(err)
	}
	builder, err := friend.NewFriendshipKeyBuilder(friend.DefaultFriendRedisProfile(namespace))
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	environment := &benchmarkEnvironment{client: client, friendKeys: builder}
	pairID, err := friend.RelationIDFor(10, 20)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := builder.BucketFor(friend.FriendPairAuthority, pairID)
	if err != nil {
		t.Fatal(err)
	}
	recordKey, err := builder.Record(ref, pairID)
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, diagnosticErr := environment.diagnoseFriendAuthorityFailures(context.Background(), []failedOperation{{
		index: 3, err: friend.ErrFriendReconcile, sample: sampleResult{Diagnostic: &operationDiagnosticInput{
			OperationID: "raw-operation", AuthorityKind: friend.FriendPairAuthority, AuthorityID: pairID,
			AuthorityBucketID: ref.BucketID, PairLowPlayerID: 10, PairHighPlayerID: 20, recordKey: recordKey.String(),
			receiptKey: "exact-receipt", receiptField: "exact-field", attemptKey: "exact-attempt", attemptStateField: "exact-state",
		}},
	}})
	if diagnosticErr == nil || len(diagnostics) != 1 || diagnostics[0].FinalClassification != "UNKNOWN" || diagnostics[0].DiagnosticError == "" {
		t.Fatalf("diagnostics=%+v err=%v", diagnostics, diagnosticErr)
	}
}

func TestFailureDiagnosticsAreHashedBoundedAndSecretFree(t *testing.T) {
	rawOperation := "raw-operation-id-must-not-leak"
	rawPair := "raw-pair-id-must-not-leak"
	diagnostic := finalizeFailureDiagnostic(failureDiagnostic{OperationIndex: 7,
		OperationIDHash: diagnosticHash("cap-redis-operation-id-v1", rawOperation),
		PairIDHash:      diagnosticHash("cap-redis-pair-id-v1", rawPair), BucketID: 17,
		ErrorClass: "ERR_FRIEND_RECONCILE", FinalClassification: "NOT_COMMITTED",
		ClassificationBasis: "NO_PAIR_RECORD_RECEIPT_OR_ATTEMPT"})
	if len(diagnostic.OperationIDHash) != 64 || len(diagnostic.PairIDHash) != 64 || len(diagnostic.EvidenceHash) != 64 {
		t.Fatalf("diagnostic hashes=%+v", diagnostic)
	}
	index, aggregate := aggregateFailureEvidence([]failureDiagnostic{diagnostic})
	if len(index) != 1 || len(aggregate) != 64 || index[0].EvidenceHash != diagnostic.EvidenceHash {
		t.Fatalf("index=%+v aggregate=%q", index, aggregate)
	}
	encoded, err := json.Marshal(struct {
		Diagnostic failureDiagnostic
		Index      []failureHashIndex
	}{diagnostic, index})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || strings.Contains(string(encoded), rawOperation) || strings.Contains(string(encoded), rawPair) {
		t.Fatalf("raw identity leaked: %s", encoded)
	}
	if diagnosticHash("domain-a", rawOperation) == diagnosticHash("domain-b", rawOperation) {
		t.Fatal("diagnostic hashes are not domain separated")
	}
}

func TestAllScenariosAreUniquelyNamed(t *testing.T) {
	seen := map[string]bool{}
	for _, scenario := range allScenarios() {
		if scenario.name == "" || scenario.group == "" || scenario.prepare == nil {
			t.Fatalf("incomplete scenario=%+v", scenario)
		}
		if seen[scenario.name] {
			t.Fatalf("duplicate scenario %q", scenario.name)
		}
		seen[scenario.name] = true
	}
	if len(seen) != 24 {
		t.Fatalf("scenario count=%d want=24", len(seen))
	}
}
