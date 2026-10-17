package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"farmgame/internal/testkit/capstaticroute"

	"github.com/alicebob/miniredis/v2"
)

func TestDryRunGeneratesRunAndNamespaceWithoutNetwork(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"--shard-count", "4"}, &output); err != nil {
		t.Fatal(err)
	}
	var intent capstaticroute.Intent
	if json.Unmarshal(output.Bytes(), &intent) != nil || !strings.HasPrefix(intent.RunID, "capr2-") ||
		!strings.HasPrefix(intent.NamespaceEnvironment, "l2r2") || intent.Activatable || intent.ActiveBindingAuthorized ||
		intent.CapacityConstantEligible || intent.Supports30mDAU || intent.BindingScope != "LOCAL_FUNCTIONAL_ONLY" {
		t.Fatalf("generated intent=%+v", intent)
	}
}

func TestDryRunDoesNotRequireOrReadRedis(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{
		"--run-id", "dry-run-1", "--namespace-environment", "l2r2dry1", "--shard-count", "4",
		"--redis-url-file", filepath.Join(t.TempDir(), "must-not-be-read"),
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	var intent capstaticroute.Intent
	if err := json.Unmarshal(output.Bytes(), &intent); err != nil {
		t.Fatal(err)
	}
	if intent.Activatable || intent.CompanyRedisContacted || intent.ActiveBindingAuthorized || intent.CapacityConstantEligible ||
		intent.Supports30mDAU || intent.BindingScope != "LOCAL_FUNCTIONAL_ONLY" || intent.ShardCount != 4 || len(intent.Holders) != 4 {
		t.Fatalf("intent=%+v", intent)
	}
}

func TestParseLocalRedisURLRejectsRemoteAndCredentials(t *testing.T) {
	for _, value := range []string{
		"redis://redis.company.example:6379/0",
		"rediss://127.0.0.1:6379/0",
		"redis://user:secret@127.0.0.1:6379/0",
		"redis://farm-local-redis.farm-l2-r2-deps.svc.cluster.local:6380/0",
		"redis://127.0.0.1:6379/1",
	} {
		if _, err := parseLocalRedisURL(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
	if _, err := parseLocalRedisURL("redis://127.0.0.1:5002/0"); err != nil {
		t.Fatal(err)
	}
}

func TestApplyLocalFourByFourAndReuseIdentity(t *testing.T) {
	server := miniredis.RunT(t)
	directory := t.TempDir()
	urlFile := filepath.Join(directory, "redis.url")
	identityFile := filepath.Join(directory, "identity.json")
	routeFile := filepath.Join(directory, "route.json")
	ledgerFile := filepath.Join(directory, "ledger.json")
	receiptFile := filepath.Join(directory, "receipt.json")
	if err := os.WriteFile(urlFile, []byte("redis://"+server.Addr()+"/0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{
		"--apply-local-profile-b", "--run-id", "apply-4", "--namespace-environment", "l2r2apply4", "--shard-count", "4",
		"--minimum-remaining-lease", "1s", "--redis-url-file", urlFile, "--identity-file", identityFile,
		"--route-out", routeFile, "--ledger-out", ledgerFile, "--receipt-out", receiptFile,
	}
	var output bytes.Buffer
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	firstIdentity, err := os.ReadFile(identityFile)
	if err != nil {
		t.Fatal(err)
	}
	var receipt capstaticroute.Receipt
	data, err := os.ReadFile(receiptFile)
	if err != nil || json.Unmarshal(data, &receipt) != nil {
		t.Fatalf("receipt read: %v", err)
	}
	firstReceipt := append([]byte(nil), data...)
	if !receipt.Complete || !receipt.BootstrapMaterializable || receipt.Activatable || receipt.Counters.Verified != 8 || receipt.CompanyRedisContacted ||
		!receipt.DurableSnapshotCommitted || receipt.DurableSnapshotRevision != 1 || receipt.DurableSnapshotChecksumSHA256 == "" ||
		receipt.ActiveBindingAuthorized || receipt.CapacityConstantEligible || receipt.Supports30mDAU || receipt.BindingScope != "LOCAL_FUNCTIONAL_ONLY" {
		t.Fatalf("receipt=%+v", receipt)
	}
	if _, err := os.Stat(routeFile); err != nil {
		t.Fatal(err)
	}
	ledgerData, err := os.ReadFile(ledgerFile)
	if err != nil {
		t.Fatal(err)
	}
	var ledger capstaticroute.Ledger
	if json.Unmarshal(ledgerData, &ledger) != nil || !ledger.Complete || !ledger.BootstrapMaterializable || ledger.Activatable || len(ledger.Records) != 8 || receipt.ExactLedgerSHA256 == "" {
		t.Fatalf("ledger=%+v", ledger)
	}
	output.Reset()
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := os.ReadFile(identityFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstIdentity, secondIdentity) {
		t.Fatal("identity changed across rerun")
	}
	data, err = os.ReadFile(receiptFile)
	if err != nil || json.Unmarshal(data, &receipt) != nil {
		t.Fatalf("second receipt read: %v", err)
	}
	if receipt.Counters.ExistingExact != 8 || receipt.Counters.ClaimApplied != 0 || !receipt.DurableSnapshotCommitted || receipt.DurableSnapshotRevision != 2 {
		t.Fatalf("rerun receipt=%+v", receipt)
	}
	routeData, err := os.ReadFile(routeFile)
	if err != nil {
		t.Fatal(err)
	}
	var durableRoute struct {
		GlobalRevision uint64 `json:"global_revision"`
	}
	if json.Unmarshal(routeData, &durableRoute) != nil || durableRoute.GlobalRevision != 2 {
		t.Fatalf("rerun durable route revision=%d", durableRoute.GlobalRevision)
	}
	history, err := os.ReadDir(filepath.Join(directory, "attempt-history"))
	if err != nil || len(history) != 1 {
		t.Fatalf("attempt history entries/error=%d/%v", len(history), err)
	}
	archivedReceipt, err := os.ReadFile(filepath.Join(directory, "attempt-history", history[0].Name(), "receipt.json"))
	if err != nil || !bytes.Equal(archivedReceipt, firstReceipt) {
		t.Fatalf("prior receipt was not retained exactly: %v", err)
	}
	for _, path := range []string{urlFile, identityFile, routeFile, ledgerFile} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("protected file %s mode/error=%v/%v", path, info.Mode().Perm(), err)
		}
	}
	if strings.Contains(string(data), server.Addr()) || strings.Contains(string(data), "redis://") {
		t.Fatal("receipt leaked Redis binding")
	}
	output.Reset()
	if err := run([]string{"--validate-bundle", "--require-fresh", "--route-out", routeFile, "--ledger-out", ledgerFile, "--receipt-out", receiptFile}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "EXACT_BUNDLE_VALID") || !strings.Contains(output.String(), `"company_redis_contacted":false`) {
		t.Fatalf("validation output=%s", output.String())
	}
}

func TestApplyRejectsAliasedInputAndOutputPaths(t *testing.T) {
	directory := t.TempDir()
	shared := filepath.Join(directory, "shared")
	if err := os.WriteFile(shared, []byte("redis://127.0.0.1:6379/0"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"--apply-local-profile-b", "--run-id", "alias", "--namespace-environment", "l2r2alias", "--shard-count", "4",
		"--redis-url-file", shared, "--identity-file", filepath.Join(directory, "identity.json"),
		"--route-out", shared, "--ledger-out", filepath.Join(directory, "ledger.json"), "--receipt-out", filepath.Join(directory, "receipt.json"),
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "distinct") {
		t.Fatalf("aliased paths err=%v", err)
	}
}

func TestProtectedRedisInputModeRejectedBeforeConnection(t *testing.T) {
	directory := t.TempDir()
	urlFile := filepath.Join(directory, "redis.url")
	if err := os.WriteFile(urlFile, []byte("redis://127.0.0.1:1/0"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"--apply-local-profile-b", "--run-id", "bad-mode", "--namespace-environment", "l2r2badmode", "--shard-count", "4",
		"--redis-url-file", urlFile, "--identity-file", filepath.Join(directory, "identity.json"),
		"--route-out", filepath.Join(directory, "route.json"), "--ledger-out", filepath.Join(directory, "ledger.json"), "--receipt-out", filepath.Join(directory, "receipt.json"),
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "protected input") {
		t.Fatalf("err=%v", err)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "route.json")); !os.IsNotExist(statErr) {
		t.Fatalf("route unexpectedly exists: %v", statErr)
	}
}

func TestIdentityFileMismatchRejects(t *testing.T) {
	directory := t.TempDir()
	identity := runtimeIdentity{SchemaVersion: identitySchema, RunID: "other", ProfileID: "functional", Generation: 1, ControllerNonce: "a1b2c3d4e5f60718293a4b5c"}
	data, _ := json.Marshal(identity)
	path := filepath.Join(directory, "identity.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"--run-id", "wanted", "--namespace-environment", "l2r2identity", "--identity-file", path}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("identity mismatch accepted")
	}
}

func Example_run_dryRun() {
	var output bytes.Buffer
	_ = run([]string{"--run-id", "example", "--namespace-environment", "l2r2example", "--shard-count", "4"}, &output)
	fmt.Println(strings.Contains(output.String(), `"activatable":false`))
	// Output: true
}
