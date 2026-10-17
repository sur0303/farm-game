package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/testkit/capaudit"
	"farmgame/internal/testkit/capcorrectness"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

func serviceExposing(t *testing.T, body string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	return server.URL + "/metrics"
}

func gathered(t *testing.T, exporter *capcorrectness.Exporter) map[string]float64 {
	t.Helper()
	registry, err := exporter.Registry()
	if err != nil {
		t.Fatal(err)
	}
	return served(t, registry)
}

func served(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.Metric {
			out[family.GetName()] += value(metric)
		}
	}
	return out
}

func value(metric *dto.Metric) float64 {
	if metric.Counter != nil {
		return metric.Counter.GetValue()
	}
	if metric.Gauge != nil {
		return metric.Gauge.GetValue()
	}
	return 0
}

// The exporter republishes each service's own counter under the contract name
// the oracle reads, and leaves every gate nobody measures out of the exposition.
func TestConfiguredServicesAreServedUnderContractNames(t *testing.T) {
	config := fileConfig{
		SchemaVersion: configSchema,
		Listen:        "127.0.0.1:0",
		Services: map[string]string{
			"farm-svr":  serviceExposing(t, "farm_change_version_gap_total 0\n"),
			"farm-gate": serviceExposing(t, "farm_gate_command_receipt_conflict_leak_total{command_type=\"PLANT_CROP\"} 0\nfarm_gate_command_receipt_conflict_leak_total{command_type=\"HARVEST_CROP\"} 2\n"),
		},
	}
	exporter, _, err := buildExporter(config)
	if err != nil {
		t.Fatal(err)
	}

	got := gathered(t, exporter)
	if got["farm_version_gap_total"] != 0 {
		t.Fatalf("farm_version_gap_total=%v, want an explicit 0", got["farm_version_gap_total"])
	}
	if _, served := got["farm_version_gap_total"]; !served {
		t.Fatal("a measured zero must be served, not omitted")
	}
	if got["receipt_conflict_leak_total"] != 2 {
		t.Fatalf("receipt_conflict_leak_total=%v, want 2 summed across command types", got["receipt_conflict_leak_total"])
	}
	for gate := range pending() {
		if _, served := got[gate]; served {
			t.Fatalf("%s has no instrumentation yet and must not be served", gate)
		}
	}
	for _, gate := range audited() {
		if _, served := got[gate]; served {
			t.Fatalf("%s needs storage to audit, which this config does not give it", gate)
		}
	}
}

// An audited gate is answered from storage, and only after the runner has
// actually read it. Serving it before the first pass would publish a zero the
// audit had not yet earned.
func TestAnAuditedGateIsServedOnlyAfterItHasBeenRead(t *testing.T) {
	exporter, runner, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: testAudit(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	before := gathered(t, exporter)
	for _, gate := range audited() {
		if _, served := before[gate]; served {
			t.Fatalf("%s was served before the audit had read anything", gate)
		}
	}

	runner.RunOnce(context.Background())
	after := gathered(t, exporter)
	for _, gate := range audited() {
		if value, served := after[gate]; !served || value != 0 {
			t.Fatalf("%s served=%v value=%v, want an explicit 0 once read", gate, served, value)
		}
	}
}

// The per-actor gates need to be told which actors the run uses, because Redis
// has no actor index. Without the ledger they stay unmeasured; with it they
// are answered.
func TestThePerActorGatesFollowTheLedger(t *testing.T) {
	withoutLedger := testAudit(t)
	exporter, runner, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: withoutLedger,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner.RunOnce(context.Background())
	served := gathered(t, exporter)
	for _, gate := range swept() {
		if _, found := served[gate]; found {
			t.Fatalf("%s was served without a ledger naming the actors to read", gate)
		}
	}

	withLedger := testAudit(t)
	withLedger.ActorLedger = writeLedger(t, "r-test")
	withLedger.RunID = "r-test"
	exporter, runner, err = buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: withLedger,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner.RunOnce(context.Background())
	served = gathered(t, exporter)
	for _, gate := range swept() {
		if value, found := served[gate]; !found || value != 0 {
			t.Fatalf("%s served=%v value=%v, want an explicit 0 once the actors were read", gate, found, value)
		}
	}
}

// Auditing one run against another run's identities would read the wrong
// actors and find them clean.
func TestALedgerFromAnotherRunIsRefused(t *testing.T) {
	audit := testAudit(t)
	audit.ActorLedger = writeLedger(t, "r-other")
	audit.RunID = "r-test"
	if _, _, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: audit,
	}); err == nil {
		t.Fatal("a ledger belonging to a different run must be refused")
	}
}

func writeLedger(t *testing.T, runID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "actor-ledger.json")
	body := fmt.Sprintf(`{"schema_version":%q,"run_id":%q,"actor_base":900000001,"actor_count":4}`,
		capaudit.LedgerSchema, runID)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAuditStorageMustBeLoopback(t *testing.T) {
	audit := testAudit(t)
	audit.RedisAddr = "198.18.0.2:6379"
	_, _, err := buildExporter(fileConfig{SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: audit})
	if err == nil {
		t.Fatal("auditing storage outside loopback could read a different deployment than the one under test")
	}
}

// Shard counts come from the route snapshot, so a missing one has to stop the
// audit rather than let it walk whatever shards it guesses.
func TestAuditWithoutARouteSnapshotIsRefused(t *testing.T) {
	audit := testAudit(t)
	audit.RouteSnapshot = filepath.Join(t.TempDir(), "absent.json")
	_, _, err := buildExporter(fileConfig{SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: audit})
	if err == nil {
		t.Fatal("an audit that cannot read the route snapshot does not know which shards exist")
	}
}

func testAudit(t *testing.T) *auditConfig {
	t.Helper()
	server := miniredis.RunT(t)
	route := filepath.Join(t.TempDir(), "route-snapshot.json")
	shardRoute := func(holder int) string {
		return fmt.Sprintf(`{"holder_index":%d,"state":"ACTIVE","shard_revision":1,"expected_writer_epoch":1,`+
			`"expected_lease_epoch":1,"lease_expire_at_ms":4102444800000}`, holder)
	}
	holder := func(name string, port int) string {
		return fmt.Sprintf(`{"instance_id":"%s","boot_id":"%s-test","endpoint":"127.0.0.1:%d","state":"ACTIVE","weight":1}`,
			name, name, port)
	}
	shape := `{"realm":"classic-farm","global_revision":1,"hash_version":2,"hash_seed":11,
"player":{"shard_count":1,"routes":[` + shardRoute(0) + `]},
"farm":{"shard_count":1,"routes":[` + shardRoute(1) + `]},
"holders":[` + holder("player-svr-0", 19100) + `,` + holder("farm-svr-0", 19101) + `]}`
	if err := os.WriteFile(route, []byte(shape), 0o600); err != nil {
		t.Fatal(err)
	}
	config := &auditConfig{
		RedisAddr: server.Addr(), WorkStorePrefix: "farm:test:workflow",
		RouteSnapshot: route, Namespace: "test", Zone: "z1",
	}
	claimShards(t, config)
	return config
}

// claimShards gives the fixture the authority rows a deployed shard has. The
// audit refuses to report on a keyspace nobody has claimed, so a fixture
// without them is not a deployment under test.
func claimShards(t *testing.T, config *auditConfig) {
	t.Helper()
	profile, shards, err := keyspaceOf(config)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: config.RedisAddr})
	defer func() { _ = client.Close() }()
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		count, err := shards.ShardCount(domain)
		if err != nil {
			t.Fatal(err)
		}
		for shard := uint32(0); shard < count; shard++ {
			ref, err := profile.AuthorityBucketForShard(shards, domain, shard)
			if err != nil {
				t.Fatal(err)
			}
			key, err := keys.ShardAuthority(ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(context.Background(), key.String(), "writer_epoch", "1").Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Reading a keyspace nobody writes finds nothing wrong in it. That has to cost
// the audit gates rather than pass them, or a mistyped namespace would report
// a clean run.
func TestAnEmptyKeyspaceCostsEveryAuditGate(t *testing.T) {
	audit := testAudit(t)
	audit.Namespace = "elsewhere"
	audit.ActorLedger = writeLedger(t, "r-test")
	audit.RunID = "r-test"
	exporter, runner, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0", Audit: audit,
	})
	if err != nil {
		t.Fatal(err)
	}
	runner.RunOnce(context.Background())

	served := gathered(t, exporter)
	for _, gate := range append(audited(), swept()...) {
		if _, found := served[gate]; found {
			t.Fatalf("%s reported a value from a keyspace the deployment does not use", gate)
		}
	}
}

// A service missing from the config costs the gates it owns. The run is then
// incompletely observed, which is a different verdict from a clean one.
func TestAnUnconfiguredServiceCostsItsGates(t *testing.T) {
	exporter, _, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0",
		Services: map[string]string{"farm-svr": serviceExposing(t, "farm_change_version_gap_total 0\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, served := gathered(t, exporter)["receipt_conflict_leak_total"]; served {
		t.Fatal("receipt_conflict_leak_total was served without farm-gate in the config")
	}
}

// Every gate has to be accounted for exactly once, so that no invariant can be
// forgotten by simply never being mentioned, and none can be claimed twice by
// two sources that could disagree.
func TestEveryGateHasExactlyOneSource(t *testing.T) {
	isAudited := map[string]bool{}
	for _, gate := range append(audited(), swept()...) {
		isAudited[gate] = true
	}
	for _, gate := range capcorrectness.Gates() {
		sources := 0
		for _, claimed := range []bool{scraped()[gate.Name] != binding{}, isAudited[gate.Name], pending()[gate.Name] != ""} {
			if claimed {
				sources++
			}
		}
		if sources != 1 {
			t.Fatalf("%s is claimed by %d sources, want exactly 1", gate.Name, sources)
		}
	}
}

// Status is where an operator learns why a run could not be judged, so an
// absent gate has to arrive with its reason attached.
func TestStatusNamesEveryGateAndWhyItIsMissing(t *testing.T) {
	exporter, _, err := buildExporter(fileConfig{
		SchemaVersion: configSchema, Listen: "127.0.0.1:0",
		Services: map[string]string{"farm-svr": serviceExposing(t, "farm_change_version_gap_total 3\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	observations := exporter.Snapshot(context.Background())
	if len(observations) != len(capcorrectness.Gates()) {
		t.Fatalf("status reported %d gates, want all %d", len(observations), len(capcorrectness.Gates()))
	}
	for _, observation := range observations {
		switch observation.Gate {
		case "farm_version_gap_total":
			if observation.Problem != "" || observation.Value != 3 {
				t.Fatalf("measured gate reported %+v", observation)
			}
		default:
			if observation.Problem == "" {
				t.Fatalf("%s is absent from /metrics but reports no problem", observation.Gate)
			}
		}
	}
}
