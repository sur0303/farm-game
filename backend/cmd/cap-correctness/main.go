// Command cap-correctness serves the hard correctness gates of a capacity run
// at the endpoint the oracle adapter scrapes.
//
// Which service counts a gate, and under which name, is a property of the code
// and lives in bindings below. The config only says where those services are
// reachable, so a deployment cannot quietly repoint a gate at something else.
// Gates with no instrumentation yet are declared unmeasured rather than
// omitted: the oracle then reports the run as incompletely observed, which is
// the true answer, instead of reading a zero nobody produced.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/testkit/capaudit"
	"farmgame/internal/testkit/capcorrectness"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const configSchema = "cap-correctness-config.v1"

type fileConfig struct {
	SchemaVersion string `json:"schema_version"`
	Listen        string `json:"listen"`
	// Services maps a service name used by the bindings to its loopback
	// metrics endpoint, normally a port-forward to the pod under test.
	Services map[string]string `json:"services"`
	// Audit is the storage the audit checks read. Without it the gates that
	// can only be decided from stored state stay unmeasured.
	Audit *auditConfig `json:"audit,omitempty"`
}

type auditConfig struct {
	// RedisAddr is a loopback address, normally a port-forward to the Redis
	// the services under test use.
	RedisAddr string `json:"redis_addr"`
	// ProjectionRedisAddr is where friend list projections live when the
	// deployment splits them off the authority. Empty means the same Redis;
	// pointing the audit at the wrong half would read someone else's lists.
	ProjectionRedisAddr string `json:"projection_redis_addr,omitempty"`
	// WorkStorePrefix is WORKFLOW_STORE_PREFIX as given to the services.
	WorkStorePrefix string `json:"work_store_prefix"`
	// RouteSnapshot is the snapshot the services route by. Shard counts come
	// from it rather than from this file, so a resharding cannot leave the
	// audit quietly walking a stale fraction of the keyspace.
	RouteSnapshot string `json:"route_snapshot"`
	// Namespace and Zone select the Redis keyspace, matching
	// REDIS_NAMESPACE_ENVIRONMENT and REDIS_NAMESPACE_ZONE.
	Namespace string `json:"namespace"`
	Zone      string `json:"zone"`
	// DedicatedScheduleIndex mirrors FARM_SCHEDULE_DUE_INDEX_MODE.
	DedicatedScheduleIndex bool `json:"dedicated_schedule_index"`
	// ActorLedger names the identities the run may touch. Redis has no actor
	// index, so the per-actor gates cannot be observed without it.
	ActorLedger string `json:"actor_ledger"`
	// RunID must match the ledger, so one run cannot be audited against
	// another run's identities and found clean.
	RunID string `json:"run_id"`
	// IntervalSeconds is how often the checks re-read storage. Checks walk
	// indexes, so this is deliberately slower than a metrics scrape.
	IntervalSeconds int `json:"interval_seconds"`
}

// binding names the service that counts a gate and the metric it counts it
// under. The exporter republishes it under the contract name.
type binding struct {
	service string
	metric  string
}

// scraped lists every gate a service already counts for itself.
func scraped() map[string]binding {
	return map[string]binding{
		"farm_version_gap_total":      {"farm-svr", "farm_change_version_gap_total"},
		"receipt_conflict_leak_total": {"farm-gate", "farm_gate_command_receipt_conflict_leak_total"},
		// Disconnect removes a connection together with its lease schedules, so
		// a lease that reaches its deadline locally is one the Gate was still
		// holding and failed to renew. That is the unexpected expiry.
		"unexpected_lease_expiry_total": {"farm-gate", "farm_gate_subscription_lease_local_expired_total"},
	}
}

// audited lists the gates decided by reading stored state directly, because
// the services only refuse violations rather than counting ones that got past.
func audited() []string {
	return []string{capaudit.StepUnknownGate, capaudit.DueResidualGate}
}

// swept lists the gates decided by reading the run's own actors, which needs
// the ledger naming them.
func swept() []string {
	return []string{
		capaudit.RecordVersionRegressionGate,
		capaudit.StaleHolderCommitGate,
		capaudit.AuthorityClientMismatchGate,
		capaudit.DuplicateGrantGate,
		capaudit.DuplicateDebitGate,
		capaudit.FriendIndexGate,
	}
}

// pending states, for each remaining gate, what has to exist before it can be
// observed. These are not excuses in a comment: each string is served on
// /status, so the reason a run was incomplete is always at hand.
func pending() map[string]string {
	return map[string]string{
		"wrong_scope_dispatch_total":  "needs the independent audit of which scope served each actor",
		"silent_drop_total":           "needs a conservation check between what the server pushed and what the client received; the delivery path forbids selective drops, so there is no branch to count",
		"invalid_order_total":         "needs an order oracle over the applied sequence",
		"h5_pending_journal_residual": "needs the H5 client witness; the pending journal lives in the browser and this repository holds no implementation of it",
	}
}

func main() {
	configPath := flag.String("config", "", "path to reviewed cap-correctness config")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("--config is required")
	}
	if err := run(*configPath); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string) error {
	file, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	exporter, runner, err := buildExporter(file)
	if err != nil {
		return err
	}
	registry, err := exporter.Registry()
	if err != nil {
		return err
	}
	// The keyspace is read here, before the endpoint serves anything: a count
	// taken once the run is under way would read the run's own writes as
	// residue and climb as it proceeds.
	residual := captureNamespaceResidual(context.Background(), file.Audit)
	if err := residual.publish(registry); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", statusHandler(exporter, []precheckObservation{residual}))

	listener, err := net.Listen("tcp", file.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second,
	}
	log.Printf("cap-correctness listen=%s measured=%d unmeasured=%d",
		file.Listen, len(measurable(file)), len(capcorrectness.Gates())-len(measurable(file)))
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if runner != nil {
		go runner.Run(ctx)
	}
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// status is what an operator reads to learn why a run could not be judged:
// every gate with its value or its problem, and every environment fact that had
// to hold before the run began.
type status struct {
	Gates    []capcorrectness.Observation `json:"gates"`
	Precheck []precheckObservation        `json:"precheck"`
}

func statusHandler(exporter *capcorrectness.Exporter, precheck []precheckObservation) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := json.MarshalIndent(status{Gates: exporter.Snapshot(r.Context()), Precheck: precheck}, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(append(body, '\n'))
	}
}

func loadConfig(path string) (fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("read config: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var file fileConfig
	if err := decoder.Decode(&file); err != nil {
		return fileConfig{}, fmt.Errorf("decode config: %w", err)
	}
	if file.SchemaVersion != configSchema {
		return fileConfig{}, fmt.Errorf("unsupported cap-correctness config schema %q", file.SchemaVersion)
	}
	if err := validateLoopback(file.Listen); err != nil {
		return fileConfig{}, err
	}
	return file, nil
}

// buildExporter binds a scrape for every gate whose service is configured, an
// audit for every gate decided from storage, and declares the rest unmeasured.
// Anything left out of the config therefore costs the gates it owns, and says
// so, rather than passing them by default.
func buildExporter(file fileConfig) (*capcorrectness.Exporter, *capaudit.Runner, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	producers := map[string]capcorrectness.Producer{}
	for gate, bound := range scraped() {
		endpoint, configured := file.Services[bound.service]
		if !configured {
			producers[gate] = capcorrectness.Unmeasured(
				fmt.Sprintf("service %s is not in the config, so %s cannot be read", bound.service, bound.metric))
			continue
		}
		producer, err := capcorrectness.NewScrapeProducer(client, endpoint, bound.metric, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("gate %s: %w", gate, err)
		}
		producers[gate] = producer
	}
	runner, err := buildAuditRunner(file.Audit)
	if err != nil {
		return nil, nil, err
	}
	for _, gate := range append(audited(), swept()...) {
		switch {
		case runner == nil:
			producers[gate] = capcorrectness.Unmeasured("storage to audit is not in the config")
		case !runnerAnswers(runner, gate):
			producers[gate] = capcorrectness.Unmeasured("the run's actor ledger is not in the config, so its actors cannot be read")
		default:
			producers[gate] = auditProducer(runner, gate)
		}
	}
	for gate, reason := range pending() {
		producers[gate] = capcorrectness.Unmeasured(reason)
	}
	exporter, err := capcorrectness.New(producers, 10*time.Second)
	if err != nil {
		return nil, nil, err
	}
	return exporter, runner, nil
}

// auditProducer serves the runner's last answer. The runner decides whether
// that answer is still usable, so a scrape never revives a stale count.
func auditProducer(runner *capaudit.Runner, gate string) capcorrectness.Producer {
	return capcorrectness.ProducerFunc(func(context.Context) (float64, error) {
		return runner.Value(gate)
	})
}

func buildAuditRunner(config *auditConfig) (*capaudit.Runner, error) {
	if config == nil {
		return nil, nil
	}
	if err := validateLoopback(config.RedisAddr); err != nil {
		return nil, fmt.Errorf("audit redis: %w", err)
	}
	interval := time.Duration(config.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	client := redis.NewClient(&redis.Options{Addr: config.RedisAddr})
	stepUnknown, err := capaudit.NewStepUnknownCheck(capaudit.StepUnknownConfig{
		Client: client, Prefix: config.WorkStorePrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("audit work store: %w", err)
	}
	profile, shards, err := keyspaceOf(config)
	if err != nil {
		return nil, err
	}
	dueResidual, err := capaudit.NewDueResidualCheck(capaudit.DueResidualConfig{
		Client: client, Profile: profile, Shards: shards,
		DedicatedScheduleIndex: config.DedicatedScheduleIndex,
	})
	if err != nil {
		return nil, fmt.Errorf("audit due indexes: %w", err)
	}
	groups, perActor, err := ledgerChecks(config, client, profile, shards)
	if err != nil {
		return nil, err
	}
	presence, err := capaudit.NewShardPresence(capaudit.ShardPresenceConfig{
		Client: client, Profile: profile, Shards: shards,
	})
	if err != nil {
		return nil, fmt.Errorf("audit keyspace presence: %w", err)
	}
	checks := append([]capaudit.Check{stepUnknown, dueResidual}, perActor...)
	return capaudit.NewRunner(capaudit.Config{
		Groups: groups, Before: []capaudit.Precondition{presence},
		Interval: interval, FreshFor: 3 * interval, Timeout: interval,
	}, checks...)
}

// ledgerChecks builds everything that has to walk the run's own actors, which
// is everything Redis offers no index for. Without the ledger these are simply
// absent, and the gates they would answer stay unmeasured.
func ledgerChecks(config *auditConfig, client *redis.Client, profile rediskey.PhysicalProfile,
	shards rediskey.AuthorityShardResolver) ([]capaudit.GroupCheck, []capaudit.Check, error) {
	if config.ActorLedger == "" {
		return nil, nil, nil
	}
	ledger, err := capaudit.LoadLedger(config.ActorLedger, config.RunID)
	if err != nil {
		return nil, nil, err
	}
	sweep, err := capaudit.NewActorSweep(capaudit.ActorSweepConfig{
		Client: client, Profile: profile, Shards: shards, Ledger: ledger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("audit actors: %w", err)
	}
	friendIndex, err := friendIndexCheck(config, client, profile.Namespace, ledger)
	if err != nil {
		return nil, nil, fmt.Errorf("audit friend lists: %w", err)
	}
	return []capaudit.GroupCheck{sweep}, []capaudit.Check{friendIndex}, nil
}

// friendIndexCheck reads friend lists through the same repositories the
// services write them with, so the audit cannot disagree with the deployment
// about where a list lives or how a row is encoded.
func friendIndexCheck(config *auditConfig, authority *redis.Client, namespace rediskey.Namespace,
	ledger capaudit.Ledger) (*capaudit.FriendIndexCheck, error) {
	projection := redis.UniversalClient(authority)
	if config.ProjectionRedisAddr != "" {
		if err := validateLoopback(config.ProjectionRedisAddr); err != nil {
			return nil, fmt.Errorf("projection redis: %w", err)
		}
		projection = redis.NewClient(&redis.Options{Addr: config.ProjectionRedisAddr})
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForDomains(authority, projection,
		friend.RedisFriendshipOptions{Profile: friend.DefaultFriendRedisProfile(namespace)})
	if err != nil {
		return nil, err
	}
	reconciler, err := friend.NewFriendIndexReconciler(repositories.Index, repositories.Pair, repositories.Quota)
	if err != nil {
		return nil, err
	}
	return capaudit.NewFriendIndexCheck(capaudit.FriendIndexConfig{Reconciler: reconciler, Ledger: ledger})
}

func runnerAnswers(runner *capaudit.Runner, gate string) bool {
	return slices.Contains(runner.Gates(), gate)
}

// keyspaceOf reconstructs the placement the services use. The shard counts come
// from the route snapshot itself, so the audit walks the same partitioning the
// deployment does.
func keyspaceOf(config *auditConfig) (rediskey.PhysicalProfile, rediskey.AuthorityShardResolver, error) {
	resolver, err := actorrouting.LoadStaticResolver(config.RouteSnapshot)
	if err != nil {
		return rediskey.PhysicalProfile{}, nil, fmt.Errorf("load route snapshot: %w", err)
	}
	snapshot := resolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return rediskey.PhysicalProfile{}, nil, fmt.Errorf("build route calculator: %w", err)
	}
	namespace, err := rediskey.NewNamespace(config.Namespace, config.Zone)
	if err != nil {
		return rediskey.PhysicalProfile{}, nil, fmt.Errorf("build namespace: %w", err)
	}
	return rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}, calculator, nil
}

func measurable(file fileConfig) []string {
	var out []string
	for gate, bound := range scraped() {
		if _, configured := file.Services[bound.service]; configured {
			out = append(out, gate)
		}
	}
	if file.Audit != nil {
		out = append(out, audited()...)
		if file.Audit.ActorLedger != "" {
			out = append(out, swept()...)
		}
	}
	return out
}

func validateLoopback(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen must be host:port: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("cap-correctness must listen on an explicit loopback address")
	}
	return nil
}
