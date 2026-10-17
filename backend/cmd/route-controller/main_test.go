package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	routecontrolpb "farmgame/api/gen/routecontrol"
	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/routeplan"
	"farmgame/internal/storage/authoritystore"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type recoveryHolderReaderFunc func(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error)

type preparedStatusController struct {
	status *holdercontrolpb.ShardStatus
}

func (*preparedStatusController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*preparedStatusController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*preparedStatusController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*preparedStatusController) ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error {
	return nil
}
func (c *preparedStatusController) GetStatus(context.Context, bool) (*holdercontrolpb.ShardStatus, error) {
	return c.status, nil
}

func (f recoveryHolderReaderFunc) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return f(ctx, domain, holderID)
}

type planTTLAuthorityReaderFunc func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)

func (f planTTLAuthorityReaderFunc) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	return f(ctx, domain, shard)
}

func validCommandConfig() config {
	return config{
		listen:                   "127.0.0.1:0",
		controllerID:             "controller-a",
		boot:                     "boot-a",
		redisURL:                 "redis://127.0.0.1:6379/0",
		namespaceEnvironment:     "test",
		namespaceZone:            "z1",
		redisOperation:           time.Second,
		maxSnapshotBytes:         1 << 20,
		maxClients:               4,
		queueSize:                2,
		maxMessageBytes:          1 << 20,
		tlsCert:                  "/cert.pem",
		tlsKey:                   "/key.pem",
		tlsCA:                    "/ca.pem",
		allowedClientIdentity:    []string{"client.test"},
		planReadIdentity:         []string{"plan-reader.test"},
		operatorIdentity:         []string{"operator.test"},
		singleControllerContract: routeplan.SingleControllerContract,
		physicalProfile:          "redis-physical-v1",
		authorityLayout:          "v1",
		workflowLayout:           "v1",
		workflowHashVersion:      "1",
		workflowBucketCount:      "256",
		gateVerifierClientCert:   "/gate-client.pem",
		gateVerifierClientKey:    "/gate-client.key",
		gateVerifierClientCA:     "/gate-ca.pem",
		gateVerifierPeers: []gateverification.Peer{{
			Name: "gate-a", Address: "127.0.0.1:19091", ServerName: "gate.test", ExpectedIdentity: "gate.test",
		}},
		gateVerifierTimeout: 5 * time.Second,
		planOperation:       defaultPlanOperationTimeout,
		planTTL:             defaultPlanTTL,
		leaseProjectionMode: leaseProjectionModeDisabled,
	}
}

func TestRouteControllerConfigRequiresReadOnlyRuntimeIdentityAndTLS(t *testing.T) {
	base := validCommandConfig()
	if err := base.validate(); err != nil {
		t.Fatal(err)
	}
	fields := []struct {
		name  string
		clear func(*config)
	}{
		{"controller id", func(c *config) { c.controllerID = "" }},
		{"boot", func(c *config) { c.boot = "" }},
		{"redis url", func(c *config) { c.redisURL = "" }},
		{"redis namespace environment", func(c *config) { c.namespaceEnvironment = "" }},
		{"redis namespace zone", func(c *config) { c.namespaceZone = "" }},
		{"redis physical profile", func(c *config) { c.physicalProfile = "" }},
		{"redis authority layout", func(c *config) { c.authorityLayout = "" }},
		{"redis workflow layout", func(c *config) { c.workflowLayout = "" }},
		{"redis workflow hash version", func(c *config) { c.workflowHashVersion = "" }},
		{"redis workflow bucket count", func(c *config) { c.workflowBucketCount = "" }},
		{"tls certificate", func(c *config) { c.tlsCert = "" }},
		{"tls key", func(c *config) { c.tlsKey = "" }},
		{"tls ca", func(c *config) { c.tlsCA = "" }},
		{"Gate verifier client certificate", func(c *config) { c.gateVerifierClientCert = "" }},
		{"Gate verifier client key", func(c *config) { c.gateVerifierClientKey = "" }},
		{"Gate verifier client CA", func(c *config) { c.gateVerifierClientCA = "" }},
		{"allowed client identity", func(c *config) { c.allowedClientIdentity = nil }},
		{"plan read identity", func(c *config) { c.planReadIdentity = nil }},
		{"operator identity", func(c *config) { c.operatorIdentity = nil }},
		{"single-controller attestation", func(c *config) { c.singleControllerContract = "" }},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			candidate := base
			field.clear(&candidate)
			if err := candidate.validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(field.name)) {
				t.Fatalf("validate err=%v, want required %q", err, field.name)
			}
		})
	}
}

func TestRouteControllerGateVerifierTimeoutEnvironmentIsStrict(t *testing.T) {
	const name = "ROUTE_CONTROLLER_GATE_VERIFIER_TIMEOUT"
	previous, present := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, previous)
		} else {
			_ = os.Unsetenv(name)
		}
	})
	cfg, err := parseConfig(nil)
	if err != nil || cfg.gateVerifierTimeout != 5*time.Second {
		t.Fatalf("absent timeout cfg=%v err=%v", cfg.gateVerifierTimeout, err)
	}
	for _, value := range []string{"", " ", "garbage", "0", "0s", "-1s", "30.000000001s", "31s"} {
		t.Run(value, func(t *testing.T) {
			if err := os.Setenv(name, value); err != nil {
				t.Fatal(err)
			}
			if cfg, err := parseConfig(nil); err == nil {
				t.Fatalf("present invalid timeout %q silently produced %v", value, cfg.gateVerifierTimeout)
			}
		})
	}
	if err := os.Setenv(name, "30s"); err != nil {
		t.Fatal(err)
	}
	if cfg, err := parseConfig(nil); err != nil || cfg.gateVerifierTimeout != 30*time.Second {
		t.Fatalf("upper bound cfg=%v err=%v", cfg.gateVerifierTimeout, err)
	}
	if _, err := parseConfig([]string{"-gate-verifier-timeout=0"}); err == nil {
		t.Fatal("present zero timeout flag passed parse")
	}
	if _, err := parseConfig([]string{"-gate-verifier-timeout=30s1ms"}); err == nil {
		t.Fatal("malformed timeout flag passed parse")
	}
}

func TestRouteControllerPlanTTLDefaultsOverridesAndBounds(t *testing.T) {
	for _, name := range []string{"ROUTE_CONTROLLER_PLAN_TTL", "ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT"} {
		previous, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		name := name
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, previous)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}

	t.Run("defaults", func(t *testing.T) {
		cfg, err := parseConfig(nil)
		if err != nil || cfg.planOperation != defaultPlanOperationTimeout || cfg.planTTL != defaultPlanTTL {
			t.Fatalf("operation=%v ttl=%v err=%v", cfg.planOperation, cfg.planTTL, err)
		}
	})
	t.Run("environment override", func(t *testing.T) {
		t.Setenv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", "118s")
		t.Setenv("ROUTE_CONTROLLER_PLAN_TTL", "120s")
		cfg, err := parseConfig(nil)
		if err != nil || cfg.planOperation != 118*time.Second || cfg.planTTL != 120*time.Second {
			t.Fatalf("operation=%v ttl=%v err=%v", cfg.planOperation, cfg.planTTL, err)
		}
	})
	t.Run("CLI override", func(t *testing.T) {
		cfg, err := parseConfig([]string{"--plan-operation-timeout=118s", "--plan-ttl=120s"})
		if err != nil || cfg.planOperation != 118*time.Second || cfg.planTTL != 120*time.Second {
			t.Fatalf("operation=%v ttl=%v err=%v", cfg.planOperation, cfg.planTTL, err)
		}
	})
	for _, test := range []struct {
		name      string
		operation string
		ttl       string
	}{
		{name: "equal budget", operation: "119s", ttl: "120s"},
		{name: "insufficient budget", operation: "119001ms", ttl: "120s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", test.operation)
			t.Setenv("ROUTE_CONTROLLER_PLAN_TTL", test.ttl)
			if cfg, err := parseConfig(nil); err == nil {
				t.Fatalf("insufficient operation=%v ttl=%v accepted", cfg.planOperation, cfg.planTTL)
			}
		})
	}
	t.Run("minimum pending recovery budget", func(t *testing.T) {
		minimum, ok := checkedAddDuration(targetSnapshotConvergenceTimeout, recoveryStartupHeadroom)
		if !ok {
			t.Fatal("minimum recovery budget overflowed")
		}
		t.Setenv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", minimum.String())
		t.Setenv("ROUTE_CONTROLLER_PLAN_TTL", "120s")
		cfg, err := parseConfig(nil)
		if err != nil || cfg.planOperation != minimum {
			t.Fatalf("minimum operation=%v err=%v", cfg.planOperation, err)
		}
		t.Setenv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", (minimum - time.Millisecond).String())
		if cfg, err := parseConfig(nil); err == nil {
			t.Fatalf("undersized recovery operation=%v accepted", cfg.planOperation)
		}
	})
	t.Run("maximum", func(t *testing.T) {
		t.Setenv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", maxPlanOperationTimeout.String())
		t.Setenv("ROUTE_CONTROLLER_PLAN_TTL", maxPlanTTL.String())
		cfg, err := parseConfig(nil)
		if err != nil || cfg.planOperation != maxPlanOperationTimeout || cfg.planTTL != maxPlanTTL {
			t.Fatalf("operation=%v ttl=%v err=%v", cfg.planOperation, cfg.planTTL, err)
		}
	})
}

func TestRouteControllerDeadlineRefreshTimingDefaultsOverridesAndBounds(t *testing.T) {
	for _, name := range []string{"ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL", "ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD"} {
		previous, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		name := name
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, previous)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}

	cfg, err := parseConfig(nil)
	if err != nil || cfg.deadlineRefreshInterval != defaultDeadlineRefreshInterval || cfg.deadlineRefreshLead != defaultDeadlineRefreshLead {
		t.Fatalf("defaults interval=%v lead=%v err=%v", cfg.deadlineRefreshInterval, cfg.deadlineRefreshLead, err)
	}
	cfg, err = parseConfig([]string{"--deadline-refresh-interval=2s", "--deadline-refresh-lead=31s"})
	if err != nil || cfg.deadlineRefreshInterval != 2*time.Second || cfg.deadlineRefreshLead != 31*time.Second {
		t.Fatalf("CLI interval=%v lead=%v err=%v", cfg.deadlineRefreshInterval, cfg.deadlineRefreshLead, err)
	}
	for _, test := range []struct {
		name, interval, lead string
	}{
		{name: "interval zero", interval: "0s", lead: "30s"},
		{name: "interval above maximum", interval: "31s", lead: "45s"},
		{name: "lead above maximum", interval: "1s", lead: "60s"},
		{name: "insufficient headroom", interval: "3s", lead: "10s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL", test.interval)
			t.Setenv("ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD", test.lead)
			if parsed, parseErr := parseConfig(nil); parseErr == nil {
				t.Fatalf("invalid refresh timing accepted: interval=%v lead=%v", parsed.deadlineRefreshInterval, parsed.deadlineRefreshLead)
			}
		})
	}
	for _, value := range []string{"", " ", "garbage", "-1s"} {
		t.Run("strict interval "+value, func(t *testing.T) {
			t.Setenv("ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL", value)
			if _, parseErr := parseConfig(nil); parseErr == nil {
				t.Fatalf("non-canonical interval %q accepted", value)
			}
		})
	}
}

func TestRouteControllerLeaseProjectionModeIsExplicitAndStrict(t *testing.T) {
	const name = "ROUTE_CONTROLLER_LEASE_PROJECTION_MODE"
	previous, present := os.LookupEnv(name)
	_ = os.Unsetenv(name)
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, previous)
		} else {
			_ = os.Unsetenv(name)
		}
	})
	if cfg, err := parseConfig(nil); err != nil || cfg.leaseProjectionMode != leaseProjectionModeDisabled {
		t.Fatalf("default mode=%q err=%v", cfg.leaseProjectionMode, err)
	}
	for _, mode := range []string{leaseProjectionModeDisabled, leaseProjectionModeAllActive} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(name, mode)
			if cfg, err := parseConfig(nil); err != nil || cfg.leaseProjectionMode != mode {
				t.Fatalf("mode=%q parsed=%q err=%v", mode, cfg.leaseProjectionMode, err)
			}
		})
	}
	for _, mode := range []string{"", " ", "enabled", "ALL-ACTIVE"} {
		t.Run("reject "+mode, func(t *testing.T) {
			t.Setenv(name, mode)
			if _, err := parseConfig(nil); err == nil {
				t.Fatalf("invalid mode %q accepted", mode)
			}
		})
	}
	_ = os.Unsetenv(name)
	if _, err := parseConfig([]string{"--lease-projection-mode="}); err == nil {
		t.Fatal("explicit empty mode flag accepted")
	}
}

func TestRouteControllerPlanTTLEnvironmentAndFlagAreStrict(t *testing.T) {
	const name = "ROUTE_CONTROLLER_PLAN_TTL"
	previous, present := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, previous)
		} else {
			_ = os.Unsetenv(name)
		}
	})

	for _, value := range []string{"", " ", "garbage", "0", "0s", "-1s", "168h1ms", "2562047h47m16.854775808s"} {
		t.Run("environment "+value, func(t *testing.T) {
			t.Setenv(name, value)
			if cfg, err := parseConfig(nil); err == nil {
				t.Fatalf("present invalid Plan TTL %q silently produced %v", value, cfg.planTTL)
			}
		})
	}
	for _, value := range []string{"", "garbage", "0", "0s", "-1s", "168h1ms", "2562047h47m16.854775808s"} {
		t.Run("flag "+value, func(t *testing.T) {
			if cfg, err := parseConfig([]string{"--plan-ttl=" + value}); err == nil {
				t.Fatalf("present invalid Plan TTL flag %q silently produced %v", value, cfg.planTTL)
			}
		})
	}
}

func TestRouteControllerPlanTTLUsesExecutorMillisecondBudget(t *testing.T) {
	if err := validatePlanTiming(30*time.Second+time.Nanosecond, 31*time.Second+time.Millisecond); err == nil {
		t.Fatal("millisecond-equal executor budget accepted")
	}
	if err := validatePlanTiming(30*time.Second+time.Nanosecond, 31*time.Second+2*time.Millisecond); err != nil {
		t.Fatalf("next full millisecond budget rejected: %v", err)
	}
	if _, ok := checkedAddDuration(time.Duration(1<<63-1), time.Nanosecond); ok {
		t.Fatal("duration overflow accepted")
	}
}

func TestConfiguredPlanRuntimeUsesOneExactPlanTTL(t *testing.T) {
	for _, planTTL := range []time.Duration{120 * time.Second, 25 * time.Hour, maxPlanTTL} {
		t.Run(planTTL.String(), func(t *testing.T) {
			cfg := validCommandConfig()
			cfg.planTTL = planTTL
			namespace, err := cfg.keyspaceTuple()
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(1_800_000_000, 0)
			server := miniredis.RunT(t)
			server.SetTime(now)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			store, err := newConfiguredPlanStore(cfg, client, namespace.Namespace())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EnsureLoaded(context.Background()); err != nil {
				t.Fatal(err)
			}
			executor, err := newConfiguredPlanExecutor(cfg, routeplan.PlanExecutorOptions{
				Store:            store,
				Manager:          (*authority.RouteAssignmentManager)(nil),
				AuthorityReader:  planTTLAuthorityReaderFunc(nil),
				SnapshotStore:    (*core.MemoryRouteSnapshotStore)(nil),
				HolderReader:     (*core.HolderRegistry)(nil),
				SingleController: routeplan.SingleControllerAttestation{Contract: routeplan.SingleControllerContract, ControllerID: cfg.controllerID, BootID: cfg.boot},
			})
			if err != nil {
				t.Fatal(err)
			}
			value := reflect.ValueOf(executor).Elem()
			if got := time.Duration(value.FieldByName("planTTL").Int()); got != planTTL {
				t.Fatalf("executor Plan TTL=%v want=%v", got, planTTL)
			}
			if got := time.Duration(value.FieldByName("timeout").Int()); got != cfg.planOperation {
				t.Fatalf("executor operation timeout=%v want=%v", got, cfg.planOperation)
			}

			exact := configuredPlanTTLFixture(now.UnixMilli(), "plan-exact", planTTL)
			result, err := store.CompareAndSwap(context.Background(), 0, exact)
			if err != nil || result.Status != routeplan.CASCommitted {
				t.Fatalf("exact-horizon initial CAS status=%v err=%v", result.Status, err)
			}
			planKey, err := store.Keys().Plan(exact.PlanID)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := server.TTL(planKey.String()), planTTL+productionPlanAuditRetention; got != want {
				t.Fatalf("physical retention TTL=%v want=%v", got, want)
			}

			overlong := configuredPlanTTLFixture(now.UnixMilli(), "plan-overlong", planTTL+time.Millisecond)
			if _, err := store.CompareAndSwap(context.Background(), 0, overlong); !errors.Is(err, routeplan.ErrInvalidPlan) {
				t.Fatalf("overlong initial CAS err=%v", err)
			}
		})
	}
}

func TestConfiguredPlanRuntimeUsesProductionRecoveryCapacity(t *testing.T) {
	cfg := validCommandConfig()
	namespace, err := cfg.keyspaceTuple()
	if err != nil {
		t.Fatal(err)
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := newConfiguredPlanStore(cfg, client, namespace.Namespace())
	if err != nil {
		t.Fatal(err)
	}
	value := reflect.ValueOf(store).Elem().FieldByName("options")
	if got := int(value.FieldByName("MaxItems").Int()); got != productionRoutePlanMaxItems {
		t.Fatalf("RoutePlan MaxItems=%d want=%d", got, productionRoutePlanMaxItems)
	}
	if got := int(value.FieldByName("MaxBytes").Int()); got != productionRoutePlanMaxBytes {
		t.Fatalf("RoutePlan MaxBytes=%d want=%d", got, productionRoutePlanMaxBytes)
	}
	if got := int(value.FieldByName("MaxRestartRead").Int()); got != productionRoutePlanMaxItems {
		t.Fatalf("RoutePlan MaxRestartRead=%d want=%d", got, productionRoutePlanMaxItems)
	}
}

func TestConfiguredPlanRuntimeRejectsUnsafePlanTiming(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation time.Duration
		planTTL   time.Duration
	}{
		{name: "above maximum", operation: defaultPlanOperationTimeout, planTTL: maxPlanTTL + time.Millisecond},
		{name: "operation safety mismatch", operation: 119 * time.Second, planTTL: 120 * time.Second},
		{name: "millisecond safety mismatch", operation: 30*time.Second + time.Nanosecond, planTTL: 31*time.Second + time.Millisecond},
		{name: "pending recovery headroom", operation: targetSnapshotConvergenceTimeout + recoveryStartupHeadroom - time.Millisecond, planTTL: defaultPlanTTL},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validCommandConfig()
			cfg.planOperation = test.operation
			cfg.planTTL = test.planTTL
			if err := cfg.validate(); err == nil {
				t.Fatal("unsafe timing passed startup validation")
			}
			namespace, err := cfg.keyspaceTuple()
			if err != nil {
				t.Fatal(err)
			}
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			if store, err := newConfiguredPlanStore(cfg, client, namespace.Namespace()); err == nil || store != nil {
				t.Fatalf("unsafe PlanStore construction store=%v err=%v", store, err)
			}
			if executor, err := newConfiguredPlanExecutor(cfg, routeplan.PlanExecutorOptions{}); err == nil || executor != nil {
				t.Fatalf("unsafe Executor construction executor=%v err=%v", executor, err)
			}
		})
	}
}

func configuredPlanTTLFixture(now int64, id string, planTTL time.Duration) routeplan.Plan {
	return routeplan.Plan{
		PlanID: id, TransitionID: "transition-" + id, Domain: actorrouting.DomainPlayer,
		SourceHolderID: "holder-a", SourceBootID: "boot-a", SourceEndpoint: "holder-a.internal:443",
		TargetHolderID: "holder-b", TargetBootID: "boot-b", TargetEndpoint: "holder-b.internal:443",
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		RequestedTTLms:      time.Minute.Milliseconds(), Operation: routeplan.OperationPlannedDrain,
		Phase: routeplan.PhasePending, Revision: 1, CreatedAtMs: now, UpdatedAtMs: now, ExpiresAtMs: now + planTTL.Milliseconds(),
	}
}

func TestRouteControllerUsesExactCanonicalRedisTuple(t *testing.T) {
	cfg := validCommandConfig()
	tuple, err := cfg.keyspaceTuple()
	if err != nil {
		t.Fatal(err)
	}
	profile := tuple.Profile()
	if profile.Namespace.Environment != cfg.namespaceEnvironment || profile.Namespace.Zone != cfg.namespaceZone ||
		profile.ProfileVersion != cfg.physicalProfile || profile.AuthorityLayoutVersion != cfg.authorityLayout ||
		profile.WorkflowLayoutVersion != cfg.workflowLayout || profile.WorkflowHashVersion != 1 || profile.WorkflowBucketCount != 256 {
		t.Fatalf("profile=%+v", profile)
	}
	for _, mutate := range []func(*config){
		func(value *config) { value.workflowHashVersion = "01" },
		func(value *config) { value.workflowBucketCount = "0256" },
		func(value *config) { value.workflowLayout = "v2 " },
	} {
		candidate := cfg
		mutate(&candidate)
		if err := candidate.validate(); err == nil {
			t.Fatalf("noncanonical tuple accepted: %+v", candidate)
		}
	}
}

func TestRouteControllerConfigRejectsUnboundedValues(t *testing.T) {
	base := validCommandConfig()
	tests := []func(*config){
		func(c *config) { c.redisOperation = 0 },
		func(c *config) { c.maxSnapshotBytes = 0 },
		func(c *config) { c.maxClients = 0 },
		func(c *config) { c.queueSize = 0 },
		func(c *config) { c.maxMessageBytes = 0 },
		func(c *config) { c.planTTL = 0 },
		func(c *config) { c.planTTL = maxPlanTTL + time.Millisecond },
		func(c *config) { c.planOperation = 119 * time.Second; c.planTTL = 120 * time.Second },
		func(c *config) { c.planOperation = time.Duration(1<<63 - 1) },
	}
	for _, mutate := range tests {
		candidate := base
		mutate(&candidate)
		if err := candidate.validate(); err == nil {
			t.Fatal("invalid bound unexpectedly accepted")
		}
	}
}

func TestRouteControllerParsesRequiredClientIdentityAllowlist(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-allowed-client-identity", "client.test",
		"-allowed-client-identity", "spiffe://farm.test/client-a,client-b.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(cfg.allowedClientIdentity, ","), "client.test,spiffe://farm.test/client-a,client-b.test"; got != want {
		t.Fatalf("allowlist=%q want=%q", got, want)
	}
}

func TestRouteControllerGatePeerListIsExplicitBoundedAndStrict(t *testing.T) {
	valid := `[{"name":"gate-a","address":"127.0.0.1:19091","server_name":"gate.test","expected_identity":"gate.test"}]`
	peers, err := parseGateVerifierPeers(valid)
	if err != nil || len(peers) != 1 || peers[0].Name != "gate-a" {
		t.Fatalf("peers=%+v err=%v", peers, err)
	}
	for _, raw := range []string{
		`[]`,
		`[{"name":"gate-a","address":"127.0.0.1:19091","server_name":"gate.test","expected_identity":"gate.test","unknown":true}]`,
		valid + `{}`,
		`not-json`,
	} {
		if peers, err := parseGateVerifierPeers(raw); err == nil || peers != nil {
			t.Fatalf("raw=%q peers=%+v err=%v", raw, peers, err)
		}
	}
}

func TestRouteControllerEmptyDurableSnapshotFailsStartup(t *testing.T) {
	if err := requireDurableSnapshot(false); err == nil {
		t.Fatal("empty durable snapshot unexpectedly allowed")
	}
	if err := requireDurableSnapshot(true); err != nil {
		t.Fatalf("present durable snapshot rejected: %v", err)
	}
}

func TestMethodACLSeparatesSnapshotReadersFromPlanOperators(t *testing.T) {
	authorizer, err := newMethodAuthorizer(
		[]string{"snapshot-client.test"},
		[]string{"plan-reader.test"},
		[]string{"operator.test"},
	)
	if err != nil {
		t.Fatal(err)
	}
	serverAuthorizer, err := newServerMethodAuthorizer(
		[]string{"snapshot-client.test"},
		[]string{"plan-reader.test"},
		[]string{"operator.test"},
	)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return struct{}{}, nil
	}
	snapshotClient := verifiedDNSPeerContext(t, "snapshot-client.test")
	if _, err := serverAuthorizer.UnaryServerInterceptor(snapshotClient, struct{}{}, &grpc.UnaryServerInfo{
		FullMethod: routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName,
	}, handler); err != nil || !called {
		t.Fatalf("snapshot read err=%v called=%v", err, called)
	}
	called = false
	if _, err := serverAuthorizer.UnaryServerInterceptor(snapshotClient, struct{}{}, &grpc.UnaryServerInfo{
		FullMethod: routeplanpb.RoutePlanService_MoveShard_FullMethodName,
	}, func(ctx context.Context, _ any) (any, error) {
		called = true
		if err := authorizer.Authorize(ctx, routeplanpb.RoutePlanService_MoveShard_FullMethodName); err != nil {
			return nil, status.Error(codes.PermissionDenied, "route plan method is not authorized")
		}
		return struct{}{}, nil
	}); status.Code(err) != codes.PermissionDenied || !called {
		t.Fatalf("snapshot client mutation status=%s called=%v", status.Code(err), called)
	}
	planReader := verifiedDNSPeerContext(t, "plan-reader.test")
	for _, method := range []string{routeplanpb.RoutePlanService_GetStatus_FullMethodName, routeplanpb.RoutePlanService_GetPlan_FullMethodName} {
		if _, err := serverAuthorizer.UnaryServerInterceptor(planReader, struct{}{}, &grpc.UnaryServerInfo{
			FullMethod: method,
		}, func(ctx context.Context, _ any) (any, error) {
			return struct{}{}, authorizer.Authorize(ctx, method)
		}); err != nil {
			t.Fatalf("plan read method=%s err=%v", method, err)
		}
	}
	called = false
	if _, err := serverAuthorizer.UnaryServerInterceptor(planReader, struct{}{}, &grpc.UnaryServerInfo{
		FullMethod: routeplanpb.RoutePlanService_DrainHolder_FullMethodName,
	}, func(ctx context.Context, _ any) (any, error) {
		called = true
		if err := authorizer.Authorize(ctx, routeplanpb.RoutePlanService_DrainHolder_FullMethodName); err != nil {
			return nil, status.Error(codes.PermissionDenied, "route plan method is not authorized")
		}
		return struct{}{}, nil
	}); status.Code(err) != codes.PermissionDenied || !called {
		t.Fatalf("plan reader mutation status=%s called=%v", status.Code(err), called)
	}
}

func TestPendingProjectionTargetValidatorRequiresExactFreshAppliedTarget(t *testing.T) {
	now := time.Now()
	record := core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-b", BootID: "boot-b",
		DeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload-b", Ordinal: 1},
		LogicEndpoint:      "holder-b.internal:443", HolderControlEndpoint: "holder-b.control:9443", TLSDNSIdentity: "holder-b.internal",
		State: core.HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 7,
		LeaseUntil: now.Add(time.Minute), CapacityWeight: 1,
	}
	reader := recoveryHolderReaderFunc(func(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
		return record, true, nil
	})
	validator := newPendingProjectionTargetValidator(reader)
	recovery := authority.TransitionRecovery{
		TransitionID: "pending-target", Domain: actorrouting.DomainPlayer,
		NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b.internal:443",
		TargetPodUID: "pod-b", TargetWorkloadUID: "workload-b", TargetOrdinal: 1, DeploymentIdentityPresent: true,
	}
	if err := validator(context.Background(), recovery, 7); err != nil {
		t.Fatalf("exact target rejected: %v", err)
	}
	record.SnapshotRevision = 8
	if err := validator(context.Background(), recovery, 7); err != nil {
		t.Fatalf("later unrelated target revision rejected: %v", err)
	}
	record.SnapshotRevision = 7
	for name, mutate := range map[string]func(*core.HolderRecord){
		"boot":              func(value *core.HolderRecord) { value.BootID = "other-boot" },
		"pod uid":           func(value *core.HolderRecord) { value.DeploymentIdentity.PodUID = "other-pod" },
		"workload uid":      func(value *core.HolderRecord) { value.DeploymentIdentity.WorkloadUID = "other-workload" },
		"ordinal":           func(value *core.HolderRecord) { value.DeploymentIdentity.Ordinal = 2 },
		"endpoint":          func(value *core.HolderRecord) { value.LogicEndpoint = "other.internal:443" },
		"logic not socket":  func(value *core.HolderRecord) { value.LogicEndpoint = "not-a-socket" },
		"logic uppercase":   func(value *core.HolderRecord) { value.LogicEndpoint = "Holder-b.internal:443" },
		"control padded":    func(value *core.HolderRecord) { value.HolderControlEndpoint = " holder-b.internal:443" },
		"control zero port": func(value *core.HolderRecord) { value.HolderControlEndpoint = "holder-b.internal:0" },
		"control overflow":  func(value *core.HolderRecord) { value.HolderControlEndpoint = "holder-b.internal:65536" },
		"swapped endpoints": func(value *core.HolderRecord) {
			value.LogicEndpoint, value.HolderControlEndpoint = value.HolderControlEndpoint, value.LogicEndpoint
		},
		"missing control":   func(value *core.HolderRecord) { value.HolderControlEndpoint = "" },
		"not ready":         func(value *core.HolderRecord) { value.ProcessReady = false },
		"draining":          func(value *core.HolderRecord) { value.Draining = true },
		"ineligible state":  func(value *core.HolderRecord) { value.State = core.HolderStateWarming },
		"expired lease":     func(value *core.HolderRecord) { value.LeaseUntil = now.Add(-time.Second) },
		"snapshot revision": func(value *core.HolderRecord) { value.SnapshotRevision = 6 },
		"tls identity":      func(value *core.HolderRecord) { value.TLSDNSIdentity = "" },
		"zero capacity":     func(value *core.HolderRecord) { value.CapacityWeight = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := record
			mutate(&candidate)
			invalid := newPendingProjectionTargetValidator(recoveryHolderReaderFunc(func(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
				return candidate, true, nil
			}))
			if err := invalid(context.Background(), recovery, 7); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
}

func TestPendingProjectionTargetValidatorAllowsOnlyPrePublicationWarmingTakeover(t *testing.T) {
	now := time.Now()
	target := core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-b", BootID: "boot-b",
		LogicEndpoint: "holder-b.internal:443", HolderControlEndpoint: "holder-b.control:9443", TLSDNSIdentity: "holder-b.internal",
		State: core.HolderStateWarming, ProcessReady: false, ServingShardCount: 0,
		SnapshotRevision: 7, LeaseUntil: now.Add(time.Minute), CapacityWeight: 1,
	}
	validator := newPendingProjectionTargetValidator(recoveryHolderReaderFunc(func(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
		return target, true, nil
	}))
	recovery := authority.TransitionRecovery{
		TransitionID: "cold-takeover", Domain: actorrouting.DomainPlayer,
		NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b.internal:443",
		InternalDrain: false, Published: false,
	}
	if err := validator(context.Background(), recovery, 7); err != nil {
		t.Fatalf("pre-publication WARMING target rejected: %v", err)
	}
	recovery.Published = true
	recovery.LastState = actorrouting.ShardStateAssigning
	if err := validator(context.Background(), recovery, 7); err == nil {
		t.Fatal("post-publication WARMING target accepted without assignment health")
	}
}

func TestPendingDrainRecoveryFactoryNeverRequiresSourceHolder(t *testing.T) {
	target := core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-b", BootID: "boot-b",
		LogicEndpoint: "holder-b.internal:443", HolderControlEndpoint: "holder-b.internal:443", TLSDNSIdentity: "holder-b.internal",
		State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: time.Now().Add(time.Minute), CapacityWeight: 1,
	}
	var requested []string
	reader := recoveryHolderReaderFunc(func(_ context.Context, _ actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
		requested = append(requested, holderID)
		if holderID == target.HolderID {
			return target, true, nil
		}
		return core.HolderRecord{}, false, nil
	})
	factory := newRecoveryDrainControllerFactory(config{}, reader)
	_, _, err := factory(context.Background(), authority.TransitionRecovery{
		TransitionID: "pending-drain", InternalDrain: true, Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a"},
		NewHolderID:   target.HolderID, NewHolderBootID: target.BootID, Endpoint: target.LogicEndpoint,
	})
	if err == nil || !strings.Contains(err.Error(), "TLS client") {
		t.Fatalf("factory err=%v", err)
	}
	if len(requested) != 1 || requested[0] != target.HolderID {
		t.Fatalf("factory looked up obsolete source: %v", requested)
	}
	if err := (pendingColdLoadController{}).StopNewActivationAndTurns(context.Background(), actorrouting.DomainPlayer, 0); err == nil {
		t.Fatal("pending controller accepted source callback")
	}
}

func TestPendingColdLoadInspectorRequestsRefreshForOlderDeadline(t *testing.T) {
	value := authoritystore.ShardWriteAuthority{
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "holder-b", HolderBootID: "boot-b",
		AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
	}
	status := &holdercontrolpb.ShardStatus{
		Prepared: true,
		PreparedGeneration: &holdercontrolpb.AuthorityGeneration{
			HolderId: value.HolderID, HolderBootId: value.HolderBootID, AssignmentRevision: value.AssignmentRevision,
			WriterEpoch: value.WriterEpoch, LeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs + 1000,
		},
	}
	controller := pendingColdLoadController{inner: &preparedStatusController{status: status}}
	prepared, err := controller.ColdLoadPrepared(context.Background(), value.Domain, value.VirtualShard, value)
	if err != nil || !prepared {
		t.Fatalf("monotonic prepared=%v err=%v", prepared, err)
	}
	status.PreparedGeneration.LeaseUntilMs = value.LeaseUntilMs - 1
	if prepared, err := controller.ColdLoadPrepared(context.Background(), value.Domain, value.VirtualShard, value); err != nil || prepared {
		t.Fatalf("older deadline should request refresh prepared=%v err=%v", prepared, err)
	}
	status.PreparedGeneration.LeaseUntilMs = value.LeaseUntilMs + 1000
	status.PreparedGeneration.WriterEpoch++
	if prepared, err := controller.ColdLoadPrepared(context.Background(), value.Domain, value.VirtualShard, value); err == nil || prepared {
		t.Fatalf("identity drift prepared=%v err=%v", prepared, err)
	}
}

func verifiedDNSPeerContext(t *testing.T, identity string) context.Context {
	t.Helper()
	rawSAN, err := asn1.Marshal([]asn1.RawValue{{Class: asn1.ClassContextSpecific, Tag: 2, Bytes: []byte(identity)}})
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		DNSNames: []string{identity},
		Extensions: []pkix.Extension{{
			Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Value: rawSAN,
		}},
	}
	state := tls.ConnectionState{
		HandshakeComplete: true, Version: tls.VersionTLS13,
		VerifiedChains: [][]*x509.Certificate{{certificate}},
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}
