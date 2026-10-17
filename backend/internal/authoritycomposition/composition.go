// Package authoritycomposition composes the frozen route, lease, and Redis
// authority seams for one actor process.  It deliberately contains process
// bootstrap only; actor registries and the Redis authority implementation
// remain the owners of their existing contracts.
//
// Readiness is deliberately split in two: ProcessReady reports only
// common-dependency health (Redis-backed seams, control-plane applier and
// lease-renewal loops), while CanServeShard reports per-shard admission from
// a locally installed HolderLeaseGuard.  A Holder may therefore start with
// zero local shards, stay process-ready, and fail closed for any shard it has
// not been granted.
package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/holderidentity"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/plantonline"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// Mode selects exactly one actor persistence path.  The empty environment
// value is treated as Legacy for the short rollback path documented by AR2.
type Mode string

const (
	ModeLegacy    Mode = "legacy"
	ModeAuthority Mode = "authority"
)

// ErrLocalHolderNotRegistered reports a static-startup misconfiguration: the
// process-local holder ID is absent from Snapshot.Holders.  A registered
// holder with zero assigned routes is a valid zero-shard topology, but an
// unregistered holder is not, because no RouteManager snapshot can ever assign
// a shard to it and a typo would silently produce a permanently idle process.
var ErrLocalHolderNotRegistered = errors.New("authority composition: local holder is not registered in route snapshot")

// ErrLocalRouteBootMismatch reports a static-startup misconfiguration: an
// ACTIVE snapshot route targets the local holder ID but expects a different
// boot ID than this process.  In the static mode (Remote Watch OFF) no
// process instance would ever serve that shard, so a 0-shard Ready would
// silently hide an unserved shard behind a healthy process.  The process must
// fail closed at startup instead.  (The future Dynamic Watch mode may start a
// new boot as a 0-shard Ready and expose membership_topology_degraded, with
// RouteManager re-assigning; that contract is registered, not implemented.)
var ErrLocalRouteBootMismatch = errors.New("authority composition: local holder route expects a different boot id")

// checkLocalRouteBootMismatch scans every allowed-domain ACTIVE route owned by
// the local holder ID and fails when the expected holder boot differs from the
// process boot.  Routes owned by other holders are deliberately ignored (a
// foreign holder route is never local and must not affect this process).
func checkLocalRouteBootMismatch(snapshot actorrouting.RouteSnapshot, holderID, bootID string, allowedDomains map[actorrouting.RouteDomain]bool) error {
	domains := []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
	for _, domain := range domains {
		if len(allowedDomains) != 0 && !allowedDomains[domain] {
			continue
		}
		ds := snapshotDomain(snapshot, domain)
		for shard := uint32(0); shard < ds.ShardCount; shard++ {
			if int(shard) >= len(ds.Routes) {
				continue
			}
			route := ds.Routes[shard]
			if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 ||
				route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
				// Inactive or incomplete routes carry no service intent.
				continue
			}
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				continue
			}
			holder := snapshot.Holders[route.HolderIndex]
			if holder.State != actorrouting.HolderStateActive || holder.InstanceID != holderID {
				continue // foreign holder route: never local, ignored.
			}
			if holder.BootID != bootID {
				return fmt.Errorf("%w: holder %q route expects boot %q, process boot %q",
					ErrLocalRouteBootMismatch, holderID, holder.BootID, bootID)
			}
		}
	}
	return nil
}

// ModeFromEnvironment is intentionally small and strict.  An unknown mode is
// a startup error rather than an opportunity to accidentally serve legacy
// writes while the operator believes authority mode is enabled.
//
// The production default is Authority. Legacy is only selected by an explicit
// rollback/test value ("legacy" or "rollback"); an empty environment must not
// silently construct the old blobstore writer path.
func ModeFromEnvironment() (Mode, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("ACTOR_AUTHORITY_MODE")))
	switch raw {
	case "", string(ModeAuthority):
		return ModeAuthority, nil
	case string(ModeLegacy), "rollback":
		return ModeLegacy, nil
	default:
		return "", fmt.Errorf("authority composition: ACTOR_AUTHORITY_MODE must be %q or %q, got %q", ModeAuthority, ModeLegacy, raw)
	}
}

// Options is the immutable authority process configuration.  Profile, route
// hash, domain layout, and lease parameters are validated together before any
// actor registry is constructed.
type Options struct {
	Client                         redis.UniversalClient
	Domain                         actorrouting.RouteDomain
	RouteSnapshotPath              string
	AssignmentMode                 AssignmentMode
	HolderID                       string
	HolderBootID                   string
	DeploymentIdentity             holderidentity.DeploymentIdentity
	Profile                        rediskey.PhysicalProfile
	LeaseTTL                       time.Duration
	SafetyMargin                   time.Duration
	RenewInterval                  time.Duration
	AssignmentWatchBuffer          int
	Limits                         authoritystore.Limits
	LeaseLimits                    authoritystore.LeaseLimits
	ScheduleDueIndexMode           redisauthority.ScheduleDueIndexMode
	ScheduleDueIndexDedicatedReady bool
	Now                            func() time.Time
	// CommitObserver receives read-only byte/latency observations from the
	// exact ActorAuthorityStore Commit seam. It cannot affect a store result.
	CommitObserver authoritystore.CommitObserver
	// EnableSoftSave opts this process into the explicit dirty_best_effort
	// experiment. The default remains the synchronous durable Authority CAS.
	EnableSoftSave bool

	// Per-shard lease scheduler tuning (RT1B-3C-A).  Zero values fall back to
	// defaults derived from LeaseTTL/RenewInterval, so existing command roots
	// that do not set them keep working.
	LeaseBaseDelay       time.Duration
	LeaseMaxDelay        time.Duration
	LeaseJitter          time.Duration
	RenewAttemptTimeout  time.Duration
	RenewRecoveryTimeout time.Duration
	// Lease recovery budget (RT1B-3D): the recovery path uses its own cadence,
	// and the frozen startup invariant is
	//   2*RenewRecoveryTimeout + LeaseRecoveryMaxDelay +
	//   LeaseRecoveryJitterMax + LeaseRecoverySchedulerSlack < SafetyMargin
	// Zero values fall back to safe defaults; an unsatisfied budget fails
	// scheduler construction (fail closed).
	LeaseRecoveryBaseDelay      time.Duration
	LeaseRecoveryMaxDelay       time.Duration
	LeaseRecoveryJitterMax      time.Duration
	LeaseRecoverySchedulerSlack time.Duration
	MembershipMaxRetries        int
	MembershipRetryDelay        time.Duration
	// MembershipInstallerDecorator, when set on the composed runtime path,
	// wraps the terminal-gated installer before the reconciler starts. The
	// returned installer is shared by reconciliation and HolderControl.
	MembershipInstallerDecorator func(
		MembershipInstaller,
	) (MembershipInstaller, error)
	// membershipActionHook is TEST_ONLY. It runs synchronously inside the
	// reconciler's single action owner at exact local commit phase boundaries.
	// Production never sets it.
	membershipActionHook func(membershipActionPhase)

	// OnFatal, when set, is invoked (outside any runtime lock, at most once
	// per process) when the membership runtime enters the terminal Fatal
	// state.  A graceful Close does not invoke it.  RT1B-3C-B wires this to
	// ProcessReady=false and the readiness probe.
	OnFatal func(error)
}

// OptionsFromEnvironment is the static/legacy environment parser. Remote
// command roots must parse KeyspaceTuple once and call
// OptionsFromEnvironmentWithProfile so the seven placement fields cannot be
// read, defaulted, or normalized a second time during construction.
func OptionsFromEnvironment(client redis.UniversalClient, domain actorrouting.RouteDomain) (Options, error) {
	tuple, err := rediskey.ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		return Options{}, fmt.Errorf("authority composition: Redis keyspace tuple: %w", err)
	}
	return optionsFromEnvironmentWithProfile(client, domain, tuple.Profile(), os.Getenv)
}

// OptionsFromEnvironmentWithProfile accepts the already parsed immutable
// placement value and reads only non-placement process configuration.
func OptionsFromEnvironmentWithProfile(client redis.UniversalClient, domain actorrouting.RouteDomain, profile rediskey.PhysicalProfile) (Options, error) {
	return optionsFromEnvironmentWithProfile(client, domain, profile, os.Getenv)
}

func optionsFromEnvironmentWithProfile(client redis.UniversalClient, domain actorrouting.RouteDomain, profile rediskey.PhysicalProfile, getenv func(string) string) (Options, error) {
	if getenv == nil || profile.Validate() != nil || profile.AuthorityLayoutVersion != "v1" || profile.WorkflowLayoutVersion != "v1" {
		return Options{}, errors.New("authority composition: canonical physical profile is required")
	}
	get := func(key string) (string, error) {
		value := strings.TrimSpace(getenv(key))
		if value == "" {
			return "", fmt.Errorf("authority composition: %s is required in authority mode", key)
		}
		return value, nil
	}
	requiredProfileDuration := func(key string) (time.Duration, error) {
		raw, err := get(key)
		if err != nil {
			return 0, err
		}
		value, parseErr := time.ParseDuration(raw)
		if parseErr != nil || value <= 0 {
			return 0, fmt.Errorf("authority composition: %s must be a positive duration", key)
		}
		return value, nil
	}
	leaseTTL, err := requiredProfileDuration("ACTOR_AUTHORITY_LEASE_TTL")
	if err != nil {
		return Options{}, err
	}
	safetyMargin, err := requiredProfileDuration("ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN")
	if err != nil {
		return Options{}, err
	}
	renewInterval, err := requiredProfileDuration("ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL")
	if err != nil {
		return Options{}, err
	}
	assignmentMode, err := assignmentModeFromEnvironment(getenv)
	if err != nil {
		return Options{}, err
	}
	membershipMode, err := membershipModeFromValue(getenv("AUTHORITY_MEMBERSHIP_MODE"))
	if err != nil {
		return Options{}, err
	}
	if err := ValidateModeCombination(membershipMode, assignmentMode); err != nil {
		return Options{}, err
	}
	routeSnapshotPath := strings.TrimSpace(getenv("ACTOR_ROUTE_SNAPSHOT"))
	if assignmentMode == AssignmentModeStatic && routeSnapshotPath == "" {
		return Options{}, fmt.Errorf("authority composition: ACTOR_ROUTE_SNAPSHOT is required in static assignment mode")
	}
	holderID, err := get("ACTOR_HOLDER_POD")
	if err != nil {
		return Options{}, err
	}
	deploymentIdentity, err := holderidentity.ParseEnvironment(getenv)
	if err != nil {
		return Options{}, fmt.Errorf("authority composition: %w", err)
	}
	if assignmentMode == AssignmentModeRemoteWatch && !deploymentIdentity.Exact() {
		return Options{}, errors.New("authority composition: remote-watch assignment requires exact Kubernetes holder identity")
	}
	if assignmentMode == AssignmentModeStatic && !deploymentIdentity.IsZero() {
		return Options{}, errors.New("authority composition: static assignment requires the frozen zero deployment identity")
	}
	var holderBootID string
	if deploymentIdentity.IsZero() {
		holderBootID, err = get("ACTOR_HOLDER_BOOT_ID")
		if err != nil {
			return Options{}, err
		}
	} else {
		if assignmentMode != AssignmentModeRemoteWatch {
			return Options{}, errors.New("authority composition: exact Kubernetes holder identity requires remote-watch assignment")
		}
		configuredBoot := strings.TrimSpace(getenv("ACTOR_HOLDER_BOOT_ID"))
		if strings.TrimSpace(getenv("HOLDER_PROCESS_BOOT_MODE")) == "pod-uid-then-random" {
			if configuredBoot == "" || !strings.HasPrefix(configuredBoot, holderID+"-") {
				return Options{}, errors.New("authority composition: trusted holder entrypoint boot must be bound to holder ID")
			}
			holderBootID = configuredBoot
		} else {
			if configuredBoot != "" {
				return Options{}, errors.New("authority composition: ACTOR_HOLDER_BOOT_ID is forbidden with exact Kubernetes holder identity")
			}
			holderBootID, err = holderidentity.RandomBootID()
			if err != nil {
				return Options{}, fmt.Errorf("authority composition: generate holder process boot: %w", err)
			}
		}
	}
	scheduleDueIndexMode, err := redisauthority.ParseScheduleDueIndexMode(os.Getenv("FARM_SCHEDULE_DUE_INDEX_MODE"))
	if err != nil {
		return Options{}, fmt.Errorf("authority composition: FARM_SCHEDULE_DUE_INDEX_MODE: %w", err)
	}
	scheduleDueIndexDedicatedReady, err := optionalBool("FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY")
	if err != nil {
		return Options{}, err
	}
	return Options{
		Client: client, Domain: domain, RouteSnapshotPath: routeSnapshotPath,
		AssignmentMode: assignmentMode,
		HolderID:       holderID, HolderBootID: holderBootID,
		DeploymentIdentity: deploymentIdentity,
		Profile:            profile,
		LeaseTTL:           leaseTTL, SafetyMargin: safetyMargin, RenewInterval: renewInterval,
		AssignmentWatchBuffer:          envPositiveIntBoundedWithGetter(getenv, "ACTOR_ROUTE_WATCH_BUFFER", 64, maxRemoteAssignmentBuffer),
		LeaseLimits:                    authoritystore.DefaultLeaseLimits(),
		ScheduleDueIndexMode:           scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexDedicatedReady,
		Now:                            time.Now,
	}, nil
}

// Runtime is the holder-side authority composition.  Publisher is retained
// privately on purpose: RT1 gives route publication only to the authority
// manager, while this data-plane runtime receives the read/watch capability.
type Runtime struct {
	Domain             actorrouting.RouteDomain
	HolderID           string
	HolderBootID       string
	DeploymentIdentity holderidentity.DeploymentIdentity
	Profile            rediskey.PhysicalProfile

	Store              *redisauthority.Store
	ScriptRegistry     *redisauthority.ScriptRegistry
	Provider           *routeauthority.Provider
	AssignmentProvider *actorrouting.DynamicAssignmentProvider
	publisher          *actorrouting.DynamicAssignmentPublisher
	Resolver           *actorrouting.StaticResolver
	Applier            *actorrouting.SnapshotApplier
	// ownsApplier is a construction-time ownership contract. Static runtimes
	// own their provider watch; a remote runtime shares the applier whose sole
	// Run/Close owner is AssignmentComposition.
	ownsApplier bool
	Calculator  *actorrouting.Calculator
	Guards      *routeauthority.LeaseGuardRegistry
	FenceSource *routeauthority.DynamicFenceSource
	Placement   workidentity.BucketResolver

	// membership is the injected local shard-membership reconciler port.  It
	// is set exactly once through SetMembershipReconciler and read through
	// MembershipReconciler; the pointer is stable and cannot be replaced at
	// runtime.  It does not affect ProcessReady/CanServeShard.
	membership atomic.Pointer[ShardMembershipReconciler]

	// adapter/scheduler are the RT1B-3C-A membership pipeline installed by
	// NewMembershipRuntime.  The scheduler is the SOLE lease-renewal owner on
	// that path; the legacy global runRenewal loop is never started there.
	adapter             *RealMembershipAdapter
	membershipInstaller MembershipInstaller
	scheduler           *PerShardLeaseScheduler
	// recoveryBudget captures the effective lease recovery cadence/budget
	// resolved at scheduler construction, for startup logging and operator
	// diagnostics (RT1B-3D).  Read-only; set exactly once in
	// newMembershipRuntime.
	recoveryBudget RecoveryBudgetInfo
	// OnFatal is the runtime-level fatal callback (Options.OnFatal), invoked at
	// most once when the runtime enters Fatal, never on a graceful Close.
	OnFatal func(error)

	leaseMu sync.RWMutex
	leases  map[leaseKey]*routeauthority.HolderLeaseRuntime

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// membershipState is the ONE-AND-ONLY lifecycle truth.  ProcessReady is
	// derived from it (state == Ready); there is no separately writable ready
	// bool, so a fatal can never be hidden by a leftover true.  The one-way
	// order is Bootstrap → Ready (CAS) and any fault → Fatal or Close →
	// Closed (both terminal, never reversible).
	membershipState atomic.Int32
	// fatalErr records the primary fatal cause when the runtime enters Fatal.
	// Close() never writes it (a graceful shutdown is not a fault).
	fatalErr atomic.Pointer[errorRecord]

	// observeInterval is the membership-observer poll interval set by
	// NewMembershipRuntime (legacy New path leaves it zero and unused).
	observeInterval time.Duration
}

// membershipState is the one-way runtime composition lifecycle.  Fatal is the
// terminal fault state (a real failure: applier/observer exit, scheduler
// OnFatal, topology rejection, unsafe remove).  Closed is the terminal
// shutdown state reached only by Close() and is intentionally NOT Fatal: a
// graceful shutdown must not be recorded as a runtime fault (no FatalErr, no
// OnFatal, no fatal metrics).
const (
	membershipStateBootstrap int32 = iota
	membershipStateReady
	membershipStateFatal
	membershipStateClosed
)

type leaseKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

// New builds and activates the authority path.  It performs a Redis-backed
// holder renewal for every locally assigned shard before returning, so an
// authority Registry cannot be served with a guessed writer or lease epoch.
//
// NOTE (RT1B-3C-A): New retains the legacy global runRenewal loop for
// backward compatibility with existing command roots.  NewMembershipRuntime is
// the per-shard composition where PerShardLeaseScheduler is the sole renewal
// owner and runRenewal is never started.
func New(ctx context.Context, opts Options) (*Runtime, error) {
	return newLegacyRuntime(ctx, opts, nil)
}

func newLegacyRuntime(ctx context.Context, opts Options, assignment *AssignmentComposition) (*Runtime, error) {
	runtime, snapshot, err := buildCompositionCommonWithAssignment(ctx, opts, assignment)
	if err != nil {
		return nil, err
	}
	if err := runtime.installLocalGrants(ctx, opts, snapshot); err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runtime.ctx, runtime.cancel = runCtx, cancel
	if !runtime.membershipState.CompareAndSwap(membershipStateBootstrap, membershipStateReady) {
		return nil, errors.New("authority composition: runtime became fatal during startup")
	}
	runtime.wg.Add(1)
	if runtime.ownsApplier {
		runtime.wg.Add(1)
		go runtime.runApplier(runCtx)
	}
	go runtime.runRenewal(runCtx, opts)
	return runtime, nil
}

// buildCompositionCommon builds every dependency shared by the legacy New path
// and the RT1B-3C-A NewMembershipRuntime path: static snapshot, holder
// registration gate, route calculator/provider/resolver/applier, lease guard
// registry + dynamic fence source, Redis authority scripts/store, actor
// authority provider, and physical placement.  It returns the Runtime skeleton
// (with empty lease table) plus the startup snapshot; each path then decides
// how to activate local shards and which renewal loop to run.
func buildCompositionCommon(ctx context.Context, opts Options) (*Runtime, actorrouting.RouteSnapshot, error) {
	return buildCompositionCommonWithAssignment(ctx, opts, nil)
}

func buildCompositionCommonWithAssignment(ctx context.Context, opts Options, assignment *AssignmentComposition) (*Runtime, actorrouting.RouteSnapshot, error) {
	if ctx == nil {
		return nil, actorrouting.RouteSnapshot{}, errors.New("authority composition: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, actorrouting.RouteSnapshot{}, err
	}
	if opts.LeaseLimits == (authoritystore.LeaseLimits{}) {
		opts.LeaseLimits = authoritystore.DefaultLeaseLimits()
	}
	if err := validateOptions(opts); err != nil {
		return nil, actorrouting.RouteSnapshot{}, err
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.AssignmentMode == "" {
		opts.AssignmentMode = AssignmentModeStatic
	}
	if opts.AssignmentMode == AssignmentModeRemoteWatch && assignment == nil {
		return nil, actorrouting.RouteSnapshot{}, ErrRemoteAssignmentComposition
	}
	if opts.AssignmentMode == AssignmentModeStatic && assignment != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: static assignment cannot receive remote composition")
	}

	var (
		snapshot  actorrouting.RouteSnapshot
		provider  *actorrouting.DynamicAssignmentProvider
		publisher *actorrouting.DynamicAssignmentPublisher
		resolver  *actorrouting.StaticResolver
		applier   *actorrouting.SnapshotApplier
		err       error
	)
	if assignment != nil {
		if assignment.Provider == nil || assignment.Resolver == nil || assignment.Applier == nil || assignment.Pump == nil {
			return nil, actorrouting.RouteSnapshot{}, errors.New("authority composition: incomplete remote assignment composition")
		}
		provider, resolver, applier = assignment.Provider, assignment.Resolver, assignment.Applier
		snapshot, err = provider.GetSnapshot(ctx)
		if err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: read remote route snapshot: %w", err)
		}
	} else {
		staticProvider, loadErr := actorrouting.LoadStaticAssignmentProvider(opts.RouteSnapshotPath)
		if loadErr != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: load route snapshot: %w", loadErr)
		}
		snapshot, err = staticProvider.GetSnapshot(ctx)
		if err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: read route snapshot: %w", err)
		}
		provider, publisher, err = actorrouting.NewDynamicAssignmentProvider(snapshot, opts.AssignmentWatchBuffer)
		if err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: dynamic route provider: %w", err)
		}
		resolverOptions, optionsErr := ResolverOptionsFromEnvironment()
		if optionsErr != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: route resolver options: %w", optionsErr)
		}
		resolver, err = actorrouting.NewStaticResolverFromProviderWithOptions(ctx, provider, resolverOptions)
		if err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: route resolver: %w", err)
		}
		applier, err = actorrouting.NewSnapshotApplier(provider, resolver)
		if err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: route snapshot applier: %w", err)
		}
		if err := applier.Bootstrap(ctx); err != nil {
			return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: route snapshot bootstrap: %w", err)
		}
	}
	if err := validateSnapshotEndpointLimit(snapshot, ActorRouteEndpointLimitFromEnvironment()); err != nil {
		return nil, actorrouting.RouteSnapshot{}, err
	}
	// Local-holder identity is validated in the composition startup layer, not
	// inside the generic static snapshot loader.  The holder must be present in
	// Snapshot.Holders; whether it owns any route is decided by
	// installLocalGrants, which permits a registered holder with zero routes as
	// a valid zero-shard topology.  Failing here precedes any renewal/applier
	// goroutine, so a misconfigured holder leaves no running residue.
	if opts.AssignmentMode == AssignmentModeStatic && !holderRegisteredInSnapshot(snapshot, opts.HolderID) {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("%w: local holder %q is absent from Snapshot.Holders", ErrLocalHolderNotRegistered, opts.HolderID)
	}
	// Static-startup boot-mismatch gate (RT1B-3C-C): in the static mode (Remote
	// Watch OFF) an ACTIVE route owned by the local holder ID but expecting a
	// different boot would leave that shard permanently unserved while this
	// process reports 0-shard Ready.  Fail closed at startup instead.
	if opts.AssignmentMode == AssignmentModeStatic {
		if err := checkLocalRouteBootMismatch(snapshot, opts.HolderID, opts.HolderBootID,
			map[actorrouting.RouteDomain]bool{opts.Domain: true}); err != nil {
			return nil, actorrouting.RouteSnapshot{}, err
		}
	}
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: route calculator: %w", err)
	}
	if opts.Profile.AuthorityLayoutVersion == "" {
		return nil, actorrouting.RouteSnapshot{}, errors.New("authority composition: authority layout is required")
	}
	if err := opts.Profile.Validate(); err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: physical profile: %w", err)
	}
	if opts.Profile.AuthorityLayoutVersion != "v1" || opts.Profile.WorkflowLayoutVersion != "v1" || opts.Profile.WorkflowHashVersion != 1 {
		return nil, actorrouting.RouteSnapshot{}, errors.New("authority composition: only the frozen v1 authority/workflow physical layout is admissible")
	}

	registry := routeauthority.NewLeaseGuardRegistry()
	fenceSource, err := routeauthority.NewDynamicFenceSource(routeauthority.DynamicFenceSourceOptions{
		Calculator: calculator, Snapshots: provider, Guards: registry,
		AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion, Now: opts.Now,
	})
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: dynamic fence source: %w", err)
	}
	scripts, err := redisauthority.NewScriptRegistry(opts.Client)
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: authority script registry: %w", err)
	}
	if err := scripts.EnsureLoaded(ctx); err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: authority script warmup: %w", err)
	}
	store, err := redisauthority.NewStore(opts.Client, scripts, redisauthority.Options{
		Profile: opts.Profile, Resolver: calculator, Limits: opts.Limits, LeaseLimits: opts.LeaseLimits,
		ScheduleDueIndexMode: opts.ScheduleDueIndexMode, ScheduleDueIndexDedicatedReady: opts.ScheduleDueIndexDedicatedReady,
	})
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: Redis authority store: %w", err)
	}
	authorityProvider, err := routeauthority.NewProvider(routeauthority.ProviderOptions{
		Store: authoritystore.NewObservedActorStore(store, opts.CommitObserver), Fence: fenceSource, AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion,
		BootID: opts.HolderBootID, EnableSoftSave: opts.EnableSoftSave,
	})
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: actor authority provider: %w", err)
	}
	placement, err := newPlacementResolver(opts.Profile, calculator)
	if err != nil {
		return nil, actorrouting.RouteSnapshot{}, fmt.Errorf("authority composition: physical placement: %w", err)
	}

	runtime := &Runtime{
		Domain: opts.Domain, HolderID: opts.HolderID, HolderBootID: opts.HolderBootID,
		DeploymentIdentity: opts.DeploymentIdentity,
		Profile:            opts.Profile, Store: store, ScriptRegistry: scripts,
		Provider: authorityProvider, AssignmentProvider: provider, publisher: publisher, Resolver: resolver, Applier: applier, Calculator: calculator,
		Guards: registry, FenceSource: fenceSource, Placement: placement,
		ownsApplier: assignment == nil,
		leases:      make(map[leaseKey]*routeauthority.HolderLeaseRuntime),
	}
	return runtime, snapshot, nil
}

func validateOptions(opts Options) error {
	if opts.Client == nil {
		return errors.New("authority composition: Redis client is required")
	}
	if opts.Domain != actorrouting.DomainPlayer && opts.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("authority composition: invalid actor domain %q", opts.Domain)
	}
	if opts.AssignmentMode == "" {
		opts.AssignmentMode = AssignmentModeStatic
	}
	if (opts.AssignmentMode != AssignmentModeRemoteWatch && strings.TrimSpace(opts.RouteSnapshotPath) == "") || strings.TrimSpace(opts.HolderID) == "" || strings.TrimSpace(opts.HolderBootID) == "" {
		return errors.New("authority composition: route snapshot, holder ID, and holder boot ID are required")
	}
	if err := opts.DeploymentIdentity.ValidateOptional(); err != nil {
		return fmt.Errorf("authority composition: invalid deployment identity: %w", err)
	}
	if opts.AssignmentMode == AssignmentModeRemoteWatch && !opts.DeploymentIdentity.Exact() {
		return errors.New("authority composition: remote-watch assignment requires exact Kubernetes holder identity")
	}
	if opts.AssignmentMode == AssignmentModeStatic && !opts.DeploymentIdentity.IsZero() {
		return errors.New("authority composition: static assignment requires the frozen zero deployment identity")
	}
	if opts.AssignmentMode != AssignmentModeStatic && opts.AssignmentMode != AssignmentModeRemoteWatch {
		return fmt.Errorf("%w: %q", ErrAssignmentModeInvalid, opts.AssignmentMode)
	}
	if opts.LeaseTTL <= 0 || opts.RenewInterval <= 0 || opts.SafetyMargin < 0 || opts.RenewInterval >= opts.LeaseTTL {
		return errors.New("authority composition: lease TTL, safety margin, and renew interval are inconsistent")
	}
	if opts.SafetyMargin >= opts.LeaseTTL {
		return errors.New("authority composition: lease safety margin must be less than TTL")
	}
	if opts.LeaseLimits.MinTTL < time.Millisecond || opts.LeaseLimits.MaxTTL < opts.LeaseLimits.MinTTL {
		return errors.New("authority composition: lease limits are incomplete")
	}
	if opts.LeaseTTL < opts.LeaseLimits.MinTTL || (opts.LeaseLimits.MaxTTL > 0 && opts.LeaseTTL > opts.LeaseLimits.MaxTTL) {
		return errors.New("authority composition: lease TTL is outside configured adapter bounds")
	}
	return nil
}

// holderRegisteredInSnapshot reports whether the local holder ID appears in
// Snapshot.Holders.  Route ownership is intentionally not considered here:
// presence is the registration gate, route assignment is installLocalGrants'
// concern.
func holderRegisteredInSnapshot(snapshot actorrouting.RouteSnapshot, holderID string) bool {
	for _, holder := range snapshot.Holders {
		if holder.InstanceID == holderID {
			return true
		}
	}
	return false
}

// installLocalGrants claims and renews every shard the static snapshot assigns
// to this holder.  A registered holder with no local assignment is a valid
// zero-shard topology: it installs no lease guard, stays process-ready, and
// every shard query fails closed.  A local assignment that is stale or not
// ACTIVE still aborts startup so the process never guesses a writer
// generation.
func (r *Runtime) installLocalGrants(ctx context.Context, opts Options, snapshot actorrouting.RouteSnapshot) error {
	routes := snapshot.Player
	if opts.Domain == actorrouting.DomainFarm {
		routes = snapshot.Farm
	}
	for shard := uint32(0); shard < routes.ShardCount; shard++ {
		route := routes.Routes[shard]
		if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			return fmt.Errorf("authority composition: %s shard %d holder index is invalid", opts.Domain, shard)
		}
		holder := snapshot.Holders[route.HolderIndex]
		if holder.InstanceID != opts.HolderID {
			continue
		}
		if holder.BootID != opts.HolderBootID {
			if opts.AssignmentMode == AssignmentModeRemoteWatch {
				continue
			}
			return fmt.Errorf("authority composition: %s shard %d belongs to holder boot %q, process boot is %q", opts.Domain, shard, holder.BootID, opts.HolderBootID)
		}
		if holder.State != actorrouting.HolderStateActive || route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 || route.LeaseUntilMs <= opts.Now().UnixMilli() {
			if opts.AssignmentMode == AssignmentModeRemoteWatch {
				continue
			}
			return fmt.Errorf("authority composition: local %s shard %d is not ACTIVE with a complete route generation", opts.Domain, shard)
		}
		requestStarted := opts.Now()
		result, err := r.Store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
			Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: opts.HolderID, BootID: opts.HolderBootID},
			AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion, Domain: opts.Domain, VirtualShard: shard,
			ExpectedFence: authoritystore.ShardWriteFence{
				ExpectedHolderID: opts.HolderID, ExpectedHolderBootID: opts.HolderBootID,
				ExpectedAssignmentRevision: route.ShardRevision, ExpectedWriterEpoch: route.ExpectedWriterEpoch,
				ExpectedLeaseEpoch: route.ExpectedLeaseEpoch,
			},
			RequestedTTLms: opts.LeaseTTL.Milliseconds(),
		})
		if err != nil {
			return fmt.Errorf("authority composition: renew %s shard %d: %w", opts.Domain, shard, err)
		}
		if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
			return fmt.Errorf("authority composition: renew %s shard %d rejected with %s", opts.Domain, shard, result.Status)
		}
		if err := validateGrantedRoute(opts, snapshot, shard, route, result); err != nil {
			return err
		}
		guard, err := authoritystore.NewHolderLeaseGuard(opts.HolderID, opts.HolderBootID, opts.SafetyMargin, opts.Now)
		if err != nil {
			return fmt.Errorf("authority composition: %s shard %d lease guard: %w", opts.Domain, shard, err)
		}
		leaseRuntime, err := routeauthority.InstallHolderLeaseGrant(r.Guards, opts.Domain, shard, guard, requestStarted, result, opts.Now)
		if err != nil {
			return fmt.Errorf("authority composition: install %s shard %d lease grant: %w", opts.Domain, shard, err)
		}
		if state, ok := r.Guards.Snapshot(opts.Domain, shard); !ok || !state.Admitted || state.Authority.State != authoritystore.ShardAuthorityActive {
			return fmt.Errorf("authority composition: %s shard %d lease grant is not locally ACTIVE", opts.Domain, shard)
		}
		r.leases[leaseKey{domain: opts.Domain, shard: shard}] = leaseRuntime
	}
	return nil
}

func validateGrantedRoute(opts Options, snapshot actorrouting.RouteSnapshot, shard uint32, route actorrouting.ShardRoute, result authoritystore.ShardAuthorityTransitionResult) error {
	authority := result.Authority
	if authority.AuthorityLayoutVersion != opts.Profile.AuthorityLayoutVersion || authority.HashVersion != snapshot.HashVersion || authority.HashSeed != snapshot.HashSeed || authority.Domain != opts.Domain || authority.VirtualShard != shard || authority.HolderID != opts.HolderID || authority.HolderBootID != opts.HolderBootID || authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch || authority.LeaseEpoch != route.ExpectedLeaseEpoch || authority.State != authoritystore.ShardAuthorityActive || authority.LeaseUntilMs <= 0 || result.AcceptedTTLms <= 0 {
		return fmt.Errorf("authority composition: Redis %s shard %d generation does not exactly match RouteSnapshot", opts.Domain, shard)
	}
	return nil
}

func (r *Runtime) runApplier(ctx context.Context) {
	defer r.wg.Done()
	if err := r.Applier.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		r.enterFatal(fmt.Errorf("authority composition: route applier exited: %w", err))
	}
}

func (r *Runtime) runRenewal(ctx context.Context, opts Options) {
	defer r.wg.Done()
	ticker := time.NewTicker(opts.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.leaseMu.RLock()
			leases := make(map[leaseKey]*routeauthority.HolderLeaseRuntime, len(r.leases))
			for key, lease := range r.leases {
				leases[key] = lease
			}
			r.leaseMu.RUnlock()
			for key, lease := range leases {
				if !r.renewLease(ctx, opts, key, lease) {
					r.enterFatal(fmt.Errorf("authority composition: legacy lease renewal failed for %v", key))
					return
				}
			}
		}
	}
}

func (r *Runtime) renewLease(ctx context.Context, opts Options, key leaseKey, lease *routeauthority.HolderLeaseRuntime) bool {
	if lease == nil {
		return false
	}
	snapshot := lease.Guard().Snapshot()
	if !snapshot.Admitted {
		return false
	}
	authority := snapshot.Authority
	result, err := lease.Renew(ctx, r.Store, authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: opts.HolderID, BootID: opts.HolderBootID},
		AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion, Domain: key.domain, VirtualShard: key.shard,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
			ExpectedAssignmentRevision: authority.AssignmentRevision, ExpectedWriterEpoch: authority.WriterEpoch,
			ExpectedLeaseEpoch: authority.LeaseEpoch,
		},
		RequestedTTLms: opts.LeaseTTL.Milliseconds(),
	})
	return err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied)
}

// ProcessReady reports common-dependency health for authority mode.  It is
// derived from the single lifecycle truth (membershipState == Ready), so it is
// true only after the composition converged and becomes false on any fatal
// control-plane/applier/lease failure.  It deliberately does not depend on any
// local shard lease, so a zero-shard holder stays process-ready.
func (r *Runtime) ProcessReady() bool {
	if r == nil {
		return false
	}
	return r.membershipState.Load() == membershipStateReady
}

// Ready is the process-level readiness alias retained for the existing
// command root.  It expresses exactly the same common-dependency health as
// ProcessReady and never requires a local shard.
func (r *Runtime) Ready() bool {
	return r.ProcessReady()
}

// CanServeShard reports single-shard admission independently of process
// readiness.  A shard is servable only when its HolderLeaseGuard is locally
// installed, admitted, and its authority generation is ACTIVE.  A zero-shard
// holder therefore reports false for every shard while remaining
// process-ready.
func (r *Runtime) CanServeShard(domain actorrouting.RouteDomain, shard uint32) bool {
	if r == nil {
		return false
	}
	snapshot, ok := r.Guards.Snapshot(domain, shard)
	if !ok {
		return false
	}
	return r.ownsGuard(snapshot) && snapshot.Admitted && snapshot.Authority.State == authoritystore.ShardAuthorityActive
}

// ServingShardCount returns the number of local shards that are currently
// servable.  It is the observable complement of CanServeShard and is zero for
// a healthy zero-shard holder.  It is derived from the guard registry so both
// the legacy New path and the RT1B-3C-A membership path report the same count.
func (r *Runtime) ServingShardCount() int {
	if r == nil {
		return 0
	}
	count := 0
	for _, snap := range r.Guards.SnapshotAll() {
		if !r.ownsGuard(snap) {
			continue
		}
		if snap.Admitted && snap.Authority.State == authoritystore.ShardAuthorityActive {
			count++
		}
	}
	return count
}

func (r *Runtime) MembershipEvidenceStats() MembershipEvidenceStats {
	if r == nil {
		return MembershipEvidenceStats{}
	}
	membership := r.membership.Load()
	if membership == nil {
		return MembershipEvidenceStats{}
	}
	return membership.EvidenceStats()
}

func (r *Runtime) LeaseSchedulerEntryCount() int {
	if r == nil || r.scheduler == nil {
		return 0
	}
	return r.scheduler.EntryCount()
}

// SetMembershipReconciler injects the membership reconciler port exactly once.
// A second injection (including a replacement pointer) is rejected so external
// code can never swap the reconciler while it is running.
func (r *Runtime) SetMembershipReconciler(m *ShardMembershipReconciler) error {
	if r == nil {
		return errors.New("authority composition: nil runtime")
	}
	if m == nil {
		return errors.New("authority composition: membership reconciler is required")
	}
	if !r.membership.CompareAndSwap(nil, m) {
		return errors.New("authority composition: membership reconciler is already set and cannot be replaced")
	}
	return nil
}

// MembershipReconciler returns the injected reconciler (nil when unset).  The
// returned pointer is stable; callers may use its thread-safe methods but can
// never replace the runtime reference.
func (r *Runtime) MembershipReconciler() *ShardMembershipReconciler {
	if r == nil {
		return nil
	}
	return r.membership.Load()
}

// MembershipInstaller exposes only the existing prepare/commit port to the
// HolderControl composition seam.  It does not expose the scheduler, Redis
// authority store, or any route-manager capability.
func (r *Runtime) MembershipInstaller() MembershipInstaller {
	if r == nil {
		return nil
	}
	return r.membershipInstaller
}

// enterFatal moves the composition into the terminal fatal state and FAILS
// THE LOCAL DATA PLANE CLOSED immediately: every guard owned by this runtime
// stops admitting, every owned scheduler entry is cancelled, snapshot inputs
// are stopped, and the fatal cause is recorded.  It is the only way the state
// leaves Bootstrap/Ready and it can never be reversed (Fatal → Fatal).
// Callers must NOT hold r.scheduler.mu / r.Guards locks: the cleanup takes
// both.  Close() must NOT use enterFatal -- a graceful shutdown is Closed, not
// Fatal (no FatalErr, no OnFatal, no fatal metrics).
func (r *Runtime) enterFatal(err error) {
	for {
		switch r.membershipState.Load() {
		case membershipStateFatal, membershipStateClosed:
			// Who wins the terminal race wins: a graceful Close that already
			// moved to Closed must not be re-interpreted as a fault (no
			// FatalErr, no OnFatal), and an existing Fatal keeps its cause.
			return
		case membershipStateBootstrap:
			if r.membershipState.CompareAndSwap(membershipStateBootstrap, membershipStateFatal) {
				r.enterFatalCleanup(err)
				return
			}
		case membershipStateReady:
			if r.membershipState.CompareAndSwap(membershipStateReady, membershipStateFatal) {
				r.enterFatalCleanup(err)
				return
			}
		}
	}
}

// enterFatalCleanup runs once when the state CAS to Fatal succeeds, in a strict
// order that leaves no re-admit window:
//
//  1. record the primary fatal cause
//  2. stop Runtime-owned snapshot inputs (observer and static applier ctx;
//     the remote applier remains AssignmentComposition-owned)
//  3. stop the reconciler (closed → retry loop stops, in-flight/in-flight
//     commitPrepared is rejected, no further Guard installs)
//  4. cancel owned scheduler entries (generation-fenced; never
//     scheduler.Close, which would wg.Wait on the entry goroutine that may be
//     running this cleanup via OnFatal)
//  5. fence/remove owned guards (also cleans any guard installed by an
//     in-flight commit that finished during step 3)
//  6. notify the external OnFatal (outside every lock, at most once)
func (r *Runtime) enterFatalCleanup(err error) {
	if err != nil {
		r.fatalErr.Store(&errorRecord{err: err})
	}
	if r.cancel != nil {
		r.cancel()
	}
	if m := r.membership.Load(); m != nil {
		m.Close()
	}
	if r.scheduler != nil {
		for key, gen := range r.scheduler.ActiveEntries() {
			_ = r.scheduler.Cancel(key, gen)
		}
	}
	r.failClosedLocalState()
	if onFatal := r.OnFatal; onFatal != nil {
		onFatal(err)
	}
}

// FatalErr returns the primary fatal cause recorded when the runtime entered
// Fatal, or nil while the runtime is not fatal.  A graceful Close never
// records one.
func (r *Runtime) FatalErr() error {
	if r == nil {
		return nil
	}
	if rec := r.fatalErr.Load(); rec != nil {
		return rec.err
	}
	return nil
}

// ownsGuard reports whether the guard snapshot belongs to THIS runtime: the
// same domain, holder instance and holder boot.  Shared registries or future
// multi-domain compositions must never let one runtime fence/remove or count
// another runtime's guards.  The authority layout is frozen to a single v1
// profile per holder process (Options.Profile validation), so within one
// holder/boot only one authority layout ever runs; layout migration must not
// share a registry between two runtimes.
func (r *Runtime) ownsGuard(snap routeauthority.LeaseGenerationSnapshot) bool {
	a := snap.Authority
	return a.Domain == r.Domain && a.HolderID == r.HolderID && a.HolderBootID == r.HolderBootID
}

// failClosedLocalState fences and removes every guard OWNED by this runtime so
// a failed startup, a scheduler fatal, or Close leaves no admitted shard
// behind.  Guards of other runtimes/domains/holders are never touched.
func (r *Runtime) failClosedLocalState() {
	for _, snap := range r.Guards.SnapshotAll() {
		if snap.Authority.AssignmentRevision == 0 || !r.ownsGuard(snap) {
			continue
		}
		r.Guards.Remove(snap.Authority.Domain, snap.Authority.VirtualShard)
	}
}

// Close gracefully stops the composition in a complete local fail-closed order
// and moves the lifecycle to Closed (NOT Fatal -- a graceful shutdown is not a
// runtime fault, so it records no FatalErr and invokes no OnFatal):
//
//	state=Closed
//	→ stop Runtime-owned snapshot inputs (observer/static applier)
//	→ Close the reconciler (no more Observe/Activate)
//	→ Close the scheduler (cancel entries, wait renew goroutines)
//	→ fence/remove every installed guard
//	→ fence legacy leases
//
// After Close returns there are no admitted guards, no scheduler entries and
// no running membership goroutines; a late snapshot is rejected and an
// in-flight renewal can never re-admit.  Close is idempotent.  Redis itself
// remains owned by the command root.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	// Whoever wins the terminal race wins: if a fatal already moved the state
	// to Fatal (or another Close to Closed), keep it -- a Close must never
	// overwrite Fatal or its primary cause, and must never record a fault for
	// a graceful shutdown.
	for {
		switch r.membershipState.Load() {
		case membershipStateClosed, membershipStateFatal:
			// already terminal, keep it
		case membershipStateBootstrap:
			if r.membershipState.CompareAndSwap(membershipStateBootstrap, membershipStateClosed) {
				break
			}
			continue
		case membershipStateReady:
			if r.membershipState.CompareAndSwap(membershipStateReady, membershipStateClosed) {
				break
			}
			continue
		}
		break
	}
	// Stop snapshot inputs, then cancel and join the reconciler's retry/action
	// lifetime before waiting for the observer/applier owners. An observer
	// parked in a context-compliant Port receives the Runtime cancellation;
	// reconciler.Close supplies the action barrier, so no late Observe can
	// re-install after cleanup.
	if r.cancel != nil {
		r.cancel()
	}
	if m := r.membership.Load(); m != nil {
		m.Close()
	}
	if r.cancel != nil {
		r.wg.Wait()
	}
	// Stop the reconciler before the scheduler so no retry can Activate a new
	// entry after entries are cancelled.
	if r.scheduler != nil {
		r.scheduler.Close()
	}
	r.failClosedLocalState()
	r.leaseMu.RLock()
	for _, lease := range r.leases {
		if lease != nil && lease.Guard() != nil {
			lease.Guard().Fence()
		}
	}
	r.leaseMu.RUnlock()
	return nil
}

// ParticipantRouteFence layers the existing Plant route proof with the
// local authority fence.  Its resolver reads a complete dynamic snapshot but
// deliberately does not re-adjudicate the Redis wall-clock lease; the
// HolderLeaseGuard is the one-time converted local deadline for hot-path
// admission, while DynamicFenceSource verifies the exact authority tuple.
type ParticipantRouteFence struct {
	delegate *plantonline.RouteFence
	source   *routeauthority.DynamicFenceSource
	domain   actorrouting.RouteDomain
}

var _ contracts.ParticipantRouteFenceVerifier = (*ParticipantRouteFence)(nil)

func NewParticipantRouteFence(runtime *Runtime) (*ParticipantRouteFence, error) {
	if runtime == nil || runtime.AssignmentProvider == nil || runtime.Calculator == nil || runtime.FenceSource == nil || runtime.Resolver == nil {
		return nil, errors.New("authority composition: participant route fence dependencies are incomplete")
	}
	resolver := &snapshotResolver{source: runtime.AssignmentProvider, calculator: runtime.Calculator}
	delegate, err := plantonline.NewRouteFence(resolver, runtime.Resolver.Realm(), runtime.Domain, runtime.HolderID, runtime.HolderBootID)
	if err != nil {
		return nil, err
	}
	return &ParticipantRouteFence{delegate: delegate, source: runtime.FenceSource, domain: runtime.Domain}, nil
}

func (f *ParticipantRouteFence) VerifyRoute(ctx context.Context, call *contracts.ParticipantCall) error {
	if f == nil || f.delegate == nil || f.source == nil || call == nil || call.RouteHeader == nil {
		return errors.New("authority composition: incomplete participant route proof")
	}
	if err := f.delegate.VerifyRoute(ctx, call); err != nil {
		return err
	}
	_, err := f.source.WriteFence(ctx, authoritystore.ActorAddress{Domain: f.domain, ActorID: call.RouteHeader.GetActorId()})
	return err
}

type snapshotResolver struct {
	source     routeauthority.RouteSnapshotSource
	calculator *actorrouting.Calculator
}

func (r *snapshotResolver) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	if r == nil || r.source == nil || r.calculator == nil || ctx == nil {
		return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
	}
	if err := ctx.Err(); err != nil {
		return actorrouting.Assignment{}, err
	}

	domain := actorrouting.RouteDomain(address.Kind)
	shard, err := r.calculator.ShardFor(domain, address.ID)
	if err != nil {
		return actorrouting.Assignment{}, err
	}

	// The participant route proof needs exactly one coherent route/Holder pair.
	// Production's DynamicAssignmentProvider implements the shard-only view, so
	// avoid copying every domain's route table on each Participant call. The
	// complete-snapshot fallback remains for static/test providers and preserves
	// the original compatibility contract.
	var (
		realm          string
		globalRevision uint64
		route          actorrouting.ShardRoute
		holder         actorrouting.HolderRef
	)
	if fast, ok := r.source.(routeauthority.ShardRouteSnapshotSource); ok {
		view, lookupErr := fast.LookupShard(domain, shard)
		if lookupErr != nil {
			return actorrouting.Assignment{}, lookupErr
		}
		realm = view.Realm
		globalRevision = view.GlobalRevision
		route = view.Route
		holder = view.Holder
	} else {
		snapshot := r.source.Snapshot()
		realm = snapshot.Realm
		globalRevision = snapshot.GlobalRevision
		routes := snapshot.Player
		if domain == actorrouting.DomainFarm {
			routes = snapshot.Farm
		}
		if shard >= routes.ShardCount || int(shard) >= len(routes.Routes) {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		route = routes.Routes[shard]
		if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		holder = snapshot.Holders[route.HolderIndex]
	}
	if address.Realm != realm {
		return actorrouting.Assignment{}, actorrouting.ErrInvalidAddress
	}
	if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
		return actorrouting.Assignment{}, actorrouting.ErrShardDraining
	}
	if holder.State != actorrouting.HolderStateActive || holder.InstanceID == "" || holder.BootID == "" {
		return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
	}
	return actorrouting.Assignment{
		Kind: actorrouting.Kind(address.Kind), ShardID: shard, HolderPod: holder.InstanceID,
		HolderBootID: holder.BootID, Endpoint: holder.Endpoint, ShardRevision: route.ShardRevision,
		AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch,
		LeaseEpoch: route.ExpectedLeaseEpoch, LeaseUntilMs: route.LeaseUntilMs,
		SnapshotRevision: globalRevision, HolderState: holder.State, ShardState: route.State,
	}, nil
}

func newPlacementResolver(profile rediskey.PhysicalProfile, calculator *actorrouting.Calculator) (workidentity.BucketResolver, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if calculator == nil {
		return nil, errors.New("authority composition: placement route calculator is required")
	}
	// Exercise the exact workflow bucket path with a canonical stable ID. The
	// old process roots used a human-readable probe, but W31 IDs are generated
	// sid1_ identities rather than arbitrary strings.
	probe, err := workidentity.StableOpaqueID("authority-composition-placement-probe")
	if err != nil {
		return nil, err
	}
	if _, err := profile.WorkflowBucket(probe); err != nil {
		return nil, err
	}
	return rediskey.PlacementResolver{Profile: profile, Shards: calculator}, nil
}

func requiredDuration(key string) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, fmt.Errorf("authority composition: %s is required in authority mode", key)
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("authority composition: %s must be a positive duration: %q", key, raw)
	}
	return value, nil
}

func requiredUint(key string) (uint32, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, fmt.Errorf("authority composition: %s is required in authority mode", key)
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("authority composition: %s must be a positive uint32: %q", key, raw)
	}
	return uint32(parsed), nil
}

func envPositiveInt(key string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func optionalBool(key string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("authority composition: %s must be boolean: %w", key, err)
	}
	return value, nil
}
