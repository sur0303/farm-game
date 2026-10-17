package routeplan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/controlfence"
	routecontrol "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
)

type controlMutationTestCoordinator struct {
	mu           sync.Mutex
	issuer       *controlfence.Issuer
	expired      bool
	acquireCalls int
	verifyCalls  int
	resolveCalls int
	kinds        []core.ControlOperationKind
	operationIDs []string
	owners       []core.MutationLeaseOwner
	resolved     map[string]bool
}

type knownCreateFailureStore struct {
	PlanStore
	err error
}

func (s knownCreateFailureStore) CompareAndSwap(context.Context, uint64, Plan) (CASResult, error) {
	return CASResult{}, s.err
}

func newControlMutationTestCoordinator() *controlMutationTestCoordinator {
	return &controlMutationTestCoordinator{issuer: controlfence.NewIssuer(), resolved: make(map[string]bool)}
}

func (c *controlMutationTestCoordinator) Acquire(ctx context.Context, request core.ControlOperationRequest) (core.ControlOperationToken, error) {
	if ctx == nil {
		return core.ControlOperationToken{}, errors.New("nil context")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquireCalls++
	if c.resolved[request.OperationID] {
		return core.ControlOperationToken{}, errors.New("operation already resolved")
	}
	c.operationIDs = append(c.operationIDs, request.OperationID)
	c.kinds = append(c.kinds, request.Kind)
	c.owners = append(c.owners, request.Owner)
	token := c.issuer.Issue(request.OperationID, 1)
	if !token.Issued() {
		return core.ControlOperationToken{}, errors.New("unissued token")
	}
	return token, nil
}

func (c *controlMutationTestCoordinator) Verify(_ context.Context, token core.ControlOperationToken, _ core.MutationScope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.verifyCalls++
	if c.expired {
		return errors.New("expired control token")
	}
	if !c.issuer.IssuedBy(token) {
		return errors.New("foreign control token")
	}
	return nil
}

func (c *controlMutationTestCoordinator) Resolve(_ context.Context, token core.ControlOperationToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolveCalls++
	c.resolved[token.OperationID()] = true
	return nil
}

func (c *controlMutationTestCoordinator) Mode(context.Context, core.MutationScope) (core.ControlGateMode, error) {
	return core.ControlGateOpen, nil
}

func controlMutationTestOptions(coordinator core.ControlMutationCoordinator) ControlMutationOptions {
	var nonceMu sync.Mutex
	nonce := 0
	return ControlMutationOptions{
		Coordinator: coordinator,
		Scope:       core.MutationScope{Namespace: "farm", Realm: "classic-farm"},
		Owner:       core.MutationLeaseOwner{ControllerID: "route-controller", ControllerBootID: "boot-a"},
		NonceSource: func() (string, error) {
			nonceMu.Lock()
			defer nonceMu.Unlock()
			nonce++
			return fmt.Sprintf("nonce-%d", nonce), nil
		},
	}
}

func TestControlMutationOffIsNoop(t *testing.T) {
	ctx := context.Background()
	bound, resolve, err := (ControlMutationOptions{}).Begin(ctx, "off", core.ControlOperationRoutePlan)
	if err != nil || bound != ctx || resolve != nil {
		t.Fatalf("off begin changed behavior: bound_same=%t resolve_nil=%t err=%v", bound == ctx, resolve == nil, err)
	}
}

func TestControlMutationAllocatesNoncePerTopLevelOperation(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	options := controlMutationTestOptions(coordinator)
	for _, operationID := range []string{"first", "second"} {
		_, resolve, err := options.Begin(context.Background(), operationID, core.ControlOperationRoutePlan)
		if err != nil {
			t.Fatal(err)
		}
		if err := resolve(); err != nil {
			t.Fatal(err)
		}
	}
	if len(coordinator.owners) != 2 {
		t.Fatalf("owners=%d, want 2", len(coordinator.owners))
	}
	if coordinator.owners[0].OperationNonce == coordinator.owners[1].OperationNonce {
		t.Fatalf("top-level operations reused nonce %q", coordinator.owners[0].OperationNonce)
	}
	if options.Owner.OperationNonce != "" {
		t.Fatalf("composition owner retained operation nonce %q", options.Owner.OperationNonce)
	}
}

func TestControlMutationRejectsStaticOrFailedNonceSource(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	static := controlMutationTestOptions(coordinator)
	static.Owner.OperationNonce = "static"
	if err := static.Validate(); !errors.Is(err, ErrControlMutation) {
		t.Fatalf("static nonce validation error=%v", err)
	}

	want := errors.New("entropy unavailable")
	failed := controlMutationTestOptions(coordinator)
	failed.NonceSource = func() (string, error) { return "", want }
	if _, _, err := failed.Begin(context.Background(), "failed", core.ControlOperationRoutePlan); !errors.Is(err, ErrControlMutation) || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("nonce source error=%v", err)
	}
}

func TestControlMutationNestedInheritanceUsesOneToken(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	options := controlMutationTestOptions(coordinator)
	outer, resolve, err := options.Begin(context.Background(), "outer", core.ControlOperationRoutePlan)
	if err != nil {
		t.Fatal(err)
	}
	nested, nestedResolve, err := options.Begin(outer, "nested", core.ControlOperationProjection)
	if err != nil {
		t.Fatal(err)
	}
	outerToken, outerOK := authority.ControlOperationTokenFromContext(outer)
	nestedToken, nestedOK := authority.ControlOperationTokenFromContext(nested)
	if !outerOK || !nestedOK || outerToken != nestedToken || nestedResolve != nil || coordinator.acquireCalls != 1 || coordinator.verifyCalls != 2 {
		t.Fatalf("nested control token was reacquired or changed: outer_ok=%t nested_ok=%t same=%t nested_resolve_nil=%t acquires=%d verifies=%d", outerOK, nestedOK, outerToken == nestedToken, nestedResolve == nil, coordinator.acquireCalls, coordinator.verifyCalls)
	}
	if err := resolve(); err != nil || coordinator.resolveCalls != 1 {
		t.Fatalf("outer resolve err=%v resolves=%d", err, coordinator.resolveCalls)
	}
}

func TestControlMutationForeignAndExpiredTokensRejected(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	options := controlMutationTestOptions(coordinator)
	foreign := controlfence.NewIssuer().Issue("foreign", 1)
	if _, _, err := options.Begin(authority.WithControlOperationToken(context.Background(), foreign), "foreign-nested", core.ControlOperationRoutePlan); !errors.Is(err, ErrControlMutation) {
		t.Fatalf("foreign token accepted: %v", err)
	}
	bound, _, err := options.Begin(context.Background(), "expired", core.ControlOperationRoutePlan)
	if err != nil {
		t.Fatal(err)
	}
	token, ok := authority.ControlOperationTokenFromContext(bound)
	if !ok {
		t.Fatal("coordinator did not issue an inherited token")
	}
	coordinator.expired = true
	if _, _, err := options.Begin(authority.WithControlOperationToken(context.Background(), token), "expired-nested", core.ControlOperationRoutePlan); !errors.Is(err, ErrControlMutation) {
		t.Fatalf("expired token accepted: %v", err)
	}
}

func TestControlMutationBootstrapAndRecoveryKinds(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	options := controlMutationTestOptions(coordinator)
	ctx, resolve, err := options.Begin(context.Background(), "bootstrap", core.ControlOperationBootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolve(); err != nil {
		t.Fatal(err)
	}
	if _, resolve, err = options.Begin(context.Background(), "recovery", core.ControlOperationRecovery); err != nil {
		t.Fatal(err)
	} else if err := resolve(); err != nil {
		t.Fatal(err)
	}
	if len(coordinator.kinds) != 2 || coordinator.kinds[0] != core.ControlOperationBootstrap || coordinator.kinds[1] != core.ControlOperationRecovery || ctx == nil || coordinator.resolveCalls != 2 {
		t.Fatalf("bootstrap/recovery kinds=%v resolves=%d", coordinator.kinds, coordinator.resolveCalls)
	}
}

func TestPlanExecutorRecoveryUsesRecoveryControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	if err := fixture.executor.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 || len(coordinator.kinds) != 1 || coordinator.kinds[0] != core.ControlOperationRecovery {
		t.Fatalf("recovery control calls=%d resolves=%d kinds=%v", coordinator.acquireCalls, coordinator.resolveCalls, coordinator.kinds)
	}
}

func TestDurableSnapshotBootstrapRequiresIssuedTokenWhenGated(t *testing.T) {
	fixture := newExecutorFixture(t)
	publisher, err := routecontrol.NewDurableSnapshotPublisher(fixture.snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	publisher.RequireControlToken()
	if _, err := publisher.Bootstrap(context.Background(), routecontrol.NestedControlOperationToken{}, fixture.manager.snapshot); !errors.Is(err, routecontrol.ErrSnapshotCASConflict) {
		t.Fatalf("gated bootstrap accepted zero token: %v", err)
	}
}

func TestPlanExecutorSubmitAndExecuteUseDistinctControlOperations(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	if len(coordinator.operationIDs) != 2 || coordinator.operationIDs[0] == coordinator.operationIDs[1] || coordinator.resolveCalls != 2 {
		t.Fatalf("submit/execute operation IDs=%v resolves=%d", coordinator.operationIDs, coordinator.resolveCalls)
	}
}

func TestPlanExecutorBusyRecoveryDoesNotAcquireControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	key := shardKey{domain: record.Plan.Domain, shard: record.Plan.VirtualShard}
	if err := fixture.executor.acquireShard(key, "foreground-auto-failover"); err != nil {
		t.Fatal(err)
	}
	defer fixture.executor.releaseShard(key)

	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); !errors.Is(err, ErrShardBusy) {
		t.Fatalf("overlapping recovery err=%v, want %v", err, ErrShardBusy)
	}
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 {
		t.Fatalf("busy recovery allocated or leaked a token: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
}

func TestPlanExecutorForegroundBatchExcludesGenericRecovery(t *testing.T) {
	fixture := newExecutorFixture(t)
	release, err := fixture.executor.BeginForegroundBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- fixture.executor.Recover(context.Background()) }()
	select {
	case err := <-recoveryDone:
		t.Fatalf("generic recovery entered active foreground batch: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case err := <-recoveryDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("generic recovery did not resume after foreground batch released")
	}
}

func TestControlMutationBeginWrapsAlreadyResolvedAcquire(t *testing.T) {
	coordinator := newControlMutationTestCoordinator()
	options := controlMutationTestOptions(coordinator)
	_, resolve, err := options.Begin(context.Background(), "op-1", core.ControlOperationRoutePlan)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolve(); err != nil {
		t.Fatal(err)
	}
	_, _, err = options.Begin(context.Background(), "op-1", core.ControlOperationRoutePlan)
	if !errors.Is(err, ErrControlOperationResolved) || !errors.Is(err, ErrControlMutation) {
		t.Fatalf("begin resolved operation err=%v", err)
	}
}

func TestPlanExecutorSubmitRetriesAfterTargetHeartbeatAdvances(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)

	target := fixture.holders.records["holder-b"]
	target.State = routecontrol.HolderStateWarming
	target.ProcessReady = true
	target.Sequence = 1
	fixture.holders.records["holder-b"] = target
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("first submit err=%v, want target-not-ready conflict", err)
	}
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 {
		t.Fatalf("first submit control calls acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}

	target.State = routecontrol.HolderStateReadyZeroShard
	target.ProcessReady = true
	target.Sequence++
	fixture.holders.records["holder-b"] = target
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil || record.Plan.PlanID != fixture.moveRequest().PlanID {
		t.Fatalf("retry record=%+v err=%v", record.Plan, err)
	}
	if coordinator.acquireCalls != 2 || coordinator.resolveCalls != 2 {
		t.Fatalf("retry control calls acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
	if len(coordinator.operationIDs) != 2 || coordinator.operationIDs[0] == coordinator.operationIDs[1] {
		t.Fatalf("submit operation IDs did not advance with target heartbeat: %v", coordinator.operationIDs)
	}
}

func TestPlanExecutorPausedSnapshotPendingRetryUsesNewControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	fixture.manager.skipSnapshot = true
	record := fixture.persistOwnedMove(t, fixture.ownedMoveRequest())
	paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, ErrVerification) || paused.Plan.Phase != PhasePaused || paused.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
		t.Fatalf("paused=%+v err=%v", paused.Plan, err)
	}
	pausedExecuteID := planControlOperationID("execute", fmt.Sprintf("%s/revision-%d", paused.Plan.PlanID, paused.Plan.Revision))
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 {
		t.Fatalf("first execute leaked control operation: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}

	recoveredSnapshot := targetSnapshot(fixture.manager.snapshot, fixture.target)
	if _, err := fixture.snapshot.CompareAndSwap(context.Background(), routecontrol.NestedControlOperationToken{}, 1, recoveredSnapshot); err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Phase != PhaseSucceeded {
		t.Fatalf("retry=%+v err=%v", completed.Plan, err)
	}
	if coordinator.acquireCalls != 2 || coordinator.resolveCalls != 2 {
		t.Fatalf("retry did not allocate a new execute operation: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
	lastID := coordinator.operationIDs[len(coordinator.operationIDs)-1]
	if lastID == pausedExecuteID {
		t.Fatalf("retry acquired already-resolved execute id %s", lastID)
	}
}

func TestPlanExecutorPausedRetryDoesNotReuseResolvedExecuteOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	fixture.manager.skipSnapshot = true
	record := fixture.persistOwnedMove(t, fixture.ownedMoveRequest())
	paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, ErrVerification) || paused.Plan.Phase != PhasePaused {
		t.Fatalf("paused=%+v err=%v", paused.Plan, err)
	}
	pausedRevision := paused.Plan.Revision
	fixture.executor.executionGuard = &executorGuardFake{err: errors.New("projection recovery unavailable")}

	retried, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if errors.Is(err, ErrControlOperationResolved) {
		t.Fatalf("paused retry hit resolved execute revision %d: err=%v", pausedRevision, err)
	}
	if retried.Plan.Phase != PhasePaused || retried.Plan.Revision <= pausedRevision {
		t.Fatalf("paused retry=%+v err=%v, want a new paused revision", retried.Plan, err)
	}
	if coordinator.acquireCalls != 2 || coordinator.resolveCalls != 2 {
		t.Fatalf("paused retry control calls acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
}

func TestPlanExecutorDurablePausedOutcomeResolvesControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	fixture.executor.executionGuard = &executorGuardFake{err: errors.New("projection recovery unavailable")}

	paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err == nil || paused.Plan.Phase != PhasePaused {
		t.Fatalf("execute result=%+v err=%v, want durable PAUSED", paused.Plan, err)
	}
	if coordinator.acquireCalls != 2 || coordinator.resolveCalls != 2 {
		t.Fatalf("durable paused outcome leaked control operation: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
}

func TestPlanExecutorKnownCreateFailureResolvesControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	fixture.executor.store = knownCreateFailureStore{PlanStore: fixture.store, err: fmt.Errorf("%w: LIVE_PLAN", ErrPlanCorrupt)}

	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); !errors.Is(err, ErrPlanCorrupt) {
		t.Fatalf("submit err=%v, want %v", err, ErrPlanCorrupt)
	}
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 {
		t.Fatalf("known fail-before-write outcome leaked control operation: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
}

func TestPlanExecutorSubmitValidationFailureReleasesControlOperation(t *testing.T) {
	fixture := newExecutorFixture(t)
	coordinator := newControlMutationTestCoordinator()
	fixture.executor.control = controlMutationTestOptions(coordinator)
	target := fixture.holders.records["holder-b"]
	target.ProcessReady = false
	fixture.holders.records["holder-b"] = target
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); err == nil {
		t.Fatal("invalid target unexpectedly submitted")
	}
	if coordinator.acquireCalls != 1 || coordinator.resolveCalls != 1 {
		t.Fatalf("validation failure leaked control operation: acquires=%d resolves=%d", coordinator.acquireCalls, coordinator.resolveCalls)
	}
}
