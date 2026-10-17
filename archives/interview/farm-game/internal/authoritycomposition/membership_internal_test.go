package authoritycomposition

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

// stubPorts is a minimal in-package Fake used only by internal tests.  The
// richer Fake lives in the testkit package for external tests.
type stubPorts struct {
	mu         sync.Mutex
	installs   []ShardMembership
	fences     []ShardMembership
	removes    []ShardMembership
	order      []string
	installErr error
	fenceErr   error
	removeErr  error
}

func newStubPorts() *stubPorts { return &stubPorts{} }

func (s *stubPorts) PrepareMembership(ctx context.Context, m ShardMembership) (*PreparedMembership, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.order = append(s.order, "prepare")
	return &PreparedMembership{Key: m.Key, Generation: m.Generation, AcceptedTTLms: 1000}, nil
}

func (s *stubPorts) CommitPreparedMembership(ctx context.Context, prepared PreparedMembership) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.installs = append(s.installs, ShardMembership{Key: prepared.Key, Generation: prepared.Generation})
	s.order = append(s.order, "install")
	return s.installErr
}

func (s *stubPorts) FenceMembership(ctx context.Context, m ShardMembership) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fences = append(s.fences, m)
	s.order = append(s.order, "fence")
	return s.fenceErr
}

func (s *stubPorts) RemoveMembership(ctx context.Context, m ShardMembership) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removes = append(s.removes, m)
	s.order = append(s.order, "remove")
	return s.removeErr
}

func membershipTestHolders() []actorrouting.HolderRef {
	return []actorrouting.HolderRef{
		{InstanceID: "holder-1", BootID: "boot-1", Endpoint: "h1:9100", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: "holder-2", BootID: "boot-2", Endpoint: "h2:9100", State: actorrouting.HolderStateActive, Weight: 1},
	}
}

func membershipTestSnapshot(rev uint64, routes []actorrouting.ShardRoute) actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm: "membership-test", GlobalRevision: rev,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608060001,
		Player:  actorrouting.DomainSnapshot{ShardCount: uint32(len(routes)), Routes: routes},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 0},
		Holders: membershipTestHolders(),
	}
}

func membershipTestActiveRoute(holderIndex int, rev, writer, lease uint64) actorrouting.ShardRoute {
	return actorrouting.ShardRoute{
		HolderIndex: holderIndex, State: actorrouting.ShardStateActive,
		ShardRevision: rev, ExpectedWriterEpoch: writer, ExpectedLeaseEpoch: lease,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
	}
}

func TestDesiredLocalShardsSelectsOnlyLocalActiveCompleteRoutes(t *testing.T) {
	snapshot := membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1), // holder-2 (local), ACTIVE -> desired
	})
	desired := desiredLocalShards(snapshot, "holder-2", "boot-2", nil)
	if len(desired) != 1 {
		t.Fatalf("desired=%d, want 1", len(desired))
	}
	if gen, ok := desired[ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0}]; !ok ||
		gen.HolderID != "holder-2" || gen.HolderBootID != "boot-2" || gen.AssignmentRevision != 1 {
		t.Fatalf("unexpected desired generation: %+v ok=%v", gen, ok)
	}

	// Foreign holder route is never desired.
	foreign := desiredLocalShards(membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(0, 1, 1, 1),
	}), "holder-2", "boot-2", nil)
	if len(foreign) != 0 {
		t.Fatalf("foreign holder route became desired: %d", len(foreign))
	}

	// Boot mismatch is never desired.
	bootMismatch := desiredLocalShards(membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	}), "holder-2", "wrong-boot", nil)
	if len(bootMismatch) != 0 {
		t.Fatalf("boot-mismatched route became desired: %d", len(bootMismatch))
	}

	// Incomplete route (zero epochs) is never desired.
	incomplete := desiredLocalShards(membershipTestSnapshot(1, []actorrouting.ShardRoute{
		{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1},
	}), "holder-2", "boot-2", nil)
	if len(incomplete) != 0 {
		t.Fatalf("incomplete route became desired: %d", len(incomplete))
	}

	// Registered holder with zero routes -> empty desired (zero-shard topology).
	zero := desiredLocalShards(membershipTestSnapshot(1, nil), "holder-2", "boot-2", nil)
	if len(zero) != 0 {
		t.Fatalf("zero-route snapshot produced desired shards: %d", len(zero))
	}
}

func TestCommitPreparedRejectsExpiredActionContextBeforeLocalAdmission(t *testing.T) {
	ports := newStubPorts()
	key := ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0}
	generation := MembershipGeneration{
		HolderID: "holder-2", HolderBootID: "boot-2",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
	}
	action := ShardAction{Type: MembershipActionAdd, Shard: key, Generation: generation, Token: 1}
	reconciler := &ShardMembershipReconciler{
		installer: ports, fencer: ports, remover: ports,
		installed: make(map[ShardKey]ShardMembership),
		pending:   map[ShardKey]ShardAction{key: action},
		retries:   make(map[ShardKey]int), degraded: make(map[ShardKey]ShardDegraded),
		inflight: map[ShardKey]uint64{key: action.Token},
		ctx:      context.Background(),
	}
	actionCtx, cancelAction := context.WithCancel(context.Background())
	cancelAction()
	reconciler.commitPrepared(actionCtx, action, action.Token, &PreparedMembership{
		Key: key, Generation: generation, AcceptedTTLms: 1000,
	})
	if len(ports.installs) != 0 || len(reconciler.installed) != 0 {
		t.Fatalf("expired startup action admitted locally: installs=%d installed=%d", len(ports.installs), len(reconciler.installed))
	}
	if degraded := reconciler.degraded[key]; degraded.LastErr != context.Canceled.Error() {
		t.Fatalf("expired startup action degraded=%+v", degraded)
	}
}

func TestDesiredLocalShardsCoversBothDomains(t *testing.T) {
	snapshot := membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	})
	snapshot.Farm = actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 2, 2, 2),
	}}
	desired := desiredLocalShards(snapshot, "holder-2", "boot-2", nil)
	if len(desired) != 2 {
		t.Fatalf("desired=%d, want 2 (player+farm)", len(desired))
	}
	if gen, ok := desired[ShardKey{Domain: actorrouting.DomainFarm, Shard: 0}]; !ok || gen.WriterEpoch != 2 {
		t.Fatalf("farm generation not selected: %+v ok=%v", gen, ok)
	}
}

func TestDiffMembershipClassifiesAddRemoveReplaceUnchanged(t *testing.T) {
	key := ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0}
	gen1 := MembershipGeneration{AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, HolderID: "holder-2", HolderBootID: "boot-2", LeaseUntilMs: 1}
	gen2 := MembershipGeneration{AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, HolderID: "holder-2", HolderBootID: "boot-2", LeaseUntilMs: 2}

	// ADD
	adds := diffMembership(map[ShardKey]MembershipGeneration{key: gen1}, nil)
	if len(adds) != 1 || adds[0].Type != MembershipActionAdd {
		t.Fatalf("add actions=%+v", adds)
	}
	// REMOVE
	removes := diffMembership(nil, map[ShardKey]ShardMembership{key: {Key: key, Generation: gen1}})
	if len(removes) != 1 || removes[0].Type != MembershipActionRemove {
		t.Fatalf("remove actions=%+v", removes)
	}
	// UNCHANGED
	unchanged := diffMembership(
		map[ShardKey]MembershipGeneration{key: gen1},
		map[ShardKey]ShardMembership{key: {Key: key, Generation: gen1}})
	if len(unchanged) != 1 || unchanged[0].Type != MembershipActionUnchanged {
		t.Fatalf("unchanged actions=%+v", unchanged)
	}
	// A deadline-only extension is the same immutable fenced generation and
	// must not fence/reinstall it. The desired deadline is still carried in the
	// UNCHANGED action for the deadline-only membership update path.
	deadlineExtended := gen1
	deadlineExtended.LeaseUntilMs++
	deadlineRefresh := diffMembership(
		map[ShardKey]MembershipGeneration{key: deadlineExtended},
		map[ShardKey]ShardMembership{key: {Key: key, Generation: gen1}})
	if len(deadlineRefresh) != 1 || deadlineRefresh[0].Type != MembershipActionUnchanged || deadlineRefresh[0].Generation != deadlineExtended {
		t.Fatalf("deadline refresh actions=%+v", deadlineRefresh)
	}
	// REPLACE (same key, new fenced generation)
	replaces := diffMembership(
		map[ShardKey]MembershipGeneration{key: gen2},
		map[ShardKey]ShardMembership{key: {Key: key, Generation: gen1}})
	if len(replaces) != 1 || replaces[0].Type != MembershipActionReplace || replaces[0].OldGeneration != gen1 || replaces[0].Generation != gen2 {
		t.Fatalf("replace actions=%+v", replaces)
	}
}

func TestDiffMembershipOrdersRemoveBeforeAdd(t *testing.T) {
	playerKey := ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0}
	farmKey := ShardKey{Domain: actorrouting.DomainFarm, Shard: 0}
	oldGen := MembershipGeneration{AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, HolderID: "holder-2", HolderBootID: "boot-2", LeaseUntilMs: 1}
	newGen := MembershipGeneration{AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, HolderID: "holder-2", HolderBootID: "boot-2", LeaseUntilMs: 2}

	actions := diffMembership(
		map[ShardKey]MembershipGeneration{farmKey: newGen},
		map[ShardKey]ShardMembership{
			playerKey: {Key: playerKey, Generation: oldGen},
		})
	// Expected: REMOVE(playerKey) then ADD(farmKey).
	if len(actions) != 2 {
		t.Fatalf("actions=%d, want 2", len(actions))
	}
	if actions[0].Type != MembershipActionRemove || actions[0].Shard != playerKey {
		t.Fatalf("first action must be REMOVE, got %+v", actions[0])
	}
	if actions[1].Type != MembershipActionAdd || actions[1].Shard != farmKey {
		t.Fatalf("second action must be ADD, got %+v", actions[1])
	}
}

func TestReconcilerRevisionAndInstalledLifecycle(t *testing.T) {
	ports := newStubPorts()
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: "holder-2", HolderBootID: "boot-2",
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 3, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	// 0 -> 1: add shard 0.
	rev1 := membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	})
	result := reconciler.Observe(rev1)
	if result.Stale || result.Duplicate || !result.FullyReconciled {
		t.Fatalf("rev1 result=%+v", result)
	}
	if reconciler.LastObservedRevision() != 1 || reconciler.LastFullyReconciledRevision() != 1 {
		t.Fatalf("revision observed=%d reconciled=%d, want 1/1",
			reconciler.LastObservedRevision(), reconciler.LastFullyReconciledRevision())
	}
	members := reconciler.currentInstalledMembership()
	if len(members) != 1 || members[0].Key.Shard != 0 || members[0].Generation.AssignmentRevision != 1 {
		t.Fatalf("installed=%+v", members)
	}

	// Duplicate snapshot is idempotent.
	dup := reconciler.Observe(rev1)
	if !dup.Duplicate || len(dup.Actions) != 0 || !dup.FullyReconciled {
		t.Fatalf("duplicate result=%+v", dup)
	}

	// Stale snapshot is rejected and touches nothing.
	stale := reconciler.Observe(membershipTestSnapshot(0, nil))
	if !stale.Stale || len(stale.Actions) != 0 {
		t.Fatalf("stale result=%+v", stale)
	}

	// 1 -> 0: shard 0 moves to the other holder (ShardCount stays 1 so the
	// topology fingerprint is stable; removal is expressed by reassignment).
	rev2 := membershipTestSnapshot(2, []actorrouting.ShardRoute{
		membershipTestActiveRoute(0, 2, 2, 2),
	})
	result2 := reconciler.Observe(rev2)
	if result2.Stale || !result2.FullyReconciled || len(result2.Actions) != 1 || result2.Actions[0].Type != MembershipActionRemove {
		t.Fatalf("rev2 result=%+v", result2)
	}
	if reconciler.LastFullyReconciledRevision() != 2 {
		t.Fatalf("reconciled revision=%d, want 2", reconciler.LastFullyReconciledRevision())
	}
	if len(reconciler.currentInstalledMembership()) != 0 {
		t.Fatalf("membership not emptied after remove")
	}
	ports.mu.Lock()
	fenceCount := len(ports.fences)
	removeCount := len(ports.removes)
	ports.mu.Unlock()
	if fenceCount != 1 || removeCount != 1 {
		t.Fatalf("fences=%d removes=%d, want 1/1", fenceCount, removeCount)
	}
}

func TestReconcilerDeadlineExtensionAdvancesBookkeepingWithoutMembershipSideEffects(t *testing.T) {
	ports := newStubPorts()
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: "holder-2", HolderBootID: "boot-2",
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 3, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	initial := membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	})
	if result := reconciler.Observe(initial); !result.FullyReconciled {
		t.Fatalf("initial result=%+v", result)
	}
	extended := actorrouting.CloneRouteSnapshot(initial)
	extended.GlobalRevision++
	extended.Player.Routes[0].LeaseUntilMs += int64(time.Minute / time.Millisecond)
	result := reconciler.Observe(extended)
	if !result.FullyReconciled || result.Rejected || result.Stale || len(result.Actions) != 0 || reconciler.LastFullyReconciledRevision() != extended.GlobalRevision {
		t.Fatalf("deadline refresh result=%+v revision=%d", result, reconciler.LastFullyReconciledRevision())
	}
	installed := reconciler.currentInstalledMembership()
	if len(installed) != 1 || installed[0].Generation.LeaseUntilMs != extended.Player.Routes[0].LeaseUntilMs {
		t.Fatalf("deadline bookkeeping did not advance: %+v", installed)
	}
	ports.mu.Lock()
	installs, fences, removes := len(ports.installs), len(ports.fences), len(ports.removes)
	ports.mu.Unlock()
	if installs != 1 || fences != 0 || removes != 0 {
		t.Fatalf("deadline refresh replayed membership side effects: installs=%d fences=%d removes=%d", installs, fences, removes)
	}
}

func TestReconcilerGenerationReplacementFencesBeforeInstall(t *testing.T) {
	ports := newStubPorts()
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: "holder-2", HolderBootID: "boot-2",
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 3, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	reconciler.Observe(membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	}))
	// Same shard, new writer epoch -> REPLACE.
	result := reconciler.Observe(membershipTestSnapshot(2, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 2, 2, 2),
	}))
	if len(result.Actions) != 1 || result.Actions[0].Type != MembershipActionReplace {
		t.Fatalf("expected REPLACE, got %+v", result.Actions)
	}
	if !result.FullyReconciled || reconciler.LastFullyReconciledRevision() != 2 {
		t.Fatalf("replacement did not reconcile: result=%+v revision=%d", result, reconciler.LastFullyReconciledRevision())
	}
	ports.mu.Lock()
	order := append([]string(nil), ports.order...)
	fences := append([]ShardMembership(nil), ports.fences...)
	installs := append([]ShardMembership(nil), ports.installs...)
	ports.mu.Unlock()
	if len(fences) != 1 || len(installs) != 2 {
		t.Fatalf("fences=%d installs=%d, want 1/2", len(fences), len(installs))
	}
	// order: [prepare(rev1), install(rev1), fence, prepare(rev2), install(rev2)]
	if order[len(order)-3] != "fence" || order[len(order)-2] != "prepare" || order[len(order)-1] != "install" {
		t.Fatalf("fence must precede prepare/install, order=%v", order)
	}
	if fences[0].Generation.AssignmentRevision != 1 || fences[0].Generation.WriterEpoch != 1 || fences[0].Generation.HolderBootID != "boot-2" {
		t.Fatalf("fence must target the old generation, got %+v", fences[0])
	}
}

// TestNewGenerationGetsNewActionToken proves that replanning under a newer
// generation issues a strictly increasing ActionToken, so an in-flight Port
// call for the previous plan can never commit into the new plan.
func TestNewGenerationGetsNewActionToken(t *testing.T) {
	ports := newStubPorts()
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: "holder-2", HolderBootID: "boot-2",
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 3, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()
	key := ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0}

	reconciler.Observe(membershipTestSnapshot(1, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 1, 1, 1),
	}))
	reconciler.mu.Lock()
	first := reconciler.tokenSeq
	reconciler.mu.Unlock()
	if first == 0 {
		t.Fatal("first plan must issue an action token")
	}

	// Force the replacement install to fail so the REPLACE action stays
	// pending and its token can be inspected.
	ports.installErr = errors.New("injected install failure")
	reconciler.Observe(membershipTestSnapshot(2, []actorrouting.ShardRoute{
		membershipTestActiveRoute(1, 2, 2, 2),
	}))
	reconciler.mu.Lock()
	second := reconciler.tokenSeq
	planned := reconciler.pending[key].Token
	reconciler.mu.Unlock()
	if second <= first {
		t.Fatalf("tokens must strictly increase: first=%d second=%d", first, second)
	}
	if planned != second {
		t.Fatalf("pending action must carry the newest token, got %d want %d", planned, second)
	}
}

// TestRuntimeMembershipReferenceCannotBeReplaced proves the composition
// Runtime exposes the membership reconciler through a stable reference: it can
// be injected exactly once and read through a thread-safe getter, but the
// pointer can never be swapped at runtime.
func TestRuntimeMembershipReferenceCannotBeReplaced(t *testing.T) {
	runtime := &Runtime{}
	if runtime.MembershipReconciler() != nil {
		t.Fatal("unset runtime must return a nil membership reconciler")
	}
	first := &ShardMembershipReconciler{}
	if err := runtime.SetMembershipReconciler(first); err != nil {
		t.Fatal(err)
	}
	if runtime.MembershipReconciler() != first {
		t.Fatal("getter must return the injected reconciler")
	}
	replacement := &ShardMembershipReconciler{}
	if err := runtime.SetMembershipReconciler(replacement); err == nil {
		t.Fatal("replacement of the membership reconciler must be rejected")
	}
	if runtime.MembershipReconciler() != first {
		t.Fatal("membership pointer was replaced")
	}
	if err := runtime.SetMembershipReconciler(nil); err == nil {
		t.Fatal("nil injection must be rejected")
	}
}
