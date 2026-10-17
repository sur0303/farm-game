package holdercontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/holderidentity"
	"farmgame/internal/holderkey"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingAuthorizer struct {
	mu      sync.Mutex
	allow   bool
	methods []string
}

func (a *recordingAuthorizer) Authorize(_ context.Context, method string) error {
	a.mu.Lock()
	a.methods = append(a.methods, method)
	allow := a.allow
	a.mu.Unlock()
	if !allow {
		return errors.New("test authorizer denied")
	}
	return nil
}

type recordingRuntime struct {
	mu sync.Mutex

	statuses     map[uint32]actorruntime.ShardDrainStatus
	stopCalls    []uint32
	waitCalls    []uint32
	factsCalls   []uint32
	rearmCalls   []uint32
	prepared     map[uint32]AuthorityGeneration
	prepareCalls []struct {
		shard uint32
		gen   AuthorityGeneration
	}
}

func (r *recordingRuntime) StopNewActivationAndTurns(_ context.Context, shard uint32) error {
	r.mu.Lock()
	r.ensureStatus(shard)
	r.stopCalls = append(r.stopCalls, shard)
	status := r.statuses[shard]
	status.Phase = actorruntime.ShardDrainPhaseDraining
	r.statuses[shard] = status
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) WaitCurrentTurns(_ context.Context, shard uint32) error {
	r.mu.Lock()
	r.ensureStatus(shard)
	r.waitCalls = append(r.waitCalls, shard)
	status := r.statuses[shard]
	status.WaitComplete = true
	status.AcceptedJobs = 0
	r.statuses[shard] = status
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) ConfirmFactsCommitted(_ context.Context, shard uint32) error {
	r.mu.Lock()
	r.ensureStatus(shard)
	r.factsCalls = append(r.factsCalls, shard)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) RearmShard(_ context.Context, shard uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStatus(shard)
	status := r.statuses[shard]
	if status.Phase != actorruntime.ShardDrainPhaseDraining ||
		status.AcceptedJobs != 0 ||
		!status.WaitComplete {
		return actorruntime.ErrShardDrainNotComplete
	}
	r.rearmCalls = append(r.rearmCalls, shard)
	r.statuses[shard] = actorruntime.ShardDrainStatus{
		Phase: actorruntime.ShardDrainPhaseOpen,
	}
	return nil
}

func (r *recordingRuntime) PrepareShardGeneration(_ context.Context, shard uint32, generation AuthorityGeneration) error {
	r.mu.Lock()
	if r.prepared == nil {
		r.prepared = make(map[uint32]AuthorityGeneration)
	}
	r.prepared[shard] = generation
	r.prepareCalls = append(r.prepareCalls, struct {
		shard uint32
		gen   AuthorityGeneration
	}{shard: shard, gen: generation})
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) PreparedShardGeneration(_ context.Context, shard uint32) (PreparedGenerationStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	generation, ok := r.prepared[shard]
	if !ok {
		return PreparedGenerationStatus{}, nil
	}
	copyGeneration := generation
	return PreparedGenerationStatus{Prepared: true, Generation: &copyGeneration}, nil
}

func (r *recordingRuntime) GetShardStatus(_ context.Context, shard uint32) (actorruntime.ShardDrainStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureStatus(shard)
	return r.statuses[shard], nil
}

func (r *recordingRuntime) ensureStatus(shard uint32) {
	if r.statuses == nil {
		r.statuses = make(map[uint32]actorruntime.ShardDrainStatus)
	}
}

func serviceTestGeneration() AuthorityGeneration {
	return AuthorityGeneration{
		HolderID:           "holder-a",
		HolderBootID:       "boot-a",
		AssignmentRevision: 4,
		WriterEpoch:        7,
		LeaseEpoch:         8,
		LeaseUntilMs:       serviceTestNow().Add(time.Minute).UnixMilli(),
	}
}

func serviceTestNow() time.Time {
	return time.UnixMilli(1_800_000_000_000)
}

func newPlayerLocalService(t *testing.T, auth *recordingAuthorizer, rt *recordingRuntime) *LocalService {
	t.Helper()
	service, err := NewLocalService(LocalServiceOptions{
		Authorizer:    auth,
		LocalHolderID: "holder-a",
		LocalBootID:   "boot-a",
		Now:           serviceTestNow,
		Player: DomainRuntimeOptions{
			Runtime:    rt,
			Preparer:   rt,
			ShardCount: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestLocalServiceRequiresExactAuthAndPhaseOrder(t *testing.T) {
	auth := &recordingAuthorizer{}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)

	stopReq := &holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: "PLAYER", Shard: 0}
	if _, err := service.StopNewActivationAndTurns(context.Background(), stopReq); err == nil {
		t.Fatal("denied Stop unexpectedly succeeded")
	}
	if len(rt.stopCalls) != 0 {
		t.Fatal("denied RPC reached runtime")
	}

	auth.allow = true
	if _, err := service.WaitCurrentTurns(context.Background(), &holdercontrolpb.WaitCurrentTurnsRequest{Domain: "PLAYER", Shard: 0}); !errors.Is(err, ErrShardPhaseOrder) {
		t.Fatalf("Wait before Stop err=%v", err)
	}
	if _, err := service.ConfirmFactsCommitted(context.Background(), &holdercontrolpb.ConfirmFactsCommittedRequest{Domain: "PLAYER", Shard: 0}); !errors.Is(err, ErrShardPhaseOrder) {
		t.Fatalf("Facts before Stop/Wait err=%v", err)
	}
	if _, err := service.StopNewActivationAndTurns(context.Background(), stopReq); err != nil {
		t.Fatal(err)
	}
	if _, err := service.StopNewActivationAndTurns(context.Background(), stopReq); err != nil {
		t.Fatal(err)
	}
	if len(rt.stopCalls) != 1 {
		t.Fatalf("Stop was not idempotent: %v", rt.stopCalls)
	}
	if _, err := service.WaitCurrentTurns(context.Background(), &holdercontrolpb.WaitCurrentTurnsRequest{Domain: "PLAYER", Shard: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ConfirmFactsCommitted(context.Background(), &holdercontrolpb.ConfirmFactsCommittedRequest{Domain: "PLAYER", Shard: 0}); err != nil {
		t.Fatal(err)
	}
	status, err := service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 0})
	if err != nil {
		t.Fatal(err)
	}
	if status.GetStatus().GetPhase() != holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED {
		t.Fatalf("phase=%v", status.GetStatus().GetPhase())
	}
	if status.GetStatus().GetHolderId() != "holder-a" || status.GetStatus().GetHolderBootId() != "boot-a" {
		t.Fatalf("status identity=%q/%q", status.GetStatus().GetHolderId(), status.GetStatus().GetHolderBootId())
	}

	// Shard 1 has an independent phase and can be stopped without changing
	// shard 0's terminal facts phase.
	if _, err := service.StopNewActivationAndTurns(context.Background(), &holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: "PLAYER", Shard: 1}); err != nil {
		t.Fatal(err)
	}
	if len(rt.stopCalls) != 2 || rt.stopCalls[1] != 1 {
		t.Fatalf("other shard Stop calls=%v", rt.stopCalls)
	}
}

func TestLocalServiceRearmsCompletedSourceForNewTargetGeneration(t *testing.T) {
	auth := &recordingAuthorizer{allow: true}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	ctx := context.Background()
	if _, err := service.StopNewActivationAndTurns(
		ctx,
		&holdercontrolpb.StopNewActivationAndTurnsRequest{
			Domain: "PLAYER",
			Shard:  0,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.WaitCurrentTurns(
		ctx,
		&holdercontrolpb.WaitCurrentTurnsRequest{
			Domain: "PLAYER",
			Shard:  0,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ConfirmFactsCommitted(
		ctx,
		&holdercontrolpb.ConfirmFactsCommittedRequest{
			Domain: "PLAYER",
			Shard:  0,
		},
	); err != nil {
		t.Fatal(err)
	}

	response, err := service.PrepareShardGeneration(
		ctx,
		&holdercontrolpb.PrepareShardGenerationRequest{
			Domain:     "PLAYER",
			Shard:      0,
			Generation: encodeGeneration(serviceTestGeneration()),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	status := response.GetStatus()
	if status.GetPhase() != holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN ||
		!status.GetAdmitting() ||
		status.GetWaitComplete() ||
		!status.GetPrepared() {
		t.Fatalf("rearmed target status=%+v", status)
	}
	if len(rt.rearmCalls) != 1 || rt.rearmCalls[0] != 0 {
		t.Fatalf("rearm calls=%v", rt.rearmCalls)
	}
	if _, err := service.StopNewActivationAndTurns(
		ctx,
		&holdercontrolpb.StopNewActivationAndTurnsRequest{
			Domain: "PLAYER",
			Shard:  0,
		},
	); err != nil {
		t.Fatalf("second drain err=%v", err)
	}
	if len(rt.stopCalls) != 2 {
		t.Fatalf("second drain calls=%v", rt.stopCalls)
	}
}

func TestLocalServicePrepareExactReuseConflictAndBounds(t *testing.T) {
	auth := &recordingAuthorizer{allow: true}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	gen := serviceTestGeneration()
	req := &holdercontrolpb.PrepareShardGenerationRequest{
		Domain:     "PLAYER",
		Shard:      1,
		Generation: encodeGeneration(gen),
	}
	response, err := service.PrepareShardGeneration(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetStatus().GetPrepared() || len(rt.prepareCalls) != 1 {
		t.Fatalf("prepare response=%+v calls=%d", response.GetStatus(), len(rt.prepareCalls))
	}
	if _, err := service.PrepareShardGeneration(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(rt.prepareCalls) != 2 {
		t.Fatalf("exact Prepare replay was not delegated: %d", len(rt.prepareCalls))
	}
	nextGeneration := gen
	nextGeneration.WriterEpoch++
	next := &holdercontrolpb.PrepareShardGenerationRequest{
		Domain:     req.Domain,
		Shard:      req.Shard,
		Generation: encodeGeneration(nextGeneration),
	}
	if _, err := service.PrepareShardGeneration(context.Background(), next); err != nil {
		t.Fatalf("new generation was not delegated to preparer: %v", err)
	}
	if len(rt.prepareCalls) != 3 {
		t.Fatalf("new generation prepare calls=%d", len(rt.prepareCalls))
	}

	tooLongGeneration := encodeGeneration(gen)
	tooLongGeneration.HolderId = string(make([]byte, MaxHolderIDBytes+1))
	tooLong := &holdercontrolpb.PrepareShardGenerationRequest{
		Domain:     req.Domain,
		Shard:      req.Shard,
		Generation: tooLongGeneration,
	}
	if _, err := service.PrepareShardGeneration(context.Background(), tooLong); err == nil {
		t.Fatal("oversized holder id accepted")
	}
	if _, err := service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 2}); err == nil {
		t.Fatal("out-of-range shard accepted")
	}
}

func TestLocalServicePreparedStatusFollowsPreparerCommitAndExpiry(t *testing.T) {
	now := time.UnixMilli(1_000)
	inner := &fakeMembershipInstaller{}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{
		Inner:     inner,
		Retention: time.Second,
		Now:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	preparer := &DomainGenerationPreparer{
		Domain:    actorrouting.DomainPlayer,
		Installer: installer,
	}
	auth := &recordingAuthorizer{allow: true}
	rt := &recordingRuntime{}
	service, err := NewLocalService(LocalServiceOptions{
		Authorizer:    auth,
		LocalHolderID: "holder-a",
		LocalBootID:   "boot-a",
		Now:           func() time.Time { return now },
		Player: DomainRuntimeOptions{
			Runtime:    rt,
			Preparer:   preparer,
			ShardCount: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := serviceTestGeneration()
	generation.LeaseUntilMs = now.Add(time.Minute).UnixMilli()
	req := &holdercontrolpb.PrepareShardGenerationRequest{
		Domain:     "PLAYER",
		Shard:      0,
		Generation: encodeGeneration(generation),
	}
	response, err := service.PrepareShardGeneration(context.Background(), req)
	if err != nil || !response.GetStatus().GetPrepared() {
		t.Fatalf("prepare response=%+v err=%v", response.GetStatus(), err)
	}

	now = now.Add(2 * time.Second)
	status, err := service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{
		Domain: "PLAYER",
		Shard:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.GetStatus().GetPrepared() {
		t.Fatal("expired preparer entry remained Prepared in service status")
	}

	member := authoritycomposition.ShardMembership{
		Key: authoritycomposition.ShardKey{Domain: actorrouting.DomainPlayer, Shard: 0},
		Generation: authoritycomposition.MembershipGeneration{
			AssignmentRevision: generation.AssignmentRevision,
			WriterEpoch:        generation.WriterEpoch,
			LeaseEpoch:         generation.LeaseEpoch,
			HolderID:           generation.HolderID,
			HolderBootID:       generation.HolderBootID,
			LeaseUntilMs:       generation.LeaseUntilMs,
		},
	}
	if _, err := installer.PrepareMembership(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	prepared, err := installer.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.CommitPreparedMembership(context.Background(), *prepared); err != nil {
		t.Fatal(err)
	}
	status, err = service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{
		Domain: "PLAYER",
		Shard:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.GetStatus().GetPrepared() {
		t.Fatal("committed preparer entry remained Prepared in service status")
	}
}

func TestLocalServiceCallsInjectedAuthorizerForEveryMethod(t *testing.T) {
	auth := &recordingAuthorizer{}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	ctx := context.Background()
	gen := encodeGeneration(serviceTestGeneration())

	calls := []func() error{
		func() error {
			_, err := service.StopNewActivationAndTurns(ctx, &holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: "PLAYER", Shard: 0})
			return err
		},
		func() error {
			_, err := service.WaitCurrentTurns(ctx, &holdercontrolpb.WaitCurrentTurnsRequest{Domain: "PLAYER", Shard: 0})
			return err
		},
		func() error {
			_, err := service.ConfirmFactsCommitted(ctx, &holdercontrolpb.ConfirmFactsCommittedRequest{Domain: "PLAYER", Shard: 0})
			return err
		},
		func() error {
			_, err := service.PrepareShardGeneration(ctx, &holdercontrolpb.PrepareShardGenerationRequest{Domain: "PLAYER", Shard: 0, Generation: gen})
			return err
		},
		func() error {
			_, err := service.GetShardStatus(ctx, &holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 0})
			return err
		},
		func() error {
			_, err := service.GetRuntimeIdentity(ctx, &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"})
			return err
		},
		func() error {
			_, err := service.ConfirmZeroShard(ctx, &holdercontrolpb.ConfirmZeroShardRequest{Domain: "PLAYER", BootId: "boot-a"})
			return err
		},
	}
	for _, call := range calls {
		if err := call(); err == nil {
			t.Fatal("unauthorized method unexpectedly succeeded")
		}
	}
	auth.mu.Lock()
	defer auth.mu.Unlock()
	want := []string{
		holdercontrolpb.HolderControlService_StopNewActivationAndTurns_FullMethodName,
		holdercontrolpb.HolderControlService_WaitCurrentTurns_FullMethodName,
		holdercontrolpb.HolderControlService_ConfirmFactsCommitted_FullMethodName,
		holdercontrolpb.HolderControlService_PrepareShardGeneration_FullMethodName,
		holdercontrolpb.HolderControlService_GetShardStatus_FullMethodName,
		holdercontrolpb.HolderControlService_GetRuntimeIdentity_FullMethodName,
		holdercontrolpb.HolderControlService_ConfirmZeroShard_FullMethodName,
	}
	if len(auth.methods) != len(want) {
		t.Fatalf("auth methods=%v", auth.methods)
	}
	for i := range want {
		if auth.methods[i] != want[i] {
			t.Fatalf("auth method[%d]=%q want %q", i, auth.methods[i], want[i])
		}
	}
}

func TestLocalServiceRejectsUnconfiguredDomain(t *testing.T) {
	auth := &recordingAuthorizer{allow: true}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	if _, err := service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{Domain: "FARM", Shard: 0}); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("unconfigured domain err=%v", err)
	}
}

func TestLocalServiceMapsErrorsToExplicitGRPCStatus(t *testing.T) {
	auth := &recordingAuthorizer{}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	if _, err := service.StopNewActivationAndTurns(context.Background(), &holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: "PLAYER", Shard: 0}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("auth code=%v err=%v", status.Code(err), err)
	}
	auth.allow = true
	if _, err := service.WaitCurrentTurns(context.Background(), &holdercontrolpb.WaitCurrentTurnsRequest{Domain: "PLAYER", Shard: 0}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("phase code=%v err=%v", status.Code(err), err)
	}
	if _, err := service.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{Domain: "INVALID", Shard: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("input code=%v err=%v", status.Code(err), err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GetShardStatus(canceled, &holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 0}); status.Code(err) != codes.Canceled {
		t.Fatalf("context code=%v err=%v", status.Code(err), err)
	}
}

func newRuntimeIdentityTestService(t *testing.T, process ProcessStatus, tokenSource WorkloadIdentityTokenSource, publications ...holderkey.Publication) *LocalService {
	t.Helper()
	var publication holderkey.Publication
	if len(publications) > 1 {
		t.Fatal("at most one test publication")
	}
	if len(publications) == 1 {
		publication = publications[0]
	}
	service, err := NewLocalService(LocalServiceOptions{
		Authorizer:    &recordingAuthorizer{allow: true},
		LocalHolderID: "holder-a",
		LocalBootID:   "boot-a",
		Now:           serviceTestNow,
		DeploymentIdentity: holderidentity.DeploymentIdentity{
			PodUID: "pod-a", WorkloadUID: "workload-a", Ordinal: 2,
		},
		ProcessStatusSource: ProcessStatusSourceFunc(func(context.Context) (ProcessStatus, error) {
			return process, nil
		}),
		WorkloadIdentityTokenSource: tokenSource,
		CommitProofKey:              publication,
		Player:                      DomainRuntimeOptions{Runtime: &recordingRuntime{}, ShardCount: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestLocalServiceRuntimeIdentityAndConfirmZeroShard(t *testing.T) {
	token := "opaque-bound-token"
	service := newRuntimeIdentityTestService(t, ProcessStatus{ProcessReady: true, ServingShardCount: 0, SnapshotRevision: 42}, WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) { return token, nil }))
	identity, err := service.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"})
	if err != nil {
		t.Fatal(err)
	}
	if identity.GetDomain() != "PLAYER" || identity.GetHolderId() != "holder-a" || identity.GetBootId() != "boot-a" || identity.GetWorkloadUid() != "workload-a" || identity.GetPodUid() != "pod-a" || identity.GetOrdinal() != 2 || !identity.GetProcessReady() || identity.GetDraining() || identity.GetServingShardCount() != 0 || identity.GetObservedRouteRevision() != 42 || identity.GetLifecycleState() != "READY_ZERO_SHARD" || identity.GetObservedAtMs() != serviceTestNow().UnixMilli() || identity.GetWorkloadIdentityToken() != token {
		t.Fatalf("identity=%v", identity)
	}
	confirmed, err := service.ConfirmZeroShard(context.Background(), &holdercontrolpb.ConfirmZeroShardRequest{Domain: "PLAYER", BootId: "boot-a"})
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.GetHolderId() != identity.GetHolderId() || confirmed.GetPodUid() != identity.GetPodUid() || confirmed.GetBootId() != identity.GetBootId() || confirmed.GetServingShardCount() != 0 || confirmed.GetWorkloadIdentityToken() != token {
		t.Fatalf("confirmed=%v identity=%v", confirmed, identity)
	}
}

func TestLocalServiceRuntimeIdentityPublishesOnlyPublicBootKey(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := holderkey.New(public)
	if err != nil {
		t.Fatal(err)
	}
	service := newRuntimeIdentityTestService(t, ProcessStatus{ProcessReady: true},
		WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) { return "token", nil }), publication)
	identity, err := service.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"})
	if err != nil {
		t.Fatal(err)
	}
	if identity.GetCommitProofKeyId() != publication.KeyID || string(identity.GetCommitProofPublicKey()) != string(public) {
		t.Fatalf("identity key publication=%v", identity)
	}
	if string(identity.GetCommitProofPublicKey()) == string(private) || len(identity.GetCommitProofPublicKey()) != ed25519.PublicKeySize {
		t.Fatal("private key material entered HolderControl")
	}
	identity.CommitProofPublicKey[0] ^= 0xff
	second, err := service.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"})
	if err != nil || string(second.GetCommitProofPublicKey()) != string(public) {
		t.Fatalf("response mutation escaped service copy: identity=%v err=%v", second, err)
	}
}

func TestLocalServiceRuntimeIdentityKeepsReconcilingServingGenerationObservable(t *testing.T) {
	service := newRuntimeIdentityTestService(t, ProcessStatus{
		ProcessReady: false, ServingShardCount: 7, SnapshotRevision: 43,
	}, WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) { return "token", nil }))
	identity, err := service.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"})
	if err != nil {
		t.Fatal(err)
	}
	if identity.GetProcessReady() || identity.GetDraining() || identity.GetServingShardCount() != 7 ||
		identity.GetObservedRouteRevision() != 43 || identity.GetLifecycleState() != "SERVING" {
		t.Fatalf("identity=%v", identity)
	}
	if _, err := service.ConfirmZeroShard(context.Background(), &holdercontrolpb.ConfirmZeroShardRequest{
		Domain: "PLAYER", BootId: "boot-a",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reconciling serving generation confirmed as zero-shard: code=%v err=%v", status.Code(err), err)
	}
}

func TestLocalServiceRuntimeIdentityFailsClosedForTokenAndPreconditions(t *testing.T) {
	missing := newRuntimeIdentityTestService(t, ProcessStatus{ProcessReady: true}, nil)
	if _, err := missing.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("missing token code=%v err=%v", status.Code(err), err)
	}
	if _, err := missing.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 0}); err != nil {
		t.Fatalf("legacy RPC affected by missing token: %v", err)
	}
	oversized := newRuntimeIdentityTestService(t, ProcessStatus{ProcessReady: true}, WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) {
		return strings.Repeat("x", MaxWorkloadIdentityTokenBytes+1), nil
	}))
	if _, err := oversized.GetRuntimeIdentity(context.Background(), &holdercontrolpb.GetRuntimeIdentityRequest{Domain: "PLAYER"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("oversized token code=%v err=%v", status.Code(err), err)
	}
	for name, process := range map[string]ProcessStatus{
		"not ready": {ProcessReady: false},
		"serving":   {ProcessReady: true, ServingShardCount: 1},
	} {
		t.Run(name, func(t *testing.T) {
			service := newRuntimeIdentityTestService(t, process, WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) { return "token", nil }))
			if _, err := service.ConfirmZeroShard(context.Background(), &holdercontrolpb.ConfirmZeroShardRequest{Domain: "PLAYER", BootId: "boot-a"}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("code=%v err=%v", status.Code(err), err)
			}
		})
	}
	service := newRuntimeIdentityTestService(t, ProcessStatus{ProcessReady: true}, WorkloadIdentityTokenSourceFunc(func(context.Context) (string, error) { return "token", nil }))
	if _, err := service.ConfirmZeroShard(context.Background(), &holdercontrolpb.ConfirmZeroShardRequest{Domain: "PLAYER", BootId: "other-boot"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("boot code=%v err=%v", status.Code(err), err)
	}
}

func TestFileWorkloadIdentityTokenSourceEnforcesBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("file-token"), 0600); err != nil {
		t.Fatal(err)
	}
	source := FileWorkloadIdentityTokenSource{Path: path}
	got, err := source.Token(context.Background())
	if err != nil || got != "file-token" {
		t.Fatalf("token=%q err=%v", got, err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", MaxWorkloadIdentityTokenBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); !errors.Is(err, ErrWorkloadIdentityToken) {
		t.Fatalf("oversized file err=%v", err)
	}
}
func TestNewLocalServiceRequiresImmutableIdentityClockAndBoundedTimeout(t *testing.T) {
	base := LocalServiceOptions{
		Authorizer:    &recordingAuthorizer{allow: true},
		LocalHolderID: "holder-a",
		LocalBootID:   "boot-a",
		Now:           serviceTestNow,
		Player: DomainRuntimeOptions{
			Runtime:    &recordingRuntime{},
			ShardCount: 1,
		},
	}
	for name, mutate := range map[string]func(*LocalServiceOptions){
		"missing holder": func(opts *LocalServiceOptions) { opts.LocalHolderID = "" },
		"missing boot":   func(opts *LocalServiceOptions) { opts.LocalBootID = "" },
		"missing now":    func(opts *LocalServiceOptions) { opts.Now = nil },
		"short timeout":  func(opts *LocalServiceOptions) { opts.OperationTimeout = time.Microsecond },
		"long timeout":   func(opts *LocalServiceOptions) { opts.OperationTimeout = 31 * time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			opts := base
			mutate(&opts)
			if _, err := NewLocalService(opts); err == nil {
				t.Fatal("invalid options unexpectedly accepted")
			}
		})
	}
}

func TestLocalServiceRejectsForeignExpiredAndNonAdmittingPrepare(t *testing.T) {
	auth := &recordingAuthorizer{allow: true}
	rt := &recordingRuntime{}
	service := newPlayerLocalService(t, auth, rt)
	base := serviceTestGeneration()
	for name, mutate := range map[string]func(*AuthorityGeneration){
		"foreign": func(generation *AuthorityGeneration) { generation.HolderID = "foreign" },
		"expired": func(generation *AuthorityGeneration) { generation.LeaseUntilMs = serviceTestNow().UnixMilli() },
	} {
		t.Run(name, func(t *testing.T) {
			generation := base
			mutate(&generation)
			_, err := service.PrepareShardGeneration(context.Background(), &holdercontrolpb.PrepareShardGenerationRequest{
				Domain: "PLAYER", Shard: 1, Generation: encodeGeneration(generation),
			})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("code=%v err=%v", status.Code(err), err)
			}
		})
	}

	rt.mu.Lock()
	rt.ensureStatus(0)
	rt.statuses[0] = actorruntime.ShardDrainStatus{Phase: actorruntime.ShardDrainPhaseDraining}
	rt.mu.Unlock()
	_, err := service.PrepareShardGeneration(context.Background(), &holdercontrolpb.PrepareShardGenerationRequest{
		Domain: "PLAYER", Shard: 0, Generation: encodeGeneration(base),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("non-admitting code=%v err=%v", status.Code(err), err)
	}
	if len(rt.prepareCalls) != 0 {
		t.Fatalf("preparer called while gate was closed: %d", len(rt.prepareCalls))
	}
}

var _ ShardRuntime = (*recordingRuntime)(nil)
var _ ShardGenerationPreparer = (*recordingRuntime)(nil)
var _ = actorrouting.DomainPlayer
