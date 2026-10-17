package participantproof

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	proofTestSubject  uint64 = 77
	proofTestFarm     uint64 = 77
	proofTestPlot     uint32 = 3
	proofTestSeed     uint32 = 1001
	proofTestRuleVer  uint64 = 42
	proofTestBucket   uint32 = 19
	proofTestLeaseTTL        = time.Minute
)

var proofTestNow = time.UnixMilli(1_800_000_000_000)

type proofTestRepository struct {
	record *workflow.WorkRecord
	err    error
}

func (r *proofTestRepository) CreateIfAbsent(context.Context, *workflow.WorkRecord) (*workflow.WorkRecord, bool, error) {
	return nil, false, errors.New("unused CreateIfAbsent")
}

func (r *proofTestRepository) Get(context.Context, string, string) (*workflow.WorkRecord, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.record == nil {
		return nil, workflow.ErrNotFound
	}
	return r.record, nil
}

func (r *proofTestRepository) Advance(context.Context, *workflow.WorkRecord, workflow.AdvanceOptions) (*workflow.WorkRecord, error) {
	return nil, errors.New("unused Advance")
}

func (r *proofTestRepository) ClaimDue(context.Context, string, string, int, time.Duration) ([]*workflow.ClaimedWork, error) {
	return nil, errors.New("unused ClaimDue")
}

func (r *proofTestRepository) RenewLease(context.Context, string, string, string, string, uint64, time.Duration) (*workflow.WorkRecord, error) {
	return nil, errors.New("unused RenewLease")
}

func (r *proofTestRepository) PendingDue(context.Context) (int64, error) {
	return 0, errors.New("unused PendingDue")
}

type proofTestBucketResolver struct{}

func (proofTestBucketResolver) IndependentWorkBucket(string, uint32) (uint32, error) {
	return proofTestBucket, nil
}

func (proofTestBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return proofTestBucket, nil
}

type proofTestRouteVerifier struct {
	calls int
	err   error
}

func (v *proofTestRouteVerifier) VerifyRoute(context.Context, *contracts.ParticipantCall) error {
	v.calls++
	return v.err
}

func TestNewRequiresAllPhysicalProofDependencies(t *testing.T) {
	repository := &proofTestRepository{}
	resolver := proofTestBucketResolver{}
	route := &proofTestRouteVerifier{}
	tests := []struct {
		name    string
		options Options
		want    error
	}{
		{name: "repository", options: Options{BucketResolver: resolver, RouteFenceVerifier: route}, want: ErrRepositoryUnavailable},
		{name: "bucket resolver", options: Options{Repository: repository, RouteFenceVerifier: route}, want: ErrBucketResolverUnavailable},
		{name: "route fence", options: Options{Repository: repository, BucketResolver: resolver}, want: ErrRouteFenceUnavailable},
		{name: "authoritative clock", options: Options{Repository: repository, BucketResolver: resolver, RouteFenceVerifier: route}, want: ErrClockUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewWithOptions(test.options)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v, want %v", err, test.want)
			}
		})
	}
}

func TestVerifierValidCoordinatorAndWorkerProofs(t *testing.T) {
	record, calls := proofTestRecord(t, 0, workflow.StepPending, nil)
	repo := &proofTestRepository{record: record}
	route := &proofTestRouteVerifier{}
	verifier, err := NewWithOptions(Options{
		Repository:         repo,
		BucketResolver:     proofTestBucketResolver{},
		RouteFenceVerifier: route,
		Now:                func() time.Time { return proofTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := calls.coordinator(t)
	if err := verifier.VerifyRoute(context.Background(), coordinator); err != nil {
		t.Fatalf("route proof: %v", err)
	}
	if err := verifier.VerifyWorkAddress(context.Background(), coordinator); err != nil {
		t.Fatalf("address proof: %v", err)
	}
	if err := verifier.VerifyIdentityBinding(context.Background(), coordinator); err != nil {
		t.Fatalf("identity proof: %v", err)
	}
	if err := verifier.VerifyWorkVersion(context.Background(), coordinator); err != nil {
		t.Fatalf("version proof: %v", err)
	}
	if err := verifier.VerifyPlantDependency(context.Background(), coordinator); err != nil {
		t.Fatalf("Plant dependency proof: %v", err)
	}
	if route.calls != 1 {
		t.Fatalf("route verifier calls=%d, want 1", route.calls)
	}

	record.LeaseOwner = "worker-1"
	record.LeaseEpoch = 1
	record.LeaseTokenHash = proofLeaseHash("token-a")
	record.LeaseExpireAtMs = proofTestNow.Add(proofTestLeaseTTL).UnixMilli()
	worker := calls.worker(t)
	if err := verifier.VerifyWorkLease(context.Background(), worker); err != nil {
		t.Fatalf("lease proof: %v", err)
	}
	if err := verifier.VerifyIdentityBinding(context.Background(), worker); err != nil {
		t.Fatalf("worker identity proof: %v", err)
	}
}

func TestVerifierSkipsWorkRecordForPlantSourceOrder(t *testing.T) {
	verifier := mustProofVerifier(t, &proofTestRepository{})
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	call := &contracts.ParticipantCall{
		Spec: contracts.ParticipantCommandSpec{
			Command:  contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
			WorkType: identity.WorkTypePlant,
		},
		Work: &contractspb.WorkAddress{
			WorkId:        mustStableID(t, "plant-source-order"),
			WorkType:      contractspb.WorkType_WORK_TYPE_PLANT,
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
			BucketVersion: 1,
			BucketId:      proofTestBucket,
			SourceAuthority: &contractspb.SourceAuthorityRef{
				AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
				CanonicalAuthorityId:   fmt.Sprintf("%d", proofTestSubject),
				AuthorityLayoutVersion: "v1",
				ActorRouteDomain:       &domain,
			},
		},
		Payload:         &contractspb.ReserveSeedPayload{PlayerId: proofTestSubject},
		SubjectPlayerID: proofTestSubject,
		CallerKind:      contracts.ParticipantCallerCoordinator,
		OperationID:     "plant-source-op",
	}
	if err := verifier.VerifyWorkAddress(context.Background(), call); err != nil {
		t.Fatalf("plant source address proof: %v", err)
	}
	if err := verifier.VerifyIdentityBinding(context.Background(), call); err != nil {
		t.Fatalf("plant source identity proof: %v", err)
	}
}

func TestVerifierRejectsStaleForgedMissingAndWrongWork(t *testing.T) {
	record, calls := proofTestRecord(t, 0, workflow.StepPending, nil)
	verifier := mustProofVerifier(t, &proofTestRepository{record: record})

	stale := calls.coordinator(t)
	stale.ExpectedWorkVersion++
	if err := verifier.VerifyWorkVersion(context.Background(), stale); !errors.Is(err, ErrWorkVersionMismatch) {
		t.Fatalf("stale version err=%v", err)
	}

	forged := calls.coordinator(t)
	payload := proto.Clone(forged.Payload).(*contractspb.ReserveSeedPayload)
	payload.PlotId++
	forged.Payload = payload
	forged.CanonicalStepPayloadHash, _ = contracts.CanonicalParticipantHash(forged.Spec, payload)
	if err := verifier.VerifyWorkVersion(context.Background(), forged); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("forged payload err=%v", err)
	}

	missingRecord := *verifier
	missingRecord.repository = &proofTestRepository{err: workflow.ErrNotFound}
	if err := missingRecord.VerifyWorkAddress(context.Background(), calls.coordinator(t)); !errors.Is(err, ErrWorkNotFound) {
		t.Fatalf("missing work err=%v", err)
	}

	wrongWork := calls.coordinator(t)
	wrongWork.Work = proto.Clone(wrongWork.Work).(*contractspb.WorkAddress)
	wrongWork.Work.WorkId = mustStableID(t, "wrong-work")
	if err := verifier.VerifyWorkAddress(context.Background(), wrongWork); !errors.Is(err, ErrWorkAddressMismatch) {
		t.Fatalf("wrong work err=%v", err)
	}
}

func TestVerifierRejectsWrongSubjectAndOperation(t *testing.T) {
	record, calls := proofTestRecord(t, 0, workflow.StepPending, nil)
	verifier := mustProofVerifier(t, &proofTestRepository{record: record})

	wrongSubject := calls.coordinator(t)
	wrongSubject.SubjectPlayerID++
	if err := verifier.VerifyIdentityBinding(context.Background(), wrongSubject); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong subject err=%v", err)
	}

	wrongOperation := calls.coordinator(t)
	wrongOperation.OperationID = "forged-operation"
	if err := verifier.VerifyIdentityBinding(context.Background(), wrongOperation); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("wrong operation err=%v", err)
	}

	wrongPayloadSubject := calls.worker(t)
	payload := proto.Clone(wrongPayloadSubject.Payload).(*contractspb.ReserveSeedPayload)
	payload.PlayerId++
	wrongPayloadSubject.Payload = payload
	wrongPayloadSubject.CanonicalStepPayloadHash, _ = contracts.CanonicalParticipantHash(wrongPayloadSubject.Spec, payload)
	if err := verifier.VerifyIdentityBinding(context.Background(), wrongPayloadSubject); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("wrong worker payload subject err=%v", err)
	}
}

func TestIdentityProofAllowsExactHistoricalRetryAfterWorkAdvances(t *testing.T) {
	reserveResult := &contractspb.ReserveSeedResult{
		SeedItemId:       proofTestSeed,
		ReservedQuantity: plantQuantity,
	}
	record, calls := proofTestRecord(t, 1, workflow.StepApplied, reserveResult)
	record.WorkVersion = 2
	verifier := mustProofVerifier(t, &proofTestRepository{record: record})

	retry := calls.coordinator(t)
	retry.ExpectedWorkVersion = record.WorkVersion
	if err := verifier.VerifyIdentityBinding(context.Background(), retry); err != nil {
		t.Fatalf("exact historical retry identity proof: %v", err)
	}
	if err := verifier.VerifyWorkVersion(context.Background(), retry); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("historical retry must not pass first-execution current-step proof: %v", err)
	}

	forgedPayload := proto.Clone(retry.Payload).(*contractspb.ReserveSeedPayload)
	forgedPayload.PlotId++
	forged := *retry
	forged.Payload = forgedPayload
	forged.CanonicalStepPayloadHash, _ = contracts.CanonicalParticipantHash(forged.Spec, forgedPayload)
	if err := verifier.VerifyIdentityBinding(context.Background(), &forged); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("forged historical retry err=%v", err)
	}

	mismatched := *retry
	mismatched.InvocationID = mustStableID(t, "mismatched-retry-invocation")
	if err := verifier.VerifyIdentityBinding(context.Background(), &mismatched); !errors.Is(err, ErrStepMismatch) {
		t.Fatalf("mismatched historical retry err=%v", err)
	}
}

func TestPlantDependencyRequiresPersistedResultAndHash(t *testing.T) {
	_, seedCalls := proofTestRecord(t, 0, workflow.StepPending, nil)
	reserveResult := &contractspb.ReserveSeedResult{
		ReservationId:    seedCalls.reservation,
		SeedItemId:       proofTestSeed,
		ReservedQuantity: plantQuantity,
	}
	record, calls := proofTestRecord(t, 1, workflow.StepApplied, reserveResult)
	verifier := mustProofVerifier(t, &proofTestRepository{record: record})
	commit := calls.commitPlant(t)
	if err := verifier.VerifyCommitPlantDependency(context.Background(), commit); err != nil {
		t.Fatalf("valid COMMIT_PLANT dependency err=%v", err)
	}

	missing := *record
	missing.Steps = append([]workflow.StepRecord(nil), record.Steps...)
	missing.Steps[0].Result = nil
	missing.Steps[0].ResultHash = [32]byte{}
	verifier.repository = &proofTestRepository{record: &missing}
	if err := verifier.VerifyCommitPlantDependency(context.Background(), commit); !errors.Is(err, ErrDependencyMismatch) {
		t.Fatalf("missing receipt err=%v", err)
	}

	wrongHash := *record
	wrongHash.Steps = append([]workflow.StepRecord(nil), record.Steps...)
	wrongHash.Steps[0].ResultHash[0]++
	verifier.repository = &proofTestRepository{record: &wrongHash}
	if err := verifier.VerifyCommitPlantDependency(context.Background(), commit); !errors.Is(err, ErrDependencyMismatch) {
		t.Fatalf("wrong dependency hash err=%v", err)
	}
}

func TestWorkerLeaseTakeoverRejectsOldProof(t *testing.T) {
	record, calls := proofTestRecord(t, 0, workflow.StepPending, nil)
	record.LeaseOwner = "worker-a"
	record.LeaseEpoch = 1
	record.LeaseTokenHash = proofLeaseHash("token-a")
	record.LeaseExpireAtMs = proofTestNow.Add(proofTestLeaseTTL).UnixMilli()
	repo := &proofTestRepository{record: record}
	verifier := mustProofVerifier(t, repo)
	old := calls.worker(t)
	old.CallerInstanceID = "worker-a"
	if err := verifier.VerifyWorkLease(context.Background(), old); err != nil {
		t.Fatalf("old lease initially valid: %v", err)
	}

	forgedToken := calls.worker(t)
	forgedToken.CallerInstanceID = "worker-a"
	forgedToken.LeaseOwnerToken = "forged-token"
	if err := verifier.VerifyWorkLease(context.Background(), forgedToken); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("forged token err=%v", err)
	}

	record.LeaseExpireAtMs = proofTestNow.UnixMilli()
	if err := verifier.VerifyWorkLease(context.Background(), old); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired lease err=%v", err)
	}
	record.LeaseExpireAtMs = proofTestNow.Add(proofTestLeaseTTL).UnixMilli()

	record.LeaseOwner = "worker-b"
	record.LeaseEpoch = 2
	record.LeaseTokenHash = proofLeaseHash("token-b")
	old.LeaseOwnerToken = "token-b"
	if err := verifier.VerifyWorkLease(context.Background(), old); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("takeover owner/epoch err=%v", err)
	}

	newCall := calls.worker(t)
	newCall.CallerInstanceID = "worker-b"
	newCall.LeaseOwnerToken = "token-b"
	newCall.LeaseEpoch = 2
	if err := verifier.VerifyWorkLease(context.Background(), newCall); err != nil {
		t.Fatalf("new lease proof: %v", err)
	}
}

func TestWorkerLeaseRequiresAuthoritativeClockAndUsesItsBoundary(t *testing.T) {
	record, calls := proofTestRecord(t, 0, workflow.StepPending, nil)
	record.LeaseOwner = "worker-1"
	record.LeaseEpoch = 1
	record.LeaseTokenHash = proofLeaseHash("token-a")
	expiry := proofTestNow.Add(time.Minute)
	record.LeaseExpireAtMs = expiry.UnixMilli()
	repository := &proofTestRepository{record: record}
	base := Options{
		Repository:         repository,
		BucketResolver:     proofTestBucketResolver{},
		RouteFenceVerifier: &proofTestRouteVerifier{},
	}
	if _, err := NewWithOptions(base); !errors.Is(err, ErrClockUnavailable) {
		t.Fatalf("missing clock constructor err=%v", err)
	}

	zeroClock := base
	zeroClock.Now = func() time.Time { return time.Time{} }
	zeroVerifier, err := NewWithOptions(zeroClock)
	if err != nil {
		t.Fatal(err)
	}
	if err := zeroVerifier.VerifyWorkLease(context.Background(), calls.worker(t)); !errors.Is(err, ErrClockUnavailable) {
		t.Fatalf("invalid clock err=%v", err)
	}

	verifier := mustProofVerifier(t, repository)
	verifier.now = func() time.Time { return expiry.Add(-time.Millisecond) }
	if err := verifier.VerifyWorkLease(context.Background(), calls.worker(t)); err != nil {
		t.Fatalf("clock-before-expiry lease proof: %v", err)
	}
	verifier.now = func() time.Time { return expiry }
	if err := verifier.VerifyWorkLease(context.Background(), calls.worker(t)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("clock-at-expiry err=%v", err)
	}
	verifier.now = func() time.Time { return expiry.Add(time.Millisecond) }
	if err := verifier.VerifyWorkLease(context.Background(), calls.worker(t)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("clock-after-expiry err=%v", err)
	}
	verifier.now = nil
	if err := verifier.VerifyWorkLease(context.Background(), calls.worker(t)); !errors.Is(err, ErrClockUnavailable) {
		t.Fatalf("removed clock err=%v", err)
	}
}

func TestSourceWorkAddressLayoutVersionFailsClosed(t *testing.T) {
	record, address := sourceProofRecord(t)
	verifier := mustProofVerifier(t, &proofTestRepository{record: record})
	for _, layout := range []string{"layout-v1", "different-layout-v2"} {
		t.Run(layout, func(t *testing.T) {
			request := proto.Clone(address).(*contractspb.WorkAddress)
			request.SourceAuthority.AuthorityLayoutVersion = layout
			err := verifier.VerifyWorkAddress(context.Background(), &contracts.ParticipantCall{Work: request})
			if !errors.Is(err, ErrSourceLayoutUnproven) || !errors.Is(err, ErrWorkAddressMismatch) {
				t.Fatalf("layout %q err=%v", layout, err)
			}
		})
	}
}

func mustProofVerifier(t *testing.T, repository workflow.WorkRepository) *Verifier {
	t.Helper()
	verifier, err := NewWithOptions(Options{
		Repository:         repository,
		BucketResolver:     proofTestBucketResolver{},
		RouteFenceVerifier: &proofTestRouteVerifier{},
		Now:                func() time.Time { return proofTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

type proofTestCalls struct {
	record      *workflow.WorkRecord
	address     *contractspb.WorkAddress
	reserve     *contractspb.ReserveSeedPayload
	reservation string
	operation   string
}

func proofTestRecord(t *testing.T, current int, reserveStatus workflow.StepStatus, reserveResult *contractspb.ReserveSeedResult) (*workflow.WorkRecord, proofTestCalls) {
	t.Helper()
	workID := mustStableID(t, "plant-proof")
	operation := "plant-proof-op"
	reservation, err := workidentity.SeedReservationID(workID)
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := workidentity.CropCycleID(workID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &contractspb.CropRuleSnapshot{CropDefinitionId: 1, CropRuleVersion: proofTestRuleVer, SeedItemId: proofTestSeed}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	root := &contractspb.CommitPlantPayload{
		OwnerPlayerId: proofTestSubject, FarmId: proofTestFarm, PlotId: proofTestPlot, ExpectedPlotVersion: 1,
		ReservationId: reservation, CropCycleId: cycle, CropRuleSnapshot: snapshot, CropRuleSnapshotHash: snapshotHash[:],
	}
	rootBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	rootHash, err := contracts.CanonicalHash(root)
	if err != nil {
		t.Fatal(err)
	}
	address := workidentity.WorkAddress{WorkID: workID, WorkType: "PLANT", PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: proofTestBucket}
	record, err := workflow.NewWorkRecord(address, proofTestSubject, operation, rootBytes, rootHash, []string{"RESERVE_SEED", "COMMIT_PLANT", "COMMIT_SEED_RESERVATION", "RELEASE_SEED_RESERVATION"}, proofTestNow, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	record.WorkVersion = 1
	record.CurrentStep = current
	if current > 0 {
		record.State = workflow.WorkStateRunning
	}
	reserve := &contractspb.ReserveSeedPayload{
		PlayerId: proofTestSubject, FarmId: proofTestFarm, PlotId: proofTestPlot, ReservationId: reservation,
		CropDefinitionId: 1, SeedItemId: proofTestSeed, Quantity: plantQuantity,
		CropRuleVersion: proofTestRuleVer, CropRuleSnapshotHash: snapshotHash[:],
	}
	setStepPayload(t, &record.Steps[0], reserve)
	if reserveResult != nil {
		if reserveResult.ReservationId == "" {
			reserveResult.ReservationId = reservation
		}
		resultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(reserveResult)
		if err != nil {
			t.Fatal(err)
		}
		resultHash, err := contracts.CanonicalParticipantHash(mustSpec(t, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED), reserveResult)
		if err != nil {
			t.Fatal(err)
		}
		record.Steps[0].Status = reserveStatus
		record.Steps[0].Result = resultBytes
		record.Steps[0].ResultHash = resultHash
	}
	if current == 1 {
		resultHash := record.Steps[0].ResultHash
		commit := proto.Clone(root).(*contractspb.CommitPlantPayload)
		commit.ReserveSeedResultHash = append([]byte(nil), resultHash[:]...)
		setStepPayload(t, &record.Steps[1], commit)
	}
	return record, proofTestCalls{
		record: record, address: wireAddress(t, address), reserve: reserve,
		reservation: reservation, operation: operation,
	}
}

func setStepPayload(t *testing.T, step *workflow.StepRecord, payload proto.Message) {
	t.Helper()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	step.Payload = raw
	step.PayloadHash = sha256.Sum256(raw)
}

func (c proofTestCalls) coordinator(t *testing.T) *contracts.ParticipantCall {
	t.Helper()
	spec := mustSpec(t, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	hash, err := contracts.CanonicalParticipantHash(spec, c.reserve)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: proofTestSubject},
		Work: c.address, CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step),
		InvocationID: c.record.Steps[0].InvocationID, OperationID: c.operation, SubjectPlayerID: proofTestSubject,
		ExpectedWorkVersion: c.record.WorkVersion, Payload: c.reserve, CanonicalStepPayloadHash: hash,
	}
}

func (c proofTestCalls) worker(t *testing.T) *contracts.ParticipantCall {
	call := c.coordinator(t)
	call.CallerKind = contracts.ParticipantCallerWorkerRecovery
	call.OperationID = ""
	call.SubjectPlayerID = 0
	call.CallerInstanceID = "worker-1"
	call.CallerBootID = "worker-boot-1"
	call.LeaseOwnerToken = "token-a"
	call.LeaseEpoch = 1
	return call
}

func (c proofTestCalls) commitPlant(t *testing.T) *contracts.ParticipantCall {
	t.Helper()
	spec := mustSpec(t, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	step := &c.record.Steps[1]
	payload := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(step.Payload, payload); err != nil {
		t.Fatal(err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: proofTestFarm},
		Work: c.address, CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step),
		InvocationID: step.InvocationID, OperationID: c.operation, SubjectPlayerID: proofTestSubject,
		ExpectedWorkVersion: c.record.WorkVersion, Payload: payload, CanonicalStepPayloadHash: hash,
	}
}

func mustSpec(t *testing.T, command contractspb.ParticipantCommandType) contracts.ParticipantCommandSpec {
	t.Helper()
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func wireAddress(t *testing.T, address workidentity.WorkAddress) *contractspb.WorkAddress {
	t.Helper()
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok {
		t.Fatalf("work type %q is not registered", address.WorkType)
	}
	return &contractspb.WorkAddress{
		WorkId: address.WorkID, WorkType: contractspb.WorkType(workType),
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		BucketVersion: address.BucketVersion, BucketId: address.BucketID,
	}
}

func mustStableID(t *testing.T, suffix string) string {
	t.Helper()
	id, err := workidentity.StableOpaqueID("participant-proof-test", suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func proofLeaseHash(token string) string {
	digest := sha256.Sum256([]byte("wf-lease-token\x00" + token))
	return fmt.Sprintf("%x", digest[:])
}

func sourceProofRecord(t *testing.T) (*workflow.WorkRecord, *contractspb.WorkAddress) {
	t.Helper()
	workID := mustStableID(t, "source-layout-proof")
	payload := []byte("source-proof-root")
	payloadHash := sha256.Sum256(payload)
	address := workidentity.WorkAddress{
		WorkID:            workID,
		WorkType:          "GUARD_SYNC_ORDER",
		PlacementKind:     workidentity.SourcePersistenceBucket,
		BucketVersion:     1,
		BucketID:          proofTestBucket,
		SourceRouteDomain: workidentity.RouteDomainPlayer,
		SourceActorID:     proofTestSubject,
	}
	record, err := workflow.NewWorkRecord(address, proofTestSubject, "source-layout-operation", payload, payloadHash,
		[]string{"APPLY_GUARD_SNAPSHOT"}, proofTestNow, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok {
		t.Fatalf("work type %q is not registered", address.WorkType)
	}
	routeDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	return record, &contractspb.WorkAddress{
		WorkId:        address.WorkID,
		WorkType:      contractspb.WorkType(workType),
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: address.BucketVersion,
		BucketId:      address.BucketID,
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
			CanonicalAuthorityId:   fmt.Sprintf("%d", proofTestSubject),
			AuthorityLayoutVersion: "layout-v1",
			ActorRouteDomain:       &routeDomain,
		},
	}
}
