package plantrecovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/scenario/plant"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPlantRecoveryRestartAndTakeoverReusesExactInvocation(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := testRoot(t)
	record, err := workflow.NewWorkRecord(testPlantAddress(root.WorkflowID), 10001, "op-root-for-worker-test",
		root.PayloadBytes, root.PayloadHash, plantStepNames(), now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	player := &workerSpy{loseFirst: true, response: reserveResponse}
	farm := &workerSpy{err: errors.New("farm participant unavailable")}
	resolver := testRouteResolver{}
	first := newTestComposition(t, repo, resolver, player, farm, "worker-a", "token-a", "boot-a", clock, time.Second, time.Second)
	stats, err := first.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Plant.Claimed != 1 || stats.Plant.Retried != 1 || stats.Plant.Completed != 0 {
		t.Fatalf("first stats=%+v", stats.Plant)
	}

	stored, err := repo.Get(context.Background(), FamilyPlant, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Steps[0].Status != workflow.StepUnknown || stored.Steps[0].PayloadHash == ([32]byte{}) {
		t.Fatalf("response-loss boundary was not durable: %+v", stored.Steps[0])
	}
	firstCall := player.callsCopy()[0]

	// The first worker released the record for durable backoff. Move the
	// repository clock beyond that due score, then let a new owner claim it.
	now = time.UnixMilli(stored.NextDueAtMs + 1)
	second := newTestComposition(t, repo, resolver, player, farm, "worker-b", "token-b", "boot-b", clock, time.Second, time.Second)
	stats, err = second.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Plant.Claimed != 1 || stats.Plant.Retried != 1 {
		t.Fatalf("takeover stats=%+v", stats.Plant)
	}

	calls := player.callsCopy()
	if len(calls) != 2 {
		t.Fatalf("player ResumeStep calls=%d, want 2", len(calls))
	}
	secondCall := calls[1]
	if firstCall.GetRecovery().GetParticipantInvocationId() != secondCall.GetRecovery().GetParticipantInvocationId() ||
		!bytesEqual(firstCall.GetTypedPayload(), secondCall.GetTypedPayload()) {
		t.Fatalf("takeover changed durable invocation/payload: first=%v second=%v", firstCall.GetRecovery(), secondCall.GetRecovery())
	}
	if firstCall.GetRecovery().GetCallerInstanceId() != "worker-a" || firstCall.GetRecovery().GetCallerBootId() != "boot-a" ||
		firstCall.GetRecovery().GetLeaseOwnerToken() != "token-a" || firstCall.GetRecovery().GetLeaseEpoch() == 0 {
		t.Fatalf("first lease identity=%v", firstCall.GetRecovery())
	}
	if secondCall.GetRecovery().GetCallerInstanceId() != "worker-b" || secondCall.GetRecovery().GetCallerBootId() != "boot-b" ||
		secondCall.GetRecovery().GetLeaseOwnerToken() != "token-b" || secondCall.GetRecovery().GetLeaseEpoch() <= firstCall.GetRecovery().GetLeaseEpoch() {
		t.Fatalf("takeover lease identity=%v first_epoch=%d", secondCall.GetRecovery(), firstCall.GetRecovery().GetLeaseEpoch())
	}
	if len(farm.callsCopy()) != 1 {
		t.Fatalf("farm calls=%d, want one post-replay attempt", len(farm.callsCopy()))
	}
}

func TestPlantRecoveryStaleWorkProofReloadsSameInvocation(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := testRoot(t)
	record, err := workflow.NewWorkRecord(testPlantAddress(root.WorkflowID), 10001, "op-root-for-worker-test",
		root.PayloadBytes, root.PayloadHash, plantStepNames(), now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	// The participant verifier rejects the first ResumeStep with a WorkVersion
	// proof mismatch (another driver advanced the record). The RPC adapter
	// surfaces workflow.ErrStaleWorkProof, the engine reloads the authoritative
	// record and re-invokes the SAME invocation; it must never classify the
	// proof race as StepUnknown/REJECTED or double-apply the receipt.
	player := &workerSpy{staleOnce: true, response: reserveResponse}
	farm := &workerSpy{err: errors.New("farm participant unavailable")}
	composition := newTestComposition(t, repo, testRouteResolver{}, player, farm,
		"worker-stale", "token-stale", "boot-stale", clock, time.Second, time.Second)
	stats, err := composition.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Plant.Claimed != 1 || stats.Plant.Completed != 0 {
		t.Fatalf("stats=%+v", stats.Plant)
	}

	calls := player.callsCopy()
	if len(calls) != 2 {
		t.Fatalf("player ResumeStep calls=%d, want 1 stale + 1 reload success", len(calls))
	}
	if calls[0].GetRecovery().GetParticipantInvocationId() != calls[1].GetRecovery().GetParticipantInvocationId() {
		t.Fatalf("stale-proof reload changed invocation: %s -> %s",
			calls[0].GetRecovery().GetParticipantInvocationId(), calls[1].GetRecovery().GetParticipantInvocationId())
	}
	if !bytesEqual(calls[0].GetTypedPayload(), calls[1].GetTypedPayload()) {
		t.Fatal("stale-proof reload changed payload")
	}
	stored, err := repo.Get(context.Background(), FamilyPlant, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Steps[0].Status != workflow.StepApplied {
		t.Fatalf("RESERVE_SEED after stale reload status=%s, want StepApplied (not StepUnknown/REJECTED)", stored.Steps[0].Status)
	}
	// Farm step is a separate generic RPC failure: it must still pause as
	// StepUnknown (retry scheduled), never misclassified as stale.
	if stored.Steps[1].Status != workflow.StepUnknown || stats.Plant.Retried != 1 {
		t.Fatalf("farm failure step=%s stats=%+v", stored.Steps[1].Status, stats.Plant)
	}
	if len(farm.callsCopy()) != 1 {
		t.Fatalf("farm calls=%d, want 1", len(farm.callsCopy()))
	}
}

func TestPlantRecoveryStopsStartingStepsAfterConservativeLeaseDeadline(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := testRoot(t)
	record, err := workflow.NewWorkRecord(testPlantAddress(root.WorkflowID), 10001, "op-root-for-worker-test",
		root.PayloadBytes, root.PayloadHash, plantStepNames(), now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	player := &workerSpy{delay: 60 * time.Millisecond, response: reserveResponse}
	farm := &workerSpy{err: errors.New("farm must not start after lease deadline")}
	composition := newTestComposition(t, repo, testRouteResolver{}, player, farm, "worker-deadline", "token-deadline", "boot-deadline", clock, 50*time.Millisecond, time.Second)
	stats, err := composition.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Plant.Retried != 1 {
		t.Fatalf("deadline stats=%+v", stats.Plant)
	}
	if len(farm.callsCopy()) != 0 {
		t.Fatalf("new Farm step started after conservative lease deadline: %d calls", len(farm.callsCopy()))
	}
}

func TestPlantRecoveryBatchDoesNotStartLateItemAfterClaimDeadline(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	firstRoot := testRootOperation(t, "op-batch-first")
	secondRoot := testRootOperation(t, "op-batch-second")
	first, err := workflow.NewWorkRecord(testPlantAddress(firstRoot.WorkflowID), 10001, "op-batch-first",
		firstRoot.PayloadBytes, firstRoot.PayloadHash, plantStepNames(), now.Add(-2*time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second, err := workflow.NewWorkRecord(testPlantAddress(secondRoot.WorkflowID), 10001, "op-batch-second",
		secondRoot.PayloadBytes, secondRoot.PayloadHash, plantStepNames(), now.Add(-time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	player := &workerSpy{
		delay:    10 * time.Millisecond,
		response: reserveResponse,
		onCall: func(request *contractspb.ParticipantRequest) {
			if request.GetRecovery().GetWork().GetWorkId() != firstRoot.WorkflowID {
				t.Fatalf("delayed callback ran for work=%s, want first=%s", request.GetRecovery().GetWork().GetWorkId(), firstRoot.WorkflowID)
			}
			// The first claimed item consumes the available processing window.
			// The second item remains in the same bounded batch, so its handler
			// must consult its original claim-start deadline rather than create
			// a fresh deadline when processing begins.
			now = now.Add(2 * time.Second)
		},
	}
	farm := &workerSpy{err: errors.New("late Farm step must not start")}
	composition := newTestCompositionWithLimit(t, repo, testRouteResolver{}, player, farm,
		"worker-batch", "token-batch", "boot-batch", clock, time.Second, time.Second, 2)
	stats, err := composition.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Plant.Claimed != 2 || stats.Plant.LeaseLost != 2 {
		t.Fatalf("batch stats=%+v, want two claims and no late processing", stats.Plant)
	}
	calls := player.callsCopy()
	if len(calls) != 1 {
		t.Fatalf("player ResumeStep calls=%d, want only the earlier item", len(calls))
	}
	if calls[0].GetRecovery().GetWork().GetWorkId() != firstRoot.WorkflowID {
		t.Fatalf("executed work=%s, want earlier work=%s", calls[0].GetRecovery().GetWork().GetWorkId(), firstRoot.WorkflowID)
	}
	if len(farm.callsCopy()) != 0 {
		t.Fatalf("Farm calls=%d, want no late Plant step", len(farm.callsCopy()))
	}
}

func TestRecoveryMetricsUseOnlyBoundedFamilies(t *testing.T) {
	metrics := NewMetrics(prometheusTestRegistry())
	metrics.Observe(RoundStats{
		Plant:  workflow.RecoveryStats{Claimed: 2, Completed: 1, Retried: 1, LeaseLost: 0},
		Unlock: workflow.RecoveryStats{Claimed: 3, Completed: 2, Retried: 0, LeaseLost: 1},
	})
	if got := testutil.ToFloat64(metrics.Claimed.WithLabelValues(FamilyPlant)); got != 2 {
		t.Fatalf("Plant claimed=%v", got)
	}
	if got := testutil.ToFloat64(metrics.Completed.WithLabelValues(FamilyUnlockPlot)); got != 2 {
		t.Fatalf("Unlock completed=%v", got)
	}
}

func newTestComposition(t *testing.T, repo workflow.WorkRepository, resolver actorrouting.Resolver, player, farm *workerSpy,
	owner, token, bootID string, now func() time.Time, leaseTTL, operationTimeout time.Duration) *Composition {
	return newTestCompositionWithLimit(t, repo, resolver, player, farm, owner, token, bootID, now, leaseTTL, operationTimeout, 1)
}

func newTestCompositionWithLimit(t *testing.T, repo workflow.WorkRepository, resolver actorrouting.Resolver, player, farm *workerSpy,
	owner, token, bootID string, now func() time.Time, leaseTTL, operationTimeout time.Duration, claimLimit int) *Composition {
	t.Helper()
	composition, err := New(Config{
		Repository:          repo,
		RouteResolver:       resolver,
		Realm:               "test-realm",
		PlayerClients:       map[string]contractspb.WorkerRecoveryIngressClient{"player": player},
		FarmClients:         map[string]contractspb.WorkerRecoveryIngressClient{"farm": farm},
		Owner:               owner,
		Token:               token,
		BootID:              bootID,
		ClaimLimit:          claimLimit,
		LeaseTTL:            leaseTTL,
		SafetyMargin:        leaseTTL / 5,
		OperationTimeout:    operationTimeout,
		ParticipantBudgetMs: 1000,
		Now:                 now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return composition
}

func testRoot(t *testing.T) plant.RootIntent {
	return testRootOperation(t, "op-root-for-worker-test")
}

func testRootOperation(t *testing.T, operationID string) plant.RootIntent {
	t.Helper()
	root, err := plant.BuildRoot(plant.PlantInput{
		SubjectPlayerID:         10001,
		OperationID:             operationID,
		TargetFarmID:            10001,
		PlotID:                  3,
		ExpectedPlotVersion:     7,
		CropDefinitionID:        7,
		ExpectedCropRuleVersion: 42,
		SeedItemID:              701,
		CropRuleSnapshot: &contractspb.CropRuleSnapshot{
			SchemaVersion:     1,
			CropDefinitionId:  7,
			CropRuleVersion:   42,
			SeedItemId:        701,
			HarvestItemId:     702,
			GrowDurationMs:    60_000,
			BaseYield:         50,
			CompendiumRule:    &contractspb.CompendiumUnlockRuleSnapshot{CropDefinitionId: 7, CatalogVersionAtPlant: 9, DefinitionVersionAtPlant: 11},
			StealPlanTemplate: &contractspb.StealPlanTemplate{PolicyVersion: 1, TotalCapRatioBp: 2000, SlotRatioBp: []uint32{200, 200, 200, 200, 200, 200, 200, 200, 200, 200}},
			CarePlanTemplate:  &contractspb.CarePlanTemplate{CareRuleVersion: 4},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func testPlantAddress(workID string) workidentity.WorkAddress {
	return workidentity.WorkAddress{WorkID: workID, WorkType: FamilyPlant, PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: 7}
}

func plantStepNames() []string {
	return []string{"RESERVE_SEED", "COMMIT_PLANT", "COMMIT_SEED_RESERVATION", "RELEASE_SEED_RESERVATION"}
}

type testRouteResolver struct{}

func (testRouteResolver) Resolve(_ context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	endpoint := "player"
	holder := "player-holder"
	boot := "player-boot"
	if address.Kind == actorrouting.DomainFarm {
		endpoint = "farm"
		holder = "farm-holder"
		boot = "farm-boot"
	}
	return actorrouting.Assignment{
		Kind: address.Kind, ShardID: 3, HolderPod: holder, HolderBootID: boot, Endpoint: endpoint,
		ShardRevision: 4, AssignmentRevision: 4, WriterEpoch: 5, LeaseEpoch: 6,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(), SnapshotRevision: 7,
		HolderState: actorrouting.HolderStateActive, ShardState: actorrouting.ShardStateActive,
	}, nil
}

func TestPlantRecoveryRouteLeaseProofCompatibility(t *testing.T) {
	resolver := testRouteResolver{}
	header, err := (&plantHandler{resolver: resolver, realm: "realm-a"}).route(context.Background(), workflow.StepCall{},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, 10001)
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := resolver.Resolve(context.Background(), actorrouting.Address{Realm: "realm-a", Kind: actorrouting.DomainFarm, ID: 10001})
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != assignment.LeaseEpoch || header.GetExpectedLeaseUntilMs() != uint64(assignment.LeaseUntilMs) {
		t.Fatalf("recovery producer omitted lease proof: %+v", header)
	}
	legacy := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	legacy.ExpectedLeaseEpoch, legacy.ExpectedLeaseUntilMs = 0, nil
	partial := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	partial.ExpectedLeaseUntilMs = nil
	if !sameAssignment(header, assignment) || !sameAssignment(legacy, assignment) || sameAssignment(partial, assignment) {
		t.Fatalf("mixed-version assignment current=%v legacy=%v partial=%v", sameAssignment(header, assignment), sameAssignment(legacy, assignment), sameAssignment(partial, assignment))
	}
}

type workerSpy struct {
	mu        sync.Mutex
	calls     []*contractspb.ParticipantRequest
	loseFirst bool
	stored    *contractspb.ParticipantResponse
	response  func(*contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)
	err       error
	delay     time.Duration
	onCall    func(*contractspb.ParticipantRequest)
	// staleOnce simulates a WorkVersion proof mismatch (FailedPrecondition
	// "participant proof: WorkVersion ...") on the first ResumeStep call;
	// subsequent calls execute normally. The RPC adapter must surface it as
	// workflow.ErrStaleWorkProof and the engine must reload and re-invoke.
	staleOnce bool
}

func (s *workerSpy) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	s.calls = append(s.calls, proto.Clone(request).(*contractspb.ParticipantRequest))
	if s.stored != nil {
		response := proto.Clone(s.stored).(*contractspb.ParticipantResponse)
		s.mu.Unlock()
		return response, nil
	}
	responseFactory := s.response
	configuredErr := s.err
	loseFirst := s.loseFirst
	onCall := s.onCall
	staleOnce := s.staleOnce
	if s.loseFirst {
		s.loseFirst = false
	}
	if s.staleOnce {
		s.staleOnce = false
	}
	s.mu.Unlock()
	if onCall != nil {
		onCall(request)
	}
	if staleOnce {
		return nil, status.Error(codes.FailedPrecondition, "participant proof: WorkVersion mismatch")
	}
	if configuredErr != nil {
		return nil, configuredErr
	}
	if responseFactory == nil {
		return nil, errors.New("worker spy response is not configured")
	}
	response, err := responseFactory(request)
	if err != nil {
		return nil, err
	}
	if loseFirst {
		s.mu.Lock()
		s.stored = proto.Clone(response).(*contractspb.ParticipantResponse)
		s.mu.Unlock()
		return nil, errors.New("response lost after participant receipt")
	}
	return response, nil
}

func (s *workerSpy) callsCopy() []*contractspb.ParticipantRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*contractspb.ParticipantRequest, len(s.calls))
	for i, call := range s.calls {
		result[i] = proto.Clone(call).(*contractspb.ParticipantRequest)
	}
	return result
}

func reserveResponse(request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	payload := &contractspb.ReserveSeedPayload{}
	if err := proto.Unmarshal(request.GetTypedPayload(), payload); err != nil {
		return nil, err
	}
	result := &contractspb.ReserveSeedResult{ReservationId: payload.GetReservationId(), SeedItemId: payload.GetSeedItemId(), ReservedQuantity: payload.GetQuantity(), PlayerRecordVersion: 2}
	typed, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, err
	}
	spec, err := contracts.LookupParticipantCommand(request.GetParticipantCommand())
	if err != nil {
		return nil, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand:      request.GetParticipantCommand(),
		ParticipantInvocationId: request.GetRecovery().GetParticipantInvocationId(),
		Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		TypedResult:             typed,
		ParticipantResultHash:   hash[:],
		CommittedRecordVersion:  2,
	}, nil
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}

func prometheusTestRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}
