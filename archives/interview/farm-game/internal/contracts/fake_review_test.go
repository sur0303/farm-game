package contracts

import (
	"context"
	"sync"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// guardSnapshotRequest builds a valid APPLY_GUARD_SNAPSHOT request whose
// work address lives in the source persistence bucket of a PLAYER_ACTOR.
func guardSnapshotRequest(t *testing.T) *contractspb.ParticipantRequest {
	t.Helper()
	orderID, err := identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor, "10001", "PET_GUARD_SLOT", 7)
	if err != nil {
		t.Fatalf("order id: %v", err)
	}
	payload := &contractspb.ApplyGuardSnapshotPayload{
		OrderId:        orderID,
		SourcePlayerId: 10001,
		TargetFarmId:   10001,
		GuardSnapshot: &contractspb.GuardSnapshot{
			SchemaVersion: 1,
			GuardVersion:  7,
			GuardUntilMs:  999999,
			GuardRule:     &contractspb.GuardRuleSnapshot{PetConfigVersion: 1, GuardRuleVersion: 2},
		},
	}
	raw, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
			ActorId:     10001,
		},
		CallerContext: &contractspb.ParticipantRequest_Coordinator{
			Coordinator: &contractspb.CoordinatorWorkContext{
				Work: &contractspb.WorkAddress{
					WorkId:        orderID,
					WorkType:      contractspb.WorkType_WORK_TYPE_GUARD_SYNC_ORDER,
					PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
					BucketVersion: 1,
					SourceAuthority: &contractspb.SourceAuthorityRef{
						AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
						CanonicalAuthorityId:   "10001",
						AuthorityLayoutVersion: "auth-v1",
						ActorRouteDomain:       routeDomainPtr(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER),
					},
				},
				ExpectedWorkVersion:     1,
				ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_GUARD_SNAPSHOT,
				ParticipantInvocationId: mustInvocation(t, identity.WorkTypeGuardSyncOrder, orderID, identity.StepApplyGuardSnapshot),
				OperationId:             "op-guard-1",
				SubjectPlayerId:         10001,
				RemainingBudgetMs:       1000,
			},
		},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
		TypedPayload:       raw,
	}
}

func routeDomainPtr(d logicrpcpb.RouteDomain) *logicrpcpb.RouteDomain { return &d }

func appliedGuardHandler(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
	p := payload.(*contractspb.ApplyGuardSnapshotPayload)
	return &StepOutcome{
		Outcome:                contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		CommittedRecordVersion: 3,
		Result: &contractspb.ApplyGuardSnapshotResult{
			GuardVersion:           p.GetGuardSnapshot().GetGuardVersion(),
			ApplyStatus:            contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED,
			CommittedRecordVersion: 3,
		},
	}, nil
}

func registerGuard(t *testing.T, server *FakeParticipantServer) {
	t.Helper()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT, appliedGuardHandler)
}

func TestFakePreservesExactHandlerCommittedRecordVersion(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(context.Context, *contractspb.ParticipantRequest, proto.Message) (*StepOutcome, error) {
			return &StepOutcome{
				Outcome:                contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
				CommittedRecordVersion: 42,
				Result:                 &contractspb.ReserveSeedResult{ReservationId: "reservation", SeedItemId: 701, ReservedQuantity: 1},
			}, nil
		})
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	first, err := server.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := server.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetCommittedRecordVersion() != 42 || replay.GetCommittedRecordVersion() != 42 || !replay.GetReplayedFromReceipt() {
		t.Fatalf("fake invented or changed handler version first=%+v replay=%+v", first, replay)
	}
}

// 审查项 1:RouteHeader 存在性、actor_id 与目标 Domain 绑定。
func TestFakeRouteHeaderBinding(t *testing.T) {
	server := NewFakeParticipantServer()
	registerGuard(t, server)
	client := NewFakeParticipantClient(server)

	valid := guardSnapshotRequest(t)
	if _, err := client.ExecuteStep(context.Background(), valid); err != nil {
		t.Fatalf("valid request: %v", err)
	}

	missing := guardSnapshotRequest(t)
	missing.RouteHeader = nil
	if _, err := client.ExecuteStep(context.Background(), missing); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing route header must fail, got %v", err)
	}

	zeroActor := guardSnapshotRequest(t)
	zeroActor.RouteHeader.ActorId = 0
	if _, err := client.ExecuteStep(context.Background(), zeroActor); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero actor id must fail, got %v", err)
	}

	wrongDomain := guardSnapshotRequest(t)
	wrongDomain.RouteHeader.RouteDomain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	if _, err := client.ExecuteStep(context.Background(), wrongDomain); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("APPLY_GUARD_SNAPSHOT with PLAYER route header must fail, got %v", err)
	}

	// 反向钉死:RESERVE_SEED 不能携带 FARM RouteHeader。
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	seedReq := newReserveSeedRequest(t, workID, &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1})
	seedReq.RouteHeader.RouteDomain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	if _, err := client.ExecuteStep(context.Background(), seedReq); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RESERVE_SEED with FARM route header must fail, got %v", err)
	}
}

// The shared ingress kernel also binds the typed schema's actor target to the
// route header. This must fail before a Player/Farm adapter or mailbox sees it.
func TestFakeTypedPayloadActorBinding(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)

	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", payload)
	forged := proto.Clone(payload).(*contractspb.ReserveSeedPayload)
	forged.PlayerId = 10002
	raw, err := proto.Marshal(forged)
	if err != nil {
		t.Fatalf("marshal forged payload: %v", err)
	}
	req.TypedPayload = raw

	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("typed payload actor mismatch must fail closed, got %v", err)
	}
}

// 审查项 2:WorkType 与 source authority 形状校验。
func TestFakeWorkAddressAndSourceValidation(t *testing.T) {
	server := NewFakeParticipantServer()
	registerGuard(t, server)
	client := NewFakeParticipantClient(server)

	cases := []struct {
		name   string
		mutate func(*contractspb.ParticipantRequest)
	}{
		{"work type mismatch", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.WorkType = contractspb.WorkType_WORK_TYPE_CARE_REWARD
		}},
		{"invalid work id", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.WorkId = "not-a-sid"
		}},
		{"missing source authority", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority = nil
		}},
		{"source kind not allowed", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority.AuthorityKind = contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR
			r.GetCoordinator().Work.SourceAuthority.ActorRouteDomain = routeDomainPtr(logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM)
		}},
		{"actor source wrong route domain", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority.ActorRouteDomain = routeDomainPtr(logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM)
		}},
		{"actor source missing route domain", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority.ActorRouteDomain = nil
		}},
		{"missing canonical authority id", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority.CanonicalAuthorityId = ""
		}},
		{"missing layout version", func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.SourceAuthority.AuthorityLayoutVersion = ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := guardSnapshotRequest(t)
			tc.mutate(req)
			if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("must fail closed, got %v", err)
			}
		})
	}

	t.Run("friend pair must not carry route domain", func(t *testing.T) {
		req := guardSnapshotRequest(t)
		req.GetCoordinator().Work.SourceAuthority.AuthorityKind = contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FRIEND_PAIR
		// PLAYER route domain 仍在 → FRIEND_PAIR 伪造 Actor 身份必须拒绝。
		if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v", err)
		}
	})
}

// 审查项 3:错误 Typed Result Schema 必须 fail closed 且不写 Receipt。
func TestFakeRejectsWrongResultSchema(t *testing.T) {
	server := NewFakeParticipantServer()
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			calls++
			if calls == 1 {
				// 返回了 APPLY_GUARD_SNAPSHOT 的 Result Schema。
				return &StepOutcome{
					Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
					Result:  &contractspb.ApplyGuardSnapshotResult{GuardVersion: 1},
				}, nil
			}
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)

	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.Internal {
		t.Fatalf("wrong result schema must fail closed, got %v", err)
	}
	// 未写 Receipt:修正 handler 后同 invocation 重试必须真正执行并成功。
	resp, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("retry after schema fix: %v", err)
	}
	if resp.GetReplayedFromReceipt() {
		t.Fatal("schema-mismatched call must not leave a receipt")
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

// 审查项 5:replay 不得泄露可变的 stored Failure/Result。
func TestFakeReplayIsolation(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			return &StepOutcome{
				Outcome:                contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED,
				CommittedRecordVersion: 1,
				Result: &contractspb.ReserveSeedResult{
					ReservationId: "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
					SeedItemId:    701,
				},
				Failure: &contractspb.ParticipantFailure{
					Error: &logicrpcpb.ErrorDetail{ErrorMessage: "original"},
				},
			}, nil
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)

	first, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// 调用方污染首次响应。
	first.GetFailure().GetError().ErrorMessage = "mutated"
	first.TypedResult[0] ^= 0xff

	second, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.GetReplayedFromReceipt() {
		t.Fatal("second call must replay")
	}
	if got := second.GetFailure().GetError().GetErrorMessage(); got != "original" {
		t.Fatalf("stored failure polluted: %q", got)
	}
	if second.TypedResult[0] == first.TypedResult[0] && len(second.GetTypedResult()) > 0 {
		// first 已被 XOR,相等说明共享了底层数组。
		t.Fatal("stored typed result shares memory with caller")
	}
}

// 审查项 4/6:并发相同 invocation 只执行一次 Handler;不同 payload 并发
// 得到 CONFLICT 且不重复执行;不同 invocation 独立推进。
func TestFakeConcurrentSameInvocation(t *testing.T) {
	server := NewFakeParticipantServer()
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			<-release // 阻塞,制造并发窗口
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}

	const n = 8
	responses := make([]*contractspb.ParticipantResponse, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses[i], errs[i] = client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, payload))
		}(i)
	}
	// 等首个 handler 进入后再放行,保证其余请求都在等待 in-flight。
	for {
		mu.Lock()
		if calls > 0 {
			mu.Unlock()
			break
		}
		mu.Unlock()
	}
	close(release)
	wg.Wait()

	mu.Lock()
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	mu.Unlock()
	replays := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if responses[i].GetReplayedFromReceipt() {
			replays++
		}
		if responses[i].GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED ||
			responses[i].GetCommittedRecordVersion() != 1 {
			t.Fatalf("request %d diverged: outcome=%v version=%d", i, responses[i].GetOutcome(), responses[i].GetCommittedRecordVersion())
		}
	}
	if replays != n-1 {
		t.Fatalf("replays = %d, want %d", replays, n-1)
	}
}

func TestFakeConcurrentConflictingPayload(t *testing.T) {
	server := NewFakeParticipantServer()
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			<-release
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	original := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	conflicting := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 9, SeedItemId: 701, Quantity: 1}

	var wg sync.WaitGroup
	var leaderResp *contractspb.ParticipantResponse
	var leaderErr error
	conflictErrs := make([]error, 4)
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderResp, leaderErr = client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, original))
	}()
	for {
		mu.Lock()
		if calls > 0 {
			mu.Unlock()
			break
		}
		mu.Unlock()
	}
	for i := range conflictErrs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, conflictErrs[i] = client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, conflicting))
		}(i)
	}
	close(release)
	wg.Wait()

	if leaderErr != nil {
		t.Fatalf("leader: %v", leaderErr)
	}
	if leaderResp.GetReplayedFromReceipt() {
		t.Fatal("leader must execute, not replay")
	}
	mu.Lock()
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1 (conflicting payload must not execute)", calls)
	}
	mu.Unlock()
	for i, err := range conflictErrs {
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.FailedPrecondition || st.Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
			t.Fatalf("conflicting request %d must get PARTICIPANT_IDEMPOTENCY_CONFLICT, got %v", i, err)
		}
	}
}

func TestFakeConcurrentDistinctInvocations(t *testing.T) {
	server := NewFakeParticipantServer()
	var mu sync.Mutex
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)

	// 两个不同 work_id → 不同 invocation,必须互不阻塞地独立推进。
	workA := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	workB := "sid1_UxPZD3u5t4O3VczETFXvXyR2QkXmHAZx6Geck87do1s"
	payloadA := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	payloadB := &contractspb.ReserveSeedPayload{PlayerId: 10002, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}

	var wg sync.WaitGroup
	resps := make([]*contractspb.ParticipantResponse, 2)
	errs := make([]error, 2)
	for i, args := range [][2]interface{}{{workA, payloadA}, {workB, payloadB}} {
		wg.Add(1)
		go func(i int, workID string, p *contractspb.ReserveSeedPayload) {
			defer wg.Done()
			resps[i], errs[i] = client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, p))
		}(i, args[0].(string), args[1].(*contractspb.ReserveSeedPayload))
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if resps[i].GetReplayedFromReceipt() {
			t.Fatalf("request %d: distinct invocations must both execute", i)
		}
	}
	mu.Lock()
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
	mu.Unlock()
}

// 审查项 4 续:Handler 错误释放等待者,后续重试可以成功。
func TestFakeConcurrentErrorReleasesWaiters(t *testing.T) {
	server := NewFakeParticipantServer()
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			<-release
			if n == 1 {
				return nil, status.Error(codes.Unavailable, "injected transient failure")
			}
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}

	var wg sync.WaitGroup
	resps := make([]*contractspb.ParticipantResponse, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resps[i], errs[i] = client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, payload))
		}(i)
	}
	for {
		mu.Lock()
		if calls > 0 {
			mu.Unlock()
			break
		}
		mu.Unlock()
	}
	close(release)
	wg.Wait()

	// 一个拿到注入错误,另一个在释放后成为新 leader 并成功(或 replay)。
	failures, successes := 0, 0
	for i := range errs {
		if errs[i] != nil {
			if status.Code(errs[i]) != codes.Unavailable {
				t.Fatalf("request %d unexpected error: %v", i, errs[i])
			}
			failures++
			continue
		}
		if resps[i].GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("request %d outcome = %v", i, resps[i].GetOutcome())
		}
		successes++
	}
	if failures != 1 || successes != 1 {
		t.Fatalf("failures=%d successes=%d, want 1/1", failures, successes)
	}
}
