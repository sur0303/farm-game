package contracts

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestParticipantCanonicalHashFramesSchemaAndCommand(t *testing.T) {
	spec, err := LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, Quantity: 1}
	preimage, err := CanonicalParticipantBytes(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(preimage) < 8 || binary.BigEndian.Uint32(preimage[:4]) != ParticipantCanonicalSchemaVersion ||
		binary.BigEndian.Uint32(preimage[4:8]) != uint32(spec.Command) {
		t.Fatalf("participant preimage prefix=%x", preimage[:minInt(len(preimage), 8)])
	}
	semantic, err := CanonicalBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(preimage[8:], semantic) {
		t.Fatalf("participant preimage does not contain semantic fields")
	}
	legacyHash, err := CanonicalHash(payload)
	if err != nil {
		t.Fatal(err)
	}
	participantHash, err := CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(legacyHash[:], participantHash[:]) {
		t.Fatal("participant hash must not be the unframed typed-message hash")
	}
	result := &contractspb.ReserveSeedResult{ReservationId: "reservation", SeedItemId: 701, ReservedQuantity: 1, PlayerRecordVersion: 7}
	resultLegacy, err := CanonicalHash(result)
	if err != nil {
		t.Fatal(err)
	}
	resultParticipant, err := CanonicalParticipantHash(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(resultLegacy[:], resultParticipant[:]) {
		t.Fatal("participant result hash must use the framed preimage")
	}
	if _, err := CanonicalParticipantHash(spec, &contractspb.CommitSeedReservationPayload{}); err == nil {
		t.Fatal("participant hash must reject a typed schema from another command")
	}
}

func TestParticipantIngressBudgetRouteBucketAndIdentityProofs(t *testing.T) {
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	allow := ParticipantIngressOptions{
		RouteFenceVerifier: ParticipantRouteFenceVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		WorkAddressVerifier: ParticipantWorkAddressVerifierFunc(func(context.Context, *ParticipantCall) error {
			return nil
		}),
		IdentityBindingVerifier: ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error {
			return nil
		}),
	}
	if _, err := ValidateAndDecodeWithOptions(context.Background(), request, ParticipantCallerCoordinator, allow); err != nil {
		t.Fatalf("valid injected proofs: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*contractspb.ParticipantRequest)
		ctx    context.Context
		want   codes.Code
	}{
		{name: "exhausted budget", mutate: func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().RemainingBudgetMs = 0
		}, ctx: context.Background(), want: codes.InvalidArgument},
		{name: "budget is clipped to context deadline", mutate: func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().RemainingBudgetMs = 250
		}, ctx: contextWithBudget(50 * time.Millisecond), want: codes.OK},
		{name: "deadline exhausted", mutate: func(*contractspb.ParticipantRequest) {}, ctx: expiredContext(), want: codes.DeadlineExceeded},
		{name: "partial route holder fence", mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedHolderBootId = ""
		}, ctx: context.Background(), want: codes.InvalidArgument},
		{name: "missing bucket version", mutate: func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().Work.BucketVersion = 0
		}, ctx: context.Background(), want: codes.InvalidArgument},
		{name: "subject mismatch", mutate: func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().SubjectPlayerId = 10002
		}, ctx: context.Background(), want: codes.FailedPrecondition},
		{name: "operation mismatch", mutate: func(r *contractspb.ParticipantRequest) {
			r.GetCoordinator().OperationId = "other-operation"
		}, ctx: context.Background(), want: codes.FailedPrecondition},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := cloneParticipantRequest(request)
			tc.mutate(candidate)
			options := allow
			if tc.name == "subject mismatch" || tc.name == "operation mismatch" {
				options.IdentityBindingVerifier = ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error {
					if candidate.GetCoordinator().GetSubjectPlayerId() != request.GetCoordinator().GetSubjectPlayerId() ||
						candidate.GetCoordinator().GetOperationId() != request.GetCoordinator().GetOperationId() {
						return status.Error(codes.FailedPrecondition, "subject/operation binding mismatch")
					}
					return nil
				})
			}
			if _, err := ValidateAndDecodeWithOptions(tc.ctx, candidate, ParticipantCallerCoordinator, options); status.Code(err) != tc.want {
				t.Fatalf("error=%v code=%v want=%v", err, status.Code(err), tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		request *contractspb.ParticipantRequest
		options ParticipantIngressOptions
	}{
		{name: "route fence", request: func() *contractspb.ParticipantRequest {
			candidate := cloneParticipantRequest(request)
			candidate.RouteHeader.VirtualShard = 9
			return candidate
		}(), options: ParticipantIngressOptions{
			RouteFenceVerifier: ParticipantRouteFenceVerifierFunc(func(_ context.Context, call *ParticipantCall) error {
				if call.RouteHeader.GetVirtualShard() != request.GetRouteHeader().GetVirtualShard() {
					return status.Error(codes.FailedPrecondition, "route fence mismatch")
				}
				return nil
			}),
			WorkAddressVerifier: allow.WorkAddressVerifier, IdentityBindingVerifier: allow.IdentityBindingVerifier,
		}},
		{name: "bucket identity", request: func() *contractspb.ParticipantRequest {
			candidate := cloneParticipantRequest(request)
			candidate.GetCoordinator().Work.BucketId = 17
			return candidate
		}(), options: ParticipantIngressOptions{
			RouteFenceVerifier: allow.RouteFenceVerifier,
			WorkAddressVerifier: ParticipantWorkAddressVerifierFunc(func(_ context.Context, call *ParticipantCall) error {
				if call.Work.GetBucketId() != request.GetCoordinator().GetWork().GetBucketId() {
					return status.Error(codes.FailedPrecondition, "persistence bucket mismatch")
				}
				return nil
			}),
			IdentityBindingVerifier: allow.IdentityBindingVerifier,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ValidateAndDecodeWithOptions(context.Background(), tc.request, ParticipantCallerCoordinator, tc.options); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestParticipantBudgetUsesCommandMaximumAndVerifierElapsedTime(t *testing.T) {
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	request.GetCoordinator().RemainingBudgetMs = ParticipantMaxBudgetMs + 1000
	start := time.UnixMilli(10_000)
	call, err := ValidateAndDecodeWithOptions(context.Background(), request, ParticipantCallerCoordinator, ParticipantIngressOptions{
		RouteFenceVerifier:      ParticipantRouteFenceVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		WorkAddressVerifier:     ParticipantWorkAddressVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		IdentityBindingVerifier: ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		Now:                     func() time.Time { return start },
	})
	if err != nil {
		t.Fatal(err)
	}
	if call.RemainingBudgetMs != ParticipantMaxBudgetMs {
		t.Fatalf("command max was not applied, remaining=%d", call.RemainingBudgetMs)
	}

	request.GetCoordinator().RemainingBudgetMs = 10
	clockCalls := 0
	_, err = ValidateAndDecodeWithOptions(context.Background(), request, ParticipantCallerCoordinator, ParticipantIngressOptions{
		RouteFenceVerifier:  ParticipantRouteFenceVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		WorkAddressVerifier: ParticipantWorkAddressVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		IdentityBindingVerifier: ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error {
			return nil
		}),
		Now: func() time.Time {
			clockCalls++
			if clockCalls < 3 {
				return start
			}
			return start.Add(11 * time.Millisecond)
		},
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("elapsed verifier budget must fail before mailbox, err=%v", err)
	}
}

func TestParticipantStrictPhysicalRouteFieldsFailClosed(t *testing.T) {
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	base := ParticipantIngressOptions{
		RequirePhysicalProof:    true,
		RequireRouteLeaseProof:  true,
		RouteFenceVerifier:      ParticipantRouteFenceVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		WorkAddressVerifier:     ParticipantWorkAddressVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		IdentityBindingVerifier: ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
	}
	for _, mutate := range []func(*contractspb.ParticipantRequest){
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ExpectedHolderId = "" },
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ExpectedHolderBootId = "" },
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ShardRevision = 0 },
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ExpectedWriterEpoch = 0 },
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ExpectedLeaseEpoch = 0 },
		func(r *contractspb.ParticipantRequest) { r.RouteHeader.ExpectedLeaseUntilMs = nil },
		func(r *contractspb.ParticipantRequest) {
			zero := uint64(0)
			r.RouteHeader.ExpectedLeaseUntilMs = &zero
		},
	} {
		candidate := cloneParticipantRequest(request)
		mutate(candidate)
		if _, err := ValidateAndDecodeWithOptions(context.Background(), candidate, ParticipantCallerCoordinator, base); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("incomplete physical route must fail, err=%v", err)
		}
	}
}

func TestParticipantRouteLeaseProofMixedVersionMatrix(t *testing.T) {
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	allow := func(strict bool) ParticipantIngressOptions {
		return ParticipantIngressOptions{
			RequirePhysicalProof:   true,
			RequireRouteLeaseProof: strict,
			RouteFenceVerifier: ParticipantRouteFenceVerifierFunc(func(_ context.Context, call *ParticipantCall) error {
				route := call.RouteHeader
				hasEpoch := route.GetExpectedLeaseEpoch() != 0
				hasDeadline := route.ExpectedLeaseUntilMs != nil
				if !hasEpoch && !hasDeadline {
					return nil
				}
				if !hasEpoch || !hasDeadline || route.GetExpectedLeaseEpoch() != 1 || route.GetExpectedLeaseUntilMs() != 1_900_000_000_000 {
					return status.Error(codes.FailedPrecondition, "route lease mismatch")
				}
				return nil
			}),
			WorkAddressVerifier:     ParticipantWorkAddressVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
			IdentityBindingVerifier: ParticipantIdentityBindingVerifierFunc(func(context.Context, *ParticipantCall) error { return nil }),
		}
	}
	cases := []struct {
		name   string
		strict bool
		mutate func(*contractspb.ParticipantRequest)
		code   codes.Code
	}{
		{name: "new producer to compatible server", code: codes.OK},
		{name: "new producer to strict server", strict: true, code: codes.OK},
		{name: "legacy producer to compatible server", mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedLeaseEpoch, r.RouteHeader.ExpectedLeaseUntilMs = 0, nil
		}, code: codes.OK},
		{name: "legacy producer to strict server", strict: true, mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedLeaseEpoch, r.RouteHeader.ExpectedLeaseUntilMs = 0, nil
		}, code: codes.InvalidArgument},
		{name: "partial legacy epoch", mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedLeaseUntilMs = nil
		}, code: codes.InvalidArgument},
		{name: "partial legacy deadline", mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedLeaseEpoch = 0
		}, code: codes.InvalidArgument},
		{name: "mismatched new proof", mutate: func(r *contractspb.ParticipantRequest) {
			r.RouteHeader.ExpectedLeaseEpoch++
		}, code: codes.FailedPrecondition},
	}
	before := LegacyParticipantRouteLeaseProofCount(ParticipantCallerCoordinator)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := cloneParticipantRequest(request)
			if tc.mutate != nil {
				tc.mutate(candidate)
			}
			_, err := ValidateAndDecodeWithOptions(context.Background(), candidate, ParticipantCallerCoordinator, allow(tc.strict))
			if status.Code(err) != tc.code {
				t.Fatalf("error=%v code=%s want=%s", err, status.Code(err), tc.code)
			}
		})
	}
	if got := LegacyParticipantRouteLeaseProofCount(ParticipantCallerCoordinator); got != before+1 {
		t.Fatalf("legacy compatibility metric delta=%d, want 1", got-before)
	}
}

func TestParticipantWorkerLeaseAndStepShapeFailClosed(t *testing.T) {
	coordinator := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	base := &contractspb.ParticipantRequest{
		RouteHeader:        coordinator.GetRouteHeader(),
		ParticipantCommand: coordinator.GetParticipantCommand(),
		TypedPayload:       coordinator.GetTypedPayload(),
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId:        "worker-1",
			CallerBootId:            "worker-boot-1",
			Work:                    coordinator.GetCoordinator().GetWork(),
			ExpectedWorkVersion:     1,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
			ParticipantInvocationId: coordinator.GetCoordinator().GetParticipantInvocationId(),
			LeaseOwnerToken:         "lease-token",
			LeaseEpoch:              1,
			RemainingBudgetMs:       1000,
		}},
	}
	for _, mutate := range []func(*contractspb.InternalCallerContext){
		func(c *contractspb.InternalCallerContext) { c.ExpectedWorkVersion = 0 },
		func(c *contractspb.InternalCallerContext) { c.CallerInstanceId = "" },
		func(c *contractspb.InternalCallerContext) { c.CallerBootId = "" },
		func(c *contractspb.InternalCallerContext) { c.LeaseOwnerToken = "" },
		func(c *contractspb.InternalCallerContext) { c.LeaseEpoch = 0 },
		func(c *contractspb.InternalCallerContext) {
			c.ParticipantStep = contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_SEED_RESERVATION
		},
	} {
		candidate := cloneParticipantRequest(base)
		mutate(candidate.GetRecovery())
		if _, err := ValidateAndDecodeWithOptions(context.Background(), candidate, ParticipantCallerWorkerRecovery, ParticipantIngressOptions{}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("worker shape mutation must fail, err=%v", err)
		}
	}
}

func expiredContext() context.Context {
	return fixedDeadlineContext{deadline: time.Now().Add(-time.Millisecond), err: context.DeadlineExceeded}
}

func contextWithBudget(budget time.Duration) context.Context {
	return fixedDeadlineContext{deadline: time.Now().Add(budget)}
}

// fixedDeadlineContext gives the ingress kernel a deterministic context
// deadline without allocating a timer that outlives the table-driven case.
type fixedDeadlineContext struct {
	deadline time.Time
	err      error
}

func (c fixedDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c fixedDeadlineContext) Done() <-chan struct{}       { return nil }
func (c fixedDeadlineContext) Err() error                  { return c.err }
func (c fixedDeadlineContext) Value(any) any               { return nil }

func TestParticipantCommitProofCarrierIsCoordinatorCareHarvestOnly(t *testing.T) {
	request := newReserveSeedRequest(t, "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q", &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1,
	})
	request.SourceOrderCommitProof = &contractspb.SourceOrderCommitProof{SchemaVersion: 2}
	if _, err := ValidateAndDecodeWithOptions(context.Background(), request, ParticipantCallerCoordinator, ParticipantIngressOptions{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("non-CARE/HARVEST Coordinator proof accepted: %v", err)
	}

	recovery := cloneParticipantRequest(request)
	recovery.CallerContext = &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{}}
	if _, err := ValidateAndDecodeWithOptions(context.Background(), recovery, ParticipantCallerWorkerRecovery, ParticipantIngressOptions{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WorkerRecovery commit proof accepted: %v", err)
	}

	cloned := cloneSourceOrderCommitProof(request.GetSourceOrderCommitProof())
	cloned.SchemaVersion++
	if request.GetSourceOrderCommitProof().GetSchemaVersion() != 2 {
		t.Fatal("commit proof clone aliases request")
	}
}

func cloneParticipantRequest(request *contractspb.ParticipantRequest) *contractspb.ParticipantRequest {
	return proto.Clone(request).(*contractspb.ParticipantRequest)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
