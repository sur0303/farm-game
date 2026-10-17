package theftonline

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
)

type capturingFarmRecovery struct {
	request *contractspb.ParticipantRequest
	outcome contractspb.ParticipantOutcome
}

func (f *capturingFarmRecovery) ResumeStep(
	_ context.Context,
	request *contractspb.ParticipantRequest,
	_ ...grpc.CallOption,
) (*contractspb.ParticipantResponse, error) {
	f.request = request
	return &contractspb.ParticipantResponse{
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT,
		Outcome:            f.outcome,
	}, nil
}

func TestSourceOrderAdvancerCommitsVisitorAndGuardOrders(t *testing.T) {
	order := sourceOrderDispatch(t)
	ids, err := theftrecovery.DeriveTheftIdentity(order)
	if err != nil {
		t.Fatal(err)
	}
	var gotInput theft.Input
	var gotLease theftrecovery.LeaseFence
	advancer, err := NewSourceOrderAdvancer(SourceOrderAdvancerConfig{
		SourceCommit: func(_ context.Context, input theft.Input, lease theftrecovery.LeaseFence) (string, string, error) {
			gotInput, gotLease = input, lease
			return "visitor-order", "guard-order", nil
		},
		FinalizeNoChange: func(context.Context, theftrecovery.DispatchOrder) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := theftrecovery.LeaseFence{HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: 3, WorkVersion: 7}
	result, err := advancer.CreateIfAbsentOrContinue(context.Background(), theftrecovery.AdvanceRequest{
		Identity: ids, Order: order, Lease: lease,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != theftrecovery.AdvanceApplied || result.ReceiptID != "visitor-order" {
		t.Fatalf("result=%+v", result)
	}
	wantProof := sourceOrderAdvanceProof(order, "visitor-order", "guard-order")
	if string(result.ResultProofHash) != string(wantProof) {
		t.Fatalf("proof=%x want=%x", result.ResultProofHash, wantProof)
	}
	if gotInput.OperationID != order.OperationID || gotInput.TargetFarmID != order.TargetFarmID ||
		gotLease.LeaseEpoch != 3 || gotLease.HolderID != "worker-1" {
		t.Fatalf("input=%+v lease=%+v", gotInput, gotLease)
	}
}

func TestNewRecoverySourceCommitRequiresPairLease(t *testing.T) {
	commit := NewRecoverySourceCommit(RecoverySourceCommitConfig{
		Farm: &capturingFarmRecovery{},
		Route: func(context.Context, uint64) (*logicrpcpb.ActorRouteHeader, error) {
			return nil, errors.New("unused")
		},
		BucketFor: func(uint64) (rediskey.AuthorityBucketRef, error) {
			return rediskey.AuthorityBucketRef{}, errors.New("unused")
		},
	})
	_, _, err := commit(context.Background(), theft.Input{TargetFarmID: 9, OperationID: "op"}, theftrecovery.LeaseFence{})
	if !errors.Is(err, ErrInvalidComposition) {
		t.Fatalf("err=%v", err)
	}
}

func TestNewRecoverySourceCommitDerivesSourceOrderIDsForApplied(t *testing.T) {
	order := sourceOrderDispatch(t)
	input, err := inputFromDispatch(order)
	if err != nil {
		t.Fatal(err)
	}
	farm := &capturingFarmRecovery{outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED}
	commit := NewRecoverySourceCommit(RecoverySourceCommitConfig{
		Farm: farm,
		Route: func(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
			return &logicrpcpb.ActorRouteHeader{
				RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID,
			}, nil
		},
		BucketFor: func(uint64) (rediskey.AuthorityBucketRef, error) {
			return rediskey.AuthorityBucketRef{}, nil
		},
	})
	visitorID, guardID, err := commit(context.Background(), input, theftrecovery.LeaseFence{
		HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: 2, WorkVersion: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantVisitor, err := sourceorder.TheftVisitorSourceOrderID(order.TargetFarmID, order.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	wantGuard, err := sourceorder.TheftGuardSourceOrderID(order.TargetFarmID, order.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if visitorID != wantVisitor || guardID != wantGuard {
		t.Fatalf("visitor=%s guard=%s", visitorID, guardID)
	}
	recovery := farm.request.GetRecovery()
	if recovery == nil || recovery.GetLeaseEpoch() != 2 || recovery.GetExpectedWorkVersion() != 7 ||
		recovery.GetParticipantStep() != contractspb.ParticipantStep_PARTICIPANT_STEP_DECIDE_THEFT {
		t.Fatalf("recovery=%v", recovery)
	}
}

func TestNewRecoverySourceCommitDoesNotInventSourceOrdersForNoChange(t *testing.T) {
	order := sourceOrderDispatch(t)
	input, err := inputFromDispatch(order)
	if err != nil {
		t.Fatal(err)
	}
	farm := &capturingFarmRecovery{outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE}
	commit := NewRecoverySourceCommit(RecoverySourceCommitConfig{
		Farm: farm,
		Route: func(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
			return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID}, nil
		},
		BucketFor: func(uint64) (rediskey.AuthorityBucketRef, error) { return rediskey.AuthorityBucketRef{}, nil },
	})
	visitorID, guardID, err := commit(context.Background(), input, theftrecovery.LeaseFence{
		HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: 2, WorkVersion: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if visitorID != "" || guardID != "" {
		t.Fatalf("NO_CHANGE must not create source orders: visitor=%q guard=%q", visitorID, guardID)
	}
}

func sourceOrderDispatch(t *testing.T) theftrecovery.DispatchOrder {
	t.Helper()
	now := time.UnixMilli(1_700_000_000_000)
	const visitor, owner, farmID = uint64(10001), uint64(20002), uint64(20002)
	pairID, err := identity.CanonicalFriendRelationID(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	operationID := "theft-source-order-dispatch"
	workID, err := workidentity.StableOpaqueID("theft-dispatch-source", operationID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := workidentity.StableOpaqueID("theft-cycle-source", operationID)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("TLP1test-source-dispatch-payload")
	payloadHash := sha256.Sum256(payload)
	canonical := sha256.Sum256([]byte("canonical-source-theft-payload"))
	binding := sha256.Sum256([]byte("farm=20002/plot=3/cycle=" + cycleID))
	permitID, err := workidentity.StableOpaqueID("permit-source", operationID)
	if err != nil {
		t.Fatal(err)
	}
	permit := friend.ActionPermitSnapshot{
		PermitID: permitID, RelationID: pairID, RelationEpoch: 7,
		VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: farmID, OperationID: operationID,
		Scope: friend.ActionScopeTheft, PayloadHash: canonical[:], TargetBindingHash: binding[:],
		IssuedAtAuthorityMs: now.UnixMilli(), ExpireAtAuthorityMs: now.Add(time.Hour).UnixMilli(),
		ExecutionAcceptedAtAuthorityMs: now.Add(time.Millisecond).UnixMilli(),
	}
	order := theftrecovery.DispatchOrder{
		Work: theftrecovery.WorkAddress{
			WorkID: workID, WorkType: theftrecovery.FriendActionDispatchOrder,
			PlacementKind: theftrecovery.SourcePersistenceBucket, BucketVersion: "friend-auth-v1", BucketID: 7,
			SourceAuthorityKind: theftrecovery.FriendPairAuthority, CanonicalSourceAuthority: pairID,
			SourceLayoutVersion: "friend-auth-v1",
		},
		Payload: payload, PayloadHash: payloadHash[:], CanonicalPayloadHash: canonical[:], TargetBindingHash: binding[:],
		PairID: pairID, OperationID: operationID, VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: farmID,
		PlotID: 3, CropCycleID: cycleID, Permit: permit,
		Admission: theftrecovery.AdmissionProof{
			ReceiptID: "admission-receipt", PayloadHash: canonical[:],
			PermitHash: theftrecovery.PermitHash(permit), AcceptedAtMs: permit.ExecutionAcceptedAtAuthorityMs,
		},
		NextDueAtMs: now.UnixMilli(), DeadlineAtMs: now.Add(2 * time.Hour).UnixMilli(),
	}
	normalized, err := theftrecovery.NormalizeDispatchOrder(order)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}
