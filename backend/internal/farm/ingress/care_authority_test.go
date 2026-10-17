package ingress

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/identity"
	"farmgame/internal/logic/trusted"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func TestAuthorityCareIngressUsesFarmActorOrderPath(t *testing.T) {
	const farmID = uint64(1330)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time {
		return time.UnixMilli(ingressTestNowMs)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plantPayload, workflowID := ingressTestPayload(t, farmID, "care-ingress-plant", 1)
	plantResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, farmID, "care-ingress-plant", plantPayload, workflowID))
	if err != nil || plantResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant response=%v err=%v", plantResponse, err)
	}
	var plantResult contractspb.CommitPlantResult
	if err := proto.Unmarshal(plantResponse.GetTypedResult(), &plantResult); err != nil {
		t.Fatal(err)
	}
	incidentID, err := identity.CareIncidentID(plantResult.GetCropCycleId(), "WATER", 1)
	if err != nil {
		t.Fatal(err)
	}

	carePayload := &publicpb.CareRequest{
		PlotId: 0, TargetFarmId: farmID, CropCycleId: plantResult.GetCropCycleId(),
		IncidentId: incidentID, CareType: publicpb.CareType_CARE_TYPE_WATER,
	}
	command := typedCommand(t, "care-ingress-owner", "care-ingress-owner-request", commonpb.CommandType_CARE, carePayload)
	payloadHash := sha256.Sum256(command.GetPayload())
	service := NewService(rig.registry, nil)
	service.SetSourceCommitEvidenceEnabled(true)
	var enqueued struct {
		farmID  uint64
		orderID string
	}
	service.SetCareOrderEnqueuer(func(_ context.Context, farmID uint64, order authoritystore.AttachedOrder, _ *authoritystore.ExactCommitEvidence) error {
		enqueued.farmID, enqueued.orderID = farmID, order.OrderID
		return nil
	})
	action, err := service.careAuthority(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: command,
		Trusted: &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: farmID}},
	}, &trusted.VerifiedCommand{Decoded: carePayload, RouteKey: farmID, PayloadHash: payloadHash[:]}, time.UnixMilli(ingressTestNowMs+10_000))
	if err != nil || action == nil || action.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("authority CARE action=%v err=%v", action, err)
	}
	if action.Settlement != nil || action.CareResult == nil || action.CareResult.CareRewardOrderID == "" || action.Change == nil || action.SourceCommitEvidence == nil {
		t.Fatalf("authority CARE leaked legacy settlement or order/change: action=%+v", action)
	}
	if enqueued.farmID != farmID || enqueued.orderID != action.CareResult.CareRewardOrderID {
		t.Fatalf("durable Care order was not handed to fast path: enqueued=%+v result=%q", enqueued, action.CareResult.CareRewardOrderID)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetActiveSettlements()) != 0 || len(record.GetRecentCareAttempts()) != 0 || record.GetCore().GetFarmVersion() != 2 {
		t.Fatalf("authority CARE wrote legacy state: settlements=%d care_attempts=%d farm_version=%d", len(record.GetActiveSettlements()), len(record.GetRecentCareAttempts()), record.GetCore().GetFarmVersion())
	}

	outcome := &logicrpcpb.BusinessCommandOutcome{}
	applyCareCommandResult(outcome, action, farmID)
	if outcome.Status != commonpb.CommandStatus_SUCCEEDED || len(outcome.ResultPayload) == 0 {
		t.Fatalf("public CARE projection=%+v", outcome)
	}
	var publicResult publicpb.CareResult
	if err := proto.Unmarshal(outcome.ResultPayload, &publicResult); err != nil {
		t.Fatal(err)
	}
	if publicResult.GetTargetFarmId() != farmID || publicResult.GetCropCycleId() != plantResult.GetCropCycleId() ||
		publicResult.GetIncidentId() != incidentID || publicResult.GetCareRewardOrderId() == "" ||
		!publicResult.GetRewardObligationCreated() ||
		publicResult.GetOutcome() != publicpb.CareOutcome_CARE_OUTCOME_RESOLVED {
		t.Fatalf("public CARE result=%+v", &publicResult)
	}
}

func TestAuthorityFriendCareRequiresPairActionAdmission(t *testing.T) {
	const farmID = uint64(1331)
	const visitor = uint64(77)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time {
		return time.UnixMilli(ingressTestNowMs)
	}))
	if err != nil {
		t.Fatal(err)
	}
	plantPayload, workflowID := ingressTestPayload(t, farmID, "care-friend-plant", 1)
	plantResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, farmID, "care-friend-plant", plantPayload, workflowID))
	if err != nil || plantResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant response=%v err=%v", plantResponse, err)
	}
	var plantResult contractspb.CommitPlantResult
	if err := proto.Unmarshal(plantResponse.GetTypedResult(), &plantResult); err != nil {
		t.Fatal(err)
	}
	incidentID, err := identity.CareIncidentID(plantResult.GetCropCycleId(), "WATER", 1)
	if err != nil {
		t.Fatal(err)
	}
	carePayload := &publicpb.CareRequest{
		PlotId: 0, TargetFarmId: farmID, CropCycleId: plantResult.GetCropCycleId(),
		IncidentId: incidentID, CareType: publicpb.CareType_CARE_TYPE_WATER,
	}
	command := typedCommand(t, "care-friend-missing-admission", "care-friend-missing-admission-r", commonpb.CommandType_CARE, carePayload)
	payloadHash := sha256.Sum256(command.GetPayload())
	service := NewService(rig.registry, nil)
	_, err = service.careAuthority(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: command,
		Trusted: &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: visitor}},
	}, &trusted.VerifiedCommand{Decoded: carePayload, RouteKey: farmID, PayloadHash: payloadHash[:]}, time.UnixMilli(ingressTestNowMs+10_000))
	if err == nil {
		t.Fatal("friend CARE without Pair ActionAdmission must fail closed")
	}
}
