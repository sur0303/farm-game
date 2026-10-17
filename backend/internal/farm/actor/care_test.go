package actor

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/friend"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

func careTestHash(operationID string) []byte {
	hash := sha256.Sum256([]byte(operationID))
	return hash[:]
}

func careTestAdmittedPermit(farmID, visitor uint64, operationID string, payloadHash []byte, plotID uint32, cycleID, incidentID string, careType domain.CareType, relationEpoch uint64, nowMs int64) *friend.ActionPermitSnapshot {
	binding := CareTargetBindingHash(plotID, cycleID, incidentID, careType)
	return &friend.ActionPermitSnapshot{
		PermitID: "test-permit-" + operationID, RelationID: "test-relation",
		RelationEpoch: relationEpoch, VisitorPlayerID: visitor, OwnerPlayerID: farmID,
		TargetFarmID: farmID, OperationID: operationID, Scope: friend.ActionScopeCare,
		PayloadHash: append([]byte(nil), payloadHash...), TargetBindingHash: binding,
		IssuedAtAuthorityMs: nowMs, ExpireAtAuthorityMs: nowMs + 30_000,
		ExecutionAcceptedAtAuthorityMs: nowMs,
	}
}

func submitCareTest(t *testing.T, registry *Registry, farmID, resolver uint64, cycleID, operationID string, incidentID string, careType domain.CareType, contextValue contractspb.CareContext, relationEpoch uint64, nowMs int64) (*ActionResult, error) {
	t.Helper()
	payloadHash := careTestHash(operationID)
	var permit *friend.ActionPermitSnapshot
	if contextValue == contractspb.CareContext_CARE_CONTEXT_FRIEND {
		permit = careTestAdmittedPermit(farmID, resolver, operationID, payloadHash, 1, cycleID, incidentID, careType, relationEpoch, nowMs)
	}
	value, err := registry.Submit(context.Background(), farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: operationID, PayloadHash: payloadHash, ResolverPlayerID: resolver,
			PlotID: 1, CycleID: cycleID, IncidentID: incidentID,
			CareType: careType, Context: contextValue, RelationEpoch: relationEpoch,
			AdmittedPermit: permit, TrustedNowMs: nowMs,
		})
	})
	if err != nil {
		return nil, err
	}
	return value.(*ActionResult), nil
}

func plantCareFixture(t *testing.T, farmID uint64) (*ar1Rig, string, string, string) {
	t.Helper()
	rig := ar1MemoryRig(t, farmID)
	plant := submitParticipantCall(t, rig.registry, farmID, participantTestCall(t, farmID, "plant-care", 1, 1), participantTestNowMs)
	if plant.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant outcome=%v", plant)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := decodeParticipantProof(record.GetRecentOperationReceipts()[0].GetResultPayload())
	if err != nil || proof.careSnapshot == nil || len(proof.careSnapshot.GetIncidents()) != 2 {
		t.Fatalf("plant proof=%v err=%v", proof, err)
	}
	return rig, proof.result.GetCropCycleId(), proof.careSnapshot.GetIncidents()[0].GetIncidentId(), proof.careSnapshot.GetIncidents()[1].GetIncidentId()
}

func TestAuthorityCareOwnerAndFriendResolveIndependentIncidents(t *testing.T) {
	rig, cycleID, waterID, pestID := plantCareFixture(t, 1301)
	ctx := context.Background()
	// The crop is mature at +60s, but maturity itself does not prohibit Care;
	// only Harvest/cycle end closes the Incident.
	nowMs := participantTestNowMs + 70_000
	owner, err := rig.registry.Submit(ctx, 1301, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: "care-owner-water", PayloadHash: careTestHash("care-owner-water"), ResolverPlayerID: 1301,
			PlotID: 1, CycleID: cycleID, IncidentID: waterID, CareType: domain.CareTypeWater,
			Context: contractspb.CareContext_CARE_CONTEXT_OWNER, TrustedNowMs: nowMs,
		})
	})
	if err != nil || owner.(*ActionResult).Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("owner=%v err=%v", owner, err)
	}
	ownerAction := owner.(*ActionResult)
	if ownerAction.CareResult == nil || ownerAction.CareResult.CareRewardOrderID == "" || ownerAction.Change == nil {
		t.Fatalf("owner care result=%+v change=%v", ownerAction.CareResult, ownerAction.Change)
	}
	if order := ownerAction.CommittedSourceOrder; order == nil || order.OrderID != ownerAction.CareResult.CareRewardOrderID ||
		order.OperationID != "care-owner-water" || order.SubjectPlayerID != 1301 || len(order.Payload) == 0 {
		t.Fatalf("owner committed source order=%+v", order)
	}
	friendHash := careTestHash("care-friend-pest")
	friend, err := rig.registry.Submit(ctx, 1301, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: "care-friend-pest", PayloadHash: friendHash, ResolverPlayerID: 77,
			PlotID: 1, CycleID: cycleID, IncidentID: pestID, CareType: domain.CareTypeRemovePest,
			Context: contractspb.CareContext_CARE_CONTEXT_FRIEND, RelationEpoch: 9,
			AdmittedPermit: careTestAdmittedPermit(1301, 77, "care-friend-pest", friendHash, 1, cycleID, pestID, domain.CareTypeRemovePest, 9, nowMs),
			TrustedNowMs:   nowMs,
		})
	})
	if err != nil || friend.(*ActionResult).Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("friend=%v err=%v", friend, err)
	}
	if friend.(*ActionResult).CareResult == nil || !friend.(*ActionResult).CareResult.FriendInteractionEligible {
		t.Fatalf("friend result=%+v", friend.(*ActionResult).CareResult)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(ctx, 1301)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 3 || len(record.GetRecentOperationReceipts()) != 3 {
		t.Fatalf("record version=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetRecentOperationReceipts()))
	}
	resolved := 0
	for _, receipt := range record.GetRecentOperationReceipts() {
		proof, found, decodeErr := decodeCareProof(receipt.GetResultPayload())
		if !found {
			continue
		}
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if proof.outcome == domain.CareOutcomeResolved {
			resolved++
		}
	}
	if resolved != 2 {
		t.Fatalf("resolved care proofs=%d", resolved)
	}
}

func TestAuthorityCareSameIncidentTwentyConcurrentOneSuccess(t *testing.T) {
	rig, cycleID, waterID, _ := plantCareFixture(t, 1302)
	// Keep the runtime mailbox bounded but above this focused contention
	// vector so every admitted request reaches the same serialized Incident
	// decision and can observe SUCCEEDED/NO_CHANGE rather than admission BUSY.
	rig.registry.Stop()
	rig.registry = NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 64})
	t.Cleanup(rig.registry.Stop)
	nowMs := participantTestNowMs + 30_000
	type result struct {
		action *ActionResult
		err    error
	}
	results := make(chan result, 20)
	var wg sync.WaitGroup
	for index := 0; index < 20; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := rig.registry.Submit(context.Background(), 1302, func(ctx context.Context, actor *FarmActor) (any, error) {
				operationID := "care-race-" + string(rune('a'+index))
				payloadHash := careTestHash(operationID)
				resolver := uint64(100 + index)
				return actor.ResolveCareIncident(ctx, CareResolutionRequest{
					OperationID: operationID, PayloadHash: payloadHash, ResolverPlayerID: resolver,
					PlotID: 1, CycleID: cycleID, IncidentID: waterID, CareType: domain.CareTypeWater,
					Context: contractspb.CareContext_CARE_CONTEXT_FRIEND, RelationEpoch: 1,
					AdmittedPermit: careTestAdmittedPermit(1302, resolver, operationID, payloadHash, 1, cycleID, waterID, domain.CareTypeWater, 1, nowMs),
					TrustedNowMs:   nowMs,
				})
			})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{action: value.(*ActionResult)}
		}()
	}
	wg.Wait()
	close(results)
	succeeded, noChange := 0, 0
	for value := range results {
		if value.err != nil {
			t.Fatal(value.err)
		}
		switch value.action.Status {
		case commonpb.CommandStatus_SUCCEEDED:
			succeeded++
		case commonpb.CommandStatus_NO_CHANGE:
			noChange++
		default:
			t.Fatalf("care race status=%v", value.action.Status)
		}
	}
	if succeeded != 1 || noChange != 19 {
		t.Fatalf("succeeded=%d no_change=%d", succeeded, noChange)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1302)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 2 {
		t.Fatalf("farm version=%d want 2", record.GetCore().GetFarmVersion())
	}
	resolved, noChangeProofs := 0, 0
	for _, receipt := range record.GetRecentOperationReceipts() {
		proof, found, decodeErr := decodeCareProof(receipt.GetResultPayload())
		if !found {
			continue
		}
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if proof.outcome == domain.CareOutcomeResolved {
			resolved++
		} else {
			noChangeProofs++
		}
	}
	if resolved != 1 || noChangeProofs != 19 {
		t.Fatalf("care proofs resolved=%d no_change=%d", resolved, noChangeProofs)
	}
}

func TestAuthorityCareRejectsEarlyAndAfterHarvest(t *testing.T) {
	rig, cycleID, waterID, _ := plantCareFixture(t, 1303)
	if _, err := rig.registry.Submit(context.Background(), 1303, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: "care-early", PayloadHash: careTestHash("care-early"), ResolverPlayerID: 1303,
			PlotID: 1, CycleID: cycleID, IncidentID: waterID, CareType: domain.CareTypeWater,
			Context: contractspb.CareContext_CARE_CONTEXT_OWNER, TrustedNowMs: participantTestNowMs + 1,
		})
	}); err == nil {
		t.Fatal("early CARE succeeded")
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1303)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 1 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("early changed record version=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetRecentOperationReceipts()))
	}
	plantVersion := record.GetCore().GetPlots()[0].GetPlotVersion()
	harvestCall := participantHarvestCall(t, 1303, "harvest-care-end", cycleID, 1, plantVersion)
	harvest, err := submitHarvestParticipantCall(t, rig.registry, 1303, harvestCall, participantTestNowMs+61_000)
	if err != nil || harvest.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("harvest=%+v err=%v", harvest, err)
	}
	if _, err := submitCareTest(t, rig.registry, 1303, 1303, cycleID, "care-after-harvest", waterID, domain.CareTypeWater, contractspb.CareContext_CARE_CONTEXT_OWNER, 0, participantTestNowMs+62_000); err == nil {
		t.Fatal("CARE after harvest succeeded")
	}
}

func TestAuthorityCareResponseLossReplaysWithoutSecondOrder(t *testing.T) {
	rig, cycleID, waterID, _ := plantCareFixture(t, 1304)
	operationID := "care-response-loss"
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	_, err := rig.registry.Submit(context.Background(), 1304, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: operationID, PayloadHash: careTestHash(operationID), ResolverPlayerID: 1304,
			PlotID: 1, CycleID: cycleID, IncidentID: waterID, CareType: domain.CareTypeWater,
			Context: contractspb.CareContext_CARE_CONTEXT_OWNER, TrustedNowMs: participantTestNowMs + 30_000,
		})
	})
	if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("first error=%v", err)
	}
	waitForState(t, rig.registry, 1304, activation.Active)
	replayed, err := rig.registry.Submit(context.Background(), 1304, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, CareResolutionRequest{
			OperationID: operationID, PayloadHash: careTestHash(operationID), ResolverPlayerID: 1304,
			PlotID: 1, CycleID: cycleID, IncidentID: waterID, CareType: domain.CareTypeWater,
			Context: contractspb.CareContext_CARE_CONTEXT_OWNER, TrustedNowMs: participantTestNowMs + 31_000,
		})
	})
	if err != nil || !replayed.(*ActionResult).Replayed || replayed.(*ActionResult).Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("replayed=%v err=%v", replayed, err)
	}
	if got := rig.provider.Metrics().Snapshot().ResolveCommitted; got == 0 {
		t.Fatal("authority unknown was not resolved as committed")
	}
}
