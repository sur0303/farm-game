package actor

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/protobuf/proto"
)

func TestProductionCareAndHarvestBuildersMintSemanticCommitEvidence(t *testing.T) {
	const (
		farmID        = uint64(42)
		playerID      = uint64(9001)
		committedAtMs = int64(1_800_000_000_000)
	)
	careRequest := CareResolutionRequest{
		OperationID: "production-care-proof", ResolverPlayerID: playerID,
		PlotID: 1, CycleID: "production-care-cycle", IncidentID: "production-care-incident",
		CareType: domain.CareTypeWater, Context: contractspb.CareContext_CARE_CONTEXT_FRIEND,
	}
	careState := domain.FarmState{FarmID: farmID, OwnerPlayerID: farmID, FarmVersion: 7,
		Cycles: []domain.CropCycle{{CycleID: careRequest.CycleID, PlotID: careRequest.PlotID,
			CarePlan: domain.CarePlanSnapshot{CareRuleVersion: 3}}}}
	careDecision := domain.CareDecision{
		PlotID: careRequest.PlotID, CycleID: careRequest.CycleID, IncidentID: careRequest.IncidentID,
		CareType: domain.CareTypeWater, Outcome: domain.CareOutcomeResolved,
		ResolvedByPlayerID: playerID, ResolvedAtMs: committedAtMs,
		RewardObligationCreated: true, CommittedPlotVersion: 5, CommittedFarmVersion: 8,
	}
	_, careOrder, _, err := buildCareRewardOrder(farmID, careRequest, careState, careDecision)
	if err != nil {
		t.Fatal(err)
	}
	assertProductionBuilderCommitEvidence(t, careOrder, farmID, committedAtMs,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)

	harvestOrderID, err := identity.SourceOrderID(identity.WorkTypeHarvest, identity.SourceKindFarmActor,
		"42", "production-harvest-proof", 1)
	if err != nil {
		t.Fatal(err)
	}
	harvestDecision := &contractspb.HarvestDecision{
		WorkflowId: harvestOrderID, OwnerPlayerId: playerID, FarmId: farmID, PlotId: 1,
		CropCycleId: "production-harvest-cycle", CropDefinitionId: 2, RewardItemId: 3,
		RewardQuantity: 4, RewardExp: 5, CropRuleVersion: 6,
		HarvestDecisionHash: make([]byte, sha256.Size),
	}
	harvestOrder, err := buildHarvestRewardOrder(playerID, harvestOrderID, "production-harvest-proof",
		make([]byte, sha256.Size), harvestDecision, committedAtMs)
	if err != nil {
		t.Fatal(err)
	}
	assertProductionBuilderCommitEvidence(t, harvestOrder, farmID, committedAtMs,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
}

func assertProductionBuilderCommitEvidence(t *testing.T, order authoritystore.AttachedOrder, farmID uint64,
	committedAtMs int64, command contractspb.ParticipantCommandType,
) {
	t.Helper()
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(order.Payload, payload); err != nil {
		t.Fatal(err)
	}
	semanticHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	if order.PayloadHash != authoritystore.PayloadHash(semanticHash) {
		t.Fatalf("production %s hash=%x, want semantic hash %x", order.OrderType, order.PayloadHash, semanticHash)
	}
	if rawHash := sha256.Sum256(order.Payload); order.PayloadHash == authoritystore.PayloadHash(rawHash) {
		t.Fatalf("production %s unexpectedly used raw protobuf SHA", order.OrderType)
	}

	calculator := ar1Calculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1",
		Now: func() time.Time { return time.UnixMilli(committedAtMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: address.Domain, VirtualShard: shard,
		HolderID: "farm-holder", HolderBootID: "farm-boot",
		AssignmentRevision: 7, WriterEpoch: 11, LeaseEpoch: 13,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: committedAtMs + 30_000,
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision, ExpectedWriterEpoch: authority.WriterEpoch,
		ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
	const claimID = "builder-proof-claim"
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "builder-proof-load", NewWriterClaimID: claimID,
		InitialModelFields: authoritystore.ModelState{"proof": []byte("initial")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
	operationID := order.OperationID + "-stamp"
	commitHash := authoritystore.PayloadHash(sha256.Sum256([]byte(operationID)))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID: order.SubjectPlayerID, StableCommandTypeName: "BUILDER_PROOF_TEST", OperationID: operationID,
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindFarmActor, CanonicalID: "42"},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("committed")}
	request := authoritystore.AuthorityCommitRequest{
		CommitRequestID: "builder-proof-commit", ActorAddress: address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: fence, ExpectedWriterClaimID: claimID, ExpectedRecordVersion: loaded.Snapshot.RecordVersion,
		IdempotencyKey: key, CanonicalPayloadHash: commitHash,
		CandidateState: authoritystore.ModelState{"proof": []byte(order.OrderType)},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "builder-proof-receipt", IdempotencyKey: key,
			CanonicalPayloadHash: commitHash, Outcome: outcome},
		OperationResult: outcome, AttachedOrders: []authoritystore.AttachedOrder{order},
	}
	result, err := store.CommitExact(context.Background(), request)
	if err != nil || result.Status != authoritystore.CommitCommitted || result.CommitStamp == nil {
		t.Fatalf("commit=%+v err=%v", result, err)
	}
	if _, err := sourceorder.NewDurableCommitEvidence(request, result, order.OrderID); err != nil {
		t.Fatalf("production %s order did not produce semantic durable evidence: %v", order.OrderType, err)
	}
}
