package participant

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"google.golang.org/protobuf/proto"
)

func TestRedisCareDependencyVerifierProvesDirectAndWorkerLease(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.UnixMilli(1_800_000_000_000)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("test-realm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608050001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	const farmID = uint64(7001)
	const playerID = uint64(9001)
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
	if err != nil {
		t.Fatal(err)
	}
	store, err := authority.NewStore(client, nil, authority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}

	orderID, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, strconv.FormatUint(farmID, 10), "incident-verifier", 12)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := workidentity.StableOpaqueID("care-cycle", "verifier")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &contractspb.CareResolutionSnapshot{
		SourceFarmId: farmID, SourceOwnerPlayerId: 7002, CarerPlayerId: playerID, PlotId: 1,
		CropCycleId: cycleID, IncidentId: "incident-verifier", CareType: contractspb.CareType_CARE_TYPE_WATER,
		CareContext: contractspb.CareContext_CARE_CONTEXT_FRIEND, ResolvedAtMs: now.UnixMilli(),
		CommittedPlotVersion: 8, CommittedFarmVersion: 12, CareRewardRuleVersion: 2, RewardExp: 5,
		FriendInteractionEligible: true,
	}
	resolutionHash, err := CareResolutionHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.CareResolutionHash = append([]byte(nil), resolutionHash[:]...)
	payload := &contractspb.ApplyCareRewardPayload{CarerPlayerId: playerID, CareRewardOrderId: orderID,
		CareResolutionSnapshot: snapshot, CareResolutionHash: append([]byte(nil), snapshot.CareResolutionHash...)}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	orderKey, err := store.Keys().SourceOrder(bucket, "CARE_REWARD", orderID)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := store.Keys().SourceDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	workRef, err := workidentity.EncodeWorkRefV1("CARE_REWARD", orderID)
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]interface{}{
		"schema_version": "1", "work_id": orderID, "work_type": "CARE_REWARD", "placement_kind": "SOURCE_PERSISTENCE_BUCKET",
		"bucket_version": "1", "bucket_id": strconv.FormatUint(uint64(bucket.VirtualShard()), 10), "source_authority_kind": "FARM_ACTOR",
		"canonical_source_authority_id": strconv.FormatUint(farmID, 10), "authority_layout_version": bucket.LayoutVersion(), "actor_route_domain": "FARM",
		"subject_player_id": strconv.FormatUint(playerID, 10), "operation_id": "care-operation", "payload_hash": hex.EncodeToString(payloadHash[:]),
		"state": "PENDING", "work_version": "1", "current_step": "", "typed_payload": string(payloadBytes),
		"next_due_at_ms": strconv.FormatInt(now.UnixMilli(), 10), "retry_count": "0", "last_error_class": "",
		"deadline_at_ms": strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10), "max_retry_count": "8", "reconcile_required_at_ms": "0",
		"lease_holder_id": "", "lease_holder_boot_id": "", "lease_epoch": "0", "lease_token_hash": "", "lease_expire_at_ms": "0",
		"created_at_ms": strconv.FormatInt(now.UnixMilli(), 10), "updated_at_ms": strconv.FormatInt(now.UnixMilli(), 10), "completed_at_ms": "0",
	}
	if err := client.HSet(context.Background(), orderKey.String(), fields).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), dueKey.String(), redis.Z{Score: float64(now.UnixMilli()), Member: workRef}).Err(); err != nil {
		t.Fatal(err)
	}
	verifier, err := NewRedisCareDependencyVerifier(store, func(uint64) (rediskey.AuthorityBucketRef, error) { return bucket, nil })
	if err != nil {
		t.Fatal(err)
	}
	farmDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	work := &contractspb.WorkAddress{WorkId: orderID, WorkType: contractspb.WorkType_WORK_TYPE_CARE_REWARD,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET, BucketVersion: 1,
		BucketId: bucket.VirtualShard(), SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:        contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR,
			CanonicalAuthorityId: strconv.FormatUint(farmID, 10), AuthorityLayoutVersion: bucket.LayoutVersion(), ActorRouteDomain: &farmDomain,
		}}
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, orderID, identity.StepApplyCareReward, 1)
	if err != nil {
		t.Fatal(err)
	}
	call := &contracts.ParticipantCall{Spec: spec, Work: work, CallerKind: contracts.ParticipantCallerCoordinator,
		ParticipantStep: string(identity.StepApplyCareReward), InvocationID: invocation, OperationID: "care-operation",
		SubjectPlayerID: playerID, ExpectedWorkVersion: 1, Payload: payload, CanonicalStepPayloadHash: payloadHash}
	proof, err := verifier.VerifyCareDependency(context.Background(), call)
	if err != nil || proof.ExecutionMode != CareDependencyExecutionModeCoordinator || proof.OrderState != CareRewardOrderStatePending {
		t.Fatalf("direct proof=%+v err=%v", proof, err)
	}

	claim, err := store.WorkLease().ClaimWorkWithBoot(context.Background(), bucket, "CARE_REWARD", orderID, "worker-1", "boot-1", "token-1", 30_000)
	if err != nil || claim.Status != authority.WorkClaimClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	workerCall := *call
	workerCall.CallerKind = contracts.ParticipantCallerWorkerRecovery
	workerCall.OperationID = ""
	workerCall.SubjectPlayerID = 0
	workerCall.CallerInstanceID = "worker-1"
	workerCall.CallerBootID = "boot-1"
	workerCall.LeaseOwnerToken = "token-1"
	workerCall.LeaseEpoch = claim.LeaseEpoch
	workerProof, err := verifier.VerifyCareDependency(context.Background(), &workerCall)
	if err != nil || workerProof.ExecutionMode != CareDependencyExecutionModeRecoveryLease || !workerProof.LeaseValid {
		t.Fatalf("worker proof=%+v err=%v", workerProof, err)
	}
	server.SetTime(time.UnixMilli(claim.LeaseExpireAtMs + 1))
	if _, err := verifier.VerifyCareDependency(context.Background(), &workerCall); err == nil {
		t.Fatal("expired Worker lease was admitted")
	}

	if err := client.HSet(context.Background(), orderKey.String(), "payload_hash", fmt.Sprintf("%064x", 99)).Err(); err != nil {
		t.Fatal(err)
	}
	server.SetTime(now)
	if _, err := verifier.VerifyCareDependency(context.Background(), call); err == nil {
		t.Fatal("payload hash mismatch was admitted")
	}
}
