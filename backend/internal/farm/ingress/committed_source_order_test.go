package ingress

import (
	"bytes"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func ingressEnvelopeFixture(t *testing.T) (rediskey.AuthorityBucketRef, *contractspb.CreatedSourceOrderEnvelope, authorityOrderTuple) {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "committed-envelope")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: ns, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("committed-envelope",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130001}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	playerID := uint64(8801)
	farmID := uint64(8801)
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
	if err != nil {
		t.Fatal(err)
	}
	workflowID, err := workidentity.WorkflowID(farmID, "PLANT", "plant-envelope-op")
	if err != nil {
		t.Fatal(err)
	}
	reservationID, err := identity.SeedReservationID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := identity.CropCycleID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	definition, ok := cropcatalog.ByDefinitionID(cropcatalog.CarrotDefinitionID)
	if !ok {
		t.Fatal("carrot missing")
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.CommitPlantPayload{
		OwnerPlayerId: playerID, FarmId: farmID, PlotId: 1, ExpectedPlotVersion: 1,
		ReservationId: reservationID, ReserveSeedResultHash: bytes.Repeat([]byte{9}, 32),
		CropCycleId: cycleID, CropRuleSnapshot: snapshot,
		CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := sourceorder.PlantPayloadHash(payload)
	if err != nil {
		t.Fatal(err)
	}
	envelope := &contractspb.CreatedSourceOrderEnvelope{
		Work: &contractspb.WorkAddress{
			WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_PLANT,
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
			BucketVersion: 1, BucketId: bucket.VirtualShard(),
			SourceAuthority: &contractspb.SourceAuthorityRef{
				AuthorityKind:        contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
				CanonicalAuthorityId: "8801", AuthorityLayoutVersion: bucket.LayoutVersion(),
				ActorRouteDomain: ptrRouteDomain(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER),
			},
		},
		WorkVersion: 1, OperationId: "plant-envelope-op", SubjectPlayerId: playerID,
		TypedPayload: raw, PayloadHash: append([]byte(nil), hash[:]...), DueAtMs: 1000,
	}
	return bucket, envelope, authorityOrderTuple{playerID: playerID, farmID: farmID, workflowID: workflowID}
}

type authorityOrderTuple struct {
	playerID, farmID uint64
	workflowID       string
}

func TestAttachedPlayerSourceOrderValidatesEnvelope(t *testing.T) {
	bucket, envelope, tuple := ingressEnvelopeFixture(t)
	order, err := attachedPlayerSourceOrder(envelope, bucket, tuple.playerID, tuple.workflowID, "PLANT")
	if err != nil {
		t.Fatal(err)
	}
	if order.OrderID != tuple.workflowID || order.OrderType != "PLANT" || order.SubjectPlayerID != tuple.playerID ||
		order.OperationID != "plant-envelope-op" || order.DueAtMs != 1000 || len(order.Payload) == 0 {
		t.Fatalf("order=%+v", order)
	}
	if _, err := sourceorder.DecodeCommitPlantPayload(order.OrderID, order.Payload); err != nil {
		t.Fatalf("decoded payload rejected: %v", err)
	}
}

func TestAttachedPlayerSourceOrderRejectsTampering(t *testing.T) {
	bucket, envelope, tuple := ingressEnvelopeFixture(t)
	workID := envelope.GetWork().GetWorkId()
	tampered := proto.Clone(envelope).(*contractspb.CreatedSourceOrderEnvelope)
	tampered.PayloadHash = append([]byte(nil), envelope.PayloadHash...)
	tampered.PayloadHash[0] ^= 0xff
	if _, err := attachedPlayerSourceOrder(tampered, bucket, tuple.playerID, workID, "PLANT"); err == nil {
		t.Fatal("tampered payload hash accepted")
	}
	badSubject := proto.Clone(envelope).(*contractspb.CreatedSourceOrderEnvelope)
	badSubject.SubjectPlayerId = tuple.playerID + 1
	if _, err := attachedPlayerSourceOrder(badSubject, bucket, tuple.playerID, workID, "PLANT"); err == nil {
		t.Fatal("wrong subject accepted")
	}
	wrongType := proto.Clone(envelope).(*contractspb.CreatedSourceOrderEnvelope)
	wrongType.Work.WorkType = contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT
	if _, err := attachedPlayerSourceOrder(wrongType, bucket, tuple.playerID, workID, "PLANT"); err == nil {
		t.Fatal("wrong work type accepted")
	}
	badActor := proto.Clone(envelope).(*contractspb.CreatedSourceOrderEnvelope)
	badActor.Work.SourceAuthority.CanonicalAuthorityId = "9999"
	if _, err := attachedPlayerSourceOrder(badActor, bucket, tuple.playerID, workID, "PLANT"); err == nil {
		t.Fatal("wrong source actor accepted")
	}
}

func ptrRouteDomain(v logicrpcpb.RouteDomain) *logicrpcpb.RouteDomain { return &v }
