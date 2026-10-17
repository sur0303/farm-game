package main

import (
	"context"
	"strings"
	"testing"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	farmdomain "farmgame/internal/farm/domain"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func TestTheftAuditV1V2AggregateAndOracleValuesAreEquivalent(t *testing.T) {
	const (
		farmID   = uint64(930001)
		playerID = uint64(930002)
		version  = uint64(41)
	)
	farm := theftAuditFixtureFarm(farmID)
	player := theftAuditFixturePlayer(playerID)
	header, facts := theftAuditOpportunityBytes(t, farm.GetCore().GetFarmVersion())

	farmV1 := authoritystore.ModelFieldReadResult{
		ActorID: farmID, RecordVersion: version, ModelLayout: authoritystore.ModelLayoutV1, Found: true,
		ModelFields: authoritystore.ModelState{
			"record": auditProtoBytes(t, farm),
			farmdomain.FarmOpportunityHeaderModelField: header,
			farmdomain.FarmOpportunityFactsModelField:  facts,
		},
	}
	farmV2Fields, err := farmactormodelv2.Encode(farm, header, facts)
	if err != nil {
		t.Fatal(err)
	}
	farmV2 := authoritystore.ModelFieldReadResult{
		ActorID: farmID, RecordVersion: version, ModelLayout: authoritystore.ModelLayoutV2, Found: true, ModelFields: farmV2Fields,
	}
	playerV1 := authoritystore.ModelFieldReadResult{
		ActorID: playerID, RecordVersion: version, ModelLayout: authoritystore.ModelLayoutV1, Found: true,
		ModelFields: authoritystore.ModelState{"record": auditProtoBytes(t, player)},
	}
	playerV2Fields, err := playeractormodelv2.Encode(player)
	if err != nil {
		t.Fatal(err)
	}
	playerV2 := authoritystore.ModelFieldReadResult{
		ActorID: playerID, RecordVersion: version, ModelLayout: authoritystore.ModelLayoutV2, Found: true, ModelFields: playerV2Fields,
	}

	decodedFarmV1, err := decodeTheftAuditActorRow(actorrouting.DomainFarm, farmV1, farmID)
	if err != nil {
		t.Fatal(err)
	}
	decodedFarmV2, err := decodeTheftAuditActorRow(actorrouting.DomainFarm, farmV2, farmID)
	if err != nil {
		t.Fatal(err)
	}
	decodedPlayerV1, err := decodeTheftAuditActorRow(actorrouting.DomainPlayer, playerV1, playerID)
	if err != nil {
		t.Fatal(err)
	}
	decodedPlayerV2, err := decodeTheftAuditActorRow(actorrouting.DomainPlayer, playerV2, playerID)
	if err != nil {
		t.Fatal(err)
	}
	if decodedFarmV1.RecordVersion != version || decodedFarmV2.RecordVersion != version ||
		decodedPlayerV1.RecordVersion != version || decodedPlayerV2.RecordVersion != version {
		t.Fatal("exact authority RecordVersion was not retained")
	}
	if !proto.Equal(decodedFarmV1.Farm, decodedFarmV2.Farm) || !proto.Equal(decodedPlayerV1.Player, decodedPlayerV2.Player) {
		t.Fatal("v1 and v2 reconstructed aggregates differ")
	}

	cycle := decodedFarmV2.Farm.GetCore().GetCropCycles()[0]
	if decodedFarmV2.Farm.GetCore().GetFarmVersion() != 17 || cycle.GetBaseYield() != 100 ||
		cycle.GetStealableLimit() != 20 || cycle.GetStolenTotal() != 20 ||
		len(decodedFarmV2.Farm.GetRecentOperationReceipts()) != 1 ||
		decodedFarmV2.Farm.GetRecentOperationReceipts()[0].GetOperationId() != "farm-participant-receipt" ||
		len(decodedFarmV2.Farm.GetRecentTheftAttempts()) != 1 ||
		decodedFarmV2.Farm.GetRecentTheftAttempts()[0].GetVisitorPlayerId() != playerID {
		t.Fatalf("farm theft oracle carriers not reconstructed: %+v", decodedFarmV2.Farm)
	}
	stacks := decodedPlayerV2.Player.GetInventory().GetStacks()
	if len(stacks) != 1 || stacks[0].GetItemId() != cropcatalog.CarrotItemID || stacks[0].GetCount() != 18 ||
		len(decodedPlayerV2.Player.GetRecentOperationReceipts()) != 1 ||
		decodedPlayerV2.Player.GetRecentOperationReceipts()[0].GetOperationId() != "visitor-participant-receipt" {
		t.Fatalf("player inventory/receipt oracle carriers not reconstructed: %+v", decodedPlayerV2.Player)
	}
}

func TestTheftAuditRejectsPartialMixedMalformedAndUnknownLayouts(t *testing.T) {
	const (
		farmID   = uint64(940001)
		playerID = uint64(940002)
	)
	farm := theftAuditFixtureFarm(farmID)
	header, facts := theftAuditOpportunityBytes(t, farm.GetCore().GetFarmVersion())
	farmFields, err := farmactormodelv2.Encode(farm, header, facts)
	if err != nil {
		t.Fatal(err)
	}
	playerFields, err := playeractormodelv2.Encode(theftAuditFixturePlayer(playerID))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		domain  actorrouting.RouteDomain
		actorID uint64
		row     authoritystore.ModelFieldReadResult
	}{
		{name: "player v2 partial", domain: actorrouting.DomainPlayer, actorID: playerID, row: mutatedTheftAuditRow(playerID, authoritystore.ModelLayoutV2, playerFields, func(state authoritystore.ModelState) { delete(state, "inventory") })},
		{name: "player v2 retained record", domain: actorrouting.DomainPlayer, actorID: playerID, row: mutatedTheftAuditRow(playerID, authoritystore.ModelLayoutV2, playerFields, func(state authoritystore.ModelState) { state["record"] = []byte{1} })},
		{name: "player v2 malformed", domain: actorrouting.DomainPlayer, actorID: playerID, row: mutatedTheftAuditRow(playerID, authoritystore.ModelLayoutV2, playerFields, func(state authoritystore.ModelState) { state["inventory"] = []byte("bad-envelope") })},
		{name: "farm v2 partial", domain: actorrouting.DomainFarm, actorID: farmID, row: mutatedTheftAuditRow(farmID, authoritystore.ModelLayoutV2, farmFields, func(state authoritystore.ModelState) { delete(state, "plots") })},
		{name: "farm v2 retained record", domain: actorrouting.DomainFarm, actorID: farmID, row: mutatedTheftAuditRow(farmID, authoritystore.ModelLayoutV2, farmFields, func(state authoritystore.ModelState) { state["record"] = []byte{1} })},
		{name: "farm v2 malformed", domain: actorrouting.DomainFarm, actorID: farmID, row: mutatedTheftAuditRow(farmID, authoritystore.ModelLayoutV2, farmFields, func(state authoritystore.ModelState) { state["opportunity_facts"] = []byte("bad-envelope") })},
		{name: "player v1 mixed", domain: actorrouting.DomainPlayer, actorID: playerID, row: mutatedTheftAuditRow(playerID, authoritystore.ModelLayoutV1, authoritystore.ModelState{"record": auditProtoBytes(t, theftAuditFixturePlayer(playerID))}, func(state authoritystore.ModelState) { state["profile"] = playerFields["profile"] })},
		{name: "farm v1 mixed", domain: actorrouting.DomainFarm, actorID: farmID, row: mutatedTheftAuditRow(farmID, authoritystore.ModelLayoutV1, authoritystore.ModelState{"record": auditProtoBytes(t, farm), farmdomain.FarmOpportunityHeaderModelField: header, farmdomain.FarmOpportunityFactsModelField: facts}, func(state authoritystore.ModelState) { state["farm_meta"] = farmFields["farm_meta"] })},
		{name: "unknown layout", domain: actorrouting.DomainPlayer, actorID: playerID, row: mutatedTheftAuditRow(playerID, authoritystore.ModelLayoutUnknown, playerFields, func(authoritystore.ModelState) {})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, decodeErr := decodeTheftAuditActorRow(test.domain, test.row, test.actorID); decodeErr == nil {
				t.Fatal("invalid layout decoded and could reach business-value assertions")
			}
		})
	}
}

func TestTheftAuditReaderRequestsV1AlternativeAndExactV2CoreOnce(t *testing.T) {
	const playerID = uint64(950001)
	fields, err := playeractormodelv2.Encode(theftAuditFixturePlayer(playerID))
	if err != nil {
		t.Fatal(err)
	}
	source := &capturingModelFieldReader{row: authoritystore.ModelFieldReadResult{
		ActorID: playerID, RecordVersion: 73, ModelLayout: authoritystore.ModelLayoutV2, Found: true, ModelFields: fields,
	}}
	reader, err := newAuditActorReader(source)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reader.Load(context.Background(), actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RecordVersion != 73 || loaded.Player.GetPlayerId() != playerID || source.calls != 1 {
		t.Fatalf("loaded=%+v calls=%d", loaded, source.calls)
	}
	if got, want := strings.Join(source.fieldNames, ","), "record,compendium,economy,inventory,mail_claim_state,pet,profile,progression,task"; got != want {
		t.Fatalf("requested fields=%q want=%q", got, want)
	}
}

type capturingModelFieldReader struct {
	row        authoritystore.ModelFieldReadResult
	fieldNames []string
	calls      int
}

func (r *capturingModelFieldReader) BatchReadModelFields(_ context.Context, _ actorrouting.RouteDomain, _ []uint64, fieldNames []string) ([]authoritystore.ModelFieldReadResult, error) {
	r.calls++
	r.fieldNames = append([]string(nil), fieldNames...)
	return []authoritystore.ModelFieldReadResult{r.row}, nil
}

func theftAuditFixtureFarm(farmID uint64) *farmpb.FarmRecord {
	return &farmpb.FarmRecord{
		FarmId: farmID,
		Core: &farmpb.FarmCore{FarmId: farmID, FarmVersion: 17, CropCycles: []*farmpb.CropCycle{{
			CropCycleId: "theft-hot-cycle", PlotIndex: 2, BaseYield: 100, StealableLimit: 20, StolenTotal: 20,
		}}},
		RecentOperationReceipts: []*farmpb.FarmOperationReceipt{{
			OperationId: "farm-participant-receipt", Status: commonpb.CommandStatus_SUCCEEDED,
			ErrorCode: commonpb.ErrorCode_OK, ResultPayload: []byte("farm-result"), PayloadHash: []byte("farm-hash"),
		}},
		RecentTheftAttempts: []*farmpb.TheftAttemptReceipt{{
			VisitorPlayerId: farmID + 1, CropCycleId: "theft-hot-cycle", OperationId: "theft-attempt", CommittedAtMs: 100,
		}},
	}
}

func theftAuditFixturePlayer(playerID uint64) *playerpb.PlayerRecord {
	return &playerpb.PlayerRecord{
		PlayerId: playerID, EconomyVersion: 8,
		Inventory: &economypb.Inventory{Stacks: []*economypb.ItemStack{{ItemId: cropcatalog.CarrotItemID, Count: 18}}},
		RecentOperationReceipts: []*playerpb.PlayerOperationReceipt{{
			OperationId: "visitor-participant-receipt", Status: commonpb.CommandStatus_SUCCEEDED,
			ErrorCode: commonpb.ErrorCode_OK, ResultPayload: []byte("visitor-result"), PayloadHash: []byte("visitor-hash"),
		}},
	}
}

func theftAuditOpportunityBytes(t *testing.T, farmVersion uint64) ([]byte, []byte) {
	t.Helper()
	header, err := farmdomain.MarshalFarmOpportunityHeaderV2(farmdomain.FarmOpportunityHeaderV2{
		SchemaVersion: farmdomain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: farmVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := farmdomain.MarshalFarmOpportunityFactsV1(farmdomain.FarmOpportunityFactsV1{
		SchemaVersion: farmdomain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: farmVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	return header, facts
}

func auditProtoBytes(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mutatedTheftAuditRow(actorID uint64, layout authoritystore.ModelLayoutVersion, source authoritystore.ModelState, mutate func(authoritystore.ModelState)) authoritystore.ModelFieldReadResult {
	state := make(authoritystore.ModelState, len(source))
	for name, value := range source {
		state[name] = append([]byte(nil), value...)
	}
	mutate(state)
	return authoritystore.ModelFieldReadResult{ActorID: actorID, RecordVersion: 1, ModelLayout: layout, Found: true, ModelFields: state}
}
