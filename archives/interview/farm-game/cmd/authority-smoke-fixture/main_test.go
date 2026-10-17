package main

import (
	"reflect"
	"strings"
	"testing"

	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	farmdomain "farmgame/internal/farm/domain"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func TestAuditV1V2BusinessValuesAreEquivalent(t *testing.T) {
	const (
		playerID = uint64(81001)
		farmID   = uint64(81002)
		plot     = uint64(3)
	)
	player := auditFixturePlayer(playerID)
	farm := auditFixtureFarm(farmID, int32(plot))
	headerBytes, factsBytes := auditFixtureOpportunityBytes(t, farm.GetCore().GetFarmVersion())

	playerV1 := authoritystore.ModelFieldReadResult{
		ActorID: playerID, Found: true, ModelLayout: authoritystore.ModelLayoutV1,
		ModelFields: authoritystore.ModelState{"record": auditMarshalProto(t, player)},
	}
	playerV2Fields, err := playeractormodelv2.Encode(player)
	if err != nil {
		t.Fatal(err)
	}
	playerV2 := authoritystore.ModelFieldReadResult{
		ActorID: playerID, Found: true, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: playerV2Fields,
	}
	farmV1 := authoritystore.ModelFieldReadResult{
		ActorID: farmID, Found: true, ModelLayout: authoritystore.ModelLayoutV1,
		ModelFields: authoritystore.ModelState{
			"record": auditMarshalProto(t, farm),
			farmdomain.FarmOpportunityHeaderModelField: headerBytes,
			farmdomain.FarmOpportunityFactsModelField:  factsBytes,
		},
	}
	farmV2Fields, err := farmactormodelv2.Encode(farm, headerBytes, factsBytes)
	if err != nil {
		t.Fatal(err)
	}
	farmV2 := authoritystore.ModelFieldReadResult{
		ActorID: farmID, Found: true, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: farmV2Fields,
	}

	decodedPlayerV1, err := decodeAuditPlayerRow(playerV1, playerID)
	if err != nil {
		t.Fatal(err)
	}
	decodedPlayerV2, err := decodeAuditPlayerRow(playerV2, playerID)
	if err != nil {
		t.Fatal(err)
	}
	decodedFarmV1, err := decodeAuditFarmRow(farmV1, farmID)
	if err != nil {
		t.Fatal(err)
	}
	decodedFarmV2, err := decodeAuditFarmRow(farmV2, farmID)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decodedPlayerV1, decodedPlayerV2) || !proto.Equal(decodedFarmV1.Record, decodedFarmV2.Record) {
		t.Fatal("v1 and v2 reconstructed aggregate records differ")
	}
	if decodedFarmV1.Header != decodedFarmV2.Header || !reflect.DeepEqual(decodedFarmV1.Facts, decodedFarmV2.Facts) {
		t.Fatal("v1 and v2 reconstructed opportunity projections differ")
	}

	v1 := deriveAuditBusinessValues(decodedPlayerV1, decodedFarmV1.Record, plot)
	v2 := deriveAuditBusinessValues(decodedPlayerV2, decodedFarmV2.Record, plot)
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("v1 values=%+v v2 values=%+v", v1, v2)
	}
	if v2.FarmVersion != 5 || v2.StolenTotal != 10 || v2.PlayerProduce != 510 ||
		v2.TaskStateVersion != 9 || v2.TaskGrowthCursor != 1 || v2.TaskClaimableCount != 0 ||
		v2.TaskTotalHarvest != 4 || !v2.AllDailyClaimed {
		t.Fatalf("CP-B authority values not reconstructed: %+v", v2)
	}
}

func TestAuditV2RejectsPartialMixedMalformedAndUnknown(t *testing.T) {
	const (
		playerID = uint64(82001)
		farmID   = uint64(82002)
	)
	playerFields, err := playeractormodelv2.Encode(auditFixturePlayer(playerID))
	if err != nil {
		t.Fatal(err)
	}
	headerBytes, factsBytes := auditFixtureOpportunityBytes(t, 5)
	farmFields, err := farmactormodelv2.Encode(auditFixtureFarm(farmID, 3), headerBytes, factsBytes)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		row  authoritystore.ModelFieldReadResult
		farm bool
	}{
		{name: "player partial", row: auditRowWithMutation(playerID, playerFields, func(state authoritystore.ModelState) { delete(state, "inventory") })},
		{name: "player mixed record", row: auditRowWithMutation(playerID, playerFields, func(state authoritystore.ModelState) { state["record"] = []byte{1} })},
		{name: "player malformed", row: auditRowWithMutation(playerID, playerFields, func(state authoritystore.ModelState) { state["economy"] = []byte("not-an-envelope") })},
		{name: "farm partial", farm: true, row: auditRowWithMutation(farmID, farmFields, func(state authoritystore.ModelState) { delete(state, "plots") })},
		{name: "farm mixed record", farm: true, row: auditRowWithMutation(farmID, farmFields, func(state authoritystore.ModelState) { state["record"] = []byte{1} })},
		{name: "farm malformed", farm: true, row: auditRowWithMutation(farmID, farmFields, func(state authoritystore.ModelState) { state["opportunity_header"] = []byte("not-an-envelope") })},
		{name: "unknown player layout", row: authoritystore.ModelFieldReadResult{ActorID: playerID, Found: true, ModelLayout: authoritystore.ModelLayoutUnknown, ModelFields: cloneAuditState(playerFields)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decodeErr error
			if test.farm {
				_, decodeErr = decodeAuditFarmRow(test.row, farmID)
			} else {
				_, decodeErr = decodeAuditPlayerRow(test.row, playerID)
			}
			if decodeErr == nil {
				t.Fatal("invalid row decoded and could produce a zero-value PASS")
			}
		})
	}
}

func TestAuditReadFieldNamesIncludeV1AlternativeAndExactV2Core(t *testing.T) {
	tests := []struct {
		domain actorrouting.RouteDomain
		want   string
	}{
		{domain: actorrouting.DomainPlayer, want: "record,compendium,economy,inventory,mail_claim_state,pet,profile,progression,task"},
		{domain: actorrouting.DomainFarm, want: "record,activity_summary,farm_meta,guard,opportunity_facts,opportunity_header,plots"},
	}
	for _, test := range tests {
		names, err := auditReadFieldNames(test.domain)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(names, ","); got != test.want {
			t.Fatalf("%s read names=%q want=%q", test.domain, got, test.want)
		}
	}
}

func auditFixturePlayer(playerID uint64) *playerpb.PlayerRecord {
	daily := make([]*taskpb.TaskDomainInstance, playertask.DailySlotCount)
	for index := range daily {
		daily[index] = &taskpb.TaskDomainInstance{
			InstanceId: "daily-claimed-" + string(rune('a'+index)), TaskId: "daily.fixture",
			Target: 1, Progress: 1, State: int32(playertask.StateClaimed),
		}
	}
	return &playerpb.PlayerRecord{
		PlayerId: playerID, BootstrapId: "audit-fixture", EconomyVersion: 12,
		Wallet:        &economypb.Wallet{Coin: 700},
		Inventory:     &economypb.Inventory{Stacks: []*economypb.ItemStack{{ItemId: cropcatalog.CarrotItemID, Count: 510}}},
		Progression:   &economypb.Progression{Level: 4, Exp: 90},
		PublicProfile: &playerpb.PlayerPublicProfile{DisplayName: "audit-fixture", AvatarId: 2},
		TaskDomainState: &taskpb.TaskDomainState{
			DailyPeriodId: "20260810", DailyConfigVersion: 1, DailyTasks: daily,
			GrowthCursor: 1, TotalHarvestCount: 4, TaskStateVersion: 9, ClaimableCount: 0, GrowthChainVersion: 1,
		},
	}
}

func auditFixtureFarm(farmID uint64, plotIndex int32) *farmpb.FarmRecord {
	return &farmpb.FarmRecord{
		FarmId: farmID,
		Core: &farmpb.FarmCore{
			FarmId: farmID, FarmVersion: 5,
			CropCycles: []*farmpb.CropCycle{{CropCycleId: "audit-cycle", PlotIndex: plotIndex, StolenTotal: 10}},
		},
	}
}

func auditFixtureOpportunityBytes(t *testing.T, farmVersion uint64) ([]byte, []byte) {
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

func auditMarshalProto(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func auditRowWithMutation(actorID uint64, source authoritystore.ModelState, mutate func(authoritystore.ModelState)) authoritystore.ModelFieldReadResult {
	state := cloneAuditState(source)
	mutate(state)
	return authoritystore.ModelFieldReadResult{ActorID: actorID, Found: true, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: state}
}

func cloneAuditState(source authoritystore.ModelState) authoritystore.ModelState {
	clone := make(authoritystore.ModelState, len(source))
	for name, value := range source {
		clone[name] = append([]byte(nil), value...)
	}
	return clone
}
