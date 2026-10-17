package actor

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	farmmodelv2 "farmgame/internal/farm/actormodelv2"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func TestFarmV1ToV2ByteGoldensAndBusinessEquivalentRoundTrip(t *testing.T) {
	record := farmV2GoldenRecord()
	v1, header, facts, err := buildFarmAuthorityModelState(record)
	if err != nil {
		t.Fatal(err)
	}
	const wantV1RecordHex = "088e0512ab02088e05100c1a1410041a0c6379636c652d676f6c64656e200130011a0808011001200130011a0808021001200130011a0808031001200130011a0808041001200130011a0808051001200230011a0808061001200230011a0808071001200230011a0808081001200230011a0808091001200230011a08080a1001200230011a08080b1001200230011a08080c1001200230011a08080d1001200230011a08080e1001200230011a08080f1001200230011a0808101001200230011a0808111001200230011a0808121001200230011a0808131001200230011a0808141001200230011a0808151001200230011a0808161001200230011a08081710012002300122280a0c6379636c652d676f6c64656e1801200228e0badce1c13330a08a81e2c133380a4003480150011a490a0b6c65676163792d6f702d3110011801220d6c65676163792d726573756c742880c8e2e1c1333220424242424242424242424242424242424242424242424242424242424242424222370a08736574746c652d3110011801208e0528d10f30083a0c6379636c652d676f6c64656e40e4c8e2e1c1334a0a088e05100118d10f20082a21088906120c6379636c652d676f6c64656e1a0774686566742d3120c8c9e2e1c133322008f806120c6379636c652d676f6c64656e1a06636172652d3120accae2e1c133"
	if got := hex.EncodeToString(v1["record"]); got != wantV1RecordHex {
		t.Errorf("v1 record bytes=%s", got)
	}
	v2, err := farmmodelv2.ConvertV1Bytes(v1["record"], v1[FarmOpportunityHeaderModelField], v1[FarmOpportunityFactsModelField])
	if err != nil {
		t.Fatal(err)
	}
	wantFields := map[string]string{
		"activity_summary":   "414d463202010001000000c91a490a0b6c65676163792d6f702d3110011801220d6c65676163792d726573756c742880c8e2e1c1333220424242424242424242424242424242424242424242424242424242424242424222370a08736574746c652d3110011801208e0528d10f30083a0c6379636c652d676f6c64656e40e4c8e2e1c1334a0a088e05100118d10f20082a21088906120c6379636c652d676f6c64656e1a0774686566742d3120c8c9e2e1c133322008f806120c6379636c652d676f6c64656e1a06636172652d3120accae2e1c133",
		"farm_meta":          "414d4632020200010000000a088e051205088e05100c",
		"guard":              "414d4632020300010000000844495341424c4544",
		"opportunity_facts":  "414d46320204000100000058464f463100000001000000000000000c00000001000000010000000c6379636c652d676f6c64656e0000019c1c40452000000009000000000000000000000000000000000000000000000000000000010000000000000309",
		"opportunity_header": "414d46320205000100000030464f483200000002000000000000000c0000019c1c404520000000000000000000000000000000000000019c1c404520",
		"plots":              "414d4632020600010000012912a6021a1410041a0c6379636c652d676f6c64656e200130011a0808011001200130011a0808021001200130011a0808031001200130011a0808041001200130011a0808051001200230011a0808061001200230011a0808071001200230011a0808081001200230011a0808091001200230011a08080a1001200230011a08080b1001200230011a08080c1001200230011a08080d1001200230011a08080e1001200230011a08080f1001200230011a0808101001200230011a0808111001200230011a0808121001200230011a0808131001200230011a0808141001200230011a0808151001200230011a0808161001200230011a08081710012002300122280a0c6379636c652d676f6c64656e1801200228e0badce1c13330a08a81e2c133380a400348015001",
	}
	for _, name := range fieldv2.SortedNames(v2) {
		got := hex.EncodeToString(v2[name])
		if got != wantFields[name] {
			t.Errorf("field %s bytes=%s", name, got)
		}
	}
	decoded, headerBytes, factsBytes, err := farmmodelv2.Decode(v2)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded, record) || !bytes.Equal(headerBytes, v1[FarmOpportunityHeaderModelField]) ||
		!bytes.Equal(factsBytes, v1[FarmOpportunityFactsModelField]) {
		t.Fatalf("farm business/projection round trip differs\nwant=%v\ngot=%v", record, decoded)
	}
	rebuilt, rebuiltHeader, rebuiltFacts, err := buildFarmAuthorityModelState(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rebuilt["record"], v1["record"]) || rebuiltHeader != header || !reflect.DeepEqual(rebuiltFacts, facts) {
		t.Fatal("decoded v2 does not rebuild exact v1 business/projection state")
	}
}

func TestFarmV2FailsClosedOnPartialUnknownCrossFieldAndCapacity(t *testing.T) {
	v1, _, _, err := buildFarmAuthorityModelState(farmV2GoldenRecord())
	if err != nil {
		t.Fatal(err)
	}
	v2, err := farmmodelv2.ConvertV1Bytes(v1["record"], v1[FarmOpportunityHeaderModelField], v1[FarmOpportunityFactsModelField])
	if err != nil {
		t.Fatal(err)
	}
	partial := cloneFarmV2State(v2)
	delete(partial, "activity_summary")
	if _, _, _, err := farmmodelv2.Decode(partial); !errors.Is(err, farmmodelv2.ErrMalformed) {
		t.Fatalf("partial err=%v", err)
	}
	unknown := cloneFarmV2State(v2)
	unknown["settlements"] = []byte("unknown")
	if _, _, _, err := farmmodelv2.Decode(unknown); !errors.Is(err, farmmodelv2.ErrMalformed) {
		t.Fatalf("unknown err=%v", err)
	}
	cross := cloneFarmV2State(v2)
	cross["farm_meta"] = append([]byte(nil), v2["plots"]...)
	if _, _, _, err := farmmodelv2.Decode(cross); err == nil {
		t.Fatal("cross-field substitution accepted")
	}
	malformedProjection := cloneFarmV2State(v2)
	malformedProjection["opportunity_header"], err = fieldv2.Marshal(actorrouting.DomainFarm, "opportunity_header", []byte("not-a-header"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := farmmodelv2.Decode(malformedProjection); !errors.Is(err, farmmodelv2.ErrMalformed) {
		t.Fatalf("malformed projection err=%v", err)
	}
	over := farmV2GoldenRecord()
	over.Core.Plots = make([]*farmpb.Plot, 25)
	if _, err := farmmodelv2.Encode(over, v1[FarmOpportunityHeaderModelField], v1[FarmOpportunityFactsModelField]); !errors.Is(err, farmmodelv2.ErrMalformed) {
		t.Fatalf("over-capacity err=%v", err)
	}
}

func TestFarmV2NilAndEmptyRepeatedCarriersHaveOneDeterministicEncoding(t *testing.T) {
	nilRecord := &farmpb.FarmRecord{FarmId: 9, Core: &farmpb.FarmCore{FarmId: 9}}
	emptyRecord := &farmpb.FarmRecord{
		FarmId:                  9,
		Core:                    &farmpb.FarmCore{FarmId: 9, Plots: []*farmpb.Plot{}, CropCycles: []*farmpb.CropCycle{}},
		RecentOperationReceipts: []*farmpb.FarmOperationReceipt{}, ActiveSettlements: []*farmpb.FarmSettlement{},
		RecentTheftAttempts: []*farmpb.TheftAttemptReceipt{}, RecentCareAttempts: []*farmpb.CareAttemptReceipt{},
	}
	headerBytes, err := domain.MarshalFarmOpportunityHeaderV2(domain.FarmOpportunityHeaderV2{SchemaVersion: domain.FarmOpportunityHeaderSchemaV2})
	if err != nil {
		t.Fatal(err)
	}
	factsBytes, err := domain.MarshalFarmOpportunityFactsV1(domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1})
	if err != nil {
		t.Fatal(err)
	}
	nilState, err := farmmodelv2.Encode(nilRecord, headerBytes, factsBytes)
	if err != nil {
		t.Fatal(err)
	}
	emptyState, err := farmmodelv2.Encode(emptyRecord, headerBytes, factsBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nilState, emptyState) {
		t.Fatal("nil and empty repeated carriers produced different bytes")
	}
	decoded, header, facts, err := farmmodelv2.Decode(nilState)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := farmmodelv2.Encode(decoded, header, facts)
	if err != nil || !reflect.DeepEqual(reencoded, nilState) {
		t.Fatalf("minimal deterministic re-encode differs err=%v", err)
	}
}

func TestFarmCommandCarrierDirtyMatrix(t *testing.T) {
	base := farmV2GoldenRecord()
	baseState, _, _, err := buildFarmAuthorityCandidate(authoritystore.ModelLayoutV2, base)
	if err != nil {
		t.Fatal(err)
	}
	allBusinessFields := []string{"activity_summary", "farm_meta", "opportunity_facts", "opportunity_header", "plots"}
	tests := []struct {
		name   string
		want   []string
		mutate func(*farmpb.FarmRecord)
	}{
		{name: "WATER", want: allBusinessFields, mutate: func(record *farmpb.FarmRecord) {
			record.Core.FarmVersion++
			record.Core.Plots[0].PlotVersion++
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{OperationId: "water-matrix"})
		}},
		{name: "THEFT", want: allBusinessFields, mutate: func(record *farmpb.FarmRecord) {
			record.Core.FarmVersion++
			record.Core.Plots[0].PlotVersion++
			record.RecentTheftAttempts = append(record.RecentTheftAttempts, &farmpb.TheftAttemptReceipt{VisitorPlayerId: 900, CropCycleId: "cycle-golden", OperationId: "theft-matrix"})
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{OperationId: "theft-matrix"})
		}},
		{name: "HARVEST", want: allBusinessFields, mutate: func(record *farmpb.FarmRecord) {
			record.Core.FarmVersion++
			record.Core.Plots[0].PlotVersion++
			record.Core.Plots[0].ActiveCropCycleId = ""
			record.Core.CropCycles = nil
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{OperationId: "harvest-matrix"})
		}},
		{name: "RECEIPT_ONLY", want: []string{"activity_summary"}, mutate: func(record *farmpb.FarmRecord) {
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{OperationId: "receipt-matrix"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(base).(*farmpb.FarmRecord)
			test.mutate(candidate)
			candidateState, _, _, err := buildFarmAuthorityCandidate(authoritystore.ModelLayoutV2, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if got := farmChangedCodecFields(baseState, candidateState); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("dirty=%v want=%v", got, test.want)
			}
		})
	}
}

func farmChangedCodecFields(before, after authoritystore.ModelState) []string {
	names := fieldv2.SortedNames(after)
	changed := make([]string, 0, len(names))
	for _, name := range names {
		if !bytes.Equal(before[name], after[name]) {
			changed = append(changed, name)
		}
	}
	return changed
}

func farmV2GoldenRecord() *farmpb.FarmRecord {
	record := InitialRecord(654)
	record.Core.FarmVersion = 12
	record.Core.Plots[0].ActiveCropCycleId = "cycle-golden"
	record.Core.Plots[0].PlotVersion = 4
	record.Core.CropCycles = []*farmpb.CropCycle{{
		CropCycleId: "cycle-golden", PlotIndex: 0, CropDefinitionId: 1, RuleVersion: 2,
		PlantAt: 1769999900000, MatureAt: 1770000500000, BaseYield: 10, StealableLimit: 3,
		StolenTotal: 1, Watered: true,
	}}
	record.RecentOperationReceipts = []*farmpb.FarmOperationReceipt{{
		OperationId: "legacy-op-1", Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
		ResultPayload: []byte("legacy-result"), CommittedAtMs: 1770000000000, PayloadHash: bytes.Repeat([]byte{0x42}, 32),
	}}
	record.ActiveSettlements = []*farmpb.FarmSettlement{{
		SettlementId: "settle-1", Kind: farmpb.FarmSettlementKind_HARVEST,
		State: farmpb.FarmSettlementState_TARGET_COMMITTED, BeneficiaryPlayerId: 654,
		ItemId: 2001, ItemCount: 8, CropCycleId: "cycle-golden", UpdatedAtMs: 1770000000100,
		Participants: []*farmpb.FarmSettlementParticipant{{PlayerId: 654, Role: farmpb.FarmParticipantRole_OWNER, ItemId: 2001, ItemCount: 8}},
	}}
	record.RecentTheftAttempts = []*farmpb.TheftAttemptReceipt{{VisitorPlayerId: 777, CropCycleId: "cycle-golden", OperationId: "theft-1", CommittedAtMs: 1770000000200}}
	record.RecentCareAttempts = []*farmpb.CareAttemptReceipt{{HelperPlayerId: 888, CropCycleId: "cycle-golden", OperationId: "care-1", CommittedAtMs: 1770000000300}}
	return record
}

func cloneFarmV2State(in authoritystore.ModelState) authoritystore.ModelState {
	out := make(authoritystore.ModelState, len(in))
	for name, value := range in {
		out[name] = append([]byte(nil), value...)
	}
	return out
}
