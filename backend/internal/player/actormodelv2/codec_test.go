package actormodelv2

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
	"farmgame/internal/actormodel/fieldv2"

	"google.golang.org/protobuf/proto"
)

func TestPlayerCommandCarrierDirtyMatrix(t *testing.T) {
	base := playerGoldenRecord()
	baseState, err := Encode(base)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		want   []string
		mutate func(*playerpb.PlayerRecord)
	}{
		{name: "BUY", want: []string{"economy", "inventory", "task"}, mutate: func(record *playerpb.PlayerRecord) {
			record.Wallet.Coin--
			record.Inventory.Stacks[0].Count++
			record.Tasks[0].Progress++
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{OperationId: "buy-matrix"})
		}},
		{name: "CLAIM_TASK", want: []string{"economy", "inventory", "progression", "task"}, mutate: func(record *playerpb.PlayerRecord) {
			record.Wallet.Coin++
			record.Inventory.Stacks[0].Count++
			record.Progression.Exp++
			record.TaskDomainState.TaskStateVersion++
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{OperationId: "task-matrix"})
		}},
		{name: "HARVEST_REWARD", want: []string{"compendium", "inventory", "progression", "task"}, mutate: func(record *playerpb.PlayerRecord) {
			record.Inventory.Stacks[0].Count++
			record.Progression.Exp++
			record.CodexEntries = append(record.CodexEntries, &playerpb.CodexEntry{CropDefinitionId: 99})
			record.Tasks[0].Progress++
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{OperationId: "harvest-matrix"})
		}},
		{name: "RECEIPT_ONLY", want: []string{"task"}, mutate: func(record *playerpb.PlayerRecord) {
			record.RecentOperationReceipts = append(record.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{OperationId: "receipt-matrix"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(base).(*playerpb.PlayerRecord)
			test.mutate(candidate)
			candidateState, err := Encode(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if got := changedCodecFields(baseState, candidateState); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("dirty=%v want=%v", got, test.want)
			}
		})
	}
}

func changedCodecFields(before, after map[string][]byte) []string {
	names := fieldv2.SortedNames(after)
	changed := make([]string, 0, len(names))
	for _, name := range names {
		if !bytes.Equal(before[name], after[name]) {
			changed = append(changed, name)
		}
	}
	return changed
}

func TestPlayerV1ToV2ByteGoldensAndBusinessRoundTrip(t *testing.T) {
	record := playerGoldenRecord()
	beforeEncode := proto.Clone(record).(*playerpb.PlayerRecord)
	v1, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	const wantV1Hex = "08c10210091a0b626f6f742d676f6c64656e22030889062a0e0a0508e90710040a0508d10f100232080803102d1a0201073a430a086f702d6275792d3110011801220a6275792d726573756c742880c8e2e1c1333220686868686868686868686868686868686868686868686868686868686868686842390a07736565642d723110e9071802200128e4c8e2e1c133322069696969696969696969696969696969696969696969696969696969696969694a17080110c8c9e2e1c1331a0c6f702d686172766573742d31520f080b10021805200128accae2e1c1335a320a0a323032362d30382d313010021a180a076461696c792d311207686172766573744803500158012001300838034004480162110a0d476f6c64656e204661726d657210066a400a09756e6c6f636b2d723110c102180620c801280230013890cbe2e1c13342207575757575757575757575757575757575757575757575757575757575757575"
	if got := hex.EncodeToString(v1); got != wantV1Hex {
		t.Errorf("v1 bytes=%s", got)
	}
	fromBytes, err := ConvertV1Bytes(v1)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(record, beforeEncode) {
		t.Fatal("Encode mutated its borrowed PlayerRecord")
	}
	if !reflect.DeepEqual(fromBytes, direct) {
		t.Fatal("byte converter and record converter disagree")
	}
	wantFields := map[string]string{
		"compendium":       "414d463201010001000000194a17080110c8c9e2e1c1331a0c6f702d686172766573742d31",
		"economy":          "414d46320102000100000049100922030889066a400a09756e6c6f636b2d723110c102180620c801280230013890cbe2e1c13342207575757575757575757575757575757575757575757575757575757575757575",
		"inventory":        "414d4632010300010000004b2a0e0a0508e90710040a0508d10f100242390a07736565642d723110e9071802200128e4c8e2e1c13332206969696969696969696969696969696969696969696969696969696969696969",
		"mail_claim_state": "414d4632010400010000000844495341424c4544",
		"pet":              "414d4632010500010000000844495341424c4544",
		"profile":          "414d4632010600010000002308c1021a0b626f6f742d676f6c64656e62110a0d476f6c64656e204661726d65721006",
		"progression":      "414d4632010700010000000a32080803102d1a020107",
		"task":             "414d4632010800010000008a3a430a086f702d6275792d3110011801220a6275792d726573756c742880c8e2e1c13332206868686868686868686868686868686868686868686868686868686868686868520f080b10021805200128accae2e1c1335a320a0a323032362d30382d313010021a180a076461696c792d3112076861727665737448035001580120013008380340044801",
	}
	for _, name := range fieldv2.SortedNames(fromBytes) {
		got := hex.EncodeToString(fromBytes[name])
		if got != wantFields[name] {
			t.Errorf("field %s bytes=%s", name, got)
		}
	}
	decoded, err := Decode(fromBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded, record) {
		t.Fatalf("business round trip differs\nwant=%v\ngot=%v", record, decoded)
	}
}

func TestPlayerV2FailsClosedOnPartialUnknownCrossFieldAndCapacity(t *testing.T) {
	state, err := Encode(playerGoldenRecord())
	if err != nil {
		t.Fatal(err)
	}

	partial := cloneState(state)
	delete(partial, "task")
	if _, err := Decode(partial); !errors.Is(err, ErrMalformed) {
		t.Fatalf("partial err=%v", err)
	}
	unknown := cloneState(state)
	unknown["unknown"] = []byte("x")
	if _, err := Decode(unknown); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unknown err=%v", err)
	}
	cross := cloneState(state)
	cross["profile"] = append([]byte(nil), state["economy"]...)
	if _, err := Decode(cross); err == nil {
		t.Fatal("cross-field substitution accepted")
	}
	over := playerGoldenRecord()
	over.Tasks = make([]*taskpb.TaskInstance, fieldv2.MaxRepeatedItems+1)
	if _, err := Encode(over); !errors.Is(err, ErrMalformed) {
		t.Fatalf("over-capacity err=%v", err)
	}

	nonCanonical, err := proto.Marshal(playerGoldenRecord())
	if err != nil {
		t.Fatal(err)
	}
	// Appending a duplicate scalar is valid protobuf but not the one frozen
	// deterministic byte representation.
	nonCanonical = append(nonCanonical, 0x08, 0xc1, 0x02)
	if _, err := ConvertV1Bytes(nonCanonical); !errors.Is(err, ErrMalformed) {
		t.Fatalf("non-canonical v1 err=%v", err)
	}
}

func TestPlayerV2NilAndEmptyRepeatedCarriersHaveOneDeterministicEncoding(t *testing.T) {
	nilRecord := &playerpb.PlayerRecord{PlayerId: 1}
	emptyRecord := &playerpb.PlayerRecord{
		PlayerId: 1, RecentOperationReceipts: []*playerpb.PlayerOperationReceipt{},
		ItemReservations: []*playerpb.ItemReservation{}, CodexEntries: []*playerpb.CodexEntry{},
		Tasks: []*taskpb.TaskInstance{}, UnlockCoinReservations: []*playerpb.UnlockCoinReservation{},
	}
	nilState, err := Encode(nilRecord)
	if err != nil {
		t.Fatal(err)
	}
	emptyState, err := Encode(emptyRecord)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nilState, emptyState) {
		t.Fatal("nil and empty repeated carriers produced different bytes")
	}
	decoded, err := Decode(nilState)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := Encode(decoded)
	if err != nil || !reflect.DeepEqual(reencoded, nilState) {
		t.Fatalf("minimal deterministic re-encode differs err=%v", err)
	}
}

func playerGoldenRecord() *playerpb.PlayerRecord {
	return &playerpb.PlayerRecord{
		PlayerId: 321, EconomyVersion: 9, BootstrapId: "boot-golden",
		Wallet:      &economypb.Wallet{Coin: 777},
		Inventory:   &economypb.Inventory{Stacks: []*economypb.ItemStack{{ItemId: 1001, Count: 4}, {ItemId: 2001, Count: 2}}},
		Progression: &economypb.Progression{Level: 3, Exp: 45, Unlocked: []int32{1, 7}},
		RecentOperationReceipts: []*playerpb.PlayerOperationReceipt{{
			OperationId: "op-buy-1", Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
			ResultPayload: []byte("buy-result"), CommittedAtMs: 1770000000000, PayloadHash: []byte(strings.Repeat("h", 32)),
		}},
		ItemReservations: []*playerpb.ItemReservation{{
			ReservationId: "seed-r1", ItemId: 1001, Count: 2, State: playerpb.ReservationState_PREPARED,
			UpdatedAtMs: 1770000000100, PayloadHash: []byte(strings.Repeat("i", 32)),
		}},
		CodexEntries: []*playerpb.CodexEntry{{CropDefinitionId: 1, FirstHarvestedAtMs: 1770000000200, SourceOperationId: "op-harvest-1"}},
		Tasks:        []*taskpb.TaskInstance{{TaskDefinitionId: 11, Progress: 2, Target: 5, State: taskpb.TaskState_ACTIVE, UpdatedAtMs: 1770000000300}},
		TaskDomainState: &taskpb.TaskDomainState{
			DailyPeriodId: "2026-08-10", DailyConfigVersion: 2,
			DailyTasks:   []*taskpb.TaskDomainInstance{{InstanceId: "daily-1", TaskId: "harvest", Target: 3, Progress: 1, State: 1}},
			GrowthCursor: 1, TotalHarvestCount: 8, HighestLevel: 3, TaskStateVersion: 4, ClaimableCount: 1,
		},
		PublicProfile: &playerpb.PlayerPublicProfile{DisplayName: "Golden Farmer", AvatarId: 6},
		UnlockCoinReservations: []*playerpb.UnlockCoinReservation{{
			ReservationId: "unlock-r1", FarmId: 321, PlotId: 6, AmountNormalCoin: 200,
			State: playerpb.ReservationState_COMMITTED, UnlockRuleVersion: 1,
			UpdatedAtMs: 1770000000400, PayloadHash: []byte(strings.Repeat("u", 32)),
		}},
	}
}

func cloneState(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for name, value := range in {
		out[name] = append([]byte(nil), value...)
	}
	return out
}
