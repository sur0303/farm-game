package contracts

import (
	"testing"

	contractspb "farmgame/api/gen/contracts"
)

func TestCanonicalHashDeterministicAndOrderIndependent(t *testing.T) {
	a := &contractspb.ReserveSeedPayload{
		PlayerId:         10001,
		FarmId:           20002,
		PlotId:           3,
		ReservationId:    "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
		CropDefinitionId: 7,
		SeedItemId:       701,
		Quantity:         1,
		CropRuleVersion:  42,
	}
	b := &contractspb.ReserveSeedPayload{}
	b.CropRuleVersion = 42
	b.Quantity = 1
	b.SeedItemId = 701
	b.CropDefinitionId = 7
	b.ReservationId = "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo"
	b.PlotId = 3
	b.FarmId = 20002
	b.PlayerId = 10001

	ha, err := CanonicalHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := CanonicalHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if ha != hb {
		t.Fatal("same semantic fields produced different hashes")
	}
}

func TestCanonicalHashFieldSensitivity(t *testing.T) {
	base := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, Quantity: 1}
	h0, err := CanonicalHash(base)
	if err != nil {
		t.Fatalf("hash base: %v", err)
	}
	mutations := []func(*contractspb.ReserveSeedPayload){
		func(m *contractspb.ReserveSeedPayload) { m.PlayerId = 10002 },
		func(m *contractspb.ReserveSeedPayload) { m.FarmId = 20003 },
		func(m *contractspb.ReserveSeedPayload) { m.PlotId = 4 },
		func(m *contractspb.ReserveSeedPayload) { m.Quantity = 2 },
		func(m *contractspb.ReserveSeedPayload) { m.ReservationId = "x" },
		func(m *contractspb.ReserveSeedPayload) { m.CropRuleSnapshotHash = []byte{1, 2, 3} },
	}
	for i, mutate := range mutations {
		m := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, Quantity: 1}
		mutate(m)
		h, err := CanonicalHash(m)
		if err != nil {
			t.Fatalf("mutation %d: %v", i, err)
		}
		if h == h0 {
			t.Fatalf("mutation %d did not change the hash", i)
		}
	}
}

func TestCanonicalHashPresenceBit(t *testing.T) {
	// An unset optional field and an explicitly zero optional field must
	// hash differently (contract 29 §7.11 presence bit).
	unset := &contractspb.CommitPlotUnlockPayload{FarmId: 1}
	set := &contractspb.CommitPlotUnlockPayload{FarmId: 1, PredecessorPlotId: new(uint32)}
	hu, err := CanonicalHash(unset)
	if err != nil {
		t.Fatalf("hash unset: %v", err)
	}
	hs, err := CanonicalHash(set)
	if err != nil {
		t.Fatalf("hash set: %v", err)
	}
	if hu == hs {
		t.Fatal("optional presence must change the hash")
	}
}

func TestCanonicalHashRejectsUnknownFields(t *testing.T) {
	m := &contractspb.ReserveSeedPayload{PlayerId: 1}
	m.ProtoReflect().SetUnknown([]byte{0x08, 0x01})
	if _, err := CanonicalHash(m); err == nil {
		t.Fatal("unknown fields must fail closed")
	}
}

func TestCanonicalHashRepeatedCountPrefix(t *testing.T) {
	empty := &contractspb.StealPlanTemplate{PolicyVersion: 1, TotalCapRatioBp: 2000}
	one := &contractspb.StealPlanTemplate{PolicyVersion: 1, TotalCapRatioBp: 2000, SlotRatioBp: []uint32{200}}
	two := &contractspb.StealPlanTemplate{PolicyVersion: 1, TotalCapRatioBp: 2000, SlotRatioBp: []uint32{200, 200}}
	he, _ := CanonicalHash(empty)
	h1, _ := CanonicalHash(one)
	h2, _ := CanonicalHash(two)
	if he == h1 || h1 == h2 || he == h2 {
		t.Fatal("repeated element count must participate in the hash")
	}
}

func TestCanonicalHashNestedMessageLengthDelimited(t *testing.T) {
	// Nested messages are length-delimited, so a nested value cannot
	// collide with a different parent field layout.
	a := &contractspb.GuardSnapshot{
		SchemaVersion: 1,
		GuardVersion:  7,
		GuardRule:     &contractspb.GuardRuleSnapshot{PetConfigVersion: 1, GuardRuleVersion: 2},
	}
	b := &contractspb.GuardSnapshot{
		SchemaVersion: 1,
		GuardVersion:  7,
		GuardRule:     &contractspb.GuardRuleSnapshot{PetConfigVersion: 1, GuardRuleVersion: 3},
	}
	ha, err := CanonicalHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := CanonicalHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if ha == hb {
		t.Fatal("nested semantic difference must change the hash")
	}
}
