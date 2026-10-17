package authority

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func TestBatchReadOpportunityHeadersPreservesOrderAndReadsOnlyHeader(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	for farmID, fields := range map[uint64]map[string]any{
		2001: {
			"_meta:record_version":       "7",
			"_meta:model_layout_version": "2",
			"model:opportunity_header":   []byte("header-2001"),
			"model:opportunity_facts":    []byte("facts-must-not-be-read"),
		},
		2002: {
			"_meta:record_version":       "9",
			"_meta:model_layout_version": "1",
			"model:opportunity_header":   []byte("header-2002"),
		},
	} {
		key, err := rig.store.Keys().ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
		if err != nil {
			t.Fatal(err)
		}
		if err := rig.client.HSet(ctx, key.String(), fields).Err(); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := rig.store.BatchReadOpportunityHeaders(ctx, []uint64{2002, 2999, 2001})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want=3", len(rows))
	}
	if rows[0].ActorID != 2002 || !rows[0].Found || rows[0].RecordVersion != 9 ||
		rows[0].ModelLayout != authoritystore.ModelLayoutV1 || string(rows[0].ModelFields["opportunity_header"]) != "header-2002" {
		t.Fatalf("row[0]=%+v", rows[0])
	}
	if rows[1].ActorID != 2999 || rows[1].Found || rows[1].Err != nil {
		t.Fatalf("row[1]=%+v", rows[1])
	}
	if rows[2].ActorID != 2001 || !rows[2].Found || rows[2].RecordVersion != 7 ||
		rows[2].ModelLayout != authoritystore.ModelLayoutV2 || string(rows[2].ModelFields["opportunity_header"]) != "header-2001" {
		t.Fatalf("row[2]=%+v", rows[2])
	}
	if _, exists := rows[2].ModelFields["opportunity_facts"]; exists {
		t.Fatalf("narrow read unexpectedly returned facts: %+v", rows[2])
	}
}

func TestBatchReadOpportunityHeadersDoesNotTurnBrokenV2IntoKnownNone(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	key, err := rig.store.Keys().ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 2003})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(ctx, key.String(), map[string]any{
		"_meta:record_version":       "2",
		"_meta:model_layout_version": "2",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	rows, err := rig.store.BatchReadOpportunityHeaders(ctx, []uint64{2003})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].Found || rows[0].Err == nil {
		t.Fatalf("broken v2 must remain UNKNOWN-capable: %+v", rows)
	}
}

func TestBatchReadOpportunityHeadersRejectsInvalidBatch(t *testing.T) {
	rig := newTestRig(t)
	if _, err := rig.store.BatchReadOpportunityHeaders(context.Background(), nil); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("empty batch err=%v", err)
	}
	ids := make([]uint64, maxOpportunityHeaderBatch+1)
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	if _, err := rig.store.BatchReadOpportunityHeaders(context.Background(), ids); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("oversized batch err=%v", err)
	}
}
