package authority

import (
	"context"
	"testing"

	"farmgame/internal/actorrouting"
)

func TestBatchReadModelFieldsPreservesOrderAndMissingRows(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()

	for farmID, fields := range map[uint64]map[string]any{
		2001: {
			"_meta:record_version":     "7",
			"model:opportunity_header": []byte("header-2001"),
			"model:opportunity_facts":  []byte("facts-2001"),
		},
		2002: {
			"_meta:record_version":     "9",
			"model:opportunity_header": []byte("header-2002"),
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

	rows, err := rig.store.BatchReadModelFields(ctx, actorrouting.DomainFarm, []uint64{2002, 2999, 2001}, []string{
		"opportunity_header", "opportunity_facts",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want=3", len(rows))
	}
	if rows[0].ActorID != 2002 || !rows[0].Found || rows[0].RecordVersion != 9 ||
		string(rows[0].ModelFields["opportunity_header"]) != "header-2002" {
		t.Fatalf("row[0]=%+v", rows[0])
	}
	if _, exists := rows[0].ModelFields["opportunity_facts"]; exists {
		t.Fatalf("missing field was synthesized: %+v", rows[0])
	}
	if rows[1].ActorID != 2999 || rows[1].Found || rows[1].RecordVersion != 0 || len(rows[1].ModelFields) != 0 {
		t.Fatalf("row[1]=%+v", rows[1])
	}
	if rows[2].ActorID != 2001 || !rows[2].Found || rows[2].RecordVersion != 7 ||
		string(rows[2].ModelFields["opportunity_header"]) != "header-2001" ||
		string(rows[2].ModelFields["opportunity_facts"]) != "facts-2001" {
		t.Fatalf("row[2]=%+v", rows[2])
	}
}
