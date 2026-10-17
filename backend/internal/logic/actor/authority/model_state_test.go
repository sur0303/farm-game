package authority

import (
	"context"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func TestBindingCommitsAndLoadsCompleteModelStateAtomically(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 77, "holder-a", 1, 1)
	provider := newProvider(t, store, fence)
	initial := authoritystore.ModelState{
		RecordModelField:     []byte("record-v1"),
		"opportunity_header": []byte("header-v1"),
		"opportunity_facts":  []byte("facts-v1"),
	}
	binding := claimBinding(t, provider, actorrouting.DomainFarm, 77, initial)

	loaded, version, err := binding.GetModelState(context.Background(), 77)
	if err != nil || version != 1 || string(loaded["opportunity_header"]) != "header-v1" {
		t.Fatalf("initial state/version mismatch: state=%q version=%d err=%v", loaded, version, err)
	}

	candidate := authoritystore.ModelState{
		RecordModelField:     []byte("record-v2"),
		"opportunity_header": []byte("header-v2"),
		"opportunity_facts":  []byte("facts-v2"),
	}
	if err := binding.CASEpochModelStateWithOutcomeAndOrders(context.Background(), 77, version, 0, candidate,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, nil); err != nil {
		t.Fatal(err)
	}
	loaded, version, err = binding.GetModelState(context.Background(), 77)
	if err != nil || version != 2 || string(loaded[RecordModelField]) != "record-v2" ||
		string(loaded["opportunity_header"]) != "header-v2" || string(loaded["opportunity_facts"]) != "facts-v2" {
		t.Fatalf("committed model state was torn: state=%q version=%d err=%v", loaded, version, err)
	}
}
