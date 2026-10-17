package main

import (
	"context"
	"testing"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routeplan"
)

type countingCachedHolderReader struct {
	freshCalls  int
	cachedCalls int
}

func (r *countingCachedHolderReader) Get(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
	r.freshCalls++
	return core.HolderRecord{HolderID: "fresh"}, true, nil
}

func (r *countingCachedHolderReader) GetCached(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
	r.cachedCalls++
	return core.HolderRecord{HolderID: "cached"}, true, nil
}

func TestHolderLookupForPlanUsesCachedViewForScalePlanOwnedChild(t *testing.T) {
	reader := &countingCachedHolderReader{}
	lookup := holderLookupForPlan(reader, routeplan.Plan{OwnerScalePlanID: "scale-plan-1"})
	record, found, err := lookup(context.Background(), actorrouting.DomainFarm, "farm-svr-2")
	if err != nil || !found || record.HolderID != "cached" {
		t.Fatalf("lookup record=%+v found=%v err=%v", record, found, err)
	}
	if reader.cachedCalls != 1 || reader.freshCalls != 0 {
		t.Fatalf("cached calls=%d fresh calls=%d", reader.cachedCalls, reader.freshCalls)
	}
}

func TestHolderLookupForPlanUsesCachedViewForLegacyWaveChild(t *testing.T) {
	reader := &countingCachedHolderReader{}
	lookup := holderLookupForPlan(reader, routeplan.Plan{OwnerWaveID: "wave-1"})
	if _, _, err := lookup(context.Background(), actorrouting.DomainFarm, "farm-svr-2"); err != nil {
		t.Fatal(err)
	}
	if reader.cachedCalls != 1 || reader.freshCalls != 0 {
		t.Fatalf("cached calls=%d fresh calls=%d", reader.cachedCalls, reader.freshCalls)
	}
}

func TestHolderLookupForPlanRefreshesManualRoutePlan(t *testing.T) {
	reader := &countingCachedHolderReader{}
	lookup := holderLookupForPlan(reader, routeplan.Plan{})
	record, found, err := lookup(context.Background(), actorrouting.DomainFarm, "farm-svr-2")
	if err != nil || !found || record.HolderID != "fresh" {
		t.Fatalf("lookup record=%+v found=%v err=%v", record, found, err)
	}
	if reader.freshCalls != 1 || reader.cachedCalls != 0 {
		t.Fatalf("fresh calls=%d cached calls=%d", reader.freshCalls, reader.cachedCalls)
	}
}
