package main

import (
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func TestValidProbeIdentityRequiresDistinctOldAndCurrentGenerations(t *testing.T) {
	if !validProbeIdentity(
		actorrouting.DomainFarmWorker,
		"farm-worker-a",
		"old-boot",
		"farm-worker-b",
		"current-boot",
		1,
		2,
		3,
	) {
		t.Fatal("distinct complete worker generations rejected")
	}
	if validProbeIdentity(
		actorrouting.DomainFarmWorker,
		"farm-worker-a",
		"same-boot",
		"farm-worker-a",
		"same-boot",
		1,
		2,
		3,
	) {
		t.Fatal("probe accepted current generation as the old generation")
	}
	if validProbeIdentity(
		actorrouting.DomainFarmWorker,
		"farm-worker-a",
		"old-boot",
		"farm-worker-b",
		"current-boot",
		0,
		2,
		3,
	) {
		t.Fatal("probe accepted an incomplete old fence")
	}
}

func TestFencedStatusRejectsOnlyOldGenerationOutcomes(t *testing.T) {
	for _, status := range []authoritystore.ShardAuthorityTransitionStatus{
		authoritystore.ShardTransitionFenceMismatch,
		authoritystore.ShardTransitionUnauthorized,
		authoritystore.ShardTransitionLeaseExpired,
	} {
		if !fencedStatus(status) {
			t.Fatalf("status %s rejected", status)
		}
	}
	for _, status := range []authoritystore.ShardAuthorityTransitionStatus{
		authoritystore.ShardTransitionApplied,
		authoritystore.ShardTransitionAlreadyApplied,
	} {
		if fencedStatus(status) {
			t.Fatalf("unsafe status %s accepted", status)
		}
	}
}
