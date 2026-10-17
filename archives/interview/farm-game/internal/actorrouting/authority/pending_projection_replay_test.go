package authority

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
)

func TestPendingProjectionValidationUsesCommittedMemoRecovery(t *testing.T) {
	now := time.Now()
	request := transitionRequest{
		kind: transitionTransfer, transitionID: "planned-drain-replay", domain: actorrouting.DomainPlayer,
		virtualShard: 0,
		expectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "player-a", ExpectedHolderBootID: "boot-a",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		},
		newHolderID: "player-b", newHolderBoot: "boot-b", endpoint: "player-b:9000",
		ttl: time.Hour, mode: TransferModeDrainHandoff, internalDrain: true,
	}
	fingerprint := transitionFingerprint(request)
	result := authoritystore.ShardAuthorityTransitionResult{
		Status: authoritystore.ShardTransitionApplied,
		Authority: authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2,
			Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			HolderID: "player-b", HolderBootID: "boot-b", AssignmentRevision: 2,
			WriterEpoch: 2, LeaseEpoch: 2, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
			State: authoritystore.ShardAuthorityActive, LastTransitionID: request.transitionID,
		},
	}
	memo := &transitionMemo{
		fingerprint: fingerprint, request: request, result: result,
		lastState: actorrouting.ShardStateAssigning, published: false,
		authorityAttempted: true, createdAt: now, lastUsedAt: now,
	}
	manager := &RouteAssignmentManager{
		now: time.Now, snapshot: managerSnapshot(),
		transitions: map[string]*transitionMemo{request.transitionID: memo},
		placementExclusions: newFakePlacementExclusions(routecontrol.PlacementExclusion{
			Domain: actorrouting.DomainPlayer, HolderID: "player-b", BootID: "boot-b", Revision: 1,
		}),
	}
	fallback := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, now)
	if fallback.Committed {
		t.Fatal("pre-commit fallback unexpectedly committed")
	}
	called := 0
	err := manager.validatePendingProjectionTarget(context.Background(), request, fallback, func(_ context.Context, recovery TransitionRecovery, _ uint64) error {
		called++
		if !recovery.Committed || recovery.Published || recovery.Result.Authority.LastTransitionID != request.transitionID {
			t.Fatalf("validator did not receive exact committed memo recovery: %+v", recovery)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("validator calls=%d want 1", called)
	}
}
