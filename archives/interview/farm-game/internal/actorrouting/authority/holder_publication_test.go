package authority

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
	"farmgame/internal/routecontrol"
)

func authorityTestPublication(t *testing.T) holderkey.Publication {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := holderkey.New(public)
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func TestManagerPublishesKeysIdempotentlyAndMovesHolderWithExactBootKey(t *testing.T) {
	var published []actorrouting.RouteSnapshot
	manager, _ := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	playerA := authorityTestPublication(t)
	playerB := authorityTestPublication(t)
	keys := map[string]holderkey.Publication{
		"PLAYER/player-a/player-a-boot": playerA,
		"PLAYER/player-b/player-b-boot": playerB,
	}
	if err := manager.SetHolderPublicKeyResolver(HolderPublicKeyResolverFunc(func(domain actorrouting.RouteDomain, holderID, bootID string) (holderkey.Publication, bool, error) {
		value, found := keys[string(domain)+"/"+holderID+"/"+bootID]
		return value, found, nil
	})); err != nil {
		t.Fatal(err)
	}

	result, err := manager.ReconcileHolderPublicKeys(context.Background())
	if err != nil || !result.Changed || !result.Committed || result.SnapshotRevision != 2 || len(published) != 1 {
		t.Fatalf("reconcile=%+v publishes=%d err=%v", result, len(published), err)
	}
	got, found, err := manager.CurrentSnapshot().Holders[0].CommitProofPublication()
	if err != nil || !found || !got.Equal(playerA) {
		t.Fatalf("player-a publication found=%v got=%+v err=%v", found, got, err)
	}
	result, err = manager.ReconcileHolderPublicKeys(context.Background())
	if err != nil || result.Changed || len(published) != 1 {
		t.Fatalf("idempotent reconcile=%+v publishes=%d err=%v", result, len(published), err)
	}

	delete(keys, "PLAYER/player-a/player-a-boot")
	result, err = manager.ReconcileHolderPublicKeys(context.Background())
	if err != nil || !result.Changed || !result.Committed {
		t.Fatalf("authoritative key removal reconcile=%+v err=%v", result, err)
	}
	if _, found, err := manager.CurrentSnapshot().Holders[0].CommitProofPublication(); err != nil || found {
		t.Fatalf("removed exact holder/boot key remained found=%t err=%v", found, err)
	}
	keys["PLAYER/player-a/player-a-boot"] = playerA
	if result, err = manager.ReconcileHolderPublicKeys(context.Background()); err != nil || !result.Changed {
		t.Fatalf("same-boot same-key restoration=%+v err=%v", result, err)
	}
	transient := errors.New("verified registry unavailable")
	if err := manager.SetHolderPublicKeyResolver(HolderPublicKeyResolverFunc(func(actorrouting.RouteDomain, string, string) (holderkey.Publication, bool, error) {
		return holderkey.Publication{}, false, transient
	})); err != nil {
		t.Fatal(err)
	}
	beforeTransient := manager.CurrentSnapshot()
	if _, err := manager.ReconcileHolderPublicKeys(context.Background()); !errors.Is(err, transient) {
		t.Fatalf("transient resolver err=%v", err)
	}
	if after := manager.CurrentSnapshot(); after.GlobalRevision != beforeTransient.GlobalRevision {
		t.Fatalf("transient resolver published revision %d -> %d", beforeTransient.GlobalRevision, after.GlobalRevision)
	}
	if err := manager.SetHolderPublicKeyResolver(HolderPublicKeyResolverFunc(func(domain actorrouting.RouteDomain, holderID, bootID string) (holderkey.Publication, bool, error) {
		value, found := keys[string(domain)+"/"+holderID+"/"+bootID]
		return value, found, nil
	})); err != nil {
		t.Fatal(err)
	}

	claim, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "holder-key-move", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := manager.CurrentSnapshot()
	route := snapshot.Player.Routes[0]
	holder := snapshot.Holders[route.HolderIndex]
	moved, found, err := holder.CommitProofPublication()
	if err != nil || !found || !moved.Equal(playerB) || holder.InstanceID != claim.Authority.HolderID || holder.BootID != claim.Authority.HolderBootID {
		t.Fatalf("moved holder=%+v publication=%+v found=%v err=%v", holder, moved, found, err)
	}
	for _, retained := range snapshot.Holders {
		if retained.InstanceID == "player-a" && retained.CommitProofKeyID != "" {
			t.Fatal("retired player-a boot key remained in compacted snapshot")
		}
	}
}
