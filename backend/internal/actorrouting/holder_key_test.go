package actorrouting

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"farmgame/internal/holderkey"
)

func holderKeySnapshot(revision uint64) RouteSnapshot {
	deadline := time.Now().Add(time.Hour).UnixMilli()
	return RouteSnapshot{
		Realm: "holder-key-test", GlobalRevision: revision,
		HashVersion: HashVersionXXH64V2, HashSeed: 202608040001,
		Player: DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline}}},
		Farm:   DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 1, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline}}},
		Holders: []HolderRef{
			{InstanceID: "player-a", BootID: "player-boot", Endpoint: "player:9000", State: HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-boot", Endpoint: "farm:9000", State: HolderStateActive, Weight: 1},
		},
	}
}

func randomHolderPublication(t *testing.T) holderkey.Publication {
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

func TestHolderPublicKeyValidationBoundsAndPreventsSharing(t *testing.T) {
	publication := randomHolderPublication(t)
	snapshot := holderKeySnapshot(1)
	if err := snapshot.Holders[1].SetCommitProofPublication(publication); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStaticResolver(snapshot); err != nil {
		t.Fatalf("valid publication rejected: %v", err)
	}

	partial := CloneRouteSnapshot(snapshot)
	partial.Holders[1].CommitProofPublicKeyBase64 = ""
	if _, err := NewStaticResolver(partial); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("partial publication err=%v", err)
	}
	shared := CloneRouteSnapshot(snapshot)
	shared.Holders[0].CommitProofKeyID = shared.Holders[1].CommitProofKeyID
	shared.Holders[0].CommitProofPublicKeyBase64 = shared.Holders[1].CommitProofPublicKeyBase64
	if _, err := NewStaticResolver(shared); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("shared holder key err=%v", err)
	}

	duplicateRef := CloneRouteSnapshot(snapshot)
	duplicate := duplicateRef.Holders[1]
	duplicate.Endpoint = "farm-duplicate-ref:9000"
	duplicateRef.Holders = append(duplicateRef.Holders, duplicate)
	if _, err := NewStaticResolver(duplicateRef); err != nil {
		t.Fatalf("same holder/boot duplicate ref must share one publication: %v", err)
	}
}

func TestHolderPublicKeyCardinalityIsBounded(t *testing.T) {
	snapshot := holderKeySnapshot(1)
	for index := 0; index <= MaxCommitProofPublishedKeys; index++ {
		seed := sha256.Sum256([]byte(fmt.Sprintf("holder-key-%d", index)))
		private := ed25519.NewKeyFromSeed(seed[:])
		publication, err := holderkey.New(private.Public().(ed25519.PublicKey))
		if err != nil {
			t.Fatal(err)
		}
		holder := HolderRef{InstanceID: fmt.Sprintf("unused-%d", index), BootID: fmt.Sprintf("boot-%d", index), Endpoint: fmt.Sprintf("unused-%d:9000", index), State: HolderStateAssigning, Weight: 1}
		if err := holder.SetCommitProofPublication(publication); err != nil {
			t.Fatal(err)
		}
		snapshot.Holders = append(snapshot.Holders, holder)
	}
	if _, err := NewStaticResolver(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("over-cardinality publication err=%v", err)
	}
}

func TestLookupRouteInstallsKeyWithSnapshotAndForbidsBootLocalRotation(t *testing.T) {
	initial := holderKeySnapshot(1)
	resolver, err := NewStaticResolver(initial)
	if err != nil {
		t.Fatal(err)
	}
	published := CloneRouteSnapshot(initial)
	published.GlobalRevision = 2
	publication := randomHolderPublication(t)
	if err := published.Holders[1].SetCommitProofPublication(publication); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Replace(published); err != nil {
		t.Fatal(err)
	}
	view, err := resolver.LookupRoute(context.Background(), Address{Realm: published.Realm, Kind: DomainFarm, ID: 42})
	if err != nil || view.GlobalRevision != 2 {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	got, found, err := view.Holder.CommitProofPublication()
	if err != nil || !found || !got.Equal(publication) {
		t.Fatalf("publication found=%v got=%+v err=%v", found, got, err)
	}

	rotated := CloneRouteSnapshot(published)
	rotated.GlobalRevision = 3
	if err := rotated.Holders[1].SetCommitProofPublication(randomHolderPublication(t)); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Replace(rotated); !errors.Is(err, ErrSnapshotHolderGeneration) {
		t.Fatalf("same-boot key rotation err=%v", err)
	}
}
