package sourceorder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/holderkey"
)

func routeKeySnapshot(t *testing.T, statement CommitProofStatement, publication holderkey.Publication, revision uint64) actorrouting.RouteSnapshot {
	t.Helper()
	snapshot := actorrouting.RouteSnapshot{
		Realm: statement.SourceRealm, GlobalRevision: revision,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: statement.LeaseUntilMs,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: statement.AssignmentRevision,
			ExpectedWriterEpoch: statement.WriterEpoch, ExpectedLeaseEpoch: statement.LeaseEpoch, LeaseUntilMs: statement.LeaseUntilMs,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-boot", Endpoint: "player:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: statement.SourceHolderID, BootID: statement.SourceHolderBootID, Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	if err := snapshot.Holders[1].SetCommitProofPublication(publication); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestEphemeralHolderBootSignerIsUniqueAcrossProcessRestarts(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for restart := 0; restart < 64; restart++ {
		signer, publication, err := NewEphemeralHolderBootSigner("realm", "holder-a", "boot-a")
		if err != nil || signer == nil || !signer.MatchesPublication(publication) {
			t.Fatalf("restart=%d signer=%v publication=%+v err=%v", restart, signer, publication, err)
		}
		if _, duplicate := seen[publication.KeyID]; duplicate {
			t.Fatalf("process restart reused key ID %q", publication.KeyID)
		}
		seen[publication.KeyID] = struct{}{}
		for _, rendered := range []string{fmt.Sprint(signer), fmt.Sprintf("%+v", signer), fmt.Sprintf("%#v", signer)} {
			if !strings.Contains(rendered, "REDACTED") || strings.Contains(rendered, "private:[") {
				t.Fatalf("signer debug rendering exposed private field: %q", rendered)
			}
		}
	}
}

func TestRouteCommitProofKeyringWatchesBootRotationRevocationAndHolderMove(t *testing.T) {
	statement := testCommitProofStatement(t)
	statement.Work.BucketID = 0
	signerA, publicA, err := NewEphemeralHolderBootSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID)
	if err != nil {
		t.Fatal(err)
	}
	proofA, err := signerA.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := actorrouting.NewStaticResolver(routeKeySnapshot(t, statement, publicA, 1))
	if err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.UnixMilli(statement.CommittedAtMs + 1_000) }
	validator, err := NewRouteCommitProofValidator(resolver, statement.SourceRealm, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proofA, commitProofExpected(statement)); err != nil {
		t.Fatalf("initial proof rejected: %v", err)
	}

	rotatedStatement := statement
	rotatedStatement.SourceHolderBootID = "farm-boot-b"
	rotatedStatement.AssignmentRevision++
	rotatedStatement.WriterEpoch++
	rotatedStatement.LeaseEpoch++
	signerB, publicB, err := NewEphemeralHolderBootSigner(rotatedStatement.SourceRealm, rotatedStatement.SourceHolderID, rotatedStatement.SourceHolderBootID)
	if err != nil {
		t.Fatal(err)
	}
	proofB, err := signerB.Sign(rotatedStatement)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.Replace(routeKeySnapshot(t, rotatedStatement, publicB, 2)); err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proofA, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofRoute) {
		t.Fatalf("retired boot proof err=%v", err)
	}
	if err := validator.Verify(context.Background(), proofB, commitProofExpected(rotatedStatement)); err != nil {
		t.Fatalf("rotated boot proof rejected: %v", err)
	}

	revoked := routeKeySnapshot(t, rotatedStatement, publicB, 3)
	revoked.Holders[1].ClearCommitProofPublication()
	if err := resolver.Replace(revoked); err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proofB, commitProofExpected(rotatedStatement)); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("revoked key proof err=%v", err)
	}

	movedStatement := rotatedStatement
	movedStatement.SourceHolderID = "farm-holder-c"
	movedStatement.SourceHolderBootID = "farm-boot-c"
	movedStatement.AssignmentRevision++
	movedStatement.WriterEpoch++
	movedStatement.LeaseEpoch++
	signerC, publicC, err := NewEphemeralHolderBootSigner(movedStatement.SourceRealm, movedStatement.SourceHolderID, movedStatement.SourceHolderBootID)
	if err != nil {
		t.Fatal(err)
	}
	proofC, err := signerC.Sign(movedStatement)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.Replace(routeKeySnapshot(t, movedStatement, publicC, 4)); err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proofB, commitProofExpected(rotatedStatement)); !errors.Is(err, ErrCommitProofRoute) {
		t.Fatalf("moved holder old proof err=%v", err)
	}
	if err := validator.Verify(context.Background(), proofC, commitProofExpected(movedStatement)); err != nil {
		t.Fatalf("moved holder proof rejected: %v", err)
	}
}

func TestRouteCommitProofKeyringFollowsRevisionedRouteWatch(t *testing.T) {
	statement := testCommitProofStatement(t)
	statement.Work.BucketID = 0
	signer, publication, err := NewEphemeralHolderBootSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	initial := routeKeySnapshot(t, statement, publication, 1)
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := actorrouting.NewStaticResolver(initial)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := actorrouting.NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Run(ctx) }()
	waitUntil := func(revision uint64) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for resolver.Snapshot().GlobalRevision != revision {
			if time.Now().After(deadline) {
				t.Fatalf("route watch did not apply revision %d", revision)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitUntil(1)
	validator, err := NewRouteCommitProofValidator(resolver, statement.SourceRealm, time.Minute,
		func() time.Time { return time.UnixMilli(statement.CommittedAtMs + 1_000) })
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proof, commitProofExpected(statement)); err != nil {
		t.Fatal(err)
	}
	revoked := actorrouting.CloneRouteSnapshot(initial)
	revoked.GlobalRevision = 2
	revoked.Holders[1].ClearCommitProofPublication()
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, revoked); err != nil {
		t.Fatal(err)
	}
	waitUntil(2)
	if err := validator.Verify(context.Background(), proof, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("watched revocation proof err=%v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("route watch exit=%v", err)
	}
}

func TestProductionPreferReadinessRequiresAllActiveRouteKeys(t *testing.T) {
	statement := testCommitProofStatement(t)
	statement.Work.BucketID = 0
	signer, publication, err := NewEphemeralHolderBootSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := routeKeySnapshot(t, statement, publication, 1)
	if err := ValidateRouteCommitProofKeys(snapshot, actorrouting.DomainFarm); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalCommitProofSignerPublication(snapshot, actorrouting.DomainFarm,
		statement.SourceHolderID, statement.SourceHolderBootID, publication); err != nil {
		t.Fatal(err)
	}
	if !signer.MatchesPublication(publication) {
		t.Fatal("ephemeral signer/publication mismatch")
	}
	snapshot.Holders[1].ClearCommitProofPublication()
	if err := ValidateRouteCommitProofKeys(snapshot, actorrouting.DomainFarm); !errors.Is(err, ErrCommitProofBypassUnavailable) {
		t.Fatalf("missing destination key err=%v", err)
	}
	if err := ValidateProductionPreferComposition(CommitProofModePrefer, false, true); !errors.Is(err, ErrCommitProofBypassUnavailable) {
		t.Fatalf("static prefer composition err=%v", err)
	}
	if err := ValidateProductionPreferComposition(CommitProofModePrefer, true, true); err != nil {
		t.Fatalf("fully isolated explicit prefer rejected: %v", err)
	}
}
