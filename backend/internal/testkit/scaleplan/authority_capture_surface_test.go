package scaleplan_test

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// C13 preflight AUTHORITY_CAPTURE_READ_SURFACE, section 12.7: decide whether
// the existing read-only authority surface can produce a consistent snapshot of
// lease epoch and assignment revision, before the capture token is frozen. The
// determination is SUFFICIENT, and it rests on facts that a paragraph in a
// document cannot keep true, so they are pinned here.
//
// Why the question is not trivial. Authority records are keyed one per
// (domain, layout version, virtual shard) -- see AuthorityBucketForShard in
// rediskey/profile.go:54 and AuthorityKeys.ShardAuthority in keyspace.go:155 --
// and the only read is ReadShardWriteAuthorityObservation
// (redis/authority/reader.go:164), which is atomic for a single key. There is
// no domain-wide authority revision key and no cross-shard atomic read, so a
// domain of N shards must be read in N calls. Section 8.2 forbids stitching
// per-shard calls together, which reads at first like a verdict of
// insufficient.
//
// Why it is nevertheless sufficient. Section 8.2 does not ask for an atomic
// read, it asks for a bracket: read everything, read the identity again, and
// accept only if nothing moved. That bracket is sound without atomicity
// exactly when the compared quantities cannot return to an earlier value.
// Suppose two strictly sequential passes, shard i read at t_i in the first and
// u_i in the second, with every t_i before every u_i. If each shard's epochs
// are equal across the two passes and the quantities are monotonic, then no
// shard changed anywhere in [t_i, u_i]. The intersection of those intervals
// contains [max t_i, min u_i], which is non-empty because the passes do not
// overlap. Every shard was therefore simultaneously unchanged over a real
// interval, and the capture linearizes to it.
//
// So the verdict has no delta for A1R to implement in the read packages, but
// it does carry three obligations on whoever writes the adapter:
//
//	1. the two passes must not overlap, or the common interval vanishes;
//	2. compare per-shard epochs, not a sum, since a sum lets two shards
//	   moving in opposite directions cancel (this is why the token carries
//	   LeaseEpochDigest rather than a total);
//	3. compare active cardinality too, since monotonicity says nothing about
//	   a shard that appears or disappears between passes.
//
// Obligation 3 is why the token gained ActiveCardinality. Obligations 1 and 2
// are A1R implementation constraints recorded in the handoff.
//
// The premise the whole argument stands on is monotonicity, and the way it
// would most plausibly break is someone making unassign delete the record
// instead of marking it. Then a shard could read absent, be claimed, be
// unassigned, and read absent again, with both brackets agreeing about a shard
// that was briefly live. TestAuthorityEpochsAreMonotonicAcrossUnassign fails
// if that ever happens, and the C13 verdict has to be reopened.

const captureLayoutVersion = "auth-v1"

type captureClock struct{ ms int64 }

func (c *captureClock) Now() time.Time          { return time.UnixMilli(c.ms) }
func (c *captureClock) advance(d time.Duration) { c.ms += d.Milliseconds() }

func authorityFixture(t *testing.T) (*authoritystore.MemoryStore, *captureClock) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator("test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 0x9E3779B97F4A7C15}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	clock := &captureClock{ms: 1_700_000_000_000}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: captureLayoutVersion, Now: clock.Now,
		LeaseLimits: authoritystore.LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: 10 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, clock
}

func captureRouteManager() authoritystore.TrustedShardAuthorityCaller {
	return authoritystore.TrustedShardAuthorityCaller{
		Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "rm-boot-1"}
}

func captureFenceOf(value authoritystore.ShardWriteAuthority) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{
		ExpectedHolderID: value.HolderID, ExpectedHolderBootID: value.HolderBootID,
		ExpectedAssignmentRevision: value.AssignmentRevision,
		ExpectedWriterEpoch:        value.WriterEpoch, ExpectedLeaseEpoch: value.LeaseEpoch,
	}
}

// The three quantities the capture compares must all be reachable from the
// existing read result. If any of them were absent the verdict would be
// insufficient regardless of the monotonicity argument.
func TestAuthorityReadSurfaceExposesTheComparedQuantities(t *testing.T) {
	store, _ := authorityFixture(t)
	if _, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: captureRouteManager(), TransitionID: "t-claim-1", AuthorityLayoutVersion: captureLayoutVersion,
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		NewHolderID: "holder-a", NewHolderBootID: "boot-a", RequestedTTLms: 1_000,
	}); err != nil {
		t.Fatal(err)
	}

	value, found := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)
	if !found {
		t.Fatal("claimed shard is not readable")
	}
	if value.LeaseEpoch == 0 || value.AssignmentRevision == 0 || value.WriterEpoch == 0 {
		t.Fatalf("read surface does not carry the compared quantities: %+v", value)
	}
}

// The load-bearing fact. Unassign must leave a readable record with strictly
// higher epochs rather than removing it.
func TestAuthorityEpochsAreMonotonicAcrossUnassign(t *testing.T) {
	store, _ := authorityFixture(t)
	if _, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: captureRouteManager(), TransitionID: "t-claim-1", AuthorityLayoutVersion: captureLayoutVersion,
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		NewHolderID: "holder-a", NewHolderBootID: "boot-a", RequestedTTLms: 1_000,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, _ := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)

	if _, err := store.UnassignShardAuthority(context.Background(), authoritystore.UnassignShardAuthorityRequest{
		Caller: captureRouteManager(), TransitionID: "t-unassign-1", AuthorityLayoutVersion: captureLayoutVersion,
		Domain: actorrouting.DomainFarm, VirtualShard: 17, ExpectedFence: captureFenceOf(claimed),
	}); err != nil {
		t.Fatal(err)
	}

	unassigned, found := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)
	if !found {
		t.Fatal("unassign removed the record; bracketed capture can no longer rule out an " +
			"absent-claimed-absent cycle, so the C13 verdict must be reopened")
	}
	if unassigned.AssignmentRevision <= claimed.AssignmentRevision ||
		unassigned.LeaseEpoch <= claimed.LeaseEpoch ||
		unassigned.WriterEpoch <= claimed.WriterEpoch {
		t.Fatalf("unassign did not advance every compared quantity: %+v -> %+v", claimed, unassigned)
	}

	// A shard that comes back must not reuse an epoch the capture already saw.
	if _, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: captureRouteManager(), TransitionID: "t-claim-2", AuthorityLayoutVersion: captureLayoutVersion,
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		NewHolderID: "holder-b", NewHolderBootID: "boot-b", RequestedTTLms: 1_000,
	}); err != nil {
		t.Fatal(err)
	}
	reclaimed, _ := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)
	if reclaimed.AssignmentRevision <= unassigned.AssignmentRevision || reclaimed.LeaseEpoch <= unassigned.LeaseEpoch {
		t.Fatalf("reclaim reset an epoch, which makes a capture bracket forgeable: %+v -> %+v", unassigned, reclaimed)
	}
}

// Renew is the transition the exemption in section 8.6 lets run during a
// capture, so it is the one transition that must leave every compared quantity
// alone. Section 8.2 excludes the deadline from the token for this reason;
// this checks the store actually behaves the way that exclusion assumes.
func TestRenewDoesNotAdvanceAnyComparedQuantity(t *testing.T) {
	store, clock := authorityFixture(t)
	if _, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: captureRouteManager(), TransitionID: "t-claim-1", AuthorityLayoutVersion: captureLayoutVersion,
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		NewHolderID: "holder-a", NewHolderBootID: "boot-a", RequestedTTLms: 1_000,
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)

	// Renew carries no TransitionID, unlike claim, transfer and unassign. That
	// omission is the type system agreeing with section 8.6: a renew is not a
	// transition, so there is nothing for the quiescence enumeration to see and
	// nothing for the capture bracket to trip over.
	clock.advance(200 * time.Millisecond)
	result, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "holder-a", BootID: "boot-a"},
		AuthorityLayoutVersion: captureLayoutVersion,
		Domain:                 actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: captureFenceOf(before), RequestedTTLms: 1_000,
	})
	if err != nil || result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("renew=%+v err=%v", result, err)
	}

	after, _ := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)
	if captureFenceOf(after) != captureFenceOf(before) {
		t.Fatalf("renew moved a quantity the capture compares, so the liveness exemption in "+
			"section 8.6 does not hold: %+v -> %+v", captureFenceOf(before), captureFenceOf(after))
	}
	if after.LeaseUntilMs <= before.LeaseUntilMs {
		t.Fatal("renew did not extend the deadline, so it is not a renew")
	}
}
