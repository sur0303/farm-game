package scaleplan_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
	testkit "farmgame/internal/testkit/scaleplan"
)

// Section 8.6 exempts holder-side same-generation lease renew from the control
// gate, and the exemption is not optional: putting renew behind the gate would
// stop every holder in the realm from renewing exactly while a gate is CLOSING,
// which is every scale operation, so leases would expire in bulk and authority
// would be lost across the board. The plan is explicit that an exemption is not
// a pass on proof, and lists four obligations. These tests are where A0.5
// discharges them, since the six files that actually renew are in no stage's
// write set.

const renewDomain = actorrouting.RouteDomain("FARM")

func placementFixture() *testkit.FakePlacements {
	return testkit.NewFakePlacements(core.ShardPlacement{
		Domain: renewDomain, VirtualShard: 7, Active: true,
		Holder:   core.HolderRef{HolderID: "holder-a", BootID: "boot-1"},
		Endpoint: "198.18.0.2:9000",
		ExpectedSourceFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-1",
			ExpectedAssignmentRevision: 4, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 3,
		},
		KnownFresh: true,
		LeaseUntil: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
	})
}

// Obligation 1: make the invariant explicit instead of leaving it to a comment
// and an implicit check inside renewalMatchesRequest.
func TestSameGenerationRenewMovesOnlyTheDeadline(t *testing.T) {
	placements := placementFixture()
	before, err := placements.CaptureShard(context.Background(), renewDomain, 7)
	if err != nil {
		t.Fatal(err)
	}

	deadline := before.Placement.LeaseUntil.Add(30 * time.Second)
	if !placements.RenewLease(renewDomain, 7, deadline) {
		t.Fatal("renew rejected")
	}

	after, err := placements.CaptureShard(context.Background(), renewDomain, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Placement.LeaseUntil.Equal(deadline) {
		t.Fatalf("renew did not extend the deadline: %s", after.Placement.LeaseUntil)
	}
	if after.Placement.Holder != before.Placement.Holder {
		t.Fatal("renew changed holder identity")
	}
	if after.Placement.ExpectedSourceFence != before.Placement.ExpectedSourceFence {
		t.Fatalf("renew changed the write fence: %+v -> %+v",
			before.Placement.ExpectedSourceFence, after.Placement.ExpectedSourceFence)
	}
	if after.Placement.Active != before.Placement.Active {
		t.Fatal("renew changed placement activity")
	}

	// A renew that tried to move the deadline backwards is not a renew.
	if placements.RenewLease(renewDomain, 7, deadline.Add(-time.Second)) {
		t.Fatal("deadline moved backwards")
	}
}

// Obligation 2, enforced mechanically. A reviewer reading the struct is exactly
// what let three inert parameters through this stage, so this asserts on the
// type rather than on prose.
func TestCaptureTokenCannotCarryAClockReading(t *testing.T) {
	tokenType := reflect.TypeOf(core.PlacementCaptureToken{})
	timeType := reflect.TypeOf(time.Time{})
	durationType := reflect.TypeOf(time.Duration(0))

	for i := 0; i < tokenType.NumField(); i++ {
		field := tokenType.Field(i)
		if field.Type == timeType || field.Type == durationType {
			t.Fatalf("%s is a clock reading; a renew would invalidate every capture", field.Name)
		}
		for _, banned := range []string{"deadline", "expiry", "expires", "until", "ttl", "leaseuntil"} {
			if strings.Contains(strings.ToLower(field.Name), banned) {
				t.Fatalf("%s names a deadline; see section 8.2", field.Name)
			}
		}
		if !field.Type.Comparable() {
			t.Fatalf("%s is not comparable, but Revalidate compares tokens with ==", field.Name)
		}
	}

	// The substitutes must actually be present. Excluding the deadline is
	// trivially satisfied by an empty token, which would pass the gate while
	// proving nothing.
	for _, required := range []string{"LeaseEpochDigest", "AssignmentRevision", "ActiveCardinality"} {
		if _, ok := tokenType.FieldByName(required); !ok {
			t.Fatalf("token is missing %s, so it cannot detect a real placement change", required)
		}
	}
}

// Obligation 3, the adversarial half that matters: renew runs throughout and
// the capture must survive it.
func TestCaptureSurvivesContinuousRenew(t *testing.T) {
	placements := placementFixture()
	capture, err := placements.CaptureShard(context.Background(), renewDomain, 7)
	if err != nil {
		t.Fatal(err)
	}

	deadline := capture.Placement.LeaseUntil
	for i := 0; i < 50; i++ {
		deadline = deadline.Add(time.Second)
		if !placements.RenewLease(renewDomain, 7, deadline) {
			t.Fatalf("renew %d rejected", i)
		}
		if err := placements.Revalidate(context.Background(), capture.Token); err != nil {
			t.Fatalf("renew %d invalidated the capture: %v", i, err)
		}
	}
}

// Obligation 3, the other half: a real control-plane change must invalidate.
// Without this the test above would also pass against a token that compares
// nothing at all.
func TestCaptureFailsOnRealPlacementChange(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testkit.FakePlacements) bool
	}{
		{"transfer to another holder", func(p *testkit.FakePlacements) bool {
			return p.Move(renewDomain, 7, core.HolderRef{HolderID: "holder-b", BootID: "boot-9"}, "198.18.0.199:9000")
		}},
		{"unassign", func(p *testkit.FakePlacements) bool {
			return p.Unassign(renewDomain, 7)
		}},
		{"a new shard appears", func(p *testkit.FakePlacements) bool {
			p.Set(core.ShardPlacement{
				Domain: renewDomain, VirtualShard: 8, Active: true,
				Holder: core.HolderRef{HolderID: "holder-c", BootID: "boot-3"},
			})
			return true
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			placements := placementFixture()
			capture, err := placements.CaptureShard(context.Background(), renewDomain, 7)
			if err != nil {
				t.Fatal(err)
			}
			if !testCase.mutate(placements) {
				t.Fatal("mutation did not apply")
			}
			if err := placements.Revalidate(context.Background(), capture.Token); !errors.Is(err, core.ErrConflict) {
				t.Fatalf("capture survived a real placement change: %v", err)
			}
		})
	}
}

// Obligation 4 is a rule about what invalidates the exemption rather than a
// property of today's code, so this pins the boundary the rule is stated
// against: renew touches the deadline and nothing else. If a renew path ever
// gains the ability to move holder, generation or placement, this fails and
// the exemption has to go back to an A0 amendment rather than be patched in an
// adapter.
func TestRenewCannotReachGenerationOrPlacement(t *testing.T) {
	placements := placementFixture()
	before, err := placements.CaptureShard(context.Background(), renewDomain, 7)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		placements.RenewLease(renewDomain, 7, before.Placement.LeaseUntil.Add(time.Duration(i+1)*time.Minute))
	}
	after, err := placements.CaptureShard(context.Background(), renewDomain, 7)
	if err != nil {
		t.Fatal(err)
	}
	if after.Token != before.Token {
		t.Fatalf("renew moved the capture token:\n before %+v\n after  %+v", before.Token, after.Token)
	}
}
