package holdercontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/storage/authoritystore"
)

type fakeMembershipInstaller struct {
	mu sync.Mutex

	prepareCalls   int
	commitCalls    int
	prepareErr     error
	commitErr      error
	prepareHook    func()
	leaseExtension time.Duration
}

func (f *fakeMembershipInstaller) PrepareMembership(_ context.Context, member authoritycomposition.ShardMembership) (*authoritycomposition.PreparedMembership, error) {
	f.mu.Lock()
	f.prepareCalls++
	prepareErr := f.prepareErr
	hook := f.prepareHook
	leaseExtension := f.leaseExtension
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if prepareErr != nil {
		return nil, prepareErr
	}
	preparedGeneration := member.Generation
	preparedGeneration.LeaseUntilMs += leaseExtension.Milliseconds()
	return &authoritycomposition.PreparedMembership{
		Key:            member.Key,
		Generation:     preparedGeneration,
		RequestStarted: time.UnixMilli(100),
		AcceptedTTLms:  5_000,
		Authority: authoritystore.ShardWriteAuthority{
			Domain:             member.Key.Domain,
			VirtualShard:       member.Key.Shard,
			HolderID:           preparedGeneration.HolderID,
			HolderBootID:       preparedGeneration.HolderBootID,
			AssignmentRevision: preparedGeneration.AssignmentRevision,
			WriterEpoch:        preparedGeneration.WriterEpoch,
			LeaseEpoch:         preparedGeneration.LeaseEpoch,
			State:              authoritystore.ShardAuthorityActive,
			LeaseUntilMs:       preparedGeneration.LeaseUntilMs,
		},
	}, nil
}

func (f *fakeMembershipInstaller) CommitPreparedMembership(_ context.Context, _ authoritycomposition.PreparedMembership) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitCalls++
	return f.commitErr
}

func membershipTestMember(revision uint64) authoritycomposition.ShardMembership {
	return authoritycomposition.ShardMembership{
		Key: authoritycomposition.ShardKey{
			Domain: actorrouting.DomainPlayer,
			Shard:  3,
		},
		Generation: authoritycomposition.MembershipGeneration{
			AssignmentRevision: revision,
			WriterEpoch:        revision,
			LeaseEpoch:         revision,
			HolderID:           "holder-a",
			HolderBootID:       "boot-a",
			LeaseUntilMs:       1770000000000 + int64(revision),
		},
	}
}

func TestPrepareExpiryStartsAfterInnerPrepareReturns(t *testing.T) {
	now := time.UnixMilli(1_000)
	inner := &fakeMembershipInstaller{
		prepareHook: func() {
			now = now.Add(2 * time.Second)
		},
	}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{
		Inner:     inner,
		Retention: time.Second,
		Now:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	member := membershipTestMember(1)
	if _, err := installer.PrepareMembership(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	status, err := installer.PreparedShardGeneration(context.Background(), member.Key)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Prepared || status.Generation == nil {
		t.Fatalf("prepared entry expired from pre-inner timestamp: %+v", status)
	}
}

func TestPrepareOnlyMembershipInstallerAcceptsExactRenewedDeadline(t *testing.T) {
	inner := &fakeMembershipInstaller{leaseExtension: 5 * time.Second}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{Inner: inner, Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	member := membershipTestMember(1)
	prepared, err := installer.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Generation.LeaseUntilMs != member.Generation.LeaseUntilMs+5_000 ||
		prepared.Authority.LeaseUntilMs != prepared.Generation.LeaseUntilMs {
		t.Fatalf("renewed prepared=%+v", prepared)
	}
	replayed, err := installer.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	if inner.prepareCalls != 1 || *replayed != *prepared {
		t.Fatalf("prepare calls=%d replayed=%+v prepared=%+v", inner.prepareCalls, replayed, prepared)
	}
}

func TestPrepareOnlyMembershipInstallerRefreshesMonotonicDeadline(t *testing.T) {
	inner := &fakeMembershipInstaller{leaseExtension: 5 * time.Second}
	installer, err := NewPrepareOnlyMembershipInstaller(
		PrepareOnlyMembershipInstallerOptions{
			Inner:     inner,
			Retention: time.Minute,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	member := membershipTestMember(1)
	first, err := installer.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	refreshedMember := member
	refreshedMember.Generation.LeaseUntilMs =
		first.Generation.LeaseUntilMs + 1_000
	refreshed, err := installer.PrepareMembership(
		context.Background(),
		refreshedMember,
	)
	if err != nil {
		t.Fatal(err)
	}
	if inner.prepareCalls != 2 ||
		refreshed.Generation.LeaseUntilMs !=
			refreshedMember.Generation.LeaseUntilMs+5_000 {
		t.Fatalf(
			"prepare calls=%d refreshed=%+v",
			inner.prepareCalls,
			refreshed,
		)
	}
	status, err := installer.PreparedShardGeneration(
		context.Background(),
		member.Key,
	)
	if err != nil || !status.Prepared || status.Generation == nil ||
		status.Generation.LeaseUntilMs != refreshed.Generation.LeaseUntilMs {
		t.Fatalf("refreshed status=%+v err=%v", status, err)
	}

	inner.mu.Lock()
	inner.prepareErr = errors.New("renew unavailable")
	inner.mu.Unlock()
	failedMember := member
	failedMember.Generation.LeaseUntilMs =
		refreshed.Generation.LeaseUntilMs + 1_000
	if _, err := installer.PrepareMembership(
		context.Background(),
		failedMember,
	); err == nil {
		t.Fatal("deadline refresh failure accepted")
	}
	status, err = installer.PreparedShardGeneration(
		context.Background(),
		member.Key,
	)
	if err != nil || !status.Prepared || status.Generation == nil ||
		status.Generation.LeaseUntilMs != refreshed.Generation.LeaseUntilMs {
		t.Fatalf("failed refresh replaced safe cache status=%+v err=%v", status, err)
	}
}

func TestPrepareOnlyMembershipInstallerReusesRPCPrepareAndCommitsOnlyOnce(t *testing.T) {
	inner := &fakeMembershipInstaller{}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{
		Inner:     inner,
		Retention: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	member := membershipTestMember(1)
	generation := AuthorityGeneration{
		HolderID:           member.Generation.HolderID,
		HolderBootID:       member.Generation.HolderBootID,
		AssignmentRevision: member.Generation.AssignmentRevision,
		WriterEpoch:        member.Generation.WriterEpoch,
		LeaseEpoch:         member.Generation.LeaseEpoch,
		LeaseUntilMs:       member.Generation.LeaseUntilMs,
	}
	if err := installer.PrepareShardGeneration(context.Background(), member.Key, generation); err != nil {
		t.Fatal(err)
	}
	if inner.commitCalls != 0 {
		t.Fatalf("RPC Prepare committed: %d", inner.commitCalls)
	}
	prepared, err := installer.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	if inner.prepareCalls != 1 {
		t.Fatalf("exact Prepare did not reuse cache: %d", inner.prepareCalls)
	}
	if err := installer.CommitPreparedMembership(context.Background(), *prepared); err != nil {
		t.Fatal(err)
	}
	if inner.commitCalls != 1 || installer.PreparedCount() != 0 {
		t.Fatalf("commit state calls=%d prepared=%d", inner.commitCalls, installer.PreparedCount())
	}
	if err := installer.CommitPreparedMembership(context.Background(), *prepared); !errors.Is(err, ErrPreparedNotFound) {
		t.Fatalf("replayed commit err=%v", err)
	}
}

func TestPrepareOnlyMembershipInstallerRejectsConflictOverflowAndExpiry(t *testing.T) {
	now := time.UnixMilli(1000)
	inner := &fakeMembershipInstaller{}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{
		Inner:      inner,
		MaxEntries: 1,
		Retention:  time.Second,
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	first := membershipTestMember(1)
	if _, err := installer.PrepareMembership(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := installer.PrepareMembership(context.Background(), membershipTestMember(2)); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("same-shard conflict err=%v", err)
	}
	now = now.Add(2 * time.Second)
	if _, err := installer.PrepareMembership(context.Background(), first); !errors.Is(err, ErrPreparedExpired) {
		t.Fatalf("expired Prepare err=%v", err)
	}

	second := membershipTestMember(2)
	if _, err := installer.PrepareMembership(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	third := second
	third.Key.Shard = 4
	if _, err := installer.PrepareMembership(context.Background(), third); !errors.Is(err, ErrPreparedOverflow) {
		t.Fatalf("capacity err=%v", err)
	}
}

func TestPrepareOnlyMembershipInstallerFailsClosedOnAuthorityIdentityMismatch(t *testing.T) {
	inner := &mismatchedMembershipInstaller{}
	installer, err := NewPrepareOnlyMembershipInstaller(PrepareOnlyMembershipInstallerOptions{Inner: inner})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := installer.PrepareMembership(context.Background(), membershipTestMember(1)); !errors.Is(err, ErrPreparedIdentity) {
		t.Fatalf("mismatched authority err=%v", err)
	}
	if installer.PreparedCount() != 0 {
		t.Fatal("mismatched authority was cached")
	}
}

type mismatchedMembershipInstaller struct{}

func (*mismatchedMembershipInstaller) PrepareMembership(_ context.Context, member authoritycomposition.ShardMembership) (*authoritycomposition.PreparedMembership, error) {
	return &authoritycomposition.PreparedMembership{
		Key:            member.Key,
		Generation:     member.Generation,
		RequestStarted: time.UnixMilli(100),
		AcceptedTTLms:  1000,
		Authority: authoritystore.ShardWriteAuthority{
			Domain:             member.Key.Domain,
			VirtualShard:       member.Key.Shard,
			HolderID:           "other-holder",
			HolderBootID:       member.Generation.HolderBootID,
			AssignmentRevision: member.Generation.AssignmentRevision,
			WriterEpoch:        member.Generation.WriterEpoch,
			LeaseEpoch:         member.Generation.LeaseEpoch,
			State:              authoritystore.ShardAuthorityActive,
			LeaseUntilMs:       member.Generation.LeaseUntilMs,
		},
	}, nil
}

func (*mismatchedMembershipInstaller) CommitPreparedMembership(context.Context, authoritycomposition.PreparedMembership) error {
	return nil
}
