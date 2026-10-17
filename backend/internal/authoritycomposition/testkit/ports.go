// Package testkit provides Fake implementations of the RT1B-2A membership
// Ports (and the RT1B-3A two-phase installer).  The Fake records every
// prepare/install/fence/remove call in arrival order and can inject per-call
// failures or a block on the network phase (prepare) for
// close-during-reconcile and supersede tests.  It is deliberately small and
// dependency-free beyond the core types.
package testkit

import (
	"context"
	"sync"
	"time"

	"farmgame/internal/authoritycomposition"
	"farmgame/internal/storage/authoritystore"
)

// FakeMembershipPorts is the Fake installer/fencer/remover.  All methods are
// safe for concurrent use; the recorded call slices are coherent snapshots
// under lock.  Blocking applies to PrepareMembership only: CommitPreparedMembership
// is a purely local bounded call and must never block.
type FakeMembershipPorts struct {
	mu sync.Mutex

	installs []authoritycomposition.ShardMembership
	prepares []authoritycomposition.ShardMembership
	fences   []authoritycomposition.ShardMembership
	removes  []authoritycomposition.ShardMembership
	order    []string

	installErr error
	fenceErr   error
	removeErr  error

	// Per-call predicate errors: when set, they override the plain error and
	// let tests scope a failure to one domain/shard.
	installErrFunc func(member authoritycomposition.ShardMembership) error
	fenceErrFunc   func(member authoritycomposition.ShardMembership) error
	removeErrFunc  func(member authoritycomposition.ShardMembership) error

	prepareBlock <-chan struct{}
	fenceBlock   <-chan struct{}
	removeBlock  <-chan struct{}
}

// NewFakeMembershipPorts returns a clean Fake with no injected failures.
func NewFakeMembershipPorts() *FakeMembershipPorts {
	return &FakeMembershipPorts{}
}

// Configure failures or blocking.  nil clears the injected behavior.
func (f *FakeMembershipPorts) SetInstallErr(err error) { f.mu.Lock(); defer f.mu.Unlock(); f.installErr = err }
func (f *FakeMembershipPorts) SetFenceErr(err error)   { f.mu.Lock(); defer f.mu.Unlock(); f.fenceErr = err }
func (f *FakeMembershipPorts) SetRemoveErr(err error)  { f.mu.Lock(); defer f.mu.Unlock(); f.removeErr = err }

// SetInstallErrFunc scopes the injected commit failure to selected members.
func (f *FakeMembershipPorts) SetInstallErrFunc(fn func(authoritycomposition.ShardMembership) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.installErrFunc = fn
}

// SetFenceErrFunc scopes the injected fence failure to selected members.
func (f *FakeMembershipPorts) SetFenceErrFunc(fn func(authoritycomposition.ShardMembership) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenceErrFunc = fn
}

// SetRemoveErrFunc scopes the injected remove failure to selected members.
func (f *FakeMembershipPorts) SetRemoveErrFunc(fn func(authoritycomposition.ShardMembership) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeErrFunc = fn
}

// SetPrepareBlock blocks PrepareMembership (the network phase) until the
// channel is closed or the reconciler context is cancelled.
func (f *FakeMembershipPorts) SetPrepareBlock(ch <-chan struct{}) { f.mu.Lock(); defer f.mu.Unlock(); f.prepareBlock = ch }

func (f *FakeMembershipPorts) SetFenceBlock(ch <-chan struct{}) { f.mu.Lock(); defer f.mu.Unlock(); f.fenceBlock = ch }
func (f *FakeMembershipPorts) SetRemoveBlock(ch <-chan struct{}) { f.mu.Lock(); defer f.mu.Unlock(); f.removeBlock = ch }

// PrepareMembership implements authoritycomposition.MembershipInstaller.  It
// records the prepare and may block (simulating the Renew network phase).
func (f *FakeMembershipPorts) PrepareMembership(ctx context.Context, member authoritycomposition.ShardMembership) (*authoritycomposition.PreparedMembership, error) {
	f.mu.Lock()
	f.prepares = append(f.prepares, member)
	f.order = append(f.order, "prepare")
	block := f.prepareBlock
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	return &authoritycomposition.PreparedMembership{
		Key:            member.Key,
		Generation:     member.Generation,
		RequestStarted: time.Now(),
		AcceptedTTLms:  1000,
		Authority: authoritystore.ShardWriteAuthority{
			Domain:             member.Key.Domain,
			VirtualShard:       member.Key.Shard,
			HolderID:           member.Generation.HolderID,
			HolderBootID:       member.Generation.HolderBootID,
			AssignmentRevision: member.Generation.AssignmentRevision,
			WriterEpoch:        member.Generation.WriterEpoch,
			LeaseEpoch:         member.Generation.LeaseEpoch,
			State:              authoritystore.ShardAuthorityActive,
			LeaseUntilMs:       time.Now().Add(time.Hour).UnixMilli(),
		},
	}, nil
}

// CommitPreparedMembership implements authoritycomposition.MembershipInstaller.
// It is the local bounded phase; it records the install and applies any
// injected commit error.  It never blocks.
func (f *FakeMembershipPorts) CommitPreparedMembership(ctx context.Context, prepared authoritycomposition.PreparedMembership) error {
	member := authoritycomposition.ShardMembership{Key: prepared.Key, Generation: prepared.Generation}
	f.mu.Lock()
	f.installs = append(f.installs, member)
	f.order = append(f.order, "install")
	err := f.installErr
	if f.installErrFunc != nil {
		err = f.installErrFunc(member)
	}
	f.mu.Unlock()
	return err
}

// FenceMembership implements authoritycomposition.MembershipFencer.
func (f *FakeMembershipPorts) FenceMembership(ctx context.Context, member authoritycomposition.ShardMembership) error {
	f.mu.Lock()
	f.fences = append(f.fences, member)
	f.order = append(f.order, "fence")
	block := f.fenceBlock
	err := f.fenceErr
	if f.fenceErrFunc != nil {
		err = f.fenceErrFunc(member)
	}
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-block:
		}
	}
	return err
}

// RemoveMembership implements authoritycomposition.MembershipRemover.
func (f *FakeMembershipPorts) RemoveMembership(ctx context.Context, member authoritycomposition.ShardMembership) error {
	f.mu.Lock()
	f.removes = append(f.removes, member)
	f.order = append(f.order, "remove")
	block := f.removeBlock
	err := f.removeErr
	if f.removeErrFunc != nil {
		err = f.removeErrFunc(member)
	}
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-block:
		}
	}
	return err
}

// Installs returns the recorded commit (install) calls.
func (f *FakeMembershipPorts) Installs() []authoritycomposition.ShardMembership {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]authoritycomposition.ShardMembership(nil), f.installs...)
}

// Prepares returns the recorded prepare calls.
func (f *FakeMembershipPorts) Prepares() []authoritycomposition.ShardMembership {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]authoritycomposition.ShardMembership(nil), f.prepares...)
}

// Fences returns the recorded fence calls.
func (f *FakeMembershipPorts) Fences() []authoritycomposition.ShardMembership {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]authoritycomposition.ShardMembership(nil), f.fences...)
}

// Removes returns the recorded remove calls.
func (f *FakeMembershipPorts) Removes() []authoritycomposition.ShardMembership {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]authoritycomposition.ShardMembership(nil), f.removes...)
}

// Order returns the recorded call order ("prepare"/"install"/"fence"/"remove").
func (f *FakeMembershipPorts) Order() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

// Reset clears every recorded call and injected behavior.
func (f *FakeMembershipPorts) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.installs = nil
	f.prepares = nil
	f.fences = nil
	f.removes = nil
	f.order = nil
	f.installErr = nil
	f.fenceErr = nil
	f.removeErr = nil
	f.installErrFunc = nil
	f.fenceErrFunc = nil
	f.removeErrFunc = nil
	f.prepareBlock = nil
	f.fenceBlock = nil
	f.removeBlock = nil
}
