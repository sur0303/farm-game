package holdercontrol

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/storage/authoritystore"
)

const (
	DefaultPreparedMembershipMaxEntries = 256
	DefaultPreparedMembershipMaxBytes   = 1 << 20
	DefaultPreparedMembershipRetention  = 5 * time.Minute
	preparedMembershipFixedBytes        = 128
)

type PrepareOnlyMembershipInstallerOptions struct {
	Inner      authoritycomposition.MembershipInstaller
	MaxEntries int
	MaxBytes   int
	Retention  time.Duration
	Now        func() time.Time
}

type preparedMembershipEntry struct {
	prepared   authoritycomposition.PreparedMembership
	bytes      int
	expiresAt  time.Time
	committing bool
}

// PrepareOnlyMembershipInstaller is the holder-side capability wrapper. RPC
// Prepare and the reconciler share the same exact prepared value; only the
// reconciler's CommitPreparedMembership can invoke the inner commit.
type PrepareOnlyMembershipInstaller struct {
	inner      authoritycomposition.MembershipInstaller
	maxEntries int
	maxBytes   int
	retention  time.Duration
	now        func() time.Time

	mu        sync.Mutex
	entries   map[authoritycomposition.ShardKey]preparedMembershipEntry
	preparing map[authoritycomposition.ShardKey]bool
	bytes     int
}

type DomainGenerationPreparer struct {
	Domain    actorrouting.RouteDomain
	Installer *PrepareOnlyMembershipInstaller
}

var _ ShardGenerationPreparer = (*DomainGenerationPreparer)(nil)
var _ ShardGenerationStatusReader = (*DomainGenerationPreparer)(nil)

func (p *DomainGenerationPreparer) PrepareShardGeneration(ctx context.Context, shard uint32, generation AuthorityGeneration) error {
	if p == nil || p.Installer == nil {
		return ErrRuntimeUnavailable
	}
	if p.Domain != actorrouting.DomainPlayer && p.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: domain preparer domain", ErrInvalidRequest)
	}
	return p.Installer.PrepareShardGeneration(ctx, authoritycomposition.ShardKey{
		Domain: p.Domain,
		Shard:  shard,
	}, generation)
}

func (p *DomainGenerationPreparer) PreparedShardGeneration(ctx context.Context, shard uint32) (PreparedGenerationStatus, error) {
	if p == nil || p.Installer == nil {
		return PreparedGenerationStatus{}, ErrRuntimeUnavailable
	}
	if p.Domain != actorrouting.DomainPlayer && p.Domain != actorrouting.DomainFarm {
		return PreparedGenerationStatus{}, fmt.Errorf("%w: domain preparer domain", ErrInvalidRequest)
	}
	return p.Installer.PreparedShardGeneration(ctx, authoritycomposition.ShardKey{
		Domain: p.Domain,
		Shard:  shard,
	})
}

var _ authoritycomposition.MembershipInstaller = (*PrepareOnlyMembershipInstaller)(nil)

func NewPrepareOnlyMembershipInstaller(opts PrepareOnlyMembershipInstallerOptions) (*PrepareOnlyMembershipInstaller, error) {
	if opts.Inner == nil {
		return nil, fmt.Errorf("%w: membership installer inner is required", ErrRuntimeUnavailable)
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = DefaultPreparedMembershipMaxEntries
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultPreparedMembershipMaxBytes
	}
	if opts.Retention <= 0 {
		opts.Retention = DefaultPreparedMembershipRetention
	}
	if opts.MaxBytes < preparedMembershipFixedBytes {
		return nil, fmt.Errorf("%w: prepared membership byte cap is too small", ErrPreparedOverflow)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &PrepareOnlyMembershipInstaller{
		inner:      opts.Inner,
		maxEntries: opts.MaxEntries,
		maxBytes:   opts.MaxBytes,
		retention:  opts.Retention,
		now:        opts.Now,
		entries:    make(map[authoritycomposition.ShardKey]preparedMembershipEntry),
		preparing:  make(map[authoritycomposition.ShardKey]bool),
	}, nil
}

func (i *PrepareOnlyMembershipInstaller) PrepareMembership(ctx context.Context, member authoritycomposition.ShardMembership) (*authoritycomposition.PreparedMembership, error) {
	if i == nil || i.inner == nil {
		return nil, ErrRuntimeUnavailable
	}
	if err := validateMembershipInput(member); err != nil {
		return nil, err
	}
	key := member.Key
	now := i.now()
	i.mu.Lock()
	if entry, ok := i.entries[key]; ok {
		if !entry.expiresAt.After(now) {
			if !i.preparing[key] {
				delete(i.entries, key)
				i.bytes -= entry.bytes
			}
			i.mu.Unlock()
			return nil, ErrPreparedExpired
		}
		if entry.prepared.Key != member.Key ||
			!samePreparedGenerationIdentity(
				entry.prepared.Generation,
				member.Generation,
			) {
			i.mu.Unlock()
			return nil, ErrGenerationConflict
		}
		if entry.committing || i.preparing[key] {
			i.mu.Unlock()
			return nil, ErrPreparedBusy
		}
		if entry.prepared.Generation.LeaseUntilMs <
			member.Generation.LeaseUntilMs {
			i.preparing[key] = true
			i.mu.Unlock()
			return i.refreshPreparedMembership(ctx, member, entry)
		}
		prepared := entry.prepared
		i.mu.Unlock()
		return &prepared, nil
	}
	i.pruneLocked(now)
	if i.preparing[key] {
		i.mu.Unlock()
		return nil, ErrPreparedBusy
	}
	if len(i.entries)+len(i.preparing) >= i.maxEntries {
		i.mu.Unlock()
		return nil, ErrPreparedOverflow
	}
	i.preparing[key] = true
	i.mu.Unlock()

	prepared, err := i.inner.PrepareMembership(ctx, member)
	now = i.now()
	i.mu.Lock()
	delete(i.preparing, key)
	if err != nil {
		i.mu.Unlock()
		return nil, err
	}
	if prepared == nil || !preparedGenerationEqualPrepared(*prepared, member) {
		i.mu.Unlock()
		return nil, ErrPreparedIdentity
	}
	size := preparedMembershipSize(*prepared)
	if size > i.maxBytes || i.bytes+size > i.maxBytes {
		i.mu.Unlock()
		return nil, ErrPreparedOverflow
	}
	expiresAt := now.Add(i.retention)
	if !expiresAt.After(now) {
		i.mu.Unlock()
		return nil, ErrPreparedExpired
	}
	if existing, exists := i.entries[key]; exists {
		i.mu.Unlock()
		if preparedGenerationEqualPrepared(existing.prepared, member) {
			return nil, ErrPreparedBusy
		}
		return nil, ErrGenerationConflict
	}
	i.entries[key] = preparedMembershipEntry{
		prepared:  *prepared,
		bytes:     size,
		expiresAt: expiresAt,
	}
	i.bytes += size
	i.mu.Unlock()
	copyPrepared := *prepared
	return &copyPrepared, nil
}

func (i *PrepareOnlyMembershipInstaller) refreshPreparedMembership(
	ctx context.Context,
	member authoritycomposition.ShardMembership,
	previous preparedMembershipEntry,
) (*authoritycomposition.PreparedMembership, error) {
	prepared, err := i.inner.PrepareMembership(ctx, member)
	now := i.now()
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.preparing, member.Key)
	current, ok := i.entries[member.Key]
	if !ok || current.committing ||
		!reflect.DeepEqual(current, previous) {
		return nil, ErrPreparedBusy
	}
	if err != nil {
		return nil, err
	}
	if prepared == nil ||
		!preparedGenerationEqualPrepared(*prepared, member) {
		return nil, ErrPreparedIdentity
	}
	size := preparedMembershipSize(*prepared)
	nextBytes := i.bytes - current.bytes + size
	if size > i.maxBytes || nextBytes > i.maxBytes {
		return nil, ErrPreparedOverflow
	}
	expiresAt := now.Add(i.retention)
	if !expiresAt.After(now) {
		return nil, ErrPreparedExpired
	}
	i.entries[member.Key] = preparedMembershipEntry{
		prepared:  *prepared,
		bytes:     size,
		expiresAt: expiresAt,
	}
	i.bytes = nextBytes
	copyPrepared := *prepared
	return &copyPrepared, nil
}

// PrepareShardGeneration is the RPC-facing Prepare-only operation. It has no
// commit path and accepts only the route generation tuple from the caller.
func (i *PrepareOnlyMembershipInstaller) PrepareShardGeneration(ctx context.Context, key authoritycomposition.ShardKey, generation AuthorityGeneration) error {
	member := authoritycomposition.ShardMembership{
		Key: key,
		Generation: authoritycomposition.MembershipGeneration{
			AssignmentRevision: generation.AssignmentRevision,
			WriterEpoch:        generation.WriterEpoch,
			LeaseEpoch:         generation.LeaseEpoch,
			HolderID:           generation.HolderID,
			HolderBootID:       generation.HolderBootID,
			LeaseUntilMs:       generation.LeaseUntilMs,
		},
	}
	_, err := i.PrepareMembership(ctx, member)
	return err
}

func (i *PrepareOnlyMembershipInstaller) CommitPreparedMembership(ctx context.Context, prepared authoritycomposition.PreparedMembership) error {
	if i == nil || i.inner == nil {
		return ErrRuntimeUnavailable
	}
	if err := validatePreparedInput(prepared); err != nil {
		return err
	}
	now := i.now()
	key := prepared.Key
	i.mu.Lock()
	entry, ok := i.entries[key]
	if !ok {
		i.pruneLocked(now)
		i.mu.Unlock()
		return ErrPreparedNotFound
	}
	if i.preparing[key] {
		i.mu.Unlock()
		return ErrPreparedBusy
	}
	if !entry.expiresAt.After(now) {
		delete(i.entries, key)
		i.bytes -= entry.bytes
		i.mu.Unlock()
		return ErrPreparedExpired
	}
	if !reflect.DeepEqual(entry.prepared, prepared) {
		i.mu.Unlock()
		return ErrPreparedIdentity
	}
	if entry.committing {
		i.mu.Unlock()
		return ErrPreparedBusy
	}
	entry.committing = true
	i.entries[key] = entry
	i.mu.Unlock()

	err := i.inner.CommitPreparedMembership(ctx, prepared)
	i.mu.Lock()
	defer i.mu.Unlock()
	current, stillPresent := i.entries[key]
	if !stillPresent || !reflect.DeepEqual(current.prepared, prepared) {
		return ErrPreparedIdentity
	}
	if !current.expiresAt.After(i.now()) {
		delete(i.entries, key)
		i.bytes -= current.bytes
		return ErrPreparedExpired
	}
	if err != nil {
		current.committing = false
		i.entries[key] = current
		return err
	}
	delete(i.entries, key)
	i.bytes -= current.bytes
	return nil
}

func (i *PrepareOnlyMembershipInstaller) PreparedShardGeneration(ctx context.Context, key authoritycomposition.ShardKey) (PreparedGenerationStatus, error) {
	if i == nil || i.inner == nil {
		return PreparedGenerationStatus{}, ErrRuntimeUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PreparedGenerationStatus{}, err
	}
	if key.Domain != actorrouting.DomainPlayer && key.Domain != actorrouting.DomainFarm {
		return PreparedGenerationStatus{}, fmt.Errorf("%w: membership domain", ErrInvalidRequest)
	}
	now := i.now()
	i.mu.Lock()
	defer i.mu.Unlock()
	entry, ok := i.entries[key]
	if !ok {
		i.pruneLocked(now)
		return PreparedGenerationStatus{}, nil
	}
	if !entry.expiresAt.After(now) {
		if !entry.committing && !i.preparing[key] {
			delete(i.entries, key)
			i.bytes -= entry.bytes
		}
		return PreparedGenerationStatus{}, nil
	}
	generation := authorityGenerationFromMembership(entry.prepared.Generation)
	return PreparedGenerationStatus{
		Prepared:   true,
		Generation: &generation,
		Committing: entry.committing,
	}, nil
}

func (i *PrepareOnlyMembershipInstaller) PreparedCount() int {
	if i == nil {
		return 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.pruneLocked(i.now())
	return len(i.entries)
}

func (i *PrepareOnlyMembershipInstaller) PreparedBytes() int {
	if i == nil {
		return 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.pruneLocked(i.now())
	return i.bytes
}

func (i *PrepareOnlyMembershipInstaller) pruneLocked(now time.Time) {
	for key, entry := range i.entries {
		if !entry.expiresAt.After(now) &&
			!entry.committing &&
			!i.preparing[key] {
			delete(i.entries, key)
			i.bytes -= entry.bytes
		}
	}
	if i.bytes < 0 {
		i.bytes = 0
	}
}

func preparedGenerationEqualPrepared(prepared authoritycomposition.PreparedMembership, member authoritycomposition.ShardMembership) bool {
	return prepared.Key == member.Key &&
		samePreparedGenerationIdentity(prepared.Generation, member.Generation) &&
		prepared.Generation.LeaseUntilMs >= member.Generation.LeaseUntilMs &&
		prepared.Authority.State == authoritystore.ShardAuthorityActive &&
		prepared.Authority.Domain == member.Key.Domain &&
		prepared.Authority.VirtualShard == member.Key.Shard &&
		prepared.Authority.HolderID == prepared.Generation.HolderID &&
		prepared.Authority.HolderBootID == prepared.Generation.HolderBootID &&
		prepared.Authority.AssignmentRevision == prepared.Generation.AssignmentRevision &&
		prepared.Authority.WriterEpoch == prepared.Generation.WriterEpoch &&
		prepared.Authority.LeaseEpoch == prepared.Generation.LeaseEpoch &&
		prepared.Authority.LeaseUntilMs == prepared.Generation.LeaseUntilMs &&
		prepared.AcceptedTTLms > 0
}

func preparedGenerationEqual(prepared authoritycomposition.PreparedMembership, member authoritycomposition.ShardMembership) bool {
	return prepared.Key == member.Key &&
		samePreparedGenerationIdentity(prepared.Generation, member.Generation) &&
		prepared.Generation.LeaseUntilMs >= member.Generation.LeaseUntilMs
}

func samePreparedGenerationIdentity(left, right authoritycomposition.MembershipGeneration) bool {
	return left.AssignmentRevision == right.AssignmentRevision &&
		left.WriterEpoch == right.WriterEpoch &&
		left.LeaseEpoch == right.LeaseEpoch &&
		left.HolderID == right.HolderID &&
		left.HolderBootID == right.HolderBootID
}

func authorityGenerationFromMembership(generation authoritycomposition.MembershipGeneration) AuthorityGeneration {
	return AuthorityGeneration{
		HolderID:           generation.HolderID,
		HolderBootID:       generation.HolderBootID,
		AssignmentRevision: generation.AssignmentRevision,
		WriterEpoch:        generation.WriterEpoch,
		LeaseEpoch:         generation.LeaseEpoch,
		LeaseUntilMs:       generation.LeaseUntilMs,
	}
}

func preparedMembershipSize(prepared authoritycomposition.PreparedMembership) int {
	return preparedMembershipFixedBytes +
		len(prepared.Key.Domain) +
		len(prepared.Generation.HolderID) +
		len(prepared.Generation.HolderBootID) +
		len(prepared.Authority.AuthorityLayoutVersion) +
		len(prepared.Authority.LastTransitionID)
}

func validateMembershipInput(member authoritycomposition.ShardMembership) error {
	if member.Key.Domain != actorrouting.DomainPlayer && member.Key.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: membership domain", ErrInvalidRequest)
	}
	if member.Generation.AssignmentRevision == 0 || member.Generation.WriterEpoch == 0 ||
		member.Generation.LeaseEpoch == 0 || member.Generation.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: incomplete membership generation", ErrInvalidRequest)
	}
	if err := validateBoundString("holder_id", member.Generation.HolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	return validateBoundString("holder_boot_id", member.Generation.HolderBootID, MaxBootIDBytes)
}

func validatePreparedInput(prepared authoritycomposition.PreparedMembership) error {
	if err := validateMembershipInput(authoritycomposition.ShardMembership{Key: prepared.Key, Generation: prepared.Generation}); err != nil {
		return err
	}
	if prepared.AcceptedTTLms <= 0 {
		return fmt.Errorf("%w: accepted lease ttl", ErrInvalidRequest)
	}
	return nil
}
