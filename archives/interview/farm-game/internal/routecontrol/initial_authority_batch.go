package routecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
)

var (
	ErrInitialAuthorityBatchInvalid       = errors.New("routecontrol: invalid initial authority batch")
	ErrInitialAuthorityBatchOpen          = errors.New("routecontrol: initial authority batch is open")
	ErrInitialAuthorityBatchNotOpen       = errors.New("routecontrol: initial authority batch is not open")
	ErrInitialAuthorityBatchCommitted     = errors.New("routecontrol: initial authority batch is already committed")
	ErrInitialAuthorityBatchTokenRequired = errors.New("routecontrol: issued control token required for initial authority batch")
	ErrInitialAuthorityBatchUnsupported   = errors.New("routecontrol: initial authority batch store capability is unavailable")
)

// InitialAuthorityBatchRecord is the exact authority evidence paired with one
// initial PLAYER/FARM route. It is intentionally independent of the frozen
// authority-store interface so the sealed batch capability can be consumed by
// the route snapshot adapters without creating a package cycle.
type InitialAuthorityBatchRecord struct {
	Realm                  string
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	HolderID               string
	HolderBootID           string
	Endpoint               string
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	AssignmentRevision     uint64
	WriterEpoch            uint64
	LeaseEpoch             uint64
	LeaseUntilMs           int64
	LastTransitionID       string
	AcceptedTTLms          int64
}

// InitialAuthorityTransitionID is the deterministic Claim identity used by
// the initial batch. Binding holder boot and endpoint into the ID lets a crash
// resume reject an Authority record when the candidate endpoint changed even
// though the frozen Authority tuple has no endpoint field of its own. The
// realm is hashed as well so the ID remains below the 256-byte durable key
// limit even when a deployment uses a long realm label.
func InitialAuthorityTransitionID(realm string, domain actorrouting.RouteDomain, shard uint32, holderID, holderBootID, endpoint string) string {
	realmHash := sha256.Sum256([]byte(realm))
	base := fmt.Sprintf("route-controller/initial-assignment/realm-%x/%s/%d", realmHash[:], domain, shard)
	hash := sha256.New()
	for _, value := range []string{holderID, holderBootID, endpoint} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return base + "/target/" + hex.EncodeToString(hash.Sum(nil))
}

// InitialAuthorityBatchCapability is sealed. Only an
// InitialAuthorityBatchPublisher can issue one; callers cannot manufacture a
// value that a durable store will accept by implementing this interface.
type InitialAuthorityBatchCapability interface {
	initialAuthorityBatchCapability()
}

type initialAuthorityBatchCapability struct {
	owner    *InitialAuthorityBatchPublisher
	expected StoredRouteSnapshot
	next     actorrouting.RouteSnapshot
	records  []InitialAuthorityBatchRecord
	nowMs    int64
}

func (*initialAuthorityBatchCapability) initialAuthorityBatchCapability() {}

// InitialAuthorityBatchValues exposes a read-only projection for the durable
// store adapters. The capability remains sealed because the concrete value and
// its owner are private to routecontrol.
func InitialAuthorityBatchValues(capability InitialAuthorityBatchCapability) (StoredRouteSnapshot, actorrouting.RouteSnapshot, []InitialAuthorityBatchRecord, time.Time, error) {
	sealed, ok := capability.(*initialAuthorityBatchCapability)
	if !ok || sealed == nil || sealed.owner == nil {
		return StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, nil, time.Time{}, ErrInitialAuthorityBatchInvalid
	}
	records := append([]InitialAuthorityBatchRecord(nil), sealed.records...)
	return sealed.expected.Clone(), actorrouting.CloneRouteSnapshot(sealed.next), records, time.UnixMilli(sealed.nowMs), nil
}

// ValidateInitialAuthorityBatch is the public, fail-closed validator used by
// the publisher and by both durable store implementations. It permits only a
// complete fresh initial assignment; ordinary route changes must use the
// existing CAS/authority publication validators.
func ValidateInitialAuthorityBatch(expected StoredRouteSnapshot, next actorrouting.RouteSnapshot, records []InitialAuthorityBatchRecord) error {
	return ValidateInitialAuthorityBatchAt(expected, next, records, "", time.Now())
}

// ValidateInitialAuthorityBatchAt is the deterministic form used by tests and
// by the sealed capability. The expected durable envelope is checked again by
// the store's atomic operation, so this validation is not a substitute for
// Redis CAS.
func ValidateInitialAuthorityBatchAt(expected StoredRouteSnapshot, next actorrouting.RouteSnapshot, records []InitialAuthorityBatchRecord, layout string, now time.Time) error {
	if err := validateStoredRouteSnapshot(expected); err != nil {
		return fmt.Errorf("%w: expected envelope: %v", ErrInitialAuthorityBatchInvalid, err)
	}
	candidate, err := CanonicalizeRouteSnapshot(next)
	if err != nil {
		return fmt.Errorf("%w: next snapshot: %v", ErrInitialAuthorityBatchInvalid, err)
	}
	next = candidate.Snapshot
	previous := expected.Snapshot
	if previous.Realm != next.Realm || previous.HashVersion != next.HashVersion || previous.HashSeed != next.HashSeed ||
		previous.Player.ShardCount != next.Player.ShardCount || previous.Farm.ShardCount != next.Farm.ShardCount ||
		!reflect.DeepEqual(previous.FarmWorker, next.FarmWorker) {
		return fmt.Errorf("%w: topology or FARM_WORKER changed", ErrInitialAuthorityBatchInvalid)
	}
	if next.GlobalRevision <= previous.GlobalRevision {
		return fmt.Errorf("%w: final global revision did not advance", ErrInitialAuthorityBatchInvalid)
	}
	total := uint64(previous.Player.ShardCount) + uint64(previous.Farm.ShardCount)
	if total == 0 || previous.GlobalRevision > ^uint64(0)-total || next.GlobalRevision != previous.GlobalRevision+total {
		return fmt.Errorf("%w: global revision=%d want=%d", ErrInitialAuthorityBatchInvalid, next.GlobalRevision, previous.GlobalRevision+total)
	}
	if err := validateInitialPlaceholderDomains(previous); err != nil {
		return err
	}
	if err := validateInitialActiveDomains(next); err != nil {
		return err
	}
	if len(records) != int(total) {
		return fmt.Errorf("%w: records=%d want=%d", ErrInitialAuthorityBatchInvalid, len(records), total)
	}

	type batchKey struct {
		domain actorrouting.RouteDomain
		shard  uint32
	}
	recordByKey := make(map[batchKey]InitialAuthorityBatchRecord, len(records))
	transitionIDs := make(map[string]struct{}, len(records))
	batchLayout := ""
	targetHolders := make(map[actorrouting.HolderRef]struct{}, len(records))
	for _, record := range records {
		if record.Domain != actorrouting.DomainPlayer && record.Domain != actorrouting.DomainFarm {
			return fmt.Errorf("%w: unsupported record domain %s", ErrInitialAuthorityBatchInvalid, record.Domain)
		}
		if record.Realm != previous.Realm {
			return fmt.Errorf("%w: record realm drift", ErrInitialAuthorityBatchInvalid)
		}
		if layout != "" && record.AuthorityLayoutVersion != layout {
			return fmt.Errorf("%w: record layout drift", ErrInitialAuthorityBatchInvalid)
		}
		if batchLayout == "" {
			batchLayout = record.AuthorityLayoutVersion
		} else if batchLayout != record.AuthorityLayoutVersion {
			return fmt.Errorf("%w: authority layout differs across records", ErrInitialAuthorityBatchInvalid)
		}
		if record.AuthorityLayoutVersion == "" || record.HashVersion != previous.HashVersion || record.HashSeed != previous.HashSeed ||
			record.HolderID == "" || record.HolderBootID == "" || record.Endpoint == "" || record.LastTransitionID == "" ||
			record.AssignmentRevision == 0 || record.WriterEpoch == 0 || record.LeaseEpoch == 0 || record.LeaseUntilMs <= 0 ||
			record.AcceptedTTLms <= 0 || record.LeaseUntilMs <= now.UnixMilli() {
			return fmt.Errorf("%w: incomplete or stale authority %s/%d", ErrInitialAuthorityBatchInvalid, record.Domain, record.VirtualShard)
		}
		if record.LastTransitionID != InitialAuthorityTransitionID(record.Realm, record.Domain, record.VirtualShard, record.HolderID, record.HolderBootID, record.Endpoint) {
			return fmt.Errorf("%w: transition identity mismatch %s/%d", ErrInitialAuthorityBatchInvalid, record.Domain, record.VirtualShard)
		}
		key := batchKey{domain: record.Domain, shard: record.VirtualShard}
		if _, exists := recordByKey[key]; exists {
			return fmt.Errorf("%w: duplicate authority %s/%d", ErrInitialAuthorityBatchInvalid, record.Domain, record.VirtualShard)
		}
		if _, exists := transitionIDs[record.LastTransitionID]; exists {
			return fmt.Errorf("%w: duplicate transition id", ErrInitialAuthorityBatchInvalid)
		}
		transitionIDs[record.LastTransitionID] = struct{}{}
		recordByKey[key] = record
		targetHolders[actorrouting.HolderRef{InstanceID: record.HolderID, BootID: record.HolderBootID, Endpoint: record.Endpoint, State: actorrouting.HolderStateActive, Weight: 1}] = struct{}{}
	}

	if len(next.Holders) < len(previous.Holders) {
		return fmt.Errorf("%w: holder topology shrank", ErrInitialAuthorityBatchInvalid)
	}
	isTargetHolder := func(holder actorrouting.HolderRef) bool {
		if holder.State != actorrouting.HolderStateActive || holder.Weight == 0 {
			return false
		}
		for target := range targetHolders {
			if target.InstanceID == holder.InstanceID && target.BootID == holder.BootID && target.Endpoint == holder.Endpoint {
				return true
			}
		}
		return false
	}
	for index, previousHolder := range previous.Holders {
		currentHolder := next.Holders[index]
		if currentHolder == previousHolder || isTargetHolder(currentHolder) {
			continue
		}
		return fmt.Errorf("%w: holder order/identity drift at index %d", ErrInitialAuthorityBatchInvalid, index)
	}
	for _, holder := range next.Holders[len(previous.Holders):] {
		if !isTargetHolder(holder) {
			return fmt.Errorf("%w: unpinned holder %s/%s", ErrInitialAuthorityBatchInvalid, holder.InstanceID, holder.BootID)
		}
	}

	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		var previousRoutes, nextRoutes actorrouting.DomainSnapshot
		switch domain {
		case actorrouting.DomainPlayer:
			previousRoutes, nextRoutes = previous.Player, next.Player
		case actorrouting.DomainFarm:
			previousRoutes, nextRoutes = previous.Farm, next.Farm
		}
		for shard := uint32(0); shard < previousRoutes.ShardCount; shard++ {
			key := batchKey{domain: domain, shard: shard}
			record, found := recordByKey[key]
			if !found {
				return fmt.Errorf("%w: missing authority %s/%d", ErrInitialAuthorityBatchInvalid, domain, shard)
			}
			before := previousRoutes.Routes[shard]
			after := nextRoutes.Routes[shard]
			if before.ShardRevision != 1 || before.ExpectedWriterEpoch != 1 || before.ExpectedLeaseEpoch != 1 || before.LeaseUntilMs != 0 ||
				after.State != actorrouting.ShardStateActive || after.ShardRevision != before.ShardRevision ||
				after.ExpectedWriterEpoch != before.ExpectedWriterEpoch || after.ExpectedLeaseEpoch != before.ExpectedLeaseEpoch ||
				after.ShardRevision != record.AssignmentRevision || after.ExpectedWriterEpoch != record.WriterEpoch ||
				after.ExpectedLeaseEpoch != record.LeaseEpoch || after.LeaseUntilMs != record.LeaseUntilMs {
				return fmt.Errorf("%w: generation mismatch %s/%d", ErrInitialAuthorityBatchInvalid, domain, shard)
			}
			if after.HolderIndex < 0 || after.HolderIndex >= len(next.Holders) {
				return fmt.Errorf("%w: target holder index %s/%d", ErrInitialAuthorityBatchInvalid, domain, shard)
			}
			holder := next.Holders[after.HolderIndex]
			if holder.InstanceID != record.HolderID || holder.BootID != record.HolderBootID || holder.Endpoint != record.Endpoint || holder.State != actorrouting.HolderStateActive || holder.Weight == 0 {
				return fmt.Errorf("%w: target identity mismatch %s/%d", ErrInitialAuthorityBatchInvalid, domain, shard)
			}
		}
	}
	return nil
}

func validateInitialPlaceholderDomains(snapshot actorrouting.RouteSnapshot) error {
	for _, routes := range []actorrouting.DomainSnapshot{snapshot.Player, snapshot.Farm} {
		if routes.ShardCount == 0 || uint32(len(routes.Routes)) != routes.ShardCount {
			return fmt.Errorf("%w: incomplete initial route table", ErrInitialAuthorityBatchInvalid)
		}
		for _, route := range routes.Routes {
			if route.State != actorrouting.ShardStateAssigning || route.ShardRevision != 1 || route.ExpectedWriterEpoch != 1 || route.ExpectedLeaseEpoch != 1 || route.LeaseUntilMs != 0 {
				return fmt.Errorf("%w: non-placeholder initial route", ErrInitialAuthorityBatchInvalid)
			}
		}
	}
	return nil
}

func validateInitialActiveDomains(snapshot actorrouting.RouteSnapshot) error {
	for _, routes := range []actorrouting.DomainSnapshot{snapshot.Player, snapshot.Farm} {
		if routes.ShardCount == 0 || uint32(len(routes.Routes)) != routes.ShardCount {
			return fmt.Errorf("%w: incomplete final route table", ErrInitialAuthorityBatchInvalid)
		}
		for _, route := range routes.Routes {
			if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
				return fmt.Errorf("%w: partial or stale final activation", ErrInitialAuthorityBatchInvalid)
			}
		}
	}
	return nil
}

// InitialAuthorityBatchStore is the sealed-store capability. Ordinary
// RouteSnapshotStore implementations do not gain a jump-revision write path
// by implementing CompareAndSwap; only this capability can request it.
type InitialAuthorityBatchStore interface {
	CompareAndSwapInitialAuthorityBatch(context.Context, NestedControlOperationToken, InitialAuthorityBatchCapability) (SnapshotCASResult, error)
}

// InitialAuthorityBatchPublisher is the manager-owned switchable publisher.
// In its normal state it delegates exactly to DurableSnapshotPublisher. During
// one explicitly opened initial batch it accepts complete manager snapshots in
// memory and performs no durable CAS or Watch notification.
type InitialAuthorityBatchPublisher struct {
	durable *DurableSnapshotPublisher
	now     func() time.Time

	mu         sync.Mutex
	started    bool
	committing bool
	mode       initialAuthorityBatchMode
	expected   StoredRouteSnapshot
	buffered   actorrouting.RouteSnapshot
	sealed     *initialAuthorityBatchCapability
	wave       *authorityWaveBatchState
}

type initialAuthorityBatchMode uint8

const (
	initialAuthorityBatchDurable initialAuthorityBatchMode = iota
	initialAuthorityBatchBuffering
	initialAuthorityBatchSealed
)

func NewInitialAuthorityBatchPublisher(durable *DurableSnapshotPublisher, clocks ...func() time.Time) (*InitialAuthorityBatchPublisher, error) {
	if durable == nil {
		return nil, fmt.Errorf("%w: durable publisher required", ErrInitialAuthorityBatchInvalid)
	}
	if len(clocks) > 1 {
		return nil, fmt.Errorf("%w: one clock allowed", ErrInitialAuthorityBatchInvalid)
	}
	now := time.Now
	if len(clocks) == 1 && clocks[0] != nil {
		now = clocks[0]
	}
	return &InitialAuthorityBatchPublisher{durable: durable, now: now}, nil
}

// BeginInitialAuthorityBatch verifies that the manager still starts from the
// exact durable snapshot, then irreversibly arms the in-memory complete-
// snapshot buffer for this process startup.
func (p *InitialAuthorityBatchPublisher) BeginInitialAuthorityBatch(ctx context.Context, initial actorrouting.RouteSnapshot) error {
	if p == nil || p.durable == nil || ctx == nil {
		return ErrInitialAuthorityBatchInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.started || p.mode != initialAuthorityBatchDurable {
		p.mu.Unlock()
		return fmt.Errorf("%w: batch already started or sealed", ErrInitialAuthorityBatchInvalid)
	}
	p.mu.Unlock()
	current, found, err := p.durable.store.LoadCurrent(ctx)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(current.Snapshot, initial) {
		return fmt.Errorf("%w: manager/durable initial snapshot mismatch", ErrInitialAuthorityBatchInvalid)
	}
	if err := validateStoredRouteSnapshot(current); err != nil {
		return fmt.Errorf("%w: durable initial envelope: %v", ErrInitialAuthorityBatchInvalid, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started || p.mode != initialAuthorityBatchDurable {
		return fmt.Errorf("%w: batch changed while opening", ErrInitialAuthorityBatchInvalid)
	}
	p.started = true
	p.mode = initialAuthorityBatchBuffering
	p.expected = current.Clone()
	p.buffered = actorrouting.CloneRouteSnapshot(current.Snapshot)
	return nil
}

func (p *InitialAuthorityBatchPublisher) Publish(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if p == nil {
		return ErrInitialAuthorityBatchInvalid
	}
	if ctx == nil {
		return ErrSnapshotContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.durable.checkToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	mode := p.mode
	if mode == initialAuthorityBatchBuffering {
		p.mu.Unlock()
		return fmt.Errorf("%w: ordinary snapshot publication while initial batch is open", ErrInitialAuthorityBatchOpen)
	}
	if wave := p.wave; wave != nil {
		err := bufferAuthorityWaveSnapshot(wave, snapshot, false)
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()
	return p.durable.Publish(ctx, token, snapshot)
}

func (p *InitialAuthorityBatchPublisher) PublishAuthoritySnapshot(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if p == nil {
		return ErrInitialAuthorityBatchInvalid
	}
	if ctx == nil {
		return ErrSnapshotContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.durable.checkToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	if p.mode == initialAuthorityBatchBuffering {
		if p.committing || p.sealed != nil {
			p.mu.Unlock()
			return ErrInitialAuthorityBatchOpen
		}
		err := actorrouting.ValidateAuthoritySnapshotUpdate(p.buffered, snapshot)
		if err == nil {
			p.buffered = actorrouting.CloneRouteSnapshot(snapshot)
		}
		p.mu.Unlock()
		return err
	}
	if wave := p.wave; wave != nil {
		err := bufferAuthorityWaveSnapshot(wave, snapshot, true)
		p.mu.Unlock()
		return err
	}
	mode := p.mode
	p.mu.Unlock()
	if mode != initialAuthorityBatchDurable && mode != initialAuthorityBatchSealed {
		return ErrInitialAuthorityBatchInvalid
	}
	return p.durable.PublishAuthoritySnapshot(ctx, token, snapshot)
}

// PublishAuthorityWaveSnapshot is refused while the startup buffer or a bounded
// OwnerWave batch is open: those paths still ingest one route change per call.
func (p *InitialAuthorityBatchPublisher) PublishAuthorityWaveSnapshot(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if p == nil {
		return ErrInitialAuthorityBatchInvalid
	}
	if ctx == nil {
		return ErrSnapshotContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.durable.checkToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	if p.mode == initialAuthorityBatchBuffering || p.wave != nil {
		p.mu.Unlock()
		return fmt.Errorf("%w: coalesced authority snapshot while a batch buffer is open", ErrSnapshotCASConflict)
	}
	mode := p.mode
	p.mu.Unlock()
	if mode != initialAuthorityBatchDurable && mode != initialAuthorityBatchSealed {
		return ErrInitialAuthorityBatchInvalid
	}
	return p.durable.PublishAuthorityWaveSnapshot(ctx, token, snapshot)
}

// SealInitialAuthorityBatch creates the unforgeable final capability after
// the caller has collected every exact authority record. It does not write.
func (p *InitialAuthorityBatchPublisher) SealInitialAuthorityBatch(records []InitialAuthorityBatchRecord) (InitialAuthorityBatchCapability, error) {
	if p == nil {
		return nil, ErrInitialAuthorityBatchInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == initialAuthorityBatchSealed {
		return nil, ErrInitialAuthorityBatchCommitted
	}
	if p.mode != initialAuthorityBatchBuffering || p.committing || p.sealed != nil {
		return nil, ErrInitialAuthorityBatchNotOpen
	}
	now := p.now()
	if now.IsZero() {
		now = time.Now()
	}
	copyRecords := append([]InitialAuthorityBatchRecord(nil), records...)
	if err := ValidateInitialAuthorityBatchAt(p.expected, p.buffered, copyRecords, "", now); err != nil {
		return nil, err
	}
	capability := &initialAuthorityBatchCapability{owner: p, expected: p.expected.Clone(), next: actorrouting.CloneRouteSnapshot(p.buffered), records: copyRecords, nowMs: now.UnixMilli()}
	p.sealed = capability
	return capability, nil
}

// CommitInitialAuthorityBatch performs exactly one atomic durable batch CAS and
// only then sends the manager-only Watch notification. Success seals this
// publisher permanently onto the durable adapters.
func (p *InitialAuthorityBatchPublisher) CommitInitialAuthorityBatch(ctx context.Context, token NestedControlOperationToken, capability InitialAuthorityBatchCapability) (SnapshotCASResult, error) {
	if p == nil || ctx == nil {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchInvalid
	}
	if !token.Issued() {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchTokenRequired
	}
	sealed, ok := capability.(*initialAuthorityBatchCapability)
	if !ok || sealed == nil || sealed.owner != p {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchInvalid
	}
	p.mu.Lock()
	if p.mode == initialAuthorityBatchSealed {
		p.mu.Unlock()
		return SnapshotCASResult{}, ErrInitialAuthorityBatchCommitted
	}
	if p.mode != initialAuthorityBatchBuffering || p.committing || p.sealed != sealed {
		p.mu.Unlock()
		return SnapshotCASResult{}, ErrInitialAuthorityBatchNotOpen
	}
	p.committing = true
	p.mu.Unlock()
	result, err := p.durable.commitInitialAuthorityBatch(ctx, token, sealed)
	p.mu.Lock()
	p.committing = false
	if err == nil || initialAuthorityBatchCommitWasDurable(err) {
		p.mode = initialAuthorityBatchSealed
	}
	p.mu.Unlock()
	return result, err
}

func initialAuthorityBatchCommitWasDurable(err error) bool {
	var publishErr *SnapshotPublishError
	return errors.As(err, &publishErr) && publishErr.SnapshotCommitted()
}

// InitialAuthorityBatchSealed reports whether this startup publisher has
// irreversibly returned to its normal durable path.
func (p *InitialAuthorityBatchPublisher) InitialAuthorityBatchSealed() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode == initialAuthorityBatchSealed
}
