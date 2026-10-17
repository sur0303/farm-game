package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"farmgame/internal/actorrouting"
)

const MaxAuthorityWaveBatchSize = 32

type AuthorityWaveBatchStage uint8

const (
	AuthorityWaveBatchStageInvalid AuthorityWaveBatchStage = iota
	AuthorityWaveBatchStageAssigning
	AuthorityWaveBatchStageActive
)

var (
	ErrAuthorityWaveBatchInvalid       = errors.New("routecontrol: invalid authority wave batch")
	ErrAuthorityWaveBatchOpen          = errors.New("routecontrol: authority wave batch is open")
	ErrAuthorityWaveBatchNotOpen       = errors.New("routecontrol: authority wave batch is not open")
	ErrAuthorityWaveBatchTokenRequired = errors.New("routecontrol: issued control token required for authority wave batch")
	ErrAuthorityWaveBatchUnsupported   = errors.New("routecontrol: authority wave batch store capability is unavailable")
)

// AuthorityWaveMember freezes the only shard keys that a ScalePlan wave may
// change while complete snapshots are buffered in process.
type AuthorityWaveMember struct {
	Domain       actorrouting.RouteDomain
	VirtualShard uint32
}

type AuthorityWaveBatchCapability interface {
	authorityWaveBatchCapability()
}

type AuthorityWaveBatchStep struct {
	Snapshot  actorrouting.RouteSnapshot
	Authority bool
}

type authorityWaveBatchCapability struct {
	owner    *InitialAuthorityBatchPublisher
	waveID   string
	expected StoredRouteSnapshot
	steps    []AuthorityWaveBatchStep
	members  []AuthorityWaveMember
	nowMs    int64
}

func (*authorityWaveBatchCapability) authorityWaveBatchCapability() {}

type authorityWaveBatchState struct {
	waveID     string
	expected   StoredRouteSnapshot
	buffered   actorrouting.RouteSnapshot
	members    []AuthorityWaveMember
	memberSet  map[AuthorityWaveMember]struct{}
	stepCount  map[AuthorityWaveMember]uint8
	activated  map[AuthorityWaveMember]struct{}
	steps      []AuthorityWaveBatchStep
	committing bool
	sealed     *authorityWaveBatchCapability
	stage      AuthorityWaveBatchStage
}

func authorityWaveInitialCounts(snapshot actorrouting.RouteSnapshot, members []AuthorityWaveMember) (AuthorityWaveBatchStage, map[AuthorityWaveMember]uint8, error) {
	counts := make(map[AuthorityWaveMember]uint8, len(members))
	stage := AuthorityWaveBatchStageInvalid
	for _, member := range members {
		var routes []actorrouting.ShardRoute
		switch member.Domain {
		case actorrouting.DomainPlayer:
			routes = snapshot.Player.Routes
		case actorrouting.DomainFarm:
			routes = snapshot.Farm.Routes
		default:
			return AuthorityWaveBatchStageInvalid, nil, fmt.Errorf("%w: unsupported domain %s", ErrAuthorityWaveBatchInvalid, member.Domain)
		}
		if int(member.VirtualShard) >= len(routes) {
			return AuthorityWaveBatchStageInvalid, nil, fmt.Errorf("%w: shard out of range", ErrAuthorityWaveBatchInvalid)
		}
		memberStage := AuthorityWaveBatchStageInvalid
		switch routes[member.VirtualShard].State {
		case actorrouting.ShardStateActive:
			memberStage = AuthorityWaveBatchStageAssigning
			counts[member] = 0
		case actorrouting.ShardStateAssigning:
			memberStage = AuthorityWaveBatchStageActive
			counts[member] = 1
		default:
			return AuthorityWaveBatchStageInvalid, nil, fmt.Errorf("%w: unsupported route state %s for %s/%d", ErrAuthorityWaveBatchInvalid, routes[member.VirtualShard].State, member.Domain, member.VirtualShard)
		}
		if stage == AuthorityWaveBatchStageInvalid {
			stage = memberStage
		} else if stage != memberStage {
			return AuthorityWaveBatchStageInvalid, nil, fmt.Errorf("%w: mixed wave stages", ErrAuthorityWaveBatchInvalid)
		}
	}
	return stage, counts, nil
}

func AuthorityWaveBatchValues(capability AuthorityWaveBatchCapability) (StoredRouteSnapshot, actorrouting.RouteSnapshot, []AuthorityWaveBatchStep, []AuthorityWaveMember, time.Time, error) {
	sealed, ok := capability.(*authorityWaveBatchCapability)
	if !ok || sealed == nil || sealed.owner == nil || sealed.waveID == "" || len(sealed.steps) == 0 {
		return StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, nil, nil, time.Time{}, ErrAuthorityWaveBatchInvalid
	}
	steps := cloneAuthorityWaveSteps(sealed.steps)
	members := append([]AuthorityWaveMember(nil), sealed.members...)
	return sealed.expected.Clone(), actorrouting.CloneRouteSnapshot(steps[len(steps)-1].Snapshot), steps, members, time.UnixMilli(sealed.nowMs), nil
}

func ValidateAuthorityWaveBatch(expected StoredRouteSnapshot, steps []AuthorityWaveBatchStep, members []AuthorityWaveMember) error {
	if err := validateStoredRouteSnapshot(expected); err != nil {
		return fmt.Errorf("%w: expected snapshot: %v", ErrAuthorityWaveBatchInvalid, err)
	}
	if len(members) == 0 || len(members) > MaxAuthorityWaveBatchSize || len(steps) != len(members) {
		return fmt.Errorf("%w: batch bounds", ErrAuthorityWaveBatchInvalid)
	}
	allowed, err := normalizeAuthorityWaveMembers(expected.Snapshot, members)
	if err != nil {
		return err
	}
	stage, counts, err := authorityWaveInitialCounts(expected.Snapshot, members)
	if err != nil {
		return err
	}
	previous := expected.Snapshot
	for index, step := range steps {
		if (stage == AuthorityWaveBatchStageAssigning && step.Authority) || (stage == AuthorityWaveBatchStageActive && !step.Authority) {
			return fmt.Errorf("%w: step %d crosses wave stage", ErrAuthorityWaveBatchInvalid, index)
		}
		var validateErr error
		if step.Authority {
			validateErr = actorrouting.ValidateAuthoritySnapshotUpdate(previous, step.Snapshot)
		} else {
			validateErr = actorrouting.ValidateRouteSnapshotUpdate(previous, step.Snapshot)
		}
		if validateErr != nil {
			return fmt.Errorf("%w: step %d: %v", ErrAuthorityWaveBatchInvalid, index, validateErr)
		}
		member, ok := authorityWaveChangedMember(previous, step.Snapshot)
		if !ok {
			return fmt.Errorf("%w: step %d did not change exactly one route", ErrAuthorityWaveBatchInvalid, index)
		}
		if _, ok := allowed[member]; !ok {
			return fmt.Errorf("%w: unowned shard %s/%d", ErrAuthorityWaveBatchInvalid, member.Domain, member.VirtualShard)
		}
		count := counts[member]
		if (!step.Authority && count != 0) || (step.Authority && count != 1) {
			return fmt.Errorf("%w: invalid two-stage order for %s/%d", ErrAuthorityWaveBatchInvalid, member.Domain, member.VirtualShard)
		}
		counts[member] = count + 1
		previous = step.Snapshot
	}
	return nil
}

func normalizeAuthorityWaveMembers(snapshot actorrouting.RouteSnapshot, members []AuthorityWaveMember) (map[AuthorityWaveMember]struct{}, error) {
	result := make(map[AuthorityWaveMember]struct{}, len(members))
	for _, member := range members {
		var count uint32
		switch member.Domain {
		case actorrouting.DomainPlayer:
			count = snapshot.Player.ShardCount
		case actorrouting.DomainFarm:
			count = snapshot.Farm.ShardCount
		default:
			return nil, fmt.Errorf("%w: unsupported domain %s", ErrAuthorityWaveBatchInvalid, member.Domain)
		}
		if member.VirtualShard >= count {
			return nil, fmt.Errorf("%w: shard out of range", ErrAuthorityWaveBatchInvalid)
		}
		if _, duplicate := result[member]; duplicate {
			return nil, fmt.Errorf("%w: duplicate member", ErrAuthorityWaveBatchInvalid)
		}
		result[member] = struct{}{}
	}
	return result, nil
}

func authorityWaveChangedMember(previous, next actorrouting.RouteSnapshot) (AuthorityWaveMember, bool) {
	var result AuthorityWaveMember
	changes := 0
	for _, table := range []struct {
		domain actorrouting.RouteDomain
		left   []actorrouting.ShardRoute
		right  []actorrouting.ShardRoute
	}{
		{actorrouting.DomainPlayer, previous.Player.Routes, next.Player.Routes},
		{actorrouting.DomainFarm, previous.Farm.Routes, next.Farm.Routes},
	} {
		if len(table.left) != len(table.right) {
			return AuthorityWaveMember{}, false
		}
		for index := range table.left {
			if !authorityWaveRouteEqual(previous, next, table.left[index], table.right[index]) {
				changes++
				result = AuthorityWaveMember{Domain: table.domain, VirtualShard: uint32(index)}
			}
		}
	}
	return result, changes == 1
}

// authorityWaveRouteEqual compares the logical route rather than its
// HolderIndex encoding. Canonicalization may insert or reorder HolderRefs and
// consequently renumber every unchanged route in the snapshot. Treating those
// index-only rewrites as Shard changes makes the first move to a newly
// discovered Holder look like a many-Shard wave and rejects an otherwise legal
// bounded batch.
func authorityWaveRouteEqual(previous, next actorrouting.RouteSnapshot, left, right actorrouting.ShardRoute) bool {
	if left.HolderIndex < 0 || left.HolderIndex >= len(previous.Holders) ||
		right.HolderIndex < 0 || right.HolderIndex >= len(next.Holders) {
		return false
	}
	leftHolder := previous.Holders[left.HolderIndex]
	rightHolder := next.Holders[right.HolderIndex]
	left.HolderIndex = 0
	right.HolderIndex = 0
	return reflect.DeepEqual(left, right) && reflect.DeepEqual(leftHolder, rightHolder)
}

func (p *InitialAuthorityBatchPublisher) BeginAuthorityWaveBatch(ctx context.Context, waveID string, initial actorrouting.RouteSnapshot, members []AuthorityWaveMember) error {
	if p == nil || p.durable == nil || ctx == nil || waveID == "" {
		return ErrAuthorityWaveBatchInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	copyMembers := append([]AuthorityWaveMember(nil), members...)
	sort.Slice(copyMembers, func(i, j int) bool {
		if copyMembers[i].Domain != copyMembers[j].Domain {
			return copyMembers[i].Domain < copyMembers[j].Domain
		}
		return copyMembers[i].VirtualShard < copyMembers[j].VirtualShard
	})
	if len(copyMembers) == 0 || len(copyMembers) > MaxAuthorityWaveBatchSize {
		return ErrAuthorityWaveBatchInvalid
	}
	p.mu.Lock()
	if p.mode == initialAuthorityBatchBuffering {
		p.mu.Unlock()
		return ErrAuthorityWaveBatchOpen
	}
	if p.wave != nil {
		if p.wave.waveID == waveID {
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		return ErrAuthorityWaveBatchOpen
	}
	p.mu.Unlock()
	current, found, err := p.durable.store.LoadCurrent(ctx)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(current.Snapshot, initial) {
		return fmt.Errorf("%w: manager/durable snapshot mismatch", ErrAuthorityWaveBatchInvalid)
	}
	memberSet, err := normalizeAuthorityWaveMembers(initial, copyMembers)
	if err != nil {
		return err
	}
	stage, counts, err := authorityWaveInitialCounts(initial, copyMembers)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave != nil || p.mode == initialAuthorityBatchBuffering {
		return ErrAuthorityWaveBatchOpen
	}
	p.wave = &authorityWaveBatchState{
		waveID: waveID, expected: current.Clone(), buffered: actorrouting.CloneRouteSnapshot(initial),
		members: copyMembers, memberSet: memberSet, stepCount: counts, activated: make(map[AuthorityWaveMember]struct{}, len(copyMembers)), stage: stage,
	}
	return nil
}

func (p *InitialAuthorityBatchPublisher) AuthorityWaveBatchStage(waveID string) (AuthorityWaveBatchStage, bool) {
	if p == nil {
		return AuthorityWaveBatchStageInvalid, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave == nil || p.wave.waveID != waveID {
		return AuthorityWaveBatchStageInvalid, false
	}
	return p.wave.stage, true
}

func (p *InitialAuthorityBatchPublisher) AuthorityWaveBatchMemberStage(member AuthorityWaveMember) (AuthorityWaveBatchStage, bool) {
	if p == nil {
		return AuthorityWaveBatchStageInvalid, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave == nil {
		return AuthorityWaveBatchStageInvalid, false
	}
	if _, ok := p.wave.memberSet[member]; !ok {
		return AuthorityWaveBatchStageInvalid, false
	}
	return p.wave.stage, true
}

func (p *InitialAuthorityBatchPublisher) AuthorityWaveBatchBaseRevision(waveID string) (uint64, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave == nil || p.wave.waveID != waveID {
		return 0, false
	}
	return p.wave.expected.Revision, true
}

func (p *InitialAuthorityBatchPublisher) AuthorityWaveBatchContains(waveID string, member AuthorityWaveMember) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave == nil || p.wave.waveID != waveID {
		return false
	}
	count, ok := p.wave.stepCount[member]
	if !ok {
		return false
	}
	switch p.wave.stage {
	case AuthorityWaveBatchStageAssigning:
		return count >= 1
	case AuthorityWaveBatchStageActive:
		return count >= 2
	default:
		return false
	}
}

func (p *InitialAuthorityBatchPublisher) SealAuthorityWaveBatch(waveID string) (AuthorityWaveBatchCapability, int, error) {
	if p == nil || waveID == "" {
		return nil, 0, ErrAuthorityWaveBatchInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	wave := p.wave
	if wave == nil || wave.waveID != waveID || wave.committing {
		return nil, 0, ErrAuthorityWaveBatchNotOpen
	}
	if wave.sealed != nil {
		return wave.sealed, len(wave.steps), nil
	}
	if len(wave.steps) == 0 {
		return nil, 0, ErrAuthorityWaveBatchNotOpen
	}
	if err := ValidateAuthorityWaveBatch(wave.expected, wave.steps, wave.members); err != nil {
		return nil, 0, err
	}
	now := p.now()
	if now.IsZero() {
		now = time.Now()
	}
	capability := &authorityWaveBatchCapability{owner: p, waveID: waveID, expected: wave.expected.Clone(), steps: cloneAuthorityWaveSteps(wave.steps), members: append([]AuthorityWaveMember(nil), wave.members...), nowMs: now.UnixMilli()}
	wave.sealed = capability
	return capability, len(wave.steps), nil
}

func (p *InitialAuthorityBatchPublisher) CommitAuthorityWaveBatch(ctx context.Context, token NestedControlOperationToken, capability AuthorityWaveBatchCapability) (SnapshotCASResult, error) {
	if p == nil || ctx == nil || !token.Issued() {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchTokenRequired
	}
	sealed, ok := capability.(*authorityWaveBatchCapability)
	if !ok || sealed == nil || sealed.owner != p {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchInvalid
	}
	p.mu.Lock()
	wave := p.wave
	if wave == nil || wave.waveID != sealed.waveID || wave.sealed != sealed || wave.committing {
		p.mu.Unlock()
		return SnapshotCASResult{}, ErrAuthorityWaveBatchNotOpen
	}
	wave.committing = true
	p.mu.Unlock()
	result, err := p.durable.commitAuthorityWaveBatch(ctx, token, sealed)
	p.mu.Lock()
	if p.wave == wave {
		wave.committing = false
		if err == nil || initialAuthorityBatchCommitWasDurable(err) {
			p.wave = nil
		}
	}
	p.mu.Unlock()
	return result, err
}

func (p *InitialAuthorityBatchPublisher) AbortEmptyAuthorityWaveBatch(waveID string) error {
	if p == nil || waveID == "" {
		return ErrAuthorityWaveBatchInvalid
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wave == nil || p.wave.waveID != waveID {
		return ErrAuthorityWaveBatchNotOpen
	}
	if len(p.wave.steps) != 0 || p.wave.committing || p.wave.sealed != nil {
		return ErrAuthorityWaveBatchOpen
	}
	p.wave = nil
	return nil
}

func cloneAuthorityWaveSteps(values []AuthorityWaveBatchStep) []AuthorityWaveBatchStep {
	result := make([]AuthorityWaveBatchStep, len(values))
	for index := range values {
		result[index] = AuthorityWaveBatchStep{Snapshot: actorrouting.CloneRouteSnapshot(values[index].Snapshot), Authority: values[index].Authority}
	}
	return result
}

func bufferAuthorityWaveSnapshot(wave *authorityWaveBatchState, snapshot actorrouting.RouteSnapshot, authority bool) error {
	if wave == nil {
		return fmt.Errorf("%w: missing wave", ErrAuthorityWaveBatchOpen)
	}
	if wave.committing {
		return fmt.Errorf("%w: commit is in progress", ErrAuthorityWaveBatchOpen)
	}
	if wave.sealed != nil {
		return fmt.Errorf("%w: wave is already sealed", ErrAuthorityWaveBatchOpen)
	}
	if len(wave.steps) >= len(wave.members)*2 {
		return fmt.Errorf("%w: step capacity exhausted steps=%d members=%d", ErrAuthorityWaveBatchOpen, len(wave.steps), len(wave.members))
	}
	member, changed := authorityWaveChangedMember(wave.buffered, snapshot)
	_, owned := wave.memberSet[member]
	count := wave.stepCount[member]
	var err error
	if (wave.stage == AuthorityWaveBatchStageAssigning && authority) || (wave.stage == AuthorityWaveBatchStageActive && !authority) {
		return fmt.Errorf("%w: publication kind crosses stage stage=%d authority=%t", ErrAuthorityWaveBatchInvalid, wave.stage, authority)
	}
	if authority {
		err = actorrouting.ValidateAuthoritySnapshotUpdate(wave.buffered, snapshot)
		if err == nil && count != 1 {
			err = fmt.Errorf("%w: authority step count=%d member=%s/%d", ErrAuthorityWaveBatchInvalid, count, member.Domain, member.VirtualShard)
		}
	} else {
		err = actorrouting.ValidateRouteSnapshotUpdate(wave.buffered, snapshot)
		if err == nil && count != 0 {
			err = fmt.Errorf("%w: assigning step count=%d member=%s/%d", ErrAuthorityWaveBatchInvalid, count, member.Domain, member.VirtualShard)
		}
	}
	if err == nil && (!changed || !owned) {
		err = fmt.Errorf("%w: changed=%t owned=%t member=%s/%d", ErrAuthorityWaveBatchInvalid, changed, owned, member.Domain, member.VirtualShard)
	}
	if err != nil {
		return fmt.Errorf("authority wave buffer stage=%d authority=%t steps=%d member=%s/%d: %w", wave.stage, authority, len(wave.steps), member.Domain, member.VirtualShard, err)
	}
	wave.buffered = actorrouting.CloneRouteSnapshot(snapshot)
	wave.steps = append(wave.steps, AuthorityWaveBatchStep{Snapshot: actorrouting.CloneRouteSnapshot(snapshot), Authority: authority})
	wave.stepCount[member] = count + 1
	if authority {
		wave.activated[member] = struct{}{}
	}
	return nil
}

// AuthorityWaveBatchStore is a sealed jump-revision capability. Ordinary CAS
// remains strict N->N+1.
type AuthorityWaveBatchStore interface {
	CompareAndSwapAuthorityWaveBatch(context.Context, NestedControlOperationToken, AuthorityWaveBatchCapability) (SnapshotCASResult, error)
}
