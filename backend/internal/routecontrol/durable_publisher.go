package routecontrol

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/actorrouting"
)

// SnapshotNotifier is the process-local watch/publisher boundary. It is
// deliberately separate from durable storage: notification is replayable and
// never rolls back a committed Store result.
type SnapshotNotifier interface {
	Notify(context.Context, actorrouting.RouteSnapshot) error
}

type SnapshotNotifierFunc func(context.Context, actorrouting.RouteSnapshot) error

func (f SnapshotNotifierFunc) Notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if f == nil {
		return nil
	}
	return f(ctx, snapshot)
}

// AuthoritySnapshotNotifier is the manager-only local notification
// capability. It deliberately has a different method name and interface from
// SnapshotNotifier so an ordinary notifier cannot be used for authority
// publication by accident.
type AuthoritySnapshotNotifier interface {
	AuthorityNotify(context.Context, actorrouting.RouteSnapshot) error
}

type AuthorityNotifier = AuthoritySnapshotNotifier

type AuthoritySnapshotNotifierFunc func(context.Context, actorrouting.RouteSnapshot) error

func (f AuthoritySnapshotNotifierFunc) AuthorityNotify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if f == nil {
		return nil
	}
	return f(ctx, snapshot)
}

type AuthorityNotifierFunc = AuthoritySnapshotNotifierFunc

// SnapshotNotifierAdapter is useful when one local hub intentionally exposes
// both capabilities. Callers must pass it to the matching constructor
// arguments; the publisher never infers one capability from the other.
type SnapshotNotifierAdapter struct {
	NotifyFunc          func(context.Context, actorrouting.RouteSnapshot) error
	AuthorityNotifyFunc func(context.Context, actorrouting.RouteSnapshot) error
}

func (a SnapshotNotifierAdapter) Notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if a.NotifyFunc == nil {
		return nil
	}
	return a.NotifyFunc(ctx, snapshot)
}

func (a SnapshotNotifierAdapter) AuthorityNotify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if a.AuthorityNotifyFunc == nil {
		return nil
	}
	return a.AuthorityNotifyFunc(ctx, snapshot)
}

var ErrSnapshotAuthorityNotifyRequired = errors.New("routecontrol: authority snapshot notifier required")

// DurableSnapshotPublisher applies the store-before-notify protocol. The
// authority method is a separate capability path and uses the existing
// actorrouting authority-update validator.
type DurableSnapshotPublisher struct {
	store             RouteSnapshotStore
	ordinaryNotifier  SnapshotNotifier
	authorityNotifier AuthoritySnapshotNotifier
	// gated mirrors the startup binding between runtime mode and coordinator
	// presence. When a coordinator is assembled, an unissued token is refused;
	// when none is, this publisher takes the G21-identical no-token path and
	// the parameter is inert by design rather than by omission.
	gated bool
}

// RequireControlToken switches this publisher into gated mode. It is called by
// the composition root that also assembles the coordinator, so the two cannot
// disagree: a process with a coordinator refuses ungated publication, and a
// process without one behaves exactly as G21 does today.
func (p *DurableSnapshotPublisher) RequireControlToken() {
	if p != nil {
		p.gated = true
	}
}

func (p *DurableSnapshotPublisher) checkToken(token NestedControlOperationToken) error {
	if p.gated && !token.Issued() {
		return fmt.Errorf("%w: snapshot publication without an issued control token", ErrSnapshotCASConflict)
	}
	return nil
}

func NewDurableSnapshotPublisher(store RouteSnapshotStore, ordinary SnapshotNotifier, authority ...AuthoritySnapshotNotifier) (*DurableSnapshotPublisher, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store required", ErrSnapshotCASConflict)
	}
	if len(authority) > 1 {
		return nil, fmt.Errorf("%w: one authority notifier allowed", ErrSnapshotCASConflict)
	}
	var authorityNotifier AuthoritySnapshotNotifier
	if len(authority) == 1 {
		authorityNotifier = authority[0]
	}
	return &DurableSnapshotPublisher{
		store:             store,
		ordinaryNotifier:  ordinary,
		authorityNotifier: authorityNotifier,
	}, nil
}

func NewSnapshotPublisher(store RouteSnapshotStore, ordinary SnapshotNotifier, authority ...AuthoritySnapshotNotifier) (*DurableSnapshotPublisher, error) {
	return NewDurableSnapshotPublisher(store, ordinary, authority...)
}

// Publish verifies an inherited token rather than acquiring one: it always runs
// inside a caller's control operation (section 8.6).
func (p *DurableSnapshotPublisher) Publish(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return p.publish(ctx, token, snapshot, snapshotPublishOrdinary)
}

func (p *DurableSnapshotPublisher) PublishAuthoritySnapshot(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if p == nil || p.authorityNotifier == nil {
		return ErrSnapshotAuthorityNotifyRequired
	}
	return p.publish(ctx, token, snapshot, snapshotPublishAuthority)
}

// PublishAuthorityWaveSnapshot is the coalesced Drain path: one N→N+1 CAS may
// change many owned routes, including several same-generation ASSIGNING→ACTIVE
// activations. GlobalRevision stays a cursor, not a cross-shard fence.
func (p *DurableSnapshotPublisher) PublishAuthorityWaveSnapshot(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if p == nil || p.authorityNotifier == nil {
		return ErrSnapshotAuthorityNotifyRequired
	}
	return p.publish(ctx, token, snapshot, snapshotPublishAuthorityWave)
}

// Bootstrap is the publisher-level convenience path for the independent
// bootstrap capability. It still requires the underlying Store to implement
// RouteSnapshotBootstrapper and never falls back to ordinary CAS.
// Bootstrap takes its own top-level startup/recovery token rather than an
// inherited one: it runs on the startup path, where two controllers are most
// likely to overlap (section 8.6).
func (p *DurableSnapshotPublisher) Bootstrap(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) (SnapshotCASResult, error) {
	if p == nil || p.store == nil {
		return SnapshotCASResult{}, ErrSnapshotCASConflict
	}
	// Bootstrap acquires its own top-level token rather than inheriting one:
	// it runs at startup and during recovery, when there is no enclosing
	// ScalePlan operation whose token it could legitimately be part of.
	if err := p.checkToken(token); err != nil {
		return SnapshotCASResult{}, err
	}
	bootstrapper, ok := p.store.(RouteSnapshotBootstrapper)
	if !ok {
		return SnapshotCASResult{}, ErrSnapshotBootstrapRequired
	}
	candidate, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	result, err := bootstrapper.Bootstrap(ctx, token, candidate.Snapshot)
	if err != nil {
		return result, err
	}
	if result.Status == SnapshotCASCommitted || result.Status == SnapshotCASAlreadyCommitted {
		if err := p.notify(ctx, result.Stored.Snapshot); err != nil {
			return result, &SnapshotPublishError{Status: result.Status, Committed: true, Err: err}
		}
	}
	return result, nil
}

type snapshotPublishKind uint8

const (
	snapshotPublishOrdinary snapshotPublishKind = iota
	snapshotPublishAuthority
	snapshotPublishAuthorityWave
)

func (p *DurableSnapshotPublisher) publish(ctx context.Context, token NestedControlOperationToken, snapshot actorrouting.RouteSnapshot, kind snapshotPublishKind) error {
	if p == nil || p.store == nil {
		return ErrSnapshotCASConflict
	}
	if err := p.checkToken(token); err != nil {
		return err
	}
	if err := requireContext(ctx); err != nil {
		return err
	}
	candidate, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return err
	}
	current, found, err := p.store.LoadCurrent(ctx)
	if err != nil {
		return err
	}
	if !found {
		return ErrSnapshotBootstrapRequired
	}

	// Exact replay is allowed on both paths and is sent through CAS so a
	// concurrent publisher cannot accidentally advance the durable revision.
	if candidate.EnvelopeEqual(current) {
		result, err := p.store.CompareAndSwap(ctx, token, current.Revision, candidate.Snapshot)
		if err != nil {
			return err
		}
		return p.notifyResult(ctx, result, kind != snapshotPublishOrdinary)
	}
	if candidate.Revision != current.Revision+1 {
		if candidate.Revision <= current.Revision {
			return fmt.Errorf("%w: current=%d next=%d", ErrSnapshotCASConflict, current.Revision, candidate.Revision)
		}
		return fmt.Errorf("%w: strict next revision current=%d next=%d", ErrSnapshotCASConflict, current.Revision, candidate.Revision)
	}
	switch kind {
	case snapshotPublishAuthority:
		err = actorrouting.ValidateAuthoritySnapshotUpdate(current.Snapshot, candidate.Snapshot)
	case snapshotPublishAuthorityWave:
		err = actorrouting.ValidateAuthorityWaveSnapshotUpdate(current.Snapshot, candidate.Snapshot)
	default:
		err = actorrouting.ValidateRouteSnapshotUpdate(current.Snapshot, candidate.Snapshot)
	}
	if err != nil {
		return err
	}
	result, err := p.store.CompareAndSwap(ctx, token, current.Revision, candidate.Snapshot)
	if err != nil {
		return err
	}
	return p.notifyResult(ctx, result, kind != snapshotPublishOrdinary)
}

func (p *DurableSnapshotPublisher) notifyResult(ctx context.Context, result SnapshotCASResult, authority bool) error {
	switch result.Status {
	case SnapshotCASCommitted, SnapshotCASAlreadyCommitted:
		var err error
		if authority {
			err = p.notifyAuthority(ctx, result.Stored.Snapshot)
		} else {
			err = p.notify(ctx, result.Stored.Snapshot)
		}
		if err != nil {
			return &SnapshotPublishError{Status: result.Status, Committed: true, Err: err}
		}
		return nil
	case SnapshotCASRevisionConflict, SnapshotCASStaleExpectation:
		return fmt.Errorf("%w: status=%s", ErrSnapshotCASConflict, result.Status)
	default:
		return fmt.Errorf("%w: unknown CAS status %q", ErrSnapshotCASConflict, result.Status)
	}
}

func (p *DurableSnapshotPublisher) notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if p.ordinaryNotifier == nil {
		return nil
	}
	if err := p.ordinaryNotifier.Notify(ctx, actorrouting.CloneRouteSnapshot(snapshot)); err != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotNotify, err)
	}
	return nil
}

func (p *DurableSnapshotPublisher) notifyAuthority(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if p.authorityNotifier == nil {
		return ErrSnapshotAuthorityNotifyRequired
	}
	if err := p.authorityNotifier.AuthorityNotify(ctx, actorrouting.CloneRouteSnapshot(snapshot)); err != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotNotify, err)
	}
	return nil
}

type SnapshotPublishError struct {
	Status    SnapshotCASStatus
	Committed bool
	Err       error
}

func (e *SnapshotPublishError) Error() string {
	if e == nil {
		return ErrSnapshotNotify.Error()
	}
	return fmt.Sprintf("routecontrol: publish status=%s committed=%t: %v", e.Status, e.Committed, e.Err)
}

// SnapshotCommitted lets lower-level coordinators distinguish a durable CAS
// commit followed by a local notification failure from a rejected write,
// without importing this package or depending on error text.
func (e *SnapshotPublishError) SnapshotCommitted() bool {
	return e != nil && e.Committed
}

func (e *SnapshotPublishError) Unwrap() error {
	if e == nil || e.Err == nil {
		return ErrSnapshotNotify
	}
	return errors.Join(ErrSnapshotNotify, e.Err)
}
