package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"farmgame/internal/controlfence"
	core "farmgame/internal/scaleplan"
)

var (
	ErrCoordinatorInvalid      = errors.New("scaleplan control adapter: invalid coordinator request")
	ErrCoordinatorScope        = errors.New("scaleplan control adapter: scope mismatch")
	ErrCoordinatorClosing      = errors.New("scaleplan control adapter: gate is closing")
	ErrCoordinatorExclusive    = errors.New("scaleplan control adapter: gate is exclusive")
	ErrCoordinatorActive       = errors.New("scaleplan control adapter: active operations remain")
	ErrCoordinatorOperation    = errors.New("scaleplan control adapter: operation conflict")
	ErrCoordinatorResolved     = errors.New("scaleplan control adapter: operation already resolved")
	ErrCoordinatorForeignToken = errors.New("scaleplan control adapter: foreign token")
	ErrCoordinatorExpired      = errors.New("scaleplan control adapter: token expired")
	ErrCoordinatorNotExclusive = errors.New("scaleplan control adapter: gate is not exclusive")
)

const (
	defaultCoordinatorLeaseTTL = 30 * time.Second
	// The coordinator lease must outlive the longest admitted ScalePlan
	// operation plus its resolution buffer. It is process-local and remains
	// bounded; this does not extend any holder or shard authority lease.
	maxCoordinatorLeaseTTL = 31 * time.Minute
)

// ControlMutationCoordinatorOptions fixes the in-process coordinator's
// serialization domain and clock at composition time. The in-process
// implementation is intentionally single-instance: controller attempts share
// one coordinator object and are distinguished by MutationLeaseOwner.
type ControlMutationCoordinatorOptions struct {
	Scope    core.MutationScope
	LeaseTTL time.Duration
	Now      func() time.Time
	Epoch    uint64
}

// ExclusiveControlCoordinator is the A3 extension used by blocked
// quiescence. The ordinary scaleplan port remains the small core interface.
type ExclusiveControlCoordinator interface {
	core.ControlMutationCoordinator
	AcquireExclusive(context.Context, core.ControlOperationRequest) (ExclusiveControlFence, error)
}

// ExclusiveControlFence is an opaque, owner-bound EXCLUSIVE interval.
type ExclusiveControlFence interface {
	Revalidate(context.Context) error
	Release(context.Context) error
}

type controlCoordinator struct {
	mu sync.Mutex

	scope core.MutationScope
	ttl   time.Duration
	now   func() time.Time

	epoch  uint64
	mode   core.ControlGateMode
	issuer *controlfence.Issuer

	active   map[string]*activeOperation
	resolved map[string]controlfence.NestedControlOperationToken

	exclusive *exclusiveOperation
	changed   chan struct{}
}

type activeOperation struct {
	request  core.ControlOperationRequest
	token    controlfence.NestedControlOperationToken
	expires  time.Time
	resolved bool
}

type exclusiveOperation struct {
	scope       core.MutationScope
	owner       core.MutationLeaseOwner
	operationID string
	token       core.ControlOperationToken
	epoch       uint64
	expires     time.Time
}

type exclusiveFence struct {
	owner       *controlCoordinator
	scope       core.MutationScope
	ownerID     core.MutationLeaseOwner
	token       core.ControlOperationToken
	operationID string
	epoch       uint64
	expires     time.Time
}

// NewControlMutationCoordinator constructs the production in-process gate.
// It returns the extension interface so callers that need only the core port
// can assign the result directly to scaleplan.ControlMutationCoordinator.
func NewControlMutationCoordinator(options ControlMutationCoordinatorOptions) (ExclusiveControlCoordinator, error) {
	if err := options.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCoordinatorInvalid, err)
	}
	if options.LeaseTTL == 0 {
		options.LeaseTTL = defaultCoordinatorLeaseTTL
	}
	if options.LeaseTTL <= 0 || options.LeaseTTL > maxCoordinatorLeaseTTL {
		return nil, fmt.Errorf("%w: lease ttl", ErrCoordinatorInvalid)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Now().IsZero() {
		return nil, fmt.Errorf("%w: clock", ErrCoordinatorInvalid)
	}
	if options.Epoch == 0 {
		options.Epoch = 1
	}
	return &controlCoordinator{
		scope:    options.Scope,
		ttl:      options.LeaseTTL,
		now:      options.Now,
		epoch:    options.Epoch,
		mode:     core.ControlGateOpen,
		issuer:   controlfence.NewIssuer(),
		active:   make(map[string]*activeOperation),
		resolved: make(map[string]controlfence.NestedControlOperationToken),
		changed:  make(chan struct{}),
	}, nil
}

func (c *controlCoordinator) Acquire(ctx context.Context, request core.ControlOperationRequest) (core.ControlOperationToken, error) {
	if err := coordinatorContext(ctx); err != nil {
		return core.ControlOperationToken{}, err
	}
	if c == nil {
		return core.ControlOperationToken{}, ErrCoordinatorInvalid
	}
	if err := request.Validate(); err != nil {
		return core.ControlOperationToken{}, fmt.Errorf("%w: %v", ErrCoordinatorInvalid, err)
	}
	if request.Scope != c.scope {
		return core.ControlOperationToken{}, ErrCoordinatorScope
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.hasExpiredActiveLocked(now) {
		return core.ControlOperationToken{}, ErrCoordinatorExpired
	}

	if current, found := c.active[request.OperationID]; found {
		if current.request != request {
			return core.ControlOperationToken{}, ErrCoordinatorOperation
		}
		if !current.expires.After(now) {
			return core.ControlOperationToken{}, ErrCoordinatorExpired
		}
		return current.token, nil
	}
	if _, found := c.resolved[request.OperationID]; found {
		return core.ControlOperationToken{}, ErrCoordinatorResolved
	}

	switch c.mode {
	case core.ControlGateOpen:
		if request.ResolutionOnly {
			return core.ControlOperationToken{}, ErrCoordinatorClosing
		}
	case core.ControlGateClosing:
		if !request.ResolutionOnly || c.exclusive == nil || request.Owner != c.exclusive.owner ||
			request.Scope != c.exclusive.scope {
			return core.ControlOperationToken{}, ErrCoordinatorClosing
		}
	case core.ControlGateExclusive:
		return core.ControlOperationToken{}, ErrCoordinatorExclusive
	default:
		return core.ControlOperationToken{}, ErrCoordinatorInvalid
	}

	token := c.issuer.Issue(request.OperationID, c.epoch)
	if !token.Issued() {
		return core.ControlOperationToken{}, ErrCoordinatorInvalid
	}
	c.active[request.OperationID] = &activeOperation{
		request: request,
		token:   token,
		expires: now.Add(c.ttl),
	}
	return token, nil
}

func (c *controlCoordinator) Verify(ctx context.Context, token core.ControlOperationToken, scope core.MutationScope) error {
	if err := coordinatorContext(ctx); err != nil {
		return err
	}
	if c == nil {
		return ErrCoordinatorInvalid
	}
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrCoordinatorScope, err)
	}
	if scope != c.scope {
		return ErrCoordinatorScope
	}
	if !token.Issued() || !c.issuer.IssuedBy(token) {
		return ErrCoordinatorForeignToken
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == core.ControlGateExclusive && c.exclusive != nil && c.exclusive.token == token {
		if !c.exclusive.expires.After(c.now()) {
			return ErrCoordinatorExpired
		}
		return nil
	}
	record, found := c.active[token.OperationID()]
	if !found || record.token != token {
		if resolved, wasResolved := c.resolved[token.OperationID()]; wasResolved && resolved == token {
			return ErrCoordinatorResolved
		}
		return ErrCoordinatorForeignToken
	}
	if !record.expires.After(c.now()) {
		return ErrCoordinatorExpired
	}
	return nil
}

func (c *controlCoordinator) Resolve(ctx context.Context, token core.ControlOperationToken) error {
	if err := coordinatorContext(ctx); err != nil {
		return err
	}
	if c == nil {
		return ErrCoordinatorInvalid
	}
	if !token.Issued() || !c.issuer.IssuedBy(token) {
		return ErrCoordinatorForeignToken
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	record, found := c.active[token.OperationID()]
	if !found || record.token != token {
		if resolved, wasResolved := c.resolved[token.OperationID()]; wasResolved && resolved == token {
			return nil
		}
		return ErrCoordinatorForeignToken
	}
	if !record.expires.After(c.now()) {
		return ErrCoordinatorExpired
	}
	delete(c.active, token.OperationID())
	record.resolved = true
	c.resolved[token.OperationID()] = token
	c.signalLocked()
	return nil
}

func (c *controlCoordinator) Mode(ctx context.Context, scope core.MutationScope) (core.ControlGateMode, error) {
	if err := coordinatorContext(ctx); err != nil {
		return "", err
	}
	if c == nil {
		return "", ErrCoordinatorInvalid
	}
	if err := scope.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrCoordinatorScope, err)
	}
	if scope != c.scope {
		return "", ErrCoordinatorScope
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.hasExpiredActiveLocked(now) {
		return "", ErrCoordinatorExpired
	}
	if c.exclusive != nil && !c.exclusive.expires.After(now) {
		return "", ErrCoordinatorExpired
	}
	return c.mode, nil
}

func (c *controlCoordinator) AcquireExclusive(ctx context.Context, request core.ControlOperationRequest) (ExclusiveControlFence, error) {
	if err := coordinatorContext(ctx); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrCoordinatorInvalid
	}
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCoordinatorInvalid, err)
	}
	if request.Scope != c.scope {
		return nil, ErrCoordinatorScope
	}
	if request.ResolutionOnly {
		return nil, ErrCoordinatorInvalid
	}

	for {
		c.mu.Lock()
		now := c.now()
		if c.hasExpiredActiveLocked(now) {
			c.mu.Unlock()
			return nil, ErrCoordinatorExpired
		}
		switch c.mode {
		case core.ControlGateOpen:
			if _, found := c.resolved[request.OperationID]; found {
				c.mu.Unlock()
				return nil, ErrCoordinatorResolved
			}
			if _, found := c.active[request.OperationID]; found {
				c.mu.Unlock()
				return nil, ErrCoordinatorOperation
			}
			c.epoch++
			if c.epoch == 0 {
				c.mu.Unlock()
				return nil, ErrCoordinatorInvalid
			}
			c.mode = core.ControlGateClosing
			exclusiveToken := c.issuer.Issue(request.OperationID, c.epoch)
			c.exclusive = &exclusiveOperation{
				scope: request.Scope, owner: request.Owner, operationID: request.OperationID,
				token: exclusiveToken, epoch: c.epoch, expires: now.Add(c.ttl),
			}
			c.signalLocked()
		case core.ControlGateClosing, core.ControlGateExclusive:
			if c.exclusive == nil || c.exclusive.scope != request.Scope ||
				c.exclusive.owner != request.Owner || c.exclusive.operationID != request.OperationID {
				c.mu.Unlock()
				return nil, ErrCoordinatorClosing
			}
			if !c.exclusive.expires.After(now) {
				c.mu.Unlock()
				return nil, ErrCoordinatorExpired
			}
		default:
			c.mu.Unlock()
			return nil, ErrCoordinatorInvalid
		}

		if len(c.active) == 0 {
			c.mode = core.ControlGateExclusive
			fence := &exclusiveFence{
				owner: c, scope: c.scope, ownerID: request.Owner,
				token: c.exclusive.token, operationID: request.OperationID, epoch: c.exclusive.epoch,
				expires: c.exclusive.expires,
			}
			c.signalLocked()
			c.mu.Unlock()
			return fence, nil
		}
		changed := c.changed
		c.mu.Unlock()
		if err := waitForCoordinatorChange(ctx, changed, c.ttl); err != nil {
			return nil, err
		}
	}
}

func (f *exclusiveFence) Revalidate(ctx context.Context) error {
	if err := coordinatorContext(ctx); err != nil {
		return err
	}
	if f == nil || f.owner == nil {
		return ErrCoordinatorInvalid
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != core.ControlGateExclusive || c.exclusive == nil ||
		c.exclusive.scope != f.scope || c.exclusive.owner != f.ownerID ||
		c.exclusive.operationID != f.operationID || c.exclusive.epoch != f.epoch {
		return ErrCoordinatorNotExclusive
	}
	if !c.exclusive.expires.After(c.now()) || !f.expires.After(c.now()) {
		return ErrCoordinatorExpired
	}
	if c.hasExpiredActiveLocked(c.now()) {
		return ErrCoordinatorExpired
	}
	if len(c.active) != 0 {
		return ErrCoordinatorActive
	}
	return nil
}

// ControlToken returns the unforgeable inherited token for writes performed
// while this exclusive fence is held. It is deliberately an optional
// extension of ExclusiveControlFence so callers that only need the gate do
// not receive a second mutation surface.
func (f *exclusiveFence) ControlToken() core.ControlOperationToken {
	if f == nil {
		return core.ControlOperationToken{}
	}
	return f.token
}
func (f *exclusiveFence) Release(ctx context.Context) error {
	if err := coordinatorContext(ctx); err != nil {
		return err
	}
	if f == nil || f.owner == nil {
		return ErrCoordinatorInvalid
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode == core.ControlGateOpen {
		if c.exclusive == nil && c.releasedFenceMatchesLocked(f) {
			return nil
		}
		return ErrCoordinatorNotExclusive
	}
	if c.mode != core.ControlGateExclusive || c.exclusive == nil ||
		c.exclusive.scope != f.scope || c.exclusive.owner != f.ownerID ||
		c.exclusive.operationID != f.operationID || c.exclusive.epoch != f.epoch {
		return ErrCoordinatorNotExclusive
	}
	if !c.exclusive.expires.After(c.now()) {
		return ErrCoordinatorExpired
	}
	if c.hasExpiredActiveLocked(c.now()) {
		return ErrCoordinatorExpired
	}
	if len(c.active) != 0 {
		return ErrCoordinatorActive
	}
	c.resolved[f.operationID] = c.exclusive.token
	c.exclusive = nil
	c.mode = core.ControlGateOpen
	c.signalLocked()
	return nil
}

func (c *controlCoordinator) releasedFenceMatchesLocked(f *exclusiveFence) bool {
	token, found := c.resolved[f.operationID]
	return found && token.Epoch() == f.epoch && c.issuer.IssuedBy(token)
}

func (c *controlCoordinator) hasExpiredActiveLocked(now time.Time) bool {
	for _, record := range c.active {
		if !record.expires.After(now) {
			return true
		}
	}
	return false
}

func (c *controlCoordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func waitForCoordinatorChange(ctx context.Context, changed <-chan struct{}, ttl time.Duration) error {
	timer := time.NewTimer(ttl)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer.C:
		return ErrCoordinatorActive
	}
}

func coordinatorContext(ctx context.Context) error {
	if ctx == nil {
		return ErrCoordinatorInvalid
	}
	return ctx.Err()
}

var _ core.ControlMutationCoordinator = (*controlCoordinator)(nil)
var _ ExclusiveControlCoordinator = (*controlCoordinator)(nil)
var _ ExclusiveControlFence = (*exclusiveFence)(nil)
