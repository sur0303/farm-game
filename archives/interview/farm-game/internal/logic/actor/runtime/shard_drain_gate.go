package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrShardDrainGateInvalid = errors.New("runtime: invalid shard drain gate")
	ErrShardDraining         = errors.New("runtime: shard is draining")
	ErrShardDrainNotStarted  = errors.New("runtime: shard drain has not started")
	ErrShardDrainNotComplete = errors.New("runtime: shard drain is not complete")
)

// ShardDrainPhase is process-local admission state, not an Authority
// generation state machine.
type ShardDrainPhase uint8

const (
	ShardDrainPhaseOpen ShardDrainPhase = iota
	ShardDrainPhaseDraining
)

func (p ShardDrainPhase) String() string {
	switch p {
	case ShardDrainPhaseOpen:
		return "OPEN"
	case ShardDrainPhaseDraining:
		return "DRAINING"
	default:
		return "UNSPECIFIED"
	}
}

// ShardForActor maps an actor key to a virtual shard. Callers should bind a
// domain-specific Calculator.ShardFor closure before constructing a gate.
type ShardForActor func(actorID uint64) (uint32, error)

// AdmissionGate is the executor's narrow user-job admission seam.
type AdmissionGate interface {
	Admit(context.Context, uint64) (release func(), err error)
}

// ShardDrainController is the full control seam used by a holder runtime.
// Executor only requires AdmissionGate; the extra methods are for the
// Stop/Wait/Facts control path.
type ShardDrainController interface {
	AdmissionGate
	BeginDrain(shard uint32) error
	Wait(context.Context, uint32) error
	Status(shard uint32) (ShardDrainStatus, bool)
}

type ShardDrainRearmer interface {
	Rearm(shard uint32) error
}

type ShardDrainGateOptions struct {
	ShardCount uint32
	ShardFor   ShardForActor
}

type ShardDrainStatus struct {
	Phase        ShardDrainPhase
	AcceptedJobs uint64
	WaitComplete bool
}

type shardDrainState struct {
	mu           sync.Mutex
	phase        ShardDrainPhase
	acceptedJobs uint64
	waitComplete bool
	zero         chan struct{}
	zeroClosed   bool
}

// ShardDrainGate is a fixed-size, per-shard admission barrier. It never
// stores actor IDs, so active actor cardinality cannot grow its state.
type ShardDrainGate struct {
	shardFor ShardForActor
	states   []shardDrainState
}

var _ AdmissionGate = (*ShardDrainGate)(nil)
var _ ShardDrainController = (*ShardDrainGate)(nil)

func NewShardDrainGate(opts ShardDrainGateOptions) (*ShardDrainGate, error) {
	if opts.ShardCount == 0 || opts.ShardFor == nil {
		return nil, fmt.Errorf("%w: positive shard count and shard mapper required", ErrShardDrainGateInvalid)
	}
	states := make([]shardDrainState, opts.ShardCount)
	for i := range states {
		states[i].zero = make(chan struct{})
	}
	return &ShardDrainGate{shardFor: opts.ShardFor, states: states}, nil
}

func NewShardDrainGateWithOptions(opts ShardDrainGateOptions) (*ShardDrainGate, error) {
	return NewShardDrainGate(opts)
}

func (g *ShardDrainGate) shardState(shard uint32) (*shardDrainState, error) {
	if g == nil || len(g.states) == 0 || uint64(shard) >= uint64(len(g.states)) {
		return nil, fmt.Errorf("%w: shard %d", ErrShardDrainGateInvalid, shard)
	}
	return &g.states[shard], nil
}

func (g *ShardDrainGate) ShardCount() uint32 {
	if g == nil {
		return 0
	}
	return uint32(len(g.states))
}

// Admit is the user-job linearization point. It increments the shard count
// before mailbox creation and returns a once-only release for every outcome.
func (g *ShardDrainGate) Admit(ctx context.Context, actorID uint64) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g == nil || g.shardFor == nil || actorID == 0 {
		return nil, fmt.Errorf("%w: actor admission", ErrShardDrainGateInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	shard, err := g.shardFor(actorID)
	if err != nil {
		return nil, fmt.Errorf("%w: actor %d: %v", ErrShardDrainGateInvalid, actorID, err)
	}
	state, err := g.shardState(shard)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state.phase != ShardDrainPhaseOpen {
		return nil, ErrShardDraining
	}
	state.acceptedJobs++
	var once sync.Once
	return func() {
		once.Do(func() {
			state.mu.Lock()
			if state.acceptedJobs > 0 {
				state.acceptedJobs--
			}
			if state.phase == ShardDrainPhaseDraining && state.acceptedJobs == 0 && !state.zeroClosed {
				state.zeroClosed = true
				close(state.zero)
			}
			state.mu.Unlock()
		})
	}, nil
}

// BeginDrain closes admission for exactly one shard. It is idempotent and
// never changes any other shard.
func (g *ShardDrainGate) BeginDrain(shard uint32) error {
	state, err := g.shardState(shard)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.phase == ShardDrainPhaseDraining {
		return nil
	}
	state.phase = ShardDrainPhaseDraining
	if state.acceptedJobs == 0 && !state.zeroClosed {
		state.zeroClosed = true
		close(state.zero)
	}
	return nil
}

// Wait records that the caller observed the accepted-job barrier. A deadline
// does not advance that terminal observation.
func (g *ShardDrainGate) Wait(ctx context.Context, shard uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := g.shardState(shard)
	if err != nil {
		return err
	}
	state.mu.Lock()
	if state.phase != ShardDrainPhaseDraining {
		state.mu.Unlock()
		return ErrShardDrainNotStarted
	}
	if state.waitComplete {
		state.mu.Unlock()
		return nil
	}
	zero := state.zero
	state.mu.Unlock()
	select {
	case <-zero:
		state.mu.Lock()
		state.waitComplete = true
		state.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Rearm opens a fully drained shard for a later fenced assignment. The caller
// must independently prove the new Authority generation; this gate owns only
// process-local admission and cannot grant shard ownership.
func (g *ShardDrainGate) Rearm(shard uint32) error {
	state, err := g.shardState(shard)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.phase == ShardDrainPhaseOpen {
		return nil
	}
	if state.acceptedJobs != 0 || !state.waitComplete || !state.zeroClosed {
		return ErrShardDrainNotComplete
	}
	state.phase = ShardDrainPhaseOpen
	state.waitComplete = false
	state.zero = make(chan struct{})
	state.zeroClosed = false
	return nil
}

func (g *ShardDrainGate) Status(shard uint32) (ShardDrainStatus, bool) {
	state, err := g.shardState(shard)
	if err != nil {
		return ShardDrainStatus{}, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return ShardDrainStatus{
		Phase:        state.phase,
		AcceptedJobs: state.acceptedJobs,
		WaitComplete: state.waitComplete,
	}, true
}
