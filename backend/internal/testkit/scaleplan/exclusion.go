package scaleplan

import (
	"context"
	"fmt"
	"sync"

	core "farmgame/internal/scaleplan"
)

type ExclusionUnknownMode uint8

type ExclusionReleaseUnknownMode uint8

const (
	ExclusionReleaseKnown ExclusionReleaseUnknownMode = iota
	ExclusionReleaseApplyThenUnknown
	ExclusionReleaseNoApplyThenUnknown
)
const (
	ExclusionKnown ExclusionUnknownMode = iota
	ExclusionApplyThenUnknown
	ExclusionNoApplyThenUnknown
)

type FakeExclusions struct {
	mu                   sync.Mutex
	states               map[string]core.PlacementExclusion
	holders              *FakeHolders
	calls                int
	unknown              ExclusionUnknownMode
	releaseCalls         int
	releaseUnknown       ExclusionReleaseUnknownMode
	forcedReleaseOutcome core.PlacementReleaseOutcome
	onGet                func()
}

func NewFakeExclusions(holders *FakeHolders, states ...core.PlacementExclusion) *FakeExclusions {
	f := &FakeExclusions{states: make(map[string]core.PlacementExclusion), holders: holders}
	for _, state := range states {
		f.states[holderKey(state.Holder.Domain, state.Holder.HolderID)] = state
	}
	return f
}

func (f *FakeExclusions) Get(ctx context.Context, holder core.HolderRef) (core.PlacementExclusion, bool, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.PlacementExclusion{}, false, err
	}
	f.mu.Lock()
	state, found := f.states[holderKey(holder.Domain, holder.HolderID)]
	if !found || (holder.BootID != "" && state.Holder.BootID != holder.BootID) {
		f.mu.Unlock()
		return core.PlacementExclusion{}, false, nil
	}
	hook := f.onGet
	f.onGet = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return state, true, nil
}

func (f *FakeExclusions) Exclude(ctx context.Context, holder core.HolderRef, expected core.PlacementExclusionPrecondition) (core.PlacementExclusionResult, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.PlacementExclusionResult{}, err
	}
	f.mu.Lock()
	f.calls++
	key := holderKey(holder.Domain, holder.HolderID)
	state, found := f.states[key]
	if !found || state.Holder != holder || state.Revision != expected.ExpectedRevision ||
		state.Excluded != expected.ExpectedExcluded || expected.OwnerPlanID == "" {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state}, fmt.Errorf("%w: exclusion precondition", core.ErrConflict)
	}
	mode := f.unknown
	f.unknown = ExclusionKnown
	if mode != ExclusionNoApplyThenUnknown {
		state.Excluded = true
		state.OwnerPlanID = expected.OwnerPlanID
		state.Revision++
		f.states[key] = state
	}
	f.mu.Unlock()
	if mode != ExclusionNoApplyThenUnknown && f.holders != nil {
		_ = f.holders.Update(holder, func(value *core.HolderState) {
			value.PlacementEligible = false
			value.PlacementRevision = state.Revision
		})
	}
	result := core.PlacementExclusionResult{Committed: mode == ExclusionKnown, State: state}
	if mode == ExclusionApplyThenUnknown || mode == ExclusionNoApplyThenUnknown {
		return result, core.ErrOutcomeUnknown
	}
	return result, nil
}

func (f *FakeExclusions) Set(state core.PlacementExclusion) {
	f.mu.Lock()
	f.states[holderKey(state.Holder.Domain, state.Holder.HolderID)] = state
	f.mu.Unlock()
}

func (f *FakeExclusions) SetGetHook(hook func()) {
	f.mu.Lock()
	f.onGet = hook
	f.mu.Unlock()
}

func (f *FakeExclusions) SetUnknown(mode ExclusionUnknownMode) {
	f.mu.Lock()
	f.unknown = mode
	f.mu.Unlock()
}

func (f *FakeExclusions) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}
func (f *FakeExclusions) ReleaseForOwner(ctx context.Context, holder core.HolderRef, expected core.PlacementExclusionRelease) (core.PlacementExclusionResult, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if f.holders != nil {
		current, found, err := f.holders.GetHolder(ctx, core.HolderRef{Domain: holder.Domain, HolderID: holder.HolderID, Ordinal: -1})
		if err != nil {
			return core.PlacementExclusionResult{}, err
		}
		if !found {
			return core.PlacementExclusionResult{Outcome: core.PlacementReleaseNotFound}, nil
		}
		if current.Ref.BootID != holder.BootID {
			return core.PlacementExclusionResult{Outcome: core.PlacementReleaseBootRollover}, nil
		}
	}
	f.mu.Lock()
	f.releaseCalls++
	key := holderKey(holder.Domain, holder.HolderID)
	state, found := f.states[key]
	forced := f.forcedReleaseOutcome
	f.forcedReleaseOutcome = ""
	mode := f.releaseUnknown
	f.releaseUnknown = ExclusionReleaseKnown
	if !found {
		f.mu.Unlock()
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseNotFound}, nil
	}
	if forced != "" {
		f.mu.Unlock()
		result := core.PlacementExclusionResult{Outcome: forced, State: state}
		if forced == core.PlacementReleaseUnknown {
			return result, core.ErrOutcomeUnknown
		}
		return result, nil
	}
	if !holder.Exact() || !state.Holder.Exact() || state.Holder != holder || state.Revision == 0 ||
		expected.OwnerPlanID == "" || expected.ExpectedRevision == 0 {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state, Outcome: core.PlacementReleaseMalformed}, nil
	}
	if !state.Excluded {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state, Outcome: core.PlacementReleaseAlreadyReleased}, nil
	}
	if state.OwnerPlanID == "" {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state, Outcome: core.PlacementReleaseMalformed}, nil
	}
	if state.OwnerPlanID != expected.OwnerPlanID {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state, Outcome: core.PlacementReleaseOwnerMismatch}, nil
	}
	if state.Revision != expected.ExpectedRevision {
		f.mu.Unlock()
		return core.PlacementExclusionResult{State: state, Outcome: core.PlacementReleaseRevisionConflict}, nil
	}
	applied := mode != ExclusionReleaseNoApplyThenUnknown
	if applied {
		state.Excluded = false
		state.OwnerPlanID = ""
		state.Revision++
		f.states[key] = state
	}
	f.mu.Unlock()
	if applied && f.holders != nil {
		_ = f.holders.Update(holder, func(value *core.HolderState) {
			value.PlacementEligible = true
			value.PlacementRevision = state.Revision
		})
	}
	result := core.PlacementExclusionResult{Committed: mode == ExclusionReleaseKnown, State: state}
	if mode == ExclusionReleaseApplyThenUnknown || mode == ExclusionReleaseNoApplyThenUnknown {
		result.Outcome = core.PlacementReleaseUnknown
		return result, core.ErrOutcomeUnknown
	}
	result.Outcome = core.PlacementReleaseReleased
	return result, nil
}

func (f *FakeExclusions) SetReleaseUnknown(mode ExclusionReleaseUnknownMode) {
	f.mu.Lock()
	f.releaseUnknown = mode
	f.mu.Unlock()
}

func (f *FakeExclusions) SetReleaseOutcome(outcome core.PlacementReleaseOutcome) {
	f.mu.Lock()
	f.forcedReleaseOutcome = outcome
	f.mu.Unlock()
}

func (f *FakeExclusions) ReleaseCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseCalls
}
