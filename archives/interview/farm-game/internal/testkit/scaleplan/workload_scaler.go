package scaleplan

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	core "farmgame/internal/scaleplan"
)

type ScaleUnknownMode uint8

const (
	ScaleKnown ScaleUnknownMode = iota
	ScaleApplyThenUnknown
	ScaleNoApplyThenUnknown
)

type FakeWorkloadScaler struct {
	mu      sync.Mutex
	state   core.WorkloadState
	calls   int
	unknown ScaleUnknownMode
	onGet   func()
	onScale func(*core.WorkloadState, int32)
}

func NewFakeWorkloadScaler(state core.WorkloadState) *FakeWorkloadScaler {
	state.Pods = append([]core.WorkloadPodState(nil), state.Pods...)
	return &FakeWorkloadScaler{state: state}
}

func (f *FakeWorkloadScaler) Get(ctx context.Context, ref core.WorkloadRef) (core.WorkloadState, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx == nil {
			return core.WorkloadState{}, core.ErrInvalidPlan
		}
		return core.WorkloadState{}, ctx.Err()
	}
	f.mu.Lock()
	if f.state.Ref != ref {
		f.mu.Unlock()
		return core.WorkloadState{}, fmt.Errorf("%w: workload ref", core.ErrConflict)
	}
	state := cloneWorkload(f.state)
	hook := f.onGet
	f.onGet = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return state, nil
}

func (f *FakeWorkloadScaler) Scale(ctx context.Context, ref core.WorkloadRef, expected core.ScalePrecondition, desired int32) (core.ScaleMutationResult, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx == nil {
			return core.ScaleMutationResult{}, core.ErrInvalidPlan
		}
		return core.ScaleMutationResult{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.state.Ref != ref || f.state.UID != expected.WorkloadUID || f.state.ResourceVersion != expected.ResourceVersion ||
		f.state.Replicas != expected.ExpectedReplicas {
		return core.ScaleMutationResult{State: cloneWorkload(f.state)}, fmt.Errorf("%w: scale precondition", core.ErrConflict)
	}
	mode := f.unknown
	f.unknown = ScaleKnown
	if mode != ScaleNoApplyThenUnknown {
		f.state.Replicas = desired
		f.state.Generation++
		f.state.ObservedGeneration = f.state.Generation
		f.state.ResourceVersion = strconv.FormatInt(f.state.Generation+1000, 10)
		if f.onScale != nil {
			f.onScale(&f.state, desired)
		}
	}
	result := core.ScaleMutationResult{Committed: mode == ScaleKnown, State: cloneWorkload(f.state)}
	if mode == ScaleApplyThenUnknown || mode == ScaleNoApplyThenUnknown {
		return result, core.ErrOutcomeUnknown
	}
	return result, nil
}

func (f *FakeWorkloadScaler) SetUnknown(mode ScaleUnknownMode) {
	f.mu.Lock()
	f.unknown = mode
	f.mu.Unlock()
}

func (f *FakeWorkloadScaler) SetGetHook(hook func()) {
	f.mu.Lock()
	f.onGet = hook
	f.mu.Unlock()
}

func (f *FakeWorkloadScaler) SetScaleHook(hook func(*core.WorkloadState, int32)) {
	f.mu.Lock()
	f.onScale = hook
	f.mu.Unlock()
}

func (f *FakeWorkloadScaler) SetState(state core.WorkloadState) {
	f.mu.Lock()
	f.state = cloneWorkload(state)
	f.mu.Unlock()
}

func (f *FakeWorkloadScaler) State() core.WorkloadState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneWorkload(f.state)
}

func (f *FakeWorkloadScaler) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func cloneWorkload(state core.WorkloadState) core.WorkloadState {
	state.Pods = append([]core.WorkloadPodState(nil), state.Pods...)
	return state
}
