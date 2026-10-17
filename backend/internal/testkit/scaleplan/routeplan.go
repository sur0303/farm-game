package scaleplan

import (
	"context"
	"fmt"
	"sync"

	core "farmgame/internal/scaleplan"
)

type RoutePlanUnknownMode uint8

const (
	RoutePlanKnown RoutePlanUnknownMode = iota
	RoutePlanApplyThenUnknown
	RoutePlanNoApplyThenUnknown
)

type FakeRoutePlans struct {
	mu                 sync.Mutex
	requests           map[string]core.RoutePlanMoveRequest
	views              map[string]core.RoutePlanView
	verified           map[string]core.RoutePlanVerification
	ensureCalls        map[string]int
	unknown            RoutePlanUnknownMode
	nextPhase          string
	defaultPhase       string
	ensureFailureAfter int
	ensureFailure      error
	onGet              func()
	onEnsure           func(core.RoutePlanMoveRequest)
}

func NewFakeRoutePlans() *FakeRoutePlans {
	return &FakeRoutePlans{requests: make(map[string]core.RoutePlanMoveRequest), views: make(map[string]core.RoutePlanView),
		verified: make(map[string]core.RoutePlanVerification), ensureCalls: make(map[string]int), ensureFailureAfter: -1}
}

func (f *FakeRoutePlans) EnsureMove(ctx context.Context, request core.RoutePlanMoveRequest) (core.RoutePlanView, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx == nil {
			return core.RoutePlanView{}, core.ErrInvalidPlan
		}
		return core.RoutePlanView{}, ctx.Err()
	}
	f.mu.Lock()
	f.ensureCalls[request.PlanID]++
	if existing, found := f.requests[request.PlanID]; found {
		view := f.views[request.PlanID]
		f.mu.Unlock()
		if existing != request {
			return view, fmt.Errorf("%w: same child id with different payload", core.ErrConflict)
		}
		return view, nil
	}
	if f.ensureFailureAfter >= 0 && len(f.requests) >= f.ensureFailureAfter {
		err := f.ensureFailure
		f.mu.Unlock()
		if err == nil {
			err = core.ErrDependency
		}
		return core.RoutePlanView{}, err
	}
	mode := f.unknown
	phase := f.nextPhase
	if phase == "" {
		phase = f.defaultPhase
	}
	if phase == "" {
		phase = core.RoutePlanPhaseSucceeded
	}
	f.unknown = RoutePlanKnown
	f.nextPhase = ""
	if mode == RoutePlanNoApplyThenUnknown {
		f.mu.Unlock()
		return core.RoutePlanView{}, core.ErrOutcomeUnknown
	}
	view := core.RoutePlanView{PlanID: request.PlanID, PayloadDigest: request.PayloadDigest, Phase: phase}
	f.requests[request.PlanID] = request
	f.views[request.PlanID] = view
	f.verified[request.PlanID] = core.RoutePlanVerification{PlanID: request.PlanID, Consistent: true}
	hook := f.onEnsure
	f.mu.Unlock()
	if hook != nil {
		hook(request)
	}
	if mode == RoutePlanApplyThenUnknown {
		return view, core.ErrOutcomeUnknown
	}
	return view, nil
}

func (f *FakeRoutePlans) GetMove(ctx context.Context, expected core.RoutePlanMoveRequest) (core.RoutePlanView, bool, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx == nil {
			return core.RoutePlanView{}, false, core.ErrInvalidPlan
		}
		return core.RoutePlanView{}, false, ctx.Err()
	}
	f.mu.Lock()
	request, found := f.requests[expected.PlanID]
	view := f.views[expected.PlanID]
	hook := f.onGet
	f.onGet = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if !found {
		return core.RoutePlanView{}, false, nil
	}
	if request != expected {
		return view, true, fmt.Errorf("%w: existing child immutable request differs", core.ErrConflict)
	}
	return view, true, nil
}

func (f *FakeRoutePlans) Verify(ctx context.Context, id string) (core.RoutePlanVerification, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx == nil {
			return core.RoutePlanVerification{}, core.ErrInvalidPlan
		}
		return core.RoutePlanVerification{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, found := f.verified[id]
	if !found {
		return core.RoutePlanVerification{}, core.ErrPlanNotFound
	}
	return value, nil
}

func (f *FakeRoutePlans) SetGetHook(hook func()) {
	f.mu.Lock()
	f.onGet = hook
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetEnsureHook(hook func(core.RoutePlanMoveRequest)) {
	f.mu.Lock()
	f.onEnsure = hook
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetNextPhase(phase string) {
	f.mu.Lock()
	f.nextPhase = phase
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetDefaultPhase(phase string) {
	f.mu.Lock()
	f.defaultPhase = phase
	f.mu.Unlock()
}

// SetEnsureFailureAfter makes new child creation fail once the requested
// number of distinct child records exists. Existing records remain
// idempotently readable, which lets restart tests model a partial-create
// crash without inventing a second child identity.
func (f *FakeRoutePlans) SetEnsureFailureAfter(count int, err error) {
	f.mu.Lock()
	f.ensureFailureAfter = count
	f.ensureFailure = err
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetUnknown(mode RoutePlanUnknownMode) {
	f.mu.Lock()
	f.unknown = mode
	f.mu.Unlock()
}

func (f *FakeRoutePlans) CorruptRequest(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	request, found := f.requests[id]
	if !found {
		return false
	}
	request.RequestedTTLms++
	f.requests[id] = request
	return true
}

func (f *FakeRoutePlans) Delete(id string) {
	f.mu.Lock()
	delete(f.requests, id)
	delete(f.views, id)
	delete(f.verified, id)
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetView(id, phase string) {
	f.SetViewWithError(id, phase, "")
}

func (f *FakeRoutePlans) SetViewWithError(id, phase, errorCode string) {
	f.mu.Lock()
	value := f.views[id]
	value.Phase = phase
	value.ErrorCode = errorCode
	f.views[id] = value
	f.mu.Unlock()
}

func (f *FakeRoutePlans) SetVerification(id string, consistent bool, code string) {
	f.mu.Lock()
	f.verified[id] = core.RoutePlanVerification{PlanID: id, Consistent: consistent, ErrorCode: code}
	f.mu.Unlock()
}

func (f *FakeRoutePlans) EnsureCalls(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ensureCalls[id]
}

func (f *FakeRoutePlans) Requests() []core.RoutePlanMoveRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]core.RoutePlanMoveRequest, 0, len(f.requests))
	for _, request := range f.requests {
		out = append(out, request)
	}
	return out
}
