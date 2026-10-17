package scaleplan

import (
	"context"
	"sort"
	"sync"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

type FakeHolders struct {
	mu      sync.Mutex
	holders map[string]core.HolderState
}

func NewFakeHolders(values ...core.HolderState) *FakeHolders {
	f := &FakeHolders{holders: make(map[string]core.HolderState)}
	for _, value := range values {
		f.holders[holderKey(value.Ref.Domain, value.Ref.HolderID)] = value
	}
	return f
}

func (f *FakeHolders) GetHolder(ctx context.Context, ref core.HolderRef) (core.HolderState, bool, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.HolderState{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, found := f.holders[holderKey(ref.Domain, ref.HolderID)]
	if !found {
		return core.HolderState{}, false, nil
	}
	if ref.BootID != "" && value.Ref.BootID != ref.BootID {
		return core.HolderState{}, false, nil
	}
	if ref.PodUID != "" && value.Ref.PodUID != ref.PodUID {
		return core.HolderState{}, false, nil
	}
	if ref.WorkloadUID != "" && value.Ref.WorkloadUID != ref.WorkloadUID {
		return core.HolderState{}, false, nil
	}
	if ref.Ordinal >= 0 && value.Ref.Ordinal != ref.Ordinal {
		return core.HolderState{}, false, nil
	}
	return value, true, nil
}

func (f *FakeHolders) ListDomainHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderState, error) {
	if err := fakeContextErr(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]core.HolderState, 0)
	for _, value := range f.holders {
		if value.Ref.Domain == domain {
			out = append(out, value)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ref.Ordinal == out[j].Ref.Ordinal {
			return out[i].Ref.HolderID < out[j].Ref.HolderID
		}
		return out[i].Ref.Ordinal < out[j].Ref.Ordinal
	})
	return out, nil
}

func (f *FakeHolders) Set(value core.HolderState) {
	f.mu.Lock()
	f.holders[holderKey(value.Ref.Domain, value.Ref.HolderID)] = value
	f.mu.Unlock()
}

func (f *FakeHolders) Delete(ref core.HolderRef) {
	f.mu.Lock()
	delete(f.holders, holderKey(ref.Domain, ref.HolderID))
	f.mu.Unlock()
}

func (f *FakeHolders) Update(ref core.HolderRef, mutate func(*core.HolderState)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := holderKey(ref.Domain, ref.HolderID)
	value, found := f.holders[key]
	if !found || (ref.BootID != "" && value.Ref.BootID != ref.BootID) {
		return core.ErrPlanNotFound
	}
	mutate(&value)
	f.holders[key] = value
	return nil
}

func holderKey(domain actorrouting.RouteDomain, id string) string {
	return string(domain) + "\x1f" + id
}
