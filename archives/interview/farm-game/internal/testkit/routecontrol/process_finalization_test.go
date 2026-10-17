package routecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type finalizationSeamGroup struct {
	mu           sync.Mutex
	cleanupErrs  []error
	closeErrs    []error
	cleanupCalls int
	closeCalls   int
	owns         bool
}

func (g *finalizationSeamGroup) interrupt() error {
	return nil
}

func (g *finalizationSeamGroup) kill() error {
	return nil
}

func (g *finalizationSeamGroup) cleanup(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cleanupCalls++
	if len(g.cleanupErrs) == 0 {
		return nil
	}
	err := g.cleanupErrs[0]
	g.cleanupErrs = g.cleanupErrs[1:]
	return err
}

func (g *finalizationSeamGroup) close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeCalls++
	if len(g.closeErrs) != 0 {
		err := g.closeErrs[0]
		g.closeErrs = g.closeErrs[1:]
		if err != nil {
			return err
		}
	}
	g.owns = false
	return nil
}

func (g *finalizationSeamGroup) ownsCapability() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.owns
}

func (g *finalizationSeamGroup) counts() (cleanup, close int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cleanupCalls, g.closeCalls
}

func newFinalizationSeamProcess(group processGroupCapability) *Process {
	return &Process{
		group: group,
		done:  make(chan struct{}),
	}
}

func TestProcessFinalizationKeepsCleanupErrorAfterCloseSuccess(t *testing.T) {
	injected := errors.New("injected fail-closed cleanup error")
	group := &finalizationSeamGroup{
		cleanupErrs: []error{injected},
		owns:        true,
	}
	process := newFinalizationSeamProcess(group)
	process.finalize()

	firstResult, firstErr := process.Wait(context.Background())
	secondResult, secondErr := process.Wait(context.Background())
	if firstResult != secondResult {
		t.Fatalf("Wait results differ: first=%+v second=%+v", firstResult, secondResult)
	}
	if firstErr == nil || !errors.Is(firstErr, injected) {
		t.Fatalf("first Wait error=%v, want injected cleanup error", firstErr)
	}
	if firstErr != secondErr {
		t.Fatalf("Wait errors differ: first=%v second=%v", firstErr, secondErr)
	}
	if !errors.Is(secondErr, injected) {
		t.Fatalf("second Wait error=%v, want sticky injected cleanup error", secondErr)
	}
	cleanupCalls, closeCalls := group.counts()
	if cleanupCalls != 1 || closeCalls != 1 {
		t.Fatalf("finalization calls cleanup=%d close=%d, want 1/1", cleanupCalls, closeCalls)
	}
}

func TestProcessFinalizationRetriesCloseOwnershipFailure(t *testing.T) {
	injected := errors.New("injected close ownership error")
	group := &finalizationSeamGroup{
		closeErrs: []error{injected},
		owns:      true,
	}
	process := newFinalizationSeamProcess(group)
	process.finalize()

	if _, err := process.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after close retry error=%v, want success", err)
	}
	if _, err := process.Wait(context.Background()); err != nil {
		t.Fatalf("second Wait error=%v, want success", err)
	}
	cleanupCalls, closeCalls := group.counts()
	if cleanupCalls != 1 || closeCalls != 2 {
		t.Fatalf("finalization calls cleanup=%d close=%d, want 1/2", cleanupCalls, closeCalls)
	}
}
