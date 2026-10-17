package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"farmgame/internal/worker/readiness"
)

type observedRound struct {
	stats             map[readiness.Family]readiness.ScanStats
	due               map[readiness.Family]readiness.DueObservation
	terminalLatencies map[readiness.Family][]time.Duration
	err               error
	nextDelay         time.Duration
}

// runObservedLoop establishes every lifecycle handle before closing started.
// The close is the startup happens-before barrier used by main and tests. A
// panic terminates only this scan loop and synchronously marks its Families
// FAILED; process liveness remains independent.
func runObservedLoop(ctx context.Context, interval time.Duration, registry *readiness.Registry, metrics *readiness.Metrics,
	families []readiness.Family, scan func(context.Context) observedRound, started chan<- struct{}, rounds chan<- struct{}, stopped chan<- struct{}) {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	handles := make([]*readiness.LoopHandle, 0, len(families))
	for _, family := range families {
		handle, err := registry.StartLoop(family)
		if err != nil {
			log.Printf("start %s recovery lifecycle: %v", family, err)
			continue
		}
		handles = append(handles, handle)
	}
	if started != nil {
		close(started)
	}
	defer func() {
		class := "LOOP_EXITED"
		if recovered := recover(); recovered != nil {
			class = "LOOP_PANIC"
			log.Printf("farm-worker recovery loop panic families=%v", families)
		}
		for _, handle := range handles {
			handle.Stop(class)
		}
		if stopped != nil {
			close(stopped)
		}
	}()
	if len(handles) == 0 || scan == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			attempt := time.Now()
			for _, handle := range handles {
				handle.ScanStarted(attempt)
			}
			result := scan(ctx)
			finished := time.Now()
			duration := finished.Sub(attempt)
			for _, handle := range handles {
				family := handle.Family()
				if result.err != nil {
					handle.ScanFailed(finished, readinessErrorClass(result.err))
				} else {
					handle.ScanSucceeded(finished, result.due[family])
				}
				if metrics != nil {
					metrics.ObserveScan(family, duration, result.stats[family])
					for _, latency := range result.terminalLatencies[family] {
						metrics.ObserveTerminalLatency(family, latency)
					}
				}
			}
			if rounds != nil {
				select {
				case rounds <- struct{}{}:
				default:
				}
			}
			nextDelay := interval
			if result.nextDelay > nextDelay {
				nextDelay = result.nextDelay
			}
			timer.Reset(nextDelay)
		}
	}
}

// emptySweepBackoff inserts a pause only between complete, empty finite-space
// sweeps. The caller resets emptySweeps as soon as work or an error is seen.
func emptySweepBackoff(base, max time.Duration, emptySweeps int) time.Duration {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if max < base {
		max = base
	}
	if emptySweeps <= 1 {
		return base
	}
	delay := base
	for step := 1; step < emptySweeps && delay < max; step++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

type adaptiveShardSweep struct {
	revision    uint64
	owned       uint64
	scanned     uint64
	emptySweeps int
}

// next delays only after a complete empty traversal of the current assignment.
// A partial rotating round never triggers idle backoff, because a later bucket
// in the same assignment may already contain due work.
func (s *adaptiveShardSweep) next(base, max time.Duration, revision, owned, scanned, candidates uint64, scanErr error) time.Duration {
	if s == nil {
		return base
	}
	if scanErr != nil || candidates != 0 {
		*s = adaptiveShardSweep{revision: revision, owned: owned}
		return base
	}
	if s.revision != revision || s.owned != owned {
		*s = adaptiveShardSweep{revision: revision, owned: owned}
	}
	if owned == 0 {
		s.emptySweeps++
		return emptySweepBackoff(base, max, s.emptySweeps)
	}
	s.scanned += scanned
	if s.scanned < owned {
		return base
	}
	s.scanned = 0
	s.emptySweeps++
	return emptySweepBackoff(base, max, s.emptySweeps)
}

func suppressExpectedOnly(err, expected error) error {
	if err == nil || expected == nil || !errorTreeContainsOnly(err, expected) {
		return err
	}
	return nil
}

func errorTreeContainsOnly(err, expected error) bool {
	if err == nil {
		return true
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		children := multi.Unwrap()
		if len(children) == 0 {
			return errors.Is(err, expected)
		}
		for _, child := range children {
			if !errorTreeContainsOnly(child, expected) {
				return false
			}
		}
		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return errorTreeContainsOnly(child, expected)
	}
	return errors.Is(err, expected)
}

func readinessErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "SCAN_TIMEOUT"
	case errors.Is(err, context.Canceled):
		return "SCAN_CANCELED"
	default:
		return "SCAN_ERROR"
	}
}

func startObservedLoop(ctx context.Context, interval time.Duration, registry *readiness.Registry, metrics *readiness.Metrics,
	families []readiness.Family, scan func(context.Context) observedRound) <-chan struct{} {
	started := make(chan struct{})
	go runObservedLoop(ctx, interval, registry, metrics, families, scan, started, nil, nil)
	return started
}

func waitLoopStarts(channels ...<-chan struct{}) {
	var wait sync.WaitGroup
	wait.Add(len(channels))
	for _, channel := range channels {
		channel := channel
		go func() {
			defer wait.Done()
			<-channel
		}()
	}
	wait.Wait()
}

func provenDue(depth, oldestDueAtMs int64, available bool) readiness.DueObservation {
	observation := readiness.DueObservation{Depth: depth, DepthAvailable: available, OldestDueAgeAvailable: available}
	if available && oldestDueAtMs > 0 {
		observation.OldestDueAt = time.UnixMilli(oldestDueAtMs)
	}
	return observation
}

func mergeDue(observations ...readiness.DueObservation) readiness.DueObservation {
	result := readiness.DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true}
	for _, observation := range observations {
		if !observation.DepthAvailable || !observation.OldestDueAgeAvailable {
			return readiness.DueObservation{}
		}
		result.Depth += observation.Depth
		if !observation.OldestDueAt.IsZero() && (result.OldestDueAt.IsZero() || observation.OldestDueAt.Before(result.OldestDueAt)) {
			result.OldestDueAt = observation.OldestDueAt
		}
	}
	return result
}
