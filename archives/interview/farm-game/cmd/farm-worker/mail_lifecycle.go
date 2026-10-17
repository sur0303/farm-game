package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	workerreadiness "farmgame/internal/worker/readiness"
)

type mailObservedSource struct {
	name       string
	interval   time.Duration
	staleAfter time.Duration
	scan       func(context.Context) observedRound
}

type mailSourceUpdate struct {
	index    int
	started  time.Time
	finished time.Time
	result   observedRound
}

type mailSourceState struct {
	observed bool
	update   mailSourceUpdate
}

// startMailObservedLoops gives claim-projection recovery and mailbox
// maintenance independent timers and scan goroutines. A small aggregator owns
// the single MAIL readiness handle; a slow source cannot block the other one.
func startMailObservedLoops(ctx context.Context, registry *workerreadiness.Registry, metrics *workerreadiness.Metrics,
	sources []mailObservedSource) <-chan struct{} {
	started := make(chan struct{})
	go runMailObservedLoops(ctx, registry, metrics, sources, started)
	return started
}

func runMailObservedLoops(ctx context.Context, registry *workerreadiness.Registry, metrics *workerreadiness.Metrics,
	sources []mailObservedSource, started chan<- struct{}) {
	handle, err := registry.StartLoop(workerreadiness.FamilyMail)
	if err != nil {
		log.Printf("start MAIL recovery lifecycle: %v", err)
		close(started)
		return
	}
	defer func() {
		class := "LOOP_EXITED"
		if recovered := recover(); recovered != nil {
			class = "LOOP_PANIC"
			log.Printf("farm-worker MAIL readiness aggregator panic")
		}
		handle.Stop(class)
	}()
	if len(sources) == 0 {
		close(started)
		return
	}

	sourceCtx, stopSources := context.WithCancel(ctx)
	defer stopSources()
	updates := make(chan mailSourceUpdate, len(sources)*2)
	for index := range sources {
		source := normalizeMailSource(sources[index])
		sources[index] = source
		go runMailObservedSource(sourceCtx, index, source, updates)
	}
	close(started)

	states := make([]mailSourceState, len(sources))
	for {
		select {
		case <-ctx.Done():
			return
		case update := <-updates:
			states[update.index] = mailSourceState{observed: true, update: update}
			handle.ScanStarted(update.started)
			observedAt := time.Now()
			if metrics != nil {
				metrics.ObserveScan(workerreadiness.FamilyMail, update.finished.Sub(update.started),
					update.result.stats[workerreadiness.FamilyMail])
				for _, latency := range update.result.terminalLatencies[workerreadiness.FamilyMail] {
					metrics.ObserveTerminalLatency(workerreadiness.FamilyMail, latency)
				}
			}
			due, aggregateErr := aggregateMailSources(sources, states, observedAt)
			if aggregateErr != nil {
				handle.ScanFailed(observedAt, readinessErrorClass(aggregateErr))
			} else {
				handle.ScanSucceeded(observedAt, due)
			}
		}
	}
}

func normalizeMailSource(source mailObservedSource) mailObservedSource {
	if source.interval <= 0 {
		source.interval = 250 * time.Millisecond
	}
	if source.staleAfter <= 0 {
		source.staleAfter = 3 * source.interval
		if source.staleAfter < 5*time.Second {
			source.staleAfter = 5 * time.Second
		}
	}
	return source
}

func runMailObservedSource(ctx context.Context, index int, source mailObservedSource, updates chan<- mailSourceUpdate) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			started := time.Now()
			result, panicked := invokeMailSource(ctx, source)
			update := mailSourceUpdate{index: index, started: started, finished: time.Now(), result: result}
			select {
			case updates <- update:
			case <-ctx.Done():
				return
			}
			if panicked {
				return
			}
			timer.Reset(source.interval)
		}
	}
}

func invokeMailSource(ctx context.Context, source mailObservedSource) (result observedRound, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			result = observedRound{err: fmt.Errorf("MAIL source %s panic: %v", source.name, recovered)}
			log.Printf("farm-worker MAIL source panic source=%s", source.name)
		}
	}()
	if source.scan == nil {
		return observedRound{err: fmt.Errorf("MAIL source %s has no scan", source.name)}, false
	}
	return source.scan(ctx), false
}

func aggregateMailSources(sources []mailObservedSource, states []mailSourceState, now time.Time) (workerreadiness.DueObservation, error) {
	observations := make([]workerreadiness.DueObservation, 0, len(sources))
	var errs []error
	for index, source := range sources {
		state := states[index]
		if !state.observed {
			errs = append(errs, fmt.Errorf("MAIL source %s has not completed", source.name))
			continue
		}
		if state.update.result.err != nil {
			errs = append(errs, fmt.Errorf("MAIL source %s: %w", source.name, state.update.result.err))
			continue
		}
		if now.Sub(state.update.finished) > source.staleAfter {
			errs = append(errs, fmt.Errorf("MAIL source %s observation is stale", source.name))
			continue
		}
		observation, ok := state.update.result.due[workerreadiness.FamilyMail]
		if !ok {
			errs = append(errs, fmt.Errorf("MAIL source %s omitted due observation", source.name))
			continue
		}
		observations = append(observations, observation)
	}
	if err := errors.Join(errs...); err != nil {
		return workerreadiness.DueObservation{}, err
	}
	return mergeDue(observations...), nil
}
