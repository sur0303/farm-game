package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/testkit/capdriver"
)

const harnessCorrectnessProtocolViolation = "harness_correctness_protocol_violation"

type runAccumulator struct {
	mu                 sync.Mutex
	planned            int64
	launched           int64
	unsent             int64
	completed          int64
	rejected           int64
	timeout            int64
	failed             int64
	requestBytes       int64
	responseBytes      int64
	correctness        map[string]int64
	allowedCorrectness map[string]bool
	arrivalLag         *boundedHistogram
	latency            *boundedHistogram
	// intervalLatency holds only the operations that completed since the last
	// sample. The cumulative histogram is too slow to show a tier that
	// deteriorates late in its hold.
	intervalLatency *boundedHistogram
}

func newRunAccumulator(requiredGates ...[]string) *runAccumulator {
	allowed := make(map[string]bool)
	if len(requiredGates) > 0 {
		for _, name := range requiredGates[0] {
			allowed[name] = true
		}
	}
	return &runAccumulator{correctness: make(map[string]int64), allowedCorrectness: allowed, arrivalLag: newMillisecondsHistogram(), latency: newMillisecondsHistogram(), intervalLatency: newMillisecondsHistogram()}
}

func (a *runAccumulator) addPlanned() { a.mu.Lock(); a.planned++; a.mu.Unlock() }
func (a *runAccumulator) addUnsent(lagMS float64) {
	a.arrivalLag.observe(lagMS)
	a.mu.Lock()
	a.unsent++
	a.mu.Unlock()
}
func (a *runAccumulator) addResult(result operationResult) bool {
	lag := result.LaunchedAt.Sub(result.ScheduledAt).Seconds() * 1000
	latency := result.CompletedAt.Sub(result.LaunchedAt).Seconds() * 1000
	a.arrivalLag.observe(math.Max(0, lag))
	a.latency.observe(math.Max(0, latency))
	a.intervalLatency.observe(math.Max(0, latency))
	correctnessSignal := false
	a.mu.Lock()
	defer a.mu.Unlock()
	a.launched++
	a.requestBytes += int64(result.RequestBytes)
	a.responseBytes += int64(result.ResponseBytes)
	switch result.Status {
	case "COMPLETED":
		a.completed++
	case "REJECTED":
		a.rejected++
	case "TIMEOUT":
		a.timeout++
	default:
		a.failed++
	}
	for name, value := range result.Correctness {
		if !a.allowedCorrectness[name] || value < 0 {
			correctnessSignal = true
			a.correctness[harnessCorrectnessProtocolViolation] = 1
			continue
		}
		if value > a.correctness[name] {
			a.correctness[name] = value
		}
		if value > 0 {
			correctnessSignal = true
		}
	}
	return correctnessSignal
}

func (a *runAccumulator) snapshot() plannedActual {
	a.mu.Lock()
	defer a.mu.Unlock()
	ratio := 0.0
	if a.planned > 0 {
		ratio = float64(a.launched) / float64(a.planned)
	}
	return plannedActual{
		Planned: a.planned, Launched: a.launched, ScheduledUnsent: a.unsent,
		Completed: a.completed, Rejected: a.rejected, Timeout: a.timeout, Failed: a.failed,
		ActualToPlannedRatio: ratio, ArrivalLagMS: a.arrivalLag.quantiles(), LatencyMS: a.latency.quantiles(),
		RequestBytes: a.requestBytes, ResponseBytes: a.responseBytes,
	}
}

// takeLatencyInterval reports the p99 and count of the operations that completed
// since the previous sample asked.
func (a *runAccumulator) takeLatencyInterval() (float64, int64) {
	return a.intervalLatency.takeInterval()
}

func (a *runAccumulator) correctnessSnapshot() map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int64, len(a.correctness))
	for key, value := range a.correctness {
		out[key] = value
	}
	return out
}

type scheduler struct {
	plan        runPlan
	client      *http.Client
	accumulator *runAccumulator
	correctness atomic.Bool
}

func newScheduler(plan runPlan, requiredGates ...[]string) *scheduler {
	transport := &http.Transport{MaxIdleConns: plan.MaximumInflight, MaxIdleConnsPerHost: plan.MaximumInflight, MaxConnsPerHost: plan.MaximumInflight, IdleConnTimeout: 30 * time.Second, ForceAttemptHTTP2: false}
	return &scheduler{
		plan:        plan,
		client:      &http.Client{Timeout: time.Duration(plan.RequestTimeoutMS) * time.Millisecond, Transport: transport},
		accumulator: newRunAccumulator(requiredGates...),
	}
}

func (s *scheduler) run(parent context.Context) (*runAccumulator, error) {
	rate, err := scheduleRate(s.plan.Targets)
	if err != nil {
		return nil, err
	}
	arrivalCtx, stopArrivals := context.WithCancel(parent)
	defer stopArrivals()
	operationCtx, stopOperations := context.WithCancel(context.Background())
	defer stopOperations()
	start := time.Now().Add(250 * time.Millisecond)
	end := start.Add(time.Duration(s.plan.DurationSeconds) * time.Second)
	semaphore := make(chan struct{}, s.plan.MaximumInflight)
	var workers sync.WaitGroup
	var sequence uint64
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-arrivalCtx.Done():
			workers.Wait()
			if s.correctness.Load() {
				return s.accumulator, nil
			}
			return s.accumulator, parent.Err()
		case now := <-ticker.C:
			if now.Before(start) {
				continue
			}
			elapsed := now.Sub(start)
			should := plannedBy(rate, time.Duration(s.plan.RampSeconds)*time.Second, elapsed)
			for sequence < should {
				sequence++
				s.accumulator.addPlanned()
				scheduled := start.Add(scheduleOffset(rate, time.Duration(s.plan.RampSeconds)*time.Second, sequence))
				lag := now.Sub(scheduled)
				if lag > time.Duration(s.plan.MaximumLaunchLagMS)*time.Millisecond {
					s.accumulator.addUnsent(lag.Seconds() * 1000)
					continue
				}
				select {
				case semaphore <- struct{}{}:
					workers.Add(1)
					go func(seq uint64, planned time.Time) {
						defer workers.Done()
						defer func() { <-semaphore }()
						result := s.launch(operationCtx, seq, planned)
						if s.accumulator.addResult(result) {
							s.correctness.Store(true)
							stopArrivals()
						}
					}(sequence, scheduled)
				default:
					s.accumulator.addUnsent(lag.Seconds() * 1000)
				}
			}
			if !now.Before(end) {
				workers.Wait()
				return s.accumulator, nil
			}
		}
	}
}

func plannedBy(rate float64, ramp time.Duration, elapsed time.Duration) uint64 {
	if elapsed <= 0 || rate <= 0 {
		return 0
	}
	seconds := elapsed.Seconds()
	if ramp <= 0 {
		return uint64(math.Floor(rate * seconds))
	}
	rampSeconds := ramp.Seconds()
	if seconds < rampSeconds {
		return uint64(math.Floor(rate * seconds * seconds / (2 * rampSeconds)))
	}
	return uint64(math.Floor(rate*rampSeconds/2 + rate*(seconds-rampSeconds)))
}

func scheduleOffset(rate float64, ramp time.Duration, sequence uint64) time.Duration {
	if sequence == 0 || rate <= 0 {
		return 0
	}
	n := float64(sequence)
	if ramp <= 0 {
		return time.Duration(n / rate * float64(time.Second))
	}
	rampSeconds := ramp.Seconds()
	rampCount := rate * rampSeconds / 2
	seconds := 0.0
	if n <= rampCount {
		seconds = math.Sqrt(2 * n * rampSeconds / rate)
	} else {
		seconds = rampSeconds + (n-rampCount)/rate
	}
	return time.Duration(seconds * float64(time.Second))
}

func (s *scheduler) launch(ctx context.Context, sequence uint64, scheduled time.Time) operationResult {
	launched := time.Now()
	requestValue := operationRequest{
		SchemaVersion: capdriver.OperationSchema, Sequence: sequence,
		ScheduledAt: scheduled.Format(time.RFC3339Nano),
		Service:     capdriver.Service(s.plan.Service), Profile: s.plan.Profile, Phase: s.plan.Phase,
	}
	body, _ := json.Marshal(requestValue)
	result := operationResult{Sequence: sequence, ScheduledAt: scheduled, LaunchedAt: launched, Status: "FAILED", ErrorCode: "DRIVER_REQUEST_FAILED", RequestBytes: len(body)}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.plan.DriverURL, bytes.NewReader(body))
	if err != nil {
		result.CompletedAt = time.Now()
		return result
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		result.CompletedAt = time.Now()
		if ctx.Err() == nil {
			result.Status = "TIMEOUT"
			result.ErrorCode = "DRIVER_TIMEOUT"
		}
		return result
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	result.ResponseBytes = len(raw)
	result.CompletedAt = time.Now()
	if readErr != nil || response.StatusCode != http.StatusOK {
		result.ErrorCode = "DRIVER_HTTP_ERROR"
		return result
	}
	var decoded operationResponse
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil || decoded.Sequence != sequence ||
		decoded.SchemaVersion != capdriver.OperationSchema {
		result.ErrorCode = "DRIVER_PROTOCOL_ERROR"
		return result
	}
	if !allowedCode(s.plan, decoded.ErrorClass) {
		result.ErrorCode = "UNALLOWLISTED_DRIVER_ERROR"
		return result
	}
	switch decoded.Status {
	case capdriver.StatusCompleted, capdriver.StatusRejected,
		capdriver.StatusTimeout, capdriver.StatusFailed:
		result.Status = string(decoded.Status)
	default:
		result.ErrorCode = "DRIVER_PROTOCOL_ERROR"
		return result
	}
	result.ErrorCode = decoded.ErrorClass
	result.QueueItems = float64(decoded.QueueDepth)
	result.Correctness = correctnessCounts(decoded.Correctness)
	return result
}

// correctnessCounts widens the driver's gate counters into the accumulator's
// width. A nil map stays nil so a driver that reports no gates is not mistaken
// for one that reported an empty observation.
func correctnessCounts(gates map[string]int) map[string]int64 {
	if gates == nil {
		return nil
	}
	counts := make(map[string]int64, len(gates))
	for name, value := range gates {
		counts[name] = int64(value)
	}
	return counts
}

func (s *scheduler) debugSummary() string {
	value := s.accumulator.snapshot()
	return fmt.Sprintf("planned=%d launched=%d unsent=%d completed=%d", value.Planned, value.Launched, value.ScheduledUnsent, value.Completed)
}
