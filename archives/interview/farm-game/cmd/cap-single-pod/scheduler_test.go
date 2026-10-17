package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/testkit/capdriver"
)

func schedulerPlan(endpoint string) runPlan {
	return runPlan{Service: "gate", Profile: "smoke", Phase: "SMOKE", DurationSeconds: 1, RampSeconds: 0,
		Targets: []target{{Kind: "arrivals_per_second", Value: 50}}, MaximumLaunchLagMS: 50, MaximumInflight: 32,
		DriverURL: endpoint, RequestTimeoutMS: 500, AllowedErrorCodes: []string{"EXPECTED_REJECT"}}
}

func TestSchedulerOpenLoopRecordsPlannedAndActual(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request operationRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			t.Error("decode")
		}
		_ = json.NewEncoder(w).Encode(operationResponse{
			SchemaVersion: capdriver.OperationSchema, Sequence: request.Sequence,
			Status: capdriver.StatusCompleted,
		})
	}))
	defer server.Close()
	s := newScheduler(schedulerPlan(server.URL))
	accumulator, err := s.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := accumulator.snapshot()
	if got.Planned < 45 || got.Planned > 51 || got.Launched != got.Planned || got.Completed != got.Launched || got.ScheduledUnsent != 0 {
		t.Fatalf("actual=%+v", got)
	}
	if got.ActualToPlannedRatio != 1 {
		t.Fatalf("ratio=%f", got.ActualToPlannedRatio)
	}
}

func TestSchedulerCorrectnessSignalStopsNewArrivals(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request operationRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(operationResponse{
			SchemaVersion: capdriver.OperationSchema, Sequence: request.Sequence,
			Status: capdriver.StatusCompleted, Correctness: map[string]int{"duplicate_grant_total": 1},
		})
	}))
	defer server.Close()
	plan := schedulerPlan(server.URL)
	plan.DurationSeconds = 3
	plan.Targets = []target{{Kind: "arrivals_per_second", Value: 100}}
	s := newScheduler(plan, []string{"duplicate_grant_total"})
	accumulator, err := s.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := accumulator.snapshot()
	if !s.correctness.Load() || accumulator.correctnessSnapshot()["duplicate_grant_total"] == 0 {
		t.Fatal("correctness signal missing")
	}
	if got.Planned >= 200 || calls.Load() >= 200 {
		t.Fatalf("arrivals did not stop: %+v calls=%d", got, calls.Load())
	}
}

func TestUnknownGateRejected(t *testing.T) {
	acc := newRunAccumulator([]string{"duplicate_grant_total"})
	if !acc.addResult(operationResult{ScheduledAt: time.Now(), LaunchedAt: time.Now(), CompletedAt: time.Now(), Status: "COMPLETED", Correctness: map[string]int64{"unknown_gate": 1}}) {
		t.Fatal("unknown gate not signaled")
	}
	if acc.correctnessSnapshot()[harnessCorrectnessProtocolViolation] != 1 {
		t.Fatal("protocol violation not latched")
	}
}
func TestNegativeCorrectnessValueRejected(t *testing.T) {
	acc := newRunAccumulator([]string{"duplicate_grant_total"})
	if !acc.addResult(operationResult{ScheduledAt: time.Now(), LaunchedAt: time.Now(), CompletedAt: time.Now(), Status: "COMPLETED", Correctness: map[string]int64{"duplicate_grant_total": -1}}) {
		t.Fatal("negative value not signaled")
	}
	if acc.correctnessSnapshot()[harnessCorrectnessProtocolViolation] != 1 {
		t.Fatal("protocol violation not latched")
	}
}
func TestCorrectnessViolationIsLatched(t *testing.T) {
	acc := newRunAccumulator([]string{"duplicate_grant_total"})
	now := time.Now()
	acc.addResult(operationResult{ScheduledAt: now, LaunchedAt: now, CompletedAt: now, Status: "COMPLETED", Correctness: map[string]int64{"duplicate_grant_total": 1}})
	acc.addResult(operationResult{ScheduledAt: now, LaunchedAt: now, CompletedAt: now, Status: "COMPLETED", Correctness: map[string]int64{"duplicate_grant_total": 0}})
	if acc.correctnessSnapshot()["duplicate_grant_total"] != 1 {
		t.Fatalf("gates=%v", acc.correctnessSnapshot())
	}
}

func TestPlannedByAndScheduleOffsetRamp(t *testing.T) {
	ramp := 2 * time.Second
	rate := 100.0
	if got := plannedBy(rate, ramp, time.Second); got != 25 {
		t.Fatalf("planned=%d", got)
	}
	if got := plannedBy(rate, ramp, 2*time.Second); got != 100 {
		t.Fatalf("planned=%d", got)
	}
	for sequence := uint64(1); sequence <= 200; sequence++ {
		offset := scheduleOffset(rate, ramp, sequence)
		if offset < 0 {
			t.Fatal("negative offset")
		}
	}
}

func TestHistogramBoundedQuantiles(t *testing.T) {
	h := newMillisecondsHistogram()
	for i := 1; i <= 100; i++ {
		h.observe(float64(i))
	}
	q := h.quantiles()
	if q["p50"] < 50 || q["p99"] < 99 || q["max"] != 100 {
		t.Fatalf("q=%v", q)
	}
}
