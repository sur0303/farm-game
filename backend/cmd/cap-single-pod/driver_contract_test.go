package main

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"farmgame/internal/testkit/capdriver"
)

type countingAdapter struct{ served atomic.Int64 }

func (a *countingAdapter) Execute(ctx context.Context, request capdriver.OperationRequest) capdriver.Result {
	a.served.Add(1)
	return capdriver.Result{Status: capdriver.StatusCompleted, Correctness: map[string]int{}}
}

// The harness and the driver are developed as separate binaries against the
// same versioned schema, and both reject unknown fields. Every earlier test
// paired one of them with a hand-written stub of the other, which let the two
// real implementations disagree on field names, on the schedule encoding and
// on the phase vocabulary without any test noticing. This one runs the real
// scheduler against the real driver.
func TestSchedulerDrivesRealDriver(t *testing.T) {
	for _, phase := range capdriver.Phases() {
		t.Run(phase, func(t *testing.T) {
			adapter := &countingAdapter{}
			driver, err := capdriver.NewServer(capdriver.Config{
				BinarySHA256: driverContractSHA, ConfigSHA256: driverContractSHA,
				Service: capdriver.ServiceGate, Profile: "smoke", MaximumInflight: 32, Ready: true,
			}, adapter)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(driver.Handler())
			defer server.Close()

			accumulator, err := newScheduler(driverContractPlan(server.URL+"/operation", phase)).run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got := accumulator.snapshot()
			if got.Completed == 0 || got.Completed != got.Launched {
				t.Fatalf("driver rejected the harness: %+v", got)
			}
			if adapter.served.Load() != int64(got.Completed) {
				t.Fatalf("adapter served %d of %d completions", adapter.served.Load(), got.Completed)
			}
		})
	}
}

func driverContractPlan(endpoint string, phase string) runPlan {
	plan := schedulerPlan(endpoint)
	plan.Phase = phase
	return plan
}

const driverContractSHA = "0000000000000000000000000000000000000000000000000000000000000000"
