package main

import (
	"context"
	"fmt"
	"sync/atomic"
)

type measuredOperation func(context.Context, int) (sampleResult, error)

type failedOperation struct {
	index  int
	sample sampleResult
	err    error
}

type friendSnapshotExpectation struct {
	scriptSHA     string
	expectedCalls uint64
	observedCalls func() uint64
}

type preparedScenario struct {
	operations       []measuredOperation
	keyDistribution  string
	notes            []string
	observeDue       func(context.Context) (expected, observed int64, err error)
	diagnoseFailures func(context.Context, []failedOperation) ([]failureDiagnostic, error)
	validate         func(context.Context) error
	friendSnapshot   *friendSnapshotExpectation
}

type scenarioDefinition struct {
	name    string
	group   string
	prepare func(context.Context, *benchmarkEnvironment, string, int, int) (*preparedScenario, error)
}

var sequence atomic.Uint64

func nextID() uint64 { return sequence.Add(1) }

func payloadFor(index int) []byte {
	// Deterministic four-point distribution: observed quantiles are preserved
	// in every cell rather than inferred from one average object.
	position := index % 100
	size := 256
	switch {
	case position >= 99:
		size = 16 * 1024
	case position >= 95:
		size = 4 * 1024
	case position >= 50:
		size = 1024
	}
	out := make([]byte, size)
	for i := range out {
		out[i] = byte((index*31+i*17)%251 + 1)
	}
	return out
}

func splitOperations(total, workers int, build func(worker, ordinal int) measuredOperation) []measuredOperation {
	if workers <= 0 {
		workers = 1
	}
	out := make([]measuredOperation, total)
	ordinals := make([]int, workers)
	for i := 0; i < total; i++ {
		worker := i % workers
		out[i] = build(worker, ordinals[worker])
		ordinals[worker]++
	}
	return out
}

func requireOutcome(name string, got, want fmt.Stringer) error {
	if got.String() != want.String() {
		return fmt.Errorf("%s outcome=%s want=%s", name, got.String(), want.String())
	}
	return nil
}
