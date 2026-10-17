package scaleplan_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	core "farmgame/internal/scaleplan"
)

// TestScalePlan1024ShardParallelProfilesPressure exercises the complete parent
// ScalePlan wave/checkpoint state machine with the frozen 1024-Shard manifest.
// Child RoutePlans are represented by the deterministic test adapter, so this
// measures controller orchestration/CAS overhead rather than network or Holder
// drain latency. The real kind acceptance test covers those external paths.
func TestScalePlan1024ShardParallelProfilesPressure(t *testing.T) {
	if os.Getenv("SCALEPLAN_RUN_1024_PRESSURE") != "1" {
		t.Skip("set SCALEPLAN_RUN_1024_PRESSURE=1 to run the 1024-Shard pressure profile")
	}
	for _, parallelism := range []uint32{1, 8, 16, 32} {
		t.Run(fmt.Sprintf("parallelism_%d", parallelism), func(t *testing.T) {
			const moves = 1024
			const rounds = 1

			var total time.Duration
			for round := 0; round < rounds; round++ {
				f := newParallelScaleOutFixture(t, moves)
				limits, err := core.FunctionalLimitsForParallelism(parallelism)
				if err != nil {
					t.Fatal(err)
				}
				f.submission.IdempotencyKey = fmt.Sprintf("pressure-%d-%d", parallelism, round)
				f.submission.Limits = limits

				started := time.Now()
				record, err := f.executor.Submit(context.Background(), f.submission)
				if err != nil {
					t.Fatal(err)
				}
				executeCalls := 0
				for step := 0; step < moves*8; step++ {
					record, err = f.executor.Execute(context.Background(), record.Plan.PlanID)
					executeCalls++
					if err == nil && record.Plan.Status == core.StatusSucceeded {
						break
					}
					if !errors.Is(err, core.ErrPending) {
						t.Fatalf("round=%d step=%d status=%s phase=%s err=%v", round, step, record.Plan.Status, record.Plan.Phase, err)
					}
					for _, move := range record.Plan.ResolvedMoves {
						f.routePlans.SetView(move.ChildPlanID, core.RoutePlanPhaseSucceeded)
					}
				}
				elapsed := time.Since(started)
				total += elapsed
				if record.Plan.Status != core.StatusSucceeded || len(record.Plan.ResolvedMoves) != moves {
					t.Fatalf("round=%d status=%s phase=%s resolved=%d", round, record.Plan.Status, record.Plan.Phase, len(record.Plan.ResolvedMoves))
				}
				wantWaves := (moves + int(parallelism) - 1) / int(parallelism)
				wantWaveIndex := uint32(moves)
				if limits.LegacySerial() {
					// The 1/1 profile intentionally preserves the historical inclusive
					// terminal checkpoint; parallel profiles use an exclusive cursor.
					wantWaveIndex = uint32(moves - 1)
				}
				if record.Plan.WaveIndex != wantWaveIndex || len(f.routePlans.Requests()) != moves {
					t.Fatalf("round=%d wave_index=%d children=%d want=%d", round, record.Plan.WaveIndex, len(f.routePlans.Requests()), moves)
				}
				t.Logf("round=%d parallelism=%d shards=%d waves=%d execute_calls=%d duration=%s throughput=%.2f_shards_per_sec",
					round+1, parallelism, moves, wantWaves, executeCalls, elapsed, float64(moves)/elapsed.Seconds())
			}
			average := total / rounds
			t.Logf("SUMMARY parallelism=%d shards=1024 rounds=%d average=%s throughput=%.2f_shards_per_sec",
				parallelism, rounds, average, float64(1024)/average.Seconds())
		})
	}
}
