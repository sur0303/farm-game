package selfprobe

import (
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestProbeReportsWorkItObserved(t *testing.T) {
	probe := Start(20 * time.Millisecond)

	// Burn real CPU on every processor so the sample has something to find.
	// Without load the report would be indistinguishable from a probe that
	// silently measures nothing.
	var waiting sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < runtime.GOMAXPROCS(0); worker++ {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	waiting.Wait()

	report := probe.Stop()
	if report.Samples < 2 {
		t.Fatalf("samples = %d, want the probe to have ticked", report.Samples)
	}
	if report.Procs != runtime.GOMAXPROCS(0) {
		t.Fatalf("procs = %d", report.Procs)
	}
	if report.GoroutinesMax < 2 {
		t.Fatalf("goroutines max = %d", report.GoroutinesMax)
	}
	if report.CPUPercentMax <= 0 {
		t.Fatalf("cpu percent max = %v after a busy window", report.CPUPercentMax)
	}
	if report.RSSBytesMax == 0 && runtime.GOOS == "linux" {
		t.Fatal("resident set was not read on linux")
	}
	if got := probe.Samples(); len(got) != report.Samples {
		t.Fatalf("samples = %d, report said %d", len(got), report.Samples)
	}
	// Stopping twice must stay safe: the caller stops on the run's exit path
	// and again from a deferred cleanup.
	if second := probe.Stop(); second.Samples != report.Samples {
		t.Fatalf("second stop = %+v", second)
	}
}

func TestPercentileReportsTheBucketHoldingTheQuantile(t *testing.T) {
	buckets := []float64{0, 0.001, 0.010, 0.100, math.Inf(1)}
	counts := []uint64{90, 9, 1, 0}

	if got := percentileMs(counts, buckets, 0.50); got != 1 {
		t.Fatalf("p50 = %v ms, want the 1ms bucket", got)
	}
	if got := percentileMs(counts, buckets, 0.99); got != 10 {
		t.Fatalf("p99 = %v ms, want the 10ms bucket", got)
	}
	if got := percentileMs(nil, buckets, 0.99); got != 0 {
		t.Fatalf("empty histogram = %v", got)
	}
	// The overflow bucket has no upper edge to report. It must still report
	// the lower one: zero here would turn the worst observation the runtime
	// made into the best number the gate ever sees.
	if got := percentileMs([]uint64{0, 0, 0, 5}, buckets, 0.99); got != 100 {
		t.Fatalf("overflow bucket = %v ms, want its 100ms lower edge", got)
	}
}

func TestCountsAreDifferencedAgainstThePreviousRead(t *testing.T) {
	// The runtime histogram is cumulative, so a per-interval percentile is
	// only meaningful after subtracting the previous read.
	got := subtractCounts([]uint64{10, 20, 30}, []uint64{4, 20, 0})
	if len(got) != 3 || got[0] != 6 || got[1] != 0 || got[2] != 30 {
		t.Fatalf("delta = %v", got)
	}
	// A shape change means there is no comparable previous read.
	if got := subtractCounts([]uint64{1, 2}, []uint64{1}); len(got) != 2 || got[0] != 1 {
		t.Fatalf("mismatched shape = %v", got)
	}
	if got := subtractCounts(nil, nil); got != nil {
		t.Fatalf("empty read = %v", got)
	}
}
