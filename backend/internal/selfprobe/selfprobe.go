// Package selfprobe samples the health of the process it runs in.
//
// A load generator reports latency, and latency is only attributable to the
// system under test while the generator itself has headroom. Once the driver
// runs out of CPU or its own goroutines start queueing behind each other,
// every measurement it takes includes that delay, and a saturated driver
// reads as a calm server. This package is what makes that visible: it records
// what the process cost, and how long its own goroutines waited to be
// scheduled, alongside the latency the process reported.
package selfprobe

import (
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

// schedLatencyMetric is the runtime's own distribution of the time a
// goroutine spent runnable before it got a processor. It is the most direct
// evidence that a process has more concurrency than it can serve.
const schedLatencyMetric = "/sched/latencies:seconds"

// DefaultInterval keeps a run of a minute or more above the sample count an
// evidence review expects without making the probe itself a cost.
const DefaultInterval = time.Second

// maxSamples bounds retention so a long run cannot grow the probe without
// limit. At DefaultInterval this is an hour of history.
const maxSamples = 3600

// Sample is one observation of the running process.
type Sample struct {
	At         time.Time `json:"at"`
	Goroutines int       `json:"goroutines"`
	HeapBytes  uint64    `json:"heap_bytes"`
	RSSBytes   uint64    `json:"rss_bytes"`
	OpenFiles  int       `json:"open_files"`
	// CPUPercent is the CPU time consumed since the previous sample as a
	// share of what was available in that window, so 100 means every
	// GOMAXPROCS processor was busy for the whole interval.
	CPUPercent float64 `json:"cpu_percent"`
	// SchedLatency percentiles cover only the interval since the previous
	// sample, so a late spike is not diluted by a healthy start.
	SchedLatencyP50Ms float64 `json:"sched_latency_p50_ms"`
	SchedLatencyP99Ms float64 `json:"sched_latency_p99_ms"`
}

// Report is the bounded aggregate of everything one probe observed.
type Report struct {
	Samples  int           `json:"samples"`
	Procs    int           `json:"procs"`
	Duration time.Duration `json:"-"`

	GoroutinesMax int    `json:"goroutines_max"`
	HeapBytesMax  uint64 `json:"heap_bytes_max"`
	RSSBytesMax   uint64 `json:"rss_bytes_max"`
	OpenFilesMax  int    `json:"open_files_max"`

	CPUPercentMax float64 `json:"cpu_percent_max"`
	CPUPercentAvg float64 `json:"cpu_percent_avg"`

	SchedLatencyP99MsMax float64 `json:"sched_latency_p99_ms_max"`
	SchedLatencyP99MsAvg float64 `json:"sched_latency_p99_ms_avg"`
}

// Probe samples the current process on a fixed interval until it is stopped.
type Probe struct {
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}

	mu      sync.Mutex
	samples []Sample
	started time.Time

	// Reading a delta needs the previous cumulative counters, which only the
	// sampling goroutine touches.
	lastAt    time.Time
	lastCPU   time.Duration
	lastSched []uint64
	buckets   []float64
}

// Start begins sampling. A non-positive interval selects DefaultInterval.
func Start(interval time.Duration) *Probe {
	if interval <= 0 {
		interval = DefaultInterval
	}
	probe := &Probe{
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		started:  time.Now(),
		lastAt:   time.Now(),
		lastCPU:  processCPUTime(),
	}
	probe.lastSched, probe.buckets = readSchedLatency()
	go probe.loop()
	return probe
}

func (probe *Probe) loop() {
	defer close(probe.done)
	ticker := time.NewTicker(probe.interval)
	defer ticker.Stop()
	for {
		select {
		case <-probe.stop:
			return
		case <-ticker.C:
			probe.observe()
		}
	}
}

func (probe *Probe) observe() {
	now := time.Now()
	cpu := processCPUTime()
	counts, buckets := readSchedLatency()

	sample := Sample{
		At:         now,
		Goroutines: runtime.NumGoroutine(),
		HeapBytes:  heapBytes(),
		RSSBytes:   residentBytes(),
		OpenFiles:  openFiles(),
	}
	if elapsed := now.Sub(probe.lastAt); elapsed > 0 {
		available := float64(elapsed) * float64(runtime.GOMAXPROCS(0))
		sample.CPUPercent = float64(cpu-probe.lastCPU) / available * 100
	}
	if delta := subtractCounts(counts, probe.lastSched); delta != nil {
		sample.SchedLatencyP50Ms = percentileMs(delta, buckets, 0.50)
		sample.SchedLatencyP99Ms = percentileMs(delta, buckets, 0.99)
	}
	probe.lastAt, probe.lastCPU, probe.lastSched, probe.buckets = now, cpu, counts, buckets

	probe.mu.Lock()
	if len(probe.samples) < maxSamples {
		probe.samples = append(probe.samples, sample)
	}
	probe.mu.Unlock()
}

// Stop ends sampling and returns the aggregate. It is safe to call twice.
func (probe *Probe) Stop() Report {
	if probe == nil {
		return Report{}
	}
	select {
	case <-probe.stop:
	default:
		close(probe.stop)
	}
	<-probe.done
	return probe.Report()
}

// Report aggregates the samples taken so far.
func (probe *Probe) Report() Report {
	if probe == nil {
		return Report{}
	}
	probe.mu.Lock()
	samples := probe.samples
	started := probe.started
	probe.mu.Unlock()

	report := Report{Samples: len(samples), Procs: runtime.GOMAXPROCS(0), Duration: time.Since(started)}
	if len(samples) == 0 {
		return report
	}
	var cpuTotal, schedTotal float64
	for _, sample := range samples {
		if sample.Goroutines > report.GoroutinesMax {
			report.GoroutinesMax = sample.Goroutines
		}
		if sample.HeapBytes > report.HeapBytesMax {
			report.HeapBytesMax = sample.HeapBytes
		}
		if sample.RSSBytes > report.RSSBytesMax {
			report.RSSBytesMax = sample.RSSBytes
		}
		if sample.OpenFiles > report.OpenFilesMax {
			report.OpenFilesMax = sample.OpenFiles
		}
		if sample.CPUPercent > report.CPUPercentMax {
			report.CPUPercentMax = sample.CPUPercent
		}
		if sample.SchedLatencyP99Ms > report.SchedLatencyP99MsMax {
			report.SchedLatencyP99MsMax = sample.SchedLatencyP99Ms
		}
		cpuTotal += sample.CPUPercent
		schedTotal += sample.SchedLatencyP99Ms
	}
	report.CPUPercentAvg = cpuTotal / float64(len(samples))
	report.SchedLatencyP99MsAvg = schedTotal / float64(len(samples))
	return report
}

// Samples returns a copy of the retained observations.
func (probe *Probe) Samples() []Sample {
	if probe == nil {
		return nil
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return append([]Sample(nil), probe.samples...)
}

func heapBytes() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample[0].Value.Uint64()
}

// readSchedLatency returns the cumulative scheduling-latency histogram. The
// counts are copied because the runtime may reuse the returned buffers.
func readSchedLatency() ([]uint64, []float64) {
	sample := []metrics.Sample{{Name: schedLatencyMetric}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return nil, nil
	}
	histogram := sample[0].Value.Float64Histogram()
	if histogram == nil {
		return nil, nil
	}
	return append([]uint64(nil), histogram.Counts...), histogram.Buckets
}

func subtractCounts(current, previous []uint64) []uint64 {
	if len(current) == 0 {
		return nil
	}
	if len(previous) != len(current) {
		return current
	}
	delta := make([]uint64, len(current))
	for index := range current {
		if current[index] >= previous[index] {
			delta[index] = current[index] - previous[index]
		}
	}
	return delta
}

// percentileMs reports the bucket holding the quantile, in milliseconds. The
// boundaries are the runtime's own, so the value is the bucket's upper edge
// rather than an interpolation across a distribution nobody knows the shape
// of. The unbounded overflow bucket reports its lower edge instead: there is
// no upper edge to state, and reporting zero would let the worst observation
// pass the gate as if it were the best.
func percentileMs(counts []uint64, buckets []float64, quantile float64) float64 {
	if len(counts) == 0 || len(buckets) < 2 {
		return 0
	}
	var total uint64
	for _, count := range counts {
		total += count
	}
	if total == 0 {
		return 0
	}
	rank := quantile * float64(total)
	var cumulative float64
	for index, count := range counts {
		if count == 0 {
			continue
		}
		if cumulative+float64(count) < rank {
			cumulative += float64(count)
			continue
		}
		edge := math.Inf(1)
		if index+1 < len(buckets) {
			edge = buckets[index+1]
		}
		if math.IsInf(edge, 1) && index < len(buckets) {
			edge = buckets[index]
		}
		if math.IsInf(edge, 0) || math.IsNaN(edge) {
			return 0
		}
		return edge * 1000
	}
	return 0
}

// residentBytes reads the process resident set from /proc. Anywhere without
// procfs it reports zero rather than guessing.
func residentBytes() uint64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

// openFiles counts descriptors without stat-ing each one, because at the
// connection counts this probe is meant for, the counting must stay cheaper
// than what it measures.
func openFiles() int {
	directory, err := os.Open("/proc/self/fd")
	if err != nil {
		return 0
	}
	defer directory.Close()
	names, err := directory.Readdirnames(-1)
	if err != nil {
		return 0
	}
	// The open directory handle itself is one of the names.
	return len(names) - 1
}
