package main

import (
	"math"
	"sort"
	"strconv"
	"sync"
)

type boundedHistogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []int64
	count  int64
	sum    float64
	max    float64
}

func newMillisecondsHistogram() *boundedHistogram {
	bounds := []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000, 120000}
	return &boundedHistogram{bounds: bounds, counts: make([]int64, len(bounds)+1)}
}

func (h *boundedHistogram) observe(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	index := sort.SearchFloat64s(h.bounds, value)
	h.counts[index]++
	h.count++
	h.sum += value
	if value > h.max {
		h.max = value
	}
}

func (h *boundedHistogram) snapshot() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	quantiles := make(map[string]float64)
	for name, q := range map[string]float64{"p50": .5, "p95": .95, "p99": .99, "max": 1} {
		quantiles[name] = h.quantileLocked(q)
	}
	buckets := make([]map[string]any, 0, len(h.counts))
	for index, count := range h.counts {
		upper := "+Inf"
		if index < len(h.bounds) {
			upper = formatFloat(h.bounds[index])
		}
		buckets = append(buckets, map[string]any{"upper_ms": upper, "count": count})
	}
	mean := 0.0
	if h.count > 0 {
		mean = h.sum / float64(h.count)
	}
	return map[string]any{"count": h.count, "mean_ms": mean, "quantiles_ms": quantiles, "buckets": buckets}
}

func (h *boundedHistogram) quantiles() map[string]float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return map[string]float64{"p50": h.quantileLocked(.5), "p95": h.quantileLocked(.95), "p99": h.quantileLocked(.99), "max": h.max}
}

// takeInterval reports the p99 and count of what has been observed since the
// previous call and starts a new interval.
func (h *boundedHistogram) takeInterval() (float64, int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p99, count := h.quantileLocked(.99), h.count
	h.counts = make([]int64, len(h.bounds)+1)
	h.count, h.sum, h.max = 0, 0, 0
	return p99, count
}

func (h *boundedHistogram) quantileLocked(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	if q >= 1 {
		return h.max
	}
	wanted := int64(math.Ceil(q * float64(h.count)))
	var cumulative int64
	for index, count := range h.counts {
		cumulative += count
		if cumulative >= wanted {
			if index < len(h.bounds) {
				return h.bounds[index]
			}
			return h.max
		}
	}
	return h.max
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
