package readpath

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// HeaderReadMetrics is deliberately aggregate-only. It never labels by farm,
// player, shard, or pod target, so enabling the experiment cannot introduce a
// high-cardinality metrics path.
type HeaderReadMetrics struct {
	fullCalls         atomic.Uint64
	shadowCalls       atomic.Uint64
	narrowCalls       atomic.Uint64
	farms             atomic.Uint64
	narrowUnknown     atomic.Uint64
	fullFallback      atomic.Uint64
	mismatches        atomic.Uint64
	narrowUnavailable atomic.Uint64
}

func (m *HeaderReadMetrics) ObserveHeaderRead(value HeaderReadObservation) {
	if m == nil {
		return
	}
	switch value.Mode {
	case HeaderReadNarrowShadow:
		m.shadowCalls.Add(1)
	case HeaderReadNarrow:
		m.narrowCalls.Add(1)
	default:
		m.fullCalls.Add(1)
	}
	if value.FarmCount > 0 {
		m.farms.Add(uint64(value.FarmCount))
	}
	if value.NarrowUnknown > 0 {
		m.narrowUnknown.Add(uint64(value.NarrowUnknown))
	}
	if value.FullFallbackCount > 0 {
		m.fullFallback.Add(uint64(value.FullFallbackCount))
	}
	if value.MismatchCount > 0 {
		m.mismatches.Add(uint64(value.MismatchCount))
	}
	if value.NarrowUnavailable {
		m.narrowUnavailable.Add(1)
	}
}

func RegisterHeaderReadMetrics(reg prometheus.Registerer, metrics *HeaderReadMetrics) {
	if reg == nil || metrics == nil {
		return
	}
	reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "farm_opportunity_header_read_calls_total", Help: "Header coarse-read calls by implementation mode.",
			ConstLabels: prometheus.Labels{"mode": string(HeaderReadFull)},
		}, func() float64 { return float64(metrics.fullCalls.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "farm_opportunity_header_read_calls_total", Help: "Header coarse-read calls by implementation mode.",
			ConstLabels: prometheus.Labels{"mode": string(HeaderReadNarrowShadow)},
		}, func() float64 { return float64(metrics.shadowCalls.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "farm_opportunity_header_read_calls_total", Help: "Header coarse-read calls by implementation mode.",
			ConstLabels: prometheus.Labels{"mode": string(HeaderReadNarrow)},
		}, func() float64 { return float64(metrics.narrowCalls.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_opportunity_header_read_farms_total", Help: "Farm headers inspected across all coarse-read modes."}, func() float64 {
			return float64(metrics.farms.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_opportunity_header_read_unknown_total", Help: "Narrow header rows that remained UNKNOWN before bounded fallback."}, func() float64 {
			return float64(metrics.narrowUnknown.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_opportunity_header_read_full_fallback_total", Help: "Farm rows retried through the complete Authority read path."}, func() float64 {
			return float64(metrics.fullFallback.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_opportunity_header_read_shadow_mismatch_total", Help: "Shadow comparisons where narrow and complete Header outcomes differed."}, func() float64 {
			return float64(metrics.mismatches.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_opportunity_header_read_narrow_unavailable_total", Help: "Narrow Header batch reads that failed before row decoding."}, func() float64 {
			return float64(metrics.narrowUnavailable.Load())
		}),
	)
}

func RegisterHeaderReadModeMetrics(reg prometheus.Registerer, provider *Provider) {
	if reg == nil || provider == nil {
		return
	}
	for _, mode := range []HeaderReadMode{
		HeaderReadFull,
		HeaderReadNarrowShadow,
		HeaderReadNarrow,
	} {
		mode := mode
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "farm_opportunity_header_read_mode",
			Help: "Current opportunity Header read mode; exactly one mode is 1.",
			ConstLabels: prometheus.Labels{
				"mode": string(mode),
			},
		}, func() float64 {
			if provider.HeaderReadMode() == mode {
				return 1
			}
			return 0
		}))
	}
}
