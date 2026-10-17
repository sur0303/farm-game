package bootlease

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func RegisterMetrics(reg prometheus.Registerer, lease *Lease) {
	if reg == nil || lease == nil {
		return
	}
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "boot_lease", Name: "trusted",
			Help: "1 while this exact GateBoot fence is within its last Redis-confirmed local trust deadline.",
		}, func() float64 {
			if lease.Trusted(time.Now()) {
				return 1
			}
			return 0
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "boot_lease", Name: "seconds_remaining",
			Help: "Seconds remaining before this GateBoot becomes untrusted; never negative.",
		}, func() float64 {
			remaining := time.Until(lease.TrustedUntil()).Seconds()
			if remaining < 0 {
				return 0
			}
			return remaining
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "boot_lease", Name: "renew_attempts_total",
			Help: "GateBoot lease authority renew attempts.",
		}, func() float64 { return float64(lease.Stats().RenewAttempts) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "boot_lease", Name: "renew_successes_total",
			Help: "Successful GateBoot lease renewals.",
		}, func() float64 { return float64(lease.Stats().RenewSuccesses) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "boot_lease", Name: "renew_failures_total",
			Help: "Failed GateBoot lease renewals; failures never extend local trust.",
		}, func() float64 { return float64(lease.Stats().RenewFailures) }),
	)
}
