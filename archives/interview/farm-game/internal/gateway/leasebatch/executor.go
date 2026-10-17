package leasebatch

import (
	"context"
	"time"
)

// Clock is intentionally tiny so formal simulations can advance dispatch time
// without sleeping and production can use RealClock.
type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// DispatchReport records the actual clock cost separately from planning. The
// context deadline is the hard cooperative I/O cutoff; Exceeded also catches a
// store that returned after ignoring cancellation.
type DispatchReport struct {
	Started  time.Time
	Finished time.Time
	Duration time.Duration
	Exceeded bool
}

func Dispatch(ctx context.Context, clock Clock, maxDuration time.Duration,
	dispatch func(context.Context) error) (DispatchReport, error) {
	if clock == nil {
		clock = RealClock{}
	}
	report := DispatchReport{Started: clock.Now()}
	dispatchCtx, cancel := context.WithTimeout(ctx, maxDuration)
	err := dispatch(dispatchCtx)
	cancel()
	report.Finished = clock.Now()
	report.Duration = report.Finished.Sub(report.Started)
	report.Exceeded = report.Duration >= maxDuration
	return report, err
}
