package workflow

import (
	"math/rand"
	"time"
)

// BackoffConfig bounds the retry cadence shared by the engine, the
// DirectDispatcher and the recovery loop. Values are configuration
// candidates (PHYSICAL_PENDING); they are not business rules.
type BackoffConfig struct {
	Base     time.Duration
	Max      time.Duration
	JitterPPM uint32 // jitter fraction in parts per million, e.g. 200_000 = 20%
}

func (c BackoffConfig) withDefaults() BackoffConfig {
	if c.Base <= 0 {
		c.Base = 250 * time.Millisecond
	}
	if c.Max <= 0 {
		c.Max = 30 * time.Second
	}
	if c.Max < c.Base {
		c.Max = c.Base
	}
	if c.JitterPPM > 1_000_000 {
		c.JitterPPM = 1_000_000
	}
	return c
}

// NextBackoff returns the exponential delay for the given attempt (1-based)
// with symmetric jitter. rng may be nil for jitter-free deterministic tests.
func (c BackoffConfig) NextBackoff(attempt uint64, rng *rand.Rand) time.Duration {
	c = c.withDefaults()
	delay := c.Base
	for i := uint64(1); i < attempt && delay < c.Max; i++ {
		delay *= 2
		if delay > c.Max {
			delay = c.Max
			break
		}
	}
	if rng == nil || c.JitterPPM == 0 {
		return delay
	}
	spread := int64(delay) * int64(c.JitterPPM) / 1_000_000
	if spread <= 0 {
		return delay
	}
	return delay + time.Duration(rng.Int63n(2*spread+1)-spread)
}
