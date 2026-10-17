package downlink

// WriterCapacity is the set of core per-connection downlink capacity
// parameters. Zero values fall back to the safe defaults used by the legacy
// constructor. Hint soft quota is evaluated before sequence allocation so the
// FIFO (once admitted) is never selectively drained (28 L2-PROTO-26).
type WriterCapacity struct {
	MaxItems int
	MaxBytes int
	Hint     HintPolicy
}

// WithHint returns a capacity copy with the given hint soft quota.
func (c WriterCapacity) WithHint(maxHintItems, maxHintBytes int) WriterCapacity {
	c.Hint = HintPolicy{MaxHintItems: maxHintItems, MaxHintBytes: maxHintBytes}
	return c
}
