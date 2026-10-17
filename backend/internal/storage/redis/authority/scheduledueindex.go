package authority

import (
	"fmt"
	"strings"
)

// ScheduleDueIndexMode selects how the plan-82 C5 schedule DueIndex is
// written and read. The empty value and "shared" are deliberately identical:
// existing deployments keep the legacy behavior until an operator explicitly
// enables one of the migration modes.
type ScheduleDueIndexMode string

const (
	// ScheduleDueIndexShared writes and reads only the legacy shared
	// {authority-bucket}:due index. This is the compatibility baseline and the
	// default for every existing deployment.
	ScheduleDueIndexShared ScheduleDueIndexMode = "shared"
	// ScheduleDueIndexDualShadow atomically dual-writes the shared and the
	// dedicated schedule-due index, but authority discovery still reads the
	// shared index. It supports bounded shadow comparison.
	ScheduleDueIndexDualShadow ScheduleDueIndexMode = "dual_shadow"
	// ScheduleDueIndexDedicatedFallback atomically dual-writes both indexes
	// and performs bounded reads from both sides, then de-duplicates and
	// globally orders the union. It never misses a still-valid schedule that
	// exists only in the shared index, but it intentionally retains the old
	// shared-scan cost and is therefore only a migration mode.
	ScheduleDueIndexDedicatedFallback ScheduleDueIndexMode = "dedicated_fallback"
	// ScheduleDueIndexDedicatedOnly writes and reads only the dedicated
	// schedule-due index. It must only be enabled after backfill completed and
	// every old worker exited, because shared-only schedules are invisible in
	// this mode.
	ScheduleDueIndexDedicatedOnly ScheduleDueIndexMode = "dedicated_only"
)

// Validate rejects unknown migration modes so a typo cannot silently change
// the deployed discovery behavior.
func (m ScheduleDueIndexMode) Validate() error {
	switch m {
	case ScheduleDueIndexShared, ScheduleDueIndexDualShadow,
		ScheduleDueIndexDedicatedFallback, ScheduleDueIndexDedicatedOnly:
		return nil
	default:
		return fmt.Errorf("unknown schedule due index mode %q", m)
	}
}

// ParseScheduleDueIndexMode parses a configuration value. The empty string is
// the compatibility default "shared"; an unknown value is an error so a
// deployment typo fails closed instead of silently flipping the index.
func ParseScheduleDueIndexMode(raw string) (ScheduleDueIndexMode, error) {
	value := ScheduleDueIndexMode(strings.ToLower(strings.TrimSpace(raw)))
	if value == "" {
		return ScheduleDueIndexShared, nil
	}
	if err := value.Validate(); err != nil {
		return "", err
	}
	return value, nil
}

func (m ScheduleDueIndexMode) writesDedicated() bool {
	return m == ScheduleDueIndexDualShadow || m == ScheduleDueIndexDedicatedFallback || m == ScheduleDueIndexDedicatedOnly
}

func (m ScheduleDueIndexMode) writesShared() bool {
	return m != ScheduleDueIndexDedicatedOnly
}

func (m ScheduleDueIndexMode) sharedWriteArg() string {
	if m.writesShared() {
		return "1"
	}
	return "0"
}

// commitIndexMode is passed to the versioned authority Lua contract:
// 0 writes only the legacy shared index, 1 atomically dual-writes, and 2
// writes only the dedicated index while removing any legacy member.
func (m ScheduleDueIndexMode) commitIndexMode() int {
	switch m {
	case ScheduleDueIndexDualShadow, ScheduleDueIndexDedicatedFallback:
		return 1
	case ScheduleDueIndexDedicatedOnly:
		return 2
	default:
		return 0
	}
}

func (m ScheduleDueIndexMode) readsDedicated() bool {
	return m == ScheduleDueIndexDedicatedFallback || m == ScheduleDueIndexDedicatedOnly
}

func (m ScheduleDueIndexMode) readsShared() bool {
	return m == ScheduleDueIndexShared || m == ScheduleDueIndexDualShadow || m == ScheduleDueIndexDedicatedFallback
}

func (m ScheduleDueIndexMode) String() string { return string(m) }
