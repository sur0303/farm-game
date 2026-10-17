// Package datamode defines the process-wide actor persistence experiment mode.
//
// The default remains the existing synchronous, writer-fenced Authority CAS
// path. DirtyBestEffort is deliberately opt-in because it accepts a bounded
// crash-loss window and weaker cross-actor semantics.
package datamode

import (
	"fmt"
	"strings"
)

const EnvKey = "FARM_GAME_DATA_MODE"

type Mode string

const (
	DurableCAS      Mode = "durable_cas"
	DirtyBestEffort Mode = "dirty_best_effort"
)

func (m Mode) String() string { return string(m) }

func (m Mode) DirtyWritebackEnabled() bool { return m == DirtyBestEffort }

func Parse(raw string) (Mode, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", string(DurableCAS):
		return DurableCAS, nil
	case string(DirtyBestEffort):
		return DirtyBestEffort, nil
	default:
		return "", fmt.Errorf("actor data mode: unsupported %q (want %q or %q)", raw, DurableCAS, DirtyBestEffort)
	}
}

func FromLookup(lookup func(string) (string, bool)) (Mode, error) {
	if lookup == nil {
		return DurableCAS, nil
	}
	raw, _ := lookup(EnvKey)
	return Parse(raw)
}
