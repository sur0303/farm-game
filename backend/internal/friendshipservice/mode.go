package friendshipservice

import (
	"fmt"
	"strings"
)

const ModeEnvironment = "FRIENDSHIP_SERVICE_MODE"

type Mode string

const (
	ModeLocalGate    Mode = "local_gate"
	ModeRemoteShadow Mode = "remote_shadow"
	ModeRemotePlayer Mode = "remote_player"
)

// ParseMode rejects blank and unknown configured values. An absent variable
// is handled separately by ModeFromEnvironment so an explicitly empty value
// cannot silently select the rollback mode.
func ParseMode(raw string) (Mode, error) {
	value := Mode(strings.ToLower(strings.TrimSpace(raw)))
	switch value {
	case ModeLocalGate, ModeRemoteShadow, ModeRemotePlayer:
		return value, nil
	default:
		return "", fmt.Errorf("%s must be one of %s, %s, %s; got %q",
			ModeEnvironment, ModeLocalGate, ModeRemoteShadow, ModeRemotePlayer, raw)
	}
}

// ModeFromEnvironment deliberately defaults an absent variable to
// local_gate, matching the frozen R1 process layout. A present-but-empty or
// otherwise invalid variable fails closed.
func ModeFromEnvironment(lookup func(string) (string, bool)) (Mode, error) {
	if lookup == nil {
		return "", fmt.Errorf("%s environment lookup is required", ModeEnvironment)
	}
	raw, present := lookup(ModeEnvironment)
	if !present {
		return ModeLocalGate, nil
	}
	return ParseMode(raw)
}
