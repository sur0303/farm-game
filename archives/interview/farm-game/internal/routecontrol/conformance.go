package routecontrol

import (
	"farmgame/internal/controlfence"
)

// ErrNotImplementedUntilA1R is the fail-closed sentinel returned by methods
// that A0.5 adds to an interface but deliberately does not implement. Callers
// must treat it as an unknown outcome and block, never as a success.
//
// It aliases the single declaration in internal/controlfence so that
// errors.Is holds across every tree the seam touches and A1R's exit gate
// remains one grep.
var ErrNotImplementedUntilA1R = controlfence.ErrNotImplementedUntilA1R

// NestedControlOperationToken is an alias, not a distinct type: the token has
// to be nameable from internal/actorrouting, which routecontrol imports. See
// internal/controlfence for why the declaration cannot live here.
type NestedControlOperationToken = controlfence.NestedControlOperationToken
