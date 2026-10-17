// Package publicprofile contains the narrow cross-service reader for the
// authority-owned public player profile.
package publicprofile

import (
	"context"

	publicpb "farmgame/api/gen/public"
)

// Reader returns only the public profile projection. Implementations must not
// expose a PlayerRecord or use a private-state RPC as a substitute.
type Reader interface {
	Read(context.Context, uint64) (*publicpb.PublicProfileView, error)
}

// ProfileReader is the descriptive alias used by Gateway/Farm composition.
type ProfileReader = Reader
