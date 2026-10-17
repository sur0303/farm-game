package control

import (
	"context"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

// HolderRecordReader is the exact read-only surface implemented by the
// production HolderRegistry. It intentionally excludes registration and
// heartbeat mutation methods.
type HolderRecordReader interface {
	Get(context.Context, actorrouting.RouteDomain, string) (routecore.HolderRecord, bool, error)
	List(context.Context, actorrouting.RouteDomain) ([]routecore.HolderRecord, error)
}

// PlacementExclusionReader is the read half of the existing exclusion seam.
// A3 does not add or emulate release.
type PlacementExclusionReader interface {
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (routecore.PlacementExclusion, bool, error)
}

// AuthorityReader is the existing exact-key Authority read surface.
type AuthorityReader interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
}

// WorkloadStateReader is the read-only half of WorkloadScaler used by
// quiescence. Scale mutation is deliberately not accepted by this adapter.
type WorkloadStateReader interface {
	Get(context.Context, core.WorkloadRef) (core.WorkloadState, error)
}

// TransitionEnumeration is the universal Manager-side enumeration contract.
// Both reads are required; an absent implementation is not an empty set.
type TransitionEnumeration interface {
	LoadTransitionRecoveries(context.Context) ([]authority.TransitionRecovery, error)
	LoadTransitionTombstones(context.Context) ([]authority.TransitionTombstone, error)
}

// CompletePlacementCapture is one complete placement snapshot plus its
// bracket token. It is an adapter-local extension because the frozen core
// ShardPlacementReader surface only returns selected keys.
type CompletePlacementCapture struct {
	Token      core.PlacementCaptureToken
	Placements []core.ShardPlacement
}

// CompletePlacementReader is implemented by the A3 placement adapter.
// Quiescence requires it instead of stitching CaptureShard calls together.
type CompletePlacementReader interface {
	CaptureComplete(context.Context) (CompletePlacementCapture, error)
	Revalidate(context.Context, core.PlacementCaptureToken) error
}
