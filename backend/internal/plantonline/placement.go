package plantonline

import (
	"fmt"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// PlacementConfig is the immutable W31-A physical profile shared by the
// producer and both typed Participant authorities.  It is deliberately
// supplied by the process composition; this package never invents a second
// bucket hash or derives one from a Redis prefix.
type PlacementConfig struct {
	ProfileVersion         string
	NamespaceEnvironment   string
	NamespaceZone          string
	AuthorityLayoutVersion string
	WorkflowLayoutVersion  string
	WorkflowHashVersion    uint32
	WorkflowBucketCount    uint32
}

// NewPlacementResolver constructs the existing rediskey.PlacementResolver
// using the same immutable RouteSnapshot hash profile used by Actor routing.
func NewPlacementResolver(snapshot actorrouting.RouteSnapshot, config PlacementConfig) (workidentity.BucketResolver, error) {
	if snapshot.Realm == "" || snapshot.GlobalRevision == 0 {
		return nil, fmt.Errorf("plantonline: route snapshot realm/revision is required")
	}
	shards, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("plantonline: route hash profile: %w", err)
	}
	namespace, err := rediskey.NewNamespace(config.NamespaceEnvironment, config.NamespaceZone)
	if err != nil {
		return nil, fmt.Errorf("plantonline: Redis namespace: %w", err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         config.ProfileVersion,
		Namespace:              namespace,
		AuthorityLayoutVersion: config.AuthorityLayoutVersion,
		WorkflowLayoutVersion:  config.WorkflowLayoutVersion,
		WorkflowHashVersion:    config.WorkflowHashVersion,
		WorkflowBucketCount:    config.WorkflowBucketCount,
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("plantonline: Redis physical profile: %w", err)
	}
	resolver := rediskey.PlacementResolver{Profile: profile, Shards: shards}
	// Exercise the exact W31 path during construction. A resolver with an
	// invalid profile must never reach a Participant server. The probe itself
	// must be a canonical sid1_ identity; arbitrary human-readable strings are
	// intentionally rejected by the frozen WorkAddress contract.
	probe, err := workidentity.StableOpaqueID("plantonline-placement-probe")
	if err != nil {
		return nil, fmt.Errorf("plantonline: placement probe identity: %w", err)
	}
	if _, err := resolver.IndependentWorkBucket(probe, 1); err != nil {
		return nil, fmt.Errorf("plantonline: workflow physical placement: %w", err)
	}
	return resolver, nil
}
