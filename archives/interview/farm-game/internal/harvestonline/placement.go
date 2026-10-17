package harvestonline

import (
	"fmt"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// PlacementConfig is the immutable W31 physical profile supplied by process
// composition. Harvest does not derive a second bucket hash.
type PlacementConfig struct {
	ProfileVersion         string
	NamespaceEnvironment   string
	NamespaceZone          string
	AuthorityLayoutVersion string
	WorkflowLayoutVersion  string
	WorkflowHashVersion    uint32
	WorkflowBucketCount    uint32
}

// NewPlacementResolver constructs the same rediskey.PlacementResolver shape
// used by the existing online compositions, without depending on the Plant
// package or its process wiring.
func NewPlacementResolver(snapshot actorrouting.RouteSnapshot, config PlacementConfig) (workidentity.BucketResolver, error) {
	if snapshot.Realm == "" || snapshot.GlobalRevision == 0 {
		return nil, fmt.Errorf("harvestonline: route snapshot realm/revision is required")
	}
	shards, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("harvestonline: route hash profile: %w", err)
	}
	namespace, err := rediskey.NewNamespace(config.NamespaceEnvironment, config.NamespaceZone)
	if err != nil {
		return nil, fmt.Errorf("harvestonline: Redis namespace: %w", err)
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
		return nil, fmt.Errorf("harvestonline: Redis physical profile: %w", err)
	}
	resolver := rediskey.PlacementResolver{Profile: profile, Shards: shards}
	probe, err := workidentity.StableOpaqueID("harvestonline-placement-probe")
	if err != nil {
		return nil, fmt.Errorf("harvestonline: placement probe identity: %w", err)
	}
	if _, err := resolver.IndependentWorkBucket(probe, 1); err != nil {
		return nil, fmt.Errorf("harvestonline: workflow physical placement: %w", err)
	}
	return resolver, nil
}
