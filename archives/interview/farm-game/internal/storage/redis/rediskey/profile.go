package rediskey

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"farmgame/internal/actorrouting"
	"farmgame/internal/workidentity"
)

var ErrInvalidPhysicalProfile = errors.New("rediskey: invalid physical profile")

// PhysicalProfile keeps tunable placement parameters outside domain models.
// Values become immutable once the corresponding layout contains data.
type PhysicalProfile struct {
	ProfileVersion         string
	Namespace              Namespace
	AuthorityLayoutVersion string
	WorkflowLayoutVersion  string
	WorkflowHashVersion    uint32
	WorkflowBucketCount    uint32
}

func (p PhysicalProfile) Validate() error {
	if !validLowerSegment(p.ProfileVersion, 64) || p.Namespace.Validate() != nil ||
		!validLayoutVersion(p.AuthorityLayoutVersion) || !validLayoutVersion(p.WorkflowLayoutVersion) ||
		p.WorkflowHashVersion != 1 || p.WorkflowBucketCount == 0 || p.WorkflowBucketCount&(p.WorkflowBucketCount-1) != 0 {
		return ErrInvalidPhysicalProfile
	}
	return nil
}

func (p PhysicalProfile) AuthorityBucketForActor(resolver AuthorityShardResolver, address actorrouting.ActorAddress) (AuthorityBucketRef, error) {
	if err := p.Validate(); err != nil {
		return AuthorityBucketRef{}, err
	}
	if err := validateAuthorityResolverProfile(resolver); err != nil {
		return AuthorityBucketRef{}, err
	}
	if address.ActorID == 0 {
		return AuthorityBucketRef{}, ErrInvalidBucketRef
	}
	if address.Domain != actorrouting.DomainPlayer && address.Domain != actorrouting.DomainFarm {
		return AuthorityBucketRef{}, ErrInvalidBucketRef
	}
	virtualShard, err := resolver.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		return AuthorityBucketRef{}, err
	}
	return newAuthorityBucketRef(address.Domain, p.AuthorityLayoutVersion, virtualShard)
}

// AuthorityBucketForShard is reserved for RouteManager/Worker bucket scans.
// Business command code must use AuthorityBucketForActor.
func (p PhysicalProfile) AuthorityBucketForShard(resolver AuthorityShardResolver, domain actorrouting.RouteDomain, virtualShard uint32) (AuthorityBucketRef, error) {
	if err := p.Validate(); err != nil {
		return AuthorityBucketRef{}, err
	}
	if err := validateAuthorityResolverProfile(resolver); err != nil {
		return AuthorityBucketRef{}, err
	}
	count, err := resolver.ShardCount(domain)
	if err != nil || virtualShard >= count {
		return AuthorityBucketRef{}, ErrInvalidBucketRef
	}
	return newAuthorityBucketRef(domain, p.AuthorityLayoutVersion, virtualShard)
}

// WorkflowBucket v1 is SHA-256(workflow_id UTF-8), first uint64 big-endian,
// masked by a power-of-two bucket count. Changing any byte requires a new
// WorkflowHashVersion and layout migration.
func (p PhysicalProfile) WorkflowBucket(workflowID string) (WorkflowBucketRef, error) {
	if err := p.Validate(); err != nil {
		return WorkflowBucketRef{}, err
	}
	if workidentity.ValidateStableOpaqueID(workflowID) != nil {
		return WorkflowBucketRef{}, ErrInvalidComponent
	}
	digest := sha256.Sum256([]byte(workflowID))
	bucket := uint32(binary.BigEndian.Uint64(digest[:8])) & (p.WorkflowBucketCount - 1)
	return newWorkflowBucketRef(p.WorkflowLayoutVersion, bucket, workflowID)
}

// WorkflowBucketAt is reserved for bounded Worker scans. It cannot build a
// WorkRecord key because it is not bound to a workflow identity.
func (p PhysicalProfile) WorkflowBucketAt(bucketID uint32) (WorkflowBucketRef, error) {
	if err := p.Validate(); err != nil || bucketID >= p.WorkflowBucketCount {
		return WorkflowBucketRef{}, ErrInvalidBucketRef
	}
	return newWorkflowBucketRef(p.WorkflowLayoutVersion, bucketID, "")
}

// AuthorityShardResolver must be backed by the same immutable RouteSnapshot
// used by Actor routing. W31-A does not permit a second persistence hash.
type AuthorityShardResolver interface {
	ShardFor(actorrouting.RouteDomain, uint64) (uint32, error)
	ShardCount(actorrouting.RouteDomain) (uint32, error)
	Profile() actorrouting.HashProfile
}

func validateAuthorityResolverProfile(resolver AuthorityShardResolver) error {
	if resolver == nil {
		return ErrInvalidPhysicalProfile
	}
	if err := resolver.Profile().Validate(); err != nil {
		return fmt.Errorf("%w: routing hash profile: %v", ErrInvalidPhysicalProfile, err)
	}
	return nil
}

// PlacementResolver connects W7D WorkAddress validation to W31-A placement.
type PlacementResolver struct {
	Profile PhysicalProfile
	Shards  AuthorityShardResolver
}

func (r PlacementResolver) IndependentWorkBucket(workID string, bucketVersion uint32) (uint32, error) {
	if bucketVersion != 1 || r.Profile.WorkflowLayoutVersion != "v1" {
		return 0, fmt.Errorf("%w: workflow bucket version", ErrInvalidPhysicalProfile)
	}
	ref, err := r.Profile.WorkflowBucket(workID)
	return ref.BucketID(), err
}

func (r PlacementResolver) AuthorityPersistenceBucket(domain workidentity.RouteDomain, actorID uint64, bucketVersion uint32) (uint32, error) {
	if bucketVersion != 1 || r.Profile.AuthorityLayoutVersion != "v1" || r.Shards == nil || actorID == 0 {
		return 0, fmt.Errorf("%w: authority bucket version", ErrInvalidPhysicalProfile)
	}
	var routeDomain actorrouting.RouteDomain
	switch domain {
	case workidentity.RouteDomainPlayer:
		routeDomain = actorrouting.DomainPlayer
	case workidentity.RouteDomainFarm:
		routeDomain = actorrouting.DomainFarm
	default:
		return 0, ErrInvalidBucketRef
	}
	return r.Shards.ShardFor(routeDomain, actorID)
}

var _ workidentity.BucketResolver = PlacementResolver{}
