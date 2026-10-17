package authoritycomposition

import (
	"sort"

	"farmgame/internal/actorrouting"
)

// desiredLocalShards computes the shards this local holder/boot must serve
// from one complete RouteSnapshot.  Only ACTIVE routes whose holder row
// matches the local holder ID and boot ID are desired; anything else is
// deliberately excluded.  A registered holder with zero matching routes yields
// an empty desired set, which is the valid zero-shard topology.
//
// AllowedDomains restricts which domains are considered; a domain outside the
// allowed set is ignored entirely (neither added nor removed), which lets a
// player runtime reconcile only PLAYER shards and a farm runtime only FARM
// shards.  An empty allowed set admits every domain.
//
// Fields are taken verbatim from the snapshot; the membership core never
// re-derives them. A changed ShardRevision / WriterEpoch / LeaseEpoch or Holder
// identity is a generation replacement. A LeaseUntilMs-only advance is the
// same generation: the sole per-shard scheduler refreshes the authoritative
// lease and local guard without fencing membership.
func desiredLocalShards(snapshot actorrouting.RouteSnapshot, holderID, bootID string, allowedDomains map[actorrouting.RouteDomain]bool) map[ShardKey]MembershipGeneration {
	desired := make(map[ShardKey]MembershipGeneration)
	domains := []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
	for _, domain := range domains {
		if len(allowedDomains) != 0 && !allowedDomains[domain] {
			continue
		}
		ds := snapshotDomain(snapshot, domain)
		for shard := uint32(0); shard < ds.ShardCount; shard++ {
			if int(shard) >= len(ds.Routes) {
				continue
			}
			route := ds.Routes[shard]
			if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 ||
				route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
				// Inactive or incomplete routes are never desired: the holder
				// must not serve a shard it cannot prove a full generation for.
				continue
			}
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				continue
			}
			holder := snapshot.Holders[route.HolderIndex]
			if holder.State != actorrouting.HolderStateActive || holder.InstanceID != holderID || holder.BootID != bootID {
				continue
			}
			desired[ShardKey{Domain: domain, Shard: shard}] = MembershipGeneration{
				AssignmentRevision: route.ShardRevision,
				WriterEpoch:        route.ExpectedWriterEpoch,
				LeaseEpoch:         route.ExpectedLeaseEpoch,
				HolderID:           holder.InstanceID,
				HolderBootID:       holder.BootID,
				LeaseUntilMs:       route.LeaseUntilMs,
			}
		}
	}
	return desired
}

// diffMembership computes the deterministic, ordered action set between the
// desired generations and the currently installed memberships.
//
//   - a key only in installed        -> REMOVE (fence + remove)
//   - a key only in desired          -> ADD (install/admit)
//   - a key in both with the same immutable fenced generation -> UNCHANGED,
//     including a monotonic LeaseUntilMs projection refresh
//   - a key in both with a new immutable fenced generation -> REPLACE (fence
//     old, then install new)
//
// REMOVE/REPLACE actions always precede ADD actions so an old generation is
// fenced before a new one is admitted on the same key.
func diffMembership(desired map[ShardKey]MembershipGeneration, installed map[ShardKey]ShardMembership) []ShardAction {
	actions := make([]ShardAction, 0)
	var removes []ShardAction
	var adds []ShardAction

	keys := make([]ShardKey, 0, len(desired)+len(installed))
	seen := make(map[ShardKey]bool)
	for key := range desired {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	for key := range installed {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sortShardKeys(keys)

	for _, key := range keys {
		desiredGen, inDesired := desired[key]
		installedMember, inInstalled := installed[key]
		switch {
		case inDesired && inInstalled:
			if installedMember.Generation.equal(desiredGen) {
				actions = append(actions, ShardAction{Type: MembershipActionUnchanged, Shard: key, Generation: desiredGen, OldGeneration: installedMember.Generation})
			} else {
				removes = append(removes, ShardAction{Type: MembershipActionReplace, Shard: key, Generation: desiredGen, OldGeneration: installedMember.Generation})
			}
		case inDesired && !inInstalled:
			adds = append(adds, ShardAction{Type: MembershipActionAdd, Shard: key, Generation: desiredGen})
		case !inDesired && inInstalled:
			removes = append(removes, ShardAction{Type: MembershipActionRemove, Shard: key, OldGeneration: installedMember.Generation})
		}
	}
	// Frozen order: fence-class actions first, then adds.
	actions = append(actions, removes...)
	actions = append(actions, adds...)
	return actions
}

// sortShardKeys returns a deterministic ordering (domain first, then shard).
func sortShardKeys(keys []ShardKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Domain != keys[j].Domain {
			return keys[i].Domain < keys[j].Domain
		}
		return keys[i].Shard < keys[j].Shard
	})
}

func sortMemberships(members []ShardMembership) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Key.Domain != members[j].Key.Domain {
			return members[i].Key.Domain < members[j].Key.Domain
		}
		return members[i].Key.Shard < members[j].Key.Shard
	})
}

func snapshotDomain(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	if domain == actorrouting.DomainPlayer {
		return snapshot.Player
	}
	return snapshot.Farm
}
