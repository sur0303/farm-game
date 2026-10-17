package sourceorder

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
)

// ValidateProductionPreferComposition keeps prefer an explicit production
// opt-in and rejects static/shared-key compositions. Off and shadow remain
// valid mixed-rollout modes even while route key publication is incomplete.
func ValidateProductionPreferComposition(mode CommitProofMode, dynamicRouteWatch, holderBootIsolatedKey bool) error {
	if err := mode.ValidatePreferRollout(); err != nil {
		return err
	}
	if mode != CommitProofModePrefer {
		return nil
	}
	if !dynamicRouteWatch || !holderBootIsolatedKey {
		return fmt.Errorf("%w: prefer requires dynamic RouteSnapshot watch and a holder/boot-isolated ephemeral key", ErrCommitProofBypassUnavailable)
	}
	return nil
}

// ValidateRouteCommitProofKeys requires one canonical key on every ACTIVE
// holder in the selected source domain. Duplicate/shared keys and cardinality
// are already rejected by RouteSnapshot validation.
func ValidateRouteCommitProofKeys(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) error {
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	var routes actorrouting.DomainSnapshot
	switch domain {
	case actorrouting.DomainPlayer:
		routes = snapshot.Player
	case actorrouting.DomainFarm:
		routes = snapshot.Farm
	default:
		return ErrCommitProofRoute
	}
	seen := make(map[int]struct{}, len(routes.Routes))
	for _, route := range routes.Routes {
		if route.State != actorrouting.ShardStateActive {
			continue
		}
		if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			return ErrCommitProofRoute
		}
		if _, duplicate := seen[route.HolderIndex]; duplicate {
			continue
		}
		seen[route.HolderIndex] = struct{}{}
		if _, found, err := snapshot.Holders[route.HolderIndex].CommitProofPublication(); err != nil || !found {
			return fmt.Errorf("%w: active %s holder key unavailable", ErrCommitProofBypassUnavailable, domain)
		}
	}
	return nil
}

// ValidateLocalCommitProofSignerPublication proves that every ACTIVE route for
// this exact local holder boot carries the signer's public half. A zero-shard
// holder has no source signing surface and is vacuously ready; its first future
// assignment must include the key before PublishedCommitProofIssuer can mint.
func ValidateLocalCommitProofSignerPublication(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain,
	holderID, bootID string, publication holderkey.Publication) error {
	if publication.Validate() != nil || !validProofToken(holderID, 128) || !validProofToken(bootID, 128) {
		return ErrCommitProofInvalid
	}
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	var routes actorrouting.DomainSnapshot
	switch domain {
	case actorrouting.DomainPlayer:
		routes = snapshot.Player
	case actorrouting.DomainFarm:
		routes = snapshot.Farm
	default:
		return ErrCommitProofRoute
	}
	for _, route := range routes.Routes {
		if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			continue
		}
		holder := snapshot.Holders[route.HolderIndex]
		if holder.InstanceID != holderID || holder.BootID != bootID {
			continue
		}
		published, found, err := holder.CommitProofPublication()
		if err != nil || !found || !published.Equal(publication) {
			return fmt.Errorf("%w: exact local signer is not published", ErrCommitProofBypassUnavailable)
		}
	}
	return nil
}

// WaitForCommitProofRouteReadiness waits only on process-local route snapshots;
// it performs no Redis or network reads. Callers bound it with their existing
// remote-assignment startup deadline.
func WaitForCommitProofRouteReadiness(ctx context.Context, snapshot func() actorrouting.RouteSnapshot,
	check func(actorrouting.RouteSnapshot) error) error {
	if ctx == nil || snapshot == nil || check == nil {
		return ErrCommitProofInvalid
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		last = check(snapshot())
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrCommitProofBypassUnavailable, ctx.Err(), last)
		case <-ticker.C:
		}
	}
}
