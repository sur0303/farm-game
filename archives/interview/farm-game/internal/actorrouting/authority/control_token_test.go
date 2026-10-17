package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

type authorityControlTestCoordinator struct {
	issuer      *controlfence.Issuer
	expired     bool
	verifyCalls int
}

func newAuthorityControlTestCoordinator() *authorityControlTestCoordinator {
	return &authorityControlTestCoordinator{issuer: controlfence.NewIssuer()}
}

func (c *authorityControlTestCoordinator) Acquire(ctx context.Context, request core.ControlOperationRequest) (core.ControlOperationToken, error) {
	if ctx == nil {
		return core.ControlOperationToken{}, errors.New("nil context")
	}
	return c.issuer.Issue(request.OperationID, 1), nil
}

func (c *authorityControlTestCoordinator) Verify(_ context.Context, token core.ControlOperationToken, _ core.MutationScope) error {
	c.verifyCalls++
	if c.expired {
		return errors.New("expired control token")
	}
	if !c.issuer.IssuedBy(token) {
		return errors.New("foreign control token")
	}
	return nil
}

func (c *authorityControlTestCoordinator) Resolve(context.Context, core.ControlOperationToken) error {
	return nil
}

func (c *authorityControlTestCoordinator) Mode(context.Context, core.MutationScope) (core.ControlGateMode, error) {
	return core.ControlGateOpen, nil
}

func gatedAuthorityManagerForTest(t *testing.T, coordinator core.ControlMutationCoordinator, publisher SnapshotPublisher) (*RouteAssignmentManager, *recordingAuthorityStore) {
	t.Helper()
	manager, store := newManagerForTest(t, publisher)
	manager.controlCoordinator = coordinator
	manager.controlScope = core.MutationScope{Namespace: "farm", Realm: "classic-farm"}
	return manager, store
}

func controlClaimRequest(transitionID string) ClaimRequest {
	return ClaimRequest{
		TransitionID:    transitionID,
		Domain:          actorrouting.DomainPlayer,
		VirtualShard:    0,
		NewHolderID:     "player-b",
		NewHolderBootID: "player-b-boot",
		Endpoint:        "player-b:9000",
		RequestedTTL:    10 * time.Second,
	}
}

func TestRouteAssignmentManagerControlTokenOffNoopAndPropagation(t *testing.T) {
	offPublisher := testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	})
	offManager, offStore := newManagerForTest(t, offPublisher)
	if _, err := offManager.Claim(context.Background(), controlClaimRequest("claim-off")); err != nil || offStore.claim != 1 {
		t.Fatalf("mode=off/no coordinator changed G21 behavior: err=%v claims=%d", err, offStore.claim)
	}

	coordinator := newAuthorityControlTestCoordinator()
	var publishedToken routecontrol.NestedControlOperationToken
	publisher := testSnapshotPublisherFunc(func(_ context.Context, token routecontrol.NestedControlOperationToken, _ actorrouting.RouteSnapshot) error {
		publishedToken = token
		return nil
	})
	manager, store := gatedAuthorityManagerForTest(t, coordinator, publisher)
	if _, err := manager.Claim(context.Background(), controlClaimRequest("claim-required")); !errors.Is(err, ErrControlTokenRequired) || store.claim != 0 {
		t.Fatalf("manual mutation without token was not blocked: err=%v claims=%d", err, store.claim)
	}
	token := coordinator.issuer.Issue("claim-required", 1)
	result, err := manager.Claim(WithControlOperationToken(context.Background(), token), controlClaimRequest("claim-required"))
	if err != nil || result.Status != authoritystore.ShardTransitionApplied || publishedToken != token || store.claim != 1 {
		t.Fatalf("issued token did not cross Manager/publisher: result=%+v err=%v same_token=%t claims=%d", result, err, publishedToken == token, store.claim)
	}
}

func TestRouteAssignmentManagerForeignAndExpiredTokensRejected(t *testing.T) {
	foreignCoordinator := newAuthorityControlTestCoordinator()
	foreignManager, foreignStore := gatedAuthorityManagerForTest(t, foreignCoordinator, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	foreign := controlfence.NewIssuer().Issue("foreign", 1)
	if _, err := foreignManager.Claim(WithControlOperationToken(context.Background(), foreign), controlClaimRequest("claim-foreign")); !errors.Is(err, ErrControlTokenRejected) || foreignStore.claim != 0 {
		t.Fatalf("foreign token accepted: err=%v claims=%d", err, foreignStore.claim)
	}

	expiredCoordinator := newAuthorityControlTestCoordinator()
	expiredManager, expiredStore := gatedAuthorityManagerForTest(t, expiredCoordinator, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	expired := expiredCoordinator.issuer.Issue("expired", 1)
	expiredCoordinator.expired = true
	if _, err := expiredManager.Claim(WithControlOperationToken(context.Background(), expired), controlClaimRequest("claim-expired")); !errors.Is(err, ErrControlTokenRejected) || expiredStore.claim != 0 {
		t.Fatalf("expired token accepted: err=%v claims=%d", err, expiredStore.claim)
	}
}

func TestRouteAssignmentManagerRecoveryRequiresControlToken(t *testing.T) {
	coordinator := newAuthorityControlTestCoordinator()
	manager, _ := gatedAuthorityManagerForTest(t, coordinator, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.RecoverPendingProjections(ctx, nil, nil); !errors.Is(err, ErrControlTokenRequired) {
		t.Fatalf("recovery without token was not blocked: %v", err)
	}
}
