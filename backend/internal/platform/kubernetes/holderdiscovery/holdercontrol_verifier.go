package holderdiscovery

import (
	"context"
	"fmt"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
	"farmgame/internal/routecontrol/verifiedholder"

	"google.golang.org/grpc/status"
)

type HolderControlIdentityClient interface {
	GetRuntimeIdentity(context.Context, *holdercontrolpb.GetRuntimeIdentityRequest) (*holdercontrolpb.RuntimeIdentity, error)
}

type HolderControlClientProvider interface {
	Client(context.Context, ObservedPod) (HolderControlIdentityClient, func(), error)
}

type HolderControlClientProviderFunc func(context.Context, ObservedPod) (HolderControlIdentityClient, func(), error)

func (f HolderControlClientProviderFunc) Client(ctx context.Context, pod ObservedPod) (HolderControlIdentityClient, func(), error) {
	if f == nil {
		return nil, nil, ErrRuntimeIdentity
	}
	return f(ctx, pod)
}

type HolderControlVerifier struct {
	provider HolderControlClientProvider
}

func NewHolderControlVerifier(provider HolderControlClientProvider) (*HolderControlVerifier, error) {
	if provider == nil {
		return nil, ErrInvalidDiscovery
	}
	return &HolderControlVerifier{provider: provider}, nil
}

func (v *HolderControlVerifier) Verify(ctx context.Context, pod ObservedPod) (RuntimeIdentity, error) {
	if v == nil || v.provider == nil || ctx == nil {
		return RuntimeIdentity{}, ErrRuntimeIdentity
	}
	client, release, err := v.provider.Client(ctx, pod)
	if err != nil || client == nil {
		return RuntimeIdentity{}, fmt.Errorf("%w: holder control client unavailable", ErrRuntimeIdentity)
	}
	if release != nil {
		defer release()
	}
	response, err := client.GetRuntimeIdentity(ctx, &holdercontrolpb.GetRuntimeIdentityRequest{Domain: string(pod.Workload.Domain)})
	if err != nil || response == nil {
		return RuntimeIdentity{}, fmt.Errorf("%w: holder control RPC code=%s", ErrRuntimeIdentity, status.Code(err))
	}
	lifecycle, err := parseLifecycle(response.GetLifecycleState())
	if err != nil {
		return RuntimeIdentity{}, err
	}
	domain := actorrouting.RouteDomain(response.GetDomain())
	var publication holderkey.Publication
	keyID, public := response.GetCommitProofKeyId(), response.GetCommitProofPublicKey()
	if keyID != "" || len(public) != 0 {
		if keyID == "" || len(public) == 0 {
			return RuntimeIdentity{}, fmt.Errorf("%w: partial commit proof public key", ErrRuntimeIdentity)
		}
		publication = holderkey.Publication{KeyID: keyID, PublicKey: append([]byte(nil), public...)}
		if err := publication.Validate(); err != nil {
			return RuntimeIdentity{}, fmt.Errorf("%w: commit proof public key", ErrRuntimeIdentity)
		}
	}
	return RuntimeIdentity{
		Domain: domain, HolderID: response.GetHolderId(), BootID: response.GetBootId(), WorkloadUID: response.GetWorkloadUid(),
		PodUID: response.GetPodUid(), Ordinal: response.GetOrdinal(), ProcessReady: response.GetProcessReady(), Draining: response.GetDraining(),
		ServingShardCount: response.GetServingShardCount(), ObservedRouteRevision: response.GetObservedRouteRevision(), Lifecycle: lifecycle,
		CapacityWeight: 1, ObservedAt: time.UnixMilli(response.GetObservedAtMs()), WorkloadIdentityToken: response.GetWorkloadIdentityToken(),
		CommitProofKey: publication,
	}, nil
}

func parseLifecycle(value string) (verifiedholder.LifecycleState, error) {
	state := verifiedholder.LifecycleState(value)
	switch state {
	case verifiedholder.LifecycleWarming, verifiedholder.LifecycleReadyZeroShard, verifiedholder.LifecycleServing, verifiedholder.LifecycleDraining:
		return state, nil
	default:
		return "", fmt.Errorf("%w: lifecycle %q", ErrRuntimeIdentity, value)
	}
}

var _ RuntimeIdentityVerifier = (*HolderControlVerifier)(nil)
