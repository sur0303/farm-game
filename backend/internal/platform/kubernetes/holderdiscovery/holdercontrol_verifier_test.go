package holderdiscovery

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
	"farmgame/internal/routecontrol/verifiedholder"
	scaleplancore "farmgame/internal/scaleplan"
)

type fakeIdentityClient struct {
	response *holdercontrolpb.RuntimeIdentity
	err      error
}

func (c fakeIdentityClient) GetRuntimeIdentity(context.Context, *holdercontrolpb.GetRuntimeIdentityRequest) (*holdercontrolpb.RuntimeIdentity, error) {
	return c.response, c.err
}

func TestHolderControlVerifierMapsRuntimeIdentity(t *testing.T) {
	now := time.Unix(100, 0)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := holderkey.New(public)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewHolderControlVerifier(HolderControlClientProviderFunc(func(context.Context, ObservedPod) (HolderControlIdentityClient, func(), error) {
		return fakeIdentityClient{response: &holdercontrolpb.RuntimeIdentity{Domain: "FARM", HolderId: "farm-svr-1", BootId: "boot-1", WorkloadUid: "workload-1", PodUid: "pod-1", Ordinal: 1,
			ProcessReady: true, LifecycleState: "READY_ZERO_SHARD", ObservedAtMs: now.UnixMilli(), WorkloadIdentityToken: "token",
			CommitProofKeyId: publication.KeyID, CommitProofPublicKey: public}}, func() {}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(context.Background(), ObservedPod{Workload: scaleWorkloadFarm(), HolderID: "farm-svr-1"})
	if err != nil {
		t.Fatal(err)
	}
	if identity.Domain != actorrouting.DomainFarm || identity.Lifecycle != verifiedholder.LifecycleReadyZeroShard || identity.BootID != "boot-1" ||
		!identity.CommitProofKey.Equal(publication) {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func scaleWorkloadFarm() scaleplancore.WorkloadRef {
	return scaleplancore.WorkloadRef{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
}
