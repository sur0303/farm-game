package scaleplan

import "testing"

func TestSealedBlockedTerminationBindsRepositoryAndStableWriteAuthority(t *testing.T) {
	identityA, identityB := &repositoryIdentity{}, &repositoryIdentity{}
	sealerA, sealerB := &blockedTerminationSealer{}, &blockedTerminationSealer{}
	lease := BackendMutationLease{
		RepositoryID: "repository-a",
		Scope:        MutationScope{Namespace: "farm", Realm: "realm-a"},
		Owner: MutationLeaseOwner{
			ControllerID: "controller-a", ControllerBootID: "boot-a", OperationNonce: "nonce-a",
		},
		Fence: "7", ExpiresAtMs: 1000,
	}
	sealed := SealedBlockedTerminationMutation{
		authority: identityA, owner: sealerA,
		request: BlockedTerminationRequest{PlanID: "plan-a"},
		lease:   lease, checkpoint: "checkpoint", evidence: "evidence",
	}

	if !sealed.authorizedFor(identityA, sealerA) {
		t.Fatal("minting facade rejected its own seal")
	}
	if sealed.authorizedFor(identityB, sealerA) || sealed.authorizedFor(identityA, sealerB) {
		t.Fatal("a foreign repository identity or owner capability accepted the seal")
	}

	renewed := lease
	renewed.ExpiresAtMs++
	if !sealed.BoundTo(renewed) {
		t.Fatal("same write authority was rejected only because renewal moved the deadline")
	}
	foreignRepository := renewed
	foreignRepository.RepositoryID = "repository-b"
	if sealed.BoundTo(foreignRepository) {
		t.Fatal("another repository accepted an equal scope/owner/fence tuple")
	}
	foreignScope := renewed
	foreignScope.Scope.Realm = "realm-b"
	if sealed.BoundTo(foreignScope) {
		t.Fatal("another mutation scope accepted the seal")
	}
	foreignOwner := renewed
	foreignOwner.Owner.OperationNonce = "nonce-b"
	if sealed.BoundTo(foreignOwner) {
		t.Fatal("another mutation owner accepted the seal")
	}
	newFence := renewed
	newFence.Fence = "8"
	if sealed.BoundTo(newFence) {
		t.Fatal("a superseded writer fence accepted the old seal")
	}
}
