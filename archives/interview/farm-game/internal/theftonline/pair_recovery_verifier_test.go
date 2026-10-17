package theftonline

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"farmgame/internal/contracts"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPairDecideRecoveryVerifierUsesPairLeaseWithoutDestinationWorkRecord(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.UnixMilli(1_800_000_000_000)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "pair-decide-proof")
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	pairs, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := pairs.EnsureScriptsLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}

	const visitor, owner = uint64(91001), uint64(92002)
	pairID, err := identity.CanonicalFriendRelationID(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activationHash := sha256.Sum256([]byte("pair-decide-proof-activate"))
	activated, err := pairs.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "pair-decide-proof-token", TokenGeneration: 1, OperationID: "pair-decide-proof-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "pair-decide-proof-activate", ParticipantStep: "ACTIVATE"}, activationHash[:])
	if err != nil || activated.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", activated, err)
	}

	operationID := "pair-decide-proof-operation"
	cycleID, err := workidentity.StableOpaqueID("pair-decide-proof-cycle", operationID)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256([]byte("pair-decide-proof-payload"))
	targetHash := farmingress.TheftTargetBindingHash(visitor, owner, 3, cycleID)
	admission, err := pairs.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash,
		PermitTTLMs: time.Hour.Milliseconds(), WorkflowTTLMs: (2 * time.Hour).Milliseconds(),
	})
	if err != nil || admission.Outcome.Status != friend.StatusSucceeded {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	order, err := theftrecovery.LoadPairTheftDispatchByOperation(context.Background(), pairs, visitor, owner, operationID)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := theftrecovery.NewFriendPairWorkLeaseRepository(pairs, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := leases.Claim(context.Background(), theftrecovery.ClaimRequest{Order: order,
		HolderID: "worker-a", BootID: "boot-a", LeaseToken: "token-a", RequestedTTL: time.Second})
	if err != nil || claim.Status != theftrecovery.LeaseClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}

	permit, err := actionPermitProto(admission.Permit)
	if err != nil {
		t.Fatal(err)
	}
	bucketFor := func(uint64) (rediskey.AuthorityBucketRef, error) { return rediskey.AuthorityBucketRef{}, nil }
	call, err := farmingress.BuildTheftSourceCall(theft.Input{VisitorPlayerID: visitor, OwnerPlayerID: owner,
		TargetFarmID: owner, PlotID: 3, CropCycleID: cycleID, OperationID: operationID,
		ActionPermitSnapshot: permit, AdmissionHash: targetHash, WorkflowDeadlineAtMs: admission.WorkflowDeadlineAtMs}, bucketFor)
	if err != nil {
		t.Fatal(err)
	}
	call.CallerKind = contracts.ParticipantCallerWorkerRecovery
	call.ExpectedWorkVersion = claim.WorkVersion
	call.CallerInstanceID, call.CallerBootID, call.LeaseOwnerToken = "worker-a", "boot-a", "token-a"
	call.LeaseEpoch = claim.LeaseEpoch
	verifier, err := NewPairDecideRecoveryVerifier(pairs, leases, bucketFor)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyDecideTheftRecovery(context.Background(), call); err != nil {
		t.Fatalf("valid Pair recovery proof rejected: %v", err)
	}

	wrongVersion := *call
	wrongVersion.ExpectedWorkVersion++
	if err := verifier.VerifyDecideTheftRecovery(context.Background(), &wrongVersion); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong Pair WorkVersion admitted: %v", err)
	}
	if err := client.ZRem(context.Background(), mustPairDueKey(t, pairs), order.WorkRef).Err(); err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyDecideTheftRecovery(context.Background(), call); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing Pair DueIndex proof admitted: %v", err)
	}
}

func mustPairDueKey(t *testing.T, pairs *friend.RedisFriendshipRepositories) string {
	t.Helper()
	key, err := pairs.PairDueIndexKey(0)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
