package theftonline

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	theftscenario "farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type resultProcessorSpy struct {
	calls     int
	candidate theftrecovery.DispatchCandidate
}

func (p *resultProcessorSpy) ProcessResultOrder(_ context.Context, candidate theftrecovery.DispatchCandidate) (bool, error) {
	p.calls++
	p.candidate = candidate
	return true, nil
}

func TestDispatchFastPathCompletesPairAndProcessesResultUnderWorkLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, err := rediskeyForDispatchTestNamespace()
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.EnsureScriptsLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}

	const visitor, owner = uint64(81001), uint64(82002)
	pairID, err := identity.CanonicalFriendRelationID(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activationHash := sha256.Sum256([]byte("dispatch-fast-path-activate"))
	if outcome, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "dispatch-fast-path-token", TokenGeneration: 1,
		OperationID: "dispatch-fast-path-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "dispatch-fast-path-activate", ParticipantStep: "ACTIVATE"}, activationHash[:]); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("activate outcome=%+v err=%v", outcome, err)
	}

	operationID := "dispatch-fast-path-operation"
	payloadHash := sha256.Sum256([]byte(operationID + ":payload"))
	cycleID, err := workidentity.StableOpaqueID("dispatch-fast-path-cycle", operationID)
	if err != nil {
		t.Fatal(err)
	}
	targetHash := farmingress.TheftTargetBindingHash(visitor, owner, 3, cycleID)
	admission, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner,
		TargetFarmID: owner, Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash,
		PermitTTLMs: time.Hour.Milliseconds(), WorkflowTTLMs: 24 * time.Hour.Milliseconds(),
	})
	if err != nil || admission.Outcome.Status != friend.StatusSucceeded || admission.DispatchOrderID == "" {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}

	targetStore, err := NewRedisTargetBindingStore(client, "dispatch-fast-path-target", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	binding := farmingress.TheftTargetBinding{
		PairID: pairID, DispatchOrderID: admission.DispatchOrderID,
		VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		OperationID: operationID, PlotID: 3, CropCycleID: cycleID,
		PayloadHash: payloadHash[:], TargetBindingHash: targetHash,
	}
	if err := targetStore.Put(context.Background(), binding); err != nil {
		t.Fatal(err)
	}

	processor := &resultProcessorSpy{}
	visitorOrderID, err := workidentity.StableOpaqueID("dispatch-fast-path-visitor-order", operationID)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := NewDispatch(DispatchConfig{
		PairRepositories: repositories, TargetStore: targetStore,
		FastPathHolderID: "farm-svr-test", FastPathBootID: "farm-svr-test-boot",
		FastPathLeaseToken: "farm-svr-test-lease", FastPathLeaseTTL: time.Second,
		ResultProcessor: processor,
		SourceCommit: func(context.Context, theftscenario.Input) (string, string, error) {
			return visitorOrderID, "", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.DispatchTheft(context.Background(), admission.DispatchOrderID); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 || processor.candidate.Member == "" || processor.candidate.BucketID != 0 {
		t.Fatalf("result processor calls=%d candidate=%+v", processor.calls, processor.candidate)
	}

	source, err := repositories.ReadPairDispatchOrder(context.Background(), 0, theftrecovery.FriendActionDispatchOrder, admission.DispatchOrderID)
	if err != nil {
		t.Fatal(err)
	}
	if source.Fields["state"] != "COMPLETED" || source.Fields["lease_expire_at_ms"] != "0" {
		t.Fatalf("fast-path dispatch not terminal: state=%q lease_expire=%q", source.Fields["state"], source.Fields["lease_expire_at_ms"])
	}
	if err := dispatch.DispatchTheft(context.Background(), admission.DispatchOrderID); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 {
		t.Fatalf("terminal replay invoked source/result again: calls=%d", processor.calls)
	}
}

// Keep the test's namespace construction local to avoid sharing production
// profile defaults with unrelated miniredis tests.
func rediskeyForDispatchTestNamespace() (rediskey.Namespace, error) {
	return rediskey.NewNamespace("test", "dispatch-fast-path")
}
