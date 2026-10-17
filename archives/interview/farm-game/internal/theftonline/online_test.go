package theftonline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/farm/domain"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func TestRedisTargetBindingStoreSurvivesAdmissionEnrichmentAndCanonicalizesPair(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := NewRedisTargetBindingStore(client, "test:theft-target", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := workidentity.StableOpaqueID("theft-online-cycle", "cycle-1")
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := bytes.Repeat([]byte{0x31}, 32)
	binding := farmingress.TheftTargetBinding{
		VisitorPlayerID: 30003, OwnerPlayerID: 20002, TargetFarmID: 20002,
		OperationID: "theft-online-op", PlotID: 3, CropCycleID: cycle,
		PayloadHash:       payloadHash,
		TargetBindingHash: farmingress.TheftTargetBindingHash(30003, 20002, 3, cycle),
	}
	if err := store.Put(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	pairID, err := identity.CanonicalFriendRelationID(binding.VisitorPlayerID, binding.OwnerPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	dispatchID, err := workidentity.StableOpaqueID("theft-online-dispatch", "one")
	if err != nil {
		t.Fatal(err)
	}
	binding.PairID, binding.DispatchOrderID = pairID, dispatchID
	if err := store.Put(context.Background(), binding); err != nil {
		t.Fatalf("admission enrichment: %v", err)
	}
	got, err := store.Lookup(context.Background(), TargetBindingLookup{
		VisitorPlayerID: binding.VisitorPlayerID, OwnerPlayerID: binding.OwnerPlayerID,
		TargetFarmID: binding.TargetFarmID, OperationID: binding.OperationID,
		PayloadHash: payloadHash, TargetBindingHash: binding.TargetBindingHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.PairID != pairID || got.DispatchOrderID != dispatchID || got.PlotID != binding.PlotID || got.CropCycleID != cycle {
		t.Fatalf("binding=%+v", got)
	}
	byDispatch, err := store.LookupByDispatchOrderID(context.Background(), dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	if byDispatch.PairID != pairID || byDispatch.DispatchOrderID != dispatchID || byDispatch.OperationID != binding.OperationID {
		t.Fatalf("dispatch locator=%+v", byDispatch)
	}
	otherDispatch, err := workidentity.StableOpaqueID("theft-online-dispatch", "two")
	if err != nil {
		t.Fatal(err)
	}
	binding.DispatchOrderID = otherDispatch
	if err := store.Put(context.Background(), binding); !errors.Is(err, ErrTargetBindingConflict) {
		t.Fatalf("dispatch identity conflict err=%v", err)
	}
}

func TestProductionWorkerAdvancerRejectsMemoryWorkRepository(t *testing.T) {
	_, err := NewWorkerAdvancer(WorkerAdvancerConfig{
		Repository:    workflow.NewMemoryWorkRepository(time.Now),
		BucketVersion: 1,
	})
	if !errors.Is(err, ErrInvalidComposition) {
		t.Fatalf("memory worker composition err=%v", err)
	}
}

func TestActionPermitProtoMapsDomainTheftScopeToWireTheftScope(t *testing.T) {
	pairID, err := identity.CanonicalFriendRelationID(11, 22)
	if err != nil {
		t.Fatal(err)
	}
	permitID, err := workidentity.StableOpaqueID("theft-online-permit", "scope")
	if err != nil {
		t.Fatal(err)
	}
	permit := friend.ActionPermitSnapshot{
		PermitID: permitID, RelationID: pairID, RelationEpoch: 1,
		VisitorPlayerID: 11, OwnerPlayerID: 22, TargetFarmID: 22,
		OperationID: "scope-op", Scope: friend.ActionScopeTheft,
		PayloadHash: bytes.Repeat([]byte{0x41}, 32), TargetBindingHash: bytes.Repeat([]byte{0x42}, 32),
		IssuedAtAuthorityMs: 100, ExpireAtAuthorityMs: 1000, ExecutionAcceptedAtAuthorityMs: 101,
	}
	wire, err := actionPermitProto(permit)
	if err != nil {
		t.Fatal(err)
	}
	if wire.GetActionScope() != contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT {
		t.Fatalf("wire ActionScope=%v, want THEFT", wire.GetActionScope())
	}
}

func TestRedisGuardSnapshotStoreIsVersionedAndMissingIsZero(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := NewRedisGuardSnapshotStore(client, "test:theft-guard")
	if err != nil {
		t.Fatal(err)
	}
	missing, err := store.ReadParticipantGuardSnapshot(context.Background(), 901)
	if err != nil || missing != (domain.GuardSnapshot{}) {
		t.Fatalf("missing snapshot=%+v err=%v", missing, err)
	}
	snapshot := domain.GuardSnapshot{
		SchemaVersion: 1, GuardVersion: 3, ActiveGuardPetID: 77,
		GuardUntilMs: time.Now().Add(time.Hour).UnixMilli(),
		Rule: domain.GuardRule{PetConfigVersion: 1, GuardRuleVersion: 2,
			TriggerProbabilityBP: 10000, InterceptPolicy: domain.InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1},
	}
	snapshot.SnapshotHash = domain.CanonicalGuardSnapshotHash(snapshot)
	if err := store.Put(context.Background(), 901, snapshot); err != nil {
		t.Fatal(err)
	}
	got, err := store.ReadParticipantGuardSnapshot(context.Background(), 901)
	if err != nil || got != snapshot {
		t.Fatalf("stored snapshot=%+v err=%v want=%+v", got, err, snapshot)
	}
	if err := store.Put(context.Background(), 901, snapshot); err != nil {
		t.Fatalf("equal replay: %v", err)
	}
	older := snapshot
	older.GuardVersion--
	older.SnapshotHash = domain.CanonicalGuardSnapshotHash(older)
	if !errors.Is(store.Put(context.Background(), 901, older), ErrGuardSnapshotSuperseded) {
		t.Fatalf("older snapshot was accepted")
	}
	conflicting := snapshot
	conflicting.Rule.RandomKeyVersion++
	conflicting.SnapshotHash = domain.CanonicalGuardSnapshotHash(conflicting)
	if !errors.Is(store.Put(context.Background(), 901, conflicting), ErrGuardSnapshotConflict) {
		t.Fatalf("same-version conflict was accepted")
	}
}

func TestRedisGuardSnapshotStoreConcurrentV3V4NeverRegresses(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 64})
	store, err := NewRedisGuardSnapshotStore(client, "test:theft-guard-race")
	if err != nil {
		t.Fatal(err)
	}
	makeSnapshot := func(version uint64) domain.GuardSnapshot {
		snapshot := domain.GuardSnapshot{SchemaVersion: 1, GuardVersion: version, ActiveGuardPetID: uint32(version),
			GuardUntilMs: time.Now().Add(time.Hour).UnixMilli(), Rule: domain.GuardRule{PetConfigVersion: 1,
				GuardRuleVersion: version, TriggerProbabilityBP: 5000, InterceptPolicy: domain.InterceptPolicyFullGross,
				RandomAlgorithmVersion: 1, RandomKeyVersion: version}}
		snapshot.SnapshotHash = domain.CanonicalGuardSnapshotHash(snapshot)
		return snapshot
	}
	v3, v4 := makeSnapshot(3), makeSnapshot(4)
	for round := uint64(1); round <= 100; round++ {
		farmID := 10_000 + round
		start := make(chan struct{})
		var wg sync.WaitGroup
		for index := 0; index < 32; index++ {
			candidate := v3
			if index%2 == 0 {
				candidate = v4
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := store.Put(context.Background(), farmID, candidate)
				if err != nil && !errors.Is(err, ErrGuardSnapshotSuperseded) {
					t.Errorf("round %d concurrent Put: %v", round, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		got, err := store.ReadParticipantGuardSnapshot(context.Background(), farmID)
		if err != nil || got.GuardVersion != 4 || got.SnapshotHash != v4.SnapshotHash {
			t.Fatalf("round %d final snapshot=%+v err=%v, want v4", round, got, err)
		}
	}
}

func TestRedisResultPublisherUsesExistingResultDirectoryAndBindsPairWorkAddress(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, err := rediskey.NewNamespace("test", "theft-result-directory")
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewRedisResultPublisher(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	pairID, err := identity.CanonicalFriendRelationID(10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: 10001,
		StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: "theft-result-directory-operation",
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID}}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256([]byte("theft-result-directory-payload"))
	digest, err := key.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	publication := theft.ResultPublication{WorkflowID: workflowID, OperationID: "theft-result-directory-operation",
		OperationDigest: digest, SubjectPlayerID: 10001, PairID: pairID,
		WorkAddress: workidentity.WorkAddress{WorkID: workflowID, WorkType: string(identity.WorkTypeTheft),
			PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: 7},
		PayloadHash: payloadHash[:], Outcome: &publicpb.StoredOperationOutcome{OriginalCommandType: commonpb.CommandType_THEFT,
			OriginalOperationId: "theft-result-directory-operation", Status: commonpb.CommandStatus_SUCCEEDED,
			FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: 20002}, ResultPayload: []byte("result-one")}}
	if err := publisher.Publish(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), publication); err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	directory, err := redisauthority.NewResultDirectoryStore(client, nil, namespace, 0)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := directory.LookupResult(context.Background(), publication.SubjectPlayerID, publication.OperationDigest)
	if err != nil || !found || entry.RootKind != "FRIEND_PAIR" || entry.RootID != pairID ||
		entry.CommandType != "THEFT" || entry.SourceRef != resultWorkAddress(publication.WorkAddress) {
		t.Fatalf("ResultDirectory entry=%+v found=%v err=%v", entry, found, err)
	}
	publication.Outcome.ResultPayload = []byte("result-two")
	if err := publisher.Publish(context.Background(), publication); err == nil {
		t.Fatal("same operation digest with different terminal outcome did not conflict")
	}
}

func TestRedisResultPublisherPublishesAllLegalTerminalStatuses(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, err := rediskey.NewNamespace("test", "theft-all-terminal-results")
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewRedisResultPublisher(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := redisauthority.NewResultDirectoryStore(client, nil, namespace, 0)
	if err != nil {
		t.Fatal(err)
	}
	pairID, _ := identity.CanonicalFriendRelationID(61001, 62002)
	for index, status := range []commonpb.CommandStatus{commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_NO_CHANGE, commonpb.CommandStatus_REJECTED} {
		operationID := fmt.Sprintf("terminal-status-%d", index)
		key := identity.EffectiveIdempotencyKey{SubjectPlayerID: 61001, StableCommandTypeName: "THEFT", OperationID: operationID,
			Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID}}
		digest, _ := key.OperationDigest()
		workflowID, _ := identity.WorkflowID(key)
		payloadHash := sha256.Sum256([]byte("payload-" + operationID))
		outcome := &publicpb.StoredOperationOutcome{OriginalCommandType: commonpb.CommandType_THEFT,
			OriginalOperationId: operationID, Status: status,
			FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: 62002}}
		publication := theft.ResultPublication{WorkflowID: workflowID, OperationID: operationID, OperationDigest: digest,
			SubjectPlayerID: 61001, PairID: pairID, PayloadHash: payloadHash[:], Outcome: outcome,
			WorkAddress: workidentity.WorkAddress{WorkID: workflowID, WorkType: "THEFT",
				PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: 1, BucketID: 7}}
		if status == commonpb.CommandStatus_NO_CHANGE {
			publication.WorkflowID = ""
			publication.WorkAddress = workidentity.WorkAddress{}
			publication.SourceRef = "pair-result-v1:" + digest
		}
		if err := publisher.Publish(context.Background(), publication); err != nil {
			t.Fatalf("publish %s: %v", status, err)
		}
		entry, found, err := directory.LookupResult(context.Background(), 61001, digest)
		if err != nil || !found || entry.FinalStatusClass != status.String() {
			t.Fatalf("entry status=%s entry=%+v found=%v err=%v", status, entry, found, err)
		}
		binding, err := redisauthority.DecodeResultBinding(entry.MinimalOutcome)
		if err != nil || binding.SubjectPlayerID != 61001 || binding.OperationID != operationID ||
			!bytes.Equal(binding.PayloadHash, payloadHash[:]) {
			t.Fatalf("binding=%+v err=%v", binding, err)
		}
		stored := &publicpb.StoredOperationOutcome{}
		if err := proto.Unmarshal(binding.Outcome, stored); err != nil || stored.GetStatus() != status {
			t.Fatalf("stored=%v err=%v", stored, err)
		}
	}
}

type retryResultPublisher struct {
	calls int
	fail  bool
}

func (p *retryResultPublisher) Publish(_ context.Context, publication theft.ResultPublication) error {
	p.calls++
	if p.fail {
		p.fail = false
		return errors.New("projection unavailable")
	}
	if publication.Outcome == nil || publication.OperationDigest == "" || len(publication.PayloadHash) != sha256.Size ||
		publication.SourceRef != "pair-result-v1:"+publication.OperationDigest {
		return errors.New("invalid publication")
	}
	return nil
}

func TestResultOrderProcessorFinalizesNoChangeWithoutSourceOrder(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, _ := rediskey.NewNamespace("test", "theft-result-processor-no-change")
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	const visitor, owner = uint64(73001), uint64(74002)
	const operationID = "processor-no-change-operation"
	pairID, _ := friend.RelationIDFor(visitor, owner)
	activationHash := sha256.Sum256([]byte("processor-no-change-activate"))
	if outcome, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "processor-no-change-token", TokenGeneration: 1, OperationID: "processor-no-change-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "processor-no-change-activate", ParticipantStep: "ACTIVATE"}, activationHash[:]); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	payloadHash := sha256.Sum256([]byte("processor-no-change-payload"))
	targetHash := sha256.Sum256([]byte("processor-no-change-target"))
	admission, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash[:], PermitTTLMs: 100, WorkflowTTLMs: 60_000,
	})
	if err != nil || admission.Outcome.Status != friend.StatusSucceeded {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.LookupPairActionResult(context.Background(), visitor, owner, operationID)
	if err != nil || !found {
		t.Fatalf("binding=%+v found=%v err=%v", binding, found, err)
	}
	publisher := &retryResultPublisher{}
	processor, err := NewResultOrderProcessor(repositories, publisher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.FinalizeNoChange(context.Background(), theftrecovery.DispatchOrder{
		Work: theftrecovery.WorkAddress{WorkID: binding.DispatchOrderID}, OperationID: operationID,
		VisitorPlayerID: visitor, OwnerPlayerID: owner,
	}); err != nil {
		t.Fatal(err)
	}
	terminal, found, err := repositories.LookupPairActionResult(context.Background(), visitor, owner, operationID)
	if err != nil || !found || terminal.ResultState != "NO_CHANGE" {
		t.Fatalf("terminal=%+v found=%v err=%v", terminal, found, err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls=%d", publisher.calls)
	}
	completed, err := processor.ProcessResultOrder(context.Background(), theftrecovery.DispatchCandidate{BucketID: binding.BucketID, Member: binding.ResultWorkRef})
	if err != nil || !completed || publisher.calls != 1 {
		t.Fatalf("stale result candidate completed=%v err=%v calls=%d", completed, err, publisher.calls)
	}
}

func TestResultOrderProcessorRetriesPublisherAndCompletesIdempotently(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, _ := rediskey.NewNamespace("test", "theft-result-processor-retry")
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256([]byte("processor-retry-payload"))
	targetHash := sha256.Sum256([]byte("processor-retry-target"))
	const visitor, owner = uint64(71001), uint64(72002)
	const operationID = "processor-retry-operation"
	pairID, _ := friend.RelationIDFor(visitor, owner)
	activationHash := sha256.Sum256([]byte("processor-retry-activate"))
	if outcome, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1, TokenID: "processor-retry-token",
		TokenGeneration: 1, OperationID: "processor-retry-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "processor-retry-activate", ParticipantStep: "ACTIVATE"}, activationHash[:]); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	blockHash := sha256.Sum256([]byte("processor-retry-block"))
	if outcome, err := repositories.Pair.BlockPlayer(context.Background(), friend.PairMutationCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "BLOCK_PLAYER", OperationID: "processor-retry-block"},
		PayloadHash: blockHash[:], CallerPlayerID: visitor, PeerPlayerID: owner,
	}); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("block=%+v err=%v", outcome, err)
	}
	admission, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash[:], PermitTTLMs: 100, WorkflowTTLMs: 60_000,
	})
	if err != nil || admission.Outcome.Status != friend.StatusRejected {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.LookupPairActionResult(context.Background(), visitor, owner, operationID)
	if err != nil || !found {
		t.Fatalf("binding=%+v found=%v err=%v", binding, found, err)
	}
	publisher := &retryResultPublisher{fail: true}
	processor, err := NewResultOrderProcessor(repositories, publisher, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate := theftrecovery.DispatchCandidate{BucketID: binding.BucketID, Member: binding.ResultWorkRef}
	if completed, err := processor.ProcessResultOrder(context.Background(), candidate); err == nil || completed {
		t.Fatalf("first completion=%v err=%v", completed, err)
	}
	if completed, err := processor.ProcessResultOrder(context.Background(), candidate); err != nil || !completed {
		t.Fatalf("retry completion=%v err=%v", completed, err)
	}
	if completed, err := processor.ProcessResultOrder(context.Background(), candidate); err != nil || !completed {
		t.Fatalf("idempotent completion=%v err=%v", completed, err)
	}
	if publisher.calls != 2 {
		t.Fatalf("publisher calls=%d", publisher.calls)
	}
}
