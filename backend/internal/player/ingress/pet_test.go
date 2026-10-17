package ingress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/dirtywriteback"
	playeractor "farmgame/internal/player/actor"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/guardrecovery"

	"google.golang.org/protobuf/proto"
)

const (
	petAuthorityPlayerID = uint64(9)
	petAuthorityNowMs    = int64(1_770_300_000_000)
)

type petAuthorityRig struct {
	service  *Service
	registry *playeractor.Registry
	fault    *authoritystore.FaultStore
}

func newPetAuthorityRig(t *testing.T) *petAuthorityRig {
	return newPetAuthorityRigWithSoftSave(t, false)
}

func newPetDirtyAuthorityRig(t *testing.T) *petAuthorityRig {
	return newPetAuthorityRigWithSoftSave(t, true)
}

func newPetAuthorityRigWithSoftSave(t *testing.T, enableSoftSave bool) *petAuthorityRig {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040021},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return time.UnixMilli(petAuthorityNowMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, petAuthorityPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	authorityRecord := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1",
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           shard,
		HolderID:               "pet-holder",
		HolderBootID:           "pet-holder-boot",
		AssignmentRevision:     1,
		WriterEpoch:            1,
		LeaseEpoch:             1,
		State:                  authoritystore.ShardAuthorityActive,
		LeaseUntilMs:           petAuthorityNowMs + int64(time.Minute/time.Millisecond),
	}
	if err := store.SetShardWriteAuthority(authorityRecord); err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           authorityRecord.HolderID,
		ExpectedHolderBootID:       authorityRecord.HolderBootID,
		ExpectedAssignmentRevision: authorityRecord.AssignmentRevision,
		ExpectedWriterEpoch:        authorityRecord.WriterEpoch,
		ExpectedLeaseEpoch:         authorityRecord.LeaseEpoch,
	}
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  fault,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "pet-holder-boot",
		EnableSoftSave:         enableSoftSave,
	})
	if err != nil {
		t.Fatal(err)
	}
	registryOptions := playeractor.RegistryOptions{MailboxSize: 8}
	if enableSoftSave {
		// Keep the actor dirty for the whole test. The assertion below is about
		// command-path Redis commits; Registry.Stop still performs the normal
		// bounded drain during cleanup.
		registryOptions.DirtyWriteback = dirtywriteback.Options{
			FlushInterval: time.Hour,
			MaxDirtyAge:   time.Hour,
			RetryDelay:    time.Second,
			FlushTimeout:  time.Second,
			Workers:       1,
			QueueSize:     16,
		}
	}
	registry := playeractor.NewAuthorityRegistry(provider, registryOptions)
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), petAuthorityPlayerID, "pet-bootstrap"); err != nil {
		t.Fatal(err)
	}
	service := NewService(registry)
	service.nowMs = func() int64 { return petAuthorityNowMs }
	return &petAuthorityRig{service: service, registry: registry, fault: fault}
}

type petGuardDispatchStub struct {
	mu        sync.Mutex
	failCount int
	calls     []authoritystore.AttachedOrder
}

func (s *petGuardDispatchStub) Dispatch(_ context.Context, order authoritystore.AttachedOrder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	order.Payload = append([]byte(nil), order.Payload...)
	s.calls = append(s.calls, order)
	if s.failCount > 0 {
		s.failCount--
		return errors.New("injected GuardSync transport failure")
	}
	return nil
}

func (s *petGuardDispatchStub) snapshot() []authoritystore.AttachedOrder {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]authoritystore.AttachedOrder, len(s.calls))
	copy(out, s.calls)
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
	}
	return out
}

func petReadRecord(t *testing.T, rig *petAuthorityRig) (*playerpb.PlayerRecord, uint64) {
	t.Helper()
	record, version, err := rig.registry.Read(context.Background(), petAuthorityPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	return record, version
}

func petRequireStatus(t *testing.T, outcome *logicrpcpb.BusinessCommandOutcome, want commonpb.CommandStatus) {
	t.Helper()
	if outcome == nil || outcome.GetStatus() != want {
		t.Fatalf("status=%v error=%v, want %v", outcome.GetStatus(), outcome.GetError(), want)
	}
}

func petRequireDurableCommit(t *testing.T, rig *petAuthorityRig, beforeVersion uint64, outcome *logicrpcpb.BusinessCommandOutcome, wantStatus authoritystore.TerminalStatus) authoritystore.AuthorityCommitRequest {
	t.Helper()
	request, ok := rig.fault.LastCommitRequest()
	if !ok {
		t.Fatal("durable command did not issue an authority commit")
	}
	if request.ExpectedRecordVersion != beforeVersion {
		t.Fatalf("expected record version=%d, want %d", request.ExpectedRecordVersion, beforeVersion)
	}
	if request.OperationResult.Status != wantStatus {
		t.Fatalf("stored operation status=%v, want %v", request.OperationResult.Status, wantStatus)
	}
	if !bytes.Equal(request.OperationResult.Payload, outcome.GetResultPayload()) {
		t.Fatal("stored operation payload differs from public result payload")
	}
	if len(request.CandidateState) == 0 {
		t.Fatal("durable command commit did not carry PlayerRecord candidate state")
	}
	_, afterVersion := petReadRecord(t, rig)
	if afterVersion != beforeVersion+1 {
		t.Fatalf("record version=%d, want %d", afterVersion, beforeVersion+1)
	}
	return request
}

func petRequireGuardOrder(t *testing.T, request authoritystore.AuthorityCommitRequest, playerID uint64, guardVersion uint64) authoritystore.AttachedOrder {
	t.Helper()
	if len(request.AttachedOrders) != 1 {
		t.Fatalf("attached orders=%d, want 1", len(request.AttachedOrders))
	}
	order := request.AttachedOrders[0]
	if order.OrderType != "GUARD_SYNC_ORDER" || order.SubjectPlayerID != playerID {
		t.Fatalf("order identity=%+v", order)
	}
	expectedID, err := identity.SourceOrderID(
		identity.WorkTypeGuardSyncOrder,
		identity.SourceKindPlayerActor,
		"9",
		"PET_GUARD_SLOT",
		guardVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if order.OrderID != expectedID {
		t.Fatalf("order id=%q, want %q", order.OrderID, expectedID)
	}
	var payload contractspb.ApplyGuardSnapshotPayload
	if err := proto.Unmarshal(order.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.GetOrderId() != order.OrderID ||
		payload.GetSourcePlayerId() != playerID ||
		payload.GetTargetFarmId() != playerID ||
		payload.GetGuardSnapshot().GetGuardVersion() != guardVersion {
		t.Fatalf("guard payload=%+v", &payload)
	}
	if !bytes.Equal(payload.GetSnapshotHash(), payload.GetGuardSnapshot().GetSnapshotHash()) ||
		len(payload.GetSnapshotHash()) != 32 {
		t.Fatalf("guard snapshot hash is not duplicated canonically")
	}
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		t.Fatal(err)
	}
	canonicalHash, err := contracts.CanonicalParticipantHash(spec, &payload)
	if err != nil {
		t.Fatal(err)
	}
	var wantHash authoritystore.PayloadHash
	copy(wantHash[:], canonicalHash[:])
	if order.PayloadHash != wantHash {
		t.Fatalf("payload hash=%x, want canonical participant hash %x", order.PayloadHash, wantHash)
	}
	// 到这里为止验的都是生产侧自己:同一个 CanonicalParticipantHash 算两遍。
	// 真正的接缝在消费侧——GUARD_SYNC worker 会用严格解码器复核大小上限、未知
	// 字段和「确定性序列化后逐字节相等」。这三条里任何一条不满足,worker 都会
	// 把订单判成 bad payload 一直 reschedule:狗喂了、guard_until 也涨了,农场
	// 却永远收不到快照,而且没有任何一处报错。所以用 worker 自己的解码器验一遍
	// 玩家真实产出的字节。
	decoded, err := guardrecovery.DecodeApplyGuardSnapshotPayload(order.OrderID, order.Payload)
	if err != nil {
		t.Fatalf("GUARD_SYNC worker rejects the order this command produced: %v", err)
	}
	if decoded.GetGuardSnapshot().GetGuardVersion() != guardVersion {
		t.Fatalf("worker decoded guard version=%d, want %d", decoded.GetGuardSnapshot().GetGuardVersion(), guardVersion)
	}
	if got := guardrecovery.ApplyGuardSnapshotPayloadHash(order.Payload); got != fmt.Sprintf("%x", wantHash[:]) {
		t.Fatalf("worker completion-proof hash=%s, want %x", got, wantHash[:])
	}
	return order
}

func TestPetGetStateListsVersionedCatalog(t *testing.T) {
	rig := newPetAuthorityRig(t)
	commitCalls := rig.fault.Calls(authoritystore.MethodCommit)
	outcome := rig.service.executeForTest(t, context.Background(),
		playerCommandEnvelope(t, "pet-get-1", commonpb.CommandType_GET_PET_STATE, &publicpb.GetPetStateRequest{}))
	petRequireStatus(t, outcome, commonpb.CommandStatus_SUCCEEDED)
	if rig.fault.Calls(authoritystore.MethodCommit) != commitCalls {
		t.Fatal("GET_PET_STATE unexpectedly used a durable authority commit")
	}
	var result publicpb.GetPetStateResult
	if err := proto.Unmarshal(outcome.GetResultPayload(), &result); err != nil {
		t.Fatal(err)
	}
	page := result.GetPage()
	if page.GetPetCatalogVersion() != 1 ||
		page.GetFeedRuleVersion() != 1 ||
		page.GetDogFoodItemId() != playerpet.P0DogFoodItemID ||
		page.GetGuardSyncStatus() != publicpb.GuardSyncStatus_GUARD_SYNC_NOT_REQUIRED ||
		len(page.GetPets()) != 2 {
		t.Fatalf("pet page=%+v", page)
	}
	if page.GetState().GetPetStateVersion() != 0 || len(page.GetState().GetOwnedPetIds()) != 0 {
		t.Fatalf("initial pet state=%+v", page.GetState())
	}
	var dog *publicpb.PetCatalogItemView
	for _, item := range page.GetPets() {
		if item.GetPetId() == 1 {
			dog = item
			break
		}
	}
	if dog == nil || dog.GetState() != publicpb.PetDisplayState_PET_LOCKED ||
		!dog.GetCoinUnlockEnabled() || dog.GetCoinPrice() != 100 || dog.GetPriceVersion() != 1 {
		t.Fatalf("dog catalog item=%+v", dog)
	}
}

func TestPetUnlockFeedSwitchAndDurableGuardOrder(t *testing.T) {
	rig := newPetAuthorityRig(t)
	ctx := context.Background()

	unlockCommand := playerCommandEnvelope(t, "pet-unlock-1", commonpb.CommandType_PET_UNLOCK_BY_COIN,
		&publicpb.PetUnlockByCoinRequest{
			PetId: 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		})
	_, beforeUnlock := petReadRecord(t, rig)
	unlock := rig.service.executeForTest(t, ctx, unlockCommand)
	petRequireStatus(t, unlock, commonpb.CommandStatus_SUCCEEDED)
	var unlockResult publicpb.PetUnlockByCoinResult
	if err := proto.Unmarshal(unlock.GetResultPayload(), &unlockResult); err != nil {
		t.Fatal(err)
	}
	if unlockResult.GetMutation().GetCoinDelta() != -100 ||
		unlockResult.GetMutation().GetGuardSyncRequired() {
		t.Fatalf("unlock mutation=%+v", unlockResult.GetMutation())
	}
	unlockCommit := petRequireDurableCommit(t, rig, beforeUnlock, unlock, authoritystore.TerminalSucceeded)
	if len(unlockCommit.AttachedOrders) != 0 {
		t.Fatalf("unlock attached orders=%+v", unlockCommit.AttachedOrders)
	}
	record, unlockVersion := petReadRecord(t, rig)
	if record.GetWallet().GetCoin() != 0 {
		t.Fatalf("coin=%d, want 0", record.GetWallet().GetCoin())
	}
	state, err := playerpet.StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Owns(1) || state.PetStateVersion != 1 {
		t.Fatalf("unlocked state=%+v", state)
	}

	commitCalls := rig.fault.Calls(authoritystore.MethodCommit)
	replayedUnlock := rig.service.executeForTest(t, ctx, unlockCommand)
	petRequireStatus(t, replayedUnlock, commonpb.CommandStatus_SUCCEEDED)
	if !bytes.Equal(unlock.GetResultPayload(), replayedUnlock.GetResultPayload()) ||
		rig.fault.Calls(authoritystore.MethodCommit) != commitCalls {
		t.Fatal("same operation replay was not stable and commit-free")
	}
	_, replayVersion := petReadRecord(t, rig)
	if replayVersion != unlockVersion {
		t.Fatalf("replay version=%d, want %d", replayVersion, unlockVersion)
	}

	switchCommand := playerCommandEnvelope(t, "pet-switch-1", commonpb.CommandType_PET_SWITCH,
		&publicpb.PetSwitchRequest{
			TargetPetId: 1, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		})
	_, beforeSwitch := petReadRecord(t, rig)
	switched := rig.service.executeForTest(t, ctx, switchCommand)
	petRequireStatus(t, switched, commonpb.CommandStatus_SUCCEEDED)
	var switchResult publicpb.PetSwitchResult
	if err := proto.Unmarshal(switched.GetResultPayload(), &switchResult); err != nil {
		t.Fatal(err)
	}
	if !switchResult.GetMutation().GetGuardSyncRequired() ||
		switchResult.GetMutation().GetGuardSyncOrderId() == "" {
		t.Fatalf("switch mutation=%+v", switchResult.GetMutation())
	}
	switchCommit := petRequireDurableCommit(t, rig, beforeSwitch, switched, authoritystore.TerminalSucceeded)
	petRequireGuardOrder(t, switchCommit, petAuthorityPlayerID, 1)
	record, switchVersion := petReadRecord(t, rig)
	state, err = playerpet.StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveGuardPetID != 1 || state.GuardVersion != 1 || switchVersion != beforeSwitch+1 {
		t.Fatalf("switched state=%+v version=%d", state, switchVersion)
	}

	noChangeSwitch := rig.service.executeForTest(t, ctx, playerCommandEnvelope(t, "pet-switch-2",
		commonpb.CommandType_PET_SWITCH, &publicpb.PetSwitchRequest{
			TargetPetId: 1, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		}))
	petRequireStatus(t, noChangeSwitch, commonpb.CommandStatus_NO_CHANGE)
	noChangeCommit := petRequireDurableCommit(t, rig, switchVersion, noChangeSwitch, authoritystore.TerminalNoChange)
	if len(noChangeCommit.AttachedOrders) != 0 {
		t.Fatalf("switch NO_CHANGE attached orders=%+v", noChangeCommit.AttachedOrders)
	}

	if _, err := rig.registry.Submit(ctx, petAuthorityPlayerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
		if !playeractor.AddItem(record, playerpet.P0DogFoodItemID, 1) {
			return nil, false, errors.New("failed to seed dog food")
		}
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	feedCommand := playerCommandEnvelope(t, "pet-feed-1", commonpb.CommandType_PET_FEED,
		&publicpb.PetFeedRequest{
			Quantity: 1, ExpectedFeedRuleVersion: 1,
			ExpectedActivePetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		})
	_, beforeFeed := petReadRecord(t, rig)
	feed := rig.service.executeForTest(t, ctx, feedCommand)
	petRequireStatus(t, feed, commonpb.CommandStatus_SUCCEEDED)
	var feedResult publicpb.PetFeedResult
	if err := proto.Unmarshal(feed.GetResultPayload(), &feedResult); err != nil {
		t.Fatal(err)
	}
	if feedResult.GetMutation().GetItemId() != playerpet.P0DogFoodItemID ||
		feedResult.GetMutation().GetItemDelta() != -1 ||
		!feedResult.GetMutation().GetGuardSyncRequired() {
		t.Fatalf("feed mutation=%+v", feedResult.GetMutation())
	}
	feedCommit := petRequireDurableCommit(t, rig, beforeFeed, feed, authoritystore.TerminalSucceeded)
	petRequireGuardOrder(t, feedCommit, petAuthorityPlayerID, 2)
	record, _ = petReadRecord(t, rig)
	state, err = playerpet.StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if playeractor.ItemCount(record, playerpet.P0DogFoodItemID) != 0 ||
		state.ActiveGuardPetID != 1 || state.GuardVersion != 2 ||
		state.GuardUntilMs != petAuthorityNowMs+8*60*60*1000 {
		t.Fatalf("fed state=%+v items=%d", state, playeractor.ItemCount(record, playerpet.P0DogFoodItemID))
	}
}

func TestPetDirtyGuardSyncFailureRetriesSameOperationWithoutDoubleConsume(t *testing.T) {
	rig := newPetDirtyAuthorityRig(t)
	ctx := context.Background()
	dispatch := &petGuardDispatchStub{}
	if err := rig.service.SetBestEffortGuardSyncDispatcher(dispatch); err != nil {
		t.Fatal(err)
	}
	baselineCommits := rig.fault.Calls(authoritystore.MethodCommit)

	unlock := rig.service.executeForTest(t, ctx, playerCommandEnvelope(t, "pet-dirty-unlock",
		commonpb.CommandType_PET_UNLOCK_BY_COIN, &publicpb.PetUnlockByCoinRequest{
			PetId: 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		}))
	petRequireStatus(t, unlock, commonpb.CommandStatus_SUCCEEDED)
	switched := rig.service.executeForTest(t, ctx, playerCommandEnvelope(t, "pet-dirty-switch",
		commonpb.CommandType_PET_SWITCH, &publicpb.PetSwitchRequest{
			TargetPetId: 1, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		}))
	petRequireStatus(t, switched, commonpb.CommandStatus_SUCCEEDED)

	if _, err := rig.registry.Submit(ctx, petAuthorityPlayerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
		if !playeractor.AddItem(record, playerpet.P0DogFoodItemID, 1) {
			return nil, false, errors.New("failed to seed dog food")
		}
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	dispatch.mu.Lock()
	dispatch.failCount = 1
	dispatch.mu.Unlock()

	feedCommand := playerCommandEnvelope(t, "pet-dirty-feed", commonpb.CommandType_PET_FEED,
		&publicpb.PetFeedRequest{
			Quantity: 1, ExpectedFeedRuleVersion: 1,
			ExpectedActivePetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		})
	first := rig.service.executeForTest(t, ctx, feedCommand)
	petRequireStatus(t, first, commonpb.CommandStatus_UNAVAILABLE)
	if first.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_STORE_UNAVAILABLE {
		t.Fatalf("first retryable error=%v", first.GetError())
	}
	record, _ := petReadRecord(t, rig)
	state, err := playerpet.StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if playeractor.ItemCount(record, playerpet.P0DogFoodItemID) != 0 || state.GuardVersion != 2 {
		t.Fatalf("first attempt state=%+v food=%d", state, playeractor.ItemCount(record, playerpet.P0DogFoodItemID))
	}

	replayed := rig.service.executeForTest(t, ctx, feedCommand)
	petRequireStatus(t, replayed, commonpb.CommandStatus_SUCCEEDED)
	record, _ = petReadRecord(t, rig)
	state, err = playerpet.StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if playeractor.ItemCount(record, playerpet.P0DogFoodItemID) != 0 || state.GuardVersion != 2 {
		t.Fatalf("replay double-applied mutation: state=%+v food=%d", state, playeractor.ItemCount(record, playerpet.P0DogFoodItemID))
	}

	calls := dispatch.snapshot()
	if len(calls) != 3 {
		t.Fatalf("GuardSync dispatch calls=%d, want switch + failed feed + replayed feed", len(calls))
	}
	failedFeed, repairedFeed := calls[1], calls[2]
	if failedFeed.OperationID != "pet-dirty-feed" || repairedFeed.OperationID != failedFeed.OperationID ||
		failedFeed.OrderID != repairedFeed.OrderID || failedFeed.PayloadHash != repairedFeed.PayloadHash ||
		!bytes.Equal(failedFeed.Payload, repairedFeed.Payload) {
		t.Fatalf("same-operation replay rebuilt a different GuardSync order: first=%+v replay=%+v", failedFeed, repairedFeed)
	}
	if got := rig.fault.Calls(authoritystore.MethodCommit); got != baselineCommits {
		t.Fatalf("dirty command path issued Redis authority commits: got=%d baseline=%d", got, baselineCommits)
	}
	if stats := rig.registry.DirtyWritebackStats(); stats.Marks == 0 || stats.QueueDepth == 0 {
		t.Fatalf("dirty scheduler was not marked: %+v", stats)
	}
}

func TestPetDifferentOperationNoChangeAndRejectedReplayAreDurable(t *testing.T) {
	rig := newPetAuthorityRig(t)
	ctx := context.Background()
	unlock := playerCommandEnvelope(t, "pet-seed-unlock", commonpb.CommandType_PET_UNLOCK_BY_COIN,
		&publicpb.PetUnlockByCoinRequest{
			PetId: 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		})
	first := rig.service.executeForTest(t, ctx, unlock)
	petRequireStatus(t, first, commonpb.CommandStatus_SUCCEEDED)

	noChangeCommand := playerCommandEnvelope(t, "pet-new-operation-no-change",
		commonpb.CommandType_PET_UNLOCK_BY_COIN, &publicpb.PetUnlockByCoinRequest{
			PetId: 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		})
	_, beforeNoChange := petReadRecord(t, rig)
	noChange := rig.service.executeForTest(t, ctx, noChangeCommand)
	petRequireStatus(t, noChange, commonpb.CommandStatus_NO_CHANGE)
	noChangeCommit := petRequireDurableCommit(t, rig, beforeNoChange, noChange, authoritystore.TerminalNoChange)
	if len(noChangeCommit.AttachedOrders) != 0 {
		t.Fatalf("unlock NO_CHANGE attached orders=%+v", noChangeCommit.AttachedOrders)
	}
	commitCalls := rig.fault.Calls(authoritystore.MethodCommit)
	_, noChangeVersion := petReadRecord(t, rig)
	replayedNoChange := rig.service.executeForTest(t, ctx, noChangeCommand)
	petRequireStatus(t, replayedNoChange, commonpb.CommandStatus_NO_CHANGE)
	if !bytes.Equal(noChange.GetResultPayload(), replayedNoChange.GetResultPayload()) ||
		rig.fault.Calls(authoritystore.MethodCommit) != commitCalls {
		t.Fatal("NO_CHANGE replay was not stable and commit-free")
	}
	_, replayVersion := petReadRecord(t, rig)
	if replayVersion != noChangeVersion {
		t.Fatalf("NO_CHANGE replay version=%d, want %d", replayVersion, noChangeVersion)
	}

	rejectedCommand := playerCommandEnvelope(t, "pet-new-operation-rejected",
		commonpb.CommandType_PET_UNLOCK_BY_COIN, &publicpb.PetUnlockByCoinRequest{
			PetId: 2, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		})
	_, beforeRejected := petReadRecord(t, rig)
	rejected := rig.service.executeForTest(t, ctx, rejectedCommand)
	petRequireStatus(t, rejected, commonpb.CommandStatus_REJECTED)
	if rejected.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_COIN {
		t.Fatalf("rejected error=%v", rejected.GetError())
	}
	rejectedCommit := petRequireDurableCommit(t, rig, beforeRejected, rejected, authoritystore.TerminalRejected)
	if rejectedCommit.OperationResult.ErrorCode != commonpb.ErrorCode_ERR_INSUFFICIENT_COIN.String() {
		t.Fatalf("stored rejection error=%q", rejectedCommit.OperationResult.ErrorCode)
	}
	if len(rejectedCommit.AttachedOrders) != 0 {
		t.Fatalf("rejected attached orders=%+v", rejectedCommit.AttachedOrders)
	}
	commitCalls = rig.fault.Calls(authoritystore.MethodCommit)
	_, rejectedVersion := petReadRecord(t, rig)
	replayedRejected := rig.service.executeForTest(t, ctx, rejectedCommand)
	petRequireStatus(t, replayedRejected, commonpb.CommandStatus_REJECTED)
	if replayedRejected.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_COIN ||
		!bytes.Equal(rejected.GetResultPayload(), replayedRejected.GetResultPayload()) ||
		rig.fault.Calls(authoritystore.MethodCommit) != commitCalls {
		t.Fatal("REJECTED replay was not stable and commit-free")
	}
	_, finalVersion := petReadRecord(t, rig)
	if finalVersion != rejectedVersion {
		t.Fatalf("REJECTED replay version=%d, want %d", finalVersion, rejectedVersion)
	}
}
