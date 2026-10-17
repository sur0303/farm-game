package participant

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	plantTestPlayerID           uint64 = 9001
	plantTestLayout                    = "v1"
	plantTestHolder                    = "participant-test-holder"
	plantTestHolderBoot                = "participant-test-boot"
	participantTestLeaseUntilMs uint64 = 1_900_000_000_000
)

var plantTestNow = time.UnixMilli(1_780_000_000_000)

type participantTestRig struct {
	name     string
	registry *playeractor.Registry
	provider *authority.Provider
	fault    *authoritystore.FaultStore
	capacity *participantCapacityGate
	base     authoritystore.ActorAuthorityStore
	memory   *authoritystore.MemoryStore
	calc     *actorrouting.Calculator

	reassign func(t *testing.T, holder string) authoritystore.ShardWriteFence
}

// participantCapacityGate models a Store/Lua ErrCapacityExceeded after the
// admission snapshot has passed. The production Participant boundary must
// normalize this raw authority sentinel and the failed commit must leave no
// PlayerRecord mutation.
type participantCapacityGate struct {
	authoritystore.ActorAuthorityStore
	fail atomic.Bool
}

type physicalUsageFaultMode uint8

const (
	physicalUsageUnavailable physicalUsageFaultMode = iota + 1
	physicalUsageStale
)

// physicalUsageFaultStore preserves the concrete Store limits/projector while
// corrupting only its version-bound aggregate usage view. Participant
// admission must fail closed instead of reconstructing outer receipt usage
// from PlayerRecord.RecentOperationReceipts.
type physicalUsageFaultStore struct {
	authoritystore.ActorAuthorityStore
	mode          physicalUsageFaultMode
	corruptLoadAt int32
	loadCalls     atomic.Int32
}

func (s *physicalUsageFaultStore) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCapabilitiesProvider).PhysicalCapabilities()
}

func (s *physicalUsageFaultStore) ProjectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCommitUsageProjector).ProjectPhysicalCommit(req)
}

func (s *physicalUsageFaultStore) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	result, err := s.ActorAuthorityStore.LoadAndClaim(ctx, req)
	if call := s.loadCalls.Add(1); s.corruptLoadAt == 0 || call >= s.corruptLoadAt {
		result.Snapshot.PhysicalUsage = s.corrupt(result.Snapshot.PhysicalUsage)
	}
	return result, err
}

func (s *physicalUsageFaultStore) Commit(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	result, err := s.ActorAuthorityStore.Commit(ctx, req)
	result.Snapshot.PhysicalUsage = s.corrupt(result.Snapshot.PhysicalUsage)
	return result, err
}

func (s *physicalUsageFaultStore) FenceAndClaim(ctx context.Context, req authoritystore.FenceAndClaimRequest) (authoritystore.FenceAndClaimResult, error) {
	result, err := s.ActorAuthorityStore.FenceAndClaim(ctx, req)
	result.Snapshot.PhysicalUsage = s.corrupt(result.Snapshot.PhysicalUsage)
	return result, err
}

func (s *physicalUsageFaultStore) corrupt(usage authoritystore.PhysicalUsage) authoritystore.PhysicalUsage {
	if s.mode == physicalUsageUnavailable {
		return authoritystore.PhysicalUsage{}
	}
	if usage.RecordVersion > 0 {
		usage.RecordVersion--
	}
	return usage
}

func (s *participantCapacityGate) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	provider, ok := s.ActorAuthorityStore.(authoritystore.PhysicalCapabilitiesProvider)
	if !ok {
		return authoritystore.PhysicalCapabilities{}, authoritystore.ErrInvalidRequest
	}
	return provider.PhysicalCapabilities()
}

func (s *participantCapacityGate) ProjectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	projector, ok := s.ActorAuthorityStore.(authoritystore.PhysicalCommitUsageProjector)
	if !ok {
		return authoritystore.PhysicalCommitUsage{}, authoritystore.ErrInvalidRequest
	}
	return projector.ProjectPhysicalCommit(req)
}

func (s *participantCapacityGate) Commit(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	if s.fail.Load() {
		return authoritystore.CommitResult{}, authoritystore.ErrCapacityExceeded
	}
	return s.ActorAuthorityStore.Commit(ctx, req)
}

func TestPlantHappyReplayAndRealRecordVersion(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			server := newAllowServer(trig.registry)
			workID := plantWorkID(t, "happy-"+backend)

			reserve := reserveRequest(t, workID, plantTestPlayerID, false)
			beforeFirst := trig.registry.RuntimeStats().Submitted
			first := mustExecute(t, server.ExecuteStep, reserve)
			if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || first.GetReplayedFromReceipt() {
				t.Fatalf("reserve first=%+v", first)
			}
			if afterFirst := trig.registry.RuntimeStats().Submitted; afterFirst != beforeFirst+1 {
				t.Fatalf("normal valid work did not reach exactly one actor turn: submitted %d -> %d", beforeFirst, afterFirst)
			}
			reserveResult := &contractspb.ReserveSeedResult{}
			mustUnmarshal(t, first.GetTypedResult(), reserveResult)
			if reserveResult.GetPlayerRecordVersion() != first.GetCommittedRecordVersion() || reserveResult.GetAvailableSeedQuantityAfter() != 0 {
				t.Fatalf("reserve version/result=%d/%d available=%d", reserveResult.GetPlayerRecordVersion(), first.GetCommittedRecordVersion(), reserveResult.GetAvailableSeedQuantityAfter())
			}

			replayed := mustExecute(t, server.ExecuteStep, reserve)
			if !replayed.GetReplayedFromReceipt() || replayed.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() || !bytes.Equal(replayed.GetTypedResult(), first.GetTypedResult()) {
				t.Fatalf("reserve replay=%+v first=%+v", replayed, first)
			}

			// Rebuild the authority Registry after the first commit. The typed
			// handler must find the same receipt in PlayerRecord, not a process map.
			if trig.provider != nil {
				trig.registry.Stop()
				trig.registry = playeractor.NewAuthorityRegistry(trig.provider, playeractor.RegistryOptions{MailboxSize: 8})
				t.Cleanup(trig.registry.Stop)
				server = newAllowServer(trig.registry)
				coldReplay := mustExecute(t, server.ExecuteStep, reserve)
				if !coldReplay.GetReplayedFromReceipt() || coldReplay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() ||
					!bytes.Equal(coldReplay.GetTypedResult(), first.GetTypedResult()) ||
					!bytes.Equal(coldReplay.GetParticipantResultHash(), first.GetParticipantResultHash()) {
					t.Fatalf("cold replay=%+v", coldReplay)
				}
			}

			commit := commitRequest(t, workID, plantTestPlayerID, false)
			commitResponse := mustExecute(t, server.ExecuteStep, commit)
			if commitResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || commitResponse.GetReplayedFromReceipt() {
				t.Fatalf("commit=%+v", commitResponse)
			}
			commitResult := &contractspb.CommitSeedReservationResult{}
			mustUnmarshal(t, commitResponse.GetTypedResult(), commitResult)
			if commitResult.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED ||
				commitResult.GetAvailableSeedQuantity() != 0 || commitResult.GetPlayerRecordVersion() != commitResponse.GetCommittedRecordVersion() {
				t.Fatalf("commit result=%+v response=%+v", commitResult, commitResponse)
			}

			workerReplay := mustExecute(t, server.ResumeStep, commitRequest(t, workID, plantTestPlayerID, true))
			if !workerReplay.GetReplayedFromReceipt() || workerReplay.GetCommittedRecordVersion() != commitResponse.GetCommittedRecordVersion() {
				t.Fatalf("worker replay=%+v", workerReplay)
			}
			record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if version != commitResponse.GetCommittedRecordVersion() || playeractor.ItemCount(record, playeractor.ItemCarrotSeed) != 0 {
				t.Fatalf("record version=%d response=%d seeds=%d", version, commitResponse.GetCommittedRecordVersion(), playeractor.ItemCount(record, playeractor.ItemCarrotSeed))
			}
			reservation := playeractorReservation(record, mustSeedReservation(t, workID))
			if reservation == nil || reservation.GetState() != playerpb.ReservationState_COMMITTED {
				t.Fatalf("reservation=%v", reservation)
			}
		})
	}
}

func TestPlantDurableResultProofFirstCommitReplayCollisionAndReload(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	seedPlayer(t, trig.registry, plantTestPlayerID, 1)
	server := newAllowServer(trig.registry)
	request := reserveRequest(t, plantWorkID(t, "durable-result-proof"), plantTestPlayerID, false)
	first := mustExecute(t, server.ExecuteStep, request)
	if len(first.GetParticipantResultHash()) != 32 {
		t.Fatalf("first commit result hash length=%d", len(first.GetParticipantResultHash()))
	}

	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	record := mustReadRecord(t, trig.registry)
	receipt := playeringress.FindParticipantOperationReceipt(record, call.InvocationID)
	if receipt == nil || len(receipt.GetResultPayload()) < participantResultProofFixedBytes ||
		!bytes.Equal(receipt.GetResultPayload()[:4], []byte(participantResultProofMagic)) {
		t.Fatalf("first commit did not persist canonical result proof: %+v", receipt)
	}
	decoded, committedHash, err := decodeParticipantResultProof(call.Spec, call.InvocationID, receipt.GetPayloadHash(), receipt.GetResultPayload())
	if err != nil {
		t.Fatal(err)
	}
	decodedBytes, _, err := contracts.ValidateTypedResult(call.Spec, decoded)
	if err != nil || !bytes.Equal(decodedBytes, first.GetTypedResult()) || !bytes.Equal(committedHash, first.GetParticipantResultHash()) {
		t.Fatalf("persisted proof result/hash mismatch decoded=%x hash=%x err=%v", decodedBytes, committedHash, err)
	}

	exact := mustExecute(t, server.ExecuteStep, request)
	if !exact.GetReplayedFromReceipt() || !bytes.Equal(exact.GetTypedResult(), first.GetTypedResult()) ||
		!bytes.Equal(exact.GetParticipantResultHash(), first.GetParticipantResultHash()) {
		t.Fatalf("exact replay=%+v first=%+v", exact, first)
	}

	collision := proto.Clone(request).(*contractspb.ParticipantRequest)
	changed := &contractspb.ReserveSeedPayload{}
	mustUnmarshal(t, collision.GetTypedPayload(), changed)
	changed.PlotId++
	collision.TypedPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := server.ExecuteStep(context.Background(), collision); response != nil || status.Code(err) != codes.FailedPrecondition ||
		status.Convert(err).Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("different payload collision response=%v err=%v", response, err)
	}

	trig.registry.Stop()
	trig.registry = playeractor.NewAuthorityRegistry(trig.provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(trig.registry.Stop)
	cold := mustExecute(t, newAllowServer(trig.registry).ExecuteStep, request)
	if !cold.GetReplayedFromReceipt() || !bytes.Equal(cold.GetTypedResult(), first.GetTypedResult()) ||
		!bytes.Equal(cold.GetParticipantResultHash(), first.GetParticipantResultHash()) ||
		cold.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
		t.Fatalf("restart replay=%+v first=%+v", cold, first)
	}
}

func TestPlantDurableResultProofTamperAndLegacyFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(t *testing.T, receipt *playerpb.PlayerOperationReceipt, first *contractspb.ParticipantResponse)
	}{
		{name: "tampered result hash", tamper: func(t *testing.T, receipt *playerpb.PlayerOperationReceipt, _ *contractspb.ParticipantResponse) {
			t.Helper()
			_, resultHashOffset, _, _, ok := participantResultProofOffsets(receipt.GetResultPayload())
			if !ok {
				t.Fatal("valid proof offsets unavailable")
			}
			receipt.ResultPayload[resultHashOffset] ^= 0xff
		}},
		{name: "tampered typed result", tamper: func(t *testing.T, receipt *playerpb.PlayerOperationReceipt, _ *contractspb.ParticipantResponse) {
			t.Helper()
			_, _, typedLengthOffset, typedOffset, ok := participantResultProofOffsets(receipt.GetResultPayload())
			if !ok {
				t.Fatal("valid proof offsets unavailable")
			}
			result := &contractspb.ReserveSeedResult{}
			mustUnmarshal(t, receipt.GetResultPayload()[typedOffset:], result)
			result.AvailableSeedQuantityAfter++
			typed, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			receipt.ResultPayload = append(append([]byte(nil), receipt.GetResultPayload()[:typedOffset]...), typed...)
			binary.BigEndian.PutUint32(receipt.ResultPayload[typedLengthOffset:typedOffset], uint32(len(typed)))
		}},
		{name: "malformed stored bytes", tamper: func(_ *testing.T, receipt *playerpb.PlayerOperationReceipt, _ *contractspb.ParticipantResponse) {
			receipt.ResultPayload = []byte(participantResultProofMagic)
		}},
		{name: "legacy raw typed result", tamper: func(_ *testing.T, receipt *playerpb.PlayerOperationReceipt, first *contractspb.ParticipantResponse) {
			receipt.ResultPayload = append([]byte(nil), first.GetTypedResult()...)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trig := newParticipantRig(t, "memory", plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			server := newAllowServer(trig.registry)
			request := reserveRequest(t, plantWorkID(t, "proof-tamper-"+tc.name), plantTestPlayerID, false)
			first := mustExecute(t, server.ExecuteStep, request)
			invocationID := request.GetCoordinator().GetParticipantInvocationId()
			if _, err := trig.registry.Submit(context.Background(), plantTestPlayerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
				receipt := playeringress.FindParticipantOperationReceipt(rec, invocationID)
				if receipt == nil {
					return nil, false, fmt.Errorf("participant receipt missing")
				}
				tc.tamper(t, receipt, first)
				return nil, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			beforeSubmitted := trig.registry.RuntimeStats().Submitted
			if response, err := server.ExecuteStep(context.Background(), request); response != nil || status.Code(err) != codes.Internal {
				t.Fatalf("tampered proof response=%v err=%v", response, err)
			}
			if afterSubmitted := trig.registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted {
				t.Fatalf("corrupt durable proof entered actor mailbox: submitted %d -> %d", beforeSubmitted, afterSubmitted)
			}
		})
	}
}

func TestPlantTerminalReceiptPrecedesStaleWorkAndLease(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			firstServer := newAllowServer(trig.registry)
			workID := plantWorkID(t, "stale-replay-"+backend)
			request := reserveRequest(t, workID, plantTestPlayerID, false)
			first := mustExecute(t, firstServer.ExecuteStep, request)
			beforeReplay := trig.registry.RuntimeStats().Submitted

			var workChecks, leaseChecks atomic.Int32
			staleServer := NewServer(Options{
				Registry:                trig.registry,
				RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
				WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
				IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
				PlantDependencyVerifier: PlantDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
				WorkVersionVerifier: WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
					workChecks.Add(1)
					return status.Error(codes.FailedPrecondition, "stale work version")
				}),
				WorkLeaseVerifier: WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
					leaseChecks.Add(1)
					return status.Error(codes.FailedPrecondition, "stale work lease")
				}),
			})

			staleCoordinator := proto.Clone(request).(*contractspb.ParticipantRequest)
			staleCoordinator.GetCoordinator().ExpectedWorkVersion = 999
			replayed := mustExecute(t, staleServer.ExecuteStep, staleCoordinator)
			if !replayed.GetReplayedFromReceipt() || !bytes.Equal(replayed.GetTypedResult(), first.GetTypedResult()) ||
				replayed.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() || workChecks.Load() != 0 {
				t.Fatalf("stale coordinator replay=%+v work checks=%d", replayed, workChecks.Load())
			}

			staleWorker := reserveRequest(t, workID, plantTestPlayerID, true)
			staleWorker.GetRecovery().ExpectedWorkVersion = 999
			workerReplay := mustExecute(t, staleServer.ResumeStep, staleWorker)
			if !workerReplay.GetReplayedFromReceipt() || !bytes.Equal(workerReplay.GetTypedResult(), first.GetTypedResult()) ||
				workerReplay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() || leaseChecks.Load() != 0 {
				t.Fatalf("stale worker replay=%+v lease checks=%d", workerReplay, leaseChecks.Load())
			}
			if afterReplay := trig.registry.RuntimeStats().Submitted; afterReplay != beforeReplay {
				t.Fatalf("receipt replay entered actor mailbox: submitted %d -> %d", beforeReplay, afterReplay)
			}
		})
	}
}

func TestResumeStepRejectsStaleWorkVersionWithValidLeaseBeforeMailbox(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	seedPlayer(t, registry, plantTestPlayerID, 1)
	server := newAllowServer(registry)
	request := reserveRequest(t, plantWorkID(t, "worker-stale-version"), plantTestPlayerID, true)
	request.GetRecovery().ExpectedWorkVersion = 2
	before, beforeVersion := readPlayerVersion(t, registry)
	if _, err := server.ResumeStep(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale worker WorkVersion err=%v", err)
	}
	after, afterVersion := readPlayerVersion(t, registry)
	if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) ||
		playeractorReservation(after, mustSeedReservation(t, request.GetRecovery().GetWork().GetWorkId())) != nil ||
		hasPlayerReceipt(after, request.GetRecovery().GetParticipantInvocationId()) {
		t.Fatalf("stale worker entered handler/receipt commit: version %d/%d", beforeVersion, afterVersion)
	}
}

func TestPlantInsufficientSeedIsTerminalAndReplayed(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 0)
			server := newAllowServer(trig.registry)
			workID := plantWorkID(t, "insufficient-"+backend)
			request := reserveRequest(t, workID, plantTestPlayerID, false)
			first := mustExecute(t, server.ExecuteStep, request)
			if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED ||
				first.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM || first.GetReplayedFromReceipt() {
				t.Fatalf("insufficient first=%+v", first)
			}
			if playeractorReservation(mustReadRecord(t, trig.registry), mustSeedReservation(t, workID)) != nil {
				t.Fatal("insufficient reserve must not create a reservation")
			}
			seedPlayer(t, trig.registry, plantTestPlayerID, 2)
			replay := mustExecute(t, server.ExecuteStep, request)
			if !replay.GetReplayedFromReceipt() || replay.GetOutcome() != first.GetOutcome() ||
				!bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) || !bytes.Equal(replay.GetParticipantResultHash(), first.GetParticipantResultHash()) ||
				replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
				t.Fatalf("insufficient replay=%+v first=%+v", replay, first)
			}
			if playeractorReservation(mustReadRecord(t, trig.registry), mustSeedReservation(t, workID)) != nil {
				t.Fatal("replayed insufficient reserve must not create a reservation")
			}
		})
	}
}

func TestPlantTerminalReceiptSurvivesHotEvictionAndInventoryChange(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 0)
			server := newAllowServer(trig.registry)
			workID := plantWorkID(t, "durable-archive-"+backend)
			request := reserveRequest(t, workID, plantTestPlayerID, false)
			first := mustExecute(t, server.ExecuteStep, request)
			if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED ||
				first.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM {
				t.Fatalf("first terminal rejection=%+v", first)
			}
			invocation := request.GetCoordinator().GetParticipantInvocationId()
			nowMs := time.Now().UnixMilli()
			if _, err := trig.registry.Submit(context.Background(), plantTestPlayerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
				receipt := playeringress.FindParticipantOperationReceipt(rec, invocation)
				if receipt == nil {
					return nil, false, fmt.Errorf("terminal receipt missing before archive")
				}
				receipt.CommittedAtMs = nowMs - (8 * 24 * time.Hour).Milliseconds()
				playeringress.CompactParticipantOperationReceipts(rec, nowMs)
				if receipt.GetCommittedAtMs() != 0 {
					return nil, false, fmt.Errorf("receipt was not compacted to archive marker")
				}
				return nil, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			// The business state changes after the original terminal rejection;
			// the archived result must still win over current inventory rules.
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			if trig.provider != nil {
				trig.registry.Stop()
				trig.registry = playeractor.NewAuthorityRegistry(trig.provider, playeractor.RegistryOptions{MailboxSize: 8})
				t.Cleanup(trig.registry.Stop)
				server = newAllowServer(trig.registry)
			}
			replay := mustExecute(t, server.ExecuteStep, request)
			if !replay.GetReplayedFromReceipt() || replay.GetOutcome() != first.GetOutcome() ||
				!bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) || replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
				t.Fatalf("archived terminal replay=%+v first=%+v", replay, first)
			}
			record := mustReadRecord(t, trig.registry)
			if playeractor.ItemCount(record, playeractor.ItemCarrotSeed) != 1 || playeractorReservation(record, mustSeedReservation(t, workID)) != nil {
				t.Fatal("archived rejection was re-executed after inventory changed")
			}

			conflict := proto.Clone(request).(*contractspb.ParticipantRequest)
			changed := &contractspb.ReserveSeedPayload{}
			mustUnmarshal(t, conflict.GetTypedPayload(), changed)
			changed.PlotId++
			conflict.TypedPayload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(changed)
			if _, err := server.ExecuteStep(context.Background(), conflict); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
				t.Fatalf("archived different-hash conflict=%v", err)
			}
		})
	}
}

func TestPlantWorkRecordAndFarmProofMutationMatrix(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	seedPlayer(t, trig.registry, plantTestPlayerID, 64)
	server := newAllowServer(trig.registry)

	reserveCases := []struct {
		name   string
		mutate func(*contractspb.ReserveSeedPayload)
	}{
		{"farm", func(p *contractspb.ReserveSeedPayload) { p.FarmId++ }},
		{"plot", func(p *contractspb.ReserveSeedPayload) { p.PlotId++ }},
		{"crop_definition", func(p *contractspb.ReserveSeedPayload) { p.CropDefinitionId++ }},
		{"seed_definition", func(p *contractspb.ReserveSeedPayload) { p.SeedItemId++ }},
		{"quantity", func(p *contractspb.ReserveSeedPayload) { p.Quantity = 2 }},
		{"rule_version", func(p *contractspb.ReserveSeedPayload) { p.CropRuleVersion++ }},
		{"rule_hash", func(p *contractspb.ReserveSeedPayload) { p.CropRuleSnapshotHash[0]++ }},
		{"reservation_identity", func(p *contractspb.ReserveSeedPayload) { p.ReservationId = "sid1_forged-reservation" }},
	}
	for _, tc := range reserveCases {
		t.Run("reserve/"+tc.name, func(t *testing.T) {
			workID := plantWorkID(t, "proof-reserve-"+tc.name)
			request := reserveRequest(t, workID, plantTestPlayerID, false)
			mutateTypedPayload(t, request, func(payload proto.Message) { tc.mutate(payload.(*contractspb.ReserveSeedPayload)) })
			before, version := readPlayerVersion(t, trig.registry)
			if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) == codes.OK {
				t.Fatal("mutated Reserve proof unexpectedly succeeded")
			}
			after, afterVersion := readPlayerVersion(t, trig.registry)
			if afterVersion != version || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) {
				t.Fatalf("mutated Reserve changed PlayerRecord version/seeds %d/%d", version, afterVersion)
			}
		})
	}

	commitCases := []struct {
		name   string
		mutate func(*contractspb.CommitSeedReservationPayload)
	}{
		{"farm", func(p *contractspb.CommitSeedReservationPayload) { p.FarmId++ }},
		{"plot", func(p *contractspb.CommitSeedReservationPayload) { p.PlotId++ }},
		{"cycle_identity", func(p *contractspb.CommitSeedReservationPayload) { p.CropCycleId = "sid1_forged-cycle" }},
		{"reservation_identity", func(p *contractspb.CommitSeedReservationPayload) { p.ReservationId = "sid1_forged-reservation" }},
		{"farm_decision_hash", func(p *contractspb.CommitSeedReservationPayload) { p.CommitPlantResultHash[0]++ }},
	}
	for _, tc := range commitCases {
		t.Run("commit/"+tc.name, func(t *testing.T) {
			workID := plantWorkID(t, "proof-commit-"+tc.name)
			mustExecute(t, server.ExecuteStep, reserveRequest(t, workID, plantTestPlayerID, false))
			request := commitRequest(t, workID, plantTestPlayerID, false)
			mutateTypedPayload(t, request, func(payload proto.Message) { tc.mutate(payload.(*contractspb.CommitSeedReservationPayload)) })
			before, version := readPlayerVersion(t, trig.registry)
			if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) == codes.OK {
				t.Fatal("mutated Commit proof unexpectedly succeeded")
			}
			after, afterVersion := readPlayerVersion(t, trig.registry)
			reservation := playeractorReservation(after, mustSeedReservation(t, workID))
			if afterVersion != version || reservation == nil || reservation.GetState() != playerpb.ReservationState_PREPARED || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) {
				t.Fatalf("mutated Commit changed state version=%d/%d reservation=%v", version, afterVersion, reservation)
			}
		})
	}

	releaseCases := []struct {
		name   string
		mutate func(*contractspb.ReleaseSeedReservationPayload)
	}{
		{"farm", func(p *contractspb.ReleaseSeedReservationPayload) { p.FarmId++ }},
		{"plot", func(p *contractspb.ReleaseSeedReservationPayload) { p.PlotId++ }},
		{"reservation_identity", func(p *contractspb.ReleaseSeedReservationPayload) { p.ReservationId = "sid1_forged-reservation" }},
		{"seed_definition", func(p *contractspb.ReleaseSeedReservationPayload) { p.SeedItemId++ }},
		{"quantity", func(p *contractspb.ReleaseSeedReservationPayload) { p.Quantity = 2 }},
		{"release_reason", func(p *contractspb.ReleaseSeedReservationPayload) { p.ReleaseReason = "FARM_NO_CHANGE" }},
		{"farm_decision_hash", func(p *contractspb.ReleaseSeedReservationPayload) { p.TerminalFarmResultHash[0]++ }},
	}
	for _, tc := range releaseCases {
		t.Run("release/"+tc.name, func(t *testing.T) {
			workID := plantWorkID(t, "proof-release-"+tc.name)
			mustExecute(t, server.ExecuteStep, reserveRequest(t, workID, plantTestPlayerID, false))
			request := releaseRequest(t, workID, plantTestPlayerID, false)
			mutateTypedPayload(t, request, func(payload proto.Message) { tc.mutate(payload.(*contractspb.ReleaseSeedReservationPayload)) })
			before, version := readPlayerVersion(t, trig.registry)
			if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) == codes.OK {
				t.Fatal("mutated Release proof unexpectedly succeeded")
			}
			after, afterVersion := readPlayerVersion(t, trig.registry)
			reservation := playeractorReservation(after, mustSeedReservation(t, workID))
			if afterVersion != version || reservation == nil || reservation.GetState() != playerpb.ReservationState_PREPARED || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) {
				t.Fatalf("mutated Release changed state version=%d/%d reservation=%v", version, afterVersion, reservation)
			}
		})
	}
}

func TestPlantReceiptCapacityReturnsStableTypedError(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	seedPlayer(t, trig.registry, plantTestPlayerID, 1)
	nowMs := time.Now().UnixMilli()
	if _, err := trig.registry.Submit(context.Background(), plantTestPlayerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		for i := 0; i < 1024; i++ {
			rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
				OperationId: playeringress.ParticipantReceiptOperationKey(fmt.Sprintf("capacity_%04d", i)), Status: commonpb.CommandStatus_SUCCEEDED,
				ErrorCode: commonpb.ErrorCode_OK, ResultPayload: []byte{1}, CommittedAtMs: nowMs - (8 * 24 * time.Hour).Milliseconds(),
				PayloadHash: bytes.Repeat([]byte{byte(i%251 + 1)}, 32),
			})
		}
		playeringress.CompactParticipantOperationReceipts(rec, nowMs)
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	server := newAllowServer(trig.registry)
	request := reserveRequest(t, plantWorkID(t, "receipt-capacity"), plantTestPlayerID, false)
	_, beforeVersion := readPlayerVersion(t, trig.registry)
	_, err := server.ExecuteStep(context.Background(), request)
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted || st.Message() != "ERR_RATE_LIMITED" {
		t.Fatalf("capacity error=%v", err)
	}
	foundDetail := false
	for _, detail := range st.Details() {
		if failure, ok := detail.(*contractspb.ParticipantFailure); ok && failure.GetError().GetErrorCode() == commonpb.ErrorCode_ERR_RATE_LIMITED {
			foundDetail = true
		}
	}
	if !foundDetail {
		t.Fatalf("capacity error missing typed ParticipantFailure details: %v", st.Details())
	}
	after, afterVersion := readPlayerVersion(t, trig.registry)
	if afterVersion != beforeVersion || playeractorReservation(after, mustSeedReservation(t, request.GetCoordinator().GetWork().GetWorkId())) != nil {
		t.Fatalf("capacity rejection mutated version/state %d/%d", beforeVersion, afterVersion)
	}
}

func TestPlantStorePhysicalCapacityRejectsBeforeMailboxAdmission(t *testing.T) {
	cases := []struct {
		name   string
		limits func() authoritystore.Limits
	}{
		{name: "single receipt result envelope", limits: func() authoritystore.Limits {
			limits := authoritystore.DefaultLimits()
			limits.MaxSingleReceiptBytes = 1
			return limits
		}},
		{name: "projected serialized commit", limits: func() authoritystore.Limits {
			limits := authoritystore.DefaultLimits()
			limits.MaxCommitBytes = 1
			return limits
		}},
	}
	for _, tc := range cases {
		for _, backend := range []string{"memory", "redis"} {
			t.Run(tc.name+"/"+backend, func(t *testing.T) {
				trig := newParticipantRigWithLimits(t, backend, plantTestPlayerID, tc.limits())
				if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "capacity-preflight"); err != nil {
					t.Fatal(err)
				}
				server := newAllowServer(trig.registry)
				request := reserveRequest(t, plantWorkID(t, "physical-capacity-"+tc.name), plantTestPlayerID, false)
				before, beforeVersion := readPlayerVersion(t, trig.registry)
				beforeSubmitted := trig.registry.RuntimeStats().Submitted
				if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
					t.Fatalf("physical capacity error=%v", err)
				}
				if afterSubmitted := trig.registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted {
					t.Fatalf("capacity rejection entered actor mailbox: submitted %d -> %d", beforeSubmitted, afterSubmitted)
				}
				after, afterVersion := readPlayerVersion(t, trig.registry)
				if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) ||
					playeractorReservation(after, mustSeedReservation(t, request.GetCoordinator().GetWork().GetWorkId())) != nil ||
					hasPlayerReceipt(after, request.GetCoordinator().GetParticipantInvocationId()) {
					t.Fatalf("physical capacity failure mutated PlayerRecord version=%d/%d", beforeVersion, afterVersion)
				}
			})
		}
	}
}

func TestPlantOuterReceiptUsageRejectsSmallNestedCarrierBeforeMailbox(t *testing.T) {
	limits := authoritystore.DefaultLimits()
	limits.MaxReceipts = 1
	trig := newParticipantRigWithLimits(t, "memory", plantTestPlayerID, limits)
	registry := trig.registry
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "receipt-items"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Submit(context.Background(), plantTestPlayerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		// Consume one outer Authority receipt without adding anything to the
		// nested PlayerRecord receipt carrier.
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	record, version, err := registry.ReadOutsideMailbox(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 {
		t.Fatalf("nested receipt carrier is not small: %d", len(record.GetRecentOperationReceipts()))
	}
	snapshot, ok := trig.memory.Inspect(authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: plantTestPlayerID})
	if !ok || snapshot.PhysicalUsage.ReceiptItems != limits.MaxReceipts || snapshot.PhysicalUsage.RecordVersion != version {
		t.Fatalf("outer usage=%+v version=%d", snapshot.PhysicalUsage, version)
	}
	server := newAllowServer(registry)
	request := reserveRequest(t, plantWorkID(t, "receipt-item-capacity"), plantTestPlayerID, false)
	before, beforeVersion := readPlayerVersion(t, registry)
	beforeSubmitted := registry.RuntimeStats().Submitted
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
		t.Fatalf("item capacity error=%v", err)
	}
	if afterSubmitted := registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted {
		t.Fatalf("outer capacity rejection entered decision mailbox: submitted %d -> %d", beforeSubmitted, afterSubmitted)
	}
	after, afterVersion := readPlayerVersion(t, registry)
	if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) ||
		playeractorReservation(after, mustSeedReservation(t, request.GetCoordinator().GetWork().GetWorkId())) != nil ||
		hasPlayerReceipt(after, request.GetCoordinator().GetParticipantInvocationId()) {
		t.Fatalf("item capacity rejection mutated record version=%d/%d", beforeVersion, afterVersion)
	}
}

func TestPlantSuccessfulCommitAdvancesUsageAndFullReplayDoesNotConsumeAgain(t *testing.T) {
	limits := authoritystore.DefaultLimits()
	limits.MaxReceipts = 1
	trig := newParticipantRigWithLimits(t, "memory", plantTestPlayerID, limits)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "usage-advance"); err != nil {
		t.Fatal(err)
	}
	server := newAllowServer(trig.registry)
	firstRequest := reserveRequest(t, plantWorkID(t, "usage-advance-first"), plantTestPlayerID, false)
	first := mustExecute(t, server.ExecuteStep, firstRequest)
	if first.GetReplayedFromReceipt() {
		t.Fatalf("first outcome=%+v", first)
	}

	// This check does not load/reactivate the actor. It proves CASEpoch advanced
	// the binding's version-bound aggregate usage after the confirmed commit.
	if err := trig.registry.CheckParticipantAuthorityCapacity(context.Background(), plantTestPlayerID,
		first.GetCommittedRecordVersion(), len(authority.RecordModelField)+1); !errors.Is(err, playeractor.ErrParticipantPhysicalCapacity) {
		t.Fatalf("cached usage was not advanced to full: %v", err)
	}

	beforeReplay, ok := trig.memory.Inspect(authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: plantTestPlayerID})
	if !ok || beforeReplay.PhysicalUsage.ReceiptItems != 1 {
		t.Fatalf("usage after first commit=%+v", beforeReplay.PhysicalUsage)
	}
	replay := mustExecute(t, server.ExecuteStep, firstRequest)
	if !replay.GetReplayedFromReceipt() || replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() ||
		!bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) {
		t.Fatalf("full-capacity replay=%+v first=%+v", replay, first)
	}
	afterReplay, ok := trig.memory.Inspect(authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: plantTestPlayerID})
	if !ok || afterReplay.RecordVersion != beforeReplay.RecordVersion || afterReplay.PhysicalUsage != beforeReplay.PhysicalUsage {
		t.Fatalf("replay consumed outer usage before=%+v after=%+v", beforeReplay.PhysicalUsage, afterReplay.PhysicalUsage)
	}

	secondRequest := reserveRequest(t, plantWorkID(t, "usage-advance-second"), plantTestPlayerID, false)
	beforeSubmitted := trig.registry.RuntimeStats().Submitted
	if _, err := server.ExecuteStep(context.Background(), secondRequest); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
		t.Fatalf("following invocation did not observe full usage: %v", err)
	}
	if afterSubmitted := trig.registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted {
		t.Fatalf("following full-capacity call entered mailbox: %d -> %d", beforeSubmitted, afterSubmitted)
	}
}

func TestPlantPhysicalUsageUnavailableOrStaleFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode physicalUsageFaultMode
	}{
		{name: "unavailable", mode: physicalUsageUnavailable},
		{name: "stale", mode: physicalUsageStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trig := newParticipantRig(t, "memory", plantTestPlayerID)
			trig.registry.Stop()
			fence := trig.reassign(t, "usage-fault-holder")
			store := &physicalUsageFaultStore{ActorAuthorityStore: trig.base, mode: tc.mode}
			provider, err := authority.NewProvider(authority.ProviderOptions{
				Store: store, Fence: authority.StaticFenceSource{Fence: fence},
				AuthorityLayoutVersion: plantTestLayout, BootID: "usage-fault-boot",
			})
			if err != nil {
				t.Fatal(err)
			}
			registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
			t.Cleanup(registry.Stop)
			if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "usage-fault"); err != nil {
				t.Fatal(err)
			}
			server := newAllowServer(registry)
			request := reserveRequest(t, plantWorkID(t, "usage-fault-"+tc.name), plantTestPlayerID, false)
			beforeSubmitted := registry.RuntimeStats().Submitted
			if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
				t.Fatalf("physical usage %s did not fail closed: %v", tc.name, err)
			}
			if afterSubmitted := registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted {
				t.Fatalf("untrusted usage entered mailbox: %d -> %d", beforeSubmitted, afterSubmitted)
			}
		})
	}
}

func TestPlantInMailboxPhysicalUsageRefreshFailsClosedBeforeMutation(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	trig.registry.Stop()
	fence := trig.reassign(t, "mailbox-usage-fault-holder")
	// Ensure performs loads 1/2, receipt admission performs load 3, and the
	// mailbox's fresh pre-mutation load is 4. Corrupt only that last view.
	store := &physicalUsageFaultStore{
		ActorAuthorityStore: trig.base,
		mode:                physicalUsageUnavailable,
		corruptLoadAt:       4,
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: plantTestLayout, BootID: "mailbox-usage-fault-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "mailbox-usage-fault"); err != nil {
		t.Fatal(err)
	}
	server := newAllowServer(registry)
	request := reserveRequest(t, plantWorkID(t, "mailbox-usage-fault"), plantTestPlayerID, false)
	before, beforeVersion := readPlayerVersion(t, registry)
	// readPlayerVersion adds one load, so shift corruption to the next load.
	store.corruptLoadAt = store.loadCalls.Load() + 2
	beforeSubmitted := registry.RuntimeStats().Submitted
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
		t.Fatalf("in-mailbox untrusted usage did not fail closed: %v", err)
	}
	if afterSubmitted := registry.RuntimeStats().Submitted; afterSubmitted != beforeSubmitted+1 {
		t.Fatalf("expected one mailbox pre-mutation check: %d -> %d", beforeSubmitted, afterSubmitted)
	}
	after, afterVersion := readPlayerVersion(t, registry)
	if afterVersion != beforeVersion || !proto.Equal(after, before) || hasPlayerReceipt(after, request.GetCoordinator().GetParticipantInvocationId()) {
		t.Fatalf("in-mailbox capacity failure mutated record version=%d/%d", beforeVersion, afterVersion)
	}
}

func TestAuthorityRegistryCapacityComesOnlyFromConcreteStore(t *testing.T) {
	limits := authoritystore.DefaultLimits()
	limits.MaxReceipts = 7
	limits.MaxReceiptBytes = 7000
	limits.MaxSingleReceiptBytes = 701
	limits.MaxModelBytes = 70000
	limits.MaxCommitBytes = 71000
	trig := newParticipantRigWithLimits(t, "memory", plantTestPlayerID, limits)
	got := trig.registry.ParticipantPhysicalCapacity()
	want := playeractor.ParticipantPhysicalCapacity{
		MaxReceiptItems:       limits.MaxReceipts,
		MaxModelBytes:         limits.MaxModelBytes,
		MaxReceiptBytes:       limits.MaxReceiptBytes,
		MaxSingleReceiptBytes: limits.MaxSingleReceiptBytes,
		MaxCommitBytes:        limits.MaxCommitBytes,
	}
	if got != want {
		t.Fatalf("Registry limits=%+v want concrete Store limits=%+v", got, want)
	}
	if err := trig.registry.ParticipantAdmissionError(); err != nil {
		t.Fatalf("concrete Store capabilities were not accepted: %v", err)
	}
}

func TestPlantAuthorityCapacityErrorMapsStableAndDoesNotCommit(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			server := newAllowServer(trig.registry)
			request := reserveRequest(t, plantWorkID(t, "authority-capacity-"+backend), plantTestPlayerID, false)
			before, beforeVersion := readPlayerVersion(t, trig.registry)
			trig.capacity.fail.Store(true)
			if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "ERR_RATE_LIMITED" {
				t.Fatalf("authority capacity error=%v", err)
			}
			after, afterVersion := readPlayerVersion(t, trig.registry)
			if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) ||
				playeractorReservation(after, mustSeedReservation(t, request.GetCoordinator().GetWork().GetWorkId())) != nil ||
				hasPlayerReceipt(after, request.GetCoordinator().GetParticipantInvocationId()) {
				t.Fatalf("authority capacity failure mutated PlayerRecord version=%d/%d", beforeVersion, afterVersion)
			}
		})
	}
}

func TestPlantDependencyAdmissionAndFinalBudgetAreBeforeCommit(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	registry := trig.registry
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "budget-admission"); err != nil {
		t.Fatal(err)
	}
	request := reserveRequest(t, plantWorkID(t, "dependency-admission-budget"), plantTestPlayerID, false)
	clock := time.UnixMilli(10_000)
	var dependencyCalls, handlerCalls atomic.Int32
	server := NewServer(Options{
		Registry:                registry,
		Now:                     func() time.Time { return clock },
		WorkVersionVerifier:     WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		PlantDependencyVerifier: PlantDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			dependencyCalls.Add(1)
			clock = clock.Add(2 * time.Second)
			return nil
		}),
		testHandler: resolvingTestHandler{resolver: newPlantHandler(registry, nil).(ReceiptResolver), delegate: HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
			handlerCalls.Add(1)
			return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
		})},
	})
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("dependency elapsed budget error=%v", err)
	}
	if dependencyCalls.Load() != 1 || handlerCalls.Load() != 0 {
		t.Fatalf("dependency/mailbox order dependency=%d handler=%d", dependencyCalls.Load(), handlerCalls.Load())
	}

	// Advance time from the local, non-blocking receipt/capacity path inside
	// the Actor callback. The final check must abort before decide/commit.
	seedPlayer(t, registry, plantTestPlayerID, 1)
	clock = time.UnixMilli(20_000)
	workID := plantWorkID(t, "final-budget-before-commit")
	finalRequest := reserveRequest(t, workID, plantTestPlayerID, false)
	before, beforeVersion := readPlayerVersion(t, registry)
	finalServer := newAllowServer(registry)
	finalServer.ingress.Now = func() time.Time { return clock }
	finalServer.handler = newPlantHandler(registry, func() int64 {
		clock = clock.Add(2 * time.Second)
		return clock.UnixMilli()
	})
	if _, err := finalServer.ExecuteStep(context.Background(), finalRequest); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("final in-turn budget error=%v", err)
	}
	after, afterVersion := readPlayerVersion(t, registry)
	if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) ||
		playeractorReservation(after, mustSeedReservation(t, workID)) != nil || hasPlayerReceipt(after, finalRequest.GetCoordinator().GetParticipantInvocationId()) {
		t.Fatalf("expired final budget mutated PlayerRecord version=%d/%d", beforeVersion, afterVersion)
	}
}

func TestPlantFarmDependencyAndReleaseReasonAreFailClosed(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	seedPlayer(t, trig.registry, plantTestPlayerID, 1)
	allow := newAllowServer(trig.registry)
	workID := plantWorkID(t, "dependency-proof")
	mustExecute(t, allow.ExecuteStep, reserveRequest(t, workID, plantTestPlayerID, false))

	rejectedDependency := NewServer(Options{
		Registry:                trig.registry,
		WorkVersionVerifier:     WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkLeaseVerifier:       WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		PlantDependencyVerifier: PlantDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			return status.Error(codes.FailedPrecondition, "farm dependency state/hash mismatch")
		}),
	})
	_, err := rejectedDependency.ExecuteStep(context.Background(), commitRequest(t, workID, plantTestPlayerID, false))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit dependency rejection=%v", err)
	}
	record := mustReadRecord(t, trig.registry)
	commitInvocation, err := identity.ParticipantInvocationID(identity.WorkTypePlant, workID, identity.StepCommitSeedReservation, 1)
	if err != nil {
		t.Fatal(err)
	}
	if playeractorReservation(record, mustSeedReservation(t, workID)).GetState() != playerpb.ReservationState_PREPARED || hasPlayerReceipt(record, commitInvocation) {
		t.Fatal("dependency rejection changed reservation or fabricated receipt")
	}

	badReason := releaseRequest(t, workID, plantTestPlayerID, false)
	badPayload := &contractspb.ReleaseSeedReservationPayload{}
	mustUnmarshal(t, badReason.GetTypedPayload(), badPayload)
	badPayload.ReleaseReason = "arbitrary-refund"
	badReason.TypedPayload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(badPayload)
	if _, err := allow.ExecuteStep(context.Background(), badReason); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("arbitrary release reason error=%v", err)
	}
	afterBadReason := mustReadRecord(t, trig.registry)
	if playeractor.ItemCount(afterBadReason, playeractor.ItemCarrotSeed) != 0 || playeractorReservation(afterBadReason, mustSeedReservation(t, workID)).GetState() != playerpb.ReservationState_PREPARED {
		t.Fatal("invalid release reason changed PlayerRecord")
	}
}

func hasPlayerReceipt(record *playerpb.PlayerRecord, operationID string) bool {
	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt != nil && receipt.GetOperationId() == playeringress.ParticipantReceiptOperationKey(operationID) {
			return true
		}
	}
	return false
}

func TestPlantCommitReleaseConcurrentSingleTransition(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			server := newAllowServer(trig.registry)
			workID := plantWorkID(t, "commit-release-race-"+backend)
			mustExecute(t, server.ExecuteStep, reserveRequest(t, workID, plantTestPlayerID, false))
			responses := make(chan *contractspb.ParticipantResponse, 2)
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			for _, request := range []*contractspb.ParticipantRequest{
				commitRequest(t, workID, plantTestPlayerID, false),
				releaseRequest(t, workID, plantTestPlayerID, false),
			} {
				request := request
				wg.Add(1)
				go func() {
					defer wg.Done()
					response, err := server.ExecuteStep(context.Background(), request)
					if err != nil {
						errs <- err
						return
					}
					responses <- response
				}()
			}
			wg.Wait()
			close(responses)
			close(errs)
			for err := range errs {
				t.Fatalf("commit/release race error=%v", err)
			}
			applied, rejected := 0, 0
			for response := range responses {
				switch response.GetOutcome() {
				case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
					applied++
				case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED:
					rejected++
				default:
					t.Fatalf("unexpected commit/release outcome=%v", response.GetOutcome())
				}
			}
			if applied != 1 || rejected != 1 {
				t.Fatalf("commit/release transition applied=%d rejected=%d", applied, rejected)
			}
			record := mustReadRecord(t, trig.registry)
			reservation := playeractorReservation(record, mustSeedReservation(t, workID))
			if reservation == nil || (reservation.GetState() != playerpb.ReservationState_COMMITTED && reservation.GetState() != playerpb.ReservationState_RELEASED) {
				t.Fatalf("race final reservation=%v", reservation)
			}
			seeds := playeractor.ItemCount(record, playeractor.ItemCarrotSeed)
			if reservation.GetState() == playerpb.ReservationState_COMMITTED && seeds != 0 || reservation.GetState() == playerpb.ReservationState_RELEASED && seeds != 1 {
				t.Fatalf("race final state=%v seeds=%d", reservation.GetState(), seeds)
			}
		})
	}
}

func TestPlantReleaseAndCommittedReleaseRejection(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 2)
			server := newAllowServer(trig.registry)

			releaseWork := plantWorkID(t, "release-"+backend)
			mustExecute(t, server.ExecuteStep, reserveRequest(t, releaseWork, plantTestPlayerID, false))
			release := mustExecute(t, server.ExecuteStep, releaseRequest(t, releaseWork, plantTestPlayerID, false))
			if release.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
				t.Fatalf("release=%+v", release)
			}
			releaseResult := &contractspb.ReleaseSeedReservationResult{}
			mustUnmarshal(t, release.GetTypedResult(), releaseResult)
			if releaseResult.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED || releaseResult.GetAvailableSeedQuantityAfter() != 2 {
				t.Fatalf("release result=%+v", releaseResult)
			}
			releaseReplay := mustExecute(t, server.ExecuteStep, releaseRequest(t, releaseWork, plantTestPlayerID, false))
			if !releaseReplay.GetReplayedFromReceipt() || releaseReplay.GetCommittedRecordVersion() != release.GetCommittedRecordVersion() {
				t.Fatalf("release replay=%+v", releaseReplay)
			}

			commitWork := plantWorkID(t, "release-after-commit-"+backend)
			mustExecute(t, server.ExecuteStep, reserveRequest(t, commitWork, plantTestPlayerID, false))
			mustExecute(t, server.ExecuteStep, commitRequest(t, commitWork, plantTestPlayerID, false))
			before, _, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			beforeSeeds := playeractor.ItemCount(before, playeractor.ItemCarrotSeed)
			forbidden := mustExecute(t, server.ExecuteStep, releaseRequest(t, commitWork, plantTestPlayerID, false))
			if forbidden.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || forbidden.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INTERNAL {
				t.Fatalf("release after commit=%+v", forbidden)
			}
			after, _, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != beforeSeeds || playeractorReservation(after, mustSeedReservation(t, commitWork)).GetState() != playerpb.ReservationState_COMMITTED {
				t.Fatalf("release after commit changed state/seeds: seeds=%d before=%d", playeractor.ItemCount(after, playeractor.ItemCarrotSeed), beforeSeeds)
			}
		})
	}
}

func TestPlantConflictConcurrencyAndUnimplemented(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 32)
			server := newAllowServer(trig.registry)
			workID := plantWorkID(t, "conflict-"+backend)
			request := reserveRequest(t, workID, plantTestPlayerID, false)
			first := mustExecute(t, server.ExecuteStep, request)
			before, beforeVersion, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			changedPayload := reserveRequest(t, workID, plantTestPlayerID, false)
			changed := &contractspb.ReserveSeedPayload{}
			mustUnmarshal(t, changedPayload.GetTypedPayload(), changed)
			changed.PlotId++
			changedPayload.TypedPayload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(changed)
			_, conflictErr := server.ExecuteStep(context.Background(), changedPayload)
			if status.Code(conflictErr) != codes.FailedPrecondition || status.Convert(conflictErr).Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
				t.Fatalf("payload conflict=%v", conflictErr)
			}
			after, afterVersion, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrotSeed) != playeractor.ItemCount(before, playeractor.ItemCarrotSeed) || first.GetReplayedFromReceipt() {
				t.Fatalf("conflict mutated version/seeds before=%d after=%d", beforeVersion, afterVersion)
			}

			concurrentWork := plantWorkID(t, "concurrent-"+backend)
			concurrentRequest := reserveRequest(t, concurrentWork, plantTestPlayerID, false)
			const n = 24
			responses := make(chan *contractspb.ParticipantResponse, n)
			errs := make(chan error, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					response, err := server.ExecuteStep(context.Background(), concurrentRequest)
					if err != nil {
						errs <- err
						return
					}
					responses <- response
				}()
			}
			wg.Wait()
			close(responses)
			close(errs)
			applied, replayed := 0, 0
			for err := range errs {
				t.Fatalf("concurrent reserve error=%v", err)
			}
			var committedVersion uint64
			for response := range responses {
				if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
					t.Fatalf("concurrent outcome=%v", response.GetOutcome())
				}
				if response.GetReplayedFromReceipt() {
					replayed++
				} else {
					applied++
					committedVersion = response.GetCommittedRecordVersion()
				}
			}
			if applied != 1 || replayed != n-1 || committedVersion == 0 {
				t.Fatalf("concurrent applied=%d replayed=%d version=%d", applied, replayed, committedVersion)
			}

			malformedUnlock := unimplementedRequest(t, plantWorkID(t, "malformed-unlock-"+backend), plantTestPlayerID)
			if _, err := server.ExecuteStep(context.Background(), malformedUnlock); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("malformed unlock command err=%v", err)
			}
		})
	}
}

func TestPlantTimeoutUnknownResolutionsAndWriterFence(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			seedPlayer(t, trig.registry, plantTestPlayerID, 2)
			server := newAllowServer(trig.registry)

			beforeWork := plantWorkID(t, "timeout-before-"+backend)
			beforeRequest := reserveRequest(t, beforeWork, plantTestPlayerID, false)
			trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
			trig.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownBeforeApply)
			unknown := mustExecute(t, server.ExecuteStep, beforeRequest)
			if unknown.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN || unknown.GetReplayedFromReceipt() || unknown.GetCommittedRecordVersion() != 0 || !unknown.GetFailure().GetRetryableAfterResolution() {
				t.Fatalf("timeout before=%+v", unknown)
			}
			pendingRecord, _, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if playeractor.ItemCount(pendingRecord, playeractor.ItemCarrotSeed) != 2 || playeractorReservation(pendingRecord, mustSeedReservation(t, beforeWork)) != nil {
				t.Fatal("TIMEOUT_UNKNOWN before apply wrote business state or receipt")
			}
			resolved := mustExecute(t, server.ExecuteStep, beforeRequest)
			if resolved.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || resolved.GetReplayedFromReceipt() {
				t.Fatalf("timeout not-committed retry=%+v", resolved)
			}

			seedPlayer(t, trig.registry, plantTestPlayerID, 1)
			afterWork := plantWorkID(t, "timeout-after-"+backend)
			afterRequest := reserveRequest(t, afterWork, plantTestPlayerID, false)
			trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
			after := mustExecute(t, server.ExecuteStep, afterRequest)
			if after.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || after.GetReplayedFromReceipt() {
				t.Fatalf("timeout committed resolution=%+v", after)
			}
			afterReplay := mustExecute(t, server.ExecuteStep, afterRequest)
			if !afterReplay.GetReplayedFromReceipt() || afterReplay.GetCommittedRecordVersion() != after.GetCommittedRecordVersion() {
				t.Fatalf("timeout committed replay=%+v", afterReplay)
			}

			// A stale binding is rejected after the shard fence changes; it
			// cannot append a late ParticipantReceipt or mutate inventory.
			staleWork := plantWorkID(t, "stale-fence-"+backend)
			staleRequest := reserveRequest(t, staleWork, plantTestPlayerID, false)
			trig.reassign(t, "holder-b")
			_, staleErr := server.ExecuteStep(context.Background(), staleRequest)
			if status.Code(staleErr) != codes.FailedPrecondition || status.Convert(staleErr).Message() != "WRITER_EPOCH_MISMATCH" {
				t.Fatalf("stale writer err=%v", staleErr)
			}
			metrics := trig.registry.AuthorityMetrics()
			if metrics.CommitEpochMismatch == 0 && metrics.ClaimFencedTotal == 0 {
				t.Fatal("stale writer was not fenced by authority")
			}
		})
	}
}

func TestParticipantVerifiersRunBeforeMailboxAndShareHandler(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	registry := trig.registry
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "verifier-bootstrap"); err != nil {
		t.Fatal(err)
	}
	var handlerCalls atomic.Int32
	var coordinatorChecks atomic.Int32
	var leaseChecks atomic.Int32
	spy := HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
		handlerCalls.Add(1)
		return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
	})
	failClosed := NewServer(Options{
		testHandler:         resolvingTestHandler{resolver: newPlantHandler(registry, nil).(ReceiptResolver), delegate: spy},
		WorkVersionVerifier: WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkLeaseVerifier:   WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
	})
	if _, err := failClosed.ExecuteStep(context.Background(), reserveRequest(t, plantWorkID(t, "unconfigured-ingress"), plantTestPlayerID, false)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing physical ingress proofs must fail closed, got %v", err)
	}
	if handlerCalls.Load() != 0 {
		t.Fatal("missing physical ingress proofs reached handler")
	}
	server := NewServer(Options{
		Registry:                registry,
		testHandler:             resolvingTestHandler{resolver: newPlantHandler(registry, nil).(ReceiptResolver), delegate: spy},
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkVersionVerifier: WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			coordinatorChecks.Add(1)
			return status.Error(codes.FailedPrecondition, "work version rejected")
		}),
		WorkLeaseVerifier: WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			leaseChecks.Add(1)
			return status.Error(codes.FailedPrecondition, "work lease rejected")
		}),
	})
	workID := plantWorkID(t, "verifier")
	coordinator := reserveRequest(t, workID, plantTestPlayerID, false)
	if _, err := server.ExecuteStep(context.Background(), coordinator); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("coordinator verifier err=%v", err)
	}
	worker := reserveRequest(t, workID, plantTestPlayerID, true)
	if _, err := server.ResumeStep(context.Background(), worker); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("worker verifier err=%v", err)
	}
	if coordinatorChecks.Load() != 1 || leaseChecks.Load() != 1 || handlerCalls.Load() != 0 {
		t.Fatalf("verifier checks coordinator=%d lease=%d handler=%d", coordinatorChecks.Load(), leaseChecks.Load(), handlerCalls.Load())
	}

	allowServer := NewServer(Options{
		Registry:                registry,
		testHandler:             resolvingTestHandler{resolver: newPlantHandler(registry, nil).(ReceiptResolver), delegate: spy},
		PlantDependencyVerifier: fixturePlantWorkRecordVerifier{},
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkVersionVerifier:     WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkLeaseVerifier:       WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
	})
	if _, err := allowServer.ExecuteStep(context.Background(), coordinator); err != nil {
		t.Fatalf("shared handler coordinator: %v", err)
	}
	if _, err := allowServer.ResumeStep(context.Background(), worker); err != nil {
		t.Fatalf("shared handler worker: %v", err)
	}
	if handlerCalls.Load() != 2 {
		t.Fatalf("shared handler calls=%d", handlerCalls.Load())
	}
}

func TestParticipantCustomHandlerCannotBypassReceiptResolver(t *testing.T) {
	server := NewServer(Options{testHandler: HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
		return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
	})})
	if _, err := server.ExecuteStep(context.Background(), nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unresolved custom handler configuration err=%v", err)
	}
	if _, err := NewServerChecked(Options{testHandler: HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
		return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
	})}); err == nil {
		t.Fatal("NewServerChecked accepted a handler without durable receipt resolution")
	}
}

func TestLegacyRegistryCannotConstructCheckedParticipant(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "legacy-participant"); err != nil {
		t.Fatal(err)
	}
	if err := registry.ParticipantAdmissionError(); err == nil {
		t.Fatal("legacy Registry advertised an atomic Participant receipt capability")
	}
	if _, err := NewServerChecked(Options{Registry: registry}); err == nil {
		t.Fatal("NewServerChecked accepted a legacy blobstore Registry")
	}
	server := NewServer(Options{Registry: registry})
	if _, err := server.ExecuteStep(context.Background(), nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy server did not fail closed before admission: %v", err)
	}
}

func TestParticipantTerminalResultRequiresDurableReceiptProof(t *testing.T) {
	server := NewServer(Options{
		testHandler: resolvingTestHandler{
			resolver: noOpReceiptResolver{},
			delegate: HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
				return &contracts.StepOutcome{
					Outcome:                contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
					CommittedRecordVersion: 1,
					Result:                 &contractspb.ReserveSeedResult{ReservationId: "r", SeedItemId: 1, ReservedQuantity: 1, PlayerRecordVersion: 1},
				}, nil
			}),
		},
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkVersionVerifier:     WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		PlantDependencyVerifier: fixturePlantWorkRecordVerifier{},
	})
	response, err := server.ExecuteStep(context.Background(), reserveRequest(t, plantWorkID(t, "no-op-resolver"), plantTestPlayerID, false))
	if status.Code(err) != codes.Internal || response != nil {
		t.Fatalf("server trusted terminal result without durable receipt proof response=%v err=%v", response, err)
	}
	if _, err := NewServerChecked(Options{testHandler: resolvingTestHandler{resolver: noOpReceiptResolver{}, delegate: HandlerFunc(func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
		return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
	})}}); err == nil {
		t.Fatal("NewServerChecked accepted a custom resolver/handler seam")
	}
}

func TestParticipantCapacityPreflightPrunesExpiredPublicReceipts(t *testing.T) {
	limits := authoritystore.DefaultLimits()
	limits.MaxReceipts = 2
	trig := newParticipantRigWithLimits(t, "memory", plantTestPlayerID, limits)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "retention-capacity"); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
	if _, err := trig.registry.Submit(context.Background(), plantTestPlayerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if !playeractor.AddItem(rec, playeractor.ItemCarrotSeed, 1) {
			return nil, false, fmt.Errorf("seed setup failed")
		}
		for i, id := range []string{"public-expired-1", "public-live-1"} {
			committedAt := old
			if i == 1 {
				committedAt = time.Now().UnixMilli()
			}
			rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
				OperationId: id, Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
				ResultPayload: []byte("public"), PayloadHash: bytes.Repeat([]byte{7}, 32), CommittedAtMs: committedAt,
			})
		}
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	server := newAllowServer(trig.registry)
	response := mustExecute(t, server.ExecuteStep, reserveRequest(t, plantWorkID(t, "expired-public-capacity"), plantTestPlayerID, false))
	if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("expired public receipts incorrectly consumed capacity: %+v", response)
	}
}

func newAllowServer(registry *playeractor.Registry) *Server {
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		panic(err)
	}
	return NewServer(Options{
		Registry:                 registry,
		WorkVersionVerifier:      fixtureWorkVersionVerifier{},
		WorkLeaseVerifier:        fixtureWorkLeaseVerifier{},
		RouteFenceVerifier:       fixtureRouteFenceVerifier{calc: calc},
		WorkAddressVerifier:      fixtureWorkAddressVerifier{},
		IdentityBindingVerifier:  fixtureIdentityBindingVerifier{},
		PlantDependencyVerifier:  fixturePlantWorkRecordVerifier{},
		UnlockDependencyVerifier: fixtureUnlockWorkRecordVerifier{},
	})
}

type resolvingTestHandler struct {
	resolver ReceiptResolver
	delegate Handler
}

func (resolvingTestHandler) participantTestOnly() {}

func (h resolvingTestHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	return h.resolver.ResolveParticipantReceipt(ctx, call)
}

func (h resolvingTestHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	return h.delegate.Handle(ctx, call)
}

type noOpReceiptResolver struct{}

func (noOpReceiptResolver) ResolveParticipantReceipt(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	return nil, false, nil
}

// These fixtures are concrete WorkRecord/Farm proofs, not permissive no-op
// callbacks. They model the immutable fields frozen by 29/32 while keeping
// the real Work Repository/FarmActor out of this PI1 write set.
type fixtureWorkVersionVerifier struct{}

func (fixtureWorkVersionVerifier) VerifyWorkVersion(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.ExpectedWorkVersion != 1 {
		return status.Error(codes.FailedPrecondition, "fixture WorkRecord version mismatch")
	}
	return nil
}

type fixtureWorkLeaseVerifier struct{}

func (fixtureWorkLeaseVerifier) VerifyWorkLease(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.LeaseEpoch != 1 || call.LeaseOwnerToken != "lease-token" || call.CallerInstanceID != "worker-1" || call.CallerBootID != "worker-boot-1" {
		return status.Error(codes.FailedPrecondition, "fixture WorkLease mismatch")
	}
	return nil
}

type fixtureRouteFenceVerifier struct {
	calc *actorrouting.Calculator
}

func (v fixtureRouteFenceVerifier) VerifyRoute(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || v.calc == nil {
		return status.Error(codes.FailedPrecondition, "fixture route proof missing")
	}
	route := call.RouteHeader
	shard, err := v.calc.ShardFor(actorrouting.DomainPlayer, route.GetActorId())
	if err != nil || route.GetVirtualShard() != shard || route.GetShardRevision() != 1 || route.GetExpectedWriterEpoch() != 1 ||
		route.GetExpectedLeaseEpoch() != 1 || route.GetExpectedLeaseUntilMs() != participantTestLeaseUntilMs ||
		route.GetExpectedHolderId() != plantTestHolder || route.GetExpectedHolderBootId() != plantTestHolderBoot {
		return status.Error(codes.FailedPrecondition, "fixture route fence mismatch")
	}
	return nil
}

type fixtureWorkAddressVerifier struct{}

func (fixtureWorkAddressVerifier) VerifyWorkAddress(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.Work == nil || call.Work.GetBucketVersion() != 1 || call.Work.GetBucketId() != fixtureWorkflowBucket(call.Work.GetWorkId()) {
		return status.Error(codes.FailedPrecondition, "fixture persistence bucket mismatch")
	}
	return nil
}

type fixtureIdentityBindingVerifier struct{}

func (fixtureIdentityBindingVerifier) VerifyIdentityBinding(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.Work == nil {
		return status.Error(codes.FailedPrecondition, "fixture WorkRecord binding missing")
	}
	if call.CallerKind == contracts.ParticipantCallerCoordinator && call.OperationID != "operation-"+call.Work.GetWorkId() {
		return status.Error(codes.FailedPrecondition, "fixture operation binding mismatch")
	}
	return nil
}

type fixturePlantWorkRecordVerifier struct{}

func (fixturePlantWorkRecordVerifier) VerifyPlantDependency(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.Work == nil || call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_PLANT || call.ExpectedWorkVersion != 1 {
		return status.Error(codes.FailedPrecondition, "fixture Plant WorkRecord mismatch")
	}
	wantReservation := mustSeedReservationForTest(call.Work.GetWorkId())
	wantCycle := mustCropCycleForTest(call.Work.GetWorkId())
	switch payload := call.Payload.(type) {
	case *contractspb.ReserveSeedPayload:
		if payload.GetPlayerId() != plantTestPlayerID || payload.GetFarmId() != 7001 || payload.GetPlotId() != 3 ||
			payload.GetReservationId() != wantReservation || payload.GetCropDefinitionId() != 1 || payload.GetSeedItemId() != uint32(playeractor.ItemCarrotSeed) ||
			payload.GetQuantity() != 1 || payload.GetCropRuleVersion() != 42 || !bytes.Equal(payload.GetCropRuleSnapshotHash(), bytes.Repeat([]byte{1}, 32)) {
			return status.Error(codes.FailedPrecondition, "fixture Plant WorkRecord crop proof mismatch")
		}
	case *contractspb.CommitSeedReservationPayload:
		if payload.GetPlayerId() != plantTestPlayerID || payload.GetFarmId() != 7001 || payload.GetPlotId() != 3 ||
			payload.GetCropCycleId() != wantCycle || payload.GetReservationId() != wantReservation || !bytes.Equal(payload.GetCommitPlantResultHash(), bytes.Repeat([]byte{2}, 32)) {
			return status.Error(codes.FailedPrecondition, "fixture Farm COMMITTED decision proof mismatch")
		}
	case *contractspb.ReleaseSeedReservationPayload:
		if payload.GetPlayerId() != plantTestPlayerID || payload.GetFarmId() != 7001 || payload.GetPlotId() != 3 ||
			payload.GetReservationId() != wantReservation || payload.GetSeedItemId() != uint32(playeractor.ItemCarrotSeed) || payload.GetQuantity() != 1 ||
			payload.GetReleaseReason() != "FARM_REJECTED" || !bytes.Equal(payload.GetTerminalFarmResultHash(), bytes.Repeat([]byte{3}, 32)) {
			return status.Error(codes.FailedPrecondition, "fixture Farm REJECTED decision proof mismatch")
		}
	default:
		return status.Error(codes.FailedPrecondition, "fixture Plant payload schema mismatch")
	}
	return nil
}

func fixtureWorkflowBucket(workID string) uint32 {
	namespace, err := rediskey.NewNamespace("participant", "z1")
	if err != nil {
		panic(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: plantTestLayout, WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64}
	ref, err := profile.WorkflowBucket(workID)
	if err != nil {
		panic(err)
	}
	return ref.BucketID()
}

func mustSeedReservationForTest(workID string) string {
	id, err := identity.SeedReservationID(workID)
	if err != nil {
		panic(err)
	}
	return id
}

func mustCropCycleForTest(workID string) string {
	id, err := identity.CropCycleID(workID)
	if err != nil {
		panic(err)
	}
	return id
}

func newParticipantRig(t *testing.T, backend string, playerID uint64) *participantTestRig {
	return newParticipantRigWithLimits(t, backend, playerID, authoritystore.Limits{})
}

func newParticipantRigWithLimits(t *testing.T, backend string, playerID uint64, limits authoritystore.Limits) *participantTestRig {
	t.Helper()
	if backend == "legacy" {
		registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 16)
		t.Cleanup(registry.Stop)
		return &participantTestRig{name: backend, registry: registry}
	}
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	trig := &participantTestRig{name: backend, calc: calc}
	var fence authoritystore.ShardWriteFence
	if backend == "memory" {
		store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: plantTestLayout, Now: func() time.Time { return plantTestNow }, Limits: limits})
		if err != nil {
			t.Fatal(err)
		}
		trig.memory = store
		trig.base = store
		set := func(holder string, epoch, lease uint64) authoritystore.ShardWriteFence {
			shard, err := calc.ShardFor(actorrouting.DomainPlayer, playerID)
			if err != nil {
				t.Fatal(err)
			}
			a := authoritystore.ShardWriteAuthority{
				AuthorityLayoutVersion: plantTestLayout, Domain: actorrouting.DomainPlayer, VirtualShard: shard,
				HolderID: holder, HolderBootID: holder + "-boot", AssignmentRevision: epoch,
				WriterEpoch: epoch, LeaseEpoch: lease, State: authoritystore.ShardAuthorityActive,
				LeaseUntilMs: plantTestNow.Add(time.Minute).UnixMilli(),
			}
			if err := store.SetShardWriteAuthority(a); err != nil {
				t.Fatal(err)
			}
			return authoritystore.ShardWriteFence{ExpectedHolderID: holder, ExpectedHolderBootID: holder + "-boot", ExpectedAssignmentRevision: epoch, ExpectedWriterEpoch: epoch, ExpectedLeaseEpoch: lease}
		}
		fence = set("holder-a", 1, 1)
		trig.reassign = func(t *testing.T, holder string) authoritystore.ShardWriteFence {
			t.Helper()
			return set(holder, 2, 2)
		}
	} else {
		server := miniredis.RunT(t)
		server.SetTime(plantTestNow)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		namespace, err := rediskey.NewNamespace("participant", "z1")
		if err != nil {
			t.Fatal(err)
		}
		profile := rediskey.PhysicalProfile{ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: plantTestLayout, WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64}
		store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calc, Limits: limits})
		if err != nil {
			t.Fatal(err)
		}
		trig.base = store
		transition := 0
		set := func(holder string) authoritystore.ShardWriteFence {
			transition++
			shard, err := calc.ShardFor(actorrouting.DomainPlayer, playerID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
				Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager", BootID: "route-manager-boot"},
				TransitionID: fmt.Sprintf("participant-transition-%d", transition), AuthorityLayoutVersion: plantTestLayout,
				Domain: actorrouting.DomainPlayer, VirtualShard: shard, NewHolderID: holder, NewHolderBootID: holder + "-boot", RequestedTTLms: 60_000,
			})
			if err != nil || result.Status != authoritystore.ShardTransitionApplied {
				t.Fatalf("redis shard claim result=%+v err=%v", result, err)
			}
			return authoritystore.ShardWriteFence{ExpectedHolderID: result.Authority.HolderID, ExpectedHolderBootID: result.Authority.HolderBootID, ExpectedAssignmentRevision: result.Authority.AssignmentRevision, ExpectedWriterEpoch: result.Authority.WriterEpoch, ExpectedLeaseEpoch: result.Authority.LeaseEpoch}
		}
		fence = set("holder-a")
		trig.reassign = func(t *testing.T, holder string) authoritystore.ShardWriteFence {
			t.Helper()
			server.SetTime(plantTestNow.Add(2 * time.Minute))
			return set(holder)
		}
	}
	trig.capacity = &participantCapacityGate{ActorAuthorityStore: trig.base}
	trig.fault = authoritystore.NewFaultStore(trig.capacity)
	provider, err := authority.NewProvider(authority.ProviderOptions{Store: trig.fault, Fence: authority.StaticFenceSource{Fence: fence}, AuthorityLayoutVersion: plantTestLayout, BootID: "holder-a-boot"})
	if err != nil {
		t.Fatal(err)
	}
	trig.provider = provider
	trig.registry = playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 64})
	t.Cleanup(trig.registry.Stop)
	return trig
}

func seedPlayer(t *testing.T, registry *playeractor.Registry, playerID uint64, count int64) {
	t.Helper()
	if _, _, err := registry.Ensure(context.Background(), playerID, "bootstrap-participant"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Submit(context.Background(), playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if !playeractor.AddItem(rec, playeractor.ItemCarrotSeed, count) {
			t.Fatalf("seed add failed")
		}
		rec.EconomyVersion++
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func mustReadRecord(t *testing.T, registry *playeractor.Registry) *playerpb.PlayerRecord {
	t.Helper()
	record, _, err := registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func plantWorkID(t *testing.T, operation string) string {
	t.Helper()
	workID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       plantTestPlayerID,
		StableCommandTypeName: "PLANT",
		OperationID:           operation,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "7001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return workID
}

func mustSeedReservation(t *testing.T, workID string) string {
	t.Helper()
	id, err := identity.SeedReservationID(workID)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustCropCycle(t *testing.T, workID string) string {
	t.Helper()
	id, err := identity.CropCycleID(workID)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func reserveRequest(t *testing.T, workID string, playerID uint64, worker bool) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, &contractspb.ReserveSeedPayload{
		PlayerId: playerID, FarmId: 7001, PlotId: 3, ReservationId: mustSeedReservation(t, workID), CropDefinitionId: 1, SeedItemId: uint32(playeractor.ItemCarrotSeed), Quantity: 1, CropRuleVersion: 42, CropRuleSnapshotHash: bytes.Repeat([]byte{1}, 32),
	}, worker)
}

func commitRequest(t *testing.T, workID string, playerID uint64, worker bool) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION, &contractspb.CommitSeedReservationPayload{
		PlayerId: playerID, FarmId: 7001, PlotId: 3, CropCycleId: mustCropCycle(t, workID), ReservationId: mustSeedReservation(t, workID), CommitPlantResultHash: bytes.Repeat([]byte{2}, 32),
	}, worker)
}

func releaseRequest(t *testing.T, workID string, playerID uint64, worker bool) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION, &contractspb.ReleaseSeedReservationPayload{
		PlayerId: playerID, FarmId: 7001, PlotId: 3, ReservationId: mustSeedReservation(t, workID), SeedItemId: uint32(playeractor.ItemCarrotSeed), Quantity: 1, ReleaseReason: "FARM_REJECTED", TerminalFarmResultHash: bytes.Repeat([]byte{3}, 32),
	}, worker)
}

func unimplementedRequest(t *testing.T, workID string, playerID uint64) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN, &contractspb.ReserveUnlockCoinPayload{PlayerId: playerID}, false)
}

func makePlantRequest(t *testing.T, workID string, playerID uint64, command contractspb.ParticipantCommandType, payload proto.Message, worker bool) *contractspb.ParticipantRequest {
	t.Helper()
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	workType := contractspb.WorkType(contractspb.WorkType_value["WORK_TYPE_"+string(spec.WorkType)])
	step := contractspb.ParticipantStep(contractspb.ParticipantStep_value["PARTICIPANT_STEP_"+string(spec.Step)])
	invocation, err := identity.ParticipantInvocationID(spec.WorkType, workID, spec.Step, 1)
	if err != nil {
		t.Fatal(err)
	}
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	virtualShard, err := calc.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	work := &contractspb.WorkAddress{WorkId: workID, WorkType: workType, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1, BucketId: fixtureWorkflowBucket(workID)}
	leaseUntilMs := uint64(participantTestLeaseUntilMs)
	request := &contractspb.ParticipantRequest{RouteHeader: &logicrpcpb.ActorRouteHeader{
		RouteDomain: spec.TargetRouteDomain, ActorId: playerID, VirtualShard: virtualShard, ShardRevision: 1,
		ExpectedHolderId: plantTestHolder, ExpectedHolderBootId: plantTestHolderBoot, ExpectedWriterEpoch: 1,
		ExpectedLeaseEpoch: 1, ExpectedLeaseUntilMs: &leaseUntilMs,
	}, ParticipantCommand: command, TypedPayload: raw}
	if worker {
		request.CallerContext = &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{CallerType: contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY, CallerInstanceId: "worker-1", CallerBootId: "worker-boot-1", Work: work, ExpectedWorkVersion: 1, ParticipantStep: step, ParticipantInvocationId: invocation, LeaseOwnerToken: "lease-token", LeaseEpoch: 1, RemainingBudgetMs: 1000}}
	} else {
		request.CallerContext = &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{Work: work, ExpectedWorkVersion: 1, ParticipantStep: step, ParticipantInvocationId: invocation, OperationId: "operation-" + workID, SubjectPlayerId: playerID, RemainingBudgetMs: 1000}}
	}
	return request
}

func playeractorReservation(rec *playerpb.PlayerRecord, id string) *playerpb.ItemReservation {
	for _, reservation := range rec.GetItemReservations() {
		if reservation != nil && reservation.GetReservationId() == id {
			return reservation
		}
	}
	return nil
}

func mustExecute(t *testing.T, fn func(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error), request *contractspb.ParticipantRequest) *contractspb.ParticipantResponse {
	t.Helper()
	response, err := fn(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func mustUnmarshal(t *testing.T, raw []byte, message proto.Message) {
	t.Helper()
	if err := proto.Unmarshal(raw, message); err != nil {
		t.Fatal(err)
	}
}

func mutateTypedPayload(t *testing.T, request *contractspb.ParticipantRequest, mutate func(proto.Message)) {
	t.Helper()
	spec, err := contracts.LookupParticipantCommand(request.GetParticipantCommand())
	if err != nil {
		t.Fatal(err)
	}
	payload := spec.NewPayload()
	mustUnmarshal(t, request.GetTypedPayload(), payload)
	mutate(payload)
	request.TypedPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
}

func readPlayerVersion(t *testing.T, registry *playeractor.Registry) (*playerpb.PlayerRecord, uint64) {
	t.Helper()
	record, version, err := registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	return record, version
}
