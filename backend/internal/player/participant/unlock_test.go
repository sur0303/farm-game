package participant

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestUnlockCoinReservationReserveCommitReleaseReplayExactlyOnce(t *testing.T) {
	t.Run("reserve and commit", func(t *testing.T) {
		rig := newParticipantRig(t, "memory", plantTestPlayerID)
		seedPlayer(t, rig.registry, plantTestPlayerID, 0)
		server := newAllowServer(rig.registry)
		workID := unlockWorkID(t, "unlock-reserve-commit")
		reserve := unlockReserveRequest(t, workID, plantTestPlayerID, false, 6, 1, 100)
		first := mustExecute(t, server.ExecuteStep, reserve)
		if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("reserve=%+v", first)
		}
		reserveResult := &contractspb.ReserveUnlockCoinResult{}
		mustUnmarshal(t, first.GetTypedResult(), reserveResult)
		if reserveResult.GetAvailableNormalCoinAfter() != 0 || reserveResult.GetReservedAmountNormalCoin() != 100 {
			t.Fatalf("reserve result=%+v", reserveResult)
		}
		record, _ := readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != 0 || len(record.GetUnlockCoinReservations()) != 1 || record.GetUnlockCoinReservations()[0].GetState() != playerpb.ReservationState_PREPARED {
			t.Fatalf("reserved PlayerRecord=%+v", record)
		}
		beforeEconomy := record.GetEconomyVersion()
		replay := mustExecute(t, server.ExecuteStep, reserve)
		if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) {
			t.Fatalf("reserve replay=%+v first=%+v", replay, first)
		}
		record, _ = readPlayerVersion(t, rig.registry)
		if record.GetEconomyVersion() != beforeEconomy || record.GetWallet().GetCoin() != 0 {
			t.Fatalf("reserve replay changed economy=%d/%d coin=%d", record.GetEconomyVersion(), beforeEconomy, record.GetWallet().GetCoin())
		}
		conflict := proto.Clone(reserve).(*contractspb.ParticipantRequest)
		mutateTypedPayload(t, conflict, func(message proto.Message) {
			message.(*contractspb.ReserveUnlockCoinPayload).AmountNormalCoin = 99
		})
		if _, err := server.ExecuteStep(context.Background(), conflict); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("same invocation modified payload err=%v", err)
		}

		commit := unlockCommitRequest(t, workID, plantTestPlayerID, false, 6, 100)
		committed := mustExecute(t, server.ExecuteStep, commit)
		if committed.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("commit=%+v", committed)
		}
		record, _ = readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != 0 || record.GetUnlockCoinReservations()[0].GetState() != playerpb.ReservationState_COMMITTED {
			t.Fatalf("committed PlayerRecord=%+v", record)
		}
		commitReplay := mustExecute(t, server.ExecuteStep, commit)
		if !commitReplay.GetReplayedFromReceipt() || record.GetWallet().GetCoin() != 0 {
			t.Fatalf("commit replay=%+v", commitReplay)
		}
	})

	t.Run("reserve and release", func(t *testing.T) {
		rig := newParticipantRig(t, "memory", plantTestPlayerID)
		seedPlayer(t, rig.registry, plantTestPlayerID, 0)
		server := newAllowServer(rig.registry)
		workID := unlockWorkID(t, "unlock-reserve-release")
		reserve := unlockReserveRequest(t, workID, plantTestPlayerID, false, 6, 1, 100)
		mustExecute(t, server.ExecuteStep, reserve)
		release := unlockReleaseRequest(t, workID, plantTestPlayerID, false, 6, 100, "FARM_REJECTED")
		first := mustExecute(t, server.ExecuteStep, release)
		if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("release=%+v", first)
		}
		record, _ := readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != 100 || record.GetUnlockCoinReservations()[0].GetState() != playerpb.ReservationState_RELEASED {
			t.Fatalf("released PlayerRecord=%+v", record)
		}
		economy := record.GetEconomyVersion()
		replay := mustExecute(t, server.ExecuteStep, release)
		if !replay.GetReplayedFromReceipt() {
			t.Fatalf("release replay=%+v", replay)
		}
		record, _ = readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != 100 || record.GetEconomyVersion() != economy {
			t.Fatalf("release replay changed PlayerRecord=%+v", record)
		}
	})
}

func TestUnlockCoinReservationInsufficientLevelOrCoinDoesNotDebit(t *testing.T) {
	t.Run("level", func(t *testing.T) {
		rig := newParticipantRig(t, "memory", plantTestPlayerID)
		seedPlayer(t, rig.registry, plantTestPlayerID, 0)
		server := newAllowServer(rig.registry)
		response := mustExecute(t, server.ExecuteStep, unlockReserveRequest(t, unlockWorkID(t, "unlock-level-low"), plantTestPlayerID, false, 7, 2, 200))
		if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || response.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_LEVEL_REQUIREMENT_NOT_MET {
			t.Fatalf("level rejection=%+v", response)
		}
		record, _ := readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin || len(record.GetUnlockCoinReservations()) != 0 {
			t.Fatalf("level rejection mutated coin/reservations=%+v", record)
		}
	})

	t.Run("coin", func(t *testing.T) {
		rig := newParticipantRig(t, "memory", plantTestPlayerID)
		seedPlayer(t, rig.registry, plantTestPlayerID, 0)
		if _, err := rig.registry.Submit(context.Background(), plantTestPlayerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
			record.Wallet.Coin = 0
			return nil, true, nil
		}); err != nil {
			t.Fatal(err)
		}
		server := newAllowServer(rig.registry)
		response := mustExecute(t, server.ExecuteStep, unlockReserveRequest(t, unlockWorkID(t, "unlock-coin-low"), plantTestPlayerID, false, 6, 1, 100))
		if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || response.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_COIN {
			t.Fatalf("coin rejection=%+v", response)
		}
		record, _ := readPlayerVersion(t, rig.registry)
		if record.GetWallet().GetCoin() != 0 || len(record.GetUnlockCoinReservations()) != 0 {
			t.Fatalf("coin rejection mutated reservations=%+v", record)
		}
	})
}

type fixtureUnlockWorkRecordVerifier struct{}

func (fixtureUnlockWorkRecordVerifier) VerifyUnlockDependency(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.Work == nil || call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT || call.ExpectedWorkVersion != 1 {
		return status.Error(codes.FailedPrecondition, "fixture UnlockPlot WorkRecord mismatch")
	}
	return nil
}

func unlockWorkID(t *testing.T, operation string) string {
	t.Helper()
	workID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       plantTestPlayerID,
		StableCommandTypeName: string(identity.WorkTypeUnlockPlot),
		OperationID:           operation,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(plantTestPlayerID, 10)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return workID
}

func unlockRuleHash() []byte {
	ruleHash := domain.PlotUnlockRuleSnapshotHash(domain.DefaultPlotUnlockRule())
	return ruleHash[:]
}

func unlockReservationID(t *testing.T, workID string) string {
	t.Helper()
	reservationID, err := identity.UnlockCoinReservationID(workID)
	if err != nil {
		t.Fatal(err)
	}
	return reservationID
}

func unlockReserveRequest(t *testing.T, workID string, playerID uint64, worker bool, plotID uint32, level uint32, amount uint64) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN, &contractspb.ReserveUnlockCoinPayload{
		PlayerId: playerID, FarmId: playerID, PlotId: plotID, ReservationId: unlockReservationID(t, workID),
		RequiredPlayerLevel: level, AmountNormalCoin: amount, UnlockRuleVersion: 1, UnlockRuleSnapshotHash: unlockRuleHash(),
	}, worker)
}

func unlockCommitRequest(t *testing.T, workID string, playerID uint64, worker bool, plotID uint32, amount uint64) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION, &contractspb.CommitUnlockCoinReservationPayload{
		PlayerId: playerID, FarmId: playerID, PlotId: plotID, ReservationId: unlockReservationID(t, workID), AmountNormalCoin: amount,
		CommitPlotUnlockResultHash: bytes.Repeat([]byte{0x71}, 32),
	}, worker)
}

func unlockReleaseRequest(t *testing.T, workID string, playerID uint64, worker bool, plotID uint32, amount uint64, reason string) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION, &contractspb.ReleaseUnlockCoinReservationPayload{
		PlayerId: playerID, FarmId: playerID, PlotId: plotID, ReservationId: unlockReservationID(t, workID), AmountNormalCoin: amount,
		ReleaseReason: reason, TerminalFarmResultHash: bytes.Repeat([]byte{0x72}, 32),
	}, worker)
}
