package unlockplot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func TestUnlockPlotCreateIfAbsentAndOperationPayloadConflict(t *testing.T) {
	player := newUnlockScriptedParticipant()
	farm := newUnlockScriptedParticipant()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Repository: workflow.NewMemoryWorkRepository(nil), Player: player, Farm: farm,
		BucketResolver: unlockTestBucketResolver{}, BucketVersion: 1,
		Now: func() time.Time { return time.UnixMilli(1_800_000_000_000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	input := testUnlockInput("unlock-create-conflict")
	first, created, err := coordinator.Create(context.Background(), input)
	if err != nil || !created || first == nil {
		t.Fatalf("first create=%+v created=%v err=%v", first, created, err)
	}
	second, created, err := coordinator.Create(context.Background(), input)
	if err != nil || created || second.WorkVersion != first.WorkVersion {
		t.Fatalf("same intent=%+v created=%v err=%v", second, created, err)
	}
	changed := input
	changed.PlotID = 7
	changed.RequiredPlayerLevel = 2
	changed.AmountNormalCoin = 200
	if _, _, err := coordinator.Create(context.Background(), changed); !errors.Is(err, workflow.ErrIdempotencyConflict) {
		t.Fatalf("same operation modified payload err=%v", err)
	}
}

func TestUnlockPlotCoordinatorResponseLossWorkerResumeStableTerminal(t *testing.T) {
	player := newUnlockScriptedParticipant()
	farm := newUnlockScriptedParticipant()
	clock := func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Repository: workflow.NewMemoryWorkRepository(clock), Player: player, Farm: farm,
		BucketResolver: unlockTestBucketResolver{}, BucketVersion: 1, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := testUnlockInput("unlock-response-loss")
	root, err := BuildRoot(input)
	if err != nil {
		t.Fatal(err)
	}
	player.loseOnce(root.WorkflowID, string(identity.StepReserveUnlockCoin))
	record, _, startErr := coordinator.Start(context.Background(), input)
	if record == nil || startErr == nil || record.State != workflow.WorkStateStepUnknown {
		t.Fatalf("response loss record=%+v err=%v", record, startErr)
	}
	if err := coordinator.Resume(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	// Reserve and final Commit are the two distinct durable Player effects. The
	// response-loss retry must replay Reserve rather than apply a third effect.
	if record.State != workflow.WorkStateCompleted || record.CurrentStep != 2 || player.effectCount(root.WorkflowID) != 2 {
		t.Fatalf("resumed state=%s step=%d effects=%d", record.State, record.CurrentStep, player.effectCount(root.WorkflowID))
	}
	if got := player.callCount(root.WorkflowID, string(identity.StepReserveUnlockCoin)); got != 2 {
		t.Fatalf("reserve calls=%d want response-loss retry 2", got)
	}
	if _, _, err := coordinator.Start(context.Background(), input); err != nil {
		t.Fatalf("terminal same-op replay err=%v", err)
	}
	if got := player.effectCount(root.WorkflowID); got != 2 {
		t.Fatalf("terminal replay duplicated Player effect=%d", got)
	}
}

func testUnlockInput(operation string) UnlockInput {
	ruleHash := DefaultRuleSnapshotHash()
	return UnlockInput{
		SubjectPlayerID: 10001, OperationID: operation, TargetFarmID: 10001,
		PlotID: 6, ExpectedPlotVersion: 1, ExpectedUnlockRuleVersion: 1,
		RequiredPlayerLevel: 1, AmountNormalCoin: 100, RuleSnapshotHash: ruleHash[:],
	}
}

type unlockTestBucketResolver struct{}

func (unlockTestBucketResolver) IndependentWorkBucket(workID string, bucketVersion uint32) (uint32, error) {
	digest := sha256.Sum256(append([]byte{byte(bucketVersion)}, []byte(workID)...))
	return binary.BigEndian.Uint32(digest[:4]), nil
}

func (unlockTestBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 0, errors.New("unlock test does not use source persistence buckets")
}

type unlockScriptedParticipant struct {
	mu       sync.Mutex
	receipts map[string]workflow.StepResult
	lose     map[string]bool
	calls    map[string]int
	effects  map[string]int
}

func newUnlockScriptedParticipant() *unlockScriptedParticipant {
	return &unlockScriptedParticipant{
		receipts: make(map[string]workflow.StepResult), lose: make(map[string]bool),
		calls: make(map[string]int), effects: make(map[string]int),
	}
}

func (p *unlockScriptedParticipant) loseOnce(workID, step string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lose[workID+"\x00"+step] = true
}

func (p *unlockScriptedParticipant) Invoke(_ context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[call.Work.WorkID+"\x00"+call.StepName]++
	if stored, ok := p.receipts[call.InvocationID]; ok {
		stored.Replayed = true
		return stored, nil
	}
	result, err := p.buildResult(call)
	if err != nil {
		return workflow.StepResult{}, err
	}
	p.receipts[call.InvocationID] = result
	p.effects[call.Work.WorkID]++
	key := call.Work.WorkID + "\x00" + call.StepName
	if p.lose[key] {
		delete(p.lose, key)
		return workflow.StepResult{}, errors.New("unlock test response lost after durable participant effect")
	}
	return result, nil
}

func (p *unlockScriptedParticipant) buildResult(call workflow.StepCall) (workflow.StepResult, error) {
	command, err := commandForStep(call.StepName)
	if err != nil {
		return workflow.StepResult{}, err
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return workflow.StepResult{}, err
	}
	var message proto.Message
	switch call.StepName {
	case string(identity.StepReserveUnlockCoin):
		payload := &contractspb.ReserveUnlockCoinPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		message = &contractspb.ReserveUnlockCoinResult{
			ReservationId: payload.GetReservationId(), ReservedAmountNormalCoin: payload.GetAmountNormalCoin(),
			AvailableNormalCoinAfter: 0, PlayerLevelAtReserve: payload.GetRequiredPlayerLevel(), PlayerRecordVersion: 2,
		}
	case string(identity.StepCommitPlotUnlock):
		payload := &contractspb.CommitPlotUnlockPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		message = &contractspb.CommitPlotUnlockResult{
			FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), AppliedUnlockRuleVersion: payload.GetExpectedUnlockRuleVersion(),
			UnlockDecisionHash: bytes.Repeat([]byte{0x73}, 32), CommittedPlotVersion: payload.GetExpectedPlotVersion() + 1, CommittedFarmVersion: 1,
		}
	case string(identity.StepCommitUnlockCoinReservation):
		payload := &contractspb.CommitUnlockCoinReservationPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		message = &contractspb.CommitUnlockCoinReservationResult{
			ReservationId: payload.GetReservationId(), ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED,
			NormalCoinBalance: 0, PlayerRecordVersion: 3,
		}
	case string(identity.StepReleaseUnlockCoinReservation):
		payload := &contractspb.ReleaseUnlockCoinReservationPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		message = &contractspb.ReleaseUnlockCoinReservationResult{
			ReservationId: payload.GetReservationId(), ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED,
			AvailableNormalCoinAfter: payload.GetAmountNormalCoin(), PlayerRecordVersion: 3,
		}
	default:
		return workflow.StepResult{}, fmt.Errorf("unknown UnlockPlot step %q", call.StepName)
	}
	resultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	return workflow.StepResult{Outcome: workflow.StepOutcomeApplied, Result: resultBytes, ResultHash: resultHash}, nil
}

func (p *unlockScriptedParticipant) effectCount(workID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.effects[workID]
}

func (p *unlockScriptedParticipant) callCount(workID, step string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[workID+"\x00"+step]
}
