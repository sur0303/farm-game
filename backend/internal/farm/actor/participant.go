package actor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ParticipantReceiptOperationKey is the explicit internal namespace for Farm
// Participant receipts.  It cannot be confused with a public operation_id or
// with the legacy FarmOperationReceipt carrier used by the old command path.
const participantReceiptOperationPrefix = "\x00FI1-A1-FARM-PARTICIPANT-RECEIPT-V1\x00"

const (
	participantProofMagic      = "FARM-COMMIT-PLANT-RESULT-PROOF-V1"
	harvestProofMagic          = "FARM-COMMIT-HARVEST-RESULT-PROOF-V1"
	theftProofMagic            = "FARM-DECIDE-THEFT-RESULT-PROOF-V1"
	guardSyncProofMagic        = "FARM-APPLY-GUARD-SNAPSHOT-RESULT-PROOF-V1"
	farmMutationReceiptMagic   = "FARM-MUTATION-RESULT-HEADER-V1"
	participantProofVersion    = uint32(1)
	farmMutationReceiptVersion = uint32(1)
	participantMaxProofBytes   = 64 * 1024
	participantMaxCareItems    = 32
	participantMaxPlots        = 24
)

// TheftPRF is the Farm-side random provider.  Its input is the deterministic
// theft attempt and the already-installed GuardSnapshot; no request field can
// influence the draw.  The committed GuardDecisionSnapshot is the replay
// authority after this function returns.
type TheftPRF func(theftAttemptID string, guard domain.GuardSnapshot) (uint32, error)

// HMACTheftPRF constructs the production composition seam recommended by the
// Theft contract.  A deployment supplies a secret key; the fallback provider
// below exists only for local compatibility fixtures.
func HMACTheftPRF(secret []byte) TheftPRF {
	key := append([]byte(nil), secret...)
	return func(theftAttemptID string, guard domain.GuardSnapshot) (uint32, error) {
		if len(key) < 16 {
			return 0, errors.New("farm participant: theft PRF secret is too short")
		}
		mac := hmac.New(sha256.New, key)
		writeParticipantLP(mac, []byte("farm-theft-prf-v1"))
		writeParticipantLP(mac, []byte(theftAttemptID))
		writeParticipantUint64(mac, guard.GuardVersion)
		writeParticipantUint64(mac, guard.Rule.GuardRuleVersion)
		writeParticipantUint64(mac, guard.Rule.RandomAlgorithmVersion)
		writeParticipantUint64(mac, guard.Rule.RandomKeyVersion)
		var digest [32]byte
		copy(digest[:], mac.Sum(nil))
		return binary.BigEndian.Uint32(digest[:4]) % 10000, nil
	}
}

func defaultTheftPRF(theftAttemptID string, guard domain.GuardSnapshot) (uint32, error) {
	// A deterministic fallback keeps the actor usable in existing local
	// fixtures. Production must inject HMACTheftPRF with a deployment secret.
	return HMACTheftPRF([]byte("farmgame-local-theft-prf-v1"))(theftAttemptID, guard)
}

// participantPublicPlotIndex is the sole boundary adapter between the
// Participant contract's 1-based PlotID and the legacy Farm projection's
// 0-based Plot.Index. All Participant reads/writes go through it so Plot 1
// cannot silently become public index 1 and Plot 24 cannot create index 24.
func participantPublicPlotIndex(plotID uint32) (int32, bool) {
	if plotID == 0 || plotID > participantMaxPlots {
		return 0, false
	}
	return int32(plotID - 1), true
}

func participantPlotID(publicIndex int32) (uint32, bool) {
	if publicIndex < 0 || publicIndex >= participantMaxPlots {
		return 0, false
	}
	return uint32(publicIndex) + 1, true
}

// ParticipantReceiptOperationKey returns the durable operation key for one
// ParticipantInvocationID.  The namespace is part of the persistence
// contract; callers must not substitute the public operation ID.
func ParticipantReceiptOperationKey(invocationID string) string {
	return participantReceiptOperationPrefix + invocationID
}

type participantCropState struct {
	cycle         domain.CropCycle
	rule          *contractspb.CropRuleSnapshot
	ruleHash      []byte
	stealSnapshot *contractspb.StealPlanSnapshot
	careSnapshot  *contractspb.CarePlanSnapshot
	compendium    *contractspb.CompendiumUnlockSnapshot
}

type participantFarmState struct {
	state domain.FarmState
}

// CommitPlantTurnResult is the implementation-only bridge between the Farm
// Participant mailbox turn and the existing realtime ActionResult path.
//
// The typed Participant response is intentionally kept separate from this
// value: the typed result and its hash are still produced from the durable
// ParticipantReceipt, while Action carries the one FarmChange tuple that the
// realtime handoff already understands.
type CommitPlantTurnResult struct {
	Outcome *contracts.StepOutcome
	Action  *ActionResult

	deliveryErr error
}

// CommitHarvestTurnResult is the implementation-only bridge between the Farm
// Participant mailbox turn and the existing realtime FarmChange path. The
// typed result is recovered from the durable receipt; Action is only the
// one candidate change captured from the same committed record.
type CommitHarvestTurnResult struct {
	Outcome *contracts.StepOutcome
	Action  *ActionResult

	deliveryErr error
}

// DecideTheftTurnResult carries the one FarmChange candidate from the same
// mailbox turn as the typed TheftTargetDecision.  No Player/Workflow call is
// made from this value or from the actor turn.
type DecideTheftTurnResult struct {
	Outcome *contracts.StepOutcome
	Action  *ActionResult

	deliveryErr error
}

// ApplyGuardSnapshotTurnResult carries the optional public meta-only change
// committed by the same FarmActor turn as the GuardSync receipt. The typed
// result remains rooted in the durable receipt; observers run only after the
// mailbox turn has completed.
type ApplyGuardSnapshotTurnResult struct {
	Outcome *contracts.StepOutcome
	Action  *ActionResult

	deliveryErr error
}

// resetParticipantCache is called after an authoritative reload.  The
// durable FarmOperationReceipt proof is the only source used to rebuild the
// extra frozen participant fields; no current configuration is consulted.
func (a *FarmActor) resetParticipantCache() error {
	a.participantState = nil
	a.participantCycles = nil
	a.theftGuardSnapshot = domain.GuardSnapshot{}
	return a.restoreParticipantGuardSnapshot()
}

// SetParticipantPlotUnlockedForTest seeds a legal domain fixture for a
// package-level test.  Production unlocks are intentionally out of scope for
// FI1-A1 and are not exposed as a Participant command.
func (a *FarmActor) SetParticipantPlotUnlockedForTest(plotID uint32, unlocked bool) error {
	if a == nil || plotID == 0 || plotID > participantMaxPlots {
		return errors.New("farm actor: participant plot id out of range")
	}
	publicIndex, ok := participantPublicPlotIndex(plotID)
	if !ok || a.record == nil || a.record.Core == nil {
		return errors.New("farm actor: participant plot is not durable")
	}
	var target *farmpb.Plot
	for _, candidate := range a.record.Core.Plots {
		if candidate != nil && candidate.Index == publicIndex {
			target = candidate
			break
		}
	}
	if target == nil {
		return errors.New("farm actor: participant plot is not durable")
	}
	if unlocked {
		target.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
	} else {
		target.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
	}
	if a.participantState != nil {
		plot := a.participantState.state.FindPlot(plotID)
		if plot != nil {
			if unlocked {
				plot.LockState = domain.PlotUnlocked
			} else {
				plot.LockState = domain.PlotLocked
			}
		}
	}
	return nil
}

// SetParticipantGuardSnapshot installs the latest locally synchronized guard
// snapshot for the next FarmActor turn.  GuardSync owns the production call
// site; this small boundary is also useful to deterministic Farm tests.
func (a *FarmActor) SetParticipantGuardSnapshot(snapshot domain.GuardSnapshot) error {
	if a == nil {
		return errors.New("farm actor: participant guard snapshot target is nil")
	}
	if snapshot.GuardVersion != 0 {
		if snapshot.SchemaVersion == 0 || snapshot.SnapshotHash == "" ||
			snapshot.SnapshotHash != domain.CanonicalGuardSnapshotHash(snapshot) {
			return errors.New("farm actor: participant guard snapshot hash is invalid")
		}
		if err := domain.ValidateGuardRule(snapshot.Rule); err != nil {
			return err
		}
	}
	current := a.theftGuardSnapshot
	switch {
	case snapshot.GuardVersion < current.GuardVersion:
		return nil
	case snapshot.GuardVersion == current.GuardVersion && current.GuardVersion != 0:
		if snapshot.SnapshotHash != current.SnapshotHash {
			return errors.New("farm actor: participant guard snapshot version conflicts with Farm authority")
		}
		return nil
	}
	a.theftGuardSnapshot = snapshot
	if a.participantState != nil {
		a.participantState.state.Guard = snapshot
	}
	return nil
}

// ValidateApplyGuardSnapshotPayload is the Farm-side GuardSync dependency
// check. The shared Participant ingress has already fenced the target route
// and authoritative Work version; this check additionally binds the source
// PLAYER_ACTOR order identity, owner, immutable snapshot and canonical hash
// before the target FarmActor mailbox is entered.
func ValidateApplyGuardSnapshotPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "farm participant GuardSync call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return status.Error(codes.Unimplemented, "farm participant command is not GuardSync")
	}
	payload, ok := call.Payload.(*contractspb.ApplyGuardSnapshotPayload)
	if !ok || payload == nil || payload.GetGuardSnapshot() == nil {
		return status.Error(codes.Internal, "farm participant GuardSync payload schema mismatch")
	}
	source := call.Work.GetSourceAuthority()
	if call.RouteHeader.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		payload.GetTargetFarmId() == 0 || payload.GetTargetFarmId() != call.RouteHeader.GetActorId() ||
		payload.GetSourcePlayerId() == 0 || payload.GetSourcePlayerId() != payload.GetTargetFarmId() ||
		(call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetSourcePlayerId()) || payload.GetOrderId() == "" ||
		payload.GetOrderId() != call.Work.GetWorkId() || source == nil ||
		source.GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		source.GetCanonicalAuthorityId() != fmt.Sprintf("%d", payload.GetSourcePlayerId()) ||
		source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		return status.Error(codes.FailedPrecondition, "farm participant GuardSync source/target binding is invalid")
	}
	snapshot := payload.GetGuardSnapshot()
	if snapshot.GetGuardVersion() == 0 {
		return status.Error(codes.InvalidArgument, "farm participant GuardSync version is required")
	}
	wantOrderID, err := identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor,
		fmt.Sprintf("%d", payload.GetSourcePlayerId()), "PET_GUARD_SLOT", snapshot.GetGuardVersion())
	if err != nil || payload.GetOrderId() != wantOrderID {
		return status.Error(codes.FailedPrecondition, "farm participant GuardSync order identity is invalid")
	}
	if _, err := participantDomainGuardSnapshot(payload); err != nil {
		return err
	}
	return nil
}

func participantDomainGuardSnapshot(payload *contractspb.ApplyGuardSnapshotPayload) (domain.GuardSnapshot, error) {
	if payload == nil || payload.GetGuardSnapshot() == nil {
		return domain.GuardSnapshot{}, status.Error(codes.InvalidArgument, "farm participant GuardSnapshot is required")
	}
	wire := payload.GetGuardSnapshot()
	if wire.GetSchemaVersion() == 0 || wire.GetGuardVersion() == 0 || wire.GetGuardRule() == nil ||
		len(wire.GetSnapshotHash()) != sha256.Size || len(payload.GetSnapshotHash()) != sha256.Size ||
		!bytes.Equal(wire.GetSnapshotHash(), payload.GetSnapshotHash()) {
		return domain.GuardSnapshot{}, status.Error(codes.FailedPrecondition, "farm participant GuardSnapshot hash binding is invalid")
	}
	rule := wire.GetGuardRule()
	interceptPolicy := domain.InterceptPolicyUnspecified
	if rule.GetInterceptPolicy() == contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS {
		interceptPolicy = domain.InterceptPolicyFullGross
	}
	snapshot := domain.GuardSnapshot{
		SchemaVersion: uint64(wire.GetSchemaVersion()), GuardVersion: wire.GetGuardVersion(),
		ActiveGuardPetID: wire.GetActiveGuardPetId(), GuardUntilMs: wire.GetGuardUntilMs(),
		Rule: domain.GuardRule{
			PetConfigVersion: rule.GetPetConfigVersion(), GuardRuleVersion: rule.GetGuardRuleVersion(),
			TriggerProbabilityBP: rule.GetTriggerProbabilityBp(), InterceptPolicy: interceptPolicy,
			RandomAlgorithmVersion: rule.GetRandomAlgorithmVersion(), RandomKeyVersion: rule.GetRandomKeyVersion(),
		},
		SnapshotHash: hex.EncodeToString(wire.GetSnapshotHash()),
	}
	if err := domain.ValidateGuardRule(snapshot.Rule); err != nil {
		return domain.GuardSnapshot{}, status.Error(codes.FailedPrecondition, "farm participant GuardSnapshot rule is invalid")
	}
	want, err := hex.DecodeString(domain.CanonicalGuardSnapshotHash(snapshot))
	if err != nil || !bytes.Equal(want, wire.GetSnapshotHash()) {
		return domain.GuardSnapshot{}, status.Error(codes.FailedPrecondition, "farm participant GuardSnapshot canonical hash is invalid")
	}
	return snapshot, nil
}

func participantProtoGuardSnapshot(snapshot domain.GuardSnapshot) (*contractspb.GuardSnapshot, error) {
	hash, err := hex.DecodeString(snapshot.SnapshotHash)
	if err != nil || len(hash) != sha256.Size || snapshot.SchemaVersion > math.MaxUint32 {
		return nil, errors.New("farm participant: persisted GuardSnapshot hash is invalid")
	}
	activePetID := snapshot.ActiveGuardPetID
	return &contractspb.GuardSnapshot{
		SchemaVersion: uint32(snapshot.SchemaVersion), GuardVersion: snapshot.GuardVersion,
		ActiveGuardPetId: &activePetID, GuardUntilMs: snapshot.GuardUntilMs,
		GuardRule: &contractspb.GuardRuleSnapshot{
			PetConfigVersion: snapshot.Rule.PetConfigVersion, GuardRuleVersion: snapshot.Rule.GuardRuleVersion,
			TriggerProbabilityBp: snapshot.Rule.TriggerProbabilityBP, InterceptPolicy: theftGuardPolicyProto(snapshot.Rule.InterceptPolicy),
			RandomAlgorithmVersion: snapshot.Rule.RandomAlgorithmVersion, RandomKeyVersion: snapshot.Rule.RandomKeyVersion,
		},
		SnapshotHash: hash,
	}, nil
}

// ParticipantFarmOpportunity returns the last domain opportunity header
// known by the actor.  It is a read-only diagnostic seam; the header is
// advanced only as part of a successful candidate commit.
func (a *FarmActor) ParticipantFarmOpportunity() (domain.OpportunityHeader, bool) {
	if a == nil || a.participantState == nil {
		return domain.OpportunityHeader{}, false
	}
	return a.participantState.state.Header, true
}

// ValidateCommitPlantPayload is the Farm business-payload validator shared by
// ingress admission and the in-mailbox handler.  It is intentionally layered
// on top of contracts.ValidateAndDecode; it does not decode a second wire
// format or implement another Participant ingress kernel.
func ValidateCommitPlantPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlant call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
		return status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	payload, ok := call.Payload.(*contractspb.CommitPlantPayload)
	if !ok || payload == nil {
		return status.Error(codes.Internal, "farm participant CommitPlant payload schema mismatch")
	}
	if call.RouteHeader.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		payload.GetFarmId() == 0 || payload.GetFarmId() != call.RouteHeader.GetActorId() ||
		payload.GetOwnerPlayerId() == 0 || payload.GetOwnerPlayerId() != payload.GetFarmId() ||
		payload.GetPlotId() == 0 || payload.GetPlotId() > participantMaxPlots ||
		payload.GetExpectedPlotVersion() == 0 {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlant target/version is invalid")
	}
	if call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetOwnerPlayerId() {
		return status.Error(codes.FailedPrecondition, "farm participant CommitPlant owner binding is invalid")
	}
	if len(payload.GetReserveSeedResultHash()) != sha256.Size || len(payload.GetCropRuleSnapshotHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlant dependency hash is invalid")
	}
	wantReservation, err := identity.SeedReservationID(call.Work.GetWorkId())
	if err != nil || payload.GetReservationId() != wantReservation {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlant reservation binding is invalid")
	}
	wantCycle, err := identity.CropCycleID(call.Work.GetWorkId())
	if err != nil || payload.GetCropCycleId() != wantCycle {
		return status.Error(codes.InvalidArgument, "farm participant CommitPlant crop cycle binding is invalid")
	}
	if err := validateCropRuleSnapshot(payload.GetCropRuleSnapshot()); err != nil {
		return err
	}
	ruleHash, err := contracts.CanonicalHash(payload.GetCropRuleSnapshot())
	if err != nil || !bytes.Equal(ruleHash[:], payload.GetCropRuleSnapshotHash()) {
		return status.Error(codes.FailedPrecondition, "farm participant CropRuleSnapshot hash is inconsistent")
	}
	return nil
}

// ValidateCommitHarvestPayload is the pure business-payload validator shared
// by ingress admission and the in-mailbox handler. The authoritative owner,
// cycle and plot-version checks are repeated by DecideHarvest in the actor
// turn; this function only rejects a malformed wire shape before dependency
// proof or mailbox entry.
func ValidateCommitHarvestPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "farm participant CommitHarvest call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST {
		return status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	payload, ok := call.Payload.(*contractspb.CommitHarvestPayload)
	if !ok || payload == nil {
		return status.Error(codes.Internal, "farm participant CommitHarvest payload schema mismatch")
	}
	if call.RouteHeader.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		payload.GetFarmId() == 0 || payload.GetFarmId() != call.RouteHeader.GetActorId() ||
		payload.GetOwnerPlayerId() == 0 || payload.GetPlotId() == 0 || payload.GetPlotId() > participantMaxPlots ||
		payload.GetCropCycleId() == "" || payload.GetExpectedPlotVersion() == 0 {
		return status.Error(codes.InvalidArgument, "farm participant CommitHarvest target/version is invalid")
	}
	if call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetOwnerPlayerId() {
		return status.Error(codes.FailedPrecondition, "farm participant CommitHarvest owner binding is invalid")
	}
	return nil
}

// ValidateDecideTheftPayload is the Theft-specific layer on the shared
// Participant ingress kernel.  It validates only frozen identity/binding
// facts; gross quantity, slot, guard result and unit price are all derived in
// the FarmActor turn.
func ValidateDecideTheftPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		return status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	payload, ok := call.Payload.(*contractspb.DecideTheftPayload)
	if !ok || payload == nil {
		return status.Error(codes.Internal, "farm participant DECIDE_THEFT payload schema mismatch")
	}
	if call.RouteHeader.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		payload.GetFarmId() == 0 || payload.GetFarmId() != call.RouteHeader.GetActorId() ||
		payload.GetOwnerPlayerId() == 0 || payload.GetOwnerPlayerId() != payload.GetFarmId() ||
		payload.GetVisitorPlayerId() == 0 || payload.GetVisitorPlayerId() == payload.GetOwnerPlayerId() ||
		payload.GetPlotId() == 0 || payload.GetPlotId() > participantMaxPlots ||
		payload.GetCropCycleId() == "" || payload.GetTheftAttemptId() == "" || payload.GetSettlementId() == "" {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT target binding is invalid")
	}
	if call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetVisitorPlayerId() {
		return status.Error(codes.FailedPrecondition, "farm participant DECIDE_THEFT visitor subject binding is invalid")
	}
	if call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_THEFT || call.Work.GetWorkId() == "" {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT Work binding is invalid")
	}
	wantAttempt, err := identity.TheftAttemptID(payload.GetFarmId(), payload.GetCropCycleId(), payload.GetVisitorPlayerId())
	if err != nil || payload.GetTheftAttemptId() != wantAttempt {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT theft_attempt_id binding is invalid")
	}
	wantSettlement, err := identity.TheftSettlementID(call.Work.GetWorkId())
	if err != nil || payload.GetSettlementId() != wantSettlement {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT settlement_id binding is invalid")
	}
	permit := payload.GetActionPermitSnapshot()
	if permit == nil || permit.GetPermitId() == "" || permit.GetVisitorPlayerId() != payload.GetVisitorPlayerId() ||
		permit.GetOwnerPlayerId() != payload.GetOwnerPlayerId() || permit.GetTargetFarmId() != payload.GetFarmId() ||
		permit.GetActionScope() != contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT ||
		permit.GetFriendRelationEpoch() == 0 || permit.GetOperationId() == "" ||
		len(permit.GetCanonicalPayloadHash()) != sha256.Size || len(permit.GetTargetBindingHash()) != sha256.Size ||
		permit.GetIssuedAtAuthorityMs() <= 0 || permit.GetExpireAtAuthorityMs() <= permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() < permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() >= permit.GetExpireAtAuthorityMs() {
		return status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT ActionPermitSnapshot is invalid")
	}
	if call.CallerKind == contracts.ParticipantCallerCoordinator && call.OperationID != permit.GetOperationId() {
		return status.Error(codes.FailedPrecondition, "farm participant DECIDE_THEFT execution-accept operation binding is invalid")
	}
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil || !bytes.Equal(permitHash[:], payload.GetActionPermitSnapshotHash()) || len(payload.GetActionPermitSnapshotHash()) != sha256.Size {
		return status.Error(codes.FailedPrecondition, "farm participant DECIDE_THEFT ActionPermitSnapshot hash is inconsistent")
	}
	wantPayloadHash, err := contracts.CanonicalParticipantHash(call.Spec, payload)
	if err != nil || !bytes.Equal(wantPayloadHash[:], call.CanonicalStepPayloadHash[:]) {
		return status.Error(codes.FailedPrecondition, "farm participant DECIDE_THEFT typed payload hash is inconsistent")
	}
	return nil
}

func validateCropRuleSnapshot(snapshot *contractspb.CropRuleSnapshot) error {
	if snapshot == nil || snapshot.GetSchemaVersion() != 1 || snapshot.GetCropDefinitionId() == 0 ||
		snapshot.GetCropRuleVersion() == 0 || snapshot.GetSeedItemId() == 0 || snapshot.GetHarvestItemId() == 0 ||
		snapshot.GetGrowDurationMs() <= 0 || snapshot.GetBaseYield() == 0 ||
		snapshot.GetBaseYield() > uint64(math.MaxInt32) || snapshot.GetBaseYield()%50 != 0 ||
		snapshot.GetCropDefinitionId() > uint32(math.MaxInt32) {
		return status.Error(codes.InvalidArgument, "farm participant CropRuleSnapshot is incomplete")
	}
	if snapshot.GetCropRuleVersion() > uint64(math.MaxInt32) {
		// The existing public Farm projection has a signed int32 rule field;
		// reject a value it cannot represent instead of truncating it.
		return status.Error(codes.InvalidArgument, "farm participant CropRuleSnapshot rule version is out of range")
	}
	compendium := snapshot.GetCompendiumRule()
	if compendium == nil || compendium.GetCropDefinitionId() != snapshot.GetCropDefinitionId() ||
		compendium.GetCatalogVersionAtPlant() == 0 || compendium.GetDefinitionVersionAtPlant() == 0 {
		return status.Error(codes.InvalidArgument, "farm participant compendium rule snapshot is incomplete")
	}
	steal := snapshot.GetStealPlanTemplate()
	if steal == nil || steal.GetPolicyVersion() == 0 || steal.GetTotalCapRatioBp() != 2000 || len(steal.GetSlotRatioBp()) != 10 {
		return status.Error(codes.InvalidArgument, "farm participant steal plan snapshot is not P0")
	}
	for _, ratio := range steal.GetSlotRatioBp() {
		if ratio != 200 {
			return status.Error(codes.InvalidArgument, "farm participant steal plan must have ten 2-percent slots")
		}
	}
	care := snapshot.GetCarePlanTemplate()
	if care == nil || care.GetCareRuleVersion() == 0 || len(care.GetIncidents()) > participantMaxCareItems {
		return status.Error(codes.InvalidArgument, "farm participant care plan snapshot is incomplete")
	}
	seen := make(map[string]struct{}, len(care.GetIncidents()))
	seenCareTypes := make(map[contractspb.CareType]struct{}, 2)
	for _, incident := range care.GetIncidents() {
		if incident == nil || incident.GetOrdinal() != 1 {
			return status.Error(codes.InvalidArgument, "farm participant care incident template is incomplete")
		}
		var careName string
		switch incident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER:
			careName = "WATER"
		case contractspb.CareType_CARE_TYPE_REMOVE_PEST:
			careName = "REMOVE_PEST"
		default:
			return status.Error(codes.InvalidArgument, "farm participant care incident type is unknown")
		}
		if _, exists := seenCareTypes[incident.GetCareType()]; exists {
			return status.Error(codes.InvalidArgument, "farm participant care plan has duplicate incident type")
		}
		seenCareTypes[incident.GetCareType()] = struct{}{}
		key := fmt.Sprintf("%s/%d", careName, incident.GetOrdinal())
		if _, exists := seen[key]; exists {
			return status.Error(codes.InvalidArgument, "farm participant care incident ordinal is duplicated")
		}
		seen[key] = struct{}{}
		switch incident.GetScheduleMode() {
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET:
			if incident.FixedOffsetMs == nil || incident.GetFixedOffsetMs() <= 0 || incident.GetFixedOffsetMs() >= snapshot.GetGrowDurationMs() ||
				incident.RandomWindowStartMs != nil || incident.RandomWindowEndMs != nil || incident.RandomAlgorithmVersion != nil {
				return status.Error(codes.InvalidArgument, "farm participant fixed care incident offset is invalid")
			}
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_DETERMINISTIC_RANDOM_WINDOW:
			if incident.RandomWindowStartMs == nil || incident.RandomWindowEndMs == nil || incident.RandomAlgorithmVersion == nil ||
				incident.GetRandomAlgorithmVersion() == 0 || incident.GetRandomWindowStartMs() <= 0 ||
				incident.GetRandomWindowEndMs() < incident.GetRandomWindowStartMs() || incident.GetRandomWindowEndMs() >= snapshot.GetGrowDurationMs() ||
				incident.FixedOffsetMs != nil {
				return status.Error(codes.InvalidArgument, "farm participant random care incident window is invalid")
			}
		default:
			return status.Error(codes.InvalidArgument, "farm participant care incident schedule is unknown")
		}
	}
	return nil
}

// buildParticipantState converts the existing public Farm projection into the
// domain state consumed by DecidePlant.  The conversion is a boundary adapter;
// the decision itself never receives a generated Proto as its authority state.
func (a *FarmActor) buildParticipantState() domain.FarmState {
	state := domain.FarmState{FarmID: a.farmID, OwnerPlayerID: a.farmID}
	var publicPlots [participantMaxPlots]*farmpb.Plot
	if a != nil && a.record != nil && a.record.Core != nil {
		state.FarmVersion = a.record.Core.FarmVersion
		for _, publicPlot := range a.record.Core.Plots {
			if publicPlot == nil || publicPlot.GetIndex() < 0 || publicPlot.GetIndex() >= participantMaxPlots {
				continue
			}
			publicPlots[publicPlot.GetIndex()] = publicPlot
		}
	}
	state.Plots = make([]domain.Plot, participantMaxPlots)
	for plotID := uint32(1); plotID <= participantMaxPlots; plotID++ {
		plot := domain.Plot{PlotID: plotID, LockState: domain.PlotLocked, PlotVersion: 1}
		if plotID <= 5 {
			plot.LockState = domain.PlotUnlocked
		}
		publicIndex, _ := participantPublicPlotIndex(plotID)
		if publicPlot := publicPlots[publicIndex]; publicPlot != nil {
			if publicPlot.GetPlotVersion() != 0 {
				plot.PlotVersion = publicPlot.GetPlotVersion()
			}
			if publicPlot.GetLockState() == farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED {
				plot.LockState = domain.PlotUnlocked
			} else {
				plot.LockState = domain.PlotLocked
			}
			plot.ActiveCropCycleID = publicPlot.GetActiveCropCycleId()
		}
		state.Plots[plotID-1] = plot
	}
	if a != nil && a.record != nil && a.record.Core != nil {
		state.Cycles = make([]domain.CropCycle, 0, len(a.record.Core.CropCycles))
		for _, publicCycle := range a.record.Core.CropCycles {
			if publicCycle == nil || publicCycle.GetCropCycleId() == "" {
				continue
			}
			if a.participantCycles == nil {
				a.participantCycles = make(map[string]*participantCropState)
			}
			if _, exists := a.participantCycles[publicCycle.GetCropCycleId()]; !exists {
				if proof, ok := participantCropFromReceipts(a.record, publicCycle.GetCropCycleId()); ok {
					a.participantCycles[publicCycle.GetCropCycleId()] = proof
				}
			}
			cycle := participantDomainCycle(publicCycle, a.participantCycles[publicCycle.GetCropCycleId()])
			mergeParticipantTheftCycle(a.record, &cycle)
			state.Cycles = append(state.Cycles, cycle)
		}
	}
	if header, ok := participantLatestOpportunityHeader(a.record); ok {
		state.Header = header
	}
	if a.participantState != nil {
		state.Guard = a.participantState.state.Guard
	}
	if a.theftGuardSnapshot.GuardVersion != 0 || a.theftGuardSnapshot.ActiveGuardPetID != 0 {
		state.Guard = a.theftGuardSnapshot
	}
	return state
}

func participantDomainCycle(publicCycle *farmpb.CropCycle, frozen *participantCropState) domain.CropCycle {
	if frozen != nil {
		cycle := cloneDomainCycle(frozen.cycle)
		// The typed Plant proof owns immutable rule facts; the mutable theft
		// total is also reflected in the existing Farm projection by the
		// legacy Theft path. Merge only that monotonic settlement fact so a
		// later Harvest cannot silently restore the pre-theft quantity.
		if publicCycle != nil && publicCycle.GetStolenTotal() >= 0 {
			cycle.StolenAmount = uint32(publicCycle.GetStolenTotal())
		}
		return cycle
	}
	cycle := domain.CropCycle{
		CycleID:          publicCycle.GetCropCycleId(),
		CropDefinitionID: uint32(max(publicCycle.GetCropDefinitionId(), 0)), RuleVersion: uint64(max(publicCycle.GetRuleVersion(), 0)),
		PlantAtMs: publicCycle.GetPlantAt(), MatureAtMs: publicCycle.GetMatureAt(),
		BaseYield: uint32(max(publicCycle.GetBaseYield(), 0)), StealableLimit: uint32(max(publicCycle.GetStealableLimit(), 0)),
		StolenAmount: uint32(max(publicCycle.GetStolenTotal(), 0)),
	}
	if plotID, ok := participantPlotID(publicCycle.GetPlotIndex()); ok {
		cycle.PlotID = plotID
	}
	return cycle
}

func cloneDomainCycle(cycle domain.CropCycle) domain.CropCycle {
	clone := cycle
	clone.StealPlan.SlotRatioBP = append([]uint32(nil), cycle.StealPlan.SlotRatioBP...)
	clone.StealPlan.SlotQuantity = append([]uint32(nil), cycle.StealPlan.SlotQuantity...)
	clone.Incidents = append([]domain.CareIncident(nil), cycle.Incidents...)
	clone.Attempts = append([]domain.TheftAttemptRecord(nil), cycle.Attempts...)
	return clone
}

func participantCropFromReceipts(record *farmpb.FarmRecord, cycleID string) (*participantCropState, bool) {
	if record == nil {
		return nil, false
	}
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		receipt := record.GetRecentOperationReceipts()[index]
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			continue
		}
		proof, err := decodeParticipantProof(receipt.GetResultPayload())
		if err != nil || proof.outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED ||
			proof.result == nil || proof.result.GetCropCycleId() != cycleID || proof.rule == nil {
			continue
		}
		crop := proof.cropState()
		if crop == nil {
			continue
		}
		return crop, true
	}
	return nil, false
}

// mergeParticipantTheftCycle rebuilds the per-cycle attempt fence from the
// durable typed proofs plus the legacy TheftAttemptReceipt carrier.  The
// visitor/cycle pair, not operation_id, is the idempotency boundary.
func mergeParticipantTheftCycle(record *farmpb.FarmRecord, cycle *domain.CropCycle) {
	if record == nil || cycle == nil || cycle.CycleID == "" {
		return
	}
	seen := make(map[uint64]struct{})
	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			continue
		}
		proof, err := decodeTheftParticipantProof(receipt.GetResultPayload())
		if err != nil || proof.outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || proof.result == nil {
			continue
		}
		decision := proof.result.GetTheftTargetDecision()
		if decision == nil || decision.GetCropCycleId() != cycle.CycleID || decision.GetVisitorPlayerId() == 0 {
			continue
		}
		if _, exists := seen[decision.GetVisitorPlayerId()]; exists {
			continue
		}
		seen[decision.GetVisitorPlayerId()] = struct{}{}
		cycle.Attempts = append(cycle.Attempts, domain.TheftAttemptRecord{
			VisitorPlayerID: decision.GetVisitorPlayerId(), StealSlotIndex: decision.GetStealSlotIndex(),
			DecidedAtMs: receipt.GetCommittedAtMs(),
		})
		if cycle.NextStealSlotIndex <= decision.GetStealSlotIndex() {
			cycle.NextStealSlotIndex = decision.GetStealSlotIndex() + 1
		}
	}
	for _, receipt := range record.GetRecentTheftAttempts() {
		if receipt == nil || receipt.GetCropCycleId() != cycle.CycleID || receipt.GetVisitorPlayerId() == 0 {
			continue
		}
		if _, exists := seen[receipt.GetVisitorPlayerId()]; exists {
			continue
		}
		seen[receipt.GetVisitorPlayerId()] = struct{}{}
		cycle.Attempts = append(cycle.Attempts, domain.TheftAttemptRecord{
			VisitorPlayerID: receipt.GetVisitorPlayerId(), StealSlotIndex: uint32(len(cycle.Attempts)),
			DecidedAtMs: receipt.GetCommittedAtMs(),
		})
		cycle.NextStealSlotIndex++
	}
}

func participantLatestOpportunityHeader(record *farmpb.FarmRecord) (domain.OpportunityHeader, bool) {
	if record == nil {
		return domain.OpportunityHeader{}, false
	}
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		receipt := record.GetRecentOperationReceipts()[index]
		if receipt == nil {
			continue
		}
		if care, found, careErr := decodeCareProof(receipt.GetResultPayload()); found {
			if careErr != nil {
				return domain.OpportunityHeader{}, false
			}
			if careErr := validateCareReceiptBinding(receipt, care); careErr != nil {
				return domain.OpportunityHeader{}, false
			}
			return care.header, true
		}
		if strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			proof, err := decodeParticipantProof(receipt.GetResultPayload())
			if err == nil {
				return proof.header, true
			}
			theftProof, theftErr := decodeTheftParticipantProof(receipt.GetResultPayload())
			if theftErr == nil {
				return theftProof.header, true
			}
			harvestProof, harvestErr := decodeHarvestParticipantProof(receipt.GetResultPayload())
			if harvestErr == nil {
				return harvestProof.header, true
			}
			guardProof, guardErr := decodeGuardSyncParticipantProof(receipt.GetResultPayload())
			if guardErr == nil {
				return guardProof.header, true
			}
			unlockProof, unlockErr := decodeCommitPlotUnlockProof(receipt.GetResultPayload())
			if unlockErr == nil && unlockProof.result != nil {
				result := unlockProof.result
				header := domain.OpportunityHeader{
					SourceFarmVersion:   result.GetOpportunitySourceFarmVersion(),
					DirtyGeneration:     result.GetOpportunityDirtyGeneration(),
					NextOpportunityAtMs: result.GetOpportunityNextAtMs(),
					CandidateMask:       domain.OpportunityMask(result.GetOpportunityCandidateMask()),
				}
				if header.SourceFarmVersion != result.GetCommittedFarmVersion() {
					return domain.OpportunityHeader{}, false
				}
				return header, true
			}
			// A malformed newest Participant receipt cannot be replaced by an
			// older header. Doing so would make a stale opportunity look current.
			return domain.OpportunityHeader{}, false
		}
		_, header, found, err := decodeFarmMutationReceiptPayload(receipt.GetResultPayload())
		if found {
			if err != nil {
				return domain.OpportunityHeader{}, false
			}
			return header, true
		}
		// Every Farm mutation receipt written by this actor now carries the
		// authoritative header. An old/plain receipt is not evidence that the
		// latest header is still current, so fail closed instead of falling back
		// to an older Participant proof.
		return domain.OpportunityHeader{}, false
	}
	return domain.OpportunityHeader{}, false
}

func participantReceiptRetentionFull(record *farmpb.FarmRecord, nowMs int64) bool {
	if record == nil {
		return false
	}
	cutoff := nowMs - operationReceiptRetention.Milliseconds()
	retained := 0
	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt == nil {
			continue
		}
		if strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) || receipt.GetCommittedAtMs() >= cutoff {
			retained++
		}
	}
	return retained >= maxRecentReceipts
}

// encodeFarmMutationReceiptPayload keeps the existing FarmOperationReceipt
// carrier and adds the authoritative opportunity header without changing any
// generated contract. The embedded FarmCommandResult remains the same
// compact replay result used by legacy Farm commands.
func encodeFarmMutationReceiptPayload(result *farmpb.FarmCommandResult, header domain.OpportunityHeader) ([]byte, error) {
	if result == nil {
		result = &farmpb.FarmCommandResult{}
	}
	resultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(compactOperationResult(result))
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(farmMutationReceiptMagic)+len(resultBytes)+64)
	buf = append(buf, []byte(farmMutationReceiptMagic)...)
	putParticipantUint32(&buf, farmMutationReceiptVersion)
	writeParticipantLPBytes(&buf, resultBytes)
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	return buf, nil
}

func decodeFarmMutationReceiptPayload(data []byte) (*farmpb.FarmCommandResult, domain.OpportunityHeader, bool, error) {
	if !bytes.HasPrefix(data, []byte(farmMutationReceiptMagic)) {
		return nil, domain.OpportunityHeader{}, false, nil
	}
	reader := participantProofReader{data: data[len(farmMutationReceiptMagic):]}
	version, err := reader.u32()
	if err != nil || version != farmMutationReceiptVersion {
		return nil, domain.OpportunityHeader{}, true, errors.New("farm participant: Farm mutation receipt header version is invalid")
	}
	resultBytes, err := reader.lp()
	if err != nil {
		return nil, domain.OpportunityHeader{}, true, errors.New("farm participant: Farm mutation receipt result is invalid")
	}
	var result farmpb.FarmCommandResult
	// A protobuf zero-value message has a canonical empty encoding. Harvest
	// receipts compact away their Snapshot and therefore legitimately carry a
	// zero-byte FarmCommandResult while the following opportunity header remains
	// authoritative.
	if len(resultBytes) > 0 {
		if err := proto.Unmarshal(resultBytes, &result); err != nil {
			return nil, domain.OpportunityHeader{}, true, errors.New("farm participant: Farm mutation receipt result is malformed")
		}
	}
	sourceVersion, err := reader.u64()
	if err != nil {
		return &result, domain.OpportunityHeader{}, true, err
	}
	dirtyGeneration, err := reader.u64()
	if err != nil {
		return &result, domain.OpportunityHeader{}, true, err
	}
	nextOpportunityAtMs, err := reader.i64()
	if err != nil {
		return &result, domain.OpportunityHeader{}, true, err
	}
	candidateMask, err := reader.u32()
	if err != nil || reader.remaining() != 0 {
		return &result, domain.OpportunityHeader{}, true, errors.New("farm participant: Farm mutation receipt header is malformed")
	}
	if sourceVersion == 0 || dirtyGeneration == 0 {
		return &result, domain.OpportunityHeader{}, true, errors.New("farm participant: Farm mutation receipt header is incomplete")
	}
	return &result, domain.OpportunityHeader{
		SourceFarmVersion: sourceVersion, DirtyGeneration: dirtyGeneration,
		NextOpportunityAtMs: nextOpportunityAtMs, CandidateMask: domain.OpportunityMask(candidateMask),
	}, true, nil
}

func requireParticipantOpportunityContinuity(record *farmpb.FarmRecord, state *domain.FarmState) error {
	if record == nil || record.Core == nil {
		return status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity is unavailable")
	}
	header, found := participantLatestOpportunityHeader(record)
	if record.Core.FarmVersion == 0 {
		if found && header.SourceFarmVersion != 0 {
			return status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity is invalid")
		}
		if state != nil {
			state.Header = domain.OpportunityHeader{}
		}
		return nil
	}
	if !found || header.SourceFarmVersion != record.Core.FarmVersion || header.DirtyGeneration == 0 {
		return status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity cannot be proven")
	}
	if state != nil {
		state.Header = header
	}
	return nil
}

func (a *FarmActor) nextLegacyOpportunityHeader(now time.Time) (domain.OpportunityHeader, error) {
	if a == nil || a.record == nil || a.record.Core == nil || a.record.Core.FarmVersion == 0 {
		return domain.OpportunityHeader{}, status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity is unavailable")
	}
	previous, found := participantLatestOpportunityHeader(a.record)
	if !found {
		if a.record.Core.FarmVersion != 1 {
			return domain.OpportunityHeader{}, status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity cannot be proven")
		}
		previous = domain.OpportunityHeader{}
	} else if previous.SourceFarmVersion+1 != a.record.Core.FarmVersion {
		return domain.OpportunityHeader{}, status.Error(codes.FailedPrecondition, "farm participant Opportunity header continuity is invalid")
	}
	state := a.buildParticipantState()
	state.Header = previous
	header := domain.ComputeOpportunity(&state, now.UnixMilli(), a.record.Core.FarmVersion, previous.DirtyGeneration+1)
	return header, nil
}

func participantPlantCommittedPlotVersion(record *farmpb.FarmRecord, cycleID string) (uint64, bool) {
	if record == nil || cycleID == "" {
		return 0, false
	}
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		receipt := record.GetRecentOperationReceipts()[index]
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			continue
		}
		proof, err := decodeParticipantProof(receipt.GetResultPayload())
		if err == nil && proof.outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
			proof.result != nil && proof.result.GetCropCycleId() == cycleID {
			return proof.result.GetCommittedPlotVersion(), proof.result.GetCommittedPlotVersion() != 0
		}
	}
	return 0, false
}

func participantHarvestMayUseTheftAdvancedPlot(record *farmpb.FarmRecord, state *domain.FarmState, payload *contractspb.CommitHarvestPayload, currentPlotVersion uint64) bool {
	if record == nil || record.Core == nil || state == nil || payload == nil || currentPlotVersion <= payload.GetExpectedPlotVersion() {
		return false
	}
	plot := state.FindPlot(payload.GetPlotId())
	cycle := state.FindCycle(payload.GetCropCycleId())
	if plot == nil || cycle == nil || plot.ActiveCropCycleID != payload.GetCropCycleId() || cycle.Ended || cycle.StolenAmount == 0 {
		return false
	}
	originPlotVersion, ok := participantPlantCommittedPlotVersion(record, payload.GetCropCycleId())
	if !ok || payload.GetExpectedPlotVersion() < originPlotVersion || currentPlotVersion < originPlotVersion {
		return false
	}
	// Every legal Theft turn increments PlotVersion once, while gross quantity
	// may be greater than one. Count the committed attempt proofs rather than
	// equating a version delta with stolen quantity.
	return currentPlotVersion-originPlotVersion == uint64(participantTheftMutationCount(record, payload.GetCropCycleId()))
}

func participantTheftMutationCount(record *farmpb.FarmRecord, cycleID string) int {
	if record == nil || cycleID == "" {
		return 0
	}
	seen := make(map[uint64]struct{})
	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			continue
		}
		proof, err := decodeTheftParticipantProof(receipt.GetResultPayload())
		if err != nil || proof.outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || proof.result == nil {
			continue
		}
		decision := proof.result.GetTheftTargetDecision()
		if decision != nil && decision.GetCropCycleId() == cycleID && decision.GetVisitorPlayerId() != 0 {
			seen[decision.GetVisitorPlayerId()] = struct{}{}
		}
	}
	for _, receipt := range record.GetRecentTheftAttempts() {
		if receipt != nil && receipt.GetCropCycleId() == cycleID && receipt.GetVisitorPlayerId() != 0 {
			seen[receipt.GetVisitorPlayerId()] = struct{}{}
		}
	}
	return len(seen)
}

func participantPublicPlot(record *farmpb.FarmRecord, plotID uint32) *farmpb.Plot {
	if record == nil || record.Core == nil {
		return nil
	}
	publicIndex, ok := participantPublicPlotIndex(plotID)
	if !ok {
		return nil
	}
	for _, plot := range record.Core.Plots {
		if plot != nil && plot.GetIndex() == publicIndex {
			return plot
		}
	}
	return nil
}

func (a *FarmActor) ensureParticipantCycles() error {
	if a.participantCycles != nil {
		return nil
	}
	a.participantCycles = make(map[string]*participantCropState)
	if a.record == nil {
		return errors.New("farm actor: participant record is missing")
	}
	for _, receipt := range a.record.GetRecentOperationReceipts() {
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) {
			continue
		}
		proof, err := decodeParticipantProof(receipt.GetResultPayload())
		if err != nil {
			// Harvest receipts use the same durable namespace but carry a
			// different typed proof. They do not rebuild an active CropCycle;
			// the HarvestDecision itself is recovered by its own resolver.
			if _, harvestErr := decodeHarvestParticipantProof(receipt.GetResultPayload()); harvestErr == nil {
				continue
			}
			if _, theftErr := decodeTheftParticipantProof(receipt.GetResultPayload()); theftErr == nil {
				continue
			}
			if _, guardErr := decodeGuardSyncParticipantProof(receipt.GetResultPayload()); guardErr == nil {
				continue
			}
			if _, unlockErr := decodeCommitPlotUnlockProof(receipt.GetResultPayload()); unlockErr == nil {
				continue
			}
			return fmt.Errorf("farm actor: malformed participant receipt: %w", err)
		}
		if proof.outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED && proof.rule != nil && proof.result != nil {
			crop := proof.cropState()
			if crop == nil {
				return errors.New("farm actor: applied participant receipt has unusable frozen snapshots")
			}
			a.participantCycles[proof.result.GetCropCycleId()] = crop
		}
	}
	// CARE resolution is persisted in the FarmOperationReceipt carrier as a
	// namespaced business proof. Merge only the resolved fields into the frozen
	// Plant crop; due/type/incident identity still come exclusively from the
	// Plant proof and are never reconstructed from current configuration.
	for _, receipt := range a.record.GetRecentOperationReceipts() {
		if receipt == nil {
			continue
		}
		care, found, careErr := decodeCareProof(receipt.GetResultPayload())
		if !found {
			continue
		}
		if careErr != nil {
			return fmt.Errorf("farm actor: malformed CARE receipt: %w", careErr)
		}
		if careErr := validateCareReceiptBinding(receipt, care); careErr != nil {
			return careErr
		}
		if care.outcome != domain.CareOutcomeResolved {
			continue
		}
		if care.snapshot == nil || care.snapshot.GetSourceFarmId() != a.farmID {
			return errors.New("farm actor: CARE receipt source farm binding is invalid")
		}
		crop := a.participantCycles[care.cycleID]
		if crop == nil {
			return errors.New("farm actor: CARE receipt has no frozen crop proof")
		}
		incident := crop.cycle.FindIncident(care.incidentID)
		if incident == nil || incident.CareType != care.careType {
			return errors.New("farm actor: CARE receipt incident binding is invalid")
		}
		incident.ResolvedAtMs = care.resolvedAtMs
		incident.ResolvedByPlayerID = care.resolvedByPlayerID
	}
	return nil
}

// CommitPlant preserves the original direct FarmActor seam used by legacy
// in-process callers.  The typed Participant ingress uses
// CommitPlantWithAction below so it can carry the complete ActionResult
// across the Participant boundary.
func (a *FarmActor) CommitPlant(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*contracts.StepOutcome, error) {
	turn, err := a.commitPlantWithAction(ctx, call, guard, nowMs)
	if err != nil || turn == nil || turn.Outcome == nil {
		if turn == nil {
			return nil, err
		}
		return turn.Outcome, err
	}
	if turn.deliveryErr != nil {
		return nil, turn.deliveryErr
	}
	return turn.Outcome, nil
}

// CommitPlantWithAction is the typed Participant mailbox seam.  It proves the
// authority commit and returns the complete ActionResult produced by the
// existing local FarmSubscriptionHub handoff.  The ingress observer receives
// it only after SubmitParticipant has left the actor mailbox, so any
// Gate/downlink work remains outside the mailbox.
func (a *FarmActor) CommitPlantWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitPlantTurnResult, error) {
	return a.commitPlantWithAction(ctx, call, guard, nowMs)
}

// CommitHarvest preserves the direct FarmActor seam used by package tests and
// non-ingress callers. The typed Participant ingress uses
// CommitHarvestWithAction so it can hand the exact committed FarmChange to its
// observer after the mailbox turn returns.
func (a *FarmActor) CommitHarvest(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*contracts.StepOutcome, error) {
	turn, err := a.commitHarvestWithAction(ctx, call, guard, nowMs)
	if err != nil || turn == nil || turn.Outcome == nil {
		if turn == nil {
			return nil, err
		}
		return turn.Outcome, err
	}
	if turn.deliveryErr != nil {
		return nil, turn.deliveryErr
	}
	return turn.Outcome, nil
}

// CommitHarvestWithAction is the typed Participant mailbox seam. It commits
// the CropCycle end, empty Plot, HarvestDecision and receipt in one CAS and
// returns the candidate FarmChange generated from that same record.
func (a *FarmActor) CommitHarvestWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitHarvestTurnResult, error) {
	return a.commitHarvestWithAction(ctx, call, guard, nowMs)
}

// ApplyGuardSnapshot is the direct typed GuardSync seam. Production ingress
// uses ApplyGuardSnapshotWithAction so a real public appearance change can be
// handed to the existing delivery path after the mailbox turn.
func (a *FarmActor) ApplyGuardSnapshot(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*contracts.StepOutcome, error) {
	turn, err := a.applyGuardSnapshotWithAction(ctx, call, guard, nowMs)
	if err != nil || turn == nil || turn.Outcome == nil {
		if turn == nil {
			return nil, err
		}
		return turn.Outcome, err
	}
	if turn.deliveryErr != nil {
		return nil, turn.deliveryErr
	}
	return turn.Outcome, nil
}

// ApplyGuardSnapshotWithAction is one complete FarmActor mailbox turn: it
// compares the incoming immutable snapshot, commits the full effective
// snapshot plus Participant receipt to Farm authority, and only then updates
// the hot actor used by the next Theft turn. No Redis/network read occurs
// inside this method.
func (a *FarmActor) ApplyGuardSnapshotWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*ApplyGuardSnapshotTurnResult, error) {
	return a.applyGuardSnapshotWithAction(ctx, call, guard, nowMs)
}

func (a *FarmActor) applyGuardSnapshotWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*ApplyGuardSnapshotTurnResult, error) {
	if a == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant GuardSync actor call is incomplete")
	}
	if replay, found, err := ResolveApplyGuardSnapshotParticipantReceipt(a.record, a.recordVersion, call); err != nil {
		return nil, err
	} else if found {
		return &ApplyGuardSnapshotTurnResult{Outcome: replay}, nil
	}
	if err := ValidateApplyGuardSnapshotPayload(call); err != nil {
		return nil, err
	}
	trustedNowMs := time.Now().UnixMilli()
	if nowMs != nil {
		trustedNowMs = nowMs()
	}
	if trustedNowMs <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "farm participant trusted clock is unavailable")
	}
	if participantReceiptRetentionFull(a.record, trustedNowMs) {
		return nil, farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm participant receipt retention window is full")
	}
	state := a.buildParticipantState()
	if err := requireParticipantOpportunityContinuity(a.record, &state); err != nil {
		return nil, err
	}
	payload := call.Payload.(*contractspb.ApplyGuardSnapshotPayload)
	incoming, err := participantDomainGuardSnapshot(payload)
	if err != nil {
		return nil, err
	}
	candidate, decision, domainErr := domain.ApplyGuardSnapshot(state, payload.GetSourcePlayerId(), payload.GetTargetFarmId(), incoming)

	outcome := contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	errorCode := commonpb.ErrorCode_OK
	applyStatus := contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED
	effectiveState := state
	appearanceChanged := false
	if domainErr != nil || decision.Outcome == domain.GuardApplyInvariantViolation {
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
		errorCode = commonpb.ErrorCode_ERR_INTERNAL
		applyStatus = contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_UNSPECIFIED
	} else if decision.Outcome == domain.GuardApplySuperseded {
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
		applyStatus = contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_SUPERSEDED
	} else if decision.Outcome == domain.GuardApplyApplied {
		effectiveState = candidate.State
		appearanceChanged = decision.AppearanceChanged
		if appearanceChanged {
			if state.FarmVersion == math.MaxUint64 {
				return nil, status.Error(codes.Internal, "farm participant GuardSync FarmVersion overflows")
			}
			effectiveState.FarmVersion = state.FarmVersion + 1
			effectiveState.Header = domain.ComputeOpportunity(&effectiveState, trustedNowMs,
				effectiveState.FarmVersion, state.Header.DirtyGeneration+1)
		}
	}
	effectiveSnapshot := effectiveState.Guard
	result := &contractspb.ApplyGuardSnapshotResult{
		GuardVersion: effectiveSnapshot.GuardVersion, ApplyStatus: applyStatus,
		CommittedRecordVersion: a.recordVersion + 1, PublicAppearanceChanged: appearanceChanged,
	}
	if effectiveSnapshot.GuardVersion != 0 {
		hash, decodeErr := hex.DecodeString(effectiveSnapshot.SnapshotHash)
		if decodeErr != nil || len(hash) != sha256.Size {
			return nil, status.Error(codes.Internal, "farm participant effective GuardSnapshot hash is invalid")
		}
		result.SnapshotHash = hash
	}
	if appearanceChanged {
		committedFarmVersion := effectiveState.FarmVersion
		result.CommittedFarmVersion = &committedFarmVersion
	}
	resultHash, err := contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	proof, err := encodeGuardSyncParticipantProof(call, outcome, errorCode, result, resultHash[:],
		a.recordVersion+1, effectiveSnapshot, effectiveState.Header)
	if err != nil {
		return nil, err
	}
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	if err := appendParticipantReceipt(candidateRecord, call.InvocationID, call.CanonicalStepPayloadHash[:], proof, outcome, errorCode, trustedNowMs); err != nil {
		return nil, err
	}
	if candidateRecord.Core == nil {
		candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
	}
	var committedChange *farmpb.FarmChange
	if appearanceChanged {
		candidateRecord.Core.FarmVersion = effectiveState.FarmVersion
		committedChange = &farmpb.FarmChange{OwnerId: a.farmID, FarmVersion: effectiveState.FarmVersion}
	}
	a.record = candidateRecord
	if err := guard.CheckModelRecord(candidateRecord); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.saveWithCandidate(ctx, before, call.InvocationID, commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED, committedChange); err != nil {
		return nil, err
	}
	// The in-memory guard changes only after the fenced authority CAS is known
	// committed. The executor cannot start the next Theft turn before this
	// assignment and the optional local handoff complete.
	a.theftGuardSnapshot = effectiveSnapshot
	a.participantState = &participantFarmState{state: effectiveState}
	stepOutcome := &contracts.StepOutcome{
		Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result,
		CommittedRecordVersion: a.recordVersion, Failure: participantFailure(errorCode),
	}
	if !appearanceChanged {
		a.guardClear()
		return &ApplyGuardSnapshotTurnResult{Outcome: stepOutcome}, nil
	}
	change, recipients, resyncs, deliveryErr := a.publishCandidate(nil, time.UnixMilli(trustedNowMs))
	if change == nil {
		change = committedChange
	}
	return &ApplyGuardSnapshotTurnResult{
		Outcome: stepOutcome,
		Action: &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED,
			Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(trustedNowMs))},
			Change: change, Recipients: recipients, ActivationEpoch: a.activationEpoch, Resyncs: resyncs},
		deliveryErr: deliveryErr,
	}, nil
}

// DecideTheft preserves the direct FarmActor seam used by package tests.  The
// ingress path uses DecideTheftWithAction so the exact committed FarmChange
// remains attached to the same authority turn.
func (a *FarmActor) DecideTheft(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*contracts.StepOutcome, error) {
	turn, err := a.decideTheftWithAction(ctx, call, guard, nowMs)
	if err != nil || turn == nil || turn.Outcome == nil {
		if turn == nil {
			return nil, err
		}
		return turn.Outcome, err
	}
	if turn.deliveryErr != nil {
		return nil, turn.deliveryErr
	}
	return turn.Outcome, nil
}

// DecideTheftWithAction is the single FarmActor Theft mailbox turn.  It
// performs receipt-first replay, rechecks all target facts, derives the slot
// and guard result, then persists the typed proof and public projection in one
// Authority Commit.  It never waits for Workflow or Player participants.
func (a *FarmActor) DecideTheftWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*DecideTheftTurnResult, error) {
	return a.decideTheftWithAction(ctx, call, guard, nowMs)
}

func (a *FarmActor) decideTheftWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*DecideTheftTurnResult, error) {
	if a == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant DECIDE_THEFT actor call is incomplete")
	}
	if replay, found, err := ResolveDecideTheftParticipantReceipt(a.record, a.recordVersion, call); err != nil {
		return nil, err
	} else if found {
		return &DecideTheftTurnResult{Outcome: replay}, nil
	}
	if err := ValidateDecideTheftPayload(call); err != nil {
		return nil, err
	}
	if err := a.ensureParticipantCycles(); err != nil {
		return nil, err
	}
	trustedNowMs := time.Now().UnixMilli()
	if nowMs != nil {
		trustedNowMs = nowMs()
	}
	if trustedNowMs <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "farm participant trusted clock is unavailable")
	}
	if participantReceiptRetentionFull(a.record, trustedNowMs) {
		return nil, farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm participant receipt retention window is full")
	}
	state := a.buildParticipantState()
	if err := requireParticipantOpportunityContinuity(a.record, &state); err != nil {
		return nil, err
	}
	if state.Guard.GuardVersion != 0 {
		if err := domain.ValidateGuardRule(state.Guard.Rule); err != nil {
			return nil, status.Error(codes.FailedPrecondition, "farm participant effective GuardSnapshot is invalid")
		}
	}
	payload := call.Payload.(*contractspb.DecideTheftPayload)
	before := proto.Clone(a.record).(*farmpb.FarmRecord)

	// Probe only the deterministic target guards first.  Rejected and
	// NO_CHANGE attempts do not consume a PRF draw; the first legal attempt
	// receives exactly one server-side draw which is persisted below.
	_, _, domainErr := domain.DecideTheft(state, domain.TheftCommand{
		VisitorPlayerID: payload.GetVisitorPlayerId(), PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(),
	}, 9999, trustedNowMs)
	var candidate domain.CandidateState
	var decision domain.TheftDecision
	if domainErr == nil {
		prf := a.theftPRF
		if prf == nil {
			prf = defaultTheftPRF
		}
		draw, err := prf(payload.GetTheftAttemptId(), state.Guard)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "farm participant Theft PRF is unavailable")
		}
		if draw >= 10000 {
			return nil, status.Error(codes.Internal, "farm participant Theft PRF returned an invalid draw")
		}
		candidate, decision, domainErr = domain.DecideTheft(state, domain.TheftCommand{
			VisitorPlayerID: payload.GetVisitorPlayerId(), PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(),
		}, draw, trustedNowMs)
	}

	result := &contractspb.DecideTheftResult{
		CommittedPlotVersion: currentParticipantPlotVersion(&state, payload.GetPlotId()),
		CommittedFarmVersion: state.FarmVersion,
		FarmRecordVersion:    a.recordVersion + 1,
		TheftTargetDecision:  theftTargetDecisionBase(call, payload),
	}
	needsChange := domainErr == nil
	outcome := contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	errorCode := commonpb.ErrorCode_OK
	if domainErr != nil {
		if domainErr.Status == domain.StatusNoChange {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
		} else {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
		}
		errorCode = mapParticipantTheftDomainError(domainErr)
	} else {
		frozen := a.participantCycles[payload.GetCropCycleId()]
		cycle := state.FindCycle(payload.GetCropCycleId())
		if frozen == nil || frozen.rule == nil || cycle == nil {
			return nil, status.Error(codes.FailedPrecondition, "farm participant Theft dependency proof is missing frozen CropCycle facts")
		}
		if frozen.rule.GetHarvestItemId() == 0 || uint64(decision.GrossStolenQuantity) > math.MaxUint64 ||
			uint64(decision.Guard.GuardLostQuantity) > uint64(decision.GrossStolenQuantity) ||
			uint64(decision.NetReceivedQuantity)+uint64(decision.Guard.GuardLostQuantity) != uint64(decision.GrossStolenQuantity) {
			return nil, status.Error(codes.Internal, "farm participant Theft decision quantities are invalid")
		}
		result.TheftTargetDecision = buildTheftTargetDecision(call, payload, cycle, frozen, state.Guard, decision)
		result.CommittedPlotVersion = decision.CommittedPlotVersion
		result.CommittedFarmVersion = decision.CommittedFarmVersion
		decisionHash, err := theftDecisionHash(result.GetTheftTargetDecision())
		if err != nil {
			return nil, err
		}
		result.TheftTargetDecision.TheftDecisionHash = append([]byte(nil), decisionHash[:]...)
	}
	resultHash, err := contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	commitHeader := state.Header
	if needsChange {
		commitHeader = candidate.State.Header
	}
	proof, err := encodeTheftParticipantProof(call, outcome, errorCode, result, resultHash[:], a.recordVersion+1, commitHeader)
	if err != nil {
		return nil, err
	}
	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	if err := appendParticipantReceipt(candidateRecord, call.InvocationID, call.CanonicalStepPayloadHash[:], proof, outcome, errorCode, trustedNowMs); err != nil {
		return nil, err
	}
	if needsChange {
		if candidateRecord.Core == nil {
			candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
		}
		publicPlot := participantPublicPlot(candidateRecord, payload.GetPlotId())
		publicCycle := participantPublicCycle(candidateRecord, payload.GetCropCycleId())
		if publicPlot == nil || publicCycle == nil {
			return nil, status.Error(codes.Internal, "farm participant Theft public target projection is missing")
		}
		if decision.GrossStolenQuantity > math.MaxInt32-uint32(max(publicCycle.GetStolenTotal(), 0)) {
			return nil, status.Error(codes.Internal, "farm participant Theft public stolen_total overflows")
		}
		publicCycle.StolenTotal += int32(decision.GrossStolenQuantity)
		publicPlot.PlotVersion = decision.CommittedPlotVersion
		candidateRecord.Core.FarmVersion = decision.CommittedFarmVersion
		publicCycleID := publicCycle.GetCropCycleId()
		candidateRecord.RecentTheftAttempts = append(candidateRecord.RecentTheftAttempts, &farmpb.TheftAttemptReceipt{
			VisitorPlayerId: payload.GetVisitorPlayerId(), CropCycleId: publicCycleID,
			OperationId: payload.GetActionPermitSnapshot().GetOperationId(), CommittedAtMs: trustedNowMs,
		})
		committedChange := &farmpb.FarmChange{
			OwnerId: a.farmID, FarmVersion: candidateRecord.Core.FarmVersion,
			ChangedPlots: []*farmpb.PlotView{plotView(candidateRecord, publicPlot, time.UnixMilli(trustedNowMs))},
		}
		a.record = candidateRecord
		if err := guard.CheckModelRecord(candidateRecord); err != nil {
			a.record = before
			a.guardClear()
			return nil, err
		}
		var orders []authoritystore.AttachedOrder
		var visitorOrderID, guardOrderID string
		if call.Work != nil &&
			call.Work.GetPlacementKind() == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET &&
			payload.GetActionPermitSnapshot() != nil {
			var orderErr error
			orders, visitorOrderID, guardOrderID, orderErr = buildTheftSourceOrders(
				a.farmID, payload.GetActionPermitSnapshot().GetOperationId(),
				payload.GetVisitorPlayerId(), payload.GetOwnerPlayerId(), resultHash[:],
				result.GetTheftTargetDecision(), trustedNowMs)
			if orderErr != nil {
				a.record = before
				a.guardClear()
				return nil, orderErr
			}
		}
		if len(orders) > 0 {
			if err := a.saveWithCandidateAndOrders(ctx, before, call.InvocationID, commonpb.CommandType_THEFT, committedChange, orders); err != nil {
				return nil, err
			}
		} else if err := a.saveWithCandidate(ctx, before, call.InvocationID, commonpb.CommandType_THEFT, committedChange); err != nil {
			return nil, err
		}
		a.participantState = &participantFarmState{state: candidate.State}
		change, recipients, resyncs, deliveryErr := a.publishCandidate(publicPlot, time.UnixMilli(trustedNowMs))
		if change == nil {
			change = committedChange
		}
		committedTheftOrders := make([]authoritystore.AttachedOrder, len(orders))
		copy(committedTheftOrders, orders)
		for i := range committedTheftOrders {
			committedTheftOrders[i].Payload = append([]byte(nil), committedTheftOrders[i].Payload...)
		}
		return &DecideTheftTurnResult{
			Outcome:     &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result, CommittedRecordVersion: a.recordVersion, Failure: participantFailure(errorCode)},
			Action:      &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(trustedNowMs))}, Change: change, FriendOpportunityInvalidated: true, Recipients: recipients, ActivationEpoch: a.activationEpoch, Resyncs: resyncs, TheftVisitorOrderID: visitorOrderID, TheftGuardOrderID: guardOrderID, CommittedTheftSourceOrders: committedTheftOrders},
			deliveryErr: deliveryErr,
		}, nil
	}

	// Rejection and NO_CHANGE persist only the receipt proof.  They never
	// advance FarmVersion, PlotVersion, Attempt or any settlement obligation.
	a.record = candidateRecord
	if err := guard.CheckModelRecord(a.record); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, call.InvocationID, commonpb.CommandType_THEFT); err != nil {
		return nil, err
	}
	a.guardClear()
	a.participantState = &participantFarmState{state: state}
	return &DecideTheftTurnResult{Outcome: &contracts.StepOutcome{
		Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result,
		CommittedRecordVersion: a.recordVersion, Failure: participantFailure(errorCode),
	}}, nil
}

func participantPublicCycle(record *farmpb.FarmRecord, cycleID string) *farmpb.CropCycle {
	if record == nil || record.Core == nil || cycleID == "" {
		return nil
	}
	for _, cycle := range record.Core.CropCycles {
		if cycle != nil && cycle.GetCropCycleId() == cycleID {
			return cycle
		}
	}
	return nil
}

func theftTargetDecisionBase(call *contracts.ParticipantCall, payload *contractspb.DecideTheftPayload) *contractspb.TheftTargetDecision {
	return &contractspb.TheftTargetDecision{
		WorkflowId: call.Work.GetWorkId(), SettlementId: payload.GetSettlementId(), TheftAttemptId: payload.GetTheftAttemptId(),
		VisitorPlayerId: payload.GetVisitorPlayerId(), OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(),
		PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
	}
}

func buildTheftTargetDecision(call *contracts.ParticipantCall, payload *contractspb.DecideTheftPayload, cycle *domain.CropCycle, frozen *participantCropState, guardSnapshot domain.GuardSnapshot, decision domain.TheftDecision) *contractspb.TheftTargetDecision {
	guard := decision.Guard
	return &contractspb.TheftTargetDecision{
		WorkflowId: call.Work.GetWorkId(), SettlementId: payload.GetSettlementId(), TheftAttemptId: payload.GetTheftAttemptId(),
		VisitorPlayerId: payload.GetVisitorPlayerId(), OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(),
		PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(), RewardItemId: frozen.rule.GetHarvestItemId(),
		StealSlotIndex: decision.StealSlotIndex, AppliedSlotRatioBp: decision.AppliedSlotRatioBP,
		GrossStolenQuantity: uint64(decision.GrossStolenQuantity), GuardLostQuantity: uint64(guard.GuardLostQuantity),
		NetReceivedQuantity: uint64(decision.NetReceivedQuantity), CropUnitCoinValue: cycle.CropUnitCoinValue,
		GuardCoinValue: guard.GuardCoinValue, TheftOutcome: theftOutcomeProto(decision.Outcome), CropRuleVersion: cycle.RuleVersion,
		GuardDecisionSnapshot: &contractspb.GuardDecisionSnapshot{
			TheftAttemptId: payload.GetTheftAttemptId(), GuardVersion: guard.GuardVersion, GuardRuleVersion: guard.GuardRuleVersion,
			TriggerProbabilityBp: guardSnapshot.Rule.TriggerProbabilityBP, InterceptPolicy: theftGuardPolicyProto(guardSnapshot.Rule.InterceptPolicy),
			RandomAlgorithmVersion: guardSnapshot.Rule.RandomAlgorithmVersion, RandomKeyVersion: guardSnapshot.Rule.RandomKeyVersion,
			RandomDrawBp: guard.RandomDrawBP, GuardTriggered: guard.GuardTriggered,
			GrossStolenQuantity: uint64(decision.GrossStolenQuantity), GuardLostQuantity: uint64(guard.GuardLostQuantity),
			NetReceivedQuantity: uint64(decision.NetReceivedQuantity), CropUnitCoinValue: cycle.CropUnitCoinValue, GuardCoinValue: guard.GuardCoinValue,
		},
	}
}

func theftOutcomeProto(outcome domain.TheftOutcome) contractspb.TheftOutcome {
	if outcome == domain.TheftOutcomeBlockedByGuard {
		return contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD
	}
	return contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN
}

func theftGuardPolicyProto(policy domain.InterceptPolicy) contractspb.GuardInterceptPolicy {
	if policy == domain.InterceptPolicyFullGross {
		return contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS
	}
	return contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_UNSPECIFIED
}

func mapParticipantTheftDomainError(err *domain.Error) commonpb.ErrorCode {
	if err == nil {
		return commonpb.ErrorCode_OK
	}
	switch err.Code {
	case domain.CodeCropNotMature:
		return commonpb.ErrorCode_ERR_CROP_NOT_MATURE
	case domain.CodeVisitorIsOwner:
		return commonpb.ErrorCode_ERR_PERMISSION_DENIED
	case domain.CodeStealAlreadyAttempted:
		return commonpb.ErrorCode_ERR_STEAL_ALREADY_DONE
	case domain.CodeNoStealableQuota:
		return commonpb.ErrorCode_ERR_STEAL_QUOTA_EXHAUSTED
	case domain.CodePlotNotFound:
		return commonpb.ErrorCode_ERR_PLOT_NOT_FOUND
	case domain.CodeCycleMismatch, domain.CodeCycleEnded:
		return commonpb.ErrorCode_ERR_VERSION_CONFLICT
	case domain.CodeInvalidArgument:
		return commonpb.ErrorCode_ERR_INTERNAL
	default:
		return commonpb.ErrorCode_ERR_INTERNAL
	}
}

func (a *FarmActor) commitHarvestWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitHarvestTurnResult, error) {
	if a == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant actor call is incomplete")
	}
	if replay, found, err := ResolveCommitHarvestParticipantReceipt(a.record, a.recordVersion, call); err != nil {
		return nil, err
	} else if found {
		return &CommitHarvestTurnResult{Outcome: replay}, nil
	}
	if err := ValidateCommitHarvestPayload(call); err != nil {
		return nil, err
	}
	if err := a.ensureParticipantCycles(); err != nil {
		return nil, err
	}
	payload := call.Payload.(*contractspb.CommitHarvestPayload)
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	trustedNowMs := time.Now().UnixMilli()
	if nowMs != nil {
		trustedNowMs = nowMs()
	}
	if trustedNowMs <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "farm participant trusted clock is unavailable")
	}
	if participantReceiptRetentionFull(a.record, trustedNowMs) {
		return nil, farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm participant receipt retention window is full")
	}
	state := a.buildParticipantState()
	if err := requireParticipantOpportunityContinuity(a.record, &state); err != nil {
		return nil, err
	}

	plotVersion := currentParticipantPlotVersion(&state, payload.GetPlotId())
	decision := domain.HarvestDecision{
		PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(),
		CommittedPlotVersion: plotVersion, CommittedFarmVersion: state.FarmVersion,
	}
	candidate := state
	var domainCandidate domain.CandidateState
	var domainErr *domain.Error
	if plot := state.FindPlot(payload.GetPlotId()); plot != nil && payload.GetExpectedPlotVersion() != plot.PlotVersion &&
		!participantHarvestMayUseTheftAdvancedPlot(a.record, &state, payload, plot.PlotVersion) {
		domainErr = &domain.Error{Status: domain.StatusRejected, Code: domain.CodeStalePlotVersion,
			Message: fmt.Sprintf("plot %d version %d, expected %d", payload.GetPlotId(), plot.PlotVersion, payload.GetExpectedPlotVersion())}
	} else {
		domainCandidate, decision, domainErr = domain.DecideHarvest(state, domain.HarvestCommand{
			PlayerID: payload.GetOwnerPlayerId(), PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(),
		}, trustedNowMs)
	}
	if domainErr == nil {
		candidate = domainCandidate.State
	}

	committedRecordVersion := a.recordVersion + 1
	result := &contractspb.CommitHarvestResult{
		CommittedPlotVersion: plotVersion,
		CommittedFarmVersion: state.FarmVersion,
		FarmRecordVersion:    committedRecordVersion,
	}
	// A rejected result still carries the immutable target and current
	// versions, matching COMMIT_PLANT's stable typed-result replay shape. It is
	// not a HarvestDecision until the domain transition succeeds.
	result.HarvestDecision = &contractspb.HarvestDecision{
		WorkflowId: call.Work.GetWorkId(), OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(),
		PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
	}

	var frozen *participantCropState
	if a.participantCycles != nil {
		frozen = a.participantCycles[payload.GetCropCycleId()]
	}
	if domainErr == nil {
		if frozen == nil || frozen.rule == nil || frozen.compendium == nil {
			return nil, status.Error(codes.FailedPrecondition, "farm participant Harvest dependency proof is missing frozen CropCycle facts")
		}
		cycle := state.FindCycle(payload.GetCropCycleId())
		if cycle == nil || cycle.StolenAmount > cycle.StealableLimit || cycle.StealableLimit > cycle.BaseYield {
			return nil, status.Error(codes.Internal, "farm participant CropCycle settlement facts are invalid")
		}
		decisionResult := &contractspb.HarvestDecision{
			WorkflowId: call.Work.GetWorkId(), OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(),
			PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
			CropDefinitionId: cycle.CropDefinitionID, RewardItemId: frozen.rule.GetHarvestItemId(),
			RewardQuantity: uint64(decision.RewardQuantity), RewardExp: decision.FrozenHarvestExp,
			CropRuleVersion: cycle.RuleVersion, CompendiumUnlockSnapshot: proto.Clone(frozen.compendium).(*contractspb.CompendiumUnlockSnapshot),
		}
		if decisionResult.GetRewardItemId() == 0 || decisionResult.GetCompendiumUnlockSnapshot().GetCropDefinitionId() != decisionResult.GetCropDefinitionId() ||
			decisionResult.GetCompendiumUnlockSnapshot().GetPlantedByPlayerId() != payload.GetOwnerPlayerId() {
			return nil, status.Error(codes.FailedPrecondition, "farm participant frozen Harvest facts are inconsistent")
		}
		decisionHash, err := harvestDecisionHash(decisionResult)
		if err != nil {
			return nil, err
		}
		decisionResult.HarvestDecisionHash = append([]byte(nil), decisionHash[:]...)
		result.HarvestDecision = decisionResult
		result.CommittedPlotVersion = decision.CommittedPlotVersion
		result.CommittedFarmVersion = decision.CommittedFarmVersion
	}

	outcome := contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	errorCode := commonpb.ErrorCode_OK
	needsChange := domainErr == nil
	if domainErr != nil {
		if domainErr.Status == domain.StatusNoChange {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
		} else {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
		}
		errorCode = mapParticipantHarvestDomainError(domainErr)
	}
	resultHash, err := contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	commitHeader := state.Header
	if needsChange {
		commitHeader = candidate.Header
	}
	proof, err := encodeHarvestParticipantProof(call, outcome, errorCode, result, resultHash[:], committedRecordVersion, commitHeader)
	if err != nil {
		return nil, err
	}
	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	if err := appendParticipantReceipt(candidateRecord, call.InvocationID, call.CanonicalStepPayloadHash[:], proof, outcome, errorCode, trustedNowMs); err != nil {
		return nil, err
	}
	if needsChange {
		if candidateRecord.Core == nil {
			candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
		}
		publicPlot := participantPublicPlot(candidateRecord, payload.GetPlotId())
		if publicPlot == nil {
			publicIndex, ok := participantPublicPlotIndex(payload.GetPlotId())
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "farm participant PlotID is out of range")
			}
			publicPlot = &farmpb.Plot{Index: publicIndex, PlotVersion: plotVersion}
			candidateRecord.Core.Plots = append(candidateRecord.Core.Plots, publicPlot)
		}
		publicPlot.ActiveCropCycleId = ""
		publicPlot.PlotVersion = decision.CommittedPlotVersion
		removeParticipantPublicCycle(candidateRecord, payload.GetCropCycleId())
		candidateRecord.Core.FarmVersion = decision.CommittedFarmVersion
		committedChange := &farmpb.FarmChange{
			OwnerId: a.farmID, FarmVersion: candidateRecord.Core.FarmVersion,
			ChangedPlots: []*farmpb.PlotView{plotView(candidateRecord, publicPlot, time.UnixMilli(trustedNowMs))},
		}
		a.record = candidateRecord
		if err := guard.CheckModelRecord(candidateRecord); err != nil {
			a.record = before
			a.guardClear()
			return nil, err
		}
		if err := a.saveWithCandidate(ctx, before, call.InvocationID, commonpb.CommandType_HARVEST, committedChange); err != nil {
			return nil, err
		}
		a.participantState = &participantFarmState{state: candidate}
		delete(a.participantCycles, payload.GetCropCycleId())
		change, recipients, resyncs, deliveryErr := a.publishCandidate(publicPlot, time.UnixMilli(trustedNowMs))
		if change == nil {
			change = committedChange
		}
		return &CommitHarvestTurnResult{
			Outcome:     &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result, CommittedRecordVersion: committedRecordVersion, Failure: participantFailure(errorCode)},
			Action:      &ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(trustedNowMs))}, Change: change, FriendOpportunityInvalidated: true, Recipients: recipients, ActivationEpoch: a.activationEpoch, Resyncs: resyncs},
			deliveryErr: deliveryErr,
		}, nil
	}

	// A rejected/no-change Harvest still commits its receipt, but never mutates
	// the active crop, FarmVersion, opportunity header or realtime stream.
	a.record = candidateRecord
	if err := guard.CheckModelRecord(a.record); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, call.InvocationID, commonpb.CommandType_HARVEST); err != nil {
		return nil, err
	}
	a.guardClear()
	a.participantState = &participantFarmState{state: state}
	return &CommitHarvestTurnResult{Outcome: &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result, CommittedRecordVersion: committedRecordVersion, Failure: participantFailure(errorCode)}}, nil
}

func (a *FarmActor) commitPlantWithAction(ctx context.Context, call *contracts.ParticipantCall, guard ParticipantCapacityGuard, nowMs func() int64) (*CommitPlantTurnResult, error) {
	if a == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant actor call is incomplete")
	}
	if replay, found, err := ResolveCommitPlantParticipantReceipt(a.record, a.recordVersion, call); err != nil {
		return nil, err
	} else if found {
		return &CommitPlantTurnResult{Outcome: replay}, nil
	}
	if err := ValidateCommitPlantPayload(call); err != nil {
		return nil, err
	}
	if err := a.ensureParticipantCycles(); err != nil {
		return nil, err
	}
	payload := call.Payload.(*contractspb.CommitPlantPayload)
	before := proto.Clone(a.record).(*farmpb.FarmRecord)
	candidateRecord := proto.Clone(before).(*farmpb.FarmRecord)
	state := a.buildParticipantState()
	if err := requireParticipantOpportunityContinuity(a.record, &state); err != nil {
		return nil, err
	}
	trustedNowMs := time.Now().UnixMilli()
	if nowMs != nil {
		trustedNowMs = nowMs()
	}
	if trustedNowMs <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "farm participant trusted clock is unavailable")
	}
	rule, compendium, err := participantDomainRule(payload)
	if err != nil {
		return nil, err
	}
	decision := domain.PlantDecision{
		PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(), CropDefinitionID: payload.GetCropRuleSnapshot().GetCropDefinitionId(),
		AppliedRuleVersion:   payload.GetCropRuleSnapshot().GetCropRuleVersion(),
		CommittedPlotVersion: currentParticipantPlotVersion(&state, payload.GetPlotId()), CommittedFarmVersion: state.FarmVersion,
	}
	candidate := state
	var candidateCrop *participantCropState
	var domainErr *domain.Error
	var domainCandidate domain.CandidateState
	var domainDecision domain.PlantDecision
	var decideErr *domain.Error
	if plot := state.FindPlot(payload.GetPlotId()); plot != nil && plot.ActiveCropCycleID != "" {
		if plot.ActiveCropCycleID == payload.GetCropCycleId() {
			decideErr = &domain.Error{Status: domain.StatusNoChange, Code: domain.CodeInvalidArgument, Message: "plot already holds this crop cycle"}
		} else {
			sourceordermetrics.ObservePlantInvariantReject()
			decideErr = &domain.Error{Status: domain.StatusRejected, Code: domain.CodePlotNotEmpty, Message: "plot has an active crop cycle"}
		}
	} else {
		domainCandidate, domainDecision, decideErr = domain.DecidePlant(state, domain.PlantCommand{
			PlotID: payload.GetPlotId(), CycleID: payload.GetCropCycleId(),
			ExpectedCropRuleVersion: payload.GetCropRuleSnapshot().GetCropRuleVersion(), Compendium: compendium,
		}, rule, trustedNowMs)
	}
	if decideErr != nil {
		domainErr = decideErr
		decision.CommittedPlotVersion = currentParticipantPlotVersion(&state, payload.GetPlotId())
		decision.CommittedFarmVersion = state.FarmVersion
	} else {
		candidate = domainCandidate.State
		decision = domainDecision
		// The DF0 domain helper deliberately has a small P0 CarePlan shape.
		// Replace its incident IDs/times with the full SCB-1 frozen snapshot
		// after the decision, then recompute the same opportunity header.
		candidateCycle := candidate.FindCycle(payload.GetCropCycleId())
		careSnapshot, careIncidents, careErr := buildCareSnapshots(payload.GetCropRuleSnapshot().GetCarePlanTemplate(), payload.GetCropCycleId(), trustedNowMs, payload.GetCropRuleSnapshot().GetGrowDurationMs())
		if careErr != nil {
			return nil, careErr
		}
		candidateCycle.Incidents = careIncidents
		candidate.Header = domain.ComputeOpportunity(&candidate, trustedNowMs, candidate.FarmVersion, candidate.Header.DirtyGeneration)
		stealSnapshot := contractStealSnapshot(candidateCycle)
		candidateCrop = &participantCropState{
			cycle:         cloneDomainCycle(*candidateCycle),
			rule:          proto.Clone(payload.GetCropRuleSnapshot()).(*contractspb.CropRuleSnapshot),
			ruleHash:      append([]byte(nil), payload.GetCropRuleSnapshotHash()...),
			stealSnapshot: stealSnapshot, careSnapshot: careSnapshot,
			compendium: compendiumSnapshot(compendium),
		}
	}

	result := plantResult(payload, decision, domainErr)
	resultHash, err := contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	outcome := contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	errorCode := commonpb.ErrorCode_OK
	needsChange := domainErr == nil
	if domainErr != nil {
		if domainErr.Status == domain.StatusNoChange {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
		} else {
			outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
		}
		errorCode = mapParticipantDomainError(domainErr)
	}
	committedRecordVersion := a.recordVersion + 1
	decisionHash := plantDecisionHash(payload, result, payload.GetCropRuleSnapshotHash())
	result.PlantDecisionHash = decisionHash[:]
	resultHash, err = contracts.CanonicalParticipantHash(call.Spec, result)
	if err != nil {
		return nil, err
	}
	commitHeader := state.Header
	if needsChange {
		commitHeader = candidate.Header
	}
	proof, err := encodeParticipantProof(call, outcome, errorCode, result, resultHash[:], committedRecordVersion, candidateCrop, commitHeader)
	if err != nil {
		return nil, err
	}
	if err := appendParticipantReceipt(candidateRecord, call.InvocationID, call.CanonicalStepPayloadHash[:], proof, outcome, errorCode, trustedNowMs); err != nil {
		return nil, err
	}
	if needsChange {
		if candidateRecord.Core == nil {
			candidateRecord.Core = &farmpb.FarmCore{FarmId: a.farmID}
		}
		publicPlot := participantPublicPlot(candidateRecord, payload.GetPlotId())
		if publicPlot == nil {
			publicIndex, ok := participantPublicPlotIndex(payload.GetPlotId())
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "farm participant PlotID is out of range")
			}
			publicPlot = &farmpb.Plot{Index: publicIndex, PlotVersion: 1}
			candidateRecord.Core.Plots = append(candidateRecord.Core.Plots, publicPlot)
		}
		publicPlot.ActiveCropCycleId = payload.GetCropCycleId()
		publicPlot.PlotVersion = decision.CommittedPlotVersion
		candidateRecord.Core.FarmVersion = decision.CommittedFarmVersion
		candidateRecord.Core.CropCycles = append(candidateRecord.Core.CropCycles, publicCropCycle(candidateCrop, payload.GetPlotId()))
		committedChange := &farmpb.FarmChange{
			OwnerId:      a.farmID,
			FarmVersion:  candidateRecord.Core.FarmVersion,
			ChangedPlots: []*farmpb.PlotView{plotView(candidateRecord, publicPlot, time.UnixMilli(trustedNowMs))},
		}
		a.record = candidateRecord
		if err := guard.CheckModelRecord(candidateRecord); err != nil {
			a.record = before
			a.guardClear()
			return nil, err
		}
		if err := a.saveWithCandidate(ctx, before, call.InvocationID, commonpb.CommandType_PLANT, committedChange); err != nil {
			return nil, err
		}
		a.participantState = &participantFarmState{state: candidate}
		a.participantCycles[payload.GetCropCycleId()] = candidateCrop
		change, recipients, resyncs, deliveryErr := a.publishCandidate(publicPlot, time.UnixMilli(trustedNowMs))
		if change == nil {
			// The candidate was built from the same committed state. Preserve it
			// for the observer even if an unexpected Hub-lane error prevented the
			// normal handoff/fence completion.
			change = committedChange
		}
		return &CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{
				Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
				DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result,
				CommittedRecordVersion: committedRecordVersion,
				Failure:                participantFailure(errorCode),
			},
			Action: &ActionResult{
				Status:          commonpb.CommandStatus_SUCCEEDED,
				Result:          &farmpb.FarmCommandResult{Snapshot: a.Snapshot(time.UnixMilli(trustedNowMs))},
				Change:          change,
				Recipients:      recipients,
				ActivationEpoch: a.activationEpoch,
				Resyncs:         resyncs,
			},
			deliveryErr: deliveryErr,
		}, nil
	}

	// REJECTED/NO_CHANGE is still a durable Authority mutation of the receipt
	// carrier.  It must never emit a FarmChange or advance FarmVersion.
	a.record = candidateRecord
	if err := guard.CheckModelRecord(a.record); err != nil {
		a.record = before
		return nil, err
	}
	if err := a.save(ctx, before, call.InvocationID, commonpb.CommandType_PLANT); err != nil {
		return nil, err
	}
	a.guardClear()
	a.participantState = &participantFarmState{state: state}
	return &CommitPlantTurnResult{Outcome: &contracts.StepOutcome{
		Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash[:], Result: result,
		CommittedRecordVersion: committedRecordVersion, Failure: participantFailure(errorCode),
	}}, nil
}

func currentParticipantPlotVersion(state *domain.FarmState, plotID uint32) uint64 {
	if state == nil {
		return 0
	}
	plot := state.FindPlot(plotID)
	if plot == nil {
		return 0
	}
	return plot.PlotVersion
}

func participantDomainRule(payload *contractspb.CommitPlantPayload) (domain.CropRule, domain.CompendiumUnlockSnapshot, error) {
	snapshot := payload.GetCropRuleSnapshot()
	care := snapshot.GetCarePlanTemplate()
	plan := domain.CarePlanConfig{CareRuleVersion: care.GetCareRuleVersion()}
	// CarePlanTemplate has no top-level schedule mode; the per-incident mode
	// is the frozen authority.  P0 domain rules use FIXED_OFFSET when at least
	// one fixed incident is present, and random incidents are materialized by
	// buildCareSnapshots below.
	plan.ScheduleMode = domain.ScheduleModeFixedOffset
	for _, incident := range care.GetIncidents() {
		switch incident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER:
			if incident.GetScheduleMode() == contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET {
				plan.WaterEnabled, plan.WaterOffsetMs = true, incident.GetFixedOffsetMs()
			}
		case contractspb.CareType_CARE_TYPE_REMOVE_PEST:
			if incident.GetScheduleMode() == contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET {
				plan.PestEnabled, plan.PestOffsetMs = true, incident.GetFixedOffsetMs()
			}
		}
	}
	compendiumRule := snapshot.GetCompendiumRule()
	compendium := domain.CompendiumUnlockSnapshot{
		CropDefinitionID: compendiumRule.GetCropDefinitionId(), UnlockEligible: compendiumRule.GetUnlockEligible(),
		PlantedByPlayerID: payload.GetOwnerPlayerId(), ActivityID: compendiumRule.GetActivityId(),
		CatalogVersionAtPlant: compendiumRule.GetCatalogVersionAtPlant(), DefinitionVersionAtPlant: compendiumRule.GetDefinitionVersionAtPlant(),
	}
	rule := domain.CropRule{
		CropDefinitionID: snapshot.GetCropDefinitionId(), RuleVersion: snapshot.GetCropRuleVersion(), GrowDurationMs: snapshot.GetGrowDurationMs(),
		BaseYield: uint32(snapshot.GetBaseYield()), HarvestExp: snapshot.GetHarvestExp(), CropUnitCoinValue: snapshot.GetCropUnitCoinValue(),
		StealPlan: domain.StealPlanConfig{PolicyVersion: snapshot.GetStealPlanTemplate().GetPolicyVersion(), TotalCapRatioBP: snapshot.GetStealPlanTemplate().GetTotalCapRatioBp(), SlotRatioBP: append([]uint32(nil), snapshot.GetStealPlanTemplate().GetSlotRatioBp()...)},
		CarePlan:  plan,
	}
	return rule, compendium, nil
}

func buildCareSnapshots(template *contractspb.CarePlanTemplate, cycleID string, plantAtMs, growDurationMs int64) (*contractspb.CarePlanSnapshot, []domain.CareIncident, error) {
	if template == nil {
		return nil, nil, errors.New("farm participant: care plan template is missing")
	}
	snapshot := &contractspb.CarePlanSnapshot{CareRuleVersion: template.GetCareRuleVersion()}
	incidents := make([]domain.CareIncident, 0, len(template.GetIncidents()))
	for _, templateIncident := range template.GetIncidents() {
		careName := ""
		careType := domain.CareTypeUnspecified
		switch templateIncident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER:
			careName, careType = "WATER", domain.CareTypeWater
		case contractspb.CareType_CARE_TYPE_REMOVE_PEST:
			careName, careType = "REMOVE_PEST", domain.CareTypeRemovePest
		}
		incidentID, err := identity.CareIncidentID(cycleID, careName, uint64(templateIncident.GetOrdinal()))
		if err != nil {
			return nil, nil, err
		}
		var offset int64
		switch templateIncident.GetScheduleMode() {
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET:
			offset = templateIncident.GetFixedOffsetMs()
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_DETERMINISTIC_RANDOM_WINDOW:
			offset, err = deterministicCareOffset(cycleID, templateIncident, growDurationMs)
			if err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, errors.New("farm participant: unknown care schedule")
		}
		due, err := checkedParticipantAdd(plantAtMs, offset)
		if err != nil {
			return nil, nil, err
		}
		snapshot.Incidents = append(snapshot.Incidents, &contractspb.CareIncidentSnapshot{IncidentId: incidentID, CareType: templateIncident.GetCareType(), Ordinal: templateIncident.GetOrdinal(), DueAtMs: due})
		incidents = append(incidents, domain.CareIncident{IncidentID: incidentID, CareType: careType, DueAtMs: due})
	}
	return snapshot, incidents, nil
}

func deterministicCareOffset(cycleID string, incident *contractspb.CarePlanIncidentTemplate, growDurationMs int64) (int64, error) {
	start, end := incident.GetRandomWindowStartMs(), incident.GetRandomWindowEndMs()
	if start <= 0 || end < start || end >= growDurationMs {
		return 0, errors.New("farm participant: random care window is invalid")
	}
	span := uint64(end-start) + 1
	h := sha256.New()
	writeParticipantLP(h, []byte(cycleID))
	writeParticipantLP(h, []byte(fmt.Sprintf("%d", incident.GetCareType())))
	writeParticipantLP(h, []byte(fmt.Sprintf("%d", incident.GetOrdinal())))
	writeParticipantLP(h, []byte(fmt.Sprintf("%d", incident.GetRandomAlgorithmVersion())))
	digest := h.Sum(nil)
	return start + int64(binary.BigEndian.Uint64(digest[:8])%span), nil
}

func checkedParticipantAdd(base, offset int64) (int64, error) {
	if offset <= 0 || base > int64(1<<63-1)-offset {
		return 0, errors.New("farm participant: trusted time arithmetic overflow")
	}
	return base + offset, nil
}

func contractStealSnapshot(cycle *domain.CropCycle) *contractspb.StealPlanSnapshot {
	if cycle == nil {
		return nil
	}
	quantities := make([]uint64, len(cycle.StealPlan.SlotQuantity))
	for index, quantity := range cycle.StealPlan.SlotQuantity {
		quantities[index] = uint64(quantity)
	}
	ratios := append([]uint32(nil), cycle.StealPlan.SlotRatioBP...)
	return &contractspb.StealPlanSnapshot{PolicyVersion: cycle.StealPlan.PolicyVersion, TotalCapRatioBp: cycle.StealPlan.TotalCapRatioBP, TotalCapQuantity: uint64(cycle.StealableLimit), SlotRatioBp: ratios, SlotQuantity: quantities}
}

func compendiumSnapshot(value domain.CompendiumUnlockSnapshot) *contractspb.CompendiumUnlockSnapshot {
	var activityID *string
	if value.ActivityID != "" {
		activityID = &value.ActivityID
	}
	return &contractspb.CompendiumUnlockSnapshot{CropDefinitionId: value.CropDefinitionID, UnlockEligible: value.UnlockEligible, PlantedByPlayerId: value.PlantedByPlayerID, ActivityId: activityID, CatalogVersionAtPlant: value.CatalogVersionAtPlant, DefinitionVersionAtPlant: value.DefinitionVersionAtPlant}
}

func publicCropCycle(cycle *participantCropState, plotID uint32) *farmpb.CropCycle {
	publicIndex, ok := participantPublicPlotIndex(plotID)
	if !ok {
		publicIndex = 0
	}
	if cycle == nil {
		return &farmpb.CropCycle{PlotIndex: publicIndex}
	}
	return &farmpb.CropCycle{
		CropCycleId: cycle.cycle.CycleID, PlotIndex: publicIndex, CropDefinitionId: int32(cycle.cycle.CropDefinitionID), RuleVersion: int32(cycle.cycle.RuleVersion),
		PlantAt: cycle.cycle.PlantAtMs, MatureAt: cycle.cycle.MatureAtMs, BaseYield: int32(cycle.cycle.BaseYield), StealableLimit: int32(cycle.cycle.StealableLimit),
	}
}

func removeParticipantPublicCycle(record *farmpb.FarmRecord, cycleID string) {
	if record == nil || record.Core == nil || cycleID == "" {
		return
	}
	cycles := record.Core.CropCycles[:0]
	for _, cycle := range record.Core.CropCycles {
		if cycle != nil && cycle.GetCropCycleId() == cycleID {
			continue
		}
		cycles = append(cycles, cycle)
	}
	record.Core.CropCycles = cycles
}

func plantResult(payload *contractspb.CommitPlantPayload, decision domain.PlantDecision, domainErr *domain.Error) *contractspb.CommitPlantResult {
	result := &contractspb.CommitPlantResult{
		FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
		CropDefinitionId: payload.GetCropRuleSnapshot().GetCropDefinitionId(), AppliedCropRuleVersion: payload.GetCropRuleSnapshot().GetCropRuleVersion(),
		CommittedPlotVersion: decision.CommittedPlotVersion, CommittedFarmVersion: decision.CommittedFarmVersion,
	}
	if domainErr == nil {
		result.PlantAtMs, result.MatureAtMs = decision.PlantAtMs, decision.MatureAtMs
	}
	return result
}

func plantDecisionHash(payload *contractspb.CommitPlantPayload, result *contractspb.CommitPlantResult, ruleHash []byte) [32]byte {
	h := sha256.New()
	writeParticipantLP(h, []byte("farm-commit-plant-decision-v1"))
	writeParticipantUint64(h, payload.GetOwnerPlayerId())
	writeParticipantUint64(h, payload.GetFarmId())
	writeParticipantUint32(h, payload.GetPlotId())
	writeParticipantLP(h, []byte(payload.GetCropCycleId()))
	writeParticipantUint64(h, result.GetAppliedCropRuleVersion())
	writeParticipantInt64(h, result.GetPlantAtMs())
	writeParticipantInt64(h, result.GetMatureAtMs())
	writeParticipantUint64(h, result.GetCommittedPlotVersion())
	writeParticipantUint64(h, result.GetCommittedFarmVersion())
	writeParticipantLP(h, ruleHash)
	writeParticipantLP(h, payload.GetReserveSeedResultHash())
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func mapParticipantDomainError(err *domain.Error) commonpb.ErrorCode {
	if err == nil {
		return commonpb.ErrorCode_OK
	}
	switch err.Code {
	case domain.CodePlotNotFound:
		return commonpb.ErrorCode_ERR_PLOT_NOT_FOUND
	case domain.CodePlotLocked:
		return commonpb.ErrorCode_ERR_PLOT_LOCKED
	case domain.CodePlotNotEmpty:
		return commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY
	case domain.CodeStalePlotVersion:
		return commonpb.ErrorCode_ERR_VERSION_CONFLICT
	case domain.CodeRuleVersionChanged, domain.CodeCropRuleUnknown:
		return commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH
	default:
		return commonpb.ErrorCode_ERR_INTERNAL
	}
}

func mapParticipantHarvestDomainError(err *domain.Error) commonpb.ErrorCode {
	if err == nil {
		return commonpb.ErrorCode_OK
	}
	switch err.Code {
	case domain.CodePlotNotFound:
		return commonpb.ErrorCode_ERR_PLOT_NOT_FOUND
	case domain.CodePlotEmpty:
		return commonpb.ErrorCode_ERR_PLOT_EMPTY
	case domain.CodeCropNotMature:
		return commonpb.ErrorCode_ERR_CROP_NOT_MATURE
	case domain.CodeCycleMismatch, domain.CodeCycleEnded, domain.CodeStalePlotVersion:
		return commonpb.ErrorCode_ERR_VERSION_CONFLICT
	case domain.CodeInvalidArgument:
		return commonpb.ErrorCode_ERR_PERMISSION_DENIED
	default:
		return mapParticipantDomainError(err)
	}
}

// harvestDecisionHash is the shared canonical hash of every semantic
// HarvestDecision field except harvest_decision_hash itself. The same helper
// is consumed by PlayerActor and the generic Harvest workflow; Harvest never
// consults a live catalog.
func harvestDecisionHash(decision *contractspb.HarvestDecision) ([32]byte, error) {
	if decision == nil {
		return [32]byte{}, errors.New("farm participant: HarvestDecision is missing")
	}
	withoutHash := proto.Clone(decision).(*contractspb.HarvestDecision)
	withoutHash.HarvestDecisionHash = nil
	return contracts.CanonicalHash(withoutHash)
}

func validateHarvestDecisionHash(decision *contractspb.HarvestDecision) error {
	if decision == nil || len(decision.GetHarvestDecisionHash()) != sha256.Size {
		return errors.New("farm participant: HarvestDecision hash is missing")
	}
	copyOfDecision := proto.Clone(decision).(*contractspb.HarvestDecision)
	copyOfDecision.HarvestDecisionHash = nil
	want, err := harvestDecisionHash(copyOfDecision)
	if err != nil || !bytes.Equal(want[:], decision.GetHarvestDecisionHash()) {
		return errors.New("farm participant: HarvestDecision hash is invalid")
	}
	return nil
}

func participantFailure(code commonpb.ErrorCode) *contractspb.ParticipantFailure {
	if code == commonpb.ErrorCode_OK || code == commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return nil
	}
	return &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: code}}
}

func appendParticipantReceipt(record *farmpb.FarmRecord, invocationID string, stepHash, proof []byte, outcome contractspb.ParticipantOutcome, code commonpb.ErrorCode, nowMs int64) error {
	if record == nil || invocationID == "" || len(stepHash) != sha256.Size || len(proof) == 0 || len(proof) > participantMaxProofBytes {
		return status.Error(codes.Internal, "farm participant receipt is incomplete")
	}
	if record.GetCore() == nil {
		record.Core = &farmpb.FarmCore{FarmId: record.GetFarmId()}
	}
	cutoff := nowMs - operationReceiptRetention.Milliseconds()
	receipts := record.GetRecentOperationReceipts()
	retainedCount := 0
	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}
		if strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) || receipt.GetCommittedAtMs() >= cutoff {
			retainedCount++
		}
	}
	if retainedCount >= maxRecentReceipts {
		return farmError(commonpb.ErrorCode_ERR_RATE_LIMITED, "farm participant receipt retention window is full")
	}
	// candidateRecord is an actor-owned protobuf clone. Compact its repeated
	// carrier in place instead of allocating and copying a second pointer slice
	// on every participant turn. Count first so a retention rejection leaves the
	// candidate byte-for-byte unchanged.
	writeIndex := 0
	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}
		if strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) || receipt.GetCommittedAtMs() >= cutoff {
			receipts[writeIndex] = receipt
			writeIndex++
		}
	}
	clear(receipts[writeIndex:])
	record.RecentOperationReceipts = receipts[:writeIndex]
	statusValue := commonpb.CommandStatus_REJECTED
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		statusValue = commonpb.CommandStatus_SUCCEEDED
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		statusValue = commonpb.CommandStatus_NO_CHANGE
	}
	receipt := &farmpb.FarmOperationReceipt{OperationId: ParticipantReceiptOperationKey(invocationID), Status: statusValue, ErrorCode: code, ResultPayload: append([]byte(nil), proof...), CommittedAtMs: nowMs, PayloadHash: append([]byte(nil), stepHash...)}
	record.RecentOperationReceipts = append(record.RecentOperationReceipts, receipt)
	return nil
}

// EstimateParticipantCommitPlantModelBytes builds a conservative candidate
// model using the maximum accepted Farm Participant proof.  It is used only
// for pre-mailbox capacity admission; CommitPlant repeats the guard against
// its exact candidate before CASEpoch.  The returned size includes the
// Authority model-field name because Binding.ProjectCASEpochPhysicalCommit
// charges the complete ModelState shape.
func EstimateParticipantCommitPlantModelBytes(record *farmpb.FarmRecord) (int, error) {
	if record == nil {
		return 0, ErrParticipantCapacityUnavailable
	}
	candidate := proto.Clone(record).(*farmpb.FarmRecord)
	if candidate.Core == nil {
		candidate.Core = &farmpb.FarmCore{FarmId: candidate.GetFarmId()}
	}
	stepHash := make([]byte, sha256.Size)
	if err := appendParticipantReceipt(candidate, "capacity-estimate", stepHash, make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
		return 0, err
	}
	return proto.Size(candidate) + len("record"), nil
}

// EstimateParticipantCommitHarvestModelBytes is the conservative detached
// admission estimate for a COMMIT_HARVEST receipt. The in-turn guard repeats
// the check against the exact candidate record before its single CAS.
func EstimateParticipantCommitHarvestModelBytes(record *farmpb.FarmRecord) (int, error) {
	if record == nil {
		return 0, ErrParticipantCapacityUnavailable
	}
	candidate := proto.Clone(record).(*farmpb.FarmRecord)
	if candidate.Core == nil {
		candidate.Core = &farmpb.FarmCore{FarmId: candidate.GetFarmId()}
	}
	stepHash := make([]byte, sha256.Size)
	if err := appendParticipantReceipt(candidate, "capacity-estimate-harvest", stepHash, make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
		return 0, err
	}
	return proto.Size(candidate) + len("record"), nil
}

// EstimateParticipantDecideTheftModelBytes is the detached physical-capacity
// estimate for one DECIDE_THEFT receipt.  The exact turn repeats the guard
// against its complete candidate before CASEpoch.
func EstimateParticipantDecideTheftModelBytes(record *farmpb.FarmRecord) (int, error) {
	if record == nil {
		return 0, ErrParticipantCapacityUnavailable
	}
	candidate := proto.Clone(record).(*farmpb.FarmRecord)
	if candidate.Core == nil {
		candidate.Core = &farmpb.FarmCore{FarmId: candidate.GetFarmId()}
	}
	stepHash := make([]byte, sha256.Size)
	if err := appendParticipantReceipt(candidate, "capacity-estimate-theft", stepHash, make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
		return 0, err
	}
	return proto.Size(candidate) + len("record"), nil
}

// EstimateParticipantApplyGuardSnapshotModelBytes is the detached physical
// capacity estimate for one GuardSync receipt. The mailbox turn repeats this
// check against the exact record containing the full effective snapshot.
func EstimateParticipantApplyGuardSnapshotModelBytes(record *farmpb.FarmRecord) (int, error) {
	if record == nil {
		return 0, ErrParticipantCapacityUnavailable
	}
	candidate := proto.Clone(record).(*farmpb.FarmRecord)
	if candidate.Core == nil {
		candidate.Core = &farmpb.FarmCore{FarmId: candidate.GetFarmId()}
	}
	if err := appendParticipantReceipt(candidate, "capacity-estimate-guard-sync", make([]byte, sha256.Size),
		make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
		return 0, err
	}
	return proto.Size(candidate) + len("record"), nil
}

// ResolveCommitPlantParticipantReceipt is the durable receipt-first lookup
// used by ingress before WorkVersion/Lease verification and again inside the
// FarmActor turn.  It never submits a mailbox task and never re-runs business
// validation for a matching receipt.
func ResolveCommitPlantParticipantReceipt(record *farmpb.FarmRecord, recordVersion uint64, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT || record == nil {
		return nil, false, nil
	}
	wantKey := ParticipantReceiptOperationKey(call.InvocationID)
	var receipt *farmpb.FarmOperationReceipt
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		candidate := record.GetRecentOperationReceipts()[index]
		if candidate != nil && candidate.GetOperationId() == wantKey {
			receipt = candidate
			break
		}
	}
	if receipt == nil {
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	proof, err := decodeParticipantProof(receipt.GetResultPayload())
	if err != nil {
		return nil, true, err
	}
	if proof.invocationID != call.InvocationID || !bytes.Equal(proof.stepHash, call.CanonicalStepPayloadHash[:]) || proof.recordVersion == 0 || proof.recordVersion > recordVersion {
		return nil, true, status.Error(codes.Internal, "farm participant durable receipt binding is corrupt")
	}
	if receipt.GetErrorCode() != proof.errorCode || receipt.GetStatus() != participantCommandStatus(proof.outcome) {
		return nil, true, status.Error(codes.Internal, "farm participant durable receipt status is corrupt")
	}
	return &contracts.StepOutcome{
		Outcome: proof.outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
		CommittedResultHash: append([]byte(nil), proof.resultHash...), Result: proof.result,
		CommittedRecordVersion: proof.recordVersion, Failure: participantFailure(proof.errorCode),
	}, true, nil
}

// ResolveCommitHarvestParticipantReceipt is the durable receipt-first lookup
// used before Work/lease verification and again inside the FarmActor turn. A
// matching invocation returns the original CommitHarvestResult without
// consulting the now-empty Plot or emitting another FarmChange.
func ResolveCommitHarvestParticipantReceipt(record *farmpb.FarmRecord, recordVersion uint64, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST || record == nil {
		return nil, false, nil
	}
	wantKey := ParticipantReceiptOperationKey(call.InvocationID)
	var receipt *farmpb.FarmOperationReceipt
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		candidate := record.GetRecentOperationReceipts()[index]
		if candidate != nil && candidate.GetOperationId() == wantKey {
			receipt = candidate
			break
		}
	}
	if receipt == nil {
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	proof, err := decodeHarvestParticipantProof(receipt.GetResultPayload())
	if err != nil {
		return nil, true, err
	}
	if proof.invocationID != call.InvocationID || !bytes.Equal(proof.stepHash, call.CanonicalStepPayloadHash[:]) || proof.recordVersion == 0 || proof.recordVersion > recordVersion {
		return nil, true, status.Error(codes.Internal, "farm participant durable Harvest receipt binding is corrupt")
	}
	if receipt.GetErrorCode() != proof.errorCode || receipt.GetStatus() != participantCommandStatus(proof.outcome) {
		return nil, true, status.Error(codes.Internal, "farm participant durable Harvest receipt status is corrupt")
	}
	return &contracts.StepOutcome{
		Outcome: proof.outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
		CommittedResultHash: append([]byte(nil), proof.resultHash...), Result: proof.result,
		CommittedRecordVersion: proof.recordVersion, Failure: participantFailure(proof.errorCode),
	}, true, nil
}

// ResolveApplyGuardSnapshotParticipantReceipt provides receipt-first replay
// before Work/route/lease revalidation and again inside the FarmActor turn.
// The original typed result is returned even if newer GuardSync versions have
// since advanced the active farm.
func ResolveApplyGuardSnapshotParticipantReceipt(record *farmpb.FarmRecord, recordVersion uint64, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT || record == nil {
		return nil, false, nil
	}
	wantKey := ParticipantReceiptOperationKey(call.InvocationID)
	var receipt *farmpb.FarmOperationReceipt
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		candidate := record.GetRecentOperationReceipts()[index]
		if candidate != nil && candidate.GetOperationId() == wantKey {
			receipt = candidate
			break
		}
	}
	if receipt == nil {
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	proof, err := decodeGuardSyncParticipantProof(receipt.GetResultPayload())
	if err != nil {
		return nil, true, err
	}
	if proof.invocationID != call.InvocationID || !bytes.Equal(proof.stepHash, call.CanonicalStepPayloadHash[:]) ||
		proof.recordVersion == 0 || proof.recordVersion > recordVersion {
		return nil, true, status.Error(codes.Internal, "farm participant durable GuardSync receipt binding is corrupt")
	}
	if receipt.GetErrorCode() != proof.errorCode || receipt.GetStatus() != participantCommandStatus(proof.outcome) {
		return nil, true, status.Error(codes.Internal, "farm participant durable GuardSync receipt status is corrupt")
	}
	return &contracts.StepOutcome{
		Outcome: proof.outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
		CommittedResultHash: append([]byte(nil), proof.resultHash...), Result: proof.result,
		CommittedRecordVersion: proof.recordVersion, Failure: participantFailure(proof.errorCode),
	}, true, nil
}

type theftParticipantProof struct {
	outcome       contractspb.ParticipantOutcome
	errorCode     commonpb.ErrorCode
	command       contractspb.ParticipantCommandType
	invocationID  string
	stepHash      []byte
	resultHash    []byte
	recordVersion uint64
	result        *contractspb.DecideTheftResult
	header        domain.OpportunityHeader
}

// theftDecisionHash covers every semantic TheftTargetDecision field except
// theft_decision_hash, matching the Harvest proof convention and the frozen
// cross-participant hash contract.
func theftDecisionHash(decision *contractspb.TheftTargetDecision) ([32]byte, error) {
	if decision == nil {
		return [32]byte{}, errors.New("farm participant: TheftTargetDecision is missing")
	}
	withoutHash := proto.Clone(decision).(*contractspb.TheftTargetDecision)
	withoutHash.TheftDecisionHash = nil
	return contracts.CanonicalHash(withoutHash)
}

func validateTheftDecision(decision *contractspb.TheftTargetDecision) error {
	if decision == nil || decision.GetWorkflowId() == "" || decision.GetSettlementId() == "" ||
		decision.GetTheftAttemptId() == "" || decision.GetVisitorPlayerId() == 0 || decision.GetOwnerPlayerId() == 0 ||
		decision.GetOwnerPlayerId() != decision.GetFarmId() || decision.GetVisitorPlayerId() == decision.GetOwnerPlayerId() ||
		decision.GetPlotId() == 0 || decision.GetPlotId() > participantMaxPlots || decision.GetCropCycleId() == "" {
		return errors.New("farm participant: TheftTargetDecision identity is invalid")
	}
	wantAttempt, err := identity.TheftAttemptID(decision.GetFarmId(), decision.GetCropCycleId(), decision.GetVisitorPlayerId())
	if err != nil || decision.GetTheftAttemptId() != wantAttempt {
		return errors.New("farm participant: TheftTargetDecision attempt binding is invalid")
	}
	wantSettlement, err := identity.TheftSettlementID(decision.GetWorkflowId())
	if err != nil || decision.GetSettlementId() != wantSettlement {
		return errors.New("farm participant: TheftTargetDecision settlement binding is invalid")
	}
	if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN &&
		decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD {
		return errors.New("farm participant: TheftTargetDecision outcome is invalid")
	}
	if decision.GetGrossStolenQuantity() == 0 || decision.GetGuardLostQuantity() > decision.GetGrossStolenQuantity() ||
		decision.GetNetReceivedQuantity()+decision.GetGuardLostQuantity() != decision.GetGrossStolenQuantity() ||
		(decision.GetTheftOutcome() == contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD && decision.GetGuardLostQuantity() != decision.GetGrossStolenQuantity()) ||
		(decision.GetTheftOutcome() == contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN && decision.GetGuardLostQuantity() != 0) {
		return errors.New("farm participant: TheftTargetDecision quantity conservation is invalid")
	}
	if len(decision.GetTheftDecisionHash()) != sha256.Size {
		return errors.New("farm participant: TheftTargetDecision hash is missing")
	}
	computed, err := theftDecisionHash(decision)
	if err != nil || !bytes.Equal(computed[:], decision.GetTheftDecisionHash()) {
		return errors.New("farm participant: TheftTargetDecision hash is invalid")
	}
	guard := decision.GetGuardDecisionSnapshot()
	if guard == nil || guard.GetTheftAttemptId() != decision.GetTheftAttemptId() ||
		guard.GetGrossStolenQuantity() != decision.GetGrossStolenQuantity() ||
		guard.GetGuardLostQuantity() != decision.GetGuardLostQuantity() ||
		guard.GetNetReceivedQuantity() != decision.GetNetReceivedQuantity() ||
		guard.GetCropUnitCoinValue() != decision.GetCropUnitCoinValue() ||
		guard.GetGuardCoinValue() != decision.GetGuardCoinValue() ||
		guard.GetRandomDrawBp() >= 10000 ||
		(guard.GetGuardTriggered() && guard.GetInterceptPolicy() != contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS) ||
		(!guard.GetGuardTriggered() && guard.GetGuardLostQuantity() != 0) {
		return errors.New("farm participant: GuardDecisionSnapshot is inconsistent")
	}
	if guard.GetGuardCoinValue() != 0 && guard.GetGuardLostQuantity() != 0 &&
		guard.GetGuardCoinValue()/guard.GetGuardLostQuantity() != guard.GetCropUnitCoinValue() {
		return errors.New("farm participant: GuardDecisionSnapshot coin multiplication is invalid")
	}
	return nil
}

func encodeTheftParticipantProof(call *contracts.ParticipantCall, outcome contractspb.ParticipantOutcome, code commonpb.ErrorCode, result *contractspb.DecideTheftResult, resultHash []byte, recordVersion uint64, header domain.OpportunityHeader) ([]byte, error) {
	if call == nil || result == nil || len(resultHash) != sha256.Size || recordVersion == 0 {
		return nil, errors.New("farm participant: Theft result proof input is incomplete")
	}
	typedResult, canonicalHash, err := contracts.ValidateTypedResult(call.Spec, result)
	if err != nil || !bytes.Equal(canonicalHash, resultHash) {
		return nil, errors.New("farm participant: Theft result proof hash is inconsistent")
	}
	if outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		if err := validateTheftDecision(result.GetTheftTargetDecision()); err != nil {
			return nil, err
		}
		if result.GetFarmRecordVersion() != recordVersion || result.GetCommittedPlotVersion() == 0 || result.GetCommittedFarmVersion() == 0 {
			return nil, errors.New("farm participant: applied Theft result proof versions are invalid")
		}
	}
	buf := make([]byte, 0, len(theftProofMagic)+len(typedResult)+256)
	writeParticipantLPBytes(&buf, []byte(theftProofMagic))
	putParticipantUint32(&buf, participantProofVersion)
	putParticipantUint32(&buf, uint32(call.Spec.Command))
	writeParticipantLPBytes(&buf, []byte(call.InvocationID))
	writeParticipantLPBytes(&buf, call.CanonicalStepPayloadHash[:])
	writeParticipantLPBytes(&buf, resultHash)
	putParticipantUint64(&buf, recordVersion)
	putParticipantUint32(&buf, uint32(code))
	putParticipantUint32(&buf, uint32(outcome))
	writeParticipantLPBytes(&buf, typedResult)
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	if len(buf) > participantMaxProofBytes {
		return nil, errors.New("farm participant: Theft result proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeTheftParticipantProof(data []byte) (theftParticipantProof, error) {
	var proof theftParticipantProof
	if len(data) == 0 || len(data) > participantMaxProofBytes {
		return proof, errors.New("farm participant: Theft result proof size is invalid")
	}
	reader := participantProofReader{data: data}
	magic, err := reader.lp()
	if err != nil || string(magic) != theftProofMagic {
		return proof, errors.New("farm participant: Theft result proof magic is invalid")
	}
	version, err := reader.u32()
	if err != nil || version != participantProofVersion {
		return proof, errors.New("farm participant: Theft result proof version is invalid")
	}
	command, err := reader.u32()
	if err != nil || command != uint32(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT) {
		return proof, errors.New("farm participant: Theft result proof command is invalid")
	}
	invocation, err := reader.lp()
	if err != nil || len(invocation) == 0 {
		return proof, errors.New("farm participant: Theft result proof invocation is invalid")
	}
	stepHash, err := reader.lp()
	if err != nil || len(stepHash) != sha256.Size {
		return proof, errors.New("farm participant: Theft result proof step hash is invalid")
	}
	resultHash, err := reader.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, errors.New("farm participant: Theft result proof result hash is invalid")
	}
	recordVersion, err := reader.u64()
	if err != nil || recordVersion == 0 {
		return proof, errors.New("farm participant: Theft result proof record version is invalid")
	}
	errorValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	outcomeValue, err := reader.u32()
	if err != nil || outcomeValue < uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED) || outcomeValue > uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) {
		return proof, errors.New("farm participant: Theft result proof outcome is invalid")
	}
	if _, ok := commonpb.ErrorCode_name[int32(errorValue)]; !ok {
		return proof, errors.New("farm participant: Theft result proof error code is unknown")
	}
	typedResult, err := reader.lp()
	if err != nil || len(typedResult) == 0 {
		return proof, errors.New("farm participant: Theft result proof typed result is missing")
	}
	headerSourceVersion, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerDirtyGeneration, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerNextOpportunity, err := reader.i64()
	if err != nil {
		return proof, err
	}
	headerMask, err := reader.u32()
	if err != nil || reader.remaining() != 0 {
		return proof, errors.New("farm participant: Theft result proof header is malformed")
	}
	result := &contractspb.DecideTheftResult{}
	if err := proto.Unmarshal(typedResult, result); err != nil {
		return proof, errors.New("farm participant: Theft result proof typed result is malformed")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		return proof, err
	}
	_, canonical, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonical, resultHash) {
		return proof, errors.New("farm participant: Theft result proof typed hash is invalid")
	}
	proof = theftParticipantProof{
		outcome: contractspb.ParticipantOutcome(outcomeValue), errorCode: commonpb.ErrorCode(errorValue),
		command: contractspb.ParticipantCommandType(command), invocationID: string(invocation),
		stepHash: append([]byte(nil), stepHash...), resultHash: append([]byte(nil), resultHash...), recordVersion: recordVersion,
		result: result, header: domain.OpportunityHeader{SourceFarmVersion: headerSourceVersion, DirtyGeneration: headerDirtyGeneration, NextOpportunityAtMs: headerNextOpportunity, CandidateMask: domain.OpportunityMask(headerMask)},
	}
	if proof.outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		if err := validateTheftDecision(result.GetTheftTargetDecision()); err != nil {
			return theftParticipantProof{}, err
		}
		if result.GetFarmRecordVersion() != recordVersion || result.GetCommittedPlotVersion() == 0 || result.GetCommittedFarmVersion() == 0 {
			return theftParticipantProof{}, errors.New("farm participant: applied Theft result proof versions are invalid")
		}
	}
	return proof, nil
}

// ResolveDecideTheftParticipantReceipt is the receipt-first replay boundary.
// A matching invocation returns the persisted result without rechecking
// current maturity, GuardSnapshot, permit expiry or public target state.
func ResolveDecideTheftParticipantReceipt(record *farmpb.FarmRecord, recordVersion uint64, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT || record == nil {
		return nil, false, nil
	}
	wantKey := ParticipantReceiptOperationKey(call.InvocationID)
	var receipt *farmpb.FarmOperationReceipt
	for index := len(record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		candidate := record.GetRecentOperationReceipts()[index]
		if candidate != nil && candidate.GetOperationId() == wantKey {
			receipt = candidate
			break
		}
	}
	if receipt == nil {
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	proof, err := decodeTheftParticipantProof(receipt.GetResultPayload())
	if err != nil {
		return nil, true, err
	}
	if proof.invocationID != call.InvocationID || !bytes.Equal(proof.stepHash, call.CanonicalStepPayloadHash[:]) || proof.recordVersion == 0 || proof.recordVersion > recordVersion {
		return nil, true, status.Error(codes.Internal, "farm participant durable Theft receipt binding is corrupt")
	}
	if receipt.GetErrorCode() != proof.errorCode || receipt.GetStatus() != participantCommandStatus(proof.outcome) {
		return nil, true, status.Error(codes.Internal, "farm participant durable Theft receipt status is corrupt")
	}
	return &contracts.StepOutcome{
		Outcome: proof.outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
		CommittedResultHash: append([]byte(nil), proof.resultHash...), Result: proof.result,
		CommittedRecordVersion: proof.recordVersion, Failure: participantFailure(proof.errorCode),
	}, true, nil
}

type harvestParticipantProof struct {
	outcome       contractspb.ParticipantOutcome
	errorCode     commonpb.ErrorCode
	command       contractspb.ParticipantCommandType
	invocationID  string
	stepHash      []byte
	resultHash    []byte
	recordVersion uint64
	result        *contractspb.CommitHarvestResult
	header        domain.OpportunityHeader
	endedCycleID  string
	endReason     domain.CycleEndReason
}

func encodeHarvestParticipantProof(call *contracts.ParticipantCall, outcome contractspb.ParticipantOutcome, code commonpb.ErrorCode, result *contractspb.CommitHarvestResult, resultHash []byte, recordVersion uint64, header domain.OpportunityHeader) ([]byte, error) {
	if call == nil || result == nil || len(resultHash) != sha256.Size || recordVersion == 0 {
		return nil, errors.New("farm participant: Harvest result proof input is incomplete")
	}
	typedResult, canonicalHash, err := contracts.ValidateTypedResult(call.Spec, result)
	if err != nil || !bytes.Equal(canonicalHash, resultHash) {
		return nil, errors.New("farm participant: Harvest result proof hash is inconsistent")
	}
	decision := result.GetHarvestDecision()
	endedCycleID := ""
	if decision != nil {
		endedCycleID = decision.GetCropCycleId()
	}
	buf := make([]byte, 0, len(harvestProofMagic)+len(typedResult)+256)
	writeParticipantLPBytes(&buf, []byte(harvestProofMagic))
	putParticipantUint32(&buf, participantProofVersion)
	putParticipantUint32(&buf, uint32(call.Spec.Command))
	writeParticipantLPBytes(&buf, []byte(call.InvocationID))
	writeParticipantLPBytes(&buf, call.CanonicalStepPayloadHash[:])
	writeParticipantLPBytes(&buf, resultHash)
	putParticipantUint64(&buf, recordVersion)
	putParticipantUint32(&buf, uint32(code))
	putParticipantUint32(&buf, uint32(outcome))
	writeParticipantLPBytes(&buf, typedResult)
	writeParticipantLPBytes(&buf, []byte(endedCycleID))
	putParticipantUint32(&buf, uint32(domain.CycleEndReasonHarvested))
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	if len(buf) > participantMaxProofBytes {
		return nil, errors.New("farm participant: Harvest result proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeHarvestParticipantProof(data []byte) (harvestParticipantProof, error) {
	var proof harvestParticipantProof
	if len(data) == 0 || len(data) > participantMaxProofBytes {
		return proof, errors.New("farm participant: Harvest result proof size is invalid")
	}
	reader := participantProofReader{data: data}
	magic, err := reader.lp()
	if err != nil || string(magic) != harvestProofMagic {
		return proof, errors.New("farm participant: Harvest result proof magic is invalid")
	}
	version, err := reader.u32()
	if err != nil || version != participantProofVersion {
		return proof, errors.New("farm participant: Harvest result proof version is invalid")
	}
	command, err := reader.u32()
	if err != nil || command != uint32(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST) {
		return proof, errors.New("farm participant: Harvest result proof command is invalid")
	}
	invocation, err := reader.lp()
	if err != nil || len(invocation) == 0 {
		return proof, errors.New("farm participant: Harvest result proof invocation is invalid")
	}
	stepHash, err := reader.lp()
	if err != nil || len(stepHash) != sha256.Size {
		return proof, errors.New("farm participant: Harvest result proof step hash is invalid")
	}
	resultHash, err := reader.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, errors.New("farm participant: Harvest result proof result hash is invalid")
	}
	recordVersion, err := reader.u64()
	if err != nil || recordVersion == 0 {
		return proof, errors.New("farm participant: Harvest result proof record version is invalid")
	}
	errorValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	outcomeValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	if outcomeValue < uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED) || outcomeValue > uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) {
		return proof, errors.New("farm participant: Harvest result proof outcome is not terminal")
	}
	if _, ok := commonpb.ErrorCode_name[int32(errorValue)]; !ok {
		return proof, errors.New("farm participant: Harvest result proof error code is unknown")
	}
	typedResult, err := reader.lp()
	if err != nil || len(typedResult) == 0 {
		return proof, errors.New("farm participant: Harvest result proof typed result is missing")
	}
	endedCycleID, err := reader.lp()
	if err != nil {
		return proof, err
	}
	endReasonValue, err := reader.u32()
	if err != nil || endReasonValue != uint32(domain.CycleEndReasonHarvested) {
		return proof, errors.New("farm participant: Harvest result proof end reason is invalid")
	}
	headerSourceVersion, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerDirtyGeneration, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerNextOpportunity, err := reader.i64()
	if err != nil {
		return proof, err
	}
	headerMask, err := reader.u32()
	if err != nil {
		return proof, err
	}
	if reader.remaining() != 0 {
		return proof, errors.New("farm participant: Harvest result proof has trailing bytes")
	}
	result := &contractspb.CommitHarvestResult{}
	if err := proto.Unmarshal(typedResult, result); err != nil {
		return proof, errors.New("farm participant: Harvest result proof typed result is malformed")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if err != nil {
		return proof, err
	}
	_, canonical, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonical, resultHash) {
		return proof, errors.New("farm participant: Harvest result proof typed hash is invalid")
	}
	proof = harvestParticipantProof{
		outcome: contractspb.ParticipantOutcome(outcomeValue), errorCode: commonpb.ErrorCode(errorValue),
		command: contractspb.ParticipantCommandType(command), invocationID: string(invocation),
		stepHash: append([]byte(nil), stepHash...), resultHash: append([]byte(nil), resultHash...),
		recordVersion: recordVersion, result: result, header: domain.OpportunityHeader{
			SourceFarmVersion: headerSourceVersion, DirtyGeneration: headerDirtyGeneration,
			NextOpportunityAtMs: headerNextOpportunity, CandidateMask: domain.OpportunityMask(headerMask),
		}, endedCycleID: string(endedCycleID), endReason: domain.CycleEndReason(endReasonValue),
	}
	if proof.outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		decision := result.GetHarvestDecision()
		if decision == nil || decision.GetCropCycleId() == "" || string(endedCycleID) != decision.GetCropCycleId() {
			return harvestParticipantProof{}, errors.New("farm participant: applied Harvest result proof cycle binding is invalid")
		}
		if err := validateHarvestDecisionHash(decision); err != nil {
			return harvestParticipantProof{}, err
		}
		if result.GetFarmRecordVersion() != recordVersion || result.GetCommittedPlotVersion() == 0 || result.GetCommittedFarmVersion() == 0 {
			return harvestParticipantProof{}, errors.New("farm participant: applied Harvest result proof versions are invalid")
		}
	}
	return proof, nil
}

type guardSyncParticipantProof struct {
	outcome           contractspb.ParticipantOutcome
	errorCode         commonpb.ErrorCode
	command           contractspb.ParticipantCommandType
	invocationID      string
	stepHash          []byte
	resultHash        []byte
	recordVersion     uint64
	result            *contractspb.ApplyGuardSnapshotResult
	effectiveSnapshot domain.GuardSnapshot
	header            domain.OpportunityHeader
}

func encodeGuardSyncParticipantProof(call *contracts.ParticipantCall, outcome contractspb.ParticipantOutcome,
	code commonpb.ErrorCode, result *contractspb.ApplyGuardSnapshotResult, resultHash []byte, recordVersion uint64,
	effectiveSnapshot domain.GuardSnapshot, header domain.OpportunityHeader) ([]byte, error) {
	if call == nil || result == nil || len(resultHash) != sha256.Size || recordVersion == 0 || effectiveSnapshot.GuardVersion == 0 {
		return nil, errors.New("farm participant: GuardSync result proof input is incomplete")
	}
	typedResult, canonicalHash, err := contracts.ValidateTypedResult(call.Spec, result)
	if err != nil || !bytes.Equal(canonicalHash, resultHash) {
		return nil, errors.New("farm participant: GuardSync result proof hash is inconsistent")
	}
	wireSnapshot, err := participantProtoGuardSnapshot(effectiveSnapshot)
	if err != nil {
		return nil, err
	}
	snapshotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(wireSnapshot)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, len(guardSyncProofMagic)+len(typedResult)+len(snapshotBytes)+256)
	writeParticipantLPBytes(&buf, []byte(guardSyncProofMagic))
	putParticipantUint32(&buf, participantProofVersion)
	putParticipantUint32(&buf, uint32(call.Spec.Command))
	writeParticipantLPBytes(&buf, []byte(call.InvocationID))
	writeParticipantLPBytes(&buf, call.CanonicalStepPayloadHash[:])
	writeParticipantLPBytes(&buf, resultHash)
	putParticipantUint64(&buf, recordVersion)
	putParticipantUint32(&buf, uint32(code))
	putParticipantUint32(&buf, uint32(outcome))
	writeParticipantLPBytes(&buf, typedResult)
	writeParticipantLPBytes(&buf, snapshotBytes)
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	if len(buf) > participantMaxProofBytes {
		return nil, errors.New("farm participant: GuardSync result proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeGuardSyncParticipantProof(data []byte) (guardSyncParticipantProof, error) {
	var proof guardSyncParticipantProof
	if len(data) == 0 || len(data) > participantMaxProofBytes {
		return proof, errors.New("farm participant: GuardSync result proof size is invalid")
	}
	reader := participantProofReader{data: data}
	magic, err := reader.lp()
	if err != nil || string(magic) != guardSyncProofMagic {
		return proof, errors.New("farm participant: GuardSync result proof magic is invalid")
	}
	version, err := reader.u32()
	if err != nil || version != participantProofVersion {
		return proof, errors.New("farm participant: GuardSync result proof version is invalid")
	}
	command, err := reader.u32()
	if err != nil || command != uint32(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT) {
		return proof, errors.New("farm participant: GuardSync result proof command is invalid")
	}
	invocation, err := reader.lp()
	if err != nil || len(invocation) == 0 {
		return proof, errors.New("farm participant: GuardSync result proof invocation is invalid")
	}
	stepHash, err := reader.lp()
	if err != nil || len(stepHash) != sha256.Size {
		return proof, errors.New("farm participant: GuardSync result proof step hash is invalid")
	}
	resultHash, err := reader.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, errors.New("farm participant: GuardSync result proof result hash is invalid")
	}
	recordVersion, err := reader.u64()
	if err != nil || recordVersion == 0 {
		return proof, errors.New("farm participant: GuardSync result proof record version is invalid")
	}
	errorValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	outcomeValue, err := reader.u32()
	if err != nil || outcomeValue < uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED) ||
		outcomeValue > uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) {
		return proof, errors.New("farm participant: GuardSync result proof outcome is invalid")
	}
	if _, ok := commonpb.ErrorCode_name[int32(errorValue)]; !ok {
		return proof, errors.New("farm participant: GuardSync result proof error code is unknown")
	}
	typedResult, err := reader.lp()
	if err != nil || len(typedResult) == 0 {
		return proof, errors.New("farm participant: GuardSync result proof typed result is missing")
	}
	snapshotBytes, err := reader.lp()
	if err != nil || len(snapshotBytes) == 0 {
		return proof, errors.New("farm participant: GuardSync result proof snapshot is missing")
	}
	headerSourceVersion, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerDirtyGeneration, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerNextOpportunity, err := reader.i64()
	if err != nil {
		return proof, err
	}
	headerMask, err := reader.u32()
	if err != nil || reader.remaining() != 0 {
		return proof, errors.New("farm participant: GuardSync result proof has trailing bytes")
	}
	result := &contractspb.ApplyGuardSnapshotResult{}
	if err := proto.Unmarshal(typedResult, result); err != nil {
		return proof, errors.New("farm participant: GuardSync result proof typed result is malformed")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT)
	if err != nil {
		return proof, err
	}
	_, canonical, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonical, resultHash) {
		return proof, errors.New("farm participant: GuardSync result proof typed hash is invalid")
	}
	wireSnapshot := &contractspb.GuardSnapshot{}
	if err := proto.Unmarshal(snapshotBytes, wireSnapshot); err != nil {
		return proof, errors.New("farm participant: GuardSync result proof snapshot is malformed")
	}
	effective, err := participantDomainGuardSnapshot(&contractspb.ApplyGuardSnapshotPayload{
		GuardSnapshot: wireSnapshot, SnapshotHash: append([]byte(nil), wireSnapshot.GetSnapshotHash()...),
	})
	if err != nil {
		return proof, errors.New("farm participant: GuardSync result proof snapshot is invalid")
	}
	if result.GetGuardVersion() != effective.GuardVersion || !bytes.Equal(result.GetSnapshotHash(), wireSnapshot.GetSnapshotHash()) ||
		result.GetCommittedRecordVersion() != recordVersion {
		return proof, errors.New("farm participant: GuardSync result proof effective snapshot binding is invalid")
	}
	outcome := contractspb.ParticipantOutcome(outcomeValue)
	switch result.GetApplyStatus() {
	case contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED:
		if outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			return proof, errors.New("farm participant: GuardSync APPLIED proof outcome is invalid")
		}
	case contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_SUPERSEDED:
		if outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
			return proof, errors.New("farm participant: GuardSync SUPERSEDED proof outcome is invalid")
		}
	default:
		if outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
			return proof, errors.New("farm participant: GuardSync rejected proof outcome is invalid")
		}
	}
	proof = guardSyncParticipantProof{
		outcome: outcome, errorCode: commonpb.ErrorCode(errorValue), command: contractspb.ParticipantCommandType(command),
		invocationID: string(invocation), stepHash: append([]byte(nil), stepHash...), resultHash: append([]byte(nil), resultHash...),
		recordVersion: recordVersion, result: result, effectiveSnapshot: effective,
		header: domain.OpportunityHeader{SourceFarmVersion: headerSourceVersion, DirtyGeneration: headerDirtyGeneration,
			NextOpportunityAtMs: headerNextOpportunity, CandidateMask: domain.OpportunityMask(headerMask)},
	}
	return proof, nil
}

func participantProofHasMagic(data []byte, magic string) bool {
	reader := participantProofReader{data: data}
	value, err := reader.lp()
	return err == nil && string(value) == magic
}

func (a *FarmActor) restoreParticipantGuardSnapshot() error {
	if a == nil || a.record == nil {
		return nil
	}
	for index := len(a.record.GetRecentOperationReceipts()) - 1; index >= 0; index-- {
		receipt := a.record.GetRecentOperationReceipts()[index]
		if receipt == nil || !strings.HasPrefix(receipt.GetOperationId(), participantReceiptOperationPrefix) ||
			!participantProofHasMagic(receipt.GetResultPayload(), guardSyncProofMagic) {
			continue
		}
		proof, err := decodeGuardSyncParticipantProof(receipt.GetResultPayload())
		if err != nil {
			return fmt.Errorf("farm actor: restore GuardSync receipt: %w", err)
		}
		a.theftGuardSnapshot = proof.effectiveSnapshot
		return nil
	}
	return nil
}

func participantCommandStatus(outcome contractspb.ParticipantOutcome) commonpb.CommandStatus {
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		return commonpb.CommandStatus_SUCCEEDED
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		return commonpb.CommandStatus_NO_CHANGE
	default:
		return commonpb.CommandStatus_REJECTED
	}
}

type participantProof struct {
	outcome       contractspb.ParticipantOutcome
	errorCode     commonpb.ErrorCode
	command       contractspb.ParticipantCommandType
	invocationID  string
	stepHash      []byte
	resultHash    []byte
	recordVersion uint64
	result        *contractspb.CommitPlantResult
	rule          *contractspb.CropRuleSnapshot
	ruleHash      []byte
	stealSnapshot *contractspb.StealPlanSnapshot
	careSnapshot  *contractspb.CarePlanSnapshot
	compendium    *contractspb.CompendiumUnlockSnapshot
	header        domain.OpportunityHeader
}

func (p participantProof) cropState() *participantCropState {
	if p.result == nil || p.rule == nil || p.stealSnapshot == nil || p.careSnapshot == nil || p.compendium == nil {
		return nil
	}
	cycle := domain.CropCycle{
		CycleID: p.result.GetCropCycleId(), PlotID: p.result.GetPlotId(), CropDefinitionID: p.result.GetCropDefinitionId(), RuleVersion: p.result.GetAppliedCropRuleVersion(),
		PlantAtMs: p.result.GetPlantAtMs(), MatureAtMs: p.result.GetMatureAtMs(), BaseYield: uint32(p.rule.GetBaseYield()), StealableLimit: uint32(p.stealSnapshot.GetTotalCapQuantity()),
		HarvestExp: p.rule.GetHarvestExp(), CropUnitCoinValue: p.rule.GetCropUnitCoinValue(),
		StealPlan:  domain.StealPlanSnapshot{PolicyVersion: p.stealSnapshot.GetPolicyVersion(), TotalCapRatioBP: p.stealSnapshot.GetTotalCapRatioBp(), SlotRatioBP: append([]uint32(nil), p.stealSnapshot.GetSlotRatioBp()...)},
		CarePlan:   domain.CarePlanSnapshot{CareRuleVersion: p.careSnapshot.GetCareRuleVersion()},
		Compendium: domain.CompendiumUnlockSnapshot{CropDefinitionID: p.compendium.GetCropDefinitionId(), UnlockEligible: p.compendium.GetUnlockEligible(), PlantedByPlayerID: p.compendium.GetPlantedByPlayerId(), ActivityID: p.compendium.GetActivityId(), CatalogVersionAtPlant: p.compendium.GetCatalogVersionAtPlant(), DefinitionVersionAtPlant: p.compendium.GetDefinitionVersionAtPlant()},
	}
	for _, quantity := range p.stealSnapshot.GetSlotQuantity() {
		if quantity > uint64(^uint32(0)) {
			return nil
		}
		cycle.StealPlan.SlotQuantity = append(cycle.StealPlan.SlotQuantity, uint32(quantity))
	}
	for _, incident := range p.careSnapshot.GetIncidents() {
		if incident == nil {
			return nil
		}
		careType := domain.CareTypeUnspecified
		if incident.GetCareType() == contractspb.CareType_CARE_TYPE_WATER {
			careType = domain.CareTypeWater
		} else if incident.GetCareType() == contractspb.CareType_CARE_TYPE_REMOVE_PEST {
			careType = domain.CareTypeRemovePest
		}
		cycle.Incidents = append(cycle.Incidents, domain.CareIncident{IncidentID: incident.GetIncidentId(), CareType: careType, DueAtMs: incident.GetDueAtMs()})
	}
	return &participantCropState{cycle: cycle, rule: proto.Clone(p.rule).(*contractspb.CropRuleSnapshot), ruleHash: append([]byte(nil), p.ruleHash...), stealSnapshot: proto.Clone(p.stealSnapshot).(*contractspb.StealPlanSnapshot), careSnapshot: proto.Clone(p.careSnapshot).(*contractspb.CarePlanSnapshot), compendium: proto.Clone(p.compendium).(*contractspb.CompendiumUnlockSnapshot)}
}

func encodeParticipantProof(call *contracts.ParticipantCall, outcome contractspb.ParticipantOutcome, code commonpb.ErrorCode, result *contractspb.CommitPlantResult, resultHash []byte, recordVersion uint64, crop *participantCropState, header domain.OpportunityHeader) ([]byte, error) {
	if call == nil || result == nil || len(resultHash) != sha256.Size || recordVersion == 0 {
		return nil, errors.New("farm participant: result proof input is incomplete")
	}
	typedResult, canonicalHash, err := contracts.ValidateTypedResult(call.Spec, result)
	if err != nil || !bytes.Equal(canonicalHash, resultHash) {
		return nil, errors.New("farm participant: result proof hash is inconsistent")
	}
	buf := make([]byte, 0, len(participantProofMagic)+len(typedResult)+1024)
	writeParticipantLPBytes(&buf, []byte(participantProofMagic))
	putParticipantUint32(&buf, participantProofVersion)
	putParticipantUint32(&buf, uint32(call.Spec.Command))
	writeParticipantLPBytes(&buf, []byte(call.InvocationID))
	writeParticipantLPBytes(&buf, call.CanonicalStepPayloadHash[:])
	writeParticipantLPBytes(&buf, resultHash)
	putParticipantUint64(&buf, recordVersion)
	putParticipantUint32(&buf, uint32(code))
	putParticipantUint32(&buf, uint32(outcome))
	writeParticipantLPBytes(&buf, typedResult)
	if crop == nil {
		writeParticipantLPBytes(&buf, nil)
	} else {
		writeParticipantLPBytes(&buf, crop.ruleHash)
	}
	if crop == nil {
		for index := 0; index < 4; index++ {
			writeParticipantLPBytes(&buf, nil)
		}
	} else {
		values := []proto.Message{crop.rule, crop.stealSnapshot, crop.careSnapshot, crop.compendium}
		for _, value := range values {
			data, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
			if err != nil {
				return nil, err
			}
			writeParticipantLPBytes(&buf, data)
		}
	}
	putParticipantUint64(&buf, header.SourceFarmVersion)
	putParticipantUint64(&buf, header.DirtyGeneration)
	putParticipantInt64(&buf, header.NextOpportunityAtMs)
	putParticipantUint32(&buf, uint32(header.CandidateMask))
	if len(buf) > participantMaxProofBytes {
		return nil, errors.New("farm participant: result proof exceeds fixed limit")
	}
	return buf, nil
}

func decodeParticipantProof(data []byte) (participantProof, error) {
	var proof participantProof
	if len(data) == 0 || len(data) > participantMaxProofBytes {
		return proof, errors.New("farm participant: result proof size is invalid")
	}
	reader := participantProofReader{data: data}
	magic, err := reader.lp()
	if err != nil || string(magic) != participantProofMagic {
		return proof, errors.New("farm participant: result proof magic is invalid")
	}
	version, err := reader.u32()
	if err != nil || version != participantProofVersion {
		return proof, errors.New("farm participant: result proof version is invalid")
	}
	command, err := reader.u32()
	if err != nil || command != uint32(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT) {
		return proof, errors.New("farm participant: result proof command is invalid")
	}
	invocation, err := reader.lp()
	if err != nil || len(invocation) == 0 {
		return proof, errors.New("farm participant: result proof invocation is invalid")
	}
	stepHash, err := reader.lp()
	if err != nil || len(stepHash) != sha256.Size {
		return proof, errors.New("farm participant: result proof step hash is invalid")
	}
	resultHash, err := reader.lp()
	if err != nil || len(resultHash) != sha256.Size {
		return proof, errors.New("farm participant: result proof result hash is invalid")
	}
	recordVersion, err := reader.u64()
	if err != nil || recordVersion == 0 {
		return proof, errors.New("farm participant: result proof record version is invalid")
	}
	errorValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	outcomeValue, err := reader.u32()
	if err != nil {
		return proof, err
	}
	if outcomeValue < uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED) || outcomeValue > uint32(contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) {
		return proof, errors.New("farm participant: result proof outcome is not terminal")
	}
	if _, ok := commonpb.ErrorCode_name[int32(errorValue)]; !ok {
		return proof, errors.New("farm participant: result proof error code is unknown")
	}
	typedResult, err := reader.lp()
	if err != nil || len(typedResult) == 0 {
		return proof, errors.New("farm participant: result proof typed result is missing")
	}
	ruleHash, err := reader.lp()
	if err != nil || (ruleHash != nil && len(ruleHash) != sha256.Size) {
		return proof, errors.New("farm participant: result proof crop rule hash is invalid")
	}
	result := &contractspb.CommitPlantResult{}
	if err := proto.Unmarshal(typedResult, result); err != nil {
		return proof, errors.New("farm participant: result proof typed result is malformed")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		return proof, err
	}
	_, canonical, err := contracts.ValidateTypedResult(spec, result)
	if err != nil || !bytes.Equal(canonical, resultHash) {
		return proof, errors.New("farm participant: result proof typed hash is invalid")
	}
	values := make([][]byte, 4)
	for index := range values {
		values[index], err = reader.lp()
		if err != nil {
			return proof, err
		}
	}
	headerSourceVersion, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerDirtyGeneration, err := reader.u64()
	if err != nil {
		return proof, err
	}
	headerNextOpportunity, err := reader.i64()
	if err != nil {
		return proof, err
	}
	headerMask, err := reader.u32()
	if err != nil {
		return proof, err
	}
	if reader.remaining() != 0 {
		return proof, errors.New("farm participant: result proof has trailing bytes")
	}
	proof = participantProof{outcome: contractspb.ParticipantOutcome(outcomeValue), errorCode: commonpb.ErrorCode(errorValue), command: contractspb.ParticipantCommandType(command), invocationID: string(invocation), stepHash: append([]byte(nil), stepHash...), resultHash: append([]byte(nil), resultHash...), recordVersion: recordVersion, result: result, ruleHash: append([]byte(nil), ruleHash...), header: domain.OpportunityHeader{SourceFarmVersion: headerSourceVersion, DirtyGeneration: headerDirtyGeneration, NextOpportunityAtMs: headerNextOpportunity, CandidateMask: domain.OpportunityMask(headerMask)}}
	if values[0] != nil {
		proof.rule = &contractspb.CropRuleSnapshot{}
		if err := proto.Unmarshal(values[0], proof.rule); err != nil {
			return participantProof{}, err
		}
		if err := validateCropRuleSnapshot(proof.rule); err != nil {
			return participantProof{}, err
		}
	}
	if proof.outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		if proof.rule == nil || len(proof.ruleHash) != sha256.Size || values[1] == nil || values[2] == nil || values[3] == nil {
			return participantProof{}, errors.New("farm participant: applied result proof is missing frozen snapshots")
		}
		computedRuleHash, err := contracts.CanonicalHash(proof.rule)
		if err != nil || !bytes.Equal(computedRuleHash[:], proof.ruleHash) {
			return participantProof{}, errors.New("farm participant: applied result proof crop rule hash is invalid")
		}
		proof.stealSnapshot = &contractspb.StealPlanSnapshot{}
		proof.careSnapshot = &contractspb.CarePlanSnapshot{}
		proof.compendium = &contractspb.CompendiumUnlockSnapshot{}
		if err := proto.Unmarshal(values[1], proof.stealSnapshot); err != nil {
			return participantProof{}, err
		}
		if err := proto.Unmarshal(values[2], proof.careSnapshot); err != nil {
			return participantProof{}, err
		}
		if err := proto.Unmarshal(values[3], proof.compendium); err != nil {
			return participantProof{}, err
		}
		if err := validateParticipantRuntimeSnapshots(proof.rule, proof.stealSnapshot, proof.careSnapshot, proof.compendium, proof.result); err != nil {
			return participantProof{}, err
		}
	}
	return proof, nil
}

func validateParticipantRuntimeSnapshots(rule *contractspb.CropRuleSnapshot, steal *contractspb.StealPlanSnapshot, care *contractspb.CarePlanSnapshot, compendium *contractspb.CompendiumUnlockSnapshot, result *contractspb.CommitPlantResult) error {
	if rule == nil || steal == nil || care == nil || compendium == nil || result == nil {
		return errors.New("farm participant: applied runtime snapshots are incomplete")
	}
	if compendium.GetPlantedByPlayerId() == 0 || compendium.GetCropDefinitionId() != rule.GetCropDefinitionId() {
		return errors.New("farm participant: applied compendium snapshot is invalid")
	}
	compendiumRule := rule.GetCompendiumRule()
	if compendium.GetUnlockEligible() != compendiumRule.GetUnlockEligible() || compendium.GetActivityId() != compendiumRule.GetActivityId() ||
		compendium.GetCatalogVersionAtPlant() != compendiumRule.GetCatalogVersionAtPlant() || compendium.GetDefinitionVersionAtPlant() != compendiumRule.GetDefinitionVersionAtPlant() {
		return errors.New("farm participant: applied compendium snapshot fields are inconsistent")
	}
	stealTemplate := rule.GetStealPlanTemplate()
	if steal.GetPolicyVersion() != stealTemplate.GetPolicyVersion() || steal.GetTotalCapRatioBp() != 2000 ||
		len(steal.GetSlotRatioBp()) != 10 || len(steal.GetSlotQuantity()) != 10 {
		return errors.New("farm participant: applied steal snapshot is invalid")
	}
	var quantityTotal uint64
	expectedSlotQuantity := rule.GetBaseYield() / 50
	for index, ratio := range steal.GetSlotRatioBp() {
		if ratio != 200 || steal.GetSlotQuantity()[index] == 0 || steal.GetSlotQuantity()[index] != expectedSlotQuantity {
			return errors.New("farm participant: applied steal snapshot slot is invalid")
		}
		quantityTotal += steal.GetSlotQuantity()[index]
	}
	if steal.GetTotalCapQuantity() != quantityTotal || quantityTotal != rule.GetBaseYield()/5 {
		return errors.New("farm participant: applied steal snapshot total is invalid")
	}
	if care.GetCareRuleVersion() != rule.GetCarePlanTemplate().GetCareRuleVersion() || len(care.GetIncidents()) > participantMaxCareItems {
		return errors.New("farm participant: applied care snapshot version is invalid")
	}
	wantCare, _, err := buildCareSnapshots(rule.GetCarePlanTemplate(), result.GetCropCycleId(), result.GetPlantAtMs(), rule.GetGrowDurationMs())
	if err != nil || !proto.Equal(wantCare, care) {
		return errors.New("farm participant: applied care snapshot schedule is inconsistent")
	}
	seenCareTypes := make(map[contractspb.CareType]struct{}, len(care.GetIncidents()))
	for _, incident := range care.GetIncidents() {
		if incident == nil || incident.GetOrdinal() != 1 || incident.GetIncidentId() == "" || incident.GetDueAtMs() <= result.GetPlantAtMs() || incident.GetDueAtMs() >= result.GetMatureAtMs() {
			return errors.New("farm participant: applied care snapshot incident is invalid")
		}
		if _, exists := seenCareTypes[incident.GetCareType()]; exists {
			return errors.New("farm participant: applied care snapshot has duplicate incident type")
		}
		seenCareTypes[incident.GetCareType()] = struct{}{}
		careName := ""
		switch incident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER:
			careName = "WATER"
		case contractspb.CareType_CARE_TYPE_REMOVE_PEST:
			careName = "REMOVE_PEST"
		default:
			return errors.New("farm participant: applied care snapshot type is invalid")
		}
		wantID, err := identity.CareIncidentID(result.GetCropCycleId(), careName, uint64(incident.GetOrdinal()))
		if err != nil || incident.GetIncidentId() != wantID {
			return errors.New("farm participant: applied care snapshot incident id is invalid")
		}
	}
	return nil
}

type participantProofReader struct {
	data []byte
	off  int
}

func (r *participantProofReader) lp() ([]byte, error) {
	if r == nil || r.off+4 > len(r.data) {
		return nil, errors.New("farm participant: truncated result proof")
	}
	length := int(binary.BigEndian.Uint32(r.data[r.off:]))
	r.off += 4
	if length < 0 || length > participantMaxProofBytes || r.off+length > len(r.data) {
		return nil, errors.New("farm participant: result proof length is invalid")
	}
	value := r.data[r.off : r.off+length]
	r.off += length
	if length == 0 {
		return nil, nil
	}
	return value, nil
}

func (r *participantProofReader) u32() (uint32, error) {
	if r == nil || r.off+4 > len(r.data) {
		return 0, errors.New("farm participant: truncated result proof")
	}
	value := binary.BigEndian.Uint32(r.data[r.off:])
	r.off += 4
	return value, nil
}

func (r *participantProofReader) u64() (uint64, error) {
	if r == nil || r.off+8 > len(r.data) {
		return 0, errors.New("farm participant: truncated result proof")
	}
	value := binary.BigEndian.Uint64(r.data[r.off:])
	r.off += 8
	return value, nil
}

func (r *participantProofReader) i64() (int64, error) {
	value, err := r.u64()
	return int64(value), err
}

func (r *participantProofReader) remaining() int {
	if r == nil {
		return 0
	}
	return len(r.data) - r.off
}

func writeParticipantLP(writer interface{ Write([]byte) (int, error) }, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func writeParticipantLPBytes(dst *[]byte, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	*dst = append(*dst, length[:]...)
	*dst = append(*dst, value...)
}

func putParticipantUint32(dst *[]byte, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	*dst = append(*dst, encoded[:]...)
}

func putParticipantUint64(dst *[]byte, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	*dst = append(*dst, encoded[:]...)
}

func putParticipantInt64(dst *[]byte, value int64) {
	putParticipantUint64(dst, uint64(value))
}

func writeParticipantUint32(writer interface{ Write([]byte) (int, error) }, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeParticipantUint64(writer interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeParticipantInt64(writer interface{ Write([]byte) (int, error) }, value int64) {
	writeParticipantUint64(writer, uint64(value))
}

// CheckModelRecord is the in-turn physical guard helper.  It is kept on the
// guard so the callback can validate the exact candidate after appending its
// durable ParticipantReceipt, before the actor's CASEpoch call.
func (g ParticipantCapacityGuard) CheckModelRecord(record *farmpb.FarmRecord) error {
	if g.bestEffort {
		return nil
	}
	layout := authoritystore.ModelLayoutV1
	if g.binding != nil {
		layout = g.binding.ModelLayout()
	}
	modelBytes, err := farmAuthorityModelBytesForLayout(layout, record)
	if err != nil {
		return fmt.Errorf("%w: candidate model projection: %v", ErrParticipantCapacityUnavailable, err)
	}
	return g.Check(modelBytes)
}
