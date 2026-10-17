package friend

// In-memory adapters of the three friendship authority ports.
//
// Each authority entry is guarded by its own mutex, modeling the Redis
// Primary linearization point of 33A: one Lua commit at a time per
// authority record. The adapters implement the same conditional results as
// the future Lua scripts and never hide races behind artificial latency, so
// contract tests exercise real concurrency semantics.

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
)

// storedPairOutcome is the persisted receipt of one pair mutation.
type storedPairOutcome struct {
	outcome              Outcome
	permit               *ActionPermitSnapshot
	dispatchOrderID      string
	workflowDeadlineAtMs int64
}

type pairEntry struct {
	mu       sync.Mutex
	record   *PairRecord // nil until the first fact exists
	receipts map[string]storedPairOutcome
}

// MemoryPairRepository is the in-memory FRIEND_PAIR adapter.
type MemoryPairRepository struct {
	mu      sync.Mutex
	entries map[string]*pairEntry
}

// NewMemoryPairRepository creates an empty repository.
func NewMemoryPairRepository() *MemoryPairRepository {
	return &MemoryPairRepository{entries: make(map[string]*pairEntry)}
}

func (m *MemoryPairRepository) entryFor(playerA, playerB uint64) (*pairEntry, error) {
	if !ValidPair(playerA, playerB) {
		return nil, ErrInvalidPair
	}
	low, high := CanonicalPair(playerA, playerB)
	pairID, err := RelationIDFor(low, high)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[pairID]
	if !ok {
		entry = &pairEntry{receipts: make(map[string]storedPairOutcome)}
		m.entries[pairID] = entry
	}
	return entry, nil
}

func snapshotOf(record *PairRecord, low, high uint64, pairID string) PairSnapshot {
	if record == nil {
		return PairSnapshot{PairID: pairID, LowPlayerID: low, HighPlayerID: high}
	}
	snap := PairSnapshot{
		PairID:        record.PairID,
		LowPlayerID:   record.LowPlayerID,
		HighPlayerID:  record.HighPlayerID,
		Relation:      record.Relation,
		LowBlocksHigh: record.LowBlocksHigh,
		HighBlocksLow: record.HighBlocksLow,
		Barrier:       record.Barrier,
		RecordVersion: record.RecordVersion,
	}
	if record.PendingTermination != nil {
		order := *record.PendingTermination
		snap.PendingTermination = &order
	}
	return snap
}

// GetPair implements PairRepository.
func (m *MemoryPairRepository) GetPair(ctx context.Context, playerA, playerB uint64) (PairSnapshot, error) {
	entry, err := m.entryFor(playerA, playerB)
	if err != nil {
		return PairSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return PairSnapshot{}, err
	}
	low, high := CanonicalPair(playerA, playerB)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return snapshotOf(entry.record, low, high, pairIDMust(low, high)), nil
}

func (m *MemoryPairRepository) GetPairByRelationID(ctx context.Context, relationID string) (PairSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return PairSnapshot{}, err
	}
	if relationID == "" {
		return PairSnapshot{}, ErrInvalidPair
	}
	m.mu.Lock()
	entry := m.entries[relationID]
	m.mu.Unlock()
	if entry == nil {
		return PairSnapshot{}, ErrNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.record == nil || entry.record.PairID != relationID {
		return PairSnapshot{}, ErrNotFound
	}
	return snapshotOf(entry.record, entry.record.LowPlayerID, entry.record.HighPlayerID, relationID), nil
}

func pairIDMust(low, high uint64) string {
	id, err := RelationIDFor(low, high)
	if err != nil {
		panic(err)
	}
	return id
}

// loadOrCreate returns the mutable record, creating the minimal record only
// when allowCreate is set (33A-14: pure NO_CHANGE on a never-existing pair
// must not create an empty main record).
func (e *pairEntry) loadOrCreate(low, high uint64, allowCreate bool) (*PairRecord, error) {
	if e.record != nil {
		return e.record, nil
	}
	if !allowCreate {
		return nil, nil
	}
	record, err := NewPairRecord(low, high)
	if err != nil {
		return nil, err
	}
	e.record = record
	return record, nil
}

// checkReceipt implements the identity/payload replay rule. The second
// return value reports a replay hit.
func (e *pairEntry) checkReceipt(identity CommitIdentity, payloadHash []byte) (storedPairOutcome, bool, error) {
	if !identity.Valid() {
		return storedPairOutcome{}, false, fmt.Errorf("%w: commit identity", ErrInvalidCommand)
	}
	stored, ok := e.receipts[identity.Key()]
	if !ok {
		return storedPairOutcome{}, false, nil
	}
	if !payloadEqual(stored.outcome.PayloadHash, payloadHash) {
		return storedPairOutcome{}, false, ErrIdempotentConflict
	}
	return stored, true, nil
}

func (e *pairEntry) storeReceipt(identity CommitIdentity, payloadHash []byte, outcome Outcome, permit *ActionPermitSnapshot, dispatchOrderID string, workflowDeadlineAtMs ...int64) {
	outcome.Identity = identity
	outcome.PayloadHash = append([]byte(nil), payloadHash...)
	var deadline int64
	if len(workflowDeadlineAtMs) > 0 {
		deadline = workflowDeadlineAtMs[0]
	}
	e.receipts[identity.Key()] = storedPairOutcome{outcome: outcome, permit: permit, dispatchOrderID: dispatchOrderID,
		workflowDeadlineAtMs: deadline}
}

func (m *MemoryPairRepository) mutatePair(ctx context.Context, cmd PairMutationCommand, apply func(record *PairRecord) (Outcome, error)) (Outcome, error) {
	entry, err := m.entryFor(cmd.CallerPlayerID, cmd.PeerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if stored, replay, err := entry.checkReceipt(cmd.Identity, cmd.PayloadHash); err != nil {
		return Outcome{}, err
	} else if replay {
		return stored.outcome, nil
	}
	low, high := CanonicalPair(cmd.CallerPlayerID, cmd.PeerPlayerID)
	record, err := entry.loadOrCreate(low, high, true)
	if err != nil {
		return Outcome{}, err
	}
	outcome, err := apply(record)
	if err != nil {
		return Outcome{}, err
	}
	if outcome.Status == StatusSucceeded {
		record.RecordVersion++
		record.UpdatedAtMs = cmd.NowMs
	}
	entry.storeReceipt(cmd.Identity, cmd.PayloadHash, outcome, nil, "")
	return outcome, nil
}

// DeleteFriend implements PairRepository.
func (m *MemoryPairRepository) DeleteFriend(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return m.mutatePair(ctx, cmd, func(record *PairRecord) (Outcome, error) {
		outcome, _, err := record.ApplyDelete(cmd.CallerPlayerID, cmd.Identity.OperationID, cmd.NowMs)
		return outcome, err
	})
}

// BlockPlayer implements PairRepository.
func (m *MemoryPairRepository) BlockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return m.mutatePair(ctx, cmd, func(record *PairRecord) (Outcome, error) {
		outcome, _, err := record.ApplyBlock(cmd.CallerPlayerID, cmd.PeerPlayerID, cmd.Identity.OperationID, cmd.NowMs)
		return outcome, err
	})
}

// UnblockPlayer implements PairRepository.
func (m *MemoryPairRepository) UnblockPlayer(ctx context.Context, cmd PairMutationCommand) (Outcome, error) {
	return m.mutatePair(ctx, cmd, func(record *PairRecord) (Outcome, error) {
		outcome, err := record.ApplyUnblock(cmd.CallerPlayerID, cmd.PeerPlayerID, cmd.NowMs)
		return outcome, err
	})
}

// ValidatePairForAdd implements PairRepository.
func (m *MemoryPairRepository) ValidatePairForAdd(ctx context.Context, inviter, redeemer uint64) (PairValidationSnapshot, error) {
	entry, err := m.entryFor(inviter, redeemer)
	if err != nil {
		return PairValidationSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return PairValidationSnapshot{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	low, high := CanonicalPair(inviter, redeemer)
	record, err := entry.loadOrCreate(low, high, false)
	if err != nil {
		return PairValidationSnapshot{}, err
	}
	if record == nil {
		pairID := pairIDMust(low, high)
		return PairValidationSnapshot{
			PairID:              pairID,
			RelationID:          pairID,
			TargetRelationEpoch: 1,
			BarrierState:        BarrierNone,
		}, nil
	}
	return record.ValidateForAdd(inviter, redeemer)
}

// ActivateFriendRelation implements PairRepository.
func (m *MemoryPairRepository) ActivateFriendRelation(ctx context.Context, cmd ActivateCommand, identity CommitIdentity, payloadHash []byte) (Outcome, error) {
	entry, err := m.entryFor(cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if stored, replay, err := entry.checkReceipt(identity, payloadHash); err != nil {
		return Outcome{}, err
	} else if replay {
		return stored.outcome, nil
	}
	low, high := CanonicalPair(cmd.InviterPlayerID, cmd.RedeemerPlayerID)
	record, err := entry.loadOrCreate(low, high, true)
	if err != nil {
		return Outcome{}, err
	}
	outcome, err := record.ApplyActivate(cmd)
	if err != nil {
		return Outcome{}, err
	}
	if outcome.Status == StatusSucceeded {
		record.RecordVersion++
	}
	entry.storeReceipt(identity, payloadHash, outcome, nil, "")
	return outcome, nil
}

// CommitReactivationGenerationFloors implements PairRepository.
func (m *MemoryPairRepository) CommitReactivationGenerationFloors(ctx context.Context, playerA, playerB uint64, expectedDeletedEpoch, barrierRevision, lowFloor, highFloor uint64, nowMs int64) (Outcome, error) {
	entry, err := m.entryFor(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.record == nil {
		return Outcome{Status: StatusNoChange, Reason: ReasonNotFriends}, nil
	}
	outcome := entry.record.CommitReactivationGenerationFloors(expectedDeletedEpoch, barrierRevision, lowFloor, highFloor, nowMs)
	if outcome.Status == StatusSucceeded {
		entry.record.RecordVersion++
	}
	return outcome, nil
}

// CompleteTerminationOrder implements PairRepository.
func (m *MemoryPairRepository) CompleteTerminationOrder(ctx context.Context, playerA, playerB uint64, orderID string, nowMs int64) (Outcome, error) {
	entry, err := m.entryFor(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	order := entry.recordPendingOrder()
	if order == nil || order.OrderID != orderID {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}, nil
	}
	if order.Completed {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}, nil
	}
	if !order.floorsReady() || !order.QuotaReleasedLow || !order.QuotaReleasedHigh ||
		entry.record.Relation.Status != RelationStatusDeleted || entry.record.Barrier.State != BarrierReady ||
		entry.record.Relation.RelationEpoch != order.CapturedActiveEpoch+1 ||
		entry.record.Barrier.TerminatedRelationEpoch != order.CapturedActiveEpoch ||
		entry.record.Barrier.BarrierRevision != order.BarrierRevision {
		return Outcome{Status: StatusNoChange, Reason: ReasonRelationCleanupPending}, nil
	}
	order.Completed = true
	entry.record.RecordVersion++
	entry.record.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded}, nil
}

// MarkTerminationQuotaReleased records one side of the termination cleanup
// barrier.  It is intentionally private to the service orchestration rather
// than a new public SO0 port: CompleteTerminationOrder is the only frozen
// completion method, and it now verifies both progress flags atomically.
func (m *MemoryPairRepository) MarkTerminationQuotaReleased(ctx context.Context, playerA, playerB uint64,
	orderID string, playerID uint64, nowMs int64) (Outcome, error) {
	entry, err := m.entryFor(playerA, playerB)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	order := entry.recordPendingOrder()
	if order == nil || order.OrderID != orderID || order.Completed {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}, nil
	}
	var changed bool
	switch playerID {
	case entry.record.LowPlayerID:
		changed = !order.QuotaReleasedLow
		order.QuotaReleasedLow = true
	case entry.record.HighPlayerID:
		changed = !order.QuotaReleasedHigh
		order.QuotaReleasedHigh = true
	default:
		return Outcome{}, fmt.Errorf("%w: termination player", ErrInvalidCommand)
	}
	if !changed {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}, nil
	}
	entry.record.RecordVersion++
	entry.record.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: order.CapturedActiveEpoch + 1}, nil
}

func (e *pairEntry) recordPendingOrder() *TerminationOrder {
	if e.record == nil {
		return nil
	}
	return e.record.PendingTermination
}

// GetBarrierProof implements PairRepository.
func (m *MemoryPairRepository) GetBarrierProof(ctx context.Context, playerA, playerB uint64) (BarrierProof, error) {
	entry, err := m.entryFor(playerA, playerB)
	if err != nil {
		return BarrierProof{}, err
	}
	if err := ctx.Err(); err != nil {
		return BarrierProof{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.record == nil {
		return BarrierProof{}, ErrBarrierProofMismatch
	}
	return entry.record.GetBarrierProof()
}

// CommitFriendActionAdmission implements PairRepository (33 §8.3).
func (m *MemoryPairRepository) CommitFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error) {
	entry, err := m.entryFor(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return ActionAdmissionOutcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if stored, replay, err := entry.checkReceipt(cmd.Identity, cmd.PayloadHash); err != nil {
		return ActionAdmissionOutcome{}, err
	} else if replay {
		return ActionAdmissionOutcome{
			Outcome:              stored.outcome,
			Permit:               cloneActionPermit(stored.permit),
			DispatchOrderID:      stored.dispatchOrderID,
			WorkflowDeadlineAtMs: stored.workflowDeadlineAtMs,
		}, nil
	}
	low, high := CanonicalPair(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	record, err := entry.loadOrCreate(low, high, false)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	if record == nil || !record.CanIssuePermit(cmd.VisitorPlayerID, cmd.OwnerPlayerID) {
		outcome := Outcome{Status: StatusRejected, Reason: ReasonVisitNotPermitted}
		entry.storeReceipt(cmd.Identity, cmd.PayloadHash, outcome, nil, "")
		return ActionAdmissionOutcome{Outcome: outcome}, nil
	}
	if cmd.TargetFarmID != cmd.OwnerPlayerID ||
		(cmd.Scope != ActionScopeCare && cmd.Scope != ActionScopeTheft) ||
		len(cmd.TargetBindingHash) == 0 {
		return ActionAdmissionOutcome{}, fmt.Errorf("%w: admission target", ErrInvalidCommand)
	}
	epoch := record.Relation.RelationEpoch
	permitID, err := stableID("farm-action-permit-id-v1", record.PairID, itoa(cmd.VisitorPlayerID), cmd.Identity.OperationID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	ttl := cmd.PermitTTLMs
	if ttl <= 0 {
		ttl = DefaultActionPermitTTL.Milliseconds()
	}
	workflowTTL := cmd.WorkflowTTLMs
	if workflowTTL <= 0 {
		workflowTTL = DefaultActionWorkflowTTLMs
	}
	if workflowTTL <= ttl {
		return ActionAdmissionOutcome{}, fmt.Errorf("%w: workflow TTL must exceed permit TTL", ErrInvalidCommand)
	}
	workflowDeadlineAtMs := cmd.NowMs + workflowTTL
	permit := ActionPermitSnapshot{
		PermitID:                       permitID,
		RelationID:                     record.Relation.RelationID,
		RelationEpoch:                  epoch,
		VisitorPlayerID:                cmd.VisitorPlayerID,
		OwnerPlayerID:                  cmd.OwnerPlayerID,
		TargetFarmID:                   cmd.TargetFarmID,
		OperationID:                    cmd.Identity.OperationID,
		Scope:                          cmd.Scope,
		PayloadHash:                    append([]byte(nil), cmd.PayloadHash...),
		TargetBindingHash:              append([]byte(nil), cmd.TargetBindingHash...),
		IssuedAtAuthorityMs:            cmd.NowMs,
		ExpireAtAuthorityMs:            cmd.NowMs + ttl,
		ExecutionAcceptedAtAuthorityMs: cmd.NowMs,
	}
	operationDigest, err := workOperationDigest(cmd, record.PairID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	dispatchOrderID, err := ActionDispatchOrderID(record.PairID, operationDigest, epoch)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	outcome := Outcome{Status: StatusSucceeded, CommittedEpoch: epoch, CommittedAtMs: cmd.NowMs}
	record.RecordVersion++
	record.UpdatedAtMs = cmd.NowMs
	entry.storeReceipt(cmd.Identity, cmd.PayloadHash, outcome, &permit, dispatchOrderID, workflowDeadlineAtMs)
	return ActionAdmissionOutcome{Outcome: outcome, Permit: permit, DispatchOrderID: dispatchOrderID,
		WorkflowDeadlineAtMs: workflowDeadlineAtMs}, nil
}

// GetFriendActionAdmission implements PairRepository.
func (m *MemoryPairRepository) GetFriendActionAdmission(ctx context.Context, cmd ActionAdmissionCommand) (ActionAdmissionOutcome, error) {
	entry, err := m.entryFor(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return ActionAdmissionOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return ActionAdmissionOutcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	stored, ok := entry.receipts[cmd.Identity.Key()]
	if !ok || stored.permit == nil {
		return ActionAdmissionOutcome{}, ErrOutcomeNotFound
	}
	if !payloadEqual(stored.outcome.PayloadHash, cmd.PayloadHash) {
		return ActionAdmissionOutcome{}, ErrIdempotentConflict
	}
	return ActionAdmissionOutcome{
		Outcome:              stored.outcome,
		Permit:               cloneActionPermit(stored.permit),
		DispatchOrderID:      stored.dispatchOrderID,
		WorkflowDeadlineAtMs: stored.workflowDeadlineAtMs,
	}, nil
}

func cloneActionPermit(permit *ActionPermitSnapshot) ActionPermitSnapshot {
	if permit == nil {
		return ActionPermitSnapshot{}
	}
	clone := *permit
	clone.PayloadHash = append([]byte(nil), permit.PayloadHash...)
	clone.TargetBindingHash = append([]byte(nil), permit.TargetBindingHash...)
	return clone
}

func workOperationDigest(cmd ActionAdmissionCommand, pairID string) (string, error) {
	digest, err := friendOperationLocatorDigest(cmd.Identity, FriendPairAuthority, pairID)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// --- Quota ---

type quotaEntry struct {
	mu       sync.Mutex
	record   *QuotaRecord
	receipts map[string]Outcome
}

// MemoryQuotaRepository is the in-memory FRIEND_QUOTA adapter.
type MemoryQuotaRepository struct {
	mu         sync.Mutex
	entries    map[uint64]*quotaEntry
	maxFriends int
}

// NewMemoryQuotaRepository creates an empty repository with the configured
// per-player hard limit (33 §5.1).
func NewMemoryQuotaRepository(maxFriends int) *MemoryQuotaRepository {
	if maxFriends <= 0 {
		maxFriends = defaultMaxFriends()
	}
	return &MemoryQuotaRepository{entries: make(map[uint64]*quotaEntry), maxFriends: maxFriends}
}

func (m *MemoryQuotaRepository) entryFor(playerID uint64) (*quotaEntry, error) {
	if playerID == 0 {
		return nil, fmt.Errorf("%w: player id", ErrInvalidCommand)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[playerID]
	if !ok {
		entry = &quotaEntry{receipts: make(map[string]Outcome)}
		m.entries[playerID] = entry
	}
	return entry, nil
}

func (e *quotaEntry) load(playerID uint64, maxFriends int) (*QuotaRecord, error) {
	if e.record != nil {
		return e.record, nil
	}
	record, err := NewQuotaRecord(playerID, maxFriends)
	if err != nil {
		return nil, err
	}
	e.record = record
	return record, nil
}

func (e *quotaEntry) checkReceipt(identity CommitIdentity, payloadHash []byte) (Outcome, bool, error) {
	if !identity.Valid() {
		return Outcome{}, false, fmt.Errorf("%w: commit identity", ErrInvalidCommand)
	}
	stored, ok := e.receipts[identity.Key()]
	if !ok {
		return Outcome{}, false, nil
	}
	if !payloadEqual(stored.PayloadHash, payloadHash) {
		return Outcome{}, false, ErrIdempotentConflict
	}
	return stored, true, nil
}

func (e *quotaEntry) storeReceipt(identity CommitIdentity, payloadHash []byte, outcome Outcome) {
	outcome.Identity = identity
	outcome.PayloadHash = append([]byte(nil), payloadHash...)
	e.receipts[identity.Key()] = outcome
}

// GetQuota implements QuotaRepository.
func (m *MemoryQuotaRepository) GetQuota(ctx context.Context, playerID uint64) (QuotaSnapshot, error) {
	entry, err := m.entryFor(playerID)
	if err != nil {
		return QuotaSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return QuotaSnapshot{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	snap := QuotaSnapshot{PlayerID: playerID, ActiveRelations: map[string]uint64{}, EffectiveMaxFriends: m.maxFriends}
	if entry.record == nil {
		return snap, nil
	}
	for relationID, epoch := range entry.record.ActiveRelations {
		snap.ActiveRelations[relationID] = epoch
	}
	for _, res := range entry.record.Reservations {
		if res.State == ReservationReserved {
			snap.OpenReservations++
		}
	}
	snap.EffectiveMaxFriends = entry.record.EffectiveMaxFriends
	snap.RecordVersion = entry.record.RecordVersion
	return snap, nil
}

func (m *MemoryQuotaRepository) mutateQuota(ctx context.Context, playerID uint64, identity CommitIdentity, payloadHash []byte, nowMs int64, apply func(record *QuotaRecord) (Outcome, error)) (Outcome, error) {
	entry, err := m.entryFor(playerID)
	if err != nil {
		return Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if stored, replay, err := entry.checkReceipt(identity, payloadHash); err != nil {
		return Outcome{}, err
	} else if replay {
		return stored, nil
	}
	record, err := entry.load(playerID, m.maxFriends)
	if err != nil {
		return Outcome{}, err
	}
	before := quotaBusinessStateOf(record)
	outcome, err := apply(record)
	if err != nil {
		return Outcome{}, err
	}
	if !reflect.DeepEqual(before, quotaBusinessStateOf(record)) {
		record.RecordVersion++
		record.UpdatedAtMs = nowMs
	}
	entry.storeReceipt(identity, payloadHash, outcome)
	return outcome, nil
}

// quotaBusinessStateOf excludes the physical version/timestamp fields.  A
// successful idempotent Reserve/Finalize replay can legitimately return
// SUCCEEDED without changing the authority record; the memory adapter must
// therefore use the same record-version semantics as the Redis Lua adapter.
type quotaBusinessState struct {
	playerID            uint64
	active              map[string]uint64
	reservations        map[string]QuotaReservation
	fences              map[string]ReleaseFence
	effectiveMaxFriends int
}

func quotaBusinessStateOf(record *QuotaRecord) quotaBusinessState {
	state := quotaBusinessState{playerID: record.PlayerID, active: make(map[string]uint64, len(record.ActiveRelations)),
		reservations: make(map[string]QuotaReservation, len(record.Reservations)),
		fences:       make(map[string]ReleaseFence, len(record.ReleaseFences)), effectiveMaxFriends: record.EffectiveMaxFriends}
	for relationID, epoch := range record.ActiveRelations {
		state.active[relationID] = epoch
	}
	for reservationID, reservation := range record.Reservations {
		if reservation != nil {
			state.reservations[reservationID] = *reservation
		}
	}
	for fenceID, fence := range record.ReleaseFences {
		state.fences[fenceID] = fence
	}
	return state
}

// ReserveFriendSlot implements QuotaRepository.
func (m *MemoryQuotaRepository) ReserveFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	return m.mutateQuota(ctx, playerID, identity, payloadHash, nowMs, func(record *QuotaRecord) (Outcome, error) {
		return record.ReserveFriendSlot(reservation, nowMs)
	})
}

// FinalizeFriendSlot implements QuotaRepository.
func (m *MemoryQuotaRepository) FinalizeFriendSlot(ctx context.Context, playerID uint64, reservation QuotaReservation, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	return m.mutateQuota(ctx, playerID, identity, payloadHash, nowMs, func(record *QuotaRecord) (Outcome, error) {
		return record.FinalizeFriendSlot(reservation, nowMs)
	})
}

// ReleaseFriendReservation implements QuotaRepository.
func (m *MemoryQuotaRepository) ReleaseFriendReservation(ctx context.Context, playerID uint64, reservationID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	return m.mutateQuota(ctx, playerID, identity, payloadHash, nowMs, func(record *QuotaRecord) (Outcome, error) {
		return record.ReleaseFriendReservation(reservationID, nowMs), nil
	})
}

// RemoveActiveRelationIfEpochMatches implements QuotaRepository.
func (m *MemoryQuotaRepository) RemoveActiveRelationIfEpochMatches(ctx context.Context, playerID uint64, relationID string, capturedActiveEpoch uint64, identity CommitIdentity, payloadHash []byte, nowMs int64) (Outcome, error) {
	return m.mutateQuota(ctx, playerID, identity, payloadHash, nowMs, func(record *QuotaRecord) (Outcome, error) {
		return record.RemoveActiveRelationIfEpochMatches(relationID, capturedActiveEpoch, nowMs), nil
	})
}

// --- Share issuer ---

// storedShareOutcome persists the terminal outcome plus the token view at
// commit time, so replays return the original link (33 §4.5).
type storedShareOutcome struct {
	outcome           Outcome
	token             ShareToken
	rotateReceipt     RotateReceipt
	validationReceipt ValidationReceipt
}

type shareEntry struct {
	mu       sync.Mutex
	record   *ShareIssuerRecord
	receipts map[string]storedShareOutcome
}

// MemoryShareIssuerRepository is the in-memory FRIEND_SHARE_ISSUER adapter.
type MemoryShareIssuerRepository struct {
	mu       sync.Mutex
	entries  map[uint64]*shareEntry
	tokenTTL int64
}

// NewMemoryShareIssuerRepository creates an empty repository.
func NewMemoryShareIssuerRepository(tokenTTLMs int64) *MemoryShareIssuerRepository {
	if tokenTTLMs <= 0 {
		tokenTTLMs = defaultShareTokenTTLMs()
	}
	return &MemoryShareIssuerRepository{entries: make(map[uint64]*shareEntry), tokenTTL: tokenTTLMs}
}

func (m *MemoryShareIssuerRepository) entryFor(inviter uint64) (*shareEntry, error) {
	if inviter == 0 {
		return nil, fmt.Errorf("%w: inviter id", ErrInvalidCommand)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[inviter]
	if !ok {
		entry = &shareEntry{receipts: make(map[string]storedShareOutcome)}
		m.entries[inviter] = entry
	}
	return entry, nil
}

func (e *shareEntry) load(inviter uint64) (*ShareIssuerRecord, error) {
	if e.record != nil {
		return e.record, nil
	}
	record, err := NewShareIssuerRecord(inviter)
	if err != nil {
		return nil, err
	}
	e.record = record
	return record, nil
}

func (e *shareEntry) checkReceipt(identity CommitIdentity, payloadHash []byte) (storedShareOutcome, bool, error) {
	if !identity.Valid() {
		return storedShareOutcome{}, false, fmt.Errorf("%w: commit identity", ErrInvalidCommand)
	}
	stored, ok := e.receipts[identity.Key()]
	if !ok {
		return storedShareOutcome{}, false, nil
	}
	if !payloadEqual(stored.outcome.PayloadHash, payloadHash) {
		return storedShareOutcome{}, false, ErrIdempotentConflict
	}
	return stored, true, nil
}

func (e *shareEntry) storeReceipt(identity CommitIdentity, payloadHash []byte, stored storedShareOutcome) {
	stored.outcome.Identity = identity
	stored.outcome.PayloadHash = append([]byte(nil), payloadHash...)
	e.receipts[identity.Key()] = stored
}

// GetIssuer implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) GetIssuer(ctx context.Context, inviter uint64) (ShareIssuerSnapshot, error) {
	entry, err := m.entryFor(inviter)
	if err != nil {
		return ShareIssuerSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return ShareIssuerSnapshot{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	snap := ShareIssuerSnapshot{InviterPlayerID: inviter, RotateReceipts: map[string]RotateReceipt{}}
	if entry.record == nil {
		return snap, nil
	}
	snap.Current = entry.record.Current
	for pairID, receipt := range entry.record.RotateReceipts {
		snap.RotateReceipts[pairID] = receipt
	}
	snap.RecordVersion = entry.record.RecordVersion
	return snap, nil
}

// GetGenerationSnapshot implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) GetGenerationSnapshot(ctx context.Context, inviter uint64) (uint64, error) {
	entry, err := m.entryFor(inviter)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.record == nil {
		return 0, nil
	}
	return entry.record.GenerationSnapshot(), nil
}

func (m *MemoryShareIssuerRepository) mutateShare(ctx context.Context, inviter uint64, identity CommitIdentity, payloadHash []byte, nowMs int64, apply func(record *ShareIssuerRecord) (storedShareOutcome, error)) (storedShareOutcome, error) {
	entry, err := m.entryFor(inviter)
	if err != nil {
		return storedShareOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return storedShareOutcome{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if stored, replay, err := entry.checkReceipt(identity, payloadHash); err != nil {
		return storedShareOutcome{}, err
	} else if replay {
		return stored, nil
	}
	record, err := entry.load(inviter)
	if err != nil {
		return storedShareOutcome{}, err
	}
	stored, err := apply(record)
	if err != nil {
		return storedShareOutcome{}, err
	}
	if stored.rotateReceipt.ReceiptID != "" {
		// Keep the in-memory oracle aligned with Redis: a rotate proof carries
		// the exact participant/public operation and payload binding that was
		// persisted in the outer authority receipt.
		stored.rotateReceipt.OperationID = identity.OperationID
		if stored.rotateReceipt.OperationID == "" {
			stored.rotateReceipt.OperationID = identity.ParticipantInvocationID
		}
		stored.rotateReceipt.PayloadHash = append([]byte(nil), payloadHash...)
		record.RotateReceipts[stored.rotateReceipt.PairID] = stored.rotateReceipt
	}
	if stored.outcome.Status == StatusSucceeded {
		record.RecordVersion++
		record.UpdatedAtMs = nowMs
	}
	entry.storeReceipt(identity, payloadHash, stored)
	return stored, nil
}

// GenerateShareToken implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) GenerateShareToken(ctx context.Context, inviter uint64, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	stored, err := m.mutateShare(ctx, inviter, identity, payloadHash, nowMs, func(record *ShareIssuerRecord) (storedShareOutcome, error) {
		token, outcome, err := record.GenerateShareToken(newTokenID, m.tokenTTL, nowMs)
		return storedShareOutcome{outcome: outcome, token: token}, err
	})
	return stored.token, stored.outcome, err
}

// RotateShareToken implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) RotateShareToken(ctx context.Context, inviter uint64, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	stored, err := m.mutateShare(ctx, inviter, identity, payloadHash, nowMs, func(record *ShareIssuerRecord) (storedShareOutcome, error) {
		token, outcome, err := record.RotateShareToken(newTokenID, m.tokenTTL, nowMs)
		return storedShareOutcome{outcome: outcome, token: token}, err
	})
	return stored.token, stored.outcome, err
}

// RotateForPairReactivation implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) RotateForPairReactivation(ctx context.Context, inviter uint64, proof BarrierProof, newTokenID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, RotateReceipt, Outcome, error) {
	stored, err := m.mutateShare(ctx, inviter, identity, payloadHash, nowMs, func(record *ShareIssuerRecord) (storedShareOutcome, error) {
		token, receipt, outcome, err := record.RotateForPairReactivation(proof, newTokenID, m.tokenTTL, nowMs)
		return storedShareOutcome{outcome: outcome, token: token, rotateReceipt: receipt}, err
	})
	return stored.token, stored.rotateReceipt, stored.outcome, err
}

// RevokeShareToken implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) RevokeShareToken(ctx context.Context, inviter uint64, identity CommitIdentity, payloadHash []byte, nowMs int64) (ShareToken, Outcome, error) {
	stored, err := m.mutateShare(ctx, inviter, identity, payloadHash, nowMs, func(record *ShareIssuerRecord) (storedShareOutcome, error) {
		outcome := record.RevokeShareToken(nowMs)
		return storedShareOutcome{outcome: outcome, token: record.Current}, nil
	})
	return stored.token, stored.outcome, err
}

// AcceptShareTokenAdmission implements ShareIssuerRepository.
func (m *MemoryShareIssuerRepository) AcceptShareTokenAdmission(ctx context.Context, inviter uint64, tokenID string, tokenGeneration uint64, redeemer uint64, workflowID string, identity CommitIdentity, payloadHash []byte, nowMs int64) (ValidationReceipt, Outcome, error) {
	var receipt ValidationReceipt
	stored, err := m.mutateShare(ctx, inviter, identity, payloadHash, nowMs, func(record *ShareIssuerRecord) (storedShareOutcome, error) {
		var outcome Outcome
		var err error
		receipt, outcome, err = record.AcceptShareTokenAdmission(tokenID, tokenGeneration, SharePurposeAddFriend, redeemer, workflowID, nowMs)
		return storedShareOutcome{outcome: outcome, validationReceipt: receipt}, err
	})
	if err == nil {
		receipt = stored.validationReceipt
	}
	return receipt, stored.outcome, err
}
