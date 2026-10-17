package friend

// Friend quota authority (contract 33 §5; 33A §5).
//
// Both sides of a friendship consume one exact slot. Reservations created by
// AddFriendWorkflow guarantee the versioned product limit under concurrency;
// Finalize binds the reservation to the actually activated relation_epoch;
// termination releases by (relation_id, captured_active_epoch) and installs
// a RelationReleaseFence so a late Finalize can never resurrect the slot.

import (
	"fmt"

	"farmgame/internal/socialprofile"
)

func defaultMaxFriends() int { return socialprofile.Current().EffectiveMaxFriends }

// ReservationState is the lifecycle of one quota reservation.
type ReservationState uint8

const (
	ReservationReserved  ReservationState = 1
	ReservationFinalized ReservationState = 2
	ReservationReleased  ReservationState = 3
)

// QuotaReservation reserves one slot for one in-flight activation.
type QuotaReservation struct {
	ReservationID       string
	WorkflowID          string
	RelationID          string
	TargetRelationEpoch uint64
	State               ReservationState
	CreatedAtMs         int64
}

// ReleaseFence blocks a late Finalize after termination released the slot
// (33A §5.4 FROZEN_33A).
type ReleaseFence struct {
	FenceID       string
	RelationID    string
	RelationEpoch uint64
	CommittedAtMs int64
}

// QuotaRecord is the per-player quota authority record (33 §5.1).
type QuotaRecord struct {
	PlayerID            uint64
	ActiveRelations     map[string]uint64 // relation_id -> active relation_epoch
	Reservations        map[string]*QuotaReservation
	ReleaseFences       map[string]ReleaseFence // fence_id -> fence
	EffectiveMaxFriends int
	RecordVersion       uint64
	UpdatedAtMs         int64
}

// NewQuotaRecord creates an empty record with the configured limit.
func NewQuotaRecord(playerID uint64, maxFriends int) (*QuotaRecord, error) {
	if playerID == 0 {
		return nil, fmt.Errorf("%w: player id", ErrInvalidCommand)
	}
	if maxFriends <= 0 {
		maxFriends = defaultMaxFriends()
	}
	return &QuotaRecord{
		PlayerID:            playerID,
		ActiveRelations:     make(map[string]uint64),
		Reservations:        make(map[string]*QuotaReservation),
		ReleaseFences:       make(map[string]ReleaseFence),
		EffectiveMaxFriends: maxFriends,
	}, nil
}

// occupancy implements the Reserve invariant (33 §5.1):
// occupancy = active + RESERVED reservations.
func (r *QuotaRecord) occupancy() int {
	reserved := 0
	for _, res := range r.Reservations {
		if res.State == ReservationReserved {
			reserved++
		}
	}
	return len(r.ActiveRelations) + reserved
}

// fenceFor finds a release fence matching relation+epoch.
func (r *QuotaRecord) fenceFor(relationID string, epoch uint64) (ReleaseFence, bool) {
	fenceID, err := QuotaReleaseFenceID(r.PlayerID, relationID, epoch)
	if err != nil {
		return ReleaseFence{}, false
	}
	fence, ok := r.ReleaseFences[fenceID]
	return fence, ok
}

// ReserveFriendSlot creates one reservation if occupancy allows (33 §5.1,
// 33A §5.3). Same reservation_id with identical fields replays; grandfathered
// overflow (limit lowered below current occupancy) blocks new reservations
// but never deletes existing relations.
func (r *QuotaRecord) ReserveFriendSlot(res QuotaReservation, nowMs int64) (Outcome, error) {
	if res.ReservationID == "" || res.WorkflowID == "" || res.RelationID == "" || res.TargetRelationEpoch == 0 {
		return Outcome{}, fmt.Errorf("%w: reservation fields", ErrInvalidCommand)
	}
	if existing, ok := r.Reservations[res.ReservationID]; ok {
		if existing.WorkflowID == res.WorkflowID && existing.RelationID == res.RelationID &&
			existing.TargetRelationEpoch == res.TargetRelationEpoch &&
			existing.State == ReservationReserved {
			return Outcome{Status: StatusSucceeded, CommittedEpoch: res.TargetRelationEpoch}, nil
		}
		return Outcome{}, ErrIdempotentConflict
	}
	if activeEpoch, ok := r.ActiveRelations[res.RelationID]; ok && activeEpoch == res.TargetRelationEpoch {
		return Outcome{Status: StatusNoChange, Reason: ReasonAlreadyFriends, CommittedEpoch: activeEpoch}, nil
	}
	if _, fenced := r.fenceFor(res.RelationID, res.TargetRelationEpoch); fenced {
		return Outcome{Status: StatusNoChange, Reason: ReasonRelationAlreadyTerminated, CommittedEpoch: res.TargetRelationEpoch}, nil
	}
	if r.occupancy() >= r.EffectiveMaxFriends {
		return Outcome{Status: StatusRejected, Reason: ReasonFriendLimitReached}, nil
	}
	res.State = ReservationReserved
	res.CreatedAtMs = nowMs
	r.Reservations[res.ReservationID] = &res
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: res.TargetRelationEpoch}, nil
}

// FinalizeFriendSlot converts a reservation into an active slot. The full
// reservation fields are re-supplied (as the participant payload would), so
// the ReleaseFence check works even when the reservation row was already
// removed by termination (33A §5.4/5.5).
func (r *QuotaRecord) FinalizeFriendSlot(input QuotaReservation, nowMs int64) (Outcome, error) {
	if input.ReservationID == "" || input.RelationID == "" || input.TargetRelationEpoch == 0 {
		return Outcome{}, fmt.Errorf("%w: finalize fields", ErrInvalidCommand)
	}
	if _, fenced := r.fenceFor(input.RelationID, input.TargetRelationEpoch); fenced {
		// Late finalize after termination: deterministic NO_CHANGE, the slot
		// must not be re-occupied (33A §5.4).
		if res, ok := r.Reservations[input.ReservationID]; ok && res.State == ReservationReserved {
			delete(r.Reservations, input.ReservationID)
		}
		r.UpdatedAtMs = nowMs
		return Outcome{Status: StatusNoChange, Reason: ReasonRelationAlreadyTerminated, CommittedEpoch: input.TargetRelationEpoch}, nil
	}
	res, ok := r.Reservations[input.ReservationID]
	if !ok {
		// Idempotent replay after a successful finalize.
		if epoch, active := r.ActiveRelations[input.RelationID]; active && epoch == input.TargetRelationEpoch {
			return Outcome{Status: StatusSucceeded, CommittedEpoch: epoch}, nil
		}
		return Outcome{}, ErrReservationNotFound
	}
	if res.State == ReservationFinalized {
		return Outcome{Status: StatusSucceeded, CommittedEpoch: res.TargetRelationEpoch}, nil
	}
	if res.State == ReservationReleased {
		return Outcome{Status: StatusNoChange, Reason: ReasonRelationAlreadyTerminated, CommittedEpoch: res.TargetRelationEpoch}, nil
	}
	if res.RelationID != input.RelationID || res.TargetRelationEpoch != input.TargetRelationEpoch ||
		res.WorkflowID != input.WorkflowID {
		return Outcome{}, ErrIdempotentConflict
	}
	if epoch, ok := r.ActiveRelations[res.RelationID]; ok {
		if epoch == res.TargetRelationEpoch {
			delete(r.Reservations, input.ReservationID)
			return Outcome{Status: StatusSucceeded, CommittedEpoch: epoch}, nil
		}
		return Outcome{}, ErrIdempotentConflict
	}
	r.ActiveRelations[res.RelationID] = res.TargetRelationEpoch
	delete(r.Reservations, input.ReservationID)
	r.UpdatedAtMs = nowMs
	return Outcome{Status: StatusSucceeded, CommittedEpoch: res.TargetRelationEpoch}, nil
}

// ReleaseFriendReservation compensates a reservation when activation never
// happened (33A §5.6). It is distinct from RemoveActiveRelationIfEpochMatches.
func (r *QuotaRecord) ReleaseFriendReservation(reservationID string, nowMs int64) Outcome {
	res, ok := r.Reservations[reservationID]
	if !ok {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone}
	}
	if res.State == ReservationReserved {
		delete(r.Reservations, reservationID)
		r.UpdatedAtMs = nowMs
		return Outcome{Status: StatusSucceeded, CommittedEpoch: res.TargetRelationEpoch}
	}
	return Outcome{Status: StatusNoChange, Reason: ReasonNone, CommittedEpoch: res.TargetRelationEpoch}
}

// RemoveActiveRelationIfEpochMatches releases the slot of a terminated
// relation and atomically installs the ReleaseFence (33A §5.4). It removes
// the matching active entry OR the still-open reservation, never an entry
// with a different epoch.
func (r *QuotaRecord) RemoveActiveRelationIfEpochMatches(relationID string, capturedActiveEpoch uint64, nowMs int64) Outcome {
	if _, fenced := r.fenceFor(relationID, capturedActiveEpoch); fenced {
		return Outcome{Status: StatusNoChange, Reason: ReasonNone, CommittedEpoch: capturedActiveEpoch}
	}
	fenceID, err := QuotaReleaseFenceID(r.PlayerID, relationID, capturedActiveEpoch)
	if err == nil {
		r.ReleaseFences[fenceID] = ReleaseFence{
			FenceID: fenceID, RelationID: relationID,
			RelationEpoch: capturedActiveEpoch, CommittedAtMs: nowMs,
		}
	}
	if epoch, ok := r.ActiveRelations[relationID]; ok && epoch == capturedActiveEpoch {
		delete(r.ActiveRelations, relationID)
	}
	for id, res := range r.Reservations {
		if res.RelationID == relationID && res.TargetRelationEpoch == capturedActiveEpoch && res.State == ReservationReserved {
			delete(r.Reservations, id)
		}
	}
	r.UpdatedAtMs = nowMs
	// Installing the fence is itself the state change; replay hits the fence
	// above and returns NO_CHANGE.
	return Outcome{Status: StatusSucceeded, CommittedEpoch: capturedActiveEpoch}
}

// ActiveCount reports the number of active relations (read path only).
func (r *QuotaRecord) ActiveCount() int { return len(r.ActiveRelations) }

// HasActiveRelation reports whether relationID occupies a slot at epoch.
func (r *QuotaRecord) HasActiveRelation(relationID string, epoch uint64) bool {
	active, ok := r.ActiveRelations[relationID]
	return ok && active == epoch
}
