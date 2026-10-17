package actor

import (
	"crypto/sha256"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/opportunitywakeup"
	"farmgame/internal/storage/authoritystore"
)

// currentFarmOpportunitySchedule converts the post-mutation compact Facts
// into the one mutable wakeup slot owned by this Farm authority. The first
// boundary is intentionally computed from the epoch rather than wall clock:
// an overdue schedule is claimed immediately and lets the worker derive the
// current opportunity before advancing to the next strictly-future boundary.
// This keeps time capture out of the storage seam and remains correct after a
// long worker outage.
func currentFarmOpportunitySchedule(farmID uint64, facts domain.FarmOpportunityFactsV1) (authoritystore.AttachedSchedule, error) {
	scheduleID, err := opportunitywakeup.ScheduleIDForFarm(farmID)
	if err != nil {
		return authoritystore.AttachedSchedule{}, err
	}
	payload, found, err := opportunitywakeup.FromFacts(farmID, facts, 0)
	if err != nil {
		return authoritystore.AttachedSchedule{}, err
	}
	schedule := authoritystore.AttachedSchedule{
		ScheduleID:      scheduleID,
		ScheduleType:    opportunitywakeup.WorkType,
		Generation:      facts.SourceFarmVersion,
		SubjectPlayerID: farmID,
	}
	if !found {
		// A zero due point is a generation-fenced cancellation. Keeping the
		// tombstone in the same Authority Commit prevents an older claimed
		// wakeup from publishing after Harvest removed the last crop.
		schedule.PayloadHash = sha256.Sum256(nil)
		return schedule, nil
	}
	encoded, err := opportunitywakeup.Marshal(payload)
	if err != nil {
		return authoritystore.AttachedSchedule{}, err
	}
	schedule.DueAtMs = payload.DueAtMs
	schedule.Payload = encoded
	schedule.PayloadHash = sha256.Sum256(encoded)
	return schedule, nil
}
