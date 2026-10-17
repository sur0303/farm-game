package workflow

import (
	"strconv"
	"sync"
	"time"
)

// ClaimTimingSource exposes the conservative deadline assigned at the one
// shared ClaimDue boundary. Recovery handlers consume their own record's
// entry before the Engine is allowed to start a participant step.
type ClaimTimingSource interface {
	SafeDeadline(*WorkRecord) (time.Time, bool)
}

// ClaimTimingRegistry is shared by every generic handler in one recovery
// composition. Entries are keyed by the leased WorkRecord, not by a work
// family, so a mixed PLANT/HARVEST ClaimDue batch keeps one claim-start
// deadline for every returned record.
type ClaimTimingRegistry struct {
	mu        sync.Mutex
	deadlines map[string]time.Time
}

func NewClaimTimingRegistry() *ClaimTimingRegistry {
	return &ClaimTimingRegistry{deadlines: make(map[string]time.Time)}
}

// RecordClaimed records all items returned by one ClaimDue call immediately
// after that call returns. claimStart is captured immediately before the
// call, which is the conservative local approximation of the authority's
// claim-start monotonic timestamp.
func (r *ClaimTimingRegistry) RecordClaimed(claimStart time.Time, claimed []*ClaimedWork, safetyMargin time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, deadline := range r.deadlines {
		if !deadline.After(claimStart) {
			delete(r.deadlines, key)
		}
	}
	for _, item := range claimed {
		if item == nil || item.Record == nil {
			continue
		}
		r.deadlines[claimTimingKey(item.Record)] = claimStart.Add(item.AcceptedTTL - safetyMargin)
	}
}

func (r *ClaimTimingRegistry) SafeDeadline(record *WorkRecord) (time.Time, bool) {
	if r == nil || record == nil {
		return time.Time{}, false
	}
	key := claimTimingKey(record)
	r.mu.Lock()
	defer r.mu.Unlock()
	deadline, ok := r.deadlines[key]
	if ok {
		delete(r.deadlines, key)
	}
	return deadline, ok
}

func claimTimingKey(record *WorkRecord) string {
	return record.Address.WorkType + "\x00" + record.Address.WorkID + "\x00" + strconv.FormatUint(record.LeaseEpoch, 10)
}
