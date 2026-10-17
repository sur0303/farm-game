package domain

// OpportunityMask is the non-viewer-personalized candidate mask of the
// FarmOpportunityHeader (design 27 §4.2). It only filters candidates; the
// final ruling always belongs to the FarmActor (receipts, permits, per
// visitor state).
type OpportunityMask uint32

const (
	OpportunityNone OpportunityMask = 0
	// OpportunityHarvestReady: at least one crop cycle is mature and the
	// owner can harvest.
	OpportunityHarvestReady OpportunityMask = 1 << iota
	// OpportunityCareActive: at least one care incident is due and
	// unresolved; some visitor (or the owner) may resolve it.
	OpportunityCareActive
	// OpportunityTheftCandidate: at least one mature cycle still has
	// stealable quota; whether a concrete friend may steal is decided by
	// the FarmActor, not by this mask.
	OpportunityTheftCandidate
)

// OpportunityHeader is the fixed-size derived head persisted inside the Farm
// authority record (design 27 §4.2). It is recomputed from the same
// CandidateState on every opportunity-changing commit and never replaces
// the FarmActor's final ruling.
type OpportunityHeader struct {
	SourceFarmVersion uint64
	DirtyGeneration   uint64
	// NextOpportunityAtMs is the earliest future instant at which a new
	// opportunity may appear (a pending incident due time or a growing
	// crop's maturity). Zero means no future opportunity is known.
	NextOpportunityAtMs int64
	CandidateMask       OpportunityMask
}

// ComputeOpportunity derives the opportunity view of a candidate state at a
// single trusted timestamp. Passing newFarmVersion and dirtyGeneration
// produces the header to persist with the commit; passing the state's own
// version/generation re-derives the current header for reads.
func ComputeOpportunity(state *FarmState, trustedNowMs int64, newFarmVersion, dirtyGeneration uint64) OpportunityHeader {
	header := OpportunityHeader{
		SourceFarmVersion: newFarmVersion,
		DirtyGeneration:   dirtyGeneration,
	}
	next := int64(0)
	consider := func(atMs int64) {
		if atMs <= trustedNowMs {
			return
		}
		if next == 0 || atMs < next {
			next = atMs
		}
	}
	for i := range state.Cycles {
		cycle := &state.Cycles[i]
		if cycle.Ended {
			continue
		}
		if cycle.Mature(trustedNowMs) {
			header.CandidateMask |= OpportunityHarvestReady
			if cycle.RemainingStealable() > 0 {
				header.CandidateMask |= OpportunityTheftCandidate
			}
		} else {
			consider(cycle.MatureAtMs)
		}
		for j := range cycle.Incidents {
			incident := &cycle.Incidents[j]
			if incident.Resolved() {
				continue
			}
			if trustedNowMs >= incident.DueAtMs {
				header.CandidateMask |= OpportunityCareActive
			} else {
				consider(incident.DueAtMs)
			}
		}
	}
	header.NextOpportunityAtMs = next
	return header
}
