package domain

import (
	"errors"
	"math"
)

const opportunityRevisionBoundaryBits = 16

// FarmOpportunityRevisionForSource reserves the low 16 bits for deterministic
// time-derived boundaries. A new Farm authority commit therefore always moves
// the revision beyond every boundary of the previous FarmVersion.
func FarmOpportunityRevisionForSource(sourceFarmVersion uint64) (uint64, error) {
	if sourceFarmVersion == 0 || sourceFarmVersion > math.MaxUint64>>opportunityRevisionBoundaryBits {
		return 0, errors.New("farm opportunity: source FarmVersion cannot be encoded as an opportunity revision")
	}
	return sourceFarmVersion << opportunityRevisionBoundaryBits, nil
}

// FarmOpportunityRevisionAt returns the same monotonic revision to both the
// Scheduler Hint and the exact read path. Distinct reached timestamps count as
// one boundary each; coincident WATER/PEST/MATURE edges share one revision and
// one wakeup.
func FarmOpportunityRevisionAt(facts FarmOpportunityFactsV1, trustedNowMs int64) (uint64, error) {
	if err := validateWakeupFacts(facts); err != nil || trustedNowMs <= 0 {
		if err != nil {
			return 0, err
		}
		return 0, errors.New("farm opportunity: trusted revision time is invalid")
	}
	base, err := FarmOpportunityRevisionForSource(facts.SourceFarmVersion)
	if err != nil {
		return 0, err
	}
	reached := make(map[int64]struct{}, len(facts.Cycles)*3)
	for _, cycle := range facts.Cycles {
		if cycle.RemainingTheftSlots > 0 && cycle.MatureAtMs <= trustedNowMs {
			reached[cycle.MatureAtMs] = struct{}{}
		}
		if cycle.Water.IncidentID != "" && cycle.Water.DueAtMs < cycle.MatureAtMs && cycle.Water.DueAtMs <= trustedNowMs {
			reached[cycle.Water.DueAtMs] = struct{}{}
		}
		if cycle.Pest.IncidentID != "" && cycle.Pest.DueAtMs <= trustedNowMs {
			reached[cycle.Pest.DueAtMs] = struct{}{}
		}
	}
	if len(reached) >= 1<<opportunityRevisionBoundaryBits {
		return 0, errors.New("farm opportunity: too many reached boundaries for one FarmVersion")
	}
	return base + uint64(len(reached)), nil
}
