package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
)

// OpportunityBoundaryMask identifies why a farm needs to be woken at one
// timestamp. It is intentionally farm-global: viewer-specific theft receipts
// are evaluated only after the H5 follows the hint with an exact pull.
type OpportunityBoundaryMask uint32

const (
	OpportunityBoundaryStealable OpportunityBoundaryMask = 1 << iota
	OpportunityBoundaryWater
	OpportunityBoundaryPest
)

// OpportunityBoundary is the next time-derived change worth checking for one
// farm. Digest is a deterministic identity for the facts that reach the same
// boundary; it prevents a worker from repeatedly notifying the same boundary.
type OpportunityBoundary struct {
	AtMs   int64
	Mask   OpportunityBoundaryMask
	Digest [sha256.Size]byte
}

// GlobalFarmOpportunity is deliberately not an authorization result. It says
// only that at least one friend may have something to inspect. Friendship,
// ActionPermit and viewer theft receipts are still checked by the exact path.
type GlobalFarmOpportunity struct {
	Mask             OpportunityBoundaryMask
	ActionableFromMs int64
}

// EvaluateGlobalFarmOpportunityFacts derives the current non-personalized
// opportunity from authority facts using one trusted time.
func EvaluateGlobalFarmOpportunityFacts(facts FarmOpportunityFactsV1, trustedNowMs int64) (GlobalFarmOpportunity, error) {
	if err := validateWakeupFacts(facts); err != nil || trustedNowMs <= 0 {
		if err != nil {
			return GlobalFarmOpportunity{}, err
		}
		return GlobalFarmOpportunity{}, errors.New("farm opportunity: trusted wakeup time is invalid")
	}
	result := GlobalFarmOpportunity{}
	mark := func(mask OpportunityBoundaryMask, atMs int64) {
		result.Mask |= mask
		if result.ActionableFromMs == 0 || atMs < result.ActionableFromMs {
			result.ActionableFromMs = atMs
		}
	}
	for _, cycle := range facts.Cycles {
		mature := cycle.MatureAtMs <= trustedNowMs
		if mature && cycle.RemainingTheftSlots > 0 {
			mark(OpportunityBoundaryStealable, cycle.MatureAtMs)
		}
		if !mature && cycle.Water.IncidentID != "" && cycle.Water.DueAtMs <= trustedNowMs {
			mark(OpportunityBoundaryWater, cycle.Water.DueAtMs)
		}
		if cycle.Pest.IncidentID != "" && cycle.Pest.DueAtMs <= trustedNowMs {
			mark(OpportunityBoundaryPest, cycle.Pest.DueAtMs)
		}
	}
	return result, nil
}

// NextFarmOpportunityBoundary returns the earliest strictly-future boundary.
// It never creates timers and never mutates a crop to MATURE. At the boundary,
// a worker must re-read current authority facts and evaluate them again.
func NextFarmOpportunityBoundary(facts FarmOpportunityFactsV1, afterMs int64) (OpportunityBoundary, bool, error) {
	if err := validateWakeupFacts(facts); err != nil {
		return OpportunityBoundary{}, false, err
	}
	if afterMs < 0 {
		return OpportunityBoundary{}, false, errors.New("farm opportunity: boundary cursor is invalid")
	}
	type candidate struct {
		at    int64
		mask  OpportunityBoundaryMask
		token string
	}
	candidates := make([]candidate, 0, len(facts.Cycles)*3)
	for _, cycle := range facts.Cycles {
		if cycle.RemainingTheftSlots > 0 && cycle.MatureAtMs > afterMs {
			candidates = append(candidates, candidate{at: cycle.MatureAtMs, mask: OpportunityBoundaryStealable,
				token: wakeupToken(cycle.PlotID, cycle.CycleID, "STEAL", "")})
		}
		// WATER is invalid at and after maturity. A due timestamp equal to the
		// maturity boundary therefore does not deserve a separate wakeup bit.
		if cycle.Water.IncidentID != "" && cycle.Water.DueAtMs > afterMs && cycle.Water.DueAtMs < cycle.MatureAtMs {
			candidates = append(candidates, candidate{at: cycle.Water.DueAtMs, mask: OpportunityBoundaryWater,
				token: wakeupToken(cycle.PlotID, cycle.CycleID, "WATER", cycle.Water.IncidentID)})
		}
		if cycle.Pest.IncidentID != "" && cycle.Pest.DueAtMs > afterMs {
			candidates = append(candidates, candidate{at: cycle.Pest.DueAtMs, mask: OpportunityBoundaryPest,
				token: wakeupToken(cycle.PlotID, cycle.CycleID, "PEST", cycle.Pest.IncidentID)})
		}
	}
	if len(candidates) == 0 {
		return OpportunityBoundary{}, false, nil
	}
	earliest := candidates[0].at
	for _, item := range candidates[1:] {
		if item.at < earliest {
			earliest = item.at
		}
	}
	tokens := make([]string, 0, len(candidates))
	boundary := OpportunityBoundary{AtMs: earliest}
	for _, item := range candidates {
		if item.at != earliest {
			continue
		}
		boundary.Mask |= item.mask
		tokens = append(tokens, item.token)
	}
	sort.Strings(tokens)
	boundary.Digest = opportunityBoundaryDigest(facts.SourceFarmVersion, boundary.AtMs, boundary.Mask, tokens)
	return boundary, true, nil
}

func validateWakeupFacts(facts FarmOpportunityFactsV1) error {
	if facts.SchemaVersion != FarmOpportunityFactsSchemaV1 || facts.SourceFarmVersion == 0 || len(facts.Cycles) > MaxOpportunityCycles {
		return errors.New("farm opportunity: wakeup facts are invalid")
	}
	for _, cycle := range facts.Cycles {
		if cycle.PlotID == 0 || cycle.CycleID == "" ||
			len(cycle.TheftVisitorIDs) > MaxTheftVisitorsPerCycle {
			return errors.New("farm opportunity: wakeup cycle is invalid")
		}
		if cycle.RemainingTheftSlots > 0 && cycle.MatureAtMs <= 0 {
			return errors.New("farm opportunity: wakeup theft boundary is invalid")
		}
		if cycle.Water.IncidentID != "" && cycle.Water.DueAtMs <= 0 {
			return errors.New("farm opportunity: wakeup water incident is invalid")
		}
		if cycle.Pest.IncidentID != "" && cycle.Pest.DueAtMs <= 0 {
			return errors.New("farm opportunity: wakeup pest incident is invalid")
		}
	}
	return nil
}

func wakeupToken(plotID uint32, cycleID, kind, incidentID string) string {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, plotID)
	return string(buf) + "\x00" + cycleID + "\x00" + kind + "\x00" + incidentID
}

func opportunityBoundaryDigest(sourceVersion uint64, atMs int64, mask OpportunityBoundaryMask, tokens []string) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("FARM-OPPORTUNITY-BOUNDARY-V1"))
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], sourceVersion)
	_, _ = h.Write(scalar[:])
	binary.BigEndian.PutUint64(scalar[:], uint64(atMs))
	_, _ = h.Write(scalar[:])
	var maskBytes [4]byte
	binary.BigEndian.PutUint32(maskBytes[:], uint32(mask))
	_, _ = h.Write(maskBytes[:])
	for _, token := range tokens {
		binary.BigEndian.PutUint64(scalar[:], uint64(len(token)))
		_, _ = h.Write(scalar[:])
		_, _ = h.Write([]byte(token))
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}
