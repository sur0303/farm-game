package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

const (
	FarmOpportunityHeaderSchemaV2   uint32 = 2
	FarmOpportunityFactsSchemaV1    uint32 = 1
	MaxOpportunityCycles                   = 24
	MaxTheftVisitorsPerCycle               = 10
	FarmOpportunityHeaderModelField        = "opportunity_header"
	FarmOpportunityFactsModelField         = "opportunity_facts"
)

var (
	headerV2Magic = [4]byte{'F', 'O', 'H', '2'}
	factsV1Magic  = [4]byte{'F', 'O', 'F', '1'}
)

// FarmOpportunityHeaderV2 is a small, non-personalized authority read head.
// It persists only fact timestamps; whether an opportunity is active is
// derived by comparing them with one trusted time captured by the caller.
type FarmOpportunityHeaderV2 struct {
	SchemaVersion       uint32
	SourceFarmVersion   uint64
	NextStealableAtMs   int64
	NextWaterAtMs       int64
	NextPestAtMs        int64
	NextOpportunityAtMs int64
}

// OpportunityIncidentFactV1 describes the one unresolved global incident of
// a care type on a crop cycle. Empty IncidentID means no such opportunity.
type OpportunityIncidentFactV1 struct {
	IncidentID string
	DueAtMs    int64
}

// FarmOpportunityCycleFactV1 is the bounded viewer-exact subset required by
// friend opportunity queries. It intentionally excludes full crop config and
// presentation data.
type FarmOpportunityCycleFactV1 struct {
	PlotID              uint32
	CycleID             string
	MatureAtMs          int64
	RemainingTheftSlots uint32
	Water               OpportunityIncidentFactV1
	Pest                OpportunityIncidentFactV1
	TheftVisitorIDs     []uint64
}

// FarmOpportunityFactsV1 is committed beside the Farm record in the same
// authority CAS. It is not a cache and never replaces FarmActor adjudication.
type FarmOpportunityFactsV1 struct {
	SchemaVersion     uint32
	SourceFarmVersion uint64
	Cycles            []FarmOpportunityCycleFactV1
}

type ViewerOpportunityMask uint32

const ViewerOpportunityNone ViewerOpportunityMask = 0

const (
	ViewerCanSteal ViewerOpportunityMask = 1 << iota
	ViewerCanWater
	ViewerCanRemovePest
)

type EligibleTheftTarget struct {
	PlotID  uint32
	CycleID string
}

type EvaluatedFarmOpportunity struct {
	Mask                 ViewerOpportunityMask
	LatestActionableAtMs int64
	EligibleTheftTargets []EligibleTheftTarget
}

// EvaluateFarmOpportunityFacts computes the viewer-specific exact read answer
// from the bounded authority facts. FriendRelation/ActionPermit checks remain
// outside this pure Farm projection and final mutation still belongs to Actor.
func EvaluateFarmOpportunityFacts(facts FarmOpportunityFactsV1, viewerPlayerID uint64, trustedNowMs int64) (EvaluatedFarmOpportunity, error) {
	if facts.SchemaVersion != FarmOpportunityFactsSchemaV1 || viewerPlayerID == 0 || trustedNowMs <= 0 {
		return EvaluatedFarmOpportunity{}, errors.New("farm opportunity: exact evaluation input is invalid")
	}
	result := EvaluatedFarmOpportunity{}
	mark := func(mask ViewerOpportunityMask, atMs int64) {
		result.Mask |= mask
		if atMs > result.LatestActionableAtMs {
			result.LatestActionableAtMs = atMs
		}
	}
	for _, cycle := range facts.Cycles {
		mature := cycle.MatureAtMs <= trustedNowMs
		if mature && cycle.RemainingTheftSlots > 0 {
			attempted := false
			for _, visitorID := range cycle.TheftVisitorIDs {
				if visitorID == viewerPlayerID {
					attempted = true
					break
				}
			}
			if !attempted {
				mark(ViewerCanSteal, cycle.MatureAtMs)
				result.EligibleTheftTargets = append(result.EligibleTheftTargets, EligibleTheftTarget{PlotID: cycle.PlotID, CycleID: cycle.CycleID})
			}
		}
		// FarmActor rejects WATER at/after maturity. Read eligibility must use
		// the same trusted-time boundary and never advertise an impossible action.
		if !mature && cycle.Water.IncidentID != "" && cycle.Water.DueAtMs <= trustedNowMs {
			mark(ViewerCanWater, cycle.Water.DueAtMs)
		}
		if cycle.Pest.IncidentID != "" && cycle.Pest.DueAtMs <= trustedNowMs {
			mark(ViewerCanRemovePest, cycle.Pest.DueAtMs)
		}
	}
	sort.Slice(result.EligibleTheftTargets, func(i, j int) bool {
		if result.EligibleTheftTargets[i].PlotID != result.EligibleTheftTargets[j].PlotID {
			return result.EligibleTheftTargets[i].PlotID < result.EligibleTheftTargets[j].PlotID
		}
		return result.EligibleTheftTargets[i].CycleID < result.EligibleTheftTargets[j].CycleID
	})
	return result, nil
}

// DeriveFarmOpportunityReadModels deterministically projects the compact
// Header/Facts pair from one authoritative FarmState. No wall clock is read.
func DeriveFarmOpportunityReadModels(state *FarmState) (FarmOpportunityHeaderV2, FarmOpportunityFactsV1, error) {
	header := FarmOpportunityHeaderV2{SchemaVersion: FarmOpportunityHeaderSchemaV2}
	facts := FarmOpportunityFactsV1{SchemaVersion: FarmOpportunityFactsSchemaV1}
	if state == nil {
		return header, facts, errors.New("farm opportunity: nil state")
	}
	header.SourceFarmVersion = state.FarmVersion
	facts.SourceFarmVersion = state.FarmVersion

	for i := range state.Cycles {
		cycle := &state.Cycles[i]
		if cycle.Ended {
			continue
		}
		if cycle.CycleID == "" || cycle.PlotID == 0 || cycle.MatureAtMs <= 0 {
			return header, facts, errors.New("farm opportunity: active cycle is incomplete")
		}
		if cycle.StolenAmount > cycle.StealableLimit || cycle.StealableLimit > cycle.BaseYield {
			return header, facts, errors.New("farm opportunity: theft quantities are invalid")
		}

		fact := FarmOpportunityCycleFactV1{
			PlotID: cycle.PlotID, CycleID: cycle.CycleID, MatureAtMs: cycle.MatureAtMs,
		}
		seenVisitors := make(map[uint64]struct{}, len(cycle.Attempts))
		for _, attempt := range cycle.Attempts {
			if attempt.VisitorPlayerID == 0 {
				return header, facts, errors.New("farm opportunity: theft visitor is empty")
			}
			if _, duplicated := seenVisitors[attempt.VisitorPlayerID]; duplicated {
				continue
			}
			seenVisitors[attempt.VisitorPlayerID] = struct{}{}
			fact.TheftVisitorIDs = append(fact.TheftVisitorIDs, attempt.VisitorPlayerID)
		}
		if len(fact.TheftVisitorIDs) > MaxTheftVisitorsPerCycle {
			return header, facts, errors.New("farm opportunity: theft visitor bound exceeded")
		}
		sort.Slice(fact.TheftVisitorIDs, func(i, j int) bool { return fact.TheftVisitorIDs[i] < fact.TheftVisitorIDs[j] })

		totalSlots := len(cycle.StealPlan.SlotQuantity)
		if totalSlots == 0 && cycle.StealableLimit > 0 {
			totalSlots = MaxTheftVisitorsPerCycle
		}
		if totalSlots > MaxTheftVisitorsPerCycle {
			return header, facts, errors.New("farm opportunity: theft slot bound exceeded")
		}
		if cycle.RemainingStealable() > 0 && totalSlots > len(fact.TheftVisitorIDs) {
			fact.RemainingTheftSlots = uint32(totalSlots - len(fact.TheftVisitorIDs))
			header.NextStealableAtMs = earliestNonZero(header.NextStealableAtMs, cycle.MatureAtMs)
		}

		for _, incident := range cycle.Incidents {
			if incident.Resolved() {
				continue
			}
			if incident.IncidentID == "" || incident.DueAtMs <= 0 {
				return header, facts, errors.New("farm opportunity: unresolved incident is incomplete")
			}
			switch incident.CareType {
			case CareTypeWater:
				if fact.Water.IncidentID != "" {
					return header, facts, errors.New("farm opportunity: duplicate unresolved water incident")
				}
				fact.Water = OpportunityIncidentFactV1{IncidentID: incident.IncidentID, DueAtMs: incident.DueAtMs}
				header.NextWaterAtMs = earliestNonZero(header.NextWaterAtMs, incident.DueAtMs)
			case CareTypeRemovePest:
				if fact.Pest.IncidentID != "" {
					return header, facts, errors.New("farm opportunity: duplicate unresolved pest incident")
				}
				fact.Pest = OpportunityIncidentFactV1{IncidentID: incident.IncidentID, DueAtMs: incident.DueAtMs}
				header.NextPestAtMs = earliestNonZero(header.NextPestAtMs, incident.DueAtMs)
			default:
				return header, facts, errors.New("farm opportunity: unknown care type")
			}
		}
		facts.Cycles = append(facts.Cycles, fact)
	}
	if len(facts.Cycles) > MaxOpportunityCycles {
		return header, facts, errors.New("farm opportunity: active cycle bound exceeded")
	}
	sort.Slice(facts.Cycles, func(i, j int) bool {
		if facts.Cycles[i].PlotID != facts.Cycles[j].PlotID {
			return facts.Cycles[i].PlotID < facts.Cycles[j].PlotID
		}
		return facts.Cycles[i].CycleID < facts.Cycles[j].CycleID
	})
	header.NextOpportunityAtMs = earliestNonZero(header.NextStealableAtMs, header.NextWaterAtMs, header.NextPestAtMs)
	return header, facts, nil
}

func earliestNonZero(values ...int64) int64 {
	var earliest int64
	for _, value := range values {
		if value > 0 && (earliest == 0 || value < earliest) {
			earliest = value
		}
	}
	return earliest
}

// MarshalFarmOpportunityHeaderV2 uses a fixed-width deterministic encoding.
func MarshalFarmOpportunityHeaderV2(header FarmOpportunityHeaderV2) ([]byte, error) {
	if header.SchemaVersion != FarmOpportunityHeaderSchemaV2 {
		return nil, errors.New("farm opportunity: header schema is invalid")
	}
	buf := make([]byte, 48)
	copy(buf[:4], headerV2Magic[:])
	binary.BigEndian.PutUint32(buf[4:8], header.SchemaVersion)
	binary.BigEndian.PutUint64(buf[8:16], header.SourceFarmVersion)
	binary.BigEndian.PutUint64(buf[16:24], uint64(header.NextStealableAtMs))
	binary.BigEndian.PutUint64(buf[24:32], uint64(header.NextWaterAtMs))
	binary.BigEndian.PutUint64(buf[32:40], uint64(header.NextPestAtMs))
	binary.BigEndian.PutUint64(buf[40:48], uint64(header.NextOpportunityAtMs))
	return buf, nil
}

func UnmarshalFarmOpportunityHeaderV2(data []byte) (FarmOpportunityHeaderV2, error) {
	if len(data) != 48 || string(data[:4]) != string(headerV2Magic[:]) {
		return FarmOpportunityHeaderV2{}, errors.New("farm opportunity: header encoding is invalid")
	}
	header := FarmOpportunityHeaderV2{
		SchemaVersion: binary.BigEndian.Uint32(data[4:8]), SourceFarmVersion: binary.BigEndian.Uint64(data[8:16]),
		NextStealableAtMs: int64(binary.BigEndian.Uint64(data[16:24])), NextWaterAtMs: int64(binary.BigEndian.Uint64(data[24:32])),
		NextPestAtMs: int64(binary.BigEndian.Uint64(data[32:40])), NextOpportunityAtMs: int64(binary.BigEndian.Uint64(data[40:48])),
	}
	if header.SchemaVersion != FarmOpportunityHeaderSchemaV2 {
		return FarmOpportunityHeaderV2{}, errors.New("farm opportunity: unsupported header schema")
	}
	return header, nil
}

// MarshalFarmOpportunityFactsV1 uses a bounded, deterministic binary encoding.
func MarshalFarmOpportunityFactsV1(facts FarmOpportunityFactsV1) ([]byte, error) {
	if facts.SchemaVersion != FarmOpportunityFactsSchemaV1 || len(facts.Cycles) > MaxOpportunityCycles {
		return nil, errors.New("farm opportunity: facts shape is invalid")
	}
	buf := make([]byte, 0, 32+len(facts.Cycles)*96)
	buf = append(buf, factsV1Magic[:]...)
	buf = appendU32(buf, facts.SchemaVersion)
	buf = appendU64(buf, facts.SourceFarmVersion)
	buf = appendU32(buf, uint32(len(facts.Cycles)))
	for _, cycle := range facts.Cycles {
		if cycle.PlotID == 0 || cycle.CycleID == "" || len(cycle.TheftVisitorIDs) > MaxTheftVisitorsPerCycle {
			return nil, errors.New("farm opportunity: cycle fact is invalid")
		}
		buf = appendU32(buf, cycle.PlotID)
		var err error
		buf, err = appendString(buf, cycle.CycleID)
		if err != nil {
			return nil, err
		}
		buf = appendI64(buf, cycle.MatureAtMs)
		buf = appendU32(buf, cycle.RemainingTheftSlots)
		buf, err = appendIncident(buf, cycle.Water)
		if err != nil {
			return nil, err
		}
		buf, err = appendIncident(buf, cycle.Pest)
		if err != nil {
			return nil, err
		}
		buf = appendU32(buf, uint32(len(cycle.TheftVisitorIDs)))
		for _, visitorID := range cycle.TheftVisitorIDs {
			if visitorID == 0 {
				return nil, errors.New("farm opportunity: visitor id is invalid")
			}
			buf = appendU64(buf, visitorID)
		}
	}
	return buf, nil
}

func UnmarshalFarmOpportunityFactsV1(data []byte) (FarmOpportunityFactsV1, error) {
	reader := opportunityReader{data: data}
	magic, err := reader.take(4)
	if err != nil || string(magic) != string(factsV1Magic[:]) {
		return FarmOpportunityFactsV1{}, errors.New("farm opportunity: facts encoding is invalid")
	}
	schema, err := reader.u32()
	if err != nil || schema != FarmOpportunityFactsSchemaV1 {
		return FarmOpportunityFactsV1{}, errors.New("farm opportunity: unsupported facts schema")
	}
	source, err := reader.u64()
	if err != nil {
		return FarmOpportunityFactsV1{}, err
	}
	count, err := reader.u32()
	if err != nil || count > MaxOpportunityCycles {
		return FarmOpportunityFactsV1{}, errors.New("farm opportunity: facts cycle count is invalid")
	}
	facts := FarmOpportunityFactsV1{SchemaVersion: schema, SourceFarmVersion: source, Cycles: make([]FarmOpportunityCycleFactV1, 0, count)}
	for i := uint32(0); i < count; i++ {
		plotID, err := reader.u32()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		cycleID, err := reader.str()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		matureAt, err := reader.i64()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		remaining, err := reader.u32()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		water, err := reader.incident()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		pest, err := reader.incident()
		if err != nil {
			return FarmOpportunityFactsV1{}, err
		}
		visitorCount, err := reader.u32()
		if err != nil || visitorCount > MaxTheftVisitorsPerCycle {
			return FarmOpportunityFactsV1{}, errors.New("farm opportunity: visitor count is invalid")
		}
		cycle := FarmOpportunityCycleFactV1{PlotID: plotID, CycleID: cycleID, MatureAtMs: matureAt, RemainingTheftSlots: remaining, Water: water, Pest: pest}
		for j := uint32(0); j < visitorCount; j++ {
			visitorID, err := reader.u64()
			if err != nil {
				return FarmOpportunityFactsV1{}, err
			}
			cycle.TheftVisitorIDs = append(cycle.TheftVisitorIDs, visitorID)
		}
		if cycle.PlotID == 0 || cycle.CycleID == "" {
			return FarmOpportunityFactsV1{}, errors.New("farm opportunity: decoded cycle fact is invalid")
		}
		facts.Cycles = append(facts.Cycles, cycle)
	}
	if reader.remaining() != 0 {
		return FarmOpportunityFactsV1{}, errors.New("farm opportunity: facts have trailing bytes")
	}
	return facts, nil
}

func appendIncident(dst []byte, incident OpportunityIncidentFactV1) ([]byte, error) {
	var err error
	dst, err = appendString(dst, incident.IncidentID)
	if err != nil {
		return nil, err
	}
	return appendI64(dst, incident.DueAtMs), nil
}

func appendString(dst []byte, value string) ([]byte, error) {
	if len(value) > 256 {
		return nil, errors.New("farm opportunity: string exceeds bound")
	}
	dst = appendU32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func appendU32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendU64(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendI64(dst []byte, value int64) []byte { return appendU64(dst, uint64(value)) }

type opportunityReader struct {
	data   []byte
	offset int
}

func (r *opportunityReader) remaining() int { return len(r.data) - r.offset }

func (r *opportunityReader) take(size int) ([]byte, error) {
	if size < 0 || r.remaining() < size {
		return nil, errors.New("farm opportunity: truncated encoding")
	}
	value := r.data[r.offset : r.offset+size]
	r.offset += size
	return value, nil
}

func (r *opportunityReader) u32() (uint32, error) {
	data, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(data), nil
}

func (r *opportunityReader) u64() (uint64, error) {
	data, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data), nil
}

func (r *opportunityReader) i64() (int64, error) {
	value, err := r.u64()
	return int64(value), err
}

func (r *opportunityReader) str() (string, error) {
	size, err := r.u32()
	if err != nil || size > 256 {
		return "", errors.New("farm opportunity: string length is invalid")
	}
	data, err := r.take(int(size))
	return string(data), err
}

func (r *opportunityReader) incident() (OpportunityIncidentFactV1, error) {
	id, err := r.str()
	if err != nil {
		return OpportunityIncidentFactV1{}, err
	}
	due, err := r.i64()
	if err != nil {
		return OpportunityIncidentFactV1{}, err
	}
	if (id == "") != (due == 0) {
		return OpportunityIncidentFactV1{}, fmt.Errorf("farm opportunity: incident encoding is inconsistent")
	}
	return OpportunityIncidentFactV1{IncidentID: id, DueAtMs: due}, nil
}
