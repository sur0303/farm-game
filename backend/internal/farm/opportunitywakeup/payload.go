// Package opportunitywakeup defines the deterministic source-local schedule
// payload used to wake one farm at its next time-derived opportunity boundary.
package opportunitywakeup

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"

	"farmgame/internal/farm/domain"
	"farmgame/internal/workidentity"
)

const (
	SchemaVersion uint32 = 1
	WorkType             = "FARM_OPPORTUNITY_WAKEUP"
	payloadSize          = 76
)

var payloadMagic = [4]byte{'F', 'O', 'W', '1'}

// ScheduleIDForFarm is unique inside a shared FARM Authority bucket. The
// source actor is deliberately part of the stable opaque ID because schedule
// records are bucket-local, not nested under an individual actor key.
func ScheduleIDForFarm(farmID uint64) (string, error) {
	if farmID == 0 {
		return "", errors.New("farm opportunity wakeup: farm id is invalid")
	}
	return workidentity.StableOpaqueID("farm-opportunity-wakeup-schedule-v1", strconv.FormatUint(farmID, 10))
}

// Payload is farm-global invalidation metadata. It does not contain a viewer,
// ActionPermit or final action mask. SourceFarmVersion is also the schedule
// generation, so a newer Farm authority commit can atomically supersede an
// already claimed older wakeup.
type Payload struct {
	FarmID             uint64
	ScheduleGeneration uint64
	SourceFarmVersion  uint64
	DueAtMs            int64
	BoundaryMask       domain.OpportunityBoundaryMask
	BoundaryDigest     [32]byte
}

func (p Payload) Validate() error {
	if p.FarmID == 0 || p.ScheduleGeneration == 0 || p.SourceFarmVersion == 0 ||
		p.ScheduleGeneration != p.SourceFarmVersion || p.DueAtMs <= 0 || p.BoundaryMask == 0 ||
		p.BoundaryDigest == ([32]byte{}) {
		return errors.New("farm opportunity wakeup: payload is invalid")
	}
	return nil
}

// FromFacts computes the first strictly-future boundary after one trusted
// cursor. false means the current farm has no future boundary to schedule.
func FromFacts(farmID uint64, facts domain.FarmOpportunityFactsV1, afterMs int64) (Payload, bool, error) {
	boundary, found, err := domain.NextFarmOpportunityBoundary(facts, afterMs)
	if err != nil || !found {
		return Payload{}, found, err
	}
	payload := Payload{
		FarmID: farmID, ScheduleGeneration: facts.SourceFarmVersion, SourceFarmVersion: facts.SourceFarmVersion,
		DueAtMs: boundary.AtMs, BoundaryMask: boundary.Mask, BoundaryDigest: boundary.Digest,
	}
	return payload, true, payload.Validate()
}

func Marshal(payload Payload) ([]byte, error) {
	if err := payload.Validate(); err != nil {
		return nil, err
	}
	data := make([]byte, payloadSize)
	copy(data[:4], payloadMagic[:])
	binary.BigEndian.PutUint32(data[4:8], SchemaVersion)
	binary.BigEndian.PutUint64(data[8:16], payload.FarmID)
	binary.BigEndian.PutUint64(data[16:24], payload.ScheduleGeneration)
	binary.BigEndian.PutUint64(data[24:32], payload.SourceFarmVersion)
	binary.BigEndian.PutUint64(data[32:40], uint64(payload.DueAtMs))
	binary.BigEndian.PutUint32(data[40:44], uint32(payload.BoundaryMask))
	copy(data[44:76], payload.BoundaryDigest[:])
	return data, nil
}

func Unmarshal(data []byte) (Payload, error) {
	if len(data) != payloadSize || !bytes.Equal(data[:4], payloadMagic[:]) || binary.BigEndian.Uint32(data[4:8]) != SchemaVersion {
		return Payload{}, errors.New("farm opportunity wakeup: payload encoding is invalid")
	}
	payload := Payload{
		FarmID: binary.BigEndian.Uint64(data[8:16]), ScheduleGeneration: binary.BigEndian.Uint64(data[16:24]),
		SourceFarmVersion: binary.BigEndian.Uint64(data[24:32]), DueAtMs: int64(binary.BigEndian.Uint64(data[32:40])),
		BoundaryMask: domain.OpportunityBoundaryMask(binary.BigEndian.Uint32(data[40:44])),
	}
	copy(payload.BoundaryDigest[:], data[44:76])
	return payload, payload.Validate()
}
