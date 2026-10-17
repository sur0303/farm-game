// Package actormodelv2 contains the deterministic FarmRecord-to-field
// converter. M1-A exposes it as a foundation without enabling FarmActor v2
// conversion in normal runtime.
package actormodelv2

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	farmdomain "farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

var ErrMalformed = errors.New("farm actor model v2: malformed state")

func ConvertV1Bytes(recordBytes, opportunityHeaderBytes, opportunityFactsBytes []byte) (authoritystore.ModelState, error) {
	var record farmpb.FarmRecord
	if err := unmarshalCanonical(recordBytes, &record); err != nil {
		return nil, err
	}
	return Encode(&record, opportunityHeaderBytes, opportunityFactsBytes)
}

func Encode(record *farmpb.FarmRecord, opportunityHeaderBytes, opportunityFactsBytes []byte) (authoritystore.ModelState, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	if len(opportunityHeaderBytes) == 0 || len(opportunityFactsBytes) == 0 ||
		len(opportunityHeaderBytes) > fieldv2.MaxFieldValueBytes || len(opportunityFactsBytes) > fieldv2.MaxFieldValueBytes {
		return nil, fmt.Errorf("%w: opportunity projections", ErrMalformed)
	}
	if err := validateOpportunityBytes(record, opportunityHeaderBytes, opportunityFactsBytes); err != nil {
		return nil, err
	}
	subsets := map[string]*farmpb.FarmRecord{
		"farm_meta": {
			FarmId: record.GetFarmId(),
			Core:   &farmpb.FarmCore{FarmId: record.GetCore().GetFarmId(), FarmVersion: record.GetCore().GetFarmVersion()},
		},
		"plots": {
			Core: &farmpb.FarmCore{
				Plots: cloneMessages(record.GetCore().GetPlots()), CropCycles: cloneMessages(record.GetCore().GetCropCycles()),
			},
		},
		// All embedded legacy business carriers live exactly once in this
		// summary. Guard proofs remain receipts; the separate guard field stays
		// the deterministic disabled gameplay state.
		"activity_summary": {
			RecentOperationReceipts: cloneMessages(record.GetRecentOperationReceipts()),
			ActiveSettlements:       cloneMessages(record.GetActiveSettlements()),
			RecentTheftAttempts:     cloneMessages(record.GetRecentTheftAttempts()),
			RecentCareAttempts:      cloneMessages(record.GetRecentCareAttempts()),
		},
	}
	state := make(authoritystore.ModelState, 6)
	for name, subset := range subsets {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(subset)
		if err != nil {
			return nil, fmt.Errorf("%w: marshal %s: %v", ErrMalformed, name, err)
		}
		state[name], err = fieldv2.Marshal(actorrouting.DomainFarm, name, payload)
		if err != nil {
			return nil, err
		}
	}
	var err error
	state["guard"], err = fieldv2.DisabledState(actorrouting.DomainFarm, "guard")
	if err != nil {
		return nil, err
	}
	state["opportunity_header"], err = fieldv2.Marshal(actorrouting.DomainFarm, "opportunity_header", opportunityHeaderBytes)
	if err != nil {
		return nil, err
	}
	state["opportunity_facts"], err = fieldv2.Marshal(actorrouting.DomainFarm, "opportunity_facts", opportunityFactsBytes)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func Decode(state authoritystore.ModelState) (*farmpb.FarmRecord, []byte, []byte, error) {
	if state == nil {
		return nil, nil, nil, fmt.Errorf("%w: nil state", ErrMalformed)
	}
	if err := fieldv2.ValidateExactNames(actorrouting.DomainFarm, fieldv2.SortedNames(state)); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := fieldv2.ValidateDisabledState(actorrouting.DomainFarm, "guard", state["guard"]); err != nil {
		return nil, nil, nil, err
	}
	parts := make(map[string]*farmpb.FarmRecord, 3)
	for _, name := range []string{"farm_meta", "plots", "activity_summary"} {
		payload, err := fieldv2.Unmarshal(actorrouting.DomainFarm, name, state[name])
		if err != nil {
			return nil, nil, nil, err
		}
		part := &farmpb.FarmRecord{}
		if err := unmarshalCanonical(payload, part); err != nil {
			return nil, nil, nil, fmt.Errorf("%w: decode %s: %v", ErrMalformed, name, err)
		}
		if err := validateSubset(name, part); err != nil {
			return nil, nil, nil, err
		}
		parts[name] = part
	}
	meta, plots, activity := parts["farm_meta"], parts["plots"], parts["activity_summary"]
	record := &farmpb.FarmRecord{
		FarmId: meta.GetFarmId(),
		Core: &farmpb.FarmCore{
			FarmId: meta.GetCore().GetFarmId(), FarmVersion: meta.GetCore().GetFarmVersion(),
			Plots: plots.GetCore().GetPlots(), CropCycles: plots.GetCore().GetCropCycles(),
		},
		RecentOperationReceipts: activity.GetRecentOperationReceipts(), ActiveSettlements: activity.GetActiveSettlements(),
		RecentTheftAttempts: activity.GetRecentTheftAttempts(), RecentCareAttempts: activity.GetRecentCareAttempts(),
	}
	if err := validateRecord(record); err != nil {
		return nil, nil, nil, err
	}
	header, err := fieldv2.Unmarshal(actorrouting.DomainFarm, "opportunity_header", state["opportunity_header"])
	if err != nil || len(header) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: opportunity header", ErrMalformed)
	}
	facts, err := fieldv2.Unmarshal(actorrouting.DomainFarm, "opportunity_facts", state["opportunity_facts"])
	if err != nil || len(facts) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: opportunity facts", ErrMalformed)
	}
	if err := validateOpportunityBytes(record, header, facts); err != nil {
		return nil, nil, nil, err
	}
	return record, header, facts, nil
}

func validateOpportunityBytes(record *farmpb.FarmRecord, headerBytes, factsBytes []byte) error {
	header, err := farmdomain.UnmarshalFarmOpportunityHeaderV2(headerBytes)
	if err != nil {
		return fmt.Errorf("%w: opportunity header codec", ErrMalformed)
	}
	facts, err := farmdomain.UnmarshalFarmOpportunityFactsV1(factsBytes)
	if err != nil {
		return fmt.Errorf("%w: opportunity facts codec", ErrMalformed)
	}
	version := record.GetCore().GetFarmVersion()
	if header.SourceFarmVersion != version || facts.SourceFarmVersion != version {
		return fmt.Errorf("%w: opportunity FarmVersion", ErrMalformed)
	}
	canonicalHeader, headerErr := farmdomain.MarshalFarmOpportunityHeaderV2(header)
	canonicalFacts, factsErr := farmdomain.MarshalFarmOpportunityFactsV1(facts)
	if headerErr != nil || factsErr != nil || !bytes.Equal(canonicalHeader, headerBytes) || !bytes.Equal(canonicalFacts, factsBytes) {
		return fmt.Errorf("%w: non-canonical opportunity codec", ErrMalformed)
	}
	return nil
}

func validateRecord(record *farmpb.FarmRecord) error {
	if record == nil || record.GetFarmId() == 0 || record.GetCore() == nil ||
		record.GetCore().GetFarmId() != record.GetFarmId() ||
		len(record.GetCore().GetPlots()) > 24 {
		return fmt.Errorf("%w: farm identity/core", ErrMalformed)
	}
	if err := fieldv2.ValidateNoUnknown(record); err != nil {
		return fmt.Errorf("%w: unknown fields", ErrMalformed)
	}
	counts := []int{len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()),
		len(record.GetActiveSettlements()), len(record.GetRecentTheftAttempts()), len(record.GetRecentCareAttempts())}
	for _, count := range counts {
		if count > fieldv2.MaxRepeatedItems {
			return fmt.Errorf("%w: repeated carrier capacity", ErrMalformed)
		}
	}
	return nil
}

func validateSubset(name string, part *farmpb.FarmRecord) error {
	check, ok := proto.Clone(part).(*farmpb.FarmRecord)
	if !ok {
		return fmt.Errorf("%w: clone subset", ErrMalformed)
	}
	switch name {
	case "farm_meta":
		check.FarmId = 0
		if check.Core != nil {
			check.Core.FarmId, check.Core.FarmVersion = 0, 0
			if len(check.Core.ProtoReflect().GetUnknown()) != 0 {
				return fmt.Errorf("%w: farm_meta unknown core", ErrMalformed)
			}
			check.Core = nil
		}
	case "plots":
		if check.Core != nil {
			check.Core.Plots, check.Core.CropCycles = nil, nil
			if len(check.Core.ProtoReflect().GetUnknown()) != 0 {
				return fmt.Errorf("%w: plots unknown core", ErrMalformed)
			}
			check.Core = nil
		}
	case "activity_summary":
		check.RecentOperationReceipts, check.ActiveSettlements = nil, nil
		check.RecentTheftAttempts, check.RecentCareAttempts = nil, nil
	default:
		return fmt.Errorf("%w: unknown subset", ErrMalformed)
	}
	remaining, err := proto.MarshalOptions{Deterministic: true}.Marshal(check)
	if err != nil || len(remaining) != 0 {
		return fmt.Errorf("%w: %s contains foreign fields", ErrMalformed, name)
	}
	return nil
}

func unmarshalCanonical(data []byte, message proto.Message) error {
	if len(data) > fieldv2.MaxFieldValueBytes {
		return fmt.Errorf("%w: bytes capacity", ErrMalformed)
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return fmt.Errorf("%w: protobuf", ErrMalformed)
	}
	if err := fieldv2.ValidateNoUnknown(message); err != nil {
		return fmt.Errorf("%w: protobuf unknown fields", ErrMalformed)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil || !bytes.Equal(canonical, data) {
		return fmt.Errorf("%w: non-canonical protobuf", ErrMalformed)
	}
	return nil
}

func cloneMessages[T proto.Message](in []T) []T {
	if in == nil {
		return nil
	}
	out := make([]T, len(in))
	for i, item := range in {
		if !reflect.ValueOf(item).IsNil() {
			out[i] = proto.Clone(item).(T)
		}
	}
	return out
}
