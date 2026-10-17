package actor

import (
	"errors"
	"fmt"

	farmpb "farmgame/api/gen/farm"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"
)

const (
	FarmOpportunityHeaderModelField = domain.FarmOpportunityHeaderModelField
	FarmOpportunityFactsModelField  = domain.FarmOpportunityFactsModelField
)

// deriveFarmOpportunityModels rebuilds the compact read facts directly from
// the durable FarmRecord and its typed receipts. It does not trust mutable
// actor caches, which is important while a candidate CARE receipt has been
// appended but the post-commit cache has not yet been installed.
func deriveFarmOpportunityModels(record *farmpb.FarmRecord) (domain.FarmOpportunityHeaderV2, domain.FarmOpportunityFactsV1, error) {
	if record == nil || record.Core == nil || record.FarmId == 0 {
		return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, errors.New("farm actor: opportunity source record is incomplete")
	}
	state := domain.FarmState{FarmID: record.FarmId, OwnerPlayerID: record.FarmId, FarmVersion: record.Core.FarmVersion}
	cycleByID := make(map[string]int, len(record.Core.CropCycles))
	for _, publicCycle := range record.Core.CropCycles {
		if publicCycle == nil || publicCycle.GetCropCycleId() == "" {
			continue
		}
		frozen, _ := participantCropFromReceipts(record, publicCycle.GetCropCycleId())
		cycle := participantDomainCycle(publicCycle, frozen)
		mergeParticipantTheftCycle(record, &cycle)
		state.Cycles = append(state.Cycles, cycle)
		cycleByID[cycle.CycleID] = len(state.Cycles) - 1
	}

	for _, receipt := range record.GetRecentOperationReceipts() {
		if receipt == nil {
			continue
		}
		care, found, err := decodeCareProof(receipt.GetResultPayload())
		if !found {
			continue
		}
		if err != nil {
			return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, fmt.Errorf("farm actor: decode CARE opportunity proof: %w", err)
		}
		if err := validateCareReceiptBinding(receipt, care); err != nil {
			return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, err
		}
		if care.outcome != domain.CareOutcomeResolved {
			continue
		}
		cycleIndex, ok := cycleByID[care.cycleID]
		if !ok {
			// Historical CARE receipts outlive a harvested/ended public cycle.
			// They no longer contribute to the current opportunity read model.
			continue
		}
		cycle := &state.Cycles[cycleIndex]
		incident := cycle.FindIncident(care.incidentID)
		if incident == nil || incident.CareType != care.careType {
			return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, errors.New("farm actor: CARE opportunity incident binding is invalid")
		}
		incident.ResolvedAtMs = care.resolvedAtMs
		incident.ResolvedByPlayerID = care.resolvedByPlayerID
	}
	return domain.DeriveFarmOpportunityReadModels(&state)
}

func buildFarmAuthorityModelState(record *farmpb.FarmRecord) (authoritystore.ModelState, domain.FarmOpportunityHeaderV2, domain.FarmOpportunityFactsV1, error) {
	header, facts, headerBytes, factsBytes, err := encodeFarmOpportunityModels(record)
	if err != nil {
		return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, err
	}
	recordBytes, err := marshalRecord(record)
	if err != nil {
		return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, err
	}
	return authoritystore.ModelState{
		"record":                        recordBytes,
		FarmOpportunityHeaderModelField: headerBytes,
		FarmOpportunityFactsModelField:  factsBytes,
	}, header, facts, nil
}

func buildFarmAuthorityCandidate(layout authoritystore.ModelLayoutVersion, record *farmpb.FarmRecord) (authoritystore.ModelState, domain.FarmOpportunityHeaderV2, domain.FarmOpportunityFactsV1, error) {
	header, facts, headerBytes, factsBytes, err := encodeFarmOpportunityModels(record)
	if err != nil {
		return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, err
	}
	switch layout {
	case authoritystore.ModelLayoutV1:
		recordBytes, marshalErr := marshalRecord(record)
		if marshalErr != nil {
			return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, marshalErr
		}
		return authoritystore.ModelState{
			"record":                        recordBytes,
			FarmOpportunityHeaderModelField: headerBytes,
			FarmOpportunityFactsModelField:  factsBytes,
		}, header, facts, nil
	case authoritystore.ModelLayoutV2:
		// V2 has no monolithic record field. Encode it directly instead of first
		// serializing a full V1 record that would immediately be discarded.
		state, encodeErr := farmactormodelv2.Encode(record, headerBytes, factsBytes)
		if encodeErr != nil {
			return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, encodeErr
		}
		return state, header, facts, nil
	default:
		return nil, domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, errors.New("farm actor: unknown model layout")
	}
}

func encodeFarmOpportunityModels(record *farmpb.FarmRecord) (domain.FarmOpportunityHeaderV2, domain.FarmOpportunityFactsV1, []byte, []byte, error) {
	header, facts, err := deriveFarmOpportunityModels(record)
	if err != nil {
		return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, nil, nil, err
	}
	headerBytes, err := domain.MarshalFarmOpportunityHeaderV2(header)
	if err != nil {
		return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, nil, nil, err
	}
	factsBytes, err := domain.MarshalFarmOpportunityFactsV1(facts)
	if err != nil {
		return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, nil, nil, err
	}
	return header, facts, headerBytes, factsBytes, nil
}

func farmAuthorityModelBytes(record *farmpb.FarmRecord) (int, error) {
	return farmAuthorityModelBytesForLayout(authoritystore.ModelLayoutV1, record)
}

func farmAuthorityModelBytesForLayout(layout authoritystore.ModelLayoutVersion, record *farmpb.FarmRecord) (int, error) {
	state, _, _, err := buildFarmAuthorityCandidate(layout, record)
	if err != nil {
		return 0, err
	}
	total := 0
	for name, value := range state {
		total += len(name) + len(value)
	}
	return total, nil
}

func installFarmOpportunityModels(actor *FarmActor, state authoritystore.ModelState) error {
	if actor == nil {
		return errors.New("farm actor: nil actor")
	}
	headerBytes, headerFound := state[FarmOpportunityHeaderModelField]
	factsBytes, factsFound := state[FarmOpportunityFactsModelField]
	if !headerFound || !factsFound {
		actor.opportunityModelsAvailable = false
		actor.opportunityHeaderV2 = domain.FarmOpportunityHeaderV2{}
		actor.opportunityFactsV1 = domain.FarmOpportunityFactsV1{}
		return nil
	}
	header, err := domain.UnmarshalFarmOpportunityHeaderV2(headerBytes)
	if err != nil {
		return fmt.Errorf("farm actor: decode opportunity header: %w", err)
	}
	facts, err := domain.UnmarshalFarmOpportunityFactsV1(factsBytes)
	if err != nil {
		return fmt.Errorf("farm actor: decode opportunity facts: %w", err)
	}
	wantVersion := uint64(0)
	if actor.record != nil && actor.record.Core != nil {
		wantVersion = actor.record.Core.FarmVersion
	}
	if header.SourceFarmVersion != wantVersion || facts.SourceFarmVersion != wantVersion {
		return errors.New("farm actor: opportunity models do not match FarmVersion")
	}
	actor.opportunityHeaderV2 = header
	actor.opportunityFactsV1 = facts
	actor.opportunityModelsAvailable = true
	return nil
}

// OpportunityReadModels reports the atomically loaded/committed compact
// authority view. false means legacy/missing fields and requires fallback;
// it never means that the farm has no opportunity.
func (a *FarmActor) OpportunityReadModels() (domain.FarmOpportunityHeaderV2, domain.FarmOpportunityFactsV1, bool) {
	if a == nil || !a.opportunityModelsAvailable {
		return domain.FarmOpportunityHeaderV2{}, domain.FarmOpportunityFactsV1{}, false
	}
	return a.opportunityHeaderV2, a.opportunityFactsV1, true
}
