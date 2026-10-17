package opportunityrecovery

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"
)

var ErrFarmNotFound = errors.New("opportunity recovery: Farm authority not found")

// ModelFieldBatchReaderAdapter composes the existing read-only model-field
// port into the worker's one-farm Header/Facts seam. It never claims a writer
// lease and never falls back to the full Farm record.
type ModelFieldBatchReaderAdapter struct {
	reader authoritystore.ModelFieldBatchReader
}

var _ FarmOpportunityReader = (*ModelFieldBatchReaderAdapter)(nil)

func NewModelFieldBatchReaderAdapter(reader authoritystore.ModelFieldBatchReader) (*ModelFieldBatchReaderAdapter, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: ModelFieldBatchReader is required", ErrInvalidConfig)
	}
	return &ModelFieldBatchReaderAdapter{reader: reader}, nil
}

// NewFarmOpportunityReader is a short composition-root alias for the model
// field adapter.
func NewFarmOpportunityReader(reader authoritystore.ModelFieldBatchReader) (*ModelFieldBatchReaderAdapter, error) {
	return NewModelFieldBatchReaderAdapter(reader)
}

func (r *ModelFieldBatchReaderAdapter) ReadFarmOpportunity(ctx context.Context, farmID uint64) (FarmOpportunity, error) {
	if r == nil || r.reader == nil {
		return FarmOpportunity{}, fmt.Errorf("%w: nil ModelFieldBatchReader adapter", ErrInvalidConfig)
	}
	if farmID == 0 {
		return FarmOpportunity{}, fmt.Errorf("%w: farm id is required", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := r.reader.BatchReadModelFields(ctx, actorrouting.DomainFarm, []uint64{farmID}, []string{
		domain.FarmOpportunityHeaderModelField, domain.FarmOpportunityFactsModelField,
	})
	if err != nil {
		return FarmOpportunity{}, err
	}
	if len(rows) != 1 {
		return FarmOpportunity{}, errors.New("opportunity recovery: Farm model read cardinality mismatch")
	}
	row := rows[0]
	if row.ActorID != 0 && row.ActorID != farmID {
		return FarmOpportunity{}, fmt.Errorf("opportunity recovery: Farm model row identity mismatch (want=%d got=%d)", farmID, row.ActorID)
	}
	if row.Err != nil {
		return FarmOpportunity{}, row.Err
	}
	if !row.Found {
		return FarmOpportunity{}, ErrFarmNotFound
	}
	headerBytes, headerFound := row.ModelFields[domain.FarmOpportunityHeaderModelField]
	factsBytes, factsFound := row.ModelFields[domain.FarmOpportunityFactsModelField]
	if !headerFound || !factsFound {
		return FarmOpportunity{}, errors.New("opportunity recovery: Farm Header/Facts fields are incomplete")
	}
	if row.ModelLayout == authoritystore.ModelLayoutV2 {
		headerBytes, err = fieldv2.Unmarshal(actorrouting.DomainFarm, domain.FarmOpportunityHeaderModelField, headerBytes)
		if err == nil {
			factsBytes, err = fieldv2.Unmarshal(actorrouting.DomainFarm, domain.FarmOpportunityFactsModelField, factsBytes)
		}
		if err != nil {
			return FarmOpportunity{}, err
		}
	} else if row.ModelLayout != authoritystore.ModelLayoutV1 {
		return FarmOpportunity{}, errors.New("opportunity recovery: unknown Farm model layout")
	}
	header, headerErr := domain.UnmarshalFarmOpportunityHeaderV2(headerBytes)
	facts, factsErr := domain.UnmarshalFarmOpportunityFactsV1(factsBytes)
	if headerErr != nil || factsErr != nil {
		return FarmOpportunity{}, errors.Join(headerErr, factsErr)
	}
	farm := FarmOpportunity{FarmID: farmID, OwnerID: farmID, Header: header, Facts: facts}
	if err := validateFarmOpportunity(farm, farmID); err != nil {
		return FarmOpportunity{}, err
	}
	return farm, nil
}
