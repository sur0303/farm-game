package readpath

import (
	"context"
	"testing"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"
)

type fakeModelReader struct {
	rows map[uint64]authoritystore.ModelFieldReadResult
}

func TestProviderDecodesV2OpportunityEnvelopesWithoutBlobFallback(t *testing.T) {
	headerBytes, err := domain.MarshalFarmOpportunityHeaderV2(domain.FarmOpportunityHeaderV2{
		SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: 12, NextOpportunityAtMs: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	factsBytes, err := domain.MarshalFarmOpportunityFactsV1(domain.FarmOpportunityFactsV1{
		SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	headerField, err := fieldv2.Marshal(actorrouting.DomainFarm, domain.FarmOpportunityHeaderModelField, headerBytes)
	if err != nil {
		t.Fatal(err)
	}
	factsField, err := fieldv2.Marshal(actorrouting.DomainFarm, domain.FarmOpportunityFactsModelField, factsBytes)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewProvider(fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		4: {ActorID: 4, Found: true, RecordVersion: 8, ModelLayout: authoritystore.ModelLayoutV2,
			ModelFields: authoritystore.ModelState{
				domain.FarmOpportunityHeaderModelField: headerField,
				domain.FarmOpportunityFactsModelField:  factsField,
			}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	headers, err := provider.BatchReadHeaders(context.Background(), []uint64{4})
	if err != nil || headers[0].Status != StatusFound || headers[0].Header.SourceFarmVersion != 12 {
		t.Fatalf("headers=%+v err=%v", headers, err)
	}
	evaluated, err := provider.BatchEvaluate(context.Background(), 99, []uint64{4}, 200)
	wantRevision, _ := domain.FarmOpportunityRevisionForSource(12)
	if err != nil || evaluated[0].Status != StatusFound || evaluated[0].OpportunityRevision != wantRevision {
		t.Fatalf("evaluated=%+v err=%v", evaluated, err)
	}
}

func (f fakeModelReader) BatchReadModelFields(_ context.Context, _ actorrouting.RouteDomain, actorIDs []uint64, fields []string) ([]authoritystore.ModelFieldReadResult, error) {
	results := make([]authoritystore.ModelFieldReadResult, 0, len(actorIDs))
	for _, actorID := range actorIDs {
		result, ok := f.rows[actorID]
		if !ok {
			result = authoritystore.ModelFieldReadResult{ActorID: actorID}
		}
		selected := authoritystore.ModelState{}
		for _, field := range fields {
			if value, exists := result.ModelFields[field]; exists {
				selected[field] = value
			}
		}
		result.ModelFields = selected
		results = append(results, result)
	}
	return results, nil
}

func TestProviderReturnsExactViewerStateAndLegacyUnknown(t *testing.T) {
	header := domain.FarmOpportunityHeaderV2{
		SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: 9,
		NextStealableAtMs: 100, NextOpportunityAtMs: 100,
	}
	facts := domain.FarmOpportunityFactsV1{
		SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 9,
		Cycles: []domain.FarmOpportunityCycleFactV1{{
			PlotID: 1, CycleID: "cycle", MatureAtMs: 100, RemainingTheftSlots: 9,
			TheftVisitorIDs: []uint64{8},
		}},
	}
	headerBytes, _ := domain.MarshalFarmOpportunityHeaderV2(header)
	factsBytes, _ := domain.MarshalFarmOpportunityFactsV1(facts)
	provider, err := NewProvider(fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: {ActorID: 1, Found: true, RecordVersion: 3, ModelLayout: authoritystore.ModelLayoutV1, ModelFields: authoritystore.ModelState{
			domain.FarmOpportunityHeaderModelField: headerBytes,
			domain.FarmOpportunityFactsModelField:  factsBytes,
		}},
		2: {ActorID: 2, Found: true, RecordVersion: 1, ModelLayout: authoritystore.ModelLayoutV1, ModelFields: authoritystore.ModelState{"record": []byte("legacy")}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	results, err := provider.BatchEvaluate(context.Background(), 8, []uint64{1, 2, 3}, 200)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusFound || results[0].Evaluation.Mask&domain.ViewerCanSteal != 0 {
		t.Fatalf("receipt did not fence viewer: %+v", results[0])
	}
	if results[1].Status != StatusUnknown || results[1].Err != nil {
		t.Fatalf("legacy fields must be UNKNOWN fallback: %+v", results[1])
	}
	if results[2].Status != StatusNotFound {
		t.Fatalf("missing farm status=%v", results[2].Status)
	}

	results, err = provider.BatchEvaluate(context.Background(), 9, []uint64{1}, 200)
	if err != nil || results[0].Evaluation.Mask&domain.ViewerCanSteal == 0 {
		t.Fatalf("fresh viewer did not receive steal opportunity: result=%+v err=%v", results[0], err)
	}
}

func TestProviderTreatsGenesisEmptyFarmAsKnownNone(t *testing.T) {
	headerBytes, err := domain.MarshalFarmOpportunityHeaderV2(domain.FarmOpportunityHeaderV2{
		SchemaVersion: domain.FarmOpportunityHeaderSchemaV2,
	})
	if err != nil {
		t.Fatal(err)
	}
	factsBytes, err := domain.MarshalFarmOpportunityFactsV1(domain.FarmOpportunityFactsV1{
		SchemaVersion: domain.FarmOpportunityFactsSchemaV1,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewProvider(fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		7: {ActorID: 7, Found: true, RecordVersion: 1, ModelLayout: authoritystore.ModelLayoutV1,
			ModelFields: authoritystore.ModelState{
				domain.FarmOpportunityHeaderModelField: headerBytes,
				domain.FarmOpportunityFactsModelField:  factsBytes,
			}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	results, err := provider.BatchEvaluate(context.Background(), 99, []uint64{7}, 200)
	if err != nil || len(results) != 1 || results[0].Status != StatusFound ||
		results[0].OpportunityRevision != 0 || results[0].Evaluation.Mask != 0 || results[0].Err != nil {
		t.Fatalf("genesis result=%+v err=%v", results, err)
	}
}
