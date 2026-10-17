package readpath

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"
)

type fakeHeaderReader struct {
	rows  map[uint64]authoritystore.ModelFieldReadResult
	err   error
	calls int
}

func (f *fakeHeaderReader) BatchReadOpportunityHeaders(_ context.Context, farmIDs []uint64) ([]authoritystore.ModelFieldReadResult, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	results := make([]authoritystore.ModelFieldReadResult, 0, len(farmIDs))
	for _, farmID := range farmIDs {
		row, exists := f.rows[farmID]
		if !exists {
			row = authoritystore.ModelFieldReadResult{ActorID: farmID}
		}
		results = append(results, row)
	}
	return results, nil
}

type captureHeaderObserver struct {
	observations []HeaderReadObservation
}

func (o *captureHeaderObserver) ObserveHeaderRead(value HeaderReadObservation) {
	o.observations = append(o.observations, value)
}

func encodedHeaderRow(t *testing.T, farmID, recordVersion, farmVersion uint64) authoritystore.ModelFieldReadResult {
	t.Helper()
	payload, err := domain.MarshalFarmOpportunityHeaderV2(domain.FarmOpportunityHeaderV2{
		SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: farmVersion, NextOpportunityAtMs: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := fieldv2.Marshal(actorrouting.DomainFarm, domain.FarmOpportunityHeaderModelField, payload)
	if err != nil {
		t.Fatal(err)
	}
	return authoritystore.ModelFieldReadResult{
		ActorID: farmID, Found: true, RecordVersion: recordVersion, ModelLayout: authoritystore.ModelLayoutV2,
		ModelFields: authoritystore.ModelState{domain.FarmOpportunityHeaderModelField: envelope},
	}
}

func TestHeaderReadNarrowUsesNarrowResult(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 4, 10)}}
	narrow := &fakeHeaderReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 4, 10)}}
	observer := &captureHeaderObserver{}
	provider, err := NewProviderWithHeaderReadOptions(full, HeaderReadOptions{
		Mode: HeaderReadNarrow, NarrowReader: narrow, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := provider.BatchReadHeaders(context.Background(), []uint64{1})
	if err != nil || rows[0].Status != StatusFound || rows[0].Header.SourceFarmVersion != 10 || narrow.calls != 1 {
		t.Fatalf("rows=%+v calls=%d err=%v", rows, narrow.calls, err)
	}
	if len(observer.observations) != 1 || observer.observations[0].FullFallbackCount != 0 {
		t.Fatalf("observations=%+v", observer.observations)
	}
}

func TestHeaderReadNarrowFallsBackForBoundedUnknown(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 4, 11)}}
	narrow := &fakeHeaderReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: {ActorID: 1, Found: true, RecordVersion: 4, ModelLayout: authoritystore.ModelLayoutV2, Err: errors.New("broken narrow row")},
	}}
	observer := &captureHeaderObserver{}
	provider, err := NewProviderWithHeaderReadOptions(full, HeaderReadOptions{
		Mode: HeaderReadNarrow, NarrowReader: narrow, MaxFullFallback: 1, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := provider.BatchReadHeaders(context.Background(), []uint64{1})
	if err != nil || rows[0].Status != StatusFound || rows[0].Header.SourceFarmVersion != 11 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if got := observer.observations[0]; got.NarrowUnknown != 1 || got.FullFallbackCount != 1 {
		t.Fatalf("observation=%+v", got)
	}
}

func TestHeaderReadNarrowKeepsUnknownWhenFallbackBoundExceeded(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: encodedHeaderRow(t, 1, 4, 11), 2: encodedHeaderRow(t, 2, 5, 12),
	}}
	narrow := &fakeHeaderReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: {ActorID: 1, Found: true, Err: errors.New("bad")},
		2: {ActorID: 2, Found: true, Err: errors.New("bad")},
	}}
	provider, err := NewProviderWithHeaderReadOptions(full, HeaderReadOptions{
		Mode: HeaderReadNarrow, NarrowReader: narrow, MaxFullFallback: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := provider.BatchReadHeaders(context.Background(), []uint64{1, 2})
	if err != nil || rows[0].Status != StatusUnknown || rows[1].Status != StatusUnknown {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestHeaderReadShadowReturnsFullAndReportsMismatch(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 4, 11)}}
	narrow := &fakeHeaderReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 5, 12)}}
	observer := &captureHeaderObserver{}
	provider, err := NewProviderWithHeaderReadOptions(full, HeaderReadOptions{
		Mode: HeaderReadNarrowShadow, NarrowReader: narrow, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := provider.BatchReadHeaders(context.Background(), []uint64{1})
	if err != nil || rows[0].RecordVersion != 4 || rows[0].Header.SourceFarmVersion != 11 {
		t.Fatalf("shadow changed authoritative result: rows=%+v err=%v", rows, err)
	}
	if got := observer.observations[0]; got.MismatchCount != 1 {
		t.Fatalf("observation=%+v", got)
	}
}

func TestHeaderReadNarrowGlobalFailureFallsBackToFull(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{1: encodedHeaderRow(t, 1, 4, 11)}}
	narrow := &fakeHeaderReader{err: errors.New("redis unavailable")}
	provider, err := NewProviderWithHeaderReadOptions(full, HeaderReadOptions{Mode: HeaderReadNarrow, NarrowReader: narrow})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := provider.BatchReadHeaders(context.Background(), []uint64{1})
	if err != nil || rows[0].Status != StatusFound {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestHeaderReadModeRequiresNarrowReader(t *testing.T) {
	if _, err := NewProviderWithHeaderReadOptions(fakeModelReader{}, HeaderReadOptions{Mode: HeaderReadNarrow}); err == nil {
		t.Fatal("narrow mode without reader was accepted")
	}
	if _, err := ParseHeaderReadMode("mystery"); err == nil {
		t.Fatal("unknown mode was accepted")
	}
}

func TestProviderSwitchesHeaderReadModeWithoutRestart(t *testing.T) {
	full := fakeModelReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: encodedHeaderRow(t, 1, 4, 11),
	}}
	narrow := &fakeHeaderReader{rows: map[uint64]authoritystore.ModelFieldReadResult{
		1: encodedHeaderRow(t, 1, 4, 11),
	}}
	observer := &captureHeaderObserver{}
	provider, err := NewProviderWithHeaderReadOptions(
		full,
		HeaderReadOptions{
			Mode:         HeaderReadFull,
			NarrowReader: narrow,
			Observer:     observer,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []HeaderReadMode{
		HeaderReadNarrowShadow,
		HeaderReadNarrow,
	} {
		if err := provider.SetHeaderReadMode(mode); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.BatchReadHeaders(
			context.Background(),
			[]uint64{1},
		); err != nil {
			t.Fatal(err)
		}
		if got := provider.HeaderReadMode(); got != mode {
			t.Fatalf("mode=%q want=%q", got, mode)
		}
	}
	if len(observer.observations) != 2 ||
		observer.observations[0].Mode != HeaderReadNarrowShadow ||
		observer.observations[1].Mode != HeaderReadNarrow {
		t.Fatalf("observations=%+v", observer.observations)
	}
	if err := provider.SetHeaderReadMode("invalid"); err == nil {
		t.Fatal("invalid dynamic mode was accepted")
	}
	if got := provider.HeaderReadMode(); got != HeaderReadNarrow {
		t.Fatalf("invalid update changed mode to %q", got)
	}
}
