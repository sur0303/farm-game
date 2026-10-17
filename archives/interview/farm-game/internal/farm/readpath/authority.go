// Package readpath provides bounded, read-only Farm authority queries. It
// never activates FarmActor or participates in the actor mailbox.
package readpath

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/storage/authoritystore"
)

const MaxBatchFarmIDs = 64

type Status uint8

const (
	StatusUnknown Status = iota
	StatusFound
	StatusNotFound
)

type HeaderResult struct {
	FarmID        uint64
	RecordVersion uint64
	Status        Status
	Header        domain.FarmOpportunityHeaderV2
	Err           error
}

func (r HeaderResult) IsCandidate(trustedNowMs int64) bool {
	return r.Status == StatusFound && r.Header.NextOpportunityAtMs > 0 && r.Header.NextOpportunityAtMs <= trustedNowMs
}

type EvaluationResult struct {
	FarmID              uint64
	Status              Status
	OpportunityRevision uint64
	Evaluation          domain.EvaluatedFarmOpportunity
	Err                 error
}

type HeaderReadMode string

const (
	HeaderReadFull         HeaderReadMode = "full"
	HeaderReadNarrowShadow HeaderReadMode = "narrow-shadow"
	HeaderReadNarrow       HeaderReadMode = "narrow"
	defaultFullFallback                   = 8
)

func ParseHeaderReadMode(value string) (HeaderReadMode, error) {
	mode := HeaderReadMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		return HeaderReadFull, nil
	}
	switch mode {
	case HeaderReadFull, HeaderReadNarrowShadow, HeaderReadNarrow:
		return mode, nil
	default:
		return "", fmt.Errorf("farm readpath: unsupported header read mode %q", value)
	}
}

// OpportunityHeaderBatchReader reads only the Farm opportunity Header and
// the minimum metadata needed to decode it. Its result is a coarse candidate,
// never final authorization. Implementations must preserve input cardinality
// and order.
type OpportunityHeaderBatchReader interface {
	BatchReadOpportunityHeaders(ctx context.Context, farmIDs []uint64) ([]authoritystore.ModelFieldReadResult, error)
}

type HeaderReadObservation struct {
	Mode              HeaderReadMode
	FarmCount         int
	NarrowUnknown     int
	FullFallbackCount int
	MismatchCount     int
	NarrowUnavailable bool
}

type HeaderReadObserver interface {
	ObserveHeaderRead(HeaderReadObservation)
}

type HeaderReadOptions struct {
	Mode            HeaderReadMode
	NarrowReader    OpportunityHeaderBatchReader
	MaxFullFallback int
	Observer        HeaderReadObserver
}

type Provider struct {
	reader          authoritystore.ModelFieldBatchReader
	narrowReader    OpportunityHeaderBatchReader
	headerReadMode  atomic.Value
	maxFullFallback int
	observer        HeaderReadObserver
}

func NewProvider(reader authoritystore.ModelFieldBatchReader) (*Provider, error) {
	return NewProviderWithHeaderReadOptions(reader, HeaderReadOptions{Mode: HeaderReadFull})
}

func NewProviderWithHeaderReadOptions(reader authoritystore.ModelFieldBatchReader, options HeaderReadOptions) (*Provider, error) {
	if reader == nil {
		return nil, errors.New("farm readpath: model field reader is required")
	}
	mode, err := ParseHeaderReadMode(string(options.Mode))
	if err != nil {
		return nil, err
	}
	if mode != HeaderReadFull && options.NarrowReader == nil {
		return nil, errors.New("farm readpath: narrow header reader is required")
	}
	if options.MaxFullFallback <= 0 {
		options.MaxFullFallback = defaultFullFallback
	}
	if options.MaxFullFallback > MaxBatchFarmIDs {
		return nil, fmt.Errorf("farm readpath: max full fallback exceeds %d", MaxBatchFarmIDs)
	}
	provider := &Provider{
		reader: reader, narrowReader: options.NarrowReader,
		maxFullFallback: options.MaxFullFallback, observer: options.Observer,
	}
	provider.headerReadMode.Store(mode)
	return provider, nil
}

func (p *Provider) SetHeaderReadMode(mode HeaderReadMode) error {
	if p == nil {
		return errors.New("farm readpath: provider is required")
	}
	parsed, err := ParseHeaderReadMode(string(mode))
	if err != nil {
		return err
	}
	if parsed != HeaderReadFull && p.narrowReader == nil {
		return errors.New("farm readpath: narrow header reader is required")
	}
	p.headerReadMode.Store(parsed)
	return nil
}

func (p *Provider) HeaderReadMode() HeaderReadMode {
	if p == nil {
		return ""
	}
	mode, _ := p.headerReadMode.Load().(HeaderReadMode)
	return mode
}

func (p *Provider) BatchReadHeaders(ctx context.Context, farmIDs []uint64) ([]HeaderResult, error) {
	if err := validateBatch(farmIDs); err != nil {
		return nil, err
	}
	switch p.HeaderReadMode() {
	case HeaderReadNarrowShadow:
		full, err := p.batchReadHeadersFull(ctx, farmIDs)
		if err != nil {
			return nil, err
		}
		observation := HeaderReadObservation{Mode: HeaderReadNarrowShadow, FarmCount: len(farmIDs)}
		narrow, narrowErr := p.batchReadHeadersNarrow(ctx, farmIDs)
		if narrowErr != nil {
			observation.NarrowUnavailable = true
		} else {
			observation.NarrowUnknown = countUnknownHeaders(narrow)
			observation.MismatchCount = countHeaderMismatches(full, narrow)
		}
		p.observeHeaderRead(observation)
		return full, nil
	case HeaderReadNarrow:
		return p.batchReadHeadersNarrowWithFallback(ctx, farmIDs)
	default:
		rows, err := p.batchReadHeadersFull(ctx, farmIDs)
		p.observeHeaderRead(HeaderReadObservation{Mode: HeaderReadFull, FarmCount: len(farmIDs)})
		return rows, err
	}
}

func (p *Provider) batchReadHeadersFull(ctx context.Context, farmIDs []uint64) ([]HeaderResult, error) {
	rows, err := p.reader.BatchReadModelFields(ctx, actorrouting.DomainFarm, farmIDs, []string{domain.FarmOpportunityHeaderModelField})
	if err != nil {
		return nil, err
	}
	return decodeHeaderRows(farmIDs, rows)
}

func (p *Provider) batchReadHeadersNarrow(ctx context.Context, farmIDs []uint64) ([]HeaderResult, error) {
	rows, err := p.narrowReader.BatchReadOpportunityHeaders(ctx, farmIDs)
	if err != nil {
		return nil, err
	}
	return decodeHeaderRows(farmIDs, rows)
}

func decodeHeaderRows(farmIDs []uint64, rows []authoritystore.ModelFieldReadResult) ([]HeaderResult, error) {
	if len(rows) != len(farmIDs) {
		return nil, errors.New("farm readpath: header batch cardinality mismatch")
	}
	results := make([]HeaderResult, 0, len(rows))
	for index, row := range rows {
		result := HeaderResult{FarmID: row.ActorID, RecordVersion: row.RecordVersion, Status: StatusUnknown, Err: row.Err}
		if row.ActorID != farmIDs[index] {
			result.FarmID = farmIDs[index]
			result.Err = errors.New("farm readpath: header row identity mismatch")
			results = append(results, result)
			continue
		}
		if row.Err != nil {
			results = append(results, result)
			continue
		}
		if !row.Found {
			result.Status = StatusNotFound
			results = append(results, result)
			continue
		}
		data, ok := row.ModelFields[domain.FarmOpportunityHeaderModelField]
		if !ok {
			if row.ModelLayout == authoritystore.ModelLayoutV2 {
				result.Err = errors.New("farm readpath: v2 opportunity header is missing")
			}
			results = append(results, result) // legacy/missing => UNKNOWN
			continue
		}
		if row.ModelLayout == authoritystore.ModelLayoutV2 {
			decoded, decodeErr := fieldv2.Unmarshal(actorrouting.DomainFarm, domain.FarmOpportunityHeaderModelField, data)
			if decodeErr != nil {
				result.Err = decodeErr
				results = append(results, result)
				continue
			}
			data = decoded
		} else if row.ModelLayout != authoritystore.ModelLayoutV1 {
			result.Err = errors.New("farm readpath: unknown model layout")
			results = append(results, result)
			continue
		}
		header, decodeErr := domain.UnmarshalFarmOpportunityHeaderV2(data)
		if decodeErr != nil {
			result.Err = decodeErr
			results = append(results, result)
			continue
		}
		result.Status = StatusFound
		result.Header = header
		results = append(results, result)
	}
	return results, nil
}

func (p *Provider) batchReadHeadersNarrowWithFallback(ctx context.Context, farmIDs []uint64) ([]HeaderResult, error) {
	observation := HeaderReadObservation{Mode: HeaderReadNarrow, FarmCount: len(farmIDs)}
	narrow, err := p.batchReadHeadersNarrow(ctx, farmIDs)
	if err != nil {
		observation.NarrowUnavailable = true
		observation.FullFallbackCount = len(farmIDs)
		p.observeHeaderRead(observation)
		return p.batchReadHeadersFull(ctx, farmIDs)
	}
	unknownIDs := make([]uint64, 0, len(narrow))
	unknownIndexes := make([]int, 0, len(narrow))
	for index, result := range narrow {
		if result.Status == StatusUnknown {
			unknownIDs = append(unknownIDs, result.FarmID)
			unknownIndexes = append(unknownIndexes, index)
		}
	}
	observation.NarrowUnknown = len(unknownIDs)
	if len(unknownIDs) == 0 || len(unknownIDs) > p.maxFullFallback {
		p.observeHeaderRead(observation)
		return narrow, nil
	}
	full, fullErr := p.batchReadHeadersFull(ctx, unknownIDs)
	observation.FullFallbackCount = len(unknownIDs)
	if fullErr == nil && len(full) == len(unknownIndexes) {
		for index, replacement := range full {
			narrow[unknownIndexes[index]] = replacement
		}
	} else if fullErr != nil {
		for _, index := range unknownIndexes {
			narrow[index].Err = errors.Join(narrow[index].Err, fmt.Errorf("farm readpath: complete Header fallback failed: %w", fullErr))
		}
	}
	p.observeHeaderRead(observation)
	return narrow, nil
}

func countUnknownHeaders(results []HeaderResult) int {
	count := 0
	for _, result := range results {
		if result.Status == StatusUnknown {
			count++
		}
	}
	return count
}

func countHeaderMismatches(full, narrow []HeaderResult) int {
	if len(full) != len(narrow) {
		return len(full)
	}
	count := 0
	for index := range full {
		left, right := full[index], narrow[index]
		if left.FarmID != right.FarmID || left.RecordVersion != right.RecordVersion || left.Status != right.Status ||
			(left.Err == nil) != (right.Err == nil) ||
			(left.Status == StatusFound && left.Header != right.Header) {
			count++
		}
	}
	return count
}

func (p *Provider) observeHeaderRead(observation HeaderReadObservation) {
	if p.observer != nil {
		p.observer.ObserveHeaderRead(observation)
	}
}

func (p *Provider) BatchEvaluate(ctx context.Context, viewerPlayerID uint64, farmIDs []uint64, trustedNowMs int64) ([]EvaluationResult, error) {
	if viewerPlayerID == 0 || trustedNowMs <= 0 {
		return nil, errors.New("farm readpath: exact evaluation identity/time is invalid")
	}
	if err := validateBatch(farmIDs); err != nil {
		return nil, err
	}
	rows, err := p.reader.BatchReadModelFields(ctx, actorrouting.DomainFarm, farmIDs, []string{
		domain.FarmOpportunityHeaderModelField, domain.FarmOpportunityFactsModelField,
	})
	if err != nil {
		return nil, err
	}
	if len(rows) != len(farmIDs) {
		return nil, errors.New("farm readpath: facts batch cardinality mismatch")
	}
	results := make([]EvaluationResult, 0, len(rows))
	for _, row := range rows {
		result := EvaluationResult{FarmID: row.ActorID, Status: StatusUnknown, Err: row.Err}
		if row.Err != nil {
			results = append(results, result)
			continue
		}
		if !row.Found {
			result.Status = StatusNotFound
			results = append(results, result)
			continue
		}
		headerBytes, headerFound := row.ModelFields[domain.FarmOpportunityHeaderModelField]
		factsBytes, factsFound := row.ModelFields[domain.FarmOpportunityFactsModelField]
		if !headerFound || !factsFound {
			if row.ModelLayout == authoritystore.ModelLayoutV2 {
				result.Err = errors.New("farm readpath: v2 opportunity fields are incomplete")
			}
			results = append(results, result) // compatibility fallback owns this row
			continue
		}
		if row.ModelLayout == authoritystore.ModelLayoutV2 {
			headerBytes, err = fieldv2.Unmarshal(actorrouting.DomainFarm, domain.FarmOpportunityHeaderModelField, headerBytes)
			if err == nil {
				factsBytes, err = fieldv2.Unmarshal(actorrouting.DomainFarm, domain.FarmOpportunityFactsModelField, factsBytes)
			}
			if err != nil {
				result.Err = err
				results = append(results, result)
				continue
			}
		} else if row.ModelLayout != authoritystore.ModelLayoutV1 {
			result.Err = errors.New("farm readpath: unknown model layout")
			results = append(results, result)
			continue
		}
		header, headerErr := domain.UnmarshalFarmOpportunityHeaderV2(headerBytes)
		facts, factsErr := domain.UnmarshalFarmOpportunityFactsV1(factsBytes)
		if headerErr != nil || factsErr != nil {
			result.Err = errors.Join(headerErr, factsErr)
			results = append(results, result)
			continue
		}
		if header.SourceFarmVersion != facts.SourceFarmVersion {
			result.Err = errors.New("farm readpath: header/facts FarmVersion mismatch")
			results = append(results, result)
			continue
		}
		// A freshly initialized farm has not committed its first business
		// mutation yet, so FarmVersion is legitimately zero. Its complete,
		// empty Header/Facts pair is a known NONE result rather than UNKNOWN.
		// Treating it as UNKNOWN makes a new friend's opportunity endpoint
		// return 503 until that friend performs the first mutation.
		if isGenesisEmptyOpportunity(header, facts) {
			result.Status = StatusFound
			results = append(results, result)
			continue
		}
		evaluation, evaluateErr := domain.EvaluateFarmOpportunityFacts(facts, viewerPlayerID, trustedNowMs)
		if evaluateErr != nil {
			result.Err = evaluateErr
			results = append(results, result)
			continue
		}
		opportunityRevision, revisionErr := domain.FarmOpportunityRevisionAt(facts, trustedNowMs)
		if revisionErr != nil {
			result.Err = revisionErr
			results = append(results, result)
			continue
		}
		result.Status = StatusFound
		result.OpportunityRevision = opportunityRevision
		result.Evaluation = evaluation
		results = append(results, result)
	}
	return results, nil
}

func isGenesisEmptyOpportunity(header domain.FarmOpportunityHeaderV2, facts domain.FarmOpportunityFactsV1) bool {
	return header.SourceFarmVersion == 0 && facts.SourceFarmVersion == 0 && len(facts.Cycles) == 0 &&
		header.NextStealableAtMs == 0 && header.NextWaterAtMs == 0 && header.NextPestAtMs == 0 &&
		header.NextOpportunityAtMs == 0
}

func validateBatch(farmIDs []uint64) error {
	if len(farmIDs) == 0 || len(farmIDs) > MaxBatchFarmIDs {
		return fmt.Errorf("farm readpath: farm batch must contain 1..%d ids", MaxBatchFarmIDs)
	}
	for _, farmID := range farmIDs {
		if farmID == 0 {
			return errors.New("farm readpath: farm id is required")
		}
	}
	return nil
}
