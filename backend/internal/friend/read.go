package friend

import (
	"context"
	"errors"
	"fmt"
	"sync"

	publicpb "farmgame/api/gen/public"
)

const (
	FriendActionCanTheft uint32 = uint32(ActionCanSteal)
	FriendActionCanWater uint32 = uint32(ActionCanWater)
	FriendActionCanPest  uint32 = uint32(ActionCanRemovePest)
	FriendActionCanVisit uint32 = uint32(ActionCanVisit)
)

type OpportunityReadStatus uint8

const (
	OpportunityReadUnknown OpportunityReadStatus = iota
	OpportunityReadReady
)

// FriendIndexScan is a bounded page candidate scan. Entries are projection
// data only and must be batch-validated against Pair authority before use.
type FriendIndexScan struct {
	Entries        []FriendIndexEntry
	RosterRevision uint64
	HasMore        bool
}

// FriendIndexRepository owns the rebuildable per-viewer projection. Apply and
// Remove are epoch-fenced; Scan never authorizes a relationship.
type FriendIndexRepository interface {
	Apply(context.Context, uint64, FriendIndexEntry) (bool, error)
	Remove(context.Context, uint64, uint64, string, uint64) (bool, error)
	Scan(context.Context, uint64, string, int) (FriendIndexScan, error)
}

// FriendIndexEntryReader is a bounded direct projection observation seam. It
// never authorizes friendship; callers must compare the row with Pair.
type FriendIndexEntryReader interface {
	Lookup(context.Context, uint64, uint64) (FriendIndexEntry, bool, error)
}

// FriendIndexBatchReader reads many projection rows in one round trip.
// Missing peers are omitted; callers must still compare each row with Pair.
type FriendIndexBatchReader interface {
	BatchLookup(context.Context, uint64, []uint64) ([]FriendIndexEntry, error)
}

// FriendIndexRevisionReader is the cheap roster-revision fence. It does not
// scan members and does not authorize a relationship.
type FriendIndexRevisionReader interface {
	RosterRevision(context.Context, uint64) (uint64, error)
}

// PairBatchReader is an optional optimized canonical read. ReadAPI falls back
// to bounded concurrent GetPair calls when an adapter has no native batch.
type PairBatchReader interface {
	BatchGetPairs(context.Context, uint64, []uint64) ([]PairSnapshot, error)
}

type PublicProfileBatchReader interface {
	BatchGetPublicProfiles(context.Context, []uint64) (map[uint64]*publicpb.PublicProfileView, error)
}

type FriendTheftTarget struct {
	PlotID      uint32
	CropCycleID string
}

type FriendOpportunityEvaluation struct {
	OwnerPlayerID        uint64
	Status               OpportunityReadStatus
	ActionMask           uint32
	OpportunityRevision  uint64
	EligibleTheftTargets []FriendTheftTarget
}

type FriendOpportunityBatchEvaluator interface {
	BatchEvaluateFriendOpportunity(context.Context, uint64, []uint64, int64) ([]FriendOpportunityEvaluation, error)
}

type FriendPage struct {
	Friends        []FriendCard
	NextCursor     string
	RosterRevision uint64
	ValidatedAtMs  int64
}

// ValidatedFriendRoster is the bounded FriendIndex projection used by the
// opportunity navigator coarse pass. It deliberately omits profiles and farm
// facts. Canonical Pair validation is deferred to the Top-K exact window
// inside Navigator; the selected target is still assembled into a full
// FriendCard afterwards.
type ValidatedFriendRoster struct {
	FarmIDs        []uint64
	RosterRevision uint64
}

type ReadAPI struct {
	Index         FriendIndexRepository
	Pairs         PairRepository
	Profiles      PublicProfileBatchReader
	Opportunities FriendOpportunityBatchEvaluator
	NowMs         func() int64
	PairWorkers   int
}

func NewReadAPI(index FriendIndexRepository, pairs PairRepository, profiles PublicProfileBatchReader,
	opportunities FriendOpportunityBatchEvaluator, nowMs func() int64) (*ReadAPI, error) {
	if index == nil || pairs == nil || profiles == nil || opportunities == nil || nowMs == nil {
		return nil, fmt.Errorf("%w: friend read dependencies", ErrInvalidCommand)
	}
	return &ReadAPI{Index: index, Pairs: pairs, Profiles: profiles, Opportunities: opportunities, NowMs: nowMs, PairWorkers: 8}, nil
}

// GetFriendList executes the formal page path: projection scan, canonical Pair
// batch validation, one profile batch, and one exact opportunity batch. A
// dependency-wide failure fails the page; per-farm read uncertainty remains an
// explicit UNKNOWN card and never becomes an empty/no-opportunity answer.
func (a *ReadAPI) GetFriendList(ctx context.Context, viewer uint64, cursor string, pageSize int) (FriendPage, error) {
	if a == nil || viewer == 0 {
		return FriendPage{}, fmt.Errorf("%w: friend read viewer", ErrInvalidCommand)
	}
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	if pageSize > MaxPageSize {
		pageSize = MaxPageSize
	}
	budget := pageSize * refillBudgetPages
	scan, err := a.Index.Scan(ctx, viewer, cursor, budget)
	if err != nil {
		return FriendPage{}, err
	}
	peerIDs := make([]uint64, len(scan.Entries))
	for i, entry := range scan.Entries {
		peerIDs[i] = entry.FriendPlayerID
	}
	pairs, err := a.batchPairs(ctx, viewer, peerIDs)
	if err != nil {
		return FriendPage{}, err
	}
	if len(pairs) != len(scan.Entries) {
		return FriendPage{}, errors.New("friend: pair batch cardinality mismatch")
	}
	validatedEntries := make([]FriendIndexEntry, 0, pageSize)
	lastScanned := -1
	for i, projected := range scan.Entries {
		lastScanned = i
		pair := pairs[i]
		if pair.Relation.Status != RelationStatusActive || pair.HasActiveBlock() ||
			pair.Relation.RelationID != projected.RelationID || pair.Relation.RelationEpoch != projected.RelationEpoch ||
			!pair.CanIssuePermit(viewer, projected.FriendPlayerID) {
			continue
		}
		validatedEntries = append(validatedEntries, projected)
		if len(validatedEntries) == pageSize {
			break
		}
	}

	nowMs := a.NowMs()
	page := FriendPage{Friends: make([]FriendCard, 0, len(validatedEntries)), RosterRevision: scan.RosterRevision, ValidatedAtMs: nowMs}
	if len(validatedEntries) == 0 {
		if scan.HasMore && lastScanned >= 0 {
			page.NextCursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[lastScanned])
		}
		return page, nil
	}
	validIDs := make([]uint64, len(validatedEntries))
	for i, entry := range validatedEntries {
		validIDs[i] = entry.FriendPlayerID
	}
	profiles, err := a.Profiles.BatchGetPublicProfiles(ctx, validIDs)
	if err != nil {
		return FriendPage{}, err
	}
	evaluations, err := a.Opportunities.BatchEvaluateFriendOpportunity(ctx, viewer, validIDs, nowMs)
	if err != nil {
		return FriendPage{}, err
	}
	if len(evaluations) != len(validIDs) {
		return FriendPage{}, errors.New("friend: opportunity batch cardinality mismatch")
	}
	for i, entry := range validatedEntries {
		profile := profiles[entry.FriendPlayerID]
		if profile == nil || profile.GetPlayerId() != entry.FriendPlayerID {
			return FriendPage{}, errors.New("friend: public profile batch incomplete")
		}
		evaluation := evaluations[i]
		if evaluation.OwnerPlayerID != entry.FriendPlayerID {
			return FriendPage{}, errors.New("friend: opportunity batch order mismatch")
		}
		if evaluation.Status == OpportunityReadReady &&
			(((evaluation.ActionMask & FriendActionCanTheft) != 0) != (len(evaluation.EligibleTheftTargets) > 0)) {
			// One farm's malformed exact detail is uncertainty for that card. Do
			// not fail the whole roster and never expose a mask/target mismatch.
			evaluation.Status = OpportunityReadUnknown
			evaluation.ActionMask = 0
			evaluation.OpportunityRevision = 0
			evaluation.EligibleTheftTargets = nil
		}
		if evaluation.Status != OpportunityReadReady {
			evaluation.EligibleTheftTargets = nil
		}
		mask := FriendActionCanVisit
		if evaluation.Status == OpportunityReadReady {
			mask |= evaluation.ActionMask & (FriendActionCanTheft | FriendActionCanWater | FriendActionCanPest)
		}
		page.Friends = append(page.Friends, FriendCard{
			FriendPlayerID: entry.FriendPlayerID, RelationID: entry.RelationID, RelationEpoch: entry.RelationEpoch,
			ActivatedAtMs: entry.ActivatedAtMs, PublicProfile: profile, Action: ActionMask(mask),
			OpportunityStatus: evaluation.Status, OpportunityRevision: evaluation.OpportunityRevision,
			EligibleTheftTargets: append([]FriendTheftTarget(nil), evaluation.EligibleTheftTargets...), ValidatedAtMs: nowMs,
		})
	}
	if lastScanned >= 0 && (scan.HasMore || lastScanned+1 < len(scan.Entries)) {
		page.NextCursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[lastScanned])
	}
	return page, nil
}

// GetFriendRosterRevision returns the cheap projection revision used only to
// decide whether a cached CandidateWindow must be rebuilt. It does not
// authorize a relationship; the selected card and the final operation still
// revalidate the canonical Pair.
func (a *ReadAPI) GetFriendRosterRevision(ctx context.Context, viewer uint64) (uint64, error) {
	if a == nil || viewer == 0 {
		return 0, fmt.Errorf("%w: friend roster revision", ErrInvalidCommand)
	}
	if reader, ok := a.Index.(FriendIndexRevisionReader); ok {
		return reader.RosterRevision(ctx, viewer)
	}
	scan, err := a.Index.Scan(ctx, viewer, "", 1)
	if err != nil {
		return 0, err
	}
	return scan.RosterRevision, nil
}

// ScanFriendIndexRoster reads the bounded FriendIndex projection without
// loading PublicProfile, canonical Pair, or farm facts. The current product
// limit is 300, so a complete scan is bounded and is used only by
// GET_NEXT_FRIEND_OPPORTUNITY coarse Header sorting. Stale Index rows
// (deleted / blocked) are filtered later by KeepActiveFriends on the Top-K
// exact window.
func (a *ReadAPI) ScanFriendIndexRoster(ctx context.Context, viewer uint64, limit int) (ValidatedFriendRoster, error) {
	if a == nil || viewer == 0 || limit <= 0 {
		return ValidatedFriendRoster{}, fmt.Errorf("%w: friend index roster", ErrInvalidCommand)
	}
	cursor := ""
	result := ValidatedFriendRoster{FarmIDs: make([]uint64, 0, limit)}
	for len(result.FarmIDs) < limit {
		batchLimit := MaxScanBatchSize
		if remaining := limit - len(result.FarmIDs); remaining < batchLimit {
			batchLimit = remaining
		}
		scan, err := a.Index.Scan(ctx, viewer, cursor, batchLimit)
		if err != nil {
			return ValidatedFriendRoster{}, err
		}
		if result.RosterRevision == 0 {
			result.RosterRevision = scan.RosterRevision
		} else if result.RosterRevision != scan.RosterRevision {
			return ValidatedFriendRoster{}, ErrRosterChanged
		}
		if len(scan.Entries) == 0 {
			if scan.HasMore {
				return ValidatedFriendRoster{}, errors.New("friend: empty roster scan has more rows")
			}
			break
		}
		for _, projected := range scan.Entries {
			result.FarmIDs = append(result.FarmIDs, projected.FriendPlayerID)
			if len(result.FarmIDs) == limit {
				break
			}
		}
		if !scan.HasMore || len(result.FarmIDs) == limit {
			break
		}
		cursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
	return result, nil
}

// GetValidatedFriendRoster reads the bounded friendship roster and checks
// every FriendIndex row against the canonical Pair before returning it. Prefer
// ScanFriendIndexRoster for GET_NEXT_FRIEND_OPPORTUNITY so Pair Authority is
// only consulted for the Top-K exact window; this helper remains for callers
// that still need a fully Pair-validated id list up front.
func (a *ReadAPI) GetValidatedFriendRoster(ctx context.Context, viewer uint64, limit int) (ValidatedFriendRoster, error) {
	if a == nil || viewer == 0 || limit <= 0 {
		return ValidatedFriendRoster{}, fmt.Errorf("%w: validated friend roster", ErrInvalidCommand)
	}
	cursor := ""
	result := ValidatedFriendRoster{FarmIDs: make([]uint64, 0, limit)}
	for len(result.FarmIDs) < limit {
		batchLimit := MaxScanBatchSize
		if remaining := limit - len(result.FarmIDs); remaining < batchLimit {
			batchLimit = remaining
		}
		scan, err := a.Index.Scan(ctx, viewer, cursor, batchLimit)
		if err != nil {
			return ValidatedFriendRoster{}, err
		}
		if result.RosterRevision == 0 {
			result.RosterRevision = scan.RosterRevision
		} else if result.RosterRevision != scan.RosterRevision {
			return ValidatedFriendRoster{}, ErrRosterChanged
		}
		if len(scan.Entries) == 0 {
			if scan.HasMore {
				return ValidatedFriendRoster{}, errors.New("friend: empty roster scan has more rows")
			}
			break
		}
		peerIDs := make([]uint64, len(scan.Entries))
		for i, entry := range scan.Entries {
			peerIDs[i] = entry.FriendPlayerID
		}
		pairs, err := a.batchPairs(ctx, viewer, peerIDs)
		if err != nil {
			return ValidatedFriendRoster{}, err
		}
		if len(pairs) != len(scan.Entries) {
			return ValidatedFriendRoster{}, errors.New("friend: pair batch cardinality mismatch")
		}
		for i, projected := range scan.Entries {
			if !pairMatchesProjectedFriend(viewer, projected, pairs[i]) {
				continue
			}
			result.FarmIDs = append(result.FarmIDs, projected.FriendPlayerID)
			if len(result.FarmIDs) == limit {
				break
			}
		}
		if !scan.HasMore || len(result.FarmIDs) == limit {
			break
		}
		cursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
	return result, nil
}

// KeepActiveFriends returns the subset of farmIDs that still match an active,
// unblocked canonical Pair for viewer. Input order is preserved. Missing
// FriendIndex rows and revoked relationships are dropped so Navigator can
// refill the exact window from the remaining Header-sorted tail.
func (a *ReadAPI) KeepActiveFriends(ctx context.Context, viewer uint64, farmIDs []uint64) ([]uint64, error) {
	entries, err := a.KeepActiveFriendEntries(ctx, viewer, farmIDs)
	if err != nil {
		return nil, err
	}
	kept := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		kept = append(kept, entry.FriendPlayerID)
	}
	return kept, nil
}

// KeepActiveFriendEntries is the Top-K Pair filter that also returns the
// matching Index rows so the selected card can reuse them on the same request.
func (a *ReadAPI) KeepActiveFriendEntries(ctx context.Context, viewer uint64, farmIDs []uint64) ([]FriendIndexEntry, error) {
	if a == nil || viewer == 0 {
		return nil, fmt.Errorf("%w: keep active friends", ErrInvalidCommand)
	}
	if len(farmIDs) == 0 {
		return nil, nil
	}
	projected, err := a.lookupIndexEntries(ctx, viewer, farmIDs)
	if err != nil {
		return nil, err
	}
	if len(projected) == 0 {
		return nil, nil
	}
	peers := make([]uint64, len(projected))
	for i, entry := range projected {
		peers[i] = entry.FriendPlayerID
	}
	pairs, err := a.batchPairs(ctx, viewer, peers)
	if err != nil {
		return nil, err
	}
	if len(pairs) != len(peers) {
		return nil, errors.New("friend: pair batch cardinality mismatch")
	}
	kept := make([]FriendIndexEntry, 0, len(peers))
	for i, entry := range projected {
		if !pairMatchesProjectedFriend(viewer, entry, pairs[i]) {
			continue
		}
		kept = append(kept, entry)
	}
	return kept, nil
}

func (a *ReadAPI) lookupIndexEntries(ctx context.Context, viewer uint64, farmIDs []uint64) ([]FriendIndexEntry, error) {
	peers := make([]uint64, 0, len(farmIDs))
	seen := make(map[uint64]struct{}, len(farmIDs))
	for _, farmID := range farmIDs {
		if farmID == 0 || farmID == viewer {
			continue
		}
		if _, ok := seen[farmID]; ok {
			continue
		}
		seen[farmID] = struct{}{}
		peers = append(peers, farmID)
	}
	if len(peers) == 0 {
		return nil, nil
	}
	if reader, ok := a.Index.(FriendIndexBatchReader); ok {
		return reader.BatchLookup(ctx, viewer, peers)
	}
	reader, ok := a.Index.(FriendIndexEntryReader)
	if !ok {
		return nil, errors.New("friend: direct index lookup is unavailable")
	}
	projected := make([]FriendIndexEntry, 0, len(peers))
	for _, farmID := range peers {
		entry, found, err := reader.Lookup(ctx, viewer, farmID)
		if err != nil {
			return nil, err
		}
		if found {
			projected = append(projected, entry)
		}
	}
	return projected, nil
}

func pairMatchesProjectedFriend(viewer uint64, projected FriendIndexEntry, pair PairSnapshot) bool {
	return pair.Relation.Status == RelationStatusActive && !pair.HasActiveBlock() &&
		pair.Relation.RelationID == projected.RelationID &&
		pair.Relation.RelationEpoch == projected.RelationEpoch &&
		pair.CanIssuePermit(viewer, projected.FriendPlayerID)
}

// GetValidatedFriendCard revalidates one navigator target at execution time
// and assembles its profile plus exact opportunity details. A missing/stale
// FriendIndex row or a revoked Pair returns found=false instead of exposing a
// stale recommendation.
func (a *ReadAPI) GetValidatedFriendCard(ctx context.Context, viewer, target uint64) (FriendCard, bool, error) {
	if a == nil || !ValidPair(viewer, target) {
		return FriendCard{}, false, fmt.Errorf("%w: validated friend card", ErrInvalidCommand)
	}
	reader, ok := a.Index.(FriendIndexEntryReader)
	if !ok {
		return FriendCard{}, false, errors.New("friend: direct index lookup is unavailable")
	}
	entry, found, err := reader.Lookup(ctx, viewer, target)
	if err != nil || !found {
		return FriendCard{}, false, err
	}
	pair, err := a.Pairs.GetPair(ctx, viewer, target)
	if err != nil {
		return FriendCard{}, false, err
	}
	if !pairMatchesProjectedFriend(viewer, entry, pair) {
		return FriendCard{}, false, nil
	}
	profiles, err := a.Profiles.BatchGetPublicProfiles(ctx, []uint64{target})
	if err != nil {
		return FriendCard{}, false, err
	}
	profile := profiles[target]
	if profile == nil || profile.GetPlayerId() != target {
		return FriendCard{}, false, errors.New("friend: public profile batch incomplete")
	}
	nowMs := a.NowMs()
	evaluations, err := a.Opportunities.BatchEvaluateFriendOpportunity(ctx, viewer, []uint64{target}, nowMs)
	if err != nil {
		return FriendCard{}, false, err
	}
	if len(evaluations) != 1 || evaluations[0].OwnerPlayerID != target {
		return FriendCard{}, false, errors.New("friend: opportunity target response mismatch")
	}
	evaluation := evaluations[0]
	if evaluation.Status == OpportunityReadReady &&
		(((evaluation.ActionMask & FriendActionCanTheft) != 0) != (len(evaluation.EligibleTheftTargets) > 0)) {
		evaluation = FriendOpportunityEvaluation{OwnerPlayerID: target, Status: OpportunityReadUnknown}
	}
	if evaluation.Status != OpportunityReadReady {
		evaluation.EligibleTheftTargets = nil
	}
	return friendCardFromEvaluation(entry, profile, evaluation, nowMs), true, nil
}

// AssembleFriendCard builds the popup card from Index and Facts already
// validated on this request. It only reads PublicProfile; it does not look up
// Index, Pair, or farm Facts again.
func (a *ReadAPI) AssembleFriendCard(ctx context.Context, viewer uint64, entry FriendIndexEntry, evaluation FriendOpportunityEvaluation) (FriendCard, error) {
	if a == nil || !ValidPair(viewer, entry.FriendPlayerID) || evaluation.OwnerPlayerID != entry.FriendPlayerID {
		return FriendCard{}, fmt.Errorf("%w: assembled friend card", ErrInvalidCommand)
	}
	profiles, err := a.Profiles.BatchGetPublicProfiles(ctx, []uint64{entry.FriendPlayerID})
	if err != nil {
		return FriendCard{}, err
	}
	profile := profiles[entry.FriendPlayerID]
	if profile == nil || profile.GetPlayerId() != entry.FriendPlayerID {
		return FriendCard{}, errors.New("friend: public profile batch incomplete")
	}
	if evaluation.Status == OpportunityReadReady &&
		(((evaluation.ActionMask & FriendActionCanTheft) != 0) != (len(evaluation.EligibleTheftTargets) > 0)) {
		evaluation = FriendOpportunityEvaluation{OwnerPlayerID: entry.FriendPlayerID, Status: OpportunityReadUnknown}
	}
	if evaluation.Status != OpportunityReadReady {
		evaluation.EligibleTheftTargets = nil
	}
	return friendCardFromEvaluation(entry, profile, evaluation, a.NowMs()), nil
}

func friendCardFromEvaluation(entry FriendIndexEntry, profile *publicpb.PublicProfileView, evaluation FriendOpportunityEvaluation, nowMs int64) FriendCard {
	mask := FriendActionCanVisit
	if evaluation.Status == OpportunityReadReady {
		mask |= evaluation.ActionMask & (FriendActionCanTheft | FriendActionCanWater | FriendActionCanPest)
	}
	return FriendCard{
		FriendPlayerID: entry.FriendPlayerID, RelationID: entry.RelationID, RelationEpoch: entry.RelationEpoch,
		ActivatedAtMs: entry.ActivatedAtMs, PublicProfile: profile, Action: ActionMask(mask),
		OpportunityStatus: evaluation.Status, OpportunityRevision: evaluation.OpportunityRevision,
		EligibleTheftTargets: append([]FriendTheftTarget(nil), evaluation.EligibleTheftTargets...), ValidatedAtMs: nowMs,
	}
}

// ReusableFriendOpportunityEvaluation reports whether a same-request Facts
// result can be assembled into a card without another farm RPC.
func ReusableFriendOpportunityEvaluation(eval FriendOpportunityEvaluation) bool {
	if eval.OwnerPlayerID == 0 {
		return false
	}
	if eval.Status == OpportunityReadUnknown {
		return true
	}
	if eval.Status != OpportunityReadReady {
		return false
	}
	hasTheft := eval.ActionMask&FriendActionCanTheft != 0
	return hasTheft == (len(eval.EligibleTheftTargets) > 0)
}

func cursorAfterEntry(viewer, revision uint64, entry FriendIndexEntry) string {
	return ListCursor{ViewerPlayerID: viewer, SortMode: SortModeActivatedDesc, RosterRevision: revision,
		LastActivatedAtMs: entry.ActivatedAtMs, LastFriendPlayerID: entry.FriendPlayerID}.Encode()
}

func (a *ReadAPI) batchPairs(ctx context.Context, viewer uint64, peers []uint64) ([]PairSnapshot, error) {
	if len(peers) == 0 {
		return []PairSnapshot{}, nil
	}
	if batch, ok := a.Pairs.(PairBatchReader); ok {
		return batch.BatchGetPairs(ctx, viewer, peers)
	}
	workers := a.PairWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(peers) {
		workers = len(peers)
	}
	results := make([]PairSnapshot, len(peers))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	worker := func() {
		defer wg.Done()
		for index := range jobs {
			pair, err := a.Pairs.GetPair(ctx, viewer, peers[index])
			if err != nil {
				once.Do(func() { firstErr = err })
				continue
			}
			results[index] = pair
		}
	}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}
	for i := range peers {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// MemoryFriendIndexRepository is the deterministic contract adapter used by
// tests and local composition. Production uses the same repository interface.
type MemoryFriendIndexRepository struct {
	mu      sync.RWMutex
	indexes map[uint64]*FriendIndex
}

func NewMemoryFriendIndexRepository() *MemoryFriendIndexRepository {
	return &MemoryFriendIndexRepository{indexes: make(map[uint64]*FriendIndex)}
}

func (r *MemoryFriendIndexRepository) index(viewer uint64) *FriendIndex {
	index := r.indexes[viewer]
	if index == nil {
		index = NewFriendIndex(viewer)
		r.indexes[viewer] = index
	}
	return index
}

func (r *MemoryFriendIndexRepository) Apply(ctx context.Context, viewer uint64, entry FriendIndexEntry) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if viewer == 0 || entry.FriendPlayerID == 0 || entry.RelationID == "" || entry.RelationEpoch == 0 || entry.ActivatedAtMs <= 0 {
		return false, fmt.Errorf("%w: friend index entry", ErrInvalidCommand)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.index(viewer).ApplyRelationActivated(entry), nil
}

func (r *MemoryFriendIndexRepository) Remove(ctx context.Context, viewer, peer uint64, relationID string, epoch uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.index(viewer).ApplyRelationTerminated(peer, relationID, epoch), nil
}

func (r *MemoryFriendIndexRepository) Lookup(ctx context.Context, viewer, peer uint64) (FriendIndexEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return FriendIndexEntry{}, false, err
	}
	if !ValidPair(viewer, peer) {
		return FriendIndexEntry{}, false, fmt.Errorf("%w: friend index lookup", ErrInvalidCommand)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	index := r.indexes[viewer]
	if index == nil {
		return FriendIndexEntry{}, false, nil
	}
	entry, ok := index.Entries[peer]
	return entry, ok, nil
}

func (r *MemoryFriendIndexRepository) RosterRevision(ctx context.Context, viewer uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if viewer == 0 {
		return 0, fmt.Errorf("%w: friend index revision", ErrInvalidCommand)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	index := r.indexes[viewer]
	if index == nil {
		return 0, nil
	}
	return index.RosterRevision, nil
}

func (r *MemoryFriendIndexRepository) BatchLookup(ctx context.Context, viewer uint64, peers []uint64) ([]FriendIndexEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries := make([]FriendIndexEntry, 0, len(peers))
	seen := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		if !ValidPair(viewer, peer) {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		entry, found, err := r.Lookup(ctx, viewer, peer)
		if err != nil {
			return nil, err
		}
		if found {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (r *MemoryFriendIndexRepository) Scan(ctx context.Context, viewer uint64, cursor string, limit int) (FriendIndexScan, error) {
	if err := ctx.Err(); err != nil {
		return FriendIndexScan{}, err
	}
	if viewer == 0 || limit <= 0 {
		return FriendIndexScan{}, fmt.Errorf("%w: friend index scan", ErrInvalidCommand)
	}
	r.mu.RLock()
	index := r.indexes[viewer]
	if index == nil {
		r.mu.RUnlock()
		return FriendIndexScan{Entries: []FriendIndexEntry{}}, nil
	}
	ordered := index.sortedEntries()
	revision := index.RosterRevision
	r.mu.RUnlock()
	position := ListCursor{}
	if cursor != "" {
		decoded, err := DecodeListCursor(cursor)
		if err != nil || decoded.ViewerPlayerID != viewer || decoded.RosterRevision != revision {
			if err == nil {
				err = ErrRosterChanged
			}
			return FriendIndexScan{}, err
		}
		position = decoded
	}
	start := 0
	if cursor != "" {
		for start < len(ordered) && !cursorAfter(ordered[start], position) {
			start++
		}
	}
	end := start + limit
	if end > len(ordered) {
		end = len(ordered)
	}
	return FriendIndexScan{Entries: append([]FriendIndexEntry(nil), ordered[start:end]...), RosterRevision: revision, HasMore: end < len(ordered)}, nil
}
