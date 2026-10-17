package friend

// Friend opportunity query core (contract 33 ?6.3/6.4, 31 ?11.3-11.5).
//
// P0 uses a CandidateReader backed by Farm Authority Header/Facts. An optional
// FarmOpportunitySummary can implement the same port after A/B evidence, but
// it never changes exact evaluation or final FarmActor adjudication. The
// per-viewer TheftAttempt facts stay inside Farm authority and are never
// copied into FriendRelation or this package.
//
// Continuous navigation keeps a bounded, lossy runtime state: scan cursor,
// candidate window (max 8) and this round's visited set. Visited is marked
// by the server only, after a recommended friend farm reached
// SUBSCRIPTION_READY ? no care/theft success is required (33 ?6.4).
// Nothing here persists per-offline-friend fanout for one mature farm.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	publicpb "farmgame/api/gen/public"
)

// OpportunityMask is the owner-independent coarse filter of one farm.
type OpportunityMask uint32

const (
	OpportunityNone      OpportunityMask = 0
	OpportunityStealable OpportunityMask = 1 << 0
	OpportunityNeedWater OpportunityMask = 1 << 1
	OpportunityHasPest   OpportunityMask = 1 << 2
)

// HasOpportunity reports whether the mask advertises any public chance.
func (m OpportunityMask) HasOpportunity() bool { return m != OpportunityNone }

// FarmOpportunitySummary is the rebuildable projection row (31 ?11.3).
type FarmOpportunitySummary struct {
	FarmID                      uint64 // equals owner player id in P0
	SummaryRevision             uint64
	SourceOpportunityGeneration uint64
	SourceFarmVersion           uint64
	Mask                        OpportunityMask
	NextOpportunityAtMs         int64
	UpdatedAtMs                 int64
}

// IsCandidate implements the coarse filter including the time-compensation
// rule (31 ?11.3): an overdue next_opportunity_at counts as candidate even
// if the summary refresh lags.
func (s FarmOpportunitySummary) IsCandidate(nowMs int64) bool {
	return s.Mask.HasOpportunity() || (s.NextOpportunityAtMs > 0 && s.NextOpportunityAtMs <= nowMs)
}

// ActionMask is the exact per-viewer interaction state of one card.
type ActionMask uint32

const (
	ActionNone          ActionMask = 0
	ActionCanSteal      ActionMask = 1 << 0
	ActionCanWater      ActionMask = 1 << 1
	ActionCanRemovePest ActionMask = 1 << 2
	ActionCanVisit      ActionMask = 1 << 3
	// ActionUnknown marks a card whose exact check timed out. It is never
	// collapsed into ActionNone (33 ?6.3).
	ActionUnknown ActionMask = 1 << 7
)

// Actionable reports whether the viewer can do anything on this farm now.
func (m ActionMask) Actionable() bool {
	return m&(ActionCanSteal|ActionCanWater|ActionCanRemovePest) != 0
}

// SummaryReader is the farm-side coarse projection read port (owned by
// farm-svr; SO0 only consumes it).
type SummaryReader interface {
	BatchGetSummaries(ctx context.Context, farmIDs []uint64) (map[uint64]FarmOpportunitySummary, error)
}

// CandidateState is the coarse, owner-independent read result. UNKNOWN must
// continue to exact evaluation; it can never be collapsed into NONE.
type CandidateState uint8

const (
	CandidateUnknown CandidateState = iota
	CandidateNone
	CandidateYes
)

type FarmOpportunityCandidate struct {
	FarmID            uint64
	State             CandidateState
	SourceFarmVersion uint64
	ActionableAtMs    int64
}

// CandidateReader is the replaceable coarse-filter port. P0 supplies an
// Authority Header implementation; a Summary implementation is optional.
type CandidateReader interface {
	BatchGetCandidates(ctx context.Context, farmIDs []uint64, nowMs int64) (map[uint64]FarmOpportunityCandidate, error)
}

// SummaryCandidateReader adapts the optional Summary projection to the same
// P0 port. Missing Summary is UNKNOWN and therefore falls through to exact
// evaluation instead of hiding a possible opportunity.
type SummaryCandidateReader struct{ summaries SummaryReader }

func NewSummaryCandidateReader(summaries SummaryReader) (*SummaryCandidateReader, error) {
	if summaries == nil {
		return nil, fmt.Errorf("%w: summary reader", ErrInvalidCommand)
	}
	return &SummaryCandidateReader{summaries: summaries}, nil
}

func (r *SummaryCandidateReader) BatchGetCandidates(ctx context.Context, farmIDs []uint64, nowMs int64) (map[uint64]FarmOpportunityCandidate, error) {
	rows, err := r.summaries.BatchGetSummaries(ctx, farmIDs)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]FarmOpportunityCandidate, len(farmIDs))
	for _, farmID := range farmIDs {
		candidate := FarmOpportunityCandidate{FarmID: farmID, State: CandidateUnknown}
		if row, ok := rows[farmID]; ok {
			candidate.SourceFarmVersion = row.SourceFarmVersion
			candidate.ActionableAtMs = row.NextOpportunityAtMs
			if row.IsCandidate(nowMs) {
				candidate.State = CandidateYes
			} else {
				candidate.State = CandidateNone
			}
		}
		result[farmID] = candidate
	}
	return result, nil
}

// ExactEvaluator is the FarmReadAPI.BatchEvaluate port (owned by farm-svr).
// A missing entry in the result map means UNKNOWN for that card.
type ExactEvaluator interface {
	BatchEvaluate(ctx context.Context, viewer uint64, farmIDs []uint64, nowMs int64) (map[uint64]ActionMask, error)
}

// FriendCard is one validated roster card with its exact action state.
type FriendCard struct {
	FriendPlayerID       uint64
	RelationID           string
	RelationEpoch        uint64
	ActivatedAtMs        int64
	Action               ActionMask
	PublicProfile        *publicpb.PublicProfileView
	OpportunityStatus    OpportunityReadStatus
	OpportunityRevision  uint64
	EligibleTheftTargets []FriendTheftTarget
	ValidatedAtMs        int64
}

// EvaluatePage computes the exact action mask for every card of the current
// page (33 ?6.3: all valid cards enter BatchEvaluate, not only the ones the
// summary likes). Missing/timeout entries degrade to ActionUnknown.
func EvaluatePage(ctx context.Context, evaluator ExactEvaluator, viewer uint64, page []FriendIndexEntry, nowMs int64) ([]FriendCard, error) {
	if evaluator == nil {
		return nil, fmt.Errorf("%w: evaluator", ErrInvalidCommand)
	}
	farmIDs := make([]uint64, 0, len(page))
	for _, entry := range page {
		farmIDs = append(farmIDs, entry.FriendPlayerID)
	}
	exact, err := evaluator.BatchEvaluate(ctx, viewer, farmIDs, nowMs)
	if err != nil {
		return nil, err
	}
	cards := make([]FriendCard, 0, len(page))
	for _, entry := range page {
		mask, ok := exact[entry.FriendPlayerID]
		if !ok {
			mask = ActionUnknown
		}
		cards = append(cards, FriendCard{
			FriendPlayerID: entry.FriendPlayerID,
			RelationID:     entry.RelationID,
			RelationEpoch:  entry.RelationEpoch,
			ActivatedAtMs:  entry.ActivatedAtMs,
			Action:         mask,
		})
	}
	return cards, nil
}

// NavigationWindowMode selects the persisted CandidateWindow strategy.
//
// FullTail keeps the complete globally ordered coarse tail so a loaded state
// can answer every subsequent GET_NEXT without re-scanning the roster. TopK
// keeps only the bounded hot window plus at most topK pending entries; when
// that persisted tail is exhausted the Navigator performs a bounded internal
// rescan (excluding visited farms and already confirmed negatives) instead of
// answering NONE. The final answer is always the exact canonical check, so a
// window mode is only a capacity optimization and never an authority change.
type NavigationWindowMode string

const (
	// NavigationWindowModeFullTail is the historical default: the persisted
	// pending tail covers the complete coarse candidate set.
	NavigationWindowModeFullTail NavigationWindowMode = "full-tail"
	// NavigationWindowModeTopK is the grayscale experiment: only the top K
	// coarse candidates survive the payload; a truncated tail forces a
	// bounded rescan when it is exhausted.
	NavigationWindowModeTopK NavigationWindowMode = "top-k"
)

// ParseNavigationWindowMode maps configuration to a mode. Empty and unknown
// values deliberately fail closed to the safe full-tail default so a bad
// rollout can never silently truncate a persisted tail.
func ParseNavigationWindowMode(value string) NavigationWindowMode {
	switch NavigationWindowMode(strings.TrimSpace(value)) {
	case NavigationWindowModeTopK:
		return NavigationWindowModeTopK
	case NavigationWindowModeFullTail, "":
		return NavigationWindowModeFullTail
	default:
		return NavigationWindowModeFullTail
	}
}

// Navigation window/batch bounds (31 ?11.5: scan 32-64 per batch, keep at
// most 8 candidates).
const (
	DefaultScanBatchSize = 32
	MaxScanBatchSize     = 64
	CandidateWindowSize  = 8
	// DefaultTopKWindowSize is the initial grayscale top-k persisted tail
	// limit. The hot window stays CandidateWindowSize; the extra entries are
	// the bounded pending tail that avoids an immediate cold rescan. Ten
	// entries are sufficient for the home popup and reduce persisted state.
	DefaultTopKWindowSize = 10
	// MaxTopKWindowSize caps the grayscale knob. Any configured value above
	// this bound fails closed to the default.
	MaxTopKWindowSize = 128
	// maxRescanAttempts bounds the internal top-k re-scan loop so a hostile
	// or pathological roster can never cause an unbounded cold-scan storm.
	maxRescanAttempts = 3
	// maxExactChecksPerNext bounds exact Facts work and the RelationFilter
	// Pair batch. A small overfetch (2x the hot window) keeps false positives
	// and UNKNOWN rows from immediately forcing a second round-trip.
	maxExactChecksPerNext = CandidateWindowSize * 2
	// maxVisited bounds the lossy visited set. It deliberately exceeds the
	// current product profile while remaining physically bounded.
	maxVisited = 512
)

// CandidateWindowEntry is one server-side navigation candidate. The coarse
// source version fences the cached exact result: when a later coarse read
// observes a different non-zero source version, the cached action is stale
// and must be checked again before the entry can be returned.
type CandidateWindowEntry struct {
	FarmID            uint64
	SourceFarmVersion uint64
	ActionableAtMs    int64
	ActionMask        ActionMask
	ValidatedAtMs     int64
	indexEntry        FriendIndexEntry
}

func newCandidateWindowEntry(candidate FarmOpportunityCandidate) CandidateWindowEntry {
	return CandidateWindowEntry{
		FarmID:            candidate.FarmID,
		SourceFarmVersion: candidate.SourceFarmVersion,
		ActionableAtMs:    candidate.ActionableAtMs,
		ActionMask:        ActionUnknown,
	}
}

// sortCandidateWindowEntries is the one complete-roster order used by
// navigation. Farm ID is the deterministic tie-breaker when the coarse
// actionable time is equal (or unavailable).
func sortCandidateWindowEntries(entries []CandidateWindowEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].ActionableAtMs != entries[j].ActionableAtMs {
			return entries[i].ActionableAtMs > entries[j].ActionableAtMs
		}
		return entries[i].FarmID < entries[j].FarmID
	})
}

func cloneCandidateWindowEntries(entries []CandidateWindowEntry) []CandidateWindowEntry {
	return append([]CandidateWindowEntry(nil), entries...)
}

// VisitFence proves the server-side origin of one visited mark: only a just
// activated friend ActiveSubscription may mark visited (31 ?11.5). H5 never
// submits cursor, visited sets or "mark visited" commands.
type VisitFence struct {
	ViewerPlayerID     uint64
	TargetFarmID       uint64
	ActiveSessionEpoch uint64
	SubscriptionID     string
}

// Navigation is the per-viewer runtime navigation state.
type Navigation struct {
	ViewerPlayerID  uint64
	RosterRevision  uint64
	ScanPosition    int
	Visited         map[uint64]int // farm_id -> visit ordinal
	CandidateWindow []CandidateWindowEntry
	ExpireAtMs      int64
	visitSeq        int
	markedFences    map[string]struct{}

	// WindowMode and WindowSize record the strategy that produced this state.
	// They are part of the persisted payload so a decoded state can restore
	// the exact same bounded behavior on the next request.
	WindowMode NavigationWindowMode
	WindowSize int

	// pendingCandidates is the globally ordered coarse candidate tail that
	// has not yet entered the bounded exact-check window. In TopK mode this
	// tail is bounded by WindowSize; TailComplete records whether it is the
	// full coarse tail or a truncated prefix.
	pendingCandidates []CandidateWindowEntry
	scannedFarmIDs    map[uint64]struct{}
	coarseScanDone    bool
	// TailComplete is false whenever the persisted tail is not the complete
	// coarse candidate tail: a fresh round that has not finished its first
	// full coarse scan, a TopK truncated payload, or an unknown-schema
	// payload that must degrade to a safe cold scan. FullTail states keep
	// the historical true value after the complete coarse scan.
	TailComplete bool
	// exactNegatives are farms exact-checked as ActionNone in this round.
	// TopK rescans skip them so a truncated tail can surface lower-priority
	// candidates. UNKNOWN never enters this set.
	exactNegatives map[uint64]struct{}
	// relationNegatives are farms whose FriendIndex projection failed the
	// canonical Pair check in this round (deleted, blocked, epoch mismatch).
	// They are skipped on TopK rescans the same way exactNegatives are.
	relationNegatives map[uint64]struct{}
}

// NewNavigation creates a fresh navigation round. New rounds start with an
// incomplete tail so a truncated persisted state can never be mistaken for
// a complete one.
func NewNavigation(viewer, rosterRevision uint64, ttlMs, nowMs int64) *Navigation {
	if ttlMs <= 0 {
		ttlMs = 30 * 60 * 1000
	}
	return &Navigation{
		ViewerPlayerID: viewer,
		RosterRevision: rosterRevision,
		Visited:        make(map[uint64]int),
		ExpireAtMs:     nowMs + ttlMs,
		markedFences:   make(map[string]struct{}),
		scannedFarmIDs: make(map[uint64]struct{}),
		WindowMode:     NavigationWindowModeFullTail,
	}
}

// pendingTailLimit is the persisted pending-candidate bound for this state.
// FullTail keeps the historical unbounded tail (0). TopK returns the clamped
// WindowSize so scanCoarseCandidates can drop lower-priority coarse rows and
// mark TailComplete=false for a later bounded rescan.
func (n *Navigation) pendingTailLimit() int {
	if n == nil || n.WindowMode != NavigationWindowModeTopK {
		return 0
	}
	return n.TopKWindowLimit()
}

// MarkVisited records one server-side visited mark after the recommended
// farm reached SUBSCRIPTION_READY (33 ?6.4). It is idempotent per fence and
// never requires care/theft success.
func (n *Navigation) MarkVisited(fence VisitFence) bool {
	if fence.ViewerPlayerID != n.ViewerPlayerID || fence.TargetFarmID == 0 || fence.SubscriptionID == "" {
		return false
	}
	if n.markedFences == nil {
		n.markedFences = make(map[string]struct{})
	}
	if n.Visited == nil {
		n.Visited = make(map[uint64]int)
	}
	key := itoa(fence.ActiveSessionEpoch) + ":" + fence.SubscriptionID + ":" + itoa(fence.TargetFarmID)
	if _, dup := n.markedFences[key]; dup {
		return false
	}
	n.markedFences[key] = struct{}{}
	if _, already := n.Visited[fence.TargetFarmID]; !already {
		n.visitSeq++
		if len(n.Visited) >= maxVisited {
			// Evict the oldest ordinal to stay bounded.
			oldestFarm, oldestOrd := uint64(0), int(^uint(0)>>1)
			for farm, ord := range n.Visited {
				if ord < oldestOrd {
					oldestFarm, oldestOrd = farm, ord
				}
			}
			delete(n.Visited, oldestFarm)
		}
		n.Visited[fence.TargetFarmID] = n.visitSeq
	}
	// Drop the farm from the candidate window immediately.
	kept := n.CandidateWindow[:0]
	for _, entry := range n.CandidateWindow {
		if entry.FarmID != fence.TargetFarmID {
			kept = append(kept, entry)
		}
	}
	n.CandidateWindow = kept
	keptPending := n.pendingCandidates[:0]
	for _, entry := range n.pendingCandidates {
		if entry.FarmID != fence.TargetFarmID {
			keptPending = append(keptPending, entry)
		}
	}
	n.pendingCandidates = keptPending
	return true
}

// NavigationResultStatus is explicit so UNKNOWN cannot be confused with a
// successful FOUND result or an exhausted NONE result.
type NavigationResultStatus uint8

const (
	NavigationResultNone NavigationResultStatus = iota
	NavigationResultFound
	NavigationResultUnknown
)

// NavigationResult is the answer of one GET_NEXT_FRIEND_OPPORTUNITY.
type NavigationResult struct {
	Status  NavigationResultStatus
	Found   bool
	Unknown bool
	FarmID  uint64
	// Exhausted reports the roster was fully scanned without candidates.
	Exhausted bool
	// Evaluation and IndexEntry are filled on Found for the same request so
	// the card assembler can skip a second Index/Pair/Facts round trip.
	Evaluation FriendOpportunityEvaluation
	IndexEntry FriendIndexEntry
}

// Navigator drives continuous friend opportunity navigation.
type Navigator struct {
	Candidates CandidateReader
	Evaluator  ExactEvaluator
	// Relations optionally gates the exact Facts window against canonical
	// Pair Authority. When nil, the roster is trusted as already Pair-valid
	// (unit tests that inject a pre-validated id list). Production cold
	// queries set this so FriendIndex can be Header-sorted before the Top-K
	// Pair batch.
	Relations RelationFilter
	BatchSize int
}

// RelationFilter keeps only farm IDs that still match an active friendship.
// Deleted, blocked, and epoch-mismatched projections must be dropped so the
// navigator can refill from the Header-sorted tail. Input order is preserved.
type RelationFilter interface {
	KeepActiveFriends(ctx context.Context, viewer uint64, farmIDs []uint64) ([]uint64, error)
}

type relationEntryFilter interface {
	KeepActiveFriendEntries(ctx context.Context, viewer uint64, farmIDs []uint64) ([]FriendIndexEntry, error)
}

func keepActiveFriendEntries(ctx context.Context, relations RelationFilter, viewer uint64, farmIDs []uint64) ([]FriendIndexEntry, error) {
	if entries, ok := relations.(relationEntryFilter); ok {
		return entries.KeepActiveFriendEntries(ctx, viewer, farmIDs)
	}
	active, err := relations.KeepActiveFriends(ctx, viewer, farmIDs)
	if err != nil {
		return nil, err
	}
	out := make([]FriendIndexEntry, 0, len(active))
	for _, farmID := range active {
		out = append(out, FriendIndexEntry{FriendPlayerID: farmID})
	}
	return out, nil
}

// NewNavigator binds the query ports.
func NewNavigator(candidates CandidateReader, evaluator ExactEvaluator, batchSize int) (*Navigator, error) {
	if candidates == nil || evaluator == nil {
		return nil, fmt.Errorf("%w: navigator ports", ErrInvalidCommand)
	}
	if batchSize <= 0 {
		batchSize = DefaultScanBatchSize
	}
	if batchSize > MaxScanBatchSize {
		batchSize = MaxScanBatchSize
	}
	return &Navigator{Candidates: candidates, Evaluator: evaluator, BatchSize: batchSize}, nil
}

// Next returns the next actionable friend farm for this round (33 ?6.4):
// first re-check the remaining candidate window exactly, then continue
// scanning the roster in bounded batches, skipping visited farms. Coarse
// Header candidates are collected for the complete FriendIndex roster before
// any Pair or Facts result is returned, which makes the actionable-time order
// global rather than batch-local. When Relations is set, only the Top-K
// Header-sorted prefix is Pair-validated before Facts; stale Index rows are
// dropped and the next sorted batch refills until the window is filled or the
// tail is exhausted. A truncated TopK tail that runs dry triggers a bounded
// internal cold rescan instead of answering NONE.
//
// NONE is only ever returned when the tail that was just exhausted was
// complete at evaluation time: either a loaded complete full-tail/top-k tail,
// or a fresh scan that finished in this call. A truncated tail that is
// exhausted is never answered as NONE; it triggers a bounded rescan and, when
// the rescan budget is used up while the tail is still truncated, answers
// UNKNOWN.
func (v *Navigator) Next(ctx context.Context, nav *Navigation, roster []uint64, rosterRevision uint64, nowMs int64) (NavigationResult, error) {
	if v == nil || nav == nil {
		return NavigationResult{}, fmt.Errorf("%w: navigation", ErrInvalidCommand)
	}
	if rosterRevision != nav.RosterRevision {
		// Roster changed: restart scanning, keep this round's visited set.
		// A roster revision change also clears confirmed exact negatives: a
		// farm that was exact-negative under the previous roster can become
		// actionable under the new roster and must be re-considered.
		nav.RosterRevision = rosterRevision
		nav.resetForRosterRevision()
	}
	if nav.Visited == nil {
		nav.Visited = make(map[uint64]int)
	}
	if nav.scannedFarmIDs == nil {
		nav.scannedFarmIDs = make(map[uint64]struct{})
	}
	for attempt := 0; attempt <= maxRescanAttempts; attempt++ {
		if !nav.coarseScanDone {
			if err := v.scanCoarseCandidates(ctx, nav, roster, nowMs); err != nil {
				return NavigationResult{}, err
			}
		}
		// A cached window must be re-fenced against the latest coarse source
		// before an exact result can be used or returned.
		if err := v.refreshCandidateWindow(ctx, nav, nowMs); err != nil {
			return NavigationResult{}, err
		}
		result, err := v.evaluateCandidateWindow(ctx, nav, nowMs)
		if err != nil {
			return NavigationResult{}, err
		}
		// nav.TailComplete is updated by scanCoarseCandidates and cleared by
		// resetCandidateScan, so at this point it describes the tail that
		// produced this result. A definitive NONE requires a complete tail; a
		// truncated exhausted tail must rescan (bounded) instead.
		if result.Status != NavigationResultNone || nav.TailComplete {
			return result, nil
		}
		// Truncated top-k exhausted. Rebuild the coarse order from the roster
		// while keeping visited marks; never claim a permanent NONE while the
		// tail was incomplete.
		if len(roster) == 0 || attempt == maxRescanAttempts {
			return NavigationResult{Status: NavigationResultUnknown, Unknown: true}, nil
		}
		nav.resetCandidateScan()
	}
	return NavigationResult{Status: NavigationResultUnknown, Unknown: true}, nil
}

func (n *Navigation) resetCandidateScan() {
	n.ScanPosition = 0
	n.CandidateWindow = nil
	n.pendingCandidates = nil
	n.scannedFarmIDs = make(map[uint64]struct{})
	n.coarseScanDone = false
	n.TailComplete = false
}

// resetForRosterRevision restarts the scan for a new roster revision.
// Same-revision top-k rescans preserve exactNegatives and relationNegatives;
// a revision change must drop them because the coarse source changed
// underneath the round.
func (n *Navigation) resetForRosterRevision() {
	n.resetCandidateScan()
	n.exactNegatives = nil
	n.relationNegatives = nil
}

// NeedsRosterScan reports whether the complete bounded roster must be read
// before Next can return a globally ordered result. A matching completed scan
// can reuse CandidateWindow; final relationship and farm checks are still
// performed for every selected target. A truncated top-k tail also requires
// the roster so the internal rescan can rebuild the coarse order.
func (n *Navigation) NeedsRosterScan(rosterRevision uint64) bool {
	return n == nil || n.RosterRevision != rosterRevision || !n.coarseScanDone || !n.TailComplete
}

// ReconcileWindowMode reconciles a loaded persisted tail with the active
// configuration. A persisted top-k payload is only valid while the process
// still serves top-k; switching to empty/unknown/full-tail (or back to top-k
// with a different size) must never serve a stale or truncated tail as the
// complete answer. The rebuild preserves the lossy visited marks and fences.
func (n *Navigation) ReconcileWindowMode(active NavigationWindowMode, activeSize int) *Navigation {
	if n == nil {
		return nil
	}
	matches := n.WindowMode == active
	if matches && n.WindowMode == NavigationWindowModeTopK && n.WindowSize != activeSize {
		matches = false
	}
	if matches {
		return n
	}
	rebuild := &Navigation{
		ViewerPlayerID: n.ViewerPlayerID,
		RosterRevision: n.RosterRevision,
		Visited:        cloneNavigationVisited(n.Visited),
		visitSeq:       n.visitSeq,
		markedFences:   make(map[string]struct{}, len(n.markedFences)),
		WindowMode:     active,
		WindowSize:     activeSize,
		scannedFarmIDs: make(map[uint64]struct{}),
	}
	for fence := range n.markedFences {
		rebuild.markedFences[fence] = struct{}{}
	}
	if n.ExpireAtMs > 0 {
		rebuild.ExpireAtMs = n.ExpireAtMs
	}
	return rebuild
}

// InvalidateCandidates makes the next request rebuild the complete coarse
// order while preserving this round's visited set. Services use it after an
// exhausted answer so a later maturity/hint can be discovered without
// waiting for the lossy navigation TTL to expire.
func (n *Navigation) InvalidateCandidates() {
	if n == nil {
		return
	}
	n.resetCandidateScan()
	// Drop confirmed exact and relation negatives so a later maturity or
	// re-added friendship can re-enter the coarse order. UNKNOWN was never
	// recorded here.
	n.exactNegatives = nil
	n.relationNegatives = nil
}

// TopKWindowLimit returns the persisted pending-tail bound for this
// navigation state. FullTail keeps the historical unbounded tail; TopK uses
// WindowSize clamped to the supported maximum, falling back to the default
// when the decoded value is out of range.
func (n *Navigation) TopKWindowLimit() int {
	if n == nil || n.WindowMode != NavigationWindowModeTopK {
		return 0
	}
	if n.WindowSize <= 0 || n.WindowSize > MaxTopKWindowSize {
		return DefaultTopKWindowSize
	}
	return n.WindowSize
}

// DiscardCandidate removes a target that failed the final relationship/card
// revalidation. A normal FOUND response must not call this: recommendations
// remain stable until SUBSCRIPTION_READY marks the farm visited.
func (n *Navigation) DiscardCandidate(farmID uint64) {
	if n == nil || farmID == 0 {
		return
	}
	window := n.CandidateWindow[:0]
	for _, entry := range n.CandidateWindow {
		if entry.FarmID != farmID {
			window = append(window, entry)
		}
	}
	n.CandidateWindow = window
	pending := n.pendingCandidates[:0]
	for _, entry := range n.pendingCandidates {
		if entry.FarmID != farmID {
			pending = append(pending, entry)
		}
	}
	n.pendingCandidates = pending
}

// HasVisited reports whether this navigation round already entered farmID.
// Ordinary next-opportunity scans skip visited farms so the player can walk
// the remaining window. A later real-time Hint may still ClearVisited when
// the farm is actionable again (WATER visit, then THEFT at mature).
func (n *Navigation) HasVisited(farmID uint64) bool {
	if n == nil || farmID == 0 {
		return false
	}
	_, visited := n.Visited[farmID]
	return visited
}

// ClearVisited drops a farm from the current-round visited set. Hint
// revalidation calls this before UpsertValidatedCandidate so a new boundary
// can occupy the home popup after a prior visit.
func (n *Navigation) ClearVisited(farmID uint64) {
	if n == nil || farmID == 0 || n.Visited == nil {
		return
	}
	delete(n.Visited, farmID)
}

// UpsertValidatedCandidate merges one viewer-exact Hint result into the same
// bounded hot window used by normal navigation. The caller has already
// validated friendship, profile and farm facts. Keeping this mutation inside
// Navigation means the repository CAS publishes the popup answer and its
// subsequent navigation state together. In TopK mode the hot window remains
// CandidateWindowSize and the bounded top-k pendingTailLimit holds overflow.
func (n *Navigation) UpsertValidatedCandidate(entry CandidateWindowEntry) bool {
	if n == nil || entry.FarmID == 0 || entry.ValidatedAtMs <= 0 || !entry.ActionMask.Actionable() || n.HasVisited(entry.FarmID) {
		return false
	}
	n.DiscardCandidate(entry.FarmID)
	n.CandidateWindow = append(n.CandidateWindow, entry)
	sortCandidateWindowEntries(n.CandidateWindow)
	if len(n.CandidateWindow) > CandidateWindowSize {
		overflow := cloneCandidateWindowEntries(n.CandidateWindow[CandidateWindowSize:])
		n.CandidateWindow = cloneCandidateWindowEntries(n.CandidateWindow[:CandidateWindowSize])
		n.pendingCandidates = append(overflow, n.pendingCandidates...)
		sortCandidateWindowEntries(n.pendingCandidates)
		if limit := n.pendingTailLimit(); limit > 0 && len(n.pendingCandidates) > limit {
			n.pendingCandidates = cloneCandidateWindowEntries(n.pendingCandidates[:limit])
			n.TailComplete = false
		}
	}
	return true
}

func (v *Navigator) scanCoarseCandidates(ctx context.Context, nav *Navigation, roster []uint64, nowMs int64) error {
	for nav.ScanPosition < len(roster) {
		end := nav.ScanPosition + v.BatchSize
		if end > len(roster) {
			end = len(roster)
		}
		batch := make([]uint64, 0, end-nav.ScanPosition)
		for _, farmID := range roster[nav.ScanPosition:end] {
			if _, visited := nav.Visited[farmID]; visited {
				continue
			}
			if _, rejected := nav.exactNegatives[farmID]; rejected {
				continue
			}
			if _, rejected := nav.relationNegatives[farmID]; rejected {
				continue
			}
			if _, scanned := nav.scannedFarmIDs[farmID]; scanned {
				continue
			}
			batch = append(batch, farmID)
		}
		if len(batch) == 0 {
			nav.ScanPosition = end
			continue
		}
		coarse, err := v.Candidates.BatchGetCandidates(ctx, batch, nowMs)
		if err != nil {
			// Keep ScanPosition and scannedFarmIDs unchanged so a retry does
			// not silently lose the failed batch.
			return err
		}
		for _, farmID := range batch {
			nav.scannedFarmIDs[farmID] = struct{}{}
			candidate, ok := coarse[farmID]
			if !ok {
				candidate = FarmOpportunityCandidate{FarmID: farmID, State: CandidateUnknown}
			} else {
				// The request key, not an untrusted row field, identifies the
				// candidate in this roster.
				candidate.FarmID = farmID
			}
			switch candidate.State {
			case CandidateNone:
				// A known coarse negative needs no exact check.
			case CandidateYes, CandidateUnknown:
				nav.pendingCandidates = append(nav.pendingCandidates, newCandidateWindowEntry(candidate))
			default:
				// Invalid states fail open to exact UNKNOWN rather than
				// silently hiding a possible opportunity.
				candidate.State = CandidateUnknown
				nav.pendingCandidates = append(nav.pendingCandidates, newCandidateWindowEntry(candidate))
			}
		}
		nav.ScanPosition = end
	}
	sortCandidateWindowEntries(nav.pendingCandidates)
	// TopK keeps only the globally ordered prefix. Overflowing drops lower-
	// priority coarse candidates that were never exact-checked; TailComplete
	// stays false so an exhausted window triggers a bounded rescan instead of
	// a false NONE. FullTail never truncates here.
	truncated := false
	if limit := nav.pendingTailLimit(); limit > 0 && len(nav.pendingCandidates) > limit {
		nav.pendingCandidates = cloneCandidateWindowEntries(nav.pendingCandidates[:limit])
		truncated = true
	}
	nav.coarseScanDone = true
	nav.TailComplete = !truncated
	return nil
}

func (v *Navigator) refreshCandidateWindow(ctx context.Context, nav *Navigation, nowMs int64) error {
	if len(nav.CandidateWindow) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(nav.CandidateWindow))
	for _, entry := range nav.CandidateWindow {
		ids = append(ids, entry.FarmID)
	}
	current, err := v.Candidates.BatchGetCandidates(ctx, ids, nowMs)
	if err != nil {
		return err
	}
	refreshed := make([]CandidateWindowEntry, 0, len(nav.CandidateWindow))
	for _, entry := range nav.CandidateWindow {
		candidate, ok := current[entry.FarmID]
		if !ok {
			// A missing coarse row is UNKNOWN, not a negative. The old
			// metadata remains useful for ordering, but its exact result is
			// no longer trusted until rechecked.
			entry.ActionMask = ActionUnknown
			entry.ValidatedAtMs = 0
			refreshed = append(refreshed, entry)
			continue
		}
		candidate.FarmID = entry.FarmID
		switch candidate.State {
		case CandidateNone:
			// The source now proves that this old candidate is no longer
			// coarse-positive.
			continue
		case CandidateYes:
			if sourceFarmVersionChanged(entry.SourceFarmVersion, candidate.SourceFarmVersion) {
				entry.ActionMask = ActionUnknown
				entry.ValidatedAtMs = 0
			}
			if candidate.SourceFarmVersion != 0 {
				entry.SourceFarmVersion = candidate.SourceFarmVersion
			}
			entry.ActionableAtMs = candidate.ActionableAtMs
		case CandidateUnknown:
			if sourceFarmVersionChanged(entry.SourceFarmVersion, candidate.SourceFarmVersion) {
				entry.ActionMask = ActionUnknown
				entry.ValidatedAtMs = 0
			}
			if candidate.SourceFarmVersion != 0 {
				entry.SourceFarmVersion = candidate.SourceFarmVersion
			}
			if candidate.ActionableAtMs != 0 {
				entry.ActionableAtMs = candidate.ActionableAtMs
			}
			// Coarse uncertainty invalidates a cached exact answer even when
			// no version was available to compare.
			entry.ActionMask = ActionUnknown
			entry.ValidatedAtMs = 0
		default:
			entry.ActionMask = ActionUnknown
			entry.ValidatedAtMs = 0
		}
		refreshed = append(refreshed, entry)
	}
	sortCandidateWindowEntries(refreshed)
	nav.CandidateWindow = refreshed
	return nil
}

func sourceFarmVersionChanged(previous, current uint64) bool {
	return previous != 0 && current != 0 && previous != current
}

func (v *Navigator) evaluateCandidateWindow(ctx context.Context, nav *Navigation, nowMs int64) (NavigationResult, error) {
	if len(nav.CandidateWindow) > CandidateWindowSize {
		overflow := cloneCandidateWindowEntries(nav.CandidateWindow[CandidateWindowSize:])
		nav.CandidateWindow = cloneCandidateWindowEntries(nav.CandidateWindow[:CandidateWindowSize])
		nav.pendingCandidates = append(overflow, nav.pendingCandidates...)
		sortCandidateWindowEntries(nav.pendingCandidates)
	}
	if len(nav.CandidateWindow) == 0 && len(nav.pendingCandidates) == 0 {
		return NavigationResult{
			Status: NavigationResultNone, Exhausted: true,
		}, nil
	}

	source := cloneCandidateWindowEntries(nav.CandidateWindow)
	source = append(source, nav.pendingCandidates...)
	sortCandidateWindowEntries(source)

	work, remainder, err := v.selectExactCheckCandidates(ctx, nav, source)
	if err != nil {
		return NavigationResult{}, err
	}
	if len(work) == 0 {
		nav.CandidateWindow = nil
		nav.pendingCandidates = remainder
		if len(nav.pendingCandidates) > 0 {
			return NavigationResult{
				Status: NavigationResultUnknown, Unknown: true,
			}, nil
		}
		return NavigationResult{
			Status: NavigationResultNone, Exhausted: true,
		}, nil
	}

	ids := make([]uint64, 0, len(work))
	for _, entry := range work {
		ids = append(ids, entry.FarmID)
	}
	exact, evaluations, err := v.evaluateExactWindow(ctx, nav.ViewerPlayerID, ids, nowMs)
	if err != nil {
		return NavigationResult{}, err
	}

	foundIndex := -1
	unknownSeen := false
	retained := make([]CandidateWindowEntry, 0, len(work))
	for i := range work {
		mask, ok := exact[work[i].FarmID]
		if !ok {
			mask = ActionUnknown
		}
		work[i].ActionMask = mask
		work[i].ValidatedAtMs = nowMs
		if mask&ActionUnknown != 0 {
			unknownSeen = true
			continue
		}
		if foundIndex == -1 && !unknownSeen && mask.Actionable() {
			foundIndex = i
		}
	}
	for _, entry := range work {
		if entry.ActionMask&ActionUnknown != 0 || entry.ActionMask.Actionable() {
			retained = append(retained, entry)
			continue
		}
		// Confirmed exact negative for this round. Keep it out of TopK
		// rescans; UNKNOWN must never become a permanent negative.
		if nav.exactNegatives == nil {
			nav.exactNegatives = make(map[uint64]struct{})
		}
		nav.exactNegatives[entry.FarmID] = struct{}{}
	}

	sortCandidateWindowEntries(retained)
	if len(retained) > CandidateWindowSize {
		overflow := cloneCandidateWindowEntries(retained[CandidateWindowSize:])
		retained = cloneCandidateWindowEntries(retained[:CandidateWindowSize])
		remainder = append(overflow, remainder...)
	}
	nav.CandidateWindow = retained
	nav.pendingCandidates = remainder
	sortCandidateWindowEntries(nav.pendingCandidates)

	if foundIndex >= 0 {
		evaluation := evaluations[work[foundIndex].FarmID]
		if evaluation.OwnerPlayerID == 0 {
			evaluation.OwnerPlayerID = work[foundIndex].FarmID
			evaluation.Status = OpportunityReadReady
			evaluation.ActionMask = uint32(work[foundIndex].ActionMask)
		}
		return NavigationResult{
			Status:     NavigationResultFound,
			Found:      true,
			FarmID:     work[foundIndex].FarmID,
			Evaluation: evaluation,
			IndexEntry: work[foundIndex].indexEntry,
		}, nil
	}
	if len(nav.CandidateWindow) > 0 || len(nav.pendingCandidates) > 0 {
		// Either an exact check is UNKNOWN, or there are still coarse
		// candidates not exact-checked because of the overfetch bound.
		return NavigationResult{
			Status: NavigationResultUnknown, Unknown: true,
		}, nil
	}
	return NavigationResult{
		Status: NavigationResultNone, Exhausted: true,
	}, nil
}

// selectExactCheckCandidates builds the bounded Facts work set. Without a
// RelationFilter the historical window+pending overfetch is preserved. With
// Relations, only active friendships enter Facts: each Pair batch is at most
// maxExactChecksPerNext, stale Index rows are recorded as relationNegatives,
// and the next Header-sorted batch refills until the exact budget is filled
// or the sorted source is exhausted.
func (v *Navigator) selectExactCheckCandidates(
	ctx context.Context,
	nav *Navigation,
	source []CandidateWindowEntry,
) ([]CandidateWindowEntry, []CandidateWindowEntry, error) {
	if v.Relations == nil {
		windowCount := len(nav.CandidateWindow)
		if windowCount > CandidateWindowSize {
			windowCount = CandidateWindowSize
		}
		pendingTaken := maxExactChecksPerNext - windowCount
		if pendingTaken < 0 {
			pendingTaken = 0
		}
		if pendingTaken > len(nav.pendingCandidates) {
			pendingTaken = len(nav.pendingCandidates)
		}
		work := cloneCandidateWindowEntries(nav.CandidateWindow)
		if len(work) > CandidateWindowSize {
			work = cloneCandidateWindowEntries(work[:CandidateWindowSize])
		}
		work = append(work, nav.pendingCandidates[:pendingTaken]...)
		sortCandidateWindowEntries(work)
		remainder := cloneCandidateWindowEntries(nav.pendingCandidates[pendingTaken:])
		return work, remainder, nil
	}

	kept := make([]CandidateWindowEntry, 0, maxExactChecksPerNext)
	rejected := make(map[uint64]struct{})
	offset := 0
	for len(kept) < maxExactChecksPerNext && offset < len(source) {
		batchSize := maxExactChecksPerNext
		if remaining := len(source) - offset; remaining < batchSize {
			batchSize = remaining
		}
		batch := source[offset : offset+batchSize]
		offset += batchSize
		ids := make([]uint64, 0, len(batch))
		for _, entry := range batch {
			ids = append(ids, entry.FarmID)
		}
		activeEntries, err := keepActiveFriendEntries(ctx, v.Relations, nav.ViewerPlayerID, ids)
		if err != nil {
			return nil, nil, err
		}
		activeSet := make(map[uint64]FriendIndexEntry, len(activeEntries))
		for _, entry := range activeEntries {
			activeSet[entry.FriendPlayerID] = entry
		}
		for _, entry := range batch {
			indexEntry, ok := activeSet[entry.FarmID]
			if !ok {
				rejected[entry.FarmID] = struct{}{}
				continue
			}
			if len(kept) < maxExactChecksPerNext {
				entry.indexEntry = indexEntry
				kept = append(kept, entry)
			}
		}
	}
	if len(rejected) > 0 {
		if nav.relationNegatives == nil {
			nav.relationNegatives = make(map[uint64]struct{}, len(rejected))
		}
		for farmID := range rejected {
			nav.relationNegatives[farmID] = struct{}{}
		}
	}
	remainder := make([]CandidateWindowEntry, 0, len(source)-len(kept))
	keptSet := make(map[uint64]struct{}, len(kept))
	for _, entry := range kept {
		keptSet[entry.FarmID] = struct{}{}
	}
	for _, entry := range source {
		if _, drop := rejected[entry.FarmID]; drop {
			continue
		}
		if _, taken := keptSet[entry.FarmID]; taken {
			continue
		}
		remainder = append(remainder, entry)
	}
	sortCandidateWindowEntries(kept)
	sortCandidateWindowEntries(remainder)
	return kept, remainder, nil
}

func (v *Navigator) evaluateExactWindow(ctx context.Context, viewer uint64, ids []uint64, nowMs int64) (map[uint64]ActionMask, map[uint64]FriendOpportunityEvaluation, error) {
	evaluations := make(map[uint64]FriendOpportunityEvaluation, len(ids))
	if batch, ok := v.Evaluator.(FriendOpportunityBatchEvaluator); ok {
		rows, err := batch.BatchEvaluateFriendOpportunity(ctx, viewer, ids, nowMs)
		if err != nil {
			return nil, nil, err
		}
		if len(rows) != len(ids) {
			return nil, nil, errors.New("friend: opportunity exact batch cardinality mismatch")
		}
		exact := make(map[uint64]ActionMask, len(rows))
		for i, row := range rows {
			if row.OwnerPlayerID != ids[i] {
				return nil, nil, errors.New("friend: opportunity exact batch order mismatch")
			}
			evaluations[row.OwnerPlayerID] = row
			if row.Status != OpportunityReadReady {
				exact[row.OwnerPlayerID] = ActionUnknown
				continue
			}
			exact[row.OwnerPlayerID] = ActionMask(row.ActionMask)
		}
		return exact, evaluations, nil
	}
	exact, err := v.Evaluator.BatchEvaluate(ctx, viewer, ids, nowMs)
	if err != nil {
		return nil, nil, err
	}
	return exact, evaluations, nil
}
