package friend

import (
	"context"
	"fmt"
	"sort"
)

type FriendIndexReconcileReport struct {
	ViewerPlayerID uint64
	CanonicalRows  int
	ProjectedRows  int
	AppliedRows    int
	RemovedRows    int
}

// FriendIndexReconciler repairs both missing and stale projection rows from
// authoritative Quota relation ids plus canonical Pair snapshots. It performs
// bounded reads (the current versioned friend limit), never scans Redis keys, and never
// treats FriendIndex as authorization truth.
type FriendIndexReconciler struct {
	Index  FriendIndexRepository
	Pairs  PairRepository
	ByID   PairRelationReader
	Quotas QuotaRepository
}

func NewFriendIndexReconciler(index FriendIndexRepository, pairs PairRepository, quotas QuotaRepository) (*FriendIndexReconciler, error) {
	byID, ok := pairs.(PairRelationReader)
	if index == nil || pairs == nil || quotas == nil || !ok {
		return nil, fmt.Errorf("%w: friend index reconciler dependencies", ErrInvalidCommand)
	}
	return &FriendIndexReconciler{Index: index, Pairs: pairs, ByID: byID, Quotas: quotas}, nil
}

// FriendIndexInspection reports what a reconcile would have to change.
type FriendIndexInspection struct {
	ViewerPlayerID uint64
	// MissingRows are pairs the canonical authority says are active while the
	// projection either lacks the row or disagrees about it. These are the
	// ones a player notices: the friend is gone from the list.
	MissingRows int
	// ExtraRows are projected rows the canonical authority does not support.
	// A read drops these, so they cost work rather than showing a ghost.
	ExtraRows int
}

func (i FriendIndexInspection) Unrepaired() int { return i.MissingRows + i.ExtraRows }

// InspectViewer reports what ReconcileViewer would change, without changing
// anything.
//
// A pair whose last relation or block change is newer than settledBeforeMs is
// left out. Repair is queued before the pair commits and runs a bounded number
// of rounds, so a disagreement inside that window is work in progress; only one
// that outlived the window is work abandoned. Passing zero inspects every pair,
// which is what a quiesced system wants.
func (r *FriendIndexReconciler) InspectViewer(ctx context.Context, viewer uint64, settledBeforeMs int64) (FriendIndexInspection, error) {
	inspection := FriendIndexInspection{ViewerPlayerID: viewer}
	if r == nil || viewer == 0 {
		return inspection, fmt.Errorf("%w: friend index inspect viewer", ErrInvalidCommand)
	}
	expected, err := r.canonicalRows(ctx, viewer)
	if err != nil {
		return inspection, err
	}
	projected, err := r.projectedRows(ctx, viewer)
	if err != nil {
		return inspection, err
	}
	seen := make(map[uint64]FriendIndexEntry, len(projected))
	for _, entry := range projected {
		seen[entry.FriendPlayerID] = entry
	}
	for peer, want := range expected {
		if !settled(want.pair, settledBeforeMs) {
			continue
		}
		got, found := seen[peer]
		if !found || got.RelationID != want.entry.RelationID || got.RelationEpoch != want.entry.RelationEpoch {
			inspection.MissingRows++
		}
	}
	for _, entry := range projected {
		if _, supported := expected[entry.FriendPlayerID]; supported {
			continue
		}
		pair, readErr := r.Pairs.GetPair(ctx, viewer, entry.FriendPlayerID)
		if readErr != nil {
			return inspection, readErr
		}
		if settled(pair, settledBeforeMs) {
			inspection.ExtraRows++
		}
	}
	return inspection, nil
}

// canonicalRow is what the authority says the projection should hold, together
// with the pair it came from so its age can be judged.
type canonicalRow struct {
	entry FriendIndexEntry
	pair  PairSnapshot
}

// canonicalRows applies the same acceptance test as ReconcileViewer, so the
// audit and the repair cannot disagree about what a correct projection is.
func (r *FriendIndexReconciler) canonicalRows(ctx context.Context, viewer uint64) (map[uint64]canonicalRow, error) {
	quota, err := r.Quotas.GetQuota(ctx, viewer)
	if err != nil {
		return nil, err
	}
	relationIDs := make([]string, 0, len(quota.ActiveRelations))
	for relationID := range quota.ActiveRelations {
		relationIDs = append(relationIDs, relationID)
	}
	sort.Strings(relationIDs)
	rows := make(map[uint64]canonicalRow, len(relationIDs))
	for _, relationID := range relationIDs {
		pair, readErr := r.ByID.GetPairByRelationID(ctx, relationID)
		if readErr != nil {
			return nil, readErr
		}
		if pair.Relation.Status != RelationStatusActive || pair.HasActiveBlock() || pair.Relation.RelationID != relationID {
			continue
		}
		var peer uint64
		switch viewer {
		case pair.LowPlayerID:
			peer = pair.HighPlayerID
		case pair.HighPlayerID:
			peer = pair.LowPlayerID
		default:
			return nil, fmt.Errorf("%w: quota relation does not contain viewer", ErrFriendReconcile)
		}
		rows[peer] = canonicalRow{pair: pair, entry: FriendIndexEntry{
			FriendPlayerID: peer, RelationID: relationID,
			RelationEpoch: pair.Relation.RelationEpoch, ActivatedAtMs: pair.Relation.ActivatedAtMs,
		}}
	}
	return rows, nil
}

func (r *FriendIndexReconciler) projectedRows(ctx context.Context, viewer uint64) ([]FriendIndexEntry, error) {
	projected := make([]FriendIndexEntry, 0, defaultMaxFriends())
	cursor := ""
	for {
		scan, err := r.Index.Scan(ctx, viewer, cursor, MaxPageSize*refillBudgetPages)
		if err != nil {
			return nil, err
		}
		projected = append(projected, scan.Entries...)
		if !scan.HasMore || len(scan.Entries) == 0 {
			return projected, nil
		}
		cursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
}

// settled reports whether every change this pair has to project is older than
// the cutoff. Blocks count: removing a blocked friend from a list is a
// projection the repair owes just as much as an activation.
func settled(pair PairSnapshot, beforeMs int64) bool {
	if beforeMs <= 0 {
		return true
	}
	latest := pair.Relation.ActivatedAtMs
	for _, at := range []int64{
		pair.Relation.DeletedAtMs,
		pair.LowBlocksHigh.CreatedAtMs, pair.LowBlocksHigh.RemovedAtMs,
		pair.HighBlocksLow.CreatedAtMs, pair.HighBlocksLow.RemovedAtMs,
	} {
		if at > latest {
			latest = at
		}
	}
	return latest < beforeMs
}

func (r *FriendIndexReconciler) ReconcileViewer(ctx context.Context, viewer uint64) (FriendIndexReconcileReport, error) {
	report := FriendIndexReconcileReport{ViewerPlayerID: viewer}
	if r == nil || viewer == 0 {
		return report, fmt.Errorf("%w: friend index reconcile viewer", ErrInvalidCommand)
	}
	quota, err := r.Quotas.GetQuota(ctx, viewer)
	if err != nil {
		return report, err
	}
	relationIDs := make([]string, 0, len(quota.ActiveRelations))
	for relationID := range quota.ActiveRelations {
		relationIDs = append(relationIDs, relationID)
	}
	sort.Strings(relationIDs)
	for _, relationID := range relationIDs {
		pair, readErr := r.ByID.GetPairByRelationID(ctx, relationID)
		if readErr != nil {
			return report, readErr
		}
		if pair.Relation.Status != RelationStatusActive || pair.HasActiveBlock() || pair.Relation.RelationID != relationID {
			continue
		}
		var peer uint64
		switch viewer {
		case pair.LowPlayerID:
			peer = pair.HighPlayerID
		case pair.HighPlayerID:
			peer = pair.LowPlayerID
		default:
			return report, fmt.Errorf("%w: quota relation does not contain viewer", ErrFriendReconcile)
		}
		report.CanonicalRows++
		changed, applyErr := r.Index.Apply(ctx, viewer, FriendIndexEntry{FriendPlayerID: peer,
			RelationID: relationID, RelationEpoch: pair.Relation.RelationEpoch, ActivatedAtMs: pair.Relation.ActivatedAtMs})
		if applyErr != nil {
			return report, applyErr
		}
		if changed {
			report.AppliedRows++
		}
	}

	// Snapshot all projected rows before mutating so roster_revision cursor
	// fencing cannot invalidate this reconciliation pass midway.
	projected := make([]FriendIndexEntry, 0, defaultMaxFriends())
	cursor := ""
	for {
		scan, scanErr := r.Index.Scan(ctx, viewer, cursor, MaxPageSize*refillBudgetPages)
		if scanErr != nil {
			return report, scanErr
		}
		projected = append(projected, scan.Entries...)
		if !scan.HasMore || len(scan.Entries) == 0 {
			break
		}
		cursor = cursorAfterEntry(viewer, scan.RosterRevision, scan.Entries[len(scan.Entries)-1])
	}
	report.ProjectedRows = len(projected)
	for _, entry := range projected {
		pair, readErr := r.Pairs.GetPair(ctx, viewer, entry.FriendPlayerID)
		if readErr != nil {
			return report, readErr
		}
		if pair.Relation.Status == RelationStatusActive && !pair.HasActiveBlock() &&
			pair.Relation.RelationID == entry.RelationID && pair.Relation.RelationEpoch == entry.RelationEpoch &&
			pair.CanIssuePermit(viewer, entry.FriendPlayerID) {
			continue
		}
		changed, removeErr := r.Index.Remove(ctx, viewer, entry.FriendPlayerID, entry.RelationID, entry.RelationEpoch)
		if removeErr != nil {
			return report, removeErr
		}
		if changed {
			report.RemovedRows++
		}
	}
	return report, nil
}
