package friend

import (
	"context"
	"testing"
)

type fakeSummaries struct {
	rows map[uint64]FarmOpportunitySummary
}

func (f fakeSummaries) BatchGetSummaries(ctx context.Context, farmIDs []uint64) (map[uint64]FarmOpportunitySummary, error) {
	out := make(map[uint64]FarmOpportunitySummary, len(farmIDs))
	for _, id := range farmIDs {
		if row, ok := f.rows[id]; ok {
			out[id] = row
		}
	}
	return out, nil
}

type fakeCandidateReader struct {
	rows  map[uint64]FarmOpportunityCandidate
	calls [][]uint64
}

func (f *fakeCandidateReader) BatchGetCandidates(ctx context.Context, farmIDs []uint64, nowMs int64) (map[uint64]FarmOpportunityCandidate, error) {
	f.calls = append(f.calls, append([]uint64(nil), farmIDs...))
	out := make(map[uint64]FarmOpportunityCandidate, len(farmIDs))
	for _, id := range farmIDs {
		if row, ok := f.rows[id]; ok {
			out[id] = row
		}
	}
	return out, nil
}

func summaryCandidates(t *testing.T, summaries SummaryReader) CandidateReader {
	t.Helper()
	reader, err := NewSummaryCandidateReader(summaries)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

type fakeEvaluator struct {
	exact map[uint64]ActionMask
	// calls records every BatchEvaluate farm set for assertions.
	calls [][]uint64
}

func (f *fakeEvaluator) BatchEvaluate(ctx context.Context, viewer uint64, farmIDs []uint64, nowMs int64) (map[uint64]ActionMask, error) {
	f.calls = append(f.calls, append([]uint64(nil), farmIDs...))
	out := make(map[uint64]ActionMask, len(farmIDs))
	for _, id := range farmIDs {
		if mask, ok := f.exact[id]; ok {
			out[id] = mask
		}
	}
	return out, nil
}

func TestEvaluatePageExactForAllCards(t *testing.T) {
	page := []FriendIndexEntry{
		{FriendPlayerID: 1, RelationID: "r1", RelationEpoch: 1},
		{FriendPlayerID: 2, RelationID: "r2", RelationEpoch: 1},
		{FriendPlayerID: 3, RelationID: "r3", RelationEpoch: 1},
	}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{
		1: ActionCanSteal,
		2: ActionNone,
		// 3 missing: simulates a per-card timeout.
	}}
	cards, err := EvaluatePage(context0, eval, 10001, page, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 3 {
		t.Fatalf("cards = %d", len(cards))
	}
	if cards[0].Action != ActionCanSteal || cards[1].Action != ActionNone {
		t.Fatalf("cards = %+v", cards)
	}
	if cards[2].Action != ActionUnknown {
		t.Fatalf("timeout card must be UNKNOWN, got %v", cards[2].Action)
	}
	// Every card entered BatchEvaluate, not only summary-positive ones.
	if len(eval.calls) != 1 || len(eval.calls[0]) != 3 {
		t.Fatalf("evaluate calls = %+v", eval.calls)
	}
}

func TestNavigationSummaryFalsePositiveExcluded(t *testing.T) {
	roster := []uint64{11, 12, 13, 14}
	summaries := fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, Mask: OpportunityStealable}, // false positive
		12: {FarmID: 12, Mask: OpportunityStealable}, // true positive
		13: {FarmID: 13, Mask: OpportunityNone},      // coarse negative
		14: {FarmID: 14, Mask: OpportunityNeedWater}, // true positive
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{
		11: ActionNone, // exact check disproves the summary
		12: ActionCanSteal,
		14: ActionCanWater,
	}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(summaryCandidates(t, summaries), eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.FarmID != 12 {
		t.Fatalf("next = %+v, want farm 12", result)
	}
	// False positive 11 and true candidate 12 both went through exact check.
	if len(eval.calls) == 0 {
		t.Fatal("BatchEvaluate never called")
	}
}

func TestNavigationVisitedRound(t *testing.T) {
	roster := []uint64{11, 12, 13}
	summaries := fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, Mask: OpportunityStealable},
		12: {FarmID: 12, Mask: OpportunityStealable},
		13: {FarmID: 13, Mask: OpportunityStealable},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{
		11: ActionCanSteal, 12: ActionCanSteal, 13: ActionCanSteal,
	}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, _ := NewNavigator(summaryCandidates(t, summaries), eval, 2)

	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !first.Found {
		t.Fatalf("first = %+v %v", first, err)
	}
	// Player enters the recommended farm and reaches SUBSCRIPTION_READY; no
	// care/theft success is required (33 §6.4).
	if !nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: first.FarmID, ActiveSessionEpoch: 3, SubscriptionID: "sub-1"}) {
		t.Fatal("mark visited failed")
	}
	// Idempotent per fence.
	if nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: first.FarmID, ActiveSessionEpoch: 3, SubscriptionID: "sub-1"}) {
		t.Fatal("duplicate fence must not re-mark")
	}
	// Back home: the next recommendation excludes the visited farm.
	second, err := navigator.Next(context0, nav, roster, 1, 1100)
	if err != nil || !second.Found {
		t.Fatalf("second = %+v %v", second, err)
	}
	if second.FarmID == first.FarmID {
		t.Fatalf("visited farm %d recommended again", first.FarmID)
	}
	// Visiting everything exhausts the round deterministically.
	nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: second.FarmID, ActiveSessionEpoch: 3, SubscriptionID: "sub-2"})
	third, err := navigator.Next(context0, nav, roster, 1, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if third.Found {
		nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: third.FarmID, ActiveSessionEpoch: 3, SubscriptionID: "sub-3"})
		third, err = navigator.Next(context0, nav, roster, 1, 1300)
		if err != nil {
			t.Fatal(err)
		}
	}
	if third.Found || !third.Exhausted {
		t.Fatalf("exhausted round = %+v", third)
	}
	// H5 can never mark visited: fences require server fields.
	if nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 11}) {
		t.Fatal("fence without subscription must be rejected")
	}
}

func TestNavigationRosterChangeResetsScanButKeepsVisited(t *testing.T) {
	roster := []uint64{11, 12}
	summaries := fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, Mask: OpportunityStealable},
		12: {FarmID: 12, Mask: OpportunityStealable},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{11: ActionCanSteal, 12: ActionCanSteal}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, _ := NewNavigator(summaryCandidates(t, summaries), eval, 1)
	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !first.Found {
		t.Fatal(first)
	}
	nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: first.FarmID, ActiveSessionEpoch: 1, SubscriptionID: "s1"})
	// Roster revision changes: scan restarts but this round's visited set is
	// preserved, so the visited farm is not re-recommended.
	result, err := navigator.Next(context0, nav, roster, 2, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.FarmID == first.FarmID {
		t.Fatalf("after roster change = %+v (first was %d)", result, first.FarmID)
	}
}

func TestNavigationUnknownCandidateNotHidden(t *testing.T) {
	roster := []uint64{11}
	summaries := fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, Mask: OpportunityStealable},
	}}
	// Exact evaluator times out for farm 11: UNKNOWN is explicit, never a
	// successful FOUND result or an exhausted NONE result.
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, _ := NewNavigator(summaryCandidates(t, summaries), eval, 4)
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Found || !result.Unknown || result.Exhausted || result.Status != NavigationResultUnknown {
		t.Fatalf("unknown candidate was not explicit: %+v", result)
	}
	if len(nav.CandidateWindow) != 1 || nav.CandidateWindow[0].FarmID != 11 ||
		nav.CandidateWindow[0].ActionMask != ActionUnknown || nav.CandidateWindow[0].ValidatedAtMs != 1000 {
		t.Fatalf("unknown candidate was not retained with metadata: %+v", nav.CandidateWindow)
	}
	// The retained UNKNOWN is retryable. Once exact evaluation resolves it,
	// the same navigation round can return FOUND.
	eval.exact[11] = ActionCanSteal
	retry, err := navigator.Next(context0, nav, roster, 1, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Found || retry.Unknown || retry.Status != NavigationResultFound || retry.FarmID != 11 {
		t.Fatalf("resolved unknown candidate = %+v", retry)
	}
}

func TestNavigationGlobalCandidateOrderAcrossBatches(t *testing.T) {
	roster := []uint64{1, 2, 3, 4}
	candidates := &fakeCandidateReader{rows: map[uint64]FarmOpportunityCandidate{
		1: {FarmID: 1, State: CandidateYes, SourceFarmVersion: 1, ActionableAtMs: 100},
		2: {FarmID: 2, State: CandidateYes, SourceFarmVersion: 1, ActionableAtMs: 400},
		3: {FarmID: 3, State: CandidateYes, SourceFarmVersion: 1, ActionableAtMs: 400},
		4: {FarmID: 4, State: CandidateYes, SourceFarmVersion: 1, ActionableAtMs: 200},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{
		1: ActionCanSteal, 2: ActionCanSteal, 3: ActionCanSteal, 4: ActionCanSteal,
	}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(candidates, eval, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.Status != NavigationResultFound || result.FarmID != 2 {
		t.Fatalf("global next = %+v, want farm 2", result)
	}
	if len(eval.calls) != 1 {
		t.Fatalf("exact calls = %+v, want one complete-roster call", eval.calls)
	}
	want := []uint64{2, 3, 4, 1}
	if len(eval.calls[0]) != len(want) {
		t.Fatalf("exact order = %v, want %v", eval.calls[0], want)
	}
	for i, id := range want {
		if eval.calls[0][i] != id {
			t.Fatalf("exact order = %v, want %v", eval.calls[0], want)
		}
	}
	if len(candidates.calls) != 2 || candidates.calls[0][0] != 1 || candidates.calls[0][1] != 2 ||
		candidates.calls[1][0] != 3 || candidates.calls[1][1] != 4 {
		t.Fatalf("coarse batches = %+v", candidates.calls)
	}
	if len(nav.CandidateWindow) > CandidateWindowSize {
		t.Fatalf("candidate window exceeded bound: %d", len(nav.CandidateWindow))
	}
}

func TestNavigationOverfetchRemainsUnknownUntilUncheckedCandidatesAreEvaluated(t *testing.T) {
	rows := make(map[uint64]FarmOpportunityCandidate, 17)
	exactRows := make(map[uint64]ActionMask, 16)
	roster := make([]uint64, 0, 17)
	for id := uint64(1); id <= 17; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, ActionableAtMs: int64(1000 - id)}
		if id <= 16 {
			exactRows[id] = ActionNone
		}
	}
	candidates := &fakeCandidateReader{rows: rows}
	eval := &fakeEvaluator{exact: exactRows}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(candidates, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if first.Found || !first.Unknown || first.Exhausted || first.Status != NavigationResultUnknown {
		t.Fatalf("unchecked candidate was hidden as NONE: %+v", first)
	}
	if len(eval.calls) != 1 || len(eval.calls[0]) != maxExactChecksPerNext {
		t.Fatalf("bounded exact overfetch calls = %+v, want %d", eval.calls, maxExactChecksPerNext)
	}
	if len(nav.pendingCandidates) != 1 || nav.pendingCandidates[0].FarmID != 17 {
		t.Fatalf("unchecked candidate tail = %+v", nav.pendingCandidates)
	}

	eval.exact[17] = ActionCanWater
	second, err := navigator.Next(context0, nav, roster, 1, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Found || second.FarmID != 17 || second.Status != NavigationResultFound {
		t.Fatalf("resolved overfetch tail = %+v", second)
	}
}

func TestNavigationWindowVersionFenceInvalidatesCachedExactResult(t *testing.T) {
	candidates := &fakeCandidateReader{rows: map[uint64]FarmOpportunityCandidate{
		11: {FarmID: 11, State: CandidateYes, SourceFarmVersion: 2, ActionableAtMs: 200},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{}}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.coarseScanDone = true
	nav.CandidateWindow = []CandidateWindowEntry{{
		FarmID:            11,
		SourceFarmVersion: 1,
		ActionableAtMs:    100,
		ActionMask:        ActionCanSteal,
		ValidatedAtMs:     900,
	}}
	navigator, err := NewNavigator(candidates, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, nil, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Found || !result.Unknown || result.Status != NavigationResultUnknown {
		t.Fatalf("stale cached exact result was returned: %+v", result)
	}
	if len(nav.CandidateWindow) != 1 {
		t.Fatalf("stale entry was not retained: %+v", nav.CandidateWindow)
	}
	entry := nav.CandidateWindow[0]
	if entry.SourceFarmVersion != 2 || entry.ActionableAtMs != 200 || entry.ActionMask != ActionUnknown || entry.ValidatedAtMs != 1000 {
		t.Fatalf("stale entry metadata = %+v", entry)
	}
	if len(candidates.calls) != 1 || len(candidates.calls[0]) != 1 || candidates.calls[0][0] != 11 {
		t.Fatalf("version refresh calls = %+v", candidates.calls)
	}
	if len(eval.calls) != 1 || len(eval.calls[0]) != 1 || eval.calls[0][0] != 11 {
		t.Fatalf("stale exact calls = %+v", eval.calls)
	}

	eval.exact[11] = ActionCanWater
	resolved, err := navigator.Next(context0, nav, nil, 1, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Found || resolved.FarmID != 11 || resolved.Status != NavigationResultFound {
		t.Fatalf("resolved stale entry = %+v", resolved)
	}
}

func TestNavigationWindowRefreshUsesTrustedNowForDueSummary(t *testing.T) {
	summaries := fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, Mask: OpportunityNone, NextOpportunityAtMs: 1500},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{11: ActionCanWater}}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.coarseScanDone = true
	nav.CandidateWindow = []CandidateWindowEntry{{
		FarmID: 11, ActionableAtMs: 1500, ActionMask: ActionUnknown,
	}}
	navigator, err := NewNavigator(summaryCandidates(t, summaries), eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, nil, 1, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.FarmID != 11 || result.Status != NavigationResultFound {
		t.Fatalf("due summary was dropped during window refresh: %+v", result)
	}
}

func TestNavigationWindowAndPendingVisitedFenceStayBounded(t *testing.T) {
	rows := make(map[uint64]FarmOpportunityCandidate, 10)
	exactRows := make(map[uint64]ActionMask, 10)
	roster := make([]uint64, 0, 10)
	for id := uint64(1); id <= 10; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, SourceFarmVersion: id, ActionableAtMs: int64(1000 - id)}
		exactRows[id] = ActionCanSteal
	}
	nav := NewNavigation(10001, 1, 0, 0)
	eval := &fakeEvaluator{exact: exactRows}
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !first.Found || first.FarmID != 1 {
		t.Fatalf("first bounded result = %+v, %v", first, err)
	}
	if len(nav.CandidateWindow) != CandidateWindowSize {
		t.Fatalf("candidate window size = %d, want %d", len(nav.CandidateWindow), CandidateWindowSize)
	}
	for _, entry := range nav.CandidateWindow {
		if entry.ActionMask != ActionCanSteal || entry.ValidatedAtMs != 1000 || entry.SourceFarmVersion == 0 {
			t.Fatalf("window entry lacks exact metadata: %+v", entry)
		}
	}
	if !nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 2, ActiveSessionEpoch: 1, SubscriptionID: "sub-2"}) {
		t.Fatal("window visit fence failed")
	}
	if !nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 10, ActiveSessionEpoch: 1, SubscriptionID: "sub-10"}) {
		t.Fatal("pending visit fence failed")
	}
	for _, entry := range nav.CandidateWindow {
		if entry.FarmID == 2 || entry.FarmID == 10 {
			t.Fatalf("visited farm remained in window: %+v", nav.CandidateWindow)
		}
	}
	for _, entry := range nav.pendingCandidates {
		if entry.FarmID == 2 || entry.FarmID == 10 {
			t.Fatalf("visited farm remained pending: %+v", nav.pendingCandidates)
		}
	}
}

func TestOpportunitySummaryTimeCompensation(t *testing.T) {
	summary := FarmOpportunitySummary{FarmID: 1, Mask: OpportunityNone, NextOpportunityAtMs: 500}
	if summary.IsCandidate(400) {
		t.Fatal("future opportunity must not be a candidate yet")
	}
	if !summary.IsCandidate(500) {
		t.Fatal("due opportunity must compensate summary lag")
	}
}

func TestSummaryCandidateReaderCarriesSourceFarmVersion(t *testing.T) {
	reader := summaryCandidates(t, fakeSummaries{rows: map[uint64]FarmOpportunitySummary{
		11: {FarmID: 11, SourceFarmVersion: 7, Mask: OpportunityStealable, NextOpportunityAtMs: 900},
	}})
	rows, err := reader.BatchGetCandidates(context0, []uint64{11}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if rows[11].State != CandidateYes || rows[11].SourceFarmVersion != 7 {
		t.Fatalf("summary candidate = %+v", rows[11])
	}
}

func TestNavigationMissingSummaryFallsThroughToExactEvaluation(t *testing.T) {
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{11: ActionCanSteal}}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(summaryCandidates(t, fakeSummaries{rows: map[uint64]FarmOpportunitySummary{}}), eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, []uint64{11}, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.FarmID != 11 {
		t.Fatalf("missing coarse row hid exact opportunity: %+v", result)
	}
	if len(eval.calls) != 1 || len(eval.calls[0]) != 1 || eval.calls[0][0] != 11 {
		t.Fatalf("exact evaluation calls=%v", eval.calls)
	}
}

func TestParseNavigationWindowModeDefaultsToFullTail(t *testing.T) {
	for _, value := range []string{"", "full-tail", "FULL-TAIL", "nope", " top-k "} {
		got := ParseNavigationWindowMode(value)
		switch value {
		case " top-k ":
			if got != NavigationWindowModeTopK {
				t.Fatalf("ParseNavigationWindowMode(%q)=%q, want top-k", value, got)
			}
		default:
			if got != NavigationWindowModeFullTail {
				t.Fatalf("ParseNavigationWindowMode(%q)=%q, want full-tail", value, got)
			}
		}
	}
}

func TestTopKTruncationRescansPastExactNegatives(t *testing.T) {
	roster := []uint64{1, 2, 3, 4}
	candidates := &fakeCandidateReader{rows: map[uint64]FarmOpportunityCandidate{
		1: {FarmID: 1, State: CandidateYes, ActionableAtMs: 400},
		2: {FarmID: 2, State: CandidateYes, ActionableAtMs: 300},
		3: {FarmID: 3, State: CandidateYes, ActionableAtMs: 200},
		4: {FarmID: 4, State: CandidateYes, ActionableAtMs: 100},
	}}
	eval := &fakeEvaluator{exact: map[uint64]ActionMask{
		1: ActionNone, 2: ActionNone, 3: ActionCanSteal, 4: ActionCanSteal,
	}}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.WindowMode = NavigationWindowModeTopK
	nav.WindowSize = 2
	navigator, err := NewNavigator(candidates, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.Status != NavigationResultFound || result.FarmID != 3 {
		t.Fatalf("top-k rescan = %+v, want farm 3 after exact negatives", result)
	}
	if _, ok := nav.exactNegatives[1]; !ok {
		t.Fatalf("exact negatives missing farm 1: %+v", nav.exactNegatives)
	}
	if _, ok := nav.exactNegatives[2]; !ok {
		t.Fatalf("exact negatives missing farm 2: %+v", nav.exactNegatives)
	}
	// Remaining non-rejected candidates fit in the top-k bound, so the rebuilt
	// tail is complete. A still-truncated roster must keep TailComplete=false.
	if !nav.TailComplete {
		t.Fatal("rescan that fit in top-k must mark the rebuilt tail complete")
	}
}

func TestTopKExhaustionNeverReturnsFalseNone(t *testing.T) {
	// More coarse candidates than maxRescanAttempts*topK so a truncated tail
	// still has unchecked farms when the rescan budget ends.
	roster := make([]uint64, 0, 10)
	rows := make(map[uint64]FarmOpportunityCandidate, 10)
	exact := make(map[uint64]ActionMask, 10)
	for id := uint64(1); id <= 10; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, ActionableAtMs: int64(1000 - id)}
		exact[id] = ActionNone
	}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.WindowMode = NavigationWindowModeTopK
	nav.WindowSize = 2
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, &fakeEvaluator{exact: exact}, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Exhausted || result.Status == NavigationResultNone {
		t.Fatalf("truncated top-k returned false NONE: %+v", result)
	}
	if !result.Unknown || result.Status != NavigationResultUnknown {
		t.Fatalf("truncated top-k without actionable farms = %+v, want UNKNOWN", result)
	}
	if nav.TailComplete {
		t.Fatal("rescan budget exhaustion must keep an incomplete truncated tail")
	}
}

func TestTopKCompleteScanReturnsDefinitiveNoneNotRepeatedUnknown(t *testing.T) {
	// An empty/all-negative roster that fits inside K completes the tail in
	// one scan; the exhausted answer is a definitive NONE, never a repeated
	// UNKNOWN/rescan.
	roster := []uint64{1, 2, 3}
	rows := map[uint64]FarmOpportunityCandidate{
		1: {FarmID: 1, State: CandidateYes, ActionableAtMs: 900},
		2: {FarmID: 2, State: CandidateYes, ActionableAtMs: 800},
		3: {FarmID: 3, State: CandidateYes, ActionableAtMs: 700},
	}
	exact := map[uint64]ActionMask{1: ActionNone, 2: ActionNone, 3: ActionNone}
	candidates := &fakeCandidateReader{rows: rows}
	eval := &fakeEvaluator{exact: exact}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.WindowMode = NavigationWindowModeTopK
	nav.WindowSize = 32
	navigator, err := NewNavigator(candidates, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != NavigationResultNone || !result.Exhausted {
		t.Fatalf("complete all-negative top-k scan = %+v, want NONE", result)
	}
	if !nav.TailComplete {
		t.Fatalf("complete top-k scan must mark the tail complete: %+v", nav)
	}
	if len(candidates.calls) != 1 {
		t.Fatalf("complete top-k scan rescanned: candidate batches=%v", candidates.calls)
	}

	// The same all-negative roster with an empty roster yields a definitive
	// NONE after a single complete scan of zero candidates.
	empty := NewNavigation(10001, 1, 0, 0)
	empty.WindowMode = NavigationWindowModeTopK
	empty.WindowSize = 32
	emptyResult, err := navigator.Next(context0, empty, nil, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if emptyResult.Status != NavigationResultNone || !emptyResult.Exhausted {
		t.Fatalf("empty roster top-k = %+v, want NONE", emptyResult)
	}
}

func TestFullTailKeepsCompletePendingAndMarksTailComplete(t *testing.T) {
	roster := make([]uint64, 0, 20)
	rows := make(map[uint64]FarmOpportunityCandidate, 20)
	exact := make(map[uint64]ActionMask, 20)
	for id := uint64(1); id <= 20; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, ActionableAtMs: int64(1000 - id)}
		exact[id] = ActionCanSteal
	}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, &fakeEvaluator{exact: exact}, 8)
	if err != nil {
		t.Fatal(err)
	}
	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !result.Found {
		t.Fatalf("full-tail next = %+v err=%v", result, err)
	}
	if !nav.TailComplete || nav.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("full-tail state = mode=%q complete=%v", nav.WindowMode, nav.TailComplete)
	}
	if len(nav.CandidateWindow)+len(nav.pendingCandidates) < 20 {
		t.Fatalf("full-tail dropped candidates: window=%d pending=%d",
			len(nav.CandidateWindow), len(nav.pendingCandidates))
	}
}

func TestRosterRevisionClearsExactNegativesFullTail(t *testing.T) {
	roster := []uint64{11, 12}
	rows := map[uint64]FarmOpportunityCandidate{
		11: {FarmID: 11, State: CandidateYes, ActionableAtMs: 900},
		12: {FarmID: 12, State: CandidateYes, ActionableAtMs: 800},
	}
	exact := map[uint64]ActionMask{11: ActionNone, 12: ActionCanSteal}
	eval := &fakeEvaluator{exact: exact}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	// First round: farm 11 is exact-negative and is recorded as a confirmed
	// negative; farm 12 is returned.
	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !first.Found || first.FarmID != 12 {
		t.Fatalf("first round = %+v err=%v", first, err)
	}
	if _, ok := nav.exactNegatives[11]; !ok {
		t.Fatalf("farm 11 not recorded as exact negative: %+v", nav.exactNegatives)
	}
	nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 12, ActiveSessionEpoch: 1, SubscriptionID: "s1"})
	// Roster revision changes and farm 11 becomes actionable. The stale
	// negative must be cleared or the revised roster would skip it.
	exact[11] = ActionCanWater
	second, err := navigator.Next(context0, nav, roster, 2, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Found || second.FarmID != 11 {
		t.Fatalf("after roster revision = %+v, want farm 11 FOUND", second)
	}
}

func TestRosterRevisionClearsExactNegativesTopK(t *testing.T) {
	roster := []uint64{11, 12, 13}
	rows := map[uint64]FarmOpportunityCandidate{
		11: {FarmID: 11, State: CandidateYes, ActionableAtMs: 900},
		12: {FarmID: 12, State: CandidateYes, ActionableAtMs: 800},
		13: {FarmID: 13, State: CandidateYes, ActionableAtMs: 700},
	}
	exact := map[uint64]ActionMask{11: ActionNone, 12: ActionNone, 13: ActionCanSteal}
	eval := &fakeEvaluator{exact: exact}
	nav := NewNavigation(10001, 1, 0, 0)
	nav.WindowMode = NavigationWindowModeTopK
	nav.WindowSize = 2
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, eval, 4)
	if err != nil {
		t.Fatal(err)
	}
	first, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil || !first.Found || first.FarmID != 13 {
		t.Fatalf("first round = %+v err=%v", first, err)
	}
	for _, farmID := range []uint64{11, 12} {
		if _, ok := nav.exactNegatives[farmID]; !ok {
			t.Fatalf("farm %d not recorded as exact negative: %+v", farmID, nav.exactNegatives)
		}
	}
	nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 13, ActiveSessionEpoch: 1, SubscriptionID: "s1"})
	// Roster revision: farms 11 and 12 become actionable. A same-revision
	// rescan would skip them; a revision reset must clear the negatives.
	exact[11] = ActionCanWater
	exact[12] = ActionCanWater
	second, err := navigator.Next(context0, nav, roster, 2, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Found || second.FarmID != 11 {
		t.Fatalf("after roster revision = %+v, want farm 11 FOUND", second)
	}
	if len(nav.exactNegatives) != 0 {
		t.Fatalf("roster revision kept stale exact negatives: %+v", nav.exactNegatives)
	}
}

type filteringRelations struct {
	active map[uint64]struct{}
	calls  [][]uint64
}

func (f *filteringRelations) KeepActiveFriends(_ context.Context, _ uint64, farmIDs []uint64) ([]uint64, error) {
	f.calls = append(f.calls, append([]uint64(nil), farmIDs...))
	kept := make([]uint64, 0, len(farmIDs))
	for _, farmID := range farmIDs {
		if _, ok := f.active[farmID]; ok {
			kept = append(kept, farmID)
		}
	}
	return kept, nil
}

func TestRelationFilterDefersPairUntilTopKAndRefillsStaleIndex(t *testing.T) {
	// Header order prefers actionable time: 101..116 first (stale Index),
	// then 201 (active). Pair is consulted in batches of 16; Facts must only
	// see the surviving friendship.
	roster := make([]uint64, 0, 17)
	rows := make(map[uint64]FarmOpportunityCandidate, 17)
	exact := make(map[uint64]ActionMask, 17)
	active := map[uint64]struct{}{201: {}}
	for id := uint64(101); id <= 116; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, ActionableAtMs: int64(2000 - id)}
		exact[id] = ActionCanSteal
	}
	roster = append(roster, 201)
	rows[201] = FarmOpportunityCandidate{FarmID: 201, State: CandidateYes, ActionableAtMs: 100}
	exact[201] = ActionCanSteal

	relations := &filteringRelations{active: active}
	eval := &fakeEvaluator{exact: exact}
	nav := NewNavigation(10001, 1, 0, 0)
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, eval, 8)
	if err != nil {
		t.Fatal(err)
	}
	navigator.Relations = relations

	result, err := navigator.Next(context0, nav, roster, 1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.FarmID != 201 {
		t.Fatalf("next=%+v, want active farm 201 after stale Top-16 refill", result)
	}
	if len(relations.calls) < 2 {
		t.Fatalf("relation batches=%v, want at least one Top-16 batch plus refill", relations.calls)
	}
	if len(relations.calls[0]) != 16 {
		t.Fatalf("first Pair batch size=%d, want 16", len(relations.calls[0]))
	}
	if len(eval.calls) != 1 || len(eval.calls[0]) != 1 || eval.calls[0][0] != 201 {
		t.Fatalf("Facts batches=%v, want only active farm 201", eval.calls)
	}
	for id := uint64(101); id <= 116; id++ {
		if _, ok := nav.relationNegatives[id]; !ok {
			t.Fatalf("stale farm %d missing from relationNegatives: %+v", id, nav.relationNegatives)
		}
	}

	nav.MarkVisited(VisitFence{ViewerPlayerID: 10001, TargetFarmID: 201, ActiveSessionEpoch: 1, SubscriptionID: "s-201"})
	exhausted, err := navigator.Next(context0, nav, roster, 1, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if exhausted.Status != NavigationResultNone || !exhausted.Exhausted {
		t.Fatalf("exhausted=%+v, want NONE only after the sorted tail is drained", exhausted)
	}
}
