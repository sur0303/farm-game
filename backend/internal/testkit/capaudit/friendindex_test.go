package capaudit

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/friend"
)

type stubInspector struct {
	byViewer map[uint64]friend.FriendIndexInspection
	failOn   uint64
	// settledSeen records the cutoff each call was given.
	settledSeen []int64
}

func (s *stubInspector) InspectViewer(_ context.Context, viewer uint64, settledBeforeMs int64) (friend.FriendIndexInspection, error) {
	s.settledSeen = append(s.settledSeen, settledBeforeMs)
	if viewer == s.failOn {
		return friend.FriendIndexInspection{}, errors.New("projection unreadable")
	}
	return s.byViewer[viewer], nil
}

func TestUnrepairedRowsAreCountedAcrossTheWholeRun(t *testing.T) {
	ledger := testLedger(3)
	inspector := &stubInspector{byViewer: map[uint64]friend.FriendIndexInspection{
		ledger.ActorBase:     {MissingRows: 2},
		ledger.ActorBase + 1: {ExtraRows: 1},
	}}
	check, err := NewFriendIndexCheck(FriendIndexConfig{Reconciler: inspector, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("unrepaired=%d, want both the rows a player lost and the rows they should not have", got)
	}
	if len(inspector.settledSeen) != 3 {
		t.Fatalf("inspected %d viewers, want every identity in the ledger", len(inspector.settledSeen))
	}
}

// The cutoff has to be the repair window behind now, or a pair that changed a
// moment ago would be judged before repair had its rounds.
func TestTheCutoffTrailsNowByTheRepairWindow(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	inspector := &stubInspector{}
	check, err := NewFriendIndexCheck(FriendIndexConfig{
		Reconciler: inspector, Ledger: testLedger(1),
		Window: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := check.Observe(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := now.Add(-time.Minute).UnixMilli()
	if len(inspector.settledSeen) != 1 || inspector.settledSeen[0] != want {
		t.Fatalf("cutoff=%v, want %d", inspector.settledSeen, want)
	}
}

// A player whose projection cannot be read is not a player with a clean one.
func TestAViewerThatCannotBeInspectedFailsTheCheck(t *testing.T) {
	ledger := testLedger(2)
	check, err := NewFriendIndexCheck(FriendIndexConfig{
		Reconciler: &stubInspector{failOn: ledger.ActorBase + 1}, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := check.Observe(context.Background()); err == nil {
		t.Fatal("an unreadable projection must fail the check rather than count as zero")
	}
}

func TestFriendGateIsNamedByTheContract(t *testing.T) {
	check, err := NewFriendIndexCheck(FriendIndexConfig{Reconciler: &stubInspector{}, Ledger: testLedger(1)})
	if err != nil {
		t.Fatal(err)
	}
	if check.Gate() != "friend_pair_index_unrepaired" {
		t.Fatalf("Gate()=%s", check.Gate())
	}
}
