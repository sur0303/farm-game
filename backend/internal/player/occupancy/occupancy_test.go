package occupancy

import (
	"testing"

	playerpb "farmgame/api/gen/player"
)

func TestPlotOccupancyRoundTrip(t *testing.T) {
	rec := &playerpb.PlayerRecord{}
	if err := OccupyPlot(rec, 3, "cycle-1", 11, 100); err != nil {
		t.Fatal(err)
	}
	cycle, occupied := PlotOccupied(rec, 3)
	if !occupied || cycle != "cycle-1" {
		t.Fatalf("occupied=%v cycle=%q", occupied, cycle)
	}
	if err := OccupyPlot(rec, 3, "cycle-2", 11, 200); err == nil {
		t.Fatal("second occupy must fail")
	}
	ClearPlot(rec, 3)
	if _, occupied := PlotOccupied(rec, 3); occupied {
		t.Fatal("occupancy survived clear")
	}
}

func TestUnlockOccupancyRoundTrip(t *testing.T) {
	rec := &playerpb.PlayerRecord{}
	if err := OccupyUnlock(rec, 6, "res-a", 100, 50); err != nil {
		t.Fatal(err)
	}
	got, occupied := UnlockOccupied(rec, 6)
	if !occupied || got != "res-a" {
		t.Fatalf("occupied=%v reservation=%q", occupied, got)
	}
	if err := OccupyUnlock(rec, 6, "res-b", 100, 60); err == nil {
		t.Fatal("second unlock occupy must fail")
	}
	ClearUnlock(rec, 6)
	if _, occupied := UnlockOccupied(rec, 6); occupied {
		t.Fatal("unlock occupancy survived clear")
	}
}
