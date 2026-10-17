package farmbotclient

import (
	"sort"
	"sync"

	farmpb "farmgame/api/gen/farm"
)

// FarmViewStats describes how well one session tracked the realtime stream.
// A gap is the honest signal that the client can no longer trust its view and
// has to take a fresh join, exactly like the H5 client.
type FarmViewStats struct {
	FarmVersion uint64 `json:"farm_version"`
	Applied     uint64 `json:"applied"`
	Duplicates  uint64 `json:"duplicates"`
	Gaps        uint64 `json:"gaps"`
	Stale       bool   `json:"stale"`
}

// FarmView is the client-side materialized farm state: the join snapshot plus
// the FARM_CHANGE deltas the background reader applies in order.
//
// A bot that stays online needs this to choose a valid plot for its next
// action without re-joining, and tracking it is also what makes the bot a
// real consumer of broadcast fan-out rather than a sink that discards it.
type FarmView struct {
	mu          sync.Mutex
	seeded      bool
	stale       bool
	ownerID     uint64
	farmVersion uint64
	plots       map[uint32]*farmpb.PlotView
	applied     uint64
	duplicates  uint64
	gaps        uint64
}

// NewFarmView returns an empty view. It stays empty until a join snapshot
// seeds it.
func NewFarmView() *FarmView {
	return &FarmView{plots: make(map[uint32]*farmpb.PlotView)}
}

// Reset seeds the view from a join snapshot and clears staleness. Plot views
// are shared by pointer and must be treated as read-only.
func (view *FarmView) Reset(snapshot *farmpb.FarmSnapshot) {
	if view == nil || snapshot == nil {
		return
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	view.seeded = true
	view.stale = false
	view.ownerID = snapshot.GetOwnerId()
	view.farmVersion = snapshot.GetFarmVersion()
	view.plots = make(map[uint32]*farmpb.PlotView, len(snapshot.GetPlots()))
	for _, plot := range snapshot.GetPlots() {
		if plot == nil {
			continue
		}
		view.plots[plot.GetPlotIndex()] = plot
	}
}

// Apply folds one realtime change into the view. Out-of-order or skipped
// farm versions do not silently patch the view: the view is marked stale so
// the caller re-joins instead of acting on a state it cannot prove.
func (view *FarmView) Apply(change *farmpb.FarmChange) {
	if view == nil || change == nil {
		return
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	if !view.seeded || change.GetOwnerId() != view.ownerID {
		return
	}
	switch {
	case change.GetFarmVersion() <= view.farmVersion:
		view.duplicates++
		return
	case change.GetFarmVersion() != view.farmVersion+1:
		view.gaps++
		view.stale = true
		return
	}
	for _, plot := range change.GetChangedPlots() {
		if plot == nil {
			continue
		}
		view.plots[plot.GetPlotIndex()] = plot
	}
	view.farmVersion = change.GetFarmVersion()
	view.applied++
}

// Invalidate marks the view untrustworthy, which is what a REJOIN_REQUIRED
// push means for a client that wants to keep acting.
func (view *FarmView) Invalidate() {
	if view == nil {
		return
	}
	view.mu.Lock()
	view.stale = true
	view.mu.Unlock()
}

// Usable reports whether the view is seeded and still trustworthy.
func (view *FarmView) Usable() bool {
	if view == nil {
		return false
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	return view.seeded && !view.stale
}

// Snapshot materializes the current view in stable plot order. The returned
// snapshot reuses plot pointers and must be treated as read-only.
func (view *FarmView) Snapshot() *farmpb.FarmSnapshot {
	if view == nil {
		return nil
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	if !view.seeded {
		return nil
	}
	indexes := make([]uint32, 0, len(view.plots))
	for index := range view.plots {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(left, right int) bool { return indexes[left] < indexes[right] })
	plots := make([]*farmpb.PlotView, 0, len(indexes))
	for _, index := range indexes {
		plots = append(plots, view.plots[index])
	}
	return &farmpb.FarmSnapshot{
		OwnerId:     view.ownerID,
		FarmVersion: view.farmVersion,
		Plots:       plots,
	}
}

// Stats reports the realtime tracking counters for one session.
func (view *FarmView) Stats() FarmViewStats {
	if view == nil {
		return FarmViewStats{}
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	return FarmViewStats{
		FarmVersion: view.farmVersion,
		Applied:     view.applied,
		Duplicates:  view.duplicates,
		Gaps:        view.gaps,
		Stale:       view.stale,
	}
}
