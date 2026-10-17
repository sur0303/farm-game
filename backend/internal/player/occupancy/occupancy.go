// Package occupancy tracks the Player-side plot/unlock shadow occupancy used
// by SourceOrder Plant and UnlockPlot. It reuses ItemReservation rows with a
// stable id prefix so the v2 Player field schema does not need a cutover.
package occupancy

import (
	"fmt"
	"strconv"
	"strings"

	playerpb "farmgame/api/gen/player"
)

const (
	plotPrefix   = "plotocc:"
	unlockPrefix = "unlockocc:"
)

func PlotReservationID(plotID uint32, cycleID string) string {
	return plotPrefix + strconv.FormatUint(uint64(plotID), 10) + ":" + cycleID
}

func UnlockReservationID(plotID uint32, reservationID string) string {
	return unlockPrefix + strconv.FormatUint(uint64(plotID), 10) + ":" + reservationID
}

func PlotOccupied(rec *playerpb.PlayerRecord, plotID uint32) (cycleID string, occupied bool) {
	prefix := plotPrefix + strconv.FormatUint(uint64(plotID), 10) + ":"
	for _, reservation := range rec.GetItemReservations() {
		if reservation == nil {
			continue
		}
		if reservation.GetState() != playerpb.ReservationState_COMMITTED &&
			reservation.GetState() != playerpb.ReservationState_PREPARED {
			continue
		}
		id := reservation.GetReservationId()
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix), true
		}
	}
	return "", false
}

func UnlockOccupied(rec *playerpb.PlayerRecord, plotID uint32) (reservationID string, occupied bool) {
	prefix := unlockPrefix + strconv.FormatUint(uint64(plotID), 10) + ":"
	for _, reservation := range rec.GetItemReservations() {
		if reservation == nil {
			continue
		}
		if reservation.GetState() != playerpb.ReservationState_COMMITTED &&
			reservation.GetState() != playerpb.ReservationState_PREPARED {
			continue
		}
		id := reservation.GetReservationId()
		if strings.HasPrefix(id, prefix) {
			return strings.TrimPrefix(id, prefix), true
		}
	}
	return "", false
}

func ClearPlot(rec *playerpb.PlayerRecord, plotID uint32) {
	if rec == nil {
		return
	}
	prefix := plotPrefix + strconv.FormatUint(uint64(plotID), 10) + ":"
	kept := rec.ItemReservations[:0]
	if kept == nil {
		kept = rec.ItemReservations[:0:0]
	}
	for _, reservation := range rec.ItemReservations {
		if reservation == nil || strings.HasPrefix(reservation.GetReservationId(), prefix) {
			continue
		}
		kept = append(kept, reservation)
	}
	rec.ItemReservations = kept
}

func OccupyPlot(rec *playerpb.PlayerRecord, plotID uint32, cycleID string, seedItemID int32, nowMs int64) error {
	if rec == nil || plotID == 0 || cycleID == "" {
		return fmt.Errorf("occupancy: plot and cycle are required")
	}
	if _, occupied := PlotOccupied(rec, plotID); occupied {
		return fmt.Errorf("occupancy: plot %d already occupied", plotID)
	}
	rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
		ReservationId: PlotReservationID(plotID, cycleID),
		ItemId:        seedItemID,
		Count:         1,
		State:         playerpb.ReservationState_COMMITTED,
		UpdatedAtMs:   nowMs,
	})
	return nil
}

func OccupyUnlock(rec *playerpb.PlayerRecord, plotID uint32, reservationID string, coinAmount int64, nowMs int64) error {
	if rec == nil || plotID == 0 || reservationID == "" {
		return fmt.Errorf("occupancy: unlock plot and reservation are required")
	}
	if _, occupied := UnlockOccupied(rec, plotID); occupied {
		return fmt.Errorf("occupancy: plot %d unlock already occupied", plotID)
	}
	rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
		ReservationId: UnlockReservationID(plotID, reservationID),
		ItemId:        0,
		Count:         coinAmount,
		State:         playerpb.ReservationState_COMMITTED,
		UpdatedAtMs:   nowMs,
	})
	return nil
}

func ClearUnlock(rec *playerpb.PlayerRecord, plotID uint32) {
	if rec == nil {
		return
	}
	prefix := unlockPrefix + strconv.FormatUint(uint64(plotID), 10) + ":"
	kept := rec.ItemReservations[:0]
	if kept == nil {
		kept = rec.ItemReservations[:0:0]
	}
	for _, reservation := range rec.ItemReservations {
		if reservation == nil || strings.HasPrefix(reservation.GetReservationId(), prefix) {
			continue
		}
		kept = append(kept, reservation)
	}
	rec.ItemReservations = kept
}
