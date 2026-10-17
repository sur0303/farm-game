package task_test

import (
	"testing"
	"time"

	"farmgame/internal/player/task"
)

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %s: %v", name, err)
	}
	return loc
}

func utcMs(t *testing.T, y int, m time.Month, d, hh, mm, ss, millis int) int64 {
	t.Helper()
	return time.Date(y, m, d, hh, mm, ss, millis*int(time.Millisecond), time.UTC).UnixMilli()
}

// TestDailyPeriodIDBoundary pins the strict civil-midnight boundary
// (Q34-02). Asia/Shanghai is a fixed +08:00 zone without DST; New York is
// a DST-observing zone proving the derivation follows the civil date, not
// a fixed 24h offset.
func TestDailyPeriodIDBoundary(t *testing.T) {
	shanghai := mustLocation(t, "Asia/Shanghai")
	newYork := mustLocation(t, "America/New_York")

	cases := []struct {
		name string
		loc  *time.Location
		now  int64
		want string
	}{
		// 2026-08-03 23:59:59.999 +08:00 is still period 20260803.
		{"shanghai just before midnight", shanghai, utcMs(t, 2026, 8, 3, 15, 59, 59, 999), "20260803"},
		// 2026-08-04 00:00:00.000 +08:00 flips to the new period.
		{"shanghai exactly midnight", shanghai, utcMs(t, 2026, 8, 3, 16, 0, 0, 0), "20260804"},
		{"shanghai one second after", shanghai, utcMs(t, 2026, 8, 3, 16, 0, 1, 0), "20260804"},
		// Month rollover: 2026-08-31 24:00 -> 20260901.
		{"shanghai month rollover", shanghai, utcMs(t, 2026, 8, 31, 16, 0, 0, 0), "20260901"},
		// Year rollover: 2027-01-01 00:00 +08:00.
		{"shanghai year rollover", shanghai, utcMs(t, 2026, 12, 31, 16, 0, 0, 0), "20270101"},
		// DST spring-forward week in New York: the period still flips at
		// civil 00:00 EST even though the coming day has 23 hours.
		{"new york before spring forward day", newYork, utcMs(t, 2026, 3, 8, 4, 59, 59, 999), "20260307"},
		{"new york spring forward day start", newYork, utcMs(t, 2026, 3, 8, 5, 0, 0, 0), "20260308"},
		// DST fall-back day: period flips at civil 00:00 EDT.
		{"new york fall back day start", newYork, utcMs(t, 2026, 11, 1, 4, 0, 0, 0), "20261101"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := task.DailyPeriodID(tc.now, tc.loc); got != tc.want {
				t.Fatalf("DailyPeriodID(%d) = %s, want %s", tc.now, got, tc.want)
			}
		})
	}
}

// TestNextResetAtMs pins next_reset_at_ms to the next civil local midnight.
// Expected values are fixed UTC instants so the assertions do not share
// the implementation's arithmetic.
func TestNextResetAtMs(t *testing.T) {
	shanghai := mustLocation(t, "Asia/Shanghai")
	newYork := mustLocation(t, "America/New_York")

	cases := []struct {
		name string
		loc  *time.Location
		now  int64
		want int64
	}{
		{"shanghai noon", shanghai, utcMs(t, 2026, 8, 3, 4, 0, 0, 0), utcMs(t, 2026, 8, 3, 16, 0, 0, 0)},
		{"shanghai just before midnight", shanghai, utcMs(t, 2026, 8, 3, 15, 59, 59, 999), utcMs(t, 2026, 8, 3, 16, 0, 0, 0)},
		{"shanghai exactly midnight rolls to next day", shanghai, utcMs(t, 2026, 8, 3, 16, 0, 0, 0), utcMs(t, 2026, 8, 4, 16, 0, 0, 0)},
		// Spring-forward day: 2026-03-08 in New York is 23 hours long;
		// the next reset is 2026-03-09 00:00 EDT = 04:00 UTC, not 05:00.
		{"new york spring forward short day", newYork, utcMs(t, 2026, 3, 8, 5, 0, 0, 0), utcMs(t, 2026, 3, 9, 4, 0, 0, 0)},
		// Fall-back day: 2026-11-01 is 25 hours long; the next reset is
		// 2026-11-02 00:00 EST = 05:00 UTC.
		{"new york fall back long day", newYork, utcMs(t, 2026, 11, 1, 4, 0, 0, 0), utcMs(t, 2026, 11, 2, 5, 0, 0, 0)},
		{"new york fall back day noon", newYork, utcMs(t, 2026, 11, 1, 17, 0, 0, 0), utcMs(t, 2026, 11, 2, 5, 0, 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := task.NextResetAtMs(tc.now, tc.loc); got != tc.want {
				gotT := time.UnixMilli(got).UTC()
				wantT := time.UnixMilli(tc.want).UTC()
				t.Fatalf("NextResetAtMs = %v (%v), want %v (%v)", got, gotT, tc.want, wantT)
			}
		})
	}
}
