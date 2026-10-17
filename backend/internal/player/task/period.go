package task

import "time"

// DefaultGameTimezone is the frozen P0 game timezone (Q34-02).
const DefaultGameTimezone = "Asia/Shanghai"

// DailyPeriodID derives the logical daily period (YYYYMMDD) of
// trusted_now_ms in the configured game timezone. The boundary is the
// civil local midnight: in a fixed-offset timezone such as Asia/Shanghai
// (no DST since 1991) it is exactly 00:00+08:00; in a DST-observing
// timezone it follows the civil date, so spring-forward/fall-back never
// merge or duplicate periods.
func DailyPeriodID(nowMs int64, loc *time.Location) string {
	return time.UnixMilli(nowMs).In(loc).Format("20060102")
}

// NextResetAtMs returns the start of the next daily period in
// milliseconds, i.e. the next civil local midnight in the game timezone.
// It is a UI scheduling hint only; the client clock never decides the
// period (Q34-02).
func NextResetAtMs(nowMs int64, loc *time.Location) int64 {
	now := time.UnixMilli(nowMs).In(loc)
	y, m, d := now.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, loc).UnixMilli()
}

// periodOfFact classifies a fact occurrence time against the current
// period. YYYYMMDD compares lexicographically in chronological order.
func periodOfFact(occurredAtMs int64, loc *time.Location) string {
	return DailyPeriodID(occurredAtMs, loc)
}
