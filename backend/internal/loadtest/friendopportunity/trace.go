package friendopportunity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
)

type MutationCause string

const (
	MutationPlant   MutationCause = "Plant"
	MutationHarvest MutationCause = "Harvest"
	MutationCare    MutationCause = "Care"
	MutationTheft   MutationCause = "Theft"
)

type BoundaryKind string

const (
	BoundaryStealable BoundaryKind = "Stealable"
	BoundaryWater     BoundaryKind = "Water"
	BoundaryPest      BoundaryKind = "Pest"
)

type ScheduleOperation string

const (
	ScheduleUpsert ScheduleOperation = "upsert"
	ScheduleCancel ScheduleOperation = "cancel"
)

// SessionRequest is an independently generated open-loop user-session arrival.
// An accepted session issues an immediate GET_NEXT and then fixed-interval
// fallback polls until the configured run horizon.
type SessionRequest struct {
	ID          int   `json:"id"`
	SessionID   int64 `json:"session_id"`
	UserID      int64 `json:"user_id"`
	PlannedAtMS int64 `json:"planned_at_ms"`
}

// FarmBoundaryEvent is one farm/plot future-boundary mutation. It is never
// expanded by session arrivals, GET_NEXT calls, friends, or hint recipients.
type FarmBoundaryEvent struct {
	ID                int               `json:"id"`
	FarmID            int               `json:"farm_id"`
	PlotID            int               `json:"plot_id"`
	MutationCause     MutationCause     `json:"mutation_cause"`
	BoundaryKind      BoundaryKind      `json:"boundary_kind"`
	ScheduleOperation ScheduleOperation `json:"schedule_operation"`
	MutationAtMS      int64             `json:"mutation_at_ms"`
	BoundaryAtMS      int64             `json:"boundary_at_ms"`
	OnlineTargetIDs   []int             `json:"online_target_ids,omitempty"`
}

type Trace struct {
	SchemaVersion   string              `json:"schema_version"`
	Seed            int64               `json:"seed"`
	SessionSeed     int64               `json:"session_seed"`
	BoundarySeed    int64               `json:"boundary_seed"`
	FriendIDs       []int               `json:"friend_ids"`
	PlotIDs         []int               `json:"plot_ids"`
	SessionRequests []SessionRequest    `json:"session_requests"`
	BoundaryEvents  []FarmBoundaryEvent `json:"boundary_events"`
}

func GenerateTrace(c RunConfig) (Trace, error) {
	if err := c.Validate(); err != nil {
		return Trace{}, err
	}
	sessionSeed := deriveSeed(c.Seed, 0x5353455353494f4e)
	boundarySeed := deriveSeed(c.Seed, 0x424f554e44415259)
	trace := Trace{
		SchemaVersion: TraceSchemaVersion,
		Seed:          c.Seed,
		SessionSeed:   sessionSeed,
		BoundarySeed:  boundarySeed,
		FriendIDs:     sequentialIDs(c.FriendCount),
		PlotIDs:       sequentialIDs(c.PlotCount),
	}
	trace.SessionRequests = generateSessionRequests(c, sessionSeed)
	trace.BoundaryEvents = generateBoundaryEvents(c, boundarySeed)
	if err := trace.Validate(c); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

func generateSessionRequests(c RunConfig, seed int64) []SessionRequest {
	count, _ := c.PlannedSessionCount()
	rng := newDeterministicRNG(seed)
	requests := make([]SessionRequest, 0, count)
	for i := 0; i < count; i++ {
		at := int64(math.Floor(float64(i) * 1000 / c.ArrivalRatePerSecond))
		if at >= c.DurationMS {
			break
		}
		requests = append(requests, SessionRequest{
			ID:          i + 1,
			SessionID:   int64(i + 1),
			UserID:      1_000_000 + int64(rng.Intn(9_000_000)),
			PlannedAtMS: at,
		})
	}
	return requests
}

type farmPlot struct {
	farmID int
	plotID int
}

func generateBoundaryEvents(c RunConfig, seed int64) []FarmBoundaryEvent {
	total := c.FriendCount * c.PlotCount
	count := total * c.OpportunityDensityPercent / 100
	if count == 0 {
		return []FarmBoundaryEvent{}
	}
	rng := newDeterministicRNG(seed)
	candidates := make([]farmPlot, 0, total)
	for farmID := 1; farmID <= c.FriendCount; farmID++ {
		for plotID := 1; plotID <= c.PlotCount; plotID++ {
			candidates = append(candidates, farmPlot{farmID: farmID, plotID: plotID})
		}
	}
	for i := len(candidates) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}

	causes := []MutationCause{MutationPlant, MutationHarvest, MutationCare, MutationTheft}
	kinds := []BoundaryKind{BoundaryStealable, BoundaryWater, BoundaryPest}
	events := make([]FarmBoundaryEvent, 0, count)
	for i, candidate := range candidates[:count] {
		boundaryAt := boundaryTime(c, i, count, rng)
		maxLead := boundaryAt
		if tenth := c.DurationMS / 10; maxLead > tenth && tenth > 0 {
			maxLead = tenth
		}
		lead := int64(1)
		if maxLead > 1 {
			lead += int64(rng.Intn(int(maxLead)))
		}
		operation := ScheduleUpsert
		if i%5 == 4 {
			operation = ScheduleCancel
		}
		event := FarmBoundaryEvent{
			ID:                i + 1,
			FarmID:            candidate.farmID,
			PlotID:            candidate.plotID,
			MutationCause:     causes[i%len(causes)],
			BoundaryKind:      kinds[rng.Intn(len(kinds))],
			ScheduleOperation: operation,
			MutationAtMS:      boundaryAt - lead,
			BoundaryAtMS:      boundaryAt,
		}
		if operation == ScheduleUpsert {
			event.OnlineTargetIDs = sampleOnlineTargets(c, candidate.farmID, rng)
		}
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].MutationAtMS != events[j].MutationAtMS {
			return events[i].MutationAtMS < events[j].MutationAtMS
		}
		if events[i].BoundaryAtMS != events[j].BoundaryAtMS {
			return events[i].BoundaryAtMS < events[j].BoundaryAtMS
		}
		if events[i].FarmID != events[j].FarmID {
			return events[i].FarmID < events[j].FarmID
		}
		return events[i].PlotID < events[j].PlotID
	})
	for i := range events {
		events[i].ID = i + 1
	}
	return events
}

func boundaryTime(c RunConfig, index, count int, rng *deterministicRNG) int64 {
	if c.DurationMS <= 2 {
		return 1
	}
	var at int64
	switch c.ExpiryPattern {
	case ExpiryBursty:
		clusters := []int64{c.DurationMS / 4, c.DurationMS / 2, c.DurationMS * 3 / 4}
		center := clusters[index%len(clusters)]
		spread := c.DurationMS / 100
		if spread < 1 {
			spread = 1
		}
		at = center + int64(rng.Intn(int(2*spread+1))) - spread
	default:
		at = int64(index+1) * c.DurationMS / int64(count+1)
		jitterRange := c.DurationMS / int64(4*(count+1))
		if jitterRange > 0 {
			at += int64(rng.Intn(int(2*jitterRange+1))) - jitterRange
		}
	}
	if at < 1 {
		return 1
	}
	if at >= c.DurationMS {
		return c.DurationMS - 1
	}
	return at
}

func sampleOnlineTargets(c RunConfig, farmID int, rng *deterministicRNG) []int {
	threshold := int(math.Round(c.OnlineRatePercent * 100))
	targets := make([]int, 0, int(math.Ceil(float64(c.FriendCount)*c.OnlineRatePercent/100)))
	for candidate := 1; candidate <= c.FriendCount; candidate++ {
		if candidate == farmID {
			continue
		}
		if rng.Intn(10_000) < threshold {
			targets = append(targets, candidate)
		}
	}
	return targets
}

func (t Trace) Validate(c RunConfig) error {
	if t.SchemaVersion != TraceSchemaVersion {
		return fmt.Errorf("friendopportunity: trace schema %q, want %q", t.SchemaVersion, TraceSchemaVersion)
	}
	if t.Seed != c.Seed || t.SessionSeed != deriveSeed(c.Seed, 0x5353455353494f4e) ||
		t.BoundarySeed != deriveSeed(c.Seed, 0x424f554e44415259) {
		return fmt.Errorf("friendopportunity: trace seeds do not match config")
	}
	expectedSessions, err := c.PlannedSessionCount()
	if err != nil {
		return err
	}
	if len(t.SessionRequests) != expectedSessions {
		return fmt.Errorf("friendopportunity: got %d session requests, want %d", len(t.SessionRequests), expectedSessions)
	}
	expectedBoundaries := c.FriendCount * c.PlotCount * c.OpportunityDensityPercent / 100
	if len(t.BoundaryEvents) != expectedBoundaries {
		return fmt.Errorf("friendopportunity: got %d boundary events, want %d", len(t.BoundaryEvents), expectedBoundaries)
	}
	for i, request := range t.SessionRequests {
		if request.ID != i+1 || request.SessionID <= 0 || request.PlannedAtMS < 0 || request.PlannedAtMS >= c.DurationMS {
			return fmt.Errorf("friendopportunity: invalid session request at index %d", i)
		}
	}
	for i, event := range t.BoundaryEvents {
		if event.ID != i+1 || event.FarmID < 1 || event.FarmID > c.FriendCount ||
			event.PlotID < 1 || event.PlotID > c.PlotCount ||
			event.MutationAtMS < 0 || event.MutationAtMS >= event.BoundaryAtMS ||
			event.BoundaryAtMS >= c.DurationMS {
			return fmt.Errorf("friendopportunity: invalid boundary event at index %d", i)
		}
		switch event.ScheduleOperation {
		case ScheduleUpsert:
		case ScheduleCancel:
			if len(event.OnlineTargetIDs) != 0 {
				return fmt.Errorf("friendopportunity: cancel event %d has online targets", event.ID)
			}
		default:
			return fmt.Errorf("friendopportunity: invalid schedule operation %q", event.ScheduleOperation)
		}
		seen := make(map[int]struct{}, len(event.OnlineTargetIDs))
		for _, targetID := range event.OnlineTargetIDs {
			if targetID < 1 || targetID > c.FriendCount || targetID == event.FarmID {
				return fmt.Errorf("friendopportunity: invalid online target %d for event %d", targetID, event.ID)
			}
			if _, exists := seen[targetID]; exists {
				return fmt.Errorf("friendopportunity: duplicate online target %d for event %d", targetID, event.ID)
			}
			seen[targetID] = struct{}{}
		}
	}
	return nil
}

type TraceHashes struct {
	TraceSHA256    string `json:"trace_sha256"`
	SessionSHA256  string `json:"session_trace_sha256"`
	BoundarySHA256 string `json:"boundary_trace_sha256"`
}

func (t Trace) Hashes() TraceHashes {
	session := hashJSON(struct {
		Schema string           `json:"schema"`
		Seed   int64            `json:"seed"`
		Events []SessionRequest `json:"events"`
	}{TraceSchemaVersion, t.SessionSeed, t.SessionRequests})
	boundary := hashJSON(struct {
		Schema string              `json:"schema"`
		Seed   int64               `json:"seed"`
		Events []FarmBoundaryEvent `json:"events"`
	}{TraceSchemaVersion, t.BoundarySeed, t.BoundaryEvents})
	combined := hashJSON(struct {
		Schema   string `json:"schema"`
		Session  string `json:"session_trace_sha256"`
		Boundary string `json:"boundary_trace_sha256"`
	}{TraceSchemaVersion, session, boundary})
	return TraceHashes{TraceSHA256: combined, SessionSHA256: session, BoundarySHA256: boundary}
}

func hashJSON(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sequentialIDs(count int) []int {
	ids := make([]int, count)
	for i := range ids {
		ids[i] = i + 1
	}
	return ids
}

func deriveSeed(seed int64, stream uint64) int64 {
	x := uint64(seed) + stream + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return int64(x ^ (x >> 31))
}

type deterministicRNG struct {
	state uint64
}

func newDeterministicRNG(seed int64) *deterministicRNG {
	state := uint64(seed)
	if state == 0 {
		state = 0x9e3779b97f4a7c15
	}
	return &deterministicRNG{state: state}
}

func (r *deterministicRNG) Uint64() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func (r *deterministicRNG) Intn(n int) int {
	if n <= 0 {
		panic("friendopportunity: Intn called with non-positive n")
	}
	return int(r.Uint64() % uint64(n))
}
