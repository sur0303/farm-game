package friend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNavigationConflict means that another request changed the lossy
// navigation after the caller loaded its snapshot. Callers must load again;
// an old snapshot must never overwrite a newer visited mark or cursor.
var ErrNavigationConflict = errors.New("friend: navigation CAS conflict")

// NavigationSnapshot is the versioned Redis value used by Navigator V3.
// Navigation itself intentionally remains a pure domain value; Version is a
// repository fence and is not part of the lossy product state.
type NavigationSnapshot struct {
	Navigation *Navigation
	Version    uint64
}

// NavigationRepository is the durable runtime port for Navigator V3. The
// visited methods are retained because farm ingress writes the same runtime
// state after a server-side subscription-ready fence.
type NavigationRepository interface {
	NavigationVisitedRepository
	Load(context.Context, uint64) (NavigationSnapshot, error)
	Save(context.Context, NavigationSnapshot) error
}

const (
	navigationStatePayloadField = "payload"
	navigationStateVersionField = "version"
	navigationTopKPayloadField  = "payload"
	navigationTopKVersionField  = "version"
	maxNavigationPayloadBytes   = 64 * 1024
	maxNavigationScanPosition   = 1_000_000
	maxNavigationFences         = maxVisited * 4
)

var saveNavigationScript = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], ARGV[6])
if not current then current = '0' end
if tonumber(current) ~= tonumber(ARGV[1]) then return 0 end

redis.call('HSET', KEYS[1], ARGV[6], ARGV[3], ARGV[7], ARGV[2])

local visited_count = tonumber(ARGV[5])
local index = 8
for i = 1, visited_count do
  redis.call('SADD', KEYS[2], ARGV[index])
  index = index + 1
end

local fence_count = tonumber(ARGV[index])
index = index + 1
for i = 1, fence_count do
  redis.call('SADD', KEYS[3], ARGV[index])
  index = index + 1
end

local ttl = ARGV[4]
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('PEXPIRE', KEYS[2], ttl)
redis.call('PEXPIRE', KEYS[3], ttl)
return 1
`)

// saveNavigationTopKScript publishes the legacy full-tail placeholder, the
// versioned top-k record, the visited set and the fence set in ONE Redis
// Cluster single-slot Lua operation. The optimistic CAS checks only the
// legacy navigation state version: it is the single fence for the whole
// viewer-local key family, so a MarkVisited or an old-binary legacy write
// between Load and Save aborts the whole operation and can never split the
// two versioned records.
var saveNavigationTopKScript = redis.NewScript(`
local current = redis.call('HGET', KEYS[1], 'version')
if not current then current = '0' end
if tonumber(current) ~= tonumber(ARGV[1]) then return 0 end

redis.call('HSET', KEYS[1], 'version', ARGV[3], 'payload', ARGV[2])
redis.call('HSET', KEYS[4], 'version', ARGV[3], 'payload', ARGV[#ARGV])

local visited_count = tonumber(ARGV[5])
local index = 6
for i = 1, visited_count do
  redis.call('SADD', KEYS[2], ARGV[index])
  index = index + 1
end

local fence_count = tonumber(ARGV[index])
index = index + 1
for i = 1, fence_count do
  redis.call('SADD', KEYS[3], ARGV[index])
  index = index + 1
end

local ttl = ARGV[4]
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('PEXPIRE', KEYS[2], ttl)
redis.call('PEXPIRE', KEYS[3], ttl)
redis.call('PEXPIRE', KEYS[4], ttl)
return 1
`)

type navigationWire struct {
	ViewerPlayerID    uint64                 `json:"viewer_player_id"`
	RosterRevision    uint64                 `json:"roster_revision"`
	ScanPosition      int                    `json:"scan_position"`
	Visited           map[uint64]int         `json:"visited,omitempty"`
	CandidateWindow   []CandidateWindowEntry `json:"candidate_window,omitempty"`
	PendingCandidates []CandidateWindowEntry `json:"pending_candidates,omitempty"`
	CoarseScanDone    bool                   `json:"coarse_scan_done,omitempty"`
	WindowMode        NavigationWindowMode   `json:"window_mode,omitempty"`
	WindowSize        int                    `json:"window_size,omitempty"`
	TailComplete      *bool                  `json:"tail_complete,omitempty"`
	ExpireAtMs        int64                  `json:"expire_at_ms"`
	VisitSeq          int                    `json:"visit_seq"`
	MarkedFences      []string               `json:"marked_fences,omitempty"`
}

// navigationTopKWire is the versioned top-k snapshot record. It is stored
// under its own Redis hash so an old binary that ignores new fields can
// never mistake a truncated top-k tail for a complete legacy full-tail.
type navigationTopKWire struct {
	ViewerPlayerID    uint64                 `json:"viewer_player_id"`
	RosterRevision    uint64                 `json:"roster_revision"`
	ScanPosition      int                    `json:"scan_position"`
	Visited           map[uint64]int         `json:"visited,omitempty"`
	CandidateWindow   []CandidateWindowEntry `json:"candidate_window,omitempty"`
	PendingCandidates []CandidateWindowEntry `json:"pending_candidates,omitempty"`
	CoarseScanDone    bool                   `json:"coarse_scan_done,omitempty"`
	WindowMode        NavigationWindowMode   `json:"window_mode,omitempty"`
	WindowSize        int                    `json:"window_size,omitempty"`
	TailComplete      bool                   `json:"tail_complete"`
	ExpireAtMs        int64                  `json:"expire_at_ms"`
	VisitSeq          int                    `json:"visit_seq"`
}

func encodeNavigation(navigation *Navigation) ([]byte, []uint64, []string, error) {
	if err := validateNavigation(navigation); err != nil {
		return nil, nil, nil, err
	}
	tailComplete := navigation.TailComplete
	wireTopK := navigation.WindowMode == NavigationWindowModeTopK
	wire := navigationWire{
		ViewerPlayerID: navigation.ViewerPlayerID, RosterRevision: navigation.RosterRevision,
		ScanPosition: navigation.ScanPosition, Visited: cloneNavigationVisited(navigation.Visited),
		CandidateWindow:   append([]CandidateWindowEntry(nil), navigation.CandidateWindow...),
		PendingCandidates: append([]CandidateWindowEntry(nil), navigation.pendingCandidates...),
		CoarseScanDone:    navigation.coarseScanDone, WindowMode: navigation.WindowMode,
		WindowSize: navigation.WindowSize, TailComplete: &tailComplete,
		ExpireAtMs: navigation.ExpireAtMs, VisitSeq: navigation.visitSeq,
	}
	if wireTopK {
		// The versioned top-k record is the authoritative payload. The legacy
		// key deliberately carries an incomplete full-tail placeholder so an
		// old binary (which ignores the new fields and the new record) can
		// never accept a truncated top-k tail as a complete legacy full-tail:
		// an old reader must rescan, not serve a false NONE. Visited marks and
		// marked fences are preserved in the placeholder: the fences are also
		// published to the shared Redis fence set by the save script so the
		// in-memory markedFences never depend on the top-k JSON alone.
		wire.ScanPosition = 0
		wire.CandidateWindow = nil
		wire.PendingCandidates = nil
		wire.CoarseScanDone = false
		wire.WindowMode = NavigationWindowModeFullTail
		wire.WindowSize = 0
		wire.TailComplete = nil
	}
	for fence := range navigation.markedFences {
		wire.MarkedFences = append(wire.MarkedFences, fence)
	}
	sort.Strings(wire.MarkedFences)
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: navigation encode: %v", ErrFriendReconcile, err)
	}
	if len(payload) > maxNavigationPayloadBytes {
		return nil, nil, nil, ErrFriendAuthorityCapacity
	}
	visited := make([]uint64, 0, len(navigation.Visited))
	for farmID := range navigation.Visited {
		visited = append(visited, farmID)
	}
	sort.Slice(visited, func(i, j int) bool { return visited[i] < visited[j] })
	if len(visited) > maxVisited {
		return nil, nil, nil, ErrFriendAuthorityCapacity
	}
	if len(wire.MarkedFences) > maxNavigationFences {
		return nil, nil, nil, ErrFriendAuthorityCapacity
	}
	return payload, visited, wire.MarkedFences, nil
}

func decodeNavigation(payload string, viewer uint64) (*Navigation, error) {
	var wire navigationWire
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		return nil, fmt.Errorf("%w: navigation payload: %v", ErrFriendReconcile, err)
	}
	if wire.ViewerPlayerID != viewer {
		return nil, ErrFriendReconcile
	}
	marked := make(map[string]struct{}, len(wire.MarkedFences))
	for _, fence := range wire.MarkedFences {
		if fence == "" {
			return nil, ErrFriendReconcile
		}
		marked[fence] = struct{}{}
	}
	windowMode := ParseNavigationWindowMode(string(wire.WindowMode))
	navigation := &Navigation{
		ViewerPlayerID: wire.ViewerPlayerID, RosterRevision: wire.RosterRevision,
		ScanPosition: wire.ScanPosition, Visited: cloneNavigationVisited(wire.Visited),
		CandidateWindow:   append([]CandidateWindowEntry(nil), wire.CandidateWindow...),
		pendingCandidates: append([]CandidateWindowEntry(nil), wire.PendingCandidates...),
		coarseScanDone:    wire.CoarseScanDone, WindowMode: windowMode, WindowSize: wire.WindowSize,
		ExpireAtMs: wire.ExpireAtMs, visitSeq: wire.VisitSeq, markedFences: marked,
		scannedFarmIDs: make(map[uint64]struct{}),
	}
	// Historical full-tail payloads omit TailComplete. A completed coarse scan
	// in full-tail mode is the complete tail; unknown/truncated top-k payloads
	// must keep TailComplete=false so exhaustion rescans instead of NONE.
	switch {
	case wire.TailComplete != nil:
		navigation.TailComplete = *wire.TailComplete
	case wire.CoarseScanDone && windowMode == NavigationWindowModeFullTail:
		navigation.TailComplete = true
	default:
		navigation.TailComplete = false
	}
	if err := validateNavigation(navigation); err != nil {
		return nil, err
	}
	if navigation.markedFences == nil {
		navigation.markedFences = make(map[string]struct{})
	}
	return navigation, nil
}

func encodeNavigationTopK(navigation *Navigation) ([]byte, error) {
	if err := validateNavigation(navigation); err != nil {
		return nil, err
	}
	wire := navigationTopKWire{
		ViewerPlayerID: navigation.ViewerPlayerID, RosterRevision: navigation.RosterRevision,
		ScanPosition: navigation.ScanPosition, Visited: cloneNavigationVisited(navigation.Visited),
		CandidateWindow:   append([]CandidateWindowEntry(nil), navigation.CandidateWindow...),
		PendingCandidates: append([]CandidateWindowEntry(nil), navigation.pendingCandidates...),
		CoarseScanDone:    navigation.coarseScanDone, WindowMode: navigation.WindowMode,
		WindowSize: navigation.WindowSize, TailComplete: navigation.TailComplete,
		ExpireAtMs: navigation.ExpireAtMs, VisitSeq: navigation.visitSeq,
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: navigation top-k encode: %v", ErrFriendReconcile, err)
	}
	if len(payload) > maxNavigationPayloadBytes {
		return nil, ErrFriendAuthorityCapacity
	}
	return payload, nil
}

func decodeNavigationTopK(payload string, viewer uint64) (*Navigation, error) {
	var wire navigationTopKWire
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		return nil, fmt.Errorf("%w: navigation top-k payload: %v", ErrFriendReconcile, err)
	}
	if wire.ViewerPlayerID != viewer {
		return nil, ErrFriendReconcile
	}
	navigation := &Navigation{
		ViewerPlayerID: wire.ViewerPlayerID, RosterRevision: wire.RosterRevision,
		ScanPosition: wire.ScanPosition, Visited: cloneNavigationVisited(wire.Visited),
		CandidateWindow:   append([]CandidateWindowEntry(nil), wire.CandidateWindow...),
		pendingCandidates: append([]CandidateWindowEntry(nil), wire.PendingCandidates...),
		coarseScanDone:    wire.CoarseScanDone, WindowMode: ParseNavigationWindowMode(string(wire.WindowMode)),
		WindowSize: wire.WindowSize, TailComplete: wire.TailComplete,
		ExpireAtMs: wire.ExpireAtMs, visitSeq: wire.VisitSeq,
		markedFences: make(map[string]struct{}), scannedFarmIDs: make(map[uint64]struct{}),
	}
	if err := validateNavigation(navigation); err != nil {
		return nil, err
	}
	return navigation, nil
}

func decodeLegacyRosterRevision(payload string) uint64 {
	var wire navigationWire
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		return 0
	}
	return wire.RosterRevision
}

// navigationFieldVersion parses the legacy version field. The legacy version
// is the single optimistic-CAS fence for the viewer-local key family.
func navigationFieldVersion(fields map[string]string, field string) (uint64, error) {
	raw := fields[field]
	if raw == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, ErrFriendReconcile
	}
	return parsed, nil
}

// decodeNavigationRecords picks the authoritative payload for this load.
//
// In full-tail mode only the legacy record is read: the top-k record is
// never touched on the normal path and stale top-k keys expire naturally.
//
// In top-k mode the versioned top-k record is served only when it is fresh:
// its version must equal the legacy fence version (one atomic CAS wrote both)
// and its roster revision must match the legacy payload. A missing top-k
// record, a corrupt top-k payload, or any version/revision mismatch (an
// old-binary legacy-only save, or a visit that advanced only the legacy
// fence) fails closed to the legacy record. The caller reconciles that state
// against the active mode and rebuilds, healing the top-k record on the next
// atomic save. A version split can therefore never wedge future saves.
func (r *RedisNavigationRepository) decodeNavigationRecords(fields, topKFields map[string]string, viewer uint64) (uint64, *Navigation, error) {
	version, err := navigationFieldVersion(fields, navigationStateVersionField)
	if err != nil {
		return 0, nil, err
	}
	legacyPayload := fields[navigationStatePayloadField]
	if !r.topKEnabled() {
		if legacyPayload == "" {
			return version, nil, nil
		}
		navigation, decodeErr := decodeNavigation(legacyPayload, viewer)
		if decodeErr != nil {
			return 0, nil, decodeErr
		}
		return version, navigation, nil
	}
	topKPayload := topKFields[navigationTopKPayloadField]
	if topKPayload == "" || legacyPayload == "" {
		if legacyPayload == "" {
			return version, nil, nil
		}
		navigation, decodeErr := decodeNavigation(legacyPayload, viewer)
		if decodeErr != nil {
			return 0, nil, decodeErr
		}
		return version, navigation, nil
	}
	topKVersion, versionErr := navigationFieldVersion(topKFields, navigationTopKVersionField)
	if versionErr != nil {
		return 0, nil, versionErr
	}
	topKNavigation, topKErr := decodeNavigationTopK(topKPayload, viewer)
	if topKErr != nil {
		// Corrupt top-k payloads fail closed to the legacy record instead of
		// wedging the whole navigation read.
		navigation, legacyErr := decodeNavigation(legacyPayload, viewer)
		if legacyErr != nil {
			return 0, nil, legacyErr
		}
		return version, navigation, nil
	}
	if topKVersion != version || topKNavigation.RosterRevision != decodeLegacyRosterRevision(legacyPayload) ||
		topKNavigation.WindowMode != NavigationWindowModeTopK {
		navigation, legacyErr := decodeNavigation(legacyPayload, viewer)
		if legacyErr != nil {
			return 0, nil, legacyErr
		}
		return version, navigation, nil
	}
	return version, topKNavigation, nil
}

func validateNavigation(navigation *Navigation) error {
	if navigation == nil || navigation.ViewerPlayerID == 0 || navigation.ScanPosition < 0 ||
		navigation.ScanPosition > maxNavigationScanPosition || len(navigation.Visited) > maxVisited ||
		len(navigation.CandidateWindow) > CandidateWindowSize ||
		len(navigation.markedFences) > maxNavigationFences {
		return fmt.Errorf("%w: navigation state", ErrInvalidCommand)
	}
	pendingLimit := maxVisited
	if navigation.WindowMode == NavigationWindowModeTopK {
		if limit := navigation.TopKWindowLimit(); limit > 0 {
			pendingLimit = limit
		}
	}
	if len(navigation.pendingCandidates) > pendingLimit || len(navigation.exactNegatives) > maxVisited {
		return fmt.Errorf("%w: navigation state", ErrInvalidCommand)
	}
	if navigation.Visited == nil {
		navigation.Visited = make(map[uint64]int)
	}
	if navigation.markedFences == nil {
		navigation.markedFences = make(map[string]struct{})
	}
	seenWindow := make(map[uint64]struct{}, len(navigation.CandidateWindow))
	for farmID, ordinal := range navigation.Visited {
		if farmID == 0 || farmID == navigation.ViewerPlayerID || ordinal <= 0 {
			return fmt.Errorf("%w: navigation visited", ErrInvalidCommand)
		}
	}
	for _, entry := range navigation.CandidateWindow {
		farmID := entry.FarmID
		if farmID == 0 || farmID == navigation.ViewerPlayerID {
			return fmt.Errorf("%w: navigation candidate window", ErrInvalidCommand)
		}
		if _, duplicate := seenWindow[farmID]; duplicate {
			return fmt.Errorf("%w: duplicate navigation candidate", ErrInvalidCommand)
		}
		if _, visited := navigation.Visited[farmID]; visited {
			return fmt.Errorf("%w: visited navigation candidate", ErrInvalidCommand)
		}
		seenWindow[farmID] = struct{}{}
	}
	for _, entry := range navigation.pendingCandidates {
		farmID := entry.FarmID
		if farmID == 0 || farmID == navigation.ViewerPlayerID {
			return fmt.Errorf("%w: pending navigation candidate", ErrInvalidCommand)
		}
		if _, duplicate := seenWindow[farmID]; duplicate {
			return fmt.Errorf("%w: duplicate pending navigation candidate", ErrInvalidCommand)
		}
		if _, visited := navigation.Visited[farmID]; visited {
			return fmt.Errorf("%w: visited pending navigation candidate", ErrInvalidCommand)
		}
		seenWindow[farmID] = struct{}{}
	}
	maxOrdinal := navigation.visitSeq
	for _, ordinal := range navigation.Visited {
		if ordinal > maxOrdinal {
			maxOrdinal = ordinal
		}
	}
	if maxOrdinal < 0 {
		return fmt.Errorf("%w: navigation visit sequence", ErrInvalidCommand)
	}
	navigation.visitSeq = maxOrdinal
	return nil
}

func cloneNavigationVisited(input map[uint64]int) map[uint64]int {
	if len(input) == 0 {
		return make(map[uint64]int)
	}
	output := make(map[uint64]int, len(input))
	for farmID, ordinal := range input {
		output[farmID] = ordinal
	}
	return output
}

func navigationFenceMember(fence VisitFence) string {
	return strconv.FormatUint(fence.ActiveSessionEpoch, 10) + ":" + fence.SubscriptionID + ":" + strconv.FormatUint(fence.TargetFarmID, 10)
}

func navigationVisitedFromRedis(ctx context.Context, client redis.UniversalClient, key string) (map[uint64]struct{}, error) {
	values, err := client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return navigationVisitedFromMembers(values)
}

func navigationVisitedFromMembers(values []string) (map[uint64]struct{}, error) {
	if len(values) > maxVisited {
		return nil, ErrFriendAuthorityCapacity
	}
	visited := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		farmID, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || farmID == 0 {
			return nil, ErrFriendReconcile
		}
		visited[farmID] = struct{}{}
	}
	return visited, nil
}

func navigationFencesFromRedis(ctx context.Context, client redis.UniversalClient, key string) (map[string]struct{}, error) {
	values, err := client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return navigationFencesFromMembers(values)
}

func navigationFencesFromMembers(values []string) (map[string]struct{}, error) {
	if len(values) > maxNavigationFences {
		return nil, ErrFriendAuthorityCapacity
	}
	fences := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return nil, ErrFriendReconcile
		}
		fences[value] = struct{}{}
	}
	return fences, nil
}

func mergeNavigationVisited(navigation *Navigation, visited map[uint64]struct{}) {
	if len(visited) == 0 {
		return
	}
	if navigation.Visited == nil {
		navigation.Visited = make(map[uint64]int)
	}
	for farmID := range visited {
		if _, exists := navigation.Visited[farmID]; exists {
			continue
		}
		if len(navigation.Visited) >= maxVisited {
			oldestFarm, oldestOrdinal := uint64(0), int(^uint(0)>>1)
			for existingFarm, ordinal := range navigation.Visited {
				if ordinal < oldestOrdinal {
					oldestFarm, oldestOrdinal = existingFarm, ordinal
				}
			}
			delete(navigation.Visited, oldestFarm)
		}
		navigation.visitSeq++
		navigation.Visited[farmID] = navigation.visitSeq
	}
	window := navigation.CandidateWindow[:0]
	for _, entry := range navigation.CandidateWindow {
		if _, seen := navigation.Visited[entry.FarmID]; !seen {
			window = append(window, entry)
		}
	}
	navigation.CandidateWindow = window
	pending := navigation.pendingCandidates[:0]
	for _, entry := range navigation.pendingCandidates {
		if _, seen := navigation.Visited[entry.FarmID]; !seen {
			pending = append(pending, entry)
		}
	}
	navigation.pendingCandidates = pending
}

// Load returns an empty snapshot for a missing runtime key. A visited mark may
// exist before the first GET_NEXT call; in that case a partial Navigation is
// returned so the caller can initialize its roster fields without losing it.
func (r *RedisNavigationRepository) Load(ctx context.Context, viewer uint64) (NavigationSnapshot, error) {
	if r == nil || viewer == 0 {
		return NavigationSnapshot{}, fmt.Errorf("%w: navigation viewer", ErrInvalidCommand)
	}
	stateKey, err := r.keys.FriendNavigationStateKey(viewer)
	if err != nil {
		return NavigationSnapshot{}, err
	}
	visitedKey, err := r.keys.FriendNavigationVisitedKey(viewer)
	if err != nil {
		return NavigationSnapshot{}, err
	}
	fencesKey, err := r.keys.FriendNavigationFencesKey(viewer)
	if err != nil {
		return NavigationSnapshot{}, err
	}
	var topKStateKey string
	if r.topKEnabled() {
		key, keyErr := r.keys.FriendNavigationTopKStateKey(viewer)
		if keyErr != nil {
			return NavigationSnapshot{}, keyErr
		}
		topKStateKey = key.String()
	}
	pipe := r.client.Pipeline()
	stateCmd := pipe.HGetAll(ctx, stateKey.String())
	var topKCmd *redis.MapStringStringCmd
	if topKStateKey != "" {
		topKCmd = pipe.HGetAll(ctx, topKStateKey)
	}
	visitedCmd := pipe.SMembers(ctx, visitedKey.String())
	fencesCmd := pipe.SMembers(ctx, fencesKey.String())
	if _, err = pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return NavigationSnapshot{}, err
	}
	fields := stateCmd.Val()
	var topKFields map[string]string
	if topKCmd != nil {
		topKFields = topKCmd.Val()
	}
	version, navigation, err := r.decodeNavigationRecords(fields, topKFields, viewer)
	if err != nil {
		return NavigationSnapshot{}, err
	}
	visited, err := navigationVisitedFromMembers(visitedCmd.Val())
	if err != nil {
		return NavigationSnapshot{}, err
	}
	fences, err := navigationFencesFromMembers(fencesCmd.Val())
	if err != nil {
		return NavigationSnapshot{}, err
	}
	if navigation == nil && (len(visited) != 0 || len(fences) != 0) {
		navigation = &Navigation{ViewerPlayerID: viewer, Visited: make(map[uint64]int), markedFences: make(map[string]struct{})}
	}
	if navigation == nil {
		return NavigationSnapshot{Version: version}, nil
	}
	mergeNavigationVisited(navigation, visited)
	for fence := range fences {
		navigation.markedFences[fence] = struct{}{}
	}
	if err := validateNavigation(navigation); err != nil {
		return NavigationSnapshot{}, err
	}
	return NavigationSnapshot{Navigation: navigation, Version: version}, nil
}

// Save performs a single-slot optimistic CAS. The script only adds visited
// and fence members; it never removes the Redis-side sets, so a mark that
// races with a stale Save remains visible on the next Load.
func (r *RedisNavigationRepository) Save(ctx context.Context, snapshot NavigationSnapshot) error {
	if r == nil || snapshot.Navigation == nil {
		return fmt.Errorf("%w: navigation snapshot", ErrInvalidCommand)
	}
	navigation := snapshot.Navigation
	payload, visited, fences, err := encodeNavigation(navigation)
	if err != nil {
		return err
	}
	if snapshot.Version == ^uint64(0) {
		return ErrNavigationConflict
	}
	stateKey, err := r.keys.FriendNavigationStateKey(navigation.ViewerPlayerID)
	if err != nil {
		return err
	}
	visitedKey, err := r.keys.FriendNavigationVisitedKey(navigation.ViewerPlayerID)
	if err != nil {
		return err
	}
	fencesKey, err := r.keys.FriendNavigationFencesKey(navigation.ViewerPlayerID)
	if err != nil {
		return err
	}
	if navigation.WindowMode != NavigationWindowModeTopK || !r.topKEnabled() {
		// Full-tail is the historical default path: only the three legacy
		// keys are touched. Existing stale top-k records are left to expire
		// naturally; a later top-k enable detects them as stale and rebuilds.
		// A top-k-configured repository still writes a full-tail navigation
		// through the legacy path so the top-k record is never half-written.
		args := make([]interface{}, 0, 8+len(visited)+len(fences))
		args = append(args, snapshot.Version, string(payload), snapshot.Version+1, r.ttl.Milliseconds(), len(visited),
			navigationStateVersionField, navigationStatePayloadField)
		for _, farmID := range visited {
			args = append(args, farmID)
		}
		args = append(args, len(fences))
		for _, fence := range fences {
			args = append(args, fence)
		}
		result, runErr := saveNavigationScript.Run(ctx, r.client,
			[]string{stateKey.String(), visitedKey.String(), fencesKey.String()}, args...).Int()
		if runErr != nil {
			return runErr
		}
		if result != 1 {
			return ErrNavigationConflict
		}
		return nil
	}
	topKStateKey, err := r.keys.FriendNavigationTopKStateKey(navigation.ViewerPlayerID)
	if err != nil {
		return err
	}
	topKPayload, topKErr := encodeNavigationTopK(navigation)
	if topKErr != nil {
		return topKErr
	}
	// One single-slot Lua operation CASes the legacy fence version and
	// publishes the legacy placeholder, the top-k record, the visited set and
	// the fence set together, so no interleaved MarkVisited or old-binary
	// legacy write can ever split the two versioned records.
	topKArgs := make([]interface{}, 0, 9+len(visited)+len(fences))
	topKArgs = append(topKArgs, snapshot.Version, string(payload), snapshot.Version+1, r.ttl.Milliseconds(), len(visited))
	for _, farmID := range visited {
		topKArgs = append(topKArgs, farmID)
	}
	topKArgs = append(topKArgs, len(fences))
	for _, fence := range fences {
		topKArgs = append(topKArgs, fence)
	}
	topKArgs = append(topKArgs, string(topKPayload))
	topKResult, topKErr := saveNavigationTopKScript.Run(ctx, r.client,
		[]string{stateKey.String(), visitedKey.String(), fencesKey.String(), topKStateKey.String()}, topKArgs...).Int()
	if topKErr != nil {
		return topKErr
	}
	if topKResult != 1 {
		return ErrNavigationConflict
	}
	return nil
}

// LoadNavigation and SaveNavigation are descriptive aliases for callers that
// prefer the domain name at composition boundaries.
func (r *RedisNavigationRepository) LoadNavigation(ctx context.Context, viewer uint64) (NavigationSnapshot, error) {
	return r.Load(ctx, viewer)
}

func (r *RedisNavigationRepository) SaveNavigation(ctx context.Context, snapshot NavigationSnapshot) error {
	return r.Save(ctx, snapshot)
}

// MemoryNavigationRepository is a small concurrency-safe adapter for service
// tests and local composition. It follows the same version/merge semantics as
// the Redis implementation.
type MemoryNavigationRepository struct {
	mu     sync.Mutex
	states map[uint64]NavigationSnapshot
	ttl    time.Duration
}

func NewMemoryNavigationRepository(ttl time.Duration) *MemoryNavigationRepository {
	if ttl <= 0 {
		ttl = DefaultNavigationVisitedTTL
	}
	return &MemoryNavigationRepository{states: make(map[uint64]NavigationSnapshot), ttl: ttl}
}

func cloneNavigation(input *Navigation) *Navigation {
	if input == nil {
		return nil
	}
	output := *input
	output.Visited = cloneNavigationVisited(input.Visited)
	output.CandidateWindow = append([]CandidateWindowEntry(nil), input.CandidateWindow...)
	output.pendingCandidates = append([]CandidateWindowEntry(nil), input.pendingCandidates...)
	output.scannedFarmIDs = make(map[uint64]struct{}, len(input.scannedFarmIDs))
	for farmID := range input.scannedFarmIDs {
		output.scannedFarmIDs[farmID] = struct{}{}
	}
	output.exactNegatives = make(map[uint64]struct{}, len(input.exactNegatives))
	for farmID := range input.exactNegatives {
		output.exactNegatives[farmID] = struct{}{}
	}
	output.markedFences = make(map[string]struct{}, len(input.markedFences))
	for fence := range input.markedFences {
		output.markedFences[fence] = struct{}{}
	}
	return &output
}

func (r *MemoryNavigationRepository) Load(_ context.Context, viewer uint64) (NavigationSnapshot, error) {
	if r == nil || viewer == 0 {
		return NavigationSnapshot{}, fmt.Errorf("%w: navigation viewer", ErrInvalidCommand)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.states[viewer]
	if !ok {
		return NavigationSnapshot{}, nil
	}
	snapshot.Navigation = cloneNavigation(snapshot.Navigation)
	return snapshot, nil
}

func (r *MemoryNavigationRepository) Save(_ context.Context, snapshot NavigationSnapshot) error {
	if r == nil || snapshot.Navigation == nil {
		return fmt.Errorf("%w: navigation snapshot", ErrInvalidCommand)
	}
	if err := validateNavigation(snapshot.Navigation); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.states[snapshot.Navigation.ViewerPlayerID]
	if (!ok && snapshot.Version != 0) || (ok && current.Version != snapshot.Version) {
		return ErrNavigationConflict
	}
	if ok && current.Navigation != nil {
		merged := cloneNavigation(snapshot.Navigation)
		mergeNavigationVisited(merged, current.Navigation.VisitedAsSet())
		for fence := range current.Navigation.markedFences {
			merged.markedFences[fence] = struct{}{}
		}
		snapshot.Navigation = merged
	}
	snapshot.Version++
	snapshot.Navigation = cloneNavigation(snapshot.Navigation)
	r.states[snapshot.Navigation.ViewerPlayerID] = snapshot
	return nil
}

func (r *MemoryNavigationRepository) MarkVisited(_ context.Context, fence VisitFence) error {
	if fence.ViewerPlayerID == 0 || fence.TargetFarmID == 0 || fence.ViewerPlayerID == fence.TargetFarmID ||
		fence.ActiveSessionEpoch == 0 || fence.SubscriptionID == "" {
		return fmt.Errorf("%w: friend navigation VisitFence", ErrInvalidCommand)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := r.states[fence.ViewerPlayerID]
	if snapshot.Navigation == nil {
		snapshot.Navigation = NewNavigation(fence.ViewerPlayerID, 0, r.ttl.Milliseconds(), time.Now().UnixMilli())
	}
	if snapshot.Navigation.MarkVisited(fence) {
		snapshot.Version++
	}
	r.states[fence.ViewerPlayerID] = snapshot
	return nil
}

func (r *MemoryNavigationRepository) ListVisited(_ context.Context, viewer uint64) (map[uint64]struct{}, error) {
	if r == nil || viewer == 0 {
		return nil, fmt.Errorf("%w: navigation viewer", ErrInvalidCommand)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := r.states[viewer]
	if snapshot.Navigation == nil {
		return map[uint64]struct{}{}, nil
	}
	return snapshot.Navigation.VisitedAsSet(), nil
}

func (n *Navigation) VisitedAsSet() map[uint64]struct{} {
	visited := make(map[uint64]struct{}, len(n.Visited))
	for farmID := range n.Visited {
		visited[farmID] = struct{}{}
	}
	return visited
}

var _ NavigationRepository = (*RedisNavigationRepository)(nil)
var _ NavigationRepository = (*MemoryNavigationRepository)(nil)
