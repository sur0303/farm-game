package authority

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// Work lease primitives (31 §7). All times are adjudicated by Redis TIME;
// callers pass only requested TTLs and receive accepted TTL, lease epoch and
// work version. Work records are created exclusively by the current frozen
// CommitActorMutationWithOrders script in the same slot.
type WorkLeaseStore struct {
	client      *Store
	leaseLimits authoritystore.LeaseLimits
}

// ErrSourceOrderNotFound is returned by the read-only SourceOrder view used by
// the DirectDispatcher.  It is deliberately separate from a claim status:
// reading an order must never acquire its work lease.
var ErrSourceOrderNotFound = errors.New("redisauthority: source order not found")

// WorkLease exposes the work lease facade bound to this store's client,
// registry and key builder.
func (s *Store) WorkLease() *WorkLeaseStore {
	return &WorkLeaseStore{client: s, leaseLimits: s.opts.LeaseLimits}
}

type WorkClaimStatus uint8

const (
	WorkClaimUnknown WorkClaimStatus = iota
	WorkClaimClaimed
	WorkClaimLeaseHeld
	WorkClaimNotDue
	WorkClaimNotFound
	WorkClaimNotClaimable
	WorkClaimReconcileRequired
	WorkClaimUnavailableNotSent
	WorkClaimTimeoutUnknown
)

func (s WorkClaimStatus) String() string {
	switch s {
	case WorkClaimClaimed:
		return "CLAIMED"
	case WorkClaimLeaseHeld:
		return "LEASE_HELD"
	case WorkClaimNotDue:
		return "NOT_DUE"
	case WorkClaimNotFound:
		return "NOT_FOUND"
	case WorkClaimNotClaimable:
		return "NOT_CLAIMABLE"
	case WorkClaimReconcileRequired:
		return "RECONCILE_REQUIRED"
	case WorkClaimUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case WorkClaimTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type WorkClaim struct {
	Status            WorkClaimStatus
	WorkID            string
	WorkType          string
	SubjectPlayerID   uint64
	OperationID       string
	Payload           []byte
	PayloadHash       string
	RetryCount        uint64
	WorkVersion       uint64
	Generation        uint64
	LeaseEpoch        uint64
	LeaseExpireAtMs   int64
	LeaseHolderBootID string
	AcceptedTTLms     int64
	// LeaseConflictExpireAtMs carries the current holder's expiry on LEASE_HELD.
	LeaseConflictExpireAtMs int64
}

// DueWorkRef is the bounded discovery view of the existing Source DueIndex.
// The WorkRef is only a locator; callers must still load/claim the SourceOrder
// before treating it as executable work.
type DueWorkRef struct {
	Ref     workidentity.WorkRef
	DueAtMs int64
	Member  string
}

// DueWorkCursor is an inclusive, mutation-stable continuation in Redis sorted
// set order (score, then member). Index and an internal client identity bind it
// to the exact physical Redis client/namespace/DueIndex tuple. Callers must only
// feed NextCursor back to the same bucket read; it is a discovery hint, not an
// execution or snapshot fence.
//
// The cursor names the first physical member not consumed by the previous
// page. It remains valid if that member, or any members before it, are removed:
// the next atomic read finds the first current member greater than or equal to
// the saved pair instead of relying on a mutable rank.
type DueWorkCursor struct {
	Score          float64
	Member         string
	Index          string
	clientIdentity string
}

// IsZero reports whether this cursor starts a new physical traversal.
func (c DueWorkCursor) IsZero() bool {
	return c.Index == "" && c.Member == "" && c.Score == 0 && c.clientIdentity == ""
}

// DueWorkPage is a family-aware, bounded SourceOrder discovery page. RawScanned
// counts every physical due member consumed, including schedules, malformed
// members, and SourceOrders from other families. NextCursor is the inclusive
// first unconsumed physical member, so applying a family limit cannot skip refs
// already fetched from Redis. Complete is deliberately true only when a
// cursor-zero atomic read observed the entire due prefix. A non-zero traversal
// that reaches its suffix returns a zero NextCursor with Complete false; callers
// must not turn that suffix into a whole-index empty-backlog claim.
type DueWorkPage struct {
	Refs       []DueWorkRef
	NextCursor DueWorkCursor
	RawScanned int
	Complete   bool
}

const (
	maxDueIndexDiscoveryEntries     = 4096
	maxDueIndexCareDiscoveryEntries = 65536
	// Schedules currently share the bucket DueIndex with immutable orders.
	// Discovery must page past a bounded non-schedule prefix; taking only the
	// first limit members can permanently starve a valid sr1_ schedule when
	// older wr1_ orders occupy that window.
	maxDueIndexScheduleDiscoveryEntries = 65536
)

// SourceOrderSnapshot is a read-only projection of the existing SourceOrder
// HASH.  It is used by the direct fast path to build a Coordinator request;
// it does not introduce another Redis record or lifecycle.
type SourceOrderSnapshot struct {
	WorkID                     string
	WorkType                   string
	PlacementKind              string
	BucketVersion              uint32
	BucketID                   uint32
	SourceAuthorityKind        string
	CanonicalSourceAuthorityID string
	AuthorityLayoutVersion     string
	ActorRouteDomain           string
	SubjectPlayerID            uint64
	OperationID                string
	PayloadHash                string
	State                      string
	WorkVersion                uint64
	CurrentStep                string
	TypedPayload               []byte
	NextDueAtMs                int64
	RetryCount                 uint64
	LastErrorClass             string
	DeadlineAtMs               int64
	MaxRetryCount              uint64
	ReconcileRequiredAtMs      int64
	LeaseHolderID              string
	LeaseHolderBootID          string
	LeaseEpoch                 uint64
	LeaseTokenHash             string
	LeaseExpireAtMs            int64
	CreatedAtMs                int64
	UpdatedAtMs                int64
	CompletedAtMs              int64
}

type WorkLeaseStatus uint8

const (
	WorkLeaseUnknown WorkLeaseStatus = iota
	WorkLeaseRenewed
	WorkLeaseCompleted
	WorkLeaseAlreadyCompleted
	WorkLeaseFenceMismatch
	WorkLeaseExpired
	WorkLeaseStaleVersion
	WorkLeaseStaleGeneration
	WorkLeaseNotFound
	WorkLeaseReconcileRequired
	WorkLeaseUnavailableNotSent
	WorkLeaseTimeoutUnknown
)

func (s WorkLeaseStatus) String() string {
	switch s {
	case WorkLeaseRenewed:
		return "RENEWED"
	case WorkLeaseCompleted:
		return "COMPLETED"
	case WorkLeaseAlreadyCompleted:
		return "ALREADY_COMPLETED"
	case WorkLeaseFenceMismatch:
		return "FENCE_MISMATCH"
	case WorkLeaseExpired:
		return "LEASE_EXPIRED"
	case WorkLeaseStaleVersion:
		return "STALE_VERSION"
	case WorkLeaseStaleGeneration:
		return "STALE_GENERATION"
	case WorkLeaseNotFound:
		return "NOT_FOUND"
	case WorkLeaseReconcileRequired:
		return "RECONCILE_REQUIRED"
	case WorkLeaseUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case WorkLeaseTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type WorkLeaseRenewResult struct {
	Status          WorkLeaseStatus
	LeaseExpireAtMs int64
	AcceptedTTLms   int64
}

type WorkCompleteResult struct {
	Status WorkLeaseStatus
}

// WorkCompletionProof is the bounded terminal Participant evidence required
// by the CompleteWork script family. Hashes and scalar identity are computed
// before Lua; Lua only verifies that the proof is bound to the same SourceOrder
// and fence.
type WorkCompletionProof struct {
	SourceAuthorityKind        string
	CanonicalSourceAuthorityID string
	AuthorityLayoutVersion     string
	ActorRouteDomain           string
	PayloadHash                string
	InvocationID               string
	ParticipantStep            string
	ParticipantOutcome         string
	ParticipantResultHash      string
	ParticipantResultVersion   uint64
	ResultProofHash            string
}

// WorkCompletionProofHash is the bounded, deterministic proof-of-proof hash
// accepted by the CompleteWork script family. It covers every result-proof
// scalar except the hash itself; callers must compute it before entering Lua.
func WorkCompletionProofHash(proof WorkCompletionProof) string {
	h := sha256.New()
	for _, field := range []string{
		"CARE-COMPLETE-PROOF-V1", proof.SourceAuthorityKind,
		proof.CanonicalSourceAuthorityID, proof.AuthorityLayoutVersion,
		proof.ActorRouteDomain, proof.PayloadHash, proof.InvocationID,
		proof.ParticipantStep, proof.ParticipantOutcome, proof.ParticipantResultHash,
		strconv.FormatUint(proof.ParticipantResultVersion, 10),
	} {
		_, _ = h.Write([]byte(strconv.Itoa(len(field))))
		_, _ = h.Write([]byte(":"))
		_, _ = h.Write([]byte(field))
		_, _ = h.Write([]byte("|"))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

type WorkRescheduleResult struct {
	Status      WorkLeaseStatus
	RetryCount  uint64
	NextDueAtMs int64
}

type ScheduledDueRef struct {
	ScheduleType string
	ScheduleID   string
	DueAtMs      int64
	Member       string
}

// ScheduledWorkSnapshot is the bounded read-only view of a mutable source
// schedule. Generation is the source supersession fence; WorkVersion is the
// claim/advance fence within that generation.
type ScheduledWorkSnapshot struct {
	ScheduleID        string
	ScheduleType      string
	Generation        uint64
	State             string
	WorkVersion       uint64
	SubjectPlayerID   uint64
	OperationID       string
	PayloadHash       string
	Payload           []byte
	NextDueAtMs       int64
	LeaseHolderID     string
	LeaseHolderBootID string
	LeaseEpoch        uint64
	LeaseTokenHash    string
	LeaseExpireAtMs   int64
	CreatedAtMs       int64
	UpdatedAtMs       int64
	CompletedAtMs     int64
}

type ScheduledWorkAdvanceResult struct {
	Status      WorkLeaseStatus
	WorkVersion uint64
	Generation  uint64
	NextDueAtMs int64
}

type ScanLeaseStatus uint8

const (
	ScanLeaseUnknown ScanLeaseStatus = iota
	ScanLeaseClaimed
	ScanLeaseHeldByOther
	ScanLeaseRenewed
	ScanLeaseNotFound
	ScanLeaseFenceMismatch
	ScanLeaseExpired
	ScanLeaseUnavailableNotSent
	ScanLeaseTimeoutUnknown
)

type ScanLeaseResult struct {
	Status          ScanLeaseStatus
	LeaseEpoch      uint64
	LeaseExpireAtMs int64
	AcceptedTTLms   int64
}

func (s ScanLeaseStatus) String() string {
	switch s {
	case ScanLeaseClaimed:
		return "CLAIMED"
	case ScanLeaseHeldByOther:
		return "HELD_BY_OTHER"
	case ScanLeaseRenewed:
		return "RENEWED"
	case ScanLeaseNotFound:
		return "NOT_FOUND"
	case ScanLeaseFenceMismatch:
		return "FENCE_MISMATCH"
	case ScanLeaseExpired:
		return "LEASE_EXPIRED"
	case ScanLeaseUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case ScanLeaseTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

func (w *WorkLeaseStore) keysFor(bucket rediskey.AuthorityBucketRef, workType, workID string) (tag string, workKey, dueKey, bucketMetaKey rediskey.RedisKey, workRef string, err error) {
	if err = bucket.Validate(); err != nil {
		return "", "", "", "", "", err
	}
	if workidentity.ValidateStableOpaqueID(workID) != nil {
		return "", "", "", "", "", fmt.Errorf("%w: work id", authoritystore.ErrInvalidRequest)
	}
	tag, err = w.client.keys.HashTag(bucket)
	if err != nil {
		return "", "", "", "", "", err
	}
	if workKey, err = w.client.keys.SourceOrder(bucket, workType, workID); err != nil {
		return "", "", "", "", "", err
	}
	if dueKey, err = w.client.keys.SourceDueIndex(bucket); err != nil {
		return "", "", "", "", "", err
	}
	if bucketMetaKey, err = w.client.keys.BucketMeta(bucket); err != nil {
		return "", "", "", "", "", err
	}
	if workRef, err = workidentity.EncodeWorkRefV1(workType, workID); err != nil {
		return "", "", "", "", "", fmt.Errorf("%w: work ref: %v", authoritystore.ErrInvalidRequest, err)
	}
	if err = rediskey.AssertSameSlot(workKey, dueKey, bucketMetaKey); err != nil {
		return "", "", "", "", "", err
	}
	return tag, workKey, dueKey, bucketMetaKey, workRef, nil
}

func (w *WorkLeaseStore) scheduleKeysFor(bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID string) (tag string, scheduleKey, dueKey, bucketMetaKey rediskey.RedisKey, scheduleRef string, err error) {
	if err = bucket.Validate(); err != nil {
		return "", "", "", "", "", err
	}
	tag, err = w.client.keys.HashTag(bucket)
	if err != nil {
		return "", "", "", "", "", err
	}
	if scheduleKey, err = w.client.keys.SourceSchedule(bucket, scheduleType, scheduleID); err != nil {
		return "", "", "", "", "", fmt.Errorf("%w: schedule key", authoritystore.ErrInvalidRequest)
	}
	if dueKey, err = w.client.keys.SourceDueIndex(bucket); err != nil {
		return "", "", "", "", "", err
	}
	if bucketMetaKey, err = w.client.keys.BucketMeta(bucket); err != nil {
		return "", "", "", "", "", err
	}
	if scheduleRef, err = workidentity.EncodeScheduleRefV1(scheduleType, scheduleID); err != nil {
		return "", "", "", "", "", fmt.Errorf("%w: schedule ref: %v", authoritystore.ErrInvalidRequest, err)
	}
	if err = rediskey.AssertSameSlot(scheduleKey, dueKey, bucketMetaKey); err != nil {
		return "", "", "", "", "", err
	}
	return tag, scheduleKey, dueKey, bucketMetaKey, scheduleRef, nil
}

// scheduleDualKeysFor extends scheduleKeysFor with the dedicated schedule due
// index. It is used by dual/fallback modes where the same Lua operation must
// atomically update both indexes. dedicatedOnly is allowed for claim/advance/
// complete: the shared key is still resolved (same hash tag) and the dual
// script is used so the dedicated index remains the only member owner, but
// the shared key must stay in the script because Commit always maintains it
// in shared/dual modes and stale shared members must be removed on complete.
func (w *WorkLeaseStore) scheduleDualKeysFor(bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID string) (tag string, scheduleKey, dueKey, scheduleDueKey, bucketMetaKey rediskey.RedisKey, scheduleRef string, err error) {
	if err = bucket.Validate(); err != nil {
		return "", "", "", "", "", "", err
	}
	tag, scheduleKey, dueKey, bucketMetaKey, scheduleRef, err = w.scheduleKeysFor(bucket, scheduleType, scheduleID)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	if scheduleDueKey, err = w.client.keys.SourceScheduleDueIndex(bucket); err != nil {
		return "", "", "", "", "", "", err
	}
	if err = rediskey.AssertSameSlot(scheduleKey, dueKey, scheduleDueKey, bucketMetaKey); err != nil {
		return "", "", "", "", "", "", err
	}
	return tag, scheduleKey, dueKey, scheduleDueKey, bucketMetaKey, scheduleRef, nil
}

// ListDueWork reads the existing Source DueIndex using Redis TIME.  It is a
// discovery operation only: no claim, state transition, or new key is made.
// The returned list is intentionally bounded so one hot bucket cannot turn a
// worker round into an unbounded scan.
func (w *WorkLeaseStore) ListDueWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]DueWorkRef, error) {
	if w == nil || w.client == nil {
		return nil, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if limit <= 0 || limit > 4096 {
		return nil, fmt.Errorf("%w: due limit", authoritystore.ErrInvalidRequest)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := w.client.Client().Time(ctx).Result()
	if err != nil {
		return nil, err
	}
	return w.ListDueWorkAt(ctx, bucket, now.UnixMilli(), limit)
}

// ListDueWorkAt is the deterministic-clock variant of ListDueWork, useful for
// probes and tests. Production worker code should prefer ListDueWork so the
// same Redis clock adjudicates discovery and ClaimWork.
func (w *WorkLeaseStore) ListDueWorkAt(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]DueWorkRef, error) {
	page, err := w.ListDueWorkPageAt(ctx, bucket, nowMs, "", DueWorkCursor{}, limit)
	return page.Refs, err
}

// listDueWorkPageLua atomically lower-bounds the sorted set by an arbitrary
// (score, member) pair. Redis has no native combined score/member range bound;
// a rank is therefore found by bounded binary search inside one Lua command.
// Mutations cannot interleave that search and rank read. The result is capped
// at 4097 physical members (one lookahead beyond the public page bound).
// ARGV[1] is either an explicit clock in milliseconds or an empty string,
// which makes the same Redis command obtain TIME. ARGV[2..4] carry cursor
// validity, score and member; ARGV[5] is the hard response count.
const listDueWorkPageLua = `
local now_ms
if ARGV[1] == '' then
  local now = redis.call('TIME')
  now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
else
  now_ms = tonumber(ARGV[1])
end
local cursor_score = 0
local cursor_member = ''
if ARGV[2] == '1' then
  cursor_score = tonumber(ARGV[3])
  cursor_member = ARGV[4]
end
local size = redis.call('ZCARD', KEYS[1])
local low = 0
local high = size
while low < high do
  local middle = math.floor((low + high) / 2)
  local item = redis.call('ZRANGE', KEYS[1], middle, middle, 'WITHSCORES')
  if #item == 0 then
    high = middle
  else
    local score = tonumber(item[2])
    if score < cursor_score or (score == cursor_score and item[1] < cursor_member) then
      low = middle + 1
    else
      high = middle
    end
  end
end
local result = {}
local count = tonumber(ARGV[5])
if low < size then
  local items = redis.call('ZRANGE', KEYS[1], low, low + count - 1, 'WITHSCORES')
  for index = 1, #items, 2 do
    local score = tonumber(items[index + 1])
    if score > now_ms then
      break
    end
    result[#result + 1] = items[index]
    result[#result + 1] = items[index + 1]
  end
end
return result
`

// ListDueWorkPage reads one bounded raw page from the shared Source DueIndex.
// Redis TIME and the physical page read execute in the same command, binding
// the clock to the exact client and index being traversed. family selects one
// exact SourceOrder work type; an empty family returns every decodable order.
// Schedules, malformed members, and other families consume RawScanned but are
// never removed by this read API.
func (w *WorkLeaseStore) ListDueWorkPage(ctx context.Context, bucket rediskey.AuthorityBucketRef, family string, cursor DueWorkCursor, limit int) (DueWorkPage, error) {
	return w.listDueWorkPage(ctx, bucket, "", family, cursor, limit)
}

// ListDueWorkPageAt is the deterministic-clock variant. Each page is one
// bounded, atomic Redis command. A cursor-zero Complete result is therefore an
// explicit point-in-time physical completeness proof. A multi-page traversal
// remains incomplete even when its suffix is later exhausted.
func (w *WorkLeaseStore) ListDueWorkPageAt(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, family string, cursor DueWorkCursor, limit int) (DueWorkPage, error) {
	if nowMs < 0 {
		return DueWorkPage{}, fmt.Errorf("%w: due page time", authoritystore.ErrInvalidRequest)
	}
	return w.listDueWorkPage(ctx, bucket, strconv.FormatInt(nowMs, 10), family, cursor, limit)
}

func (w *WorkLeaseStore) listDueWorkPage(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowArg, family string, cursor DueWorkCursor, limit int) (DueWorkPage, error) {
	var page DueWorkPage
	if w == nil || w.client == nil {
		return page, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if limit <= 0 || limit > maxDueIndexDiscoveryEntries {
		return page, fmt.Errorf("%w: due page limit", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return page, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	family = strings.TrimSpace(family)
	dueKey, err := w.client.keys.SourceDueIndex(bucket)
	if err != nil {
		return page, err
	}
	clientIdentity := fmt.Sprintf("%T:%p", w.client.Client(), w.client.Client())
	cursorValid := !cursor.IsZero()
	if cursorValid {
		if cursor.Index != dueKey.String() || cursor.clientIdentity != clientIdentity || cursor.Score < 0 || math.IsNaN(cursor.Score) || math.IsInf(cursor.Score, 0) {
			return page, fmt.Errorf("%w: due page cursor identity/score", authoritystore.ErrInvalidRequest)
		}
	}
	validArg, scoreArg := "0", "0"
	if cursorValid {
		validArg = "1"
		scoreArg = strconv.FormatFloat(cursor.Score, 'g', -1, 64)
	}
	values, err := w.client.Client().Eval(ctx, listDueWorkPageLua, []string{dueKey.String()},
		nowArg, validArg, scoreArg, cursor.Member, maxDueIndexDiscoveryEntries+1).Slice()
	if err != nil {
		return page, err
	}
	if len(values)%2 != 0 || len(values) > 2*(maxDueIndexDiscoveryEntries+1) {
		return page, fmt.Errorf("%w: due page result shape", errScriptContract)
	}
	type rawDueMember struct {
		member string
		score  float64
	}
	members := make([]rawDueMember, 0, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		member, ok := values[index].(string)
		if !ok {
			if raw, bytesOK := values[index].([]byte); bytesOK {
				member, ok = string(raw), true
			}
		}
		scoreRaw, scoreOK := values[index+1].(string)
		if !scoreOK {
			if raw, bytesOK := values[index+1].([]byte); bytesOK {
				scoreRaw, scoreOK = string(raw), true
			}
		}
		score, parseErr := strconv.ParseFloat(scoreRaw, 64)
		if !ok || !scoreOK || parseErr != nil || score < 0 || math.IsNaN(score) || math.IsInf(score, 0) {
			return DueWorkPage{}, fmt.Errorf("%w: due page member/score", errScriptContract)
		}
		members = append(members, rawDueMember{member: member, score: score})
	}
	refCapacity := len(members)
	if refCapacity > limit {
		refCapacity = limit
	}
	page.Refs = make([]DueWorkRef, 0, refCapacity)
	rawLimit := len(members)
	if rawLimit > maxDueIndexDiscoveryEntries {
		rawLimit = maxDueIndexDiscoveryEntries
	}
	for index := 0; index < rawLimit; index++ {
		member := members[index]
		page.RawScanned++
		ref, decodeErr := workidentity.DecodeWorkRefV1(member.member)
		if decodeErr == nil && (family == "" || ref.WorkType == family) {
			page.Refs = append(page.Refs, DueWorkRef{Ref: ref, DueAtMs: int64(member.score), Member: member.member})
		}
		if len(page.Refs) == limit {
			break
		}
	}
	if page.RawScanned < len(members) {
		next := members[page.RawScanned]
		page.NextCursor = DueWorkCursor{Score: next.score, Member: next.member, Index: dueKey.String(), clientIdentity: clientIdentity}
		return page, nil
	}
	page.Complete = !cursorValid
	return page, nil
}

// ListDueCareWork is the compatibility Care-specific view. It follows at most
// 65536 raw members and never mutates malformed or schedule entries. New
// shared-family callers should use ListDueWorkPage and preserve its explicit
// cursor/completeness result.
func (w *WorkLeaseStore) ListDueCareWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]DueWorkRef, error) {
	if w == nil || w.client == nil || limit <= 0 || limit > maxDueIndexDiscoveryEntries {
		return nil, fmt.Errorf("%w: due limit", authoritystore.ErrInvalidRequest)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := w.client.Client().Time(ctx).Result()
	if err != nil {
		return nil, err
	}
	out := make([]DueWorkRef, 0, limit)
	cursor := DueWorkCursor{}
	for scanned := 0; scanned < maxDueIndexCareDiscoveryEntries && len(out) < limit; {
		page, pageErr := w.ListDueWorkPageAt(ctx, bucket, now.UnixMilli(), string(identity.WorkTypeCareReward), cursor, limit-len(out))
		if pageErr != nil {
			return nil, pageErr
		}
		out = append(out, page.Refs...)
		scanned += page.RawScanned
		if page.Complete || page.NextCursor.IsZero() || page.RawScanned == 0 {
			break
		}
		cursor = page.NextCursor
	}
	return out, nil
}

// DueIndexScore proves direct/worker admission against the existing DueIndex
// without scanning the bucket.
func (w *WorkLeaseStore) DueIndexScore(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (int64, bool, error) {
	if w == nil || w.client == nil {
		return 0, false, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	_, _, dueKey, _, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return 0, false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	score, err := w.client.Client().ZScore(ctx, dueKey.String(), workRef).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return int64(score), true, nil
}

// TrustedNowMs exposes the same Redis TIME source used by the Lua lease
// scripts to preflight a read-only admission proof.
func (w *WorkLeaseStore) TrustedNowMs(ctx context.Context) (int64, error) {
	if w == nil || w.client == nil {
		return 0, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now, err := w.client.Client().Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return now.UnixMilli(), nil
}

// ListDueWorkRefs is a descriptive alias retained for callers that name the
// index member rather than the work itself.
func (w *WorkLeaseStore) ListDueWorkRefs(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]DueWorkRef, error) {
	return w.ListDueWorkAt(ctx, bucket, nowMs, limit)
}

// ListDueScheduledWork discovers only sr1_ schedule members. It never claims
// or deletes a valid schedule and uses the caller's bounded UTC millisecond
// boundary; production callers may obtain that boundary from TrustedNowMs.
func (w *WorkLeaseStore) ListDueScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]ScheduledDueRef, error) {
	refs, _, err := w.ListDueScheduledWorkWithStats(ctx, bucket, nowMs, limit)
	return refs, err
}

// ScheduleDueDiscoveryStats carries low-cardinality read-mode observability.
// It deliberately contains no farm/player/operation labels.
type ScheduleDueDiscoveryStats struct {
	ReadMode      ScheduleDueIndexMode
	DedicatedHits int
	FallbackHits  int
	ShadowMisses  int
}

// ScheduleDueBackfillStats is returned by one bounded shared-index scan page.
// Callers persist the returned cursor outside Redis authority state and repeat
// until Cursor is zero. Re-running any page is safe.
type ScheduleDueBackfillStats struct {
	Cursor   uint64
	Scanned  int
	Repaired int
	Skipped  int
}

// ListDueScheduledWorkWithStats is the mode-aware bounded discovery used by
// the worker. In dual_shadow mode the shared index remains authoritative and
// every dedicated member is cross-checked against the shared score; a
// mismatch is reported as a shadow miss but never hides a valid schedule. In
// dedicated_fallback mode both bounded sides are read, de-duplicated and
// globally ordered so a shared-only earlier item cannot be hidden by a full
// dedicated page. This deliberately retains shared-scan cost during migration.
func (w *WorkLeaseStore) ListDueScheduledWorkWithStats(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]ScheduledDueRef, ScheduleDueDiscoveryStats, error) {
	var stats ScheduleDueDiscoveryStats
	if w == nil || w.client == nil || limit <= 0 || limit > 4096 || nowMs < 0 {
		return nil, stats, fmt.Errorf("%w: schedule due limit/time", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return nil, stats, err
	}
	stats.ReadMode = w.client.opts.ScheduleDueIndexMode
	if ctx == nil {
		ctx = context.Background()
	}
	dedicatedKey, err := w.client.keys.SourceScheduleDueIndex(bucket)
	if err != nil {
		return nil, stats, err
	}
	if w.client.opts.ScheduleDueIndexMode.readsDedicated() {
		out, hits, err := w.listDedicatedDue(ctx, dedicatedKey.String(), bucket, nowMs, limit)
		stats.DedicatedHits = hits
		if err != nil {
			return nil, stats, err
		}
		if w.client.opts.ScheduleDueIndexMode != ScheduleDueIndexDedicatedFallback {
			return out, stats, nil
		}
		fallback, err := w.listSharedDue(ctx, bucket, nowMs, limit)
		if err != nil {
			return nil, stats, err
		}
		merged, fallbackHits := mergeScheduledDueRefs(out, fallback, limit)
		stats.FallbackHits = fallbackHits
		return merged, stats, nil
	}
	if w.client.opts.ScheduleDueIndexMode == ScheduleDueIndexDualShadow {
		shared, err := w.listSharedDue(ctx, bucket, nowMs, limit)
		if err != nil {
			return nil, stats, err
		}
		shadow, _, err := w.listDedicatedDue(ctx, dedicatedKey.String(), bucket, nowMs, limit)
		if err != nil {
			return nil, stats, err
		}
		shadowByMember := make(map[string]int64, len(shadow))
		for _, ref := range shadow {
			shadowByMember[ref.Member] = ref.DueAtMs
		}
		for _, ref := range shared {
			if score, ok := shadowByMember[ref.Member]; !ok || score != ref.DueAtMs {
				stats.ShadowMisses++
			}
		}
		return shared, stats, nil
	}
	shared, err := w.listSharedDue(ctx, bucket, nowMs, limit)
	return shared, stats, err
}

func (w *WorkLeaseStore) listDedicatedDue(ctx context.Context, dedicatedKey string, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]ScheduledDueRef, int, error) {
	out := make([]ScheduledDueRef, 0, limit)
	members, err := w.client.Client().ZRangeByScoreWithScores(ctx, dedicatedKey, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(nowMs, 10), Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, 0, err
	}
	for _, member := range members {
		raw, ok := member.Member.(string)
		if !ok {
			continue
		}
		ref, decodeErr := workidentity.DecodeScheduleRefV1(raw)
		if decodeErr != nil {
			// A dedicated index must contain only schedule identities. Returning
			// an error prevents a corrupt member from being mistaken for an empty
			// bucket and makes the worker surface the repair requirement.
			return nil, 0, fmt.Errorf("%w: corrupt dedicated schedule member %q: %v", errScriptContract, raw, decodeErr)
		}
		out = append(out, ScheduledDueRef{ScheduleType: ref.ScheduleType, ScheduleID: ref.ScheduleID, DueAtMs: int64(member.Score), Member: raw})
	}
	return out, len(out), nil
}

// mergeScheduledDueRefs returns the globally earliest unique refs. Both
// inputs are independently ordered by Redis score/member, but simply
// appending the fallback list can place an older shared-only schedule behind
// newer dedicated entries. Sorting the bounded union preserves the scheduler
// contract during rolling migration.
func mergeScheduledDueRefs(dedicated, shared []ScheduledDueRef, limit int) ([]ScheduledDueRef, int) {
	byMember := make(map[string]ScheduledDueRef, len(dedicated)+len(shared))
	for _, ref := range dedicated {
		byMember[ref.Member] = ref
	}
	fallbackHits := 0
	for _, ref := range shared {
		if _, exists := byMember[ref.Member]; !exists {
			fallbackHits++
		}
		byMember[ref.Member] = ref
	}
	merged := make([]ScheduledDueRef, 0, len(byMember))
	for _, ref := range byMember {
		merged = append(merged, ref)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].DueAtMs != merged[j].DueAtMs {
			return merged[i].DueAtMs < merged[j].DueAtMs
		}
		return merged[i].Member < merged[j].Member
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, fallbackHits
}

func (w *WorkLeaseStore) listSharedDue(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]ScheduledDueRef, error) {
	dueKey, err := w.client.keys.SourceDueIndex(bucket)
	if err != nil {
		return nil, err
	}
	pageSize := limit
	if pageSize > 128 {
		pageSize = 128
	}
	out := make([]ScheduledDueRef, 0, limit)
	offset := int64(0)
	scanned := 0
	for scanned < maxDueIndexScheduleDiscoveryEntries && len(out) < limit {
		count := pageSize
		if remaining := maxDueIndexScheduleDiscoveryEntries - scanned; count > remaining {
			count = remaining
		}
		members, err := w.client.Client().ZRangeByScoreWithScores(ctx, dueKey.String(), &redis.ZRangeBy{
			Min: "0", Max: strconv.FormatInt(nowMs, 10), Offset: offset, Count: int64(count),
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(members) == 0 {
			break
		}
		for _, member := range members {
			scanned++
			raw, ok := member.Member.(string)
			if !ok {
				continue
			}
			ref, decodeErr := workidentity.DecodeScheduleRefV1(raw)
			if decodeErr != nil {
				continue
			}
			out = append(out, ScheduledDueRef{ScheduleType: ref.ScheduleType, ScheduleID: ref.ScheduleID, DueAtMs: int64(member.Score), Member: raw})
			if len(out) == limit {
				break
			}
		}
		offset += int64(len(members))
		if len(members) < count {
			break
		}
	}
	return out, nil
}

// BackfillScheduleDueIndex repairs one shared DueIndex schedule member into
// the dedicated index. The same-slot Lua fence proves the schedule record's
// identity, generation, active state and next_due score before writing, so a
// concurrent newer generation, a completed record, or a stale score can never
// be overwritten or resurrected.
func (w *WorkLeaseStore) BackfillScheduleDueIndex(ctx context.Context, bucket rediskey.AuthorityBucketRef, ref ScheduledDueRef) (bool, error) {
	if w == nil || w.client == nil || ctx == nil {
		return false, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if !w.scheduleDueBackfillAllowed() {
		return false, fmt.Errorf("%w: schedule due backfill requires dual_shadow or dedicated_fallback mode", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return false, err
	}
	if ref.ScheduleType == "" || ref.ScheduleID == "" || ref.DueAtMs <= 0 {
		return false, fmt.Errorf("%w: backfill due ref", authoritystore.ErrInvalidRequest)
	}
	expectedMember, err := workidentity.EncodeScheduleRefV1(ref.ScheduleType, ref.ScheduleID)
	if err != nil || ref.Member != expectedMember {
		return false, fmt.Errorf("%w: backfill schedule identity", authoritystore.ErrInvalidRequest)
	}
	scheduleKey, err := w.client.keys.SourceSchedule(bucket, ref.ScheduleType, ref.ScheduleID)
	if err != nil {
		return false, err
	}
	scheduleDueKey, err := w.client.keys.SourceScheduleDueIndex(bucket)
	if err != nil {
		return false, err
	}
	if err := rediskey.AssertSameSlot(scheduleKey, scheduleDueKey); err != nil {
		return false, err
	}
	tag, err := w.client.keys.HashTag(bucket)
	if err != nil {
		return false, err
	}
	expectedDueAtMs := ref.DueAtMs
	expectedGeneration := uint64(0)
	expectedState := ""
	snapshot, snapshotErr := w.GetScheduledWork(ctx, bucket, ref.ScheduleType, ref.ScheduleID)
	if snapshotErr == nil {
		expectedDueAtMs = snapshot.NextDueAtMs
		expectedGeneration = snapshot.Generation
		expectedState = snapshot.State
	} else if !errors.Is(snapshotErr, ErrSourceOrderNotFound) {
		return false, snapshotErr
	}
	args := []interface{}{
		tag, ref.Member, ref.ScheduleType, ref.ScheduleID,
		strconv.FormatInt(expectedDueAtMs, 10),
		strconv.FormatUint(expectedGeneration, 10), expectedState,
	}
	reply, err := w.client.registry.Run(ctx, ScriptBackfillScheduleDueIndex, []string{scheduleKey.String(), scheduleDueKey.String()}, args...)
	if err != nil {
		return false, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return false, fmt.Errorf("%w: empty backfill reply", errScriptContract)
	}
	switch reply[0] {
	case "BACKFILLED":
		return true, nil
	case "REMOVED", "STALE_GENERATION", "STALE_STATE", "STALE_SCORE", "FENCE_MISMATCH":
		// The shared member no longer represents a claimable schedule: never
		// resurrect completed/cancelled work and never overwrite a newer
		// generation or score.
		return false, nil
	default:
		return false, fmt.Errorf("%w: backfill status %q", errScriptContract, reply[0])
	}
}

// BackfillScheduleDueIndexPage performs one idempotent, bounded migration page
// from the legacy mixed DueIndex. Redis ZSCAN MATCH keeps ordinary wr1_
// members out of the returned page; each sr1_ member is then conditionally
// reconciled by BackfillScheduleDueIndex's same-slot Lua fence.
func (w *WorkLeaseStore) BackfillScheduleDueIndexPage(ctx context.Context, bucket rediskey.AuthorityBucketRef, cursor uint64, count int64) (ScheduleDueBackfillStats, error) {
	stats := ScheduleDueBackfillStats{Cursor: cursor}
	if w == nil || w.client == nil || ctx == nil || count <= 0 || count > 1024 {
		return stats, fmt.Errorf("%w: schedule due backfill page", authoritystore.ErrInvalidRequest)
	}
	if !w.scheduleDueBackfillAllowed() {
		return stats, fmt.Errorf("%w: schedule due backfill requires dual_shadow or dedicated_fallback mode", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return stats, err
	}
	dueKey, err := w.client.keys.SourceDueIndex(bucket)
	if err != nil {
		return stats, err
	}
	entries, next, err := w.client.Client().ZScan(ctx, dueKey.String(), cursor, "sr1_*", count).Result()
	if err != nil {
		return stats, err
	}
	stats.Cursor = next
	if len(entries)%2 != 0 {
		return stats, fmt.Errorf("%w: odd ZSCAN schedule result", errScriptContract)
	}
	for index := 0; index < len(entries); index += 2 {
		member := entries[index]
		score, parseErr := strconv.ParseFloat(entries[index+1], 64)
		if parseErr != nil || score <= 0 {
			return stats, fmt.Errorf("%w: invalid schedule due score", errScriptContract)
		}
		identity, decodeErr := workidentity.DecodeScheduleRefV1(member)
		if decodeErr != nil {
			return stats, fmt.Errorf("%w: corrupt shared schedule member %q: %v", errScriptContract, member, decodeErr)
		}
		stats.Scanned++
		repaired, repairErr := w.BackfillScheduleDueIndex(ctx, bucket, ScheduledDueRef{
			ScheduleType: identity.ScheduleType,
			ScheduleID:   identity.ScheduleID,
			DueAtMs:      int64(score),
			Member:       member,
		})
		if repairErr != nil {
			return stats, repairErr
		}
		if repaired {
			stats.Repaired++
		} else {
			stats.Skipped++
		}
	}
	return stats, nil
}

func (w *WorkLeaseStore) scheduleDueBackfillAllowed() bool {
	if w == nil || w.client == nil {
		return false
	}
	mode := w.client.opts.ScheduleDueIndexMode
	return mode == ScheduleDueIndexDualShadow || mode == ScheduleDueIndexDedicatedFallback
}

// ListDueScheduledWorkLegacy preserves the exact pre-C5 shared-index path for
// callers that deliberately want the legacy discovery behavior regardless of
// the store mode (for example compatibility probes).
func (w *WorkLeaseStore) ListDueScheduledWorkLegacy(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]ScheduledDueRef, error) {
	return w.listSharedDue(ctx, bucket, nowMs, limit)
}

// GetSourceOrder performs a read-only, bounded load of the existing
// SourceOrder HASH.  It is intentionally not used by the recovery worker for
// claiming; the worker must obtain all executable facts from ClaimWork.  The
// DirectDispatcher uses it to construct its Coordinator request without
// inventing a second durable schema.
func (w *WorkLeaseStore) GetSourceOrder(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (SourceOrderSnapshot, error) {
	if w == nil || w.client == nil {
		return SourceOrderSnapshot{}, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	_, workKey, _, _, _, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fields, err := w.client.Client().HGetAll(ctx, workKey.String()).Result()
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	if len(fields) == 0 {
		return SourceOrderSnapshot{}, ErrSourceOrderNotFound
	}
	parseUint := func(name string, required bool) (uint64, error) {
		value := fields[name]
		if value == "" {
			if required {
				return 0, fmt.Errorf("%w: missing %s", errScriptContract, name)
			}
			return 0, nil
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: field %s", errScriptContract, name)
		}
		return parsed, nil
	}
	workVersion, err := parseUint("work_version", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	bucketVersion, err := parseUint("bucket_version", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	bucketID, err := parseUint("bucket_id", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	subject, err := parseUint("subject_player_id", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	nextDue, err := parseUint("next_due_at_ms", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	retry, err := parseUint("retry_count", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	leaseEpoch, err := parseUint("lease_epoch", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	leaseExpire, err := parseUint("lease_expire_at_ms", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	created, err := parseUint("created_at_ms", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	updated, err := parseUint("updated_at_ms", true)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	completed, err := parseUint("completed_at_ms", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	deadline, err := parseUint("deadline_at_ms", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	maxRetry, err := parseUint("max_retry_count", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	reconcileAt, err := parseUint("reconcile_required_at_ms", false)
	if err != nil {
		return SourceOrderSnapshot{}, err
	}
	if fields["work_id"] != workID || fields["work_type"] != workType || fields["state"] == "" || fields["typed_payload"] == "" {
		return SourceOrderSnapshot{}, fmt.Errorf("%w: source order identity/state", errScriptContract)
	}
	if bucketVersion != 1 || bucketID > uint64(^uint32(0)) || uint32(bucketID) != bucket.VirtualShard() {
		return SourceOrderSnapshot{}, fmt.Errorf("%w: source order bucket", errScriptContract)
	}
	if fields["placement_kind"] == "" || fields["source_authority_kind"] == "" ||
		fields["canonical_source_authority_id"] == "" || fields["authority_layout_version"] == "" || fields["actor_route_domain"] == "" ||
		fields["operation_id"] == "" || fields["payload_hash"] == "" {
		return SourceOrderSnapshot{}, fmt.Errorf("%w: source order placement", errScriptContract)
	}
	return SourceOrderSnapshot{
		WorkID: workID, WorkType: workType, PlacementKind: fields["placement_kind"],
		BucketVersion: uint32(bucketVersion), BucketID: uint32(bucketID),
		SourceAuthorityKind:        fields["source_authority_kind"],
		CanonicalSourceAuthorityID: fields["canonical_source_authority_id"],
		AuthorityLayoutVersion:     fields["authority_layout_version"], ActorRouteDomain: fields["actor_route_domain"],
		SubjectPlayerID: subject, OperationID: fields["operation_id"], PayloadHash: fields["payload_hash"],
		State: fields["state"], WorkVersion: workVersion, CurrentStep: fields["current_step"],
		TypedPayload: []byte(fields["typed_payload"]), NextDueAtMs: int64(nextDue), RetryCount: retry,
		LastErrorClass: fields["last_error_class"], DeadlineAtMs: int64(deadline), MaxRetryCount: maxRetry,
		ReconcileRequiredAtMs: int64(reconcileAt),
		LeaseHolderID:         fields["lease_holder_id"], LeaseHolderBootID: fields["lease_holder_boot_id"], LeaseEpoch: leaseEpoch, LeaseTokenHash: fields["lease_token_hash"], LeaseExpireAtMs: int64(leaseExpire),
		CreatedAtMs: int64(created), UpdatedAtMs: int64(updated), CompletedAtMs: int64(completed),
	}, nil
}

// SourceOrderAdmissionEvidence is the single-RTT admission view returned by
// VerifySourceOrderAdmission. It contains every field the destination verifier
// binds plus the same Redis TIME and DueIndex score used by WorkLease claim.
type SourceOrderAdmissionEvidence struct {
	Found                    bool
	WorkID, WorkType         string
	State                    string
	WorkVersion              uint64
	PayloadHash              string
	SubjectPlayerID          uint64
	OperationID              string
	NextDueAtMs              int64
	LeaseHolderID            string
	LeaseHolderBootID        string
	LeaseEpoch               uint64
	LeaseTokenHash           string
	LeaseExpireAtMs          int64
	PlacementKind            string
	BucketVersion            uint32
	BucketID                 uint32
	SourceAuthorityKind      string
	CanonicalSourceAuthority string
	AuthorityLayoutVersion   string
	ActorRouteDomain         string
	DueScoreMs               int64
	TrustedNowMs             int64
}

// VerifySourceOrderAdmission atomically reads the bounded SourceOrder fields,
// Redis TIME and DueIndex score in one EVALSHA. typed_payload is never
// returned; payload_hash plus the exact metadata bind the order. This removes
// the HGETALL/TIME/ZSCORE fan-out from destination admission while keeping
// every binding the verifier previously applied.
func (w *WorkLeaseStore) VerifySourceOrderAdmission(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (SourceOrderAdmissionEvidence, error) {
	var out SourceOrderAdmissionEvidence
	if w == nil || w.client == nil {
		return out, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, _, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return out, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reply, err := w.client.registry.Run(ctx, ScriptVerifySourceOrderV1,
		[]string{workKey.String(), dueKey.String()}, tag, workRef, workType, workID)
	if err != nil {
		return out, err
	}
	if len(reply) == 0 {
		return out, fmt.Errorf("%w: empty verify reply", errScriptContract)
	}
	if reply[0] == "NOT_FOUND" {
		return out, nil
	}
	if reply[0] != "OK" || len(reply) != 23 {
		return out, fmt.Errorf("%w: verify status %q", errScriptContract, reply[0])
	}
	parseUint := func(name, value string) (uint64, error) {
		if value == "" {
			return 0, nil
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil && strings.ContainsAny(value, "eE") {
			// Lua number formatting differs between servers (integer vs
			// scientific); ParseFloat keeps the evidence binding identical.
			f, ferr := strconv.ParseFloat(value, 64)
			if ferr == nil && f > 0 && f < 1e18 {
				return uint64(f), nil
			}
		}
		if parseErr != nil {
			return 0, fmt.Errorf("%w: verify %s", errScriptContract, name)
		}
		return parsed, nil
	}
	workVersion, err := parseUint("work_version", reply[4])
	if err != nil {
		return out, err
	}
	subject, err := parseUint("subject_player_id", reply[6])
	if err != nil {
		return out, err
	}
	nextDue, err := parseUint("next_due_at_ms", reply[8])
	if err != nil {
		return out, err
	}
	leaseEpoch, err := parseUint("lease_epoch", reply[11])
	if err != nil {
		return out, err
	}
	leaseExpire, err := parseUint("lease_expire_at_ms", reply[13])
	if err != nil {
		return out, err
	}
	bucketVersion, err := parseUint("bucket_version", reply[15])
	if err != nil {
		return out, err
	}
	bucketID, err := parseUint("bucket_id", reply[16])
	if err != nil {
		return out, err
	}
	dueScore, err := parseUint("due_score", reply[21])
	if err != nil {
		return out, err
	}
	nowMs, err := parseUint("now", reply[22])
	if err != nil {
		return out, err
	}
	out.Found = true
	out.WorkID = reply[1]
	out.WorkType = reply[2]
	out.State = reply[3]
	out.WorkVersion = workVersion
	out.PayloadHash = reply[5]
	out.SubjectPlayerID = subject
	out.OperationID = reply[7]
	out.NextDueAtMs = int64(nextDue)
	out.LeaseHolderID = reply[9]
	out.LeaseHolderBootID = reply[10]
	out.LeaseEpoch = leaseEpoch
	out.LeaseTokenHash = reply[12]
	out.LeaseExpireAtMs = int64(leaseExpire)
	out.PlacementKind = reply[14]
	out.BucketVersion = uint32(bucketVersion)
	out.BucketID = uint32(bucketID)
	out.SourceAuthorityKind = reply[17]
	out.CanonicalSourceAuthority = reply[18]
	out.AuthorityLayoutVersion = reply[19]
	out.ActorRouteDomain = reply[20]
	out.DueScoreMs = int64(dueScore)
	out.TrustedNowMs = int64(nowMs)
	return out, nil
}

// SourceOrderRecoveryEvidence adds retry_count and created_at_ms to the
// admission evidence so the Worker grace check can run in the same single
// EVALSHA that already returns Redis TIME.
type SourceOrderRecoveryEvidence struct {
	SourceOrderAdmissionEvidence
	RetryCount  uint64
	CreatedAtMs int64
}

// VerifySourceOrderRecoveryEvidence is the Worker recovery variant of
// VerifySourceOrderAdmission: one EVALSHA returns the bounded order fields,
// due score, Redis TIME, retry_count and created_at_ms.
func (w *WorkLeaseStore) VerifySourceOrderRecoveryEvidence(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (SourceOrderRecoveryEvidence, error) {
	var out SourceOrderRecoveryEvidence
	if w == nil || w.client == nil {
		return out, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, _, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return out, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reply, err := w.client.registry.Run(ctx, ScriptVerifySourceOrderV2,
		[]string{workKey.String(), dueKey.String()}, tag, workRef, workType, workID)
	if err != nil {
		return out, err
	}
	if len(reply) == 0 {
		return out, fmt.Errorf("%w: empty verify reply", errScriptContract)
	}
	if reply[0] == "NOT_FOUND" {
		return out, nil
	}
	if reply[0] != "OK" || len(reply) != 25 {
		return out, fmt.Errorf("%w: verify status %q", errScriptContract, reply[0])
	}
	parseUint := func(name, value string) (uint64, error) {
		if value == "" {
			return 0, nil
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil && strings.ContainsAny(value, "eE") {
			f, ferr := strconv.ParseFloat(value, 64)
			if ferr == nil && f > 0 && f < 1e18 {
				return uint64(f), nil
			}
		}
		if parseErr != nil {
			return 0, fmt.Errorf("%w: verify %s", errScriptContract, name)
		}
		return parsed, nil
	}
	workVersion, err := parseUint("work_version", reply[4])
	if err != nil {
		return out, err
	}
	subject, err := parseUint("subject_player_id", reply[6])
	if err != nil {
		return out, err
	}
	nextDue, err := parseUint("next_due_at_ms", reply[8])
	if err != nil {
		return out, err
	}
	leaseEpoch, err := parseUint("lease_epoch", reply[11])
	if err != nil {
		return out, err
	}
	leaseExpire, err := parseUint("lease_expire_at_ms", reply[13])
	if err != nil {
		return out, err
	}
	bucketVersion, err := parseUint("bucket_version", reply[15])
	if err != nil {
		return out, err
	}
	bucketID, err := parseUint("bucket_id", reply[16])
	if err != nil {
		return out, err
	}
	dueScore, err := parseUint("due_score", reply[23])
	if err != nil {
		return out, err
	}
	nowMs, err := parseUint("now", reply[24])
	if err != nil {
		return out, err
	}
	retry, err := parseUint("retry_count", reply[21])
	if err != nil {
		return out, err
	}
	created, err := parseUint("created_at_ms", reply[22])
	if err != nil {
		return out, err
	}
	out.Found = true
	out.WorkID = reply[1]
	out.WorkType = reply[2]
	out.State = reply[3]
	out.WorkVersion = workVersion
	out.PayloadHash = reply[5]
	out.SubjectPlayerID = subject
	out.OperationID = reply[7]
	out.NextDueAtMs = int64(nextDue)
	out.LeaseHolderID = reply[9]
	out.LeaseHolderBootID = reply[10]
	out.LeaseEpoch = leaseEpoch
	out.LeaseTokenHash = reply[12]
	out.LeaseExpireAtMs = int64(leaseExpire)
	out.PlacementKind = reply[14]
	out.BucketVersion = uint32(bucketVersion)
	out.BucketID = uint32(bucketID)
	out.SourceAuthorityKind = reply[17]
	out.CanonicalSourceAuthority = reply[18]
	out.AuthorityLayoutVersion = reply[19]
	out.ActorRouteDomain = reply[20]
	out.DueScoreMs = int64(dueScore)
	out.TrustedNowMs = int64(nowMs)
	out.RetryCount = retry
	out.CreatedAtMs = int64(created)
	return out, nil
}

func (w *WorkLeaseStore) GetScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID string) (ScheduledWorkSnapshot, error) {
	if w == nil || w.client == nil {
		return ScheduledWorkSnapshot{}, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	_, scheduleKey, _, _, _, err := w.scheduleKeysFor(bucket, scheduleType, scheduleID)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fields, err := w.client.Client().HGetAll(ctx, scheduleKey.String()).Result()
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	if len(fields) == 0 {
		return ScheduledWorkSnapshot{}, ErrSourceOrderNotFound
	}
	parseUint := func(name string, required bool) (uint64, error) {
		value := fields[name]
		if value == "" {
			if required {
				return 0, fmt.Errorf("%w: missing schedule %s", errScriptContract, name)
			}
			return 0, nil
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: schedule field %s", errScriptContract, name)
		}
		return parsed, nil
	}
	parseInt := func(name string, required bool) (int64, error) {
		value := fields[name]
		if value == "" {
			if required {
				return 0, fmt.Errorf("%w: missing schedule %s", errScriptContract, name)
			}
			return 0, nil
		}
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: schedule field %s", errScriptContract, name)
		}
		return parsed, nil
	}
	generation, err := parseUint("generation", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	workVersion, err := parseUint("work_version", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	subject, err := parseUint("subject_player_id", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	leaseEpoch, err := parseUint("lease_epoch", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	nextDue, err := parseInt("next_due_at_ms", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	leaseExpire, err := parseInt("lease_expire_at_ms", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	created, err := parseInt("created_at_ms", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	updated, err := parseInt("updated_at_ms", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	completed, err := parseInt("completed_at_ms", true)
	if err != nil {
		return ScheduledWorkSnapshot{}, err
	}
	if fields["schedule_id"] != scheduleID || fields["schedule_type"] != scheduleType || fields["state"] == "" || fields["payload_hash"] == "" {
		return ScheduledWorkSnapshot{}, fmt.Errorf("%w: schedule identity/state", errScriptContract)
	}
	return ScheduledWorkSnapshot{
		ScheduleID: scheduleID, ScheduleType: scheduleType, Generation: generation,
		State: fields["state"], WorkVersion: workVersion, SubjectPlayerID: subject,
		OperationID: fields["operation_id"], PayloadHash: fields["payload_hash"],
		Payload: []byte(fields["typed_payload"]), NextDueAtMs: nextDue,
		LeaseHolderID: fields["lease_holder_id"], LeaseHolderBootID: fields["lease_holder_boot_id"],
		LeaseEpoch: leaseEpoch, LeaseTokenHash: fields["lease_token_hash"], LeaseExpireAtMs: leaseExpire,
		CreatedAtMs: created, UpdatedAtMs: updated, CompletedAtMs: completed,
	}, nil
}

func (w *WorkLeaseStore) ClaimWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, leaseToken string, requestedTTLms int64) (WorkClaim, error) {
	return w.ClaimWorkWithBoot(ctx, bucket, workType, workID, holderID, holderID, leaseToken, requestedTTLms)
}

func (w *WorkLeaseStore) ClaimWorkWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, leaseToken string, requestedTTLms int64) (WorkClaim, error) {
	if err := ctx.Err(); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("lease_token", leaseToken); err != nil {
		return WorkClaim{}, err
	}
	if requestedTTLms <= 0 {
		return WorkClaim{}, fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, bucketMetaKey, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return WorkClaim{}, err
	}
	args := []interface{}{
		tag, holderID, bootID, sha256Hex("lease-token", leaseToken),
		strconv.FormatInt(requestedTTLms, 10),
		strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10),
		workRef,
	}
	reply, err := w.client.registry.RunNoRetry(ctx, ScriptClaimWorkV5,
		[]string{workKey.String(), dueKey.String(), bucketMetaKey.String()}, args...)
	if err != nil {
		return WorkClaim{Status: workTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkClaim{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "CLAIMED":
		if len(reply) < 12 {
			return WorkClaim{}, fmt.Errorf("%w: short claim reply", errScriptContract)
		}
		epoch, _ := strconv.ParseUint(reply[1], 10, 64)
		workVersion, _ := strconv.ParseUint(reply[2], 10, 64)
		expireAt, _ := strconv.ParseInt(reply[3], 10, 64)
		acceptedTTL, _ := strconv.ParseInt(reply[4], 10, 64)
		subject, _ := strconv.ParseUint(reply[8], 10, 64)
		retryCount, _ := strconv.ParseUint(reply[10], 10, 64)
		return WorkClaim{
			Status:            WorkClaimClaimed,
			WorkID:            reply[6],
			WorkType:          reply[7],
			SubjectPlayerID:   subject,
			OperationID:       reply[9],
			Payload:           []byte(reply[5]),
			PayloadHash:       reply[11],
			RetryCount:        retryCount,
			WorkVersion:       workVersion,
			LeaseEpoch:        epoch,
			LeaseExpireAtMs:   expireAt,
			LeaseHolderBootID: bootID,
			AcceptedTTLms:     acceptedTTL,
		}, nil
	case "LEASE_HELD":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		return WorkClaim{Status: WorkClaimLeaseHeld, LeaseConflictExpireAtMs: expireAt}, nil
	case "NOT_DUE":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		return WorkClaim{Status: WorkClaimNotDue, LeaseConflictExpireAtMs: expireAt}, nil
	case "NOT_FOUND":
		return WorkClaim{Status: WorkClaimNotFound}, nil
	case "NOT_CLAIMABLE":
		return WorkClaim{Status: WorkClaimNotClaimable}, nil
	case "RECONCILE_REQUIRED":
		return WorkClaim{Status: WorkClaimReconcileRequired}, nil
	case "MALFORMED_RECORD":
		return WorkClaim{}, fmt.Errorf("%w: source order terminal record", errMalformedBlob)
	default:
		return WorkClaim{}, fmt.Errorf("%w: claim status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) ClaimScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, leaseToken string, requestedTTLms int64) (WorkClaim, error) {
	return w.ClaimScheduledWorkWithBoot(ctx, bucket, scheduleType, scheduleID, holderID, holderID, leaseToken, requestedTTLms)
}

func (w *WorkLeaseStore) ClaimScheduledWorkWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID, leaseToken string, requestedTTLms int64) (WorkClaim, error) {
	if err := ctx.Err(); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkClaim{}, err
	}
	if err := validateStableID("lease_token", leaseToken); err != nil {
		return WorkClaim{}, err
	}
	if requestedTTLms <= 0 {
		return WorkClaim{}, fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest)
	}
	var tag, scheduleRef string
	var scriptKeys []string
	scriptName := ScriptClaimScheduledWork
	if w.client.opts.ScheduleDueIndexMode.writesDedicated() {
		var scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis, _, scheduleRef, err = w.scheduleDualKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return WorkClaim{}, err
		}
		scriptName = ScriptClaimScheduledWorkDual
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String(), scheduleDueKeyRedis.String()}
	} else {
		var scheduleKeyRedis, dueKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, _, scheduleRef, err = w.scheduleKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return WorkClaim{}, err
		}
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String()}
	}
	args := []interface{}{
		tag, holderID, bootID, sha256Hex("lease-token", leaseToken),
		strconv.FormatInt(requestedTTLms, 10),
		strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10), scheduleRef,
	}
	if w.client.opts.ScheduleDueIndexMode.writesDedicated() {
		args = append(args, w.client.opts.ScheduleDueIndexMode.sharedWriteArg())
	}
	reply, err := w.client.registry.Run(ctx, scriptName, scriptKeys, args...)
	if err != nil {
		return WorkClaim{Status: workTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkClaim{}, fmt.Errorf("%w: empty schedule claim reply", errScriptContract)
	}
	switch reply[0] {
	case "CLAIMED":
		if len(reply) < 13 {
			return WorkClaim{}, fmt.Errorf("%w: short schedule claim reply", errScriptContract)
		}
		epoch, err := strconv.ParseUint(reply[1], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule lease epoch", errScriptContract)
		}
		workVersion, err := strconv.ParseUint(reply[2], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule work version", errScriptContract)
		}
		expireAt, err := strconv.ParseInt(reply[3], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule lease expiry", errScriptContract)
		}
		acceptedTTL, err := strconv.ParseInt(reply[4], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule ttl", errScriptContract)
		}
		subject, err := strconv.ParseUint(reply[8], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule subject", errScriptContract)
		}
		retryCount, err := strconv.ParseUint(reply[10], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule retry count", errScriptContract)
		}
		generation, err := strconv.ParseUint(reply[12], 10, 64)
		if err != nil {
			return WorkClaim{}, fmt.Errorf("%w: schedule generation", errScriptContract)
		}
		return WorkClaim{Status: WorkClaimClaimed, WorkID: reply[6], WorkType: reply[7], SubjectPlayerID: subject,
			OperationID: reply[9], Payload: []byte(reply[5]), PayloadHash: reply[11], RetryCount: retryCount,
			WorkVersion: workVersion, Generation: generation, LeaseEpoch: epoch, LeaseExpireAtMs: expireAt,
			LeaseHolderBootID: bootID, AcceptedTTLms: acceptedTTL}, nil
	case "LEASE_HELD":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		return WorkClaim{Status: WorkClaimLeaseHeld, LeaseConflictExpireAtMs: expireAt}, nil
	case "NOT_DUE":
		dueAt, _ := strconv.ParseInt(reply[1], 10, 64)
		return WorkClaim{Status: WorkClaimNotDue, LeaseConflictExpireAtMs: dueAt}, nil
	case "NOT_FOUND":
		return WorkClaim{Status: WorkClaimNotFound}, nil
	case "NOT_CLAIMABLE":
		return WorkClaim{Status: WorkClaimNotClaimable}, nil
	case "MALFORMED_RECORD":
		return WorkClaim{}, fmt.Errorf("%w: schedule record", errMalformedBlob)
	default:
		return WorkClaim{}, fmt.Errorf("%w: schedule claim status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) AdvanceScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string, nextDueAtMs int64, payload []byte, payloadHash string) (ScheduledWorkAdvanceResult, error) {
	if err := ctx.Err(); err != nil {
		return ScheduledWorkAdvanceResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return ScheduledWorkAdvanceResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return ScheduledWorkAdvanceResult{}, err
	}
	if err := validateStableID("lease_token", leaseToken); err != nil {
		return ScheduledWorkAdvanceResult{}, err
	}
	if leaseEpoch == 0 || workVersion == 0 || generation == 0 || nextDueAtMs <= 0 || len(payload) > w.client.opts.MaxSchedulePayloadBytes || len(payload) > maxOrderPayloadHard || !validHexDigest(payloadHash) {
		return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance fence/due/payload", authoritystore.ErrInvalidRequest)
	}
	wantHash := sha256.Sum256(payload)
	if payloadHash != hexEncode(wantHash[:]) {
		return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule payload hash", authoritystore.ErrInvalidRequest)
	}
	var tag, scheduleRef string
	var scriptKeys []string
	scriptName := ScriptAdvanceScheduledWork
	if w.client.opts.ScheduleDueIndexMode.writesDedicated() {
		var scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis, bucketMetaKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis, bucketMetaKeyRedis, scheduleRef, err = w.scheduleDualKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return ScheduledWorkAdvanceResult{}, err
		}
		scriptName = ScriptAdvanceScheduledWorkDual
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String(), bucketMetaKeyRedis.String(), scheduleDueKeyRedis.String()}
	} else {
		var scheduleKeyRedis, dueKeyRedis, bucketMetaKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, bucketMetaKeyRedis, scheduleRef, err = w.scheduleKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return ScheduledWorkAdvanceResult{}, err
		}
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String(), bucketMetaKeyRedis.String()}
	}
	args := []interface{}{tag, holderID, bootID, strconv.FormatUint(leaseEpoch, 10), sha256Hex("lease-token", leaseToken),
		strconv.FormatUint(workVersion, 10), strconv.FormatUint(generation, 10), scheduleRef, scheduleType, scheduleID,
		strconv.FormatInt(nextDueAtMs, 10), string(payload), payloadHash, strconv.Itoa(w.client.opts.MaxBucketPendingWorkBytes)}
	if w.client.opts.ScheduleDueIndexMode.writesDedicated() {
		args = append(args, w.client.opts.ScheduleDueIndexMode.sharedWriteArg())
	}
	reply, err := w.client.registry.Run(ctx, scriptName, scriptKeys, args...)
	if err != nil {
		return ScheduledWorkAdvanceResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: empty schedule advance reply", errScriptContract)
	}
	switch reply[0] {
	case "ADVANCED":
		if len(reply) < 4 {
			return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: short schedule advance reply", errScriptContract)
		}
		newVersion, err := strconv.ParseUint(reply[1], 10, 64)
		if err != nil {
			return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance version", errScriptContract)
		}
		gotGeneration, err := strconv.ParseUint(reply[2], 10, 64)
		if err != nil {
			return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance generation", errScriptContract)
		}
		due, err := strconv.ParseInt(reply[3], 10, 64)
		if err != nil {
			return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance due", errScriptContract)
		}
		return ScheduledWorkAdvanceResult{Status: WorkLeaseRenewed, WorkVersion: newVersion, Generation: gotGeneration, NextDueAtMs: due}, nil
	case "STALE_GENERATION":
		return ScheduledWorkAdvanceResult{Status: WorkLeaseStaleGeneration}, nil
	case "STALE_VERSION":
		return ScheduledWorkAdvanceResult{Status: WorkLeaseStaleVersion}, nil
	case "FENCE_MISMATCH":
		return ScheduledWorkAdvanceResult{Status: WorkLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return ScheduledWorkAdvanceResult{Status: WorkLeaseExpired}, nil
	case "NOT_FOUND":
		return ScheduledWorkAdvanceResult{Status: WorkLeaseNotFound}, nil
	case "CAPACITY_EXCEEDED":
		return ScheduledWorkAdvanceResult{}, authoritystore.ErrCapacityExceeded
	case "MALFORMED_RECORD":
		return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule record", errMalformedBlob)
	default:
		return ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: schedule advance status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) RenewWorkLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID string, leaseEpoch uint64, leaseToken string, requestedTTLms int64) (WorkLeaseRenewResult, error) {
	return w.RenewWorkLeaseWithBoot(ctx, bucket, workType, workID, holderID, holderID, leaseEpoch, leaseToken, requestedTTLms)
}

func (w *WorkLeaseStore) RenewWorkLeaseWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, leaseEpoch uint64, leaseToken string, requestedTTLms int64) (WorkLeaseRenewResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if leaseEpoch == 0 || requestedTTLms <= 0 {
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: lease epoch/ttl", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, _, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return WorkLeaseRenewResult{}, err
	}
	args := []interface{}{
		tag, holderID, bootID,
		strconv.FormatUint(leaseEpoch, 10),
		sha256Hex("lease-token", leaseToken),
		strconv.FormatInt(requestedTTLms, 10),
		strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10),
		workRef,
	}
	reply, err := w.client.registry.Run(ctx, ScriptRenewWorkLease, []string{workKey.String(), dueKey.String()}, args...)
	if err != nil {
		return WorkLeaseRenewResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "RENEWED":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		acceptedTTL, _ := strconv.ParseInt(reply[2], 10, 64)
		return WorkLeaseRenewResult{Status: WorkLeaseRenewed, LeaseExpireAtMs: expireAt, AcceptedTTLms: acceptedTTL}, nil
	case "FENCE_MISMATCH":
		return WorkLeaseRenewResult{Status: WorkLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return WorkLeaseRenewResult{Status: WorkLeaseExpired}, nil
	case "NOT_FOUND":
		return WorkLeaseRenewResult{Status: WorkLeaseNotFound}, nil
	default:
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: renew status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) RenewScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch uint64, leaseToken string, requestedTTLms int64) (WorkLeaseRenewResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if err := validateStableID("lease_token", leaseToken); err != nil {
		return WorkLeaseRenewResult{}, err
	}
	if leaseEpoch == 0 || requestedTTLms <= 0 {
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: schedule lease epoch/ttl", authoritystore.ErrInvalidRequest)
	}
	tag, scheduleKey, _, _, _, err := w.scheduleKeysFor(bucket, scheduleType, scheduleID)
	if err != nil {
		return WorkLeaseRenewResult{}, err
	}
	args := []interface{}{tag, holderID, bootID, strconv.FormatUint(leaseEpoch, 10), sha256Hex("lease-token", leaseToken),
		strconv.FormatInt(requestedTTLms, 10), strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10), strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10)}
	reply, err := w.client.registry.Run(ctx, ScriptRenewScheduledWork, []string{scheduleKey.String()}, args...)
	if err != nil {
		return WorkLeaseRenewResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: empty schedule renew reply", errScriptContract)
	}
	switch reply[0] {
	case "RENEWED":
		expire, _ := strconv.ParseInt(reply[1], 10, 64)
		accepted, _ := strconv.ParseInt(reply[2], 10, 64)
		return WorkLeaseRenewResult{Status: WorkLeaseRenewed, LeaseExpireAtMs: expire, AcceptedTTLms: accepted}, nil
	case "FENCE_MISMATCH":
		return WorkLeaseRenewResult{Status: WorkLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return WorkLeaseRenewResult{Status: WorkLeaseExpired}, nil
	case "NOT_FOUND":
		return WorkLeaseRenewResult{Status: WorkLeaseNotFound}, nil
	default:
		return WorkLeaseRenewResult{}, fmt.Errorf("%w: schedule renew status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) CompleteWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID string, leaseEpoch, workVersion uint64, leaseToken string) (WorkCompleteResult, error) {
	return WorkCompleteResult{}, fmt.Errorf("%w: Participant result proof is required", authoritystore.ErrInvalidRequest)
}

func (w *WorkLeaseStore) CompleteWorkWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, leaseEpoch, workVersion uint64, leaseToken string, proof WorkCompletionProof) (WorkCompleteResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkCompleteResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkCompleteResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkCompleteResult{}, err
	}
	if leaseEpoch == 0 || workVersion == 0 {
		return WorkCompleteResult{}, fmt.Errorf("%w: lease epoch/work version", authoritystore.ErrInvalidRequest)
	}
	for name, value := range map[string]string{
		"source_authority_kind":         proof.SourceAuthorityKind,
		"canonical_source_authority_id": proof.CanonicalSourceAuthorityID,
		"authority_layout_version":      proof.AuthorityLayoutVersion,
		"actor_route_domain":            proof.ActorRouteDomain,
		"payload_hash":                  proof.PayloadHash,
		"participant_invocation_id":     proof.InvocationID,
		"participant_step":              proof.ParticipantStep,
		"participant_outcome":           proof.ParticipantOutcome,
		"participant_result_hash":       proof.ParticipantResultHash,
		"result_proof_hash":             proof.ResultProofHash,
	} {
		if err := validateStableID(name, value); err != nil {
			return WorkCompleteResult{}, err
		}
	}
	if !validHexDigest(proof.PayloadHash) || !validHexDigest(proof.ParticipantResultHash) || !validHexDigest(proof.ResultProofHash) || proof.ParticipantResultVersion == 0 || proof.ResultProofHash != WorkCompletionProofHash(proof) {
		return WorkCompleteResult{}, fmt.Errorf("%w: result proof hash/version", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, bucketMetaKey, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return WorkCompleteResult{}, err
	}
	args := []interface{}{
		tag, holderID, bootID,
		strconv.FormatUint(leaseEpoch, 10),
		sha256Hex("lease-token", leaseToken),
		strconv.FormatUint(workVersion, 10),
		workRef,
		workType, workID, proof.SourceAuthorityKind, proof.CanonicalSourceAuthorityID,
		proof.AuthorityLayoutVersion, proof.ActorRouteDomain, proof.PayloadHash,
		proof.InvocationID, proof.ParticipantStep, proof.ParticipantOutcome,
		proof.ParticipantResultHash, strconv.FormatUint(proof.ParticipantResultVersion, 10), proof.ResultProofHash,
	}
	reply, err := w.client.registry.RunNoRetry(ctx, ScriptCompleteWorkV5, []string{workKey.String(), dueKey.String(), bucketMetaKey.String()}, args...)
	if err != nil {
		return WorkCompleteResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkCompleteResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseCompleted}, nil
	case "ALREADY_COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseAlreadyCompleted}, nil
	case "FENCE_MISMATCH":
		return WorkCompleteResult{Status: WorkLeaseFenceMismatch}, nil
	case "STALE_VERSION":
		return WorkCompleteResult{Status: WorkLeaseStaleVersion}, nil
	case "NOT_FOUND":
		return WorkCompleteResult{Status: WorkLeaseNotFound}, nil
	case "LEASE_EXPIRED":
		return WorkCompleteResult{Status: WorkLeaseExpired}, nil
	case "RESULT_PROOF_INVALID":
		return WorkCompleteResult{Status: WorkLeaseFenceMismatch}, nil
	case "RECONCILE_REQUIRED":
		return WorkCompleteResult{Status: WorkLeaseReconcileRequired}, nil
	case "MALFORMED_RECORD":
		return WorkCompleteResult{}, fmt.Errorf("%w: source order terminal record", errMalformedBlob)
	default:
		return WorkCompleteResult{}, fmt.Errorf("%w: complete status %q", errScriptContract, reply[0])
	}
}

type WorkClaimAndCompleteRequest struct {
	Bucket         rediskey.AuthorityBucketRef
	WorkType       string
	WorkID         string
	HolderID       string
	BootID         string
	LeaseToken     string
	RequestedTTLms int64
	Proof          WorkCompletionProof
}

type WorkClaimAndCompleteBatchResult struct {
	Result WorkCompleteResult
	Err    error
}

func (w *WorkLeaseStore) ClaimAndCompleteWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, leaseToken string, requestedTTLms int64, proof WorkCompletionProof) (WorkCompleteResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkCompleteResult{}, err
	}
	call, err := w.prepareClaimAndComplete(WorkClaimAndCompleteRequest{
		Bucket: bucket, WorkType: workType, WorkID: workID, HolderID: holderID, BootID: bootID,
		LeaseToken: leaseToken, RequestedTTLms: requestedTTLms, Proof: proof,
	})
	if err != nil {
		return WorkCompleteResult{}, err
	}
	reply, runErr := w.client.registry.RunNoRetry(ctx, ScriptClaimAndCompleteWorkV4, call.keys, call.args...)
	return decodeClaimAndComplete(reply, runErr)
}

// ClaimAndCompleteWithProofBatch pipelines independent direct completions.
// Each item remains one atomic Lua invocation and receives its own result.
func (w *WorkLeaseStore) ClaimAndCompleteWithProofBatch(ctx context.Context, requests []WorkClaimAndCompleteRequest) []WorkClaimAndCompleteBatchResult {
	results := make([]WorkClaimAndCompleteBatchResult, len(requests))
	if len(requests) == 0 {
		return results
	}
	if len(requests) > 256 {
		err := fmt.Errorf("%w: completion batch exceeds 256", authoritystore.ErrInvalidRequest)
		for i := range results {
			results[i].Err = err
		}
		return results
	}
	calls := make([]scriptBatchCall, 0, len(requests))
	indexes := make([]int, 0, len(requests))
	for i, request := range requests {
		call, err := w.prepareClaimAndComplete(request)
		if err != nil {
			results[i].Err = err
			continue
		}
		calls = append(calls, call)
		indexes = append(indexes, i)
	}
	batch := w.client.registry.runBatch(ctx, ScriptClaimAndCompleteWorkV4, calls)
	for i, item := range batch {
		result, err := decodeClaimAndComplete(item.reply, item.err)
		results[indexes[i]] = WorkClaimAndCompleteBatchResult{Result: result, Err: err}
	}
	return results
}

func (w *WorkLeaseStore) prepareClaimAndComplete(request WorkClaimAndCompleteRequest) (scriptBatchCall, error) {
	if w == nil || w.client == nil {
		return scriptBatchCall{}, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if err := validateStableID("holder_id", request.HolderID); err != nil {
		return scriptBatchCall{}, err
	}
	if err := validateStableID("boot_id", request.BootID); err != nil {
		return scriptBatchCall{}, err
	}
	if err := validateStableID("lease_token", request.LeaseToken); err != nil {
		return scriptBatchCall{}, err
	}
	if request.RequestedTTLms <= 0 {
		return scriptBatchCall{}, fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest)
	}
	proof := request.Proof
	for name, value := range map[string]string{
		"source_authority_kind": proof.SourceAuthorityKind, "canonical_source_authority_id": proof.CanonicalSourceAuthorityID,
		"authority_layout_version": proof.AuthorityLayoutVersion, "actor_route_domain": proof.ActorRouteDomain,
		"payload_hash": proof.PayloadHash, "participant_invocation_id": proof.InvocationID,
		"participant_step": proof.ParticipantStep, "participant_outcome": proof.ParticipantOutcome,
		"participant_result_hash": proof.ParticipantResultHash, "result_proof_hash": proof.ResultProofHash,
	} {
		if err := validateStableID(name, value); err != nil {
			return scriptBatchCall{}, err
		}
	}
	if !validHexDigest(proof.PayloadHash) || !validHexDigest(proof.ParticipantResultHash) || !validHexDigest(proof.ResultProofHash) ||
		proof.ParticipantResultVersion == 0 || proof.ResultProofHash != WorkCompletionProofHash(proof) {
		return scriptBatchCall{}, fmt.Errorf("%w: result proof hash/version", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, bucketMetaKey, workRef, err := w.keysFor(request.Bucket, request.WorkType, request.WorkID)
	if err != nil {
		return scriptBatchCall{}, err
	}
	return scriptBatchCall{
		keys: []string{workKey.String(), dueKey.String(), bucketMetaKey.String()},
		args: []interface{}{
			tag, request.HolderID, request.BootID, sha256Hex("lease-token", request.LeaseToken),
			strconv.FormatInt(request.RequestedTTLms, 10), strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
			strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10), workRef, request.WorkType, request.WorkID,
			proof.SourceAuthorityKind, proof.CanonicalSourceAuthorityID, proof.AuthorityLayoutVersion,
			proof.ActorRouteDomain, proof.PayloadHash, proof.InvocationID, proof.ParticipantStep,
			proof.ParticipantOutcome, proof.ParticipantResultHash,
			strconv.FormatUint(proof.ParticipantResultVersion, 10), proof.ResultProofHash,
		},
	}, nil
}

func decodeClaimAndComplete(reply []string, err error) (WorkCompleteResult, error) {
	if err != nil {
		return WorkCompleteResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkCompleteResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseCompleted}, nil
	case "ALREADY_COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseAlreadyCompleted}, nil
	case "FENCE_MISMATCH", "RESULT_PROOF_INVALID", "LEASE_HELD", "NOT_DUE", "NOT_CLAIMABLE":
		return WorkCompleteResult{Status: WorkLeaseFenceMismatch}, nil
	case "NOT_FOUND":
		return WorkCompleteResult{Status: WorkLeaseNotFound}, nil
	case "RECONCILE_REQUIRED":
		return WorkCompleteResult{Status: WorkLeaseReconcileRequired}, nil
	case "MALFORMED_RECORD":
		return WorkCompleteResult{}, fmt.Errorf("%w: source order terminal record", errMalformedBlob)
	default:
		return WorkCompleteResult{}, fmt.Errorf("%w: claim-and-complete status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) CompleteScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string) (WorkCompleteResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkCompleteResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkCompleteResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkCompleteResult{}, err
	}
	if err := validateStableID("lease_token", leaseToken); err != nil {
		return WorkCompleteResult{}, err
	}
	if leaseEpoch == 0 || workVersion == 0 || generation == 0 {
		return WorkCompleteResult{}, fmt.Errorf("%w: schedule complete fence", authoritystore.ErrInvalidRequest)
	}
	var tag, scheduleRef string
	var scriptKeys []string
	scriptName := ScriptCompleteScheduledWork
	if w.client.opts.ScheduleDueIndexMode.writesDedicated() {
		var scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis, bucketMetaKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, scheduleDueKeyRedis, bucketMetaKeyRedis, scheduleRef, err = w.scheduleDualKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return WorkCompleteResult{}, err
		}
		scriptName = ScriptCompleteScheduledWorkDual
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String(), bucketMetaKeyRedis.String(), scheduleDueKeyRedis.String()}
	} else {
		var scheduleKeyRedis, dueKeyRedis, bucketMetaKeyRedis rediskey.RedisKey
		var err error
		tag, scheduleKeyRedis, dueKeyRedis, bucketMetaKeyRedis, scheduleRef, err = w.scheduleKeysFor(bucket, scheduleType, scheduleID)
		if err != nil {
			return WorkCompleteResult{}, err
		}
		scriptKeys = []string{scheduleKeyRedis.String(), dueKeyRedis.String(), bucketMetaKeyRedis.String()}
	}
	args := []interface{}{tag, holderID, bootID, strconv.FormatUint(leaseEpoch, 10), sha256Hex("lease-token", leaseToken),
		strconv.FormatUint(workVersion, 10), strconv.FormatUint(generation, 10), scheduleRef, scheduleType, scheduleID}
	reply, err := w.client.registry.Run(ctx, scriptName, scriptKeys, args...)
	if err != nil {
		return WorkCompleteResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkCompleteResult{}, fmt.Errorf("%w: empty schedule complete reply", errScriptContract)
	}
	switch reply[0] {
	case "COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseCompleted}, nil
	case "ALREADY_COMPLETED":
		return WorkCompleteResult{Status: WorkLeaseAlreadyCompleted}, nil
	case "STALE_GENERATION":
		return WorkCompleteResult{Status: WorkLeaseStaleGeneration}, nil
	case "STALE_VERSION":
		return WorkCompleteResult{Status: WorkLeaseStaleVersion}, nil
	case "FENCE_MISMATCH":
		return WorkCompleteResult{Status: WorkLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return WorkCompleteResult{Status: WorkLeaseExpired}, nil
	case "NOT_FOUND":
		return WorkCompleteResult{Status: WorkLeaseNotFound}, nil
	case "MALFORMED_RECORD":
		return WorkCompleteResult{}, fmt.Errorf("%w: schedule record", errMalformedBlob)
	default:
		return WorkCompleteResult{}, fmt.Errorf("%w: schedule complete status %q", errScriptContract, reply[0])
	}
}

// RescheduleWork persists retry history and the next due point on the same
// SourceOrder WorkRecord. The Lua script adjudicates lease expiry and the
// total deadline with Redis TIME, so a restarted worker cannot reset attempts.
func (w *WorkLeaseStore) RescheduleWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, leaseEpoch, workVersion uint64, leaseToken string, retryCount uint64, requestedDelayMs int64, errorClass string) (WorkRescheduleResult, error) {
	if err := ctx.Err(); err != nil {
		return WorkRescheduleResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return WorkRescheduleResult{}, err
	}
	if err := validateStableID("boot_id", bootID); err != nil {
		return WorkRescheduleResult{}, err
	}
	if leaseEpoch == 0 || workVersion == 0 || retryCount == 0 || requestedDelayMs <= 0 {
		return WorkRescheduleResult{}, fmt.Errorf("%w: reschedule fence/retry/delay", authoritystore.ErrInvalidRequest)
	}
	if errorClass == "" {
		errorClass = "UNKNOWN"
	}
	if len(errorClass) > 128 {
		return WorkRescheduleResult{}, fmt.Errorf("%w: error class", authoritystore.ErrInvalidRequest)
	}
	tag, workKey, dueKey, bucketMetaKey, workRef, err := w.keysFor(bucket, workType, workID)
	if err != nil {
		return WorkRescheduleResult{}, err
	}
	args := []interface{}{
		tag, holderID, bootID, strconv.FormatUint(leaseEpoch, 10), sha256Hex("lease-token", leaseToken),
		strconv.FormatUint(workVersion, 10), workRef, strconv.FormatUint(retryCount, 10),
		strconv.FormatInt(requestedDelayMs, 10), errorClass,
	}
	reply, err := w.client.registry.RunNoRetry(ctx, ScriptRescheduleWorkV2, []string{workKey.String(), dueKey.String(), bucketMetaKey.String()}, args...)
	if err != nil {
		return WorkRescheduleResult{Status: workLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return WorkRescheduleResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	result := WorkRescheduleResult{}
	switch reply[0] {
	case "RESCHEDULED":
		result.Status = WorkLeaseRenewed
		if len(reply) > 1 {
			result.RetryCount, _ = strconv.ParseUint(reply[1], 10, 64)
		}
		if len(reply) > 2 {
			result.NextDueAtMs, _ = strconv.ParseInt(reply[2], 10, 64)
		}
		return result, nil
	case "RECONCILE_REQUIRED":
		result.Status = WorkLeaseReconcileRequired
		if len(reply) > 1 {
			result.RetryCount, _ = strconv.ParseUint(reply[1], 10, 64)
		}
		return result, nil
	case "FENCE_MISMATCH":
		return WorkRescheduleResult{Status: WorkLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return WorkRescheduleResult{Status: WorkLeaseExpired}, nil
	case "NOT_FOUND":
		return WorkRescheduleResult{Status: WorkLeaseNotFound}, nil
	case "MALFORMED_RECORD":
		return WorkRescheduleResult{}, fmt.Errorf("%w: source order terminal record", errMalformedBlob)
	default:
		return WorkRescheduleResult{}, fmt.Errorf("%w: reschedule status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) ClaimBucketScanLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, holderID string, requestedTTLms int64) (ScanLeaseResult, error) {
	if err := ctx.Err(); err != nil {
		return ScanLeaseResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return ScanLeaseResult{}, err
	}
	if requestedTTLms <= 0 {
		return ScanLeaseResult{}, fmt.Errorf("%w: requested_ttl_ms", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return ScanLeaseResult{}, err
	}
	tag, err := w.client.keys.HashTag(bucket)
	if err != nil {
		return ScanLeaseResult{}, err
	}
	leaseKey, err := w.client.keys.BucketScanLease(bucket)
	if err != nil {
		return ScanLeaseResult{}, err
	}
	args := []interface{}{
		tag, holderID,
		strconv.FormatInt(requestedTTLms, 10),
		strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10),
	}
	reply, err := w.client.registry.Run(ctx, ScriptClaimBucketScanLease, []string{leaseKey.String()}, args...)
	if err != nil {
		return ScanLeaseResult{Status: scanLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return ScanLeaseResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "CLAIMED":
		epoch, _ := strconv.ParseUint(reply[1], 10, 64)
		expireAt, _ := strconv.ParseInt(reply[2], 10, 64)
		acceptedTTL, _ := strconv.ParseInt(reply[3], 10, 64)
		return ScanLeaseResult{Status: ScanLeaseClaimed, LeaseEpoch: epoch, LeaseExpireAtMs: expireAt, AcceptedTTLms: acceptedTTL}, nil
	case "HELD_BY_OTHER":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		return ScanLeaseResult{Status: ScanLeaseHeldByOther, LeaseExpireAtMs: expireAt}, nil
	default:
		return ScanLeaseResult{}, fmt.Errorf("%w: scan claim status %q", errScriptContract, reply[0])
	}
}

func (w *WorkLeaseStore) RenewBucketScanLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, holderID string, leaseEpoch uint64, requestedTTLms int64) (ScanLeaseResult, error) {
	if err := ctx.Err(); err != nil {
		return ScanLeaseResult{}, err
	}
	if err := validateStableID("holder_id", holderID); err != nil {
		return ScanLeaseResult{}, err
	}
	if leaseEpoch == 0 || requestedTTLms <= 0 {
		return ScanLeaseResult{}, fmt.Errorf("%w: lease epoch/ttl", authoritystore.ErrInvalidRequest)
	}
	if err := bucket.Validate(); err != nil {
		return ScanLeaseResult{}, err
	}
	tag, err := w.client.keys.HashTag(bucket)
	if err != nil {
		return ScanLeaseResult{}, err
	}
	leaseKey, err := w.client.keys.BucketScanLease(bucket)
	if err != nil {
		return ScanLeaseResult{}, err
	}
	args := []interface{}{
		tag, holderID,
		strconv.FormatUint(leaseEpoch, 10),
		strconv.FormatInt(requestedTTLms, 10),
		strconv.FormatInt(w.leaseLimits.MinTTL.Milliseconds(), 10),
		strconv.FormatInt(w.leaseLimits.MaxTTL.Milliseconds(), 10),
	}
	reply, err := w.client.registry.Run(ctx, ScriptRenewBucketScanLease, []string{leaseKey.String()}, args...)
	if err != nil {
		return ScanLeaseResult{Status: scanLeaseTransportStatus(err)}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return ScanLeaseResult{}, fmt.Errorf("%w: empty reply", errScriptContract)
	}
	switch reply[0] {
	case "RENEWED":
		expireAt, _ := strconv.ParseInt(reply[1], 10, 64)
		acceptedTTL, _ := strconv.ParseInt(reply[2], 10, 64)
		return ScanLeaseResult{Status: ScanLeaseRenewed, LeaseExpireAtMs: expireAt, AcceptedTTLms: acceptedTTL}, nil
	case "FENCE_MISMATCH":
		return ScanLeaseResult{Status: ScanLeaseFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return ScanLeaseResult{Status: ScanLeaseExpired}, nil
	case "NOT_FOUND":
		return ScanLeaseResult{Status: ScanLeaseNotFound}, nil
	default:
		return ScanLeaseResult{}, fmt.Errorf("%w: scan renew status %q", errScriptContract, reply[0])
	}
}

func workTransportStatus(err error) WorkClaimStatus {
	if classifyWriteError(err) == fateNotSent {
		return WorkClaimUnavailableNotSent
	}
	return WorkClaimTimeoutUnknown
}

func workLeaseTransportStatus(err error) WorkLeaseStatus {
	if classifyWriteError(err) == fateNotSent {
		return WorkLeaseUnavailableNotSent
	}
	return WorkLeaseTimeoutUnknown
}

func scanLeaseTransportStatus(err error) ScanLeaseStatus {
	if classifyWriteError(err) == fateNotSent {
		return ScanLeaseUnavailableNotSent
	}
	return ScanLeaseTimeoutUnknown
}

// HashLeaseToken is the physical token digest used by Claim/Renew/Complete.
// Callers that only need to compare a read-only SourceOrder lease should use
// this helper rather than duplicating the prefixing rule.
func HashLeaseToken(token string) string { return sha256Hex("lease-token", token) }

func validHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func transportErrIfContract(err error) error {
	if err != nil && isContractViolation(err) {
		return fmt.Errorf("%w: %v", errScriptContract, err)
	}
	return nil
}
