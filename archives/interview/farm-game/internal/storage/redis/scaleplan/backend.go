package scaleplan

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/codec"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

const (
	defaultMaxItems        = 1024
	defaultMaxBytes        = 1 << 30
	defaultMaxRecordBytes  = 1 << 20
	defaultPageSize        = 256
	defaultCleanupLimit    = 64
	defaultLeaseTTL        = 20 * time.Second
	defaultTerminalTTL     = 38 * 24 * time.Hour
	maxItemsHard           = 1 << 20
	maxRecordBytesHard     = 8 << 20
	maxPageSizeHard        = 256
	maxCleanupLimitHard    = 256
	maxScriptManifestBytes = 16 << 20
)

var (
	ErrLeaseBusy              = errors.New("redisscaleplan: mutation lease busy")
	ErrLeaseLost              = errors.New("redisscaleplan: mutation lease lost")
	ErrMiniredisTimeSemantics = errors.New("redisscaleplan: BLOCKED_MINIREDIS_TIME_SEMANTICS")
	ErrBoundExceeded          = errors.New("redisscaleplan: bounded operation exceeded")
	ErrScriptContract         = errors.New("redisscaleplan: script contract violation")
	ErrOutcomeUnknown         = core.ErrOutcomeUnknown
)

// Options is intentionally small.  Limits are enforced both before EVAL and
// again by the Lua boundary; a caller cannot enlarge the hard bounds.
type Options struct {
	Namespace         rediskey.Namespace
	Realm             string
	MaxItems          int
	MaxBytes          int64
	MaxRecordBytes    int
	PageSize          int
	LeaseTTL          time.Duration
	TerminalRetention time.Duration
	AfterMutation     func(string) error // test-only response-loss injection
}

type BackendOptions = Options

type Backend struct {
	client  redis.UniversalClient
	keys    KeyBuilder
	options Options
	mu      sync.Mutex
}

type Store = Backend

// TerminalCleanupResult reports one bounded terminal-retention batch.  A
// caller that receives MoreDue should invoke CleanupTerminal again rather than
// widening the batch.
type TerminalCleanupResult struct {
	Cleaned int
	MoreDue bool
}

type CleanupResult = TerminalCleanupResult

func NewBackend(client redis.UniversalClient, args ...interface{}) (*Backend, error) {
	return newBackend(client, args...)
}
func NewStore(client redis.UniversalClient, args ...interface{}) (*Backend, error) {
	return newBackend(client, args...)
}
func New(client redis.UniversalClient, args ...interface{}) (*Backend, error) {
	return newBackend(client, args...)
}

func NewWithOptions(client redis.UniversalClient, options Options) (*Backend, error) {
	return newBackend(client, options)
}

func newBackend(client redis.UniversalClient, args ...interface{}) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client", core.ErrInvalidPlan)
	}
	var options Options
	var builder KeyBuilder
	haveBuilder := false
	for _, arg := range args {
		switch value := arg.(type) {
		case Options:
			options = value
		case *Options:
			if value == nil {
				return nil, fmt.Errorf("%w: nil options", core.ErrInvalidPlan)
			}
			options = *value
		case KeyBuilder:
			builder = value
			haveBuilder = true
		case rediskey.Namespace:
			options.Namespace = value
		case string:
			if options.Realm == "" {
				options.Realm = value
			} else {
				return nil, fmt.Errorf("%w: duplicate realm", core.ErrInvalidPlan)
			}
		default:
			return nil, fmt.Errorf("%w: unsupported constructor argument %T", core.ErrInvalidPlan, arg)
		}
	}
	if !haveBuilder {
		var err error
		builder, err = NewKeyBuilder(options.Namespace, options.Realm)
		if err != nil {
			return nil, err
		}
	} else {
		if options.Realm != "" && options.Realm != builder.Realm() {
			return nil, ErrInvalidKey
		}
		options.Namespace, options.Realm = builder.Namespace(), builder.Realm()
	}
	if options.MaxItems == 0 {
		options.MaxItems = defaultMaxItems
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultMaxBytes
	}
	if options.MaxRecordBytes == 0 {
		options.MaxRecordBytes = defaultMaxRecordBytes
	}
	if options.PageSize == 0 {
		options.PageSize = defaultPageSize
	}
	if options.LeaseTTL == 0 {
		options.LeaseTTL = defaultLeaseTTL
	}
	if options.TerminalRetention == 0 {
		options.TerminalRetention = defaultTerminalTTL
	}
	if options.MaxItems < 1 || options.MaxItems > maxItemsHard || options.MaxBytes < 1 || options.MaxBytes > defaultMaxBytes ||
		options.MaxRecordBytes < 1 || options.MaxRecordBytes > maxRecordBytesHard || options.PageSize < 1 || options.PageSize > maxPageSizeHard ||
		options.LeaseTTL <= 0 || options.LeaseTTL > 60*time.Second || options.TerminalRetention <= 0 {
		return nil, fmt.Errorf("%w: scaleplan limits", core.ErrInvalidPlan)
	}
	return &Backend{client: client, keys: builder, options: options}, nil
}

func (b *Backend) Keys() KeyBuilder              { return b.keys }
func (b *Backend) Namespace() rediskey.Namespace { return b.keys.Namespace() }
func (b *Backend) Realm() string                 { return b.keys.Realm() }
func (b *Backend) RepositoryBindingID() string   { return "redis-scaleplan-v1-" + b.keys.RealmDigest() }

func (b *Backend) ScriptSHA(name string) string {
	source, ok := scriptSources[name]
	if !ok {
		return ""
	}
	// redis/go-redis uses SHA1 for EVALSHA.  Keeping this helper local avoids a
	// second script registry dependency and still permits golden tests.
	return sha1Hex(source)
}

func sha1Hex(value string) string {
	sum := sha1.Sum([]byte(value)) //nolint:gosec // Redis script cache identity.
	return hex.EncodeToString(sum[:])
}

func (b *Backend) runScript(ctx context.Context, name string, keys []rediskey.RedisKey, args ...interface{}) ([]string, error) {
	if ctx == nil {
		return nil, core.ErrInvalidPlan
	}
	if err := b.keys.AssertSameSlot(keys...); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	source, ok := scriptSources[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown script %s", ErrScriptContract, name)
	}
	keyStrings := make([]string, len(keys))
	for i := range keys {
		keyStrings[i] = keys[i].String()
	}
	value, err := b.client.EvalSha(ctx, sha1Hex(source), keyStrings, args...).Result()
	if err != nil {
		if strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT") {
			value, err = b.client.Eval(ctx, source, keyStrings, args...).Result()
		}
	}
	if err != nil {
		return nil, err
	}
	return scriptReplyStrings(value)
}

func scriptReplyStrings(value interface{}) ([]string, error) {
	items, ok := value.([]interface{})
	if !ok {
		if strings, ok2 := value.([]string); ok2 {
			return strings, nil
		}
		return nil, fmt.Errorf("%w: reply %T", ErrScriptContract, value)
	}
	out := make([]string, len(items))
	for i, item := range items {
		switch v := item.(type) {
		case string:
			out[i] = v
		case []byte:
			out[i] = string(v)
		case int64:
			out[i] = strconv.FormatInt(v, 10)
		case nil:
			out[i] = ""
		default:
			return nil, fmt.Errorf("%w: reply element %T", ErrScriptContract, item)
		}
	}
	return out, nil
}

func (b *Backend) mutationError(out []string) error {
	if len(out) == 0 {
		return fmt.Errorf("%w: empty reply", ErrScriptContract)
	}
	switch out[0] {
	case "LEASE_BUSY":
		return ErrLeaseBusy
	case "LEASE_LOST":
		return ErrLeaseLost
	case "FENCE_EXHAUSTED":
		return fmt.Errorf("%w: fence exhausted", core.ErrInvalidPlan)
	case "REALM_CORRUPT_LATCHED":
		return fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	case "CORRUPT":
		return fmt.Errorf("%w: %s", core.ErrCorruptRecord, strings.Join(out[1:], ":"))
	case "MINIREDIS_TIME_UNSUPPORTED":
		return ErrMiniredisTimeSemantics
	case "RETRY":
		return fmt.Errorf("%w: concurrent index changed", core.ErrRevisionConflict)
	case "REVISION_CONFLICT":
		return core.ErrRevisionConflict
	case "IDEMPOTENCY_CONFLICT":
		return core.ErrIdempotencyConflict
	case "WORKLOAD_BUSY":
		return core.ErrWorkloadBusy
	case "CAPACITY_EXCEEDED":
		return core.ErrCapacityExceeded
	case "PROOF_EXPIRED":
		return core.ErrVerification
	case "NOT_BLOCKED":
		return core.ErrBlocked
	case "MALFORMED":
		return fmt.Errorf("%w: malformed script input", core.ErrInvalidPlan)
	}
	return nil
}

func (b *Backend) maybeUnknown(name string) error {
	if b.options.AfterMutation == nil {
		return nil
	}
	if err := b.options.AfterMutation(name); err != nil {
		return fmt.Errorf("%w: %v", core.ErrOutcomeUnknown, err)
	}
	return nil
}

func parseUint(value string, maximum uint64) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("%w: noncanonical integer", core.ErrCorruptRecord)
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("%w: noncanonical integer", core.ErrCorruptRecord)
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > maximum || strconv.FormatUint(n, 10) != value {
		return 0, fmt.Errorf("%w: integer bound", core.ErrCorruptRecord)
	}
	return n, nil
}
func canonicalUint(value uint64) string { return strconv.FormatUint(value, 10) }
func canonicalInt(value int64) string   { return strconv.FormatInt(value, 10) }

func isTerminal(status core.ExecutionStatus) bool { return status.Terminal() }
func terminalRetention(updated int64, retention time.Duration) (string, error) {
	if updated <= 0 {
		return "", fmt.Errorf("%w: terminal time", core.ErrInvalidPlan)
	}
	value := updated + retention.Milliseconds()
	if value < updated || value > int64(core.MaxLuaSafeInteger) {
		return "", fmt.Errorf("%w: retention bound", core.ErrInvalidPlan)
	}
	return canonicalInt(value), nil
}

type planEnvelope struct {
	plan           core.Plan
	canonical      []byte
	checksum       string
	bytes          string
	member         string
	idemDigest     string
	workloadDigest string
	terminalRetain string
}

func makeEnvelope(b *Backend, plan core.Plan) (planEnvelope, error) {
	canonical, err := codec.EncodePlan(plan)
	if err != nil {
		return planEnvelope{}, err
	}
	if len(canonical) > b.options.MaxRecordBytes {
		return planEnvelope{}, fmt.Errorf("%w: record bytes", core.ErrCapacityExceeded)
	}
	checksum := codec.Checksum(canonical)
	member, err := b.keys.PlanMember(plan.PlanID)
	if err != nil {
		return planEnvelope{}, err
	}
	idem, err := b.keys.IdempotencyDigest(plan.IdempotencyKey)
	if err != nil {
		return planEnvelope{}, err
	}
	workload, err := b.keys.ActiveDigest(plan.Submission.Workload.Key())
	if err != nil {
		return planEnvelope{}, err
	}
	retain := "0"
	if plan.Status.Terminal() {
		retentionBase := plan.CompletedAtMs
		if retentionBase == 0 {
			retentionBase = plan.UpdatedAtMs
		}
		retain, err = terminalRetention(retentionBase, b.options.TerminalRetention)
		if err != nil {
			return planEnvelope{}, err
		}
	}
	return planEnvelope{plan: plan.Clone(), canonical: canonical, checksum: checksum, bytes: canonicalUint(uint64(len(canonical))), member: member, idemDigest: idem, workloadDigest: workload, terminalRetain: retain}, nil
}

func planFields(e planEnvelope) []interface{} {
	p := e.plan
	return []interface{}{e.bytes, canonicalUint(p.Revision), string(p.Status), canonicalInt(p.UpdatedAtMs), canonicalInt(p.CompletedAtMs), canonicalInt(p.RecoveryRetainUntilMs), e.terminalRetain, string(e.canonical), e.checksum}
}

type metaState struct {
	exists                                                                              bool
	count, bytes, active, recoverable, recoverableBytes, terminal, terminalBytes, epoch uint64
}

var metaFieldNames = []string{"schema", "realm", "realm_digest", "count", "bytes", "active_count", "recoverable_count", "recoverable_bytes", "terminal_count", "terminal_bytes", "store_epoch"}
var planFieldNames = []string{"schema", "realm", "plan_id", "revision", "status", "updated_at_ms", "completed_at_ms", "recovery_retain_until_ms", "terminal_retain_until_ms", "canonical_json", "checksum_sha256", "canonical_bytes"}
var idemFieldNames = []string{"schema", "realm", "key_digest", "raw_key", "payload_digest", "plan_id", "retain_until_ms"}
var activeFieldNames = []string{"schema", "realm", "workload_digest", "canonical_workload_key", "plan_id", "plan_revision"}

func resultStrings(values []interface{}) ([]string, error) {
	out := make([]string, len(values))
	for i, v := range values {
		switch x := v.(type) {
		case nil:
			out[i] = ""
		case string:
			out[i] = x
		case []byte:
			out[i] = string(x)
		default:
			return nil, fmt.Errorf("%w: redis value %T", ErrScriptContract, v)
		}
	}
	return out, nil
}

func (b *Backend) hashValues(ctx context.Context, key rediskey.RedisKey, fields []string) ([]string, bool, error) {
	typ, err := b.client.Type(ctx, key.String()).Result()
	if err != nil {
		return nil, false, err
	}
	if typ == "none" {
		return nil, false, nil
	}
	if typ != "hash" {
		return nil, true, fmt.Errorf("%w: key %s type %s", core.ErrCorruptRecord, key, typ)
	}
	length, err := b.client.HLen(ctx, key.String()).Result()
	if err != nil {
		return nil, true, err
	}
	if int(length) != len(fields) {
		return nil, true, fmt.Errorf("%w: hash field count %s", core.ErrCorruptRecord, key)
	}
	values, err := b.client.HMGet(ctx, key.String(), fields...).Result()
	if err != nil {
		return nil, true, err
	}
	out, err := resultStrings(values)
	if err != nil {
		return nil, true, err
	}
	for i := range out {
		if out[i] == "" {
			return nil, true, fmt.Errorf("%w: missing field %s", core.ErrCorruptRecord, fields[i])
		}
	}
	return out, true, nil
}

func (b *Backend) hashValuesLoose(ctx context.Context, key rediskey.RedisKey, fields []string) ([]string, bool, error) {
	typ, err := b.client.Type(ctx, key.String()).Result()
	if err != nil {
		return nil, false, err
	}
	if typ == "none" {
		return nil, false, nil
	}
	if typ != "hash" {
		return nil, true, fmt.Errorf("%w: key type %s", core.ErrCorruptRecord, typ)
	}
	values, err := b.client.HMGet(ctx, key.String(), fields...).Result()
	if err != nil {
		return nil, true, err
	}
	out, err := resultStrings(values)
	if err != nil {
		return nil, true, err
	}
	for i := range out {
		if out[i] == "" {
			return nil, true, fmt.Errorf("%w: missing field %s", core.ErrCorruptRecord, fields[i])
		}
	}
	return out, true, nil
}

func (b *Backend) readMeta(ctx context.Context) (metaState, error) {
	typ, err := b.client.Type(ctx, b.keys.Meta().String()).Result()
	if err != nil {
		return metaState{}, err
	}
	if typ == "none" {
		for _, key := range []rediskey.RedisKey{b.keys.Recoverable(), b.keys.Terminal(), b.keys.Accounting()} {
			kind, e := b.client.Type(ctx, key.String()).Result()
			if e != nil {
				return metaState{}, e
			}
			if kind != "none" {
				return metaState{}, fmt.Errorf("%w: metadata missing with %s", core.ErrCorruptRecord, key)
			}
		}
		return metaState{}, nil
	}
	values, _, err := b.hashValues(ctx, b.keys.Meta(), metaFieldNames)
	if err != nil {
		return metaState{}, err
	}
	if values[0] != "scale-plan-meta-v1" || values[1] != b.keys.Realm() || values[2] != b.keys.RealmDigest() {
		return metaState{}, fmt.Errorf("%w: meta identity", core.ErrCorruptRecord)
	}
	parsed := make([]uint64, 8)
	for i := 0; i < 8; i++ {
		parsed[i], err = parseUint(values[3+i], core.MaxLuaSafeInteger)
		if err != nil {
			return metaState{}, err
		}
	}
	if parsed[0] > uint64(b.options.MaxItems) || parsed[1] > uint64(b.options.MaxBytes) || parsed[2] > 1 || parsed[3] > uint64(b.options.MaxItems) || parsed[5] > uint64(b.options.MaxItems) || parsed[0] != parsed[3]+parsed[5] || parsed[1] != parsed[4]+parsed[6] {
		return metaState{}, fmt.Errorf("%w: meta accounting", core.ErrCorruptRecord)
	}
	for _, key := range []rediskey.RedisKey{b.keys.Recoverable(), b.keys.Terminal(), b.keys.Accounting()} {
		kind, e := b.client.Type(ctx, key.String()).Result()
		if e != nil {
			return metaState{}, e
		}
		if kind == "none" {
			if key == b.keys.Accounting() && parsed[0] != 0 {
				return metaState{}, fmt.Errorf("%w: missing accounting", core.ErrCorruptRecord)
			}
			if key == b.keys.Recoverable() && parsed[3] != 0 {
				return metaState{}, fmt.Errorf("%w: missing recoverable", core.ErrCorruptRecord)
			}
			if key == b.keys.Terminal() && parsed[5] != 0 {
				return metaState{}, fmt.Errorf("%w: missing terminal", core.ErrCorruptRecord)
			}
			continue
		}
		want := "zset"
		if key == b.keys.Accounting() {
			want = "hash"
		}
		if kind != want {
			return metaState{}, fmt.Errorf("%w: index shape %s", core.ErrCorruptRecord, key)
		}
	}
	recCard, err := b.client.ZCard(ctx, b.keys.Recoverable().String()).Result()
	if err != nil && parsed[3] != 0 {
		return metaState{}, err
	}
	termCard, err := b.client.ZCard(ctx, b.keys.Terminal().String()).Result()
	if err != nil && parsed[5] != 0 {
		return metaState{}, err
	}
	accountCard, err := b.client.HLen(ctx, b.keys.Accounting().String()).Result()
	if err != nil && parsed[0] != 0 {
		return metaState{}, err
	}
	if uint64(recCard) != parsed[3] || uint64(termCard) != parsed[5] || uint64(accountCard) != parsed[0] {
		return metaState{}, fmt.Errorf("%w: index cardinality", core.ErrCorruptRecord)
	}
	return metaState{exists: true, count: parsed[0], bytes: parsed[1], active: parsed[2], recoverable: parsed[3], recoverableBytes: parsed[4], terminal: parsed[5], terminalBytes: parsed[6], epoch: parsed[7]}, nil
}

func (b *Backend) readRawPlan(ctx context.Context, id string) (planEnvelope, bool, error) {
	key, err := b.keys.Plan(id)
	if err != nil {
		return planEnvelope{}, false, err
	}
	kind, err := b.client.Type(ctx, key.String()).Result()
	if err != nil {
		return planEnvelope{}, false, err
	}
	if kind == "none" {
		return planEnvelope{}, false, nil
	}
	if kind != "hash" {
		return planEnvelope{}, true, fmt.Errorf("%w: plan type", core.ErrCorruptRecord)
	}
	values, _, err := b.hashValuesLoose(ctx, key, planFieldNames)
	if err != nil {
		return planEnvelope{}, true, err
	}
	if values[0] != codec.RecordSchema || values[1] != b.keys.Realm() || values[2] != id {
		return planEnvelope{}, true, fmt.Errorf("%w: plan identity", core.ErrCorruptRecord)
	}
	bytes, err := parseUint(values[11], uint64(b.options.MaxRecordBytes))
	if err != nil || bytes != uint64(len(values[9])) {
		return planEnvelope{}, true, fmt.Errorf("%w: plan byte count", core.ErrCorruptRecord)
	}
	if !codec.VerifyChecksum([]byte(values[9]), values[10]) {
		return planEnvelope{}, true, fmt.Errorf("%w: plan checksum", core.ErrCorruptRecord)
	}
	plan, err := codec.DecodePlan([]byte(values[9]))
	if err != nil {
		return planEnvelope{}, true, fmt.Errorf("%w: plan payload: %v", core.ErrCorruptRecord, err)
	}
	if plan.PlanID != id {
		return planEnvelope{}, true, fmt.Errorf("%w: plan payload id", core.ErrCorruptRecord)
	}
	member, err := b.keys.PlanMember(id)
	if err != nil {
		return planEnvelope{}, true, err
	}
	idem, err := b.keys.IdempotencyDigest(plan.IdempotencyKey)
	if err != nil {
		return planEnvelope{}, true, err
	}
	workload, err := b.keys.ActiveDigest(plan.Submission.Workload.Key())
	if err != nil {
		return planEnvelope{}, true, err
	}
	return planEnvelope{plan: plan, canonical: []byte(values[9]), checksum: values[10], bytes: values[11], member: member, idemDigest: idem, workloadDigest: workload, terminalRetain: values[8]}, true, nil
}

func (b *Backend) readIdempotency(ctx context.Context, key string) ([]string, bool, error) {
	redisKey, err := b.keys.Idempotency(key)
	if err != nil {
		return nil, false, err
	}
	return b.hashValues(ctx, redisKey, idemFieldNames)
}
func (b *Backend) readActive(ctx context.Context, key string) ([]string, bool, error) {
	redisKey, err := b.keys.Active(key)
	if err != nil {
		return nil, false, err
	}
	return b.hashValues(ctx, redisKey, activeFieldNames)
}

func (b *Backend) latchAfterCorruption(ctx context.Context, lease core.BackendMutationLease, class string) {
	// Failure to place the latch must not replace the original corruption
	// result.  The script itself performs lease/fence validation and CASes an
	// absent latch, so stale callers cannot DoS a realm.
	epoch, err := b.client.HGet(ctx, b.keys.Meta().String(), "store_epoch").Result()
	if err != nil || epoch == "" {
		return
	}
	digest := codec.Checksum([]byte(class))
	_, _ = b.runScript(ctx, "LatchScalePlanCorruptionV1", []rediskey.RedisKey{b.keys.Meta(), b.keys.Lease(), b.keys.CorruptLatch()}, lease.RepositoryID, lease.Scope.Namespace, lease.Scope.Realm, lease.Owner.ControllerID, lease.Owner.ControllerBootID, lease.Owner.OperationNonce, lease.Fence, epoch, class, digest, b.RepositoryBindingID())
}

func validateScope(scope core.MutationScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return nil
}
func validateOwner(owner core.MutationLeaseOwner) error { return owner.Validate() }
func (b *Backend) scopeRealm(scope core.MutationScope) error {
	if scope.Realm != b.keys.Realm() {
		return fmt.Errorf("%w: lease realm", core.ErrInvalidPlan)
	}
	return nil
}

func (b *Backend) AcquireMutationLease(ctx context.Context, scope core.MutationScope, owner core.MutationLeaseOwner) (core.BackendMutationLease, error) {
	if ctx == nil {
		return core.BackendMutationLease{}, core.ErrInvalidPlan
	}
	if err := validateScope(scope); err != nil {
		return core.BackendMutationLease{}, err
	}
	if err := b.scopeRealm(scope); err != nil {
		return core.BackendMutationLease{}, err
	}
	if err := validateOwner(owner); err != nil {
		return core.BackendMutationLease{}, err
	}
	out, err := b.runScript(ctx, "AcquireMutationLeaseV1", []rediskey.RedisKey{b.keys.Lease(), b.keys.LeaseFence()}, b.RepositoryBindingID(), scope.Namespace, scope.Realm, owner.ControllerID, owner.ControllerBootID, owner.OperationNonce, canonicalInt(b.options.LeaseTTL.Milliseconds()))
	if err != nil {
		return core.BackendMutationLease{}, err
	}
	if e := b.mutationError(out); e != nil {
		if errors.Is(e, ErrLeaseBusy) {
			return core.BackendMutationLease{}, e
		}
		return core.BackendMutationLease{}, e
	}
	if len(out) < 3 || out[0] != "ACQUIRED" {
		return core.BackendMutationLease{}, fmt.Errorf("%w: acquire reply", ErrScriptContract)
	}
	lease := core.BackendMutationLease{RepositoryID: b.RepositoryBindingID(), Scope: scope, Owner: owner, Fence: out[1]}
	lease.ExpiresAtMs, _ = strconv.ParseInt(out[2], 10, 64)
	if err := lease.Validate(); err != nil {
		return core.BackendMutationLease{}, err
	}
	if hookErr := b.maybeUnknown("AcquireMutationLeaseV1"); hookErr != nil {
		if verified, ok := b.readLeaseOwner(ctx, lease); ok {
			return verified, nil
		}
		return core.BackendMutationLease{}, hookErr
	}
	return lease, nil
}

func (b *Backend) readLeaseOwner(ctx context.Context, expected core.BackendMutationLease) (core.BackendMutationLease, bool) {
	values, found, err := b.hashValues(ctx, b.keys.Lease(), []string{"schema", "repository_id", "namespace", "realm", "controller_id", "controller_boot_id", "operation_nonce", "fence", "expires_at_ms"})
	if err != nil || !found || len(values) != 9 {
		return core.BackendMutationLease{}, false
	}
	if values[1] != expected.RepositoryID || values[2] != expected.Scope.Namespace || values[3] != expected.Scope.Realm || values[4] != expected.Owner.ControllerID || values[5] != expected.Owner.ControllerBootID || values[6] != expected.Owner.OperationNonce || values[7] != expected.Fence {
		return core.BackendMutationLease{}, false
	}
	exp, err := strconv.ParseInt(values[8], 10, 64)
	if err != nil {
		return core.BackendMutationLease{}, false
	}
	expected.ExpiresAtMs = exp
	return expected, true
}

func (b *Backend) RenewMutationLease(ctx context.Context, lease core.BackendMutationLease) (core.BackendMutationLease, error) {
	if ctx == nil {
		return core.BackendMutationLease{}, core.ErrInvalidPlan
	}
	if err := lease.Validate(); err != nil {
		return core.BackendMutationLease{}, err
	}
	if err := b.scopeRealm(lease.Scope); err != nil {
		return core.BackendMutationLease{}, err
	}
	out, err := b.runScript(ctx, "RenewMutationLeaseV1", []rediskey.RedisKey{b.keys.Lease()}, lease.RepositoryID, lease.Scope.Namespace, lease.Scope.Realm, lease.Owner.ControllerID, lease.Owner.ControllerBootID, lease.Owner.OperationNonce, lease.Fence, canonicalInt(b.options.LeaseTTL.Milliseconds()))
	if err != nil {
		return core.BackendMutationLease{}, err
	}
	if e := b.mutationError(out); e != nil {
		return core.BackendMutationLease{}, e
	}
	if len(out) < 2 || out[0] != "RENEWED" {
		return core.BackendMutationLease{}, fmt.Errorf("%w: renew reply", ErrScriptContract)
	}
	renewed := lease
	renewed.ExpiresAtMs, _ = strconv.ParseInt(out[1], 10, 64)
	if err := renewed.Validate(); err != nil {
		return core.BackendMutationLease{}, err
	}
	if hookErr := b.maybeUnknown("RenewMutationLeaseV1"); hookErr != nil {
		if verified, ok := b.readLeaseOwner(ctx, lease); ok && verified.ExpiresAtMs > lease.ExpiresAtMs {
			return verified, nil
		}
		return core.BackendMutationLease{}, hookErr
	}
	return renewed, nil
}

func (b *Backend) ReleaseMutationLease(ctx context.Context, lease core.BackendMutationLease) error {
	if ctx == nil {
		return core.ErrInvalidPlan
	}
	if err := lease.Validate(); err != nil {
		return err
	}
	out, err := b.runScript(ctx, "ReleaseMutationLeaseV1", []rediskey.RedisKey{b.keys.Lease()}, lease.RepositoryID, lease.Scope.Namespace, lease.Scope.Realm, lease.Owner.ControllerID, lease.Owner.ControllerBootID, lease.Owner.OperationNonce, lease.Fence)
	if err != nil {
		return err
	}
	if e := b.mutationError(out); e != nil {
		return e
	}
	if len(out) == 0 {
		return fmt.Errorf("%w: release reply", ErrScriptContract)
	}
	if out[0] == "RELEASED" || out[0] == "ALREADY_RELEASED" {
		if hookErr := b.maybeUnknown("ReleaseMutationLeaseV1"); hookErr != nil {
			if _, found := b.readLeaseOwner(ctx, lease); !found {
				return nil
			}
			return hookErr
		}
		return nil
	}
	return fmt.Errorf("%w: release status %s", ErrScriptContract, out[0])
}

func (b *Backend) applyKeys(e planEnvelope) []rediskey.RedisKey {
	idem, _ := b.keys.Idempotency(e.plan.IdempotencyKey)
	active, _ := b.keys.Active(e.plan.Submission.Workload.Key())
	plan, _ := b.keys.Plan(e.plan.PlanID)
	return []rediskey.RedisKey{b.keys.Meta(), b.keys.Lease(), b.keys.CorruptLatch(), idem, active, b.keys.Accounting(), plan, b.keys.Recoverable(), b.keys.Terminal()}
}

func applyFieldValues(e planEnvelope) (canonical, checksum, bytes, revision, status, updated, completed, recovery, terminal string) {
	return string(e.canonical), e.checksum, e.bytes, canonicalUint(e.plan.Revision), string(e.plan.Status), canonicalInt(e.plan.UpdatedAtMs), canonicalInt(e.plan.CompletedAtMs), canonicalInt(e.plan.RecoveryRetainUntilMs), e.terminalRetain
}

func (b *Backend) applyArgs(mode string, lease core.BackendMutationLease, meta metaState, current, candidate planEnvelope, payloadDigest string) []interface{} {
	cc, cs, cb, cr, cstatus, cu, ccm, crec, cterm := applyFieldValues(current)
	pc, ps, pb, pr, pstatus, pu, pcm, prec, pterm := applyFieldValues(candidate)
	if !meta.exists {
		meta.epoch = 0
	}
	rawKey := current.plan.IdempotencyKey
	if mode == "CREATE" {
		rawKey = candidate.plan.IdempotencyKey
	}
	return []interface{}{mode, lease.RepositoryID, lease.Scope.Namespace, lease.Scope.Realm, lease.Owner.ControllerID, lease.Owner.ControllerBootID, lease.Owner.OperationNonce, lease.Fence,
		b.keys.RealmDigest(), canonicalUint(meta.epoch), cc, cs, cb, cr, cstatus, cu, ccm, crec, cterm, pc, ps, pb, pr, pstatus, pu, pcm, prec, pterm,
		candidate.plan.PlanID, candidate.member, candidate.idemDigest, rawKey, candidate.workloadDigest, candidate.plan.Submission.Workload.Key(), payloadDigest,
		canonicalUint(meta.count), canonicalUint(meta.bytes), canonicalUint(meta.active), canonicalUint(meta.recoverable), canonicalUint(meta.recoverableBytes), canonicalUint(meta.terminal), canonicalUint(meta.terminalBytes), canonicalUint(meta.epoch)}
}

type terminalCleanupManifest struct {
	keys  []rediskey.RedisKey
	args  []interface{}
	count int
	bytes uint64
}

func (b *Backend) buildTerminalCleanupManifest(ctx context.Context, meta metaState) (terminalCleanupManifest, error) {
	manifest := terminalCleanupManifest{}
	if !meta.exists || meta.terminal == 0 {
		return manifest, nil
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		return manifest, err
	}
	items, err := b.client.ZRangeWithScores(ctx, b.keys.Terminal().String(), 0, defaultCleanupLimit-1).Result()
	if err != nil {
		return manifest, err
	}
	if len(items) == 0 {
		return manifest, fmt.Errorf("%w: terminal cleanup index empty", core.ErrCorruptRecord)
	}
	manifest.keys = make([]rediskey.RedisKey, 0, len(items)*2)
	manifest.args = make([]interface{}, 0, len(items)*5)
	manifestBytes := 0
	for _, item := range items {
		member, ok := item.Member.(string)
		if !ok {
			if raw, okBytes := item.Member.([]byte); okBytes {
				member = string(raw)
			} else {
				return terminalCleanupManifest{}, fmt.Errorf("%w: terminal member type", core.ErrCorruptRecord)
			}
		}
		if item.Score < 0 || item.Score > float64(core.MaxLuaSafeInteger) || math.Trunc(item.Score) != item.Score {
			return terminalCleanupManifest{}, fmt.Errorf("%w: terminal score", core.ErrCorruptRecord)
		}
		score := int64(item.Score)
		if score > now {
			break
		}
		id, err := b.keys.DecodePlanMember(member)
		if err != nil {
			return terminalCleanupManifest{}, fmt.Errorf("%w: terminal member", core.ErrCorruptRecord)
		}
		raw, found, err := b.readRawPlan(ctx, id)
		if err != nil {
			return terminalCleanupManifest{}, err
		}
		if !found || !raw.plan.Status.Terminal() {
			return terminalCleanupManifest{}, fmt.Errorf("%w: terminal plan reference", core.ErrCorruptRecord)
		}
		retained, err := parseUint(raw.terminalRetain, core.MaxLuaSafeInteger)
		if err != nil || retained != uint64(score) {
			return terminalCleanupManifest{}, fmt.Errorf("%w: terminal retention reference", core.ErrCorruptRecord)
		}
		bytes, err := parseUint(raw.bytes, uint64(b.options.MaxRecordBytes))
		if err != nil || bytes != uint64(len(raw.canonical)) {
			return terminalCleanupManifest{}, fmt.Errorf("%w: terminal accounting reference", core.ErrCorruptRecord)
		}
		planKey, err := b.keys.Plan(id)
		if err != nil {
			return terminalCleanupManifest{}, err
		}
		idemKey, err := b.keys.Idempotency(raw.plan.IdempotencyKey)
		if err != nil {
			return terminalCleanupManifest{}, err
		}
		manifest.keys = append(manifest.keys, planKey, idemKey)
		manifest.args = append(manifest.args, raw.plan.PlanID, raw.idemDigest, raw.plan.IdempotencyKey, raw.bytes, member)
		manifest.count++
		if manifest.bytes > ^uint64(0)-bytes {
			return terminalCleanupManifest{}, ErrBoundExceeded
		}
		manifest.bytes += bytes
		manifestBytes += len(planKey) + len(idemKey) + len(raw.plan.IdempotencyKey) + len(raw.bytes) + len(member)
		if manifestBytes > maxScriptManifestBytes {
			return terminalCleanupManifest{}, ErrBoundExceeded
		}
	}
	return manifest, nil
}

func subtractTerminalCleanup(meta *metaState, manifest terminalCleanupManifest) error {
	count := uint64(manifest.count)
	if count > meta.count || count > meta.terminal || manifest.bytes > meta.bytes || manifest.bytes > meta.terminalBytes {
		return fmt.Errorf("%w: terminal cleanup accounting", core.ErrCorruptRecord)
	}
	meta.count -= count
	meta.bytes -= manifest.bytes
	meta.terminal -= count
	meta.terminalBytes -= manifest.bytes
	return nil
}

func appendApplyTail(args []interface{}, proofExpiry string, manifest terminalCleanupManifest) []interface{} {
	args = append(args, proofExpiry, canonicalUint(uint64(manifest.count)))
	return append(args, manifest.args...)
}

func isCorrupt(err error) bool { return errors.Is(err, core.ErrCorruptRecord) }

func (b *Backend) CreateOrGet(ctx context.Context, lease core.BackendMutationLease, command core.DurableCreateCommand) (result core.CreateResult, err error) {
	submission, plan := command.Submission, command.Plan
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	latched, err := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if err != nil {
		return result, err
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	if err = core.ValidateInitialPlan(plan); err != nil {
		return result, err
	}
	if submission.Key != plan.IdempotencyKey || submission.PayloadDigest != plan.PayloadDigest || submission.WorkloadKey != plan.Submission.Workload.Key() {
		return result, core.ErrInvalidPlan
	}
	if _, err = b.keys.Idempotency(submission.Key); err != nil {
		return result, err
	}
	if _, err = b.keys.Active(submission.WorkloadKey); err != nil {
		return result, err
	}

	meta, err := b.readMeta(ctx)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, err
	}
	iv, found, err := b.readIdempotency(ctx, submission.Key)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, err
	}
	if found {
		if len(iv) != len(idemFieldNames) || iv[0] != "scale-plan-idempotency-v1" || iv[1] != b.keys.Realm() || iv[2] != submissionDigest(b, submission.Key) || iv[3] != submission.Key || iv[5] == "" {
			err = fmt.Errorf("%w: idempotency", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CREATE")
			return result, err
		}
		existing, ok, readErr := b.readRawPlan(ctx, iv[5])
		if readErr != nil {
			if isCorrupt(readErr) {
				b.latchAfterCorruption(ctx, lease, "CREATE")
			}
			return result, readErr
		}
		if !ok {
			err = fmt.Errorf("%w: idempotency dangling", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CREATE")
			return result, err
		}
		if iv[4] == submission.PayloadDigest {
			return core.CreateResult{Status: core.CreateExistingSamePayload, Record: core.PlanRecord{Plan: existing.plan}}, nil
		}
		return core.CreateResult{Status: core.CreateIdempotencyConflict, Record: core.PlanRecord{Plan: existing.plan}}, core.ErrIdempotencyConflict
	}

	active, activeFound, err := b.readActive(ctx, submission.WorkloadKey)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, err
	}
	if activeFound {
		if len(active) != len(activeFieldNames) || active[0] != "scale-plan-active-workload-v1" || active[1] != b.keys.Realm() || active[3] != submission.WorkloadKey || active[4] == "" {
			err = fmt.Errorf("%w: active record", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CREATE")
			return result, err
		}
		existing, ok, readErr := b.readRawPlan(ctx, active[4])
		if readErr != nil {
			if isCorrupt(readErr) {
				b.latchAfterCorruption(ctx, lease, "CREATE")
			}
			return result, readErr
		}
		if !ok || existing.plan.Status.Terminal() || active[5] != canonicalUint(existing.plan.Revision) {
			err = fmt.Errorf("%w: active reference", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CREATE")
			return result, err
		}
		return core.CreateResult{Status: core.CreateWorkloadBusy, Record: core.PlanRecord{Plan: existing.plan}}, core.ErrWorkloadBusy
	}
	// active_count is realm-wide while active records are keyed by workload.
	// Another workload (for example FARM while PLAYER is reconciling) may own
	// the realm's single active slot. ApplyPlanV1 reports WORKLOAD_BUSY for that
	// case. It is ordinary control-plane contention, not evidence that this
	// workload's active reference was lost, so it must never latch corruption.

	candidate, err := makeEnvelope(b, plan)
	if err != nil {
		return result, err
	}
	manifest, err := b.buildTerminalCleanupManifest(ctx, meta)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, err
	}
	next := meta
	if err = subtractTerminalCleanup(&next, manifest); err != nil {
		b.latchAfterCorruption(ctx, lease, "CREATE")
		return result, err
	}
	next.exists = true
	next.count++
	next.bytes += uint64(len(candidate.canonical))
	next.active++
	next.recoverable++
	next.recoverableBytes += uint64(len(candidate.canonical))
	next.epoch++
	if next.count > uint64(b.options.MaxItems) || next.bytes > uint64(b.options.MaxBytes) {
		return core.CreateResult{Status: core.CreateCapacityExceeded}, core.ErrCapacityExceeded
	}
	args := b.applyArgs("CREATE", lease, next, planEnvelope{}, candidate, submission.PayloadDigest)
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	args = appendApplyTail(args, "", manifest)
	out, err := b.runScript(ctx, "ApplyPlanV1", append(b.applyKeys(candidate), manifest.keys...), args...)
	if err != nil {
		return result, err
	}
	if len(out) > 0 && out[0] == "WORKLOAD_BUSY" {
		return core.CreateResult{Status: core.CreateWorkloadBusy}, core.ErrWorkloadBusy
	}
	if len(out) > 0 && out[0] == "CAPACITY_EXCEEDED" {
		return core.CreateResult{Status: core.CreateCapacityExceeded}, core.ErrCapacityExceeded
	}
	if scriptErr := b.mutationError(out); scriptErr != nil {
		if isCorrupt(scriptErr) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, scriptErr
	}
	if len(out) < 2 || out[0] != "CREATED" {
		return result, fmt.Errorf("%w: create status", ErrScriptContract)
	}
	result = core.CreateResult{Status: core.CreateCommitted, Record: core.PlanRecord{Plan: candidate.plan}}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (b *Backend) createOrGetLegacy(ctx context.Context, lease core.BackendMutationLease, command core.DurableCreateCommand) (result core.CreateResult, err error) {
	submission, plan := command.Submission, command.Plan
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	latched, latchErr := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if latchErr != nil {
		return result, latchErr
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	if err = core.ValidateInitialPlan(plan); err != nil {
		return result, err
	}
	if submission.Key != plan.IdempotencyKey || submission.PayloadDigest != plan.PayloadDigest || submission.WorkloadKey != plan.Submission.Workload.Key() {
		return result, core.ErrInvalidPlan
	}
	if _, err = b.keys.Idempotency(submission.Key); err != nil {
		return result, err
	}
	if _, err = b.keys.Active(submission.WorkloadKey); err != nil {
		return result, err
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, err
	}
	if meta.exists && meta.count >= uint64(b.options.MaxItems) {
		return core.CreateResult{Status: core.CreateCapacityExceeded}, core.ErrCapacityExceeded
	}
	// Idempotency is checked before workload busy.  The existing record is the
	// replay answer even when the caller lost the original response.
	iv, found, err := b.readIdempotency(ctx, submission.Key)
	if err != nil {
		return result, err
	}
	if found {
		if len(iv) != 7 || iv[0] != "scale-plan-idempotency-v1" || iv[1] != b.keys.Realm() || iv[2] != submissionDigest(b, submission.Key) || iv[3] != submission.Key || iv[5] == "" {
			err = fmt.Errorf("%w: idempotency", core.ErrCorruptRecord)
			return result, err
		}
		existing, ok, e := b.readRawPlan(ctx, iv[5])
		if e != nil {
			err = e
			return result, err
		}
		if !ok {
			err = fmt.Errorf("%w: idempotency dangling", core.ErrCorruptRecord)
			return result, err
		}
		if iv[4] == submission.PayloadDigest {
			result = core.CreateResult{Status: core.CreateExistingSamePayload, Record: core.PlanRecord{Plan: existing.plan}}
			return result, nil
		}
		result = core.CreateResult{Status: core.CreateIdempotencyConflict, Record: core.PlanRecord{Plan: existing.plan}}
		return result, core.ErrIdempotencyConflict
	}
	active, activeFound, err := b.readActive(ctx, submission.WorkloadKey)
	if err != nil {
		return result, err
	}
	if activeFound {
		if len(active) != 6 || active[0] != "scale-plan-active-workload-v1" || active[1] != b.keys.Realm() || active[3] != submission.WorkloadKey || active[4] == "" {
			err = fmt.Errorf("%w: active record", core.ErrCorruptRecord)
			return result, err
		}
		existing, ok, e := b.readRawPlan(ctx, active[4])
		if e != nil {
			return result, e
		}
		if !ok {
			return result, fmt.Errorf("%w: active dangling", core.ErrCorruptRecord)
		}
		result = core.CreateResult{Status: core.CreateWorkloadBusy, Record: core.PlanRecord{Plan: existing.plan}}
		return result, core.ErrWorkloadBusy
	}
	if meta.exists && meta.active > 0 {
		result = core.CreateResult{Status: core.CreateWorkloadBusy}
		return result, core.ErrWorkloadBusy
	}
	candidate, err := makeEnvelope(b, plan)
	if err != nil {
		return result, err
	}
	next := meta
	next.exists = true
	next.count++
	next.bytes += uint64(len(candidate.canonical))
	next.active++
	next.recoverable++
	next.recoverableBytes += uint64(len(candidate.canonical))
	next.epoch++
	if next.count > uint64(b.options.MaxItems) || next.bytes > uint64(b.options.MaxBytes) {
		return core.CreateResult{Status: core.CreateCapacityExceeded}, core.ErrCapacityExceeded
	}
	if err := b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	args := b.applyArgs("CREATE", lease, next, planEnvelope{}, candidate, submission.PayloadDigest) // expected metadata is the post-state; applyArgs uses it for the write.
	// The script needs the old epoch, while applyArgs above carries the final
	// counters.  Keep the counters and epoch separate in the final positions.
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	out, err := b.runScript(ctx, "ApplyPlanV1", b.applyKeys(candidate), args...)
	if err != nil {
		return result, err
	}
	if e := b.mutationError(out); e != nil {
		if isCorrupt(e) {
			b.latchAfterCorruption(ctx, lease, "CREATE")
		}
		return result, e
	}
	if len(out) < 2 {
		return result, fmt.Errorf("%w: create reply", ErrScriptContract)
	}
	result = core.CreateResult{Status: core.CreateCommitted, Record: core.PlanRecord{Plan: candidate.plan}}
	if out[0] == "WORKLOAD_BUSY" {
		result.Status = core.CreateWorkloadBusy
		return result, core.ErrWorkloadBusy
	}
	if out[0] == "CAPACITY_EXCEEDED" {
		result.Status = core.CreateCapacityExceeded
		return result, core.ErrCapacityExceeded
	}
	if out[0] != "CREATED" {
		return result, fmt.Errorf("%w: create status %s", ErrScriptContract, out[0])
	}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func submissionDigest(b *Backend, key string) string {
	value, err := b.keys.IdempotencyDigest(key)
	if err != nil {
		return ""
	}
	return value
}

func (b *Backend) validateLeaseInput(lease core.BackendMutationLease) error {
	if err := lease.Validate(); err != nil {
		return err
	}
	if lease.RepositoryID != b.RepositoryBindingID() {
		return fmt.Errorf("%w: repository lease", core.ErrInvalidPlan)
	}
	return b.scopeRealm(lease.Scope)
}

func (b *Backend) Load(ctx context.Context, id string) (core.PlanRecord, bool, error) {
	if ctx == nil {
		return core.PlanRecord{}, false, core.ErrInvalidPlan
	}
	if err := validateIdentity(id); err != nil {
		return core.PlanRecord{}, false, err
	}
	raw, found, err := b.readRawPlan(ctx, id)
	if err != nil {
		return core.PlanRecord{}, false, err
	}
	if !found {
		member, _ := b.keys.PlanMember(id)
		r, er := b.client.ZScore(ctx, b.keys.Recoverable().String(), member).Result()
		if er == nil || !errors.Is(er, redis.Nil) {
			if er != nil {
				return core.PlanRecord{}, false, er
			}
			_ = r
			return core.PlanRecord{}, false, fmt.Errorf("%w: dangling recoverable", core.ErrCorruptRecord)
		}
		_, er = b.client.ZScore(ctx, b.keys.Terminal().String(), member).Result()
		if er == nil {
			return core.PlanRecord{}, false, fmt.Errorf("%w: dangling terminal", core.ErrCorruptRecord)
		}
		if !errors.Is(er, redis.Nil) {
			return core.PlanRecord{}, false, er
		}
		exists, er := b.client.HExists(ctx, b.keys.Accounting().String(), member).Result()
		if er != nil {
			return core.PlanRecord{}, false, er
		}
		if exists {
			return core.PlanRecord{}, false, fmt.Errorf("%w: dangling accounting", core.ErrCorruptRecord)
		}
		return core.PlanRecord{}, false, nil
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		return core.PlanRecord{}, false, err
	}
	if !meta.exists {
		return core.PlanRecord{}, false, fmt.Errorf("%w: plan without meta", core.ErrCorruptRecord)
	}
	idemKey, err := b.keys.Idempotency(raw.plan.IdempotencyKey)
	if err != nil {
		return core.PlanRecord{}, false, err
	}
	activeKey, err := b.keys.Active(raw.plan.Submission.Workload.Key())
	if err != nil {
		return core.PlanRecord{}, false, err
	}
	planKey, _ := b.keys.Plan(id)
	out, err := b.runScript(ctx, "LoadScalePlanV1", []rediskey.RedisKey{planKey, b.keys.Recoverable(), b.keys.Terminal(), b.keys.Accounting(), activeKey, idemKey, b.keys.Meta()}, raw.member, b.keys.Realm(), b.keys.RealmDigest(), id, canonicalUint(meta.epoch))
	if err != nil {
		return core.PlanRecord{}, false, err
	}
	if len(out) == 0 {
		return core.PlanRecord{}, false, fmt.Errorf("%w: load reply", ErrScriptContract)
	}
	if out[0] == "NOT_FOUND" {
		return core.PlanRecord{}, false, nil
	}
	if out[0] != "FOUND" {
		if e := b.mutationError(out); e != nil {
			return core.PlanRecord{}, false, e
		}
		return core.PlanRecord{}, false, fmt.Errorf("%w: load status %s", ErrScriptContract, out[0])
	}
	if len(out) < 7 || out[1] != string(raw.canonical) || out[2] != raw.checksum || out[3] != raw.bytes {
		return core.PlanRecord{}, false, fmt.Errorf("%w: load changed envelope", core.ErrRevisionConflict)
	}
	return core.PlanRecord{Plan: raw.plan.Clone()}, true, nil
}

func (b *Backend) CompareAndSwap(ctx context.Context, lease core.BackendMutationLease, command core.DurableCASCommand) (result core.CASResult, err error) {
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	latched, err := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if err != nil {
		return result, err
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	if err = command.Plan.Validate(); err != nil {
		return result, err
	}
	current, found, err := b.readRawPlan(ctx, command.Plan.PlanID)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		return result, err
	}
	if !found {
		return core.CASResult{Status: core.CASRevisionConflict}, core.ErrRevisionConflict
	}
	if current.plan.Revision == command.Plan.Revision && reflect.DeepEqual(current.plan, command.Plan) {
		return core.CASResult{Status: core.CASAlreadyCommitted, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	if command.Expected != current.plan.Revision || command.Plan.Revision != command.Expected+1 {
		return core.CASResult{Status: core.CASRevisionConflict, Record: core.PlanRecord{Plan: current.plan}}, core.ErrRevisionConflict
	}
	if err = core.ValidatePlanUpdate(current.plan, command.Plan); err != nil {
		return core.CASResult{Status: core.CASRevisionConflict, Record: core.PlanRecord{Plan: current.plan}}, err
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		return result, err
	}
	if !meta.exists || meta.active != 1 {
		err = fmt.Errorf("%w: active metadata", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	iv, ok, err := b.readIdempotency(ctx, current.plan.IdempotencyKey)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		return result, err
	}
	expectedIdemDigest, _ := b.keys.IdempotencyDigest(current.plan.IdempotencyKey)
	if !ok || len(iv) != len(idemFieldNames) || iv[0] != "scale-plan-idempotency-v1" || iv[1] != b.keys.Realm() || iv[2] != expectedIdemDigest || iv[3] != current.plan.IdempotencyKey || iv[4] != current.plan.PayloadDigest || iv[5] != current.plan.PlanID {
		err = fmt.Errorf("%w: idempotency reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	workloadKey := current.plan.Submission.Workload.Key()
	av, ok, err := b.readActive(ctx, workloadKey)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		return result, err
	}
	expectedActiveDigest, _ := b.keys.ActiveDigest(workloadKey)
	if !ok || len(av) != len(activeFieldNames) || av[0] != "scale-plan-active-workload-v1" || av[1] != b.keys.Realm() || av[2] != expectedActiveDigest || av[3] != workloadKey || av[4] != current.plan.PlanID || av[5] != canonicalUint(current.plan.Revision) {
		err = fmt.Errorf("%w: active reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	score, err := b.client.ZScore(ctx, b.keys.Recoverable().String(), current.member).Result()
	if err != nil || score != float64(current.plan.UpdatedAtMs) {
		err = fmt.Errorf("%w: recoverable reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	accounted, err := b.client.HGet(ctx, b.keys.Accounting().String(), current.member).Result()
	if err != nil || accounted != current.bytes {
		err = fmt.Errorf("%w: accounting reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	candidate, err := makeEnvelope(b, command.Plan)
	if err != nil {
		return result, err
	}
	manifest, err := b.buildTerminalCleanupManifest(ctx, meta)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		return result, err
	}
	next := meta
	if err = subtractTerminalCleanup(&next, manifest); err != nil {
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	if next.bytes < uint64(len(current.canonical)) {
		err = fmt.Errorf("%w: current byte accounting", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "CAS")
		return result, err
	}
	next.bytes = next.bytes - uint64(len(current.canonical)) + uint64(len(candidate.canonical))
	if isTerminal(candidate.plan.Status) {
		if next.recoverable == 0 || next.recoverableBytes < uint64(len(current.canonical)) {
			err = fmt.Errorf("%w: recoverable accounting", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CAS")
			return result, err
		}
		next.recoverable--
		next.recoverableBytes -= uint64(len(current.canonical))
		next.terminal++
		next.terminalBytes += uint64(len(candidate.canonical))
		if next.active == 0 {
			err = fmt.Errorf("%w: active accounting", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CAS")
			return result, err
		}
		next.active--
	} else {
		if next.recoverableBytes < uint64(len(current.canonical)) {
			err = fmt.Errorf("%w: recoverable byte accounting", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "CAS")
			return result, err
		}
		next.recoverableBytes = next.recoverableBytes - uint64(len(current.canonical)) + uint64(len(candidate.canonical))
	}
	next.epoch++
	if next.bytes > uint64(b.options.MaxBytes) {
		return core.CASResult{Status: core.CASCapacityExceeded, Record: core.PlanRecord{Plan: current.plan}}, core.ErrCapacityExceeded
	}
	args := b.applyArgs("CAS", lease, next, current, candidate, current.plan.PayloadDigest)
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	args = appendApplyTail(args, "", manifest)
	out, err := b.runScript(ctx, "ApplyPlanV1", append(b.applyKeys(candidate), manifest.keys...), args...)
	if err != nil {
		return result, err
	}
	if scriptErr := b.mutationError(out); scriptErr != nil {
		if isCorrupt(scriptErr) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		if errors.Is(scriptErr, core.ErrRevisionConflict) {
			result.Status = core.CASRevisionConflict
			result.Record = core.PlanRecord{Plan: current.plan}
		}
		return result, scriptErr
	}
	if len(out) < 2 || out[0] != "COMMITTED" {
		return result, fmt.Errorf("%w: cas status", ErrScriptContract)
	}
	result = core.CASResult{Status: core.CASCommitted, Record: core.PlanRecord{Plan: candidate.plan}}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (b *Backend) compareAndSwapLegacy(ctx context.Context, lease core.BackendMutationLease, command core.DurableCASCommand) (result core.CASResult, err error) {
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	latched, latchErr := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if latchErr != nil {
		return result, latchErr
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	if err = command.Plan.Validate(); err != nil {
		return result, err
	}
	current, found, err := b.readRawPlan(ctx, command.Plan.PlanID)
	if err != nil {
		return result, err
	}
	if !found {
		return core.CASResult{Status: core.CASRevisionConflict}, core.ErrRevisionConflict
	}
	if current.plan.Revision == command.Plan.Revision && reflect.DeepEqual(current.plan, command.Plan) {
		return core.CASResult{Status: core.CASAlreadyCommitted, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	if err = core.ValidatePlanUpdate(current.plan, command.Plan); err != nil {
		return core.CASResult{Status: core.CASRevisionConflict, Record: core.PlanRecord{Plan: current.plan}}, err
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		return result, err
	}
	if !meta.exists || meta.active != 1 {
		return result, fmt.Errorf("%w: active metadata", core.ErrCorruptRecord)
	}
	iv, ok, err := b.readIdempotency(ctx, current.plan.IdempotencyKey)
	if err != nil {
		return result, err
	}
	if !ok || iv[5] != current.plan.PlanID || iv[4] != current.plan.PayloadDigest {
		return result, fmt.Errorf("%w: idempotency reference", core.ErrCorruptRecord)
	}
	av, ok, err := b.readActive(ctx, current.plan.Submission.Workload.Key())
	if err != nil {
		return result, err
	}
	if !ok || av[4] != current.plan.PlanID || av[5] != canonicalUint(current.plan.Revision) {
		return result, fmt.Errorf("%w: active reference", core.ErrCorruptRecord)
	}
	score, err := b.client.ZScore(ctx, b.keys.Recoverable().String(), current.member).Result()
	if err != nil {
		return result, fmt.Errorf("%w: recoverable index", core.ErrCorruptRecord)
	}
	if score != float64(current.plan.UpdatedAtMs) {
		return result, fmt.Errorf("%w: recoverable score", core.ErrCorruptRecord)
	}
	accounted, err := b.client.HGet(ctx, b.keys.Accounting().String(), current.member).Result()
	if err != nil || accounted != current.bytes {
		return result, fmt.Errorf("%w: accounting reference", core.ErrCorruptRecord)
	}
	candidate, err := makeEnvelope(b, command.Plan)
	if err != nil {
		return result, err
	}
	next := meta
	next.epoch++
	next.bytes = next.bytes - uint64(len(current.canonical)) + uint64(len(candidate.canonical))
	if isTerminal(candidate.plan.Status) {
		next.recoverable--
		next.recoverableBytes -= uint64(len(current.canonical))
		next.terminal++
		next.terminalBytes += uint64(len(candidate.canonical))
		next.active--
	} else {
		next.recoverableBytes = next.recoverableBytes - uint64(len(current.canonical)) + uint64(len(candidate.canonical))
	}
	if next.bytes > uint64(b.options.MaxBytes) {
		return core.CASResult{Status: core.CASCapacityExceeded, Record: core.PlanRecord{Plan: current.plan}}, core.ErrCapacityExceeded
	}
	args := b.applyArgs("CAS", lease, next, current, candidate, current.plan.PayloadDigest)
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	out, err := b.runScript(ctx, "ApplyPlanV1", b.applyKeys(candidate), args...)
	if err != nil {
		return result, err
	}
	if e := b.mutationError(out); e != nil {
		if isCorrupt(e) {
			b.latchAfterCorruption(ctx, lease, "CAS")
		}
		if errors.Is(e, core.ErrRevisionConflict) {
			result.Status = core.CASRevisionConflict
			result.Record = core.PlanRecord{Plan: current.plan}
		}
		return result, e
	}
	if len(out) < 2 {
		return result, fmt.Errorf("%w: cas reply", ErrScriptContract)
	}
	result = core.CASResult{Status: core.CASCommitted, Record: core.PlanRecord{Plan: candidate.plan}}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (b *Backend) ListRecoverable(ctx context.Context, cursor core.RecoverableCursor) (core.RecoverablePage, error) {
	if ctx == nil {
		return core.RecoverablePage{}, core.ErrInvalidPlan
	}
	limit := cursor.Limit
	if limit == 0 {
		limit = b.options.PageSize
	}
	if limit < 1 || limit > maxPageSizeHard {
		return core.RecoverablePage{}, fmt.Errorf("%w: page size", core.ErrInvalidPlan)
	}
	offset := uint64(0)
	epochSet := false
	expectedEpoch := uint64(0)
	if cursor.Token != "" {
		parts := strings.Split(cursor.Token, ":")
		if len(parts) != 2 {
			return core.RecoverablePage{}, fmt.Errorf("%w: cursor", core.ErrInvalidPlan)
		}
		var err error
		expectedEpoch, err = parseUint(parts[0], core.MaxLuaSafeInteger)
		if err != nil {
			return core.RecoverablePage{}, err
		}
		offset, err = parseUint(parts[1], maxItemsHard)
		if err != nil {
			return core.RecoverablePage{}, err
		}
		epochSet = true
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		return core.RecoverablePage{}, err
	}
	if !meta.exists || meta.recoverable == 0 {
		return core.RecoverablePage{Done: true, Next: core.RecoverableCursor{Limit: limit}}, nil
	}
	if epochSet && expectedEpoch != meta.epoch {
		return core.RecoverablePage{}, core.ErrRevisionConflict
	}
	expectedEpoch = meta.epoch
	members, err := b.client.ZRange(ctx, b.keys.Recoverable().String(), int64(offset), int64(offset)+int64(limit)-1).Result()
	if err != nil {
		return core.RecoverablePage{}, err
	}
	keys := []rediskey.RedisKey{b.keys.Meta(), b.keys.Recoverable(), b.keys.Accounting()}
	for _, member := range members {
		id, err := b.keys.DecodePlanMember(member)
		if err != nil {
			return core.RecoverablePage{}, fmt.Errorf("%w: recoverable member", core.ErrCorruptRecord)
		}
		key, err := b.keys.Plan(id)
		if err != nil {
			return core.RecoverablePage{}, err
		}
		keys = append(keys, key)
	}
	out, err := b.runScript(ctx, "ListRecoverableScalePlansV1", keys, b.keys.Realm(), b.keys.RealmDigest(), canonicalUint(offset), canonicalUint(uint64(limit)), canonicalUint(expectedEpoch), canonicalUint(meta.recoverable))
	if err != nil {
		return core.RecoverablePage{}, err
	}
	if len(out) == 0 {
		return core.RecoverablePage{}, fmt.Errorf("%w: list reply", ErrScriptContract)
	}
	if out[0] != "PAGE" {
		if e := b.mutationError(out); e != nil {
			return core.RecoverablePage{}, e
		}
		return core.RecoverablePage{}, fmt.Errorf("%w: list status %s", ErrScriptContract, out[0])
	}
	if len(out) < 4 {
		return core.RecoverablePage{}, fmt.Errorf("%w: short list", ErrScriptContract)
	}
	pageCount, err := strconv.Atoi(out[len(out)-1])
	if err != nil || pageCount < 0 {
		return core.RecoverablePage{}, fmt.Errorf("%w: list count", ErrScriptContract)
	}
	records := make([]core.PlanRecord, 0, pageCount)
	pos := 3
	for i := 0; i < pageCount; i++ {
		if pos+4 >= len(out) {
			return core.RecoverablePage{}, fmt.Errorf("%w: list record", ErrScriptContract)
		}
		member, err := b.keys.DecodePlanMember(out[pos])
		if err != nil {
			return core.RecoverablePage{}, fmt.Errorf("%w: list member", core.ErrCorruptRecord)
		}
		canonical, checksum, bytesText := out[pos+1], out[pos+2], out[pos+3]
		plan, err := codec.DecodePlan([]byte(canonical))
		if err != nil || plan.PlanID != member || !codec.VerifyChecksum([]byte(canonical), checksum) || bytesText != canonicalUint(uint64(len(canonical))) {
			return core.RecoverablePage{}, fmt.Errorf("%w: list record validation", core.ErrCorruptRecord)
		}
		records = append(records, core.PlanRecord{Plan: plan})
		pos += 5
	}
	done := offset+uint64(pageCount) >= meta.recoverable
	next := core.RecoverableCursor{Limit: limit}
	if !done {
		next.Token = canonicalUint(expectedEpoch) + ":" + canonicalUint(offset+uint64(pageCount))
	}
	return core.RecoverablePage{Records: records, Next: next, Done: done}, nil
}

// CleanupTerminal removes only terminal records whose retention score has
// elapsed.  The manifest is explicit and bounded; the Lua script validates
// every plan, idempotency record, index member, and accounting byte count
// before its first DEL/ZREM/HDEL.
func (b *Backend) CleanupTerminal(ctx context.Context, lease core.BackendMutationLease, limit int) (result TerminalCleanupResult, err error) {
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	if limit == 0 {
		limit = defaultCleanupLimit
	}
	if limit < 1 || limit > maxCleanupLimitHard {
		return result, fmt.Errorf("%w: terminal cleanup limit", core.ErrInvalidPlan)
	}
	latched, err := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if err != nil {
		return result, err
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
		}
		return result, err
	}
	if !meta.exists || meta.terminal == 0 {
		return result, nil
	}
	now, err := b.redisNow(ctx)
	if err != nil {
		return result, err
	}
	items, err := b.client.ZRangeWithScores(ctx, b.keys.Terminal().String(), 0, int64(limit)-1).Result()
	if err != nil {
		return result, err
	}
	if len(items) == 0 {
		return result, fmt.Errorf("%w: terminal index empty with metadata", core.ErrCorruptRecord)
	}

	keys := []rediskey.RedisKey{b.keys.Meta(), b.keys.Lease(), b.keys.CorruptLatch(), b.keys.Terminal(), b.keys.Accounting()}
	args := []interface{}{lease.RepositoryID, lease.Scope.Namespace, lease.Scope.Realm, lease.Owner.ControllerID, lease.Owner.ControllerBootID, lease.Owner.OperationNonce, lease.Fence, canonicalUint(uint64(limit)), b.keys.RealmDigest(), canonicalUint(meta.epoch)}
	manifestBytes := 0
	for _, item := range items {
		member, ok := item.Member.(string)
		if !ok {
			if raw, okBytes := item.Member.([]byte); okBytes {
				member = string(raw)
			} else {
				err = fmt.Errorf("%w: terminal member type", core.ErrCorruptRecord)
				b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
				return result, err
			}
		}
		if item.Score > float64(now) {
			break
		}
		id, decodeErr := b.keys.DecodePlanMember(member)
		if decodeErr != nil {
			err = fmt.Errorf("%w: terminal member", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
			return result, err
		}
		raw, found, readErr := b.readRawPlan(ctx, id)
		if readErr != nil {
			if isCorrupt(readErr) {
				b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
			}
			return result, readErr
		}
		if !found || !raw.plan.Status.Terminal() {
			err = fmt.Errorf("%w: terminal plan reference", core.ErrCorruptRecord)
			b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
			return result, err
		}
		planKey, keyErr := b.keys.Plan(id)
		if keyErr != nil {
			return result, keyErr
		}
		idemKey, keyErr := b.keys.Idempotency(raw.plan.IdempotencyKey)
		if keyErr != nil {
			return result, keyErr
		}
		keys = append(keys, planKey, idemKey)
		args = append(args, raw.plan.PlanID, raw.idemDigest, raw.plan.IdempotencyKey, raw.bytes, member)
		manifestBytes += len(planKey) + len(idemKey) + len(raw.plan.IdempotencyKey) + len(raw.bytes) + len(member)
		if manifestBytes > maxScriptManifestBytes {
			return result, ErrBoundExceeded
		}
	}
	if len(keys) == 5 {
		return result, nil
	}

	out, runErr := b.runScript(ctx, "CleanupTerminalScalePlansV1", keys, args...)
	if runErr != nil {
		return result, runErr
	}
	if scriptErr := b.mutationError(out); scriptErr != nil {
		if isCorrupt(scriptErr) {
			b.latchAfterCorruption(ctx, lease, "TERMINAL_CLEANUP")
		}
		return result, scriptErr
	}
	if len(out) < 2 || out[0] != "CLEANED" {
		return result, fmt.Errorf("%w: cleanup reply", ErrScriptContract)
	}
	cleaned, parseErr := strconv.Atoi(out[1])
	if parseErr != nil || cleaned < 0 || cleaned > len(items) {
		return result, fmt.Errorf("%w: cleanup count", ErrScriptContract)
	}
	result.Cleaned = cleaned
	result.MoreDue = len(out) > 2 && out[2] == "MORE_DUE"
	if hookErr := b.maybeUnknown("CleanupTerminalScalePlansV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (b *Backend) redisNow(ctx context.Context) (int64, error) {
	value, err := b.client.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return value.UnixMilli(), nil
}

func (b *Backend) ApplySealedBlockedTermination(ctx context.Context, lease core.BackendMutationLease, sealed core.SealedBlockedTerminationMutation) (result core.BlockedTerminationResult, err error) {
	// Reject an unsealed or differently bound capability before even checking
	// the context or issuing a Redis command.
	if !sealed.Authorized() || !sealed.BoundTo(lease) {
		return result, core.ErrVerification
	}
	if ctx == nil {
		return result, core.ErrInvalidPlan
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	latched, err := b.client.Exists(ctx, b.keys.CorruptLatch().String()).Result()
	if err != nil {
		return result, err
	}
	if latched != 0 {
		return result, fmt.Errorf("%w: realm corruption latch", core.ErrCorruptRecord)
	}
	request := sealed.Request()
	proof := sealed.Proof()
	if request.PlanID == "" {
		return result, core.ErrInvalidPlan
	}
	current, found, err := b.readRawPlan(ctx, request.PlanID)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		return result, err
	}
	if !found {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationRevisionConflict}, core.ErrPlanNotFound
	}
	if current.plan.Status == core.StatusFailed && current.plan.BlockedTermination != nil {
		receipt := current.plan.BlockedTermination
		if receipt.TerminationID == request.TerminationID && receipt.ExpectedBlockedRevision == request.ExpectedRevision && receipt.ExpectedBlockedReason == request.ExpectedBlockedReason && receipt.ExpectedPayloadDigest == request.ExpectedPayloadDigest && receipt.ExpectedCheckpointDigest == request.ExpectedCheckpointDigest {
			return core.BlockedTerminationResult{Status: core.BlockedTerminationAlreadyCommitted, Record: core.PlanRecord{Plan: current.plan}}, nil
		}
		return core.BlockedTerminationResult{Status: core.BlockedTerminationRevisionConflict, Record: core.PlanRecord{Plan: current.plan}}, core.ErrRevisionConflict
	}
	if current.plan.Status != core.StatusBlocked {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationNotBlocked, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	checkpoint, err := core.BlockedCheckpointDigest(current.plan)
	if err != nil {
		return result, err
	}
	proofDigest, err := core.BlockedQuiescenceProofDigest(proof)
	if err != nil {
		return result, err
	}
	if request.ExpectedRevision != current.plan.Revision || request.ExpectedBlockedReason != current.plan.BlockedReason || request.ExpectedPayloadDigest != current.plan.PayloadDigest || request.ExpectedCheckpointDigest != checkpoint || proof.ExpectedCheckpointDigest != checkpoint || proofDigest != sealed.EvidenceDigest() {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationExpectationMismatch, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	commitMs, err := b.redisNow(ctx)
	if err != nil {
		return result, err
	}
	if commitMs >= proof.ExpiresAtMs {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationProofExpired, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	candidate := current.plan.Clone()
	candidate.Status = core.StatusFailed
	candidate.BlockedReason = ""
	candidate.PausedReason = ""
	candidate.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	candidate.Revision++
	candidate.UpdatedAtMs = commitMs
	candidate.CompletedAtMs = commitMs
	candidate.BlockedTermination = &core.BlockedTerminationReceipt{ProtocolVersion: core.BlockedTerminationReceiptVersion, TerminationID: request.TerminationID, ExpectedBlockedRevision: request.ExpectedRevision, ExpectedBlockedReason: request.ExpectedBlockedReason, ExpectedBlockedUpdatedAtMs: current.plan.UpdatedAtMs, ExpectedPayloadDigest: request.ExpectedPayloadDigest, ExpectedCheckpointDigest: request.ExpectedCheckpointDigest, ProofDigest: proofDigest, ProvedAtMs: proof.CapturedAtMs, ProofExpiresAtMs: proof.ExpiresAtMs, EvidenceRetainUntilMs: proof.EvidenceRetainUntilMs, CommittedAtMs: commitMs, IntentCount: proof.IntentCount, ChildCount: proof.ChildCount, ResultCode: "OPERATOR_SAFE_TERMINATION"}
	proofCopy := proof
	candidate.BlockedTerminationProof = &proofCopy
	if err = core.ValidateBlockedTerminationUpdate(current.plan, candidate, request, proof, *candidate.BlockedTermination); err != nil {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationUnsafe, Record: core.PlanRecord{Plan: current.plan}}, err
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		return result, err
	}
	if !meta.exists || meta.active != 1 {
		err = fmt.Errorf("%w: active metadata", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	iv, ok, err := b.readIdempotency(ctx, current.plan.IdempotencyKey)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		return result, err
	}
	expectedIdemDigest, _ := b.keys.IdempotencyDigest(current.plan.IdempotencyKey)
	if !ok || len(iv) != len(idemFieldNames) || iv[0] != "scale-plan-idempotency-v1" || iv[1] != b.keys.Realm() || iv[2] != expectedIdemDigest || iv[3] != current.plan.IdempotencyKey || iv[4] != current.plan.PayloadDigest || iv[5] != current.plan.PlanID {
		err = fmt.Errorf("%w: idempotency reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	workloadKey := current.plan.Submission.Workload.Key()
	av, ok, err := b.readActive(ctx, workloadKey)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		return result, err
	}
	expectedActiveDigest, _ := b.keys.ActiveDigest(workloadKey)
	if !ok || len(av) != len(activeFieldNames) || av[0] != "scale-plan-active-workload-v1" || av[1] != b.keys.Realm() || av[2] != expectedActiveDigest || av[3] != workloadKey || av[4] != current.plan.PlanID || av[5] != canonicalUint(current.plan.Revision) {
		err = fmt.Errorf("%w: active reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	score, err := b.client.ZScore(ctx, b.keys.Recoverable().String(), current.member).Result()
	if err != nil || score != float64(current.plan.UpdatedAtMs) {
		err = fmt.Errorf("%w: recoverable reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	accounted, err := b.client.HGet(ctx, b.keys.Accounting().String(), current.member).Result()
	if err != nil || accounted != current.bytes {
		err = fmt.Errorf("%w: accounting reference", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	candidateEnv, err := makeEnvelope(b, candidate)
	if err != nil {
		return result, err
	}
	manifest, err := b.buildTerminalCleanupManifest(ctx, meta)
	if err != nil {
		if isCorrupt(err) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		return result, err
	}
	next := meta
	if err = subtractTerminalCleanup(&next, manifest); err != nil {
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	if next.bytes < uint64(len(current.canonical)) || next.recoverable == 0 || next.recoverableBytes < uint64(len(current.canonical)) || next.active == 0 {
		err = fmt.Errorf("%w: blocked terminal accounting", core.ErrCorruptRecord)
		b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		return result, err
	}
	next.bytes = next.bytes - uint64(len(current.canonical)) + uint64(len(candidateEnv.canonical))
	next.recoverable--
	next.recoverableBytes -= uint64(len(current.canonical))
	next.terminal++
	next.terminalBytes += uint64(len(candidateEnv.canonical))
	next.active--
	next.epoch++
	if next.bytes > uint64(b.options.MaxBytes) {
		return result, core.ErrCapacityExceeded
	}
	args := b.applyArgs("BLOCKED", lease, next, current, candidateEnv, current.plan.PayloadDigest)
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	args = appendApplyTail(args, canonicalInt(proof.ExpiresAtMs), manifest)
	out, err := b.runScript(ctx, "ApplyPlanV1", append(b.applyKeys(candidateEnv), manifest.keys...), args...)
	if err != nil {
		return result, err
	}
	if scriptErr := b.mutationError(out); scriptErr != nil {
		if isCorrupt(scriptErr) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		if errors.Is(scriptErr, core.ErrVerification) {
			result.Status = core.BlockedTerminationProofExpired
		}
		return result, scriptErr
	}
	if len(out) < 2 || out[0] != "COMMITTED" {
		return result, fmt.Errorf("%w: blocked terminal status", ErrScriptContract)
	}
	result = core.BlockedTerminationResult{Status: core.BlockedTerminationCommitted, Record: core.PlanRecord{Plan: candidate}}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (b *Backend) applySealedBlockedTerminationLegacy(ctx context.Context, lease core.BackendMutationLease, sealed core.SealedBlockedTerminationMutation) (result core.BlockedTerminationResult, err error) {
	// These checks must remain before validateLeaseInput or any Redis command;
	// a backend must not turn a forged opaque value into a store probe.
	if !sealed.Authorized() || !sealed.BoundTo(lease) {
		return result, core.ErrVerification
	}
	if err = b.validateLeaseInput(lease); err != nil {
		return result, err
	}
	request := sealed.Request()
	proof := sealed.Proof()
	if request.PlanID == "" {
		return result, core.ErrInvalidPlan
	}
	current, found, err := b.readRawPlan(ctx, request.PlanID)
	if err != nil {
		return result, err
	}
	if !found {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationRevisionConflict}, core.ErrPlanNotFound
	}
	if current.plan.Status == core.StatusFailed && current.plan.BlockedTermination != nil {
		r := current.plan.BlockedTermination
		if r.TerminationID == request.TerminationID && r.ExpectedBlockedRevision == request.ExpectedRevision && r.ExpectedBlockedReason == request.ExpectedBlockedReason && r.ExpectedPayloadDigest == request.ExpectedPayloadDigest && r.ExpectedCheckpointDigest == request.ExpectedCheckpointDigest {
			return core.BlockedTerminationResult{Status: core.BlockedTerminationAlreadyCommitted, Record: core.PlanRecord{Plan: current.plan}}, nil
		}
		return core.BlockedTerminationResult{Status: core.BlockedTerminationRevisionConflict, Record: core.PlanRecord{Plan: current.plan}}, core.ErrRevisionConflict
	}
	if current.plan.Status != core.StatusBlocked {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationNotBlocked, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	checkpoint, err := core.BlockedCheckpointDigest(current.plan)
	if err != nil {
		return result, err
	}
	proofDigest, err := core.BlockedQuiescenceProofDigest(proof)
	if err != nil {
		return result, err
	}
	if request.ExpectedRevision != current.plan.Revision || request.ExpectedBlockedReason != current.plan.BlockedReason || request.ExpectedPayloadDigest != current.plan.PayloadDigest || request.ExpectedCheckpointDigest != checkpoint || proof.ExpectedCheckpointDigest != checkpoint || proofDigest != sealed.EvidenceDigest() {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationExpectationMismatch, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	commitMs, err := b.redisNow(ctx)
	if err != nil {
		return result, err
	}
	if commitMs >= proof.ExpiresAtMs {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationProofExpired, Record: core.PlanRecord{Plan: current.plan}}, nil
	}
	candidate := current.plan.Clone()
	candidate.Status = core.StatusFailed
	candidate.BlockedReason = ""
	candidate.PausedReason = ""
	candidate.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	candidate.Revision++
	candidate.UpdatedAtMs = commitMs
	candidate.CompletedAtMs = commitMs
	candidate.BlockedTermination = &core.BlockedTerminationReceipt{ProtocolVersion: core.BlockedTerminationReceiptVersion, TerminationID: request.TerminationID, ExpectedBlockedRevision: request.ExpectedRevision, ExpectedBlockedReason: request.ExpectedBlockedReason, ExpectedBlockedUpdatedAtMs: current.plan.UpdatedAtMs, ExpectedPayloadDigest: request.ExpectedPayloadDigest, ExpectedCheckpointDigest: request.ExpectedCheckpointDigest, ProofDigest: proofDigest, ProvedAtMs: proof.CapturedAtMs, ProofExpiresAtMs: proof.ExpiresAtMs, EvidenceRetainUntilMs: proof.EvidenceRetainUntilMs, CommittedAtMs: commitMs, IntentCount: proof.IntentCount, ChildCount: proof.ChildCount, ResultCode: "OPERATOR_SAFE_TERMINATION"}
	proofCopy := proof
	candidate.BlockedTerminationProof = &proofCopy
	if err = core.ValidateBlockedTerminationUpdate(current.plan, candidate, request, proof, *candidate.BlockedTermination); err != nil {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationUnsafe, Record: core.PlanRecord{Plan: current.plan}}, err
	}
	meta, err := b.readMeta(ctx)
	if err != nil {
		return result, err
	}
	if !meta.exists || meta.active != 1 {
		return result, fmt.Errorf("%w: active metadata", core.ErrCorruptRecord)
	}
	iv, ok, err := b.readIdempotency(ctx, current.plan.IdempotencyKey)
	if err != nil {
		return result, err
	}
	if !ok || iv[5] != current.plan.PlanID {
		return result, fmt.Errorf("%w: idempotency reference", core.ErrCorruptRecord)
	}
	av, ok, err := b.readActive(ctx, current.plan.Submission.Workload.Key())
	if err != nil {
		return result, err
	}
	if !ok || av[4] != current.plan.PlanID {
		return result, fmt.Errorf("%w: active reference", core.ErrCorruptRecord)
	}
	score, err := b.client.ZScore(ctx, b.keys.Recoverable().String(), current.member).Result()
	if err != nil || score != float64(current.plan.UpdatedAtMs) {
		return result, fmt.Errorf("%w: recoverable reference", core.ErrCorruptRecord)
	}
	candidateEnv, err := makeEnvelope(b, candidate)
	if err != nil {
		return result, err
	}
	next := meta
	next.epoch++
	next.bytes = next.bytes - uint64(len(current.canonical)) + uint64(len(candidateEnv.canonical))
	next.recoverable--
	next.recoverableBytes -= uint64(len(current.canonical))
	next.terminal++
	next.terminalBytes += uint64(len(candidateEnv.canonical))
	next.active--
	args := b.applyArgs("BLOCKED", lease, next, current, candidateEnv, current.plan.PayloadDigest)
	args[9] = canonicalUint(meta.epoch)
	args[35] = canonicalUint(next.count)
	args[36] = canonicalUint(next.bytes)
	args[37] = canonicalUint(next.active)
	args[38] = canonicalUint(next.recoverable)
	args[39] = canonicalUint(next.recoverableBytes)
	args[40] = canonicalUint(next.terminal)
	args[41] = canonicalUint(next.terminalBytes)
	args[42] = canonicalUint(next.epoch)
	args = append(args, canonicalInt(proof.ExpiresAtMs))
	out, err := b.runScript(ctx, "ApplyPlanV1", b.applyKeys(candidateEnv), args...)
	if err != nil {
		return result, err
	}
	if e := b.mutationError(out); e != nil {
		if isCorrupt(e) {
			b.latchAfterCorruption(ctx, lease, "BLOCKED_TERMINATION")
		}
		if errors.Is(e, core.ErrVerification) {
			result.Status = core.BlockedTerminationProofExpired
		}
		return result, e
	}
	result = core.BlockedTerminationResult{Status: core.BlockedTerminationCommitted, Record: core.PlanRecord{Plan: candidate}}
	if hookErr := b.maybeUnknown("ApplyPlanV1"); hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

var _ core.DurableRepositoryBackend = (*Backend)(nil)
