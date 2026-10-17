package routecontrol

import (
	"context"
	"crypto/sha1" //nolint:gosec // Persisted corruption checksum, not authentication.
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/rediskey"
)

const (
	defaultHolderStoreOperationTimeout = 2 * time.Second
	defaultHolderStoreMaxRecordBytes   = 16 << 10
	defaultHolderStoreMaxResourceBytes = 4 << 10
	defaultHolderStoreRecordTTL        = 3 * time.Minute
	holderKeyLayoutVersion             = "v2"
	holderReadMaxAttempts              = 16
	maxLuaSafeInteger                  = int64(1<<53 - 1)
	maxLuaSafeUint                     = uint64(1<<53 - 1)
	maxRegistryRevision                = "18446744073709551615"
	holderRegistryTTLMarginMillis      = int64(1)
)

var (
	ErrHolderStoreInvalid       = core.ErrHolderRegistryInvalid
	ErrHolderStoreCorrupt       = core.ErrHolderRegistryCorrupt
	ErrHolderStoreBootConflict  = core.ErrHolderBootConflict
	ErrHolderStoreLeaseExpired  = core.ErrHolderLeaseExpired
	ErrHolderStoreReplay        = core.ErrHolderHeartbeatReplay
	ErrHolderStoreStale         = core.ErrHolderHeartbeatStale
	ErrHolderStoreCapacity      = core.ErrHolderRegistryCapacity
	ErrHolderStoreUnavailable   = core.ErrHolderRegistryUnavailable
	ErrHolderStoreTargetNoMatch = core.ErrHolderTargetUnavailable
)

// HolderStoreOptions bounds the Redis holder registry. MaxRecordBytes is a
// per-record limit; MaxTotalRecordBytes is the bounded aggregate stored in the
// meta hash. Logical LeaseUntil remains authoritative over physical TTL.
type HolderStoreOptions struct {
	Namespace rediskey.Namespace

	OperationTimeout      time.Duration
	MaxHolders            int
	MaxRecordBytes        int
	MaxTotalRecordBytes   int
	MaxResourceBytes      int
	MaxTotalResourceBytes int
	RecordTTL             time.Duration

	LeaseTTL    time.Duration
	MinLeaseTTL time.Duration
	MaxLeaseTTL time.Duration
	ClockSkew   time.Duration
	Now         func() time.Time
}

type HolderOptions = HolderStoreOptions
type HolderStoreConfig = HolderStoreOptions

func DefaultHolderStoreOptions(namespace rediskey.Namespace) HolderStoreOptions {
	registry := core.DefaultHolderRegistryOptions()
	return HolderStoreOptions{
		Namespace:             namespace,
		OperationTimeout:      defaultHolderStoreOperationTimeout,
		MaxHolders:            registry.MaxHolderCount,
		MaxRecordBytes:        defaultHolderStoreMaxRecordBytes,
		MaxTotalRecordBytes:   defaultHolderStoreMaxRecordBytes * registry.MaxHolderCount,
		MaxResourceBytes:      defaultHolderStoreMaxResourceBytes,
		MaxTotalResourceBytes: defaultHolderStoreMaxResourceBytes * registry.MaxHolderCount,
		RecordTTL:             defaultHolderStoreRecordTTL,
		LeaseTTL:              registry.LeaseTTL,
		MinLeaseTTL:           registry.MinLeaseTTL,
		MaxLeaseTTL:           registry.MaxLeaseTTL,
		ClockSkew:             registry.ClockSkew,
		Now:                   registry.Now,
	}
}

func (o HolderStoreOptions) normalized() (HolderStoreOptions, error) {
	if err := o.Namespace.Validate(); err != nil {
		return HolderStoreOptions{}, ErrInvalidOptions
	}
	registry := core.DefaultHolderRegistryOptions()
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = defaultHolderStoreOperationTimeout
	}
	if o.MaxHolders <= 0 {
		o.MaxHolders = registry.MaxHolderCount
	}
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = defaultHolderStoreMaxRecordBytes
	}
	if o.MaxTotalRecordBytes <= 0 {
		if o.MaxRecordBytes > int(^uint(0)>>1)/o.MaxHolders {
			return HolderStoreOptions{}, ErrInvalidOptions
		}
		o.MaxTotalRecordBytes = o.MaxRecordBytes * o.MaxHolders
	}
	if o.MaxResourceBytes <= 0 {
		o.MaxResourceBytes = defaultHolderStoreMaxResourceBytes
	}
	if o.MaxTotalResourceBytes <= 0 {
		if o.MaxResourceBytes > int(^uint(0)>>1)/o.MaxHolders {
			return HolderStoreOptions{}, ErrInvalidOptions
		}
		o.MaxTotalResourceBytes = o.MaxResourceBytes * o.MaxHolders
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = registry.LeaseTTL
	}
	if o.MinLeaseTTL <= 0 {
		o.MinLeaseTTL = registry.MinLeaseTTL
	}
	if o.MaxLeaseTTL <= 0 {
		o.MaxLeaseTTL = registry.MaxLeaseTTL
	}
	if o.ClockSkew < 0 {
		return HolderStoreOptions{}, ErrInvalidOptions
	}
	if o.ClockSkew == 0 {
		o.ClockSkew = registry.ClockSkew
	}
	if o.RecordTTL <= 0 {
		o.RecordTTL = defaultHolderStoreRecordTTL
	}
	durationValues := map[string]time.Duration{
		"operation timeout": o.OperationTimeout,
		"record ttl":        o.RecordTTL,
		"lease ttl":         o.LeaseTTL,
		"minimum lease ttl": o.MinLeaseTTL,
		"maximum lease ttl": o.MaxLeaseTTL,
		"clock skew":        o.ClockSkew,
	}
	durationMillis := make(map[string]int64, len(durationValues))
	for name, value := range durationValues {
		millis, err := holderDurationMillis(name, value)
		if err != nil {
			return HolderStoreOptions{}, err
		}
		durationMillis[name] = millis
	}
	maxLeaseMillis := durationMillis["maximum lease ttl"]
	skewMillis := durationMillis["clock skew"]
	recordTTLMillis := durationMillis["record ttl"]
	if maxLeaseMillis > maxLuaSafeInteger-skewMillis-holderRegistryTTLMarginMillis ||
		recordTTLMillis <= maxLeaseMillis+skewMillis ||
		recordTTLMillis > maxLuaSafeInteger-holderRegistryTTLMarginMillis {
		return HolderStoreOptions{}, fmt.Errorf(
			"%w: record TTL must safely exceed max lease plus skew and leave registry TTL margin",
			ErrInvalidOptions,
		)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxHolders <= 0 || o.MaxRecordBytes <= 0 || o.MaxTotalRecordBytes <= 0 ||
		o.MaxResourceBytes <= 0 || o.MaxTotalResourceBytes <= 0 ||
		o.MinLeaseTTL <= 0 || o.LeaseTTL < o.MinLeaseTTL ||
		o.LeaseTTL > o.MaxLeaseTTL {
		return HolderStoreOptions{}, ErrInvalidOptions
	}
	for name, value := range map[string]int{
		"max holders": o.MaxHolders, "max record bytes": o.MaxRecordBytes,
		"max total record bytes": o.MaxTotalRecordBytes, "max resource bytes": o.MaxResourceBytes,
		"max total resource bytes": o.MaxTotalResourceBytes,
	} {
		if int64(value) <= 0 || int64(value) > maxLuaSafeInteger {
			return HolderStoreOptions{}, fmt.Errorf("%w: %s exceeds Lua safe integer", ErrInvalidOptions, name)
		}
	}
	return o, nil
}

// HolderKeyBuilder keeps the registry records and bounded indexes in one
// byte-identical Redis hash-tag.
type HolderKeyBuilder struct {
	tag string
}

func NewHolderKeyBuilder(namespace rediskey.Namespace) (HolderKeyBuilder, error) {
	base, err := NewKeyBuilderV1(namespace)
	if err != nil {
		return HolderKeyBuilder{}, err
	}
	return HolderKeyBuilder{tag: base.HashTag() + ":holders:" + holderKeyLayoutVersion}, nil
}

func (b HolderKeyBuilder) HashTag() string {
	return b.tag
}

func (b HolderKeyBuilder) key(suffix string) rediskey.RedisKey {
	return rediskey.RedisKey("{" + b.tag + "}:" + suffix)
}

func (b HolderKeyBuilder) Meta() rediskey.RedisKey {
	return b.key("meta")
}

func (b HolderKeyBuilder) Index() rediskey.RedisKey {
	return b.key("index")
}

func (b HolderKeyBuilder) Accounting() rediskey.RedisKey {
	return b.key("accounting")
}

func (b HolderKeyBuilder) PlacementRecord(domain actorrouting.RouteDomain, holderID string) (rediskey.RedisKey, error) {
	if err := validateHolderKeyComponent(string(domain), 16); err != nil {
		return "", err
	}
	if err := validateHolderKeyComponent(holderID, core.DefaultHolderMaxIDBytes); err != nil {
		return "", err
	}
	return b.key("placement:" + hex.EncodeToString([]byte(domain)) + ":" + hex.EncodeToString([]byte(holderID))), nil
}

func (b HolderKeyBuilder) Record(domain actorrouting.RouteDomain, holderID string) (rediskey.RedisKey, error) {
	if err := validateHolderKeyComponent(string(domain), 16); err != nil {
		return "", err
	}
	if err := validateHolderKeyComponent(holderID, core.DefaultHolderMaxIDBytes); err != nil {
		return "", err
	}
	return b.key("record:" + hex.EncodeToString([]byte(domain)) + ":" + hex.EncodeToString([]byte(holderID))), nil
}

func (b HolderKeyBuilder) Member(domain actorrouting.RouteDomain, holderID string) (string, error) {
	if err := validateHolderKeyComponent(string(domain), 16); err != nil {
		return "", err
	}
	if err := validateHolderKeyComponent(holderID, core.DefaultHolderMaxIDBytes); err != nil {
		return "", err
	}
	return string(domain) + "\x00" + holderID, nil
}

func (b HolderKeyBuilder) AssertSameSlot(keys ...rediskey.RedisKey) error {
	return rediskey.AssertSameSlot(keys...)
}

// HolderStore is the Redis durable holder registry adapter.
type HolderStore struct {
	client  redis.UniversalClient
	scripts *holderScriptRegistry
	keys    HolderKeyBuilder
	opts    HolderStoreOptions

	readAttemptHook func(int, holderReadStage)
	metaReadHook    func(holderMetaReadStage)
}

type holderReadStage string

const holderReadAfterMeta holderReadStage = "after_meta"

type holderMetaReadStage string

const holderMetaReadAfterRevision holderMetaReadStage = "after_revision"

type RedisHolderStore = HolderStore

var _ core.HolderRegistryStore = (*HolderStore)(nil)
var _ core.PlacementExclusionStore = (*HolderStore)(nil)

func NewHolderStore(client redis.UniversalClient, opts HolderStoreOptions) (*HolderStore, error) {
	if err := validateHolderRedisClient(client); err != nil {
		return nil, err
	}
	normalized, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	keys, err := NewHolderKeyBuilder(normalized.Namespace)
	if err != nil {
		return nil, err
	}
	scripts, err := newHolderScriptRegistry(client)
	if err != nil {
		return nil, err
	}
	return &HolderStore{client: client, scripts: scripts, keys: keys, opts: normalized}, nil
}

func NewRedisHolderStore(client redis.UniversalClient, opts HolderStoreOptions) (*HolderStore, error) {
	return NewHolderStore(client, opts)
}

func NewHolderStoreWithRegistry(client redis.UniversalClient, scripts *holderScriptRegistry, opts HolderStoreOptions) (*HolderStore, error) {
	if err := validateHolderRedisClient(client); err != nil {
		return nil, err
	}
	normalized, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	if scripts == nil {
		return nil, ErrInvalidOptions
	}
	keys, err := NewHolderKeyBuilder(normalized.Namespace)
	if err != nil {
		return nil, err
	}
	return &HolderStore{client: client, scripts: scripts, keys: keys, opts: normalized}, nil
}

func (s *HolderStore) Client() redis.UniversalClient {
	if s == nil {
		return nil
	}
	return s.client
}

func (s *HolderStore) Keys() HolderKeyBuilder {
	if s == nil {
		return HolderKeyBuilder{}
	}
	return s.keys
}

func (s *HolderStore) ScriptSHA(name string) (string, bool) {
	if s == nil || s.scripts == nil {
		return "", false
	}
	return s.scripts.scriptSHA(name)
}

func (s *HolderStore) EnsureLoaded(ctx context.Context) error {
	if s == nil || s.scripts == nil {
		return ErrInvalidOptions
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return s.scripts.ensureLoaded(opCtx)
}

func (s *HolderStore) RegisterHolder(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	if s == nil || s.client == nil || s.scripts == nil {
		return core.HolderRecord{}, ErrInvalidOptions
	}
	now := s.opts.Now()
	input = normalizeRegistration(input, now, s.opts.LeaseTTL)
	if err := core.ValidateHolderRegistration(input, now, s.registryOptions()); err != nil {
		return core.HolderRecord{}, err
	}
	if err := validateLuaSafeHolderRecord(input); err != nil {
		return core.HolderRecord{}, err
	}
	recordBytes, resourceWire, recordDigest, err := encodeHolderRecord(input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if err := s.validateHolderPayload(recordBytes, resourceWire); err != nil {
		return core.HolderRecord{}, err
	}
	recordKey, member, err := s.recordAndMember(input.Domain, input.HolderID)
	if err != nil {
		return core.HolderRecord{}, err
	}
	registry := s.registryOptions()
	tag := s.keys.HashTag()
	args := []any{
		tag, s.opts.MaxHolders, s.opts.MaxRecordBytes, s.opts.MaxResourceBytes,
		s.opts.RecordTTL.Milliseconds(), s.opts.MinLeaseTTL.Milliseconds(),
		s.opts.MaxLeaseTTL.Milliseconds(), s.opts.ClockSkew.Milliseconds(),
		registry.MaxServingShardCount, registry.MaxCapacityWeight,
		holderRecordSchema, registry.MaxIDBytes, registry.MaxEndpointBytes,
		registry.MaxTLSIdentityBytes, string(input.Domain), input.HolderID, input.BootID,
		input.DeploymentIdentity.PodUID, input.DeploymentIdentity.WorkloadUID,
		ordinalWire(input.DeploymentIdentity), input.LogicEndpoint, input.HolderControlEndpoint,
		input.TLSDNSIdentity, string(input.State),
		boolArg(input.ProcessReady), boolArg(input.Draining), input.ServingShardCount,
		input.SnapshotRevision, input.StartedAt.UnixMilli(), input.HeartbeatAt.UnixMilli(),
		input.LeaseUntil.UnixMilli(), input.Sequence, input.CapacityWeight, resourceWire,
		recordDigest, recordBytes, s.opts.MaxTotalRecordBytes,
		s.opts.MaxTotalResourceBytes,
	}
	reply, err := s.runMutation(ctx, ScriptRegisterHolder,
		[]string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Accounting().String(), recordKey.String()}, args...)
	if err != nil {
		return core.HolderRecord{}, err
	}
	return s.decodeMutationReply(reply, member, input, false, now)
}

func (s *HolderStore) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return s.RegisterHolder(ctx, input)
}

func (s *HolderStore) HeartbeatHolder(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	if s == nil || s.client == nil || s.scripts == nil {
		return core.HolderRecord{}, ErrInvalidOptions
	}
	current, err := s.loadHeartbeatCurrent(ctx, input.Domain, input.HolderID)
	if err != nil {
		return core.HolderRecord{}, err
	}
	now := s.opts.Now()
	input = normalizeHeartbeat(input, now, current, s.opts.LeaseTTL)
	candidate := core.HolderRecord{
		Domain: current.Domain, HolderID: current.HolderID, BootID: input.BootID,
		DeploymentIdentity: current.DeploymentIdentity,
		LogicEndpoint:      current.LogicEndpoint, HolderControlEndpoint: current.HolderControlEndpoint,
		TLSDNSIdentity: current.TLSDNSIdentity,
		State:          input.State, ProcessReady: input.ProcessReady, Draining: input.Draining,
		ServingShardCount: input.ServingShardCount, SnapshotRevision: input.SnapshotRevision,
		StartedAt: current.StartedAt, HeartbeatAt: input.HeartbeatAt, LeaseUntil: input.LeaseUntil,
		Sequence: input.Sequence, CapacityWeight: input.CapacityWeight,
		ResourceSummary: input.ResourceSummary,
	}
	if err := core.ValidateHolderRecordCandidate(candidate, now, s.registryOptions()); err != nil {
		return core.HolderRecord{}, err
	}
	if err := validateLuaSafeHolderRecord(candidate); err != nil {
		return core.HolderRecord{}, err
	}
	recordBytes, resourceWire, recordDigest, err := encodeHolderRecord(candidate)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if err := s.validateHolderPayload(recordBytes, resourceWire); err != nil {
		return core.HolderRecord{}, err
	}
	recordKey, member, err := s.recordAndMember(input.Domain, input.HolderID)
	if err != nil {
		return core.HolderRecord{}, err
	}
	registry := s.registryOptions()
	tag := s.keys.HashTag()
	args := []any{
		tag, s.opts.MaxHolders, s.opts.MaxRecordBytes, s.opts.MaxResourceBytes,
		s.opts.RecordTTL.Milliseconds(), s.opts.MinLeaseTTL.Milliseconds(),
		s.opts.MaxLeaseTTL.Milliseconds(), s.opts.ClockSkew.Milliseconds(),
		registry.MaxServingShardCount, registry.MaxCapacityWeight,
		holderRecordSchema, registry.MaxIDBytes, registry.MaxEndpointBytes,
		registry.MaxTLSIdentityBytes, string(input.Domain), input.HolderID, input.BootID,
		string(input.State), boolArg(input.ProcessReady), boolArg(input.Draining),
		input.ServingShardCount, input.SnapshotRevision, input.HeartbeatAt.UnixMilli(),
		input.LeaseUntil.UnixMilli(), input.Sequence, input.CapacityWeight, resourceWire,
		recordDigest, recordBytes, s.opts.MaxTotalRecordBytes,
		s.opts.MaxTotalResourceBytes,
	}
	reply, err := s.runMutation(ctx, ScriptHeartbeatHolder,
		[]string{s.keys.Meta().String(), s.keys.Index().String(), s.keys.Accounting().String(), recordKey.String()}, args...)
	if err != nil {
		return core.HolderRecord{}, err
	}
	return s.decodeMutationReply(reply, member, candidate, true, now)
}

func (s *HolderStore) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	return s.HeartbeatHolder(ctx, input)
}

func (s *HolderStore) GetPlacementExclusion(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (core.PlacementExclusion, bool, error) {
	if s == nil || s.client == nil || s.scripts == nil {
		return core.PlacementExclusion{}, false, ErrInvalidOptions
	}
	holderKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return core.PlacementExclusion{}, false, err
	}
	placementKey, err := s.keys.PlacementRecord(domain, holderID)
	if err != nil {
		return core.PlacementExclusion{}, false, err
	}
	if err := s.keys.AssertSameSlot(holderKey, placementKey); err != nil {
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: placement key slots: %v", ErrScriptContract, err)
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return core.PlacementExclusion{}, false, err
	}
	defer cancel()
	registry := s.registryOptions()
	reply, err := s.scripts.run(opCtx, ScriptGetPlacementExclusion,
		[]string{holderKey.String(), placementKey.String()}, s.keys.HashTag(), string(domain), holderID, bootID,
		registry.MaxIDBytes, registry.MaxEndpointBytes, registry.MaxTLSIdentityBytes,
		s.opts.MaxRecordBytes, s.opts.MaxResourceBytes, registry.MaxServingShardCount, registry.MaxCapacityWeight,
		s.opts.MaxLeaseTTL.Milliseconds(), s.opts.ClockSkew.Milliseconds())
	if err != nil {
		return core.PlacementExclusion{}, false, err
	}
	return decodePlacementExclusionReply(reply)
}

func (s *HolderStore) ExcludePlacement(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string, precondition core.PlacementExclusionPrecondition) (core.PlacementExclusionResult, error) {
	if s == nil || s.client == nil || s.scripts == nil {
		return core.PlacementExclusionResult{}, ErrInvalidOptions
	}
	holderKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	placementKey, err := s.keys.PlacementRecord(domain, holderID)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if err := s.keys.AssertSameSlot(holderKey, placementKey); err != nil {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement key slots: %v", ErrScriptContract, err)
	}
	registry := s.registryOptions()
	reply, err := s.runMutation(ctx, ScriptExcludePlacement,
		[]string{holderKey.String(), placementKey.String()}, s.keys.HashTag(), string(domain), holderID, bootID,
		strconv.FormatUint(precondition.ExpectedRevision, 10), boolArg(precondition.ExpectedExcluded), precondition.OwnerPlanID,
		registry.MaxIDBytes, registry.MaxEndpointBytes, registry.MaxTLSIdentityBytes,
		s.opts.MaxRecordBytes, s.opts.MaxResourceBytes, registry.MaxServingShardCount, registry.MaxCapacityWeight,
		s.opts.MaxLeaseTTL.Milliseconds(), s.opts.ClockSkew.Milliseconds())
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if len(reply) == 0 {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: empty placement mutation reply", ErrScriptContract)
	}
	status := reply[0]
	if status != "COMMITTED" && status != "REPLAY" {
		return core.PlacementExclusionResult{}, placementStatusError(status)
	}
	state, found, err := decodePlacementExclusionReply(append([]string{"FOUND"}, reply[1:]...))
	if err != nil || !found {
		if err == nil {
			err = ErrHolderStoreCorrupt
		}
		return core.PlacementExclusionResult{}, err
	}
	return core.PlacementExclusionResult{Committed: status == "COMMITTED", State: state}, nil
}

func decodePlacementExclusionReply(reply []string) (core.PlacementExclusion, bool, error) {
	if len(reply) == 1 && (reply[0] == "NOT_FOUND" || reply[0] == "BOOT_MISMATCH") {
		return core.PlacementExclusion{}, false, nil
	}
	if len(reply) == 0 {
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: empty placement reply", ErrScriptContract)
	}
	if reply[0] != "FOUND" {
		return core.PlacementExclusion{}, false, placementStatusError(reply[0])
	}
	if len(reply) != 7 {
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: placement reply fields=%d", ErrScriptContract, len(reply))
	}
	revision, err := strconv.ParseUint(reply[5], 10, 64)
	if err != nil {
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: placement revision", ErrHolderStoreCorrupt)
	}
	var excluded bool
	switch reply[4] {
	case "0":
	case "1":
		excluded = true
	default:
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: placement excluded flag", ErrHolderStoreCorrupt)
	}
	state := core.PlacementExclusion{Domain: actorrouting.RouteDomain(reply[1]), HolderID: reply[2], BootID: reply[3],
		Excluded: excluded, Revision: revision, OwnerPlanID: reply[6]}
	if err := core.ValidatePlacementExclusion(state); err != nil {
		return core.PlacementExclusion{}, false, fmt.Errorf("%w: placement state: %v", ErrHolderStoreCorrupt, err)
	}
	return state, true, nil
}

func placementStatusError(status string) error {
	switch status {
	case "NOT_FOUND", "BOOT_MISMATCH":
		return core.ErrHolderNotFound
	case "CONFLICT", "OWNER_CONFLICT":
		return core.ErrPlacementExclusionConflict
	case "REVISION_EXHAUSTED":
		return core.ErrPlacementExclusionUnavailable
	case "MALFORMED", "BAD_NUMBER":
		return ErrHolderStoreCorrupt
	case "CROSS_SLOT":
		return fmt.Errorf("%w: placement CROSS_SLOT", ErrScriptContract)
	default:
		return fmt.Errorf("%w: placement status=%q", ErrScriptContract, status)
	}
}

// loadHeartbeatCurrent reads only the heartbeat's immutable record baseline.
// The heartbeat Lua script atomically reconciles and validates the complete
// registry before committing, so requiring a stable multi-record read here
// adds no safety. It can instead starve startup when unrelated holders
// concurrently register or heartbeat and advance the global revision.
func (s *HolderStore) loadHeartbeatCurrent(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, error) {
	recordKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return core.HolderRecord{}, err
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return core.HolderRecord{}, err
	}
	defer cancel()
	kind, err := s.client.Type(opCtx, recordKey.String()).Result()
	if err != nil {
		return core.HolderRecord{}, err
	}
	if kind == "none" {
		return core.HolderRecord{}, core.ErrHolderNotFound
	}
	if kind != "hash" {
		return core.HolderRecord{}, fmt.Errorf(
			"%w: record type=%s",
			ErrHolderStoreCorrupt,
			kind,
		)
	}
	nowMillis, err := s.redisNowMillis(opCtx)
	if err != nil {
		return core.HolderRecord{}, err
	}
	record, err := s.loadRecord(
		opCtx,
		recordKey,
		time.UnixMilli(nowMillis),
	)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if record.Domain != domain || record.HolderID != holderID {
		return core.HolderRecord{}, fmt.Errorf(
			"%w: heartbeat identity mismatch",
			ErrHolderStoreCorrupt,
		)
	}
	return record, nil
}

// GetHolderForHeartbeat returns the exact record baseline needed by
// HolderRegistry's heartbeat preflight. HeartbeatHolder's Lua mutation still
// reconciles and validates the complete registry atomically before commit.
// This point read prevents unrelated holder heartbeats from starving one
// another on the revision-fenced observational Get path.
func (s *HolderStore) GetHolderForHeartbeat(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	record, err := s.loadHeartbeatCurrent(ctx, domain, holderID)
	if errors.Is(err, core.ErrHolderNotFound) {
		return core.HolderRecord{}, false, nil
	}
	if err != nil {
		return core.HolderRecord{}, false, err
	}
	return record, true, nil
}

func (s *HolderStore) GetHolder(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	if s == nil || s.client == nil {
		return core.HolderRecord{}, false, ErrInvalidOptions
	}
	recordKey, member, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return core.HolderRecord{}, false, err
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return core.HolderRecord{}, false, err
	}
	defer cancel()
	for attempt := 1; attempt <= holderReadMaxAttempts; attempt++ {
		snapshot, revision, readErr := s.readHolderSnapshotAttempt(opCtx, attempt)
		record, found := snapshot.records[member]
		if readErr == nil && !found {
			// With no SCAN path, Get is the exact-key residual fence. A target
			// record outside a complete meta/index/accounting snapshot is corrupt,
			// including when all three registry keys have naturally disappeared.
			kind, typeErr := s.client.Type(opCtx, recordKey.String()).Result()
			if typeErr != nil {
				readErr = typeErr
			} else if kind != "none" {
				readErr = fmt.Errorf("%w: target record exists outside registry snapshot", ErrHolderStoreCorrupt)
			}
		}
		if !revision.known {
			return core.HolderRecord{}, found, readErr
		}
		stable, err := s.revisionStillCurrent(opCtx, revision)
		if err != nil {
			return core.HolderRecord{}, found, err
		}
		if !stable {
			continue
		}
		return record, found, readErr
	}
	return core.HolderRecord{}, false, fmt.Errorf(
		"%w: registry revision changed during Get after %d attempts",
		ErrHolderStoreUnavailable, holderReadMaxAttempts,
	)
}

func (s *HolderStore) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return s.GetHolder(ctx, domain, holderID)
}

func (s *HolderStore) ListHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderRecord, error) {
	if s == nil || s.client == nil {
		return nil, ErrInvalidOptions
	}
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return nil, ErrHolderStoreInvalid
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	for attempt := 1; attempt <= holderReadMaxAttempts; attempt++ {
		snapshot, revision, readErr := s.readHolderSnapshotAttempt(opCtx, attempt)
		records := make([]core.HolderRecord, 0, len(snapshot.records))
		if readErr == nil {
			for _, member := range snapshot.members {
				record, found := snapshot.records[member]
				if found && record.Domain == domain && record.LeaseUntil.After(snapshot.now) {
					records = append(records, record)
				}
			}
		}
		if !revision.known {
			return nil, readErr
		}
		stable, err := s.revisionStillCurrent(opCtx, revision)
		if err != nil {
			return nil, err
		}
		if !stable {
			continue
		}
		return records, readErr
	}
	return nil, fmt.Errorf(
		"%w: registry revision changed during List after %d attempts",
		ErrHolderStoreUnavailable, holderReadMaxAttempts,
	)
}

type holderRegistrySnapshot struct {
	records map[string]core.HolderRecord
	members []string
	now     time.Time
}

// readHolderSnapshotAttempt is the complete bounded accounting fence shared by
// Get and List. It validates every indexed member, accounting sidecar, record,
// and aggregate before either API can return a target-specific success.
func (s *HolderStore) readHolderSnapshotAttempt(
	ctx context.Context,
	attempt int,
) (holderRegistrySnapshot, holderRevisionToken, error) {
	meta, revision, err := s.loadMetaSnapshot(ctx)
	if err != nil {
		return holderRegistrySnapshot{}, revision, err
	}
	s.runReadAttemptHook(attempt, holderReadAfterMeta)
	if !meta.present {
		return holderRegistrySnapshot{records: make(map[string]core.HolderRecord)}, revision, nil
	}
	nowMillis, err := s.redisNowMillis(ctx)
	if err != nil {
		return holderRegistrySnapshot{}, revision, err
	}
	now := time.UnixMilli(nowMillis)
	indexed, err := s.client.ZCard(ctx, s.keys.Index().String()).Result()
	if err != nil {
		return holderRegistrySnapshot{}, revision, err
	}
	if indexed > int64(s.opts.MaxHolders) || indexed != int64(meta.count) {
		return holderRegistrySnapshot{}, revision, fmt.Errorf(
			"%w: index count=%d meta=%d max=%d",
			ErrHolderStoreCorrupt, indexed, meta.count, s.opts.MaxHolders,
		)
	}
	accountingCount, err := s.client.HLen(ctx, s.keys.Accounting().String()).Result()
	if err != nil {
		return holderRegistrySnapshot{}, revision, err
	}
	if accountingCount != indexed {
		return holderRegistrySnapshot{}, revision, fmt.Errorf(
			"%w: accounting count=%d index=%d", ErrHolderStoreCorrupt, accountingCount, indexed,
		)
	}
	var entries []redis.Z
	if indexed > 0 {
		entries, err = s.client.ZRangeWithScores(
			ctx, s.keys.Index().String(), 0, indexed-1,
		).Result()
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
	}
	if int64(len(entries)) != indexed {
		return holderRegistrySnapshot{}, revision, fmt.Errorf(
			"%w: index count=%d meta=%d", ErrHolderStoreCorrupt, len(entries), meta.count,
		)
	}
	snapshot := holderRegistrySnapshot{
		records: make(map[string]core.HolderRecord, len(entries)),
		members: make([]string, 0, len(entries)), now: now,
	}
	var totalBytes, totalResources int
	for _, entry := range entries {
		member, ok := redisMemberString(entry.Member)
		if !ok {
			return holderRegistrySnapshot{}, revision, fmt.Errorf("%w: index member type", ErrHolderStoreCorrupt)
		}
		snapshot.members = append(snapshot.members, member)
		memberDomain, holderID, err := parseHolderMember(member)
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		recordKey, _, err := s.recordAndMember(memberDomain, holderID)
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		if !validRedisScore(entry.Score) {
			return holderRegistrySnapshot{}, revision, fmt.Errorf("%w: invalid index score", ErrHolderStoreCorrupt)
		}
		accountingRaw, err := s.client.HGet(
			ctx, s.keys.Accounting().String(), member,
		).Result()
		if err != nil {
			return holderRegistrySnapshot{}, revision, fmt.Errorf("%w: missing member accounting", ErrHolderStoreCorrupt)
		}
		accounting, err := parseHolderAccounting(
			accountingRaw, s.opts.MaxRecordBytes, s.opts.MaxResourceBytes,
		)
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		nextTotalBytes, ok := addBoundedHolderAggregate(
			totalBytes, accounting.recordBytes,
			s.opts.MaxRecordBytes, s.opts.MaxTotalRecordBytes,
		)
		if !ok {
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: record accounting aggregate capacity", ErrHolderStoreCorrupt,
			)
		}
		nextTotalResources, ok := addBoundedHolderAggregate(
			totalResources, accounting.resourceBytes,
			s.opts.MaxResourceBytes, s.opts.MaxTotalResourceBytes,
		)
		if !ok {
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: resource accounting aggregate capacity", ErrHolderStoreCorrupt,
			)
		}
		totalBytes = nextTotalBytes
		totalResources = nextTotalResources
		kind, err := s.client.Type(ctx, recordKey.String()).Result()
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		if kind == "none" {
			if entry.Score <= float64(nowMillis) {
				continue
			}
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: fresh member missing record", ErrHolderStoreCorrupt,
			)
		}
		if kind != "hash" {
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: record type=%s", ErrHolderStoreCorrupt, kind,
			)
		}
		record, err := s.loadRecord(ctx, recordKey, now)
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		if record.Domain != memberDomain || record.HolderID != holderID {
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: indexed identity mismatch", ErrHolderStoreCorrupt,
			)
		}
		if !scoreMatchesLease(entry.Score, record.LeaseUntil) {
			return holderRegistrySnapshot{}, revision, fmt.Errorf(
				"%w: index score does not match lease", ErrHolderStoreCorrupt,
			)
		}
		recordBytes, resourceWire, _, err := encodeHolderRecord(record)
		if err != nil {
			return holderRegistrySnapshot{}, revision, err
		}
		if accounting.recordBytes != recordBytes || accounting.resourceBytes != len(resourceWire) {
			return holderRegistrySnapshot{}, revision, fmt.Errorf("%w: record accounting mismatch", ErrHolderStoreCorrupt)
		}
		snapshot.records[member] = record
	}
	if totalBytes != meta.recordBytes || totalResources != meta.resourceBytes {
		return holderRegistrySnapshot{}, revision, fmt.Errorf("%w: aggregate counters mismatch", ErrHolderStoreCorrupt)
	}
	return snapshot, revision, nil
}

// addBoundedHolderAggregate validates both the per-value and aggregate bounds
// before addition. This keeps the Go reader aligned with Redis Lua's exact
// integer domain and prevents a forged accounting set from wrapping int back
// to an apparently valid meta counter.
func addBoundedHolderAggregate(total, value, maxValue, maxTotal int) (int, bool) {
	if total < 0 || value <= 0 || value > maxValue || total > maxTotal || value > maxTotal {
		return 0, false
	}
	total64, value64 := int64(total), int64(value)
	if total64 > maxLuaSafeInteger || value64 > maxLuaSafeInteger ||
		total64 > maxLuaSafeInteger-value64 {
		return 0, false
	}
	if total > maxTotal-value {
		return 0, false
	}
	return total + value, true
}

func (s *HolderStore) runReadAttemptHook(attempt int, stage holderReadStage) {
	if s.readAttemptHook != nil {
		s.readAttemptHook(attempt, stage)
	}
}

func (s *HolderStore) List(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderRecord, error) {
	return s.ListHolders(ctx, domain)
}

// DeleteHolderForTest intentionally deletes exactly one record key. It does
// not expose production cleanup and does not issue broad deletion commands.
func (s *HolderStore) DeleteHolderForTest(ctx context.Context, domain actorrouting.RouteDomain, holderID string) error {
	if s == nil || s.client == nil {
		return ErrInvalidOptions
	}
	recordKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return err
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return s.client.Del(opCtx, recordKey.String()).Err()
}

func (s *HolderStore) ExistsHolderForTest(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (bool, error) {
	if s == nil || s.client == nil {
		return false, ErrInvalidOptions
	}
	recordKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return false, err
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	exists, err := s.client.Exists(opCtx, recordKey.String()).Result()
	return exists == 1, err
}

func (s *HolderStore) recordAndMember(domain actorrouting.RouteDomain, holderID string) (rediskey.RedisKey, string, error) {
	recordKey, err := s.keys.Record(domain, holderID)
	if err != nil {
		return "", "", err
	}
	member, err := s.keys.Member(domain, holderID)
	if err != nil {
		return "", "", err
	}
	if err := s.keys.AssertSameSlot(s.keys.Meta(), s.keys.Index(), s.keys.Accounting(), recordKey); err != nil {
		return "", "", fmt.Errorf("%w: holder key slots: %v", ErrScriptContract, err)
	}
	return recordKey, member, nil
}

type holderMetaCounters struct {
	present       bool
	revision      string
	count         int
	recordBytes   int
	resourceBytes int
}

type holderRevisionToken struct {
	known   bool
	present bool
	value   string
}

func (s *HolderStore) loadMetaSnapshot(ctx context.Context) (holderMetaCounters, holderRevisionToken, error) {
	metaType, err := s.client.Type(ctx, s.keys.Meta().String()).Result()
	if err != nil {
		return holderMetaCounters{}, holderRevisionToken{}, err
	}
	if metaType == "none" {
		revision := holderRevisionToken{known: true}
		indexType, err := s.client.Type(ctx, s.keys.Index().String()).Result()
		if err != nil {
			return holderMetaCounters{}, revision, err
		}
		accountingType, err := s.client.Type(ctx, s.keys.Accounting().String()).Result()
		if err != nil {
			return holderMetaCounters{}, revision, err
		}
		if indexType != "none" || accountingType != "none" {
			return holderMetaCounters{}, revision, fmt.Errorf(
				"%w: meta=%s index=%s accounting=%s",
				ErrHolderStoreCorrupt, metaType, indexType, accountingType,
			)
		}
		return holderMetaCounters{}, revision, nil
	}
	if metaType != "hash" {
		return holderMetaCounters{}, holderRevisionToken{}, fmt.Errorf(
			"%w: meta type=%s", ErrHolderStoreCorrupt, metaType,
		)
	}
	revision, err := s.captureRevisionAfterHash(ctx)
	if err != nil {
		return holderMetaCounters{}, holderRevisionToken{}, err
	}
	if s.metaReadHook != nil {
		s.metaReadHook(holderMetaReadAfterRevision)
	}
	fieldCount, err := s.client.HLen(ctx, s.keys.Meta().String()).Result()
	if err != nil {
		return holderMetaCounters{}, revision, err
	}
	if fieldCount != int64(len(holderMetaFields)) {
		return holderMetaCounters{}, revision, fmt.Errorf(
			"%w: meta field count=%d", ErrHolderStoreCorrupt, fieldCount,
		)
	}
	if err := s.validateMetaFieldLengths(ctx); err != nil {
		return holderMetaCounters{}, revision, err
	}
	meta, err := s.client.HGetAll(ctx, s.keys.Meta().String()).Result()
	if err != nil {
		return holderMetaCounters{}, revision, err
	}
	if len(meta) != len(holderMetaFields) || meta["schema_version"] != holderMetaSchema {
		return holderMetaCounters{}, revision, fmt.Errorf("%w: meta fields", ErrHolderStoreCorrupt)
	}
	for _, field := range holderMetaFields {
		if _, ok := meta[field]; !ok {
			return holderMetaCounters{}, revision, fmt.Errorf(
				"%w: meta missing field=%s", ErrHolderStoreCorrupt, field,
			)
		}
	}
	if meta["registry_revision"] != revision.value {
		return holderMetaCounters{}, revision, fmt.Errorf(
			"%w: meta revision changed during snapshot", ErrHolderStoreCorrupt,
		)
	}
	if err := validateRegistryRevision(meta["registry_revision"]); err != nil {
		return holderMetaCounters{}, revision, err
	}
	recordBytes, err := parseNonNegativeInt(meta["record_bytes"])
	if err != nil || recordBytes > s.opts.MaxTotalRecordBytes {
		return holderMetaCounters{}, revision, fmt.Errorf("%w: meta record_bytes", ErrHolderStoreCorrupt)
	}
	resourceBytes, err := parseNonNegativeInt(meta["resource_bytes"])
	if err != nil || resourceBytes > s.opts.MaxTotalResourceBytes {
		return holderMetaCounters{}, revision, fmt.Errorf("%w: meta resource_bytes", ErrHolderStoreCorrupt)
	}
	count, err := parseNonNegativeInt(meta["holder_count"])
	if err != nil || count > s.opts.MaxHolders {
		return holderMetaCounters{}, revision, fmt.Errorf(
			"%w: meta holder count=%d", ErrHolderStoreCorrupt, count,
		)
	}
	indexType, err := s.client.Type(ctx, s.keys.Index().String()).Result()
	if err != nil {
		return holderMetaCounters{}, revision, err
	}
	if indexType != "zset" {
		return holderMetaCounters{}, revision, fmt.Errorf(
			"%w: meta=%s index=%s", ErrHolderStoreCorrupt, metaType, indexType,
		)
	}
	accountingType, err := s.client.Type(ctx, s.keys.Accounting().String()).Result()
	if err != nil {
		return holderMetaCounters{}, revision, err
	}
	if accountingType != "hash" {
		return holderMetaCounters{}, revision, fmt.Errorf(
			"%w: meta=%s accounting=%s", ErrHolderStoreCorrupt, metaType, accountingType,
		)
	}
	return holderMetaCounters{
		present: true, revision: meta["registry_revision"], count: count,
		recordBytes: recordBytes, resourceBytes: resourceBytes,
	}, revision, nil
}

func (s *HolderStore) captureRevisionAfterHash(ctx context.Context) (holderRevisionToken, error) {
	revision, err := s.client.HGet(ctx, s.keys.Meta().String(), "registry_revision").Result()
	if err == nil {
		return holderRevisionToken{known: true, present: true, value: revision}, nil
	}
	if !errors.Is(err, redis.Nil) {
		return holderRevisionToken{}, err
	}
	// TYPE already observed a hash. If it vanished before HGET, retain a
	// present sentinel so the outer fence cannot mistake the disappearance for
	// a stable absent snapshot. A stable hash missing its revision remains a
	// stable corrupt snapshot.
	kind, typeErr := s.client.Type(ctx, s.keys.Meta().String()).Result()
	if typeErr != nil {
		return holderRevisionToken{}, typeErr
	}
	if kind == "none" {
		return holderRevisionToken{known: true, present: true}, nil
	}
	return holderRevisionToken{known: true}, nil
}

type holderFieldLimit struct {
	name  string
	limit int64
}

func (s *HolderStore) validateMetaFieldLengths(ctx context.Context) error {
	limits := []holderFieldLimit{
		{name: "schema_version", limit: int64(len(holderMetaSchema))},
		{name: "registry_revision", limit: int64(len(maxRegistryRevision))},
		{name: "holder_count", limit: int64(len("9007199254740991"))},
		{name: "record_bytes", limit: int64(len("9007199254740991"))},
		{name: "resource_bytes", limit: int64(len("9007199254740991"))},
	}
	return s.validateHashFieldLengths(ctx, s.keys.Meta(), limits)
}

func (s *HolderStore) validateRecordFieldLengths(ctx context.Context, key rediskey.RedisKey) error {
	registry := s.registryOptions()
	numericLimit := int64(len("9007199254740991"))
	limits := []holderFieldLimit{
		{name: "schema_version", limit: int64(len(holderRecordSchema))},
		{name: "domain", limit: 16},
		{name: "holder_id", limit: int64(registry.MaxIDBytes)},
		{name: "boot_id", limit: int64(registry.MaxIDBytes)},
		{name: "pod_uid", limit: int64(registry.MaxIDBytes)},
		{name: "workload_uid", limit: int64(registry.MaxIDBytes)},
		{name: "ordinal", limit: numericLimit},
		{name: "logic_endpoint", limit: int64(registry.MaxEndpointBytes)},
		{name: "holder_control_endpoint", limit: int64(registry.MaxEndpointBytes)},
		{name: "tls_dns_identity", limit: int64(registry.MaxTLSIdentityBytes)},
		{name: "state", limit: 32},
		{name: "process_ready", limit: 1},
		{name: "draining", limit: 1},
		{name: "serving_shard_count", limit: numericLimit},
		{name: "snapshot_revision", limit: numericLimit},
		{name: "started_at_ms", limit: numericLimit},
		{name: "heartbeat_at_ms", limit: numericLimit},
		{name: "lease_until_ms", limit: numericLimit},
		{name: "sequence", limit: numericLimit},
		{name: "capacity_weight", limit: numericLimit},
		{name: "resource_summary", limit: int64(s.opts.MaxResourceBytes)},
		{name: "record_bytes", limit: numericLimit},
		{name: "record_digest", limit: 40},
		{name: "record_integrity", limit: 40},
	}
	return s.validateHashFieldLengths(ctx, key, limits)
}

func (s *HolderStore) validateHashFieldLengths(ctx context.Context, key rediskey.RedisKey, limits []holderFieldLimit) error {
	pipe := s.client.Pipeline()
	existsCmds := make([]*redis.BoolCmd, len(limits))
	lengthCmds := make([]*redis.IntCmd, len(limits))
	for index, field := range limits {
		existsCmds[index] = pipe.HExists(ctx, key.String(), field.name)
		lengthCmds[index] = pipe.HStrLen(ctx, key.String(), field.name)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: bounded field length check: %v", ErrHolderStoreCorrupt, err)
	}
	for index, field := range limits {
		exists, err := existsCmds[index].Result()
		if err != nil {
			return fmt.Errorf("%w: field=%s existence: %v", ErrHolderStoreCorrupt, field.name, err)
		}
		if !exists {
			return fmt.Errorf("%w: missing field=%s", ErrHolderStoreCorrupt, field.name)
		}
		length, err := lengthCmds[index].Result()
		if err != nil {
			return fmt.Errorf("%w: field=%s length: %v", ErrHolderStoreCorrupt, field.name, err)
		}
		if length > field.limit {
			return fmt.Errorf("%w: field=%s bytes=%d", ErrHolderStoreCorrupt, field.name, length)
		}
	}
	return nil
}

func (s *HolderStore) readRevisionToken(ctx context.Context) (holderRevisionToken, error) {
	revision, err := s.client.HGet(ctx, s.keys.Meta().String(), "registry_revision").Result()
	if errors.Is(err, redis.Nil) {
		return holderRevisionToken{known: true}, nil
	}
	if err != nil {
		return holderRevisionToken{}, err
	}
	return holderRevisionToken{known: true, present: true, value: revision}, nil
}

func (s *HolderStore) revisionStillCurrent(ctx context.Context, initial holderRevisionToken) (bool, error) {
	current, err := s.readRevisionToken(ctx)
	if err != nil {
		return false, err
	}
	return initial.known && current.known &&
		initial.present == current.present && initial.value == current.value, nil
}

func validateRegistryRevision(value string) error {
	if value == "" || value == "0" || value[0] == '0' ||
		len(value) > len(maxRegistryRevision) ||
		(len(value) == len(maxRegistryRevision) && value > maxRegistryRevision) {
		return fmt.Errorf("%w: registry revision", ErrHolderStoreCorrupt)
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("%w: registry revision", ErrHolderStoreCorrupt)
		}
	}
	return nil
}

func (s *HolderStore) redisNowMillis(ctx context.Context) (int64, error) {
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	millis := now.UnixMilli()
	if millis < 0 || millis > maxLuaSafeInteger {
		return 0, fmt.Errorf("%w: Redis TIME outside safe range", ErrHolderStoreCorrupt)
	}
	return millis, nil
}

func validRedisScore(score float64) bool {
	return !math.IsNaN(score) && !math.IsInf(score, 0) &&
		score >= 0 && score <= float64(maxLuaSafeInteger) && math.Trunc(score) == score
}

func scoreMatchesLease(score float64, lease time.Time) bool {
	if !validRedisScore(score) {
		return false
	}
	leaseMillis := lease.UnixMilli()
	return leaseMillis >= 0 && leaseMillis <= maxLuaSafeInteger && score == float64(leaseMillis)
}

func redisMemberString(member any) (string, bool) {
	switch value := member.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

type holderAccountingCounters struct {
	recordBytes   int
	resourceBytes int
}

func holderAccountingValue(recordBytes, resourceBytes int) string {
	return strconv.Itoa(recordBytes) + ":" + strconv.Itoa(resourceBytes)
}

func parseHolderAccounting(value string, maxRecord, maxResource int) (holderAccountingCounters, error) {
	separator := strings.IndexByte(value, ':')
	if separator <= 0 || separator == len(value)-1 || strings.IndexByte(value[separator+1:], ':') >= 0 {
		return holderAccountingCounters{}, fmt.Errorf("%w: accounting wire", ErrHolderStoreCorrupt)
	}
	recordBytes, err := parseCanonicalPositiveInt(value[:separator])
	if err != nil || recordBytes > maxRecord {
		return holderAccountingCounters{}, fmt.Errorf("%w: accounting record bytes", ErrHolderStoreCorrupt)
	}
	resourceBytes, err := parseCanonicalPositiveInt(value[separator+1:])
	if err != nil || resourceBytes > maxResource {
		return holderAccountingCounters{}, fmt.Errorf("%w: accounting resource bytes", ErrHolderStoreCorrupt)
	}
	return holderAccountingCounters{recordBytes: recordBytes, resourceBytes: resourceBytes}, nil
}

func holderRecordIntegrity(values map[string]string) (string, error) {
	digest := sha1.New() //nolint:gosec // Persisted corruption checksum, not authentication.
	for _, field := range holderRecordFields {
		if field == "record_integrity" {
			continue
		}
		value, ok := values[field]
		if !ok {
			return "", fmt.Errorf("%w: integrity missing field=%s", ErrHolderStoreCorrupt, field)
		}
		_, _ = digest.Write([]byte(strconv.Itoa(len(field))))
		_, _ = digest.Write([]byte(":"))
		_, _ = digest.Write([]byte(field))
		_, _ = digest.Write([]byte(strconv.Itoa(len(value))))
		_, _ = digest.Write([]byte(":"))
		_, _ = digest.Write([]byte(value))
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func validateHolderRecordIntegrity(values map[string]string) error {
	expected, err := holderRecordIntegrity(values)
	if err != nil {
		return err
	}
	if values["record_integrity"] != expected {
		return fmt.Errorf("%w: record integrity mismatch", ErrHolderStoreCorrupt)
	}
	return nil
}

func (s *HolderStore) loadRecord(ctx context.Context, key rediskey.RedisKey, now time.Time) (core.HolderRecord, error) {
	kind, err := s.client.Type(ctx, key.String()).Result()
	if err != nil {
		return core.HolderRecord{}, err
	}
	if kind != "hash" {
		return core.HolderRecord{}, fmt.Errorf("%w: record type=%s", ErrHolderStoreCorrupt, kind)
	}
	fieldCount, err := s.client.HLen(ctx, key.String()).Result()
	if err != nil {
		return core.HolderRecord{}, err
	}
	if fieldCount != int64(len(holderRecordFields)) {
		return core.HolderRecord{}, fmt.Errorf(
			"%w: record field count=%d", ErrHolderStoreCorrupt, fieldCount,
		)
	}
	if err := s.validateRecordFieldLengths(ctx, key); err != nil {
		return core.HolderRecord{}, err
	}
	values, err := s.client.HGetAll(ctx, key.String()).Result()
	if err != nil {
		return core.HolderRecord{}, err
	}
	if len(values) != len(holderRecordFields) {
		return core.HolderRecord{}, fmt.Errorf("%w: record field count=%d", ErrHolderStoreCorrupt, len(values))
	}
	for _, field := range holderRecordFields {
		if _, ok := values[field]; !ok {
			return core.HolderRecord{}, fmt.Errorf("%w: missing field=%s", ErrHolderStoreCorrupt, field)
		}
	}
	if err := validateHolderRecordIntegrity(values); err != nil {
		return core.HolderRecord{}, err
	}
	record, recordBytes, resourceBytes, err := decodeHolderRecord(values)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if recordBytes > s.opts.MaxRecordBytes || resourceBytes > s.opts.MaxResourceBytes {
		return core.HolderRecord{}, fmt.Errorf("%w: record/resource bytes", ErrHolderStoreCorrupt)
	}
	encodedBytes, resourceWire, recordDigest, err := encodeHolderRecord(record)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if recordBytes != encodedBytes || resourceBytes != len(resourceWire) ||
		values["resource_summary"] != resourceWire {
		return core.HolderRecord{}, fmt.Errorf("%w: record byte counter mismatch", ErrHolderStoreCorrupt)
	}
	if values["record_digest"] != recordDigest {
		return core.HolderRecord{}, fmt.Errorf("%w: record digest mismatch", ErrHolderStoreCorrupt)
	}
	if err := core.ValidateHolderRecord(record, now, s.registryOptions()); err != nil {
		return core.HolderRecord{}, fmt.Errorf("%w: validation: %v", ErrHolderStoreCorrupt, err)
	}
	if err := validateLuaSafeHolderRecord(record); err != nil {
		return core.HolderRecord{}, err
	}
	return cloneHolderStoreRecord(record), nil
}

func (s *HolderStore) decodeMutationReply(
	reply []string,
	member string,
	expected core.HolderRecord,
	heartbeat bool,
	preflightNow time.Time,
) (core.HolderRecord, error) {
	if len(reply) == 0 {
		return core.HolderRecord{}, fmt.Errorf("%w: empty holder mutation reply", ErrScriptContract)
	}
	status := reply[0]
	switch status {
	case "COMMITTED", "REPLACED", "ALREADY":
	case "REPLAY":
		if heartbeat {
			return core.HolderRecord{}, ErrHolderStoreReplay
		}
	default:
		return core.HolderRecord{}, holderStatusError(status)
	}
	if len(reply) != 1+len(holderRecordFields) {
		return core.HolderRecord{}, fmt.Errorf("%w: holder reply fields=%d", ErrScriptContract, len(reply))
	}
	values := make(map[string]string, len(holderRecordFields))
	for index, field := range holderRecordFields {
		values[field] = reply[index+1]
	}
	if err := validateMutationReplyFields(values, s.opts.MaxRecordBytes, s.opts.MaxResourceBytes); err != nil {
		return core.HolderRecord{}, err
	}
	if err := validateHolderRecordIntegrity(values); err != nil {
		return core.HolderRecord{}, err
	}
	record, recordBytes, resourceBytes, err := decodeHolderRecord(values)
	if err != nil {
		return core.HolderRecord{}, err
	}
	memberDomain, holderID, err := parseHolderMember(member)
	if err != nil || memberDomain != record.Domain || holderID != record.HolderID {
		return core.HolderRecord{}, fmt.Errorf("%w: mutation identity mismatch", ErrHolderStoreCorrupt)
	}
	encoded, resourceWire, recordDigest, err := encodeHolderRecord(record)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if recordBytes != encoded || resourceBytes != len(resourceWire) ||
		values["resource_summary"] != resourceWire ||
		values["record_digest"] != recordDigest {
		return core.HolderRecord{}, fmt.Errorf("%w: mutation bytes or digest mismatch", ErrHolderStoreCorrupt)
	}
	if err := core.ValidateHolderRecord(record, preflightNow, s.registryOptions()); err != nil {
		return core.HolderRecord{}, fmt.Errorf("%w: mutation record: %v", ErrHolderStoreCorrupt, err)
	}
	if err := validateLuaSafeHolderRecord(record); err != nil {
		return core.HolderRecord{}, err
	}
	if !core.HolderRecordEqual(record, expected) {
		return core.HolderRecord{}, fmt.Errorf("%w: mutation response differs from candidate", ErrHolderStoreCorrupt)
	}
	return cloneHolderStoreRecord(record), nil
}

func (s *HolderStore) validateHolderPayload(recordBytes int, resourceJSON string) error {
	if recordBytes <= 0 || recordBytes > s.opts.MaxRecordBytes ||
		len(resourceJSON) > s.opts.MaxResourceBytes {
		return fmt.Errorf("%w: holder payload exceeds per-record limits", ErrHolderStoreCapacity)
	}
	if recordBytes > s.opts.MaxTotalRecordBytes || len(resourceJSON) > s.opts.MaxTotalResourceBytes {
		return fmt.Errorf("%w: holder payload exceeds aggregate limits", ErrHolderStoreCapacity)
	}
	return nil
}

func validateMutationReplyFields(values map[string]string, maxRecord, maxResource int) error {
	for _, field := range holderRecordFields {
		value, ok := values[field]
		if !ok {
			return fmt.Errorf("%w: mutation missing field=%s", ErrHolderStoreCorrupt, field)
		}
		if field == "record_bytes" && len(value) > len("9007199254740991") {
			return fmt.Errorf("%w: mutation record_bytes field too large", ErrHolderStoreCorrupt)
		}
		if field == "resource_summary" && len(value) > maxResource {
			return fmt.Errorf("%w: mutation resource field too large", ErrHolderStoreCorrupt)
		}
		if field == "record_integrity" && len(value) != 40 {
			return fmt.Errorf("%w: mutation integrity field size", ErrHolderStoreCorrupt)
		}
	}
	return nil
}

func holderStatusError(status string) error {
	switch status {
	case "CROSS_SLOT":
		return fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	case "MALFORMED", "BAD_TIME", "BAD_NUMBER":
		return ErrHolderStoreCorrupt
	case "REVISION_EXHAUSTED":
		return ErrHolderStoreUnavailable
	case "CAPACITY_EXCEEDED":
		return ErrHolderStoreCapacity
	case "FRESH_BOOT_CONFLICT", "CONFLICT", "BOOT_MISMATCH", "IDENTITY_MISMATCH":
		return ErrHolderStoreBootConflict
	case "LEASE_EXPIRED":
		return ErrHolderStoreLeaseExpired
	case "STALE":
		return ErrHolderStoreStale
	case "BAD_STATE":
		return core.ErrHolderStateTransition
	default:
		return fmt.Errorf("%w: holder status=%q", ErrScriptContract, status)
	}
}

func (s *HolderStore) runMutation(ctx context.Context, name string, keys []string, args ...any) ([]string, error) {
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	reply, runErr := s.scripts.run(opCtx, name, keys, args...)
	cancel()
	if runErr == nil {
		return reply, nil
	}
	if !errors.Is(runErr, context.DeadlineExceeded) {
		return nil, runErr
	}
	// The first attempt may have reached Redis before the client deadline.
	// Re-enter with the exact same key/sequence/digest identity.
	retryCtx, retryCancel := context.WithTimeout(context.Background(), s.opts.OperationTimeout)
	defer retryCancel()
	retryReply, retryErr := s.scripts.run(retryCtx, name, keys, args...)
	if retryErr != nil {
		return nil, runErr
	}
	if len(retryReply) > 0 && retryReply[0] == "REPLAY" {
		retryReply[0] = "COMMITTED"
	}
	return retryReply, nil
}

func (s *HolderStore) operationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: context required", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	opCtx, cancel := context.WithTimeout(ctx, s.opts.OperationTimeout)
	return opCtx, cancel, nil
}

func (s *HolderStore) registryOptions() core.HolderRegistryOptions {
	registry := core.DefaultHolderRegistryOptions()
	registry.Now = s.opts.Now
	registry.LeaseTTL = s.opts.LeaseTTL
	registry.MinLeaseTTL = s.opts.MinLeaseTTL
	registry.MaxLeaseTTL = s.opts.MaxLeaseTTL
	registry.ClockSkew = s.opts.ClockSkew
	registry.MaxHolderCount = s.opts.MaxHolders
	registry.MaxResourceSummaryBytes = s.opts.MaxResourceBytes
	return registry
}

const (
	holderResourceWirePrefix = core.HolderResourceSummaryWirePrefix
	holderDigestWirePrefix   = "HD5:"
	holderRecordWirePrefix   = "HR5:"
)

var holderSemanticFields = [...]string{
	"schema_version", "domain", "holder_id", "boot_id", "pod_uid", "workload_uid", "ordinal",
	"logic_endpoint", "holder_control_endpoint",
	"tls_dns_identity", "state", "process_ready", "draining", "serving_shard_count",
	"snapshot_revision", "started_at_ms", "heartbeat_at_ms", "lease_until_ms",
	"sequence", "capacity_weight", "resource_summary",
}

func ordinalWire(identity holderidentity.DeploymentIdentity) string {
	if identity.IsZero() {
		return ""
	}
	return strconv.FormatInt(int64(identity.Ordinal), 10)
}

func encodeHolderRecord(record core.HolderRecord) (int, string, string, error) {
	resourceWire := encodeHolderResourceSummary(record.ResourceSummary)
	values := holderSemanticValues(record, resourceWire)
	digest, err := holderSemanticDigest(values)
	if err != nil {
		return 0, "", "", err
	}
	values["record_digest"] = digest
	recordBytes, err := holderRecordWireBytes(values)
	if err != nil {
		return 0, "", "", err
	}
	return recordBytes, resourceWire, digest, nil
}

func holderSemanticValues(record core.HolderRecord, resourceWire string) map[string]string {
	return map[string]string{
		"schema_version": holderRecordSchema,
		"domain":         string(record.Domain), "holder_id": record.HolderID, "boot_id": record.BootID,
		"pod_uid": record.DeploymentIdentity.PodUID, "workload_uid": record.DeploymentIdentity.WorkloadUID,
		"ordinal":        ordinalWire(record.DeploymentIdentity),
		"logic_endpoint": record.LogicEndpoint, "holder_control_endpoint": record.HolderControlEndpoint,
		"tls_dns_identity": record.TLSDNSIdentity,
		"state":            string(record.State), "process_ready": boolArg(record.ProcessReady),
		"draining":            boolArg(record.Draining),
		"serving_shard_count": strconv.FormatUint(uint64(record.ServingShardCount), 10),
		"snapshot_revision":   strconv.FormatUint(record.SnapshotRevision, 10),
		"started_at_ms":       strconv.FormatInt(record.StartedAt.UnixMilli(), 10),
		"heartbeat_at_ms":     strconv.FormatInt(record.HeartbeatAt.UnixMilli(), 10),
		"lease_until_ms":      strconv.FormatInt(record.LeaseUntil.UnixMilli(), 10),
		"sequence":            strconv.FormatUint(record.Sequence, 10),
		"capacity_weight":     strconv.FormatUint(uint64(record.CapacityWeight), 10),
		"resource_summary":    resourceWire,
	}
}

func encodeHolderResourceSummary(summary map[string]string) string {
	return core.CanonicalHolderResourceSummaryWire(summary)
}

func decodeHolderResourceSummary(raw string) (map[string]string, error) {
	if !strings.HasPrefix(raw, holderResourceWirePrefix) {
		return nil, fmt.Errorf("%w: resource summary prefix", ErrHolderStoreCorrupt)
	}
	position := len(holderResourceWirePrefix)
	count, next, err := parseHolderWireUint(raw, position)
	if err != nil {
		return nil, fmt.Errorf("%w: resource summary count", ErrHolderStoreCorrupt)
	}
	position = next
	if count > uint64(len(raw)) {
		return nil, fmt.Errorf("%w: resource summary count", ErrHolderStoreCorrupt)
	}
	summary := make(map[string]string, int(count))
	previous := ""
	for index := uint64(0); index < count; index++ {
		key, next, err := parseHolderWireFrame(raw, position)
		if err != nil || key == "" || (index > 0 && key <= previous) {
			return nil, fmt.Errorf("%w: resource summary key order", ErrHolderStoreCorrupt)
		}
		position = next
		value, next, err := parseHolderWireFrame(raw, position)
		if err != nil {
			return nil, fmt.Errorf("%w: resource summary value", ErrHolderStoreCorrupt)
		}
		position = next
		summary[key] = value
		previous = key
	}
	if position != len(raw) {
		return nil, fmt.Errorf("%w: resource summary trailing bytes", ErrHolderStoreCorrupt)
	}
	if len(summary) == 0 {
		return nil, nil
	}
	return summary, nil
}

func writeHolderFrame(wire *strings.Builder, value string) {
	wire.WriteString(strconv.Itoa(len(value)))
	wire.WriteByte(':')
	wire.WriteString(value)
}

func parseHolderWireFrame(raw string, position int) (string, int, error) {
	length, next, err := parseHolderWireUint(raw, position)
	if err != nil || length > uint64(len(raw)-next) {
		return "", position, ErrHolderStoreCorrupt
	}
	end := next + int(length)
	return raw[next:end], end, nil
}

func parseHolderWireUint(raw string, position int) (uint64, int, error) {
	if position >= len(raw) {
		return 0, position, ErrHolderStoreCorrupt
	}
	separator := strings.IndexByte(raw[position:], ':')
	if separator <= 0 {
		return 0, position, ErrHolderStoreCorrupt
	}
	end := position + separator
	value := raw[position:end]
	parsed, err := parseCanonicalSafeUint(value)
	if err != nil {
		return 0, position, err
	}
	return parsed, end + 1, nil
}

func holderSemanticDigest(values map[string]string) (string, error) {
	digest := sha1.New() //nolint:gosec // Exact persisted corruption checksum, not authentication.
	_, _ = digest.Write([]byte(holderDigestWirePrefix))
	for _, field := range holderSemanticFields {
		value, ok := values[field]
		if !ok {
			return "", fmt.Errorf("%w: digest missing field=%s", ErrHolderStoreCorrupt, field)
		}
		writeHolderHashFrame(digest, field)
		writeHolderHashFrame(digest, value)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func holderRecordWireBytes(values map[string]string) (int, error) {
	var wire strings.Builder
	wire.WriteString(holderRecordWirePrefix)
	for _, field := range holderSemanticFields {
		value, ok := values[field]
		if !ok {
			return 0, fmt.Errorf("%w: record wire missing field=%s", ErrHolderStoreCorrupt, field)
		}
		writeHolderFrame(&wire, field)
		writeHolderFrame(&wire, value)
	}
	digest, ok := values["record_digest"]
	if !ok {
		return 0, fmt.Errorf("%w: record wire missing digest", ErrHolderStoreCorrupt)
	}
	writeHolderFrame(&wire, "record_digest")
	writeHolderFrame(&wire, digest)
	return wire.Len(), nil
}

type holderHashWriter interface {
	Write([]byte) (int, error)
}

func writeHolderHashFrame(wire holderHashWriter, value string) {
	_, _ = wire.Write([]byte(strconv.Itoa(len(value))))
	_, _ = wire.Write([]byte(":"))
	_, _ = wire.Write([]byte(value))
}

func decodeHolderRecord(values map[string]string) (core.HolderRecord, int, int, error) {
	if values["schema_version"] != holderRecordSchema {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: schema=%q", ErrHolderStoreCorrupt, values["schema_version"])
	}
	identity := holderidentity.DeploymentIdentity{
		PodUID: values["pod_uid"], WorkloadUID: values["workload_uid"],
	}
	if identity.PodUID == "" && identity.WorkloadUID == "" && values["ordinal"] == "" {
		identity = holderidentity.DeploymentIdentity{}
	} else {
		ordinal, err := parseCanonicalSafeInt(values["ordinal"])
		if err != nil || ordinal < 0 || ordinal > int64(^uint32(0)>>1) {
			return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: deployment ordinal", ErrHolderStoreCorrupt)
		}
		identity.Ordinal = int32(ordinal)
		if err := identity.ValidateExact(); err != nil {
			return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: deployment identity", ErrHolderStoreCorrupt)
		}
	}
	processReady, err := parseBool(values["process_ready"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, err
	}
	draining, err := parseBool(values["draining"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, err
	}
	serving, err := parseUint32(values["serving_shard_count"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, err
	}
	snapshot, err := parseCanonicalSafeUint(values["snapshot_revision"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: snapshot revision", ErrHolderStoreCorrupt)
	}
	started, err := parseCanonicalSafeInt(values["started_at_ms"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: started time", ErrHolderStoreCorrupt)
	}
	heartbeat, err := parseCanonicalSafeInt(values["heartbeat_at_ms"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: heartbeat time", ErrHolderStoreCorrupt)
	}
	lease, err := parseCanonicalSafeInt(values["lease_until_ms"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: lease time", ErrHolderStoreCorrupt)
	}
	sequence, err := parseCanonicalSafeUint(values["sequence"])
	if err != nil || sequence == 0 {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: sequence", ErrHolderStoreCorrupt)
	}
	weight, err := parseUint32(values["capacity_weight"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, err
	}
	resourceRaw := values["resource_summary"]
	summary, err := decodeHolderResourceSummary(resourceRaw)
	if err != nil {
		return core.HolderRecord{}, 0, 0, err
	}
	recordBytes, err := parseCanonicalPositiveInt(values["record_bytes"])
	if err != nil {
		return core.HolderRecord{}, 0, 0, fmt.Errorf("%w: record bytes", ErrHolderStoreCorrupt)
	}
	record := core.HolderRecord{
		Domain: actorrouting.RouteDomain(values["domain"]), HolderID: values["holder_id"],
		BootID: values["boot_id"], DeploymentIdentity: identity,
		LogicEndpoint:         values["logic_endpoint"],
		HolderControlEndpoint: values["holder_control_endpoint"],
		TLSDNSIdentity:        values["tls_dns_identity"], State: core.HolderState(values["state"]),
		ProcessReady: processReady, Draining: draining, ServingShardCount: serving,
		SnapshotRevision: snapshot, StartedAt: time.UnixMilli(started),
		HeartbeatAt: time.UnixMilli(heartbeat), LeaseUntil: time.UnixMilli(lease),
		Sequence: sequence, CapacityWeight: weight, ResourceSummary: summary,
	}
	return record, recordBytes, len(resourceRaw), nil
}

func normalizeRegistration(record core.HolderRegistration, now time.Time, ttl time.Duration) core.HolderRegistration {
	if record.State == "" {
		record.State = core.HolderStateWarming
	}
	if record.StartedAt.IsZero() {
		record.StartedAt = now
	}
	if record.HeartbeatAt.IsZero() {
		record.HeartbeatAt = now
	}
	if record.LeaseUntil.IsZero() {
		record.LeaseUntil = record.HeartbeatAt.Add(ttl)
	}
	if record.Sequence == 0 {
		record.Sequence = 1
	}
	if record.CapacityWeight == 0 {
		record.CapacityWeight = 1
	}
	if record.ResourceSummary == nil {
		record.ResourceSummary = nil
	}
	return record
}

func normalizeHeartbeat(input core.HolderHeartbeat, now time.Time, current core.HolderRecord, ttl time.Duration) core.HolderHeartbeat {
	if input.State == "" {
		input.State = current.State
	}
	if input.HeartbeatAt.IsZero() {
		input.HeartbeatAt = now
	}
	if input.LeaseUntil.IsZero() {
		input.LeaseUntil = input.HeartbeatAt.Add(ttl)
	}
	if input.Sequence == 0 {
		if current.Sequence >= maxLuaSafeUint {
			return input
		}
		input.Sequence = current.Sequence + 1
	}
	return input
}

func validateHolderKeyComponent(value string, max int) error {
	if value == "" || strings.TrimSpace(value) != value || len([]byte(value)) > max ||
		strings.ContainsRune(value, '\x00') {
		return ErrInvalidKey
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return ErrInvalidKey
		}
	}
	return nil
}

func parseHolderMember(member string) (actorrouting.RouteDomain, string, error) {
	parts := strings.SplitN(member, "\x00", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: malformed index member", ErrHolderStoreCorrupt)
	}
	domain := actorrouting.RouteDomain(parts[0])
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return "", "", fmt.Errorf("%w: index domain=%q", ErrHolderStoreCorrupt, domain)
	}
	if err := validateHolderKeyComponent(parts[1], core.DefaultHolderMaxIDBytes); err != nil {
		return "", "", fmt.Errorf("%w: index holder id", ErrHolderStoreCorrupt)
	}
	return domain, parts[1], nil
}

func boolArg(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func parseBool(value string) (bool, error) {
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("%w: boolean=%q", ErrHolderStoreCorrupt, value)
	}
}

func parseUint32(value string) (uint32, error) {
	parsed, err := parseCanonicalSafeUint(value)
	if err != nil || parsed > math.MaxUint32 {
		return 0, fmt.Errorf("%w: uint32=%q", ErrHolderStoreCorrupt, value)
	}
	return uint32(parsed), nil
}

func parseNonNegativeInt(value string) (int, error) {
	parsed, err := parseCanonicalSafeUint(value)
	if err != nil || parsed > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%w: integer=%q", ErrHolderStoreCorrupt, value)
	}
	return int(parsed), nil
}

func parseCanonicalSafeUint(value string) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("%w: noncanonical integer=%q", ErrHolderStoreCorrupt, value)
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("%w: noncanonical integer=%q", ErrHolderStoreCorrupt, value)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed > maxLuaSafeUint {
		return 0, fmt.Errorf("%w: integer=%q", ErrHolderStoreCorrupt, value)
	}
	return parsed, nil
}

func parseCanonicalSafeInt(value string) (int64, error) {
	parsed, err := parseCanonicalSafeUint(value)
	if err != nil {
		return 0, err
	}
	return int64(parsed), nil
}

func parseCanonicalPositiveInt(value string) (int, error) {
	parsed, err := parseCanonicalSafeUint(value)
	if err != nil || parsed == 0 || parsed > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%w: positive integer=%q", ErrHolderStoreCorrupt, value)
	}
	return int(parsed), nil
}

func validateHolderRedisClient(client redis.UniversalClient) error {
	switch typed := client.(type) {
	case *redis.Client:
		if typed != nil {
			return nil
		}
	case *redis.ClusterClient:
		if typed != nil {
			return nil
		}
	}
	if client == nil {
		return ErrInvalidOptions
	}
	return fmt.Errorf("%w: unsupported Redis client type %T", ErrInvalidOptions, client)
}

func holderDurationMillis(name string, value time.Duration) (int64, error) {
	if value < time.Millisecond {
		return 0, fmt.Errorf("%w: %s must be at least 1ms", ErrInvalidOptions, name)
	}
	millis := value / time.Millisecond
	if millis <= 0 || int64(millis) > maxLuaSafeInteger {
		return 0, fmt.Errorf("%w: %s milliseconds outside Lua safe range", ErrInvalidOptions, name)
	}
	return int64(millis), nil
}

func validateLuaSafeHolderRecord(record core.HolderRecord) error {
	if record.SnapshotRevision > maxLuaSafeUint || record.Sequence > maxLuaSafeUint {
		return fmt.Errorf("%w: holder uint64 exceeds Lua safe integer", ErrHolderStoreInvalid)
	}
	for name, value := range map[string]int64{
		"started_at_ms":   record.StartedAt.UnixMilli(),
		"heartbeat_at_ms": record.HeartbeatAt.UnixMilli(),
		"lease_until_ms":  record.LeaseUntil.UnixMilli(),
	} {
		if value < 0 || value > maxLuaSafeInteger {
			return fmt.Errorf("%w: %s outside Lua safe integer", ErrHolderStoreInvalid, name)
		}
	}
	return nil
}

func cloneHolderStoreRecord(record core.HolderRecord) core.HolderRecord {
	if len(record.ResourceSummary) == 0 {
		record.ResourceSummary = nil
		return record
	}
	summary := record.ResourceSummary
	record.ResourceSummary = make(map[string]string, len(summary))
	for key, value := range summary {
		record.ResourceSummary[key] = value
	}
	return record
}

// ReleasePlacementExclusionForOwner reverses an exclusion owned by the named
// plan. The holder and placement records are loaded and the true-to-false CAS
// is committed by one Lua script, so a boot rollover can never be inferred
// from a separate read.
func (s *HolderStore) ReleasePlacementExclusionForOwner(ctx context.Context, token core.NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release core.PlacementExclusionRelease) (core.PlacementExclusionResult, error) {
	if s == nil || s.client == nil || s.scripts == nil {
		return core.PlacementExclusionResult{}, ErrInvalidOptions
	}
	if !token.Issued() {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: release requires an issued control token", core.ErrPlacementExclusionInvalid)
	}
	if release.ExpectedRevision == 0 || release.OwnerPlanID == "" {
		return core.PlacementExclusionResult{}, core.ErrPlacementExclusionInvalid
	}
	holderKey, _, err := s.recordAndMember(domain, holderID)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	placementKey, err := s.keys.PlacementRecord(domain, holderID)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if err := s.keys.AssertSameSlot(holderKey, placementKey); err != nil {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement key slots: %v", ErrScriptContract, err)
	}
	opCtx, cancel, err := s.operationContext(ctx)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	defer cancel()
	registry := s.registryOptions()
	reply, err := s.scripts.run(opCtx, ScriptReleasePlacementExclusion,
		[]string{holderKey.String(), placementKey.String()}, s.keys.HashTag(), string(domain), holderID, bootID,
		release.OwnerPlanID, strconv.FormatUint(release.ExpectedRevision, 10),
		registry.MaxIDBytes, registry.MaxEndpointBytes, registry.MaxTLSIdentityBytes,
		s.opts.MaxRecordBytes, s.opts.MaxResourceBytes, registry.MaxServingShardCount, registry.MaxCapacityWeight,
		s.opts.MaxLeaseTTL.Milliseconds(), s.opts.ClockSkew.Milliseconds())
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if len(reply) == 0 {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: empty placement release reply", ErrScriptContract)
	}
	status := reply[0]
	result := core.PlacementExclusionResult{}
	switch status {
	case "COMMITTED":
		result.Committed = true
		result.Outcome = core.PlacementReleaseReleased
	case "ALREADY_RELEASED":
		result.Outcome = core.PlacementReleaseAlreadyReleased
	case "BOOT_MISMATCH":
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseByBootRollover}, nil
	case "NOT_FOUND":
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseNotFound}, nil
	case "OWNER_CONFLICT":
		result.Outcome = core.PlacementReleaseOwnerMismatch
	case "CONFLICT":
		result.Outcome = core.PlacementReleaseRevisionConflict
	case "MALFORMED":
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseMalformed}, nil
	case "REVISION_EXHAUSTED":
		return core.PlacementExclusionResult{}, core.ErrPlacementExclusionUnavailable
	case "CROSS_SLOT":
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement CROSS_SLOT", ErrScriptContract)
	default:
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement release status=%q", ErrScriptContract, status)
	}
	if len(reply) != 7 {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement release reply fields=%d", ErrScriptContract, len(reply))
	}
	state, found, err := decodePlacementExclusionReply(append([]string{"FOUND"}, reply[1:]...))
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if !found {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: placement release state missing", ErrHolderStoreCorrupt)
	}
	result.State = state
	return result, nil
}
