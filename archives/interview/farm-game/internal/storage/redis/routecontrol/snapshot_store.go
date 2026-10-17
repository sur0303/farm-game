package routecontrol

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/rediskey"
)

var (
	ErrBootstrapExists = core.ErrSnapshotCASConflict
)

// SnapshotStore is the Redis implementation of the routecontrol logical
// snapshot store. The snapshot is one fixed-field HASH, never a partial patch.
type SnapshotStore struct {
	client   redis.UniversalClient
	registry *scriptRegistry
	keys     KeyBuilderV1
	opts     Options
}

var _ core.RouteSnapshotStore = (*SnapshotStore)(nil)
var _ core.RouteSnapshotBootstrapper = (*SnapshotStore)(nil)
var _ core.InitialAuthorityBatchStore = (*SnapshotStore)(nil)

func NewSnapshotStore(client redis.UniversalClient, opts Options) (*SnapshotStore, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	keys, err := NewKeyBuilderV1(opts.Namespace)
	if err != nil {
		return nil, err
	}
	registry, err := newScriptRegistry(client)
	if err != nil {
		return nil, err
	}
	return &SnapshotStore{client: client, registry: registry, keys: keys, opts: opts}, nil
}

func NewStore(client redis.UniversalClient, opts Options) (*SnapshotStore, error) {
	return NewSnapshotStore(client, opts)
}

func NewRedisSnapshotStore(client redis.UniversalClient, opts Options) (*SnapshotStore, error) {
	return NewSnapshotStore(client, opts)
}

func (s *SnapshotStore) Client() redis.UniversalClient {
	if s == nil {
		return nil
	}
	return s.client
}

func NewSnapshotStoreWithRegistry(client redis.UniversalClient, registry *scriptRegistry, opts Options) (*SnapshotStore, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	keys, err := NewKeyBuilderV1(opts.Namespace)
	if err != nil {
		return nil, err
	}
	if client == nil || registry == nil {
		return nil, ErrInvalidOptions
	}
	return &SnapshotStore{client: client, registry: registry, keys: keys, opts: opts}, nil
}

func (s *SnapshotStore) Keys() KeyBuilderV1 {
	if s == nil {
		return KeyBuilderV1{}
	}
	return s.keys
}

func (s *SnapshotStore) Registry() *scriptRegistry {
	if s == nil {
		return nil
	}
	return s.registry
}

func (s *SnapshotStore) ScriptSHA(name string) (string, bool) {
	if s == nil || s.registry == nil {
		return "", false
	}
	return s.registry.scriptSHA(name)
}

func (s *SnapshotStore) EnsureLoaded(ctx context.Context) error {
	if s == nil || s.registry == nil {
		return ErrInvalidOptions
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return s.registry.ensureLoaded(opCtx)
}

func (s *SnapshotStore) LoadCurrent(ctx context.Context) (core.StoredRouteSnapshot, bool, error) {
	if s == nil {
		return core.StoredRouteSnapshot{}, false, ErrInvalidOptions
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return core.StoredRouteSnapshot{}, false, err
	}
	defer cancel()
	return s.loadCurrent(opCtx)
}

func (s *SnapshotStore) loadCurrent(ctx context.Context) (core.StoredRouteSnapshot, bool, error) {
	snapshotKey := s.keys.Snapshot()
	kind, err := s.client.Type(ctx, snapshotKey.String()).Result()
	if err != nil {
		return core.StoredRouteSnapshot{}, false, err
	}
	if kind == "none" {
		fenceKind, fenceErr := s.client.Type(ctx, snapshotBootstrapFenceKey(s.keys).String()).Result()
		if fenceErr != nil {
			return core.StoredRouteSnapshot{}, false, fenceErr
		}
		if fenceKind != "none" {
			return core.StoredRouteSnapshot{}, false, fmt.Errorf("%w: dangling bootstrap fence", ErrMalformedRecord)
		}
		return core.StoredRouteSnapshot{}, false, nil
	}
	if kind != "hash" {
		return core.StoredRouteSnapshot{}, true, fmt.Errorf("%w: snapshot key type=%s", ErrMalformedRecord, kind)
	}
	if err := s.validateBootstrapFence(ctx); err != nil {
		return core.StoredRouteSnapshot{}, true, err
	}
	if count, err := s.client.HLen(ctx, snapshotKey.String()).Result(); err != nil {
		return core.StoredRouteSnapshot{}, true, err
	} else if count != 7 {
		return core.StoredRouteSnapshot{}, true, fmt.Errorf("%w: snapshot field count=%d", ErrMalformedRecord, count)
	}
	keyNames, err := s.client.HKeys(ctx, snapshotKey.String()).Result()
	if err != nil {
		return core.StoredRouteSnapshot{}, true, err
	}
	expectedKeys := map[string]struct{}{
		snapshotFieldSchema: {}, snapshotFieldRealm: {}, snapshotFieldRevision: {},
		snapshotFieldJSON: {}, snapshotFieldChecksum: {}, snapshotFieldTopology: {},
		snapshotFieldCommitted: {},
	}
	if len(keyNames) != len(expectedKeys) {
		return core.StoredRouteSnapshot{}, true, fmt.Errorf("%w: snapshot field count=%d", ErrMalformedRecord, len(keyNames))
	}
	for _, keyName := range keyNames {
		if _, ok := expectedKeys[keyName]; !ok {
			return core.StoredRouteSnapshot{}, true, fmt.Errorf("%w: snapshot unexpected field=%q", ErrMalformedRecord, keyName)
		}
	}
	values, err := s.client.HMGet(ctx, snapshotKey.String(),
		snapshotFieldSchema, snapshotFieldRealm, snapshotFieldRevision,
		snapshotFieldJSON, snapshotFieldChecksum, snapshotFieldTopology,
		snapshotFieldCommitted).Result()
	if err != nil {
		return core.StoredRouteSnapshot{}, true, err
	}
	fields, err := snapshotFieldsFromRedis(values)
	if err != nil {
		return core.StoredRouteSnapshot{}, true, err
	}
	if len(fields.CanonicalJSON) > s.opts.MaxSnapshotBytes {
		return core.StoredRouteSnapshot{}, true, fmt.Errorf("%w: snapshot bytes=%d limit=%d", core.ErrSnapshotCapacity, len(fields.CanonicalJSON), s.opts.MaxSnapshotBytes)
	}
	stored, err := core.DecodeStoredRouteSnapshot(fields)
	if err != nil {
		return core.StoredRouteSnapshot{}, true, err
	}
	return stored, true, nil
}

func (s *SnapshotStore) Bootstrap(ctx context.Context, token core.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) (core.SnapshotCASResult, error) {
	if s == nil {
		return core.SnapshotCASResult{}, ErrInvalidOptions
	}
	candidate, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if candidate.Revision != 1 {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: bootstrap revision=%d", core.ErrSnapshotCASConflict, candidate.Revision)
	}
	if len(candidate.CanonicalJSON) > s.opts.MaxSnapshotBytes {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: snapshot bytes=%d limit=%d", core.ErrSnapshotCapacity, len(candidate.CanonicalJSON), s.opts.MaxSnapshotBytes)
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	defer cancel()
	current, found, loadErr := s.loadCurrent(opCtx)
	if loadErr != nil {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: existing envelope: %v", core.ErrSnapshotCASConflict, loadErr)
	}
	tag := s.keys.HashTag()
	if err := s.keys.AssertSameSlot(s.keys.Snapshot(), snapshotBootstrapFenceKey(s.keys)); err != nil {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	expectedExists := "0"
	expected := core.SnapshotEnvelopeFields{}
	if found {
		expectedExists = "1"
		expected = current.EnvelopeFields()
	}
	now := s.nowMs()
	if err := opCtx.Err(); err != nil {
		return core.SnapshotCASResult{}, err
	}
	fenceOperation, fenceEpoch := bootstrapFenceValues(token)
	reply, err := s.registry.run(opCtx, ScriptBootstrapSnapshot, []string{s.keys.Snapshot().String(), snapshotBootstrapFenceKey(s.keys).String()},
		tag, candidate.SchemaVersion, candidate.Realm, decimalUint64(candidate.Revision),
		string(candidate.CanonicalJSON), candidate.ChecksumSHA256, candidate.TopologyFingerprint,
		strconv.FormatInt(now, 10), strconv.Itoa(s.opts.MaxSnapshotBytes), expectedExists,
		expected.SchemaVersion, expected.Realm, expected.Revision, string(expected.CanonicalJSON),
		expected.ChecksumSHA256, expected.TopologyFingerprint, expected.CommittedAtMs,
		snapshotBootstrapFenceSchema, fenceOperation, fenceEpoch)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if isCrossSlotReply(reply) {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	if len(reply) == 0 {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: empty bootstrap reply", ErrScriptContract)
	}
	switch reply[0] {
	case "COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASCommitted, Stored: stored}, nil
	case "ALREADY_COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: stored}, nil
	case "CONFLICT", "CURRENT_CHANGED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, core.ErrSnapshotCASConflict
	case "MALFORMED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: existing envelope", core.ErrSnapshotCASConflict)
	case "BAD_REVISION":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: bootstrap revision", ErrScriptContract)
	case "CAPACITY_EXCEEDED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: %v", core.ErrSnapshotCapacity, ErrCapacityExceeded)
	default:
		return core.SnapshotCASResult{}, fmt.Errorf("%w: bootstrap status %q", ErrScriptContract, reply[0])
	}
}

func (s *SnapshotStore) CompareAndSwapInitialAuthorityBatch(ctx context.Context, token core.NestedControlOperationToken, capability core.InitialAuthorityBatchCapability) (core.SnapshotCASResult, error) {
	if s == nil {
		return core.SnapshotCASResult{}, ErrInvalidOptions
	}
	if !token.Issued() {
		return core.SnapshotCASResult{}, core.ErrInitialAuthorityBatchTokenRequired
	}
	expected, nextSnapshot, records, validatedAt, err := core.InitialAuthorityBatchValues(capability)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if err := core.ValidateInitialAuthorityBatchAt(expected, nextSnapshot, records, "", validatedAt); err != nil {
		return core.SnapshotCASResult{}, err
	}
	candidate, err := core.CanonicalizeRouteSnapshot(nextSnapshot)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if len(candidate.CanonicalJSON) > s.opts.MaxSnapshotBytes {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: snapshot bytes=%d limit=%d", core.ErrSnapshotCapacity, len(candidate.CanonicalJSON), s.opts.MaxSnapshotBytes)
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return core.SnapshotCASResult{}, err
	}
	if err := s.keys.AssertSameSlot(s.keys.Snapshot()); err != nil {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	expectedFields := expected.EnvelopeFields()
	now := s.nowMs()
	reply, err := s.registry.run(opCtx, ScriptCompareAndSwapInitialAuthorityBatch, []string{s.keys.Snapshot().String()},
		s.keys.HashTag(), expectedFields.SchemaVersion, expectedFields.Realm, expectedFields.Revision,
		string(expectedFields.CanonicalJSON), expectedFields.ChecksumSHA256, expectedFields.TopologyFingerprint,
		expectedFields.CommittedAtMs, candidate.SchemaVersion, candidate.Realm, decimalUint64(candidate.Revision),
		string(candidate.CanonicalJSON), candidate.ChecksumSHA256, candidate.TopologyFingerprint,
		strconv.FormatInt(now, 10), strconv.Itoa(s.opts.MaxSnapshotBytes))
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if isCrossSlotReply(reply) {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	if len(reply) == 0 {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: empty initial authority batch reply", ErrScriptContract)
	}
	switch reply[0] {
	case "COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASCommitted, Stored: stored}, nil
	case "ALREADY_COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: stored}, nil
	case "CURRENT_CHANGED":
		return core.SnapshotCASResult{Status: core.SnapshotCASStaleExpectation}, core.ErrSnapshotCASConflict
	case "REVISION_CONFLICT":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, nil
	case "BOOTSTRAP_REQUIRED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, core.ErrSnapshotBootstrapRequired
	case "MALFORMED":
		return core.SnapshotCASResult{}, ErrMalformedRecord
	case "CAPACITY_EXCEEDED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: %v", core.ErrSnapshotCapacity, ErrCapacityExceeded)
	default:
		return core.SnapshotCASResult{}, fmt.Errorf("%w: initial authority batch status %q", ErrScriptContract, reply[0])
	}
}

func (s *SnapshotStore) CompareAndSwap(ctx context.Context, _ core.NestedControlOperationToken, expectedRevision uint64, snapshot actorrouting.RouteSnapshot) (core.SnapshotCASResult, error) {
	if s == nil {
		return core.SnapshotCASResult{}, ErrInvalidOptions
	}
	candidate, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if len(candidate.CanonicalJSON) > s.opts.MaxSnapshotBytes {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: snapshot bytes=%d limit=%d", core.ErrSnapshotCapacity, len(candidate.CanonicalJSON), s.opts.MaxSnapshotBytes)
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	defer cancel()
	current, found, loadErr := s.loadCurrent(opCtx)
	if loadErr != nil {
		return core.SnapshotCASResult{}, loadErr
	}
	if expectedRevision == 0 {
		if candidate.Revision != 1 {
			return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, nil
		}
	} else if candidate.Revision != expectedRevision {
		if expectedRevision == math.MaxUint64 || candidate.Revision != expectedRevision+1 {
			return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, nil
		}
	}
	tag := s.keys.HashTag()
	if err := s.keys.AssertSameSlot(s.keys.Snapshot()); err != nil {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	strictNext := ""
	if expectedRevision > 0 && expectedRevision != math.MaxUint64 && candidate.Revision == expectedRevision+1 {
		strictNext = "STRICT_NEXT"
	}
	expectedExists := "0"
	expected := core.SnapshotEnvelopeFields{}
	if found {
		expectedExists = "1"
		expected = current.EnvelopeFields()
	}
	now := s.nowMs()
	if err := opCtx.Err(); err != nil {
		return core.SnapshotCASResult{}, err
	}
	reply, err := s.registry.run(opCtx, ScriptCompareAndSwapSnapshot, []string{s.keys.Snapshot().String()},
		tag, expectedExists, expected.SchemaVersion, expected.Realm, expected.Revision,
		string(expected.CanonicalJSON), expected.ChecksumSHA256, expected.TopologyFingerprint,
		expected.CommittedAtMs, decimalUint64(expectedRevision), decimalUint64(candidate.Revision),
		candidate.SchemaVersion, candidate.Realm, string(candidate.CanonicalJSON),
		candidate.ChecksumSHA256, candidate.TopologyFingerprint,
		strconv.FormatInt(now, 10), strictNext, strconv.Itoa(s.opts.MaxSnapshotBytes))
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if isCrossSlotReply(reply) {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	if len(reply) == 0 {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: empty CAS reply", ErrScriptContract)
	}
	switch reply[0] {
	case "COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASCommitted, Stored: stored}, nil
	case "ALREADY_COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: stored}, nil
	case "STALE_EXPECTATION", "REVISION_CONFLICT", "CURRENT_CHANGED":
		current, found, loadErr := s.loadCurrent(opCtx)
		if loadErr != nil {
			return core.SnapshotCASResult{}, loadErr
		}
		if !found {
			return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, core.ErrSnapshotCASConflict
		}
		if reply[0] == "CURRENT_CHANGED" {
			if candidate.Revision == current.Revision && candidate.EnvelopeEqual(current) {
				return core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: current}, nil
			}
			if candidate.Revision == current.Revision {
				return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict, Stored: current}, nil
			}
			if expectedRevision != current.Revision {
				return core.SnapshotCASResult{Status: core.SnapshotCASStaleExpectation, Stored: current}, nil
			}
			return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict, Stored: current}, nil
		}
		status := core.SnapshotCASStaleExpectation
		if reply[0] == "REVISION_CONFLICT" {
			status = core.SnapshotCASRevisionConflict
		}
		return core.SnapshotCASResult{Status: status, Stored: current}, nil
	case "BOOTSTRAP_REQUIRED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, core.ErrSnapshotBootstrapRequired
	case "MALFORMED":
		return core.SnapshotCASResult{}, ErrMalformedRecord
	case "CAPACITY_EXCEEDED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: %v", core.ErrSnapshotCapacity, ErrCapacityExceeded)
	default:
		return core.SnapshotCASResult{}, fmt.Errorf("%w: CAS status %q", ErrScriptContract, reply[0])
	}
}

func (s *SnapshotStore) context(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, core.ErrSnapshotContextRequired
	}
	timeout := s.opts.OperationTimeout
	if timeout <= 0 {
		return ctx, func() {}, nil
	}
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	return opCtx, cancel, nil
}

func (s *SnapshotStore) nowMs() int64 {
	now := s.opts.Now()
	if now.Before(time.UnixMilli(0)) {
		return 0
	}
	return now.UnixMilli()
}

func snapshotBootstrapFenceKey(keys KeyBuilderV1) rediskey.RedisKey {
	return keys.key("snapshot-bootstrap-fence")
}

func bootstrapFenceValues(token core.NestedControlOperationToken) (string, string) {
	if !token.Issued() {
		return "", "0"
	}
	return token.OperationID(), strconv.FormatUint(token.Epoch(), 10)
}

func (s *SnapshotStore) validateBootstrapFence(ctx context.Context) error {
	key := snapshotBootstrapFenceKey(s.keys)
	kind, err := s.client.Type(ctx, key.String()).Result()
	if err != nil {
		return err
	}
	if kind == "none" {
		return nil
	}
	if kind != "hash" {
		return fmt.Errorf("%w: bootstrap fence type=%s", ErrMalformedRecord, kind)
	}
	count, err := s.client.HLen(ctx, key.String()).Result()
	if err != nil {
		return err
	}
	if count != 3 {
		return fmt.Errorf("%w: bootstrap fence field count=%d", ErrMalformedRecord, count)
	}
	fields, err := s.client.HKeys(ctx, key.String()).Result()
	if err != nil {
		return err
	}
	expected := map[string]struct{}{
		snapshotFenceFieldSchema: {}, snapshotFenceFieldOperation: {}, snapshotFenceFieldEpoch: {},
	}
	if len(fields) != len(expected) {
		return fmt.Errorf("%w: bootstrap fence field count=%d", ErrMalformedRecord, len(fields))
	}
	for _, field := range fields {
		if _, ok := expected[field]; !ok {
			return fmt.Errorf("%w: bootstrap fence unexpected field=%q", ErrMalformedRecord, field)
		}
	}
	values, err := s.client.HMGet(ctx, key.String(), snapshotFenceFieldSchema,
		snapshotFenceFieldOperation, snapshotFenceFieldEpoch).Result()
	if err != nil {
		return err
	}
	if len(values) != 3 || values[0] == nil || values[1] == nil || values[2] == nil {
		return fmt.Errorf("%w: bootstrap fence missing field", ErrMalformedRecord)
	}
	schema, okSchema := values[0].(string)
	operation, okOperation := values[1].(string)
	epoch, okEpoch := values[2].(string)
	if !okSchema || !okOperation || !okEpoch || schema != snapshotBootstrapFenceSchema ||
		len(operation) > 256 || len(epoch) < 1 || len(epoch) > 20 ||
		(epoch != "0" && (epoch[0] == '0' || !allASCIIDigits(epoch))) ||
		(operation == "" && epoch != "0") {
		return fmt.Errorf("%w: bootstrap fence values", ErrMalformedRecord)
	}
	return nil
}

func allASCIIDigits(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func snapshotFieldsFromRedis(values []interface{}) (core.SnapshotEnvelopeFields, error) {
	if len(values) != 7 {
		return core.SnapshotEnvelopeFields{}, fmt.Errorf("%w: snapshot field count=%d", ErrMalformedRecord, len(values))
	}
	fields := make([]string, len(values))
	for i, value := range values {
		switch typed := value.(type) {
		case string:
			fields[i] = typed
		case []byte:
			fields[i] = string(typed)
		case nil:
			return core.SnapshotEnvelopeFields{}, fmt.Errorf("%w: snapshot field %d missing", ErrMalformedRecord, i)
		default:
			return core.SnapshotEnvelopeFields{}, fmt.Errorf("%w: snapshot field %d type=%T", ErrMalformedRecord, i, value)
		}
	}
	return core.SnapshotEnvelopeFields{
		SchemaVersion:       fields[0],
		Realm:               fields[1],
		Revision:            fields[2],
		CanonicalJSON:       []byte(fields[3]),
		ChecksumSHA256:      fields[4],
		TopologyFingerprint: fields[5],
		CommittedAtMs:       fields[6],
	}, nil
}

func decodeReplyStored(reply []string) (core.StoredRouteSnapshot, error) {
	if len(reply) != 8 {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: snapshot script reply fields=%d", ErrScriptContract, len(reply))
	}
	return core.DecodeStoredRouteSnapshot(core.SnapshotEnvelopeFields{
		SchemaVersion:       reply[1],
		Realm:               reply[2],
		Revision:            reply[3],
		CanonicalJSON:       []byte(reply[4]),
		ChecksumSHA256:      reply[5],
		TopologyFingerprint: reply[6],
		CommittedAtMs:       reply[7],
	})
}

// Keep the Redis key type in this package's public surface for tests and
// diagnostics without allowing callers to construct it themselves.
func SnapshotRedisKey(store *SnapshotStore) rediskey.RedisKey {
	if store == nil {
		return ""
	}
	return store.keys.Snapshot()
}
