package routecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/storage/redis/rediskey"
)

const transitionFingerprintSeparator = "\x00"

// TransitionStore is the durable adapter for the existing manager-owned
// TransitionRecoveryStore and TransitionTombstoneStore seams. It contains no
// transition state machine; it only persists and replays the supplied records.
type TransitionStore struct {
	client   redis.UniversalClient
	registry *scriptRegistry
	keys     KeyBuilderV1
	opts     Options
}

type transitionPhysicalState struct {
	recoveries map[string]authority.TransitionRecovery
	tombstones map[string]authority.TransitionTombstone
	items      int64
	bytes      int64
}

var _ authority.TransitionTombstoneStore = (*TransitionStore)(nil)
var _ authority.TransitionRecoveryStore = (*TransitionStore)(nil)
var _ authority.ExactTransitionEvidenceStore = (*TransitionStore)(nil)

func NewTransitionStore(client redis.UniversalClient, opts Options) (*TransitionStore, error) {
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
	return &TransitionStore{client: client, registry: registry, keys: keys, opts: opts}, nil
}

func NewDurableTransitionStore(client redis.UniversalClient, opts Options) (*TransitionStore, error) {
	return NewTransitionStore(client, opts)
}

func NewRedisTransitionStore(client redis.UniversalClient, opts Options) (*TransitionStore, error) {
	return NewTransitionStore(client, opts)
}

func (s *TransitionStore) Keys() KeyBuilderV1 {
	if s == nil {
		return KeyBuilderV1{}
	}
	return s.keys
}

func (s *TransitionStore) Client() redis.UniversalClient {
	if s == nil {
		return nil
	}
	return s.client
}

func (s *TransitionStore) ScriptSHA(name string) (string, bool) {
	if s == nil || s.registry == nil {
		return "", false
	}
	return s.registry.scriptSHA(name)
}

func (s *TransitionStore) EnsureLoaded() error {
	if s == nil || s.registry == nil {
		return ErrInvalidOptions
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	return s.registry.ensureLoaded(ctx)
}

func (s *TransitionStore) LoadTransitionRecoveries(ctx context.Context) ([]authority.TransitionRecovery, error) {
	if s == nil {
		return nil, ErrInvalidOptions
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	state, err := s.validatePhysicalState(opCtx)
	if err != nil {
		return nil, err
	}
	entries := make([]authority.TransitionRecovery, 0, len(state.recoveries))
	for _, entry := range state.recoveries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].UpdatedAt.Equal(entries[j].UpdatedAt) {
			return entries[i].TransitionID < entries[j].TransitionID
		}
		return entries[i].UpdatedAt.Before(entries[j].UpdatedAt)
	})
	return entries, nil
}

func (s *TransitionStore) LookupTransitionRecovery(ctx context.Context, id string) (authority.TransitionRecovery, bool, error) {
	if err := s.validateID(id); err != nil {
		return authority.TransitionRecovery{}, false, err
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return authority.TransitionRecovery{}, false, err
	}
	defer cancel()
	state, err := s.validatePhysicalState(opCtx)
	if err != nil {
		return authority.TransitionRecovery{}, false, err
	}
	entry, found := state.recoveries[id]
	if !found {
		return authority.TransitionRecovery{}, false, nil
	}
	return entry, true, nil
}

// ObserveTransitionEvidence is the exact-ID read-only verification path. Its
// caller context and the store operation timeout are both enforced.
func (s *TransitionStore) ObserveTransitionEvidence(ctx context.Context, id string) (authority.TransitionRecovery, bool, authority.TransitionTombstone, bool, error) {
	if s == nil || ctx == nil {
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, ErrInvalidOptions
	}
	if err := s.validateID(id); err != nil {
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, err
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, err
	}
	defer cancel()
	reply, err := s.runTransition(opCtx, ScriptObserveTransitionEvidence, id,
		strconv.Itoa(s.opts.MaxTransitionItems), strconv.Itoa(s.opts.MaxTransitionBytes),
		strconv.Itoa(s.opts.MaxTransitionRecordSize))
	if err != nil {
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, err
	}
	if len(reply) != 3 || reply[0] != "OK" {
		if firstReply(reply) == "MALFORMED" {
			return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false,
				fmt.Errorf("%w: observe: %w", authority.ErrTransitionReconcile, ErrMalformedRecord)
		}
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, fmt.Errorf("%w: observe reply", ErrScriptContract)
	}
	switch reply[1] {
	case "MISSING":
		if reply[2] != "" {
			return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, ErrMalformedRecord
		}
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, nil
	case "RECOVERY":
		entry, decodeErr := decodeRecovery(id, reply[2])
		if decodeErr != nil {
			return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, decodeErr
		}
		return entry, true, authority.TransitionTombstone{}, false, nil
	case "TOMBSTONE":
		entry, decodeErr := decodeTombstone(id, reply[2])
		if decodeErr != nil {
			return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, decodeErr
		}
		return authority.TransitionRecovery{}, false, entry, true, nil
	default:
		return authority.TransitionRecovery{}, false, authority.TransitionTombstone{}, false, ErrMalformedRecord
	}
}

func (s *TransitionStore) ReserveTransitionRecovery(ctx context.Context, recovery authority.TransitionRecovery) error {
	if s == nil {
		return ErrInvalidOptions
	}
	if err := validateRecoveryIdentity(s.keys, recovery); err != nil {
		return err
	}
	recovery.Committed = false
	value, err := encodeRecovery(recovery)
	if err != nil {
		return err
	}
	if len(value) > s.opts.MaxTransitionRecordSize {
		return transitionCapacityError()
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.validatePhysicalState(opCtx); err != nil {
		return err
	}
	reply, err := s.runTransition(opCtx, ScriptReserveTransitionRecovery,
		recovery.TransitionID, recovery.Fingerprint, value,
		strconv.Itoa(s.opts.MaxTransitionItems), strconv.Itoa(s.opts.MaxTransitionBytes),
		strconv.Itoa(s.opts.MaxTransitionRecordSize),
		scoreFor(recovery.UpdatedAt))
	if err != nil {
		return err
	}
	switch firstReply(reply) {
	case "RESERVED", "ALREADY":
		return nil
	case "CONFLICT":
		return authority.ErrTransitionReconcile
	case "CAPACITY_EXCEEDED":
		return transitionCapacityError()
	case "MALFORMED":
		return malformedTransitionError("reserve")
	default:
		return fmt.Errorf("%w: reserve status %q", ErrScriptContract, firstReply(reply))
	}
}

func (s *TransitionStore) CommitTransitionRecovery(ctx context.Context, recovery authority.TransitionRecovery) error {
	if s == nil {
		return ErrInvalidOptions
	}
	if err := validateRecoveryIdentity(s.keys, recovery); err != nil {
		return err
	}
	if !recovery.Committed {
		return transitionCapacityError()
	}
	value, err := encodeRecovery(recovery)
	if err != nil {
		return err
	}
	if len(value) > s.opts.MaxTransitionRecordSize {
		return transitionCapacityError()
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.validatePhysicalState(opCtx); err != nil {
		return err
	}
	reply, err := s.runTransition(opCtx, ScriptCommitTransitionRecovery,
		recovery.TransitionID, recovery.Fingerprint, value,
		scoreFor(recovery.UpdatedAt), strconv.Itoa(s.opts.MaxTransitionItems),
		strconv.Itoa(s.opts.MaxTransitionBytes), strconv.Itoa(s.opts.MaxTransitionRecordSize))
	if err != nil {
		return err
	}
	switch firstReply(reply) {
	case "COMMITTED":
		return nil
	case "NOT_FOUND", "CONFLICT":
		return authority.ErrTransitionReconcile
	case "CAPACITY_EXCEEDED":
		return transitionCapacityError()
	case "MALFORMED":
		return malformedTransitionError("commit")
	default:
		return fmt.Errorf("%w: commit status %q", ErrScriptContract, firstReply(reply))
	}
}

func (s *TransitionStore) DeleteTransitionRecovery(ctx context.Context, id string) error {
	if s == nil {
		return ErrInvalidOptions
	}
	if err := s.validateID(id); err != nil {
		return err
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.validatePhysicalState(opCtx); err != nil {
		return err
	}
	reply, err := s.runTransition(opCtx, ScriptDeleteTransitionRecovery, id,
		strconv.Itoa(s.opts.MaxTransitionItems), strconv.Itoa(s.opts.MaxTransitionBytes),
		strconv.Itoa(s.opts.MaxTransitionRecordSize))
	if err != nil {
		return err
	}
	switch firstReply(reply) {
	case "DELETED":
		return nil
	case "MALFORMED":
		return malformedTransitionError("delete")
	default:
		return fmt.Errorf("%w: delete status %q", ErrScriptContract, firstReply(reply))
	}
}

func (s *TransitionStore) LoadTransitionTombstones(ctx context.Context) ([]authority.TransitionTombstone, error) {
	if s == nil {
		return nil, ErrInvalidOptions
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	state, err := s.validatePhysicalState(opCtx)
	if err != nil {
		return nil, err
	}
	entries := make([]authority.TransitionTombstone, 0, len(state.tombstones))
	for _, entry := range state.tombstones {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ExpiredAt.Equal(entries[j].ExpiredAt) {
			return entries[i].TransitionID < entries[j].TransitionID
		}
		return entries[i].ExpiredAt.Before(entries[j].ExpiredAt)
	})
	return entries, nil
}

func (s *TransitionStore) LookupTransitionTombstone(ctx context.Context, id string) (authority.TransitionTombstone, bool, error) {
	if s == nil {
		return authority.TransitionTombstone{}, false, ErrInvalidOptions
	}
	if err := s.validateID(id); err != nil {
		return authority.TransitionTombstone{}, false, err
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return authority.TransitionTombstone{}, false, err
	}
	defer cancel()
	state, err := s.validatePhysicalState(opCtx)
	if err != nil {
		return authority.TransitionTombstone{}, false, err
	}
	entry, found := state.tombstones[id]
	if !found {
		return authority.TransitionTombstone{}, false, nil
	}
	return entry, true, nil
}

func (s *TransitionStore) SaveTransitionTombstone(ctx context.Context, entry authority.TransitionTombstone) error {
	if s == nil {
		return ErrInvalidOptions
	}
	if err := s.keys.ValidateTransitionID(entry.TransitionID); err != nil {
		return err
	}
	if err := validateTransitionFingerprint(entry.Fingerprint); err != nil {
		return err
	}
	value, err := encodeTombstone(entry)
	if err != nil {
		return err
	}
	if len(value) > s.opts.MaxTransitionRecordSize {
		return transitionCapacityError()
	}
	opCtx, cancel, err := s.operationContextFrom(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err := s.validatePhysicalState(opCtx); err != nil {
		return err
	}
	reply, err := s.runTransition(opCtx, ScriptSaveTransitionTombstone,
		entry.TransitionID, entry.Fingerprint, value,
		strconv.Itoa(s.opts.MaxTransitionItems), strconv.Itoa(s.opts.MaxTransitionBytes),
		strconv.Itoa(s.opts.MaxTransitionRecordSize),
		scoreFor(entry.ExpiredAt))
	if err != nil {
		return err
	}
	switch firstReply(reply) {
	case "SAVED":
		return nil
	case "CONFLICT":
		return authority.ErrTransitionReconcile
	case "CAPACITY_EXCEEDED":
		return transitionCapacityError()
	case "MALFORMED":
		return malformedTransitionError("save tombstone")
	default:
		return fmt.Errorf("%w: tombstone status %q", ErrScriptContract, firstReply(reply))
	}
}

func (s *TransitionStore) runTransition(ctx context.Context, script, id string, args ...interface{}) ([]string, error) {
	if err := s.validateID(id); err != nil {
		return nil, err
	}
	keys := s.transitionKeys()
	if err := s.keys.AssertSameSlot(keys...); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	rawKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		rawKeys = append(rawKeys, key.String())
	}
	scriptArgs := make([]interface{}, 0, len(args)+2)
	scriptArgs = append(scriptArgs, s.keys.HashTag(), id)
	scriptArgs = append(scriptArgs, args...)
	reply, err := s.registry.run(ctx, script, rawKeys, scriptArgs...)
	if err != nil {
		return nil, err
	}
	if isCrossSlotReply(reply) {
		return nil, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	return reply, nil
}

func (s *TransitionStore) runTransitionSnapshot(ctx context.Context) ([]string, error) {
	keys := s.transitionKeys()
	if err := s.keys.AssertSameSlot(keys...); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	rawKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		rawKeys = append(rawKeys, key.String())
	}
	reply, err := s.registry.run(ctx, ScriptReadTransitionSnapshot, rawKeys,
		s.keys.HashTag(),
		strconv.Itoa(s.opts.MaxTransitionItems),
		strconv.Itoa(s.opts.MaxTransitionBytes),
		strconv.Itoa(s.opts.MaxTransitionRecordSize))
	if err != nil {
		return nil, err
	}
	if isCrossSlotReply(reply) {
		return nil, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	return reply, nil
}

func (s *TransitionStore) transitionKeys() []rediskey.RedisKey {
	return []rediskey.RedisKey{
		s.keys.Meta(),
		s.keys.Recoveries(),
		s.keys.RecoveryIndex(),
		s.keys.Tombstones(),
		s.keys.TombstoneIndex(),
	}
}

func (s *TransitionStore) operationContext() (context.Context, context.CancelFunc) {
	timeout := s.opts.OperationTimeout
	if timeout <= 0 {
		timeout = defaultOperationTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (s *TransitionStore) operationContextFrom(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	timeout := s.opts.OperationTimeout
	if timeout <= 0 {
		timeout = defaultOperationTimeout
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return bounded, cancel, nil
}

func (s *TransitionStore) validateID(id string) error {
	if s == nil || s.keys.ValidateTransitionID(id) != nil {
		return ErrInvalidKey
	}
	return nil
}

func (s *TransitionStore) validatePhysicalState(ctx context.Context) (transitionPhysicalState, error) {
	reply, err := s.runTransitionSnapshot(ctx)
	if err != nil {
		return transitionPhysicalState{}, err
	}
	return s.decodeTransitionSnapshot(reply)
}

func (s *TransitionStore) validatePhysicalCounts(ctx context.Context) error {
	_, err := s.validatePhysicalState(ctx)
	return err
}

func (s *TransitionStore) decodeTransitionSnapshot(reply []string) (transitionPhysicalState, error) {
	if len(reply) == 0 {
		return transitionPhysicalState{}, fmt.Errorf("%w: empty transition snapshot reply", ErrScriptContract)
	}
	if reply[0] == "MALFORMED" {
		return transitionPhysicalState{}, ErrMalformedRecord
	}
	if reply[0] != "OK" {
		return transitionPhysicalState{}, fmt.Errorf("%w: transition snapshot status %q", ErrScriptContract, reply[0])
	}

	cursor := 1
	next := func(name string) (string, error) {
		if cursor >= len(reply) {
			return "", fmt.Errorf("%w: transition snapshot missing %s", ErrScriptContract, name)
		}
		value := reply[cursor]
		cursor++
		return value, nil
	}
	parseCount := func(name string, maximum int64) (int64, error) {
		value, err := next(name)
		if err != nil {
			return 0, err
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 || parsed > maximum || strconv.FormatInt(parsed, 10) != value {
			return 0, fmt.Errorf("%w: transition snapshot %s=%q", ErrMalformedRecord, name, value)
		}
		return parsed, nil
	}
	readValues := func(name string, count int64) ([]string, error) {
		if count < 0 || count > int64(len(reply)-cursor) {
			return nil, fmt.Errorf("%w: transition snapshot %s count=%d", ErrMalformedRecord, name, count)
		}
		values := append([]string(nil), reply[cursor:cursor+int(count)]...)
		cursor += int(count)
		return values, nil
	}
	readHash := func(name string, fieldCount int64) (map[string]string, error) {
		rawCount, err := parseCount(name+" values", int64(len(reply)))
		if err != nil {
			return nil, err
		}
		if rawCount != fieldCount*2 {
			return nil, fmt.Errorf("%w: transition snapshot %s fields=%d values=%d",
				ErrMalformedRecord, name, fieldCount, rawCount)
		}
		raw, err := readValues(name, rawCount)
		if err != nil {
			return nil, err
		}
		values := make(map[string]string, int(fieldCount))
		for i := 0; i < len(raw); i += 2 {
			if _, exists := values[raw[i]]; exists {
				return nil, fmt.Errorf("%w: duplicate %s id %q", ErrMalformedRecord, name, raw[i])
			}
			values[raw[i]] = raw[i+1]
		}
		return values, nil
	}
	readIndex := func(name string) ([]string, error) {
		count, err := parseCount(name+" count", int64(s.opts.MaxTransitionItems))
		if err != nil {
			return nil, err
		}
		values, err := readValues(name, count)
		if err != nil {
			return nil, err
		}
		ids := make([]string, 0, len(values))
		seen := make(map[string]struct{}, len(values))
		for _, id := range values {
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("%w: duplicate %s id %q", ErrMalformedRecord, name, id)
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		return ids, nil
	}

	metaPresent, err := next("meta presence")
	if err != nil {
		return transitionPhysicalState{}, err
	}
	if metaPresent != "0" && metaPresent != "1" {
		return transitionPhysicalState{}, fmt.Errorf("%w: transition snapshot meta presence=%q", ErrMalformedRecord, metaPresent)
	}
	metaItemsText, err := next("meta items")
	if err != nil {
		return transitionPhysicalState{}, err
	}
	metaBytesText, err := next("meta bytes")
	if err != nil {
		return transitionPhysicalState{}, err
	}
	recoveryCount, err := parseCount("recovery count", int64(s.opts.MaxTransitionItems))
	if err != nil {
		return transitionPhysicalState{}, err
	}
	recoveries, err := readHash("recoveries", recoveryCount)
	if err != nil {
		return transitionPhysicalState{}, err
	}
	recoveryIndexIDs, err := readIndex("recovery index")
	if err != nil {
		return transitionPhysicalState{}, err
	}
	tombstoneCount, err := parseCount("tombstone count", int64(s.opts.MaxTransitionItems))
	if err != nil {
		return transitionPhysicalState{}, err
	}
	tombstones, err := readHash("tombstones", tombstoneCount)
	if err != nil {
		return transitionPhysicalState{}, err
	}
	tombstoneIndexIDs, err := readIndex("tombstone index")
	if err != nil {
		return transitionPhysicalState{}, err
	}
	if cursor != len(reply) {
		return transitionPhysicalState{}, fmt.Errorf("%w: transition snapshot trailing fields=%d",
			ErrScriptContract, len(reply)-cursor)
	}
	if int64(len(recoveries)) != recoveryCount || int64(len(recoveryIndexIDs)) != recoveryCount ||
		int64(len(tombstones)) != tombstoneCount || int64(len(tombstoneIndexIDs)) != tombstoneCount {
		return transitionPhysicalState{}, fmt.Errorf("%w: transition snapshot count mismatch", ErrMalformedRecord)
	}

	state := transitionPhysicalState{
		recoveries: make(map[string]authority.TransitionRecovery, len(recoveries)),
		tombstones: make(map[string]authority.TransitionTombstone, len(tombstones)),
		items:      recoveryCount + tombstoneCount,
	}
	if state.items > int64(s.opts.MaxTransitionItems) {
		return transitionPhysicalState{}, fmt.Errorf("%w: transition snapshot item count=%d",
			ErrMalformedRecord, state.items)
	}
	for id, raw := range recoveries {
		if err := s.keys.ValidateTransitionID(id); err != nil {
			return transitionPhysicalState{}, fmt.Errorf("%w: recovery id %q: %v", ErrMalformedRecord, id, err)
		}
		if len(raw) == 0 || len(raw) > s.opts.MaxTransitionRecordSize {
			return transitionPhysicalState{}, fmt.Errorf("%w: recovery %q size=%d", ErrMalformedRecord, id, len(raw))
		}
		entry, err := decodeRecovery(id, raw)
		if err != nil {
			return transitionPhysicalState{}, err
		}
		state.recoveries[id] = entry
		if state.bytes > int64(s.opts.MaxTransitionBytes)-int64(len(raw)) {
			return transitionPhysicalState{}, fmt.Errorf("%w: physical bytes exceed limit", ErrMalformedRecord)
		}
		state.bytes += int64(len(raw))
	}
	for id, raw := range tombstones {
		if err := s.keys.ValidateTransitionID(id); err != nil {
			return transitionPhysicalState{}, fmt.Errorf("%w: tombstone id %q: %v", ErrMalformedRecord, id, err)
		}
		if _, exists := state.recoveries[id]; exists {
			return transitionPhysicalState{}, fmt.Errorf("%w: recovery/tombstone id overlap %q", ErrMalformedRecord, id)
		}
		if len(raw) == 0 || len(raw) > s.opts.MaxTransitionRecordSize {
			return transitionPhysicalState{}, fmt.Errorf("%w: tombstone %q size=%d", ErrMalformedRecord, id, len(raw))
		}
		entry, err := decodeTombstone(id, raw)
		if err != nil {
			return transitionPhysicalState{}, err
		}
		state.tombstones[id] = entry
		if state.bytes > int64(s.opts.MaxTransitionBytes)-int64(len(raw)) {
			return transitionPhysicalState{}, fmt.Errorf("%w: physical bytes exceed limit", ErrMalformedRecord)
		}
		state.bytes += int64(len(raw))
	}
	if state.bytes > int64(s.opts.MaxTransitionBytes) {
		return transitionPhysicalState{}, fmt.Errorf("%w: physical bytes=%d limit=%d",
			ErrMalformedRecord, state.bytes, s.opts.MaxTransitionBytes)
	}

	if metaPresent == "0" {
		if state.items != 0 || metaItemsText != "" || metaBytesText != "" {
			return transitionPhysicalState{}, fmt.Errorf("%w: records exist without meta", ErrMalformedRecord)
		}
	} else {
		metaItems, err := parseTransitionCount(metaItemsText, int64(s.opts.MaxTransitionItems))
		if err != nil {
			return transitionPhysicalState{}, err
		}
		metaBytes, err := parseTransitionCount(metaBytesText, int64(s.opts.MaxTransitionBytes))
		if err != nil {
			return transitionPhysicalState{}, err
		}
		if metaItems != state.items {
			return transitionPhysicalState{}, fmt.Errorf("%w: meta items=%d actual items=%d",
				ErrMalformedRecord, metaItems, state.items)
		}
		if metaBytes != state.bytes {
			return transitionPhysicalState{}, fmt.Errorf("%w: meta bytes=%d actual bytes=%d",
				ErrMalformedRecord, metaBytes, state.bytes)
		}
	}

	for _, id := range recoveryIndexIDs {
		if _, exists := state.recoveries[id]; !exists {
			return transitionPhysicalState{}, fmt.Errorf("%w: recovery index member %q missing hash record",
				ErrMalformedRecord, id)
		}
	}
	for _, id := range tombstoneIndexIDs {
		if _, exists := state.tombstones[id]; !exists {
			return transitionPhysicalState{}, fmt.Errorf("%w: tombstone index member %q missing hash record",
				ErrMalformedRecord, id)
		}
		if _, overlap := state.recoveries[id]; overlap {
			return transitionPhysicalState{}, fmt.Errorf("%w: recovery/tombstone index overlap %q",
				ErrMalformedRecord, id)
		}
	}
	return state, nil
}

func parseTransitionCount(value string, maximum int64) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || parsed > maximum || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("%w: count=%q", ErrMalformedRecord, value)
	}
	return parsed, nil
}

func validateRecoveryIdentity(keys KeyBuilderV1, recovery authority.TransitionRecovery) error {
	if err := keys.ValidateTransitionID(recovery.TransitionID); err != nil {
		return err
	}
	return validateTransitionFingerprint(recovery.Fingerprint)
}

func encodeRecovery(recovery authority.TransitionRecovery) ([]byte, error) {
	// Current wire records always make the Authority call boundary explicit.
	// The presence/version observations are decoder metadata and intentionally
	// remain outside the canonical JSON payload.
	recovery.AuthorityAttemptedPresent = true
	recovery.RecoveryWireVersion = authority.TransitionRecoveryWireAuthorityAttempted
	raw, err := json.Marshal(recovery)
	if err != nil {
		return nil, fmt.Errorf("%w: recovery JSON: %v", ErrMalformedRecord, err)
	}
	return []byte(recovery.Fingerprint + transitionFingerprintSeparator + string(raw)), nil
}

func encodeTombstone(entry authority.TransitionTombstone) ([]byte, error) {
	raw, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("%w: tombstone JSON: %v", ErrMalformedRecord, err)
	}
	return []byte(entry.Fingerprint + transitionFingerprintSeparator + string(raw)), nil
}

func decodeRecovery(id, raw string) (authority.TransitionRecovery, error) {
	fingerprint, payload, err := splitRecord(raw)
	if err != nil {
		return authority.TransitionRecovery{}, err
	}
	var entry authority.TransitionRecovery
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: recovery JSON: %v", ErrMalformedRecord, err)
	}
	var wire struct {
		AuthorityAttempted       json.RawMessage `json:"authority_attempted"`
		LegacyAuthorityAttempted json.RawMessage `json:"AuthorityAttempted"`
	}
	if err := json.Unmarshal([]byte(payload), &wire); err != nil {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: recovery boundary JSON: %v", ErrMalformedRecord, err)
	}
	currentPresent := len(wire.AuthorityAttempted) != 0
	legacyPresent := len(wire.LegacyAuthorityAttempted) != 0
	if currentPresent && legacyPresent {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: duplicate recovery boundary fields", ErrMalformedRecord)
	}
	if legacyPresent {
		if string(wire.LegacyAuthorityAttempted) != "true" {
			return authority.TransitionRecovery{}, fmt.Errorf("%w: non-canonical legacy recovery boundary", ErrMalformedRecord)
		}
		entry.AuthorityAttempted = true
	}
	attemptedPresent := currentPresent || legacyPresent
	if entry.TransitionID != id || entry.Fingerprint != fingerprint {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: recovery identity %q", ErrMalformedRecord, id)
	}
	canonical, err := json.Marshal(entry)
	if err != nil {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: recovery canonical JSON", ErrMalformedRecord)
	}
	// Exact deployment fields were added as omitempty. Old non-exact records
	// therefore retain byte-for-byte canonical JSON and their v1 fingerprint;
	// only exact planned-drain records serialize the new v2 identity tuple.
	if !attemptedPresent {
		// Legacy canonical JSON omitted only a false AuthorityAttempted field.
		// Reconstruct that exact legacy form without treating false as proof.
		field := []byte(`,"authority_attempted":false`)
		if bytes.Count(canonical, field) != 1 {
			return authority.TransitionRecovery{}, fmt.Errorf("%w: recovery boundary field", ErrMalformedRecord)
		}
		canonical = bytes.Replace(canonical, field, nil, 1)
	} else if legacyPresent {
		field := []byte(`,"authority_attempted":true`)
		if bytes.Count(canonical, field) != 1 {
			return authority.TransitionRecovery{}, fmt.Errorf("%w: legacy recovery boundary field", ErrMalformedRecord)
		}
		canonical = bytes.Replace(canonical, field, []byte(`,"AuthorityAttempted":true`), 1)
	}
	if string(canonical) != payload {
		return authority.TransitionRecovery{}, fmt.Errorf("%w: non-canonical recovery JSON", ErrMalformedRecord)
	}
	entry.AuthorityAttemptedPresent = attemptedPresent
	if currentPresent {
		entry.RecoveryWireVersion = authority.TransitionRecoveryWireAuthorityAttempted
	} else {
		entry.RecoveryWireVersion = authority.TransitionRecoveryWireLegacy
	}
	return entry, nil
}

func decodeTombstone(id, raw string) (authority.TransitionTombstone, error) {
	fingerprint, payload, err := splitRecord(raw)
	if err != nil {
		return authority.TransitionTombstone{}, err
	}
	var entry authority.TransitionTombstone
	if err := json.Unmarshal([]byte(payload), &entry); err != nil {
		return authority.TransitionTombstone{}, fmt.Errorf("%w: tombstone JSON: %v", ErrMalformedRecord, err)
	}
	if entry.TransitionID != id || entry.Fingerprint != fingerprint {
		return authority.TransitionTombstone{}, fmt.Errorf("%w: tombstone identity %q", ErrMalformedRecord, id)
	}
	canonical, err := json.Marshal(entry)
	if err != nil || string(canonical) != payload {
		return authority.TransitionTombstone{}, fmt.Errorf("%w: non-canonical tombstone JSON", ErrMalformedRecord)
	}
	return entry, nil
}

func splitRecord(raw string) (string, string, error) {
	separator := strings.Index(raw, transitionFingerprintSeparator)
	if separator <= 0 || separator == len(raw)-1 {
		return "", "", fmt.Errorf("%w: record framing", ErrMalformedRecord)
	}
	fingerprint := raw[:separator]
	if validateTransitionFingerprint(fingerprint) != nil {
		return "", "", fmt.Errorf("%w: record fingerprint", ErrMalformedRecord)
	}
	return fingerprint, raw[separator+len(transitionFingerprintSeparator):], nil
}

func scoreFor(value time.Time) string {
	return strconv.FormatInt(value.UnixMilli(), 10)
}

func firstReply(reply []string) string {
	if len(reply) == 0 {
		return ""
	}
	return reply[0]
}

func transitionCapacityError() error {
	return fmt.Errorf("%w: %v", authority.ErrTransitionTombstoneCap, ErrCapacityExceeded)
}

func malformedTransitionError(operation string) error {
	return fmt.Errorf("%w: %s: %v", authority.ErrTransitionReconcile, operation, ErrMalformedRecord)
}
