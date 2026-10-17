package friend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// friendAuthorityCompleteSnapshotSource is the sole production Friendship
// snapshot truth. It returns one bucket envelope and a bounded set of complete
// canonical records from a single Redis execution. A Go pipeline only batches
// network I/O: a writer script may still execute between separate commands.
const friendAuthorityCompleteSnapshotSource = `
-- READ_FRIEND_AUTHORITY_COMPLETE_SNAPSHOT_V2
-- KEYS: bucket-meta, receipt-due, attempt-due, source-due-or-meta,
--       then one or more canonical record keys from the same bucket
-- ARGV: hash-tag, source-due-enabled, record-count, record-field-count,
--       record fields...
local tag = ARGV[1]
if not tag or tag == '' then return {'CONTRACT_ERROR'} end
for index = 1, #KEYS do
  if string.sub(KEYS[index], 1, #tag + 2) ~= '{' .. tag .. '}' then
    return {'CROSS_SLOT'}
  end
end
local function keyType(key)
  local value = redis.call('TYPE', key)
  if type(value) == 'table' then return value.ok or '' end
  return value
end
local function add(out, value)
  out[#out + 1] = value
end
local function addHash(out, key, fields)
  local kind = keyType(key)
  add(out, kind)
  if kind ~= 'hash' then
    add(out, '0')
    for _ = 1, #fields do add(out, '0') end
    return
  end
  add(out, tostring(redis.call('HLEN', key)))
  if #fields == 0 then return end
  local values = redis.call('HMGET', key, unpack(fields))
  for _, value in ipairs(values) do
    if value == false then add(out, '0') else add(out, '1' .. value) end
  end
end
local function addDue(out, key)
  local kind = keyType(key)
  add(out, kind)
  if kind == 'zset' then add(out, tostring(redis.call('ZCARD', key))) else add(out, '0') end
end
local recordCount = tonumber(ARGV[3])
local fieldCount = tonumber(ARGV[4])
if not recordCount or recordCount < 1 or recordCount ~= #KEYS - 4 or
   not fieldCount or fieldCount < 0 or fieldCount ~= #ARGV - 4 then
  return {'CONTRACT_ERROR'}
end
local metaFields = {
  'schema_version', 'authority_kind', 'layout_version', 'bucket_id',
  'receipt_count', 'receipt_bytes', 'pending_work_count', 'pending_work_bytes',
  'attempt_count', 'attempt_bytes', 'last_updated_at_ms'
}
local recordFields = {}
for index = 1, fieldCount do recordFields[index] = ARGV[4 + index] end
local out = {'FCAS2'}
addHash(out, KEYS[1], metaFields)
addDue(out, KEYS[2])
addDue(out, KEYS[3])
if ARGV[2] == '1' then addDue(out, KEYS[4]) else add(out, 'none'); add(out, '0') end
for index = 1, recordCount do addHash(out, KEYS[4 + index], recordFields) end
return out
`

type friendAuthoritySnapshotRecord struct {
	keyType    string
	fieldCount int64
	fields     map[string]string
}

type friendAuthoritySnapshot struct {
	metaType       string
	metaFieldCount int64
	metaFields     map[string]string
	receiptDueType string
	receiptDueCard uint64
	attemptDueType string
	attemptDueCard uint64
	sourceDueType  string
	sourceDueCard  uint64
	records        []friendAuthoritySnapshotRecord
}

func (a *redisFriendAuthority) authoritySnapshotArgs(group friendRedisGroup, recordKeys, recordFields []string) ([]string, []interface{}, error) {
	if len(recordKeys) == 0 || len(recordKeys) > MaxPageSize*refillBudgetPages {
		return nil, nil, fmt.Errorf("%w: authority snapshot record count", ErrInvalidCommand)
	}
	keys := make([]string, 0, 4+len(recordKeys))
	keys = append(keys, group.meta, group.receiptDue, group.attemptDue)
	if group.sourceDue != "" {
		keys = append(keys, group.sourceDue)
	} else {
		// Redis Cluster requires every declared key to occupy the same slot.
		// The fourth key is ignored when this flag is zero.
		keys = append(keys, group.meta)
	}
	keys = append(keys, recordKeys...)
	if err := a.assertSameSlot(keys...); err != nil {
		return nil, nil, err
	}
	args := make([]interface{}, 0, 4+len(recordFields))
	args = append(args, group.tag)
	if group.sourceDue != "" {
		args = append(args, "1")
	} else {
		args = append(args, "0")
	}
	args = append(args, strconv.Itoa(len(recordKeys)), strconv.Itoa(len(recordFields)))
	for _, field := range recordFields {
		args = append(args, field)
	}
	return keys, args, nil
}

func (a *redisFriendAuthority) runAuthorityCompleteSnapshot(ctx context.Context, group friendRedisGroup,
	recordKeys, recordFields []string) (friendAuthoritySnapshot, error) {
	keys, args, err := a.authoritySnapshotArgs(group, recordKeys, recordFields)
	if err != nil {
		return friendAuthoritySnapshot{}, err
	}
	reply, err := a.scripts.run(ctx, scriptFriendReadCompleteSnapshot, keys, args...)
	if err != nil {
		return friendAuthoritySnapshot{}, err
	}
	return decodeFriendAuthoritySnapshot(reply, len(recordKeys), recordFields)
}

func (a *redisFriendAuthority) readAuthorityCompleteSnapshot(ctx context.Context, group friendRedisGroup,
	recordKeys, recordFields []string) (snapshot friendAuthoritySnapshot, err error) {
	started := time.Now()
	present := false
	defer func() {
		a.envelopeReads.atomicReadTotal.Add(1)
		a.envelopeReads.atomicReadLatencyNanos.Add(uint64(time.Since(started)))
		switch {
		case errors.Is(err, ErrFriendReconcile):
			a.envelopeReads.reconcileTotal.Add(1)
		case err != nil:
			a.envelopeReads.technicalFailureTotal.Add(1)
		case present:
			a.envelopeReads.presentTotal.Add(1)
		default:
			a.envelopeReads.absentTotal.Add(1)
		}
	}()
	snapshot, err = a.runAuthorityCompleteSnapshot(ctx, group, recordKeys, recordFields)
	if err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if err = validateFriendAuthoritySnapshot(snapshot, group, group.ref.Kind); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	for _, record := range snapshot.records {
		present = present || record.keyType == "hash"
	}
	return snapshot, nil
}

func queueFriendAuthorityCompleteSnapshot(ctx context.Context, pipe redis.Pipeliner, keys []string, args []interface{}) *redis.Cmd {
	// Script.Run cannot perform NOSCRIPT fallback inside a pipeline. The exact
	// registered read-only source is sent once per unique bucket; single reads
	// use ScriptRegistry EVALSHA with bounded NOSCRIPT recovery.
	return pipe.Eval(ctx, friendScriptSources()[scriptFriendReadCompleteSnapshot], keys, args...)
}

func decodeFriendAuthoritySnapshot(raw interface{}, recordCount int, recordFields []string) (friendAuthoritySnapshot, error) {
	values, ok := raw.([]interface{})
	if !ok {
		if stringsReply, stringsOK := raw.([]string); stringsOK {
			values = make([]interface{}, len(stringsReply))
			for index := range stringsReply {
				values[index] = stringsReply[index]
			}
			ok = true
		}
	}
	if !ok {
		return friendAuthoritySnapshot{}, fmt.Errorf("%w: authority snapshot reply type %T", ErrFriendReconcile, raw)
	}
	expected := 1 + 2 + len(friendBucketMetaManifest) + 6 + recordCount*(2+len(recordFields))
	if len(values) != expected {
		return friendAuthoritySnapshot{}, fmt.Errorf("%w: authority snapshot reply length", ErrFriendReconcile)
	}
	position := 0
	nextString := func(class string) (string, error) {
		if position >= len(values) {
			return "", fmt.Errorf("%w: authority snapshot %s missing", ErrFriendReconcile, class)
		}
		value, ok := values[position].(string)
		position++
		if !ok {
			return "", fmt.Errorf("%w: authority snapshot %s type %T", ErrFriendReconcile, class, values[position-1])
		}
		return value, nil
	}
	nextCount := func(class string) (uint64, error) {
		value, err := nextString(class)
		if err != nil {
			return 0, err
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || strconv.FormatUint(parsed, 10) != value {
			return 0, fmt.Errorf("%w: authority snapshot %s", ErrFriendReconcile, class)
		}
		return parsed, nil
	}
	nextFields := func(names []string) (map[string]string, error) {
		fields := make(map[string]string, len(names))
		for _, name := range names {
			encoded, err := nextString("field " + name)
			if err != nil {
				return nil, err
			}
			switch encoded {
			case "0":
				continue
			default:
				if len(encoded) == 0 || encoded[0] != '1' {
					return nil, fmt.Errorf("%w: authority snapshot field %s encoding", ErrFriendReconcile, name)
				}
				fields[name] = encoded[1:]
			}
		}
		return fields, nil
	}
	version, err := nextString("version")
	if err != nil || version != "FCAS2" {
		return friendAuthoritySnapshot{}, fmt.Errorf("%w: authority snapshot version", ErrFriendReconcile)
	}
	var snapshot friendAuthoritySnapshot
	if snapshot.metaType, err = nextString("meta type"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	metaFieldCount, err := nextCount("meta field count")
	if err != nil || metaFieldCount > uint64(^uint64(0)>>1) {
		return friendAuthoritySnapshot{}, fmt.Errorf("%w: authority snapshot meta field count", ErrFriendReconcile)
	}
	snapshot.metaFieldCount = int64(metaFieldCount)
	if snapshot.metaFields, err = nextFields(friendBucketMetaManifest); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.receiptDueType, err = nextString("receipt due type"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.receiptDueCard, err = nextCount("receipt due cardinality"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.attemptDueType, err = nextString("attempt due type"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.attemptDueCard, err = nextCount("attempt due cardinality"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.sourceDueType, err = nextString("source due type"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	if snapshot.sourceDueCard, err = nextCount("source due cardinality"); err != nil {
		return friendAuthoritySnapshot{}, err
	}
	snapshot.records = make([]friendAuthoritySnapshotRecord, recordCount)
	for index := range snapshot.records {
		record := &snapshot.records[index]
		if record.keyType, err = nextString("record type"); err != nil {
			return friendAuthoritySnapshot{}, err
		}
		fieldCount, countErr := nextCount("record field count")
		if countErr != nil || fieldCount > uint64(^uint64(0)>>1) {
			return friendAuthoritySnapshot{}, fmt.Errorf("%w: authority snapshot record field count", ErrFriendReconcile)
		}
		record.fieldCount = int64(fieldCount)
		if record.fields, err = nextFields(recordFields); err != nil {
			return friendAuthoritySnapshot{}, err
		}
	}
	return snapshot, nil
}

func validateFriendAuthoritySnapshot(snapshot friendAuthoritySnapshot, group friendRedisGroup,
	kind FriendshipAuthorityKind) error {
	recordExists := false
	for _, record := range snapshot.records {
		if record.keyType != "none" && record.keyType != "hash" {
			return fmt.Errorf("%w: canonical key type", ErrFriendReconcile)
		}
		recordExists = recordExists || record.keyType == "hash"
	}
	for _, due := range []struct {
		name, keyType string
	}{
		{name: "receipt", keyType: snapshot.receiptDueType},
		{name: "attempt", keyType: snapshot.attemptDueType},
		{name: "source", keyType: snapshot.sourceDueType},
	} {
		if due.keyType != "none" && due.keyType != "zset" {
			return fmt.Errorf("%w: bucket %s due key type", ErrFriendReconcile, due.name)
		}
	}
	if group.sourceDue == "" && (snapshot.sourceDueType != "none" || snapshot.sourceDueCard != 0) {
		return fmt.Errorf("%w: non-pair source due", ErrFriendReconcile)
	}
	if snapshot.metaType == "none" {
		if recordExists {
			return fmt.Errorf("%w: canonical record without bucket meta", ErrFriendReconcile)
		}
		if snapshot.receiptDueCard != 0 || snapshot.attemptDueCard != 0 || snapshot.sourceDueCard != 0 {
			return fmt.Errorf("%w: bucket discovery without meta", ErrFriendReconcile)
		}
		return nil
	}
	if snapshot.metaType != "hash" {
		return fmt.Errorf("%w: bucket meta key type", ErrFriendReconcile)
	}
	if snapshot.metaFieldCount != int64(len(friendBucketMetaManifest)) || len(snapshot.metaFields) != len(friendBucketMetaManifest) {
		return fmt.Errorf("%w: bucket meta manifest", ErrFriendReconcile)
	}
	fields := snapshot.metaFields
	if fields["schema_version"] != "friend-auth-v1" || fields["authority_kind"] != string(kind) ||
		fields["layout_version"] != group.ref.LayoutVersion || fields["bucket_id"] != itoa(uint64(group.ref.BucketID)) {
		return fmt.Errorf("%w: bucket meta identity", ErrFriendReconcile)
	}
	counters := make(map[string]uint64, 7)
	for _, field := range []string{"receipt_count", "receipt_bytes", "pending_work_count", "pending_work_bytes",
		"attempt_count", "attempt_bytes", "last_updated_at_ms"} {
		value := fields[field]
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || strconv.FormatUint(parsed, 10) != value {
			return fmt.Errorf("%w: bucket meta counter %s", ErrFriendReconcile, field)
		}
		counters[field] = parsed
	}
	if snapshot.receiptDueCard != counters["receipt_count"] {
		return fmt.Errorf("%w: bucket receipt count", ErrFriendReconcile)
	}
	// Compacted cursors intentionally outlive their attempt-due discovery
	// member. Discovery remains a strict lower bound and may never exceed the
	// number of bounded cursor identities charged to the bucket.
	if snapshot.attemptDueCard > counters["attempt_count"] {
		return fmt.Errorf("%w: bucket attempt count", ErrFriendReconcile)
	}
	if group.sourceDue == "" {
		if counters["pending_work_count"] != 0 || counters["pending_work_bytes"] != 0 {
			return fmt.Errorf("%w: non-pair pending work", ErrFriendReconcile)
		}
	} else if snapshot.sourceDueCard != counters["pending_work_count"] {
		return fmt.Errorf("%w: bucket pending work count", ErrFriendReconcile)
	}
	for _, pair := range [][2]string{{"receipt_count", "receipt_bytes"}, {"pending_work_count", "pending_work_bytes"},
		{"attempt_count", "attempt_bytes"}} {
		if (counters[pair[0]] == 0) != (counters[pair[1]] == 0) {
			return fmt.Errorf("%w: bucket counter/bytes %s", ErrFriendReconcile, pair[0])
		}
	}
	return nil
}
