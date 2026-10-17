package routecontrol

import (
	"crypto/sha1"
	"encoding/hex"
)

const (
	ScriptBootstrapSnapshot                   = "BootstrapRouteSnapshotV1"
	ScriptCompareAndSwapSnapshot              = "CompareAndSwapRouteSnapshotV1"
	ScriptCompareAndSwapInitialAuthorityBatch = "CompareAndSwapInitialAuthorityBatchV1"
	ScriptReadTransitionSnapshot              = "ReadTransitionSnapshotV2"
	ScriptObserveTransitionEvidence           = "ObserveTransitionEvidenceV1"
	ScriptReserveTransitionRecovery           = "ReserveTransitionRecoveryV2"
	ScriptCommitTransitionRecovery            = "CommitTransitionRecoveryV2"
	ScriptDeleteTransitionRecovery            = "DeleteTransitionRecoveryV2"
	ScriptSaveTransitionTombstone             = "SaveTransitionTombstoneV2"
)

const snapshotBootstrapFenceSchema = "route-snapshot-bootstrap-fence-v1"

const (
	snapshotFieldSchema         = "schema_version"
	snapshotFieldRealm          = "realm"
	snapshotFieldRevision       = "revision"
	snapshotFieldJSON           = "canonical_json"
	snapshotFieldChecksum       = "checksum_sha256"
	snapshotFieldTopology       = "topology_fingerprint"
	snapshotFieldCommitted      = "committed_at_ms"
	metaFieldItems              = "item_count"
	metaFieldBytes              = "byte_count"
	snapshotFenceFieldSchema    = "schema_version"
	snapshotFenceFieldOperation = "operation_id"
	snapshotFenceFieldEpoch     = "epoch"
)

const luaKeyTagCheck = `
local tag = ARGV[1]
for i = 1, #KEYS do
  if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then
    return {'CROSS_SLOT'}
  end
end
`

const luaSnapshotShape = `
local function snapshot_shape_is_valid()
  if redis.call('TYPE', KEYS[1]).ok ~= 'hash' then return false end
  if redis.call('HLEN', KEYS[1]) ~= 7 then return false end
  local fields = {
    'schema_version', 'realm', 'revision', 'canonical_json',
    'checksum_sha256', 'topology_fingerprint', 'committed_at_ms'
  }
  for i = 1, #fields do
    if redis.call('HEXISTS', KEYS[1], fields[i]) ~= 1 then return false end
  end
  return true
end
`

const luaSnapshotBootstrapFenceShape = `
local function snapshot_bootstrap_fence_is_valid()
  local kind = redis.call('TYPE', KEYS[2]).ok
  if kind == 'none' then return true end
  if kind ~= 'hash' or redis.call('HLEN', KEYS[2]) ~= 3 then return false end
  local fields = {'schema_version', 'operation_id', 'epoch'}
  for i = 1, #fields do
    if redis.call('HEXISTS', KEYS[2], fields[i]) ~= 1 then return false end
  end
  local schema = redis.call('HGET', KEYS[2], 'schema_version')
  local operation = redis.call('HGET', KEYS[2], 'operation_id')
  local epoch = redis.call('HGET', KEYS[2], 'epoch')
  if schema ~= 'route-snapshot-bootstrap-fence-v1' or operation == nil or epoch == nil or
     #operation > 256 or #epoch < 1 or #epoch > 20 or
     (epoch ~= '0' and string.match(epoch, '^[1-9][0-9]*$') == nil) then
    return false
  end
  if operation == '' then return epoch == '0' end
  return true
end
`

const luaBootstrapSnapshot = luaKeyTagCheck + `
` + luaSnapshotShape + `
` + luaSnapshotBootstrapFenceShape + `
if not snapshot_bootstrap_fence_is_valid() then
  return {'MALFORMED'}
end
local fence_enabled = true
if ARGV[19] == '' and ARGV[20] == '0' then
  fence_enabled = false
elseif ARGV[18] ~= 'route-snapshot-bootstrap-fence-v1' or ARGV[19] == nil or ARGV[20] == nil or
   #ARGV[19] > 256 or #ARGV[20] < 1 or #ARGV[20] > 20 or
   (ARGV[20] ~= '0' and string.match(ARGV[20], '^[1-9][0-9]*$') == nil) or
   (ARGV[19] == '' and ARGV[20] ~= '0') then
  return {'MALFORMED'}
end
if #ARGV[5] > tonumber(ARGV[9]) then
  return {'CAPACITY_EXCEEDED'}
end
if redis.call('EXISTS', KEYS[1]) == 1 then
  if not snapshot_shape_is_valid() then
    return {'MALFORMED'}
  end
  if ARGV[10] ~= '1' then
    return {'CURRENT_CHANGED'}
  end
  local current_schema = redis.call('HGET', KEYS[1], 'schema_version')
  local current_realm = redis.call('HGET', KEYS[1], 'realm')
  local current_revision = redis.call('HGET', KEYS[1], 'revision')
  local current_json = redis.call('HGET', KEYS[1], 'canonical_json')
  local current_checksum = redis.call('HGET', KEYS[1], 'checksum_sha256')
  local current_topology = redis.call('HGET', KEYS[1], 'topology_fingerprint')
  local current_committed = redis.call('HGET', KEYS[1], 'committed_at_ms')
  if current_schema ~= ARGV[11] or current_realm ~= ARGV[12] or
     current_revision ~= ARGV[13] or current_json ~= ARGV[14] or
     current_checksum ~= ARGV[15] or current_topology ~= ARGV[16] or
     current_committed ~= ARGV[17] then
    return {'CURRENT_CHANGED'}
  end
  if current_schema == ARGV[2] and current_realm == ARGV[3] and
     current_revision == ARGV[4] and current_json == ARGV[5] and
     current_checksum == ARGV[6] and current_topology == ARGV[7] and
     current_revision == '1' then
    if fence_enabled and redis.call('TYPE', KEYS[2]).ok == 'none' then
      redis.call('HSET', KEYS[2],
        'schema_version', ARGV[18], 'operation_id', ARGV[19], 'epoch', ARGV[20])
    end
    return {'ALREADY_COMMITTED', current_schema, current_realm, current_revision,
      current_json, current_checksum, current_topology, current_committed}
  end
  return {'CONFLICT'}
end
if ARGV[10] ~= '0' then
  return {'CURRENT_CHANGED'}
end
if ARGV[4] ~= '1' then
  return {'BAD_REVISION'}
end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' then
  return {'MALFORMED'}
end
redis.call('HSET', KEYS[1],
  'schema_version', ARGV[2],
  'realm', ARGV[3],
  'revision', ARGV[4],
  'canonical_json', ARGV[5],
  'checksum_sha256', ARGV[6],
  'topology_fingerprint', ARGV[7],
  'committed_at_ms', ARGV[8])
if fence_enabled then
  redis.call('HSET', KEYS[2],
    'schema_version', ARGV[18], 'operation_id', ARGV[19], 'epoch', ARGV[20])
end
return {'COMMITTED', ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8]}
`

const luaCompareAndSwapSnapshot = luaKeyTagCheck + `
` + luaSnapshotShape + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  if ARGV[2] == '1' then return {'CURRENT_CHANGED'} end
  return {'BOOTSTRAP_REQUIRED'}
end
if not snapshot_shape_is_valid() then
  return {'MALFORMED'}
end
if ARGV[2] ~= '1' then
  return {'CURRENT_CHANGED'}
end
local current_schema = redis.call('HGET', KEYS[1], 'schema_version')
local current_realm = redis.call('HGET', KEYS[1], 'realm')
local current_revision = redis.call('HGET', KEYS[1], 'revision')
local current_json = redis.call('HGET', KEYS[1], 'canonical_json')
local current_checksum = redis.call('HGET', KEYS[1], 'checksum_sha256')
local current_topology = redis.call('HGET', KEYS[1], 'topology_fingerprint')
local current_committed = redis.call('HGET', KEYS[1], 'committed_at_ms')
if current_schema ~= ARGV[3] or current_realm ~= ARGV[4] or
   current_revision ~= ARGV[5] or current_json ~= ARGV[6] or
   current_checksum ~= ARGV[7] or current_topology ~= ARGV[8] or
   current_committed ~= ARGV[9] then
  return {'CURRENT_CHANGED'}
end
if current_revision == ARGV[11] then
  if current_schema == ARGV[12] and current_realm == ARGV[13] and
     current_json == ARGV[14] and current_checksum == ARGV[15] and
     current_topology == ARGV[16] then
    return {'ALREADY_COMMITTED', current_schema, current_realm, current_revision,
      current_json, current_checksum, current_topology, current_committed}
  end
  return {'REVISION_CONFLICT'}
end
if current_revision ~= ARGV[10] then
  return {'STALE_EXPECTATION', current_revision}
end
if ARGV[18] ~= 'STRICT_NEXT' then
  return {'REVISION_CONFLICT'}
end
if #ARGV[14] > tonumber(ARGV[19]) then
  return {'CAPACITY_EXCEEDED'}
end
redis.call('HSET', KEYS[1],
  'schema_version', ARGV[12],
  'realm', ARGV[13],
  'revision', ARGV[11],
  'canonical_json', ARGV[14],
  'checksum_sha256', ARGV[15],
  'topology_fingerprint', ARGV[16],
  'committed_at_ms', ARGV[17])
return {'COMMITTED', ARGV[12], ARGV[13], ARGV[11], ARGV[14], ARGV[15], ARGV[16], ARGV[17]}
`

const luaCompareAndSwapInitialAuthorityBatch = luaKeyTagCheck + `
` + luaSnapshotShape + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'BOOTSTRAP_REQUIRED'}
end
if not snapshot_shape_is_valid() then
  return {'MALFORMED'}
end
local current_schema = redis.call('HGET', KEYS[1], 'schema_version')
local current_realm = redis.call('HGET', KEYS[1], 'realm')
local current_revision = redis.call('HGET', KEYS[1], 'revision')
local current_json = redis.call('HGET', KEYS[1], 'canonical_json')
local current_checksum = redis.call('HGET', KEYS[1], 'checksum_sha256')
local current_topology = redis.call('HGET', KEYS[1], 'topology_fingerprint')
local current_committed = redis.call('HGET', KEYS[1], 'committed_at_ms')
if current_schema ~= ARGV[2] or current_realm ~= ARGV[3] or
   current_revision ~= ARGV[4] or current_json ~= ARGV[5] or
   current_checksum ~= ARGV[6] or current_topology ~= ARGV[7] or
   current_committed ~= ARGV[8] then
  return {'CURRENT_CHANGED'}
end
if current_revision == ARGV[11] then
  if current_schema == ARGV[9] and current_realm == ARGV[10] and
     current_json == ARGV[12] and current_checksum == ARGV[13] and
     current_topology == ARGV[14] then
    return {'ALREADY_COMMITTED', current_schema, current_realm, current_revision,
      current_json, current_checksum, current_topology, current_committed}
  end
  return {'REVISION_CONFLICT'}
end
local function canonical_decimal(value)
  return value and (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
end
local function decimal_less_or_equal(left, right)
  if #left ~= #right then return #left < #right end
  return left <= right
end
if not canonical_decimal(ARGV[11]) or decimal_less_or_equal(ARGV[11], current_revision) then
  return {'REVISION_CONFLICT'}
end
if #ARGV[12] > tonumber(ARGV[16]) then
  return {'CAPACITY_EXCEEDED'}
end
redis.call('HSET', KEYS[1],
  'schema_version', ARGV[9],
  'realm', ARGV[10],
  'revision', ARGV[11],
  'canonical_json', ARGV[12],
  'checksum_sha256', ARGV[13],
  'topology_fingerprint', ARGV[14],
  'committed_at_ms', ARGV[15])
return {'COMMITTED', ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15]}
`

const luaTransitionCommon = luaKeyTagCheck + `
if #KEYS ~= 5 then return {'MALFORMED'} end
local max_safe_integer = 9007199254740991
local max_items_limit = 1048576
local max_bytes_limit = 1073741824
local max_record_limit = 268435456
local max_id_size = 256
local max_fingerprint_size = 256
local physical_entry_overhead = 4096
local physical_base_overhead = 65536

local function continuation_byte(value, index)
  local byte = string.byte(value, index)
  if not byte or byte < 128 or byte > 191 then return nil end
  return byte
end

local function component_codepoint_disallowed(codepoint)
  -- Match Go unicode.IsControl (Cc) and unicode.IsSpace exactly for the
  -- current Unicode White_Space set. UTF-8 byte length remains the durable
  -- component bound, so existing v1 multilingual identities stay readable.
  if codepoint <= 31 or (codepoint >= 127 and codepoint <= 159) then return true end
  return codepoint == 32 or codepoint == 133 or codepoint == 160 or codepoint == 5760 or
    (codepoint >= 8192 and codepoint <= 8202) or codepoint == 8232 or codepoint == 8233 or
    codepoint == 8239 or codepoint == 8287 or codepoint == 12288
end

local function valid_component(value, maximum)
  if not value or #value < 1 or #value > maximum then return false end
  local index = 1
  while index <= #value do
    local first = string.byte(value, index)
    local codepoint = nil
    local width = nil
    if first <= 127 then
      codepoint = first
      width = 1
    elseif first >= 194 and first <= 223 then
      local second = continuation_byte(value, index + 1)
      if not second then return false end
      codepoint = (first - 192) * 64 + second - 128
      width = 2
    elseif first >= 224 and first <= 239 then
      local second = continuation_byte(value, index + 1)
      local third = continuation_byte(value, index + 2)
      if not second or not third or (first == 224 and second < 160) or
         (first == 237 and second > 159) then return false end
      codepoint = (first - 224) * 4096 + (second - 128) * 64 + third - 128
      width = 3
    elseif first >= 240 and first <= 244 then
      local second = continuation_byte(value, index + 1)
      local third = continuation_byte(value, index + 2)
      local fourth = continuation_byte(value, index + 3)
      if not second or not third or not fourth or (first == 240 and second < 144) or
         (first == 244 and second > 143) then return false end
      codepoint = (first - 240) * 262144 + (second - 128) * 4096 +
        (third - 128) * 64 + fourth - 128
      width = 4
    else
      return false
    end
    if component_codepoint_disallowed(codepoint) then return false end
    index = index + width
  end
  return true
end

local function existing_fingerprint(value)
  if not value then return nil end
  local separator = string.find(value, string.char(0), 1, true)
  if not separator or separator <= 1 or separator > max_fingerprint_size + 1 or separator >= #value then return nil end
  local fingerprint = string.sub(value, 1, separator - 1)
  if not valid_component(fingerprint, max_fingerprint_size) then return nil end
  return fingerprint
end

local function parse_count(value, maximum)
  if not value or value == '' or
     (not string.match(value, '^0$') and not string.match(value, '^[1-9][0-9]*$')) then
    return nil
  end
  local number = tonumber(value)
  if not number or number < 0 or number > maximum or string.format('%.0f', number) ~= value then
    return nil
  end
  return number
end

local function key_type(key, expected)
  local kind = redis.call('TYPE', key).ok
  return kind == 'none' or kind == expected
end

local function validate_record(value, maximum)
  return value and #value > 1 and #value <= maximum and existing_fingerprint(value) ~= nil
end

local function samples_syntax_unsupported(reply)
  if type(reply) ~= 'table' or not reply.err then return false end
  local message = string.lower(reply.err)
  return string.find(message, 'syntax', 1, true) ~= nil or
    string.find(message, 'wrong number', 1, true) ~= nil or
    string.find(message, 'unknown subcommand', 1, true) ~= nil
end

local function memory_within(key, maximum, count)
  local usage = redis.pcall('MEMORY', 'USAGE', key, 'SAMPLES', '0')
  if type(usage) == 'table' and usage.err then
    if count > 5 or not samples_syntax_unsupported(usage) then return false end
    usage = redis.pcall('MEMORY', 'USAGE', key)
    if type(usage) == 'table' and usage.err then return false end
  end
  if not usage then return redis.call('TYPE', key).ok == 'none' end
  usage = tonumber(usage)
  return usage and usage >= 0 and usage <= maximum
end

local function validate_state(max_items_text, max_bytes_text, max_record_text)
  local max_items = parse_count(max_items_text, max_items_limit)
  local max_bytes = parse_count(max_bytes_text, max_bytes_limit)
  local max_record_size = parse_count(max_record_text, max_record_limit)
  if not max_items or not max_bytes or not max_record_size or
     max_items < 1 or max_bytes < 1 or max_record_size < 1 or max_record_size > max_bytes then
    return false
  end
  if not key_type(KEYS[1], 'hash') or not key_type(KEYS[2], 'hash') or
     not key_type(KEYS[3], 'zset') or not key_type(KEYS[4], 'hash') or
     not key_type(KEYS[5], 'zset') then
    return false
  end

  local meta_count = redis.call('TYPE', KEYS[1]).ok == 'none' and 0 or redis.call('HLEN', KEYS[1])
  local recovery_count = redis.call('HLEN', KEYS[2])
  local tombstone_count = redis.call('HLEN', KEYS[4])
  local recovery_index_count = redis.call('ZCARD', KEYS[3])
  local tombstone_index_count = redis.call('ZCARD', KEYS[5])
  if recovery_count > max_items or tombstone_count > max_items or
     recovery_index_count > max_items or tombstone_index_count > max_items or
     recovery_count ~= recovery_index_count or tombstone_count ~= tombstone_index_count or
     recovery_count + tombstone_count > max_items then
    return false
  end

  -- These ceilings are intentionally generous for normal Redis encodings,
  -- but fixed from configured logical limits. Cardinality is read first with
  -- O(1) commands; no field, member, or value is materialized before exact
  -- MEMORY USAGE SAMPLES 0 proves the physical bound.
  local per_entry = max_id_size + physical_entry_overhead
  if max_items > math.floor((max_safe_integer - physical_base_overhead) / per_entry) then return false end
  local index_ceiling = max_items * per_entry + physical_base_overhead
  local allocator_expansion = 4
  if max_bytes > math.floor((max_safe_integer - index_ceiling) / allocator_expansion) then return false end
  local hash_ceiling = max_bytes * allocator_expansion + index_ceiling
  if not memory_within(KEYS[1], physical_base_overhead, meta_count) or
     not memory_within(KEYS[2], hash_ceiling, recovery_count) or
     not memory_within(KEYS[3], index_ceiling, recovery_index_count) or
     not memory_within(KEYS[4], hash_ceiling, tombstone_count) or
     not memory_within(KEYS[5], index_ceiling, tombstone_index_count) then
    return false
  end

  local meta_type = redis.call('TYPE', KEYS[1]).ok
  local items = recovery_count + tombstone_count
  local bytes = 0
  local meta_bytes
  if meta_type == 'none' then
    if items ~= 0 then return false end
  else
    if meta_count ~= 2 then return false end
    if redis.call('HEXISTS', KEYS[1], 'item_count') ~= 1 or
       redis.call('HEXISTS', KEYS[1], 'byte_count') ~= 1 or
       redis.call('HSTRLEN', KEYS[1], 'item_count') > 20 or
       redis.call('HSTRLEN', KEYS[1], 'byte_count') > 20 then return false end
    local meta_items = redis.call('HGET', KEYS[1], 'item_count')
    meta_bytes = redis.call('HGET', KEYS[1], 'byte_count')
    local parsed_items = parse_count(meta_items, max_items)
    local parsed_bytes = parse_count(meta_bytes, max_bytes)
    if not parsed_items or not parsed_bytes then return false end
    if parsed_items ~= items then return false end
    meta_bytes = parsed_bytes
  end

  local recovery_ids = {}
  local recovery_keys = redis.call('HKEYS', KEYS[2])
  for _, id in ipairs(recovery_keys) do
    if not valid_component(id, max_id_size) then return false end
    local value_size = redis.call('HSTRLEN', KEYS[2], id)
    if value_size < 2 or value_size > max_record_size then return false end
    local value = redis.call('HGET', KEYS[2], id)
    if not validate_record(value, max_record_size) then return false end
    if recovery_ids[id] then return false end
    recovery_ids[id] = true
    bytes = bytes + #value
  end
  local tombstone_keys = redis.call('HKEYS', KEYS[4])
  for _, id in ipairs(tombstone_keys) do
    if not valid_component(id, max_id_size) then return false end
    local value_size = redis.call('HSTRLEN', KEYS[4], id)
    if value_size < 2 or value_size > max_record_size then return false end
    local value = redis.call('HGET', KEYS[4], id)
    if not validate_record(value, max_record_size) then return false end
    if recovery_ids[id] then return false end
    bytes = bytes + #value
  end
  if bytes > max_bytes then return false end

  local recovery_index_ids = redis.call('ZRANGE', KEYS[3], 0, recovery_index_count - 1)
  for _, id in ipairs(recovery_index_ids) do
    if not valid_component(id, max_id_size) then return false end
    if not recovery_ids[id] or not redis.call('ZSCORE', KEYS[3], id) then return false end
  end
  local tombstone_index_ids = redis.call('ZRANGE', KEYS[5], 0, tombstone_index_count - 1)
  for _, id in ipairs(tombstone_index_ids) do
    if not valid_component(id, max_id_size) then return false end
    if recovery_ids[id] then return false end
    if redis.call('HEXISTS', KEYS[4], id) ~= 1 or not redis.call('ZSCORE', KEYS[5], id) then return false end
  end
  for _, id in ipairs(recovery_keys) do
    if not redis.call('ZSCORE', KEYS[3], id) then return false end
  end
  for _, id in ipairs(tombstone_keys) do
    if recovery_ids[id] or not redis.call('ZSCORE', KEYS[5], id) then return false end
  end
  if meta_bytes ~= nil and meta_bytes ~= bytes then
    return false
  end
  return true, items, bytes, max_items, max_bytes, max_record_size
end
`

const luaReserveTransitionRecovery = luaTransitionCommon + `
local id = ARGV[2]
local fingerprint = ARGV[3]
local value = ARGV[4]
local input_max_record = parse_count(ARGV[7], max_record_limit)
if not valid_component(id, max_id_size) or not valid_component(fingerprint, max_fingerprint_size) or
   not input_max_record or not validate_record(value, input_max_record) or existing_fingerprint(value) ~= fingerprint then
  return {'MALFORMED'}
end
local valid, items, bytes, max_items, max_bytes, max_record_size = validate_state(ARGV[5], ARGV[6], ARGV[7])
if not valid then return {'MALFORMED'} end
local existing = redis.call('HGET', KEYS[2], id)
if existing then
  if existing_fingerprint(existing) == fingerprint then return {'ALREADY'} end
  return {'CONFLICT'}
end
if redis.call('HEXISTS', KEYS[4], id) == 1 then return {'CONFLICT'} end
if items + 1 > max_items or bytes + #value > max_bytes then return {'CAPACITY_EXCEEDED'} end
redis.call('HSET', KEYS[2], id, value)
redis.call('ZADD', KEYS[3], ARGV[8], id)
redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items + 1),
  'byte_count', string.format('%.0f', bytes + #value))
return {'RESERVED'}
`

const luaCommitTransitionRecovery = luaTransitionCommon + `
local id = ARGV[2]
local fingerprint = ARGV[3]
local value = ARGV[4]
local input_max_record = parse_count(ARGV[8], max_record_limit)
if not valid_component(id, max_id_size) or not valid_component(fingerprint, max_fingerprint_size) or
   not input_max_record or not validate_record(value, input_max_record) or existing_fingerprint(value) ~= fingerprint then
  return {'MALFORMED'}
end
local valid, items, bytes, max_items, max_bytes, max_record_size = validate_state(ARGV[6], ARGV[7], ARGV[8])
if not valid then return {'MALFORMED'} end
local existing = redis.call('HGET', KEYS[2], id)
if not existing then return {'NOT_FOUND'} end
if existing_fingerprint(existing) ~= fingerprint then return {'CONFLICT'} end
local next_bytes = bytes - #existing + #value
if items < 1 or next_bytes < 0 or next_bytes > max_bytes then return {'MALFORMED'} end
redis.call('HSET', KEYS[2], id, value)
redis.call('ZADD', KEYS[3], ARGV[5], id)
redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items),
  'byte_count', string.format('%.0f', next_bytes))
return {'COMMITTED'}
`

const luaDeleteTransitionRecovery = luaTransitionCommon + `
local id = ARGV[2]
if not valid_component(id, max_id_size) then return {'MALFORMED'} end
local valid, items, bytes = validate_state(ARGV[3], ARGV[4], ARGV[5])
if not valid then return {'MALFORMED'} end
local existing = redis.call('HGET', KEYS[2], id)
if not existing then return {'DELETED'} end
if items < 1 or bytes < #existing then return {'MALFORMED'} end
redis.call('HDEL', KEYS[2], id)
redis.call('ZREM', KEYS[3], id)
redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items - 1),
  'byte_count', string.format('%.0f', bytes - #existing))
return {'DELETED'}
`

const luaSaveTransitionTombstone = luaTransitionCommon + `
local id = ARGV[2]
local fingerprint = ARGV[3]
local value = ARGV[4]
local input_max_record = parse_count(ARGV[7], max_record_limit)
if not valid_component(id, max_id_size) or not valid_component(fingerprint, max_fingerprint_size) or
   not input_max_record or not validate_record(value, input_max_record) or existing_fingerprint(value) ~= fingerprint then
  return {'MALFORMED'}
end
local valid, items, bytes, max_items, max_bytes, max_record_size = validate_state(ARGV[5], ARGV[6], ARGV[7])
if not valid then return {'MALFORMED'} end
local existing_tombstone = redis.call('HGET', KEYS[4], id)
local existing_recovery = redis.call('HGET', KEYS[2], id)
if existing_tombstone and existing_recovery then return {'MALFORMED'} end
if existing_tombstone then
  if existing_fingerprint(existing_tombstone) ~= fingerprint then return {'CONFLICT'} end
  local next_bytes = bytes - #existing_tombstone + #value
  if next_bytes < 0 or next_bytes > max_bytes then return {'CAPACITY_EXCEEDED'} end
  redis.call('HSET', KEYS[4], id, value)
  redis.call('ZADD', KEYS[5], ARGV[8], id)
  redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items),
    'byte_count', string.format('%.0f', next_bytes))
  return {'SAVED'}
end
if existing_recovery and existing_fingerprint(existing_recovery) ~= fingerprint then return {'CONFLICT'} end
local old_bytes = 0
if existing_recovery then old_bytes = #existing_recovery end
if not existing_recovery and items + 1 > max_items then return {'CAPACITY_EXCEEDED'} end
if bytes - old_bytes + #value > max_bytes then return {'CAPACITY_EXCEEDED'} end
if existing_recovery then
  redis.call('HDEL', KEYS[2], id)
  redis.call('ZREM', KEYS[3], id)
end
redis.call('HSET', KEYS[4], id, value)
redis.call('ZADD', KEYS[5], ARGV[8], id)
if existing_recovery then
  redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items),
    'byte_count', string.format('%.0f', bytes - old_bytes + #value))
else
  redis.call('HSET', KEYS[1], 'item_count', string.format('%.0f', items + 1),
    'byte_count', string.format('%.0f', bytes + #value))
end
return {'SAVED'}
`

// luaReadTransitionSnapshot runs the bounded physical read and all
// cross-key structural checks under one Redis script invocation. The Go
// adapter still performs the full record/ID/canonical-JSON validation after
// receiving this point-in-time snapshot.
const luaReadTransitionSnapshot = luaTransitionCommon + `
local valid, items, bytes = validate_state(ARGV[2], ARGV[3], ARGV[4])
if not valid then return {'MALFORMED'} end

local recovery_count = redis.call('HLEN', KEYS[2])
local tombstone_count = redis.call('HLEN', KEYS[4])
local recovery_index_ids = redis.call('ZRANGE', KEYS[3], 0, recovery_count - 1)
local tombstone_index_ids = redis.call('ZRANGE', KEYS[5], 0, tombstone_count - 1)
if #recovery_index_ids ~= recovery_count or #tombstone_index_ids ~= tombstone_count then
  return {'MALFORMED'}
end

-- Cardinality was capped by validate_state before either HGETALL can run.
local recovery_values = redis.call('HGETALL', KEYS[2])
local tombstone_values = redis.call('HGETALL', KEYS[4])
if #recovery_values ~= recovery_count * 2 or #tombstone_values ~= tombstone_count * 2 then
  return {'MALFORMED'}
end

local meta_present = '0'
local meta_items = ''
local meta_bytes = ''
if redis.call('TYPE', KEYS[1]).ok ~= 'none' then
  meta_present = '1'
  meta_items = redis.call('HGET', KEYS[1], 'item_count')
  meta_bytes = redis.call('HGET', KEYS[1], 'byte_count')
  if not meta_items or not meta_bytes then return {'MALFORMED'} end
end

local result = {
  'OK', meta_present, meta_items, meta_bytes,
  tostring(recovery_count), tostring(#recovery_values)
}
for _, value in ipairs(recovery_values) do table.insert(result, value) end
table.insert(result, tostring(#recovery_index_ids))
for _, id in ipairs(recovery_index_ids) do table.insert(result, id) end
table.insert(result, tostring(tombstone_count))
table.insert(result, tostring(#tombstone_values))
for _, value in ipairs(tombstone_values) do table.insert(result, value) end
table.insert(result, tostring(#tombstone_index_ids))
for _, id in ipairs(tombstone_index_ids) do table.insert(result, id) end
return result
`

// luaObserveTransitionEvidence performs the same cardinality-first physical
// validation as every transition operation, then returns only the two values
// for the caller-supplied exact ID. It is strictly read-only.
const luaObserveTransitionEvidence = luaTransitionCommon + `
local id = ARGV[2]
if not valid_component(id, max_id_size) then return {'MALFORMED'} end
local valid = validate_state(ARGV[3], ARGV[4], ARGV[5])
if not valid then return {'MALFORMED'} end
local recovery = redis.call('HGET', KEYS[2], id)
local tombstone = redis.call('HGET', KEYS[4], id)
if recovery and tombstone then return {'MALFORMED'} end
if recovery then return {'OK', 'RECOVERY', recovery} end
if tombstone then return {'OK', 'TOMBSTONE', tombstone} end
return {'OK', 'MISSING', ''}
`

func builtinScripts() []scriptDef {
	return []scriptDef{
		newScriptDef(ScriptBootstrapSnapshot, luaBootstrapSnapshot),
		newScriptDef(ScriptCompareAndSwapSnapshot, luaCompareAndSwapSnapshot),
		newScriptDef(ScriptCompareAndSwapInitialAuthorityBatch, luaCompareAndSwapInitialAuthorityBatch),
		newScriptDef(ScriptReadTransitionSnapshot, luaReadTransitionSnapshot),
		newScriptDef(ScriptObserveTransitionEvidence, luaObserveTransitionEvidence),
		newScriptDef(ScriptReserveTransitionRecovery, luaReserveTransitionRecovery),
		newScriptDef(ScriptCommitTransitionRecovery, luaCommitTransitionRecovery),
		newScriptDef(ScriptDeleteTransitionRecovery, luaDeleteTransitionRecovery),
		newScriptDef(ScriptSaveTransitionTombstone, luaSaveTransitionTombstone),
	}
}

func newScriptDef(name, source string) scriptDef {
	sum := sha1.Sum([]byte(source)) // Redis identifies scripts with SHA-1.
	return scriptDef{name: name, source: source, sha: hex.EncodeToString(sum[:])}
}
