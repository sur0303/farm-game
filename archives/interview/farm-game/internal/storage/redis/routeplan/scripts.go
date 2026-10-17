package routeplan

import (
	"context"
	"crypto/sha1" //nolint:gosec // Redis script cache identity, not authentication.
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

const scriptCompareAndSwapPlan = "CompareAndSwapRoutePlanV4"
const scriptCompareAndSwapCreatePlan = "CompareAndSwapCreateRoutePlanV1"
const scriptCompareAndSwapExistingPlan = "CompareAndSwapExistingRoutePlanV1"
const scriptLoadPlan = "LoadRoutePlanV3"
const scriptReadPlanIndex = "ReadRoutePlanIndexV1"
const scriptStopOwnedPlan = "StopOwnedRoutePlanV1"

// readPlanIndexLua proves a physical upper bound before ZRANGE can
// materialize members in Lua or return them to Go. The only accepted members
// are canonical lowercase hex Plan IDs, each within the fixed ID bound.
const readPlanIndexLua = `
if #KEYS ~= 1 or #ARGV ~= 3 then return {'CORRUPT', 'INPUT'} end
local function canonical_uint(value, maximum)
  if type(value) ~= 'string' or (value ~= '0' and string.match(value, '^[1-9][0-9]*$') == nil) then return nil end
  local number = tonumber(value)
  if not number or number < 0 or number > maximum or string.format('%.0f', number) ~= value then return nil end
  return number
end
local max_items = canonical_uint(ARGV[1], 1048576)
local max_bytes = canonical_uint(ARGV[2], 1073741824)
local max_record = canonical_uint(ARGV[3], 268435456)
if not max_items or not max_bytes or not max_record or max_items < 1 or max_bytes < 1 or max_record < 1 or max_record > max_bytes then return {'CORRUPT', 'BOUND'} end
local kind = redis.call('TYPE', KEYS[1]).ok
if kind == 'none' then return {'OK', '0'} end
if kind ~= 'zset' then return {'CORRUPT', 'SHAPE'} end
local count = redis.call('ZCARD', KEYS[1])
if count < 0 or count > max_items then return {'CORRUPT', 'COUNT'} end
local max_safe_integer = 9007199254740991
local physical_base = 65536
local physical_per_item = 256 + 4096
if max_items > math.floor((max_safe_integer - physical_base) / physical_per_item) then return {'CORRUPT', 'BOUND'} end
local physical_ceiling = physical_base + max_items * physical_per_item
local function samples_syntax_unsupported(reply)
  if type(reply) ~= 'table' or not reply.err then return false end
  local message = string.lower(reply.err)
  return string.find(message, 'syntax', 1, true) ~= nil or
    string.find(message, 'wrong number', 1, true) ~= nil or
    string.find(message, 'unknown subcommand', 1, true) ~= nil
end
local usage = redis.pcall('MEMORY', 'USAGE', KEYS[1], 'SAMPLES', '0')
if type(usage) == 'table' and usage.err then
  if count > 5 or not samples_syntax_unsupported(usage) then return {'CORRUPT', 'MEMORY_SAMPLES_UNSUPPORTED'} end
  usage = redis.pcall('MEMORY', 'USAGE', KEYS[1])
  if type(usage) == 'table' and usage.err then return {'CORRUPT', 'PHYSICAL_BYTES'} end
end
usage = tonumber(usage)
if not usage or usage < 0 or usage > physical_ceiling then return {'CORRUPT', 'PHYSICAL_BYTES'} end
local values = redis.call('ZRANGE', KEYS[1], 0, count - 1, 'WITHSCORES')
if #values ~= count * 2 then return {'CORRUPT', 'COUNT'} end
local result = {'OK', string.format('%.0f', count)}
for index = 1, #values, 2 do
  local member = values[index]
  local score = tonumber(values[index + 1])
  if #member < 2 or #member > 256 or #member % 2 ~= 0 or string.match(member, '^[0-9a-f]+$') == nil or
     not score or score < 0 or score > 9007199254740991 or score ~= math.floor(score) then return {'CORRUPT', 'MEMBER'} end
  result[#result + 1] = member
  result[#result + 1] = string.format('%.0f', score)
end
return result
`

const loadPlanLua = `
if #KEYS ~= 3 or ARGV[3] == nil or #ARGV[3] < 1 or #ARGV[3] > 256 then return {'CORRUPT', 'INPUT'} end
local key_type = redis.call('TYPE', KEYS[1]).ok
local index_type = redis.call('TYPE', KEYS[2]).ok
local accounting_type = redis.call('TYPE', KEYS[3]).ok
if (index_type ~= 'none' and index_type ~= 'zset') or (accounting_type ~= 'none' and accounting_type ~= 'hash') then return {'CORRUPT', 'REFERENCE_SHAPE'} end
local score = false
if index_type == 'zset' then score = redis.call('ZSCORE', KEYS[2], ARGV[3]) end
local accounted = false
if accounting_type == 'hash' and redis.call('HEXISTS', KEYS[3], ARGV[3]) == 1 then
  if redis.call('HSTRLEN', KEYS[3], ARGV[3]) > 20 then return {'CORRUPT', 'ACCOUNTING_BYTES'} end
  accounted = redis.call('HGET', KEYS[3], ARGV[3])
end
if key_type == 'none' then
  if score ~= false or accounted ~= false then return {'CORRUPT', 'DANGLING_REFERENCE'} end
  return {'NOT_FOUND'}
end
if key_type ~= 'hash' then return {'CORRUPT', 'SHAPE'} end
if score == false or accounted == false then return {'CORRUPT', 'REFERENCE_MISSING'} end
local field_count = redis.call('HLEN', KEYS[1])
local fields_v1 = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase'}
local fields_v2 = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase','attempt','started_at_ms','completed_at_ms'}
local fields = nil
local status = nil
if field_count == 8 then fields = fields_v1; status = 'FOUND_V1'
elseif field_count == 11 then fields = fields_v2; status = 'FOUND'
else return {'CORRUPT', 'SHAPE'} end
local limits_v1 = {32,128,20,tonumber(ARGV[1]),64,20,20,16}
local limits_v2 = {32,128,20,tonumber(ARGV[1]),64,20,20,16,20,20,20}
local limits = field_count == 8 and limits_v1 or limits_v2
local total_limit = tonumber(ARGV[2])
local max_audit_ms = tonumber(ARGV[4])
if limits[4] == nil or total_limit == nil or max_audit_ms == nil or limits[4] <= 0 or total_limit <= 0 or max_audit_ms <= 0 then return {'CORRUPT', 'BOUND'} end
local total = 0
for index = 1, #fields do
  if redis.call('HEXISTS', KEYS[1], fields[index]) ~= 1 then return {'CORRUPT', 'FIELD_MISSING'} end
  local size = redis.call('HSTRLEN', KEYS[1], fields[index])
  if size > limits[index] then return {'CORRUPT', 'FIELD_BYTES'} end
  total = total + size
end
if total > total_limit then return {'CORRUPT', 'TOTAL_BYTES'} end
local values = redis.call('HMGET', KEYS[1], unpack(fields))
if (field_count == 8 and values[1] ~= 'route-plan-v1') or (field_count == 11 and values[1] ~= 'route-plan-v2') then return {'CORRUPT', 'SCHEMA'} end
local expected_bytes = #values[4] + #values[5] + 64
local expires = tonumber(values[7])
local physical = tonumber(score)
local retention_ms = expires and physical and physical - expires or nil
if accounted ~= tostring(expected_bytes) or physical == nil or retention_ms == nil or retention_ms <= 0 or
  retention_ms > max_audit_ms or physical > 9007199254740991 then return {'CORRUPT', 'REFERENCE_VALUE'} end
local result = {status}
for index = 1, #values do result[#result + 1] = values[index] end
return result
`

// The script validates the complete bounded index/accounting state before the
// candidate mutation. It touches only the exact keys supplied by the store;
// there is no KEYS or SCAN discovery path.
const compareAndSwapPlanLua = `
local tag = ARGV[1]
local mode = ARGV[2]
local now_ms = tonumber(ARGV[3])
local max_items = tonumber(ARGV[4])
local max_bytes = tonumber(ARGV[5])
local max_record = tonumber(ARGV[6])
local audit_ms = tonumber(ARGV[7])
local live_count = tonumber(ARGV[21])
local expired_count = tonumber(ARGV[22])
for i = 1, #KEYS do
  if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then return {'CROSS_SLOT'} end
end
if (mode ~= 'CAS' and mode ~= 'READ') or now_ms == nil or max_items == nil or max_bytes == nil or max_record == nil or audit_ms == nil or live_count == nil or expired_count == nil then return {'MALFORMED'} end
if live_count < 0 or expired_count < 0 or live_count + expired_count > max_items or #KEYS ~= 5 + live_count + expired_count then return {'MALFORMED'} end

local function exact_hash(key, expected_fields)
  if redis.call('TYPE', key).ok ~= 'hash' then return nil end
  if redis.call('HLEN', key) ~= expected_fields then return nil end
  return true
end

local function exact_plan(key, plan_id, revision, canonical, checksum, updated, expires, phase, attempt, started, completed)
  local field_count = redis.call('HLEN', key)
	local fields = nil
	local limits = nil
  if field_count == 8 then
		fields = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase'}
		limits = {32,128,20,max_record,64,20,20,16}
	elseif field_count == 11 then
			fields = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase','attempt','started_at_ms','completed_at_ms'}
			limits = {32,128,20,max_record,64,20,20,16,20,20,20}
	else
		return nil
	end
	for index = 1, #fields do
		if redis.call('HEXISTS', key, fields[index]) ~= 1 or redis.call('HSTRLEN', key, fields[index]) > limits[index] then return nil end
	end
	local value = redis.call('HMGET', key, unpack(fields))
	if field_count == 8 then
		return value[1] == 'route-plan-v1' and value[2] == plan_id and value[3] == revision and value[4] == canonical and value[5] == checksum and value[6] == updated and value[7] == expires and value[8] == phase
	end
		return value[1] == 'route-plan-v2' and value[2] == plan_id and value[3] == revision and value[4] == canonical and value[5] == checksum and value[6] == updated and value[7] == expires and value[8] == phase and value[9] == attempt and value[10] == started and value[11] == completed
end

local function phase_valid(from, to)
  if from == 'SUCCEEDED' or from == 'FAILED' or from == 'PAUSED' then return false end
  if from == to then return true end
  if to == 'FAILED' or to == 'PAUSED' then return true end
  if from == 'PENDING' then return to == 'DRAINING' or to == 'ASSIGNING' end
  if from == 'DRAINING' then return to == 'ASSIGNING' end
  if from == 'ASSIGNING' then return to == 'PREPARING' or to == 'ACTIVE' end
  if from == 'PREPARING' then return to == 'ACTIVE' end
  if from == 'ACTIVE' then return to == 'SUCCEEDED' end
  return false
end

local function canonical_uint(value, max_text)
  if type(value) ~= 'string' or (value ~= '0' and string.match(value, '^[1-9][0-9]*$') == nil) then return false end
  if #value < #max_text then return true end
  if #value > #max_text then return false end
  return value <= max_text
end

local function increment_uint64(value)
  if not canonical_uint(value, '18446744073709551615') or value == '18446744073709551615' then return nil end
  local carry = 1
  local out = ''
  for index = #value, 1, -1 do
    local digit = string.byte(value, index) - 48 + carry
    if digit >= 10 then digit = digit - 10; carry = 1 else carry = 0 end
    out = string.char(48 + digit) .. out
  end
  if carry == 1 then out = '1' .. out end
  return out
end

local function compare_uint(left, right, max_text)
  if not canonical_uint(left, max_text) or not canonical_uint(right, max_text) then return nil end
  if #left < #right then return -1 end
  if #left > #right then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end

-- canonicalPlan has a fixed field order. Compare the exact raw immutable
-- segments instead of decoding JSON numbers through Lua doubles or trusting a
-- caller-supplied digest. JSON string quotes are escaped, so these structural
-- markers cannot be mistaken for text inside a field value.
local function replace_once(value, old, new)
  local first, last = string.find(value, old, 1, true)
  if first == nil then return nil end
  return string.sub(value, 1, first - 1) .. new .. string.sub(value, last + 1)
end

-- A retained canonical v1 plan may advance after rollout. Project only the
-- exact zero-valued v2 compatibility fields back out before the independent
-- Lua frozen-field comparison. Any non-zero generation/owner claim cannot
-- match these literals and therefore remains a revision conflict.
local function legacy_projection(value)
  local projected = replace_once(value, '"schema_version":"route-plan-v2"', '"schema_version":"route-plan-v1"')
  if projected == nil then return nil end
  projected = replace_once(projected, ',"source_deployment_identity":{"pod_uid":"","workload_uid":"","ordinal":0}', '')
  if projected == nil then return nil end
  projected = replace_once(projected, ',"target_deployment_identity":{"pod_uid":"","workload_uid":"","ordinal":0}', '')
  if projected == nil then return nil end
  projected = replace_once(projected, ',"owner_scale_plan_id":"","owner_retain_until_ms":0', '')
  return projected
end

local function frozen_parts(value)
  if type(value) ~= 'string' then return nil end
  local phase = string.find(value, ',"phase":', 1, true)
  local created = phase and string.find(value, ',"created_at_ms":', phase + 1, true) or nil
  local updated = created and string.find(value, ',"updated_at_ms":', created + 1, true) or nil
  local expires = updated and string.find(value, ',"expires_at_ms":', updated + 1, true) or nil
  local error_code = expires and string.find(value, ',"error_code":', expires + 1, true) or nil
  local proof = error_code and string.find(value, ',"takeover_proof":', error_code + 1, true) or nil
  if phase == nil or created == nil or updated == nil or expires == nil or error_code == nil or proof == nil or
    not (phase < created and created < updated and updated < expires and expires < error_code and error_code < proof) then return nil end
  return string.sub(value, 1, phase - 1), string.sub(value, created, updated - 1),
    string.sub(value, expires, error_code - 1), string.sub(value, proof)
end

local meta_exists = redis.call('EXISTS', KEYS[1]) == 1
local count = 0
local bytes = 0
if meta_exists then
  if not exact_hash(KEYS[1], 3) then return {'MALFORMED'} end
  local meta = redis.call('HMGET', KEYS[1], 'schema', 'count', 'bytes')
  if meta[1] ~= 'route-plan-meta-v1' then return {'MALFORMED'} end
  count = tonumber(meta[2]); bytes = tonumber(meta[3])
  if count == nil or bytes == nil or tostring(count) ~= meta[2] or tostring(bytes) ~= meta[3] or count < 0 or bytes < 0 or count > max_items or bytes > max_bytes then return {'MALFORMED', 'META_INTEGER'} end
else
  if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 or redis.call('EXISTS', KEYS[4]) == 1 or live_count ~= 0 or expired_count ~= 0 then return {'MALFORMED'} end
end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' and redis.call('TYPE', KEYS[2]).ok ~= 'zset' then return {'MALFORMED'} end
if redis.call('TYPE', KEYS[3]).ok ~= 'none' and redis.call('TYPE', KEYS[3]).ok ~= 'list' then return {'MALFORMED'} end
if meta_exists then
  if redis.call('TYPE', KEYS[2]).ok ~= 'zset' or redis.call('TYPE', KEYS[3]).ok ~= 'list' or redis.call('TYPE', KEYS[4]).ok ~= 'hash' then return {'MALFORMED'} end
  if redis.call('ZCARD', KEYS[2]) ~= count or redis.call('HLEN', KEYS[4]) ~= count or count ~= live_count + expired_count then return {'MALFORMED'} end
  if redis.call('LLEN', KEYS[3]) < 1 or redis.call('LLEN', KEYS[3]) > count + 1 or redis.call('LLEN', KEYS[3]) > max_items + 1 then return {'MALFORMED'} end
end

local restart_values = redis.call('LRANGE', KEYS[3], 0, -1)
local restart = {}
if meta_exists and restart_values[1] ~= 'route-plan-restart-v1' then return {'MALFORMED'} end
for index = 2, #restart_values do
  local member = restart_values[index]
  if restart[member] then return {'MALFORMED'} end
  restart[member] = true
end

local common_expiry = -1
local desired_common_expiry = -1
if meta_exists then
  local tail = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
  if #tail ~= 2 then return {'MALFORMED'} end
  common_expiry = tonumber(tail[2])
  if common_expiry == nil then return {'MALFORMED'} end
  for index = 1, 4 do
    local key_expiry = redis.call('PEXPIRETIME', KEYS[index])
    if tonumber(key_expiry) == nil or tonumber(key_expiry) ~= common_expiry then return {'MALFORMED', 'SHARED_TTL'} end
  end
  desired_common_expiry = common_expiry
end

local computed_bytes = 0
local known_restart = {}
local max_accounting_text = string.format('%.0f', max_record + 128)
-- The caller decoded each exact canonical envelope before invoking this
-- script. Legacy v1 hashes did not duplicate these three audit fields, so
-- retain the exact decoded values for candidate replay/update validation.
local plan_audit = {}
local offset = 23
for index = 1, live_count do
  local member = ARGV[offset]
  local plan_id = ARGV[offset + 1]
  local revision = ARGV[offset + 2]
  local canonical = ARGV[offset + 3]
  local checksum = ARGV[offset + 4]
  local updated = ARGV[offset + 5]
  local expires = ARGV[offset + 6]
  local phase = ARGV[offset + 7]
  local attempt = ARGV[offset + 8]
  local started = ARGV[offset + 9]
  local completed = ARGV[offset + 10]
	  local record_bytes_text = ARGV[offset + 11]
	  local record_bytes = tonumber(record_bytes_text)
  local physical = tonumber(ARGV[offset + 12])
  local restart_expected = ARGV[offset + 13] == '1'
  local key = KEYS[5 + index]
	if member == nil or known_restart[member] or not canonical_uint(record_bytes_text, max_accounting_text) or record_bytes == nil or record_bytes <= 64 or record_bytes > max_record + 128 or physical == nil or record_bytes ~= #canonical + #checksum + 64 then return {'MALFORMED', 'LIVE_BASIC'} end
  known_restart[member] = true
  local score = tonumber(redis.call('ZSCORE', KEYS[2], member))
  if score ~= physical or physical <= now_ms or physical <= tonumber(expires) or physical - tonumber(expires) > audit_ms then return {'MALFORMED', 'LIVE_SCORE'} end
  local desired_physical = tonumber(expires) + audit_ms
  if desired_physical > 9007199254740991 then return {'MALFORMED', 'LIVE_SCORE'} end
  if desired_physical > desired_common_expiry then desired_common_expiry = desired_physical end
  if not exact_plan(key, plan_id, revision, canonical, checksum, updated, expires, phase, attempt, started, completed) then return {'MALFORMED', 'LIVE_PLAN'} end
  plan_audit[member] = {attempt, started, completed}
	  if redis.call('HSTRLEN', KEYS[4], member) > 20 then return {'MALFORMED', 'LIVE_ACCOUNTING'} end
	  local stored_bytes_text = redis.call('HGET', KEYS[4], member)
	  if not canonical_uint(stored_bytes_text, max_accounting_text) or stored_bytes_text ~= record_bytes_text then return {'MALFORMED', 'LIVE_ACCOUNTING'} end
  if tonumber(redis.call('PEXPIRETIME', key)) ~= common_expiry then return {'MALFORMED', 'LIVE_TTL'} end
  if restart_expected and not restart[member] then return {'MALFORMED', 'LIVE_RESTART_MISSING'} end
  if not restart_expected and restart[member] and tonumber(expires) > now_ms then return {'MALFORMED', 'LIVE_RESTART_EXTRA'} end
  computed_bytes = computed_bytes + record_bytes
  offset = offset + 14
end

local expired_offset = offset
local expired_bytes = 0
local retained_candidate_expired = false
for index = 1, expired_count do
  local member = ARGV[offset]
  local physical = tonumber(ARGV[offset + 1])
  local present = ARGV[offset + 2] == '1'
  local plan_id = ARGV[offset + 3]
  local revision = ARGV[offset + 4]
  local canonical = ARGV[offset + 5]
  local checksum = ARGV[offset + 6]
  local updated = ARGV[offset + 7]
  local expires = ARGV[offset + 8]
  local phase = ARGV[offset + 9]
  local attempt = ARGV[offset + 10]
  local started = ARGV[offset + 11]
  local completed = ARGV[offset + 12]
	  local expected_bytes_text = ARGV[offset + 13]
	  local expected_bytes = tonumber(expected_bytes_text)
  local key = KEYS[5 + live_count + index]
  local score = tonumber(redis.call('ZSCORE', KEYS[2], member))
	  if redis.call('HSTRLEN', KEYS[4], member) > 20 then return {'MALFORMED', 'EXPIRED_ACCOUNTING'} end
	  local record_bytes_text = redis.call('HGET', KEYS[4], member)
	  local record_bytes = tonumber(record_bytes_text)
  local key_exists = redis.call('EXISTS', key) == 1
	  if member == nil or known_restart[member] or physical == nil or score ~= physical or
	    not canonical_uint(record_bytes_text, max_accounting_text) or not canonical_uint(expected_bytes_text, max_accounting_text) or
	    record_bytes_text ~= expected_bytes_text or record_bytes == nil or record_bytes <= 64 or record_bytes > max_record + 128 then return {'MALFORMED', 'EXPIRED_ACCOUNTING'} end
  known_restart[member] = true
  if present then
    if not key_exists or physical > now_ms or expected_bytes ~= record_bytes or physical <= tonumber(expires) or physical - tonumber(expires) > audit_ms or
      not exact_plan(key, plan_id, revision, canonical, checksum, updated, expires, phase, attempt, started, completed) then return {'MALFORMED', 'EXPIRED_PLAN'} end
    local desired_physical = tonumber(expires) + audit_ms
    if desired_physical > 9007199254740991 then return {'MALFORMED', 'EXPIRED_PLAN'} end
    if desired_physical > desired_common_expiry then desired_common_expiry = desired_physical end
    plan_audit[member] = {attempt, started, completed}
    if tonumber(redis.call('PEXPIRETIME', key)) ~= common_expiry then return {'MALFORMED', 'EXPIRED_TTL'} end
	if mode == 'CAS' and member == ARGV[9] then retained_candidate_expired = true end
  else
    return {'MALFORMED', 'EXPIRED_HASH_MISSING'}
  end
  computed_bytes = computed_bytes + record_bytes
  expired_bytes = expired_bytes + record_bytes
  offset = offset + 14
end
if computed_bytes ~= bytes then return {'MALFORMED', 'TOTAL_BYTES'} end
for member, _ in pairs(restart) do if not known_restart[member] then return {'MALFORMED'} end end

-- Physical members remain immutable evidence until the shared tail TTL drops
-- the complete bounded set. Never delete one hash/index/accounting tuple while
-- another member keeps the shared set alive; that would permit PlanID reuse.
local remaining_count = count
local remaining_bytes = bytes

-- Decide every CAS outcome before the first cleanup write. A replay,
-- conflict, capacity rejection, or malformed candidate therefore cannot leave
-- index/accounting members deleted while meta still reports the old totals.
local current_exists = false
local current_revision = 0
local old_bytes = 0
local candidate_physical = 0
local candidate_bytes = 0
local projected = remaining_bytes
local shared_expiry_text = nil
if mode == 'CAS' then
  local expected = tonumber(ARGV[10])
  local candidate = tonumber(ARGV[11])
	  if expected == nil or candidate == nil or expected < 0 or candidate <= 0 or expected > 9007199254740991 or candidate > 9007199254740991 or #ARGV[12] > max_record or
		    not canonical_uint(ARGV[14], '9223372036854775807') or ARGV[14] == '0' or not canonical_uint(ARGV[15], '9223372036854775807') or ARGV[15] == '0' or
		    not canonical_uint(ARGV[18], '18446744073709551615') or not canonical_uint(ARGV[19], '9223372036854775807') or not canonical_uint(ARGV[20], '9223372036854775807') then return {'MALFORMED'} end
	local candidate_terminal = ARGV[16] == 'SUCCEEDED' or ARGV[16] == 'FAILED' or ARGV[16] == 'PAUSED'
  current_exists = redis.call('EXISTS', KEYS[5]) == 1
  if current_exists then
    if redis.call('ZSCORE', KEYS[2], ARGV[9]) == false then return {'MALFORMED', 'CANDIDATE_INDEX'} end
    local current = nil
    local current_fields = redis.call('HLEN', KEYS[5])
	    if current_fields == 11 then
	      current = redis.call('HMGET', KEYS[5], 'schema', 'plan_id', 'revision', 'canonical_json', 'checksum', 'updated_at_ms', 'expires_at_ms', 'phase', 'attempt', 'started_at_ms', 'completed_at_ms')
      if current[1] ~= 'route-plan-v2' then return {'MALFORMED', 'CANDIDATE_SCHEMA'} end
    elseif current_fields == 8 then
      local legacy = redis.call('HMGET', KEYS[5], 'schema', 'plan_id', 'revision', 'canonical_json', 'checksum', 'updated_at_ms', 'expires_at_ms', 'phase')
      local audit = plan_audit[ARGV[9]]
      if legacy[1] ~= 'route-plan-v1' or audit == nil then return {'MALFORMED', 'CANDIDATE_SCHEMA'} end
	      current = {legacy[1], legacy[2], legacy[3], legacy[4], legacy[5], legacy[6], legacy[7], legacy[8], audit[1], audit[2], audit[3]}
    else
      return {'MALFORMED', 'CANDIDATE_SHAPE'}
    end
    if current[2] ~= ARGV[8] then return {'MALFORMED'} end
    current_revision = tonumber(current[3]); old_bytes = #current[4] + #current[5] + 64
	    if current_revision == candidate and current[4] == ARGV[12] and current[5] == ARGV[13] and current[6] == ARGV[14] and current[7] == ARGV[15] and current[8] == ARGV[16] and current[9] == ARGV[18] and current[10] == ARGV[19] and current[11] == ARGV[20] then return {'ALREADY_COMMITTED'} end
		if retained_candidate_expired then return {'REVISION_CONFLICT'} end
	    if current_revision ~= expected or candidate ~= expected + 1 then return {'REVISION_CONFLICT'} end
	    if current[8] == 'SUCCEEDED' or current[8] == 'FAILED' or current[8] == 'PAUSED' then return {'REVISION_CONFLICT'} end
	    local old_canonical = current[4]
	    local new_canonical = ARGV[12]
	    if string.sub(old_canonical, 1, 33) == '{"schema_version":"route-plan-v1"' and
	      string.sub(new_canonical, 1, 33) == '{"schema_version":"route-plan-v2"' then
	      new_canonical = legacy_projection(new_canonical)
	    end
	    local old_a, old_b, old_c, old_d = frozen_parts(old_canonical)
	    local new_a, new_b, new_c, new_d = frozen_parts(new_canonical)
	    if old_a == nil or new_a == nil or old_a ~= new_a or old_b ~= new_b or old_c ~= new_c or old_d ~= new_d then return {'REVISION_CONFLICT'} end
	    if not phase_valid(current[8], ARGV[16]) then return {'REVISION_CONFLICT'} end
	    if compare_uint(ARGV[14], current[6], '9223372036854775807') == nil or compare_uint(ARGV[14], current[6], '9223372036854775807') < 0 then return {'REVISION_CONFLICT'} end
    if not canonical_uint(current[9], '18446744073709551615') or (ARGV[18] ~= current[9] and ARGV[18] ~= increment_uint64(current[9])) then return {'REVISION_CONFLICT'} end
    if not canonical_uint(current[10], '9223372036854775807') or (current[10] ~= '0' and ARGV[19] ~= current[10]) then return {'REVISION_CONFLICT'} end
    if not canonical_uint(current[11], '9223372036854775807') or (current[11] ~= '0' and ARGV[20] ~= current[11]) then return {'REVISION_CONFLICT'} end
  elseif expected ~= 0 or candidate ~= 1 then
    return {'REVISION_CONFLICT'}
  elseif ARGV[16] ~= 'PENDING' or ARGV[18] ~= '0' or ARGV[19] ~= '0' or ARGV[20] ~= '0' then
    return {'REVISION_CONFLICT'}
  end
	  if ARGV[19] ~= '0' and compare_uint(ARGV[19], ARGV[14], '9223372036854775807') > 0 then return {'REVISION_CONFLICT'} end
	  if (candidate_terminal and ARGV[20] ~= ARGV[14]) or (not candidate_terminal and ARGV[20] ~= '0') then return {'REVISION_CONFLICT'} end
	  if not candidate_terminal and compare_uint(ARGV[15], ARGV[14], '9223372036854775807') <= 0 then return {'REVISION_CONFLICT'} end
  local candidate_expires = tonumber(ARGV[15])
  candidate_physical = candidate_expires and candidate_expires + audit_ms or nil
  if candidate_expires == nil or candidate_physical == nil or candidate_physical <= now_ms or candidate_physical > 9007199254740991 or (not current_exists and candidate_expires <= now_ms) then return {'MALFORMED'} end
  candidate_bytes = #ARGV[12] + #ARGV[13] + 64
  projected = remaining_bytes - old_bytes + candidate_bytes
  if (not current_exists and remaining_count >= max_items) or projected > max_bytes then return {'CAPACITY_EXCEEDED'} end
  local shared_expiry = candidate_physical
  if common_expiry > shared_expiry then shared_expiry = common_expiry end
  if shared_expiry <= now_ms or shared_expiry > 9007199254740991 then return {'MALFORMED', 'WRITE_TTL'} end
  shared_expiry_text = string.format('%.0f', shared_expiry)
elseif count > 0 then
  shared_expiry_text = string.format('%.0f', common_expiry)
end

offset = expired_offset
for index = 1, expired_count do
  local member = ARGV[offset]
  redis.call('LREM', KEYS[3], 0, member)
	  offset = offset + 14
end
offset = 23
for index = 1, live_count do
	if ARGV[offset + 13] ~= '1' then redis.call('LREM', KEYS[3], 0, ARGV[offset]) end
	offset = offset + 14
end
count = remaining_count
bytes = remaining_bytes

if mode == 'READ' then
  -- Successful bounded validation is also the atomic retention migration:
  -- every retained member receives the configured horizon and every shared
  -- key receives the same new tail before the script returns.
  offset = 23
  for index = 1, live_count do
    local desired_physical = tonumber(ARGV[offset + 6]) + audit_ms
    redis.call('ZADD', KEYS[2], tostring(desired_physical), ARGV[offset])
    offset = offset + 14
  end
  offset = expired_offset
  for index = 1, expired_count do
    local desired_physical = tonumber(ARGV[offset + 8]) + audit_ms
    redis.call('ZADD', KEYS[2], tostring(desired_physical), ARGV[offset])
    offset = offset + 14
  end
  common_expiry = desired_common_expiry
  shared_expiry_text = string.format('%.0f', common_expiry)
  if count == 0 then
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
  else
    redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', tostring(count), 'bytes', tostring(bytes))
    for index = 1, 4 do redis.call('PEXPIREAT', KEYS[index], shared_expiry_text) end
    for index = 1, live_count do redis.call('PEXPIREAT', KEYS[5 + index], shared_expiry_text) end
    for index = 1, expired_count do redis.call('PEXPIREAT', KEYS[5 + live_count + index], shared_expiry_text) end
  end
  return {'OK'}
end

redis.call('HSET', KEYS[5],
  'schema', 'route-plan-v2', 'plan_id', ARGV[8], 'revision', ARGV[11],
	  'canonical_json', ARGV[12], 'checksum', ARGV[13], 'updated_at_ms', ARGV[14],
	  'expires_at_ms', ARGV[15], 'phase', ARGV[16], 'attempt', ARGV[18],
	  'started_at_ms', ARGV[19], 'completed_at_ms', ARGV[20])
redis.call('ZADD', KEYS[2], tostring(candidate_physical), ARGV[9])
redis.call('HSET', KEYS[4], ARGV[9], tostring(candidate_bytes))
redis.call('LREM', KEYS[3], 0, ARGV[9])
if redis.call('EXISTS', KEYS[3]) == 0 then redis.call('RPUSH', KEYS[3], 'route-plan-restart-v1') end
if ARGV[17] == '1' then redis.call('RPUSH', KEYS[3], ARGV[9]) end
if not current_exists then count = count + 1 end
redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', tostring(count), 'bytes', tostring(projected))
for index = 1, 4 do redis.call('PEXPIREAT', KEYS[index], shared_expiry_text) end
redis.call('PEXPIREAT', KEYS[5], shared_expiry_text)
for index = 1, live_count do redis.call('PEXPIREAT', KEYS[5 + index], shared_expiry_text) end
for index = 1, expired_count do redis.call('PEXPIREAT', KEYS[5 + live_count + index], shared_expiry_text) end
return {'COMMITTED'}
`

// compareAndSwapCreatePlanLua is the new-plan hot path. A foreground batch
// uses one frozen creation timestamp, so at most its first member has to grow
// the shared audit tail through the bounded reconciliation script. Every later
// member validates and updates only the four shared bookkeeping keys plus its
// own immutable Plan key. The script never shortens the shared tail and asks
// the caller to use the full proof when extending it would require touching
// retained Plan hashes.
const compareAndSwapCreatePlanLua = `
if #KEYS ~= 5 or #ARGV ~= 19 then return {'MALFORMED', 'INPUT'} end
local tag = ARGV[1]
for index = 1, #KEYS do
  if string.sub(KEYS[index], 1, #tag + 2) ~= '{' .. tag .. '}' then return {'CROSS_SLOT'} end
end

local function canonical_uint(value, maximum, allow_zero)
  if type(value) ~= 'string' or value == '' or
     (allow_zero and value ~= '0' and string.match(value, '^[1-9][0-9]*$') == nil) or
     (not allow_zero and string.match(value, '^[1-9][0-9]*$') == nil) then return false end
  if not allow_zero and value == '0' then return false end
  if value == '0' then return true end
  if #value < #maximum then return true end
  if #value > #maximum then return false end
  return value <= maximum
end
local function exact_hash(key, fields)
  if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HLEN', key) ~= #fields then return false end
  for index = 1, #fields do
    if redis.call('HEXISTS', key, fields[index]) ~= 1 then return false end
  end
  return true
end
local function member_positions(key, member)
  local result = redis.call('LPOS', key, member, 'COUNT', 2)
  if result == false then return 0 end
  return #result
end

local now_ms = tonumber(ARGV[2])
local max_items = tonumber(ARGV[3])
local max_bytes = tonumber(ARGV[4])
local max_record = tonumber(ARGV[5])
local audit_ms = tonumber(ARGV[6])
if now_ms == nil or max_items == nil or max_bytes == nil or max_record == nil or audit_ms == nil or
   max_items < 1 or max_items > 1048576 or max_bytes < 1 or max_bytes > 1073741824 or
   max_record < 1 or max_record > 268435456 or max_record > max_bytes or audit_ms <= 0 then
  return {'MALFORMED', 'BOUND'}
end
if #ARGV[7] < 1 or #ARGV[7] > 128 or #ARGV[8] < 2 or #ARGV[8] > 256 or
   #ARGV[8] % 2 ~= 0 or string.match(ARGV[8], '^[0-9a-f]+$') == nil then return {'MALFORMED', 'IDENTITY'} end
if ARGV[9] ~= 'route-plan-v2' or ARGV[10] ~= '1' or #ARGV[11] > max_record or
   #ARGV[12] ~= 64 or string.match(ARGV[12], '^[0-9a-f]+$') == nil or
   not canonical_uint(ARGV[13], '9223372036854775807', false) or
   not canonical_uint(ARGV[14], '9223372036854775807', false) or
   ARGV[15] ~= 'PENDING' or ARGV[16] ~= '1' or ARGV[17] ~= '0' or ARGV[18] ~= '0' or ARGV[19] ~= '0' then
  return {'REVISION_CONFLICT'}
end
local updated = tonumber(ARGV[13])
local expires = tonumber(ARGV[14])
local physical = expires and expires + audit_ms or nil
if updated == nil or expires == nil or expires <= updated or expires <= now_ms or physical == nil or
   physical <= expires or physical > 9007199254740991 then return {'MALFORMED', 'CANDIDATE_TTL'} end
local candidate_bytes = #ARGV[11] + #ARGV[12] + 64
if candidate_bytes > max_record + 128 or candidate_bytes > max_bytes then return {'CAPACITY_EXCEEDED'} end

local meta_type = redis.call('TYPE', KEYS[1]).ok
local index_type = redis.call('TYPE', KEYS[2]).ok
local restart_type = redis.call('TYPE', KEYS[3]).ok
local accounting_type = redis.call('TYPE', KEYS[4]).ok
local plan_type = redis.call('TYPE', KEYS[5]).ok
local empty = meta_type == 'none' and index_type == 'none' and restart_type == 'none' and accounting_type == 'none'
if empty then
  if plan_type ~= 'none' then return {'MALFORMED', 'ORPHAN_PLAN'} end
  redis.call('HSET', KEYS[5],
    'schema', ARGV[9], 'plan_id', ARGV[7], 'revision', ARGV[10],
    'canonical_json', ARGV[11], 'checksum', ARGV[12], 'updated_at_ms', ARGV[13],
    'expires_at_ms', ARGV[14], 'phase', ARGV[15], 'attempt', ARGV[17],
    'started_at_ms', ARGV[18], 'completed_at_ms', ARGV[19])
  redis.call('ZADD', KEYS[2], string.format('%.0f', physical), ARGV[8])
  redis.call('RPUSH', KEYS[3], 'route-plan-restart-v1', ARGV[8])
  redis.call('HSET', KEYS[4], ARGV[8], tostring(candidate_bytes))
  redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', '1', 'bytes', tostring(candidate_bytes))
  local expiry_text = string.format('%.0f', physical)
  for index = 1, 5 do redis.call('PEXPIREAT', KEYS[index], expiry_text) end
  return {'COMMITTED'}
end
if meta_type ~= 'hash' or index_type ~= 'zset' or restart_type ~= 'list' or accounting_type ~= 'hash' then
  return {'MALFORMED', 'SHAPE'}
end
local meta_fields = {'schema','count','bytes'}
if not exact_hash(KEYS[1], meta_fields) or redis.call('HGET', KEYS[1], 'schema') ~= 'route-plan-meta-v1' then
  return {'MALFORMED', 'META_SHAPE'}
end
local count_text = redis.call('HGET', KEYS[1], 'count')
local bytes_text = redis.call('HGET', KEYS[1], 'bytes')
if not canonical_uint(count_text, '1048576', true) or not canonical_uint(bytes_text, '1073741824', true) then
  return {'MALFORMED', 'META_INTEGER'}
end
local count = tonumber(count_text)
local bytes = tonumber(bytes_text)
if count == nil or bytes == nil or count < 1 or count > max_items or bytes < 0 or bytes > max_bytes or
   redis.call('ZCARD', KEYS[2]) ~= count or redis.call('HLEN', KEYS[4]) ~= count then
  return {'MALFORMED', 'META_COUNTS'}
end
local restart_len = redis.call('LLEN', KEYS[3])
if restart_len < 1 or restart_len > count + 1 or restart_len > max_items + 1 or
   redis.call('LINDEX', KEYS[3], 0) ~= 'route-plan-restart-v1' then return {'MALFORMED', 'RESTART_SHAPE'} end
local tail = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #tail ~= 2 then return {'MALFORMED', 'TTL_TAIL'} end
local common_expiry = tonumber(tail[2])
if common_expiry == nil or common_expiry <= now_ms or common_expiry > 9007199254740991 then return {'MALFORMED', 'TTL_TAIL'} end
for index = 1, 4 do
  if tonumber(redis.call('PEXPIRETIME', KEYS[index])) ~= common_expiry then return {'MALFORMED', 'SHARED_TTL'} end
end

local score_text = redis.call('ZSCORE', KEYS[2], ARGV[8])
local accounted = redis.call('HGET', KEYS[4], ARGV[8])
local restart_positions = member_positions(KEYS[3], ARGV[8])
if plan_type ~= 'none' then
  local fields = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase','attempt','started_at_ms','completed_at_ms'}
  if not exact_hash(KEYS[5], fields) then return {'MALFORMED', 'CANDIDATE_SHAPE'} end
  local value = redis.call('HMGET', KEYS[5], unpack(fields))
  if value[1] ~= ARGV[9] or value[2] ~= ARGV[7] or value[3] ~= ARGV[10] or
     value[4] ~= ARGV[11] or value[5] ~= ARGV[12] or value[6] ~= ARGV[13] or
     value[7] ~= ARGV[14] or value[8] ~= ARGV[15] or value[9] ~= ARGV[17] or
     value[10] ~= ARGV[18] or value[11] ~= ARGV[19] then return {'REVISION_CONFLICT'} end
  if tonumber(score_text) ~= physical or accounted ~= tostring(candidate_bytes) or restart_positions ~= 1 or
     tonumber(redis.call('PEXPIRETIME', KEYS[5])) ~= common_expiry then return {'MALFORMED', 'CANDIDATE_REFERENCE'} end
  return {'ALREADY_COMMITTED'}
end
if score_text ~= false or accounted ~= false or restart_positions ~= 0 then return {'MALFORMED', 'DANGLING_CANDIDATE'} end
if count >= max_items or bytes + candidate_bytes > max_bytes then return {'CAPACITY_EXCEEDED'} end
if physical > common_expiry then return {'TAIL_EXTENSION_REQUIRED'} end

redis.call('HSET', KEYS[5],
  'schema', ARGV[9], 'plan_id', ARGV[7], 'revision', ARGV[10],
  'canonical_json', ARGV[11], 'checksum', ARGV[12], 'updated_at_ms', ARGV[13],
  'expires_at_ms', ARGV[14], 'phase', ARGV[15], 'attempt', ARGV[17],
  'started_at_ms', ARGV[18], 'completed_at_ms', ARGV[19])
redis.call('ZADD', KEYS[2], string.format('%.0f', physical), ARGV[8])
redis.call('HSET', KEYS[4], ARGV[8], tostring(candidate_bytes))
redis.call('RPUSH', KEYS[3], ARGV[8])
redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', tostring(count + 1), 'bytes', tostring(bytes + candidate_bytes))
local common_expiry_text = string.format('%.0f', common_expiry)
for index = 1, 5 do redis.call('PEXPIREAT', KEYS[index], common_expiry_text) end
return {'COMMITTED'}
`

// compareAndSwapExistingPlanLua is the execution hot path. The caller has
// already loaded and decoded one existing record and passes that exact tuple
// back as the CAS precondition. Redis rechecks the tuple and all target-owned
// references atomically, without materializing every RoutePlan in the index.
const compareAndSwapExistingPlanLua = `
if #KEYS ~= 5 or #ARGV ~= 32 then return {'MALFORMED', 'INPUT'} end
local tag = ARGV[1]
for index = 1, #KEYS do
  if string.sub(KEYS[index], 1, #tag + 2) ~= '{' .. tag .. '}' then return {'CROSS_SLOT'} end
end

local function canonical_uint(value, maximum, allow_zero)
  if type(value) ~= 'string' or value == '' or (not allow_zero and value == '0') or
     (allow_zero and value ~= '0' and string.match(value, '^[1-9][0-9]*$') == nil) or
     (not allow_zero and string.match(value, '^[1-9][0-9]*$') == nil) then return false end
  if #value < #maximum then return true end
  if #value > #maximum then return false end
  return value <= maximum
end
local function increment_uint64(value)
  if not canonical_uint(value, '18446744073709551615', false) or value == '18446744073709551615' then return nil end
  local carry = 1
  local out = ''
  for index = #value, 1, -1 do
    local digit = string.byte(value, index) - 48 + carry
    if digit >= 10 then digit = digit - 10; carry = 1 else carry = 0 end
    out = string.char(48 + digit) .. out
  end
  if carry == 1 then out = '1' .. out end
  return out
end
local function exact_hash(key, fields)
  if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HLEN', key) ~= #fields then return false end
  for index = 1, #fields do if redis.call('HEXISTS', key, fields[index]) ~= 1 then return false end end
  return true
end

local now_ms = tonumber(ARGV[2])
local max_items = tonumber(ARGV[3])
local max_bytes = tonumber(ARGV[4])
local max_record = tonumber(ARGV[5])
local audit_ms = tonumber(ARGV[6])
if now_ms == nil or max_items == nil or max_bytes == nil or max_record == nil or audit_ms == nil or
   max_items < 1 or max_bytes < 1 or max_record < 1 or max_record > max_bytes or audit_ms <= 0 or
   max_items > 1048576 or max_bytes > 1073741824 or max_record > 268435456 then return {'MALFORMED', 'BOUND'} end
if #ARGV[7] < 1 or #ARGV[7] > 128 or #ARGV[8] < 2 or #ARGV[8] > 256 or
   string.match(ARGV[8], '^[0-9a-f]+$') == nil or #ARGV[8] % 2 ~= 0 then return {'MALFORMED', 'IDENTITY'} end

local meta_fields = {'schema','count','bytes'}
if not exact_hash(KEYS[1], meta_fields) or redis.call('HGET', KEYS[1], 'schema') ~= 'route-plan-meta-v1' or
   redis.call('TYPE', KEYS[2]).ok ~= 'zset' or redis.call('TYPE', KEYS[3]).ok ~= 'list' or
   redis.call('TYPE', KEYS[4]).ok ~= 'hash' then return {'MALFORMED', 'SHAPE'} end
local count_text = redis.call('HGET', KEYS[1], 'count')
local bytes_text = redis.call('HGET', KEYS[1], 'bytes')
if not canonical_uint(count_text, '1048576', true) or not canonical_uint(bytes_text, '1073741824', true) then return {'MALFORMED', 'META_INTEGER'} end
local count = tonumber(count_text)
local bytes = tonumber(bytes_text)
if count == nil or bytes == nil or count < 1 or count > max_items or bytes < 0 or bytes > max_bytes or
   redis.call('ZCARD', KEYS[2]) ~= count or redis.call('HLEN', KEYS[4]) ~= count then return {'MALFORMED', 'META_COUNTS'} end

local current_fields = nil
local current_limits = nil
local current_field_count = redis.call('HLEN', KEYS[5])
if current_field_count == 8 then
  current_fields = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase'}
  current_limits = {32,128,20,max_record,64,20,20,16}
elseif current_field_count == 11 then
  current_fields = {'schema','plan_id','revision','canonical_json','checksum','updated_at_ms','expires_at_ms','phase','attempt','started_at_ms','completed_at_ms'}
  current_limits = {32,128,20,max_record,64,20,20,16,20,20,20}
else return {'MALFORMED', 'CURRENT_SCHEMA'} end
if not exact_hash(KEYS[5], current_fields) then return {'MALFORMED', 'CURRENT_SHAPE'} end
for index = 1, #current_fields do
  if redis.call('HSTRLEN', KEYS[5], current_fields[index]) > current_limits[index] then return {'MALFORMED', 'CURRENT_FIELD'} end
end
local current = redis.call('HMGET', KEYS[5], unpack(current_fields))
if (current_field_count == 8 and current[1] ~= 'route-plan-v1') or
   (current_field_count == 11 and current[1] ~= 'route-plan-v2') then return {'MALFORMED', 'CURRENT_SCHEMA'} end
if current[2] ~= ARGV[11] or current[3] ~= ARGV[12] or
   current[4] ~= ARGV[13] or current[5] ~= ARGV[14] or current[6] ~= ARGV[15] or
   current[7] ~= ARGV[16] or current[8] ~= ARGV[17] then return {'REVISION_CONFLICT'} end
if #current_fields == 11 and (current[9] ~= ARGV[18] or current[10] ~= ARGV[19] or current[11] ~= ARGV[20]) then return {'REVISION_CONFLICT'} end
if ARGV[11] ~= ARGV[7] or not canonical_uint(ARGV[9], '18446744073709551615', false) then return {'REVISION_CONFLICT'} end

local score_text = redis.call('ZSCORE', KEYS[2], ARGV[8])
if score_text == false or redis.call('HSTRLEN', KEYS[4], ARGV[8]) > 20 then return {'MALFORMED', 'REFERENCE_MISSING'} end
local score = tonumber(score_text)
local current_expires = tonumber(ARGV[16])
local current_bytes = #ARGV[13] + #ARGV[14] + 64
if score == nil or current_expires == nil or score ~= current_expires + audit_ms or score > 9007199254740991 or
   redis.call('HGET', KEYS[4], ARGV[8]) ~= tostring(current_bytes) then return {'MALFORMED', 'REFERENCE_VALUE'} end

local tail = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
if #tail ~= 2 then return {'MALFORMED', 'TTL_TAIL'} end
local common_expiry = tonumber(tail[2])
if common_expiry == nil or common_expiry < score then return {'MALFORMED', 'TTL_TAIL'} end
for index = 1, 4 do
  if tonumber(redis.call('PEXPIRETIME', KEYS[index])) ~= common_expiry then return {'MALFORMED', 'SHARED_TTL'} end
end
if tonumber(redis.call('PEXPIRETIME', KEYS[5])) ~= common_expiry then return {'MALFORMED', 'PLAN_TTL'} end

local restart_values = redis.call('LRANGE', KEYS[3], 0, count)
if #restart_values < 1 or #restart_values > count + 1 or restart_values[1] ~= 'route-plan-restart-v1' then return {'MALFORMED', 'RESTART_SHAPE'} end
local restart_count = 0
for index = 2, #restart_values do if restart_values[index] == ARGV[8] then restart_count = restart_count + 1 end end
local current_terminal = ARGV[17] == 'SUCCEEDED' or ARGV[17] == 'FAILED' or ARGV[17] == 'PAUSED'
if restart_count > 1 or (not current_terminal and current_expires > now_ms and restart_count ~= 1) or
   (current_terminal and restart_count ~= 0) then return {'MALFORMED', 'RESTART_REFERENCE'} end

if ARGV[21] ~= 'route-plan-v2' or ARGV[22] ~= ARGV[7] or not canonical_uint(ARGV[23], '18446744073709551615', false) or
   #ARGV[24] > max_record or #ARGV[25] < 1 or #ARGV[25] > 64 or
   not canonical_uint(ARGV[26], '9223372036854775807', false) or not canonical_uint(ARGV[27], '9223372036854775807', false) or
   not canonical_uint(ARGV[30], '18446744073709551615', true) or not canonical_uint(ARGV[31], '9223372036854775807', true) or
   not canonical_uint(ARGV[32], '9223372036854775807', true) or (ARGV[29] ~= '0' and ARGV[29] ~= '1') then return {'MALFORMED', 'CANDIDATE'} end

if ARGV[23] == ARGV[12] and ARGV[24] == ARGV[13] and ARGV[25] == ARGV[14] and ARGV[26] == ARGV[15] and
   ARGV[27] == ARGV[16] and ARGV[28] == ARGV[17] and ARGV[30] == ARGV[18] and ARGV[31] == ARGV[19] and ARGV[32] == ARGV[20] then
  return {'ALREADY_COMMITTED'}
end
if score <= now_ms then return {'REVISION_CONFLICT'} end
if ARGV[12] ~= ARGV[9] or ARGV[23] ~= increment_uint64(ARGV[9]) then return {'REVISION_CONFLICT'} end
local candidate_expires = tonumber(ARGV[27])
if candidate_expires == nil or candidate_expires + audit_ms ~= score then return {'REVISION_CONFLICT'} end
local candidate_bytes = #ARGV[24] + #ARGV[25] + 64
local projected = bytes - current_bytes + candidate_bytes
if candidate_bytes > max_record + 128 or projected < 0 or projected > max_bytes then return {'CAPACITY_EXCEEDED'} end

redis.call('HSET', KEYS[5],
  'schema', ARGV[21], 'plan_id', ARGV[22], 'revision', ARGV[23],
  'canonical_json', ARGV[24], 'checksum', ARGV[25], 'updated_at_ms', ARGV[26],
  'expires_at_ms', ARGV[27], 'phase', ARGV[28], 'attempt', ARGV[30],
  'started_at_ms', ARGV[31], 'completed_at_ms', ARGV[32])
redis.call('ZADD', KEYS[2], score_text, ARGV[8])
redis.call('HSET', KEYS[4], ARGV[8], tostring(candidate_bytes))
redis.call('LREM', KEYS[3], 0, ARGV[8])
if ARGV[29] == '1' then redis.call('RPUSH', KEYS[3], ARGV[8]) end
redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', count_text, 'bytes', tostring(projected))
local common_expiry_text = string.format('%.0f', common_expiry)
for index = 1, 4 do redis.call('PEXPIREAT', KEYS[index], common_expiry_text) end
redis.call('PEXPIREAT', KEYS[5], common_expiry_text)
return {'COMMITTED'}
`

// stopOwnedPlanLua is deliberately a separate mutation path. The caller may
// pre-read the canonical plan to construct the terminal envelope, but this
// script revalidates the complete target tuple and all shared references before
// its first write. The terminal plan, restart list, accounting and meta update
// therefore commit as one Redis operation.
const stopOwnedPlanLua = `
if #KEYS ~= 6 or #ARGV ~= 35 then return {'CORRUPT', 'INPUT'} end
local tag = ARGV[1]
for index = 1, #KEYS do
  if string.sub(KEYS[index], 1, #tag + 2) ~= '{' .. tag .. '}' then return {'CROSS_SLOT'} end
end
local now_ms = tonumber(ARGV[5])
local max_items = tonumber(ARGV[6])
local max_bytes = tonumber(ARGV[7])
local max_record = tonumber(ARGV[8])
local audit_ms = tonumber(ARGV[9])
if now_ms == nil or max_items == nil or max_bytes == nil or max_record == nil or audit_ms == nil or
   max_items < 1 or max_bytes < 1 or max_record < 1 or audit_ms <= 0 or
   max_items > 9007199254740991 or max_bytes > 9007199254740991 or max_record > max_bytes or
   audit_ms > 9007199254740991 then return {'CORRUPT', 'BOUND'} end

local function exact_hash(key, fields)
  if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HLEN', key) ~= #fields then return false end
  for index = 1, #fields do
    if redis.call('HEXISTS', key, fields[index]) ~= 1 then return false end
  end
  return true
end

local function canonical_uint(value, maximum, allow_zero)
  if value == nil or value == '' or (not allow_zero and value == '0') or
     (allow_zero and value ~= '0' and string.match(value, '^[1-9][0-9]*$') == nil) or
     (not allow_zero and string.match(value, '^[1-9][0-9]*$') == nil) then return false end
  if value == '0' then return true end
  if #value < #maximum then return true end
  if #value > #maximum then return false end
  return value <= maximum
end

local function stop_audit_status(key)
  local kind = redis.call('TYPE', key).ok
  if kind == 'none' then return 'MISSING' end
  if kind ~= 'hash' or redis.call('HLEN', key) ~= 4 then return 'CORRUPT' end
  local fields = {'schema', 'owner_scale_plan_id', 'operation_id', 'fence_epoch'}
  if not exact_hash(key, fields) then return 'CORRUPT' end
  local schema = redis.call('HGET', key, 'schema')
  local owner = redis.call('HGET', key, 'owner_scale_plan_id')
  local operation = redis.call('HGET', key, 'operation_id')
  local epoch = redis.call('HGET', key, 'fence_epoch')
  if schema ~= 'route-plan-stop-audit-v1' or owner == nil or operation == nil or epoch == nil or
     #owner < 1 or #owner > 128 or #operation < 1 or #operation > 256 or
     not canonical_uint(epoch, '9007199254740991', true) then return 'CORRUPT' end
  return 'VALID'
end

local function exact_field(key, field, expected, maximum)
  if redis.call('HSTRLEN', key, field) > maximum then return false end
  return redis.call('HGET', key, field) == expected
end

local function exact_current_plan(key)
  local field_count = redis.call('HLEN', key)
  local values = nil
  if field_count == 8 then
    local fields = {'schema', 'plan_id', 'revision', 'canonical_json', 'checksum', 'updated_at_ms', 'expires_at_ms', 'phase'}
    if not exact_hash(key, fields) then return 'CORRUPT' end
    values = redis.call('HMGET', key, unpack(fields))
    for index = 1, #values do
      if redis.call('HSTRLEN', key, fields[index]) > ({32,128,20,max_record,64,20,20,16})[index] then return 'CORRUPT' end
    end
    if values[1] ~= ARGV[10] or values[2] ~= ARGV[11] or values[3] ~= ARGV[12] or
       values[4] ~= ARGV[13] or values[5] ~= ARGV[14] or values[6] ~= ARGV[15] or
       values[7] ~= ARGV[16] or values[8] ~= ARGV[17] then return 'CONFLICT' end
  elseif field_count == 11 then
    local fields = {'schema', 'plan_id', 'revision', 'canonical_json', 'checksum', 'updated_at_ms', 'expires_at_ms', 'phase', 'attempt', 'started_at_ms', 'completed_at_ms'}
    if not exact_hash(key, fields) then return 'CORRUPT' end
    values = redis.call('HMGET', key, unpack(fields))
    local limits = {32,128,20,max_record,64,20,20,16,20,20,20}
    for index = 1, #values do
      if redis.call('HSTRLEN', key, fields[index]) > limits[index] then return 'CORRUPT' end
    end
    if values[1] ~= ARGV[10] or values[2] ~= ARGV[11] or values[3] ~= ARGV[12] or
       values[4] ~= ARGV[13] or values[5] ~= ARGV[14] or values[6] ~= ARGV[15] or
       values[7] ~= ARGV[16] or values[8] ~= ARGV[17] or values[9] ~= ARGV[18] or
       values[10] ~= ARGV[19] or values[11] ~= ARGV[20] then return 'CONFLICT' end
  else
    return 'CORRUPT'
  end
  return values
end

if not exact_hash(KEYS[1], {'schema', 'count', 'bytes'}) or
   redis.call('HGET', KEYS[1], 'schema') ~= 'route-plan-meta-v1' then return {'CORRUPT', 'META'} end
if redis.call('TYPE', KEYS[2]).ok ~= 'zset' or redis.call('TYPE', KEYS[3]).ok ~= 'list' or
   redis.call('TYPE', KEYS[4]).ok ~= 'hash' then return {'CORRUPT', 'SHAPE'} end
local count_text = redis.call('HGET', KEYS[1], 'count')
local bytes_text = redis.call('HGET', KEYS[1], 'bytes')
if not canonical_uint(count_text, '1048576', true) or not canonical_uint(bytes_text, '1073741824', true) then return {'CORRUPT', 'META_INTEGER'} end
local count = tonumber(count_text)
local bytes = tonumber(bytes_text)
if count == nil or bytes == nil or count < 1 or count > max_items or bytes < 0 or bytes > max_bytes or
   redis.call('ZCARD', KEYS[2]) ~= count or redis.call('HLEN', KEYS[4]) ~= count then return {'CORRUPT', 'META_COUNTS'} end

local current_result = exact_current_plan(KEYS[5])
if current_result == 'CORRUPT' then return {'CORRUPT', 'PLAN'} end
if current_result == 'CONFLICT' then return {'REVISION_CONFLICT'} end
local current = current_result
if not canonical_uint(current[3], '18446744073709551615', false) then return {'CORRUPT', 'REVISION'} end
local audit_status = stop_audit_status(KEYS[6])
if audit_status == 'CORRUPT' then return {'CORRUPT', 'AUDIT'} end

local member_score = redis.call('ZSCORE', KEYS[2], ARGV[2])
if member_score == false then return {'CORRUPT', 'INDEX_MISSING'} end
local score = tonumber(member_score)
local expires = tonumber(current[7])
if score == nil or expires == nil or score <= expires or score - expires ~= audit_ms then return {'CORRUPT', 'INDEX_SCORE'} end
if score <= now_ms then return {'REVISION_CONFLICT'} end
local current_bytes = #current[4] + #current[5] + 64
if redis.call('HGET', KEYS[4], ARGV[2]) ~= string.format('%d', current_bytes) then return {'CORRUPT', 'ACCOUNTING'} end

local restart_values = redis.call('LRANGE', KEYS[3], 0, -1)
if #restart_values < 1 or #restart_values > max_items + 1 or restart_values[1] ~= 'route-plan-restart-v1' then return {'CORRUPT', 'RESTART_SHAPE'} end
local restart_count = 0
local duplicate = false
for index = 2, #restart_values do
  if restart_values[index] == ARGV[2] then restart_count = restart_count + 1 end
  for previous = 2, index - 1 do
    if restart_values[previous] == restart_values[index] then duplicate = true end
  end
end
if duplicate then return {'CORRUPT', 'RESTART_DUPLICATE'} end
local current_terminal = current[8] == 'SUCCEEDED' or current[8] == 'FAILED' or current[8] == 'PAUSED'
if current_terminal then
  if restart_count ~= 0 or (ARGV[32] ~= 'ALREADY_STOPPED' and ARGV[32] ~= 'ALREADY_TERMINAL') then return {'CORRUPT', 'TERMINAL_RESTART'} end
  if ARGV[32] == 'ALREADY_STOPPED' then
    if audit_status ~= 'VALID' or redis.call('HGET', KEYS[6], 'owner_scale_plan_id') ~= ARGV[33] then return {'CORRUPT', 'AUDIT_MISSING'} end
  elseif audit_status == 'VALID' and redis.call('HGET', KEYS[6], 'owner_scale_plan_id') ~= ARGV[33] then
    return {'CORRUPT', 'AUDIT_OWNER'}
  end
  return {ARGV[32]}
end
if current[3] ~= ARGV[3] then return {'REVISION_CONFLICT'} end
if ARGV[4] ~= '' and current[8] ~= ARGV[4] then return {'REVISION_CONFLICT'} end
if restart_count ~= 1 then return {'CORRUPT', 'RESTART_MISSING'} end
if audit_status ~= 'MISSING' then return {'CORRUPT', 'AUDIT_PRESENT'} end
if ARGV[21] ~= 'route-plan-v2' or ARGV[22] ~= ARGV[11] or ARGV[28] ~= 'PAUSED' then return {'CORRUPT', 'CANDIDATE'} end
if not canonical_uint(ARGV[23], '18446744073709551615', false) or ARGV[23] ~= string.format('%d', tonumber(ARGV[3]) + 1) then return {'REVISION_CONFLICT'} end
if #ARGV[24] > max_record or #ARGV[25] == 0 or #ARGV[25] > 64 or
   ARGV[27] ~= current[7] or not canonical_uint(ARGV[26], '9223372036854775807', false) or
   not canonical_uint(ARGV[29], '18446744073709551615', true) or
   not canonical_uint(ARGV[30], '9223372036854775807', true) or
   not canonical_uint(ARGV[31], '9223372036854775807', true) then return {'CORRUPT', 'CANDIDATE_FIELDS'} end
local candidate_expires = tonumber(ARGV[27])
local candidate_score = candidate_expires and candidate_expires + audit_ms or nil
if candidate_score == nil or candidate_score ~= score or candidate_score > 9007199254740991 then return {'CORRUPT', 'CANDIDATE_SCORE'} end
local candidate_bytes = #ARGV[24] + #ARGV[25] + 64
local projected = bytes - current_bytes + candidate_bytes
if candidate_bytes > max_record + 128 or projected < 0 or projected > max_bytes then return {'CAPACITY_EXCEEDED'} end

redis.call('HSET', KEYS[5],
  'schema', ARGV[21], 'plan_id', ARGV[22], 'revision', ARGV[23],
  'canonical_json', ARGV[24], 'checksum', ARGV[25], 'updated_at_ms', ARGV[26],
  'expires_at_ms', ARGV[27], 'phase', ARGV[28], 'attempt', ARGV[29],
  'started_at_ms', ARGV[30], 'completed_at_ms', ARGV[31])
redis.call('HSET', KEYS[4], ARGV[2], string.format('%d', candidate_bytes))
redis.call('LREM', KEYS[3], 0, ARGV[2])
redis.call('HSET', KEYS[1], 'schema', 'route-plan-meta-v1', 'count', count_text, 'bytes', string.format('%d', projected))
redis.call('HSET', KEYS[6],
  'schema', 'route-plan-stop-audit-v1', 'owner_scale_plan_id', ARGV[33],
  'operation_id', ARGV[34], 'fence_epoch', ARGV[35])
redis.call('PEXPIREAT', KEYS[6], string.format('%.0f', score))
return {'COMMITTED'}
`

type scriptDef struct {
	name string
	src  string
	sha  string
}

type scriptRegistry struct {
	client redis.UniversalClient
	defs   map[string]scriptDef
}

func newScriptRegistry(client redis.UniversalClient) (*scriptRegistry, error) {
	if client == nil {
		return nil, ErrInvalidKey
	}
	defs := make(map[string]scriptDef, 6)
	for _, value := range []scriptDef{{name: scriptCompareAndSwapPlan, src: compareAndSwapPlanLua}, {name: scriptCompareAndSwapCreatePlan, src: compareAndSwapCreatePlanLua}, {name: scriptCompareAndSwapExistingPlan, src: compareAndSwapExistingPlanLua}, {name: scriptLoadPlan, src: loadPlanLua}, {name: scriptReadPlanIndex, src: readPlanIndexLua}, {name: scriptStopOwnedPlan, src: stopOwnedPlanLua}} {
		sum := sha1.Sum([]byte(value.src))
		value.sha = fmt.Sprintf("%x", sum[:])
		defs[value.name] = value
	}
	return &scriptRegistry{client: client, defs: defs}, nil
}

func (r *scriptRegistry) scriptSHA(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	def, ok := r.defs[name]
	return def.sha, ok
}

func (r *scriptRegistry) load(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrInvalidKey
	}
	for _, name := range []string{scriptCompareAndSwapPlan, scriptCompareAndSwapCreatePlan, scriptCompareAndSwapExistingPlan, scriptLoadPlan, scriptReadPlanIndex, scriptStopOwnedPlan} {
		def := r.defs[name]
		sha, err := r.client.ScriptLoad(ctx, def.src).Result()
		if err != nil {
			return err
		}
		if sha != def.sha {
			return fmt.Errorf("%w: script sha mismatch", ErrInvalidKey)
		}
	}
	return nil
}

func (r *scriptRegistry) run(ctx context.Context, keys []string, args ...any) ([]string, error) {
	return r.runNamedMode(ctx, scriptCompareAndSwapPlan, keys, true, args...)
}

func (r *scriptRegistry) runNamed(ctx context.Context, name string, keys []string, args ...any) ([]string, error) {
	return r.runNamedMode(ctx, name, keys, false, args...)
}

func (r *scriptRegistry) runNamedMode(ctx context.Context, name string, keys []string, noRetry bool, args ...any) ([]string, error) {
	if r == nil {
		return nil, ErrInvalidKey
	}
	def, ok := r.defs[name]
	if !ok {
		return nil, ErrInvalidKey
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var reply any
		var err error
		if noRetry {
			reply, err = evalShaNoRetry(ctx, r.client, def.sha, keys, args...)
		} else {
			reply, err = r.client.EvalSha(ctx, def.sha, keys, args...).Result()
		}
		if err == nil {
			return scriptStrings(reply)
		}
		if !strings.Contains(err.Error(), "NOSCRIPT") {
			return nil, err
		}
		lastErr = err
		sha, loadErr := r.client.ScriptLoad(ctx, def.src).Result()
		if loadErr != nil {
			return nil, loadErr
		}
		if sha != def.sha {
			return nil, fmt.Errorf("%w: script sha mismatch", ErrInvalidKey)
		}
	}
	return nil, lastErr
}

type noRetryEvalShaCommand struct{ *redis.Cmd }

func (*noRetryEvalShaCommand) NoRetry() bool { return true }

func evalShaNoRetry(ctx context.Context, client redis.UniversalClient, sha string, keys []string, args ...any) (any, error) {
	commandArgs := make([]any, 3+len(keys), 3+len(keys)+len(args))
	commandArgs[0] = "evalsha"
	commandArgs[1] = sha
	commandArgs[2] = len(keys)
	for index, key := range keys {
		commandArgs[3+index] = key
	}
	commandArgs = append(commandArgs, args...)
	base := redis.NewCmd(ctx, commandArgs...)
	if len(keys) > 0 {
		base.SetFirstKeyPos(3)
	}
	command := &noRetryEvalShaCommand{Cmd: base}
	processErr := client.Process(ctx, command)
	reply, resultErr := command.Result()
	if resultErr != nil {
		return nil, resultErr
	}
	if processErr != nil {
		return nil, processErr
	}
	return reply, nil
}

func scriptStrings(reply any) ([]string, error) {
	array, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: script reply type", ErrInvalidKey)
	}
	result := make([]string, 0, len(array))
	for _, item := range array {
		switch value := item.(type) {
		case string:
			result = append(result, value)
		case []byte:
			result = append(result, string(value))
		case int64:
			result = append(result, fmt.Sprintf("%d", value))
		case nil:
			result = append(result, "")
		default:
			return nil, fmt.Errorf("%w: script reply element", ErrInvalidKey)
		}
	}
	return result, nil
}
