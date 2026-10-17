package routecontrol

import (
	"context"
	"crypto/sha1" //nolint:gosec // Redis SCRIPT identity, not a security primitive.
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

const (
	ScriptRegisterHolder            = "RegisterHolderV6"
	ScriptHeartbeatHolder           = "HeartbeatHolderV6"
	ScriptGetPlacementExclusion     = "GetPlacementExclusionV1"
	ScriptExcludePlacement          = "ExcludePlacementV1"
	ScriptReleasePlacementExclusion = "ReleasePlacementExclusionV1"

	HolderScriptRegister  = ScriptRegisterHolder
	HolderScriptHeartbeat = ScriptHeartbeatHolder
)

const holderRecordSchema = "holder-record-v5"

var holderRecordFields = [...]string{
	"schema_version",
	"domain",
	"holder_id",
	"boot_id",
	"pod_uid",
	"workload_uid",
	"ordinal",
	"logic_endpoint",
	"holder_control_endpoint",
	"tls_dns_identity",
	"state",
	"process_ready",
	"draining",
	"serving_shard_count",
	"snapshot_revision",
	"started_at_ms",
	"heartbeat_at_ms",
	"lease_until_ms",
	"sequence",
	"capacity_weight",
	"resource_summary",
	"record_bytes",
	"record_digest",
	"record_integrity",
}

const holderMetaSchema = "holder-registry-v5"

var holderMetaFields = [...]string{
	"schema_version",
	"registry_revision",
	"holder_count",
	"record_bytes",
	"resource_bytes",
}

const holderLuaCommon = `
local MAX_SAFE_INTEGER = 9007199254740991
local MAX_SAFE_INTEGER_STRING = '9007199254740991'
local MAX_REVISION_STRING = '18446744073709551615'
local MAX_INT32_STRING = '2147483647'
local RECORD_SCHEMA = 'holder-record-v5'
local META_SCHEMA = 'holder-registry-v5'
local RESOURCE_PREFIX = 'RS1:'
local DIGEST_PREFIX = 'HD5:'
local RECORD_WIRE_PREFIX = 'HR5:'
local MAX_RESOURCE_KEY = 64
local MAX_RESOURCE_VALUE = 256
local REGISTRY_TTL_MARGIN = 1

local RECORD_SEMANTIC_FIELDS = {
  'schema_version', 'domain', 'holder_id', 'boot_id',
  'pod_uid', 'workload_uid', 'ordinal',
  'logic_endpoint', 'holder_control_endpoint', 'tls_dns_identity', 'state',
  'process_ready', 'draining', 'serving_shard_count',
  'snapshot_revision', 'started_at_ms', 'heartbeat_at_ms',
  'lease_until_ms', 'sequence', 'capacity_weight', 'resource_summary'
}

local RECORD_VALUE_FIELDS = {
  'schema_version', 'domain', 'holder_id', 'boot_id',
  'pod_uid', 'workload_uid', 'ordinal',
  'logic_endpoint', 'holder_control_endpoint', 'tls_dns_identity', 'state',
  'process_ready', 'draining', 'serving_shard_count',
  'snapshot_revision', 'started_at_ms', 'heartbeat_at_ms',
  'lease_until_ms', 'sequence', 'capacity_weight',
  'resource_summary', 'record_bytes', 'record_digest'
}

local RECORD_FIELDS = {
  'schema_version', 'domain', 'holder_id', 'boot_id',
  'pod_uid', 'workload_uid', 'ordinal',
  'logic_endpoint', 'holder_control_endpoint', 'tls_dns_identity', 'state',
  'process_ready', 'draining', 'serving_shard_count',
  'snapshot_revision', 'started_at_ms', 'heartbeat_at_ms',
  'lease_until_ms', 'sequence', 'capacity_weight',
  'resource_summary', 'record_bytes', 'record_digest', 'record_integrity'
}

local function same_tag(tag)
  if tag == nil or tag == '' then return false end
  for i = 1, #KEYS do
    if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then
      return false
    end
  end
  return true
end

local function normalized_uint(value)
  if value == nil or value == '' or string.match(value, '^%d+$') == nil then return nil end
  value = string.gsub(value, '^0+', '')
  if value == '' then value = '0' end
  return value
end

local function safe_uint(value)
  local normalized = normalized_uint(value)
  if normalized == nil or normalized ~= value or #value > #MAX_SAFE_INTEGER_STRING or
     (#value == #MAX_SAFE_INTEGER_STRING and value > MAX_SAFE_INTEGER_STRING) then
    return nil
  end
  return value
end

local function safe_num(value)
  value = safe_uint(value)
  if value == nil then return nil end
  return tonumber(value)
end

local function valid_revision(value)
  return value ~= nil and value ~= '0' and normalized_uint(value) == value and
    (#value < #MAX_REVISION_STRING or
     (#value == #MAX_REVISION_STRING and value <= MAX_REVISION_STRING))
end

local function increment_revision(value)
  if value == '0' then return '1' end
  if not valid_revision(value) or value == MAX_REVISION_STRING then return nil end
  local result = {}
  local carry = 1
  for i = #value, 1, -1 do
    local digit = string.byte(value, i) - string.byte('0')
    local next_digit = digit + carry
    if next_digit >= 10 then
      next_digit = next_digit - 10
      carry = 1
    else
      carry = 0
    end
    result[i] = string.char(string.byte('0') + next_digit)
  end
  if carry == 1 then
    table.insert(result, 1, '1')
  end
  local revision = table.concat(result)
  if not valid_revision(revision) then return nil end
  return revision
end

local function uint_cmp(left, right)
  left = safe_uint(left)
  right = safe_uint(right)
  if left == nil or right == nil then return nil end
  if #left < #right then return -1 end
  if #left > #right then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end

local function uint_at_least(left, right)
  local cmp = uint_cmp(left, right)
  return cmp ~= nil and cmp >= 0
end

local function uint_greater(left, right)
  local cmp = uint_cmp(left, right)
  return cmp ~= nil and cmp > 0
end

local function required_hash(key, fields)
  if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HLEN', key) ~= #fields then return false end
  for i = 1, #fields do
    if redis.call('HEXISTS', key, fields[i]) ~= 1 then return false end
  end
  return true
end

local function field_len_ok(key, field, limit)
  return redis.call('HSTRLEN', key, field) <= limit
end

local function valid_meta(key, max_holders, max_total_record, max_total_resource)
  local kind = redis.call('TYPE', key).ok
  if kind == 'none' then return true end
  if not required_hash(key, {
    'schema_version', 'registry_revision', 'holder_count', 'record_bytes', 'resource_bytes'
  }) then return false end
  if not field_len_ok(key, 'schema_version', #META_SCHEMA) or
     not field_len_ok(key, 'registry_revision', #MAX_REVISION_STRING) or
     not field_len_ok(key, 'holder_count', #MAX_SAFE_INTEGER_STRING) or
     not field_len_ok(key, 'record_bytes', #MAX_SAFE_INTEGER_STRING) or
     not field_len_ok(key, 'resource_bytes', #MAX_SAFE_INTEGER_STRING) then
    return false
  end
  if redis.call('HGET', key, 'schema_version') ~= META_SCHEMA then return false end
  local revision = redis.call('HGET', key, 'registry_revision')
  local count = safe_num(redis.call('HGET', key, 'holder_count'))
  local bytes = safe_num(redis.call('HGET', key, 'record_bytes'))
  local resources = safe_num(redis.call('HGET', key, 'resource_bytes'))
  return valid_revision(revision) and count ~= nil and bytes ~= nil and resources ~= nil and
    count <= max_holders and bytes <= max_total_record and resources <= max_total_resource
end

local function valid_digest(value)
  return value ~= nil and #value == 40 and string.match(value, '^[0-9a-f]+$') ~= nil
end

local function canonical_safe_num(value)
  local normalized = safe_uint(value)
  if normalized == nil or normalized ~= value then return nil end
  return tonumber(normalized)
end

local function valid_text(value, limit)
  if value == nil or value == '' or #value > limit or
     string.match(value, '^%s') ~= nil or string.match(value, '%s$') ~= nil then
    return false
  end
  for position = 1, #value do
    local byte = string.byte(value, position)
    if byte < 32 or byte == 127 then return false end
  end
  return true
end

local function valid_port(value)
  if value == nil or value == '' or string.match(value, '^%d+$') == nil or
     (#value > 1 and string.sub(value, 1, 1) == '0') then return false end
  local number = tonumber(value)
  return number ~= nil and number >= 1 and number <= 65535 and tostring(number) == value
end

local function valid_ipv4(value)
  local start_at = 1
  for part = 1, 4 do
    local separator = string.find(value, '.', start_at, true)
    if (part < 4 and separator == nil) or (part == 4 and separator ~= nil) then return false end
    local finish = separator and separator - 1 or #value
    local token = string.sub(value, start_at, finish)
    if token == '' or string.match(token, '^%d+$') == nil or
       (#token > 1 and string.sub(token, 1, 1) == '0') then return false end
    local number = tonumber(token)
    if number == nil or number > 255 or tostring(number) ~= token then return false end
    start_at = finish + 2
  end
  return true
end

local function numeric_dotted(value)
  if string.find(value, '.', 1, true) == nil then return false end
  for index = 1, #value do
    local byte = string.byte(value, index)
    if byte ~= 46 and (byte < 48 or byte > 57) then return false end
  end
  return true
end

local function valid_dns_host(value)
  if value == nil or value == '' or #value > 253 or numeric_dotted(value) then return false end
  local start_at = 1
  while true do
    local separator = string.find(value, '.', start_at, true)
    local finish = separator and separator - 1 or #value
    local length = finish - start_at + 1
    if length <= 0 or length > 63 or string.sub(value, start_at, start_at) == '-' or
       string.sub(value, finish, finish) == '-' then return false end
    for index = start_at, finish do
      local byte = string.byte(value, index)
      if not ((byte >= 97 and byte <= 122) or (byte >= 48 and byte <= 57) or byte == 45) then
        return false
      end
    end
    if separator == nil then break end
    start_at = separator + 1
  end
  return true
end

local function parse_ipv6_side(value, groups)
  if value == '' then return true end
  local start_at = 1
  while true do
    local separator = string.find(value, ':', start_at, true)
    local finish = separator and separator - 1 or #value
    local token = string.sub(value, start_at, finish)
    if token == '' or #token > 4 or string.match(token, '^[0-9a-f]+$') == nil or
       (#token > 1 and string.sub(token, 1, 1) == '0') then return false end
    local number = tonumber(token, 16)
    if number == nil then return false end
    groups[#groups + 1] = number
    if separator == nil then break end
    start_at = separator + 1
  end
  return true
end

local function valid_ipv6(value)
  if value == nil or value == '' or string.find(value, '.', 1, true) or
     string.find(value, '%', 1, true) or string.match(value, '^[0-9a-f:]+$') == nil then return false end
  local compressed = string.find(value, '::', 1, true)
  if compressed ~= nil and string.find(value, '::', compressed + 2, true) ~= nil then return false end
  local groups = {}
  local left_count = 0
  local compressed_length = 0
  if compressed ~= nil then
    local left = string.sub(value, 1, compressed - 1)
    local right = string.sub(value, compressed + 2)
    if not parse_ipv6_side(left, groups) then return false end
    left_count = #groups
    if not parse_ipv6_side(right, groups) then return false end
    compressed_length = 8 - #groups
    if compressed_length < 1 then return false end
    local expanded = {}
    for index = 1, left_count do expanded[#expanded + 1] = groups[index] end
    for _ = 1, compressed_length do expanded[#expanded + 1] = 0 end
    for index = left_count + 1, #groups do expanded[#expanded + 1] = groups[index] end
    groups = expanded
  else
    if not parse_ipv6_side(value, groups) or #groups ~= 8 then return false end
  end
  if #groups ~= 8 then return false end
  -- Go netip classifies the complete 0000:0000:0000:0000:0000:ffff::/96
  -- range as IPv4-in-IPv6, including pure-hex forms. Holder endpoints reject
  -- that entire range rather than accepting a Lua-only spelling.
  if groups[1] == 0 and groups[2] == 0 and groups[3] == 0 and groups[4] == 0 and
     groups[5] == 0 and groups[6] == 65535 then return false end
  local best_start, best_length = 0, 0
  local index = 1
  while index <= 8 do
    if groups[index] ~= 0 then
      index = index + 1
    else
      local finish = index
      while finish <= 8 and groups[finish] == 0 do finish = finish + 1 end
      local length = finish - index
      if length > best_length then best_start, best_length = index, length end
      index = finish
    end
  end
  if compressed ~= nil then
    return best_length >= 2 and best_start == left_count + 1 and best_length == compressed_length
  end
  return best_length < 2
end

local function valid_endpoint(value, limit)
  if value == nil or value == '' or #value > limit then return false end
  local host, port = nil, nil
  if string.sub(value, 1, 1) == '[' then
    local close_at = string.find(value, ']', 2, true)
    if close_at == nil or string.sub(value, close_at + 1, close_at + 1) ~= ':' then return false end
    host = string.sub(value, 2, close_at - 1)
    port = string.sub(value, close_at + 2)
    return valid_port(port) and valid_ipv6(host)
  end
  local separator = string.find(value, ':', 1, true)
  if separator == nil or string.find(value, ':', separator + 1, true) ~= nil then return false end
  host = string.sub(value, 1, separator - 1)
  port = string.sub(value, separator + 1)
  return valid_port(port) and (valid_ipv4(host) or valid_dns_host(host))
end

local function valid_resource_text(value, limit)
  if value == nil or value == '' or #value > limit or
     string.byte(value, 1) == 32 or string.byte(value, #value) == 32 then
    return false
  end
  for position = 1, #value do
    local byte = string.byte(value, position)
    if byte < 32 or byte > 126 then return false end
  end
  return true
end

local function record_values(key)
  return redis.call('HMGET', key, unpack(RECORD_FIELDS))
end

local function record_value_integrity(values)
  if values == nil or #values < #RECORD_VALUE_FIELDS then return nil end
  local framed = {}
  for i = 1, #RECORD_VALUE_FIELDS do
    local field = RECORD_VALUE_FIELDS[i]
    local value = values[i]
    if value == nil or value == false then return nil end
    framed[#framed + 1] = tostring(#field) .. ':' .. field .. tostring(#value) .. ':' .. value
  end
  return redis.sha1hex(table.concat(framed))
end

local function framed_value(field, value)
  if field == nil or value == nil or value == false then return nil end
  return tostring(#field) .. ':' .. field .. tostring(#value) .. ':' .. value
end

local function record_semantic_digest(values)
  if values == nil or #values < #RECORD_SEMANTIC_FIELDS then return nil end
  local framed = {DIGEST_PREFIX}
  for i = 1, #RECORD_SEMANTIC_FIELDS do
    local item = framed_value(RECORD_SEMANTIC_FIELDS[i], values[i])
    if item == nil then return nil end
    framed[#framed + 1] = item
  end
  return redis.sha1hex(table.concat(framed))
end

local function record_wire_bytes(values)
  if values == nil or #values < #RECORD_VALUE_FIELDS then return nil end
  local bytes = #RECORD_WIRE_PREFIX
  for i = 1, #RECORD_SEMANTIC_FIELDS do
    local field = RECORD_SEMANTIC_FIELDS[i]
    local value = values[i]
    if value == nil or value == false then return nil end
    bytes = bytes + #tostring(#field) + 1 + #field + #tostring(#value) + 1 + #value
  end
  local digest_field = 'record_digest'
  local digest_value = values[23]
  if digest_value == nil or digest_value == false then return nil end
  return bytes + #tostring(#digest_field) + 1 + #digest_field +
    #tostring(#digest_value) + 1 + #digest_value
end

local function read_wire_uint(raw, position)
  if raw == nil or position > #raw then return nil, nil end
  local separator = string.find(raw, ':', position, true)
  if separator == nil or separator == position then return nil, nil end
  local token = string.sub(raw, position, separator - 1)
  local value = canonical_safe_num(token)
  if value == nil then return nil, nil end
  return value, separator + 1
end

local function read_wire_frame(raw, position)
  local length, start_at = read_wire_uint(raw, position)
  if length == nil or start_at + length - 1 > #raw then return nil, nil end
  return string.sub(raw, start_at, start_at + length - 1), start_at + length
end

local function valid_resource_summary(raw)
  if raw == nil or string.sub(raw, 1, #RESOURCE_PREFIX) ~= RESOURCE_PREFIX then return false end
  local count, position = read_wire_uint(raw, #RESOURCE_PREFIX + 1)
  if count == nil or count > #raw then return false end
  local previous = nil
  for i = 1, count do
    local key, after_key = read_wire_frame(raw, position)
    if key == nil or not valid_resource_text(key, MAX_RESOURCE_KEY) or
       (previous ~= nil and key <= previous) then return false end
    local value, after_value = read_wire_frame(raw, after_key)
    if value == nil or not valid_resource_text(value, MAX_RESOURCE_VALUE) then return false end
    previous = key
    position = after_value
  end
  return position == #raw + 1
end

local function valid_tls_dns(value, limit)
  if not valid_text(value, limit) or string.find(value, '*', 1, true) or
     string.find(value, ':', 1, true) or string.find(value, '/', 1, true) or
     string.find(value, '..', 1, true) then return false end
  local start_at = 1
  while true do
    local separator = string.find(value, '.', start_at, true)
    local finish = separator and separator - 1 or #value
    local length = finish - start_at + 1
    if length <= 0 or length > 63 or string.sub(value, start_at, start_at) == '-' or
       string.sub(value, finish, finish) == '-' then return false end
    if separator == nil then break end
    start_at = separator + 1
  end
  return true
end

local function valid_holder_domain(domain)
  return domain == 'PLAYER' or domain == 'FARM' or domain == 'FARM_WORKER'
end

local function valid_deployment_uid(value, limit)
  if value == nil or value == '' or #value > limit then return false end
  for position = 1, #value do
    local byte = string.byte(value, position)
    if byte < 33 or byte > 126 then return false end
  end
  return true
end

local function valid_deployment_identity(pod_uid, workload_uid, ordinal, max_id)
  local zero = pod_uid == '' and workload_uid == '' and ordinal == ''
  if zero then return true end
  local value = safe_uint(ordinal)
  return valid_deployment_uid(pod_uid, max_id) and
    valid_deployment_uid(workload_uid, max_id) and value ~= nil and
    (#value < #MAX_INT32_STRING or
      (#value == #MAX_INT32_STRING and value <= MAX_INT32_STRING))
end

local function valid_record_values(values, max_id, max_endpoint, max_tls, max_record, max_resource,
  max_shards, max_capacity)
  if values == nil or #values ~= #RECORD_FIELDS or values[1] ~= RECORD_SCHEMA then return false end
  local limits = {
    {'schema_version', #RECORD_SCHEMA}, {'domain', 16}, {'holder_id', max_id}, {'boot_id', max_id},
    {'pod_uid', max_id}, {'workload_uid', max_id}, {'ordinal', #MAX_SAFE_INTEGER_STRING},
    {'logic_endpoint', max_endpoint}, {'holder_control_endpoint', max_endpoint},
    {'tls_dns_identity', max_tls}, {'state', 32},
    {'process_ready', 1}, {'draining', 1}, {'serving_shard_count', #MAX_SAFE_INTEGER_STRING},
    {'snapshot_revision', #MAX_SAFE_INTEGER_STRING}, {'started_at_ms', #MAX_SAFE_INTEGER_STRING},
    {'heartbeat_at_ms', #MAX_SAFE_INTEGER_STRING}, {'lease_until_ms', #MAX_SAFE_INTEGER_STRING},
    {'sequence', #MAX_SAFE_INTEGER_STRING}, {'capacity_weight', #MAX_SAFE_INTEGER_STRING},
    {'resource_summary', max_resource}, {'record_bytes', #MAX_SAFE_INTEGER_STRING},
    {'record_digest', 40}, {'record_integrity', 40}
  }
  for i = 1, #limits do
    if values[i] == nil or values[i] == false or #values[i] > limits[i][2] then return false end
  end
  if not valid_holder_domain(values[2]) or
     not valid_text(values[3], max_id) or not valid_text(values[4], max_id) or
     not valid_deployment_identity(values[5], values[6], values[7], max_id) or
     not valid_endpoint(values[8], max_endpoint) or not valid_endpoint(values[9], max_endpoint) or
     not valid_tls_dns(values[10], max_tls) or values[24] ~= record_value_integrity(values) then
    return false
  end
  local serving = canonical_safe_num(values[14])
  local snapshot = canonical_safe_num(values[15])
  local started = canonical_safe_num(values[16])
  local heartbeat = canonical_safe_num(values[17])
  local lease = canonical_safe_num(values[18])
  local sequence = canonical_safe_num(values[19])
  local capacity = canonical_safe_num(values[20])
  local bytes = canonical_safe_num(values[22])
  local expected_digest = record_semantic_digest(values)
  local expected_bytes = record_wire_bytes(values)
  return serving ~= nil and serving <= max_shards and snapshot ~= nil and
    started ~= nil and started > 0 and heartbeat ~= nil and heartbeat > 0 and
    lease ~= nil and lease > 0 and sequence ~= nil and sequence > 0 and
    capacity ~= nil and capacity <= max_capacity and bytes ~= nil and bytes > 0 and
    bytes <= max_record and #values[21] <= max_resource and valid_resource_summary(values[21]) and
    valid_digest(values[23]) and values[23] == expected_digest and
    expected_bytes ~= nil and bytes == expected_bytes
end

local function valid_record(key, max_id, max_endpoint, max_tls, max_record, max_resource,
  max_shards, max_capacity)
  if not required_hash(key, RECORD_FIELDS) then return false end
  local limits = {
    {'schema_version', #RECORD_SCHEMA}, {'domain', 16}, {'holder_id', max_id}, {'boot_id', max_id},
    {'pod_uid', max_id}, {'workload_uid', max_id}, {'ordinal', #MAX_SAFE_INTEGER_STRING},
    {'logic_endpoint', max_endpoint}, {'holder_control_endpoint', max_endpoint},
    {'tls_dns_identity', max_tls}, {'state', 32},
    {'process_ready', 1}, {'draining', 1}, {'serving_shard_count', #MAX_SAFE_INTEGER_STRING},
    {'snapshot_revision', #MAX_SAFE_INTEGER_STRING}, {'started_at_ms', #MAX_SAFE_INTEGER_STRING},
    {'heartbeat_at_ms', #MAX_SAFE_INTEGER_STRING}, {'lease_until_ms', #MAX_SAFE_INTEGER_STRING},
    {'sequence', #MAX_SAFE_INTEGER_STRING}, {'capacity_weight', #MAX_SAFE_INTEGER_STRING},
    {'resource_summary', max_resource}, {'record_bytes', #MAX_SAFE_INTEGER_STRING},
    {'record_digest', 40}, {'record_integrity', 40}
  }
  for i = 1, #limits do
    if not field_len_ok(key, limits[i][1], limits[i][2]) then return false end
  end
  return valid_record_values(record_values(key), max_id, max_endpoint, max_tls,
    max_record, max_resource, max_shards, max_capacity)
end

local function valid_flags(state, process_ready, draining, serving, max_shards)
  if process_ready ~= '0' and process_ready ~= '1' then return false end
  if draining ~= '0' and draining ~= '1' then return false end
  local serving_num = safe_num(serving)
  if serving_num == nil or serving_num > max_shards then return false end
  if state == 'WARMING' then
    return process_ready == '0' and draining == '0' and serving_num == 0
  elseif state == 'READY_ZERO_SHARD' then
    return process_ready == '1' and draining == '0' and serving_num == 0
  elseif state == 'SERVING' then
    return process_ready == '1' and draining == '0' and serving_num > 0
  elseif state == 'DRAINING' then
    return draining == '1'
  elseif state == 'DRAINED' then
    return draining == '1' and process_ready == '0' and serving_num == 0
  end
  return false
end

-- Structural persistence matrix only. The Go HolderRegistry must complete its
-- runtime-backed InitialServingVerifier before invoking WARMING -> SERVING;
-- this backend script cannot grant that exceptional admission by itself.
local function valid_transition(previous, next)
  if previous == next then return true end
  if previous == 'REGISTERING' then return next == 'WARMING' end
  if previous == 'WARMING' then return next == 'READY_ZERO_SHARD' or next == 'SERVING' or next == 'DRAINING' end
  if previous == 'READY_ZERO_SHARD' then return next == 'SERVING' or next == 'DRAINING' end
  -- Dynamic assignment can remove the last shard without ending this Holder
  -- boot. Registry liveness never grants shard Authority, so the process may
  -- become an eligible zero-shard target again.
  if previous == 'SERVING' then return next == 'READY_ZERO_SHARD' or next == 'DRAINING' end
  if previous == 'DRAINING' then return next == 'DRAINED' end
  return false
end

local function redis_now()
  local time = redis.call('TIME')
  local seconds = safe_num(time[1])
  local micros = safe_num(time[2])
  if seconds == nil or micros == nil or micros > 999999 or seconds > math.floor(MAX_SAFE_INTEGER / 1000) then
    return nil
  end
  local millis = seconds * 1000 + math.floor(micros / 1000)
  if millis > MAX_SAFE_INTEGER then return nil end
  return millis
end

local function valid_persisted_times(started, heartbeat, lease, now, max_ttl, skew)
  local started_num = safe_num(started)
  local heartbeat_num = safe_num(heartbeat)
  local lease_num = safe_num(lease)
  if started_num == nil or heartbeat_num == nil or lease_num == nil then return false end
  if heartbeat_num < started_num then return false end
  if now > MAX_SAFE_INTEGER - skew then return false end
  local max_now = now + skew
  if heartbeat_num > max_now or lease_num <= heartbeat_num then return false end
  if max_now > MAX_SAFE_INTEGER - max_ttl or lease_num > max_now + max_ttl then return false end
  return true
end

local function valid_candidate_times(started, heartbeat, lease, now, min_ttl, max_ttl, skew)
  if not valid_persisted_times(started, heartbeat, lease, now, max_ttl, skew) then return false end
  local lease_num = safe_num(lease)
  return lease_num ~= nil and lease_num >= now and lease_num - now >= min_ttl
end

local function valid_complete_record(key, max_id, max_endpoint, max_tls, max_record,
  max_resource, max_shards, max_capacity, now, max_ttl, skew)
  if not valid_record(key, max_id, max_endpoint, max_tls, max_record, max_resource,
    max_shards, max_capacity) then return false end
  local values = record_values(key)
  return valid_flags(values[11], values[12], values[13], values[14], max_shards) and
    valid_persisted_times(values[16], values[17], values[18], now, max_ttl, skew)
end

local function record_reply(status, key, max_id, max_endpoint, max_tls, max_record,
  max_resource, max_shards, max_capacity, now, max_ttl, skew)
  if not valid_complete_record(key, max_id, max_endpoint, max_tls, max_record,
    max_resource, max_shards, max_capacity, now, max_ttl, skew) then return {'MALFORMED'} end
  local values = record_values(key)
  local result = {status}
  for i = 1, #values do result[#result + 1] = values[i] or '' end
  return result
end

local function set_meta(meta, revision, count, bytes, resources)
  redis.call('HSET', meta,
    'schema_version', META_SCHEMA,
    'registry_revision', revision,
    'holder_count', string.format('%.0f', count),
    'record_bytes', string.format('%.0f', bytes),
    'resource_bytes', string.format('%.0f', resources))
end

local function expire_registry_keys(ttl)
	local registry_ttl = ttl + REGISTRY_TTL_MARGIN
	redis.call('PEXPIRE', KEYS[3], registry_ttl)
  redis.call('PEXPIRE', KEYS[2], registry_ttl)
  redis.call('PEXPIRE', KEYS[1], registry_ttl)
end

local function expire_keys(ttl)
	redis.call('PEXPIRE', KEYS[4], ttl)
  expire_registry_keys(ttl)
end

local function valid_ttl_contract(record_ttl, min_ttl, max_ttl, skew)
  if min_ttl > max_ttl or max_ttl > MAX_SAFE_INTEGER - skew - REGISTRY_TTL_MARGIN then
    return false
  end
  return record_ttl > max_ttl + skew and
    record_ttl <= MAX_SAFE_INTEGER - REGISTRY_TTL_MARGIN
end

local function hex_encode(value)
  local result = {}
  for i = 1, #value do
    result[#result + 1] = string.format('%02x', string.byte(value, i))
  end
  return table.concat(result)
end

local function record_key_for_member(record_base, member)
  local separator = string.char(0)
  local position = string.find(member, separator, 1, true)
  if position == nil or position == 1 or position == #member then return nil end
  local domain = string.sub(member, 1, position - 1)
  local holder = string.sub(member, position + 1)
  return record_base .. ':record:' .. hex_encode(domain) .. ':' .. hex_encode(holder), domain, holder
end

local function accounting_value(record_bytes, resource_bytes)
  return string.format('%.0f:%.0f', record_bytes, resource_bytes)
end

local function parse_accounting(value, max_record, max_resource)
  if value == nil or value == false then return nil, nil end
  local separator = string.find(value, ':', 1, true)
  if separator == nil or separator == 1 or separator == #value or
     string.find(value, ':', separator + 1, true) ~= nil then return nil, nil end
  local record_bytes = canonical_safe_num(string.sub(value, 1, separator - 1))
  local resource_bytes = canonical_safe_num(string.sub(value, separator + 1))
  if record_bytes == nil or record_bytes <= 0 or record_bytes > max_record or
     resource_bytes == nil or resource_bytes <= 0 or resource_bytes > max_resource then
    return nil, nil
  end
  return record_bytes, resource_bytes
end

local function reconcile(meta, index, accounting, record_base, now, max_holders, max_record, max_resource,
  max_total_record, max_total_resource, max_id, max_endpoint, max_tls,
  min_ttl, max_ttl, skew, max_shards, max_capacity)
  local indexed = redis.call('ZCARD', index)
  if indexed > max_holders then return false, nil, nil, nil, nil, 'MALFORMED' end
	if redis.call('TYPE', accounting).ok ~= 'hash' or redis.call('HLEN', accounting) ~= indexed then
		return false, nil, nil, nil, nil, 'MALFORMED'
	end
  local meta_count = canonical_safe_num(redis.call('HGET', meta, 'holder_count'))
  local meta_bytes = canonical_safe_num(redis.call('HGET', meta, 'record_bytes'))
  local meta_resources = canonical_safe_num(redis.call('HGET', meta, 'resource_bytes'))
  if meta_count == nil or meta_count ~= indexed or meta_bytes == nil or meta_resources == nil then
    return false, nil, nil, nil, nil, 'MALFORMED'
  end
  local entries = redis.call('ZRANGE', index, 0, max_holders - 1, 'WITHSCORES')
  if #entries ~= indexed * 2 then return false, nil, nil, nil, nil, 'MALFORMED' end
  local checked = {}
  for i = 1, #entries, 2 do
    local member = entries[i]
    local score = entries[i + 1]
    local score_num = safe_num(score)
		local accounted_bytes, accounted_resources = parse_accounting(
			redis.call('HGET', accounting, member), max_record, max_resource)
    local record_key, domain, holder = record_key_for_member(record_base, member)
		if score_num == nil or accounted_bytes == nil or record_key == nil then
			return false, nil, nil, nil, nil, 'MALFORMED'
		end
		if not valid_holder_domain(domain) or not valid_text(holder, max_id) then
			return false, nil, nil, nil, nil, 'MALFORMED'
		end
    local record_kind = redis.call('TYPE', record_key).ok
    if record_kind == 'none' then
      if score_num <= now then
				checked[#checked + 1] = {
					member = member, record_key = false, lease = score,
					record_bytes = string.format('%.0f', accounted_bytes),
					resource_bytes = accounted_resources
				}
      else
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
    elseif record_kind == 'hash' then
      if not valid_complete_record(record_key, max_id, max_endpoint, max_tls,
        max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew) then
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
      local values = record_values(record_key)
      if values[2] ~= domain or values[3] ~= holder or uint_cmp(score, values[18]) ~= 0 then
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
			if canonical_safe_num(values[22]) ~= accounted_bytes or #values[21] ~= accounted_resources then
				return false, nil, nil, nil, nil, 'MALFORMED'
			end
      checked[#checked + 1] = {
	        member = member, record_key = record_key, lease = values[18],
					record_bytes = values[22], resource_summary = values[21], resource_bytes = accounted_resources
      }
    else
      return false, nil, nil, nil, nil, 'MALFORMED'
    end
  end

  -- Only classify stale entries or trust aggregate counters after every
  -- existing record hash has passed the exact integrity and semantic checks.
  local count, bytes, resources = 0, 0, 0
  local complete_bytes, complete_resources = 0, 0
  local stale = {}
  for i = 1, #checked do
    local item = checked[i]
    local item_bytes = canonical_safe_num(item.record_bytes)
    local item_resources = item.resource_bytes
    if item_bytes == nil or item_bytes <= 0 or item_bytes > max_record or
       item_resources == nil or item_resources <= 0 or item_resources > max_resource or
       item_bytes > max_total_record or item_resources > max_total_resource or
       complete_bytes > max_total_record - item_bytes or
       complete_resources > max_total_resource - item_resources or
       complete_bytes > MAX_SAFE_INTEGER - item_bytes or
       complete_resources > MAX_SAFE_INTEGER - item_resources then
      return false, nil, nil, nil, nil, 'MALFORMED'
    end
    complete_bytes = complete_bytes + item_bytes
    complete_resources = complete_resources + item_resources
    if safe_num(item.lease) <= now then
      stale[#stale + 1] = {item.member, item.record_key}
    else
      local record_bytes = canonical_safe_num(item.record_bytes)
      local resource_bytes = item.resource_bytes
      if record_bytes == nil or record_bytes <= 0 or record_bytes > max_record or
         resource_bytes > max_resource or record_bytes > max_total_record or
         resource_bytes > max_total_resource or
         bytes > max_total_record - record_bytes or
         resources > max_total_resource - resource_bytes then
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
      if bytes > MAX_SAFE_INTEGER - record_bytes or resources > MAX_SAFE_INTEGER - resource_bytes then
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
      count = count + 1
      bytes = bytes + record_bytes
      resources = resources + resource_bytes
      if count > max_holders then
        return false, nil, nil, nil, nil, 'MALFORMED'
      end
    end
  end
  if meta_bytes ~= complete_bytes or meta_resources ~= complete_resources then
    return false, nil, nil, nil, nil, 'MALFORMED'
  end
  return true, count, bytes, resources, stale, nil
end

local function remove_stale(index, accounting, stale)
  for i = 1, #stale do
    redis.call('ZREM', index, stale[i][1])
		redis.call('HDEL', accounting, stale[i][1])
    if stale[i][2] ~= false then
      redis.call('DEL', stale[i][2])
    end
  end
end
`

// holderRegisterScript is a fixed, versioned script family independent of
// the RouteSnapshot/transition script registry in scripts.go.
const holderRegisterScript = holderLuaCommon + `
if not same_tag(ARGV[1]) then return {'CROSS_SLOT'} end
local max_holders, max_record, max_resource = safe_num(ARGV[2]), safe_num(ARGV[3]), safe_num(ARGV[4])
local record_ttl, min_ttl, max_ttl, skew = safe_num(ARGV[5]), safe_num(ARGV[6]), safe_num(ARGV[7]), safe_num(ARGV[8])
local max_shards, max_capacity = safe_num(ARGV[9]), safe_num(ARGV[10])
local max_id, max_endpoint, max_tls = safe_num(ARGV[12]), safe_num(ARGV[13]), safe_num(ARGV[14])
local max_total_record, max_total_resource = safe_num(ARGV[37]), safe_num(ARGV[38])
if max_holders == nil or max_holders <= 0 or max_record == nil or max_record <= 0 or
   max_resource == nil or max_resource <= 0 or record_ttl == nil or record_ttl <= 0 or
   min_ttl == nil or min_ttl <= 0 or max_ttl == nil or max_ttl <= 0 or skew == nil or
   max_shards == nil or max_capacity == nil or max_id == nil or max_endpoint == nil or
   max_tls == nil or max_total_record == nil or max_total_record <= 0 or
   max_total_resource == nil or max_total_resource <= 0 or
   not valid_ttl_contract(record_ttl, min_ttl, max_ttl, skew) then
  return {'BAD_NUMBER'}
end
if ARGV[11] ~= RECORD_SCHEMA then return {'MALFORMED'} end
local now = redis_now()
if now == nil then return {'BAD_NUMBER'} end
local candidate_values = {
  RECORD_SCHEMA, ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20],
  ARGV[21], ARGV[22], ARGV[23], ARGV[24], ARGV[25], ARGV[26], ARGV[27],
  ARGV[28], ARGV[29], ARGV[30], ARGV[31], ARGV[32], ARGV[33], ARGV[34],
  ARGV[36], ARGV[35]
}
candidate_values[24] = record_value_integrity(candidate_values)
if not valid_record_values(candidate_values, max_id, max_endpoint, max_tls, max_record,
  max_resource, max_shards, max_capacity) then return {'MALFORMED'} end
if ARGV[24] ~= 'WARMING' or not valid_flags(ARGV[24], ARGV[25], ARGV[26], ARGV[27], max_shards) then
  return {'BAD_STATE'}
end
if not valid_candidate_times(ARGV[29], ARGV[30], ARGV[31], now, min_ttl, max_ttl, skew) then
  return {'BAD_TIME'}
end
if #ARGV[34] > max_resource or safe_num(ARGV[36]) > max_record then
  return {'CAPACITY_EXCEEDED'}
end

local meta_kind = redis.call('TYPE', KEYS[1]).ok
local index_kind = redis.call('TYPE', KEYS[2]).ok
local accounting_kind = redis.call('TYPE', KEYS[3]).ok
local record_kind = redis.call('TYPE', KEYS[4]).ok
if not valid_meta(KEYS[1], max_holders, max_total_record, max_total_resource) then return {'MALFORMED'} end
if index_kind ~= 'none' and index_kind ~= 'zset' then return {'MALFORMED'} end
if accounting_kind ~= 'none' and accounting_kind ~= 'hash' then return {'MALFORMED'} end
local record_base = string.match(KEYS[4], '^(.*):record:')
if record_base == nil then return {'MALFORMED'} end
local count, bytes, resources, stale = 0, 0, 0, {}
local revision = '0'
if meta_kind == 'none' and index_kind == 'none' and accounting_kind == 'none' and record_kind == 'none' then
  count, bytes, resources = 0, 0, 0
elseif meta_kind == 'hash' and index_kind == 'zset' and accounting_kind == 'hash' then
  revision = redis.call('HGET', KEYS[1], 'registry_revision')
  local ok, reconciled_count, reconciled_bytes, reconciled_resources, stale_members, status =
    reconcile(KEYS[1], KEYS[2], KEYS[3], record_base, now, max_holders, max_record, max_resource,
      max_total_record, max_total_resource, max_id, max_endpoint, max_tls,
      min_ttl, max_ttl, skew, max_shards, max_capacity)
  if not ok then return {status or 'MALFORMED'} end
  count, bytes, resources, stale = reconciled_count, reconciled_bytes, reconciled_resources, stale_members
else
  return {'MALFORMED'}
end

local member = ARGV[15] .. string.char(0) .. ARGV[16]
local existing = redis.call('EXISTS', KEYS[4]) == 1
if existing then
  local member_score = redis.call('ZSCORE', KEYS[2], member)
  if member_score == false then return {'MALFORMED'} end
  if safe_num(member_score) > now then
    if not valid_complete_record(KEYS[4], max_id, max_endpoint, max_tls,
      max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew) then
      return {'MALFORMED'}
    end
    local current = record_values(KEYS[4])
    if current[4] == ARGV[17] then
      if current[1] == RECORD_SCHEMA and current[2] == ARGV[15] and current[3] == ARGV[16] and
         current[5] == ARGV[18] and current[6] == ARGV[19] and current[7] == ARGV[20] and
         current[8] == ARGV[21] and current[9] == ARGV[22] and current[10] == ARGV[23] and
         current[11] == ARGV[24] and current[12] == ARGV[25] and current[13] == ARGV[26] and
         current[14] == ARGV[27] and current[15] == ARGV[28] and current[16] == ARGV[29] and
         current[17] == ARGV[30] and current[18] == ARGV[31] and current[19] == ARGV[32] and
         current[20] == ARGV[33] and current[21] == ARGV[34] and current[22] == ARGV[36] and
         current[23] == ARGV[35] then
        if #stale > 0 then
          local next_revision = increment_revision(revision)
          if next_revision == nil then return {'REVISION_EXHAUSTED'} end
          remove_stale(KEYS[2], KEYS[3], stale)
          set_meta(KEYS[1], next_revision, count, bytes, resources)
          expire_registry_keys(record_ttl)
        end
        return record_reply('ALREADY', KEYS[4], max_id, max_endpoint, max_tls,
          max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew)
      end
      return {'CONFLICT'}
    end
    return {'FRESH_BOOT_CONFLICT'}
  end
  existing = false
end
if count >= max_holders then return {'CAPACITY_EXCEEDED'} end
if bytes > max_total_record - safe_num(ARGV[36]) or
   resources > max_total_resource - #ARGV[34] then
  return {'CAPACITY_EXCEEDED'}
end
local next_revision = increment_revision(revision)
if next_revision == nil then return {'REVISION_EXHAUSTED'} end
local next_values = candidate_values
local next_integrity = candidate_values[24]
remove_stale(KEYS[2], KEYS[3], stale)
redis.call('HSET', KEYS[4],
  'schema_version', RECORD_SCHEMA, 'domain', ARGV[15], 'holder_id', ARGV[16],
  'boot_id', ARGV[17], 'pod_uid', ARGV[18], 'workload_uid', ARGV[19], 'ordinal', ARGV[20],
  'logic_endpoint', ARGV[21], 'holder_control_endpoint', ARGV[22],
  'tls_dns_identity', ARGV[23], 'state', ARGV[24], 'process_ready', ARGV[25],
  'draining', ARGV[26], 'serving_shard_count', ARGV[27], 'snapshot_revision', ARGV[28],
  'started_at_ms', ARGV[29], 'heartbeat_at_ms', ARGV[30], 'lease_until_ms', ARGV[31],
  'sequence', ARGV[32], 'capacity_weight', ARGV[33], 'resource_summary', ARGV[34],
  'record_bytes', ARGV[36], 'record_digest', ARGV[35], 'record_integrity', next_integrity)
redis.call('ZADD', KEYS[2], ARGV[31], member)
redis.call('HSET', KEYS[3], member, accounting_value(safe_num(ARGV[36]), #ARGV[34]))
set_meta(KEYS[1], next_revision, count + 1, bytes + safe_num(ARGV[36]), resources + #ARGV[34])
expire_keys(record_ttl)
return record_reply('COMMITTED', KEYS[4], max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew)
`

const holderHeartbeatScript = holderLuaCommon + `
if not same_tag(ARGV[1]) then return {'CROSS_SLOT'} end
local max_holders, max_record, max_resource = safe_num(ARGV[2]), safe_num(ARGV[3]), safe_num(ARGV[4])
local record_ttl, min_ttl, max_ttl, skew = safe_num(ARGV[5]), safe_num(ARGV[6]), safe_num(ARGV[7]), safe_num(ARGV[8])
local max_shards, max_capacity = safe_num(ARGV[9]), safe_num(ARGV[10])
local max_id, max_endpoint, max_tls = safe_num(ARGV[12]), safe_num(ARGV[13]), safe_num(ARGV[14])
local max_total_record, max_total_resource = safe_num(ARGV[30]), safe_num(ARGV[31])
if max_holders == nil or max_holders <= 0 or max_record == nil or max_record <= 0 or
   max_resource == nil or max_resource <= 0 or record_ttl == nil or record_ttl <= 0 or
   min_ttl == nil or min_ttl <= 0 or max_ttl == nil or max_ttl <= 0 or skew == nil or
   max_shards == nil or max_capacity == nil or max_id == nil or max_endpoint == nil or
   max_tls == nil or max_total_record == nil or max_total_record <= 0 or
   max_total_resource == nil or max_total_resource <= 0 or
   not valid_ttl_contract(record_ttl, min_ttl, max_ttl, skew) then
  return {'BAD_NUMBER'}
end
if ARGV[11] ~= RECORD_SCHEMA then return {'MALFORMED'} end
local now = redis_now()
if now == nil then return {'BAD_NUMBER'} end
if not valid_holder_domain(ARGV[15]) or
   not valid_text(ARGV[16], max_id) or not valid_text(ARGV[17], max_id) or
   not valid_flags(ARGV[18], ARGV[19], ARGV[20], ARGV[21], max_shards) then
  return {'MALFORMED'}
end
if canonical_safe_num(ARGV[21]) == nil or canonical_safe_num(ARGV[22]) == nil or
   canonical_safe_num(ARGV[23]) == nil or canonical_safe_num(ARGV[24]) == nil or
   canonical_safe_num(ARGV[25]) == nil or canonical_safe_num(ARGV[26]) == nil or
   canonical_safe_num(ARGV[29]) == nil then return {'BAD_NUMBER'} end
if #ARGV[27] > max_resource or not valid_resource_summary(ARGV[27]) or
   not valid_digest(ARGV[28]) or canonical_safe_num(ARGV[29]) <= 0 then
  return {'MALFORMED'}
end
if canonical_safe_num(ARGV[29]) > max_record then return {'CAPACITY_EXCEEDED'} end
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' or redis.call('TYPE', KEYS[2]).ok ~= 'zset' or
   redis.call('TYPE', KEYS[3]).ok ~= 'hash' or
   not valid_meta(KEYS[1], max_holders, max_total_record, max_total_resource) or
   not valid_complete_record(KEYS[4], max_id, max_endpoint, max_tls,
     max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew) then
  return {'MALFORMED'}
end
local member = ARGV[15] .. string.char(0) .. ARGV[16]
local score = redis.call('ZSCORE', KEYS[2], member)
if score == false then return {'MALFORMED'} end
local current = redis.call('HMGET', KEYS[4],
  'domain', 'holder_id', 'boot_id', 'state', 'process_ready', 'draining',
  'serving_shard_count', 'snapshot_revision', 'started_at_ms',
  'heartbeat_at_ms', 'lease_until_ms', 'sequence', 'capacity_weight',
  'resource_summary', 'record_bytes', 'record_digest')
if current[1] ~= ARGV[15] or current[2] ~= ARGV[16] then return {'IDENTITY_MISMATCH'} end
if current[3] ~= ARGV[17] then return {'BOOT_MISMATCH'} end
if current[4] == 'DRAINED' then return {'BAD_STATE'} end
if uint_cmp(score, current[11]) ~= 0 then return {'MALFORMED'} end
if safe_num(current[11]) <= now then return {'LEASE_EXPIRED'} end
if not valid_flags(current[4], current[5], current[6], current[7], max_shards) then return {'MALFORMED'} end
if not valid_persisted_times(current[9], current[10], current[11], now, max_ttl, skew) then return {'MALFORMED'} end
if not valid_transition(current[4], ARGV[18]) or
   not valid_flags(ARGV[18], ARGV[19], ARGV[20], ARGV[21], max_shards) then
  return {'BAD_STATE'}
end
if current[4] == 'SERVING' and ARGV[18] == 'READY_ZERO_SHARD' and
   not uint_greater(ARGV[22], current[8]) then
  return {'BAD_STATE'}
end
if not valid_candidate_times(current[9], ARGV[23], ARGV[24], now, min_ttl, max_ttl, skew) then return {'BAD_TIME'} end
if safe_num(ARGV[21]) > max_shards or safe_num(ARGV[26]) > max_capacity then
  return {'CAPACITY_EXCEEDED'}
end
if not uint_at_least(ARGV[22], current[8]) then return {'STALE'} end
if not uint_at_least(ARGV[23], current[10]) or not uint_at_least(ARGV[24], current[11]) then
  return {'STALE'}
end

local next_values = record_values(KEYS[4])
next_values[11], next_values[12], next_values[13] = ARGV[18], ARGV[19], ARGV[20]
next_values[14], next_values[15] = ARGV[21], ARGV[22]
next_values[17], next_values[18], next_values[19] = ARGV[23], ARGV[24], ARGV[25]
next_values[20], next_values[21] = ARGV[26], ARGV[27]
next_values[22], next_values[23] = ARGV[29], ARGV[28]
next_values[24] = record_value_integrity(next_values)
if not valid_record_values(next_values, max_id, max_endpoint, max_tls, max_record,
  max_resource, max_shards, max_capacity) then return {'MALFORMED'} end

local record_base = string.match(KEYS[4], '^(.*):record:')
if record_base == nil then return {'MALFORMED'} end
local ok, count, bytes, resources, stale, status =
  reconcile(KEYS[1], KEYS[2], KEYS[3], record_base, now, max_holders, max_record, max_resource,
    max_total_record, max_total_resource, max_id, max_endpoint, max_tls,
    min_ttl, max_ttl, skew, max_shards, max_capacity)
if not ok then return {status or 'MALFORMED'} end
if redis.call('ZSCORE', KEYS[2], member) == false then return {'MALFORMED'} end
if not uint_greater(ARGV[25], current[12]) then
  if uint_cmp(ARGV[25], current[12]) == 0 and
     current[1] == ARGV[15] and current[2] == ARGV[16] and current[4] == ARGV[18] and
     current[5] == ARGV[19] and current[6] == ARGV[20] and current[7] == ARGV[21] and
     current[8] == ARGV[22] and current[10] == ARGV[23] and current[11] == ARGV[24] and
     current[12] == ARGV[25] and current[13] == ARGV[26] and
     current[14] == ARGV[27] and current[15] == ARGV[29] and current[16] == ARGV[28] then
    return record_reply('REPLAY', KEYS[4], max_id, max_endpoint, max_tls,
      max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew)
  end
  return {'STALE'}
end
local old_bytes = safe_num(current[15])
local old_resource = #current[14]
local new_bytes = safe_num(ARGV[29])
if old_bytes == nil or new_bytes == nil or bytes < old_bytes or resources < old_resource then return {'MALFORMED'} end
if bytes - old_bytes > max_total_record - new_bytes or
   resources - old_resource > max_total_resource - #ARGV[27] then
  return {'CAPACITY_EXCEEDED'}
end
local revision = redis.call('HGET', KEYS[1], 'registry_revision')
local next_revision = increment_revision(revision)
if next_revision == nil then return {'REVISION_EXHAUSTED'} end
local next_integrity = next_values[24]
remove_stale(KEYS[2], KEYS[3], stale)
redis.call('HSET', KEYS[4],
  'state', ARGV[18], 'process_ready', ARGV[19], 'draining', ARGV[20],
  'serving_shard_count', ARGV[21], 'snapshot_revision', ARGV[22],
  'heartbeat_at_ms', ARGV[23], 'lease_until_ms', ARGV[24],
  'sequence', ARGV[25], 'capacity_weight', ARGV[26],
  'resource_summary', ARGV[27], 'record_bytes', ARGV[29], 'record_digest', ARGV[28],
  'record_integrity', next_integrity)
redis.call('ZADD', KEYS[2], ARGV[24], member)
redis.call('HSET', KEYS[3], member, accounting_value(new_bytes, #ARGV[27]))
set_meta(KEYS[1], next_revision, count, bytes - old_bytes + new_bytes, resources - old_resource + #ARGV[27])
expire_keys(record_ttl)
return record_reply('COMMITTED', KEYS[4], max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew)
`

const placementExclusionLuaCommon = holderLuaCommon + `
local PLACEMENT_SCHEMA = 'placement-exclusion-v1'
local PLACEMENT_FIELDS = {
  'schema_version', 'domain', 'holder_id', 'boot_id',
  'excluded', 'revision', 'owner_plan_id', 'record_integrity'
}

local function frame(value)
  return tostring(#value) .. ':' .. value
end

local function integrity(domain, holder, boot, excluded, revision, owner)
  return redis.sha1hex('PE1:' .. frame(domain) .. frame(holder) .. frame(boot) ..
    frame(excluded) .. frame(revision) .. frame(owner))
end

local function placement_reply(status, domain, holder, boot, excluded, revision, owner)
  return {status, domain, holder, boot, excluded, revision, owner}
end

local function current_holder(domain, holder, boot, max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, max_ttl, skew)
  if redis.call('TYPE', KEYS[1]).ok == 'none' then return 'NOT_FOUND' end
  local now = redis_now()
  if now == nil or not valid_complete_record(KEYS[1], max_id, max_endpoint, max_tls,
    max_record, max_resource, max_shards, max_capacity, now, max_ttl, skew) then return 'MALFORMED' end
  local value = redis.call('HMGET', KEYS[1], 'domain', 'holder_id', 'boot_id')
  if value[1] ~= domain or value[2] ~= holder then return 'MALFORMED' end
  if value[3] ~= boot then return 'BOOT_MISMATCH' end
  return 'OK'
end

local function load_placement(domain, holder, boot, max_id)
  local kind = redis.call('TYPE', KEYS[2]).ok
  if kind == 'none' then return '0', '1', '' end
  if kind ~= 'hash' or redis.call('HLEN', KEYS[2]) ~= #PLACEMENT_FIELDS then return nil end
  local values = redis.call('HMGET', KEYS[2], unpack(PLACEMENT_FIELDS))
  if values[1] ~= PLACEMENT_SCHEMA or values[2] ~= domain or values[3] ~= holder or
     not valid_text(values[4], max_id) or (values[5] ~= '0' and values[5] ~= '1') or
     safe_num(values[6]) == nil or values[6] == '0' or
     ((values[5] == '1') ~= (values[7] ~= '')) or
     (values[7] ~= '' and not valid_text(values[7], max_id)) or
     values[8] ~= integrity(values[2], values[3], values[4], values[5], values[6], values[7]) then
    return nil
  end
  if values[4] == boot then return values[5], values[6], values[7] end
  local revision = safe_num(values[6])
  if revision == nil or revision >= MAX_SAFE_INTEGER then return 'EXHAUSTED' end
  return '0', tostring(revision + 1), ''
end
`

const getPlacementExclusionScript = placementExclusionLuaCommon + `
if not same_tag(ARGV[1]) then return {'CROSS_SLOT'} end
local max_id, max_endpoint, max_tls = safe_num(ARGV[5]), safe_num(ARGV[6]), safe_num(ARGV[7])
local max_record, max_resource = safe_num(ARGV[8]), safe_num(ARGV[9])
local max_shards, max_capacity = safe_num(ARGV[10]), safe_num(ARGV[11])
local max_ttl, skew = safe_num(ARGV[12]), safe_num(ARGV[13])
if max_id == nil or max_endpoint == nil or max_tls == nil or max_record == nil or max_resource == nil or
   max_shards == nil or max_capacity == nil or max_ttl == nil or skew == nil or
   not valid_text(ARGV[2], 16) or not valid_text(ARGV[3], max_id) or not valid_text(ARGV[4], max_id) then
  return {'MALFORMED'}
end
local holder_status = current_holder(ARGV[2], ARGV[3], ARGV[4], max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, max_ttl, skew)
if holder_status ~= 'OK' then return {holder_status} end
local excluded, revision, owner = load_placement(ARGV[2], ARGV[3], ARGV[4], max_id)
if excluded == nil then return {'MALFORMED'} end
if excluded == 'EXHAUSTED' then return {'REVISION_EXHAUSTED'} end
return placement_reply('FOUND', ARGV[2], ARGV[3], ARGV[4], excluded, revision, owner)
`

const excludePlacementScript = placementExclusionLuaCommon + `
if not same_tag(ARGV[1]) then return {'CROSS_SLOT'} end
local max_id, max_endpoint, max_tls = safe_num(ARGV[8]), safe_num(ARGV[9]), safe_num(ARGV[10])
local max_record, max_resource = safe_num(ARGV[11]), safe_num(ARGV[12])
local max_shards, max_capacity = safe_num(ARGV[13]), safe_num(ARGV[14])
local max_ttl, skew = safe_num(ARGV[15]), safe_num(ARGV[16])
local expected_revision = safe_num(ARGV[5])
if max_id == nil or max_endpoint == nil or max_tls == nil or max_record == nil or max_resource == nil or
   max_shards == nil or max_capacity == nil or max_ttl == nil or skew == nil or
   expected_revision == nil or expected_revision == 0 or ARGV[6] ~= '0' or
   not valid_text(ARGV[2], 16) or not valid_text(ARGV[3], max_id) or
   not valid_text(ARGV[4], max_id) or not valid_text(ARGV[7], max_id) then
  return {'MALFORMED'}
end
local holder_status = current_holder(ARGV[2], ARGV[3], ARGV[4], max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, max_ttl, skew)
if holder_status ~= 'OK' then return {holder_status} end
local excluded, revision, owner = load_placement(ARGV[2], ARGV[3], ARGV[4], max_id)
if excluded == nil then return {'MALFORMED'} end
if excluded == 'EXHAUSTED' then return {'REVISION_EXHAUSTED'} end
if excluded == '1' then
  local revision_number = safe_num(revision)
  if owner == ARGV[7] and revision_number ~= nil and revision_number == expected_revision + 1 then
    return placement_reply('REPLAY', ARGV[2], ARGV[3], ARGV[4], excluded, revision, owner)
  end
  return {'OWNER_CONFLICT'}
end
if revision ~= ARGV[5] then return {'CONFLICT'} end
local revision_number = safe_num(revision)
if revision_number == nil or revision_number >= MAX_SAFE_INTEGER then return {'REVISION_EXHAUSTED'} end
local next_revision = tostring(revision_number + 1)
local next_integrity = integrity(ARGV[2], ARGV[3], ARGV[4], '1', next_revision, ARGV[7])
redis.call('HSET', KEYS[2],
  'schema_version', PLACEMENT_SCHEMA, 'domain', ARGV[2], 'holder_id', ARGV[3], 'boot_id', ARGV[4],
  'excluded', '1', 'revision', next_revision, 'owner_plan_id', ARGV[7], 'record_integrity', next_integrity)
return placement_reply('COMMITTED', ARGV[2], ARGV[3], ARGV[4], '1', next_revision, ARGV[7])
`

const releasePlacementExclusionScript = placementExclusionLuaCommon + `
if not same_tag(ARGV[1]) then return {'CROSS_SLOT'} end
local max_id, max_endpoint, max_tls = safe_num(ARGV[7]), safe_num(ARGV[8]), safe_num(ARGV[9])
local max_record, max_resource = safe_num(ARGV[10]), safe_num(ARGV[11])
local max_shards, max_capacity = safe_num(ARGV[12]), safe_num(ARGV[13])
local max_ttl, skew = safe_num(ARGV[14]), safe_num(ARGV[15])
local expected_revision = safe_num(ARGV[6])
if max_id == nil or max_endpoint == nil or max_tls == nil or max_record == nil or max_resource == nil or
   max_shards == nil or max_capacity == nil or max_ttl == nil or skew == nil or
   expected_revision == nil or expected_revision == 0 or
   not valid_text(ARGV[2], 16) or not valid_text(ARGV[3], max_id) or
   not valid_text(ARGV[4], max_id) or not valid_text(ARGV[5], max_id) then
  return {'MALFORMED'}
end
local holder_status = current_holder(ARGV[2], ARGV[3], ARGV[4], max_id, max_endpoint, max_tls,
  max_record, max_resource, max_shards, max_capacity, max_ttl, skew)
if holder_status ~= 'OK' then return {holder_status} end
local excluded, revision, owner = load_placement(ARGV[2], ARGV[3], ARGV[4], max_id)
if excluded == nil then return {'MALFORMED'} end
if excluded == 'EXHAUSTED' then return {'REVISION_EXHAUSTED'} end
if excluded == '0' then
  return placement_reply('ALREADY_RELEASED', ARGV[2], ARGV[3], ARGV[4], excluded, revision, owner)
end
if owner ~= ARGV[5] then
  return placement_reply('OWNER_CONFLICT', ARGV[2], ARGV[3], ARGV[4], excluded, revision, owner)
end
if revision ~= ARGV[6] then
  return placement_reply('CONFLICT', ARGV[2], ARGV[3], ARGV[4], excluded, revision, owner)
end
local revision_number = safe_num(revision)
if revision_number == nil or revision_number >= MAX_SAFE_INTEGER then return {'REVISION_EXHAUSTED'} end
local next_revision = tostring(revision_number + 1)
local next_integrity = integrity(ARGV[2], ARGV[3], ARGV[4], '0', next_revision, '')
redis.call('HSET', KEYS[2],
  'schema_version', PLACEMENT_SCHEMA, 'domain', ARGV[2], 'holder_id', ARGV[3], 'boot_id', ARGV[4],
  'excluded', '0', 'revision', next_revision, 'owner_plan_id', '', 'record_integrity', next_integrity)
return placement_reply('COMMITTED', ARGV[2], ARGV[3], ARGV[4], '0', next_revision, '')
`

type holderScriptDefinition struct {
	name   string
	source string
	sha    string
}

type holderScriptRegistry struct {
	client  redis.UniversalClient
	scripts map[string]holderScriptDefinition
}

func newHolderScriptRegistry(client redis.UniversalClient) (*holderScriptRegistry, error) {
	if err := validateHolderRedisClient(client); err != nil {
		return nil, err
	}
	definitions := []holderScriptDefinition{
		{name: ScriptRegisterHolder, source: holderRegisterScript},
		{name: ScriptHeartbeatHolder, source: holderHeartbeatScript},
		{name: ScriptGetPlacementExclusion, source: getPlacementExclusionScript},
		{name: ScriptExcludePlacement, source: excludePlacementScript},
		{name: ScriptReleasePlacementExclusion, source: releasePlacementExclusionScript},
	}
	registry := &holderScriptRegistry{client: client, scripts: make(map[string]holderScriptDefinition, len(definitions))}
	for _, definition := range definitions {
		sum := sha1.Sum([]byte(definition.source))
		definition.sha = fmt.Sprintf("%x", sum[:])
		if _, exists := registry.scripts[definition.name]; exists {
			return nil, fmt.Errorf("%w: duplicate holder script %q", ErrScriptContract, definition.name)
		}
		registry.scripts[definition.name] = definition
	}
	return registry, nil
}

func (r *holderScriptRegistry) scriptSHA(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	definition, ok := r.scripts[name]
	return definition.sha, ok
}

func (r *holderScriptRegistry) ensureLoaded(ctx context.Context) error {
	if r == nil {
		return ErrScriptContract
	}
	for _, definition := range r.scripts {
		if err := r.load(ctx, definition); err != nil {
			return fmt.Errorf("%w: load %s: %v", ErrScriptContract, definition.name, err)
		}
	}
	return nil
}

func (r *holderScriptRegistry) load(ctx context.Context, definition holderScriptDefinition) error {
	if cluster, ok := r.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(nodeCtx context.Context, node *redis.Client) error {
			sum, err := node.ScriptLoad(nodeCtx, definition.source).Result()
			if err != nil {
				return err
			}
			if sum != definition.sha {
				return fmt.Errorf("script sha mismatch got=%s want=%s", sum, definition.sha)
			}
			return nil
		})
	}
	sum, err := r.client.ScriptLoad(ctx, definition.source).Result()
	if err != nil {
		return err
	}
	if sum != definition.sha {
		return fmt.Errorf("script sha mismatch got=%s want=%s", sum, definition.sha)
	}
	return nil
}

func (r *holderScriptRegistry) run(ctx context.Context, name string, keys []string, args ...any) ([]string, error) {
	if r == nil {
		return nil, ErrScriptContract
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: holder script keys required", ErrScriptContract)
	}
	redisKeys := make([]rediskey.RedisKey, len(keys))
	for index, key := range keys {
		redisKeys[index] = rediskey.RedisKey(key)
	}
	if err := rediskey.AssertSameSlot(redisKeys...); err != nil {
		return nil, fmt.Errorf("%w: holder script keys: %v", ErrScriptContract, err)
	}
	definition, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown holder script %q", ErrScriptContract, name)
	}
	for attempt := 0; attempt < 2; attempt++ {
		reply, err := r.client.EvalSha(ctx, definition.sha, keys, args...).Result()
		if err == nil {
			return holderScriptStrings(reply)
		}
		if !strings.Contains(err.Error(), "NOSCRIPT") {
			return nil, err
		}
		if err := r.load(ctx, definition); err != nil {
			return nil, fmt.Errorf("%w: NOSCRIPT reload %s: %v", ErrScriptContract, definition.name, err)
		}
	}
	return nil, fmt.Errorf("%w: NOSCRIPT retry exhausted", ErrScriptContract)
}

func holderScriptStrings(reply any) ([]string, error) {
	array, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: reply type %T", ErrScriptContract, reply)
	}
	if len(array) > 1+len(holderRecordFields) {
		return nil, fmt.Errorf("%w: reply field count=%d", ErrScriptContract, len(array))
	}
	out := make([]string, 0, len(array))
	for _, item := range array {
		switch value := item.(type) {
		case string:
			out = append(out, value)
		case []byte:
			out = append(out, string(value))
		case int64:
			out = append(out, fmt.Sprintf("%d", value))
		case nil:
			out = append(out, "")
		default:
			return nil, fmt.Errorf("%w: reply element %T", ErrScriptContract, item)
		}
	}
	return out, nil
}
