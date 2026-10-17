// Package authority implements the contract-30/31 ActorAuthorityStore and
// ShardWriteAuthorityStore on top of company Redis, using the frozen auth-v1
// KeyBuilder (internal/storage/redis/rediskey) and a fixed, versioned Lua
// script family. It never constructs keys by string concatenation and never
// runs cross-slot scripts.
//
// Script return protocol: every script returns an array whose first element
// is a string status token. Transport-level UNAVAILABLE_NOT_SENT and
// TIMEOUT_UNKNOWN are never produced by Lua; they are classified in Go from
// the transport error, because a script that executed successfully always
// yields a definitive reply.
package authority

import (
	"crypto/sha1" //nolint:gosec // SHA-1 is the Redis script cache identity, not a security primitive.
	"strings"
)

// Script names. Sources are immutable once registered; any change requires a
// new script version name, because golden tests pin the exact SHA-1.
const (
	ScriptClaimShardAuthority      = "ClaimShardAuthorityV2"
	ScriptRenewShardAuthority      = "RenewShardAuthorityV2"
	ScriptTransferShardAuthority   = "TransferShardAuthorityV2"
	ScriptTakeoverExpiredAuthority = "TakeoverExpiredShardAuthorityV1"
	ScriptUnassignShardAuthority   = "UnassignShardAuthorityV2"
	ScriptLoadAndClaimActorV5      = "LoadAndClaimActorV5"
	ScriptLoadAndClaimActor        = "LoadAndClaimActorV6"
	ScriptCommitActorWithOrdersV12 = "CommitActorMutationWithOrdersV12"
	ScriptCommitActorWithOrdersV13 = "CommitActorMutationWithOrdersV13"
	ScriptCommitActorWithOrdersV15 = "CommitActorMutationWithOrdersV15"
	// Ordinary Commit and CommitAuditedClaim run V15, which skips the
	// current-model HGET audit and unchanged-field candidate reads on audited
	// claims (proven by LoadAndClaim plus RecordVersion CAS). V13 stays
	// registered for rollback. V14 is the explicit exact-output capability.
	ScriptCommitActorWithOrders      = ScriptCommitActorWithOrdersV15
	ScriptCommitActorWithOrdersExact = "CommitActorMutationWithOrdersV14"
	ScriptFenceAndClaimActor         = "FenceAndClaimActorV1"
	ScriptClaimWorkV1                = "ClaimWorkV1"
	ScriptClaimWorkV2                = "ClaimWorkV2"
	ScriptClaimWork                  = "ClaimWorkV4" // frozen compatibility name
	ScriptClaimWorkV5                = "ClaimWorkV5"
	ScriptRenewWorkLease             = "RenewWorkLeaseV2"
	ScriptCompleteWorkV1             = "CompleteWorkV1"
	ScriptCompleteWork               = "CompleteWorkV3"
	ScriptCompleteWorkV4             = "CompleteWorkV4"
	ScriptCompleteWorkV5             = "CompleteWorkV5"
	ScriptClaimAndCompleteWork       = "ClaimAndCompleteWorkV1"
	ScriptClaimAndCompleteWorkV2     = "ClaimAndCompleteWorkV2"
	ScriptClaimAndCompleteWorkV3     = "ClaimAndCompleteWorkV3"
	ScriptVerifySourceOrderV1        = "VerifySourceOrderV1"
	ScriptVerifySourceOrderV2        = "VerifySourceOrderV2"
	ScriptClaimAndCompleteWorkV4     = "ClaimAndCompleteWorkV4"
	ScriptRescheduleWork             = "RescheduleWorkV1"
	ScriptRescheduleWorkV2           = "RescheduleWorkV2"
	ScriptClaimBucketScanLease       = "ClaimBucketScanLeaseV1"
	ScriptRenewBucketScanLease       = "RenewBucketScanLeaseV1"
	ScriptPublishResultEntry         = "PublishResultEntryV1"
	ScriptClaimScheduledWork         = "ClaimScheduledWorkV1"
	ScriptAdvanceScheduledWork       = "AdvanceScheduledWorkV1"
	ScriptRenewScheduledWork         = "RenewScheduledWorkV1"
	ScriptCompleteScheduledWork      = "CompleteScheduledWorkV1"
	ScriptClaimScheduledWorkDual     = "ClaimScheduledWorkDualV1"
	ScriptAdvanceScheduledWorkDual   = "AdvanceScheduledWorkDualV1"
	ScriptCompleteScheduledWorkDual  = "CompleteScheduledWorkDualV1"
	ScriptBackfillScheduleDueIndex   = "BackfillScheduleDueIndexV1"
	ScriptReconcilePendingWork       = "ReconcileBucketPendingWorkV1"
)

// luaCommon holds the LP helpers shared by scripts that parse or build
// length-prefixed blobs. Lua has no cross-script modules, so each script that
// needs it embeds this chunk verbatim; script golden tests freeze the bytes.
const luaCommonLP = `
local function lp1(s)
  local n = #s
  local b1 = math.floor(n / 16777216) % 256
  local b2 = math.floor(n / 65536) % 256
  local b3 = math.floor(n / 256) % 256
  local b4 = n % 256
  return string.char(b1, b2, b3, b4) .. s
end
local function lp_fields(blob)
  local out = {}
  local i = 1
  while i <= #blob do
    if i + 3 > #blob then return nil end
    local b1, b2, b3, b4 = string.byte(blob, i, i + 3)
    local len = b1 * 16777216 + b2 * 65536 + b3 * 256 + b4
    i = i + 4
    if i + len - 1 > #blob then return nil end
    out[#out + 1] = string.sub(blob, i, i + len - 1)
    i = i + len
  end
  return out
end
local function fmt_ms(v)
  return string.format('%.0f', v)
end
`

const luaCheckTag = `
local tag = ARGV[1]
for i = 1, #KEYS do
  if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then
    return {'CROSS_SLOT'}
  end
end
`

const luaRedisTime = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
`

const luaClaimShardAuthoritySrc = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ttl = tonumber(ARGV[7])
local min_ttl = tonumber(ARGV[8])
local max_ttl = tonumber(ARGV[9])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end

local function snapshot(status, remaining)
  local v = redis.call('HMGET', KEYS[1],
    'holder_instance_id', 'holder_boot_id', 'writer_epoch', 'lease_epoch',
    'assignment_revision', 'lease_state', 'lease_expire_at_ms',
    'last_transition_id', 'last_transition_at_ms')
  return {status, v[1] or '', v[2] or '', v[3] or '0', v[4] or '0', v[5] or '0',
    v[6] or '', v[7] or '0', v[8] or '', v[9] or '0', string.format('%.0f', remaining)}
end

if redis.call('EXISTS', KEYS[1]) == 1 then
  local cur = redis.call('HMGET', KEYS[1],
    'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
    'writer_epoch', 'lease_epoch', 'assignment_revision',
    'last_transition_id', 'last_transition_digest')
  if cur[1] ~= ARGV[1] then
    return {'UNAVAILABLE_NOT_SENT', 'LAYOUT_MISMATCH'}
  end
  if cur[7] == ARGV[10] then
    if cur[8] == ARGV[11] then
      local remaining = 0
      if cur[2] == 'ACTIVE' then
        remaining = tonumber(cur[3]) - now
        if remaining < 0 then remaining = 0 end
      end
      return snapshot('ALREADY_APPLIED', remaining)
    end
    return snapshot('RECONCILE_REQUIRED', 0)
  end
  if cur[2] == 'ACTIVE' and tonumber(cur[3]) > now then
    return snapshot('ACTIVE_LEASE_CONFLICT', tonumber(cur[3]) - now)
  end
  local expire = now + ttl
  redis.call('HSET', KEYS[1],
    'holder_instance_id', ARGV[5],
    'holder_boot_id', ARGV[6],
    'writer_epoch', tostring(tonumber(cur[4]) + 1),
    'lease_epoch', tostring(tonumber(cur[5]) + 1),
    'assignment_revision', tostring(tonumber(cur[6]) + 1),
    'lease_state', 'ACTIVE',
    'lease_expire_at_ms', string.format('%.0f', expire),
    'last_transition_id', ARGV[10],
    'last_transition_at_ms', string.format('%.0f', now),
    'last_transition_digest', ARGV[11])
  return snapshot('APPLIED', ttl)
end

local expire = now + ttl
redis.call('HSET', KEYS[1],
  'schema_version', '1',
  'authority_layout_version', ARGV[1],
  'hash_version', ARGV[2],
  'route_domain', ARGV[3],
  'virtual_shard_id', ARGV[4],
  'holder_instance_id', ARGV[5],
  'holder_boot_id', ARGV[6],
  'writer_epoch', '1',
  'lease_epoch', '1',
  'assignment_revision', '1',
  'lease_state', 'ACTIVE',
  'lease_expire_at_ms', string.format('%.0f', expire),
  'last_transition_id', ARGV[10],
  'last_transition_at_ms', string.format('%.0f', now),
  'last_transition_digest', ARGV[11])
return snapshot('APPLIED', ttl)
`

const luaRenewShardAuthoritySrc = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ttl = tonumber(ARGV[7])
local min_ttl = tonumber(ARGV[8])
local max_ttl = tonumber(ARGV[9])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end

local function snapshot(status, remaining)
  local v = redis.call('HMGET', KEYS[1],
    'holder_instance_id', 'holder_boot_id', 'writer_epoch', 'lease_epoch',
    'assignment_revision', 'lease_state', 'lease_expire_at_ms',
    'last_transition_id', 'last_transition_at_ms')
  return {status, v[1] or '', v[2] or '', v[3] or '0', v[4] or '0', v[5] or '0',
    v[6] or '', v[7] or '0', v[8] or '', v[9] or '0', string.format('%.0f', remaining)}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local cur = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if cur[1] ~= ARGV[1] then
  return {'UNAVAILABLE_NOT_SENT', 'LAYOUT_MISMATCH'}
end
if cur[2] ~= 'ACTIVE' or tonumber(cur[3]) <= now then
  return snapshot('LEASE_EXPIRED', 0)
end
if cur[4] ~= ARGV[2] or cur[5] ~= ARGV[3] or cur[6] ~= ARGV[4] or cur[7] ~= ARGV[5] or cur[8] ~= ARGV[6] then
  return snapshot('FENCE_MISMATCH', 0)
end
local old_expire = tonumber(cur[3])
local new_expire = now + ttl
if new_expire > old_expire then
  redis.call('HSET', KEYS[1], 'lease_expire_at_ms', string.format('%.0f', new_expire))
  return snapshot('APPLIED', ttl)
end
return snapshot('APPLIED', old_expire - now)
`

const luaTransferShardAuthoritySrc = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ttl = tonumber(ARGV[9])
local min_ttl = tonumber(ARGV[10])
local max_ttl = tonumber(ARGV[11])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end

local function snapshot(status, remaining)
  local v = redis.call('HMGET', KEYS[1],
    'holder_instance_id', 'holder_boot_id', 'writer_epoch', 'lease_epoch',
    'assignment_revision', 'lease_state', 'lease_expire_at_ms',
    'last_transition_id', 'last_transition_at_ms')
  return {status, v[1] or '', v[2] or '', v[3] or '0', v[4] or '0', v[5] or '0',
    v[6] or '', v[7] or '0', v[8] or '', v[9] or '0', string.format('%.0f', remaining)}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local cur = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch', 'last_transition_id', 'last_transition_digest')
if cur[1] ~= ARGV[1] then
  return {'UNAVAILABLE_NOT_SENT', 'LAYOUT_MISMATCH'}
end
if cur[9] == ARGV[12] then
  if cur[10] == ARGV[13] then
    local remaining = 0
    if cur[2] == 'ACTIVE' then
      remaining = tonumber(cur[3]) - now
      if remaining < 0 then remaining = 0 end
    end
    return snapshot('ALREADY_APPLIED', remaining)
  end
  return snapshot('RECONCILE_REQUIRED', 0)
end
if cur[2] ~= 'ACTIVE' or tonumber(cur[3]) <= now then
  return snapshot('LEASE_EXPIRED', 0)
end
if cur[4] ~= ARGV[2] or cur[5] ~= ARGV[3] or cur[6] ~= ARGV[4] or cur[7] ~= ARGV[5] or cur[8] ~= ARGV[6] then
  return snapshot('FENCE_MISMATCH', 0)
end
local expire = now + ttl
redis.call('HSET', KEYS[1],
  'holder_instance_id', ARGV[7],
  'holder_boot_id', ARGV[8],
  'writer_epoch', tostring(tonumber(cur[7]) + 1),
  'lease_epoch', tostring(tonumber(cur[8]) + 1),
  'assignment_revision', tostring(tonumber(cur[6]) + 1),
  'lease_state', 'ACTIVE',
  'lease_expire_at_ms', string.format('%.0f', expire),
  'last_transition_id', ARGV[12],
  'last_transition_at_ms', string.format('%.0f', now),
  'last_transition_digest', ARGV[13])
return snapshot('APPLIED', ttl)
`

// luaTakeoverExpiredShardAuthoritySrc is the crash-only counterpart of a
// transfer. It accepts exactly one fenced, ACTIVE-but-expired generation and
// rejects a still-live lease. Replay is checked before the expiry predicate so
// a successful takeover remains idempotent after the new lease becomes live.
const luaTakeoverExpiredShardAuthoritySrc = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local ttl = tonumber(ARGV[9])
local min_ttl = tonumber(ARGV[10])
local max_ttl = tonumber(ARGV[11])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end

local function snapshot(status, remaining)
  local v = redis.call('HMGET', KEYS[1],
    'holder_instance_id', 'holder_boot_id', 'writer_epoch', 'lease_epoch',
    'assignment_revision', 'lease_state', 'lease_expire_at_ms',
    'last_transition_id', 'last_transition_at_ms')
  return {status, v[1] or '', v[2] or '', v[3] or '0', v[4] or '0', v[5] or '0',
    v[6] or '', v[7] or '0', v[8] or '', v[9] or '0', string.format('%.0f', remaining)}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local cur = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch', 'last_transition_id', 'last_transition_digest')
if cur[1] ~= ARGV[1] then
  return {'UNAVAILABLE_NOT_SENT', 'LAYOUT_MISMATCH'}
end
if cur[9] == ARGV[12] then
  if cur[10] == ARGV[13] then
    local remaining = 0
    if cur[2] == 'ACTIVE' then
      remaining = tonumber(cur[3]) - now
      if remaining < 0 then remaining = 0 end
    end
    return snapshot('ALREADY_APPLIED', remaining)
  end
  return snapshot('RECONCILE_REQUIRED', 0)
end
if cur[2] ~= 'ACTIVE' then
  return snapshot('LEASE_EXPIRED', 0)
end
if tonumber(cur[3]) > now then
  return snapshot('ACTIVE_LEASE_CONFLICT', tonumber(cur[3]) - now)
end
if cur[4] ~= ARGV[2] or cur[5] ~= ARGV[3] or cur[6] ~= ARGV[4] or cur[7] ~= ARGV[5] or cur[8] ~= ARGV[6] then
  return snapshot('FENCE_MISMATCH', 0)
end
local expire = now + ttl
redis.call('HSET', KEYS[1],
  'holder_instance_id', ARGV[7],
  'holder_boot_id', ARGV[8],
  'writer_epoch', tostring(tonumber(cur[7]) + 1),
  'lease_epoch', tostring(tonumber(cur[8]) + 1),
  'assignment_revision', tostring(tonumber(cur[6]) + 1),
  'lease_state', 'ACTIVE',
  'lease_expire_at_ms', string.format('%.0f', expire),
  'last_transition_id', ARGV[12],
  'last_transition_at_ms', string.format('%.0f', now),
  'last_transition_digest', ARGV[13])
return snapshot('APPLIED', ttl)
`

const luaUnassignShardAuthoritySrc = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local function snapshot(status, remaining)
  local v = redis.call('HMGET', KEYS[1],
    'holder_instance_id', 'holder_boot_id', 'writer_epoch', 'lease_epoch',
    'assignment_revision', 'lease_state', 'lease_expire_at_ms',
    'last_transition_id', 'last_transition_at_ms')
  return {status, v[1] or '', v[2] or '', v[3] or '0', v[4] or '0', v[5] or '0',
    v[6] or '', v[7] or '0', v[8] or '', v[9] or '0', string.format('%.0f', remaining)}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local cur = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch', 'last_transition_id', 'last_transition_digest')
if cur[1] ~= ARGV[1] then
  return {'UNAVAILABLE_NOT_SENT', 'LAYOUT_MISMATCH'}
end
if cur[8] == ARGV[7] then
  if cur[9] == ARGV[8] then
    return snapshot('ALREADY_APPLIED', 0)
  end
  return snapshot('RECONCILE_REQUIRED', 0)
end
if cur[2] ~= 'ACTIVE' then
  return snapshot('FENCE_MISMATCH', 0)
end
if cur[3] ~= ARGV[2] or cur[4] ~= ARGV[3] or cur[5] ~= ARGV[4] or cur[6] ~= ARGV[5] or cur[7] ~= ARGV[6] then
  return snapshot('FENCE_MISMATCH', 0)
end
redis.call('HSET', KEYS[1],
  'holder_instance_id', '',
  'holder_boot_id', '',
  'writer_epoch', tostring(tonumber(cur[6]) + 1),
  'lease_epoch', tostring(tonumber(cur[7]) + 1),
  'assignment_revision', tostring(tonumber(cur[5]) + 1),
  'lease_state', 'UNASSIGNED',
  'lease_expire_at_ms', '0',
  'last_transition_id', ARGV[7],
  'last_transition_at_ms', string.format('%.0f', now),
  'last_transition_digest', ARGV[8])
return snapshot('APPLIED', 0)
`

// LoadAndClaimActorV5 ARGV layout:
//
//	1 tag, 2 layout, 3 holder, 4 boot, 5 revision, 6 writer_epoch, 7 lease_epoch,
//	8 claim_operation_id, 9 new_writer_claim_id, 10 schema_version,
//	11 limit_profile_version, 12 initial_digest, 13 initial_manifest_lp,
//	14 initial_count, 15.. initial field/value pairs,
//	15+2*count max_model_fields, 16+2*count max_model_bytes,
//	17+2*count max_receipts, 18+2*count max_receipt_bytes,
//	19+2*count max_resolutions.
const luaLoadAndClaimActorV5Src = luaCheckTag + luaCommonLP + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'WRITER_EPOCH_MISMATCH'}
end
local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if sh[1] ~= ARGV[2] then
  return {'LAYOUT_MISMATCH'}
end
if sh[2] ~= 'ACTIVE' or tonumber(sh[3]) <= now then
  return {'WRITER_EPOCH_MISMATCH'}
end
if sh[4] ~= ARGV[3] or sh[5] ~= ARGV[4] or sh[6] ~= ARGV[5] or sh[7] ~= ARGV[6] or sh[8] ~= ARGV[7] then
  return {'WRITER_EPOCH_MISMATCH'}
end

local count = tonumber(ARGV[14])
if count > 32 then
  return {'CAPACITY_EXCEEDED'}
end
local max_fields = tonumber(ARGV[15 + 2 * count])
local max_bytes = tonumber(ARGV[16 + 2 * count])
local max_receipts = tonumber(ARGV[17 + 2 * count])
local max_receipt_bytes = tonumber(ARGV[18 + 2 * count])
local max_resolutions = tonumber(ARGV[19 + 2 * count])
local initial_model_layout = ARGV[20 + 2 * count]
local exact_v2_manifest = ARGV[21 + 2 * count]

if initial_model_layout ~= '1' and initial_model_layout ~= '2' then
  return {'CONTRACT_ERROR'}
end
if initial_model_layout == '2' and ARGV[13] ~= exact_v2_manifest then
  return {'CONTRACT_ERROR'}
end

if redis.call('EXISTS', KEYS[2]) == 0 then
  local total = 0
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    total = total + #ARGV[fi + 1] + #ARGV[fi + 2]
  end
  if count > max_fields or total > max_bytes then
    return {'CAPACITY_EXCEEDED'}
  end
  local hset_args = {
    '_meta:schema_version', ARGV[10],
    '_meta:authority_layout_version', ARGV[2],
    '_meta:writer_epoch', sh[7],
    '_meta:writer_claim_id', '',
    '_meta:record_version', '1',
    '_meta:business_state_digest', ARGV[12],
    '_meta:updated_at_ms', string.format('%.0f', now),
    '_meta:limit_profile_version', ARGV[11],
    '_meta:model_layout_version', initial_model_layout,
    '_meta:model_field_count', tostring(count),
    '_meta:model_total_bytes', tostring(total),
    '_meta:receipt_count', '0',
    '_meta:receipt_bytes', '0',
    '_meta:receipt_manifest', '',
    '_meta:resolution_count', '0',
    '_meta:last_claim_operation_id', '',
    '_meta:model_manifest', ARGV[13]}
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    hset_args[#hset_args + 1] = 'model:' .. ARGV[fi + 1]
    hset_args[#hset_args + 1] = ARGV[fi + 2]
  end
  redis.call('HSET', KEYS[2], unpack(hset_args))
end

local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:last_claim_operation_id',
  '_meta:record_version', '_meta:business_state_digest',
  '_meta:model_manifest', '_meta:schema_version',
  '_meta:authority_layout_version', '_meta:writer_epoch',
  '_meta:model_field_count', '_meta:model_total_bytes',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:resolution_count',
  '_meta:limit_profile_version', '_meta:model_layout_version')

local record_version = tonumber(meta[3])
local stored_writer_epoch = tonumber(meta[8])
local model_count = tonumber(meta[9])
local model_bytes = tonumber(meta[10])
local receipt_count = tonumber(meta[11])
local receipt_bytes = tonumber(meta[12])
local resolution_count = tonumber(meta[14])
local model_layout = meta[16]
local fixed_meta_fields = 17
if not model_layout then
  model_layout = '1'
  fixed_meta_fields = 16
end
if not record_version or record_version < 1 or record_version % 1 ~= 0 or
   not stored_writer_epoch or stored_writer_epoch < 1 or stored_writer_epoch % 1 ~= 0 or
   not model_count or model_count < 0 or model_count % 1 ~= 0 or model_count > max_fields or
   not model_bytes or model_bytes < 0 or model_bytes % 1 ~= 0 or model_bytes > max_bytes or
   not receipt_count or receipt_count < 0 or receipt_count % 1 ~= 0 or receipt_count > max_receipts or
   not receipt_bytes or receipt_bytes < 0 or receipt_bytes % 1 ~= 0 or receipt_bytes > max_receipt_bytes or
   not resolution_count or resolution_count < 0 or resolution_count % 1 ~= 0 or resolution_count > max_resolutions or
   not meta[4] or #meta[4] ~= 64 or not string.match(meta[4], '^[0-9a-f]+$') or
   meta[6] ~= ARGV[10] or meta[7] ~= ARGV[2] or not meta[13] or not meta[15] or meta[15] == '' or
   (model_layout ~= '1' and model_layout ~= '2') or
   (model_layout == '2' and meta[5] ~= exact_v2_manifest) then
  return {'MALFORMED_RECORD'}
end
if (receipt_count == 0 and (receipt_bytes ~= 0 or meta[13] ~= '')) or
   (receipt_count > 0 and receipt_bytes == 0) then
  return {'MALFORMED_RECORD'}
end

if #meta[5] > max_fields * 68 or #meta[13] > max_receipts * 68 then
  return {'MALFORMED_RECORD'}
end
local model_fields = lp_fields(meta[5])
local receipt_fields = lp_fields(meta[13])
if not model_fields or #model_fields ~= model_count or
   not receipt_fields or #receipt_fields ~= receipt_count then
  return {'MALFORMED_RECORD'}
end
local seen_model = {}
local model_values = {}
local calculated_model_bytes = 0
for _, field in ipairs(model_fields) do
  if field == '' or seen_model[field] then
    return {'MALFORMED_RECORD'}
  end
  seen_model[field] = true
  local value = redis.call('HGET', KEYS[2], 'model:' .. field)
  if not value then
    return {'MALFORMED_RECORD'}
  end
  model_values[#model_values + 1] = value
  calculated_model_bytes = calculated_model_bytes + #field + #value
end
if calculated_model_bytes ~= model_bytes then
  return {'MALFORMED_RECORD'}
end
local seen_receipt = {}
local receipt_values = {}
local calculated_receipt_bytes = 0
-- A new/reconciled WriterClaim performs the complete receipt-value audit.
-- Exact ALREADY_CLAIMED re-entry is safe to use the counters/manifests that
-- this already-audited claim has maintained atomically in the Commit script;
-- a targeted receipt is still parsed and hash-checked whenever it is replayed.
local same_claim = meta[2] == ARGV[8] and meta[1] == ARGV[9]
for _, digest in ipairs(receipt_fields) do
  if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') or seen_receipt[digest] then
    return {'MALFORMED_RECORD'}
  end
  seen_receipt[digest] = true
  if not same_claim then
    local value = redis.call('HGET', KEYS[2], 'receipt:op:' .. digest)
    if not value or #value > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local fields = lp_fields(string.sub(value, 65))
    if #value < 64 or not fields or #fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    receipt_values[#receipt_values + 1] = value
    calculated_receipt_bytes = calculated_receipt_bytes + #value
  end
end
if (not same_claim and calculated_receipt_bytes ~= receipt_bytes) or
   redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + model_count + receipt_count + resolution_count then
  return {'MALFORMED_RECORD'}
end

local function result(status, claim)
  local out = {status, meta[3], meta[4], meta[5], claim, meta[6], sh[7],
    meta[9], meta[10], meta[11], meta[12], meta[13], meta[14], meta[7], meta[15], model_layout,
    tostring(fixed_meta_fields + model_count + receipt_count + resolution_count)}
  -- These values were already read above for the exact corruption and byte
  -- accounting checks. Returning the same immutable snapshot avoids a second
  -- client-side HMGET+HLEN without weakening any validation.
  for _, value in ipairs(model_values) do out[#out + 1] = value end
  if status ~= 'ALREADY_CLAIMED' then
    for _, value in ipairs(receipt_values) do out[#out + 1] = value end
  end
  return out
end
if meta[2] == ARGV[8] then
  if meta[1] ~= ARGV[9] then
    return result('RECONCILE_REQUIRED', meta[1])
  end
  return result('ALREADY_CLAIMED', meta[1])
end
if meta[1] == ARGV[9] then
  return result('RECONCILE_REQUIRED', meta[1])
end
redis.call('HSET', KEYS[2],
  '_meta:writer_claim_id', ARGV[9],
  '_meta:last_claim_operation_id', ARGV[8],
  '_meta:writer_epoch', sh[7],
  '_meta:updated_at_ms', string.format('%.0f', now))
return result('CLAIMED', ARGV[9])
`

// LoadAndClaimActorV6 preserves V5 byte-for-byte and removes per-receipt Redis
// commands only for exact re-entry into the same fully audited WriterClaim.
// The monolithic receipt manifest is still returned and decoded, so response
// bytes and Go allocation remain O(receipt history), not strict O(1). Model
// values still cross the Lua/Go boundary so the bounded Farm6/Player8 envelope,
// byte count, and business digest checks remain authoritative.
var luaLoadAndClaimActorSrc = loadAndClaimActorV6(luaLoadAndClaimActorV5Src)

func replaceFrozenScriptBlock(source, oldBlock, newBlock, label string) string {
	if strings.Count(source, oldBlock) != 1 {
		panic(label + " frozen insertion point missing")
	}
	return strings.Replace(source, oldBlock, newBlock, 1)
}

func loadAndClaimActorV6(source string) string {
	source = replaceFrozenScriptBlock(source, `if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'WRITER_EPOCH_MISMATCH'}
end
local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if sh[1] ~= ARGV[2] then`, `local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if not sh[1] then
  return {'WRITER_EPOCH_MISMATCH'}
end
if sh[1] ~= ARGV[2] then`, "LoadAndClaimActorV6 shard read")

	source = replaceFrozenScriptBlock(source, `if redis.call('EXISTS', KEYS[2]) == 0 then
  local total = 0
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    total = total + #ARGV[fi + 1] + #ARGV[fi + 2]
  end
  if count > max_fields or total > max_bytes then
    return {'CAPACITY_EXCEEDED'}
  end
  local hset_args = {
    '_meta:schema_version', ARGV[10],
    '_meta:authority_layout_version', ARGV[2],
    '_meta:writer_epoch', sh[7],
    '_meta:writer_claim_id', '',
    '_meta:record_version', '1',
    '_meta:business_state_digest', ARGV[12],
    '_meta:updated_at_ms', string.format('%.0f', now),
    '_meta:limit_profile_version', ARGV[11],
    '_meta:model_layout_version', initial_model_layout,
    '_meta:model_field_count', tostring(count),
    '_meta:model_total_bytes', tostring(total),
    '_meta:receipt_count', '0',
    '_meta:receipt_bytes', '0',
    '_meta:receipt_manifest', '',
    '_meta:resolution_count', '0',
    '_meta:last_claim_operation_id', '',
    '_meta:model_manifest', ARGV[13]}
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    hset_args[#hset_args + 1] = 'model:' .. ARGV[fi + 1]
    hset_args[#hset_args + 1] = ARGV[fi + 2]
  end
  redis.call('HSET', KEYS[2], unpack(hset_args))
end

local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:last_claim_operation_id',
  '_meta:record_version', '_meta:business_state_digest',
  '_meta:model_manifest', '_meta:schema_version',
  '_meta:authority_layout_version', '_meta:writer_epoch',
  '_meta:model_field_count', '_meta:model_total_bytes',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:resolution_count',
  '_meta:limit_profile_version', '_meta:model_layout_version')`, `local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:last_claim_operation_id',
  '_meta:record_version', '_meta:business_state_digest',
  '_meta:model_manifest', '_meta:schema_version',
  '_meta:authority_layout_version', '_meta:writer_epoch',
  '_meta:model_field_count', '_meta:model_total_bytes',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:resolution_count',
  '_meta:limit_profile_version', '_meta:model_layout_version')
local actor_missing = true
for i = 1, 16 do
  if meta[i] then actor_missing = false break end
end
if actor_missing then
  -- HMGET is the hot existence proof. The fallback distinguishes a truly
  -- absent actor from a corrupt hash containing only unknown fields.
  if redis.call('EXISTS', KEYS[2]) == 1 then
    return {'MALFORMED_RECORD'}
  end
  local total = 0
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    total = total + #ARGV[fi + 1] + #ARGV[fi + 2]
  end
  if count > max_fields or total > max_bytes then
    return {'CAPACITY_EXCEEDED'}
  end
  local hset_args = {
    '_meta:schema_version', ARGV[10],
    '_meta:authority_layout_version', ARGV[2],
    '_meta:writer_epoch', sh[7],
    '_meta:writer_claim_id', '',
    '_meta:record_version', '1',
    '_meta:business_state_digest', ARGV[12],
    '_meta:updated_at_ms', string.format('%.0f', now),
    '_meta:limit_profile_version', ARGV[11],
    '_meta:model_layout_version', initial_model_layout,
    '_meta:model_field_count', tostring(count),
    '_meta:model_total_bytes', tostring(total),
    '_meta:receipt_count', '0',
    '_meta:receipt_bytes', '0',
    '_meta:receipt_manifest', '',
    '_meta:resolution_count', '0',
    '_meta:last_claim_operation_id', '',
    '_meta:model_manifest', ARGV[13]}
  for i = 1, count do
    local fi = 14 + 2 * (i - 1)
    hset_args[#hset_args + 1] = 'model:' .. ARGV[fi + 1]
    hset_args[#hset_args + 1] = ARGV[fi + 2]
  end
  redis.call('HSET', KEYS[2], unpack(hset_args))
  meta = {'', '', '1', ARGV[12], ARGV[13], ARGV[10], ARGV[2], sh[7],
    tostring(count), tostring(total), '0', '0', '', '0', ARGV[11], initial_model_layout}
end`, "LoadAndClaimActorV6 actor read")

	source = replaceFrozenScriptBlock(source, `local model_fields = lp_fields(meta[5])
local receipt_fields = lp_fields(meta[13])
if not model_fields or #model_fields ~= model_count or
   not receipt_fields or #receipt_fields ~= receipt_count then
  return {'MALFORMED_RECORD'}
end`, `local model_fields = lp_fields(meta[5])
local same_claim = meta[2] == ARGV[8] and meta[1] == ARGV[9]
local receipt_fields = {}
if not same_claim then receipt_fields = lp_fields(meta[13]) end
if not model_fields or #model_fields ~= model_count or
   (not same_claim and (not receipt_fields or #receipt_fields ~= receipt_count)) then
  return {'MALFORMED_RECORD'}
end`, "LoadAndClaimActorV6 manifest parsing")

	source = replaceFrozenScriptBlock(source, `local seen_receipt = {}
local receipt_values = {}
local calculated_receipt_bytes = 0
-- A new/reconciled WriterClaim performs the complete receipt-value audit.
-- Exact ALREADY_CLAIMED re-entry is safe to use the counters/manifests that
-- this already-audited claim has maintained atomically in the Commit script;
-- a targeted receipt is still parsed and hash-checked whenever it is replayed.
local same_claim = meta[2] == ARGV[8] and meta[1] == ARGV[9]
for _, digest in ipairs(receipt_fields) do
  if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') or seen_receipt[digest] then
    return {'MALFORMED_RECORD'}
  end
  seen_receipt[digest] = true
  if not same_claim then
    local value = redis.call('HGET', KEYS[2], 'receipt:op:' .. digest)
    if not value or #value > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local fields = lp_fields(string.sub(value, 65))
    if #value < 64 or not fields or #fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    receipt_values[#receipt_values + 1] = value
    calculated_receipt_bytes = calculated_receipt_bytes + #value
  end
end
if (not same_claim and calculated_receipt_bytes ~= receipt_bytes) or
   redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + model_count + receipt_count + resolution_count then
  return {'MALFORMED_RECORD'}
end`, `local receipt_values = {}
if not same_claim then
  local seen_receipt = {}
  local calculated_receipt_bytes = 0
  -- A new/reconciled WriterClaim performs the complete receipt-value audit.
  -- Exact ALREADY_CLAIMED re-entry relies only on the receipt invariant this
  -- claim established and V13 maintains atomically.
  for _, digest in ipairs(receipt_fields) do
    if #digest ~= 64 or not string.match(digest, '^[0-9a-f]+$') or seen_receipt[digest] then
      return {'MALFORMED_RECORD'}
    end
    seen_receipt[digest] = true
    local value = redis.call('HGET', KEYS[2], 'receipt:op:' .. digest)
    if not value or #value > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local fields = lp_fields(string.sub(value, 65))
    if #value < 64 or not fields or #fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    receipt_values[#receipt_values + 1] = value
    calculated_receipt_bytes = calculated_receipt_bytes + #value
  end
  if calculated_receipt_bytes ~= receipt_bytes then
    return {'MALFORMED_RECORD'}
  end
end
-- HLEN keeps Redis command count constant on the audited branch. The
-- monolithic receipt manifest is still returned below, so response bytes and
-- Go decoding remain O(receipt history); this is not a strict O(1) operation.
if redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + model_count + receipt_count + resolution_count then
  return {'MALFORMED_RECORD'}
end`, "LoadAndClaimActorV6 receipt audit")
	return source
}

// CommitActorMutationWithOrdersV12 carries sparse upsert field/value pairs plus
// delete names. Unchanged model bytes stay on the actor HASH and are merged in
// Lua; ARGV no longer repeats the full CandidateState. PENDING SourceOrder
// hashes omit idle lease/error/completed fields; ClaimWork treats missing as
// zero. Receipt/outcome/order/due atomicity and the one-way v1-to-v2 migration
// intent are unchanged.
// ARGV layout:
//
//	1 tag, 2 holder, 3 boot, 4 revision, 5 writer_epoch, 6 lease_epoch,
//	7 expected_claim, 8 expected_record_version, 9 layout, 10 domain,
//	11 actor_id, 12 virtual_shard, 13 operation_digest, 14 payload_hash,
//	15 key_identity_lp, 16 receipt_id, 17 subject, 18 operation_id,
//	19 outcome_status, 20 error_code, 21 outcome_payload, 22 candidate_digest,
//	23 schema_version, 24 limit_profile_version, 25 receipt_ref,
//	26 upsert_count U, 27.. U field/value pairs, resulting_manifest_lp,
//	order_count N, per order: order_id, order_type, payload, due_at_ms,
//	work_ref, payload_hash, subject_player_id (0 falls back to ARGV[17]),
//	operation_id (empty falls back to ARGV[18]),
//	schedule_count S, per schedule: schedule_id, schedule_type, payload,
//	due_at_ms, generation, schedule_ref, payload_hash, subject_player_id,
//	delete_count D, D names,
//	intent, resulting_model_layout, resulting_total_bytes, resulting_digest,
//	exact_v2_manifest_lp, legacy_replace,
//	then 12 limits: max_model_fields, max_model_bytes, max_receipts,
//	max_receipt_bytes, max_bucket_pending_work, max_bucket_pending_work_bytes,
//	max_order_payload_bytes, max_schedule_payload_bytes, receipt_full_retention_ms,
//	work_total_deadline_ms, work_max_retry_count, max_schedules,
//	audited_claim (1 only for a Binding established by full LoadAndClaim).
const luaCommitActorWithOrdersV12Src = luaCheckTag + luaCommonLP + luaRedisTime + `
local U = tonumber(ARGV[26])
local fbase = 27
local manifest = ARGV[fbase + 2 * U]
local npos = fbase + 2 * U + 1
local N = tonumber(ARGV[npos])
local obase = npos + 1
local spos = obase + 8 * N
local S = tonumber(ARGV[spos])
local sbase = spos + 1
local dpos = sbase + 8 * S
local D = tonumber(ARGV[dpos])
local dbase = dpos + 1
local mpos = dbase + D
local intent = ARGV[mpos]
local resulting_layout = ARGV[mpos + 1]
local resulting_total_bytes = tonumber(ARGV[mpos + 2])
local resulting_digest = ARGV[mpos + 3]
local exact_v2_manifest = ARGV[mpos + 4]
local legacy_replace = ARGV[mpos + 5]
local schedule_due_key = nil
local schedule_index_mode = tonumber(ARGV[mpos + 6])
if schedule_index_mode == nil or schedule_index_mode < 0 or schedule_index_mode > 2 then
  return {'CONTRACT_ERROR'}
end
local extra_keys = 0
if schedule_index_mode > 0 then extra_keys = 1 end
local lpos = mpos + 7
if schedule_index_mode > 0 then
  schedule_due_key = KEYS[6 + N + S]
end

if U == nil or D == nil or N == nil or S == nil or resulting_total_bytes == nil then
  return {'CONTRACT_ERROR'}
end
if U > 32 or D > 32 or N > 4 or S > 16 then
  return {'CAPACITY_EXCEEDED'}
end
if #KEYS ~= 5 + N + S + extra_keys then
  return {'CONTRACT_ERROR'}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'WRITER_EPOCH_MISMATCH'}
end
local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if sh[1] ~= ARGV[9] then
  return {'LAYOUT_MISMATCH'}
end
if sh[2] ~= 'ACTIVE' or tonumber(sh[3]) <= now then
  return {'WRITER_EPOCH_MISMATCH'}
end
if sh[4] ~= ARGV[2] or sh[5] ~= ARGV[3] or sh[6] ~= ARGV[4] or sh[7] ~= ARGV[5] or sh[8] ~= ARGV[6] then
  return {'WRITER_EPOCH_MISMATCH'}
end

if redis.call('EXISTS', KEYS[2]) == 0 then
  return {'ACTOR_NOT_FOUND'}
end
local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:record_version',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:model_field_count',
  '_meta:model_total_bytes', '_meta:model_manifest',
  '_meta:resolution_count', '_meta:schema_version',
  '_meta:model_layout_version')
if meta[1] ~= ARGV[7] then
  return {'WRITER_CLAIM_MISMATCH', meta[2]}
end

local receipt_field = 'receipt:op:' .. ARGV[13]
local existing = redis.call('HGET', KEYS[2], receipt_field)
if existing then
  -- The digest only locates the receipt; the stored full key identity and
  -- payload hash decide replay vs conflict. This deliberately precedes the
  -- RecordVersion and every candidate/capacity check.
  local id_fields = lp_fields(string.sub(existing, 65))
  if string.sub(existing, 1, 64) == ARGV[14] and id_fields and id_fields[1] == ARGV[15] then
    return {'ALREADY_COMMITTED', existing, meta[2]}
  end
  return {'IDEMPOTENCY_CONFLICT', meta[2]}
end
if meta[2] ~= ARGV[8] then
  return {'RECORD_VERSION_CONFLICT', meta[2]}
end

local max_fields = tonumber(ARGV[lpos])
local max_bytes = tonumber(ARGV[lpos + 1])
local max_receipts = tonumber(ARGV[lpos + 2])
local max_receipt_bytes = tonumber(ARGV[lpos + 3])
local max_pending = tonumber(ARGV[lpos + 4])
local max_pending_bytes = tonumber(ARGV[lpos + 5])
local max_order_payload = tonumber(ARGV[lpos + 6])
local max_schedule_payload = tonumber(ARGV[lpos + 7])
local retention_ms = tonumber(ARGV[lpos + 8])
local work_deadline_ms = tonumber(ARGV[lpos + 9])
local work_max_retry = tonumber(ARGV[lpos + 10])
local max_schedules = tonumber(ARGV[lpos + 11])
local audited_claim = ARGV[lpos + 12]

if max_fields == nil or max_bytes == nil or max_receipts == nil or max_receipt_bytes == nil or
   max_pending == nil or max_pending_bytes == nil or max_order_payload == nil or
   max_schedule_payload == nil or retention_ms == nil or work_deadline_ms == nil or
   work_max_retry == nil or max_schedules == nil or
   (audited_claim ~= '0' and audited_claim ~= '1') then
  return {'CONTRACT_ERROR'}
end
local receipt_count = tonumber(meta[3])
local receipt_bytes = tonumber(meta[4])
local model_count = tonumber(meta[6])
local stored_model_bytes = tonumber(meta[7])
local resolution_count = tonumber(meta[9])
local current_layout = meta[11]
local fixed_meta_fields = 17
if not current_layout then
  current_layout = '1'
  fixed_meta_fields = 16
end
if (meta[5] and #meta[5] > max_receipts * 68) or
   (meta[8] and #meta[8] > max_fields * 68) then
  return {'MALFORMED_RECORD'}
end
local receipt_fields = lp_fields(meta[5] or '')
local model_fields = lp_fields(meta[8] or '')
if meta[10] ~= ARGV[23] or
   not receipt_count or receipt_count < 0 or receipt_count % 1 ~= 0 or receipt_count > max_receipts or
   not receipt_bytes or receipt_bytes < 0 or receipt_bytes % 1 ~= 0 or receipt_bytes > max_receipt_bytes or
   not model_count or model_count < 0 or model_count % 1 ~= 0 or model_count > max_fields or
   not stored_model_bytes or stored_model_bytes < 0 or stored_model_bytes % 1 ~= 0 or stored_model_bytes > max_bytes or
   not resolution_count or resolution_count < 0 or resolution_count % 1 ~= 0 or
   not meta[5] or not receipt_fields or #receipt_fields ~= receipt_count or
   not meta[8] or not model_fields or #model_fields ~= model_count or
   (current_layout ~= '1' and current_layout ~= '2') or
   (current_layout == '2' and meta[8] ~= exact_v2_manifest) or
   (receipt_count == 0 and (receipt_bytes ~= 0 or meta[5] ~= '')) or
   (receipt_count > 0 and receipt_bytes == 0) then
  return {'MALFORMED_RECORD'}
end
local seen_model = {}
local current_values = {}
local calculated_model_bytes = 0
for _, stored_field in ipairs(model_fields) do
  if #stored_field == 0 or #stored_field > 64 or
     not string.match(stored_field, '^[a-z0-9_]+$') or seen_model[stored_field] then
    return {'MALFORMED_RECORD'}
  end
  seen_model[stored_field] = true
  if audited_claim == '0' then
    local stored_value = redis.call('HGET', KEYS[2], 'model:' .. stored_field)
    if not stored_value then
      return {'MALFORMED_RECORD'}
    end
    current_values[stored_field] = stored_value
    calculated_model_bytes = calculated_model_bytes + #stored_field + #stored_value
  end
end
if audited_claim == '0' and calculated_model_bytes ~= stored_model_bytes then
  return {'MALFORMED_RECORD'}
end
local seen_receipt = {}
local calculated_receipt_bytes = 0
for _, stored_digest in ipairs(receipt_fields) do
  if #stored_digest ~= 64 or not string.match(stored_digest, '^[0-9a-f]+$') or seen_receipt[stored_digest] then
    return {'MALFORMED_RECORD'}
  end
  seen_receipt[stored_digest] = true
  if audited_claim == '0' then
    local stored_receipt = redis.call('HGET', KEYS[2], 'receipt:op:' .. stored_digest)
    if not stored_receipt or #stored_receipt > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local stored_receipt_fields = lp_fields(string.sub(stored_receipt, 65))
    if #stored_receipt < 64 or not stored_receipt_fields or #stored_receipt_fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    calculated_receipt_bytes = calculated_receipt_bytes + #stored_receipt
  end
end
if (audited_claim == '0' and calculated_receipt_bytes ~= receipt_bytes) or
   redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + #model_fields + #receipt_fields + resolution_count then
  return {'MALFORMED_RECORD'}
end

local upsert_values = {}
local upserts = {}
local previous_upsert = nil
for i = 1, U do
  local fi = fbase + 2 * (i - 1)
  local field = ARGV[fi]
  local value = ARGV[fi + 1]
  if field == nil or value == nil or #field == 0 or #field > 64 or not string.match(field, '^[a-z0-9_]+$') or
     upserts[field] or (previous_upsert and previous_upsert >= field) then
    return {'CONTRACT_ERROR'}
  end
  previous_upsert = field
  upserts[field] = true
  upsert_values[field] = value
end
local deletes = {}
local previous_delete = nil
for i = 1, D do
  local field = ARGV[dbase + i - 1]
  if upserts[field] or deletes[field] or
     (previous_delete and previous_delete >= field) then
    return {'CONTRACT_ERROR'}
  end
  previous_delete = field
  deletes[field] = true
end

if legacy_replace == '1' then
  if current_layout ~= '1' or resulting_layout ~= '1' or intent ~= 'UPDATE' then
    return {'CONTRACT_ERROR'}
  end
elseif legacy_replace ~= '0' then
  return {'CONTRACT_ERROR'}
elseif intent == 'UPDATE' then
  if current_layout ~= resulting_layout then
    return {'CONTRACT_ERROR'}
  end
elseif intent == 'MIGRATE_V1_TO_V2' then
  if current_layout ~= '1' or resulting_layout ~= '2' or not seen_model['record'] or
     not deletes['record'] or upserts['record'] then
    return {'CONTRACT_ERROR'}
  end
else
  return {'CONTRACT_ERROR'}
end
if resulting_layout ~= '1' and resulting_layout ~= '2' then
  return {'CONTRACT_ERROR'}
end
if resulting_layout == '2' and manifest ~= exact_v2_manifest then
  return {'CONTRACT_ERROR'}
end

local new_fields = lp_fields(manifest)
if not new_fields then
  return {'CONTRACT_ERROR'}
end
local previous_field = nil
for i = 1, #new_fields do
  local field = new_fields[i]
  if #field == 0 or #field > 64 or not string.match(field, '^[a-z0-9_]+$') or
     (previous_field and previous_field >= field) then
    return {'CONTRACT_ERROR'}
  end
  previous_field = field
end

local resulting_count = 0
local model_bytes = 0
if audited_claim == '1' then
  -- LoadAndClaim fully audited this exact WriterClaim. RecordVersion CAS plus
  -- the Commit-only mutation contract means unchanged model values cannot
  -- differ here. Validate the sparse manifest transformation without HGETing
  -- every historical field; generic Commit below retains the full audit.
  local expected_fields = {}
  if legacy_replace == '1' then
    for field, _ in pairs(upsert_values) do
      expected_fields[field] = true
      resulting_count = resulting_count + 1
    end
  else
    for field, _ in pairs(seen_model) do
      if not deletes[field] then
        expected_fields[field] = true
        resulting_count = resulting_count + 1
      end
    end
    for field, _ in pairs(deletes) do
      if not seen_model[field] then return {'CONTRACT_ERROR'} end
    end
    for field, _ in pairs(upsert_values) do
      if deletes[field] then return {'CONTRACT_ERROR'} end
      if not expected_fields[field] then
        expected_fields[field] = true
        resulting_count = resulting_count + 1
      end
    end
  end
  if #new_fields ~= resulting_count then return {'CONTRACT_ERROR'} end
  for _, field in ipairs(new_fields) do
    if not expected_fields[field] then return {'CONTRACT_ERROR'} end
  end
  model_bytes = resulting_total_bytes
else
  local resulting_values = {}
  if legacy_replace == '1' then
    for field, value in pairs(upsert_values) do
      resulting_values[field] = value
      resulting_count = resulting_count + 1
    end
  else
    for field, value in pairs(current_values) do
      if deletes[field] then
        -- dropped
      elseif upserts[field] then
        if upsert_values[field] == value then return {'CONTRACT_ERROR'} end
        resulting_values[field] = upsert_values[field]
        resulting_count = resulting_count + 1
      else
        resulting_values[field] = value
        resulting_count = resulting_count + 1
      end
    end
    for field, value in pairs(upsert_values) do
      if resulting_values[field] == nil then
        if deletes[field] or current_values[field] then return {'CONTRACT_ERROR'} end
        resulting_values[field] = value
        resulting_count = resulting_count + 1
      end
    end
    for field, _ in pairs(deletes) do
      if current_values[field] == nil or resulting_values[field] ~= nil then
        return {'CONTRACT_ERROR'}
      end
    end
  end
  if #new_fields ~= resulting_count then return {'CONTRACT_ERROR'} end
  for _, field in ipairs(new_fields) do
    local value = resulting_values[field]
    if value == nil then return {'CONTRACT_ERROR'} end
    model_bytes = model_bytes + #field + #value
  end
end
if resulting_count > max_fields or model_bytes < 0 or model_bytes > max_bytes or model_bytes ~= resulting_total_bytes or
   ARGV[22] ~= resulting_digest or #resulting_digest ~= 64 or
   not string.match(resulting_digest, '^[0-9a-f]+$') then
  return {'CAPACITY_EXCEEDED'}
end

local receipt_value = ARGV[14] .. lp1(ARGV[15]) .. lp1(ARGV[16]) ..
  lp1(ARGV[19]) .. lp1(ARGV[20]) .. lp1(ARGV[21]) ..
  lp1(string.format('%.0f', now))
if #receipt_value > 1024 then
  return {'CAPACITY_EXCEEDED'}
end
if receipt_count + 1 > max_receipts or receipt_bytes + #receipt_value > max_receipt_bytes then
  return {'CAPACITY_EXCEEDED'}
end

local new_orders = 0
local new_order_bytes = 0
for j = 1, N do
  local oi = obase + 8 * (j - 1)
  local order_key = KEYS[5 + j]
  local payload = ARGV[oi + 2]
  if #payload > max_order_payload or #payload > 16384 then
    return {'CAPACITY_EXCEEDED'}
  end
  if redis.call('EXISTS', order_key) == 1 then
    local cur = redis.call('HMGET', order_key, 'work_type', 'typed_payload', 'next_due_at_ms', 'payload_hash', 'subject_player_id', 'operation_id')
    local subject = ARGV[oi + 6]
    if subject == nil or subject == '' or subject == '0' then
      subject = ARGV[17]
    end
    local operation_id = ARGV[oi + 7]
    if operation_id == nil or operation_id == '' then
      operation_id = ARGV[18]
    end
    if cur[1] ~= ARGV[oi + 1] or cur[2] ~= payload or cur[3] ~= ARGV[oi + 3] or cur[4] ~= ARGV[oi + 5] or cur[5] ~= subject or cur[6] ~= operation_id then
      return {'WORK_CONFLICT'}
    end
  else
    new_orders = new_orders + 1
    new_order_bytes = new_order_bytes + #payload
  end
end
local schedule_pending_delta = 0
local schedule_bytes_delta = 0
local new_schedules = 0
for j = 1, S do
  local si = sbase + 8 * (j - 1)
  local schedule_key = KEYS[5 + N + j]
  local payload = ARGV[si + 2]
  local due = tonumber(ARGV[si + 3])
  local generation = tonumber(ARGV[si + 4])
  local payload_hash = ARGV[si + 6]
  local subject = ARGV[si + 7]
  if due == nil or due < 0 or generation == nil or generation <= 0 or
     #payload > max_schedule_payload or #payload > 16384 or
     #payload_hash ~= 64 or not string.match(payload_hash, '^[0-9a-f]+$') or
     subject == nil or subject == '' or subject == '0' then
    return {'CONTRACT_ERROR'}
  end
  if redis.call('EXISTS', schedule_key) == 1 then
    local cur = redis.call('HMGET', schedule_key,
      'schedule_id', 'schedule_type', 'typed_payload', 'next_due_at_ms',
      'generation', 'payload_hash', 'subject_player_id', 'state', 'work_version')
    local current_generation = tonumber(cur[5])
    local current_work_version = tonumber(cur[9])
    if cur[1] ~= ARGV[si] or cur[2] ~= ARGV[si + 1] or current_generation == nil or
       current_generation <= 0 or current_work_version == nil or current_work_version <= 0 or
       cur[3] == nil or cur[4] == nil or cur[6] == nil or cur[7] == nil or cur[8] == nil or
       #cur[6] ~= 64 or not string.match(cur[6], '^[0-9a-f]+$') then
      return {'MALFORMED_RECORD'}
    end
    if generation == current_generation then
      if cur[3] ~= payload or cur[4] ~= ARGV[si + 3] or cur[6] ~= payload_hash or cur[7] ~= subject then
        return {'SCHEDULE_CONFLICT'}
      end
    elseif generation > current_generation and due > 0 then
      if cur[8] == 'PENDING' or cur[8] == 'SCHEDULE_DISPATCHING' then
        schedule_bytes_delta = schedule_bytes_delta + #payload - #cur[3]
      else
        schedule_pending_delta = schedule_pending_delta + 1
        schedule_bytes_delta = schedule_bytes_delta + #payload
      end
    elseif generation > current_generation and (cur[8] == 'PENDING' or cur[8] == 'SCHEDULE_DISPATCHING') then
      schedule_pending_delta = schedule_pending_delta - 1
      schedule_bytes_delta = schedule_bytes_delta - #cur[3]
    end
  else
    -- A zero-due first write is retained as a superseded tombstone so an
    -- older generation can never recreate a cancelled schedule.
    if due > 0 then
      schedule_pending_delta = schedule_pending_delta + 1
      schedule_bytes_delta = schedule_bytes_delta + #payload
    end
    new_schedules = new_schedules + 1
  end
end
local pending = tonumber(redis.call('HGET', KEYS[5], 'pending_work_count') or '0')
local pending_bytes = tonumber(redis.call('HGET', KEYS[5], 'pending_work_bytes') or '0')
local schedule_count = tonumber(redis.call('HGET', KEYS[5], 'schedule_count') or '0')
if pending == nil or pending < 0 or pending_bytes == nil or pending_bytes < 0 or
   schedule_count == nil or schedule_count < 0 or schedule_count + new_schedules > max_schedules or
   pending + new_orders + schedule_pending_delta < 0 or
   pending + new_orders + schedule_pending_delta > max_pending or
   pending_bytes + new_order_bytes + schedule_bytes_delta < 0 or
   pending_bytes + new_order_bytes + schedule_bytes_delta > max_pending_bytes then
  return {'CAPACITY_EXCEEDED'}
end

-- Linearization point: every check above passed, everything below writes.
if legacy_replace == '1' then
  local keep = {}
  for _, field in ipairs(new_fields) do keep[field] = true end
  for _, field in ipairs(model_fields) do
    if not keep[field] then redis.call('HDEL', KEYS[2], 'model:' .. field) end
  end
else
  for _, field in ipairs(model_fields) do
    if deletes[field] then redis.call('HDEL', KEYS[2], 'model:' .. field) end
  end
end
for _, field in ipairs(new_fields) do
  if upserts[field] then
    redis.call('HSET', KEYS[2], 'model:' .. field, upsert_values[field])
  end
end

redis.call('HSET', KEYS[2], receipt_field, receipt_value)
redis.call('HSET', KEYS[2],
  '_meta:record_version', tostring(tonumber(ARGV[8]) + 1),
  '_meta:business_state_digest', ARGV[22],
  '_meta:updated_at_ms', string.format('%.0f', now),
  '_meta:model_field_count', tostring(resulting_count),
  '_meta:model_total_bytes', tostring(model_bytes),
  '_meta:model_manifest', manifest,
  '_meta:model_layout_version', resulting_layout,
  '_meta:receipt_count', tostring(receipt_count + 1),
  '_meta:receipt_bytes', tostring(receipt_bytes + #receipt_value),
  '_meta:receipt_manifest', meta[5] .. lp1(ARGV[13]),
  '_meta:schema_version', ARGV[23],
  '_meta:limit_profile_version', ARGV[24])

local source_kind = 'PLAYER_ACTOR'
if ARGV[10] == 'FARM' then
  source_kind = 'FARM_ACTOR'
end
if redis.call('EXISTS', KEYS[5]) == 0 then
  redis.call('HSET', KEYS[5],
    'schema_version', '1',
    'authority_layout_version', ARGV[9],
    'route_domain', ARGV[10],
    'bucket_id', ARGV[12])
end
for j = 1, N do
  local oi = obase + 8 * (j - 1)
  local order_key = KEYS[5 + j]
  if redis.call('EXISTS', order_key) == 0 then
    local payload = ARGV[oi + 2]
    local subject = ARGV[oi + 6]
    if subject == nil or subject == '' or subject == '0' then
      subject = ARGV[17]
    end
    local operation_id = ARGV[oi + 7]
    if operation_id == nil or operation_id == '' then
      operation_id = ARGV[18]
    end
    redis.call('HSET', order_key,
      'schema_version', '1',
      'work_id', ARGV[oi],
      'work_type', ARGV[oi + 1],
      'placement_kind', 'SOURCE_PERSISTENCE_BUCKET',
      'bucket_version', '1',
      'bucket_id', ARGV[12],
      'source_authority_kind', source_kind,
      'canonical_source_authority_id', ARGV[11],
      'authority_layout_version', ARGV[9],
      'actor_route_domain', ARGV[10],
      'subject_player_id', subject,
      'operation_id', operation_id,
      'payload_hash', ARGV[oi + 5],
      'state', 'PENDING',
      'work_version', '1',
      'typed_payload', payload,
      'next_due_at_ms', ARGV[oi + 3],
      'retry_count', '0',
      'deadline_at_ms', string.format('%.0f', now + work_deadline_ms),
      'max_retry_count', tostring(work_max_retry),
      'created_at_ms', string.format('%.0f', now),
      'updated_at_ms', string.format('%.0f', now))
    redis.call('ZADD', KEYS[3], ARGV[oi + 3], ARGV[oi + 4])
    redis.call('HINCRBY', KEYS[5], 'pending_work_count', 1)
    redis.call('HINCRBY', KEYS[5], 'pending_work_bytes', #payload)
  end
end
for j = 1, S do
  local si = sbase + 8 * (j - 1)
  local schedule_key = KEYS[5 + N + j]
  local due = tonumber(ARGV[si + 3])
  local generation = tonumber(ARGV[si + 4])
  local state = 'SUPERSEDED'
  local completed_at = string.format('%.0f', now)
  local work_version = 1
  local created_at = string.format('%.0f', now)
  local exists = redis.call('EXISTS', schedule_key) == 1
  local skip_schedule = false
  if exists then
    local cur = redis.call('HMGET', schedule_key, 'generation', 'work_version', 'created_at_ms')
    if tonumber(cur[1]) and generation <= tonumber(cur[1]) then
      skip_schedule = true
    else
      work_version = (tonumber(cur[2]) or 0) + 1
      if work_version <= 1 then work_version = 1 end
      created_at = cur[3] or created_at
    end
  end
  if not skip_schedule and due > 0 then
    state = 'PENDING'
    completed_at = '0'
  end
  if not skip_schedule then
  redis.call('HSET', schedule_key,
    'schema_version', '1',
    'schedule_id', ARGV[si],
    'schedule_type', ARGV[si + 1],
    'work_id', ARGV[si],
    'work_type', ARGV[si + 1],
    'placement_kind', 'SOURCE_PERSISTENCE_BUCKET',
    'bucket_version', '1',
    'bucket_id', ARGV[12],
    'source_authority_kind', source_kind,
    'canonical_source_authority_id', ARGV[11],
    'authority_layout_version', ARGV[9],
    'actor_route_domain', ARGV[10],
    'subject_player_id', ARGV[si + 7],
    'operation_id', ARGV[18],
    'payload_hash', ARGV[si + 6],
    'generation', ARGV[si + 4],
    'state', state,
    'work_version', tostring(work_version),
    'current_step', '',
    'typed_payload', ARGV[si + 2],
    'next_due_at_ms', ARGV[si + 3],
    'retry_count', '0',
    'last_error_class', '',
    'deadline_at_ms', '0',
    'max_retry_count', '0',
    'reconcile_required_at_ms', '0',
    'lease_holder_id', '',
    'lease_holder_boot_id', '',
    'lease_epoch', '0',
    'lease_token_hash', '',
    'lease_expire_at_ms', '0',
    'created_at_ms', created_at,
    'updated_at_ms', string.format('%.0f', now),
    'completed_at_ms', completed_at)
  if due > 0 then
    if schedule_index_mode ~= 2 then
      redis.call('ZADD', KEYS[3], ARGV[si + 3], ARGV[si + 5])
    else
      redis.call('ZREM', KEYS[3], ARGV[si + 5])
    end
    if schedule_due_key then
      redis.call('ZADD', schedule_due_key, ARGV[si + 3], ARGV[si + 5])
    end
  else
    redis.call('ZREM', KEYS[3], ARGV[si + 5])
    if schedule_due_key then
      redis.call('ZREM', schedule_due_key, ARGV[si + 5])
    end
  end
  end
end
if schedule_pending_delta ~= 0 then
  redis.call('HINCRBY', KEYS[5], 'pending_work_count', schedule_pending_delta)
end
if schedule_bytes_delta ~= 0 then
  redis.call('HINCRBY', KEYS[5], 'pending_work_bytes', schedule_bytes_delta)
end
if new_schedules ~= 0 then
  redis.call('HINCRBY', KEYS[5], 'schedule_count', new_schedules)
end
redis.call('ZADD', KEYS[4], string.format('%.0f', now + retention_ms), ARGV[25])
redis.call('HINCRBY', KEYS[5], 'receipt_due_count', 1)

return {'COMMITTED', tostring(tonumber(ARGV[8]) + 1), tostring(model_bytes),
  tostring(receipt_count + 1), tostring(receipt_bytes + #receipt_value)}
`

// CommitActorMutationWithOrdersV13 keeps V12 registered and immutable while
// specializing only the exact audited-claim branch. Generic callers retain
// the complete receipt/hash audit and all corruption fences. V13 appends the
// exact sorted CandidateState values after V12's audited_claim argument; the
// bounded values prove that the sparse Redis transition produces the same
// model snapshot that Go validated and digested.
var luaCommitActorWithOrdersV13Src = commitActorWithOrdersV13(luaCommitActorWithOrdersV12Src)

var luaCommitActorWithOrdersV15Src = commitActorWithOrdersV15(luaCommitActorWithOrdersV13Src)

// V14 adds the exact Redis TIME from the first COMMITTED invocation for a
// sealed CommitStamp. It is registered only as the explicit exact-output
// capability; ordinary commits remain on frozen V13.
var luaCommitActorWithOrdersSrc = commitActorWithOrdersV14(luaCommitActorWithOrdersV13Src)

func commitActorWithOrdersV14(source string) string {
	return replaceFrozenScriptBlock(source, `return {'COMMITTED', tostring(tonumber(ARGV[8]) + 1), tostring(model_bytes),
  tostring(receipt_count + 1), tostring(receipt_bytes + #receipt_value)}`,
		`return {'COMMITTED', tostring(tonumber(ARGV[8]) + 1), tostring(model_bytes),
  tostring(receipt_count + 1), tostring(receipt_bytes + #receipt_value), tostring(now)}`,
		"CommitActorMutationWithOrdersV14 trusted commit time")
}

// commitActorWithOrdersV15 removes the per-model-field HGET and unchanged
// candidate re-read from the audited-claim path. LoadAndClaim already audited
// the exact WriterClaim and RecordVersion CAS plus the Commit-only mutation
// contract keeps unmodified model values byte-identical, so the audited path
// proves the sparse transition from ARGV alone. Generic commits keep the full
// current-model audit and the complete candidate comparison.
func commitActorWithOrdersV15(source string) string {
	source = replaceFrozenScriptBlock(source, `  seen_model[stored_field] = true
  -- The exact claim removes receipt-history work, never the bounded current
  -- model read. Candidate comparison below proves the resulting Farm6/Player8
  -- snapshot and rejects same-length corruption in every retained value.
  local stored_value = redis.call('HGET', KEYS[2], 'model:' .. stored_field)
  if not stored_value then
    return {'MALFORMED_RECORD'}
  end
  current_values[stored_field] = stored_value
  calculated_model_bytes = calculated_model_bytes + #stored_field + #stored_value
end
if calculated_model_bytes ~= stored_model_bytes then`, `  seen_model[stored_field] = true
  if audited_claim == '0' then
    -- Generic Commit keeps the full current-model audit. Audited claims
    -- proved this exact WriterClaim during LoadAndClaim; RecordVersion CAS
    -- plus the Commit-only mutation contract keeps unchanged values
    -- byte-identical, so no per-field HGET is done.
    local stored_value = redis.call('HGET', KEYS[2], 'model:' .. stored_field)
    if not stored_value then
      return {'MALFORMED_RECORD'}
    end
    current_values[stored_field] = stored_value
    calculated_model_bytes = calculated_model_bytes + #stored_field + #stored_value
  end
end
if audited_claim == '0' and calculated_model_bytes ~= stored_model_bytes then`, "CommitActorMutationWithOrdersV15 audited model read skip")

	source = replaceFrozenScriptBlock(source, `local resulting_values = {}
  if legacy_replace == '1' then`, `local resulting_values = {}
if audited_claim == '1' then
  -- Audited claims carry the already-validated total; no model read.
  -- resulting_values only needs the upserted fields: unchanged fields are
  -- proven by LoadAndClaim plus RecordVersion CAS and are not re-read.
  model_bytes = resulting_total_bytes
  resulting_count = #new_fields
  for field, value in pairs(upsert_values) do
    resulting_values[field] = value
  end
elseif legacy_replace == '1' then`, "CommitActorMutationWithOrdersV15 audited model bytes")

	source = replaceFrozenScriptBlock(source, `  if #new_fields ~= resulting_count then return {'CONTRACT_ERROR'} end
  for _, field in ipairs(new_fields) do
    local value = resulting_values[field]
    if value == nil then return {'CONTRACT_ERROR'} end
    model_bytes = model_bytes + #field + #value
  end`, `  if audited_claim == '0' and #new_fields ~= resulting_count then return {'CONTRACT_ERROR'} end
  if audited_claim == '0' then
    for _, field in ipairs(new_fields) do
      local value = resulting_values[field]
      if value == nil then return {'CONTRACT_ERROR'} end
      model_bytes = model_bytes + #field + #value
    end
  end`, "CommitActorMutationWithOrdersV15 audited manifest count")

	source = replaceFrozenScriptBlock(source, `for i, field in ipairs(new_fields) do
  local actual_value = resulting_values[field]
  if actual_value == nil or actual_value ~= ARGV[candidate_base + i - 1] then
    return {'MALFORMED_RECORD'}
  end
end`, `for i, field in ipairs(new_fields) do
  local actual_value = resulting_values[field]
  if upserts[field] then
    -- Upserted values are proven against the exact candidate.
    if actual_value == nil or actual_value ~= ARGV[candidate_base + i - 1] then
      return {'MALFORMED_RECORD'}
    end
  elseif audited_claim == '1' then
    -- Unchanged field on an audited claim: proven by LoadAndClaim + CAS.
  elseif actual_value == nil or actual_value ~= ARGV[candidate_base + i - 1] then
    return {'MALFORMED_RECORD'}
  end
end`, "CommitActorMutationWithOrdersV15 audited candidate values")

	return source
}

func commitActorWithOrdersV13(source string) string {
	source = replaceFrozenScriptBlock(source, `local lpos = mpos + 7
if schedule_index_mode > 0 then`, `local lpos = mpos + 7
local audited_claim = ARGV[lpos + 12]
local candidate_count = tonumber(ARGV[lpos + 13])
local candidate_base = lpos + 14
if schedule_index_mode > 0 then`, "CommitActorMutationWithOrdersV13 audited arguments")

	source = replaceFrozenScriptBlock(source, `if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'WRITER_EPOCH_MISMATCH'}
end
local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if sh[1] ~= ARGV[9] then`, `local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if not sh[1] then
  return {'WRITER_EPOCH_MISMATCH'}
end
if sh[1] ~= ARGV[9] then`, "CommitActorMutationWithOrdersV13 shard read")

	source = replaceFrozenScriptBlock(source, `if redis.call('EXISTS', KEYS[2]) == 0 then
  return {'ACTOR_NOT_FOUND'}
end
local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:record_version',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:model_field_count',
  '_meta:model_total_bytes', '_meta:model_manifest',
  '_meta:resolution_count', '_meta:schema_version',
  '_meta:model_layout_version')
if meta[1] ~= ARGV[7] then
  return {'WRITER_CLAIM_MISMATCH', meta[2]}
end

local receipt_field = 'receipt:op:' .. ARGV[13]
local existing = redis.call('HGET', KEYS[2], receipt_field)`, `local receipt_field = 'receipt:op:' .. ARGV[13]
local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:record_version',
  '_meta:receipt_count', '_meta:receipt_bytes',
  '_meta:receipt_manifest', '_meta:model_field_count',
  '_meta:model_total_bytes', '_meta:model_manifest',
  '_meta:resolution_count', '_meta:schema_version',
  '_meta:model_layout_version', '_meta:business_state_digest', receipt_field)
local actor_missing = true
for i = 1, 12 do
  if meta[i] then actor_missing = false break end
end
if actor_missing then
  -- Preserve the old NOT_FOUND/corrupt distinction without paying EXISTS on
  -- a valid actor. Unknown-only hashes remain fail-closed.
  if redis.call('EXISTS', KEYS[2]) == 0 then return {'ACTOR_NOT_FOUND'} end
  return {'MALFORMED_RECORD'}
end
if meta[1] ~= ARGV[7] then
  return {'WRITER_CLAIM_MISMATCH', meta[2]}
end

local existing = meta[13]`, "CommitActorMutationWithOrdersV13 actor and receipt read")

	source = replaceFrozenScriptBlock(source, `local audited_claim = ARGV[lpos + 12]

if max_fields == nil`, `
if max_fields == nil`, "CommitActorMutationWithOrdersV13 audited argument move")

	source = replaceFrozenScriptBlock(source, `if (meta[5] and #meta[5] > max_receipts * 68) or
   (meta[8] and #meta[8] > max_fields * 68) then
  return {'MALFORMED_RECORD'}
end
local receipt_fields = lp_fields(meta[5] or '')
local model_fields = lp_fields(meta[8] or '')`, `if (meta[5] and #meta[5] > max_receipts * 68) or
   (meta[8] and #meta[8] > max_fields * 68) then
  return {'MALFORMED_RECORD'}
end
local receipt_fields = {}
if audited_claim == '0' then receipt_fields = lp_fields(meta[5] or '') end
local model_fields = lp_fields(meta[8] or '')`, "CommitActorMutationWithOrdersV13 receipt manifest parsing")

	source = replaceFrozenScriptBlock(source, `   not resolution_count or resolution_count < 0 or resolution_count % 1 ~= 0 or
   not meta[5] or not receipt_fields or #receipt_fields ~= receipt_count or
   not meta[8] or not model_fields or #model_fields ~= model_count or`, `   not resolution_count or resolution_count < 0 or resolution_count % 1 ~= 0 or
   not meta[5] or (audited_claim == '0' and (not receipt_fields or #receipt_fields ~= receipt_count)) or
   not meta[8] or not model_fields or #model_fields ~= model_count or
   not meta[12] or #meta[12] ~= 64 or not string.match(meta[12], '^[0-9a-f]+$') or`, "CommitActorMutationWithOrdersV13 model digest validation")

	source = replaceFrozenScriptBlock(source, `  seen_model[stored_field] = true
  if audited_claim == '0' then
    local stored_value = redis.call('HGET', KEYS[2], 'model:' .. stored_field)
    if not stored_value then
      return {'MALFORMED_RECORD'}
    end
    current_values[stored_field] = stored_value
    calculated_model_bytes = calculated_model_bytes + #stored_field + #stored_value
  end
end
if audited_claim == '0' and calculated_model_bytes ~= stored_model_bytes then`, `  seen_model[stored_field] = true
  -- The exact claim removes receipt-history work, never the bounded current
  -- model read. Candidate comparison below proves the resulting Farm6/Player8
  -- snapshot and rejects same-length corruption in every retained value.
  local stored_value = redis.call('HGET', KEYS[2], 'model:' .. stored_field)
  if not stored_value then
    return {'MALFORMED_RECORD'}
  end
  current_values[stored_field] = stored_value
  calculated_model_bytes = calculated_model_bytes + #stored_field + #stored_value
end
if calculated_model_bytes ~= stored_model_bytes then`, "CommitActorMutationWithOrdersV13 model value audit")

	source = replaceFrozenScriptBlock(source, `local seen_receipt = {}
local calculated_receipt_bytes = 0
for _, stored_digest in ipairs(receipt_fields) do
  if #stored_digest ~= 64 or not string.match(stored_digest, '^[0-9a-f]+$') or seen_receipt[stored_digest] then
    return {'MALFORMED_RECORD'}
  end
  seen_receipt[stored_digest] = true
  if audited_claim == '0' then
    local stored_receipt = redis.call('HGET', KEYS[2], 'receipt:op:' .. stored_digest)
    if not stored_receipt or #stored_receipt > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local stored_receipt_fields = lp_fields(string.sub(stored_receipt, 65))
    if #stored_receipt < 64 or not stored_receipt_fields or #stored_receipt_fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    calculated_receipt_bytes = calculated_receipt_bytes + #stored_receipt
  end
end
if (audited_claim == '0' and calculated_receipt_bytes ~= receipt_bytes) or
   redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + #model_fields + #receipt_fields + resolution_count then
  return {'MALFORMED_RECORD'}
end`, `if audited_claim == '0' then
  local seen_receipt = {}
  local calculated_receipt_bytes = 0
  for _, stored_digest in ipairs(receipt_fields) do
    if #stored_digest ~= 64 or not string.match(stored_digest, '^[0-9a-f]+$') or seen_receipt[stored_digest] then
      return {'MALFORMED_RECORD'}
    end
    seen_receipt[stored_digest] = true
    local stored_receipt = redis.call('HGET', KEYS[2], 'receipt:op:' .. stored_digest)
    if not stored_receipt or #stored_receipt > 1024 then
      return {'MALFORMED_RECORD'}
    end
    local stored_receipt_fields = lp_fields(string.sub(stored_receipt, 65))
    if #stored_receipt < 64 or not stored_receipt_fields or #stored_receipt_fields ~= 6 then
      return {'MALFORMED_RECORD'}
    end
    calculated_receipt_bytes = calculated_receipt_bytes + #stored_receipt
  end
  if calculated_receipt_bytes ~= receipt_bytes then
    return {'MALFORMED_RECORD'}
  end
end
-- HLEN keeps Redis command count constant after full claim audit, but HMGET
-- still materializes the monolithic meta[5] receipt manifest. Script input
-- bytes therefore remain O(receipt history); this is not strict O(1).
if redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + #model_fields + receipt_count + resolution_count then
  return {'MALFORMED_RECORD'}
end`, "CommitActorMutationWithOrdersV13 generic receipt audit")

	source = replaceFrozenScriptBlock(source, `if audited_claim == '1' then
  -- LoadAndClaim fully audited this exact WriterClaim. RecordVersion CAS plus
  -- the Commit-only mutation contract means unchanged model values cannot
  -- differ here. Validate the sparse manifest transformation without HGETing
  -- every historical field; generic Commit below retains the full audit.
  local expected_fields = {}
  if legacy_replace == '1' then
    for field, _ in pairs(upsert_values) do
      expected_fields[field] = true
      resulting_count = resulting_count + 1
    end
  else
    for field, _ in pairs(seen_model) do
      if not deletes[field] then
        expected_fields[field] = true
        resulting_count = resulting_count + 1
      end
    end
    for field, _ in pairs(deletes) do
      if not seen_model[field] then return {'CONTRACT_ERROR'} end
    end
    for field, _ in pairs(upsert_values) do
      if deletes[field] then return {'CONTRACT_ERROR'} end
      if not expected_fields[field] then
        expected_fields[field] = true
        resulting_count = resulting_count + 1
      end
    end
  end
  if #new_fields ~= resulting_count then return {'CONTRACT_ERROR'} end
  for _, field in ipairs(new_fields) do
    if not expected_fields[field] then return {'CONTRACT_ERROR'} end
  end
  model_bytes = resulting_total_bytes
else
  local resulting_values = {}`, `local resulting_values = {}`, "CommitActorMutationWithOrdersV13 common model transition")

	source = replaceFrozenScriptBlock(source, `  for _, field in ipairs(new_fields) do
    local value = resulting_values[field]
    if value == nil then return {'CONTRACT_ERROR'} end
    model_bytes = model_bytes + #field + #value
  end
end
if resulting_count > max_fields or`, `  for _, field in ipairs(new_fields) do
    local value = resulting_values[field]
    if value == nil then return {'CONTRACT_ERROR'} end
    model_bytes = model_bytes + #field + #value
  end
if resulting_count > max_fields or`, "CommitActorMutationWithOrdersV13 common model transition end")

	source = replaceFrozenScriptBlock(source, `if resulting_count > max_fields or model_bytes < 0 or model_bytes > max_bytes or model_bytes ~= resulting_total_bytes or
   ARGV[22] ~= resulting_digest or #resulting_digest ~= 64 or`, `if not candidate_count or candidate_count ~= #new_fields then
  return {'CONTRACT_ERROR'}
end
for i, field in ipairs(new_fields) do
  local actual_value = resulting_values[field]
  if actual_value == nil or actual_value ~= ARGV[candidate_base + i - 1] then
    return {'MALFORMED_RECORD'}
  end
end
if resulting_count > max_fields or model_bytes < 0 or model_bytes > max_bytes or model_bytes ~= resulting_total_bytes or
   ARGV[22] ~= resulting_digest or #resulting_digest ~= 64 or`, "CommitActorMutationWithOrdersV13 exact resulting values")

	source = replaceFrozenScriptBlock(source, `for _, field in ipairs(new_fields) do
  if upserts[field] then
    redis.call('HSET', KEYS[2], 'model:' .. field, upsert_values[field])
  end
end

redis.call('HSET', KEYS[2], receipt_field, receipt_value)
redis.call('HSET', KEYS[2],
  '_meta:record_version', tostring(tonumber(ARGV[8]) + 1),`, `local actor_hset_args = {}
for _, field in ipairs(new_fields) do
  if upserts[field] then
    actor_hset_args[#actor_hset_args + 1] = 'model:' .. field
    actor_hset_args[#actor_hset_args + 1] = upsert_values[field]
  end
end
actor_hset_args[#actor_hset_args + 1] = receipt_field
actor_hset_args[#actor_hset_args + 1] = receipt_value
local actor_meta_args = {
  '_meta:record_version', tostring(tonumber(ARGV[8]) + 1),`, "CommitActorMutationWithOrdersV13 variadic actor HSET start")

	source = replaceFrozenScriptBlock(source, `  '_meta:receipt_manifest', meta[5] .. lp1(ARGV[13]),
  '_meta:schema_version', ARGV[23],
  '_meta:limit_profile_version', ARGV[24])

local source_kind`, `  '_meta:receipt_manifest', meta[5] .. lp1(ARGV[13]),
  '_meta:schema_version', ARGV[23],
  '_meta:limit_profile_version', ARGV[24]}
for _, value in ipairs(actor_meta_args) do
  actor_hset_args[#actor_hset_args + 1] = value
end
redis.call('HSET', KEYS[2], unpack(actor_hset_args))

local source_kind`, "CommitActorMutationWithOrdersV13 variadic actor HSET end")
	return source
}

// FenceAndClaimActorV1 ARGV layout:
//
//	1 tag, 2 holder, 3 boot, 4 revision, 5 writer_epoch, 6 lease_epoch,
//	7 resolve_operation_id, 8 old_claim, 9 new_claim, 10 original_digest,
//	11 original_payload_hash, 12 original_key_identity_lp,
//	13 before_record_version, 14 before_digest,
//	15 resolve_field_suffix, 16 request_digest, 17 max_resolutions, 18 layout.
//
// VerifySourceOrderV1 returns one bounded SourceOrder admission evidence set
// with the same Redis TIME and DueIndex binding used by WorkLease claim and
// admission. It never returns typed_payload; payload_hash plus exact metadata
// bind the order. KEYS: [1]=work record, [2]=due index. ARGV: 1 tag,
// 2 work_ref, 3 work_type, 4 work_id.
var luaVerifySourceOrderSrc = luaCheckTag + luaRedisTime + `
local work = redis.call('HMGET', KEYS[1],
  'work_id','work_type','state','work_version','payload_hash','subject_player_id',
  'operation_id','next_due_at_ms','lease_holder_id','lease_holder_boot_id','lease_epoch',
  'lease_token_hash','lease_expire_at_ms','placement_kind','bucket_version','bucket_id',
  'source_authority_kind','canonical_source_authority_id','authority_layout_version','actor_route_domain')
local missing = true
for i = 1, #work do
  if work[i] then missing = false break end
end
if missing then return {'NOT_FOUND'} end
local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
local score_s = ''
if score then
  if type(score) == 'string' then score_s = score else score_s = string.format('%.0f', score) end
end
local out = {'OK'}
for i = 1, 20 do out[i + 1] = work[i] or '' end
out[22] = score_s
out[23] = tostring(now)
return out
`

// VerifySourceOrderV2 is the Worker recovery variant of VerifySourceOrderV1.
// It additionally returns retry_count and created_at_ms so the grace check
// (fresh retry=0 order within RecoveryGrace of creation) needs exactly one
// EVALSHA instead of GetSourceOrder plus a separate TIME. Reply layout is
// identical to V1 with retry_count=24 and created_at_ms=25 appended.
var luaVerifySourceOrderV2Src = luaCheckTag + luaRedisTime + `
local work = redis.call('HMGET', KEYS[1],
  'work_id','work_type','state','work_version','payload_hash','subject_player_id',
  'operation_id','next_due_at_ms','lease_holder_id','lease_holder_boot_id','lease_epoch',
  'lease_token_hash','lease_expire_at_ms','placement_kind','bucket_version','bucket_id',
  'source_authority_kind','canonical_source_authority_id','authority_layout_version','actor_route_domain',
  'retry_count','created_at_ms')
local missing = true
for i = 1, #work do
  if work[i] then missing = false break end
end
if missing then return {'NOT_FOUND'} end
local score = redis.call('ZSCORE', KEYS[2], ARGV[2])
local score_s = ''
if score then
  if type(score) == 'string' then score_s = score else score_s = string.format('%.0f', score) end
end
local out = {'OK'}
for i = 1, 22 do out[i + 1] = work[i] or '' end
out[24] = score_s
out[25] = tostring(now)
return out
`

const luaFenceAndClaimActorSrc = luaCheckTag + luaCommonLP + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'WRITER_EPOCH_MISMATCH'}
end
local sh = redis.call('HMGET', KEYS[1],
  'authority_layout_version', 'lease_state', 'lease_expire_at_ms',
  'holder_instance_id', 'holder_boot_id', 'assignment_revision',
  'writer_epoch', 'lease_epoch')
if sh[1] ~= ARGV[18] then
  return {'LAYOUT_MISMATCH'}
end
if sh[2] ~= 'ACTIVE' or tonumber(sh[3]) <= now then
  return {'WRITER_EPOCH_MISMATCH'}
end
if sh[4] ~= ARGV[2] or sh[5] ~= ARGV[3] or sh[6] ~= ARGV[4] or sh[7] ~= ARGV[5] or sh[8] ~= ARGV[6] then
  return {'WRITER_EPOCH_MISMATCH'}
end

if redis.call('EXISTS', KEYS[2]) == 0 then
  return {'ACTOR_NOT_FOUND'}
end
local resolve_field = 'receipt:resolve:' .. ARGV[15]
local existing = redis.call('HGET', KEYS[2], resolve_field)
if existing then
  if string.sub(existing, 1, 64) == ARGV[16] then
    return {'RESOLUTION_REPLAY', existing}
  end
  return {'RECONCILE_REQUIRED'}
end

local meta = redis.call('HMGET', KEYS[2],
  '_meta:writer_claim_id', '_meta:record_version',
  '_meta:business_state_digest', '_meta:resolution_count')
if meta[1] ~= ARGV[8] then
  return {'WRITER_CLAIM_MISMATCH', meta[2]}
end
if tonumber(meta[4]) + 1 > tonumber(ARGV[17]) then
  return {'CAPACITY_EXCEEDED'}
end

local status = 'RECONCILE_REQUIRED'
local out_status = ''
local out_err = ''
local out_payload = ''
local orig_receipt_id = ''
local rcpt = redis.call('HGET', KEYS[2], 'receipt:op:' .. ARGV[10])
if rcpt then
  -- The digest only locates the receipt; the stored full key identity and
  -- payload hash must both match before COMMITTED can be declared.
  local fields = lp_fields(string.sub(rcpt, 65))
  if not fields or #fields < 6 then
    return {'CONTRACT_ERROR'}
  end
  if string.sub(rcpt, 1, 64) == ARGV[11] and fields[1] == ARGV[12] then
    status = 'COMMITTED'
    orig_receipt_id = fields[2]
    out_status = fields[3]
    out_err = fields[4]
    out_payload = fields[5]
  end
else
  if meta[2] == ARGV[13] and meta[3] == ARGV[14] then
    status = 'NOT_COMMITTED'
  end
end

local rv = ARGV[16] .. lp1(ARGV[7]) .. lp1(ARGV[8]) .. lp1(ARGV[9]) ..
  lp1(ARGV[10]) .. lp1(ARGV[11]) .. lp1(ARGV[12]) .. lp1(ARGV[13]) .. lp1(ARGV[14]) ..
  lp1(meta[2]) .. lp1(meta[3]) .. lp1(status) .. lp1(out_status) ..
  lp1(out_err) .. lp1(out_payload) .. lp1(orig_receipt_id) ..
  lp1(string.format('%.0f', now))
redis.call('HSET', KEYS[2], resolve_field, rv,
  '_meta:writer_claim_id', ARGV[9],
  '_meta:writer_epoch', sh[7],
  '_meta:resolution_count', tostring(tonumber(meta[4]) + 1),
  '_meta:updated_at_ms', string.format('%.0f', now))
return {status, meta[2], meta[3], out_status, out_err, out_payload, orig_receipt_id}
`

// ClaimWorkV2 ARGV: 1 tag, 2 holder, 3 boot, 4 token_hash,
// 5 requested_ttl_ms, 6 min_ttl_ms, 7 max_ttl_ms, 8 work_ref.
// KEYS: work record, due index. Care orders remain in the durable
// REWARD_DISPATCHING state across lease loss; the lease tuple is the fence.
const luaClaimWorkV2Src = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'state', 'lease_expire_at_ms', 'next_due_at_ms', 'lease_epoch',
  'work_version', 'typed_payload', 'work_id', 'work_type',
  'subject_player_id', 'operation_id', 'retry_count', 'payload_hash')
if v[1] ~= 'PENDING' and v[1] ~= 'REWARD_DISPATCHING' then
  return {'NOT_CLAIMABLE', v[1]}
end
if tonumber(v[2]) > now then
  return {'LEASE_HELD', v[2]}
end
if tonumber(v[3]) > now then
  return {'NOT_DUE', v[3]}
end
local ttl = tonumber(ARGV[5])
local min_ttl = tonumber(ARGV[6])
local max_ttl = tonumber(ARGV[7])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
local epoch = tonumber(v[4]) + 1
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[2],
  'lease_holder_boot_id', ARGV[3],
  'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[4],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'state', 'REWARD_DISPATCHING',
  'next_due_at_ms', string.format('%.0f', expire),
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', expire), ARGV[8])
return {'CLAIMED', tostring(epoch), v[5], string.format('%.0f', expire),
  tostring(ttl), v[6], v[7], v[8], v[9], v[10], v[11], v[12]}
`

// RenewWorkLeaseV2 ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch,
// 5 token_hash, 6 requested_ttl_ms, 7 min_ttl_ms, 8 max_ttl_ms, 9 work_ref.
const luaRenewWorkLeaseSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch', 'lease_token_hash', 'lease_expire_at_ms')
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[3] ~= ARGV[4] or v[4] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[5]) <= now then
  return {'LEASE_EXPIRED'}
end
local ttl = tonumber(ARGV[6])
local min_ttl = tonumber(ARGV[7])
local max_ttl = tonumber(ARGV[8])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
redis.call('HSET', KEYS[1],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'next_due_at_ms', string.format('%.0f', expire),
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', expire), ARGV[9])
return {'RENEWED', string.format('%.0f', expire), tostring(ttl)}
`

// CompleteWorkV3 ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch,
// 5 token_hash, 6 work_version, 7 work_ref, 8 work_type, 9 work_id,
// 10 source_kind, 11 source_id, 12 authority_layout, 13 route_domain,
// 14 payload_hash, 15 invocation, 16 participant_step, 17 outcome,
// 18 participant_result_hash, 19 participant_result_version, 20 proof_hash.
// KEYS: work record, due index, bucket-meta. All result proof fields are
// precomputed bounded scalars; Lua performs only fence/identity checks.
const luaCompleteWorkV2Src = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch', 'lease_token_hash',
  'lease_expire_at_ms', 'work_version', 'state', 'work_id', 'work_type',
  'source_authority_kind', 'canonical_source_authority_id',
  'authority_layout_version', 'actor_route_domain', 'payload_hash')
local function valid_hash(value)
  return value and #value == 64 and string.match(value, '^[0-9a-f]+$') ~= nil
end
if v[8] ~= ARGV[9] or v[9] ~= ARGV[8] or v[10] ~= ARGV[10] or
   v[11] ~= ARGV[11] or v[12] ~= ARGV[12] or v[13] ~= ARGV[13] or
   v[14] ~= ARGV[14] then
  return {'FENCE_MISMATCH'}
end
if v[6] ~= ARGV[6] then
  return {'STALE_VERSION', v[6]}
end
if v[7] == 'COMPLETED' then
  return {'ALREADY_COMPLETED', v[7]}
end
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[3] ~= ARGV[4] or v[4] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[5]) <= now then
  return {'LEASE_EXPIRED'}
end
if v[7] ~= 'REWARD_DISPATCHING' then
  return {'FENCE_MISMATCH'}
end
local allowed_step = {
  CARE_REWARD = 'APPLY_CARE_REWARD',
  HARVEST = 'GRANT_HARVEST_REWARD',
  PLANT = 'COMMIT_PLANT',
  UNLOCK_PLOT = 'COMMIT_PLOT_UNLOCK',
  THEFT_VISITOR = 'SETTLE_THEFT_VISITOR',
  THEFT_GUARD = 'CREDIT_GUARD_INCOME',
}
local step = allowed_step[ARGV[8]]
if not step then
  return {'RESULT_PROOF_INVALID'}
end
if ARGV[16] ~= step or
   (ARGV[17] ~= 'APPLIED' and ARGV[17] ~= 'NO_CHANGE') or
   not valid_hash(ARGV[14]) or not valid_hash(ARGV[18]) or
   not tonumber(ARGV[19]) or tonumber(ARGV[19]) <= 0 or not valid_hash(ARGV[20]) or
   ARGV[15] == '' then
  return {'RESULT_PROOF_INVALID'}
end
redis.call('HSET', KEYS[1],
  'state', 'COMPLETED',
  'current_step', ARGV[16],
  'next_due_at_ms', '0',
  'completed_at_ms', string.format('%.0f', now),
  'updated_at_ms', string.format('%.0f', now))
redis.call('HDEL', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms')
redis.call('ZREM', KEYS[2], ARGV[7])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
return {'COMPLETED'}
`

// ClaimAndCompleteWorkV1 ARGV: 1 tag, 2 holder, 3 boot, 4 token_hash,
// 5 requested_ttl_ms, 6 min_ttl_ms, 7 max_ttl_ms, 8 work_ref,
// 9 work_type, 10 work_id, 11 source_kind, 12 source_id,
// 13 authority_layout, 14 route_domain, 15 payload_hash, 16 invocation,
// 17 participant_step, 18 outcome, 19 participant_result_hash,
// 20 participant_result_version, 21 proof_hash.
// KEYS: work record, due index, bucket-meta.
// Direct post-proof path: claim an unowned PENDING/expired order and complete
// it in one EVALSHA. A live lease held by another worker stays LEASE_HELD so
// recovery still owns the order. Fence and result-proof checks match CompleteWorkV3.
const luaClaimAndCompleteWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch', 'lease_token_hash',
  'lease_expire_at_ms', 'work_version', 'state', 'work_id', 'work_type',
  'source_authority_kind', 'canonical_source_authority_id',
  'authority_layout_version', 'actor_route_domain', 'payload_hash',
  'next_due_at_ms', 'retry_count', 'deadline_at_ms', 'max_retry_count')
local function valid_hash(value)
  return value and #value == 64 and string.match(value, '^[0-9a-f]+$') ~= nil
end
if v[8] ~= ARGV[10] or v[9] ~= ARGV[9] then
  return {'FENCE_MISMATCH'}
end
if v[7] == 'COMPLETED' then
  return {'ALREADY_COMPLETED', v[7]}
end
if v[10] ~= ARGV[11] or v[11] ~= ARGV[12] or v[12] ~= ARGV[13] or
   v[13] ~= ARGV[14] or v[14] ~= ARGV[15] then
  return {'FENCE_MISMATCH'}
end
local allowed_step = {
  CARE_REWARD = 'APPLY_CARE_REWARD',
  HARVEST = 'GRANT_HARVEST_REWARD',
  PLANT = 'COMMIT_PLANT',
  UNLOCK_PLOT = 'COMMIT_PLOT_UNLOCK',
  THEFT_VISITOR = 'SETTLE_THEFT_VISITOR',
  THEFT_GUARD = 'CREDIT_GUARD_INCOME',
}
local step = allowed_step[ARGV[9]]
if not step then
  return {'RESULT_PROOF_INVALID'}
end
if ARGV[17] ~= step or
   (ARGV[18] ~= 'APPLIED' and ARGV[18] ~= 'NO_CHANGE') or
   not valid_hash(ARGV[15]) or not valid_hash(ARGV[19]) or
   not tonumber(ARGV[20]) or tonumber(ARGV[20]) <= 0 or not valid_hash(ARGV[21]) or
   ARGV[16] == '' then
  return {'RESULT_PROOF_INVALID'}
end
if v[7] ~= 'PENDING' and v[7] ~= 'REWARD_DISPATCHING' then
  return {'NOT_CLAIMABLE', v[7]}
end
local expire_at = tonumber(v[5]) or 0
local held = expire_at > now
if held and (v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[4] ~= ARGV[4]) then
  return {'LEASE_HELD', v[5]}
end
if tonumber(v[15]) > now then
  return {'NOT_DUE', v[15]}
end
local deadline = tonumber(v[17]) or 0
local max_retry = tonumber(v[18]) or 0
local retry = tonumber(v[16]) or 0
if (deadline > 0 and now >= deadline) or (max_retry > 0 and retry >= max_retry) then
  redis.call('HSET', KEYS[1],
    'state', 'RECONCILE_REQUIRED',
    'reconcile_required_at_ms', string.format('%.0f', now),
    'updated_at_ms', string.format('%.0f', now),
    'next_due_at_ms', '0',
    'lease_holder_id', '',
    'lease_holder_boot_id', '',
    'lease_token_hash', '',
    'lease_expire_at_ms', '0')
  redis.call('ZREM', KEYS[2], ARGV[8])
  return {'RECONCILE_REQUIRED', tostring(retry)}
end
if not held then
  local ttl = tonumber(ARGV[5])
  local min_ttl = tonumber(ARGV[6])
  local max_ttl = tonumber(ARGV[7])
  if ttl < min_ttl then ttl = min_ttl end
  if ttl > max_ttl then ttl = max_ttl end
  local expire = now + ttl
  local epoch = (tonumber(v[3]) or 0) + 1
  redis.call('HSET', KEYS[1],
    'lease_holder_id', ARGV[2],
    'lease_holder_boot_id', ARGV[3],
    'lease_epoch', tostring(epoch),
    'lease_token_hash', ARGV[4],
    'lease_expire_at_ms', string.format('%.0f', expire),
    'state', 'REWARD_DISPATCHING',
    'next_due_at_ms', string.format('%.0f', expire),
    'updated_at_ms', string.format('%.0f', now))
  redis.call('ZADD', KEYS[2], string.format('%.0f', expire), ARGV[8])
end
redis.call('HSET', KEYS[1],
  'state', 'COMPLETED',
  'current_step', ARGV[17],
  'next_due_at_ms', '0',
  'completed_at_ms', string.format('%.0f', now),
  'updated_at_ms', string.format('%.0f', now))
redis.call('HDEL', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms')
redis.call('ZREM', KEYS[2], ARGV[8])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
return {'COMPLETED'}
`

// ClaimAndCompleteWorkV2 keeps the frozen V1 script available for old
// callers, but removes the deadline/max-retry rejection from the direct
// post-proof path. A valid Participant proof is already the authoritative
// terminal fact; a late proof must complete the order rather than turn a
// successful mutation into RECONCILE_REQUIRED. The recovery path still
// enforces deadline/max-retry before it invokes a participant.
var luaClaimAndCompleteWorkV2Src = claimAndCompleteWithoutDeadline(luaClaimAndCompleteWorkSrc)

// V3 keeps V2's late-proof behavior and removes the transient lease HSET/ZADD
// immediately followed by completion in the same atomic Lua call. A live
// lease held by another owner still returns LEASE_HELD; an unleased PENDING
// order with a valid Participant proof can linearize directly to COMPLETED.
var luaClaimAndCompleteWorkV3Src = completeDirectWithoutTransientClaim(luaClaimAndCompleteWorkV2Src)

func completeDirectWithoutTransientClaim(source string) string {
	const startMarker = "\nif not held then"
	const endMarker = "\nredis.call('HSET', KEYS[1],\n  'state', 'COMPLETED'"
	start := strings.Index(source, startMarker)
	if start < 0 {
		panic("ClaimAndCompleteWorkV2 transient claim block missing")
	}
	relativeEnd := strings.Index(source[start+len(startMarker):], endMarker)
	if relativeEnd < 0 {
		panic("ClaimAndCompleteWorkV2 completion block missing")
	}
	end := start + len(startMarker) + relativeEnd
	return source[:start] + source[end:]
}

func claimAndCompleteWithoutDeadline(source string) string {
	const startMarker = "\nlocal deadline = tonumber(v[17]) or 0"
	const endMarker = "\nif not held then"
	start := strings.Index(source, startMarker)
	if start < 0 {
		panic("ClaimAndCompleteWorkV1 deadline block missing")
	}
	relativeEnd := strings.Index(source[start+len(startMarker):], endMarker)
	if relativeEnd < 0 {
		panic("ClaimAndCompleteWorkV1 claim block missing")
	}
	end := start + len(startMarker) + relativeEnd
	return source[:start] + source[end:]
}

// CompleteWorkV4 adds the terminal outcomes used by the SourceOrder-backed
// mail delivery/projection participants. V3 remains immutable for existing
// callers; the new version avoids silently changing its proof contract.
var luaCompleteWorkV4Src = withMailCompletionSemantics(luaCompleteWorkV2Src)

func withMailCompletionSemantics(source string) string {
	source = strings.Replace(source,
		"  THEFT_GUARD = 'CREDIT_GUARD_INCOME',\n",
		"  THEFT_GUARD = 'CREDIT_GUARD_INCOME',\n"+
			"  MAIL_DELIVERY_ORDER = 'RESUME_MAIL_DELIVERY_ORDER',\n"+
			"  MAIL_CLAIM_PROJECTION_ORDER = 'APPLY_MAIL_CLAIM_PROJECTION',\n", 1)
	source = strings.Replace(source,
		"(ARGV[17] ~= 'APPLIED' and ARGV[17] ~= 'NO_CHANGE')",
		"(ARGV[17] ~= 'APPLIED' and ARGV[17] ~= 'NO_CHANGE' and\n"+
			"    ARGV[17] ~= 'INSERTED' and ARGV[17] ~= 'ALREADY_DELIVERED' and\n"+
			"    ARGV[17] ~= 'TARGET_ALREADY_PURGED')", 1)
	return source
}

// V5 charges terminal completion against both bucket counters using the
// immutable typed_payload bytes stored on the SourceOrder. V4 remains frozen,
// but rolling V4 completions require the bounded/final reconciliation below.
var luaCompleteWorkV5Src = withSourceOrderTerminalAccounting(
	luaCompleteWorkV4Src,
	"  'authority_layout_version', 'actor_route_domain', 'payload_hash')",
	"  'authority_layout_version', 'actor_route_domain', 'payload_hash', 'typed_payload')",
	"\nredis.call('HSET', KEYS[1],\n  'state', 'COMPLETED'",
	"15",
)

// ClaimAndCompleteWorkV4 applies the same exact terminal accounting to V3's
// mutation-free direct-completion path. Older SHAs remain registered and
// byte-frozen, but an old worker can still leave pending_work_bytes charged;
// rolling deployments must run the bounded bucket reconciliation below.
var luaClaimAndCompleteWorkV4Src = withSourceOrderTerminalAccounting(
	luaClaimAndCompleteWorkV3Src,
	"  'next_due_at_ms', 'retry_count', 'deadline_at_ms', 'max_retry_count')",
	"  'next_due_at_ms', 'retry_count', 'deadline_at_ms', 'max_retry_count', 'typed_payload')",
	"\nredis.call('HSET', KEYS[1],\n  'state', 'COMPLETED'",
	"19",
)

func withSourceOrderTerminalAccounting(source, oldFields, newFields, mutationMarker, payloadIndex string) string {
	if strings.Count(source, oldFields) != 1 || strings.Count(source, mutationMarker) != 1 {
		panic("SourceOrder terminal accounting insertion point missing")
	}
	source = strings.Replace(source, oldFields, newFields, 1)
	validation := `
local function valid_nonnegative_counter(value)
  if not value or string.match(value, '^%d+$') == nil then return false end
  if #value > 1 and string.sub(value, 1, 1) == '0' then return false end
  if #value > 19 or (#value == 19 and value > '9223372036854775807') then return false end
  return true
end
local function counter_covers(value, required)
  local minimum = tostring(required)
  if #value ~= #minimum then return #value > #minimum end
  return value >= minimum
end
local typed_payload = v[` + payloadIndex + `]
local pending_count = redis.call('HGET', KEYS[3], 'pending_work_count')
local pending_bytes = redis.call('HGET', KEYS[3], 'pending_work_bytes')
local due_index_type = redis.call('TYPE', KEYS[2])
if type(due_index_type) == 'table' then due_index_type = due_index_type['ok'] end
if not typed_payload or #typed_payload > 16384 or
   (due_index_type ~= 'none' and due_index_type ~= 'zset') or
   not valid_nonnegative_counter(pending_count) or pending_count == '0' or
   not valid_nonnegative_counter(pending_bytes) or not counter_covers(pending_bytes, #typed_payload) then
  return {'MALFORMED_RECORD'}
end
`
	source = strings.Replace(source, mutationMarker, validation+mutationMarker, 1)
	const countDecrement = "redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)"
	if strings.Count(source, countDecrement) != 1 {
		panic("SourceOrder terminal count decrement missing")
	}
	return strings.Replace(source, countDecrement,
		countDecrement+"\nredis.call('HINCRBY', KEYS[3], 'pending_work_bytes', -#typed_payload)", 1)
}

const luaClaimWorkV1Src = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'state', 'lease_expire_at_ms', 'next_due_at_ms', 'lease_epoch',
  'work_version', 'typed_payload', 'work_id', 'work_type',
  'subject_player_id', 'operation_id', 'retry_count', 'payload_hash')
if v[1] ~= 'PENDING' then
  return {'NOT_CLAIMABLE', v[1]}
end
if tonumber(v[2]) > now then
  return {'LEASE_HELD', v[2]}
end
if tonumber(v[3]) > now then
  return {'NOT_DUE', v[3]}
end
local ttl = tonumber(ARGV[4])
local min_ttl = tonumber(ARGV[5])
local max_ttl = tonumber(ARGV[6])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
local epoch = tonumber(v[4]) + 1
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[2],
  'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[3],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', expire), ARGV[7])
return {'CLAIMED', tostring(epoch), v[5], string.format('%.0f', expire),
  tostring(ttl), v[6], v[7], v[8], v[9], v[10], v[11], v[12]}
`

const luaCompleteWorkV1Src = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'lease_holder_id', 'lease_epoch', 'lease_token_hash', 'work_version', 'state')
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[3] ~= ARGV[4] then
  return {'FENCE_MISMATCH'}
end
if v[4] ~= ARGV[5] then
  return {'STALE_VERSION', v[4]}
end
if v[5] ~= 'PENDING' then
  return {'ALREADY_COMPLETED', v[5]}
end
redis.call('HSET', KEYS[1],
  'state', 'COMPLETED',
  'completed_at_ms', string.format('%.0f', now),
  'updated_at_ms', string.format('%.0f', now),
  'lease_expire_at_ms', '0')
redis.call('ZREM', KEYS[2], ARGV[6])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
return {'COMPLETED'}
`

// ClaimWorkV4 ARGV: 1 tag, 2 holder, 3 boot, 4 token_hash,
// 5 requested_ttl_ms, 6 min_ttl_ms, 7 max_ttl_ms, 8 work_ref.
// KEYS: work record, due index. Extends V3: missing idle lease fields on a
// PENDING order are treated as expire=0 / epoch=0 so create no longer HSETs
// empty lease tuples. Deadline and max_retry fences are unchanged.
const luaClaimWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'state', 'lease_expire_at_ms', 'next_due_at_ms', 'lease_epoch',
  'work_version', 'typed_payload', 'work_id', 'work_type',
  'subject_player_id', 'operation_id', 'retry_count', 'payload_hash',
  'deadline_at_ms', 'max_retry_count')
if v[1] ~= 'PENDING' and v[1] ~= 'REWARD_DISPATCHING' then
  return {'NOT_CLAIMABLE', v[1]}
end
if (tonumber(v[2]) or 0) > now then
  return {'LEASE_HELD', v[2]}
end
if tonumber(v[3]) > now then
  return {'NOT_DUE', v[3]}
end
local deadline = tonumber(v[13]) or 0
local max_retry = tonumber(v[14]) or 0
local retry = tonumber(v[11]) or 0
if (deadline > 0 and now >= deadline) or (max_retry > 0 and retry >= max_retry) then
  redis.call('HSET', KEYS[1],
    'state', 'RECONCILE_REQUIRED',
    'reconcile_required_at_ms', string.format('%.0f', now),
    'updated_at_ms', string.format('%.0f', now),
    'next_due_at_ms', '0',
    'lease_holder_id', '',
    'lease_holder_boot_id', '',
    'lease_token_hash', '',
    'lease_expire_at_ms', '0')
  redis.call('ZREM', KEYS[2], ARGV[8])
  return {'RECONCILE_REQUIRED', tostring(retry)}
end
local ttl = tonumber(ARGV[5])
local min_ttl = tonumber(ARGV[6])
local max_ttl = tonumber(ARGV[7])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
local epoch = (tonumber(v[4]) or 0) + 1
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[2],
  'lease_holder_boot_id', ARGV[3],
  'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[4],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'state', 'REWARD_DISPATCHING',
  'next_due_at_ms', string.format('%.0f', expire),
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', expire), ARGV[8])
return {'CLAIMED', tostring(epoch), v[5], string.format('%.0f', expire),
  tostring(ttl), v[6], v[7], v[8], v[9], v[10], v[11], v[12]}
`

// ClaimWorkV5 is a new frozen successor to V4. On the deadline/max-retry
// terminal branch it validates both bucket counters and the exact DueIndex
// member/score before any write, then releases one count and the stored
// typed_payload byte length. V4 remains byte-for-byte registered because
// mixed binaries can still reference its SHA; a final reconciliation is
// required after all V4 workers drain.
var luaClaimWorkV5Src = withClaimWorkTerminalAccounting(luaClaimWorkSrc)

func withClaimWorkTerminalAccounting(source string) string {
	const mutationMarker = "\n  redis.call('HSET', KEYS[1],\n    'state', 'RECONCILE_REQUIRED'"
	if strings.Count(source, mutationMarker) != 1 {
		panic("ClaimWorkV4 terminal insertion point missing")
	}
	const validation = `
local function valid_nonnegative_counter_v5(value)
  if not value or string.match(value, '^%d+$') == nil then return false end
  if #value > 1 and string.sub(value, 1, 1) == '0' then return false end
  if #value > 19 or (#value == 19 and value > '9223372036854775807') then return false end
  return true
end
local function counter_covers_v5(value, required)
  local minimum = tostring(required)
  if #value ~= #minimum then return #value > #minimum end
  return value >= minimum
end
local function exact_integer_score_v5(value, expected)
  if not valid_nonnegative_counter_v5(expected) or
     (#expected > 16 or (#expected == 16 and expected > '9007199254740991')) then return false end
  return value and string.format('%.0f', tonumber(value)) == expected
end
local typed_payload_v5 = v[6]
local pending_count_v5 = redis.call('HGET', KEYS[3], 'pending_work_count')
local pending_bytes_v5 = redis.call('HGET', KEYS[3], 'pending_work_bytes')
local due_index_type_v5 = redis.call('TYPE', KEYS[2])
if type(due_index_type_v5) == 'table' then due_index_type_v5 = due_index_type_v5['ok'] end
local due_score_v5 = false
if due_index_type_v5 == 'zset' then
  due_score_v5 = redis.call('ZSCORE', KEYS[2], ARGV[8])
end
if not typed_payload_v5 or #typed_payload_v5 > 16384 or
   due_index_type_v5 ~= 'zset' or not exact_integer_score_v5(due_score_v5, v[3]) or
   not valid_nonnegative_counter_v5(pending_count_v5) or pending_count_v5 == '0' or
   not valid_nonnegative_counter_v5(pending_bytes_v5) or
   not counter_covers_v5(pending_bytes_v5, #typed_payload_v5) then
  return {'MALFORMED_RECORD'}
end
`
	source = strings.Replace(source, mutationMarker, validation+mutationMarker, 1)
	const terminalTail = "  redis.call('ZREM', KEYS[2], ARGV[8])\n  return {'RECONCILE_REQUIRED', tostring(retry)}"
	if strings.Count(source, terminalTail) != 1 {
		panic("ClaimWorkV4 terminal accounting point missing")
	}
	return strings.Replace(source, terminalTail,
		"  redis.call('ZREM', KEYS[2], ARGV[8])\n"+
			"  redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)\n"+
			"  redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', -#typed_payload_v5)\n"+
			"  return {'RECONCILE_REQUIRED', tostring(retry)}", 1)
}

// RescheduleWorkV1 ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch,
// 5 token_hash, 6 work_version, 7 work_ref, 8 retry_count,
// 9 requested_delay_ms, 10 error_class.  Redis TIME owns both the due score
// and the deadline decision; the worker supplies only a bounded delay/class.
const luaRescheduleWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch', 'lease_token_hash',
  'lease_expire_at_ms', 'work_version', 'state', 'deadline_at_ms', 'max_retry_count')
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[3] ~= ARGV[4] or v[4] ~= ARGV[5] or v[6] ~= ARGV[6] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[5]) <= now then
  return {'LEASE_EXPIRED'}
end
if v[7] ~= 'REWARD_DISPATCHING' then
  return {'FENCE_MISMATCH'}
end
local attempt = tonumber(ARGV[8])
local delay = tonumber(ARGV[9])
if attempt < 1 then attempt = 1 end
if delay < 1 then delay = 1 end
	local deadline = tonumber(v[8]) or 0
	local max_retry = tonumber(v[9]) or 0
local next_due = now + delay
if (max_retry > 0 and attempt >= max_retry) or (deadline > 0 and next_due >= deadline) then
  redis.call('HSET', KEYS[1],
    'state', 'RECONCILE_REQUIRED', 'retry_count', tostring(attempt),
    'last_error_class', ARGV[10], 'next_due_at_ms', '0',
    'reconcile_required_at_ms', string.format('%.0f', now),
    'lease_holder_id', '', 'lease_holder_boot_id', '', 'lease_token_hash', '',
    'lease_expire_at_ms', '0', 'updated_at_ms', string.format('%.0f', now))
  redis.call('ZREM', KEYS[2], ARGV[7])
  redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
  return {'RECONCILE_REQUIRED', tostring(attempt)}
end
redis.call('HSET', KEYS[1],
  'retry_count', tostring(attempt), 'last_error_class', ARGV[10],
  'next_due_at_ms', string.format('%.0f', next_due),
  'lease_holder_id', '', 'lease_holder_boot_id', '', 'lease_token_hash', '',
  'lease_expire_at_ms', '0', 'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', next_due), ARGV[7])
return {'RESCHEDULED', tostring(attempt), string.format('%.0f', next_due)}
`

// RescheduleWorkV2 decrements both exact SourceOrder accounting counters on
// its terminal branch. V1 remains byte-frozen for SHA compatibility; it is
// not accounting-compatible and a rolling/final reconciliation must repair
// terminal records produced by V1.
var luaRescheduleWorkV2Src = withSourceOrderTerminalAccounting(
	luaRescheduleWorkSrc,
	"  'lease_expire_at_ms', 'work_version', 'state', 'deadline_at_ms', 'max_retry_count')",
	"  'lease_expire_at_ms', 'work_version', 'state', 'deadline_at_ms', 'max_retry_count', 'typed_payload')",
	"\n  redis.call('HSET', KEYS[1],\n    'state', 'RECONCILE_REQUIRED'",
	"10",
)

// ClaimScheduledWorkV1 keeps the business due point in next_due_at_ms while
// Redis TIME adjudicates only the lease. This avoids turning a source-owned
// absolute wakeup into a worker-owned relative deadline.
// ARGV: 1 tag, 2 holder, 3 boot, 4 token_hash, 5 requested_ttl_ms,
// 6 min_ttl_ms, 7 max_ttl_ms, 8 schedule_ref.
const luaClaimScheduledWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'state', 'lease_expire_at_ms', 'next_due_at_ms', 'lease_epoch',
  'work_version', 'typed_payload', 'schedule_id', 'schedule_type',
  'subject_player_id', 'operation_id', 'retry_count', 'payload_hash', 'generation')
if v[1] ~= 'PENDING' and v[1] ~= 'SCHEDULE_DISPATCHING' then
  return {'NOT_CLAIMABLE', v[1] or ''}
end
if tonumber(v[2]) == nil or tonumber(v[3]) == nil or tonumber(v[4]) == nil or tonumber(v[5]) == nil or
   tonumber(v[2]) < 0 or tonumber(v[3]) <= 0 or tonumber(v[5]) <= 0 or tonumber(v[13]) <= 0 or
   v[6] == nil or v[7] == nil or v[8] == nil or v[9] == nil or v[10] == nil or v[11] == nil or v[12] == nil or v[13] == nil then
  return {'MALFORMED_RECORD'}
end
if tonumber(v[2]) > now then
  return {'LEASE_HELD', v[2]}
end
if tonumber(v[3]) <= 0 or tonumber(v[3]) > now then
  return {'NOT_DUE', v[3]}
end
local ttl = tonumber(ARGV[5])
local min_ttl = tonumber(ARGV[6])
local max_ttl = tonumber(ARGV[7])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
local epoch = tonumber(v[4]) + 1
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[2],
  'lease_holder_boot_id', ARGV[3],
  'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[4],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'state', 'SCHEDULE_DISPATCHING',
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], v[3], ARGV[8])
return {'CLAIMED', tostring(epoch), v[5], string.format('%.0f', expire),
  tostring(ttl), v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13]}
`

// AdvanceScheduledWorkV1 replaces the payload and absolute next boundary
// only while the exact claimed schedule generation/work version is held.
// ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch, 5 token_hash,
// 6 work_version, 7 generation, 8 schedule_ref, 9 schedule_type,
// 10 schedule_id, 11 next_due_at_ms, 12 payload, 13 payload_hash,
// 14 max_pending_work_bytes.
const luaAdvanceScheduledWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'schedule_type', 'schedule_id', 'typed_payload', 'generation', 'work_version',
  'state', 'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms')
local generation = tonumber(v[4])
local work_version = tonumber(v[5])
if v[1] ~= ARGV[9] or v[2] ~= ARGV[10] or generation == nil or work_version == nil then
  return {'FENCE_MISMATCH'}
end
if generation ~= tonumber(ARGV[7]) then
  return {'STALE_GENERATION', tostring(generation)}
end
if work_version ~= tonumber(ARGV[6]) then
  return {'STALE_VERSION', tostring(work_version)}
end
if v[6] == 'SUPERSEDED' or v[6] == 'COMPLETED' then
  return {'STALE_GENERATION', tostring(generation)}
end
if v[6] ~= 'SCHEDULE_DISPATCHING' or v[7] ~= ARGV[2] or v[8] ~= ARGV[3] or
   v[9] ~= ARGV[4] or v[10] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[11]) == nil or tonumber(v[11]) <= now then
  return {'LEASE_EXPIRED'}
end
local next_due = tonumber(ARGV[11])
if next_due == nil or next_due <= 0 or #ARGV[12] > 16384 or #ARGV[13] ~= 64 or
   not string.match(ARGV[13], '^[0-9a-f]+$') then
  return {'CONTRACT_ERROR'}
end
local pending_bytes = tonumber(redis.call('HGET', KEYS[3], 'pending_work_bytes') or '0')
if pending_bytes == nil or pending_bytes < 0 or pending_bytes + #ARGV[12] - #v[3] < 0 or
   pending_bytes + #ARGV[12] - #v[3] > tonumber(ARGV[14]) then
  return {'CAPACITY_EXCEEDED'}
end
local next_version = work_version + 1
redis.call('HSET', KEYS[1],
  'typed_payload', ARGV[12],
  'payload_hash', ARGV[13],
  'next_due_at_ms', string.format('%.0f', next_due),
  'work_version', tostring(next_version),
  'state', 'PENDING',
  'lease_holder_id', '',
  'lease_holder_boot_id', '',
  'lease_token_hash', '',
  'lease_expire_at_ms', '0',
  'completed_at_ms', '0',
  'updated_at_ms', string.format('%.0f', now))
redis.call('ZADD', KEYS[2], string.format('%.0f', next_due), ARGV[8])
local delta = #ARGV[12] - #v[3]
if delta ~= 0 then redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', delta) end
return {'ADVANCED', tostring(next_version), tostring(generation), string.format('%.0f', next_due)}
`

// RenewScheduledWorkV1 extends only the Redis TIME lease; the source-owned
// due point and generation remain untouched.
// ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch, 5 token_hash,
// 6 requested_ttl_ms, 7 min_ttl_ms, 8 max_ttl_ms.
const luaRenewScheduledWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then return {'NOT_FOUND'} end
local v = redis.call('HMGET', KEYS[1], 'lease_holder_id', 'lease_holder_boot_id',
  'lease_epoch', 'lease_token_hash', 'lease_expire_at_ms', 'state')
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] or v[3] ~= ARGV[4] or v[4] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[5]) == nil or tonumber(v[5]) <= now then return {'LEASE_EXPIRED'} end
if v[6] ~= 'SCHEDULE_DISPATCHING' then return {'FENCE_MISMATCH'} end
local ttl = tonumber(ARGV[6])
local min_ttl = tonumber(ARGV[7])
local max_ttl = tonumber(ARGV[8])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
redis.call('HSET', KEYS[1], 'lease_expire_at_ms', string.format('%.0f', expire),
  'updated_at_ms', string.format('%.0f', now))
return {'RENEWED', string.format('%.0f', expire), tostring(ttl)}
`

// CompleteScheduledWorkV1 ends a claimed schedule without inventing a next
// boundary. The bucket counters are decremented only after proving the record
// is pending-counted, so retries can never make them negative.
// ARGV: 1 tag, 2 holder, 3 boot, 4 lease_epoch, 5 token_hash,
// 6 work_version, 7 generation, 8 schedule_ref, 9 schedule_type,
// 10 schedule_id.
const luaCompleteScheduledWorkSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then return {'NOT_FOUND'} end
local v = redis.call('HMGET', KEYS[1], 'schedule_type', 'schedule_id', 'typed_payload',
  'generation', 'work_version', 'state', 'lease_holder_id', 'lease_holder_boot_id',
  'lease_epoch', 'lease_token_hash', 'lease_expire_at_ms')
local generation = tonumber(v[4])
local work_version = tonumber(v[5])
if v[1] ~= ARGV[9] or v[2] ~= ARGV[10] or generation == nil or work_version == nil then
  return {'FENCE_MISMATCH'}
end
if generation ~= tonumber(ARGV[7]) then return {'STALE_GENERATION', tostring(generation)} end
if work_version ~= tonumber(ARGV[6]) then return {'STALE_VERSION', tostring(work_version)} end
if v[6] == 'COMPLETED' or v[6] == 'SUPERSEDED' then return {'ALREADY_COMPLETED'} end
if v[6] ~= 'SCHEDULE_DISPATCHING' or v[7] ~= ARGV[2] or v[8] ~= ARGV[3] or
   v[9] ~= ARGV[4] or v[10] ~= ARGV[5] then return {'FENCE_MISMATCH'} end
if tonumber(v[11]) == nil or tonumber(v[11]) <= now then return {'LEASE_EXPIRED'} end
local pending = tonumber(redis.call('HGET', KEYS[3], 'pending_work_count') or '0')
local pending_bytes = tonumber(redis.call('HGET', KEYS[3], 'pending_work_bytes') or '0')
if pending == nil or pending <= 0 or pending_bytes == nil or pending_bytes < #v[3] then
  return {'MALFORMED_RECORD'}
end
redis.call('HSET', KEYS[1], 'state', 'COMPLETED', 'next_due_at_ms', '0',
  'completed_at_ms', string.format('%.0f', now), 'updated_at_ms', string.format('%.0f', now),
  'lease_holder_id', '', 'lease_holder_boot_id', '', 'lease_token_hash', '', 'lease_expire_at_ms', '0')
redis.call('ZREM', KEYS[2], ARGV[8])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', -#v[3])
return {'COMPLETED'}
`

// ClaimScheduledWorkDualV1 is the dual-index variant of
// ClaimScheduledWorkV1. KEYS: schedule record, shared due index, dedicated
// schedule due index. Both indexes are updated atomically in the same slot;
// authority discovery never switches before the dedicated member exists.
const luaClaimScheduledWorkDualSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'state', 'lease_expire_at_ms', 'next_due_at_ms', 'lease_epoch',
  'work_version', 'typed_payload', 'schedule_id', 'schedule_type',
  'subject_player_id', 'operation_id', 'retry_count', 'payload_hash', 'generation')
if v[1] ~= 'PENDING' and v[1] ~= 'SCHEDULE_DISPATCHING' then
  return {'NOT_CLAIMABLE', v[1] or ''}
end
if tonumber(v[2]) == nil or tonumber(v[3]) == nil or tonumber(v[4]) == nil or tonumber(v[5]) == nil or
   tonumber(v[2]) < 0 or tonumber(v[3]) <= 0 or tonumber(v[5]) <= 0 or tonumber(v[13]) <= 0 or
   v[6] == nil or v[7] == nil or v[8] == nil or v[9] == nil or v[10] == nil or v[11] == nil or v[12] == nil or v[13] == nil then
  return {'MALFORMED_RECORD'}
end
if tonumber(v[2]) > now then
  return {'LEASE_HELD', v[2]}
end
if tonumber(v[3]) <= 0 or tonumber(v[3]) > now then
  return {'NOT_DUE', v[3]}
end
local ttl = tonumber(ARGV[5])
local min_ttl = tonumber(ARGV[6])
local max_ttl = tonumber(ARGV[7])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
local epoch = tonumber(v[4]) + 1
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[2],
  'lease_holder_boot_id', ARGV[3],
  'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[4],
  'lease_expire_at_ms', string.format('%.0f', expire),
  'state', 'SCHEDULE_DISPATCHING',
  'updated_at_ms', string.format('%.0f', now))
if ARGV[9] == '1' then
  redis.call('ZADD', KEYS[2], v[3], ARGV[8])
else
  redis.call('ZREM', KEYS[2], ARGV[8])
end
redis.call('ZADD', KEYS[3], v[3], ARGV[8])
return {'CLAIMED', tostring(epoch), v[5], string.format('%.0f', expire),
  tostring(ttl), v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13]}
`

// AdvanceScheduledWorkDualV1 is the dual-index variant of
// AdvanceScheduledWorkV1. KEYS: schedule record, shared due index, bucket
// meta, dedicated schedule due index.
const luaAdvanceScheduledWorkDualSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1],
  'schedule_type', 'schedule_id', 'typed_payload', 'generation', 'work_version',
  'state', 'lease_holder_id', 'lease_holder_boot_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms')
local generation = tonumber(v[4])
local work_version = tonumber(v[5])
if v[1] ~= ARGV[9] or v[2] ~= ARGV[10] or generation == nil or work_version == nil then
  return {'FENCE_MISMATCH'}
end
if generation ~= tonumber(ARGV[7]) then
  return {'STALE_GENERATION', tostring(generation)}
end
if work_version ~= tonumber(ARGV[6]) then
  return {'STALE_VERSION', tostring(work_version)}
end
if v[6] == 'SUPERSEDED' or v[6] == 'COMPLETED' then
  return {'STALE_GENERATION', tostring(generation)}
end
if v[6] ~= 'SCHEDULE_DISPATCHING' or v[7] ~= ARGV[2] or v[8] ~= ARGV[3] or
   v[9] ~= ARGV[4] or v[10] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[11]) == nil or tonumber(v[11]) <= now then
  return {'LEASE_EXPIRED'}
end
local next_due = tonumber(ARGV[11])
if next_due == nil or next_due <= 0 or #ARGV[12] > 16384 or #ARGV[13] ~= 64 or
   not string.match(ARGV[13], '^[0-9a-f]+$') then
  return {'CONTRACT_ERROR'}
end
local pending_bytes = tonumber(redis.call('HGET', KEYS[3], 'pending_work_bytes') or '0')
if pending_bytes == nil or pending_bytes < 0 or pending_bytes + #ARGV[12] - #v[3] < 0 or
   pending_bytes + #ARGV[12] - #v[3] > tonumber(ARGV[14]) then
  return {'CAPACITY_EXCEEDED'}
end
local next_version = work_version + 1
redis.call('HSET', KEYS[1],
  'typed_payload', ARGV[12],
  'payload_hash', ARGV[13],
  'next_due_at_ms', string.format('%.0f', next_due),
  'work_version', tostring(next_version),
  'state', 'PENDING',
  'lease_holder_id', '',
  'lease_holder_boot_id', '',
  'lease_token_hash', '',
  'lease_expire_at_ms', '0',
  'completed_at_ms', '0',
  'updated_at_ms', string.format('%.0f', now))
if ARGV[15] == '1' then
  redis.call('ZADD', KEYS[2], string.format('%.0f', next_due), ARGV[8])
else
  redis.call('ZREM', KEYS[2], ARGV[8])
end
redis.call('ZADD', KEYS[4], string.format('%.0f', next_due), ARGV[8])
local delta = #ARGV[12] - #v[3]
if delta ~= 0 then redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', delta) end
return {'ADVANCED', tostring(next_version), tostring(generation), string.format('%.0f', next_due)}
`

// CompleteScheduledWorkDualV1 is the dual-index variant of
// CompleteScheduledWorkV1. KEYS: schedule record, shared due index, bucket
// meta, dedicated schedule due index.
const luaCompleteScheduledWorkDualSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then return {'NOT_FOUND'} end
local v = redis.call('HMGET', KEYS[1], 'schedule_type', 'schedule_id', 'typed_payload',
  'generation', 'work_version', 'state', 'lease_holder_id', 'lease_holder_boot_id',
  'lease_epoch', 'lease_token_hash', 'lease_expire_at_ms')
local generation = tonumber(v[4])
local work_version = tonumber(v[5])
if v[1] ~= ARGV[9] or v[2] ~= ARGV[10] or generation == nil or work_version == nil then
  return {'FENCE_MISMATCH'}
end
if generation ~= tonumber(ARGV[7]) then return {'STALE_GENERATION', tostring(generation)} end
if work_version ~= tonumber(ARGV[6]) then return {'STALE_VERSION', tostring(work_version)} end
if v[6] == 'COMPLETED' or v[6] == 'SUPERSEDED' then return {'ALREADY_COMPLETED'} end
if v[6] ~= 'SCHEDULE_DISPATCHING' or v[7] ~= ARGV[2] or v[8] ~= ARGV[3] or
   v[9] ~= ARGV[4] or v[10] ~= ARGV[5] then return {'FENCE_MISMATCH'} end
if tonumber(v[11]) == nil or tonumber(v[11]) <= now then return {'LEASE_EXPIRED'} end
local pending = tonumber(redis.call('HGET', KEYS[3], 'pending_work_count') or '0')
local pending_bytes = tonumber(redis.call('HGET', KEYS[3], 'pending_work_bytes') or '0')
if pending == nil or pending <= 0 or pending_bytes == nil or pending_bytes < #v[3] then
  return {'MALFORMED_RECORD'}
end
redis.call('HSET', KEYS[1], 'state', 'COMPLETED', 'next_due_at_ms', '0',
  'completed_at_ms', string.format('%.0f', now), 'updated_at_ms', string.format('%.0f', now),
  'lease_holder_id', '', 'lease_holder_boot_id', '', 'lease_token_hash', '', 'lease_expire_at_ms', '0')
redis.call('ZREM', KEYS[2], ARGV[8])
redis.call('ZREM', KEYS[4], ARGV[8])
redis.call('HINCRBY', KEYS[3], 'pending_work_count', -1)
redis.call('HINCRBY', KEYS[3], 'pending_work_bytes', -#v[3])
return {'COMPLETED'}
`

// BackfillScheduleDueIndexV1 fills one schedule due member into the dedicated
// index only after proving the current schedule record's identity, generation,
// state and next_due score. A stale backfill can never overwrite a newer
// schedule because the record itself is the fence; a completed or superseded
// record is never resurrected. KEYS: schedule record, dedicated schedule due
// index. ARGV: 1 tag, 2 schedule_ref, 3 schedule_type, 4 schedule_id,
// 5 expected_next_due_at_ms, 6 expected_generation, 7 expected_state.
const luaBackfillScheduleDueIndexSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('ZREM', KEYS[2], ARGV[2])
  return {'REMOVED'}
end
local v = redis.call('HMGET', KEYS[1], 'schedule_type', 'schedule_id', 'generation',
  'state', 'next_due_at_ms')
if v[1] ~= ARGV[3] or v[2] ~= ARGV[4] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[3]) == nil or tonumber(v[3]) <= 0 or
   tonumber(v[3]) ~= tonumber(ARGV[6]) then
  return {'STALE_GENERATION', v[3] or ''}
end
if v[4] ~= ARGV[7] then
  return {'STALE_STATE', v[4] or ''}
end
if v[4] ~= 'PENDING' and v[4] ~= 'SCHEDULE_DISPATCHING' then
  redis.call('ZREM', KEYS[2], ARGV[2])
  return {'REMOVED'}
end
if tonumber(v[5]) == nil or tonumber(v[5]) <= 0 or
   tonumber(v[5]) ~= tonumber(ARGV[5]) then
  return {'STALE_SCORE', v[5] or ''}
end
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[2])
return {'BACKFILLED'}
`

// ReconcileBucketPendingWorkV1 is an explicit bounded rolling/historical
// repair. KEYS are bucket meta, shared DueIndex, dedicated schedule DueIndex,
// followed by the exact record key for every member in their bounded union.
// ARGV: 1 tag, 2 route_domain, 3 layout, 4 bucket_id, 5 max_pending_count,
// 6 max_pending_bytes, 7 shared_cardinality, 8 dedicated_cardinality; then
// per record: kind (O/S), member, shared score or empty, dedicated score or
// empty, type, id. Record hashes are authoritative for counted state/payload;
// both indexes are an atomically revalidated complete locator/fence. Every
// validation runs before the first ZREM/HSET, so a concurrent lifecycle write
// yields STALE_SNAPSHOT or is fully ordered before/after this repair.
const luaReconcilePendingWorkSrc = luaCheckTag + `
local function key_type(key)
  local value = redis.call('TYPE', key)
  if type(value) == 'table' then return value['ok'] end
  return value
end
local function canonical_nonnegative(value)
  if not value or string.match(value, '^%d+$') == nil then return false end
  if #value > 1 and string.sub(value, 1, 1) == '0' then return false end
  if #value > 19 or (#value == 19 and value > '9223372036854775807') then return false end
  return true
end
local function exact_integer_score(value, expected)
  if not canonical_nonnegative(expected) or
     (#expected > 16 or (#expected == 16 and expected > '9007199254740991')) then return false end
  return value and string.format('%.0f', tonumber(value)) == expected
end
local record_count = #KEYS - 3
if record_count < 0 or record_count > 4096 or #ARGV ~= 8 + record_count * 6 then
  return {'CAPACITY_EXCEEDED'}
end
if not canonical_nonnegative(ARGV[4]) or not canonical_nonnegative(ARGV[5]) or
   not canonical_nonnegative(ARGV[6]) or not canonical_nonnegative(ARGV[7]) or
   not canonical_nonnegative(ARGV[8]) then
  return {'CONTRACT_ERROR'}
end
if tonumber(ARGV[7]) > 4096 or tonumber(ARGV[8]) > 4096 or
   tonumber(ARGV[7]) > tonumber(ARGV[5]) or tonumber(ARGV[8]) > tonumber(ARGV[5]) then
  return {'CAPACITY_EXCEEDED'}
end
if key_type(KEYS[1]) ~= 'hash' then return {'MALFORMED_RECORD'} end
local meta = redis.call('HMGET', KEYS[1], 'schema_version', 'route_domain',
  'authority_layout_version', 'bucket_id', 'pending_work_count', 'pending_work_bytes')
if meta[1] ~= '1' or meta[2] ~= ARGV[2] or meta[3] ~= ARGV[3] or meta[4] ~= ARGV[4] then
  return {'MALFORMED_RECORD'}
end
local shared_type = key_type(KEYS[2])
local dedicated_type = key_type(KEYS[3])
if (shared_type ~= 'none' and shared_type ~= 'zset') or
   (dedicated_type ~= 'none' and dedicated_type ~= 'zset') then
  return {'MALFORMED_RECORD'}
end
if redis.call('ZCARD', KEYS[2]) ~= tonumber(ARGV[7]) or
   redis.call('ZCARD', KEYS[3]) ~= tonumber(ARGV[8]) then
  return {'STALE_SNAPSHOT'}
end

local seen_members = {}
local seen_keys = {}
local stale_members = {}
local supplied_shared = 0
local supplied_dedicated = 0
local pending_count = 0
local pending_bytes = 0
for i = 1, record_count do
  local base = 9 + (i - 1) * 6
  local kind = ARGV[base]
  local member = ARGV[base + 1]
  local shared_score = ARGV[base + 2]
  local dedicated_score = ARGV[base + 3]
  local record_type = ARGV[base + 4]
  local record_id = ARGV[base + 5]
  local record_key = KEYS[3 + i]
  if (kind ~= 'O' and kind ~= 'S') or member == '' or record_type == '' or record_id == '' or
     seen_members[member] or seen_keys[record_key] or key_type(record_key) ~= 'hash' then
    return {'MALFORMED_RECORD'}
  end
  seen_members[member] = true
  seen_keys[record_key] = true

  local actual_shared = false
  local actual_dedicated = false
  if shared_type == 'zset' then actual_shared = redis.call('ZSCORE', KEYS[2], member) end
  if dedicated_type == 'zset' then actual_dedicated = redis.call('ZSCORE', KEYS[3], member) end
  if (shared_score == '' and actual_shared) or
     (shared_score ~= '' and not exact_integer_score(actual_shared, shared_score)) or
     (dedicated_score == '' and actual_dedicated) or
     (dedicated_score ~= '' and (kind ~= 'S' or not exact_integer_score(actual_dedicated, dedicated_score))) then
    return {'STALE_SNAPSHOT'}
  end
  if shared_score ~= '' then supplied_shared = supplied_shared + 1 end
  if dedicated_score ~= '' then supplied_dedicated = supplied_dedicated + 1 end

  local v
  if kind == 'O' then
    v = redis.call('HMGET', record_key, 'schema_version', 'work_type', 'work_id',
      'typed_payload', 'state', 'next_due_at_ms', 'bucket_id',
      'authority_layout_version', 'actor_route_domain', 'placement_kind', 'payload_hash')
  else
    v = redis.call('HMGET', record_key, 'schema_version', 'schedule_type', 'schedule_id',
      'typed_payload', 'state', 'next_due_at_ms', 'bucket_id',
      'authority_layout_version', 'actor_route_domain', 'placement_kind', 'payload_hash')
  end
  if v[1] ~= '1' or v[2] ~= record_type or v[3] ~= record_id or not v[4] or
     #v[4] > 16384 or not v[5] or not canonical_nonnegative(v[6]) or
     v[7] ~= ARGV[4] or v[8] ~= ARGV[3] or v[9] ~= ARGV[2] or
     v[10] ~= 'SOURCE_PERSISTENCE_BUCKET' or not v[11] or #v[11] ~= 64 or
     string.match(v[11], '^[0-9a-f]+$') == nil then
    return {'MALFORMED_RECORD'}
  end

  local counted = (kind == 'O' and (v[5] == 'PENDING' or v[5] == 'REWARD_DISPATCHING')) or
    (kind == 'S' and (v[5] == 'PENDING' or v[5] == 'SCHEDULE_DISPATCHING'))
  local terminal = (kind == 'O' and (v[5] == 'COMPLETED' or v[5] == 'RECONCILE_REQUIRED')) or
    (kind == 'S' and (v[5] == 'COMPLETED' or v[5] == 'SUPERSEDED'))
  if not counted and not terminal then return {'MALFORMED_RECORD'} end
  if counted then
    if (not actual_shared and not actual_dedicated) or
       (actual_shared and not exact_integer_score(actual_shared, v[6])) or
       (actual_dedicated and not exact_integer_score(actual_dedicated, v[6])) then
      return {'STALE_SNAPSHOT'}
    end
    pending_count = pending_count + 1
    pending_bytes = pending_bytes + #v[4]
  else
    stale_members[#stale_members + 1] = member
  end
end
if supplied_shared ~= tonumber(ARGV[7]) or supplied_dedicated ~= tonumber(ARGV[8]) then
  return {'STALE_SNAPSHOT'}
end
if pending_count > tonumber(ARGV[5]) or pending_bytes > tonumber(ARGV[6]) then
  return {'CAPACITY_EXCEEDED'}
end

-- Linearization point: the complete bounded index snapshot and every record
-- passed validation. Terminal stale members are removed before exact counters
-- are installed, all in this one atomic script.
for _, member in ipairs(stale_members) do
  redis.call('ZREM', KEYS[2], member)
  redis.call('ZREM', KEYS[3], member)
end
local old_count = meta[5] or ''
local old_bytes = meta[6] or ''
redis.call('HSET', KEYS[1],
  'pending_work_count', tostring(pending_count),
  'pending_work_bytes', tostring(pending_bytes))
return {'RECONCILED', old_count, old_bytes, tostring(pending_count), tostring(pending_bytes)}
`

// ClaimBucketScanLeaseV1 ARGV: 1 tag, 2 holder, 3 requested_ttl_ms,
// 4 min_ttl_ms, 5 max_ttl_ms. KEYS: scan lease.
const luaClaimBucketScanLeaseSrc = luaCheckTag + luaRedisTime + `
local epoch = 1
if redis.call('EXISTS', KEYS[1]) == 1 then
  local v = redis.call('HMGET', KEYS[1], 'holder_id', 'lease_expire_at_ms', 'lease_epoch')
  if tonumber(v[2]) > now and v[1] ~= ARGV[2] then
    return {'HELD_BY_OTHER', v[2]}
  end
  epoch = tonumber(v[3]) + 1
end
local ttl = tonumber(ARGV[3])
local min_ttl = tonumber(ARGV[4])
local max_ttl = tonumber(ARGV[5])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
redis.call('HSET', KEYS[1],
  'holder_id', ARGV[2],
  'lease_epoch', tostring(epoch),
  'lease_expire_at_ms', string.format('%.0f', expire))
return {'CLAIMED', tostring(epoch), string.format('%.0f', expire), tostring(ttl)}
`

// RenewBucketScanLeaseV1 ARGV: 1 tag, 2 holder, 3 lease_epoch,
// 4 requested_ttl_ms, 5 min_ttl_ms, 6 max_ttl_ms. KEYS: scan lease.
const luaRenewBucketScanLeaseSrc = luaCheckTag + luaRedisTime + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1], 'holder_id', 'lease_epoch', 'lease_expire_at_ms')
if v[1] ~= ARGV[2] or v[2] ~= ARGV[3] then
  return {'FENCE_MISMATCH'}
end
if tonumber(v[3]) <= now then
  return {'LEASE_EXPIRED'}
end
local ttl = tonumber(ARGV[4])
local min_ttl = tonumber(ARGV[5])
local max_ttl = tonumber(ARGV[6])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
redis.call('HSET', KEYS[1], 'lease_expire_at_ms', string.format('%.0f', expire))
return {'RENEWED', string.format('%.0f', expire), tostring(ttl)}
`

// PublishResultEntryV1 ARGV: 1 field, 2 value, 3 max_value_bytes.
// Projection keys are single-key operations and carry no hash tag.
const luaPublishResultEntrySrc = `
if #ARGV[2] > tonumber(ARGV[3]) then
  return {'CAPACITY_EXCEEDED'}
end
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
  if existing == ARGV[2] then
    return {'ALREADY_PUBLISHED'}
  end
  return {'CONFLICT'}
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return {'PUBLISHED'}
`

// scriptDef is one registered, immutable Lua source.
type scriptDef struct {
	name   string
	source string
	sha    string
}

func newScriptDef(name, source string) scriptDef {
	sum := sha1.Sum([]byte(source)) //nolint:gosec // Redis script cache identity.
	return scriptDef{name: name, source: source, sha: hexEncode(sum[:])}
}

func builtinScripts() []scriptDef {
	return []scriptDef{
		newScriptDef(ScriptClaimShardAuthority, luaClaimShardAuthoritySrc),
		newScriptDef(ScriptRenewShardAuthority, luaRenewShardAuthoritySrc),
		newScriptDef(ScriptTransferShardAuthority, luaTransferShardAuthoritySrc),
		newScriptDef(ScriptTakeoverExpiredAuthority, luaTakeoverExpiredShardAuthoritySrc),
		newScriptDef(ScriptUnassignShardAuthority, luaUnassignShardAuthoritySrc),
		newScriptDef(ScriptLoadAndClaimActorV5, luaLoadAndClaimActorV5Src),
		newScriptDef(ScriptLoadAndClaimActor, luaLoadAndClaimActorSrc),
		newScriptDef(ScriptCommitActorWithOrdersV12, luaCommitActorWithOrdersV12Src),
		newScriptDef(ScriptCommitActorWithOrdersV13, luaCommitActorWithOrdersV13Src),
		newScriptDef(ScriptCommitActorWithOrders, luaCommitActorWithOrdersV15Src),
		newScriptDef(ScriptCommitActorWithOrdersExact, luaCommitActorWithOrdersSrc),
		newScriptDef(ScriptFenceAndClaimActor, luaFenceAndClaimActorSrc),
		newScriptDef(ScriptClaimWorkV5, luaClaimWorkV5Src),
		newScriptDef(ScriptRenewWorkLease, luaRenewWorkLeaseSrc),
		newScriptDef(ScriptCompleteWorkV5, luaCompleteWorkV5Src),
		newScriptDef(ScriptVerifySourceOrderV1, luaVerifySourceOrderSrc),
		newScriptDef(ScriptVerifySourceOrderV2, luaVerifySourceOrderV2Src),
		newScriptDef(ScriptClaimAndCompleteWorkV3, luaClaimAndCompleteWorkV3Src),
		newScriptDef(ScriptClaimAndCompleteWorkV4, luaClaimAndCompleteWorkV4Src),
		newScriptDef(ScriptRescheduleWorkV2, luaRescheduleWorkV2Src),
		newScriptDef(ScriptClaimBucketScanLease, luaClaimBucketScanLeaseSrc),
		newScriptDef(ScriptRenewBucketScanLease, luaRenewBucketScanLeaseSrc),
		newScriptDef(ScriptPublishResultEntry, luaPublishResultEntrySrc),
		newScriptDef(ScriptClaimScheduledWork, luaClaimScheduledWorkSrc),
		newScriptDef(ScriptAdvanceScheduledWork, luaAdvanceScheduledWorkSrc),
		newScriptDef(ScriptRenewScheduledWork, luaRenewScheduledWorkSrc),
		newScriptDef(ScriptCompleteScheduledWork, luaCompleteScheduledWorkSrc),
		newScriptDef(ScriptClaimScheduledWorkDual, luaClaimScheduledWorkDualSrc),
		newScriptDef(ScriptAdvanceScheduledWorkDual, luaAdvanceScheduledWorkDualSrc),
		newScriptDef(ScriptCompleteScheduledWorkDual, luaCompleteScheduledWorkDualSrc),
		newScriptDef(ScriptBackfillScheduleDueIndex, luaBackfillScheduleDueIndexSrc),
		newScriptDef(ScriptReconcilePendingWork, luaReconcilePendingWorkSrc),
	}
}
