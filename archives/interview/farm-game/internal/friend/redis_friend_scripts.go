package friend

import "fmt"

const (
	scriptFriendReadCompleteSnapshot = "ReadFriendAuthorityCompleteSnapshotV2"
	scriptFriendPrepare              = "FriendPrepareCommitAttemptV1"
	scriptFriendResolve              = "FriendFenceAndResolveCommitAttemptV1"
	scriptFriendCompact              = "FriendCompactCompletedCommitMarkerV1"
	scriptPairDelete                 = "FriendPairDeleteV1"
	scriptPairBlock                  = "FriendPairBlockV1"
	scriptPairUnblock                = "FriendPairUnblockV1"
	scriptPairAction                 = "FriendPairActionAdmissionV1"
	scriptPairActivate               = "FriendPairActivateV1"
	scriptPairFloors                 = "FriendPairCommitReactivationFloorsV1"
	scriptPairComplete               = "FriendPairCompleteTerminationV1"
	scriptPairQuotaReleased          = "FriendPairMarkQuotaReleasedV1"
	scriptQuotaReserve               = "FriendQuotaReserveV1"
	scriptQuotaFinalize              = "FriendQuotaFinalizeV1"
	scriptQuotaRelease               = "FriendQuotaReleaseReservationV1"
	scriptQuotaRemove                = "FriendQuotaRemoveActiveEpochV1"
	scriptShareGenerate              = "FriendShareGenerateV1"
	scriptShareRotate                = "FriendShareRotateV1"
	scriptShareReactivate            = "FriendShareRotateForReactivationV1"
	scriptShareRevoke                = "FriendShareRevokeV1"
	scriptShareAccept                = "FriendShareAcceptAdmissionV1"
)

const friendLuaPrelude = `
local tag = ARGV[1]
for i = 1, #KEYS do
  if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then
    return {'CROSS_SLOT'}
  end
end
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local SEP = string.char(31)
local function num(key, field, fallback)
  local v = redis.call('HGET', key, field)
  if not v then return fallback end
  return tonumber(v) or fallback
end
local staged = {}
local stagedFields = {}
local stagedDeltaByKey = {}
local stagedNewCountByKey = {}
local function stagedSize(field, value)
  if field == '_meta:logical_bytes' or not value then return 0 end
  return #field + #value
end
local function trackStaged(key, field, final)
  if not stagedFields[key] then stagedFields[key] = {} end
  local state = stagedFields[key][field]
  if not state then
    local old = redis.call('HGET', key, field)
    state = {old=old, final=nil}
    stagedFields[key][field] = state
    stagedDeltaByKey[key] = (stagedDeltaByKey[key] or 0) - stagedSize(field, old)
    if not old and field ~= '_meta:logical_bytes' then
      stagedNewCountByKey[key] = (stagedNewCountByKey[key] or 0) + 1
    end
  end
  stagedDeltaByKey[key] = (stagedDeltaByKey[key] or 0) - stagedSize(field, state.final) + stagedSize(field, final)
  if state.final == nil and final ~= nil and not state.old and field ~= '_meta:logical_bytes' then
    -- The new-field count was already charged when the field was first seen.
  elseif state.final ~= nil and final == nil and not state.old and field ~= '_meta:logical_bytes' then
    stagedNewCountByKey[key] = math.max(0, (stagedNewCountByKey[key] or 0) - 1)
  end
  state.final = final
end
local function textLP(value)
  return tostring(#value) .. ':' .. value
end
local function typedValue(...)
  local values = {...}
  local out = 'TLP1'
  for i = 1, #values do out = out .. textLP(values[i]) end
  return out
end
local function receiptValue(kind, status, reason, epoch, detail, typed, prefix)
  return prefix .. 'FR3' .. textLP(kind) .. textLP(status) .. textLP(reason) ..
    textLP(tostring(epoch)) .. textLP(detail) .. textLP(tostring(now)) .. textLP(typed) ..
    textLP(ARGV[7]) .. textLP(ARGV[8]) .. textLP(ARGV[6])
end
local function resolutionValueAt(status, attemptID, candidateHash, resolutionID, ordinal, resolvedAt, markerDigest,
    receiptField, receiptKind, receiptEpoch)
  return 'FRS3' .. textLP(status) .. textLP(attemptID) .. textLP(candidateHash) ..
    textLP(resolutionID) .. textLP(tostring(ordinal)) .. textLP(tostring(resolvedAt)) .. textLP(markerDigest or '') ..
    textLP(receiptField or '') .. textLP(receiptKind or '') .. textLP(tostring(receiptEpoch or 0))
end
local function resolutionValue(status, attemptID, candidateHash, resolutionID, ordinal, markerDigest,
    receiptField, receiptKind, receiptEpoch)
  return resolutionValueAt(status, attemptID, candidateHash, resolutionID, ordinal, now, markerDigest,
    receiptField, receiptKind, receiptEpoch)
end
local function readTextLP(value, position)
  local colon = string.find(value, ':', position, true)
  if not colon then return nil, #value + 1 end
  local length = tonumber(string.sub(value, position, colon - 1))
  if not length or length < 0 then return nil, #value + 1 end
  local first = colon + 1
  local last = first + length - 1
  if last > #value then return nil, #value + 1 end
  return string.sub(value, first, last), last + 1
end
local function resolutionMatches(value, resolutionID, attemptID, candidateHash, ordinal)
  if string.sub(value, 1, 4) ~= 'FRS3' then return false, '' end
  local pos = 5
  local status, nextPos = readTextLP(value, pos); if not status then return false, '' end; pos = nextPos
  local storedAttempt; storedAttempt, nextPos = readTextLP(value, pos); if not storedAttempt then return false, '' end; pos = nextPos
  local storedHash; storedHash, nextPos = readTextLP(value, pos); if not storedHash then return false, '' end; pos = nextPos
  local storedResolution; storedResolution, nextPos = readTextLP(value, pos); if not storedResolution then return false, '' end; pos = nextPos
  local storedOrdinal; storedOrdinal, nextPos = readTextLP(value, pos); if not storedOrdinal then return false, '' end; pos = nextPos
  local storedTime, nextTime; storedTime, nextTime = readTextLP(value, pos); if not storedTime then return false, '' end; pos = nextTime
  local storedMarker; storedMarker, nextPos = readTextLP(value, pos); if not storedMarker then return false, '' end; pos = nextPos
  local storedReceiptField; storedReceiptField, nextPos = readTextLP(value, pos); if not storedReceiptField then return false, '' end; pos = nextPos
  local storedKind; storedKind, nextPos = readTextLP(value, pos); if not storedKind then return false, '' end; pos = nextPos
  local storedEpoch; storedEpoch, nextPos = readTextLP(value, pos); if not storedEpoch or nextPos ~= #value + 1 then return false, '' end
	if storedResolution ~= resolutionID or storedAttempt ~= attemptID or storedHash ~= candidateHash or storedOrdinal ~= tostring(ordinal) or
	   string.match(storedTime, '^%d+$') == nil or tonumber(storedTime) == nil or tostring(math.floor(tonumber(storedTime))) ~= storedTime or
	   (storedEpoch ~= '0' and string.match(storedEpoch, '^[1-9]%d*$') == nil) or tonumber(storedEpoch) == nil or
	   tostring(math.floor(tonumber(storedEpoch))) ~= storedEpoch then
    return false, ''
  end
  if status ~= 'COMMITTED' and status ~= 'NOT_COMMITTED' and status ~= 'RECONCILE_REQUIRED' then
    return false, ''
  end
  if storedMarker == '' or
     (status == 'COMMITTED' and (storedReceiptField == '' or storedKind == '' or string.match(storedEpoch, '^%d+$') == nil)) or
     (status ~= 'COMMITTED' and (storedReceiptField ~= '' or storedKind ~= '' or storedEpoch ~= '0')) then
    return false, ''
  end
  local exact = resolutionValueAt(status, storedAttempt, storedHash, storedResolution, ordinal, storedTime,
    storedMarker, storedReceiptField, storedKind, storedEpoch)
  if exact ~= value then return false, '' end
  return true, status, storedMarker, storedReceiptField, storedKind, storedEpoch, storedTime
end
local function receiptCandidateMatches(value, prefix, candidateHash)
  local prefixLength = string.len(prefix or '')
  local version = string.sub(value, prefixLength + 1, prefixLength + 3)
  if version == 'FR2' then return true end
  if version ~= 'FR3' then return false end
  local pos = prefixLength + 4
  for i = 1, 7 do
    local _, nextPos = readTextLP(value, pos)
    if not nextPos or nextPos > #value + 1 then return false end
    pos = nextPos
  end
  local _, nextPos = readTextLP(value, pos)
  if not nextPos or nextPos > #value + 1 then return false end
  pos = nextPos
  local storedHash
  storedHash, nextPos = readTextLP(value, pos)
  if not storedHash then return false end
  return storedHash == candidateHash
end
local function receiptAttemptMatches(value, prefix, attemptID, candidateHash, ordinal)
  local prefixLength = string.len(prefix or '')
  local version = string.sub(value, prefixLength + 1, prefixLength + 3)
  if version ~= 'FR3' then return false end
  local pos = prefixLength + 4
  for i = 1, 7 do
    local _, nextPos = readTextLP(value, pos)
    if not nextPos or nextPos > #value + 1 then return false end
    pos = nextPos
  end
  local storedAttempt, nextPos = readTextLP(value, pos)
  if not storedAttempt then return false end
  pos = nextPos
  local storedHash
  storedHash, nextPos = readTextLP(value, pos)
  if not storedHash then return false end
  pos = nextPos
  local storedOrdinal
  storedOrdinal, nextPos = readTextLP(value, pos)
  if not storedOrdinal then return false end
  return storedAttempt == attemptID and storedHash == candidateHash and storedOrdinal == tostring(ordinal)
end
local function stageSet(key, ...)
  local values = {...}
  if #values % 2 ~= 0 then return false end
  for i = 1, #values, 2 do
    local field, value = values[i], values[i + 1]
    trackStaged(key, field, value)
    local replaced = false
    for j = #staged, 1, -1 do
      if staged[j].key == key and staged[j].field == field then
        staged[j].delete = false; staged[j].value = value; replaced = true; break
      end
    end
    if not replaced then staged[#staged + 1] = {key=key, field=field, value=value, delete=false} end
  end
	  return true
end
local function stageDelete(key, ...)
  local fields = {...}
  for _, field in ipairs(fields) do
    trackStaged(key, field, nil)
    local replaced = false
    for j = #staged, 1, -1 do
      if staged[j].key == key and staged[j].field == field then
        staged[j].delete = true; staged[j].value = nil; replaced = true; break
      end
    end
    if not replaced then staged[#staged + 1] = {key=key, field=field, value=nil, delete=true} end
  end
end
local function plannedBytes(key)
  -- The Lua-maintained counter is the authoritative delta base.  There is
  -- intentionally no HKEYS/HGET fallback here: a pre-existing record without
  -- the frozen counter is corrupt and must be reconciled, not guessed.
  local total = 0
  local storedBytes = redis.call('HGET', key, '_meta:logical_bytes')
  if not storedBytes then
    if redis.call('EXISTS', key) == 1 then return nil, 'LOGICAL_BYTES_MISSING' end
  elseif not string.match(storedBytes, '^%d+$') then
    return nil, 'LOGICAL_BYTES_MISSING'
  else
    total = tonumber(storedBytes)
    if not total then return nil, 'LOGICAL_BYTES_MISSING' end
  end
  return total + (stagedDeltaByKey[key] or 0), ''
end
local function applyStaged(logicalBytes)
	local function applyItem(item)
		if item.delete then redis.call('HDEL', item.key, item.field)
		else redis.call('HSET', item.key, item.field, item.value) end
	end
	-- Publish the record version/digest before the remaining staged fields. If
	-- Redis reports a runtime error between two HSETs, the resolver observes a
	-- changed canonical fence and returns RECONCILE_REQUIRED instead of
	-- mistaking a partially applied record for NOT_COMMITTED.
	for _, item in ipairs(staged) do
		if item.key == KEYS[1] and (item.field == '_meta:record_version' or item.field == '_meta:business_state_digest') then
			applyItem(item)
		end
	end
	for _, item in ipairs(staged) do
		if not (item.key == KEYS[1] and (item.field == '_meta:record_version' or item.field == '_meta:business_state_digest')) then
			applyItem(item)
		end
	end
	redis.call('HSET', KEYS[1], '_meta:logical_bytes', tostring(logicalBytes))
end
local function cursorBase(digest)
  return 'cursor:' .. digest
end
local function commitCursor(attemptKey, digest, attemptDueKey, attemptDueMember, state, retentionMS)
  local base = cursorBase(digest)
  redis.call('HSET', attemptKey, base .. ':state', state)
  if state ~= 'OPEN' then
    redis.call('ZADD', attemptDueKey, now + tonumber(retentionMS), attemptDueMember)
  end
end
local function checkOpenCursor(attemptKey, digest, attemptID, ordinal, candidateHash)
  local base = cursorBase(digest)
  local state = redis.call('HGET', attemptKey, base .. ':state')
  if not state then return false, 'ATTEMPT_REQUIRED' end
  if state ~= 'OPEN' then return false, 'ATTEMPT_FENCED_' .. state end
  if redis.call('HGET', attemptKey, base .. ':attempt_id') ~= attemptID or
     redis.call('HGET', attemptKey, base .. ':ordinal') ~= ordinal or
     redis.call('HGET', attemptKey, base .. ':candidate_hash') ~= candidateHash then
    return false, 'ATTEMPT_CONFLICT'
  end
  return true, ''
end
local function prepareMeta(metaKey, kind, layout, bucket)
  redis.call('HSETNX', metaKey, 'schema_version', 'friend-auth-v1')
  redis.call('HSETNX', metaKey, 'authority_kind', kind)
  redis.call('HSETNX', metaKey, 'layout_version', layout)
  redis.call('HSETNX', metaKey, 'bucket_id', bucket)
  redis.call('HSETNX', metaKey, 'receipt_count', '0')
  redis.call('HSETNX', metaKey, 'receipt_bytes', '0')
  redis.call('HSETNX', metaKey, 'pending_work_count', '0')
  redis.call('HSETNX', metaKey, 'pending_work_bytes', '0')
  redis.call('HSETNX', metaKey, 'attempt_count', '0')
  redis.call('HSETNX', metaKey, 'attempt_bytes', '0')
  redis.call('HSETNX', metaKey, 'last_updated_at_ms', '0')
end
local function checkMeta(metaKey, kind, layout, bucket)
  local schema = redis.call('HGET', metaKey, 'schema_version')
  local storedKind = redis.call('HGET', metaKey, 'authority_kind')
  local storedLayout = redis.call('HGET', metaKey, 'layout_version')
  local storedBucket = redis.call('HGET', metaKey, 'bucket_id')
  if schema ~= 'friend-auth-v1' or storedKind ~= kind then return false, 'SCHEMA_MISMATCH' end
  if storedLayout ~= layout or storedBucket ~= tostring(bucket) then return false, 'LAYOUT_MISMATCH' end
  return true, ''
end
local function friendKeyType(key)
  local reply = redis.call('TYPE', key)
  return reply.ok or reply[1] or ''
end
local function friendKeyTypeOK(key, expected)
  local actual = friendKeyType(key)
  return actual == 'none' or actual == expected
end
local function strictUnsigned(value)
  if not value or (value ~= '0' and string.match(value, '^[1-9]%d*$') == nil) then return false end
  return tonumber(value) ~= nil
end
local function validateBucketMeta(metaKey)
  local actual = friendKeyType(metaKey)
  if actual == 'none' then return true, '' end
  if actual ~= 'hash' then return false, 'CORRUPT_RECORD' end
  local count = redis.call('HLEN', metaKey)
  if count ~= 11 then return false, 'MALFORMED_COUNTER' end
  local required = {'schema_version', 'authority_kind', 'layout_version', 'bucket_id',
    'receipt_count', 'receipt_bytes', 'pending_work_count', 'pending_work_bytes',
    'attempt_count', 'attempt_bytes', 'last_updated_at_ms'}
  local values = redis.call('HMGET', metaKey, unpack(required))
  for index, _ in ipairs(required) do
    if values[index] == false then return false, 'MALFORMED_COUNTER' end
  end
  for _, index in ipairs({4, 5, 6, 7, 8, 9, 10, 11}) do
    if not strictUnsigned(values[index]) then return false, 'MALFORMED_COUNTER' end
  end
  return true, ''
end
local function validateBucketMetaAuthority(metaKey, recordKey, kind, layout, bucket, allowCreate)
  local metaType = friendKeyType(metaKey)
  local recordType = friendKeyType(recordKey)
  if recordType ~= 'none' and recordType ~= 'hash' then return false, 'CORRUPT_RECORD' end
  if metaType == 'none' then
    if recordType ~= 'none' or not allowCreate then return false, 'CORRUPT_RECORD' end
    return true, 'MISSING'
  end
  local shapeOK, shapeWhy = validateBucketMeta(metaKey)
  if not shapeOK then return false, shapeWhy end
  local identityOK, identityWhy = checkMeta(metaKey, kind, layout, bucket)
  if not identityOK then return false, identityWhy end
  if recordType == 'none' and not allowCreate then return false, 'CORRUPT_RECORD' end
  return true, 'EXISTING'
end
local function validateBucketMetaCounts(metaKey, attemptDueKey, receiptDueKey, sourceDueKey, requireNoPending)
  if friendKeyType(metaKey) == 'none' then return true, '' end
  if friendKeyType(attemptDueKey) ~= 'none' and friendKeyType(attemptDueKey) ~= 'zset' then return false, 'CORRUPT_RECORD' end
  if friendKeyType(receiptDueKey) ~= 'none' and friendKeyType(receiptDueKey) ~= 'zset' then return false, 'CORRUPT_RECORD' end
  if sourceDueKey and sourceDueKey ~= '' and friendKeyType(sourceDueKey) ~= 'none' and friendKeyType(sourceDueKey) ~= 'zset' then
    return false, 'CORRUPT_RECORD'
  end
  local receiptCount = tonumber(redis.call('HGET', metaKey, 'receipt_count'))
  local pendingCount = tonumber(redis.call('HGET', metaKey, 'pending_work_count'))
  local attemptCount = tonumber(redis.call('HGET', metaKey, 'attempt_count'))
  local receiptBytes = tonumber(redis.call('HGET', metaKey, 'receipt_bytes'))
  local pendingBytes = tonumber(redis.call('HGET', metaKey, 'pending_work_bytes'))
  local attemptBytes = tonumber(redis.call('HGET', metaKey, 'attempt_bytes'))
  if not receiptCount or not pendingCount or not attemptCount or not receiptBytes or not pendingBytes or not attemptBytes then
    return false, 'MALFORMED_COUNTER'
  end
  if receiptCount ~= redis.call('ZCARD', receiptDueKey) or attemptCount < redis.call('ZCARD', attemptDueKey) then
    return false, 'MALFORMED_COUNTER'
  end
  if sourceDueKey and sourceDueKey ~= '' then
    if pendingCount ~= redis.call('ZCARD', sourceDueKey) then return false, 'MALFORMED_COUNTER' end
  elseif requireNoPending and (pendingCount ~= 0 or pendingBytes ~= 0) then
    return false, 'MALFORMED_COUNTER'
  end
  if (receiptCount == 0) ~= (receiptBytes == 0) or (pendingCount == 0) ~= (pendingBytes == 0) or
     (attemptCount == 0) ~= (attemptBytes == 0) then return false, 'MALFORMED_COUNTER' end
  return true, ''
end
local function requireNumericField(key, field, allowSigned)
  local value = redis.call('HGET', key, field)
  if not value then return false end
  if allowSigned then
    if not strictUnsigned(value) and string.match(value, '^%-[1-9]%d*$') == nil then return false end
    return tonumber(value) ~= nil
  end
  return strictUnsigned(value)
end
local function validLowerHex(value, bytes)
  return value and #value == bytes * 2 and string.match(value, '^[0-9a-f]+$') ~= nil
end
local function canonicalLogicalBytes(recordKey)
  local total = 0
  for _, field in ipairs(redis.call('HKEYS', recordKey)) do
    if field ~= '_meta:logical_bytes' then
      local value = redis.call('HGET', recordKey, field)
      if not value then return nil end
      total = total + #field + #value
    end
  end
  return total
end
local function validateCanonicalRecord(recordKey, kind, authorityID)
  local keyType = friendKeyType(recordKey)
  if keyType == 'none' then return true, '' end
  if keyType ~= 'hash' or redis.call('HLEN', recordKey) == 0 then return false, 'CORRUPT_RECORD' end
  local schema = redis.call('HGET', recordKey, '_meta:schema_version')
  local expected = kind == 'FRIEND_PAIR' and 'friend-pair-v1' or
    (kind == 'FRIEND_QUOTA' and 'friend-quota-v1' or 'friend-share-issuer-v1')
  if schema ~= expected or (redis.call('HGET', recordKey, '_meta:authority_layout_version') or '') == '' or
     not requireNumericField(recordKey, '_meta:record_version', false) or
     not requireNumericField(recordKey, '_meta:updated_at_ms', false) or
     not requireNumericField(recordKey, '_meta:logical_bytes', false) or
     not validLowerHex(redis.call('HGET', recordKey, '_meta:business_state_digest'), 32) then
    return false, 'CORRUPT_RECORD'
  end
  if kind == 'FRIEND_PAIR' then
	if redis.call('HLEN', recordKey) ~= 40 then return false, 'CORRUPT_RECORD' end
    for _, field in ipairs({'identity:pair_id','identity:low_player_id','identity:high_player_id','relation:id',
        'relation:status','relation:epoch','relation:activated_at_ms','relation:deleted_at_ms',
        'relation:activation_token_id','relation:activation_token_generation','relation:termination_reason',
        'relation:terminated_by_player_id','relation:last_change_operation_id',
        'block:low_to_high:status','block:low_to_high:epoch','block:low_to_high:changed_at_ms','block:low_to_high:removed_at_ms',
        'block:high_to_low:status','block:high_to_low:epoch','block:high_to_low:changed_at_ms','block:high_to_low:removed_at_ms',
        'reactivation:state',
        'reactivation:terminated_relation_epoch','reactivation:barrier_revision',
        'reactivation:low_generation_floor','reactivation:high_generation_floor','termination:order_id',
        'termination:captured_epoch','termination:barrier_revision','termination:completed',
        'termination:low_floor_captured','termination:high_floor_captured','termination:quota_released_low',
        'termination:quota_released_high'}) do
      if redis.call('HGET', recordKey, field) == nil then return false, 'CORRUPT_RECORD' end
    end
    for _, field in ipairs({'identity:low_player_id','identity:high_player_id','relation:status','relation:epoch',
        'relation:activated_at_ms','relation:deleted_at_ms','relation:activation_token_generation','relation:termination_reason',
        'relation:terminated_by_player_id','block:low_to_high:status','block:low_to_high:epoch',
        'block:low_to_high:changed_at_ms','block:low_to_high:removed_at_ms','block:high_to_low:status',
        'block:high_to_low:epoch','block:high_to_low:changed_at_ms','block:high_to_low:removed_at_ms',
        'reactivation:state','reactivation:terminated_relation_epoch','reactivation:barrier_revision',
        'reactivation:low_generation_floor','reactivation:high_generation_floor','termination:captured_epoch',
        'termination:barrier_revision','termination:completed','termination:low_floor_captured',
        'termination:high_floor_captured','termination:quota_released_low','termination:quota_released_high'}) do
      if not requireNumericField(recordKey, field, false) then return false, 'CORRUPT_RECORD' end
    end
	local lowPlayer = tonumber(redis.call('HGET', recordKey, 'identity:low_player_id'))
	local highPlayer = tonumber(redis.call('HGET', recordKey, 'identity:high_player_id'))
	if not lowPlayer or not highPlayer or lowPlayer <= 0 or highPlayer <= lowPlayer or
	   tonumber(redis.call('HGET', recordKey, 'relation:status')) > 2 or
	   tonumber(redis.call('HGET', recordKey, 'block:low_to_high:status')) > 2 or
	   tonumber(redis.call('HGET', recordKey, 'block:high_to_low:status')) > 2 or
	   tonumber(redis.call('HGET', recordKey, 'reactivation:state')) > 3 or
	   tonumber(redis.call('HGET', recordKey, 'relation:termination_reason')) > 2 or
	   tonumber(redis.call('HGET', recordKey, 'termination:completed')) > 1 or
	   tonumber(redis.call('HGET', recordKey, 'termination:low_floor_captured')) > 1 or
	   tonumber(redis.call('HGET', recordKey, 'termination:high_floor_captured')) > 1 or
	   tonumber(redis.call('HGET', recordKey, 'termination:quota_released_low')) > 1 or
	   tonumber(redis.call('HGET', recordKey, 'termination:quota_released_high')) > 1 or
	   (authorityID and (redis.call('HGET', recordKey, 'identity:pair_id') ~= authorityID or
	     redis.call('HGET', recordKey, 'relation:id') ~= authorityID)) then return false, 'CORRUPT_RECORD' end
  elseif kind == 'FRIEND_QUOTA' then
    for _, field in ipairs({'_meta:active_count','_meta:reserved_count','_meta:effective_max_friends'}) do
      if not requireNumericField(recordKey, field, false) then return false, 'MALFORMED_COUNTER' end
    end
	local activeCount = tonumber(redis.call('HGET', recordKey, '_meta:active_count'))
	local reservedCount = tonumber(redis.call('HGET', recordKey, '_meta:reserved_count'))
	local effectiveMax = tonumber(redis.call('HGET', recordKey, '_meta:effective_max_friends'))
	if not activeCount or not reservedCount or activeCount < 0 or reservedCount < 0 or
	   not effectiveMax or effectiveMax <= 0 or effectiveMax > 512 or activeCount + reservedCount > 512 or
	   redis.call('HLEN', recordKey) ~= 9 + activeCount + reservedCount * 6 then
	  return false, 'CORRUPT_RECORD'
	end
	local fixed = {['_meta:schema_version']=true,['_meta:authority_layout_version']=true,
	  ['_meta:record_version']=true,['_meta:updated_at_ms']=true,['_meta:logical_bytes']=true,
	  ['_meta:business_state_digest']=true,['_meta:active_count']=true,['_meta:reserved_count']=true,
	  ['_meta:effective_max_friends']=true}
	local activeSeen = 0
	local reservationIDs = {}
	for _, field in ipairs(redis.call('HKEYS', recordKey)) do
	  if fixed[field] then
	  elseif string.sub(field, 1, 7) == 'active:' then
	    local relationID = string.sub(field, 8)
	    if relationID == '' or not strictUnsigned(redis.call('HGET', recordKey, field)) or
	       tonumber(redis.call('HGET', recordKey, field)) <= 0 then return false, 'CORRUPT_RECORD' end
	    activeSeen = activeSeen + 1
	  else
	    local reservationID, suffix = string.match(field, '^reservation:(.+):([^:]+)$')
	    if reservationID and (suffix == 'workflow_id' or suffix == 'relation_id' or suffix == 'target_epoch' or
	        suffix == 'state' or suffix == 'created_at_ms') then
	      reservationIDs[reservationID] = true
	    elseif not string.match(field, '^reservation_by_relation:.+:%d+$') then
	      return false, 'CORRUPT_RECORD'
	    end
	  end
	end
	if activeSeen ~= activeCount then return false, 'CORRUPT_RECORD' end
	local reservationSeen = 0
	for reservationID, _ in pairs(reservationIDs) do
	  reservationSeen = reservationSeen + 1
	  local prefix = 'reservation:' .. reservationID .. ':'
	  local workflowID = redis.call('HGET', recordKey, prefix .. 'workflow_id')
	  local relationID = redis.call('HGET', recordKey, prefix .. 'relation_id')
	  local targetEpoch = redis.call('HGET', recordKey, prefix .. 'target_epoch')
	  local createdAt = redis.call('HGET', recordKey, prefix .. 'created_at_ms')
	  if not workflowID or workflowID == '' or not relationID or relationID == '' or
	     redis.call('HGET', recordKey, prefix .. 'state') ~= '1' or not strictUnsigned(targetEpoch) or
	     tonumber(targetEpoch) <= 0 or not strictUnsigned(createdAt) or
	     redis.call('HGET', recordKey, 'reservation_by_relation:' .. relationID .. ':' .. targetEpoch) ~= reservationID then
	    return false, 'CORRUPT_RECORD'
	  end
	end
	if reservationSeen ~= reservedCount then return false, 'CORRUPT_RECORD' end
  else
	if redis.call('HLEN', recordKey) ~= 18 then return false, 'CORRUPT_RECORD' end
    for _, field in ipairs({'issuer:player_id','token:current_generation','token:current_token_id','token:status',
        'token:purpose','token:format_version','token:key_id','token:issued_at_ms','token:expires_at_ms',
        'token:digest','token:envelope','token:last_operation_id'}) do
      if redis.call('HGET', recordKey, field) == nil then return false, 'CORRUPT_RECORD' end
    end
    for _, field in ipairs({'token:current_generation','token:status','token:purpose','token:format_version'}) do
      if not requireNumericField(recordKey, field, false) then return false, 'CORRUPT_RECORD' end
    end
    for _, field in ipairs({'token:issued_at_ms','token:expires_at_ms'}) do
      if not requireNumericField(recordKey, field, true) then return false, 'CORRUPT_RECORD' end
    end
	local tokenStatus = tonumber(redis.call('HGET', recordKey, 'token:status'))
	if not validLowerHex(redis.call('HGET', recordKey, 'token:digest'), 32) or
	   tokenStatus > 2 or
	   tonumber(redis.call('HGET', recordKey, 'token:purpose')) > 1 or
	   tonumber(redis.call('HGET', recordKey, 'token:format_version')) > 1 or
	   (tokenStatus == 1 and ((redis.call('HGET', recordKey, 'token:current_token_id') or '') == '' or
	     (redis.call('HGET', recordKey, 'token:key_id') or '') == '' or
	     (redis.call('HGET', recordKey, 'token:envelope') or '') == '')) or
	   (authorityID and redis.call('HGET', recordKey, 'issuer:player_id') ~= authorityID) then return false, 'CORRUPT_RECORD' end
  end
  local logicalBytes = canonicalLogicalBytes(recordKey)
  if not logicalBytes or tostring(logicalBytes) ~= redis.call('HGET', recordKey, '_meta:logical_bytes') then
    return false, 'CORRUPT_RECORD'
  end
  return true, ''
end
local function validateAttemptCursor(attemptKey, digest, kind, authorityID)
  local base = 'cursor:' .. digest
  local state = redis.call('HGET', attemptKey, base .. ':state')
  local fixed = {'schema_version','authority_kind','authority_id','commit_identity_digest','ordinal','attempt_id',
    'candidate_hash','state','resolution_id','before_version','expected_record_version','before_digest','updated_at_ms'}
  if not state then
    for _, field in ipairs(fixed) do
      if redis.call('HEXISTS', attemptKey, base .. ':' .. field) == 1 then return false, 'CORRUPT_ATTEMPT' end
    end
    return true, ''
  end
  for _, field in ipairs(fixed) do
    if redis.call('HEXISTS', attemptKey, base .. ':' .. field) ~= 1 then return false, 'CORRUPT_ATTEMPT' end
  end
  if redis.call('HGET', attemptKey, base .. ':schema_version') ~= 'friend-commit-attempt-v1' or
     redis.call('HGET', attemptKey, base .. ':authority_kind') ~= kind or
     redis.call('HGET', attemptKey, base .. ':authority_id') ~= authorityID or
     redis.call('HGET', attemptKey, base .. ':commit_identity_digest') ~= digest or
     not validLowerHex(redis.call('HGET', attemptKey, base .. ':candidate_hash'), 32) or
     not strictUnsigned(redis.call('HGET', attemptKey, base .. ':ordinal')) or
     not strictUnsigned(redis.call('HGET', attemptKey, base .. ':before_version')) or
     not strictUnsigned(redis.call('HGET', attemptKey, base .. ':expected_record_version')) or
     not strictUnsigned(redis.call('HGET', attemptKey, base .. ':updated_at_ms')) then
    return false, 'CORRUPT_ATTEMPT'
  end
  local beforeDigest = redis.call('HGET', attemptKey, base .. ':before_digest')
  if beforeDigest ~= '0' and not validLowerHex(beforeDigest, 32) then return false, 'CORRUPT_ATTEMPT' end
  if state ~= 'OPEN' and state ~= 'COMMITTED' and state ~= 'NOT_COMMITTED' and state ~= 'RECONCILE_REQUIRED' then
    return false, 'CORRUPT_ATTEMPT'
  end
  if redis.call('HGET', attemptKey, base .. ':attempt_id') == '' then return false, 'CORRUPT_ATTEMPT' end
  return true, ''
end
local function markerBase(digest)
  return 'commit_marker:' .. digest .. ':'
end
local function keyInScript(name)
  for i = 1, #KEYS do if KEYS[i] == name then return KEYS[i] end end
  return nil
end
-- FCM2 is a bounded immutable commit proof.  The old FCM1 implementation
-- copied HGETALL snapshots of mutable hashes into the attempt partition.  It
-- was both unbounded and invalidated by a legitimate later authority commit.
-- FCM2 records only fixed fields, hashes/lengths and the exact immutable
-- SourceOrder tuple locators needed to prove operation A before its
-- ResolutionReceipt is written.  It is intentionally staged with complete=0
-- before the first canonical write and sealed only after all writes succeed.
local function markerBytes(fields)
  local total = 0
  for i = 1, #fields, 2 do total = total + #fields[i] + #fields[i + 1] + 16 end
  return total
end
local function markerTag(name)
  local fixed = {
    version='v', digest='d', candidate_hash='c', attempt_id='a', ordinal='o',
    record_exists='e', record_version='rv', record_digest='rd', record_bytes='rb',
    receipt_field='rf', receipt_prefix='rp', receipt_kind='rk', receipt_status='rs',
    receipt_epoch='re', receipt_due_member='rm', receipt_due_score='rx',
    order_count='oc', final_receipt_count='fc', final_receipt_bytes='fb',
    final_pending_work_count='pc', final_pending_work_bytes='pb',
    extra_field='xf', extra_value='xv', extra_due_member='xm', extra_due_score='xx',
    layout='ly', bucket='bu', authority='au'
  }
  if fixed[name] then return fixed[name] end
  local index, field = string.match(name, '^order:(%d+):(.+)$')
  if not index then return nil end
  local suffix = {work_id='i', work_type='t', blob_len='b', blob_sha1='z', work_ref='r',
    payload_hash='p', next_due_at_ms='n', due_member='m', due_score='s',
    schema_version='q', placement_kind='k', bucket_version='v', bucket_id='u',
    source_authority_kind='h', canonical_source_authority_id='j'}
  if not suffix[field] then return nil end
  return 'o' .. index .. suffix[field]
end
local function hashEntryBytes(field, value)
  if not value then return 0 end
  return #field + #value + 16
end
local function markerPut(attemptKey, metaKey, base, fields, maxBytes, maxAggregateBytes)
  if #fields % 2 ~= 0 then return false end
  local proof = ''
  for i = 1, #fields, 2 do
    local name = fields[i]
    if string.sub(name, 1, #base) == base then name = string.sub(name, #base + 1) end
    local tag = markerTag(name)
    if not tag then return false, 'TAG:' .. name end
    proof = proof .. textLP(tag) .. textLP(fields[i + 1])
  end
  local physicalBytes = #base + #('proof') + #proof + #base + #('complete') + 1 + 32
  if physicalBytes > maxBytes then return false, physicalBytes end
  local proofField = base .. 'proof'
  local completeField = base .. 'complete'
  local oldProof = redis.call('HGET', attemptKey, proofField)
  local oldComplete = redis.call('HGET', attemptKey, completeField)
  local delta = hashEntryBytes(proofField, proof) + hashEntryBytes(completeField, '0') -
    hashEntryBytes(proofField, oldProof) - hashEntryBytes(completeField, oldComplete)
  local aggregate = num(metaKey, 'attempt_bytes', 0)
  if delta > 0 and aggregate + delta > maxAggregateBytes then return false, 'AGGREGATE' end
  -- One bounded proof value avoids multiplying the long attempt field prefix
  -- by every immutable field.  It is still fixed-schema LP data, never a
  -- snapshot or an unbounded hash copy.
  redis.call('HSET', attemptKey, proofField, proof, completeField, '0')
  if delta ~= 0 then redis.call('HINCRBY', metaKey, 'attempt_bytes', delta) end
  return true, physicalBytes
end
local function markerValue(attemptKey, base, field)
  local proofBytes = redis.call('HSTRLEN', attemptKey, base .. 'proof')
  if proofBytes <= 0 or proofBytes > 8192 then return nil end
  local proof = redis.call('HGET', attemptKey, base .. 'proof')
  if not proof then return nil end
  local wanted = markerTag(field)
  if not wanted then return nil end
  local pos = 1
  while pos <= #proof do
    local stored, nextPos = readTextLP(proof, pos)
    if not stored then return nil end
    local value; value, nextPos = readTextLP(proof, nextPos)
    if not value then return nil end
    if stored == wanted then return value end
    pos = nextPos
  end
  return nil
end
local function markerManifestOK(attemptKey, base)
  local proofBytes = redis.call('HSTRLEN', attemptKey, base .. 'proof')
  if proofBytes <= 0 or proofBytes > 8192 then return false end
  local proof = redis.call('HGET', attemptKey, base .. 'proof')
  local seen = {}
  local pos = 1
  while pos <= #proof do
    local tag, nextPos = readTextLP(proof, pos)
    if not tag or tag == '' or seen[tag] then return false end
    local _, afterValue = readTextLP(proof, nextPos)
    if not afterValue then return false end
    seen[tag] = true
    pos = afterValue
  end
  if pos ~= #proof + 1 then return false end
  local fixed = {'v','d','c','a','o','e','rv','rd','rb','rf','rp','rk','rs','re','rm','rx','oc',
    'fc','fb','pc','pb','xf','xv','xm','xx','ly','bu','au'}
  local fixedSet = {}
  for _, tag in ipairs(fixed) do
    if not seen[tag] then return false end
    fixedSet[tag] = true
  end
  local orderCount = tonumber(markerValue(attemptKey, base, 'order_count') or '-1')
  if not orderCount or orderCount < 0 or orderCount > 2 or orderCount ~= math.floor(orderCount) then return false end
  local expectedCount = #fixed + orderCount * 7
  local actualCount = 0
  for tag, _ in pairs(seen) do
    actualCount = actualCount + 1
    local index, suffix = string.match(tag, '^o(%d+)([itbzrpn])$')
    if not string.match(tag, '^o%d+[itbzrpn]$') and not fixedSet[tag] then return false end
    if index and (tonumber(index) < 1 or tonumber(index) > orderCount or suffix == '') then return false end
  end
  if actualCount ~= expectedCount then return false end
  for i = 1, orderCount do
    for _, suffix in ipairs({'i','t','b','z','r','p','n'}) do
      if not seen['o' .. tostring(i) .. suffix] then return false end
    end
  end
  return true
end
local function markerBasic(attemptKey, markerDigest, attemptID, ordinal, candidateHash)
  local base = markerBase(markerDigest)
  return redis.call('HGET', attemptKey, base .. 'complete') == '1' and markerManifestOK(attemptKey, base) and
    markerValue(attemptKey, base, 'version') == 'FCM2' and
    markerValue(attemptKey, base, 'digest') == markerDigest and
    markerValue(attemptKey, base, 'attempt_id') == attemptID and
    markerValue(attemptKey, base, 'ordinal') == tostring(ordinal) and
    markerValue(attemptKey, base, 'candidate_hash') == candidateHash and
    strictUnsigned(markerValue(attemptKey, base, 'order_count'))
end
local function markerReceiptHeader(value, prefix, kind, status, epoch, attemptID, ordinal, candidateHash)
  if not value or not receiptAttemptMatches(value, prefix, attemptID, candidateHash, ordinal) then return false end
  local pos = #prefix + 4
  local fields = {}
  for i = 1, 10 do
    local part, nextPos = readTextLP(value, pos)
    if not part then return false end
    fields[i] = part; pos = nextPos
  end
  return fields[1] == kind and fields[2] == status and fields[4] == tostring(epoch) and pos == #value + 1
end
local function resolutionReceiptMatches(value, prefix, kind, epoch, attemptID, ordinal, candidateHash)
  if not value or not receiptAttemptMatches(value, prefix, attemptID, candidateHash, ordinal) then return false end
  local pos = #prefix + 4
  local fields = {}
  for i = 1, 10 do
    local part, nextPos = readTextLP(value, pos)
    if not part then return false end
    fields[i] = part; pos = nextPos
  end
  return fields[1] == kind and fields[4] == tostring(epoch) and pos == #value + 1
end
local function receiptStoredPrefix(value)
  if not value or #value < 8 then return nil end
  local function u32(position)
    local a, b, c, d = string.byte(value, position, position + 3)
    if not a or not b or not c or not d then return nil end
    return ((a * 256 + b) * 256 + c) * 256 + d
  end
  local identityBytes = u32(1)
  if not identityBytes then return nil end
  local payloadLengthAt = 5 + identityBytes
  local payloadBytes = u32(payloadLengthAt)
  if not payloadBytes then return nil end
  local prefixLength = payloadLengthAt + 4 + payloadBytes - 1
  if prefixLength >= #value then return nil end
  return string.sub(value, 1, prefixLength)
end
local validateSourceOrder
local function markerVerify(attemptKey, digest, markerDigest, attemptID, ordinal, candidateHash, recordKey, receiptKey,
    receiptDueKey, metaKey, sourceDueKey, extraKey)
  if not markerBasic(attemptKey, markerDigest, attemptID, ordinal, candidateHash) then return false end
  local base = markerBase(markerDigest)
  -- complete=1 is written only after the canonical record, immutable orders,
  -- receipt/due facts and counters have all been applied.  Re-comparing the
  -- mutable record version/digest or aggregate counters here would make an
  -- exact proof for attempt A disappear after a legitimate later commit B.
  -- The immutable Receipt still has to bind this exact attempt/candidate.
  local receiptField = markerValue(attemptKey, base, 'receipt_field')
  local receipt = receiptField and redis.call('HGET', receiptKey, receiptField) or nil
  if not receipt or not markerReceiptHeader(receipt, markerValue(attemptKey, base, 'receipt_prefix') or '',
      markerValue(attemptKey, base, 'receipt_kind') or '', markerValue(attemptKey, base, 'receipt_status') or '',
      markerValue(attemptKey, base, 'receipt_epoch') or '0',
      attemptID, ordinal, candidateHash) then return false end
  local dueMember = markerValue(attemptKey, base, 'receipt_due_member')
  local dueScore = markerValue(attemptKey, base, 'receipt_due_score')
  if not dueMember or not dueScore or tostring(redis.call('ZSCORE', receiptDueKey, dueMember) or '') ~= dueScore then return false end
  local orderCount = tonumber(markerValue(attemptKey, base, 'order_count') or '-1')
  if not orderCount or orderCount < 0 or orderCount > 2 then return false end
  for i = 1, orderCount do
    local orderKey = KEYS[8 + i]
    if not sourceDueKey or sourceDueKey == '' or not orderKey or friendKeyType(orderKey) ~= 'hash' then return false end
    local payload = redis.call('HGET', orderKey, 'typed_payload')
    local shape = 'BASE'
    local storedWorkType = markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':work_type')
    if storedWorkType == 'FRIEND_ACTION_DISPATCH_ORDER' then shape = 'DISPATCH'
    elseif storedWorkType == 'RESULT_PUBLISH_ORDER' and markerValue(attemptKey, base, 'receipt_kind') == 'ACTION_ADMISSION' and
        markerValue(attemptKey, base, 'receipt_status') == 'SUCCEEDED' then shape = 'ACTION_RESULT' end
    local orderOK = validateSourceOrder(orderKey, sourceDueKey,
      markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':work_id'), storedWorkType, payload or '',
      markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':work_ref'),
      markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':payload_hash'),
      markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':next_due_at_ms'),
      markerValue(attemptKey, base, 'layout'), markerValue(attemptKey, base, 'bucket'),
      markerValue(attemptKey, base, 'authority'), shape)
    if not orderOK or not payload or #payload ~= tonumber(markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':blob_len') or '-1') or
       redis.sha1hex(payload) ~= markerValue(attemptKey, base, 'order:' .. tostring(i) .. ':blob_sha1') then return false end
    -- The SourceOrder schema freezes its due membership and initial worker
    -- state/version; any divergence makes this marker unusable as proof.
  end
  local extraField = markerValue(attemptKey, base, 'extra_field')
  local requiresExtra = markerValue(attemptKey, base, 'receipt_kind') == 'QUOTA_REMOVE'
  if requiresExtra and (not extraKey or extraKey == '' or not extraField) then return false end
  if extraKey and extraKey ~= '' and extraField then
    if redis.call('HGET', extraKey, extraField) ~= markerValue(attemptKey, base, 'extra_value') then return false end
    local extraMember = markerValue(attemptKey, base, 'extra_due_member')
    local extraScore = markerValue(attemptKey, base, 'extra_due_score')
    if not extraMember or not extraScore or tostring(redis.call('ZSCORE', receiptDueKey, extraMember) or '') ~= extraScore then return false end
  end
  return true
end
local function writeCommitMarker(attemptKey, markerDigest, attemptID, ordinal, candidateHash,
    recordKey, recordExists, recordVersion, recordDigest, recordBytes, receiptKey, receiptField, receiptPrefix,
    receiptBlob, receiptKind, receiptStatus, receiptEpoch, receiptDueKey, receiptDueMember, receiptDueScore,
    metaKey, sourceDueKey, orderKeys, orderTuples, extraKey, extraField, extraValue, extraDueMember, extraDueScore,
    sourceLayout, sourceBucket, sourceAuthorityKind, sourceAuthorityID,
    finalReceiptCount, finalReceiptBytes, finalPendingCount, finalPendingBytes, maxBytes, maxAggregateBytes)
  local base = markerBase(markerDigest)
  local fields = {base .. 'version','FCM2', base .. 'digest',markerDigest, base .. 'candidate_hash',candidateHash,
    base .. 'attempt_id',attemptID, base .. 'ordinal',tostring(ordinal),
    base .. 'record_exists',recordExists and '1' or '0', base .. 'record_version',tostring(recordVersion),
    base .. 'record_digest',recordDigest, base .. 'record_bytes',tostring(recordBytes), base .. 'receipt_field',receiptField,
    base .. 'receipt_prefix',receiptPrefix, base .. 'receipt_kind',receiptKind, base .. 'receipt_status',receiptStatus,
    base .. 'receipt_epoch',tostring(receiptEpoch),
    base .. 'receipt_due_member',receiptDueMember, base .. 'receipt_due_score',tostring(receiptDueScore),
    base .. 'order_count',tostring(#orderKeys), base .. 'layout',sourceLayout or '',
    base .. 'bucket',sourceBucket or '', base .. 'authority',sourceAuthorityID or '',
    base .. 'final_receipt_count',tostring(finalReceiptCount), base .. 'final_receipt_bytes',tostring(finalReceiptBytes),
    base .. 'final_pending_work_count',tostring(finalPendingCount), base .. 'final_pending_work_bytes',tostring(finalPendingBytes),
    base .. 'extra_field',extraField or '', base .. 'extra_value',extraValue or '',
    base .. 'extra_due_member',extraDueMember or '', base .. 'extra_due_score',extraDueScore and tostring(extraDueScore) or ''}
  for i, orderKey in ipairs(orderKeys) do
    local p = base .. 'order:' .. tostring(i) .. ':'
    local tuple = orderTuples[i]
    fields[#fields + 1] = p .. 'work_id'; fields[#fields + 1] = tuple[1]
    fields[#fields + 1] = p .. 'work_type'; fields[#fields + 1] = tuple[2]
    fields[#fields + 1] = p .. 'blob_len'; fields[#fields + 1] = tostring(#tuple[3])
    fields[#fields + 1] = p .. 'blob_sha1'; fields[#fields + 1] = redis.sha1hex(tuple[3])
    fields[#fields + 1] = p .. 'work_ref'; fields[#fields + 1] = tuple[4]
    fields[#fields + 1] = p .. 'payload_hash'; fields[#fields + 1] = tuple[5]
    fields[#fields + 1] = p .. 'next_due_at_ms'; fields[#fields + 1] = tuple[6]
  end
  local markerOK, markerSize = markerPut(attemptKey, metaKey, base, fields, maxBytes, maxAggregateBytes)
  if not markerOK then return false, markerSize end
  return true, markerSize
end
local function completeCommitMarker(attemptKey, markerDigest)
  redis.call('HSET', attemptKey, markerBase(markerDigest) .. 'complete', '1')
end
local function deleteCommitMarker(attemptKey, metaKey, markerDigest)
  local base = markerBase(markerDigest)
  local proofField = base .. 'proof'
  local completeField = base .. 'complete'
  local proof = redis.call('HGET', attemptKey, proofField)
  local complete = redis.call('HGET', attemptKey, completeField)
  local released = hashEntryBytes(proofField, proof) + hashEntryBytes(completeField, complete)
  if released == 0 then return true end
  local aggregate = num(metaKey, 'attempt_bytes', 0)
  if aggregate < released then return false end
  redis.call('HDEL', attemptKey, proofField, completeField)
  redis.call('HINCRBY', metaKey, 'attempt_bytes', -released)
  return true
end
validateSourceOrder = function(orderKey, dueKey, workID, workType, blob, workRef, payloadHash, dueMS,
    layout, bucketID, authorityID, shape)
  local function validStableID(value)
    return value and #value >= 8 and #value <= 64 and string.match(value, '^sid1_[A-Za-z0-9_-]+$') ~= nil
  end
  local function validWorkRef(value)
    return value and #value >= 8 and #value <= 256 and string.match(value, '^wr1_[A-Za-z0-9_-]+$') ~= nil
  end
  local function validTypedPayload(value)
    if not value or #value < 4 or string.sub(value, 1, 4) ~= 'TLP1' then return false end
    local pos = 5
    while pos <= #value do
      local colon = string.find(value, ':', pos, true)
      if not colon then return false end
      local ntext = string.sub(value, pos, colon - 1)
      if not string.match(ntext, '^%d+$') then return false end
      local n = tonumber(ntext)
      if not n then return false end
      pos = colon + 1 + n
      if pos > #value + 1 then return false end
    end
    return pos == #value + 1
  end
  local function typedPayloadParts(value)
    if not validTypedPayload(value) then return nil end
    local parts = {}
    local pos = 5
    while pos <= #value do
      local colon = string.find(value, ':', pos, true)
      local n = tonumber(string.sub(value, pos, colon - 1))
      local first = colon + 1
      parts[#parts + 1] = string.sub(value, first, first + n - 1)
      pos = first + n
    end
    return parts
  end
  local function validDue(value)
    if not value or not string.match(value, '^%d+$') then return false end
    local n = tonumber(value)
    return n ~= nil and n >= 0 and n <= 9223372036854775807 and tostring(math.floor(n)) == value
  end
  -- Validate the complete six-field tuple before even checking whether the
  -- order HASH exists.  MISSING is not permission to defer malformed data.
  if not validStableID(workID) or not workType or not string.match(workType, '^[A-Z0-9_]+$') or
     #workType > 64 or not validTypedPayload(blob) or not validWorkRef(workRef) or
     not payloadHash or #payloadHash ~= 64 or string.match(payloadHash, '^[0-9a-f]+$') == nil or
     not validDue(dueMS) or #blob > 16384 or #workRef > 1024 then
    return false, 'WORK_RECONCILE'
  end
  local orderType = friendKeyType(orderKey)
  local dueType = friendKeyType(dueKey)
  if dueType ~= 'none' and dueType ~= 'zset' then return false, 'CORRUPT_RECORD' end
  if orderType == 'none' then
    if dueType == 'zset' and redis.call('ZSCORE', dueKey, workRef) then return false, 'WORK_RECONCILE' end
    return true, 'MISSING'
  end
  if orderType ~= 'hash' then return false, 'CORRUPT_RECORD' end
  local fieldCount = redis.call('HLEN', orderKey)
  if shape == 'DISPATCH' then
    if fieldCount ~= 37 then return false, 'WORK_RECONCILE' end
  elseif shape == 'ACTION_RESULT' then
    if fieldCount ~= 23 or not redis.call('HEXISTS', orderKey, 'dispatch_order_id') then
      return false, 'WORK_RECONCILE'
    end
  elseif shape == 'ACTION_RESULT_OPTIONAL' then
    if fieldCount ~= 22 and (fieldCount ~= 23 or not redis.call('HEXISTS', orderKey, 'dispatch_order_id')) then
      return false, 'WORK_RECONCILE'
    end
  elseif shape == 'BASE' then
    if fieldCount ~= 22 then return false, 'WORK_RECONCILE' end
  else
    return false, 'WORK_RECONCILE'
  end
  local expected = {
    schema_version='friend-source-order-v1', work_id=workID, work_type=workType,
    placement_kind='SOURCE_PERSISTENCE_BUCKET', bucket_version=layout, bucket_id=bucketID,
    source_authority_kind='FRIEND_PAIR', canonical_source_authority_id=authorityID,
    typed_payload=blob, work_ref=workRef, payload_hash=payloadHash, next_due_at_ms=dueMS}
  for field, value in pairs(expected) do
    if redis.call('HGET', orderKey, field) ~= value then return false, 'WORK_RECONCILE' end
  end
  for _, field in ipairs({'state', 'work_version', 'retry_count', 'lease_epoch', 'lease_holder_id',
      'lease_token_hash', 'lease_expire_at_ms', 'created_at_ms', 'updated_at_ms', 'completed_at_ms'}) do
    if not redis.call('HGET', orderKey, field) then return false, 'WORK_RECONCILE' end
  end
  if redis.call('HGET', orderKey, 'state') ~= 'PENDING' or redis.call('HGET', orderKey, 'work_version') ~= '1' or
     redis.call('HGET', orderKey, 'retry_count') ~= '0' or redis.call('HGET', orderKey, 'lease_epoch') ~= '0' or
     redis.call('HGET', orderKey, 'lease_holder_id') ~= '' or redis.call('HGET', orderKey, 'lease_token_hash') ~= '' or
     redis.call('HGET', orderKey, 'lease_expire_at_ms') ~= '0' or redis.call('HGET', orderKey, 'completed_at_ms') ~= '0' then
    return false, 'WORK_RECONCILE'
  end
  local created = redis.call('HGET', orderKey, 'created_at_ms')
  local updated = redis.call('HGET', orderKey, 'updated_at_ms')
  if not strictUnsigned(created) or not strictUnsigned(updated) or tonumber(created) <= 0 or created ~= updated then
    return false, 'WORK_RECONCILE'
  end
  if dueType ~= 'zset' then return false, 'WORK_RECONCILE' end
  local score = redis.call('ZSCORE', dueKey, workRef)
  if not score or tonumber(score) ~= tonumber(dueMS) or tostring(math.floor(tonumber(score))) ~= dueMS then return false, 'WORK_RECONCILE' end
  if shape == 'DISPATCH' then
    for _, field in ipairs({'permit_id', 'relation_id', 'relation_epoch', 'visitor_player_id',
        'owner_player_id', 'target_farm_id', 'operation_id', 'scope', 'permit_payload_hash',
        'target_binding_hash', 'issued_at_authority_ms', 'expire_at_authority_ms',
        'execution_accepted_at_authority_ms', 'workflow_deadline_at_ms', 'dispatch_order_id'}) do
      if not redis.call('HGET', orderKey, field) then return false, 'WORK_RECONCILE' end
    end
    local parts = typedPayloadParts(blob)
    local issued = redis.call('HGET', orderKey, 'issued_at_authority_ms')
    local expires = redis.call('HGET', orderKey, 'expire_at_authority_ms')
    local accepted = redis.call('HGET', orderKey, 'execution_accepted_at_authority_ms')
    local workflowDeadline = redis.call('HGET', orderKey, 'workflow_deadline_at_ms')
    if not parts or #parts ~= 14 or parts[1] ~= 'ACTION_DISPATCH' or parts[2] ~= authorityID or
       redis.call('HGET', orderKey, 'relation_id') ~= parts[2] or redis.call('HGET', orderKey, 'relation_epoch') ~= parts[4] or
       redis.call('HGET', orderKey, 'permit_id') ~= parts[5] or redis.call('HGET', orderKey, 'permit_payload_hash') ~= parts[6] or
       redis.call('HGET', orderKey, 'target_binding_hash') ~= parts[7] or redis.call('HGET', orderKey, 'visitor_player_id') ~= parts[8] or
       redis.call('HGET', orderKey, 'owner_player_id') ~= parts[9] or redis.call('HGET', orderKey, 'target_farm_id') ~= parts[10] or
       redis.call('HGET', orderKey, 'scope') ~= parts[11] or redis.call('HGET', orderKey, 'operation_id') ~= parts[12] or
       redis.call('HGET', orderKey, 'dispatch_order_id') ~= workID or not strictUnsigned(parts[4]) or tonumber(parts[4]) <= 0 or
       not strictUnsigned(parts[11]) or not strictUnsigned(parts[13]) or tonumber(parts[13]) <= 0 or
       not strictUnsigned(parts[14]) or tonumber(parts[14]) <= tonumber(parts[13]) or
       not strictUnsigned(issued) or not strictUnsigned(expires) or not strictUnsigned(accepted) or not strictUnsigned(workflowDeadline) or
       tonumber(issued) <= 0 or accepted ~= issued or tonumber(expires) - tonumber(issued) ~= tonumber(parts[13]) or
       tonumber(workflowDeadline) - tonumber(issued) ~= tonumber(parts[14]) then
      return false, 'WORK_RECONCILE'
    end
  elseif shape == 'ACTION_RESULT' or shape == 'ACTION_RESULT_OPTIONAL' then
    local parts = typedPayloadParts(blob)
    if not parts or #parts ~= 11 or parts[1] ~= 'ACTION_RESULT' or parts[2] ~= authorityID or parts[4] ~= '1' or
       (fieldCount == 23 and redis.call('HGET', orderKey, 'dispatch_order_id') ~= parts[6]) or
       not strictUnsigned(parts[7]) or not strictUnsigned(parts[8]) or not strictUnsigned(parts[9]) or
       #parts[10] ~= 64 or not string.match(parts[10], '^[0-9a-f]+$') or
       #parts[11] ~= 64 or not string.match(parts[11], '^[0-9a-f]+$') then return false, 'WORK_RECONCILE' end
  end
  return true, 'EXISTING'
end
local function stagedByteDelta(key)
  return stagedDeltaByKey[key] or 0
end
local function stagedNewFieldCount(key)
  return stagedNewCountByKey[key] or 0
end
local function stagedFinal(key, field)
  local values = stagedFields[key]
  local value = values and values[field]
  if value then return value.final end
  return nil
end
local function sourceOrderLogicalBytes(workID, workType, blob, workRef, payloadHash, dueMS)
  return 512 + 16 + #workID + 16 + #workType + 16 + #blob + 16 + #workRef +
    16 + #payloadHash + 16 + #dueMS
end
`

// Prepare is deliberately only a bounded recovery cursor write.  The
// business mutation remains in its own fixed script and cannot be replaced by
// a client-side lock or a blind second attempt.
const friendPrepareSource = friendLuaPrelude + `
-- KEYS: authority record, attempt partition, attempt-due, receipt-due, bucket-meta,
--       optional source-due and exact source-order keys
-- ARGV: tag,digest,ordinal,attempt_id,candidate_hash,due_member,retention_ms,
--       expected_record_version,refresh_marker,authority_kind,layout,bucket,authority_id,
--       max_attempt_count,max_attempt_aggregate_bytes,max_cursor_bytes,resolution_reserve_bytes,
--       source_order_count, then tuple values plus dispatch-validation flag, allow_create
if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
   not friendKeyTypeOK(KEYS[3], 'zset') or not friendKeyTypeOK(KEYS[4], 'zset') or
   not friendKeyTypeOK(KEYS[5], 'hash') then
  return {'CORRUPT_RECORD'}
end
local metaOK, metaWhy = validateBucketMeta(KEYS[5])
if not metaOK then return {metaWhy} end
local digest = ARGV[2]
local ordinal = tonumber(ARGV[3])
local base = cursorBase(digest)
if not validLowerHex(digest, 32) or not ordinal or ordinal <= 0 or ARGV[4] == '' or
   not validLowerHex(ARGV[5], 32) or ARGV[10] == '' or ARGV[11] == '' or ARGV[12] == '' or ARGV[13] == '' then
  return {'CONTRACT_ERROR'}
end
local maxAttemptCount = tonumber(ARGV[14])
local maxAttemptBytes = tonumber(ARGV[15])
local maxCursorBytes = tonumber(ARGV[16])
local resolutionReserveBytes = tonumber(ARGV[17])
local sourceOrderCount = tonumber(ARGV[18])
if not maxAttemptCount or maxAttemptCount <= 0 or not maxAttemptBytes or maxAttemptBytes <= 0 or
   not maxCursorBytes or maxCursorBytes <= 0 or not resolutionReserveBytes or resolutionReserveBytes <= 0 or
   not sourceOrderCount or sourceOrderCount < 0 or sourceOrderCount > 2 or
   #KEYS ~= 5 + (sourceOrderCount > 0 and sourceOrderCount + 1 or 0) or #ARGV ~= 19 + sourceOrderCount * 7 or
   (ARGV[#ARGV] ~= '0' and ARGV[#ARGV] ~= '1') then
  return {'CONTRACT_ERROR'}
end
local authorityOK, authorityWhy = validateBucketMetaAuthority(
  KEYS[5], KEYS[1], ARGV[10], ARGV[11], ARGV[12], ARGV[#ARGV] == '1')
if not authorityOK then return {authorityWhy} end
local countOK, countWhy = validateBucketMetaCounts(KEYS[5], KEYS[3], KEYS[4], sourceOrderCount > 0 and KEYS[6] or '', ARGV[10] ~= 'FRIEND_PAIR')
if not countOK then return {countWhy} end
local canonicalOK, canonicalWhy = validateCanonicalRecord(KEYS[1], ARGV[10], ARGV[13])
if not canonicalOK then return {canonicalWhy} end
if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('HGET', KEYS[1], '_meta:authority_layout_version') ~= ARGV[11] then
  return {'LAYOUT_MISMATCH'}
end
local attemptOK, attemptWhy = validateAttemptCursor(KEYS[2], digest, ARGV[10], ARGV[13])
if not attemptOK then return {attemptWhy} end
if sourceOrderCount > 0 then
  if not friendKeyTypeOK(KEYS[6], 'zset') then return {'CORRUPT_RECORD'} end
  local pos = 19
  for i = 1, sourceOrderCount do
    local orderOK, orderWhy = validateSourceOrder(KEYS[6 + i], KEYS[6], ARGV[pos], ARGV[pos + 1],
      ARGV[pos + 2], ARGV[pos + 3], ARGV[pos + 4], ARGV[pos + 5], ARGV[11], ARGV[12], ARGV[13],
      ARGV[pos + 6])
    if not orderOK then return {orderWhy} end
    pos = pos + 7
  end
end
local state = redis.call('HGET', KEYS[2], base .. ':state')
if state then
  local oldOrdinal = tonumber(redis.call('HGET', KEYS[2], base .. ':ordinal') or '0')
  local oldAttempt = redis.call('HGET', KEYS[2], base .. ':attempt_id') or ''
  local oldHash = redis.call('HGET', KEYS[2], base .. ':candidate_hash') or ''
  if state == 'OPEN' and oldOrdinal == ordinal and oldAttempt == ARGV[4] and oldHash == ARGV[5] then
    return {'ALREADY_PREPARED', redis.call('HGET', KEYS[2], base .. ':before_version') or '0', redis.call('HGET', KEYS[2], base .. ':before_digest') or '0'}
  end
  if state == 'COMMITTED' and oldOrdinal == ordinal and oldAttempt == ARGV[4] and oldHash == ARGV[5] then
    return {'ALREADY_COMMITTED'}
  end
  if state == 'NOT_COMMITTED' and (oldHash == ARGV[5] or ARGV[9] == 'REFRESH_VERSION') and ordinal == oldOrdinal + 1 then
    -- A resolved attempt may be replaced only by the next ordinal of the
    -- same CommitIdentity and candidate payload.  The caller cannot jump or
    -- change its candidate after an unknown outcome.
  else
    return {'ATTEMPT_CONFLICT', state}
  end
end
local beforeVersion = redis.call('HGET', KEYS[1], '_meta:record_version') or '0'
local beforeDigest = redis.call('HGET', KEYS[1], '_meta:business_state_digest') or '0'
local cursorValues = {
  schema_version='friend-commit-attempt-v1', authority_kind=ARGV[10], authority_id=ARGV[13],
  commit_identity_digest=digest, ordinal=ARGV[3], attempt_id=ARGV[4], candidate_hash=ARGV[5], state='OPEN',
  resolution_id='', before_version=beforeVersion, expected_record_version=ARGV[8] or beforeVersion,
  before_digest=beforeDigest, updated_at_ms=tostring(now)}
local cursorBytes = 0
for field, value in pairs(cursorValues) do
  local fullField = base .. ':' .. field
  cursorBytes = cursorBytes + hashEntryBytes(fullField, value)
end
if cursorBytes > maxCursorBytes then return {'CAPACITY_ATTEMPT_CURSOR'} end
local newCursor = state == false or state == nil
local countDelta = newCursor and 1 or 0
-- Charge a fixed cursor reservation rather than mutable field deltas. Cursor
-- state and resolution_id grow after Prepare; the reservation keeps those
-- later transitions inside the original bounded allocation.
local byteDelta = newCursor and (maxCursorBytes + resolutionReserveBytes) or 0
local currentAttemptCount = num(KEYS[5], 'attempt_count', 0)
local currentAttemptBytes = num(KEYS[5], 'attempt_bytes', 0)
if currentAttemptCount + countDelta > maxAttemptCount or currentAttemptBytes + byteDelta > maxAttemptBytes then
  return {'CAPACITY_ATTEMPT'}
end
prepareMeta(KEYS[5], ARGV[10], ARGV[11], ARGV[12])
redis.call('HSET', KEYS[2],
  base .. ':schema_version', 'friend-commit-attempt-v1',
  base .. ':authority_kind', ARGV[10],
  base .. ':authority_id', ARGV[13],
  base .. ':commit_identity_digest', digest,
  base .. ':ordinal', ARGV[3],
  base .. ':attempt_id', ARGV[4],
  base .. ':candidate_hash', ARGV[5],
  base .. ':state', 'OPEN',
  base .. ':resolution_id', '',
  base .. ':before_version', beforeVersion,
  base .. ':expected_record_version', ARGV[8] or beforeVersion,
  base .. ':before_digest', beforeDigest,
  base .. ':updated_at_ms', tostring(now))
if countDelta ~= 0 then redis.call('HINCRBY', KEYS[5], 'attempt_count', countDelta) end
if byteDelta ~= 0 then redis.call('HINCRBY', KEYS[5], 'attempt_bytes', byteDelta) end
redis.call('HSET', KEYS[5], 'last_updated_at_ms', tostring(now))
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[7]), ARGV[6])
return {'PREPARED', beforeVersion, beforeDigest}
`

const friendResolveSource = friendLuaPrelude + `
-- KEYS: authority record, receipt partition, attempt partition,
--       resolution partition, attempt-due, receipt-due, bucket-meta,
--       source-due and source-order keys copied from the business script
-- ARGV: tag,digest,receipt_field,candidate_prefix,attempt_id,ordinal,
--       candidate_hash,resolution_field,resolution_id,attempt_due_member,
--       resolution_mode (PRECOMMIT_NOT_COMMITTED is used only after a script
--       returned a proven pre-mutation contract code; empty carries no proof)
--       authority_kind,layout,bucket,authority_id,source_due_key,extra_key,marker_digest
local digest = ARGV[2]
if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
   not friendKeyTypeOK(KEYS[3], 'hash') or not friendKeyTypeOK(KEYS[4], 'hash') or
   not friendKeyTypeOK(KEYS[5], 'zset') or not friendKeyTypeOK(KEYS[6], 'zset') or
   friendKeyType(KEYS[7]) ~= 'hash' then
  return {'CORRUPT_RECORD'}
end
local metaOK, metaWhy = validateBucketMeta(KEYS[7])
if not metaOK then return {metaWhy} end
local base = cursorBase(digest)
local resolutionMode = ARGV[11]
local markerSourceDueKey = ARGV[#ARGV - 2] or ''
local markerExtraKey = ARGV[#ARGV - 1] or ''
local markerDigest = ARGV[#ARGV]
local authorityMetaOK, authorityMetaWhy = validateBucketMetaAuthority(
  KEYS[7], KEYS[1], ARGV[12], ARGV[13], ARGV[14], true)
if not authorityMetaOK then return {authorityMetaWhy} end
local metaCountOK, metaCountWhy = validateBucketMetaCounts(KEYS[7], KEYS[5], KEYS[6], markerSourceDueKey, ARGV[12] ~= 'FRIEND_PAIR')
if not metaCountOK then return {metaCountWhy} end
local canonicalOK, canonicalWhy = validateCanonicalRecord(KEYS[1], ARGV[12], ARGV[15])
if not canonicalOK then return {'RECONCILE_REQUIRED'} end
local oldResolution = redis.call('HGET', KEYS[4], ARGV[8])
if oldResolution then
	local same, oldStatus, oldMarker, oldReceiptField, oldKind, oldEpoch =
		resolutionMatches(oldResolution, ARGV[9], ARGV[5], ARGV[7], tonumber(ARGV[6]))
	if same then
		-- A ResolutionReceipt is immutable evidence for this ordinal.  Do not
		-- re-compare it to operation B's later mutable record/meta/order state.
		if oldMarker ~= markerDigest then return {'RECONCILE_REQUIRED'} end
		if oldStatus == 'COMMITTED' then
			if oldReceiptField ~= ARGV[3] or not resolutionReceiptMatches(
				redis.call('HGET', KEYS[2], oldReceiptField), ARGV[4], oldKind, oldEpoch,
				ARGV[5], tonumber(ARGV[6]), ARGV[7]) then return {'RECONCILE_REQUIRED'} end
		end
		if oldStatus ~= 'RECONCILE_REQUIRED' then
			if not deleteCommitMarker(KEYS[3], KEYS[7], markerDigest) then return {'RECONCILE_REQUIRED'} end
			redis.call('HSET', KEYS[3], base .. ':state', oldStatus, base .. ':resolution_id', ARGV[9])
			redis.call('ZREM', KEYS[5], ARGV[10])
		end
		return {'RESOLUTION_REPLAY', oldStatus, oldResolution}
	end
	return {'RECONCILE_REQUIRED'}
end
local existing = redis.call('HGET', KEYS[2], ARGV[3])
if existing then
  if string.sub(existing, 1, #ARGV[4]) == ARGV[4] then
    if not receiptCandidateMatches(existing, ARGV[4], ARGV[7]) then return {'IDEMPOTENCY_CONFLICT'} end
    if not receiptAttemptMatches(existing, ARGV[4], ARGV[5], ARGV[7], tonumber(ARGV[6])) then
      return {'RECONCILE_REQUIRED'}
    end
	local existingCursorState = redis.call('HGET', KEYS[3], base .. ':state')
	if existingCursorState ~= 'COMMITTED' then return {'RECONCILE_REQUIRED'} end
	if not markerVerify(KEYS[3], digest, markerDigest, ARGV[5], tonumber(ARGV[6]), ARGV[7],
		KEYS[1], KEYS[2], KEYS[6], KEYS[7], markerSourceDueKey, markerExtraKey) then
		return {'RECONCILE_REQUIRED'}
	end
	    local markerBaseKey = markerBase(markerDigest)
	    local resolution = resolutionValue('COMMITTED', ARGV[5], ARGV[7], ARGV[9], tonumber(ARGV[6]), markerDigest,
	      markerValue(KEYS[3], markerBaseKey, 'receipt_field') or ARGV[3],
	      markerValue(KEYS[3], markerBaseKey, 'receipt_kind') or '',
	      markerValue(KEYS[3], markerBaseKey, 'receipt_epoch') or '0')
		if redis.call('HSETNX', KEYS[4], ARGV[8], resolution) ~= 1 or redis.call('HGET', KEYS[4], ARGV[8]) ~= resolution then
			return {'RECONCILE_REQUIRED'}
		end
		if not deleteCommitMarker(KEYS[3], KEYS[7], markerDigest) then return {'RECONCILE_REQUIRED'} end
		redis.call('HSET', KEYS[3], base .. ':state', 'COMMITTED')
		redis.call('HSET', KEYS[3], base .. ':resolution_id', ARGV[9])
		redis.call('ZREM', KEYS[5], ARGV[10])
		return {'COMMITTED', resolution, existing}
  end
  return {'IDEMPOTENCY_CONFLICT'}
end
local cursorState = redis.call('HGET', KEYS[3], base .. ':state')
if not cursorState then return {'ATTEMPT_REQUIRED'} end
if cursorState ~= 'OPEN' and cursorState ~= 'NOT_COMMITTED' and cursorState ~= 'COMMITTED' then
	return {'ATTEMPT_FENCED_' .. cursorState}
end
if redis.call('HGET', KEYS[3], base .. ':attempt_id') ~= ARGV[5] or
   redis.call('HGET', KEYS[3], base .. ':ordinal') ~= ARGV[6] or
   redis.call('HGET', KEYS[3], base .. ':candidate_hash') ~= ARGV[7] then
  return {'ATTEMPT_CONFLICT'}
end
local status = 'RECONCILE_REQUIRED'
if cursorState == 'COMMITTED' then
	if markerVerify(KEYS[3], digest, markerDigest, ARGV[5], tonumber(ARGV[6]), ARGV[7],
		KEYS[1], KEYS[2], KEYS[6], KEYS[7], markerSourceDueKey, markerExtraKey) then
			status = 'COMMITTED'
	end
elseif resolutionMode == 'PRECOMMIT_NOT_COMMITTED' then
  status = 'NOT_COMMITTED'
end
local markerBaseKey = markerBase(markerDigest)
local resolution = resolutionValue(status, ARGV[5], ARGV[7], ARGV[9], tonumber(ARGV[6]), markerDigest,
  status == 'COMMITTED' and (markerValue(KEYS[3], markerBaseKey, 'receipt_field') or ARGV[3]) or '',
  status == 'COMMITTED' and (markerValue(KEYS[3], markerBaseKey, 'receipt_kind') or '') or '',
  status == 'COMMITTED' and (markerValue(KEYS[3], markerBaseKey, 'receipt_epoch') or '0') or '0')
if redis.call('HSETNX', KEYS[4], ARGV[8], resolution) ~= 1 or redis.call('HGET', KEYS[4], ARGV[8]) ~= resolution then
  return {'RECONCILE_REQUIRED'}
end
if status ~= 'RECONCILE_REQUIRED' and not deleteCommitMarker(KEYS[3], KEYS[7], markerDigest) then
  return {'RECONCILE_REQUIRED'}
end
redis.call('HSET', KEYS[3], base .. ':state', status, base .. ':resolution_id', ARGV[9])
if status ~= 'RECONCILE_REQUIRED' then redis.call('ZREM', KEYS[5], ARGV[10]) end
return {status, resolution}
`

const friendCompactMarkerSource = friendLuaPrelude + `
-- KEYS: attempt partition, resolution partition, attempt-due, receipt partition, bucket-meta,
--       receipt-due, optional source-due
-- ARGV: tag,digest,ordinal,attempt_id,candidate_hash,marker_digest,
--       resolution_field,resolution_id,attempt_due_member,authority_kind,layout,bucket
if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
   not friendKeyTypeOK(KEYS[3], 'zset') or not friendKeyTypeOK(KEYS[4], 'hash') or
   not friendKeyTypeOK(KEYS[5], 'hash') or not friendKeyTypeOK(KEYS[6], 'zset') or
   (#KEYS == 7 and not friendKeyTypeOK(KEYS[7], 'zset')) or #KEYS < 6 or #KEYS > 7 then return {'CORRUPT_RECORD'} end
local metaOK, metaWhy = validateBucketMeta(KEYS[5])
if not metaOK then return {metaWhy} end
local identityOK, identityWhy = checkMeta(KEYS[5], ARGV[10], ARGV[11], ARGV[12])
if not identityOK then return {identityWhy} end
local metaCountOK, metaCountWhy = validateBucketMetaCounts(KEYS[5], KEYS[3], KEYS[6], #KEYS == 7 and KEYS[7] or '', ARGV[10] ~= 'FRIEND_PAIR')
if not metaCountOK then return {metaCountWhy} end
local base = cursorBase(ARGV[2])
if redis.call('HGET', KEYS[1], base .. ':state') ~= 'COMMITTED' or
   redis.call('HGET', KEYS[1], base .. ':ordinal') ~= ARGV[3] or
   redis.call('HGET', KEYS[1], base .. ':attempt_id') ~= ARGV[4] or
   redis.call('HGET', KEYS[1], base .. ':candidate_hash') ~= ARGV[5] then return {'SKIPPED'} end
local oldResolution = redis.call('HGET', KEYS[2], ARGV[7])
if oldResolution then
  local same, oldStatus, oldMarker, oldReceiptField, oldKind, oldEpoch =
    resolutionMatches(oldResolution, ARGV[8], ARGV[4], ARGV[5], tonumber(ARGV[3]))
  local oldReceipt = oldReceiptField and redis.call('HGET', KEYS[4], oldReceiptField) or nil
  if not same or oldStatus ~= 'COMMITTED' or oldMarker ~= ARGV[6] or not oldReceipt then
    return {'RECONCILE_REQUIRED'}
  end
  local oldPrefix = receiptStoredPrefix(oldReceipt)
  if not oldPrefix or not resolutionReceiptMatches(oldReceipt, oldPrefix, oldKind, oldEpoch,
      ARGV[4], tonumber(ARGV[3]), ARGV[5]) then return {'RECONCILE_REQUIRED'} end
  local markerProof = redis.call('HGET', KEYS[1], markerBase(ARGV[6]) .. 'proof')
  local markerComplete = redis.call('HGET', KEYS[1], markerBase(ARGV[6]) .. 'complete')
  if (markerProof and not markerComplete) or (markerComplete and not markerProof) or
     (markerProof and not markerBasic(KEYS[1], ARGV[6], ARGV[4], tonumber(ARGV[3]), ARGV[5])) then
    return {'RECONCILE_REQUIRED'}
  end
  if not deleteCommitMarker(KEYS[1], KEYS[5], ARGV[6]) then return {'RECONCILE_REQUIRED'} end
  redis.call('HSET', KEYS[1], base .. ':resolution_id', ARGV[8])
  redis.call('ZREM', KEYS[3], ARGV[9])
  return {'COMPACTED'}
end
if not markerBasic(KEYS[1], ARGV[6], ARGV[4], tonumber(ARGV[3]), ARGV[5]) then return {'RECONCILE_REQUIRED'} end
local markerKey = markerBase(ARGV[6])
local receiptField = markerValue(KEYS[1], markerKey, 'receipt_field')
local receiptKind = markerValue(KEYS[1], markerKey, 'receipt_kind')
local receiptEpoch = markerValue(KEYS[1], markerKey, 'receipt_epoch')
local receiptPrefix = markerValue(KEYS[1], markerKey, 'receipt_prefix')
local receipt = receiptField and redis.call('HGET', KEYS[4], receiptField) or nil
if not receipt or not markerReceiptHeader(receipt, receiptPrefix or '', receiptKind or '',
    markerValue(KEYS[1], markerKey, 'receipt_status') or '', receiptEpoch or '0',
    ARGV[4], tonumber(ARGV[3]), ARGV[5]) then return {'RECONCILE_REQUIRED'} end
local resolution = resolutionValue('COMMITTED', ARGV[4], ARGV[5], ARGV[8], tonumber(ARGV[3]), ARGV[6],
  receiptField, receiptKind, receiptEpoch)
-- FRIEND_COMPACT_FAULT_BEFORE_RESOLUTION
if redis.call('HSETNX', KEYS[2], ARGV[7], resolution) ~= 1 or redis.call('HGET', KEYS[2], ARGV[7]) ~= resolution then
  return {'RECONCILE_REQUIRED'}
end
-- FRIEND_COMPACT_FAULT_AFTER_RESOLUTION
if not deleteCommitMarker(KEYS[1], KEYS[5], ARGV[6]) then return {'RECONCILE_REQUIRED'} end
-- FRIEND_COMPACT_FAULT_AFTER_MARKER_DELETE
redis.call('HSET', KEYS[1], base .. ':resolution_id', ARGV[8])
-- FRIEND_COMPACT_FAULT_AFTER_CURSOR_INDEX
redis.call('ZREM', KEYS[3], ARGV[9])
-- FRIEND_COMPACT_FAULT_AFTER_DISCOVERY_INDEX
return {'COMPACTED'}
`

// pairMutationSource emits one immutable script source for each named Pair
// operation.  The source is shared structurally but the operation kind is
// compiled into the script, so no arbitrary mutation enum reaches Lua.
func pairMutationSource(kind string) string {
	return friendLuaPrelude + fmt.Sprintf(`
-- KEYS: pair record, receipt, attempt, attempt-due, receipt-due, source-due,
--       bucket-meta,
--       one key per bounded source order
-- ARGV: tag,digest,prefix,receipt_field,receipt_kind,ordinal,attempt_id,
--       candidate_hash,attempt_due_member,receipt_due_member,
--       max_receipts,max_receipt_bytes,max_pending,max_pending_bytes,
--       attempt_retention_ms,receipt_retention_ms,order_count,
--       order tuples (id,type,blob,work_ref,payload_hash,due_ms),
--       pair_id,bucket_id,layout,low,high,caller,peer,operation_id,
--       expected_record_version,max_record_bytes
local MUTATION_KIND = %q
local digest = ARGV[2]
local orderCount = tonumber(ARGV[17])
if orderCount ~= 1 and orderCount ~= 2 then return {'CONTRACT_ERROR'} end
if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
   not friendKeyTypeOK(KEYS[3], 'hash') or not friendKeyTypeOK(KEYS[4], 'zset') or
   not friendKeyTypeOK(KEYS[5], 'zset') or not friendKeyTypeOK(KEYS[6], 'zset') or
   not friendKeyTypeOK(KEYS[7], 'hash') then
  return {'CORRUPT_RECORD'}
end
for i = 1, orderCount do
  if not friendKeyTypeOK(KEYS[7 + i], 'hash') then return {'CORRUPT_RECORD'} end
end
local strictMetaOK, strictMetaWhy = validateBucketMeta(KEYS[7])
if not strictMetaOK then return {strictMetaWhy} end
local base = 17 + orderCount * 6
local authorityMetaOK, authorityMetaWhy = validateBucketMetaAuthority(
  KEYS[7], KEYS[1], 'FRIEND_PAIR', ARGV[base + 3], ARGV[base + 2], MUTATION_KIND == 'BLOCK')
if not authorityMetaOK then return {authorityMetaWhy} end
local metaCountOK, metaCountWhy = validateBucketMetaCounts(KEYS[7], KEYS[4], KEYS[5], KEYS[6], false)
if not metaCountOK then return {metaCountWhy} end
local canonicalOK, canonicalWhy = validateCanonicalRecord(KEYS[1], 'FRIEND_PAIR', ARGV[base + 1])
if not canonicalOK then return {canonicalWhy} end
local existing = redis.call('HGET', KEYS[2], ARGV[4])
if existing then
  if string.sub(existing, 1, #ARGV[3]) == ARGV[3] then
    if not receiptCandidateMatches(existing, ARGV[3], ARGV[8]) then return {'IDEMPOTENCY_CONFLICT'} end
	if not markerVerify(KEYS[3], digest, ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
		KEYS[1], KEYS[2], KEYS[5], KEYS[7], KEYS[6], '') then return {'RECONCILE_REQUIRED'} end
    return {'REPLAY', existing}
  end
  return {'IDEMPOTENCY_CONFLICT'}
end
local ok, why = checkOpenCursor(KEYS[3], digest, ARGV[7], ARGV[6], ARGV[8])
if not ok then return {why} end
local pairID = ARGV[base + 1]
local bucketID = ARGV[base + 2]
local layout = ARGV[base + 3]
local low = ARGV[base + 4]
local high = ARGV[base + 5]
local caller = ARGV[base + 6]
local peer = ARGV[base + 7]
local operationID = ARGV[base + 8]
local expectedRecordVersion = tonumber(ARGV[base + 9]) or 0
local maxRecordBytes = tonumber(ARGV[base + 10]) or 0
if low == '' or high == '' or caller == '' or peer == '' or operationID == '' or
   caller ~= low and caller ~= high or peer ~= low and peer ~= high or caller == peer then
  return {'CONTRACT_ERROR'}
end

local hasRecord = redis.call('EXISTS', KEYS[1]) == 1
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local metaOK, metaWhy = checkMeta(KEYS[7], 'FRIEND_PAIR', layout, bucketID)
if not metaOK then return {metaWhy} end
local relStatus = redis.call('HGET', KEYS[1], 'relation:status') or '0'
local relEpoch = num(KEYS[1], 'relation:epoch', 0)
local blockStatus
local blockEpoch
if caller == low then
  blockStatus = redis.call('HGET', KEYS[1], 'block:low_to_high:status') or '0'
  blockEpoch = num(KEYS[1], 'block:low_to_high:epoch', 0)
else
  blockStatus = redis.call('HGET', KEYS[1], 'block:high_to_low:status') or '0'
  blockEpoch = num(KEYS[1], 'block:high_to_low:epoch', 0)
end
local lowBlockStatus = redis.call('HGET', KEYS[1], 'block:low_to_high:status') or '0'
local highBlockStatus = redis.call('HGET', KEYS[1], 'block:high_to_low:status') or '0'
local status = 'SUCCEEDED'
local reason = ''
local detail = ''
local changed = false
local createRecord = false
local termination = false
local capturedEpoch = 0
local newEpoch = relEpoch
local termOrderID = ''
local resultOrderID = ''

if MUTATION_KIND == 'DELETE' then
  if relStatus ~= '1' then
    status = 'NO_CHANGE'; reason = 'NOT_FRIENDS'
  else
    changed = true; termination = true; createRecord = true
    capturedEpoch = relEpoch; newEpoch = relEpoch + 1
    relStatus = '2'
    termOrderID = ARGV[18]
  end
elseif MUTATION_KIND == 'BLOCK' then
  if blockStatus == '1' then
    status = 'NO_CHANGE'; reason = 'ALREADY_BLOCKED'
  else
    changed = true; createRecord = true; blockStatus = '1'; blockEpoch = blockEpoch + 1
    if caller == low then lowBlockStatus = '1' else highBlockStatus = '1' end
    if relStatus == '1' then
      termination = true; capturedEpoch = relEpoch; newEpoch = relEpoch + 1; relStatus = '2'
      termOrderID = ARGV[18]
    end
  end
elseif MUTATION_KIND == 'UNBLOCK' then
  if blockStatus ~= '1' then
    status = 'NO_CHANGE'; reason = 'ALREADY_UNBLOCKED'
  else
    changed = true; createRecord = true; blockStatus = '2'
    if caller == low then lowBlockStatus = '2' else highBlockStatus = '2' end
  end
else
  return {'CONTRACT_ERROR'}
end
if termination and orderCount ~= 2 then return {'CONTRACT_ERROR'} end
-- The Go adapter may have observed an ACTIVE relation before another Pair
-- commit terminated it.  In that race the second tuple is still the stable
-- ResultPublishOrder, but the stale termination tuple must not be created.
local firstOrder = 1
if not termination and orderCount == 2 then firstOrder = 2 end
resultOrderID = ARGV[17 + (orderCount - 1) * 6 + 1]

-- Validate all order identities and capacity before the first business write.
local receiptBlob = receiptValue(ARGV[5], status, reason, newEpoch, detail, '', ARGV[3])
	if #receiptBlob > tonumber(ARGV[#ARGV - 4]) then return {'CAPACITY_ITEM_LIMIT'} end
local receiptCount = num(KEYS[7], 'receipt_count', 0)
local receiptBytes = num(KEYS[7], 'receipt_bytes', 0)
local pendingCount = num(KEYS[7], 'pending_work_count', 0)
local pendingBytes = num(KEYS[7], 'pending_work_bytes', 0)
local newOrders = 0
local newOrderBytes = 0
for i = 1, orderCount do
	local oi = 18 + (i - 1) * 6
  local orderKey = KEYS[7 + i]
	if sourceOrderLogicalBytes(ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5]) > tonumber(ARGV[#ARGV - 3]) then
		return {'CAPACITY_ITEM_LIMIT'}
	end
	local orderOK, orderState = validateSourceOrder(orderKey, KEYS[6], ARGV[oi], ARGV[oi + 1], ARGV[oi + 2],
		ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5], layout, bucketID, pairID, 'BASE')
	if not orderOK then return {orderState} end
	if i < firstOrder and orderState == 'MISSING' then return {'WORK_RECONCILE'} end
	if i >= firstOrder and orderState == 'MISSING' then
		newOrders = newOrders + 1; newOrderBytes = newOrderBytes + sourceOrderLogicalBytes(ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5])
	end
end
if receiptCount + 1 > tonumber(ARGV[11]) or receiptBytes + #receiptBlob > tonumber(ARGV[12]) then
  return {'CAPACITY_RECEIPT'}
end
if pendingCount + newOrders > tonumber(ARGV[13]) or pendingBytes + newOrderBytes > tonumber(ARGV[14]) then
  return {'CAPACITY_WORK'}
end

if changed then
	  if createRecord and not hasRecord then
		stageSet(KEYS[1], 'identity:pair_id', pairID, 'identity:low_player_id', low,
	      'identity:high_player_id', high, 'relation:id', pairID,
	      'relation:status', '0', 'relation:epoch', '0',
	      'block:low_to_high:status', '0', 'block:low_to_high:epoch', '0',
	      'block:low_to_high:changed_at_ms', '0', 'block:low_to_high:removed_at_ms', '0',
	      'block:high_to_low:status', '0', 'block:high_to_low:epoch', '0',
	      'block:high_to_low:changed_at_ms', '0', 'block:high_to_low:removed_at_ms', '0',
	      'reactivation:state', '0', 'reactivation:terminated_relation_epoch', '0',
	      'reactivation:barrier_revision', '0', 'reactivation:low_generation_floor', '0',
	      'reactivation:high_generation_floor', '0', 'relation:activated_at_ms', '0',
	      'relation:deleted_at_ms', '0', 'relation:activation_token_id', '',
	      'relation:activation_token_generation', '0', 'relation:termination_reason', '0',
	      'relation:terminated_by_player_id', '0', 'relation:last_change_operation_id', '',
	      'termination:order_id', '', 'termination:captured_epoch', '0',
	      'termination:barrier_revision', '0', 'termination:completed', '0',
	      'termination:low_floor_captured', '0', 'termination:high_floor_captured', '0',
	      'termination:quota_released_low', '0', 'termination:quota_released_high', '0')
  end
  if MUTATION_KIND == 'DELETE' and changed then
		stageSet(KEYS[1], 'relation:status', relStatus, 'relation:epoch', tostring(newEpoch),
      'relation:deleted_at_ms', tostring(now), 'relation:termination_reason', '1',
      'relation:terminated_by_player_id', caller, 'relation:last_change_operation_id', operationID,
      'reactivation:state', '1', 'reactivation:terminated_relation_epoch', tostring(capturedEpoch),
      'reactivation:barrier_revision', tostring(num(KEYS[1], 'reactivation:barrier_revision', 0) + 1),
      'reactivation:low_generation_floor', '0', 'reactivation:high_generation_floor', '0',
      'termination:order_id', termOrderID, 'termination:captured_epoch', tostring(capturedEpoch),
      'termination:barrier_revision', tostring(num(KEYS[1], 'reactivation:barrier_revision', 0) + 1),
      'termination:completed', '0', 'termination:low_floor_captured', '0',
      'termination:high_floor_captured', '0', 'termination:quota_released_low', '0',
      'termination:quota_released_high', '0')
  elseif MUTATION_KIND == 'BLOCK' and changed then
    local barrierRevision = num(KEYS[1], 'reactivation:barrier_revision', 0)
    if caller == low then
		stageSet(KEYS[1], 'block:low_to_high:status', '1', 'block:low_to_high:epoch', tostring(blockEpoch),
        'block:low_to_high:changed_at_ms', tostring(now))
    else
		stageSet(KEYS[1], 'block:high_to_low:status', '1', 'block:high_to_low:epoch', tostring(blockEpoch),
        'block:high_to_low:changed_at_ms', tostring(now))
    end
    if termination then
		stageSet(KEYS[1], 'relation:status', '2', 'relation:epoch', tostring(newEpoch),
        'relation:deleted_at_ms', tostring(now), 'relation:termination_reason', '2',
        'relation:terminated_by_player_id', caller, 'relation:last_change_operation_id', operationID,
        'reactivation:state', '1', 'reactivation:terminated_relation_epoch', tostring(capturedEpoch),
        'reactivation:barrier_revision', tostring(barrierRevision + 1),
        'reactivation:low_generation_floor', '0', 'reactivation:high_generation_floor', '0',
        'termination:order_id', termOrderID, 'termination:captured_epoch', tostring(capturedEpoch),
        'termination:barrier_revision', tostring(barrierRevision + 1),
        'termination:completed', '0', 'termination:low_floor_captured', '0',
        'termination:high_floor_captured', '0', 'termination:quota_released_low', '0',
        'termination:quota_released_high', '0')
    end
  elseif MUTATION_KIND == 'UNBLOCK' and changed then
    if caller == low then
		stageSet(KEYS[1], 'block:low_to_high:status', '2', 'block:low_to_high:epoch', tostring(blockEpoch + 1),
		  'block:low_to_high:removed_at_ms', tostring(now))
    else
		stageSet(KEYS[1], 'block:high_to_low:status', '2', 'block:high_to_low:epoch', tostring(blockEpoch + 1),
		  'block:high_to_low:removed_at_ms', tostring(now))
    end
  end
  local oldVersion = num(KEYS[1], '_meta:record_version', 0)
	stageSet(KEYS[1], '_meta:schema_version', 'friend-pair-v1',
    '_meta:authority_layout_version', layout, '_meta:record_version', tostring(oldVersion + 1),
    '_meta:updated_at_ms', tostring(now), '_meta:business_state_digest', ARGV[8])
end

local plannedRecordBytes, plannedRecordErr = plannedBytes(KEYS[1])
if plannedRecordErr ~= '' then return {plannedRecordErr} end
if maxRecordBytes > 0 and plannedRecordBytes > maxRecordBytes then
  return {'CAPACITY_BYTE_LIMIT'}
end
local markerOrders = {}
local markerTuples = {}
for i = firstOrder, orderCount do
  local oi = 18 + (i - 1) * 6
  markerOrders[#markerOrders + 1] = KEYS[7 + i]
  markerTuples[#markerTuples + 1] = {ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5]}
end
local finalVersion = num(KEYS[1], '_meta:record_version', 0)
if changed then finalVersion = finalVersion + 1 end
local finalDigest = changed and ARGV[8] or (redis.call('HGET', KEYS[1], '_meta:business_state_digest') or '0')
local finalExists = hasRecord or changed
local markerOK, markerSize = writeCommitMarker(KEYS[3], ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
    KEYS[1], finalExists, finalVersion, finalDigest, plannedRecordBytes, KEYS[2], ARGV[4], ARGV[3],
    receiptBlob, ARGV[5], status, newEpoch, KEYS[5], ARGV[10], now + tonumber(ARGV[16]), KEYS[7], KEYS[6],
	markerOrders, markerTuples, '', '', '', '', '', layout, bucketID, 'FRIEND_PAIR', pairID,
	receiptCount + 1, receiptBytes + #receiptBlob,
	pendingCount + newOrders, pendingBytes + newOrderBytes, tonumber(ARGV[#ARGV - 2]), tonumber(ARGV[#ARGV - 1]))
if not markerOK then return {'CAPACITY_MARKER_' .. tostring(markerSize or 0)} end
local metaKind = 'FRIEND_PAIR'
prepareMeta(KEYS[7], metaKind, layout, bucketID)
if #staged > 0 then applyStaged(plannedRecordBytes) end
for i = firstOrder, orderCount do
	local oi = 18 + (i - 1) * 6
  local orderKey = KEYS[7 + i]
  if redis.call('EXISTS', orderKey) == 0 then
		redis.call('HSET', orderKey, 'schema_version', 'friend-source-order-v1',
      'work_id', ARGV[oi], 'work_type', ARGV[oi + 1],
      'placement_kind', 'SOURCE_PERSISTENCE_BUCKET', 'bucket_version', layout,
      'bucket_id', bucketID, 'source_authority_kind', 'FRIEND_PAIR',
      'canonical_source_authority_id', pairID, 'state', 'PENDING', 'work_version', '1',
		  'typed_payload', ARGV[oi + 2], 'work_ref', ARGV[oi + 3], 'payload_hash', ARGV[oi + 4],
		  'next_due_at_ms', ARGV[oi + 5], 'retry_count', '0', 'lease_epoch', '0',
      'lease_holder_id', '', 'lease_token_hash', '', 'lease_expire_at_ms', '0',
      'created_at_ms', tostring(now), 'updated_at_ms', tostring(now), 'completed_at_ms', '0')
		redis.call('ZADD', KEYS[6], tonumber(ARGV[oi + 5]), ARGV[oi + 3])
  end
end
redis.call('HSET', KEYS[2], ARGV[4], receiptBlob)
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[16]), ARGV[10])
redis.call('HINCRBY', KEYS[7], 'receipt_count', 1)
redis.call('HINCRBY', KEYS[7], 'receipt_bytes', #receiptBlob)
redis.call('HINCRBY', KEYS[7], 'pending_work_count', newOrders)
redis.call('HINCRBY', KEYS[7], 'pending_work_bytes', newOrderBytes)
redis.call('HSET', KEYS[7], 'last_updated_at_ms', tostring(now))
completeCommitMarker(KEYS[3], ARGV[#ARGV])
	commitCursor(KEYS[3], digest, KEYS[4], ARGV[9], 'COMMITTED', ARGV[15])
return {'COMMITTED', receiptBlob, tostring(num(KEYS[1], '_meta:record_version', 0)), tostring(newEpoch)}
`, kind)
}

// friendSimpleSource wraps mutations which have no Pair source orders.  The
// first fourteen arguments are the same commit cursor/receipt contract used
// by the Pair scripts; operation-specific arguments start at 15.  Keeping
// this wrapper private lets each authority operation remain a fixed script
// while sharing only the receipt/cursor bookkeeping.
func friendSimpleSource(kind, operation string) string {
	metaKind := "FRIEND_PAIR"
	if len(kind) >= len("QUOTA_") && kind[:len("QUOTA_")] == "QUOTA_" {
		metaKind = "FRIEND_QUOTA"
	} else if len(kind) >= len("SHARE_") && kind[:len("SHARE_")] == "SHARE_" {
		metaKind = "FRIEND_SHARE_ISSUER"
	}
	allowCreate := kind == "ACTIVATE_RELATION" || kind == "QUOTA_RESERVE" || kind == "QUOTA_REMOVE" ||
		kind == "SHARE_GENERATE" || kind == "SHARE_ROTATE" || kind == "SHARE_REACTIVATE" || kind == "SHARE_REVOKE"
	return friendLuaPrelude + fmt.Sprintf(`
	-- KEYS: authority record, receipt partition, attempt partition, attempt-due,
	--       receipt-due, bucket meta, optional quota fence
-- ARGV: tag,digest,prefix,receipt_field,receipt_kind,ordinal,attempt_id,
--       candidate_hash,attempt_due_member,receipt_due_member,
--       max_receipts,max_receipt_bytes,attempt_retention_ms,receipt_retention_ms,
--       layout,bucket_id,authority_id,max_record_bytes,operation fields...
	local META_KIND = %q
	local ALLOW_CREATE = %t
	if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
	   not friendKeyTypeOK(KEYS[3], 'hash') or not friendKeyTypeOK(KEYS[4], 'zset') or
	   not friendKeyTypeOK(KEYS[5], 'zset') or not friendKeyTypeOK(KEYS[6], 'hash') then
	  return {'CORRUPT_RECORD'}
	end
		local quotaFenceKey = ''
		if META_KIND == 'FRIEND_QUOTA' then
		  if #KEYS >= 7 then
		    quotaFenceKey = KEYS[7]
		    if not friendKeyTypeOK(quotaFenceKey, 'hash') then return {'CORRUPT_RECORD'} end
		  end
	elseif #KEYS >= 7 and not friendKeyTypeOK(KEYS[7], 'hash') then
	  return {'CORRUPT_RECORD'}
	end
local strictMetaOK, strictMetaWhy = validateBucketMeta(KEYS[6])
if not strictMetaOK then return {strictMetaWhy} end
local authorityMetaOK, authorityMetaWhy = validateBucketMetaAuthority(
  KEYS[6], KEYS[1], META_KIND, ARGV[15], ARGV[16], ALLOW_CREATE)
if not authorityMetaOK then return {authorityMetaWhy} end
local metaCountOK, metaCountWhy = validateBucketMetaCounts(KEYS[6], KEYS[4], KEYS[5], '', META_KIND ~= 'FRIEND_PAIR')
if not metaCountOK then return {metaCountWhy} end
local canonicalOK, canonicalWhy = validateCanonicalRecord(KEYS[1], META_KIND, ARGV[17])
if not canonicalOK then return {canonicalWhy} end
local markerExtraKey = ''
local markerExtraField = ''
	local markerExtraValue = ''
	local markerExtraDueMember = ''
	if ARGV[5] == 'QUOTA_REMOVE' and quotaFenceKey ~= '' then
	markerExtraKey = quotaFenceKey
	markerExtraField = 'relation-release-fence:' .. ARGV[21]
	markerExtraDueMember = 'fence:' .. ARGV[21]
end
local existing = redis.call('HGET', KEYS[2], ARGV[4])
if existing then
  if string.sub(existing, 1, #ARGV[3]) == ARGV[3] then
	if not receiptCandidateMatches(existing, ARGV[3], ARGV[8]) then return {'IDEMPOTENCY_CONFLICT'} end
	if not markerVerify(KEYS[3], ARGV[2], ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
		KEYS[1], KEYS[2], KEYS[5], KEYS[6], '', markerExtraKey) then return {'RECONCILE_REQUIRED'} end
    return {'REPLAY', existing}
  end
  return {'IDEMPOTENCY_CONFLICT'}
end
local ok, why = checkOpenCursor(KEYS[3], ARGV[2], ARGV[7], ARGV[6], ARGV[8])
if not ok then return {why} end
local status = 'SUCCEEDED'
local reason = ''
local detail = ''
local epoch = 0
local typed = ''
local changed = false
	local RECORD_SCHEMA = 'friend-pair-v1'
if META_KIND == 'FRIEND_QUOTA' then RECORD_SCHEMA = 'friend-quota-v1' end
if META_KIND == 'FRIEND_SHARE_ISSUER' then RECORD_SCHEMA = 'friend-share-issuer-v1' end
%s
if markerExtraKey ~= '' then
	markerExtraValue = stagedFinal(markerExtraKey, markerExtraField) or ''
end
local blob = receiptValue(ARGV[5], status, reason, epoch, detail, typed, ARGV[3])
if #blob > tonumber(ARGV[#ARGV - 4]) then return {'CAPACITY_ITEM_LIMIT'} end
local receiptCount = num(KEYS[6], 'receipt_count', 0)
local receiptBytes = num(KEYS[6], 'receipt_bytes', 0)
local extraReceiptCount = stagedNewFieldCount(KEYS[2])
local extraReceiptBytes = stagedByteDelta(KEYS[2])
	if quotaFenceKey ~= '' then
	  extraReceiptCount = extraReceiptCount + stagedNewFieldCount(quotaFenceKey)
	  extraReceiptBytes = extraReceiptBytes + stagedByteDelta(quotaFenceKey)
end
if receiptCount + 1 + extraReceiptCount > tonumber(ARGV[11]) or
   receiptBytes + #blob + extraReceiptBytes > tonumber(ARGV[12]) then
	return {'CAPACITY_RECEIPT'}
end
if changed or redis.call('EXISTS', KEYS[1]) == 1 then
  -- The issuer identity is a canonical scalar, not a routing/index copy.
	-- Keep it on the ShareIssuer record even for a deterministic no-change
	-- admission against an already existing issuer.
	if META_KIND == 'FRIEND_SHARE_ISSUER' then
		stageSet(KEYS[1], 'issuer:player_id', ARGV[17])
	end
end
if changed then
	local oldVersion = num(KEYS[1], '_meta:record_version', 0)
	stageSet(KEYS[1], '_meta:schema_version', RECORD_SCHEMA,
		'_meta:authority_layout_version', ARGV[15], '_meta:record_version', tostring(oldVersion + 1),
		'_meta:updated_at_ms', tostring(now), '_meta:business_state_digest', ARGV[8])
end
local maxRecordBytes = tonumber(ARGV[18]) or 0
local plannedRecordBytes, plannedRecordErr = plannedBytes(KEYS[1])
if plannedRecordErr ~= '' then return {plannedRecordErr} end
if maxRecordBytes > 0 and plannedRecordBytes > maxRecordBytes then
	return {'CAPACITY_BYTE_LIMIT'}
end
local finalVersion = num(KEYS[1], '_meta:record_version', 0)
if changed then finalVersion = finalVersion + 1 end
local finalDigest = changed and ARGV[8] or (redis.call('HGET', KEYS[1], '_meta:business_state_digest') or '0')
local finalExists = redis.call('EXISTS', KEYS[1]) == 1 or changed
local markerOK, markerSize = writeCommitMarker(KEYS[3], ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
    KEYS[1], finalExists, finalVersion, finalDigest, plannedRecordBytes, KEYS[2], ARGV[4], ARGV[3],
    blob, ARGV[5], status, epoch, KEYS[5], ARGV[10], now + tonumber(ARGV[14]), KEYS[6], '', {}, {},
    markerExtraKey, markerExtraField, markerExtraValue, markerExtraDueMember,
	markerExtraKey ~= '' and now + tonumber(ARGV[14]) or '', ARGV[15], ARGV[16], META_KIND, ARGV[17],
	receiptCount + 1 + extraReceiptCount,
	receiptBytes + #blob + extraReceiptBytes, num(KEYS[6], 'pending_work_count', 0),
	num(KEYS[6], 'pending_work_bytes', 0), tonumber(ARGV[#ARGV - 2]), tonumber(ARGV[#ARGV - 1]))
if not markerOK then return {'CAPACITY_MARKER_' .. tostring(markerSize or 0)} end
prepareMeta(KEYS[6], META_KIND, ARGV[15], ARGV[16])

	if #staged > 0 then applyStaged(plannedRecordBytes) end
	redis.call('HSET', KEYS[2], ARGV[4], blob)
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[14]), ARGV[10])
	if META_KIND == 'FRIEND_QUOTA' and string.sub(ARGV[5], 1, 6) == 'QUOTA_' and ARGV[5] == 'QUOTA_REMOVE' then
		redis.call('ZADD', KEYS[5], now + tonumber(ARGV[14]), 'fence:' .. ARGV[21])
	end
redis.call('HINCRBY', KEYS[6], 'receipt_count', 1 + extraReceiptCount)
redis.call('HINCRBY', KEYS[6], 'receipt_bytes', #blob + extraReceiptBytes)
redis.call('HSET', KEYS[6], 'last_updated_at_ms', tostring(now))
completeCommitMarker(KEYS[3], ARGV[#ARGV])
	commitCursor(KEYS[3], ARGV[2], KEYS[4], ARGV[9], 'COMMITTED', ARGV[13])
return {'COMMITTED', blob, tostring(num(KEYS[1], '_meta:record_version', 0)), tostring(epoch)}
`, metaKind, allowCreate, operation)
}

const pairActivateOperation = `
local pairID = ARGV[19]
local layout = ARGV[15]
local low = ARGV[20]
local high = ARGV[21]
local inviter = ARGV[22]
local redeemer = ARGV[23]
local targetEpoch = tonumber(ARGV[24])
local tokenID = ARGV[25]
local tokenGeneration = tonumber(ARGV[26])
local operationID = ARGV[28]
local expectedRecordVersion = tonumber(ARGV[29]) or 0
local rotateID = ARGV[30]
local rotatePairID = ARGV[31]
local rotateEpoch = tonumber(ARGV[32]) or 0
local rotateRevision = tonumber(ARGV[33]) or 0
local rotateTokenID = ARGV[34]
local rotateGeneration = tonumber(ARGV[35]) or 0
local rotateLowFloor = tonumber(ARGV[36]) or 0
local rotateHighFloor = tonumber(ARGV[37]) or 0
local rotateCommittedAt = tonumber(ARGV[38]) or 0
local expectedRotateID = ARGV[39]
local rotateProofValid = ARGV[27] == '1'
local rotateOperationID = ARGV[40]
local rotatePayloadHash = ARGV[41]
local lowQuotaReservationID = ARGV[42]
local lowQuotaProof = ARGV[43]
local lowQuotaPresent = ARGV[44] == '1'
local highQuotaReservationID = ARGV[45]
local highQuotaProof = ARGV[46]
local highQuotaPresent = ARGV[47] == '1'
-- The deprecated boolean compatibility field is intentionally ignored.  A
-- non-empty, fully checked RotateReceipt is the only proof of reactivation.
local rotatePresent = rotateID ~= ''
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local relationStatus = redis.call('HGET', KEYS[1], 'relation:status') or '0'
local relationEpoch = num(KEYS[1], 'relation:epoch', 0)
epoch = relationEpoch
if relationStatus == '1' then
  status = 'NO_CHANGE'; reason = 'ALREADY_FRIENDS'
elseif targetEpoch ~= relationEpoch + 1 then
  status = 'REJECTED'; reason = 'DOMAIN_EPOCH_CONFLICT'
elseif (redis.call('HGET', KEYS[1], 'block:low_to_high:status') or '0') == '1' or
       (redis.call('HGET', KEYS[1], 'block:high_to_low:status') or '0') == '1' then
  status = 'REJECTED'; reason = 'BLOCKED'
elseif inviter ~= low and inviter ~= high or redeemer ~= low and redeemer ~= high or inviter == redeemer then
  return {'CONTRACT_ERROR'}
else
  local barrier = tonumber(redis.call('HGET', KEYS[1], 'reactivation:state') or '0')
  local reactivation = relationEpoch > 0 or barrier ~= 0
  local floor = 0
  if inviter == low then floor = num(KEYS[1], 'reactivation:low_generation_floor', 0)
  else floor = num(KEYS[1], 'reactivation:high_generation_floor', 0) end
  if reactivation and barrier ~= 2 then
    status = 'REJECTED'; reason = 'RELATION_REACTIVATION_PENDING'
  elseif reactivation and tokenGeneration <= floor then
    status = 'REJECTED'; reason = 'SHARE_TOKEN_PREDATES_RELATION_DELETE'
	elseif reactivation and (not rotateProofValid or not rotatePresent or rotateID == '' or rotatePairID ~= pairID or
		rotateID ~= expectedRotateID or rotateCommittedAt <= 0 or
		rotateEpoch ~= num(KEYS[1], 'reactivation:terminated_relation_epoch', 0) or
		rotateRevision ~= num(KEYS[1], 'reactivation:barrier_revision', 0) or rotateTokenID ~= tokenID or
		rotateGeneration ~= tokenGeneration or rotateLowFloor ~= num(KEYS[1], 'reactivation:low_generation_floor', 0) or
		rotateHighFloor ~= num(KEYS[1], 'reactivation:high_generation_floor', 0) or rotateOperationID == '' or
		rotatePayloadHash == '' or #rotatePayloadHash ~= 64) then
		status = 'REJECTED'; reason = 'REACTIVATION_ROTATE_REQUIRED'
  elseif reactivation and (not lowQuotaPresent or not highQuotaPresent or lowQuotaReservationID == '' or
      highQuotaReservationID == '' or not validLowerHex(lowQuotaProof, 32) or not validLowerHex(highQuotaProof, 32)) then
		status = 'REJECTED'; reason = 'RELATION_REACTIVATION_PENDING'
  else
	    if redis.call('EXISTS', KEYS[1]) == 0 then
		stageSet(KEYS[1], 'identity:pair_id', pairID,
	        'identity:low_player_id', low, 'identity:high_player_id', high,
	        'relation:id', pairID, 'relation:status', '0', 'relation:epoch', '0',
	        'block:low_to_high:status', '0', 'block:low_to_high:epoch', '0',
	        'block:low_to_high:changed_at_ms', '0', 'block:low_to_high:removed_at_ms', '0',
	        'block:high_to_low:status', '0', 'block:high_to_low:epoch', '0',
	        'block:high_to_low:changed_at_ms', '0', 'block:high_to_low:removed_at_ms', '0',
	        'reactivation:state', '0', 'reactivation:terminated_relation_epoch', '0',
	        'reactivation:barrier_revision', '0', 'reactivation:low_generation_floor', '0',
	        'reactivation:high_generation_floor', '0', 'relation:activated_at_ms', '0',
	        'relation:deleted_at_ms', '0', 'relation:activation_token_id', '',
	        'relation:activation_token_generation', '0', 'relation:termination_reason', '0',
	        'relation:terminated_by_player_id', '0', 'relation:last_change_operation_id', '',
	        'termination:order_id', '', 'termination:captured_epoch', '0',
	        'termination:barrier_revision', '0', 'termination:completed', '0',
	        'termination:low_floor_captured', '0', 'termination:high_floor_captured', '0',
	        'termination:quota_released_low', '0', 'termination:quota_released_high', '0')
	    end
		stageSet(KEYS[1], 'identity:pair_id', pairID,
      'identity:low_player_id', low, 'identity:high_player_id', high,
      'relation:id', pairID, 'relation:status', '1', 'relation:epoch', tostring(targetEpoch),
      'relation:activated_at_ms', tostring(now), 'relation:deleted_at_ms', '0',
      'relation:activation_token_id', tokenID,
      'relation:activation_token_generation', tostring(tokenGeneration),
      'relation:termination_reason', '0', 'relation:terminated_by_player_id', '0',
      'relation:last_change_operation_id', operationID,
      'reactivation:state', reactivation and '3' or '0',
      'termination:order_id', '', 'termination:completed', '0')
    epoch = targetEpoch
    changed = true
  end
end
`

const pairFloorsOperation = `
local expectedEpoch = tonumber(ARGV[20])
local revision = tonumber(ARGV[21])
local lowFloor = tonumber(ARGV[22])
local highFloor = tonumber(ARGV[23])
local expectedRecordVersion = tonumber(ARGV[24]) or 0
local relationEpoch = num(KEYS[1], 'relation:epoch', 0)
epoch = relationEpoch
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local barrier = tonumber(redis.call('HGET', KEYS[1], 'reactivation:state') or '0')
local storedRevision = num(KEYS[1], 'reactivation:barrier_revision', 0)
local terminatedEpoch = num(KEYS[1], 'reactivation:terminated_relation_epoch', 0)
if redis.call('EXISTS', KEYS[1]) == 0 then
  status = 'NO_CHANGE'; reason = 'NOT_FRIENDS'
elseif barrier ~= 1 or storedRevision ~= revision or terminatedEpoch + 1 ~= expectedEpoch or relationEpoch ~= expectedEpoch then
  status = 'NO_CHANGE'; reason = 'DOMAIN_EPOCH_CONFLICT'
else
	  stageSet(KEYS[1], 'reactivation:state', '2',
    'reactivation:low_generation_floor', tostring(lowFloor),
    'reactivation:high_generation_floor', tostring(highFloor),
    'termination:low_floor_captured', '1', 'termination:high_floor_captured', '1')
  epoch = relationEpoch
  changed = true
end
`

const pairCompleteOperation = `
local orderID = ARGV[20]
local expectedRecordVersion = tonumber(ARGV[21]) or 0
epoch = num(KEYS[1], 'relation:epoch', 0)
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
if redis.call('HGET', KEYS[1], 'termination:order_id') ~= orderID then
	status = 'NO_CHANGE'; reason = ''
elseif redis.call('HGET', KEYS[1], 'termination:completed') == '1' then
	status = 'NO_CHANGE'; reason = ''
elseif redis.call('HGET', KEYS[1], 'termination:low_floor_captured') ~= '1' or
	   redis.call('HGET', KEYS[1], 'termination:high_floor_captured') ~= '1' or
	   redis.call('HGET', KEYS[1], 'termination:quota_released_low') ~= '1' or
	   redis.call('HGET', KEYS[1], 'termination:quota_released_high') ~= '1' then
	status = 'NO_CHANGE'; reason = 'RELATION_CLEANUP_PENDING'
elseif redis.call('HGET', KEYS[1], 'relation:status') ~= '2' or
       redis.call('HGET', KEYS[1], 'reactivation:state') ~= '2' or
       num(KEYS[1], 'relation:epoch', 0) ~= num(KEYS[1], 'termination:captured_epoch', 0) + 1 or
       num(KEYS[1], 'reactivation:terminated_relation_epoch', 0) ~= num(KEYS[1], 'termination:captured_epoch', 0) or
       num(KEYS[1], 'reactivation:barrier_revision', 0) ~= num(KEYS[1], 'termination:barrier_revision', 0) then
	status = 'NO_CHANGE'; reason = 'RELATION_CLEANUP_PENDING'
else
	  stageSet(KEYS[1], 'termination:completed', '1')
  changed = true
end
`

const pairQuotaReleasedOperation = `
local orderID = ARGV[20]
local playerID = ARGV[21]
local expectedRecordVersion = tonumber(ARGV[22]) or 0
epoch = num(KEYS[1], 'relation:epoch', 0)
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local storedOrder = redis.call('HGET', KEYS[1], 'termination:order_id')
if not storedOrder or storedOrder ~= orderID or redis.call('HGET', KEYS[1], 'termination:completed') == '1' then
  status = 'NO_CHANGE'; reason = ''
elseif playerID == (redis.call('HGET', KEYS[1], 'identity:low_player_id') or '') then
  if redis.call('HGET', KEYS[1], 'termination:quota_released_low') == '1' then
    status = 'NO_CHANGE'; reason = ''
  else
    stageSet(KEYS[1], 'termination:quota_released_low', '1')
    changed = true
  end
elseif playerID == (redis.call('HGET', KEYS[1], 'identity:high_player_id') or '') then
  if redis.call('HGET', KEYS[1], 'termination:quota_released_high') == '1' then
    status = 'NO_CHANGE'; reason = ''
  else
    stageSet(KEYS[1], 'termination:quota_released_high', '1')
    changed = true
  end
else
  return {'CONTRACT_ERROR'}
end
`

const quotaReserveOperation = `
local maxFriends = tonumber(ARGV[19])
local reservationID = ARGV[20]
local workflowID = ARGV[21]
local relationID = ARGV[22]
local targetEpoch = tonumber(ARGV[23])
local fenceDigest = ARGV[24]
local expectedRecordVersion = tonumber(ARGV[25]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then return {'RECORD_VERSION_CONFLICT'} end
if not maxFriends or maxFriends <= 0 or not targetEpoch or targetEpoch <= 0 or
   reservationID == '' or workflowID == '' or relationID == '' or fenceDigest == '' then
  return {'CONTRACT_ERROR'}
end
local storedLimit = redis.call('HGET', KEYS[1], '_meta:effective_max_friends')
if storedLimit and tonumber(storedLimit) ~= maxFriends then
  -- The versioned product profile is the current admission policy. Updating
  -- it is non-destructive: an occupancy already above the new limit remains
  -- grandfathered, while this and later fresh reservations fail closed.
  stageSet(KEYS[1], '_meta:effective_max_friends', tostring(maxFriends))
  changed = true
end
if not storedLimit then stageSet(KEYS[1], '_meta:effective_max_friends', tostring(maxFriends),
  '_meta:active_count', '0', '_meta:reserved_count', '0') end
epoch = targetEpoch
local state = redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':state')
if state then
  if state == '1' and redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':workflow_id') == workflowID and
     redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':relation_id') == relationID and
     num(KEYS[1], 'reservation:' .. reservationID .. ':target_epoch', 0) == targetEpoch then
    status = 'SUCCEEDED'
  else return {'IDEMPOTENCY_CONFLICT'} end
elseif redis.call('HGET', KEYS[1], 'active:' .. relationID) == tostring(targetEpoch) then
	status = 'NO_CHANGE'; reason = 'ALREADY_FRIENDS'; epoch = targetEpoch
elseif quotaFenceKey ~= '' and redis.call('HGET', quotaFenceKey, 'relation-release-fence:' .. fenceDigest) then
  status = 'NO_CHANGE'; reason = 'RELATION_ALREADY_TERMINATED'
elseif num(KEYS[1], '_meta:active_count', 0) + num(KEYS[1], '_meta:reserved_count', 0) >= maxFriends then
  status = 'REJECTED'; reason = 'FRIEND_LIMIT_REACHED'
else
	  stageSet(KEYS[1], 'reservation:' .. reservationID .. ':workflow_id', workflowID,
    'reservation:' .. reservationID .. ':relation_id', relationID,
    'reservation:' .. reservationID .. ':target_epoch', tostring(targetEpoch),
    'reservation:' .. reservationID .. ':state', '1',
    'reservation:' .. reservationID .. ':created_at_ms', tostring(now),
    'reservation_by_relation:' .. relationID .. ':' .. tostring(targetEpoch), reservationID)
	  stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) + 1))
  epoch = targetEpoch; changed = true
end
`

const quotaFinalizeOperation = `
local reservationID = ARGV[19]
local workflowID = ARGV[20]
local relationID = ARGV[21]
local targetEpoch = tonumber(ARGV[22])
local fenceDigest = ARGV[23]
local expectedRecordVersion = tonumber(ARGV[24]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then return {'RECORD_VERSION_CONFLICT'} end
if not targetEpoch or targetEpoch <= 0 or reservationID == '' or workflowID == '' or relationID == '' or fenceDigest == '' then
  return {'CONTRACT_ERROR'}
end
epoch = targetEpoch
local active = redis.call('HGET', KEYS[1], 'active:' .. relationID)
if active and active ~= tostring(targetEpoch) then
  return {'RELATION_EPOCH_CONFLICT'}
end
local reservationPrefix = 'reservation:' .. reservationID .. ':'
local state = redis.call('HGET', KEYS[1], reservationPrefix .. 'state')
if state then
  if state ~= '1' or redis.call('HGET', KEYS[1], reservationPrefix .. 'workflow_id') ~= workflowID or
     redis.call('HGET', KEYS[1], reservationPrefix .. 'relation_id') ~= relationID or
     num(KEYS[1], reservationPrefix .. 'target_epoch', 0) ~= targetEpoch then
    return {'IDEMPOTENCY_CONFLICT'}
  end
end
local fence = quotaFenceKey ~= '' and redis.call('HGET', quotaFenceKey, 'relation-release-fence:' .. fenceDigest) or false
if fence then
  if state == '1' then
    stageDelete(KEYS[1], reservationPrefix .. 'workflow_id', reservationPrefix .. 'relation_id',
      reservationPrefix .. 'target_epoch', reservationPrefix .. 'state', reservationPrefix .. 'created_at_ms',
      'reservation_by_relation:' .. relationID .. ':' .. tostring(targetEpoch))
    stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) - 1))
    changed = true
  end
  status = 'NO_CHANGE'; reason = 'RELATION_ALREADY_TERMINATED'
elseif active == tostring(targetEpoch) then
  -- The active fact may already have been installed by an earlier finalize.
  -- Consume only this matching reservation and never increment active_count.
  if state == '1' then
    stageDelete(KEYS[1], reservationPrefix .. 'workflow_id', reservationPrefix .. 'relation_id',
      reservationPrefix .. 'target_epoch', reservationPrefix .. 'state', reservationPrefix .. 'created_at_ms',
      'reservation_by_relation:' .. relationID .. ':' .. tostring(targetEpoch))
    stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) - 1))
    changed = true
  end
	status = 'SUCCEEDED'
elseif not state then
  return {'RESERVATION_NOT_FOUND'}
else
	stageSet(KEYS[1], 'active:' .. relationID, tostring(targetEpoch),
    '_meta:active_count', tostring(num(KEYS[1], '_meta:active_count', 0) + 1))
  stageDelete(KEYS[1], reservationPrefix .. 'workflow_id', reservationPrefix .. 'relation_id',
    reservationPrefix .. 'target_epoch', reservationPrefix .. 'state', reservationPrefix .. 'created_at_ms',
    'reservation_by_relation:' .. relationID .. ':' .. tostring(targetEpoch))
  stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) - 1))
	changed = true
end
`

const quotaReleaseOperation = `
local reservationID = ARGV[19]
local expectedRecordVersion = tonumber(ARGV[20]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then return {'RECORD_VERSION_CONFLICT'} end
epoch = num(KEYS[1], 'reservation:' .. reservationID .. ':target_epoch', 0)
if redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':state') == '1' then
  local relationID = redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':relation_id') or ''
  local targetEpoch = redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':target_epoch') or '0'
	  stageDelete(KEYS[1], 'reservation:' .. reservationID .. ':workflow_id',
    'reservation:' .. reservationID .. ':relation_id', 'reservation:' .. reservationID .. ':target_epoch',
    'reservation:' .. reservationID .. ':state', 'reservation:' .. reservationID .. ':created_at_ms',
    'reservation_by_relation:' .. relationID .. ':' .. targetEpoch)
	  stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) - 1))
  changed = true
else
  status = 'NO_CHANGE'
end
`

const quotaRemoveOperation = `
local relationID = ARGV[19]
local capturedEpoch = tonumber(ARGV[20])
local fenceDigest = ARGV[21]
local terminationOrderID = ARGV[22]
local participantInvocationID = ARGV[23]
local maxFriends = tonumber(ARGV[24])
local expectedRecordVersion = tonumber(ARGV[25]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then return {'RECORD_VERSION_CONFLICT'} end
if not capturedEpoch or capturedEpoch <= 0 or relationID == '' or fenceDigest == '' or terminationOrderID == '' or
   participantInvocationID == '' or not maxFriends or maxFriends <= 0 or maxFriends > 512 then
  return {'CONTRACT_ERROR'}
end
epoch = capturedEpoch
if quotaFenceKey ~= '' and redis.call('HGET', quotaFenceKey, 'relation-release-fence:' .. fenceDigest) then
	status = 'NO_CHANGE'
else
	if redis.call('EXISTS', KEYS[1]) == 0 then
	  stageSet(KEYS[1], '_meta:effective_max_friends', tostring(maxFriends),
	    '_meta:active_count', '0', '_meta:reserved_count', '0')
	end
	local fenceBlob = 'FRF1' .. textLP(ARGV[17]) .. textLP(relationID) .. textLP(tostring(capturedEpoch)) ..
		textLP(terminationOrderID) .. textLP(participantInvocationID) .. textLP(tostring(now))
	if #fenceBlob > tonumber(ARGV[#ARGV - 3]) then return {'CAPACITY_ITEM_LIMIT'} end
	stageSet(quotaFenceKey, 'relation-release-fence:' .. fenceDigest, fenceBlob)
	  if redis.call('HGET', KEYS[1], 'active:' .. relationID) == tostring(capturedEpoch) then
	    stageDelete(KEYS[1], 'active:' .. relationID)
	    stageSet(KEYS[1], '_meta:active_count', tostring(num(KEYS[1], '_meta:active_count', 0) - 1))
  end
  local reservationID = redis.call('HGET', KEYS[1], 'reservation_by_relation:' .. relationID .. ':' .. tostring(capturedEpoch))
  if reservationID and redis.call('HGET', KEYS[1], 'reservation:' .. reservationID .. ':state') == '1' then
	    stageDelete(KEYS[1], 'reservation:' .. reservationID .. ':workflow_id',
      'reservation:' .. reservationID .. ':relation_id', 'reservation:' .. reservationID .. ':target_epoch',
      'reservation:' .. reservationID .. ':state', 'reservation:' .. reservationID .. ':created_at_ms',
      'reservation_by_relation:' .. relationID .. ':' .. tostring(capturedEpoch))
	    stageSet(KEYS[1], '_meta:reserved_count', tostring(num(KEYS[1], '_meta:reserved_count', 0) - 1))
  end
  changed = true
end
`

const shareGenerateOperation = `
local newTokenID = ARGV[19]
local ttl = tonumber(ARGV[20])
local expectedRecordVersion = tonumber(ARGV[24]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local generation = num(KEYS[1], 'token:current_generation', 0)
local currentStatus = tonumber(redis.call('HGET', KEYS[1], 'token:status') or '0')
local expires = num(KEYS[1], 'token:expires_at_ms', 0)
if currentStatus == 1 and redis.call('HGET', KEYS[1], 'token:current_token_id') and now < expires then
  local currentID = redis.call('HGET', KEYS[1], 'token:current_token_id') or ''
  local issued = redis.call('HGET', KEYS[1], 'token:issued_at_ms') or '0'
  local purpose = redis.call('HGET', KEYS[1], 'token:purpose') or '0'
  local format = redis.call('HGET', KEYS[1], 'token:format_version') or '0'
	  typed = typedValue(currentID, tostring(generation), issued, tostring(expires), '1', purpose, format)
  epoch = generation
  status = 'NO_CHANGE'
else
  if newTokenID == '' then return {'CONTRACT_ERROR'} end
  generation = generation + 1
	  stageSet(KEYS[1], 'token:current_token_id', newTokenID, 'token:current_generation', tostring(generation),
    'token:issued_at_ms', tostring(now), 'token:expires_at_ms', tostring(now + ttl),
    'token:status', '1', 'token:purpose', '1', 'token:format_version', '1',
	    'token:key_id', ARGV[23], 'token:digest', ARGV[21], 'token:envelope', ARGV[22],
    'token:last_operation_id', ARGV[2])
	  typed = typedValue(newTokenID, tostring(generation), tostring(now), tostring(now + ttl), '1', '1', '1')
  epoch = generation; changed = true
end
`

const shareRotateOperation = `
local newTokenID = ARGV[19]
local ttl = tonumber(ARGV[20])
local expectedRecordVersion = tonumber(ARGV[24]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
if newTokenID == '' or ttl <= 0 then return {'CONTRACT_ERROR'} end
local generation = num(KEYS[1], 'token:current_generation', 0) + 1
	stageSet(KEYS[1], 'token:current_token_id', newTokenID, 'token:current_generation', tostring(generation),
  'token:issued_at_ms', tostring(now), 'token:expires_at_ms', tostring(now + ttl),
  'token:status', '1', 'token:purpose', '1', 'token:format_version', '1',
	  'token:key_id', ARGV[23], 'token:digest', ARGV[21], 'token:envelope', ARGV[22],
  'token:last_operation_id', ARGV[2])
	typed = typedValue(newTokenID, tostring(generation), tostring(now), tostring(now + ttl), '1', '1', '1')
epoch = generation; changed = true
`

const shareReactivateOperation = `
local newTokenID = ARGV[19]
local ttl = tonumber(ARGV[20])
local pairID = ARGV[21]
local proofEpoch = ARGV[22]
local proofRevision = ARGV[23]
local proofLowFloor = ARGV[24]
local proofHighFloor = ARGV[25]
local rotateID = ARGV[26]
local expectedRecordVersion = tonumber(ARGV[30]) or 0
local rotateOperationID = ARGV[31]
local rotatePayloadHash = ARGV[32]
local proofEpochNumber = tonumber(proofEpoch)
local proofRevisionNumber = tonumber(proofRevision)
local proofLowFloorNumber = tonumber(proofLowFloor)
local proofHighFloorNumber = tonumber(proofHighFloor)
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
if newTokenID == '' or ttl <= 0 or pairID == '' or not proofEpochNumber or proofEpochNumber <= 0 or
   not proofRevisionNumber or proofRevisionNumber <= 0 or not proofLowFloorNumber or proofLowFloorNumber < 0 or
   not proofHighFloorNumber or proofHighFloorNumber < 0 or rotateID == '' or rotateOperationID == '' or
   rotatePayloadHash == '' or #rotatePayloadHash ~= 64 then return {'CONTRACT_ERROR'} end
local generation = num(KEYS[1], 'token:current_generation', 0) + 1
	stageSet(KEYS[1], 'token:current_token_id', newTokenID, 'token:current_generation', tostring(generation),
  'token:issued_at_ms', tostring(now), 'token:expires_at_ms', tostring(now + ttl),
  'token:status', '1', 'token:purpose', '1', 'token:format_version', '1',
	  'token:key_id', ARGV[29], 'token:digest', ARGV[27], 'token:envelope', ARGV[28],
	  'token:last_operation_id', ARGV[2])
	typed = typedValue(newTokenID, tostring(generation), tostring(now), tostring(now + ttl), '1', '1', '1',
	  rotateID, pairID, proofEpoch, proofRevision, newTokenID, tostring(generation), tostring(now),
	  proofLowFloor, proofHighFloor, rotateOperationID, rotatePayloadHash)
epoch = generation; changed = true
`

const shareRevokeOperation = `
local expectedRecordVersion = tonumber(ARGV[19]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
if redis.call('EXISTS', KEYS[1]) == 0 then
	stageSet(KEYS[1], 'token:current_generation', '0', 'token:current_token_id', '', 'token:status', '0',
	  'token:purpose', '0', 'token:format_version', '0', 'token:key_id', '', 'token:issued_at_ms', '0',
	  'token:expires_at_ms', '0', 'token:digest', string.rep('0', 64), 'token:envelope', '',
	  'token:last_operation_id', '')
end
local generation = num(KEYS[1], 'token:current_generation', 0) + 1
local tokenID = redis.call('HGET', KEYS[1], 'token:current_token_id') or ''
local issued = redis.call('HGET', KEYS[1], 'token:issued_at_ms') or '0'
local expires = redis.call('HGET', KEYS[1], 'token:expires_at_ms') or '0'
local purpose = redis.call('HGET', KEYS[1], 'token:purpose') or '0'
local format = redis.call('HGET', KEYS[1], 'token:format_version') or '0'
	stageSet(KEYS[1], 'token:current_generation', tostring(generation), 'token:status', '2',
  'token:last_operation_id', ARGV[2])
	typed = typedValue(tokenID, tostring(generation), issued, expires, '2', purpose, format)
epoch = generation; changed = true
`

const shareAcceptOperation = `
local tokenID = ARGV[19]
local tokenGeneration = tonumber(ARGV[20])
local redeemer = ARGV[21]
local workflowID = ARGV[22]
local purpose = tonumber(ARGV[23])
local validationID = ARGV[24]
local currentID = redis.call('HGET', KEYS[1], 'token:current_token_id') or ''
local currentGeneration = num(KEYS[1], 'token:current_generation', 0)
local currentStatus = tonumber(redis.call('HGET', KEYS[1], 'token:status') or '0')
local currentPurpose = tonumber(redis.call('HGET', KEYS[1], 'token:purpose') or '0')
local currentExpiry = num(KEYS[1], 'token:expires_at_ms', 0)
epoch = tokenGeneration
if redeemer == ARGV[17] then
  status = 'REJECTED'; reason = 'SELF_ADD'
elseif purpose ~= 1 or currentPurpose ~= 1 then
  status = 'REJECTED'; reason = 'SHARE_TOKEN_PURPOSE_MISMATCH'
elseif currentStatus == 2 or currentID ~= tokenID then
  status = 'REJECTED'; reason = 'SHARE_TOKEN_REVOKED'
elseif currentGeneration ~= tokenGeneration then
  status = 'REJECTED'; reason = 'SHARE_TOKEN_STALE_GENERATION'
elseif currentStatus ~= 1 or now >= currentExpiry then
  status = 'REJECTED'; reason = 'SHARE_TOKEN_EXPIRED'
else
  -- The outer SHARE_ACCEPT AuthorityReceipt is the durable
  -- ShareTokenValidationReceipt.  Keep the validation payload in its typed
  -- value/receipt partition; never grow the canonical issuer record with a
  -- second validation history.
	  typed = typedValue(validationID, workflowID, tostring(tokenGeneration), ARGV[17], redeemer,
	    tostring(now), tokenID)
  changed = true
  status = 'SUCCEEDED'
end
`

const pairActionSource = `
-- KEYS: pair record, receipt, attempt, attempt-due, receipt-due, source-due,
--       bucket meta, dispatch order, result order
-- ARGV: the common 1..16 fields, order_count=17, two order tuples, then:
--       pair_id,bucket_id,layout,visitor,owner,target_farm,scope,binding_hex,
--       operation_id,operation_digest,permit_id,relation_id,
--       operation_id,payload_hex,target_binding,permit_ttl_ms,dispatch_id,
--       workflow_ttl_ms,expected_record_version,max_record_bytes
if not friendKeyTypeOK(KEYS[1], 'hash') or not friendKeyTypeOK(KEYS[2], 'hash') or
   not friendKeyTypeOK(KEYS[3], 'hash') or not friendKeyTypeOK(KEYS[4], 'zset') or
   not friendKeyTypeOK(KEYS[5], 'zset') or not friendKeyTypeOK(KEYS[6], 'zset') or
   not friendKeyTypeOK(KEYS[7], 'hash') or not friendKeyTypeOK(KEYS[8], 'hash') or
   not friendKeyTypeOK(KEYS[9], 'hash') then
  return {'CORRUPT_RECORD'}
end
local strictMetaOK, strictMetaWhy = validateBucketMeta(KEYS[7])
if not strictMetaOK then return {strictMetaWhy} end
local authorityMetaOK, authorityMetaWhy = validateBucketMetaAuthority(
  KEYS[7], KEYS[1], 'FRIEND_PAIR', ARGV[32], ARGV[31], false)
if not authorityMetaOK then return {authorityMetaWhy} end
local metaCountOK, metaCountWhy = validateBucketMetaCounts(KEYS[7], KEYS[4], KEYS[5], KEYS[6], false)
if not metaCountOK then return {metaCountWhy} end
local canonicalOK, canonicalWhy = validateCanonicalRecord(KEYS[1], 'FRIEND_PAIR', ARGV[30])
if not canonicalOK then return {canonicalWhy} end
local existing = redis.call('HGET', KEYS[2], ARGV[4])
if existing then
  if string.sub(existing, 1, #ARGV[3]) == ARGV[3] then
    if not receiptCandidateMatches(existing, ARGV[3], ARGV[8]) then return {'IDEMPOTENCY_CONFLICT'} end
	if not markerVerify(KEYS[3], ARGV[2], ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
		KEYS[1], KEYS[2], KEYS[5], KEYS[7], KEYS[6], '') then return {'RECONCILE_REQUIRED'} end
    return {'REPLAY', existing}
  end
  return {'IDEMPOTENCY_CONFLICT'}
end
local ok, why = checkOpenCursor(KEYS[3], ARGV[2], ARGV[7], ARGV[6], ARGV[8])
if not ok then return {why} end
local pairID = ARGV[30]
local bucketID = ARGV[31]
local layout = ARGV[32]
local visitor = ARGV[33]
local owner = ARGV[34]
local targetFarm = ARGV[35]
local scope = tonumber(ARGV[36])
local bindingHex = ARGV[37]
local operationID = ARGV[38]
local operationDigest = ARGV[39]
local permitID64 = ARGV[40]
local relationID64 = ARGV[41]
local operationID64 = ARGV[42]
local payloadHex = ARGV[43]
local binding64 = ARGV[44]
local ttl = tonumber(ARGV[45])
local dispatchID = ARGV[46]
local workflowTTL = tonumber(ARGV[47])
local expectedRecordVersion = tonumber(ARGV[48]) or 0
local maxRecordBytes = tonumber(ARGV[49]) or 0
if num(KEYS[1], '_meta:record_version', 0) ~= expectedRecordVersion then
  return {'RECORD_VERSION_CONFLICT'}
end
local metaOK, metaWhy = checkMeta(KEYS[7], 'FRIEND_PAIR', layout, bucketID)
if not metaOK then return {metaWhy} end
local low = redis.call('HGET', KEYS[1], 'identity:low_player_id') or ''
local high = redis.call('HGET', KEYS[1], 'identity:high_player_id') or ''
local relationStatus = redis.call('HGET', KEYS[1], 'relation:status') or '0'
local relationEpoch = num(KEYS[1], 'relation:epoch', 0)
local activeBlock = (redis.call('HGET', KEYS[1], 'block:low_to_high:status') or '0') == '1' or
  (redis.call('HGET', KEYS[1], 'block:high_to_low:status') or '0') == '1'
local accepted = relationStatus == '1' and not activeBlock and
  ((visitor == low and owner == high) or (visitor == high and owner == low)) and
  targetFarm == owner and (scope == 2 or scope == 3) and bindingHex ~= '' and ttl > 0 and workflowTTL > ttl
local status = 'SUCCEEDED'
local reason = ''
local epoch = relationEpoch
local detail = ''
local typed = ''
local usedOrders = 1
if not accepted then
  status = 'REJECTED'; reason = 'VISIT_NOT_PERMITTED'
else
  usedOrders = 2
  local expires = now + ttl
	  typed = typedValue(permitID64, relationID64, tostring(relationEpoch), visitor, owner, targetFarm,
	    operationID64, tostring(scope), payloadHex, binding64, tostring(now), tostring(expires), tostring(now), dispatchID,
	    tostring(now + workflowTTL))
end
local receiptBlob = receiptValue(ARGV[5], status, reason, epoch, detail, typed, ARGV[3])
if #receiptBlob > tonumber(ARGV[#ARGV - 4]) then return {'CAPACITY_ITEM_LIMIT'} end
local receiptCount = num(KEYS[7], 'receipt_count', 0)
local receiptBytes = num(KEYS[7], 'receipt_bytes', 0)
local pendingCount = num(KEYS[7], 'pending_work_count', 0)
local pendingBytes = num(KEYS[7], 'pending_work_bytes', 0)
local newOrders = 0
local newOrderBytes = 0
for i = 1, usedOrders do
	local oi = 18 + (i - 1) * 6
  local old = redis.call('HGET', KEYS[7 + i], 'payload_hash')
	if sourceOrderLogicalBytes(ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5]) > tonumber(ARGV[#ARGV - 3]) then
		return {'CAPACITY_ITEM_LIMIT'}
	end
	local orderOK, orderState = validateSourceOrder(KEYS[7 + i], KEYS[6], ARGV[oi], ARGV[oi + 1], ARGV[oi + 2],
		ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5], layout, bucketID, pairID,
		ARGV[oi + 1] == 'FRIEND_ACTION_DISPATCH_ORDER' and 'DISPATCH' or 'ACTION_RESULT_OPTIONAL')
	if not orderOK then return {orderState} end
	if orderState == 'EXISTING' and accepted and ARGV[oi + 1] == 'FRIEND_ACTION_DISPATCH_ORDER' then
		local issued = tonumber(redis.call('HGET', KEYS[7 + i], 'issued_at_authority_ms') or '')
		local expires = tonumber(redis.call('HGET', KEYS[7 + i], 'expire_at_authority_ms') or '')
		local acceptedAt = tonumber(redis.call('HGET', KEYS[7 + i], 'execution_accepted_at_authority_ms') or '')
		local workflowDeadline = tonumber(redis.call('HGET', KEYS[7 + i], 'workflow_deadline_at_ms') or '')
		if redis.call('HGET', KEYS[7 + i], 'permit_id') ~= permitID64 or
			redis.call('HGET', KEYS[7 + i], 'relation_id') ~= relationID64 or
			redis.call('HGET', KEYS[7 + i], 'relation_epoch') ~= tostring(relationEpoch) or
			redis.call('HGET', KEYS[7 + i], 'visitor_player_id') ~= visitor or
			redis.call('HGET', KEYS[7 + i], 'owner_player_id') ~= owner or
			redis.call('HGET', KEYS[7 + i], 'target_farm_id') ~= targetFarm or
			redis.call('HGET', KEYS[7 + i], 'operation_id') ~= operationID64 or
			redis.call('HGET', KEYS[7 + i], 'scope') ~= tostring(scope) or
			redis.call('HGET', KEYS[7 + i], 'permit_payload_hash') ~= payloadHex or
			redis.call('HGET', KEYS[7 + i], 'target_binding_hash') ~= binding64 or
			redis.call('HGET', KEYS[7 + i], 'dispatch_order_id') ~= dispatchID or
			not issued or not expires or not acceptedAt or not workflowDeadline or issued <= 0 or expires < issued or
			acceptedAt < issued or acceptedAt > expires or workflowDeadline - issued ~= workflowTTL or workflowDeadline <= expires then
			return {'WORK_RECONCILE'}
		end
	elseif orderState == 'EXISTING' and accepted and ARGV[oi + 1] == 'RESULT_PUBLISH_ORDER' then
		if redis.call('HGET', KEYS[7 + i], 'dispatch_order_id') ~= dispatchID then return {'WORK_RECONCILE'} end
	end
	if orderState == 'MISSING' then
		local orderBytes = sourceOrderLogicalBytes(ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5])
		newOrders = newOrders + 1; newOrderBytes = newOrderBytes + orderBytes
	  end
end
if receiptCount + 1 > tonumber(ARGV[11]) or receiptBytes + #receiptBlob > tonumber(ARGV[12]) then return {'CAPACITY_RECEIPT'} end
if pendingCount + newOrders > tonumber(ARGV[13]) or pendingBytes + newOrderBytes > tonumber(ARGV[14]) then return {'CAPACITY_WORK'} end
if accepted then
	local oldVersion = num(KEYS[1], '_meta:record_version', 0)
	stageSet(KEYS[1], '_meta:schema_version', 'friend-pair-v1',
		'_meta:authority_layout_version', layout, '_meta:record_version', tostring(oldVersion + 1),
		'_meta:updated_at_ms', tostring(now), '_meta:business_state_digest', ARGV[8])
end
local plannedRecordBytes, plannedRecordErr = plannedBytes(KEYS[1])
if plannedRecordErr ~= '' then return {plannedRecordErr} end
if maxRecordBytes > 0 and plannedRecordBytes > maxRecordBytes then return {'CAPACITY_BYTE_LIMIT'} end
local markerOrders = {}
local markerTuples = {}
for i = 1, usedOrders do
  local oi = 18 + (i - 1) * 6
  markerOrders[#markerOrders + 1] = KEYS[7 + i]
  markerTuples[#markerTuples + 1] = {ARGV[oi], ARGV[oi + 1], ARGV[oi + 2], ARGV[oi + 3], ARGV[oi + 4], ARGV[oi + 5]}
end
local finalVersion = num(KEYS[1], '_meta:record_version', 0)
if accepted then finalVersion = finalVersion + 1 end
local finalDigest = accepted and ARGV[8] or (redis.call('HGET', KEYS[1], '_meta:business_state_digest') or '0')
local markerOK, markerSize = writeCommitMarker(KEYS[3], ARGV[#ARGV], ARGV[7], tonumber(ARGV[6]), ARGV[8],
    KEYS[1], redis.call('EXISTS', KEYS[1]) == 1, finalVersion, finalDigest, plannedRecordBytes, KEYS[2], ARGV[4], ARGV[3],
    receiptBlob, ARGV[5], status, epoch, KEYS[5], ARGV[10], now + tonumber(ARGV[16]), KEYS[7], KEYS[6],
	markerOrders, markerTuples, '', '', '', '', '', layout, bucketID, 'FRIEND_PAIR', pairID,
	receiptCount + 1, receiptBytes + #receiptBlob,
	pendingCount + newOrders, pendingBytes + newOrderBytes, tonumber(ARGV[#ARGV - 2]), tonumber(ARGV[#ARGV - 1]))
if not markerOK then return {'CAPACITY_MARKER_' .. tostring(markerSize or 0)} end
prepareMeta(KEYS[7], 'FRIEND_PAIR', layout, bucketID)
if #staged > 0 then applyStaged(plannedRecordBytes) end
for i = 1, usedOrders do
	local oi = 18 + (i - 1) * 6
	local orderKey = KEYS[7 + i]
	if redis.call('EXISTS', orderKey) == 0 then
		local orderPayload = ARGV[oi + 2]
		redis.call('HSET', orderKey, 'schema_version', 'friend-source-order-v1', 'work_id', ARGV[oi],
      'work_type', ARGV[oi + 1], 'placement_kind', 'SOURCE_PERSISTENCE_BUCKET', 'bucket_version', layout,
      'bucket_id', bucketID, 'source_authority_kind', 'FRIEND_PAIR', 'canonical_source_authority_id', pairID,
	      'state', 'PENDING', 'work_version', '1', 'typed_payload', orderPayload, 'payload_hash', ARGV[oi + 4],
	      'work_ref', ARGV[oi + 3], 'next_due_at_ms', ARGV[oi + 5], 'retry_count', '0', 'lease_epoch', '0', 'lease_holder_id', '',
      'lease_token_hash', '', 'lease_expire_at_ms', '0', 'created_at_ms', tostring(now),
      'updated_at_ms', tostring(now), 'completed_at_ms', '0')
		if ARGV[oi + 1] == 'FRIEND_ACTION_DISPATCH_ORDER' and accepted then
		  redis.call('HSET', orderKey,
		    'permit_id', permitID64, 'relation_id', relationID64, 'relation_epoch', tostring(relationEpoch),
		    'visitor_player_id', visitor, 'owner_player_id', owner, 'target_farm_id', targetFarm,
		    'operation_id', operationID64, 'scope', tostring(scope), 'permit_payload_hash', payloadHex,
		    'target_binding_hash', binding64, 'issued_at_authority_ms', tostring(now),
		    'expire_at_authority_ms', tostring(now + ttl), 'execution_accepted_at_authority_ms', tostring(now),
		    'workflow_deadline_at_ms', tostring(now + workflowTTL),
		    'dispatch_order_id', dispatchID)
		elseif ARGV[oi + 1] == 'RESULT_PUBLISH_ORDER' and accepted then
		  -- The result order is the stable operation-scoped locator used to
		  -- recover the dispatch id after the AdmissionReceipt is compacted.
		  redis.call('HSET', orderKey, 'dispatch_order_id', dispatchID)
		end
		redis.call('ZADD', KEYS[6], tonumber(ARGV[oi + 5]), ARGV[oi + 3])
  end
end
redis.call('HSET', KEYS[2], ARGV[4], receiptBlob)
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[16]), ARGV[10])
redis.call('HINCRBY', KEYS[7], 'receipt_count', 1)
redis.call('HINCRBY', KEYS[7], 'receipt_bytes', #receiptBlob)
redis.call('HINCRBY', KEYS[7], 'pending_work_count', newOrders)
redis.call('HINCRBY', KEYS[7], 'pending_work_bytes', newOrderBytes)
redis.call('HSET', KEYS[7], 'last_updated_at_ms', tostring(now))
completeCommitMarker(KEYS[3], ARGV[#ARGV])
	commitCursor(KEYS[3], ARGV[2], KEYS[4], ARGV[9], 'COMMITTED', ARGV[15])
return {'COMMITTED', receiptBlob, tostring(num(KEYS[1], '_meta:record_version', 0)), tostring(epoch), accepted and dispatchID or ''}
`

func friendScriptSources() map[string]string {
	return map[string]string{
		scriptFriendReadCompleteSnapshot: friendAuthorityCompleteSnapshotSource,
		scriptFriendPrepare:              friendPrepareSource,
		scriptFriendResolve:              friendResolveSource,
		scriptFriendCompact:              friendCompactMarkerSource,
		scriptPairDelete:                 pairMutationSource("DELETE"),
		scriptPairBlock:                  pairMutationSource("BLOCK"),
		scriptPairUnblock:                pairMutationSource("UNBLOCK"),
		scriptPairAction:                 friendLuaPrelude + pairActionSource,
		scriptPairActivate:               friendSimpleSource("ACTIVATE_RELATION", pairActivateOperation),
		scriptPairFloors:                 friendSimpleSource("REACTIVATION_FLOORS", pairFloorsOperation),
		scriptPairComplete:               friendSimpleSource("COMPLETE_TERMINATION", pairCompleteOperation),
		scriptPairQuotaReleased:          friendSimpleSource("PAIR_QUOTA_RELEASED", pairQuotaReleasedOperation),
		scriptQuotaReserve:               friendSimpleSource("QUOTA_RESERVE", quotaReserveOperation),
		scriptQuotaFinalize:              friendSimpleSource("QUOTA_FINALIZE", quotaFinalizeOperation),
		scriptQuotaRelease:               friendSimpleSource("QUOTA_RELEASE", quotaReleaseOperation),
		scriptQuotaRemove:                friendSimpleSource("QUOTA_REMOVE", quotaRemoveOperation),
		scriptShareGenerate:              friendSimpleSource("SHARE_GENERATE", shareGenerateOperation),
		scriptShareRotate:                friendSimpleSource("SHARE_ROTATE", shareRotateOperation),
		scriptShareReactivate:            friendSimpleSource("SHARE_REACTIVATE", shareReactivateOperation),
		scriptShareRevoke:                friendSimpleSource("SHARE_REVOKE", shareRevokeOperation),
		scriptShareAccept:                friendSimpleSource("SHARE_ACCEPT", shareAcceptOperation),
	}
}
