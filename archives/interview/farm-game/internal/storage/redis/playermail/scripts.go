package playermail

import (
	"crypto/sha1" //nolint:gosec // SHA-1 is the Redis script cache identity, not a security primitive.
	"strings"
)

// Script names. Sources are immutable once registered; any change requires
// a new script version name because golden tests pin the exact SHA-1.
const (
	ScriptInsertMailIfAbsent       = "InsertMailIfAbsentV1"
	ScriptMarkMailRead             = "MarkMailReadV1"             // frozen, superseded by V2
	ScriptDeleteMail               = "DeleteMailV1"               // frozen, superseded by V2
	ScriptApplyMailClaimProjection = "ApplyMailClaimProjectionV1" // frozen, superseded by V2
	ScriptExpireMailVisibility     = "ExpireMailVisibilityV1"
	ScriptGcMail                   = "GcMailV1" // frozen, superseded by V2
	ScriptClaimMailScanLease       = "ClaimMailScanLeaseV1"

	// V2 family (I0 review FIX-1/2/3): counter transitions use the
	// materialized-count predicate instead of a wall-clock expire check
	// (FIX-1); DeleteMailV2 binds the claim proof to mail id + claim
	// material hash + receipt ref (FIX-2); GcMailV2 purges Entry/State
	// together with the delivery dedup record at their shared effective
	// purge point (FIX-3).
	ScriptMarkMailReadV2             = "MarkMailReadV2"
	ScriptDeleteMailV2               = "DeleteMailV2"
	ScriptApplyMailClaimProjectionV2 = "ApplyMailClaimProjectionV2"
	ScriptGcMailV2                   = "GcMailV2"
	ScriptMarkMailReadV3             = "MarkMailReadV3"
	ScriptDeleteMailV3               = "DeleteMailV3"
	ScriptApplyMailClaimProjectionV3 = "ApplyMailClaimProjectionV3"
	ScriptGcMailV3                   = "GcMailV3"
	ScriptGcMailReceiptV1            = "GcMailReceiptV1"
	// Maintenance V2/V4 atomically repairs stale due members and advances
	// Redis-time NOT_DUE scores. Older names remain byte-frozen history.
	ScriptExpireMailVisibilityV2 = "ExpireMailVisibilityV2"
	ScriptGcMailV4               = "GcMailV4"
	ScriptGcMailReceiptV2        = "GcMailReceiptV2"
)

// Script return protocol: every script returns an array whose first element
// is a string status token. UNAVAILABLE_NOT_SENT and TIMEOUT_UNKNOWN are
// never produced by Lua; they are classified in Go from transport errors.
// CROSS_SLOT and RECONCILE_REQUIRED fail closed as Go contract errors.

// luaCommon holds the shared LP/time helpers. Each script embeds it
// verbatim; the script golden tests freeze the bytes.
const luaCommon = `
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
local function lp_rebuild(f)
  local out = lp1(f[1])
  for i = 2, #f do out = out .. lp1(f[i]) end
  return out
end
local function fmt_ms(v)
  return string.format('%.0f', v)
end
local tag = ARGV[1]
for i = 1, #KEYS do
  if string.sub(KEYS[i], 1, #tag + 2) ~= '{' .. tag .. '}' then
    return {'CROSS_SLOT'}
  end
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local M_REVISION = '_meta:mailbox_revision'
local M_VISIBLE = '_meta:visible_count'
local M_RETAINED = '_meta:retained_count'
local M_RETAINED_BYTES = '_meta:retained_bytes'
local M_UNREAD = '_meta:unread_count'
local M_CLAIMABLE = '_meta:claimable_count'
local M_DELIVERY_RECORDS = '_meta:delivery_tombstone_count'
local M_SCHEMA = '_meta:schema_version'

local function read_meta(box)
  local m = redis.call('HMGET', box, M_REVISION, M_VISIBLE, M_RETAINED,
    M_RETAINED_BYTES, M_UNREAD, M_CLAIMABLE, M_DELIVERY_RECORDS)
  return {
    revision = tonumber(m[1] or '0'),
    visible = tonumber(m[2] or '0'),
    retained = tonumber(m[3] or '0'),
    retained_bytes = tonumber(m[4] or '0'),
    unread = tonumber(m[5] or '0'),
    claimable = tonumber(m[6] or '0'),
    delivery_records = tonumber(m[7] or '0')
  }
end
local function write_meta(box, meta)
  redis.call('HSET', box, M_REVISION, fmt_ms(meta.revision),
    M_VISIBLE, fmt_ms(meta.visible),
    M_RETAINED, fmt_ms(meta.retained),
    M_RETAINED_BYTES, fmt_ms(meta.retained_bytes),
    M_UNREAD, fmt_ms(meta.unread),
    M_CLAIMABLE, fmt_ms(meta.claimable),
    M_DELIVERY_RECORDS, fmt_ms(meta.delivery_records))
end
local function summary_tail(status, extra, meta)
  local out = {status}
  for _, v in ipairs(extra) do out[#out + 1] = v end
  out[#out + 1] = fmt_ms(meta.revision)
  out[#out + 1] = fmt_ms(meta.visible)
  out[#out + 1] = fmt_ms(meta.unread)
  out[#out + 1] = fmt_ms(meta.claimable)
  out[#out + 1] = fmt_ms(meta.retained)
  out[#out + 1] = fmt_ms(meta.retained_bytes)
  out[#out + 1] = fmt_ms(meta.delivery_records)
  return out
end
-- sf: MailUserStateV1 fields; ef: MailEntryV1 fields.
local function is_visible(sf, ef)
  return tonumber(sf[4]) == 0 and sf[12] == '0' and tonumber(sf[5]) == 0 and
    tonumber(ef[17]) > now
end
local function bounded_zrem_by_mail(zkey, mail_id)
  local members = redis.call('ZRANGE', zkey, 0, 127)
  for _, m in ipairs(members) do
    if string.sub(m, 10) == mail_id then
      redis.call('ZREM', zkey, m)
    end
  end
end
`

const luaInsertMailIfAbsentSrc = luaCommon + `
-- KEYS: box, index, safe-evict, expire-due, gc-due
local entry_field = ARGV[2]
local entry_blob = ARGV[3]
local state_field = ARGV[4]
local state_blob = ARGV[5]
local delivery_field = ARGV[6]
local recipient = ARGV[7]
local delivery_id = ARGV[8]
local delivery_hash = ARGV[9]
local mail_id = ARGV[10]
local sort_member = ARGV[11]
local expire_due_member = ARGV[12]
local gc_due_member = ARGV[13]
local visible_at = tonumber(ARGV[14])
local expire_at = tonumber(ARGV[15])
local purge_at = tonumber(ARGV[16])
local has_attachment = ARGV[17]
local max_visible = tonumber(ARGV[18])
local max_retained = tonumber(ARGV[19])
local max_retained_bytes = tonumber(ARGV[20])
local max_delivery_records = tonumber(ARGV[21])
local max_lazy_evict = tonumber(ARGV[22])
local max_entry_bytes = tonumber(ARGV[23])
local max_state_bytes = tonumber(ARGV[24])
local delivery_retain_ms = tonumber(ARGV[25])

if visible_at > now then
  return {'NOT_VISIBLE_YET'}
end
if #entry_blob > max_entry_bytes or #state_blob > max_state_bytes then
  return {'CAPACITY_EXCEEDED', 'arg_bytes'}
end

local existing = redis.call('HGET', KEYS[1], delivery_field)
if existing then
  local f = lp_fields(existing)
  if not f or #f < 8 then
    return {'RECONCILE_REQUIRED', 'dedup_blob'}
  end
  if f[4] == delivery_hash then
    local meta = read_meta(KEYS[1])
    return summary_tail('ALREADY_DELIVERED', {f[5]}, meta)
  end
  return {'DELIVERY_ID_CONFLICT'}
end
if redis.call('HEXISTS', KEYS[1], entry_field) == 1 then
  return {'RECONCILE_REQUIRED', 'entry_without_dedup'}
end

local meta = read_meta(KEYS[1])
local delta = #entry_blob + #state_blob
-- Every capacity check completes before the first write (31 §6.2).
if meta.retained + 1 > max_retained or meta.retained_bytes + delta > max_retained_bytes then
  return {'CAPACITY_EXCEEDED', 'retained'}
end
if meta.delivery_records + 1 > max_delivery_records then
  return {'CAPACITY_EXCEEDED', 'delivery_records'}
end

-- Bounded lazy eviction of the oldest safe mail (35A §4.1).
local evicted = 0
local iterations = 0
while meta.visible >= max_visible and evicted < max_lazy_evict and iterations < max_lazy_evict * 3 do
  iterations = iterations + 1
  local cand = redis.call('ZRANGE', KEYS[3], -1, -1)
  if #cand == 0 then break end
  local member = cand[1]
  local mid = string.sub(member, 10)
  local sblob = redis.call('HGET', KEYS[1], 'state:' .. mid)
  local eblob = redis.call('HGET', KEYS[1], 'entry:' .. mid)
  redis.call('ZREM', KEYS[3], member)
  if sblob and eblob then
    local sf = lp_fields(sblob)
    local ef = lp_fields(eblob)
    if sf and ef and #sf >= 12 and #ef >= 20 then
      if tonumber(sf[3]) > 0 and tonumber(sf[4]) == 0 and tonumber(sf[5]) == 0 and
         sf[12] == '0' and (ef[20] == '0' or sf[6] == '1') then
        sf[12] = '1'
        sf[11] = fmt_ms(tonumber(sf[11]) + 1)
        local nb = lp_rebuild(sf)
        redis.call('HSET', KEYS[1], 'state:' .. mid, nb)
        redis.call('ZREM', KEYS[2], member)
        meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
        meta.visible = meta.visible - 1
        evicted = evicted + 1
      end
    end
  end
end
if meta.visible >= max_visible then
  return {'MAILBOX_FULL_PROTECTED'}
end

local dedup_blob = lp1('DeliveryDedupV1') .. lp1(recipient) .. lp1(delivery_id) ..
  lp1(delivery_hash) .. lp1(mail_id) .. lp1(fmt_ms(now)) ..
  lp1(fmt_ms(now + delivery_retain_ms)) .. lp1('0')
redis.call('HSET', KEYS[1], M_SCHEMA, 'player-mail-box-v1',
  entry_field, entry_blob, state_field, state_blob, delivery_field, dedup_blob)
meta.revision = meta.revision + 1
meta.visible = meta.visible + 1
meta.unread = meta.unread + 1
if has_attachment == '1' then
  meta.claimable = meta.claimable + 1
end
meta.retained = meta.retained + 1
meta.retained_bytes = meta.retained_bytes + delta
meta.delivery_records = meta.delivery_records + 1
write_meta(KEYS[1], meta)
redis.call('ZADD', KEYS[2], 0, sort_member)
redis.call('ZADD', KEYS[4], expire_at, expire_due_member)
redis.call('ZADD', KEYS[5], purge_at, gc_due_member)
return summary_tail('INSERTED', {mail_id}, meta)
`

const luaMarkMailReadSrc = luaCommon + `
-- KEYS: box, safe-evict
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local mutation_id = ARGV[7]
local retain_window = tonumber(ARGV[8])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'MARK_READ' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'MAIL_NOT_FOUND'
  elseif tonumber(sf[3]) > 0 then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    local was_visible = is_visible(sf, ef)
    sf[3] = fmt_ms(now)
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    local nb = lp_rebuild(sf)
    redis.call('HSET', KEYS[1], state_field, nb)
    meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
    if was_visible and meta.unread > 0 then
      meta.unread = meta.unread - 1
    end
    if ef[20] == '0' or sf[6] == '1' then
      redis.call('ZADD', KEYS[2], 0, sort_member)
    end
    meta.revision = meta.revision + 1
    status = 'READ_MARKED'
  end
end
local rb = lp1('MutationReceiptV1') .. lp1('MARK_READ') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], mutation_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaDeleteMailSrc = luaCommon + `
-- KEYS: box, index, safe-evict
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local eligibility = ARGV[7]
local mutation_id = ARGV[8]
local retain_window = tonumber(ARGV[9])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'DELETE' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    -- Defense in depth (35A §4.3): the script re-checks the server-
    -- constructed eligibility against the immutable entry facts.
    local allow = false
    if ef[20] == '0' then
      allow = true
    elseif now > tonumber(ef[18]) then
      allow = true
    elseif sf[6] == '1' then
      allow = true
    elseif eligibility == 'ALLOW_WITH_CLAIM_PROOF' then
      allow = true
    end
    if not allow then
      status = 'REJECT_UNCLAIMED'
    else
      local was_visible = is_visible(sf, ef)
      local was_unread = tonumber(sf[3]) == 0
      local was_claimable = ef[20] == '1' and sf[6] == '0'
      sf[4] = fmt_ms(now)
      sf[11] = fmt_ms(tonumber(sf[11]) + 1)
      local nb = lp_rebuild(sf)
      redis.call('HSET', KEYS[1], state_field, nb)
      meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
      if was_visible then
        redis.call('ZREM', KEYS[2], sort_member)
        redis.call('ZREM', KEYS[3], sort_member)
        if meta.visible > 0 then
          meta.visible = meta.visible - 1
        end
        if was_unread and meta.unread > 0 then
          meta.unread = meta.unread - 1
        end
        if was_claimable and meta.claimable > 0 then
          meta.claimable = meta.claimable - 1
        end
      end
      meta.revision = meta.revision + 1
      status = 'DELETED'
    end
  end
end
local rb = lp1('MutationReceiptV1') .. lp1('DELETE') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], mutation_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaApplyMailClaimProjectionSrc = luaCommon + `
-- KEYS: box, safe-evict
local projection_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local work_id = ARGV[7]
local work_version = ARGV[8]
local claim_receipt_ref = ARGV[9]
local claim_hash = ARGV[10]
local claimed_at = ARGV[11]
local retain_window = tonumber(ARGV[12])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], projection_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == work_id and f[3] == work_version and f[4] == mail_id then
    return summary_tail('RECEIPT', {f[6], f[7]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'projection_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'TARGET_ALREADY_PURGED'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  local eblob = redis.call('HGET', KEYS[1], entry_field)
  if not eblob then
    return {'RECONCILE_REQUIRED', 'entry_missing'}
  end
  local ef = lp_fields(eblob)
  if not ef or #ef < 20 then
    return {'RECONCILE_REQUIRED', 'entry_blob'}
  end
  if ef[14] == '' or ef[14] ~= claim_hash then
    status = 'CLAIM_MATERIAL_HASH_MISMATCH'
  elseif sf[6] == '1' then
    status = 'NO_CHANGE'
  else
    local was_visible = is_visible(sf, ef)
    -- Monotonic false -> true only: never clears deleted_at and never
    -- re-adds the index member (35A PM3).
    sf[6] = '1'
    sf[7] = claim_receipt_ref
    sf[9] = claimed_at
    sf[10] = work_version
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    local nb = lp_rebuild(sf)
    redis.call('HSET', KEYS[1], state_field, nb)
    meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
    if was_visible and ef[20] == '1' and meta.claimable > 0 then
      meta.claimable = meta.claimable - 1
    end
    if tonumber(sf[3]) > 0 then
      redis.call('ZADD', KEYS[2], 0, sort_member)
    end
    meta.revision = meta.revision + 1
    status = 'APPLIED'
  end
end
local rb = lp1('ProjectionReceiptV1') .. lp1(work_id) .. lp1(work_version) ..
  lp1(mail_id) .. lp1(claim_hash) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], projection_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaExpireMailVisibilitySrc = luaCommon + `
-- KEYS: box, index, safe-evict, expire-due
local state_field = ARGV[2]
local entry_field = ARGV[3]
local mail_id = ARGV[4]
local sort_member = ARGV[5]
local due_member = ARGV[6]
local mail_version = ARGV[7]
local expected_state_version = ARGV[8]

local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  redis.call('ZREM', KEYS[4], due_member)
  return {'TARGET_ALREADY_PURGED'}
end
local sf = lp_fields(sblob)
if not sf or #sf < 12 then
  return {'RECONCILE_REQUIRED', 'state_blob'}
end
local eblob = redis.call('HGET', KEYS[1], entry_field)
if not eblob then
  redis.call('ZREM', KEYS[4], due_member)
  return {'RECONCILE_REQUIRED', 'entry_missing'}
end
local ef = lp_fields(eblob)
if not ef or #ef < 20 then
  return {'RECONCILE_REQUIRED', 'entry_blob'}
end
if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', sf[11], ef[17]}
end
if tonumber(sf[4]) > 0 or sf[12] == '1' or tonumber(sf[5]) > 0 then
  redis.call('ZREM', KEYS[4], due_member)
  return {'NO_CHANGE', sf[11], ef[17]}
end
if now < tonumber(ef[17]) then
  return {'NOT_DUE', sf[11], ef[17]}
end

local meta = read_meta(KEYS[1])
local was_unread = tonumber(sf[3]) == 0
local was_claimable = ef[20] == '1' and sf[6] == '0'
sf[5] = fmt_ms(now)
sf[11] = fmt_ms(tonumber(sf[11]) + 1)
local nb = lp_rebuild(sf)
redis.call('HSET', KEYS[1], state_field, nb)
meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
redis.call('ZREM', KEYS[2], sort_member)
redis.call('ZREM', KEYS[3], sort_member)
if meta.visible > 0 then
  meta.visible = meta.visible - 1
end
if was_unread and meta.unread > 0 then
  meta.unread = meta.unread - 1
end
if was_claimable and meta.claimable > 0 then
  meta.claimable = meta.claimable - 1
end
meta.revision = meta.revision + 1
write_meta(KEYS[1], meta)
redis.call('ZREM', KEYS[4], due_member)
local out = summary_tail('EXPIRED', {sf[11], ef[17]}, meta)
return out
`

const luaGcMailSrc = luaCommon + `
-- KEYS: box, index, safe-evict, gc-due
local state_field = ARGV[2]
local entry_field = ARGV[3]
local delivery_field = ARGV[4]
local mail_id = ARGV[5]
local due_member = ARGV[6]
local mail_version = ARGV[7]
local expected_state_version = ARGV[8]

local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  if redis.call('HEXISTS', KEYS[1], entry_field) == 1 then
    return {'RECONCILE_REQUIRED', 'entry_without_state'}
  end
  bounded_zrem_by_mail(KEYS[2], mail_id)
  bounded_zrem_by_mail(KEYS[3], mail_id)
  redis.call('ZREM', KEYS[4], due_member)
  return {'TARGET_ALREADY_PURGED'}
end
local sf = lp_fields(sblob)
if not sf or #sf < 12 then
  return {'RECONCILE_REQUIRED', 'state_blob'}
end
local eblob = redis.call('HGET', KEYS[1], entry_field)
if not eblob then
  return {'RECONCILE_REQUIRED', 'entry_missing'}
end
local ef = lp_fields(eblob)
if not ef or #ef < 20 then
  return {'RECONCILE_REQUIRED', 'entry_blob'}
end
if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', ef[3], sf[11], ef[19]}
end
local deletable = tonumber(sf[4]) > 0 or sf[12] == '1' or tonumber(sf[5]) > 0
if not deletable or now < tonumber(ef[19]) then
  return {'NOT_DUE', ef[3], sf[11], ef[19]}
end

local meta = read_meta(KEYS[1])
local freed = #eblob + #sblob
redis.call('HDEL', KEYS[1], entry_field, state_field)
bounded_zrem_by_mail(KEYS[2], mail_id)
bounded_zrem_by_mail(KEYS[3], mail_id)
if meta.retained > 0 then
  meta.retained = meta.retained - 1
end
meta.retained_bytes = meta.retained_bytes - freed
if meta.retained_bytes < 0 then
  meta.retained_bytes = 0
end
redis.call('ZREM', KEYS[4], due_member)

-- DeliveryDedup outlives the entry (35A §6.3): compress to a tombstone,
-- and only remove after its own retention window.
local dblob = redis.call('HGET', KEYS[1], delivery_field)
if dblob then
  local df = lp_fields(dblob)
  if df and #df >= 8 then
    if now >= tonumber(df[7]) then
      redis.call('HDEL', KEYS[1], delivery_field)
      if meta.delivery_records > 0 then
        meta.delivery_records = meta.delivery_records - 1
      end
    elseif df[8] == '0' then
      df[8] = '1'
      redis.call('HSET', KEYS[1], delivery_field, lp_rebuild(df))
    end
  end
end
write_meta(KEYS[1], meta)
return {'PURGED', fmt_ms(freed), fmt_ms(meta.retained), fmt_ms(meta.retained_bytes)}
`

const luaClaimMailScanLeaseSrc = luaCommon + `
-- KEYS: scan-lease
local owner = ARGV[2]
local ttl = tonumber(ARGV[3])
local min_ttl = tonumber(ARGV[4])
local max_ttl = tonumber(ARGV[5])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end

local cur = redis.call('HMGET', KEYS[1], 'owner', 'expire_at_ms', 'lease_epoch')
local cur_owner = cur[1] or ''
local cur_expire = tonumber(cur[2] or '0')
local epoch = tonumber(cur[3] or '0')
if cur_owner ~= '' and cur_owner ~= owner and cur_expire > now then
  return {'HELD_BY_OTHER', fmt_ms(cur_expire - now)}
end
epoch = epoch + 1
redis.call('HSET', KEYS[1], 'schema_version', 'player-mail-scan-lease-v1',
  'owner', owner, 'expire_at_ms', fmt_ms(now + ttl), 'lease_epoch', fmt_ms(epoch))
return {'LEASED', fmt_ms(ttl), fmt_ms(epoch)}
`

// luaCommonV2 extends the frozen luaCommon with the FIX-1 materialized-count
// predicate. luaCommon itself is byte-frozen so the V1 script sources keep
// their pinned SHA-1; V2 scripts embed luaCommonV2.
const luaCommonV2 = luaCommon + `
-- FIX-1: counter/index transitions only ask whether the old state was
-- still materially counted. The expire_at wall-clock comparison belongs to
-- the read path, not to summary transitions.
local function is_counted(sf)
  return tonumber(sf[4]) == 0 and sf[12] == '0' and tonumber(sf[5]) == 0
end
`

// luaCommonV3 adds the PM4 receipt watermarks and the per-mail receipt
// reference list. V1/V2 sources remain byte-frozen; all receipt-aware writes
// use the V3 family below.
const luaCommonV3 = luaCommon + `
local function is_counted(sf)
  return tonumber(sf[4]) == 0 and sf[12] == '0' and tonumber(sf[5]) == 0
end

local M_RECEIPT_ITEMS = '_meta:receipt_count'
local M_RECEIPT_BYTES = '_meta:receipt_bytes'
local read_meta_without_receipts = read_meta
local write_meta_without_receipts = write_meta
read_meta = function(box)
  local meta = read_meta_without_receipts(box)
  local m = redis.call('HMGET', box, M_RECEIPT_ITEMS, M_RECEIPT_BYTES)
  meta.receipt_items = tonumber(m[1] or '0')
  meta.receipt_bytes = tonumber(m[2] or '0')
  return meta
end
write_meta = function(box, meta)
  write_meta_without_receipts(box, meta)
  redis.call('HSET', box, M_RECEIPT_ITEMS, fmt_ms(meta.receipt_items),
    M_RECEIPT_BYTES, fmt_ms(meta.receipt_bytes))
end

local function receipt_index_fields(blob, max_items)
  if not blob then return {} end
  local fields = lp_fields(blob)
  if not fields or #fields > max_items then return nil end
  for _, ref in ipairs(fields) do
    if #ref < 3 or #ref > 1024 then return nil end
  end
  return fields
end

local function receipt_index_entry(ref)
  local sep = string.find(ref, string.char(0), 1, true)
  if not sep or sep == 1 or sep == #ref then return nil, nil end
  return string.sub(ref, 1, sep - 1), string.sub(ref, sep + 1)
end

local function receipt_info(blob)
  local fields = lp_fields(blob)
  if not fields then return nil, nil, nil end
  if fields[1] == 'MutationReceiptV1' and #fields >= 8 then
    return 'MUTATION', fields[3], tonumber(fields[8])
  end
  if fields[1] == 'ProjectionReceiptV1' and #fields >= 8 then
    return 'PROJECTION', fields[4], tonumber(fields[8])
  end
  return nil, nil, nil
end

local function receipt_field_kind(field)
  if string.sub(field, 1, 9) == 'mutation:' then return 'MUTATION' end
  if string.sub(field, 1, 11) == 'projection:' then return 'PROJECTION' end
  return nil
end

local function canonical_receipt_player(value)
  if value == '' or (string.len(value) > 1 and string.sub(value, 1, 1) == '0') then
    return false
  end
  for i = 1, string.len(value) do
    local c = string.byte(value, i)
    if c < 48 or c > 57 then return false end
  end
  return value ~= '0'
end

local function receipt_member_matches(member, kind, mail_id)
  local code = kind == 'MUTATION' and 'M' or 'P'
  local prefix = 'rdr1_' .. code .. ':'
  if string.sub(member, 1, string.len(prefix)) ~= prefix then return false end
  local rest = string.sub(member, string.len(prefix) + 1)
  local first = string.find(rest, ':', 1, true)
  if not first then return false end
  local second = string.find(rest, ':', first + 1, true)
  if not second or second == first + 1 then return false end
  local player = string.sub(rest, 1, first - 1)
  local target_mail = string.sub(rest, first + 1, second - 1)
  local field = string.sub(rest, second + 1)
  local field_kind = receipt_field_kind(field)
  local field_len = kind == 'MUTATION' and 73 or 75
  return canonical_receipt_player(player) and target_mail == mail_id and
    field_kind == kind and string.len(field) == field_len
end

-- Inspect all references for one mail before any cleanup write. This keeps
-- malformed or cross-mail index state fail closed instead of partially
-- deleting evidence.
local function inspect_mail_receipts(box, mail_id, index_field, max_items)
  local index_blob = redis.call('HGET', box, index_field)
  local refs = receipt_index_fields(index_blob, max_items)
  if not refs then return nil end
  local seen_fields = {}
  local seen_members = {}
  local records = {}
  for _, ref in ipairs(refs) do
    local field, member = receipt_index_entry(ref)
    if not field or not member then return nil end
    if seen_fields[field] or seen_members[member] then return nil end
    seen_fields[field] = true
    seen_members[member] = true
    local blob = redis.call('HGET', box, field)
    if not blob then return nil end
    local kind, target_mail, retain_until = receipt_info(blob)
    if not kind or target_mail ~= mail_id or not retain_until or retain_until < 0 then
      return nil
    end
    if not receipt_member_matches(member, kind, mail_id) or #member > 512 then return nil end
    records[#records + 1] = {
      ref = ref, field = field, member = member, blob = blob,
      retain_until = retain_until, kind = kind
    }
  end
  return records
end

local function apply_receipt_cleanup(box, receipt_due, index_field, records,
    only_field, only_member)
  local kept = {}
  local removed_count = 0
  local removed_bytes = 0
  for _, record in ipairs(records) do
    local selected = only_field == '' or
      (record.field == only_field and record.member == only_member)
    if selected and now >= record.retain_until then
      redis.call('HDEL', box, record.field)
      redis.call('ZREM', receipt_due, record.member)
      removed_count = removed_count + 1
      removed_bytes = removed_bytes + #record.blob
    else
      kept[#kept + 1] = record.ref
    end
  end
  if #kept == 0 then
    redis.call('HDEL', box, index_field)
  else
    redis.call('HSET', box, index_field, lp_rebuild(kept))
  end
  return removed_count, removed_bytes
end

local function receipt_meta_valid(meta, max_items, max_bytes)
  return meta.receipt_items and meta.receipt_items >= 0 and
    meta.receipt_items == math.floor(meta.receipt_items) and
    meta.receipt_items <= max_items and meta.receipt_bytes and
    meta.receipt_bytes >= 0 and meta.receipt_bytes == math.floor(meta.receipt_bytes) and
    meta.receipt_bytes <= max_bytes
end

local function receipt_index_snapshot(box, mail_id, index_field, max_items)
  local records = inspect_mail_receipts(box, mail_id, index_field, max_items)
  if not records then return nil, nil end
  local refs = {}
  local bytes = 0
  for _, record in ipairs(records) do
    refs[#refs + 1] = record.ref
    bytes = bytes + #record.blob
  end
  return refs, bytes
end
`

const luaMarkMailReadV2Src = luaCommonV2 + `
-- KEYS: box, safe-evict
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local mutation_id = ARGV[7]
local retain_window = tonumber(ARGV[8])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'MARK_READ' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'MAIL_NOT_FOUND'
  elseif tonumber(sf[3]) > 0 then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    -- FIX-1: decrement unread iff the mail was still materially counted.
    local was_counted = is_counted(sf)
    sf[3] = fmt_ms(now)
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    local nb = lp_rebuild(sf)
    redis.call('HSET', KEYS[1], state_field, nb)
    meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
    if was_counted and meta.unread > 0 then
      meta.unread = meta.unread - 1
    end
    if ef[20] == '0' or sf[6] == '1' then
      redis.call('ZADD', KEYS[2], 0, sort_member)
    end
    meta.revision = meta.revision + 1
    status = 'READ_MARKED'
  end
end
local rb = lp1('MutationReceiptV1') .. lp1('MARK_READ') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], mutation_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaDeleteMailV2Src = luaCommonV2 + `
-- KEYS: box, index, safe-evict
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local eligibility = ARGV[7]
local proof_mail_id = ARGV[8]
local proof_claim_hash = ARGV[9]
local proof_receipt_ref = ARGV[10]
local mutation_id = ARGV[11]
local retain_window = tonumber(ARGV[12])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'DELETE' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    -- Defense in depth (35A §4.3 / FIX-2): the script re-checks the
    -- server-constructed eligibility against the immutable entry facts,
    -- and a proof-carrying delete must bind this exact mail id, the
    -- entry's claim_material_hash and a non-empty receipt ref.
    local allow = false
    if ef[20] == '0' then
      allow = true
    elseif now > tonumber(ef[18]) then
      allow = true
    elseif sf[6] == '1' then
      allow = true
    elseif eligibility == 'ALLOW_WITH_CLAIM_PROOF' then
      if proof_mail_id == mail_id and proof_claim_hash ~= '' and
         ef[14] ~= '' and proof_claim_hash == ef[14] and proof_receipt_ref ~= '' then
        allow = true
      else
        return {'CLAIM_PROOF_INVALID'}
      end
    end
    if not allow then
      status = 'REJECT_UNCLAIMED'
    else
      -- FIX-1: move counters iff the mail was still materially counted.
      local was_counted = is_counted(sf)
      local was_unread = tonumber(sf[3]) == 0
      local was_claimable = ef[20] == '1' and sf[6] == '0'
      sf[4] = fmt_ms(now)
      sf[11] = fmt_ms(tonumber(sf[11]) + 1)
      local nb = lp_rebuild(sf)
      redis.call('HSET', KEYS[1], state_field, nb)
      meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
      if was_counted then
        redis.call('ZREM', KEYS[2], sort_member)
        redis.call('ZREM', KEYS[3], sort_member)
        if meta.visible > 0 then
          meta.visible = meta.visible - 1
        end
        if was_unread and meta.unread > 0 then
          meta.unread = meta.unread - 1
        end
        if was_claimable and meta.claimable > 0 then
          meta.claimable = meta.claimable - 1
        end
      end
      meta.revision = meta.revision + 1
      status = 'DELETED'
    end
  end
end
local rb = lp1('MutationReceiptV1') .. lp1('DELETE') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], mutation_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaApplyMailClaimProjectionV2Src = luaCommonV2 + `
-- KEYS: box, safe-evict
local projection_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local work_id = ARGV[7]
local work_version = ARGV[8]
local claim_receipt_ref = ARGV[9]
local claim_hash = ARGV[10]
local claimed_at = ARGV[11]
local retain_window = tonumber(ARGV[12])

local meta = read_meta(KEYS[1])
local existing = redis.call('HGET', KEYS[1], projection_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == work_id and f[3] == work_version and f[4] == mail_id then
    return summary_tail('RECEIPT', {f[6], f[7]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'projection_receipt_mismatch'}
end

local status
local applied = fmt_ms(now)
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'TARGET_ALREADY_PURGED'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  local eblob = redis.call('HGET', KEYS[1], entry_field)
  if not eblob then
    return {'RECONCILE_REQUIRED', 'entry_missing'}
  end
  local ef = lp_fields(eblob)
  if not ef or #ef < 20 then
    return {'RECONCILE_REQUIRED', 'entry_blob'}
  end
  if ef[14] == '' or ef[14] ~= claim_hash then
    status = 'CLAIM_MATERIAL_HASH_MISMATCH'
  elseif sf[6] == '1' then
    status = 'NO_CHANGE'
  else
    -- FIX-1: decrement claimable iff the mail was still materially counted.
    local was_counted = is_counted(sf)
    -- Monotonic false -> true only: never clears deleted_at and never
    -- re-adds the index member (35A PM3).
    sf[6] = '1'
    sf[7] = claim_receipt_ref
    sf[9] = claimed_at
    sf[10] = work_version
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    local nb = lp_rebuild(sf)
    redis.call('HSET', KEYS[1], state_field, nb)
    meta.retained_bytes = meta.retained_bytes + (#nb - #sblob)
    if was_counted and ef[20] == '1' and meta.claimable > 0 then
      meta.claimable = meta.claimable - 1
    end
    if tonumber(sf[3]) > 0 then
      redis.call('ZADD', KEYS[2], 0, sort_member)
    end
    meta.revision = meta.revision + 1
    status = 'APPLIED'
  end
end
local rb = lp1('ProjectionReceiptV1') .. lp1(work_id) .. lp1(work_version) ..
  lp1(mail_id) .. lp1(claim_hash) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(now + retain_window))
redis.call('HSET', KEYS[1], projection_field, rb)
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaGcMailV2Src = luaCommonV2 + `
-- KEYS: box, index, safe-evict, gc-due
local state_field = ARGV[2]
local entry_field = ARGV[3]
local delivery_field = ARGV[4]
local mail_id = ARGV[5]
local due_member = ARGV[6]
local mail_version = ARGV[7]
local expected_state_version = ARGV[8]

local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  if redis.call('HEXISTS', KEYS[1], entry_field) == 1 then
    return {'RECONCILE_REQUIRED', 'entry_without_state'}
  end
  bounded_zrem_by_mail(KEYS[2], mail_id)
  bounded_zrem_by_mail(KEYS[3], mail_id)
  redis.call('ZREM', KEYS[4], due_member)
  return {'TARGET_ALREADY_PURGED'}
end
local sf = lp_fields(sblob)
if not sf or #sf < 12 then
  return {'RECONCILE_REQUIRED', 'state_blob'}
end
local eblob = redis.call('HGET', KEYS[1], entry_field)
if not eblob then
  return {'RECONCILE_REQUIRED', 'entry_missing'}
end
local ef = lp_fields(eblob)
if not ef or #ef < 20 then
  return {'RECONCILE_REQUIRED', 'entry_blob'}
end
if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', ef[3], sf[11], ef[19]}
end

-- FIX-3: the dedup record is read in the same atomic step; a missing or
-- malformed record fails closed into reconcile instead of silently
-- dropping the delivery anti-replay evidence.
local dblob = redis.call('HGET', KEYS[1], delivery_field)
if not dblob then
  return {'RECONCILE_REQUIRED', 'dedup_missing'}
end
local df = lp_fields(dblob)
if not df or #df < 8 then
  return {'RECONCILE_REQUIRED', 'dedup_blob'}
end
local dedup_retain = tonumber(df[7])
if not dedup_retain then
  return {'RECONCILE_REQUIRED', 'dedup_blob'}
end

-- Entry/State and the dedup record share one effective purge point: the
-- later of the claim-evidence window and the delivery retention window.
local effective_purge = tonumber(ef[19])
if dedup_retain > effective_purge then
  effective_purge = dedup_retain
end
local deletable = tonumber(sf[4]) > 0 or sf[12] == '1' or tonumber(sf[5]) > 0
if not deletable or now < effective_purge then
  return {'NOT_DUE', ef[3], sf[11], fmt_ms(effective_purge)}
end

local meta = read_meta(KEYS[1])
local freed = #eblob + #sblob
redis.call('HDEL', KEYS[1], entry_field, state_field, delivery_field)
bounded_zrem_by_mail(KEYS[2], mail_id)
bounded_zrem_by_mail(KEYS[3], mail_id)
if meta.retained > 0 then
  meta.retained = meta.retained - 1
end
meta.retained_bytes = meta.retained_bytes - freed
if meta.retained_bytes < 0 then
  meta.retained_bytes = 0
end
redis.call('ZREM', KEYS[4], due_member)
if meta.delivery_records > 0 then
  meta.delivery_records = meta.delivery_records - 1
end
write_meta(KEYS[1], meta)
return {'PURGED', fmt_ms(freed), fmt_ms(meta.retained), fmt_ms(meta.retained_bytes)}
`

const luaMarkMailReadV3Src = luaCommonV3 + `
-- KEYS: box, safe-evict, receipt-due
-- ARGV: tag, mutation_field, state_field, entry_field, mail_id,
--       sort_member, mutation_id, retain_window, receipt_index_field,
--       receipt_due_member, max_receipt_items, max_receipt_total_bytes,
--       max_single_receipt_bytes
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local mutation_id = ARGV[7]
local retain_window = tonumber(ARGV[8])
local receipt_index_field = ARGV[9]
local receipt_due_member = ARGV[10]
local max_receipt_items = tonumber(ARGV[11])
local max_receipt_total_bytes = tonumber(ARGV[12])
local max_single_receipt_bytes = tonumber(ARGV[13])

if not retain_window or not max_receipt_items or not max_receipt_total_bytes or
   not max_single_receipt_bytes or receipt_due_member == '' then
  return {'RECONCILE_REQUIRED', 'receipt_limits'}
end
local meta = read_meta(KEYS[1])
if not receipt_meta_valid(meta, max_receipt_items, max_receipt_total_bytes) then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'MARK_READ' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end
local refs, indexed_receipt_bytes = receipt_index_snapshot(
  KEYS[1], mail_id, receipt_index_field, max_receipt_items)
if not refs or meta.receipt_items < #refs or
   meta.receipt_bytes < indexed_receipt_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_index'}
end

local status
local applied = fmt_ms(now)
local new_state_blob = nil
local add_safe_evict = false
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'MAIL_NOT_FOUND'
  elseif tonumber(sf[3]) > 0 then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    local was_counted = is_counted(sf)
    sf[3] = fmt_ms(now)
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    new_state_blob = lp_rebuild(sf)
    if was_counted and meta.unread > 0 then
      meta.unread = meta.unread - 1
    end
    if ef[20] == '0' or sf[6] == '1' then
      add_safe_evict = true
    end
    meta.retained_bytes = meta.retained_bytes + (#new_state_blob - #sblob)
    meta.revision = meta.revision + 1
    status = 'READ_MARKED'
  end
end

local rb = lp1('MutationReceiptV1') .. lp1('MARK_READ') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
if #rb > max_single_receipt_bytes or
   meta.receipt_items + 1 > max_receipt_items or
   meta.receipt_bytes + #rb > max_receipt_total_bytes then
  return {'CAPACITY_EXCEEDED', 'receipt'}
end
refs[#refs + 1] = mutation_field .. string.char(0) .. receipt_due_member
local new_index = lp_rebuild(refs)

if new_state_blob then
  redis.call('HSET', KEYS[1], state_field, new_state_blob)
end
if add_safe_evict then
  redis.call('ZADD', KEYS[2], 0, sort_member)
end
redis.call('HSET', KEYS[1], mutation_field, rb, receipt_index_field, new_index)
redis.call('ZADD', KEYS[3], now + retain_window, receipt_due_member)
meta.receipt_items = meta.receipt_items + 1
meta.receipt_bytes = meta.receipt_bytes + #rb
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaDeleteMailV3Src = luaCommonV3 + `
-- KEYS: box, index, safe-evict, receipt-due
-- ARGV: tag, mutation_field, state_field, entry_field, mail_id,
--       sort_member, eligibility, proof_mail_id, proof_claim_hash,
--       proof_receipt_ref, mutation_id, retain_window, receipt_index_field,
--       receipt_due_member, max_receipt_items, max_receipt_total_bytes,
--       max_single_receipt_bytes
local mutation_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local eligibility = ARGV[7]
local proof_mail_id = ARGV[8]
local proof_claim_hash = ARGV[9]
local proof_receipt_ref = ARGV[10]
local mutation_id = ARGV[11]
local retain_window = tonumber(ARGV[12])
local receipt_index_field = ARGV[13]
local receipt_due_member = ARGV[14]
local max_receipt_items = tonumber(ARGV[15])
local max_receipt_total_bytes = tonumber(ARGV[16])
local max_single_receipt_bytes = tonumber(ARGV[17])

if not retain_window or not max_receipt_items or not max_receipt_total_bytes or
   not max_single_receipt_bytes or receipt_due_member == '' then
  return {'RECONCILE_REQUIRED', 'receipt_limits'}
end
local meta = read_meta(KEYS[1])
if not receipt_meta_valid(meta, max_receipt_items, max_receipt_total_bytes) then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
local existing = redis.call('HGET', KEYS[1], mutation_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == 'DELETE' and f[3] == mail_id and f[4] == mutation_id then
    return summary_tail('RECEIPT', {f[5], f[6]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'mutation_receipt_mismatch'}
end
local refs, indexed_receipt_bytes = receipt_index_snapshot(
  KEYS[1], mail_id, receipt_index_field, max_receipt_items)
if not refs or meta.receipt_items < #refs or
   meta.receipt_bytes < indexed_receipt_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_index'}
end

local status
local applied = fmt_ms(now)
local new_state_blob = nil
local remove_index = false
local was_unread = false
local was_claimable = false
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'MAIL_NOT_FOUND'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  if tonumber(sf[4]) > 0 or sf[12] == '1' then
    status = 'NO_CHANGE'
  else
    local eblob = redis.call('HGET', KEYS[1], entry_field)
    if not eblob then
      return {'RECONCILE_REQUIRED', 'entry_missing'}
    end
    local ef = lp_fields(eblob)
    if not ef or #ef < 20 then
      return {'RECONCILE_REQUIRED', 'entry_blob'}
    end
    local allow = false
    if ef[20] == '0' then
      allow = true
    elseif now >= tonumber(ef[18]) then
      allow = true
    elseif sf[6] == '1' then
      allow = true
    elseif eligibility == 'ALLOW_WITH_CLAIM_PROOF' then
      if proof_mail_id == mail_id and proof_claim_hash ~= '' and
         ef[14] ~= '' and proof_claim_hash == ef[14] and proof_receipt_ref ~= '' then
        allow = true
      else
        return {'CLAIM_PROOF_INVALID'}
      end
    end
    if not allow then
      status = 'REJECT_UNCLAIMED'
    else
      local was_counted = is_counted(sf)
      was_unread = tonumber(sf[3]) == 0
      was_claimable = ef[20] == '1' and sf[6] == '0'
      sf[4] = fmt_ms(now)
      sf[11] = fmt_ms(tonumber(sf[11]) + 1)
      new_state_blob = lp_rebuild(sf)
      meta.retained_bytes = meta.retained_bytes + (#new_state_blob - #sblob)
      if was_counted then
        remove_index = true
        if meta.visible > 0 then meta.visible = meta.visible - 1 end
        if was_unread and meta.unread > 0 then meta.unread = meta.unread - 1 end
        if was_claimable and meta.claimable > 0 then meta.claimable = meta.claimable - 1 end
      end
      meta.revision = meta.revision + 1
      status = 'DELETED'
    end
  end
end

local rb = lp1('MutationReceiptV1') .. lp1('DELETE') .. lp1(mail_id) ..
  lp1(mutation_id) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(meta.revision)) .. lp1(fmt_ms(now + retain_window))
if #rb > max_single_receipt_bytes or
   meta.receipt_items + 1 > max_receipt_items or
   meta.receipt_bytes + #rb > max_receipt_total_bytes then
  return {'CAPACITY_EXCEEDED', 'receipt'}
end
refs[#refs + 1] = mutation_field .. string.char(0) .. receipt_due_member
local new_index = lp_rebuild(refs)

if new_state_blob then
  redis.call('HSET', KEYS[1], state_field, new_state_blob)
end
if remove_index then
  redis.call('ZREM', KEYS[2], sort_member)
  redis.call('ZREM', KEYS[3], sort_member)
end
redis.call('HSET', KEYS[1], mutation_field, rb, receipt_index_field, new_index)
redis.call('ZADD', KEYS[4], now + retain_window, receipt_due_member)
meta.receipt_items = meta.receipt_items + 1
meta.receipt_bytes = meta.receipt_bytes + #rb
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaApplyMailClaimProjectionV3Src = luaCommonV3 + `
-- KEYS: box, safe-evict, receipt-due
-- ARGV: tag, projection_field, state_field, entry_field, mail_id,
--       sort_member, work_id, work_version, claim_receipt_ref, claim_hash,
--       claimed_at, retain_window, receipt_index_field, receipt_due_member,
--       max_receipt_items, max_receipt_total_bytes, max_single_receipt_bytes
local projection_field = ARGV[2]
local state_field = ARGV[3]
local entry_field = ARGV[4]
local mail_id = ARGV[5]
local sort_member = ARGV[6]
local work_id = ARGV[7]
local work_version = ARGV[8]
local claim_receipt_ref = ARGV[9]
local claim_hash = ARGV[10]
local claimed_at = ARGV[11]
local retain_window = tonumber(ARGV[12])
local receipt_index_field = ARGV[13]
local receipt_due_member = ARGV[14]
local max_receipt_items = tonumber(ARGV[15])
local max_receipt_total_bytes = tonumber(ARGV[16])
local max_single_receipt_bytes = tonumber(ARGV[17])

if not retain_window or not max_receipt_items or not max_receipt_total_bytes or
   not max_single_receipt_bytes or receipt_due_member == '' then
  return {'RECONCILE_REQUIRED', 'receipt_limits'}
end
local meta = read_meta(KEYS[1])
if not receipt_meta_valid(meta, max_receipt_items, max_receipt_total_bytes) then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
local existing = redis.call('HGET', KEYS[1], projection_field)
if existing then
  local f = lp_fields(existing)
  if f and #f >= 8 and f[2] == work_id and f[3] == work_version and f[4] == mail_id and f[5] == claim_hash then
    return summary_tail('RECEIPT', {f[6], f[7]}, meta)
  end
  return {'RECONCILE_REQUIRED', 'projection_receipt_mismatch'}
end
local refs, indexed_receipt_bytes = receipt_index_snapshot(
  KEYS[1], mail_id, receipt_index_field, max_receipt_items)
if not refs or meta.receipt_items < #refs or
   meta.receipt_bytes < indexed_receipt_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_index'}
end

local status
local applied = fmt_ms(now)
local new_state_blob = nil
local add_safe_evict = false
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  status = 'TARGET_ALREADY_PURGED'
else
  local sf = lp_fields(sblob)
  if not sf or #sf < 12 then
    return {'RECONCILE_REQUIRED', 'state_blob'}
  end
  local eblob = redis.call('HGET', KEYS[1], entry_field)
  if not eblob then
    return {'RECONCILE_REQUIRED', 'entry_missing'}
  end
  local ef = lp_fields(eblob)
  if not ef or #ef < 20 then
    return {'RECONCILE_REQUIRED', 'entry_blob'}
  end
  if ef[14] == '' or ef[14] ~= claim_hash then
    status = 'CLAIM_MATERIAL_HASH_MISMATCH'
  elseif sf[6] == '1' then
    status = 'NO_CHANGE'
  else
    local was_counted = is_counted(sf)
    sf[6] = '1'
    sf[7] = claim_receipt_ref
    sf[9] = claimed_at
    sf[10] = work_version
    sf[11] = fmt_ms(tonumber(sf[11]) + 1)
    new_state_blob = lp_rebuild(sf)
    meta.retained_bytes = meta.retained_bytes + (#new_state_blob - #sblob)
    if was_counted and ef[20] == '1' and meta.claimable > 0 then
      meta.claimable = meta.claimable - 1
    end
    if tonumber(sf[3]) > 0 then
      add_safe_evict = true
    end
    meta.revision = meta.revision + 1
    status = 'APPLIED'
  end
end

local rb = lp1('ProjectionReceiptV1') .. lp1(work_id) .. lp1(work_version) ..
  lp1(mail_id) .. lp1(claim_hash) .. lp1(status) .. lp1(applied) ..
  lp1(fmt_ms(now + retain_window))
if #rb > max_single_receipt_bytes or
   meta.receipt_items + 1 > max_receipt_items or
   meta.receipt_bytes + #rb > max_receipt_total_bytes then
  return {'CAPACITY_EXCEEDED', 'receipt'}
end
refs[#refs + 1] = projection_field .. string.char(0) .. receipt_due_member
local new_index = lp_rebuild(refs)

if new_state_blob then
  redis.call('HSET', KEYS[1], state_field, new_state_blob)
end
if add_safe_evict then
  redis.call('ZADD', KEYS[2], 0, sort_member)
end
redis.call('HSET', KEYS[1], projection_field, rb, receipt_index_field, new_index)
redis.call('ZADD', KEYS[3], now + retain_window, receipt_due_member)
meta.receipt_items = meta.receipt_items + 1
meta.receipt_bytes = meta.receipt_bytes + #rb
write_meta(KEYS[1], meta)
return summary_tail(status, {applied}, meta)
`

const luaGcMailV3Src = luaCommonV3 + `
-- KEYS: box, index, safe-evict, gc-due, receipt-due
-- ARGV: tag, state_field, entry_field, delivery_field,
--       receipt_index_field, mail_id, due_member, mail_version,
--       expected_state_version, max_receipt_items, max_receipt_total_bytes
local state_field = ARGV[2]
local entry_field = ARGV[3]
local delivery_field = ARGV[4]
local receipt_index_field = ARGV[5]
local mail_id = ARGV[6]
local due_member = ARGV[7]
local mail_version = ARGV[8]
local expected_state_version = ARGV[9]
local max_receipt_items = tonumber(ARGV[10])
local max_receipt_total_bytes = tonumber(ARGV[11])

if not max_receipt_items or not max_receipt_total_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_limits'}
end
local meta = read_meta(KEYS[1])
if not receipt_meta_valid(meta, max_receipt_items, max_receipt_total_bytes) then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
local sblob = redis.call('HGET', KEYS[1], state_field)
if not sblob then
  if redis.call('HEXISTS', KEYS[1], entry_field) == 1 then
    return {'RECONCILE_REQUIRED', 'entry_without_state'}
  end
  local records = inspect_mail_receipts(KEYS[1], mail_id, receipt_index_field, max_receipt_items)
  if not records then
    return {'RECONCILE_REQUIRED', 'receipt_index'}
  end
  local total_bytes = 0
  for _, record in ipairs(records) do total_bytes = total_bytes + #record.blob end
  if meta.receipt_items < #records or meta.receipt_bytes < total_bytes then
    return {'RECONCILE_REQUIRED', 'receipt_meta'}
  end
  local removed_count, removed_bytes = apply_receipt_cleanup(
    KEYS[1], KEYS[5], receipt_index_field, records, '', '')
  meta.receipt_items = meta.receipt_items - removed_count
  meta.receipt_bytes = meta.receipt_bytes - removed_bytes
  if removed_count > 0 then write_meta(KEYS[1], meta) end
  bounded_zrem_by_mail(KEYS[2], mail_id)
  bounded_zrem_by_mail(KEYS[3], mail_id)
  redis.call('ZREM', KEYS[4], due_member)
  return {'TARGET_ALREADY_PURGED'}
end
local sf = lp_fields(sblob)
if not sf or #sf < 12 then
  return {'RECONCILE_REQUIRED', 'state_blob'}
end
local eblob = redis.call('HGET', KEYS[1], entry_field)
if not eblob then
  return {'RECONCILE_REQUIRED', 'entry_missing'}
end
local ef = lp_fields(eblob)
if not ef or #ef < 20 then
  return {'RECONCILE_REQUIRED', 'entry_blob'}
end
if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', ef[3], sf[11], ef[19]}
end
local dblob = redis.call('HGET', KEYS[1], delivery_field)
if not dblob then
  return {'RECONCILE_REQUIRED', 'dedup_missing'}
end
local df = lp_fields(dblob)
if not df or #df < 8 or not tonumber(df[7]) then
  return {'RECONCILE_REQUIRED', 'dedup_blob'}
end
local dedup_retain = tonumber(df[7])
local effective_purge = tonumber(ef[19])
if dedup_retain > effective_purge then effective_purge = dedup_retain end
local deletable = tonumber(sf[4]) > 0 or sf[12] == '1' or tonumber(sf[5]) > 0
if not deletable or now < effective_purge then
  return {'NOT_DUE', ef[3], sf[11], fmt_ms(effective_purge)}
end

local records = inspect_mail_receipts(KEYS[1], mail_id, receipt_index_field, max_receipt_items)
if not records then
  return {'RECONCILE_REQUIRED', 'receipt_index'}
end
local total_bytes = 0
for _, record in ipairs(records) do total_bytes = total_bytes + #record.blob end
if meta.receipt_items < #records or meta.receipt_bytes < total_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
local removed_count, removed_bytes = apply_receipt_cleanup(
  KEYS[1], KEYS[5], receipt_index_field, records, '', '')
meta.receipt_items = meta.receipt_items - removed_count
meta.receipt_bytes = meta.receipt_bytes - removed_bytes

local freed = #eblob + #sblob + removed_bytes
redis.call('HDEL', KEYS[1], entry_field, state_field, delivery_field)
bounded_zrem_by_mail(KEYS[2], mail_id)
bounded_zrem_by_mail(KEYS[3], mail_id)
if meta.retained > 0 then meta.retained = meta.retained - 1 end
meta.retained_bytes = meta.retained_bytes - (#eblob + #sblob)
if meta.retained_bytes < 0 then meta.retained_bytes = 0 end
redis.call('ZREM', KEYS[4], due_member)
if meta.delivery_records > 0 then meta.delivery_records = meta.delivery_records - 1 end
write_meta(KEYS[1], meta)
return {'PURGED', fmt_ms(freed), fmt_ms(meta.retained), fmt_ms(meta.retained_bytes)}
`

const luaGcMailReceiptSrc = luaCommonV3 + `
-- KEYS: box, receipt-due
-- ARGV: tag, receipt_index_field, receipt_field, due_member, mail_id,
--       max_receipt_items, max_receipt_total_bytes
local receipt_index_field = ARGV[2]
local receipt_field = ARGV[3]
local due_member = ARGV[4]
local mail_id = ARGV[5]
local max_receipt_items = tonumber(ARGV[6])
local max_receipt_total_bytes = tonumber(ARGV[7])
if not max_receipt_items or not max_receipt_total_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_limits'}
end
local meta = read_meta(KEYS[1])
if not receipt_meta_valid(meta, max_receipt_items, max_receipt_total_bytes) then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
if not redis.call('HEXISTS', KEYS[1], receipt_index_field) then
  return {'TARGET_ALREADY_PURGED'}
end
local records = inspect_mail_receipts(KEYS[1], mail_id, receipt_index_field, max_receipt_items)
if not records then
  return {'RECONCILE_REQUIRED', 'receipt_index'}
end
local total_bytes = 0
local found = false
local retain_until = 0
for _, record in ipairs(records) do
  total_bytes = total_bytes + #record.blob
  if record.field == receipt_field and record.member == due_member then
    found = true
    retain_until = record.retain_until
  end
end
if meta.receipt_items < #records or meta.receipt_bytes < total_bytes then
  return {'RECONCILE_REQUIRED', 'receipt_meta'}
end
if not found then
  return {'TARGET_ALREADY_PURGED'}
end
if now < retain_until then
  return {'NOT_DUE', fmt_ms(retain_until)}
end
local removed_count, removed_bytes = apply_receipt_cleanup(
  KEYS[1], KEYS[2], receipt_index_field, records, receipt_field, due_member)
if removed_count ~= 1 then
  return {'RECONCILE_REQUIRED', 'receipt_ref'}
end
meta.receipt_items = meta.receipt_items - removed_count
meta.receipt_bytes = meta.receipt_bytes - removed_bytes
write_meta(KEYS[1], meta)
return {'PURGED', fmt_ms(removed_bytes), fmt_ms(meta.receipt_items), fmt_ms(meta.receipt_bytes)}
`

var luaExpireMailVisibilityV2Src = deriveScript(luaExpireMailVisibilitySrc,
	"local expected_state_version = ARGV[8]",
	`local expected_state_version = ARGV[8]
local fresh_due_member = ARGV[9]
local fresh_mail_version = ARGV[10]
local fresh_state_version = ARGV[11]`,
	`if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', sf[11], ef[17]}
end`,
	`if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  -- The pre-read candidate is only installed if it still names the exact
  -- versions observed here. On a race the old member is retained, so repair
  -- can retry without any ZREM/ZADD loss window.
  if fresh_due_member ~= '' and ef[3] == fresh_mail_version and sf[11] == fresh_state_version then
    redis.call('ZREM', KEYS[4], due_member)
    redis.call('ZADD', KEYS[4], tonumber(ef[17]), fresh_due_member)
  end
  return {'STALE_REF', sf[11], ef[17]}
end`,
	`if now < tonumber(ef[17]) then
  return {'NOT_DUE', sf[11], ef[17]}
end`,
	`if now < tonumber(ef[17]) then
  redis.call('ZADD', KEYS[4], tonumber(ef[17]), due_member)
  return {'NOT_DUE', sf[11], ef[17]}
end`)

var luaGcMailV4Src = deriveScript(luaGcMailV3Src,
	`-- ARGV: tag, state_field, entry_field, delivery_field,
--       receipt_index_field, mail_id, due_member, mail_version,
--       expected_state_version, max_receipt_items, max_receipt_total_bytes`,
	`-- ARGV: tag, state_field, entry_field, delivery_field,
--       receipt_index_field, mail_id, due_member, mail_version,
--       expected_state_version, max_receipt_items, max_receipt_total_bytes,
--       fresh_due_member, fresh_mail_version, fresh_state_version`,
	"local max_receipt_total_bytes = tonumber(ARGV[11])",
	`local max_receipt_total_bytes = tonumber(ARGV[11])
local fresh_due_member = ARGV[12]
local fresh_mail_version = ARGV[13]
local fresh_state_version = ARGV[14]`,
	`if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  redis.call('ZREM', KEYS[4], due_member)
  return {'STALE_REF', ef[3], sf[11], ef[19]}
end`,
	`if ef[3] ~= mail_version or sf[11] ~= expected_state_version then
  if fresh_due_member ~= '' and ef[3] == fresh_mail_version and sf[11] == fresh_state_version then
    redis.call('ZREM', KEYS[4], due_member)
    redis.call('ZADD', KEYS[4], tonumber(ef[19]), fresh_due_member)
  end
  return {'STALE_REF', ef[3], sf[11], ef[19]}
end`,
	`if not deletable or now < effective_purge then
  return {'NOT_DUE', ef[3], sf[11], fmt_ms(effective_purge)}
end`,
	`if not deletable or now < effective_purge then
  redis.call('ZADD', KEYS[4], effective_purge, due_member)
  return {'NOT_DUE', ef[3], sf[11], fmt_ms(effective_purge)}
end`)

var luaGcMailReceiptV2Src = deriveScript(luaGcMailReceiptSrc,
	`if now < retain_until then
  return {'NOT_DUE', fmt_ms(retain_until)}
end`,
	`if now < retain_until then
  redis.call('ZADD', KEYS[2], retain_until, due_member)
  return {'NOT_DUE', fmt_ms(retain_until)}
end`)

func deriveScript(base string, replacements ...string) string {
	if len(replacements)%2 != 0 {
		panic("playermail: script replacement pairs required")
	}
	result := base
	for index := 0; index < len(replacements); index += 2 {
		old, replacement := replacements[index], replacements[index+1]
		if strings.Count(result, old) != 1 {
			panic("playermail: script derivation source mismatch")
		}
		result = strings.Replace(result, old, replacement, 1)
	}
	return result
}

// scriptDef is one immutable registered script.
type scriptDef struct {
	name   string
	source string
	sha    string
}

func builtinScripts() []scriptDef {
	defs := []struct {
		name   string
		source string
	}{
		{ScriptInsertMailIfAbsent, luaInsertMailIfAbsentSrc},
		{ScriptMarkMailRead, luaMarkMailReadSrc},
		{ScriptDeleteMail, luaDeleteMailSrc},
		{ScriptApplyMailClaimProjection, luaApplyMailClaimProjectionSrc},
		{ScriptExpireMailVisibility, luaExpireMailVisibilitySrc},
		{ScriptGcMail, luaGcMailSrc},
		{ScriptClaimMailScanLease, luaClaimMailScanLeaseSrc},
		{ScriptMarkMailReadV2, luaMarkMailReadV2Src},
		{ScriptDeleteMailV2, luaDeleteMailV2Src},
		{ScriptApplyMailClaimProjectionV2, luaApplyMailClaimProjectionV2Src},
		{ScriptGcMailV2, luaGcMailV2Src},
		{ScriptMarkMailReadV3, luaMarkMailReadV3Src},
		{ScriptDeleteMailV3, luaDeleteMailV3Src},
		{ScriptApplyMailClaimProjectionV3, luaApplyMailClaimProjectionV3Src},
		{ScriptGcMailV3, luaGcMailV3Src},
		{ScriptGcMailReceiptV1, luaGcMailReceiptSrc},
		{ScriptExpireMailVisibilityV2, luaExpireMailVisibilityV2Src},
		{ScriptGcMailV4, luaGcMailV4Src},
		{ScriptGcMailReceiptV2, luaGcMailReceiptV2Src},
	}
	out := make([]scriptDef, 0, len(defs))
	for _, d := range defs {
		sum := sha1.Sum([]byte(d.source)) //nolint:gosec // script cache identity
		out = append(out, scriptDef{name: d.name, source: d.source, sha: hexEncode(sum[:])})
	}
	return out
}

func hexEncode(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, digits[v>>4], digits[v&0x0f])
	}
	return string(out)
}
