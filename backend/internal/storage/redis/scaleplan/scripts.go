package scaleplan

// This package uses a fixed, versioned script family.  Every Redis key touched
// by a script is supplied by KeyBuilder; there is no Redis key-discovery path.

const scriptTime = `
local t = redis.call('TIME')
local sec = tonumber(t[1]); local usec = tonumber(t[2])
if sec == nil or usec == nil then return nil end
local now = sec * 1000 + math.floor(usec / 1000)
`

const scriptAcquireLease = scriptTime + `
if #KEYS ~= 2 or #ARGV ~= 7 then return {'MALFORMED'} end
local repo, namespace, realm, controller, boot, nonce, ttl = ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6],ARGV[7]
local ttl_n = tonumber(ttl)
if ttl_n == nil or ttl_n < 1 or ttl_n > 60000 or string.format('%d',ttl_n) ~= ttl then return {'MALFORMED'} end
local typ = redis.call('TYPE',KEYS[1]).ok
if typ ~= 'none' and typ ~= 'hash' then return {'CORRUPT','LEASE_SHAPE'} end
if typ == 'hash' then
  if redis.call('HLEN',KEYS[1]) ~= 9 then return {'LEASE_BUSY'} end
  local old = redis.call('HMGET',KEYS[1],'schema','repository_id','namespace','realm','controller_id','controller_boot_id','operation_nonce','fence','expires_at_ms')
  local exp = tonumber(old[9]); if exp == nil then return {'CORRUPT','LEASE_EXPIRY'} end
  if now < exp then return {'LEASE_BUSY'} end
end
local fence_type = redis.call('TYPE',KEYS[2]).ok
if fence_type ~= 'none' and fence_type ~= 'string' then return {'CORRUPT','FENCE_SHAPE'} end
local old_fence = redis.call('GET',KEYS[2]); if old_fence == false then old_fence = '0' end
if old_fence == '' or string.match(old_fence,'^[0-9]+$') == nil or (#old_fence > 1 and string.sub(old_fence,1,1)=='0') then return {'CORRUPT','FENCE_INTEGER'} end
local n=tonumber(old_fence); if n == nil or n > 9007199254740991 or string.format('%d',n) ~= old_fence then return {'CORRUPT','FENCE_INTEGER'} end
if n >= 9007199254740991 then return {'FENCE_EXHAUSTED'} end
local next=n+1;local fence=string.format('%d',next);local expires=now+ttl_n;if expires>9007199254740991 then return {'FENCE_EXHAUSTED'} end
redis.call('SET',KEYS[2],fence)
redis.call('HSET',KEYS[1],'schema','scale-plan-lease-v1','repository_id',repo,'namespace',namespace,'realm',realm,'controller_id',controller,'controller_boot_id',boot,'operation_nonce',nonce,'fence',fence,'expires_at_ms',string.format('%d',expires))
return {'ACQUIRED',fence,string.format('%d',expires)}
`

const scriptRenewLease = scriptTime + `
if #KEYS ~= 1 or #ARGV ~= 8 then return {'MALFORMED'} end
local repo,namespace,realm,controller,boot,nonce,fence,ttl=ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6],ARGV[7],ARGV[8]
local ttl_n=tonumber(ttl);if ttl_n==nil or ttl_n<1 or ttl_n>60000 or string.format('%d',ttl_n)~=ttl then return {'MALFORMED'} end
if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HLEN',KEYS[1])~=9 then return {'LEASE_LOST'} end
local v=redis.call('HMGET',KEYS[1],'schema','repository_id','namespace','realm','controller_id','controller_boot_id','operation_nonce','fence','expires_at_ms');local exp=tonumber(v[9])
if v[1]~='scale-plan-lease-v1' or v[2]~=repo or v[3]~=namespace or v[4]~=realm or v[5]~=controller or v[6]~=boot or v[7]~=nonce or v[8]~=fence or exp==nil or now>=exp then return {'LEASE_LOST'} end
local next=now+ttl_n;if next>9007199254740991 then return {'FENCE_EXHAUSTED'} end
redis.call('HSET',KEYS[1],'expires_at_ms',string.format('%d',next));return {'RENEWED',string.format('%d',next)}
`

const scriptReleaseLease = `
if #KEYS ~= 1 or #ARGV ~= 7 then return {'MALFORMED'} end
if redis.call('TYPE',KEYS[1]).ok=='none' then return {'ALREADY_RELEASED'} end
if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HLEN',KEYS[1])~=9 then return {'LEASE_LOST'} end
local v=redis.call('HMGET',KEYS[1],'schema','repository_id','namespace','realm','controller_id','controller_boot_id','operation_nonce','fence','expires_at_ms')
if v[1]~='scale-plan-lease-v1' or v[2]~=ARGV[1] or v[3]~=ARGV[2] or v[4]~=ARGV[3] or v[5]~=ARGV[4] or v[6]~=ARGV[5] or v[7]~=ARGV[6] or v[8]~=ARGV[7] then return {'LEASE_LOST'} end
redis.call('DEL',KEYS[1]);return {'RELEASED'}
`

const scriptApplyPlan = `
-- ARGV: mode, repository, namespace, realm, controller, boot, nonce, fence,
-- realm_digest, expected_epoch, then expected/current and candidate fields.
if #KEYS < 9 or (#KEYS-9)%2 ~= 0 or #ARGV < 45 then return {'MALFORMED'} end
local t=redis.call('TIME');local sec=tonumber(t[1]);local usec=tonumber(t[2]);if sec==nil or usec==nil then return {'MINIREDIS_TIME_UNSUPPORTED'} end
local now=sec*1000+math.floor(usec/1000);local mode=ARGV[1]
if mode=='BLOCKED' and (ARGV[44]==nil or tonumber(ARGV[44])==nil or now>=tonumber(ARGV[44])) then return {'PROOF_EXPIRED'} end
local lease=redis.call('TYPE',KEYS[2]).ok;if lease~='hash' or redis.call('HLEN',KEYS[2])~=9 then return {'LEASE_LOST'} end
local lv=redis.call('HMGET',KEYS[2],'schema','repository_id','namespace','realm','controller_id','controller_boot_id','operation_nonce','fence','expires_at_ms');local exp=tonumber(lv[9])
if lv[1]~='scale-plan-lease-v1' or lv[2]~=ARGV[2] or lv[3]~=ARGV[3] or lv[4]~=ARGV[4] or lv[5]~=ARGV[5] or lv[6]~=ARGV[6] or lv[7]~=ARGV[7] or lv[8]~=ARGV[8] or exp==nil or now>=exp then return {'LEASE_LOST'} end
if redis.call('TYPE',KEYS[3]).ok~='none' then return {'REALM_CORRUPT_LATCHED'} end
local meta_type=redis.call('TYPE',KEYS[1]).ok;local epoch=ARGV[10];local meta=redis.call('HMGET',KEYS[1],'schema','realm','realm_digest','count','bytes','active_count','recoverable_count','recoverable_bytes','terminal_count','terminal_bytes','store_epoch')
if mode=='CREATE' and meta_type=='none' then for _,k in ipairs({KEYS[7],KEYS[4],KEYS[5]}) do if redis.call('TYPE',k).ok~='none' then return {'CORRUPT','CREATE_DANGLING'} end end;meta={'scale-plan-meta-v1',ARGV[4],ARGV[9],'0','0','0','0','0','0','0','0'}
elseif meta_type~='hash' or redis.call('HLEN',KEYS[1])~=11 then return {'CORRUPT','META_SHAPE'} end
if meta[1]~='scale-plan-meta-v1' or meta[2]~=ARGV[4] or meta[3]~=ARGV[9] or meta[11]~=epoch then return {'RETRY'} end
for i=4,11 do local n=tonumber(meta[i]);if n==nil or n<0 or n>9007199254740991 or string.format('%d',n)~=meta[i] then return {'CORRUPT','META_INTEGER'} end end
if tonumber(meta[6])>1 or tonumber(meta[4])~=tonumber(meta[7])+tonumber(meta[9]) or tonumber(meta[5])~=tonumber(meta[8])+tonumber(meta[10]) then return {'CORRUPT','META_ACCOUNTING'} end
local accounting_type=redis.call('TYPE',KEYS[6]).ok;local recoverable_type=redis.call('TYPE',KEYS[8]).ok;local terminal_type=redis.call('TYPE',KEYS[9]).ok;if accounting_type~='hash' and not (accounting_type=='none' and meta[4]=='0') then return {'CORRUPT','ACCOUNTING_SHAPE'} end;if recoverable_type~='zset' and not (recoverable_type=='none' and meta[7]=='0') then return {'CORRUPT','RECOVERABLE_SHAPE'} end;if terminal_type~='zset' and not (terminal_type=='none' and meta[9]=='0') then return {'CORRUPT','TERMINAL_SHAPE'} end;if redis.call('HLEN',KEYS[6])~=tonumber(meta[4]) or redis.call('ZCARD',KEYS[8])~=tonumber(meta[7]) or redis.call('ZCARD',KEYS[9])~=tonumber(meta[9]) then return {'CORRUPT','INDEX_CARDINALITY'} end
if redis.call('TYPE',KEYS[7]).ok~='hash' and mode~='CREATE' then return {'CORRUPT','PLAN_SHAPE'} end
local current_canon,current_sum,current_bytes,current_rev,current_status,current_updated,current_completed,current_recovery,current_terminal=ARGV[11],ARGV[12],ARGV[13],ARGV[14],ARGV[15],ARGV[16],ARGV[17],ARGV[18],ARGV[19]
local candidate_canon,candidate_sum,candidate_bytes,candidate_rev,candidate_status,candidate_updated,candidate_completed,candidate_recovery,candidate_terminal=ARGV[20],ARGV[21],ARGV[22],ARGV[23],ARGV[24],ARGV[25],ARGV[26],ARGV[27],ARGV[28]
if tonumber(candidate_bytes)==nil or tonumber(candidate_bytes)~=#candidate_canon or tonumber(candidate_bytes)<1 or #candidate_sum~=64 or not string.match(candidate_sum,'^[0-9a-f]+$') then return {'MALFORMED'} end
if mode~='CREATE' then
  if redis.call('HLEN',KEYS[7])~=12 then return {'CORRUPT','PLAN_SHAPE'} end
  local p=redis.call('HMGET',KEYS[7],'schema','realm','plan_id','revision','status','updated_at_ms','completed_at_ms','recovery_retain_until_ms','terminal_retain_until_ms','canonical_json','checksum_sha256','canonical_bytes')
  local expected={ 'scale-plan-record-v1',ARGV[4],ARGV[29],current_rev,current_status,current_updated,current_completed,current_recovery,current_terminal,current_canon,current_sum,current_bytes }
  for i=1,#expected do if p[i]~=expected[i] then return {'CORRUPT','CURRENT_PLAN'} end end
  if redis.call('TYPE',KEYS[4]).ok~='hash' or redis.call('TYPE',KEYS[5]).ok~='hash' or redis.call('HLEN',KEYS[4])~=7 or redis.call('HLEN',KEYS[5])~=6 then return {'CORRUPT','REFERENCE_SHAPE'} end
  if redis.call('HGET',KEYS[4],'plan_id')~=ARGV[29] or redis.call('HGET',KEYS[5],'plan_id')~=ARGV[29] or redis.call('HGET',KEYS[6],ARGV[30])~=current_bytes then return {'CORRUPT','REFERENCE'} end
  if current_status=='SUCCEEDED' or current_status=='FAILED' or current_status=='CANCELLED' then return {'REVISION_CONFLICT'} end
else
  if redis.call('TYPE',KEYS[7]).ok~='none' or redis.call('TYPE',KEYS[4]).ok~='none' or redis.call('TYPE',KEYS[5]).ok~='none' then return {'CORRUPT','CREATE_DANGLING'} end
end
if mode=='CREATE' then
  if tonumber(meta[6])~=0 then return {'WORKLOAD_BUSY'} end
else
  if tonumber(meta[6])<1 then return {'CORRUPT','ACTIVE_COUNT'} end
end
local cleanup_count=tonumber(ARGV[45]);if cleanup_count==nil or cleanup_count<0 or cleanup_count>64 or string.format('%d',cleanup_count)~=ARGV[45] or #KEYS~=9+2*cleanup_count then return {'MALFORMED'} end
if cleanup_count>0 then
  if redis.call('TYPE',KEYS[9]).ok~='zset' or redis.call('TYPE',KEYS[6]).ok~='hash' or redis.call('ZCARD',KEYS[9])~=tonumber(meta[9]) or redis.call('HLEN',KEYS[6])~=tonumber(meta[4]) then return {'CORRUPT','CLEANUP_INDEX'} end
  local cleanup=redis.call('ZRANGE',KEYS[9],0,cleanup_count-1,'WITHSCORES');if #cleanup~=cleanup_count*2 then return {'RETRY'} end
  for i=1,cleanup_count do local member=cleanup[(i-1)*2+1];local score=tonumber(cleanup[(i-1)*2+2]);local base=46+(i-1)*5;local plan_id=ARGV[base];local idem_digest=ARGV[base+1];local raw_key=ARGV[base+2];local bytes=ARGV[base+3];local expected_member=ARGV[base+4];local pkey=KEYS[9+(i*2)-1];local ikey=KEYS[9+(i*2)];if member~=expected_member or score==nil or score>now or plan_id==nil or bytes==nil then return {'RETRY'} end;if redis.call('TYPE',pkey).ok~='hash' or redis.call('HLEN',pkey)~=12 or redis.call('TYPE',ikey).ok~='hash' or redis.call('HLEN',ikey)~=7 then return {'CORRUPT','CLEANUP_RECORD'} end;local p=redis.call('HMGET',pkey,'schema','realm','plan_id','revision','status','updated_at_ms','completed_at_ms','recovery_retain_until_ms','terminal_retain_until_ms','canonical_json','checksum_sha256','canonical_bytes');if p[1]~='scale-plan-record-v1' or p[2]~=ARGV[4] or p[3]~=plan_id or (p[5]~='SUCCEEDED' and p[5]~='FAILED' and p[5]~='CANCELLED') or tonumber(p[9])~=score or p[12]~=bytes or redis.call('HGET',KEYS[6],member)~=bytes then return {'CORRUPT','CLEANUP_REFERENCE'} end;local iv=redis.call('HMGET',ikey,'schema','realm','key_digest','raw_key','payload_digest','plan_id','retain_until_ms');if iv[1]~='scale-plan-idempotency-v1' or iv[2]~=ARGV[4] or iv[3]~=idem_digest or iv[4]~=raw_key or iv[6]~=plan_id then return {'CORRUPT','CLEANUP_IDEMPOTENCY'} end end
end
-- The script has now validated every touched key and all expected old bytes.
if cleanup_count>0 then for i=1,cleanup_count do local member=ARGV[50+(i-1)*5];local pkey=KEYS[9+(i*2)-1];local ikey=KEYS[9+(i*2)];redis.call('DEL',pkey,ikey);redis.call('ZREM',KEYS[9],member);redis.call('HDEL',KEYS[6],member) end end
local plan_id=ARGV[29];local member=ARGV[30];local idem_digest=ARGV[31];local raw_key=ARGV[32];local workload_digest=ARGV[33];local raw_workload=ARGV[34]
redis.call('HSET',KEYS[7],'schema','scale-plan-record-v1','realm',ARGV[4],'plan_id',plan_id,'revision',candidate_rev,'status',candidate_status,'updated_at_ms',candidate_updated,'completed_at_ms',candidate_completed,'recovery_retain_until_ms',candidate_recovery,'terminal_retain_until_ms',candidate_terminal,'canonical_json',candidate_canon,'checksum_sha256',candidate_sum,'canonical_bytes',candidate_bytes)
redis.call('HSET',KEYS[6],member,candidate_bytes)
if mode=='CREATE' then
  redis.call('HSET',KEYS[4],'schema','scale-plan-idempotency-v1','realm',ARGV[4],'key_digest',idem_digest,'raw_key',raw_key,'payload_digest',ARGV[35],'plan_id',plan_id,'retain_until_ms',candidate_recovery)
  redis.call('HSET',KEYS[5],'schema','scale-plan-active-workload-v1','realm',ARGV[4],'workload_digest',workload_digest,'canonical_workload_key',raw_workload,'plan_id',plan_id,'plan_revision',candidate_rev)
  redis.call('ZADD',KEYS[8],tonumber(candidate_updated),member)
else
  local terminal=(candidate_status=='SUCCEEDED' or candidate_status=='FAILED' or candidate_status=='CANCELLED')
  if terminal then redis.call('ZREM',KEYS[8],member);redis.call('ZADD',KEYS[9],tonumber(candidate_terminal),member);redis.call('HDEL',KEYS[5],'schema','realm','workload_digest','canonical_workload_key','plan_id','plan_revision');redis.call('HSET',KEYS[4],'retain_until_ms',candidate_terminal)
  else redis.call('ZADD',KEYS[8],tonumber(candidate_updated),member);redis.call('HSET',KEYS[5],'plan_revision',candidate_rev) end
end
redis.call('HSET',KEYS[1],'schema','scale-plan-meta-v1','realm',ARGV[4],'realm_digest',ARGV[9],'count',ARGV[36],'bytes',ARGV[37],'active_count',ARGV[38],'recoverable_count',ARGV[39],'recoverable_bytes',ARGV[40],'terminal_count',ARGV[41],'terminal_bytes',ARGV[42],'store_epoch',ARGV[43])
if mode=='CREATE' then return {'CREATED',plan_id} end
if mode=='BLOCKED' then return {'COMMITTED',plan_id} end
return {'COMMITTED',plan_id}
`

const scriptLoad = `
if #KEYS~=7 or #ARGV~=5 then return {'MALFORMED'} end
local ptype=redis.call('TYPE',KEYS[1]).ok;local member=ARGV[1];local realm=ARGV[2];local digest=ARGV[3]
if ptype=='none' then if redis.call('ZSCORE',KEYS[2],member)~=false or redis.call('ZSCORE',KEYS[3],member)~=false or redis.call('HEXISTS',KEYS[4],member)==1 then return {'CORRUPT','DANGLING'} end;return {'NOT_FOUND'} end
if ptype~='hash' or redis.call('HLEN',KEYS[1])~=12 then return {'CORRUPT','PLAN_SHAPE'} end
local p=redis.call('HMGET',KEYS[1],'schema','realm','plan_id','revision','status','updated_at_ms','completed_at_ms','recovery_retain_until_ms','terminal_retain_until_ms','canonical_json','checksum_sha256','canonical_bytes');if p[1]~='scale-plan-record-v1' or p[2]~=realm or p[3]~=ARGV[4] then return {'CORRUPT','PLAN_ID'} end
local terminal=(p[5]=='SUCCEEDED' or p[5]=='FAILED' or p[5]=='CANCELLED');local index=terminal and KEYS[3] or KEYS[2];local score=redis.call('ZSCORE',index,member);if score==false or (terminal and tonumber(score)~=tonumber(p[9])) or ((not terminal) and tonumber(score)~=tonumber(p[6])) then return {'CORRUPT','INDEX'} end
if redis.call('HGET',KEYS[4],member)~=p[12] then return {'CORRUPT','ACCOUNTING'} end
if redis.call('TYPE',KEYS[7]).ok~='hash' or redis.call('HLEN',KEYS[7])~=11 then return {'CORRUPT','META'} end
local m=redis.call('HMGET',KEYS[7],'schema','realm','realm_digest','count','bytes','active_count','recoverable_count','recoverable_bytes','terminal_count','terminal_bytes','store_epoch');if m[1]~='scale-plan-meta-v1' or m[2]~=realm or m[3]~=digest or m[11]~=ARGV[5] then return {'CORRUPT','META'} end
if terminal then if redis.call('TYPE',KEYS[5]).ok~='none' then return {'CORRUPT','TERMINAL_ACTIVE'} end else if redis.call('TYPE',KEYS[5]).ok~='hash' or redis.call('HLEN',KEYS[5])~=6 or redis.call('HGET',KEYS[5],'plan_id')~=p[3] then return {'CORRUPT','ACTIVE'} end end
if redis.call('TYPE',KEYS[6]).ok~='hash' or redis.call('HLEN',KEYS[6])~=7 or redis.call('HGET',KEYS[6],'plan_id')~=p[3] then return {'CORRUPT','IDEMPOTENCY'} end
return {'FOUND',p[10],p[11],p[12],p[6],p[5],p[4]}
`

const scriptList = `
if #KEYS<3 or #ARGV~=6 then return {'MALFORMED'} end
local realm,digest,offset_text,limit_text,epoch,count=ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6];local offset=tonumber(offset_text);local limit=tonumber(limit_text);if offset==nil or limit==nil or offset<0 or limit<1 or limit>256 then return {'MALFORMED'} end
if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HLEN',KEYS[1])~=11 or redis.call('TYPE',KEYS[2]).ok~='zset' or redis.call('TYPE',KEYS[3]).ok~='hash' then return {'CORRUPT','META'} end
local m=redis.call('HMGET',KEYS[1],'schema','realm','realm_digest','count','bytes','active_count','recoverable_count','recoverable_bytes','terminal_count','terminal_bytes','store_epoch');if m[1]~='scale-plan-meta-v1' or m[2]~=realm or m[3]~=digest or m[7]~=count or m[11]~=epoch then return {'RETRY'} end;if redis.call('ZCARD',KEYS[2])~=tonumber(m[7]) or redis.call('HLEN',KEYS[3])~=tonumber(m[4]) then return {'CORRUPT','INDEX_CARDINALITY'} end
	local vals=redis.call('ZRANGE',KEYS[2],offset,offset+limit-1,'WITHSCORES');if #vals%2~=0 then return {'CORRUPT','INDEX'} end;local out={'PAGE',m[11],m[7]};for i=1,#vals,2 do local member=vals[i];local score=tonumber(vals[i+1]);local pkey=KEYS[3+math.floor((i+1)/2)];if string.match(member,'^[0-9a-f]+$')==nil or score==nil or redis.call('TYPE',pkey).ok~='hash' or redis.call('HLEN',pkey)~=12 then return {'CORRUPT','PLAN'} end;local p=redis.call('HMGET',pkey,'schema','realm','plan_id','revision','status','updated_at_ms','completed_at_ms','recovery_retain_until_ms','terminal_retain_until_ms','canonical_json','checksum_sha256','canonical_bytes');if p[1]~='scale-plan-record-v1' or p[2]~=realm or p[3]=='' or p[5]=='SUCCEEDED' or p[5]=='FAILED' or p[5]=='CANCELLED' or score~=tonumber(p[6]) or redis.call('HGET',KEYS[3],member)~=p[12] then return {'CORRUPT','REFERENCE',member,p[12],string.format('%d',score),p[6]} end;out[#out+1]=member;out[#out+1]=p[10];out[#out+1]=p[11];out[#out+1]=p[12];out[#out+1]=p[6] end;out[#out+1]=string.format('%d',#vals/2);return out
`

const scriptLatch = scriptTime + `
if #KEYS~=3 or #ARGV~=11 then return {'MALFORMED'} end
local repo,namespace,realm,controller,boot,nonce,fence,expected_epoch=ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6],ARGV[7],ARGV[8]
if redis.call('TYPE',KEYS[2]).ok~='hash' or redis.call('HGET',KEYS[2],'repository_id')~=repo or redis.call('HGET',KEYS[2],'namespace')~=namespace or redis.call('HGET',KEYS[2],'realm')~=realm or redis.call('HGET',KEYS[2],'controller_id')~=controller or redis.call('HGET',KEYS[2],'controller_boot_id')~=boot or redis.call('HGET',KEYS[2],'operation_nonce')~=nonce or redis.call('HGET',KEYS[2],'fence')~=fence or tonumber(redis.call('HGET',KEYS[2],'expires_at_ms'))<=now then return {'LEASE_LOST'} end
if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HGET',KEYS[1],'store_epoch')==false then return {'CORRUPT','META'} end;if redis.call('HGET',KEYS[1],'store_epoch')~=expected_epoch then return {'RETRY'} end;if redis.call('TYPE',KEYS[3]).ok~='none' then return {'ALREADY_LATCHED'} end
redis.call('HSET',KEYS[3],'schema','scale-plan-corrupt-latch-v1','realm',realm,'detected_at_ms',string.format('%d',now),'store_epoch_at_detection',expected_epoch,'class',ARGV[9],'evidence_digest',ARGV[10],'detector_identity',ARGV[11]);return {'LATCHED'}
`

const scriptCleanupTerminal = scriptTime + `
if #KEYS < 5 or (#KEYS - 5) % 2 ~= 0 or #ARGV < 10 then return {'MALFORMED'} end
local repo,namespace,realm,controller,boot,nonce,fence,limit,realm_digest,epoch = ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6],ARGV[7],ARGV[8],ARGV[9],ARGV[10]
if tonumber(limit)==nil or tonumber(limit)<1 or tonumber(limit)>256 or string.format('%d',tonumber(limit))~=limit then return {'MALFORMED'} end
if redis.call('TYPE',KEYS[2]).ok~='hash' or redis.call('HLEN',KEYS[2])~=9 then return {'LEASE_LOST'} end
local lease=redis.call('HMGET',KEYS[2],'schema','repository_id','namespace','realm','controller_id','controller_boot_id','operation_nonce','fence','expires_at_ms');local exp=tonumber(lease[9])
if lease[1]~='scale-plan-lease-v1' or lease[2]~=repo or lease[3]~=namespace or lease[4]~=realm or lease[5]~=controller or lease[6]~=boot or lease[7]~=nonce or lease[8]~=fence or exp==nil or now>=exp then return {'LEASE_LOST'} end
if redis.call('TYPE',KEYS[3]).ok~='none' then return {'REALM_CORRUPT_LATCHED'} end
if redis.call('TYPE',KEYS[1]).ok~='hash' or redis.call('HLEN',KEYS[1])~=11 or (redis.call('TYPE',KEYS[4]).ok~='none' and redis.call('TYPE',KEYS[4]).ok~='zset') or (redis.call('TYPE',KEYS[5]).ok~='none' and redis.call('TYPE',KEYS[5]).ok~='hash') then return {'CORRUPT','META'} end
local m=redis.call('HMGET',KEYS[1],'schema','realm','realm_digest','count','bytes','active_count','recoverable_count','recoverable_bytes','terminal_count','terminal_bytes','store_epoch');if m[1]~='scale-plan-meta-v1' or m[2]~=realm or m[3]~=realm_digest or m[11]~=epoch then return {'RETRY'} end;for i=4,11 do local n=tonumber(m[i]);if n==nil or n<0 or n>9007199254740991 or string.format('%d',n)~=m[i] then return {'CORRUPT','META_INTEGER'} end end;if tonumber(m[4])<tonumber(m[9]) or tonumber(m[5])<tonumber(m[10]) or tonumber(m[9])<1 then return {'CORRUPT','META_ACCOUNTING'} end;if redis.call('ZCARD',KEYS[4])~=tonumber(m[9]) or redis.call('HLEN',KEYS[5])~=tonumber(m[4]) then return {'CORRUPT','INDEX_CARDINALITY'} end
local vals=redis.call('ZRANGE',KEYS[4],0,tonumber(limit)-1,'WITHSCORES');if #vals%2~=0 then return {'CORRUPT','TERMINAL_INDEX'} end;local count=math.floor(#vals/2);if count==0 then return {'CLEANED','0'} end;local first_score=tonumber(vals[2]);if first_score==nil then return {'CORRUPT','TERMINAL_SCORE'} end;if first_score>now then return {'CLEANED','0'} end;local expected_args=10+count*5;if #ARGV<expected_args then return {'MALFORMED'} end
for i=1,count do local member=vals[(i-1)*2+1];local score=tonumber(vals[(i-1)*2+2]);local base=11+(i-1)*5;local plan_id=ARGV[base];local idem_digest=ARGV[base+1];local raw_key=ARGV[base+2];local bytes=ARGV[base+3];local expected_member=ARGV[base+4];local pkey=KEYS[6+(i-1)*2];local ikey=KEYS[7+(i-1)*2];if member~=expected_member or score==nil or score>now or plan_id==nil or bytes==nil then return {'RETRY'} end;if redis.call('TYPE',pkey).ok~='hash' or redis.call('HLEN',pkey)~=12 or redis.call('TYPE',ikey).ok~='hash' or redis.call('HLEN',ikey)~=7 then return {'CORRUPT','TERMINAL_RECORD'} end;local p=redis.call('HMGET',pkey,'schema','realm','plan_id','revision','status','updated_at_ms','completed_at_ms','recovery_retain_until_ms','terminal_retain_until_ms','canonical_json','checksum_sha256','canonical_bytes');if p[1]~='scale-plan-record-v1' or p[2]~=realm or p[3]~=plan_id or (p[5]~='SUCCEEDED' and p[5]~='FAILED' and p[5]~='CANCELLED') or tonumber(p[9])~=score or p[12]~=bytes or redis.call('HGET',KEYS[5],member)~=bytes then return {'CORRUPT','TERMINAL_REFERENCE'} end;local iv=redis.call('HMGET',ikey,'schema','realm','key_digest','raw_key','payload_digest','plan_id','retain_until_ms');if iv[1]~='scale-plan-idempotency-v1' or iv[2]~=realm or iv[3]~=idem_digest or iv[4]~=raw_key or iv[6]~=plan_id then return {'CORRUPT','IDEMPOTENCY_REFERENCE'} end end
for i=1,count do local member=vals[(i-1)*2+1];local base=11+(i-1)*5;redis.call('DEL',KEYS[6+(i-1)*2],KEYS[7+(i-1)*2]);redis.call('ZREM',KEYS[4],member);redis.call('HDEL',KEYS[5],member) end
local total=tonumber(m[4])-count;local bytes=tonumber(m[5]);local terminal=tonumber(m[9])-count;local terminal_bytes=tonumber(m[10]);for i=1,count do local base=11+(i-1)*5;local n=tonumber(ARGV[base+3]);if n==nil or n<1 or string.format('%d',n)~=ARGV[base+3] or bytes<n or terminal_bytes<n then return {'CORRUPT','ACCOUNTING'} end;bytes=bytes-n;terminal_bytes=terminal_bytes-n end;local next_epoch=tonumber(epoch)+1;if next_epoch>9007199254740991 then return {'FENCE_EXHAUSTED'} end;redis.call('HSET',KEYS[1],'count',string.format('%d',total),'bytes',string.format('%d',bytes),'terminal_count',string.format('%d',terminal),'terminal_bytes',string.format('%d',terminal_bytes),'store_epoch',string.format('%d',next_epoch));local out={'CLEANED',string.format('%d',count)};if redis.call('ZCOUNT',KEYS[4],'-inf',now)>0 then out[#out+1]='MORE_DUE' end;return out
`

var scriptSources = map[string]string{
	"AcquireMutationLeaseV1":      scriptAcquireLease,
	"RenewMutationLeaseV1":        scriptRenewLease,
	"ReleaseMutationLeaseV1":      scriptReleaseLease,
	"ApplyPlanV1":                 scriptApplyPlan,
	"LoadScalePlanV1":             scriptLoad,
	"ListRecoverableScalePlansV1": scriptList,
	"LatchScalePlanCorruptionV1":  scriptLatch,
	"CleanupTerminalScalePlansV1": scriptCleanupTerminal,
}
