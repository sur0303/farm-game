package friend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const redisFriendIndexRevisionField = "roster_revision"

var redisFriendIndexApplyScript = redis.NewScript(`
local field = ARGV[1]
local relation = ARGV[2]
local epoch = tonumber(ARGV[3])
local activated = ARGV[4]
local member = ARGV[5]
local old_relation = redis.call('HGET', KEYS[1], field)
local old_epoch = tonumber(redis.call('HGET', KEYS[2], field) or '0')
if old_epoch > epoch then return 0 end
if old_epoch == epoch and old_relation and old_relation ~= relation then return -1 end
local old_activated = redis.call('HGET', KEYS[3], field)
local old_member = redis.call('HGET', KEYS[4], field)
if old_epoch == epoch and old_relation == relation and old_activated == activated and old_member == member then return 0 end
if old_member then redis.call('ZREM', KEYS[5], old_member) end
redis.call('HSET', KEYS[1], field, relation)
redis.call('HSET', KEYS[2], field, tostring(epoch))
redis.call('HSET', KEYS[3], field, activated)
redis.call('HSET', KEYS[4], field, member)
redis.call('ZADD', KEYS[5], 0, member)
redis.call('HINCRBY', KEYS[6], '` + redisFriendIndexRevisionField + `', 1)
return 1
`)

var redisFriendIndexRemoveScript = redis.NewScript(`
local field = ARGV[1]
local relation = ARGV[2]
local epoch = tonumber(ARGV[3])
local old_relation = redis.call('HGET', KEYS[1], field)
local old_epoch = tonumber(redis.call('HGET', KEYS[2], field) or '0')
if not old_relation or old_relation ~= relation or old_epoch > epoch then return 0 end
local old_member = redis.call('HGET', KEYS[4], field)
if old_member then redis.call('ZREM', KEYS[5], old_member) end
redis.call('HDEL', KEYS[1], field)
redis.call('HDEL', KEYS[2], field)
redis.call('HDEL', KEYS[3], field)
redis.call('HDEL', KEYS[4], field)
redis.call('HINCRBY', KEYS[6], '` + redisFriendIndexRevisionField + `', 1)
return 1
`)

// redisFriendIndexScanScript performs the revision-fenced roster read in one
// same-slot Redis operation. The previous implementation used an HGET,
// ZRANGEBYLEX, a three-HMGET pipeline, and a second HGET. Keeping the
// revision check in this script preserves the same consistency contract while
// removing the extra client-side round trips and pipeline bookkeeping from the
// cold GET_NEXT_FRIEND_OPPORTUNITY path.
var redisFriendIndexScanScript = redis.NewScript(`
local min = ARGV[1]
local limit = tonumber(ARGV[2])
local before = redis.call('HGET', KEYS[6], '` + redisFriendIndexRevisionField + `') or '0'
local members = redis.call('ZRANGEBYLEX', KEYS[5], min, '+', 'LIMIT', 0, limit + 1)
local has_more = 0
if #members > limit then has_more = 1 end
local count = #members
if count > limit then count = limit end
local result = {'OK', before, tostring(has_more)}
for i = 1, count do
  local member = members[i]
  local separator = string.find(member, ':', 1, true)
  if not separator then return {'MALFORMED'} end
  local field = string.gsub(string.sub(member, separator + 1), '^0+', '')
  if field == '' then field = '0' end
  result[#result + 1] = member
  result[#result + 1] = redis.call('HGET', KEYS[1], field) or ''
  result[#result + 1] = redis.call('HGET', KEYS[2], field) or ''
  result[#result + 1] = redis.call('HGET', KEYS[3], field) or ''
end
local after = redis.call('HGET', KEYS[6], '` + redisFriendIndexRevisionField + `') or '0'
if before ~= after then return {'RETRY'} end
return result
`)

// RedisFriendIndexRepository is a paged, rebuildable projection. Its Lua
// writes are atomic only within one viewer key family; Pair remains the sole
// relationship truth and every page is revalidated by ReadAPI.
type RedisFriendIndexRepository struct {
	client redis.UniversalClient
	keys   FriendshipKeyBuilder
}

func NewRedisFriendIndexRepository(client redis.UniversalClient, profile FriendRedisProfile) (*RedisFriendIndexRepository, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: friend index redis client", ErrInvalidCommand)
	}
	keys, err := NewFriendshipKeyBuilder(profile)
	if err != nil {
		return nil, err
	}
	return &RedisFriendIndexRepository{client: client, keys: keys}, nil
}

func friendIndexMember(activatedAtMs int64, friendPlayerID uint64) (string, error) {
	if activatedAtMs <= 0 || friendPlayerID == 0 {
		return "", fmt.Errorf("%w: friend index sort tuple", ErrInvalidCommand)
	}
	return fmt.Sprintf("%019d:%020d", int64(math.MaxInt64)-activatedAtMs, friendPlayerID), nil
}

func friendIDFromIndexMember(member string) (uint64, error) {
	separator := strings.LastIndexByte(member, ':')
	if separator <= 0 || separator+1 >= len(member) {
		return 0, errors.New("friend: malformed index member")
	}
	id, err := strconv.ParseUint(member[separator+1:], 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("friend: malformed index friend id")
	}
	return id, nil
}

func (r *RedisFriendIndexRepository) keyStrings(viewer uint64) ([]string, error) {
	relation, epoch, activated, sortMember, order, meta, err := r.keys.FriendIndexKeys(viewer)
	if err != nil {
		return nil, err
	}
	return []string{relation.String(), epoch.String(), activated.String(), sortMember.String(), order.String(), meta.String()}, nil
}

func (r *RedisFriendIndexRepository) Apply(ctx context.Context, viewer uint64, entry FriendIndexEntry) (bool, error) {
	member, err := friendIndexMember(entry.ActivatedAtMs, entry.FriendPlayerID)
	if err != nil || entry.RelationID == "" || entry.RelationEpoch == 0 {
		return false, fmt.Errorf("%w: friend index apply", ErrInvalidCommand)
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return false, err
	}
	value, err := redisFriendIndexApplyScript.Run(ctx, r.client, keys,
		strconv.FormatUint(entry.FriendPlayerID, 10), entry.RelationID,
		strconv.FormatUint(entry.RelationEpoch, 10), strconv.FormatInt(entry.ActivatedAtMs, 10), member).Int64()
	if err != nil {
		return false, err
	}
	if value < 0 {
		return false, errors.New("friend: conflicting relation at same index epoch")
	}
	return value == 1, nil
}

func (r *RedisFriendIndexRepository) Remove(ctx context.Context, viewer, peer uint64, relationID string, epoch uint64) (bool, error) {
	if viewer == 0 || peer == 0 || relationID == "" || epoch == 0 {
		return false, fmt.Errorf("%w: friend index remove", ErrInvalidCommand)
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return false, err
	}
	value, err := redisFriendIndexRemoveScript.Run(ctx, r.client, keys,
		strconv.FormatUint(peer, 10), relationID, strconv.FormatUint(epoch, 10)).Int64()
	return value == 1, err
}

func (r *RedisFriendIndexRepository) Lookup(ctx context.Context, viewer, peer uint64) (FriendIndexEntry, bool, error) {
	if !ValidPair(viewer, peer) {
		return FriendIndexEntry{}, false, fmt.Errorf("%w: friend index lookup", ErrInvalidCommand)
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return FriendIndexEntry{}, false, err
	}
	field := strconv.FormatUint(peer, 10)
	pipe := r.client.Pipeline()
	relationCmd := pipe.HGet(ctx, keys[0], field)
	epochCmd := pipe.HGet(ctx, keys[1], field)
	activatedCmd := pipe.HGet(ctx, keys[2], field)
	_, err = pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return FriendIndexEntry{}, false, err
	}
	if relationCmd.Val() == "" && epochCmd.Val() == "" && activatedCmd.Val() == "" {
		return FriendIndexEntry{}, false, nil
	}
	epoch, epochErr := strconv.ParseUint(epochCmd.Val(), 10, 64)
	activated, activatedErr := strconv.ParseInt(activatedCmd.Val(), 10, 64)
	if relationCmd.Val() == "" || epochErr != nil || activatedErr != nil || epoch == 0 || activated <= 0 {
		return FriendIndexEntry{}, false, errors.New("friend: malformed direct index row")
	}
	return FriendIndexEntry{FriendPlayerID: peer, RelationID: relationCmd.Val(),
		RelationEpoch: epoch, ActivatedAtMs: activated}, true, nil
}

func (r *RedisFriendIndexRepository) RosterRevision(ctx context.Context, viewer uint64) (uint64, error) {
	if viewer == 0 {
		return 0, fmt.Errorf("%w: friend index revision", ErrInvalidCommand)
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return 0, err
	}
	revision, err := r.client.HGet(ctx, keys[5], redisFriendIndexRevisionField).Uint64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return revision, nil
}

func (r *RedisFriendIndexRepository) BatchLookup(ctx context.Context, viewer uint64, peers []uint64) ([]FriendIndexEntry, error) {
	if viewer == 0 {
		return nil, fmt.Errorf("%w: friend index batch lookup", ErrInvalidCommand)
	}
	fields := make([]string, 0, len(peers))
	ids := make([]uint64, 0, len(peers))
	seen := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		if !ValidPair(viewer, peer) {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		ids = append(ids, peer)
		fields = append(fields, strconv.FormatUint(peer, 10))
	}
	if len(fields) == 0 {
		return nil, nil
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return nil, err
	}
	pipe := r.client.Pipeline()
	relationCmd := pipe.HMGet(ctx, keys[0], fields...)
	epochCmd := pipe.HMGet(ctx, keys[1], fields...)
	activatedCmd := pipe.HMGet(ctx, keys[2], fields...)
	_, err = pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	relations := relationCmd.Val()
	epochs := epochCmd.Val()
	activateds := activatedCmd.Val()
	if len(relations) != len(fields) || len(epochs) != len(fields) || len(activateds) != len(fields) {
		return nil, errors.New("friend: index batch cardinality mismatch")
	}
	entries := make([]FriendIndexEntry, 0, len(fields))
	for i, peer := range ids {
		relation, _ := relations[i].(string)
		epochRaw, _ := epochs[i].(string)
		activatedRaw, _ := activateds[i].(string)
		if relation == "" && epochRaw == "" && activatedRaw == "" {
			continue
		}
		epoch, epochErr := strconv.ParseUint(epochRaw, 10, 64)
		activated, activatedErr := strconv.ParseInt(activatedRaw, 10, 64)
		if relation == "" || epochErr != nil || activatedErr != nil || epoch == 0 || activated <= 0 {
			return nil, errors.New("friend: malformed direct index row")
		}
		entries = append(entries, FriendIndexEntry{FriendPlayerID: peer, RelationID: relation,
			RelationEpoch: epoch, ActivatedAtMs: activated})
	}
	return entries, nil
}

func (r *RedisFriendIndexRepository) Scan(ctx context.Context, viewer uint64, cursor string, limit int) (FriendIndexScan, error) {
	if viewer == 0 || limit <= 0 || limit > MaxPageSize*refillBudgetPages {
		return FriendIndexScan{}, fmt.Errorf("%w: friend index scan", ErrInvalidCommand)
	}
	keys, err := r.keyStrings(viewer)
	if err != nil {
		return FriendIndexScan{}, err
	}
	position := ListCursor{}
	if cursor != "" {
		position, err = DecodeListCursor(cursor)
		if err != nil || position.ViewerPlayerID != viewer {
			return FriendIndexScan{}, ErrInvalidCursor
		}
	}
	min := "-"
	if cursor != "" {
		member, memberErr := friendIndexMember(position.LastActivatedAtMs, position.LastFriendPlayerID)
		if memberErr != nil {
			return FriendIndexScan{}, ErrInvalidCursor
		}
		min = "(" + member
	}
	for attempt := 0; attempt < 3; attempt++ {
		values, runErr := redisFriendIndexScanScript.Run(ctx, r.client, keys, min, limit).StringSlice()
		if runErr != nil {
			return FriendIndexScan{}, runErr
		}
		if len(values) == 0 || values[0] == "RETRY" {
			continue
		}
		if values[0] == "MALFORMED" || values[0] != "OK" || len(values) < 3 {
			return FriendIndexScan{}, errors.New("friend: malformed index scan reply")
		}
		revision, parseErr := strconv.ParseUint(values[1], 10, 64)
		if parseErr != nil {
			return FriendIndexScan{}, parseErr
		}
		hasMore := values[2] == "1"
		rowValues := values[3:]
		if len(rowValues)%4 != 0 {
			return FriendIndexScan{}, errors.New("friend: index scan row cardinality mismatch")
		}
		entries := make([]FriendIndexEntry, 0, len(rowValues)/4)
		for i := 0; i < len(rowValues); i += 4 {
			member := rowValues[i]
			id, idErr := friendIDFromIndexMember(member)
			if idErr != nil {
				return FriendIndexScan{}, idErr
			}
			relation := rowValues[i+1]
			epoch, epochErr := strconv.ParseUint(rowValues[i+2], 10, 64)
			activated, activatedErr := strconv.ParseInt(rowValues[i+3], 10, 64)
			if relation == "" || epochErr != nil || activatedErr != nil || epoch == 0 || activated <= 0 {
				return FriendIndexScan{}, errors.New("friend: malformed index row")
			}
			entries = append(entries, FriendIndexEntry{FriendPlayerID: id, RelationID: relation,
				RelationEpoch: epoch, ActivatedAtMs: activated})
		}
		if cursor != "" && position.RosterRevision != revision {
			return FriendIndexScan{}, ErrRosterChanged
		}
		return FriendIndexScan{Entries: entries, RosterRevision: revision, HasMore: hasMore}, nil
	}
	return FriendIndexScan{}, ErrRosterChanged
}
