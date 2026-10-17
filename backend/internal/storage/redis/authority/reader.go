package authority

import (
	"context"
	"crypto/sha1" //nolint:gosec // Redis script cache identity, not authentication.
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

var (
	ErrAuthorityReaderInvalid = errors.New("redisauthority: invalid authority reader")
	ErrAuthorityReaderCorrupt = errors.New("redisauthority: corrupt shard authority")
)

const (
	authoritySchemaVersion = "1"
	maxAuthorityFieldBytes = 512
)

const readShardAuthorityLua = `
local key_type = redis.call('TYPE', KEYS[1]).ok
if key_type == 'none' then return {'NOT_FOUND'} end
if key_type ~= 'hash' then return {'CORRUPT', 'TYPE'} end
local expected = #ARGV - 1
if redis.call('HLEN', KEYS[1]) ~= expected then return {'CORRUPT', 'FIELD_COUNT'} end
local max_field = tonumber(ARGV[1])
if max_field == nil or max_field <= 0 then return {'CORRUPT', 'BOUND'} end
for index = 2, #ARGV do
  if redis.call('HEXISTS', KEYS[1], ARGV[index]) ~= 1 then return {'CORRUPT', 'FIELD_MISSING'} end
  if redis.call('HSTRLEN', KEYS[1], ARGV[index]) > max_field then return {'CORRUPT', 'FIELD_BYTES'} end
end
local values = redis.call('HMGET', KEYS[1], unpack(ARGV, 2, #ARGV))
local result = {'FOUND'}
for index = 1, #values do result[#result + 1] = values[index] end
return result
`

var shardAuthorityFields = [...]string{
	"schema_version",
	"authority_layout_version",
	"hash_version",
	"route_domain",
	"virtual_shard_id",
	"holder_instance_id",
	"holder_boot_id",
	"writer_epoch",
	"lease_epoch",
	"assignment_revision",
	"lease_state",
	"lease_expire_at_ms",
	"last_transition_id",
	"last_transition_at_ms",
	"last_transition_digest",
}

// AuthorityReaderOptions describes the immutable key layout and the bounded
// read budget. It has no mutation capability and never falls back to a scan.
type AuthorityReaderOptions struct {
	Profile          rediskey.PhysicalProfile
	Resolver         rediskey.AuthorityShardResolver
	AuthorityLayout  string
	OperationTimeout time.Duration
	MaxFieldBytes    int
}

func DefaultAuthorityReaderOptions(profile rediskey.PhysicalProfile, resolver rediskey.AuthorityShardResolver) AuthorityReaderOptions {
	return AuthorityReaderOptions{
		Profile:          profile,
		Resolver:         resolver,
		AuthorityLayout:  profile.AuthorityLayoutVersion,
		OperationTimeout: 2 * time.Second,
		MaxFieldBytes:    maxAuthorityFieldBytes,
	}
}

func (o AuthorityReaderOptions) normalized() (AuthorityReaderOptions, error) {
	if err := o.Profile.Validate(); err != nil {
		return AuthorityReaderOptions{}, fmt.Errorf("%w: profile: %v", ErrAuthorityReaderInvalid, err)
	}
	if o.Resolver == nil {
		return AuthorityReaderOptions{}, fmt.Errorf("%w: resolver required", ErrAuthorityReaderInvalid)
	}
	if err := o.Resolver.Profile().Validate(); err != nil {
		return AuthorityReaderOptions{}, fmt.Errorf("%w: resolver profile: %v", ErrAuthorityReaderInvalid, err)
	}
	if o.AuthorityLayout == "" {
		o.AuthorityLayout = o.Profile.AuthorityLayoutVersion
	}
	if o.AuthorityLayout != o.Profile.AuthorityLayoutVersion {
		return AuthorityReaderOptions{}, fmt.Errorf("%w: layout", ErrAuthorityReaderInvalid)
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = 2 * time.Second
	}
	if o.OperationTimeout > 30*time.Second {
		return AuthorityReaderOptions{}, fmt.Errorf("%w: timeout", ErrAuthorityReaderInvalid)
	}
	if o.MaxFieldBytes <= 0 {
		o.MaxFieldBytes = maxAuthorityFieldBytes
	}
	if o.MaxFieldBytes > maxAuthorityFieldBytes {
		o.MaxFieldBytes = maxAuthorityFieldBytes
	}
	return o, nil
}

// AuthorityReader reads exactly one frozen shard-authority hash key.
type AuthorityReader struct {
	client    redis.UniversalClient
	keys      rediskey.AuthorityKeys
	opts      AuthorityReaderOptions
	scriptSHA string
}

var _ interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
	ReadShardWriteAuthorityObservation(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error)
} = (*AuthorityReader)(nil)

func NewAuthorityReader(client redis.UniversalClient, options AuthorityReaderOptions) (*AuthorityReader, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client required", ErrAuthorityReaderInvalid)
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	keys, err := rediskey.NewAuthorityKeys(normalized.Profile, normalized.Resolver)
	if err != nil {
		return nil, fmt.Errorf("%w: keys: %v", ErrAuthorityReaderInvalid, err)
	}
	sum := sha1.Sum([]byte(readShardAuthorityLua))
	return &AuthorityReader{client: client, keys: keys, opts: normalized, scriptSHA: fmt.Sprintf("%x", sum[:])}, nil
}

func NewShardAuthorityReader(client redis.UniversalClient, options AuthorityReaderOptions) (*AuthorityReader, error) {
	return NewAuthorityReader(client, options)
}

func (r *AuthorityReader) Read(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	return r.ReadShardWriteAuthority(ctx, domain, shard)
}

func (r *AuthorityReader) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	observation, found, err := r.ReadShardWriteAuthorityObservation(ctx, domain, shard)
	return observation.Authority, found, err
}

// ReadShardWriteAuthorityObservation returns the exact transition digest from
// the same atomic Lua read as the authority tuple. Manager recovery uses this
// optional proof seam; ordinary snapshot reads continue to expose only the
// frozen ShardWriteAuthority value.
func (r *AuthorityReader) ReadShardWriteAuthorityObservation(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error) {
	if r == nil || r.client == nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, ErrAuthorityReaderInvalid
	}
	if ctx == nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, fmt.Errorf("%w: context required", ErrAuthorityReaderInvalid)
	}
	if err := ctx.Err(); err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, err
	}
	bucket, err := r.opts.Profile.AuthorityBucketForShard(r.opts.Resolver, domain, shard)
	if err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, fmt.Errorf("%w: bucket: %v", ErrAuthorityReaderInvalid, err)
	}
	key, err := r.keys.ShardAuthority(bucket)
	if err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, fmt.Errorf("%w: key: %v", ErrAuthorityReaderInvalid, err)
	}
	bounded, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	args := make([]any, 0, len(shardAuthorityFields)+1)
	args = append(args, strconv.Itoa(r.opts.MaxFieldBytes))
	for _, field := range shardAuthorityFields {
		args = append(args, field)
	}
	values, err := r.runReadScript(bounded, key.String(), args...)
	if err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, err
	}
	if len(values) == 1 && values[0] == "NOT_FOUND" {
		return authoritystore.ShardWriteAuthorityObservation{}, false, nil
	}
	if len(values) > 0 && values[0] == "CORRUPT" {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: atomic read %s", ErrAuthorityReaderCorrupt, strings.Join(values[1:], ","))
	}
	if len(values) != len(shardAuthorityFields)+1 || values[0] != "FOUND" {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: value count", ErrAuthorityReaderCorrupt)
	}
	return decodeAuthorityObservation(r.opts, domain, shard, values[1:])
}

func (r *AuthorityReader) runReadScript(ctx context.Context, key string, args ...any) ([]string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		reply, err := r.client.EvalSha(ctx, r.scriptSHA, []string{key}, args...).Result()
		if err == nil {
			return decodeAuthorityReadReply(reply)
		}
		if !strings.Contains(err.Error(), "NOSCRIPT") {
			return nil, err
		}
		lastErr = err
		sha, loadErr := r.client.ScriptLoad(ctx, readShardAuthorityLua).Result()
		if loadErr != nil {
			return nil, loadErr
		}
		if sha != r.scriptSHA {
			return nil, fmt.Errorf("%w: script sha", ErrAuthorityReaderCorrupt)
		}
	}
	return nil, lastErr
}

func decodeAuthorityObservation(opts AuthorityReaderOptions, domain actorrouting.RouteDomain, shard uint32, values []string) (authoritystore.ShardWriteAuthorityObservation, bool, error) {
	if values[0] != authoritySchemaVersion || values[1] != opts.AuthorityLayout || values[3] != string(domain) {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: schema/layout/domain", ErrAuthorityReaderCorrupt)
	}
	profile := opts.Resolver.Profile()
	hashVersion, err := parseCanonicalUint("hash_version", values[2], uint64(^uint32(0)))
	if err != nil || uint32(hashVersion) != profile.Version {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: hash profile", ErrAuthorityReaderCorrupt)
	}
	virtualShard, err := parseCanonicalUint("virtual_shard_id", values[4], uint64(^uint32(0)))
	if err != nil || uint32(virtualShard) != shard {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: shard tuple", ErrAuthorityReaderCorrupt)
	}
	assignment, err := parseCanonicalUint("assignment_revision", values[9], ^uint64(0))
	if err != nil || assignment == 0 {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: assignment revision", ErrAuthorityReaderCorrupt)
	}
	writer, err := parseCanonicalUint("writer_epoch", values[7], ^uint64(0))
	if err != nil || writer == 0 {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: writer epoch", ErrAuthorityReaderCorrupt)
	}
	leaseEpoch, err := parseCanonicalUint("lease_epoch", values[8], ^uint64(0))
	if err != nil || leaseEpoch == 0 {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: lease epoch", ErrAuthorityReaderCorrupt)
	}
	leaseUntil, err := parseCanonicalInt("lease_expire_at_ms", values[11])
	if err != nil || leaseUntil < 0 {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: lease expiry", ErrAuthorityReaderCorrupt)
	}
	transitionAt, err := parseCanonicalInt("last_transition_at_ms", values[13])
	if err != nil || transitionAt <= 0 {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: transition timestamp", ErrAuthorityReaderCorrupt)
	}
	if err := validateStableAuthorityText("holder_instance_id", values[5], true); err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, true, err
	}
	if err := validateStableAuthorityText("holder_boot_id", values[6], true); err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, true, err
	}
	if err := validateStableAuthorityText("last_transition_id", values[12], false); err != nil {
		return authoritystore.ShardWriteAuthorityObservation{}, true, err
	}
	if len(values[14]) != 64 || hex.EncodeToString(mustDecodeHex(values[14])) != values[14] {
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: transition digest", ErrAuthorityReaderCorrupt)
	}
	var state authoritystore.ShardAuthorityState
	switch values[10] {
	case "ACTIVE":
		if values[5] == "" || values[6] == "" || leaseUntil <= 0 {
			return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: active tuple", ErrAuthorityReaderCorrupt)
		}
		state = authoritystore.ShardAuthorityActive
	case "UNASSIGNED":
		if values[5] != "" || values[6] != "" || leaseUntil != 0 {
			return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: unassigned tuple", ErrAuthorityReaderCorrupt)
		}
		state = authoritystore.ShardAuthorityUnassigned
	default:
		return authoritystore.ShardWriteAuthorityObservation{}, true, fmt.Errorf("%w: lease state", ErrAuthorityReaderCorrupt)
	}
	return authoritystore.ShardWriteAuthorityObservation{Authority: authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: opts.AuthorityLayout,
		HashVersion:            uint32(hashVersion),
		HashSeed:               profile.Seed,
		Domain:                 domain,
		VirtualShard:           shard,
		HolderID:               values[5],
		HolderBootID:           values[6],
		AssignmentRevision:     assignment,
		WriterEpoch:            writer,
		LeaseEpoch:             leaseEpoch,
		State:                  state,
		LeaseUntilMs:           leaseUntil,
		LastTransitionID:       values[12],
		LastTransitionAtMs:     transitionAt,
	}, LastTransitionDigest: values[14]}, true, nil
}

func parseCanonicalUint(name, value string, max uint64) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("%s: non-canonical integer", name)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value || parsed > max {
		return 0, fmt.Errorf("%s: invalid integer", name)
	}
	return parsed, nil
}

func parseCanonicalInt(name, value string) (int64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') || (len(value) > 2 && strings.HasPrefix(value, "-0")) {
		return 0, fmt.Errorf("%s: non-canonical integer", name)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("%s: invalid integer", name)
	}
	return parsed, nil
}

func validateStableAuthorityText(name, value string, emptyAllowed bool) error {
	if value == "" && emptyAllowed {
		return nil
	}
	if value == "" || len([]byte(value)) > maxAuthorityFieldBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s", ErrAuthorityReaderCorrupt, name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s control", ErrAuthorityReaderCorrupt, name)
		}
	}
	return nil
}

func redisText(value any) (string, bool) {
	switch item := value.(type) {
	case string:
		return item, true
	case []byte:
		return string(item), true
	default:
		return "", false
	}
}

func mustDecodeHex(value string) []byte {
	decoded, _ := hex.DecodeString(value)
	return decoded
}
