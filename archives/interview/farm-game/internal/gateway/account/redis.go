package account

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/gateway/redisscript"
	"farmgame/internal/observability/redisworkload"

	"github.com/redis/go-redis/v9"
)

const createAccountScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('SET', KEYS[2], ARGV[4])
end
local player_id = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'username', ARGV[1],
  'player_id', player_id,
  'password_verifier', ARGV[2],
  'created_at_ms', ARGV[3],
  'password_kdf_version', ARGV[5],
  'password_kdf_params', ARGV[6],
  'account_status', 'ACTIVE',
  'password_changed_at_ms', ARGV[3])
return player_id
`

const createAccountScriptName = "create_account_v1"

const updatePasswordVerifierScript = `
if (redis.call('HGET', KEYS[1], 'password_verifier') or '') ~= ARGV[1] then
  return 0
end
redis.call('HSET', KEYS[1],
  'password_verifier', ARGV[2],
  'password_kdf_version', ARGV[3],
  'password_kdf_params', ARGV[4],
  'password_changed_at_ms', ARGV[5])
return 1
`

const updatePasswordVerifierScriptName = "update_password_verifier_v1"

const updateAccountStatusScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'account_status', ARGV[1])
return 1
`

const updateAccountStatusScriptName = "update_account_status_v1"

type RedisStore struct {
	client       redis.UniversalClient
	scripts      *redisscript.Registry
	prefix       string
	playerIDBase uint64
}

func NewRedisStore(client redis.UniversalClient, prefix string, playerIDBase uint64) (*RedisStore, error) {
	if client == nil || strings.TrimSpace(prefix) == "" {
		return nil, errors.New("account: redis client and prefix required")
	}
	if playerIDBase == 0 {
		playerIDBase = 1_000_000
	}
	scripts, err := redisscript.New(client,
		redisscript.Definition{Name: createAccountScriptName, Source: createAccountScript},
		redisscript.Definition{Name: updatePasswordVerifierScriptName, Source: updatePasswordVerifierScript},
		redisscript.Definition{Name: updateAccountStatusScriptName, Source: updateAccountStatusScript},
	)
	if err != nil {
		return nil, err
	}
	return &RedisStore{
		client: client, scripts: scripts,
		prefix: strings.TrimSuffix(strings.TrimSpace(prefix), ":"), playerIDBase: playerIDBase,
	}, nil
}

// EnsureScriptsLoaded preloads the closed Gate account script family. The
// foreground path remains able to recover an exact-source NOSCRIPT after a
// Redis failover.
func (r *RedisStore) EnsureScriptsLoaded(ctx context.Context) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	if r == nil || r.scripts == nil {
		return errors.New("account: Redis script registry unavailable")
	}
	return r.scripts.EnsureLoaded(ctx)
}

func (r *RedisStore) keyBase() string { return r.prefix + ":{web-account}" }
func (r *RedisStore) usernameKey(username string) string {
	return r.keyBase() + ":username:" + username
}
func (r *RedisStore) sequenceKey() string { return r.keyBase() + ":player-id-sequence" }

func (r *RedisStore) Create(ctx context.Context, username, passwordVerifier string, now time.Time) (*Credential, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	kdfVersion, kdfParams := verifierMetadata(passwordVerifier)
	raw, err := r.scripts.Run(ctx, createAccountScriptName, []string{
		r.usernameKey(username), r.sequenceKey(),
	}, username, passwordVerifier, now.UnixMilli(), r.playerIDBase,
		kdfVersion, kdfParams).Int64()
	if err != nil {
		return nil, err
	}
	if raw == 0 {
		return nil, ErrAccountExists
	}
	if raw < 0 {
		return nil, fmt.Errorf("account: invalid allocated player id %d", raw)
	}
	return &Credential{
		Username: username, PlayerID: uint64(raw),
		PasswordVerifier: passwordVerifier, PasswordKDFVersion: kdfVersion,
		PasswordKDFParams: kdfParams, AccountStatus: AccountStatusActive,
		CreatedAt: now, PasswordChangedAt: now,
	}, nil
}

func (r *RedisStore) Get(ctx context.Context, username string) (*Credential, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	values, err := r.client.HMGet(ctx, r.usernameKey(username),
		"username", "player_id", "password_verifier", "created_at_ms",
		"password_kdf_version", "password_kdf_params", "account_status",
		"password_changed_at_ms").Result()
	if err != nil {
		return nil, err
	}
	if len(values) != 8 || values[0] == nil {
		return nil, ErrAccountNotFound
	}
	asString := func(value any) string { return fmt.Sprint(value) }
	playerID, err := strconv.ParseUint(asString(values[1]), 10, 64)
	if err != nil || playerID == 0 {
		return nil, errors.New("account: malformed player id")
	}
	createdAtMs, err := strconv.ParseInt(asString(values[3]), 10, 64)
	if err != nil {
		return nil, errors.New("account: malformed creation time")
	}
	credential := &Credential{
		Username: asString(values[0]), PlayerID: playerID,
		PasswordVerifier: asString(values[2]), CreatedAt: time.UnixMilli(createdAtMs),
	}
	credential.PasswordKDFVersion = optionalString(values[4])
	credential.PasswordKDFParams = optionalString(values[5])
	credential.AccountStatus = optionalString(values[6])
	if credential.AccountStatus == "" {
		credential.AccountStatus = AccountStatusActive
	}
	if changedAt := optionalString(values[7]); changedAt != "" {
		changedAtMS, parseErr := strconv.ParseInt(changedAt, 10, 64)
		if parseErr != nil {
			return nil, errors.New("account: malformed password change time")
		}
		credential.PasswordChangedAt = time.UnixMilli(changedAtMS)
	} else {
		credential.PasswordChangedAt = credential.CreatedAt
	}
	if credential.PasswordKDFVersion == "" {
		credential.PasswordKDFVersion, credential.PasswordKDFParams = verifierMetadata(credential.PasswordVerifier)
	}
	if credential.Username == "" || credential.PasswordVerifier == "" {
		return nil, errors.New("account: malformed credential record")
	}
	return credential, nil
}

func (r *RedisStore) UpdatePasswordVerifier(ctx context.Context, username, expectedVerifier, nextVerifier string, changedAt time.Time) (bool, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	version, params := verifierMetadata(nextVerifier)
	value, err := r.scripts.Run(ctx, updatePasswordVerifierScriptName,
		[]string{r.usernameKey(username)}, expectedVerifier, nextVerifier,
		version, params, changedAt.UnixMilli()).Int64()
	return value == 1, err
}

func (r *RedisStore) UpdateAccountStatus(ctx context.Context, username, status string) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	value, err := r.scripts.Run(ctx, updateAccountStatusScriptName,
		[]string{r.usernameKey(username)}, status).Int64()
	if err != nil {
		return err
	}
	if value == 0 {
		return ErrAccountNotFound
	}
	return nil
}

func optionalString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
