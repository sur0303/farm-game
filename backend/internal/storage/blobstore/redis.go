package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func classifyRedisError(err error, write bool) error {
	if err == nil {
		return nil
	}
	// A write timeout can happen after Redis executed the Lua script but
	// before the reply reached us. Retrying it would risk a duplicate effect.
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &netErr) && netErr.Timeout()) {
		if write {
			return fmt.Errorf("%w: %v", ErrTimeoutUnknown, err)
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	// A failed dial is known not to have reached the command processor.
	if errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	// For non-dial transport failures, be conservative: the command may have
	// been sent even when the client cannot tell whether it was executed.
	if write {
		return fmt.Errorf("%w: %v", ErrTimeoutUnknown, err)
	}
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

const insertScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], 'version', '1', 'data', ARGV[1])
return 1
`

const casScript = `
local current = redis.call('HGET', KEYS[1], 'version')
if not current then
  return -1
end
local writer_epoch = redis.call('HGET', KEYS[1], 'writer_epoch')
if writer_epoch and writer_epoch ~= '0' then
  return -2
end
if current ~= ARGV[1] then
  return 0
end
local next_version = tostring(tonumber(current) + 1)
redis.call('HSET', KEYS[1], 'version', next_version, 'data', ARGV[2])
return 1
`

const insertEpochScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], 'version', '1', 'writer_epoch', ARGV[1], 'data', ARGV[2])
return 1
`

const casEpochScript = `
local current = redis.call('HGET', KEYS[1], 'version')
if not current then
  return -1
end
local writer_epoch = redis.call('HGET', KEYS[1], 'writer_epoch') or '0'
if writer_epoch ~= ARGV[2] then
  return -2
end
if current ~= ARGV[1] then
  return 0
end
local next_version = tostring(tonumber(current) + 1)
redis.call('HSET', KEYS[1], 'version', next_version, 'data', ARGV[3])
return 1
`

const fenceScript = `
local current = redis.call('HGET', KEYS[1], 'version')
if not current then
  return -1
end
local writer_epoch = tonumber(redis.call('HGET', KEYS[1], 'writer_epoch') or '0')
local requested = tonumber(ARGV[1])
if requested < writer_epoch then
  return -2
end
if requested > writer_epoch then
  redis.call('HSET', KEYS[1], 'writer_epoch', ARGV[1])
end
-- requested == writer_epoch:同一持有者重复 Ensure/操作不写,消除登录等读多写少路径的写放大。
return 1
`

type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisStore(client redis.UniversalClient, prefix string) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("blobstore: redis client required")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil, errors.New("blobstore: redis key prefix required")
	}
	return &RedisStore{client: client, prefix: strings.TrimSuffix(prefix, ":")}, nil
}

func (s *RedisStore) key(id uint64) string {
	return s.prefix + ":" + strconv.FormatUint(id, 10)
}

func (s *RedisStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	values, err := s.client.HMGet(ctx, s.key(id), "version", "data").Result()
	if err != nil {
		return nil, 0, classifyRedisError(err, false)
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return nil, 0, ErrNotFound
	}
	version, err := strconv.ParseUint(fmt.Sprint(values[0]), 10, 64)
	if err != nil || version == 0 {
		return nil, 0, fmt.Errorf("%w: corrupt version", ErrUnavailable)
	}
	var data []byte
	switch value := values[1].(type) {
	case string:
		data = []byte(value)
	case []byte:
		data = append([]byte(nil), value...)
	default:
		return nil, 0, fmt.Errorf("%w: corrupt data", ErrUnavailable)
	}
	return data, version, nil
}

func (s *RedisStore) Insert(ctx context.Context, id uint64, data []byte) error {
	result, err := s.client.Eval(ctx, insertScript, []string{s.key(id)}, data).Int64()
	if err != nil {
		return classifyRedisError(err, true)
	}
	if result == 0 {
		return ErrExists
	}
	return nil
}

func (s *RedisStore) CAS(ctx context.Context, id uint64, expectVersion uint64, data []byte) error {
	result, err := s.client.Eval(ctx, casScript, []string{s.key(id)}, strconv.FormatUint(expectVersion, 10), data).Int64()
	if err != nil {
		return classifyRedisError(err, true)
	}
	switch result {
	case 1:
		return nil
	case 0:
		return ErrCASConflict
	case -1:
		return ErrNotFound
	case -2:
		return ErrWriterEpochMismatch
	default:
		return fmt.Errorf("%w: unexpected cas result %d", ErrTimeoutUnknown, result)
	}
}

func (s *RedisStore) InsertEpoch(ctx context.Context, id uint64, writerEpoch uint64, data []byte) error {
	result, err := s.client.Eval(ctx, insertEpochScript, []string{s.key(id)}, strconv.FormatUint(writerEpoch, 10), data).Int64()
	if err != nil {
		return classifyRedisError(err, true)
	}
	if result == 0 {
		return ErrExists
	}
	return nil
}

func (s *RedisStore) CASEpoch(ctx context.Context, id uint64, expectVersion, writerEpoch uint64, data []byte) error {
	result, err := s.client.Eval(ctx, casEpochScript, []string{s.key(id)},
		strconv.FormatUint(expectVersion, 10), strconv.FormatUint(writerEpoch, 10), data).Int64()
	if err != nil {
		return classifyRedisError(err, true)
	}
	switch result {
	case 1:
		return nil
	case 0:
		return ErrCASConflict
	case -1:
		return ErrNotFound
	case -2:
		return ErrWriterEpochMismatch
	default:
		return fmt.Errorf("%w: unexpected fenced cas result %d", ErrTimeoutUnknown, result)
	}
}

func (s *RedisStore) Fence(ctx context.Context, id uint64, writerEpoch uint64) error {
	result, err := s.client.Eval(ctx, fenceScript, []string{s.key(id)}, strconv.FormatUint(writerEpoch, 10)).Int64()
	if err != nil {
		return classifyRedisError(err, true)
	}
	switch result {
	case 1:
		return nil
	case -1:
		return ErrNotFound
	case -2:
		return ErrWriterEpochMismatch
	default:
		return fmt.Errorf("%w: unexpected fence result %d", ErrTimeoutUnknown, result)
	}
}
