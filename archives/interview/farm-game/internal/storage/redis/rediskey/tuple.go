package rediskey

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
)

const (
	RedisNamespaceEnvironmentEnv = "REDIS_NAMESPACE_ENVIRONMENT"
	RedisNamespaceZoneEnv        = "REDIS_NAMESPACE_ZONE"
	RedisPhysicalProfileEnv      = "REDIS_PHYSICAL_PROFILE_VERSION"
	RedisAuthorityLayoutEnv      = "REDIS_AUTHORITY_LAYOUT_VERSION"
	RedisWorkflowLayoutEnv       = "REDIS_WORKFLOW_LAYOUT_VERSION"
	RedisWorkflowHashEnv         = "REDIS_WORKFLOW_HASH_VERSION"
	RedisWorkflowBucketCountEnv  = "REDIS_WORKFLOW_BUCKET_COUNT"

	RedisKeyspaceFingerprintVersion = "redis-keyspace-tuple-v1"
	redisKeyspaceFingerprintDomain  = "farmgame/redis-keyspace-tuple/v1"
)

var ErrInvalidKeyspaceTuple = errors.New("rediskey: invalid keyspace tuple")

// KeyspaceTupleInput is the complete non-secret Redis placement tuple in its
// original textual form. Numeric fields remain strings until validation so
// non-canonical inputs such as 01 cannot be silently normalized.
type KeyspaceTupleInput struct {
	NamespaceEnvironment string
	NamespaceZone        string
	PhysicalProfile      string
	AuthorityLayout      string
	WorkflowLayout       string
	WorkflowHashVersion  string
	WorkflowBucketCount  string
}

// KeyspaceTuple is the one canonical non-secret placement value shared by all
// composed remote-watch roles. It deliberately has no Redis URL, credential,
// prefix, or other secret-bearing field.
type KeyspaceTuple struct {
	profile PhysicalProfile
}

// ParseKeyspaceTupleEnvironment reads exactly the seven approved non-secret
// environment variables. Missing, empty, malformed, or non-canonical values
// fail closed; callers choose whether their deployment mode requires it.
func ParseKeyspaceTupleEnvironment(getenv func(string) string) (KeyspaceTuple, error) {
	if getenv == nil {
		return KeyspaceTuple{}, fmt.Errorf("%w: environment reader", ErrInvalidKeyspaceTuple)
	}
	return ParseKeyspaceTuple(KeyspaceTupleInput{
		NamespaceEnvironment: getenv(RedisNamespaceEnvironmentEnv),
		NamespaceZone:        getenv(RedisNamespaceZoneEnv),
		PhysicalProfile:      getenv(RedisPhysicalProfileEnv),
		AuthorityLayout:      getenv(RedisAuthorityLayoutEnv),
		WorkflowLayout:       getenv(RedisWorkflowLayoutEnv),
		WorkflowHashVersion:  getenv(RedisWorkflowHashEnv),
		WorkflowBucketCount:  getenv(RedisWorkflowBucketCountEnv),
	})
}

func ParseKeyspaceTuple(input KeyspaceTupleInput) (KeyspaceTuple, error) {
	hashVersion, err := parseCanonicalTupleUint(RedisWorkflowHashEnv, input.WorkflowHashVersion)
	if err != nil {
		return KeyspaceTuple{}, err
	}
	bucketCount, err := parseCanonicalTupleUint(RedisWorkflowBucketCountEnv, input.WorkflowBucketCount)
	if err != nil {
		return KeyspaceTuple{}, err
	}
	namespace, err := NewNamespace(input.NamespaceEnvironment, input.NamespaceZone)
	if err != nil {
		return KeyspaceTuple{}, fmt.Errorf("%w: namespace", ErrInvalidKeyspaceTuple)
	}
	profile := PhysicalProfile{
		ProfileVersion:         input.PhysicalProfile,
		Namespace:              namespace,
		AuthorityLayoutVersion: input.AuthorityLayout,
		WorkflowLayoutVersion:  input.WorkflowLayout,
		WorkflowHashVersion:    hashVersion,
		WorkflowBucketCount:    bucketCount,
	}
	if err := profile.Validate(); err != nil {
		return KeyspaceTuple{}, fmt.Errorf("%w: physical profile", ErrInvalidKeyspaceTuple)
	}
	if profile.AuthorityLayoutVersion != "v1" || profile.WorkflowLayoutVersion != "v1" {
		return KeyspaceTuple{}, fmt.Errorf("%w: unsupported layout", ErrInvalidKeyspaceTuple)
	}
	return KeyspaceTuple{profile: profile}, nil
}

func parseCanonicalTupleUint(name, raw string) (uint32, error) {
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != raw {
		return 0, fmt.Errorf("%w: %s", ErrInvalidKeyspaceTuple, name)
	}
	return uint32(parsed), nil
}

func (t KeyspaceTuple) Validate() error {
	if err := t.profile.Validate(); err != nil {
		return ErrInvalidKeyspaceTuple
	}
	return nil
}

func (t KeyspaceTuple) Profile() PhysicalProfile { return t.profile }

func (t KeyspaceTuple) Namespace() Namespace { return t.profile.Namespace }

// Fingerprint is a versioned, domain-separated SHA-256 over labeled,
// length-delimited canonical fields. Only this digest is suitable for logs;
// connection URLs and credentials can never enter the value.
func (t KeyspaceTuple) Fingerprint() (string, error) {
	if err := t.Validate(); err != nil {
		return "", err
	}
	digest := sha256.New()
	writeTupleFrame(digest, redisKeyspaceFingerprintDomain)
	for _, field := range [...]struct {
		name  string
		value string
	}{
		{"namespace_environment", t.profile.Namespace.Environment},
		{"namespace_zone", t.profile.Namespace.Zone},
		{"physical_profile", t.profile.ProfileVersion},
		{"authority_layout", t.profile.AuthorityLayoutVersion},
		{"workflow_layout", t.profile.WorkflowLayoutVersion},
		{"workflow_hash_version", strconv.FormatUint(uint64(t.profile.WorkflowHashVersion), 10)},
		{"workflow_bucket_count", strconv.FormatUint(uint64(t.profile.WorkflowBucketCount), 10)},
	} {
		writeTupleFrame(digest, field.name)
		writeTupleFrame(digest, field.value)
	}
	return RedisKeyspaceFingerprintVersion + ":" + hex.EncodeToString(digest.Sum(nil)), nil
}

func writeTupleFrame(destination hash.Hash, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write([]byte(value))
}
