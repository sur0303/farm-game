package rediskey

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/workidentity"
)

const redisKeyspaceLiveOptIn = "FARM_REDIS_TUPLE_LIVE_TEST"

// TestLiveRedisTupleExactKey is the company-Redis keyspace proof. A connection
// URL alone never activates it: the separate explicit opt-in and the complete
// immutable tuple are both mandatory. It owns one cryptographically unique
// tuple-built exact key, refuses SETNX collisions, and never enumerates or
// broadly cleans shared keyspace.
func TestLiveRedisTupleExactKey(t *testing.T) {
	if os.Getenv(redisKeyspaceLiveOptIn) != "1" {
		t.Skip(redisKeyspaceLiveOptIn + "=1 is required")
	}
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Fatal("FARM_REDIS_URL is required")
	}
	tuple, err := ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		t.Fatal("complete canonical Redis tuple is required")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal("company Redis URL is invalid")
	}
	client := redis.NewClient(options)
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("company Redis client close failed")
		}
	}()

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("cryptographic probe identity unavailable")
	}
	probeID, err := workidentity.StableOpaqueID("rt2w-company-redis-live-v1", hex.EncodeToString(nonce[:]))
	if err != nil {
		t.Fatal("cryptographic probe identity construction failed")
	}
	ref, err := tuple.Profile().WorkflowBucket(probeID)
	if err != nil {
		t.Fatal("tuple workflow placement failed")
	}
	keys, err := NewWorkflowKeys(tuple.Profile())
	if err != nil {
		t.Fatal("tuple workflow key builder failed")
	}
	key, err := keys.Work(ref)
	if err != nil {
		t.Fatal("exact tuple probe key construction failed")
	}
	marker := "rt2w-keyspace-tuple-v1:" + hex.EncodeToString(nonce[:])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal("company Redis ping failed")
	}
	if err := executeExactLiveProbe(ctx, client, key.String(), marker, 5*time.Second); err != nil {
		t.Fatal("exact tuple probe SETNX/readback/owned cleanup failed")
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		t.Fatal("tuple fingerprint failed")
	}
	t.Logf("exact company Redis tuple proof passed fingerprint=%s cleanup_EXISTS=0", fingerprint)
}

var (
	errExactProbeCollision     = errors.New("exact probe key collision")
	errExactProbeOwnershipLost = errors.New("exact probe marker ownership lost")
)

const exactProbeMarkerDeleteScript = `
local value = redis.call('GET', KEYS[1])
if value == false then return 0 end
if value ~= ARGV[1] then return -1 end
return redis.call('DEL', KEYS[1])
`

type exactProbeClient interface {
	SetNX(context.Context, string, interface{}, time.Duration) *redis.BoolCmd
	Get(context.Context, string) *redis.StringCmd
	Eval(context.Context, string, []string, ...interface{}) *redis.Cmd
	Exists(context.Context, ...string) *redis.IntCmd
}

// executeExactLiveProbe installs its ambiguity cleanup before SETNX. A
// transport error may have committed, so every return path performs the one
// fixed marker-equals-then-DEL operation. Only a confirmed marker deletion is
// followed by EXISTS=0 proof; an absent or replacement value is never deleted.
func executeExactLiveProbe(ctx context.Context, client exactProbeClient, key, marker string, cleanupTimeout time.Duration) (err error) {
	if ctx == nil || client == nil || key == "" || marker == "" || cleanupTimeout <= 0 {
		return errors.New("invalid exact probe")
	}
	confirmedOwned := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		deleted, cleanupErr := cleanupOwnedLiveProbe(cleanupCtx, client, key, marker)
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
			return
		}
		if confirmedOwned && !deleted {
			err = errors.Join(err, errors.New("owned exact probe marker disappeared before cleanup"))
		}
	}()

	created, setErr := client.SetNX(ctx, key, marker, 2*time.Minute).Result()
	if setErr != nil {
		return fmt.Errorf("exact SETNX ambiguous: %w", setErr)
	}
	if !created {
		return errExactProbeCollision
	}
	confirmedOwned = true
	value, getErr := client.Get(ctx, key).Result()
	if getErr != nil || value != marker {
		return errors.New("exact probe readback mismatch")
	}
	return nil
}

// cleanupOwnedLiveProbe returns deleted=true only after the atomic operation
// proves marker ownership and deletes it. Missing and foreign values do not
// trigger EXISTS; a foreign value reports safe ownership loss.
func cleanupOwnedLiveProbe(ctx context.Context, client exactProbeClient, key, marker string) (bool, error) {
	if ctx == nil || client == nil || key == "" || marker == "" {
		return false, errors.New("invalid exact cleanup")
	}
	result, err := client.Eval(ctx, exactProbeMarkerDeleteScript, []string{key}, marker).Int64()
	if err != nil {
		return false, fmt.Errorf("atomic marker cleanup failed")
	}
	switch result {
	case 0:
		return false, nil
	case -1:
		return false, errExactProbeOwnershipLost
	case 1:
		exists, existsErr := client.Exists(ctx, key).Result()
		if existsErr != nil || exists != 0 {
			return false, fmt.Errorf("exact EXISTS=0 proof failed")
		}
		return true, nil
	default:
		return false, fmt.Errorf("atomic marker cleanup returned invalid result")
	}
}

func TestOwnedLiveProbeCleanupDeletesExactMarkerAndProvesAbsent(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	const ownedKey = "{classic-farm:test:cleanup:wf:v1:b:1}:work:rt2w-live-owned"
	if err := executeExactLiveProbe(context.Background(), client, ownedKey, "owned", time.Second); err != nil {
		t.Fatal(err)
	}
	if exists := server.Exists(ownedKey); exists {
		t.Fatal("exact cleanup left the owned key present")
	}
}

type exactProbeFake struct {
	key             string
	value           string
	exists          bool
	setErr          error
	replaceAfterGet string
	setCalls        int
	getCalls        int
	evalCalls       int
	existsCalls     int
	evalScript      string
	evalKeys        []string
	evalArgs        []interface{}
}

func (f *exactProbeFake) SetNX(_ context.Context, key string, value interface{}, _ time.Duration) *redis.BoolCmd {
	f.setCalls++
	created := !f.exists
	if created {
		f.key, f.value, f.exists = key, fmt.Sprint(value), true
	}
	return redis.NewBoolResult(created, f.setErr)
}

func (f *exactProbeFake) Get(_ context.Context, key string) *redis.StringCmd {
	f.getCalls++
	if !f.exists || f.key != key {
		return redis.NewStringResult("", redis.Nil)
	}
	value := f.value
	if f.replaceAfterGet != "" {
		f.value = f.replaceAfterGet
		f.replaceAfterGet = ""
	}
	return redis.NewStringResult(value, nil)
}

func (f *exactProbeFake) Eval(_ context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.evalCalls++
	f.evalScript = script
	f.evalKeys = append([]string(nil), keys...)
	f.evalArgs = append([]interface{}(nil), args...)
	if len(keys) != 1 || len(args) != 1 || !f.exists || f.key != keys[0] {
		return redis.NewCmdResult(int64(0), nil)
	}
	if f.value != fmt.Sprint(args[0]) {
		return redis.NewCmdResult(int64(-1), nil)
	}
	f.exists = false
	f.value = ""
	return redis.NewCmdResult(int64(1), nil)
}

func (f *exactProbeFake) Exists(_ context.Context, keys ...string) *redis.IntCmd {
	f.existsCalls++
	if len(keys) == 1 && f.exists && f.key == keys[0] {
		return redis.NewIntResult(1, nil)
	}
	return redis.NewIntResult(0, nil)
}

func TestExactLiveProbeCommittedThenSetNXErrorCleansMarker(t *testing.T) {
	fake := &exactProbeFake{setErr: context.DeadlineExceeded}
	const key = "{classic-farm:test:cleanup:wf:v1:b:1}:work:committed-then-error"
	const marker = "owned-marker"
	err := executeExactLiveProbe(context.Background(), fake, key, marker, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) || fake.exists {
		t.Fatalf("err=%v key_exists=%t value=%q", err, fake.exists, fake.value)
	}
	if fake.setCalls != 1 || fake.getCalls != 0 || fake.evalCalls != 1 || fake.existsCalls != 1 ||
		fake.evalScript != exactProbeMarkerDeleteScript || len(fake.evalKeys) != 1 || fake.evalKeys[0] != key ||
		len(fake.evalArgs) != 1 || fake.evalArgs[0] != marker {
		t.Fatalf("unexpected exact operations: %+v", fake)
	}
}

func TestExactLiveProbeReplacementBeforeCleanupIsUntouched(t *testing.T) {
	fake := &exactProbeFake{replaceAfterGet: "replacement-value"}
	const key = "{classic-farm:test:cleanup:wf:v1:b:1}:work:replacement"
	err := executeExactLiveProbe(context.Background(), fake, key, "owned-marker", time.Second)
	if !errors.Is(err, errExactProbeOwnershipLost) || !fake.exists || fake.value != "replacement-value" {
		t.Fatalf("err=%v key_exists=%t value=%q", err, fake.exists, fake.value)
	}
	if fake.setCalls != 1 || fake.getCalls != 1 || fake.evalCalls != 1 || fake.existsCalls != 0 {
		t.Fatalf("replacement path operations: %+v", fake)
	}
}

func TestExactLiveProbeCollisionIsUntouchedAndFailsSafely(t *testing.T) {
	const key = "{classic-farm:test:cleanup:wf:v1:b:1}:work:collision"
	fake := &exactProbeFake{key: key, value: "collision-value", exists: true}
	err := executeExactLiveProbe(context.Background(), fake, key, "owned-marker", time.Second)
	if !errors.Is(err, errExactProbeCollision) || !errors.Is(err, errExactProbeOwnershipLost) ||
		!fake.exists || fake.value != "collision-value" {
		t.Fatalf("err=%v key_exists=%t value=%q", err, fake.exists, fake.value)
	}
	if fake.setCalls != 1 || fake.getCalls != 0 || fake.evalCalls != 1 || fake.existsCalls != 0 {
		t.Fatalf("collision path operations: %+v", fake)
	}
}
