package authority

import (
	"context"
	"strings"
	"testing"

	"farmgame/internal/storage/redis/rediskey"
)

func testResultEntry(digest string) ResultEntry {
	return ResultEntry{
		CommandType:      "PLANT_CROP",
		OperationDigest:  digest,
		RootKind:         "PLAYER",
		RootID:           "10001",
		SourceRef:        "receipt-op-1",
		FinalStatusClass: "SUCCEEDED",
		MinimalOutcome:   []byte("ok"),
	}
}

func TestResultDirectoryPublishIdempotentAndConflict(t *testing.T) {
	rig := newTestRig(t)
	namespace := testProfile(t).Namespace
	dir, err := NewResultDirectoryStore(rig.client, rig.store.Registry(), namespace, 0)
	if err != nil {
		t.Fatal(err)
	}

	// The frozen physical key is v2; writes must never land on v1.
	proj, err := rediskey.NewProjectionKeys(namespace)
	if err != nil {
		t.Fatal(err)
	}
	v2Key, err := proj.ResultDirectory(10001)
	if err != nil {
		t.Fatal(err)
	}
	if string(v2Key) != "classic-farm:test:z1:proj:result:v2:10001" {
		t.Fatalf("result directory key=%q", v2Key)
	}

	digest := strings.Repeat("ab", 32)
	entry := testResultEntry(digest)

	first, err := dir.PublishResult(context.Background(), 10001, entry)
	if err != nil {
		t.Fatal(err)
	}
	if first != ResultPublishPublished {
		t.Fatalf("first=%s", first)
	}

	// Same digest, same entry: idempotent replay.
	replay, err := dir.PublishResult(context.Background(), 10001, entry)
	if err != nil {
		t.Fatal(err)
	}
	if replay != ResultPublishAlreadyPublished {
		t.Fatalf("replay=%s", replay)
	}

	// Same digest, different entry: fail closed.
	mutated := entry
	mutated.FinalStatusClass = "REJECTED"
	conflict, err := dir.PublishResult(context.Background(), 10001, mutated)
	if err != nil {
		t.Fatal(err)
	}
	if conflict != ResultPublishConflict {
		t.Fatalf("conflict=%s", conflict)
	}

	// Lookup returns the original entry; the conflict did not overwrite it.
	got, found, err := dir.LookupResult(context.Background(), 10001, digest)
	if err != nil || !found {
		t.Fatalf("lookup found=%v err=%v", found, err)
	}
	if got.FinalStatusClass != "SUCCEEDED" || string(got.MinimalOutcome) != "ok" || got.SourceRef != "receipt-op-1" {
		t.Fatalf("got=%+v", got)
	}

	// The entry physically lives under the v2 key and nowhere else.
	if exists, err := rig.client.HExists(context.Background(), string(v2Key), digest).Result(); err != nil || !exists {
		t.Fatalf("entry missing under v2 key: exists=%v err=%v", exists, err)
	}
	if exists, err := rig.client.HExists(context.Background(), "classic-farm:test:z1:proj:result:v1:10001", digest).Result(); err != nil || exists {
		t.Fatalf("entry must never land on v1: exists=%v err=%v", exists, err)
	}

	// Missing digest is a plain NOT_FOUND, never proof of non-execution.
	_, found, err = dir.LookupResult(context.Background(), 10001, strings.Repeat("cd", 32))
	if err != nil || found {
		t.Fatalf("missing found=%v err=%v", found, err)
	}

	// Entry size limit.
	oversized := testResultEntry(strings.Repeat("ef", 32))
	oversized.MinimalOutcome = make([]byte, 4096)
	if _, err := dir.PublishResult(context.Background(), 10001, oversized); err == nil {
		t.Fatal("oversized entry must be rejected")
	}
}
