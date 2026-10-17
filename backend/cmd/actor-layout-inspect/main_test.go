package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
)

type spyRedisReader struct {
	scans    map[string][]string
	hashes   map[string]map[string]string
	hashErrs map[string]error
	commands []string
}

func (s *spyRedisReader) Scan(_ context.Context, _ uint64, match string, _ int64) ([]string, uint64, error) {
	s.commands = append(s.commands, "SCAN "+match)
	return append([]string(nil), s.scans[match]...), 0, nil
}

func (s *spyRedisReader) HGetAll(_ context.Context, key string) (map[string]string, error) {
	s.commands = append(s.commands, "HGETALL "+key)
	if err := s.hashErrs[key]; err != nil {
		return nil, err
	}
	clone := make(map[string]string, len(s.hashes[key]))
	for name, value := range s.hashes[key] {
		clone[name] = value
	}
	return clone, nil
}

func TestInspectorUsesOnlyReadCommandsAndReportsPersistedBytes(t *testing.T) {
	playerKey := "{classic-farm:dev:z1:auth:player:v1:s:3}:actor:101"
	farmKey := "{classic-farm:dev:z1:auth:farm:v1:s:7}:actor:202"
	orderKey := "{classic-farm:dev:z1:auth:farm:v1:s:7}:order:CARE_REWARD:order-1"
	spy := &spyRedisReader{
		scans: map[string][]string{
			actorKeyPattern("dev:z1", "player"):                {playerKey},
			actorKeyPattern("dev:z1", "farm"):                  {farmKey},
			"{classic-farm:dev:z1:auth:player:v1:s:3}:order:*": {},
			"{classic-farm:dev:z1:auth:farm:v1:s:7}:order:*":   {orderKey},
		},
		hashes: map[string]map[string]string{
			playerKey: {
				"_meta:record_version": "4", "model:record": "player-record", "model:public_profile": "profile",
				"receipt:op:aaa": "player-receipt",
			},
			farmKey: {
				"_meta:record_version": "9", "model:record": "farm-record", "model:opportunity_header": "header",
				"model:opportunity_facts": "facts", "receipt:op:bbb": "farm-receipt", "receipt:resolve:ccc": "resolution",
			},
			orderKey: {
				"canonical_source_authority_id": "202", "typed_payload": "order-payload", "work_id": "order-1", "work_type": "CARE_REWARD",
			},
		},
		hashErrs: map[string]error{},
	}
	inspector, err := newInspector(spy, inspectConfig{Namespace: "dev:z1", SamplePlayers: 1, SampleFarms: 1, Now: func() time.Time {
		return time.Unix(1_700_000_000, 0)
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := inspector.inspect(context.Background())
	if got.Status != "complete" || got.ExitCode != 0 || len(got.Failures) != 0 {
		t.Fatalf("report status=%s exit=%d failures=%+v", got.Status, got.ExitCode, got.Failures)
	}
	if got.Domains["player"].Sampled != 1 || got.Domains["farm"].Sampled != 1 {
		t.Fatalf("sample counts=%+v", got.Domains)
	}
	if got.Domains["player"].ActorRecordSerializedBytes.P50 != len("player-record") ||
		got.Domains["farm"].ReceiptBytesPerItem.P50 != len("farm-receipt") ||
		got.Domains["farm"].OrderPayloadBytesPerItem.P50 != len("order-payload") {
		t.Fatalf("byte statistics=%+v", got.Domains)
	}
	if field := got.Domains["player"].ModelFields["public_profile"]; field.PresentSamples != 1 || field.LogicalBytes.P50 != len("public_profile")+len("profile") {
		t.Fatalf("public_profile field=%+v", field)
	}
	for _, command := range spy.commands {
		if !strings.HasPrefix(command, "SCAN ") && !strings.HasPrefix(command, "HGETALL ") {
			t.Fatalf("inspector issued non-read command %q", command)
		}
		for _, forbidden := range []string{"CLAIM", "COMMIT", "EVAL", "SET", "DEL", "HSET", "ZADD"} {
			if strings.HasPrefix(command, forbidden+" ") {
				t.Fatalf("inspector issued write/authority command %q", command)
			}
		}
	}
}

func TestInspectorRecordsPerItemFailureAndReturnsPartial(t *testing.T) {
	key := "{classic-farm:dev:z1:auth:player:v1:s:3}:actor:101"
	spy := &spyRedisReader{
		scans:  map[string][]string{actorKeyPattern("dev:z1", "player"): {key}},
		hashes: map[string]map[string]string{}, hashErrs: map[string]error{key: errors.New("injected read failure")},
	}
	inspector, err := newInspector(spy, inspectConfig{Namespace: "dev:z1", SamplePlayers: 1})
	if err != nil {
		t.Fatal(err)
	}
	got := inspector.inspect(context.Background())
	if got.Status != "partial" || got.ExitCode != 2 || got.Domains["player"].Sampled != 0 || len(got.Failures) == 0 {
		t.Fatalf("partial report=%+v", got)
	}
	if failure := got.Failures[0]; failure.ActorID != 101 || failure.Stage != "actor_hash" || !strings.Contains(failure.Error, "injected") {
		t.Fatalf("failure=%+v", failure)
	}
}

func TestDistributionUsesNearestRank(t *testing.T) {
	values := make([]int, 100)
	for index := range values {
		values[index] = index + 1
	}
	got := summarize(values)
	if got.P50 != 50 || got.P95 != 95 || got.P99 != 99 || got.Max != 100 {
		t.Fatalf("distribution=%+v", got)
	}
}

func TestScanUntilReportsTruncatedWhenOneBatchExceedsLimit(t *testing.T) {
	spy := &spyRedisReader{scans: map[string][]string{"actor:*": {"actor:1", "actor:2"}}}
	inspector := &inspector{reader: spy}
	keys, complete, err := inspector.scanUntil(context.Background(), "actor:*", 1)
	if err != nil || complete || len(keys) != 1 {
		t.Fatalf("keys=%v complete=%v err=%v", keys, complete, err)
	}
}

func TestRedisErrorRedactsFullURL(t *testing.T) {
	secretURL := "redis://user:password@example:6379/0"
	got := redactRedisError(fmt.Errorf("dial failed for %s password=%s", secretURL, "password"), secretURL, "password")
	if strings.Contains(got.Error(), "password") || !strings.Contains(got.Error(), "<redacted-secret>") {
		t.Fatalf("redaction=%q", got)
	}
}

func TestAnalyzeActorReportsOnlyExactV2AsV2(t *testing.T) {
	state, err := playeractormodelv2.Encode(&playerpb.PlayerRecord{PlayerId: 303})
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{"_meta:model_layout_version": "2"}
	for name, value := range state {
		fields["model:"+name] = string(value)
	}
	sample, err := analyzeActor("player", 303, "bucket", fields)
	if err != nil || sample.modelLayout != "v2" || sample.actorRecordBytes != 0 {
		t.Fatalf("sample=%+v err=%v", sample, err)
	}
	delete(fields, "model:inventory")
	if _, err := analyzeActor("player", 303, "bucket", fields); err == nil {
		t.Fatal("partial v2 reported valid")
	}
	fields["model:inventory"] = string(state["inventory"])
	fields["model:record"] = "forbidden-rescue"
	if _, err := analyzeActor("player", 303, "bucket", fields); err == nil {
		t.Fatal("mixed-invalid v2 reported valid")
	}
}
