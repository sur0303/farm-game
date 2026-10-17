package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"
)

func respCommand(values ...string) []byte {
	var output bytes.Buffer
	fmt.Fprintf(&output, "*%d\r\n", len(values))
	for _, value := range values {
		fmt.Fprintf(&output, "$%d\r\n%s\r\n", len(value), value)
	}
	return output.Bytes()
}

func TestReadRESPFramePreservesExactNestedFrame(t *testing.T) {
	input := "%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n"
	frame, err := readRESPFrame(bufio.NewReader(bytes.NewBufferString(input)))
	if err != nil {
		t.Fatal(err)
	}
	if string(frame) != input {
		t.Fatalf("frame=%q", frame)
	}
}

func TestDecodeCommandUsesBulkLengthsNotDelimiters(t *testing.T) {
	frame := respCommand("SET", "key", "value\r\nwith-delimiter")
	values, err := decodeCommand(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || string(values[2]) != "value\r\nwith-delimiter" {
		t.Fatalf("values=%q", values)
	}
}

func TestClassifyActorCommitFromFrozenPhysicalKeyOrder(t *testing.T) {
	frame := respCommand("EVALSHA", "0123456789012345678901234567890123456789", "6",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:shard-authority",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:actor:10001",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:due",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:receipt-due",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:bucket-meta",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:order:CARE:opaque",
		"secret-payload")
	command, class := classifyRequest(frame)
	if command != "EVALSHA" || class.ActorCommit != 1 || class.Receipts != 1 ||
		class.Orders != 1 || class.DueMutations != 1 || class.Category != "actor_commit" {
		t.Fatalf("command=%s class=%+v", command, class)
	}
}

func TestClassifyWorkflowWriteRequiresMutation(t *testing.T) {
	write := respCommand("EVAL", "redis.call('HSET', KEYS[1], 'state', ARGV[1]); redis.call('ZADD', KEYS[2], 1, 'x')", "2",
		"smoke:run:workflow:work:opaque", "smoke:run:workflow:due", "v")
	_, class := classifyRequest(write)
	if class.WorkflowWrite != 1 || class.DueMutations != 1 || class.Category != "workflow_write" {
		t.Fatalf("write class=%+v", class)
	}
	read := respCommand("EVAL", "return redis.call('ZCOUNT', KEYS[1], '-inf', ARGV[1])", "1", "smoke:run:workflow:due", "1")
	_, class = classifyRequest(read)
	if class.WorkflowWrite != 0 {
		t.Fatalf("read-only script classified as write: %+v", class)
	}
}

func TestAggregateWindowUsesInMemoryOperationBindingAndExcludesBackground(t *testing.T) {
	operationID := "operation-123456"
	unrelatedStateID := "sid1_0123456789012345678901234567890123456789012"
	actor := respCommand("EVALSHA", "0123456789012345678901234567890123456789", "5",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:shard-authority",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:actor:10001",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:due",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:receipt-due",
		"{classic-farm:trace:z1:auth:player:v1:s:1}:bucket-meta", operationID, unrelatedStateID)
	command, class := classifyRequest(actor)
	background := respCommand("ZRANGEBYSCORE", "unrelated:friend:due", unrelatedStateID, "1")
	window := &traceWindow{
		row: traceRow{Command: "BUY", Status: "PASS"}, operationID: []byte(operationID),
		events: []capturedEvent{
			{request: background, response: []byte("*0\r\n"), command: "ZRANGEBYSCORE", class: requestClass{Category: "friend_authority"}},
			{request: actor, response: []byte("*1\r\n+COMMITTED\r\n"), command: command, class: class},
		},
	}
	row := aggregateWindow(window)
	if row.RedisOps != 1 || row.ActorCommit != 1 || row.Receipts != 1 || row.RedisCommands["ZRANGEBYSCORE"] != 0 {
		t.Fatalf("row=%+v", row)
	}
	if len(row.OperationBindingSHA) != 64 || bytes.Contains([]byte(row.OperationBindingSHA), []byte(operationID)) {
		t.Fatalf("binding hash=%q", row.OperationBindingSHA)
	}
}

func TestAggregateWindowCountsOnlyExactSourceNoScriptRecovery(t *testing.T) {
	operationID := "operation-123456"
	source := "return ARGV[1]"
	digest := sha1.Sum([]byte(source))
	sha := hex.EncodeToString(digest[:])
	evalsha := respCommand("EVALSHA", sha, "0", operationID)
	eval := respCommand("EVAL", source, "0", operationID)
	window := &traceWindow{row: traceRow{Command: "CLAIM_TASK", Status: "PASS"}, operationID: []byte(operationID), events: []capturedEvent{
		{request: evalsha, response: []byte("-NOSCRIPT redacted\r\n"), command: "EVALSHA", class: requestClass{Category: "other"}},
		{request: eval, response: []byte("+OK\r\n"), command: "EVAL", class: requestClass{Category: "other"}},
	}}
	row := aggregateWindow(window)
	if row.RedisErrors != 0 || row.NoScriptRecovered != 1 || row.RedisErrorCodes["NOSCRIPT_RECOVERED"] != 1 {
		t.Fatalf("row=%+v", row)
	}
	window.events[1].request = respCommand("EVAL", "return 'different'", "0", operationID)
	row = aggregateWindow(window)
	if row.RedisErrors != 1 || row.RedisErrorCodes["NOSCRIPT_UNRESOLVED"] != 1 {
		t.Fatalf("mismatched source row=%+v", row)
	}
}

func TestBuildReportFailsClosedWithoutExactCommandWindows(t *testing.T) {
	p := &proxy{cfg: config{trainRun: "run", businessCommit: "b", evidenceHead: "e", h5Commit: "h", toolHead: "t"}}
	for _, command := range expectedBusinessCommands[:len(expectedBusinessCommands)-1] {
		p.windows = append(p.windows, &traceWindow{row: traceRow{
			Command: command, Status: "PASS", ActorCommit: 1, Receipts: 1, RedisOps: 1,
			RequestBytes: 10, ResponseBytes: 5, TotalBytes: 15,
			RedisCommands: map[string]int{"EVALSHA": 1}, PhysicalCategories: map[string]int{"actor_commit": 1},
		}})
	}
	report := p.buildReport()
	if report.Verdict != "FAIL" || report.Checks["exact_command_set"] {
		t.Fatalf("report=%+v", report)
	}
}
