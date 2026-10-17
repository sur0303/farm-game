package main

import (
	"encoding/base64"
	"errors"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"

	ticketstore "farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/gorilla/websocket"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "read timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestStaleCloseEvidenceRejectsReadTimeout(t *testing.T) {
	if evidence, closed := staleCloseEvidence(timeoutError{}); closed || evidence != "" {
		t.Fatalf("timeout classified as close: evidence=%q closed=%v", evidence, closed)
	}
}

func TestStaleCloseEvidenceAcceptsTransportClosure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "websocket close", err: &websocket.CloseError{Code: websocket.CloseAbnormalClosure}},
		{name: "EOF", err: io.EOF},
		{name: "closed connection", err: net.ErrClosed},
		{name: "connection reset", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence, closed := staleCloseEvidence(test.err)
			if !closed || evidence == "" {
				t.Fatalf("transport close not recognized: evidence=%q closed=%v", evidence, closed)
			}
		})
	}
}

func TestStaleCloseEvidenceRejectsUnexpectedError(t *testing.T) {
	if evidence, closed := staleCloseEvidence(errors.New("decode failed")); closed || evidence != "" {
		t.Fatalf("unexpected error classified as close: evidence=%q closed=%v", evidence, closed)
	}
}

func TestCleanupKeysRequiresPerRunTestPrefix(t *testing.T) {
	for _, prefix := range []string{"", "farm:v03:dualgate", "farm:test:dualgate:", "farm:test:other:run"} {
		if _, err := cleanupKeys(prefix, runEvidence{playerID: 7}, true); err == nil {
			t.Fatalf("cleanupKeys(%q) unexpectedly accepted unsafe prefix", prefix)
		}
	}
}

func TestCleanupKeysAreExactAndFenceScoped(t *testing.T) {
	playerID := uint64(700000000000001)
	random := make([]byte, ticketstore.DefaultTicketBytes)
	for index := range random {
		random[index] = byte(index + 1)
	}
	token := "v1." + strconv.FormatUint(playerID, 10) + "." + base64.RawURLEncoding.EncodeToString(random)
	keys, err := cleanupKeys("farm:test:dualgate:run-1", runEvidence{
		playerID: playerID, tickets: []string{token}, workflowIDs: []string{"plant-1"},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"farm:test:dualgate:run-1:player:700000000000001":                     true,
		"farm:test:dualgate:run-1:farm:700000000000001":                       true,
		"farm:test:dualgate:run-1:workflow:{workflow}:due":                    true,
		"farm:test:dualgate:run-1:workflow:{workflow}:record:plant-1":         true,
		rediskey.Session(playerID):                                            true,
		rediskey.FarmOnline(playerID):                                         true,
		rediskey.ConnectionTicketForPlayer(playerID, ticketstore.Hash(token)): true,
	}
	if len(keys) != len(want) {
		t.Fatalf("cleanup keys=%v, want %d exact keys", keys, len(want))
	}
	for _, key := range keys {
		if !want[key] {
			t.Fatalf("unexpected cleanup key %q", key)
		}
	}
}
