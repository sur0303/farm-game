package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestClassifyRequestNormalizesLowercaseEvalshaAndExtractsSHA(t *testing.T) {
	sha := strings.Repeat("a", 40)
	frame := []byte(fmt.Sprintf("*2\r\n$7\r\nevalsha\r\n$40\r\n%s\r\n", sha))
	command, gotSHA := classifyRequest(frame)
	if command != "EVALSHA" || gotSHA != sha {
		t.Fatalf("classify lowercase evalsha = (%q, %q), want (EVALSHA, %q)", command, gotSHA, sha)
	}
}

func TestClassifyRequestNormalizesOrdinaryCommand(t *testing.T) {
	frame := []byte("*1\r\n$4\r\nping\r\n")
	command, sha := classifyRequest(frame)
	if command != "PING" || sha != "" {
		t.Fatalf("classify ping = (%q, %q), want (PING, empty)", command, sha)
	}
}
