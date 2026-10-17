package main

import (
	"testing"
	"time"
)

func TestSummarizeAvailabilityAndRoleChange(t *testing.T) {
	start := time.Unix(1_800_000_000, 0).UTC()
	loaded := manifest{
		Attempts: []attempt{
			{State: "ACKNOWLEDGED", StartedAt: start, ReturnedAt: start.Add(10 * time.Millisecond), WaitReplicas: 1, WaitAcks: 1},
			{State: "UNKNOWN", StartedAt: start.Add(time.Second), ReturnedAt: start.Add(1750 * time.Millisecond)},
			{State: "UNKNOWN", StartedAt: start.Add(2 * time.Second), ReturnedAt: start.Add(2750 * time.Millisecond)},
			{State: "ACKNOWLEDGED", StartedAt: start.Add(3 * time.Second), ReturnedAt: start.Add(3100 * time.Millisecond), WaitReplicas: 1},
		},
		Roles: []roleSample{
			{MasterReplID: "before"},
			{Error: "TIMEOUT"},
			{MasterReplID: "after"},
			{MasterReplID: "after"},
		},
	}
	summary := summarizeAvailability(loaded, "primary")
	if summary.Attempts != 4 || summary.Acknowledged != 2 || summary.Unknown != 2 ||
		summary.ReplicaAcknowledged != 1 {
		t.Fatalf("counts=%+v", summary)
	}
	if summary.MaxObservedUnavailableMS != 2100 {
		t.Fatalf("max unavailable=%d", summary.MaxObservedUnavailableMS)
	}
	if summary.RoleChanges != 1 {
		t.Fatalf("role changes=%d", summary.RoleChanges)
	}
}

func TestMarkerParsingAndKeys(t *testing.T) {
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	sequence, gotHash, err := parseMarker("17|" + hash)
	if err != nil || sequence != 17 || gotHash != hash {
		t.Fatalf("marker seq=%d hash=%q err=%v", sequence, gotHash, err)
	}
	if _, _, err := parseMarker("0|" + hash); err == nil {
		t.Fatal("zero sequence accepted")
	}
	hashKey, sequenceKey := probeKeys("abc")
	if hashKey != "farm:test:ha:{abc}:attempts" ||
		sequenceKey != "farm:test:ha:{abc}:sequence" {
		t.Fatalf("keys=%q %q", hashKey, sequenceKey)
	}
}

func TestClassifyErrorDoesNotPersistDetails(t *testing.T) {
	if got := classifyError(contextDeadlineError{}); got != "OTHER" {
		t.Fatalf("custom error class=%q", got)
	}
}

type contextDeadlineError struct{}

func (contextDeadlineError) Error() string { return "secret-bearing failure" }
