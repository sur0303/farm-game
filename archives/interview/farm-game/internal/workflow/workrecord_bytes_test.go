package workflow

import (
	"encoding/json"
	"testing"

	"farmgame/internal/workidentity"
)

func TestEncodeWorkRecordBytesStayInSagaRange(t *testing.T) {
	record := &WorkRecord{
		Address: workidentity.WorkAddress{
			WorkID:   "sid1_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WorkType: "PLANT", PlacementKind: workidentity.IndependentWorkBucket,
			BucketVersion: 1, BucketID: 7,
		},
		WorkVersion: 3, State: WorkStateRunning,
		CurrentStep: 1, NextDueAtMs: 1_700_000_000_000,
		Payload: bytesOf(1800),
		Steps: []StepRecord{
			{Name: "RESERVE_SEED", Payload: bytesOf(400), Result: bytesOf(300)},
			{Name: "COMMIT_PLANT", Payload: bytesOf(1200), Result: bytesOf(800)},
			{Name: "COMMIT_SEED_RESERVATION", Payload: bytesOf(200), Result: bytesOf(120)},
			{Name: "RELEASE_SEED_RESERVATION", Payload: bytesOf(200)},
		},
	}
	data, err := encodeWorkRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatal("work record encoding is not JSON")
	}
	if len(data) < 4_000 {
		t.Fatalf("plant saga fixture too small: %d bytes", len(data))
	}
	t.Logf("plant WorkRecord JSON bytes=%d (online saga rewrite size)", len(data))
}

func bytesOf(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte('a' + (i % 26))
	}
	return buf
}
