package ingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParticipantAdmissionLanesSerializeOneFarmAndNotOthers(t *testing.T) {
	lanes := newParticipantAdmissionLanes(4)
	releaseFirst, err := lanes.acquire(context.Background(), 1001)
	if err != nil {
		t.Fatal(err)
	}

	otherRelease, err := lanes.acquire(context.Background(), 1002)
	if err != nil {
		t.Fatalf("different farm must not be blocked: %v", err)
	}
	otherRelease()

	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := lanes.acquire(context.Background(), 1001)
		if acquireErr != nil {
			return
		}
		acquired <- release
	}()
	select {
	case <-acquired:
		t.Fatal("same farm acquired before prior holder released")
	case <-time.After(20 * time.Millisecond):
	}
	releaseFirst()
	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("same farm did not advance after release")
	}
}

func TestParticipantAdmissionLanesBoundAndCancel(t *testing.T) {
	lanes := newParticipantAdmissionLanes(1)
	release, err := lanes.acquire(context.Background(), 2001)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lanes.acquire(context.Background(), 2001); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("queue overflow=%v want ResourceExhausted", err)
	}
	release()

	cancellable := newParticipantAdmissionLanes(2)
	release, err = cancellable.acquire(context.Background(), 2002)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cancellable.acquire(ctx, 2002); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter=%v want context.Canceled", err)
	}
	release()
}
