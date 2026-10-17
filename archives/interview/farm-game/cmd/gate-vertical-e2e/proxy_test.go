package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
)

// fakeUpstreamClient implements logicrpcpb.GatewayDownlinkServiceClient.
type fakeUpstreamClient struct {
	mu            sync.Mutex
	activateCalls int
}

func (f *fakeUpstreamClient) DeliverBatch(_ context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	return &logicrpcpb.DeliverBatchResponse{}, nil
}

func (f *fakeUpstreamClient) ActivateSubscription(_ context.Context, in *logicrpcpb.ActivateSubscriptionRequest, _ ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	f.mu.Lock()
	f.activateCalls++
	f.mu.Unlock()
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: in.GetReadyThroughVersion(),
	}, nil
}

func newProxyWithPlan(hold bool) (*DownlinkProxy, *fakeUpstreamClient) {
	up := &fakeUpstreamClient{}
	p := &DownlinkProxy{upstream: up}
	p.mu.Lock()
	p.plan = FaultPlan{HoldActivateResponse: hold}
	p.mu.Unlock()
	return p, up
}

func releaseActivate(t *testing.T, p *DownlinkProxy) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/admin/release-activate", nil)
	w := httptest.NewRecorder()
	p.handleReleaseActivate(w, req)
	return w.Code
}

func waitHeld(t *testing.T, p *DownlinkProxy) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !p.activateHeld.Load() {
		if time.Now().After(deadline) {
			t.Fatal("ActivateSubscription never reached held state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestActivateSubscriptionForward(t *testing.T) {
	p, up := newProxyWithPlan(false)
	resp, err := p.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{ReadyThroughVersion: 7})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if resp.GetReadyThroughVersion() != 7 {
		t.Fatalf("version = %d, want 7", resp.GetReadyThroughVersion())
	}
	up.mu.Lock()
	calls := up.activateCalls
	up.mu.Unlock()
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls)
	}
}

func TestActivateSubscriptionHoldRelease(t *testing.T) {
	p, _ := newProxyWithPlan(true)
	ctx := context.Background()
	result := make(chan *logicrpcpb.ActivateSubscriptionResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := p.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{ReadyThroughVersion: 9})
		if err != nil {
			errCh <- err
			return
		}
		result <- resp
	}()
	waitHeld(t, p)
	if code := releaseActivate(t, p); code != http.StatusOK {
		t.Fatalf("release status = %d, want 200", code)
	}
	select {
	case resp := <-result:
		if resp.GetReadyThroughVersion() != 9 {
			t.Fatalf("version = %d, want 9", resp.GetReadyThroughVersion())
		}
	case err := <-errCh:
		t.Fatalf("activate: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("ActivateSubscription did not return after release")
	}
	if p.activateHeld.Load() {
		t.Fatal("activateHeld still set after release")
	}
}

func TestActivateSubscriptionHoldCancel(t *testing.T) {
	p, _ := newProxyWithPlan(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{ReadyThroughVersion: 1})
		done <- err
	}()
	waitHeld(t, p)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ActivateSubscription did not return on cancel")
	}
	if p.activateHeld.Load() {
		t.Fatal("activateHeld still set after cancel")
	}
}

func TestActivateSubscriptionReleaseBeforeHold(t *testing.T) {
	p, _ := newProxyWithPlan(true)
	// Release before any hold must return 409 without panicking on a nil channel.
	if code := releaseActivate(t, p); code != http.StatusConflict {
		t.Fatalf("early release status = %d, want 409", code)
	}
	// Then a normal hold -> release cycle still works.
	ctx := context.Background()
	result := make(chan error, 1)
	go func() {
		_, err := p.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{ReadyThroughVersion: 5})
		result <- err
	}()
	waitHeld(t, p)
	if code := releaseActivate(t, p); code != http.StatusOK {
		t.Fatalf("release status = %d, want 200", code)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("activate after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("activate did not return after release")
	}
}

// TestActivateSubscriptionHoldReleaseNoRace hammers many hold->release cycles.
// The original Unlock-without-Lock bug panicked immediately; run with -race to
// also catch channel/marker data races.
func TestActivateSubscriptionHoldReleaseNoRace(t *testing.T) {
	p, up := newProxyWithPlan(true)
	for i := 0; i < 8; i++ {
		ctx := context.Background()
		errCh := make(chan error, 1)
		go func(v uint64) {
			_, err := p.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{ReadyThroughVersion: v})
			errCh <- err
		}(uint64(i + 1))
		waitHeld(t, p)
		if code := releaseActivate(t, p); code != http.StatusOK {
			t.Fatalf("round %d release status = %d", i, code)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("round %d activate: %v", i, err)
		}
		if p.activateHeld.Load() {
			t.Fatalf("round %d: held not cleared", i)
		}
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.activateCalls != 8 {
		t.Fatalf("upstream calls = %d, want 8", up.activateCalls)
	}
}
