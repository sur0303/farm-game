package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting/authority"
)

func TestLeaseProjectionReadinessRequiresRecentObservationAndLeaseBudget(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	clock := now
	runtime := &leaseProjectionRuntime{
		interval: 5 * time.Second, minimumRemaining: 15 * time.Second, waveTimeout: 10 * time.Second,
		now: func() time.Time { return clock },
		status: leaseProjectionStatus{
			KnownFresh: true, GlobalRevision: 8, ObservedShards: 2048,
			MinimumLeaseUntilMs: now.Add(30 * time.Second).UnixMilli(), LastSuccessAtMs: now.UnixMilli(),
		},
	}
	if !runtime.Ready() || !runtime.Status().KnownFresh {
		t.Fatal("fresh exact projection is not ready")
	}
	clock = now.Add(16 * time.Second)
	if runtime.Ready() || runtime.Status().FailureClass != "STALE_OR_INSUFFICIENT_DEADLINE" {
		t.Fatalf("insufficient deadline status=%+v", runtime.Status())
	}
	clock = now.Add(31 * time.Second)
	if runtime.Ready() {
		t.Fatal("stale observation remained ready")
	}
}

func TestRouteControllerHealthSeparatesLivenessFromProjectionReadiness(t *testing.T) {
	runtime := &leaseProjectionRuntime{interval: time.Second, minimumRemaining: time.Second, waveTimeout: time.Second, now: time.Now}
	server := newRouteControllerHealthServer(runtime, nil)
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/evidencez": http.StatusServiceUnavailable} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s status=%d want=%d body=%s", path, response.Code, want, response.Body.String())
		}
		if path == "/readyz" && (!strings.Contains(response.Body.String(), `"known_fresh":false`) || !strings.Contains(response.Body.String(), "STALE_OR_INSUFFICIENT_DEADLINE")) {
			t.Fatalf("ready body=%s", response.Body.String())
		}
	}
}

func TestRouteControllerHealthDisabledStaticProjectionStillWaitsForDynamicStartupRecovery(t *testing.T) {
	startupReady := false
	server := newRouteControllerHealthServer(nil, nil, func() bool { return startupReady })
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"mode":"disabled"`) || !strings.Contains(response.Body.String(), `"startup_ready":false`) {
		t.Fatalf("pre-recovery ready status=%d body=%s", response.Code, response.Body.String())
	}
	startupReady = true
	response = httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"startup_ready":true`) {
		t.Fatalf("post-recovery ready status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLeaseProjectionFailureClassesAreBounded(t *testing.T) {
	cases := map[error]string{
		authority.ErrLeaseProjectionStale:     "STALE_CALLBACK_REJECTED",
		authority.ErrLeaseProjectionTooClose:  "INSUFFICIENT_DEADLINE",
		authority.ErrSnapshotPublish:          "PUBLISH_PENDING",
		authority.ErrLeaseProjectionPending:   "PUBLISH_PENDING",
		errors.New("private endpoint detail"): "AUTHORITY_OBSERVATION_FAILED",
	}
	for err, want := range cases {
		if got := leaseProjectionFailureClass(err); got != want {
			t.Fatalf("err=%v class=%s want=%s", err, got, want)
		}
	}
}
