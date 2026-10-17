package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"farmgame/internal/authoritycomposition"
	farmactor "farmgame/internal/farm/actor"
	farmdomain "farmgame/internal/farm/domain"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func TestFarmNotReadyIncludesFailClosedRouteDiagnostics(t *testing.T) {
	server := newHealthServer(":0", prometheus.NewRegistry(), func() bool { return false }, func() map[string]any {
		return map[string]any{"assignment_state": "RESYNCING", "assignment_last_error": "watch unavailable"}
	})
	recorder := httptest.NewRecorder()
	server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	runtime, _ := body["route_runtime"].(map[string]any)
	if body["status"] != "not_ready" || runtime["assignment_state"] != "RESYNCING" || runtime["assignment_last_error"] != "watch unavailable" {
		t.Fatalf("not-ready diagnostics=%v", body)
	}
}

func TestTheftPRFDeploymentConfigurationFailsClosed(t *testing.T) {
	getenv := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	for _, test := range []struct {
		name   string
		values map[string]string
	}{
		{name: "production missing", values: map[string]string{}},
		{name: "production short", values: map[string]string{"THEFT_PRF_HMAC_SECRET": "too-short"}},
		{name: "unknown mode", values: map[string]string{"THEFT_PRF_MODE": "local", "THEFT_PRF_HMAC_SECRET": strings.Repeat("x", 32)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := theftPRFFromConfig(getenv(test.values)); err == nil {
				t.Fatal("invalid production Theft PRF configuration was accepted")
			}
		})
	}
	production, err := theftPRFFromConfig(getenv(map[string]string{
		"THEFT_PRF_MODE": "production", "THEFT_PRF_HMAC_SECRET": strings.Repeat("p", 32),
	}))
	if err != nil || production == nil {
		t.Fatalf("production PRF=%v err=%v", production, err)
	}
	if _, err := production("attempt-production", farmactorTestGuard()); err != nil {
		t.Fatal(err)
	}
	demo, err := theftPRFFromConfig(getenv(map[string]string{"THEFT_PRF_MODE": "demo"}))
	if err != nil || demo == nil {
		t.Fatalf("explicit demo PRF=%v err=%v", demo, err)
	}
}

func TestFarmRemoteRequiresCompleteTuple(t *testing.T) {
	for name, value := range map[string]string{
		rediskey.RedisNamespaceEnvironmentEnv: "prod",
		rediskey.RedisNamespaceZoneEnv:        "z1",
		rediskey.RedisPhysicalProfileEnv:      "redis-physical-v1",
		rediskey.RedisAuthorityLayoutEnv:      "v1",
		rediskey.RedisWorkflowLayoutEnv:       "v1",
		rediskey.RedisWorkflowHashEnv:         "1",
		rediskey.RedisWorkflowBucketCountEnv:  "256",
	} {
		t.Setenv(name, value)
	}
	tuple, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch)
	if err != nil || tuple == nil {
		t.Fatalf("tuple=%v err=%v", tuple, err)
	}
	t.Setenv(rediskey.RedisWorkflowBucketCountEnv, "0")
	if _, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch); err == nil {
		t.Fatal("farm remote startup accepted a zero bucket tuple")
	}
}

func TestFarmRemoteNamespaceAndResultStoreNeverReadEnvironment(t *testing.T) {
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: "prod", NamespaceZone: "z1", PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	panicGetter := func(key string) string {
		reads++
		panic("remote Farm constructor read environment: " + key)
	}
	namespace, err := farmRedisNamespace(&tuple, panicGetter)
	if err != nil || namespace != tuple.Namespace() {
		t.Fatalf("namespace=%+v err=%v", namespace, err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	if publisher, err := newFarmResultPublisher(client, &tuple, panicGetter); err != nil || publisher == nil {
		t.Fatalf("publisher=%v err=%v", publisher, err)
	}
	if reads != 0 {
		t.Fatalf("remote Farm tuple environment reads=%d", reads)
	}
}

func TestFarmStaticNamespaceDefaultsRemain(t *testing.T) {
	reads := 0
	namespace, err := farmRedisNamespace(nil, func(string) string { reads++; return "" })
	if err != nil || namespace.Environment != "dev" || namespace.Zone != "z1" || reads != 2 {
		t.Fatalf("namespace=%+v reads=%d err=%v", namespace, reads, err)
	}
}

func farmactorTestGuard() farmdomain.GuardSnapshot { return farmdomain.GuardSnapshot{} }

func TestFarmProductionCompositionRegistersTypedParticipantServices(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 4)
	defer registry.Stop()
	participant := buildFarmParticipantServer(registry, nil, nil)
	server := grpc.NewServer()
	registerFarmParticipantServices(server, participant)
	services := server.GetServiceInfo()
	if _, ok := services["contracts.CoordinatorParticipantService"]; !ok {
		t.Fatal("CoordinatorParticipantService was not registered")
	}
	if _, ok := services["contracts.WorkerRecoveryIngress"]; !ok {
		t.Fatal("WorkerRecoveryIngress was not registered")
	}
}

type recordingFarmDirectShutdown struct {
	events *[]string
	alive  func() bool
	wait   bool
}

func (s recordingFarmDirectShutdown) Shutdown(ctx context.Context) error {
	if s.alive == nil || !s.alive() {
		return errors.New("runtime dependency fenced before Direct shutdown")
	}
	*s.events = append(*s.events, "direct")
	if s.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func TestFarmProductionShutdownDrainsDirectBeforeRuntimeFence(t *testing.T) {
	for _, test := range []struct {
		name      string
		wait      bool
		wantError bool
	}{
		{name: "drained"},
		{name: "bounded timeout", wait: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0, 2)
			registryAlive, participantAlive, redisAlive, routesAlive := true, true, true, true
			direct := recordingFarmDirectShutdown{events: &events, wait: test.wait, alive: func() bool {
				return registryAlive && participantAlive && redisAlive && routesAlive
			}}
			err := shutdownDirectBeforeFarmRuntime(direct, 10*time.Millisecond, func() {
				events = append(events, "runtime-fence")
				registryAlive, participantAlive, redisAlive, routesAlive = false, false, false, false
			})
			if (err != nil) != test.wantError {
				t.Fatalf("err=%v wantError=%v", err, test.wantError)
			}
			if len(events) != 2 || events[0] != "direct" || events[1] != "runtime-fence" {
				t.Fatalf("shutdown order=%v", events)
			}
		})
	}
}

func TestTheftProductionCompositionFailsClosedWhenDispatchPortMissing(t *testing.T) {
	service := farmingress.NewService(nil, nil)
	namespace, err := rediskey.NewNamespace("test", "farm-svr")
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	repositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureTheftIngress(service, repositories, nil); err == nil {
		t.Fatal("missing concrete dispatch port must fail composition")
	}
	if service.TheftIngressReady() {
		t.Fatal("THEFT ingress cannot be ready without Pair and dispatch dependencies")
	}
}
