package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"farmgame/internal/authoritycomposition"
	playeractor "farmgame/internal/player/actor"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func TestPlayerNotReadyIncludesFailClosedRouteDiagnostics(t *testing.T) {
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

func TestPlayerProductionCompositionRegistersTypedParticipantServices(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 4)
	defer registry.Stop()
	participant := buildPlayerParticipantServer(registry, nil, nil)
	server := grpc.NewServer()
	registerPlayerParticipantServices(server, participant)
	services := server.GetServiceInfo()
	if _, ok := services["contracts.CoordinatorParticipantService"]; !ok {
		t.Fatal("CoordinatorParticipantService was not registered")
	}
	if _, ok := services["contracts.WorkerRecoveryIngress"]; !ok {
		t.Fatal("WorkerRecoveryIngress was not registered")
	}
}

func TestPlayerRemoteRequiresCompleteTuple(t *testing.T) {
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
	t.Setenv(rediskey.RedisWorkflowHashEnv, "01")
	if _, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch); err == nil {
		t.Fatal("player remote startup accepted a noncanonical tuple")
	}
}

func TestPlayerRemoteNamespaceAndResultStoreNeverReadEnvironment(t *testing.T) {
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
		panic("remote Player constructor read environment: " + key)
	}
	namespace, err := playerRedisNamespace(&tuple, panicGetter)
	if err != nil || namespace != tuple.Namespace() {
		t.Fatalf("namespace=%+v err=%v", namespace, err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	if directory, err := newPlayerResultDirectory(client, &tuple, panicGetter); err != nil || directory == nil {
		t.Fatalf("directory=%v err=%v", directory, err)
	}
	if reads != 0 {
		t.Fatalf("remote Player tuple environment reads=%d", reads)
	}
}

func TestPlayerStaticNamespaceDefaultsRemain(t *testing.T) {
	reads := 0
	namespace, err := playerRedisNamespace(nil, func(string) string { reads++; return "" })
	if err != nil || namespace.Environment != "dev" || namespace.Zone != "z1" || reads != 2 {
		t.Fatalf("namespace=%+v reads=%d err=%v", namespace, reads, err)
	}
}
