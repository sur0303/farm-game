package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"farmgame/internal/authoritycomposition"
	"farmgame/internal/routecontrol/gateverification"
)

type remoteComposeDocument struct {
	Services map[string]struct {
		Environment map[string]any `json:"environment"`
	} `json:"services"`
}

func remoteComposeValues() map[string]string {
	return map[string]string{
		"ACTOR_ROUTE_REALM": "classic-farm", "FARM_REDIS_URL": "redis://127.0.0.1:6379/0",
		"PERMIT_SECRET": "compose-test-placeholder", "WSS_ENDPOINT": "wss://farm.compose.test/ws",
		"ROUTE_CONTROL_CONTROLLER_ID": "controller.test",
		"ROUTE_CONTROL_SERVER_NAME":   "route-controller.test", "ROUTE_CONTROL_SERVER_IDENTITY": "route-controller.test",
		"ROUTE_CONTROLLER_BOOT": "boot-compose", "ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES": "route-client.test",
		"ROUTE_CONTROLLER_PLAN_READ_IDENTITIES": "plan-reader.test", "ROUTE_CONTROLLER_OPERATOR_IDENTITIES": "operator.test",
		"HOLDER_CONTROL_CONTROLLER_IDENTITY": "holder-operator.test",
		"GATE_VERIFIER_SERVER_NAME":          "gate-verifier.test", "GATE_VERIFIER_SERVER_IDENTITY": "gate-verifier.test",
		"GATE_VERIFIER_CONTROLLER_IDENTITY": "gate-verifier-controller.test",
		"PLAYER_HOLDER_ID":                  "player-a", "PLAYER_HOLDER_POD_UID": "player-pod-uid-a",
		"PLAYER_HOLDER_WORKLOAD_UID": "player-workload-uid", "PLAYER_HOLDER_ORDINAL": "0",
		"PLAYER_LOGIC_ENDPOINT": "player-svr:9100", "PLAYER_HOLDER_ENDPOINT": "player-svr:19100",
		"PLAYER_HOLDER_TLS_DNS_IDENTITY": "player-holder.test",
		"FARM_HOLDER_ID":                 "farm-a", "FARM_HOLDER_POD_UID": "farm-pod-uid-a",
		"FARM_HOLDER_WORKLOAD_UID": "farm-workload-uid", "FARM_HOLDER_ORDINAL": "0",
		"FARM_LOGIC_ENDPOINT": "farm-svr:9090", "FARM_HOLDER_ENDPOINT": "farm-svr:19101",
		"FARM_HOLDER_TLS_DNS_IDENTITY": "farm-holder.test",
		"REDIS_NAMESPACE_ENVIRONMENT":  "prod-compose", "REDIS_NAMESPACE_ZONE": "zone3",
		"REDIS_PHYSICAL_PROFILE_VERSION": "redis-compose-v3", "REDIS_AUTHORITY_LAYOUT_VERSION": "v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION": "v1", "REDIS_WORKFLOW_HASH_VERSION": "1",
		"REDIS_WORKFLOW_BUCKET_COUNT": "512",
	}
}

func renderRemoteCompose(t *testing.T, values map[string]string, retiredBootLimit, planTTL *string) remoteComposeDocument {
	t.Helper()
	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
	command := exec.Command("docker", "compose",
		"-f", filepath.Join(repositoryRoot, "deploy", "docker-compose.yml"),
		"-f", filepath.Join(repositoryRoot, "deploy", "docker-compose.remote-watch.example.yml"),
		"config", "--format", "json")
	command.Env = make([]string, 0, len(os.Environ())+len(values)+2)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "ROUTE_CONTROL_RETIRED_BOOT_LIMIT=") && !strings.HasPrefix(entry, "ROUTE_CONTROLLER_PLAN_TTL=") {
			command.Env = append(command.Env, entry)
		}
	}
	for name, value := range values {
		command.Env = append(command.Env, name+"="+value)
	}
	if retiredBootLimit != nil {
		command.Env = append(command.Env, "ROUTE_CONTROL_RETIRED_BOOT_LIMIT="+*retiredBootLimit)
	}
	if planTTL != nil {
		command.Env = append(command.Env, "ROUTE_CONTROLLER_PLAN_TTL="+*planTTL)
	}
	rendered, err := command.Output()
	if errors.Is(err, exec.ErrNotFound) {
		t.Skip("docker compose is unavailable")
	}
	if err != nil {
		t.Fatalf("render base+remote Compose: %v", err)
	}
	var document remoteComposeDocument
	if err := json.Unmarshal(rendered, &document); err != nil {
		t.Fatalf("decode rendered Compose: %v", err)
	}
	return document
}

func TestRemoteComposeRetiredBootLimitPreservesExplicitEmpty(t *testing.T) {
	values := remoteComposeValues()
	empty := ""
	for _, test := range []struct {
		name             string
		retiredBootLimit *string
		want             string
		wantParserReject bool
	}{
		{name: "unset defaults to eight", want: "8"},
		{name: "explicit empty remains empty", retiredBootLimit: &empty, want: "", wantParserReject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			document := renderRemoteCompose(t, values, test.retiredBootLimit, nil)
			for _, serviceName := range []string{"farm-gate", "player-svr", "farm-svr", "farm-worker"} {
				got, ok := document.Services[serviceName].Environment["ROUTE_CONTROL_RETIRED_BOOT_LIMIT"].(string)
				if !ok || got != test.want {
					t.Fatalf("%s ROUTE_CONTROL_RETIRED_BOOT_LIMIT=%v want=%q", serviceName, document.Services[serviceName].Environment["ROUTE_CONTROL_RETIRED_BOOT_LIMIT"], test.want)
				}
			}
			if test.wantParserReject {
				t.Setenv("ROUTE_CONTROL_RETIRED_BOOT_LIMIT", test.want)
				client, err := authoritycomposition.NewRemoteSnapshotClientFromEnvironment(context.Background())
				if client != nil || err == nil || !strings.Contains(err.Error(), "ROUTE_CONTROL_RETIRED_BOOT_LIMIT") {
					t.Fatalf("production parser accepted explicit empty value: client=%v err=%v", client, err)
				}
			}
		})
	}
}

func TestRemoteComposePlanTTLPreservesExplicitEmpty(t *testing.T) {
	values := remoteComposeValues()
	empty := ""
	short := "120s"
	for _, test := range []struct {
		name             string
		planTTL          *string
		want             string
		wantParserReject bool
	}{
		{name: "unset defaults to 24h", want: "24h"},
		{name: "explicit empty remains empty", planTTL: &empty, want: "", wantParserReject: true},
		{name: "RT2Q can use 120s", planTTL: &short, want: "120s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			document := renderRemoteCompose(t, values, nil, test.planTTL)
			got, ok := document.Services["route-controller"].Environment["ROUTE_CONTROLLER_PLAN_TTL"].(string)
			if !ok || got != test.want {
				t.Fatalf("route-controller ROUTE_CONTROLLER_PLAN_TTL=%v want=%q", document.Services["route-controller"].Environment["ROUTE_CONTROLLER_PLAN_TTL"], test.want)
			}
			if test.wantParserReject {
				t.Setenv("ROUTE_CONTROLLER_PLAN_TTL", test.want)
				if cfg, err := parseConfig(nil); err == nil || !strings.Contains(err.Error(), "ROUTE_CONTROLLER_PLAN_TTL") {
					t.Fatalf("production parser accepted explicit empty value: ttl=%v err=%v", cfg.planTTL, err)
				}
			}
		})
	}
}

func TestRemoteComposePinsValidatedDeadlineRefreshLeaseProfile(t *testing.T) {
	document := renderRemoteCompose(t, remoteComposeValues(), nil, nil)
	controller := document.Services["route-controller"].Environment
	if controller["ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL"] != "1s" || controller["ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD"] != "30s" || controller["ROUTE_CONTROLLER_METRICS_LISTEN"] != "0.0.0.0:9090" {
		t.Fatalf("route-controller deadline refresh profile=%v", controller)
	}
	for _, serviceName := range []string{"player-svr", "farm-svr"} {
		environment := document.Services[serviceName].Environment
		if environment["ACTOR_AUTHORITY_LEASE_TTL"] != "60s" || environment["ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN"] != "10s" || environment["ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL"] != "40s" {
			t.Fatalf("%s lease profile=%v", serviceName, environment)
		}
	}
}

func TestRemoteComposeRendersSameTupleForEveryRole(t *testing.T) {
	values := remoteComposeValues()
	document := renderRemoteCompose(t, values, nil, nil)
	tupleNames := []string{
		"REDIS_NAMESPACE_ENVIRONMENT", "REDIS_NAMESPACE_ZONE", "REDIS_PHYSICAL_PROFILE_VERSION",
		"REDIS_AUTHORITY_LAYOUT_VERSION", "REDIS_WORKFLOW_LAYOUT_VERSION",
		"REDIS_WORKFLOW_HASH_VERSION", "REDIS_WORKFLOW_BUCKET_COUNT",
	}
	for _, serviceName := range []string{"route-controller", "farm-gate", "player-svr", "farm-svr", "farm-worker"} {
		service, found := document.Services[serviceName]
		if !found {
			t.Fatalf("rendered Compose missing %s", serviceName)
		}
		for _, name := range tupleNames {
			if got, ok := service.Environment[name].(string); !ok || got != values[name] {
				t.Fatalf("%s %s=%v want=%q", serviceName, name, service.Environment[name], values[name])
			}
		}
	}
	for _, assertion := range []struct {
		service string
		logic   string
		control string
	}{
		{"player-svr", "PLAYER_LOGIC_ENDPOINT", "PLAYER_HOLDER_ENDPOINT"},
		{"farm-svr", "FARM_LOGIC_ENDPOINT", "FARM_HOLDER_ENDPOINT"},
	} {
		environment := document.Services[assertion.service].Environment
		if environment[assertion.logic] != values[assertion.logic] || environment[assertion.control] != values[assertion.control] ||
			environment[assertion.logic] == environment[assertion.control] {
			t.Fatalf("%s did not render distinct logic/control endpoints", assertion.service)
		}
	}
	controllerEnvironment := document.Services["route-controller"].Environment
	for name, want := range map[string]string{
		"ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CERT": "/run/farm/gate-verifier/client.crt",
		"ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_KEY":  "/run/farm/gate-verifier/client.key",
		"ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CA":   "/run/farm/gate-verifier/server-ca.crt",
	} {
		if got := controllerEnvironment[name]; got != want {
			t.Fatalf("route-controller %s=%v want=%q", name, got, want)
		}
	}
	var peers []gateverification.Peer
	peersRaw, ok := controllerEnvironment["ROUTE_CONTROLLER_GATE_VERIFIER_PEERS"].(string)
	if !ok || json.Unmarshal([]byte(peersRaw), &peers) != nil || len(peers) != 1 ||
		peers[0].Name != "farm-gate-1" || peers[0].Address != "farm-gate:19091" ||
		peers[0].ServerName != values["GATE_VERIFIER_SERVER_NAME"] || peers[0].ExpectedIdentity != values["GATE_VERIFIER_SERVER_IDENTITY"] {
		t.Fatalf("route-controller exact Gate peer list=%v", controllerEnvironment["ROUTE_CONTROLLER_GATE_VERIFIER_PEERS"])
	}
	gateEnvironment := document.Services["farm-gate"].Environment
	for name, want := range map[string]string{
		"GATE_VERIFIER_LISTEN":                  ":19091",
		"GATE_VERIFIER_TLS_CERT":                "/run/farm/gate-verifier/server.crt",
		"GATE_VERIFIER_TLS_KEY":                 "/run/farm/gate-verifier/server.key",
		"GATE_VERIFIER_CLIENT_CA":               "/run/farm/gate-verifier/controller-ca.crt",
		"GATE_VERIFIER_ALLOWED_CLIENT_IDENTITY": values["GATE_VERIFIER_CONTROLLER_IDENTITY"],
	} {
		if got := gateEnvironment[name]; got != want {
			t.Fatalf("farm-gate %s=%v want=%q", name, got, want)
		}
	}
}
