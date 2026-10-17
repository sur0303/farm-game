package main

import (
	"testing"

	"farmgame/internal/testkit/capdriver"
)

func gateFile() fileConfig {
	return fileConfig{
		SchemaVersion: "cap-l2-driver-config.v1", Listen: "127.0.0.1:19001",
		Service: capdriver.ServiceGate, Profile: "gate-qualification", MaximumInflight: 4,
		Adapter: adapterGateSession,
		Gate: &gateConfig{
			HTTPURL: "http://127.0.0.1:8080", WSURL: "ws://127.0.0.1:8080/ws",
			LoadSecretEnv: "CAP_DRIVER_TEST_SECRET", PlayerBase: 900000000,
		},
	}
}

func TestDesignBlockerStaysUnbound(t *testing.T) {
	file := fileConfig{
		SchemaVersion: "cap-l2-driver-config.v1", Listen: "127.0.0.1:19001",
		Service: capdriver.ServicePlayer, Profile: "l2-player-r2", MaximumInflight: 4,
		Adapter: adapterDesignBlocker,
	}
	adapter, err := buildAdapter(file)
	if err != nil || adapter != nil {
		t.Fatalf("adapter=%v err=%v", adapter, err)
	}
	file.Gate = gateFile().Gate
	if _, err := buildAdapter(file); err == nil {
		t.Fatal("a blocked service must not carry a deployment")
	}
}

func TestUnknownAdapterFailsClosed(t *testing.T) {
	file := gateFile()
	file.Adapter = "player_session_v1"
	if _, err := buildAdapter(file); err == nil {
		t.Fatal("an unimplemented adapter must fail closed")
	}
}

func TestGateAdapterRequiresItsServiceDeploymentAndSecret(t *testing.T) {
	t.Setenv("CAP_DRIVER_TEST_SECRET", "secret")
	adapter, err := buildAdapter(gateFile())
	if err != nil || adapter == nil {
		t.Fatalf("adapter=%v err=%v", adapter, err)
	}

	wrongService := gateFile()
	wrongService.Service = capdriver.ServiceFarm
	if _, err := buildAdapter(wrongService); err == nil {
		t.Fatal("the gate adapter must refuse another service")
	}

	noDeployment := gateFile()
	noDeployment.Gate = nil
	if _, err := buildAdapter(noDeployment); err == nil {
		t.Fatal("the gate adapter must require a deployment")
	}

	remote := gateFile()
	remote.Gate.HTTPURL = "http://gate.example.com:8080"
	if _, err := buildAdapter(remote); err == nil {
		t.Fatal("the gate adapter must refuse a non-loopback deployment")
	}

	t.Setenv("CAP_DRIVER_TEST_SECRET", "")
	if _, err := buildAdapter(gateFile()); err == nil {
		t.Fatal("the gate adapter must refuse an empty secret")
	}
}

func TestValidateLoopback(t *testing.T) {
	for _, value := range []string{"127.0.0.1:19001", "[::1]:19001"} {
		if err := validateLoopback(value); err != nil {
			t.Fatalf("%s rejected: %v", value, err)
		}
	}
	for _, value := range []string{":19001", "0.0.0.0:19001", "192.0.2.1:19001", "bad"} {
		if err := validateLoopback(value); err == nil {
			t.Fatalf("%s accepted", value)
		}
	}
}
