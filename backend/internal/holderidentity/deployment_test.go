package holderidentity

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeploymentIdentityValidation(t *testing.T) {
	if err := (DeploymentIdentity{}).ValidateOptional(); err != nil {
		t.Fatalf("zero compatibility identity: %v", err)
	}
	exact := DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "statefulset-uid", Ordinal: 3}
	if !exact.Exact() || exact.IsZero() || exact.ValidateExact() != nil || exact.ValidateOptional() != nil {
		t.Fatalf("exact identity rejected: %+v", exact)
	}
	for name, identity := range map[string]DeploymentIdentity{
		"pod-only":       {PodUID: "pod-uid"},
		"workload-only":  {WorkloadUID: "workload-uid"},
		"partial":        {PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: -1},
		"leading-space":  {PodUID: " pod-uid", WorkloadUID: "workload-uid"},
		"internal-space": {PodUID: "pod uid", WorkloadUID: "workload-uid"},
		"control":        {PodUID: "pod\x00uid", WorkloadUID: "workload-uid"},
		"malformed-utf8": {PodUID: string([]byte{0xff}), WorkloadUID: "workload-uid"},
		"unicode-space":  {PodUID: "pod\u00a0uid", WorkloadUID: "workload-uid"},
		"unicode-letter": {PodUID: "pöd-uid", WorkloadUID: "workload-uid"},
		"oversized":      {PodUID: strings.Repeat("p", MaxIdentityBytes+1), WorkloadUID: "workload-uid"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := identity.ValidateOptional(); !errors.Is(err, ErrInvalidDeploymentIdentity) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestParseEnvironmentAllAbsentCompleteAndMalformed(t *testing.T) {
	getenv := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	zero, err := ParseEnvironment(getenv(nil))
	if err != nil || !zero.IsZero() {
		t.Fatalf("zero=%+v err=%v", zero, err)
	}
	identity, err := ParseEnvironment(getenv(map[string]string{
		IdentityModeEnvironment:     IdentityModeKubernetes,
		"ACTOR_HOLDER_POD_UID":      "pod-uid",
		"ACTOR_HOLDER_WORKLOAD_UID": "workload-uid",
		"ACTOR_HOLDER_ORDINAL":      "12",
	}))
	if err != nil || identity.Ordinal != 12 || !identity.Exact() {
		t.Fatalf("identity=%+v err=%v", identity, err)
	}
	for name, values := range map[string]map[string]string{
		"partial":      {"ACTOR_HOLDER_POD_UID": "pod-uid"},
		"k8s-missing":  {IdentityModeEnvironment: IdentityModeKubernetes},
		"static-tuple": {IdentityModeEnvironment: IdentityModeStatic, "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "0"},
		"absent-mode":  {"ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "0"},
		"case-alias":   {IdentityModeEnvironment: "KUBERNETES-PROCESS-V1", "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "0"},
		"space-alias":  {IdentityModeEnvironment: " kubernetes-process-v1 ", "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "0"},
		"bad-mode":     {IdentityModeEnvironment: "surprise"},
		"leading-zero": {IdentityModeEnvironment: IdentityModeKubernetes, "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "01"},
		"negative":     {IdentityModeEnvironment: IdentityModeKubernetes, "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "-1"},
		"plus":         {IdentityModeEnvironment: IdentityModeKubernetes, "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "+1"},
		"overflow":     {IdentityModeEnvironment: IdentityModeKubernetes, "ACTOR_HOLDER_POD_UID": "pod-uid", "ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "2147483648"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseEnvironment(getenv(values)); !errors.Is(err, ErrInvalidDeploymentIdentity) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestRemoteWatchComposeRequiresExactClaimsAndForbidsConfiguredBoot(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "deploy", "docker-compose.remote-watch.example.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		"ACTOR_HOLDER_IDENTITY_MODE: \"kubernetes-process-v1\"",
		"PLAYER_HOLDER_POD_UID", "PLAYER_HOLDER_WORKLOAD_UID", "PLAYER_HOLDER_ORDINAL",
		"FARM_HOLDER_POD_UID", "FARM_HOLDER_WORKLOAD_UID", "FARM_HOLDER_ORDINAL",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("remote-watch Compose missing %q", required)
		}
	}
	for _, forbidden := range []string{"PLAYER_HOLDER_BOOT_ID", "FARM_HOLDER_BOOT_ID", "ACTOR_HOLDER_BOOT_ID:"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("remote-watch Compose retains forbidden configured Boot %q", forbidden)
		}
	}
}

func TestRandomBootID(t *testing.T) {
	first, err := randomBootID(bytes.NewReader(bytes.Repeat([]byte{0xab}, bootEntropyBytes)))
	if err != nil || first != strings.Repeat("ab", bootEntropyBytes) {
		t.Fatalf("boot=%q err=%v", first, err)
	}
	if _, err := randomBootID(io.LimitReader(bytes.NewReader([]byte{1}), 1)); err == nil {
		t.Fatal("short entropy source accepted")
	}
	actual, err := RandomBootID()
	if err != nil || len(actual) != bootEntropyBytes*2 {
		t.Fatalf("actual=%q err=%v", actual, err)
	}
	next, err := RandomBootID()
	if err != nil || next == actual {
		t.Fatalf("two process starts reused boot first=%q next=%q err=%v", actual, next, err)
	}
}
