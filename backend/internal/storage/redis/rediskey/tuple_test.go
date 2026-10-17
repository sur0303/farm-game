package rediskey

import (
	"errors"
	"strings"
	"testing"
)

func validKeyspaceTupleValues() map[string]string {
	return map[string]string{
		RedisNamespaceEnvironmentEnv: "prod",
		RedisNamespaceZoneEnv:        "z1",
		RedisPhysicalProfileEnv:      "redis-physical-v1",
		RedisAuthorityLayoutEnv:      "v1",
		RedisWorkflowLayoutEnv:       "v1",
		RedisWorkflowHashEnv:         "1",
		RedisWorkflowBucketCountEnv:  "256",
	}
}

func tupleEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestRedisTupleRequiresAllFields(t *testing.T) {
	values := validKeyspaceTupleValues()
	tuple, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	if tuple.Profile().Namespace.Environment != "prod" || tuple.Profile().WorkflowBucketCount != 256 {
		t.Fatalf("tuple=%+v", tuple.Profile())
	}
	for name := range values {
		t.Run(name, func(t *testing.T) {
			missing := validKeyspaceTupleValues()
			delete(missing, name)
			if _, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(missing)); !errors.Is(err, ErrInvalidKeyspaceTuple) {
				t.Fatalf("missing %s err=%v", name, err)
			}
			empty := validKeyspaceTupleValues()
			empty[name] = ""
			if _, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(empty)); !errors.Is(err, ErrInvalidKeyspaceTuple) {
				t.Fatalf("empty %s err=%v", name, err)
			}
		})
	}
}

func TestRedisTupleRejectsMalformedAndNoncanonicalValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		field string
		value string
	}{
		{"namespace uppercase", RedisNamespaceEnvironmentEnv, "Prod"},
		{"profile whitespace", RedisPhysicalProfileEnv, " redis-physical-v1"},
		{"layout malformed", RedisAuthorityLayoutEnv, "V1"},
		{"authority layout unsupported", RedisAuthorityLayoutEnv, "v2"},
		{"workflow layout unsupported", RedisWorkflowLayoutEnv, "v2"},
		{"hash noncanonical", RedisWorkflowHashEnv, "01"},
		{"hash unsupported", RedisWorkflowHashEnv, "2"},
		{"bucket zero", RedisWorkflowBucketCountEnv, "0"},
		{"bucket noncanonical", RedisWorkflowBucketCountEnv, "0256"},
		{"bucket not power of two", RedisWorkflowBucketCountEnv, "255"},
		{"bucket overflow", RedisWorkflowBucketCountEnv, "4294967296"},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := validKeyspaceTupleValues()
			values[test.field] = test.value
			if _, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values)); !errors.Is(err, ErrInvalidKeyspaceTuple) {
				t.Fatalf("value=%q err=%v", test.value, err)
			}
		})
	}
}

func TestRedisTupleFingerprintIsStableVersionedAndSecretFree(t *testing.T) {
	values := validKeyspaceTupleValues()
	values["FARM_REDIS_URL"] = "redis://user:do-not-leak@example.invalid/0"
	first, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := first.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(fingerprint, RedisKeyspaceFingerprintVersion+":") || len(fingerprint) != len(RedisKeyspaceFingerprintVersion)+1+sha256HexBytes {
		t.Fatalf("fingerprint shape=%q", fingerprint)
	}
	if strings.Contains(fingerprint, "do-not-leak") || strings.Contains(fingerprint, "example.invalid") {
		t.Fatal("fingerprint contains Redis connection material")
	}
	values["FARM_REDIS_URL"] = "redis://different:secret@example.invalid/1"
	second, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, _ := second.Fingerprint()
	if fingerprint != secondFingerprint {
		t.Fatalf("secret changed fingerprint: %q != %q", fingerprint, secondFingerprint)
	}
	values[RedisPhysicalProfileEnv] = "redis-physical-v2"
	different, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	differentFingerprint, _ := different.Fingerprint()
	if fingerprint == differentFingerprint {
		t.Fatal("different layout produced the same fingerprint")
	}
}

func TestAllRemoteRolesSameFingerprint(t *testing.T) {
	values := validKeyspaceTupleValues()
	roles := []string{"route-controller", "farm-gate", "player-svr", "farm-svr", "farm-worker"}
	want := ""
	for _, role := range roles {
		tuple, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values))
		if err != nil {
			t.Fatalf("%s parse: %v", role, err)
		}
		fingerprint, err := tuple.Fingerprint()
		if err != nil {
			t.Fatalf("%s fingerprint: %v", role, err)
		}
		if want == "" {
			want = fingerprint
		} else if fingerprint != want {
			t.Fatalf("%s fingerprint=%q want=%q", role, fingerprint, want)
		}
	}
}

func TestDifferentLayoutFailsStartup(t *testing.T) {
	for _, name := range []string{RedisAuthorityLayoutEnv, RedisWorkflowLayoutEnv} {
		values := validKeyspaceTupleValues()
		values[name] = "v2"
		if _, err := ParseKeyspaceTupleEnvironment(tupleEnvironment(values)); !errors.Is(err, ErrInvalidKeyspaceTuple) {
			t.Fatalf("%s=v2 err=%v", name, err)
		}
	}
}

const sha256HexBytes = 64
