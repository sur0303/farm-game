package gatewayapp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/socialprofile"
)

func TestDemoVerifierLoadIdentityIsExplicitAndSigned(t *testing.T) {
	secret := []byte("load-test-only")
	playerID := uint64(900001)
	id := strconv.FormatUint(playerID, 10)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(id))
	token := "load:" + id + ":" + hex.EncodeToString(mac.Sum(nil))
	verifier := &demoVerifier{token: "normal", playerID: 1, loadTestSecret: secret}
	if got, err := verifier.Verify(context.Background(), token); err != nil || got != playerID {
		t.Fatalf("signed load token=(%d,%v)", got, err)
	}
	if _, err := verifier.Verify(context.Background(), "load:"+id+":00"); err == nil {
		t.Fatal("forged load token succeeded")
	}
	if _, err := (&demoVerifier{token: "normal", playerID: 1}).Verify(context.Background(), token); err == nil {
		t.Fatal("load token succeeded while feature disabled")
	}
}

func TestDemoVerifierSupportsTwoExplicitDevelopmentIdentities(t *testing.T) {
	verifier := &demoVerifier{
		token:             "token-a",
		playerID:          10001,
		secondaryToken:    "token-b",
		secondaryPlayerID: 10002,
	}

	tests := []struct {
		token string
		want  uint64
		ok    bool
	}{
		{token: "token-a", want: 10001, ok: true},
		{token: "token-b", want: 10002, ok: true},
		{token: "unknown", ok: false},
	}
	for _, test := range tests {
		got, err := verifier.Verify(context.Background(), test.token)
		if test.ok && (err != nil || got != test.want) {
			t.Fatalf("Verify(%q)=(%d,%v), want (%d,nil)", test.token, got, err, test.want)
		}
		if !test.ok && err == nil {
			t.Fatalf("Verify(%q) unexpectedly succeeded with player %d", test.token, got)
		}
	}
}

func TestDemoVerifierDoesNotEnableAnUnconfiguredSecondaryIdentity(t *testing.T) {
	verifier := &demoVerifier{token: "token-a", playerID: 10001}
	if _, err := verifier.Verify(context.Background(), "token-b"); err == nil {
		t.Fatal("unconfigured secondary token unexpectedly succeeded")
	}
}

func TestParseAppProfileRejectsUnknownValue(t *testing.T) {
	for _, value := range []string{"production", "Showcase-prod", "dev"} {
		if _, err := parseAppProfile(value); err == nil {
			t.Fatalf("parseAppProfile(%q) unexpectedly succeeded", value)
		}
	}
	if got, err := parseAppProfile(""); err != nil || got != appProfileLocal {
		t.Fatalf("empty profile=(%q,%v), want local", got, err)
	}
}

func TestShowcaseRejectsUnsafeEnvironmentAndDefaults(t *testing.T) {
	t.Setenv("FARM_APP_PROFILE", "showcase")
	t.Setenv("WSS_ENDPOINT", "wss://farm.example/ws")
	t.Setenv("HTTP_SESSION_COOKIE_SECURE", "1")
	t.Setenv("DEMO_PLATFORM_TOKEN", "demo-platform-token")
	if err := loadConfig().validateProfile(); err == nil {
		t.Fatal("showcase accepted DEMO_PLATFORM_TOKEN")
	}

	t.Setenv("DEMO_PLATFORM_TOKEN", "")
	t.Setenv("GATE_E2E_COMMAND_RESULT_FAULTS", "1")
	if err := loadConfig().validateProfile(); err == nil {
		t.Fatal("showcase accepted public fault endpoint")
	}

	t.Setenv("GATE_E2E_COMMAND_RESULT_FAULTS", "")
	t.Setenv("MOCK_FRIEND_PROVIDER", "mock")
	if err := loadConfig().validateProfile(); err == nil {
		t.Fatal("showcase accepted mock friend provider")
	}
}

func TestShowcaseHasNoDevSessionRoutes(t *testing.T) {
	mux := http.NewServeMux()
	registerTestAuthRoutes(mux, appProfileShowcase, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("showcase test auth handler was called")
	}))
	for _, path := range []string{"/v1/dev/session", "/api/v1/dev/session", "/v1/auth/login", "/api/v1/auth/login"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("showcase route %s status=%d, want 404", path, response.Code)
		}
	}

	mux = http.NewServeMux()
	registerTestAuthRoutes(mux, appProfileTest, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/dev/session", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("test dev session status=%d, want 204", response.Code)
	}
}

func TestTestProfileKeepsExplicitDevelopmentIdentityDefaults(t *testing.T) {
	t.Setenv("FARM_APP_PROFILE", "test")
	t.Setenv("DEMO_PLATFORM_TOKEN", "")
	t.Setenv("DEMO_PLAYER_ID", "")
	cfg := loadConfig()
	if cfg.demoToken != "demo-platform-token" || cfg.demoPlayerID != 100001 {
		t.Fatalf("test profile defaults=(%q,%d), want demo identity", cfg.demoToken, cfg.demoPlayerID)
	}
}

func TestLoadConfigSocialProductDefaultsCannotDrift(t *testing.T) {
	t.Setenv("FRIEND_LIMIT", "")
	t.Setenv("SHARE_TOKEN_TTL", "")
	cfg := loadConfig()
	profile := socialprofile.Current()
	if cfg.friendLimit != profile.EffectiveMaxFriends || cfg.shareTokenTTL != profile.ShareTokenTTL {
		t.Fatalf("gateway social defaults=(%d,%s), profile=(%d,%s)", cfg.friendLimit, cfg.shareTokenTTL, profile.EffectiveMaxFriends, profile.ShareTokenTTL)
	}
}

func TestLoadConfigParsesGateControlKickTimeout(t *testing.T) {
	t.Setenv("GATE_CONTROL_KICK_TIMEOUT", "2750ms")
	if got := loadConfig().gateControlKickTimeout; got != 2750*time.Millisecond {
		t.Fatalf("gate control kick timeout=%s, want 2.75s", got)
	}
}

func TestLeaseRenewalBatchConfigWiringAndSafety(t *testing.T) {
	t.Setenv("GATE_ROUTE_LEASE_TTL", "30s")
	t.Setenv("GATE_ROUTE_LEASE_RENEW_INTERVAL", "7s")
	t.Setenv("GATE_ROUTE_LEASE_BUCKETS", "32")
	t.Setenv("GATE_ROUTE_LEASE_MAX_JITTER", "100ms")
	t.Setenv("GATE_ROUTE_LEASE_BATCH_MAX_ITEMS", "17")
	t.Setenv("GATE_ROUTE_LEASE_BATCH_MAX_BYTES", "8192")
	t.Setenv("GATE_ROUTE_LEASE_MAX_DISPATCH_DURATION", "2s")
	t.Setenv("GATE_ROUTE_LEASE_TRANSIENT_BACKOFF", "300ms")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_TTL", "20s")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_RENEW_INTERVAL", "5s")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_BUCKETS", "16")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_MAX_JITTER", "100ms")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_BATCH_MAX_ITEMS", "19")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_BATCH_MAX_BYTES", "12288")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_RENEW_TIMEOUT", "1500ms")
	t.Setenv("GATE_SUBSCRIPTION_LEASE_TRANSIENT_BACKOFF", "400ms")
	t.Setenv("GATE_LEASE_SCHEDULER_TICK", "5ms")
	cfg := loadConfig()
	renewal, err := cfg.leaseRenewalConfig()
	if err != nil {
		t.Fatal(err)
	}
	if renewal.SessionRoute.Batch.MaxItems != 17 || renewal.SessionRoute.Batch.MaxBytes != 8192 ||
		renewal.SessionRoute.Batch.BucketCount != 32 || renewal.SessionRoute.Batch.Interval != 7*time.Second ||
		renewal.Subscription.Batch.MaxItems != 19 || renewal.Subscription.Batch.MaxBytes != 12288 ||
		renewal.Subscription.Batch.BucketCount != 16 || renewal.Subscription.Batch.MaxDispatchDuration != 1500*time.Millisecond {
		t.Fatalf("lease renewal config not explicitly wired: %+v", renewal)
	}

	t.Setenv("GATE_ROUTE_LEASE_RENEW_INTERVAL", "28s")
	if _, err := loadConfig().leaseRenewalConfig(); err == nil {
		t.Fatal("unsafe interval + dispatch window was accepted")
	}
	t.Setenv("GATE_ROUTE_LEASE_RENEW_INTERVAL", "7s")
	t.Setenv("GATE_ROUTE_LEASE_BATCH_MAX_ITEMS", "0")
	if _, err := loadConfig().leaseRenewalConfig(); err == nil {
		t.Fatal("zero max_items was accepted")
	}
}

func TestLoadConfigKeepsActorRouteEndpointLimitPositive(t *testing.T) {
	for _, raw := range []string{"0", "-1", "not-an-int"} {
		t.Setenv("ACTOR_ROUTE_MAX_ENDPOINTS", raw)
		if got := loadConfig().actorRouteMaxEndpoints; got <= 0 {
			t.Fatalf("ACTOR_ROUTE_MAX_ENDPOINTS=%q produced non-positive limit %d", raw, got)
		}
	}
}

func TestHolderConfigurationBoundsFallbacksAndValues(t *testing.T) {
	if got := BoundedHolderTimeout(0, time.Hour); got > 30*time.Second {
		t.Fatalf("timeout fallback=%s exceeds hard bound", got)
	}
	if got := BoundedHolderTimeout(time.Hour, time.Second); got > 30*time.Second {
		t.Fatalf("timeout value=%s exceeds hard bound", got)
	}
	if got := BoundedHolderMessageBytes(0, 8<<20); got > 4<<20 {
		t.Fatalf("message fallback=%d exceeds hard bound", got)
	}
	if got := BoundedHolderMessageBytes(8<<20, 1<<20); got > 4<<20 {
		t.Fatalf("message value=%d exceeds hard bound", got)
	}
}

func TestLoadConfigParsesPreAuthBudgets(t *testing.T) {
	t.Setenv("GATE_PREAUTH_MAX_CONNECTIONS", "123")
	t.Setenv("GATE_PREAUTH_MAX_CONNECTIONS_PER_IP", "7")
	t.Setenv("GATE_PREAUTH_MAX_FRAMES_PER_CONNECTION", "1")
	t.Setenv("GATE_PREAUTH_MAX_BYTES_PER_CONNECTION", "8192")
	t.Setenv("GATE_PREAUTH_MAX_BYTES_PER_IP", "65536")
	t.Setenv("GATE_PREAUTH_MAX_BYTES_POD", "1048576")
	t.Setenv("GATE_PREAUTH_HELLO_TIMEOUT", "1750ms")
	cfg := loadConfig()
	if cfg.preAuthMaxConnections != 123 ||
		cfg.preAuthMaxConnectionsPerIP != 7 ||
		cfg.preAuthMaxFramesPerConnection != 1 ||
		cfg.preAuthMaxBytesPerConnection != 8192 ||
		cfg.preAuthMaxBytesPerIP != 65536 ||
		cfg.preAuthMaxBytesPod != 1048576 ||
		cfg.preAuthHelloTimeout != 1750*time.Millisecond {
		t.Fatalf("PRE_AUTH config=%+v", cfg)
	}
}

func TestParseCookieKeyRing(t *testing.T) {
	keyV1 := bytes.Repeat([]byte{0x11}, 32)
	keyV2 := bytes.Repeat([]byte{0x22}, 32)
	value := "2=" + base64.RawURLEncoding.EncodeToString(keyV2) +
		",1=" + base64.RawURLEncoding.EncodeToString(keyV1)
	ring, err := parseCookieKeyRing(value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ring[1], keyV1) || !bytes.Equal(ring[2], keyV2) {
		t.Fatalf("parsed Cookie key ring does not match input")
	}

	for _, invalid := range []string{
		"bad",
		"0=" + base64.RawURLEncoding.EncodeToString(keyV1),
		"1=c2hvcnQ",
		"1=" + base64.RawURLEncoding.EncodeToString(keyV1) + ",1=" + base64.RawURLEncoding.EncodeToString(keyV2),
	} {
		if _, err := parseCookieKeyRing(invalid); err == nil {
			t.Fatalf("parseCookieKeyRing(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestParseCookieActiveVersion(t *testing.T) {
	if version, err := parseCookieActiveVersion("2"); err != nil || version != 2 {
		t.Fatalf("active version=(%d,%v), want (2,nil)", version, err)
	}
	for _, invalid := range []string{"", "0", "-1", "4294967296", "not-a-version"} {
		if _, err := parseCookieActiveVersion(invalid); err == nil {
			t.Fatalf("parseCookieActiveVersion(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestHTTPTrustedProxiesDefaultToWSButExplicitInvalidIsFailSafe(t *testing.T) {
	t.Setenv("GATE_WS_TRUSTED_PROXIES", "198.18.0.196/8")
	t.Setenv("GATE_HTTP_TRUSTED_PROXIES", "")
	cfg := loadConfig()
	if len(cfg.httpTrustedProxies) != 1 || !cfg.httpTrustedProxies[0].Contains(net.ParseIP("198.18.0.197")) {
		t.Fatalf("HTTP trusted proxies did not inherit WS proxies: %v", cfg.httpTrustedProxies)
	}

	t.Setenv("GATE_HTTP_TRUSTED_PROXIES", "not-a-cidr")
	cfg = loadConfig()
	if len(cfg.httpTrustedProxies) != 0 {
		t.Fatalf("invalid explicit HTTP trusted proxies fell back to a trusted range: %v", cfg.httpTrustedProxies)
	}
}

func TestLoadConfigParsesDownlinkCapacityBudgets(t *testing.T) {
	t.Setenv("GATE_DOWNLINK_WRITER_MAX_ITEMS", "512")
	t.Setenv("GATE_DOWNLINK_WRITER_MAX_BYTES", "2097152")
	t.Setenv("GATE_DOWNLINK_HINT_MAX_ITEMS", "24")
	t.Setenv("GATE_DOWNLINK_HINT_MAX_BYTES", "98304")
	t.Setenv("GATE_DOWNLINK_DRAIN_QUANTUM", "17")

	cfg := loadConfig()
	if cfg.downlinkWriterMaxItems != 512 || cfg.downlinkWriterMaxBytes != 2<<20 ||
		cfg.downlinkHintMaxItems != 24 || cfg.downlinkHintMaxBytes != 96<<10 ||
		cfg.downlinkDrainQuantum != 17 {
		t.Fatalf("downlink capacity config=%+v", cfg)
	}
	capacity, err := cfg.validatedDownlinkCapacity()
	if err != nil {
		t.Fatal(err)
	}
	if capacity.MaxItems != 512 || capacity.Hint.MaxHintItems != 24 {
		t.Fatalf("validated capacity=%+v", capacity)
	}

	cfg.downlinkHintMaxBytes = cfg.downlinkWriterMaxBytes + 1
	if _, err := cfg.validatedDownlinkCapacity(); err == nil {
		t.Fatal("oversized Hint quota was accepted")
	}
	cfg.downlinkHintMaxBytes = 0
	cfg.downlinkDrainQuantum = 0
	if _, err := cfg.validatedDownlinkCapacity(); err == nil {
		t.Fatal("zero drain quantum was accepted")
	}
}
