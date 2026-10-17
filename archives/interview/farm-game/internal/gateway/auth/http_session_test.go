package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"farmgame/internal/gateway/session"
)

func TestRenewUsesCurrentHTTPSessionWithoutAdvancingEpoch(t *testing.T) {
	svc := newTestService(t)
	login, err := svc.Login(context.Background(), LoginRequest{
		PlatformToken: "plt-ok",
		BootstrapID:   "bootstrap-renew",
		ClientBuild:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := svc.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/renew", stringReader(`{}`))
	request.AddCookie(&http.Cookie{Name: HTTPSessionCookieName, Value: token})
	response := httptest.NewRecorder()
	NewRenewHandler(svc).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("renew status=%d body=%s", response.Code, response.Body.String())
	}
	var renewed LoginResult
	if err := json.Unmarshal(response.Body.Bytes(), &renewed); err != nil {
		t.Fatal(err)
	}
	if renewed.ConnectionTicket == "" || renewed.ConnectionTicket == login.ConnectionTicket {
		t.Fatalf("renewed ticket missing or replayed: %+v", renewed)
	}
	if renewed.ActiveSessionEpoch != login.ActiveSessionEpoch || renewed.SessionID != login.SessionID {
		t.Fatalf("renew advanced session generation: before=%+v after=%+v", login, renewed)
	}
	if _, err := svc.Authenticate(context.Background(), renewed.ConnectionTicket); err != nil {
		t.Fatalf("renewed ticket unusable: %v", err)
	}
}

func TestRenewRejectsMissingSessionCookie(t *testing.T) {
	response := httptest.NewRecorder()
	NewRenewHandler(newTestService(t)).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/api/v1/auth/renew", stringReader(`{}`)),
	)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("renew without cookie status=%d", response.Code)
	}
}

func TestLoginIssuesHttpOnlySessionCookieAndWSSReplacementDoesNotRevokeIt(t *testing.T) {
	svc := newTestService(t)
	handler := NewHandler(svc)

	login := func(bootstrap string) *http.Cookie {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login",
			stringReader(`{"platform_token":"plt-ok","bootstrap_id":"`+bootstrap+`","client_build":"test"}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("login status=%d body=%s", response.Code, response.Body.String())
		}
		result := response.Result()
		cookies := result.Cookies()
		if len(cookies) != 1 {
			t.Fatalf("cookies=%v", cookies)
		}
		cookie := cookies[0]
		if cookie.Name != HTTPSessionCookieName || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Value == "" {
			t.Fatalf("bad session cookie: %+v", cookie)
		}
		return cookie
	}

	first := login("bootstrap-1")
	binding, err := svc.AuthenticateHTTPSession(context.Background(), first.Value)
	if err != nil || binding.PlayerID != 4242 {
		t.Fatalf("authenticate first=%+v err=%v", binding, err)
	}
	_ = login("bootstrap-2")
	if _, err := svc.AuthenticateHTTPSession(context.Background(), first.Value); err != nil {
		t.Fatalf("candidate login must not revoke an authenticated Cookie: %v", err)
	}
	if err := svc.authority.Revoke(context.Background(), 4242); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AuthenticateHTTPSession(context.Background(), first.Value); err != ErrHTTPSessionInvalid {
		t.Fatalf("account revoke should stale old Cookie, err=%v", err)
	}
}

func TestHTTPSessionRejectsTampering(t *testing.T) {
	svc := newTestService(t)
	result, err := svc.Login(context.Background(), LoginRequest{PlatformToken: "plt-ok", BootstrapID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := svc.IssueHTTPSession(result)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AuthenticateHTTPSession(context.Background(), token+"x"); err != ErrHTTPSessionInvalid {
		t.Fatalf("tampered token err=%v", err)
	}
}

func TestHTTPSessionCookieKeyRingSupportsRollingRotation(t *testing.T) {
	authority := session.New(session.Config{Secret: []byte("authority-test-secret"), AuthorityEpoch: 1})
	build := func(active uint32, ring map[uint32][]byte) *Service {
		t.Helper()
		svc, err := New(
			&staticVerifier{token: "plt-ok", playerID: 4242},
			authority,
			ticketSvc(),
			Config{
				GatewayID: "gate", WssEndpoint: "wss://example.test/ws",
				PermitSecret:     []byte("rotation-test-permit-secret"),
				CookieKeyVersion: active, CookieKeyRing: ring,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		svc.SetBootstrapper(testBootstrapper{})
		return svc
	}
	keyV1 := bytes.Repeat([]byte{0x11}, cookieSigningKeyMinBytes)
	keyV2 := bytes.Repeat([]byte{0x22}, cookieSigningKeyMinBytes)
	v1 := build(1, map[uint32][]byte{1: keyV1})
	login, err := v1.Login(context.Background(), LoginRequest{
		PlatformToken: "plt-ok", BootstrapID: "rotation",
	})
	if err != nil {
		t.Fatal(err)
	}
	oldCookie, _, err := v1.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}

	rotating := build(2, map[uint32][]byte{1: keyV1, 2: keyV2})
	if _, err := rotating.AuthenticateHTTPSession(context.Background(), oldCookie); err != nil {
		t.Fatalf("old Cookie rejected inside rotation window: %v", err)
	}
	newCookie, _, err := rotating.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotating.AuthenticateHTTPSession(context.Background(), newCookie); err != nil {
		t.Fatalf("new active-version Cookie rejected: %v", err)
	}
	if _, err := v1.AuthenticateHTTPSession(context.Background(), newCookie); !errors.Is(err, ErrHTTPSessionInvalid) {
		t.Fatalf("old deployment accepted unknown future Cookie key: %v", err)
	}

	retired := build(2, map[uint32][]byte{2: keyV2})
	if _, err := retired.AuthenticateHTTPSession(context.Background(), oldCookie); !errors.Is(err, ErrHTTPSessionInvalid) {
		t.Fatalf("retired Cookie key remained valid: %v", err)
	}
	if _, err := retired.AuthenticateHTTPSession(context.Background(), newCookie); err != nil {
		t.Fatalf("active Cookie failed after old key retirement: %v", err)
	}
}

func TestHTTPSessionCookieKeyRingRejectsInvalidConfiguration(t *testing.T) {
	tests := []Config{
		{PermitSecret: []byte("cookie-config-test-permit"), CookieKeyVersion: 2, CookieKeyRing: map[uint32][]byte{1: bytes.Repeat([]byte{1}, cookieSigningKeyMinBytes)}},
		{PermitSecret: []byte("cookie-config-test-permit"), CookieKeyVersion: 1, CookieKeyRing: map[uint32][]byte{1: []byte("short")}},
		{PermitSecret: []byte("cookie-config-test-permit"), CookieKeyVersion: 1, CookieKeyRing: map[uint32][]byte{0: bytes.Repeat([]byte{1}, cookieSigningKeyMinBytes)}},
		{PermitSecret: []byte("cookie-config-test-permit"), CookieKeyVersion: 1, CookieKeyRing: map[uint32][]byte{1: []byte("farm-game-dev-secret-do-not-use-in-prod")}},
	}
	for _, cfg := range tests {
		if _, err := New(&staticVerifier{}, nil, nil, cfg); err == nil {
			t.Fatalf("invalid Cookie key ring accepted: %+v", cfg.CookieKeyRing)
		}
	}
}

func TestAuthConfigurationNeverInjectsDefaultPermitSecret(t *testing.T) {
	key := bytes.Repeat([]byte{0x31}, cookieSigningKeyMinBytes)
	_, err := New(&staticVerifier{}, nil, nil, Config{
		CookieKeyVersion: 1,
		CookieKeyRing:    map[uint32][]byte{1: key},
	})
	if err == nil {
		t.Fatal("missing permit secret was replaced by a default")
	}
}

func TestRevokeAllInvalidatesCookieAndActiveWSSPermit(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	login, err := svc.Login(ctx, LoginRequest{
		PlatformToken: "plt-ok", BootstrapID: "revoke-all", ClientBuild: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	cookieToken, _, err := svc.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}
	activated, err := svc.AuthenticateConnection(ctx, login.ConnectionTicket, session.RouteFence{
		GatewayID: "gw-test", GatewayBootID: "boot-revoke",
		ConnectionID: "conn-revoke", ConnectionGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	permitRequest := session.PermitRequest{
		GatewayID: "gw-test", GatewayBootID: "boot-revoke",
		ConnectionID: "conn-revoke", ConnectionGeneration: 1,
		RouteRevision: activated.Binding.Route.RouteRevision,
		RequestID:     "before-revoke", Action: session.ActionReadOnly,
	}
	if _, err := svc.IssuePermit(ctx, activated.Binding, permitRequest); err != nil {
		t.Fatalf("active WSS could not obtain permit before revoke: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout-all", nil)
	request.AddCookie(&http.Cookie{Name: HTTPSessionCookieName, Value: cookieToken})
	response := httptest.NewRecorder()
	NewRevokeAllHandler(svc, true).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("logout-all status=%d body=%s", response.Code, response.Body.String())
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != HTTPSessionCookieName ||
		cookies[0].Value != "" || cookies[0].Path != "/" ||
		!cookies[0].HttpOnly || !cookies[0].Secure ||
		cookies[0].SameSite != http.SameSiteLaxMode || cookies[0].MaxAge >= 0 {
		t.Fatalf("logout-all did not clear session Cookie: %+v", cookies)
	}
	if _, err := svc.AuthenticateHTTPSession(ctx, cookieToken); !errors.Is(err, ErrHTTPSessionInvalid) {
		t.Fatalf("revoked Cookie remained valid: %v", err)
	}
	permitRequest.RequestID = "after-revoke"
	if _, err := svc.IssuePermit(ctx, activated.Binding, permitRequest); !errors.Is(err, session.ErrStaleSession) {
		t.Fatalf("revoked WSS obtained a new permit: %v", err)
	}

	renew := httptest.NewRequest(http.MethodPost, "/api/v1/auth/renew", nil)
	renew.AddCookie(&http.Cookie{Name: HTTPSessionCookieName, Value: cookieToken})
	renewResponse := httptest.NewRecorder()
	NewRenewHandler(svc).ServeHTTP(renewResponse, renew)
	if renewResponse.Code != http.StatusUnauthorized {
		t.Fatalf("revoked Cookie renewed a ticket: status=%d", renewResponse.Code)
	}
}

func TestRevokeAllRequiresAuthenticatedCookie(t *testing.T) {
	response := httptest.NewRecorder()
	NewRevokeAllHandler(newTestService(t)).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout-all", nil),
	)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("logout-all without Cookie status=%d", response.Code)
	}
}

func TestRevokeAllRejectsNonPOST(t *testing.T) {
	response := httptest.NewRecorder()
	NewRevokeAllHandler(newTestService(t)).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/auth/logout-all", nil),
	)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("logout-all GET status=%d", response.Code)
	}
}
