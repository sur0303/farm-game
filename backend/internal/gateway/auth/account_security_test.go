package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"farmgame/internal/gateway/account"
	"farmgame/internal/gateway/session"
)

func TestChangePasswordRevokesCookieAndActiveWSSPermit(t *testing.T) {
	ctx := context.Background()
	accounts, err := account.NewService(account.NewMemoryStore(50_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	created, err := accounts.Register(ctx, "farmer_change", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	sessions := newTestService(t)
	login, err := sessions.LoginPlayer(ctx, created.PlayerID, "device-change", "test")
	if err != nil {
		t.Fatal(err)
	}
	cookieToken, _, err := sessions.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}
	activated, err := sessions.AuthenticateConnection(ctx, login.ConnectionTicket, session.RouteFence{
		GatewayID: "gw-test", GatewayBootID: "boot-change",
		ConnectionID: "conn-change", ConnectionGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	permitRequest := session.PermitRequest{
		GatewayID: "gw-test", GatewayBootID: "boot-change",
		ConnectionID: "conn-change", ConnectionGeneration: 1,
		RouteRevision: activated.Binding.Route.RouteRevision,
		RequestID:     "before-change", Action: session.ActionReadOnly,
	}
	if _, err := sessions.IssuePermit(ctx, activated.Binding, permitRequest); err != nil {
		t.Fatalf("active WSS could not obtain permit before change: %v", err)
	}

	response := httptest.NewRecorder()
	body := `{"username":"farmer_change","current_password":"REPLACE_WITH_LOCAL_SECRET","new_password":"REPLACE_WITH_LOCAL_SECRET"}`
	NewChangePasswordHandler(accounts, sessions, true).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/api/v1/accounts/change-password", stringReader(body)),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("change-password status=%d body=%s", response.Code, response.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload["status"] != "password_changed" {
		t.Fatalf("payload=%v err=%v", payload, err)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != HTTPSessionCookieName || cookies[0].Value != "" ||
		!cookies[0].HttpOnly || !cookies[0].Secure || cookies[0].MaxAge >= 0 {
		t.Fatalf("change-password did not clear Cookie: %+v", cookies)
	}
	if _, err := sessions.AuthenticateHTTPSession(ctx, cookieToken); !errors.Is(err, ErrHTTPSessionInvalid) {
		t.Fatalf("old Cookie remained valid: %v", err)
	}
	permitRequest.RequestID = "after-change"
	if _, err := sessions.IssuePermit(ctx, activated.Binding, permitRequest); !errors.Is(err, session.ErrStaleSession) {
		t.Fatalf("old WSS obtained a new permit: %v", err)
	}
	if _, err := accounts.Authenticate(ctx, "farmer_change", "correct horse battery"); !errors.Is(err, account.ErrInvalidCredential) {
		t.Fatalf("old password still accepted: %v", err)
	}
	if playerID, err := accounts.Authenticate(ctx, "farmer_change", "new correct horse"); err != nil || playerID != created.PlayerID {
		t.Fatalf("new password authenticate=(%d,%v)", playerID, err)
	}
}

func TestAdminDisableRevokesSessionsAndBlocksLogin(t *testing.T) {
	ctx := context.Background()
	accounts, err := account.NewService(account.NewMemoryStore(60_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	created, err := accounts.Register(ctx, "farmer_ban", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	sessions := newTestService(t)
	login, err := sessions.LoginPlayer(ctx, created.PlayerID, "device-ban", "test")
	if err != nil {
		t.Fatal(err)
	}
	cookieToken, _, err := sessions.IssueHTTPSession(login)
	if err != nil {
		t.Fatal(err)
	}
	activated, err := sessions.AuthenticateConnection(ctx, login.ConnectionTicket, session.RouteFence{
		GatewayID: "gw-test", GatewayBootID: "boot-ban",
		ConnectionID: "conn-ban", ConnectionGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	handler := NewAdminDisableHandler(accounts, sessions, "admin-secret-token")
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/disable",
		stringReader(`{"username":"farmer_ban"}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("missing admin token status=%d", unauthorized.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/disable",
		stringReader(`{"username":"farmer_ban"}`))
	request.Header.Set("X-Farm-Admin-Token", "admin-secret-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("disable status=%d body=%s", response.Code, response.Body.String())
	}

	if _, err := sessions.AuthenticateHTTPSession(ctx, cookieToken); !errors.Is(err, ErrHTTPSessionInvalid) {
		t.Fatalf("disabled account Cookie remained valid: %v", err)
	}
	if _, err := sessions.IssuePermit(ctx, activated.Binding, session.PermitRequest{
		GatewayID: "gw-test", GatewayBootID: "boot-ban",
		ConnectionID: "conn-ban", ConnectionGeneration: 1,
		RouteRevision: activated.Binding.Route.RouteRevision,
		RequestID:     "after-disable", Action: session.ActionReadOnly,
	}); !errors.Is(err, session.ErrStaleSession) {
		t.Fatalf("disabled account WSS obtained a new permit: %v", err)
	}
	if _, err := accounts.Authenticate(ctx, "farmer_ban", "correct horse battery"); !errors.Is(err, account.ErrInvalidCredential) {
		t.Fatalf("disabled account login leaked status: %v", err)
	}

	// Idempotent second disable.
	again := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/disable",
		stringReader(`{"username":"farmer_ban"}`))
	again.Header.Set("Authorization", "Bearer REPLACE_WITH_LOCAL_SECRET")
	againResponse := httptest.NewRecorder()
	handler.ServeHTTP(againResponse, again)
	if againResponse.Code != http.StatusOK {
		t.Fatalf("idempotent disable status=%d", againResponse.Code)
	}
}

func TestChangePasswordRejectsSamePassword(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(70_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accounts.Register(context.Background(), "farmer_same", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	NewChangePasswordHandler(accounts, newTestService(t)).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/change-password", stringReader(
			`{"username":"farmer_same","current_password":"REPLACE_WITH_LOCAL_SECRET","new_password":"REPLACE_WITH_LOCAL_SECRET"}`,
		)),
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("same password status=%d", response.Code)
	}
}
