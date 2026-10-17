package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"farmgame/internal/gateway/account"
)

type recordingAttemptLimiter struct {
	decision account.AttemptDecision
	err      error
	attempts []account.LoginAttempt
	resets   []string
}

func (l *recordingAttemptLimiter) Allow(_ context.Context, attempt account.LoginAttempt) (account.AttemptDecision, error) {
	l.attempts = append(l.attempts, attempt)
	return l.decision, l.err
}

func (l *recordingAttemptLimiter) ResetAccount(_ context.Context, username string) error {
	l.resets = append(l.resets, username)
	return nil
}

func (l *recordingAttemptLimiter) Close() error { return nil }

func TestWebAccountRegisterAndLoginCreateNormalGameSessions(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(30_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	sessions := newTestService(t)
	register := NewAccountRegisterHandler(accounts, sessions)
	login := NewAccountLoginHandler(accounts, sessions)

	call := func(handler http.Handler, body string) (*httptest.ResponseRecorder, LoginResult) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/accounts/register", stringReader(body)))
		var result LoginResult
		if response.Code == http.StatusOK || response.Code == http.StatusCreated {
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
		}
		return response, result
	}

	body := `{"username":"farmer_one","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"web-device-1","client_build":"farm-h5/0.2"}`
	registeredResponse, registered := call(register, body)
	if registeredResponse.Code != http.StatusCreated || registered.PlayerID != 30_001 || registered.ConnectionTicket == "" {
		t.Fatalf("register status=%d result=%+v body=%s", registeredResponse.Code, registered, registeredResponse.Body.String())
	}
	if cookies := registeredResponse.Result().Cookies(); len(cookies) != 1 || !cookies[0].HttpOnly {
		t.Fatalf("register cookies=%v", cookies)
	}
	if _, err := sessions.Authenticate(context.Background(), registered.ConnectionTicket); err != nil {
		t.Fatalf("registered ticket invalid: %v", err)
	}

	loginResponse, loggedIn := call(login, body)
	if loginResponse.Code != http.StatusOK || loggedIn.PlayerID != registered.PlayerID ||
		loggedIn.ActiveSessionEpoch <= registered.ActiveSessionEpoch {
		t.Fatalf("login status=%d result=%+v", loginResponse.Code, loggedIn)
	}
}

func TestWebAccountErrorsDoNotLeakPasswordOrMissingUsername(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(30_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	sessions := newTestService(t)
	register := NewAccountRegisterHandler(accounts, sessions)
	login := NewAccountLoginHandler(accounts, sessions)
	valid := `{"username":"farmer_two","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"web-device-2","client_build":"farm-h5/0.2"}`

	response := httptest.NewRecorder()
	register.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/register", stringReader(valid)))
	if response.Code != http.StatusCreated {
		t.Fatalf("register status=%d", response.Code)
	}
	response = httptest.NewRecorder()
	register.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/register", stringReader(valid)))
	if response.Code != http.StatusConflict {
		t.Fatalf("duplicate status=%d", response.Code)
	}
	for _, body := range []string{
		`{"username":"farmer_two","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"web-device-2","client_build":"farm-h5/0.2"}`,
		`{"username":"missing_user","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"web-device-2","client_build":"farm-h5/0.2"}`,
	} {
		response = httptest.NewRecorder()
		login.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/login", stringReader(body)))
		if response.Code != http.StatusUnauthorized || response.Body.String() == "" {
			t.Fatalf("login status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestWebAccountLimiterUsesAccountIPAndDeviceBeforeKDF(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(30_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	sessions := newTestService(t)
	register := NewAccountRegisterHandler(accounts, sessions)
	body := `{"username":"farmer_limit","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"bootstrap-fallback","device_id":"browser-device","client_build":"farm-h5/0.2"}`

	limiter := &recordingAttemptLimiter{decision: account.AttemptDecision{Allowed: true}}
	register.SetAttemptLimiter(limiter, func(*http.Request) string { return "203.0.113.9" })
	response := httptest.NewRecorder()
	register.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/register", stringReader(body)))
	if response.Code != http.StatusCreated {
		t.Fatalf("register status=%d body=%s", response.Code, response.Body.String())
	}
	if len(limiter.attempts) != 1 || len(limiter.resets) != 1 {
		t.Fatalf("limiter calls attempts=%+v resets=%+v", limiter.attempts, limiter.resets)
	}
	got := limiter.attempts[0]
	if got.Username != "farmer_limit" || got.ClientIP != "203.0.113.9" || got.DeviceID != "browser-device" {
		t.Fatalf("limiter attempt=%+v", got)
	}
}

func TestWebAccountLimiterRejectsBeforeCredentialWork(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(30_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAccountLoginHandler(accounts, newTestService(t))
	limiter := &recordingAttemptLimiter{decision: account.AttemptDecision{
		Allowed: false, RetryAfter: 1500 * time.Millisecond,
	}}
	handler.SetAttemptLimiter(limiter, func(*http.Request) string { return "192.0.2.10" })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/login", stringReader(
		`{"username":"missing_user","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"fallback-device"}`,
	)))
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "2" {
		t.Fatalf("limited status=%d retry=%q body=%s",
			response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
	if len(limiter.attempts) != 1 || limiter.attempts[0].DeviceID != "fallback-device" {
		t.Fatalf("fallback device attempt=%+v", limiter.attempts)
	}
	if len(limiter.resets) != 0 {
		t.Fatalf("denied attempt reset account bucket: %+v", limiter.resets)
	}
}

func TestWebAccountLimiterFailsClosedWhenRedisUnavailable(t *testing.T) {
	accounts, err := account.NewService(account.NewMemoryStore(30_000), account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAccountLoginHandler(accounts, newTestService(t))
	limiter := &recordingAttemptLimiter{err: errors.New("redis unavailable")}
	handler.SetAttemptLimiter(limiter, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/login", stringReader(
		`{"username":"farmer_one","password":"REPLACE_WITH_LOCAL_SECRET","bootstrap_id":"device"}`,
	)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("limiter outage status=%d body=%s", response.Code, response.Body.String())
	}
}
