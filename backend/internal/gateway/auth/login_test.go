package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/session"

	"google.golang.org/protobuf/proto"
)

// staticVerifier 是测试用的平台票据校验:固定 token → 固定 player_id。
type staticVerifier struct {
	token    string
	playerID uint64
	fail     bool
}

func (v *staticVerifier) Verify(_ context.Context, token string) (uint64, error) {
	if v.fail || token != v.token {
		return 0, errors.New("bad token")
	}
	return v.playerID, nil
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	v := &staticVerifier{token: "plt-ok", playerID: 4242}
	auth := session.New(session.Config{Secret: []byte("test-secret"), AuthorityEpoch: 1})
	tk := ticketSvc()
	svc, err := New(v, auth, tk, Config{
		GatewayID:          "gw-test",
		WssEndpoint:        "wss://farm.example.com/ws",
		InitialRuleVersion: 1,
		TicketTTL:          time.Minute,
		PermitSecret:       []byte("test-secret"),
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	svc.SetBootstrapper(testBootstrapper{})
	return svc
}

func TestLoginFlow(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	res, err := svc.Login(ctx, LoginRequest{PlatformToken: "plt-ok", BootstrapID: "boot-1", ClientBuild: "h5-1"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.PlayerID != 4242 || res.ConnectionTicket == "" || res.WssEndpoint == "" {
		t.Fatalf("login result incomplete: %+v", res)
	}
	if !ticketValid(res.ConnectionTicket) {
		t.Fatalf("issued ticket bad format")
	}
	// 用票据走 WSS ClientHello 认证。
	b, err := svc.Authenticate(ctx, res.ConnectionTicket)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if b.PlayerID != 4242 || b.ActiveSessionEpoch != res.ReservedSessionEpoch {
		t.Fatalf("auth binding mismatch: %+v vs %+v", b, res)
	}
	// 同一票据不可二次核销。
	if _, err := svc.Authenticate(ctx, res.ConnectionTicket); err != ErrTicketInvalid {
		t.Fatalf("second authenticate want ErrTicketInvalid, got %v", err)
	}
}

func TestLoginUnauthorized(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Login(context.Background(), LoginRequest{PlatformToken: "wrong", BootstrapID: "b"}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}

func TestLoginHTTPHandler(t *testing.T) {
	svc := newTestService(t)
	h := NewHandler(svc)
	body := `{"platform_token":"plt-ok","bootstrap_id":"boot-1","client_build":"h5-1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", stringReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	var res LoginResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.ConnectionTicket == "" {
		t.Fatalf("handler missing ticket")
	}
	if !strings.Contains(w.Body.String(), `"player_id":"4242"`) ||
		!strings.Contains(w.Body.String(), `"reserved_session_epoch":"1"`) ||
		!strings.Contains(w.Body.String(), `"active_session_epoch":"0"`) {
		t.Fatalf("uint64 HTTP fields must be decimal strings: %s", w.Body.String())
	}
}

func TestHTTPSLoginDoesNotFenceActiveWSSUntilClientHello(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	first, err := svc.Login(ctx, LoginRequest{PlatformToken: "plt-ok", BootstrapID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	firstBinding, err := svc.AuthenticateConnection(ctx, first.ConnectionTicket, session.RouteFence{
		GatewayID: "gw-test", GatewayBootID: "boot-a", ConnectionID: "conn-a", ConnectionGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := svc.Login(ctx, LoginRequest{PlatformToken: "plt-ok", BootstrapID: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.ActiveSessionEpoch != firstBinding.Binding.ActiveSessionEpoch {
		t.Fatalf("HTTPS login fenced active WSS: active=%d candidate_view=%d",
			firstBinding.Binding.ActiveSessionEpoch, candidate.ActiveSessionEpoch)
	}
	if _, err := svc.IssuePermit(ctx, firstBinding.Binding, session.PermitRequest{
		ConnectionID: "conn-a", RequestID: "before-hello", Action: session.ActionReadOnly,
	}); err != nil {
		t.Fatalf("old WSS must remain writable before candidate ClientHello: %v", err)
	}
	secondBinding, err := svc.AuthenticateConnection(ctx, candidate.ConnectionTicket, session.RouteFence{
		GatewayID: "gw-test", GatewayBootID: "boot-b", ConnectionID: "conn-b", ConnectionGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if secondBinding.Binding.ActiveSessionEpoch != candidate.ReservedSessionEpoch {
		t.Fatalf("activated=%d reserved=%d", secondBinding.Binding.ActiveSessionEpoch, candidate.ReservedSessionEpoch)
	}
	if _, err := svc.IssuePermit(ctx, firstBinding.Binding, session.PermitRequest{
		ConnectionID: "conn-a", RequestID: "after-hello", Action: session.ActionReadOnly,
	}); !errors.Is(err, session.ErrStaleSession) {
		t.Fatalf("old WSS permit after activation err=%v, want stale", err)
	}
}

func TestLoginHTTPUnauthorized(t *testing.T) {
	svc := newTestService(t)
	h := NewHandler(svc)
	body := `{"platform_token":"bad","bootstrap_id":"b"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/auth/login", stringReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
}

// projectionBootstrapper returns a fixed home projection so the login path can
// be observed end to end.
type projectionBootstrapper struct{ view *publicpb.GetPlayerBootstrapResult }

func (b projectionBootstrapper) EnsureOwnerInitialized(context.Context, uint64, string, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return b.view, nil
}

func (b projectionBootstrapper) EnsureOwnerInitializedWithProfile(context.Context, uint64, string, string, uint32, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return b.view, nil
}

// TestLoginAttachesHomeBootstrap pins the login-contract merge: the HTTP login
// response carries the same GetPlayerBootstrapResult the WSS command would
// have returned, and the client JSON round-trips it exactly.
func TestLoginAttachesHomeBootstrap(t *testing.T) {
	view := &publicpb.GetPlayerBootstrapResult{
		PlayerStateRevision: 7,
		Profile: &publicpb.PlayerPrivateProfileView{PlayerId: 4242, DisplayName: "tester"},
		FeatureSummary: &publicpb.PlayerFeatureSummaryView{
			Tasks: &publicpb.PlayerTaskSummaryView{DailyPeriodId: "d-1", TaskStateVersion: 3},
		},
	}
	svc := newTestService(t)
	svc.SetBootstrapper(projectionBootstrapper{view: view})
	res, err := svc.Login(context.Background(), LoginRequest{PlatformToken: "plt-ok", BootstrapID: "boot-1", ClientBuild: "h5-1"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.HomeBootstrap == nil || !proto.Equal(res.HomeBootstrap, view) {
		t.Fatalf("home_bootstrap not attached: %+v want %+v", res.HomeBootstrap, view)
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var decoded LoginResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("login JSON round trip: %v", err)
	}
	if decoded.HomeBootstrap == nil || !proto.Equal(decoded.HomeBootstrap, view) {
		t.Fatalf("home_bootstrap lost in JSON round trip: %+v", decoded.HomeBootstrap)
	}
	if decoded.PlayerID != res.PlayerID || decoded.SessionID != res.SessionID {
		t.Fatalf("identity fields lost in JSON round trip")
	}
}

// TestLoginOmitsHomeBootstrapForLegacyResponse pins old-client compatibility:
// a bootstrapper that returns no projection must produce a login response
// without home_bootstrap, and the WSS client must then fall back to
// GET_PLAYER_BOOTSTRAP.
func TestLoginOmitsHomeBootstrapForLegacyResponse(t *testing.T) {
	svc := newTestService(t)
	res, err := svc.Login(context.Background(), LoginRequest{PlatformToken: "plt-ok", BootstrapID: "boot-1", ClientBuild: "h5-1"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if res.HomeBootstrap != nil {
		t.Fatalf("legacy login must omit home_bootstrap, got %+v", res.HomeBootstrap)
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "home_bootstrap") {
		t.Fatalf("legacy login JSON must not contain home_bootstrap: %s", encoded)
	}
}
