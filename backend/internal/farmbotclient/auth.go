package farmbotclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	publicpb "farmgame/api/gen/public"
)

// Credentials are the caller-injected platform identity. Secret is copied
// by the client before a Session retains it for reconnects.
type Credentials struct {
	PlayerID    uint64
	Secret      []byte
	BootstrapID string
}

func (c Credentials) clone() Credentials {
	c.Secret = append([]byte(nil), c.Secret...)
	return c
}

// LoginResult is the identity returned by the HTTP login endpoint. Cookie
// and FarmSessionCookie contain the value of the farm_session Set-Cookie;
// both fields are populated for callers that prefer either spelling.
type LoginResult struct {
	PlayerID          uint64 `json:"player_id"`
	SessionID         string `json:"session_id"`
	ConnectionTicket  string `json:"connection_ticket"`
	WssEndpoint       string `json:"wss_endpoint,omitempty"`
	Cookie            string `json:"-"`
	FarmSessionCookie string `json:"-"`
	// HomeBootstrap is the optional home projection on the login response.
	// A present value means the client must not send GET_PLAYER_BOOTSTRAP for
	// the first paint; absence is not a login failure.
	HomeBootstrap *publicpb.GetPlayerBootstrapResult `json:"home_bootstrap,omitempty"`
}

// PlayerIDString returns the decimal HTTP/protocol identity.
func (r LoginResult) PlayerIDString() string { return strconv.FormatUint(r.PlayerID, 10) }

// Authenticator obtains a one-use connection ticket and HTTP session.
type Authenticator interface {
	Login(context.Context, Credentials) (LoginResult, error)
}

// PlatformHMACAuthenticator implements the platform HMAC login used by
// cmd/ws-stress. It never logs or formats the injected secret.
type PlatformHMACAuthenticator struct {
	config     Config
	httpClient *http.Client
}

// NewPlatformHMACAuthenticator constructs the platform HMAC authenticator.
func NewPlatformHMACAuthenticator(config Config) (*PlatformHMACAuthenticator, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	httpClient, err := normalized.httpClient()
	if err != nil {
		return nil, err
	}
	return &PlatformHMACAuthenticator{config: normalized, httpClient: httpClient}, nil
}

// NewAuthenticator is the default platform-HMAC authenticator constructor.
func NewAuthenticator(config Config) (Authenticator, error) {
	return NewPlatformHMACAuthenticator(config)
}

// Login sends POST /api/v1/auth/login with the exact platform token shape
// used by the verified stress client: load:<player_id>:hex(HMAC-SHA256(id)).
func (a *PlatformHMACAuthenticator) Login(ctx context.Context, credentials Credentials) (LoginResult, error) {
	if a == nil || a.httpClient == nil {
		return LoginResult{}, errors.New("farmbotclient: authenticator is nil")
	}
	if credentials.PlayerID == 0 {
		return LoginResult{}, errors.New("farmbotclient: player id must be non-zero")
	}
	if len(credentials.Secret) == 0 {
		return LoginResult{}, errors.New("farmbotclient: platform HMAC secret must be non-empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	playerID := strconv.FormatUint(credentials.PlayerID, 10)
	mac := hmac.New(sha256.New, credentials.Secret)
	_, _ = mac.Write([]byte(playerID))
	bootstrapID := strings.TrimSpace(credentials.BootstrapID)
	if bootstrapID == "" {
		bootstrapID = "ws-stress-" + playerID
	}
	payload, err := json.Marshal(struct {
		PlatformToken string `json:"platform_token"`
		BootstrapID   string `json:"bootstrap_id"`
		ClientBuild   string `json:"client_build"`
	}{
		PlatformToken: "load:" + playerID + ":" + hex.EncodeToString(mac.Sum(nil)),
		BootstrapID:   bootstrapID,
		ClientBuild:   a.config.ClientBuild,
	})
	if err != nil {
		return LoginResult{}, fmt.Errorf("farmbotclient: encode login request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(a.config.HTTPBaseURL, "/")+"/api/v1/auth/login", strings.NewReader(string(payload)))
	if err != nil {
		return LoginResult{}, fmt.Errorf("farmbotclient: create login request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := a.httpClient.Do(request)
	if err != nil {
		return LoginResult{}, fmt.Errorf("farmbotclient: login request: %w", err)
	}
	defer response.Body.Close()
	body, err := readBoundedBody(response.Body, a.config.MaxResponseBodyBytes)
	if err != nil {
		return LoginResult{}, fmt.Errorf("farmbotclient: read login response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		// Do not include response bytes: an upstream error body must never
		// become an accidental credential/logging channel.
		return LoginResult{}, fmt.Errorf("farmbotclient: login status %d", response.StatusCode)
	}
	result, err := decodeLoginResult(body)
	if err != nil {
		return LoginResult{}, fmt.Errorf("farmbotclient: decode login response: %w", err)
	}
	if result.PlayerID != credentials.PlayerID || strings.TrimSpace(result.SessionID) == "" || strings.TrimSpace(result.ConnectionTicket) == "" {
		return LoginResult{}, errors.New("farmbotclient: login returned invalid identity or connection ticket")
	}
	cookieValue := ""
	for _, cookie := range response.Cookies() {
		if cookie.Name == FarmSessionCookieName {
			cookieValue = cookie.Value
			break
		}
	}
	if cookieValue == "" {
		return LoginResult{}, errors.New("farmbotclient: login did not issue the farm_session cookie")
	}
	result.Cookie = cookieValue
	result.FarmSessionCookie = cookieValue
	return result, nil
}

func readBoundedBody(reader io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("farmbotclient: invalid response body limit")
	}
	limited := io.LimitReader(reader, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errors.New("farmbotclient: response body exceeds limit")
	}
	return body, nil
}

func decodeLoginResult(body []byte) (LoginResult, error) {
	var wire struct {
		PlayerID         json.RawMessage `json:"player_id"`
		SessionID        string          `json:"session_id"`
		ConnectionTicket string          `json:"connection_ticket"`
		WssEndpoint      string          `json:"wss_endpoint"`
		HomeBootstrap    json.RawMessage `json:"home_bootstrap"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return LoginResult{}, err
	}
	playerID, err := parseUint64JSON(wire.PlayerID)
	if err != nil {
		return LoginResult{}, fmt.Errorf("player_id: %w", err)
	}
	result := LoginResult{
		PlayerID: playerID, SessionID: wire.SessionID,
		ConnectionTicket: wire.ConnectionTicket, WssEndpoint: wire.WssEndpoint,
	}
	result.HomeBootstrap = decodeOptionalHomeBootstrap(wire.HomeBootstrap)
	return result, nil
}

func decodeOptionalHomeBootstrap(raw json.RawMessage) *publicpb.GetPlayerBootstrapResult {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var view publicpb.GetPlayerBootstrapResult
	if err := json.Unmarshal(raw, &view); err != nil {
		return nil
	}
	return &view
}

func parseUint64JSON(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, errors.New("missing value")
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		if strings.TrimSpace(text) == "" {
			return 0, errors.New("empty value")
		}
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, err
	}
	return value, nil
}
