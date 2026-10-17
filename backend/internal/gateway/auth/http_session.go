package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	HTTPSessionCookieName = "farm_session"
	DefaultHTTPSessionTTL = 2 * time.Hour
	maxHTTPSessionBytes   = 4096
)

var ErrHTTPSessionInvalid = errors.New("auth: invalid http session")

type httpSessionClaims struct {
	PlayerID              string `json:"player_id"`
	SessionSubject        string `json:"session_subject"`
	SessionAuthorityEpoch string `json:"session_authority_epoch"`
	IssuedAtMS            int64  `json:"issued_at_ms,omitempty"`
	ExpiresAtMS           int64  `json:"expire_at_ms,omitempty"`
	// Rolling compatibility with v0.2 Cookies. New issuers leave these empty.
	LegacyIssuedAtUnix  int64  `json:"issued_at_unix,omitempty"`
	LegacyExpiresAtUnix int64  `json:"expires_at_unix,omitempty"`
	CookieVersion       uint32 `json:"cookie_version"`
}

// IssueHTTPSession signs an HttpOnly Cookie subject. It is bound to the
// player's authority epoch, not to one candidate/active WebSocket generation.
func (s *Service) IssueHTTPSession(result *LoginResult) (string, time.Time, error) {
	if s == nil || result == nil || result.PlayerID == 0 || result.SessionID == "" {
		return "", time.Time{}, ErrHTTPSessionInvalid
	}
	ttl := s.cfg.HTTPSessionTTL
	if ttl <= 0 {
		ttl = DefaultHTTPSessionTTL
	}
	expiresAt := time.Now().Add(ttl)
	issuedAt := time.Now()
	claims := httpSessionClaims{
		PlayerID:              strconv.FormatUint(result.PlayerID, 10),
		SessionSubject:        result.SessionID,
		SessionAuthorityEpoch: strconv.FormatUint(result.SessionAuthorityEpoch, 10),
		IssuedAtMS:            issuedAt.UnixMilli(),
		ExpiresAtMS:           expiresAt.UnixMilli(),
		CookieVersion:         s.cfg.CookieKeyVersion,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	key := s.cfg.CookieKeyRing[s.cfg.CookieKeyVersion]
	if len(key) == 0 {
		return "", time.Time{}, ErrHTTPSessionInvalid
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + signature, expiresAt, nil
}

// AuthenticateHTTPSession validates the signed Cookie and its player authority
// epoch. Replacing a WebSocket does not invalidate the Cookie. Password-change
// and account-disable flows must call SessionAuthority.Revoke, as does explicit
// all-device revocation.
func (s *Service) AuthenticateHTTPSession(ctx context.Context, token string) (*SessionBinding, error) {
	if s == nil || strings.TrimSpace(token) == "" || len(token) > maxHTTPSessionBytes {
		return nil, ErrHTTPSessionInvalid
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, ErrHTTPSessionInvalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrHTTPSessionInvalid
	}
	var claims httpSessionClaims
	if err := json.Unmarshal(payload, &claims); err != nil || claims.CookieVersion == 0 {
		return nil, ErrHTTPSessionInvalid
	}
	key := s.cfg.CookieKeyRing[claims.CookieVersion]
	if len(key) == 0 {
		return nil, ErrHTTPSessionInvalid
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(parts[0]))
	got, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(got, mac.Sum(nil)) {
		return nil, ErrHTTPSessionInvalid
	}
	issuedAtMS, expiresAtMS := claims.IssuedAtMS, claims.ExpiresAtMS
	if issuedAtMS == 0 && claims.LegacyIssuedAtUnix > 0 {
		issuedAtMS = claims.LegacyIssuedAtUnix * 1000
	}
	if expiresAtMS == 0 && claims.LegacyExpiresAtUnix > 0 {
		expiresAtMS = claims.LegacyExpiresAtUnix * 1000
	}
	if issuedAtMS <= 0 || expiresAtMS <= time.Now().UnixMilli() {
		return nil, ErrHTTPSessionInvalid
	}
	playerID, err := strconv.ParseUint(claims.PlayerID, 10, 64)
	if err != nil || playerID == 0 || claims.SessionSubject == "" {
		return nil, ErrHTTPSessionInvalid
	}
	authorityEpoch, err := strconv.ParseUint(claims.SessionAuthorityEpoch, 10, 64)
	if err != nil {
		return nil, ErrHTTPSessionInvalid
	}
	currentAuthority, err := s.authority.PlayerAuthorityEpoch(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if authorityEpoch != currentAuthority {
		return nil, ErrHTTPSessionInvalid
	}
	return &SessionBinding{
		PlayerID:              playerID,
		SessionID:             claims.SessionSubject,
		SessionAuthorityEpoch: authorityEpoch,
	}, nil
}
