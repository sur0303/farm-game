// Package auth 实现 farm-gate 登录模块:HTTPS 测试登录 → 会话代次 → 建档 → 一次性票据。
//
// S2 主干(见 docs/design/05-system-skeleton.md §8.1 与 41-sequences.md §1):
//
//	H5 HTTPS 登录(平台票据)
//	→ SessionAuthority 提升/建立 active_session_epoch
//	→ BootstrapService.EnsureOwnerInitialized(player_id, bootstrap_id)
//	→ 返回一次性 connection_ticket + WSS 地址
//	→ H5 用 ticket 经 TCONND/gateproxy 发 ClientHello 完成 WSS 认证
//
// 为保持可测与可替换,平台票据校验通过 PlatformVerifier 注入;S2 仅用测试实现。
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/session"
	"farmgame/internal/permitsecret"

	publicpb "farmgame/api/gen/public"
)

func newGatewayBootID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(value[:])
}

func deriveCookieSecret(permitSecret []byte) []byte {
	mac := hmac.New(sha256.New, permitSecret)
	_, _ = mac.Write([]byte("farm-game/http-cookie/v1"))
	return mac.Sum(nil)
}

const cookieSigningKeyMinBytes = 32

// ErrUnauthorized 表示平台票据校验失败。
var ErrUnauthorized = errors.New("auth: unauthorized platform token")

// PlatformVerifier 校验平台登录票据并返回稳定 player_id。
// S2 用测试实现;生产接入真实平台 SSO/票据校验。
type PlatformVerifier interface {
	Verify(ctx context.Context, platformToken string) (playerID uint64, err error)
}

// Bootstrapper lets farm-gate delegate durable owner initialization to a
// Player/Farm service. Gate must configure one before serving login traffic.
// The returned projection is the optional home-bootstrap view attached to the
// login response; nil means the backend did not attach one and the client must
// fall back to GET_PLAYER_BOOTSTRAP.
type Bootstrapper interface {
	EnsureOwnerInitialized(ctx context.Context, playerID uint64, bootstrapID string, initialRuleVersion int32) (*publicpb.GetPlayerBootstrapResult, error)
}

// ProfileBootstrapper is the profile-aware extension used by Web account
// login. Platform/bootstrap callers that do not carry a profile continue to
// use Bootstrapper and therefore receive the Player authority defaults.
type ProfileBootstrapper interface {
	EnsureOwnerInitializedWithProfile(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32, initialRuleVersion int32) (*publicpb.GetPlayerBootstrapResult, error)
}

// LoginRequest 是一次 HTTPS 测试登录请求。
type LoginRequest struct {
	PlatformToken string // 平台登录票据
	BootstrapID   string // 同一平台账号稳定的建档幂等键
	ClientBuild   string // H5 构建号(透传给 ServerHello)
}

// LoginResult 是登录成功后的响应,直接驱动 H5 发起 WSS 认证。
type LoginResult struct {
	PlayerID              uint64 `json:"player_id"`
	SessionID             string `json:"session_id"`
	SessionAuthorityEpoch uint64 `json:"session_authority_epoch"`
	ReservedSessionEpoch  uint64 `json:"reserved_session_epoch"`
	ActiveSessionEpoch    uint64 `json:"active_session_epoch"`
	ConnectionTicket      string `json:"connection_ticket"`
	WssEndpoint           string `json:"wss_endpoint"`
	TicketTTLSeconds      int    `json:"ticket_ttl_seconds"`
	// HomeBootstrap 是登录附带的可选首页投影(与 GET_PLAYER_BOOTSTRAP 同语义)。
	// 旧客户端/无投影时省略;Renew 永远不携带。客户端不得在收到合法投影后再次
	// 发送 GET_PLAYER_BOOTSTRAP(RecoveryRequery 仅在旧响应/Reconnect/跨日刷新
	// 时使用)。
	HomeBootstrap *publicpb.GetPlayerBootstrapResult `json:"home_bootstrap,omitempty"`
}

// MarshalJSON keeps uint64 identity/epoch values exact on the browser boundary;
// the HTTP contract uses decimal strings even though the internal Go fields stay uint64.
func (r LoginResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		PlayerID              string `json:"player_id"`
		SessionID             string `json:"session_id"`
		SessionAuthorityEpoch string `json:"session_authority_epoch"`
		ReservedSessionEpoch  string `json:"reserved_session_epoch"`
		ActiveSessionEpoch    string `json:"active_session_epoch"`
		ConnectionTicket      string `json:"connection_ticket"`
		WssEndpoint           string `json:"wss_endpoint"`
		TicketTTLSeconds      int    `json:"ticket_ttl_seconds"`
		HomeBootstrap         *publicpb.GetPlayerBootstrapResult `json:"home_bootstrap,omitempty"`
	}{
		PlayerID: strconv.FormatUint(r.PlayerID, 10), SessionID: r.SessionID,
		SessionAuthorityEpoch: strconv.FormatUint(r.SessionAuthorityEpoch, 10),
		ReservedSessionEpoch:  strconv.FormatUint(r.ReservedSessionEpoch, 10),
		ActiveSessionEpoch:    strconv.FormatUint(r.ActiveSessionEpoch, 10),
		ConnectionTicket:      r.ConnectionTicket, WssEndpoint: r.WssEndpoint,
		TicketTTLSeconds: r.TicketTTLSeconds, HomeBootstrap: r.HomeBootstrap,
	})
}

func (r *LoginResult) UnmarshalJSON(data []byte) error {
	var raw struct {
		PlayerID              json.RawMessage `json:"player_id"`
		SessionID             string          `json:"session_id"`
		SessionAuthorityEpoch json.RawMessage `json:"session_authority_epoch"`
		ReservedSessionEpoch  json.RawMessage `json:"reserved_session_epoch"`
		ActiveSessionEpoch    json.RawMessage `json:"active_session_epoch"`
		ConnectionTicket      string          `json:"connection_ticket"`
		WssEndpoint           string          `json:"wss_endpoint"`
		TicketTTLSeconds      int             `json:"ticket_ttl_seconds"`
		HomeBootstrap         *publicpb.GetPlayerBootstrapResult `json:"home_bootstrap"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var err error
	if r.PlayerID, err = parseUint64JSON(raw.PlayerID); err != nil {
		return err
	}
	if r.SessionAuthorityEpoch, err = parseUint64JSON(raw.SessionAuthorityEpoch); err != nil {
		return err
	}
	if r.ActiveSessionEpoch, err = parseUint64JSON(raw.ActiveSessionEpoch); err != nil {
		return err
	}
	if len(raw.ReservedSessionEpoch) > 0 {
		if r.ReservedSessionEpoch, err = parseUint64JSON(raw.ReservedSessionEpoch); err != nil {
			return err
		}
	} else {
		r.ReservedSessionEpoch = r.ActiveSessionEpoch
	}
	r.SessionID, r.ConnectionTicket, r.WssEndpoint, r.TicketTTLSeconds = raw.SessionID, raw.ConnectionTicket, raw.WssEndpoint, raw.TicketTTLSeconds
	r.HomeBootstrap = raw.HomeBootstrap
	return nil
}

func parseUint64JSON(raw json.RawMessage) (uint64, error) {
	var s string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		return strconv.ParseUint(s, 10, 64)
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, err
	}
	return n, nil
}

// Config 配置登录模块。
type Config struct {
	GatewayID          string
	WssEndpoint        string
	InitialRuleVersion int32
	TicketTTL          time.Duration
	HTTPSessionTTL     time.Duration
	RouteLeaseTTL      time.Duration
	GatewayBootID      string
	// PermitSecret is shared with the backend services so they can verify the signed
	// SessionPermit carried in TrustedCommandContext. It is never sent to H5.
	PermitSecret     []byte
	CookieSecret     []byte
	CookieKeyVersion uint32
	// CookieKeyRing contains the active and still-accepted historical HMAC
	// keys, indexed by the version carried inside the signed Cookie claims.
	// New Cookies always use CookieKeyVersion; removing an old entry ends its
	// rotation window without storing Cookie bodies server-side.
	CookieKeyRing map[uint32][]byte
}

// Service 编排登录:平台校验 → 会话代次 → 建档 → 签发一次性票据。
type Service struct {
	verifier  PlatformVerifier
	authority session.Authority
	tickets   *ticket.Service
	bootstrap Bootstrapper
	cfg       Config
}

// New 创建登录服务。verifier 必须非 nil;tickets/authority 为 nil 时使用内存实现。
func New(
	verifier PlatformVerifier,
	authority session.Authority,
	tickets *ticket.Service,
	cfg Config,
) (*Service, error) {
	if verifier == nil {
		return nil, errors.New("auth: platform verifier required")
	}
	if len(cfg.PermitSecret) == 0 {
		return nil, errors.New("auth: permit secret required")
	}
	if authority == nil {
		authority = session.New(session.Config{Secret: append([]byte(nil), cfg.PermitSecret...), AuthorityEpoch: 1})
	}
	if tickets == nil {
		tickets = ticket.New(nil)
	}
	if cfg.TicketTTL <= 0 {
		cfg.TicketTTL = ticket.DefaultTTL
	}
	if cfg.HTTPSessionTTL <= 0 {
		cfg.HTTPSessionTTL = DefaultHTTPSessionTTL
	}
	if cfg.RouteLeaseTTL <= 0 {
		cfg.RouteLeaseTTL = session.DefaultRouteLeaseTTL
	}
	if cfg.GatewayBootID == "" {
		cfg.GatewayBootID = newGatewayBootID()
	}
	if cfg.CookieKeyVersion == 0 {
		cfg.CookieKeyVersion = 1
	}
	if err := configureCookieKeyRing(&cfg); err != nil {
		return nil, err
	}
	return &Service{
		verifier:  verifier,
		authority: authority,
		tickets:   tickets,
		cfg:       cfg,
	}, nil
}

func configureCookieKeyRing(cfg *Config) error {
	if cfg == nil {
		return errors.New("auth: cookie config required")
	}
	ring := make(map[uint32][]byte)
	for version, key := range cfg.CookieKeyRing {
		if version == 0 {
			return fmt.Errorf("auth: cookie key version %d must contain at least %d bytes", version, cookieSigningKeyMinBytes)
		}
		if err := permitsecret.ValidateKeyMaterial(fmt.Sprintf("HTTP Cookie key version %d", version), key); err != nil {
			return fmt.Errorf("auth: invalid signing key configuration: %w", err)
		}
		ring[version] = append([]byte(nil), key...)
	}
	if len(ring) == 0 {
		key := cfg.CookieSecret
		if len(key) == 0 {
			key = deriveCookieSecret(cfg.PermitSecret)
		}
		if err := permitsecret.ValidateKeyMaterial("HTTP Cookie signing key", key); err != nil {
			return fmt.Errorf("auth: invalid signing key configuration: %w", err)
		}
		ring[cfg.CookieKeyVersion] = append([]byte(nil), key...)
	}
	active, ok := ring[cfg.CookieKeyVersion]
	if !ok {
		return fmt.Errorf("auth: active cookie key version %d is absent from key ring", cfg.CookieKeyVersion)
	}
	cfg.CookieSecret = append([]byte(nil), active...)
	cfg.CookieKeyRing = ring
	return nil
}

// IssuePermitToken signs a request-scoped permit for the internal Logic RPC.
// The opaque token is safe to put in the trusted RPC envelope; it is not a
// client credential and is never logged or returned by the HTTP login API.
func (s *Service) IssuePermitToken(ctx context.Context, b *SessionBinding, req session.PermitRequest) ([]byte, error) {
	p, err := s.IssuePermit(ctx, b, req)
	if err != nil {
		return nil, err
	}
	token, err := p.Token(s.cfg.PermitSecret)
	if err != nil {
		return nil, err
	}
	return []byte(token), nil
}

func (s *Service) GatewayID() string { return s.cfg.GatewayID }

// RevokeAllSessions advances the player's authority fence. Every previously
// issued HTTP Cookie and every active/old WebSocket binding becomes unable to
// obtain a new SessionPermit. The compare and revoke happen atomically inside
// SessionAuthority, so a stale concurrent request cannot revoke a newer epoch.
func (s *Service) RevokeAllSessions(ctx context.Context, binding *SessionBinding) error {
	if s == nil || binding == nil || binding.PlayerID == 0 || binding.SessionAuthorityEpoch == 0 {
		return ErrHTTPSessionInvalid
	}
	revoked, err := s.authority.RevokeIfCurrent(ctx, binding.PlayerID, binding.SessionAuthorityEpoch)
	if err != nil {
		return err
	}
	if !revoked {
		return ErrHTTPSessionInvalid
	}
	return nil
}

// RevokePlayerAuthority unconditionally advances the player's authority fence.
// Password changes and administrative disables must use this form so every
// concurrent login generation is invalidated, not only the caller's Cookie.
func (s *Service) RevokePlayerAuthority(ctx context.Context, playerID uint64) error {
	if s == nil || playerID == 0 {
		return ErrUnauthorized
	}
	return s.authority.Revoke(ctx, playerID)
}

// SetBootstrapper switches Login's durable initialization to the remote Logic
// service. It must be called during process construction, before serving traffic.
func (s *Service) SetBootstrapper(bootstrapper Bootstrapper) { s.bootstrap = bootstrapper }

// Login 执行一次完整登录流程,返回一次性连接票据与 WSS 地址。
//
// 任何一步失败都返回 error,绝不返回"假成功"的票据;Redis/Tcaplus 不可用时由底层 fail-closed。
func (s *Service) Login(ctx context.Context, req LoginRequest) (*LoginResult, error) {
	if req.PlatformToken == "" || req.BootstrapID == "" {
		return nil, ErrUnauthorized
	}
	playerID, err := s.verifier.Verify(ctx, req.PlatformToken)
	if err != nil {
		return nil, ErrUnauthorized
	}
	return s.LoginPlayer(ctx, playerID, req.BootstrapID, req.ClientBuild)
}

// LoginPlayer continues the normal session/bootstrap/ticket flow after an
// upstream identity boundary has already authenticated a stable player_id.
// Web username/password, WeChat and future platform SSO handlers may use this
// method; none of them may bypass SessionAuthority or owner bootstrap.
func (s *Service) LoginPlayer(ctx context.Context, playerID uint64, bootstrapID, _ string) (*LoginResult, error) {
	return s.loginPlayer(ctx, playerID, bootstrapID, "", 0)
}

// LoginPlayerWithProfile is the Web account/bootstrap path. The normalized
// account username is provisioned as the initial display name and avatar 1 is
// the backend-owned default; neither value is accepted from an owner-id or
// other client display string.
func (s *Service) LoginPlayerWithProfile(ctx context.Context, playerID uint64, bootstrapID, _, displayName string, avatarID uint32) (*LoginResult, error) {
	return s.loginPlayer(ctx, playerID, bootstrapID, displayName, avatarID)
}

func (s *Service) loginPlayer(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32) (*LoginResult, error) {
	if playerID == 0 || bootstrapID == "" {
		return nil, ErrUnauthorized
	}
	candidate, err := s.authority.Reserve(ctx, playerID, "")
	if err != nil {
		return nil, err
	}
	// 建档/恢复:幂等,并发只创建一次初始资产。
	if s.bootstrap == nil {
		return nil, errors.New("auth: bootstrapper required")
	}
	var homeBootstrap *publicpb.GetPlayerBootstrapResult
	if displayName != "" || avatarID != 0 {
		profileBootstrapper, ok := s.bootstrap.(ProfileBootstrapper)
		if !ok {
			return nil, errors.New("auth: profile-aware bootstrapper required")
		}
		projection, err := profileBootstrapper.EnsureOwnerInitializedWithProfile(ctx, playerID, bootstrapID, displayName, avatarID, s.cfg.InitialRuleVersion)
		if err != nil {
			return nil, err
		}
		homeBootstrap = projection
	} else {
		projection, err := s.bootstrap.EnsureOwnerInitialized(ctx, playerID, bootstrapID, s.cfg.InitialRuleVersion)
		if err != nil {
			return nil, err
		}
		homeBootstrap = projection
	}
	// 签发一次性票据,绑定会话代次(不绑定 IP/网关实例)。
	iss, err := s.tickets.Issue(ctx, ticket.Bound{
		PlayerID:              playerID,
		SessionID:             candidate.SessionID,
		SessionAuthorityEpoch: candidate.AuthorityEpoch,
		ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
	})
	if err != nil {
		return nil, err
	}
	return &LoginResult{
		PlayerID:              playerID,
		SessionID:             candidate.SessionID,
		SessionAuthorityEpoch: candidate.AuthorityEpoch,
		ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
		ActiveSessionEpoch:    candidate.ActiveSessionEpoch,
		ConnectionTicket:      iss.Token,
		WssEndpoint:           s.cfg.WssEndpoint,
		TicketTTLSeconds:      int(s.cfg.TicketTTL.Seconds()),
		HomeBootstrap:         homeBootstrap,
	}, nil
}

// Renew issues a fresh one-time connection ticket for the already authenticated
// HTTP session. It deliberately does not call Authority.Begin: a transient WSS
// reconnect must not fence the player's still-current session generation.
func (s *Service) Renew(ctx context.Context, binding *SessionBinding) (*LoginResult, error) {
	if binding == nil || binding.PlayerID == 0 || binding.SessionID == "" {
		return nil, ErrHTTPSessionInvalid
	}
	currentAuthority, err := s.authority.PlayerAuthorityEpoch(ctx, binding.PlayerID)
	if err != nil {
		return nil, err
	}
	if binding.SessionAuthorityEpoch != currentAuthority {
		return nil, ErrHTTPSessionInvalid
	}
	candidate, err := s.authority.Reserve(ctx, binding.PlayerID, binding.SessionID)
	if err != nil {
		return nil, err
	}
	issued, err := s.tickets.Issue(ctx, ticket.Bound{
		PlayerID:              binding.PlayerID,
		SessionID:             binding.SessionID,
		SessionAuthorityEpoch: binding.SessionAuthorityEpoch,
		ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
	})
	if err != nil {
		return nil, err
	}
	return &LoginResult{
		PlayerID:              binding.PlayerID,
		SessionID:             binding.SessionID,
		SessionAuthorityEpoch: binding.SessionAuthorityEpoch,
		ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
		ActiveSessionEpoch:    candidate.ActiveSessionEpoch,
		ConnectionTicket:      issued.Token,
		WssEndpoint:           s.cfg.WssEndpoint,
		TicketTTLSeconds:      int(s.cfg.TicketTTL.Seconds()),
	}, nil
}

// WssEndpoint 暴露配置的连接端点(供 handler 透传)。
func (s *Service) WssEndpoint() string { return s.cfg.WssEndpoint }
