package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// DefaultSessionTTL 是会话 hash 的默认存活(可活跃续期)。
const DefaultSessionTTL = 2 * time.Hour

// DefaultRouteLeaseTTL bounds how long a Gate may admit new commands without
// successfully renewing its complete SessionRoute fence.
const DefaultRouteLeaseTTL = 30 * time.Second

// DefaultPermitTTL 是单条 SessionPermit 的默认存活。
const DefaultPermitTTL = 30 * time.Second

// MaxBatchGetActiveRoutes bounds a read-only route lookup. The limit applies
// to the caller-visible player set after stable first-seen de-duplication.
const MaxBatchGetActiveRoutes = 300

// MaxActiveRouteBatch is kept as a descriptive alias for callers that name
// the operation rather than the method.
const MaxActiveRouteBatch = MaxBatchGetActiveRoutes

// ErrInvalidActiveRouteBatch means a route lookup cannot be bounded or mapped
// deterministically. Missing/expired individual routes are not errors: the
// read-only resolver omits them so best-effort fan-out can continue.
var ErrInvalidActiveRouteBatch = errors.New("session: invalid active route batch")

// ErrStaleSession 表示请求基于的 active_session_epoch 已被新登录作废。
var ErrStaleSession = errors.New("session: session epoch stale, re-login required")

// ErrActivationSuperseded means a newer candidate session has already become
// active. A delayed ClientHello must not move the player back to an older
// session epoch.
var ErrActivationSuperseded = errors.New("session: connection attempt superseded")

// Candidate is allocated by HTTPS authentication. It is deliberately not an
// active WSS session until ClientHello redeems its ConnectionTicket.
type Candidate struct {
	PlayerID             uint64
	SessionID            string
	AuthorityEpoch       uint64
	ReservedSessionEpoch uint64
	ActiveSessionEpoch   uint64
}

// RouteFence identifies one concrete transport connection. Every asynchronous
// disconnect, kick and lease operation must compare the complete fence.
type RouteFence struct {
	GatewayID            string
	GatewayBootID        string
	ConnectionID         string
	ConnectionGeneration uint64
}

// ActiveRoute is the Redis-authoritative location of the player's active WSS.
type ActiveRoute struct {
	RouteFence
	PlayerID       uint64
	SessionID      string
	SessionEpoch   uint64
	RouteRevision  uint64
	LeaseExpiresAt time.Time
}

// SessionRouteResolver is the read-only route seam used by targeted
// notifications. Implementations must not renew, clear, or otherwise mutate a
// SessionRoute while resolving it. The returned map contains only routes whose
// complete connection fence is present and whose lease is still valid.
type SessionRouteResolver interface {
	BatchGetActiveRoutes(context.Context, []uint64) (map[uint64]ActiveRoute, error)
}

func normalizeActiveRouteIDs(playerIDs []uint64) ([]uint64, error) {
	if len(playerIDs) == 0 || len(playerIDs) > MaxBatchGetActiveRoutes {
		return nil, ErrInvalidActiveRouteBatch
	}
	ids := make([]uint64, 0, len(playerIDs))
	seen := make(map[uint64]struct{}, len(playerIDs))
	for _, playerID := range playerIDs {
		if playerID == 0 {
			return nil, ErrInvalidActiveRouteBatch
		}
		if _, exists := seen[playerID]; exists {
			continue
		}
		seen[playerID] = struct{}{}
		ids = append(ids, playerID)
	}
	if len(ids) == 0 {
		return nil, ErrInvalidActiveRouteBatch
	}
	return ids, nil
}

func validActiveRoute(route ActiveRoute, playerID uint64, now time.Time) bool {
	return route.PlayerID == playerID &&
		route.SessionID != "" &&
		route.GatewayID != "" &&
		route.GatewayBootID != "" &&
		route.ConnectionID != "" &&
		route.ConnectionGeneration != 0 &&
		route.SessionEpoch != 0 &&
		route.RouteRevision != 0 &&
		!route.LeaseExpiresAt.IsZero() &&
		route.LeaseExpiresAt.After(now)
}

// ActivationRequest is produced only after a ConnectionTicket has been
// validated. ReservedSessionEpoch came from the HTTPS candidate attempt.
type ActivationRequest struct {
	PlayerID             uint64
	SessionID            string
	AuthorityEpoch       uint64
	ReservedSessionEpoch uint64
	Route                RouteFence
	LeaseTTL             time.Duration
}

// ActivationResult is returned by the linearization point used by ClientHello.
type ActivationResult struct {
	Session          *Session
	Route            ActiveRoute
	OldRoute         *ActiveRoute
	AcceptedLeaseTTL time.Duration
}

type RouteRenewal struct {
	Route        ActiveRoute
	Renewed      bool
	LeaseExpired bool
	// Transient means this item's command/result was unknown or malformed while
	// other pipeline items may still have completed. It never extends trust.
	Transient        bool
	AcceptedLeaseTTL time.Duration
}

// Session 是一次活跃会话的视图,由 Authority 签发与维护。
type Session struct {
	PlayerID           uint64
	SessionID          string
	AuthorityEpoch     uint64
	ActiveSessionEpoch uint64
	GatewayID          string
	ExpiresAt          time.Time
}

// PermitRequest 是颁发 Permit 的参数。
type PermitRequest struct {
	GatewayID            string
	GatewayBootID        string
	ConnectionID         string
	ConnectionGeneration uint64
	RouteRevision        uint64
	RequestID            string
	Action               Action
	OperationID          string // 仅 ActionOwnerWrite 需要
	TTL                  time.Duration
}

// Authority manages candidate allocation, ClientHello activation and Permit
// issuance. HTTPS Reserve must never advance active_session_epoch.
type Authority interface {
	// Reserve allocates a monotonically increasing candidate epoch without
	// changing the currently active WSS. sessionID may be empty for a new HTTP
	// login subject, or supplied by Cookie renew.
	Reserve(ctx context.Context, playerID uint64, sessionID string) (*Candidate, error)
	// Activate is the ClientHello linearization point for non-Redis/in-process
	// stores. Redis ticket stores use the equivalent atomic ticket+route script.
	Activate(ctx context.Context, req ActivationRequest) (*ActivationResult, error)
	// CompareAndClearRoute removes a route only when every fence still matches.
	CompareAndClearRoute(ctx context.Context, route ActiveRoute) (bool, error)
	// RenewRoutes extends leases only for routes whose complete fences still
	// match. Implementations should batch/pipeline remote writes.
	RenewRoutes(ctx context.Context, routes []ActiveRoute, leaseTTL time.Duration) ([]RouteRenewal, error)
	// PlayerAuthorityEpoch fences a player's HTTP Cookie. It is separate from
	// active_session_epoch so replacing a WSS does not invalidate the Cookie.
	PlayerAuthorityEpoch(ctx context.Context, playerID uint64) (uint64, error)

	// InvalidateEpochCache drops any locally cached epoch for playerID.
	// Call after a session epoch changes (ClientHello activation / revoke) so
	// Permit never compares against a stale cached value.
	InvalidateEpochCache(playerID uint64)

	// Begin is retained for compatibility with older tests/tools. New login code
	// must use Reserve and activate only from ClientHello.
	Begin(ctx context.Context, playerID uint64, gatewayID string) (*Session, error)
	// ActiveEpoch 返回玩家当前 active_session_epoch。
	ActiveEpoch(ctx context.Context, playerID uint64) (uint64, error)
	// AuthorityEpoch 返回全局会话权威代次(灾难切换时整体作废)。
	AuthorityEpoch(ctx context.Context) (uint64, error)
	// Revoke unconditionally advances both the player authority fence and
	// active session fence. Administrative account-disable/password-reset
	// paths may use it when they do not start from an authenticated Cookie.
	Revoke(ctx context.Context, playerID uint64) error
	// RevokeIfCurrent performs an atomic compare-and-revoke. It returns false
	// without changing state when expectedAuthorityEpoch is already stale.
	// Authenticated logout-all must use this form so a delayed old request
	// cannot revoke a newer login generation.
	RevokeIfCurrent(ctx context.Context, playerID, expectedAuthorityEpoch uint64) (bool, error)
	// Permit 基于当前会话签发布绑动作的短时 Permit。
	Permit(ctx context.Context, s *Session, req PermitRequest) (*Permit, error)
}

// Config 配置 Authority 的密钥与 TTL。
type Config struct {
	// Secret 用于 HMAC 签名 Permit;多 gateway 必须共享同一 secret。
	Secret []byte
	// AuthorityEpoch 是会话权威代次初始值(S2 固定测试值,默认 1)。
	AuthorityEpoch uint64
	// PermitTTL 单条 Permit 存活;<=0 用 DefaultPermitTTL。
	PermitTTL time.Duration
	// EpochCacheTTL 本地缓存按玩家 active/authority epoch 的时长;<=0 禁用。
	// 非零值会放宽跨 Gate 顶号后的即时 Fence：旧 Gate 在缓存窗口内仍可能
	// 签发自洽 Permit，而 farm/player-svr 只校验 Permit 签名和绑定字段，不会
	// 再读取 Redis Authority。生产默认必须为 0；仅隔离压测可显式开启。
	// 全局 SessionAuthorityEpoch 不走这个 TTL：它是灾备围栏，进程内缓存到退出。
	EpochCacheTTL time.Duration
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 极端情况下退化(理论不会发生)。
		return hex.EncodeToString([]byte{byte(time.Now().Nanosecond())})
	}
	return hex.EncodeToString(b[:])
}

// memoryAuthority 是 Authority 的内存实现,真实模拟代次线性化与顶号。
// 用于单测、离线开发与本地链路;多节点需 Redis 实现保证代次跨实例线性化。
type memoryAuthority struct {
	cfg Config
	mu  sync.Mutex
	// 全局权威代次。
	authorityEpoch uint64
	// 每玩家的 active_session_epoch。
	active map[uint64]uint64
	// 每玩家已分配的最大候选 session epoch。
	next map[uint64]uint64
	// 每玩家 Cookie/账号撤销代次。
	playerAuthority map[uint64]uint64
	// 每玩家当前会话视图(顶号后失效)。
	sessions map[uint64]*Session
	routes   map[uint64]ActiveRoute
}

// New 创建内存 Authority。未提供 Secret 时生成仅属于本实例的随机密钥；
// 多实例必须显式共享经过启动策略校验的 Secret。
func New(cfg Config) Authority {
	if len(cfg.Secret) == 0 {
		cfg.Secret = make([]byte, 32)
		if _, err := rand.Read(cfg.Secret); err != nil {
			panic("session: generate in-memory permit secret: " + err.Error())
		}
	}
	if cfg.PermitTTL <= 0 {
		cfg.PermitTTL = DefaultPermitTTL
	}
	return &memoryAuthority{
		cfg:             cfg,
		authorityEpoch:  cfg.AuthorityEpoch,
		active:          make(map[uint64]uint64),
		next:            make(map[uint64]uint64),
		playerAuthority: make(map[uint64]uint64),
		sessions:        make(map[uint64]*Session),
		routes:          make(map[uint64]ActiveRoute),
	}
}

func (a *memoryAuthority) Reserve(_ context.Context, playerID uint64, sessionID string) (*Candidate, error) {
	if playerID == 0 {
		return nil, errors.New("session: player_id required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.authorityEpoch = maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch)
	if a.playerAuthority[playerID] < a.authorityEpoch {
		a.playerAuthority[playerID] = a.authorityEpoch
	}
	a.next[playerID] = maxU64(a.next[playerID], a.active[playerID]) + 1
	if sessionID == "" {
		sessionID = newSessionID()
	}
	return &Candidate{
		PlayerID: playerID, SessionID: sessionID,
		AuthorityEpoch:       a.playerAuthority[playerID],
		ReservedSessionEpoch: a.next[playerID],
		ActiveSessionEpoch:   a.active[playerID],
	}, nil
}

func (a *memoryAuthority) Activate(_ context.Context, req ActivationRequest) (*ActivationResult, error) {
	if req.PlayerID == 0 || req.SessionID == "" || req.ReservedSessionEpoch == 0 ||
		req.Route.GatewayID == "" || req.Route.GatewayBootID == "" ||
		req.Route.ConnectionID == "" || req.Route.ConnectionGeneration == 0 {
		return nil, errors.New("session: invalid activation request")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	currentAuthority := maxU64(a.playerAuthority[req.PlayerID], maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch))
	if req.AuthorityEpoch != currentAuthority || req.ReservedSessionEpoch <= a.active[req.PlayerID] {
		return nil, ErrActivationSuperseded
	}
	var oldRoute *ActiveRoute
	if old, ok := a.routes[req.PlayerID]; ok {
		copy := old
		oldRoute = &copy
	}
	ttl := req.LeaseTTL
	if ttl <= 0 {
		ttl = DefaultRouteLeaseTTL
	}
	revision := uint64(1)
	if oldRoute != nil {
		revision = oldRoute.RouteRevision + 1
	}
	s := &Session{
		PlayerID:           req.PlayerID,
		SessionID:          req.SessionID,
		AuthorityEpoch:     req.AuthorityEpoch,
		ActiveSessionEpoch: req.ReservedSessionEpoch,
		GatewayID:          req.Route.GatewayID,
		ExpiresAt:          time.Now().Add(DefaultSessionTTL),
	}
	a.active[req.PlayerID] = req.ReservedSessionEpoch
	a.next[req.PlayerID] = maxU64(a.next[req.PlayerID], req.ReservedSessionEpoch)
	a.sessions[req.PlayerID] = s
	route := ActiveRoute{
		RouteFence: req.Route, PlayerID: req.PlayerID, SessionID: req.SessionID,
		SessionEpoch: req.ReservedSessionEpoch, RouteRevision: revision,
		LeaseExpiresAt: time.Now().Add(ttl),
	}
	a.routes[req.PlayerID] = route
	return &ActivationResult{
		Session: s, Route: route, OldRoute: oldRoute, AcceptedLeaseTTL: ttl,
	}, nil
}

func (a *memoryAuthority) CompareAndClearRoute(_ context.Context, expected ActiveRoute) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current, ok := a.routes[expected.PlayerID]
	if !ok || !sameRouteFence(current, expected) {
		return false, nil
	}
	delete(a.routes, expected.PlayerID)
	return true, nil
}

func (a *memoryAuthority) BatchGetActiveRoutes(ctx context.Context, playerIDs []uint64) (map[uint64]ActiveRoute, error) {
	ids, err := normalizeActiveRouteIDs(playerIDs)
	if err != nil {
		return nil, err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	routes := make(map[uint64]ActiveRoute, len(ids))
	for _, playerID := range ids {
		route, ok := a.routes[playerID]
		if ok && validActiveRoute(route, playerID, now) {
			routes[playerID] = route
		}
	}
	return routes, nil
}
func (a *memoryAuthority) RenewRoutes(_ context.Context, routes []ActiveRoute, leaseTTL time.Duration) ([]RouteRenewal, error) {
	if leaseTTL <= 0 {
		leaseTTL = DefaultRouteLeaseTTL
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	expiresAt := now.Add(leaseTTL)
	results := make([]RouteRenewal, 0, len(routes))
	for _, expected := range routes {
		result := RouteRenewal{Route: expected}
		current, ok := a.routes[expected.PlayerID]
		if ok && sameRouteFence(current, expected) {
			if current.LeaseExpiresAt.IsZero() || !now.Before(current.LeaseExpiresAt) {
				result.Route = current
				result.LeaseExpired = true
				results = append(results, result)
				continue
			}
			current.LeaseExpiresAt = expiresAt
			a.routes[expected.PlayerID] = current
			result.Route = current
			result.Renewed = true
			result.AcceptedLeaseTTL = leaseTTL
		}
		results = append(results, result)
	}
	return results, nil
}

func sameRouteFence(a, b ActiveRoute) bool {
	return a.PlayerID == b.PlayerID &&
		a.GatewayID == b.GatewayID && a.GatewayBootID == b.GatewayBootID &&
		a.ConnectionID == b.ConnectionID && a.ConnectionGeneration == b.ConnectionGeneration &&
		a.SessionEpoch == b.SessionEpoch && a.RouteRevision == b.RouteRevision
}

func (a *memoryAuthority) PlayerAuthorityEpoch(_ context.Context, playerID uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maxU64(a.playerAuthority[playerID], maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch)), nil
}

// InvalidateEpochCache is a no-op for the in-memory authority (no cache).
func (a *memoryAuthority) InvalidateEpochCache(uint64) {}

func (a *memoryAuthority) Begin(ctx context.Context, playerID uint64, gatewayID string) (*Session, error) {
	candidate, err := a.Reserve(ctx, playerID, "")
	if err != nil {
		return nil, err
	}
	activated, err := a.Activate(ctx, ActivationRequest{
		PlayerID: candidate.PlayerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route:    RouteFence{GatewayID: gatewayID, GatewayBootID: "legacy", ConnectionID: newSessionID(), ConnectionGeneration: 1},
		LeaseTTL: DefaultSessionTTL,
	})
	if err != nil {
		return nil, err
	}
	return activated.Session, nil
}

func (a *memoryAuthority) ActiveEpoch(_ context.Context, playerID uint64) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active[playerID], nil
}

func (a *memoryAuthority) AuthorityEpoch(_ context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch), nil
}

func (a *memoryAuthority) Revoke(_ context.Context, playerID uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revokeLocked(playerID)
	return nil
}

func (a *memoryAuthority) RevokeIfCurrent(_ context.Context, playerID, expectedAuthorityEpoch uint64) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := maxU64(a.playerAuthority[playerID], maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch))
	if playerID == 0 || expectedAuthorityEpoch == 0 || current != expectedAuthorityEpoch {
		return false, nil
	}
	a.revokeLocked(playerID)
	return true, nil
}

func (a *memoryAuthority) revokeLocked(playerID uint64) {
	a.playerAuthority[playerID] = maxU64(a.playerAuthority[playerID], maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch)) + 1
	a.next[playerID] = maxU64(a.next[playerID], a.active[playerID]) + 1
	a.active[playerID] = a.next[playerID]
	delete(a.sessions, playerID)
	delete(a.routes, playerID)
}

func (a *memoryAuthority) Permit(_ context.Context, s *Session, req PermitRequest) (*Permit, error) {
	if req.Action.NeedsOperationBinding() && req.OperationID == "" {
		return nil, errors.New("session: owner_write permit requires operation_id")
	}
	a.mu.Lock()
	currentActive := a.active[s.PlayerID]
	currentAuthority := maxU64(a.playerAuthority[s.PlayerID], maxU64(a.authorityEpoch, a.cfg.AuthorityEpoch))
	a.mu.Unlock()
	if s.ActiveSessionEpoch == 0 || s.ActiveSessionEpoch != currentActive || s.AuthorityEpoch != currentAuthority {
		return nil, ErrStaleSession
	}
	ttl := a.cfg.PermitTTL
	if req.TTL > 0 {
		ttl = req.TTL
	}
	p := &Permit{
		PlayerID:             s.PlayerID,
		SessionID:            s.SessionID,
		AuthorityEpoch:       s.AuthorityEpoch,
		ActiveSessionEpoch:   s.ActiveSessionEpoch,
		GatewayID:            req.GatewayID,
		GatewayBootID:        req.GatewayBootID,
		ConnectionID:         req.ConnectionID,
		ConnectionGeneration: req.ConnectionGeneration,
		RouteRevision:        req.RouteRevision,
		RequestID:            req.RequestID,
		Action:               req.Action,
		OperationID:          req.OperationID,
		ExpiresAt:            time.Now().Add(ttl),
	}
	p.Sign(a.cfg.Secret)
	return p, nil
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
