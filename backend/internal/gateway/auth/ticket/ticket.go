// Package ticket 实现一次性 WSS 连接票据(connection_ticket)。
//
// 合同见 docs/design/10-protocol.md §2.1 与 30-redis.md §4:
//   - 服务端生成 32 字节密码学安全随机值,对外给 H5 的值为无 padding base64url string;
//   - 存储只保存 SHA256(ticket),不保存原始票据;
//   - 默认 TTL 30s,只可原子核销一次(GETDEL / Lua);
//   - 票据绑定 player_id / session_id / session_authority_epoch / active_session_epoch,
//     不绑定 IP、User-Agent、特定 TCONND 或 farm-gate 实例;
//   - Redis 不可用时 fail-closed,不得信任 H5 自报 player_id。
package ticket

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/gateway/session"
)

// DefaultTTL 是票据默认存活时间。
const DefaultTTL = 30 * time.Second

// DefaultTicketBytes 是票据随机字节长度(C 系冻结:32 字节)。
const DefaultTicketBytes = 32

// ErrInvalidTicket 表示票据格式非法(长度/编码)。
var ErrInvalidTicket = errors.New("ticket: invalid ticket format")

// ErrRedeemFailed 是核销失败的统称(ticket 不存在/过期/已核销/代次失效)。
// 对 H5 统一映射 ERR_CONNECTION_TICKET_INVALID(不暴露更细原因)。
var ErrRedeemFailed = errors.New("ticket: redeem failed")

// Bound 是票据核销成功后返回给 farm-gate 的绑定信息,用于建立连接身份。
type Bound struct {
	PlayerID              uint64
	SessionID             string
	SessionAuthorityEpoch uint64
	ReservedSessionEpoch  uint64
	// ActiveSessionEpoch is retained while old callers migrate. New tickets
	// carry ReservedSessionEpoch and do not become active during HTTPS login.
	ActiveSessionEpoch uint64
	IssuedAt           time.Time
	ExpiresAt          time.Time
}

// Issued 是 Issue 的返回,包含可下发给 H5 的 token 与内部绑定信息。
type Issued struct {
	Token string // 无 padding base64url string,直接给 H5
	Bound Bound
}

// Store 是票据存储后端接口。实现必须保证 Redeem 对同一 token 仅成功一次(原子)。
type Store interface {
	// Save 原子保存 token 的 SHA256 绑定,带 TTL;同 token 重复 Save 覆盖即可。
	Save(ctx context.Context, token string, bound Bound, ttl time.Duration) error
	// Redeem 原子核销 token:成功返回绑定信息,失败(不存在/过期/已用)返回 ErrRedeemFailed。
	Redeem(ctx context.Context, token string) (Bound, error)
}

// Service 签发与核销一次性连接票据。
type Service struct {
	store    Store
	ttl      time.Duration
	randFunc func([]byte) (int, error) // 可注入以测试确定性
}

// ActivationStore can atomically consume a ticket and activate SessionRoute.
// RedisStore implements this with two keys in the same Redis Cluster hash slot.
type ActivationStore interface {
	Activate(ctx context.Context, token string, route session.RouteFence, leaseTTL time.Duration) (Bound, *session.ActivationResult, error)
}

// Option 用于定制 Service。
type Option func(*Service)

// WithTTL 设置票据 TTL(<=0 用 DefaultTTL)。
func WithTTL(ttl time.Duration) Option {
	return func(s *Service) { s.ttl = ttl }
}

// WithRand 注入随机源(测试用)。
func WithRand(fn func([]byte) (int, error)) Option {
	return func(s *Service) { s.randFunc = fn }
}

// New 创建票据服务。store 为 nil 时使用内存实现。
func New(store Store, opts ...Option) *Service {
	if store == nil {
		store = NewMemoryStore()
	}
	s := &Service{store: store, ttl: DefaultTTL, randFunc: rand.Read}
	for _, o := range opts {
		o(s)
	}
	if s.ttl <= 0 {
		s.ttl = DefaultTTL
	} else if s.ttl < time.Millisecond {
		// Redis expiration is millisecond-granular. Normalize at the service
		// boundary so expires_at and the physical key TTL cannot disagree.
		s.ttl = time.Millisecond
	}
	return s
}

// Issue 生成并保存一次性票据,返回可下发给 H5 的 token 与绑定信息。
func (s *Service) Issue(ctx context.Context, bound Bound) (*Issued, error) {
	buf := make([]byte, DefaultTicketBytes)
	if _, err := s.randFunc(buf); err != nil {
		return nil, err
	}
	if bound.PlayerID == 0 {
		return nil, ErrInvalidTicket
	}
	token := fmt.Sprintf("v1.%d.%s", bound.PlayerID, base64.RawURLEncoding.EncodeToString(buf))
	now := time.Now()
	bound.IssuedAt = now
	bound.ExpiresAt = now.Add(s.ttl)
	if err := s.store.Save(ctx, token, bound, s.ttl); err != nil {
		return nil, err
	}
	return &Issued{Token: token, Bound: bound}, nil
}

// Activate is the ClientHello linearization point. Redis performs
// ticket-consume + active route replacement in one script. In-memory stores
// retain equivalent single-process semantics by atomically redeeming first and
// then activating the in-memory authority.
func (s *Service) Activate(ctx context.Context, authority session.Authority, token string, route session.RouteFence, leaseTTL time.Duration) (Bound, *session.ActivationResult, error) {
	if !ValidTokenFormat(token) || authority == nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	if store, ok := s.store.(ActivationStore); ok {
		bound, result, err := store.Activate(ctx, token, route, leaseTTL)
		if err != nil {
			return Bound{}, nil, err
		}
		// Redis 激活脚本直接提升 active_epoch(不经 authority.Activate),
		// 必须显式失效本地 epoch 缓存,否则 Permit 会拿旧值比较而误报 stale。
		authority.InvalidateEpochCache(bound.PlayerID)
		return bound, result, nil
	}
	bound, err := s.Redeem(ctx, token)
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	reserved := bound.ReservedSessionEpoch
	if reserved == 0 {
		reserved = bound.ActiveSessionEpoch
	}
	result, err := authority.Activate(ctx, session.ActivationRequest{
		PlayerID: bound.PlayerID, SessionID: bound.SessionID,
		AuthorityEpoch: bound.SessionAuthorityEpoch, ReservedSessionEpoch: reserved,
		Route: route, LeaseTTL: leaseTTL,
	})
	if err != nil {
		return Bound{}, nil, err
	}
	return bound, result, nil
}

// Redeem 原子核销票据。成功返回绑定信息;任何失败都返回 ErrRedeemFailed。
func (s *Service) Redeem(ctx context.Context, token string) (Bound, error) {
	if !ValidTokenFormat(token) {
		return Bound{}, ErrInvalidTicket
	}
	b, err := s.store.Redeem(ctx, token)
	if err != nil {
		return Bound{}, ErrRedeemFailed
	}
	return b, nil
}

// ValidTokenFormat 校验 token 是合法的 base64url(RawURLEncoding)且长度对应 32 字节。
func ValidTokenFormat(token string) bool {
	if token == "" {
		return false
	}
	randomPart := token
	if strings.HasPrefix(token, "v1.") {
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			return false
		}
		playerID, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || playerID == 0 {
			return false
		}
		randomPart = parts[2]
	}
	buf, err := base64.RawURLEncoding.DecodeString(randomPart)
	if err != nil {
		return false
	}
	return len(buf) == DefaultTicketBytes
}

// PlayerIDFromToken returns the non-secret routing hint embedded in v1
// tickets. It is used only to select the player's Redis Cluster slot; the
// ticket record remains authoritative for identity.
func PlayerIDFromToken(token string) (uint64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return 0, ErrInvalidTicket
	}
	playerID, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || playerID == 0 {
		return 0, ErrInvalidTicket
	}
	return playerID, nil
}

// Hash 返回 token 的 SHA256 十六进制串,存储键只使用此值,不保存原文。
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
