package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/session"
)

// SessionBinding 是 ClientHello 核销成功后建立的连接身份,供后续命令鉴权与 Permit 签发。
type SessionBinding struct {
	PlayerID              uint64
	SessionID             string
	SessionAuthorityEpoch uint64
	ActiveSessionEpoch    uint64
	Route                 session.ActiveRoute
}

type ConnectionAuthentication struct {
	Binding               *SessionBinding
	OldRoute              *session.ActiveRoute
	AcceptedRouteLeaseTTL time.Duration
}

// ErrTicketInvalid 是票据核销失败的对外统一错误(映射 ERR_CONNECTION_TICKET_INVALID)。
var ErrTicketInvalid = ticket.ErrRedeemFailed

// Authenticate 处理 WSS ClientHello:原子核销一次性票据,返回连接身份。
//
// 合同见 docs/design/10-protocol.md §2.1 与 41-sequences.md §1:
//   - 先校验 protocol_version/connection_seq/client_build(由上层协议层完成)再核销;
//   - 核销只能成功一次,并发只一个成功;
//   - 失败后统一返回 ErrTicketInvalid,不向 H5 暴露更细原因,并重登取得新票据。
func (s *Service) Authenticate(ctx context.Context, connectionTicket string) (*SessionBinding, error) {
	result, err := s.AuthenticateConnection(ctx, connectionTicket, session.RouteFence{
		GatewayID: s.cfg.GatewayID, GatewayBootID: s.cfg.GatewayBootID,
		ConnectionID: "compat-" + ticket.Hash(connectionTicket)[:12], ConnectionGeneration: 1,
	})
	if err != nil {
		return nil, err
	}
	return result.Binding, nil
}

// AuthenticateConnection atomically consumes a candidate ticket and activates
// the concrete Gate connection. HTTPS login/renew never calls this method.
func (s *Service) AuthenticateConnection(ctx context.Context, connectionTicket string, route session.RouteFence) (*ConnectionAuthentication, error) {
	bound, activated, err := s.tickets.Activate(ctx, s.authority, connectionTicket, route, s.cfg.RouteLeaseTTL)
	if err != nil {
		if errors.Is(err, ticket.ErrRedeemFailed) || errors.Is(err, ticket.ErrInvalidTicket) ||
			errors.Is(err, session.ErrActivationSuperseded) {
			return nil, ErrTicketInvalid
		}
		return nil, err
	}
	if activated == nil || activated.Session == nil {
		return nil, fmt.Errorf("auth: empty session activation")
	}
	binding := &SessionBinding{
		PlayerID: bound.PlayerID, SessionID: bound.SessionID,
		SessionAuthorityEpoch: bound.SessionAuthorityEpoch,
		ActiveSessionEpoch:    activated.Session.ActiveSessionEpoch,
		Route:                 activated.Route,
	}
	acceptedTTL := activated.AcceptedLeaseTTL
	if acceptedTTL <= 0 {
		// The ticket may already have advanced SessionRoute. Fail closed and
		// conditionally clear only that exact route rather than inventing a TTL
		// from local configuration.
		_, _ = s.authority.CompareAndClearRoute(ctx, activated.Route)
		return nil, errors.New("auth: activation omitted accepted route lease ttl")
	}
	return &ConnectionAuthentication{
		Binding: binding, OldRoute: activated.OldRoute,
		AcceptedRouteLeaseTTL: acceptedTTL,
	}, nil
}

// IssuePermit 为已认证连接签发绑定动作的短时 SessionPermit(Logic 侧校验)。
func (s *Service) IssuePermit(ctx context.Context, b *SessionBinding, req session.PermitRequest) (*session.Permit, error) {
	sess := &session.Session{
		PlayerID:           b.PlayerID,
		SessionID:          b.SessionID,
		AuthorityEpoch:     b.SessionAuthorityEpoch,
		ActiveSessionEpoch: b.ActiveSessionEpoch,
		GatewayID:          s.cfg.GatewayID,
	}
	return s.authority.Permit(ctx, sess, req)
}

func (s *Service) CompareAndClearRoute(ctx context.Context, binding *SessionBinding) (bool, error) {
	if binding == nil {
		return false, nil
	}
	return s.authority.CompareAndClearRoute(ctx, binding.Route)
}

func (s *Service) GatewayBootID() string        { return s.cfg.GatewayBootID }
func (s *Service) RouteLeaseTTL() time.Duration { return s.cfg.RouteLeaseTTL }

func (s *Service) RenewRoutes(ctx context.Context, routes []session.ActiveRoute) ([]session.RouteRenewal, error) {
	return s.authority.RenewRoutes(ctx, routes, s.cfg.RouteLeaseTTL)
}
