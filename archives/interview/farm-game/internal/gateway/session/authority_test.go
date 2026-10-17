package session

import (
	"context"
	"testing"
	"time"
)

func newAuth() Authority {
	return New(Config{Secret: []byte("test-secret"), AuthorityEpoch: 1, PermitTTL: time.Minute})
}

func TestBeginRaisesEpoch(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	s1, err := a.Begin(ctx, 1001, "gw-1")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if s1.ActiveSessionEpoch != 1 || s1.AuthorityEpoch != 1 {
		t.Fatalf("first session epoch wrong: %+v", s1)
	}
	s2, _ := a.Begin(ctx, 1001, "gw-2")
	if s2.ActiveSessionEpoch != 2 {
		t.Fatalf("second begin should raise epoch to 2, got %d", s2.ActiveSessionEpoch)
	}
	if s2.AuthorityEpoch != 1 {
		t.Fatalf("authority epoch unchanged: %d", s2.AuthorityEpoch)
	}
	if s2.SessionID == s1.SessionID {
		t.Fatalf("new login must mint a new session id")
	}
}

func TestKickInvalidatesOldPermit(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	s, _ := a.Begin(ctx, 1001, "gw-1")
	// 旧连接持有的 Permit。
	oldPermit, _ := a.Permit(ctx, s, PermitRequest{
		ConnectionID: "conn-old", RequestID: "r1", Action: ActionReadOnly,
	})
	tok, err := oldPermit.Token([]byte("test-secret"))
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	// 顶号发生。
	if err := a.Revoke(ctx, 1001); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	// 即使旧 token 的签名仍有效,其 active epoch 已落后于最新代次 → 校验失败。
	parsed, err := ParseToken(tok, []byte("test-secret"))
	if err != nil {
		t.Fatalf("parse should still succeed (good sig): %v", err)
	}
	authEpoch, _ := a.AuthorityEpoch(ctx)
	actEpoch, _ := a.ActiveEpoch(ctx, 1001)
	if err := parsed.Validate(time.Now(), []byte("test-secret"), authEpoch, actEpoch); err != ErrPermitEpochStale {
		t.Fatalf("kicked permit want ErrPermitEpochStale, got %v", err)
	}
}

func TestRevokeIfCurrentDoesNotRevokeNewerAuthority(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	first, err := a.Reserve(ctx, 1001, "")
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := a.RevokeIfCurrent(ctx, 1001, first.AuthorityEpoch)
	if err != nil || !revoked {
		t.Fatalf("initial conditional revoke = %v, %v", revoked, err)
	}
	newer, err := a.Reserve(ctx, 1001, "")
	if err != nil {
		t.Fatal(err)
	}
	revoked, err = a.RevokeIfCurrent(ctx, 1001, first.AuthorityEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if revoked {
		t.Fatal("stale authority epoch revoked a newer generation")
	}
	current, err := a.PlayerAuthorityEpoch(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if current != newer.AuthorityEpoch {
		t.Fatalf("stale revoke changed authority: current=%d newer=%d", current, newer.AuthorityEpoch)
	}
}

func TestPermitCanBeValidatedByAnyLogicWithSharedSecret(t *testing.T) {
	ctx := context.Background()
	secret := []byte("shared-secret")
	gwA := New(Config{Secret: secret, AuthorityEpoch: 1})
	s, _ := gwA.Begin(ctx, 2002, "gw-A")
	p, _ := gwA.Permit(ctx, s, PermitRequest{
		GatewayID: "gw-A", GatewayBootID: "boot-A",
		ConnectionID: "c", ConnectionGeneration: 7, RouteRevision: 11,
		RequestID: "r", Action: ActionJoinFarm,
	})
	tok, _ := p.Token(secret)
	// Logic 不需要持有 Gate 进程内状态；它用共享密钥和 gRPC 可信上下文
	// 校验签名中的完整路由栅栏。authority/active 的在线线性化由 Redis
	// Authority 完成，不能用另一个相互独立的 memoryAuthority 冒充。
	parsed, err := ParseToken(tok, secret)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := parsed.Validate(time.Now(), secret, s.AuthorityEpoch, s.ActiveSessionEpoch); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if parsed.GatewayID != "gw-A" || parsed.GatewayBootID != "boot-A" ||
		parsed.ConnectionGeneration != 7 || parsed.RouteRevision != 11 {
		t.Fatalf("route fence lost in codec: %+v", parsed)
	}
}

func TestPermitWrongSecretRejected(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	s, _ := a.Begin(ctx, 1001, "gw-1")
	p, _ := a.Permit(ctx, s, PermitRequest{ConnectionID: "c", RequestID: "r", Action: ActionReadOnly})
	tok, _ := p.Token([]byte("test-secret"))
	if _, err := ParseToken(tok, []byte("wrong-secret")); err != ErrPermitInvalid {
		t.Fatalf("want ErrPermitInvalid on wrong secret, got %v", err)
	}
}

func TestOwnerWriteRequiresOperationID(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	s, _ := a.Begin(ctx, 1001, "gw-1")
	if _, err := a.Permit(ctx, s, PermitRequest{Action: ActionOwnerWrite}); err == nil {
		t.Fatalf("owner_write without operation_id should fail")
	}
	if _, err := a.Permit(ctx, s, PermitRequest{Action: ActionOwnerWrite, OperationID: "op-1"}); err != nil {
		t.Fatalf("owner_write with operation_id should succeed: %v", err)
	}
}

func TestPermitExpiry(t *testing.T) {
	ctx := context.Background()
	a := New(Config{Secret: []byte("test-secret"), AuthorityEpoch: 1, PermitTTL: time.Nanosecond})
	s, _ := a.Begin(ctx, 1001, "gw-1")
	p, _ := a.Permit(ctx, s, PermitRequest{Action: ActionReadOnly})
	authEpoch, _ := a.AuthorityEpoch(ctx)
	time.Sleep(2 * time.Millisecond)
	actEpoch, _ := a.ActiveEpoch(ctx, 1001)
	if err := p.Validate(time.Now(), []byte("test-secret"), authEpoch, actEpoch); err != ErrPermitExpired {
		t.Fatalf("want ErrPermitExpired, got %v", err)
	}
}

func TestActivationAndRenewReturnAcceptedRouteLeaseTTL(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	candidate, err := a.Reserve(ctx, 3003, "")
	if err != nil {
		t.Fatal(err)
	}
	requestedTTL := 17 * time.Second
	activated, err := a.Activate(ctx, ActivationRequest{
		PlayerID: candidate.PlayerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: RouteFence{
			GatewayID: "gw-a", GatewayBootID: "boot-a",
			ConnectionID: "conn-a", ConnectionGeneration: 1,
		},
		LeaseTTL: requestedTTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activated.AcceptedLeaseTTL != requestedTTL {
		t.Fatalf("activation accepted ttl=%s want=%s", activated.AcceptedLeaseTTL, requestedTTL)
	}
	renewed, err := a.RenewRoutes(ctx, []ActiveRoute{activated.Route}, requestedTTL)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 || !renewed[0].Renewed || renewed[0].AcceptedLeaseTTL != requestedTTL {
		t.Fatalf("renew result=%+v", renewed)
	}
}

func TestExpiredRouteLeaseCannotBeRenewed(t *testing.T) {
	ctx := context.Background()
	a := newAuth()
	candidate, err := a.Reserve(ctx, 3004, "")
	if err != nil {
		t.Fatal(err)
	}
	activated, err := a.Activate(ctx, ActivationRequest{
		PlayerID: candidate.PlayerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: RouteFence{
			GatewayID: "gw-expired", GatewayBootID: "boot-expired",
			ConnectionID: "conn-expired", ConnectionGeneration: 1,
		},
		LeaseTTL: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	renewed, err := a.RenewRoutes(ctx, []ActiveRoute{activated.Route}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 || renewed[0].Renewed || !renewed[0].LeaseExpired {
		t.Fatalf("expired authority route was revived: %+v", renewed)
	}
}
