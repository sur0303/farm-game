package gatewayapp

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/gateway/account"
	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/session"
	"farmgame/internal/redisdomains"

	"github.com/alicebob/miniredis/v2"
)

func TestGateRedisDomainsKeepCredentialsAndSessionRuntimeApart(t *testing.T) {
	authorityServer := miniredis.RunT(t)
	runtimeServer := miniredis.RunT(t)
	projectionServer := miniredis.RunT(t)

	clients, err := redisdomains.OpenFromEnv(context.Background(), func(key string) string {
		switch key {
		case redisdomains.AuthorityURLEnv:
			return "redis://" + authorityServer.Addr() + "/0"
		case redisdomains.RuntimeURLEnv:
			return "redis://" + runtimeServer.Addr() + "/0"
		case redisdomains.ProjectionURLEnv:
			return "redis://" + projectionServer.Addr() + "/0"
		default:
			return ""
		}
	}, redisdomains.SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clients.Close() })

	accountStore, err := account.NewRedisStore(clients.Authority, "farm:test:web-account", 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accountStore.Create(context.Background(), "domain_user", "test-verifier", time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(authorityServer.Keys()) == 0 {
		t.Fatal("AccountCredential was not written to Authority Redis")
	}
	authorityKeysBeforeRuntimeWrites := len(authorityServer.Keys())
	if len(runtimeServer.Keys()) != 0 || len(projectionServer.Keys()) != 0 {
		t.Fatalf("AccountCredential leaked outside Authority: runtime=%v projection=%v", runtimeServer.Keys(), projectionServer.Keys())
	}

	runtimeAuthority := session.NewRedis(clients.Runtime, session.Config{
		Secret: []byte("01234567890123456789012345678901"), AuthorityEpoch: 1,
	})
	if _, err := runtimeAuthority.Reserve(context.Background(), 1_000_001, "session-domain-test"); err != nil {
		t.Fatal(err)
	}
	ticketService := ticket.New(ticket.NewRedisStore(clients.Runtime), ticket.WithTTL(time.Minute))
	if _, err := ticketService.Issue(context.Background(), ticket.Bound{
		PlayerID: 1_000_001, SessionID: "session-domain-test", SessionAuthorityEpoch: 1, ReservedSessionEpoch: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(runtimeServer.Keys()) < 2 {
		t.Fatalf("SessionAuthority/ConnectionTicket missing from Runtime Redis: %v", runtimeServer.Keys())
	}
	if len(projectionServer.Keys()) != 0 {
		t.Fatalf("session runtime state leaked to Projection Redis: %v", projectionServer.Keys())
	}
	if len(authorityServer.Keys()) != authorityKeysBeforeRuntimeWrites {
		t.Fatalf("session runtime state leaked to Authority Redis: %v", authorityServer.Keys())
	}
}
