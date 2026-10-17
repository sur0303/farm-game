package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/gateway/account"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gateway/protocol"
	"farmgame/internal/gateway/session"
	"farmgame/internal/observability/redisworkload"

	commonpb "farmgame/api/gen/common"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type redisCommandMetric struct {
	component string
	command   string
	outcome   string
}

type exactRedisCommandLog struct {
	mu       sync.Mutex
	commands []string
}

func (h *exactRedisCommandLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *exactRedisCommandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.mu.Lock()
		h.commands = append(h.commands, cmd.Name())
		h.mu.Unlock()
		return err
	}
}

func (h *exactRedisCommandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		h.mu.Lock()
		for _, cmd := range commands {
			h.commands = append(h.commands, cmd.Name())
		}
		h.mu.Unlock()
		return err
	}
}

func (h *exactRedisCommandLog) reset() {
	h.mu.Lock()
	h.commands = nil
	h.mu.Unlock()
}

func (h *exactRedisCommandLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...)
}

func TestGateRealLoginHelloCommandAndHeartbeatRedisVector(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	commandLog := &exactRedisCommandLog{}
	client.AddHook(commandLog)
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	accountStore, err := account.NewRedisStore(client, "test:gate-command-vector:accounts", 70_000)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := account.NewService(accountStore, account.Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := account.NewRedisAttemptLimiter(client, account.AttemptLimiterConfig{
		Prefix:  "test:gate-command-vector:attempts",
		HashKey: bytes.Repeat([]byte{0x41}, 32),
		Account: account.LimitPolicy{Burst: 100, Window: time.Minute},
		IP:      account.LimitPolicy{Burst: 100, Window: time.Minute},
		Device:  account.LimitPolicy{Burst: 100, Window: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := session.NewRedis(client, session.Config{
		Secret: []byte("gate-command-vector-permit-secret"), AuthorityEpoch: 1,
	})
	ticketStore := ticket.NewRedisStore(client)
	for name, load := range map[string]func(context.Context) error{
		"account": accountStore.EnsureScriptsLoaded,
		"attempt": limiter.EnsureScriptsLoaded,
		"session": authority.EnsureScriptsLoaded,
		"ticket":  ticketStore.EnsureScriptsLoaded,
	} {
		if err := load(ctx); err != nil {
			t.Fatalf("preload %s scripts: %v", name, err)
		}
	}
	// gatewayapp performs this bounded initialization before opening traffic.
	if _, err := authority.AuthorityEpoch(ctx); err != nil {
		t.Fatalf("initialize SessionAuthority epoch: %v", err)
	}

	authService, err := auth.New(
		staticVerifier{}, authority, ticket.New(ticketStore, ticket.WithTTL(time.Minute)), auth.Config{
			GatewayID: "gate-command-vector", GatewayBootID: "boot-command-vector",
			WssEndpoint: "wss://example.test/ws", RouteLeaseTTL: time.Minute,
			PermitSecret: []byte("gate-command-vector-permit-secret"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	authService.SetBootstrapper(testBootstrapper{})
	gate := NewGate(authService, &testLogic{}, 64<<10)

	const (
		username = "measured_farmer"
		password = "REPLACE_WITH_LOCAL_SECRET"
	)
	credential, err := accounts.Register(ctx, username, password)
	if err != nil {
		t.Fatal(err)
	}
	commandLog.reset()

	registry := prometheus.NewRegistry()
	if metrics, err := redisworkload.AttachFromEnvironment(client, registry, "true"); err != nil || metrics == nil {
		t.Fatalf("attach Redis workload metrics=%v err=%v", metrics, err)
	}
	bootStore, err := bootlease.NewRedisStore(client, "test:gate-command-vector:boot")
	if err != nil {
		t.Fatal(err)
	}
	boot, err := bootlease.New(bootStore, bootlease.Fence{
		GatewayID: authService.GatewayID(), BootID: authService.GatewayBootID(),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	loginHandler := auth.NewAccountLoginHandler(accounts, authService, false)
	loginHandler.SetAttemptLimiter(limiter, func(*http.Request) string { return "192.0.2.10" })
	request := httptest.NewRequest(http.MethodPost, "/api/v1/session/login", strings.NewReader(fmt.Sprintf(
		`{"username":%q,"password":%q,"bootstrap_id":"bootstrap-measured","device_id":"device-measured","client_build":"vector-test"}`,
		username, password,
	)))
	response := httptest.NewRecorder()
	loginHandler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("real account login status=%d body=%s", response.Code, response.Body.String())
	}
	var login auth.LoginResult
	if err := json.NewDecoder(response.Body).Decode(&login); err != nil {
		t.Fatalf("decode LoginResult: %v", err)
	}
	if login.PlayerID != credential.PlayerID || login.ConnectionTicket == "" {
		t.Fatalf("LoginResult=%+v want player=%d and ticket", login, credential.PlayerID)
	}

	connectionID := "connection-measured"
	helloRaw, err := proto.Marshal(&commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket, ClientBuild: "command-vector-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	helloResponse, err := gate.HandleBinary(ctx, connectionID, 1,
		clientFrameRaw(t, testFrameClientHello, 1, helloRaw))
	if err != nil {
		t.Fatalf("Gate.HandleBinary ClientHello: %v", err)
	}
	helloFrame, decodeErr := protocol.Decode(helloResponse, 64<<10)
	if decodeErr != nil || helloFrame.GetServerHello().GetPlayerId() != credential.PlayerID {
		t.Fatalf("ServerHello player=%d want=%d err=%v", helloFrame.GetServerHello().GetPlayerId(), credential.PlayerID, decodeErr)
	}

	// Recovery has dedicated control-frame tests. Mark its local result so this
	// trace can prove that a real business command adds no foreground Redis call.
	gate.mu.Lock()
	current := gate.connections[connectionID]
	current.recoveryComplete = true
	gate.connections[connectionID] = current
	gate.mu.Unlock()
	beforeCommand := commandLog.snapshot()
	commandResponse, err := gate.HandleBinary(ctx, connectionID, 1,
		encodeCommandFrame(t, "redis-vector-measured", 2))
	if err != nil {
		t.Fatalf("Gate.HandleBinary BUY: %v", err)
	}
	if got := commandLog.snapshot(); !reflect.DeepEqual(got, beforeCommand) {
		t.Fatalf("real Gate command added Redis calls: before=%v after=%v", beforeCommand, got)
	}
	commandFrame, decodeErr := protocol.Decode(commandResponse, 64<<10)
	if result := commandFrame.GetCommandResult(); decodeErr != nil || result == nil || result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("CommandResult=%+v err=%v", result, decodeErr)
	}

	// Force one real SessionRoute heartbeat through Gate's planner. This is the
	// high-cardinality foreground path observed at 20k sessions; it must be one
	// pipelined EVALSHA per independent ownership fence, never a full-body EVAL.
	if err := gate.RenewSessionLeases(ctx); err != nil {
		t.Fatalf("renew real Gate session heartbeat: %v", err)
	}
	if err := boot.Renew(ctx); err != nil {
		t.Fatalf("renew boot lease heartbeat: %v", err)
	}

	wantCommands := []string{
		"evalsha", "evalsha", "evalsha", // independent account/IP/device buckets
		"hmget", "del", // credential read and successful account reset
		"evalsha",            // reserve candidate session epoch
		"evalsha", "evalsha", // save ticket, then atomic ClientHello activation
		"evalsha", // SessionRoute heartbeat
		"set",     // process boot heartbeat
	}
	if got := commandLog.snapshot(); !reflect.DeepEqual(got, wantCommands) {
		t.Fatalf("real Gate Redis commands=%v want=%v", got, wantCommands)
	}
	wantMetrics := map[redisCommandMetric]float64{
		{redisworkload.ComponentGateAccount, "evalsha", "ok"}: 3,
		{redisworkload.ComponentGateAccount, "hmget", "ok"}:   1,
		{redisworkload.ComponentGateAccount, "del", "ok"}:     1,
		{redisworkload.ComponentGateSession, "evalsha", "ok"}: 2,
		{redisworkload.ComponentGateTicket, "evalsha", "ok"}:  2,
		{redisworkload.ComponentGateBootLease, "set", "ok"}:   1,
	}
	assertRedisCommandVectorBidirectional(t, redisCommandVector(t, registry), wantMetrics)
}

func redisCommandVector(t *testing.T, registry *prometheus.Registry) map[redisCommandMetric]float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	vector := make(map[redisCommandMetric]float64)
	for _, family := range families {
		if family.GetName() != "farm_redis_workload_commands_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			key := redisCommandMetric{}
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "component":
					key.component = label.GetValue()
				case "command":
					key.command = label.GetValue()
				case "outcome":
					key.outcome = label.GetValue()
				}
			}
			vector[key] += metric.GetCounter().GetValue()
		}
	}
	return vector
}

// Compare both target->base and base->target. This catches both an unexpected
// new command/outcome and a silently missing expected command.
func assertRedisCommandVectorBidirectional(t *testing.T, target, base map[redisCommandMetric]float64) {
	t.Helper()
	for key, got := range target {
		if want, ok := base[key]; !ok || got != want {
			t.Fatalf("target Redis command vector has %+v=%v, base=%v (target=%v)", key, got, want, target)
		}
	}
	for key, want := range base {
		if got, ok := target[key]; !ok || got != want {
			t.Fatalf("base Redis command vector requires %+v=%v, target=%v (target=%v)", key, want, got, target)
		}
	}
}
