package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gateway/commandcatalog"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/leasebatch"
	"farmgame/internal/gateway/preauth"
	"farmgame/internal/gateway/protocol"
	"farmgame/internal/gateway/session"
	"farmgame/internal/gateway/transport"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

func TestCommandResultFromOutcomePreservesRetryDirective(t *testing.T) {
	result, err := commandResultFromOutcome(
		&commonpb.ClientCommandEnvelope{
			OperationId: "join-op",
			RequestId:   "join-req",
			CommandType: commonpb.CommandType_JOIN_FARM,
		},
		&logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
			Business: &logicrpcpb.BusinessCommandOutcome{
				Status:         commonpb.CommandStatus_REJECTED,
				RetryDirective: logicrpcpb.RetryDirective_FRESH_JOIN,
				RetryAfterMs:   125,
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.RetryDirective != commonpb.RetryDirective_FRESH_JOIN || result.RetryAfterMs != 125 {
		t.Fatalf("retry metadata lost: %+v", result)
	}
}

type staticVerifier struct{}

func (staticVerifier) Verify(context.Context, string) (uint64, error) { return 101, nil }

type testBootstrapper struct{}

func (testBootstrapper) EnsureOwnerInitialized(context.Context, uint64, string, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return nil, nil
}

func (testBootstrapper) EnsureOwnerInitializedWithProfile(context.Context, uint64, string, string, uint32, int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return nil, nil
}

// testLogic is a constant-success stub: JOIN_FARM returns ACCEPTED and all
// other commands return SUCCEEDED with a fixed payload. These Gate tests
// therefore cover framing, session/admission, and result forwarding only;
// FarmActor join semantics are covered by internal/farm/ingress/
// join_barrier_test.go and service_test.go. Those tests cover ACCEPTED and
// snapshot downlink/barrier behavior, but not decoding/asserting the initial
// snapshot payload itself.
type testLogic struct{}

func (*testLogic) ExecuteCommand(_ context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	status := commonpb.CommandStatus_SUCCEEDED
	business := &logicrpcpb.BusinessCommandOutcome{Status: status}
	if req.Command.CommandType == commonpb.CommandType_JOIN_FARM {
		join := req.GetTrusted().GetJoinAttempt()
		if join == nil {
			return nil, errors.New("testLogic: missing JoinAdmissionContext")
		}
		business.Status = commonpb.CommandStatus_ACCEPTED
		business.OutcomeContext = &logicrpcpb.BusinessCommandOutcome_JoinAccepted{
			JoinAccepted: &logicrpcpb.JoinAcceptedMetadata{
				TargetFarmId: join.TargetFarmId, SubscriptionId: "server-subscription",
				JoinAttemptId: join.JoinAttemptId, AcceptedSubscriptionTtlMs: 30_000,
			},
		}
	} else {
		business.ResultPayload = mustMarshal(&farmpb.FarmCommandResult{})
	}
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: business,
	}}, nil
}

func (*testLogic) ArmSnapshot(_ context.Context, _ *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	version := uint64(0)
	return &logicrpcpb.ArmSnapshotResponse{
		Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED, SnapshotVersion: &version,
	}, nil
}

type captureCancelLogic struct {
	testLogic
	canceled chan *logicrpcpb.CancelJoinAttemptRequest
}

type captureDisconnectLogic struct {
	testLogic
	cleaned  chan *logicrpcpb.CleanupSubscriptionRequest
	canceled chan *logicrpcpb.CancelJoinAttemptRequest
}

func (l *captureDisconnectLogic) CleanupSubscription(_ context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	l.cleaned <- proto.Clone(req).(*logicrpcpb.CleanupSubscriptionRequest)
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

func (l *captureDisconnectLogic) CancelJoinAttempt(_ context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	l.canceled <- proto.Clone(req).(*logicrpcpb.CancelJoinAttemptRequest)
	return &logicrpcpb.CancelJoinAttemptResponse{
		Status: logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_CANCELLED,
	}, nil
}

func (l *captureCancelLogic) CancelJoinAttempt(_ context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	l.canceled <- proto.Clone(req).(*logicrpcpb.CancelJoinAttemptRequest)
	return &logicrpcpb.CancelJoinAttemptResponse{
		Status: logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_CANCELLED,
	}, nil
}

func mustMarshal(message proto.Message) []byte {
	payload, err := proto.Marshal(message)
	if err != nil {
		panic(err)
	}
	return payload
}

func newTestAuth(t *testing.T, cfg auth.Config) *auth.Service {
	t.Helper()
	if len(cfg.PermitSecret) == 0 {
		cfg.PermitSecret = []byte("gate-command-test-permit-secret")
	}
	svc, err := auth.New(staticVerifier{}, session.New(session.Config{AuthorityEpoch: 1}), ticket.New(nil), cfg)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetBootstrapper(testBootstrapper{})
	return svc
}

type delayedRenewAuthority struct {
	session.Authority
	delay       time.Duration
	acceptedTTL time.Duration
	mu          sync.Mutex
	lastStart   time.Time
}

type leaseExpiredRenewAuthority struct {
	session.Authority
}

func (a *leaseExpiredRenewAuthority) RenewRoutes(_ context.Context, routes []session.ActiveRoute, _ time.Duration) ([]session.RouteRenewal, error) {
	results := make([]session.RouteRenewal, 0, len(routes))
	for _, route := range routes {
		results = append(results, session.RouteRenewal{Route: route, LeaseExpired: true})
	}
	return results, nil
}

type mismatchedRenewAuthority struct {
	session.Authority
}

func (a *mismatchedRenewAuthority) RenewRoutes(_ context.Context, routes []session.ActiveRoute, _ time.Duration) ([]session.RouteRenewal, error) {
	results := make([]session.RouteRenewal, 0, len(routes))
	for _, route := range routes {
		route.GatewayBootID += "-corrupt"
		results = append(results, session.RouteRenewal{
			Route: route, Renewed: true, AcceptedLeaseTTL: time.Second,
		})
	}
	return results, nil
}

type mixedRenewAuthority struct{ session.Authority }

func (a *mixedRenewAuthority) RenewRoutes(_ context.Context, routes []session.ActiveRoute, _ time.Duration) ([]session.RouteRenewal, error) {
	results := make([]session.RouteRenewal, 0, len(routes))
	for _, route := range routes {
		result := session.RouteRenewal{Route: route}
		switch route.ConnectionID {
		case "route-success":
			result.Renewed = true
			result.AcceptedLeaseTTL = 30 * time.Second
		case "route-transient":
			result.Transient = true
		case "route-overlong":
			result.Renewed = true
			result.AcceptedLeaseTTL = time.Minute
		}
		results = append(results, result)
	}
	return results, errors.New("one physical pipeline item failed")
}

type concurrentRenewAuthority struct {
	session.Authority
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

func (a *concurrentRenewAuthority) RenewRoutes(ctx context.Context, routes []session.ActiveRoute, ttl time.Duration) ([]session.RouteRenewal, error) {
	a.mu.Lock()
	a.inFlight++
	if a.inFlight > a.maxInFlight {
		a.maxInFlight = a.inFlight
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.inFlight--
		a.mu.Unlock()
	}()
	select {
	case <-time.After(25 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return a.Authority.RenewRoutes(ctx, routes, ttl)
}

func (a *delayedRenewAuthority) RenewRoutes(ctx context.Context, routes []session.ActiveRoute, _ time.Duration) ([]session.RouteRenewal, error) {
	a.mu.Lock()
	a.lastStart = time.Now()
	a.mu.Unlock()
	select {
	case <-time.After(a.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	results := make([]session.RouteRenewal, 0, len(routes))
	for _, route := range routes {
		route.LeaseExpiresAt = time.Now().Add(24 * time.Hour)
		results = append(results, session.RouteRenewal{
			Route: route, Renewed: true, AcceptedLeaseTTL: a.acceptedTTL,
		})
	}
	return results, nil
}

func newTestAuthWithAuthority(t *testing.T, authority session.Authority, cfg auth.Config) *auth.Service {
	t.Helper()
	if len(cfg.PermitSecret) == 0 {
		cfg.PermitSecret = []byte("gate-command-test-permit-secret")
	}
	svc, err := auth.New(staticVerifier{}, authority, ticket.New(nil), cfg)
	if err != nil {
		t.Fatal(err)
	}
	svc.SetBootstrapper(testBootstrapper{})
	return svc
}

type blockingLogic struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type farmSwitchLogic struct {
	mu                   sync.Mutex
	commands             []*commonpb.ClientCommandEnvelope
	trustedSubscriptions []string
	cleanupStarted       chan *logicrpcpb.CleanupSubscriptionRequest
	cleanupRelease       chan struct{}
}

func (l *farmSwitchLogic) ExecuteCommand(_ context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	l.mu.Lock()
	l.commands = append(l.commands, proto.Clone(req.Command).(*commonpb.ClientCommandEnvelope))
	if req.Trusted.FarmContext != nil {
		l.trustedSubscriptions = append(l.trustedSubscriptions, req.Trusted.FarmContext.AdmittedSubscriptionId)
	} else {
		l.trustedSubscriptions = append(l.trustedSubscriptions, "")
	}
	l.mu.Unlock()
	business := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_ACCEPTED}
	if req.Command.CommandType == commonpb.CommandType_JOIN_FARM {
		join := req.GetTrusted().GetJoinAttempt()
		if join == nil {
			return nil, errors.New("farmSwitchLogic: missing JoinAdmissionContext")
		}
		business.OutcomeContext = &logicrpcpb.BusinessCommandOutcome_JoinAccepted{
			JoinAccepted: &logicrpcpb.JoinAcceptedMetadata{
				TargetFarmId: join.TargetFarmId,
				SubscriptionId: map[uint64]string{
					101: "server-subscription-old", 202: "server-subscription-new",
				}[join.TargetFarmId],
				JoinAttemptId:             join.JoinAttemptId,
				AcceptedSubscriptionTtlMs: 30_000,
			},
		}
	}
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: business,
	}}, nil
}

func (*farmSwitchLogic) ArmSnapshot(_ context.Context, _ *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	version := uint64(1)
	return &logicrpcpb.ArmSnapshotResponse{
		Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED, SnapshotVersion: &version,
	}, nil
}

type localGateKicker struct {
	target            *Gate
	called            chan struct{}
	deadlineRemaining chan time.Duration
	once              sync.Once
}

func (k *localGateKicker) KickOldSession(ctx context.Context, route session.ActiveRoute) error {
	if k.deadlineRemaining != nil {
		if deadline, ok := ctx.Deadline(); ok {
			k.deadlineRemaining <- time.Until(deadline)
		}
	}
	_, err := k.target.KickSession(ctx, kickRequest(&route))
	k.once.Do(func() { close(k.called) })
	return err
}

func (l *blockingLogic) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	l.once.Do(func() { close(l.started) })
	select {
	case <-l.release:
		return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
			Business: &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED},
		}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// joinFarmPayload 编码 28 §5.1 的 JoinFarmRequest:target_farm_id 是 JOIN_FARM
// 唯一目标来源,客户端不再通过公共 Envelope 提交目标。
func joinFarmPayload(t *testing.T, farmID uint64) []byte {
	t.Helper()
	payload, err := proto.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: farmID})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// appendUvarint 追加 protobuf varint。
func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// paddedBuyPayload 生成恰好 size 字节的合法 BuyRequest:在有效消息后追加
// protobuf 未知字段(field 100,LEN),解码器保留未知字段且不计入语义,
// 用于 MaxPayloadBytes 等值边界测试。
func paddedBuyPayload(t *testing.T, size int) []byte {
	t.Helper()
	base, err := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	// 未知字段头:tag=(100<<3)|2=802 → varint(0xA2 0x06),随后是长度 varint。
	const header = 2
	padLen := size - len(base) - header
	if padLen < 2 {
		t.Fatalf("size %d too small for padded payload", size)
	}
	// 长度前缀的字节数随 padLen 变化;迭代至 varint 长度与 padLen 自洽。
	var lengthPrefix []byte
	for {
		lengthPrefix = appendUvarint(nil, uint64(padLen))
		need := size - len(base) - header - len(lengthPrefix)
		if need == padLen {
			break
		}
		padLen = need
		if padLen < 0 {
			t.Fatalf("size %d too small for padded payload", size)
		}
	}
	out := append([]byte(nil), base...)
	out = append(out, 0xA2, 0x06)
	out = append(out, lengthPrefix...)
	return append(out, make([]byte, padLen)...)
}

// mustServerPush 把 ServerPushEnvelope bytes 还原为消息,便于构造 oneof 帧。
func mustServerPush(t *testing.T, body []byte) *commonpb.ServerPushEnvelope {
	t.Helper()
	push := &commonpb.ServerPushEnvelope{}
	if err := proto.Unmarshal(body, push); err != nil {
		t.Fatal(err)
	}
	return push
}

// testFrameKind 是测试本地的帧大类判别符;旧 FrameKind 枚举已随
// deprecated 编译窗口删除(28 §2:大类唯一来源是 frame_body oneof)。
type testFrameKind uint8

const (
	testFrameClientHello testFrameKind = iota + 1
	testFrameServerHello
	testFrameClientCommand
	testFrameCommandResult
	testFrameServerPush
	testFrameConnectionError
	testFrameControl
)

// clientFrameRaw 以新顶层 oneof 结构编码客户端帧(28 §2:大类由 oneof 区分)。
// body 为对应消息的已序列化 bytes,便于沿用既有测试构造。
func clientFrameRaw(t *testing.T, kind testFrameKind, seq uint64, body []byte) []byte {
	t.Helper()
	var frame *commonpb.FarmFrame
	switch kind {
	case testFrameClientHello:
		msg := &commonpb.ClientHello{}
		if err := proto.Unmarshal(body, msg); err != nil {
			t.Fatal(err)
		}
		frame = protocol.NewClientHelloFrame(seq, msg)
	case testFrameClientCommand:
		msg := &commonpb.ClientCommandEnvelope{}
		if err := proto.Unmarshal(body, msg); err != nil {
			t.Fatal(err)
		}
		frame = protocol.NewClientCommandFrame(seq, msg)
	case testFrameControl:
		msg := &commonpb.ControlFrame{}
		if err := proto.Unmarshal(body, msg); err != nil {
			t.Fatal(err)
		}
		frame = protocol.NewControlFrame(seq, msg)
	case testFrameServerPush:
		msg := &commonpb.ServerPushEnvelope{}
		if err := proto.Unmarshal(body, msg); err != nil {
			t.Fatal(err)
		}
		frame = protocol.NewServerPushFrame(seq, msg)
	default:
		t.Fatalf("unsupported frame kind %d", kind)
	}
	raw, err := protocol.Encode(frame, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestGateRejectsServerDirectionFrames 覆盖 28 §2 的 Gate 入站方向校验:
// 客户端发送 CommandResult/ServerPush/ServerHello 或服务端作用域 Control,
// 即使 oneof 能解码也必须 fail closed,不进入认证/命令管线。
func TestGateRejectsServerDirectionFrames(t *testing.T) {
	gate := NewGate(nil, nil, 64<<10)
	cases := map[string][]byte{
		"server-hello": mustRawFrame(t, protocol.NewServerHelloFrame(1, &commonpb.ServerHello{PlayerId: 1})),
		"command-result": mustRawFrame(t, protocol.NewCommandResultFrame(1,
			&commonpb.CommandResultEnvelope{RequestId: "r"})),
		"server-push": mustRawFrame(t, protocol.NewServerPushFrame(1,
			&commonpb.ServerPushEnvelope{PushType: commonpb.PushType_KICK})),
		"server-control": mustRawFrame(t, protocol.NewControlFrame(1, &commonpb.ControlFrame{
			Body: &commonpb.ControlFrame_ConnectionError{ConnectionError: &commonpb.ConnectionError{}},
		})),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := gate.HandleBinary(context.Background(), "dir-conn", 1, raw); !errors.Is(err, ErrBadMessage) {
				t.Fatalf("err=%v, want ErrBadMessage", err)
			}
		})
	}
}

func mustRawFrame(t *testing.T, frame *commonpb.FarmFrame) []byte {
	t.Helper()
	raw, err := protocol.Encode(frame, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGateDownlinkWriterCapacityMustBeBoundedBeforeAdmission(t *testing.T) {
	gate := NewGate(nil, nil, 64<<10)
	if gate.downlinkWriterCapacity.MaxItems != 256 || gate.downlinkWriterCapacity.MaxBytes != 64<<10 {
		t.Fatalf("legacy capacity=%+v", gate.downlinkWriterCapacity)
	}

	valid := downlink.WriterCapacity{
		MaxItems: 512,
		MaxBytes: 2 << 20,
		Hint:     downlink.HintPolicy{MaxHintItems: 32, MaxHintBytes: 64 << 10},
	}
	if err := gate.SetDownlinkWriterCapacity(valid); err != nil {
		t.Fatal(err)
	}
	if gate.downlinkWriterCapacity != valid {
		t.Fatalf("capacity=%+v, want %+v", gate.downlinkWriterCapacity, valid)
	}

	for name, capacity := range map[string]downlink.WriterCapacity{
		"zero total":    {},
		"negative hint": {MaxItems: 1, MaxBytes: 1, Hint: downlink.HintPolicy{MaxHintItems: -1}},
		"hint items":    {MaxItems: 1, MaxBytes: 8, Hint: downlink.HintPolicy{MaxHintItems: 2}},
		"hint bytes":    {MaxItems: 2, MaxBytes: 8, Hint: downlink.HintPolicy{MaxHintBytes: 9}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := gate.SetDownlinkWriterCapacity(capacity); err == nil {
				t.Fatal("invalid capacity was accepted")
			}
		})
	}

	gate.connections["already-admitted"] = connection{generation: 1}
	if err := gate.SetDownlinkWriterCapacity(valid); err == nil {
		t.Fatal("capacity changed after connection admission")
	}
}

func TestGateHelloAndCommandRoundTrip(t *testing.T) {
	logic := &testLogic{}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	g := NewGate(authSvc, logic, 64<<10)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "test"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	resp, err := g.HandleBinary(context.Background(), "conn-1", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	f, err := protocol.Decode(resp, 64<<10)
	if err != nil || f.GetServerHello() == nil {
		t.Fatalf("hello response=%+v err=%v", f, err)
	}
	buyPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	cmd, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{OperationId: "op-req-1", RequestId: "req-1", CommandType: commonpb.CommandType_BUY, Payload: buyPayload})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, cmd)
	resp, err = g.HandleBinary(context.Background(), "conn-1", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	f, err = protocol.Decode(resp, 64<<10)
	if err != nil || f.GetCommandResult() == nil {
		t.Fatalf("command response=%+v err=%v", f, err)
	}
	result := f.GetCommandResult()
	if result == nil || result.RequestId != "req-1" {
		t.Fatalf("result request_id=%q err=%v", result.RequestId, err)
	}
}

func TestGateHelloIndexesAndFencesPreviousPlayerConnection(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "old-connection")
	activateHello(t, gate, authSvc, "new-connection")

	gate.mu.Lock()
	oldConnection := gate.connections["old-connection"]
	newConnection := gate.connections["new-connection"]
	fence, indexed := gate.playerConnections[101]
	gate.mu.Unlock()
	if oldConnection.admission != laneStale {
		t.Fatalf("previous player connection admission=%v, want stale", oldConnection.admission)
	}
	if newConnection.admission != laneAdmissible {
		t.Fatalf("replacement player connection admission=%v, want admissible", newConnection.admission)
	}
	if !indexed || fence != (localConnectionFence{connectionID: "new-connection", generation: 1}) {
		t.Fatalf("player connection index=%+v present=%v", fence, indexed)
	}
	if _, err := gate.HandleBinary(context.Background(), "old-connection", 1,
		encodeCommandFrame(t, "old-after-replacement", 2)); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("previous player connection admitted after replacement: %v", err)
	}
}

func TestGateStaleDisconnectPreservesReplacementPlayerIndex(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "old-connection")
	activateHello(t, gate, authSvc, "new-connection")

	gate.Disconnect("old-connection", 1)
	gate.mu.Lock()
	fence, indexed := gate.playerConnections[101]
	gate.mu.Unlock()
	if !indexed || fence != (localConnectionFence{connectionID: "new-connection", generation: 1}) {
		t.Fatalf("stale disconnect changed replacement index=%+v present=%v", fence, indexed)
	}

	gate.Disconnect("new-connection", 1)
	gate.mu.Lock()
	_, indexed = gate.playerConnections[101]
	gate.mu.Unlock()
	if indexed {
		t.Fatal("exact replacement disconnect did not remove player index")
	}
}

func TestGateReusedConnectionIDIndexesExactGeneration(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHelloGeneration(t, gate, authSvc, "reused-connection", 1)
	activateHelloGeneration(t, gate, authSvc, "reused-connection", 2)

	gate.Disconnect("reused-connection", 1)
	gate.mu.Lock()
	fence, indexed := gate.playerConnections[101]
	current := gate.connections["reused-connection"]
	gate.mu.Unlock()
	if !indexed || fence != (localConnectionFence{connectionID: "reused-connection", generation: 2}) {
		t.Fatalf("reused connection index=%+v present=%v", fence, indexed)
	}
	if current.generation != 2 {
		t.Fatalf("stale disconnect removed replacement generation: %+v", current)
	}
}

func TestGatePlayerIndexRejectsOlderAuthenticationCompletion(t *testing.T) {
	gate := NewGate(nil, nil, 64<<10)
	currentBinding := &auth.SessionBinding{
		PlayerID: 101, SessionAuthorityEpoch: 1, ActiveSessionEpoch: 2,
		Route: session.ActiveRoute{RouteRevision: 4},
	}
	incomingBinding := &auth.SessionBinding{
		PlayerID: 101, SessionAuthorityEpoch: 1, ActiveSessionEpoch: 1,
		Route: session.ActiveRoute{RouteRevision: 3},
	}
	gate.mu.Lock()
	gate.connections["newer-connection"] = connection{
		generation: 2,
		binding:    currentBinding,
		admission:  laneAdmissible,
	}
	gate.playerConnections[101] = localConnectionFence{
		connectionID: "newer-connection",
		generation:   2,
	}
	admitted := gate.stalePlayerConnectionLocked(incomingBinding, "older-connection", 1)
	current := gate.connections["newer-connection"]
	fence := gate.playerConnections[101]
	gate.mu.Unlock()

	if admitted {
		t.Fatal("older authentication result replaced newer authoritative lane")
	}
	if current.admission != laneAdmissible {
		t.Fatalf("newer lane was fenced by older result: %+v", current)
	}
	if fence != (localConnectionFence{connectionID: "newer-connection", generation: 2}) {
		t.Fatalf("newer player index changed: %+v", fence)
	}
}

func TestGateBootLeaseBlocksNewHelloUntilAuthorityRenewSucceeds(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "boot-lease"})
	if err != nil {
		t.Fatal(err)
	}
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	store := bootlease.NewMemoryStore()
	lease, err := bootlease.New(store, bootlease.Fence{GatewayID: "gw", BootID: "boot"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	gate.SetBootLease(lease)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/test"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "boot-fenced", 1, raw); !errors.Is(err, ErrGateBootLeaseExpired) {
		t.Fatalf("untrusted boot hello err=%v, want ErrGateBootLeaseExpired", err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.HandleBinary(context.Background(), "boot-fenced", 1, raw); err != nil {
		t.Fatalf("hello after boot lease recovery: %v", err)
	}
}

func TestExpiredGateBootLeaseRejectsOwnerWritesButAllowsReadRecovery(t *testing.T) {
	gate := NewGate(nil, nil, 0)
	lease, err := bootlease.New(bootlease.NewMemoryStore(), bootlease.Fence{GatewayID: "gw", BootID: "boot"}, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	gate.SetBootLease(lease)
	time.Sleep(time.Millisecond)
	if err := gate.rejectGateBootLeaseAdmission("conn", 1, commandcatalog.CommandSpec{
		CommandType: commonpb.CommandType_BUY, PermitClass: commandcatalog.PermitOwnerWrite,
	}); !errors.Is(err, ErrGateBootLeaseExpired) {
		t.Fatalf("owner write err=%v, want ErrGateBootLeaseExpired", err)
	}
	if err := gate.rejectGateBootLeaseAdmission("conn", 1, commandcatalog.CommandSpec{
		CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP, PermitClass: commandcatalog.PermitReadOnly,
	}); err != nil {
		t.Fatalf("read-only recovery command rejected: %v", err)
	}
}

func TestGateCompletesPreAuthOnlyAfterSuccessfulHelloActivation(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{
		PlatformToken: "p", BootstrapID: "preauth",
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := preauth.New(preauth.Config{
		MaxConnections: 2, MaxConnectionsPerIP: 1,
		MaxFramesPerConnection: 1, MaxBytesPerConnection: 64 << 10,
		MaxBytesPerIP: 64 << 10, MaxBytesPod: 64 << 10,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	gate.SetPreAuthController(controller)
	if err := gate.BeginPreAuth("preauth-conn", 7, "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	hello, _ := proto.Marshal(&commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2",
	})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if err := gate.AdmitPreAuth("preauth-conn", 7, len(raw)); err != nil {
		t.Fatal(err)
	}
	if snapshot := controller.Snapshot("192.0.2.1"); snapshot.Connections != 1 || snapshot.PodBytes != int64(len(raw)) {
		t.Fatalf("pre-hello budget=%s", snapshot)
	}
	if _, err := gate.HandleBinary(context.Background(), "preauth-conn", 7, raw); err != nil {
		t.Fatal(err)
	}
	if !gate.IsAuthenticated("preauth-conn", 7) {
		t.Fatal("successful ClientHello did not establish authenticated Gate state")
	}
	if snapshot := controller.Snapshot("192.0.2.1"); snapshot != (preauth.Snapshot{}) {
		t.Fatalf("successful ClientHello did not release PRE_AUTH budget: %s", snapshot)
	}
}

func TestGateDisconnectReleasesUnauthenticatedBudget(t *testing.T) {
	controller := preauth.New(preauth.Config{})
	gate := NewGate(nil, nil, 64<<10)
	gate.SetPreAuthController(controller)
	if err := gate.BeginPreAuth("unbound", 3, "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	gate.Disconnect("unbound", 3)
	if snapshot := controller.Snapshot("192.0.2.1"); snapshot != (preauth.Snapshot{}) {
		t.Fatalf("pre-auth disconnect leaked budget: %s", snapshot)
	}
}

func TestGateRejectsMissingClientBuild(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "build-check"})
	if err != nil {
		t.Fatal(err)
	}
	g := NewGate(authSvc, &testLogic{}, 64<<10)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := g.HandleBinary(context.Background(), "build-check", 1, raw); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("missing client_build error=%v, want ErrUnauthenticated", err)
	}
}

func TestGateJoinQueuesAcceptedMetadataWithoutSnapshotPayload(t *testing.T) {
	logic := &testLogic{}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	g := NewGate(authSvc, logic, 64<<10)
	downlinks := downlink.NewConnectionRegistry()
	g.SetDownlinkRegistry(downlinks)
	t.Cleanup(func() { g.Disconnect("c", 1) })
	tr := transport.NewFakeTransport(4)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if err := tr.Data(raw); err != nil {
		t.Fatal(err)
	}
	in, _ := tr.Receive()
	out, err := g.HandleBinary(context.Background(), "c", 1, in)
	if err != nil || tr.Send(out) != nil {
		t.Fatalf("hello err=%v", err)
	}
	_, _ = tr.Sent()
	cmd, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{OperationId: "join-op-1", RequestId: "join-1", CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, 101)})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, cmd)
	out, err = g.HandleBinary(context.Background(), "c", 1, raw)
	if err != nil || len(out) != 0 {
		t.Fatalf("Join must use the ConnectionWriter path: response=%d err=%v", len(out), err)
	}
	queued, ok := downlinks.Dequeue("c", 1)
	if !ok {
		t.Fatal("missing queued Join ACCEPTED")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(queued, &frame); err != nil {
		t.Fatal(err)
	}
	result := frame.GetCommandResult()
	if result == nil {
		t.Fatal("join response is not a command result")
	}
	join := result.GetJoin()
	if result.Status != commonpb.CommandStatus_ACCEPTED || join == nil ||
		join.TargetFarmId != 101 || join.SubscriptionId == "" || join.JoinAttemptId == "" ||
		len(result.Payload) != 0 {
		t.Fatalf("join result=%v", result)
	}
}

func TestGateAndAsyncDownlinkShareServerSequence(t *testing.T) {
	logic := &testLogic{}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	registry := downlink.NewConnectionRegistry()
	g := NewGate(authSvc, logic, 64<<10)
	g.SetDownlinkRegistry(registry)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	response, err := g.HandleBinary(context.Background(), "c-seq", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	first, err := protocol.Decode(response, 64<<10)
	if err != nil || first.GetSequence() != 1 {
		t.Fatalf("hello frame=%v err=%v", first, err)
	}
	buySeqPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	cmd, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{OperationId: "op-req-seq", RequestId: "req-seq", CommandType: commonpb.CommandType_BUY, Payload: buySeqPayload})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, cmd)
	response, err = g.HandleBinary(context.Background(), "c-seq", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := protocol.Decode(response, 64<<10)
	if err != nil || second.GetSequence() != 2 {
		t.Fatalf("command frame=%v err=%v", second, err)
	}
	if !registry.BeginSubscription("c-seq", 1, "sub-1") {
		t.Fatal("failed to establish sequence-test subscription")
	}
	pushBody, _ := proto.Marshal(&commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SYSTEM_NOTICE,
		PushContext: &commonpb.ServerPushEnvelope_Session{
			Session: &commonpb.SessionPushContext{TargetPlayerId: 101},
		},
		Payload: []byte("push"),
	})
	push, _ := proto.Marshal(protocol.NewServerPushFrame(1, mustServerPush(t, pushBody)))
	queued, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "push-1", SubscriptionId: "sub-1", ConnectionId: "c-seq", TransportGeneration: 1, PresenceEpoch: 1, Frame: push}}})
	if err != nil || queued.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("queued=%v err=%v", queued, err)
	}
	queuedFrame, ok := registry.Dequeue("c-seq", 1)
	if !ok {
		t.Fatal("expected async push")
	}
	third, err := protocol.Decode(queuedFrame, 64<<10)
	if err != nil || third.GetSequence() != 3 {
		t.Fatalf("push frame=%v err=%v", third, err)
	}
}

var _ LogicClient = (*testLogic)(nil)
var _ = logicrpcpb.ExecuteCommandRequest{}

type captureCleaner struct {
	mu         sync.Mutex
	playerID   uint64
	connection string
	calls      int
}

func (c *captureCleaner) CleanupConnection(_ context.Context, playerID uint64, connectionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.playerID, c.connection, c.calls = playerID, connectionID, c.calls+1
	return nil
}

func TestGateDisconnectWithoutSubscriptionDoesNotScanActors(t *testing.T) {
	logic := &testLogic{}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	cleaner := &captureCleaner{}
	g := NewGate(authSvc, logic, 64<<10)
	g.SetConnectionCleaner(cleaner)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := g.HandleBinary(context.Background(), "conn-clean", 3, raw); err != nil {
		t.Fatal(err)
	}
	g.Disconnect("conn-clean", 3)
	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	if cleaner.calls != 0 {
		t.Fatalf("cleanup=%+v", cleaner)
	}
}

func (l *farmSwitchLogic) CleanupSubscription(_ context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	if l.cleanupStarted != nil {
		l.cleanupStarted <- proto.Clone(req).(*logicrpcpb.CleanupSubscriptionRequest)
	}
	if l.cleanupRelease != nil {
		<-l.cleanupRelease
	}
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

func TestGateDisconnectCleansExactActiveAndPendingJoin(t *testing.T) {
	logic := &captureDisconnectLogic{
		cleaned:  make(chan *logicrpcpb.CleanupSubscriptionRequest, 1),
		canceled: make(chan *logicrpcpb.CancelJoinAttemptRequest, 1),
	}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "disconnect-exact"})
	if err != nil {
		t.Fatal(err)
	}
	legacy := &captureCleaner{}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetConnectionCleaner(legacy)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "disconnect-conn", 4, raw); err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	current := gate.connections["disconnect-conn"]
	current.currentFarmID = 201
	current.currentSubscriptionID = "active-sub"
	current.currentJoinAttemptID = "active-attempt"
	current.currentPresenceEpoch = 17
	current.pendingJoin = &pendingJoinState{
		operationID: "pending-op", attemptID: "pending-attempt", farmID: 202,
		state: pendingJoinPending, deadline: time.Now().Add(time.Second),
	}
	gate.connections["disconnect-conn"] = current
	gate.mu.Unlock()

	gate.Disconnect("disconnect-conn", 4)
	cleanup := <-logic.cleaned
	if cleanup.Subscription == nil || cleanup.Subscription.FarmId != 201 ||
		cleanup.Subscription.SubscriptionId != "active-sub" ||
		cleanup.Subscription.JoinAttemptId != "active-attempt" || cleanup.Target == nil ||
		cleanup.Target.TargetPlayerId != 101 || cleanup.Target.ConnectionId != "disconnect-conn" ||
		cleanup.Target.ConnectionGeneration != 4 || cleanup.Target.GatewayId == "" ||
		cleanup.Target.GatewayBootId == "" || cleanup.Target.SessionEpoch == 0 ||
		cleanup.Target.SessionRouteRevision == 0 ||
		cleanup.Reason != logicrpcpb.CleanupReason_CLEANUP_REASON_DISCONNECT {
		t.Fatalf("exact cleanup request=%+v", cleanup)
	}
	cancel := <-logic.canceled
	if cancel.TargetFarmId != 202 || cancel.JoinOperationId != "pending-op" ||
		cancel.JoinAttemptId != "pending-attempt" || cancel.SubscriptionId != nil || cancel.Target == nil ||
		cancel.Target.ConnectionId != "disconnect-conn" || cancel.Target.ConnectionGeneration != 4 ||
		cancel.Reason != logicrpcpb.CancelJoinReason_CANCEL_JOIN_REASON_CONNECTION_CLOSED {
		t.Fatalf("pending cleanup request=%+v", cancel)
	}
	legacy.mu.Lock()
	legacyCalls := legacy.calls
	legacy.mu.Unlock()
	if legacyCalls != 0 {
		t.Fatalf("exact cleanup fell back to actor scan: %d", legacyCalls)
	}
}

func TestGateStaleDisconnectCannotCleanReusedConnection(t *testing.T) {
	logic := &captureDisconnectLogic{
		cleaned:  make(chan *logicrpcpb.CleanupSubscriptionRequest, 1),
		canceled: make(chan *logicrpcpb.CancelJoinAttemptRequest, 1),
	}
	gate := NewGate(newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"}), logic, 64<<10)
	gate.mu.Lock()
	gate.connections["reused"] = connection{generation: 8, currentFarmID: 1, currentSubscriptionID: "new", currentPresenceEpoch: 2}
	gate.mu.Unlock()
	gate.Disconnect("reused", 7)
	select {
	case req := <-logic.cleaned:
		t.Fatalf("stale disconnect issued cleanup: %+v", req)
	default:
	}
	gate.mu.Lock()
	_, exists := gate.connections["reused"]
	gate.mu.Unlock()
	if !exists {
		t.Fatal("stale disconnect deleted reused connection")
	}
}

func TestPendingJoinTimeoutClosesFenceAndRejectsLateCallbacks(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "join-timeout"})
	if err != nil {
		t.Fatal(err)
	}
	registry := downlink.NewConnectionRegistry()
	cleaner := &captureCleaner{}
	logic := &captureCancelLogic{canceled: make(chan *logicrpcpb.CancelJoinAttemptRequest, 1)}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetDownlinkRegistry(registry)
	gate.SetConnectionCleaner(cleaner)
	gate.SetPendingJoinTimeout(5 * time.Millisecond)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "join-timeout-conn", 3, raw); err != nil {
		t.Fatal(err)
	}
	if !gate.prepareJoin("join-timeout-conn", 3, "join-timeout-op", 101) {
		t.Fatal("prepare join failed")
	}
	var cancelReq *logicrpcpb.CancelJoinAttemptRequest
	select {
	case cancelReq = <-logic.canceled:
	case <-time.After(time.Second):
		t.Fatal("CancelJoin RPC was not issued")
	}
	if cancelReq.JoinOperationId != "join-timeout-op" || cancelReq.TargetFarmId != 101 ||
		cancelReq.JoinAttemptId == "" || cancelReq.SubscriptionId != nil || cancelReq.Target == nil ||
		cancelReq.Target.ConnectionId != "join-timeout-conn" || cancelReq.Target.ConnectionGeneration != 3 ||
		cancelReq.Reason != logicrpcpb.CancelJoinReason_CANCEL_JOIN_REASON_PENDING_TIMEOUT {
		t.Fatalf("CancelJoin request=%+v", cancelReq)
	}
	cleaner.mu.Lock()
	calls := cleaner.calls
	cleaner.mu.Unlock()
	if calls != 0 {
		t.Fatalf("PendingJoin timeout must not run connection-wide actor scan, cleanup calls=%d", calls)
	}
	if gate.acceptJoin("join-timeout-conn", 3, "join-timeout-op", 101, "late-sub") {
		t.Fatal("late ACCEPTED reopened CLOSED PendingJoin")
	}
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "late-snapshot", SubscriptionId: "late-sub", ConnectionId: "join-timeout-conn",
		TransportGeneration: 3, PresenceEpoch: 1,
		JoinOperationId: "join-timeout-op", TargetFarmId: 101,
		Frame: encodeJoinSnapshotPush(t, "late-sub"),
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("late snapshot=%v err=%v", response, err)
	}
}

func TestPendingJoinRetryReusesIntentAndDeadline(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "join-retry"})
	if err != nil {
		t.Fatal(err)
	}
	registry := downlink.NewConnectionRegistry()
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	gate.SetDownlinkRegistry(registry)
	gate.SetPendingJoinTimeout(time.Second)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "join-retry-conn", 4, raw); err != nil {
		t.Fatal(err)
	}
	if !gate.prepareJoin("join-retry-conn", 4, "join-retry-op", 101) {
		t.Fatal("first prepare join failed")
	}
	gate.mu.Lock()
	first := gate.connections["join-retry-conn"].pendingJoin
	firstDeadline := first.deadline
	firstTimer := first.timer
	gate.mu.Unlock()
	if !gate.prepareJoin("join-retry-conn", 4, "join-retry-op", 101) {
		t.Fatal("retry prepare join failed")
	}
	gate.mu.Lock()
	second := gate.connections["join-retry-conn"].pendingJoin
	gate.mu.Unlock()
	if second != first || second.timer != firstTimer || !second.deadline.Equal(firstDeadline) {
		t.Fatalf("same operation replaced PendingJoin: first=%p second=%p first_deadline=%v second_deadline=%v", first, second, firstDeadline, second.deadline)
	}
	gate.cancelJoin("join-retry-conn", 4, "join-retry-op", 101)
}

func TestGateBackendPoolRejectsBeforeUnboundedRPCWork(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	logic := &blockingLogic{started: make(chan struct{}), release: make(chan struct{})}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetBackendPool(1, time.Second)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "pool-conn", 1, raw); err != nil {
		t.Fatal(err)
	}
	setRecoveryCompleteForTest(t, gate, "pool-conn", 1, true)
	commandFrame := func(sequence uint32, requestID string) []byte {
		payload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
		body, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
			OperationId: "op-" + requestID, RequestId: requestID,
			CommandType: commonpb.CommandType_BUY, Payload: payload,
		})
		frame := clientFrameRaw(t, testFrameClientCommand, uint64(sequence), body)
		return frame
	}
	firstDone := make(chan error, 1)
	go func() {
		_, callErr := gate.HandleBinary(context.Background(), "pool-conn", 1, commandFrame(2, "first"))
		firstDone <- callErr
	}()
	select {
	case <-logic.started:
	case <-time.After(time.Second):
		t.Fatal("first request did not occupy backend slot")
	}
	response, err := gate.HandleBinary(context.Background(), "pool-conn", 1, commandFrame(3, "second"))
	if err != nil {
		t.Fatalf("busy response must not tear down connection: %v", err)
	}
	decoded, err := protocol.Decode(response, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var result commonpb.CommandResultEnvelope
	result = *decoded.GetCommandResult()
	if false {
		t.Fatal(err)
	}
	if result.Status != commonpb.CommandStatus_UNAVAILABLE ||
		result.Error == nil || result.Error.ErrorCode != commonpb.ErrorCode_ERR_SERVER_BUSY {
		t.Fatalf("busy result=%+v", &result)
	}
	close(logic.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("admitted request failed: %v", err)
	}
}

func TestAdmittedWriteDeadlineReturnsProcessingUnknown(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "write-timeout"})
	if err != nil {
		t.Fatal(err)
	}
	logic := &blockingLogic{started: make(chan struct{}), release: make(chan struct{})}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetBackendPool(1, 5*time.Millisecond)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "write-timeout-conn", 1, raw); err != nil {
		t.Fatal(err)
	}
	setRecoveryCompleteForTest(t, gate, "write-timeout-conn", 1, true)
	payload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	command, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
		OperationId: "buy-timeout-op", RequestId: "buy-timeout-req",
		CommandType: commonpb.CommandType_BUY, Payload: payload,
	})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, command)
	response, err := gate.HandleBinary(context.Background(), "write-timeout-conn", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.Decode(response, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	result := frame.GetCommandResult()
	if result == nil {
		t.Fatal("timeout response is not a command result")
	}
	if result.Status != commonpb.CommandStatus_PROCESSING ||
		result.Error == nil || result.Error.ErrorCode != commonpb.ErrorCode_ERR_TIMEOUT ||
		result.OperationId != "buy-timeout-op" {
		t.Fatalf("timeout result=%v", result)
	}
}

func TestGateSwitchFarmRevokesLocallyAndDoesNotWaitForOldCleanup(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "switch"})
	if err != nil {
		t.Fatal(err)
	}
	logic := &farmSwitchLogic{
		cleanupStarted: make(chan *logicrpcpb.CleanupSubscriptionRequest, 1),
		cleanupRelease: make(chan struct{}),
	}
	gate := NewGate(authSvc, logic, 64<<10)
	downlinks := downlink.NewConnectionRegistry()
	gate.SetDownlinkRegistry(downlinks)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "switch-conn", 7, raw); err != nil {
		t.Fatal(err)
	}
	sendJoin := func(sequence uint32, requestID string, farmID uint64) {
		t.Helper()
		body, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
			OperationId: "op-" + requestID, RequestId: requestID,
			CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, farmID),
		})
		frame := clientFrameRaw(t, testFrameClientCommand, uint64(sequence), body)
		if response, err := gate.HandleBinary(context.Background(), "switch-conn", 7, frame); err != nil || len(response) != 0 {
			t.Fatalf("Join response=%d err=%v", len(response), err)
		}
		completePendingJoinForTest(t, gate, downlinks, "switch-conn", 7, farmID, farmID)
	}
	sendJoin(2, "subscription-old", 101)
	sendJoin(3, "subscription-new", 202)
	var cleanup *logicrpcpb.CleanupSubscriptionRequest
	select {
	case cleanup = <-logic.cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("old CleanupSubscription was not issued")
	}
	close(logic.cleanupRelease)
	if cleanup.Subscription == nil || cleanup.Subscription.FarmId != 101 ||
		cleanup.Subscription.SubscriptionId != "server-subscription-old" ||
		cleanup.Subscription.JoinAttemptId == "" || cleanup.Target == nil ||
		cleanup.Target.ConnectionId != "switch-conn" || cleanup.Target.ConnectionGeneration != 7 ||
		cleanup.Reason != logicrpcpb.CleanupReason_CLEANUP_REASON_SWITCH_FARM {
		t.Fatalf("switch cleanup=%+v", cleanup)
	}

	logic.mu.Lock()
	defer logic.mu.Unlock()
	if len(logic.commands) != 2 {
		t.Fatalf("commands=%v, want JOIN old then JOIN new", logic.commands)
	}
	got := logic.commands
	if got[0].CommandType != commonpb.CommandType_JOIN_FARM ||
		got[1].CommandType != commonpb.CommandType_JOIN_FARM ||
		got[1].RequestId != "subscription-new" {
		t.Fatalf("farm switch order=%v", got)
	}
	// 旧 target_owner_id 兼容字段已删除:JOIN 目标只来自 Typed Payload。
	joinTarget := &publicpb.JoinFarmRequest{}
	if err := proto.Unmarshal(got[1].Payload, joinTarget); err != nil || joinTarget.GetTargetFarmId() != 202 {
		t.Fatalf("switch join typed target=%v err=%v", joinTarget, err)
	}
	if len(logic.trustedSubscriptions) != 2 || logic.trustedSubscriptions[1] != "" {
		t.Fatalf("Join commands unexpectedly carried old subscription=%v", logic.trustedSubscriptions)
	}
	gate.mu.Lock()
	current := gate.connections["switch-conn"]
	gate.mu.Unlock()
	if current.currentFarmID != 202 || current.currentSubscriptionID != "server-subscription-new" || current.currentPresenceEpoch != 202 {
		t.Fatalf("current farm context=%+v", current)
	}
}

func TestGateJoinRetryWithNewRequestIDKeepsSubscription(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	logic := &farmSwitchLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	downlinks := downlink.NewConnectionRegistry()
	gate.SetDownlinkRegistry(downlinks)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(context.Background(), "retry-conn", 1, raw); err != nil {
		t.Fatal(err)
	}
	sendJoin := func(sequence uint32, requestID string) {
		t.Helper()
		body, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
			OperationId: "join-intent", RequestId: requestID,
			CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, 101),
		})
		frame := clientFrameRaw(t, testFrameClientCommand, uint64(sequence), body)
		if _, err := gate.HandleBinary(context.Background(), "retry-conn", 1, frame); err != nil {
			t.Fatal(err)
		}
	}
	sendJoin(2, "trace-1")
	sendJoin(3, "trace-2")

	logic.mu.Lock()
	defer logic.mu.Unlock()
	if len(logic.commands) != 2 || logic.commands[0].CommandType != commonpb.CommandType_JOIN_FARM ||
		logic.commands[1].CommandType != commonpb.CommandType_JOIN_FARM {
		t.Fatalf("retry commands=%v, want two JOINs and no LEAVE", logic.commands)
	}
	gate.mu.Lock()
	current := gate.connections["retry-conn"]
	gate.mu.Unlock()
	if current.currentSubscriptionID != "" || current.pendingJoin == nil ||
		current.pendingJoin.operationID != "join-intent" ||
		current.pendingJoin.subscriptionID != "server-subscription-old" {
		t.Fatalf("retry pending Join=%+v active subscription=%q", current.pendingJoin, current.currentSubscriptionID)
	}
	if _, ok := downlinks.Dequeue("retry-conn", 1); !ok {
		t.Fatal("missing first queued ACCEPTED")
	}
	if _, ok := downlinks.Dequeue("retry-conn", 1); ok {
		t.Fatal("same operation retry queued duplicate ACCEPTED")
	}
}

func TestCrossGateKickUsesExactOldRouteFence(t *testing.T) {
	authority := session.New(session.Config{AuthorityEpoch: 1})
	ticketStore := ticket.NewMemoryStore()
	tickets := ticket.New(ticketStore)
	buildAuth := func(gatewayID, bootID string) *auth.Service {
		service, err := auth.New(staticVerifier{}, authority, tickets, auth.Config{
			GatewayID: gatewayID, GatewayBootID: bootID, WssEndpoint: "wss://x",
			PermitSecret: []byte("cross-gate-test-permit-secret"),
		})
		if err != nil {
			t.Fatal(err)
		}
		service.SetBootstrapper(testBootstrapper{})
		return service
	}
	authA := buildAuth("gate-a", "boot-a")
	authB := buildAuth("gate-b", "boot-b")
	gateA := NewGate(authA, &testLogic{}, 64<<10)
	downlinkA := downlink.NewConnectionRegistry()
	gateA.SetDownlinkRegistry(downlinkA)
	gateB := NewGate(authB, &testLogic{}, 64<<10)
	kicker := &localGateKicker{
		target: gateA, called: make(chan struct{}),
		deadlineRemaining: make(chan time.Duration, 1),
	}
	gateB.SetCrossGateKicker(kicker)
	gateB.SetCrossGateKickTimeout(2750 * time.Millisecond)

	activate := func(service *auth.Service, gate *Gate, connectionID string) {
		t.Helper()
		login, err := service.Login(context.Background(), auth.LoginRequest{
			PlatformToken: "p", BootstrapID: connectionID,
		})
		if err != nil {
			t.Fatal(err)
		}
		hello, _ := proto.Marshal(&commonpb.ClientHello{
			ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2",
		})
		raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
		if _, err := gate.HandleBinary(context.Background(), connectionID, 1, raw); err != nil {
			t.Fatal(err)
		}
	}
	activate(authA, gateA, "connection-a")
	activate(authB, gateB, "connection-b")
	select {
	case <-kicker.called:
	case <-time.After(time.Second):
		t.Fatal("new Gate did not call old GateControl target")
	}
	select {
	case remaining := <-kicker.deadlineRemaining:
		if remaining < 2*time.Second || remaining > 2750*time.Millisecond {
			t.Fatalf("cross-gate kick deadline remaining=%s, want configured 2.75s", remaining)
		}
	default:
		t.Fatal("cross-gate kick did not receive a deadline")
	}

	commandBody, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
		RequestId: "stale-write", CommandType: commonpb.CommandType(10), // 已删除的 GET_FARM_SNAPSHOT
	})
	commandFrame := clientFrameRaw(t, testFrameClientCommand, 2, commandBody)
	if _, err := gateA.HandleBinary(context.Background(), "connection-a", 1, commandFrame); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("old Gate admitted command after exact KICK: %v", err)
	}
	kickFrame, ok := downlinkA.Dequeue("connection-a", 1)
	if !ok {
		t.Fatal("old Gate did not enqueue KICK on the exact writer")
	}
	decoded, err := protocol.Decode(kickFrame, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	push := decoded.GetServerPush()
	if push == nil || push.PushType != commonpb.PushType_KICK {
		t.Fatalf("kick push=%v err=%v", push, err)
	}
}

func activateHello(t *testing.T, gate *Gate, authSvc *auth.Service, connectionID string) {
	activateHelloGeneration(t, gate, authSvc, connectionID, 1)
	// Most legacy Gate tests exercise a boundary other than RecoveryAdmission.
	// Keep their precondition explicit; recovery_admission_test.go drives the
	// real ClientRecoveryComplete control frame and verifies the default false.
	gate.mu.Lock()
	current := gate.connections[connectionID]
	current.recoveryComplete = true
	gate.connections[connectionID] = current
	gate.mu.Unlock()
}

func encodeCommandFrame(t *testing.T, requestID string, seq uint32) []byte {
	t.Helper()
	payload, err := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&commonpb.ClientCommandEnvelope{
		OperationId: "op-" + requestID, RequestId: requestID,
		CommandType: commonpb.CommandType_BUY, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := clientFrameRaw(t, testFrameClientCommand, uint64(seq), body)
	return raw
}

func setLocalRouteLeaseDeadline(t *testing.T, gate *Gate, connectionID string, generation uint64, deadline time.Time) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current, ok := gate.connections[connectionID]
	if !ok || current.generation != generation || current.binding == nil {
		t.Fatalf("connection %s gen=%d not ready for lease override", connectionID, generation)
	}
	current.routeLeaseDeadline = deadline
	gate.connections[connectionID] = current
}

func rejectedCounter(t *testing.T, registry *prometheus.Registry, reason string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "farm_gate_command_rejected_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func assertRejectReasonExclusive(t *testing.T, err error, want error, registry *prometheus.Registry) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err=%v, want %v", err, want)
	}
	if errors.Is(err, ErrRouteLeaseExpired) && errors.Is(err, ErrStaleSession) {
		t.Fatalf("reject reasons must be mutually exclusive, got %v", err)
	}
	leaseCount := rejectedCounter(t, registry, ReasonRouteLeaseExpired)
	staleCount := rejectedCounter(t, registry, ReasonStaleSession)
	switch {
	case errors.Is(err, ErrRouteLeaseExpired):
		if leaseCount != 1 || staleCount != 0 {
			t.Fatalf("lease reject metrics lease=%v stale=%v, want lease=1 stale=0", leaseCount, staleCount)
		}
		if errors.Is(err, ErrStaleSession) {
			t.Fatal("ROUTE_LEASE_EXPIRED must not also match ErrStaleSession")
		}
	case errors.Is(err, ErrStaleSession):
		if staleCount != 1 || leaseCount != 0 {
			t.Fatalf("stale reject metrics lease=%v stale=%v, want lease=0 stale=1", leaseCount, staleCount)
		}
		if errors.Is(err, ErrRouteLeaseExpired) {
			t.Fatal("STALE_SESSION must not also match ErrRouteLeaseExpired")
		}
	default:
		t.Fatalf("unexpected reject err=%v", err)
	}
}

// TestHandleCommandRejectsExpiredRouteLease covers Batch 0.4 scenario A:
// pure local monotonic deadline expiry returns ErrRouteLeaseExpired (not STALE).
func TestHandleCommandRejectsExpiredRouteLease(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "lease-conn")
	setLocalRouteLeaseDeadline(t, gate, "lease-conn", 1, time.Now().Add(-time.Second))
	gate.mu.Lock()
	gate.connections["lease-conn"].binding.Route.LeaseExpiresAt = time.Now().Add(24 * time.Hour)
	gate.mu.Unlock()

	_, err := gate.HandleBinary(context.Background(), "lease-conn", 1, encodeCommandFrame(t, "lease-expired", 2))
	assertRejectReasonExclusive(t, err, ErrRouteLeaseExpired, registry)
}

// TestHandleCommandRejectsStaleAfterKick covers Batch 0.4 scenario B:
// Kick marks STALE_SESSION and rejects with ErrStaleSession (not lease expired).
func TestHandleCommandRejectsStaleAfterKick(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "kick-conn")

	gate.mu.Lock()
	route := gate.connections["kick-conn"].binding.Route
	gate.mu.Unlock()
	resp, err := gate.KickSession(context.Background(), kickRequest(&route))
	if err != nil || resp.GetStatus() != logicrpcpb.GateKickStatus_KICKED {
		t.Fatalf("KickSession status=%v err=%v", resp.GetStatus(), err)
	}

	_, err = gate.HandleBinary(context.Background(), "kick-conn", 1, encodeCommandFrame(t, "after-kick", 2))
	assertRejectReasonExclusive(t, err, ErrStaleSession, registry)
}

// TestHandleCommandRejectReasonsAreMutualExclusive covers Batch 0.4 scenario C:
// a kicked lane whose lease is also past still returns only STALE_SESSION.
func TestHandleCommandRejectReasonsAreMutualExclusive(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "both-conn")

	gate.mu.Lock()
	route := gate.connections["both-conn"].binding.Route
	gate.mu.Unlock()
	if _, err := gate.KickSession(context.Background(), kickRequest(&route)); err != nil {
		t.Fatal(err)
	}
	setLocalRouteLeaseDeadline(t, gate, "both-conn", 1, time.Now().Add(-time.Minute))

	_, err := gate.HandleBinary(context.Background(), "both-conn", 1, encodeCommandFrame(t, "both-reasons", 2))
	assertRejectReasonExclusive(t, err, ErrStaleSession, registry)
}

// TestRenewSessionLeasesMarksUnrenewedFenceStale covers RenewRoutes Renewed=false
// becoming STALE_SESSION rather than ROUTE_LEASE_EXPIRED.
func TestRenewSessionLeasesMarksUnrenewedFenceStale(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "renew-conn")

	gate.mu.Lock()
	current := gate.connections["renew-conn"]
	gate.removeRouteLeaseLocked(current.binding.Route)
	current.binding.Route.RouteRevision++
	gate.connections["renew-conn"] = current
	gate.scheduleRouteLeaseLocked(current.binding.Route, current.routeLeaseDeadline, time.Now())
	gate.mu.Unlock()
	if err := gate.RenewSessionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := gate.HandleBinary(context.Background(), "renew-conn", 1, encodeCommandFrame(t, "after-renew-reject", 2))
	assertRejectReasonExclusive(t, err, ErrStaleSession, registry)
}

func TestRenewSessionLeasesReportsAuthorityExpiryWithoutStale(t *testing.T) {
	baseAuthority := session.New(session.Config{AuthorityEpoch: 1})
	authority := &leaseExpiredRenewAuthority{Authority: baseAuthority}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", WssEndpoint: "wss://x", RouteLeaseTTL: time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "authority-expired-conn")

	if err := gate.RenewSessionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := gate.HandleBinary(context.Background(), "authority-expired-conn", 1,
		encodeCommandFrame(t, "after-authority-expiry", 2))
	assertRejectReasonExclusive(t, err, ErrRouteLeaseExpired, registry)
}

func TestRenewSessionLeasesAppliesMixedResultsDespitePipelineError(t *testing.T) {
	authority := &mixedRenewAuthority{Authority: session.New(session.Config{AuthorityEpoch: 1})}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x", RouteLeaseTTL: 30 * time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	oldDeadline := time.Now().Add(20 * time.Second)
	gate.mu.Lock()
	for index, connectionID := range []string{"route-success", "route-lost", "route-transient", "route-overlong"} {
		route := session.ActiveRoute{
			PlayerID: uint64(index + 1), SessionID: fmt.Sprintf("session-%d", index+1),
			RouteFence: session.RouteFence{
				GatewayID: "gw", GatewayBootID: "boot", ConnectionID: connectionID, ConnectionGeneration: 1,
			},
			SessionEpoch: 1, RouteRevision: 1,
		}
		binding := &auth.SessionBinding{
			PlayerID: route.PlayerID, SessionID: route.SessionID, SessionAuthorityEpoch: 1,
			ActiveSessionEpoch: 1, Route: route,
		}
		gate.connections[connectionID] = connection{generation: 1, binding: binding, routeLeaseDeadline: oldDeadline}
		gate.scheduleRouteLeaseLocked(route, oldDeadline, time.Now())
	}
	gate.mu.Unlock()

	if err := gate.RenewSessionLeases(context.Background()); err == nil {
		t.Fatal("aggregate pipeline error must remain visible")
	}
	gate.mu.Lock()
	success := gate.connections["route-success"]
	lost := gate.connections["route-lost"]
	transient := gate.connections["route-transient"]
	overlong := gate.connections["route-overlong"]
	gate.mu.Unlock()
	if !success.routeLeaseDeadline.After(oldDeadline) || success.admission != laneAdmissible {
		t.Fatalf("valid sibling was not applied: %+v", success)
	}
	if lost.admission != laneStale {
		t.Fatalf("explicit lost fence not made stale: %+v", lost)
	}
	if !transient.routeLeaseDeadline.Equal(oldDeadline) || transient.admission != laneAdmissible {
		t.Fatalf("transient item extended or lost old trust: %+v", transient)
	}
	if !overlong.routeLeaseDeadline.Equal(oldDeadline) || overlong.admission != laneAdmissible {
		t.Fatalf("overlong accepted ttl extended local trust: %+v", overlong)
	}
}

func TestRenewSessionLeasesRejectsMismatchedResponseFence(t *testing.T) {
	baseAuthority := session.New(session.Config{AuthorityEpoch: 1})
	authority := &mismatchedRenewAuthority{Authority: baseAuthority}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x", RouteLeaseTTL: time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "renew-mismatch-conn")

	if err := gate.RenewSessionLeases(context.Background()); err == nil {
		t.Fatal("mismatched authority response fence was accepted")
	}
	if _, err := gate.HandleBinary(context.Background(), "renew-mismatch-conn", 1,
		encodeCommandFrame(t, "after-renew-mismatch", 2)); err != nil {
		t.Fatalf("malformed renewal response must preserve prior trusted deadline: %v", err)
	}
}

func TestRenewSessionLeasesSerializesOverlappingRounds(t *testing.T) {
	baseAuthority := session.New(session.Config{AuthorityEpoch: 1})
	authority := &concurrentRenewAuthority{Authority: baseAuthority}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x", RouteLeaseTTL: time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "renew-serialized-conn")

	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			errs <- gate.RenewSessionLeases(context.Background())
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	authority.mu.Lock()
	maxInFlight := authority.maxInFlight
	authority.mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("renewal rounds overlapped: max in flight=%d", maxInFlight)
	}
}

// TestHandleCommandSucceedsWhileLeaseAndLaneHealthy is a Batch 0.4 regression:
// an active, unexpired SessionLane still admits business commands.
func TestHandleCommandSucceedsWhileLeaseAndLaneHealthy(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, "healthy-conn")

	if _, err := gate.HandleBinary(context.Background(), "healthy-conn", 1, encodeCommandFrame(t, "healthy", 2)); err != nil {
		t.Fatalf("healthy command rejected: %v", err)
	}
	if got := rejectedCounter(t, registry, ReasonRouteLeaseExpired); got != 0 {
		t.Fatalf("lease rejected counter=%v, want 0", got)
	}
	if got := rejectedCounter(t, registry, ReasonStaleSession); got != 0 {
		t.Fatalf("stale rejected counter=%v, want 0", got)
	}
}

func TestRouteLeaseAdmissionIgnoresAuthorityAbsoluteTimestamp(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "absolute-clock-conn")

	gate.mu.Lock()
	current := gate.connections["absolute-clock-conn"]
	current.binding.Route.LeaseExpiresAt = time.UnixMilli(1)
	current.routeLeaseDeadline = time.Now().Add(time.Minute)
	gate.connections["absolute-clock-conn"] = current
	gate.mu.Unlock()

	if _, err := gate.HandleBinary(context.Background(), "absolute-clock-conn", 1,
		encodeCommandFrame(t, "ignore-remote-wall-clock", 2)); err != nil {
		t.Fatalf("authority absolute timestamp affected local admission: %v", err)
	}
}

func TestExpiredRouteLeaseIsTerminalForConnectionGeneration(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "terminal-lease-conn")
	setLocalRouteLeaseDeadline(t, gate, "terminal-lease-conn", 1, time.Now().Add(-time.Second))

	if _, err := gate.HandleBinary(context.Background(), "terminal-lease-conn", 1,
		encodeCommandFrame(t, "expire-terminal", 2)); !errors.Is(err, ErrRouteLeaseExpired) {
		t.Fatalf("first expiry err=%v", err)
	}
	if err := gate.RenewSessionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	current := gate.connections["terminal-lease-conn"]
	gate.mu.Unlock()
	if current.admission != laneRouteLeaseExpired {
		t.Fatalf("expired lane resurrected by renew: admission=%v", current.admission)
	}
}

func TestCommandAdmittedBeforeRouteLeaseExpiryCanFinish(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &blockingLogic{started: make(chan struct{}), release: make(chan struct{})}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetBackendPool(1, time.Second)
	activateHello(t, gate, authSvc, "inflight-lease-conn")
	setLocalRouteLeaseDeadline(t, gate, "inflight-lease-conn", 1, time.Now().Add(30*time.Millisecond))

	result := make(chan error, 1)
	frame := encodeCommandFrame(t, "admitted-before-expiry", 2)
	go func() {
		_, err := gate.HandleBinary(context.Background(), "inflight-lease-conn", 1, frame)
		result <- err
	}()
	select {
	case <-logic.started:
	case <-time.After(time.Second):
		t.Fatal("command never crossed ADMITTED boundary")
	}
	time.Sleep(40 * time.Millisecond)
	if err := gate.rejectCommandAdmission("inflight-lease-conn", 1); !errors.Is(err, ErrRouteLeaseExpired) {
		t.Fatalf("lease did not expire while command was in flight: %v", err)
	}
	close(logic.release)
	if err := <-result; err != nil {
		t.Fatalf("already admitted command was cancelled by lease expiry: %v", err)
	}
}

func TestConservativeRouteLeaseDeadlineUsesRequestStart(t *testing.T) {
	start := time.Now()
	deadline, ok := conservativeRouteLeaseDeadline(start, 30*time.Second, 2*time.Second)
	if !ok {
		t.Fatal("valid lease timing rejected")
	}
	if want := start.Add(28 * time.Second); !deadline.Equal(want) {
		t.Fatalf("deadline=%v want=%v", deadline, want)
	}
	if _, ok := conservativeRouteLeaseDeadline(start, time.Second, time.Second); ok {
		t.Fatal("safety margin equal to accepted ttl must fail closed")
	}
}

func TestLeaseDeadlineRequiresWorstCaseScheduleAndDispatchWindow(t *testing.T) {
	now := time.Now()
	cfg := leasebatch.Config{
		Limits:   leasebatch.Limits{MaxDispatchDuration: 3 * time.Second},
		Schedule: leasebatch.Schedule{Interval: 10 * time.Second},
	}
	if leaseDeadlineHasSchedulingWindow(now, now.Add(13*time.Second), cfg) {
		t.Fatal("deadline equal to interval plus dispatch must fail closed")
	}
	if !leaseDeadlineHasSchedulingWindow(now, now.Add(13*time.Second+time.Nanosecond), cfg) {
		t.Fatal("deadline beyond proven scheduling window rejected")
	}
}

func TestStableLeaseIdentityIsUnambiguousForDelimitedIDs(t *testing.T) {
	left := stableLeaseIdentity("route", "gate:a", "boot")
	right := stableLeaseIdentity("route", "gate", "a:boot")
	if left == right {
		t.Fatalf("length-prefixed identities collided: %q", left)
	}
	if left != stableLeaseIdentity("route", "gate:a", "boot") {
		t.Fatal("stable identity changed for the same complete fence")
	}
}

func TestRenewSessionLeaseChargesRPCLatencyAgainstAcceptedTTL(t *testing.T) {
	baseAuthority := session.New(session.Config{AuthorityEpoch: 1})
	authority := &delayedRenewAuthority{
		Authority: baseAuthority, delay: 200 * time.Millisecond, acceptedTTL: time.Second,
	}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", WssEndpoint: "wss://x", RouteLeaseTTL: time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	if err := gate.SetRouteLeaseSafetyMargin(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	activateHello(t, gate, authSvc, "renew-latency-conn")

	if err := gate.RenewSessionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	authority.mu.Lock()
	renewStarted := authority.lastStart
	authority.mu.Unlock()
	gate.mu.Lock()
	deadline := gate.connections["renew-latency-conn"].routeLeaseDeadline
	gate.mu.Unlock()

	if got, want := deadline.Sub(renewStarted), 900*time.Millisecond; got < want-50*time.Millisecond || got > want+50*time.Millisecond {
		t.Fatalf("local deadline budget=%s want about %s", got, want)
	}
	if remaining := time.Until(deadline); remaining >= 850*time.Millisecond {
		t.Fatalf("RPC latency did not consume accepted ttl: remaining=%s", remaining)
	}
}

func TestStopLeaseRenewalSchedulersRejectsLateRouteRenew(t *testing.T) {
	baseAuthority := session.New(session.Config{AuthorityEpoch: 1})
	authority := &delayedRenewAuthority{
		Authority: baseAuthority, delay: 100 * time.Millisecond, acceptedTTL: time.Second,
	}
	authSvc := newTestAuthWithAuthority(t, authority, auth.Config{
		GatewayID: "gw", GatewayBootID: "boot", WssEndpoint: "wss://x", RouteLeaseTTL: time.Second,
	})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "drain-late-renew")
	gate.mu.Lock()
	before := gate.connections["drain-late-renew"].routeLeaseDeadline
	gate.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- gate.RenewSessionLeases(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		authority.mu.Lock()
		started := !authority.lastStart.IsZero()
		authority.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renew dispatch did not start")
		}
		time.Sleep(time.Millisecond)
	}
	gate.StopLeaseRenewalSchedulers()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	after := gate.connections["drain-late-renew"].routeLeaseDeadline
	gate.mu.Unlock()
	if !after.Equal(before) {
		t.Fatalf("late renew crossed drain fence: before=%v after=%v", before, after)
	}
	routeBacklog, subscriptionBacklog := gate.leaseBatchBacklog()
	if routeBacklog != 0 || subscriptionBacklog != 0 {
		t.Fatalf("drain backlog route=%d subscription=%d", routeBacklog, subscriptionBacklog)
	}
}

// TestHandleCommandRejectsUnregisteredAndUnspecifiedCommand covers Batch 1
// Slice A / contract 28 §6.2: CommandType must be in CommandCatalog.
func TestHandleCommandRejectsUnregisteredAndUnspecifiedCommand(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "catalog-conn")

	cases := []struct {
		name        string
		commandType commonpb.CommandType
		seq         uint64
	}{
		{name: "unspecified", commandType: commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED, seq: 2},
		{name: "unregistered", commandType: commonpb.CommandType(9999), seq: 3},
		// 已删除命令(旧 GET_FARM_SNAPSHOT=10/LEAVE_FARM=7)同样未注册,不得存在双主链入口。
		{name: "removed", commandType: commonpb.CommandType(10), seq: 4},
		{name: "removed-leave", commandType: commonpb.CommandType(7), seq: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := rejectedCounter(t, registry, ReasonUnknownCommand)
			body, err := proto.Marshal(&commonpb.ClientCommandEnvelope{
				RequestId: "bad-" + tc.name, CommandType: tc.commandType,
			})
			if err != nil {
				t.Fatal(err)
			}
			raw := clientFrameRaw(t, testFrameClientCommand, tc.seq, body)
			_, err = gate.HandleBinary(context.Background(), "catalog-conn", 1, raw)
			if !errors.Is(err, ErrBadMessage) {
				t.Fatalf("err=%v, want ErrBadMessage", err)
			}
			if got := rejectedCounter(t, registry, ReasonUnknownCommand); got != before+1 {
				t.Fatalf("UNKNOWN_COMMAND counter=%v, want %v", got, before+1)
			}
		})
	}
}

// TestHandleCommandAdmitsRegisteredCommandUnderHealthyLease locks Slice A:
// a catalogued command still succeeds after Hello.
func TestHandleCommandAdmitsRegisteredCommandUnderHealthyLease(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "admit-conn")

	resp, err := gate.HandleBinary(context.Background(), "admit-conn", 1, encodeCommandFrame(t, "admit-ok", 2))
	if err != nil {
		t.Fatalf("registered command rejected: %v", err)
	}
	decoded, err := protocol.Decode(resp, 64<<10)
	if err != nil || decoded.GetCommandResult() == nil {
		t.Fatalf("response=%v err=%v", decoded, err)
	}
	if got := rejectedCounter(t, registry, ReasonUnknownCommand); got != 0 {
		t.Fatalf("UNKNOWN_COMMAND counter=%v, want 0", got)
	}
}

// TestHandleBinaryRequiresStrictConnectionSequencePlusOne covers Batch 1.6:
// after Hello (lastSeq=1) and one command (lastSeq=2), skip/rewind reject and
// exact last+1 succeeds. This must fail if Gate only checks <= lastSeq.
func TestHandleBinaryRequiresStrictConnectionSequencePlusOne(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	activateHello(t, gate, authSvc, "seq-conn")

	if _, err := gate.HandleBinary(context.Background(), "seq-conn", 1, encodeCommandFrame(t, "seq-2", 2)); err != nil {
		t.Fatalf("seq=2 should succeed after hello: %v", err)
	}
	gate.mu.Lock()
	if gate.connections["seq-conn"].lastSeq != 2 {
		gate.mu.Unlock()
		t.Fatalf("lastSeq=%d, want 2 before skip/rewind probes", gate.connections["seq-conn"].lastSeq)
	}
	gate.mu.Unlock()

	_, err := gate.HandleBinary(context.Background(), "seq-conn", 1, encodeCommandFrame(t, "seq-skip", 4))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("skip seq=4 err=%v, want ErrBadMessage", err)
	}
	if err == nil || !containsAll(err.Error(), "got 4", "want 3") {
		t.Fatalf("skip error should name got/want, got %v", err)
	}

	_, err = gate.HandleBinary(context.Background(), "seq-conn", 1, encodeCommandFrame(t, "seq-rewind", 2))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("rewind seq=2 err=%v, want ErrBadMessage", err)
	}
	if err == nil || !containsAll(err.Error(), "got 2", "want 3") {
		t.Fatalf("rewind error should name got/want, got %v", err)
	}

	gate.mu.Lock()
	if gate.connections["seq-conn"].lastSeq != 2 {
		gate.mu.Unlock()
		t.Fatalf("rejected frames must not advance lastSeq, got %d", gate.connections["seq-conn"].lastSeq)
	}
	gate.mu.Unlock()

	if _, err := gate.HandleBinary(context.Background(), "seq-conn", 1, encodeCommandFrame(t, "seq-3", 3)); err != nil {
		t.Fatalf("seq=3 (last+1) should succeed: %v", err)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

// countingLogic counts ExecuteCommand calls so Slice B admission tests can
// assert logic is not invoked on target-policy rejects.
type countingLogic struct {
	mu    sync.Mutex
	calls int
	inner LogicClient
}

func (l *countingLogic) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	if l.inner != nil {
		return l.inner.ExecuteCommand(ctx, req)
	}
	return (&testLogic{}).ExecuteCommand(ctx, req)
}

func (l *countingLogic) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func encodeTypedCommandFrame(t *testing.T, seq uint32, cmd *commonpb.ClientCommandEnvelope) []byte {
	t.Helper()
	body, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	raw := clientFrameRaw(t, testFrameClientCommand, uint64(seq), body)
	return raw
}

func encodeJoinSnapshotPush(t *testing.T, subscriptionID string) []byte {
	t.Helper()
	snapshot, err := proto.Marshal(&farmpb.FarmSnapshot{OwnerId: 101, FarmVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := proto.Marshal(protocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: 101, TargetFarmId: 101, SubscriptionId: subscriptionID,
			},
		},
		Payload: snapshot,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func encodeW2JoinSnapshotPush(t *testing.T, targetPlayerID, farmID uint64,
	subscriptionID, attemptID string, version uint64) []byte {
	t.Helper()
	payload, err := proto.Marshal(&farmpb.FarmSnapshotPush{
		JoinAttemptId: attemptID, SnapshotVersion: version,
		Snapshot: &farmpb.FarmSnapshot{OwnerId: farmID, FarmVersion: version},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := proto.Marshal(protocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: targetPlayerID, TargetFarmId: farmID, SubscriptionId: subscriptionID,
			},
		},
		Payload: payload,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func completePendingJoinForTest(t *testing.T, gate *Gate, registry *downlink.ConnectionRegistry,
	connectionID string, generation, farmID, presenceEpoch uint64) {
	t.Helper()
	acceptedRaw, ok := registry.Dequeue(connectionID, generation)
	if !ok {
		t.Fatal("missing queued Join ACCEPTED")
	}
	var acceptedFrame commonpb.FarmFrame
	if err := proto.Unmarshal(acceptedRaw, &acceptedFrame); err != nil {
		t.Fatal(err)
	}
	accepted := acceptedFrame.GetCommandResult()
	if accepted == nil {
		t.Fatal("queued Join ACCEPTED is not a command result")
	}
	join := accepted.GetJoin()
	if accepted.Status != commonpb.CommandStatus_ACCEPTED || join == nil ||
		join.TargetFarmId != farmID || join.SubscriptionId == "" || join.JoinAttemptId == "" {
		t.Fatalf("Join ACCEPTED=%v", accepted)
	}
	gate.mu.Lock()
	current := gate.connections[connectionID]
	var binding auth.SessionBinding
	if current.binding != nil {
		binding = *current.binding
	}
	gate.mu.Unlock()
	if binding.PlayerID == 0 {
		t.Fatal("missing test SessionBinding")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: binding.PlayerID, GatewayId: binding.Route.GatewayID,
		GatewayBootId: binding.Route.GatewayBootID, ConnectionId: connectionID,
		ConnectionGeneration: generation, SessionEpoch: binding.ActiveSessionEpoch,
		SessionRouteRevision: binding.Route.RouteRevision,
	}
	fence := &logicrpcpb.SubscriptionFence{
		FarmId: farmID, SubscriptionId: join.SubscriptionId, JoinAttemptId: join.JoinAttemptId,
	}
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: join.SubscriptionId + ":snapshot:1", SubscriptionId: join.SubscriptionId,
			GatewayId: target.GatewayId, ConnectionId: connectionID,
			TransportGeneration: generation, PresenceEpoch: presenceEpoch,
			Frame:     encodeW2JoinSnapshotPush(t, binding.PlayerID, farmID, join.SubscriptionId, join.JoinAttemptId, 1),
			JoinFence: fence, TargetFence: target, SnapshotVersion: 1,
		}},
	})
	if err != nil || response.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("Join Snapshot=%v err=%v", response, err)
	}
	ready, err := registry.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: fence, Target: target, ReadyThroughVersion: 1,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("Join Ready=%v err=%v", ready, err)
	}
	if _, ok := registry.Dequeue(connectionID, generation); !ok {
		t.Fatal("missing queued Snapshot")
	}
	if _, ok := registry.Dequeue(connectionID, generation); !ok {
		t.Fatal("missing queued Ready")
	}
}

func joinOwnFarm(t *testing.T, gate *Gate, connectionID string, seq uint32, farmID uint64) {
	t.Helper()
	frame := encodeTypedCommandFrame(t, seq, &commonpb.ClientCommandEnvelope{
		OperationId: "op-join-" + connectionID, RequestId: "join-" + connectionID,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, farmID),
	})
	if _, err := gate.HandleBinary(context.Background(), connectionID, 1, frame); err != nil {
		t.Fatalf("JOIN_FARM: %v", err)
	}
	// These tests exercise CommandCatalog target admission, not the distributed
	// Join barrier. Install the already-verified ActiveSubscription precondition
	// directly while preserving the consumed client sequence number.
	gate.mu.Lock()
	current := gate.connections[connectionID]
	current.currentFarmID = farmID
	current.currentSubscriptionID = "test-active-" + connectionID
	current.currentJoinAttemptID = "test-attempt-" + connectionID
	current.currentPresenceEpoch = 1
	current.subscriptionLeaseDeadline = time.Now().Add(time.Minute)
	current.pendingJoin = nil
	gate.connections[connectionID] = current
	gate.mu.Unlock()
}

// TestHandleCommandRejectsPlantTargetNotSelf covers 1.9 OWN_CURRENT_FARM:
// Plant with typed target_farm_id != playerID is rejected before logic is called.
func TestHandleCommandRejectsPlantTargetNotSelf(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "plant-not-self")
	joinOwnFarm(t, gate, "plant-not-self", 2, 101)
	before := logic.callCount()

	plantPayload, _ := proto.Marshal(&publicpb.PlantRequest{TargetFarmId: 202, CropDefinitionId: 1, ExpectedPlotVersion: 1})
	_, err := gate.HandleBinary(context.Background(), "plant-not-self", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-other", OperationId: "plant-other-op",
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != before {
		t.Fatalf("logic calls=%d, want %d (logic must not be invoked)", logic.callCount(), before)
	}
	if got := rejectedCounter(t, registry, ReasonOwnCurrentFarmMismatch); got != 1 {
		t.Fatalf("OWN_CURRENT_FARM_MISMATCH counter=%v, want 1", got)
	}
}

// TestHandleCommandAdmitsPlantOnOwnJoinedFarm covers Plant after JOIN self:
// target=playerID==currentFarmID reaches logic.
func TestHandleCommandAdmitsPlantOnOwnJoinedFarm(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	activateHello(t, gate, authSvc, "plant-ok")
	joinOwnFarm(t, gate, "plant-ok", 2, 101)
	before := logic.callCount()

	plantPayload, _ := proto.Marshal(&publicpb.PlantRequest{TargetFarmId: 101, CropDefinitionId: 1, ExpectedPlotVersion: 1})
	if _, err := gate.HandleBinary(context.Background(), "plant-ok", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-self", OperationId: "plant-self-op",
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	})); err != nil {
		t.Fatalf("plant on own farm rejected: %v", err)
	}
	if logic.callCount() != before+1 {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before+1)
	}
}

// TestHandleCommandRejectsTheftTargetNotCurrentFarm covers CURRENT_FARM:
// Theft target ≠ currentFarmID rejects without calling logic.
func TestHandleCommandRejectsTheftTargetNotCurrentFarm(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "theft-mismatch")
	joinOwnFarm(t, gate, "theft-mismatch", 2, 101)
	before := logic.callCount()

	theftPayload, _ := proto.Marshal(&publicpb.TheftRequest{TargetFarmId: 202})
	_, err := gate.HandleBinary(context.Background(), "theft-mismatch", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "theft-bad", OperationId: "theft-bad-op",
		CommandType: commonpb.CommandType_THEFT, Payload: theftPayload,
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != before {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before)
	}
	if got := rejectedCounter(t, registry, ReasonCurrentFarmMismatch); got != 1 {
		t.Fatalf("CURRENT_FARM_MISMATCH counter=%v, want 1", got)
	}
}

// TestHandleCommandRejectsWaterTargetNotCurrentFarm covers CURRENT_FARM for Water.
func TestHandleCommandRejectsWaterTargetNotCurrentFarm(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "water-mismatch")
	joinOwnFarm(t, gate, "water-mismatch", 2, 101)
	before := logic.callCount()

	waterPayload, _ := proto.Marshal(&publicpb.CareRequest{TargetFarmId: 202, CareType: publicpb.CareType_CARE_TYPE_WATER})
	_, err := gate.HandleBinary(context.Background(), "water-mismatch", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "water-bad", OperationId: "water-bad-op",
		CommandType: commonpb.CommandType_CARE, Payload: waterPayload,
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != before {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before)
	}
	if got := rejectedCounter(t, registry, ReasonCurrentFarmMismatch); got != 1 {
		t.Fatalf("CURRENT_FARM_MISMATCH counter=%v, want 1", got)
	}
}

// TestHandleCommandAdmitsTheftOnCurrentFarm covers Theft after JOIN to target farm.
func TestHandleCommandAdmitsTheftOnCurrentFarm(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	activateHello(t, gate, authSvc, "theft-ok")
	joinOwnFarm(t, gate, "theft-ok", 2, 202)
	before := logic.callCount()

	theftPayload, _ := proto.Marshal(&publicpb.TheftRequest{TargetFarmId: 202})
	if _, err := gate.HandleBinary(context.Background(), "theft-ok", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "theft-ok", OperationId: "theft-ok-op",
		CommandType: commonpb.CommandType_THEFT, Payload: theftPayload,
	})); err != nil {
		t.Fatalf("theft on current farm rejected: %v", err)
	}
	if logic.callCount() != before+1 {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before+1)
	}
}

// TestHandleCommandAdmitsWaterOnCurrentFarm covers Water after JOIN to a
// (possibly friend) farm: target==currentFarmID reaches logic.
func TestHandleCommandAdmitsWaterOnCurrentFarm(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	activateHello(t, gate, authSvc, "water-ok")
	// Join a non-self farm to prove CURRENT_FARM allows friend targets.
	joinOwnFarm(t, gate, "water-ok", 2, 202)
	before := logic.callCount()

	waterPayload, _ := proto.Marshal(&publicpb.CareRequest{TargetFarmId: 202, CareType: publicpb.CareType_CARE_TYPE_WATER})
	if _, err := gate.HandleBinary(context.Background(), "water-ok", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "water-friend", OperationId: "water-friend-op",
		CommandType: commonpb.CommandType_CARE, Payload: waterPayload,
	})); err != nil {
		t.Fatalf("water on current farm rejected: %v", err)
	}
	if logic.callCount() != before+1 {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before+1)
	}
}

// TestHandleCommandRejectsPlantWithoutSubscription covers OWN_CURRENT_FARM
// when the connection has not JOIN'd any farm yet.
func TestHandleCommandRejectsPlantWithoutSubscription(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "plant-no-sub")
	before := logic.callCount()

	plantPayload, _ := proto.Marshal(&farmpb.PlantCropRequest{PlotIndex: 0, CropDefinitionId: 1})
	_, err := gate.HandleBinary(context.Background(), "plant-no-sub", 1, encodeTypedCommandFrame(t, 2, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-nosub", OperationId: "plant-nosub-op",
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != before {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before)
	}
	if got := rejectedCounter(t, registry, ReasonSubscriptionRequired); got != 1 {
		t.Fatalf("SUBSCRIPTION_REQUIRED counter=%v, want 1", got)
	}
}

func admittedCounter(t *testing.T, registry *prometheus.Registry) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "farm_gate_command_admission_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "stage" && label.GetValue() == StageAdmitted {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func encodeTypedCommandFrameMax(t *testing.T, seq uint64, cmd *commonpb.ClientCommandEnvelope, maxFrame int) []byte {
	t.Helper()
	raw, err := protocol.Encode(protocol.NewClientCommandFrame(seq, cmd), maxFrame)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestHandleCommandRejectsPayloadTooLarge covers Slice C1: finite MaxPayloadBytes
// rejects oversize payload before logic.ExecuteCommand.
func TestHandleCommandRejectsPayloadTooLarge(t *testing.T) {
	const maxFrame = 256 << 10
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, maxFrame)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "payload-over")
	beforeLogic := logic.callCount()
	beforeAdmitted := admittedCounter(t, registry)

	over := make([]byte, commandcatalog.DefaultMaxPayloadBytes+1)
	_, err := gate.HandleBinary(context.Background(), "payload-over", 1, encodeTypedCommandFrameMax(t, 2, &commonpb.ClientCommandEnvelope{
		RequestId: "snap-over", OperationId: "op-snap-over",
		CommandType: commonpb.CommandType_BUY, Payload: over,
	}, maxFrame))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != beforeLogic {
		t.Fatalf("logic calls=%d, want %d (must not reach logic)", logic.callCount(), beforeLogic)
	}
	if got := rejectedCounter(t, registry, ReasonPayloadTooLarge); got != 1 {
		t.Fatalf("PAYLOAD_TOO_LARGE counter=%v, want 1", got)
	}
	if got := admittedCounter(t, registry); got != beforeAdmitted {
		t.Fatalf("ADMITTED=%v, want unchanged %v", got, beforeAdmitted)
	}
}

// TestHandleCommandAdmitsPayloadAtMaxBytes covers Slice C1: len == MaxPayloadBytes passes size gate.
func TestHandleCommandAdmitsPayloadAtMaxBytes(t *testing.T) {
	const maxFrame = 256 << 10
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, maxFrame)
	activateHello(t, gate, authSvc, "payload-eq")
	before := logic.callCount()

	exact := paddedBuyPayload(t, commandcatalog.DefaultMaxPayloadBytes)
	if _, err := gate.HandleBinary(context.Background(), "payload-eq", 1, encodeTypedCommandFrameMax(t, 2, &commonpb.ClientCommandEnvelope{
		RequestId: "snap-eq", OperationId: "op-snap-eq",
		CommandType: commonpb.CommandType_BUY, Payload: exact,
	}, maxFrame)); err != nil {
		t.Fatalf("exact-limit payload rejected: %v", err)
	}
	if logic.callCount() != before+1 {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), before+1)
	}
}

// TestPayloadExceedsLimitUnlimitedNotRejected covers Slice C1 的等价不变量:
// MaxPayloadBytesUnlimited 永不因大小拒绝(28 §6.1 要求每条命令声明显式
// cfg.max_payload.*,当前 33 条全部使用有界上限,Unlimited 仅为常量语义)。
func TestPayloadExceedsLimitUnlimitedNotRejected(t *testing.T) {
	if commandcatalog.PayloadExceedsLimit(commandcatalog.MaxPayloadBytesUnlimited, commandcatalog.DefaultMaxPayloadBytes+1) {
		t.Fatal("unlimited payload must never be rejected by size")
	}
	for _, spec := range commandcatalog.All() {
		if spec.MaxPayloadBytes == commandcatalog.MaxPayloadBytesUnlimited {
			t.Fatalf("%s must declare a bounded cfg.max_payload.*", spec.CommandType)
		}
	}
}

// TestHandleCommandRejectsMalformedPlantPayload covers Slice C2: illegal protobuf
// for PLANT is rejected; countingLogic must not advance.
func TestHandleCommandRejectsMalformedPlantPayload(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "plant-bad-bytes")
	joinOwnFarm(t, gate, "plant-bad-bytes", 2, 101)
	beforeLogic := logic.callCount()
	beforeAdmitted := admittedCounter(t, registry)

	_, err := gate.HandleBinary(context.Background(), "plant-bad-bytes", 1, encodeTypedCommandFrame(t, 3, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-malformed", OperationId: "plant-malformed-op",
		CommandType: commonpb.CommandType_PLANT,
		// Invalid protobuf wire (varint overflow) — must fail Unmarshal.
		Payload: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02},
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("err=%v, want ErrBadMessage", err)
	}
	if logic.callCount() != beforeLogic {
		t.Fatalf("logic calls=%d, want %d", logic.callCount(), beforeLogic)
	}
	if got := rejectedCounter(t, registry, ReasonPayloadDecodeFail); got != 1 {
		t.Fatalf("PAYLOAD_DECODE_FAILED counter=%v, want 1", got)
	}
	if got := admittedCounter(t, registry); got != beforeAdmitted {
		t.Fatalf("ADMITTED=%v, want unchanged %v on reject", got, beforeAdmitted)
	}
}

// TestHandleCommandAdmittedMetricOnSuccessAndReject covers Slice C3.
func TestHandleCommandAdmittedMetricOnSuccessAndReject(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "admit-metric")

	if _, err := gate.HandleBinary(context.Background(), "admit-metric", 1, encodeCommandFrame(t, "snap-admit", 2)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := admittedCounter(t, registry); got != 1 {
		t.Fatalf("ADMITTED after snapshot=%v, want 1", got)
	}

	joinOwnFarm(t, gate, "admit-metric", 3, 101)
	if got := admittedCounter(t, registry); got != 2 {
		t.Fatalf("ADMITTED after join=%v, want 2", got)
	}

	plantPayload, _ := proto.Marshal(&publicpb.PlantRequest{TargetFarmId: 101, CropDefinitionId: 1, ExpectedPlotVersion: 1})
	if _, err := gate.HandleBinary(context.Background(), "admit-metric", 1, encodeTypedCommandFrame(t, 4, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-admit", OperationId: "plant-admit-op",
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	})); err != nil {
		t.Fatalf("plant: %v", err)
	}
	if got := admittedCounter(t, registry); got != 3 {
		t.Fatalf("ADMITTED after plant=%v, want 3", got)
	}

	beforeRejectAdmitted := admittedCounter(t, registry)
	rejectPayload, _ := proto.Marshal(&publicpb.PlantRequest{TargetFarmId: 202, CropDefinitionId: 1, ExpectedPlotVersion: 1})
	_, err := gate.HandleBinary(context.Background(), "admit-metric", 1, encodeTypedCommandFrame(t, 5, &commonpb.ClientCommandEnvelope{
		RequestId: "plant-reject", OperationId: "plant-reject-op",
		CommandType: commonpb.CommandType_PLANT, Payload: rejectPayload,
	}))
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("reject plant err=%v, want ErrBadMessage", err)
	}
	if got := admittedCounter(t, registry); got != beforeRejectAdmitted {
		t.Fatalf("ADMITTED after reject=%v, want %v", got, beforeRejectAdmitted)
	}
	if got := rejectedCounter(t, registry, ReasonOwnCurrentFarmMismatch); got != 1 {
		t.Fatalf("OWN_CURRENT_FARM_MISMATCH=%v, want 1", got)
	}
}

// TestCommandResultBypassesSubscriptionFence covers Slice C4.3: sync CommandResult
// is returned even when downlink would STALE a mismatched farm subscription push.
func TestCommandResultBypassesSubscriptionFence(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	registry := downlink.NewConnectionRegistry()
	logic := &countingLogic{}
	gate := NewGate(authSvc, logic, 64<<10)
	gate.SetDownlinkRegistry(registry)
	activateHello(t, gate, authSvc, "cmd-result-fence")
	joinOwnFarm(t, gate, "cmd-result-fence", 2, 101)

	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_CHANGE,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: 101, TargetFarmId: 101, SubscriptionId: "wrong-sub",
			},
		},
		Payload: []byte("x"),
	}
	// ConnectionSeq 由 Gate ConnectionWriter 在入队时分配;此处占位 1。
	pushFrame, err := proto.Marshal(protocol.NewServerPushFrame(1, push))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "change-wrong", SubscriptionId: "wrong-sub", ConnectionId: "cmd-result-fence",
		TransportGeneration: 1, PresenceEpoch: 1, Frame: pushFrame,
	}}})
	if err != nil || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("wrong-sub farm push status=%v err=%v", resp, err)
	}

	out, err := gate.HandleBinary(context.Background(), "cmd-result-fence", 1, encodeCommandFrame(t, "snap-after-fence", 3))
	if err != nil {
		t.Fatalf("CommandResult blocked: %v", err)
	}
	decoded, err := protocol.Decode(out, 64<<10)
	if err != nil || decoded.GetCommandResult() == nil {
		t.Fatalf("want COMMAND_RESULT, got %+v err=%v", decoded, err)
	}
}
