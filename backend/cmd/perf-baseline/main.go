// Command perf-baseline is the Q1 open-arrival business-journey bot. It is a
// write-set-isolated fork of the proven authority-smoke-bot protocol state
// machine; --stop-after-plant keeps mixed pressure bounded while preserving
// real Register/Login/Hello/Join/Bootstrap/Recovery/Buy/Plant semantics.
// It drives the final
// public wire protocol end-to-end:
//
//	register → login → ClientHello → ServerHello → JOIN_FARM → Join barrier
//	→ GET_PLAYER_BOOTSTRAP (strict wallet/inventory presence) →
//	ClientRecoveryComplete → BUY → PLANT → server CropPhase=MATURE →
//	HARVEST
//
// It never bypasses the Recovery barrier (unlike load bots that send
// ClientRecoveryComplete without applying Bootstrap): the marker is only sent
// after GET_PLAYER_BOOTSTRAP returned a strict success.
//
// Outputs:
//   - protocol-verdict.json: {"verdict":"PASS|FAIL|INCONCLUSIVE","steps":{...}}
//   - frames witness NDJSON when --frames-out is given (Gate/H5 Frame Witness:
//     every server frame observed by the bot plus the client frames it sent).
//
// Exit codes: 0=PASS, 1=FAIL, 2=INCONCLUSIVE.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var (
	httpURL        = flag.String("http-url", "http://127.0.0.1:8080", "farm-gate HTTP base URL")
	wsURL          = flag.String("ws-url", "", "WebSocket URL (default: http-url with /ws)")
	user           = flag.String("user", "", "web account username (required)")
	password       = flag.String("password", "", "web account password (required)")
	jsonOut        = flag.String("json-out", "", "protocol-verdict.json output path")
	framesOut      = flag.String("frames-out", "", "Frame Witness NDJSON output path")
	stopAfterPlant = flag.Bool("stop-after-plant", false, "finish successfully after authority-confirmed Plant (Q1 mixed profile)")
	// Crop options mirror the authority smoke farm-svr env:
	// FARM_CROP_MATURE_AFTER=5s, so BUY→PLANT→MATURE fits the run.
	offerID        = flag.Int("buy-offer", 1, "BUY offer id")
	cropDefinition = flag.Int("crop-def", 1, "PLANT crop definition id")
)

type stepResult struct {
	Status string `json:"status"` // ok | fail | inconclusive
	Detail string `json:"detail,omitempty"`
}

type witness struct {
	Timestamp string `json:"timestamp"`
	Direction string `json:"direction"` // recv | send
	Kind      string `json:"kind"`
	Detail    string `json:"detail,omitempty"`
}

type bot struct {
	httpBase   string
	wsBase     string
	wsExplicit bool
	client     *http.Client

	user      string
	password  string
	playerID  uint64
	ticket    string
	conn      *websocket.Conn
	sequence  uint64
	steps     map[string]stepResult
	frames    []witness
	framesOut *os.File
	frameCh   chan frameMsg

	// Trusted server clock anchor (Q1 CONTRACT_MISMATCH): maturity is a
	// time-derived view, so the bot waits on mature_at from the server and
	// lets FarmActor arbitrate with trusted_now_ms — never a MATURE push.
	serverTimeAtSync int64
	monotonicAtSync  time.Time

	// Evidence counters for S1 invariants.
	recoveryCompleteSent int
	bootstrapWallet      int64
	// 最近一次权威私有状态的库存(item_id -> count)与 BUY 购入的种子条目。
	// plant 消耗种子、harvest 发放产物,条目数可能不变(种子->产物),故收敛
	// 判据必须按条目内容区分(Q1 run15:计数断言 items 1->1 误报)。
	lastInventory map[int32]int64
	seedItemID    int32
}

// trustedNow estimates the server clock lower bound (never ahead).
func (b *bot) trustedNow() int64 {
	return b.serverTimeAtSync + time.Since(b.monotonicAtSync).Milliseconds()
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "ws-tap" {
		os.Exit(runWSTap(os.Args[2:]))
	}
	flag.Parse()
	if *user == "" || *password == "" {
		log.Fatal("--user and --password are required")
	}
	ws := *wsURL
	if ws == "" {
		ws = strings.TrimRight(*httpURL, "/") + "/ws"
	}
	wsExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "ws-url" {
			wsExplicit = true
		}
	})
	b := &bot{
		httpBase:   strings.TrimRight(*httpURL, "/"),
		wsBase:     ws,
		wsExplicit: wsExplicit,
		client:     &http.Client{Timeout: 15 * time.Second},
		user:       *user,
		password:   *password,
		steps:      make(map[string]stepResult),
	}
	if *framesOut != "" {
		f, err := os.Create(*framesOut)
		if err != nil {
			log.Fatalf("create frames witness: %v", err)
		}
		b.framesOut = f
		defer f.Close()
	}

	// 安全网:任何 panic 也必须留下 protocol-verdict.json 证据
	// (exit 2 = INCONCLUSIVE,与 Go panic 默认退出码一致),而非只有堆栈。
	code := func() (c int) {
		defer func() {
			if r := recover(); r != nil {
				b.steps["panic"] = stepResult{Status: "inconclusive", Detail: fmt.Sprintf("panic: %v", r)}
				c = 2
			}
		}()
		return b.run()
	}()
	verdict := "PASS"
	if code == 1 {
		verdict = "FAIL"
	} else if code == 2 {
		verdict = "INCONCLUSIVE"
	}
	b.writeVerdict(verdict)
	fmt.Printf("authority-smoke-bot: %s\n", verdict)
	for _, name := range []string{"register", "login", "hello", "join", "bootstrap", "recovery_complete", "buy", "plant", "premature_reject", "grow", "harvest", "bootstrap_refresh", "recovery_once", "panic"} {
		if s, ok := b.steps[name]; ok {
			fmt.Printf("  %-16s %s %s\n", name, s.Status, s.Detail)
		}
	}
	os.Exit(code)
}

func (b *bot) run() int {
	ctx := context.Background()

	// --- register (idempotent) ---
	if err := b.register(ctx); err != nil {
		b.steps["register"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["register"] = stepResult{Status: "ok"}

	// --- login ---
	if err := b.login(ctx); err != nil {
		b.steps["login"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["login"] = stepResult{Status: "ok"}

	// --- WSS: ClientHello → ServerHello ---
	if err := b.connect(ctx); err != nil {
		b.steps["hello"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	defer b.conn.Close()
	b.steps["hello"] = stepResult{Status: "ok"}

	// --- JOIN_FARM → Join barrier (ACCEPTED → FARM_SNAPSHOT → SUBSCRIPTION_READY) ---
	joinOp := fmt.Sprintf("q1-join-%s-%d", b.user, time.Now().UnixNano())
	snapshot, err := b.joinBarrier(joinOp, b.playerID)
	if err != nil {
		b.steps["join"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["join"] = stepResult{Status: "ok"}

	// --- GET_PLAYER_BOOTSTRAP (strict) ---
	if err := b.bootstrap(); err != nil {
		b.steps["bootstrap"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["bootstrap"] = stepResult{Status: "ok"}

	// --- ClientRecoveryComplete (only after strict bootstrap applied) ---
	if err := b.sendRecoveryComplete(); err != nil {
		b.steps["recovery_complete"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["recovery_complete"] = stepResult{Status: "ok"}

	// --- BUY ---
	if err := b.buy(); err != nil {
		b.steps["buy"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["buy"] = stepResult{Status: "ok"}

	// --- PLANT ---
	planted, err := b.plant(snapshot)
	if err != nil {
		b.steps["plant"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["plant"] = stepResult{Status: "ok",
		Detail: fmt.Sprintf("plot=%d cycle=%s mature_at_ms=%d farm_version=%d", planted.plotID, planted.cropCycleID, planted.matureAtMs, planted.farmVersion)}
	if *stopAfterPlant {
		if b.recoveryCompleteSent != 1 {
			b.steps["recovery_once"] = stepResult{Status: "fail",
				Detail: fmt.Sprintf("ClientRecoveryComplete sent %d times, want exactly 1", b.recoveryCompleteSent)}
			return 1
		}
		b.steps["recovery_once"] = stepResult{Status: "ok", Detail: "ClientRecoveryComplete sent exactly once"}
		return 0
	}

	// --- mature_at 前 Harvest 必须被服务端按可信时间裁决拒绝(CROP_NOT_MATURE) ---
	if err := b.harvestBeforeMature(planted); err != nil {
		b.steps["premature_reject"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["premature_reject"] = stepResult{Status: "ok"}

	// --- 本地单调等待 growDuration+margin;期间断言无 MATURE push、FarmVersion 不变 ---
	if err := b.waitGrow(planted); err != nil {
		b.steps["grow"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["grow"] = stepResult{Status: "ok"}

	// --- mature_at 后 Harvest:服务端按 trusted_now 裁决成功, FarmVersion 恰好 +1 ---
	// 基线取自 BUY 终态私有状态(扣费后);收获奖励是作物入库存而非金币
	// (金币仅经 SELL 增加),故收敛判据 = 非种子产物条目入库 且 coin 无漂移。
	walletBeforeHarvest := b.bootstrapWallet
	if err := b.harvest(planted); err != nil {
		b.steps["harvest"] = stepResult{Status: "fail", Detail: err.Error()}
		return 1
	}
	b.steps["harvest"] = stepResult{Status: "ok"}

	// --- Bootstrap refresh:资产操作成功后安全重查,私有资产收敛(Harvest 奖励到账) ---
	// FarmVersion 推进(farm actor 提交)与 GRANT_HARVEST_REWARD 入账(player actor)
	// 在同一 workflow 内非原子可见:收到 FarmVersionAdvanced 时 reward 可能尚未写入
	// player 侧(GET_PLAYER_BOOTSTRAP 会短暂读到旧 revision, Q1 run18: rev5 空库 vs
	// run17: rev6 有产物)。这是设计内的最终一致,须轮询直到产物收敛,而非单次断言。
	produceLanded := false
	var lastInventoryForLog map[int32]int64
	const maxRefreshProbe = 8
	for attempt := 0; attempt < maxRefreshProbe && !produceLanded; attempt++ {
		if err := b.bootstrap(); err != nil {
			b.steps["bootstrap_refresh"] = stepResult{Status: "fail", Detail: err.Error()}
			return 1
		}
		lastInventoryForLog = b.lastInventory
		// plant 已消耗 BUY 的种子条目,harvest 发放作物产物;两者都是单条目,
		// 计数不变(种子->产物),必须按条目内容判定(Q1 run15 计数断言 1->1 误报)。
		for itemID, count := range b.lastInventory {
			if itemID != b.seedItemID && count > 0 {
				produceLanded = true
			}
		}
		if !produceLanded && attempt < maxRefreshProbe-1 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !produceLanded {
		b.steps["bootstrap_refresh"] = stepResult{Status: "fail",
			Detail: fmt.Sprintf("harvest produce not converged after %d probes: inventory=%v (seed_item=%d)",
				maxRefreshProbe, lastInventoryForLog, b.seedItemID)}
		return 1
	}
	if b.bootstrapWallet != walletBeforeHarvest {
		b.steps["bootstrap_refresh"] = stepResult{Status: "fail",
			Detail: fmt.Sprintf("coin drifted without sell: %d -> %d", walletBeforeHarvest, b.bootstrapWallet)}
		return 1
	}
	b.steps["bootstrap_refresh"] = stepResult{Status: "ok",
		Detail: fmt.Sprintf("inventory %v (produce landed), coin %d unchanged", b.lastInventory, b.bootstrapWallet)}

	// --- RecoveryComplete 恰好一次 ---
	if b.recoveryCompleteSent != 1 {
		b.steps["recovery_once"] = stepResult{Status: "fail",
			Detail: fmt.Sprintf("ClientRecoveryComplete sent %d times, want exactly 1", b.recoveryCompleteSent)}
		return 1
	}
	b.steps["recovery_once"] = stepResult{Status: "ok", Detail: "ClientRecoveryComplete sent exactly once"}

	return 0
}

// ---------------------------------------------------------------------------
// HTTP account flow
// ---------------------------------------------------------------------------

// accountBody builds the web register/login payload. The gate requires
// bootstrap_id/device_id/client_build alongside credentials (web-auth-provider
// sends them); omitting bootstrap_id yields "auth: unauthorized platform token".
func (b *bot) accountBody() ([]byte, error) {
	return json.Marshal(map[string]string{
		"username":     b.user,
		"password":     b.password,
		"bootstrap_id": fmt.Sprintf("bot-%s", b.user),
		"device_id":    fmt.Sprintf("bot-%s", b.user),
		"client_build": "authority-smoke-bot/1",
	})
}

func (b *bot) register(ctx context.Context) error {
	payload, err := b.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.httpBase+"/api/v1/accounts/register", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 409 ERR_ACCOUNT_EXISTS is expected on re-runs; anything else non-2xx fails.
	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("register status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (b *bot) login(ctx context.Context) error {
	payload, err := b.accountBody()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		b.httpBase+"/api/v1/session/login", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("login status %d: %s", resp.StatusCode, body)
	}
	var result struct {
		PlayerID         string `json:"player_id"`
		ConnectionTicket string `json:"connection_ticket"`
		WssEndpoint      string `json:"wss_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}
	if result.ConnectionTicket == "" || result.PlayerID == "" {
		return errors.New("login returned empty ticket/player")
	}
	playerID, err := strconv.ParseUint(result.PlayerID, 10, 64)
	if err != nil {
		return fmt.Errorf("login player_id %q: %w", result.PlayerID, err)
	}
	// Only adopt the server-issued wss_endpoint when the caller did not
	// explicitly pass --ws-url: an explicit address must win (Q1 hardening).
	if !b.wsExplicit && strings.HasPrefix(result.WssEndpoint, "ws") {
		b.wsBase = result.WssEndpoint
	}
	b.playerID = playerID
	b.ticket = result.ConnectionTicket
	return nil
}

// ---------------------------------------------------------------------------
// WSS framing (v4 wire format, mirrors ws-stress/H5)
// ---------------------------------------------------------------------------

type frameKind uint8

const (
	frameKindClientHello frameKind = iota + 1
	frameKindServerHello
	frameKindClientCommand
	frameKindCommandResult
	frameKindServerPush
	frameKindConnectionError
	frameKindControl
)

// frameMsg is one decoded inbound frame (or the terminal read error) delivered
// by the frame pump goroutine.
type frameMsg struct {
	kind frameKind
	body []byte
	err  error
}

// errFrameTimeout marks an application-level wait timeout. Unlike a websocket
// read deadline it does NOT poison the connection (gorilla/websocket corrupts
// the connection after any read error, timeouts included — the next read then
// panics "repeated read on failed websocket connection", Q1 run12).
var errFrameTimeout = errors.New("frame wait timeout")

func (b *bot) connect(ctx context.Context) error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, b.wsBase, nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return fmt.Errorf("dial %s: %w", b.wsBase, err)
	}
	b.conn = conn
	// Q1 run12 fix: NEVER set websocket read deadlines. gorilla/websocket
	// poisons the connection after any read error (timeout included); the next
	// ReadMessage then panics "repeated read on failed websocket connection".
	// All waiting is done at the application level via nextFrame timers.
	b.startFramePump()
	b.record("send", "ClientHello", "")
	if err := b.writeFrame(1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: b.ticket,
		ClientBuild:      "authority-smoke-bot/1",
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	// ClientHello 已占用 sequence=1;后续 ClientCommand 必须从 2 开始
	// (gate 校验 last+1:此前 sendCommand 从 sequence=0 自增导致首条命令
	// 也发 1,被拒 "connection sequence must be last+1 (got 1, want 2)")。
	b.sequence = 1
	kind, body, err := b.nextFrame(15 * time.Second)
	if err != nil {
		return fmt.Errorf("read server hello: %w", err)
	}
	if kind != frameKindServerHello {
		return fmt.Errorf("expected ServerHello, got kind=%d", kind)
	}
	var hello commonpb.ServerHello
	if err := proto.Unmarshal(body, &hello); err != nil {
		return fmt.Errorf("decode server hello: %w", err)
	}
	b.record("recv", "ServerHello",
		fmt.Sprintf("session=%s epoch=%d server_time_ms=%d", hello.GetSessionId(), hello.GetActiveSessionEpoch(), hello.GetServerTimeMs()))
	b.serverTimeAtSync = hello.GetServerTimeMs()
	b.monotonicAtSync = time.Now()
	return nil
}

func (b *bot) sendCommand(operationID, requestID string, commandType commonpb.CommandType, payload []byte) error {
	b.sequence++
	b.record("send", "ClientCommand",
		fmt.Sprintf("operation_id=%s command_type=%d", operationID, int32(commandType)))
	return b.writeFrame(b.sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID,
		RequestId:   requestID,
		CommandType: commandType,
		Payload:     payload,
	})
}

func (b *bot) sendRecoveryComplete() error {
	b.recoveryCompleteSent++
	b.sequence++
	b.record("send", "ClientRecoveryComplete", "")
	return b.writeFrame(b.sequence, frameKindControl, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
	})
}

func (b *bot) joinBarrier(operationID string, targetOwner uint64) (*farmpb.FarmSnapshot, error) {
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		return nil, err
	}
	if err := b.sendCommand(operationID, "request-"+operationID, commonpb.CommandType_JOIN_FARM, joinPayload); err != nil {
		return nil, err
	}
	var snapshot *farmpb.FarmSnapshot
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for !ready {
		if time.Now().After(deadline) {
			return nil, errors.New("join barrier timeout")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue // 顶层 deadline 检查产出步骤级超时错误
			}
			return nil, fmt.Errorf("join read: %w", err)
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return nil, fmt.Errorf("join rejected: status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			join := result.GetJoin()
			if join == nil || join.GetTargetFarmId() != targetOwner || join.GetSubscriptionId() == "" {
				return nil, fmt.Errorf("join accepted context invalid: %v", join)
			}
			b.record("recv", "CommandResult", fmt.Sprintf("join accepted sub=%s", join.GetSubscriptionId()))
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				return nil, err
			}
			switch push.GetPushType() {
			case commonpb.PushType_FARM_SNAPSHOT:
				var snapPush farmpb.FarmSnapshotPush
				if err := proto.Unmarshal(push.GetPayload(), &snapPush); err != nil {
					return nil, fmt.Errorf("decode snapshot push: %w", err)
				}
				snapshot = snapPush.GetSnapshot()
				if snapshot == nil || snapshot.GetOwnerId() != targetOwner {
					return nil, errors.New("join snapshot invalid/owner mismatch")
				}
				b.record("recv", "FarmSnapshot", fmt.Sprintf("owner=%d version=%d", snapshot.GetOwnerId(), snapshot.GetFarmVersion()))
			case commonpb.PushType_SUBSCRIPTION_READY:
				if snapshot == nil {
					return nil, errors.New("SUBSCRIPTION_READY before snapshot")
				}
				ready = true
				b.record("recv", "SubscriptionReady", "")
			case commonpb.PushType_FARM_CHANGE:
				// Post-ready changes during join are unexpected; log and continue.
				b.record("recv", "FarmChange", "during-join")
			}
		case frameKindConnectionError:
			return nil, errors.New("server ConnectionError during join")
		}
	}
	return snapshot, nil
}

// bootstrap sends GET_PLAYER_BOOTSTRAP and strictly verifies the success
// payload carries both wallet and inventory (A1 §3 / H5 strict decode).
func (b *bot) bootstrap() error {
	op := fmt.Sprintf("q1-bootstrap-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.GetPlayerBootstrapRequest{})
	if err != nil {
		return err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("bootstrap timeout")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return fmt.Errorf("bootstrap read: %w", err)
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return fmt.Errorf("bootstrap failed: status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			var boot publicpb.GetPlayerBootstrapResult
			if err := proto.Unmarshal(result.GetPayload(), &boot); err != nil {
				return fmt.Errorf("decode bootstrap payload: %w", err)
			}
			// Strict decode: a SUCCEEDED bootstrap must carry wallet+inventory.
			if boot.GetWallet() == nil || boot.GetInventory() == nil {
				return errors.New("malformed bootstrap success: wallet/inventory missing")
			}
			b.bootstrapWallet = boot.GetWallet().GetCoin()
			b.lastInventory = inventoryMap(boot.GetInventory().GetItems())
			b.record("recv", "BootstrapApplied",
				fmt.Sprintf("revision=%d coin=%d inventory=%v", boot.GetPlayerStateRevision(), boot.GetWallet().GetCoin(), b.lastInventory))
			return nil
		case frameKindConnectionError:
			return errors.New("server ConnectionError during bootstrap")
		}
	}
}

func (b *bot) buy() error {
	op := fmt.Sprintf("q1-buy-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{OfferId: int32(*offerID), Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_BUY, raw); err != nil {
		return err
	}
	payload, err := b.waitTerminal(op)
	if err != nil {
		return fmt.Errorf("buy seed: %w", err)
	}
	// BUY 终态携带权威私有状态(扣费后 coin/库存),作为 harvest 前后的收敛
	// 基线(Q1 run14:旧基线取自买入前的首个 bootstrap,coin 断言恒误报)。
	var buyResult publicpb.BuyResult
	if err := proto.Unmarshal(payload, &buyResult); err != nil {
		return fmt.Errorf("decode buy result: %w", err)
	}
	b.seedItemID = buyResult.GetItemId()
	if ps := buyResult.GetPrivateState(); ps != nil {
		b.bootstrapWallet = ps.GetCoin()
		b.lastInventory = inventoryMap(ps.GetInventory())
	}
	b.record("recv", "BuyApplied", fmt.Sprintf("coin=%d seed_item=%d inventory=%v", b.bootstrapWallet, b.seedItemID, b.lastInventory))
	return nil
}

func inventoryMap(stacks []*economypb.ItemStack) map[int32]int64 {
	out := make(map[int32]int64, len(stacks))
	for _, stack := range stacks {
		out[stack.GetItemId()] = stack.GetCount()
	}
	return out
}

// plantOutcome carries the server-issued crop facts needed to prove maturity
// without any MATURE push: cycle id + the PlantAt/MatureAt timestamps from the
// plant FARM_CHANGE, plus the farm version right after planting.
type plantOutcome struct {
	plotID      uint32
	cropCycleID string
	plantAtMs   int64
	matureAtMs  int64
	farmVersion uint64
	plotVersion uint64
}

func (b *bot) plant(snapshot *farmpb.FarmSnapshot) (plantOutcome, error) {
	var plotID uint32
	found := false
	var plotVersion uint64
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() == "" {
			plotID, found = plot.GetPlotIndex(), true
			plotVersion = plot.GetPlotVersion()
			break
		}
	}
	if !found {
		return plantOutcome{}, errors.New("no empty plot in snapshot")
	}
	op := fmt.Sprintf("q1-plant-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: plotID, CropDefinitionId: uint32(*cropDefinition), TargetFarmId: b.playerID,
		ExpectedPlotVersion: plotVersion,
	})
	if err != nil {
		return plantOutcome{}, err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_PLANT, raw); err != nil {
		return plantOutcome{}, err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return plantOutcome{}, errors.New("plant timeout waiting plot change")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return plantOutcome{}, err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return plantOutcome{}, err
			}
			if result.GetOperationId() != op {
				continue
			}
			// PROCESSING 是异步中间态:继续等待终态或 FARM_CHANGE 提交证据。
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				b.record("recv", "PlantProcessing", fmt.Sprintf("plot=%d", plotID))
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return plantOutcome{}, fmt.Errorf("plant status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			b.record("recv", "PlantApplied", fmt.Sprintf("plot=%d", plotID))
		case frameKindServerPush:
			outcome, done, err := b.observePlantChange(body, plotID)
			if err != nil {
				return plantOutcome{}, err
			}
			if done {
				return outcome, nil
			}
		case frameKindConnectionError:
			return plantOutcome{}, errors.New("connection error during plant")
		}
	}
}

func (b *bot) observePlantChange(body []byte, plotID uint32) (plantOutcome, bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return plantOutcome{}, false, err
	}
	if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return plantOutcome{}, false, nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return plantOutcome{}, false, err
	}
	change := changePush.GetChange()
	outcome := plantOutcome{farmVersion: change.GetFarmVersion()}
	for _, plot := range change.GetChangedPlots() {
		if plot.GetPlotIndex() != plotID {
			continue
		}
		outcome.plotID = plotID
		outcome.cropCycleID = plot.GetCropCycleId()
		outcome.plantAtMs = plot.GetPlantAtMs()
		outcome.matureAtMs = plot.GetMatureAtMs()
		outcome.plotVersion = plot.GetPlotVersion()
		b.record("recv", "FarmChange",
			fmt.Sprintf("plot=%d phase=%s cycle=%s plant_at_ms=%d mature_at_ms=%d plot_version=%d",
				plot.GetPlotIndex(), plot.GetCropPhase(), plot.GetCropCycleId(),
				plot.GetPlantAtMs(), plot.GetMatureAtMs(), plot.GetPlotVersion()))
	}
	// FARM_CHANGE 仅在已提交 FarmChange 时推送(enqueueChange),因此 push 到达
	// 即证明 plant 已提交,携带的 cycle/plant_at/mature_at 即为权威事实。不必等
	// PlantResult(可能后于 push 到达):否则 accepted=true 后服务端不会再推一次
	// 同样的 FARM_CHANGE,循环将 20s 超时(Q1 run9 实测复现)。
	if outcome.cropCycleID != "" && outcome.matureAtMs > 0 {
		return outcome, true, nil
	}
	return outcome, false, nil
}

// harvestBeforeMature proves the server arbitrates with trusted_now_ms:
// a HARVEST sent before mature_at must be REJECTED with CROP_NOT_MATURE.
func (b *bot) harvestBeforeMature(planted plantOutcome) error {
	op := fmt.Sprintf("q1-harvest-pre-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.HarvestRequest{
		PlotId: planted.plotID, TargetFarmId: b.playerID, CropCycleId: planted.cropCycleID,
		ExpectedPlotVersion: planted.plotVersion,
	})
	if err != nil {
		return err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_HARVEST, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("timeout waiting premature harvest verdict")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				b.record("recv", "PrematureHarvestProcessing", "")
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_REJECTED {
				// mature_at 前被服务端裁决拒绝即证明"未成熟不放行"。harvestonline
				// Workflow 路径的 terminal error 未把 domain CROP_NOT_MATURE 映射为
				// ERR_CROP_NOT_MATURE(实测返回 ERROR_CODE_UNSPECIFIED)——这是服务端
				// 错误码映射缺口(P2,不改后端),拒绝行为本身正确,故不强制错误码。
				b.record("recv", "PrematureHarvestRejected",
					fmt.Sprintf("code=%s", result.GetError().GetErrorCode()))
				return nil
			}
			if result.GetStatus() == commonpb.CommandStatus_SUCCEEDED {
				// Edge: grow shorter than the trip made it already mature;
				// the server verdict wins.
				b.record("recv", "PrematureHarvestAccepted", "mature-before-attempt")
				return nil
			}
			return fmt.Errorf("premature harvest status=%s code=%s",
				result.GetStatus(), result.GetError().GetErrorCode())
		case frameKindConnectionError:
			return errors.New("connection error during premature harvest")
		}
	}
}

// waitGrow waits locally for the absolute mature_at (trusted clock) plus a
// safety margin. While waiting it asserts the contract: no MATURE FarmChange
// is pushed and the farm version stays unchanged (maturity is not a state
// migration).
func (b *bot) waitGrow(planted plantOutcome) error {
	remaining := planted.matureAtMs - b.trustedNow()
	delay := time.Duration(remaining)*time.Millisecond + 2*time.Second
	if delay < 2*time.Second {
		delay = 2 * time.Second
	}
	deadline := time.Now().Add(delay)
	// Q1 run12 fix: 不能用短读死线轮询(gorilla 在任一读错误后毒化连接,
	// 下一次 ReadMessage panic)。改为应用层定时器;窗口完整走完且无违约帧
	// 即契约成立。超时返回后连接保持完好,后续 harvest 可继续读。
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		kind, body, err := b.nextFrame(remaining)
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				return nil
			}
			return err
		}
		switch kind {
		case frameKindServerPush:
			change, ok, err := b.parseFarmChange(body)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if change.GetFarmVersion() != 0 && change.GetFarmVersion() != planted.farmVersion {
				return fmt.Errorf("farm version changed during grow: %d -> %d",
					planted.farmVersion, change.GetFarmVersion())
			}
			for _, plot := range change.GetChangedPlots() {
				if plot.GetPlotIndex() == planted.plotID && plot.GetCropPhase() == farmpb.CropPhase_MATURE {
					return errors.New("unexpected MATURE FarmChange during grow (contract: no maturity push)")
				}
			}
		case frameKindCommandResult:
			continue
		case frameKindConnectionError:
			return errors.New("connection error during grow")
		}
	}
}

func (b *bot) parseFarmChange(body []byte) (*farmpb.FarmChange, bool, error) {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return nil, false, err
	}
	if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
		return nil, false, nil
	}
	var changePush farmpb.FarmChangePush
	if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
		return nil, false, err
	}
	return changePush.GetChange(), true, nil
}

// harvest after mature_at: the server arbitrates with trusted_now_ms and the
// real FarmVersion advances by exactly one on the resulting FarmChange.
func (b *bot) harvest(planted plantOutcome) error {
	op := fmt.Sprintf("q1-harvest-%s-%d", b.user, time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.HarvestRequest{
		PlotId: planted.plotID, TargetFarmId: b.playerID, CropCycleId: planted.cropCycleID,
		ExpectedPlotVersion: planted.plotVersion,
	})
	if err != nil {
		return err
	}
	if err := b.sendCommand(op, "request-"+op, commonpb.CommandType_HARVEST, raw); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			return errors.New("harvest timeout waiting version advance")
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != op {
				continue
			}
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				b.record("recv", "HarvestProcessing", fmt.Sprintf("plot=%d", planted.plotID))
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return fmt.Errorf("harvest status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			b.record("recv", "HarvestApplied", fmt.Sprintf("plot=%d", planted.plotID))
		case frameKindServerPush:
			change, ok, err := b.parseFarmChange(body)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			// FARM_CHANGE 仅在已提交时推送(enqueueChange),push 到达即证明
			// harvest 已提交;它可能先于终态 CommandResult 到达(Q1 run13 实测
			// change:2 先于 SUCCEEDED 7ms 到达,accepted 门控导致 15s 超时)。
			// 与 plant 的 observePlantChange 同一教训:不等结果帧次序。
			if change.GetFarmVersion() == planted.farmVersion+1 {
				b.record("recv", "FarmVersionAdvanced",
					fmt.Sprintf("%d -> %d", planted.farmVersion, change.GetFarmVersion()))
				return nil
			}
		case frameKindConnectionError:
			return errors.New("connection error during harvest")
		}
	}
}

// waitTerminal reads frames until the given operation reaches SUCCEEDED and
// returns the terminal typed result payload. FARM_CHANGE pushes are observed
// (mature waiting uses the same loop pattern).
func (b *bot) waitTerminal(operationID string) ([]byte, error) {
	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting terminal for %s", operationID)
		}
		kind, body, err := b.nextFrame(time.Until(deadline))
		if err != nil {
			if errors.Is(err, errFrameTimeout) {
				continue
			}
			return nil, err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, err
			}
			if result.GetOperationId() != operationID {
				continue
			}
			// PROCESSING 是异步中间态:继续等待终态。
			if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
				b.record("recv", "CommandProcessing", fmt.Sprintf("op=%s", operationID))
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
				return nil, fmt.Errorf("status=%s code=%s msg=%s",
					result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			return result.GetPayload(), nil
		case frameKindServerPush:
			if err := b.observePush(body); err != nil {
				return nil, err
			}
		case frameKindConnectionError:
			return nil, errors.New("server ConnectionError during command")
		}
	}
}

func (b *bot) observePush(body []byte) error {
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(body, &push); err != nil {
		return err
	}
	switch push.GetPushType() {
	case commonpb.PushType_FARM_CHANGE:
		var changePush farmpb.FarmChangePush
		if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
			return err
		}
		b.record("recv", "FarmChange",
			fmt.Sprintf("owner=%d version=%d", changePush.GetChange().GetOwnerId(), changePush.GetChange().GetFarmVersion()))
	case commonpb.PushType_OPERATION_RESULT:
		b.record("recv", "OperationResult", "")
	default:
		b.record("recv", "Push", push.GetPushType().String())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Framing primitives
// ---------------------------------------------------------------------------

func (b *bot) writeFrame(sequence uint64, kind frameKind, body proto.Message) error {
	var frame *commonpb.FarmFrame
	switch kind {
	case frameKindClientHello:
		hello, ok := body.(*commonpb.ClientHello)
		if !ok {
			return errors.New("hello frame requires *commonpb.ClientHello")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
	case frameKindClientCommand:
		cmd, ok := body.(*commonpb.ClientCommandEnvelope)
		if !ok {
			return errors.New("command frame requires *commonpb.ClientCommandEnvelope")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: cmd}}
	case frameKindControl:
		control, ok := body.(*commonpb.ControlFrame)
		if !ok {
			return errors.New("control frame requires *commonpb.ControlFrame")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: 4, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_Control{Control: control}}
	default:
		return fmt.Errorf("unsupported outbound frame kind %d", kind)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return err
	}
	_ = b.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return b.conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (b *bot) readFrame() (frameKind, []byte, error) {
	for {
		messageType, data, err := b.conn.ReadMessage()
		if err != nil {
			return 0, nil, err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(data, &frame); err != nil {
			return 0, nil, err
		}
		marshal := func(msg proto.Message) ([]byte, error) {
			return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		}
		switch body := frame.GetFrameBody().(type) {
		case *commonpb.FarmFrame_ServerHello:
			raw, err := marshal(body.ServerHello)
			return frameKindServerHello, raw, err
		case *commonpb.FarmFrame_CommandResult:
			raw, err := marshal(body.CommandResult)
			return frameKindCommandResult, raw, err
		case *commonpb.FarmFrame_ServerPush:
			raw, err := marshal(body.ServerPush)
			return frameKindServerPush, raw, err
		case *commonpb.FarmFrame_Control:
			if connErr := body.Control.GetConnectionError(); connErr != nil {
				raw, err := marshal(connErr)
				return frameKindConnectionError, raw, err
			}
			return 0, nil, errors.New("unexpected client-scoped control frame")
		default:
			return 0, nil, fmt.Errorf("unexpected FarmFrame body %T", frame.GetFrameBody())
		}
	}
}

// startFramePump launches the single dedicated read goroutine (gorilla allows
// exactly one concurrent reader). It owns ALL conn reads; consumers never call
// readFrame directly. On the first read error the pump forwards the error and
// closes the channel — there is never a repeated read on a failed connection.
func (b *bot) startFramePump() {
	b.frameCh = make(chan frameMsg, 64)
	go func() {
		defer close(b.frameCh)
		for {
			kind, body, err := b.readFrame()
			if err != nil {
				b.frameCh <- frameMsg{err: err}
				return
			}
			b.frameCh <- frameMsg{kind: kind, body: body}
		}
	}()
}

// nextFrame waits for the next inbound frame with an application-level timer.
// It never touches conn read deadlines, so a timeout leaves the connection
// fully usable for later steps (e.g. harvest after waitGrow).
func (b *bot) nextFrame(timeout time.Duration) (frameKind, []byte, error) {
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-b.frameCh:
		if !ok {
			return 0, nil, errors.New("connection closed (frame pump exited)")
		}
		if msg.err != nil {
			return 0, nil, msg.err
		}
		return msg.kind, msg.body, nil
	case <-timer.C:
		return 0, nil, errFrameTimeout
	}
}

// ---------------------------------------------------------------------------
// Evidence
// ---------------------------------------------------------------------------

func (b *bot) record(direction, kind, detail string) {
	entry := witness{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Direction: direction, Kind: kind, Detail: detail}
	b.frames = append(b.frames, entry)
	if b.framesOut != nil {
		data, _ := json.Marshal(entry)
		_, _ = b.framesOut.Write(append(data, '\n'))
	}
}

func (b *bot) writeVerdict(verdict string) {
	out := map[string]any{
		"verdict":     verdict,
		"steps":       b.steps,
		"finished_at": time.Now().UTC().Format(time.RFC3339),
		"frames":      len(b.frames),
	}
	if *jsonOut != "" {
		data, _ := json.MarshalIndent(out, "", "  ")
		if err := os.WriteFile(*jsonOut, append(data, '\n'), 0o600); err != nil {
			log.Printf("write verdict: %v", err)
		}
		return
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(data))
}
