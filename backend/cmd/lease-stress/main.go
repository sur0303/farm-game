// Command lease-stress is the protocol client used by the R1-Q1 real Gate
// lease evidence. Round A holds one owner plus 199 visitors on one real Farm
// SubscriptionHub. Round B holds subscriptions on two farms/two routed real
// Farm holders while the external verification script drives its isolated
// fault proxies and the Gate's real SIGTERM Drain path.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const clientProtocolVersion = 4

var (
	round           = flag.String("round", "a", "evidence round: a or b")
	gateHTTP        = flag.String("gate-http", "http://127.0.0.1:18500", "gate HTTP base URL")
	gateWS          = flag.String("gate-ws", "ws://127.0.0.1:18500/ws", "gate WebSocket URL")
	metricsURL      = flag.String("metrics-url", "http://127.0.0.1:18510/metrics", "gate /metrics URL")
	platformTok     = flag.String("platform-token", "lease-stress-token", "staging platform token (fixed demo player; bots 改用 load 测试身份通道)")
	loadSecret      = flag.String("load-test-secret", "", "HMAC secret for load:<playerID>:<sig> test tokens (gate 需配 LOAD_TEST_TOKEN_SECRET)")
	bootstrapPref   = flag.String("bootstrap-prefix", "", "bootstrap id prefix; default derived from pid")
	outputPath      = flag.String("output", "", "structured client evidence output (required)")
	readyFile       = flag.String("ready-file", "", "created after every Subscription Ready barrier")
	startFile       = flag.String("start-file", "", "when set, wait for this coordinator release file")
	windowDoneFile  = flag.String("window-complete-file", "", "created after the Round A observation window")
	stopFile        = flag.String("stop-file", "", "Round B coordinator stop file")
	playerBase      = flag.Uint64("player-base", 900000, "first generated visitor player id")
	ownerAID        = flag.Uint64("owner-a", 0, "explicit owner A/farm id; default player-base")
	ownerBID        = flag.Uint64("owner-b", 0, "Round B owner B/farm id")
	viewers         = flag.Int("viewers", 199, "viewer bots joining the owner's farm (owner included in farm cap)")
	joinConc        = flag.Int("join-concurrency", 20, "max concurrent join barriers")
	duration        = flag.Duration("duration", 60*time.Second, "steady-state duration after all joins")
	stepTimeout     = flag.Duration("step-timeout", 15*time.Second, "per-join read deadline")
	actionInterval  = flag.Duration("action-interval", 5*time.Second, "owner business-action interval")
	expectedRejoins = flag.Int("expected-rejoins", 0, "exact expected REJOIN_REQUIRED count before Drain")
	requireDrain    = flag.Bool("require-drain", false, "require every connection to observe DrainNotice and close")
)

type loginResult struct {
	PlayerID         string `json:"player_id"`
	ConnectionTicket string `json:"connection_ticket"`
	SessionCookie    string `json:"-"`
}

// bot 是一个轻量协议客户端:完整执行 HTTPS 换 Ticket、WS hello、v4 Join
// 屏障与恢复标记。viewer 用 readLoop 后台统计;owner 在业务读写路径上内联统计,
// 避免 gorilla/websocket 的并发读冲突。
type bot struct {
	playerID      uint64
	conn          *websocket.Conn
	seq           uint64
	sessionCookie string
	rejoins       atomic.Int64
	changes       atomic.Int64
	drainNotices  atomic.Int64
	closed        atomic.Bool
}

type clientEvidence struct {
	SchemaVersion      string          `json:"schema_version"`
	Status             string          `json:"status"`
	Round              string          `json:"round"`
	StartedAt          string          `json:"started_at"`
	FinishedAt         string          `json:"finished_at"`
	Owners             int             `json:"owners"`
	Visitors           int             `json:"visitors"`
	SubscriptionsReady int             `json:"subscriptions_ready"`
	PartitionReady     map[string]int  `json:"partition_ready"`
	ExpectedRejoins    int             `json:"expected_rejoins"`
	Rejoins            int64           `json:"rejoins"`
	UnexpectedRejoins  int64           `json:"unexpected_rejoins"`
	FarmChanges        int64           `json:"farm_changes"`
	DrainNotices       int64           `json:"drain_notices"`
	ConnectionsClosed  int             `json:"connections_closed"`
	Checks             map[string]bool `json:"checks"`
	Failures           []string        `json:"failures"`
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if *bootstrapPref == "" {
		*bootstrapPref = fmt.Sprintf("lease-stress-%d", time.Now().UnixNano()%1_000_000)
	}
	if *viewers < 1 {
		log.Fatal("-viewers must be >= 1")
	}
	if *outputPath == "" || (*round != "a" && *round != "b") {
		log.Fatal("-output and -round=a|b are required")
	}
	if err := run(); err != nil {
		log.Fatalf("lease-stress FAIL: %v", err)
	}
}

func run() error {
	started := time.Now().UTC()
	owners, bots, partitions, err := joinEvidenceBots()
	if err != nil {
		return err
	}
	allBots := append(append([]*bot(nil), owners...), bots...)
	defer func() {
		for _, b := range allBots {
			if b != nil {
				_ = b.conn.Close()
			}
		}
	}()
	for _, b := range allBots {
		go b.readLoop()
	}
	if err := signalFile(*readyFile); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}
	if err := waitForFile(*startFile, *stepTimeout); err != nil {
		return fmt.Errorf("wait coordinator start: %w", err)
	}
	log.Printf("round %s: all %d Subscription Ready barriers complete", *round, len(allBots))

	if *round == "a" {
		time.Sleep(*duration)
		if err := signalFile(*windowDoneFile); err != nil {
			return fmt.Errorf("write window complete file: %w", err)
		}
		if *stopFile != "" {
			if err := waitForFile(*stopFile, *stepTimeout); err != nil {
				return fmt.Errorf("wait Round A evidence capture: %w", err)
			}
		}
	} else if *stopFile != "" {
		if err := waitForFile(*stopFile, *duration); err != nil {
			return fmt.Errorf("wait coordinator stop: %w", err)
		}
	} else {
		time.Sleep(*duration)
	}
	if *requireDrain {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if countClosed(allBots) == len(allBots) {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	evidence := clientEvidence{
		SchemaVersion: "r1-q1-real-lease-client/v1", Status: "PASS", Round: *round,
		StartedAt: started.Format(time.RFC3339Nano), FinishedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Owners: len(owners), Visitors: len(bots), SubscriptionsReady: len(allBots), PartitionReady: partitions,
		ExpectedRejoins: *expectedRejoins, Checks: map[string]bool{}, Failures: []string{},
	}
	for _, b := range allBots {
		evidence.Rejoins += b.rejoins.Load()
		evidence.FarmChanges += b.changes.Load()
		evidence.DrainNotices += b.drainNotices.Load()
		if b.closed.Load() {
			evidence.ConnectionsClosed++
		}
	}
	evidence.UnexpectedRejoins = evidence.Rejoins - int64(*expectedRejoins)
	evidence.Checks["all_subscription_ready"] = evidence.SubscriptionsReady == len(allBots)
	evidence.Checks["expected_rejoin_exact"] = evidence.UnexpectedRejoins == 0
	if !evidence.Checks["expected_rejoin_exact"] {
		evidence.Failures = append(evidence.Failures, fmt.Sprintf("rejoins=%d want=%d", evidence.Rejoins, *expectedRejoins))
	}
	if *requireDrain {
		evidence.Checks["all_drain_notice"] = evidence.DrainNotices == int64(len(allBots))
		evidence.Checks["all_connections_closed"] = evidence.ConnectionsClosed == len(allBots)
		if !evidence.Checks["all_drain_notice"] {
			evidence.Failures = append(evidence.Failures, fmt.Sprintf("drain notices=%d want=%d", evidence.DrainNotices, len(allBots)))
		}
		if !evidence.Checks["all_connections_closed"] {
			evidence.Failures = append(evidence.Failures, fmt.Sprintf("closed=%d want=%d", evidence.ConnectionsClosed, len(allBots)))
		}
	}
	if len(evidence.Failures) > 0 {
		evidence.Status = "FAIL"
	}
	if err := writeClientEvidence(*outputPath, evidence); err != nil {
		return err
	}
	if evidence.Status != "PASS" {
		return fmt.Errorf("round %s client assertions failed: %v", *round, evidence.Failures)
	}
	return nil
}

func joinEvidenceBots() ([]*bot, []*bot, map[string]int, error) {
	ownerA := *ownerAID
	if ownerA == 0 {
		ownerA = *playerBase
	}
	owner, err := joinBotWithID(0, ownerA, 0, "")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("owner A join: %w", err)
	}
	owners := []*bot{owner}
	partitions := map[string]int{"holder_a": 1}
	shareA, err := shareCreate(owner.sessionCookie)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("owner A share: %w", err)
	}
	var ownerB *bot
	var shareB string
	if *round == "b" {
		if *ownerBID == 0 || *ownerBID == ownerA {
			return nil, nil, nil, errors.New("Round B requires distinct -owner-a and -owner-b")
		}
		ownerB, err = joinBotWithID(1, *ownerBID, 0, "")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("owner B join: %w", err)
		}
		owners = append(owners, ownerB)
		partitions["holder_b"] = 1
		shareB, err = shareCreate(ownerB.sessionCookie)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("owner B share: %w", err)
		}
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, *joinConc)
	bots := make([]*bot, *viewers)
	var firstErr error
	var errMu sync.Mutex
	used := map[uint64]bool{ownerA: true}
	if ownerB != nil {
		used[ownerB.playerID] = true
	}
	nextID := *playerBase
	visitorIDs := make([]uint64, *viewers)
	for index := range visitorIDs {
		for used[nextID] || nextID == 0 {
			nextID++
		}
		visitorIDs[index] = nextID
		used[nextID] = true
		nextID++
	}
	for index := 0; index < *viewers; index++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			target, share, partition := ownerA, shareA, "holder_a"
			if ownerB != nil && idx%2 == 1 {
				target, share, partition = ownerB.playerID, shareB, "holder_b"
			}
			b, joinErr := joinBotWithID(idx+len(owners), visitorIDs[idx], target, share)
			if joinErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("viewer %d join: %w", idx, joinErr)
				}
				errMu.Unlock()
				return
			}
			bots[idx] = b
			errMu.Lock()
			partitions[partition]++
			errMu.Unlock()
		}(index)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, nil, nil, firstErr
	}
	return owners, bots, partitions, nil
}

func signalFile(path string) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, []byte("ready\n"), 0o600)
}

func waitForFile(path string, timeout time.Duration) error {
	if path == "" {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", path)
}

func countClosed(bots []*bot) int {
	count := 0
	for _, b := range bots {
		if b.closed.Load() {
			count++
		}
	}
	return count
}

func writeClientEvidence(path string, evidence clientEvidence) error {
	raw, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o600)
}

// joinBot 完成一个 Bot 的 login -> (可选)分享接受 -> hello -> v4 Join 屏障
// -> 恢复标记。targetFarm=0 表示 Join 自己的农场;shareToken 非空时先与业主
// 建立好友关系(围观门禁前置)。
func joinBot(index int, targetFarm uint64, shareToken string) (*bot, error) {
	return joinBotWithID(index, *playerBase+uint64(index), targetFarm, shareToken)
}

func joinBotWithID(index int, playerID uint64, targetFarm uint64, shareToken string) (*bot, error) {
	if *loadSecret == "" {
		return nil, errors.New("-load-test-secret is required (bots need per-bot player ids)")
	}
	if playerID == 0 {
		return nil, errors.New("player id must be non-zero")
	}
	creds, err := login(fmt.Sprintf("%s-bot-%d", *bootstrapPref, index), loadToken(playerID))
	if err != nil {
		return nil, err
	}
	if got, err := strconv.ParseUint(creds.PlayerID, 10, 64); err != nil || got != playerID {
		return nil, fmt.Errorf("login player_id=%q, want %d", creds.PlayerID, playerID)
	}
	if shareToken != "" {
		if err := shareAccept(creds.SessionCookie, shareToken); err != nil {
			return nil, fmt.Errorf("share accept: %w", err)
		}
	}
	if targetFarm == 0 {
		targetFarm = playerID
	}
	conn, _, err := websocket.DefaultDialer.Dial(*gateWS, nil)
	if err != nil {
		return nil, err
	}
	b := &bot{playerID: playerID, conn: conn, seq: 1, sessionCookie: creds.SessionCookie}
	if err := b.write(frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: creds.ConnectionTicket, ClientBuild: "lease-stress/1",
	}); err != nil {
		conn.Close()
		return nil, err
	}
	if err := b.readServerHello(*stepTimeout); err != nil {
		conn.Close()
		return nil, err
	}
	operation := fmt.Sprintf("%s-join-%d", *bootstrapPref, index)
	if targetFarm != playerID {
		// 围观者必须先 Join 自己的农场完成恢复(joinedOwnFarm),否则
		// ClientRecoveryComplete 不被接受,连接会因 recovery incomplete 被关闭;
		// 随后 SwitchFarm 到业主农场(W2B 线性化:本地撤销先于远程 Cleanup)。
		if err := b.joinBarrierWithRetry(operation+"-own", playerID, playerID, true, *stepTimeout); err != nil {
			conn.Close()
			return nil, fmt.Errorf("own-farm recovery join: %w", err)
		}
	}
	if err := b.joinBarrierWithRetry(operation, targetFarm, playerID, targetFarm == playerID, *stepTimeout); err != nil {
		conn.Close()
		return nil, err
	}
	return b, nil
}

// joinBarrierWithRetry 与 H5/dualgate-e2e 一致的 REJOIN 恢复语义:屏障期间
// 收到 REJOIN_REQUIRED(如 Holder↔Gate 瞬时投递失败导致的 fail-closed 清理)
// 时以新 operation/attempt 重试,最多 3 次。
func (b *bot) joinBarrierWithRetry(operationID string, targetFarm, playerID uint64, sendMarker bool, timeout time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		op := operationID
		if attempt > 0 {
			op = fmt.Sprintf("%s-retry-%d", operationID, attempt)
		}
		err := b.joinBarrier(op, "request-"+op, targetFarm, playerID, sendMarker, timeout)
		if err == nil {
			return nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "REJOIN_REQUIRED") {
			return err
		}
	}
	return lastErr
}

// joinBarrier 与 dualgate-e2e 的 v4 屏障一致:ACCEPTED(join ctx)
// -> FARM_SNAPSHOT -> FARM_DELTA_BATCH* -> SUBSCRIPTION_READY -> 恢复标记。
// JOIN 目标只来自 Typed Payload(28 §5.1);旧 envelope 兼容字段已删除。
func (b *bot) joinBarrier(operationID, requestID string, targetFarm, playerID uint64, sendMarker bool, timeout time.Duration) error {
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		&publicpb.JoinFarmRequest{TargetFarmId: targetFarm})
	if err != nil {
		return err
	}
	if err := b.write(frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: requestID,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinPayload,
	}); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	attemptID := ""
	expectedFrom := uint64(0)
	sawSnapshot := false
	for {
		f, err := b.readFrame(time.Until(deadline))
		if err != nil {
			return err
		}
		switch f.kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(f.body, &result); err != nil {
				return err
			}
			if result.GetOperationId() != operationID && result.GetRequestId() != requestID {
				continue
			}
			if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return fmt.Errorf("join status=%s code=%s message=%s", result.GetStatus(),
					result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
			}
			join := result.GetJoin()
			if join == nil || join.GetTargetFarmId() != targetFarm || join.GetJoinAttemptId() == "" {
				return fmt.Errorf("join accepted context invalid: %v", join)
			}
			attemptID = join.GetJoinAttemptId()
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(f.body, &push); err != nil {
				return err
			}
			switch push.GetPushType() {
			case commonpb.PushType_FARM_SNAPSHOT:
				var snapshotPush farmpb.FarmSnapshotPush
				if err := proto.Unmarshal(push.GetPayload(), &snapshotPush); err != nil {
					return err
				}
				if attemptID == "" || snapshotPush.GetJoinAttemptId() != attemptID {
					return fmt.Errorf("snapshot attempt mismatch")
				}
				sawSnapshot = true
				expectedFrom = snapshotPush.GetSnapshotVersion() + 1
			case commonpb.PushType_FARM_DELTA_BATCH:
				var batch farmpb.FarmDeltaBatchPush
				if err := proto.Unmarshal(push.GetPayload(), &batch); err != nil {
					return err
				}
				if batch.GetJoinAttemptId() != attemptID || batch.GetFromVersion() != expectedFrom {
					return fmt.Errorf("delta batch gap: from=%d want=%d", batch.GetFromVersion(), expectedFrom)
				}
				expectedFrom = batch.GetThroughVersion() + 1
			case commonpb.PushType_SUBSCRIPTION_READY:
				var ready farmpb.SubscriptionReadyPush
				if err := proto.Unmarshal(push.GetPayload(), &ready); err != nil {
					return err
				}
				if !sawSnapshot || ready.GetJoinAttemptId() != attemptID {
					return fmt.Errorf("ready without matching snapshot")
				}
				if ready.GetReadyThroughVersion()+1 != expectedFrom {
					return fmt.Errorf("ready through=%d, catch-up stopped at %d", ready.GetReadyThroughVersion(), expectedFrom-1)
				}
				// 恢复标记只在自家农场恢复 Join 后发送:Gate 独立校验
				// currentFarmID==PlayerID,围观 Join 后重复发送会触发
				// ErrRecoveryIncomplete 关连接(协议纪律,与 H5 行为一致)。
				if !sendMarker {
					return nil
				}
				return b.write(frameKindControl, &commonpb.ControlFrame{
					Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
				})
			case commonpb.PushType_REJOIN_REQUIRED:
				return fmt.Errorf("REJOIN_REQUIRED during join barrier")
			}
		}
	}
}

// readLoop(viewer 专用)后台统计 REJOIN/FARM_CHANGE;连接关闭即退出。
func (b *bot) readLoop() {
	defer b.closed.Store(true)
	for {
		f, err := b.readFrame(0)
		if err != nil {
			return
		}
		b.countPush(f)
	}
}

// drain(owner 专用)在给定窗口内持续读并统计推送,用于收尾排空。
func (b *bot) drain(window time.Duration) {
	deadline := time.Now().Add(window)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		f, err := b.readFrame(remaining)
		if err != nil {
			return
		}
		b.countPush(f)
	}
}

// ownerAction 交替执行 buy/plant/water,持续制造真实 FarmChange;
// 读取路径上内联统计 REJOIN/FARM_CHANGE(owner 不跑 readLoop)。
func ownerAction(owner *bot, step int) error {
	var commandType commonpb.CommandType
	var payload proto.Message
	operation := fmt.Sprintf("%s-owner-action-%d", *bootstrapPref, step)
	switch step % 3 {
	case 0:
		commandType = commonpb.CommandType_BUY
		payload = &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}
	case 1:
		commandType = commonpb.CommandType_PLANT
		payload = &publicpb.PlantRequest{PlotId: uint32(step/3) % 12, CropDefinitionId: 1, TargetFarmId: owner.playerID}
	default:
		commandType = commonpb.CommandType_CARE
		payload = &publicpb.CareRequest{PlotId: uint32(step/3) % 12, TargetFarmId: owner.playerID, CareType: publicpb.CareType_CARE_TYPE_WATER}
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return err
	}
	if err := owner.write(frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operation, RequestId: "request-" + operation,
		CommandType: commandType, Payload: encoded,
	}); err != nil {
		return err
	}
	deadline := time.Now().Add(*stepTimeout)
	for {
		f, err := owner.readFrame(time.Until(deadline))
		if err != nil {
			return err
		}
		if f.kind != frameKindCommandResult {
			owner.countPush(f)
			continue
		}
		var result commonpb.CommandResultEnvelope
		if err := proto.Unmarshal(f.body, &result); err != nil {
			return err
		}
		if result.GetOperationId() != operation {
			continue
		}
		switch result.GetStatus() {
		case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_PROCESSING, commonpb.CommandStatus_NO_CHANGE:
			return nil
		default:
			return fmt.Errorf("action status=%s code=%s", result.GetStatus(), result.GetError().GetErrorCode())
		}
	}
}

func (b *bot) countPush(f *wireFrame) {
	if f.kind != frameKindServerPush {
		return
	}
	var push commonpb.ServerPushEnvelope
	if err := proto.Unmarshal(f.body, &push); err != nil {
		return
	}
	switch push.GetPushType() {
	case commonpb.PushType_REJOIN_REQUIRED:
		b.rejoins.Add(1)
	case commonpb.PushType_FARM_CHANGE:
		b.changes.Add(1)
	case commonpb.PushType_DRAIN_NOTICE:
		b.drainNotices.Add(1)
	}
}

// frameKind 是本工具对 FarmFrame 顶层 oneof 大类的本地枚举(28 §2:大类用
// oneof 区分,业务命令不走 oneof)。
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

// write 以 v4 线格式(protocol_version=4、uint64 sequence、frame_body oneof)
// 编码一条客户端帧。
func (b *bot) write(kind frameKind, body proto.Message) error {
	var frame *commonpb.FarmFrame
	switch kind {
	case frameKindClientHello:
		hello, ok := body.(*commonpb.ClientHello)
		if !ok {
			return errors.New("client hello frame requires *commonpb.ClientHello")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: b.seq,
			FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
	case frameKindClientCommand:
		cmd, ok := body.(*commonpb.ClientCommandEnvelope)
		if !ok {
			return errors.New("client command frame requires *commonpb.ClientCommandEnvelope")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: b.seq,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: cmd}}
	case frameKindControl:
		control, ok := body.(*commonpb.ControlFrame)
		if !ok {
			return errors.New("control frame requires *commonpb.ControlFrame")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: b.seq,
			FrameBody: &commonpb.FarmFrame_Control{Control: control}}
	default:
		return fmt.Errorf("unsupported outbound frame kind %d", kind)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return err
	}
	b.seq++
	_ = b.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return b.conn.WriteMessage(websocket.BinaryMessage, raw)
}

// wireFrame 是读侧解析结果:kind 为本地大类,body 为 oneof 成员的再编码 bytes。
type wireFrame struct {
	kind frameKind
	body []byte
}

func (b *bot) readServerHello(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		f, err := b.readFrame(time.Until(deadline))
		if err != nil {
			return err
		}
		switch f.kind {
		case frameKindServerHello:
			return nil
		case frameKindConnectionError:
			var connErr commonpb.ConnectionError
			_ = proto.Unmarshal(f.body, &connErr)
			return fmt.Errorf("ConnectionError %s: %s", connErr.GetErrorCode(), connErr.GetErrorMessage())
		}
	}
}

// readFrame 读取下一个二进制 FarmFrame(timeout=0 表示不设读截止),按顶层
// oneof 解出大类;服务端作用域 Control(ConnectionError)映射为本地
// ConnectionError 大类,客户端作用域 Control 属协议违规。
func (b *bot) readFrame(timeout time.Duration) (*wireFrame, error) {
	if timeout > 0 {
		_ = b.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		_ = b.conn.SetReadDeadline(time.Time{})
	}
	for {
		messageType, data, err := b.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(data, &frame); err != nil {
			return nil, fmt.Errorf("decode FarmFrame: %w", err)
		}
		marshal := func(msg proto.Message) ([]byte, error) {
			return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		}
		switch body := frame.GetFrameBody().(type) {
		case *commonpb.FarmFrame_ServerHello:
			raw, err := marshal(body.ServerHello)
			return &wireFrame{kind: frameKindServerHello, body: raw}, err
		case *commonpb.FarmFrame_CommandResult:
			raw, err := marshal(body.CommandResult)
			return &wireFrame{kind: frameKindCommandResult, body: raw}, err
		case *commonpb.FarmFrame_ServerPush:
			raw, err := marshal(body.ServerPush)
			return &wireFrame{kind: frameKindServerPush, body: raw}, err
		case *commonpb.FarmFrame_Control:
			if connErr := body.Control.GetConnectionError(); connErr != nil {
				raw, err := marshal(connErr)
				return &wireFrame{kind: frameKindConnectionError, body: raw}, err
			}
			return nil, errors.New("unexpected client-scoped control frame from server")
		default:
			return nil, fmt.Errorf("unexpected FarmFrame body %T", frame.GetFrameBody())
		}
	}
}

// loadToken 生成 load:<playerID>:<hex-hmac-sha256(playerID, secret)> 测试身份
// 票据,与 gate demoVerifier 的 loadTestSecret 分支对应。
func loadToken(playerID uint64) string {
	mac := hmac.New(sha256.New, []byte(*loadSecret))
	_, _ = mac.Write([]byte(strconv.FormatUint(playerID, 10)))
	return fmt.Sprintf("load:%d:%s", playerID, hex.EncodeToString(mac.Sum(nil)))
}

func login(bootstrapID, platformToken string) (loginResult, error) {
	payload, _ := json.Marshal(map[string]string{
		"platform_token": platformToken, "bootstrap_id": bootstrapID, "client_build": "lease-stress/1",
	})
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(*gateHTTP, "/")+"/api/v1/auth/login", bytes.NewReader(payload))
	if err != nil {
		return loginResult{}, err
	}
	request.Header.Set("content-type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return loginResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		return loginResult{}, fmt.Errorf("login status %d: %s", response.StatusCode, body)
	}
	var result loginResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return loginResult{}, err
	}
	for _, cookie := range response.Cookies() {
		if cookie.Name == "farm_session" {
			result.SessionCookie = cookie.Value
		}
	}
	if result.ConnectionTicket == "" || result.PlayerID == "" {
		return loginResult{}, errors.New("login returned empty ticket/player")
	}
	return result, nil
}

// shareCreate 业主创建多人可接受的分享(好友关系是围观 Join 的前置,
// ERR_NOT_FRIEND 证明该门禁真实生效)。
func shareCreate(sessionCookie string) (string, error) {
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(*gateHTTP, "/")+"/api/v1/share/create", nil)
	if err != nil {
		return "", err
	}
	request.AddCookie(&http.Cookie{Name: "farm_session", Value: sessionCookie})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		return "", fmt.Errorf("share create status %d: %s", response.StatusCode, body)
	}
	var result struct {
		ShareToken string `json:"share_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.ShareToken == "" {
		return "", errors.New("share create returned empty token")
	}
	return result.ShareToken, nil
}

func shareAccept(sessionCookie, shareToken string) error {
	payload, _ := json.Marshal(map[string]string{"share_token": shareToken})
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(*gateHTTP, "/")+"/api/v1/share/accept", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("content-type", "application/json")
	request.AddCookie(&http.Cookie{Name: "farm_session", Value: sessionCookie})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		return fmt.Errorf("share accept status %d: %s", response.StatusCode, body)
	}
	return nil
}

// gateMetrics 是 /metrics 文本格式的最小解析结果。
type gateMetrics struct {
	batches, entries, renewed float64
	localExpired, failClosed  float64
	histogramBuckets          map[float64]float64
	histogramCount            float64
}

type gateMetricsReport struct {
	batches, entries, renewed float64
	localExpired, failClosed  float64
	p99Bucket                 string
}

func scrapeGateMetrics(url string) (gateMetrics, error) {
	result := gateMetrics{histogramBuckets: map[float64]float64{}}
	response, err := http.Get(url) //nolint:gosec // 本地测试端点
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("metrics status %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return result, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.HasPrefix(line, "#") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		name := fields[0]
		switch {
		case name == "farm_gate_subscription_lease_renew_batches_total":
			result.batches = value
		case name == "farm_gate_subscription_lease_renew_entries_total":
			result.entries = value
		case strings.HasPrefix(name, "farm_gate_subscription_lease_results_total"):
			if strings.Contains(name, `status="RENEWED"`) {
				result.renewed = value
			}
		case strings.HasPrefix(name, "farm_gate_subscription_lease_local_expired_total"):
			result.localExpired += value
		case strings.HasPrefix(name, "farm_gate_subscription_lease_fail_closed_total"):
			result.failClosed += value
		case strings.HasPrefix(name, "farm_gate_subscription_lease_renew_duration_seconds_bucket"):
			result.histogramBuckets[parseLeLabel(name)] = value
		case name == "farm_gate_subscription_lease_renew_duration_seconds_count":
			result.histogramCount = value
		}
	}
	return result, nil
}

func parseLeLabel(metric string) float64 {
	start := strings.Index(metric, `le="`)
	if start < 0 {
		return 0
	}
	rest := metric[start+4:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return 0
	}
	if rest[:end] == "+Inf" {
		return 1 << 30
	}
	value, _ := strconv.ParseFloat(rest[:end], 64)
	return value
}

func diffGateMetrics(before, after gateMetrics) gateMetricsReport {
	report := gateMetricsReport{
		batches:      after.batches - before.batches,
		entries:      after.entries - before.entries,
		renewed:      after.renewed - before.renewed,
		localExpired: after.localExpired - before.localExpired,
		failClosed:   after.failClosed - before.failClosed,
	}
	// Prometheus 直方图桶是累积计数;差分后,最小满足 >=99% 增量的桶上界即 p99 估计。
	type bucket struct {
		upper float64
		count float64
	}
	var deltas []bucket
	for upper, afterCount := range after.histogramBuckets {
		deltas = append(deltas, bucket{upper, afterCount - before.histogramBuckets[upper]})
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i].upper < deltas[j].upper })
	total := after.histogramCount - before.histogramCount
	if total > 0 {
		threshold := total * 0.99
		for _, d := range deltas {
			if d.count >= threshold {
				report.p99Bucket = bucketLabel(d.upper)
				break
			}
		}
	}
	if report.p99Bucket == "" {
		report.p99Bucket = "n/a(no samples)"
	}
	return report
}

func bucketLabel(upper float64) string {
	if upper >= 1<<30 {
		return "+Inf"
	}
	return fmt.Sprintf("%.3fs", upper)
}
