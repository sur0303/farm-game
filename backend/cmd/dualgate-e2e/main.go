// Command dualgate-e2e drives the real-process dual-gate acceptance on the
// protocol_version 3 wire (W11A/W2A/W3A) with the W5B Subscription Lease:
// two farm-gate processes share one SessionAuthority (company persistent
// Redis), one player activates on gate A, then re-activates through gate B.
//
// Assertions, in order:
//  1. A activation: ClientHello on gate A returns ServerHello for the player.
//  2. A usable: JoinOwnFarm on A completes the v3 barrier (ACCEPTED join
//     context -> FARM_SNAPSHOT typed push -> SUBSCRIPTION_READY).
//  3. B takeover: a second login + ClientHello through gate B promotes a
//     strictly higher active_session_epoch.
//  4. Cross-gate KICK: the old connection on A receives the exact KICK push
//     (ERR_SESSION_EXPIRED) issued through GateControlService.KickSession.
//  5. Old commands rejected: a command sent on the stale A connection is
//     never admitted; the gate closes that connection.
//  6. B usable: JoinOwnFarm on B completes the v3 barrier.
//  7. Old disconnect does not delete new state: after A's connection is
//     closed and disconnect callbacks settle, B still reads snapshots
//     (SessionRoute intact), still writes (Buy/Plant succeed) and still
//     receives presence-routed FARM_CHANGE pushes (FarmPresence intact).
//  8. W5B lease Fresh Join (-rejoin-scenario): the orchestrating script
//     restarts farm-svr after the REJOIN_SCENARIO_ARMED marker; the Hub
//     rebuild invalidates the Holder-side lease, gate B delivers
//     REJOIN_REQUIRED on the live connection, and the client completes a
//     Fresh Join (new operation/attempt) plus a business write on the same
//     connection - without a full session re-login.
//
// The program exits non-zero on the first failed assertion.
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
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	ticketstore "farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// clientProtocolVersion 与 Gate 当前公网 wire 版本一致(决策 2:显式版本升级,
// 旧版本被 Gate 拒绝并计 farm_gate_command_protocol_version_rejected_total)。
const clientProtocolVersion = 4

var (
	gateAHTTP      = flag.String("gate-a-http", "http://127.0.0.1:18500", "gate A HTTP base URL")
	gateAWS        = flag.String("gate-a-ws", "ws://127.0.0.1:18500/ws", "gate A WebSocket URL")
	gateBHTTP      = flag.String("gate-b-http", "http://127.0.0.1:18501", "gate B HTTP base URL")
	gateBWS        = flag.String("gate-b-ws", "ws://127.0.0.1:18501/ws", "gate B WebSocket URL")
	platformTok    = flag.String("platform-token", "dualgate-platform-token", "staging platform token shared by both gates")
	bootstrapID    = flag.String("bootstrap-id", "", "stable bootstrap id; default derived from pid")
	stepTimeout    = flag.Duration("step-timeout", 8*time.Second, "per-step read deadline")
	settleDelay    = flag.Duration("settle-delay", 1500*time.Millisecond, "wait for old-gate disconnect callbacks")
	overallLimit   = flag.Duration("overall-timeout", 120*time.Second, "whole run deadline")
	rejoinScenario = flag.Bool("rejoin-scenario", true, "run the W5B lease Fresh Join step (requires the script to restart farm-svr on the armed marker)")
	rejoinTimeout  = flag.Duration("rejoin-timeout", 45*time.Second, "how long to wait for REJOIN_REQUIRED after the armed marker")
	cleanupPrefix  = flag.String("redis-cleanup-prefix", "", "safe per-run Redis prefix to clean after the E2E")
	cleanupSession = flag.Bool("cleanup-session", false, "also delete this test player's canonical session/ticket keys")
	expectedPlayer = flag.Uint64("expected-player-id", 0, "expected login player_id and cleanup fence")
)

type loginResult struct {
	PlayerID         string `json:"player_id"`
	ConnectionTicket string `json:"connection_ticket"`
	ActiveEpoch      string `json:"active_session_epoch"`
}

type runEvidence struct {
	playerID    uint64
	tickets     []string
	workflowIDs []string
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if *bootstrapID == "" {
		*bootstrapID = fmt.Sprintf("dualgate-e2e-%d", os.Getpid())
	}
	ctx, cancel := context.WithTimeout(context.Background(), *overallLimit)
	defer cancel()
	evidence := runEvidence{playerID: *expectedPlayer}
	runErr := run(ctx, &evidence)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	cleanupErr := cleanupRedis(cleanupCtx, evidence)
	cleanupCancel()
	if cleanupErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("cleanup Redis test data: %w", cleanupErr))
	}
	if runErr != nil {
		err := runErr
		log.Fatalf("dualgate-e2e FAIL: %v", err)
	}
	log.Printf("dualgate-e2e PASS: A activate -> v3 join -> B takeover -> KICK -> stale reject -> old-disconnect safe -> lease Fresh Join")
}

func run(ctx context.Context, evidence *runEvidence) error {
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)

	// Step 1: A 激活。HTTPS login on gate A, then ClientHello over A's WSS.
	loginA, err := login(ctx, *gateAHTTP)
	if err != nil {
		return fmt.Errorf("step1 login gate A: %w", err)
	}
	playerID, err := strconv.ParseUint(loginA.PlayerID, 10, 64)
	if err != nil || playerID == 0 {
		return fmt.Errorf("step1 login gate A: invalid player_id %q", loginA.PlayerID)
	}
	if evidence != nil {
		if evidence.playerID != 0 && evidence.playerID != playerID {
			return fmt.Errorf("step1 login gate A: player_id=%d, want %d", playerID, evidence.playerID)
		}
		evidence.playerID = playerID
		evidence.tickets = append(evidence.tickets, loginA.ConnectionTicket)
	}
	connA, err := dial(ctx, *gateAWS)
	if err != nil {
		return fmt.Errorf("step1 dial gate A: %w", err)
	}
	defer connA.Close()
	if err := writeFrame(connA, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: loginA.ConnectionTicket, ClientBuild: "dualgate-e2e/2",
	}); err != nil {
		return fmt.Errorf("step1 hello write gate A: %w", err)
	}
	helloA, err := readServerHello(connA, *stepTimeout)
	if err != nil {
		return fmt.Errorf("step1 hello read gate A: %w", err)
	}
	if helloA.GetPlayerId() != playerID || helloA.GetActiveSessionEpoch() == 0 {
		return fmt.Errorf("step1 unexpected ServerHello on A: player=%d epoch=%d", helloA.GetPlayerId(), helloA.GetActiveSessionEpoch())
	}
	log.Printf("step1 PASS [A 激活]: player=%d active_session_epoch=%d", playerID, helloA.GetActiveSessionEpoch())

	// Step 2: A 可用。JoinOwnFarm completes the v3 barrier on A.
	joinAOperation := "dualgate-" + runID + "-join-a"
	if _, _, err := joinOwnFarmV3Retry(connA, 2, joinAOperation, "request-"+joinAOperation, playerID, *stepTimeout); err != nil {
		return fmt.Errorf("step2 v3 join barrier gate A: %w", err)
	}
	log.Printf("step2 PASS [A 可用]: v3 Join barrier completed via gate A")

	// Step 3: B 顶号。Fresh login on gate B, ClientHello promotes a higher epoch.
	loginB, err := login(ctx, *gateBHTTP)
	if err != nil {
		return fmt.Errorf("step3 login gate B: %w", err)
	}
	if loginB.ConnectionTicket == "" || loginB.ConnectionTicket == loginA.ConnectionTicket {
		return fmt.Errorf("step3 gate B did not issue a fresh ticket")
	}
	if evidence != nil {
		evidence.tickets = append(evidence.tickets, loginB.ConnectionTicket)
	}
	connB, err := dial(ctx, *gateBWS)
	if err != nil {
		return fmt.Errorf("step3 dial gate B: %w", err)
	}
	defer connB.Close()
	if err := writeFrame(connB, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: loginB.ConnectionTicket, ClientBuild: "dualgate-e2e/2",
	}); err != nil {
		return fmt.Errorf("step3 hello write gate B: %w", err)
	}
	helloB, err := readServerHello(connB, *stepTimeout)
	if err != nil {
		return fmt.Errorf("step3 hello read gate B: %w", err)
	}
	if helloB.GetPlayerId() != playerID {
		return fmt.Errorf("step3 gate B ServerHello player mismatch: got %d want %d", helloB.GetPlayerId(), playerID)
	}
	if helloB.GetActiveSessionEpoch() <= helloA.GetActiveSessionEpoch() {
		return fmt.Errorf("step3 epoch did not advance: A=%d B=%d", helloA.GetActiveSessionEpoch(), helloB.GetActiveSessionEpoch())
	}
	log.Printf("step3 PASS [B 顶号]: active_session_epoch %d -> %d", helloA.GetActiveSessionEpoch(), helloB.GetActiveSessionEpoch())

	// Step 4: 跨 Gate KICK。B's activation issued KickSession to A; the exact
	// old connection must receive the KICK push (UX boundary, not security).
	kick, err := readKickPush(connA, 6*time.Second)
	if err != nil {
		return fmt.Errorf("step4 cross-gate KICK not observed on old connection: %w", err)
	}
	if kick.GetErrorCode() != commonpb.ErrorCode_ERR_SESSION_EXPIRED {
		return fmt.Errorf("step4 KICK payload code=%s, want ERR_SESSION_EXPIRED", kick.GetErrorCode())
	}
	log.Printf("step4 PASS [跨 Gate KICK]: old connection received KICK (%s)", kick.GetErrorCode())

	// Step 5: 旧命令拒绝。The stale lane on A must not admit anything new; the
	// gate closes the connection on the next inbound frame.
	staleWriteErr := writeFrame(connA, 3, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		RequestId: "dualgate-" + runID + "-stale-read", CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
	})
	rejectEvidence, err := expectStaleClosed(connA, 5*time.Second)
	if err != nil {
		return fmt.Errorf("step5 stale connection still usable: %w", err)
	}
	if staleWriteErr != nil {
		rejectEvidence = "write failed: " + staleWriteErr.Error()
	}
	log.Printf("step5 PASS [旧命令拒绝]: %s", rejectEvidence)

	// Step 6: B 新 Route 可用。
	joinBOperation := "dualgate-" + runID + "-join-b"
	joinB, seqB, err := joinOwnFarmV3Retry(connB, 2, joinBOperation, "request-"+joinBOperation, playerID, *stepTimeout)
	if err != nil {
		return fmt.Errorf("step6 v3 join barrier gate B: %w", err)
	}
	log.Printf("step6 PASS [B 可用]: v3 Join barrier snapshot v%d ready_through=%d via gate B",
		joinB.snapshot.GetFarmVersion(), joinB.readyThrough)

	// Step 7: 旧 Disconnect 不误删。Close A (no-op if the gate already closed
	// it), wait for A's disconnect callbacks (CompareAndClear / Presence Leave
	// with the old fence) to settle, then prove B's route and presence remain.
	_ = connA.Close()
	time.Sleep(*settleDelay)

	// 原经营链的这一次 BUY 同时承担两项证明(不新增资产副作用):
	// 1) 旧 Gate A 的 Disconnect 没有误删 Gate B 的当前 SessionRoute;
	// 2) Gate B 上 Buy → Plant → Care 经营链可以继续。
	buyOperation := "dualgate-" + runID + "-buy"
	buyPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return err
	}
	if err := writeFrame(connB, seqB, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: buyOperation, RequestId: "request-" + buyOperation,
		CommandType: commonpb.CommandType_BUY, Payload: buyPayload,
	}); err != nil {
		return fmt.Errorf("step7 buy write gate B: %w", err)
	}
	seqB++
	if err := readCommandSuccess(connB, "request-"+buyOperation, buyOperation, *stepTimeout); err != nil {
		return fmt.Errorf("step7 buy seed on B failed: %w", err)
	}

	plantOperation := "dualgate-" + runID + "-plant"
	if evidence != nil {
		evidence.workflowIDs = append(evidence.workflowIDs, plantOperation)
	}
	plotIndex, found := emptyPlot(joinB.snapshot)
	if !found {
		return errors.New("step7 no empty plot in B snapshot")
	}
	plotVersion := plotVersionOf(joinB.snapshot, plotIndex)
	plantPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		TargetFarmId: playerID, PlotId: plotIndex, CropDefinitionId: 1, ExpectedPlotVersion: plotVersion,
	})
	if err != nil {
		return err
	}
	if err := writeFrame(connB, seqB, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: plantOperation, RequestId: "request-" + plantOperation,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}); err != nil {
		return fmt.Errorf("step7 plant write gate B: %w", err)
	}
	seqB++
	if err := readCommandSuccess(connB, "request-"+plantOperation, plantOperation, *stepTimeout); err != nil {
		return fmt.Errorf("step7 plant on B failed: %w", err)
	}
	log.Printf("step7a PASS [Route 未被旧 Disconnect 删除]: BUY succeeded on B")
	log.Printf("step7b PASS [业务写仍可用]: Buy + Plant succeeded on B")

	// FARM_CHANGE is broadcast to FarmPresence watchers. Receiving it on B
	// after A's disconnect proves the old Leave did not remove B's presence.
	if err := expectFarmChange(connB, 6*time.Second); err != nil {
		return fmt.Errorf("step7 FarmPresence broken after old disconnect (no FARM_CHANGE on B): %w", err)
	}
	log.Printf("step7c PASS [Presence 未被旧 Disconnect 删除]: FARM_CHANGE push received on B")

	if !*rejoinScenario {
		return nil
	}
	return runLeaseFreshJoinScenario(connB, seqB, playerID, runID, joinB.snapshot)
}

// runLeaseFreshJoinScenario 是 W5B 租约证据:脚本在看到 ARMED 标记后重启
// farm-svr,Holder 侧 Hub 重建使续租返回 NOT_FOUND,Gate 在同一连接上投递
// REJOIN_REQUIRED;客户端以新 operation/attempt 完成 Fresh Join 并恢复经营。
func runLeaseFreshJoinScenario(connB *websocket.Conn, seqB uint64, playerID uint64, runID string, snapshot *farmpb.FarmSnapshot) error {
	fmt.Printf("REJOIN_SCENARIO_ARMED player=%d\n", playerID)
	log.Printf("step8 armed: waiting for REJOIN_REQUIRED after farm-svr restart")
	reason, err := expectRejoinRequired(connB, *rejoinTimeout)
	if err != nil {
		return fmt.Errorf("step8 REJOIN_REQUIRED not delivered: %w", err)
	}
	if reason != farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST &&
		reason != farmpb.RejoinReason_REJOIN_REASON_HOLDER_REBUILT {
		return fmt.Errorf("step8 rejoin reason=%s, want SUBSCRIPTION_LEASE_LOST or HOLDER_REBUILT", reason)
	}
	log.Printf("step8a PASS [租约失效下发]: REJOIN_REQUIRED reason=%s on the live connection", reason)

	freshOperation := "dualgate-" + runID + "-fresh-join"
	fresh, nextSeq, err := joinOwnFarmV3Retry(connB, seqB, freshOperation, "request-"+freshOperation, playerID, *stepTimeout)
	if err != nil {
		return fmt.Errorf("step8 fresh join after lease loss: %w", err)
	}
	log.Printf("step8b PASS [Fresh Join]: new attempt %s snapshot v%d on the same connection",
		fresh.attemptID, fresh.snapshot.GetFarmVersion())

	waterOperation := "dualgate-" + runID + "-fresh-water"
	waterPlot, found := occupiedPlot(fresh.snapshot)
	if !found {
		return errors.New("step8 no growing plot in fresh snapshot")
	}
	carePayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.CareRequest{
		TargetFarmId: playerID, PlotId: waterPlot,
		CropCycleId: cropCycleOf(fresh.snapshot, waterPlot), CareType: publicpb.CareType_CARE_TYPE_WATER,
	})
	if err != nil {
		return err
	}
	if err := writeFrame(connB, nextSeq, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: waterOperation, RequestId: "request-" + waterOperation,
		CommandType: commonpb.CommandType_CARE, Payload: carePayload,
	}); err != nil {
		return fmt.Errorf("step8 water write after fresh join: %w", err)
	}
	if err := readCommandSuccess(connB, "request-"+waterOperation, waterOperation, *stepTimeout); err != nil {
		return fmt.Errorf("step8 business write after fresh join failed: %w", err)
	}
	if err := expectFarmChange(connB, 6*time.Second); err != nil {
		return fmt.Errorf("step8 no FARM_CHANGE after fresh join write: %w", err)
	}
	log.Printf("step8c PASS [租约恢复后经营]: WaterCrop + FARM_CHANGE on the rejoined subscription")
	return nil
}

// plotVersionOf 返回快照中指定地块的版本;找不到时返回 0(由服务端裁决)。
func plotVersionOf(snapshot *farmpb.FarmSnapshot, plotIndex uint32) uint64 {
	if snapshot == nil {
		return 0
	}
	for _, plot := range snapshot.GetPlots() {
		if plot.GetPlotIndex() == plotIndex {
			return plot.GetPlotVersion()
		}
	}
	return 0
}

// cropCycleOf 返回快照中指定地块的当前茬次 ID;空地返回空串。
func cropCycleOf(snapshot *farmpb.FarmSnapshot, plotIndex uint32) string {
	if snapshot == nil {
		return ""
	}
	for _, plot := range snapshot.GetPlots() {
		if plot.GetPlotIndex() == plotIndex {
			return plot.GetCropCycleId()
		}
	}
	return ""
}

// joinBarrierResult 是 v3 Join 屏障(W2A/26 §12)完成后的客户端视图。
type joinBarrierResult struct {
	snapshot     *farmpb.FarmSnapshot
	subscription string
	attemptID    string
	readyThrough uint64
}

// joinOwnFarmV3Retry 携带与 H5(W3A)一致的 REJOIN 恢复语义:Join 屏障期间
// 收到 REJOIN_REQUIRED(如 Holder↔Gate 瞬时投递失败导致的 fail-closed 清理)
// 时以新 operation/attempt 重试,最多 3 次;其他错误不重试。
// 返回完成结果与下一个可用 connection seq。
func joinOwnFarmV3Retry(conn *websocket.Conn, sequence uint64, operationID, requestID string,
	playerID uint64, timeout time.Duration) (*joinBarrierResult, uint64, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		op := operationID
		req := requestID
		if attempt > 0 {
			op = fmt.Sprintf("%s-retry-%d", operationID, attempt)
			req = fmt.Sprintf("%s-retry-%d", requestID, attempt)
		}
		result, next, err := joinOwnFarmV3(conn, sequence, op, req, playerID, timeout)
		sequence = next
		if err == nil {
			return result, sequence, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "REJOIN_REQUIRED") {
			return nil, sequence, err
		}
		log.Printf("join fenced by REJOIN (transient delivery), retrying with fresh attempt: %v", err)
	}
	return nil, sequence, lastErr
}

// joinOwnFarmV3 发送 JOIN_FARM 并完成 v3 屏障:ACCEPTED(join context)
// -> FARM_SNAPSHOT(FarmSnapshotPush) -> FARM_DELTA_BATCH*(连续) -> SUBSCRIPTION_READY。
// attempt 不一致、版本空洞或 Ready 先于 Snapshot 都判失败。
// 返回完成结果与下一个可用 connection seq。
func joinOwnFarmV3(conn *websocket.Conn, sequence uint64, operationID, requestID string,
	playerID uint64, timeout time.Duration) (*joinBarrierResult, uint64, error) {
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		&publicpb.JoinFarmRequest{TargetFarmId: playerID})
	if err != nil {
		return nil, sequence + 1, err
	}
	if err := writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: requestID,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinPayload,
	}); err != nil {
		return nil, sequence + 1, err
	}
	sequence++
	deadline := time.Now().Add(timeout)
	result := &joinBarrierResult{}
	expectedFrom := uint64(0)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return nil, sequence, err
		}
		switch f.kind {
		case frameKindCommandResult:
			var commandResult commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(f.body, &commandResult); err != nil {
				return nil, sequence, err
			}
			if commandResult.GetOperationId() != operationID && commandResult.GetRequestId() != requestID {
				continue
			}
			if commandResult.GetStatus() != commonpb.CommandStatus_ACCEPTED {
				return nil, sequence, fmt.Errorf("join status=%s code=%s message=%s", commandResult.GetStatus(),
					commandResult.GetError().GetErrorCode(), commandResult.GetError().GetErrorMessage())
			}
			join := commandResult.GetJoin()
			if join == nil || join.GetTargetFarmId() != playerID ||
				join.GetSubscriptionId() == "" || join.GetJoinAttemptId() == "" {
				return nil, sequence, fmt.Errorf("join accepted context invalid: %v", join)
			}
			result.subscription = join.GetSubscriptionId()
			result.attemptID = join.GetJoinAttemptId()
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(f.body, &push); err != nil {
				return nil, sequence, err
			}
			switch push.GetPushType() {
			case commonpb.PushType_FARM_SNAPSHOT:
				var snapshotPush farmpb.FarmSnapshotPush
				if err := proto.Unmarshal(push.GetPayload(), &snapshotPush); err != nil {
					return nil, sequence, err
				}
				if result.attemptID == "" || snapshotPush.GetJoinAttemptId() != result.attemptID {
					return nil, sequence, fmt.Errorf("snapshot attempt %q, want %q", snapshotPush.GetJoinAttemptId(), result.attemptID)
				}
				if snapshotPush.GetSnapshot().GetOwnerId() != playerID {
					return nil, sequence, fmt.Errorf("snapshot owner=%d, want %d", snapshotPush.GetSnapshot().GetOwnerId(), playerID)
				}
				result.snapshot = snapshotPush.GetSnapshot()
				expectedFrom = snapshotPush.GetSnapshotVersion() + 1
			case commonpb.PushType_FARM_DELTA_BATCH:
				var batch farmpb.FarmDeltaBatchPush
				if err := proto.Unmarshal(push.GetPayload(), &batch); err != nil {
					return nil, sequence, err
				}
				if batch.GetJoinAttemptId() != result.attemptID {
					return nil, sequence, fmt.Errorf("delta batch attempt %q, want %q", batch.GetJoinAttemptId(), result.attemptID)
				}
				if batch.GetFromVersion() != expectedFrom || batch.GetThroughVersion() < batch.GetFromVersion() {
					return nil, sequence, fmt.Errorf("delta batch version gap: from=%d through=%d want from=%d",
						batch.GetFromVersion(), batch.GetThroughVersion(), expectedFrom)
				}
				expectedFrom = batch.GetThroughVersion() + 1
			case commonpb.PushType_SUBSCRIPTION_READY:
				var ready farmpb.SubscriptionReadyPush
				if err := proto.Unmarshal(push.GetPayload(), &ready); err != nil {
					return nil, sequence, err
				}
				if result.snapshot == nil {
					return nil, sequence, errors.New("SUBSCRIPTION_READY arrived before snapshot")
				}
				if ready.GetJoinAttemptId() != result.attemptID {
					return nil, sequence, fmt.Errorf("ready attempt %q, want %q", ready.GetJoinAttemptId(), result.attemptID)
				}
				if ready.GetReadyThroughVersion()+1 != expectedFrom {
					return nil, sequence, fmt.Errorf("ready through=%d but catch-up stopped at %d",
						ready.GetReadyThroughVersion(), expectedFrom-1)
				}
				result.readyThrough = ready.GetReadyThroughVersion()
				// 屏障完成 = 客户端本地恢复完成:发送 CONTROL 标记,
				// 解除 Gate 对 durable 业务命令的 ERR_RECOVERY_INCOMPLETE 拦截。
				if err := writeFrame(conn, sequence, frameKindControl, &commonpb.ControlFrame{
					Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
				}); err != nil {
					return nil, sequence + 1, err
				}
				sequence++
				return result, sequence, nil
			case commonpb.PushType_REJOIN_REQUIRED:
				var rejoin farmpb.RejoinRequiredPush
				_ = proto.Unmarshal(push.GetPayload(), &rejoin)
				return nil, sequence, fmt.Errorf("REJOIN_REQUIRED during join barrier: %s", rejoin.GetReason())
			}
		}
	}
}

// expectRejoinRequired 等待 Gate 在存活连接上投递 REJOIN_REQUIRED(W5B 租约
// 终态/Holder 重建),返回原因;其他推送与心跳帧跳过。
func expectRejoinRequired(conn *websocket.Conn, timeout time.Duration) (farmpb.RejoinReason, error) {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED, err
		}
		if f.kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(f.body, &push); err != nil {
			return farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED, err
		}
		if push.GetPushType() != commonpb.PushType_REJOIN_REQUIRED {
			continue
		}
		var rejoin farmpb.RejoinRequiredPush
		if err := proto.Unmarshal(push.GetPayload(), &rejoin); err != nil {
			return farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED, err
		}
		return rejoin.GetReason(), nil
	}
}

func cleanupRedis(ctx context.Context, evidence runEvidence) error {
	if strings.TrimSpace(*cleanupPrefix) == "" {
		return nil
	}
	keys, err := cleanupKeys(*cleanupPrefix, evidence, *cleanupSession)
	if err != nil {
		return err
	}
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	if rawURL == "" {
		return errors.New("FARM_REDIS_URL is required when redis-cleanup-prefix is set")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return errors.New("invalid FARM_REDIS_URL")
	}
	client := redis.NewClient(options)
	defer client.Close()
	for _, key := range keys {
		if err := client.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("delete test key %q: %w", key, err)
		}
	}
	log.Printf("cleanup PASS: removed %d exact Redis test keys", len(keys))
	return nil
}

func cleanupKeys(prefix string, evidence runEvidence, includeSession bool) ([]string, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	const safePrefix = "farm:test:dualgate:"
	if !strings.HasPrefix(prefix, safePrefix) || len(prefix) <= len(safePrefix) {
		return nil, fmt.Errorf("unsafe Redis cleanup prefix %q", prefix)
	}
	if evidence.playerID == 0 {
		return nil, errors.New("cannot clean Redis test data without player_id")
	}
	player := strconv.FormatUint(evidence.playerID, 10)
	keys := []string{
		prefix + ":player:" + player,
		prefix + ":farm:" + player,
		prefix + ":workflow:{workflow}:due",
	}
	for _, operationID := range evidence.workflowIDs {
		if operationID != "" {
			keys = append(keys, prefix+":workflow:{workflow}:record:"+operationID)
		}
	}
	if includeSession {
		keys = append(keys, rediskey.Session(evidence.playerID), rediskey.FarmOnline(evidence.playerID))
		for _, token := range evidence.tickets {
			playerID, err := ticketstore.PlayerIDFromToken(token)
			if err != nil || playerID != evidence.playerID {
				return nil, errors.New("test ticket player_id mismatch")
			}
			keys = append(keys, rediskey.ConnectionTicketForPlayer(playerID, ticketstore.Hash(token)))
		}
	}
	seen := make(map[string]struct{}, len(keys))
	unique := keys[:0]
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique, nil
}

func login(ctx context.Context, httpBase string) (loginResult, error) {
	payload, _ := json.Marshal(map[string]string{
		"platform_token": *platformTok, "bootstrap_id": *bootstrapID, "client_build": "dualgate-e2e/2",
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(httpBase, "/")+"/api/v1/auth/login", bytes.NewReader(payload))
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
	if result.ConnectionTicket == "" || result.PlayerID == "" {
		return loginResult{}, errors.New("login returned empty ticket/player")
	}
	return result, nil
}

func dial(ctx context.Context, url string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, response, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	return conn, nil
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

func writeFrame(conn *websocket.Conn, sequence uint64, kind frameKind, body proto.Message) error {
	var frame *commonpb.FarmFrame
	switch kind {
	case frameKindClientHello:
		hello, ok := body.(*commonpb.ClientHello)
		if !ok {
			return fmt.Errorf("client hello frame requires *commonpb.ClientHello")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
	case frameKindClientCommand:
		cmd, ok := body.(*commonpb.ClientCommandEnvelope)
		if !ok {
			return fmt.Errorf("client command frame requires *commonpb.ClientCommandEnvelope")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: cmd}}
	case frameKindControl:
		control, ok := body.(*commonpb.ControlFrame)
		if !ok {
			return fmt.Errorf("control frame requires *commonpb.ControlFrame")
		}
		frame = &commonpb.FarmFrame{ProtocolVersion: clientProtocolVersion, Sequence: sequence,
			FrameBody: &commonpb.FarmFrame_Control{Control: control}}
	default:
		return fmt.Errorf("unsupported outbound frame kind %d", kind)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

type frame struct {
	kind frameKind
	body []byte
}

// readFrame returns the next binary FarmFrame before the deadline, skipping
// non-binary messages. oneof body 被重新编码为 bytes,供下游按大类解析。
func readFrame(conn *websocket.Conn, timeout time.Duration) (frame, error) {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return frame{}, err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		var decoded commonpb.FarmFrame
		if err := proto.Unmarshal(data, &decoded); err != nil {
			return frame{}, fmt.Errorf("decode FarmFrame: %w", err)
		}
		marshal := func(msg proto.Message) ([]byte, error) {
			return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		}
		switch body := decoded.GetFrameBody().(type) {
		case *commonpb.FarmFrame_ServerHello:
			raw, err := marshal(body.ServerHello)
			return frame{kind: frameKindServerHello, body: raw}, err
		case *commonpb.FarmFrame_CommandResult:
			raw, err := marshal(body.CommandResult)
			return frame{kind: frameKindCommandResult, body: raw}, err
		case *commonpb.FarmFrame_ServerPush:
			raw, err := marshal(body.ServerPush)
			return frame{kind: frameKindServerPush, body: raw}, err
		case *commonpb.FarmFrame_Control:
			if connErr := body.Control.GetConnectionError(); connErr != nil {
				raw, err := marshal(connErr)
				return frame{kind: frameKindConnectionError, body: raw}, err
			}
			return frame{}, fmt.Errorf("unexpected client-scoped control frame from server")
		default:
			return frame{}, fmt.Errorf("unexpected FarmFrame body %T", decoded.GetFrameBody())
		}
	}
}

func readServerHello(conn *websocket.Conn, timeout time.Duration) (*commonpb.ServerHello, error) {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return nil, err
		}
		if f.kind == frameKindConnectionError {
			var connErr commonpb.ConnectionError
			_ = proto.Unmarshal(f.body, &connErr)
			return nil, fmt.Errorf("ConnectionError %s: %s", connErr.GetErrorCode(), connErr.GetErrorMessage())
		}
		if f.kind != frameKindServerHello {
			continue
		}
		var hello commonpb.ServerHello
		if err := proto.Unmarshal(f.body, &hello); err != nil {
			return nil, err
		}
		return &hello, nil
	}
}

func readKickPush(conn *websocket.Conn, timeout time.Duration) (*commonpb.ConnectionError, error) {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return nil, err
		}
		if f.kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(f.body, &push); err != nil {
			return nil, err
		}
		if push.PushType != commonpb.PushType_KICK {
			continue
		}
		var connErr commonpb.ConnectionError
		if err := proto.Unmarshal(push.Payload, &connErr); err != nil {
			return nil, err
		}
		return &connErr, nil
	}
}

// expectStaleClosed proves the stale connection admits nothing: every read
// must end in a transport close without any COMMAND_RESULT success.
func expectStaleClosed(conn *websocket.Conn, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			if evidence, closed := staleCloseEvidence(err); closed {
				return evidence, nil
			}
			return "", fmt.Errorf("stale connection did not produce a transport close: %w", err)
		}
		if f.kind == frameKindCommandResult {
			var result commonpb.CommandResultEnvelope
			_ = proto.Unmarshal(f.body, &result)
			switch result.Status {
			case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_PROCESSING, commonpb.CommandStatus_NO_CHANGE:
				return "", fmt.Errorf("stale connection admitted command: status=%s", result.Status)
			}
		}
	}
}

// staleCloseEvidence distinguishes an actual transport close from a read
// deadline. A quiet but still-open stale socket must fail the E2E instead of
// being mistaken for successful server-side fencing.
func staleCloseEvidence(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return fmt.Sprintf("server closed stale connection (websocket close %d)", closeErr.Code), true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "", false
		}
		return "server closed stale connection (" + err.Error() + ")", true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return "server closed stale connection (" + err.Error() + ")", true
	}
	return "", false
}

// readCommandSuccess matches on operation_id when the command carried one,
// otherwise on request_id (read-only commands leave operation_id empty).
func readCommandSuccess(conn *websocket.Conn, requestID, operationID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if f.kind != frameKindCommandResult {
			continue
		}
		var result commonpb.CommandResultEnvelope
		if err := proto.Unmarshal(f.body, &result); err != nil {
			return err
		}
		matched := (operationID != "" && result.OperationId == operationID) ||
			(operationID == "" && requestID != "" && result.RequestId == requestID)
		if !matched {
			continue
		}
		switch result.Status {
		case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_PROCESSING, commonpb.CommandStatus_NO_CHANGE:
			return nil
		default:
			return fmt.Errorf("status=%s code=%s message=%s", result.Status, result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
		}
	}
}

func expectFarmChange(conn *websocket.Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		f, err := readFrame(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if f.kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(f.body, &push); err != nil {
			return err
		}
		if push.PushType == commonpb.PushType_FARM_CHANGE {
			return nil
		}
	}
}

func emptyPlot(snapshot *farmpb.FarmSnapshot) (uint32, bool) {
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() == "" {
			return plot.GetPlotIndex(), true
		}
	}
	return 0, false
}

func occupiedPlot(snapshot *farmpb.FarmSnapshot) (uint32, bool) {
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() != "" {
			return plot.GetPlotIndex(), true
		}
	}
	return 0, false
}
