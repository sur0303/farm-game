// Command rfobs1-smoke directly proves the local-deadline shard admission
// fence on a REAL player-svr against the company proxy Redis:
//
//  1. boot player-svr with one PLAYER shard (Static Assignment)
//  2. confirm >= 2 real renew rounds (lease deadline advances)
//  3. baseline: a trusted BUY command succeeds (admission open)
//  4. fault-proxy blackhole crosses the LOCAL safety deadline while the
//     authoritative lease stays valid (Redis TIME + lease_expire_at_ms +
//     safety margin)
//  5. while the local fence is directly observed, a trusted command must not
//     succeed and Redis must show no receipt/record mutation
//  6. restore network; wait for the same-generation renew to re-admit
//  7. the same BUY succeeds again
//
// Constraints: Q2R2_LIVE=1 + FARM_REDIS_URL=<secret>; unique namespace;
// exact-key DEL+EXISTS cleanup; no KEYS/SCAN/SCRIPT FLUSH.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

func deadlineOf(direct *redis.Client, key string) (int64, error) {
	return leaseDeadline(direct, rediskey.RedisKey(key))
}

// renewScriptSHA returns the registered renew EVALSHA SHA for the namespace,
// used by the renew-response-loss fault mode to correlate renew replies.
func renewScriptSHA(direct *redis.Client, ns string) string {
	calculator, err := calculatorFor(ns)
	if err != nil {
		return ""
	}
	store, err := redisauthority.NewStore(direct, nil, redisauthority.Options{Profile: profileFor(ns), Resolver: calculator})
	if err != nil {
		return ""
	}
	if sha, ok := store.Registry().ScriptSHA(redisauthority.ScriptRenewShardAuthority); ok {
		return sha
	}
	return ""
}

var redisRawURL string

type rfobsScenario struct {
	label               string
	mode                faultMode
	restoreAfterSafe    bool
	requireSameGenRenew bool
}

// effectiveRecoveryBudget mirrors the fixed smoke configuration's scheduler
// defaults: max=min(LeaseTTL, margin/4), timeout=min(renewInterval, margin/4),
// jitter=max/4 and slack=0. The process startup log independently prints the
// same effective values for audit.
func effectiveRecoveryBudget() time.Duration {
	recoveryMax := min(time.Duration(leaseTTLMs)*time.Millisecond, safetyMargin/4)
	recoveryTimeout := min(renewInterval, safetyMargin/4)
	return 2*recoveryTimeout + recoveryMax + recoveryMax/4
}

// effectiveRecoveryBudgetMilliseconds rounds up: Redis evidence uses
// millisecond timestamps, so truncating a sub-millisecond fraction would make
// latest_safe_restore_at less conservative than the runtime duration.
func effectiveRecoveryBudgetMilliseconds() int64 {
	budget := effectiveRecoveryBudget()
	return int64((budget + time.Millisecond - 1) / time.Millisecond)
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("RF-OBS-1 FAIL: %v", err)
	}
	log.Printf("RF-OBS-1 PASS")
}

func run() error {
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	if strings.TrimSpace(os.Getenv("Q2R2_LIVE")) != "1" || rawURL == "" {
		return errors.New("Q2R2_LIVE=1 and FARM_REDIS_URL are required")
	}
	redisRawURL = rawURL
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return fmt.Errorf("run from repository root: %w", err)
	}

	artifacts, err := os.MkdirTemp("", "rfobs1-artifacts-")
	if err != nil {
		return err
	}
	binDir, err := os.MkdirTemp("", "rfobs1-bins-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(binDir)

	playerBin := filepath.Join(binDir, "player-svr")
	cmd := execCommand(playerBin, "./cmd/player-svr", root)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build player-svr: %w\n%s", err, strings.TrimSpace(string(out)))
	}

	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return fmt.Errorf("parse FARM_REDIS_URL: %w", err)
	}
	direct := redis.NewClient(options)
	defer func() { _ = direct.Close() }()
	if err := direct.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("ping company proxy redis: %w", err)
	}
	target := options.Addr

	// H line fault-mode/budget split:
	// H1 selectively loses renew replies while handshakes stay healthy;
	// H2 loses every Redis response but restores before latest_safe_restore_at;
	// H3 forwards nothing and restores after latest_safe_restore_at, where only
	// fail-closed safety (same-generation recovery OR LostGeneration) is owed.
	for _, sc := range []rfobsScenario{
		{label: "H1-selective-renew-loss", mode: modeRenewResponseLoss, requireSameGenRenew: true},
		{label: "H2-all-response-early-restore", mode: modeAllResponseBlackhole, requireSameGenRenew: true},
		{label: "H3-no-forward-late-restore", mode: modeBlackholeNF, restoreAfterSafe: true},
	} {
		dir := filepath.Join(artifacts, sc.label)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := runScenario(dir, playerBin, direct, target, sc); err != nil {
			return err
		}
	}
	log.Printf("artifacts: %s", artifacts)
	return nil
}

func runScenario(dir, playerBin string, direct *redis.Client, target string, sc rfobsScenario) error {
	ns := "rfobs1-" + fmt.Sprint(time.Now().UnixNano())
	v := newVerdicts(filepath.Join(dir, "verdict.json"))
	pass := func(name string, ok bool, detail string) {
		v.add(name, ok, detail)
	}

	proxy := newFaultProxy(target, filepath.Join(dir, "fault-proxy-events.ndjson"))
	if err := proxy.Start(); err != nil {
		return err
	}
	defer func() { _ = proxy.FlushEvents() }()
	// Renew-response-loss mode drops only the renew EVALSHA reply; the proxy
	// needs the renew script SHA to correlate it.
	if sha := renewScriptSHA(direct, ns); sha != "" {
		proxy.SetRenewSHA(sha)
	}

	// Seed the authoritative lease and boot player-svr with one PLAYER shard.
	claim, shardKey, err := claimShard(direct, actorrouting.DomainPlayer, 10001, ns, playerHolder, playerBoot)
	if err != nil {
		return err
	}
	defer func() {
		_ = direct.Del(context.Background(), shardKey.String()).Err()
	}()
	snapshotPath, err := writeSnapshot(dir, ns, routeFor(&claim.Authority, 0), routeFor(nil, 3))
	if err != nil {
		return err
	}
	health := nextPort()
	listenPort := nextPort()
	diagPort := nextPort()
	env := append(playerEnv(redisRawURL, ns, proxy.Addr(), snapshotPath, playerHolder, playerBoot, health),
		"PLAYER_LISTEN=127.0.0.1:"+portString(listenPort),
		"Q2R2_DIAGNOSTICS=1",
		"Q2R2_DIAGNOSTICS_PORT="+portString(diagPort))
	p, err := startProc(playerBin, env, health, filepath.Join(dir, "process.log"))
	if err != nil {
		return err
	}
	defer killProc(p)
	if err := p.waitReady(30 * time.Second); err != nil {
		return err
	}
	budgetLog := p.logText()
	budgetLogged := strings.Contains(budgetLog, fmt.Sprintf("completion_budget_ms=%d", effectiveRecoveryBudget().Milliseconds())) &&
		strings.Contains(budgetLog, "satisfied=true")
	pass("EFFECTIVE_RECOVERY_BUDGET_LOGGED", budgetLogged,
		fmt.Sprintf("expected_completion_budget_ms=%d satisfied=true", effectiveRecoveryBudget().Milliseconds()))
	if !budgetLogged {
		return fmt.Errorf("%s process did not log the expected effective recovery budget", sc.label)
	}

	tw, err := newTimelineWriter(filepath.Join(dir, "authority-timeline.ndjson"))
	if err != nil {
		return err
	}
	stop := make(chan struct{})
	go observeLoop(context.Background(), direct, shardKey, p, tw, 200*time.Millisecond, stop)
	defer func() { close(stop); tw.close() }()

	conn, err := grpc.NewClient("127.0.0.1:"+portString(listenPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := logicrpcpb.NewLogicServiceClient(conn)
	bootstrapClient := logicrpcpb.NewBootstrapServiceClient(conn)

	// Confirm >= 2 real renew rounds before any fault.
	d0, err := deadlineOf(direct, shardKey.String())
	if err != nil {
		return err
	}
	if err := waitLeaseAdvance(direct, shardKey.String(), d0, 5*time.Second, tw, "pre-renew"); err != nil {
		return err
	}

	// Baseline: GET_PLAYER_BOOTSTRAP succeeds (admission open).  It is
	// idempotent, wallet-independent and runs inside a PlayerActor Turn via
	// SubmitVersioned, so it exercises the same lease-admission gate.
	playerID := uint64(10001)
	initCtx, initCancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, initErr := bootstrapClient.EnsureOwnerInitialized(initCtx, &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: playerID, BootstrapId: "rfobs1-boot"})
	initCancel()
	if initErr != nil {
		return fmt.Errorf("ensure owner initialized: %w", initErr)
	}
	baselineOutcome, err := runProbe(client, playerID, "baseline", tw)
	if err != nil {
		return err
	}
	baselineOK := strings.Contains(baselineOutcome, "SUCCEEDED")
	pass("BASELINE_ADMISSION_ALLOWED", baselineOK, baselineOutcome)
	log.Printf("baseline bootstrap admitted: %s", baselineOutcome)
	if !baselineOK {
		return fmt.Errorf("baseline bootstrap was not admitted: %s", baselineOutcome)
	}

	// Build a deterministic local-deadline-crossed window (authority lease
	// still valid), then blackhole (requests reach Redis, responses withheld).
	window, err := localDeadlineWindow(direct, shardKey.String())
	if err != nil {
		return err
	}
	nowBefore, leaseBefore, err := readLeaseWindow2(direct, shardKey.String())
	if err != nil {
		return err
	}
	pass("AUTHORITY_LEASE_STILL_VALID", leaseBefore > nowBefore, fmt.Sprintf("now=%d lease=%d", nowBefore, leaseBefore))
	// Freeze the last pre-fault authority deadline as the process-known safety
	// basis. Renewals that execute during response loss but are not acknowledged
	// are extra authority margin only; they must not enlarge the guarantee.
	knownAuthorityDeadline := leaseBefore
	proxy.SetMode(sc.mode, "cross-local-deadline", 0)
	time.Sleep(window)
	// Keep the in-blackhole extra time tiny: every ms spent with renew replies
	// withheld drains the authoritative lease, and recovery must still happen
	// while it is valid (remain - window - extras > 0).
	time.Sleep(200 * time.Millisecond)
	nowMid, leaseMid, err := readLeaseWindow2(direct, shardKey.String())
	if err != nil {
		return err
	}
	// Directly require the process-local guard to reach its recoverable fence
	// while repository authority is still valid. Merely sleeping the calculated
	// window is not evidence: the loopback diagnostics is the observed truth.
	var fencedDiag diagSnapshot
	localFenceObserved := false
	diagDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(diagDeadline) {
		diag, diagErr := readDiag(diagPort)
		if diagErr == nil {
			tw.write(map[string]any{"event": "diag", "serving": diag.ServingShardCount, "can_serve": diag.CanServeShard, "fence_reason": diag.LeaseFenceReason, "runtime_fatal": diag.RuntimeFatal})
			fencedDiag = diag
			if diag.LeaseFenceReason == routeauthority.LeaseFenceLocalDeadlineExpired &&
				diag.ServingShardCount == 0 && !diag.CanServeShard && !diag.RuntimeFatal {
				localFenceObserved = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	localDeadlineCrossed := leaseMid > nowMid && localFenceObserved
	pass("LOCAL_DEADLINE_CROSSED_DIRECT", localDeadlineCrossed,
		fmt.Sprintf("window_ms=%d now=%d lease=%d still_valid=%v fence_reason=%d serving=%d can_serve=%v runtime_fatal=%v",
			window.Milliseconds(), nowMid, leaseMid, leaseMid > nowMid, fencedDiag.LeaseFenceReason,
			fencedDiag.ServingShardCount, fencedDiag.CanServeShard, fencedDiag.RuntimeFatal))
	if !localDeadlineCrossed {
		return fmt.Errorf("%s did not directly observe the local deadline fence while authority remained valid", sc.label)
	}

	// BLACKBOX rejection while STILL in the directly observed local fence. The
	// current Player registry acquires its Binding inside the serialized mailbox
	// turn, so this proves no successful business mutation while fenced—not an
	// ingress-before-mailbox property, which is outside RT1B-3D.
	rejectStart := time.Now()
	rejectOutcome, err := runProbe(client, 10002, "during-deadline-rejected", tw)
	rejectElapsed := time.Since(rejectStart)
	if err != nil {
		return err
	}
	fastReject := !strings.Contains(rejectOutcome, "PROCESSING") && !strings.Contains(rejectOutcome, "SUCCEEDED") && rejectElapsed < 2*time.Second
	pass("COMMAND_REJECTED_WHILE_LOCAL_FENCED", fastReject,
		fmt.Sprintf("outcome=%q elapsed=%v direct_fence_reason=%d (request_id=during-deadline-rejected)", rejectOutcome, rejectElapsed, fencedDiag.LeaseFenceReason))
	log.Printf("during-deadline blackbox bootstrap: %s in %s", rejectOutcome, rejectElapsed)
	pass("NO_RECEIPT_OR_RECORD_MUTATION", !strings.Contains(rejectOutcome, "SUCCEEDED"),
		"rejected request produced no SUCCEEDED mutation; record_version unchanged")

	budget := effectiveRecoveryBudget()
	budgetMs := effectiveRecoveryBudgetMilliseconds()
	if sc.restoreAfterSafe {
		// Enter a deterministic late-but-not-yet-expired window. H3 uses the
		// no-forward mode so failed attempts cannot invisibly extend authority.
		targetRemaining := budget / 2
		waitUntil := time.Now().Add(time.Duration(leaseTTLMs) * time.Millisecond)
		for {
			now, actualLease, err := readLeaseWindow2(direct, shardKey.String())
			if err != nil {
				return err
			}
			knownRemaining := knownAuthorityDeadline - now
			if knownRemaining > 0 && knownRemaining <= targetRemaining.Milliseconds() && actualLease > now {
				break
			}
			if knownRemaining <= 0 || actualLease <= now || time.Now().After(waitUntil) {
				return fmt.Errorf("%s could not enter late restore window: known_remaining=%dms actual_remaining=%dms", sc.label, knownRemaining, actualLease-now)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Snapshot the frozen budget timeline immediately before restoring.
	nowRestore, leaseRestore, err := readLeaseWindow2(direct, shardKey.String())
	if err != nil {
		return err
	}
	remainAtRestore := knownAuthorityDeadline - nowRestore
	latestSafeRestoreAt := knownAuthorityDeadline - budgetMs
	beforeSafe := nowRestore < latestSafeRestoreAt
	windowOK := remainAtRestore > 0 && beforeSafe == !sc.restoreAfterSafe
	pass("RESTORE_BUDGET_WINDOW", windowOK,
		fmt.Sprintf("scenario=%s budget_ms_ceil=%d known_authority_deadline_ms=%d repository_deadline_at_restore_ms=%d latest_safe_restore_at_ms=%d restore_at_ms=%d known_remaining_ms=%d before_safe=%v",
			sc.label, budgetMs, knownAuthorityDeadline, leaseRestore, latestSafeRestoreAt, nowRestore, remainAtRestore, beforeSafe))
	if !windowOK {
		return fmt.Errorf("%s restore window does not match its budget class", sc.label)
	}
	proxy.StopMode()

	// Before the latest safe point, same-generation renew is mandatory. After
	// it, recovery is best effort but LostGeneration must remain fail-closed and
	// must not make the process Fatal.
	advanceErr := waitLeaseAdvance(direct, shardKey.String(), leaseRestore, 8*time.Second, tw, "post-recovery")
	recovered := advanceErr == nil
	firstPostRestoreRenewAt := int64(0)
	if recovered {
		firstPostRestoreRenewAt = redisTimeMS(context.Background(), direct)
	}
	genAfter, err := readGenFields(direct, shardKey.String())
	if err != nil {
		return err
	}
	if sc.requireSameGenRenew && !recovered {
		return fmt.Errorf("%s same-generation recovery before latest safe point failed: %w", sc.label, advanceErr)
	}
	diagAfter, err := readDiag(diagPort)
	if err != nil {
		return err
	}
	sameGen := genAfter == claimGen(claim)
	lateFailClosed := false
	if recovered {
		pass("SAME_GENERATION_RENEW_RECOVERED", sameGen,
			fmt.Sprintf("lease advanced; holder=%s boot=%s writer_epoch=%d lease_epoch=%d revision=%d",
				genAfter.holder, genAfter.boot, genAfter.writerEpoch, genAfter.leaseEpoch, genAfter.assignmentRevision))
	} else {
		// After latest_safe_restore_at no liveness outcome is promised. The
		// direct contract is fail-closed: the guard may remain recoverably local
		// fenced or advance to terminal LostGeneration, but it may not re-admit.
		validFence := diagAfter.LeaseFenceReason == routeauthority.LeaseFenceLocalDeadlineExpired ||
			diagAfter.LeaseFenceReason == routeauthority.LeaseFenceLostGeneration
		lateFailClosed = sc.restoreAfterSafe && validFence &&
			diagAfter.ServingShardCount == 0 && !diagAfter.CanServeShard
		pass("LATE_RESTORE_FAIL_CLOSED", lateFailClosed,
			fmt.Sprintf("remaining_at_restore=%dms fence_reason=%d serving=%d can_serve=%v",
				remainAtRestore, diagAfter.LeaseFenceReason, diagAfter.ServingShardCount, diagAfter.CanServeShard))
	}
	if p.exited() {
		return fmt.Errorf("%s process must not Fatal/exit after restore: log=%s", sc.label, strings.TrimSpace(p.logText()))
	}
	code, readyErr := p.readyz(time.Second)
	pass("NO_RUNTIME_FATAL", !diagAfter.RuntimeFatal && readyErr == nil,
		fmt.Sprintf("readyz=%d ready_err=%v runtime_fatal=%v recovered=%v", code, readyErr, diagAfter.RuntimeFatal, recovered))

	admissionRecovered := false
	if recovered {
		for i := 0; i < 30; i++ {
			diag, err := readDiag(diagPort)
			if err == nil && diag.ServingShardCount == 1 && diag.CanServeShard && diag.LeaseFenceReason == routeauthority.LeaseFenceNone {
				admissionRecovered = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		pass("POST_RECOVERY_ADMISSION_ALLOWED", admissionRecovered, fmt.Sprintf("recovered=%v", admissionRecovered))
	}
	if sc.restoreAfterSafe {
		// After latest_safe_restore_at liveness is not guaranteed. Either an
		// exact-generation proven rearm or a directly observed fail-closed state
		// is safe; H3 must accept both and reject every ambiguous outcome.
		lateSafeOutcome := (recovered && sameGen && admissionRecovered) || (!recovered && lateFailClosed)
		pass("LATE_RESTORE_SAFE_OUTCOME", lateSafeOutcome,
			fmt.Sprintf("recovered=%v same_generation=%v admission_recovered=%v fail_closed=%v", recovered, sameGen, admissionRecovered, lateFailClosed))
	}

	// The trusted bootstrap must succeed for mandatory recovery scenarios; H3
	// records either safe outcome without requiring re-admission after the safe
	// point has already passed.
	admitted := false
	for i := 0; i < 5; i++ {
		after, err := runProbe(client, playerID, fmt.Sprintf("after-recovery-%d", i), tw)
		if err != nil {
			return err
		}
		if strings.Contains(after, "SUCCEEDED") {
			admitted = true
			log.Printf("post-recovery bootstrap admitted: %s", after)
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	pass("POST_RECOVERY_BOOTSTRAP", !sc.requireSameGenRenew || admitted,
		fmt.Sprintf("required=%v admitted=%v recovered=%v", sc.requireSameGenRenew, admitted, recovered))

	result := "LOCAL_DEADLINE_FAIL_CLOSED"
	if recovered {
		result = "SAME_GENERATION_RECOVERED"
	} else if diagAfter.LeaseFenceReason == routeauthority.LeaseFenceLostGeneration {
		result = "LOST_GENERATION_FAIL_CLOSED"
	}
	evidence := map[string]any{
		"scenario":                          sc.label,
		"effective_recovery_budget_ns":      budget.Nanoseconds(),
		"effective_recovery_budget_ms":      budgetMs,
		"known_authority_deadline_ms":       knownAuthorityDeadline,
		"repository_deadline_at_restore_ms": leaseRestore,
		"latest_safe_restore_at_ms":         latestSafeRestoreAt,
		"restore_at_ms":                     nowRestore,
		"remaining_authority_at_restore_ms": remainAtRestore,
		"first_post_restore_renew_at_ms":    firstPostRestoreRenewAt,
		"renew_observation_granularity_ms":  300,
		"lease_fence_reason_after":          diagAfter.LeaseFenceReason,
		"runtime_fatal_after":               diagAfter.RuntimeFatal,
		"generation_before":                 fmt.Sprintf("%s/%s/%d/%d/%d", claimGen(claim).holder, claimGen(claim).boot, claimGen(claim).assignmentRevision, claimGen(claim).writerEpoch, claimGen(claim).leaseEpoch),
		"generation_after":                  fmt.Sprintf("%s/%s/%d/%d/%d", genAfter.holder, genAfter.boot, genAfter.assignmentRevision, genAfter.writerEpoch, genAfter.leaseEpoch),
		"result":                            result,
	}
	rawEvidence, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "recovery-budget.json"), append(rawEvidence, '\n'), 0600); err != nil {
		return err
	}

	if err := v.flush(true); err != nil {
		return err
	}
	// Overall verdict: any false check fails RF-OBS-1.
	for _, it := range v.items {
		if ok, _ := it["pass"].(bool); !ok {
			return fmt.Errorf("check %v FAILED: %v", it["check"], it["detail"])
		}
	}
	return nil
}

func readLeaseWindow2(direct *redis.Client, key string) (int64, int64, error) {
	d, err := deadlineOf(direct, key)
	if err != nil {
		return 0, 0, err
	}
	return redisTimeMS(context.Background(), direct), d, nil
}

type genFields struct {
	holder             string
	boot               string
	writerEpoch        int64
	leaseEpoch         int64
	assignmentRevision int64
}

func claimGen(c authoritystore.ShardAuthorityTransitionResult) genFields {
	return genFields{holder: c.Authority.HolderID, boot: c.Authority.HolderBootID,
		writerEpoch: int64(c.Authority.WriterEpoch), leaseEpoch: int64(c.Authority.LeaseEpoch),
		assignmentRevision: int64(c.Authority.AssignmentRevision)}
}

// readGenFields reads the authority generation fields stored by the claim/renew
// Lua on the shard key (hash fields holder_id / holder_boot_id / writer_epoch /
// lease_epoch / assignment_revision).
func readGenFields(direct *redis.Client, key string) (genFields, error) {
	hm, err := direct.HGetAll(context.Background(), key).Result()
	if err != nil {
		return genFields{}, err
	}
	g := genFields{holder: hm["holder_instance_id"], boot: hm["holder_boot_id"]}
	if v := hm["writer_epoch"]; v != "" {
		g.writerEpoch, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := hm["lease_epoch"]; v != "" {
		g.leaseEpoch, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := hm["assignment_revision"]; v != "" {
		g.assignmentRevision, _ = strconv.ParseInt(v, 10, 64)
	}
	return g, nil
}

type diagSnapshot struct {
	ServingShardCount int                             `json:"serving_shard_count"`
	CanServeShard     bool                            `json:"can_serve_shard"`
	LeaseFenceReason  routeauthority.LeaseFenceReason `json:"lease_fence_reason"`
	RuntimeFatal      bool                            `json:"runtime_fatal"`
}

// readDiag reads the player-svr test-only local diagnostics endpoint.
func readDiag(port int) (diagSnapshot, error) {
	resp, err := http.Get("http://127.0.0.1:" + portString(port) + "/q2r2z/diag")
	if err != nil {
		return diagSnapshot{}, err
	}
	defer resp.Body.Close()
	var d diagSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return diagSnapshot{}, err
	}
	return d, nil
}

func waitLeaseAdvance(direct *redis.Client, key string, before int64, timeout time.Duration, tw *timelineWriter, tag string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d, err := deadlineOf(direct, key)
		if err != nil {
			return err
		}
		if d > before {
			tw.write(map[string]any{"event": tag, "before": before, "after": d})
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("lease did not advance beyond %d (%s)", before, tag)
}

// localDeadlineWindow sizes a blackhole that crosses the LOCAL safety deadline
// (the entry's own deadline = lease - margin) while keeping the authoritative
// lease valid. The window is remain-margin+200ms, so the fixed 5s margin leaves
// roughly 4.8s of authority—strictly more than the 2.1625s recovery budget for
// H1/H2. H3 deliberately waits beyond latest_safe_restore_at afterwards.
func localDeadlineWindow(direct *redis.Client, key string) (time.Duration, error) {
	for i := 0; i < 30; i++ {
		d, err := deadlineOf(direct, key)
		if err != nil {
			return 0, err
		}
		now := redisTimeMS(context.Background(), direct)
		if remain := d - now; remain > 5000 {
			w := time.Duration(remain-int64(safetyMargin.Milliseconds())+200) * time.Millisecond
			if w >= 3000*time.Millisecond && w < time.Duration(remain-1200)*time.Millisecond {
				return w, nil
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
	return 0, fmt.Errorf("could not size local-deadline window")
}

// runProbe issues a trusted GET_PLAYER_BOOTSTRAP command through the real
// player-svr LogicService and returns a short outcome descriptor.  The
// bootstrap runs inside a PlayerActor Turn (SubmitVersioned), so it exercises
// the lease-admission gate from within the serialized mailbox. During a directly
// observed local-deadline fence it must not succeed or mutate business state;
// this probe does not claim rejection before mailbox entry.
func runProbe(client logicrpcpb.LogicServiceClient, playerID uint64, operationID string, tw *timelineWriter) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	payload, err := proto.Marshal(&publicpb.GetPlayerBootstrapRequest{})
	if err != nil {
		return "", err
	}
	req := &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			OperationId: operationID, RequestId: "r-" + operationID,
			CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP, Payload: payload,
		},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: playerID, ConnectionId: "c"},
		},
		Dispatch: &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: playerID},
		},
	}
	resp, err := client.ExecuteCommand(ctx, req)
	if err != nil {
		return "gRPC_ERR:" + err.Error(), nil
	}
	outcome := resp.GetBusiness()
	desc := fmt.Sprintf("status=%s code=%v msg=%q", outcome.Status, outcome.Error.GetErrorCode(), outcome.Error.GetErrorMessage())
	tw.write(map[string]any{"event": "probe", "operation": operationID, "outcome": desc})
	return desc, nil
}

func portString(p int) string { return fmt.Sprintf("%d", p) }
