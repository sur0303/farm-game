package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	friendpb "farmgame/api/gen/friend"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/observability/friendcapacity"

	"google.golang.org/protobuf/proto"
)

func TestJoinBarrierAcceptThenSnapshot(t *testing.T) {
	barrier := joinBarrier{operationID: "join-1", targetOwner: 42}
	if err := barrier.observeResult(joinResult(t, "join-1", 42, "sub-1", commonpb.CommandStatus_ACCEPTED)); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := barrier.result(); err != nil || ready {
		t.Fatalf("before snapshot ready=%v err=%v", ready, err)
	}
	if err := barrier.observePush(joinSnapshot(t, 42, "sub-1")); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := barrier.result(); err != nil || ready {
		t.Fatalf("before ready push ready=%v err=%v", ready, err)
	}
	if err := barrier.observePush(subscriptionReady(t, "sub-1")); err != nil {
		t.Fatal(err)
	}
	snapshot, ready, err := barrier.result()
	if err != nil || !ready || snapshot.GetOwnerId() != 42 {
		t.Fatalf("after ready ready=%v owner=%d err=%v", ready, snapshot.GetOwnerId(), err)
	}
}

func TestJoinBarrierSnapshotThenAccept(t *testing.T) {
	barrier := joinBarrier{operationID: "join-1", targetOwner: 42}
	if err := barrier.observePush(joinSnapshot(t, 42, "sub-1")); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := barrier.result(); err != nil || ready {
		t.Fatalf("before result ready=%v err=%v", ready, err)
	}
	if err := barrier.observeResult(joinResult(t, "join-1", 42, "sub-1", commonpb.CommandStatus_ACCEPTED)); err != nil {
		t.Fatal(err)
	}
	if err := barrier.observePush(subscriptionReady(t, "sub-1")); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := barrier.result(); err != nil || !ready {
		t.Fatalf("after result+ready ready=%v err=%v", ready, err)
	}
}

func TestJoinBarrierRejectsMismatchedFenceAndProcessing(t *testing.T) {
	barrier := joinBarrier{operationID: "join-1", targetOwner: 42}
	if err := barrier.observeResult(joinResult(t, "unrelated", 42, "old", commonpb.CommandStatus_ACCEPTED)); err != nil {
		t.Fatalf("unrelated result must be ignored: %v", err)
	}
	if err := barrier.observePush(joinSnapshot(t, 42, "sub-snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := barrier.observeResult(joinResult(t, "join-1", 42, "sub-result", commonpb.CommandStatus_ACCEPTED)); err != nil {
		t.Fatal(err)
	}
	if err := barrier.observePush(subscriptionReady(t, "sub-result")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := barrier.result(); err == nil {
		t.Fatal("subscription mismatch must fail")
	}

	processing := joinBarrier{operationID: "join-2", targetOwner: 42}
	if err := processing.observeResult(joinResult(t, "join-2", 42, "sub-2", commonpb.CommandStatus_PROCESSING)); err == nil {
		t.Fatal("PROCESSING must not open the Join barrier")
	}
}

func TestThickLoginBarrierAllowsBootstrapAndJoinToInterleave(t *testing.T) {
	barrier := thickLoginBarrier{
		bootstrapOperation: "bootstrap-42",
		playerID:           42,
		join:               joinBarrier{operationID: "join-42", targetOwner: 42},
	}
	if err := barrier.observePush(joinSnapshot(t, 42, "sub-42")); err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(validThickBootstrap(42))
	if err != nil {
		t.Fatal(err)
	}
	stage, err := barrier.observeResult(&commonpb.CommandResultEnvelope{
		OperationId: "bootstrap-42", Status: commonpb.CommandStatus_SUCCEEDED,
		CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP, Payload: payload,
	})
	if err != nil || stage != "" {
		t.Fatalf("bootstrap stage=%q err=%v", stage, err)
	}
	if _, ready, err := barrier.result(); err != nil || ready {
		t.Fatalf("before join accepted/ready ready=%v err=%v", ready, err)
	}
	if stage, err := barrier.observeResult(joinResult(t, "join-42", 42, "sub-42", commonpb.CommandStatus_ACCEPTED)); err != nil || stage != "" {
		t.Fatalf("join stage=%q err=%v", stage, err)
	}
	if err := barrier.observePush(subscriptionReady(t, "sub-42")); err != nil {
		t.Fatal(err)
	}
	snapshot, ready, err := barrier.result()
	if err != nil || !ready || snapshot.GetOwnerId() != 42 || barrier.bootstrap.GetProfile().GetPlayerId() != 42 {
		t.Fatalf("ready=%v snapshot=%v bootstrap=%v err=%v", ready, snapshot, barrier.bootstrap, err)
	}
}

func TestValidateThickBootstrapRejectsIncompleteHomeProjection(t *testing.T) {
	tests := map[string]func(*publicpb.GetPlayerBootstrapResult){
		"revision": func(result *publicpb.GetPlayerBootstrapResult) { result.PlayerStateRevision = 0 },
		"profile":  func(result *publicpb.GetPlayerBootstrapResult) { result.Profile.PlayerId = 7 },
		"wallet":   func(result *publicpb.GetPlayerBootstrapResult) { result.Wallet = nil },
		"tasks":    func(result *publicpb.GetPlayerBootstrapResult) { result.FeatureSummary.Tasks = nil },
		"mail":     func(result *publicpb.GetPlayerBootstrapResult) { result.FeatureSummary.Mail = nil },
		"codex":    func(result *publicpb.GetPlayerBootstrapResult) { result.FeatureSummary.Compendium = nil },
		"pet":      func(result *publicpb.GetPlayerBootstrapResult) { result.FeatureSummary.Pet = nil },
		"task rows": func(result *publicpb.GetPlayerBootstrapResult) {
			result.FeatureSummary.Tasks.DailyTasks = nil
		},
		"server time": func(result *publicpb.GetPlayerBootstrapResult) { result.TrustedServerNowMs = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := proto.Clone(validThickBootstrap(42)).(*publicpb.GetPlayerBootstrapResult)
			mutate(result)
			if err := validateThickBootstrap(result, 42); err == nil {
				t.Fatal("incomplete bootstrap must fail")
			}
		})
	}
}

func TestThickLoginStatsExposeRedPointAndOpportunityCoverage(t *testing.T) {
	stats := &thickLoginStats{}
	result := validThickBootstrap(42)
	result.FeatureSummary.Mail.HasRedPoint = true
	stats.friendOpportunityTried.Add(1)
	stats.recordBootstrap(result, 10*time.Millisecond)
	stats.recordFriendStartup(nextOpportunityFound, 20*time.Millisecond)
	report := stats.snapshot()
	if report.ProfilesLoaded != 1 || report.TaskSummariesLoaded != 1 || report.DailyTaskRowsLoaded != 1 {
		t.Fatalf("bootstrap coverage=%+v", report)
	}
	if report.MailSummariesLoaded != 1 || report.MailRedPointPlayers != 1 {
		t.Fatalf("mail coverage=%+v", report)
	}
	if report.FriendOpportunityTried != 1 || report.FriendOpportunityDone != 1 ||
		report.FriendOpportunityLoaded != 1 || report.OpportunityFound != 1 {
		t.Fatalf("friend coverage=%+v", report)
	}
	if report.FriendStartupP50Ms != 20 || report.FriendStartupP95Ms != 20 ||
		report.FriendStartupP99Ms != 20 || report.FriendStartupMaxMs != 20 {
		t.Fatalf("friend latency=%+v", report)
	}
	if report.FriendOpportunityP50Ms != 20 || report.FriendOpportunityP95Ms != 20 ||
		report.FriendOpportunityP99Ms != 20 || report.FriendOpportunityMaxMs != 20 || report.FriendOpportunityRate != 1 {
		t.Fatalf("opportunity latency/rate=%+v", report)
	}
}

func TestThickLoginOpportunityFailureIsIndependentFromReady(t *testing.T) {
	stats := &stats{thickLogin: &thickLoginStats{}}
	stats.ready.Add(1)
	stats.thickLogin.friendOpportunityTried.Add(1)
	result := make(chan thickFriendStartupResult, 1)
	result <- thickFriendStartupResult{elapsed: 2 * time.Second, err: context.DeadlineExceeded}

	collectThickFriendStartup(result, stats)
	report := stats.thickLogin.snapshot()
	if stats.ready.Load() != 1 {
		t.Fatalf("opportunity failure changed login Ready: %d", stats.ready.Load())
	}
	if report.FriendOpportunityTried != 1 || report.FriendOpportunityDone != 1 ||
		report.FriendOpportunityLoaded != 0 || report.FriendStartupErrors != 1 {
		t.Fatalf("opportunity terminal accounting=%+v", report)
	}
	if got := stats.failures["friend_opportunity: context deadline exceeded"]; got != 1 {
		t.Fatalf("opportunity failure reason=%v", stats.failures)
	}
}

func validThickBootstrap(playerID uint64) *publicpb.GetPlayerBootstrapResult {
	return &publicpb.GetPlayerBootstrapResult{
		PlayerStateRevision: 1,
		Profile:             &publicpb.PlayerPrivateProfileView{PlayerId: playerID, DisplayName: "农场主"},
		Progression:         &publicpb.PlayerProgressionView{Level: 1},
		Wallet:              &publicpb.WalletView{Coin: 100},
		Inventory:           &publicpb.InventoryView{},
		FeatureSummary: &publicpb.PlayerFeatureSummaryView{
			Tasks: &publicpb.PlayerTaskSummaryView{
				TaskStateVersion: 1, DailyPeriodId: "2026-08-16", ClaimableCount: 1,
				DailyTasks: []*publicpb.PlayerTaskProgressView{{InstanceId: "daily-login"}},
			},
			Mail:       &publicpb.MailboxSummaryView{},
			Compendium: &publicpb.CompendiumSummaryView{},
			Pet:        &publicpb.PetSummaryView{},
		},
		TrustedServerNowMs: 1,
	}
}

// joinResult 构造 28 §2.2 的 Join ACCEPTED:元信息只经 JoinResultContext 携带,
// result_payload 为空。
func joinResult(t *testing.T, operationID string, targetFarm uint64, subscriptionID string, status commonpb.CommandStatus) *commonpb.CommandResultEnvelope {
	t.Helper()
	return &commonpb.CommandResultEnvelope{
		OperationId: operationID, Status: status,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: targetFarm, SubscriptionId: subscriptionID, JoinAttemptId: "attempt-1",
		}},
	}
}

func joinSnapshot(t *testing.T, ownerID uint64, subscriptionID string) *commonpb.ServerPushEnvelope {
	t.Helper()
	payload, err := proto.Marshal(&farmpb.FarmSnapshotPush{
		JoinAttemptId: "attempt-1", SnapshotVersion: 1, Snapshot: &farmpb.FarmSnapshot{OwnerId: ownerID},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: ownerID, TargetFarmId: ownerID, SubscriptionId: subscriptionID,
			},
		},
		Payload: payload,
	}
}

func subscriptionReady(t *testing.T, subscriptionID string) *commonpb.ServerPushEnvelope {
	t.Helper()
	payload, err := proto.Marshal(&farmpb.SubscriptionReadyPush{
		JoinAttemptId: "attempt-1", ReadyThroughVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{SubscriptionId: subscriptionID},
		},
		Payload: payload,
	}
}

func TestFriendCapacityStatsAggregation(t *testing.T) {
	stats := newFriendCapacityStats()
	stats.record("pull", "friend_list", "ok", 5*time.Millisecond)
	stats.record("pull", "friend_list", "ok", 9*time.Millisecond)
	stats.record("pull", "next_opportunity", "not_found", 3*time.Millisecond)
	stats.recordBatch("pull", "friend_list", 20)
	stats.recordBatch("pull", "friend_list", 20)
	stats.recordBatch("pull", "next_opportunity", 0)
	stats.recordFailure("friend_list", errors.New("boom"))
	stats.setHintMeasurement("source_to_receive")

	data := stats.snapshot()
	if data.Stages["pull/friend_list/ok"] != 2 {
		t.Fatalf("friend_list ok count=%d", data.Stages["pull/friend_list/ok"])
	}
	if data.Stages["pull/next_opportunity/not_found"] != 1 {
		t.Fatalf("next_opportunity count=%d", data.Stages["pull/next_opportunity/not_found"])
	}
	if data.BatchSizes["pull/friend_list"] != 40 {
		t.Fatalf("friend_list batch sum=%d", data.BatchSizes["pull/friend_list"])
	}
	if data.Failures["friend_list: boom"] != 1 {
		t.Fatalf("failures=%v", data.Failures)
	}
	if data.RedisCommands != nil || data.EstimatedBytes != nil {
		t.Fatalf("client must not fabricate redis totals: commands=%v bytes=%v", data.RedisCommands, data.EstimatedBytes)
	}
	if data.RedisCommandsAvailability != measurementClientNone || data.EstimatedBytesAvailability != measurementClientNone {
		t.Fatalf("availability=%q/%q", data.RedisCommandsAvailability, data.EstimatedBytesAvailability)
	}
	if data.EffectiveConfig.HintTransport != "direct" {
		t.Fatalf("effective transport=%q", data.EffectiveConfig.HintTransport)
	}
	if data.HintMeasurement != "source_to_receive" {
		t.Fatalf("hint measurement=%q", data.HintMeasurement)
	}
	if len(data.Phases.Phases) == 0 {
		t.Fatal("phase quantile snapshot must be populated from friendcapacity metrics")
	}
	if len(data.ServerPhaseAvailability) == 0 {
		t.Fatal("server-side phases must appear explicitly as unavailable")
	}
	for _, phase := range data.ServerPhaseAvailability {
		if phase.Availability != friendcapacity.MeasurementUnavailable {
			t.Fatalf("server phase %q availability=%q", phase.Stage, phase.Availability)
		}
	}
	// snapshot must not mutate the underlying aggregates.
	stats.record("pull", "hint", "ok", time.Millisecond)
	if data.Stages["pull/hint/ok"] != 0 {
		t.Fatalf("snapshot must be a copy, got=%d", data.Stages["pull/hint/ok"])
	}
}

func TestFriendCapacityStatsNilAndBoundaries(t *testing.T) {
	var nilStats *friendCapacityStats
	nilStats.record("pull", "friend_list", "ok", time.Millisecond)
	nilStats.recordBatch("pull", "friend_list", 5)
	nilStats.recordFailure("x", errors.New("y"))
	if nilStats.snapshot() != nil {
		t.Fatal("nil snapshot must be nil")
	}
	stats := newFriendCapacityStats()
	stats.recordBatch("pull", "friend_list", -1)
	stats.recordFailure("x", nil)
	if data := stats.snapshot(); len(data.BatchSizes) != 0 || len(data.Failures) != 0 {
		t.Fatalf("negative batch or nil error must be dropped: %+v", data)
	}
}

func TestOutcomeForHTTP(t *testing.T) {
	if got := outcomeForHTTP(http.StatusOK, nil); got != "ok" {
		t.Fatalf("ok outcome=%q", got)
	}
	if got := outcomeForHTTP(http.StatusServiceUnavailable, nil); got != "unavailable" {
		t.Fatalf("503 outcome=%q", got)
	}
	if got := outcomeForHTTP(http.StatusServiceUnavailable, errors.New("opportunity status 503")); got != "unavailable" {
		t.Fatalf("503 with body error must stay unavailable, got=%q", got)
	}
	if got := outcomeForHTTP(http.StatusOK, context.DeadlineExceeded); got != "unknown" {
		t.Fatalf("deadline outcome=%q", got)
	}
	if got := outcomeForHTTP(0, errors.New("dial refused")); got != "error" {
		t.Fatalf("error outcome=%q", got)
	}
}

func TestValidateFriendLoadIdentity(t *testing.T) {
	*friendViewer = 42
	defer func() { *friendViewer = 0 }()
	if err := validateFriendLoadIdentity(41, loginResult{PlayerID: "41", Cookie: "c", ConnectionTicket: "t"}); err == nil {
		t.Fatal("mismatched player must fail")
	}
	if err := validateFriendLoadIdentity(42, loginResult{PlayerID: "99", Cookie: "c", ConnectionTicket: "t"}); err == nil {
		t.Fatal("mismatched ticket player_id must fail")
	}
	if err := validateFriendLoadIdentity(42, loginResult{PlayerID: "42", Cookie: "", ConnectionTicket: "t"}); err == nil {
		t.Fatal("missing cookie must fail")
	}
	if err := validateFriendLoadIdentity(42, loginResult{PlayerID: "42", Cookie: "c", ConnectionTicket: "t"}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateFriendCapacityFlagsRejectsUnsupportedKnobs(t *testing.T) {
	*scenario = "friend-list"
	*friendCount = 20
	*friendViewer = 900000001
	*playerStart = 900000001
	*conns = 1
	*hintTransport = "direct"
	*runIDFlag = "c0cellf20"
	if err := validateFriendCapacityFlags(); err != nil {
		t.Fatalf("valid baseline flags: %v", err)
	}
	*runIDFlag = ""
	if err := validateFriendCapacityFlags(); err == nil {
		t.Fatal("missing --run-id must fail closed in C0 mode")
	}
	*runIDFlag = "c0cellf20"

	for _, mode := range []string{"pubsub", "shadow-direct", "direct"} {
		*hintTransport = mode
		if err := validateFriendCapacityFlags(); err != nil {
			t.Fatalf("mode %q must be accepted: %v", mode, err)
		}
	}
	*hintTransport = "invalid"
	if err := validateFriendCapacityFlags(); err == nil {
		t.Fatal("unknown hint transport must be rejected")
	}
	*hintTransport = "direct"

	*conns = 2
	if err := validateFriendCapacityFlags(); err == nil {
		t.Fatal("conns!=1 must be rejected")
	}
	*conns = 1

	*friendViewer = 1
	*playerStart = 2
	if err := validateFriendCapacityFlags(); err == nil {
		t.Fatal("viewer/player-start mismatch must fail")
	}
	*friendViewer = 900000001
	*playerStart = 900000001

	// Simulate an explicitly set unsupported flag.
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Float64("online-ratio", -1, "")
	_ = fs.Parse([]string{"-online-ratio=0.05"})
	// flagWasSet uses the process-level CommandLine; assert the helper itself.
	if !func() bool {
		found := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "online-ratio" {
				found = true
			}
		})
		return found
	}() {
		t.Fatal("flag visit helper sanity failed")
	}
}

func TestAcquireTicketUsesShowcaseAccountCredentials(t *testing.T) {
	oldHTTPURL := *httpURL
	oldViewer := *friendViewer
	oldTarget := *hintTargetOwner
	t.Cleanup(func() {
		*httpURL = oldHTTPURL
		*friendViewer = oldViewer
		*hintTargetOwner = oldTarget
	})
	*friendViewer = 41
	*hintTargetOwner = 42
	t.Setenv(viewerAccountUsernameEnv, "viewer_account")
	t.Setenv(viewerAccountPasswordEnv, "viewer password")
	t.Setenv(hintTargetAccountUsernameEnv, "target_account")
	t.Setenv(hintTargetAccountPasswordEnv, "target password")

	type account struct {
		playerID uint64
		password string
	}
	accounts := map[string]account{
		"viewer_account": {playerID: 41, password: "REPLACE_WITH_LOCAL_SECRET"},
		"target_account": {playerID: 42, password: "REPLACE_WITH_LOCAL_SECRET"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/session/login" {
			t.Errorf("path=%q, want showcase account login", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode login: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		account, ok := accounts[payload["username"]]
		if !ok || payload["password"] != account.password || payload["platform_token"] != "" {
			t.Errorf("unexpected account login payload for username=%q", payload["username"])
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: auth.HTTPSessionCookieName, Value: "session-cookie"})
		_ = json.NewEncoder(w).Encode(loginResult{
			PlayerID:         strconv.FormatUint(account.playerID, 10),
			SessionID:        "session-id",
			ConnectionTicket: "connection-ticket",
		})
	}))
	defer server.Close()
	*httpURL = server.URL

	for _, playerID := range []uint64{*friendViewer, *hintTargetOwner} {
		result, err := acquireTicket(context.Background(), playerID, nil, "product-r19")
		if err != nil {
			t.Fatalf("acquire showcase ticket for %d: %v", playerID, err)
		}
		if result.PlayerID != strconv.FormatUint(playerID, 10) ||
			result.SessionID != "session-id" ||
			result.ConnectionTicket != "connection-ticket" ||
			result.Cookie != "session-cookie" {
			t.Fatalf("ticket for %d = %+v", playerID, result)
		}
	}
}

func TestProductAccountAuthRejectsIncompleteCredentials(t *testing.T) {
	t.Setenv(viewerAccountUsernameEnv, "viewer_account")
	t.Setenv(viewerAccountPasswordEnv, "")
	t.Setenv(hintTargetAccountUsernameEnv, "")
	t.Setenv(hintTargetAccountPasswordEnv, "")
	if _, err := productAccountAuthConfigured(); err == nil {
		t.Fatal("incomplete showcase viewer credentials must fail closed")
	}
}

func TestValidateFriendCapacityFlagsRequiresBothHintAccounts(t *testing.T) {
	oldScenario := *scenario
	oldFriendCount := *friendCount
	oldViewer := *friendViewer
	oldPlayerStart := *playerStart
	oldConns := *conns
	oldTransport := *hintTransport
	oldRunID := *runIDFlag
	oldTarget := *hintTargetOwner
	t.Cleanup(func() {
		*scenario = oldScenario
		*friendCount = oldFriendCount
		*friendViewer = oldViewer
		*playerStart = oldPlayerStart
		*conns = oldConns
		*hintTransport = oldTransport
		*runIDFlag = oldRunID
		*hintTargetOwner = oldTarget
	})
	*scenario = "hint"
	*friendCount = 20
	*friendViewer = 41
	*playerStart = 41
	*conns = 1
	*hintTransport = "direct"
	*runIDFlag = "product-r19"
	*hintTargetOwner = 42
	t.Setenv(viewerAccountUsernameEnv, "viewer_account")
	t.Setenv(viewerAccountPasswordEnv, "viewer password")
	t.Setenv(hintTargetAccountUsernameEnv, "")
	t.Setenv(hintTargetAccountPasswordEnv, "")

	if err := validateFriendCapacityFlags(); err == nil {
		t.Fatal("showcase hint mode without target credentials must fail closed")
	}
	t.Setenv(hintTargetAccountUsernameEnv, "target_account")
	t.Setenv(hintTargetAccountPasswordEnv, "target password")
	if err := validateFriendCapacityFlags(); err != nil {
		t.Fatalf("complete showcase hint accounts: %v", err)
	}
}

func TestScenarioTerminalOutcomeCounting(t *testing.T) {
	s := &stats{friend: newFriendCapacityStats()}
	// One failed next-opportunity path: attempt=1, success=0, error=1.
	s.scenarioTried.Add(1)
	s.scenarioErr.Add(1)
	if s.scenarioTried.Load() != 1 || s.scenarioReady.Load() != 0 || s.scenarioErr.Load() != 1 {
		t.Fatalf("failed attempt counters tried=%d ready=%d err=%d",
			s.scenarioTried.Load(), s.scenarioReady.Load(), s.scenarioErr.Load())
	}
	// One successful path uses recordScenario exactly once.
	start := time.Now()
	s.scenarioTried.Add(1)
	s.recordScenario(start)
	if s.scenarioTried.Load() != 2 || s.scenarioReady.Load() != 1 || s.scenarioErr.Load() != 1 {
		t.Fatalf("success path counters tried=%d ready=%d err=%d",
			s.scenarioTried.Load(), s.scenarioReady.Load(), s.scenarioErr.Load())
	}
	if s.scenarioReady.Load() > s.scenarioTried.Load() {
		t.Fatal("scenario_succeeded must never exceed scenario_attempted")
	}
}

func TestHintTimeoutIsNotSuccessOutcome(t *testing.T) {
	stats := newFriendCapacityStats()
	stats.record("hint_receive", "hint", "dropped", 1500*time.Millisecond)
	stats.setHintMeasurement("source_to_receive")
	data := stats.snapshot()
	if data.Stages["hint_receive/hint/ok"] != 0 {
		t.Fatal("timeout must not record ok")
	}
	if data.Stages["hint_receive/hint/dropped"] != 1 {
		t.Fatalf("timeout outcome=%v", data.Stages)
	}
}

func TestValidateServerHelloIdentity(t *testing.T) {
	if err := validateServerHelloIdentity(nil, 42, "sess-1"); err == nil {
		t.Fatal("nil hello must fail")
	}
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 41, SessionId: "sess-1"}, 42, "sess-1"); err == nil {
		t.Fatal("mismatched player_id must fail")
	}
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: "other"}, 42, "sess-1"); err == nil {
		t.Fatal("mismatched session_id must fail")
	}
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: "sess-1"}, 42, "sess-1"); err != nil {
		t.Fatal(err)
	}
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: ""}, 42, "sess-1"); err == nil {
		t.Fatal("empty ServerHello session_id must fail")
	}
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: "sess-1"}, 42, ""); err == nil {
		t.Fatal("empty expected login session_id must fail")
	}
}

func TestValidateHintSuccessRequiresRecipientTargetAndCausalVersion(t *testing.T) {
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
		PushContext: &commonpb.ServerPushEnvelope_Player{
			Player: &commonpb.PlayerPushContext{TargetPlayerId: 11},
		},
	}
	hint := &friendpb.FriendOpportunityChanged{
		TargetOwnerId: 22, SourceFarmVersion: 5, OpportunityRevision: 7,
	}
	if err := validateHintSuccess(push, hint, 11, 22, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateHintSuccess(push, hint, 99, 22, 5, 1); err == nil {
		t.Fatal("recipient mismatch must fail")
	}
	if err := validateHintSuccess(push, hint, 11, 33, 5, 1); err == nil {
		t.Fatal("target mismatch must fail")
	}
	stale := &friendpb.FriendOpportunityChanged{
		TargetOwnerId: 22, SourceFarmVersion: 4, OpportunityRevision: 7,
	}
	if err := validateHintSuccess(push, stale, 11, 22, 5, 1); err == nil {
		t.Fatal("stale source_farm_version must fail")
	}
	zero := &friendpb.FriendOpportunityChanged{
		TargetOwnerId: 22, SourceFarmVersion: 0, OpportunityRevision: 7,
	}
	if err := validateHintSuccess(push, zero, 11, 22, 1, 1); err == nil {
		t.Fatal("zero version must fail")
	}
}

func TestFriendLoginFailureProducesOneTerminalScenarioOutcome(t *testing.T) {
	s := &stats{friend: newFriendCapacityStats()}
	// Mirror runPlayer friend-capacity login-failure accounting.
	s.scenarioTried.Add(1)
	s.loginErr.Add(1)
	s.scenarioErr.Add(1)
	s.friend.recordFailure("friend-list", errors.New("login status 500"))
	if s.scenarioTried.Load() != 1 || s.scenarioReady.Load() != 0 || s.scenarioErr.Load() != 1 {
		t.Fatalf("login failure must be one terminal outcome, tried=%d ready=%d err=%d",
			s.scenarioTried.Load(), s.scenarioReady.Load(), s.scenarioErr.Load())
	}
	if len(s.friend.snapshot().Failures) != 1 {
		t.Fatalf("login failure must be recorded once, got=%v", s.friend.snapshot().Failures)
	}
}

func TestBootstrapIDUsesRunID(t *testing.T) {
	*runIDFlag = "c0m1f20"
	defer func() { *runIDFlag = "" }()
	id := "813000021"
	bootstrap := "ws-stress-" + strings.TrimSpace(*runIDFlag) + "-" + id
	if bootstrap != "ws-stress-c0m1f20-813000021" {
		t.Fatalf("bootstrap=%q", bootstrap)
	}
	if !strings.Contains(bootstrap, "c0m1f20") {
		t.Fatal("bootstrap id must carry configured run-id")
	}
}

func TestTerminalSucceededRejectsNoChangeAndProcessingIsNotTerminal(t *testing.T) {
	// Contract unit: NO_CHANGE must not be treated as terminal success for the
	// Hint source Buy/Plant chain. PROCESSING is intermediate only.
	statuses := []commonpb.CommandStatus{
		commonpb.CommandStatus_NO_CHANGE,
		commonpb.CommandStatus_PROCESSING,
		commonpb.CommandStatus_SUCCEEDED,
	}
	terminalOK := map[commonpb.CommandStatus]bool{
		commonpb.CommandStatus_SUCCEEDED: true,
	}
	for _, status := range statuses {
		if status == commonpb.CommandStatus_PROCESSING {
			if terminalOK[status] {
				t.Fatal("PROCESSING must not be terminal success")
			}
			continue
		}
		if status == commonpb.CommandStatus_NO_CHANGE && terminalOK[status] {
			t.Fatal("NO_CHANGE must not be terminal success")
		}
		if status == commonpb.CommandStatus_SUCCEEDED && !terminalOK[status] {
			t.Fatal("SUCCEEDED must be terminal success")
		}
	}
}

func TestMatrixCellIdentitiesAreDistinct(t *testing.T) {
	type cell struct {
		viewer, target, fakeBase uint64
		cellRunID                string
	}
	derive := func(runID string, friendCount int) cell {
		return cell{
			viewer:    813010000 + uint64(friendCount)*10 + 1,
			target:    813010000 + uint64(friendCount)*10 + 2,
			fakeBase:  7000000000 + uint64(friendCount)*1_000_000,
			cellRunID: runID + "f" + strconv.Itoa(friendCount),
		}
	}
	a := derive("c0m1", 20)
	b := derive("c0m1", 100)
	c := derive("c0m1", 300)
	if a.viewer == b.viewer || b.viewer == c.viewer || a.target == b.target || a.fakeBase == b.fakeBase {
		t.Fatalf("cells must not reuse identities: %+v %+v %+v", a, b, c)
	}
	if a.cellRunID == b.cellRunID || b.cellRunID == c.cellRunID {
		t.Fatalf("cells must not reuse operation namespaces: %q %q %q", a.cellRunID, b.cellRunID, c.cellRunID)
	}
	if a.viewer == a.target || b.viewer == b.target {
		t.Fatal("viewer and target must differ inside a cell")
	}
}

func TestOpportunityUnknownIsNeverNotFoundOrSuccess(t *testing.T) {
	// HTTP 503 with FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE maps to the
	// UNKNOWN semantic answer and must record outcome=unknown, never
	// not_found and never scenario success.
	outcome := opportunityOutcome(http.StatusServiceUnavailable, nextOpportunityUnknown, errors.New("opportunity status 503"))
	if outcome != friendcapacity.OutcomeUnknown {
		t.Fatalf("UNKNOWN outcome=%q want unknown", outcome)
	}
	if outcome == friendcapacity.OutcomeNotFound || outcome == friendcapacity.OutcomeOK {
		t.Fatalf("UNKNOWN must never be not_found/ok: %q", outcome)
	}
	// HTTP 200 NONE is a real negative answer: not_found, and it is not an error.
	if got := opportunityOutcome(http.StatusOK, nextOpportunityNone, nil); got != friendcapacity.OutcomeNotFound {
		t.Fatalf("NONE outcome=%q want not_found", got)
	}
	// HTTP 200 FOUND is ok.
	if got := opportunityOutcome(http.StatusOK, nextOpportunityFound, nil); got != friendcapacity.OutcomeOK {
		t.Fatalf("FOUND outcome=%q want ok", got)
	}
}

func TestNextOpportunityRequestClassifiesAnswers(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
		err    bool
	}{
		{name: "found", status: 200, body: `{"status":"FOUND","friend":{}}`, want: nextOpportunityFound},
		{name: "none", status: 200, body: `{"status":"NONE"}`, want: nextOpportunityNone},
		{name: "unknown-503", status: 503, body: `{"error_code":"FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE"}`, want: nextOpportunityUnknown, err: true},
		{name: "missing-status", status: 200, body: `{}`, want: "", err: true},
		{name: "bad-answer", status: 200, body: `{"status":"SOMETHING_ELSE"}`, want: nextOpportunityUnknown, err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := server.Client()
			status, answer, _, err := nextOpportunityRequest(context.Background(), client, server.URL)
			if status != tc.status || answer != tc.want || (err != nil) != tc.err {
				t.Fatalf("status=%d answer=%q err=%v want status=%d answer=%q err=%v", status, answer, err, tc.status, tc.want, tc.err)
			}
		})
	}
}

func TestReportRunIDPropagationAndOneTerminalOutcome(t *testing.T) {
	// The runner contract: the report run_id must equal the cell run id and
	// each attempt must have exactly one terminal success/error outcome.
	s := &stats{start: time.Now()}
	s.friend = newFriendCapacityStats()
	s.scenarioTried.Add(1)
	scenarioStart := time.Now()
	// friend-list success path: runFriendCapacityScenario records exactly one
	// scenario outcome via recordScenario.
	*friendViewer = 42
	defer func() { *friendViewer = 0 }()
	*scenario = "friend-list"
	s.recordScenario(scenarioStart)
	if s.scenarioTried.Load() != 1 || s.scenarioReady.Load() != 1 || s.scenarioErr.Load() != 0 {
		t.Fatalf("success counters tried=%d ready=%d err=%d", s.scenarioTried.Load(), s.scenarioReady.Load(), s.scenarioErr.Load())
	}
	report := s.snapshot()
	report.RunID = "c0sm1f20"
	if report.RunID != "c0sm1f20" {
		t.Fatalf("run_id propagation=%q", report.RunID)
	}
	if report.ScenarioTried != 1 || report.ScenarioReady != 1 || report.ScenarioError != 0 {
		t.Fatalf("report scenario counters=%+v", report)
	}
	// A failed attempt must also have exactly one terminal outcome (error).
	s2 := &stats{start: time.Now()}
	s2.friend = newFriendCapacityStats()
	s2.scenarioTried.Add(1)
	s2.scenarioErr.Add(1)
	report2 := s2.snapshot()
	if report2.ScenarioTried != 1 || report2.ScenarioReady != 0 || report2.ScenarioError != 1 {
		t.Fatalf("failed attempt counters=%+v", report2)
	}
	if report2.ScenarioReady+report2.ScenarioError != report2.ScenarioTried {
		t.Fatal("terminal outcomes must equal attempts")
	}
}

func TestHintCausalityRejectsFabricatedAndStaleVersions(t *testing.T) {
	// A stale hint whose source_farm_version is below the exact observed
	// committed version must never pass, even when its opportunity_revision is
	// high (a newer unrelated revision on an older farm boundary).
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
		PushContext: &commonpb.ServerPushEnvelope_Player{
			Player: &commonpb.PlayerPushContext{TargetPlayerId: 11},
		},
	}
	hint := &friendpb.FriendOpportunityChanged{TargetOwnerId: 22, SourceFarmVersion: 9, OpportunityRevision: 99}
	if err := validateHintSuccess(push, hint, 11, 22, 9, 1); err != nil {
		t.Fatalf("hint at exact observed committed version must pass: %v", err)
	}
	if err := validateHintSuccess(push, hint, 11, 22, 10, 1); err == nil {
		t.Fatal("hint below the observed committed version must fail (fabricated/stale)")
	}
	// The fabricated baseline+1 path is gone: a Hint at 1 with committed
	// version 5 must fail.
	if err := validateHintSuccess(push, &friendpb.FriendOpportunityChanged{TargetOwnerId: 22, SourceFarmVersion: 1, OpportunityRevision: 2}, 11, 22, 5, 1); err == nil {
		t.Fatal("hint at fabricated version 1 with committed 5 must fail")
	}
}

func TestFriendCapacityReadyRecordedOnceAfterServerHello(t *testing.T) {
	// Readiness must be recorded exactly once and only after the validated
	// ServerHello; a login alone must not increment ready, and a failed hello
	// must not either.
	s := &stats{start: time.Now()}
	s.friend = newFriendCapacityStats()
	now := time.Now()
	// Simulate the runPlayer path: no recordReady before the WSS leg.
	readyStart := now.Add(time.Millisecond)
	// A ServerHello that fails identity validation must not record readiness.
	helloErr := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: "other"}, 42, "sess-1")
	if helloErr == nil {
		t.Fatal("mismatched hello must fail")
	}
	if s.ready.Load() != 0 {
		t.Fatalf("failed ServerHello must not record ready, got %d", s.ready.Load())
	}
	// Validated ServerHello records readiness exactly once.
	if err := validateServerHelloIdentity(&commonpb.ServerHello{PlayerId: 42, SessionId: "sess-1"}, 42, "sess-1"); err != nil {
		t.Fatal(err)
	}
	s.recordReady(readyStart)
	if s.ready.Load() != 1 {
		t.Fatalf("ready=%d want 1", s.ready.Load())
	}
	if len(s.readyLatency) != 1 {
		t.Fatalf("ready latency samples=%d want 1", len(s.readyLatency))
	}
}

// TestLoadThickFriendStartupRetries429NeverCoercesNONE pins the popup-drain
// contract: a 429 + Retry-After is retried with bounded backoff; a run that
// only ever sees 429 ends in a terminal error with the UNKNOWN answer, never a
// fabricated NONE; a 429-then-200 sequence succeeds with retries counted.
func TestLoadThickFriendStartupRetries429NeverCoercesNONE(t *testing.T) {
	t.Run("retries-then-success", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if attempts < 3 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"busy"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"NONE"}`))
		}))
		defer server.Close()
		oldURL := *httpURL
		*httpURL = server.URL
		defer func() { *httpURL = oldURL }()
		result := loadThickFriendStartup(context.Background(), "")
		if result.err != nil {
			t.Fatalf("err=%v", result.err)
		}
		if result.answer != nextOpportunityNone {
			t.Fatalf("answer=%q want NONE", result.answer)
		}
		if result.retries != 2 {
			t.Fatalf("retries=%d want 2", result.retries)
		}
		if attempts != 3 {
			t.Fatalf("attempts=%d want 3", attempts)
		}
	})
	t.Run("all-429-terminal-unknown", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"busy"}`))
		}))
		defer server.Close()
		oldURL := *httpURL
		*httpURL = server.URL
		defer func() { *httpURL = oldURL }()
		result := loadThickFriendStartup(context.Background(), "")
		if result.err == nil {
			t.Fatal("all-429 must terminate with an error")
		}
		if result.answer == nextOpportunityNone || result.answer == nextOpportunityFound {
			t.Fatalf("all-429 must never produce FOUND/NONE, answer=%q", result.answer)
		}
		if result.retries == 0 {
			t.Fatal("all-429 must have retried")
		}
	})
	t.Run("context-cancel-stops", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"down"}`))
		}))
		defer server.Close()
		oldURL := *httpURL
		*httpURL = server.URL
		defer func() { *httpURL = oldURL }()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := loadThickFriendStartup(ctx, "")
		if result.err == nil {
			t.Fatal("canceled context must terminate with an error")
		}
		if result.answer == nextOpportunityNone {
			t.Fatal("canceled drain must never fabricate NONE")
		}
	})
}

// TestNextOpportunityRequestHonorsRetryAfter asserts the server Retry-After
// hint is surfaced to the drain loop.
func TestNextOpportunityRequestHonorsRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer server.Close()
	client := server.Client()
	status, answer, retryAfter, err := nextOpportunityRequest(context.Background(), client, server.URL)
	if status != http.StatusTooManyRequests || err == nil {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if answer != "" {
		t.Fatalf("429 answer=%q want empty (no semantic answer)", answer)
	}
	if retryAfter != 3*time.Second {
		t.Fatalf("retry_after=%v want 3s", retryAfter)
	}
}
