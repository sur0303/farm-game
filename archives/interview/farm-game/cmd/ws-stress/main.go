// Command ws-stress runs the real FarmGate Login -> ClientHello -> JoinFarm ->
// FarmSnapshot path. It never sends arbitrary bytes that production would
// correctly reject as malformed FarmFrame.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	friendpb "farmgame/api/gen/friend"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/observability/friendcapacity"
	"farmgame/internal/testkit/gateclient"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

var (
	httpURL                      = flag.String("http-url", "http://127.0.0.1:8080", "farm-gate HTTP base URL")
	wsURL                        = flag.String("ws-url", "ws://127.0.0.1:8080/ws", "stable WebSocket URL")
	tlsCA                        = flag.String("tls-ca", "", "optional PEM CA for a wss:// endpoint")
	conns                        = flag.Int("conns", 100, "unique concurrent players")
	rate                         = flag.Int("rate", 10, "new Login/WSS attempts per second")
	duration                     = flag.Duration("duration", 30*time.Second, "test duration")
	playerStart                  = flag.Uint64("player-start", 900000000, "first load-test player id")
	scenario                     = flag.String("scenario", "join", "login, hello, join-accepted, join, thick-login, cold-thick-login, plant, hot or reconnect (cold-thick-login consumes each player ID once)")
	sharedFarm                   = flag.Uint64("shared-farm", 0, "join this farm_id for ALL clients (hot-spot scenario); 0 = each client joins its own farm")
	hotLoops                     = flag.Int("hot-loops", 10, "hot scenario: per-client water loops after plant")
	hotMixed                     = flag.Bool("hot-mixed", false, "hot scenario: non-owner clients also send water commands (permission denials are data)")
	reconnectLoops               = flag.Int("reconnect-loops", 3, "reconnect scenario: full re-login/join attempts per player")
	coldPoolSize                 = flag.Int("cold-pool-size", 0, "cold-thick-login: number of unique player IDs available to this load process")
	coldLaunchDuration           = flag.Duration("cold-launch-duration", 0, "cold-thick-login: launch window; workers drain until -duration")
	skipFriendOpportunity        = flag.Bool("skip-friend-opportunity", false, "thick-login: skip the optional home opportunity popup request")
	friendOpportunityMaxAttempts = flag.Int("friend-opportunity-max-attempts", 5, "thick-login: maximum attempts for the asynchronous opportunity popup request")
	jsonOut                      = flag.String("json-out", "", "optional result JSON path")
	theftPlot                    = flag.Uint("theft-plot-id", 0, "theft-hot: public zero-based plot id")
	theftCycle                   = flag.String("theft-crop-cycle-id", "", "theft-hot: mature crop cycle id")
	theftPrepare                 = flag.Bool("theft-prepare-crop", true, "theft-hot: owner buys/plants through the public protocol before the hot Join")
	theftMaturity                = flag.Duration("theft-maturity-wait", 2500*time.Millisecond, "theft-hot: wait after the public Plant terminal result")
	theftChanges                 = flag.Int("theft-expected-changes", 10, "theft-hot: committed FarmChange count expected on every healthy connection")
	theftPoll                    = flag.Duration("theft-poll-interval", 100*time.Millisecond, "theft-hot: public OperationResult poll interval")
	theftTimeout                 = flag.Duration("theft-terminal-timeout", 30*time.Second, "theft-hot: terminal result and broadcast deadline")
	theftSlow                    = flag.Bool("theft-slow-reader", false, "theft-hot: shared-farm owner stops reading after SubscriptionReady")
	theftRetry                   = flag.Bool("theft-retry", true, "theft-hot: retry every THEFT once with its original operation_id")
	// Friend-capacity real HTTP/WSS evidence (C0). The friend HTTP endpoints
	// are HTTPS-primary; WSS frames for these commands are disabled by the
	// social product profile, so the client drives the real HTTP endpoints
	// after a real Login (which also issues the farm_session cookie).
	friendCount     = flag.Int("friend-count", 300, "friend-capacity: expected friend count used by fixture; 20, 100 or 300")
	friendViewer    = flag.Uint64("friend-viewer", 0, "friend-capacity: required viewer player id; must equal the login player")
	friendPages     = flag.Int("friend-pages", 15, "friend-capacity: friend-list pages per client (page size 20)")
	hintLoops       = flag.Int("hint-loops", 1, "friend-capacity: hint-mode loops per client")
	hintTargetOwner = flag.Uint64("hint-target-owner", 0, "friend-capacity: farm owner whose opportunity change must produce the Hint")
	hintWait        = flag.Duration("hint-wait", 8*time.Second, "friend-capacity: max wait for a validated Hint after the source transition")
	// Unsupported C0 knobs: accepted only when omitted. Explicit values fail closed.
	onlineRatio     = flag.Float64("online-ratio", -1, "UNSUPPORTED in C0; omit this flag (presence seeding is not implemented)")
	friendBase      = flag.Uint64("friend-base", 0, "UNSUPPORTED in C0; omit this flag (fixture owns fake-base)")
	hintTransport   = flag.String("hint-transport", "direct", "friend-capacity: expected deployed Hint mode (pubsub, shadow-direct or direct)")
	jsonMetaOut     = flag.String("json-meta-out", "", "optional path for the C0 metadata sidecar (run_id, namespace, git sha, environment)")
	environmentName = flag.String("environment", "isolated-random-loopback", "test environment label for evidence")
	// runIDFlag is required for friend-capacity C0 so operation/bootstrap/evidence
	// identity matches the runner's configured run_id (+ cell identity).
	runIDFlag = flag.String("run-id", "", "explicit run id used in operation/request/bootstrap ids and evidence; required for friend-capacity C0")
)

const (
	viewerAccountUsernameEnv     = "WS_STRESS_VIEWER_ACCOUNT_USERNAME"
	viewerAccountPasswordEnv     = "WS_STRESS_VIEWER_ACCOUNT_PASSWORD"
	hintTargetAccountUsernameEnv = "WS_STRESS_HINT_TARGET_ACCOUNT_USERNAME"
	hintTargetAccountPasswordEnv = "WS_STRESS_HINT_TARGET_ACCOUNT_PASSWORD"
)

// loadHTTPTransport is shared by all one-shot login clients in one load Pod.
// Login identities remain distinct, but the HTTPS TCP/TLS connection can be
// reused safely. The default net/http transport only keeps two idle
// connections per host, which makes high-rate cold-login tests measure client
// socket churn instead of the Gate login path.
var loadHTTPTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	MaxIdleConns:          4096,
	MaxIdleConnsPerHost:   2048,
	MaxConnsPerHost:       0,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   5 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var loadHTTPClient = &http.Client{
	Transport: loadHTTPTransport,
	Timeout:   15 * time.Second,
}

type stats struct {
	attempted           atomic.Int64
	active              atomic.Int64
	peakActive          atomic.Int64
	ready               atomic.Int64
	loginErr            atomic.Int64
	connectErr          atomic.Int64
	helloErr            atomic.Int64
	joinErr             atomic.Int64
	scenarioTried       atomic.Int64
	scenarioReady       atomic.Int64
	scenarioErr         atomic.Int64
	unexpectedEnd       atomic.Int64
	start               time.Time // test start; legacy window QPS uses this configured window
	lastAttemptUnixNano atomic.Int64
	lastReadyUnixNano   atomic.Int64
	latenciesMu         sync.Mutex
	readyLatency        []time.Duration
	loginLatency        []time.Duration
	helloLatency        []time.Duration
	joinAccepted        []time.Duration
	snapshotWait        []time.Duration
	scenarioTime        []time.Duration
	// hot scenario per-op counters (owner plant -> water xN -> harvest attempt).
	hotOwnerTried atomic.Int64
	hotPlantOK    atomic.Int64
	hotPlantErr   atomic.Int64
	hotWaterOK    atomic.Int64
	hotWaterErr   atomic.Int64
	hotHarvestOK  atomic.Int64
	hotHarvestErr atomic.Int64
	// reconnect scenario counters.
	reconnectTried   atomic.Int64
	reconnectOK      atomic.Int64
	reconnectErr     atomic.Int64
	reconnectMu      sync.Mutex
	reconnectLatency []time.Duration
	failuresMu       sync.Mutex
	failures         map[string]int64
	theft            *theftHotStats
	friend           *friendCapacityStats
	thickLogin       *thickLoginStats
}

// thickLoginStats measures the H5 home bootstrap path. It deliberately keeps
// task-list page loading and the friend roster page out of the login barrier:
// H5 renders task red dots from GET_PLAYER_BOOTSTRAP and the home popup only
// needs the server-owned next-opportunity answer. The full friend-list page is
// covered by the separate friend-list scenario.
type thickLoginStats struct {
	bootstrapErrors         atomic.Int64
	friendStartupErrors     atomic.Int64
	friendStartupRetries    atomic.Int64
	friendOpportunityTried  atomic.Int64
	friendOpportunityDone   atomic.Int64
	profilesLoaded          atomic.Int64
	taskSummariesLoaded     atomic.Int64
	mailSummariesLoaded     atomic.Int64
	compendiumLoaded        atomic.Int64
	petSummariesLoaded      atomic.Int64
	taskClaimableTotal      atomic.Int64
	dailyTaskRowsLoaded     atomic.Int64
	mailRedPointPlayers     atomic.Int64
	friendOpportunityLoaded atomic.Int64
	opportunityFound        atomic.Int64
	opportunityNone         atomic.Int64
	latenciesMu             sync.Mutex
	bootstrapLatency        []time.Duration
	friendStartupLatency    []time.Duration
}

type thickLoginReport struct {
	BootstrapErrors         int64   `json:"bootstrap_errors"`
	FriendStartupErrors     int64   `json:"friend_startup_errors"`
	FriendStartupRetries    int64   `json:"friend_startup_retries"`
	ProfilesLoaded          int64   `json:"profiles_loaded"`
	TaskSummariesLoaded     int64   `json:"task_summaries_loaded"`
	MailSummariesLoaded     int64   `json:"mail_summaries_loaded"`
	CompendiumLoaded        int64   `json:"compendium_summaries_loaded"`
	PetSummariesLoaded      int64   `json:"pet_summaries_loaded"`
	TaskClaimableTotal      int64   `json:"task_claimable_total"`
	DailyTaskRowsLoaded     int64   `json:"daily_task_rows_loaded"`
	MailRedPointPlayers     int64   `json:"mail_red_point_players"`
	FriendOpportunityLoaded int64   `json:"friend_opportunity_loaded"`
	FriendOpportunityTried  int64   `json:"friend_opportunity_attempted"`
	FriendOpportunityDone   int64   `json:"friend_opportunity_completed"`
	OpportunityFound        int64   `json:"friend_opportunity_found"`
	OpportunityNone         int64   `json:"friend_opportunity_none"`
	BootstrapP50Ms          float64 `json:"bootstrap_p50_ms"`
	BootstrapP95Ms          float64 `json:"bootstrap_p95_ms"`
	FriendStartupP50Ms      float64 `json:"friend_startup_p50_ms"`
	FriendStartupP95Ms      float64 `json:"friend_startup_p95_ms"`
	FriendStartupP99Ms      float64 `json:"friend_startup_p99_ms"`
	FriendStartupMaxMs      float64 `json:"friend_startup_max_ms"`
	FriendOpportunityP50Ms  float64 `json:"friend_opportunity_p50_ms"`
	FriendOpportunityP95Ms  float64 `json:"friend_opportunity_p95_ms"`
	FriendOpportunityP99Ms  float64 `json:"friend_opportunity_p99_ms"`
	FriendOpportunityMaxMs  float64 `json:"friend_opportunity_max_ms"`
	FriendOpportunityRate   float64 `json:"friend_opportunity_success_rate"`
}

func (s *stats) recordReconnect(start time.Time) {
	s.reconnectMu.Lock()
	s.reconnectLatency = append(s.reconnectLatency, time.Since(start))
	s.reconnectMu.Unlock()
}

type report struct {
	RunID                    string              `json:"run_id"`
	CellRunID                string              `json:"cell_run_id,omitempty"`
	Scenario                 string              `json:"scenario"`
	AuthMode                 string              `json:"auth_mode"`
	ConfiguredConns          int                 `json:"configured_connections"`
	PlayerStart              uint64              `json:"player_start"`
	ConnectRate              int                 `json:"connect_rate_per_second"`
	DurationSeconds          float64             `json:"configured_duration_seconds"`
	StartedAt                string              `json:"started_at"`
	FinishedAt               string              `json:"finished_at"`
	Attempted                int64               `json:"attempted"`
	Ready                    int64               `json:"ready"`
	ActiveAtEnd              int64               `json:"active_at_end"`
	PeakActive               int64               `json:"peak_active"`
	LoginErrors              int64               `json:"login_errors"`
	ConnectErrors            int64               `json:"connect_errors"`
	HelloErrors              int64               `json:"hello_errors"`
	JoinErrors               int64               `json:"join_errors"`
	ScenarioTried            int64               `json:"scenario_attempted"`
	ScenarioReady            int64               `json:"scenario_succeeded"`
	ScenarioError            int64               `json:"scenario_errors"`
	UnexpectedEnd            int64               `json:"unexpected_disconnects"`
	ReadyRate                float64             `json:"ready_rate"`
	ElapsedMs                float64             `json:"elapsed_ms,omitempty"`
	QPS                      float64             `json:"qps,omitempty"` // legacy ready/configured-window QPS
	LaunchElapsedMs          float64             `json:"launch_elapsed_ms,omitempty"`
	LaunchQPS                float64             `json:"launch_qps,omitempty"`
	ReadyCompletionElapsedMs float64             `json:"ready_completion_elapsed_ms,omitempty"`
	ReadyCompletionQPS       float64             `json:"ready_completion_qps,omitempty"`
	P50Ms                    float64             `json:"ready_p50_ms"`
	P95Ms                    float64             `json:"ready_p95_ms"`
	P99Ms                    float64             `json:"ready_p99_ms"`
	MaxMs                    float64             `json:"ready_max_ms"`
	LoginP50Ms               float64             `json:"login_p50_ms,omitempty"`
	LoginP95Ms               float64             `json:"login_p95_ms,omitempty"`
	HelloP50Ms               float64             `json:"hello_p50_ms,omitempty"`
	HelloP95Ms               float64             `json:"hello_p95_ms,omitempty"`
	JoinAcceptedP50          float64             `json:"join_accepted_p50_ms,omitempty"`
	JoinAcceptedP95          float64             `json:"join_accepted_p95_ms,omitempty"`
	SnapshotP50Ms            float64             `json:"snapshot_p50_ms,omitempty"`
	SnapshotP95Ms            float64             `json:"snapshot_p95_ms,omitempty"`
	ScenarioP95Ms            float64             `json:"scenario_p95_ms"`
	SharedFarm               uint64              `json:"shared_farm,omitempty"`
	HotOwnerClients          int64               `json:"hot_owner_clients,omitempty"`
	HotPlantOK               int64               `json:"hot_plant_ok,omitempty"`
	HotPlantErr              int64               `json:"hot_plant_err,omitempty"`
	HotWaterOK               int64               `json:"hot_water_ok,omitempty"`
	HotWaterErr              int64               `json:"hot_water_err,omitempty"`
	HotHarvestOK             int64               `json:"hot_harvest_ok,omitempty"`
	HotHarvestErr            int64               `json:"hot_harvest_err,omitempty"`
	ReconnectTried           int64               `json:"reconnect_attempted,omitempty"`
	ReconnectOK              int64               `json:"reconnect_ok,omitempty"`
	ReconnectErr             int64               `json:"reconnect_err,omitempty"`
	ReconnectP99Ms           float64             `json:"reconnect_p99_ms,omitempty"`
	FailureReasons           map[string]int64    `json:"failure_reasons,omitempty"`
	Theft                    *theftHotReport     `json:"theft_hot,omitempty"`
	GitSHA                   string              `json:"git_sha,omitempty"`
	Environment              string              `json:"test_environment,omitempty"`
	RedisCommands            int64               `json:"redis_commands,omitempty"`
	EstimatedBytes           int64               `json:"estimated_bytes,omitempty"`
	EstimatedNote            string              `json:"estimated_bytes_note,omitempty"`
	Friend                   *friendCapacityData `json:"friend_capacity,omitempty"`
	ThickLogin               *thickLoginReport   `json:"thick_login,omitempty"`
}

func (s *stats) recordFailure(stage string, err error) {
	if err == nil {
		return
	}
	reason := stage + ": " + stableFailureReason(err)
	s.failuresMu.Lock()
	if s.failures == nil {
		s.failures = make(map[string]int64)
	}
	s.failures[reason]++
	s.failuresMu.Unlock()
}

func stableFailureReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "context deadline exceeded"
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return fmt.Sprintf("websocket close %d: %s", closeErr.Code, closeErr.Text)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "network timeout"
	}
	return err.Error()
}

func (s *stats) recordAttempt() {
	s.attempted.Add(1)
	now := time.Now().UnixNano()
	for {
		previous := s.lastAttemptUnixNano.Load()
		if now <= previous || s.lastAttemptUnixNano.CompareAndSwap(previous, now) {
			return
		}
	}
}

func (s *stats) recordReady(start time.Time) {
	s.recordReadyLatency(time.Since(start))
}

func (s *stats) recordReadyLatency(latency time.Duration) {
	s.ready.Add(1)
	now := time.Now().UnixNano()
	for {
		previous := s.lastReadyUnixNano.Load()
		if now <= previous || s.lastReadyUnixNano.CompareAndSwap(previous, now) {
			break
		}
	}
	s.recordStage(&s.readyLatency, latency)
}

func (s *stats) recordStage(bucket *[]time.Duration, latency time.Duration) {
	s.latenciesMu.Lock()
	*bucket = append(*bucket, latency)
	s.latenciesMu.Unlock()
}

func (s *thickLoginStats) recordBootstrap(result *publicpb.GetPlayerBootstrapResult, elapsed time.Duration) {
	s.profilesLoaded.Add(1)
	tasks := result.GetFeatureSummary().GetTasks()
	s.taskSummariesLoaded.Add(1)
	s.taskClaimableTotal.Add(int64(tasks.GetClaimableCount()))
	s.dailyTaskRowsLoaded.Add(int64(len(tasks.GetDailyTasks())))
	if mail := result.GetFeatureSummary().GetMail(); mail != nil {
		s.mailSummariesLoaded.Add(1)
		if mail.GetHasRedPoint() {
			s.mailRedPointPlayers.Add(1)
		}
	}
	if result.GetFeatureSummary().GetCompendium() != nil {
		s.compendiumLoaded.Add(1)
	}
	if result.GetFeatureSummary().GetPet() != nil {
		s.petSummariesLoaded.Add(1)
	}
	s.latenciesMu.Lock()
	s.bootstrapLatency = append(s.bootstrapLatency, elapsed)
	s.latenciesMu.Unlock()
}

func (s *thickLoginStats) recordFriendStartup(answer string, elapsed time.Duration) {
	s.friendOpportunityDone.Add(1)
	s.friendOpportunityLoaded.Add(1)
	switch answer {
	case nextOpportunityFound:
		s.opportunityFound.Add(1)
	case nextOpportunityNone:
		s.opportunityNone.Add(1)
	}
	s.latenciesMu.Lock()
	s.friendStartupLatency = append(s.friendStartupLatency, elapsed)
	s.latenciesMu.Unlock()
}

func (s *thickLoginStats) recordFriendStartupFailure() {
	s.friendOpportunityDone.Add(1)
	s.friendStartupErrors.Add(1)
}

func (s *thickLoginStats) recordFriendStartupRetries(retries int) {
	if retries > 0 {
		s.friendStartupRetries.Add(int64(retries))
	}
}

func (s *thickLoginStats) snapshot() *thickLoginReport {
	if s == nil {
		return nil
	}
	s.latenciesMu.Lock()
	bootstrap := append([]time.Duration(nil), s.bootstrapLatency...)
	friendStartup := append([]time.Duration(nil), s.friendStartupLatency...)
	s.latenciesMu.Unlock()
	report := &thickLoginReport{
		BootstrapErrors: s.bootstrapErrors.Load(), FriendStartupErrors: s.friendStartupErrors.Load(),
		FriendStartupRetries: s.friendStartupRetries.Load(),
		ProfilesLoaded:       s.profilesLoaded.Load(), TaskSummariesLoaded: s.taskSummariesLoaded.Load(),
		MailSummariesLoaded: s.mailSummariesLoaded.Load(), CompendiumLoaded: s.compendiumLoaded.Load(),
		PetSummariesLoaded: s.petSummariesLoaded.Load(), TaskClaimableTotal: s.taskClaimableTotal.Load(),
		DailyTaskRowsLoaded: s.dailyTaskRowsLoaded.Load(), MailRedPointPlayers: s.mailRedPointPlayers.Load(),
		FriendOpportunityLoaded: s.friendOpportunityLoaded.Load(),
		FriendOpportunityTried:  s.friendOpportunityTried.Load(), FriendOpportunityDone: s.friendOpportunityDone.Load(),
		OpportunityFound: s.opportunityFound.Load(), OpportunityNone: s.opportunityNone.Load(),
		BootstrapP50Ms: durationPercentileMs(bootstrap, .50), BootstrapP95Ms: durationPercentileMs(bootstrap, .95),
		FriendStartupP50Ms: durationPercentileMs(friendStartup, .50), FriendStartupP95Ms: durationPercentileMs(friendStartup, .95),
		FriendStartupP99Ms: durationPercentileMs(friendStartup, .99), FriendStartupMaxMs: durationPercentileMs(friendStartup, 1),
		FriendOpportunityP50Ms: durationPercentileMs(friendStartup, .50), FriendOpportunityP95Ms: durationPercentileMs(friendStartup, .95),
		FriendOpportunityP99Ms: durationPercentileMs(friendStartup, .99), FriendOpportunityMaxMs: durationPercentileMs(friendStartup, 1),
	}
	if report.FriendOpportunityTried > 0 {
		report.FriendOpportunityRate = float64(report.FriendOpportunityLoaded) / float64(report.FriendOpportunityTried)
	}
	return report
}

func durationPercentileMs(values []time.Duration, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return float64(sorted[int(float64(len(sorted)-1)*q)]) / float64(time.Millisecond)
}

func (s *stats) recordScenario(start time.Time) {
	s.scenarioReady.Add(1)
	s.latenciesMu.Lock()
	s.scenarioTime = append(s.scenarioTime, time.Since(start))
	s.latenciesMu.Unlock()
}

func (s *stats) observeActive(value int64) {
	for {
		previous := s.peakActive.Load()
		if value <= previous || s.peakActive.CompareAndSwap(previous, value) {
			return
		}
	}
}

func (s *stats) snapshot() report {
	result := report{
		Attempted: s.attempted.Load(), Ready: s.ready.Load(), ActiveAtEnd: s.active.Load(),
		PeakActive:  s.peakActive.Load(),
		LoginErrors: s.loginErr.Load(), ConnectErrors: s.connectErr.Load(),
		HelloErrors: s.helloErr.Load(), JoinErrors: s.joinErr.Load(),
		ScenarioTried: s.scenarioTried.Load(), ScenarioReady: s.scenarioReady.Load(),
		ScenarioError:   s.scenarioErr.Load(),
		UnexpectedEnd:   s.unexpectedEnd.Load(),
		SharedFarm:      *sharedFarm,
		HotOwnerClients: s.hotOwnerTried.Load(),
		HotPlantOK:      s.hotPlantOK.Load(), HotPlantErr: s.hotPlantErr.Load(),
		HotWaterOK: s.hotWaterOK.Load(), HotWaterErr: s.hotWaterErr.Load(),
		HotHarvestOK: s.hotHarvestOK.Load(), HotHarvestErr: s.hotHarvestErr.Load(),
		ReconnectTried: s.reconnectTried.Load(), ReconnectOK: s.reconnectOK.Load(),
		ReconnectErr: s.reconnectErr.Load(),
	}
	if result.Attempted > 0 {
		result.ReadyRate = float64(result.Ready) / float64(result.Attempted)
	}
	if elapsed := time.Since(s.start); elapsed > 0 {
		result.ElapsedMs = float64(elapsed) / float64(time.Millisecond)
		result.QPS = float64(result.Ready) / elapsed.Seconds()
	}
	startUnixNano := s.start.UnixNano()
	if lastAttempt := s.lastAttemptUnixNano.Load(); lastAttempt > startUnixNano {
		elapsed := time.Duration(lastAttempt - startUnixNano)
		result.LaunchElapsedMs = float64(elapsed) / float64(time.Millisecond)
		if elapsed > 0 {
			result.LaunchQPS = float64(result.Attempted) / elapsed.Seconds()
		}
	}
	if lastReady := s.lastReadyUnixNano.Load(); lastReady > startUnixNano {
		elapsed := time.Duration(lastReady - startUnixNano)
		result.ReadyCompletionElapsedMs = float64(elapsed) / float64(time.Millisecond)
		if elapsed > 0 {
			result.ReadyCompletionQPS = float64(result.Ready) / elapsed.Seconds()
		}
	}
	s.latenciesMu.Lock()
	ready := append([]time.Duration(nil), s.readyLatency...)
	logins := append([]time.Duration(nil), s.loginLatency...)
	hellos := append([]time.Duration(nil), s.helloLatency...)
	accepted := append([]time.Duration(nil), s.joinAccepted...)
	snapshots := append([]time.Duration(nil), s.snapshotWait...)
	scenarios := append([]time.Duration(nil), s.scenarioTime...)
	s.latenciesMu.Unlock()
	result.P50Ms = durationPercentileMs(ready, .50)
	result.P95Ms = durationPercentileMs(ready, .95)
	result.P99Ms = durationPercentileMs(ready, .99)
	if len(ready) > 0 {
		sorted := append([]time.Duration(nil), ready...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		result.MaxMs = float64(sorted[len(sorted)-1]) / float64(time.Millisecond)
	}
	result.LoginP50Ms = durationPercentileMs(logins, .50)
	result.LoginP95Ms = durationPercentileMs(logins, .95)
	result.HelloP50Ms = durationPercentileMs(hellos, .50)
	result.HelloP95Ms = durationPercentileMs(hellos, .95)
	result.JoinAcceptedP50 = durationPercentileMs(accepted, .50)
	result.JoinAcceptedP95 = durationPercentileMs(accepted, .95)
	result.SnapshotP50Ms = durationPercentileMs(snapshots, .50)
	result.SnapshotP95Ms = durationPercentileMs(snapshots, .95)
	result.ScenarioP95Ms = durationPercentileMs(scenarios, .95)
	s.reconnectMu.Lock()
	reconnects := append([]time.Duration(nil), s.reconnectLatency...)
	s.reconnectMu.Unlock()
	if len(reconnects) > 0 {
		sort.Slice(reconnects, func(i, j int) bool { return reconnects[i] < reconnects[j] })
		result.ReconnectP99Ms = float64(reconnects[int(float64(len(reconnects)-1)*.99)]) / float64(time.Millisecond)
	}
	s.failuresMu.Lock()
	if len(s.failures) > 0 {
		result.FailureReasons = make(map[string]int64, len(s.failures))
		for reason, count := range s.failures {
			result.FailureReasons[reason] = count
		}
	}
	s.failuresMu.Unlock()
	if s.theft != nil {
		result.Theft = s.theft.snapshot()
	}
	result.ThickLogin = s.thickLogin.snapshot()
	return result
}

func main() {
	flag.Parse()
	if *conns <= 0 || *duration <= 0 {
		log.Fatal("conns and duration must be positive")
	}
	if *rate < 0 {
		log.Fatal("rate must be >= 0 (0 = full speed, no throttle)")
	}
	if *friendOpportunityMaxAttempts <= 0 {
		log.Fatal("friend-opportunity-max-attempts must be > 0")
	}
	scenarioSet := map[string]bool{
		"login": true, "hello": true, "join-accepted": true, "join": true,
		"thick-login": true, "cold-thick-login": true, "plant": true, "hot": true, "reconnect": true, "theft-hot": true,
		"friend-list": true, "next-opportunity": true, "hint": true,
	}
	if !scenarioSet[*scenario] {
		log.Fatal("scenario must be login, hello, join-accepted, join, thick-login, cold-thick-login, plant, hot, reconnect, theft-hot, friend-list, next-opportunity or hint")
	}
	if (*scenario == "thick-login" || *scenario == "cold-thick-login") && *sharedFarm != 0 {
		log.Fatal("thick-login models H5 home startup and requires shared-farm=0")
	}
	if *scenario == "cold-thick-login" {
		if *coldPoolSize <= 0 {
			log.Fatal("cold-thick-login requires cold-pool-size > 0")
		}
		if *rate <= 0 {
			log.Fatal("cold-thick-login requires rate > 0 so the unique-ID launch rate is explicit")
		}
		if *coldLaunchDuration < 0 || *coldLaunchDuration > *duration {
			log.Fatal("cold-launch-duration must be between 0 and duration")
		}
	}
	if *scenario == "theft-hot" {
		if *conns != 200 || *sharedFarm == 0 || (!*theftPrepare && *theftCycle == "") ||
			(*theftPrepare && *theftCycle != "") || *theftChanges <= 0 || *theftTimeout <= 0 || *theftPoll <= 0 || *theftMaturity < 0 {
			log.Fatal("theft-hot requires conns=200, shared-farm, exactly one crop source (prepare or crop-cycle-id), and valid timing/change flags")
		}
		if *sharedFarm != *playerStart {
			log.Fatal("theft-hot requires shared-farm == player-start so the owner is one of the 200 real clients")
		}
	}
	runID := strings.TrimSpace(*runIDFlag)
	if friendScenarios[*scenario] {
		if err := validateFriendCapacityFlags(); err != nil {
			log.Fatal(err)
		}
		if runID == "" {
			log.Fatal("--run-id is required for friend-capacity C0 and must match the runner cell identity")
		}
	} else if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	accountAuth, err := productAccountAuthConfigured()
	if err != nil {
		log.Fatal(err)
	}
	secret := []byte(os.Getenv("LOAD_TEST_TOKEN_SECRET"))
	if len(secret) == 0 && (!friendScenarios[*scenario] || !accountAuth) {
		log.Fatal("LOAD_TEST_TOKEN_SECRET is required unless a friend-capacity scenario uses complete showcase account credentials")
	}
	authMode := "load_test_token"
	if accountAuth {
		authMode = "showcase_account"
	}
	startedAt := time.Now().UTC()
	log.Printf("ws-stress HTTP=%s WSS=%s conns=%d rate=%d/s duration=%s scenario=%s run_id=%s", *httpURL, *wsURL, *conns, *rate, *duration, *scenario, runID)
	if *scenario == "cold-thick-login" {
		log.Printf("cold ID pool start=%d size=%d launch_duration=%s", *playerStart, *coldPoolSize, *coldLaunchDuration)
	}
	gitSHA := currentGitSHA()
	if *environmentName == "" {
		*environmentName = "unspecified"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, *duration)
	defer timeoutCancel()
	s := &stats{start: time.Now()}
	if *scenario == "thick-login" || *scenario == "cold-thick-login" {
		s.thickLogin = &thickLoginStats{}
	}
	if friendScenarios[*scenario] {
		s.friend = newFriendCapacityStats()
	}
	if *scenario == "theft-hot" {
		s.theft = newTheftHotStats(*conns, *sharedFarm)
	}
	var wg sync.WaitGroup

	if *scenario == "cold-thick-login" {
		runColdThickLoginPool(ctx, runID, secret, s)
	} else if *rate <= 0 {
		// 全速无节流:立即并发启动全部 client,测后端真实极限 QPS。
		for launched := 0; launched < *conns; launched++ {
			playerID := *playerStart + uint64(launched)
			s.recordAttempt()
			wg.Add(1)
			go func() {
				defer wg.Done()
				runPlayer(ctx, playerID, runID, secret, s)
			}()
		}
	} else {
		interval := time.Second / time.Duration(*rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for launched := 0; launched < *conns; {
			select {
			case <-ctx.Done():
				launched = *conns
			case <-ticker.C:
				playerID := *playerStart + uint64(launched)
				launched++
				s.recordAttempt()
				wg.Add(1)
				go func() {
					defer wg.Done()
					runPlayer(ctx, playerID, runID, secret, s)
				}()
			}
		}
	}
	// 等全部 client 完成,或 duration 超时(全速模式通常提前完成)。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	wg.Wait()
	result := s.snapshot()
	result.RunID = runID
	result.CellRunID = runID
	result.AuthMode = authMode
	result.GitSHA = gitSHA
	result.Environment = *environmentName
	if s.friend != nil {
		result.Friend = s.friend.snapshot()
		// Redis command/byte totals are never claimed by the client process.
		// The C0 runner merges before/after /metrics scrapes into evidence; a
		// missing scrape is measurement_unavailable, never a silent zero.
		result.EstimatedNote = "redis_commands/estimated_bytes are client_unmeasured here; C0 runner merges service /metrics deltas or marks measurement_unavailable"
	}
	if *jsonMetaOut != "" {
		meta := map[string]string{
			"run_id": runID, "git_sha": gitSHA,
			"environment": *environmentName, "scenario": *scenario, "auth_mode": authMode,
		}
		if err := os.WriteFile(*jsonMetaOut, mustJSON(meta), 0o644); err != nil {
			log.Fatalf("write meta: %v", err)
		}
	}
	result.Scenario = *scenario
	result.ConfiguredConns = *conns
	result.PlayerStart = *playerStart
	result.ConnectRate = *rate
	result.DurationSeconds = duration.Seconds()
	result.StartedAt = startedAt.Format(time.RFC3339Nano)
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	data, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(data))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, append(data, '\n'), 0o644); err != nil {
			log.Fatalf("write result: %v", err)
		}
	}
}

// gateConfig binds the tool's flags to the shared gate client.
func gateConfig(runID string, secret []byte) gateclient.Config {
	return gateclient.Config{
		HTTPBaseURL:     *httpURL,
		WSURL:           *wsURL,
		TLSCAPath:       *tlsCA,
		LoadSecret:      secret,
		HTTPClient:      loadHTTPClient,
		ClientBuild:     "ws-stress/1",
		BootstrapPrefix: "ws-stress",
		RunID:           runID,
	}
}

func websocketDialer() (websocket.Dialer, error) {
	return gateConfig("", nil).Dialer()
}

type loginResult = gateclient.LoginResult

// runColdThickLoginPool consumes a disjoint, never-reused player ID range.
// Each worker performs one real H5 home login and closes the connection after
// the independent opportunity leg is accounted for; it never reconnects the
// same identity. The launch window can end before the outer duration so
// in-flight cold attempts get a bounded drain period.
func runColdThickLoginPool(ctx context.Context, runID string, secret []byte, s *stats) {
	workers := *conns
	ids := make(chan uint64, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for playerID := range ids {
				s.recordAttempt()
				runPlayer(ctx, playerID, runID, secret, s)
			}
		}()
	}

	launchCtx := ctx
	cancel := func() {}
	if *coldLaunchDuration > 0 {
		launchCtx, cancel = context.WithTimeout(ctx, *coldLaunchDuration)
	}
	defer cancel()

	var ticker *time.Ticker
	if *rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
	}
	for offset := 0; offset < *coldPoolSize; offset++ {
		if offset > 0 && ticker != nil {
			select {
			case <-ticker.C:
			case <-launchCtx.Done():
				close(ids)
				wg.Wait()
				return
			}
		}
		playerID := *playerStart + uint64(offset)
		select {
		case ids <- playerID:
		case <-launchCtx.Done():
			close(ids)
			wg.Wait()
			return
		}
	}
	close(ids)
	wg.Wait()
}

func runPlayer(ctx context.Context, playerID uint64, runID string, secret []byte, s *stats) {
	if *scenario == "theft-hot" {
		runTheftHotPlayer(ctx, playerID, runID, secret, s)
		return
	}
	if friendScenarios[*scenario] {
		// Count the attempt before Login so a login failure still produces
		// exactly one terminal scenario outcome (never zero outcomes).
		s.scenarioTried.Add(1)
		scenarioStart := time.Now()
		login, err := acquireTicket(ctx, playerID, secret, runID)
		if err != nil {
			s.loginErr.Add(1)
			s.scenarioErr.Add(1)
			s.recordFailure("login", err)
			if s.friend != nil {
				s.friend.recordFailure(*scenario, err)
			}
			return
		}
		// Readiness ends at login + WSS ServerHello with the exact HTTPS login
		// identity, before the friend workload starts. Scenario latency/outcome
		// is recorded separately from readiness.
		readyStart := time.Now()
		if err := runFriendCapacityReadyPath(ctx, playerID, login, s, readyStart); err != nil {
			s.scenarioErr.Add(1)
			s.friend.recordFailure(*scenario, err)
			return
		}
		runFriendCapacityScenario(ctx, playerID, login, runID, secret, s, scenarioStart)
		return
	}
	start := time.Now()
	login, err := acquireTicket(ctx, playerID, secret, runID)
	if err != nil {
		s.loginErr.Add(1)
		s.recordFailure("login", err)
		return
	}
	s.recordStage(&s.loginLatency, time.Since(start))
	if *scenario == "login" {
		s.recordReady(start)
		return
	}
	dialer, err := websocketDialer()
	if err != nil {
		s.connectErr.Add(1)
		s.recordFailure("connect_config", err)
		return
	}
	helloStart := time.Now()
	conn, response, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		s.connectErr.Add(1)
		s.recordFailure("connect", err)
		return
	}
	defer conn.Close()
	currentActive := s.active.Add(1)
	s.observeActive(currentActive)
	defer s.active.Add(-1)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      "ws-stress/1",
	}); err != nil {
		s.helloErr.Add(1)
		s.recordFailure("hello_write", err)
		return
	}
	var helloErr error
	if *scenario == "thick-login" || *scenario == "cold-thick-login" {
		_, helloErr = readServerHelloForPlayer(conn, playerID, login.SessionID)
	} else {
		helloErr = readUntil(conn, frameKindServerHello)
	}
	if helloErr != nil {
		s.helloErr.Add(1)
		s.recordFailure("hello_read", helloErr)
		return
	}
	s.recordStage(&s.helloLatency, time.Since(helloStart))
	if *scenario == "hello" {
		s.recordReady(start)
		return
	}
	if *scenario == "thick-login" || *scenario == "cold-thick-login" {
		loginElapsed := time.Since(start)
		friendResult, err := runThickLoginReady(ctx, conn, playerID, runID, login, loginElapsed, s)
		if err != nil {
			collectThickFriendStartup(friendResult, s)
			s.recordFailure("thick_login", err)
			return
		}
		// Login readiness deliberately excludes the opportunity popup. The
		// request began in parallel with Bootstrap/Join, but it has its own
		// completion/error/latency contract below.
		s.recordReady(start)
		if *scenario == "cold-thick-login" {
			// Ready is the cold-login measurement boundary. The optional
			// opportunity request must not occupy the next-new-identity worker;
			// keep accounting it asynchronously while closing this one-shot
			// session immediately.
			go collectThickFriendStartup(friendResult, s)
			return
		}
		collectThickFriendStartup(friendResult, s)
		holdConnection(ctx, conn, s)
		return
	}
	targetOwner := playerID
	if *sharedFarm > 0 {
		targetOwner = *sharedFarm
	}
	joinOperation := fmt.Sprintf("load-join-%s-%d", runID, playerID)
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		s.joinErr.Add(1)
		s.recordFailure("join_encode", err)
		return
	}
	joinStart := time.Now()
	if err := writeFrame(conn, 2, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: joinOperation,
		RequestId:   "request-" + joinOperation,
		CommandType: commonpb.CommandType_JOIN_FARM,
		Payload:     joinPayload,
	}); err != nil {
		s.joinErr.Add(1)
		s.recordFailure("join_write", err)
		return
	}
	if *scenario == "join-accepted" {
		if err := readUntilJoinAccepted(conn, joinOperation, targetOwner); err != nil {
			s.joinErr.Add(1)
			s.recordFailure("join_accepted", err)
			return
		}
		s.recordStage(&s.joinAccepted, time.Since(joinStart))
		s.recordReady(start)
		return
	}
	// 恢复标记只在自家农场 Join 后发送(与 lease-stress/H5 同纪律):
	// 围观共享农场时发送会触发 ErrRecoveryIncomplete 关连接。
	var acceptedAt time.Time
	snapshot, err := readJoinBarrier(
		conn, 3, joinOperation, targetOwner, targetOwner == playerID,
		func() { acceptedAt = time.Now() },
	)
	if err != nil {
		s.joinErr.Add(1)
		s.recordFailure("join_barrier", err)
		return
	}
	if !acceptedAt.IsZero() {
		s.recordStage(&s.joinAccepted, acceptedAt.Sub(joinStart))
		s.recordStage(&s.snapshotWait, time.Since(acceptedAt))
	}
	s.recordReady(start)
	switch *scenario {
	case "plant":
		s.scenarioTried.Add(1)
		scenarioStart := time.Now()
		if _, err := runPlantScenario(conn, playerID, runID, snapshot); err != nil {
			s.scenarioErr.Add(1)
			s.recordFailure("plant", err)
			return
		}
		s.recordScenario(scenarioStart)
	case "hot":
		s.scenarioTried.Add(1)
		scenarioStart := time.Now()
		if err := runHotScenario(conn, playerID, targetOwner, runID, snapshot, s); err != nil {
			s.scenarioErr.Add(1)
			s.recordFailure("hot", err)
			return
		}
		s.recordScenario(scenarioStart)
	case "reconnect":
		runReconnectLoop(ctx, playerID, runID, secret, s)
	}
	holdConnection(ctx, conn, s)
}

func holdConnection(ctx context.Context, conn *websocket.Conn, s *stats) {
	_ = conn.SetReadDeadline(time.Time{})
	readDone := make(chan error, 1)
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				readDone <- err
				return
			}
		}
	}()
	select {
	case <-ctx.Done():
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "load test complete"),
			time.Now().Add(time.Second))
	case err := <-readDone:
		if ctx.Err() == nil && !isExpectedRestart(err) {
			s.unexpectedEnd.Add(1)
		}
	}
}

type thickFriendStartupResult struct {
	answer  string
	elapsed time.Duration
	retries int
	skipped bool
	err     error
}

// runThickLoginReady mirrors the H5 home bootstrap after ServerHello:
// GET_PLAYER_BOOTSTRAP (unless the login response already attached a valid
// home_bootstrap), own-farm Join and the HTTP opportunity popup start as
// independent legs. Login readiness opens after only the private projection
// and Join snapshot/subscription converge. The opportunity result is returned
// separately so callers can account for it without putting it on the Ready
// latency critical path.
func runThickLoginReady(
	ctx context.Context,
	conn *websocket.Conn,
	playerID uint64,
	runID string,
	login loginResult,
	loginElapsed time.Duration,
	s *stats,
) (<-chan thickFriendStartupResult, error) {
	if s.thickLogin == nil {
		return nil, errors.New("thick-login stats are not initialized")
	}
	// 登录响应已附带合法首页投影时,不再发送 GET_PLAYER_BOOTSTRAP:首页数据在
	// HTTP 登录阶段已经拿到,WS 上只保留 JOIN_FARM 一条关键命令,避免 Gate 单
	// 连接 worker 串行执行 Bootstrap 再 Join。旧响应/投影校验失败才回退旧命令。
	bootstrapFromLogin := false
	if view := login.HomeBootstrap; view != nil && validateThickBootstrap(view, playerID) == nil {
		bootstrapFromLogin = true
		s.thickLogin.recordBootstrap(view, loginElapsed)
	}
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: playerID})
	if err != nil {
		s.joinErr.Add(1)
		return nil, fmt.Errorf("join encode: %w", err)
	}

	bootstrapOperation := fmt.Sprintf("load-bootstrap-%s-%d", runID, playerID)
	joinOperation := fmt.Sprintf("load-join-%s-%d", runID, playerID)
	var bootstrapStart time.Time
	nextSeq := uint64(2)
	if !bootstrapFromLogin {
		bootstrapPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.GetPlayerBootstrapRequest{})
		if err != nil {
			s.thickLogin.bootstrapErrors.Add(1)
			return nil, fmt.Errorf("bootstrap encode: %w", err)
		}
		bootstrapStart = time.Now()
		if err := writeFrame(conn, 2, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
			OperationId: bootstrapOperation, RequestId: "request-" + bootstrapOperation,
			CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP, Payload: bootstrapPayload,
		}); err != nil {
			s.thickLogin.bootstrapErrors.Add(1)
			return nil, fmt.Errorf("bootstrap write: %w", err)
		}
		nextSeq = 3
	}
	joinStart := time.Now()
	if err := writeFrame(conn, nextSeq, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: joinOperation, RequestId: "request-" + joinOperation,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinPayload,
	}); err != nil {
		s.joinErr.Add(1)
		return nil, fmt.Errorf("join write: %w", err)
	}

	friendResult := make(chan thickFriendStartupResult, 1)

	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	barrier, stage, err := readThickLoginBarrier(conn, bootstrapOperation, joinOperation, playerID, bootstrapFromLogin, login.HomeBootstrap)
	if err != nil {
		if stage == "bootstrap" {
			s.thickLogin.bootstrapErrors.Add(1)
		} else {
			s.joinErr.Add(1)
		}
		friendResult <- thickFriendStartupResult{answer: nextOpportunityUnknown, err: err}
		return friendResult, fmt.Errorf("%s: %w", stage, err)
	}
	if err := writeFrame(conn, nextSeq+1, frameKindControl, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
	}); err != nil {
		s.joinErr.Add(1)
		friendResult <- thickFriendStartupResult{answer: nextOpportunityUnknown, err: err}
		return friendResult, fmt.Errorf("recovery marker: %w", err)
	}
	if *skipFriendOpportunity {
		friendResult <- thickFriendStartupResult{skipped: true}
		return friendResult, nil
	}
	s.thickLogin.friendOpportunityTried.Add(1)

	// 机会弹窗在 Ready 之后异步排空:不阻塞登录 Ready,遵循 Retry-After 并做
	// 有上限的指数退避;429/503 只产生重试,绝不伪造 NONE。
	go func() {
		friendResult <- loadThickFriendStartup(ctx, login.Cookie)
	}()

	if !bootstrapFromLogin {
		s.thickLogin.recordBootstrap(barrier.bootstrap, barrier.bootstrapAt.Sub(bootstrapStart))
	}
	if !barrier.acceptedAt.IsZero() {
		s.recordStage(&s.joinAccepted, barrier.acceptedAt.Sub(joinStart))
		s.recordStage(&s.snapshotWait, barrier.readyAt.Sub(barrier.acceptedAt))
	}
	return friendResult, nil
}

// collectThickFriendStartup accounts for the independently timed opportunity
// popup. Its outcome never changes login Ready, but every attempted request
// still has exactly one terminal success/error observation before the client
// goroutine exits.
func collectThickFriendStartup(resultCh <-chan thickFriendStartupResult, s *stats) {
	if resultCh == nil || s == nil || s.thickLogin == nil {
		return
	}
	result := <-resultCh
	if result.skipped {
		return
	}
	s.thickLogin.recordFriendStartupRetries(result.retries)
	if result.err != nil {
		s.thickLogin.recordFriendStartupFailure()
		s.recordFailure("friend_opportunity", result.err)
		return
	}
	s.thickLogin.recordFriendStartup(result.answer, result.elapsed)
}

// loadThickFriendStartup follows the H5 home popup path: ask the server-owned
// navigator for the next actionable opportunity. FOUND and NONE are valid
// authoritative states; the full roster page is intentionally not loaded by
// thick-login and remains covered by the friend-list scenario. Retryable HTTP
// outcomes (429 with Retry-After, 503, network errors) are retried with bounded
// exponential backoff and jitter; a terminal error is never coerced into NONE.
func loadThickFriendStartup(ctx context.Context, cookie string) thickFriendStartupResult {
	started := time.Now()
	client := httpClientFor(cookie)
	base := strings.TrimRight(*httpURL, "/")
	maxAttempts := *friendOpportunityMaxAttempts
	const maxBackoff = 5 * time.Second
	backoff := 500 * time.Millisecond
	retries := 0
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			retries = attempt
			select {
			case <-ctx.Done():
				return thickFriendStartupResult{answer: nextOpportunityUnknown, elapsed: time.Since(started), retries: retries, err: ctx.Err()}
			case <-time.After(jitterDuration(backoff, 0.2)):
			}
		}
		status, answer, retryAfter, err := nextOpportunityRequest(ctx, client, base)
		if err == nil && status == http.StatusOK && (answer == nextOpportunityFound || answer == nextOpportunityNone) {
			return thickFriendStartupResult{answer: answer, elapsed: time.Since(started), retries: retries}
		}
		if ctx.Err() != nil {
			return thickFriendStartupResult{answer: nextOpportunityUnknown, elapsed: time.Since(started), retries: retries, err: ctx.Err()}
		}
		if attempt == maxAttempts-1 {
			if err == nil {
				err = fmt.Errorf("friend opportunity status=%d answer=%s", status, answer)
			}
			return thickFriendStartupResult{answer: answer, elapsed: time.Since(started), retries: retries, err: err}
		}
		// 429 时优先遵循服务器 Retry-After;503/网络错误走指数退避。
		if retryAfter > 0 && retryAfter < maxBackoff {
			backoff = retryAfter
		} else {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
	return thickFriendStartupResult{answer: nextOpportunityUnknown, elapsed: time.Since(started), retries: retries, err: errors.New("friend opportunity retries exhausted")}
}

// jitterDuration adds uniform ±jitter fraction around d (bounded below at 1ms).
func jitterDuration(d time.Duration, jitter float64) time.Duration {
	if d <= 0 {
		return time.Millisecond
	}
	spread := int64(float64(d) * jitter)
	if spread <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(2*spread+1)-spread)
}

type thickLoginBarrier struct {
	join               joinBarrier
	bootstrapOperation string
	playerID           uint64
	bootstrap          *publicpb.GetPlayerBootstrapResult
	bootstrapFromLogin bool
	bootstrapAt        time.Time
	acceptedAt         time.Time
	readyAt            time.Time
}

func (b *thickLoginBarrier) observeResult(result *commonpb.CommandResultEnvelope) (string, error) {
	if result.GetOperationId() == b.bootstrapOperation {
		if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
			return "", nil
		}
		if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			return "bootstrap", fmt.Errorf("status=%s code=%s message=%s", result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
		}
		if result.GetCommandType() != commonpb.CommandType_GET_PLAYER_BOOTSTRAP {
			return "bootstrap", fmt.Errorf("command_type=%s", result.GetCommandType())
		}
		var bootstrap publicpb.GetPlayerBootstrapResult
		if err := proto.Unmarshal(result.GetPayload(), &bootstrap); err != nil {
			return "bootstrap", fmt.Errorf("decode result: %w", err)
		}
		if err := validateThickBootstrap(&bootstrap, b.playerID); err != nil {
			return "bootstrap", err
		}
		b.bootstrap = &bootstrap
		b.bootstrapAt = time.Now()
		return "", nil
	}
	wasAccepted := b.join.acceptedSubscription != ""
	if err := b.join.observeResult(result); err != nil {
		return "join", err
	}
	if !wasAccepted && b.join.acceptedSubscription != "" {
		b.acceptedAt = time.Now()
	}
	return "", nil
}

func (b *thickLoginBarrier) observePush(push *commonpb.ServerPushEnvelope) error {
	return b.join.observePush(push)
}

func (b *thickLoginBarrier) result() (*farmpb.FarmSnapshot, bool, error) {
	snapshot, joinReady, err := b.join.result()
	if err != nil || !joinReady || (b.bootstrap == nil && !b.bootstrapFromLogin) {
		return snapshot, false, err
	}
	if b.readyAt.IsZero() {
		b.readyAt = time.Now()
	}
	return snapshot, true, nil
}

func readThickLoginBarrier(
	conn *websocket.Conn,
	bootstrapOperation string,
	joinOperation string,
	playerID uint64,
	bootstrapFromLogin bool,
	attached *publicpb.GetPlayerBootstrapResult,
) (*thickLoginBarrier, string, error) {
	barrier := &thickLoginBarrier{
		bootstrapOperation: bootstrapOperation,
		playerID:           playerID,
		join:               joinBarrier{operationID: joinOperation, targetOwner: playerID},
		bootstrapFromLogin: bootstrapFromLogin,
		bootstrap:          attached,
	}
	for {
		kind, body, err := readFrame(conn)
		if err != nil {
			return nil, "websocket", err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, "websocket", err
			}
			if stage, err := barrier.observeResult(&result); err != nil {
				return nil, stage, err
			}
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				return nil, "join", err
			}
			if err := barrier.observePush(&push); err != nil {
				return nil, "join", err
			}
		case frameKindConnectionError:
			return nil, "websocket", errors.New("server returned ConnectionError during thick login")
		}
		if _, ready, err := barrier.result(); err != nil {
			return nil, "join", err
		} else if ready {
			return barrier, "", nil
		}
	}
}

func validateThickBootstrap(result *publicpb.GetPlayerBootstrapResult, playerID uint64) error {
	if result == nil || result.GetPlayerStateRevision() == 0 {
		return errors.New("player_state_revision is missing")
	}
	profile := result.GetProfile()
	if profile == nil || profile.GetPlayerId() != playerID || strings.TrimSpace(profile.GetDisplayName()) == "" {
		return fmt.Errorf("profile identity invalid: player_id=%d display_name=%q", profile.GetPlayerId(), profile.GetDisplayName())
	}
	if result.GetProgression() == nil || result.GetWallet() == nil || result.GetInventory() == nil {
		return errors.New("progression/wallet/inventory projection is incomplete")
	}
	features := result.GetFeatureSummary()
	if features == nil || features.GetTasks() == nil {
		return errors.New("task red-point summary is missing")
	}
	tasks := features.GetTasks()
	if tasks.GetTaskStateVersion() == 0 || strings.TrimSpace(tasks.GetDailyPeriodId()) == "" || len(tasks.GetDailyTasks()) == 0 {
		return errors.New("task red-point summary is incomplete")
	}
	if features.GetMail() == nil {
		return errors.New("mail red-point summary is missing")
	}
	if features.GetCompendium() == nil {
		return errors.New("compendium red-point summary is missing")
	}
	if features.GetPet() == nil {
		return errors.New("pet red-point summary is missing")
	}
	if result.GetTrustedServerNowMs() <= 0 {
		return errors.New("trusted_server_now_ms is missing")
	}
	return nil
}

func runPlantScenario(conn *websocket.Conn, playerID uint64, runID string, snapshot *farmpb.FarmSnapshot) (uint32, error) {
	var plotIndex uint32
	found := false
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() == "" {
			plotIndex, found = plot.GetPlotIndex(), true
			break
		}
	}
	if !found {
		return 0, errors.New("plant scenario requires an empty plot")
	}
	buyPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{
		OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return 0, err
	}
	buyOperation := fmt.Sprintf("load-buy-%s-%d", runID, playerID)
	if err := writeFrame(conn, 4, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: buyOperation, RequestId: "request-" + buyOperation,
		CommandType: commonpb.CommandType_BUY,
		Payload:     buyPayload,
	}); err != nil {
		return 0, err
	}
	if err := readSuccessfulResult(conn, buyOperation); err != nil {
		return 0, fmt.Errorf("buy seed: %w", err)
	}

	// v4 Typed Payload(28 §5.2):目标农场只来自 PlantRequest.target_farm_id;
	// expected_farm_version 不再有客户端来源(Subscription Fence 承接)。
	plantPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: plotIndex, CropDefinitionId: 1, TargetFarmId: playerID,
	})
	if err != nil {
		return 0, err
	}
	plantOperation := fmt.Sprintf("load-plant-%s-%d", runID, playerID)
	if err := writeFrame(conn, 5, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: plantOperation, RequestId: "request-" + plantOperation,
		CommandType: commonpb.CommandType_PLANT,
		Payload:     plantPayload,
	}); err != nil {
		return 0, err
	}
	if err := readSuccessfulResult(conn, plantOperation); err != nil {
		return 0, fmt.Errorf("plant crop: %w", err)
	}
	return plotIndex, nil
}

func acquireTicket(ctx context.Context, playerID uint64, secret []byte, runID string) (loginResult, error) {
	credential, accountAuth, err := productAccountCredential(playerID)
	if err != nil {
		return loginResult{}, err
	}
	var account *gateclient.AccountCredential
	if accountAuth {
		account = &gateclient.AccountCredential{
			Username: credential.username,
			Password: credential.password,
		}
	}
	return gateConfig(runID, secret).Login(ctx, playerID, account)
}

type accountCredential struct {
	username string
	password string
}

func productAccountAuthConfigured() (bool, error) {
	_, hasViewer, err := accountCredentialFromEnv(viewerAccountUsernameEnv, viewerAccountPasswordEnv)
	if err != nil {
		return false, err
	}
	_, hasTarget, err := accountCredentialFromEnv(hintTargetAccountUsernameEnv, hintTargetAccountPasswordEnv)
	if err != nil {
		return false, err
	}
	return hasViewer || hasTarget, nil
}

func productAccountCredential(playerID uint64) (accountCredential, bool, error) {
	viewer, hasViewer, err := accountCredentialFromEnv(viewerAccountUsernameEnv, viewerAccountPasswordEnv)
	if err != nil {
		return accountCredential{}, false, err
	}
	target, hasTarget, err := accountCredentialFromEnv(
		hintTargetAccountUsernameEnv,
		hintTargetAccountPasswordEnv,
	)
	if err != nil {
		return accountCredential{}, false, err
	}
	switch {
	case hasViewer && playerID == *friendViewer:
		return viewer, true, nil
	case hasTarget && playerID == *hintTargetOwner:
		return target, true, nil
	case hasViewer || hasTarget:
		return accountCredential{}, false, fmt.Errorf(
			"showcase account credentials are not configured for player %d",
			playerID,
		)
	default:
		return accountCredential{}, false, nil
	}
}

func accountCredentialFromEnv(usernameEnv, passwordEnv string) (accountCredential, bool, error) {
	credential, configured, err := gateclient.AccountCredentialFromEnv(usernameEnv, passwordEnv)
	if err != nil || !configured {
		return accountCredential{}, false, err
	}
	return accountCredential{
		username: credential.Username,
		password: credential.Password,
	}, true, nil
}

// frameKind 是本工具对 FarmFrame 顶层 oneof 大类的本地枚举(28 §2:大类用
// oneof 区分,业务命令不走 oneof)。
type frameKind = gateclient.FrameKind

const (
	frameKindClientHello     = gateclient.FrameKindClientHello
	frameKindServerHello     = gateclient.FrameKindServerHello
	frameKindClientCommand   = gateclient.FrameKindClientCommand
	frameKindCommandResult   = gateclient.FrameKindCommandResult
	frameKindServerPush      = gateclient.FrameKindServerPush
	frameKindConnectionError = gateclient.FrameKindConnectionError
	frameKindControl         = gateclient.FrameKindControl
)

// writeFrame 以 v4 线格式(protocol_version=4、uint64 sequence、frame_body
// oneof)编码一条客户端帧。
func writeFrame(conn *websocket.Conn, sequence uint64, kind frameKind, body proto.Message) error {
	return gateclient.WriteFrame(conn, sequence, kind, body)
}

// readFrame 读取下一个二进制帧并按顶层 oneof 解出大类;服务端作用域
// Control(ConnectionError)映射为本地 ConnectionError 大类。
func readFrame(conn *websocket.Conn) (frameKind, []byte, error) {
	return gateclient.ReadFrame(conn)
}

func readUntil(conn *websocket.Conn, kind frameKind) error {
	return gateclient.ReadUntil(conn, kind)
}

// readServerHelloForPlayer decodes ServerHello and requires player_id and
// session_id to match the exact HTTPS login identity (wantPlayerID +
// wantSessionID). Every WSS path that feeds friend-capacity evidence uses this
// helper so a mismatched or borrowed session can never count as readiness.
func readServerHelloForPlayer(conn *websocket.Conn, wantPlayerID uint64, wantSessionID string) (*commonpb.ServerHello, error) {
	return gateclient.ReadServerHelloForPlayer(conn, wantPlayerID, wantSessionID)
}

// validateServerHelloIdentity requires the exact login player and login session
// identity. A ServerHello that only shares the player id (e.g. a stale session
// from a previous login) is rejected: the friend workload evidence must be
// bound to the exact HTTPS login session that issued the farm_session cookie.
func validateServerHelloIdentity(hello *commonpb.ServerHello, wantPlayerID uint64, wantSessionID string) error {
	return gateclient.ValidateServerHelloIdentity(hello, wantPlayerID, wantSessionID)
}

// readTerminalSucceededResult waits for a terminal SUCCEEDED result for the
// operation and returns the highest FarmChange farm_version observed before
// the terminal result. PROCESSING is intermediate and continues the wait;
// NO_CHANGE and every other non-SUCCEEDED status fail closed. The returned
// (0, false) marks a terminal SUCCEEDED with no observed FarmChange version:
// callers must treat that as causal-unavailable, never fabricate a version.
func readTerminalSucceededResult(conn *websocket.Conn, operationID string) (observedFarmVersion uint64, observed bool, err error) {
	var latestFarmVersion uint64
	observedChange := false
	for {
		kind, body, err := readFrame(conn)
		if err != nil {
			return 0, false, err
		}
		if kind == frameKindConnectionError {
			return 0, false, errors.New("server returned ConnectionError")
		}
		if kind == frameKindServerPush {
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				continue
			}
			if push.GetPushType() != commonpb.PushType_FARM_CHANGE {
				continue
			}
			var changePush farmpb.FarmChangePush
			if err := proto.Unmarshal(push.GetPayload(), &changePush); err != nil {
				continue
			}
			if version := changePush.GetChange().GetFarmVersion(); version > 0 {
				observedChange = true
				if version > latestFarmVersion {
					latestFarmVersion = version
				}
			}
			continue
		}
		if kind != frameKindCommandResult {
			continue
		}
		var result commonpb.CommandResultEnvelope
		if err := proto.Unmarshal(body, &result); err != nil {
			return 0, false, err
		}
		if result.OperationId != operationID {
			continue
		}
		switch result.Status {
		case commonpb.CommandStatus_PROCESSING:
			continue
		case commonpb.CommandStatus_SUCCEEDED:
			return latestFarmVersion, observedChange, nil
		case commonpb.CommandStatus_NO_CHANGE:
			return 0, false, fmt.Errorf("operation %s returned NO_CHANGE; terminal SUCCEEDED is required", operationID)
		default:
			return 0, false, fmt.Errorf("status=%s code=%s message=%s", result.Status, result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
		}
	}
}

// readUntilSnapshot 实现 v4 Join 屏障(与 H5/dualgate-e2e 一致):
// ACCEPTED(JoinResultContext) → FARM_SNAPSHOT(FarmSnapshotPush) →
// SUBSCRIPTION_READY;sendMarker 时随后发送 ClientRecoveryComplete。
// seq 为屏障后续帧(恢复标记)使用的连接序号。
func readUntilSnapshot(conn *websocket.Conn, seq uint64, operationID string, targetOwner uint64, sendMarker bool) (*farmpb.FarmSnapshot, error) {
	return readJoinBarrier(conn, seq, operationID, targetOwner, sendMarker, nil)
}

func readUntilJoinAccepted(conn *websocket.Conn, operationID string, targetOwner uint64) error {
	barrier := joinBarrier{operationID: operationID, targetOwner: targetOwner}
	for {
		kind, body, err := readFrame(conn)
		if err != nil {
			return err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return err
			}
			if err := barrier.observeResult(&result); err != nil {
				return err
			}
			if barrier.acceptedSubscription != "" {
				return nil
			}
		case frameKindConnectionError:
			return errors.New("server returned ConnectionError during join")
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				return err
			}
			if err := barrier.observePush(&push); err != nil {
				return err
			}
		}
	}
}

func readJoinBarrier(
	conn *websocket.Conn,
	seq uint64,
	operationID string,
	targetOwner uint64,
	sendMarker bool,
	onAccepted func(),
) (*farmpb.FarmSnapshot, error) {
	barrier := joinBarrier{operationID: operationID, targetOwner: targetOwner}
	acceptedOnce := onAccepted
	for {
		kind, body, err := readFrame(conn)
		if err != nil {
			return nil, err
		}
		switch kind {
		case frameKindCommandResult:
			var result commonpb.CommandResultEnvelope
			if err := proto.Unmarshal(body, &result); err != nil {
				return nil, err
			}
			if err := barrier.observeResult(&result); err != nil {
				return nil, err
			}
			if acceptedOnce != nil && barrier.acceptedSubscription != "" {
				acceptedOnce()
				acceptedOnce = nil
			}
		case frameKindConnectionError:
			return nil, errors.New("server returned ConnectionError during join")
		case frameKindServerPush:
			var push commonpb.ServerPushEnvelope
			if err := proto.Unmarshal(body, &push); err != nil {
				return nil, err
			}
			if err := barrier.observePush(&push); err != nil {
				return nil, err
			}
		}
		if snapshot, ready, err := barrier.result(); err != nil {
			return nil, err
		} else if ready {
			if !sendMarker {
				return snapshot, nil
			}
			if err := writeFrame(conn, seq, frameKindControl, &commonpb.ControlFrame{
				Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
			}); err != nil {
				return nil, err
			}
			return snapshot, nil
		}
	}
}

type joinBarrier struct {
	operationID          string
	targetOwner          uint64
	acceptedSubscription string
	snapshotSubscription string
	snapshot             *farmpb.FarmSnapshot
	ready                bool
}

func (b *joinBarrier) observeResult(result *commonpb.CommandResultEnvelope) error {
	if result.GetOperationId() != b.operationID {
		return nil
	}
	if result.GetStatus() != commonpb.CommandStatus_ACCEPTED {
		return fmt.Errorf("join rejected: status=%s code=%s message=%s", result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
	}
	// 28 §2.2:Join 元信息的唯一来源是 JoinResultContext,result_payload 为空。
	join := result.GetJoin()
	if join == nil || join.GetTargetFarmId() != b.targetOwner || join.GetSubscriptionId() == "" {
		return fmt.Errorf("join accepted context invalid: %v", join)
	}
	b.acceptedSubscription = join.GetSubscriptionId()
	return nil
}

func (b *joinBarrier) observePush(push *commonpb.ServerPushEnvelope) error {
	switch push.GetPushType() {
	case commonpb.PushType_FARM_SNAPSHOT:
		b.snapshotSubscription = push.GetFarmSubscription().GetSubscriptionId()
		if b.snapshotSubscription == "" {
			return errors.New("join snapshot is missing subscription_id")
		}
		var snapshotPush farmpb.FarmSnapshotPush
		if err := proto.Unmarshal(push.GetPayload(), &snapshotPush); err != nil {
			return err
		}
		if snapshotPush.GetSnapshot().GetOwnerId() != b.targetOwner {
			return fmt.Errorf("join snapshot owner=%d, want %d", snapshotPush.GetSnapshot().GetOwnerId(), b.targetOwner)
		}
		b.snapshot = snapshotPush.GetSnapshot()
	case commonpb.PushType_SUBSCRIPTION_READY:
		if b.snapshot == nil {
			return errors.New("SUBSCRIPTION_READY arrived before snapshot")
		}
		b.ready = true
	case commonpb.PushType_REJOIN_REQUIRED:
		return errors.New("REJOIN_REQUIRED during join barrier")
	}
	return nil
}

func (b *joinBarrier) result() (*farmpb.FarmSnapshot, bool, error) {
	if b.acceptedSubscription == "" || b.snapshot == nil || !b.ready {
		return nil, false, nil
	}
	if b.acceptedSubscription != b.snapshotSubscription {
		return nil, false, fmt.Errorf("join subscription mismatch: result=%q snapshot=%q", b.acceptedSubscription, b.snapshotSubscription)
	}
	return b.snapshot, true, nil
}

func readSuccessfulResult(conn *websocket.Conn, operationID string) error {
	for {
		kind, body, err := readFrame(conn)
		if err != nil {
			return err
		}
		if kind == frameKindConnectionError {
			return errors.New("server returned ConnectionError")
		}
		if kind != frameKindCommandResult {
			continue
		}
		var result commonpb.CommandResultEnvelope
		if err := proto.Unmarshal(body, &result); err != nil {
			return err
		}
		if result.OperationId != operationID {
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

func isExpectedRestart(err error) bool {
	var closeErr *websocket.CloseError
	return errors.As(err, &closeErr) &&
		(closeErr.Code == websocket.CloseServiceRestart || closeErr.Code == websocket.CloseNormalClosure)
}

// runHotScenario 是单热点 Farm 压测:owner(playID==farmID)先买种子+种,再对该
// plot 循环浇水,最后尝试收获;非 owner 可选(mixed)也发浇水,权限拒绝同样计入。
// expected_* 全部用 0(不校验版本),以便 200 个客户端并发把 turn 压到同一 farm。
func runHotScenario(conn *websocket.Conn, playerID, farmID uint64, runID string, snapshot *farmpb.FarmSnapshot, s *stats) error {
	if playerID != farmID {
		if !*hotMixed {
			return nil
		}
		// 非 owner 对第一个非空 plot 浇水(可能被权限拒绝,拒绝也是观测数据)。
		plotIndex, ok := firstNonEmptyPlot(snapshot)
		if !ok {
			return errors.New("hot: no non-empty plot for visitor water")
		}
		seq := uint64(2) // join 占 2(围观农场不发恢复标记),water 从 3 起
		for i := 0; i < *hotLoops; i++ {
			if err := runWaterOnce(conn, &seq, playerID, farmID, runID, i, plotIndex); err != nil {
				s.hotWaterErr.Add(1)
				return err
			}
			s.hotWaterOK.Add(1)
		}
		return nil
	}
	s.hotOwnerTried.Add(1)
	plotIndex, err := runPlantScenario(conn, playerID, runID, snapshot)
	if err != nil {
		s.hotPlantErr.Add(1)
		return err
	}
	s.hotPlantOK.Add(1)
	seq := uint64(5) // join=2, marker=3, buy=4, plant=5, water 从 6 起
	for i := 0; i < *hotLoops; i++ {
		if err := runWaterOnce(conn, &seq, playerID, farmID, runID, i, plotIndex); err != nil {
			s.hotWaterErr.Add(1)
			return err
		}
		s.hotWaterOK.Add(1)
	}
	// 收获:作物默认 10s 成熟,压测中通常未成熟而失败,失败本身计入观测。
	harvestPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.HarvestRequest{
		PlotId: plotIndex, TargetFarmId: farmID,
	})
	if err != nil {
		return err
	}
	if err := sendCommandAndWait(conn, &seq, fmt.Sprintf("load-harvest-%s-%d", runID, playerID),
		commonpb.CommandType_HARVEST, harvestPayload); err != nil {
		s.hotHarvestErr.Add(1)
		return nil // 未成熟是预期观测,不算整体失败
	}
	s.hotHarvestOK.Add(1)
	return nil
}

func runWaterOnce(conn *websocket.Conn, seq *uint64, playerID, farmID uint64, runID string, iteration int, plotIndex uint32) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.CareRequest{
		PlotId: plotIndex, TargetFarmId: farmID, CareType: publicpb.CareType_CARE_TYPE_WATER,
	})
	if err != nil {
		return err
	}
	return sendCommandAndWait(conn, seq, fmt.Sprintf("load-water-%s-%d-%d", runID, playerID, iteration),
		commonpb.CommandType_CARE, payload)
}

// sendCommandAndWait 发送一条命令并等待对应 operation_id 的最终结果。
// seq 在每次调用后递增,保证单连接帧序单调。
func sendCommandAndWait(conn *websocket.Conn, seq *uint64, operationID string, commandType commonpb.CommandType, payload []byte) error {
	*seq++
	if err := writeFrame(conn, *seq, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: "request-" + operationID,
		CommandType: commandType, Payload: payload,
	}); err != nil {
		return err
	}
	return readSuccessfulResult(conn, operationID)
}

func firstNonEmptyPlot(snapshot *farmpb.FarmSnapshot) (uint32, bool) {
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() != "" {
			return plot.GetPlotIndex(), true
		}
	}
	return 0, false
}

// runReconnectLoop 模拟重连风暴:每个 player 在首次 join 后立即断开并重连
// reconnect-loops 次,每次完整走 login -> hello -> join -> snapshot。
func runReconnectLoop(ctx context.Context, playerID uint64, runID string, secret []byte, s *stats) {
	for i := 0; i < *reconnectLoops; i++ {
		start := time.Now()
		s.reconnectTried.Add(1)
		if err := reconnectOnce(ctx, playerID, runID, i, secret, s); err != nil {
			s.reconnectErr.Add(1)
			s.recordFailure("reconnect", err)
			return
		}
		s.reconnectOK.Add(1)
		s.recordReconnect(start)
	}
}

func reconnectOnce(ctx context.Context, playerID uint64, runID string, attempt int, secret []byte, s *stats) error {
	login, err := acquireTicket(ctx, playerID, secret, runID)
	if err != nil {
		s.loginErr.Add(1)
		return err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		s.connectErr.Add(1)
		return err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      "ws-stress/1",
	}); err != nil {
		s.helloErr.Add(1)
		return err
	}
	if err := readUntil(conn, frameKindServerHello); err != nil {
		s.helloErr.Add(1)
		return err
	}
	targetOwner := playerID
	if *sharedFarm > 0 {
		targetOwner = *sharedFarm
	}
	joinOperation := fmt.Sprintf("load-rejoin-%s-%d-%d", runID, playerID, attempt)
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		s.joinErr.Add(1)
		return err
	}
	if err := writeFrame(conn, 2, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: joinOperation,
		RequestId:   "request-" + joinOperation,
		CommandType: commonpb.CommandType_JOIN_FARM,
		Payload:     joinPayload,
	}); err != nil {
		s.joinErr.Add(1)
		return err
	}
	if _, err := readUntilSnapshot(conn, 3, joinOperation, targetOwner, targetOwner == playerID); err != nil {
		s.joinErr.Add(1)
		return err
	}
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "reconnect loop"),
		time.Now().Add(time.Second))
	return nil
}

// friendScenarios marks scenarios that drive the real HTTP friend endpoints.
var friendScenarios = map[string]bool{
	"friend-list": true, "next-opportunity": true, "hint": true,
}

func validateFriendCapacityFlags() error {
	if *friendCount != 20 && *friendCount != 100 && *friendCount != 300 {
		return errors.New("friend-count must be 20, 100 or 300")
	}
	if strings.TrimSpace(*runIDFlag) == "" {
		return errors.New("--run-id is required for friend-capacity C0")
	}
	if *friendViewer == 0 {
		return errors.New("friend-viewer is required and must equal the login player identity")
	}
	if *friendViewer != *playerStart {
		return fmt.Errorf("friend-viewer (%d) must equal player-start (%d); C0 measures one seeded viewer identity", *friendViewer, *playerStart)
	}
	if *conns != 1 {
		return errors.New("friend-capacity C0 requires conns=1 so HTTP cookie identity matches the fixture viewer")
	}
	if flagWasSet("online-ratio") {
		return errors.New("online-ratio is unsupported in C0; omit the flag (presence seeding is not implemented)")
	}
	if flagWasSet("friend-base") {
		return errors.New("friend-base is unsupported in C0; omit the flag (fixture owns --fake-base)")
	}
	transport := strings.ToLower(strings.TrimSpace(*hintTransport))
	switch transport {
	case "pubsub", "shadow-direct", "direct":
		*hintTransport = transport
	default:
		return fmt.Errorf(
			"hint-transport %q must be pubsub, shadow-direct or direct",
			*hintTransport,
		)
	}
	if *scenario == "hint" {
		if *hintTargetOwner == 0 || *hintTargetOwner == *friendViewer {
			return errors.New("hint scenario requires --hint-target-owner distinct from friend-viewer")
		}
		if *hintWait <= 0 {
			return errors.New("hint-wait must be positive")
		}
		if *hintLoops < 1 {
			return errors.New("hint-loops must be >= 1")
		}
	}
	_, hasViewerAccount, err := accountCredentialFromEnv(
		viewerAccountUsernameEnv,
		viewerAccountPasswordEnv,
	)
	if err != nil {
		return err
	}
	_, hasTargetAccount, err := accountCredentialFromEnv(
		hintTargetAccountUsernameEnv,
		hintTargetAccountPasswordEnv,
	)
	if err != nil {
		return err
	}
	if hasTargetAccount && !hasViewerAccount {
		return errors.New("showcase hint target account requires showcase viewer account credentials")
	}
	if *scenario == "hint" && hasViewerAccount && !hasTargetAccount {
		return errors.New("showcase hint scenario requires hint target account credentials")
	}
	return nil
}

func flagWasSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// httpClientFor returns a client that sends the farm_session cookie on every
// request. The cookie is captured from the login Set-Cookie header by
// acquireTicket and stored in the stats when the login succeeds.
func httpClientFor(cookie string) *http.Client {
	jar, _ := cookiejar.New(nil)
	base := strings.TrimRight(*httpURL, "/")
	if parsed, err := url.Parse(base); err == nil && cookie != "" {
		jar.SetCookies(parsed, []*http.Cookie{{Name: auth.HTTPSessionCookieName, Value: cookie, Path: "/"}})
	}
	return &http.Client{Jar: jar, Transport: loadHTTPTransport, Timeout: 15 * time.Second}
}

// runFriendCapacityReadyPath performs the login + WSS ServerHello readiness
// leg with the exact HTTPS login player/session identity and returns once the
// WSS session is ready. The friend workload itself never runs inside this
// function, so readiness latency excludes the scenario.
func runFriendCapacityReadyPath(ctx context.Context, playerID uint64, login loginResult, s *stats, readyStart time.Time) error {
	if s.friend == nil {
		return nil
	}
	if err := validateFriendLoadIdentity(playerID, login); err != nil {
		return err
	}
	dialer, err := websocketDialer()
	if err != nil {
		s.connectErr.Add(1)
		return err
	}
	conn, _, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		s.connectErr.Add(1)
		return err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      "ws-stress/1",
	}); err != nil {
		s.helloErr.Add(1)
		return err
	}
	hello, err := readServerHelloForPlayer(conn, playerID, login.SessionID)
	if err != nil {
		s.helloErr.Add(1)
		return err
	}
	if hello.GetPlayerId() != playerID || hello.GetSessionId() != login.SessionID {
		return fmt.Errorf("ServerHello identity %d/%q does not match login %d/%q", hello.GetPlayerId(), hello.GetSessionId(), playerID, login.SessionID)
	}
	s.recordReady(readyStart)
	return nil
}

// runFriendCapacityScenario drives one viewer through the real HTTPS friend
// endpoints after a real Login. scenarioTried is counted by runPlayer before
// Login; this function records exactly one terminal outcome via recordScenario
// on success or scenarioErr on failure.
func runFriendCapacityScenario(ctx context.Context, playerID uint64, login loginResult, runID string, secret []byte, s *stats, scenarioStart time.Time) {
	if s.friend == nil {
		return
	}
	finish := func(err error) {
		if err != nil {
			s.scenarioErr.Add(1)
			s.friend.recordFailure(*scenario, err)
			return
		}
		s.recordScenario(scenarioStart)
	}
	viewer := *friendViewer
	client := httpClientFor(login.Cookie)
	base := strings.TrimRight(*httpURL, "/")

	switch *scenario {
	case "friend-list":
		finish(runFriendListScenario(ctx, client, base, viewer, s))
	case "next-opportunity":
		finish(runNextOpportunityScenario(ctx, client, base, viewer, s))
	case "hint":
		finish(runHintScenario(ctx, client, base, viewer, playerID, runID, secret, s))
	default:
		finish(fmt.Errorf("unsupported friend scenario %q", *scenario))
	}
}

func validateFriendLoadIdentity(playerID uint64, login loginResult) error {
	if *friendViewer == 0 {
		return errors.New("friend-viewer is required")
	}
	if playerID != *friendViewer {
		return fmt.Errorf("login player %d does not match friend-viewer %d", playerID, *friendViewer)
	}
	want := strconv.FormatUint(*friendViewer, 10)
	if login.PlayerID != want {
		return fmt.Errorf("login ticket player_id %q does not match friend-viewer %s", login.PlayerID, want)
	}
	if strings.TrimSpace(login.Cookie) == "" {
		return errors.New("login did not issue farm_session cookie for the configured viewer")
	}
	if strings.TrimSpace(login.ConnectionTicket) == "" {
		return errors.New("login did not issue connection ticket for the configured viewer")
	}
	return nil
}

func runFriendListScenario(ctx context.Context, client *http.Client, base string, viewer uint64, s *stats) error {
	pageSize := 20
	pages := *friendPages
	if pages < 1 {
		pages = 1
	}
	cursor := ""
	seen := 0
	for page := 0; page < pages; page++ {
		pageStart := time.Now()
		count, nextCursor, status, err := friendListRequest(ctx, client, base, pageSize, cursor)
		elapsed := time.Since(pageStart)
		outcome := outcomeForHTTP(status, err)
		s.friend.record(friendcapacity.StagePull, friendcapacity.ModeFriendList, outcome, elapsed)
		if count > 0 {
			s.friend.recordBatch(friendcapacity.StagePull, friendcapacity.ModeFriendList, count)
		}
		if err != nil || status != http.StatusOK {
			if err == nil {
				err = fmt.Errorf("friends status %d", status)
			}
			return err
		}
		seen += count
		if nextCursor == "" || count == 0 {
			break
		}
		cursor = nextCursor
	}
	if seen > 0 {
		s.friend.recordBatch(friendcapacity.StageFriendIndex, friendcapacity.ModeFriendList, seen)
	}
	_ = viewer // identity already validated; HTTP cookie is the authority.
	return nil
}

func runNextOpportunityScenario(ctx context.Context, client *http.Client, base string, viewer uint64, s *stats) error {
	started := time.Now()
	status, answer, _, err := nextOpportunityRequest(ctx, client, base)
	if err != nil {
		outcome := outcomeForHTTP(status, err)
		s.friend.record(friendcapacity.StagePull, friendcapacity.ModeNextOpportunity, outcome, time.Since(started))
		return err
	}
	outcome := opportunityOutcome(status, answer, err)
	s.friend.record(friendcapacity.StagePull, friendcapacity.ModeNextOpportunity, outcome, time.Since(started))
	if outcome == friendcapacity.OutcomeUnknown || outcome == friendcapacity.OutcomeUnavailable {
		return fmt.Errorf("opportunity status %d answer=%s", status, answer)
	}
	_ = viewer
	return nil
}

func runHintScenario(ctx context.Context, client *http.Client, base string, viewer, playerID uint64, runID string, secret []byte, s *stats) error {
	loops := *hintLoops
	if loops < 1 {
		loops = 1
	}
	for loop := 0; loop < loops; loop++ {
		if err := runHintLoop(ctx, client, base, viewer, playerID, runID, secret, s); err != nil {
			return err
		}
	}
	return nil
}

// friendListRequest performs one GET /api/v1/friends page and returns the
// number of friend cards, the next cursor, the HTTP status and an error.
func friendListRequest(ctx context.Context, client *http.Client, base string, pageSize int, cursor string) (int, string, int, error) {
	target := fmt.Sprintf("%s/api/v1/friends?page_size=%d", base, pageSize)
	if cursor != "" {
		target += "&cursor=" + url.QueryEscape(cursor)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, "", 0, err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "", 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		return 0, "", response.StatusCode, fmt.Errorf("friends status %d: %s", response.StatusCode, body)
	}
	var payload struct {
		Friends    []json.RawMessage `json:"friends"`
		NextCursor string            `json:"next_cursor,omitempty"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return 0, "", response.StatusCode, err
	}
	return len(payload.Friends), payload.NextCursor, response.StatusCode, nil
}

// nextOpportunityAnswer is the server's semantic answer for the opportunity
// endpoint. NONE and FOUND are HTTP 200; UNKNOWN maps to HTTP 503
// FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE and must never be coerced into
// NOT_FOUND or success.
const (
	nextOpportunityFound   = "FOUND"
	nextOpportunityNone    = "NONE"
	nextOpportunityUnknown = "UNKNOWN"
)

// nextOpportunityRequest performs one GET /api/v1/friends/opportunity and
// reports the HTTP status plus the semantic answer (FOUND/NONE/UNKNOWN) and
// the server Retry-After hint (0 when absent).
func nextOpportunityRequest(ctx context.Context, client *http.Client, base string) (int, string, time.Duration, error) {
	target := fmt.Sprintf("%s/api/v1/friends/opportunity", base)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, "", 0, err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "", 0, err
	}
	defer response.Body.Close()
	retryAfter := parseRetryAfterSeconds(response.Header.Get("Retry-After"))
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		if response.StatusCode == http.StatusServiceUnavailable {
			return response.StatusCode, nextOpportunityUnknown, retryAfter, fmt.Errorf("opportunity status %d: %s", response.StatusCode, body)
		}
		return response.StatusCode, "", retryAfter, fmt.Errorf("opportunity status %d: %s", response.StatusCode, body)
	}
	var payload struct {
		Status string           `json:"status"`
		Friend *json.RawMessage `json:"friend,omitempty"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return response.StatusCode, "", retryAfter, err
	}
	switch payload.Status {
	case nextOpportunityFound, nextOpportunityNone:
		return response.StatusCode, payload.Status, retryAfter, nil
	case "":
		return response.StatusCode, "", retryAfter, fmt.Errorf("opportunity response missing status field")
	default:
		return response.StatusCode, nextOpportunityUnknown, retryAfter, fmt.Errorf("opportunity status %q is not FOUND/NONE", payload.Status)
	}
}

// parseRetryAfterSeconds parses a Retry-After header (delta-seconds form; the
// HTTP-date form falls back to zero). Returns 0 when absent or unparsable.
func parseRetryAfterSeconds(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// opportunityOutcome maps an HTTP status + semantic answer onto the bounded
// friend-capacity outcomes. UNKNOWN stays unknown (never not_found/success);
// NONE is a real HTTP 200 negative answer recorded as not_found.
func opportunityOutcome(status int, answer string, err error) string {
	if answer == nextOpportunityUnknown {
		return friendcapacity.OutcomeUnknown
	}
	if answer == nextOpportunityNone {
		return friendcapacity.OutcomeNotFound
	}
	return outcomeForHTTP(status, err)
}

// runHintLoop joins as the configured viewer, drives a deterministic opportunity
// change on hint-target-owner, then waits for a validated FRIEND_OPPORTUNITY_CHANGED
// push. Timeout/drop is never success.
func runHintLoop(ctx context.Context, _ *http.Client, base string, viewer, playerID uint64, runID string, secret []byte, s *stats) error {
	if playerID != viewer {
		return fmt.Errorf("hint loop player %d does not match viewer %d", playerID, viewer)
	}
	login, err := acquireTicket(ctx, playerID, secret, runID)
	if err != nil {
		s.loginErr.Add(1)
		return err
	}
	if err := validateFriendLoadIdentity(playerID, login); err != nil {
		return err
	}
	viewerClient := httpClientFor(login.Cookie)
	if base == "" {
		base = strings.TrimRight(*httpURL, "/")
	}
	dialer, err := websocketDialer()
	if err != nil {
		s.connectErr.Add(1)
		return err
	}
	conn, _, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		s.connectErr.Add(1)
		return err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket,
		ClientBuild:      "ws-stress/1",
	}); err != nil {
		s.helloErr.Add(1)
		return err
	}
	if _, err := readServerHelloForPlayer(conn, viewer, login.SessionID); err != nil {
		s.helloErr.Add(1)
		return err
	}
	joinOperation := fmt.Sprintf("load-hint-join-%s-%d", runID, playerID)
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: playerID})
	if err != nil {
		s.joinErr.Add(1)
		return err
	}
	if err := writeFrame(conn, 2, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: joinOperation,
		RequestId:   "request-" + joinOperation,
		CommandType: commonpb.CommandType_JOIN_FARM,
		Payload:     joinPayload,
	}); err != nil {
		s.joinErr.Add(1)
		return err
	}
	if _, err := readUntilSnapshot(conn, 3, joinOperation, playerID, true); err != nil {
		s.joinErr.Add(1)
		return err
	}

	pullStart := time.Now()
	status, answer, _, pullErr := nextOpportunityRequest(ctx, viewerClient, base)
	if pullErr != nil {
		pullOutcome := opportunityOutcome(status, answer, pullErr)
		s.friend.record(friendcapacity.StagePull, friendcapacity.ModeNextOpportunity, pullOutcome, time.Since(pullStart))
		return pullErr
	}
	pullOutcome := opportunityOutcome(status, answer, pullErr)
	s.friend.record(friendcapacity.StagePull, friendcapacity.ModeNextOpportunity, pullOutcome, time.Since(pullStart))
	if pullOutcome == friendcapacity.OutcomeUnknown || pullOutcome == friendcapacity.OutcomeUnavailable {
		return fmt.Errorf("opportunity status %d answer=%s", status, answer)
	}

	sourceStart := time.Now()
	baselineVersion, committedVersion, err := driveHintSourceTransition(ctx, *hintTargetOwner, runID, secret)
	if err != nil {
		s.friend.record(friendcapacity.StageHintReceive, friendcapacity.ModeHint, friendcapacity.OutcomeUnavailable, 0)
		s.friend.setHintMeasurement("unavailable")
		return fmt.Errorf("hint source transition unavailable: %w", err)
	}
	if committedVersion == 0 {
		s.friend.record(friendcapacity.StageHintReceive, friendcapacity.ModeHint, friendcapacity.OutcomeUnavailable, 0)
		s.friend.setHintMeasurement("unavailable")
		return fmt.Errorf("hint source transition produced no observed causal farm_version (baseline=%d); hint measurement unavailable", baselineVersion)
	}
	// The exact observed committed farm_version is the causal floor: a Hint
	// whose source_farm_version is below the observed commit is stale and must
	// not pass even if it is a newer unrelated revision.
	minSource := committedVersion
	deadline := time.Now().Add(*hintWait)
	_ = conn.SetReadDeadline(deadline)
	for {
		kind, body, readErr := readFrame(conn)
		if readErr != nil {
			s.friend.record(friendcapacity.StageHintReceive, friendcapacity.ModeHint, friendcapacity.OutcomeDropped, time.Since(sourceStart))
			s.friend.setHintMeasurement("source_to_receive")
			return fmt.Errorf("hint timeout/drop waiting for target owner %d: %w", *hintTargetOwner, readErr)
		}
		if kind != frameKindServerPush {
			continue
		}
		var push commonpb.ServerPushEnvelope
		if err := proto.Unmarshal(body, &push); err != nil {
			continue
		}
		if push.GetPushType() != commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED {
			continue
		}
		var hint friendpb.FriendOpportunityChanged
		if err := proto.Unmarshal(push.GetPayload(), &hint); err != nil {
			continue
		}
		if err := validateHintSuccess(&push, &hint, viewer, *hintTargetOwner, minSource, 1); err != nil {
			continue
		}
		s.friend.record(friendcapacity.StageHintReceive, friendcapacity.ModeHint, friendcapacity.OutcomeOK, time.Since(sourceStart))
		s.friend.setHintMeasurement("source_to_receive")
		return nil
	}
}

// validateHintSuccess requires WSS recipient identity, target owner, and a
// causally new source_farm_version/opportunity_revision after the source chain.
func validateHintSuccess(push *commonpb.ServerPushEnvelope, hint *friendpb.FriendOpportunityChanged, viewer, targetOwner, minSourceFarmVersion, minOpportunityRevision uint64) error {
	if push == nil || hint == nil {
		return errors.New("hint push/payload is nil")
	}
	recipient := push.GetPlayer().GetTargetPlayerId()
	if recipient != viewer {
		return fmt.Errorf("hint recipient player_id=%d does not match friend-viewer %d", recipient, viewer)
	}
	if hint.GetTargetOwnerId() != targetOwner {
		return fmt.Errorf("hint target_owner_id=%d does not match configured target %d", hint.GetTargetOwnerId(), targetOwner)
	}
	if hint.GetSourceFarmVersion() == 0 || hint.GetOpportunityRevision() == 0 {
		return errors.New("hint missing source_farm_version/opportunity_revision")
	}
	if hint.GetSourceFarmVersion() < minSourceFarmVersion {
		return fmt.Errorf("hint source_farm_version=%d is not causally new (min=%d)", hint.GetSourceFarmVersion(), minSourceFarmVersion)
	}
	if hint.GetOpportunityRevision() < minOpportunityRevision {
		return fmt.Errorf("hint opportunity_revision=%d is not causally new (min=%d)", hint.GetOpportunityRevision(), minOpportunityRevision)
	}
	return nil
}

// driveHintSourceTransition performs a real Login/Join/Buy/Plant on the
// configured hint target owner so the opportunity boundary advances through the
// production actor path. This is the only C0-supported Hint source. Buy/Plant
// must reach terminal SUCCEEDED; PROCESSING and NO_CHANGE are not success.
func driveHintSourceTransition(ctx context.Context, targetOwner uint64, runID string, secret []byte) (baselineFarmVersion, committedFarmVersion uint64, err error) {
	login, err := acquireTicket(ctx, targetOwner, secret, runID)
	if err != nil {
		return 0, 0, err
	}
	if login.PlayerID != strconv.FormatUint(targetOwner, 10) {
		return 0, 0, fmt.Errorf("source transition login identity mismatch: got %s want %d", login.PlayerID, targetOwner)
	}
	dialer, err := websocketDialer()
	if err != nil {
		return 0, 0, err
	}
	conn, _, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket, ClientBuild: "ws-stress/1",
	}); err != nil {
		return 0, 0, err
	}
	if _, err := readServerHelloForPlayer(conn, targetOwner, login.SessionID); err != nil {
		return 0, 0, err
	}
	joinOperation := fmt.Sprintf("load-hint-source-join-%s-%d", runID, targetOwner)
	joinPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: targetOwner})
	if err != nil {
		return 0, 0, err
	}
	if err := writeFrame(conn, 2, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: joinOperation, RequestId: "request-" + joinOperation,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinPayload,
	}); err != nil {
		return 0, 0, err
	}
	snapshot, err := readUntilSnapshot(conn, 3, joinOperation, targetOwner, true)
	if err != nil {
		return 0, 0, err
	}
	baselineFarmVersion = snapshot.GetFarmVersion()
	committedFarmVersion, err = runHintSourcePlant(conn, targetOwner, runID, snapshot)
	if err != nil {
		return baselineFarmVersion, 0, err
	}
	if committedFarmVersion == 0 {
		return baselineFarmVersion, 0, errors.New("hint source Buy/Plant reached terminal SUCCEEDED but no FarmChange farm_version was observed; causal source version unavailable")
	}
	return baselineFarmVersion, committedFarmVersion, nil
}

// runHintSourcePlant buys and plants with terminal SUCCEEDED only. It returns
// the farm_version observed on a FarmChange push during the Buy/Plant chain.
// A terminal SUCCEEDED without any observed FarmChange version returns
// (0, nil) so the caller can record causal-unavailable; snapshot_version+1 is
// never fabricated because a fabricated version could pass a stale Hint.
func runHintSourcePlant(conn *websocket.Conn, playerID uint64, runID string, snapshot *farmpb.FarmSnapshot) (uint64, error) {
	var plotIndex uint32
	found := false
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() == "" {
			plotIndex, found = plot.GetPlotIndex(), true
			break
		}
	}
	if !found {
		return 0, errors.New("hint source plant requires an empty plot")
	}
	buyPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return 0, err
	}
	buyOperation := fmt.Sprintf("load-buy-%s-%d", runID, playerID)
	if err := writeFrame(conn, 4, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: buyOperation, RequestId: "request-" + buyOperation,
		CommandType: commonpb.CommandType_BUY, Payload: buyPayload,
	}); err != nil {
		return 0, err
	}
	if _, _, err := readTerminalSucceededResult(conn, buyOperation); err != nil {
		return 0, fmt.Errorf("buy seed: %w", err)
	}
	plantPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: plotIndex, CropDefinitionId: 1, TargetFarmId: playerID,
	})
	if err != nil {
		return 0, err
	}
	plantOperation := fmt.Sprintf("load-plant-%s-%d", runID, playerID)
	if err := writeFrame(conn, 5, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: plantOperation, RequestId: "request-" + plantOperation,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}); err != nil {
		return 0, err
	}
	version, observed, err := readTerminalSucceededResult(conn, plantOperation)
	if err != nil {
		return 0, fmt.Errorf("plant crop: %w", err)
	}
	if !observed {
		return 0, nil
	}
	return version, nil
}

func outcomeForHTTP(status int, err error) string {
	// Prefer the HTTP status when present so a 503 body error is recorded as
	// unavailable rather than a generic transport error.
	if status == http.StatusOK && err == nil {
		return friendcapacity.OutcomeOK
	}
	if status == http.StatusServiceUnavailable || status == http.StatusConflict {
		return friendcapacity.OutcomeUnavailable
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return friendcapacity.OutcomeUnknown
		}
		return friendcapacity.OutcomeError
	}
	if status == http.StatusOK {
		return friendcapacity.OutcomeOK
	}
	return friendcapacity.OutcomeError
}

func mustJSON(value any) []byte {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return []byte(`{}`)
	}
	return append(data, '\n')
}

func currentGitSHA() string {
	output, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

const (
	measurementUnavailable = "measurement_unavailable"
	measurementClientNone  = "client_unmeasured"
)

// friendCapacityStats aggregates the per-client friend-capacity observations.
// Phase durations are recorded through friendcapacity.Metrics. Redis command
// and byte totals are never fabricated here; the C0 runner merges service
// /metrics deltas or marks them measurement_unavailable.
type friendCapacityStats struct {
	mu              sync.Mutex
	metrics         *friendcapacity.Metrics
	stages          map[string]int64
	batchSizes      map[string]int64
	failures        map[string]int64
	hintMeasurement string
}

func newFriendCapacityStats() *friendCapacityStats {
	registry := prometheus.NewRegistry()
	return &friendCapacityStats{
		metrics:    friendcapacity.NewMetrics(registry),
		stages:     make(map[string]int64),
		batchSizes: make(map[string]int64),
		failures:   make(map[string]int64),
	}
}

func (f *friendCapacityStats) record(stage, mode, outcome string, elapsed time.Duration) {
	if f == nil {
		return
	}
	if f.metrics != nil {
		f.metrics.Observe(friendcapacity.PhaseObservation{
			Stage: stage, Mode: mode, Outcome: outcome, Layer: friendcapacity.LayerClient,
		}, elapsed)
	}
	key := stage + "/" + mode + "/" + outcome
	f.mu.Lock()
	f.stages[key]++
	f.mu.Unlock()
}

func (f *friendCapacityStats) recordBatch(stage, mode string, size int) {
	if f == nil || size <= 0 {
		return
	}
	if f.metrics != nil {
		f.metrics.ObserveBatch(stage, mode, size)
	}
	key := stage + "/" + mode
	f.mu.Lock()
	f.batchSizes[key] += int64(size)
	f.mu.Unlock()
}

func (f *friendCapacityStats) recordFailure(stage string, err error) {
	if f == nil || err == nil {
		return
	}
	reason := stage + ": " + stableFailureReason(err)
	f.mu.Lock()
	if f.failures == nil {
		f.failures = make(map[string]int64)
	}
	f.failures[reason]++
	f.mu.Unlock()
}

func (f *friendCapacityStats) setHintMeasurement(kind string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.hintMeasurement = kind
	f.mu.Unlock()
}

type friendCapacityEffectiveConfig struct {
	FriendCount   int    `json:"friend_count"`
	FriendViewer  uint64 `json:"friend_viewer"`
	HintTransport string `json:"hint_transport"`
	HintTarget    uint64 `json:"hint_target_owner,omitempty"`
	Conns         int    `json:"conns"`
}

type friendCapacityData struct {
	FriendCount                int                                `json:"friend_count"`
	EffectiveConfig            friendCapacityEffectiveConfig      `json:"effective_config"`
	ExpectedUnsupported        map[string]string                  `json:"expected_unsupported_knobs"`
	RedisCommands              *int64                             `json:"redis_commands"`
	RedisCommandsAvailability  string                             `json:"redis_commands_availability"`
	EstimatedBytes             *int64                             `json:"estimated_bytes"`
	EstimatedBytesAvailability string                             `json:"estimated_bytes_availability"`
	EstimatedBytesNote         string                             `json:"estimated_bytes_note"`
	RedisEvidenceScope         string                             `json:"redis_evidence_scope,omitempty"`
	RedisEvidenceEndpoints     []string                           `json:"redis_evidence_endpoints,omitempty"`
	Phases                     friendcapacity.PhaseStats          `json:"phases"`
	ServerPhaseAvailability    []friendcapacity.PhaseAvailability `json:"server_phase_availability"`
	BatchSizes                 map[string]int64                   `json:"batch_sizes"`
	Stages                     map[string]int64                   `json:"stages"`
	Failures                   map[string]int64                   `json:"failures,omitempty"`
	HintMeasurement            string                             `json:"hint_measurement,omitempty"`
}

func (f *friendCapacityStats) snapshot() *friendCapacityData {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &friendCapacityData{
		FriendCount: *friendCount,
		EffectiveConfig: friendCapacityEffectiveConfig{
			FriendCount: *friendCount, FriendViewer: *friendViewer,
			HintTransport: *hintTransport, HintTarget: *hintTargetOwner, Conns: *conns,
		},
		ExpectedUnsupported: map[string]string{
			"online-ratio": "unsupported_in_c0_presence_seeding",
			"friend-base":  "unsupported_in_c0_owned_by_fixture",
		},
		RedisCommands:              nil,
		RedisCommandsAvailability:  measurementClientNone,
		EstimatedBytes:             nil,
		EstimatedBytesAvailability: measurementClientNone,
		EstimatedBytesNote:         "exact Redis wire bytes require a Harness proxy; service /metrics expose estimated response bytes labeled estimated=true",
		ServerPhaseAvailability:    friendcapacity.UnavailableServerPhases(),
		BatchSizes:                 make(map[string]int64, len(f.batchSizes)),
		Stages:                     make(map[string]int64, len(f.stages)),
		HintMeasurement:            f.hintMeasurement,
	}
	if f.metrics != nil {
		out.Phases = f.metrics.QuantileSnapshot()
	}
	for key, value := range f.batchSizes {
		out.BatchSizes[key] = value
	}
	for key, value := range f.stages {
		out.Stages[key] = value
	}
	if len(f.failures) > 0 {
		out.Failures = make(map[string]int64, len(f.failures))
		for key, value := range f.failures {
			out.Failures[key] = value
		}
	}
	return out
}
