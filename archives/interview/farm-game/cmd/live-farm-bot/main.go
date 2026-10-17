package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"farmgame/internal/demokit"
	"farmgame/internal/demoorchestrator"
	"farmgame/internal/farmbotclient"
	"farmgame/internal/livebot"
	"farmgame/internal/livefarmbot"
	"farmgame/internal/selfprobe"
)

const (
	defaultHTTPURL = "http://127.0.0.1:8080"
	defaultWSURL   = "ws://127.0.0.1:8080/ws"
	// Vite UI uses 18602 and proxies its API requests to this controller on 18601.
	defaultListen       = "127.0.0.1:18601"
	defaultPlayerStart  = uint64(900000000)
	defaultSecretEnv    = "LOAD_TEST_TOKEN_SECRET"
	defaultRunID        = "live-farm-bot"
	MaxServeAfter       = 5 * time.Minute
	defaultShutdownWait = 2 * time.Second
)

var environmentNamePattern = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_]{0,127}$")

type config struct {
	httpURL           string
	wsURL             string
	listen            string
	players           int
	playerStart       uint64
	duration          time.Duration
	arrivalRate       float64
	maxOnline         int
	seed              uint64
	scenario          string
	sessionDuration   time.Duration
	thinkTime         time.Duration
	actionWarmup      time.Duration
	businessWarmup    time.Duration
	resultPoll        time.Duration
	unlockPlots       bool
	unlockPlotLimit   int
	unlockConcurrency int
	sessionActions    int
	actionMix         string
	targetFarmID      uint64
	targetFarms       int
	profileDir        string
	loadSecretEnv     string
	eventCapacity     int
	serveAfter        time.Duration
	allowNonLoopback  bool
	environment       string
	runID             string
	tlsCAPath         string
	clientBuild       string
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "live-farm-bot:", err)
		os.Exit(1)
	}
}

func run(arguments []string, stdout io.Writer) error {
	cfg, err := parseConfig(arguments, io.Discard)
	if err != nil {
		return err
	}
	return runConfig(cfg, stdout)
}

func parseConfig(arguments []string, output io.Writer) (config, error) {
	if output == nil {
		output = io.Discard
	}
	cfg := config{
		httpURL:       defaultHTTPURL,
		wsURL:         defaultWSURL,
		listen:        defaultListen,
		players:       1,
		playerStart:   defaultPlayerStart,
		duration:      30 * time.Second,
		arrivalRate:   1,
		maxOnline:     1,
		seed:          1,
		scenario:      "default",
		loadSecretEnv: defaultSecretEnv,
		eventCapacity: demoorchestrator.DefaultEventCapacity,
		environment:   string(livebot.EnvironmentLocal),
		runID:         defaultRunID,
		clientBuild:   "live-farm-bot/1",
	}
	flags := flag.NewFlagSet("live-farm-bot", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.httpURL, "http-url", cfg.httpURL, "farm-gate HTTP base URL")
	flags.StringVar(&cfg.wsURL, "ws-url", cfg.wsURL, "farm-gate WebSocket URL")
	flags.StringVar(&cfg.listen, "listen", cfg.listen, "read-only controller listen address")
	flags.IntVar(&cfg.players, "players", cfg.players, "number of real players")
	flags.Uint64Var(&cfg.playerStart, "player-start", cfg.playerStart, "first player id")
	flags.DurationVar(&cfg.duration, "duration", cfg.duration, "bounded run duration")
	flags.Float64Var(&cfg.arrivalRate, "arrival-rate", cfg.arrivalRate, "bot arrival rate per second")
	flags.IntVar(&cfg.maxOnline, "max-online", cfg.maxOnline, "maximum concurrent bots")
	flags.Uint64Var(&cfg.seed, "seed", cfg.seed, "deterministic run seed")
	flags.StringVar(&cfg.scenario, "scenario", cfg.scenario, "default, join, plant, care, theft, harvest, reconnect, mixed, or journey")
	flags.DurationVar(&cfg.sessionDuration, "session-duration", cfg.sessionDuration, "keep each bot online for this long and act repeatedly; zero runs one action and disconnects")
	flags.DurationVar(&cfg.thinkTime, "think-time", cfg.thinkTime, "average pause between actions of one online bot")
	flags.DurationVar(&cfg.actionWarmup, "action-warmup", cfg.actionWarmup, "shared post-Join hold before any unlock/business action; separates login ramp from farm load")
	flags.DurationVar(&cfg.businessWarmup, "business-warmup", cfg.businessWarmup, "shared process-relative barrier after unlock warmup and before measured business actions")
	flags.IntVar(&cfg.sessionActions, "session-actions", cfg.sessionActions, "maximum actions per online bot; zero means unlimited within the session")
	flags.StringVar(&cfg.actionMix, "action-mix", cfg.actionMix, "weighted actions for scenario=mixed, e.g. care=5,harvest=3,plant=2")
	flags.DurationVar(&cfg.resultPoll, "result-poll", cfg.resultPoll, "override how long to wait before re-asking whether an async operation settled; zero honours the server's retry hint, which floors async latency at that hint")
	flags.BoolVar(&cfg.unlockPlots, "unlock-plots", cfg.unlockPlots, "before the measured loop, have each bot buy plots one by one with real UNLOCK_PLOT commands; requires players provisioned with enough coins and level")
	flags.IntVar(&cfg.unlockPlotLimit, "unlock-plot-limit", cfg.unlockPlotLimit, "maximum plots each bot unlocks during warmup; zero means all 19 initially locked plots")
	flags.IntVar(&cfg.unlockConcurrency, "unlock-concurrency", cfg.unlockConcurrency, "maximum concurrent UNLOCK_PLOT workflows in this driver; zero uses the safe default 4")
	flags.Uint64Var(&cfg.targetFarmID, "target-farm-id", cfg.targetFarmID, "farm to join; zero means the bot's own farm")
	flags.IntVar(&cfg.targetFarms, "target-farms", cfg.targetFarms, "spread bots over this many consecutive farms from --target-farm-id; players divided by this is the crowd per farm")
	flags.StringVar(&cfg.profileDir, "profile-dir", cfg.profileDir, "write cpu/mutex/block/goroutine/heap profiles of the driver here; empty disables profiling")
	flags.StringVar(&cfg.loadSecretEnv, "load-secret-env", cfg.loadSecretEnv, "environment variable NAME containing the shared HMAC secret")
	flags.IntVar(&cfg.eventCapacity, "event-capacity", cfg.eventCapacity, "bounded LIVE event retention capacity")
	flags.DurationVar(&cfg.serveAfter, "serve-after", cfg.serveAfter, "bounded time to keep the read-only API after the run")
	flags.BoolVar(&cfg.allowNonLoopback, "allow-non-loopback", cfg.allowNonLoopback, "explicitly allow non-loopback targets/listen address")
	flags.StringVar(&cfg.environment, "environment", cfg.environment, "local, ci, or sandbox")
	flags.StringVar(&cfg.runID, "run-id", cfg.runID, "deterministic run identity")
	flags.StringVar(&cfg.tlsCAPath, "tls-ca", cfg.tlsCAPath, "optional PEM CA file for HTTPS/WSS")
	flags.StringVar(&cfg.clientBuild, "client-build", cfg.clientBuild, "client build label sent during ClientHello")
	if err := flags.Parse(arguments); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return normalizeConfig(cfg)
}

func normalizeConfig(cfg config) (config, error) {
	cfg.httpURL = strings.TrimSpace(cfg.httpURL)
	cfg.wsURL = strings.TrimSpace(cfg.wsURL)
	cfg.listen = strings.TrimSpace(cfg.listen)
	cfg.loadSecretEnv = strings.TrimSpace(cfg.loadSecretEnv)
	cfg.scenario = strings.ToLower(strings.TrimSpace(cfg.scenario))
	cfg.actionMix = strings.ToLower(strings.TrimSpace(cfg.actionMix))
	cfg.environment = strings.ToLower(strings.TrimSpace(cfg.environment))
	cfg.runID = strings.TrimSpace(cfg.runID)
	cfg.tlsCAPath = strings.TrimSpace(cfg.tlsCAPath)
	cfg.clientBuild = strings.TrimSpace(cfg.clientBuild)
	if cfg.scenario == "" {
		cfg.scenario = "default"
	}
	if cfg.environment == "" {
		cfg.environment = string(livebot.EnvironmentLocal)
	}
	if cfg.runID == "" {
		cfg.runID = defaultRunID
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg config) error {
	if err := validateEndpoint(cfg.httpURL, "http-url", cfg.allowNonLoopback, "http", "https"); err != nil {
		return err
	}
	if err := validateEndpoint(cfg.wsURL, "ws-url", cfg.allowNonLoopback, "ws", "wss"); err != nil {
		return err
	}
	if err := validateListen(cfg.listen, cfg.allowNonLoopback); err != nil {
		return err
	}
	if !environmentNamePattern.MatchString(cfg.loadSecretEnv) {
		return errors.New("load-secret-env must be an environment variable name")
	}
	switch cfg.scenario {
	case "default", "join", "plant", "care", "theft", "harvest", "reconnect", "mixed", "journey":
	default:
		return errors.New("scenario must be default, join, plant, care, theft, harvest, reconnect, mixed, or journey")
	}
	if cfg.scenario == "reconnect" && cfg.sessionDuration > 0 {
		return errors.New("scenario reconnect is a single-shot check and cannot run with session-duration")
	}
	if cfg.sessionDuration == 0 && (cfg.thinkTime > 0 || cfg.sessionActions > 0 || cfg.actionMix != "" || cfg.actionWarmup > 0 || cfg.businessWarmup > 0) {
		return errors.New("think-time, session-actions, action-mix, action-warmup and business-warmup require session-duration")
	}
	if cfg.actionWarmup < 0 || cfg.actionWarmup > 5*time.Minute {
		return errors.New("action-warmup must be between zero and 5m")
	}
	if cfg.businessWarmup < 0 || cfg.businessWarmup > 10*time.Minute ||
		(cfg.businessWarmup > 0 && cfg.businessWarmup < cfg.actionWarmup) {
		return errors.New("business-warmup must be between action-warmup and 10m")
	}
	if cfg.unlockPlotLimit < 0 || cfg.unlockPlotLimit > 19 {
		return errors.New("unlock-plot-limit must be between 0 and 19")
	}
	if cfg.unlockConcurrency < 0 || cfg.unlockConcurrency > 32 {
		return errors.New("unlock-concurrency must be between 0 and 32")
	}
	if cfg.targetFarms < 0 || cfg.targetFarms > cfg.players {
		return errors.New("target-farms must be between 0 and players")
	}
	if cfg.targetFarms > 1 && cfg.targetFarmID == 0 {
		return errors.New("target-farms needs target-farm-id to start from")
	}
	spec, err := runSpec(cfg)
	if err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	if cfg.eventCapacity <= 0 || cfg.eventCapacity > demoorchestrator.MaxEventCapacity {
		return fmt.Errorf("event-capacity must be between 1 and %d", demoorchestrator.MaxEventCapacity)
	}
	if cfg.serveAfter < 0 || cfg.serveAfter > MaxServeAfter {
		return fmt.Errorf("serve-after must be between 0 and %s", MaxServeAfter)
	}
	return nil
}

// runSpec is the single place the CLI turns into a run spec, so validation
// and execution can never disagree about what was asked for.
func runSpec(cfg config) (livebot.LiveRunSpec, error) {
	mix, err := parseActionMix(cfg.actionMix)
	if err != nil {
		return livebot.LiveRunSpec{}, err
	}
	return livebot.LiveRunSpec{
		RunID:           cfg.runID,
		Environment:     livebot.Environment(cfg.environment),
		Players:         cfg.players,
		PlayerStart:     cfg.playerStart,
		Duration:        cfg.duration,
		ArrivalRate:     cfg.arrivalRate,
		MaxOnline:       cfg.maxOnline,
		Seed:            cfg.seed,
		Scenario:        cfg.scenario,
		SessionDuration: cfg.sessionDuration,
		ThinkTime:       cfg.thinkTime,
		SessionActions:  cfg.sessionActions,
		ActionMix:       mix,
	}, nil
}

func parseActionMix(raw string) (map[string]float64, error) {
	if raw == "" {
		return nil, nil
	}
	mix := make(map[string]float64)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		action, weightText, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("action-mix entry %q must be action=weight", entry)
		}
		action = strings.TrimSpace(action)
		switch action {
		case "plant", "care", "theft", "harvest":
		default:
			return nil, fmt.Errorf("action-mix action %q must be plant, care, theft, or harvest", action)
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(weightText), 64)
		if err != nil {
			return nil, fmt.Errorf("action-mix weight for %q is not a number", action)
		}
		if _, duplicate := mix[action]; duplicate {
			return nil, fmt.Errorf("action-mix action %q appears twice", action)
		}
		mix[action] = weight
	}
	if len(mix) == 0 {
		return nil, errors.New("action-mix must contain at least one action")
	}
	return mix, nil
}

func validateEndpoint(raw, name string, allowNonLoopback bool, schemes ...string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("%s is required", name)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return fmt.Errorf("%s must be an absolute URL without userinfo", name)
	}
	supported := false
	for _, scheme := range schemes {
		if strings.EqualFold(u.Scheme, scheme) {
			supported = true
			break
		}
	}
	if !supported {
		return fmt.Errorf("%s has unsupported scheme", name)
	}
	if !allowNonLoopback && !isLoopbackHost(u.Hostname()) {
		return fmt.Errorf("%s must target loopback unless --allow-non-loopback is set", name)
	}
	return nil
}

func validateListen(raw string, allowNonLoopback bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("listen is required")
	}
	host, portText, err := net.SplitHostPort(raw)
	if err != nil {
		return errors.New("listen must be host:port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return errors.New("listen port must be between 1 and 65535")
	}
	if !allowNonLoopback && !isLoopbackHost(host) {
		return errors.New("listen must be loopback unless --allow-non-loopback is set")
	}
	return nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loadSecret(envName string) ([]byte, error) {
	if !environmentNamePattern.MatchString(envName) {
		return nil, errors.New("load-secret-env must be an environment variable name")
	}
	value, ok := os.LookupEnv(envName)
	if !ok || len(value) == 0 {
		return nil, fmt.Errorf("secret environment variable %q is missing or empty", envName)
	}
	return []byte(value), nil
}

// startProfiles writes the run's profiles into a directory, and returns the
// function that closes them. It is off unless asked for: block and mutex
// sampling cost something on every synchronization event, which is exactly
// the traffic a saturated driver has most of.
//
// The contention profiles matter more here than the CPU one. A driver that
// reports low CPU while failing to deliver its planned load is waiting, not
// computing, and waiting appears only in these two.
func startProfiles(directory string) (func() error, error) {
	if directory == "" {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create profile directory: %w", err)
	}
	cpuFile, err := os.Create(filepath.Join(directory, "cpu.pprof"))
	if err != nil {
		return nil, fmt.Errorf("create cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		return nil, fmt.Errorf("start cpu profile: %w", err)
	}
	// Sample every blocking event and every contended lock. Anything sparser
	// risks missing a hot lock that is held briefly but taken constantly,
	// which is the shape this is looking for.
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)

	return func() error {
		pprof.StopCPUProfile()
		closeErr := cpuFile.Close()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
		for _, name := range []string{"mutex", "block", "goroutine", "heap"} {
			if err := writeProfile(directory, name); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		return closeErr
	}, nil
}

func writeProfile(directory, name string) error {
	profile := pprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("profile %q is not registered", name)
	}
	file, err := os.Create(filepath.Join(directory, name+".pprof"))
	if err != nil {
		return fmt.Errorf("create %s profile: %w", name, err)
	}
	defer file.Close()
	if err := profile.WriteTo(file, 0); err != nil {
		return fmt.Errorf("write %s profile: %w", name, err)
	}
	return nil
}

func runConfig(cfg config, stdout io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	secret, err := loadSecret(cfg.loadSecretEnv)
	if err != nil {
		return err
	}
	var tlsCA []byte
	if cfg.tlsCAPath != "" {
		tlsCA, err = os.ReadFile(cfg.tlsCAPath)
		if err != nil {
			return fmt.Errorf("read TLS CA: %w", err)
		}
	}
	client, err := farmbotclient.NewClient(farmbotclient.Config{
		HTTPBaseURL: cfg.httpURL,
		WSURL:       cfg.wsURL,
		TLSCA:       tlsCA,
		ClientBuild: cfg.clientBuild,
		// An online bot must follow the realtime stream to know which plot is
		// worth acting on next. Journey walks more than one command even as a
		// single pass, so it needs the same view; a one-shot plant/harvest
		// does not.
		TrackFarmView:       cfg.sessionDuration > 0 || cfg.scenario == "journey",
		MaxIdleConnsPerHost: loginPoolSize(cfg.maxOnline),
		OperationResultPoll: cfg.resultPoll,
	})
	if err != nil {
		return err
	}
	actionStartAt := time.Time{}
	businessStartAt := time.Time{}
	barrierBase := time.Now()
	if cfg.actionWarmup > 0 {
		actionStartAt = barrierBase.Add(cfg.actionWarmup)
	}
	if cfg.businessWarmup > 0 {
		businessStartAt = barrierBase.Add(cfg.businessWarmup)
	}
	executor, err := livefarmbot.NewExecutorWithOptions(client, livefarmbot.SharedHMACCredentials(secret), livefarmbot.Options{
		TargetFarmID:      cfg.targetFarmID,
		TargetFarms:       cfg.targetFarms,
		ActionStartAt:     actionStartAt,
		BusinessStartAt:   businessStartAt,
		UnlockAllPlots:    cfg.unlockPlots,
		UnlockPlotLimit:   cfg.unlockPlotLimit,
		UnlockConcurrency: cfg.unlockConcurrency,
	})
	if err != nil {
		return err
	}
	store, err := demoorchestrator.NewEventStoreChecked(cfg.eventCapacity, demokit.EvidenceModeLive)
	if err != nil {
		return err
	}
	spec, err := runSpec(cfg)
	if err != nil {
		return err
	}
	handler := demoorchestrator.NewHTTPHandler(demoorchestrator.NewLiveEvidence(store))
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return errors.New("listen failed")
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	shutdown := func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), defaultShutdownWait)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}
	defer shutdown()

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner := livebot.NewRunner(store, executor)
	stopProfiles, err := startProfiles(cfg.profileDir)
	if err != nil {
		return err
	}
	// The driver samples itself for the whole run. Latency is only a
	// statement about the server while the process reporting it had headroom.
	probe := selfprobe.Start(selfprobe.DefaultInterval)
	result, err := runner.Run(signalContext, spec)
	driver := probe.Stop()
	if profileErr := stopProfiles(); profileErr != nil && err == nil {
		err = profileErr
	}
	if err != nil {
		return err
	}
	if err := writeSummary(stdout, result, driver); err != nil {
		return err
	}
	if cfg.serveAfter > 0 {
		timer := time.NewTimer(cfg.serveAfter)
		select {
		case <-timer.C:
		case <-signalContext.Done():
			stopTimer(timer)
		}
	}
	select {
	case serveErr := <-serveErrors:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.New("read-only controller stopped unexpectedly")
		}
	default:
	}
	return nil
}

type summaryDocument struct {
	RunID         string            `json:"run_id"`
	Status        livebot.RunStatus `json:"status"`
	Execution     string            `json:"execution"`
	CapacityClaim bool              `json:"capacity_claim"`
	Summary       livebot.Summary   `json:"summary"`
	// Latency, Phases and Throughput are the derived read of the same
	// histograms. A load run is judged on percentiles, so the summary states
	// them instead of leaving every reader to interpolate buckets.
	Latency livebot.LatencySummary            `json:"latency"`
	Phases  map[string]livebot.LatencySummary `json:"phases,omitempty"`
	// Load must be read before the latency figures: percentiles from a step
	// that failed to deliver its planned load describe a slow client.
	Load       livebot.LoadAccount `json:"load"`
	Throughput throughputDocument  `json:"throughput"`
	// Driver must be read before both. A saturated load generator delays its
	// own measurements, so its headroom decides whether this step measured a
	// server at all.
	Driver driverDocument `json:"driver"`
	// Qualified is the whole step's verdict: the driver had headroom and the
	// run delivered the load it planned.
	Qualified  bool      `json:"qualified"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// The driver gates match the load-generator qualification the capacity plan
// already applies. Scheduling latency is the addition: it is the runtime
// saying how long a runnable goroutine waited for a processor, which is
// exactly the error bar on every latency this process reports.
const (
	maxDriverCPUPercent  = 70.0
	maxArrivalLagP99Ms   = 50.0
	maxSchedLatencyP99Ms = 10.0
)

type driverDocument struct {
	Self selfprobe.Report `json:"self"`
	// ArrivalLagP99Ms is how late the scheduler launched bots against its own
	// arrival plan.
	ArrivalLagP99Ms float64 `json:"arrival_lag_p99_ms"`
	Qualified       bool    `json:"qualified"`
	// DisqualifiedBy names every gate the driver failed, so a ramp step says
	// what ran out rather than only that something did.
	DisqualifiedBy []string `json:"disqualified_by,omitempty"`
}

func qualifyDriver(report selfprobe.Report, arrivalLagP99Ms float64) driverDocument {
	document := driverDocument{Self: report, ArrivalLagP99Ms: arrivalLagP99Ms}
	if report.CPUPercentMax > maxDriverCPUPercent {
		document.DisqualifiedBy = append(document.DisqualifiedBy, "cpu_percent_max")
	}
	if arrivalLagP99Ms > maxArrivalLagP99Ms {
		document.DisqualifiedBy = append(document.DisqualifiedBy, "arrival_lag_p99_ms")
	}
	if report.SchedLatencyP99MsMax > maxSchedLatencyP99Ms {
		document.DisqualifiedBy = append(document.DisqualifiedBy, "sched_latency_p99_ms_max")
	}
	document.Qualified = len(document.DisqualifiedBy) == 0
	return document
}

// loginPoolSize keeps one pooled login connection per concurrent bot, within
// bounds. Sizing it to the run is what stops the login burst from becoming
// the driver's first bottleneck.
func loginPoolSize(maxOnline int) int {
	const minimumPool, maximumPool = 64, 4096
	if maxOnline < minimumPool {
		return minimumPool
	}
	if maxOnline > maximumPool {
		return maximumPool
	}
	return maxOnline
}

type throughputDocument struct {
	Seconds float64 `json:"seconds"`
	// ActionsPerSecond is the command rate, which is the load the server saw.
	ActionsPerSecond float64 `json:"actions_per_second"`
	// PlannedPerSecond and TurnsPerSecond are the offered and achieved rates
	// in turns, the unit the cadence is set in. They are the pair to compare;
	// comparing either against the command rate compares different things,
	// because one turn can cost more than one command.
	PlannedPerSecond float64 `json:"planned_per_second"`
	TurnsPerSecond   float64 `json:"turns_per_second"`
	OnlinePeak       int     `json:"online_peak"`
}

func writeSummary(writer io.Writer, result livebot.RunResult, driverReport selfprobe.Report) error {
	phases := result.Summary.PhaseReports()
	load := result.Summary.Load()
	driver := qualifyDriver(driverReport, phases[livebot.PhaseArrival].P99Ms)
	return json.NewEncoder(writer).Encode(summaryDocument{
		RunID:         result.RunID,
		Status:        result.Status,
		Execution:     result.Execution,
		CapacityClaim: false,
		Summary:       result.Summary,
		Latency:       result.Summary.Latency.Report(),
		Phases:        phases,
		Load:          load,
		Throughput:    throughput(result),
		Driver:        driver,
		Qualified:     driver.Qualified && (load.Qualified || load.Planless),
		StartedAt:     result.StartedAt,
		FinishedAt:    result.FinishedAt,
	})
}

func throughput(result livebot.RunResult) throughputDocument {
	document := throughputDocument{OnlinePeak: result.Summary.OnlinePeak}
	elapsed := result.FinishedAt.Sub(result.StartedAt)
	if elapsed <= 0 {
		return document
	}
	document.Seconds = elapsed.Seconds()
	document.ActionsPerSecond = float64(result.Summary.Actions) / document.Seconds
	document.PlannedPerSecond = float64(result.Summary.Planned) / document.Seconds
	document.TurnsPerSecond = float64(result.Summary.Turns) / document.Seconds
	return document
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
