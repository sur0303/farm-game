package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	redisrouteplan "farmgame/internal/storage/redis/routeplan"
	"farmgame/internal/testkit/routecontrol"
)

// Owned Transfer EVALSHA identity for S3 reply-loss. Must match the production
// ScriptTransferShardAuthority golden; harness only arms this exact SHA.
const transferShardAuthorityScriptSHA = "7cdfba89a030b50df036d390339704c88b88cc2f"

// Production ShardWriteAuthority caps lease TTL at 60s (DefaultLeaseLimits.MaxTTL).
// Plan TTL remains independently configurable and stays at 120s for RT2Q.
const (
	harnessAuthorityTTL      = "60s"
	harnessPlanTTL           = "120s"
	harnessOperationTimeout  = "30s"
	harnessLeaseSafetyMargin = "10s"
	// The scheduler's first healthy renew is half this interval. Keep that first
	// ordinary renew outside the bounded migration/Verify window; deadline-aware
	// scheduling still wakes no later than the 10s safety margin.
	harnessLeaseRenewInterval = "40s"
)

type binaryFact struct {
	Name          string `json:"name"`
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
	GoVersion     string `json:"go_version"`
	VCSRevision   string `json:"vcs_revision"`
	VCSModified   string `json:"vcs_modified"`
	VCS           string `json:"vcs"`
	MainPath      string `json:"main_path"`
	RuntimeTarget string `json:"runtime_target"`
	path          string
}

type runtimeConfigFact struct {
	Schema              string   `json:"schema"`
	Challenge           string   `json:"challenge"`
	Realm               string   `json:"realm"`
	KeyspaceFingerprint string   `json:"keyspace_fingerprint"`
	ConfigSetSHA256     string   `json:"config_set_sha256"`
	GatePeerSetSHA256   string   `json:"gate_peer_set_sha256"`
	Scenarios           []string `json:"scenarios"`
	Runtime             string   `json:"runtime"`
	RedisMode           string   `json:"redis_mode"`
	AuthorityTTL        string   `json:"authority_ttl"`
	PlanTTL             string   `json:"plan_ttl"`
	OperationTimeout    string   `json:"operation_timeout"`
}

type managedProcess struct {
	name     string
	role     string
	binary   binaryFact
	process  *routecontrol.Process
	logFile  *os.File
	identity routecontrol.ProcessIdentity
	started  time.Time
	stopped  bool
}

type processPorts struct {
	logic    string
	health   string
	holder   string
	downlink string
	control  string
	verify   string
	http     string
}

type holderSpec struct {
	name         string
	domain       string
	letter       string
	id           string
	boot         string
	logicDNS     string
	holderDNS    string
	ports        processPorts
	logicListen  string // farm process bind; may differ from advertised ports.logic
	logicProxy   *routecontrol.TCPFaultProxy
	routeClient  routecontrol.CertificateFiles
	holderServer routecontrol.CertificateFiles
	holderProxy  *routecontrol.HolderForwardProxy
	process      *managedProcess
}

type gateSpec struct {
	name           string
	dns            string
	ports          processPorts
	routeClient    routecontrol.CertificateFiles
	verifierServer routecontrol.CertificateFiles
	snapshotProxy  *routecontrol.SnapshotForwardProxy
	verifierProxy  *routecontrol.GateVerifierForwardProxy
	process        *managedProcess
}

type runRuntime struct {
	ctx       context.Context
	cfg       runConfig
	worktree  string
	challenge string
	runID     string
	scenarios []string
	writer    *routecontrol.TrustedRunWriter
	harness   routecontrol.ProcessIdentity
	tempRoot  string
	binaries  map[string]binaryFact
	processes []*managedProcess
	procMu    sync.Mutex

	realm       string
	environment string
	zone        string
	keyspace    string
	secret      string

	redisContainer string
	redisNetwork   string
	redisData      string
	redisBackend   string
	redisProxy     *routecontrol.RESPProxy
	redisURLs      map[string]string

	trust          *routecontrol.TrustBundle
	routeServer    routecontrol.CertificateFiles
	routeServerDNS string
	operator       routecontrol.CertificateFiles
	holderClient   routecontrol.CertificateFiles
	gateClient     routecontrol.CertificateFiles

	holders         []*holderSpec
	gates           []*gateSpec
	workerRoute     routecontrol.CertificateFiles
	workerSnapshot  *routecontrol.SnapshotForwardProxy
	workerProcess   *managedProcess
	workerHealth    string
	workerBootGen   int
	controller      *managedProcess
	controllerAddr  string
	controllerID    string
	controllerBoots []string
	controllerIndex int
	snapshotProxies map[string]*routecontrol.SnapshotForwardProxy
	bootstrapOutput map[string]any
}

func executeRun(cfg runConfig) (routecontrol.TrustedManifest, routecontrol.OracleResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.overallTimeout)
	defer cancel()
	worktree, err := filepath.Abs(cfg.worktree)
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	binding, err := routecontrol.CurrentTrustedGitBinding(worktree, cfg.expectedHead)
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	challenge, err := routecontrol.NewRunChallenge()
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	scenarios := scenariosFor(cfg)
	executable, err := os.Executable()
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	collectorSHA, _, err := routecontrol.SHA256File(executable)
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	runID := filepath.Base(filepath.Clean(cfg.artifactRoot))
	if runID == "." || runID == string(filepath.Separator) {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, errors.New("artifact root must have a stable run basename")
	}
	secret := "rt2q-" + challenge + "-private-material"
	redactor := routecontrol.NewRedactor(secret)
	if cfg.redisMode == "company" {
		redactor = routecontrol.NewRedactor(secret, os.Getenv(cfg.redisURLEnv))
	}
	runtime := &runRuntime{
		ctx: ctx, cfg: cfg, worktree: worktree, challenge: challenge, runID: runID, scenarios: scenarios,
		binaries: make(map[string]binaryFact), redisURLs: make(map[string]string), secret: secret,
		realm: "rt2q-" + challenge, environment: "rt2q" + challenge[:12], zone: "z" + challenge[12:24],
		controllerID: "controller-" + challenge, controllerBoots: []string{"c0-" + challenge, "c1-" + challenge, "c2-" + challenge},
		snapshotProxies: make(map[string]*routecontrol.SnapshotForwardProxy),
	}
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: runtime.environment,
		NamespaceZone:        runtime.zone,
		PhysicalProfile:      "redis-physical-v1",
		AuthorityLayout:      "v1",
		WorkflowLayout:       "v1",
		WorkflowHashVersion:  "1",
		WorkflowBucketCount:  "256",
	})
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	runtime.keyspace, err = tuple.Fingerprint()
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	writer, err := routecontrol.NewTrustedRunWriter(routecontrol.TrustedRunOptions{
		Root: cfg.artifactRoot, TaskID: taskID, RunID: runID, Challenge: challenge, Level: cfg.level,
		Runtime: cfg.runtime, RedisMode: cfg.redisMode, Scenarios: scenarios, Git: binding,
		Collector: "rt2q-route-control-harness", CollectorSHA256: collectorSHA, Keyspace: runtime.keyspace,
		Redactor: redactor,
	})
	if err != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, err
	}
	runtime.writer = writer
	runtime.harness = routecontrol.ProcessIdentity{Name: "rt2q-harness", Role: "trusted-observer", PID: os.Getpid(), BinarySHA256: collectorSHA, StartIdentity: fmt.Sprintf("pid-%d-run-%s", os.Getpid(), challenge)}
	runtimeErr := runtime.execute()
	cleanupErr := runtime.shutdownAndCleanup()
	runtimeErr = errors.Join(runtimeErr, cleanupErr)
	bindings := runtime.bindings(collectorSHA)
	oracle, finalizeErr := writer.Finalize(bindings, nil)
	if finalizeErr != nil {
		return routecontrol.TrustedManifest{}, routecontrol.OracleResult{}, errors.Join(runtimeErr, finalizeErr)
	}
	manifest, verified, verifyErr := routecontrol.VerifyTrustedRun(cfg.artifactRoot, cfg.expectedHead)
	if verifyErr != nil {
		return routecontrol.TrustedManifest{}, oracle, errors.Join(runtimeErr, verifyErr)
	}
	return manifest, verified, runtimeErr
}

func scenariosFor(cfg runConfig) []string {
	if cfg.redisMode == "company" {
		return []string{"S7"}
	}
	if cfg.scenario != "all" {
		return []string{cfg.scenario}
	}
	return []string{"S0", "S1", "S2", "S3", "S4", "S5", "S6"}
}

func (r *runRuntime) execute() error {
	tempRoot, err := os.MkdirTemp("", "rt2q-final-"+r.challenge[:8]+"-")
	if err != nil {
		return err
	}
	r.tempRoot = tempRoot
	if err := os.Chmod(tempRoot, 0o700); err != nil {
		return err
	}
	if err := r.buildBinaries(); err != nil {
		return err
	}
	if err := r.startRedisBoundary(); err != nil {
		return err
	}
	if err := r.generateTrust(); err != nil {
		return err
	}
	if err := verifyDisjointProcessPortRange(); err != nil {
		return err
	}
	if err := r.allocateTopology(); err != nil {
		return err
	}
	if err := r.startForwardProxies(); err != nil {
		return err
	}
	if err := r.writeStaticArtifacts(); err != nil {
		return err
	}
	// First publish only the production initial placeholder snapshot. Holders
	// must establish their authenticated Watches, bind their logic endpoints,
	// register, and report the supported zero-shard Ready state before any
	// Authority generation is claimed for them.
	if err := r.bootstrap(true); err != nil {
		return err
	}
	if err := r.startController(); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, r.controllerAddr, 30*time.Second); err != nil {
		return errors.New("production route-controller did not open its listener")
	}
	if err := r.startConsumers(); err != nil {
		return err
	}
	if err := r.waitConsumersReady(30 * time.Second); err != nil {
		return err
	}
	// Preserve the single-controller/single-Manager contract: stop C0 before
	// the one-shot production Manager helper creates initial generations. C1
	// then serves the resulting complete snapshot and exercises the mandatory
	// authenticated Boot-rollover resync for every already-running consumer.
	if err := r.stopManaged(r.controller, 10*time.Second); err != nil {
		return err
	}
	r.controllerIndex++
	if err := r.bootstrap(false); err != nil {
		return err
	}
	if err := r.startController(); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, r.controllerAddr, 30*time.Second); err != nil {
		return errors.New("assignment route-controller did not open its listener")
	}
	if err := r.waitConsumersReady(45 * time.Second); err != nil {
		return err
	}
	if err := r.waitControllerReady(30 * time.Second); err != nil {
		return err
	}
	for _, scenario := range r.scenarios {
		var err error
		switch scenario {
		case "S0":
			err = r.runS0()
		case "S1":
			err = r.runS1()
		case "S2":
			err = r.runS2()
		case "S3":
			err = r.runS3()
		case "S4":
			err = r.runS4()
		case "S5":
			err = r.runS5()
		case "S6":
			err = r.runS6()
		case "S7":
			err = fmt.Errorf("scenario %s runtime is not yet established", scenario)
		case "deadline-refresh":
			err = r.runDeadlineRefresh()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *runRuntime) buildBinaries() error {
	binDir := filepath.Join(r.tempRoot, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		return err
	}
	sourceDir := filepath.Join(r.tempRoot, "source")
	if err := runCommand(r.ctx, r.tempRoot, "git", "clone", "--local", "--no-hardlinks", "--no-checkout", r.worktree, sourceDir); err != nil {
		return fmt.Errorf("create isolated exact-head build source: %w", err)
	}
	if err := runCommand(r.ctx, sourceDir, "git", "checkout", "--detach", r.cfg.expectedHead); err != nil {
		return fmt.Errorf("detach isolated exact-head build source: %w", err)
	}
	if _, err := routecontrol.CurrentTrustedGitBinding(sourceDir, r.cfg.expectedHead); err != nil {
		return errors.New("isolated production build source lacks exact clean detached binding")
	}
	targets := []struct{ name, pkg string }{
		{"route-controller", "./cmd/route-controller"}, {"routectl", "./cmd/routectl"},
		{"player-svr", "./cmd/player-svr"}, {"farm-svr", "./cmd/farm-svr"},
		{"farm-worker", "./cmd/farm-worker"}, {"farm-gate", "./cmd/farm-gate"},
		{"rt2q-route-bootstrap", "./cmd/rt2q-route-bootstrap"}, {"rt2q-business-driver", "./cmd/rt2q-business-driver"},
	}
	for _, target := range targets {
		path := filepath.Join(binDir, target.name)
		command := exec.CommandContext(r.ctx, "go", "build", "-buildvcs=true", "-trimpath", "-o", path, target.pkg)
		command.Dir = sourceDir
		var stderr bytes.Buffer
		command.Stderr = &stderr
		if err := command.Run(); err != nil {
			return fmt.Errorf("build %s: %w", target.name, err)
		}
		fact, err := inspectBinary(target.name, path, r.cfg.expectedHead, r.cfg.runtime)
		if err != nil {
			return err
		}
		r.binaries[target.name] = fact
	}
	return nil
}

func inspectBinary(name, path, expectedHead, runtimeName string) (binaryFact, error) {
	digest, size, err := routecontrol.SHA256File(path)
	if err != nil {
		return binaryFact{}, err
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return binaryFact{}, err
	}
	fact := binaryFact{Name: name, SHA256: digest, Bytes: size, GoVersion: info.GoVersion, MainPath: info.Path, RuntimeTarget: runtimeName, path: path}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs":
			fact.VCS = setting.Value
		case "vcs.revision":
			fact.VCSRevision = setting.Value
		case "vcs.modified":
			fact.VCSModified = setting.Value
		}
	}
	if fact.VCSRevision != expectedHead || fact.VCSModified != "false" || fact.VCS != "git" {
		return binaryFact{}, fmt.Errorf("binary %s lacks exact clean VCS binding", name)
	}
	return fact, nil
}

func (r *runRuntime) startRedisBoundary() error {
	var rawURL string
	if r.cfg.redisMode == "company" {
		rawURL = strings.TrimSpace(os.Getenv(r.cfg.redisURLEnv))
		if rawURL == "" {
			return errors.New("company Redis environment is unavailable")
		}
	} else {
		r.redisContainer = "rt2q-redis-" + r.challenge[:12]
		r.redisNetwork = "rt2q-net-" + r.challenge[:12]
		r.redisData = filepath.Join(r.tempRoot, "redis-data")
		if err := os.Mkdir(r.redisData, 0o700); err != nil {
			return err
		}
		if err := runCommand(r.ctx, r.worktree, "docker", "network", "create", r.redisNetwork); err != nil {
			return err
		}
		if err := runCommand(r.ctx, r.worktree, "docker", "run", "-d", "--rm", "--name", r.redisContainer,
			"--network", r.redisNetwork, "-p", "127.0.0.1::6379", "-v", r.redisData+":/data",
			"redis:7.4-alpine", "redis-server", "--appendonly", "yes", "--appendfsync", "always", "--save", ""); err != nil {
			return err
		}
		command := exec.CommandContext(r.ctx, "docker", "port", r.redisContainer, "6379/tcp")
		raw, err := command.Output()
		if err != nil {
			return err
		}
		address := strings.TrimSpace(string(raw))
		if _, _, err := net.SplitHostPort(address); err != nil {
			return errors.New("isolated Redis did not expose a canonical loopback port")
		}
		rawURL = "redis://" + address + "/0"
	}
	r.redisBackend = rawURL
	proxy, err := routecontrol.NewRESPProxy(rawURL, r.challenge)
	if err != nil {
		return err
	}
	r.redisProxy = proxy
	for _, label := range []string{"bootstrap", "controller", "business", "observer", "consumer"} {
		value, err := proxy.Listen(label)
		if err != nil {
			return err
		}
		r.redisURLs[label] = value
	}
	options, _ := redis.ParseURL(r.redisURLs["observer"])
	client := redis.NewClient(options)
	defer client.Close()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(r.ctx, time.Second)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("Redis did not become functionally ready through the audit proxy")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (r *runRuntime) generateTrust() error {
	r.routeServerDNS = "route-" + r.challenge + ".rt2q.test"
	routeClients := []string{
		"player-a-route-" + r.challenge + ".rt2q.test", "player-b-route-" + r.challenge + ".rt2q.test",
		"farm-a-route-" + r.challenge + ".rt2q.test", "farm-b-route-" + r.challenge + ".rt2q.test",
		"worker-route-" + r.challenge + ".rt2q.test", "gate-1-route-" + r.challenge + ".rt2q.test",
		"gate-2-route-" + r.challenge + ".rt2q.test", "operator-" + r.challenge + ".rt2q.test",
	}
	holderServers := []string{
		"player-a-holder-" + r.challenge + ".rt2q.test", "player-b-holder-" + r.challenge + ".rt2q.test",
		"farm-a-holder-" + r.challenge + ".rt2q.test", "farm-b-holder-" + r.challenge + ".rt2q.test",
	}
	gateServers := []string{"gate-1-verify-" + r.challenge + ".rt2q.test", "gate-2-verify-" + r.challenge + ".rt2q.test"}
	bundle, err := routecontrol.GenerateTrustBundle(filepath.Join(r.tempRoot, "trust"), r.challenge, []routecontrol.TrustDomainSpec{
		{Name: "route", Servers: []string{r.routeServerDNS}, Clients: routeClients},
		{Name: "holder", Servers: holderServers, Clients: []string{"controller-holder-" + r.challenge + ".rt2q.test"}},
		{Name: "gate", Servers: gateServers, Clients: []string{"controller-gate-" + r.challenge + ".rt2q.test"}},
	}, time.Now())
	if err != nil {
		return err
	}
	r.trust = bundle
	r.routeServer, _ = bundle.Leaf("route", r.routeServerDNS)
	r.operator, _ = bundle.Leaf("route", routeClients[7])
	r.workerRoute, _ = bundle.Leaf("route", routeClients[4])
	r.holderClient, _ = bundle.Leaf("holder", "controller-holder-"+r.challenge+".rt2q.test")
	r.gateClient, _ = bundle.Leaf("gate", "controller-gate-"+r.challenge+".rt2q.test")
	return nil
}

func (r *runRuntime) allocateTopology() error {
	var err error
	r.controllerAddr, err = reserveAddress()
	if err != nil {
		return err
	}
	names := []struct{ domain, letter string }{{"PLAYER", "A"}, {"PLAYER", "B"}, {"FARM", "A"}, {"FARM", "B"}}
	for index, item := range names {
		logic, err := reserveAddress()
		if err != nil {
			return err
		}
		logicListen := logic
		var logicProxy *routecontrol.TCPFaultProxy
		if item.domain == "FARM" {
			// Advertise a TCP fault proxy so S6 can drop renew RPCs without killing farm-svr.
			upstream, upErr := reserveAddress()
			if upErr != nil {
				return upErr
			}
			logicListen = upstream
			proxy, proxyErr := routecontrol.StartTCPFaultProxy(logic, upstream)
			if proxyErr != nil {
				return proxyErr
			}
			logicProxy = proxy
		}
		health, _ := reserveAddress()
		holder, _ := reserveAddress()
		prefix := strings.ToLower(item.domain) + "-" + strings.ToLower(item.letter)
		routeLeaf, _ := r.trust.Leaf("route", prefix+"-route-"+r.challenge+".rt2q.test")
		holderDNS := prefix + "-holder-" + r.challenge + ".rt2q.test"
		holderLeaf, _ := r.trust.Leaf("holder", holderDNS)
		r.holders = append(r.holders, &holderSpec{
			name: prefix, domain: item.domain, letter: item.letter,
			id: prefix + "-" + r.challenge, boot: prefix + "-boot-" + r.challenge,
			logicDNS: logic, holderDNS: holderDNS, ports: processPorts{logic: logic, health: health, holder: holder},
			logicListen: logicListen, logicProxy: logicProxy,
			routeClient: routeLeaf, holderServer: holderLeaf,
		})
		_ = index
	}
	for index := 0; index < 2; index++ {
		httpAddress, _ := reserveAddress()
		downlink, _ := reserveAddress()
		control, _ := reserveAddress()
		verify, _ := reserveAddress()
		routeLeaf, _ := r.trust.Leaf("route", fmt.Sprintf("gate-%d-route-%s.rt2q.test", index+1, r.challenge))
		verifyDNS := fmt.Sprintf("gate-%d-verify-%s.rt2q.test", index+1, r.challenge)
		verifyLeaf, _ := r.trust.Leaf("gate", verifyDNS)
		r.gates = append(r.gates, &gateSpec{name: fmt.Sprintf("gate-%d", index+1), dns: verifyDNS,
			ports: processPorts{http: httpAddress, downlink: downlink, control: control, verify: verify}, routeClient: routeLeaf, verifierServer: verifyLeaf})
	}
	return nil
}

func (r *runRuntime) startForwardProxies() error {
	for _, holder := range r.holders {
		proxy, err := routecontrol.StartHolderForwardProxy(routecontrol.ForwardTLSConfig{
			ServerCertFile: holder.holderServer.CertFile, ServerKeyFile: holder.holderServer.KeyFile, ClientCAFile: r.trust.CAs["holder"],
			AllowedClientIdentity: r.holderClient.DNSName, UpstreamTarget: holder.ports.holder, UpstreamServerName: holder.holderDNS,
			UpstreamClientCert: r.holderClient.CertFile, UpstreamClientKey: r.holderClient.KeyFile, UpstreamServerCA: r.trust.CAs["holder"],
		})
		if err != nil {
			return err
		}
		holder.holderProxy = proxy
	}
	consumerLeaves := make(map[string]routecontrol.CertificateFiles)
	for _, holder := range r.holders {
		consumerLeaves[holder.name] = holder.routeClient
	}
	consumerLeaves["worker"] = r.workerRoute
	for _, gate := range r.gates {
		consumerLeaves[gate.name] = gate.routeClient
	}
	for name, leaf := range consumerLeaves {
		proxy, err := routecontrol.StartSnapshotForwardProxy(routecontrol.ForwardTLSConfig{
			ServerCertFile: r.routeServer.CertFile, ServerKeyFile: r.routeServer.KeyFile, ClientCAFile: r.trust.CAs["route"],
			AllowedClientIdentity: leaf.DNSName, UpstreamTarget: r.controllerAddr, UpstreamServerName: r.routeServerDNS,
			UpstreamClientCert: leaf.CertFile, UpstreamClientKey: leaf.KeyFile, UpstreamServerCA: r.trust.CAs["route"],
		})
		if err != nil {
			return err
		}
		r.snapshotProxies[name] = proxy
	}
	for _, gate := range r.gates {
		gate.snapshotProxy = r.snapshotProxies[gate.name]
		proxy, err := routecontrol.StartGateVerifierForwardProxy(gate.name, routecontrol.ForwardTLSConfig{
			ServerCertFile: gate.verifierServer.CertFile, ServerKeyFile: gate.verifierServer.KeyFile, ClientCAFile: r.trust.CAs["gate"],
			AllowedClientIdentity: r.gateClient.DNSName, UpstreamTarget: gate.ports.verify, UpstreamServerName: gate.dns,
			UpstreamClientCert: r.gateClient.CertFile, UpstreamClientKey: r.gateClient.KeyFile, UpstreamServerCA: r.trust.CAs["gate"],
		})
		if err != nil {
			return err
		}
		gate.verifierProxy = proxy
	}
	r.workerSnapshot = r.snapshotProxies["worker"]
	return nil
}

func (r *runRuntime) writeStaticArtifacts() error {
	facts := make([]binaryFact, 0, len(r.binaries))
	for _, fact := range r.binaries {
		fact.path = ""
		facts = append(facts, fact)
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].Name < facts[j].Name })
	if err := r.writer.WriteJSON("binaries.json", map[string]any{"schema": "rt2q-binaries/v1", "binaries": facts, "redis_image": "redis:7.4-alpine"}); err != nil {
		return err
	}
	gateSet := make([]string, 0, len(r.gates))
	for _, gate := range r.gates {
		gateSet = append(gateSet, gate.name+"|"+gate.dns+"|"+gate.ports.verify)
	}
	configValues := harnessConfigValues(r.realm, r.environment, r.zone, r.cfg.runtime, r.cfg.redisMode)
	configDigest := routecontrol.SHA256CanonicalSet("rt2q-config/v1", configValues)
	gateDigest := routecontrol.SHA256CanonicalSet("rt2q-gate-peers/v1", gateSet)
	return r.writer.WriteJSON("configs.json", runtimeConfigFact{Schema: "rt2q-runtime-config/v1", Challenge: r.challenge, Realm: r.realm,
		KeyspaceFingerprint: r.keyspace, ConfigSetSHA256: configDigest, GatePeerSetSHA256: gateDigest,
		Scenarios: r.scenarios, Runtime: r.cfg.runtime, RedisMode: r.cfg.redisMode,
		AuthorityTTL: harnessAuthorityTTL, PlanTTL: harnessPlanTTL, OperationTimeout: harnessOperationTimeout})
}

func (r *runRuntime) bootstrap(bootstrapOnly bool) error {
	bootIndex := r.controllerIndex
	if bootIndex < 0 || bootIndex >= len(r.controllerBoots) {
		return errors.New("bootstrap controller Boot is unavailable")
	}
	args := []string{"--challenge", r.challenge, "--realm", r.realm, "--controller-id", r.controllerID, "--controller-boot", r.controllerBoots[bootIndex],
		"--redis-environment", r.environment, "--redis-zone", r.zone}
	name, kind := "bootstrap-assign", "assignment_complete"
	if bootstrapOnly {
		args = append(args, "--bootstrap-only")
		name, kind = "bootstrap-init", "bootstrap_complete"
	}
	for _, holder := range r.holders {
		prefix := "--" + holder.name
		args = append(args, prefix, holder.id, prefix+"-boot", holder.boot, prefix+"-endpoint", holder.ports.logic)
	}
	process, err := r.startProcess(name, "production-bootstrap-helper", r.binaries["rt2q-route-bootstrap"], args,
		[]string{"RT2Q_BOOTSTRAP_REDIS_URL=" + r.redisURLs["bootstrap"]})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	result, err := process.process.Wait(ctx)
	process.stopped = true
	_ = process.logFile.Sync()
	if err != nil || result.ExitCode != 0 {
		return errors.New("production bootstrap helper failed")
	}
	raw, err := os.ReadFile(process.logFile.Name())
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&r.bootstrapOutput); err != nil {
		return errors.New("production bootstrap helper did not emit typed output")
	}
	return r.writer.Observe("snapshots.ndjson", r.scenarios[0], kind, process.identity, r.bootstrapOutput)
}

func (r *runRuntime) startController() error {
	boot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	allowed := make([]string, 0, len(r.snapshotProxies))
	for _, holder := range r.holders {
		allowed = append(allowed, holder.routeClient.DNSName)
	}
	allowed = append(allowed, r.workerRoute.DNSName)
	for _, gate := range r.gates {
		allowed = append(allowed, gate.routeClient.DNSName)
	}
	peers := make([]map[string]string, 0, len(r.gates))
	for _, gate := range r.gates {
		address := gate.ports.verify
		if gate.verifierProxy != nil {
			address = gate.verifierProxy.Address()
		}
		peers = append(peers, map[string]string{"name": gate.name, "address": address, "server_name": gate.dns, "expected_identity": gate.dns})
	}
	peerJSON, _ := json.Marshal(peers)
	env := append(r.redisTupleEnv(),
		"FARM_REDIS_URL="+r.redisURLs["controller"], "ROUTE_CONTROLLER_LISTEN="+r.controllerAddr,
		"ROUTE_CONTROLLER_ID="+r.controllerID, "ROUTE_CONTROLLER_BOOT="+boot, "ROUTE_CONTROLLER_REALM="+r.realm,
		"ROUTE_CONTROLLER_SINGLE_CONTROLLER_ATTESTATION=single-replica-recreate-no-overlap-v1",
		"ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES="+strings.Join(allowed, ","),
		"ROUTE_CONTROLLER_PLAN_READ_IDENTITIES="+r.operator.DNSName, "ROUTE_CONTROLLER_OPERATOR_IDENTITIES="+r.operator.DNSName,
		"ROUTE_CONTROLLER_TLS_CERT="+r.routeServer.CertFile, "ROUTE_CONTROLLER_TLS_KEY="+r.routeServer.KeyFile, "ROUTE_CONTROLLER_TLS_CA="+r.trust.CAs["route"],
		"ROUTE_CONTROLLER_HOLDER_CLIENT_CERT="+r.holderClient.CertFile, "ROUTE_CONTROLLER_HOLDER_CLIENT_KEY="+r.holderClient.KeyFile, "ROUTE_CONTROLLER_HOLDER_CLIENT_CA="+r.trust.CAs["holder"],
		"ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CERT="+r.gateClient.CertFile, "ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_KEY="+r.gateClient.KeyFile, "ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CA="+r.trust.CAs["gate"],
		"ROUTE_CONTROLLER_GATE_VERIFIER_PEERS="+string(peerJSON), "ROUTE_CONTROLLER_GATE_VERIFIER_TIMEOUT=5s",
		"ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT="+harnessOperationTimeout, "ROUTE_CONTROLLER_PLAN_TTL="+harnessPlanTTL,
		"ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL=1s", "ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD=30s", "ROUTE_CONTROLLER_REDIS_OPERATION_TIMEOUT=2s",
	)
	process, err := r.startProcess("controller-"+strconv.Itoa(r.controllerIndex), "production-route-controller", r.binaries["route-controller"], nil, env)
	if err != nil {
		return err
	}
	r.controller = process
	return nil
}

func (r *runRuntime) startConsumers() error {
	for _, holder := range r.holders {
		if err := r.startHolder(holder); err != nil {
			return err
		}
		// Holder roots bind the business listener before endpoint-pool startup
		// completes. Waiting only for that real listener avoids a circular pair
		// startup while still staggering HolderRegistry registration churn.
		if err := waitTCPListener(r.ctx, holder.ports.logic, 15*time.Second); err != nil {
			return fmt.Errorf("production holder %s did not bind its logic listener", holder.name)
		}
	}
	if err := r.startWorker(); err != nil {
		return err
	}
	for _, gate := range r.gates {
		if err := r.startGate(gate); err != nil {
			return err
		}
	}
	return nil
}

func (r *runRuntime) startHolder(holder *holderSpec) error {
	prefix := holder.domain
	env := append(r.commonConsumerEnv(holder.routeClient, r.snapshotProxies[holder.name]),
		"ACTOR_AUTHORITY_MODE=authority", "ACTOR_HOLDER_POD="+holder.id, "ACTOR_HOLDER_BOOT_ID="+holder.boot)
	env = append(env, harnessAuthorityLeaseEnv()...)
	env = append(env,
		"HOLDER_AGENT_LEASE_TTL=30s", "HOLDER_CONTROL_CONTROLLER_IDENTITY="+r.holderClient.DNSName,
		"FARM_REDIS_URL="+r.redisURLs["consumer"], "PERMIT_SECRET="+r.secret, "THEFT_PRF_MODE=production", "THEFT_PRF_HMAC_SECRET="+r.secret,
	)
	if holder.domain == "PLAYER" {
		env = append(env, "PLAYER_LISTEN="+holder.ports.logic, "PLAYER_HEALTH_LISTEN="+holder.ports.health,
			"PLAYER_LOGIC_ENDPOINT="+holder.ports.logic, "PLAYER_HOLDER_ENDPOINT="+holder.holderProxy.Address(),
			"PLAYER_HOLDER_TLS_DNS_IDENTITY="+holder.holderDNS, "PLAYER_HOLDER_TLS_CERT_FILE="+holder.holderServer.CertFile,
			"PLAYER_HOLDER_TLS_KEY_FILE="+holder.holderServer.KeyFile, "PLAYER_HOLDER_CLIENT_CA_FILE="+r.trust.CAs["holder"],
			"PLAYER_HOLDER_CONTROL_LISTEN="+holder.ports.holder)
	} else {
		listen := holder.logicListen
		if listen == "" {
			listen = holder.ports.logic
		}
		env = append(env, "FARM_LISTEN="+listen, "FARM_HEALTH_LISTEN="+holder.ports.health,
			"FARM_LOGIC_ENDPOINT="+holder.ports.logic, "FARM_HOLDER_ENDPOINT="+holder.holderProxy.Address(),
			"FARM_HOLDER_TLS_DNS_IDENTITY="+holder.holderDNS, "FARM_HOLDER_TLS_CERT_FILE="+holder.holderServer.CertFile,
			"FARM_HOLDER_TLS_KEY_FILE="+holder.holderServer.KeyFile, "FARM_HOLDER_CLIENT_CA_FILE="+r.trust.CAs["holder"],
			"FARM_HOLDER_CONTROL_LISTEN="+holder.ports.holder, "GATEWAY_DOWNLINK_ADDR="+r.gates[0].ports.downlink,
			// Match Gate's short subscription lease so S6 lease-loss rejoin is bounded.
			"FARM_SUBSCRIPTION_LEASE_INITIAL_TTL=3s", "FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION=3s")
	}
	process, err := r.startProcess(holder.name, "production-"+strings.ToLower(prefix)+"-holder", r.binaries[strings.ToLower(prefix)+"-svr"], nil, env)
	if err != nil {
		return err
	}
	holder.process = process
	return nil
}

func (r *runRuntime) startWorker() error {
	bootID := fmt.Sprintf("worker-boot-%s-%d", r.challenge, r.workerBootGen)
	env := append(r.commonConsumerEnv(r.workerRoute, r.workerSnapshot),
		"ACTOR_AUTHORITY_MODE=authority", "FARM_REDIS_URL="+r.redisURLs["consumer"],
		"WORKER_ID=worker-"+r.challenge, "WORKER_BOOT_ID="+bootID, "WORKER_LEASE_TOKEN="+r.secret,
		// Keep production Theft recovery enabled, but bound DueIndex churn so the
		// shared isolated Redis backend remains available for controller Plan status.
		"THEFT_PAIR_DISCOVERY_MAX_BUCKETS=8", "THEFT_WORKER_POLL_INTERVAL=1s", "THEFT_CLAIM_LIMIT=1", "THEFT_DISCOVERY_LIMIT=1",
		"WORKER_POLL_INTERVAL=1s", "WORKER_CLAIM_LIMIT=1",
	)
	health, _ := reserveAddress()
	env = append(env, "WORKER_HEALTH_LISTEN="+health)
	process, err := r.startProcess("worker", "production-farm-worker", r.binaries["farm-worker"], nil, env)
	if err != nil {
		return err
	}
	r.workerProcess = process
	r.workerHealth = health
	return nil
}

func (r *runRuntime) startGate(gate *gateSpec) error {
	env := append(r.commonConsumerEnv(gate.routeClient, gate.snapshotProxy),
		"ACTOR_AUTHORITY_MODE=authority", "FARM_REDIS_URL="+r.redisURLs["business"], "PERMIT_SECRET="+r.secret,
		"LOAD_TEST_TOKEN_SECRET="+r.secret, "GATEWAY_LISTEN="+gate.ports.http, "GATEWAY_ID="+gate.name+"-"+r.challenge,
		"WSS_ENDPOINT=ws://"+gate.ports.http+"/ws", "GATE_TRANSPORT=websocket", "GATE_WS_METRICS=1", "GATE_WS_RATE_PER_SEC=1000", "GATE_WS_RATE_BURST=1000",
		"GATEWAY_DOWNLINK_LISTEN="+gate.ports.downlink, "GATE_CONTROL_LISTEN="+gate.ports.control,
		"GATE_BOOT_LEASE_PREFIX=rt2q:"+r.challenge+":"+gate.name+":boot", "GATE_ROUTE_LEASE_TTL=30s",
		"GATE_SUBSCRIPTION_LEASE_TTL=3s", "GATE_SUBSCRIPTION_LEASE_RENEW_INTERVAL=500ms", "GATE_SUBSCRIPTION_LEASE_RENEW_TIMEOUT=250ms",
		"GATE_VERIFIER_LISTEN="+gate.ports.verify, "GATE_VERIFIER_TLS_CERT="+gate.verifierServer.CertFile,
		"GATE_VERIFIER_TLS_KEY="+gate.verifierServer.KeyFile, "GATE_VERIFIER_CLIENT_CA="+r.trust.CAs["gate"],
		"GATE_VERIFIER_ALLOWED_CLIENT_IDENTITY="+r.gateClient.DNSName,
	)
	process, err := r.startProcess(gate.name, "production-farm-gate", r.binaries["farm-gate"], nil, env)
	if err != nil {
		return err
	}
	gate.process = process
	return nil
}

func (r *runRuntime) commonConsumerEnv(leaf routecontrol.CertificateFiles, proxy *routecontrol.SnapshotForwardProxy) []string {
	return append(r.redisTupleEnv(),
		"AUTHORITY_MEMBERSHIP_MODE=true", "ACTOR_ASSIGNMENT_MODE=remote-watch", "ACTOR_ROUTE_REALM="+r.realm,
		"ROUTE_CONTROL_ADDR="+proxy.Address(), "ROUTE_CONTROL_CONTROLLER_ID="+r.controllerID,
		"ROUTE_CONTROL_SERVER_NAME="+r.routeServerDNS, "ROUTE_CONTROL_SERVER_IDENTITY="+r.routeServerDNS,
		"ROUTE_CONTROL_CLIENT_CERT_FILE="+leaf.CertFile, "ROUTE_CONTROL_CLIENT_KEY_FILE="+leaf.KeyFile,
		"ROUTE_CONTROL_SERVER_CA_FILE="+r.trust.CAs["route"], "ROUTE_CONTROL_DIAL_TIMEOUT=5s",
		"ROUTE_CONTROL_RPC_TIMEOUT=5s", "ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT=10s", "ACTOR_ROUTE_STARTUP_TIMEOUT=30s",
		"ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS=32", "ACTOR_ROUTE_MAX_WATCH_FAILURES=32",
		"ACTOR_ROUTE_RETRY_BACKOFF=10ms", "ACTOR_ROUTE_MAX_RETRY_BACKOFF=100ms", "ROUTE_CONTROL_RETIRED_BOOT_LIMIT=8",
	)
}

func (r *runRuntime) redisTupleEnv() []string {
	return []string{
		"REDIS_NAMESPACE_ENVIRONMENT=" + r.environment, "REDIS_NAMESPACE_ZONE=" + r.zone,
		"REDIS_PHYSICAL_PROFILE_VERSION=redis-physical-v1", "REDIS_AUTHORITY_LAYOUT_VERSION=v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION=v1", "REDIS_WORKFLOW_HASH_VERSION=1", "REDIS_WORKFLOW_BUCKET_COUNT=256",
	}
}

func (r *runRuntime) startProcess(name, role string, binary binaryFact, args, env []string) (*managedProcess, error) {
	logDir := filepath.Join(r.tempRoot, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(logDir, name+"-"+strconv.FormatInt(time.Now().UnixNano(), 10)+".log"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	process, err := routecontrol.StartProcess(routecontrol.ProcessSpec{Command: binary.path, Args: args, Dir: r.worktree, Env: env, Stdout: logFile, Stderr: logFile})
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}
	started := time.Now().UTC()
	managed := &managedProcess{name: name, role: role, binary: binary, process: process, logFile: logFile, started: started,
		identity: routecontrol.ProcessIdentity{Name: name, Role: role, PID: process.PID(), BinarySHA256: binary.SHA256, StartIdentity: fmt.Sprintf("pid-%d-start-%d", process.PID(), started.UnixNano())}}
	r.procMu.Lock()
	r.processes = append(r.processes, managed)
	r.procMu.Unlock()
	if err := r.writer.Observe("processes.ndjson", r.scenarios[0], "process_started", managed.identity, map[string]any{"name": name, "role": role, "pid": process.PID()}); err != nil {
		return nil, err
	}
	return managed, nil
}

func (r *runRuntime) waitControllerReady(timeout time.Duration) error {
	wantBoot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	deadline := time.Now().Add(timeout)
	var lastOutput string
	var lastErr error
	for {
		output, err := r.runRoutectl(routectlStatusPollRPCTimeout, "status")
		lastErr = err
		lastOutput = strings.TrimSpace(string(output))
		if err == nil {
			var status struct {
				Ready            bool   `json:"ready"`
				Boot             string `json:"boot"`
				SnapshotRevision uint64 `json:"snapshot_revision"`
			}
			if json.Unmarshal(output, &status) == nil && status.Ready && status.Boot == wantBoot && status.SnapshotRevision > 0 {
				_ = r.writer.Observe("routectl.ndjson", r.scenarios[0], "controller_ready", r.harness, map[string]any{
					"boot": wantBoot, "snapshot_revision": status.SnapshotRevision, "ready": true,
				})
				return nil
			}
		}
		if time.Now().After(deadline) {
			detail := map[string]any{
				"want_boot": wantBoot, "last_error": errString(lastErr), "consumers": r.consumerReadinessSnapshot(),
			}
			if lastOutput != "" {
				detail["last_status"] = lastOutput
			}
			_ = r.writer.Observe("routectl.ndjson", r.scenarios[0], "controller_ready_timeout", r.harness, detail)
			return fmt.Errorf("production route-controller did not become functionally ready want_boot=%s last_error=%v", wantBoot, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

const routectlStatusPollRPCTimeout = 2 * time.Second

func boundedRoutectlRPCTimeout(rpcTimeout time.Duration) time.Duration {
	if rpcTimeout <= 0 {
		return routectlStatusPollRPCTimeout
	}
	if rpcTimeout > 30*time.Second {
		return 30 * time.Second
	}
	return rpcTimeout
}

func (r *runRuntime) runRoutectl(rpcTimeout time.Duration, commandArgs ...string) ([]byte, error) {
	rpcTimeout = boundedRoutectlRPCTimeout(rpcTimeout)
	args := []string{"--target", r.controllerAddr, "--server-name", r.routeServerDNS, "--expected-server-identity", r.routeServerDNS,
		"--tls-cert", r.operator.CertFile, "--tls-key", r.operator.KeyFile, "--tls-ca", r.trust.CAs["route"],
		"--dial-timeout", "2s", "--rpc-timeout", rpcTimeout.String()}
	args = append(args, commandArgs...)
	command := exec.CommandContext(r.ctx, r.binaries["routectl"].path, args...)
	command.Dir = r.worktree
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message == "" {
			return stdout.Bytes(), err
		}
		return stdout.Bytes(), fmt.Errorf("%w: %s", err, message)
	}
	return stdout.Bytes(), nil
}

func (r *runRuntime) waitConsumersReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		snapshot := r.consumerReadinessSnapshot()
		all := true
		for _, ready := range snapshot {
			all = all && ready
		}
		if all && len(snapshot) > 0 {
			_ = r.writer.Observe("processes.ndjson", r.scenarios[0], "consumers_ready", r.harness, map[string]any{
				"ready": snapshot, "count": len(snapshot),
			})
			return nil
		}
		if time.Now().After(deadline) {
			_ = r.writer.Observe("processes.ndjson", r.scenarios[0], "consumers_ready_timeout", r.harness, map[string]any{
				"ready": snapshot, "count": len(snapshot),
			})
			return fmt.Errorf("production consumers did not become functionally ready: %v", snapshot)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *runRuntime) consumerReadinessSnapshot() map[string]bool {
	snapshot := make(map[string]bool, len(r.holders)+len(r.gates)+1)
	for _, holder := range r.holders {
		snapshot[holder.name] = httpReady(holder.ports.health)
	}
	for _, gate := range r.gates {
		snapshot[gate.name] = httpReady(gate.ports.http)
	}
	if r.workerProcess != nil && r.workerHealth != "" {
		snapshot["worker"] = httpReady(r.workerHealth)
	}
	return snapshot
}

func (r *runRuntime) classifyGateLoginLogFailures() map[string]int {
	classes := map[string]int{}
	logDir := filepath.Join(r.tempRoot, "logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return classes
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || (!strings.HasPrefix(name, "gate-1-") && !strings.HasPrefix(name, "gate-2-")) {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(logDir, name))
		if readErr != nil {
			continue
		}
		for _, line := range strings.Split(string(payload), "\n") {
			if !strings.Contains(line, "login failed") {
				continue
			}
			classes[classifyGateLoginFailureLine(line)]++
		}
	}
	return classes
}

// classifyGateLoginFailureLine maps Gate login failure log lines to coarse
// classes without retaining secrets, URLs, or raw Redis values.
func classifyGateLoginFailureLine(line string) string {
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "writer epoch mismatch"):
		return "writer_epoch_mismatch"
	case strings.Contains(lower, "unavailable") && strings.Contains(lower, "blobstore"):
		return "blobstore_unavailable"
	case strings.Contains(lower, "bootstrap"):
		return "bootstrap"
	case strings.Contains(lower, "no assignment"), strings.Contains(lower, "errnoassignment"):
		return "no_assignment"
	case strings.Contains(lower, "redis"), strings.Contains(lower, "timeout"):
		return "store_or_timeout"
	case strings.Contains(lower, "bad platform"), strings.Contains(lower, "unauthorized"), strings.Contains(lower, "unauthenticated"):
		return "token"
	default:
		return "other"
	}
}

func businessDriverStderrClass(stderr string) string {
	trimmed := strings.TrimSpace(stderr)
	if trimmed == "" {
		return ""
	}
	const prefix = "rt2q-business-driver failed:"
	if idx := strings.Index(trimmed, prefix); idx >= 0 {
		class := strings.TrimSpace(trimmed[idx+len(prefix):])
		if class == "" {
			return "driver_failed"
		}
		if strings.ContainsAny(class, " \t\r\n:/@") {
			return "stderr_present"
		}
		return class
	}
	if strings.Contains(trimmed, "rt2q-business-driver failed") {
		return "driver_failed"
	}
	return "stderr_present"
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func httpReady(address string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	response, err := client.Get("http://" + address + "/readyz")
	if err != nil {
		return false
	}
	_ = response.Body.Close()
	return response.StatusCode >= 200 && response.StatusCode < 300
}

func waitTCPListener(parent context.Context, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	dialer := net.Dialer{Timeout: 250 * time.Millisecond}
	for {
		ctx, cancel := context.WithTimeout(parent, 300*time.Millisecond)
		connection, err := dialer.DialContext(ctx, "tcp", address)
		cancel()
		if err == nil {
			_ = connection.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(25 * time.Millisecond)
	}
}

type deadlineRefreshRouteFact struct {
	Domain              actorrouting.RouteDomain
	Shard               uint32
	HolderID            string
	HolderBootID        string
	AssignmentRevision  uint64
	WriterEpoch         uint64
	LeaseEpoch          uint64
	SnapshotDeadlineMs  int64
	AuthorityDeadlineMs int64
	LastTransitionID    string
}

type deadlineRefreshState struct {
	Revision uint64
	Routes   map[string]deadlineRefreshRouteFact
}

func deadlineRefreshRouteKey(domain actorrouting.RouteDomain, shard uint32) string {
	return string(domain) + "/" + strconv.FormatUint(uint64(shard), 10)
}

func (r *runRuntime) readDeadlineRefreshState() (deadlineRefreshState, error) {
	client, err := r.observerRedisClient()
	if err != nil {
		return deadlineRefreshState{}, err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment, NamespaceZone: r.zone, PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		return deadlineRefreshState{}, err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001}, 2, 2)
	if err != nil {
		return deadlineRefreshState{}, err
	}
	reader, err := redisauthority.NewShardAuthorityReader(client, redisauthority.DefaultAuthorityReaderOptions(tuple.Profile(), calculator))
	if err != nil {
		return deadlineRefreshState{}, err
	}
	snapshotStore, err := redisroutecontrol.NewSnapshotStore(client, redisroutecontrol.DefaultOptions(tuple.Namespace()))
	if err != nil {
		return deadlineRefreshState{}, err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	stored, found, err := snapshotStore.LoadCurrent(ctx)
	if err != nil || !found {
		return deadlineRefreshState{}, fmt.Errorf("deadline refresh Snapshot unavailable: found=%t err=%v", found, err)
	}
	state := deadlineRefreshState{Revision: stored.Snapshot.GlobalRevision, Routes: make(map[string]deadlineRefreshRouteFact)}
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		var routes actorrouting.DomainSnapshot
		if domain == actorrouting.DomainPlayer {
			routes = stored.Snapshot.Player
		} else {
			routes = stored.Snapshot.Farm
		}
		for shard := uint32(0); shard < routes.ShardCount; shard++ {
			route := routes.Routes[shard]
			if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(stored.Snapshot.Holders) {
				return deadlineRefreshState{}, fmt.Errorf("deadline refresh route %s/%d is not complete ACTIVE", domain, shard)
			}
			holder := stored.Snapshot.Holders[route.HolderIndex]
			current, authorityFound, readErr := reader.ReadShardWriteAuthority(ctx, domain, shard)
			if readErr != nil || !authorityFound {
				return deadlineRefreshState{}, fmt.Errorf("deadline refresh Authority %s/%d unavailable: found=%t err=%v", domain, shard, authorityFound, readErr)
			}
			if current.State != authoritystore.ShardAuthorityActive || current.HolderID != holder.InstanceID || current.HolderBootID != holder.BootID ||
				current.AssignmentRevision != route.ShardRevision || current.WriterEpoch != route.ExpectedWriterEpoch || current.LeaseEpoch != route.ExpectedLeaseEpoch ||
				current.LeaseUntilMs < route.LeaseUntilMs || current.LastTransitionID == "" {
				return deadlineRefreshState{}, fmt.Errorf("deadline refresh Authority/Snapshot generation mismatch for %s/%d", domain, shard)
			}
			state.Routes[deadlineRefreshRouteKey(domain, shard)] = deadlineRefreshRouteFact{
				Domain: domain, Shard: shard, HolderID: holder.InstanceID, HolderBootID: holder.BootID,
				AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch, LeaseEpoch: route.ExpectedLeaseEpoch,
				SnapshotDeadlineMs: route.LeaseUntilMs, AuthorityDeadlineMs: current.LeaseUntilMs, LastTransitionID: current.LastTransitionID,
			}
		}
	}
	return state, nil
}

func sameDeadlineRefreshGeneration(initial, current deadlineRefreshRouteFact) bool {
	return initial.Domain == current.Domain && initial.Shard == current.Shard &&
		initial.HolderID == current.HolderID && initial.HolderBootID == current.HolderBootID &&
		initial.AssignmentRevision == current.AssignmentRevision && initial.WriterEpoch == current.WriterEpoch &&
		initial.LeaseEpoch == current.LeaseEpoch && initial.LastTransitionID == current.LastTransitionID
}

func (r *runRuntime) runDeadlineRefresh() error {
	const scenario = "deadline-refresh"
	initial, err := r.readDeadlineRefreshState()
	if err != nil {
		return err
	}
	if len(initial.Routes) != 4 {
		return fmt.Errorf("deadline refresh expected four ACTIVE shards, got %d", len(initial.Routes))
	}
	oldDeadlineMs := int64(0)
	for _, route := range initial.Routes {
		if route.SnapshotDeadlineMs > oldDeadlineMs {
			oldDeadlineMs = route.SnapshotDeadlineMs
		}
	}
	callbackBaseline := make(map[string]int, len(r.holders))
	for _, holder := range r.holders {
		if holder.holderProxy != nil {
			callbackBaseline[holder.name] = len(holder.holderProxy.Events())
		}
	}
	transferBaseline := countTransferScriptEvents(r.redisProxy.Events())
	if err := r.writer.Observe("snapshots.ndjson", scenario, "deadline_refresh_baseline", r.harness, map[string]any{
		"snapshot_revision": initial.Revision, "active_shards": len(initial.Routes), "old_snapshot_deadline_ms": oldDeadlineMs,
	}); err != nil {
		return err
	}

	var projected deadlineRefreshState
	projectionDeadline := time.Now().Add(90 * time.Second)
	for {
		current, readErr := r.readDeadlineRefreshState()
		if readErr == nil && current.Revision > initial.Revision {
			complete := true
			for key, old := range initial.Routes {
				next, ok := current.Routes[key]
				if !ok || !sameDeadlineRefreshGeneration(old, next) || next.SnapshotDeadlineMs <= old.SnapshotDeadlineMs || next.AuthorityDeadlineMs < next.SnapshotDeadlineMs {
					complete = false
					break
				}
			}
			if complete {
				projected = current
				break
			}
		}
		if time.Now().After(projectionDeadline) {
			return errors.New("renewed Authorities were not projected into a complete Snapshot before the long-lived deadline")
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err := r.writer.Observe("snapshots.ndjson", scenario, "deadline_refresh_projected", r.harness, map[string]any{
		"old_revision": initial.Revision, "new_revision": projected.Revision, "changed_shards": len(projected.Routes),
		"one_complete_snapshot_per_revision": true, "generation_unchanged": true,
	}); err != nil {
		return err
	}
	// Establish real persisted player write fences while L0 is still valid, so
	// the post-horizon business proof measures routing continuity rather than a
	// first-write bootstrap condition.
	playerStart := s0PlayerStart(r.challenge)
	if err := r.waitPlayerShardFences(scenario, 20*time.Second); err != nil {
		return err
	}
	if err := r.waitBusinessLoginReady(scenario, 25*time.Second, playerStart); err != nil {
		return err
	}

	if wait := time.Until(time.UnixMilli(oldDeadlineMs)) + 1500*time.Millisecond; wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return r.ctx.Err()
		case <-timer.C:
		}
	}
	finalState, err := r.readDeadlineRefreshState()
	if err != nil {
		return err
	}
	for key, old := range initial.Routes {
		current, ok := finalState.Routes[key]
		if !ok || !sameDeadlineRefreshGeneration(old, current) || current.SnapshotDeadlineMs <= oldDeadlineMs || current.AuthorityDeadlineMs < current.SnapshotDeadlineMs {
			return fmt.Errorf("deadline refresh generation/deadline drift after old horizon for %s", key)
		}
	}
	if err := r.waitConsumerRouteMetrics(finalState.Revision, 30*time.Second); err != nil {
		return err
	}
	wantBoot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	if err := r.observeFreshConsumerSnapshots(scenario, wantBoot, nil); err != nil {
		return err
	}

	beforeRPC := sumSnapshotCounters(r.snapshotProxies)
	if err := r.runDeadlineRefreshBusiness(playerStart, 100); err != nil {
		return err
	}
	afterRPC := sumSnapshotCounters(r.snapshotProxies)
	unaryDelta := afterRPC.UnaryRPCs - beforeRPC.UnaryRPCs
	streamDelta := afterRPC.StreamMessages - beforeRPC.StreamMessages
	callbackDelta := 0
	for _, holder := range r.holders {
		if holder.holderProxy != nil {
			callbackDelta += len(holder.holderProxy.Events()) - callbackBaseline[holder.name]
		}
	}
	transferDelta := countTransferScriptEvents(r.redisProxy.Events()) - transferBaseline
	if err := r.writer.Observe("business.ndjson", scenario, "long_lived_route", r.harness, map[string]any{
		"old_deadline_crossed": time.Now().UnixMilli() > oldDeadlineMs, "business_resolves": 100,
		"consumer_count": len(r.snapshotProxies), "consumer_revision": finalState.Revision,
		"hot_path_rpc_delta": unaryDelta, "watch_stream_delta": streamDelta,
		"holder_callback_delta": callbackDelta, "authority_transfer_delta": transferDelta,
	}); err != nil {
		return err
	}
	// A periodic complete Watch publication may legitimately overlap this
	// business window; it is control-plane propagation, not a Resolve call.
	// Unary route RPCs must remain exactly zero on the request hot path.
	if unaryDelta != 0 {
		return fmt.Errorf("deadline refresh business hot path used route RPCs: unary_delta=%d stream_delta=%d", unaryDelta, streamDelta)
	}
	if callbackDelta != 0 {
		return fmt.Errorf("deadline refresh replayed %d Holder callbacks", callbackDelta)
	}
	if transferDelta != 0 {
		return fmt.Errorf("deadline refresh replayed %d Authority transfers", transferDelta)
	}
	return nil
}

func countTransferScriptEvents(events []routecontrol.RedisAuditEvent) int {
	count := 0
	for _, event := range events {
		if strings.EqualFold(event.ScriptSHA, transferShardAuthorityScriptSHA) && event.Forwarded {
			count++
		}
	}
	return count
}

func (r *runRuntime) consumerMetricsAddresses() map[string]string {
	addresses := make(map[string]string, len(r.holders)+len(r.gates)+1)
	for _, holder := range r.holders {
		addresses[holder.name] = holder.ports.health
	}
	for _, gate := range r.gates {
		addresses[gate.name] = gate.ports.http
	}
	addresses["worker"] = r.workerHealth
	return addresses
}

func (r *runRuntime) waitConsumerRouteMetrics(revision uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := make(map[string]any)
	for {
		all := true
		observed := make(map[string]uint64)
		for name, address := range r.consumerMetricsAddresses() {
			current, err := scrapeRouteSnapshotRevision(address)
			if err != nil || current < revision {
				all = false
				if err != nil {
					last[name] = map[string]any{"error_class": "metrics_unavailable"}
				} else {
					last[name] = map[string]any{"revision": current}
				}
				continue
			}
			observed[name] = current
			last[name] = map[string]any{"revision": current}
		}
		if all && len(observed) == len(r.consumerMetricsAddresses()) {
			return r.writer.Observe("snapshots.ndjson", "deadline-refresh", "consumer_metrics_applied", r.harness, map[string]any{
				"required_revision": revision, "consumers": observed, "consumer_count": len(observed),
			})
		}
		if time.Now().After(deadline) {
			_ = r.writer.Observe("snapshots.ndjson", "deadline-refresh", "consumer_metrics_timeout", r.harness, map[string]any{
				"required_revision": revision, "consumers": last,
			})
			return fmt.Errorf("seven consumers did not expose route Snapshot revision %d", revision)
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func scrapeRouteSnapshotRevision(address string) (uint64, error) {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + address + "/metrics")
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, fmt.Errorf("metrics HTTP status %d", response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "farm_actor_route_snapshot_revision" {
			value, parseErr := strconv.ParseFloat(fields[1], 64)
			if parseErr != nil || value < 0 {
				return 0, errors.New("invalid route Snapshot revision metric")
			}
			return uint64(value), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("route Snapshot revision metric missing")
}

func (r *runRuntime) runDeadlineRefreshBusiness(playerStart uint64, countWant int) error {
	httpValues := []string{"http://" + r.gates[0].ports.http, "http://" + r.gates[1].ports.http}
	wsValues := []string{"ws://" + r.gates[0].ports.http + "/ws", "ws://" + r.gates[1].ports.http + "/ws"}
	command := exec.CommandContext(r.ctx, r.binaries["rt2q-business-driver"].path,
		"--http-bases", strings.Join(httpValues, ","), "--ws-urls", strings.Join(wsValues, ","),
		"--challenge", r.challenge, "--count", strconv.Itoa(countWant), "--player-start", strconv.FormatUint(playerStart, 10),
		"--concurrency", "10", "--timeout", "45s")
	command.Env = append(os.Environ(), "RT2Q_BUSINESS_SECRET="+r.secret)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	okCount, failed := 0, 0
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		if event["ok"] == true {
			okCount++
		} else {
			failed++
		}
		payload := map[string]any{
			"sequence": event["sequence"], "gate_index": event["gate_index"], "ok": event["ok"] == true,
			"player_id_sha256": event["player_id_sha256"], "operation_sha256": event["operation_sha256"],
		}
		if class, ok := event["failure_class"].(string); ok && class != "" {
			payload["failure_class"] = class
		}
		if err := r.writer.Observe("business.ndjson", "deadline-refresh", "business_resolve", r.harness, payload); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	waitErr := command.Wait()
	if waitErr != nil || okCount != countWant || failed != 0 {
		_ = r.writer.Observe("business.ndjson", "deadline-refresh", "business_driver_failed", r.harness, map[string]any{
			"ok_count": okCount, "failed_count": failed, "stderr_class": businessDriverStderrClass(stderr.String()),
			"gate_login_log_class": r.classifyGateLoginLogFailures(),
		})
		return fmt.Errorf("deadline refresh business driver failed: ok=%d failed=%d class=%s", okCount, failed, businessDriverStderrClass(stderr.String()))
	}
	return nil
}

func (r *runRuntime) runS0() error {
	wantBoot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	readiness := r.consumerReadinessSnapshot()
	for name, ready := range readiness {
		if !ready {
			return fmt.Errorf("consumer %s is not locally ready before S0 facts", name)
		}
	}
	for name, proxy := range r.snapshotProxies {
		events := proxy.Events()
		var latest routecontrol.SnapshotProxyEvent
		for _, event := range events {
			if event.UpstreamOK && event.Boot == wantBoot && event.Revision >= latest.Revision {
				latest = event
			}
		}
		if latest.Revision == 0 || latest.Boot != wantBoot || latest.EnvelopeSHA256 == "" {
			return fmt.Errorf("consumer %s lacks an authenticated complete snapshot for boot %s", name, wantBoot)
		}
		if err := r.writer.Observe("snapshots.ndjson", "S0", "snapshot_installed", r.harness, map[string]any{
			"consumer": name, "revision": latest.Revision, "envelope_sha256": latest.EnvelopeSHA256,
			"boot": latest.Boot, "authenticated": true, "locally_ready": readiness[name],
		}); err != nil {
			return err
		}
	}
	playerStart := s0PlayerStart(r.challenge)
	if err := r.waitPlayerShardFences("S0", 20*time.Second); err != nil {
		return err
	}
	if err := r.waitBusinessLoginReady("S0", 45*time.Second, playerStart); err != nil {
		_ = r.writer.Observe("business.ndjson", "S0", "business_login_ready_failed", r.harness, map[string]any{
			"error_class":          "login_ready_timeout",
			"gate_login_log_class": r.classifyGateLoginLogFailures(),
			"player_start_sha256":  digestText(r.challenge + ":player-start:" + strconv.FormatUint(playerStart, 10)),
		})
		return err
	}
	before := sumSnapshotCounters(r.snapshotProxies)
	httpValues := []string{"http://" + r.gates[0].ports.http, "http://" + r.gates[1].ports.http}
	wsValues := []string{"ws://" + r.gates[0].ports.http + "/ws", "ws://" + r.gates[1].ports.http + "/ws"}
	command := exec.CommandContext(r.ctx, r.binaries["rt2q-business-driver"].path,
		"--http-bases", strings.Join(httpValues, ","), "--ws-urls", strings.Join(wsValues, ","),
		"--challenge", r.challenge, "--count", "100", "--player-start", strconv.FormatUint(playerStart, 10),
		"--concurrency", "10", "--timeout", "45s")
	command.Env = append(os.Environ(), "RT2Q_BUSINESS_SECRET="+r.secret)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	count := 0
	failed := 0
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return err
		}
		payload := map[string]any{
			"sequence": event["sequence"], "gate_index": event["gate_index"], "ok": event["ok"] == true,
			"player_id_sha256": event["player_id_sha256"], "operation_sha256": event["operation_sha256"],
		}
		if event["ok"] != true {
			failed++
			if class, ok := event["failure_class"].(string); ok && class != "" {
				payload["failure_class"] = class
			}
			if err := r.writer.Observe("business.ndjson", "S0", "business_resolve", r.harness, payload); err != nil {
				return err
			}
			continue
		}
		count++
		if err := r.writer.Observe("business.ndjson", "S0", "business_resolve", r.harness, payload); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	waitErr := command.Wait()
	if waitErr != nil || count != 100 || failed != 0 {
		_ = r.writer.Observe("business.ndjson", "S0", "business_driver_failed", r.harness, map[string]any{
			"ok_count": count, "failed_count": failed, "wait_error": errString(waitErr),
			"stderr_class":         businessDriverStderrClass(stderr.String()),
			"challenge_len":        len(r.challenge),
			"secret_len":           len(r.secret),
			"gate_login_log_class": r.classifyGateLoginLogFailures(),
		})
		return errors.New("actual Gate business resolve driver failed")
	}
	after := sumSnapshotCounters(r.snapshotProxies)
	return r.writer.Observe("gate-observations.ndjson", "S0", "hot_path_counters", r.harness,
		map[string]any{"rpc_delta": after.UnaryRPCs - before.UnaryRPCs, "stream_delta": after.StreamMessages - before.StreamMessages})
}

// waitPlayerShardFences requires durable PLAYER Authority fences for shards
// 0/1 to bind the live holders and match Gate snapshot source endpoints before
// any business login probe.
func (r *runRuntime) waitPlayerShardFences(scenario string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ready := true
		detail := make([]map[string]any, 0, 2)
		for shard := uint32(0); shard < 2; shard++ {
			fence, err := r.routectlShardFence("PLAYER", shard)
			item := map[string]any{"shard": shard}
			if err != nil {
				ready = false
				item["error_class"] = "shard_fence_error"
				detail = append(detail, item)
				continue
			}
			holder := r.holderByID(fence.HolderID)
			item["holder_match"] = holder != nil && holder.boot == fence.HolderBootID
			item["writer_epoch"] = fence.WriterEpoch
			if holder == nil || holder.boot != fence.HolderBootID {
				ready = false
				detail = append(detail, item)
				continue
			}
			match, matchErr := r.snapshotSourceEndpointMatch("PLAYER", shard, holder)
			item["snapshot_endpoint_match"] = match && matchErr == nil
			if matchErr != nil || !match {
				ready = false
			}
			detail = append(detail, item)
		}
		if ready {
			return r.writer.Observe("business.ndjson", scenario, "player_shard_fences_ready", r.harness, map[string]any{
				"ok": true, "shards": detail,
			})
		}
		if time.Now().After(deadline) {
			_ = r.writer.Observe("business.ndjson", scenario, "player_shard_fences_ready", r.harness, map[string]any{
				"ok": false, "shards": detail,
			})
			return errors.New("S0 PLAYER shard fences were not bound to live holders before business login")
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// s0PlayerStart derives a challenge-scoped base player id so isolated runs do
// not reuse a fixed id band across harness invocations.
func s0PlayerStart(challenge string) uint64 {
	if len(challenge) < 8 {
		return 910000000
	}
	offset, err := strconv.ParseUint(challenge[:8], 16, 64)
	if err != nil {
		return 910000000
	}
	return 910000000 + (offset%100000)*1000
}

type s0LoginProbe struct {
	gateIndex int
	playerID  uint64
	shard     uint32
}

// waitBusinessLoginReady proves Gate→Player EnsureOwner write path is live for
// both PLAYER shards on both Gates before the hot-resolve storm. Listener
// /readyz alone is insufficient: login can still fail with blobstore writer
// epoch mismatch until Authority claim fences converge.
func (r *runRuntime) waitBusinessLoginReady(scenario string, timeout time.Duration, playerStart uint64) error {
	if len(r.gates) < 2 {
		return errors.New("S0 requires two Gates for business login readiness")
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return err
	}
	probes := make([]s0LoginProbe, 0, 4)
	for shard := uint32(0); shard < 2; shard++ {
		playerID, pickErr := r.pickPlayerIDForShardNear(calculator, shard, playerStart)
		if pickErr != nil {
			return pickErr
		}
		for gateIndex := range r.gates {
			probes = append(probes, s0LoginProbe{gateIndex: gateIndex, playerID: playerID, shard: shard})
		}
	}
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		attempts++
		okCount := 0
		classes := map[string]int{}
		for _, probe := range probes {
			bootstrap := fmt.Sprintf("rt2q-%s-%s-ready-g%d-s%d-%d", r.challenge, strings.ToLower(scenario), probe.gateIndex, probe.shard, attempts)
			status, class, probeErr := probeGateLogin(r.ctx, "http://"+r.gates[probe.gateIndex].ports.http, probe.playerID, bootstrap, []byte(r.secret))
			if probeErr != nil || status != http.StatusOK {
				if class == "" {
					class = "probe_error"
				}
				classes[class]++
				continue
			}
			okCount++
		}
		if okCount == len(probes) {
			return r.writer.Observe("business.ndjson", scenario, "business_login_ready", r.harness, map[string]any{
				"ok": true, "attempts": attempts, "probe_count": len(probes),
				"player_start_sha256": digestText(r.challenge + ":player-start:" + strconv.FormatUint(playerStart, 10)),
			})
		}
		if attempts == 1 || attempts%10 == 0 {
			_ = r.writer.Observe("business.ndjson", scenario, "business_login_probe_attempt", r.harness, map[string]any{
				"ok": false, "attempt": attempts, "ok_count": okCount, "probe_count": len(probes),
				"failure_classes": classes, "gate_login_log_class": r.classifyGateLoginLogFailures(),
			})
		}
		if time.Now().After(deadline) {
			_ = r.writer.Observe("business.ndjson", scenario, "business_login_ready", r.harness, map[string]any{
				"ok": false, "attempts": attempts, "probe_count": len(probes), "ok_count": okCount,
				"failure_classes": classes, "gate_login_log_class": r.classifyGateLoginLogFailures(),
			})
			return fmt.Errorf("S0 business login readiness not reached: classes=%v", classes)
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *runRuntime) pickPlayerIDForShardNear(calculator *actorrouting.Calculator, wantShard uint32, start uint64) (uint64, error) {
	if calculator == nil || start == 0 {
		return 0, errors.New("S0 shard player probe requires calculator and start id")
	}
	for offset := uint64(0); offset < 100000; offset++ {
		playerID := start + offset
		shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
		if err != nil {
			return 0, err
		}
		if shard == wantShard {
			return playerID, nil
		}
	}
	return 0, fmt.Errorf("S0 could not find player id for shard %d near start", wantShard)
}

func probeGateLogin(ctx context.Context, httpBase string, playerID uint64, bootstrap string, secret []byte) (statusCode int, failureClass string, err error) {
	if playerID == 0 || len(secret) < 32 || strings.TrimSpace(bootstrap) == "" {
		return 0, "invalid_probe", errors.New("invalid login probe arguments")
	}
	id := strconv.FormatUint(playerID, 10)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(id))
	payload, _ := json.Marshal(map[string]string{
		"platform_token": "load:" + id + ":" + hex.EncodeToString(mac.Sum(nil)),
		"bootstrap_id":   bootstrap, "client_build": "rt2q-harness/s0-ready",
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(httpBase, "/")+"/api/v1/auth/login", bytes.NewReader(payload))
	if err != nil {
		return 0, "request_build", err
	}
	request.Header.Set("content-type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, "transport", err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode == http.StatusOK {
		return response.StatusCode, "", nil
	}
	return response.StatusCode, "login_status_" + strconv.Itoa(response.StatusCode), nil
}

func digestText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func sumSnapshotCounters(proxies map[string]*routecontrol.SnapshotForwardProxy) routecontrol.SnapshotProxyCounters {
	var total routecontrol.SnapshotProxyCounters
	for _, proxy := range proxies {
		value := proxy.Counters()
		total.UnaryRPCs += value.UnaryRPCs
		total.WatchRPCs += value.WatchRPCs
		total.StreamMessages += value.StreamMessages
	}
	return total
}

func (r *runRuntime) runS1() error {
	if err := r.stopManaged(r.controller, 10*time.Second); err != nil {
		return err
	}
	r.controllerIndex++
	if err := r.startController(); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, r.controllerAddr, 30*time.Second); err != nil {
		return errors.New("replacement route-controller did not open its listener")
	}
	if err := r.waitConsumersReady(45 * time.Second); err != nil {
		return err
	}
	if err := r.waitControllerReady(30 * time.Second); err != nil {
		return err
	}
	wantBoot := r.controllerBoots[r.controllerIndex]
	if err := r.writer.Observe("processes.ndjson", "S1", "controller_rollover", r.controller.identity, map[string]any{"new_boot": true, "stop_first": true, "boot": wantBoot}); err != nil {
		return err
	}
	for name, proxy := range r.snapshotProxies {
		found := false
		var revision uint64
		for _, event := range proxy.Events() {
			if event.UpstreamOK && event.Boot == wantBoot {
				found = true
				revision = event.Revision
			}
		}
		if !found {
			return fmt.Errorf("consumer %s did not complete Boot rollover resync", name)
		}
		if err := r.writer.Observe("snapshots.ndjson", "S1", "consumer_rollover", r.harness, map[string]any{"consumer": name, "healthy": true, "boot": wantBoot, "revision": revision}); err != nil {
			return err
		}
	}
	return nil
}

func (r *runRuntime) runS5() error {
	source := r.holderByName("player-a")
	target := r.holderByName("player-b")
	if source == nil || target == nil || source.process == nil {
		return errors.New("S5 requires PLAYER A/B holders")
	}
	const domain = "PLAYER"
	const shard uint32 = 0
	if err := r.waitHoldersMoveReady("S5", domain, source, target, 45*time.Second); err != nil {
		return err
	}
	fence, err := r.routectlShardFence(domain, shard)
	if err != nil {
		return err
	}
	if fence.HolderID != source.id || fence.HolderBootID != source.boot {
		return errors.New("S5 source fence does not match PLAYER A")
	}
	// Need Authority remaining >> HolderRegistry TTL (30s) so Transfer remains
	// applicable after wall-clock registry expiry.
	if err := r.waitAuthorityLeaseRemaining(domain, shard, 50*time.Second, 55*time.Second); err != nil {
		return err
	}
	planID := fmt.Sprintf("rt2q-%s-s5-takeover", r.challenge)
	transitionID := fmt.Sprintf("rt2q-%s-s5-transition", r.challenge)
	if err := r.proveS5LiveTakeoverRejected(planID+"-live", transitionID+"-live", domain, shard, source, target, fence); err != nil {
		return err
	}
	if err := r.stopManaged(source.process, 10*time.Second); err != nil {
		return err
	}
	source.process = nil
	// Production TransferShardAuthorityV2 still requires an ACTIVE Authority lease.
	// Crash Takeover therefore uses trusted holder-unavailability after the real
	// HolderRegistry lease expires, while the Authority lease remains current.
	if err := r.waitHolderRegistryExpired(domain, source, 30*time.Second); err != nil {
		return err
	}
	authorityAlive, err := r.readPlayerAuthority(shard)
	if err != nil {
		return err
	}
	if authorityAlive.LeaseUntilMs <= time.Now().UnixMilli() {
		return errors.New("S5 Authority lease expired before holder-unavailable Crash Takeover; Transfer would reject LEASE_EXPIRED")
	}
	if authorityAlive.HolderID != source.id || authorityAlive.HolderBootID != source.boot ||
		authorityAlive.AssignmentRevision != fence.AssignmentRevision ||
		authorityAlive.WriterEpoch != fence.WriterEpoch || authorityAlive.LeaseEpoch != fence.LeaseEpoch {
		return errors.New("S5 Authority fence drifted before Crash Takeover")
	}
	if err := r.writer.Observe("redis-faults.ndjson", "S5", "redis_expiry_proof", r.harness, map[string]any{
		"expired": true, "domain": domain, "shard": shard,
		"old_holder": source.id, "old_boot": source.boot, "kind": "holder_registry_lease",
		"authority_lease_active": true,
	}); err != nil {
		return err
	}
	phase, revision, takeoverErr := r.executeCrashTakeover(planID, transitionID, domain, shard, source, target, fence)
	if takeoverErr != nil || phase != "SUCCEEDED" {
		_ = r.writer.Observe("plans.ndjson", "S5", "s5_takeover_failed", r.harness, map[string]any{
			"error": errString(takeoverErr), "phase": phase, "revision": revision,
		})
		return fmt.Errorf("S5 production crash takeover did not SUCCEEDED: %v", takeoverErr)
	}
	if err := r.writer.Observe("plans.ndjson", "S5", "plan_phase", r.harness, map[string]any{
		"plan_id": planID, "phase": phase, "revision": revision,
	}); err != nil {
		return err
	}
	if err := r.writer.Observe("authority.ndjson", "S5", "authority_transfer", r.harness, map[string]any{
		"domain": domain, "shard": shard, "source": source.id, "target": target.id,
		"plan_id": planID, "transition_id": transitionID, "once": true, "mode": "crash-takeover",
	}); err != nil {
		return err
	}
	verify, err := r.waitRoutectlVerifyConsistent(planID, domain, shard, 20*time.Second)
	if err != nil {
		return err
	}
	if !verify.Consistent || !verifyCheckOK(verify, "gate_resolution") {
		return errors.New("S5 final verify is not consistent with Gate resolution")
	}
	if err := r.observeS5GateTargetActive(domain, shard, target, fence); err != nil {
		return err
	}
	match, err := r.snapshotSourceEndpointMatch(domain, shard, target)
	if err != nil || !match {
		return fmt.Errorf("S5 ACTIVE snapshot does not bind target: match=%v err=%v", match, err)
	}
	return r.proveS5OldGenerationFenced(domain, shard, source, fence)
}

func (r *runRuntime) proveS5LiveTakeoverRejected(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence) error {
	code, err := r.probeTakeoverStatusCode(planID, transitionID, domain, shard, source, target, fence, true, false)
	if err != nil {
		return err
	}
	if code == "OK" {
		return errors.New("S5 live crash takeover was accepted")
	}
	return r.writer.Observe("plans.ndjson", "S5", "takeover_live_rejected", r.harness, map[string]any{
		"rejected": true, "status_code": code, "plan_id": planID,
		"holder_unavailable": true, "lease_expired": false,
	})
}

func (r *runRuntime) waitHolderRegistryExpired(domain string, source *holderSpec, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client, err := r.observerRedisClient()
	if err != nil {
		return err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment, NamespaceZone: r.zone, PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		return err
	}
	store, err := redisroutecontrol.NewHolderStore(client, redisroutecontrol.DefaultHolderStoreOptions(tuple.Namespace()))
	if err != nil {
		return err
	}
	routeDomain := actorrouting.DomainPlayer
	if domain == "FARM" {
		routeDomain = actorrouting.DomainFarm
	}
	for {
		ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
		record, found, getErr := store.Get(ctx, routeDomain, source.id)
		cancel()
		// Match PlanExecutor.validateTakeoverProof: wall-clock LeaseUntil, not List filtering.
		if getErr == nil && (!found || record.BootID != source.boot || !record.LeaseUntil.After(time.Now())) {
			return nil
		}
		if time.Now().After(deadline) {
			if getErr != nil {
				return getErr
			}
			return fmt.Errorf("S5 old holder registry lease did not expire found=%v lease_until=%s", found, record.LeaseUntil.UTC().Format(time.RFC3339Nano))
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (r *runRuntime) probeTakeoverStatusCode(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence, holderUnavailable, leaseExpired bool) (string, error) {
	_, err := r.callTakeoverAfterExpiry(planID, transitionID, domain, shard, source, target, fence, holderUnavailable, leaseExpired, "rt2q-s5-live-reject-"+r.challenge, 15*time.Second)
	if err == nil {
		return "OK", nil
	}
	if st, ok := status.FromError(err); ok {
		return st.Code().String(), nil
	}
	return "unknown", nil
}

func (r *runRuntime) executeCrashTakeover(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence) (phase string, revision uint64, err error) {
	plan, callErr := r.callTakeoverAfterExpiry(planID, transitionID, domain, shard, source, target, fence, true, false, "rt2q-s5-crash-takeover-"+r.challenge, 45*time.Second)
	if callErr != nil {
		if st, ok := status.FromError(callErr); ok {
			return "", 0, fmt.Errorf("%s: %s", st.Code(), st.Message())
		}
		return "", 0, callErr
	}
	if plan == nil {
		return "", 0, errors.New("takeover response plan is nil")
	}
	phaseName := map[routeplanpb.PlanPhase]string{
		routeplanpb.PlanPhase_PLAN_PHASE_PENDING: "PENDING", routeplanpb.PlanPhase_PLAN_PHASE_DRAINING: "DRAINING",
		routeplanpb.PlanPhase_PLAN_PHASE_ASSIGNING: "ASSIGNING", routeplanpb.PlanPhase_PLAN_PHASE_PREPARING: "PREPARING",
		routeplanpb.PlanPhase_PLAN_PHASE_ACTIVE: "ACTIVE", routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED: "SUCCEEDED",
		routeplanpb.PlanPhase_PLAN_PHASE_FAILED: "FAILED", routeplanpb.PlanPhase_PLAN_PHASE_PAUSED: "PAUSED",
	}[plan.GetPhase()]
	if phaseName == "" {
		return "", plan.GetRevision(), fmt.Errorf("unknown takeover phase %v code=%s", plan.GetPhase(), plan.GetErrorCode())
	}
	if plan.GetPhase() != routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED {
		return phaseName, plan.GetRevision(), fmt.Errorf("takeover phase=%s error_code=%s", phaseName, plan.GetErrorCode())
	}
	return phaseName, plan.GetRevision(), nil
}

func (r *runRuntime) callTakeoverAfterExpiry(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence, holderUnavailable, leaseExpired bool, evidence string, timeout time.Duration) (*routeplanpb.Plan, error) {
	tlsConfig, err := transport.LoadClientTLSConfig(r.operator.CertFile, r.operator.KeyFile, r.trust.CAs["route"])
	if err != nil {
		return nil, err
	}
	bound, err := transport.BindExactServerIdentity(tlsConfig, r.routeServerDNS, r.routeServerDNS)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, r.controllerAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(bound)),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := routeplanpb.NewRoutePlanServiceClient(conn)
	response, err := client.TakeoverAfterExpiry(ctx, &routeplanpb.TakeoverAfterExpiryRequest{
		PlanId: planID, TransitionId: transitionID, Domain: domain, Shard: shard,
		OldHolderId: source.id, OldBootId: source.boot,
		TargetHolderId: target.id, TargetBootId: target.boot, TargetEndpoint: target.ports.logic,
		HolderUnavailable: holderUnavailable, LeaseExpired: leaseExpired,
		Evidence: evidence, RequestedTtlMs: 60000,
		ExpectedSourceFence: &routeplanpb.AuthorityFence{
			HolderId: fence.HolderID, HolderBootId: fence.HolderBootID,
			AssignmentRevision: fence.AssignmentRevision, WriterEpoch: fence.WriterEpoch, LeaseEpoch: fence.LeaseEpoch,
		},
	})
	if err != nil {
		return nil, err
	}
	return response.GetPlan(), nil
}

func (r *runRuntime) waitAuthorityLeaseRemaining(domain string, shard uint32, timeout, wantRemaining time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		authority, err := r.readDomainAuthority(domain, shard)
		if err == nil {
			remaining := time.Until(time.UnixMilli(authority.LeaseUntilMs))
			if remaining >= wantRemaining {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("S5 authority lease remaining too small before crash: lease_until_ms=%d", authority.LeaseUntilMs)
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (r *runRuntime) readPlayerAuthority(shard uint32) (authoritystore.ShardWriteAuthority, error) {
	return r.readDomainAuthority("PLAYER", shard)
}

func (r *runRuntime) readDomainAuthority(domain string, shard uint32) (authoritystore.ShardWriteAuthority, error) {
	client, err := r.observerRedisClient()
	if err != nil {
		return authoritystore.ShardWriteAuthority{}, err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment, NamespaceZone: r.zone, PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		return authoritystore.ShardWriteAuthority{}, err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return authoritystore.ShardWriteAuthority{}, err
	}
	reader, err := redisauthority.NewShardAuthorityReader(client, redisauthority.DefaultAuthorityReaderOptions(tuple.Profile(), calculator))
	if err != nil {
		return authoritystore.ShardWriteAuthority{}, err
	}
	routeDomain := actorrouting.DomainPlayer
	if domain == "FARM" {
		routeDomain = actorrouting.DomainFarm
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	value, found, err := reader.ReadShardWriteAuthority(ctx, routeDomain, shard)
	if err != nil || !found {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("authority reader failed: found=%v err=%v", found, err)
	}
	return value, nil
}

func (r *runRuntime) observeS5GateTargetActive(domain string, shard uint32, target *holderSpec, oldFence shardFence) error {
	if len(r.gates) == 0 || r.gates[0].verifierProxy == nil {
		return errors.New("S5 gate verifier proxy is unavailable")
	}
	// Final verify already exercised authenticated ObserveShard; bind peer events to target generation.
	current, err := r.readDomainAuthority(domain, shard)
	if err != nil {
		return err
	}
	if current.HolderID != target.id || current.HolderBootID != target.boot || current.WriterEpoch <= oldFence.WriterEpoch {
		return fmt.Errorf("S5 authority is not on target generation: holder=%s boot=%s epoch=%d", current.HolderID, current.HolderBootID, current.WriterEpoch)
	}
	matched := false
	for _, gate := range r.gates {
		for _, event := range gate.verifierProxy.Events() {
			if event.UpstreamOK && event.Identical && event.WriterEpoch == current.WriterEpoch &&
				event.AssignmentRevision == current.AssignmentRevision && event.SnapshotRevision > 0 {
				matched = true
			}
		}
		if len(gate.verifierProxy.TLSFacts()) == 0 {
			return fmt.Errorf("S5 gate %s missing TLS peer facts", gate.name)
		}
	}
	if !matched {
		return errors.New("S5 missing authenticated Gate ObserveShard for ACTIVE target generation")
	}
	return r.writer.Observe("gate-observations.ndjson", "S5", "gate_peer_observation", r.harness, map[string]any{
		"target_active": true, "tls_ok": true,
		"assignment_revision": current.AssignmentRevision, "writer_epoch": current.WriterEpoch,
		"lease_epoch": current.LeaseEpoch, "holder": target.id, "boot": target.boot,
	})
}

func (r *runRuntime) proveS5OldGenerationFenced(domain string, shard uint32, source *holderSpec, oldFence shardFence) error {
	before, err := r.readDomainAuthority(domain, shard)
	if err != nil {
		return err
	}
	if err := r.startHolder(source); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, source.ports.logic, 15*time.Second); err != nil {
		return fmt.Errorf("S5 restarted old holder did not bind logic listener: %w", err)
	}
	client, err := r.observerRedisClient()
	if err != nil {
		return err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment, NamespaceZone: r.zone, PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		return err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return err
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: tuple.Profile(), Resolver: calculator})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	routeDomain := actorrouting.DomainPlayer
	if domain == "FARM" {
		routeDomain = actorrouting.DomainFarm
	}
	renew, err := store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: source.id, BootID: source.boot,
		},
		AuthorityLayoutVersion: "v1", Domain: routeDomain, VirtualShard: shard,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: source.id, ExpectedHolderBootID: source.boot,
			ExpectedAssignmentRevision: oldFence.AssignmentRevision,
			ExpectedWriterEpoch:        oldFence.WriterEpoch,
			ExpectedLeaseEpoch:         oldFence.LeaseEpoch,
		},
		RequestedTTLms: 60000,
	})
	if err != nil {
		return err
	}
	fenced := renew.Status == authoritystore.ShardTransitionFenceMismatch ||
		renew.Status == authoritystore.ShardTransitionUnauthorized ||
		renew.Status == authoritystore.ShardTransitionLeaseExpired
	if !fenced {
		return fmt.Errorf("S5 old-generation renew was not fenced: status=%s", renew.Status)
	}
	after, err := r.readDomainAuthority(domain, shard)
	if err != nil {
		return err
	}
	durableUnchanged := after.HolderID == before.HolderID && after.HolderBootID == before.HolderBootID &&
		after.AssignmentRevision == before.AssignmentRevision && after.WriterEpoch == before.WriterEpoch &&
		after.LeaseEpoch == before.LeaseEpoch && after.LastTransitionID == before.LastTransitionID
	if !durableUnchanged {
		return errors.New("S5 old-generation renew mutated durable Authority")
	}
	return r.writer.Observe("authority.ndjson", "S5", "old_generation_request", r.harness, map[string]any{
		"fenced": true, "durable_unchanged": true, "renew_status": renew.Status.String(),
		"old_boot": source.boot, "old_writer_epoch": oldFence.WriterEpoch,
		"current_holder": after.HolderID, "current_writer_epoch": after.WriterEpoch,
	})
}

func (r *runRuntime) runS6() error {
	if len(r.gates) == 0 || r.gates[0].snapshotProxy == nil {
		return errors.New("S6 requires a Gate with Snapshot forward proxy")
	}
	planID, domain, shard, err := r.ensureSucceededPlayerPlan("S6")
	if err != nil {
		return err
	}
	verify, err := r.waitRoutectlVerifyConsistent(planID, domain, shard, 20*time.Second)
	if err != nil {
		return err
	}
	if !verify.Consistent || len(verify.Checks) != 11 {
		return fmt.Errorf("S6 post-migration verify not consistent: consistent=%v checks=%d", verify.Consistent, len(verify.Checks))
	}
	target := r.holderByName("player-b")
	if target == nil {
		return errors.New("S6 missing PLAYER B holder")
	}
	authority, err := r.readDomainAuthority(domain, shard)
	if err != nil {
		return err
	}
	if authority.HolderID != target.id || authority.HolderBootID != target.boot {
		return fmt.Errorf("S6 authority not on PLAYER B: holder=%s boot=%s", authority.HolderID, authority.HolderBootID)
	}
	freshGate := r.gates[0]
	if err := r.restartFreshConsumers(freshGate); err != nil {
		return err
	}
	wantBoot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	if err := r.observeFreshConsumerSnapshots("S6", wantBoot, []string{freshGate.name, "worker"}); err != nil {
		return err
	}
	accessListen, err := reserveAddress()
	if err != nil {
		return err
	}
	accessProxy, err := routecontrol.StartGateAccessForwardProxy(accessListen, freshGate.ports.http)
	if err != nil {
		return err
	}
	defer accessProxy.Close()

	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return err
	}
	playerID, err := r.pickPlayerIDForShard(calculator, shard)
	if err != nil {
		return err
	}
	farmShard, err := calculator.ShardFor(actorrouting.DomainFarm, playerID)
	if err != nil {
		return err
	}
	farmFence, err := r.routectlShardFence("FARM", farmShard)
	if err != nil {
		return err
	}
	farmHolder := r.holderByID(farmFence.HolderID)
	if farmHolder == nil || farmHolder.process == nil {
		return fmt.Errorf("S6 farm holder for shard %d unavailable", farmShard)
	}
	// Preflight Authority budget: registry wait (~30s) + readiness must finish
	// while FARM Authority remains ACTIVE for same-Boot renew.
	if err := r.waitAuthorityLeaseRemaining("FARM", farmShard, 60*time.Second, 55*time.Second); err != nil {
		return err
	}

	beforeCounters := sumSnapshotCounters(r.snapshotProxies)
	httpBase := "http://" + accessProxy.Address()
	wsURL := "ws://" + accessProxy.Address() + "/ws"
	command := exec.CommandContext(r.ctx, r.binaries["rt2q-business-driver"].path,
		"--mode", "s6-reconcile",
		"--http-bases", httpBase, "--ws-urls", wsURL,
		"--challenge", r.challenge, "--player-start", strconv.FormatUint(playerID, 10),
		"--timeout", "6m",
	)
	command.Env = append(os.Environ(), "RT2Q_BUSINESS_SECRET="+r.secret)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var (
		gotT1, gotT2, gotT3 bool
		gotLost, gotReplay  bool
		gotRejoin           bool
		done                bool
		farmStopped         bool
	)
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			_ = command.Process.Kill()
			return err
		}
		kind, _ := event["kind"].(string)
		switch kind {
		case "waiting_rejoin":
			if !farmStopped {
				if farmHolder.logicProxy == nil {
					_ = command.Process.Kill()
					return errors.New("S6 farm logic TCP fault proxy is unavailable")
				}
				// Owned proxy seam: drop Gate→Farm renew RPCs until the short
				// subscription lease expires, without killing farm-svr (same-Boot
				// reclaim under Authority membership cannot serve Join in-budget).
				farmHolder.logicProxy.ArmDrop()
				farmStopped = true
				_ = r.writer.Observe("rpc-faults.ndjson", "S6", "farm_logic_drop_armed", r.harness, map[string]any{
					"holder": farmHolder.id, "farm_shard": farmShard, "reason": "subscription_lease_loss",
				})
				dropDeadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(dropDeadline) {
					select {
					case <-r.ctx.Done():
						farmHolder.logicProxy.ClearDrop()
						_ = command.Process.Kill()
						return r.ctx.Err()
					case <-time.After(100 * time.Millisecond):
					}
				}
				farmHolder.logicProxy.ClearDrop()
				_ = r.writer.Observe("rpc-faults.ndjson", "S6", "farm_logic_drop_cleared", r.harness, map[string]any{
					"holder": farmHolder.id, "farm_shard": farmShard, "health": httpReady(farmHolder.ports.health),
				})
			}
		case "rejoin_required":
			// Logic drop already cleared; farm process stayed live.
		case "t2_send":
			accessProxy.ArmDropNextCommandResult()
		case "t2_replay_session":
			if err := r.writer.Observe("business.ndjson", "S6", "t2_replay_session", r.harness, map[string]any{
				"ok": event["ok"] == true, "fresh_session": true,
			}); err != nil {
				_ = command.Process.Kill()
				return err
			}
		case "business_receipt":
			op, _ := event["operation"].(string)
			count := intFromAny(event["count"])
			if err := r.writer.Observe("business.ndjson", "S6", "business_receipt", r.harness, map[string]any{
				"operation": op, "count": count, "ok": event["ok"] == true,
				"status": event["status"], "receipt_sha256": event["receipt_sha256"],
				"holder": authority.HolderID, "boot": authority.HolderBootID, "domain": domain, "shard": shard,
			}); err != nil {
				_ = command.Process.Kill()
				return err
			}
			switch op {
			case "T1":
				gotT1 = true
			case "T2":
				gotT2 = true
			case "T3":
				gotT3 = true
			}
		case "business_response_lost":
			dropped := false
			for _, proxyEvent := range accessProxy.Events() {
				if proxyEvent.Kind == "command_result_dropped" && proxyEvent.Dropped && proxyEvent.Committed {
					dropped = true
				}
			}
			if !dropped {
				_ = command.Process.Kill()
				return errors.New("S6 response-loss was not observed on the owned Gate access proxy")
			}
			accessProxy.ClearDrop()
			if err := r.writer.Observe("business.ndjson", "S6", "business_response_lost", r.harness, map[string]any{
				"operation": "T2", "committed": true, "proxy_drop": true,
			}); err != nil {
				_ = command.Process.Kill()
				return err
			}
			gotLost = true
		case "business_replay":
			sameReceipt := event["same_receipt"] == true
			duplicateReward := event["duplicate_reward"] == true
			if err := r.writer.Observe("business.ndjson", "S6", "business_replay", r.harness, map[string]any{
				"operation": "T2", "same_receipt": sameReceipt, "duplicate_reward": duplicateReward,
			}); err != nil {
				_ = command.Process.Kill()
				return err
			}
			// Driver always emits duplicate_reward; treat missing as false so
			// omitempty cannot falsely invalidate a same-receipt replay.
			gotReplay = sameReceipt && !duplicateReward
		case "subscription_rejoin":
			if err := r.writer.Observe("business.ndjson", "S6", "subscription_rejoin", r.harness, map[string]any{
				"reason":        "SUBSCRIPTION_LEASE_LOST",
				"fresh_attempt": event["fresh_attempt"] == true,
				"fresh_fence":   event["fresh_fence"] == true,
			}); err != nil {
				_ = command.Process.Kill()
				return err
			}
			gotRejoin = event["fresh_attempt"] == true && event["fresh_fence"] == true
		case "done":
			done = true
		}
	}
	if err := scanner.Err(); err != nil {
		_ = command.Process.Kill()
		return err
	}
	waitErr := command.Wait()
	if farmStopped && farmHolder.process == nil {
		_ = r.startHolder(farmHolder)
	}
	if waitErr != nil || !done || !gotT1 || !gotT2 || !gotT3 || !gotLost || !gotReplay || !gotRejoin {
		stderrClass := businessDriverStderrClass(stderr.String())
		_ = r.writer.Observe("business.ndjson", "S6", "business_driver_failed", r.harness, map[string]any{
			"wait_error": errString(waitErr), "stderr_class": stderrClass,
			"done": done, "t1": gotT1, "t2": gotT2, "t3": gotT3, "lost": gotLost, "replay": gotReplay, "rejoin": gotRejoin,
		})
		return fmt.Errorf("S6 business reconciliation driver failed: class=%s err=%v", stderrClass, waitErr)
	}
	afterCounters := sumSnapshotCounters(r.snapshotProxies)
	if err := r.writer.Observe("gate-observations.ndjson", "S6", "hot_path_counters", r.harness, map[string]any{
		"rpc_delta":    afterCounters.UnaryRPCs - beforeCounters.UnaryRPCs,
		"stream_delta": afterCounters.StreamMessages - beforeCounters.StreamMessages,
	}); err != nil {
		return err
	}
	final, err := r.waitRoutectlVerifyConsistent(planID, domain, shard, 20*time.Second)
	if err != nil {
		return err
	}
	if !final.Consistent || !verifyCheckOK(final, "gate_resolution") || !verifyCheckOK(final, "authority_tuple") || !verifyCheckOK(final, "snapshot_tuple") || !verifyCheckOK(final, "target_holder") {
		return errors.New("S6 final routectl verify four-truth binding failed")
	}
	match, err := r.snapshotSourceEndpointMatch(domain, shard, target)
	if err != nil || !match {
		return fmt.Errorf("S6 snapshot does not bind current holder: match=%v err=%v", match, err)
	}
	if err := r.observeFreshConsumerSnapshots("S6", wantBoot, nil); err != nil {
		return err
	}
	return r.writer.Observe("business.ndjson", "S6", "business_reconcile", r.harness, map[string]any{
		"authority": true, "snapshot": true, "holder": true, "plan": true,
		"plan_id": planID, "holder_id": authority.HolderID, "boot": authority.HolderBootID,
		"no_plan_replay": true, "verify_checks": len(final.Checks),
	})
}

func (r *runRuntime) restartFreshConsumers(gate *gateSpec) error {
	if gate == nil || gate.process == nil {
		return errors.New("S6 fresh Gate process is unavailable")
	}
	if err := r.stopManaged(gate.process, 10*time.Second); err != nil {
		return err
	}
	gate.process = nil
	if err := r.startGate(gate); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, gate.ports.http, 20*time.Second); err != nil {
		return fmt.Errorf("S6 replacement Gate did not bind HTTP: %w", err)
	}
	if r.workerProcess != nil {
		if err := r.stopManaged(r.workerProcess, 10*time.Second); err != nil {
			return err
		}
		r.workerProcess = nil
		r.workerBootGen++
		if err := r.startWorker(); err != nil {
			return err
		}
	}
	if err := r.waitConsumersReady(45 * time.Second); err != nil {
		return err
	}
	return r.writer.Observe("processes.ndjson", "S6", "fresh_consumers", r.harness, map[string]any{
		"gate": gate.name, "worker_new_boot": true, "gate_restarted": true,
	})
}

func (r *runRuntime) observeFreshConsumerSnapshots(scenario, wantBoot string, required []string) error {
	names := required
	if len(names) == 0 {
		names = make([]string, 0, len(r.snapshotProxies))
		for name := range r.snapshotProxies {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	for _, name := range names {
		proxy := r.snapshotProxies[name]
		if proxy == nil {
			return fmt.Errorf("%s missing snapshot proxy for %s", scenario, name)
		}
		var latest routecontrol.SnapshotProxyEvent
		for _, event := range proxy.Events() {
			if event.UpstreamOK && event.Boot == wantBoot && event.EnvelopeSHA256 != "" && event.Revision >= latest.Revision {
				latest = event
			}
		}
		if latest.Revision == 0 || latest.EnvelopeSHA256 == "" {
			return fmt.Errorf("%s consumer %s lacks authenticated complete Snapshot for boot %s", scenario, name, wantBoot)
		}
		if len(proxy.TLSFacts()) == 0 {
			return fmt.Errorf("%s consumer %s missing TLS peer facts", scenario, name)
		}
		if err := r.writer.Observe("snapshots.ndjson", scenario, "snapshot_installed", r.harness, map[string]any{
			"consumer": name, "revision": latest.Revision, "envelope_sha256": latest.EnvelopeSHA256,
			"boot": latest.Boot, "authenticated": true, "fresh_process": name == "worker" || strings.HasPrefix(name, "gate-"),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *runRuntime) holderByID(id string) *holderSpec {
	for _, holder := range r.holders {
		if holder != nil && holder.id == id {
			return holder
		}
	}
	return nil
}

func (r *runRuntime) pickPlayerIDForShard(calculator *actorrouting.Calculator, wantShard uint32) (uint64, error) {
	for id := uint64(920000000); id < 920010000; id++ {
		got, err := calculator.ShardFor(actorrouting.DomainPlayer, id)
		if err != nil {
			return 0, err
		}
		if got == wantShard {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no test player maps to PLAYER shard %d", wantShard)
}

func (r *runRuntime) waitHolderServing(holder *holderSpec, timeout time.Duration) error {
	if holder == nil {
		return errors.New("holder is nil")
	}
	if err := waitTCPListener(r.ctx, holder.ports.logic, timeout); err != nil {
		return fmt.Errorf("holder %s logic listener: %w", holder.name, err)
	}
	deadline := time.Now().Add(timeout)
	for {
		if httpReady(holder.ports.health) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("holder %s health did not become ready", holder.name)
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (r *runRuntime) classifyHolderLogTail(holderName string) string {
	logDir := filepath.Join(r.tempRoot, "logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return ""
	}
	var newest string
	var newestMod time.Time
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, holderName+"-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			continue
		}
		if info.ModTime().After(newestMod) {
			newestMod = info.ModTime()
			newest = filepath.Join(logDir, name)
		}
	}
	if newest == "" {
		return ""
	}
	raw, err := os.ReadFile(newest)
	if err != nil || len(raw) == 0 {
		return "log_empty"
	}
	text := string(raw)
	switch {
	case strings.Contains(text, "LEASE_EXPIRED"):
		return "authority_lease_expired"
	case strings.Contains(text, "FENCE_MISMATCH"):
		return "authority_fence_mismatch"
	case strings.Contains(text, "holder registry"), strings.Contains(text, "HolderRegistry"), strings.Contains(text, "registry reject"):
		return "holder_registry"
	case strings.Contains(text, "WaitHealthy"):
		return "assignment_wait_healthy"
	case strings.Contains(text, "OnFatal"), strings.Contains(text, "scheduler fatal"):
		return "membership_fatal"
	case strings.Contains(text, "ProcessReady"):
		return "process_ready"
	case strings.Contains(text, "not healthy"), strings.Contains(text, "AssignmentComposition"):
		return "assignment_unhealthy"
	case strings.Contains(text, "not ready"), strings.Contains(text, "not_ready"):
		return "not_ready"
	case strings.Contains(text, "fatal"), strings.Contains(text, "FATAL"), strings.Contains(text, "panic"):
		return "fatal"
	default:
		// Keep only a coarse token from the last non-empty line; never raw secrets.
		lines := strings.Split(strings.TrimSpace(text), "\n")
		for i := len(lines) - 1; i >= 0 && i >= len(lines)-5; i-- {
			line := strings.ToLower(lines[i])
			for _, token := range []string{"authority", "membership", "snapshot", "holder", "lease", "ready", "error", "warn"} {
				if strings.Contains(line, token) {
					return "tail_" + token
				}
			}
		}
		return "log_present"
	}
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case json.Number:
		n, _ := typed.Int64()
		return int(n)
	default:
		return 0
	}
}

func (r *runRuntime) runS4() error {
	if len(r.gates) < 2 || r.gates[0].verifierProxy == nil || r.gates[1].verifierProxy == nil {
		return errors.New("S4 requires two Gate verifier forward proxies")
	}
	planID, domain, shard, err := r.ensureSucceededPlayerPlan("S4")
	if err != nil {
		return err
	}
	baseline, err := r.waitRoutectlVerifyConsistent(planID, domain, shard, 15*time.Second)
	if err != nil {
		return err
	}
	if !baseline.Consistent || len(baseline.Checks) != 11 || !verifyCheckOK(baseline, "gate_resolution") {
		_ = r.writer.Observe("plans.ndjson", "S4", "s4_baseline_not_consistent", r.harness, verifyReportPayload(planID, baseline))
		return fmt.Errorf("S4 baseline verify is not fully consistent: consistent=%v checks=%d", baseline.Consistent, len(baseline.Checks))
	}
	faultGate := r.gates[0]
	currentGate := r.gates[1]
	faults := []routecontrol.GateVerifierFault{
		routecontrol.GateFaultPending,
		routecontrol.GateFaultStaleRevision,
		routecontrol.GateFaultUnavailable,
		routecontrol.GateFaultUnknown,
		routecontrol.GateFaultMissingReplay,
	}
	for _, fault := range faults {
		before := len(faultGate.verifierProxy.Events())
		if err := faultGate.verifierProxy.ArmFault(fault); err != nil {
			return err
		}
		report, err := r.routectlVerifyReport(planID, domain, shard)
		if err != nil {
			faultGate.verifierProxy.ClearFault()
			_ = r.writer.Observe("plans.ndjson", "S4", "s4_fault_verify_error", r.harness, map[string]any{
				"case": string(fault), "error": errString(err),
			})
			return err
		}
		if report.Consistent || verifyCheckOK(report, "gate_resolution") {
			faultGate.verifierProxy.ClearFault()
			_ = r.writer.Observe("plans.ndjson", "S4", "s4_fault_not_fail_closed", r.harness, verifyReportPayload(planID, report))
			return fmt.Errorf("S4 fault %s did not fail-closed on gate_resolution", fault)
		}
		if !verifyCheckOK(report, "authority_tuple") || !verifyCheckOK(report, "snapshot_tuple") {
			faultGate.verifierProxy.ClearFault()
			_ = r.writer.Observe("plans.ndjson", "S4", "s4_fault_disturbed_facts", r.harness, verifyReportPayload(planID, report))
			return fmt.Errorf("S4 fault %s disturbed Authority/Snapshot facts", fault)
		}
		if !verifyCheckOK(report, "target_holder") {
			faultGate.verifierProxy.ClearFault()
			_ = r.writer.Observe("plans.ndjson", "S4", "s4_fault_disturbed_target", r.harness, verifyReportPayload(planID, report))
			return fmt.Errorf("S4 fault %s disturbed target_holder facts", fault)
		}
		applied := false
		for _, event := range faultGate.verifierProxy.Events()[before:] {
			if event.Fault != string(fault) {
				continue
			}
			if fault == routecontrol.GateFaultUnavailable {
				applied = !event.UpstreamOK && event.ErrorCode == "Unavailable"
			} else {
				applied = event.UpstreamOK && event.Mutated
			}
		}
		if !applied {
			faultGate.verifierProxy.ClearFault()
			return fmt.Errorf("S4 fault %s was not applied on authenticated ObserveShard", fault)
		}
		failed := 0
		for _, check := range report.Checks {
			if !check.OK {
				failed++
			}
		}
		payload := map[string]any{
			"case": string(fault), "nonzero": failed > 0, "consistent": false,
			"gate_resolution": false, "authority_tuple": true, "snapshot_tuple": true,
			"failed_checks": failed, "plan_id": planID,
		}
		if fault == routecontrol.GateFaultStaleRevision {
			if err := r.observeS4StalePeerBinding(faultGate, currentGate, payload); err != nil {
				faultGate.verifierProxy.ClearFault()
				return err
			}
		}
		if err := r.writer.Observe("plans.ndjson", "S4", "verify_negative", r.harness, payload); err != nil {
			faultGate.verifierProxy.ClearFault()
			return err
		}
		faultGate.verifierProxy.ClearFault()
	}
	// Bounded convergence: cleared fault must forward an unmodified ObserveShard.
	final, err := r.waitRoutectlVerifyConsistent(planID, domain, shard, 15*time.Second)
	if err != nil {
		return err
	}
	if !final.Consistent || len(final.Checks) != 11 {
		_ = r.writer.Observe("plans.ndjson", "S4", "s4_final_not_consistent", r.harness, verifyReportPayload(planID, final))
		return fmt.Errorf("S4 final verify is not fully consistent: consistent=%v checks=%d", final.Consistent, len(final.Checks))
	}
	identical := false
	for _, event := range faultGate.verifierProxy.Events() {
		if event.UpstreamOK && event.Identical && event.Fault == "" {
			identical = true
		}
	}
	if !identical {
		return errors.New("S4 did not observe an identical clear ObserveShard forward")
	}
	if err := r.writer.Observe("snapshots.ndjson", "S4", "snapshot_release", r.harness, map[string]any{
		"identical": true, "peer": faultGate.name, "plan_id": planID,
	}); err != nil {
		return err
	}
	return r.writer.Observe("plans.ndjson", "S4", "verify_final", r.harness, map[string]any{
		"zero": true, "checks": len(final.Checks), "consistent": true, "plan_id": planID,
	})
}

func (r *runRuntime) ensureSucceededPlayerPlan(scenario string) (planID, domain string, shard uint32, err error) {
	source := r.holderByName("player-a")
	target := r.holderByName("player-b")
	if source == nil || target == nil || source.holderProxy == nil {
		return "", "", 0, fmt.Errorf("%s requires PLAYER A/B holders with HolderControl proxies", scenario)
	}
	domain = "PLAYER"
	shard = 0
	if err := r.waitHoldersMoveReady(scenario, domain, source, target, 45*time.Second); err != nil {
		return "", "", 0, err
	}
	fence, err := r.routectlShardFence(domain, shard)
	if err != nil {
		return "", "", 0, err
	}
	if fence.HolderID != source.id || fence.HolderBootID != source.boot {
		return "", "", 0, fmt.Errorf("%s source fence does not match PLAYER A", scenario)
	}
	if err := source.holderProxy.ArmStopHold(domain, shard); err != nil {
		return "", "", 0, err
	}
	label := strings.ToLower(scenario)
	planID = fmt.Sprintf("rt2q-%s-%s-move", r.challenge, label)
	transitionID := fmt.Sprintf("rt2q-%s-%s-transition", r.challenge, label)
	moveArgs := r.moveShardArgs(planID, transitionID, domain, shard, source, target, fence)
	type moveResult struct {
		output []byte
		err    error
	}
	moveDone := make(chan moveResult, 1)
	go func() {
		output, moveErr := r.runRoutectl(45*time.Second, moveArgs...)
		moveDone <- moveResult{output: output, err: moveErr}
	}()
	holdCtx, holdCancel := context.WithTimeout(r.ctx, 25*time.Second)
	defer holdCancel()
	if err := source.holderProxy.WaitStopHeld(holdCtx); err != nil {
		<-moveDone
		return "", "", 0, fmt.Errorf("%s source Stop was not held: %w", scenario, err)
	}
	if err := source.holderProxy.ReleaseStop(); err != nil {
		<-moveDone
		return "", "", 0, err
	}
	move := <-moveDone
	var parsed struct {
		Plan struct {
			Phase    string `json:"phase"`
			Revision uint64 `json:"revision"`
		} `json:"plan"`
	}
	if move.err != nil || json.Unmarshal(move.output, &parsed) != nil || parsed.Plan.Phase != "SUCCEEDED" {
		return "", "", 0, fmt.Errorf("%s move-shard did not SUCCEEDED: %v", scenario, move.err)
	}
	if err := r.writer.Observe("plans.ndjson", scenario, "plan_phase", r.harness, map[string]any{
		"plan_id": planID, "phase": parsed.Plan.Phase, "revision": parsed.Plan.Revision,
	}); err != nil {
		return "", "", 0, err
	}
	return planID, domain, shard, nil
}

type verifyReport struct {
	Consistent bool
	Checks     []struct {
		Name string `json:"name"`
		OK   bool   `json:"ok"`
		Code string `json:"error_code"`
	} `json:"checks"`
	Plan struct {
		Phase string `json:"phase"`
	} `json:"plan"`
}

func (r *runRuntime) routectlVerifyReport(planID, domain string, shard uint32) (verifyReport, error) {
	output, err := r.runRoutectl(10*time.Second, "verify", "-plan-id", planID, "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10))
	var report verifyReport
	if len(output) > 0 {
		_ = json.Unmarshal(output, &report)
	}
	if err != nil && report.Plan.Phase == "" && len(report.Checks) == 0 {
		return report, err
	}
	// routectl exits non-zero when consistent=false; still return the parsed report.
	return report, nil
}

func (r *runRuntime) waitRoutectlVerifyConsistent(planID, domain string, shard uint32, timeout time.Duration) (verifyReport, error) {
	deadline := time.Now().Add(timeout)
	var report verifyReport
	for {
		current, err := r.routectlVerifyReport(planID, domain, shard)
		if err == nil && current.Consistent && current.Plan.Phase == "SUCCEEDED" && len(current.Checks) == 11 {
			return current, nil
		}
		if err == nil {
			report = current
		}
		if time.Now().After(deadline) {
			scenario := "S4"
			if len(r.scenarios) > 0 {
				scenario = r.scenarios[len(r.scenarios)-1]
			}
			_ = r.writer.Observe("plans.ndjson", scenario, "verify_not_converged", r.harness, verifyReportPayload(planID, report))
			if err != nil {
				return report, err
			}
			return report, fmt.Errorf("%s routectl verify did not converge: consistent=%v phase=%s checks=%d", scenario, report.Consistent, report.Plan.Phase, len(report.Checks))
		}
		select {
		case <-r.ctx.Done():
			return report, r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func verifyReportPayload(planID string, report verifyReport) map[string]any {
	checks := make(map[string]bool, len(report.Checks))
	for _, check := range report.Checks {
		checks[check.Name] = check.OK
	}
	return map[string]any{
		"plan_id": planID, "phase": report.Plan.Phase, "consistent": report.Consistent,
		"checks": checks, "check_count": len(report.Checks),
	}
}

func verifyCheckOK(report verifyReport, name string) bool {
	for _, check := range report.Checks {
		if check.Name == name {
			return check.OK
		}
	}
	return false
}

func (r *runRuntime) observeS4StalePeerBinding(faultGate, currentGate *gateSpec, payload map[string]any) error {
	if faultGate == nil || currentGate == nil || faultGate.verifierProxy == nil || currentGate.verifierProxy == nil {
		return errors.New("S4 stale peer proxies are unavailable")
	}
	var staleEvent, currentEvent *routecontrol.GateVerifierProxyEvent
	for index := range faultGate.verifierProxy.Events() {
		event := faultGate.verifierProxy.Events()[index]
		if event.Fault == string(routecontrol.GateFaultStaleRevision) && event.Mutated && event.UpstreamOK {
			copyEvent := event
			staleEvent = &copyEvent
		}
	}
	for index := range currentGate.verifierProxy.Events() {
		event := currentGate.verifierProxy.Events()[index]
		if event.UpstreamOK && event.Fault == "" && !event.Mutated {
			copyEvent := event
			currentEvent = &copyEvent
		}
	}
	if staleEvent == nil || currentEvent == nil {
		return errors.New("S4 missing authenticated stale/current Gate ObserveShard events")
	}
	if staleEvent.SnapshotRevision == 0 || currentEvent.SnapshotRevision == 0 || staleEvent.SnapshotRevision >= currentEvent.SnapshotRevision {
		return fmt.Errorf("S4 stale revision binding invalid: stale=%d current=%d", staleEvent.SnapshotRevision, currentEvent.SnapshotRevision)
	}
	if len(faultGate.verifierProxy.TLSFacts()) == 0 || len(currentGate.verifierProxy.TLSFacts()) == 0 {
		return errors.New("S4 missing TLS peer facts for Gate verifier proxies")
	}
	payload["stale_peer"] = faultGate.name
	payload["current_peer"] = currentGate.name
	payload["stale_snapshot_revision"] = staleEvent.SnapshotRevision
	payload["current_snapshot_revision"] = currentEvent.SnapshotRevision
	payload["stale_assignment_revision"] = staleEvent.AssignmentRevision
	payload["stale_writer_epoch"] = staleEvent.WriterEpoch
	payload["tls_ok"] = true
	return r.writer.Observe("gate-observations.ndjson", "S4", "gate_peer_observation", r.harness, map[string]any{
		"stale_peer": true, "current_peer": true, "tls_ok": true,
		"stale_name": faultGate.name, "current_name": currentGate.name,
		"stale_snapshot_revision": staleEvent.SnapshotRevision, "current_snapshot_revision": currentEvent.SnapshotRevision,
		"stale_writer_epoch": staleEvent.WriterEpoch, "current_writer_epoch": currentEvent.WriterEpoch,
	})
}

func (r *runRuntime) runS3() error {
	source := r.holderByName("farm-a")
	target := r.holderByName("farm-b")
	if source == nil || target == nil || source.holderProxy == nil || target.holderProxy == nil {
		return errors.New("S3 requires FARM A/B holders with HolderControl proxies")
	}
	if r.redisProxy == nil {
		return errors.New("S3 requires the audited RESP proxy")
	}
	const domain = "FARM"
	const shard uint32 = 0
	if err := r.waitHoldersMoveReady("S3", domain, source, target, 45*time.Second); err != nil {
		return err
	}
	fence, err := r.routectlShardFence(domain, shard)
	if err != nil {
		_ = r.writer.Observe("plans.ndjson", "S3", "s3_fence_unavailable", r.harness, map[string]any{"domain": domain, "shard": shard, "error": errString(err)})
		return err
	}
	if fence.HolderID != source.id || fence.HolderBootID != source.boot {
		return errors.New("S3 source fence does not match FARM A")
	}
	transferKey, transferSHA, err := r.farmTransferIdentity(shard)
	if err != nil {
		return err
	}
	if err := r.redisProxy.ArmReplyLoss("controller", transferKey, transferSHA, true); err != nil {
		return err
	}
	planID := fmt.Sprintf("rt2q-%s-s3-move", r.challenge)
	transitionID := fmt.Sprintf("rt2q-%s-s3-transition", r.challenge)
	moveArgs := r.moveShardArgs(planID, transitionID, domain, shard, source, target, fence)
	type moveResult struct {
		output []byte
		err    error
	}
	moveDone := make(chan moveResult, 1)
	go func() {
		output, moveErr := r.runRoutectl(45*time.Second, moveArgs...)
		moveDone <- moveResult{output: output, err: moveErr}
	}()
	faultCtx, faultCancel := context.WithTimeout(r.ctx, 40*time.Second)
	defer faultCancel()
	if err := r.redisProxy.WaitFaultTriggered(faultCtx); err != nil {
		r.redisProxy.ClearReplyLoss()
		move := <-moveDone
		_ = r.writer.Observe("plans.ndjson", "S3", "s3_reply_loss_not_triggered", r.harness, map[string]any{
			"error": errString(err), "move_error": errString(move.err), "move_output_len": len(move.output),
		})
		return fmt.Errorf("S3 transfer reply-loss barrier was not reached: %w", err)
	}
	if err := r.observeS3TransferFault(transferSHA); err != nil {
		r.redisProxy.ClearReplyLoss()
		<-moveDone
		return err
	}
	if err := r.proveS3FourTruth(planID, transitionID, domain, shard, source, target); err != nil {
		r.redisProxy.ClearReplyLoss()
		<-moveDone
		return err
	}
	// Stop/Wait/Facts are complete before Transfer; record them at the barrier so
	// a later recovery failure still preserves once-only pre-authority evidence.
	if err := r.observeS3DrainCallbacksAtBarrier(source.holderProxy, domain, shard); err != nil {
		r.redisProxy.ClearReplyLoss()
		<-moveDone
		return err
	}
	if err := r.stopManaged(r.controller, 10*time.Second); err != nil {
		r.redisProxy.ClearReplyLoss()
		<-moveDone
		return err
	}
	r.redisProxy.ClearReplyLoss()
	move := <-moveDone
	_ = r.writer.Observe("processes.ndjson", "S3", "controller_killed_at_barrier", r.harness, map[string]any{
		"plan_id": planID, "transition_id": transitionID, "move_error": errString(move.err), "move_output_len": len(move.output),
	})
	r.controllerIndex++
	if err := r.startController(); err != nil {
		return err
	}
	if err := waitTCPListener(r.ctx, r.controllerAddr, 30*time.Second); err != nil {
		return errors.New("S3 recovery route-controller did not open its listener")
	}
	if err := r.waitControllerReady(45 * time.Second); err != nil {
		return err
	}
	if err := r.waitConsumersReady(45 * time.Second); err != nil {
		return err
	}
	if err := r.waitS3PlanSucceeded(planID, 60*time.Second); err != nil {
		_ = r.observeS3RecoveryFailure(planID, domain, shard, source, target)
		return err
	}
	if err := r.observeS3PrepareOnce(domain, shard, source.holderProxy, target.holderProxy); err != nil {
		return err
	}
	if err := r.writer.Observe("authority.ndjson", "S3", "authority_transfer", r.harness, map[string]any{
		"domain": domain, "shard": shard, "source": source.id, "target": target.id,
		"plan_id": planID, "transition_id": transitionID, "once": true,
	}); err != nil {
		return err
	}
	return r.observeS3FinalFacts(planID, domain, shard, source, target, transitionID)
}

func (r *runRuntime) runS2() error {
	source := r.holderByName("player-a")
	target := r.holderByName("player-b")
	if source == nil || target == nil || source.holderProxy == nil {
		return errors.New("S2 requires PLAYER A/B holders with HolderControl proxies")
	}
	const domain = "PLAYER"
	const shard uint32 = 0
	if err := r.waitS2HoldersMoveReady(domain, source, target, 45*time.Second); err != nil {
		return err
	}
	// Authority leases renew under the holders; the expected fence must be read
	// only after move-ready preflight so SubmitMove does not see a stale epoch.
	fence, err := r.routectlShardFence(domain, shard)
	if err != nil {
		_ = r.writer.Observe("plans.ndjson", "S2", "s2_fence_unavailable", r.harness, map[string]any{"domain": domain, "shard": shard, "error": errString(err)})
		return err
	}
	if fence.HolderID != source.id || fence.HolderBootID != source.boot {
		_ = r.writer.Observe("plans.ndjson", "S2", "s2_fence_mismatch", r.harness, map[string]any{
			"want_holder": source.id, "got_holder_match": fence.HolderID == source.id, "got_boot_match": fence.HolderBootID == source.boot,
		})
		return errors.New("S2 source fence does not match PLAYER A")
	}
	if err := source.holderProxy.ArmStopHold(domain, shard); err != nil {
		return err
	}
	planID := fmt.Sprintf("rt2q-%s-s2-move", r.challenge)
	transitionID := fmt.Sprintf("rt2q-%s-s2-transition", r.challenge)
	moveArgs := r.moveShardArgs(planID, transitionID, domain, shard, source, target, fence)
	type moveResult struct {
		output []byte
		err    error
	}
	moveDone := make(chan moveResult, 1)
	var lastPhaseRevision atomic.Uint64
	// Do not poll Plan status until Stop is held. Concurrent GetStatus/ListIncomplete
	// has been observed to leave move-shard at PENDING/attempt=0 (Execute returned
	// before bumpAttempt) under this harness topology.
	go func() {
		output, moveErr := r.runRoutectl(30*time.Second, moveArgs...)
		moveDone <- moveResult{output: output, err: moveErr}
	}()
	holdCtx, holdCancel := context.WithTimeout(r.ctx, 25*time.Second)
	defer holdCancel()
	holdDone := make(chan error, 1)
	go func() { holdDone <- source.holderProxy.WaitStopHeld(holdCtx) }()
	var move moveResult
	select {
	case holdErr := <-holdDone:
		if holdErr != nil {
			move = <-moveDone
			_ = r.writer.Observe("plans.ndjson", "S2", "s2_stop_not_held", r.harness, r.s2MoveFailurePayload(source, move.err, move.output, holdErr))
			return fmt.Errorf("S2 source Stop was not held: %w", holdErr)
		}
		// Record PENDING once from durable status now that Execute has entered Drain.
		if err := r.observeS2PlanPhaseOnce(planID, &lastPhaseRevision); err != nil {
			_ = source.holderProxy.ReleaseStop()
			<-moveDone
			return err
		}
		if err := source.holderProxy.ReleaseStop(); err != nil {
			<-moveDone
			return err
		}
		phaseCtx, phaseCancel := context.WithCancel(r.ctx)
		phaseDone := make(chan error, 1)
		go func() { phaseDone <- r.pollPlanPhases(phaseCtx, planID, &lastPhaseRevision) }()
		move = <-moveDone
		return r.finishS2Move(planID, domain, shard, source, target, move.output, move.err, phaseCancel, phaseDone, &lastPhaseRevision)
	case move = <-moveDone:
		holdCancel()
		_ = <-holdDone
		_ = r.writer.Observe("plans.ndjson", "S2", "s2_stop_not_held", r.harness, r.s2MoveFailurePayload(source, move.err, move.output, errors.New("move returned before Stop was held")))
		return errors.New("S2 move-shard returned before source Stop was held")
	}
}

func (r *runRuntime) observeS2PlanPhaseOnce(planID string, lastRevision *atomic.Uint64) error {
	output, err := r.runRoutectl(routectlStatusPollRPCTimeout, "status")
	if err != nil {
		return nil
	}
	var status struct {
		IncompletePlans []struct {
			PlanID   string `json:"plan_id"`
			Phase    string `json:"phase"`
			Revision uint64 `json:"revision"`
		} `json:"incomplete_plans"`
	}
	if json.Unmarshal(output, &status) != nil {
		return nil
	}
	allowed := map[string]struct{}{
		"PENDING": {}, "DRAINING": {}, "ASSIGNING": {}, "PREPARING": {}, "ACTIVE": {}, "SUCCEEDED": {},
	}
	for _, plan := range status.IncompletePlans {
		if plan.PlanID != planID || plan.Phase == "" {
			continue
		}
		if _, ok := allowed[plan.Phase]; !ok {
			continue
		}
		previous := lastRevision.Load()
		if plan.Revision <= previous {
			continue
		}
		if !lastRevision.CompareAndSwap(previous, plan.Revision) {
			continue
		}
		return r.writer.Observe("plans.ndjson", "S2", "plan_phase", r.harness, map[string]any{
			"plan_id": planID, "phase": plan.Phase, "revision": plan.Revision,
		})
	}
	return nil
}

func (r *runRuntime) finishS2Move(planID, domain string, shard uint32, source, target *holderSpec, moveOutput []byte, moveErr error, phaseCancel context.CancelFunc, phaseDone <-chan error, lastPhaseRevision *atomic.Uint64) error {
	var parsed struct {
		Plan struct {
			Phase     string `json:"phase"`
			ErrorCode string `json:"error_code"`
			Revision  uint64 `json:"revision"`
			Attempt   uint32 `json:"attempt"`
		} `json:"plan"`
	}
	if len(moveOutput) == 0 || json.Unmarshal(moveOutput, &parsed) != nil || parsed.Plan.Revision == 0 {
		phaseCancel()
		_ = <-phaseDone
		_ = r.writer.Observe("plans.ndjson", "S2", "s2_move_failed", r.harness, r.s2MoveFailurePayload(source, moveErr, moveOutput, nil))
		return fmt.Errorf("production routectl move-shard failed: %w", moveErr)
	}
	if parsed.Plan.Phase != "SUCCEEDED" {
		phaseCancel()
		_ = <-phaseDone
		payload := r.s2MoveFailurePayload(source, moveErr, moveOutput, nil)
		r.enrichS2MoveFailureDiagnostics(payload, planID, domain, shard, source, target)
		_ = r.writer.Observe("plans.ndjson", "S2", "s2_move_failed", r.harness, payload)
		if parsed.Plan.Phase == "PAUSED" && parsed.Plan.ErrorCode == "authority_committed_snapshot_pending" {
			// Production treats PAUSED as a terminal audit record; Manager recovery
			// may publish the snapshot later but Execute will not advance to SUCCEEDED.
			return errors.New("production move-shard terminalized PAUSED with authority_committed_snapshot_pending")
		}
		return fmt.Errorf("production move-shard ended in phase %s", parsed.Plan.Phase)
	}
	if parsed.Plan.Revision > lastPhaseRevision.Load() {
		if err := r.writer.Observe("plans.ndjson", "S2", "plan_phase", r.harness, map[string]any{
			"plan_id": planID, "phase": parsed.Plan.Phase, "revision": parsed.Plan.Revision,
		}); err != nil {
			phaseCancel()
			_ = <-phaseDone
			return err
		}
		lastPhaseRevision.Store(parsed.Plan.Revision)
	}
	phaseCancel()
	if phaseErr := <-phaseDone; phaseErr != nil {
		return phaseErr
	}
	if err := r.observeHolderCallbacks("S2", []*routecontrol.HolderForwardProxy{source.holderProxy, target.holderProxy}, domain, shard); err != nil {
		return err
	}
	return r.writer.Observe("authority.ndjson", "S2", "authority_transfer", r.harness, map[string]any{
		"domain": domain, "shard": shard, "source": source.id, "target": target.id, "plan_id": planID,
	})
}

func (r *runRuntime) enrichS2MoveFailureDiagnostics(payload map[string]any, planID, domain string, shard uint32, source, target *holderSpec) {
	if payload == nil {
		return
	}
	if rev, err := r.routectlSnapshotRevision(); err == nil {
		payload["diag_snapshot_revision"] = rev
	} else {
		payload["diag_snapshot_error"] = errString(err)
	}
	if holders, err := r.routectlHolders(domain); err == nil {
		if sourceView, ok := findRoutectlHolder(holders, source.id); ok {
			payload["diag_source_state"] = sourceView.State
			payload["diag_source_snapshot_revision"] = sourceView.SnapshotRevision
			payload["diag_source_draining"] = sourceView.Draining
		}
		if targetView, ok := findRoutectlHolder(holders, target.id); ok {
			payload["diag_target_state"] = targetView.State
			payload["diag_target_snapshot_revision"] = targetView.SnapshotRevision
			payload["diag_target_draining"] = targetView.Draining
			payload["diag_target_serving_shards"] = targetView.ServingShardCount
		}
	} else {
		payload["diag_holders_error"] = errString(err)
	}
	if match, err := r.snapshotSourceEndpointMatch(domain, shard, source); err == nil {
		payload["diag_snapshot_still_source"] = match
	} else {
		payload["diag_snapshot_route_error"] = errString(err)
	}
	if target != nil {
		if match, err := r.snapshotSourceEndpointMatch(domain, shard, target); err == nil {
			payload["diag_snapshot_now_target"] = match
		}
	}
	verifyOut, verifyErr := r.runRoutectl(5*time.Second, "verify", "-plan-id", planID, "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10))
	payload["diag_verify_error"] = errString(verifyErr)
	if len(verifyOut) > 0 {
		var verify struct {
			Consistent bool `json:"consistent"`
			Plan       struct {
				Phase     string `json:"phase"`
				ErrorCode string `json:"error_code"`
				Revision  uint64 `json:"revision"`
			} `json:"plan"`
		}
		if json.Unmarshal(verifyOut, &verify) == nil {
			payload["diag_verify_consistent"] = verify.Consistent
			payload["diag_verify_phase"] = verify.Plan.Phase
			payload["diag_verify_error_code"] = verify.Plan.ErrorCode
			payload["diag_verify_revision"] = verify.Plan.Revision
		}
	}
}

func (r *runRuntime) moveShardArgs(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence) []string {
	return []string{
		"move-shard",
		"-plan-id", planID, "-transition-id", transitionID, "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10),
		"-source-holder-id", source.id, "-source-boot-id", source.boot, "-source-endpoint", source.ports.logic,
		"-target-holder-id", target.id, "-target-boot-id", target.boot, "-target-endpoint", target.ports.logic,
		"-requested-ttl-ms", "60000",
		"-expected-holder-id", fence.HolderID, "-expected-holder-boot-id", fence.HolderBootID,
		"-expected-assignment-revision", strconv.FormatUint(fence.AssignmentRevision, 10),
		"-expected-writer-epoch", strconv.FormatUint(fence.WriterEpoch, 10),
		"-expected-lease-epoch", strconv.FormatUint(fence.LeaseEpoch, 10),
	}
}

func (r *runRuntime) routectlSnapshotRevision() (uint64, error) {
	output, err := r.runRoutectl(routectlStatusPollRPCTimeout, "status")
	if err != nil {
		return 0, err
	}
	var status struct {
		SnapshotRevision uint64 `json:"snapshot_revision"`
		Ready            bool   `json:"ready"`
	}
	if json.Unmarshal(output, &status) != nil || !status.Ready || status.SnapshotRevision == 0 {
		return 0, errors.New("routectl status did not return a ready snapshot revision")
	}
	return status.SnapshotRevision, nil
}

type routectlHolderView struct {
	HolderID          string `json:"holder_id"`
	BootID            string `json:"boot_id"`
	LogicEndpoint     string `json:"logic_endpoint"`
	TLSDNSIdentity    string `json:"tls_dns_identity"`
	State             string `json:"state"`
	ProcessReady      bool   `json:"process_ready"`
	Draining          bool   `json:"draining"`
	SnapshotRevision  uint64 `json:"snapshot_revision"`
	CapacityWeight    uint32 `json:"capacity_weight"`
	ServingShardCount uint32 `json:"serving_shard_count"`
}

func (r *runRuntime) routectlHolders(domain string) ([]routectlHolderView, error) {
	output, err := r.runRoutectl(5*time.Second, "holders", "-domain", domain)
	if err != nil {
		return nil, err
	}
	var response struct {
		Holders []routectlHolderView `json:"holders"`
	}
	if json.Unmarshal(output, &response) != nil {
		return nil, errors.New("routectl holders response is not valid JSON")
	}
	return response.Holders, nil
}

func (r *runRuntime) waitS2HoldersMoveReady(domain string, source, target *holderSpec, timeout time.Duration) error {
	return r.waitHoldersMoveReady("S2", domain, source, target, timeout)
}

func (r *runRuntime) waitHoldersMoveReady(scenario, domain string, source, target *holderSpec, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for {
		snapshotRevision, err := r.routectlSnapshotRevision()
		if err != nil {
			_ = r.writer.Observe("plans.ndjson", scenario, "holder_status_unavailable", r.harness, map[string]any{"error": errString(err)})
			return err
		}
		holders, err := r.routectlHolders(domain)
		if err != nil {
			_ = r.writer.Observe("plans.ndjson", scenario, "holders_unavailable", r.harness, map[string]any{"error": errString(err)})
			return err
		}
		sourceView, sourceOK := findRoutectlHolder(holders, source.id)
		targetView, targetOK := findRoutectlHolder(holders, target.id)
		last = map[string]any{
			"snapshot_revision": snapshotRevision,
			"source_found":      sourceOK,
			"target_found":      targetOK,
		}
		if sourceOK {
			last["source_endpoint_match"] = sourceView.LogicEndpoint == source.ports.logic
			last["source_boot_match"] = sourceView.BootID == source.boot
			last["source_state"] = sourceView.State
			last["source_process_ready"] = sourceView.ProcessReady
			last["source_draining"] = sourceView.Draining
			last["source_snapshot_match"] = sourceView.SnapshotRevision == snapshotRevision
			last["source_capacity_weight"] = sourceView.CapacityWeight
			last["source_serving_shards"] = sourceView.ServingShardCount
			last["source_tls_identity_match"] = sourceView.TLSDNSIdentity == source.holderDNS
		}
		if targetOK {
			last["target_endpoint_match"] = targetView.LogicEndpoint == target.ports.logic
			last["target_boot_match"] = targetView.BootID == target.boot
			last["target_state"] = targetView.State
			last["target_process_ready"] = targetView.ProcessReady
			last["target_draining"] = targetView.Draining
			last["target_snapshot_match"] = targetView.SnapshotRevision == snapshotRevision
			last["target_capacity_weight"] = targetView.CapacityWeight
			last["target_tls_identity_match"] = targetView.TLSDNSIdentity == target.holderDNS
			last["target_serving_shards"] = targetView.ServingShardCount
		}
		sourceReady := sourceOK &&
			sourceView.LogicEndpoint == source.ports.logic && sourceView.BootID == source.boot &&
			sourceView.ProcessReady && !sourceView.Draining && sourceView.CapacityWeight > 0 &&
			sourceView.SnapshotRevision == snapshotRevision &&
			(sourceView.State == "SERVING" || sourceView.State == "READY_ZERO_SHARD")
		targetReady := targetOK &&
			targetView.LogicEndpoint == target.ports.logic && targetView.BootID == target.boot &&
			targetView.ProcessReady && !targetView.Draining && targetView.CapacityWeight > 0 &&
			targetView.SnapshotRevision == snapshotRevision &&
			(targetView.State == "SERVING" || targetView.State == "READY_ZERO_SHARD")
		if sourceReady && targetReady {
			if match, snapErr := r.snapshotSourceEndpointMatch(domain, 0, source); snapErr == nil {
				last["snapshot_source_endpoint_match"] = match
			} else {
				last["snapshot_source_endpoint_error"] = errString(snapErr)
			}
			return r.writer.Observe("plans.ndjson", scenario, "holder_preflight", r.harness, last)
		}
		if time.Now().After(deadline) {
			_ = r.writer.Observe("plans.ndjson", scenario, "holder_preflight_timeout", r.harness, last)
			return fmt.Errorf("%s holders did not become move-ready at the controller snapshot revision", scenario)
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *runRuntime) farmTransferIdentity(shard uint32) (key string, scriptSHA string, err error) {
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment,
		NamespaceZone:        r.zone,
		PhysicalProfile:      "redis-physical-v1",
		AuthorityLayout:      "v1",
		WorkflowLayout:       "v1",
		WorkflowHashVersion:  "1",
		WorkflowBucketCount:  "256",
	})
	if err != nil {
		return "", "", err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return "", "", err
	}
	profile := tuple.Profile()
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		return "", "", err
	}
	ref, err := profile.AuthorityBucketForShard(calculator, actorrouting.DomainFarm, shard)
	if err != nil {
		return "", "", err
	}
	authorityKey, err := keys.ShardAuthority(ref)
	if err != nil {
		return "", "", err
	}
	client, err := r.observerRedisClient()
	if err != nil {
		return "", "", err
	}
	defer client.Close()
	registry, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		return "", "", err
	}
	sha, ok := registry.ScriptSHA(redisauthority.ScriptTransferShardAuthority)
	if !ok || sha == "" || !strings.EqualFold(sha, transferShardAuthorityScriptSHA) {
		return "", "", errors.New("production TransferShardAuthorityV2 script SHA does not match owned S3 identity")
	}
	return authorityKey.String(), sha, nil
}

func (r *runRuntime) observeS3TransferFault(transferSHA string) error {
	forwarded := 0
	dropped := 0
	for _, event := range r.redisProxy.Events() {
		if event.Listener != "controller" || event.Command != "EVALSHA" || !strings.EqualFold(event.ScriptSHA, transferSHA) {
			continue
		}
		if event.Forwarded {
			forwarded++
		}
		if event.ReplyDropped {
			dropped++
		}
	}
	if forwarded != 1 || dropped != 1 {
		return fmt.Errorf("S3 transfer fault counts forwarded=%d dropped=%d", forwarded, dropped)
	}
	if err := r.writer.Observe("redis-faults.ndjson", "S3", "transfer_evalsha_forwarded", r.harness, map[string]any{
		"listener": "controller", "script_sha": strings.ToLower(transferSHA), "count": 1,
	}); err != nil {
		return err
	}
	return r.writer.Observe("redis-faults.ndjson", "S3", "transfer_reply_dropped", r.harness, map[string]any{
		"listener": "controller", "script_sha": strings.ToLower(transferSHA), "count": 1, "block_after_drop": true,
	})
}

func (r *runRuntime) proveS3FourTruth(planID, transitionID, domain string, shard uint32, source, target *holderSpec) error {
	client, err := r.observerRedisClient()
	if err != nil {
		return err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment,
		NamespaceZone:        r.zone,
		PhysicalProfile:      "redis-physical-v1",
		AuthorityLayout:      "v1",
		WorkflowLayout:       "v1",
		WorkflowHashVersion:  "1",
		WorkflowBucketCount:  "256",
	})
	if err != nil {
		return err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001,
	}, 2, 2)
	if err != nil {
		return err
	}
	profile := tuple.Profile()
	namespace := tuple.Namespace()
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(profile, calculator)
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	authorityValue, found, err := reader.ReadShardWriteAuthority(ctx, actorrouting.DomainFarm, shard)
	if err != nil || !found {
		return fmt.Errorf("S3 authority reader failed: found=%v err=%v", found, err)
	}
	authorityTarget := authorityValue.State == authoritystore.ShardAuthorityActive &&
		authorityValue.HolderID == target.id && authorityValue.HolderBootID == target.boot &&
		authorityValue.LastTransitionID == transitionID
	snapshotOptions := redisroutecontrol.DefaultOptions(namespace)
	snapshotStore, err := redisroutecontrol.NewSnapshotStore(client, snapshotOptions)
	if err != nil {
		return err
	}
	stored, snapFound, err := snapshotStore.LoadCurrent(ctx)
	if err != nil || !snapFound {
		return fmt.Errorf("S3 snapshot reader failed: found=%v err=%v", snapFound, err)
	}
	snapshotSource, err := farmRouteMatchesHolder(stored.Snapshot, shard, source)
	if err != nil {
		return err
	}
	planOptions := redisrouteplan.DefaultOptions(namespace)
	planStore, err := redisrouteplan.NewPlanStore(client, planOptions)
	if err != nil {
		return err
	}
	if err := planStore.EnsureLoaded(ctx); err != nil {
		return err
	}
	planRecord, planFound, err := planStore.Load(ctx, planID)
	if err != nil || !planFound {
		return fmt.Errorf("S3 plan reader failed: found=%v err=%v", planFound, err)
	}
	planAssigning := planRecord.Plan.Phase.String() == "ASSIGNING"
	transitionOptions := redisroutecontrol.DefaultOptions(namespace)
	transitionStore, err := redisroutecontrol.NewTransitionStore(client, transitionOptions)
	if err != nil {
		return err
	}
	if err := transitionStore.EnsureLoaded(); err != nil {
		return err
	}
	recovery, recoveryFound, err := transitionStore.LookupTransitionRecovery(ctx, transitionID)
	if err != nil || !recoveryFound {
		return fmt.Errorf("S3 transition recovery reader failed: found=%v err=%v", recoveryFound, err)
	}
	authorityAttempted := recovery.AuthorityAttempted
	payload := map[string]any{
		"authority_target":    authorityTarget,
		"snapshot_source":     snapshotSource,
		"plan_assigning":      planAssigning,
		"authority_attempted": authorityAttempted,
		"plan_id":             planID,
		"transition_id":       transitionID,
		"assignment_revision": authorityValue.AssignmentRevision,
		"writer_epoch":        authorityValue.WriterEpoch,
		"lease_epoch":         authorityValue.LeaseEpoch,
		"digest_bound":        authorityValue.LastTransitionID == transitionID,
	}
	if err := r.writer.Observe("plans.ndjson", "S3", "pending_projection", r.harness, payload); err != nil {
		return err
	}
	if !authorityTarget || !snapshotSource || !planAssigning || !authorityAttempted {
		return fmt.Errorf("S3 four-truth barrier unmet: %+v", payload)
	}
	return nil
}

func (r *runRuntime) s3DeadlineFacts(transitionID string, shard uint32) (map[string]any, error) {
	client, err := r.observerRedisClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: r.environment, NamespaceZone: r.zone, PhysicalProfile: "redis-physical-v1",
		AuthorityLayout: "v1", WorkflowLayout: "v1", WorkflowHashVersion: "1", WorkflowBucketCount: "256",
	})
	if err != nil {
		return nil, err
	}
	calculator, err := actorrouting.NewCalculator(r.realm, actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608100001}, 2, 2)
	if err != nil {
		return nil, err
	}
	reader, err := redisauthority.NewShardAuthorityReader(client, redisauthority.DefaultAuthorityReaderOptions(tuple.Profile(), calculator))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	value, found, err := reader.ReadShardWriteAuthority(ctx, actorrouting.DomainFarm, shard)
	if err != nil || !found {
		return nil, errors.New("S3 final Authority is unavailable")
	}
	snapshotStore, err := redisroutecontrol.NewSnapshotStore(client, redisroutecontrol.DefaultOptions(tuple.Namespace()))
	if err != nil {
		return nil, err
	}
	stored, found, err := snapshotStore.LoadCurrent(ctx)
	if err != nil || !found || int(shard) >= len(stored.Snapshot.Farm.Routes) {
		return nil, errors.New("S3 final Snapshot is unavailable")
	}
	route := stored.Snapshot.Farm.Routes[shard]
	transitionStore, err := redisroutecontrol.NewTransitionStore(client, redisroutecontrol.DefaultOptions(tuple.Namespace()))
	if err != nil {
		return nil, err
	}
	if err := transitionStore.EnsureLoaded(); err != nil {
		return nil, err
	}
	recovery, found, err := transitionStore.LookupTransitionRecovery(ctx, transitionID)
	if err != nil || !found {
		return nil, errors.New("S3 final recovery is unavailable")
	}
	durableAuthority := recovery.Result.Authority
	comparableCurrent := value
	comparableCurrent.LeaseUntilMs = durableAuthority.LeaseUntilMs
	return map[string]any{
		"authority_lease_until_ms":          value.LeaseUntilMs,
		"snapshot_lease_until_ms":           route.LeaseUntilMs,
		"recovery_lease_until_ms":           durableAuthority.LeaseUntilMs,
		"authority_snapshot_deadline_equal": value.LeaseUntilMs == route.LeaseUntilMs,
		"recovery_snapshot_deadline_equal":  durableAuthority.LeaseUntilMs == route.LeaseUntilMs,
		"authority_deadline_delta_ms":       value.LeaseUntilMs - route.LeaseUntilMs,
		"renewal_only_authority_delta":      comparableCurrent == durableAuthority,
		"last_transition_id_equal":          value.LastTransitionID == durableAuthority.LastTransitionID,
		"last_transition_at_equal":          value.LastTransitionAtMs == durableAuthority.LastTransitionAtMs,
		"recovery_committed":                recovery.Committed,
		"recovery_published":                recovery.Published,
		"recovery_cold_loaded":              recovery.ColdLoaded,
		"recovery_authority_attempted":      recovery.AuthorityAttempted,
	}, nil
}

func farmRouteMatchesHolder(snapshot actorrouting.RouteSnapshot, shard uint32, holder *holderSpec) (bool, error) {
	if holder == nil {
		return false, errors.New("holder is required")
	}
	if shard >= snapshot.Farm.ShardCount || int(shard) >= len(snapshot.Farm.Routes) {
		return false, errors.New("farm route is unavailable")
	}
	route := snapshot.Farm.Routes[shard]
	if route.State != actorrouting.ShardStateActive {
		return false, nil
	}
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return false, errors.New("farm holder index is unavailable")
	}
	ref := snapshot.Holders[route.HolderIndex]
	return ref.InstanceID == holder.id && ref.BootID == holder.boot && ref.Endpoint == holder.ports.logic, nil
}

func (r *runRuntime) observerRedisClient() (*redis.Client, error) {
	raw := r.redisURLs["observer"]
	if raw == "" {
		return nil, errors.New("observer Redis URL is unavailable")
	}
	options, err := redis.ParseURL(raw)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(options), nil
}

func (r *runRuntime) observeS3DrainCallbacksAtBarrier(proxy *routecontrol.HolderForwardProxy, domain string, shard uint32) error {
	seen := map[string]bool{}
	if proxy != nil {
		for _, event := range proxy.Events() {
			if !event.UpstreamOK || event.Domain != domain || event.Shard != shard {
				continue
			}
			kind := ""
			switch event.Method {
			case "Stop":
				kind = "holder_callback_stop"
			case "Wait":
				kind = "holder_callback_wait"
			case "Facts":
				kind = "holder_callback_facts"
			default:
				continue
			}
			if seen[kind] {
				continue
			}
			seen[kind] = true
			if err := r.writer.Observe("holders.ndjson", "S3", kind, r.harness, map[string]any{
				"domain": domain, "shard": shard, "method": event.Method, "upstream_ok": true, "at_barrier": true,
			}); err != nil {
				return err
			}
		}
	}
	for _, required := range []string{"holder_callback_stop", "holder_callback_wait", "holder_callback_facts"} {
		if !seen[required] {
			return fmt.Errorf("missing successful %s at S3 barrier", required)
		}
	}
	return nil
}

func (r *runRuntime) observeS3PrepareOnce(domain string, shard uint32, proxies ...*routecontrol.HolderForwardProxy) error {
	count := 0
	for _, proxy := range proxies {
		if proxy == nil {
			continue
		}
		for _, event := range proxy.Events() {
			if event.UpstreamOK && event.Method == "Prepare" && event.Domain == domain && event.Shard == shard {
				count++
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("S3 Prepare count=%d want=1", count)
	}
	return r.writer.Observe("holders.ndjson", "S3", "holder_callback_prepare", r.harness, map[string]any{
		"domain": domain, "shard": shard, "method": "Prepare", "upstream_ok": true, "count": 1,
	})
}

func (r *runRuntime) waitS3PlanSucceeded(planID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		verifyOut, _ := r.runRoutectl(5*time.Second, "verify", "-plan-id", planID, "-domain", "FARM", "-shard", "0")
		// routectl intentionally exits nonzero when any independent Verify check
		// is not yet consistent, while still returning the authenticated Plan in
		// JSON. Parse that production response to observe the Plan terminal.
		if len(verifyOut) > 0 {
			var verify struct {
				Consistent bool `json:"consistent"`
				Plan       struct {
					Phase     string `json:"phase"`
					ErrorCode string `json:"error_code"`
					Revision  uint64 `json:"revision"`
				} `json:"plan"`
			}
			if json.Unmarshal(verifyOut, &verify) == nil {
				if verify.Plan.Phase == "PAUSED" || verify.Plan.Phase == "FAILED" {
					_ = r.writer.Observe("plans.ndjson", "S3", "plan_paused_or_failed", r.harness, map[string]any{
						"plan_id": planID, "phase": verify.Plan.Phase, "error_code": verify.Plan.ErrorCode,
					})
					return fmt.Errorf("S3 recovery terminalized plan as %s (%s)", verify.Plan.Phase, verify.Plan.ErrorCode)
				}
				// This wait proves only the durable Plan terminal. Verification has
				// independent freshness and fleet-convergence checks and is sampled
				// immediately afterward; coupling it here can wait through the entire
				// short RT2Q Authority TTL after SUCCEEDED already committed.
				if verify.Plan.Phase == "SUCCEEDED" {
					return r.writer.Observe("plans.ndjson", "S3", "plan_phase", r.harness, map[string]any{
						"plan_id": planID, "phase": verify.Plan.Phase, "revision": verify.Plan.Revision,
					})
				}
			}
		}
		if time.Now().After(deadline) {
			return errors.New("S3 recovery did not reach Plan SUCCEEDED")
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *runRuntime) observeS3RecoveryFailure(planID, domain string, shard uint32, source, target *holderSpec) error {
	payload := map[string]any{"plan_id": planID, "domain": domain, "shard": shard}
	verifyOut, verifyErr := r.runRoutectl(5*time.Second, "verify", "-plan-id", planID, "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10))
	payload["verify_error"] = errString(verifyErr)
	if len(verifyOut) > 0 {
		var verify struct {
			Consistent bool `json:"consistent"`
			Plan       struct {
				Phase     string `json:"phase"`
				ErrorCode string `json:"error_code"`
				Revision  uint64 `json:"revision"`
			} `json:"plan"`
		}
		if json.Unmarshal(verifyOut, &verify) == nil {
			payload["verify_consistent"] = verify.Consistent
			payload["verify_phase"] = verify.Plan.Phase
			payload["verify_error_code"] = verify.Plan.ErrorCode
			payload["verify_revision"] = verify.Plan.Revision
			if verify.Plan.Phase == "PAUSED" || verify.Plan.Phase == "FAILED" {
				_ = r.writer.Observe("plans.ndjson", "S3", "plan_paused_or_failed", r.harness, map[string]any{
					"plan_id": planID, "phase": verify.Plan.Phase, "error_code": verify.Plan.ErrorCode,
				})
			}
		}
	}
	if match, err := r.snapshotSourceEndpointMatch(domain, shard, source); err == nil {
		payload["snapshot_still_source"] = match
	}
	if match, err := r.snapshotSourceEndpointMatch(domain, shard, target); err == nil {
		payload["snapshot_now_target"] = match
	}
	prepare := 0
	for _, holder := range []*holderSpec{source, target} {
		if holder == nil || holder.holderProxy == nil {
			continue
		}
		for _, event := range holder.holderProxy.Events() {
			if event.UpstreamOK && event.Method == "Prepare" && event.Domain == domain && event.Shard == shard {
				prepare++
			}
		}
	}
	payload["prepare_count"] = prepare
	return r.writer.Observe("plans.ndjson", "S3", "s3_recovery_failure", r.harness, payload)
}

func (r *runRuntime) observeS3FinalFacts(planID, domain string, shard uint32, source, target *holderSpec, transitionID string) error {
	type s3Verify struct {
		Consistent bool `json:"consistent"`
		Plan       struct {
			Phase string `json:"phase"`
		} `json:"plan"`
		Checks []struct {
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"checks"`
	}
	var verify s3Verify
	// ACTIVE publication precedes the target holder's next bounded Registry
	// heartbeat. Poll the real fail-closed VerificationPort until it observes the
	// exact ACTIVE SnapshotRevision; process health alone is not that evidence.
	deadline := time.Now().Add(10 * time.Second)
	for {
		verifyOut, _ := r.runRoutectl(5*time.Second, "verify", "-plan-id", planID, "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10))
		verify = s3Verify{}
		if len(verifyOut) > 0 && json.Unmarshal(verifyOut, &verify) == nil && verify.Consistent && verify.Plan.Phase == "SUCCEEDED" {
			break
		}
		if time.Now().After(deadline) {
			checks := make(map[string]bool, len(verify.Checks))
			for _, check := range verify.Checks {
				checks[check.Name] = check.OK
			}
			payload := map[string]any{
				"plan_id": planID, "phase": verify.Plan.Phase, "consistent": verify.Consistent, "checks": checks,
			}
			if deadlineFacts, err := r.s3DeadlineFacts(transitionID, shard); err == nil {
				for name, value := range deadlineFacts {
					payload[name] = value
				}
			}
			_ = r.writer.Observe("plans.ndjson", "S3", "s3_verify_not_converged", r.harness, payload)
			return errors.New("S3 final routectl verify did not converge to SUCCEEDED/consistent")
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	match, err := r.snapshotSourceEndpointMatch(domain, shard, target)
	if err != nil || !match {
		return fmt.Errorf("S3 final snapshot does not bind target: match=%v err=%v", match, err)
	}
	boot := r.controllerBoots[min(r.controllerIndex, len(r.controllerBoots)-1)]
	converged := 0
	for name, proxy := range r.snapshotProxies {
		healthy := false
		for _, event := range proxy.Events() {
			if event.UpstreamOK && event.Boot == boot {
				healthy = true
			}
		}
		if !healthy {
			return fmt.Errorf("S3 consumer %s did not converge on recovery boot", name)
		}
		converged++
		if err := r.writer.Observe("snapshots.ndjson", "S3", "consumer_converge", r.harness, map[string]any{
			"consumer": name, "healthy": true, "boot": boot,
		}); err != nil {
			return err
		}
	}
	if converged != 7 {
		return fmt.Errorf("S3 expected seven consumers to converge, got %d", converged)
	}
	return r.writer.Observe("plans.ndjson", "S3", "routectl_final_facts", r.harness, map[string]any{
		"plan_id": planID, "transition_id": transitionID, "consistent": true, "phase": "SUCCEEDED",
		"source": source.id, "target": target.id, "checks": len(verify.Checks),
	})
}

func findRoutectlHolder(holders []routectlHolderView, id string) (routectlHolderView, bool) {
	for _, holder := range holders {
		if holder.HolderID == id {
			return holder, true
		}
	}
	return routectlHolderView{}, false
}

func (r *runRuntime) snapshotSourceEndpointMatch(domain string, shard uint32, source *holderSpec) (bool, error) {
	output, err := r.runRoutectl(5*time.Second, "snapshot")
	if err != nil {
		return false, err
	}
	var response struct {
		CanonicalJSON json.RawMessage `json:"canonical_json"`
	}
	if json.Unmarshal(output, &response) != nil || len(response.CanonicalJSON) == 0 {
		return false, errors.New("routectl snapshot missing canonical JSON")
	}
	var snapshot struct {
		Holders []struct {
			InstanceID string `json:"instance_id"`
			BootID     string `json:"boot_id"`
			Endpoint   string `json:"endpoint"`
		} `json:"holders"`
		Player struct {
			Routes []struct {
				State       string `json:"state"`
				HolderIndex int    `json:"holder_index"`
			} `json:"routes"`
		} `json:"player"`
		Farm struct {
			Routes []struct {
				State       string `json:"state"`
				HolderIndex int    `json:"holder_index"`
			} `json:"routes"`
		} `json:"farm"`
	}
	if json.Unmarshal(response.CanonicalJSON, &snapshot) != nil {
		return false, errors.New("snapshot canonical JSON decode failed")
	}
	var routes []struct {
		State       string `json:"state"`
		HolderIndex int    `json:"holder_index"`
	}
	switch domain {
	case "PLAYER":
		routes = snapshot.Player.Routes
	case "FARM":
		routes = snapshot.Farm.Routes
	default:
		return false, errors.New("snapshot domain is unsupported")
	}
	if int(shard) >= len(routes) {
		return false, errors.New("snapshot route is unavailable")
	}
	route := routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return false, errors.New("snapshot holder index is unavailable")
	}
	holder := snapshot.Holders[route.HolderIndex]
	return holder.InstanceID == source.id && holder.BootID == source.boot && holder.Endpoint == source.ports.logic && route.State == "ACTIVE", nil
}

func (r *runRuntime) probeMoveShardStatusCode(planID, transitionID, domain string, shard uint32, source, target *holderSpec, fence shardFence) (string, error) {
	tlsConfig, err := transport.LoadClientTLSConfig(r.operator.CertFile, r.operator.KeyFile, r.trust.CAs["route"])
	if err != nil {
		return "", err
	}
	bound, err := transport.BindExactServerIdentity(tlsConfig, r.routeServerDNS, r.routeServerDNS)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, r.controllerAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(bound)),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)),
	)
	if err != nil {
		return "dial_failed", nil
	}
	defer conn.Close()
	client := routeplanpb.NewRoutePlanServiceClient(conn)
	_, err = client.MoveShard(ctx, &routeplanpb.MoveShardRequest{
		PlanId: planID, TransitionId: transitionID, Domain: domain, Shard: shard,
		SourceHolderId: source.id, SourceBootId: source.boot, SourceEndpoint: source.ports.logic,
		TargetHolderId: target.id, TargetBootId: target.boot, TargetEndpoint: target.ports.logic,
		RequestedTtlMs: 60000,
		ExpectedSourceFence: &routeplanpb.AuthorityFence{
			HolderId: fence.HolderID, HolderBootId: fence.HolderBootID,
			AssignmentRevision: fence.AssignmentRevision, WriterEpoch: fence.WriterEpoch, LeaseEpoch: fence.LeaseEpoch,
		},
	})
	if err == nil {
		return "OK", nil
	}
	if st, ok := status.FromError(err); ok {
		return st.Code().String(), nil
	}
	return "unknown", nil
}

func (r *runRuntime) s2MoveFailurePayload(source *holderSpec, moveErr error, output []byte, holdErr error) map[string]any {
	payload := map[string]any{
		"move_error":      errString(moveErr),
		"move_output_len": len(output),
		"proxy_events":    0,
		"proxy_tls_ok":    0,
		"proxy_tls_fail":  0,
	}
	if holdErr != nil {
		payload["hold_error"] = errString(holdErr)
	}
	if source != nil && source.holderProxy != nil {
		payload["proxy_events"] = len(source.holderProxy.Events())
		payload["proxy_tls_ok"] = len(source.holderProxy.TLSFacts())
		payload["proxy_tls_fail"] = len(source.holderProxy.TLSFailures())
	}
	var parsed struct {
		Plan struct {
			Phase     string `json:"phase"`
			ErrorCode string `json:"error_code"`
			Revision  uint64 `json:"revision"`
		} `json:"plan"`
	}
	if len(output) > 0 && json.Unmarshal(output, &parsed) == nil {
		payload["plan_phase"] = parsed.Plan.Phase
		payload["plan_error_code"] = parsed.Plan.ErrorCode
		payload["plan_revision"] = parsed.Plan.Revision
	}
	return payload
}

type shardFence struct {
	HolderID           string
	HolderBootID       string
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
}

func (r *runRuntime) holderByName(name string) *holderSpec {
	for _, holder := range r.holders {
		if holder.name == name {
			return holder
		}
	}
	return nil
}

func (r *runRuntime) routectlShardFence(domain string, shard uint32) (shardFence, error) {
	output, err := r.runRoutectl(5*time.Second, "shard", "-domain", domain, "-shard", strconv.FormatUint(uint64(shard), 10))
	if err != nil {
		return shardFence{}, err
	}
	var response struct {
		Authority *struct {
			Fence struct {
				HolderID           string `json:"holder_id"`
				HolderBootID       string `json:"holder_boot_id"`
				AssignmentRevision uint64 `json:"assignment_revision"`
				WriterEpoch        uint64 `json:"writer_epoch"`
				LeaseEpoch         uint64 `json:"lease_epoch"`
			} `json:"fence"`
		} `json:"authority"`
	}
	if json.Unmarshal(output, &response) != nil || response.Authority == nil {
		return shardFence{}, errors.New("routectl shard did not return authority fence")
	}
	fence := shardFence{
		HolderID: response.Authority.Fence.HolderID, HolderBootID: response.Authority.Fence.HolderBootID,
		AssignmentRevision: response.Authority.Fence.AssignmentRevision, WriterEpoch: response.Authority.Fence.WriterEpoch,
		LeaseEpoch: response.Authority.Fence.LeaseEpoch,
	}
	if fence.HolderID == "" || fence.HolderBootID == "" || fence.AssignmentRevision == 0 || fence.WriterEpoch == 0 || fence.LeaseEpoch == 0 {
		return shardFence{}, errors.New("routectl shard fence is incomplete")
	}
	return fence, nil
}

func (r *runRuntime) pollPlanPhases(ctx context.Context, planID string, lastRevision *atomic.Uint64) error {
	sessionObserved := 0
	allowed := map[string]struct{}{
		"PENDING": {}, "DRAINING": {}, "ASSIGNING": {}, "PREPARING": {}, "ACTIVE": {}, "SUCCEEDED": {},
	}
	for {
		if ctx.Err() != nil {
			if sessionObserved == 0 && lastRevision.Load() == 0 {
				return errors.New("S2 observed no incomplete Plan phases")
			}
			return nil
		}
		output, err := r.runRoutectl(routectlStatusPollRPCTimeout, "status")
		if err == nil {
			var status struct {
				IncompletePlans []struct {
					PlanID   string `json:"plan_id"`
					Phase    string `json:"phase"`
					Revision uint64 `json:"revision"`
				} `json:"incomplete_plans"`
			}
			if json.Unmarshal(output, &status) == nil {
				for _, plan := range status.IncompletePlans {
					if plan.PlanID != planID || plan.Phase == "" {
						continue
					}
					if _, ok := allowed[plan.Phase]; !ok {
						continue
					}
					previous := lastRevision.Load()
					if plan.Revision <= previous {
						continue
					}
					if !lastRevision.CompareAndSwap(previous, plan.Revision) {
						continue
					}
					sessionObserved++
					if err := r.writer.Observe("plans.ndjson", "S2", "plan_phase", r.harness, map[string]any{
						"plan_id": planID, "phase": plan.Phase, "revision": plan.Revision,
					}); err != nil {
						return err
					}
				}
			}
		}
		interval := 25 * time.Millisecond
		if sessionObserved == 0 && lastRevision.Load() == 0 {
			interval = 10 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			if sessionObserved == 0 && lastRevision.Load() == 0 {
				return errors.New("S2 observed no incomplete Plan phases")
			}
			return nil
		case <-time.After(interval):
		}
	}
}

func (r *runRuntime) observeHolderCallbacks(scenario string, proxies []*routecontrol.HolderForwardProxy, domain string, shard uint32) error {
	seen := map[string]bool{}
	for _, proxy := range proxies {
		if proxy == nil {
			continue
		}
		for _, event := range proxy.Events() {
			if !event.UpstreamOK || event.Domain != domain || event.Shard != shard {
				continue
			}
			kind := ""
			switch event.Method {
			case "Stop":
				kind = "holder_callback_stop"
			case "Wait":
				kind = "holder_callback_wait"
			case "Facts":
				kind = "holder_callback_facts"
			case "Prepare":
				kind = "holder_callback_prepare"
			default:
				continue
			}
			if seen[kind] {
				continue
			}
			seen[kind] = true
			if err := r.writer.Observe("holders.ndjson", scenario, kind, r.harness, map[string]any{
				"domain": domain, "shard": shard, "method": event.Method, "upstream_ok": true,
			}); err != nil {
				return err
			}
		}
	}
	for _, required := range []string{"holder_callback_stop", "holder_callback_wait", "holder_callback_facts", "holder_callback_prepare"} {
		if !seen[required] {
			return fmt.Errorf("missing successful %s observation", required)
		}
	}
	return nil
}

func (r *runRuntime) stopManaged(process *managedProcess, timeout time.Duration) error {
	if process == nil || process.stopped {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result, err := process.process.Shutdown(ctx)
	process.stopped = true
	_ = process.logFile.Sync()
	if observeErr := r.writer.Observe("processes.ndjson", r.scenarios[len(r.scenarios)-1], "process_stopped", process.identity,
		map[string]any{"name": process.name, "exit_code": result.ExitCode, "stopped": true}); observeErr != nil {
		err = errors.Join(err, observeErr)
	}
	return err
}

func (r *runRuntime) shutdownAndCleanup() error {
	if r.writer == nil {
		return nil
	}
	scenario := r.scenarios[len(r.scenarios)-1]
	var errs []error
	r.procMu.Lock()
	processes := append([]*managedProcess(nil), r.processes...)
	r.procMu.Unlock()
	for index := len(processes) - 1; index >= 0; index-- {
		errs = append(errs, r.stopManaged(processes[index], 10*time.Second))
	}
	if r.redisProxy != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanup, err := r.redisProxy.Cleanup(ctx, "observer")
		cancel()
		errs = append(errs, err)
		for _, item := range cleanup {
			observeErr := r.writer.Observe("cleanup.ndjson", scenario, "cleanup_key", r.harness,
				map[string]any{"key_digest": item.KeyDigest, "commands": item.Commands, "delete_reply": item.DeleteReply, "exists": item.ExistsReply, "deleted": true})
			errs = append(errs, observeErr)
		}
		for _, event := range r.redisProxy.Events() {
			errs = append(errs, r.writer.Observe("redis-faults.ndjson", scenario, "redis_audit", r.harness, event))
		}
		_ = r.redisProxy.Close()
	}
	for name, proxy := range r.snapshotProxies {
		for _, event := range proxy.Events() {
			errs = append(errs, r.writer.Observe("rpc-faults.ndjson", scenario, "snapshot_proxy_event", r.harness,
				map[string]any{"consumer": name, "event": event}))
		}
		for _, fact := range proxy.TLSFacts() {
			errs = append(errs, r.writer.Observe("tls-peers.ndjson", scenario, "tls_peer", r.harness, fact))
		}
		for _, failure := range proxy.TLSFailures() {
			errs = append(errs, r.writer.Observe("rpc-faults.ndjson", scenario, "tls_proxy_failure", r.harness,
				map[string]any{"proxy": name, "failure": failure}))
		}
		_ = proxy.Close()
	}
	for _, gate := range r.gates {
		if gate.verifierProxy == nil {
			continue
		}
		for _, event := range gate.verifierProxy.Events() {
			errs = append(errs, r.writer.Observe("gate-observations.ndjson", scenario, "gate_verifier_proxy_event", r.harness,
				map[string]any{"gate": gate.name, "event": event}))
		}
		for _, fact := range gate.verifierProxy.TLSFacts() {
			errs = append(errs, r.writer.Observe("tls-peers.ndjson", scenario, "tls_peer", r.harness, fact))
		}
		for _, failure := range gate.verifierProxy.TLSFailures() {
			errs = append(errs, r.writer.Observe("rpc-faults.ndjson", scenario, "tls_proxy_failure", r.harness,
				map[string]any{"proxy": gate.name + "-verifier", "failure": failure}))
		}
		_ = gate.verifierProxy.Close()
	}
	for _, holder := range r.holders {
		if holder.holderProxy != nil {
			for _, event := range holder.holderProxy.Events() {
				errs = append(errs, r.writer.Observe("holders.ndjson", scenario, "holder_proxy_event", r.harness,
					map[string]any{"holder": holder.name, "event": event}))
			}
			for _, fact := range holder.holderProxy.TLSFacts() {
				errs = append(errs, r.writer.Observe("tls-peers.ndjson", scenario, "tls_peer", r.harness, fact))
			}
			for _, failure := range holder.holderProxy.TLSFailures() {
				errs = append(errs, r.writer.Observe("rpc-faults.ndjson", scenario, "tls_proxy_failure", r.harness,
					map[string]any{"proxy": holder.name, "failure": failure}))
			}
			_ = holder.holderProxy.Close()
			if holder.logicProxy != nil {
				_ = holder.logicProxy.Close()
			}
		}
	}
	if r.redisContainer != "" {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		errs = append(errs, runCommand(stopCtx, r.worktree, "docker", "exec", r.redisContainer, "chown", "-R",
			strconv.Itoa(os.Getuid())+":"+strconv.Itoa(os.Getgid()), "/data"))
		errs = append(errs, runCommand(stopCtx, r.worktree, "docker", "stop", "-t", "5", r.redisContainer))
		cancel()
	}
	if r.redisNetwork != "" {
		netCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		errs = append(errs, runCommand(netCtx, r.worktree, "docker", "network", "rm", r.redisNetwork))
		cancel()
	}
	residue := false
	for _, process := range processes {
		if !process.stopped {
			residue = true
		}
		_ = process.logFile.Close()
	}
	if r.tempRoot != "" {
		errs = append(errs, os.RemoveAll(r.tempRoot))
	}
	errs = append(errs, r.writer.Observe("cleanup.ndjson", scenario, "cleanup_processes", r.harness, map[string]any{"residue": residue, "count": len(processes)}))
	return errors.Join(errs...)
}

func harnessConfigValues(realm, environment, zone, runtime, redisMode string) []string {
	return []string{realm, environment, zone, "redis-physical-v1", "v1", "v1", "1", "256",
		harnessAuthorityTTL, harnessPlanTTL, harnessOperationTimeout, runtime, redisMode}
}

func harnessAuthorityLeaseEnv() []string {
	return []string{
		"ACTOR_AUTHORITY_LEASE_TTL=" + harnessAuthorityTTL,
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=" + harnessLeaseSafetyMargin,
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=" + harnessLeaseRenewInterval,
	}
}

func (r *runRuntime) bindings(oracleSHA string) routecontrol.TrustedBindings {
	configValues := harnessConfigValues(r.realm, r.environment, r.zone, r.cfg.runtime, r.cfg.redisMode)
	processValues := make([]string, 0, len(r.processes))
	for _, process := range r.processes {
		processValues = append(processValues, process.identity.Name+"|"+process.identity.Role+"|"+strconv.Itoa(process.identity.PID)+"|"+process.identity.BinarySHA256+"|"+process.identity.StartIdentity)
	}
	tlsValues := make([]string, 0)
	if r.trust != nil {
		for _, fact := range r.trust.Facts() {
			tlsValues = append(tlsValues, fact.TrustDomain+"|"+fact.Usage+"|"+fact.DNSName+"|"+fact.CertificateDER+"|"+fact.PublicKeySHA256)
		}
	}
	gateValues := make([]string, 0, len(r.gates))
	for _, gate := range r.gates {
		address := gate.ports.verify
		if gate.verifierProxy != nil {
			address = gate.verifierProxy.Address()
		}
		gateValues = append(gateValues, gate.name+"|"+gate.dns+"|"+address)
	}
	ledgerDigest := routecontrol.SHA256CanonicalSet("empty-ledger", nil)
	if r.redisProxy != nil {
		_, ledgerDigest = r.redisProxy.Ledger()
	}
	return routecontrol.TrustedBindings{
		ConfigSetSHA256:      routecontrol.SHA256CanonicalSet("rt2q-config/v1", configValues),
		ProcessSetSHA256:     routecontrol.SHA256CanonicalSet("rt2q-processes/v1", processValues),
		TLSPeerSetSHA256:     routecontrol.SHA256CanonicalSet("rt2q-tls/v1", tlsValues),
		GatePeerSetSHA256:    routecontrol.SHA256CanonicalSet("rt2q-gate-peers/v1", gateValues),
		RedisKeyLedgerSHA256: ledgerDigest, OracleSHA256: oracleSHA,
	}
}

var processPortReservation = struct {
	sync.Mutex
	next uint16
}{next: 21000}

// reserveAddress chooses checked production listener ports below the verified
// Linux ephemeral range used by proxy :0 listeners.
func reserveAddress() (string, error) {
	processPortReservation.Lock()
	defer processPortReservation.Unlock()
	for attempts := 0; attempts < 10_000; attempts++ {
		port := processPortReservation.next
		processPortReservation.next++
		if processPortReservation.next >= 31_000 {
			processPortReservation.next = 21000
		}
		address := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))
		listener, err := net.Listen("tcp", address)
		if err != nil {
			continue
		}
		if err := listener.Close(); err != nil {
			return "", err
		}
		return address, nil
	}
	return "", errors.New("no disjoint loopback process port is available")
}

func verifyDisjointProcessPortRange() error {
	raw, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return errors.New("cannot verify Linux ephemeral port range")
	}
	var low, high int
	if _, err := fmt.Sscanf(string(raw), "%d %d", &low, &high); err != nil || low <= 31_000 || high <= low {
		return errors.New("Linux ephemeral port range overlaps trusted process ports")
	}
	return nil
}

func runCommand(ctx context.Context, directory, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

func digestValue(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
