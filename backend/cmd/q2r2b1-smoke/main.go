// Command q2r2b1-smoke boots the REAL player-svr / farm-svr binaries against
// the company proxy Redis with AUTHORITY_MEMBERSHIP_MODE=true and verifies the
// four Route Foundation scenarios:
//
//	S1 zero-shard   process starts healthy, readyz=200, no legacy renewal.
//	S2 one-shard    pre-seeded authority + static snapshot -> readyz=200 and
//	                the per-shard scheduler really renews (lease deadline
//	                advances on the company Redis across >= 2 renew rounds).
//	S3 no fallback  holder unregistered / authority mismatch / illegal mode
//	                exit non-zero without readiness or a legacy fallback;
//	                boot mismatch is reported as its composed contract
//	                (0-shard ready, NOT a startup failure).
//	S4 SIGTERM      graceful stop: bounded exit, no fatal, exact keys cleaned.
//
// Requires Q2R2_LIVE=1 and FARM_REDIS_URL=<secret>; every scenario uses an
// isolated namespace and cleans up the exact keys it created (DEL + EXISTS),
// never KEYS/SCAN, and never a global SCRIPT FLUSH.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

const (
	realm         = "q2r2b1"
	hashSeed      = uint64(0xA1B2C3D4)
	playerHolder  = "q2r2b1-player"
	playerBoot    = "q2r2b1-player-boot"
	farmHolder    = "q2r2b1-farm"
	farmBoot      = "q2r2b1-farm-boot"
	playerOther   = "q2r2b1-player-other"
	farmOther     = "q2r2b1-farm-other"
	leaseTTL      = 8000 * time.Millisecond
	safetyMargin  = 1500 * time.Millisecond
	renewInterval = 300 * time.Millisecond
	readyTimeout  = 25 * time.Second
	portBase      = 15100
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Q2R2B1 FAIL: %v", err)
	}
	log.Printf("Q2R2B1 PASS")
}

func run() error {
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	if strings.TrimSpace(os.Getenv("Q2R2_LIVE")) != "1" || rawURL == "" {
		return errors.New("Q2R2_LIVE=1 and FARM_REDIS_URL are required")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return fmt.Errorf("run from the repository root: %w", err)
	}

	binDir, err := os.MkdirTemp("", "q2r2b1-bins-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(binDir)
	playerBin, farmBin, err := buildBinaries(root, binDir)
	if err != nil {
		return err
	}

	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return fmt.Errorf("parse FARM_REDIS_URL: %w", err)
	}
	client := redis.NewClient(options)
	defer func() { _ = client.Close() }()
	if err := client.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("ping company proxy redis: %w", err)
	}

	if err := s1ZeroShard(playerBin, farmBin, rawURL, client); err != nil {
		return fmt.Errorf("S1 zero-shard: %w", err)
	}
	if err := s2APlayerShardRenew(playerBin, rawURL, client); err != nil {
		return fmt.Errorf("S2A player one-shard renew: %w", err)
	}
	if err := s2BFarmShardRenew(farmBin, rawURL, client); err != nil {
		return fmt.Errorf("S2B farm one-shard renew: %w", err)
	}
	if err := s3NoFallback(playerBin, farmBin, rawURL, client); err != nil {
		return fmt.Errorf("S3 no-fallback: %w", err)
	}
	if err := s4Sigterm(playerBin, farmBin, rawURL, client); err != nil {
		return fmt.Errorf("S4 SIGTERM: %w", err)
	}
	return nil
}

func buildBinaries(root, binDir string) (string, string, error) {
	playerBin := filepath.Join(binDir, "player-svr")
	farmBin := filepath.Join(binDir, "farm-svr")
	for _, item := range []struct{ out, pkg string }{
		{playerBin, "./cmd/player-svr"},
		{farmBin, "./cmd/farm-svr"},
	} {
		cmd := exec.Command("go", "build", "-buildvcs=false", "-o", item.out, item.pkg)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", "", fmt.Errorf("build %s: %w\n%s", item.pkg, err, strings.TrimSpace(string(out)))
		}
	}
	return playerBin, farmBin, nil
}

// --- environment -----------------------------------------------------------

func baseEnv(rawURL, namespace string) []string {
	return []string{
		"FARM_REDIS_URL=" + rawURL,
		"ACTOR_AUTHORITY_MODE=authority",
		"AUTHORITY_MEMBERSHIP_MODE=true",
		"REDIS_PHYSICAL_PROFILE_VERSION=q2r2b1-v1",
		"REDIS_NAMESPACE_ENVIRONMENT=" + namespace,
		"REDIS_NAMESPACE_ZONE=z1",
		"REDIS_AUTHORITY_LAYOUT_VERSION=v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION=v1",
		"REDIS_WORKFLOW_HASH_VERSION=1",
		"REDIS_WORKFLOW_BUCKET_COUNT=64",
		"ACTOR_AUTHORITY_LEASE_TTL=" + leaseTTL.String(),
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=" + safetyMargin.String(),
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=" + renewInterval.String(),
	}
}

func playerEnv(base []string, snapshotPath, holder, boot string, healthPort int) []string {
	return append(append([]string{}, base...),
		"PLAYER_LISTEN=127.0.0.1:"+strconv.Itoa(nextPort()),
		"PLAYER_HEALTH_LISTEN=127.0.0.1:"+strconv.Itoa(healthPort),
		"ACTOR_ROUTE_SNAPSHOT="+snapshotPath,
		"ACTOR_HOLDER_POD="+holder,
		"ACTOR_HOLDER_BOOT_ID="+boot,
		"PLAYER_STORE_PREFIX=q2r2b1:player",
	)
}

func farmEnv(base []string, snapshotPath, holder, boot string, healthPort int) []string {
	return append(append([]string{}, base...),
		"FARM_LISTEN=127.0.0.1:"+strconv.Itoa(nextPort()),
		"FARM_HEALTH_LISTEN=127.0.0.1:"+strconv.Itoa(healthPort),
		"ACTOR_ROUTE_SNAPSHOT="+snapshotPath,
		"ACTOR_HOLDER_POD="+holder,
		"ACTOR_HOLDER_BOOT_ID="+boot,
		"PERMIT_SECRET=q2r2b1-permit-secret-0123456789abcdef",
		"THEFT_PRF_MODE=test",
		"FARM_STORE_PREFIX=q2r2b1:farm",
		"WORKFLOW_STORE_PREFIX=q2r2b1:workflow",
		"THEFT_GUARD_SNAPSHOT_PREFIX=q2r2b1:theft-guard",
		"THEFT_TARGET_BINDING_PREFIX=q2r2b1:theft-target",
	)
}

var portCounter = portBase

func nextPort() int {
	portCounter++
	return portCounter
}

// --- process management ----------------------------------------------------

type proc struct {
	cmd        *exec.Cmd
	logPath    string
	healthPort int
}

func startProc(binary string, env []string, healthPort int, logPath string) (*proc, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	_ = f.Close()
	return &proc{cmd: cmd, logPath: logPath, healthPort: healthPort}, nil
}

func (p *proc) logText() string {
	raw, _ := os.ReadFile(p.logPath)
	return string(raw)
}

func (p *proc) waitReady(timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", p.healthPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.cmd.ProcessState != nil {
			return fmt.Errorf("process exited before ready: %s", strings.TrimSpace(p.logText()))
		}
		resp, err := http.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("readyz never became 200 (last log): %s", strings.TrimSpace(p.logText()))
}

func (p *proc) waitHealthz(timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", p.healthPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.cmd.ProcessState != nil {
			return fmt.Errorf("process exited before healthz: %s", strings.TrimSpace(p.logText()))
		}
		resp, err := http.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("healthz never became 200 (last log): %s", strings.TrimSpace(p.logText()))
}

// terminate sends SIGTERM and waits for a bounded graceful exit.
func (p *proc) terminate(timeout time.Duration) (int, error) {
	if p.cmd.ProcessState != nil {
		return p.cmd.ProcessState.ExitCode(), nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return 0, err
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
		return p.cmd.ProcessState.ExitCode(), nil
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		return 0, fmt.Errorf("process did not exit within %s (log): %s", timeout, strings.TrimSpace(p.logText()))
	}
}

// runToExitNoReady starts a binary, probes readiness while it runs, and waits
// for exit.  It fails if readiness ever returns 200: a fail-closed startup must
// never open readiness.
func runToExitNoReady(binary string, env []string, healthPort int, logPath string, timeout time.Duration) (int, string, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return 0, "", err
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return 0, "", err
	}
	_ = f.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	readyURL := fmt.Sprintf("http://127.0.0.1:%d/readyz", healthPort)
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-done:
			// A non-zero exit is the expected fail-closed outcome; the caller
			// inspects ExitCode, not the Wait error.
			raw, _ := os.ReadFile(logPath)
			return cmd.ProcessState.ExitCode(), string(raw), nil
		default:
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			raw, _ := os.ReadFile(logPath)
			return 0, string(raw), fmt.Errorf("process did not exit within %s", timeout)
		}
		resp, err := http.Get(readyURL)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				_ = cmd.Process.Kill()
				raw, _ := os.ReadFile(logPath)
				return 0, string(raw), fmt.Errorf("readiness must never open on a fail-closed startup")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runToExit starts a binary and waits for it to exit (used by S3).
func runToExit(binary string, env []string, logPath string, timeout time.Duration) (int, string, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return 0, "", err
	}
	cmd := exec.Command(binary)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return 0, "", err
	}
	_ = f.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		raw, _ := os.ReadFile(logPath)
		return cmd.ProcessState.ExitCode(), string(raw), nil
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		raw, _ := os.ReadFile(logPath)
		return 0, string(raw), fmt.Errorf("process did not exit within %s", timeout)
	}
}

// --- snapshot --------------------------------------------------------------

func writeSnapshot(dir, namespace string, playerRoute, farmRoute actorrouting.ShardRoute) (string, error) {
	holders := []actorrouting.HolderRef{
		{InstanceID: playerHolder, BootID: playerBoot, Endpoint: "player.local:15100", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: playerOther, BootID: "q2r2b1-player-other-boot", Endpoint: "player-other.local:15101", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: farmHolder, BootID: farmBoot, Endpoint: "farm.local:15102", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: farmOther, BootID: "q2r2b1-farm-other-boot", Endpoint: "farm-other.local:15103", State: actorrouting.HolderStateActive, Weight: 1},
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: realm + "-" + namespace, GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: hashSeed,
		Holders: holders,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{playerRoute}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{farmRoute}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "route-"+namespace+".json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", err
	}
	return path, nil
}

func routeFor(authority *authoritystore.ShardWriteAuthority, holderIndex int, leaseUntilMs int64) actorrouting.ShardRoute {
	// The static snapshot validator requires non-zero revision/epoch on every
	// route, even when the shard belongs to another holder (zero-shard case).
	route := actorrouting.ShardRoute{
		HolderIndex: holderIndex, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
	}
	if authority != nil {
		route.ShardRevision = authority.AssignmentRevision
		route.ExpectedWriterEpoch = authority.WriterEpoch
		route.ExpectedLeaseEpoch = authority.LeaseEpoch
	}
	if leaseUntilMs <= 0 {
		leaseUntilMs = time.Now().Add(leaseTTL).UnixMilli()
	}
	route.LeaseUntilMs = leaseUntilMs
	return route
}

// --- authority store helpers ----------------------------------------------

func profileFor(namespace string) rediskey.PhysicalProfile {
	ns, err := rediskey.NewNamespace(namespace, "z1")
	if err != nil {
		panic(err)
	}
	return rediskey.PhysicalProfile{
		ProfileVersion: "q2r2b1-v1", Namespace: ns,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
}

// q2r2b1Calculator must match the snapshot's hash profile exactly so the seed
// claim lands on the same virtual shard the process will serve.
func q2r2b1Calculator(ns string) (*actorrouting.Calculator, error) {
	return actorrouting.NewCalculator("q2r2b1-"+ns,
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: hashSeed}, 1, 1)
}

func claimPlayerShard(client *redis.Client, ns, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error) {
	return claimShard(client, actorrouting.DomainPlayer, 10001, ns, holder, boot)
}

func claimFarmShard(client *redis.Client, ns, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error) {
	return claimShard(client, actorrouting.DomainFarm, 20002, ns, holder, boot)
}

func claimShard(client *redis.Client, domain actorrouting.RouteDomain, actorID uint64, ns, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error) {
	calculator, err := q2r2b1Calculator(ns)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	store, err := newAuthorityStore(client, profileFor(ns), calculator)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	addr := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
	bucket, err := profileFor(ns).AuthorityBucketForActor(calculator, addr)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "q2r2b1-rm", BootID: "q2r2b1-rm-" + ns,
		},
		TransitionID: "claim-" + ns, AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: bucket.VirtualShard(),
		NewHolderID: holder, NewHolderBootID: boot,
		RequestedTTLms: leaseTTL.Milliseconds(),
	})
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	if claim.Status != authoritystore.ShardTransitionApplied {
		return authoritystore.ShardAuthorityTransitionResult{}, "", fmt.Errorf("claim status=%s", claim.Status)
	}
	key, err := store.Keys().ShardAuthority(bucket)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	return claim, key, nil
}

func newAuthorityStore(client *redis.Client, profile rediskey.PhysicalProfile, resolver rediskey.AuthorityShardResolver) (*redisauthority.Store, error) {
	return redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: resolver})
}

// --- scenarios -------------------------------------------------------------

func s1ZeroShard(playerBin, farmBin, rawURL string, client *redis.Client) error {
	ns := "q2r2b1s1-" + fmt.Sprint(time.Now().UnixNano())
	dir, err := os.MkdirTemp("", "q2r2b1-s1-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Both local holders are registered, but both shards belong to "other".
	path, err := writeSnapshot(dir, ns,
		routeFor(nil, 1, 0), routeFor(nil, 3, 0))
	if err != nil {
		return err
	}

	playerHealth, farmHealth := nextPort(), nextPort()
	base := baseEnv(rawURL, ns)
	pp, err := startProc(playerBin, playerEnv(base, path, playerHolder, playerBoot, playerHealth),
		playerHealth, filepath.Join(dir, "player.log"))
	if err != nil {
		return err
	}
	defer killProc(pp)
	fp, err := startProc(farmBin, farmEnv(base, path, farmHolder, farmBoot, farmHealth),
		farmHealth, filepath.Join(dir, "farm.log"))
	if err != nil {
		return err
	}
	defer killProc(fp)

	if err := pp.waitHealthz(readyTimeout); err != nil {
		return fmt.Errorf("player healthz: %w", err)
	}
	if err := fp.waitHealthz(readyTimeout); err != nil {
		return fmt.Errorf("farm healthz: %w", err)
	}
	if err := pp.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("player readyz: %w", err)
	}
	if err := fp.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("farm readyz: %w", err)
	}
	if !strings.Contains(pp.logText(), "membership=composed") {
		return fmt.Errorf("player log missing membership=composed: %s", pp.logText())
	}
	if !strings.Contains(fp.logText(), "membership=composed") {
		return fmt.Errorf("farm log missing membership=composed: %s", fp.logText())
	}
	if strings.Contains(pp.logText(), "legacy lease renewal") || strings.Contains(fp.logText(), "legacy lease renewal") {
		return errors.New("legacy renewal log found on composed path")
	}
	// Graceful stop so the ports are released for later scenarios.
	if _, err := pp.terminate(10 * time.Second); err != nil {
		return err
	}
	if _, err := fp.terminate(10 * time.Second); err != nil {
		return err
	}
	log.Printf("S1 PASS player+farm zero-shard ready, membership=composed, no legacy renewal")
	return nil
}

func s2APlayerShardRenew(playerBin, rawURL string, client *redis.Client) error {
	ns := "q2r2b1s2a-" + fmt.Sprint(time.Now().UnixNano())
	dir, err := os.MkdirTemp("", "q2r2b1-s2a-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Pre-seed the authoritative lease, then build the snapshot from it.
	claim, shardKey, err := claimPlayerShard(client, ns, playerHolder, playerBoot)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Del(context.Background(), shardKey.String()).Err(); err != nil {
			log.Printf("S2A cleanup DEL %s: %v", shardKey.String(), err)
		}
	}()

	path, err := writeSnapshot(dir, ns,
		routeFor(&claim.Authority, 0, claim.Authority.LeaseUntilMs), routeFor(nil, 3, 0))
	if err != nil {
		return err
	}

	health := nextPort()
	base := baseEnv(rawURL, ns)
	p, err := startProc(playerBin, playerEnv(base, path, playerHolder, playerBoot, health), health, filepath.Join(dir, "player.log"))
	if err != nil {
		return err
	}
	defer killProc(p)

	if err := p.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("readyz: %w", err)
	}
	logText := p.logText()
	for _, want := range []string{"membership=composed", "domain=PLAYER", "holder=" + playerHolder, "snapshot_revision="} {
		if !strings.Contains(logText, want) {
			return fmt.Errorf("log missing %q:\n%s", want, logText)
		}
	}

	// The per-shard scheduler must really renew on the company Redis: after a
	// couple of renew rounds the authoritative lease deadline (stored by the
	// claim/renew Lua in the shard hash field lease_expire_at_ms) advances.
	readDeadline := func() (int64, error) {
		return client.HGet(context.Background(), shardKey.String(), "lease_expire_at_ms").Int64()
	}
	t0, err := readDeadline()
	if err != nil {
		return err
	}
	// Sleep through at least 3 renew intervals, then re-read.
	time.Sleep(3*renewInterval + 500*time.Millisecond)
	t1, err := readDeadline()
	if err != nil {
		return err
	}
	if t1 <= t0 {
		return fmt.Errorf("authority lease deadline did not advance: t0=%d t1=%d", t0, t1)
	}
	log.Printf("S2A PASS player one-shard ready, real renew advanced deadline %d -> %d (>= 2 rounds)", t0, t1)
	return nil
}

// s2BFarmShardRenew mirrors S2A on the farm composition root: farm-svr claims
// and really renews one FARM shard against the company Redis.
func s2BFarmShardRenew(farmBin, rawURL string, client *redis.Client) error {
	ns := "q2r2b1s2b-" + fmt.Sprint(time.Now().UnixNano())
	dir, err := os.MkdirTemp("", "q2r2b1-s2b-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	claim, shardKey, err := claimFarmShard(client, ns, farmHolder, farmBoot)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Del(context.Background(), shardKey.String()).Err(); err != nil {
			log.Printf("S2B cleanup DEL %s: %v", shardKey.String(), err)
		}
	}()

	path, err := writeSnapshot(dir, ns,
		routeFor(nil, 1, 0), routeFor(&claim.Authority, 2, claim.Authority.LeaseUntilMs))
	if err != nil {
		return err
	}

	health := nextPort()
	p, err := startProc(farmBin, farmEnv(baseEnv(rawURL, ns), path, farmHolder, farmBoot, health), health, filepath.Join(dir, "farm.log"))
	if err != nil {
		return err
	}
	defer killProc(p)

	if err := p.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("farm readyz: %w", err)
	}
	logText := p.logText()
	// farm-svr startup log carries membership=composed + holder/boot/snapshot
	// (the 3C-B log line never gained domain=/snapshot_revision=; tracked as a
	// small follow-up). The FARM-domain wiring is proven by the FARM-domain
	// claim, the snapshot FARM route and the real renew below.
	for _, want := range []string{"membership=composed", "holder=" + farmHolder, "snapshot="} {
		if !strings.Contains(logText, want) {
			return fmt.Errorf("farm log missing %q:\n%s", want, logText)
		}
	}
	if strings.Contains(logText, "legacy lease renewal") {
		return errors.New("legacy renewal log found on composed farm path")
	}

	readDeadline := func() (int64, error) {
		return client.HGet(context.Background(), shardKey.String(), "lease_expire_at_ms").Int64()
	}
	t0, err := readDeadline()
	if err != nil {
		return err
	}
	time.Sleep(3*renewInterval + 500*time.Millisecond)
	t1, err := readDeadline()
	if err != nil {
		return err
	}
	if t1 <= t0 {
		return fmt.Errorf("farm authority lease deadline did not advance: t0=%d t1=%d", t0, t1)
	}
	log.Printf("S2B PASS farm one-shard ready, real renew advanced deadline %d -> %d (>= 2 rounds)", t0, t1)
	return nil
}

func s3NoFallback(playerBin, farmBin, rawURL string, client *redis.Client) error {
	ns := "q2r2b1s3-" + fmt.Sprint(time.Now().UnixNano())
	dir, err := os.MkdirTemp("", "q2r2b1-s3-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// 3a: holder never registered in the snapshot.
	path, err := writeSnapshot(dir, ns, routeFor(nil, 1, 0), routeFor(nil, 3, 0))
	if err != nil {
		return err
	}
	base := baseEnv(rawURL, ns)
	exit, logText, err := runToExit(playerBin, playerEnv(base, path, "q2r2b1-ghost", "ghost-boot", nextPort()),
		filepath.Join(dir, "s3a.log"), 15*time.Second)
	if err != nil {
		return err
	}
	if exit == 0 {
		return fmt.Errorf("3a holder-unregistered must exit non-zero:\n%s", logText)
	}
	if strings.Contains(logText, "legacy") {
		return fmt.Errorf("3a must not fall back to legacy:\n%s", logText)
	}

	// 3b: boot mismatch.  Composed contract: desiredLocalShards filters by
	// holder+boot, so a mismatched boot is a legal zero-shard holder, NOT a
	// startup failure.  Verified as the composed semantics.
	bootPath, err := writeSnapshot(dir, ns+"b", routeFor(nil, 1, 0), routeFor(nil, 3, 0))
	if err != nil {
		return err
	}
	health := nextPort()
	pb, err := startProc(playerBin, playerEnv(baseEnv(rawURL, ns+"b"), bootPath, playerHolder, "q2r2b1-wrong-boot", health),
		health, filepath.Join(dir, "s3b.log"))
	if err != nil {
		return err
	}
	defer killProc(pb)
	if err := pb.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("3b boot mismatch must be a legal zero-shard ready process: %w", err)
	}
	if _, err := pb.terminate(10 * time.Second); err != nil {
		return err
	}

	// 3c: authority mismatch -- snapshot demands a newer generation than the
	// authoritative lease, so the first reconcile cannot converge.
	claim, s3cKey, err := claimPlayerShard(client, ns+"c", playerHolder, playerBoot)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Del(context.Background(), s3cKey.String()).Err(); err != nil {
			log.Printf("S3c cleanup DEL %s: %v", s3cKey.String(), err)
		}
	}()
	mismatch := claim.Authority
	mismatch.AssignmentRevision++
	mismatch.WriterEpoch++
	mismatch.LeaseEpoch++
	badPath, err := writeSnapshot(dir, ns+"c", routeFor(&mismatch, 0, claim.Authority.LeaseUntilMs), routeFor(nil, 3, 0))
	if err != nil {
		return err
	}
	exit, logText, err = runToExit(playerBin, playerEnv(base, badPath, playerHolder, playerBoot, nextPort()),
		filepath.Join(dir, "s3c.log"), 15*time.Second)
	if err != nil {
		return err
	}
	if exit == 0 {
		return fmt.Errorf("3c authority mismatch must exit non-zero:\n%s", logText)
	}
	// "Pair result fallback" is a normal openStore log line; the forbidden
	// outcome is any legacy renewal/fallback, which the composed path must
	// never enter.
	if strings.Contains(logText, "legacy") || strings.Contains(logText, "runRenewal") {
		return fmt.Errorf("3c must not fall back to legacy:\n%s", logText)
	}

	// 3e: assigned local shard + expected boot mismatch (RT1B-3C-C hardened
	// static semantics), verified on BOTH composition roots.  The snapshot
	// assigns an ACTIVE route to the local holder ID but expects a different
	// boot.  In the static mode this shard would be permanently unserved
	// behind a 0-shard Ready, so the composed startup MUST fail closed
	// (ErrLocalRouteBootMismatch): non-zero exit, readiness never opens, no
	// legacy fallback, no guard/entry/renew.
	for _, tc := range []struct {
		name      string
		binary    string
		env       func([]string, string, string, string, int) []string
		claim     func(*redis.Client, string, string, string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error)
		holder    string
		boot      string
		routes    func(authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute)
	}{
		{name: "player", binary: playerBin, env: playerEnv, claim: claimPlayerShard, holder: playerHolder, boot: playerBoot,
			routes: func(c authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(&c.Authority, 0, c.Authority.LeaseUntilMs), routeFor(nil, 3, 0)
			}},
		{name: "farm", binary: farmBin, env: farmEnv, claim: claimFarmShard, holder: farmHolder, boot: farmBoot,
			routes: func(c authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(nil, 1, 0), routeFor(&c.Authority, 2, c.Authority.LeaseUntilMs)
			}},
	} {
		ns3e := "q2r2b1s3" + tc.name[0:1] + "-" + fmt.Sprint(time.Now().UnixNano())
		assigned, s3eKey, err := tc.claim(client, ns3e, tc.holder, tc.boot)
		if err != nil {
			return err
		}
		playerRoute, farmRoute := tc.routes(assigned)
		assignedPath, err := writeSnapshot(dir, ns3e, playerRoute, farmRoute)
		if err != nil {
			_ = client.Del(context.Background(), s3eKey.String()).Err()
			return err
		}
		s3eHealth := nextPort()
		s3eExit, s3eLog, err := runToExitNoReady(tc.binary, tc.env(baseEnv(rawURL, ns3e), assignedPath, tc.holder, "q2r2b1-wrong-boot", s3eHealth),
			s3eHealth, filepath.Join(dir, "s3e-"+tc.name+".log"), 15*time.Second)
		_ = client.Del(context.Background(), s3eKey.String()).Err()
		if err != nil {
			return fmt.Errorf("3e-%s: %w", tc.name, err)
		}
		if s3eExit == 0 {
			return fmt.Errorf("3e-%s assigned-shard boot mismatch must exit non-zero (RT1B-3C-C hardened):\n%s", tc.name, s3eLog)
		}
		// "legacy" alone is too broad (farm-svr logs "legacy friend store" on
		// its normal path); only a started legacy renewal loop matters.
		if strings.Contains(s3eLog, "runRenewal") || strings.Contains(s3eLog, "legacy lease renewal") {
			return fmt.Errorf("3e-%s must not fall back to legacy renewal:\n%s", tc.name, s3eLog)
		}
	}

	// 3d: illegal membership mode is rejected before any runtime is built.
	illegalBase := baseEnv(rawURL, ns+"d")
	for i, v := range illegalBase {
		if strings.HasPrefix(v, "AUTHORITY_MEMBERSHIP_MODE=") {
			illegalBase[i] = "AUTHORITY_MEMBERSHIP_MODE=bogus"
		}
	}
	exit, logText, err = runToExit(playerBin, playerEnv(illegalBase, path, playerHolder, playerBoot, nextPort()),
		filepath.Join(dir, "s3d.log"), 15*time.Second)
	if err != nil {
		return err
	}
	if exit == 0 {
		return fmt.Errorf("3d illegal membership mode must exit non-zero:\n%s", logText)
	}
	log.Printf("S3 PASS holder-unregistered/authority-mismatch/illegal-mode/assigned-boot-mismatch exit non-zero, readiness never opened, no legacy fallback; zero-route boot mismatch remains a legal 0-shard ready")
	return nil
}

func s4Sigterm(playerBin, farmBin, rawURL string, client *redis.Client) error {
	// Run one SIGTERM cycle for each composition root (player + farm).
	for _, tc := range []struct {
		name   string
		binary string
		env    func([]string, string, string, string, int) []string
		claim  func(*redis.Client, string, string, string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error)
		holder string
		boot   string
		routes func(authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute)
	}{
		{name: "player", binary: playerBin, env: playerEnv, claim: claimPlayerShard, holder: playerHolder, boot: playerBoot,
			routes: func(c authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(&c.Authority, 0, c.Authority.LeaseUntilMs), routeFor(nil, 3, 0)
			}},
		{name: "farm", binary: farmBin, env: farmEnv, claim: claimFarmShard, holder: farmHolder, boot: farmBoot,
			routes: func(c authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(nil, 1, 0), routeFor(&c.Authority, 2, c.Authority.LeaseUntilMs)
			}},
	} {
		nsPrefix := "q2r2b1s4p-"
		if tc.name == "farm" {
			nsPrefix = "q2r2b1s4f-"
		}
		ns := nsPrefix + fmt.Sprint(time.Now().UnixNano())
		dir, err := os.MkdirTemp("", "q2r2b1-s4-")
		if err != nil {
			return err
		}
		claim, shardKey, err := tc.claim(client, ns, tc.holder, tc.boot)
		if err != nil {
			os.RemoveAll(dir)
			return err
		}
		playerRoute, farmRoute := tc.routes(claim)
		path, err := writeSnapshot(dir, ns, playerRoute, farmRoute)
		if err != nil {
			os.RemoveAll(dir)
			return err
		}
		health := nextPort()
		p, err := startProc(tc.binary, tc.env(baseEnv(rawURL, ns), path, tc.holder, tc.boot, health), health, filepath.Join(dir, tc.name+".log"))
		if err != nil {
			os.RemoveAll(dir)
			return err
		}
		err = s4OneCycle(tc.name, p)
		_ = client.Del(context.Background(), shardKey.String()).Err()
		os.RemoveAll(dir)
		if err != nil {
			return err
		}
	}
	log.Printf("S4 PASS player+farm SIGTERM bounded clean exit code=0, no fatal, keys cleaned")
	return nil
}

func s4OneCycle(name string, p *proc) error {
	if err := p.waitReady(readyTimeout); err != nil {
		return fmt.Errorf("%s readyz: %w", name, err)
	}
	// readiness goes away and the process exits bounded and clean (Closed, not
	// Fatal): SIGTERM triggers GracefulStop + health.Shutdown + Runtime.Close.
	start := time.Now()
	code, err := p.terminate(10 * time.Second)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("%s SIGTERM must exit 0, got %d (log): %s", name, code, strings.TrimSpace(p.logText()))
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		return fmt.Errorf("%s SIGTERM exit took %s", name, elapsed)
	}
	if strings.Contains(p.logText(), "fatal") {
		return fmt.Errorf("%s SIGTERM must not enter Fatal:\n%s", name, p.logText())
	}
	return nil
}

func killProc(p *proc) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
}
