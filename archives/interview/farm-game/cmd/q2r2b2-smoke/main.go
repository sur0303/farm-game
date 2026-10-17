// Command q2r2b2-smoke boots the REAL player-svr / farm-svr binaries against
// the company proxy Redis THROUGH a per-process test-only TCP fault proxy, and
// verifies per-shard lease semantics across real disconnects, blackholes,
// response drops and recovery:
//
//	N1  connect refused before send            -> Transient, lease alive, no Fatal
//	N2  short blackhole < local safety deadline -> Transient backoff, ready, recover
//	N3  blackhole past local deadline, recover before authority expiry
//	N4  recovery after authoritative lease expiry -> cannot revive
//	N5  post-execution response drop (deterministic) -> Transient; commit
//	    ALREADY_COMMITTED replay stays PROBE_PENDING (Q2R-2A evidence)
//	N6  SIGTERM while a renew is blocked        -> Closed (not Fatal), bounded exit
//
// Constraints: Q2R2_LIVE=1 + FARM_REDIS_URL=<secret>; unique namespace per
// scenario; exact-key DEL + EXISTS cleanup; no KEYS/SCAN; no SCRIPT FLUSH;
// player and farm each use their own fault proxy endpoint.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// redisRawURL is the company Redis URL read once from the environment; the
// per-process fault proxies and the observer reuse its credentials.
var redisRawURL string

func main() {
	if err := run(); err != nil {
		log.Fatalf("Q2R2B2 FAIL: %v", err)
	}
	log.Printf("Q2R2B2 PASS")
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

	artifacts, err := os.MkdirTemp("", "q2r2b2-artifacts-")
	if err != nil {
		return err
	}
	binDir, err := os.MkdirTemp("", "q2r2b2-bins-")
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
	direct := redis.NewClient(options)
	defer func() { _ = direct.Close() }()
	if err := direct.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("ping company proxy redis: %w", err)
	}

	target := options.Addr
	v := newVerdicts(filepath.Join(artifacts, "verdict.json"))

	scenarios := []struct {
		name string
		fn   func(dir string) error
	}{
		{"N1-connect-refused", func(dir string) error { return n1ConnectRefused(dir, artifacts, playerBin, direct, target, v) }},
		{"N2-short-blackhole", func(dir string) error { return n2ShortBlackhole(dir, artifacts, playerBin, direct, target, v) }},
		{"N3-cross-local-deadline", func(dir string) error { return n3CrossLocalDeadline(dir, artifacts, playerBin, direct, target, v) }},
		{"N4-authority-expiry", func(dir string) error { return n4AuthorityExpiry(dir, artifacts, playerBin, direct, target, v) }},
		{"N5-response-drop", func(dir string) error { return n5ResponseDrop(dir, artifacts, playerBin, direct, target, v) }},
		{"N6-sigterm-blocked", func(dir string) error { return n6SigtermBlocked(dir, artifacts, playerBin, farmBin, direct, target, v) }},
	}
	for _, sc := range scenarios {
		dir := filepath.Join(artifacts, sc.name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := sc.fn(dir); err != nil {
			_ = v.flush(false)
			return fmt.Errorf("%s: %w", sc.name, err)
		}
	}
	writeRunManifest(artifacts, rawURL)
	if err := v.flush(true); err != nil {
		return err
	}
	log.Printf("artifacts: %s", artifacts)
	return nil
}

func buildBinaries(root, binDir string) (string, string, error) {
	playerBin := filepath.Join(binDir, "player-svr")
	farmBin := filepath.Join(binDir, "farm-svr")
	for _, item := range []struct{ out, pkg string }{
		{playerBin, "./cmd/player-svr"},
		{farmBin, "./cmd/farm-svr"},
	} {
		cmd := execCommand(item.out, item.pkg, root)
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", "", fmt.Errorf("build %s: %w\n%s", item.pkg, err, strings.TrimSpace(string(out)))
		}
	}
	return playerBin, farmBin, nil
}

func writeRunManifest(artifacts, rawURL string) {
	redacted := "set-from-secret"
	if strings.Contains(rawURL, "@") {
		redacted = "redacted"
	}
	f, _ := os.Create(filepath.Join(artifacts, "run-manifest.json"))
	defer f.Close()
	_ = json.NewEncoder(f).Encode(map[string]any{
		"redis_profile":      "company-proxy",
		"auth_enabled":       true,
		"cluster_commands_exposed": false,
		"FARM_REDIS_URL":     redacted,
		"membership_mode":    true,
		"remote_watch":       false,
		"fault_proxy":        "per-process test-only TCP proxy",
	})
}

// runOne is the shared N1..N5 helper: seed authority, boot the process against
// a fresh proxy, run the fault body, then SIGTERM and clean the exact key.
type scenarioRig struct {
	dir       string
	artifacts string
	binary    string
	direct    *redis.Client
	target    string
	rawURL    string
	envFn     func(rawURL, ns, proxyAddr, snapshotPath, holder, boot string, healthPort int) []string
	holder    string
	boot      string
	domain    actorrouting.RouteDomain
	actorID   uint64
	// routeFor returns (playerRoute, farmRoute) for the snapshot.
	routeFor func(claim authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute)
	// body runs the fault scenario; proxy and process are live.
	body func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error
}

func (r *scenarioRig) run(ns string, v *verdicts, checkName string) error {
	return r.runBody(ns, v, checkName, r.body)
}

func (r *scenarioRig) runBody(ns string, v *verdicts, checkName string, body func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error) error {
	proxy := newFaultProxy(r.target, filepath.Join(r.dir, "fault-proxy-events.ndjson"))
	if err := proxy.Start(); err != nil {
		return err
	}
	defer func() { _ = proxy.FlushEvents() }()

	claim, shardKey, err := claimShard(r.direct, r.domain, r.actorID, ns, r.holder, r.boot)
	if err != nil {
		return err
	}
	playerRoute, farmRoute := r.routeFor(claim)
	snapshotPath, err := writeSnapshot(r.dir, ns, playerRoute, farmRoute)
	if err != nil {
		return err
	}
	_ = os.WriteFile(filepath.Join(r.dir, "route-snapshot.json"), mustJSON(snapshotPath), 0600)

	health := nextPort()
	p, err := startProc(r.binary, r.envFn(r.rawURL, ns, proxy.Addr(), snapshotPath, r.holder, r.boot, health), health, filepath.Join(r.dir, "process.log"))
	if err != nil {
		return err
	}
	defer killProc(p)
	if err := p.waitReady(30 * time.Second); err != nil {
		return err
	}

	tw, err := newTimelineWriter(filepath.Join(r.dir, "authority-timeline.ndjson"))
	if err != nil {
		return err
	}
	stop := make(chan struct{})
	go observeLoop(context.Background(), r.direct, shardKey, p, tw, 250*time.Millisecond, stop)

	bodyErr := body(p, proxy, shardKey.String(), tw)

	close(stop)
	tw.close()

	// SIGTERM: the process must exit cleanly (Closed, not Fatal).
	code, termErr := p.terminate(10 * time.Second)
	logText := p.logText()
	fatalObserved := strings.Contains(strings.ToLower(logText), "fatal")
	pass := bodyErr == nil && termErr == nil && code == 0 && !fatalObserved
	v.add(checkName, pass, fmt.Sprintf("body_err=%v term_err=%v exit=%d fatal=%v", bodyErr, termErr, code, fatalObserved))

	// Exact-key cleanup.
	if err := r.direct.Del(context.Background(), shardKey.String()).Err(); err != nil {
		return fmt.Errorf("cleanup DEL: %w", err)
	}
	n, err := r.direct.Exists(context.Background(), shardKey.String()).Result()
	if err != nil || n != 0 {
		return fmt.Errorf("cleanup EXISTS=%d err=%v (residue)", n, err)
	}
	_ = os.WriteFile(filepath.Join(r.dir, "process-exit.json"), mustJSON(map[string]any{"exit_code": code}), 0600)
	if !pass {
		return fmt.Errorf("check %s failed (see verdict)", checkName)
	}
	log.Printf("%s PASS", checkName)
	return nil
}

func mustJSON(a any) []byte {
	b, _ := json.Marshal(a)
	return b
}
