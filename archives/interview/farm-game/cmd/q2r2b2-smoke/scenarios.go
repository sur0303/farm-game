package main

// N1..N6 scenarios.  Each builds a per-process fault proxy rig and asserts the
// frozen lease semantics across real network faults.

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

func logf(format string, args ...any) { log.Printf(format, args...) }

func playerRig(dir, artifacts string, binary string, direct *redis.Client, target string) *scenarioRig {
	return &scenarioRig{
		dir: dir, artifacts: artifacts, binary: binary, direct: direct, target: target, rawURL: redisRawURL,
		envFn: playerEnv, holder: playerHolder, boot: playerBoot,
		domain: actorrouting.DomainPlayer, actorID: 10001,
		routeFor: func(claim authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
			return routeFor(&claim.Authority, 0), routeFor(nil, 3)
		},
	}
}

// N1: the fault proxy listener (and all conns) goes down -> new dials are
// ECONNREFUSED (provably not sent).  The renew must classify Transient (not
// LostGeneration): the process stays ready and, after the proxy returns, the
// same-generation renew keeps the authoritative lease advancing.
func n1ConnectRefused(dir, artifacts string, binary string, direct *redis.Client, target string, v *verdicts) error {
	ns := "q2r2b2n1-" + fmt.Sprint(time.Now().UnixNano())
	rig := playerRig(dir, artifacts, binary, direct, target)
	return rig.runBody(ns, v, "N1-connect-refused", func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error {
		// Confirm renew is alive before the fault.
		d0, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		proxy.Down()
		time.Sleep(2500 * time.Millisecond)
		// Process must remain ready (Transient, not Fatal/LostGeneration).
		code, err := p.readyz(time.Second)
		if err != nil || code != 200 {
			return fmt.Errorf("process not ready during refused-connect: code=%d err=%v", code, err)
		}
		if p.exited() {
			return fmt.Errorf("process exited during refused-connect")
		}
		if err := proxy.Up(); err != nil {
			return err
		}
		// After the proxy returns, the same-generation renew resumes and the
		// authoritative deadline must advance beyond its pre-fault sample.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			d1, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if d1 > d0 {
				tw.write(map[string]any{"event": "renew-resumed", "before": d0, "after": d1})
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		return fmt.Errorf("renew did not resume after proxy recovery (before=%d)", d0)
	})
}

// N2: short blackhole (responses withheld) shorter than the local safety
// deadline.  Renew times out -> Transient backoff; the process stays ready and
// the lease deadline keeps advancing once the blackhole clears.
func n2ShortBlackhole(dir, artifacts string, binary string, direct *redis.Client, target string, v *verdicts) error {
	ns := "q2r2b2n2-" + fmt.Sprint(time.Now().UnixNano())
	rig := playerRig(dir, artifacts, binary, direct, target)
	return rig.runBody(ns, v, "N2-short-blackhole", func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error {
		proxy.SetMode(modeBlackhole, "start", 0)
		time.Sleep(2000 * time.Millisecond) // < 6.5s local safety window
		code, err := p.readyz(time.Second)
		if err != nil || code != 200 {
			return fmt.Errorf("not ready during short blackhole: code=%d err=%v", code, err)
		}
		proxy.StopMode()
		// Renew must resume.
		d0, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			d1, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if d1 > d0 {
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		return fmt.Errorf("renew did not resume after short blackhole")
	})
}

// N3: blackhole long enough to cross the LOCAL safety deadline, but with the
// authoritative lease still valid (requests reached Redis).  Local deadline
// expiry is process-internal (CanServeShard=false; not externally observable
// without a process metric), so we deterministically compute the blackhole
// window from Redis TIME + lease_expire_at_ms - safetyMargin and, after
// recovery, require the same-generation renew to re-admit (deadline advances)
// while the process stays ready.
func n3CrossLocalDeadline(dir, artifacts string, binary string, direct *redis.Client, target string, v *verdicts) error {
	ns := "q2r2b2n3-" + fmt.Sprint(time.Now().UnixNano())
	rig := playerRig(dir, artifacts, binary, direct, target)
	return rig.runBody(ns, v, "N3-cross-local-deadline", func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error {
		// Wait for a successful renew sample.
		var window time.Duration
		for i := 0; i < 30; i++ {
			now, lease, err := readLeaseWindow(context.Background(), direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if remain := lease - now; remain > 3000 {
				// cross the local deadline (lease - safetyMargin) with lease
				// still valid (>= 1s of authority lease left after window).
				window = time.Duration(remain-int64(safetyMargin.Milliseconds())-1200) * time.Millisecond
				break
			}
			time.Sleep(400 * time.Millisecond)
		}
		if window < 1500*time.Millisecond {
			return fmt.Errorf("could not size a deterministic local-deadline window (window=%v)", window)
		}
		tw.write(map[string]any{"event": "blackhole-start", "window_ms": window.Milliseconds()})
		proxy.SetMode(modeBlackhole, "cross-local-deadline", 0)
		time.Sleep(window)
		proxy.StopMode()

		d0, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			d1, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if d1 > d0 {
				code, err := p.readyz(time.Second)
				if err != nil || code != 200 {
					return fmt.Errorf("re-admitted but not ready: code=%d err=%v", code, err)
				}
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		return fmt.Errorf("same-generation renew did not re-admit after local-deadline blackhole")
	})
}

// N4: blackhole-no-forward for longer than the authoritative TTL -> Redis
// receives no renew -> the authoritative lease truly expires.  After recovery a
// late same-generation renew cannot revive it (LEASE_EXPIRED / LostGeneration);
// the process stays ready (single-shard lost generation, not Runtime Fatal).
func n4AuthorityExpiry(dir, artifacts string, binary string, direct *redis.Client, target string, v *verdicts) error {
	ns := "q2r2b2n4-" + fmt.Sprint(time.Now().UnixNano())
	rig := playerRig(dir, artifacts, binary, direct, target)
	return rig.runBody(ns, v, "N4-authority-expiry", func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error {
		// Confirm the lease is alive before the block.
		_, _, err := readLeaseWindow(context.Background(), direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		proxy.SetMode(modeBlackholeNF, "block-renews", 0)
		// Hold until the authoritative lease has truly expired (Redis TIME has
		// passed lease_expire_at_ms); with no renew reaching Redis the TTL is
		// never refreshed.
		for {
			now, lease, err := readLeaseWindow(context.Background(), direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if lease <= now+100 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		// Ensure a full TTL has passed without any renew reaching Redis.
		time.Sleep(2000 * time.Millisecond)
		proxy.StopMode()

		// The lease must have stopped advancing (cannot revive).  Sample after
		// recovery; a revived lease would keep pushing lease_expire_at_ms far
		// into the future.
		time.Sleep(2500 * time.Millisecond)
		now, lease, err := readLeaseWindow(context.Background(), direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		if lease > now+leaseTTLMs/2 {
			return fmt.Errorf("late renew revived the expired authority lease: now=%d lease=%d", now, lease)
		}
		code, err := p.readyz(time.Second)
		if err != nil || code != 200 {
			return fmt.Errorf("process not ready after authority expiry (should be 0-shard, not Fatal): code=%d err=%v", code, err)
		}
		if p.exited() {
			return fmt.Errorf("process exited after authority expiry")
		}
		// A later fresh claim by the same holder (new generation) must be
		// possible, but the OLD generation cannot resurrect.  We assert only
		// the non-revival above; generation-strict fence is a Q2R-2A store
		// property already proven on the company Redis.
		tw.write(map[string]any{"event": "no-revival", "now": now, "lease": lease})
		return nil
	})
}

// N5: post-execution response drop.  The proxy forwards the renew to Redis,
// reads the reply, discards it and closes the connection: the client sees a
// broken reply (TimeoutUnknown).  This must classify Transient -- the process
// stays ready and, after recovery, renew resumes.  The commit path
// (ALREADY_COMMITTED resolution) is proven on the company Redis by Q2R-2A and
// is registered here as real_process_post_commit_response_drop = PROBE_PENDING
// (no business commit RPC exists to inject in a real process).
func n5ResponseDrop(dir, artifacts string, binary string, direct *redis.Client, target string, v *verdicts) error {
	ns := "q2r2b2n5-" + fmt.Sprint(time.Now().UnixNano())
	rig := playerRig(dir, artifacts, binary, direct, target)
	return rig.runBody(ns, v, "N5-response-drop", func(p *proc, proxy *faultProxy, shardKey string, tw *timelineWriter) error {
		d0, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
		if err != nil {
			return err
		}
		// Drop up to 8 renew replies, then restore.
		proxy.SetMode(modeDropResponse, "drop-renew-replies", 8)
		time.Sleep(1500 * time.Millisecond)
		proxy.StopMode()

		code, err := p.readyz(time.Second)
		if err != nil || code != 200 {
			return fmt.Errorf("not ready after response drop: code=%d err=%v", code, err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			d1, err := leaseDeadline(direct, rediskey.RedisKey(shardKey))
			if err != nil {
				return err
			}
			if d1 > d0 {
				return nil
			}
			time.Sleep(300 * time.Millisecond)
		}
		return fmt.Errorf("renew did not resume after response drop")
	})
}

// N6: SIGTERM while a renew is blocked on the fault proxy.  The in-flight
// renew is cancelled by the process context; the runtime must enter Closed (not
// Fatal), drain guards/entries and exit bounded.  Run once for player and once
// for farm.
func n6SigtermBlocked(dir, artifacts string, playerBin, farmBin string, direct *redis.Client, target string, v *verdicts) error {
	for _, tc := range []struct {
		name     string
		binary   string
		envFn    func(rawURL, ns, proxyAddr, snapshotPath, holder, boot string, healthPort int) []string
		holder   string
		boot     string
		domain   actorrouting.RouteDomain
		actorID  uint64
		routeFor func(claim authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute)
	}{
		{name: "player", binary: playerBin, envFn: playerEnv, holder: playerHolder, boot: playerBoot, domain: actorrouting.DomainPlayer, actorID: 10001,
			routeFor: func(claim authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(&claim.Authority, 0), routeFor(nil, 3)
			}},
		{name: "farm", binary: farmBin, envFn: farmEnv, holder: farmHolder, boot: farmBoot, domain: actorrouting.DomainFarm, actorID: 20002,
			routeFor: func(claim authoritystore.ShardAuthorityTransitionResult) (actorrouting.ShardRoute, actorrouting.ShardRoute) {
				return routeFor(nil, 3), routeFor(&claim.Authority, 1)
			}},
	} {
		ns := "q2r2b2n6" + tc.name[0:1] + "-" + fmt.Sprint(time.Now().UnixNano())
		dir := dir + "-" + tc.name
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		proxy := newFaultProxy(target, dir+"/fault-proxy-events.ndjson")
		if err := proxy.Start(); err != nil {
			return err
		}
		claim, shardKey, err := claimShard(direct, tc.domain, tc.actorID, ns, tc.holder, tc.boot)
		if err != nil {
			return err
		}
		pr, fr := tc.routeFor(claim)
		snapshotPath, err := writeSnapshot(dir, ns, pr, fr)
		if err != nil {
			_ = direct.Del(context.Background(), shardKey.String()).Err()
			return err
		}
		health := nextPort()
		p, err := startProc(tc.binary, tc.envFn(redisRawURL, ns, proxy.Addr(), snapshotPath, tc.holder, tc.boot, health), health, dir+"/process.log")
		if err != nil {
			_ = direct.Del(context.Background(), shardKey.String()).Err()
			return err
		}
		if err := p.waitReady(30 * time.Second); err != nil {
			killProc(p)
			_ = direct.Del(context.Background(), shardKey.String()).Err()
			return err
		}
		// Block renews, then SIGTERM while the renew is in flight.
		proxy.SetMode(modeBlackhole, "block-before-sigterm", 0)
		time.Sleep(800 * time.Millisecond)
		code, err := p.terminate(10 * time.Second)
		_ = proxy.FlushEvents()
		_ = direct.Del(context.Background(), shardKey.String()).Err()
		pass := err == nil && code == 0 && !strings.Contains(strings.ToLower(p.logText()), "fatal")
		v.add("N6-sigterm-"+tc.name, pass, fmt.Sprintf("exit=%d err=%v fatal=%v", code, err, !pass))
		if !pass {
			return fmt.Errorf("N6-%s SIGTERM failed: exit=%d err=%v", tc.name, code, err)
		}
		logf("N6-%s PASS", tc.name)
	}
	return nil
}
