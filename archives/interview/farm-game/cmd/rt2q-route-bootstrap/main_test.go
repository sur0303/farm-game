package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
)

func TestValidateExistingBootstrapHolderAdoptsFreshReadyGeneration(t *testing.T) {
	now := time.Now()
	identity := holderidentity.DeploymentIdentity{PodUID: "pod-1", WorkloadUID: "workload-1", Ordinal: 1}
	target := bootstrapTarget{
		domain: actorrouting.DomainPlayer, holder: "player-svr-1", boot: "player-svr-1-pod-1",
		endpoint: "player-svr-1.local:9100", controlEndpoint: "player-svr-1.local:19100",
		tlsIdentity: "holder-control.local", deploymentIdentity: identity,
	}
	record := core.HolderRecord{
		Domain: target.domain, HolderID: target.holder, BootID: target.boot,
		DeploymentIdentity: identity, LogicEndpoint: target.endpoint,
		HolderControlEndpoint: target.controlEndpoint, TLSDNSIdentity: target.tlsIdentity,
		State: core.HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 1,
		LeaseUntil: now.Add(10 * time.Second),
	}
	if err := validateExistingBootstrapHolder(record, target, target.controlEndpoint, target.tlsIdentity, now); err != nil {
		t.Fatal(err)
	}
}

func TestValidateExistingBootstrapHolderRejectsIdentityAndReadinessDrift(t *testing.T) {
	now := time.Now()
	identity := holderidentity.DeploymentIdentity{PodUID: "pod-1", WorkloadUID: "workload-1", Ordinal: 1}
	target := bootstrapTarget{
		domain: actorrouting.DomainFarm, holder: "farm-svr-1", boot: "farm-svr-1-pod-1",
		endpoint: "farm-svr-1.local:9090", controlEndpoint: "farm-svr-1.local:19101",
		tlsIdentity: "holder-control.local", deploymentIdentity: identity,
	}
	record := core.HolderRecord{
		Domain: target.domain, HolderID: target.holder, BootID: target.boot,
		DeploymentIdentity: identity, LogicEndpoint: target.endpoint,
		HolderControlEndpoint: target.controlEndpoint, TLSDNSIdentity: target.tlsIdentity,
		State: core.HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 1,
		LeaseUntil: now.Add(10 * time.Second),
	}

	wrongBoot := record
	wrongBoot.BootID = "farm-svr-1-other-boot"
	if err := validateExistingBootstrapHolder(wrongBoot, target, target.controlEndpoint, target.tlsIdentity, now); err == nil {
		t.Fatal("expected exact generation mismatch")
	}
	notReady := record
	notReady.State = core.HolderStateWarming
	notReady.ProcessReady = false
	if err := validateExistingBootstrapHolder(notReady, target, target.controlEndpoint, target.tlsIdentity, now); err == nil {
		t.Fatal("expected READY_ZERO_SHARD rejection")
	}
	stale := record
	stale.LeaseUntil = now.Add(time.Second)
	if err := validateExistingBootstrapHolder(stale, target, target.controlEndpoint, target.tlsIdentity, now); err == nil {
		t.Fatal("expected fresh verification rejection")
	}
}

func TestBootstrapOnlyPrecedesSingleManagerAssignment(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv(redisURLEnv, "redis://"+server.Addr()+"/0")
	challenge := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	var initial bytes.Buffer
	initialArgs := append(bootstrapTestArgs(challenge, "c0-"+challenge), "--bootstrap-only")
	if err := run(initialArgs, &initial); err != nil {
		t.Fatal(err)
	}
	var initialOutput output
	if err := json.Unmarshal(initial.Bytes(), &initialOutput); err != nil {
		t.Fatal(err)
	}
	if initialOutput.InitialRevision != 1 || initialOutput.FinalRevision != 1 || len(initialOutput.Claims) != 0 {
		t.Fatalf("bootstrap-only output=%+v", initialOutput)
	}
	cfg, err := parseConfig(initialArgs)
	if err != nil {
		t.Fatal(err)
	}
	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: cfg.environment, NamespaceZone: cfg.zone, PhysicalProfile: cfg.profile,
		AuthorityLayout: cfg.authorityLayout, WorkflowLayout: cfg.workflowLayout,
		WorkflowHashVersion: cfg.workflowHash, WorkflowBucketCount: cfg.workflowBuckets,
	})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := redisroutecontrol.NewKeyBuilder(tuple.Namespace())
	if err != nil {
		t.Fatal(err)
	}
	if server.Exists("{" + keys.HashTag() + "}:snapshot-bootstrap-fence") {
		t.Fatal("bootstrap-only unexpectedly persisted a coordinator fence")
	}

	var assigned bytes.Buffer
	if err := run(bootstrapTestArgs(challenge, "c1-"+challenge), &assigned); err != nil {
		t.Fatal(err)
	}
	var assignedOutput output
	if err := json.Unmarshal(assigned.Bytes(), &assignedOutput); err != nil {
		t.Fatal(err)
	}
	if assignedOutput.InitialRevision != 1 || assignedOutput.FinalRevision != 4 || len(assignedOutput.Claims) != 3 {
		t.Fatalf("assignment output=%+v", assignedOutput)
	}
	for _, claim := range assignedOutput.Claims {
		if claim.AssignmentRevision != 1 || claim.WriterEpoch != 1 || claim.LeaseEpoch != 1 || claim.LeaseUntilMs <= 0 {
			t.Fatalf("incomplete initial claim=%+v", claim)
		}
	}
	if claimTTL.String() != "1m0s" {
		t.Fatalf("claimTTL=%v want production maximum 60s", claimTTL)
	}
}

func TestBootstrapSupportsFourByFourDynamicLocalProfile(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv(redisURLEnv, "redis://"+server.Addr()+"/0")
	challenge := "cccccccccccccccccccccccccccccccc"
	// miniredis intentionally cannot satisfy the production transition store's
	// MEMORY USAGE SAMPLES 0 proof beyond five records. Exercise the complete
	// 4x4 topology/target construction here; the dynamic-local gate executes all
	// twelve Claims against real local Redis.
	args := append(bootstrapTestArgs(challenge, "c0-"+challenge), "--shard-count", "4", "--bootstrap-only")
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := initialSnapshot(cfg)
	if snapshot.Player.ShardCount != 4 || len(snapshot.Player.Routes) != 4 ||
		snapshot.Farm.ShardCount != 4 || len(snapshot.Farm.Routes) != 4 ||
		snapshot.FarmWorker.ShardCount != 4 || len(snapshot.FarmWorker.Routes) != 4 {
		t.Fatalf("four-by-four initial snapshot=%+v", snapshot)
	}
	targets := bootstrapTargets(cfg)
	if len(targets) != 12 {
		t.Fatalf("targets=%+v", targets)
	}
	for shard := uint32(0); shard < 4; shard++ {
		player, farm, worker := false, false, false
		for _, target := range targets {
			player = player || target.domain == "PLAYER" && target.shard == shard
			farm = farm || target.domain == "FARM" && target.shard == shard
			if target.domain == "FARM_WORKER" && target.shard == shard {
				worker = true
				want := cfg.workerA
				if shard%2 == 1 {
					want = cfg.workerB
				}
				if target.holder != want {
					t.Fatalf("worker shard=%d holder=%q want=%q", shard, target.holder, want)
				}
			}
		}
		if !player || !farm || !worker {
			t.Fatalf(
				"missing shard=%d player=%t farm=%t worker=%t targets=%+v",
				shard,
				player,
				farm,
				worker,
				targets,
			)
		}
	}
	var destination bytes.Buffer
	if err := run(args, &destination); err != nil {
		t.Fatal(err)
	}
	var got output
	if err := json.Unmarshal(destination.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.InitialRevision != 1 || got.FinalRevision != 1 || len(got.Claims) != 0 {
		t.Fatalf("four-by-four bootstrap-only output=%+v", got)
	}
}

func TestBootstrapSupportsFourPlayerAndFarmHolders(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv(redisURLEnv, "redis://"+server.Addr()+"/0")
	challenge := "dddddddddddddddddddddddddddddddd"
	args := append(bootstrapTestArgs(challenge, "c0-"+challenge),
		"--player-c", "player-c-"+challenge, "--player-c-boot", "player-c-boot-"+challenge, "--player-c-endpoint", "127.0.0.1:20007",
		"--player-c-pod-uid", "player-pod-c-"+challenge, "--player-c-workload-uid", "player-workload-"+challenge, "--player-c-ordinal", "2",
		"--player-c-control-endpoint", "127.0.0.1:21007", "--player-c-tls-identity", "player-holder.local",
		"--player-d", "player-d-"+challenge, "--player-d-boot", "player-d-boot-"+challenge, "--player-d-endpoint", "127.0.0.1:20008",
		"--player-d-pod-uid", "player-pod-d-"+challenge, "--player-d-workload-uid", "player-workload-"+challenge, "--player-d-ordinal", "3",
		"--player-d-control-endpoint", "127.0.0.1:21008", "--player-d-tls-identity", "player-holder.local",
		"--farm-c", "farm-c-"+challenge, "--farm-c-boot", "farm-c-boot-"+challenge, "--farm-c-endpoint", "127.0.0.1:20009",
		"--farm-c-pod-uid", "farm-pod-c-"+challenge, "--farm-c-workload-uid", "farm-workload-"+challenge, "--farm-c-ordinal", "2",
		"--farm-c-control-endpoint", "127.0.0.1:21009", "--farm-c-tls-identity", "farm-holder.local",
		"--farm-d", "farm-d-"+challenge, "--farm-d-boot", "farm-d-boot-"+challenge, "--farm-d-endpoint", "127.0.0.1:20010",
		"--farm-d-pod-uid", "farm-pod-d-"+challenge, "--farm-d-workload-uid", "farm-workload-"+challenge, "--farm-d-ordinal", "3",
		"--farm-d-control-endpoint", "127.0.0.1:21010", "--farm-d-tls-identity", "farm-holder.local",
		"--shard-count", "4", "--bootstrap-only")
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := initialSnapshot(cfg)
	if len(snapshot.Holders) != 10 {
		t.Fatalf("holders=%d want 10", len(snapshot.Holders))
	}
	playerHolders := map[int]bool{}
	farmHolders := map[int]bool{}
	for _, route := range snapshot.Player.Routes {
		playerHolders[route.HolderIndex] = true
	}
	for _, route := range snapshot.Farm.Routes {
		farmHolders[route.HolderIndex] = true
	}
	if len(playerHolders) != 4 || len(farmHolders) != 4 {
		t.Fatalf("player holders=%d farm holders=%d", len(playerHolders), len(farmHolders))
	}
	var destination bytes.Buffer
	if err := run(args, &destination); err != nil {
		t.Fatal(err)
	}
	var got output
	if err := json.Unmarshal(destination.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.InitialRevision != 1 || got.FinalRevision != 1 || len(got.Claims) != 0 {
		t.Fatalf("four-holder bootstrap-only output=%+v", got)
	}
}

func TestLiveProductionBootstrap(t *testing.T) {
	rawURL := os.Getenv("RT2Q_BOOTSTRAP_LIVE_URL")
	if rawURL == "" {
		t.Skip("RT2Q_BOOTSTRAP_LIVE_URL is not set")
	}
	t.Setenv(redisURLEnv, rawURL)
	challenge := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	args := bootstrapTestArgs(challenge, "c0-"+challenge)
	var output bytes.Buffer
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	if output.Len() == 0 {
		t.Fatal("bootstrap emitted no typed output")
	}
}

func bootstrapTestArgs(challenge, controllerBoot string) []string {
	return []string{
		"--challenge", challenge, "--realm", "rt2q-" + challenge,
		"--controller-id", "controller-" + challenge, "--controller-boot", controllerBoot,
		"--redis-environment", "rt2q" + challenge[:12], "--redis-zone", "z" + challenge[12:24],
		"--player-a", "player-a-" + challenge, "--player-a-boot", "player-a-boot-" + challenge, "--player-a-endpoint", "127.0.0.1:20001",
		"--player-a-pod-uid", "player-pod-a-" + challenge, "--player-a-workload-uid", "player-workload-" + challenge, "--player-a-ordinal", "0",
		"--player-a-control-endpoint", "127.0.0.1:21001", "--player-a-tls-identity", "player-holder.local",
		"--player-b", "player-b-" + challenge, "--player-b-boot", "player-b-boot-" + challenge, "--player-b-endpoint", "127.0.0.1:20002",
		"--player-b-pod-uid", "player-pod-b-" + challenge, "--player-b-workload-uid", "player-workload-" + challenge, "--player-b-ordinal", "1",
		"--player-b-control-endpoint", "127.0.0.1:21002", "--player-b-tls-identity", "player-holder.local",
		"--farm-a", "farm-a-" + challenge, "--farm-a-boot", "farm-a-boot-" + challenge, "--farm-a-endpoint", "127.0.0.1:20003",
		"--farm-a-pod-uid", "farm-pod-a-" + challenge, "--farm-a-workload-uid", "farm-workload-" + challenge, "--farm-a-ordinal", "0",
		"--farm-a-control-endpoint", "127.0.0.1:21003", "--farm-a-tls-identity", "farm-holder.local",
		"--farm-b", "farm-b-" + challenge, "--farm-b-boot", "farm-b-boot-" + challenge, "--farm-b-endpoint", "127.0.0.1:20004",
		"--farm-b-pod-uid", "farm-pod-b-" + challenge, "--farm-b-workload-uid", "farm-workload-" + challenge, "--farm-b-ordinal", "1",
		"--farm-b-control-endpoint", "127.0.0.1:21004", "--farm-b-tls-identity", "farm-holder.local",
		"--farm-worker-a", "farm-worker-a-" + challenge, "--farm-worker-a-boot", "farm-worker-a-boot-" + challenge, "--farm-worker-a-endpoint", "127.0.0.1:20005",
		"--farm-worker-b", "farm-worker-b-" + challenge, "--farm-worker-b-boot", "farm-worker-b-boot-" + challenge, "--farm-worker-b-endpoint", "127.0.0.1:20006",
		"--shard-count", "1",
	}
}
