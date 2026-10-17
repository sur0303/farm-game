// Command po1-plant-e2e exercises the PO1 Plant path with the real Redis,
// player-svr, farm-svr, and farm-worker processes.  The only process supplied
// by this harness is a network downlink sink for the existing GatewayDownlink
// contract; it does not implement an Actor or a workflow participant.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/farm/domain"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/plantonline"
	harvestscenario "farmgame/internal/scenario/harvest"
	plantscenario "farmgame/internal/scenario/plant"
	theftscenario "farmgame/internal/scenario/theft"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/theftonline"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	e2eRealm       = "po1-e2e"
	e2eProfile     = "po1-e2e-v1"
	e2eZone        = "local"
	e2eHashSeed    = uint64(202608050001)
	e2eLeaseTTL    = 55 * time.Second
	e2eLeaseSafety = 3 * time.Second
	e2eLeaseRenew  = 5 * time.Second
)

type captureServer struct {
	logicrpcpb.UnimplementedGatewayDownlinkServiceServer

	mu          sync.Mutex
	items       []*logicrpcpb.DownlinkItem
	activations []*logicrpcpb.ActivateSubscriptionRequest
}

func (s *captureServer) DeliverBatch(_ context.Context, req *logicrpcpb.DeliverBatchRequest) (*logicrpcpb.DeliverBatchResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range req.GetItems() {
		if item == nil {
			continue
		}
		clone := proto.Clone(item).(*logicrpcpb.DownlinkItem)
		s.items = append(s.items, clone)
		response.Results = append(response.Results, &logicrpcpb.DeliverBatchItemResult{
			ItemId: item.GetItemId(), Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return response, nil
}

func (s *captureServer) ActivateSubscription(_ context.Context, req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req != nil {
		s.activations = append(s.activations, proto.Clone(req).(*logicrpcpb.ActivateSubscriptionRequest))
	}
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.GetReadyThroughVersion(),
	}, nil
}

func (s *captureServer) snapshotReady(playerID uint64, subscriptionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	activated := false
	for _, request := range s.activations {
		if request.GetSubscription().GetSubscriptionId() == subscriptionID &&
			request.GetSubscription().GetFarmId() != 0 &&
			request.GetTarget().GetTargetPlayerId() == playerID {
			activated = true
			break
		}
	}
	if !activated {
		return false
	}
	for _, item := range s.items {
		if item.GetTargetFence().GetTargetPlayerId() != playerID || item.GetSubscriptionId() != subscriptionID {
			continue
		}
		frame := &commonpb.FarmFrame{}
		if proto.Unmarshal(item.GetFrame(), frame) == nil && frame.GetServerPush().GetPushType() == commonpb.PushType_FARM_SNAPSHOT {
			return true
		}
	}
	return false
}

type changeEvent struct {
	playerID     uint64
	subscription string
	change       *farmpb.FarmChange
	encoded      []byte
}

func (s *captureServer) changes() []changeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []changeEvent
	for _, item := range s.items {
		frame := &commonpb.FarmFrame{}
		if proto.Unmarshal(item.GetFrame(), frame) != nil {
			continue
		}
		push := frame.GetServerPush()
		if push == nil || push.GetPushType() != commonpb.PushType_FARM_CHANGE {
			continue
		}
		payload := &farmpb.FarmChangePush{}
		if proto.Unmarshal(push.GetPayload(), payload) != nil || payload.GetChange() == nil {
			continue
		}
		context := push.GetFarmSubscription()
		encoded, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(payload.GetChange())
		result = append(result, changeEvent{
			playerID: context.GetTargetPlayerId(), subscription: context.GetSubscriptionId(),
			change: proto.Clone(payload.GetChange()).(*farmpb.FarmChange), encoded: encoded,
		})
	}
	return result
}

type joinInfo struct {
	playerID     uint64
	subscription string
	attemptID    string
	connectionID string
}

type e2e struct {
	root   string
	runID  string
	temp   string
	owner  uint64
	viewer uint64
	// friends are CARE racers (includes viewer). Multi-friend same-Incident
	// concurrency is the product proof; each friend has its own Pair.
	friends   []uint64
	farmID    uint64
	namespace string

	redisURL       string
	redisAddr      string
	redis          *redis.Client
	redisProcess   *exec.Cmd
	dockerName     string
	externalRedis  bool
	workflowPrefix string
	guardPrefix    string

	profile    rediskey.PhysicalProfile
	calculator *actorrouting.Calculator
	resolver   *actorrouting.StaticResolver
	placement  workidentity.BucketResolver
	repository *workflow.RedisWorkRepository
	authority  *redisauthority.Store
	farmClaim  authoritystore.ShardAuthorityTransitionResult

	routePath    string
	playerAddr   string
	farmAddr     string
	playerHealth string
	farmHealth   string
	workerHealth string
	playerBootID string
	farmBootID   string

	playerBinary string
	farmBinary   string
	workerBinary string
	gateBinary   string
	playerCmd    *exec.Cmd
	farmCmd      *exec.Cmd
	workerCmd    *exec.Cmd
	gateCmd      *exec.Cmd

	gateHTTPAddr     string
	gateDownlinkAddr string
	gateControlAddr  string

	capture       *captureServer
	captureServer *grpc.Server
	captureLn     net.Listener

	playerConn        *grpc.ClientConn
	farmConn          *grpc.ClientConn
	playerLogic       logicrpcpb.LogicServiceClient
	playerBootstrap   logicrpcpb.BootstrapServiceClient
	playerParticipant logicrpcpb.PlayerParticipantServiceClient
	playerRecovery    contractspb.WorkerRecoveryIngressClient
	farmLogic         logicrpcpb.LogicServiceClient
	farmParticipant   contractspb.CoordinatorParticipantServiceClient
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	root, err := os.Getwd()
	if err != nil {
		log.Fatalf("PO1 E2E cannot determine worktree: %v", err)
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	seed := uint64(time.Now().UnixNano()) % 100000000000
	owner := 900000000000000 + seed
	viewer := 900000000000001 + seed
	friends := []uint64{viewer}
	for index := 1; index < 8; index++ {
		friends = append(friends, viewer+uint64(index))
	}
	e := &e2e{
		root: root, runID: runID,
		owner: owner, viewer: viewer, friends: friends,
		workflowPrefix: "po1-e2e-" + runID + ":workflow",
		guardPrefix:    "po1-e2e-" + runID + ":guard",
		playerBootID:   "po1-player-" + runID,
		farmBootID:     "po1-farm-" + runID,
	}
	e.farmID = e.owner
	defer e.close()
	if err := e.run(); err != nil {
		log.Fatalf("PO1 E2E FAIL: %v", err)
	}
	switch {
	case e.isSourceOrderWSSMode():
		log.Printf("SOURCE_ORDER_WSS E2E PASS: real farm-gate WSS -> mail/task/shop/pet/compendium + PLANT/HARVEST/CARE/THEFT SourceOrder COMPLETED and DueIndex empty")
	case e.isCareMode():
		log.Printf("CARE E2E PASS: real Redis -> Pair ActionAdmission -> multi-friend concurrent CARE same Incident (1 SUCCEEDED) -> CareRewardOrder COMPLETED")
	case e.isTheftMode():
		log.Printf("TH3 THEFT E2E PASS: real Redis -> Pair ActionAdmission -> FRIEND_PAIR workflow -> Farm/Player receipts -> recovery/replay/rejection checks")
	case e.isHarvestMode():
		log.Printf("HV4 HARVEST E2E PASS: real Redis -> Farm commit -> coordinator/process interruption -> PROCESSING result -> same WorkRecord worker recovery -> exactly-once state/change/replay/fence checks")
	default:
		log.Printf("PO1 E2E PASS: durable root -> reserve -> farm commit -> one change per subscriber -> WR2 recovery -> result read -> replay/fence checks")
	}
}

func (e *e2e) run() error {
	if err := e.setup(); err != nil {
		return err
	}
	switch {
	case e.isSourceOrderWSSMode():
		return e.sourceOrderWSSExercise()
	case e.isCareMode():
		return e.careExercise()
	case e.isTheftMode():
		return e.theftExercise()
	case e.isHarvestMode():
		return e.harvestExercise()
	default:
		return e.exercise()
	}
}

func (e *e2e) isHarvestMode() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PO1_E2E_MODE")), "HARVEST")
}

func (e *e2e) isTheftMode() bool {
	mode := strings.ToUpper(strings.TrimSpace(os.Getenv("PO1_E2E_MODE")))
	return mode == "THEFT" || mode == "THEFT_PET"
}

func (e *e2e) isTheftPetMode() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PO1_E2E_MODE")), "THEFT_PET")
}

func (e *e2e) isCareMode() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PO1_E2E_MODE")), "CARE")
}

func (e *e2e) isSourceOrderWSSMode() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PO1_E2E_MODE")), "SOURCE_ORDER_WSS")
}

func (e *e2e) setup() error {
	var err error
	e.temp, err = os.MkdirTemp("", "po1-final-online-plant-")
	if err != nil {
		return fmt.Errorf("create E2E temp directory: %w", err)
	}
	// Isolate authority keys on shared company Redis; local default stays short.
	e.namespace = "po1e2e"
	if strings.TrimSpace(os.Getenv("FARM_REDIS_URL")) != "" {
		e.namespace = "c" + strings.ToLower(e.runID)
		if len(e.namespace) > 32 {
			e.namespace = e.namespace[:32]
		}
	}
	e.playerAddr, err = freeTCPAddress()
	if err != nil {
		return err
	}
	e.farmAddr, err = freeTCPAddress()
	if err != nil {
		return err
	}
	e.playerHealth, err = freeTCPAddress()
	if err != nil {
		return err
	}
	e.farmHealth, err = freeTCPAddress()
	if err != nil {
		return err
	}
	e.workerHealth, err = freeTCPAddress()
	if err != nil {
		return err
	}
	if e.isSourceOrderWSSMode() {
		e.gateHTTPAddr, err = freeTCPAddress()
		if err != nil {
			return err
		}
		e.gateDownlinkAddr, err = freeTCPAddress()
		if err != nil {
			return err
		}
		e.gateControlAddr, err = freeTCPAddress()
		if err != nil {
			return err
		}
	}
	if err := e.startRedis(); err != nil {
		return err
	}
	if err := e.prepareAuthority(); err != nil {
		return err
	}
	if err := e.buildBinaries(); err != nil {
		return err
	}
	if !e.isSourceOrderWSSMode() {
		if err := e.startCapture(); err != nil {
			return err
		}
	}
	if err := e.startPlayer(); err != nil {
		return err
	}
	if !e.isSourceOrderWSSMode() {
		if err := e.bootstrapPlayers(); err != nil {
			return err
		}
		if err := e.seedFriendship(); err != nil {
			return err
		}
		if e.isTheftPetMode() {
			if err := e.seedGuardSnapshot(); err != nil {
				return err
			}
		}
	}
	if err := e.startFarm(); err != nil {
		return err
	}
	if e.isSourceOrderWSSMode() {
		if err := e.startGate(); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (e *e2e) seedGuardSnapshot() error {
	store, err := theftonline.NewRedisGuardSnapshotStore(e.redis, e.guardPrefix)
	if err != nil {
		return fmt.Errorf("build GuardSync snapshot store: %w", err)
	}
	snapshot := domain.GuardSnapshot{
		SchemaVersion: 1, GuardVersion: 1, ActiveGuardPetID: 7001,
		GuardUntilMs: time.Now().Add(2 * time.Hour).UnixMilli(),
		Rule: domain.GuardRule{PetConfigVersion: 1, GuardRuleVersion: 1,
			TriggerProbabilityBP: 10000, InterceptPolicy: domain.InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1},
	}
	snapshot.SnapshotHash = domain.CanonicalGuardSnapshotHash(snapshot)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Put(ctx, e.farmID, snapshot); err != nil {
		return fmt.Errorf("seed GuardSync snapshot: %w", err)
	}
	log.Printf("TH3 pet evidence: persisted full-intercept GuardSync snapshot farm=%d version=%d", e.farmID, snapshot.GuardVersion)
	return nil
}

func (e *e2e) startRedis() error {
	if external := strings.TrimSpace(os.Getenv("FARM_REDIS_URL")); external != "" {
		options, err := redis.ParseURL(external)
		if err != nil {
			return fmt.Errorf("parse FARM_REDIS_URL: %w", err)
		}
		e.externalRedis = true
		e.redisURL = external
		e.redisAddr = options.Addr
		e.redis = redis.NewClient(options)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = e.redis.Ping(ctx).Err()
		cancel()
		if err != nil {
			return fmt.Errorf("ping company Redis at %s: %w", e.redisAddr, err)
		}
		log.Printf("PO1 E2E using external Redis at %s (namespace=%s)", e.redisAddr, e.namespace)
		return nil
	}
	port, err := freeTCPPort()
	if err != nil {
		return err
	}
	e.redisAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	e.redisURL = "redis://" + e.redisAddr + "/0"
	if binary, lookErr := exec.LookPath("redis-server"); lookErr == nil {
		e.redisProcess = exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
		e.redisProcess.Stdout = io.Discard
		e.redisProcess.Stderr = io.Discard
		if err := e.redisProcess.Start(); err != nil {
			return fmt.Errorf("start local redis-server: %w", err)
		}
	} else {
		docker, dockerErr := exec.LookPath("docker")
		if dockerErr != nil {
			return fmt.Errorf("real Redis platform adapter unavailable: redis-server is not installed and docker is unavailable: %w", dockerErr)
		}
		image := "redis:7.4-alpine"
		inspect := exec.Command(docker, "image", "inspect", image)
		if inspect.Run() != nil {
			image = "redis:7"
			inspect = exec.Command(docker, "image", "inspect", image)
			if inspect.Run() != nil {
				return fmt.Errorf("real Redis platform adapter unavailable: no local redis-server and no local Docker image redis:7.4-alpine or redis:7")
			}
		}
		e.dockerName = "po1-final-redis-" + e.runID
		e.redisProcess = exec.Command(docker, "run", "--rm", "--name", e.dockerName,
			"-p", "127.0.0.1:"+strconv.Itoa(port)+":6379", image)
		e.redisProcess.Stdout = os.Stdout
		e.redisProcess.Stderr = os.Stderr
		if err := e.redisProcess.Start(); err != nil {
			return fmt.Errorf("start Docker Redis: %w", err)
		}
	}
	e.redis = redis.NewClient(&redis.Options{Addr: e.redisAddr})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := e.redis.Ping(ctx).Err()
		cancel()
		if err == nil {
			log.Printf("PO1 E2E Redis ready at %s", e.redisAddr)
			return nil
		}
		if e.redisProcess != nil && e.redisProcess.ProcessState != nil {
			return fmt.Errorf("Redis process exited before readiness: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("real Redis did not become ready within 20s")
}

func (e *e2e) prepareAuthority() error {
	namespace, err := rediskey.NewNamespace(e.namespace, e2eZone)
	if err != nil {
		return fmt.Errorf("build Redis namespace: %w", err)
	}
	e.profile = rediskey.PhysicalProfile{
		ProfileVersion: e2eProfile, Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	calculator, err := actorrouting.NewCalculator(e2eRealm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: e2eHashSeed,
	}, 1, 1)
	if err != nil {
		return fmt.Errorf("build route calculator: %w", err)
	}
	e.calculator = calculator
	scripts, err := redisauthority.NewScriptRegistry(e.redis)
	if err != nil {
		return fmt.Errorf("build authority scripts: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := scripts.EnsureLoaded(ctx); err != nil {
		return fmt.Errorf("load authority scripts: %w", err)
	}
	e.authority, err = redisauthority.NewStore(e.redis, scripts, redisauthority.Options{
		Profile: e.profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		return fmt.Errorf("build authority store: %w", err)
	}
	playerClaim, err := e.claimShard(ctx, actorrouting.DomainPlayer, "po1-player", e.playerBootID)
	if err != nil {
		return err
	}
	farmClaim, err := e.claimShard(ctx, actorrouting.DomainFarm, "po1-farm", e.farmBootID)
	if err != nil {
		return err
	}
	e.farmClaim = farmClaim
	leaseUntil := time.Now().Add(e2eLeaseTTL).UnixMilli()
	if playerClaim.Authority.LeaseUntilMs < leaseUntil {
		leaseUntil = playerClaim.Authority.LeaseUntilMs
	}
	if farmClaim.Authority.LeaseUntilMs < leaseUntil {
		leaseUntil = farmClaim.Authority.LeaseUntilMs
	}
	e.routePath = filepath.Join(e.temp, "route-snapshot.json")
	snapshot := actorrouting.RouteSnapshot{
		Realm: e2eRealm, GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: e2eHashSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "po1-player", BootID: e.playerBootID, Endpoint: e.playerAddr, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "po1-farm", BootID: e.farmBootID, Endpoint: e.farmAddr, State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive,
			ShardRevision:       playerClaim.Authority.AssignmentRevision,
			ExpectedWriterEpoch: playerClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch:  playerClaim.Authority.LeaseEpoch,
			LeaseUntilMs:        leaseUntil,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive,
			ShardRevision:       farmClaim.Authority.AssignmentRevision,
			ExpectedWriterEpoch: farmClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch:  farmClaim.Authority.LeaseEpoch,
			LeaseUntilMs:        leaseUntil,
		}}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal route snapshot: %w", err)
	}
	if err := os.WriteFile(e.routePath, data, 0600); err != nil {
		return fmt.Errorf("write route snapshot: %w", err)
	}
	e.resolver, err = actorrouting.LoadStaticResolver(e.routePath)
	if err != nil {
		return fmt.Errorf("load E2E route snapshot: %w", err)
	}
	e.placement, err = plantonline.NewPlacementResolver(snapshot, plantonline.PlacementConfig{
		ProfileVersion: e2eProfile, NamespaceEnvironment: e.namespace, NamespaceZone: e2eZone,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	})
	if err != nil {
		return fmt.Errorf("build W31 placement: %w", err)
	}
	e.repository, err = workflow.NewRedisWorkRepository(e.redis, e.workflowPrefix)
	if err != nil {
		return fmt.Errorf("build generic WorkRepository: %w", err)
	}
	return nil
}

func (e *e2e) claimShard(ctx context.Context, domain actorrouting.RouteDomain, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, error) {
	result, err := e.authority.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "po1-e2e-route-manager", BootID: "po1-e2e-route-manager-" + e.runID,
		},
		TransitionID:           "claim-" + string(domain) + "-" + e.runID,
		AuthorityLayoutVersion: "v1", Domain: domain, VirtualShard: 0,
		NewHolderID: holder, NewHolderBootID: boot,
		RequestedTTLms: e2eLeaseTTL.Milliseconds(),
	})
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("claim %s shard: %w", domain, err)
	}
	if result.Status != authoritystore.ShardTransitionApplied {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("claim %s shard status=%s", domain, result.Status)
	}
	return result, nil
}

func (e *e2e) buildBinaries() error {
	e.playerBinary = filepath.Join(e.temp, "player-svr")
	e.farmBinary = filepath.Join(e.temp, "farm-svr")
	e.workerBinary = filepath.Join(e.temp, "farm-worker")
	targets := []struct{ output, pkg string }{
		{e.playerBinary, "./cmd/player-svr"},
		{e.farmBinary, "./cmd/farm-svr"},
		{e.workerBinary, "./cmd/farm-worker"},
	}
	if e.isSourceOrderWSSMode() {
		e.gateBinary = filepath.Join(e.temp, "farm-gate")
		targets = append(targets, struct{ output, pkg string }{e.gateBinary, "./cmd/farm-gate"})
	}
	for _, item := range targets {
		command := exec.Command("go", "build", "-buildvcs=false", "-o", item.output, item.pkg)
		command.Dir = e.root
		output, runErr := command.CombinedOutput()
		if runErr != nil {
			return fmt.Errorf("build %s: %w\n%s", item.pkg, runErr, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func (e *e2e) startCapture() error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen downlink capture: %w", err)
	}
	e.captureLn = listener
	e.capture = &captureServer{}
	e.captureServer = grpc.NewServer()
	logicrpcpb.RegisterGatewayDownlinkServiceServer(e.captureServer, e.capture)
	go func() {
		if serveErr := e.captureServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.Printf("downlink capture stopped: %v", serveErr)
		}
	}()
	return nil
}

func (e *e2e) startPlayer() error {
	e.playerCmd = e.startProcess(e.playerBinary, e.playerEnvironment())
	if err := waitReady(e.playerHealth); err != nil {
		return fmt.Errorf("player-svr readiness: %w", err)
	}
	return e.dialPlayer()
}

func (e *e2e) dialPlayer() error {
	if e.playerConn != nil {
		_ = e.playerConn.Close()
	}
	connection, err := dialGRPC(e.playerAddr)
	if err != nil {
		return fmt.Errorf("dial player-svr: %w", err)
	}
	e.playerConn = connection
	e.playerLogic = logicrpcpb.NewLogicServiceClient(connection)
	e.playerBootstrap = logicrpcpb.NewBootstrapServiceClient(connection)
	e.playerParticipant = logicrpcpb.NewPlayerParticipantServiceClient(connection)
	e.playerRecovery = contractspb.NewWorkerRecoveryIngressClient(connection)
	return nil
}

func (e *e2e) bootstrapPlayers() error {
	playerIDs := []uint64{e.owner, e.viewer, e.owner + 77}
	if e.isCareMode() {
		playerIDs = append([]uint64{e.owner}, e.friends...)
	}
	seen := make(map[uint64]struct{}, len(playerIDs))
	for _, playerID := range playerIDs {
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err := e.playerBootstrap.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: playerID, BootstrapId: "bootstrap-" + e.runID + "-" + strconv.FormatUint(playerID, 10),
		})
		cancel()
		if err != nil || response == nil || response.GetOwner() == nil {
			return fmt.Errorf("bootstrap player %d: %v", playerID, err)
		}
	}
	return nil
}

func (e *e2e) seedFriendship() error {
	if !e.isCareMode() && !e.isTheftMode() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	friends := e.friends
	if len(friends) == 0 {
		friends = []uint64{e.viewer}
	}
	// Authority CARE/THEFT paths seed the sole relationship authority used by
	// Pair-backed permits and ActionAdmission.
	namespace, err := rediskey.NewNamespace(e.namespace, e2eZone)
	if err != nil {
		return fmt.Errorf("friend Pair namespace: %w", err)
	}
	repos, err := friend.NewRedisFriendshipRepositoriesForNamespace(e.redis, namespace)
	if err != nil {
		return fmt.Errorf("friend Pair repositories: %w", err)
	}
	if err := repos.EnsureScriptsLoaded(ctx); err != nil {
		return fmt.Errorf("friend Pair scripts: %w", err)
	}
	scenario := "theft"
	if e.isCareMode() {
		scenario = "care"
	}
	for _, friendID := range friends {
		low, high := friend.CanonicalPair(e.owner, friendID)
		pairID, err := friend.RelationIDFor(low, high)
		if err != nil {
			return fmt.Errorf("friend Pair id owner/%d: %w", friendID, err)
		}
		tokenSuffix := strconv.FormatUint(friendID, 10)
		payloadHash := sha256.Sum256([]byte(scenario + "-e2e-activate-" + e.runID + "-" + tokenSuffix))
		outcome, err := repos.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
			InviterPlayerID: e.owner, RedeemerPlayerID: friendID,
			TargetRelationEpoch: 1, TokenID: scenario + "-e2e-token-" + e.runID + "-" + tokenSuffix,
			TokenGeneration: 1, OperationID: scenario + "-e2e-activate-" + e.runID + "-" + tokenSuffix,
			NowMs: time.Now().UnixMilli(),
		}, friend.CommitIdentity{
			WorkAddressID: pairID, ParticipantInvocationID: scenario + "-e2e-activate-" + e.runID + "-" + tokenSuffix,
			ParticipantStep: "ACTIVATE",
		}, payloadHash[:])
		if err != nil {
			return fmt.Errorf("ActivateFriendRelation owner/%d: %w", friendID, err)
		}
		if outcome.Status != friend.StatusSucceeded && outcome.Reason != friend.ReasonAlreadyFriends {
			return fmt.Errorf("ActivateFriendRelation owner/%d status=%v reason=%v", friendID, outcome.Status, outcome.Reason)
		}
		log.Printf("%s raw evidence: Pair Authority friendship activated pair=%s owner=%d friend=%d", strings.ToUpper(scenario), pairID, e.owner, friendID)
	}
	return nil
}

func (e *e2e) startFarm() error {
	e.farmCmd = e.startProcess(e.farmBinary, e.farmEnvironment())
	if err := waitReady(e.farmHealth); err != nil {
		return fmt.Errorf("farm-svr readiness: %w", err)
	}
	connection, err := dialGRPC(e.farmAddr)
	if err != nil {
		return fmt.Errorf("dial farm-svr: %w", err)
	}
	e.farmConn = connection
	e.farmLogic = logicrpcpb.NewLogicServiceClient(connection)
	e.farmParticipant = contractspb.NewCoordinatorParticipantServiceClient(connection)
	return nil
}

func (e *e2e) startWorker() error {
	e.workerCmd = e.startProcess(e.workerBinary, e.workerEnvironment())
	if e.isSourceOrderWSSMode() {
		// CARE/THEFT families stay not-ready without Pair fixtures; SourceOrder
		// recovery loops still run. /healthz proves the process is serving.
		if err := waitHealthz(e.workerHealth); err != nil {
			return fmt.Errorf("farm-worker liveness: %w", err)
		}
		return nil
	}
	if err := waitReady(e.workerHealth); err != nil {
		return fmt.Errorf("farm-worker readiness: %w", err)
	}
	return nil
}

func (e *e2e) startProcess(binary string, environment []string) *exec.Cmd {
	command := exec.Command(binary)
	command.Dir = e.root
	command.Env = environment
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		log.Fatalf("start %s: %v", binary, err)
	}
	return command
}

func (e *e2e) playerEnvironment() []string {
	values := map[string]string{
		"ACTOR_AUTHORITY_MODE":                 "authority",
		"ACTOR_ROUTE_SNAPSHOT":                 e.routePath,
		"ACTOR_HOLDER_POD":                     "po1-player",
		"ACTOR_HOLDER_BOOT_ID":                 e.playerBootID,
		"ACTOR_AUTHORITY_LEASE_TTL":            e2eLeaseTTL.String(),
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN":  e2eLeaseSafety.String(),
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": e2eLeaseRenew.String(),
		"REDIS_PHYSICAL_PROFILE_VERSION":       e2eProfile,
		"REDIS_NAMESPACE_ENVIRONMENT":          e.namespace,
		"REDIS_NAMESPACE_ZONE":                 e2eZone,
		"REDIS_AUTHORITY_LAYOUT_VERSION":       "v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION":        "v1",
		"REDIS_WORKFLOW_HASH_VERSION":          "1",
		"REDIS_WORKFLOW_BUCKET_COUNT":          "256",
		"ACTOR_ROUTE_WATCH_BUFFER":             "64",
		"FARM_REDIS_URL":                       e.redisURL,
		"WORKFLOW_STORE_PREFIX":                e.workflowPrefix,
		"PLAYER_LISTEN":                        e.playerAddr,
		"PLAYER_HEALTH_LISTEN":                 e.playerHealth,
	}
	e.applySourceOrderFlags(values)
	return e.environment(values)
}

func (e *e2e) farmEnvironment() []string {
	downlink := e.gateDownlinkAddr
	if !e.isSourceOrderWSSMode() {
		downlink = e.captureLn.Addr().String()
	}
	values := map[string]string{
		"ACTOR_AUTHORITY_MODE":                  "authority",
		"ACTOR_ROUTE_SNAPSHOT":                  e.routePath,
		"ACTOR_HOLDER_POD":                      "po1-farm",
		"ACTOR_HOLDER_BOOT_ID":                  e.farmBootID,
		"ACTOR_AUTHORITY_LEASE_TTL":             e2eLeaseTTL.String(),
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN":   e2eLeaseSafety.String(),
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL":  e2eLeaseRenew.String(),
		"REDIS_PHYSICAL_PROFILE_VERSION":        e2eProfile,
		"REDIS_NAMESPACE_ENVIRONMENT":           e.namespace,
		"REDIS_NAMESPACE_ZONE":                  e2eZone,
		"REDIS_AUTHORITY_LAYOUT_VERSION":        "v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION":         "v1",
		"REDIS_WORKFLOW_HASH_VERSION":           "1",
		"REDIS_WORKFLOW_BUCKET_COUNT":           "256",
		"ACTOR_ROUTE_WATCH_BUFFER":              "64",
		"FARM_REDIS_URL":                        e.redisURL,
		"THEFT_GUARD_SNAPSHOT_PREFIX":           e.guardPrefix,
		"THEFT_PRF_MODE":                        "test",
		"THEFT_PRF_HMAC_SECRET":                 "po1-e2e-theft-prf-secret-20260805-32-bytes",
		"PERMIT_SECRET":                         "po1-e2e-permit-secret-20260805-" + e.runID,
		"WORKFLOW_STORE_PREFIX":                 e.workflowPrefix,
		"PLAYER_ADDR":                           e.playerAddr,
		"FARM_LISTEN":                           e.farmAddr,
		"FARM_HEALTH_LISTEN":                    e.farmHealth,
		"GATEWAY_DOWNLINK_ADDR":                 downlink,
		"GATE_BOOT_REAPER_MODE":                 "disabled",
		"FARM_SUBSCRIPTION_LEASE_INITIAL_TTL":   "60s",
		"FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION": "60s",
		"DOWNLINK_RPC_TIMEOUT":                  "2s",
		"FARM_CROP_MATURE_AFTER":                e.cropMaturity(),
	}
	e.applySourceOrderFlags(values)
	return e.environment(values)
}

func (e *e2e) cropMaturity() string {
	if e.isSourceOrderWSSMode() {
		// Catalog carrot WaterOffset=3s; mature must stay after that window.
		return "10s"
	}
	if e.isTheftMode() {
		return "2s"
	}
	return "1h"
}

func (e *e2e) workerEnvironment() []string {
	mode := "legacy"
	if e.isCareMode() || e.isSourceOrderWSSMode() {
		mode = "authority"
	}
	values := map[string]string{
		"ACTOR_AUTHORITY_MODE":              mode,
		"ACTOR_ROUTE_SNAPSHOT":              e.routePath,
		"FARM_REDIS_URL":                    e.redisURL,
		"REDIS_PHYSICAL_PROFILE_VERSION":    e2eProfile,
		"REDIS_NAMESPACE_ENVIRONMENT":       e.namespace,
		"REDIS_NAMESPACE_ZONE":              e2eZone,
		"REDIS_AUTHORITY_LAYOUT_VERSION":    "v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION":     "v1",
		"REDIS_WORKFLOW_HASH_VERSION":       "1",
		"REDIS_WORKFLOW_BUCKET_COUNT":       "256",
		"WORKFLOW_STORE_PREFIX":             e.workflowPrefix,
		"PLAYER_ADDR":                       e.playerAddr,
		"FARM_ADDR":                         e.farmAddr,
		"WORKER_ID":                         "po1-e2e-worker",
		"WORKER_BOOT_ID":                    "po1-e2e-worker-" + e.runID,
		"WORKER_LEASE_TOKEN":                "po1-e2e-token-" + e.runID,
		"WORKER_POLL_INTERVAL":              "100ms",
		"WORKER_CLAIM_LIMIT":                "8",
		"WORKER_CLAIM_LEASE":                "5s",
		"WORKER_OPERATION_TIMEOUT":          "3s",
		"WORKER_PARTICIPANT_BUDGET_MS":      "5000",
		"CARE_WORKER_CLAIM_LIMIT":           "8",
		"CARE_WORKER_MAX_CONCURRENT":        "4",
		"CARE_WORKER_LEASE":                 "30s",
		"CARE_WORKER_LEASE_SAFETY_MARGIN":   "2s",
		"CARE_WORKER_OPERATION_TIMEOUT":     "5s",
		"CARE_WORKER_PARTICIPANT_BUDGET_MS": "5000",
		"WORKER_HEALTH_LISTEN":              e.workerHealth,
	}
	e.applySourceOrderFlags(values)
	return e.environment(values)
}

func (e *e2e) applySourceOrderFlags(values map[string]string) {
	if !e.isSourceOrderWSSMode() {
		return
	}
	values["PLANT_SOURCE_ORDER"] = "true"
	values["HARVEST_SOURCE_ORDER"] = "true"
}

func (e *e2e) environment(overrides map[string]string) []string {
	values := make(map[string]string)
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		if value == "" {
			delete(values, key)
		} else {
			values[key] = value
		}
	}
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	sort.Strings(result)
	return result
}

func (e *e2e) exercise() error {
	if err := e.buySeed(); err != nil {
		return err
	}
	ownerJoin, err := e.join(e.owner)
	if err != nil {
		return err
	}
	viewerJoin, err := e.join(e.viewer)
	if err != nil {
		return err
	}
	before, err := e.privateState(e.owner)
	if err != nil {
		return err
	}
	if itemCount(before, int32(cropcatalog.CarrotSeedItemID)) != 1 {
		return fmt.Errorf("seed setup count=%d, want 1", itemCount(before, int32(cropcatalog.CarrotSeedItemID)))
	}

	operationID := "po1-plant-" + e.runID
	input := plantscenario.PlantInput{
		SubjectPlayerID: e.owner, OperationID: operationID, TargetFarmID: e.farmID,
		PlotID: 1, ExpectedPlotVersion: 1, CropDefinitionID: uint32(cropcatalog.CarrotDefinitionID),
		ExpectedCropRuleVersion: 1, SeedItemID: uint32(cropcatalog.CarrotSeedItemID),
	}
	definition, ok := cropcatalog.ByDefinitionID(int32(cropcatalog.CarrotDefinitionID))
	if !ok {
		return errors.New("carrot definition is unavailable")
	}
	input.CropRuleSnapshot, err = cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		return fmt.Errorf("build carrot rule snapshot: %w", err)
	}
	root, err := plantscenario.BuildRoot(input)
	if err != nil {
		return fmt.Errorf("build expected Plant root: %w", err)
	}
	if _, err := root.AddressForResolver(1, e.placement); err != nil {
		return fmt.Errorf("expected root placement: %w", err)
	}
	plantPayload, err := marshal(&publicpb.PlantRequest{
		PlotId: 0, CropDefinitionId: uint32(cropcatalog.CarrotDefinitionID), TargetFarmId: e.farmID,
		ExpectedPlotVersion: 1, ExpectedCropRuleVersion: 1,
	})
	if err != nil {
		return err
	}
	plantCommand := &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + operationID, OperationId: operationID,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}

	if err := e.stopPlayer(); err != nil {
		return fmt.Errorf("inject one Player interruption: %w", err)
	}
	firstResult := make(chan struct {
		out *logicrpcpb.BusinessCommandOutcome
		err error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, executeErr := e.execute(e.farmLogic, ctx, plantCommand,
			e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
		firstResult <- struct {
			out *logicrpcpb.BusinessCommandOutcome
			err error
		}{out, executeErr}
	}()
	if _, err := e.waitWork(root.WorkflowID, func(record *workflow.WorkRecord) bool { return record != nil }); err != nil {
		return fmt.Errorf("wait durable WorkRecord before first effect: %w", err)
	}
	first := <-firstResult
	if first.err != nil {
		return fmt.Errorf("interrupted public PLANT call: %w", first.err)
	}
	if first.out == nil || first.out.GetStatus() != commonpb.CommandStatus_PROCESSING {
		return fmt.Errorf("interrupted PLANT status=%v, want PROCESSING", first.out.GetStatus())
	}
	interrupted, err := e.repository.Get(context.Background(), string(identity.WorkTypePlant), root.WorkflowID)
	if err != nil {
		return fmt.Errorf("read interrupted WorkRecord: %w", err)
	}
	if interrupted.Terminal() || interrupted.State != workflow.WorkStateStepUnknown || interrupted.Steps[0].Status != workflow.StepUnknown {
		return fmt.Errorf("interrupted WorkRecord state=%s step=%s, want STEP_UNKNOWN/UNKNOWN", interrupted.State, interrupted.Steps[0].Status)
	}
	log.Printf("PO1 E2E evidence: WorkRecord created before effect; interrupted PLANT returned PROCESSING at %s", interrupted.State)

	if err := e.startPlayer(); err != nil {
		return fmt.Errorf("restart Player after interruption: %w", err)
	}
	processing, err := e.queryOperation(operationID, e.farmID, e.owner, ownerJoin)
	if err != nil {
		return fmt.Errorf("GET_OPERATION_RESULT while WorkRecord processing: %w", err)
	}
	if processing.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING {
		return fmt.Errorf("GET_OPERATION_RESULT during recovery status=%s, want PROCESSING", processing.GetLookupStatus())
	}
	if err := e.startWorker(); err != nil {
		return fmt.Errorf("start WR2 farm-worker: %w", err)
	}
	terminal, err := e.waitWork(root.WorkflowID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
	if err != nil {
		return fmt.Errorf("wait WR2 terminal WorkRecord: %w", err)
	}
	if terminal.State != workflow.WorkStateCompleted {
		return fmt.Errorf("WR2 terminal state=%s, want COMPLETED", terminal.State)
	}
	if len(terminal.Steps) < 3 || terminal.Steps[0].Status != workflow.StepApplied || terminal.Steps[1].Status != workflow.StepApplied || terminal.Steps[2].Status != workflow.StepApplied {
		return fmt.Errorf("WR2 terminal step receipts=%v, want ReserveSeed/CommitPlant/CommitReservation all APPLIED", terminal.Steps)
	}

	completed, err := e.queryOperation(operationID, e.farmID, e.owner, ownerJoin)
	if err != nil {
		return fmt.Errorf("final GET_OPERATION_RESULT: %w", err)
	}
	if completed.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED || completed.GetCompletedOutcome() == nil || completed.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("final GET_OPERATION_RESULT=%s outcome=%v", completed.GetLookupStatus(), completed.GetCompletedOutcome())
	}
	plantResult := &publicpb.PlantResult{}
	if err := proto.Unmarshal(completed.GetCompletedOutcome().GetResultPayload(), plantResult); err != nil {
		return fmt.Errorf("decode final public PlantResult: %w", err)
	}
	if plantResult.GetTargetFarmId() != e.farmID || plantResult.GetPlotId() != 0 || plantResult.GetCropCycleId() != root.CropCycleID || plantResult.GetCommittedFarmVersion() == 0 {
		return fmt.Errorf("final PlantResult=%v does not match root", plantResult)
	}

	retryCommand := proto.Clone(plantCommand).(*commonpb.ClientCommandEnvelope)
	var retry *logicrpcpb.BusinessCommandOutcome
	var retryErr error
	for attempt := 0; attempt < 20; attempt++ {
		retryCommand.RequestId = "request-" + operationID + "-retry-" + strconv.Itoa(attempt)
		retry, retryErr = e.execute(e.farmLogic, context.Background(), retryCommand,
			e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
		if retryErr == nil && retry.GetStatus() == commonpb.CommandStatus_SUCCEEDED {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if retryErr != nil || retry.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("same-operation PLANT retry status=%v error=%v err=%v", retry.GetStatus(), retry.GetError(), retryErr)
	}
	retryFarm := &farmpb.FarmCommandResult{}
	if err := proto.Unmarshal(retry.GetResultPayload(), retryFarm); err != nil {
		return fmt.Errorf("decode same-operation FarmCommandResult: %w", err)
	}
	if err := assertOnePlantedPlot(retryFarm.GetSnapshot(), plantResult.GetCropCycleId()); err != nil {
		return fmt.Errorf("same-operation retry farm state: %w", err)
	}
	after, err := e.privateState(e.owner)
	if err != nil {
		return err
	}
	if itemCount(after, int32(cropcatalog.CarrotSeedItemID)) != 0 {
		return fmt.Errorf("same-operation retry seed count=%d, want 0", itemCount(after, int32(cropcatalog.CarrotSeedItemID)))
	}

	if err := e.assertSingleChange(ownerJoin, viewerJoin, plantResult.GetCommittedFarmVersion()); err != nil {
		return err
	}
	if err := e.assertStaleParticipantFailsClosed(terminal); err != nil {
		return err
	}
	log.Printf("PO1 E2E evidence: reserve/consume once, one crop, two equal FarmChange deliveries once, replay silent, stale route/lease rejected")
	return nil
}

// theftExercise is the TH3 vertical proof.  It intentionally keeps the
// existing process harness and swaps only the business exercise: public
// THEFT goes through Farm ingress/Pair ActionAdmission, while the durable
// FRIEND_PAIR dispatch and generic WorkRecord are recovered by farm-worker.
func (e *e2e) theftExercise() error {
	log.Printf("TH3 raw evidence: backend=real Redis (%s), processes=farm-svr/player-svr/farm-worker, external mock=GatewayDownlink capture only", e.redisAddr)
	if err := e.buySeedWithOperation("th3-seed-1-" + e.runID); err != nil {
		return err
	}
	ownerJoin, err := e.join(e.owner)
	if err != nil {
		return err
	}
	visitorJoin, err := e.join(e.viewer)
	if err != nil {
		return err
	}
	if err := e.startWorker(); err != nil {
		return fmt.Errorf("start TH3 worker: %w", err)
	}

	firstPlot, err := e.plantForTheft(ownerJoin, "th3-plant-1-"+e.runID, 0)
	if err != nil {
		return err
	}

	// Pair admission may succeed before the crop is mature; FarmActor is the
	// first business decision and must durably reject without consuming a
	// theft attempt.
	immatureOperation := "th3-immature-" + e.runID
	if _, err := e.executeTheft(visitorJoin, immatureOperation, firstPlot.GetCropCycleId(), 0); err != nil {
		return fmt.Errorf("immature THEFT admission: %w", err)
	}
	immatureID, err := e.theftWorkflowID(immatureOperation)
	if err != nil {
		return err
	}
	immature, err := e.waitTheftWork(immatureID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
	if err != nil {
		return fmt.Errorf("immature THEFT terminal record: %w", err)
	}
	if immature.State != workflow.WorkStateRejected {
		return fmt.Errorf("immature THEFT state=%s, want REJECTED", immature.State)
	}
	log.Printf("TH3 evidence: immature crop rejected durably without consuming a Farm theft attempt")

	if wait := time.Until(time.UnixMilli(firstPlot.GetMatureAtMs())); wait > 0 {
		time.Sleep(wait + 150*time.Millisecond)
	}
	var visitorCoinBefore, ownerCoinBefore int64
	if e.isTheftPetMode() {
		visitorState, stateErr := e.privateState(e.viewer)
		if stateErr != nil {
			return fmt.Errorf("read visitor wallet before pet THEFT: %w", stateErr)
		}
		ownerState, stateErr := e.privateState(e.owner)
		if stateErr != nil {
			return fmt.Errorf("read owner wallet before pet THEFT: %w", stateErr)
		}
		visitorCoinBefore, ownerCoinBefore = visitorState.GetCoin(), ownerState.GetCoin()
	}

	// The same mature cycle is raced by two different operation IDs.  Pair
	// admits both operations, but the FarmActor attempt key admits exactly one
	// visitor/cycle consumption.  A same-operation replay below must converge
	// on that same workflow and receipt.
	concurrentOps := []string{"th3-concurrent-a-" + e.runID, "th3-concurrent-b-" + e.runID}
	type theftCall struct {
		operation string
		out       *logicrpcpb.BusinessCommandOutcome
		err       error
	}
	calls := make(chan theftCall, len(concurrentOps))
	for _, operation := range concurrentOps {
		operation := operation
		go func() {
			out, callErr := e.executeTheft(visitorJoin, operation, firstPlot.GetCropCycleId(), 0)
			calls <- theftCall{operation: operation, out: out, err: callErr}
		}()
	}
	records := make(map[string]*workflow.WorkRecord, len(concurrentOps))
	for range concurrentOps {
		call := <-calls
		if call.err != nil || call.out == nil || call.out.GetStatus() != commonpb.CommandStatus_PROCESSING {
			var businessError *logicrpcpb.ErrorDetail
			if call.out != nil {
				businessError = call.out.GetError()
			}
			return fmt.Errorf("concurrent THEFT operation=%s status=%v error=%v transport=%v", call.operation, outcomeStatus(call.out), businessError, call.err)
		}
		workflowID, workflowErr := e.theftWorkflowID(call.operation)
		if workflowErr != nil {
			return workflowErr
		}
		record, waitErr := e.waitTheftWork(workflowID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
		if waitErr != nil {
			return fmt.Errorf("concurrent THEFT operation=%s: %w", call.operation, waitErr)
		}
		records[call.operation] = record
	}
	successes, rejected := 0, 0
	var successOperation string
	var successRecord *workflow.WorkRecord
	for operation, record := range records {
		switch {
		case record.State == workflow.WorkStateCompleted && theftscenario.OutcomeFor(record) == theftscenario.OutcomeSucceeded:
			successes++
			successOperation, successRecord = operation, record
		case record.State == workflow.WorkStateRejected:
			rejected++
		default:
			return fmt.Errorf("concurrent THEFT operation=%s ended in unexpected state=%s outcome=%s", operation, record.State, theftscenario.OutcomeFor(record))
		}
	}
	if successes != 1 || rejected != 1 {
		return fmt.Errorf("concurrent same-cycle THEFT successes=%d rejected=%d, want 1/1", successes, rejected)
	}
	decision, visitorReceipt, ownerReceipt, err := theftscenario.DecodeResults(successRecord)
	if err != nil {
		return fmt.Errorf("decode successful Theft receipts: %w", err)
	}
	targetDecision := decision.GetTheftTargetDecision()
	if targetDecision.GetVisitorPlayerId() != e.viewer || targetDecision.GetOwnerPlayerId() != e.owner || visitorReceipt == nil {
		return fmt.Errorf("Theft settlement identity invariant decision=%v visitor=%v owner=%v", decision, visitorReceipt, ownerReceipt)
	}
	if !e.isTheftPetMode() {
		if targetDecision.GetGuardLostQuantity() != 0 || targetDecision.GetGuardCoinValue() != 0 ||
			targetDecision.GetNetReceivedQuantity() != targetDecision.GetGrossStolenQuantity() ||
			visitorReceipt.GetGrantedNetQuantity() != targetDecision.GetNetReceivedQuantity() || ownerReceipt != nil {
			return fmt.Errorf("no-pet Theft settlement invariant decision=%v visitor=%v owner=%v", decision, visitorReceipt, ownerReceipt)
		}
	} else {
		if targetDecision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD ||
			targetDecision.GetGuardLostQuantity() != targetDecision.GetGrossStolenQuantity() ||
			targetDecision.GetNetReceivedQuantity() != 0 || targetDecision.GetGuardCoinValue() == 0 ||
			visitorReceipt.GetGrantedNetQuantity() != 0 || ownerReceipt == nil ||
			ownerReceipt.GetCreditedNormalCoin() != targetDecision.GetGuardCoinValue() {
			return fmt.Errorf("full-pet Theft settlement invariant decision=%v visitor=%v owner=%v", decision, visitorReceipt, ownerReceipt)
		}
		visitorState, stateErr := e.privateState(e.viewer)
		if stateErr != nil {
			return fmt.Errorf("read visitor wallet after pet THEFT: %w", stateErr)
		}
		ownerState, stateErr := e.privateState(e.owner)
		if stateErr != nil {
			return fmt.Errorf("read owner wallet after pet THEFT: %w", stateErr)
		}
		if visitorState.GetCoin() != visitorCoinBefore || ownerState.GetCoin()-ownerCoinBefore != int64(targetDecision.GetGuardCoinValue()) {
			return fmt.Errorf("wallet invariant visitor=%d/%d owner_delta=%d guard_coin=%d", visitorState.GetCoin(), visitorCoinBefore, ownerState.GetCoin()-ownerCoinBefore, targetDecision.GetGuardCoinValue())
		}
	}
	completed, err := e.queryOperationType(successOperation, commonpb.CommandType_THEFT, e.farmID, e.viewer)
	if err != nil {
		return fmt.Errorf("query successful THEFT: %w", err)
	}
	if completed.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED ||
		completed.GetCompletedOutcome() == nil || completed.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("successful THEFT query=%v", completed)
	}
	publicResult := &publicpb.TheftResult{}
	if err := proto.Unmarshal(completed.GetCompletedOutcome().GetResultPayload(), publicResult); err != nil {
		return fmt.Errorf("decode successful public TheftResult: %w", err)
	}
	if publicResult.GetTargetFarmId() != e.farmID || publicResult.GetPlotId() != 0 || publicResult.GetCropCycleId() != firstPlot.GetCropCycleId() || publicResult.GetSettlementId() == "" {
		return fmt.Errorf("public TheftResult=%v", publicResult)
	}

	// Replay uses a new request id but the same public operation identity.  It
	// must not consume another slot or publish another FarmChange.
	replay, err := e.executeTheft(visitorJoin, successOperation, firstPlot.GetCropCycleId(), 0)
	if err != nil || replay == nil || replay.GetStatus() != commonpb.CommandStatus_PROCESSING {
		return fmt.Errorf("same-operation THEFT replay status=%v err=%v", replay.GetStatus(), err)
	}
	replayed, err := e.waitTheftWork(successRecord.Address.WorkID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
	if err != nil || replayed.State != workflow.WorkStateCompleted || theftscenario.OutcomeFor(replayed) != theftscenario.OutcomeSucceeded {
		return fmt.Errorf("same-operation THEFT replay record=%v err=%v", replayed, err)
	}
	if e.isTheftPetMode() {
		log.Printf("TH3 evidence: mature full-pet THEFT intercepted the 2%% slot; visitor wallet unchanged and owner received ordinary guard coins")
	} else {
		log.Printf("TH3 evidence: mature no-pet THEFT consumed exactly one 2%% slot; concurrent/replay produced no duplicate reward")
	}

	// An unrelated visitor is rejected by the public friend/context fence before
	// any WorkRecord or Farm decision is created. A real Pair rejection is
	// covered by the admitted friend path above; this visitor cannot establish a
	// Farm subscription because JOIN_FARM itself is friend-gated.
	stranger := e.owner + 77
	strangerOperation := "th3-nonfriend-" + e.runID
	strangerOut, strangerErr := e.executeTheftForPlayer(stranger, strangerOperation, firstPlot.GetCropCycleId(), 0)
	if strangerErr == nil && (strangerOut == nil || strangerOut.GetStatus() != commonpb.CommandStatus_REJECTED) {
		var businessError *logicrpcpb.ErrorDetail
		if strangerOut != nil {
			businessError = strangerOut.GetError()
		}
		return fmt.Errorf("non-friend THEFT status=%v error=%v transport=%v", outcomeStatus(strangerOut), businessError, strangerErr)
	}
	if _, err := e.theftWorkflowID(strangerOperation); err != nil {
		return err
	}
	log.Printf("TH3 evidence: non-friend THEFT rejected before any WorkRecord (transport=%v outcome=%v)", strangerErr, outcomeStatus(strangerOut))

	// Prepare a second mature crop for process/reply-loss recovery.  The
	// public request is issued while all processes are alive; after the Pair
	// order and generic WorkRecord are durable, the client response is
	// deliberately discarded and Farm/Player/Worker are restarted.
	if err := e.buySeedWithOperation("th3-seed-2-" + e.runID); err != nil {
		return err
	}
	secondPlot, err := e.plantForTheft(ownerJoin, "th3-plant-2-"+e.runID, 1)
	if err != nil {
		return err
	}
	if wait := time.Until(time.UnixMilli(secondPlot.GetMatureAtMs())); wait > 0 {
		time.Sleep(wait + 150*time.Millisecond)
	}
	recoveryOperation := "th3-recovery-" + e.runID
	recoveryID, err := e.theftWorkflowID(recoveryOperation)
	if err != nil {
		return err
	}
	responseCh := make(chan theftCall, 1)
	go func() {
		out, callErr := e.executeTheft(visitorJoin, recoveryOperation, secondPlot.GetCropCycleId(), 1)
		responseCh <- theftCall{operation: recoveryOperation, out: out, err: callErr}
	}()
	if _, err := e.waitTheftWork(recoveryID, func(record *workflow.WorkRecord) bool { return record != nil }); err != nil {
		return fmt.Errorf("response-loss WorkRecord creation: %w", err)
	}
	// Closing the client/server/process boundary here is the response-loss
	// injection.  The durable Pair order and WorkRecord are the only recovery
	// inputs retained by the restarted worker.
	if err := e.stopFarm(); err != nil {
		return fmt.Errorf("stop Farm for response-loss recovery: %w", err)
	}
	if err := e.stopPlayer(); err != nil {
		return fmt.Errorf("stop Player for response-loss recovery: %w", err)
	}
	if err := e.stopWorker(); err != nil {
		return fmt.Errorf("stop Worker for response-loss recovery: %w", err)
	}
	lostResponse := <-responseCh
	log.Printf("TH3 evidence: response-loss injection discarded public THEFT response status=%v err=%v", lostResponse.out.GetStatus(), lostResponse.err)
	if err := e.startPlayer(); err != nil {
		return fmt.Errorf("restart Player for THEFT recovery: %w", err)
	}
	if _, err := e.queryOperationType(recoveryOperation, commonpb.CommandType_THEFT, e.farmID, e.viewer); err != nil {
		return fmt.Errorf("query THEFT after Player restart: %w", err)
	}
	if err := e.startFarm(); err != nil {
		return fmt.Errorf("restart Farm for THEFT recovery: %w", err)
	}
	if err := e.startWorker(); err != nil {
		return fmt.Errorf("restart Worker for THEFT recovery: %w", err)
	}
	recovered, err := e.waitTheftWork(recoveryID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
	if err != nil {
		return fmt.Errorf("wait recovered THEFT: %w", err)
	}
	if recovered.State != workflow.WorkStateCompleted || theftscenario.OutcomeFor(recovered) != theftscenario.OutcomeSucceeded {
		return fmt.Errorf("recovered THEFT record=%v", recovered)
	}
	if _, _, _, err := theftscenario.DecodeResults(recovered); err != nil {
		return fmt.Errorf("recovered THEFT receipts: %w", err)
	}
	finalRecovery, err := e.queryOperationType(recoveryOperation, commonpb.CommandType_THEFT, e.farmID, e.viewer)
	if err != nil || finalRecovery.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED || finalRecovery.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("final recovered THEFT result=%v err=%v", finalRecovery, err)
	}
	log.Printf("TH3 evidence: Farm/Player/Worker restart plus lost response resumed the same FRIEND_PAIR workflow and completed all receipts")
	return nil
}

func (e *e2e) plantForTheft(ownerJoin joinInfo, operationID string, publicPlot uint32) (*farmpb.PlotView, error) {
	payload, err := marshal(&publicpb.PlantRequest{
		PlotId: publicPlot, CropDefinitionId: uint32(cropcatalog.CarrotDefinitionID), TargetFarmId: e.farmID,
		ExpectedPlotVersion: 1, ExpectedCropRuleVersion: 1,
	})
	if err != nil {
		return nil, err
	}
	out, err := e.execute(e.farmLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + operationID, OperationId: operationID,
		CommandType: commonpb.CommandType_PLANT, Payload: payload,
	}, e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
	var completedOutcome *publicpb.StoredOperationOutcome
	if err == nil && out != nil && out.GetStatus() == commonpb.CommandStatus_PROCESSING {
		// The shared PLANT recovery worker may atomically claim the durable work
		// between creation and the online fast path. PROCESSING is therefore a
		// valid public result; wait through the public OperationResult path instead of
		// treating this scheduling race as an E2E failure.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			result, queryErr := e.queryOperationType(operationID, commonpb.CommandType_PLANT, e.farmID, e.owner)
			if queryErr != nil {
				return nil, fmt.Errorf("query prepare Theft PLANT plot=%d: %w", publicPlot, queryErr)
			}
			switch result.GetLookupStatus() {
			case publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED:
				completedOutcome = result.GetCompletedOutcome()
				if completedOutcome == nil {
					return nil, fmt.Errorf("prepare Theft PLANT plot=%d completed without outcome", publicPlot)
				}
				goto plantCompleted
			case publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING:
				time.Sleep(100 * time.Millisecond)
			default:
				return nil, fmt.Errorf("prepare Theft PLANT plot=%d lookup=%s", publicPlot, result.GetLookupStatus())
			}
		}
		return nil, fmt.Errorf("prepare Theft PLANT plot=%d remained PROCESSING", publicPlot)
	}

plantCompleted:
	var resultPayload []byte
	if completedOutcome != nil {
		if completedOutcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			return nil, fmt.Errorf("prepare Theft PLANT plot=%d completed status=%v error=%v", publicPlot, completedOutcome.GetStatus(), completedOutcome.GetError())
		}
		resultPayload = completedOutcome.GetResultPayload()
	} else {
		if err != nil || out == nil || out.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			return nil, fmt.Errorf("prepare Theft PLANT plot=%d status=%v error=%v transport=%v", publicPlot, out.GetStatus(), out.GetError(), err)
		}
		resultPayload = out.GetResultPayload()
	}
	result := &farmpb.FarmCommandResult{}
	if err := proto.Unmarshal(resultPayload, result); err != nil {
		return nil, fmt.Errorf("decode prepare Theft PLANT: %w", err)
	}
	plot := findPlot(result.GetSnapshot(), publicPlot)
	if plot == nil || plot.GetCropCycleId() == "" || plot.GetMatureAtMs() == 0 || plot.GetPlotVersion() == 0 {
		return nil, fmt.Errorf("prepare Theft PLANT did not return active plot=%d: %v", publicPlot, result.GetSnapshot())
	}
	return plot, nil
}

func (e *e2e) executeTheft(join joinInfo, operationID, cycleID string, publicPlot uint32) (*logicrpcpb.BusinessCommandOutcome, error) {
	return e.executeTheftFor(join.playerID, &join, operationID, cycleID, publicPlot)
}

func (e *e2e) executeTheftForPlayer(playerID uint64, operationID, cycleID string, publicPlot uint32) (*logicrpcpb.BusinessCommandOutcome, error) {
	return e.executeTheftFor(playerID, nil, operationID, cycleID, publicPlot)
}

func (e *e2e) executeTheftFor(playerID uint64, join *joinInfo, operationID, cycleID string, publicPlot uint32) (*logicrpcpb.BusinessCommandOutcome, error) {
	payload, err := marshal(&publicpb.TheftRequest{PlotId: publicPlot, TargetFarmId: e.farmID, CropCycleId: cycleID})
	if err != nil {
		return nil, err
	}
	return e.execute(e.farmLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + operationID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10), OperationId: operationID,
		CommandType: commonpb.CommandType_THEFT, Payload: payload,
	}, e.trusted(playerID, join, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
}

func outcomeStatus(outcome *logicrpcpb.BusinessCommandOutcome) commonpb.CommandStatus {
	if outcome == nil {
		return commonpb.CommandStatus_COMMAND_STATUS_UNSPECIFIED
	}
	return outcome.GetStatus()
}

func (e *e2e) theftWorkflowID(operationID string) (string, error) {
	pairID, err := identity.CanonicalFriendRelationID(e.viewer, e.owner)
	if err != nil {
		return "", err
	}
	return identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: e.viewer, StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: operationID,
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
}

func (e *e2e) waitTheftWork(workID string, predicate func(*workflow.WorkRecord) bool) (*workflow.WorkRecord, error) {
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		record, err := e.repository.Get(context.Background(), string(identity.WorkTypeTheft), workID)
		if err == nil && predicate(record) {
			return record, nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("THEFT WorkRecord %s did not reach requested state", workID)
}

// careExercise is the Care Authority multi-process proof against real Redis:
// Pair ActivateFriendRelation -> plant -> N distinct friends concurrent CARE
// on one WATER Incident (exactly one SUCCEEDED; Pair ActionAdmission per
// friend) -> CareRewardOrder COMPLETED for the winner.
func (e *e2e) careExercise() error {
	friends := e.friends
	if len(friends) == 0 {
		friends = []uint64{e.viewer}
	}
	raceCount := len(friends)
	log.Printf("CARE raw evidence: backend=real Redis (%s external=%v ns=%s), processes=farm-svr/player-svr/farm-worker(authority), friends=%d, mocks=GatewayDownlink capture only",
		e.redisAddr, e.externalRedis, e.namespace, raceCount)
	if err := e.buySeed(); err != nil {
		return err
	}
	ownerJoin, err := e.join(e.owner)
	if err != nil {
		return err
	}
	friendJoins := make(map[uint64]joinInfo, raceCount)
	for _, friendID := range friends {
		join, joinErr := e.join(friendID)
		if joinErr != nil {
			return fmt.Errorf("join friend %d: %w", friendID, joinErr)
		}
		friendJoins[friendID] = join
	}
	if err := e.startWorker(); err != nil {
		return fmt.Errorf("start authority farm-worker for Care recovery: %w", err)
	}

	plantOperationID := "care-plant-" + e.runID
	plantInput := plantscenario.PlantInput{
		SubjectPlayerID: e.owner, OperationID: plantOperationID, TargetFarmID: e.farmID,
		PlotID: 1, ExpectedPlotVersion: 1, CropDefinitionID: uint32(cropcatalog.CarrotDefinitionID),
		ExpectedCropRuleVersion: 1, SeedItemID: uint32(cropcatalog.CarrotSeedItemID),
	}
	definition, ok := cropcatalog.ByDefinitionID(int32(cropcatalog.CarrotDefinitionID))
	if !ok {
		return errors.New("carrot definition is unavailable")
	}
	plantInput.CropRuleSnapshot, err = cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		return fmt.Errorf("build CARE plant rule snapshot: %w", err)
	}
	plantRoot, err := plantscenario.BuildRoot(plantInput)
	if err != nil {
		return fmt.Errorf("build CARE plant root: %w", err)
	}
	plantPayload, err := marshal(&publicpb.PlantRequest{
		PlotId: 0, CropDefinitionId: uint32(cropcatalog.CarrotDefinitionID), TargetFarmId: e.farmID,
		ExpectedPlotVersion: 1, ExpectedCropRuleVersion: 1,
	})
	if err != nil {
		return err
	}
	plantOutcome, err := e.execute(e.farmLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + plantOperationID, OperationId: plantOperationID,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}, e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
	if err != nil {
		return fmt.Errorf("CARE prepare PLANT transport: %w", err)
	}
	var plantPayloadBytes []byte
	switch plantOutcome.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		plantPayloadBytes = plantOutcome.GetResultPayload()
	case commonpb.CommandStatus_PROCESSING:
		terminal, waitErr := e.waitWork(plantRoot.WorkflowID, func(record *workflow.WorkRecord) bool { return record.Terminal() })
		if waitErr != nil {
			return fmt.Errorf("CARE prepare PLANT wait worker: %w", waitErr)
		}
		if terminal.State != workflow.WorkStateCompleted {
			return fmt.Errorf("CARE prepare PLANT terminal state=%s", terminal.State)
		}
		completed, queryErr := e.queryOperation(plantOperationID, e.farmID, e.owner, ownerJoin)
		if queryErr != nil {
			return fmt.Errorf("CARE prepare PLANT result query: %w", queryErr)
		}
		if completed.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED ||
			completed.GetCompletedOutcome() == nil ||
			completed.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			return fmt.Errorf("CARE prepare PLANT lookup=%s outcome=%v", completed.GetLookupStatus(), completed.GetCompletedOutcome())
		}
		plantPayloadBytes = completed.GetCompletedOutcome().GetResultPayload()
	default:
		return fmt.Errorf("CARE prepare PLANT status=%v error=%v", plantOutcome.GetStatus(), plantOutcome.GetError())
	}
	plantResult := &publicpb.PlantResult{}
	if err := proto.Unmarshal(plantPayloadBytes, plantResult); err != nil || plantResult.GetCropCycleId() == "" {
		farmResult := &farmpb.FarmCommandResult{}
		if farmErr := proto.Unmarshal(plantPayloadBytes, farmResult); farmErr != nil {
			return fmt.Errorf("decode CARE prepare Plant result: %w", err)
		}
		found := findPlot(farmResult.GetSnapshot(), 0)
		if found == nil || found.GetCropCycleId() == "" {
			return fmt.Errorf("CARE prepare PLANT missing crop: %v", farmResult.GetSnapshot())
		}
		plantResult = &publicpb.PlantResult{
			TargetFarmId: e.farmID, PlotId: 0, CropCycleId: found.GetCropCycleId(),
			CommittedFarmVersion: farmResult.GetSnapshot().GetFarmVersion(),
		}
	}
	if plantResult.GetCropCycleId() == "" {
		return fmt.Errorf("CARE prepare PLANT missing crop_cycle_id: %+v", plantResult)
	}
	cropCycleID := plantResult.GetCropCycleId()
	incidentID, err := identity.CareIncidentID(cropCycleID, "WATER", 1)
	if err != nil {
		return fmt.Errorf("derive CARE incident: %w", err)
	}
	log.Printf("CARE raw evidence: planted crop_cycle_id=%s incident_id=%s", cropCycleID, incidentID)

	beforeExp := make(map[uint64]int64, raceCount)
	for _, friendID := range friends {
		state, stateErr := e.privateState(friendID)
		if stateErr != nil {
			return fmt.Errorf("private state friend %d: %w", friendID, stateErr)
		}
		beforeExp[friendID] = state.GetExp()
	}
	careDueWait := 11 * time.Second
	log.Printf("CARE raw evidence: waiting %s for WATER incident due window", careDueWait)
	time.Sleep(careDueWait)

	carePayload, err := marshal(&publicpb.CareRequest{
		PlotId: 0, TargetFarmId: e.farmID, CropCycleId: cropCycleID,
		IncidentId: incidentID, CareType: publicpb.CareType_CARE_TYPE_WATER,
	})
	if err != nil {
		return err
	}
	type raceResult struct {
		index    int
		friendID uint64
		outcome  *logicrpcpb.BusinessCommandOutcome
		err      error
	}
	results := make(chan raceResult, raceCount)
	var wg sync.WaitGroup
	for index, friendID := range friends {
		index, friendID := index, friendID
		join := friendJoins[friendID]
		wg.Add(1)
		go func() {
			defer wg.Done()
			operationID := fmt.Sprintf("care-friend-%s-%d", e.runID, friendID)
			out, execErr := e.execute(e.farmLogic, context.Background(), &commonpb.ClientCommandEnvelope{
				RequestId: "request-" + operationID, OperationId: operationID,
				CommandType: commonpb.CommandType_CARE, Payload: carePayload,
			}, e.trusted(friendID, &join, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
			results <- raceResult{index: index, friendID: friendID, outcome: out, err: execErr}
		}()
	}
	wg.Wait()
	close(results)

	succeeded, noChange := 0, 0
	var winner *publicpb.CareResult
	var winnerID uint64
	for item := range results {
		if item.err != nil {
			return fmt.Errorf("friend CARE race[%d] friend=%d transport: %w", item.index, item.friendID, item.err)
		}
		if item.outcome == nil {
			return fmt.Errorf("friend CARE race[%d] friend=%d empty outcome", item.index, item.friendID)
		}
		switch item.outcome.GetStatus() {
		case commonpb.CommandStatus_SUCCEEDED:
			succeeded++
			decoded := &publicpb.CareResult{}
			if err := proto.Unmarshal(item.outcome.GetResultPayload(), decoded); err != nil {
				return fmt.Errorf("friend CARE race[%d] friend=%d decode CareResult: %w", item.index, item.friendID, err)
			}
			if decoded.GetResolvedByPlayerId() != item.friendID || decoded.GetIncidentId() != incidentID ||
				decoded.GetCareRewardOrderId() == "" || !decoded.GetRewardObligationCreated() ||
				decoded.GetOutcome() != publicpb.CareOutcome_CARE_OUTCOME_RESOLVED {
				return fmt.Errorf("friend CARE winner CareResult mismatch friend=%d: %+v", item.friendID, decoded)
			}
			winner = decoded
			winnerID = item.friendID
		case commonpb.CommandStatus_NO_CHANGE:
			noChange++
		default:
			return fmt.Errorf("friend CARE race[%d] friend=%d status=%v error=%v", item.index, item.friendID, item.outcome.GetStatus(), item.outcome.GetError())
		}
	}
	if succeeded != 1 || noChange != raceCount-1 || winner == nil || winnerID == 0 {
		return fmt.Errorf("friend CARE race want 1 SUCCEEDED + %d NO_CHANGE, got succeeded=%d no_change=%d", raceCount-1, succeeded, noChange)
	}
	log.Printf("CARE raw evidence: multi-friend concurrent CARE same incident: succeeded=1 no_change=%d order=%s resolved_by=%d",
		noChange, winner.GetCareRewardOrderId(), winner.GetResolvedByPlayerId())

	if err := e.waitCareOrderCompleted(winner.GetCareRewardOrderId()); err != nil {
		return err
	}
	after, err := e.privateState(winnerID)
	if err != nil {
		return err
	}
	if after.GetExp() <= beforeExp[winnerID] {
		return fmt.Errorf("friend CARE reward exp winner=%d before=%d after=%d, want increase", winnerID, beforeExp[winnerID], after.GetExp())
	}
	log.Printf("CARE raw evidence: CareRewardOrder COMPLETED; winner=%d exp before=%d after=%d", winnerID, beforeExp[winnerID], after.GetExp())
	return nil
}

func (e *e2e) waitCareOrderCompleted(orderID string) error {
	if e.authority == nil || e.calculator == nil {
		return errors.New("authority store unavailable for CareRewardOrder wait")
	}
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, actorrouting.ActorAddress{
		Domain: actorrouting.DomainFarm, ActorID: e.farmID,
	})
	if err != nil {
		return fmt.Errorf("resolve farm authority bucket: %w", err)
	}
	deadline := time.Now().Add(45 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		snapshot, getErr := e.authority.WorkLease().GetSourceOrder(context.Background(), bucket, string(identity.WorkTypeCareReward), orderID)
		if getErr == nil {
			lastState = snapshot.State
			if snapshot.State == "COMPLETED" {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("CareRewardOrder %s did not COMPLETE within 45s last_state=%q", orderID, lastState)
}

// harvestExercise is the narrow HV4 production proof.  It deliberately
// reuses the PO1 process/Redis/capture composition: the only externally
// supplied server remains GatewayDownlink capture, while Farm, Player,
// Participant, WorkRepository and worker recovery are real processes or
// Redis-backed production implementations.
func (e *e2e) harvestExercise() error {
	log.Printf("HV4 raw evidence: backend=real Redis (%s), processes=farm-svr/player-svr/farm-worker, mocks=GatewayDownlink capture only", e.redisAddr)
	if err := e.buySeed(); err != nil {
		return err
	}
	ownerJoin, err := e.join(e.owner)
	if err != nil {
		return err
	}
	viewerJoin, err := e.join(e.viewer)
	if err != nil {
		return err
	}

	plantOperationID := "hv4-plant-" + e.runID
	definition, ok := cropcatalog.ByDefinitionID(int32(cropcatalog.CarrotDefinitionID))
	if !ok {
		return errors.New("carrot definition is unavailable")
	}
	plantInput := plantscenario.PlantInput{
		SubjectPlayerID: e.owner, OperationID: plantOperationID, TargetFarmID: e.farmID,
		PlotID: 1, ExpectedPlotVersion: 1, CropDefinitionID: uint32(cropcatalog.CarrotDefinitionID),
		ExpectedCropRuleVersion: definition.CropRuleVersion,
		SeedItemID:              uint32(cropcatalog.CarrotSeedItemID),
	}
	plantInput.CropRuleSnapshot, err = cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		return fmt.Errorf("build carrot rule snapshot: %w", err)
	}
	if _, err := plantscenario.BuildRoot(plantInput); err != nil {
		return fmt.Errorf("build expected Plant root: %w", err)
	}
	plantPayload, err := marshal(&publicpb.PlantRequest{
		PlotId: 0, CropDefinitionId: uint32(cropcatalog.CarrotDefinitionID), TargetFarmId: e.farmID,
		ExpectedPlotVersion: 1, ExpectedCropRuleVersion: definition.CropRuleVersion,
	})
	if err != nil {
		return err
	}
	plantCommand := &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + plantOperationID, OperationId: plantOperationID,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}
	plantOutcome, err := e.execute(e.farmLogic, context.Background(), plantCommand,
		e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
	if err != nil || plantOutcome == nil || plantOutcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("prepare mature crop PLANT status=%v error=%v transport=%v", plantOutcome.GetStatus(), plantOutcome.GetError(), err)
	}
	plantResult := &farmpb.FarmCommandResult{}
	if err := proto.Unmarshal(plantOutcome.GetResultPayload(), plantResult); err != nil {
		return fmt.Errorf("decode prepare PLANT result: %w", err)
	}
	plantPlot := findPlot(plantResult.GetSnapshot(), 0)
	if plantPlot == nil || plantPlot.GetCropCycleId() == "" || plantPlot.GetMatureAtMs() == 0 || plantPlot.GetPlotVersion() == 0 {
		return fmt.Errorf("prepare PLANT did not return an active crop plot: %v", plantResult.GetSnapshot())
	}
	wait := time.Until(time.UnixMilli(plantPlot.GetMatureAtMs()))
	if wait > 0 {
		log.Printf("HV4 raw evidence: planted crop_cycle_id=%s plot_version=%d waiting_for_maturity=%s", plantPlot.GetCropCycleId(), plantPlot.GetPlotVersion(), wait.Round(time.Millisecond))
		time.Sleep(wait + 100*time.Millisecond)
	}
	baselineState, err := e.privateState(e.owner)
	if err != nil {
		return err
	}
	if itemCount(baselineState, int32(cropcatalog.CarrotSeedItemID)) != 0 {
		return fmt.Errorf("prepare PLANT left carrot seed count=%d", itemCount(baselineState, int32(cropcatalog.CarrotSeedItemID)))
	}
	ownerChangesBefore := len(filterChanges(e.capture.changes(), ownerJoin.playerID, ownerJoin.subscription))
	viewerChangesBefore := len(filterChanges(e.capture.changes(), viewerJoin.playerID, viewerJoin.subscription))

	harvestOperationID := "hv4-harvest-" + e.runID
	harvestInput := harvestscenario.HarvestInput{
		SubjectPlayerID: e.owner, OperationID: harvestOperationID, TargetFarmID: e.farmID,
		PlotID: 1, CropCycleID: plantPlot.GetCropCycleId(), ExpectedPlotVersion: plantPlot.GetPlotVersion(),
	}
	harvestRoot, err := harvestscenario.BuildRoot(harvestInput)
	if err != nil {
		return fmt.Errorf("build expected Harvest root: %w", err)
	}
	if _, err := harvestRoot.AddressForResolver(1, e.placement); err != nil {
		return fmt.Errorf("expected Harvest root placement: %w", err)
	}
	harvestPayload, err := marshal(&publicpb.HarvestRequest{
		PlotId: 0, TargetFarmId: e.farmID, CropCycleId: plantPlot.GetCropCycleId(), ExpectedPlotVersion: plantPlot.GetPlotVersion(),
	})
	if err != nil {
		return err
	}
	harvestCommand := &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + harvestOperationID, OperationId: harvestOperationID,
		CommandType: commonpb.CommandType_HARVEST, Payload: harvestPayload,
	}

	if err := e.stopPlayer(); err != nil {
		return fmt.Errorf("inject Player interruption before Harvest reward: %w", err)
	}
	firstResult := make(chan struct {
		out *logicrpcpb.BusinessCommandOutcome
		err error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		out, executeErr := e.execute(e.farmLogic, ctx, harvestCommand,
			e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
		firstResult <- struct {
			out *logicrpcpb.BusinessCommandOutcome
			err error
		}{out, executeErr}
	}()
	interrupted, err := e.waitWorkType(string(identity.WorkTypeHarvest), harvestRoot.WorkflowID, func(record *workflow.WorkRecord) bool {
		return record != nil && len(record.Steps) >= 2 && record.Steps[0].Status == workflow.StepApplied && record.Steps[1].Status == workflow.StepUnknown
	})
	if err != nil {
		return fmt.Errorf("wait Farm-authoritative Harvest commit and Player UNKNOWN: %w", err)
	}
	if interrupted.Terminal() || interrupted.State != workflow.WorkStateStepUnknown {
		return fmt.Errorf("interrupted Harvest WorkRecord state=%s, want STEP_UNKNOWN", interrupted.State)
	}
	log.Printf("HV4 raw evidence: WorkRecord work_type=%s work_id=%s Farm_step=%s Player_step=%s state=%s version=%d", interrupted.Address.WorkType, interrupted.Address.WorkID, interrupted.Steps[0].Status, interrupted.Steps[1].Status, interrupted.State, interrupted.WorkVersion)
	if err := e.stopFarm(); err != nil {
		return fmt.Errorf("simulate coordinator process exit after Farm commit: %w", err)
	}
	select {
	case first := <-firstResult:
		if first.out != nil {
			log.Printf("HV4 raw evidence: interrupted public HARVEST response status=%s error=%v transport_error=%v", first.out.GetStatus(), first.out.GetError(), first.err)
		} else {
			log.Printf("HV4 raw evidence: interrupted public HARVEST response=nil transport_error=%v", first.err)
		}
	case <-time.After(5 * time.Second):
		log.Printf("HV4 raw evidence: coordinator call remained in-flight when farm-svr was killed")
	}

	if err := e.startPlayer(); err != nil {
		return fmt.Errorf("restart Player for OperationResult PROCESSING read: %w", err)
	}
	processing, err := e.queryOperationType(harvestOperationID, commonpb.CommandType_HARVEST, e.farmID, e.owner)
	if err != nil {
		return fmt.Errorf("GET_OPERATION_RESULT Harvest while Player reward is unconfirmed: %w", err)
	}
	if processing.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING {
		return fmt.Errorf("GET_OPERATION_RESULT Harvest status=%s, want PROCESSING", processing.GetLookupStatus())
	}
	log.Printf("HV4 raw evidence: GET_OPERATION_RESULT original_command=HARVEST lookup_status=%s", processing.GetLookupStatus())

	if err := e.startFarm(); err != nil {
		return fmt.Errorf("restart farm-svr coordinator/Participant: %w", err)
	}
	if err := e.assertHarvestStaleFences(interrupted); err != nil {
		return err
	}
	if err := e.startWorker(); err != nil {
		return fmt.Errorf("start shared farm-worker Harvest recovery: %w", err)
	}
	terminal, err := e.waitWorkType(string(identity.WorkTypeHarvest), harvestRoot.WorkflowID, func(record *workflow.WorkRecord) bool {
		return record != nil && record.Terminal()
	})
	if err != nil {
		return fmt.Errorf("wait same Harvest WorkRecord after worker restart: %w", err)
	}
	if terminal.State != workflow.WorkStateCompleted || len(terminal.Steps) != 2 || terminal.Steps[0].Status != workflow.StepApplied || terminal.Steps[1].Status != workflow.StepApplied {
		return fmt.Errorf("recovered Harvest WorkRecord state=%s steps=%v, want COMPLETED/APPLIED/APPLIED", terminal.State, terminal.Steps)
	}
	farmResult, grantResult, err := harvestscenario.DecodeResults(terminal)
	if err != nil {
		return fmt.Errorf("decode recovered Harvest typed results: %w", err)
	}
	decision := farmResult.GetHarvestDecision()
	if decision == nil || grantResult.GetHarvestCountAfter() != 1 {
		return fmt.Errorf("recovered Harvest result counter/decision invalid: decision=%v grant=%v", decision, grantResult)
	}

	completed, err := e.queryOperationType(harvestOperationID, commonpb.CommandType_HARVEST, e.farmID, e.owner)
	if err != nil {
		return fmt.Errorf("final GET_OPERATION_RESULT Harvest: %w", err)
	}
	if completed.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED || completed.GetCompletedOutcome() == nil || completed.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("final Harvest OperationResult lookup=%s outcome=%v", completed.GetLookupStatus(), completed.GetCompletedOutcome())
	}
	publicResult := &publicpb.HarvestResult{}
	if err := proto.Unmarshal(completed.GetCompletedOutcome().GetResultPayload(), publicResult); err != nil {
		return fmt.Errorf("decode final public HarvestResult: %w", err)
	}
	if publicResult.GetTargetFarmId() != e.farmID || publicResult.GetPlotId() != 0 || publicResult.GetCropCycleId() != harvestRoot.Payload.GetCropCycleId() || publicResult.GetHarvestedQuantity() != uint32(decision.GetRewardQuantity()) || publicResult.GetCommittedFarmVersion() != farmResult.GetCommittedFarmVersion() {
		return fmt.Errorf("final HarvestResult=%v does not match frozen typed result", publicResult)
	}
	log.Printf("HV4 raw evidence: worker restart completed same WorkRecord; OperationResult=SUCCEEDED crop_cycle_id=%s quantity=%d farm_version=%d", publicResult.GetCropCycleId(), publicResult.GetHarvestedQuantity(), publicResult.GetCommittedFarmVersion())

	finalState, err := e.privateState(e.owner)
	if err != nil {
		return err
	}
	if err := assertHarvestPrivateState(baselineState, finalState, decision, harvestOperationID); err != nil {
		return err
	}
	if err := e.assertHarvestChange(ownerJoin, viewerJoin, ownerChangesBefore, viewerChangesBefore, publicResult.GetCommittedFarmVersion(), publicResult.GetPlotId(), publicResult.GetCropCycleId()); err != nil {
		return err
	}

	retryCommand := proto.Clone(harvestCommand).(*commonpb.ClientCommandEnvelope)
	retryCommand.RequestId = "request-" + harvestOperationID + "-retry"
	retry, err := e.execute(e.farmLogic, context.Background(), retryCommand,
		e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
	if err != nil || retry == nil || retry.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("same-operation Harvest retry status=%v error=%v transport=%v", retry.GetStatus(), retry.GetError(), err)
	}
	retryResult := &publicpb.HarvestResult{}
	if err := proto.Unmarshal(retry.GetResultPayload(), retryResult); err != nil {
		return fmt.Errorf("decode same-operation Harvest retry: %w", err)
	}
	finalBytes, _ := marshal(publicResult)
	retryBytes, _ := marshal(retryResult)
	if !bytes.Equal(finalBytes, retryBytes) {
		return errors.New("same operation_id Harvest retry returned a different public result")
	}
	stateAfterRetry, err := e.privateState(e.owner)
	if err != nil {
		return err
	}
	if !proto.Equal(finalState, stateAfterRetry) {
		return errors.New("same operation_id Harvest retry mutated Player private state")
	}
	time.Sleep(500 * time.Millisecond)
	if len(filterChanges(e.capture.changes(), ownerJoin.playerID, ownerJoin.subscription)) != ownerChangesBefore+1 || len(filterChanges(e.capture.changes(), viewerJoin.playerID, viewerJoin.subscription)) != viewerChangesBefore+1 {
		return errors.New("same operation_id Harvest retry published an extra FarmChange")
	}

	differentRequest := &publicpb.HarvestRequest{
		PlotId: 0, TargetFarmId: e.farmID, CropCycleId: publicResult.GetCropCycleId(), ExpectedPlotVersion: harvestRoot.Payload.GetExpectedPlotVersion() + 1,
	}
	differentPayload, err := marshal(differentRequest)
	if err != nil {
		return err
	}
	conflictCommand := proto.Clone(harvestCommand).(*commonpb.ClientCommandEnvelope)
	conflictCommand.RequestId = "request-" + harvestOperationID + "-payload-conflict"
	conflictCommand.Payload = differentPayload
	conflict, err := e.execute(e.farmLogic, context.Background(), conflictCommand,
		e.trusted(e.owner, &ownerJoin, nil), e.dispatch(actorrouting.DomainFarm, e.farmID))
	if err != nil || conflict == nil || conflict.GetStatus() != commonpb.CommandStatus_REJECTED || conflict.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		return fmt.Errorf("different payload hash status=%v error=%v transport=%v, want IDEMPOTENCY_CONFLICT", conflict.GetStatus(), conflict.GetError(), err)
	}
	log.Printf("HV4 raw evidence: same operation_id replayed identical result without state/change; different payload hash returned %s; stale writer_epoch and lease rejected", conflict.GetError().GetErrorCode())
	return nil
}

func (e *e2e) assertHarvestStaleFences(record *workflow.WorkRecord) error {
	if record == nil || len(record.Steps) < 2 || record.Steps[0].Name != string(identity.StepCommitHarvest) {
		return errors.New("Harvest WorkRecord lacks COMMIT_HARVEST step for stale-fence proof")
	}
	work := harvestWorkAddress(record)
	valid := &contractspb.ParticipantRequest{
		RouteHeader:        e.participantHeader(),
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST,
		TypedPayload:       append([]byte(nil), record.Steps[0].Payload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: work, ExpectedWorkVersion: record.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_HARVEST,
			ParticipantInvocationId: record.Steps[0].InvocationID, OperationId: record.OperationID,
			SubjectPlayerId: record.SubjectPlayerID, RemainingBudgetMs: 5000,
		}},
	}
	staleWriter := proto.Clone(valid).(*contractspb.ParticipantRequest)
	staleWriter.RouteHeader.ExpectedWriterEpoch++
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := e.farmParticipant.ExecuteStep(ctx, staleWriter)
	cancel()
	if err == nil {
		return errors.New("stale writer_epoch COMMIT_HARVEST request unexpectedly accepted")
	}
	log.Printf("HV4 raw evidence: stale writer_epoch fail-closed error=%v", err)

	if e.playerRecovery == nil || len(record.Steps[1].Payload) == 0 {
		return errors.New("Harvest Player recovery client or persisted Grant payload is unavailable")
	}
	leaseRequest := &contractspb.ParticipantRequest{
		RouteHeader:        e.participantHeaderFor(actorrouting.DomainPlayer, record.SubjectPlayerID),
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		TypedPayload:       append([]byte(nil), record.Steps[1].Payload...),
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: "hv4-stale-lease-owner-" + e.runID, CallerBootId: "hv4-stale-lease-boot-" + e.runID,
			Work: harvestWorkAddress(record), ExpectedWorkVersion: record.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_GRANT_HARVEST_REWARD,
			ParticipantInvocationId: record.Steps[1].InvocationID,
			LeaseOwnerToken:         "hv4-stale-lease-token-" + e.runID, LeaseEpoch: record.LeaseEpoch + 1, RemainingBudgetMs: 5000,
		}},
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	_, err = e.playerRecovery.ResumeStep(ctx, leaseRequest)
	cancel()
	if err == nil {
		return errors.New("stale lease_epoch recovery request unexpectedly accepted")
	}
	log.Printf("HV4 raw evidence: stale lease_epoch fail-closed error=%v", err)
	return nil
}

func harvestWorkAddress(record *workflow.WorkRecord) *contractspb.WorkAddress {
	if record == nil {
		return nil
	}
	return &contractspb.WorkAddress{
		WorkId: record.Address.WorkID, WorkType: contractspb.WorkType_WORK_TYPE_HARVEST,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		BucketVersion: record.Address.BucketVersion, BucketId: record.Address.BucketID,
	}
}

func assertHarvestPrivateState(before, after *playerpb.PrivateState, decision *contractspb.HarvestDecision, operationID string) error {
	if before == nil || after == nil || decision == nil {
		return errors.New("Harvest private-state proof has a nil state or decision")
	}
	itemID := int32(decision.GetRewardItemId())
	if itemCount(after, itemID) != itemCount(before, itemID)+int64(decision.GetRewardQuantity()) {
		return fmt.Errorf("Harvest inventory delta=%d, want %d", itemCount(after, itemID)-itemCount(before, itemID), decision.GetRewardQuantity())
	}
	if after.GetExp() != before.GetExp()+int64(decision.GetRewardExp()) {
		return fmt.Errorf("Harvest exp delta=%d, want %d", after.GetExp()-before.GetExp(), decision.GetRewardExp())
	}
	if after.GetEconomyVersion() != before.GetEconomyVersion()+1 {
		return fmt.Errorf("Harvest economy version delta=%d, want 1", after.GetEconomyVersion()-before.GetEconomyVersion())
	}
	if len(after.GetCodexEntries()) != len(before.GetCodexEntries())+1 {
		return fmt.Errorf("Harvest codex entries before=%d after=%d, want one new entry", len(before.GetCodexEntries()), len(after.GetCodexEntries()))
	}
	foundCodex := false
	for _, entry := range after.GetCodexEntries() {
		if entry != nil && entry.GetCropDefinitionId() == int32(decision.GetCropDefinitionId()) && strings.TrimSpace(entry.GetSourceOperationId()) != "" {
			foundCodex = true
		}
	}
	if !foundCodex {
		return fmt.Errorf("Harvest codex entry for crop=%d operation=%q was not committed", decision.GetCropDefinitionId(), operationID)
	}
	beforeTask, beforeOK := taskProgress(before, 1)
	afterTask, afterOK := taskProgress(after, 1)
	if !beforeOK || !afterOK || afterTask != beforeTask+1 {
		return fmt.Errorf("Harvest task definition 1 progress before=%d after=%d, want one increment", beforeTask, afterTask)
	}
	log.Printf("HV4 raw evidence: Player exactly-once inventory item=%d +%d exp +%d task_definition=1 +1 codex +1 economy_version +1", itemID, decision.GetRewardQuantity(), decision.GetRewardExp())
	return nil
}

func (e *e2e) assertHarvestChange(ownerJoin, viewerJoin joinInfo, ownerBefore, viewerBefore int, expectedVersion uint64, plotIndex uint32, cropCycleID string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events := e.capture.changes()
		ownerEvents := filterChanges(events, ownerJoin.playerID, ownerJoin.subscription)
		viewerEvents := filterChanges(events, viewerJoin.playerID, viewerJoin.subscription)
		if len(ownerEvents) > ownerBefore+1 || len(viewerEvents) > viewerBefore+1 {
			return fmt.Errorf("Harvest published duplicate FarmChange owner=%d viewer=%d", len(ownerEvents)-ownerBefore, len(viewerEvents)-viewerBefore)
		}
		if len(ownerEvents) == ownerBefore+1 && len(viewerEvents) == viewerBefore+1 {
			ownerEvent := ownerEvents[ownerBefore]
			viewerEvent := viewerEvents[viewerBefore]
			if ownerEvent.change.GetFarmVersion() != expectedVersion || viewerEvent.change.GetFarmVersion() != expectedVersion {
				return fmt.Errorf("Harvest FarmChange versions owner=%d viewer=%d want=%d", ownerEvent.change.GetFarmVersion(), viewerEvent.change.GetFarmVersion(), expectedVersion)
			}
			if !bytes.Equal(ownerEvent.encoded, viewerEvent.encoded) {
				return errors.New("Harvest subscribers received different FarmChange payloads")
			}
			if err := assertEmptyHarvestPlot(ownerEvent.change, plotIndex, cropCycleID); err != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			all := e.capture.changes()
			if len(filterChanges(all, ownerJoin.playerID, ownerJoin.subscription)) != ownerBefore+1 || len(filterChanges(all, viewerJoin.playerID, viewerJoin.subscription)) != viewerBefore+1 {
				return errors.New("Harvest retry/recovery published an extra FarmChange")
			}
			log.Printf("HV4 raw evidence: FarmChange exactly once per subscriber version=%d changed_plot=%d crop_cycle_after=empty", expectedVersion, plotIndex)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("Harvest FarmChange not observed exactly once owner_delta=%d viewer_delta=%d", len(filterChanges(e.capture.changes(), ownerJoin.playerID, ownerJoin.subscription))-ownerBefore, len(filterChanges(e.capture.changes(), viewerJoin.playerID, viewerJoin.subscription))-viewerBefore)
}

func assertEmptyHarvestPlot(change *farmpb.FarmChange, plotIndex uint32, cropCycleID string) error {
	if change == nil {
		return errors.New("Harvest FarmChange is nil")
	}
	for _, plot := range change.GetChangedPlots() {
		if plot == nil || plot.GetPlotIndex() != plotIndex {
			continue
		}
		if plot.GetCropCycleId() != "" {
			return fmt.Errorf("Harvest FarmChange plot=%d still has crop_cycle_id=%s", plotIndex, plot.GetCropCycleId())
		}
		if cropCycleID == "" {
			return nil
		}
		return nil
	}
	return fmt.Errorf("Harvest FarmChange omitted changed plot=%d for crop_cycle_id=%s", plotIndex, cropCycleID)
}

func (e *e2e) buySeed() error {
	return e.buySeedWithOperation("po1-buy-" + e.runID)
}

func (e *e2e) buySeedWithOperation(operationID string) error {
	payload, err := marshal(&publicpb.BuyRequest{OfferId: int32(cropcatalog.CarrotOfferID), Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return err
	}
	out, err := e.execute(e.playerLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + operationID, OperationId: operationID,
		CommandType: commonpb.CommandType_BUY, Payload: payload,
	}, e.trusted(e.owner, nil, nil), e.dispatch(actorrouting.DomainPlayer, e.owner))
	if err != nil || out.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return fmt.Errorf("BUY carrot seed status=%v err=%v", out.GetStatus(), err)
	}
	return nil
}

func (e *e2e) join(playerID uint64) (joinInfo, error) {
	operationID := "po1-join-" + strconv.FormatUint(playerID, 10) + "-" + e.runID
	attemptID := "attempt-" + operationID
	payload, err := marshal(&publicpb.JoinFarmRequest{TargetFarmId: e.farmID})
	if err != nil {
		return joinInfo{}, err
	}
	info := joinInfo{playerID: playerID, connectionID: "connection-" + strconv.FormatUint(playerID, 10)}
	trusted := e.trusted(playerID, nil, &logicrpcpb.JoinAdmissionContext{JoinAttemptId: attemptID, TargetFarmId: e.farmID})
	out, err := e.execute(e.farmLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId: "request-" + operationID, OperationId: operationID,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: payload,
	}, trusted, e.dispatch(actorrouting.DomainFarm, e.farmID))
	if err != nil {
		return joinInfo{}, fmt.Errorf("JOIN_FARM player=%d: %w", playerID, err)
	}
	accepted := out.GetJoinAccepted()
	if out.GetStatus() != commonpb.CommandStatus_ACCEPTED || accepted == nil || accepted.GetSubscriptionId() == "" || accepted.GetJoinAttemptId() != attemptID {
		return joinInfo{}, fmt.Errorf("JOIN_FARM player=%d status=%s error=%v metadata=%v", playerID, out.GetStatus(), out.GetError(), accepted)
	}
	info.subscription, info.attemptID = accepted.GetSubscriptionId(), accepted.GetJoinAttemptId()
	target := e.targetFence(info)
	arm, err := e.farmLogic.ArmSnapshot(context.Background(), &logicrpcpb.ArmSnapshotRequest{
		Subscription: &logicrpcpb.SubscriptionFence{FarmId: e.farmID, SubscriptionId: info.subscription, JoinAttemptId: info.attemptID},
		Target:       target,
	})
	if err != nil || (arm.GetStatus() != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED && arm.GetStatus() != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ALREADY_ARMED) {
		return joinInfo{}, fmt.Errorf("ArmSnapshot player=%d status=%v err=%v", playerID, arm.GetStatus(), err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if e.capture.snapshotReady(playerID, info.subscription) {
			return info, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return joinInfo{}, fmt.Errorf("JOIN_FARM player=%d snapshot/activation downlink not observed", playerID)
}

func (e *e2e) trusted(playerID uint64, join *joinInfo, joinAttempt *logicrpcpb.JoinAdmissionContext) *logicrpcpb.TrustedCommandContext {
	session := &logicrpcpb.SessionPermitSnapshot{
		PlayerId: playerID, ActiveSessionEpoch: 1,
		GatewayId: "po1-e2e-gateway", GatewayBootId: "po1-e2e-gateway-boot",
		ConnectionId:         "connection-" + strconv.FormatUint(playerID, 10),
		ConnectionGeneration: 1, SessionRouteRevision: 1,
		AdmittedAtUnixMs: time.Now().UnixMilli(),
	}
	trusted := &logicrpcpb.TrustedCommandContext{Session: session, RecoveryCompleteAtAdmission: true}
	if joinAttempt != nil {
		trusted.JoinAttempt = joinAttempt
	}
	if join != nil {
		trusted.FarmContext = &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: e.farmID, AdmittedSubscriptionId: join.subscription}
	}
	return trusted
}

func (e *e2e) targetFence(join joinInfo) *logicrpcpb.DownlinkTargetFence {
	return &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: join.playerID, GatewayId: "po1-e2e-gateway", GatewayBootId: "po1-e2e-gateway-boot",
		ConnectionId: join.connectionID, ConnectionGeneration: 1, SessionEpoch: 1, SessionRouteRevision: 1,
	}
}

func (e *e2e) dispatch(domain actorrouting.RouteDomain, actorID uint64) *logicrpcpb.DispatchContext {
	assignment, err := e.resolver.Resolve(context.Background(), actorrouting.Address{Realm: e2eRealm, Kind: domain, ID: actorID})
	if err != nil {
		log.Fatalf("resolve E2E route %s/%d: %v", domain, actorID, err)
	}
	protoDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	if domain == actorrouting.DomainFarm {
		protoDomain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	}
	revision := assignment.SnapshotRevision
	return &logicrpcpb.DispatchContext{
		RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
		ActorRoute: &logicrpcpb.ActorRouteHeader{
			RouteDomain: protoDomain, ActorId: actorID, VirtualShard: assignment.ShardID,
			ShardRevision: assignment.ShardRevision, ExpectedHolderId: assignment.HolderPod,
			ExpectedHolderBootId: assignment.HolderBootID, ExpectedWriterEpoch: assignment.WriterEpoch,
			SourceSnapshotRevision: &revision,
		},
	}
}

func (e *e2e) participantHeader() *logicrpcpb.ActorRouteHeader {
	return e.participantHeaderFor(actorrouting.DomainFarm, e.farmID)
}

func (e *e2e) participantHeaderFor(domain actorrouting.RouteDomain, actorID uint64) *logicrpcpb.ActorRouteHeader {
	header := proto.Clone(e.dispatch(domain, actorID).GetActorRoute()).(*logicrpcpb.ActorRouteHeader)
	// The generated CoordinatorRouteProvider intentionally carries no source
	// snapshot tuple. A direct Participant probe must use the same complete
	// shape; a revision without source instance/boot is rejected as partial.
	header.SourceSnapshotRevision = nil
	return header
}

func (e *e2e) execute(client logicrpcpb.LogicServiceClient, ctx context.Context, command *commonpb.ClientCommandEnvelope, trusted *logicrpcpb.TrustedCommandContext, dispatch *logicrpcpb.DispatchContext) (*logicrpcpb.BusinessCommandOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	response, err := client.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
		Command: command, Trusted: trusted, Dispatch: dispatch, TraceId: command.GetRequestId(),
	})
	if err != nil {
		return nil, err
	}
	if response == nil || response.GetBusiness() == nil {
		return nil, fmt.Errorf("FirstHop returned route rejection or empty business outcome: %v", response)
	}
	return response.GetBusiness(), nil
}

func (e *e2e) queryOperation(operationID string, farmID, playerID uint64, _ joinInfo) (*publicpb.GetOperationResultResult, error) {
	return e.queryOperationType(operationID, commonpb.CommandType_PLANT, farmID, playerID)
}

func (e *e2e) queryOperationType(operationID string, commandType commonpb.CommandType, farmID, playerID uint64) (*publicpb.GetOperationResultResult, error) {
	payload, err := marshal(&publicpb.GetOperationResultRequest{
		OriginalCommandType: commandType, OriginalOperationId: operationID,
		OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: farmID}},
	})
	if err != nil {
		return nil, err
	}
	out, err := e.execute(e.playerLogic, context.Background(), &commonpb.ClientCommandEnvelope{
		RequestId:   "query-" + operationID + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		CommandType: commonpb.CommandType_GET_OPERATION_RESULT, Payload: payload,
	}, e.trusted(playerID, nil, nil), &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB})
	if err != nil {
		return nil, err
	}
	if out.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return nil, fmt.Errorf("GET_OPERATION_RESULT FirstHop status=%s error=%v", out.GetStatus(), out.GetError())
	}
	result := &publicpb.GetOperationResultResult{}
	if err := proto.Unmarshal(out.GetResultPayload(), result); err != nil {
		return nil, err
	}
	if result.GetLookupStatus() == publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND {
		return nil, errors.New("GET_OPERATION_RESULT returned NOT_FOUND after durable WorkRecord creation")
	}
	return result, nil
}

func (e *e2e) privateState(playerID uint64) (*playerpb.PrivateState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := e.playerParticipant.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: playerID})
	if err != nil || response == nil || response.GetPrivateState() == nil {
		return nil, fmt.Errorf("GetPrivateState player=%d: %v", playerID, err)
	}
	return response.GetPrivateState(), nil
}

func (e *e2e) waitWork(workID string, predicate func(*workflow.WorkRecord) bool) (*workflow.WorkRecord, error) {
	return e.waitWorkType(string(identity.WorkTypePlant), workID, predicate)
}

func (e *e2e) waitWorkType(workType, workID string, predicate func(*workflow.WorkRecord) bool) (*workflow.WorkRecord, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		record, err := e.repository.Get(context.Background(), workType, workID)
		if err == nil && predicate(record) {
			return record, nil
		}
		if err != nil && !errors.Is(err, workflow.ErrNotFound) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("WorkRecord %s did not reach requested state", workID)
}

func (e *e2e) assertSingleChange(ownerJoin, viewerJoin joinInfo, expectedVersion uint64) error {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		events := e.capture.changes()
		ownerEvents := filterChanges(events, ownerJoin.playerID, ownerJoin.subscription)
		viewerEvents := filterChanges(events, viewerJoin.playerID, viewerJoin.subscription)
		if len(ownerEvents) >= 1 && len(viewerEvents) >= 1 {
			if len(ownerEvents) != 1 || len(viewerEvents) != 1 {
				return fmt.Errorf("FarmChange duplicate delivery owner=%d viewer=%d", len(ownerEvents), len(viewerEvents))
			}
			if ownerEvents[0].change.GetFarmVersion() != expectedVersion || viewerEvents[0].change.GetFarmVersion() != expectedVersion {
				return fmt.Errorf("FarmChange versions owner=%d viewer=%d expected=%d", ownerEvents[0].change.GetFarmVersion(), viewerEvents[0].change.GetFarmVersion(), expectedVersion)
			}
			if !bytes.Equal(ownerEvents[0].encoded, viewerEvents[0].encoded) {
				return errors.New("two subscribers received different FarmChange payloads")
			}
			time.Sleep(500 * time.Millisecond)
			all := e.capture.changes()
			if len(filterChanges(all, ownerJoin.playerID, ownerJoin.subscription)) != 1 || len(filterChanges(all, viewerJoin.playerID, viewerJoin.subscription)) != 1 {
				return errors.New("same-operation retry produced a duplicate FarmChange")
			}
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("FarmChange not observed once for both current subscribers: owner=%d viewer=%d", len(filterChanges(e.capture.changes(), ownerJoin.playerID, ownerJoin.subscription)), len(filterChanges(e.capture.changes(), viewerJoin.playerID, viewerJoin.subscription)))
}

func filterChanges(events []changeEvent, playerID uint64, subscription string) []changeEvent {
	var result []changeEvent
	for _, event := range events {
		if event.playerID == playerID && event.subscription == subscription {
			result = append(result, event)
		}
	}
	return result
}

func (e *e2e) assertStaleParticipantFailsClosed(record *workflow.WorkRecord) error {
	if record == nil || len(record.Steps) < 2 {
		return errors.New("terminal WorkRecord lacks COMMIT_PLANT step")
	}
	header := e.dispatch(actorrouting.DomainFarm, e.farmID).GetActorRoute()
	valid := &contractspb.ParticipantRequest{
		RouteHeader: header, ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		TypedPayload: append([]byte(nil), record.Steps[1].Payload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: &contractspb.WorkAddress{WorkId: record.Address.WorkID, WorkType: contractspb.WorkType_WORK_TYPE_PLANT,
				PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
				BucketVersion: 1, BucketId: record.Address.BucketID},
			ExpectedWorkVersion: record.WorkVersion, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLANT,
			ParticipantInvocationId: record.Steps[1].InvocationID, OperationId: record.OperationID,
			SubjectPlayerId: record.SubjectPlayerID, RemainingBudgetMs: 5000,
		}},
	}
	for name, mutate := range map[string]func(*logicrpcpb.ActorRouteHeader){
		"stale writer epoch": func(route *logicrpcpb.ActorRouteHeader) { route.ExpectedWriterEpoch++ },
		"stale holder lease": func(route *logicrpcpb.ActorRouteHeader) {
			route.ExpectedHolderBootId = route.ExpectedHolderBootId + "-stale"
		},
	} {
		request := proto.Clone(valid).(*contractspb.ParticipantRequest)
		mutate(request.RouteHeader)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := e.farmParticipant.ExecuteStep(ctx, request)
		cancel()
		if err == nil {
			return fmt.Errorf("%s request unexpectedly accepted", name)
		}
	}
	return nil
}

func (e *e2e) stopPlayer() error {
	if e.playerConn != nil {
		_ = e.playerConn.Close()
		e.playerConn = nil
	}
	if e.playerCmd == nil {
		return nil
	}
	if e.playerCmd.Process != nil {
		_ = e.playerCmd.Process.Signal(syscall.SIGKILL)
	}
	_ = e.playerCmd.Wait()
	e.playerCmd = nil
	return nil
}

func (e *e2e) stopFarm() error {
	if e.farmConn != nil {
		_ = e.farmConn.Close()
		e.farmConn = nil
	}
	if e.farmCmd == nil {
		return nil
	}
	if e.farmCmd.Process != nil {
		_ = e.farmCmd.Process.Signal(syscall.SIGKILL)
	}
	_ = e.farmCmd.Wait()
	e.farmCmd = nil
	return nil
}

func (e *e2e) stopWorker() error {
	if e.workerCmd == nil {
		return nil
	}
	if e.workerCmd.Process != nil {
		_ = e.workerCmd.Process.Signal(syscall.SIGKILL)
	}
	_ = e.workerCmd.Wait()
	e.workerCmd = nil
	return nil
}

func (e *e2e) close() {
	if e.gateCmd != nil {
		stopCommand(e.gateCmd)
	}
	if e.workerCmd != nil {
		stopCommand(e.workerCmd)
	}
	if e.farmCmd != nil {
		stopCommand(e.farmCmd)
	}
	if e.playerCmd != nil {
		stopCommand(e.playerCmd)
	}
	if e.playerConn != nil {
		_ = e.playerConn.Close()
	}
	if e.farmConn != nil {
		_ = e.farmConn.Close()
	}
	if e.captureServer != nil {
		e.captureServer.Stop()
	}
	if e.captureLn != nil {
		_ = e.captureLn.Close()
	}
	if e.redis != nil {
		_ = e.redis.Close()
	}
	if e.dockerName != "" {
		if e.redisProcess != nil {
			stopCommand(e.redisProcess)
		}
		if docker, err := exec.LookPath("docker"); err == nil {
			_ = exec.Command(docker, "rm", "-f", e.dockerName).Run()
		}
	} else if e.redisProcess != nil {
		stopCommand(e.redisProcess)
	}
	if e.temp != "" {
		_ = os.RemoveAll(e.temp)
	}
}

func stopCommand(command *exec.Cmd) {
	if command == nil {
		return
	}
	if command.Process != nil && command.ProcessState == nil {
		_ = command.Process.Signal(syscall.SIGKILL)
	}
	_ = command.Wait()
}

func freeTCPAddress() (string, error) {
	port, err := freeTCPPort()
	if err != nil {
		return "", err
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
}

func freeTCPPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate local TCP port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func dialGRPC(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func waitReady(address string) error {
	return waitHTTPStatus(address, "/readyz", http.StatusOK, 20*time.Second)
}

func waitHealthz(address string) error {
	return waitHTTPStatus(address, "/healthz", http.StatusOK, 20*time.Second)
}

func waitHTTPStatus(address, path string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+path, nil)
		if err == nil {
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				cancel()
				if response.StatusCode == want {
					return nil
				}
			} else {
				cancel()
			}
		} else {
			cancel()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s%s did not return %d", address, path, want)
}

func marshal(message proto.Message) ([]byte, error) {
	return (proto.MarshalOptions{Deterministic: true}).Marshal(message)
}

func itemCount(state *playerpb.PrivateState, itemID int32) int64 {
	if state == nil {
		return 0
	}
	for _, item := range state.GetInventory() {
		if item.GetItemId() == itemID {
			return item.GetCount()
		}
	}
	return 0
}

func taskProgress(state *playerpb.PrivateState, definitionID int32) (int64, bool) {
	if state == nil {
		return 0, false
	}
	for _, instance := range state.GetTasks() {
		if instance != nil && instance.GetTaskDefinitionId() == definitionID {
			return instance.GetProgress(), true
		}
	}
	return 0, false
}

func findPlot(snapshot *farmpb.FarmSnapshot, plotIndex uint32) *farmpb.PlotView {
	if snapshot == nil {
		return nil
	}
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetPlotIndex() == plotIndex {
			return plot
		}
	}
	return nil
}

func assertOnePlantedPlot(snapshot *farmpb.FarmSnapshot, cropCycleID string) error {
	if snapshot == nil || snapshot.GetOwnerId() == 0 {
		return errors.New("FarmCommandResult has no authoritative snapshot")
	}
	count := 0
	for _, plot := range snapshot.GetPlots() {
		if plot.GetCropCycleId() == cropCycleID {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("crop cycle %s occurs %d times, want once", cropCycleID, count)
	}
	return nil
}
