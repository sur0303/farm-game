package ingress

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playerparticipant "farmgame/internal/player/participant"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/carerecovery"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type coordinatorStepClient func(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)

func (f coordinatorStepClient) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return f(ctx, req)
}

type sourceOrderE2EResolver func(context.Context, actorrouting.Address) (actorrouting.Assignment, error)

func (f sourceOrderE2EResolver) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	return f(ctx, address)
}

// sourceOrderE2EPlayerTransport injects faults only at the RPC response edge;
// the request still enters the real Player participant server and Authority
// registry. It is shared by Direct and the actual recovery Worker.
type sourceOrderE2EPlayerTransport struct {
	mu                    sync.Mutex
	server                *playerparticipant.Server
	tamperNextProof       bool
	stripNextProof        bool
	loseNextResponse      bool
	coordinatorCalls      int
	recoveryCalls         int
	replayed              int
	coordinatorInvocation string
	recoveryInvocation    string
}

func (p *sourceOrderE2EPlayerTransport) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	p.mu.Lock()
	p.coordinatorCalls++
	p.coordinatorInvocation = req.GetCoordinator().GetParticipantInvocationId()
	tamper, strip, lose := p.tamperNextProof, p.stripNextProof, p.loseNextResponse
	p.tamperNextProof, p.stripNextProof, p.loseNextResponse = false, false, false
	p.mu.Unlock()
	request := proto.Clone(req).(*contractspb.ParticipantRequest)
	if strip {
		request.SourceOrderCommitProof = nil
	}
	if tamper && request.GetSourceOrderCommitProof() != nil && len(request.GetSourceOrderCommitProof().GetSignature()) > 0 {
		request.SourceOrderCommitProof.Signature[0]++
	}
	response, err := p.server.ExecuteStep(ctx, request)
	if err == nil && response.GetReplayedFromReceipt() {
		p.mu.Lock()
		p.replayed++
		p.mu.Unlock()
	}
	if err == nil && lose {
		return nil, errors.New("source-order e2e: response lost after Player commit")
	}
	return response, err
}

func (p *sourceOrderE2EPlayerTransport) ResumeStep(ctx context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	p.mu.Lock()
	p.recoveryCalls++
	p.recoveryInvocation = req.GetRecovery().GetParticipantInvocationId()
	p.mu.Unlock()
	response, err := p.server.ResumeStep(ctx, proto.Clone(req).(*contractspb.ParticipantRequest))
	if err == nil && response.GetReplayedFromReceipt() {
		p.mu.Lock()
		p.replayed++
		p.mu.Unlock()
	}
	return response, err
}

func (p *sourceOrderE2EPlayerTransport) fault(tamper, strip, lose bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tamperNextProof, p.stripNextProof, p.loseNextResponse = tamper, strip, lose
}

func (p *sourceOrderE2EPlayerTransport) counts() (coordinator, recovery, replayed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.coordinatorCalls, p.recoveryCalls, p.replayed
}

func (p *sourceOrderE2EPlayerTransport) invocations() (coordinator, recovery string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.coordinatorInvocation, p.recoveryInvocation
}

// sourceOrderE2E is one miniredis authority plane shared by a Farm actor and
// a Player actor. Public PLANT/CARE/HARVEST SourceOrder paths, Direct, and the
// recovery Worker use the same Lua commit/due/complete registry as production.
type sourceOrderE2E struct {
	actorID         uint64
	redis           *miniredis.Miniredis
	store           *redisauthority.Store
	calculator      *actorrouting.Calculator
	profile         rediskey.PhysicalProfile
	farmRegistry    *farmactor.Registry
	playerRegistry  *playeractor.Registry
	farmRoute       *logicrpcpb.ActorRouteHeader
	playerRoute     *logicrpcpb.ActorRouteHeader
	farm            *Server
	direct          *carerecovery.DirectDispatcher
	service         *Service
	playerTransport *sourceOrderE2EPlayerTransport
	proofClockMs    *atomic.Int64
	bucketForFarm   func(uint64) (rediskey.AuthorityBucketRef, error)
}

func newSourceOrderE2E(t *testing.T, actorID uint64) *sourceOrderE2E {
	t.Helper()
	return newSourceOrderE2EWithProofMode(t, actorID, sourceorder.CommitProofModeOff)
}

func newSourceOrderE2EWithProofMode(t *testing.T, actorID uint64, proofMode sourceorder.CommitProofMode) *sourceOrderE2E {
	t.Helper()
	now := time.UnixMilli(ingressTestNowMs)
	proofClockMs := &atomic.Int64{}
	proofClockMs.Store(now.UnixMilli())
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "source-order-e2e")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("source-order-e2e",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130001}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{
		Profile: profile, Resolver: calculator,
		LeaseLimits: authoritystore.LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assignments := make(map[actorrouting.RouteDomain]actorrouting.Assignment)
	claim := func(domain actorrouting.RouteDomain, holder string) authoritystore.ShardWriteFence {
		t.Helper()
		shard, shardErr := calculator.ShardFor(domain, actorID)
		if shardErr != nil {
			t.Fatal(shardErr)
		}
		claimed, claimErr := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind:       authoritystore.ShardAuthorityCallerRouteManager,
				InstanceID: "source-order-e2e-rm", BootID: "source-order-e2e-rm-boot",
			},
			TransitionID: "claim-" + string(domain) + "-" + holder, AuthorityLayoutVersion: "v1",
			Domain: domain, VirtualShard: shard, NewHolderID: holder, NewHolderBootID: holder + "-boot",
			RequestedTTLms: 3_600_000,
		})
		if claimErr != nil || claimed.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("claim %s authority=%+v err=%v", domain, claimed, claimErr)
		}
		assignments[domain] = actorrouting.Assignment{
			Kind: domain, ShardID: shard, HolderPod: claimed.Authority.HolderID,
			HolderBootID: claimed.Authority.HolderBootID, ShardRevision: claimed.Authority.AssignmentRevision,
			AssignmentRevision: claimed.Authority.AssignmentRevision, WriterEpoch: claimed.Authority.WriterEpoch,
			LeaseEpoch: claimed.Authority.LeaseEpoch, LeaseUntilMs: claimed.Authority.LeaseUntilMs,
			HolderState: actorrouting.HolderStateActive, ShardState: actorrouting.ShardStateActive,
		}
		return authoritystore.ShardWriteFence{
			ExpectedHolderID: claimed.Authority.HolderID, ExpectedHolderBootID: claimed.Authority.HolderBootID,
			ExpectedAssignmentRevision: claimed.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        claimed.Authority.WriterEpoch, ExpectedLeaseEpoch: claimed.Authority.LeaseEpoch,
		}
	}
	farmFence := claim(actorrouting.DomainFarm, "farm-holder")
	playerFence := claim(actorrouting.DomainPlayer, "player-holder")
	farmProvider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: farmFence},
		AuthorityLayoutVersion: "v1", BootID: farmFence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	playerProvider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: playerFence},
		AuthorityLayoutVersion: "v1", BootID: playerFence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmRegistry := farmactor.NewAuthorityRegistry(farmProvider, farmactor.RegistryOptions{MailboxSize: 16})
	t.Cleanup(farmRegistry.Stop)
	playerRegistry := playeractor.NewAuthorityRegistry(playerProvider, playeractor.RegistryOptions{MailboxSize: 16})
	t.Cleanup(playerRegistry.Stop)
	if _, err := farmRegistry.Submit(ctx, actorID, func(context.Context, *farmactor.FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := playerRegistry.Ensure(ctx, actorID, "source-order-e2e-bootstrap"); err != nil {
		t.Fatal(err)
	}

	farmShard, err := calculator.ShardFor(actorrouting.DomainFarm, actorID)
	if err != nil {
		t.Fatal(err)
	}
	playerShard, err := calculator.ShardFor(actorrouting.DomainPlayer, actorID)
	if err != nil {
		t.Fatal(err)
	}
	farmRoute := &logicrpcpb.ActorRouteHeader{
		RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID, VirtualShard: farmShard,
		ShardRevision: farmFence.ExpectedAssignmentRevision, ExpectedHolderId: farmFence.ExpectedHolderID,
		ExpectedHolderBootId: farmFence.ExpectedHolderBootID, ExpectedWriterEpoch: farmFence.ExpectedWriterEpoch,
		ExpectedLeaseEpoch: farmFence.ExpectedLeaseEpoch, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
	}
	playerRoute := &logicrpcpb.ActorRouteHeader{
		RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: actorID, VirtualShard: playerShard,
		ShardRevision: playerFence.ExpectedAssignmentRevision, ExpectedHolderId: playerFence.ExpectedHolderID,
		ExpectedHolderBootId: playerFence.ExpectedHolderBootID, ExpectedWriterEpoch: playerFence.ExpectedWriterEpoch,
		ExpectedLeaseEpoch: playerFence.ExpectedLeaseEpoch, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
	}
	bucketForFarm := func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
		return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
	}
	bucketForPlayer := func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
		return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
	}
	playerSource, err := NewRedisPlayerSourceVerifier(store, bucketForPlayer)
	if err != nil {
		t.Fatal(err)
	}
	harvestSource, err := playerparticipant.NewRedisHarvestSourceVerifier(store, bucketForFarm)
	if err != nil {
		t.Fatal(err)
	}
	careSource, err := playerparticipant.NewRedisCareDependencyVerifier(store, bucketForFarm)
	if err != nil {
		t.Fatal(err)
	}
	farmOpts := passingFarmParticipantOptions(farmRegistry, time.Now)
	farmOpts.PlayerSourceVerifier = playerSource
	farmServer, err := NewServerChecked(farmOpts)
	if err != nil {
		t.Fatal(err)
	}
	resolver := sourceOrderE2EResolver(func(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
		if err := ctx.Err(); err != nil {
			return actorrouting.Assignment{}, err
		}
		if address.Realm != "source-order-e2e" || address.ID != actorID {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		assignment, ok := assignments[address.Kind]
		if !ok {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		return assignment, nil
	})
	var proofIssuer *sourceorder.CommitProofIssuer
	var proofValidator *sourceorder.CommitProofValidator
	if proofMode != sourceorder.CommitProofModeOff {
		seed := sha256.Sum256([]byte("source-order-e2e-holder-boot-key"))
		private := ed25519.NewKeyFromSeed(seed[:])
		farmAssignment := assignments[actorrouting.DomainFarm]
		signer, signerErr := sourceorder.NewCommitProofSigner("source-order-e2e", farmAssignment.HolderPod,
			farmAssignment.HolderBootID, "source-order-e2e-key", private)
		if signerErr != nil {
			t.Fatal(signerErr)
		}
		proofIssuer, signerErr = sourceorder.NewCommitProofIssuer(signer, resolver, "source-order-e2e", 10*time.Second,
			time.Second, func() time.Time { return time.UnixMilli(proofClockMs.Load()) })
		if signerErr != nil {
			t.Fatal(signerErr)
		}
		keyring, keyErr := sourceorder.NewCommitProofKeyring([]sourceorder.CommitProofVerificationKey{{
			SourceRealm: "source-order-e2e", HolderID: farmAssignment.HolderPod, HolderBootID: farmAssignment.HolderBootID,
			KeyID: "source-order-e2e-key", PublicKey: private.Public().(ed25519.PublicKey),
			NotBeforeMs: now.Add(-time.Hour).UnixMilli(), NotAfterMs: farmAssignment.LeaseUntilMs,
		}}, 10*time.Second)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		routeVerifier, routeErr := sourceorder.NewLocalCommitProofRouteVerifier(resolver, "source-order-e2e")
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		proofValidator, keyErr = sourceorder.NewCommitProofValidator(keyring, routeVerifier,
			func() time.Time { return time.UnixMilli(proofClockMs.Load()) })
		if keyErr != nil {
			t.Fatal(keyErr)
		}
	}
	proofMetrics := &playerparticipant.CommitProofMetrics{}
	playerServer := playerparticipant.NewServer(playerparticipant.Options{
		Registry: playerRegistry,
		WorkVersionVerifier: playerparticipant.WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			return nil
		}),
		WorkLeaseVerifier: playerparticipant.WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			return nil
		}),
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		HarvestSourceVerifier:   harvestSource,
		CareDependencyVerifier:  careSource,
		CommitProofMode:         proofMode,
		CommitProofValidator:    proofValidator,
		CommitProofSourceRealm:  "source-order-e2e",
		CommitProofMetrics:      proofMetrics,
		TaskConfigRepository:    playertask.MustNewP0Repository(),
		TaskTimezone:            playertask.DefaultGameTimezone,
		NowMs:                   func() int64 { return proofClockMs.Load() },
		Now:                     time.Now,
	})
	playerTransport := &sourceOrderE2EPlayerTransport{server: playerServer}

	direct, err := carerecovery.NewDirectDispatcher(carerecovery.DirectConfig{
		WorkLease: store.WorkLease(), SourceOrders: store.WorkLease(),
		Route: func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
			if id != actorID {
				t.Fatalf("unexpected player route id=%d", id)
			}
			return proto.Clone(playerRoute).(*logicrpcpb.ActorRouteHeader), nil
		},
		Player: playerTransport,
		Farm:   coordinatorStepClient(farmServer.ExecuteStep),
		FarmRoute: func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
			if id != actorID {
				t.Fatalf("unexpected farm route id=%d", id)
			}
			return proto.Clone(farmRoute).(*logicrpcpb.ActorRouteHeader), nil
		},
		Owner: "source-order-e2e-direct", Token: "source-order-e2e-token", BootID: "source-order-e2e-direct-boot",
		BucketForFarm: bucketForFarm, BucketForPlayer: bucketForPlayer,
		LeaseTTL: 10 * time.Second, ItemDeadline: 5 * time.Second,
		Now:             func() time.Time { return time.UnixMilli(proofClockMs.Load()) },
		CommitProofMode: proofMode, CommitProofIssuer: proofIssuer,
	})
	if err != nil {
		t.Fatal(err)
	}

	service := NewService(farmRegistry, nil)
	service.now = func() time.Time { return time.UnixMilli(proofClockMs.Load()) }
	service.SetSourceCommitEvidenceEnabled(proofMode != sourceorder.CommitProofModeOff)
	service.SetPlayerCoordinator(func(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
		return playerTransport.ExecuteStep(ctx, req)
	}, func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if id != actorID {
			t.Fatalf("unexpected player coordinator id=%d", id)
		}
		return proto.Clone(playerRoute).(*logicrpcpb.ActorRouteHeader), nil
	})
	service.SetPlayerBucketResolver(bucketForPlayer)
	service.SetPlantOrderDispatcher(func(ctx context.Context, playerID uint64, orderID string) error {
		bucket, bucketErr := bucketForPlayer(playerID)
		if bucketErr != nil {
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, string(identity.WorkTypePlant), orderID, workidentity.RouteDomainPlayer, playerID)
		if addrErr != nil {
			return addrErr
		}
		return direct.DispatchOnce(ctx, address)
	})
	dispatchCommitted := func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
		bucket, bucketErr := bucketForFarm(farmID)
		if bucketErr != nil {
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, order.OrderType, order.OrderID, workidentity.RouteDomainFarm, farmID)
		if addrErr != nil {
			return addrErr
		}
		return direct.DispatchCommittedOnceWithEvidence(ctx, address, order, evidence)
	}
	service.SetHarvestOrderEnqueuer(dispatchCommitted)
	service.SetCareOrderEnqueuer(dispatchCommitted)

	rig := &sourceOrderE2E{
		actorID: actorID, redis: server, store: store, calculator: calculator, profile: profile,
		farmRegistry: farmRegistry, playerRegistry: playerRegistry, farmRoute: farmRoute, playerRoute: playerRoute,
		farm: farmServer, direct: direct, service: service, playerTransport: playerTransport,
		proofClockMs: proofClockMs, bucketForFarm: bucketForFarm,
	}
	return rig
}

func (rig *sourceOrderE2E) setNow(now time.Time) {
	rig.redis.SetTime(now)
	rig.proofClockMs.Store(now.UnixMilli())
}

func (rig *sourceOrderE2E) bucketFor(t *testing.T, domain actorrouting.RouteDomain) rediskey.AuthorityBucketRef {
	t.Helper()
	bucket, err := rig.profile.AuthorityBucketForActor(rig.calculator, actorrouting.ActorAddress{Domain: domain, ActorID: rig.actorID})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func (rig *sourceOrderE2E) dueIDs(t *testing.T, domain actorrouting.RouteDomain) []string {
	t.Helper()
	refs, err := rig.store.WorkLease().ListDueWork(context.Background(), rig.bucketFor(t, domain), 32)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.Ref.WorkType+":"+ref.Ref.WorkID)
	}
	return ids
}

func (rig *sourceOrderE2E) sourceOrder(t *testing.T, domain actorrouting.RouteDomain, workType, orderID string) redisauthority.SourceOrderSnapshot {
	t.Helper()
	snapshot, err := rig.store.WorkLease().GetSourceOrder(context.Background(), rig.bucketFor(t, domain), workType, orderID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (rig *sourceOrderE2E) seedCarrot(t *testing.T, count int64) {
	t.Helper()
	if _, err := rig.playerRegistry.Submit(context.Background(), rig.actorID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if !playeractor.AddItem(rec, playeractor.ItemCarrotSeed, count) {
			t.Fatalf("seed add failed")
		}
		rec.EconomyVersion++
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func (rig *sourceOrderE2E) playerItem(t *testing.T, itemID int32) int64 {
	t.Helper()
	record, _, err := rig.playerRegistry.Read(context.Background(), rig.actorID)
	if err != nil {
		t.Fatal(err)
	}
	return playeractor.ItemCount(record, itemID)
}

func (rig *sourceOrderE2E) playerState(t *testing.T) (*playerpb.PlayerRecord, uint64) {
	t.Helper()
	record, version, err := rig.playerRegistry.Read(context.Background(), rig.actorID)
	if err != nil {
		t.Fatal(err)
	}
	return proto.Clone(record).(*playerpb.PlayerRecord), version
}

func (rig *sourceOrderE2E) runRecovery(t *testing.T, family string) carerecovery.RoundStats {
	t.Helper()
	worker, err := carerecovery.NewWorker(carerecovery.Config{
		WorkLease: rig.store.WorkLease(), DueIndex: rig.store.WorkLease(),
		Buckets:       []rediskey.AuthorityBucketRef{rig.bucketFor(t, actorrouting.DomainFarm)},
		BucketForFarm: rig.bucketForFarm, Route: func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
			if id != rig.actorID {
				return nil, errors.New("source-order e2e: unexpected recovery player")
			}
			return proto.Clone(rig.playerRoute).(*logicrpcpb.ActorRouteHeader), nil
		},
		Player: rig.playerTransport, Owner: "source-order-e2e-worker", Token: "source-order-e2e-worker-token",
		BootID: "source-order-e2e-worker-boot", ClaimLimit: 8, MaxConcurrent: 1,
		LeaseTTL: 10 * time.Second, SafetyMargin: time.Second, OperationTimeout: 5 * time.Second,
		ParticipantBudgetMs: 4000, Now: time.Now, Family: family,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, runErr := worker.RunOnce(context.Background())
	if runErr != nil {
		t.Fatalf("actual %s recovery: stats=%+v err=%v", family, stats, runErr)
	}
	return stats
}

func (rig *sourceOrderE2E) farmSnapshot(t *testing.T, now time.Time) *farmpb.FarmSnapshot {
	t.Helper()
	value, err := rig.farmRegistry.Submit(context.Background(), rig.actorID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := value.(*farmpb.FarmSnapshot)
	if !ok || snapshot == nil {
		t.Fatal("farm snapshot missing")
	}
	return snapshot
}

func (rig *sourceOrderE2E) plotView(t *testing.T, snapshot *farmpb.FarmSnapshot, publicPlot uint32) *farmpb.PlotView {
	t.Helper()
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetPlotIndex() == publicPlot {
			return plot
		}
	}
	t.Fatalf("plot %d missing", publicPlot)
	return nil
}

func (rig *sourceOrderE2E) execute(t *testing.T, commandType commonpb.CommandType, operationID string, payload proto.Message) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	command := typedCommand(t, operationID, operationID+"-req", commandType, payload)
	trusted, dispatch := gateSideForTest(t, rig.actorID, "source-order-e2e-conn", command)
	response, err := rig.service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: command, Trusted: trusted, Dispatch: dispatch,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := response.GetBusiness()
	if outcome == nil {
		t.Fatalf("missing business outcome: %+v", response)
	}
	return outcome
}

func TestPlantSourceOrderPublicCommandCompletesFarmAndDue(t *testing.T) {
	t.Setenv("PLANT_SOURCE_ORDER", "true")
	const actorID = uint64(9101)
	rig := newSourceOrderE2E(t, actorID)
	rig.seedCarrot(t, 2)

	first := rig.execute(t, commonpb.CommandType_PLANT, "plant-source-first", &publicpb.PlantRequest{
		PlotId: 0, TargetFarmId: actorID,
	})
	if first.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first plant=%+v", first)
	}
	if rig.playerItem(t, playeractor.ItemCarrotSeed) != 1 {
		t.Fatalf("seeds after first plant=%d", rig.playerItem(t, playeractor.ItemCarrotSeed))
	}
	planted := rig.plotView(t, rig.farmSnapshot(t, time.UnixMilli(ingressTestNowMs)), 0)
	if planted.GetCropCycleId() == "" || planted.GetPlotVersion() <= 1 {
		t.Fatalf("farm plot after plant=%+v", planted)
	}
	orderID, err := sourceorder.PlantSourceOrderID(actorID, "plant-source-first")
	if err != nil {
		t.Fatal(err)
	}
	order := rig.sourceOrder(t, actorrouting.DomainPlayer, string(identity.WorkTypePlant), orderID)
	if order.State != "COMPLETED" || order.WorkID != orderID {
		t.Fatalf("plant source order=%+v", order)
	}
	if due := rig.dueIDs(t, actorrouting.DomainPlayer); len(due) != 0 {
		t.Fatalf("player due after plant complete=%v", due)
	}

	replay := rig.execute(t, commonpb.CommandType_PLANT, "plant-source-first", &publicpb.PlantRequest{
		PlotId: 0, TargetFarmId: actorID,
	})
	if replay.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("replay plant=%+v", replay)
	}
	if rig.playerItem(t, playeractor.ItemCarrotSeed) != 1 {
		t.Fatal("replay deducted a second seed")
	}

	second := rig.execute(t, commonpb.CommandType_PLANT, "plant-source-second", &publicpb.PlantRequest{
		PlotId: 0, TargetFarmId: actorID,
	})
	if second.GetStatus() != commonpb.CommandStatus_REJECTED ||
		second.GetError() == nil || second.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY {
		t.Fatalf("second plant=%+v", second)
	}
	if rig.playerItem(t, playeractor.ItemCarrotSeed) != 1 {
		t.Fatal("rejected second plant deducted another seed")
	}
}

func TestHarvestSourceOrderPublicCommandGrantsAndClearsDue(t *testing.T) {
	t.Setenv("HARVEST_SOURCE_ORDER", "true")
	const actorID = uint64(9102)
	rig := newSourceOrderE2E(t, actorID)

	plantPayload, workflowID := ingressTestPayload(t, actorID, "harvest-source-setup-plant", 1)
	plantResponse, err := rig.farm.ExecuteStep(context.Background(), ingressTestRequest(t, rig.farmRoute, actorID, "harvest-source-setup-plant", plantPayload, workflowID))
	if err != nil || plantResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("setup plant=%+v err=%v", plantResponse, err)
	}
	var plantResult contractspb.CommitPlantResult
	if err := proto.Unmarshal(plantResponse.GetTypedResult(), &plantResult); err != nil {
		t.Fatal(err)
	}
	harvestNow := time.UnixMilli(ingressTestNowMs + 70_000)
	rig.redis.SetTime(harvestNow)
	rig.service.now = func() time.Time { return harvestNow }

	var harvestDispatchErr error
	rig.service.SetHarvestOrderEnqueuer(func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
		bucket, bucketErr := rig.profile.AuthorityBucketForActor(rig.calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
		if bucketErr != nil {
			harvestDispatchErr = bucketErr
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, string(identity.WorkTypeHarvest), order.OrderID, workidentity.RouteDomainFarm, farmID)
		if addrErr != nil {
			harvestDispatchErr = addrErr
			return addrErr
		}
		harvestDispatchErr = rig.direct.DispatchCommittedOnceWithEvidence(ctx, address, order, evidence)
		return harvestDispatchErr
	})

	harvested := rig.execute(t, commonpb.CommandType_HARVEST, "harvest-source-first", &publicpb.HarvestRequest{
		PlotId: 0, TargetFarmId: actorID, CropCycleId: plantResult.GetCropCycleId(),
		ExpectedPlotVersion: plantResult.GetCommittedPlotVersion(),
	})
	if harvested.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("harvest=%+v dispatch=%v", harvested, harvestDispatchErr)
	}
	if harvestDispatchErr != nil {
		t.Fatalf("harvest Direct: %v", harvestDispatchErr)
	}
	empty := rig.plotView(t, rig.farmSnapshot(t, harvestNow), 0)
	if empty.GetCropCycleId() != "" {
		t.Fatalf("plot still occupied after harvest=%+v", empty)
	}
	var publicResult publicpb.HarvestResult
	if err := proto.Unmarshal(harvested.GetResultPayload(), &publicResult); err != nil {
		t.Fatal(err)
	}
	if publicResult.GetTargetFarmId() != actorID || publicResult.GetCropCycleId() != plantResult.GetCropCycleId() ||
		publicResult.GetHarvestedQuantity() == 0 {
		t.Fatalf("public harvest=%+v", &publicResult)
	}
	orderID, err := farmactor.HarvestSourceOrderID(actorID, "harvest-source-first")
	if err != nil {
		t.Fatal(err)
	}
	order := rig.sourceOrder(t, actorrouting.DomainFarm, string(identity.WorkTypeHarvest), orderID)
	if order.State != "COMPLETED" || order.WorkID != orderID {
		t.Fatalf("harvest source order=%+v", order)
	}
	if due := rig.dueIDs(t, actorrouting.DomainFarm); len(due) != 0 {
		t.Fatalf("farm due after harvest complete=%v", due)
	}
	if rig.playerItem(t, playeractor.ItemCarrot) == 0 {
		t.Fatal("player was not granted harvest produce")
	}
}

func TestHarvestSourceOrderAllowsSequentialDistinctPlots(t *testing.T) {
	t.Setenv("PLANT_SOURCE_ORDER", "true")
	t.Setenv("HARVEST_SOURCE_ORDER", "true")
	const actorID = uint64(9103)
	rig := newSourceOrderE2E(t, actorID)
	rig.seedCarrot(t, 2)

	plant := func(operationID string, publicPlotID uint32) *farmpb.PlotView {
		t.Helper()
		outcome := rig.execute(t, commonpb.CommandType_PLANT, operationID, &publicpb.PlantRequest{
			PlotId: publicPlotID, TargetFarmId: actorID,
		})
		if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("plant plot %d=%+v", publicPlotID, outcome)
		}
		planted := rig.plotView(t, rig.farmSnapshot(t, time.UnixMilli(ingressTestNowMs)), publicPlotID)
		if planted.GetCropCycleId() == "" || planted.GetPlotVersion() <= 1 {
			t.Fatalf("planted plot %d=%+v", publicPlotID, planted)
		}
		return proto.Clone(planted).(*farmpb.PlotView)
	}

	firstPlant := plant("harvest-sequential-setup-1", 0)
	secondPlant := plant("harvest-sequential-setup-2", 1)
	if got := rig.playerItem(t, playeractor.ItemCarrotSeed); got != 0 {
		t.Fatalf("seeds after two public plants=%d, want 0", got)
	}
	harvestNow := time.UnixMilli(ingressTestNowMs + 70_000)
	rig.redis.SetTime(harvestNow)
	rig.service.now = func() time.Time { return harvestNow }

	harvest := func(operationID string, publicPlotID uint32, planted *farmpb.PlotView) *publicpb.HarvestResult {
		t.Helper()
		outcome := rig.execute(t, commonpb.CommandType_HARVEST, operationID, &publicpb.HarvestRequest{
			PlotId: publicPlotID, TargetFarmId: actorID, CropCycleId: planted.GetCropCycleId(),
			ExpectedPlotVersion: planted.GetPlotVersion(),
		})
		if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("harvest plot %d=%+v", publicPlotID, outcome)
		}
		result := &publicpb.HarvestResult{}
		if err := proto.Unmarshal(outcome.GetResultPayload(), result); err != nil {
			t.Fatal(err)
		}
		return result
	}

	firstHarvest := harvest("harvest-sequential-1", 0, firstPlant)
	secondHarvest := harvest("harvest-sequential-2", 1, secondPlant)
	if secondHarvest.GetCommittedFarmVersion() != firstHarvest.GetCommittedFarmVersion()+1 {
		t.Fatalf("farm revisions are not continuous: first=%d second=%d",
			firstHarvest.GetCommittedFarmVersion(), secondHarvest.GetCommittedFarmVersion())
	}
	snapshot := rig.farmSnapshot(t, harvestNow)
	if first := rig.plotView(t, snapshot, 0); first.GetCropCycleId() != "" {
		t.Fatalf("first plot still occupied: %+v", first)
	}
	if second := rig.plotView(t, snapshot, 1); second.GetCropCycleId() != "" {
		t.Fatalf("second plot still occupied: %+v", second)
	}
	if got := rig.playerItem(t, playeractor.ItemCarrot); got != 1000 {
		t.Fatalf("two harvest rewards=%d, want 1000", got)
	}
	for _, operationID := range []string{"harvest-sequential-1", "harvest-sequential-2"} {
		orderID, err := farmactor.HarvestSourceOrderID(actorID, operationID)
		if err != nil {
			t.Fatal(err)
		}
		if order := rig.sourceOrder(t, actorrouting.DomainFarm, string(identity.WorkTypeHarvest), orderID); order.State != "COMPLETED" {
			t.Fatalf("harvest order %s=%+v", operationID, order)
		}
	}
	if due := rig.dueIDs(t, actorrouting.DomainFarm); len(due) != 0 {
		t.Fatalf("farm due after two harvests=%v", due)
	}
}

func sourceOrderE2ESetupPlant(t *testing.T, rig *sourceOrderE2E, operationID string) *contractspb.CommitPlantResult {
	t.Helper()
	payload, workflowID := ingressTestPayload(t, rig.actorID, operationID, 1)
	response, err := rig.farm.ExecuteStep(context.Background(), ingressTestRequest(t, rig.farmRoute, rig.actorID, operationID, payload, workflowID))
	if err != nil || response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("setup plant=%+v err=%v", response, err)
	}
	result := &contractspb.CommitPlantResult{}
	if err := proto.Unmarshal(response.GetTypedResult(), result); err != nil {
		t.Fatal(err)
	}
	return result
}

func issueProofE2EOrder(t *testing.T, rig *sourceOrderE2E, family, operationID string, plant *contractspb.CommitPlantResult) string {
	t.Helper()
	switch family {
	case string(identity.WorkTypeCareReward):
		now := time.UnixMilli(ingressTestNowMs + 10_000)
		rig.setNow(now)
		incidentID, err := identity.CareIncidentID(plant.GetCropCycleId(), "WATER", 1)
		if err != nil {
			t.Fatal(err)
		}
		outcome := rig.execute(t, commonpb.CommandType_CARE, operationID, &publicpb.CareRequest{
			PlotId: 0, TargetFarmId: rig.actorID, CropCycleId: plant.GetCropCycleId(), IncidentId: incidentID,
			CareType: publicpb.CareType_CARE_TYPE_WATER,
		})
		if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("CARE source commit=%+v", outcome)
		}
		var result publicpb.CareResult
		if err := proto.Unmarshal(outcome.GetResultPayload(), &result); err != nil {
			t.Fatal(err)
		}
		return result.GetCareRewardOrderId()
	case string(identity.WorkTypeHarvest):
		now := time.UnixMilli(ingressTestNowMs + 70_000)
		rig.setNow(now)
		outcome := rig.execute(t, commonpb.CommandType_HARVEST, operationID, &publicpb.HarvestRequest{
			PlotId: 0, TargetFarmId: rig.actorID, CropCycleId: plant.GetCropCycleId(),
			ExpectedPlotVersion: plant.GetCommittedPlotVersion(),
		})
		if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("HARVEST source commit=%+v", outcome)
		}
		orderID, err := farmactor.HarvestSourceOrderID(rig.actorID, operationID)
		if err != nil {
			t.Fatal(err)
		}
		return orderID
	default:
		t.Fatalf("unsupported proof e2e family %q", family)
		return ""
	}
}

func TestCommitProofInvalidCAREAndHARVESTDoNotMutatePlayerThenActualWorkerRunsOnce(t *testing.T) {
	families := []string{string(identity.WorkTypeCareReward), string(identity.WorkTypeHarvest)}
	for index, family := range families {
		t.Run(family, func(t *testing.T) {
			rig := newSourceOrderE2EWithProofMode(t, uint64(9200+index), sourceorder.CommitProofModePrefer)
			plant := sourceOrderE2ESetupPlant(t, rig, "invalid-proof-setup-"+family)
			before, beforeVersion := rig.playerState(t)
			rig.playerTransport.fault(true, false, false)
			orderID := issueProofE2EOrder(t, rig, family, "invalid-proof-order-"+family, plant)
			if stats := rig.direct.Stats(); stats.ProofAttached != 1 || stats.ProofUnavailable != 0 {
				t.Fatalf("invalid-proof test did not tamper a minted proof: %+v", stats)
			}

			afterReject, afterRejectVersion := rig.playerState(t)
			if afterRejectVersion != beforeVersion || !proto.Equal(afterReject, before) {
				t.Fatalf("invalid proof mutated Player: version=%d/%d before=%+v after=%+v", beforeVersion, afterRejectVersion, before, afterReject)
			}
			order := rig.sourceOrder(t, actorrouting.DomainFarm, family, orderID)
			if order.State != "PENDING" || len(rig.dueIDs(t, actorrouting.DomainFarm)) != 1 {
				t.Fatalf("invalid proof removed durable recovery work: order=%+v due=%v", order, rig.dueIDs(t, actorrouting.DomainFarm))
			}

			stats := rig.runRecovery(t, family)
			if stats.Claimed != 1 || stats.Completed != 1 {
				t.Fatalf("actual Worker stats=%+v", stats)
			}
			afterWorker, workerVersion := rig.playerState(t)
			if workerVersion != beforeVersion+1 || len(afterWorker.GetRecentOperationReceipts()) != len(before.GetRecentOperationReceipts())+1 {
				t.Fatalf("Worker did not commit exactly one Player receipt/version: before_v=%d after_v=%d receipts=%d/%d",
					beforeVersion, workerVersion, len(before.GetRecentOperationReceipts()), len(afterWorker.GetRecentOperationReceipts()))
			}
			second := rig.runRecovery(t, family)
			coordinatorCalls, recoveryCalls, _ := rig.playerTransport.counts()
			if second.Claimed != 0 || second.Completed != 0 || coordinatorCalls != 1 || recoveryCalls != 1 {
				t.Fatalf("fallback was not exactly once: second=%+v coordinator=%d recovery=%d", second, coordinatorCalls, recoveryCalls)
			}
			finalRecord, finalVersion := rig.playerState(t)
			if finalVersion != workerVersion || !proto.Equal(finalRecord, afterWorker) {
				t.Fatal("empty second recovery mutated Player")
			}
		})
	}
}

func TestCommitProofCAREAndHARVESTResponseLossCompleteThroughWorkerReceiptReplay(t *testing.T) {
	families := []string{string(identity.WorkTypeCareReward), string(identity.WorkTypeHarvest)}
	for index, family := range families {
		t.Run(family, func(t *testing.T) {
			rig := newSourceOrderE2EWithProofMode(t, uint64(9210+index), sourceorder.CommitProofModePrefer)
			plant := sourceOrderE2ESetupPlant(t, rig, "proof-response-loss-setup-"+family)
			before, beforeVersion := rig.playerState(t)
			rig.playerTransport.fault(false, false, true)
			orderID := issueProofE2EOrder(t, rig, family, "proof-response-loss-order-"+family, plant)
			if stats := rig.direct.Stats(); stats.ProofAttached != 1 || stats.ProofUnavailable != 0 {
				t.Fatalf("response-loss test did not send a valid minted proof: %+v", stats)
			}
			afterDirect, directVersion := rig.playerState(t)
			if directVersion != beforeVersion+1 || len(afterDirect.GetRecentOperationReceipts()) != len(before.GetRecentOperationReceipts())+1 {
				t.Fatalf("lost response did not follow Player commit: version=%d/%d receipts=%d/%d", beforeVersion, directVersion,
					len(before.GetRecentOperationReceipts()), len(afterDirect.GetRecentOperationReceipts()))
			}
			if order := rig.sourceOrder(t, actorrouting.DomainFarm, family, orderID); order.State != "PENDING" {
				t.Fatalf("response loss prematurely completed source order: %+v", order)
			}
			stats := rig.runRecovery(t, family)
			if stats.Completed != 1 || stats.Claimed != 1 {
				t.Fatalf("response-loss Worker stats=%+v", stats)
			}
			afterReplay, replayVersion := rig.playerState(t)
			_, recoveryCalls, replayed := rig.playerTransport.counts()
			coordinatorInvocation, recoveryInvocation := rig.playerTransport.invocations()
			if replayVersion != directVersion || !proto.Equal(afterReplay, afterDirect) || recoveryCalls != 1 || replayed != 1 ||
				coordinatorInvocation == "" || coordinatorInvocation != recoveryInvocation {
				t.Fatalf("receipt replay was not exact/mutation-free: versions=%d/%d recovery=%d replayed=%d invocations=%q/%q",
					directVersion, replayVersion, recoveryCalls, replayed, coordinatorInvocation, recoveryInvocation)
			}
			if due := rig.dueIDs(t, actorrouting.DomainFarm); len(due) != 0 {
				t.Fatalf("response-loss replay did not complete DueIndex: %v", due)
			}
		})
	}
}

func TestCommitProofShadowAcceptsMixedVersionCAREAndHARVESTWithoutProofViaRedis(t *testing.T) {
	families := []string{string(identity.WorkTypeCareReward), string(identity.WorkTypeHarvest)}
	for index, family := range families {
		t.Run(family, func(t *testing.T) {
			rig := newSourceOrderE2EWithProofMode(t, uint64(9220+index), sourceorder.CommitProofModeShadow)
			plant := sourceOrderE2ESetupPlant(t, rig, "proof-mixed-version-setup-"+family)
			before, beforeVersion := rig.playerState(t)
			rig.playerTransport.fault(false, true, false)
			orderID := issueProofE2EOrder(t, rig, family, "proof-mixed-version-order-"+family, plant)
			if stats := rig.direct.Stats(); stats.ProofAttached != 1 || stats.ProofUnavailable != 0 {
				t.Fatalf("mixed-version test did not strip a minted new-version proof: %+v", stats)
			}
			after, afterVersion := rig.playerState(t)
			if afterVersion != beforeVersion+1 || len(after.GetRecentOperationReceipts()) != len(before.GetRecentOperationReceipts())+1 {
				t.Fatalf("shadow old coordinator did not use authoritative Redis: version=%d/%d receipts=%d/%d", beforeVersion, afterVersion,
					len(before.GetRecentOperationReceipts()), len(after.GetRecentOperationReceipts()))
			}
			if order := rig.sourceOrder(t, actorrouting.DomainFarm, family, orderID); order.State != "COMPLETED" {
				t.Fatalf("mixed-version %s order=%+v", family, order)
			}
		})
	}
}
