package gatewayapp

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	friendpb "farmgame/api/gen/friend"
	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/friend"
	"farmgame/internal/friendshipservice"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// singlePairReader deliberately hides PairBatchReader so rollback can retain
// the canonical bounded GetPair validation path without changing authority data.
type singlePairReader struct{ friend.PairRepository }

type gatewayFriendshipNotifier interface {
	auth.FriendRosterNotifier
	friend.FriendOpportunityNotifier
}

type countingOpportunityNotifier struct {
	inner     friend.FriendOpportunityNotifier
	delivered int
}

func (n *countingOpportunityNotifier) NotifyFriendOpportunityChanged(playerID, ownerID, farmVersion, revision uint64) int {
	delivered := n.inner.NotifyFriendOpportunityChanged(playerID, ownerID, farmVersion, revision)
	n.delivered += delivered
	return delivered
}

type gatewayFriendshipOptions struct {
	Mode             friendshipservice.Mode
	Authority        redis.UniversalClient
	Projection       redis.UniversalClient
	Identities       auth.HTTPIdentityResolver
	Profiles         publicprofile.Reader
	FarmConnection   *grpc.ClientConn
	PlayerConnection *grpc.ClientConn
	Notifier         gatewayFriendshipNotifier
	ShareStorePrefix string
	FriendLimit      int
	ShareTokenTTL    time.Duration
	RPCDeadline      time.Duration
}

type friendshipClientFactory func() (friendshipservice.Client, func(), error)

// selectGatewayFriendshipClient is intentionally separated from construction.
// Its remote_player branch cannot accidentally allocate Redis repositories,
// load Lua, or start a repair worker because it never invokes localFactory.
func selectGatewayFriendshipClient(mode friendshipservice.Mode, localFactory, remoteFactory friendshipClientFactory,
	recorder friendshipservice.ShadowRecorder) (friendshipservice.Client, func(), error) {
	nop := func() {}
	switch mode {
	case friendshipservice.ModeLocalGate:
		if localFactory == nil {
			return nil, nop, friendshipservice.ErrInvalidArgument
		}
		return localFactory()
	case friendshipservice.ModeRemotePlayer:
		if remoteFactory == nil {
			return nil, nop, friendshipservice.ErrInvalidArgument
		}
		return remoteFactory()
	case friendshipservice.ModeRemoteShadow:
		if localFactory == nil || remoteFactory == nil || recorder == nil {
			return nil, nop, friendshipservice.ErrInvalidArgument
		}
		formal, stopFormal, err := localFactory()
		if err != nil {
			return nil, nop, err
		}
		observed, stopObserved, err := remoteFactory()
		if err != nil {
			stopFormal()
			return nil, nop, err
		}
		shadow, err := friendshipservice.NewShadowClient(formal, observed, recorder)
		if err != nil {
			stopObserved()
			stopFormal()
			return nil, nop, err
		}
		return shadow, func() { stopObserved(); stopFormal() }, nil
	default:
		return nil, nop, fmt.Errorf("%w: FriendshipService mode %q", friendshipservice.ErrInvalidArgument, mode)
	}
}

func buildGatewayFriendship(options gatewayFriendshipOptions) (*auth.ShareHandler, func(), error) {
	nop := func() {}
	if options.Identities == nil || options.FriendLimit <= 0 || options.ShareTokenTTL <= 0 || options.RPCDeadline <= 0 {
		return nil, nop, friendshipservice.ErrInvalidArgument
	}
	localFactory := func() (friendshipservice.Client, func(), error) {
		return buildLocalGatewayFriendship(options)
	}
	remoteFactory := func() (friendshipservice.Client, func(), error) {
		if options.PlayerConnection == nil {
			return nil, nop, fmt.Errorf("%w: player-svr FriendshipService connection", friendshipservice.ErrUnavailable)
		}
		client, err := friendshipservice.NewGRPCClient(
			friendshiprpcpb.NewFriendshipServiceClient(options.PlayerConnection), options.RPCDeadline)
		return client, nop, err
	}
	var recorder friendshipservice.ShadowRecorder
	if options.Mode == friendshipservice.ModeRemoteShadow {
		metrics, err := friendshipservice.NewPrometheusShadowRecorder(prometheus.DefaultRegisterer)
		if err != nil {
			return nil, nop, err
		}
		recorder = metrics
	}
	client, cleanup, err := selectGatewayFriendshipClient(options.Mode, localFactory, remoteFactory, recorder)
	if err != nil {
		return nil, nop, err
	}
	handler, err := auth.NewFriendshipShareHandler(options.Identities, client, options.FriendLimit)
	if err != nil {
		cleanup()
		return nil, nop, err
	}
	handler.SetFriendRosterNotifier(options.Notifier)
	return handler, cleanup, nil
}

func buildLocalGatewayFriendship(options gatewayFriendshipOptions) (friendshipservice.Client, func(), error) {
	nop := func() {}
	if options.Authority == nil || options.Projection == nil || options.Profiles == nil || options.FarmConnection == nil || options.Notifier == nil {
		return nil, nop, fmt.Errorf("%w: local Gateway Friendship dependencies", friendshipservice.ErrUnavailable)
	}
	shares, err := friendshipservice.NewRedisShareProjectionStore(options.Projection, options.ShareStorePrefix, options.ShareTokenTTL)
	if err != nil {
		return nil, nop, err
	}

	namespace, err := gatewayRedisNamespace()
	if err != nil {
		return nil, nop, err
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForDomains(options.Authority, options.Projection, friend.RedisFriendshipOptions{
		Profile: friend.DefaultFriendRedisProfile(namespace),
		Limits:  friend.FriendRedisLimits{MaxFriends: options.FriendLimit, ShareTokenTTLMs: options.ShareTokenTTL.Milliseconds()},
	})
	if err != nil {
		return nil, nop, err
	}
	projectionMetrics, err := friendshipservice.NewPrometheusProjectionMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return nil, nop, err
	}
	repairQueue := friendshipservice.NewInstrumentedRepairQueue(repositories.RepairQueue, projectionMetrics)
	scriptCtx, scriptCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = repositories.EnsureScriptsLoaded(scriptCtx)
	scriptCancel()
	if err != nil {
		return nil, nop, err
	}
	domain, err := friend.NewDomainService(repositories.Pair, repositories.Quota, repositories.Share)
	if err != nil {
		return nil, nop, err
	}
	profiles, err := publicprofile.NewBatchReader(options.Profiles, 8)
	if err != nil {
		return nil, nop, err
	}
	opportunities, err := friend.NewGRPCOpportunityEvaluator(
		logicrpcpb.NewLogicServiceClient(options.FarmConnection), 2*time.Second)
	if err != nil {
		return nil, nop, err
	}
	pairReader := friend.PairRepository(repositories.Pair)
	if os.Getenv("FARM_FRIEND_PAIR_BATCH_ENABLED") == "0" {
		pairReader = singlePairReader{PairRepository: repositories.Pair}
	}
	read, err := friend.NewReadAPI(repositories.Index, pairReader, profiles, opportunities,
		func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		return nil, nop, err
	}
	reconciler, err := friend.NewFriendIndexReconciler(repositories.Index, repositories.Pair, repositories.Quota)
	if err != nil {
		return nil, nop, err
	}
	repairCtx, repairCancel := context.WithCancel(context.Background())
	go friend.RunFriendIndexRepairWorker(repairCtx, repairQueue, reconciler,
		500*time.Millisecond, 10*time.Second, 2*time.Second, time.Second,
		func(workerErr error) { log.Printf("friend index repair: %v", workerErr) },
		friendshipservice.RecordFriendIndexReconcileReport(projectionMetrics))

	hintBus, err := friend.NewRedisOpportunityHintBus(options.Projection, namespace)
	if err != nil {
		repairCancel()
		return nil, nop, err
	}
	hintCtx, hintCancel := context.WithCancel(context.Background())
	go friend.RunOpportunityHintSubscriber(hintCtx, hintBus,
		func(ctx context.Context, hint *friendpb.FriendOpportunityChanged) error {
			return friend.FanoutFriendOpportunityHint(ctx, repositories.Index, options.Notifier, hint)
		}, func(subscribeErr error) { log.Printf("friend opportunity hint subscriber: %v", subscribeErr) })

	service, err := friendshipservice.NewService(friendshipservice.ServiceOptions{
		Shares: shares, Domain: domain, Index: repositories.Index,
		Read: read, RepairQueue: repairQueue, Navigation: repositories.Navigation,
		Profiles: options.Profiles, FriendLimit: options.FriendLimit,
	})
	if err != nil {
		hintCancel()
		repairCancel()
		return nil, nop, err
	}
	return service, func() { hintCancel(); repairCancel() }, nil
}

func gatewayRedisNamespace() (rediskey.Namespace, error) {
	environment := strings.TrimSpace(os.Getenv("REDIS_NAMESPACE_ENVIRONMENT"))
	if environment == "" {
		environment = "dev"
	}
	zone := strings.TrimSpace(os.Getenv("REDIS_NAMESPACE_ZONE"))
	if zone == "" {
		zone = "z1"
	}
	return rediskey.NewNamespace(environment, zone)
}

// startGatewayTargetedOpportunityHintSubscriber is transport composition,
// not friendship authority composition. Every Gate must consume the targeted
// invalidation channel and filter it against its local connections, including
// when FriendshipService authority has moved to player-svr (remote_player).
func startGatewayTargetedOpportunityHintSubscriber(redisClient redis.UniversalClient,
	notifier friend.FriendOpportunityNotifier) (func(), error) {
	if redisClient == nil || notifier == nil {
		return func() {}, fmt.Errorf("%w: targeted opportunity Hint dependencies", friendshipservice.ErrUnavailable)
	}
	namespace, err := gatewayRedisNamespace()
	if err != nil {
		return func() {}, err
	}
	hintBus, err := friend.NewRedisOpportunityHintBus(redisClient, namespace)
	if err != nil {
		return func() {}, err
	}
	hintCtx, hintCancel := context.WithCancel(context.Background())
	diagnostics := strings.EqualFold(strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_HINT_DIAGNOSTICS")), "true") ||
		strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_HINT_DIAGNOSTICS")) == "1"
	go friend.RunTargetedOpportunityHintSubscriber(hintCtx, hintBus,
		func(_ context.Context, hint friend.TargetedOpportunityHint) error {
			counter := &countingOpportunityNotifier{inner: notifier}
			err := friend.DispatchTargetedOpportunityHint(counter, hint)
			if diagnostics {
				log.Printf("targeted friend opportunity Hint viewers=%d local_deliveries=%d err=%v",
					len(hint.ViewerPlayerIDs), counter.delivered, err)
			}
			return err
		}, func(subscribeErr error) { log.Printf("targeted friend opportunity hint subscriber: %v", subscribeErr) })
	return hintCancel, nil
}
