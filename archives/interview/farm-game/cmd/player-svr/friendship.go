package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/friendshipservice"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/publicprofile"
	"farmgame/internal/socialprofile"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type unavailableOpportunityEvaluator struct{}

func (unavailableOpportunityEvaluator) BatchEvaluateFriendOpportunity(context.Context, uint64, []uint64, int64) ([]friend.FriendOpportunityEvaluation, error) {
	return nil, fmt.Errorf("%w: FARM_ADDR is not configured", friendshipservice.ErrUnavailable)
}

// buildPlayerFriendship composes one stateless application object per healthy
// player-svr instance. All durable state remains in the shared repositories;
// the object is not registered with PlayerActor or any mailbox.
func buildPlayerFriendship(client redis.UniversalClient, repositories *friend.RedisFriendshipRepositories,
	players *playeringress.Service, registerer prometheus.Registerer) (*friendshipservice.RPCServer, func(), error) {
	nop := func() {}
	if client == nil || repositories == nil || players == nil {
		return nil, nop, friendshipservice.ErrInvalidArgument
	}
	scriptCtx, scriptCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := repositories.EnsureScriptsLoaded(scriptCtx)
	scriptCancel()
	if err != nil {
		return nil, nop, fmt.Errorf("load player Friendship authority scripts: %w", err)
	}
	domain, err := friend.NewDomainService(repositories.Pair, repositories.Quota, repositories.Share)
	if err != nil {
		return nil, nop, err
	}
	profile := socialprofile.Current()
	projectionMetrics, err := friendshipservice.NewPrometheusProjectionMetrics(registerer)
	if err != nil {
		return nil, nop, err
	}
	repairQueue := friendshipservice.NewInstrumentedRepairQueue(repositories.RepairQueue, projectionMetrics)
	shares, err := friendshipservice.NewRedisShareProjectionStore(client,
		env("SHARE_STORE_PREFIX", defaultShareStorePrefix()), profile.ShareTokenTTL)
	if err != nil {
		return nil, nop, err
	}
	profileReader, err := friendshipservice.NewLocalPlayerProfileReader(players)
	if err != nil {
		return nil, nop, err
	}
	batchProfiles, err := publicprofile.NewBatchReader(profileReader, 8)
	if err != nil {
		return nil, nop, err
	}

	var farmConnection *grpc.ClientConn
	var opportunities friend.FriendOpportunityBatchEvaluator = unavailableOpportunityEvaluator{}
	var opportunityEvaluator *friend.GRPCOpportunityEvaluator
	if farmAddr := strings.TrimSpace(os.Getenv("FARM_ADDR")); farmAddr != "" {
		farmConnection, err = grpc.Dial(farmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nop, fmt.Errorf("dial farm-svr Friendship read dependency %s: %w", farmAddr, err)
		}
		opportunityEvaluator, err = friend.NewGRPCOpportunityEvaluator(logicrpcpb.NewLogicServiceClient(farmConnection),
			envDuration("FRIENDSHIP_FARM_RPC_TIMEOUT", 2*time.Second))
		if err != nil {
			_ = farmConnection.Close()
			return nil, nop, err
		}
		opportunities = opportunityEvaluator
	} else {
		log.Printf("player-svr Friendship list reads fail closed until FARM_ADDR is configured")
	}
	navigationV3Enabled := strings.TrimSpace(os.Getenv("FARM_FRIEND_NAVIGATION_V3_ENABLED")) == "1"
	navigationWindowMode := friend.ParseNavigationWindowMode(os.Getenv("FRIEND_NAVIGATION_WINDOW_MODE"))
	navigationWindowSize := 0
	// Parse and validate the top-k window size only when Navigator V3 and
	// top-k mode are actually enabled; an invalid size must never fail a
	// full-tail/default startup.
	if navigationV3Enabled && navigationWindowMode == friend.NavigationWindowModeTopK {
		if raw := strings.TrimSpace(os.Getenv("FRIEND_NAVIGATION_WINDOW_SIZE")); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil {
				return nil, nop, fmt.Errorf("FRIEND_NAVIGATION_WINDOW_SIZE: %w", parseErr)
			}
			navigationWindowSize = parsed
		}
		if navigationWindowSize <= 0 || navigationWindowSize > friend.MaxTopKWindowSize {
			navigationWindowSize = friend.DefaultTopKWindowSize
		}
	} else {
		navigationWindowMode = friend.NavigationWindowModeFullTail
	}
	// The repository shares the legacy CAS fence with the visited-mark ingress
	// and must know whether to read/publish the top-k record. farm-svr keeps
	// the full-tail default and never touches the top-k key.
	repositories.Navigation.ConfigureNavigationWindowMode(navigationWindowMode, navigationWindowSize)
	var navigator *friend.Navigator
	if navigationV3Enabled {
		if opportunityEvaluator == nil {
			return nil, nop, fmt.Errorf("FARM_FRIEND_NAVIGATION_V3_ENABLED requires FARM_ADDR")
		}
		scanBatch := envInt("FRIEND_OPPORTUNITY_SCAN_BATCH_SIZE", friend.DefaultScanBatchSize)
		navigator, err = friend.NewNavigator(opportunityEvaluator, opportunityEvaluator, scanBatch)
		if err != nil {
			_ = farmConnection.Close()
			return nil, nop, err
		}
	}
	log.Printf("friendship opportunity navigation v3_enabled=%t window_mode=%s window_size=%d scan_batch=%d farm_rpc=%t",
		navigationV3Enabled, navigationWindowMode, navigationWindowSize,
		envInt("FRIEND_OPPORTUNITY_SCAN_BATCH_SIZE", friend.DefaultScanBatchSize), opportunityEvaluator != nil)
	read, err := friend.NewReadAPI(repositories.Index, repositories.Pair, batchProfiles, opportunities,
		func() int64 { return time.Now().UnixMilli() })
	if err != nil {
		if farmConnection != nil {
			_ = farmConnection.Close()
		}
		return nil, nop, err
	}
	if navigator != nil {
		// Cold popup sorts FriendIndex Headers first; Pair Authority is only
		// consulted for the Top-K exact window inside Navigator.
		navigator.Relations = read
	}
	reconciler, err := friend.NewFriendIndexReconciler(repositories.Index, repositories.Pair, repositories.Quota)
	if err != nil {
		if farmConnection != nil {
			_ = farmConnection.Close()
		}
		return nil, nop, err
	}
	repairCtx, repairCancel := context.WithCancel(context.Background())
	go friend.RunFriendIndexRepairWorker(repairCtx, repairQueue, reconciler,
		500*time.Millisecond, 10*time.Second, 2*time.Second, time.Second,
		func(workerErr error) { log.Printf("player Friendship index repair: %v", workerErr) },
		friendshipservice.RecordFriendIndexReconcileReport(projectionMetrics))

	application, err := friendshipservice.NewService(friendshipservice.ServiceOptions{
		Shares: shares, Domain: domain, Index: repositories.Index,
		Read: read, RepairQueue: repairQueue, Navigation: repositories.Navigation,
		Navigator: navigator, NavigationState: repositories.Navigation, NavigationV3Enabled: navigationV3Enabled,
		NavigationWindowMode: navigationWindowMode, NavigationWindowSize: navigationWindowSize,
		Profiles: profileReader, FriendLimit: profile.EffectiveMaxFriends,
	})
	if err != nil {
		repairCancel()
		if farmConnection != nil {
			_ = farmConnection.Close()
		}
		return nil, nop, err
	}
	rpcServer, err := friendshipservice.NewRPCServer(application)
	if err != nil {
		repairCancel()
		if farmConnection != nil {
			_ = farmConnection.Close()
		}
		return nil, nop, err
	}
	return rpcServer, func() {
		repairCancel()
		if farmConnection != nil {
			_ = farmConnection.Close()
		}
	}, nil
}

func defaultShareStorePrefix() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("FARM_APP_PROFILE")), "showcase") {
		return "farm:v121:m2:primary:share"
	}
	return "farm:v121:m2:dev:share"
}
