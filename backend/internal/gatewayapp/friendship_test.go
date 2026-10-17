package gatewayapp

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/friendshipservice"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type assemblyTestClient struct{}

type targetedHintNotification struct {
	playerID, ownerID, farmVersion, revision uint64
}

type targetedHintTestNotifier struct {
	notifications chan targetedHintNotification
}

func (n *targetedHintTestNotifier) NotifyFriendOpportunityChanged(playerID, ownerID, farmVersion, revision uint64) int {
	n.notifications <- targetedHintNotification{playerID: playerID, ownerID: ownerID, farmVersion: farmVersion, revision: revision}
	return 1
}

func (*assemblyTestClient) CreateShare(context.Context, friendshipservice.CreateShareInput) (friendshipservice.CreateShareResult, error) {
	return friendshipservice.CreateShareResult{}, nil
}
func (*assemblyTestClient) ResolveShare(context.Context, friendshipservice.ResolveShareInput) (friendshipservice.ResolveShareResult, error) {
	return friendshipservice.ResolveShareResult{}, nil
}
func (*assemblyTestClient) RedeemShare(context.Context, friendshipservice.RedeemShareInput) (friendshipservice.RedeemShareResult, error) {
	return friendshipservice.RedeemShareResult{}, nil
}
func (*assemblyTestClient) GetFriendList(context.Context, friendshipservice.GetFriendListInput) (friendshipservice.GetFriendListResult, error) {
	return friendshipservice.GetFriendListResult{}, nil
}
func (*assemblyTestClient) DeleteFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	return friendshipservice.PairMutationResult{}, nil
}
func (*assemblyTestClient) BlockFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	return friendshipservice.PairMutationResult{}, nil
}
func (*assemblyTestClient) UnblockFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	return friendshipservice.PairMutationResult{}, nil
}
func (*assemblyTestClient) GetNextFriendOpportunity(context.Context, friendshipservice.GetNextFriendOpportunityInput) (friendshipservice.GetNextFriendOpportunityResult, error) {
	return friendshipservice.GetNextFriendOpportunityResult{}, nil
}
func (*assemblyTestClient) AuthorizeVisit(context.Context, friendshipservice.AuthorizeVisitInput) (friendshipservice.AuthorizeVisitResult, error) {
	return friendshipservice.AuthorizeVisitResult{}, nil
}
func (*assemblyTestClient) CommitFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return friend.ActionAdmissionOutcome{}, nil
}
func (*assemblyTestClient) GetFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return friend.ActionAdmissionOutcome{}, nil
}

func TestRemotePlayerAssemblyNeverConstructsLocalFriendAuthority(t *testing.T) {
	localCalls, remoteCalls := 0, 0
	local := func() (friendshipservice.Client, func(), error) {
		localCalls++ // represents repository + Lua + repair-worker construction
		return &assemblyTestClient{}, func() {}, nil
	}
	remote := func() (friendshipservice.Client, func(), error) {
		remoteCalls++
		return &assemblyTestClient{}, func() {}, nil
	}
	client, cleanup, err := selectGatewayFriendshipClient(friendshipservice.ModeRemotePlayer, local, remote, nil)
	if err != nil || client == nil {
		t.Fatalf("remote_player assembly: client=%v err=%v", client, err)
	}
	cleanup()
	if localCalls != 0 || remoteCalls != 1 {
		t.Fatalf("factory calls local=%d remote=%d, want 0/1", localCalls, remoteCalls)
	}
}

func TestGatewayFriendshipAssemblyModeMatrixAndFailClosed(t *testing.T) {
	for _, test := range []struct {
		mode                  friendshipservice.Mode
		wantLocal, wantRemote int
	}{
		{mode: friendshipservice.ModeLocalGate, wantLocal: 1},
		{mode: friendshipservice.ModeRemoteShadow, wantLocal: 1, wantRemote: 1},
	} {
		localCalls, remoteCalls := 0, 0
		factory := func(counter *int) friendshipClientFactory {
			return func() (friendshipservice.Client, func(), error) {
				*counter++
				return &assemblyTestClient{}, func() {}, nil
			}
		}
		client, cleanup, err := selectGatewayFriendshipClient(test.mode, factory(&localCalls), factory(&remoteCalls),
			friendshipservice.NewMemoryShadowRecorder())
		if err != nil || client == nil {
			t.Fatalf("mode %q: client=%v err=%v", test.mode, client, err)
		}
		cleanup()
		if localCalls != test.wantLocal || remoteCalls != test.wantRemote {
			t.Fatalf("mode %q factory calls local=%d remote=%d", test.mode, localCalls, remoteCalls)
		}
	}
	if _, _, err := selectGatewayFriendshipClient("invalid", nil, nil, nil); err == nil {
		t.Fatal("unknown assembled mode must fail closed")
	}
}

func TestTargetedOpportunityHintSubscriberRunsOutsideFriendshipAuthorityMode(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	t.Setenv("REDIS_NAMESPACE_ENVIRONMENT", "targeted-hint-test")
	t.Setenv("REDIS_NAMESPACE_ZONE", "z1")
	notifier := &targetedHintTestNotifier{notifications: make(chan targetedHintNotification, 1)}
	stop, err := startGatewayTargetedOpportunityHintSubscriber(client, notifier)
	if err != nil {
		t.Fatalf("start targeted Hint subscriber: %v", err)
	}
	t.Cleanup(stop)
	namespace, err := gatewayRedisNamespace()
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	bus, err := friend.NewRedisOpportunityHintBus(client, namespace)
	if err != nil {
		t.Fatalf("build Hint bus: %v", err)
	}
	hint := friend.TargetedOpportunityHint{
		TargetOwnerID: 2002, SourceFarmVersion: 17, OpportunityRevision: 18,
		ViewerPlayerIDs: []uint64{1001},
	}
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case got := <-notifier.notifications:
			if got != (targetedHintNotification{playerID: 1001, ownerID: 2002, farmVersion: 17, revision: 18}) {
				t.Fatalf("notification=%+v", got)
			}
			return
		case <-ticker.C:
			if err := bus.PublishTargetedOpportunityHint(context.Background(), hint); err != nil {
				t.Fatalf("publish targeted Hint: %v", err)
			}
		case <-deadline:
			t.Fatal("targeted Hint did not reach the Gate-local notifier")
		}
	}
}
