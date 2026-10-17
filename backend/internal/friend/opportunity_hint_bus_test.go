package friend

import (
	"context"
	"testing"
	"time"

	friendpb "farmgame/api/gen/friend"

	"google.golang.org/protobuf/proto"
)

type hintNotification struct{ viewer, owner, version, revision uint64 }
type hintNotifier struct{ calls []hintNotification }

func (n *hintNotifier) NotifyFriendOpportunityChanged(viewer, owner, version, revision uint64) int {
	n.calls = append(n.calls, hintNotification{viewer, owner, version, revision})
	return 1
}

func TestOpportunityHintBusPayloadAndFriendIndexFanout(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	bus, err := NewRedisOpportunityHintBus(repositories.authority.client, repositories.authority.keys.Profile().Namespace)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	subscription := repositories.authority.client.Subscribe(ctx, bus.channel)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishFriendOpportunityHint(ctx, 501, 9, 11); err != nil {
		t.Fatal(err)
	}
	message := <-subscription.Channel()
	var hint friendpb.FriendOpportunityChanged
	if err := proto.Unmarshal([]byte(message.Payload), &hint); err != nil {
		t.Fatal(err)
	}
	if hint.TargetOwnerId != 501 || hint.SourceFarmVersion != 9 || hint.OpportunityRevision != 11 {
		t.Fatalf("hint=%v", &hint)
	}
	index := NewMemoryFriendIndexRepository()
	for _, viewer := range []uint64{601, 602} {
		if _, err := index.Apply(ctx, 501, FriendIndexEntry{FriendPlayerID: viewer,
			RelationID: pairIDMust(501, viewer), RelationEpoch: 1, ActivatedAtMs: int64(viewer)}); err != nil {
			t.Fatal(err)
		}
	}
	notifier := &hintNotifier{}
	if err := FanoutFriendOpportunityHint(ctx, index, notifier, &hint); err != nil {
		t.Fatal(err)
	}
	if len(notifier.calls) != 2 || notifier.calls[0].owner != 501 || notifier.calls[0].version != 9 {
		t.Fatalf("calls=%v", notifier.calls)
	}
}
