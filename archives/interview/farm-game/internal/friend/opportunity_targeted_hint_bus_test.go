package friend

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestTargetedOpportunityHintCollectPublishAndDispatch(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	bus, err := NewRedisOpportunityHintBus(repositories.authority.client, repositories.authority.keys.Profile().Namespace)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	index := NewMemoryFriendIndexRepository()
	for _, viewer := range []uint64{603, 601, 602} {
		if _, err := index.Apply(ctx, 501, FriendIndexEntry{FriendPlayerID: viewer,
			RelationID: pairIDMust(501, viewer), RelationEpoch: 1, ActivatedAtMs: int64(viewer)}); err != nil {
			t.Fatal(err)
		}
	}
	viewers, err := CollectTargetedOpportunityViewers(ctx, index, 501)
	if err != nil || !reflect.DeepEqual(viewers, []uint64{601, 602, 603}) {
		t.Fatalf("viewers=%v err=%v", viewers, err)
	}
	subscription := repositories.authority.client.Subscribe(ctx, bus.targetedChannel)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	want := TargetedOpportunityHint{TargetOwnerID: 501, SourceFarmVersion: 9, OpportunityRevision: 9, ViewerPlayerIDs: viewers}
	if err := bus.PublishTargetedOpportunityHint(ctx, want); err != nil {
		t.Fatal(err)
	}
	message := <-subscription.Channel()
	got, err := unmarshalTargetedOpportunityHint([]byte(message.Payload))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("hint=%+v err=%v", got, err)
	}
	notifier := &hintNotifier{}
	if err := DispatchTargetedOpportunityHint(notifier, got); err != nil {
		t.Fatal(err)
	}
	if len(notifier.calls) != 3 || notifier.calls[0].viewer != 601 || notifier.calls[2].viewer != 603 {
		t.Fatalf("calls=%v", notifier.calls)
	}
}

func TestTargetedOpportunityHintRejectsUnsortedOrExcessiveViewers(t *testing.T) {
	if _, err := marshalTargetedOpportunityHint(TargetedOpportunityHint{
		TargetOwnerID: 1, SourceFarmVersion: 2, OpportunityRevision: 2, ViewerPlayerIDs: []uint64{3, 2},
	}); err == nil {
		t.Fatal("expected unsorted viewer rejection")
	}
	viewers := make([]uint64, MaxTargetedOpportunityViewers+1)
	for i := range viewers {
		viewers[i] = uint64(i + 2)
	}
	if _, err := marshalTargetedOpportunityHint(TargetedOpportunityHint{
		TargetOwnerID: 1, SourceFarmVersion: 2, OpportunityRevision: 2, ViewerPlayerIDs: viewers,
	}); err == nil {
		t.Fatal("expected viewer limit rejection")
	}
}
