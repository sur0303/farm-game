package routecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"

	"farmgame/internal/actorrouting"
)

type recordingNotifier struct {
	mu        sync.Mutex
	failNext  bool
	snapshots []actorrouting.RouteSnapshot
}

func (n *recordingNotifier) Notify(_ context.Context, snapshot actorrouting.RouteSnapshot) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failNext {
		n.failNext = false
		return errors.New("notify unavailable")
	}
	n.snapshots = append(n.snapshots, actorrouting.CloneRouteSnapshot(snapshot))
	return nil
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.snapshots)
}

type recordingAuthorityNotifier struct {
	mu        sync.Mutex
	snapshots []actorrouting.RouteSnapshot
}

func (n *recordingAuthorityNotifier) AuthorityNotify(_ context.Context, snapshot actorrouting.RouteSnapshot) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshots = append(n.snapshots, actorrouting.CloneRouteSnapshot(snapshot))
	return nil
}

func (n *recordingAuthorityNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.snapshots)
}

func TestDurablePublisherStoresBeforeNotifyAndReplaysNotify(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	notifier := &recordingNotifier{failNext: true}
	publisher, err := NewDurableSnapshotPublisher(store, notifier)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), NestedControlOperationToken{}, testSnapshot(2)); !errors.Is(err, ErrSnapshotNotify) {
		t.Fatalf("publish failure=%v", err)
	}
	stored, found, err := store.LoadCurrent(context.Background())
	if err != nil || !found || stored.Revision != 2 {
		t.Fatalf("committed store lost after notify failure=%+v found=%t err=%v", stored, found, err)
	}
	if notifier.count() != 0 {
		t.Fatal("failed notification was recorded")
	}
	if err := publisher.Publish(context.Background(), NestedControlOperationToken{}, testSnapshot(2)); err != nil {
		t.Fatal(err)
	}
	if notifier.count() != 1 {
		t.Fatalf("replay notify count=%d", notifier.count())
	}
	stored, found, err = store.LoadCurrent(context.Background())
	if err != nil || !found || stored.Revision != 2 {
		t.Fatalf("replay advanced or lost store=%+v found=%t err=%v", stored, found, err)
	}
}

func TestDurablePublisherSeparatesOrdinaryAndAuthorityPublication(t *testing.T) {
	initial := testSnapshot(1)
	initial.Player.Routes[0].State = actorrouting.ShardStateAssigning
	initial.Farm.Routes[0].State = actorrouting.ShardStateAssigning
	initial.Holders[0].State = actorrouting.HolderStateAssigning
	initial.Holders[1].State = actorrouting.HolderStateAssigning
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, initial); err != nil {
		t.Fatal(err)
	}
	authorityNotifier := &recordingAuthorityNotifier{}
	publisher, err := NewDurableSnapshotPublisher(store, nil, authorityNotifier)
	if err != nil {
		t.Fatal(err)
	}
	next := testSnapshot(2)
	next.Farm.Routes[0].State = actorrouting.ShardStateAssigning
	next.Holders[1].State = actorrouting.HolderStateAssigning
	if err := publisher.Publish(context.Background(), NestedControlOperationToken{}, next); err == nil {
		t.Fatal("ordinary publication accepted ASSIGNING -> ACTIVE activation")
	}
	if err := publisher.PublishAuthoritySnapshot(context.Background(), NestedControlOperationToken{}, next); err != nil {
		t.Fatalf("authority publication rejected: %v", err)
	}
	if authorityNotifier.count() != 1 {
		t.Fatalf("authority notifier count=%d", authorityNotifier.count())
	}
}

func TestDurablePublisherAuthorityWaveAllowsManyActivations(t *testing.T) {
	initial := testSnapshot(1)
	initial.Player.Routes[0].State = actorrouting.ShardStateAssigning
	initial.Farm.Routes[0].State = actorrouting.ShardStateAssigning
	initial.Holders[0].State = actorrouting.HolderStateAssigning
	initial.Holders[1].State = actorrouting.HolderStateAssigning
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, initial); err != nil {
		t.Fatal(err)
	}
	authorityNotifier := &recordingAuthorityNotifier{}
	publisher, err := NewDurableSnapshotPublisher(store, nil, authorityNotifier)
	if err != nil {
		t.Fatal(err)
	}
	next := testSnapshot(2)
	if err := publisher.PublishAuthoritySnapshot(context.Background(), NestedControlOperationToken{}, next); err == nil {
		t.Fatal("single-activation authority path accepted two ASSIGNING -> ACTIVE updates")
	}
	if err := publisher.PublishAuthorityWaveSnapshot(context.Background(), NestedControlOperationToken{}, next); err != nil {
		t.Fatalf("wave publication rejected: %v", err)
	}
	if authorityNotifier.count() != 1 {
		t.Fatalf("authority notifier count=%d", authorityNotifier.count())
	}
}

func TestDurablePublisherRequiresBootstrapForEmptyStore(t *testing.T) {
	publisher, err := NewDurableSnapshotPublisher(NewMemoryRouteSnapshotStore(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); !errors.Is(err, ErrSnapshotBootstrapRequired) {
		t.Fatalf("empty publish error=%v", err)
	}
}

func TestDurablePublisherDoesNotUseOrdinaryNotifierForAuthority(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	ordinary := &recordingNotifier{}
	publisher, err := NewDurableSnapshotPublisher(store, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishAuthoritySnapshot(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); !errors.Is(err, ErrSnapshotAuthorityNotifyRequired) {
		t.Fatalf("authority without capability error=%v", err)
	}
	if ordinary.count() != 0 {
		t.Fatalf("ordinary notifier was used for authority publication: %d", ordinary.count())
	}
}

func TestDurablePublisherBootstrapNotifyFailureRetriesExactR1(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	notifier := &recordingNotifier{failNext: true}
	publisher, err := NewDurableSnapshotPublisher(store, notifier)
	if err != nil {
		t.Fatal(err)
	}
	result, err := publisher.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1))
	if !errors.Is(err, ErrSnapshotNotify) || result.Status != SnapshotCASCommitted {
		t.Fatalf("first bootstrap result=%+v err=%v", result, err)
	}
	result, err = publisher.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1))
	if err != nil || result.Status != SnapshotCASAlreadyCommitted {
		t.Fatalf("bootstrap retry result=%+v err=%v", result, err)
	}
	if notifier.count() != 1 {
		t.Fatalf("retry notification count=%d", notifier.count())
	}
}
