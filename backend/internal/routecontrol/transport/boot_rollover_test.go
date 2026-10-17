package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/remote"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type scriptedUnaryResult struct {
	envelope *routecontrolpb.RouteSnapshotEnvelope
	err      error
}

type scriptedUnaryCall struct {
	ctx     context.Context
	method  string
	respond chan scriptedUnaryResult
}

type scriptedWatchResult struct {
	envelope *routecontrolpb.RouteSnapshotEnvelope
	err      error
}

type scriptedWatchCall struct {
	after  uint64
	stream *scriptedWatchStream
}

type scriptedSnapshotRPC struct {
	unaryCalls chan scriptedUnaryCall
	watchCalls chan scriptedWatchCall
}

func newScriptedSnapshotRPC() *scriptedSnapshotRPC {
	return &scriptedSnapshotRPC{
		unaryCalls: make(chan scriptedUnaryCall, 16),
		watchCalls: make(chan scriptedWatchCall, 16),
	}
}

func (r *scriptedSnapshotRPC) GetSnapshot(ctx context.Context, _ *routecontrolpb.GetSnapshotRequest, _ ...grpc.CallOption) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	return r.unary(ctx, "GetSnapshot")
}

func (r *scriptedSnapshotRPC) GetCurrentRevision(ctx context.Context, _ *routecontrolpb.GetCurrentRevisionRequest, _ ...grpc.CallOption) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	return r.unary(ctx, "GetCurrentRevision")
}

func (r *scriptedSnapshotRPC) unary(ctx context.Context, method string) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	call := scriptedUnaryCall{ctx: ctx, method: method, respond: make(chan scriptedUnaryResult, 1)}
	select {
	case r.unaryCalls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-call.respond:
		if result.envelope == nil {
			return nil, result.err
		}
		return proto.Clone(result.envelope).(*routecontrolpb.RouteSnapshotEnvelope), result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *scriptedSnapshotRPC) WatchFrom(ctx context.Context, request *routecontrolpb.WatchFromRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[routecontrolpb.RouteSnapshotEnvelope], error) {
	stream := &scriptedWatchStream{ctx: ctx, results: make(chan scriptedWatchResult, 8)}
	call := scriptedWatchCall{after: request.GetAfterRevision(), stream: stream}
	select {
	case r.watchCalls <- call:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type scriptedWatchStream struct {
	ctx     context.Context
	results chan scriptedWatchResult
}

func (s *scriptedWatchStream) Recv() (*routecontrolpb.RouteSnapshotEnvelope, error) {
	select {
	case result := <-s.results:
		if result.envelope == nil {
			return nil, result.err
		}
		return proto.Clone(result.envelope).(*routecontrolpb.RouteSnapshotEnvelope), result.err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *scriptedWatchStream) Header() (metadata.MD, error) { return nil, nil }
func (s *scriptedWatchStream) Trailer() metadata.MD         { return nil }
func (s *scriptedWatchStream) CloseSend() error             { return nil }
func (s *scriptedWatchStream) Context() context.Context     { return s.ctx }
func (s *scriptedWatchStream) SendMsg(any) error            { return nil }
func (s *scriptedWatchStream) RecvMsg(target any) error {
	envelope, err := s.Recv()
	if err != nil {
		return err
	}
	typed, ok := target.(*routecontrolpb.RouteSnapshotEnvelope)
	if !ok {
		return fmt.Errorf("unexpected RecvMsg target %T", target)
	}
	proto.Reset(typed)
	proto.Merge(typed, envelope)
	return nil
}

func newScriptedSnapshotClient(t *testing.T, rpc *scriptedSnapshotRPC, retiredLimit int) *SnapshotClient {
	t.Helper()
	if retiredLimit == 0 {
		retiredLimit = defaultRetiredBootLimit
	}
	options := ClientOptions{
		ExpectedControllerID: "controller-a",
		ExpectedRealm:        "transport-test",
		MaxMessageBytes:      1 << 20,
		MaxConcurrent:        8,
		WatchBuffer:          8,
		CallTimeout:          time.Second,
		RetiredBootLimit:     retiredLimit,
	}.normalize()
	var closeCalls atomic.Int32
	client := &SnapshotClient{
		rpc:              rpc,
		opts:             options,
		sem:              make(chan struct{}, options.maxConcurrent),
		resyncGate:       make(chan struct{}, 1),
		closedCh:         make(chan struct{}),
		terminalCh:       make(chan struct{}),
		activeWatches:    make(map[uint64]context.CancelFunc),
		activeOperations: make(map[uint64]context.CancelFunc),
		retiredBoots:     make(map[string]struct{}, options.retiredBootLimit),
		closeFn: func() error {
			closeCalls.Add(1)
			return nil
		},
	}
	t.Cleanup(func() {
		_ = client.Close()
		if closeCalls.Load() != 1 {
			t.Errorf("client close calls=%d want 1", closeCalls.Load())
		}
	})
	return client
}

func rolloverEnvelope(t *testing.T, snapshot actorrouting.RouteSnapshot, boot string, committedAt int64) *routecontrolpb.RouteSnapshotEnvelope {
	t.Helper()
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	stored.CommittedAtMs = committedAt
	envelope, err := storedToEnvelope(stored, "controller-a", boot, "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func validHigherRolloverSnapshot(base actorrouting.RouteSnapshot) actorrouting.RouteSnapshot {
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	next.Player.Routes[0].LeaseUntilMs++
	return next
}

func seedRolloverClient(t *testing.T, client *SnapshotClient, envelope *routecontrolpb.RouteSnapshotEnvelope) actorrouting.RouteSnapshot {
	t.Helper()
	stored, err := client.validateEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return stored.Snapshot
}

func runRolloverFetch(t *testing.T, client *SnapshotClient, rpc *scriptedSnapshotRPC, baseline actorrouting.RouteSnapshot, response *routecontrolpb.RouteSnapshotEnvelope) (actorrouting.RouteSnapshot, error) {
	t.Helper()
	result := make(chan struct {
		snapshot actorrouting.RouteSnapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := client.FetchFullSnapshotForResync(context.Background(), remote.ResyncBaseline{
			Snapshot: actorrouting.CloneRouteSnapshot(baseline), Revision: baseline.GlobalRevision,
		})
		result <- struct {
			snapshot actorrouting.RouteSnapshot
			err      error
		}{snapshot: snapshot, err: err}
	}()
	call := receiveUnaryCall(t, rpc)
	if call.method != "GetSnapshot" {
		t.Fatalf("rollover unary method=%s want GetSnapshot", call.method)
	}
	call.respond <- scriptedUnaryResult{envelope: response}
	select {
	case got := <-result:
		return got.snapshot, got.err
	case <-time.After(time.Second):
		t.Fatal("rollover fetch did not finish")
		return actorrouting.RouteSnapshot{}, context.DeadlineExceeded
	}
}

func receiveUnaryCall(t *testing.T, rpc *scriptedSnapshotRPC) scriptedUnaryCall {
	t.Helper()
	select {
	case call := <-rpc.unaryCalls:
		return call
	case <-time.After(time.Second):
		t.Fatal("unary call did not start")
		return scriptedUnaryCall{}
	}
}

func receiveWatchCall(t *testing.T, rpc *scriptedSnapshotRPC) scriptedWatchCall {
	t.Helper()
	select {
	case call := <-rpc.watchCalls:
		return call
	case <-time.After(time.Second):
		t.Fatal("Watch call did not start")
		return scriptedWatchCall{}
	}
}

func establishScriptedWatch(t *testing.T, client *SnapshotClient, rpc *scriptedSnapshotRPC, after uint64, handshake *routecontrolpb.RouteSnapshotEnvelope) (<-chan actorrouting.RouteSnapshot, scriptedWatchCall, error) {
	t.Helper()
	type result struct {
		updates <-chan actorrouting.RouteSnapshot
		err     error
	}
	done := make(chan result, 1)
	go func() {
		updates, err := client.WatchFrom(context.Background(), after)
		done <- result{updates: updates, err: err}
	}()
	call := receiveWatchCall(t, rpc)
	call.stream.results <- scriptedWatchResult{envelope: handshake}
	select {
	case got := <-done:
		return got.updates, call, got.err
	case <-time.After(time.Second):
		t.Fatal("Watch handshake did not finish")
		return nil, call, context.DeadlineExceeded
	}
}

func receiveHandshake(t *testing.T, updates <-chan actorrouting.RouteSnapshot, revision uint64) {
	t.Helper()
	select {
	case snapshot, ok := <-updates:
		if !ok || snapshot.GlobalRevision != revision {
			t.Fatalf("handshake snapshot revision=%d open=%v want=%d", snapshot.GlobalRevision, ok, revision)
		}
	case <-time.After(time.Second):
		t.Fatal("Watch handshake was not published")
	}
}

func assertPinnedBoots(t *testing.T, client *SnapshotClient, current string, retired ...string) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pinnedBoot != current || len(client.retiredBoots) != len(retired) {
		t.Fatalf("current boot=%q retired=%v want current=%q retired=%v", client.pinnedBoot, client.retiredBoots, current, retired)
	}
	for _, boot := range retired {
		if _, ok := client.retiredBoots[boot]; !ok {
			t.Fatalf("retired boot %q missing from %v", boot, client.retiredBoots)
		}
	}
}

func TestSnapshotClientPumpRolloverEqualRevisionStartsNewBootWatch(t *testing.T) {
	base := transportSnapshot(10)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	c1 := rolloverEnvelope(t, base, "boot-c1", 100)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	provider, pump, err := remote.NewRemoteAssignmentProvider(client, remote.Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()

	initial := receiveUnaryCall(t, rpc)
	initial.respond <- scriptedUnaryResult{envelope: c0}
	watch0 := receiveWatchCall(t, rpc)
	watch0.stream.results <- scriptedWatchResult{envelope: c0}
	watch0.stream.results <- scriptedWatchResult{err: io.EOF}
	resync := receiveUnaryCall(t, rpc)
	resync.respond <- scriptedUnaryResult{envelope: c1}
	watch1 := receiveWatchCall(t, rpc)
	watch1.stream.results <- scriptedWatchResult{envelope: c1}
	if watch0.after != 10 || watch1.after != 10 {
		t.Fatalf("Watch revisions C0=%d C1=%d want 10/10", watch0.after, watch1.after)
	}
	if got := provider.CurrentSnapshot(); !reflect.DeepEqual(got, base) || pump.State() != remote.StateWatching {
		t.Fatalf("post-rollover revision=%d state=%s", got.GlobalRevision, pump.State())
	}
	assertPinnedBoots(t, client, "boot-c1", "boot-c0")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Pump cancellation err=%v", err)
	}
}

func TestSnapshotClientPumpRolloverHigherRevisionInstallsBeforeNewWatch(t *testing.T) {
	base := transportSnapshot(10)
	next := validHigherRolloverSnapshot(base)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	provider, pump, err := remote.NewRemoteAssignmentProvider(client, remote.Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()

	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: rolloverEnvelope(t, base, "boot-c0", 100)}
	watch0 := receiveWatchCall(t, rpc)
	watch0.stream.results <- scriptedWatchResult{envelope: rolloverEnvelope(t, base, "boot-c0", 100)}
	watch0.stream.results <- scriptedWatchResult{err: io.EOF}
	c1 := rolloverEnvelope(t, next, "boot-c1", 200)
	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c1}
	watch1 := receiveWatchCall(t, rpc)
	watch1.stream.results <- scriptedWatchResult{envelope: c1}
	if watch1.after != next.GlobalRevision || !reflect.DeepEqual(provider.CurrentSnapshot(), next) || pump.CurrentRevision() != next.GlobalRevision {
		t.Fatalf("new Watch revision=%d provider=%d pump=%d want=%d", watch1.after, provider.CurrentSnapshot().GlobalRevision, pump.CurrentRevision(), next.GlobalRevision)
	}
	later := validHigherRolloverSnapshot(next)
	watch1.stream.results <- scriptedWatchResult{envelope: rolloverEnvelope(t, later, "boot-c1", 300)}
	waitUntilTransport(t, time.Second, func() bool { return provider.CurrentSnapshot().GlobalRevision == later.GlobalRevision })
	if pump.State() != remote.StateWatching {
		t.Fatalf("post-rollover continuous Watch state=%s", pump.State())
	}
	assertPinnedBoots(t, client, "boot-c1", "boot-c0")
	cancel()
	<-done
}

func TestSnapshotClientPumpSameBootReconnectIsUnchanged(t *testing.T) {
	base := transportSnapshot(10)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	_, pump, err := remote.NewRemoteAssignmentProvider(client, remote.Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c0}
	watch0 := receiveWatchCall(t, rpc)
	watch0.stream.results <- scriptedWatchResult{envelope: c0}
	watch0.stream.results <- scriptedWatchResult{err: io.EOF}
	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c0}
	watch1 := receiveWatchCall(t, rpc)
	watch1.stream.results <- scriptedWatchResult{envelope: c0}
	assertPinnedBoots(t, client, "boot-c0")
	waitUntilTransport(t, time.Second, func() bool { return pump.Duplicates.Load() == 3 })
	if pump.State() != remote.StateWatching {
		t.Fatalf("same-Boot reconnect state=%s duplicates=%d", pump.State(), pump.Duplicates.Load())
	}
	cancel()
	<-done
}

func TestSnapshotClientNewBootOrdinaryFetchAndMidWatchRemainTerminal(t *testing.T) {
	base := transportSnapshot(10)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	c1 := rolloverEnvelope(t, base, "boot-c1", 100)

	t.Run("ordinary fetch", func(t *testing.T) {
		rpc := newScriptedSnapshotRPC()
		client := newScriptedSnapshotClient(t, rpc, 4)
		seedRolloverClient(t, client, c0)
		done := make(chan error, 1)
		go func() {
			_, err := client.FetchFullSnapshot(context.Background())
			done <- err
		}()
		receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c1}
		if err := <-done; !errors.Is(err, ErrEnvelopeBootDrift) || !errors.Is(client.LastWatchError(), ErrEnvelopeBootDrift) {
			t.Fatalf("ordinary new-Boot err=%v latched=%v", err, client.LastWatchError())
		}
		assertPinnedBoots(t, client, "boot-c0")
	})

	t.Run("mid-Watch", func(t *testing.T) {
		rpc := newScriptedSnapshotRPC()
		client := newScriptedSnapshotClient(t, rpc, 4)
		seedRolloverClient(t, client, c0)
		updates, watch, err := establishScriptedWatch(t, client, rpc, 10, c0)
		if err != nil {
			t.Fatal(err)
		}
		receiveHandshake(t, updates, 10)
		watch.stream.results <- scriptedWatchResult{envelope: c1}
		select {
		case _, ok := <-updates:
			if ok {
				t.Fatal("mid-Watch new Boot was published")
			}
		case <-time.After(time.Second):
			t.Fatal("mid-Watch drift did not close")
		}
		if !errors.Is(client.LastWatchError(), ErrEnvelopeBootDrift) {
			t.Fatalf("mid-Watch latched err=%v", client.LastWatchError())
		}
		assertPinnedBoots(t, client, "boot-c0")
	})
}

func TestSnapshotClientRolloverWhileWatchActiveFailsClosedWithoutFetch(t *testing.T) {
	base := transportSnapshot(10)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	seedRolloverClient(t, client, c0)
	updates, _, err := establishScriptedWatch(t, client, rpc, 10, c0)
	if err != nil {
		t.Fatal(err)
	}
	receiveHandshake(t, updates, 10)
	_, err = client.FetchFullSnapshotForResync(context.Background(), remote.ResyncBaseline{Snapshot: base, Revision: 10})
	if !errors.Is(err, ErrBootRolloverOverlap) || !errors.Is(client.LastWatchError(), ErrBootRolloverOverlap) {
		t.Fatalf("active-Watch rollover err=%v latched=%v", err, client.LastWatchError())
	}
	select {
	case <-rpc.unaryCalls:
		t.Fatal("overlapping rollover contacted the server")
	default:
	}
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("overlap-canceled Watch emitted an update")
		}
	case <-time.After(time.Second):
		t.Fatal("overlap did not cancel and join Watch")
	}
	assertPinnedBoots(t, client, "boot-c0")
}

func TestSnapshotClientRolloverRejectsRollbackAndEqualEnvelopeMismatchWithoutPin(t *testing.T) {
	base := transportSnapshot(10)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	conflict := actorrouting.CloneRouteSnapshot(base)
	conflict.Holders[0].Endpoint = "player-conflict:9000"
	tests := []struct {
		name     string
		response func(*testing.T) *routecontrolpb.RouteSnapshotEnvelope
		want     error
	}{
		{name: "lower revision", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			lower := actorrouting.CloneRouteSnapshot(base)
			lower.GlobalRevision--
			return rolloverEnvelope(t, lower, "boot-c1", 90)
		}, want: ErrBootRolloverRollback},
		{name: "equal payload", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			return rolloverEnvelope(t, conflict, "boot-c1", 100)
		}, want: ErrBootRolloverMismatch},
		{name: "equal checksum", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			envelope := rolloverEnvelope(t, base, "boot-c1", 100)
			replacement := byte('0')
			if envelope.Checksum[0] == replacement {
				replacement = '1'
			}
			envelope.Checksum = string(replacement) + envelope.Checksum[1:]
			return envelope
		}, want: core.ErrSnapshotChecksumMismatch},
		{name: "equal committed envelope", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			return rolloverEnvelope(t, base, "boot-c1", 101)
		}, want: ErrBootRolloverMismatch},
		{name: "noncanonical new Boot", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			envelope := rolloverEnvelope(t, base, "boot-c1", 100)
			envelope.Boot = " boot-c1 "
			return envelope
		}, want: ErrBootRolloverContract},
		{name: "empty new Boot", response: func(t *testing.T) *routecontrolpb.RouteSnapshotEnvelope {
			envelope := rolloverEnvelope(t, base, "boot-c1", 100)
			envelope.Boot = ""
			return envelope
		}, want: ErrEnvelopeMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := newScriptedSnapshotRPC()
			client := newScriptedSnapshotClient(t, rpc, 4)
			seedRolloverClient(t, client, c0)
			_, err := runRolloverFetch(t, client, rpc, base, test.response(t))
			if !errors.Is(err, test.want) || !errors.Is(client.LastWatchError(), test.want) {
				t.Fatalf("rollover err=%v latched=%v want=%v", err, client.LastWatchError(), test.want)
			}
			assertPinnedBoots(t, client, "boot-c0")
		})
	}
}

func TestSnapshotClientRolloverCannotDropBelowTransportAcceptedFloor(t *testing.T) {
	base := transportSnapshot(10)
	accepted := validHigherRolloverSnapshot(base)
	accepted.GlobalRevision = 12
	candidate := actorrouting.CloneRouteSnapshot(accepted)
	candidate.GlobalRevision = 11
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
	if _, err := client.validateEnvelope(rolloverEnvelope(t, accepted, "boot-c0", 120)); err != nil {
		t.Fatal(err)
	}
	_, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, candidate, "boot-c1", 110))
	if !errors.Is(err, ErrBootRolloverRollback) {
		t.Fatalf("transport acceptance floor err=%v", err)
	}
	assertPinnedBoots(t, client, "boot-c0")
}

func TestSnapshotClientRolloverBindsEqualRevisionToInstalledNotSpeculativeEnvelope(t *testing.T) {
	base := transportSnapshot(10)
	conflict := actorrouting.CloneRouteSnapshot(base)
	conflict.Holders[0].Endpoint = "speculative:9000"
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
	if _, err := client.validateEnvelope(rolloverEnvelope(t, conflict, "boot-c0", 100)); err != nil {
		t.Fatal(err)
	}
	_, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, base, "boot-c1", 100))
	if !errors.Is(err, ErrBootRolloverContract) {
		t.Fatalf("speculative equal envelope err=%v", err)
	}
	assertPinnedBoots(t, client, "boot-c0")
}

func TestSnapshotClientRetiredBootsCannotReturnAcrossC2(t *testing.T) {
	base := transportSnapshot(10)

	t.Run("C0 after C1", func(t *testing.T) {
		rpc := newScriptedSnapshotRPC()
		client := newScriptedSnapshotClient(t, rpc, 4)
		c0 := rolloverEnvelope(t, base, "boot-c0", 100)
		seedRolloverClient(t, client, c0)
		if _, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, base, "boot-c1", 100)); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := client.FetchFullSnapshot(context.Background())
			done <- err
		}()
		receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c0}
		if err := <-done; !errors.Is(err, ErrEnvelopeBootRetired) {
			t.Fatalf("retired C0 err=%v", err)
		}
	})

	t.Run("C1 after C2", func(t *testing.T) {
		rpc := newScriptedSnapshotRPC()
		client := newScriptedSnapshotClient(t, rpc, 4)
		seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
		c1 := rolloverEnvelope(t, base, "boot-c1", 100)
		if _, err := runRolloverFetch(t, client, rpc, base, c1); err != nil {
			t.Fatal(err)
		}
		if _, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, base, "boot-c2", 100)); err != nil {
			t.Fatal(err)
		}
		assertPinnedBoots(t, client, "boot-c2", "boot-c0", "boot-c1")
		done := make(chan error, 1)
		go func() {
			_, err := client.FetchFullSnapshot(context.Background())
			done <- err
		}()
		receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c1}
		if err := <-done; !errors.Is(err, ErrEnvelopeBootRetired) {
			t.Fatalf("retired C1 err=%v", err)
		}
	})
}

func TestSnapshotClientRetiredBootCapacityFailsClosedWithoutForgetting(t *testing.T) {
	base := transportSnapshot(10)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 1)
	seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
	if _, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, base, "boot-c1", 100)); err != nil {
		t.Fatal(err)
	}
	_, err := runRolloverFetch(t, client, rpc, base, rolloverEnvelope(t, base, "boot-c2", 100))
	if !errors.Is(err, ErrRetiredBootCapacity) || !errors.Is(client.LastWatchError(), ErrRetiredBootCapacity) {
		t.Fatalf("capacity err=%v latched=%v", err, client.LastWatchError())
	}
	assertPinnedBoots(t, client, "boot-c1", "boot-c0")
}

func TestSnapshotClientRolloverIdentityRealmAndTopologyDriftRemainTerminal(t *testing.T) {
	base := transportSnapshot(10)
	tests := []struct {
		name   string
		mutate func(*testing.T, *routecontrolpb.RouteSnapshotEnvelope) *routecontrolpb.RouteSnapshotEnvelope
		want   error
	}{
		{name: "controller", mutate: func(_ *testing.T, envelope *routecontrolpb.RouteSnapshotEnvelope) *routecontrolpb.RouteSnapshotEnvelope {
			envelope.ControllerId = "controller-b"
			return envelope
		}, want: ErrEnvelopeControllerMismatch},
		{name: "realm", mutate: func(t *testing.T, _ *routecontrolpb.RouteSnapshotEnvelope) *routecontrolpb.RouteSnapshotEnvelope {
			drift := actorrouting.CloneRouteSnapshot(base)
			drift.Realm = "other-realm"
			stored, err := core.CanonicalizeRouteSnapshot(drift)
			if err != nil {
				t.Fatal(err)
			}
			stored.CommittedAtMs = 100
			envelope, err := storedToEnvelope(stored, "controller-a", "boot-c1", "other-realm", 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			return envelope
		}, want: ErrEnvelopeRealmMismatch},
		{name: "topology", mutate: func(t *testing.T, _ *routecontrolpb.RouteSnapshotEnvelope) *routecontrolpb.RouteSnapshotEnvelope {
			drift := actorrouting.CloneRouteSnapshot(base)
			drift.HashSeed++
			return rolloverEnvelope(t, drift, "boot-c1", 100)
		}, want: ErrEnvelopeTopologyDrift},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := newScriptedSnapshotRPC()
			client := newScriptedSnapshotClient(t, rpc, 4)
			seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
			response := test.mutate(t, rolloverEnvelope(t, base, "boot-c1", 100))
			_, err := runRolloverFetch(t, client, rpc, base, response)
			if !errors.Is(err, test.want) || !errors.Is(client.LastWatchError(), test.want) {
				t.Fatalf("drift err=%v latched=%v want=%v", err, client.LastWatchError(), test.want)
			}
			assertPinnedBoots(t, client, "boot-c0")
		})
	}
}

func TestSnapshotClientDuplicateRolloverSerializesOneRetirement(t *testing.T) {
	base := transportSnapshot(10)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	seedRolloverClient(t, client, rolloverEnvelope(t, base, "boot-c0", 100))
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := client.FetchFullSnapshotForResync(context.Background(), remote.ResyncBaseline{Snapshot: base, Revision: 10})
			results <- err
		}()
	}
	c1 := rolloverEnvelope(t, base, "boot-c1", 100)
	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c1}
	receiveUnaryCall(t, rpc).respond <- scriptedUnaryResult{envelope: c1}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("duplicate rollover err=%v", err)
		}
	}
	assertPinnedBoots(t, client, "boot-c1", "boot-c0")
}

func TestSnapshotClientWatchCloseRolloverAndCloseJoinWithoutPinRace(t *testing.T) {
	base := transportSnapshot(10)
	rpc := newScriptedSnapshotRPC()
	client := newScriptedSnapshotClient(t, rpc, 4)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	seedRolloverClient(t, client, c0)
	updates, watch, err := establishScriptedWatch(t, client, rpc, 10, c0)
	if err != nil {
		t.Fatal(err)
	}
	receiveHandshake(t, updates, 10)
	watch.stream.results <- scriptedWatchResult{err: io.EOF}
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("closed Watch emitted an update")
		}
	case <-time.After(time.Second):
		t.Fatal("Watch did not close and unregister")
	}
	resyncDone := make(chan error, 1)
	go func() {
		_, err := client.FetchFullSnapshotForResync(context.Background(), remote.ResyncBaseline{Snapshot: base, Revision: 10})
		resyncDone <- err
	}()
	call := receiveUnaryCall(t, rpc)
	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close() }()
	select {
	case <-call.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel rollover RPC")
	}
	select {
	case err := <-resyncDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled rollover err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled rollover did not join")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close returned before joined operations converged")
	}
	assertPinnedBoots(t, client, "boot-c0")
}

func TestClientOptionsValidateRetiredBootLimit(t *testing.T) {
	for _, limit := range []int{-1, maxRetiredBootLimit + 1} {
		options := ClientOptions{
			ExpectedControllerID:   "controller-a",
			ServerName:             "server.test",
			ExpectedServerIdentity: "server.test",
			RetiredBootLimit:       limit,
		}
		if _, err := options.validate(); !errors.Is(err, ErrRetiredBootCapacity) {
			t.Fatalf("retired limit=%d err=%v", limit, err)
		}
	}
	options := ClientOptions{
		ExpectedControllerID:   "controller-a",
		ServerName:             "server.test",
		ExpectedServerIdentity: "server.test",
		RetiredBootLimit:       maxRetiredBootLimit,
	}
	if normalized, err := options.validate(); err != nil || normalized.retiredBootLimit != maxRetiredBootLimit {
		t.Fatalf("valid retired limit normalized=%d err=%v", normalized.retiredBootLimit, err)
	}
}
