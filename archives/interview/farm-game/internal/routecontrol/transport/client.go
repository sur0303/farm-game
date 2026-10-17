package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"
	"farmgame/internal/actorrouting"
	remote "farmgame/internal/actorrouting/remote"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// SnapshotClient is the authenticated transport adapter consumed by the
// transport-neutral RemoteAssignmentPump. It never exposes a mutation or
// authority capability.
type SnapshotClient struct {
	conn grpc.ClientConnInterface
	rpc  routecontrolpb.RouteSnapshotServiceClient
	opts normalizedClientOptions
	sem  chan struct{}

	// bootMu keeps ordinary fetches and the complete lifetime of every Watch
	// on one pinned controller generation. A restart-aware resync takes the
	// exclusive side only after the preceding Watch has fully unregistered.
	bootMu     sync.RWMutex
	resyncGate chan struct{}

	mu               sync.Mutex
	pinnedBoot       string
	pinnedRealm      string
	pinnedTopology   string
	retiredBoots     map[string]struct{}
	accepted         core.StoredRouteSnapshot
	acceptedBoot     string
	hasAccepted      bool
	acceptedConflict bool
	lastErr          error
	terminalErr      error
	closed           bool
	closedCh         chan struct{}
	terminalCh       chan struct{}
	activeWatches    map[uint64]context.CancelFunc
	activeOperations map[uint64]context.CancelFunc
	nextOperationID  uint64
	callWG           sync.WaitGroup
	closeOnce        sync.Once
	closeFn          func() error
	closeErr         error
}

type Client = SnapshotClient

var _ remote.RemoteSnapshotSource = (*SnapshotClient)(nil)
var _ remote.RestartAwareRemoteSnapshotSource = (*SnapshotClient)(nil)

// Dial creates a gRPC client with TLS credentials only. There is deliberately
// no insecure fallback path. ServerName and ExpectedServerIdentity are both
// required and must bind to the same canonical DNS identity.
func Dial(ctx context.Context, target string, tlsConfig *tls.Config, options ClientOptions) (*SnapshotClient, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if strings.TrimSpace(target) == "" {
		return nil, fmt.Errorf("%w: target required", ErrTLSConfig)
	}
	normalized, err := options.validate()
	if err != nil {
		return nil, err
	}
	clone, err := cloneTLSConfig(tlsConfig)
	if err != nil {
		return nil, err
	}
	clone.ServerName = strings.TrimSpace(options.ServerName)
	if err := validateClientTLSConfig(clone); err != nil {
		return nil, err
	}
	conn, err := grpc.DialContext(
		ctx,
		target,
		grpc.WithTransportCredentials(credentials.NewTLS(clone)),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			// gRPC enforces this bound while decoding the wire message,
			// before the envelope is handed to decodeEnvelope.
			grpc.MaxCallRecvMsgSize(normalized.maxMessageBytes),
			grpc.MaxCallSendMsgSize(normalized.maxMessageBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	client, err := newClient(conn, normalized, conn.Close)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

// DialRouteSnapshot is an explicit descriptive alias for Dial.
func DialRouteSnapshot(ctx context.Context, target string, tlsConfig *tls.Config, options ClientOptions) (*SnapshotClient, error) {
	return Dial(ctx, target, tlsConfig, options)
}

// newClient is intentionally unexported. Only Dial is a production
// constructor; package-local tests may use this seam with an explicit close
// function that proves ownership of the connection.
func newClient(conn grpc.ClientConnInterface, options normalizedClientOptions, closeFn func() error) (*SnapshotClient, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: client connection required", ErrTLSRequired)
	}
	if options.expectedControllerID == "" {
		return nil, fmt.Errorf("%w: expected controller id required", ErrEnvelopeControllerMismatch)
	}
	if options.retiredBootLimit < 1 || options.retiredBootLimit > maxRetiredBootLimit {
		return nil, fmt.Errorf("%w: limit=%d must be within [1,%d]", ErrRetiredBootCapacity, options.retiredBootLimit, maxRetiredBootLimit)
	}
	if closeFn == nil {
		return nil, fmt.Errorf("%w: owned connection close function required", ErrTLSRequired)
	}
	return &SnapshotClient{
		conn:             conn,
		rpc:              routecontrolpb.NewRouteSnapshotServiceClient(conn),
		opts:             options,
		sem:              make(chan struct{}, options.maxConcurrent),
		resyncGate:       make(chan struct{}, 1),
		closedCh:         make(chan struct{}),
		terminalCh:       make(chan struct{}),
		activeWatches:    make(map[uint64]context.CancelFunc),
		activeOperations: make(map[uint64]context.CancelFunc),
		retiredBoots:     make(map[string]struct{}, options.retiredBootLimit),
		closeFn:          closeFn,
	}, nil
}

func (c *SnapshotClient) FetchFullSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	if c == nil {
		return actorrouting.RouteSnapshot{}, ErrClientClosed
	}
	callCtx, finish, err := c.beginOrdinaryOperation(ctx, false)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer finish()
	callCtx, cancel, err := c.unaryContext(callCtx)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer cancel()
	envelope, err := c.rpc.GetSnapshot(callCtx, &routecontrolpb.GetSnapshotRequest{})
	if err != nil {
		if isGRPCContractError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeMalformed, err))
		} else if isGRPCMessageCapacityError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeCapacity, err))
		}
		return actorrouting.RouteSnapshot{}, err
	}
	stored, err := c.validateEnvelope(envelope)
	if err != nil {
		c.recordError(err)
		return actorrouting.RouteSnapshot{}, err
	}
	return stored.Snapshot, nil
}

// FetchFullSnapshotForResync is the only operation permitted to accept a new
// controller Boot. The Pump supplies its exact installed complete snapshot;
// this call is valid only after the prior Watch has closed and unregistered.
func (c *SnapshotClient) FetchFullSnapshotForResync(ctx context.Context, baseline remote.ResyncBaseline) (actorrouting.RouteSnapshot, error) {
	if c == nil {
		return actorrouting.RouteSnapshot{}, ErrClientClosed
	}
	callCtx, finish, err := c.beginRolloverOperation(ctx)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer finish()

	baselineSnapshot := actorrouting.CloneRouteSnapshot(baseline.Snapshot)
	if baseline.Revision == 0 || baselineSnapshot.GlobalRevision != baseline.Revision {
		err := fmt.Errorf("%w: baseline revision=%d snapshot revision=%d", ErrBootRolloverContract, baseline.Revision, baselineSnapshot.GlobalRevision)
		c.recordError(err)
		return actorrouting.RouteSnapshot{}, err
	}
	baselineStored, err := core.CanonicalizeRouteSnapshot(baselineSnapshot)
	if err != nil {
		err = fmt.Errorf("%w: baseline snapshot: %v", ErrBootRolloverContract, err)
		c.recordError(err)
		return actorrouting.RouteSnapshot{}, err
	}

	callCtx, cancel, err := c.unaryContext(callCtx)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer cancel()
	envelope, err := c.rpc.GetSnapshot(callCtx, &routecontrolpb.GetSnapshotRequest{})
	if err != nil {
		if isGRPCContractError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeMalformed, err))
		} else if isGRPCMessageCapacityError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeCapacity, err))
		}
		return actorrouting.RouteSnapshot{}, err
	}
	stored, err := c.validateRolloverEnvelope(envelope, baselineStored)
	if err != nil {
		c.recordError(err)
		return actorrouting.RouteSnapshot{}, err
	}
	return stored.Snapshot, nil
}

func (c *SnapshotClient) GetSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	return c.FetchFullSnapshot(ctx)
}

// GetCurrentRevision still returns the complete validated snapshot, matching
// the server method's deliberately non-scalar response contract.
func (c *SnapshotClient) GetCurrentRevision(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	if c == nil {
		return actorrouting.RouteSnapshot{}, ErrClientClosed
	}
	callCtx, finish, err := c.beginOrdinaryOperation(ctx, false)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer finish()
	callCtx, cancel, err := c.unaryContext(callCtx)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	defer cancel()
	envelope, err := c.rpc.GetCurrentRevision(callCtx, &routecontrolpb.GetCurrentRevisionRequest{})
	if err != nil {
		if isGRPCContractError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeMalformed, err))
		} else if isGRPCMessageCapacityError(err) {
			c.recordError(fmt.Errorf("%w: %v", ErrEnvelopeCapacity, err))
		}
		return actorrouting.RouteSnapshot{}, err
	}
	stored, err := c.validateEnvelope(envelope)
	if err != nil {
		c.recordError(err)
		return actorrouting.RouteSnapshot{}, err
	}
	return stored.Snapshot, nil
}

func (c *SnapshotClient) FetchCurrentRevision(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	return c.GetCurrentRevision(ctx)
}

// WatchFrom adapts the authenticated full-envelope gRPC stream to B's
// transport-neutral complete RouteSnapshot channel. It does not expose a
// channel until the server has authorized and established a real Hub
// subscription and this client has synchronously received and fully validated
// its mandatory complete-current handshake envelope. A bounded output queue
// closes an established stream if the local consumer stops draining it.
func (c *SnapshotClient) WatchFrom(ctx context.Context, afterRevision uint64) (<-chan actorrouting.RouteSnapshot, error) {
	if c == nil {
		return nil, ErrClientClosed
	}
	if ctx == nil {
		return nil, context.Canceled
	}
	streamCtx, finish, err := c.beginOrdinaryOperation(ctx, true)
	if err != nil {
		return nil, err
	}
	watchCtx, cancelWatch := context.WithCancel(streamCtx)
	type establishmentResult struct {
		stream   grpc.ServerStreamingClient[routecontrolpb.RouteSnapshotEnvelope]
		envelope *routecontrolpb.RouteSnapshotEnvelope
		err      error
	}
	established := make(chan establishmentResult, 1)
	go func() {
		stream, watchErr := c.rpc.WatchFrom(watchCtx, &routecontrolpb.WatchFromRequest{AfterRevision: afterRevision})
		if watchErr != nil {
			established <- establishmentResult{err: watchErr}
			return
		}
		envelope, recvErr := stream.Recv()
		established <- establishmentResult{stream: stream, envelope: envelope, err: recvErr}
	}()

	timer := time.NewTimer(c.opts.callTimeout)
	var result establishmentResult
	select {
	case result = <-established:
		timer.Stop()
	case <-streamCtx.Done():
		timer.Stop()
		cancelWatch()
		<-established
		finish()
		if stateErr := c.stateError(); stateErr != nil {
			return nil, stateErr
		}
		return nil, streamCtx.Err()
	case <-timer.C:
		cancelWatch()
		<-established
		finish()
		return nil, context.DeadlineExceeded
	}
	if result.err != nil {
		cancelWatch()
		c.recordError(result.err)
		finish()
		return nil, result.err
	}
	stored, err := c.validateEnvelope(result.envelope)
	if err != nil {
		cancelWatch()
		c.recordError(err)
		finish()
		return nil, err
	}
	if stored.Revision < afterRevision {
		err := fmt.Errorf("%w: watch handshake revision=%d after=%d", ErrEnvelopeMalformed, stored.Revision, afterRevision)
		cancelWatch()
		c.recordError(err)
		finish()
		return nil, err
	}

	// Reserve one bounded slot for the mandatory handshake in addition to the
	// configured ordinary-update queue. Thus a minimum-size ordinary queue can
	// still accept a racing R+1 before the caller begins draining R.
	out := make(chan actorrouting.RouteSnapshot, c.opts.watchBuffer+1)
	// The authenticated handshake is the first Pump-visible value and cannot
	// be dropped by the ordinary non-blocking slow-consumer path.
	out <- actorrouting.CloneRouteSnapshot(stored.Snapshot)
	go func() {
		defer close(out)
		defer cancelWatch()
		defer finish()
		for {
			envelope, recvErr := result.stream.Recv()
			if recvErr != nil {
				if !errors.Is(recvErr, io.EOF) && !errors.Is(recvErr, context.Canceled) {
					c.recordError(recvErr)
				}
				return
			}
			stored, decodeErr := c.validateEnvelope(envelope)
			if decodeErr != nil {
				c.recordError(decodeErr)
				return
			}
			snapshot := stored.Snapshot
			select {
			case out <- actorrouting.CloneRouteSnapshot(snapshot):
			case <-streamCtx.Done():
				return
			default:
				c.recordError(ErrWatchBackpressure)
				return
			}
		}
	}()
	return out, nil
}

// LastWatchError reports the most recent asynchronous stream failure. A
// permanent contract error is sticky and takes precedence over transient
// diagnostics.
func (c *SnapshotClient) LastWatchError() error {
	if c == nil {
		return ErrClientClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminalErr != nil {
		return c.terminalErr
	}
	return c.lastErr
}

func (c *SnapshotClient) validateEnvelope(envelope *routecontrolpb.RouteSnapshotEnvelope) (core.StoredRouteSnapshot, error) {
	if c == nil {
		return core.StoredRouteSnapshot{}, ErrClientClosed
	}
	stored, err := decodeEnvelope(envelope, c.opts.maxMessageBytes)
	if err != nil {
		return core.StoredRouteSnapshot{}, err
	}
	controllerID := strings.TrimSpace(envelope.GetControllerId())
	boot := strings.TrimSpace(envelope.GetBoot())
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.validatePinnedEnvelopeLocked(controllerID, boot, stored); err != nil {
		return core.StoredRouteSnapshot{}, err
	}
	if c.pinnedBoot == "" {
		c.pinnedBoot = boot
		c.pinnedRealm = stored.Realm
		c.pinnedTopology = stored.TopologyFingerprint
	}
	c.acceptEnvelopeLocked(boot, stored)
	return stored, nil
}

func (c *SnapshotClient) validatePinnedEnvelopeLocked(controllerID, boot string, stored core.StoredRouteSnapshot) error {
	if err := c.stateErrorLocked(); err != nil {
		return err
	}
	if controllerID != c.opts.expectedControllerID {
		return fmt.Errorf("%w: got %q", ErrEnvelopeControllerMismatch, controllerID)
	}
	if c.opts.expectedRealm != "" && stored.Realm != c.opts.expectedRealm {
		return fmt.Errorf("%w: got %q", ErrEnvelopeRealmMismatch, stored.Realm)
	}
	if c.pinnedRealm != "" && c.pinnedRealm != stored.Realm {
		return fmt.Errorf("%w: got %q want %q", ErrEnvelopeRealmMismatch, stored.Realm, c.pinnedRealm)
	}
	if c.opts.expectedTopology != "" && stored.TopologyFingerprint != c.opts.expectedTopology {
		return fmt.Errorf("%w: got %q", ErrEnvelopeTopologyDrift, stored.TopologyFingerprint)
	}
	if c.pinnedTopology != "" && c.pinnedTopology != stored.TopologyFingerprint {
		return fmt.Errorf("%w: got %q want %q", ErrEnvelopeTopologyDrift, stored.TopologyFingerprint, c.pinnedTopology)
	}
	if _, retired := c.retiredBoots[boot]; retired {
		return fmt.Errorf("%w: got %q", ErrEnvelopeBootRetired, boot)
	}
	if c.pinnedBoot != "" && c.pinnedBoot != boot {
		return fmt.Errorf("%w: got %q want %q", ErrEnvelopeBootDrift, boot, c.pinnedBoot)
	}
	return nil
}

func (c *SnapshotClient) validateRolloverEnvelope(envelope *routecontrolpb.RouteSnapshotEnvelope, baseline core.StoredRouteSnapshot) (core.StoredRouteSnapshot, error) {
	stored, err := decodeEnvelope(envelope, c.opts.maxMessageBytes)
	if err != nil {
		return core.StoredRouteSnapshot{}, err
	}
	controllerID := strings.TrimSpace(envelope.GetControllerId())
	rawBoot := envelope.GetBoot()
	boot := strings.TrimSpace(rawBoot)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.stateErrorLocked(); err != nil {
		return core.StoredRouteSnapshot{}, err
	}
	if c.pinnedBoot == "" || c.pinnedRealm == "" || c.pinnedTopology == "" || !c.hasAccepted {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: rollover requires an accepted pinned generation", ErrBootRolloverContract)
	}
	if controllerID != c.opts.expectedControllerID {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: got %q", ErrEnvelopeControllerMismatch, controllerID)
	}
	if stored.Realm != c.pinnedRealm || (c.opts.expectedRealm != "" && stored.Realm != c.opts.expectedRealm) {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: got %q want %q", ErrEnvelopeRealmMismatch, stored.Realm, c.pinnedRealm)
	}
	if stored.TopologyFingerprint != c.pinnedTopology ||
		(c.opts.expectedTopology != "" && stored.TopologyFingerprint != c.opts.expectedTopology) {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: got %q want %q", ErrEnvelopeTopologyDrift, stored.TopologyFingerprint, c.pinnedTopology)
	}
	if baseline.Revision == 0 || baseline.Realm != c.pinnedRealm || baseline.TopologyFingerprint != c.pinnedTopology {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: baseline does not match the pinned realm/topology", ErrBootRolloverContract)
	}

	// A same-Boot resync is the pre-existing reconnect path. Keep its stale,
	// duplicate, and Pump-side validation behavior unchanged.
	if boot == c.pinnedBoot {
		c.acceptEnvelopeLocked(boot, stored)
		return stored, nil
	}
	if rawBoot != boot || boot == "" {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: new boot must be canonical and nonempty", ErrBootRolloverContract)
	}
	if _, retired := c.retiredBoots[boot]; retired {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: got %q", ErrEnvelopeBootRetired, boot)
	}

	floor := baseline.Revision
	if c.accepted.Revision > floor {
		floor = c.accepted.Revision
	}
	if stored.Revision < floor {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: got revision=%d floor=%d", ErrBootRolloverRollback, stored.Revision, floor)
	}
	if stored.Revision == baseline.Revision {
		if c.acceptedBoot != c.pinnedBoot || c.accepted.Revision != baseline.Revision || c.acceptedConflict ||
			!c.accepted.EnvelopeEqual(baseline) {
			return core.StoredRouteSnapshot{}, fmt.Errorf("%w: installed baseline is not the exact accepted envelope", ErrBootRolloverContract)
		}
		expected := baseline.Clone()
		expected.CommittedAtMs = c.accepted.CommittedAtMs
		if !durableEnvelopeEqual(stored, expected) {
			return core.StoredRouteSnapshot{}, fmt.Errorf("%w: revision=%d", ErrBootRolloverMismatch, stored.Revision)
		}
	}
	if len(c.retiredBoots) >= c.opts.retiredBootLimit {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: limit=%d", ErrRetiredBootCapacity, c.opts.retiredBootLimit)
	}

	// Every precondition above is read-only. Retiring the old Boot, pinning the
	// new one, and replacing the transport acceptance floor are one mutation.
	c.retiredBoots[c.pinnedBoot] = struct{}{}
	c.pinnedBoot = boot
	c.accepted = stored.Clone()
	c.acceptedBoot = boot
	c.hasAccepted = true
	c.acceptedConflict = false
	return stored, nil
}

func durableEnvelopeEqual(left, right core.StoredRouteSnapshot) bool {
	return left.CommittedAtMs == right.CommittedAtMs && left.EnvelopeEqual(right)
}

func (c *SnapshotClient) acceptEnvelopeLocked(boot string, stored core.StoredRouteSnapshot) {
	if !c.hasAccepted || stored.Revision > c.accepted.Revision {
		c.accepted = stored.Clone()
		c.acceptedBoot = boot
		c.hasAccepted = true
		c.acceptedConflict = false
		return
	}
	if stored.Revision == c.accepted.Revision &&
		(c.acceptedBoot != boot || !durableEnvelopeEqual(c.accepted, stored)) {
		c.acceptedConflict = true
	}
}

func (c *SnapshotClient) beginOrdinaryOperation(ctx context.Context, watch bool) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, context.Canceled
	}
	if err := c.admitCall(); err != nil {
		return nil, nil, err
	}
	release, err := c.acquire(ctx)
	if err != nil {
		c.callWG.Done()
		return nil, nil, err
	}
	c.bootMu.RLock()
	opCtx, operationID, cancel, err := c.registerOperation(ctx, watch)
	if err != nil {
		c.bootMu.RUnlock()
		release()
		c.callWG.Done()
		return nil, nil, err
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			cancel()
			c.finishOperation(operationID)
			c.bootMu.RUnlock()
			release()
			c.callWG.Done()
		})
	}
	return opCtx, finish, nil
}

func (c *SnapshotClient) beginRolloverOperation(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, context.Canceled
	}
	if err := c.admitCall(); err != nil {
		return nil, nil, err
	}
	select {
	case c.resyncGate <- struct{}{}:
	case <-c.closedCh:
		c.callWG.Done()
		return nil, nil, ErrClientClosed
	case <-c.terminalCh:
		c.callWG.Done()
		return nil, nil, c.terminalError()
	case <-ctx.Done():
		c.callWG.Done()
		return nil, nil, ctx.Err()
	}
	releaseGate := func() { <-c.resyncGate }
	if !c.bootMu.TryLock() {
		releaseGate()
		if err := c.stateError(); err != nil {
			c.callWG.Done()
			return nil, nil, err
		}
		err := ErrBootRolloverOverlap
		c.recordError(err)
		c.callWG.Done()
		return nil, nil, err
	}
	opCtx, operationID, cancel, err := c.registerOperation(ctx, false)
	if err != nil {
		c.bootMu.Unlock()
		releaseGate()
		c.callWG.Done()
		return nil, nil, err
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			cancel()
			c.finishOperation(operationID)
			c.bootMu.Unlock()
			releaseGate()
			c.callWG.Done()
		})
	}
	return opCtx, finish, nil
}

func (c *SnapshotClient) registerOperation(ctx context.Context, watch bool) (context.Context, uint64, context.CancelFunc, error) {
	if ctx == nil {
		return nil, 0, nil, context.Canceled
	}
	opCtx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if err := c.stateErrorLocked(); err != nil {
		c.mu.Unlock()
		cancel()
		return nil, 0, nil, err
	}
	c.nextOperationID++
	id := c.nextOperationID
	c.activeOperations[id] = cancel
	if watch {
		c.activeWatches[id] = cancel
	}
	c.mu.Unlock()
	return opCtx, id, cancel, nil
}

func (c *SnapshotClient) finishOperation(id uint64) {
	c.mu.Lock()
	delete(c.activeOperations, id)
	delete(c.activeWatches, id)
	c.mu.Unlock()
}

func (c *SnapshotClient) admitCall() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.stateErrorLocked(); err != nil {
		return err
	}
	c.callWG.Add(1)
	return nil
}

func (c *SnapshotClient) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := c.stateError(); err != nil {
		return nil, err
	}
	select {
	case c.sem <- struct{}{}:
		if err := c.stateError(); err != nil {
			<-c.sem
			return nil, err
		}
		return func() { <-c.sem }, nil
	case <-c.closedCh:
		return nil, ErrClientClosed
	case <-c.terminalCh:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *SnapshotClient) stateError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stateErrorLocked()
}

func (c *SnapshotClient) stateErrorLocked() error {
	if c.closed {
		return ErrClientClosed
	}
	if c.terminalErr != nil {
		return c.terminalErr
	}
	return nil
}

func (c *SnapshotClient) terminalError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminalErr != nil {
		return c.terminalErr
	}
	if c.closed {
		return ErrClientClosed
	}
	return ErrEnvelopeMalformed
}

func (c *SnapshotClient) unaryContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, c.opts.callTimeout)
	return callCtx, cancel, nil
}

func (c *SnapshotClient) recordError(err error) {
	if c == nil || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if isGRPCMessageCapacityError(err) {
		err = fmt.Errorf("%w: %v", ErrEnvelopeCapacity, err)
	}
	if isGRPCContractError(err) {
		err = fmt.Errorf("%w: %v", ErrEnvelopeMalformed, err)
	}
	if isPermanentContractError(err) {
		c.latchTerminal(err)
		return
	}
	c.mu.Lock()
	if !c.closed && c.terminalErr == nil {
		c.lastErr = err
	}
	c.mu.Unlock()
}

func (c *SnapshotClient) latchTerminal(err error) {
	if c == nil || err == nil {
		return
	}
	var cancels []context.CancelFunc
	c.mu.Lock()
	if c.closed || c.terminalErr != nil {
		c.mu.Unlock()
		return
	}
	c.terminalErr = err
	c.lastErr = err
	close(c.terminalCh)
	for _, cancel := range c.activeOperations {
		cancels = append(cancels, cancel)
	}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func isPermanentContractError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrEnvelopeMalformed) ||
		errors.Is(err, ErrEnvelopeControllerMismatch) ||
		errors.Is(err, ErrEnvelopeBootDrift) ||
		errors.Is(err, ErrEnvelopeBootRetired) ||
		errors.Is(err, ErrEnvelopeRealmMismatch) ||
		errors.Is(err, ErrEnvelopeTopologyDrift) ||
		errors.Is(err, ErrEnvelopeCapacity) ||
		errors.Is(err, ErrBootRolloverOverlap) ||
		errors.Is(err, ErrBootRolloverRollback) ||
		errors.Is(err, ErrBootRolloverMismatch) ||
		errors.Is(err, ErrBootRolloverContract) ||
		errors.Is(err, ErrRetiredBootCapacity) ||
		errors.Is(err, core.ErrSnapshotEnvelopeMalformed) ||
		errors.Is(err, core.ErrSnapshotSchemaMismatch) ||
		errors.Is(err, core.ErrSnapshotChecksumMismatch) ||
		errors.Is(err, core.ErrSnapshotTopologyMismatch) ||
		errors.Is(err, core.ErrSnapshotCapacity)
}

func (c *SnapshotClient) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		var cancels []context.CancelFunc
		c.mu.Lock()
		c.closed = true
		close(c.closedCh)
		for _, cancel := range c.activeOperations {
			cancels = append(cancels, cancel)
		}
		closeFn := c.closeFn
		c.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		var err error
		if closeFn == nil {
			err = fmt.Errorf("%w: owned connection close function missing", ErrTLSRequired)
		} else {
			err = closeFn()
		}
		c.callWG.Wait()
		c.mu.Lock()
		c.closeErr = err
		c.mu.Unlock()
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}
