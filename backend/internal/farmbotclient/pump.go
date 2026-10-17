package farmbotclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	settlementpb "farmgame/api/gen/settlement"
	wireprotocol "farmgame/internal/gateway/protocol"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const (
	// defaultIdleTimeout must stay above the gate ping period so an otherwise
	// idle session is kept alive by the server ping rather than torn down.
	defaultIdleTimeout = 60 * time.Second
	pongWriteWait      = 5 * time.Second

	// resultWaiterCapacity leaves room for a duplicate result on the same
	// operation so the join barrier can still detect a changed fence.
	resultWaiterCapacity = 4
	joinEventCapacity    = 64
)

// joinEvent is one frame the join barrier must see, in the order the wire
// delivered it. Results and pushes travel together because the barrier
// enforces an ordering rule between them: SUBSCRIPTION_READY is only legal
// after both ACCEPTED and FARM_SNAPSHOT. Two channels would let the barrier
// observe them in an order the server never sent.
type joinEvent struct {
	result *commonpb.CommandResultEnvelope
	push   *commonpb.ServerPushEnvelope
}

// SessionStats is a bounded, low-cardinality account of what one connection's
// background reader has drained. It carries no player, credential, or payload
// material and is safe to place in a run summary.
type SessionStats struct {
	FramesRead     uint64 `json:"frames_read"`
	CommandResults uint64 `json:"command_results"`
	Pushes         uint64 `json:"pushes"`
	// BroadcastsApplied were fed to the farm view; BroadcastsDrained were read
	// and discarded because nothing was tracking the realtime stream. The
	// difference is what tells a reviewer whether a bot was really following
	// the farm it was acting on.
	BroadcastsApplied uint64 `json:"broadcasts_applied"`
	BroadcastsDrained uint64 `json:"broadcasts_drained"`
	UnroutedResults   uint64 `json:"unrouted_results"`
	// OperationPushes are OPERATION_RESULT notifications. They are counted
	// apart from broadcasts because they are the server saying an async
	// operation finished, and a resolution that waits for them measures the
	// server; one that waits for a timer measures the timer.
	OperationPushes uint64 `json:"operation_pushes"`
	// UnroutedPushes arrived for an operation nobody was waiting on, which
	// means the notification lost the race against the caller's own poll.
	UnroutedPushes uint64 `json:"unrouted_pushes"`
	// ResultQueries counts GET_OPERATION_RESULT commands. They are commands
	// like any other and the server pays for them, so a run that reports only
	// its game actions can be understating the load by an order of magnitude
	// while blaming the server for the latency its own polling caused.
	ResultQueries uint64 `json:"result_queries"`
}

// Add accumulates another connection's counters, which lets a caller report
// one total for a session that reconnected.
func (stats *SessionStats) Add(other SessionStats) {
	stats.FramesRead += other.FramesRead
	stats.CommandResults += other.CommandResults
	stats.Pushes += other.Pushes
	stats.BroadcastsApplied += other.BroadcastsApplied
	stats.BroadcastsDrained += other.BroadcastsDrained
	stats.UnroutedResults += other.UnroutedResults
	stats.OperationPushes += other.OperationPushes
	stats.UnroutedPushes += other.UnroutedPushes
	stats.ResultQueries += other.ResultQueries
}

// framePump is the single reader of one WebSocket connection.
//
// Reading continuously is what makes a long-lived bot session realistic: the
// gate pings on a fixed period and closes connections that stop answering,
// and an unread broadcast backlog applies backpressure to the gate write path
// instead of to the bot. The pump answers pings, enforces the inbound
// sequence contract, hands command results to the goroutine waiting on that
// operation, feeds an active join barrier, and counts and discards everything
// else.
type framePump struct {
	conn     *websocket.Conn
	maxFrame int
	idle     time.Duration
	// onPush receives every push not consumed by an active join barrier. It
	// runs on the reader goroutine and must not block.
	onPush func(*commonpb.ServerPushEnvelope)

	// sequence is only touched by the reader goroutine.
	sequence uint64

	mu      sync.Mutex
	results map[string]chan *commonpb.CommandResultEnvelope
	// wakeups lets a caller resolving an async operation sleep until the
	// server says that operation settled, instead of until a retry timer
	// expires. The channel carries no outcome: the authoritative answer still
	// comes from GET_OPERATION_RESULT, which validates identity. This only
	// removes the delay between the server finishing and the client asking.
	wakeups map[string]chan struct{}
	// joinEvents is non-nil only while a join barrier is active, and
	// joinOperation is the operation whose results belong to that barrier.
	joinEvents    chan joinEvent
	joinOperation string
	failure       error
	stats         SessionStats

	failed chan struct{}
	done   chan struct{}
}

// startFramePump takes over the read side of an already handshaken
// connection. inboundSequence is the last sequence consumed by the handshake.
func startFramePump(conn *websocket.Conn, maxFrame int, idle time.Duration, inboundSequence uint64, onPush func(*commonpb.ServerPushEnvelope)) *framePump {
	if idle <= 0 {
		idle = defaultIdleTimeout
	}
	pump := &framePump{
		conn:     conn,
		maxFrame: maxFrame,
		idle:     idle,
		onPush:   onPush,
		sequence: inboundSequence,
		results:  make(map[string]chan *commonpb.CommandResultEnvelope),
		wakeups:  make(map[string]chan struct{}),
		failed:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(idle))
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(pongWriteWait))
		if errors.Is(err, websocket.ErrCloseSent) || os.IsTimeout(err) {
			return nil
		}
		return err
	})
	go pump.loop()
	return pump
}

func (pump *framePump) loop() {
	defer close(pump.done)
	for {
		frame, err := pump.read()
		if err != nil {
			pump.fail(err)
			return
		}
		if err := pump.route(frame); err != nil {
			pump.fail(err)
			return
		}
	}
}

func (pump *framePump) read() (*commonpb.FarmFrame, error) {
	if err := pump.conn.SetReadDeadline(time.Now().Add(pump.idle)); err != nil {
		return nil, err
	}
	messageType, raw, err := pump.conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("farmbotclient: websocket read: %w", err)
	}
	if messageType != websocket.BinaryMessage {
		return nil, protocolFailure("read", errors.New("only binary WebSocket messages are accepted"))
	}
	frame, err := wireprotocol.Decode(raw, pump.maxFrame)
	if err != nil {
		return nil, protocolFailure("decode", err)
	}
	if err := wireprotocol.ValidateServerFrame(frame); err != nil {
		return nil, protocolFailure("server direction", err)
	}
	if pump.sequence == ^uint64(0) {
		return nil, protocolFailure("read sequence", errors.New("inbound sequence exhausted"))
	}
	expected := pump.sequence + 1
	if frame.GetSequence() != expected {
		return nil, protocolFailure("read sequence", fmt.Errorf("got %d, want %d", frame.GetSequence(), expected))
	}
	pump.sequence = frame.GetSequence()
	pump.mu.Lock()
	pump.stats.FramesRead++
	pump.mu.Unlock()
	return frame, nil
}

func (pump *framePump) route(frame *commonpb.FarmFrame) error {
	if control := frame.GetControl(); control != nil {
		if connectionError := control.GetConnectionError(); connectionError != nil {
			return &RemoteConnectionError{Code: connectionError.GetErrorCode(), Message: connectionError.GetErrorMessage()}
		}
		return nil
	}
	if result := frame.GetCommandResult(); result != nil {
		return pump.routeResult(result)
	}
	if push := frame.GetServerPush(); push != nil {
		return pump.routePush(push)
	}
	pump.mu.Lock()
	pump.stats.BroadcastsDrained++
	pump.mu.Unlock()
	return nil
}

func (pump *framePump) routeResult(result *commonpb.CommandResultEnvelope) error {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	pump.stats.CommandResults++
	if pump.joinEvents != nil && result.GetOperationId() == pump.joinOperation {
		return pump.deliverJoinLocked(joinEvent{result: result})
	}
	waiter, waiting := pump.results[result.GetOperationId()]
	if !waiting {
		pump.stats.UnroutedResults++
		return nil
	}
	select {
	case waiter <- result:
	default:
		pump.stats.UnroutedResults++
	}
	return nil
}

func (pump *framePump) routePush(push *commonpb.ServerPushEnvelope) error {
	pump.mu.Lock()
	pump.stats.Pushes++
	if pump.joinEvents != nil && joinBarrierPush(push.GetPushType()) {
		err := pump.deliverJoinLocked(joinEvent{push: push})
		pump.mu.Unlock()
		return err
	}
	if push.GetPushType() == commonpb.PushType_OPERATION_RESULT {
		pump.stats.OperationPushes++
		operationID, err := operationResultID(push)
		if err != nil {
			pump.mu.Unlock()
			return err
		}
		pump.wakeLocked(operationID)
		pump.mu.Unlock()
		return nil
	}
	observe := pump.onPush
	if observe != nil {
		pump.stats.BroadcastsApplied++
	} else {
		pump.stats.BroadcastsDrained++
	}
	pump.mu.Unlock()
	// The handler keeps the client-side farm view current. It runs outside the
	// pump lock so a slow handler cannot stall result delivery.
	if observe != nil {
		observe(push)
	}
	return nil
}

// deliverJoinLocked hands one frame to the active barrier. A full buffer means
// the barrier stopped reading while the server kept sending, which the client
// cannot reconcile, so it is a protocol failure rather than a silent drop.
func (pump *framePump) deliverJoinLocked(event joinEvent) error {
	select {
	case pump.joinEvents <- event:
		return nil
	default:
		return protocolFailure("join backlog", errors.New("join barrier backlog overflowed"))
	}
}

// operationResultID reads the settled operation out of a notification. A
// notification the client cannot attribute is a protocol failure rather than
// something to ignore: silently dropping it would leave the caller waiting on
// its retry timer while believing it was waiting on the server.
func operationResultID(push *commonpb.ServerPushEnvelope) (string, error) {
	var settled settlementpb.OperationResult
	if err := proto.Unmarshal(push.GetPayload(), &settled); err != nil {
		return "", protocolFailure("decode operation notification", err)
	}
	operationID := settled.GetOperationId()
	if operationID == "" {
		return "", protocolFailure("operation notification",
			errors.New("OPERATION_RESULT push carries no operation_id"))
	}
	return operationID, nil
}

// wakeLocked signals the resolver waiting on an operation. The channel has a
// slot so the notification is never lost to a resolver that has not reached
// its wait yet, and a second notification for the same operation is dropped
// because one wakeup is all a resolver needs.
func (pump *framePump) wakeLocked(operationID string) {
	wakeup, waiting := pump.wakeups[operationID]
	if !waiting {
		pump.stats.UnroutedPushes++
		return
	}
	select {
	case wakeup <- struct{}{}:
	default:
	}
}

// watchOperation registers interest in an operation's completion notification
// before the operation is issued. The returned release must be called by the
// resolver, otherwise the pump would retain an entry per operation for the
// life of the connection.
func (pump *framePump) watchOperation(operationID string) (<-chan struct{}, func()) {
	if pump == nil || operationID == "" {
		return nil, func() {}
	}
	wakeup := make(chan struct{}, 1)
	pump.mu.Lock()
	pump.wakeups[operationID] = wakeup
	pump.mu.Unlock()
	return wakeup, func() {
		pump.mu.Lock()
		delete(pump.wakeups, operationID)
		pump.mu.Unlock()
	}
}

func joinBarrierPush(pushType commonpb.PushType) bool {
	switch pushType {
	case commonpb.PushType_FARM_SNAPSHOT, commonpb.PushType_SUBSCRIPTION_READY, commonpb.PushType_REJOIN_REQUIRED:
		return true
	default:
		return false
	}
}

// fail records the first terminal reason and closes the connection so every
// waiter is released and the reader goroutine can exit.
func (pump *framePump) fail(cause error) {
	pump.mu.Lock()
	if pump.failure == nil {
		if cause == nil {
			cause = ErrNotConnected
		}
		pump.failure = cause
		close(pump.failed)
	}
	pump.mu.Unlock()
	_ = pump.conn.Close()
}

func (pump *framePump) err() error {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	return pump.failure
}

// expect registers the caller as the only receiver of results for one
// operation. The caller must release the operation when it is done.
func (pump *framePump) expect(operationID string) (<-chan *commonpb.CommandResultEnvelope, error) {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if pump.failure != nil {
		return nil, pump.failure
	}
	if _, inFlight := pump.results[operationID]; inFlight {
		return nil, errors.New("farmbotclient: operation_id is already in flight on this session")
	}
	waiter := make(chan *commonpb.CommandResultEnvelope, resultWaiterCapacity)
	pump.results[operationID] = waiter
	return waiter, nil
}

func (pump *framePump) release(operationID string) {
	pump.mu.Lock()
	delete(pump.results, operationID)
	pump.mu.Unlock()
}

// beginJoin routes the join operation's results and the barrier pushes to the
// caller, in arrival order, until endJoin. Outside a join the pump counts
// pushes as drained broadcasts.
func (pump *framePump) beginJoin(operationID string) (<-chan joinEvent, error) {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	if pump.failure != nil {
		return nil, pump.failure
	}
	if pump.joinEvents != nil {
		return nil, errors.New("farmbotclient: a join barrier is already active on this session")
	}
	if _, inFlight := pump.results[operationID]; inFlight {
		return nil, errors.New("farmbotclient: operation_id is already in flight on this session")
	}
	pump.joinEvents = make(chan joinEvent, joinEventCapacity)
	pump.joinOperation = operationID
	return pump.joinEvents, nil
}

func (pump *framePump) endJoin() {
	pump.mu.Lock()
	pump.joinEvents = nil
	pump.joinOperation = ""
	pump.mu.Unlock()
}

// stop closes the connection without waiting for the reader goroutine, so it
// is safe to call while holding the session lock.
func (pump *framePump) stop() {
	pump.fail(ErrClosed)
}

func (pump *framePump) wait() {
	<-pump.done
}

func (pump *framePump) snapshot() SessionStats {
	pump.mu.Lock()
	defer pump.mu.Unlock()
	return pump.stats
}

// await returns the next result for an expected operation, or the reason the
// caller can stop waiting. A caller deadline does not tear down the
// connection: the operation keeps its identity and stays resolvable through
// GET_OPERATION_RESULT.
//
// A frame already received outranks a connection failure or a deadline: a
// server that answers and then closes has still answered, and reporting the
// close instead would turn a real verdict into a transport error.
func (pump *framePump) await(ctx context.Context, waiter <-chan *commonpb.CommandResultEnvelope) (*commonpb.CommandResultEnvelope, error) {
	select {
	case result := <-waiter:
		return result, nil
	default:
	}
	select {
	case result := <-waiter:
		return result, nil
	case <-pump.failed:
		return nil, pump.err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// awaitJoinEvent applies the same rule to the join barrier.
func (pump *framePump) awaitJoinEvent(ctx context.Context, events <-chan joinEvent) (joinEvent, error) {
	select {
	case event := <-events:
		return event, nil
	default:
	}
	select {
	case event := <-events:
		return event, nil
	case <-pump.failed:
		return joinEvent{}, pump.err()
	case <-ctx.Done():
		return joinEvent{}, ctx.Err()
	}
}
