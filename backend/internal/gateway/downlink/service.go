package downlink

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	friendpb "farmgame/api/gen/friend"
	logicrpcpb "farmgame/api/gen/logicrpc"
	gateprotocol "farmgame/internal/gateway/protocol"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

var ErrInvalidFrame = errors.New("downlink: invalid FarmFrame")

type connectionEntry struct {
	playerID      uint64
	subscription  string
	activeAttempt string
	generation    uint64
	presenceEpoch uint64
	targetFence   *logicrpcpb.DownlinkTargetFence
	revoked       bool
	writer        *ConnectionWriter
	nextSeq       uint64
	acceptedItems map[string]struct{}
	itemOrder     []string
	pendingJoin   *pendingJoinFence
}

type pendingJoinFence struct {
	operationID         string
	attemptID           string
	farmID              uint64
	subscriptionID      string
	presenceEpoch       uint64
	snapshotVersion     uint64
	readyThroughVersion uint64
	accepted            bool
	snapshotQueued      bool
	stage               pendingJoinStage
	state               pendingJoinFenceState
	createdAt           time.Time
}

type pendingJoinStage uint8

const (
	pendingJoinStageCreated pendingJoinStage = iota + 1
	pendingJoinStageAcceptedQueued
	pendingJoinStageSnapshotArmed
	pendingJoinStageSnapshotQueued
	pendingJoinStageFlushing
	pendingJoinStageReadyQueued
	pendingJoinStageActive
)

type pendingJoinFenceState uint8

const (
	pendingJoinFencePending pendingJoinFenceState = iota + 1
	pendingJoinFenceCanceling
	pendingJoinFenceClosed
)

// JoinPromotion is emitted only after both halves of the Join barrier are
// observed for the exact connection generation and Join intent.
type JoinPromotion struct {
	ConnectionID   string
	Generation     uint64
	OperationID    string
	FarmID         uint64
	SubscriptionID string
	JoinAttemptID  string
	PresenceEpoch  uint64
}

// SubscriptionTermination is emitted only after the exact old subscription
// has either queued its public REJOIN_REQUIRED frame in the connection FIFO or
// the same writer has been closed because even that terminal frame could not
// be accepted. Gate uses the full fence to revoke only the matching command
// admission context; a delayed terminal callback can never clear a newer Join.
type SubscriptionTermination struct {
	ConnectionID     string
	Generation       uint64
	FarmID           uint64
	SubscriptionID   string
	JoinAttemptID    string
	PresenceEpoch    uint64
	ConnectionClosed bool
}

const maxAcceptedItems = 256

// ConnectionRegistry is the gateway-local index keyed by connection_id. The
// generation check prevents a delayed Logic callback from writing to a reused ID.
type ConnectionRegistry struct {
	logicrpcpb.UnimplementedGatewayDownlinkServiceServer
	mu                     sync.Mutex
	connections            map[string]connectionEntry
	metrics                *Metrics // optional; nil-safe
	joinPromoted           func(JoinPromotion)
	subscriptionTerminated func(SubscriptionTermination)
}

// SetJoinPromotionHandler attaches Gate's ACTIVE-subscription updater. The
// callback is always invoked after the registry lock is released.
func (r *ConnectionRegistry) SetJoinPromotionHandler(handler func(JoinPromotion)) {
	r.mu.Lock()
	r.joinPromoted = handler
	r.mu.Unlock()
}

func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{connections: make(map[string]connectionEntry)}
}

// SetMetrics attaches downlink observability. Safe before serving traffic.
func (r *ConnectionRegistry) SetMetrics(m *Metrics) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = m
}

func (r *ConnectionRegistry) activeBindings() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.connections)
}

// writerStatsLocked aggregates every bound writer's live snapshot while the
// registry lock is held. GaugeFunc collectors re-arm on every scrape, so queue
// depths, hint quotas and oldest-frame ages always reflect current state
// instead of drifting counters. Maps are zero-initialized per scrape and never
// carry identity labels.
func (r *ConnectionRegistry) writerStatsLocked() WriterStats {
	out := WriterStats{
		EnqueuedItems: make(map[FrameKind]uint64), EnqueuedBytes: make(map[FrameKind]uint64),
		Rejected: make(map[string]uint64), OldestAge: make(map[FrameKind]time.Duration),
	}
	for _, entry := range r.connections {
		if entry.writer == nil {
			continue
		}
		stats := entry.writer.Stats()
		out.Items += stats.Items
		out.Bytes += stats.Bytes
		out.HintItems += stats.HintItems
		out.HintBytes += stats.HintBytes
		for kind, count := range stats.EnqueuedItems {
			out.EnqueuedItems[kind] += count
		}
		for kind, count := range stats.EnqueuedBytes {
			out.EnqueuedBytes[kind] += count
		}
		for key, count := range stats.Rejected {
			out.Rejected[key] += count
		}
		for kind, age := range stats.OldestAge {
			if age > out.OldestAge[kind] {
				out.OldestAge[kind] = age
			}
		}
	}
	return out
}

// writerQueueSnapshot returns the allocation-free live queue gauges used by
// Prometheus. It intentionally does not call WriterStats, whose map copies are
// retained only for evidence/fixture consumers.
func (r *ConnectionRegistry) writerQueueSnapshot() QueueSnapshot {
	now := time.Now()
	out := QueueSnapshot{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.connections {
		if entry.writer == nil {
			continue
		}
		snapshot := entry.writer.QueueSnapshot(now)
		out.Items += snapshot.Items
		out.Bytes += snapshot.Bytes
		out.HintItems += snapshot.HintItems
		out.HintBytes += snapshot.HintBytes
		if snapshot.OldestAge > out.OldestAge {
			out.OldestAge = snapshot.OldestAge
		}
	}
	return out
}

// registryWriterSnapshot returns the live registry-wide writer aggregate. It
// is the JSON evidence seam for the C6B fixture. Prometheus GaugeFuncs use
// writerQueueSnapshot instead so a scrape does not allocate maps per writer.
func (r *ConnectionRegistry) registryWriterSnapshot() WriterStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writerStatsLocked()
}

func (r *ConnectionRegistry) pendingJoinStats() (current int, oldestAge time.Duration) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.connections {
		pending := entry.pendingJoin
		if pending == nil || (pending.state != pendingJoinFencePending && pending.state != pendingJoinFenceCanceling) {
			continue
		}
		if pending.stage == pendingJoinStageActive {
			continue
		}
		current++
		if age := now.Sub(pending.createdAt); age > oldestAge {
			oldestAge = age
		}
	}
	return current, oldestAge
}

// Metrics 记录 downlink 投递观测:批量大小(ServerPush 接收者放大)、成功投递数、
// 当前绑定数。batch_items 直方图即 farm_change_qps × avg_subscribers 的原子来源。
type Metrics struct {
	BatchSize                 prometheus.Histogram
	Delivered                 prometheus.Counter
	Bindings                  prometheus.GaugeFunc
	PendingJoinCurrent        prometheus.GaugeFunc
	PendingJoinAge            prometheus.GaugeFunc
	JoinPendingTimeout        prometheus.Counter
	JoinAcceptWithoutSnapshot prometheus.Counter
	JoinSnapshotWithoutAccept prometheus.Counter
	JoinSubscriptionMismatch  prometheus.Counter
	JoinStaleCallback         prometheus.Counter
	LegacyImplicitBind        prometheus.Counter

	// Writer* 是 ConnectionWriter 的聚合观测:kind/result/reason 全部来自
	// 固定白名单,connection_id/player_id 永远不会成为 label 值。计数在
	// writer.enqueue 内部记录,因此 DeliverBatch/ActivateSubscription/W2/
	// Notify* 每一条入队与拒绝路径都会被统计,不存在漏记路径。队列与
	// hint 深度是 GaugeFunc,每次抓取在 registry 锁下聚合所有绑定 writer 的
	// 实时快照,天然反映 Dequeue 后的递减,而不是只增不减的误导值。
	WriterEnqueuedItems *prometheus.CounterVec
	WriterEnqueuedBytes *prometheus.CounterVec
	WriterRejected      *prometheus.CounterVec
	WriterQueueItems    prometheus.GaugeFunc
	WriterQueueBytes    prometheus.GaugeFunc
	WriterHintItems     prometheus.GaugeFunc
	WriterHintBytes     prometheus.GaugeFunc
	WriterOldestAge     prometheus.GaugeFunc
}

// NewMetrics 注册并返回 downlink 指标。reg 为 nil 时使用 DefaultRegisterer。
func NewMetrics(reg prometheus.Registerer, r *ConnectionRegistry) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "farmgate", Subsystem: "downlink", Name: "batch_items",
			Help:    "DeliverBatch request item count (ServerPush fan-out size).",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		}),
		Delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "farmgate", Subsystem: "downlink", Name: "delivered_total",
			Help: "Downlink frames successfully enqueued to connection writers.",
		}),
	}
	m.Bindings = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink", Name: "bindings_active",
		Help: "Current connection registry bindings.",
	}, func() float64 { return float64(r.activeBindings()) })
	m.PendingJoinCurrent = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farm_gate", Name: "pending_join_current",
		Help: "Current non-terminal PendingJoin records across local connections.",
	}, func() float64 { current, _ := r.pendingJoinStats(); return float64(current) })
	m.PendingJoinAge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farm_gate", Name: "pending_join_age_seconds",
		Help: "Age in seconds of the oldest non-terminal local PendingJoin.",
	}, func() float64 { _, age := r.pendingJoinStats(); return age.Seconds() })
	m.JoinPendingTimeout = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "join_pending_timeout_total",
		Help: "PendingJoin records closed by their shared liveness deadline.",
	})
	m.JoinAcceptWithoutSnapshot = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "join_accept_without_snapshot_total",
		Help: "Timed-out joins that observed ACCEPTED but no queue-accepted Snapshot.",
	})
	m.JoinSnapshotWithoutAccept = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "join_snapshot_without_accept_total",
		Help: "Timed-out joins that queue-accepted Snapshot but did not observe ACCEPTED.",
	})
	m.JoinSubscriptionMismatch = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "join_subscription_mismatch_total",
		Help: "Join result and Snapshot callbacks rejected for different subscription IDs.",
	})
	m.JoinStaleCallback = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "join_stale_callback_total",
		Help: "Late or wrong-fence Join callbacks discarded after replacement or terminal close.",
	})
	m.LegacyImplicitBind = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm_gate", Name: "legacy_implicit_bind_total",
		Help: "Legacy unfenced Snapshot callbacks admitted only by compatibility mode.",
	})
	m.WriterEnqueuedItems = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "enqueued_items_total",
		Help: "Downlink frames enqueued per bounded FrameKind.",
	}, []string{"kind"})
	m.WriterEnqueuedBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "enqueued_bytes_total",
		Help: "Downlink bytes enqueued per bounded FrameKind.",
	}, []string{"kind"})
	m.WriterRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "rejected_total",
		Help: "Downlink rejections per bounded kind/result/reason (e.g. hint/reject/quota).",
	}, []string{"kind", "result", "reason"})
	m.WriterQueueItems = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "queue_items",
		Help: "Currently queued downlink items across all local writers.",
	}, func() float64 { return float64(r.writerQueueSnapshot().Items) })
	m.WriterQueueBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "queue_bytes",
		Help: "Currently queued downlink bytes across all local writers.",
	}, func() float64 { return float64(r.writerQueueSnapshot().Bytes) })
	m.WriterHintItems = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "hint_items",
		Help: "Currently queued hint items under hint soft quotas.",
	}, func() float64 { return float64(r.writerQueueSnapshot().HintItems) })
	m.WriterHintBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "hint_bytes",
		Help: "Currently queued hint bytes under hint soft quotas.",
	}, func() float64 { return float64(r.writerQueueSnapshot().HintBytes) })
	m.WriterOldestAge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farmgate", Subsystem: "downlink_writer", Name: "oldest_frame_age_seconds",
		Help: "Age in seconds of the oldest queued downlink frame (max over kinds).",
	}, func() float64 {
		return r.writerQueueSnapshot().OldestAge.Seconds()
	})
	reg.MustRegister(m.BatchSize, m.Delivered, m.Bindings, m.PendingJoinCurrent, m.PendingJoinAge,
		m.JoinPendingTimeout, m.JoinAcceptWithoutSnapshot, m.JoinSnapshotWithoutAccept,
		m.JoinSubscriptionMismatch, m.JoinStaleCallback, m.LegacyImplicitBind,
		m.WriterEnqueuedItems, m.WriterEnqueuedBytes, m.WriterRejected,
		m.WriterQueueItems, m.WriterQueueBytes, m.WriterHintItems, m.WriterHintBytes, m.WriterOldestAge)
	return m
}

func (r *ConnectionRegistry) Bind(connectionID, subscriptionID string, generation uint64, writer *ConnectionWriter) {
	r.BindWithPresence(connectionID, subscriptionID, generation, 0, writer)
}

func (r *ConnectionRegistry) BindWithPresence(connectionID, subscriptionID string, generation, presenceEpoch uint64, writer *ConnectionWriter) {
	r.mu.Lock()
	if writer != nil {
		writer.SetMetrics(r.metrics)
	}
	r.connections[connectionID] = connectionEntry{subscription: subscriptionID, generation: generation, presenceEpoch: presenceEpoch, writer: writer, nextSeq: 1, acceptedItems: make(map[string]struct{})}
	r.mu.Unlock()
}

// BindSessionTarget records the complete authenticated connection fence used
// by W2A Snapshot/Catch-up/Ready callbacks. The values originate from the
// server-side SessionBinding, never from a DownlinkItem supplied by Logic.
func (r *ConnectionRegistry) BindSessionTarget(target *logicrpcpb.DownlinkTargetFence) bool {
	if target == nil || target.TargetPlayerId == 0 || target.ConnectionId == "" ||
		target.ConnectionGeneration == 0 || target.SessionEpoch == 0 ||
		target.GatewayId == "" || target.GatewayBootId == "" || target.SessionRouteRevision == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[target.ConnectionId]
	if !ok || entry.generation != target.ConnectionGeneration ||
		(entry.playerID != 0 && entry.playerID != target.TargetPlayerId) {
		return false
	}
	entry.targetFence = proto.Clone(target).(*logicrpcpb.DownlinkTargetFence)
	entry.revoked = false
	r.connections[target.ConnectionId] = entry
	return true
}

// RevokeSessionTarget closes the authenticated downlink fence without
// removing the transport entry. The writer may still carry the terminal KICK
// already queued by NotifySessionFence, but every later routed/session push is
// rejected until a complete newer target is bound.
func (r *ConnectionRegistry) RevokeSessionTarget(target *logicrpcpb.DownlinkTargetFence) bool {
	if target == nil || target.ConnectionId == "" || target.ConnectionGeneration == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[target.ConnectionId]
	if !ok || entry.generation != target.ConnectionGeneration ||
		!sameDownlinkTarget(entry.targetFence, target) {
		return false
	}
	entry.revoked = true
	entry.targetFence = nil
	r.connections[target.ConnectionId] = entry
	return true
}

// PrepareJoin installs the single bounded PendingJoin before Logic is called.
// It deliberately leaves the current ACTIVE subscription intact until the
// ACCEPTED result and matching Snapshot have both crossed Gate's boundaries.
func (r *ConnectionRegistry) PrepareJoin(connectionID string, generation uint64, operationID string, farmID uint64) bool {
	return r.PrepareJoinAttempt(connectionID, generation, operationID, farmID, operationID)
}

// PrepareJoinAttempt creates the connection-local tombstone before FirstHop.
// join_attempt_id is a runtime fence and must not be inferred from operation_id
// in the production path; the wrapper above exists only for legacy tests.
func (r *ConnectionRegistry) PrepareJoinAttempt(connectionID string, generation uint64,
	operationID string, farmID uint64, attemptID string) bool {
	if connectionID == "" || generation == 0 || operationID == "" || farmID == 0 || attemptID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation {
		return false
	}
	entry.pendingJoin = &pendingJoinFence{
		operationID: operationID, attemptID: attemptID, farmID: farmID,
		stage: pendingJoinStageCreated,
		state: pendingJoinFencePending, createdAt: time.Now(),
	}
	r.connections[connectionID] = entry
	return true
}

// QueueJoinAccepted is the first public W2A ordering boundary. It binds the
// server-issued subscription to the exact PendingJoin and places ACCEPTED in
// the same ConnectionWriter used by Snapshot/Delta/Ready before Arm is legal.
func (r *ConnectionRegistry) QueueJoinAccepted(connectionID string, generation uint64,
	operationID string, farmID uint64, attemptID, subscriptionID string,
	result *commonpb.CommandResultEnvelope) bool {
	if result == nil || subscriptionID == "" || attemptID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.writer == nil || entry.pendingJoin == nil {
		return false
	}
	pending := entry.pendingJoin
	if pending.state != pendingJoinFencePending || pending.operationID != operationID ||
		pending.farmID != farmID || pending.attemptID != attemptID ||
		(pending.subscriptionID != "" && pending.subscriptionID != subscriptionID) {
		return false
	}
	if pending.stage >= pendingJoinStageAcceptedQueued {
		return pending.subscriptionID == subscriptionID
	}
	sequence := entry.nextSeq
	if sequence == 0 {
		sequence = 1
	}
	frame, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewCommandResultFrame(sequence, result))
	if err != nil || entry.writer.EnqueueKind(entry.generation, FrameKindCommandResult, frame) != nil {
		return false
	}
	entry.nextSeq = sequence + 1
	pending.subscriptionID = subscriptionID
	pending.accepted = true
	pending.stage = pendingJoinStageAcceptedQueued
	r.connections[connectionID] = entry
	return true
}

// MarkSnapshotArmRequested advances only after ACCEPTED is already queued.
// It is written immediately before the trusted ArmSnapshot RPC so a fast
// Snapshot callback cannot race the RPC response back to Gate.
func (r *ConnectionRegistry) MarkSnapshotArmRequested(connectionID string, generation uint64,
	operationID string, farmID uint64, attemptID, subscriptionID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.pendingJoin == nil {
		return false
	}
	pending := entry.pendingJoin
	if pending.state != pendingJoinFencePending || pending.operationID != operationID ||
		pending.farmID != farmID || pending.attemptID != attemptID ||
		pending.subscriptionID != subscriptionID || pending.stage < pendingJoinStageAcceptedQueued {
		return false
	}
	if pending.stage == pendingJoinStageAcceptedQueued {
		pending.stage = pendingJoinStageSnapshotArmed
	}
	r.connections[connectionID] = entry
	return pending.stage >= pendingJoinStageSnapshotArmed
}

// CancelJoin clears only the exact pending intent; a late failure from an older
// request therefore cannot erase a newer Join.
func (r *ConnectionRegistry) CancelJoin(connectionID string, generation uint64, operationID string, farmID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.pendingJoin == nil ||
		entry.pendingJoin.state == pendingJoinFenceClosed ||
		entry.pendingJoin.stage == pendingJoinStageActive ||
		entry.pendingJoin.operationID != operationID || entry.pendingJoin.farmID != farmID {
		return false
	}
	entry.pendingJoin.state = pendingJoinFenceCanceling
	entry.pendingJoin.state = pendingJoinFenceClosed
	r.connections[connectionID] = entry
	return true
}

// ObserveJoinTimeout records the two latch states before Gate transitions the
// exact PendingJoin through Canceling to Closed.
func (r *ConnectionRegistry) ObserveJoinTimeout(connectionID string, generation uint64, operationID string, farmID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.pendingJoin == nil ||
		entry.pendingJoin.state != pendingJoinFencePending ||
		entry.pendingJoin.stage == pendingJoinStageActive ||
		entry.pendingJoin.operationID != operationID || entry.pendingJoin.farmID != farmID {
		return false
	}
	if r.metrics != nil {
		r.metrics.JoinPendingTimeout.Inc()
		if entry.pendingJoin.accepted && !entry.pendingJoin.snapshotQueued {
			r.metrics.JoinAcceptWithoutSnapshot.Inc()
		}
		if entry.pendingJoin.snapshotQueued && !entry.pendingJoin.accepted {
			r.metrics.JoinSnapshotWithoutAccept.Inc()
		}
	}
	return true
}

// AcceptJoin records the synchronous ACCEPTED half of the barrier. Promotion
// happens here when Snapshot arrived first, or in DeliverBatch otherwise.
func (r *ConnectionRegistry) AcceptJoin(connectionID string, generation uint64, operationID string, farmID uint64, subscriptionID string) bool {
	if subscriptionID == "" {
		return false
	}
	var promotion *JoinPromotion
	r.mu.Lock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.pendingJoin == nil ||
		entry.pendingJoin.state != pendingJoinFencePending ||
		entry.pendingJoin.operationID != operationID || entry.pendingJoin.farmID != farmID ||
		(entry.pendingJoin.subscriptionID != "" && entry.pendingJoin.subscriptionID != subscriptionID) {
		if r.metrics != nil {
			if ok && entry.generation == generation && entry.pendingJoin != nil &&
				entry.pendingJoin.state == pendingJoinFencePending &&
				entry.pendingJoin.operationID == operationID && entry.pendingJoin.farmID == farmID &&
				entry.pendingJoin.subscriptionID != "" && entry.pendingJoin.subscriptionID != subscriptionID {
				r.metrics.JoinSubscriptionMismatch.Inc()
			} else {
				r.metrics.JoinStaleCallback.Inc()
			}
		}
		r.mu.Unlock()
		return false
	}
	entry.pendingJoin.subscriptionID = subscriptionID
	entry.pendingJoin.accepted = true
	if entry.pendingJoin.snapshotQueued {
		promotion = promotePendingJoin(connectionID, generation, &entry)
	}
	r.connections[connectionID] = entry
	handler := r.joinPromoted
	r.mu.Unlock()
	if promotion != nil && handler != nil {
		handler(*promotion)
	}
	return true
}

func promotePendingJoin(connectionID string, generation uint64, entry *connectionEntry) *JoinPromotion {
	pending := entry.pendingJoin
	if pending == nil || pending.state != pendingJoinFencePending || !pending.accepted || !pending.snapshotQueued || pending.subscriptionID == "" {
		return nil
	}
	entry.subscription = pending.subscriptionID
	entry.presenceEpoch = pending.presenceEpoch
	pending.state = pendingJoinFenceClosed
	return &JoinPromotion{
		ConnectionID: connectionID, Generation: generation, OperationID: pending.operationID,
		FarmID: pending.farmID, SubscriptionID: pending.subscriptionID,
		JoinAttemptID: pending.attemptID, PresenceEpoch: pending.presenceEpoch,
	}
}

func (r *ConnectionRegistry) SetSubscriptionTerminationHandler(handler func(SubscriptionTermination)) {
	r.mu.Lock()
	r.subscriptionTerminated = handler
	r.mu.Unlock()
}

// BindPlayer attaches the authenticated player identity to an already bound
// transport generation. It enables account-scoped notifications without
// bypassing the connection's bounded writer or monotonic server sequence.
func (r *ConnectionRegistry) BindPlayer(connectionID string, generation, playerID uint64) bool {
	if playerID == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation {
		return false
	}
	entry.playerID = playerID
	r.connections[connectionID] = entry
	return true
}

func (r *ConnectionRegistry) Unbind(connectionID string, generation uint64) {
	r.mu.Lock()
	if c, ok := r.connections[connectionID]; ok && c.generation == generation {
		delete(r.connections, connectionID)
	}
	r.mu.Unlock()
}

// SetNextSequence synchronizes the downlink sequence with frames emitted by
// Gate itself. The ServerHello consumes sequence 1 before Logic can push.
func (r *ConnectionRegistry) SetNextSequence(connectionID string, generation uint64, next uint64) {
	if next == 0 {
		next = 1
	}
	r.mu.Lock()
	if entry, ok := r.connections[connectionID]; ok && entry.generation == generation {
		entry.nextSeq = next
		r.connections[connectionID] = entry
	}
	r.mu.Unlock()
}

// NotifySessionReplaced tells every other local connection for playerID that a
// newer authenticated connection has taken ownership of the active session.
// The push is advisory UX: Redis active_session_epoch remains the security
// boundary, so an old connection cannot regain write authority by ignoring it.
//
// Multi-Gate deployments must fan the replacement event to the old owning Gate
// before invoking this gateway-local registry.
func (r *ConnectionRegistry) NotifySessionReplaced(playerID uint64, exceptConnectionID string) int {
	if playerID == 0 {
		return 0
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&commonpb.ConnectionError{
		ErrorCode:    commonpb.ErrorCode_ERR_SESSION_EXPIRED,
		ErrorMessage: "账号已在其他页面登录",
	})
	if err != nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	enqueued := 0
	for connectionID, entry := range r.connections {
		if connectionID == exceptConnectionID || entry.playerID != playerID || entry.writer == nil || entry.generation == 0 || entry.revoked {
			continue
		}
		push := &commonpb.ServerPushEnvelope{
			// 28 §2.4:KICK 属 session 域;本层不掌握 session_epoch,H5 对 KICK 只执行断开。
			PushType: commonpb.PushType_KICK,
			PushContext: &commonpb.ServerPushEnvelope_Session{
				Session: &commonpb.SessionPushContext{TargetPlayerId: entry.playerID},
			},
			Payload: payload,
		}
		sequence := entry.nextSeq
		if sequence == 0 {
			sequence = 1
		}
		frame, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(
			gateprotocol.NewServerPushFrame(sequence, push))
		if marshalErr != nil || entry.writer.EnqueueKind(entry.generation, FrameKindControl, frame) != nil {
			continue
		}
		entry.nextSeq = sequence + 1
		r.connections[connectionID] = entry
		enqueued++
	}
	return enqueued
}

// NotifySessionFence enqueues KICK only for the exact old transport target.
// The caller has already compared SessionRoute fields; this local index adds
// player_id and connection_generation checks before touching its writer.
func (r *ConnectionRegistry) NotifySessionFence(playerID uint64, connectionID string, generation uint64) bool {
	if playerID == 0 || connectionID == "" || generation == 0 {
		return false
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&commonpb.ConnectionError{
		ErrorCode: commonpb.ErrorCode_ERR_SESSION_EXPIRED, ErrorMessage: "账号已在其他页面登录",
	})
	if err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.playerID != playerID || entry.generation != generation || entry.revoked {
		return false
	}
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_KICK,
		PushContext: &commonpb.ServerPushEnvelope_Session{
			Session: &commonpb.SessionPushContext{TargetPlayerId: entry.playerID},
		},
		Payload: payload,
	}
	sequence := entry.nextSeq
	if sequence == 0 {
		sequence = 1
	}
	frame, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(sequence, push))
	enqueued := err == nil && entry.writer != nil && entry.writer.EnqueueKind(entry.generation, FrameKindControl, frame) == nil
	if enqueued {
		entry.nextSeq = sequence + 1
	}
	// The KICK is best-effort UX. Revocation is the security boundary and must
	// happen even when the connection writer is already unavailable or full.
	entry.revoked = true
	entry.targetFence = nil
	r.connections[connectionID] = entry
	return enqueued
}

// BeginSubscription fences delayed pushes from the previous farm before a new
// JOIN_FARM is forwarded. The first accepted snapshot seals presence_epoch.
func (r *ConnectionRegistry) BeginSubscription(connectionID string, generation uint64, subscriptionID string) bool {
	if subscriptionID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation {
		return false
	}
	entry.subscription = subscriptionID
	entry.presenceEpoch = 0
	r.connections[connectionID] = entry
	return true
}

// EndSubscription clears only the expected subscription on the exact
// connection generation. It is used after a fenced LEAVE_FARM or a failed
// JOIN_FARM so delayed callbacks cannot reopen or clear a newer subscription.
func (r *ConnectionRegistry) EndSubscription(connectionID string, generation uint64, subscriptionID string) bool {
	if subscriptionID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.subscription != subscriptionID {
		return false
	}
	entry.subscription = ""
	entry.activeAttempt = ""
	entry.presenceEpoch = 0
	if entry.pendingJoin != nil && entry.pendingJoin.stage == pendingJoinStageActive &&
		entry.pendingJoin.subscriptionID == subscriptionID {
		entry.pendingJoin = nil
	}
	r.connections[connectionID] = entry
	return true
}

// NextSequence reserves one outgoing sequence for a Gate command response.
// Both synchronous responses and asynchronous pushes therefore share one
// monotonic stream per connection generation.
func (r *ConnectionRegistry) NextSequence(connectionID string, generation uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation {
		return 0
	}
	seq := entry.nextSeq
	if seq == 0 {
		seq = 1
	}
	entry.nextSeq = seq + 1
	r.connections[connectionID] = entry
	return seq
}

// EnqueueDrainNotice places a typed rollout hint behind any already accepted
// response/push on every connection's existing bounded writer. The retry delay
// is stable per (drain, connection) and spread across maxReconnectDelay.
func (r *ConnectionRegistry) EnqueueDrainNotice(drainID string, maxReconnectDelay time.Duration, deadline time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	enqueued := 0
	maxDelayMs := maxReconnectDelay.Milliseconds()
	for connectionID, entry := range r.connections {
		if entry.writer == nil || entry.generation == 0 || entry.revoked {
			continue
		}
		retryAfterMs := uint32(0)
		if maxDelayMs > 0 {
			hash := fnv.New32a()
			_, _ = hash.Write([]byte(drainID))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(connectionID))
			retryAfterMs = hash.Sum32() % uint32(maxDelayMs+1)
		}
		noticeBody, err := proto.MarshalOptions{Deterministic: true}.Marshal(&commonpb.DrainNotice{
			DrainId:          drainID,
			ReconnectAfterMs: retryAfterMs,
			DeadlineMs:       deadline.UnixMilli(),
		})
		if err != nil {
			continue
		}
		push := &commonpb.ServerPushEnvelope{
			PushType: commonpb.PushType_DRAIN_NOTICE,
			PushContext: &commonpb.ServerPushEnvelope_Session{
				Session: &commonpb.SessionPushContext{TargetPlayerId: entry.playerID},
			},
			Payload: noticeBody,
		}
		sequence := entry.nextSeq
		if sequence == 0 {
			sequence = 1
		}
		frame, err := proto.MarshalOptions{Deterministic: true}.Marshal(
			gateprotocol.NewServerPushFrame(sequence, push))
		if err != nil || entry.writer.EnqueueKind(entry.generation, FrameKindControl, frame) != nil {
			continue
		}
		entry.nextSeq = sequence + 1
		r.connections[connectionID] = entry
		enqueued++
	}
	return enqueued
}

func (r *ConnectionRegistry) NotifyFriendOpportunityChanged(playerID, targetOwnerID, sourceFarmVersion, opportunityRevision uint64) int {
	if playerID == 0 || targetOwnerID == 0 || sourceFarmVersion == 0 || opportunityRevision == 0 {
		return 0
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&friendpb.FriendOpportunityChanged{
		TargetOwnerId: targetOwnerID, SourceFarmVersion: sourceFarmVersion, OpportunityRevision: opportunityRevision,
	})
	if err != nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	enqueued := 0
	for connectionID, entry := range r.connections {
		if entry.playerID != playerID || entry.writer == nil || entry.generation == 0 || entry.revoked {
			continue
		}
		push := &commonpb.ServerPushEnvelope{PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
			PushContext: &commonpb.ServerPushEnvelope_Player{Player: &commonpb.PlayerPushContext{TargetPlayerId: entry.playerID}},
			Payload:     payload}
		// Hint soft quota is enforced before sequence allocation: a rejected
		// hint never consumes a sequence and the already-sequenced FIFO is
		// never selectively dropped or reordered (28 L2-PROTO-26). FarmChange/
		// CommandResult/control do not share this soft quota and keep their own
		// exact delivery semantics.
		sequence := entry.nextSeq
		if sequence == 0 {
			sequence = 1
		}
		frame, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(gateprotocol.NewServerPushFrame(sequence, push))
		if marshalErr != nil || entry.writer.EnqueueKind(entry.generation, FrameKindHint, frame) != nil {
			continue
		}
		entry.nextSeq = sequence + 1
		r.connections[connectionID] = entry
		enqueued++
	}
	return enqueued
}

// NotifyFriendRosterChanged invalidates the roster for every local connection
// belonging to playerID. Multi-Gate deployments must fan this event to the
// owning Gate before invoking this local registry.
func (r *ConnectionRegistry) NotifyFriendRosterChanged(playerID, counterpartPlayerID, relationEpoch uint64, mutation friendpb.FriendMutationKind) int {
	if playerID == 0 || counterpartPlayerID == 0 || mutation == friendpb.FriendMutationKind_FRIEND_MUTATION_KIND_UNSPECIFIED {
		return 0
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&friendpb.FriendRosterChanged{
		CounterpartPlayerId: counterpartPlayerID,
		RelationEpoch:       relationEpoch,
		Mutation:            mutation,
	})
	if err != nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	enqueued := 0
	for connectionID, entry := range r.connections {
		if entry.playerID != playerID || entry.writer == nil || entry.generation == 0 || entry.revoked {
			continue
		}
		push := &commonpb.ServerPushEnvelope{
			// 28 §2.4:好友变化属 player 域。
			PushType: commonpb.PushType_FRIEND_ROSTER_CHANGED,
			PushContext: &commonpb.ServerPushEnvelope_Player{
				Player: &commonpb.PlayerPushContext{TargetPlayerId: entry.playerID},
			},
			Payload: payload,
		}
		sequence := entry.nextSeq
		if sequence == 0 {
			sequence = 1
		}
		frame, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(
			gateprotocol.NewServerPushFrame(sequence, push))
		if marshalErr != nil || entry.writer.EnqueueKind(entry.generation, FrameKindSession, frame) != nil {
			continue
		}
		entry.nextSeq = sequence + 1
		r.connections[connectionID] = entry
		enqueued++
	}
	return enqueued
}

func sameDownlinkTarget(left, right *logicrpcpb.DownlinkTargetFence) bool {
	return left != nil && right != nil &&
		left.TargetPlayerId == right.TargetPlayerId && left.GatewayId == right.GatewayId &&
		left.GatewayBootId == right.GatewayBootId && left.ConnectionId == right.ConnectionId &&
		left.ConnectionGeneration == right.ConnectionGeneration && left.SessionEpoch == right.SessionEpoch &&
		left.SessionRouteRevision == right.SessionRouteRevision
}

func decodeServerPush(raw []byte) (*commonpb.ServerPushEnvelope, error) {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil || frame.GetServerPush() == nil {
		return nil, ErrInvalidFrame
	}
	return frame.GetServerPush(), nil
}

// deliverW2JoinItemLocked validates the explicit Join/connection fences and
// advances only the Gate-local PendingJoin stage represented by the frame. r.mu
// must be held and entry must belong to item.ConnectionId/generation.
func (r *ConnectionRegistry) deliverW2JoinItemLocked(entry *connectionEntry,
	item *logicrpcpb.DownlinkItem) (logicrpcpb.DownlinkItemStatus, *SubscriptionTermination) {
	if entry == nil || item == nil || item.JoinFence == nil || item.TargetFence == nil ||
		!sameDownlinkTarget(entry.targetFence, item.TargetFence) ||
		item.ConnectionId != item.TargetFence.ConnectionId ||
		item.TransportGeneration != item.TargetFence.ConnectionGeneration ||
		item.GatewayId != item.TargetFence.GatewayId || item.SubscriptionId != item.JoinFence.SubscriptionId {
		return logicrpcpb.DownlinkItemStatus_STALE_CONNECTION, nil
	}
	if _, duplicate := entry.acceptedItems[item.ItemId]; duplicate {
		return logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED, nil
	}
	pending := entry.pendingJoin
	if pending == nil || pending.state != pendingJoinFencePending ||
		pending.farmID != item.JoinFence.FarmId || pending.attemptID != item.JoinFence.JoinAttemptId ||
		pending.subscriptionID != item.JoinFence.SubscriptionId {
		return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
	}
	push, err := decodeServerPush(item.Frame)
	if err != nil || push.GetFarmSubscription() == nil ||
		push.GetFarmSubscription().TargetPlayerId != item.TargetFence.TargetPlayerId ||
		push.GetFarmSubscription().TargetFarmId != item.JoinFence.FarmId ||
		push.GetFarmSubscription().SubscriptionId != item.JoinFence.SubscriptionId {
		return logicrpcpb.DownlinkItemStatus_UNAVAILABLE, nil
	}
	switch push.PushType {
	case commonpb.PushType_FARM_SNAPSHOT:
		var snapshot farmpb.FarmSnapshotPush
		if pending.stage != pendingJoinStageSnapshotArmed ||
			item.PresenceEpoch == 0 || proto.Unmarshal(push.Payload, &snapshot) != nil ||
			snapshot.JoinAttemptId != pending.attemptID || snapshot.SnapshotVersion != item.SnapshotVersion ||
			snapshot.GetSnapshot().GetFarmVersion() != item.SnapshotVersion {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
	case commonpb.PushType_FARM_DELTA_BATCH:
		var batch farmpb.FarmDeltaBatchPush
		expectedFrom := pending.readyThroughVersion + 1
		if pending.stage != pendingJoinStageSnapshotQueued && pending.stage != pendingJoinStageFlushing {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
		if item.FromVersion == 0 || item.ThroughVersion < item.FromVersion ||
			item.FromVersion != expectedFrom || proto.Unmarshal(push.Payload, &batch) != nil ||
			batch.JoinAttemptId != pending.attemptID || batch.FromVersion != item.FromVersion ||
			batch.ThroughVersion != item.ThroughVersion || len(batch.Deltas) == 0 {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
		for index, change := range batch.Deltas {
			if change == nil || change.FarmVersion != item.FromVersion+uint64(index) {
				return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
			}
		}
		if batch.Deltas[len(batch.Deltas)-1].FarmVersion != item.ThroughVersion {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
	case commonpb.PushType_FARM_CHANGE:
		if pending.stage != pendingJoinStageActive || entry.subscription != item.SubscriptionId ||
			entry.activeAttempt != item.JoinFence.JoinAttemptId {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
	case commonpb.PushType_REJOIN_REQUIRED:
		var rejoin farmpb.RejoinRequiredPush
		if proto.Unmarshal(push.Payload, &rejoin) != nil ||
			rejoin.Reason == farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED {
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
		if pending.stage == pendingJoinStageActive {
			if entry.subscription != item.SubscriptionId || entry.activeAttempt != item.JoinFence.JoinAttemptId ||
				item.PresenceEpoch == 0 || entry.presenceEpoch != item.PresenceEpoch {
				return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
			}
		} else if pending.stage < pendingJoinStageAcceptedQueued || pending.stage >= pendingJoinStageActive ||
			(item.PresenceEpoch != 0 && pending.presenceEpoch != 0 && pending.presenceEpoch != item.PresenceEpoch) {
			// A PendingJoin already has a real Holder Presence and therefore a
			// renewable lease before Snapshot/Ready completes. PresenceEpoch is
			// not part of the renewal RPC, so a pre-active terminal may carry 0;
			// the exact attempt/subscription/target fences remain mandatory.
			return logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION, nil
		}
	default:
		return logicrpcpb.DownlinkItemStatus_UNAVAILABLE, nil
	}
	if entry.writer == nil {
		return logicrpcpb.DownlinkItemStatus_UNAVAILABLE, nil
	}
	frame, err := normalizeFrameSequence(item.Frame, entry.nextSeq)
	if err != nil {
		return logicrpcpb.DownlinkItemStatus_UNAVAILABLE, nil
	}
	if err := entry.writer.EnqueueKind(entry.generation, ClassifyPushKind(push.PushType), frame); err != nil {
		switch err {
		case ErrQueueFull:
			if push.PushType == commonpb.PushType_REJOIN_REQUIRED {
				entry.writer.Close()
				return logicrpcpb.DownlinkItemStatus_QUEUE_FULL,
					terminateActiveSubscriptionLocked(entry, item, true)
			}
			return logicrpcpb.DownlinkItemStatus_QUEUE_FULL, nil
		case ErrStaleGeneration:
			return logicrpcpb.DownlinkItemStatus_STALE_CONNECTION, nil
		default:
			return logicrpcpb.DownlinkItemStatus_UNAVAILABLE, nil
		}
	}
	entry.nextSeq++
	if push.PushType == commonpb.PushType_FARM_SNAPSHOT {
		pending.snapshotVersion = item.SnapshotVersion
		pending.readyThroughVersion = item.SnapshotVersion
		pending.presenceEpoch = item.PresenceEpoch
		pending.snapshotQueued = true
		pending.stage = pendingJoinStageSnapshotQueued
	} else if push.PushType == commonpb.PushType_FARM_DELTA_BATCH {
		pending.readyThroughVersion = item.ThroughVersion
		pending.stage = pendingJoinStageFlushing
	}
	if entry.acceptedItems == nil {
		entry.acceptedItems = make(map[string]struct{})
	}
	entry.acceptedItems[item.ItemId] = struct{}{}
	entry.itemOrder = append(entry.itemOrder, item.ItemId)
	if len(entry.itemOrder) > maxAcceptedItems {
		oldest := entry.itemOrder[0]
		entry.itemOrder = entry.itemOrder[1:]
		delete(entry.acceptedItems, oldest)
	}
	if push.PushType == commonpb.PushType_REJOIN_REQUIRED {
		return logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
			terminateActiveSubscriptionLocked(entry, item, false)
	}
	return logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED, nil
}

func terminateActiveSubscriptionLocked(entry *connectionEntry, item *logicrpcpb.DownlinkItem,
	connectionClosed bool) *SubscriptionTermination {
	if entry == nil || item == nil || item.JoinFence == nil {
		return nil
	}
	termination := &SubscriptionTermination{
		ConnectionID: item.ConnectionId, Generation: item.TransportGeneration,
		FarmID: item.JoinFence.FarmId, SubscriptionID: item.JoinFence.SubscriptionId,
		JoinAttemptID: item.JoinFence.JoinAttemptId, PresenceEpoch: item.PresenceEpoch,
		ConnectionClosed: connectionClosed,
	}
	entry.subscription = ""
	entry.activeAttempt = ""
	entry.presenceEpoch = 0
	if entry.pendingJoin != nil {
		entry.pendingJoin.state = pendingJoinFenceClosed
	}
	return termination
}

func (r *ConnectionRegistry) DeliverBatch(ctx context.Context, req *logicrpcpb.DeliverBatchRequest) (*logicrpcpb.DeliverBatchResponse, error) {
	response := &logicrpcpb.DeliverBatchResponse{}
	if req == nil {
		return response, nil
	}
	if r.metrics != nil {
		r.metrics.BatchSize.Observe(float64(len(req.Items)))
	}
	for _, item := range req.Items {
		result := &logicrpcpb.DeliverBatchItemResult{Status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE}
		if item == nil {
			response.Results = append(response.Results, result)
			continue
		}
		result.ItemId = item.ItemId
		if err := ctx.Err(); err != nil {
			response.Results = append(response.Results, result)
			continue
		}
		if item.ItemId == "" {
			// Retries are correlated by item_id; accepting an uncorrelatable
			// item would make QueueAccepted impossible to safely interpret.
			response.Results = append(response.Results, result)
			continue
		}
		r.mu.Lock()
		entry, ok := r.connections[item.ConnectionId]
		if item.JoinFence != nil {
			if !ok {
				result.Status = logicrpcpb.DownlinkItemStatus_CONNECTION_NOT_FOUND
			} else if entry.generation == 0 || item.TransportGeneration == 0 ||
				item.TransportGeneration != entry.generation {
				result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
			} else {
				var termination *SubscriptionTermination
				result.Status, termination = r.deliverW2JoinItemLocked(&entry, item)
				if result.Status == logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED || termination != nil {
					r.connections[item.ConnectionId] = entry
				}
				if result.Status == logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED && r.metrics != nil {
					r.metrics.Delivered.Inc()
				}
				handler := r.subscriptionTerminated
				r.mu.Unlock()
				if termination != nil && handler != nil {
					handler(*termination)
				}
				response.Results = append(response.Results, result)
				continue
			}
			r.mu.Unlock()
			response.Results = append(response.Results, result)
			continue
		}
		legacyJoinFieldsPresent := item.JoinOperationId != "" || item.TargetFarmId != 0
		sessionTargetFenced := item.TargetFence != nil
		if !ok {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_CONNECTION_NOT_FOUND
		} else if entry.generation == 0 || item.TransportGeneration == 0 || item.TransportGeneration != entry.generation || entry.revoked {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
		} else if requiresCompleteSessionTargetFence(item.Frame) &&
			(!sessionTargetFenced || !validPlayerPushTarget(item.Frame, item.TargetFence)) {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
		} else if sessionTargetFenced &&
			(item.ConnectionId != item.TargetFence.ConnectionId ||
				item.TransportGeneration != item.TargetFence.ConnectionGeneration ||
				item.GatewayId != item.TargetFence.GatewayId ||
				!sameDownlinkTarget(entry.targetFence, item.TargetFence)) {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
		} else if !sessionTargetFenced && item.PresenceEpoch == 0 {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
		} else if requiresFarmSubscriptionFence(item.Frame) {
			// Every FarmSubscription push uses the explicit W2 fence path above.
			// This rejects both implicit Snapshot binding and old unfenced Change.
			if isFarmSnapshotPush(item.Frame) && r.metrics != nil {
				r.metrics.JoinStaleCallback.Inc()
			}
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION
		} else if _, duplicate := entry.acceptedItems[item.ItemId]; duplicate {
			// A retry of an already queued item stays successful even after its
			// PendingJoin has promoted to a CLOSED tombstone.
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
		} else if legacyJoinFieldsPresent {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_UNAVAILABLE
		} else if !sessionTargetFenced && entry.presenceEpoch != 0 && item.PresenceEpoch != entry.presenceEpoch {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
		} else if entry.writer == nil {
			r.mu.Unlock()
			result.Status = logicrpcpb.DownlinkItemStatus_UNAVAILABLE
		} else {
			if !sessionTargetFenced && entry.presenceEpoch == 0 {
				entry.presenceEpoch = item.PresenceEpoch
			}
			frame, frameErr := normalizeFrameSequence(item.Frame, entry.nextSeq)
			if frameErr != nil {
				r.mu.Unlock()
				result.Status = logicrpcpb.DownlinkItemStatus_UNAVAILABLE
			} else if err := entry.writer.EnqueueKind(entry.generation, ClassifyFrameKind(frame), frame); err != nil {
				r.mu.Unlock()
				switch err {
				case ErrQueueFull:
					result.Status = logicrpcpb.DownlinkItemStatus_QUEUE_FULL
				case ErrStaleGeneration:
					result.Status = logicrpcpb.DownlinkItemStatus_STALE_CONNECTION
				default:
					result.Status = logicrpcpb.DownlinkItemStatus_UNAVAILABLE
				}
			} else {
				entry.nextSeq++
				if item.ItemId != "" {
					if entry.acceptedItems == nil {
						entry.acceptedItems = make(map[string]struct{})
					}
					entry.acceptedItems[item.ItemId] = struct{}{}
					entry.itemOrder = append(entry.itemOrder, item.ItemId)
					if len(entry.itemOrder) > maxAcceptedItems {
						oldest := entry.itemOrder[0]
						entry.itemOrder = entry.itemOrder[1:]
						delete(entry.acceptedItems, oldest)
					}
				}
				r.connections[item.ConnectionId] = entry
				r.mu.Unlock()
				if r.metrics != nil {
					r.metrics.Delivered.Inc()
				}
				result.Status = logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
			}
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

// ActivateSubscription queues SUBSCRIPTION_READY(N) and performs the only
// Gate-local PendingJoin -> ActiveSubscription transition. Queue acceptance is
// the boundary; browser application is intentionally not part of this RPC.
func (r *ConnectionRegistry) ActivateSubscription(ctx context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	response := &logicrpcpb.ActivateSubscriptionResponse{Status: logicrpcpb.DeliveryStatus_DELIVERY_STALE_FENCE}
	if req == nil || req.Subscription == nil || req.Target == nil ||
		ctx.Err() != nil {
		return response, nil
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&farmpb.SubscriptionReadyPush{
		JoinAttemptId: req.Subscription.JoinAttemptId, ReadyThroughVersion: req.ReadyThroughVersion,
	})
	if err != nil {
		response.Status = logicrpcpb.DeliveryStatus_DELIVERY_UNAVAILABLE
		return response, nil
	}
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: req.Target.TargetPlayerId, TargetFarmId: req.Subscription.FarmId,
				SubscriptionId: req.Subscription.SubscriptionId,
			},
		},
		Payload: payload,
	}
	var promotion *JoinPromotion
	r.mu.Lock()
	entry, ok := r.connections[req.Target.ConnectionId]
	if !ok || entry.generation != req.Target.ConnectionGeneration ||
		!sameDownlinkTarget(entry.targetFence, req.Target) || entry.pendingJoin == nil {
		r.mu.Unlock()
		return response, nil
	}
	pending := entry.pendingJoin
	if pending.stage == pendingJoinStageActive && pending.subscriptionID == req.Subscription.SubscriptionId &&
		pending.attemptID == req.Subscription.JoinAttemptId &&
		pending.readyThroughVersion == req.ReadyThroughVersion {
		r.mu.Unlock()
		response.Status = logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED
		response.ReadyThroughVersion = req.ReadyThroughVersion
		return response, nil
	}
	if pending.state != pendingJoinFencePending || pending.farmID != req.Subscription.FarmId ||
		pending.subscriptionID != req.Subscription.SubscriptionId ||
		pending.attemptID != req.Subscription.JoinAttemptId ||
		(pending.stage != pendingJoinStageSnapshotQueued && pending.stage != pendingJoinStageFlushing) ||
		pending.readyThroughVersion != req.ReadyThroughVersion || entry.writer == nil {
		r.mu.Unlock()
		return response, nil
	}
	sequence := entry.nextSeq
	if sequence == 0 {
		sequence = 1
	}
	frame, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(sequence, push))
	if err != nil {
		r.mu.Unlock()
		response.Status = logicrpcpb.DeliveryStatus_DELIVERY_UNAVAILABLE
		return response, nil
	}
	if err := entry.writer.EnqueueKind(entry.generation, ClassifyPushKind(push.PushType), frame); err != nil {
		r.mu.Unlock()
		switch err {
		case ErrQueueFull:
			response.Status = logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_FULL
		case ErrStaleGeneration:
			response.Status = logicrpcpb.DeliveryStatus_DELIVERY_STALE_FENCE
		default:
			response.Status = logicrpcpb.DeliveryStatus_DELIVERY_UNAVAILABLE
		}
		return response, nil
	}
	entry.nextSeq = sequence + 1
	pending.stage = pendingJoinStageReadyQueued
	entry.subscription = pending.subscriptionID
	entry.activeAttempt = pending.attemptID
	entry.presenceEpoch = pending.presenceEpoch
	pending.stage = pendingJoinStageActive
	promotion = &JoinPromotion{
		ConnectionID: req.Target.ConnectionId, Generation: req.Target.ConnectionGeneration,
		OperationID: pending.operationID, FarmID: pending.farmID,
		SubscriptionID: pending.subscriptionID, JoinAttemptID: pending.attemptID,
		PresenceEpoch: pending.presenceEpoch,
	}
	r.connections[req.Target.ConnectionId] = entry
	handler := r.joinPromoted
	r.mu.Unlock()
	if handler != nil {
		handler(*promotion)
	}
	response.Status = logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED
	response.ReadyThroughVersion = req.ReadyThroughVersion
	return response, nil
}

// Notify returns the per-connection "queue non-empty" signal channel for a
// transport write loop. It must be paired with Dequeue using drain semantics:
// one signal may cover multiple queued frames.
func (r *ConnectionRegistry) Notify(connectionID string, generation uint64) (<-chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.connections[connectionID]
	if !ok || entry.generation != generation || entry.writer == nil {
		return nil, false
	}
	return entry.writer.NotifyChan(), true
}

// Pending reports whether the connection's writer still holds undrained
// frames. It implements the optional PendingDownlinkSource seam: when the
// WritePump exhausts its drain quantum, Pending re-arms the writer's notifier
// so the next Notify channel is immediately readable and the remaining frames
// drain without a second enqueue.
func (r *ConnectionRegistry) Pending(connectionID string, generation uint64) bool {
	r.mu.Lock()
	entry, ok := r.connections[connectionID]
	r.mu.Unlock()
	if !ok || entry.generation != generation || entry.writer == nil {
		return false
	}
	return entry.writer.Pending()
}

// Closed reports whether the connection writer has entered terminal teardown.
// Notify uses a closed channel for both ordinary level-triggered wakeups and
// terminal close, so transports must consult this explicit state.
func (r *ConnectionRegistry) Closed(connectionID string, generation uint64) bool {
	r.mu.Lock()
	entry, ok := r.connections[connectionID]
	r.mu.Unlock()
	if !ok || entry.generation != generation || entry.writer == nil {
		return true
	}
	return entry.writer.Closed()
}

// Dequeue returns the next queued frame for a connection. A real TCONND
// adapter can use this narrow method to drain the gateway-local writer without
// gaining access to session or actor state.
func (r *ConnectionRegistry) Dequeue(connectionID string, generation uint64) ([]byte, bool) {
	r.mu.Lock()
	entry, ok := r.connections[connectionID]
	r.mu.Unlock()
	if !ok || entry.generation != generation || entry.writer == nil {
		return nil, false
	}
	item, ok := entry.writer.Dequeue()
	if !ok {
		return nil, false
	}
	return item.Data, true
}

func normalizeFrameSequence(raw []byte, sequence uint64) ([]byte, error) {
	if sequence == 0 {
		return nil, ErrInvalidFrame
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		return nil, err
	}
	if frame.ProtocolVersion != gateprotocol.ProtocolVersion || frame.GetServerPush() == nil {
		return nil, ErrInvalidFrame
	}
	frame.Sequence = sequence
	return proto.MarshalOptions{Deterministic: true}.Marshal(&frame)
}
