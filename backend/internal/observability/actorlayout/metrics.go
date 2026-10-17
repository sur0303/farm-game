// Package actorlayout exposes non-adjudicating observations for the current
// Actor Authority layout. Metrics are distributions of bytes already crossing
// the persistence seam; they are never read by business or storage code.
package actorlayout

import (
	"context"
	"net"
	"strings"
	"sync/atomic"

	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type commitTraceKey struct{}

type commitTrace struct {
	roundTrips atomic.Uint64
}

// Metrics implements authoritystore.CommitObserver and redis.Hook. The Redis
// hook counts only calls whose context was created by BeginAuthorityCommit;
// unrelated commands on the shared client are ignored.
type Metrics struct {
	actorRecordBytes    *prometheus.HistogramVec
	commitRequestBytes  *prometheus.HistogramVec
	commitResponseBytes *prometheus.HistogramVec
	receiptBytes        *prometheus.HistogramVec
	orderBytes          *prometheus.HistogramVec
	modelStateBytes     *prometheus.HistogramVec
	serializationMs     *prometheus.HistogramVec
	redisRoundTrips     *prometheus.HistogramVec
}

var _ authoritystore.CommitObserver = (*Metrics)(nil)
var _ redis.Hook = (*Metrics)(nil)

var byteBuckets = prometheus.ExponentialBuckets(64, 2, 16)

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	metrics := &Metrics{
		actorRecordBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "actor_record_serialized_bytes", Help: "Serialized model:record payload bytes submitted by Actor domain.", Buckets: byteBuckets,
		}, []string{"domain"}),
		commitRequestBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "authority_commit_request_bytes", Help: "Logical Authority Commit request bytes; command is a bounded storage-stable label, not an operation id.", Buckets: byteBuckets,
		}, []string{"domain", "command"}),
		commitResponseBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "authority_commit_response_bytes", Help: "Logical Authority Commit response bytes.", Buckets: byteBuckets,
		}, []string{"domain"}),
		receiptBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "receipt_bytes", Help: "Per-commit outer Authority receipt bytes; Redis uses its exact physical LP projection.", Buckets: byteBuckets,
		}, []string{"domain"}),
		orderBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "order_bytes", Help: "Logical bytes per attached source order.", Buckets: byteBuckets,
		}, []string{"domain"}),
		modelStateBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "model_state_bytes", Help: "Complete candidate ModelState bytes including model field names.", Buckets: byteBuckets,
		}, []string{"domain"}),
		serializationMs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "serialization_duration_ms", Help: "Duration in milliseconds of deterministic commit envelope sizing and physical receipt projection.", Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"domain"}),
		redisRoundTrips: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "redis_round_trips", Help: "Actual go-redis process/pipeline dispatches within one Authority Commit context.", Buckets: []float64{0, 1, 2, 3, 4, 5, 8, 13},
		}, []string{"domain"}),
	}
	reg.MustRegister(
		metrics.actorRecordBytes, metrics.commitRequestBytes, metrics.commitResponseBytes,
		metrics.receiptBytes, metrics.orderBytes, metrics.modelStateBytes,
		metrics.serializationMs, metrics.redisRoundTrips,
	)
	return metrics
}

func normalizeDomain(domain string) string {
	switch strings.ToUpper(strings.TrimSpace(domain)) {
	case "PLAYER":
		return "player"
	case "FARM":
		return "farm"
	default:
		return "unknown"
	}
}

// normalizeCommand prevents an unexpected store caller from creating an
// unbounded command label. Current Actor Binding commits expose only these two
// storage-stable command names; business BUY/SELL/etc. are intentionally not
// guessed from payloads or operation IDs.
func normalizeCommand(command string) string {
	switch command {
	case "AR1_RECORD_COMMIT", "AR1_INITIAL_INSTALL":
		return command
	default:
		return "OTHER"
	}
}

func (m *Metrics) BeginAuthorityCommit(ctx context.Context, domain, command string) (context.Context, func(authoritystore.CommitObservation)) {
	if ctx == nil {
		ctx = context.Background()
	}
	trace := &commitTrace{}
	observedCtx := context.WithValue(ctx, commitTraceKey{}, trace)
	return observedCtx, func(observation authoritystore.CommitObservation) {
		if m == nil {
			return
		}
		domainLabel := normalizeDomain(domain)
		commandLabel := normalizeCommand(command)
		m.actorRecordBytes.WithLabelValues(domainLabel).Observe(float64(observation.ActorRecordBytes))
		m.commitRequestBytes.WithLabelValues(domainLabel, commandLabel).Observe(float64(observation.CommitRequestBytes))
		m.commitResponseBytes.WithLabelValues(domainLabel).Observe(float64(observation.CommitResponseBytes))
		m.receiptBytes.WithLabelValues(domainLabel).Observe(float64(observation.ReceiptBytes))
		for _, size := range observation.OrderBytes {
			m.orderBytes.WithLabelValues(domainLabel).Observe(float64(size))
		}
		m.modelStateBytes.WithLabelValues(domainLabel).Observe(float64(observation.ModelStateBytes))
		m.serializationMs.WithLabelValues(domainLabel).Observe(float64(observation.SerializationDuration.Microseconds()) / 1000)
		m.redisRoundTrips.WithLabelValues(domainLabel).Observe(float64(trace.roundTrips.Load()))
		if observation.Status == authoritystore.CommitCommitted.String() {
			sourceordermetrics.ObserveActorCommit(observation.PhysicalCommitBytes, observation.OrderPayloadBytes)
		}
	}
}

func traceFromContext(ctx context.Context) *commitTrace {
	if ctx == nil {
		return nil
	}
	trace, _ := ctx.Value(commitTraceKey{}).(*commitTrace)
	return trace
}

func (m *Metrics) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (m *Metrics) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if trace := traceFromContext(ctx); trace != nil {
			trace.roundTrips.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (m *Metrics) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if trace := traceFromContext(ctx); trace != nil {
			trace.roundTrips.Add(1)
		}
		return next(ctx, cmds)
	}
}
