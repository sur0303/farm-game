package blobstore

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type measuredStore struct {
	inner    Store
	latency  *prometheus.HistogramVec
	requests *prometheus.CounterVec
}

func NewMeasuredStore(inner Store, reg prometheus.Registerer, actorKind string) Store {
	labels := prometheus.Labels{"actor_kind": actorKind}
	store := &measuredStore{
		inner: inner,
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "store", Name: "operation_seconds",
			Help: "Actor record store latency.", ConstLabels: labels,
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"operation"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "store", Name: "operation_total",
			Help: "Actor record store outcomes.", ConstLabels: labels,
		}, []string{"operation", "result"}),
	}
	reg.MustRegister(store.latency, store.requests)
	return store
}

func (s *measuredStore) observe(operation string, start time.Time, err error) {
	s.latency.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	result := "ok"
	switch {
	case errors.Is(err, ErrCASConflict):
		result = "record_version_conflict"
	case errors.Is(err, ErrWriterEpochMismatch):
		result = "writer_epoch_mismatch"
	case errors.Is(err, ErrTimeoutUnknown):
		result = "timeout_unknown"
	case errors.Is(err, ErrUnavailable):
		result = "unavailable_not_sent"
	case errors.Is(err, ErrNotFound):
		result = "not_found"
	case errors.Is(err, ErrExists):
		result = "exists"
	case err != nil:
		result = "error"
	}
	s.requests.WithLabelValues(operation, result).Inc()
}

func (s *measuredStore) Get(ctx context.Context, id uint64) (data []byte, version uint64, err error) {
	start := time.Now()
	data, version, err = s.inner.Get(ctx, id)
	s.observe("get", start, err)
	return
}

func (s *measuredStore) Insert(ctx context.Context, id uint64, data []byte) (err error) {
	start := time.Now()
	err = s.inner.Insert(ctx, id, data)
	s.observe("insert", start, err)
	return
}

func (s *measuredStore) CAS(ctx context.Context, id, expectVersion uint64, data []byte) (err error) {
	start := time.Now()
	err = s.inner.CAS(ctx, id, expectVersion, data)
	s.observe("cas", start, err)
	return
}

func (s *measuredStore) epochStore() (EpochStore, error) {
	store, ok := s.inner.(EpochStore)
	if !ok {
		return nil, ErrUnavailable
	}
	return store, nil
}

func (s *measuredStore) InsertEpoch(ctx context.Context, id, writerEpoch uint64, data []byte) (err error) {
	start := time.Now()
	store, err := s.epochStore()
	if err == nil {
		err = store.InsertEpoch(ctx, id, writerEpoch, data)
	}
	s.observe("insert_epoch", start, err)
	return
}

func (s *measuredStore) CASEpoch(ctx context.Context, id, expectVersion, writerEpoch uint64, data []byte) (err error) {
	start := time.Now()
	store, err := s.epochStore()
	if err == nil {
		err = store.CASEpoch(ctx, id, expectVersion, writerEpoch, data)
	}
	s.observe("cas_epoch", start, err)
	return
}

func (s *measuredStore) Fence(ctx context.Context, id, writerEpoch uint64) (err error) {
	start := time.Now()
	store, err := s.epochStore()
	if err == nil {
		err = store.Fence(ctx, id, writerEpoch)
	}
	s.observe("fence", start, err)
	return
}
