package main

import (
	"context"
	"fmt"
	"time"

	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/testkit/capaudit"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// residualHelp states which moment the number describes, so a reader of the
// exposition cannot take it for a live gauge of the keyspace.
const residualHelp = "Keys the run's keyspace already held when the oracle started, before any load arrived."

// residualTimeout bounds the one count taken at startup. The scan bounds how
// much it reads by itself; this bounds a Redis that answers slowly or not at
// all, so an unreadable keyspace costs the metric rather than the oracle.
const residualTimeout = 30 * time.Second

// precheckObservation is one environment fact as it stood before the run began:
// a value the harness can read, or the reason it could not be read. A precheck
// with a problem is published nowhere and appears only here, which is what
// makes the run report as incompletely observed instead of as clean.
type precheckObservation struct {
	Check string `json:"check"`
	// Value is a pointer because the measured answer is normally zero, and a
	// zero serialized next to a problem is exactly the reading this mechanism
	// exists to prevent.
	Value   *int64 `json:"value,omitempty"`
	AtLeast bool   `json:"at_least,omitempty"`
	Problem string `json:"problem,omitempty"`
	help    string
}

// captureNamespaceResidual counts what the run's keyspace already holds.
//
// The count is taken once, at startup, and never again: recomputed on a scrape
// it would count the run's own writes as residue and climb as the run
// proceeds. Every way of not getting an answer ends as a problem string rather
// than as a zero, because a zero here is a claim that the run began on clean
// storage.
func captureNamespaceResidual(ctx context.Context, config *auditConfig) precheckObservation {
	observation := precheckObservation{Check: capaudit.NamespaceResidualMetric, help: residualHelp}
	if config == nil {
		observation.Problem = "storage to audit is not in the config, so the keyspace the run will use is unknown"
		return observation
	}
	namespace, err := rediskey.NewNamespace(config.Namespace, config.Zone)
	if err != nil {
		observation.Problem = fmt.Sprintf("the run's namespace cannot be built from the config: %v", err)
		return observation
	}
	client := redis.NewClient(&redis.Options{Addr: config.RedisAddr})
	defer func() { _ = client.Close() }()
	residual, err := capaudit.NewNamespaceResidual(capaudit.NamespaceResidualConfig{
		Client: client, Namespace: namespace, WorkStorePrefix: config.WorkStorePrefix,
	})
	if err != nil {
		observation.Problem = err.Error()
		return observation
	}
	ctx, cancel := context.WithTimeout(ctx, residualTimeout)
	defer cancel()
	count, err := residual.Count(ctx)
	if err != nil {
		observation.Problem = err.Error()
		return observation
	}
	observation.Value, observation.AtLeast = &count.Keys, count.AtLeast
	return observation
}

// publish serves the value under its contract name. An observation without one
// registers nothing: the harness then reports the metric as missing from the
// sample, which is the true answer, and /status carries why it is missing.
func (o precheckObservation) publish(registry *prometheus.Registry) error {
	if o.Value == nil {
		return nil
	}
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: o.Check, Help: o.help})
	gauge.Set(float64(*o.Value))
	return registry.Register(gauge)
}
