package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"farmgame/internal/testkit/capaudit"
	"farmgame/internal/testkit/capcorrectness"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func statusOf(t *testing.T, exporter *capcorrectness.Exporter, precheck ...precheckObservation) status {
	t.Helper()
	recorder := httptest.NewRecorder()
	statusHandler(exporter, precheck)(recorder, httptest.NewRequest(http.MethodGet, "/status", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status responded %d", recorder.Code)
	}
	var body status
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func publishedIn(t *testing.T, observation precheckObservation) map[string]float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	if err := observation.publish(registry); err != nil {
		t.Fatal(err)
	}
	return served(t, registry)
}

// The count is a before-the-run fact. Recomputed later it would read the run's
// own writes as residue, so what the fixture writes after the capture must not
// reach the published number.
//
// The shard rows a deployment writes when it claims the keyspace are not
// residue: the deployment about to be measured rewrites them, and a
// dynamic-routing deployment is never without them. A projection row is, because
// only somebody's play writes one.
func TestTheKeyspaceIsCountedOnceBeforeTheRunAndNotAgain(t *testing.T) {
	audit := testAudit(t)
	client := redis.NewClient(&redis.Options{Addr: audit.RedisAddr})
	defer func() { _ = client.Close() }()
	if err := client.Set(context.Background(), "classic-farm:test:z1:proj:redpoint:v1:900000001", "left", 0).Err(); err != nil {
		t.Fatal(err)
	}

	observed := captureNamespaceResidual(context.Background(), audit)
	if observed.Problem != "" {
		t.Fatalf("the keyspace could not be counted: %s", observed.Problem)
	}
	if observed.Value == nil || *observed.Value != 1 || observed.AtLeast {
		t.Fatalf("observation=%+v, want the one leftover projection row counted and the shard rows ignored", observed)
	}

	if err := client.Set(context.Background(), "classic-farm:test:z1:proj:redpoint:v1:900000002", "run", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := publishedIn(t, observed)[capaudit.NamespaceResidualMetric]; got != 1 {
		t.Fatalf("%s=%v after the run wrote to the keyspace, want the count taken before it", capaudit.NamespaceResidualMetric, got)
	}
}

func TestAnEmptyKeyspaceIsPublishedAsAMeasuredZero(t *testing.T) {
	audit := testAudit(t)
	client := redis.NewClient(&redis.Options{Addr: audit.RedisAddr})
	defer func() { _ = client.Close() }()
	if err := client.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}

	observed := captureNamespaceResidual(context.Background(), audit)
	served := publishedIn(t, observed)
	if value, found := served[capaudit.NamespaceResidualMetric]; !found || value != 0 {
		t.Fatalf("%s served=%v value=%v, want an explicit 0", capaudit.NamespaceResidualMetric, found, value)
	}
}

// A keyspace nobody can read costs the metric. Publishing a zero instead would
// tell the harness the run began on clean storage, which is the one thing
// nobody observed.
func TestAKeyspaceThatCannotBeReadLeavesTheMetricAbsentAndSaysWhy(t *testing.T) {
	audit := testAudit(t)
	audit.RedisAddr = "127.0.0.1:1"

	observed := captureNamespaceResidual(context.Background(), audit)
	if observed.Value != nil || observed.Problem == "" {
		t.Fatalf("observation=%+v, want no value and a reason", observed)
	}
	if _, served := publishedIn(t, observed)[capaudit.NamespaceResidualMetric]; served {
		t.Fatal("a count nobody took was published anyway")
	}

	exporter, _, err := buildExporter(fileConfig{SchemaVersion: configSchema, Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	body := statusOf(t, exporter, observed)
	if len(body.Precheck) != 1 || body.Precheck[0].Check != capaudit.NamespaceResidualMetric {
		t.Fatalf("status precheck=%+v, want the residual named", body.Precheck)
	}
	if body.Precheck[0].Problem == "" || body.Precheck[0].Value != nil {
		t.Fatalf("status precheck=%+v, want the reason and no value", body.Precheck[0])
	}
	if len(body.Gates) != len(capcorrectness.Gates()) {
		t.Fatalf("status reported %d gates, want all %d", len(body.Gates), len(capcorrectness.Gates()))
	}
}

// Without storage in the config there is no keyspace to count, and the same
// silence has to follow: no metric, and the reason on /status.
func TestWithoutStorageInTheConfigThePrecheckIsUnobserved(t *testing.T) {
	observed := captureNamespaceResidual(context.Background(), nil)

	if observed.Value != nil || observed.Problem == "" {
		t.Fatalf("observation=%+v, want no value and a reason", observed)
	}
	if _, served := publishedIn(t, observed)[capaudit.NamespaceResidualMetric]; served {
		t.Fatal("a keyspace that is not in the config was reported clean")
	}
}
