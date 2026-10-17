package capdriver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	HealthSchema    = "cap-l2-driver-health.v1"
	OperationSchema = "cap-l2-driver-operation.v1"
	maxRequestBytes = 64 << 10
)

type Service string

const (
	ServiceGate   Service = "gate"
	ServicePlayer Service = "player"
	ServiceFarm   Service = "farm"
	ServiceWorker Service = "worker"
)

func (s Service) valid() bool {
	switch s {
	case ServiceGate, ServicePlayer, ServiceFarm, ServiceWorker:
		return true
	default:
		return false
	}
}

type Status string

const (
	StatusCompleted Status = "COMPLETED"
	StatusRejected  Status = "REJECTED"
	StatusTimeout   Status = "TIMEOUT"
	StatusFailed    Status = "FAILED"
)

type Config struct {
	BinarySHA256    string
	ConfigSHA256    string
	Service         Service
	Profile         string
	MaximumInflight int
	Ready           bool
	Blocker         string
}

func (c Config) Validate() error {
	if !isSHA256(c.BinarySHA256) || !isSHA256(c.ConfigSHA256) {
		return errors.New("capdriver: binary and config SHA256 are required")
	}
	if !c.Service.valid() || strings.TrimSpace(c.Profile) == "" || c.MaximumInflight <= 0 {
		return errors.New("capdriver: service, profile, and positive maximum inflight are required")
	}
	if c.Ready && c.Blocker != "" {
		return errors.New("capdriver: ready driver cannot carry a blocker")
	}
	if !c.Ready && strings.TrimSpace(c.Blocker) == "" {
		return errors.New("capdriver: non-ready driver requires an allowlisted blocker")
	}
	if c.Blocker != "" && c.Blocker != "DESIGN_BLOCKER" && c.Blocker != "ADAPTER_NOT_BOUND" {
		return errors.New("capdriver: blocker is not allowlisted")
	}
	return nil
}

type Health struct {
	SchemaVersion   string  `json:"schema_version"`
	BinarySHA256    string  `json:"binary_sha256"`
	ConfigSHA256    string  `json:"config_sha256"`
	Service         Service `json:"service"`
	Profile         string  `json:"profile"`
	MaximumInflight int     `json:"maximum_inflight"`
	Ready           bool    `json:"ready"`
	Blocker         string  `json:"blocker,omitempty"`
	Inflight        int64   `json:"inflight"`
}

type OperationRequest struct {
	SchemaVersion string  `json:"schema_version"`
	Sequence      uint64  `json:"sequence"`
	ScheduledAt   string  `json:"scheduled_at"`
	Service       Service `json:"service"`
	Profile       string  `json:"profile"`
	Phase         string  `json:"phase"`
}

type OperationResponse struct {
	SchemaVersion string         `json:"schema_version"`
	Sequence      uint64         `json:"sequence"`
	Status        Status         `json:"status"`
	ErrorClass    string         `json:"error_class,omitempty"`
	QueueDepth    int64          `json:"queue_depth"`
	Correctness   map[string]int `json:"correctness_gates"`
}

type Result struct {
	Status      Status
	ErrorClass  string
	QueueDepth  int64
	Correctness map[string]int
}

type Adapter interface {
	Execute(context.Context, OperationRequest) Result
}

type Server struct {
	config   Config
	adapter  Adapter
	inflight atomic.Int64
	sema     chan struct{}
}

func NewServer(config Config, adapter Adapter) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if config.Ready && adapter == nil {
		return nil, errors.New("capdriver: ready driver requires an adapter")
	}
	return &Server{config: config, adapter: adapter, sema: make(chan struct{}, config.MaximumInflight)}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /operation", s.operation)
	mux.Handle("GET /metrics", s.metrics())
	return mux
}

// metrics exposes the load generator's own counters. The collector reads the
// driver's cost from its cgroup, but a stalled or backlogged driver looks
// identical to a slow service from the outside, so the work the driver knows
// about has to come from the driver itself.
func (s *Server) metrics() http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "driver_event_backlog",
		Help: "Operations accepted by the driver that have not yet completed.",
	}, func() float64 { return float64(s.inflight.Load()) }))
	if collector, ok := s.adapter.(prometheus.Collector); ok {
		registry.MustRegister(collector)
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, Health{
		SchemaVersion: HealthSchema, BinarySHA256: s.config.BinarySHA256, ConfigSHA256: s.config.ConfigSHA256,
		Service: s.config.Service, Profile: s.config.Profile, MaximumInflight: s.config.MaximumInflight,
		Ready: s.config.Ready, Blocker: s.config.Blocker, Inflight: s.inflight.Load(),
	})
}

func (s *Server) operation(w http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(w, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input OperationRequest
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, 0, "INVALID_REQUEST")
		return
	}
	if err := ensureEOF(decoder); err != nil {
		writeError(w, http.StatusBadRequest, input.Sequence, "INVALID_REQUEST")
		return
	}
	if input.SchemaVersion != OperationSchema || input.Sequence == 0 || input.Service != s.config.Service || input.Profile != s.config.Profile || !validPhase(input.Phase) {
		writeError(w, http.StatusBadRequest, input.Sequence, "IDENTITY_MISMATCH")
		return
	}
	when, err := time.Parse(time.RFC3339Nano, input.ScheduledAt)
	if err != nil || when.IsZero() {
		writeError(w, http.StatusBadRequest, input.Sequence, "INVALID_SCHEDULE")
		return
	}
	if !s.config.Ready || s.adapter == nil {
		writeError(w, http.StatusServiceUnavailable, input.Sequence, s.config.Blocker)
		return
	}
	select {
	case s.sema <- struct{}{}:
		defer func() { <-s.sema }()
	default:
		writeError(w, http.StatusTooManyRequests, input.Sequence, "DRIVER_INFLIGHT_LIMIT")
		return
	}
	s.inflight.Add(1)
	defer s.inflight.Add(-1)
	result := s.adapter.Execute(request.Context(), input)
	if !validResult(result) {
		writeError(w, http.StatusInternalServerError, input.Sequence, "ADAPTER_INVALID_RESULT")
		return
	}
	writeJSON(w, http.StatusOK, OperationResponse{
		SchemaVersion: OperationSchema, Sequence: input.Sequence, Status: result.Status,
		ErrorClass: result.ErrorClass, QueueDepth: result.QueueDepth, Correctness: cloneCorrectness(result.Correctness),
	})
}

func validResult(result Result) bool {
	switch result.Status {
	case StatusCompleted:
		return result.ErrorClass == "" && result.QueueDepth >= 0 && result.Correctness != nil
	case StatusRejected, StatusTimeout, StatusFailed:
		return allowlistedError(result.ErrorClass) && result.QueueDepth >= 0 && result.Correctness != nil
	default:
		return false
	}
}

func allowlistedError(value string) bool {
	switch value {
	case "CAPACITY_REJECTED", "DEPENDENCY_UNAVAILABLE", "REQUEST_TIMEOUT", "CORRECTNESS_STOP":
		return true
	default:
		return false
	}
}

// Phases is the canonical set of run phases. The harness and the driver must
// agree exactly: a phase the harness can plan but the driver does not know is
// rejected as an identity mismatch, which would fail every operation of an
// otherwise legitimate run. Deriving both sides from this list keeps them from
// drifting apart again.
func Phases() []string {
	return []string{
		"PRECHECK", "OBSERVABILITY_QUALIFICATION", "LOAD_GENERATOR_QUALIFICATION",
		"SMOKE", "WARMUP", "STAIRCASE", "SAFE_TIER_CANDIDATE",
		"FORMAL_1", "COOLDOWN_1", "FORMAL_2", "COOLDOWN_2", "FORMAL_3",
		"INDEPENDENT_REVIEW",
	}
}

// Services is the canonical set of measured services, for the same reason.
func Services() []Service {
	return []Service{ServiceGate, ServicePlayer, ServiceFarm, ServiceWorker}
}

func validPhase(value string) bool {
	for _, phase := range Phases() {
		if phase == value {
			return true
		}
	}
	return false
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("extra JSON value")
		}
		return err
	}
	return nil
}

func cloneCorrectness(input map[string]int) map[string]int {
	output := make(map[string]int, len(input))
	for key, value := range input {
		if strings.TrimSpace(key) == "" {
			continue
		}
		output[key] = value
	}
	return output
}

func writeError(w http.ResponseWriter, code int, sequence uint64, class string) {
	if class == "" {
		class = "ADAPTER_NOT_BOUND"
	}
	writeJSON(w, code, OperationResponse{
		SchemaVersion: OperationSchema, Sequence: sequence, Status: StatusFailed,
		ErrorClass: class, QueueDepth: 0, Correctness: map[string]int{},
	})
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func isSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func ConfigSHA256(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("capdriver: marshal config: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
