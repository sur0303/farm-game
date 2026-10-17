// Package friendopportunity contains a deterministic synthetic benchmark for
// friend-opportunity discovery. It never connects to Redis or a real Scheduler.
package friendopportunity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	TraceSchemaVersion  = "friend-opportunity.trace.v2"
	ResultSchemaVersion = "friend-opportunity.result.v2"
	CostModelVersion    = "friend-opportunity.synthetic-cost.v2"

	SafeFallbackIntervalMS int64 = 30_000
	maxTraceEvents               = 10_000_000
)

type Variant string

const (
	VariantPull               Variant = "A/Pull"
	VariantSchedulerSafe      Variant = "B1/SchedulerSafe"
	VariantSchedulerEfficient Variant = "B2/SchedulerEfficient"
)

func (v Variant) Validate() error {
	switch v {
	case VariantPull, VariantSchedulerSafe, VariantSchedulerEfficient:
		return nil
	default:
		return fmt.Errorf("friendopportunity: unknown variant %q", v)
	}
}

func (v Variant) FallbackIntervalMS(c RunConfig) int64 {
	if v == VariantSchedulerEfficient {
		return c.EfficientFallbackIntervalMS
	}
	return c.SafeFallbackIntervalMS
}

func (v Variant) UsesScheduler() bool {
	return v == VariantSchedulerSafe || v == VariantSchedulerEfficient
}

type ExpiryPattern string

const (
	ExpiryUniform ExpiryPattern = "uniform"
	ExpiryBursty  ExpiryPattern = "bursty"
)

func (p ExpiryPattern) Validate() error {
	switch p {
	case ExpiryUniform, ExpiryBursty:
		return nil
	default:
		return fmt.Errorf("friendopportunity: unknown expiry pattern %q", p)
	}
}

// CostModel contains only synthetic logical-operation sizes and service times.
// Values are serialized with every run and are not calibrated Redis results.
type CostModel struct {
	PullBaseLatencyMS           int64 `json:"pull_base_latency_ms"`
	PullPageLatencyMS           int64 `json:"pull_page_latency_ms"`
	PullFriendLatencyUS         int64 `json:"pull_friend_latency_us"`
	FriendIndexReadBaseBytes    int64 `json:"friend_index_read_base_bytes"`
	FriendIndexFriendBytes      int64 `json:"friend_index_friend_bytes"`
	AuthorityHeaderReadBytes    int64 `json:"authority_header_read_bytes"`
	AuthorityFactsReadBytes     int64 `json:"authority_facts_read_bytes"`
	ScheduleWriteBytes          int64 `json:"schedule_write_bytes"`
	WakeupClaimBytes            int64 `json:"wakeup_claim_bytes"`
	SessionRouteReadBytes       int64 `json:"session_route_read_bytes"`
	HintDeliveryLatencyMS       int64 `json:"hint_delivery_latency_ms"`
	ExactPullAfterHintLatencyMS int64 `json:"exact_pull_after_hint_latency_ms"`
}

func DefaultCostModel() CostModel {
	return CostModel{
		PullBaseLatencyMS:           1,
		PullPageLatencyMS:           1,
		PullFriendLatencyUS:         10,
		FriendIndexReadBaseBytes:    64,
		FriendIndexFriendBytes:      48,
		AuthorityHeaderReadBytes:    128,
		AuthorityFactsReadBytes:     384,
		ScheduleWriteBytes:          160,
		WakeupClaimBytes:            96,
		SessionRouteReadBytes:       64,
		HintDeliveryLatencyMS:       3,
		ExactPullAfterHintLatencyMS: 2,
	}
}

func (m CostModel) Validate() error {
	if m.PullBaseLatencyMS < 0 || m.PullPageLatencyMS < 0 || m.PullFriendLatencyUS < 0 ||
		m.HintDeliveryLatencyMS < 0 || m.ExactPullAfterHintLatencyMS < 0 {
		return errors.New("friendopportunity: cost-model latencies must be >= 0")
	}
	if m.FriendIndexReadBaseBytes < 0 || m.FriendIndexFriendBytes < 0 ||
		m.AuthorityHeaderReadBytes < 0 || m.AuthorityFactsReadBytes < 0 ||
		m.ScheduleWriteBytes < 0 || m.WakeupClaimBytes < 0 || m.SessionRouteReadBytes < 0 {
		return errors.New("friendopportunity: cost-model bytes must be >= 0")
	}
	return nil
}

type RunConfig struct {
	Seed                        int64         `json:"seed"`
	FriendCount                 int           `json:"friend_count"`
	PlotCount                   int           `json:"plot_count"`
	OpportunityDensityPercent   int           `json:"opportunity_density_percent"`
	OnlineRatePercent           float64       `json:"online_rate_percent"`
	ExpiryPattern               ExpiryPattern `json:"expiry_pattern"`
	ArrivalRatePerSecond        float64       `json:"arrival_rate_per_second"`
	DurationMS                  int64         `json:"duration_ms"`
	SafeFallbackIntervalMS      int64         `json:"safe_fallback_interval_ms"`
	EfficientFallbackIntervalMS int64         `json:"efficient_fallback_interval_ms"`
	PullPageSize                int           `json:"pull_page_size"`
	GateBatchSize               int           `json:"gate_batch_size"`
	WorkerConcurrency           int           `json:"worker_concurrency"`
	MaxInFlight                 int           `json:"max_in_flight"`
	CostModel                   CostModel     `json:"cost_model"`
}

func DefaultConfig() RunConfig {
	return RunConfig{
		Seed:                        79,
		FriendCount:                 20,
		PlotCount:                   5,
		OpportunityDensityPercent:   20,
		OnlineRatePercent:           5,
		ExpiryPattern:               ExpiryUniform,
		ArrivalRatePerSecond:        10,
		DurationMS:                  300_000,
		SafeFallbackIntervalMS:      SafeFallbackIntervalMS,
		EfficientFallbackIntervalMS: 120_000,
		PullPageSize:                20,
		GateBatchSize:               64,
		WorkerConcurrency:           1,
		CostModel:                   DefaultCostModel(),
	}
}

func SupportedFriendCounts() []int         { return []int{20, 100, 300} }
func SupportedPlotCounts() []int           { return []int{5, 12, 24} }
func SupportedOpportunityDensities() []int { return []int{0, 5, 20, 50} }
func SupportedOnlineRates() []float64      { return []float64{1.9, 5, 10} }

func (c RunConfig) Validate() error {
	if !containsInt(c.FriendCount, SupportedFriendCounts()) {
		return fmt.Errorf("friendopportunity: friend_count must be one of %v", SupportedFriendCounts())
	}
	if !containsInt(c.PlotCount, SupportedPlotCounts()) {
		return fmt.Errorf("friendopportunity: plot_count must be one of %v", SupportedPlotCounts())
	}
	if !containsInt(c.OpportunityDensityPercent, SupportedOpportunityDensities()) {
		return fmt.Errorf("friendopportunity: opportunity density must be one of %v", SupportedOpportunityDensities())
	}
	if math.IsNaN(c.OnlineRatePercent) || math.IsInf(c.OnlineRatePercent, 0) ||
		!containsFloat(c.OnlineRatePercent, SupportedOnlineRates()) {
		return fmt.Errorf("friendopportunity: online rate must be one of %v", SupportedOnlineRates())
	}
	if err := c.ExpiryPattern.Validate(); err != nil {
		return err
	}
	if math.IsNaN(c.ArrivalRatePerSecond) || math.IsInf(c.ArrivalRatePerSecond, 0) || c.ArrivalRatePerSecond <= 0 {
		return errors.New("friendopportunity: arrival rate must be finite and > 0")
	}
	if c.DurationMS <= 0 {
		return errors.New("friendopportunity: duration_ms must be > 0")
	}
	if c.SafeFallbackIntervalMS != SafeFallbackIntervalMS {
		return fmt.Errorf("friendopportunity: A/B1 fallback must remain %dms", SafeFallbackIntervalMS)
	}
	if c.EfficientFallbackIntervalMS <= 0 {
		return errors.New("friendopportunity: B2 fallback interval must be > 0")
	}
	if c.PullPageSize <= 0 || c.GateBatchSize <= 0 || c.WorkerConcurrency <= 0 {
		return errors.New("friendopportunity: page size, gate batch size, and workers must be > 0")
	}
	if c.MaxInFlight < 0 {
		return errors.New("friendopportunity: max_in_flight must be >= 0")
	}
	if err := c.CostModel.Validate(); err != nil {
		return err
	}
	_, err := c.PlannedSessionCount()
	return err
}

func (c RunConfig) PlannedSessionCount() (int, error) {
	estimate := c.ArrivalRatePerSecond * float64(c.DurationMS) / 1000
	if math.IsNaN(estimate) || math.IsInf(estimate, 0) || estimate > maxTraceEvents {
		return 0, fmt.Errorf("friendopportunity: planned session count exceeds %d", maxTraceEvents)
	}
	count := int(math.Ceil(estimate))
	if count < 1 {
		count = 1
	}
	return count, nil
}

func (c RunConfig) Fingerprint() string {
	data, _ := json.Marshal(c)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func MatrixConfigs(base RunConfig) []RunConfig {
	configs := make([]RunConfig, 0, 216)
	for _, friends := range SupportedFriendCounts() {
		for _, plots := range SupportedPlotCounts() {
			for _, density := range SupportedOpportunityDensities() {
				for _, onlineRate := range SupportedOnlineRates() {
					for _, expiry := range []ExpiryPattern{ExpiryUniform, ExpiryBursty} {
						cell := base
						cell.FriendCount, cell.PlotCount = friends, plots
						cell.OpportunityDensityPercent, cell.OnlineRatePercent = density, onlineRate
						cell.ExpiryPattern = expiry
						configs = append(configs, cell)
					}
				}
			}
		}
	}
	return configs
}

func CellID(c RunConfig) string {
	online := strings.ReplaceAll(strconv.FormatFloat(c.OnlineRatePercent, 'f', -1, 64), ".", "p")
	return fmt.Sprintf("friends-%d_plots-%d_density-%d_online-%s_expiry-%s",
		c.FriendCount, c.PlotCount, c.OpportunityDensityPercent, online, c.ExpiryPattern)
}

func containsInt(value int, values []int) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func containsFloat(value float64, values []float64) bool {
	for _, candidate := range values {
		if math.Abs(value-candidate) < 0.000001 {
			return true
		}
	}
	return false
}
