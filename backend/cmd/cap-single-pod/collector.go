package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type collectionState struct {
	mu                           sync.Mutex
	samples                      []resourceSnapshot
	oracleGates                  map[string]int64
	oracleComplete               bool
	observabilityComplete        bool
	scrapeErrors                 int64
	missingMetricSamples         int64
	lastSampleAt                 time.Time
	lastSampleValue              resourceSnapshot
	runtimeStop                  string
	serviceHighSamples           int
	serviceFDHighSamples         int
	rejectionHighSamples         int
	dependencyHighSamples        int
	dependencyErrorSamples       int
	driverHighSamples            int
	arrivalLagHighSamples        int
	actualRatioLowSamples        int
	hostStealHighSamples         int
	hostBackgroundSamples        int
	positiveQueueWindows         int
	arrivalLagPositiveWindows    int
	eventBacklogPositiveWindows  int
	socketErrorPositiveWindows   int
	arrivalLagPositiveWindowsMax int
	eventBacklogWindowsMax       int
	socketErrorWindowsMax        int
	redisWaitPositiveWindows     int
	supportQueuePositiveWindows  int
	p99DegradationSamples        int
	scrapeErrorConsecutive       int
	evidenceWriteErrors          int64
	metricTypes                  map[string]string
	previousMetricValues         map[string]float64
}

func newCollectionState(metricTypes ...map[string]string) *collectionState {
	types := map[string]string{}
	if len(metricTypes) > 0 {
		for name, kind := range metricTypes[0] {
			types[name] = kind
		}
	}
	return &collectionState{oracleGates: make(map[string]int64), oracleComplete: true, observabilityComplete: true, metricTypes: types, previousMetricValues: make(map[string]float64)}
}

func (s *collectionState) addSample(sample resourceSnapshot, sustainedSamples, scrapeGapSamples int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSampleValue = sample
	s.lastSampleAt = time.Now()

	if sample.ScrapeErrorCode != "" {
		s.scrapeErrors++
		s.scrapeErrorConsecutive++
		if len(sample.MissingMetrics) != 0 {
			s.observabilityComplete = false
			s.missingMetricSamples++
			s.setObservabilityStopLocked()
		} else if s.scrapeErrorConsecutive >= scrapeGapSamples {
			s.observabilityComplete = false
			s.setObservabilityStopLocked()
		}
		return s.runtimeStop
	}
	s.scrapeErrorConsecutive = 0

	counterResets := []string{}
	for name, value := range sample.MetricValues {
		if previous, found := s.previousMetricValues[name]; found && s.metricTypes[name] == "counter" && value < previous {
			counterResets = append(counterResets, "COUNTER_RESET:"+name)
		}
		s.previousMetricValues[name] = value
	}
	sample.MissingMetrics = uniqueSorted(sample.MissingMetrics, counterResets)
	s.lastSampleValue = sample
	s.samples = append(s.samples, sample)
	if len(sample.MissingMetrics) != 0 {
		s.observabilityComplete = false
		s.missingMetricSamples++
		s.setObservabilityStopLocked()
		return s.runtimeStop
	}
	if s.runtimeStop != "" {
		return s.runtimeStop
	}

	thirtySecondSamples := (sustainedSamples + 1) / 2
	updateConsecutive := func(condition bool, counter *int) {
		if condition {
			*counter++
		} else {
			*counter = 0
		}
	}
	updateConsecutive(sample.MetricValues["host_cpu_steal_percent"] > 2, &s.hostStealHighSamples)
	updateConsecutive(sample.MetricValues["host_background_cpu_percent"] > 10, &s.hostBackgroundSamples)
	if environmentInvalid(aggregateMetrics([]resourceSnapshot{sample})) || s.hostStealHighSamples >= thirtySecondSamples || s.hostBackgroundSamples >= sustainedSamples {
		s.runtimeStop = VerdictInconclusiveEnvironment
		return s.runtimeStop
	}

	updateConsecutive(sample.DriverCPUPercent >= 80, &s.driverHighSamples)
	updateConsecutive(sample.MetricValues["arrival_lag_p99_ms"] > 50, &s.arrivalLagHighSamples)
	actualRatio, actualRatioFound := sample.MetricValues["actual_to_planned_ratio"]
	updateConsecutive(actualRatioFound && actualRatio < .95, &s.actualRatioLowSamples)
	updateConsecutive(metricSlope(s.samples, "arrival_lag_p99_ms").Positive, &s.arrivalLagPositiveWindows)
	updateConsecutive(metricSlope(s.samples, "driver_event_backlog").Positive, &s.eventBacklogPositiveWindows)
	updateConsecutive(metricSlope(s.samples, "driver_socket_error_total").Positive, &s.socketErrorPositiveWindows)
	if s.arrivalLagPositiveWindows > s.arrivalLagPositiveWindowsMax {
		s.arrivalLagPositiveWindowsMax = s.arrivalLagPositiveWindows
	}
	if s.eventBacklogPositiveWindows > s.eventBacklogWindowsMax {
		s.eventBacklogWindowsMax = s.eventBacklogPositiveWindows
	}
	if s.socketErrorPositiveWindows > s.socketErrorWindowsMax {
		s.socketErrorWindowsMax = s.socketErrorPositiveWindows
	}

	updateConsecutive(sample.DependencyCPUPercent >= 85 || sample.DependencyMemoryPercent >= 90, &s.dependencyHighSamples)
	updateConsecutive(sample.MetricValues["dependency_error_percent"] >= 1, &s.dependencyErrorSamples)
	updateConsecutive(metricSlope(s.samples, "redis_pool_wait_seconds").Positive, &s.redisWaitPositiveWindows)
	updateConsecutive(metricSlope(s.samples, "support_service_primary_queue_items").Positive, &s.supportQueuePositiveWindows)

	updateConsecutive(sample.ServiceCPUPercent >= 90 || sample.ServiceMemoryPercent >= 90, &s.serviceHighSamples)
	updateConsecutive(sample.MetricValues["measured_service_fd_percent_of_limit"] >= 85, &s.serviceFDHighSamples)
	updateConsecutive(sample.MetricValues["controlled_rejection_or_timeout_percent"] >= 5, &s.rejectionHighSamples)
	updateConsecutive(sample.MetricValues["p99_non_linear_degradation_vs_previous_tier"] >= 2, &s.p99DegradationSamples)
	updateConsecutive(queueSlope(s.samples).Positive, &s.positiveQueueWindows)

	if s.driverHighSamples >= sustainedSamples || s.arrivalLagHighSamples >= sustainedSamples || s.actualRatioLowSamples >= thirtySecondSamples ||
		s.arrivalLagPositiveWindows >= 3 || s.eventBacklogPositiveWindows >= 3 || s.socketErrorPositiveWindows >= 3 {
		s.runtimeStop = VerdictInconclusiveDriver
	} else if s.dependencyHighSamples >= sustainedSamples || s.dependencyErrorSamples >= thirtySecondSamples || s.redisWaitPositiveWindows >= 3 || s.supportQueuePositiveWindows >= 3 {
		s.runtimeStop = VerdictInconclusiveDependency
	} else if sample.MetricValues["measured_service_oom_kill_total"] > 0 || s.serviceHighSamples >= sustainedSamples || s.serviceFDHighSamples >= sustainedSamples ||
		s.rejectionHighSamples >= sustainedSamples || s.p99DegradationSamples >= 2*sustainedSamples || s.positiveQueueWindows >= 3 {
		s.runtimeStop = VerdictFailTargetSaturation
	}
	return s.runtimeStop
}

func (s *collectionState) setObservabilityStopLocked() {
	if s.runtimeStop != VerdictInconclusiveEnvironment {
		s.runtimeStop = VerdictInconclusiveObservability
	}
}

func (s *collectionState) lastSample() resourceSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSampleValue
}

func (s *collectionState) addOracle(oracle oracleSnapshot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	incomplete := !oracle.ObservationComplete || oracle.ErrorCode != ""
	if incomplete {
		s.oracleComplete = false
		s.observabilityComplete = false
		s.setObservabilityStopLocked()
	}
	signal := false
	for name, value := range oracle.Gates {
		if value > s.oracleGates[name] {
			s.oracleGates[name] = value
		}
		if value != 0 {
			signal = true
		}
	}
	return signal || incomplete
}

func (s *collectionState) addEvidenceWriteError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evidenceWriteErrors++
	s.observabilityComplete = false
	s.setObservabilityStopLocked()
}

func (s *collectionState) diagnosticMetrics() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]float64{
		"arrival_lag_positive_slope_windows_max":          float64(s.arrivalLagPositiveWindowsMax),
		"driver_event_backlog_positive_slope_windows_max": float64(s.eventBacklogWindowsMax),
		"driver_socket_error_positive_slope_windows_max":  float64(s.socketErrorWindowsMax),
		"evidence_write_error_total":                      float64(s.evidenceWriteErrors),
	}
}

func (s *collectionState) runtimeStopCode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runtimeStop
}

func (s *collectionState) evidenceWriteErrorCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evidenceWriteErrors
}

func (s *collectionState) snapshot() ([]resourceSnapshot, map[string]int64, bool, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	samples := append([]resourceSnapshot(nil), s.samples...)
	gates := make(map[string]int64, len(s.oracleGates))
	for key, value := range s.oracleGates {
		gates[key] = value
	}
	return samples, gates, s.oracleComplete, s.observabilityComplete, s.runtimeStop
}

type evidenceSink interface{ WriteRecord([]byte) error }

type fileEvidenceSink struct {
	file *os.File
	mu   sync.Mutex
}

func (sink *fileEvidenceSink) WriteRecord(data []byte) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	written, err := sink.file.Write(data)
	if err != nil || written != len(data) {
		return errors.New("resource sample write failed")
	}
	return nil
}

type collector struct {
	plan            runPlan
	client          *http.Client
	state           *collectionState
	file            *os.File
	sink            evidenceSink
	requiredMetrics []string
	requiredGates   []string
	accumulator     *runAccumulator
	judge           *tierJudge
}

func newCollector(plan runPlan, outputPath string, accumulator *runAccumulator, judge *tierJudge) (*collector, error) {
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return nil, err
	}
	required, err := loadRequiredSpec(plan.RequiredMetricsPath, plan.Service, binding.RequiredMetricsSchemaVersion)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &collector{plan: plan, client: &http.Client{Timeout: 2 * time.Second}, state: newCollectionState(augmentMetricTypes(required.Types)), file: file, sink: &fileEvidenceSink{file: file},
		requiredMetrics: effectiveRequiredMetricNames(required), requiredGates: required.Gates, accumulator: accumulator, judge: judge}, nil
}

func (c *collector) close() error { return c.file.Close() }

func (c *collector) run(ctx context.Context, cancelRun context.CancelFunc) {
	interval := time.Duration(c.plan.SampleIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sample := c.fetchSnapshot(ctx)
			sample.ObservedAt = now.UTC().Format(time.RFC3339Nano)
			sustainedSamples := (60*1000 + c.plan.SampleIntervalMS - 1) / c.plan.SampleIntervalMS
			scrapeGapSamples := (5*1000+c.plan.SampleIntervalMS-1)/c.plan.SampleIntervalMS + 1
			stop := c.state.addSample(sample, sustainedSamples, scrapeGapSamples)
			if err := c.writeSample(c.state.lastSample()); err != nil {
				c.state.addEvidenceWriteError()
				cancelRun()
			}
			if stop != "" {
				cancelRun()
			}
			oracle := c.fetchOracle(ctx)
			if c.state.addOracle(oracle) {
				cancelRun()
			}
		}
	}
}

func (c *collector) fetchSnapshot(ctx context.Context) resourceSnapshot {
	var sample resourceSnapshot
	if !fetchStrictJSON(ctx, c.client, c.plan.SnapshotURL, &sample) {
		return resourceSnapshot{ScrapeErrorCode: "SNAPSHOT_SCRAPE_FAILED", MetricValues: map[string]float64{}}
	}
	if sample.MetricValues == nil {
		sample.MetricValues = map[string]float64{}
	}
	if sample.ScrapeErrorCode != "" && sample.ScrapeErrorCode != "COLLECTOR_SOURCE_FAILED" {
		sample.ScrapeErrorCode = "SNAPSHOT_PROTOCOL_ERROR"
		sample.MissingMetrics = []string{"UNEXPECTED_MISSING_METRIC_SET"}
	}
	normalizeSnapshotMetrics(&sample, c.accumulator, c.judge)
	if unexpectedMetricSet(sample.MetricValues, c.requiredMetrics) {
		sample.MissingMetrics = append(sample.MissingMetrics, "UNEXPECTED_METRIC_SET")
	}
	sample.MissingMetrics = uniqueSorted(sample.MissingMetrics, missingMetricNames(sample.MetricValues, c.requiredMetrics))
	sample.MissingMetrics = sanitizeMissingMetrics(sample.MissingMetrics, c.requiredMetrics)
	return sample
}

func (c *collector) fetchOracle(ctx context.Context) oracleSnapshot {
	var oracle oracleSnapshot
	if !fetchStrictJSON(ctx, c.client, c.plan.OracleURL, &oracle) {
		return oracleSnapshot{ObservationComplete: false, Gates: map[string]int64{}, ErrorCode: "ORACLE_SCRAPE_FAILED"}
	}
	if oracle.Gates == nil {
		oracle.Gates = map[string]int64{}
	}
	if !exactGateSet(oracle.Gates, c.requiredGates) {
		oracle.ObservationComplete = false
		oracle.ErrorCode = "ORACLE_GATE_SET_INCOMPLETE"
		return oracle
	}
	for _, value := range oracle.Gates {
		if value < 0 {
			oracle.ObservationComplete = false
			oracle.ErrorCode = "ORACLE_VALUE_INVALID"
			return oracle
		}
	}
	return oracle
}

func fetchStrictJSON(ctx context.Context, client *http.Client, endpoint string, target any) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false
	}
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	var trailing any
	return errors.Is(decoder.Decode(&trailing), io.EOF)
}

func normalizeSnapshotMetrics(sample *resourceSnapshot, accumulator *runAccumulator, judge *tierJudge) {
	values := sample.MetricValues
	values["service_cpu_percent"] = sample.ServiceCPUPercent
	values["service_rss_bytes"] = float64(sample.ServiceRSSBytes)
	values["service_memory_percent_of_limit"] = sample.ServiceMemoryPercent
	values["measured_service_memory_percent_of_limit"] = sample.ServiceMemoryPercent
	values["service_fd_open"] = float64(sample.ServiceFDOpen)
	values["driver_cpu_percent"] = sample.DriverCPUPercent
	values["driver_rss_bytes"] = float64(sample.DriverRSSBytes)
	values["driver_fd_open"] = float64(sample.DriverFDOpen)
	values["redis_pool_wait_seconds"] = sample.RedisPoolWaitSeconds
	if accumulator == nil {
		return
	}
	actual := accumulator.snapshot()
	values["planned_arrivals_total"] = float64(actual.Planned)
	values["launched_arrivals_total"] = float64(actual.Launched)
	values["scheduled_unsent_total"] = float64(actual.ScheduledUnsent)
	values["completed_total"] = float64(actual.Completed)
	values["rejected_total"] = float64(actual.Rejected)
	values["timeout_total"] = float64(actual.Timeout)
	values["arrival_lag_ms"] = actual.ArrivalLagMS["p99"]
	values["arrival_lag_p99_ms"] = actual.ArrivalLagMS["p99"]
	if actual.Planned == 0 {
		values["actual_to_planned_ratio"] = 1
	} else {
		values["actual_to_planned_ratio"] = actual.ActualToPlannedRatio
	}
	attempted := actual.Completed + actual.Rejected + actual.Timeout + actual.Failed
	if attempted == 0 {
		values["controlled_rejection_or_timeout_percent"] = 0
	} else {
		values["controlled_rejection_or_timeout_percent"] = 100 * float64(actual.Rejected+actual.Timeout+actual.Failed) / float64(attempted)
	}
	if judge == nil {
		return
	}
	intervalP99, intervalCount := accumulator.takeLatencyInterval()
	for name, value := range judge.observe(*sample, actual, intervalP99, intervalCount) {
		values[name] = value
	}
}

func (c *collector) writeSample(sample resourceSnapshot) error {
	encoded, err := json.Marshal(sample)
	if err != nil {
		return errors.New("resource sample encode failed")
	}
	if c.sink == nil {
		return errors.New("resource sample sink unavailable")
	}
	return c.sink.WriteRecord(append(encoded, '\n'))
}
