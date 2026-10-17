package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// theftHotStats coordinates a real 200-connection barrier. It deliberately
// lives in the protocol driver: no fixture or audit helper can declare a
// client ready without observing the public SUBSCRIPTION_READY frame.
type theftHotStats struct {
	expected int
	owner    uint64

	mu             sync.Mutex
	readyPlayers   map[uint64]struct{}
	healthyDone    map[uint64]struct{}
	versions       map[uint64]theftVersionAudit
	admissionTimes []time.Duration
	terminalTimes  []time.Duration
	fixtureReadyAt time.Time
	barrierElapsed time.Duration
	start          chan struct{}
	done           chan struct{}
	abort          chan struct{}
	fixtureReady   chan struct{}
	startOnce      sync.Once
	doneOnce       sync.Once
	abortOnce      sync.Once
	fixtureOnce    sync.Once
	fixture        theftFixture

	immediateAccepted    atomic.Int64
	immediateProcess     atomic.Int64
	immediateSuccess     atomic.Int64
	immediateNoChange    atomic.Int64
	immediateRejected    atomic.Int64
	immediateUnavailable atomic.Int64
	immediateError       atomic.Int64
	terminalSuccess      atomic.Int64
	terminalNoChange     atomic.Int64
	terminalRejected     atomic.Int64
	terminalError        atomic.Int64
	queries              atomic.Int64
	processingQueries    atomic.Int64
	unavailableQueries   atomic.Int64
	retries              atomic.Int64
	retryErrors          atomic.Int64
	rejoin               atomic.Int64
	versionRegression    atomic.Int64
	versionGap           atomic.Int64
	changeFrames         atomic.Int64

	slowMu      sync.Mutex
	slowOutcome string
}

type theftVersionAudit struct {
	Baseline uint64   `json:"baseline_farm_version"`
	Last     uint64   `json:"last_farm_version"`
	Versions []uint64 `json:"farm_change_versions"`
	Gap      bool     `json:"gap"`
	Rollback bool     `json:"rollback_or_duplicate"`
}

type theftFixture struct {
	PlotID               uint32 `json:"plot_id"`
	CropCycleID          string `json:"crop_cycle_id"`
	CommittedFarmVersion uint64 `json:"committed_farm_version"`
	PreparedThrough      string `json:"prepared_through"`
}

type theftHotReport struct {
	OwnerPlayerID        uint64                       `json:"owner_player_id"`
	ReadyBarrier         int                          `json:"subscription_ready_barrier"`
	ReadyBarrierMs       float64                      `json:"subscription_ready_barrier_ms"`
	ReadyBarrierQPS      float64                      `json:"subscription_ready_barrier_per_second"`
	HealthyCompleted     int                          `json:"healthy_connections_completed"`
	BurstVisitors        int                          `json:"burst_visitors"`
	SlowReaderEnabled    bool                         `json:"slow_reader_enabled"`
	SlowReaderPlayerID   uint64                       `json:"slow_reader_player_id,omitempty"`
	SlowReaderOutcome    string                       `json:"slow_reader_outcome,omitempty"`
	ImmediateAccepted    int64                        `json:"immediate_accepted"`
	ImmediateProcessing  int64                        `json:"immediate_processing"`
	ImmediateSucceeded   int64                        `json:"immediate_succeeded"`
	ImmediateNoChange    int64                        `json:"immediate_no_change"`
	ImmediateRejected    int64                        `json:"immediate_rejected"`
	ImmediateUnavailable int64                        `json:"immediate_unavailable"`
	ImmediateErrors      int64                        `json:"immediate_errors"`
	TerminalSucceeded    int64                        `json:"terminal_succeeded"`
	TerminalNoChange     int64                        `json:"terminal_no_change"`
	TerminalRejected     int64                        `json:"terminal_rejected"`
	TerminalErrors       int64                        `json:"terminal_errors"`
	OperationQueries     int64                        `json:"operation_result_queries"`
	ProcessingQueries    int64                        `json:"operation_result_processing"`
	UnavailableQueries   int64                        `json:"operation_result_unavailable_retried"`
	SameOperationRetry   int64                        `json:"same_operation_retries"`
	RetryErrors          int64                        `json:"same_operation_retry_errors"`
	AdmissionP50Ms       float64                      `json:"theft_admission_p50_ms"`
	AdmissionP95Ms       float64                      `json:"theft_admission_p95_ms"`
	AdmissionP99Ms       float64                      `json:"theft_admission_p99_ms"`
	TerminalP50Ms        float64                      `json:"theft_terminal_p50_ms"`
	TerminalP95Ms        float64                      `json:"theft_terminal_p95_ms"`
	TerminalP99Ms        float64                      `json:"theft_terminal_p99_ms"`
	FarmChangeFrames     int64                        `json:"farm_change_frames"`
	RejoinRequired       int64                        `json:"rejoin_required"`
	VersionGaps          int64                        `json:"farm_version_gaps"`
	VersionRegressions   int64                        `json:"farm_version_regressions"`
	ExpectedChanges      int                          `json:"expected_changes_per_healthy_connection"`
	HealthyVersions      map[string]theftVersionAudit `json:"healthy_connection_versions"`
	Fixture              theftFixture                 `json:"fixture"`
}

func newTheftHotStats(expected int, owner uint64) *theftHotStats {
	result := &theftHotStats{
		expected: expected, owner: owner,
		readyPlayers: make(map[uint64]struct{}, expected),
		healthyDone:  make(map[uint64]struct{}, expected-1),
		versions:     make(map[uint64]theftVersionAudit, expected-1),
		start:        make(chan struct{}), done: make(chan struct{}), abort: make(chan struct{}), fixtureReady: make(chan struct{}),
	}
	if !*theftPrepare {
		result.fixture = theftFixture{PlotID: uint32(*theftPlot), CropCycleID: *theftCycle, PreparedThrough: "caller_supplied_mature_fixture"}
		result.fixtureReadyAt = time.Now()
		close(result.fixtureReady)
	}
	return result
}

func (s *theftHotStats) publishFixture(fixture theftFixture) {
	s.fixtureOnce.Do(func() {
		s.mu.Lock()
		s.fixture = fixture
		s.fixtureReadyAt = time.Now()
		s.mu.Unlock()
		close(s.fixtureReady)
	})
}

func (s *theftHotStats) awaitFixture(ctx context.Context) (theftFixture, error) {
	select {
	case <-s.fixtureReady:
		s.mu.Lock()
		fixture := s.fixture
		s.mu.Unlock()
		if fixture.CropCycleID == "" {
			return theftFixture{}, errors.New("theft fixture has no crop cycle")
		}
		return fixture, nil
	case <-s.abort:
		return theftFixture{}, errors.New("theft fixture aborted")
	case <-ctx.Done():
		return theftFixture{}, ctx.Err()
	}
}

func (s *theftHotStats) markReady(playerID uint64) {
	s.mu.Lock()
	s.readyPlayers[playerID] = struct{}{}
	ready := len(s.readyPlayers)
	if ready == s.expected && !s.fixtureReadyAt.IsZero() {
		s.barrierElapsed = time.Since(s.fixtureReadyAt)
	}
	s.mu.Unlock()
	if ready == s.expected {
		s.startOnce.Do(func() { close(s.start) })
	}
}

func (s *theftHotStats) markHealthyDone(playerID uint64, audit theftVersionAudit) {
	s.mu.Lock()
	s.healthyDone[playerID] = struct{}{}
	s.versions[playerID] = audit
	done := len(s.healthyDone)
	s.mu.Unlock()
	if done == s.expected-1 {
		s.doneOnce.Do(func() { close(s.done) })
	}
}

func (s *theftHotStats) recordAdmission(elapsed time.Duration) {
	s.mu.Lock()
	s.admissionTimes = append(s.admissionTimes, elapsed)
	s.mu.Unlock()
}

func (s *theftHotStats) recordTerminal(elapsed time.Duration) {
	s.mu.Lock()
	s.terminalTimes = append(s.terminalTimes, elapsed)
	s.mu.Unlock()
}

func (s *theftHotStats) fail() { s.abortOnce.Do(func() { close(s.abort) }) }

func (s *theftHotStats) setSlowOutcome(outcome string) {
	s.slowMu.Lock()
	s.slowOutcome = outcome
	s.slowMu.Unlock()
}

func (s *theftHotStats) snapshot() *theftHotReport {
	s.mu.Lock()
	ready, healthy := len(s.readyPlayers), len(s.healthyDone)
	admissions := append([]time.Duration(nil), s.admissionTimes...)
	times := append([]time.Duration(nil), s.terminalTimes...)
	versions := make(map[string]theftVersionAudit, len(s.versions))
	for playerID, audit := range s.versions {
		versions[strconv.FormatUint(playerID, 10)] = audit
	}
	fixture := s.fixture
	barrierElapsed := s.barrierElapsed
	s.mu.Unlock()
	s.slowMu.Lock()
	slowOutcome := s.slowOutcome
	s.slowMu.Unlock()
	percentile := func(values []time.Duration, q float64) float64 {
		if len(values) == 0 {
			return 0
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return float64(values[int(float64(len(values)-1)*q)]) / float64(time.Millisecond)
	}
	barrierQPS := float64(0)
	if barrierElapsed > 0 {
		barrierQPS = float64(s.expected) / barrierElapsed.Seconds()
	}
	report := &theftHotReport{
		OwnerPlayerID: s.owner, ReadyBarrier: ready,
		ReadyBarrierMs: float64(barrierElapsed) / float64(time.Millisecond), ReadyBarrierQPS: barrierQPS,
		HealthyCompleted: healthy,
		BurstVisitors:    s.expected - 1, SlowReaderEnabled: *theftSlow,
		ImmediateAccepted: s.immediateAccepted.Load(), ImmediateProcessing: s.immediateProcess.Load(),
		ImmediateSucceeded: s.immediateSuccess.Load(), ImmediateNoChange: s.immediateNoChange.Load(),
		ImmediateRejected: s.immediateRejected.Load(), ImmediateUnavailable: s.immediateUnavailable.Load(),
		ImmediateErrors:   s.immediateError.Load(),
		TerminalSucceeded: s.terminalSuccess.Load(), TerminalNoChange: s.terminalNoChange.Load(),
		TerminalRejected: s.terminalRejected.Load(), TerminalErrors: s.terminalError.Load(),
		OperationQueries: s.queries.Load(), ProcessingQueries: s.processingQueries.Load(),
		UnavailableQueries: s.unavailableQueries.Load(),
		SameOperationRetry: s.retries.Load(), RetryErrors: s.retryErrors.Load(),
		AdmissionP50Ms: percentile(admissions, .50), AdmissionP95Ms: percentile(admissions, .95), AdmissionP99Ms: percentile(admissions, .99),
		TerminalP50Ms: percentile(times, .50), TerminalP95Ms: percentile(times, .95), TerminalP99Ms: percentile(times, .99),
		FarmChangeFrames: s.changeFrames.Load(), RejoinRequired: s.rejoin.Load(),
		VersionGaps: s.versionGap.Load(), VersionRegressions: s.versionRegression.Load(),
		ExpectedChanges: *theftChanges, HealthyVersions: versions, Fixture: fixture,
		SlowReaderOutcome: slowOutcome,
	}
	if *theftSlow {
		report.SlowReaderPlayerID = s.owner
	}
	return report
}

func runTheftHotPlayer(ctx context.Context, playerID uint64, runID string, secret []byte, s *stats) {
	login, err := acquireTicket(ctx, playerID, secret, runID)
	if err != nil {
		s.loginErr.Add(1)
		s.recordFailure("theft_login", err)
		s.theft.fail()
		return
	}
	dialer, err := websocketDialer()
	if err != nil {
		s.connectErr.Add(1)
		s.recordFailure("theft_connect_config", err)
		s.theft.fail()
		return
	}
	if *theftSlow && playerID == *sharedFarm {
		dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, dialErr := (&net.Dialer{}).DialContext(ctx, network, address)
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(1024)
			}
			return conn, dialErr
		}
	}
	conn, response, err := dialer.DialContext(ctx, *wsURL, nil)
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		s.connectErr.Add(1)
		s.recordFailure("theft_connect", err)
		s.theft.fail()
		return
	}
	defer conn.Close()
	active := s.active.Add(1)
	s.observeActive(active)
	defer s.active.Add(-1)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if err := writeFrame(conn, 1, frameKindClientHello, &commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "ws-stress/theft-hot"}); err != nil {
		s.helloErr.Add(1)
		s.recordFailure("theft_hello_write", err)
		s.theft.fail()
		return
	}
	if err := readUntil(conn, frameKindServerHello); err != nil {
		s.helloErr.Add(1)
		s.recordFailure("theft_hello_read", err)
		s.theft.fail()
		return
	}

	// Establish RecoveryComplete through the only public legal path: Join the
	// authenticated player's own farm, observe Ready, then send the marker.
	selfJoinStart := time.Now()
	selfJoin := fmt.Sprintf("theft-self-join-%s-%d", runID, playerID)
	if err := writeJoin(conn, 2, selfJoin, playerID); err != nil {
		s.joinErr.Add(1)
		s.recordFailure("theft_self_join_write", err)
		s.theft.fail()
		return
	}
	selfSnapshot, err := readUntilSnapshot(conn, 3, selfJoin, playerID, true)
	if err != nil {
		s.joinErr.Add(1)
		s.recordFailure("theft_self_join_ready", err)
		s.theft.fail()
		return
	}
	selfJoinLatency := time.Since(selfJoinStart)
	sequence := uint64(3)
	if *theftPrepare && playerID == *sharedFarm {
		fixture, prepareErr := prepareTheftCrop(conn, &sequence, playerID, runID, selfSnapshot)
		if prepareErr != nil {
			s.scenarioErr.Add(1)
			s.recordFailure("theft_prepare_crop", prepareErr)
			s.theft.fail()
			return
		}
		s.theft.publishFixture(fixture)
	}
	fixture, err := s.theft.awaitFixture(ctx)
	if err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_fixture", err)
		return
	}

	var hotSnapshot *farmpb.FarmSnapshot
	if playerID == *sharedFarm {
		// The owner is already subscribed to the hot farm: the self Join above
		// established its public Snapshot/Ready barrier and recovery marker.
		// Rejoining that same farm would replace a live subscription. Keep it
		// active and start the theft-version audit at the public Plant commit.
		baseline := selfSnapshot.GetFarmVersion()
		if fixture.CommittedFarmVersion > baseline {
			baseline = fixture.CommittedFarmVersion
		}
		hotSnapshot = &farmpb.FarmSnapshot{FarmVersion: baseline}
		s.recordReadyLatency(selfJoinLatency)
	} else {
		hotJoinStart := time.Now()
		hotJoin := fmt.Sprintf("theft-hot-join-%s-%d", runID, playerID)
		sequence++
		if err := writeJoin(conn, sequence, hotJoin, *sharedFarm); err != nil {
			s.joinErr.Add(1)
			s.recordFailure("theft_hot_join_write", err)
			s.theft.fail()
			return
		}
		hotSnapshot, err = readUntilSnapshot(conn, sequence+1, hotJoin, *sharedFarm, false)
		if err != nil {
			s.joinErr.Add(1)
			s.recordFailure("theft_hot_join_ready", err)
			s.theft.fail()
			return
		}
		s.recordReady(hotJoinStart)
	}
	s.theft.markReady(playerID)

	select {
	case <-s.theft.start:
	case <-s.theft.abort:
		return
	case <-ctx.Done():
		return
	}

	if playerID == *sharedFarm {
		if !*theftSlow {
			// The owner is healthy in baseline mode but sends no THEFT. It still
			// audits every committed FarmChange exactly like the visitors.
			audit := theftVersionAudit{Baseline: hotSnapshot.GetFarmVersion(), Last: hotSnapshot.GetFarmVersion()}
			if err := collectExpectedChanges(conn, &audit, s.theft, time.Now().Add(*theftTimeout)); err != nil {
				s.scenarioErr.Add(1)
				s.recordFailure("theft_owner_broadcast", err)
				s.theft.fail()
				return
			}
			s.theft.markHealthyDone(playerID, audit)
			return
		}
		// No ReadMessage call occurs between Ready and completion of all 199
		// healthy visitors. The tiny kernel receive buffer makes this a real slow
		// socket without inserting sleeps in any production process.
		select {
		case <-s.theft.done:
		case <-s.theft.abort:
		case <-ctx.Done():
		}
		_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		_, _, readErr := conn.ReadMessage()
		switch {
		case readErr == nil:
			s.theft.setSlowOutcome("still_connected_with_buffered_downlink")
		case errors.Is(readErr, net.ErrClosed):
			s.theft.setSlowOutcome("connection_closed")
		default:
			var closeErr *websocket.CloseError
			if errors.As(readErr, &closeErr) {
				s.theft.setSlowOutcome(fmt.Sprintf("websocket_close_%d", closeErr.Code))
			} else if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
				s.theft.setSlowOutcome("still_connected_no_readable_frame")
			} else {
				s.theft.setSlowOutcome("read_error:" + stableFailureReason(readErr))
			}
		}
		return
	}

	s.scenarioTried.Add(1)
	burstStart := time.Now()
	audit := theftVersionAudit{Baseline: hotSnapshot.GetFarmVersion(), Last: hotSnapshot.GetFarmVersion()}
	operationID := fmt.Sprintf("theft-hot-%s-%d", runID, playerID)
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.TheftRequest{
		PlotId: fixture.PlotID, TargetFarmId: *sharedFarm, CropCycleId: fixture.CropCycleID,
	})
	if err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_encode", err)
		s.theft.fail()
		return
	}
	sequence++
	if err := writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: "request-" + operationID,
		CommandType: commonpb.CommandType_THEFT, Payload: payload,
	}); err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_write", err)
		s.theft.fail()
		return
	}
	immediate, err := readCommandResultWithChanges(conn, operationID, &audit, s.theft, time.Now().Add(*theftTimeout))
	if err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_immediate", err)
		s.theft.immediateError.Add(1)
		s.theft.fail()
		return
	}
	s.theft.observeImmediate(immediate.GetStatus())
	sameOperationRetried := false
	if immediate.GetStatus() == commonpb.CommandStatus_UNAVAILABLE && *theftRetry {
		sequence++
		s.theft.retries.Add(1)
		sameOperationRetried = true
		if err := writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
			OperationId: operationID, RequestId: "admission-retry-" + operationID,
			CommandType: commonpb.CommandType_THEFT, Payload: payload,
		}); err != nil {
			s.scenarioErr.Add(1)
			s.recordFailure("theft_admission_retry_write", err)
			s.theft.retryErrors.Add(1)
			s.theft.fail()
			return
		}
		if _, err := readCommandResultWithChanges(conn, operationID, &audit, s.theft, time.Now().Add(*theftTimeout)); err != nil {
			s.scenarioErr.Add(1)
			s.recordFailure("theft_admission_retry_result", err)
			s.theft.retryErrors.Add(1)
			s.theft.fail()
			return
		}
	}
	s.theft.recordAdmission(time.Since(burstStart))
	// Always prove the stored, terminal result through the public lookup path,
	// even when the first response happens to be terminal already.
	terminal, sequence, err := pollOperationTerminal(conn, sequence, commonpb.CommandType_THEFT, operationID,
		*sharedFarm, &audit, s.theft, time.Now().Add(*theftTimeout))
	if err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_terminal", err)
		s.theft.terminalError.Add(1)
		s.theft.fail()
		return
	}
	s.theft.observeTerminal(terminal.GetStatus())
	s.theft.recordTerminal(time.Since(burstStart))

	if *theftRetry && !sameOperationRetried {
		sequence++
		s.theft.retries.Add(1)
		if err := writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
			OperationId: operationID, RequestId: "retry-" + operationID,
			CommandType: commonpb.CommandType_THEFT, Payload: payload,
		}); err != nil {
			s.theft.retryErrors.Add(1)
		} else if _, retryErr := readCommandResultWithChanges(conn, operationID, &audit, s.theft, time.Now().Add(*theftTimeout)); retryErr != nil {
			s.theft.retryErrors.Add(1)
		}
	}
	if err := collectExpectedChanges(conn, &audit, s.theft, time.Now().Add(*theftTimeout)); err != nil {
		s.scenarioErr.Add(1)
		s.recordFailure("theft_broadcast", err)
		s.theft.fail()
		return
	}
	s.recordScenario(burstStart)
	s.theft.markHealthyDone(playerID, audit)
}

func prepareTheftCrop(conn *websocket.Conn, sequence *uint64, playerID uint64, runID string, snapshot *farmpb.FarmSnapshot) (theftFixture, error) {
	var plotID uint32
	var plotVersion uint64
	found := false
	for _, plot := range snapshot.GetPlots() {
		if plot != nil && plot.GetCropCycleId() == "" {
			plotID = plot.GetPlotIndex()
			plotVersion = plot.GetPlotVersion()
			found = true
			break
		}
	}
	if !found {
		return theftFixture{}, errors.New("theft fixture requires an empty plot")
	}

	buyPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	if err != nil {
		return theftFixture{}, err
	}
	buyOperation := fmt.Sprintf("theft-fixture-buy-%s-%d", runID, playerID)
	*sequence++
	if err := writeFrame(conn, *sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: buyOperation, RequestId: "request-" + buyOperation,
		CommandType: commonpb.CommandType_BUY, Payload: buyPayload,
	}); err != nil {
		return theftFixture{}, err
	}
	buyResult, err := readCommandResultWithChanges(conn, buyOperation, nil, nil, time.Now().Add(*theftTimeout))
	if err != nil {
		return theftFixture{}, fmt.Errorf("buy seed result: %w", err)
	}
	if buyResult.GetStatus() != commonpb.CommandStatus_SUCCEEDED && buyResult.GetStatus() != commonpb.CommandStatus_NO_CHANGE {
		return theftFixture{}, fmt.Errorf("buy seed status=%s code=%s message=%s", buyResult.GetStatus(), buyResult.GetError().GetErrorCode(), buyResult.GetError().GetErrorMessage())
	}

	plantPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.PlantRequest{
		PlotId: plotID, CropDefinitionId: 1, TargetFarmId: playerID,
		ExpectedPlotVersion: plotVersion, ExpectedCropRuleVersion: 1,
	})
	if err != nil {
		return theftFixture{}, err
	}
	plantOperation := fmt.Sprintf("theft-fixture-plant-%s-%d", runID, playerID)
	*sequence++
	if err := writeFrame(conn, *sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: plantOperation, RequestId: "request-" + plantOperation,
		CommandType: commonpb.CommandType_PLANT, Payload: plantPayload,
	}); err != nil {
		return theftFixture{}, err
	}
	immediate, err := readCommandResultWithChanges(conn, plantOperation, nil, nil, time.Now().Add(*theftTimeout))
	if err != nil {
		return theftFixture{}, fmt.Errorf("plant immediate result: %w", err)
	}
	if immediate.GetStatus() != commonpb.CommandStatus_ACCEPTED && immediate.GetStatus() != commonpb.CommandStatus_PROCESSING &&
		immediate.GetStatus() != commonpb.CommandStatus_SUCCEEDED && immediate.GetStatus() != commonpb.CommandStatus_NO_CHANGE {
		return theftFixture{}, fmt.Errorf("plant immediate status=%s code=%s message=%s", immediate.GetStatus(), immediate.GetError().GetErrorCode(), immediate.GetError().GetErrorMessage())
	}
	terminal, nextSequence, err := pollOperationTerminal(conn, *sequence, commonpb.CommandType_PLANT, plantOperation,
		playerID, nil, nil, time.Now().Add(*theftTimeout))
	*sequence = nextSequence
	if err != nil {
		return theftFixture{}, fmt.Errorf("plant terminal result: %w", err)
	}
	if terminal.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return theftFixture{}, fmt.Errorf("plant terminal status=%s code=%s message=%s", terminal.GetStatus(), terminal.GetError().GetErrorCode(), terminal.GetError().GetErrorMessage())
	}
	plantResult := &publicpb.PlantResult{}
	if err := proto.Unmarshal(terminal.GetResultPayload(), plantResult); err != nil {
		return theftFixture{}, fmt.Errorf("decode PlantResult: %w", err)
	}
	if plantResult.GetTargetFarmId() != playerID || plantResult.GetPlotId() != plotID || plantResult.GetCropCycleId() == "" {
		return theftFixture{}, fmt.Errorf("invalid PlantResult farm=%d plot=%d cycle=%q", plantResult.GetTargetFarmId(), plantResult.GetPlotId(), plantResult.GetCropCycleId())
	}
	if *theftMaturity > 0 {
		time.Sleep(*theftMaturity)
	}
	return theftFixture{PlotID: plotID, CropCycleID: plantResult.GetCropCycleId(), CommittedFarmVersion: plantResult.GetCommittedFarmVersion(), PreparedThrough: "public_buy_plant_operation_result"}, nil
}

func writeJoin(conn *websocket.Conn, sequence uint64, operationID string, farmID uint64) error {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.JoinFarmRequest{TargetFarmId: farmID})
	if err != nil {
		return err
	}
	return writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: "request-" + operationID,
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: payload,
	})
}

func (s *theftHotStats) observeImmediate(status commonpb.CommandStatus) {
	switch status {
	case commonpb.CommandStatus_ACCEPTED:
		s.immediateAccepted.Add(1)
	case commonpb.CommandStatus_PROCESSING:
		s.immediateProcess.Add(1)
	case commonpb.CommandStatus_SUCCEEDED:
		s.immediateSuccess.Add(1)
	case commonpb.CommandStatus_NO_CHANGE:
		s.immediateNoChange.Add(1)
	case commonpb.CommandStatus_REJECTED:
		s.immediateRejected.Add(1)
	case commonpb.CommandStatus_UNAVAILABLE:
		s.immediateUnavailable.Add(1)
	default:
		s.immediateError.Add(1)
	}
}

func (s *theftHotStats) observeTerminal(status commonpb.CommandStatus) {
	switch status {
	case commonpb.CommandStatus_SUCCEEDED:
		s.terminalSuccess.Add(1)
	case commonpb.CommandStatus_NO_CHANGE:
		s.terminalNoChange.Add(1)
	case commonpb.CommandStatus_REJECTED:
		s.terminalRejected.Add(1)
	default:
		s.terminalError.Add(1)
	}
}

func pollOperationTerminal(conn *websocket.Conn, sequence uint64, originalType commonpb.CommandType, operationID string, targetFarm uint64, audit *theftVersionAudit, stats *theftHotStats, deadline time.Time) (*publicpb.StoredOperationOutcome, uint64, error) {
	for time.Now().Before(deadline) {
		sequence++
		queryID := fmt.Sprintf("query-%s-%d", operationID, sequence)
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.GetOperationResultRequest{
			OriginalCommandType: originalType, OriginalOperationId: operationID,
			OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: targetFarm}},
		})
		if err != nil {
			return nil, sequence, err
		}
		if stats != nil {
			stats.queries.Add(1)
		}
		if err := writeFrame(conn, sequence, frameKindClientCommand, &commonpb.ClientCommandEnvelope{
			OperationId: queryID, RequestId: "request-" + queryID,
			CommandType: commonpb.CommandType_GET_OPERATION_RESULT, Payload: payload,
		}); err != nil {
			return nil, sequence, err
		}
		result, err := readCommandResultWithChanges(conn, queryID, audit, stats, deadline)
		if err != nil {
			return nil, sequence, err
		}
		if result.GetStatus() == commonpb.CommandStatus_UNAVAILABLE {
			if stats != nil {
				stats.unavailableQueries.Add(1)
			}
			time.Sleep(*theftPoll)
			continue
		}
		if result.GetStatus() == commonpb.CommandStatus_PROCESSING {
			if stats != nil {
				stats.processingQueries.Add(1)
			}
			time.Sleep(*theftPoll)
			continue
		}
		if result.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			return nil, sequence, fmt.Errorf("OperationResult command status=%s code=%s message=%s", result.GetStatus(), result.GetError().GetErrorCode(), result.GetError().GetErrorMessage())
		}
		lookup := &publicpb.GetOperationResultResult{}
		if err := proto.Unmarshal(result.GetPayload(), lookup); err != nil {
			return nil, sequence, err
		}
		switch lookup.GetLookupStatus() {
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING:
			if stats != nil {
				stats.processingQueries.Add(1)
			}
			time.Sleep(*theftPoll)
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED:
			if lookup.GetCompletedOutcome() == nil {
				return nil, sequence, errors.New("completed OperationResult has no outcome")
			}
			return lookup.GetCompletedOutcome(), sequence, nil
		case publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND:
			time.Sleep(*theftPoll)
		default:
			return nil, sequence, fmt.Errorf("OperationResult lookup status=%s", lookup.GetLookupStatus())
		}
	}
	return nil, sequence, context.DeadlineExceeded
}

func readCommandResultWithChanges(conn *websocket.Conn, operationID string, audit *theftVersionAudit, stats *theftHotStats, deadline time.Time) (*commonpb.CommandResultEnvelope, error) {
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		kind, body, err := readFrame(conn)
		if err != nil {
			return nil, err
		}
		switch kind {
		case frameKindCommandResult:
			result := &commonpb.CommandResultEnvelope{}
			if err := proto.Unmarshal(body, result); err != nil {
				return nil, err
			}
			if result.GetOperationId() == operationID {
				return result, nil
			}
		case frameKindServerPush:
			if audit != nil && stats != nil {
				if err := observeTheftPush(body, audit, stats); err != nil {
					return nil, err
				}
			}
		case frameKindConnectionError:
			return nil, errors.New("server returned ConnectionError")
		}
	}
	return nil, context.DeadlineExceeded
}

func collectExpectedChanges(conn *websocket.Conn, audit *theftVersionAudit, stats *theftHotStats, deadline time.Time) error {
	for len(audit.Versions) < *theftChanges && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		kind, body, err := readFrame(conn)
		if err != nil {
			return err
		}
		if kind == frameKindServerPush {
			if err := observeTheftPush(body, audit, stats); err != nil {
				return err
			}
		} else if kind == frameKindConnectionError {
			return errors.New("server returned ConnectionError while collecting FarmChange")
		}
	}
	if len(audit.Versions) != *theftChanges {
		return fmt.Errorf("FarmChange count=%d want=%d baseline=%d last=%d", len(audit.Versions), *theftChanges, audit.Baseline, audit.Last)
	}
	return nil
}

func observeTheftPush(body []byte, audit *theftVersionAudit, stats *theftHotStats) error {
	push := &commonpb.ServerPushEnvelope{}
	if err := proto.Unmarshal(body, push); err != nil {
		return err
	}
	switch push.GetPushType() {
	case commonpb.PushType_FARM_CHANGE:
		changePush := &farmpb.FarmChangePush{}
		if err := proto.Unmarshal(push.GetPayload(), changePush); err != nil {
			return err
		}
		version := changePush.GetChange().GetFarmVersion()
		stats.changeFrames.Add(1)
		if version <= audit.Last {
			audit.Rollback = true
			stats.versionRegression.Add(1)
			return fmt.Errorf("farm_version rollback/duplicate: last=%d got=%d", audit.Last, version)
		}
		if version != audit.Last+1 {
			audit.Gap = true
			stats.versionGap.Add(1)
			return fmt.Errorf("farm_version gap: last=%d got=%d", audit.Last, version)
		}
		audit.Last = version
		audit.Versions = append(audit.Versions, version)
	case commonpb.PushType_REJOIN_REQUIRED:
		stats.rejoin.Add(1)
		return errors.New("REJOIN_REQUIRED during theft-hot")
	}
	return nil
}
