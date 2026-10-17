package main

import (
	"context"
	"fmt"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/gateway/session"
	"farmgame/internal/logic/downlink"

	"github.com/redis/go-redis/v9"
)

var (
	friendCounts    = []int{0, 100, 300}
	onlineRatiosPct = []int{0, 5, 20, 50}
	dispatchBatches = []int{1, 10, 100, 300}
	routeStateModes = []routeState{routeStateActive, routeStateMissing, routeStateExpired, routeStateMalformed}
)

type probeDeps struct {
	client              *redis.Client
	hook                *redisMetricHook
	authority           *session.RedisAuthority
	tracker             *exactKeyTracker
	runID               string
	ownershipGeneration uint64
}

func newProbeDeps(client *redis.Client, hook *redisMetricHook, runID string) *probeDeps {
	authority := session.NewRedis(client, session.Config{
		Secret: []byte("session-route-c2-probe-secret"), AuthorityEpoch: 1, EpochCacheTTL: 0,
	})
	return &probeDeps{
		client: client, hook: hook, authority: authority, tracker: newExactKeyTracker(),
		runID: runID, ownershipGeneration: ownershipGenerationFor(runID),
	}
}

func matrixPlans(mode string, iterations int, runID string) []fixturePlan {
	if iterations <= 0 {
		iterations = 1
	}
	var plans []fixturePlan
	cellIndex := 0
	appendPlan := func(plan fixturePlan) {
		plan.RunID = runID
		plan.CellIndex = cellIndex
		plan.PlayerIDBase = deterministicPlayerIDBase(runID, cellIndex, 0)
		plans = append(plans, plan)
		cellIndex++
	}

	for _, friends := range friendCounts {
		ratios := onlineRatiosPct
		if friends == 0 {
			ratios = []int{0}
		}
		for _, ratio := range ratios {
			for _, batch := range dispatchBatches {
				if mode == "smoke" {
					if friends != 0 && friends != 100 && friends != 300 {
						continue
					}
					if friends > 0 && ratio != 0 && ratio != 50 {
						continue
					}
					if batch != 1 && batch != 100 && batch != 300 {
						continue
					}
				}
				appendPlan(fixturePlan{
					FriendCount: friends, OnlineRatioPct: ratio, DispatchBatch: batch,
					RouteStateMode: routeStateActive, Iterations: iterations,
					CellName: fmt.Sprintf("friends=%d,online=%d%%,batch=%d,state=ACTIVE", friends, ratio, batch),
				})
			}
		}
	}

	for _, state := range routeStateModes {
		if state == routeStateActive {
			continue
		}
		friends := 100
		if mode == "smoke" {
			friends = 20
		}
		appendPlan(fixturePlan{
			FriendCount: friends, OnlineRatioPct: 0, DispatchBatch: 10,
			RouteStateMode: state, Iterations: 1,
			CellName: fmt.Sprintf("friends=%d,batch=10,state=%s", friends, state),
		})
	}
	return plans
}

func runCell(ctx context.Context, deps *probeDeps, plan fixturePlan) (cellEvidence, error) {
	mode := plan.RouteStateMode
	if mode == "" {
		mode = routeStateActive
	}
	malformedUnavailable := unavailableUint64(
		"production SessionRoute API collapses missing/expired/malformed; observed malformed unavailable")
	evidence := cellEvidence{
		CellName: plan.CellName, FriendCount: plan.FriendCount, OnlineRatioPct: plan.OnlineRatioPct,
		DispatchBatchSize: plan.DispatchBatch, RouteStateMode: string(mode),
		Iterations: plan.Iterations, TopologyLabel: topologySingleNode, ClusterVerification: topologyClusterNV,
		ObservedMalformed: malformedUnavailable,
		MalformedCount:    malformedUnavailable,
		RedisCPUUS:        unavailableFloat("INFO cpu not collected in C2.1 probe"),
		Slowlog:           unavailableFloat("SLOWLOG intentionally unused; RESET forbidden"),
	}

	template := buildFixtureCard(plan)
	evidence.ExpectedActive = len(template.ExpectedActive)
	evidence.ExpectedFiltered = len(template.ExpectedMissing) + len(template.ExpectedExpired)
	evidence.ExpectedMalformed = len(template.ExpectedMalformed)

	var latencies []time.Duration
	var allBatchSizes []int
	var totalSuccessful, totalFiltered, totalEnqueued, totalDelivered int
	var redisCommands, pipelineItems, reqBytes, respBytes uint64
	var notes []string
	var violations []string

	for iter := 0; iter < plan.Iterations; iter++ {
		iterPlan := plan
		iterPlan.PlayerIDBase = deterministicPlayerIDBase(plan.RunID, plan.CellIndex, iter)
		card := buildFixtureCard(iterPlan)

		index := friend.NewMemoryFriendIndexRepository()
		if err := installFriendIndex(ctx, index, card); err != nil {
			return evidence, err
		}
		if err := installSessionRoutes(
			ctx, deps.client, deps.authority, deps.tracker,
			deps.runID, deps.ownershipGeneration, card, mode,
		); err != nil {
			return evidence, err
		}

		gate := newGateCapture()
		config := downlink.DefaultDispatcherConfig()
		config.BatchSize = plan.DispatchBatch
		config.RPCTimeout = 2 * time.Second
		config.LaneQueueSize = 4096
		dispatcher := downlink.NewDispatcherWithConfig(gate, config)
		transport, err := friend.NewDirectOpportunityHintTransport(index, deps.authority, dispatcher)
		if err != nil {
			dispatcher.Close()
			return evidence, err
		}

		before := deps.hook.snapshot()
		start := time.Now()
		if err := transport.PublishTargetedHint(ctx, card.OwnerID, card.OwnerID, 1, uint64(iter+1)); err != nil {
			dispatcher.Close()
			return evidence, err
		}
		stats := transport.Stats()
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		waitErr := gate.waitDelivered(waitCtx, int(stats.Enqueued))
		cancel()
		latency := time.Since(start)
		dispatcher.Close()
		if waitErr != nil {
			violations = append(violations, fmt.Sprintf("dispatcher wait: %v", waitErr))
		}
		delta := diffMetrics(deps.hook.snapshot(), before)

		latencies = append(latencies, latency)
		batchSizes, delivered, _ := gate.snapshot()
		allBatchSizes = append(allBatchSizes, batchSizes...)
		totalDelivered += delivered
		totalEnqueued += int(stats.Enqueued)

		successful := int(stats.Routes)
		// MissingRoutes collapses missing/expired/malformed; do not invent observed malformed.
		filtered := int(stats.MissingRoutes)
		totalSuccessful += successful
		totalFiltered += filtered

		redisCommands += delta.Commands
		pipelineItems += delta.Items
		reqBytes += delta.WriteBytes
		respBytes += delta.ReadBytes

		wantActive := len(card.ExpectedActive)
		wantFiltered := len(card.ExpectedMissing) + len(card.ExpectedExpired)
		wantMalformed := len(card.ExpectedMalformed)
		wantCollapsed := wantFiltered + wantMalformed
		if successful != wantActive {
			violations = append(violations, fmt.Sprintf("iter=%d active got=%d want=%d", iter, successful, wantActive))
		}
		if filtered != wantCollapsed {
			violations = append(violations, fmt.Sprintf(
				"iter=%d missing_routes got=%d want=%d (expected_filtered=%d expected_malformed=%d)",
				iter, filtered, wantCollapsed, wantFiltered, wantMalformed))
		}
		if int(stats.Enqueued) != wantActive {
			violations = append(violations, fmt.Sprintf("iter=%d enqueued got=%d want=%d", iter, stats.Enqueued, wantActive))
		}
		if delivered != int(stats.Enqueued) {
			violations = append(violations, fmt.Sprintf("iter=%d delivered got=%d want=%d", iter, delivered, stats.Enqueued))
		}
		sumBatches := 0
		for _, size := range batchSizes {
			sumBatches += size
			if size <= 0 || size > plan.DispatchBatch {
				violations = append(violations, fmt.Sprintf(
					"iter=%d rpc batch size %d outside (0,%d]", iter, size, plan.DispatchBatch))
			}
		}
		if sumBatches != delivered {
			violations = append(violations, fmt.Sprintf("iter=%d batch_sum=%d delivered=%d", iter, sumBatches, delivered))
		}

		if len(card.FriendIDs) == 0 {
			notes = append(notes, "friend_count=0 short-circuits before BatchGetActiveRoutes")
			continue
		}
		// Fail-closed post-validation is separate from observed malformed classification.
		routes, routeErr := deps.authority.BatchGetActiveRoutes(ctx, card.FriendIDs)
		if routeErr != nil {
			violations = append(violations, fmt.Sprintf("iter=%d BatchGetActiveRoutes: %v", iter, routeErr))
			continue
		}
		if len(routes) != wantActive {
			violations = append(violations, fmt.Sprintf("iter=%d resolver active=%d want=%d", iter, len(routes), wantActive))
		}
		for _, playerID := range card.ExpectedActive {
			if _, ok := routes[playerID]; !ok {
				violations = append(violations, fmt.Sprintf("iter=%d missing ACTIVE player=%d", iter, playerID))
			}
		}
		closed := append(append(append([]uint64{}, card.ExpectedMissing...), card.ExpectedExpired...), card.ExpectedMalformed...)
		for _, playerID := range closed {
			if _, ok := routes[playerID]; ok {
				violations = append(violations, fmt.Sprintf(
					"iter=%d fail-closed violated player=%d mode=%s", iter, playerID, mode))
			}
		}
	}

	evidence.LatencyUS = durationsUS(latencies)
	evidence.ObservedRPCBatchSizes = allBatchSizes
	evidence.RPCBatchSizePercentiles = ints(allBatchSizes)
	evidence.SuccessfulCount = totalSuccessful
	evidence.FilteredCount = totalFiltered
	evidence.ObservedActive = totalSuccessful
	evidence.ObservedFiltered = totalFiltered
	evidence.EnqueuedCount = totalEnqueued
	evidence.DeliveredCount = totalDelivered
	evidence.RedisCommands = measuredUint64(redisCommands)
	evidence.PipelineItemCount = measuredUint64(pipelineItems)
	if reqBytes == 0 && respBytes == 0 && redisCommands > 0 {
		evidence.RequestBytes = unavailableUint64("wire bytes unavailable for this Redis transport")
		evidence.ResponseBytes = unavailableUint64("wire bytes unavailable for this Redis transport")
	} else {
		evidence.RequestBytes = measuredUint64(reqBytes)
		evidence.ResponseBytes = measuredUint64(respBytes)
	}
	evidence.Violations = uniqueStrings(violations)
	evidence.Notes = uniqueStrings(notes)
	evidence.CorrectnessOK = len(evidence.Violations) == 0
	return evidence, nil
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func runProbe(ctx context.Context, deps *probeDeps, mode string, iterations int, runID string) (probeReport, error) {
	started := time.Now().UTC()
	if deps != nil && deps.runID == "" {
		deps.runID = runID
		deps.ownershipGeneration = ownershipGenerationFor(runID)
	}
	plans := matrixPlans(mode, iterations, runID)
	report := probeReport{
		SchemaVersion: evidenceSchemaVersion, Purpose: "session_route_c2_1_current_hgetall_capacity",
		RunID: runID, StartedAtUTC: started, Mode: mode,
		TopologyLabel: topologySingleNode, ClusterVerification: topologyClusterNV,
		Supports30MDAU: false,
		Notes: []string{
			"SINGLE_NODE_ONLY evidence; do not claim 30M DAU capacity from this probe",
			"Cluster pipeline-per-node verification is NOT_VERIFIED without redis.ClusterClient",
			"Measures current HGETALL SessionRoute design; OnlineRouteBucket not implemented",
			"observed_malformed unavailable: production API collapses missing/expired/malformed",
		},
	}
	report.CorrectnessOK = true
	for _, plan := range plans {
		cell, err := runCell(ctx, deps, plan)
		if err != nil {
			return withCleanup(deps, report, err)
		}
		report.Cells = append(report.Cells, cell)
		if !cell.CorrectnessOK {
			report.CorrectnessOK = false
		}
	}
	if report.CorrectnessOK {
		report.GoNoGo = "GO_FOR_MEASUREMENT_ONLY"
	} else {
		report.GoNoGo = "NO_GO"
	}
	report.CompletedAtUTC = time.Now().UTC()
	return withCleanup(deps, report, nil)
}
