package main

import (
	"context"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	scalepb "farmgame/api/gen/scaleplan"

	"google.golang.org/grpc"
)

const defaultScalePlanListLimit uint32 = 20

type scalePlanMoveFlags struct {
	moves []*scalepb.ShardMove
}

func (f *scalePlanMoveFlags) String() string { return "" }

func (f *scalePlanMoveFlags) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 5 || parts[0] == "" || parts[2] == "" || parts[3] == "" {
		return errUsage
	}
	shard, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return errUsage
	}
	ordinal, err := strconv.ParseInt(parts[4], 10, 32)
	if err != nil || ordinal < 0 {
		return errUsage
	}
	f.moves = append(f.moves, &scalepb.ShardMove{Domain: parts[0], VirtualShard: uint32(shard),
		SourceHolderId: parts[2], TargetHolderId: parts[3], TargetOrdinal: int32(ordinal)})
	return nil
}

type scalePlanWorkloadView struct {
	Realm     string `json:"realm"`
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
}

type scalePlanMoveView struct {
	Domain         string `json:"domain"`
	VirtualShard   uint32 `json:"virtual_shard"`
	SourceHolderID string `json:"source_holder_id"`
	TargetHolderID string `json:"target_holder_id"`
	TargetOrdinal  int32  `json:"target_ordinal"`
}

type scalePlanView struct {
	PlanID                  string                `json:"plan_id"`
	PayloadDigest           string                `json:"payload_digest"`
	BlockedCheckpointDigest string                `json:"blocked_checkpoint_digest,omitempty"`
	Realm                   string                `json:"realm"`
	Domain                  string                `json:"domain"`
	Operation               string                `json:"operation"`
	Workload                scalePlanWorkloadView `json:"workload"`
	ExpectedReplicas        int32                 `json:"expected_replicas"`
	DesiredReplicas         int32                 `json:"desired_replicas"`
	Moves                   []scalePlanMoveView   `json:"moves"`
	RoutePlanTTLms          int64                 `json:"route_plan_ttl_ms"`
	PlanTTLms               int64                 `json:"plan_ttl_ms"`
	RecoveryRetentionMs     int64                 `json:"recovery_retention_ms"`
	MaxRoutePlansInflight   uint32                `json:"max_route_plans_inflight"`
	MaxShardsPerWave        uint32                `json:"max_shards_per_wave"`
	Status                  string                `json:"status"`
	Phase                   string                `json:"phase"`
	WaveIndex               uint32                `json:"wave_index"`
	Attempt                 uint64                `json:"attempt"`
	Revision                uint64                `json:"revision"`
	BlockedReason           string                `json:"blocked_reason,omitempty"`
	PausedReason            string                `json:"paused_reason,omitempty"`
	ErrorCode               string                `json:"error_code,omitempty"`
	CreatedAtMs             int64                 `json:"created_at_ms"`
	UpdatedAtMs             int64                 `json:"updated_at_ms"`
	CompletedAtMs           int64                 `json:"completed_at_ms"`
	StartDeadlineMs         int64                 `json:"start_deadline_ms"`
	RecoveryRetainUntilMs   int64                 `json:"recovery_retain_until_ms"`
}

type scalePlanOutput struct {
	Command string        `json:"command"`
	Plan    scalePlanView `json:"plan"`
}

type scalePlanListOutput struct {
	Command   string          `json:"command"`
	Plans     []scalePlanView `json:"plans"`
	Truncated bool            `json:"truncated"`
}

func runScalePlanOverConn(cfg globalConfig, args []string, conn *grpc.ClientConn, output io.Writer) error {
	if conn == nil {
		return errUsage
	}
	return runScalePlanCommand(cfg, args, scalepb.NewScalePlanServiceClient(conn), output)
}

func runScalePlanCommand(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	if len(args) == 0 || client == nil {
		return errUsage
	}
	switch args[0] {
	case "get", "plan":
		return runScalePlanGet(cfg, args[1:], client, output)
	case "list":
		return runScalePlanList(cfg, args[1:], client, output)
	case "submit":
		return runScalePlanSubmit(cfg, args[1:], client, output)
	case "pause":
		return runScalePlanPause(cfg, args[1:], client, output)
	case "resume":
		return runScalePlanResume(cfg, args[1:], client, output)
	case "terminate-blocked":
		return runScalePlanTerminateBlocked(cfg, args[1:], client, output)
	default:
		return errUsage
	}
}

func runScalePlanGet(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan get")
	planID := fs.String("plan-id", "", "exact ScalePlan ID")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validScalePlanCLIIdentity(*planID) {
		return errUsage
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.GetScalePlanResponse, error) {
		return client.GetScalePlan(ctx, &scalepb.GetScalePlanRequest{PlanId: *planID})
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	plan, err := redactScalePlan(response.GetPlan())
	if err != nil || plan.PlanID != *planID {
		return errVerificationMatch
	}
	return writeJSON(output, scalePlanOutput{Command: "scaleplan get", Plan: plan})
}

func runScalePlanList(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan list")
	limit := fs.Uint("limit", uint(defaultScalePlanListLimit), "bounded number of recoverable plans")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *limit == 0 || *limit > uint(MaxRecoverableListLimitCLI) {
		return errUsage
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.ListRecoverableScalePlansResponse, error) {
		return client.ListRecoverableScalePlans(ctx, &scalepb.ListRecoverableScalePlansRequest{Limit: uint32(*limit)})
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	plans := make([]scalePlanView, 0, len(response.GetPlans()))
	for _, item := range response.GetPlans() {
		plan, redactErr := redactScalePlan(item)
		if redactErr != nil {
			return errVerificationMatch
		}
		plans = append(plans, plan)
	}
	return writeJSON(output, scalePlanListOutput{Command: "scaleplan list", Plans: plans, Truncated: response.GetTruncated()})
}

const MaxRecoverableListLimitCLI uint = 1000

func runScalePlanSubmit(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan submit")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key")
	realm := fs.String("realm", "", "exact ScalePlan realm")
	domain := fs.String("domain", "", "PLAYER or FARM")
	operation := fs.String("operation", "", "SCALE_OUT or SCALE_IN")
	namespace := fs.String("namespace", "", "StatefulSet namespace")
	kind := fs.String("kind", "StatefulSet", "workload kind")
	name := fs.String("name", "", "StatefulSet name")
	expected := fs.Int("expected-replicas", 0, "expected replica count")
	desired := fs.Int("desired-replicas", 0, "desired replica count")
	routeTTL := fs.Int64("route-plan-ttl-ms", 60_000, "RoutePlan TTL in milliseconds")
	planTTL := fs.Int64("plan-ttl-ms", 3_600_000, "ScalePlan start TTL in milliseconds")
	recoveryTTL := fs.Int64("recovery-retention-ms", 86_400_000, "recovery retention in milliseconds")
	maxInflight := fs.Uint("max-route-plans-inflight", 1, "functional in-flight limit")
	maxWave := fs.Uint("max-shards-per-wave", 1, "functional wave limit")
	var moves scalePlanMoveFlags
	fs.Var(&moves, "move", "manifest entry domain,shard,source_holder,target_holder,target_ordinal (repeatable)")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validScalePlanCLIIdentity(*idempotencyKey) || !validScalePlanCLIIdentity(*realm) ||
		!validScalePlanDomain(*domain) || !validScalePlanOperation(*operation) || !validScalePlanCLIIdentity(*namespace) ||
		!validScalePlanCLIIdentity(*kind) || !validScalePlanCLIIdentity(*name) || *expected < 1 || *desired < 1 || len(moves.moves) == 0 ||
		*maxInflight > uint(^uint32(0)) || *maxWave > uint(^uint32(0)) {
		return errUsage
	}
	request := &scalepb.SubmitScalePlanRequest{IdempotencyKey: *idempotencyKey, Realm: *realm, Domain: *domain,
		Operation: scalePlanOperation(*operation), Workload: &scalepb.WorkloadRef{Realm: *realm, Domain: *domain, Namespace: *namespace, Kind: *kind, Name: *name},
		ExpectedReplicas: int32(*expected), DesiredReplicas: int32(*desired), Moves: moves.moves, RoutePlanTtlMs: *routeTTL,
		PlanTtlMs: *planTTL, RecoveryRetentionMs: *recoveryTTL, MaxRoutePlansInflight: uint32(*maxInflight), MaxShardsPerWave: uint32(*maxWave)}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.SubmitScalePlanResponse, error) {
		return client.SubmitScalePlan(ctx, request)
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	plan, err := redactScalePlan(response.GetPlan())
	if err != nil {
		return errVerificationMatch
	}
	return writeJSON(output, scalePlanOutput{Command: "scaleplan submit", Plan: plan})
}

func runScalePlanPause(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan pause")
	planID := fs.String("plan-id", "", "exact ScalePlan ID")
	confirm := fs.String("confirm-digest", "", "exact immutable payload digest confirmation")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validScalePlanCLIIdentity(*planID) || !validScalePlanDigest(*confirm) {
		return errUsage
	}
	before, err := preflightScalePlanMutation(cfg, client, *planID, *confirm)
	if err != nil {
		return err
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.PauseResponse, error) {
		return client.Pause(ctx, &scalepb.PauseRequest{PlanId: before.GetPlanId()})
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	return writeScalePlanMutationResult("scaleplan pause", before, response.GetPlan(), err, output)
}

func runScalePlanResume(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan resume")
	planID := fs.String("plan-id", "", "exact ScalePlan ID")
	confirm := fs.String("confirm-digest", "", "exact immutable payload digest confirmation")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validScalePlanCLIIdentity(*planID) || !validScalePlanDigest(*confirm) {
		return errUsage
	}
	before, err := preflightScalePlanMutation(cfg, client, *planID, *confirm)
	if err != nil {
		return err
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.ResumeResponse, error) {
		return client.Resume(ctx, &scalepb.ResumeRequest{PlanId: before.GetPlanId()})
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	return writeScalePlanMutationResult("scaleplan resume", before, response.GetPlan(), err, output)
}

func runScalePlanTerminateBlocked(cfg globalConfig, args []string, client scalepb.ScalePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("scaleplan terminate-blocked")
	planID := fs.String("plan-id", "", "exact ScalePlan ID")
	confirm := fs.String("confirm-digest", "", "exact immutable payload digest confirmation")
	terminationID := fs.String("termination-id", "", "new explicit termination ID")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validScalePlanCLIIdentity(*planID) || !validScalePlanDigest(*confirm) || !validScalePlanCLIIdentity(*terminationID) {
		return errUsage
	}
	before, err := preflightScalePlanMutation(cfg, client, *planID, *confirm)
	if err != nil {
		return err
	}
	if before.GetStatus() != "BLOCKED_NEEDS_OPERATOR" || !validScalePlanCode(before.GetBlockedReason()) || !validScalePlanDigest(before.GetBlockedCheckpointDigest()) {
		return errVerificationMatch
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.TerminateBlockedResponse, error) {
		return client.TerminateBlocked(ctx, &scalepb.TerminateBlockedRequest{PlanId: before.GetPlanId(), ExpectedRevision: before.GetRevision(),
			ExpectedBlockedReason: before.GetBlockedReason(), ExpectedPayloadDigest: before.GetPayloadDigest(),
			ExpectedCheckpointDigest: before.GetBlockedCheckpointDigest(), TerminationId: *terminationID})
	})
	if err != nil {
		return err
	}
	if response == nil {
		return errVerificationMatch
	}
	return writeScalePlanMutationResult("scaleplan terminate-blocked", before, response.GetPlan(), err, output)
}

func preflightScalePlanMutation(cfg globalConfig, client scalepb.ScalePlanServiceClient, planID, confirm string) (*scalepb.ScalePlan, error) {
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*scalepb.GetScalePlanResponse, error) {
		return client.GetScalePlan(ctx, &scalepb.GetScalePlanRequest{PlanId: planID})
	})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errVerificationMatch
	}
	plan := response.GetPlan()
	if plan == nil || plan.GetPlanId() != planID || !validScalePlanDigest(plan.GetPayloadDigest()) {
		return nil, errVerificationMatch
	}
	if confirm != plan.GetPayloadDigest() {
		return nil, errVerificationMatch
	}
	return plan, nil
}

func writeScalePlanMutationResult(command string, before, after *scalepb.ScalePlan, rpcErr error, output io.Writer) error {
	if rpcErr != nil {
		return rpcErr
	}
	if before == nil || after == nil || after.GetPlanId() != before.GetPlanId() || after.GetPayloadDigest() != before.GetPayloadDigest() {
		return errVerificationMatch
	}
	plan, err := redactScalePlan(after)
	if err != nil {
		return errVerificationMatch
	}
	return writeJSON(output, scalePlanOutput{Command: command, Plan: plan})
}

func redactScalePlan(plan *scalepb.ScalePlan) (scalePlanView, error) {
	if plan == nil {
		return scalePlanView{}, errVerificationMatch
	}
	operation, ok := scalePlanOperationName(plan.GetOperation())
	if plan.GetPlanId() == "" || !validScalePlanDigest(plan.GetPayloadDigest()) || plan.GetWorkload() == nil ||
		!validScalePlanDomain(plan.GetDomain()) || !ok {
		return scalePlanView{}, errVerificationMatch
	}
	workload := plan.GetWorkload()
	if workload.GetRealm() == "" || workload.GetDomain() == "" || workload.GetNamespace() == "" || workload.GetKind() == "" || workload.GetName() == "" {
		return scalePlanView{}, errVerificationMatch
	}
	view := scalePlanView{PlanID: plan.GetPlanId(), PayloadDigest: plan.GetPayloadDigest(), BlockedCheckpointDigest: plan.GetBlockedCheckpointDigest(),
		Realm: plan.GetRealm(), Domain: plan.GetDomain(), Operation: operation, Workload: scalePlanWorkloadView{Realm: workload.GetRealm(), Domain: workload.GetDomain(), Namespace: workload.GetNamespace(), Kind: workload.GetKind(), Name: workload.GetName()},
		ExpectedReplicas: plan.GetExpectedReplicas(), DesiredReplicas: plan.GetDesiredReplicas(), RoutePlanTTLms: plan.GetRoutePlanTtlMs(), PlanTTLms: plan.GetPlanTtlMs(), RecoveryRetentionMs: plan.GetRecoveryRetentionMs(),
		MaxRoutePlansInflight: plan.GetMaxRoutePlansInflight(), MaxShardsPerWave: plan.GetMaxShardsPerWave(), Status: plan.GetStatus(), Phase: plan.GetPhase(), WaveIndex: plan.GetWaveIndex(), Attempt: plan.GetAttempt(), Revision: plan.GetRevision(),
		BlockedReason: plan.GetBlockedReason(), PausedReason: plan.GetPausedReason(), ErrorCode: plan.GetErrorCode(), CreatedAtMs: plan.GetCreatedAtMs(), UpdatedAtMs: plan.GetUpdatedAtMs(), CompletedAtMs: plan.GetCompletedAtMs(), StartDeadlineMs: plan.GetStartDeadlineMs(), RecoveryRetainUntilMs: plan.GetRecoveryRetainUntilMs(),
	}
	view.Moves = make([]scalePlanMoveView, 0, len(plan.GetMoves()))
	for _, move := range plan.GetMoves() {
		if move == nil || !validScalePlanDomain(move.GetDomain()) || move.GetSourceHolderId() == "" || move.GetTargetHolderId() == "" || move.GetTargetOrdinal() < 0 {
			return scalePlanView{}, errVerificationMatch
		}
		view.Moves = append(view.Moves, scalePlanMoveView{Domain: move.GetDomain(), VirtualShard: move.GetVirtualShard(), SourceHolderID: move.GetSourceHolderId(), TargetHolderID: move.GetTargetHolderId(), TargetOrdinal: move.GetTargetOrdinal()})
	}
	if view.BlockedCheckpointDigest != "" && !validScalePlanDigest(view.BlockedCheckpointDigest) {
		return scalePlanView{}, errVerificationMatch
	}
	return view, nil
}

func scalePlanOperationName(value scalepb.ScalePlanOperation) (string, bool) {
	switch value {
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT:
		return "SCALE_OUT", true
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_IN:
		return "SCALE_IN", true
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_DRAIN_HOLDER:
		return "DRAIN_HOLDER", true
	default:
		return "", false
	}
}

func scalePlanOperation(value string) scalepb.ScalePlanOperation {
	if value == "SCALE_OUT" {
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT
	}
	if value == "SCALE_IN" {
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_IN
	}
	return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_UNSPECIFIED
}

func validScalePlanOperation(value string) bool {
	return value == "SCALE_OUT" || value == "SCALE_IN"
}

func validScalePlanDomain(value string) bool { return value == "PLAYER" || value == "FARM" }

func validScalePlanCLIIdentity(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\t")
}

func validScalePlanDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func validScalePlanCode(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
}
