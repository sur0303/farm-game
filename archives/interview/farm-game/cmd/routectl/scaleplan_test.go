package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	scalepb "farmgame/api/gen/scaleplan"

	"google.golang.org/grpc"
)

const (
	routectlScalePlanID      = "scale-plan-1"
	routectlScalePlanDigest  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	routectlCheckpointDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type fakeScalePlanClient struct {
	getResponse       *scalepb.GetScalePlanResponse
	getErr            error
	getCalls          int
	submitRequest     *scalepb.SubmitScalePlanRequest
	submitResponse    *scalepb.SubmitScalePlanResponse
	submitErr         error
	pauseRequest      *scalepb.PauseRequest
	pauseResponse     *scalepb.PauseResponse
	pauseErr          error
	resumeRequest     *scalepb.ResumeRequest
	resumeResponse    *scalepb.ResumeResponse
	resumeErr         error
	terminateRequest  *scalepb.TerminateBlockedRequest
	terminateResponse *scalepb.TerminateBlockedResponse
	terminateErr      error
	listRequest       *scalepb.ListRecoverableScalePlansRequest
	listResponse      *scalepb.ListRecoverableScalePlansResponse
	listErr           error
}

func (f *fakeScalePlanClient) SubmitScalePlan(_ context.Context, request *scalepb.SubmitScalePlanRequest, _ ...grpc.CallOption) (*scalepb.SubmitScalePlanResponse, error) {
	f.submitRequest = request
	return f.submitResponse, f.submitErr
}

func (f *fakeScalePlanClient) GetScalePlan(_ context.Context, _ *scalepb.GetScalePlanRequest, _ ...grpc.CallOption) (*scalepb.GetScalePlanResponse, error) {
	f.getCalls++
	return f.getResponse, f.getErr
}

func (f *fakeScalePlanClient) ListRecoverableScalePlans(_ context.Context, request *scalepb.ListRecoverableScalePlansRequest, _ ...grpc.CallOption) (*scalepb.ListRecoverableScalePlansResponse, error) {
	f.listRequest = request
	return f.listResponse, f.listErr
}

func (f *fakeScalePlanClient) Pause(_ context.Context, request *scalepb.PauseRequest, _ ...grpc.CallOption) (*scalepb.PauseResponse, error) {
	f.pauseRequest = request
	return f.pauseResponse, f.pauseErr
}

func (f *fakeScalePlanClient) Resume(_ context.Context, request *scalepb.ResumeRequest, _ ...grpc.CallOption) (*scalepb.ResumeResponse, error) {
	f.resumeRequest = request
	return f.resumeResponse, f.resumeErr
}

func (f *fakeScalePlanClient) TerminateBlocked(_ context.Context, request *scalepb.TerminateBlockedRequest, _ ...grpc.CallOption) (*scalepb.TerminateBlockedResponse, error) {
	f.terminateRequest = request
	return f.terminateResponse, f.terminateErr
}

func routectlScalePlanFixture(status string) *scalepb.ScalePlan {
	return &scalepb.ScalePlan{
		PlanId: routectlScalePlanID, PayloadDigest: routectlScalePlanDigest,
		Realm: "realm-a", Domain: "PLAYER", Operation: scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT,
		Workload:         &scalepb.WorkloadRef{Realm: "realm-a", Domain: "PLAYER", Namespace: "farm", Kind: "StatefulSet", Name: "player"},
		ExpectedReplicas: 1, DesiredReplicas: 2,
		Moves:          []*scalepb.ShardMove{{Domain: "PLAYER", VirtualShard: 7, SourceHolderId: "holder-a", TargetHolderId: "holder-b", TargetOrdinal: 1}},
		RoutePlanTtlMs: 60000, PlanTtlMs: 3600000, RecoveryRetentionMs: 86400000,
		MaxRoutePlansInflight: 1, MaxShardsPerWave: 1, Status: status, Phase: "MOVING",
		Revision: 9, UpdatedAtMs: 100, BlockedReason: "EXCLUSION_RELEASE_UNRELEASED",
		BlockedCheckpointDigest: routectlCheckpointDigest,
	}
}

func TestRoutectlScalePlanGetShowsImmutableDigestAndSafeFields(t *testing.T) {
	client := &fakeScalePlanClient{getResponse: &scalepb.GetScalePlanResponse{Plan: routectlScalePlanFixture("RUNNING")}}
	var output strings.Builder
	if err := runScalePlanCommand(globalConfig{rpcTimeout: time.Second}, []string{"get", "--plan-id", routectlScalePlanID}, client, &output); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if !strings.Contains(got, "\"payload_digest\":\""+routectlScalePlanDigest+"\"") ||
		!strings.Contains(got, "\"operation\":\"SCALE_OUT\"") ||
		!strings.Contains(got, "\"virtual_shard\":7") {
		t.Fatalf("output=%s", got)
	}
	if strings.Contains(got, "idempotency") || strings.Contains(got, "secret") {
		t.Fatalf("unsafe field leaked: %s", got)
	}
}

func TestRoutectlScalePlanMutationRequiresExactDigestConfirmation(t *testing.T) {
	client := &fakeScalePlanClient{
		getResponse:   &scalepb.GetScalePlanResponse{Plan: routectlScalePlanFixture("RUNNING")},
		pauseResponse: &scalepb.PauseResponse{Plan: routectlScalePlanFixture("PAUSED")},
	}
	cfg := globalConfig{rpcTimeout: time.Second}
	if err := runScalePlanCommand(cfg, []string{"pause", "--plan-id", routectlScalePlanID}, client, &strings.Builder{}); !errors.Is(err, errUsage) {
		t.Fatalf("missing confirmation err=%v", err)
	}
	if client.getCalls != 0 || client.pauseRequest != nil {
		t.Fatalf("mutation proceeded without confirmation: get=%d pause=%v", client.getCalls, client.pauseRequest)
	}
	if err := runScalePlanCommand(cfg, []string{"pause", "--plan-id", routectlScalePlanID, "--confirm-digest", strings.Repeat("c", 64)}, client, &strings.Builder{}); !errors.Is(err, errVerificationMatch) {
		t.Fatalf("wrong confirmation err=%v", err)
	}
	if client.pauseRequest != nil {
		t.Fatal("mutation proceeded with mismatched confirmation")
	}
	var output strings.Builder
	if err := runScalePlanCommand(cfg, []string{"pause", "--plan-id", routectlScalePlanID, "--confirm-digest", routectlScalePlanDigest}, client, &output); err != nil {
		t.Fatal(err)
	}
	if client.pauseRequest == nil || client.pauseRequest.GetPlanId() != routectlScalePlanID || !strings.Contains(output.String(), "\"payload_digest\":\""+routectlScalePlanDigest+"\"") {
		t.Fatalf("request=%v output=%s", client.pauseRequest, output.String())
	}
}

func TestRoutectlScalePlanTerminateBlockedBindsExactProofAndTerminationID(t *testing.T) {
	before := routectlScalePlanFixture("BLOCKED_NEEDS_OPERATOR")
	client := &fakeScalePlanClient{
		getResponse:       &scalepb.GetScalePlanResponse{Plan: before},
		terminateResponse: &scalepb.TerminateBlockedResponse{Plan: routectlScalePlanFixture("TERMINATING")},
	}
	var output strings.Builder
	if err := runScalePlanCommand(globalConfig{rpcTimeout: time.Second}, []string{"terminate-blocked",
		"--plan-id", routectlScalePlanID, "--confirm-digest", routectlScalePlanDigest, "--termination-id", "termination-1"}, client, &output); err != nil {
		t.Fatal(err)
	}
	request := client.terminateRequest
	if request == nil || request.GetPlanId() != routectlScalePlanID || request.GetExpectedRevision() != before.GetRevision() ||
		request.GetExpectedBlockedReason() != before.GetBlockedReason() || request.GetExpectedPayloadDigest() != routectlScalePlanDigest ||
		request.GetExpectedCheckpointDigest() != routectlCheckpointDigest || request.GetTerminationId() != "termination-1" {
		t.Fatalf("terminate request=%v", request)
	}
}

func TestRoutectlScalePlanSubmitBindsExplicitMoveManifestAndIdempotency(t *testing.T) {
	client := &fakeScalePlanClient{submitResponse: &scalepb.SubmitScalePlanResponse{Plan: routectlScalePlanFixture("SUBMITTED")}}
	args := []string{"submit", "--idempotency-key", "request-1", "--realm", "realm-a", "--domain", "PLAYER",
		"--operation", "SCALE_OUT", "--namespace", "farm", "--kind", "StatefulSet", "--name", "player",
		"--expected-replicas", "1", "--desired-replicas", "2", "--move", "PLAYER,7,holder-a,holder-b,1"}
	if err := runScalePlanCommand(globalConfig{rpcTimeout: time.Second}, args, client, &strings.Builder{}); err != nil {
		t.Fatal(err)
	}
	request := client.submitRequest
	if request == nil || request.GetIdempotencyKey() != "request-1" || request.GetRealm() != "realm-a" ||
		request.GetOperation() != scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT ||
		request.GetWorkload().GetName() != "player" || len(request.GetMoves()) != 1 ||
		request.GetMoves()[0].GetVirtualShard() != 7 || request.GetMoves()[0].GetSourceHolderId() != "holder-a" ||
		request.GetMoves()[0].GetTargetHolderId() != "holder-b" || request.GetMoves()[0].GetTargetOrdinal() != 1 {
		t.Fatalf("submit request=%v", request)
	}
}

func TestRoutectlScalePlanBoundedListPassesLimit(t *testing.T) {
	client := &fakeScalePlanClient{listResponse: &scalepb.ListRecoverableScalePlansResponse{
		Plans: []*scalepb.ScalePlan{routectlScalePlanFixture("BLOCKED_NEEDS_OPERATOR")}, Truncated: true,
	}}
	if err := runScalePlanCommand(globalConfig{rpcTimeout: time.Second}, []string{"list", "--limit", "3"}, client, &strings.Builder{}); err != nil {
		t.Fatal(err)
	}
	if client.listRequest == nil || client.listRequest.GetLimit() != 3 {
		t.Fatalf("list request=%v", client.listRequest)
	}
}
