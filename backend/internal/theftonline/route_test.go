package theftonline

import (
	"context"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/workflow"
)

type theftRouteResolver struct{ assignment actorrouting.Assignment }

func (r theftRouteResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

func TestTheftRoutesEmitLeaseProofAndMixedVersionComparison(t *testing.T) {
	assignment := actorrouting.Assignment{Kind: actorrouting.DomainFarm, ShardID: 2, ShardRevision: 3,
		HolderPod: "farm-a", HolderBootID: "boot-a", Endpoint: "farm:1", WriterEpoch: 4,
		LeaseEpoch: 5, LeaseUntilMs: 9_000, SnapshotRevision: 6}
	provider := CoordinatorRouteProvider(theftRouteResolver{assignment}, "realm-a")
	header, err := provider(context.Background(), workflow.StepCall{},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, 20002)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != 5 || header.GetExpectedLeaseUntilMs() != 9_000 || header.ExpectedLeaseUntilMs == nil {
		t.Fatalf("producer omitted lease proof: %+v", header)
	}
	legacy := cloneTheftRoute(header)
	legacy.ExpectedLeaseEpoch, legacy.ExpectedLeaseUntilMs = 0, nil
	partial := cloneTheftRoute(header)
	partial.ExpectedLeaseUntilMs = nil
	stale := cloneTheftRoute(header)
	stale.ExpectedLeaseEpoch++
	for _, tc := range []struct {
		name   string
		header *logicrpcpb.ActorRouteHeader
		want   bool
	}{{"current", header, true}, {"legacy", legacy, true}, {"partial", partial, false}, {"stale", stale, false}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameAssignment(tc.header, assignment); got != tc.want {
				t.Fatalf("sameAssignment=%v want=%v", got, tc.want)
			}
		})
	}
}

func cloneTheftRoute(header *logicrpcpb.ActorRouteHeader) *logicrpcpb.ActorRouteHeader {
	copy := *header
	if header.ExpectedLeaseUntilMs != nil {
		deadline := *header.ExpectedLeaseUntilMs
		copy.ExpectedLeaseUntilMs = &deadline
	}
	return &copy
}
