package harvestonline

import (
	"context"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/workflow"
)

type harvestRouteResolver struct{ assignment actorrouting.Assignment }

func (r harvestRouteResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

func TestHarvestRoutesEmitLeaseProofAndAcceptOnlyCompleteLegacyOrCurrent(t *testing.T) {
	assignment := actorrouting.Assignment{Kind: actorrouting.DomainFarm, ShardID: 3, ShardRevision: 4,
		HolderPod: "farm-a", HolderBootID: "boot-a", Endpoint: "farm:1", WriterEpoch: 5,
		LeaseEpoch: 6, LeaseUntilMs: 2_000, SnapshotRevision: 7}
	provider := CoordinatorRouteProvider(harvestRouteResolver{assignment}, "realm-a")
	header, err := provider(context.Background(), workflow.StepCall{},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != assignment.LeaseEpoch || header.ExpectedLeaseUntilMs == nil ||
		header.GetExpectedLeaseUntilMs() != uint64(assignment.LeaseUntilMs) {
		t.Fatalf("producer omitted lease proof: %+v", header)
	}
	legacy := cloneHarvestRoute(header)
	legacy.ExpectedLeaseEpoch, legacy.ExpectedLeaseUntilMs = 0, nil
	partialEpoch := cloneHarvestRoute(header)
	partialEpoch.ExpectedLeaseUntilMs = nil
	partialDeadline := cloneHarvestRoute(header)
	partialDeadline.ExpectedLeaseEpoch = 0
	wrongEpoch := cloneHarvestRoute(header)
	wrongEpoch.ExpectedLeaseEpoch++
	tooNewDeadline := cloneHarvestRoute(header)
	deadline := tooNewDeadline.GetExpectedLeaseUntilMs() + 1
	tooNewDeadline.ExpectedLeaseUntilMs = &deadline
	for _, tc := range []struct {
		name   string
		header *logicrpcpb.ActorRouteHeader
		want   bool
	}{
		{"current", header, true}, {"legacy", legacy, true}, {"partial epoch", partialEpoch, false},
		{"partial deadline", partialDeadline, false}, {"wrong epoch", wrongEpoch, false},
		{"caller ahead of receiver", tooNewDeadline, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameAssignment(tc.header, assignment); got != tc.want {
				t.Fatalf("sameAssignment=%v want=%v", got, tc.want)
			}
		})
	}
}

func cloneHarvestRoute(header *logicrpcpb.ActorRouteHeader) *logicrpcpb.ActorRouteHeader {
	copy := *header
	if header.ExpectedLeaseUntilMs != nil {
		deadline := *header.ExpectedLeaseUntilMs
		copy.ExpectedLeaseUntilMs = &deadline
	}
	return &copy
}
