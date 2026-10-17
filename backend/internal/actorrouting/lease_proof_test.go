package actorrouting

import "testing"

func TestCompatibleLeaseProofTreatsDeadlineAsRenewableProjection(t *testing.T) {
	assignment := Assignment{LeaseEpoch: 7, LeaseUntilMs: 2_000}
	current := uint64(2_000)
	callerBehind := uint64(1_900)
	callerAhead := uint64(2_100)
	zero := uint64(0)

	for _, tc := range []struct {
		name     string
		epoch    uint64
		deadline *uint64
		legacy   bool
		want     bool
	}{
		{name: "same deadline", epoch: 7, deadline: &current, want: true},
		{name: "caller behind receiver", epoch: 7, deadline: &callerBehind, want: true},
		{name: "caller ahead of receiver", epoch: 7, deadline: &callerAhead, want: true},
		{name: "wrong lease generation", epoch: 8, deadline: &current, want: false},
		{name: "missing deadline", epoch: 7, deadline: nil, want: false},
		{name: "missing epoch", epoch: 0, deadline: &current, want: false},
		{name: "zero deadline", epoch: 7, deadline: &zero, want: false},
		{name: "legacy accepted", legacy: true, want: true},
		{name: "legacy rejected", legacy: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CompatibleLeaseProof(tc.epoch, tc.deadline, assignment, tc.legacy); got != tc.want {
				t.Fatalf("CompatibleLeaseProof=%v want=%v", got, tc.want)
			}
		})
	}
}
