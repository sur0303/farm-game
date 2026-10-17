package sourceorder

import (
	"context"
	"errors"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

type admissionReaderFake struct {
	nowMs int64
	score int64
	bound bool
	err   error
}

func (f admissionReaderFake) TrustedNowMs(context.Context) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.nowMs, nil
}

func (f admissionReaderFake) DueIndexScore(context.Context, rediskey.AuthorityBucketRef, string, string) (int64, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	return f.score, f.bound, nil
}

func TestVerifyRedisAdmissionFenceWorkerRequiresExactLeaseAndDueIndex(t *testing.T) {
	for _, workType := range []string{"HARVEST", "PLANT", "UNLOCK_PLOT"} {
		t.Run(workType, func(t *testing.T) {
			call := &contracts.ParticipantCall{
				Work: &contractspb.WorkAddress{}, CallerKind: contracts.ParticipantCallerWorkerRecovery,
				ExpectedWorkVersion: 7, CallerInstanceID: "worker-a", CallerBootID: "boot-a",
				LeaseOwnerToken: "token-a", LeaseEpoch: 11,
			}
			order := authority.SourceOrderSnapshot{
				WorkID: "order-a", WorkType: workType, WorkVersion: 7, State: "REWARD_DISPATCHING",
				NextDueAtMs: 2_000, LeaseHolderID: "worker-a", LeaseHolderBootID: "boot-a",
				LeaseEpoch: 11, LeaseTokenHash: authority.HashLeaseToken("token-a"), LeaseExpireAtMs: 2_000,
			}
			reader := admissionReaderFake{nowMs: 1_000, score: 2_000, bound: true}
			if err := VerifyRedisAdmissionFence(context.Background(), reader, rediskey.AuthorityBucketRef{}, workType, order, call); err != nil {
				t.Fatalf("valid Worker fence rejected: %v", err)
			}

			cases := []struct {
				name   string
				mutate func(*authority.SourceOrderSnapshot, *contracts.ParticipantCall, *admissionReaderFake)
			}{
				{"holder", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.LeaseHolderID = "worker-b"
				}},
				{"boot", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.LeaseHolderBootID = "boot-b"
				}},
				{"token hash", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.LeaseTokenHash = authority.HashLeaseToken("token-b")
				}},
				{"lease epoch", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.LeaseEpoch++
				}},
				{"work version", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.WorkVersion++
				}},
				{"expired", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.LeaseExpireAtMs = 1_000
					o.NextDueAtMs = 1_000
				}},
				{"state", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.State = "PENDING"
				}},
				{"order due", func(o *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, _ *admissionReaderFake) {
					o.NextDueAtMs++
				}},
				{"DueIndex score", func(_ *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, r *admissionReaderFake) {
					r.score++
				}},
				{"DueIndex absent", func(_ *authority.SourceOrderSnapshot, _ *contracts.ParticipantCall, r *admissionReaderFake) {
					r.bound = false
				}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					candidateOrder := order
					candidateCall := *call
					candidateReader := reader
					tc.mutate(&candidateOrder, &candidateCall, &candidateReader)
					if err := VerifyRedisAdmissionFence(context.Background(), candidateReader, rediskey.AuthorityBucketRef{}, workType, candidateOrder, &candidateCall); !errors.Is(err, ErrSourceOrderAdmissionFence) {
						t.Fatalf("mutation admitted: %v", err)
					}
				})
			}
		})
	}
}

func TestVerifyRedisAdmissionFenceCoordinatorRequiresDueUnleasedOrder(t *testing.T) {
	call := &contracts.ParticipantCall{
		Work: &contractspb.WorkAddress{}, CallerKind: contracts.ParticipantCallerCoordinator,
		ExpectedWorkVersion: 3, OperationID: "operation-a",
	}
	order := authority.SourceOrderSnapshot{
		WorkID: "order-a", WorkType: "PLANT", WorkVersion: 3, State: "PENDING",
		OperationID: "operation-a", NextDueAtMs: 900,
	}
	reader := admissionReaderFake{nowMs: 1_000, score: 900, bound: true}
	if err := VerifyRedisAdmissionFence(context.Background(), reader, rediskey.AuthorityBucketRef{}, "PLANT", order, call); err != nil {
		t.Fatalf("valid direct fence rejected: %v", err)
	}
	unavailable := reader
	unavailable.err = errors.New("redis unavailable")
	if err := VerifyRedisAdmissionFence(context.Background(), unavailable, rediskey.AuthorityBucketRef{}, "PLANT", order, call); !errors.Is(err, ErrSourceOrderAdmissionUnavailable) {
		t.Fatalf("dependency outage classification=%v", err)
	}

	for name, mutate := range map[string]func(*authority.SourceOrderSnapshot, *admissionReaderFake){
		"not due":   func(o *authority.SourceOrderSnapshot, r *admissionReaderFake) { o.NextDueAtMs = 1_001; r.score = 1_001 },
		"operation": func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) { o.OperationID = "operation-b" },
		"holder":    func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) { o.LeaseHolderID = "worker" },
		"boot":      func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) { o.LeaseHolderBootID = "boot" },
		"token": func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) {
			o.LeaseTokenHash = authority.HashLeaseToken("token")
		},
		"epoch":  func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) { o.LeaseEpoch = 1 },
		"expiry": func(o *authority.SourceOrderSnapshot, _ *admissionReaderFake) { o.LeaseExpireAtMs = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidateOrder, candidateReader := order, reader
			mutate(&candidateOrder, &candidateReader)
			if err := VerifyRedisAdmissionFence(context.Background(), candidateReader, rediskey.AuthorityBucketRef{}, "PLANT", candidateOrder, call); !errors.Is(err, ErrSourceOrderAdmissionFence) {
				t.Fatalf("mutation admitted: %v", err)
			}
		})
	}
}
