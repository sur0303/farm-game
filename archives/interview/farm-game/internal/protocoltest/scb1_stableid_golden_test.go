package protocoltest

import (
	"testing"

	"farmgame/internal/identity"
)

// Golden vectors frozen by design contract 31 §8.1A and 33A §2.7. Any
// character, field-order or canonicalization difference must fail these
// tests. Go, Lua fixtures, workers, load tools and H5 test tools share
// these values.

func mustWorkflowID(t *testing.T, subject uint64, command, operationID string, kind identity.RootAuthorityKind, rootID string) string {
	t.Helper()
	id, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       subject,
		StableCommandTypeName: command,
		OperationID:           operationID,
		Root:                  identity.OperationRootRef{Kind: kind, CanonicalID: rootID},
	})
	if err != nil {
		t.Fatalf("workflow id: %v", err)
	}
	return id
}

func TestStableOpaqueIDGoldenVectors(t *testing.T) {
	relationID := "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to"
	plantWorkflowID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	unlockWorkflowID := "sid1_UxPZD3u5t4O3VczETFXvXyR2QkXmHAZx6Geck87do1s"
	theftWorkflowID := "sid1_0wXIbmzukfEx6A60-oLe78bM6zPRJAL-SnqbwM-Y3mE"
	addFriendWorkflowID := "sid1_dFIIRvo0XLh3V0u3P_w20K8gTHY9M_wTy__AQPavdQI"
	cropCycleID := "sid1_KDm_gSYlKXdcw1DATIGdnP1IJHPjHyK1VQUU11max00"

	t.Run("workflow-id-v2", func(t *testing.T) {
		if got := mustWorkflowID(t, 10001, "PLANT", "op-0001", identity.RootKindFarmActor, "10001"); got != plantWorkflowID {
			t.Fatalf("PLANT workflow = %s", got)
		}
		if got := mustWorkflowID(t, 10001, "UNLOCK_PLOT", "op-unlock-0001", identity.RootKindFarmActor, "10001"); got != unlockWorkflowID {
			t.Fatalf("UNLOCK_PLOT workflow = %s", got)
		}
		if got := mustWorkflowID(t, 10001, "THEFT", "op-theft-0001", identity.RootKindFriendPair, relationID); got != theftWorkflowID {
			t.Fatalf("THEFT workflow = %s", got)
		}
		if got := mustWorkflowID(t, 20002, "REDEEM_SHARE", "op-redeem-0001", identity.RootKindFriendPair, relationID); got != addFriendWorkflowID {
			t.Fatalf("REDEEM_SHARE workflow = %s", got)
		}
	})

	t.Run("source-order-id-v1", func(t *testing.T) {
		got, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, "20002", "care-plot-3-cycle-7", 12)
		if err != nil {
			t.Fatalf("care order: %v", err)
		}
		if want := "sid1_EqEZHYwCxXtF51d-UZlwKyxP4RMAXSaYCDAQMDkkbzI"; got != want {
			t.Fatalf("care order = %s, want %s", got, want)
		}
		got, err = identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor, "10001", "PET_GUARD_SLOT", 7)
		if err != nil {
			t.Fatalf("guard order: %v", err)
		}
		if want := "sid1_L5lEIo2wh82J0fBZYWtvm3BwxblL0NV8s_p9agW7GXg"; got != want {
			t.Fatalf("guard order = %s, want %s", got, want)
		}
	})

	t.Run("participant-invocation-v1", func(t *testing.T) {
		got, err := identity.ParticipantInvocationID(identity.WorkTypePlant, plantWorkflowID, identity.StepReserveSeed, 1)
		if err != nil {
			t.Fatalf("invocation: %v", err)
		}
		if want := "sid1_9WktgxCvxV9zV7eR6LkxE9p5mSm0rfNFrfQMJIdZlSY"; got != want {
			t.Fatalf("invocation = %s, want %s", got, want)
		}
	})

	t.Run("reservation-and-cycle-ids", func(t *testing.T) {
		got, err := identity.UnlockCoinReservationID(unlockWorkflowID)
		if err != nil {
			t.Fatalf("unlock reservation: %v", err)
		}
		if want := "sid1_8QJEXKpUJqlPI_gv7kAWjxhsBkntcIUcLA9r7K53hBM"; got != want {
			t.Fatalf("unlock reservation = %s, want %s", got, want)
		}

		got, err = identity.SeedReservationID(plantWorkflowID)
		if err != nil {
			t.Fatalf("seed reservation: %v", err)
		}
		if want := "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo"; got != want {
			t.Fatalf("seed reservation = %s, want %s", got, want)
		}

		got, err = identity.CropCycleID(plantWorkflowID)
		if err != nil {
			t.Fatalf("crop cycle: %v", err)
		}
		if got != cropCycleID {
			t.Fatalf("crop cycle = %s, want %s", got, cropCycleID)
		}

		got, err = identity.CareIncidentID(cropCycleID, "WATER", 1)
		if err != nil {
			t.Fatalf("care incident: %v", err)
		}
		if want := "sid1_DURawr0H1WHVobo4ivmSvQM7k5uhlPVY3Qbf_tVCXVw"; got != want {
			t.Fatalf("care incident = %s, want %s", got, want)
		}

		got, err = identity.TheftAttemptID(20002, cropCycleID, 10001)
		if err != nil {
			t.Fatalf("theft attempt: %v", err)
		}
		if want := "sid1_TKE2-YZ3x8lUCWJSbTCeyx3YzF4g4rN52d-8ljiDMXA"; got != want {
			t.Fatalf("theft attempt = %s, want %s", got, want)
		}

		got, err = identity.TheftSettlementID(theftWorkflowID)
		if err != nil {
			t.Fatalf("theft settlement: %v", err)
		}
		if want := "sid1_X-s0wd3EKTjjviu2MK4QMVVoGCtr8kvl8n6Nwe7XLuc"; got != want {
			t.Fatalf("theft settlement = %s, want %s", got, want)
		}
	})

	t.Run("friendship-33A", func(t *testing.T) {
		got, err := identity.FriendRelationID(10001, 20002)
		if err != nil {
			t.Fatalf("relation: %v", err)
		}
		if got != relationID {
			t.Fatalf("relation = %s, want %s", got, relationID)
		}

		got, err = identity.FriendQuotaReservationID(addFriendWorkflowID, 10001, relationID)
		if err != nil {
			t.Fatalf("quota reservation: %v", err)
		}
		if want := "sid1_zhgY0uuAa9NwvuL2zz9-8QoPwTr3ss8BjFhIj5iun0g"; got != want {
			t.Fatalf("quota reservation = %s, want %s", got, want)
		}

		got, err = identity.ShareTokenValidationReceiptID(addFriendWorkflowID, 10001, 20002, 7)
		if err != nil {
			t.Fatalf("share token validation receipt: %v", err)
		}
		if want := "sid1_YbhYMhVTgFYgC7ZG_EDcVCrXiz2Yyp2UdujLgGdHiEA"; got != want {
			t.Fatalf("share token validation receipt = %s, want %s", got, want)
		}

		got, err = identity.FriendQuotaReleaseFenceID(10001, relationID, 5)
		if err != nil {
			t.Fatalf("quota release fence: %v", err)
		}
		if want := "sid1_Ti_lMovux-TmFS8v9BOPHFuGez38omUc_qqP1yResNE"; got != want {
			t.Fatalf("quota release fence = %s, want %s", got, want)
		}

		got, err = identity.FriendCommitAttemptID(relationID,
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 1)
		if err != nil {
			t.Fatalf("commit attempt: %v", err)
		}
		if want := "sid1_iZcJ6qLkVb3PKDPZKhZNkaPubyHuM28Ha8BUxWx1Sn0"; got != want {
			t.Fatalf("commit attempt = %s, want %s", got, want)
		}
	})
}

// TestOperationDigestGoldenVector pins the EffectiveIdempotencyKey digest
// shape of contract 31 §8.1. The contract freezes the algorithm but no
// sample value; this self-derived vector is the regression anchor shared
// with future Lua fixtures.
func TestOperationDigestGoldenVector(t *testing.T) {
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       10001,
		StableCommandTypeName: "PLANT",
		OperationID:           "op-0001",
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "10001"},
	}
	got, err := key.OperationDigest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	const want = "fcc620db37bcf0a39e3c95eed90f433e6b32c4212559c8bd9d657cb2b3847189"
	if got != want {
		t.Fatalf("digest = %s, want %s", got, want)
	}
}

// TestIdentityValidationFailClosed covers the boundary rules: zero IDs,
// non-canonical pair order, bad digests and forged parent IDs are rejected.
func TestIdentityValidationFailClosed(t *testing.T) {
	if _, err := identity.FriendRelationID(20002, 10001); err == nil {
		t.Fatal("non-canonical pair order must fail")
	}
	if _, err := identity.FriendRelationID(0, 10001); err == nil {
		t.Fatal("zero player id must fail")
	}
	if _, err := identity.FriendCommitAttemptID("not-a-sid", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 1); err == nil {
		t.Fatal("non sid1_ pair id must fail")
	}
	if _, err := identity.FriendCommitAttemptID("sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to", "ABCDEF", 1); err == nil {
		t.Fatal("non lowercase hex digest must fail")
	}
	bad := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       10001,
		StableCommandTypeName: "plant", // 稳定名必须大写
		OperationID:           "op-0001",
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "10001"},
	}
	if _, err := bad.OperationDigest(); err == nil {
		t.Fatal("lowercase command name must fail")
	}
}
