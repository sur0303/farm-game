package friend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPairResultLookupRecoversRejectedBeforeWorkAndReleasesExactCapacity(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	bucketID := prepareBlockedPairForResultTest(t, ctx, repositories, 701, 702, "rejected-before-work")
	baselineMetaKey, _ := repositories.PairBucketMetaKey(bucketID)
	baseline, _ := repositories.authority.client.HMGet(ctx, baselineMetaKey, "pending_work_count", "pending_work_bytes").Result()
	action := ActionAdmissionCommand{
		Identity:    CommitIdentity{SubjectPlayerID: 701, CommandType: "THEFT", OperationID: "rejected-before-work"},
		PayloadHash: redisFriendHash("rejected-before-work-payload"), VisitorPlayerID: 701,
		OwnerPlayerID: 702, TargetFarmID: 702, Scope: ActionScopeTheft,
		TargetBindingHash: redisFriendHash("rejected-before-work-target"), PermitTTLMs: 30_000,
	}
	admission, err := repositories.Pair.CommitFriendActionAdmission(ctx, action)
	if err != nil || admission.Outcome.Status != StatusRejected || admission.DispatchOrderID != "" {
		t.Fatalf("rejected admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.LookupPairActionResult(ctx, 701, 702, action.Identity.OperationID)
	if err != nil || !found || binding.AdmissionStatus != StatusRejected || binding.AdmissionReason != ReasonVisitNotPermitted ||
		binding.DispatchOrderID != "" || binding.WorkflowDeadlineAtMs != 0 ||
		hex.EncodeToString(binding.PayloadHash) != hex.EncodeToString(action.PayloadHash) {
		t.Fatalf("rejected binding=%+v found=%v err=%v", binding, found, err)
	}
	metaKey, err := repositories.PairBucketMetaKey(binding.BucketID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := repositories.authority.client.HMGet(ctx, metaKey, "pending_work_count", "pending_work_bytes").Result()
	baselineCount, _ := strconv.Atoi(baseline[0].(string))
	baselineBytes, _ := strconv.Atoi(baseline[1].(string))
	if err != nil || len(before) != 2 || before[0] != strconv.Itoa(baselineCount+1) {
		t.Fatalf("pending before=%v err=%v", before, err)
	}
	charged, err := strconv.Atoi(before[1].(string))
	if err != nil || charged <= 0 {
		t.Fatalf("charged bytes=%v err=%v", before[1], err)
	}
	proof := sha256.Sum256([]byte("rejected-before-work-proof"))
	if err := repositories.CompletePairActionResultOrder(ctx, binding, "REJECTED", proof[:]); err != nil {
		t.Fatal(err)
	}
	if err := repositories.CompletePairActionResultOrder(ctx, binding, "REJECTED", proof[:]); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
	after, err := repositories.authority.client.HMGet(ctx, metaKey, "pending_work_count", "pending_work_bytes").Result()
	if err != nil || after[0] != strconv.Itoa(baselineCount) || after[1] != strconv.Itoa(baselineBytes) {
		t.Fatalf("pending after=%v charged=%d err=%v", after, charged, err)
	}
	if _, err := repositories.authority.client.ZScore(ctx, mustPairDueKey(t, repositories, binding.BucketID), binding.ResultWorkRef).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("result due member remained: %v", err)
	}
	recovered, found, err := repositories.LookupPairActionResult(ctx, 701, 702, action.Identity.OperationID)
	if err != nil || !found || recovered.ResultState != "REJECTED" {
		t.Fatalf("terminal lookup=%+v found=%v err=%v", recovered, found, err)
	}
}

func TestPairResultDeferMovesExactDueScoreAndFencesStaleWriter(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(721, 722)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 721, RedeemerPlayerID: 722, TargetRelationEpoch: 1,
		TokenID: "result-defer-token", TokenGeneration: 1, OperationID: "result-defer-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "result-defer-activate", ParticipantStep: "ACTIVATE"},
		redisFriendHash("result-defer-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	action := ActionAdmissionCommand{
		Identity:    CommitIdentity{SubjectPlayerID: 721, CommandType: "THEFT", OperationID: "result-defer"},
		PayloadHash: redisFriendHash("result-defer-payload"), VisitorPlayerID: 721,
		OwnerPlayerID: 722, TargetFarmID: 722, Scope: ActionScopeTheft,
		TargetBindingHash: redisFriendHash("result-defer-target"), PermitTTLMs: 30_000,
	}
	if admission, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); err != nil || admission.Outcome.Status != StatusSucceeded {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.LookupPairActionResult(ctx, 721, 722, action.Identity.OperationID)
	if err != nil || !found || binding.ResultWorkVersion != 1 || binding.ResultRetryCount != 0 || binding.ResultNextDueAtMs != 0 {
		t.Fatalf("initial binding=%+v found=%v err=%v", binding, found, err)
	}
	if err := repositories.DeferPairActionResultOrder(ctx, binding, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := repositories.DeferPairActionResultOrder(ctx, binding, 2*time.Second); !errors.Is(err, ErrFriendRecordVersionConflict) {
		t.Fatalf("stale defer err=%v", err)
	}
	deferred, found, err := repositories.LookupPairActionResult(ctx, 721, 722, action.Identity.OperationID)
	if err != nil || !found || deferred.ResultWorkVersion != 2 || deferred.ResultRetryCount != 1 || deferred.ResultNextDueAtMs <= 0 {
		t.Fatalf("deferred binding=%+v found=%v err=%v", deferred, found, err)
	}
	score, err := repositories.authority.client.ZScore(ctx, mustPairDueKey(t, repositories, deferred.BucketID), deferred.ResultWorkRef).Result()
	if err != nil || int64(score) != deferred.ResultNextDueAtMs {
		t.Fatalf("due score=%v want=%d err=%v", score, deferred.ResultNextDueAtMs, err)
	}
	proof := sha256.Sum256([]byte("deferred-result-proof"))
	if err := repositories.CompletePairActionResultOrder(ctx, deferred, "SUCCEEDED", proof[:]); err != nil {
		t.Fatal(err)
	}
	terminal, found, err := repositories.LookupPairActionResult(ctx, 721, 722, action.Identity.OperationID)
	if err != nil || !found || terminal.ResultState != "SUCCEEDED" || terminal.ResultWorkVersion != 3 {
		t.Fatalf("terminal binding=%+v found=%v err=%v", terminal, found, err)
	}
}

func TestPairResultCompletionCounterCorruptionFailsClosedAndRemainsDue(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	prepareBlockedPairForResultTest(t, ctx, repositories, 711, 712, "result-counter-corrupt")
	action := ActionAdmissionCommand{
		Identity:    CommitIdentity{SubjectPlayerID: 711, CommandType: "THEFT", OperationID: "result-counter-corrupt"},
		PayloadHash: redisFriendHash("result-counter-corrupt-payload"), VisitorPlayerID: 711,
		OwnerPlayerID: 712, TargetFarmID: 712, Scope: ActionScopeTheft,
		TargetBindingHash: redisFriendHash("result-counter-corrupt-target"), PermitTTLMs: 30_000,
	}
	if admission, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); err != nil || admission.Outcome.Status != StatusRejected {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.LookupPairActionResult(ctx, 711, 712, action.Identity.OperationID)
	if err != nil || !found {
		t.Fatalf("binding=%+v found=%v err=%v", binding, found, err)
	}
	metaKey, _ := repositories.PairBucketMetaKey(binding.BucketID)
	if err := repositories.authority.client.HSet(ctx, metaKey, "pending_work_bytes", 1).Err(); err != nil {
		t.Fatal(err)
	}
	proof := sha256.Sum256([]byte("counter-corrupt-proof"))
	if err := repositories.CompletePairActionResultOrder(ctx, binding, "REJECTED", proof[:]); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("completion error=%v", err)
	}
	if score, err := repositories.authority.client.ZScore(ctx, mustPairDueKey(t, repositories, binding.BucketID), binding.ResultWorkRef).Result(); err != nil || score != 0 {
		t.Fatalf("corrupt order disappeared: score=%v err=%v", score, err)
	}
	key, _ := repositories.PairSourceOrderKey(binding.BucketID, "RESULT_PUBLISH_ORDER", binding.ResultOrderID)
	if state, err := repositories.authority.client.HGet(ctx, key, "state").Result(); err != nil || state != "PENDING" {
		t.Fatalf("corrupt order state=%q err=%v", state, err)
	}
}

func prepareBlockedPairForResultTest(t *testing.T, ctx context.Context, repositories *RedisFriendshipRepositories,
	visitor, owner uint64, seed string) uint32 {
	t.Helper()
	pairID, _, _, err := pairIDForPlayers(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activationHash := redisFriendHash(seed + "-activate")
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{InviterPlayerID: visitor,
		RedeemerPlayerID: owner, TargetRelationEpoch: 1, TokenID: seed + "-token", TokenGeneration: 1,
		OperationID: seed + "-activate"}, CommitIdentity{WorkAddressID: pairID,
		ParticipantInvocationID: seed + "-activate", ParticipantStep: "ACTIVATE"}, activationHash); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: CommitIdentity{
		SubjectPlayerID: visitor, CommandType: "BLOCK_PLAYER", OperationID: seed + "-block"},
		PayloadHash: redisFriendHash(seed + "-block"), CallerPlayerID: visitor, PeerPlayerID: owner}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("block=%+v err=%v", outcome, err)
	}
	ref, err := repositories.authority.keys.BucketFor(FriendPairAuthority, pairID)
	if err != nil {
		t.Fatal(err)
	}
	return ref.BucketID
}

func mustPairDueKey(t *testing.T, repositories *RedisFriendshipRepositories, bucketID uint32) string {
	t.Helper()
	key, err := repositories.PairDueIndexKey(bucketID)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
