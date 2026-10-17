package friend

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	friendpb "farmgame/api/gen/friend"

	"github.com/redis/go-redis/v9"
)

type friendSnapshotBarrierHook struct {
	entered chan<- struct{}
	release <-chan struct{}
	blocked atomic.Bool
}

func (h *friendSnapshotBarrierHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *friendSnapshotBarrierHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.block(cmd)
		return next(ctx, cmd)
	}
}

func (h *friendSnapshotBarrierHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.block(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *friendSnapshotBarrierHook) block(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	matched := false
	switch cmd.Name() {
	case "eval":
		source, ok := args[1].(string)
		matched = ok && strings.Contains(source, "READ_FRIEND_AUTHORITY_COMPLETE_SNAPSHOT_V2")
	case "evalsha":
		hash, ok := args[1].(string)
		matched = ok && hash == redis.NewScript(friendAuthorityCompleteSnapshotSource).Hash()
	}
	if !matched || !h.blocked.CompareAndSwap(false, true) {
		return
	}
	h.entered <- struct{}{}
	<-h.release
}

type friendAtomicReadResult struct {
	pair   PairSnapshot
	permit *friendpb.ActionPermit
	err    error
}

func TestRedisFriendPairAtomicSnapshotAcrossPendingWorkCompletion(t *testing.T) {
	for _, testCase := range []struct {
		name string
		read func(context.Context, *RedisFriendshipRepositories, uint64, uint64) friendAtomicReadResult
	}{
		{name: "GetPair", read: func(ctx context.Context, repositories *RedisFriendshipRepositories, low, high uint64) friendAtomicReadResult {
			pair, err := repositories.Pair.GetPair(ctx, low, high)
			return friendAtomicReadResult{pair: pair, err: err}
		}},
		{name: "Permit", read: func(ctx context.Context, repositories *RedisFriendshipRepositories, low, high uint64) friendAtomicReadResult {
			service, err := NewPermitService(repositories.Pair, []byte(strings.Repeat("p", 32)), time.Minute)
			if err != nil {
				return friendAtomicReadResult{err: err}
			}
			permit, err := service.Issue(ctx, low, high, "snapshot-care", friendpb.FriendAction_CARE)
			return friendAtomicReadResult{permit: permit, err: err}
		}},
		{name: "BatchGetPairs", read: func(ctx context.Context, repositories *RedisFriendshipRepositories, low, high uint64) friendAtomicReadResult {
			pairs, err := repositories.Pair.BatchGetPairs(ctx, low, []uint64{high})
			if err != nil {
				return friendAtomicReadResult{err: err}
			}
			return friendAtomicReadResult{pair: pairs[0]}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			const low, high = uint64(1801), uint64(1802)
			binding, group := preparePairSnapshotPendingResult(t, ctx, repositories, low, high, "snapshot-"+testCase.name)

			// This is the deterministic old-boundary reproduction: metadata is
			// captured before terminal completion, while SourceDue is captured
			// after the same atomic completion. The two legitimate generations
			// disagree by one and the former reader reported false reconcile.
			oldMetaCount, err := repositories.authority.client.HGet(ctx, group.meta, "pending_work_count").Uint64()
			if err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			repositories.authority.client.AddHook(&friendSnapshotBarrierHook{entered: entered, release: release})
			result := make(chan friendAtomicReadResult, 1)
			go func() { result <- testCase.read(ctx, repositories, low, high) }()
			<-entered
			proof := sha256.Sum256([]byte("snapshot-completion-proof-" + testCase.name))
			if err := repositories.CompletePairActionResultOrder(ctx, binding, "SUCCEEDED", proof[:]); err != nil {
				t.Fatal(err)
			}
			oldLaterDueCount, err := repositories.authority.client.ZCard(ctx, group.sourceDue).Uint64()
			if err != nil {
				t.Fatal(err)
			}
			if oldMetaCount != oldLaterDueCount+1 {
				t.Fatalf("old boundary did not reproduce mixed generation: meta=%d later_due=%d", oldMetaCount, oldLaterDueCount)
			}
			close(release)
			read := <-result
			if read.err != nil {
				t.Fatalf("atomic read crossed terminal completion: %v", read.err)
			}
			if testCase.name == "Permit" {
				if read.permit == nil || read.permit.RelationEpoch != 1 {
					t.Fatalf("permit=%+v", read.permit)
				}
			} else if read.pair.Relation.Status != RelationStatusActive || read.pair.Relation.RelationEpoch != 1 {
				t.Fatalf("pair=%+v", read.pair)
			}
		})
	}
}

func TestRedisFriendPairAtomicSnapshotAcrossReceiptAttemptTransitions(t *testing.T) {
	t.Run("receipt and attempt creation", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		const low, high = uint64(1811), uint64(1812)
		entered := make(chan struct{})
		release := make(chan struct{})
		repositories.authority.client.AddHook(&friendSnapshotBarrierHook{entered: entered, release: release})
		result := make(chan friendAtomicReadResult, 1)
		go func() {
			pair, err := repositories.Pair.GetPair(ctx, low, high)
			result <- friendAtomicReadResult{pair: pair, err: err}
		}()
		<-entered
		outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(low,
			"BLOCK_PLAYER", "snapshot-create-block"), PayloadHash: redisFriendHash("snapshot-create-block"),
			CallerPlayerID: low, PeerPlayerID: high})
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("block=%+v err=%v", outcome, err)
		}
		close(release)
		read := <-result
		if read.err != nil || !read.pair.Exists() || read.pair.LowBlocksHigh.Status != BlockStatusActive {
			t.Fatalf("atomic post-commit read=%+v err=%v", read.pair, read.err)
		}
	})

	t.Run("completed attempt compaction", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: 300, AttemptRetentionMs: 1})
		defer server.Close()
		ctx := context.Background()
		const low, high = uint64(1821), uint64(1822)
		pairID, _, _, err := pairIDForPlayers(low, high)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(low,
			"BLOCK_PLAYER", "snapshot-compact-block"), PayloadHash: redisFriendHash("snapshot-compact-block"),
			CallerPlayerID: low, PeerPlayerID: high}); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("block=%+v err=%v", outcome, err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		beforeAttemptCount, err := repositories.authority.client.HGet(ctx, group.meta, "attempt_count").Uint64()
		if err != nil {
			t.Fatal(err)
		}
		server.FastForward(5 * time.Millisecond)
		entered := make(chan struct{})
		release := make(chan struct{})
		repositories.authority.client.AddHook(&friendSnapshotBarrierHook{entered: entered, release: release})
		result := make(chan friendAtomicReadResult, 1)
		go func() {
			pair, err := repositories.Pair.GetPair(ctx, low, high)
			result <- friendAtomicReadResult{pair: pair, err: err}
		}()
		<-entered
		if compacted, err := repositories.CompactCompletedAttemptMarkers(ctx, FriendPairAuthority, pairID, 1); err != nil || compacted != 1 {
			t.Fatalf("compacted=%d err=%v", compacted, err)
		}
		afterAttemptDue, err := repositories.authority.client.ZCard(ctx, group.attemptDue).Uint64()
		if err != nil || afterAttemptDue > beforeAttemptCount {
			t.Fatalf("attempt lower bound before_count=%d after_due=%d err=%v", beforeAttemptCount, afterAttemptDue, err)
		}
		close(release)
		read := <-result
		if read.err != nil || read.pair.LowBlocksHigh.Status != BlockStatusActive {
			t.Fatalf("atomic post-compaction read=%+v err=%v", read.pair, read.err)
		}
	})
}

func TestRedisFriendAuthorityAtomicSnapshotPersistentCorruptionFailsClosed(t *testing.T) {
	for _, testCase := range []struct {
		name, want string
		corrupt    func(context.Context, *RedisFriendshipRepositories, friendRedisGroup) error
	}{
		{name: "pending count", want: "bucket pending work count", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HIncrBy(ctx, group.meta, "pending_work_count", 1).Err()
		}},
		{name: "receipt count", want: "bucket receipt count", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HIncrBy(ctx, group.meta, "receipt_count", 1).Err()
		}},
		{name: "attempt lower bound", want: "bucket attempt count", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.ZAdd(ctx, group.attemptDue, redis.Z{Score: 1, Member: "corrupt-extra-attempt"}).Err()
		}},
		{name: "due key type", want: "bucket receipt due key type", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			if err := repositories.authority.client.Del(ctx, group.receiptDue).Err(); err != nil {
				return err
			}
			return repositories.authority.client.Set(ctx, group.receiptDue, "corrupt", 0).Err()
		}},
		{name: "meta manifest", want: "bucket meta manifest", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HDel(ctx, group.meta, "last_updated_at_ms").Err()
		}},
		{name: "malformed counter", want: "bucket meta counter last_updated_at_ms", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HSet(ctx, group.meta, "last_updated_at_ms", "01").Err()
		}},
		{name: "counter byte parity", want: "bucket counter/bytes pending_work_count", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HSet(ctx, group.meta, "pending_work_bytes", "0").Err()
		}},
		{name: "authority identity", want: "bucket meta identity", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, group friendRedisGroup) error {
			return repositories.authority.client.HSet(ctx, group.meta, "authority_kind", "UNKNOWN").Err()
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			const low, high = uint64(1831), uint64(1832)
			_, group := preparePairSnapshotPendingResult(t, ctx, repositories, low, high, "snapshot-corrupt-"+testCase.name)
			if err := testCase.corrupt(ctx, repositories, group); err != nil {
				t.Fatal(err)
			}
			_, singleErr := repositories.Pair.GetPair(ctx, low, high)
			_, batchErr := repositories.Pair.BatchGetPairs(ctx, low, []uint64{high})
			for name, err := range map[string]error{"single": singleErr, "batch": batchErr} {
				if !errors.Is(err, ErrFriendReconcile) || !strings.Contains(err.Error(), testCase.want) {
					t.Fatalf("%s error=%v want reconcile class %q", name, err, testCase.want)
				}
			}
		})
	}
}

func preparePairSnapshotPendingResult(t *testing.T, ctx context.Context, repositories *RedisFriendshipRepositories,
	low, high uint64, seed string) (PairActionResultBinding, friendRedisGroup) {
	t.Helper()
	pairID, _, _, err := pairIDForPlayers(low, high)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{InviterPlayerID: low,
		RedeemerPlayerID: high, TargetRelationEpoch: 1, TokenID: seed + "-token", TokenGeneration: 1,
		OperationID: seed + "-activate"}, CommitIdentity{WorkAddressID: pairID,
		ParticipantInvocationID: seed + "-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash(seed+"-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	action := ActionAdmissionCommand{Identity: redisFriendPublic(low, "CARE", seed+"-care"),
		PayloadHash: redisFriendHash(seed + "-care-payload"), VisitorPlayerID: low, OwnerPlayerID: high,
		TargetFarmID: high, Scope: ActionScopeCare, TargetBindingHash: redisFriendHash(seed + "-target"), PermitTTLMs: 30_000}
	if admission, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); err != nil || admission.Outcome.Status != StatusSucceeded {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	binding, found, err := repositories.lookupPairActionResult(ctx, low, high, action.Identity.OperationID, ActionScopeCare)
	if err != nil || !found {
		t.Fatalf("binding=%+v found=%v err=%v", binding, found, err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	return binding, group
}
