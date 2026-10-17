package capaudit

import (
	"context"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const friendActivatedAtMs = int64(1_800_000_000_000)

// befriend writes the canonical half of one friendship through the same Redis
// repositories the services use, and deliberately leaves the projection alone.
// That is what a projection failure looks like from storage: the pair says the
// two are friends, neither list shows it.
func befriend(t *testing.T, repositories *friend.RedisFriendshipRepositories, low, high uint64) {
	t.Helper()
	ctx := context.Background()
	relationID, err := friend.RelationIDFor(low, high)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := repositories.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
		InviterPlayerID: high, RedeemerPlayerID: low,
		TargetRelationEpoch: 1, TokenID: "token-1", TokenGeneration: 1,
		OperationID: "activate-1", NowMs: friendActivatedAtMs,
	}, friend.CommitIdentity{SubjectPlayerID: low, CommandType: "REDEEM_SHARE", OperationID: "activate-1"},
		[]byte("payload"))
	if err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("activate outcome=%+v err=%v", outcome, err)
	}
	for _, player := range []uint64{low, high} {
		id := strconv.FormatUint(player, 10)
		reservation := friend.QuotaReservation{
			ReservationID: "res-" + id, WorkflowID: "wf-" + id,
			RelationID: relationID, TargetRelationEpoch: 1, CreatedAtMs: friendActivatedAtMs,
		}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, player, reservation,
			friend.CommitIdentity{SubjectPlayerID: player, CommandType: "REDEEM_SHARE", OperationID: "reserve-" + id},
			[]byte("payload"), friendActivatedAtMs); err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Quota.FinalizeFriendSlot(ctx, player, reservation,
			friend.CommitIdentity{SubjectPlayerID: player, CommandType: "REDEEM_SHARE", OperationID: "finalize-" + id},
			[]byte("payload"), friendActivatedAtMs); err != nil {
			t.Fatal(err)
		}
	}
}

func friendFixture(t *testing.T, ledger Ledger) (*friend.RedisFriendshipRepositories, *FriendIndexCheck) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForDomains(client, client,
		friend.RedisFriendshipOptions{Profile: friend.DefaultFriendRedisProfile(namespace)})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := friend.NewFriendIndexReconciler(repositories.Index, repositories.Pair, repositories.Quota)
	if err != nil {
		t.Fatal(err)
	}
	check, err := NewFriendIndexCheck(FriendIndexConfig{
		Reconciler: reconciler, Ledger: ledger,
		// Far enough past the fixture that the repair window has expired.
		Now: func() time.Time { return time.UnixMilli(friendActivatedAtMs).Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return repositories, check
}

// The audit has to read the keyspace the services write. A friendship written
// through the real repositories, with the projection never updated, must show
// up as unrepaired on both players' lists.
func TestTheAuditReadsTheFriendshipsTheServicesWrote(t *testing.T) {
	ledger := testLedger(2)
	low, high := ledger.ActorBase, ledger.ActorBase+1
	repositories, check := friendFixture(t, ledger)
	befriend(t, repositories, low, high)

	unrepaired, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if unrepaired != 2 {
		t.Fatalf("unrepaired=%d, want one lost row on each of the two lists", unrepaired)
	}
}

// Once repair has run, the same audit has to report the run clean. A gate that
// cannot reach zero would fail every run regardless of what happened.
func TestARepairedProjectionAuditsClean(t *testing.T) {
	ledger := testLedger(2)
	low, high := ledger.ActorBase, ledger.ActorBase+1
	repositories, check := friendFixture(t, ledger)
	befriend(t, repositories, low, high)
	reconciler, err := friend.NewFriendIndexReconciler(repositories.Index, repositories.Pair, repositories.Quota)
	if err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []uint64{low, high} {
		if _, err := reconciler.ReconcileViewer(context.Background(), viewer); err != nil {
			t.Fatal(err)
		}
	}

	unrepaired, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if unrepaired != 0 {
		t.Fatalf("unrepaired=%d after repair, want a clean run", unrepaired)
	}
}
