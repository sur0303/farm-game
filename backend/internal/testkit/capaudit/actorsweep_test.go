package capaudit

import (
	"context"
	"testing"

	commonpb "farmgame/api/gen/common"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func testLedger(count uint64) Ledger {
	return Ledger{SchemaVersion: LedgerSchema, RunID: "r-test", ActorBase: 900000001, ActorCount: count}
}

func newSweep(t *testing.T, ledger Ledger) (*ActorSweep, *redis.Client, rediskey.AuthorityKeys) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	sweep, err := NewActorSweep(ActorSweepConfig{
		Client: client, Profile: profile, Shards: shards, Ledger: ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sweep, client, keys
}

func setActor(t *testing.T, sweep *ActorSweep, client *redis.Client, keys rediskey.AuthorityKeys,
	actorID, version, writerEpoch uint64) {
	t.Helper()
	address := actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: actorID}
	key, err := keys.ActorRecordFor(address)
	if err != nil {
		t.Fatal(err)
	}
	writeActor(t, client, key.String(), version, writerEpoch, &playerpb.PlayerRecord{})
	setShardEpochFor(t, sweep, client, keys, actorID, writerEpoch)
}

// writeActor stores a row the way the v1 layout does: the meta the fence uses,
// plus the whole record in one model field.
func writeActor(t *testing.T, client *redis.Client, key string, version, writerEpoch uint64, record *playerpb.PlayerRecord) {
	t.Helper()
	blob, err := proto.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), key, map[string]any{
		metaRecordVersion: version, metaWriterEpoch: writerEpoch,
		metaModelLayoutVersion: "1", modelRecordField: blob,
	}).Err(); err != nil {
		t.Fatal(err)
	}
}

// setShardEpochFor sets the fence of whichever shard owns the actor.
func setShardEpochFor(t *testing.T, sweep *ActorSweep, client *redis.Client, keys rediskey.AuthorityKeys,
	actorID, epoch uint64) {
	t.Helper()
	shard, err := sweep.shards.ShardFor(actorrouting.DomainPlayer, actorID)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := sweep.profile.AuthorityBucketForShard(sweep.shards, actorrouting.DomainPlayer, shard)
	if err != nil {
		t.Fatal(err)
	}
	key, err := keys.ShardAuthority(ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), key.String(), "writer_epoch", epoch).Err(); err != nil {
		t.Fatal(err)
	}
}

func observe(t *testing.T, sweep *ActorSweep) map[string]int64 {
	t.Helper()
	values, err := sweep.ObserveAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return values
}

// A version can only be seen to go backwards by comparing two readings, so the
// first one establishes the baseline and cannot itself be a finding.
func TestARegressionIsOnlyVisibleBetweenTwoReadings(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 7, 3)

	if got := observe(t, sweep)[RecordVersionRegressionGate]; got != 0 {
		t.Fatalf("regressions=%d on the first reading, want 0", got)
	}

	setActor(t, sweep, client, keys, actor, 5, 3)
	if got := observe(t, sweep)[RecordVersionRegressionGate]; got != 1 {
		t.Fatalf("regressions=%d after the version fell from 7 to 5, want 1", got)
	}
}

func TestOrdinaryProgressIsNotARegression(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 4, 2)
	observe(t, sweep)

	setActor(t, sweep, client, keys, actor, 9, 2)
	values := observe(t, sweep)
	if values[RecordVersionRegressionGate] != 0 || values[StaleHolderCommitGate] != 0 {
		t.Fatalf("a version that only moved forward under the current epoch is clean, got %+v", values)
	}
}

// A holder claims an actor before committing to it, and claiming stamps the
// current epoch. A record that advanced under an older epoch was therefore
// written by a generation that had already been superseded.
func TestARecordThatAdvancedUnderAnOldEpochIsAStaleCommit(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 4, 2)
	observe(t, sweep)

	setShardEpochFor(t, sweep, client, keys, actor, 3)
	setActorOnly(t, keys, client, actor, 5, 2)
	values := observe(t, sweep)
	if values[StaleHolderCommitGate] != 1 {
		t.Fatalf("stale commits=%d after an advance under epoch 2 while the shard is at 3, want 1",
			values[StaleHolderCommitGate])
	}
	if values[AuthorityClientMismatchGate] != values[StaleHolderCommitGate] {
		t.Fatalf("the mismatch gate is folded into the stale holder evidence, got %+v", values)
	}
}

// After a takeover every actor keeps the old epoch until the new holder claims
// it. That is the ordinary steady state, not a stale commit.
func TestAnUntouchedActorAfterTakeoverIsNotAStaleCommit(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 4, 2)
	observe(t, sweep)

	setShardEpochFor(t, sweep, client, keys, actor, 3)
	values := observe(t, sweep)
	if values[StaleHolderCommitGate] != 0 {
		t.Fatalf("stale commits=%d for an actor that never moved, want 0", values[StaleHolderCommitGate])
	}
}

func TestAnIdentityTheRunNeverReachedIsNotAFault(t *testing.T) {
	sweep, _, _ := newSweep(t, testLedger(4))
	values := observe(t, sweep)
	if values[RecordVersionRegressionGate] != 0 || values[StaleHolderCommitGate] != 0 {
		t.Fatalf("absent actors are not faults, got %+v", values)
	}
}

// A replay the guard caught is stored as NO_CHANGE. Only a second success is
// evidence that the same operation was applied twice.
func TestOnlyASecondSuccessIsADuplicateGrant(t *testing.T) {
	tests := []struct {
		name     string
		receipts []*playerpb.PlayerOperationReceipt
		want     int64
	}{
		{
			name: "applied twice",
			receipts: []*playerpb.PlayerOperationReceipt{
				{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
				{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
			},
			want: 1,
		},
		{
			name: "replay refused",
			receipts: []*playerpb.PlayerOperationReceipt{
				{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
				{OperationId: "op-1", Status: commonpb.CommandStatus_NO_CHANGE},
			},
			want: 0,
		},
		{
			name: "two different operations",
			receipts: []*playerpb.PlayerOperationReceipt{
				{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
				{OperationId: "op-2", Status: commonpb.CommandStatus_SUCCEEDED},
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger := testLedger(1)
			sweep, client, keys := newSweep(t, ledger)
			actor := ledger.ActorBase
			setActor(t, sweep, client, keys, actor, 1, 1)
			key, err := keys.ActorRecordFor(actorrouting.ActorAddress{
				Domain: actorrouting.DomainPlayer, ActorID: actor,
			})
			if err != nil {
				t.Fatal(err)
			}
			writeActor(t, client, key.String(), 1, 1,
				&playerpb.PlayerRecord{RecentOperationReceipts: tt.receipts})

			if got := observe(t, sweep)[DuplicateGrantGate]; got != tt.want {
				t.Fatalf("duplicate grants=%d for %s, want %d", got, tt.name, tt.want)
			}
		})
	}
}

// A debit reserves before it consumes, so a second live row under one
// reservation id is a second hold on the same funds. A released row is not.
func TestASecondLiveReservationIsADuplicateDebit(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 1, 1)
	key, err := keys.ActorRecordFor(actorrouting.ActorAddress{
		Domain: actorrouting.DomainPlayer, ActorID: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeActor(t, client, key.String(), 1, 1, &playerpb.PlayerRecord{
		ItemReservations: []*playerpb.ItemReservation{
			{ReservationId: "held-twice", State: playerpb.ReservationState_PREPARED},
			{ReservationId: "held-twice", State: playerpb.ReservationState_COMMITTED},
			{ReservationId: "released", State: playerpb.ReservationState_RELEASED},
			{ReservationId: "released", State: playerpb.ReservationState_COMMITTED},
		},
	})

	if got := observe(t, sweep)[DuplicateDebitGate]; got != 1 {
		t.Fatalf("duplicate debits=%d, want 1", got)
	}
}

// Duplicates sit in the record rather than happening between readings, so
// sweeping twice must not report the same fault twice.
func TestADuplicateIsNotCountedAgainOnTheNextSweep(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	setActor(t, sweep, client, keys, actor, 1, 1)
	key, err := keys.ActorRecordFor(actorrouting.ActorAddress{
		Domain: actorrouting.DomainPlayer, ActorID: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	writeActor(t, client, key.String(), 1, 1, &playerpb.PlayerRecord{
		RecentOperationReceipts: []*playerpb.PlayerOperationReceipt{
			{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
			{OperationId: "op-1", Status: commonpb.CommandStatus_SUCCEEDED},
		},
	})

	observe(t, sweep)
	if got := observe(t, sweep)[DuplicateGrantGate]; got != 1 {
		t.Fatalf("duplicate grants=%d on the second sweep, want the same 1", got)
	}
}

// An unreadable record is not an innocent one.
func TestARecordWithAnUnknownLayoutFailsTheSweep(t *testing.T) {
	ledger := testLedger(1)
	sweep, client, keys := newSweep(t, ledger)
	actor := ledger.ActorBase
	key, err := keys.ActorRecordFor(actorrouting.ActorAddress{
		Domain: actorrouting.DomainPlayer, ActorID: actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), key.String(), map[string]any{
		metaRecordVersion: 1, metaWriterEpoch: 1, metaModelLayoutVersion: "7",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := sweep.ObserveAll(context.Background()); err == nil {
		t.Fatal("a record this audit cannot decode must fail rather than pass")
	}
}

func TestARunTooLargeToSweepIsAFailure(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	sweep, err := NewActorSweep(ActorSweepConfig{
		Client: client, Profile: profile, Shards: shards, Ledger: testLedger(100), Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sweep.ObserveAll(context.Background()); err == nil {
		t.Fatal("sweeping part of a run would report a count that looks clean")
	}
}

func TestTheSweepNamesEveryGateItAnswers(t *testing.T) {
	sweep, _, _ := newSweep(t, testLedger(1))
	want := map[string]bool{
		"record_version_regression_total":   true,
		"stale_holder_commit_success_total": true,
		"authority_client_mismatch_total":   true,
		"duplicate_grant_total":             true,
		"duplicate_debit_total":             true,
	}
	for _, gate := range sweep.Gates() {
		if !want[gate] {
			t.Fatalf("unexpected gate %s", gate)
		}
		delete(want, gate)
	}
	if len(want) != 0 {
		t.Fatalf("gates not named: %v", want)
	}
}

func setActorOnly(t *testing.T, keys rediskey.AuthorityKeys, client *redis.Client, actorID, version, writerEpoch uint64) {
	t.Helper()
	key, err := keys.ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: actorID})
	if err != nil {
		t.Fatal(err)
	}
	writeActor(t, client, key.String(), version, writerEpoch, &playerpb.PlayerRecord{})
}
