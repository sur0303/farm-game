package friend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

func newFriendRedisTestRepositories(t *testing.T, maxFriends int) (*RedisFriendshipRepositories, *miniredis.Miniredis) {
	return newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: maxFriends})
}

func newFriendRedisTestRepositoriesWithLimits(t *testing.T, limits FriendRedisLimits) (*RedisFriendshipRepositories, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	namespace, err := rediskey.NewNamespace("test", "friend")
	if err != nil {
		t.Fatal(err)
	}
	options := RedisFriendshipOptions{Profile: DefaultFriendRedisProfile(namespace), Limits: limits}
	repositories, err := NewRedisFriendshipRepositories(redis.NewClient(&redis.Options{Addr: server.Addr(),
		PoolSize: 64, PoolTimeout: 30 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}), options)
	if err != nil {
		t.Fatal(err)
	}
	return repositories, server
}

func assertFriendSourceOrder(t *testing.T, ctx context.Context, repositories *RedisFriendshipRepositories,
	group friendRedisGroup, workType, workID string) map[string]string {
	t.Helper()
	key, err := repositories.authority.sourceOrderKey(group, workType, workID)
	if err != nil {
		t.Fatal(err)
	}
	fields := []string{"work_id", "work_type", "typed_payload", "work_ref", "payload_hash", "next_due_at_ms",
		"permit_id", "relation_id", "relation_epoch", "visitor_player_id", "owner_player_id", "target_farm_id",
		"operation_id", "scope", "permit_payload_hash", "target_binding_hash", "issued_at_authority_ms",
		"expire_at_authority_ms", "execution_accepted_at_authority_ms", "dispatch_order_id"}
	values, err := repositories.authority.readFields(ctx, key, fields...)
	if err != nil {
		t.Fatal(err)
	}
	if values["work_id"] != workID || values["work_type"] != workType {
		t.Fatalf("source identity mismatch: %v", values)
	}
	ref, err := workidentity.DecodeWorkRefV1(values["work_ref"])
	if err != nil || ref.WorkType != workType || ref.WorkID != workID {
		t.Fatalf("source work_ref=%q decoded=%+v err=%v", values["work_ref"], ref, err)
	}
	if values["payload_hash"] != hexHash([]byte(values["typed_payload"])) {
		t.Fatalf("source payload hash mismatch: %+v", values)
	}
	due, err := repositories.authority.keys.SourceDueIndex(group.ref)
	if err != nil {
		t.Fatal(err)
	}
	score, err := repositories.authority.client.ZScore(ctx, string(due), values["work_ref"]).Result()
	if err != nil || score != 0 || values["next_due_at_ms"] != "0" {
		t.Fatalf("source due mismatch: score=%v field=%q err=%v", score, values["next_due_at_ms"], err)
	}
	return values
}

func seedFriendSourceOrder(t *testing.T, ctx context.Context, repositories *RedisFriendshipRepositories,
	group friendRedisGroup, pairID string, order friendSourceOrder, omit string, dueScore *float64) string {
	t.Helper()
	key, err := repositories.authority.sourceOrderKey(group, order.workType, order.id)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]interface{}{
		"schema_version":                "friend-source-order-v1",
		"work_id":                       order.id,
		"work_type":                     order.workType,
		"placement_kind":                "SOURCE_PERSISTENCE_BUCKET",
		"bucket_version":                group.ref.LayoutVersion,
		"bucket_id":                     itoa(uint64(group.ref.BucketID)),
		"source_authority_kind":         "FRIEND_PAIR",
		"canonical_source_authority_id": pairID,
		"state":                         "PENDING",
		"work_version":                  "1",
		"typed_payload":                 order.blob,
		"work_ref":                      order.workRef,
		"payload_hash":                  order.payloadHash,
		"next_due_at_ms":                "0",
		"retry_count":                   "0",
		"lease_epoch":                   "0",
		"lease_holder_id":               "",
		"lease_token_hash":              "",
		"lease_expire_at_ms":            "0",
		"created_at_ms":                 "1",
		"updated_at_ms":                 "1",
		"completed_at_ms":               "0",
	}
	if omit != "" {
		delete(values, omit)
	}
	if err := repositories.authority.client.HSet(ctx, key, values).Err(); err != nil {
		t.Fatal(err)
	}
	if dueScore != nil {
		due, dueErr := repositories.authority.keys.SourceDueIndex(group.ref)
		if dueErr != nil {
			t.Fatal(dueErr)
		}
		if err := repositories.authority.client.ZAdd(ctx, string(due), redis.Z{Score: *dueScore, Member: order.workRef}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return key
}

func logicalBytesFromHash(t *testing.T, ctx context.Context, client redis.UniversalClient, key string) int {
	t.Helper()
	total := 0
	for _, field := range mustHKeys(t, ctx, client, key) {
		if field == "_meta:logical_bytes" {
			continue
		}
		value, err := client.HGet(ctx, key, field).Result()
		if err != nil {
			t.Fatal(err)
		}
		total += len(field) + len(value)
	}
	return total
}

func redisFriendHash(value string) []byte {
	sum := sha256.Sum256([]byte(value))
	return sum[:]
}

func redisFriendPublic(player uint64, command, operation string) CommitIdentity {
	return CommitIdentity{SubjectPlayerID: player, CommandType: command, OperationID: operation}
}

func mustHKeys(t *testing.T, ctx context.Context, client redis.UniversalClient, key string) []string {
	t.Helper()
	fields, err := client.HKeys(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func setFriendHashFieldKeepingLogicalBytes(t *testing.T, ctx context.Context, client redis.UniversalClient,
	key, field, value string) {
	t.Helper()
	oldValue, err := client.HGet(ctx, key, field).Result()
	if err != nil {
		t.Fatal(err)
	}
	oldLogicalText, err := client.HGet(ctx, key, "_meta:logical_bytes").Result()
	if err != nil {
		t.Fatal(err)
	}
	oldLogical, err := strconv.ParseUint(oldLogicalText, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	delta := int64(len(value) - len(oldValue))
	if delta < 0 && uint64(-delta) > oldLogical {
		t.Fatalf("logical bytes underflow: old=%d delta=%d", oldLogical, delta)
	}
	newLogical := uint64(int64(oldLogical) + delta)
	if err := client.HSet(ctx, key, field, value, "_meta:logical_bytes", strconv.FormatUint(newLogical, 10)).Err(); err != nil {
		t.Fatal(err)
	}
}

func rewriteFriendReceiptEpochForTest(t *testing.T, blob, epoch string) string {
	t.Helper()
	_, rest, err := readFriendLP([]byte(blob))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, err = readFriendLP(rest)
	if err != nil {
		t.Fatal(err)
	}
	prefixLength := len(blob) - len(rest)
	if !strings.HasPrefix(string(rest), friendReceiptVersion) {
		t.Fatalf("receipt version=%q", string(rest))
	}
	body := rest[len(friendReceiptVersion):]
	parts := make([]string, 10)
	for i := range parts {
		part, tail, readErr := readFriendTextLP(body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		parts[i] = string(part)
		body = tail
	}
	if len(body) != 0 {
		t.Fatal("receipt has trailing bytes")
	}
	parts[3] = epoch
	var builder strings.Builder
	builder.WriteString(blob[:prefixLength])
	builder.WriteString(friendReceiptVersion)
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func rewriteFriendResolutionEpochForTest(t *testing.T, value, epoch string) string {
	t.Helper()
	evidence, err := parseFriendResolutionEvidence(value)
	if err != nil {
		t.Fatal(err)
	}
	parts := []string{evidence.Status, evidence.AttemptID, evidence.CandidateHash, evidence.ResolutionID,
		itoa(evidence.Ordinal), strconv.FormatInt(evidence.ResolvedAtMs, 10), evidence.MarkerDigest,
		evidence.ReceiptField, evidence.ReceiptKind, epoch}
	var builder strings.Builder
	builder.WriteString("FRS3")
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func maxFriendTestInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func findShareRotateReceipt(t *testing.T, ctx context.Context, repositories *RedisFriendshipRepositories,
	authorityID, pairID, receiptID string) bool {
	t.Helper()
	group, err := repositories.authority.group(FriendShareIssuerAuthority, authorityID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	profile := repositories.authority.keys.Profile()
	for partition := uint32(0); partition < profile.ReceiptPartitionCount; partition++ {
		key, err := repositories.authority.keys.ReceiptPartition(group.ref, authorityID, partition)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range mustHKeys(t, ctx, repositories.authority.client, string(key)) {
			blob, err := repositories.authority.client.HGet(ctx, string(key), field).Result()
			if err != nil {
				t.Fatal(err)
			}
			outer, err := parseFriendReceipt([]byte(blob))
			if err != nil {
				t.Fatal(err)
			}
			if outer.Kind != "SHARE_REACTIVATE" {
				continue
			}
			rotate, err := parseFriendRotate(outer.Typed)
			if err != nil {
				t.Fatal(err)
			}
			if rotate.PairID == pairID && rotate.ReceiptID == receiptID {
				return true
			}
		}
	}
	return false
}

func TestRedisFriendAuthorityPairQuotaShareContract(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(11, 22)
	if err != nil {
		t.Fatal(err)
	}
	activateIdentity := CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "wr-activate", ParticipantStep: "ACTIVATE"}
	activated, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 11, RedeemerPlayerID: 22, TargetRelationEpoch: 1, TokenID: "token-1",
		TokenGeneration: 1, OperationID: "activate-op", NowMs: 1,
	}, activateIdentity, redisFriendHash("activate"))
	if err != nil || activated.Status != StatusSucceeded {
		t.Fatalf("activate: %+v %v", activated, err)
	}
	deleted, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{
		Identity: redisFriendPublic(11, "DELETE_FRIEND", "delete-1"), PayloadHash: redisFriendHash("delete"),
		CallerPlayerID: 11, PeerPlayerID: 22, NowMs: 2,
	})
	if err != nil || deleted.Status != StatusSucceeded || deleted.CommittedEpoch != 2 {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	deleteReplay, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{
		Identity: redisFriendPublic(11, "DELETE_FRIEND", "delete-1"), PayloadHash: redisFriendHash("delete"),
		CallerPlayerID: 11, PeerPlayerID: 22, NowMs: 999,
	})
	if err != nil || deleteReplay.CommittedAtMs != deleted.CommittedAtMs {
		t.Fatalf("delete replay: %+v %v", deleteReplay, err)
	}
	_, err = repositories.Pair.DeleteFriend(ctx, PairMutationCommand{
		Identity: redisFriendPublic(11, "DELETE_FRIEND", "delete-1"), PayloadHash: redisFriendHash("different"),
		CallerPlayerID: 11, PeerPlayerID: 22,
	})
	if !errors.Is(err, ErrIdempotentConflict) {
		t.Fatalf("expected pair conflict, got %v", err)
	}

	quota := repositories.Quota
	reservation := QuotaReservation{ReservationID: "res-1", WorkflowID: "wf-1", RelationID: pairID, TargetRelationEpoch: 1}
	reserved, err := quota.ReserveFriendSlot(ctx, 11, reservation, redisFriendPublic(11, "RESERVE", "reserve-1"), redisFriendHash("reserve"), 3)
	if err != nil || reserved.Status != StatusSucceeded {
		t.Fatalf("reserve: %+v %v", reserved, err)
	}
	finalized, err := quota.FinalizeFriendSlot(ctx, 11, reservation, redisFriendPublic(11, "FINALIZE", "finalize-1"), redisFriendHash("finalize"), 4)
	if err != nil || finalized.Status != StatusSucceeded {
		t.Fatalf("finalize: %+v %v", finalized, err)
	}
	removed, err := quota.RemoveActiveRelationIfEpochMatches(ctx, 11, pairID, 1, redisFriendPublic(11, "REMOVE", "remove-1"), redisFriendHash("remove"), 5)
	if err != nil || removed.Status != StatusSucceeded {
		t.Fatalf("remove: %+v %v", removed, err)
	}
	fenceID, err := QuotaReleaseFenceID(11, pairID, 1)
	if err != nil {
		t.Fatal(err)
	}
	// The fence field is digest-addressed in the Quota receipt partition;
	// storing the stable fence ID itself would violate 33A §2.5.
	fenceField := "relation-release-fence:" + friendFenceDigestHex(fenceID)
	fenceRef, err := repositories.authority.keys.BucketFor(FriendQuotaAuthority, "11")
	if err != nil {
		t.Fatal(err)
	}
	fenceDigest := friendDigestBytes(appendFriendLP(nil, fenceID))
	fencePartition, err := repositories.authority.keys.Receipt(fenceRef, "11", fenceDigest)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := repositories.authority.client.HExists(ctx, string(fencePartition), fenceField).Result(); err != nil || !got {
		t.Fatalf("release fence is not in digest partition key=%s err=%v", fencePartition, err)
	}
	late, err := quota.FinalizeFriendSlot(ctx, 11, reservation, redisFriendPublic(11, "FINALIZE", "finalize-2"), redisFriendHash("late"), 6)
	if err != nil || late.Status != StatusNoChange || late.Reason != ReasonRelationAlreadyTerminated {
		t.Fatalf("late finalize: %+v %v", late, err)
	}
	reservation2 := QuotaReservation{ReservationID: "res-2", WorkflowID: "wf-2", RelationID: pairID + "-2", TargetRelationEpoch: 1}
	if outcome, err := quota.ReserveFriendSlot(ctx, 11, reservation2, redisFriendPublic(11, "RESERVE", "reserve-2"), redisFriendHash("reserve-2"), 7); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve for release: %+v %v", outcome, err)
	}
	if outcome, err := quota.ReleaseFriendReservation(ctx, 11, reservation2.ReservationID, redisFriendPublic(11, "RELEASE", "release-2"), redisFriendHash("release-2"), 8); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("release: %+v %v", outcome, err)
	}

	share := repositories.Share
	token, outcome, err := share.GenerateShareToken(ctx, 11, "share-token-1", redisFriendPublic(11, "GENERATE_SHARE", "share-1"), redisFriendHash("share"), 7)
	if err != nil || outcome.Status != StatusSucceeded || token.TokenGeneration != 1 {
		t.Fatalf("share generate: %+v %+v %v", token, outcome, err)
	}
	for redeemer := uint64(22); redeemer < 122; redeemer++ {
		receipt, admission, admissionErr := share.AcceptShareTokenAdmission(ctx, 11, token.TokenID, token.TokenGeneration, redeemer,
			"wf-"+itoa(redeemer), redisFriendPublic(redeemer, "ACCEPT_SHARE", "accept-"+itoa(redeemer)), redisFriendHash("accept-"+itoa(redeemer)), 8)
		if admissionErr != nil || admission.Status != StatusSucceeded || receipt.RedeemerPlayerID != redeemer {
			t.Fatalf("share admission redeemer=%d: %+v %+v %v", redeemer, receipt, admission, admissionErr)
		}
	}
	issuer, err := share.GetIssuer(ctx, 11)
	if err != nil || issuer.Current.TokenGeneration != 1 {
		t.Fatalf("issuer: %+v %v", issuer, err)
	}
	if len(issuer.RotateReceipts) != 0 {
		t.Fatalf("unexpected rotate receipts: %+v", issuer.RotateReceipts)
	}
	shareGroup, err := repositories.authority.group(FriendShareIssuerAuthority, "11", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	receiptDue, err := repositories.authority.keys.ReceiptDueIndex(shareGroup.ref)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := repositories.authority.client.ZCard(ctx, string(receiptDue)).Result(); err != nil || count < 101 {
		t.Fatalf("share receipt due metadata count=%d err=%v", count, err)
	}
	validationReceipts := 0
	profile := repositories.authority.keys.Profile()
	for partition := uint32(0); partition < profile.ReceiptPartitionCount; partition++ {
		partitionKey, err := repositories.authority.keys.ReceiptPartition(shareGroup.ref, "11", partition)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range mustHKeys(t, ctx, repositories.authority.client, string(partitionKey)) {
			blob, err := repositories.authority.client.HGet(ctx, string(partitionKey), field).Result()
			if err != nil {
				t.Fatal(err)
			}
			outer, err := parseFriendReceipt([]byte(blob))
			if err != nil {
				t.Fatal(err)
			}
			if outer.Kind == "SHARE_ACCEPT" {
				validationReceipts++
			}
		}
	}
	if validationReceipts != 100 {
		t.Fatalf("validation receipts=%d want 100", validationReceipts)
	}
	shareFields, err := repositories.authority.readFields(ctx, shareGroup.record,
		"issuer:player_id", "token:current_generation", "token:current_token_id", "token:status", "token:purpose",
		"token:format_version", "token:key_id", "token:issued_at_ms", "token:expires_at_ms", "token:digest", "token:envelope")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"issuer:player_id", "token:current_generation", "token:current_token_id", "token:status",
		"token:purpose", "token:format_version", "token:key_id", "token:issued_at_ms", "token:expires_at_ms", "token:digest", "token:envelope"} {
		if shareFields[field] == "" {
			t.Fatalf("share canonical field missing: %s fields=%v", field, shareFields)
		}
	}
	if shareFields["token:digest"] != hexHash([]byte(token.TokenID)) {
		t.Fatalf("share token digest mismatch: %q", shareFields["token:digest"])
	}
	rotated, outcome, err := share.RotateShareToken(ctx, 11, "share-token-2", redisFriendPublic(11, "ROTATE_SHARE", "share-rotate"), redisFriendHash("rotate"), 9)
	if err != nil || outcome.Status != StatusSucceeded || rotated.TokenGeneration != 2 {
		t.Fatalf("share rotate: %+v %+v %v", rotated, outcome, err)
	}
	revoked, outcome, err := share.RevokeShareToken(ctx, 11, redisFriendPublic(11, "REVOKE_SHARE", "share-revoke"), redisFriendHash("revoke"), 10)
	if err != nil || outcome.Status != StatusSucceeded || revoked.Status != TokenStatusRevoked || revoked.TokenGeneration != 3 {
		t.Fatalf("share revoke: %+v %+v %v", revoked, outcome, err)
	}
}

type pairBatchRedisHook struct {
	commands   atomic.Uint64
	roundTrips atomic.Uint64
}

func (h *pairBatchRedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *pairBatchRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		h.roundTrips.Add(1)
		return next(ctx, cmd)
	}
}
func (h *pairBatchRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.commands.Add(uint64(len(cmds)))
		h.roundTrips.Add(1)
		return next(ctx, cmds)
	}
}

func TestRedisPairBatchMatchesCanonicalSingleReads(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	const viewer = uint64(501)
	for _, peer := range []uint64{502, 503} {
		pairID, _, _, err := pairIDForPlayers(viewer, peer)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
			InviterPlayerID: viewer, RedeemerPlayerID: peer, TargetRelationEpoch: 1,
			TokenID: "batch-token-" + itoa(peer), TokenGeneration: 1, OperationID: "batch-activate-" + itoa(peer),
		}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "batch-invoke-" + itoa(peer), ParticipantStep: "ACTIVATE"}, redisFriendHash("batch-"+itoa(peer)))
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("peer=%d outcome=%+v err=%v", peer, outcome, err)
		}
	}
	peers := []uint64{503, 504, 502}
	uniqueBuckets := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		pairID, _, _, _ := pairIDForPlayers(viewer, peer)
		group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		uniqueBuckets[group.meta] = struct{}{}
	}
	hook := &pairBatchRedisHook{}
	repositories.authority.client.AddHook(hook)
	beforeRoundTrips := hook.roundTrips.Load()
	beforeCommands := hook.commands.Load()
	batch, err := repositories.Pair.BatchGetPairs(ctx, viewer, peers)
	batchRoundTrips := hook.roundTrips.Load() - beforeRoundTrips
	batchCommands := hook.commands.Load() - beforeCommands
	if err != nil || len(batch) != len(peers) {
		t.Fatalf("batch=%v err=%v", batch, err)
	}
	if batchRoundTrips != 1 || batchCommands != uint64(len(uniqueBuckets)) {
		t.Fatalf("physical batch roundTrips=%d commands=%d uniqueBuckets=%d", batchRoundTrips, batchCommands, len(uniqueBuckets))
	}
	for index, peer := range peers {
		single, err := repositories.Pair.GetPair(ctx, viewer, peer)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(batch[index], single) {
			t.Fatalf("peer=%d batch=%+v single=%+v", peer, batch[index], single)
		}
	}
}

func TestRedisPairBatchUsesOneAtomicEnvelopePerPairInOneRoundTrip(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	const viewer = uint64(701)
	byMeta := make(map[string]uint64)
	var peers []uint64
	for peer := uint64(702); peer < 10_000 && len(peers) == 0; peer++ {
		pairID, _, _, err := pairIDForPlayers(viewer, peer)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		if first, ok := byMeta[group.meta]; ok {
			peers = []uint64{first, peer}
			break
		}
		byMeta[group.meta] = peer
	}
	if len(peers) != 2 {
		t.Fatal("could not find two pair ids in one authority bucket")
	}
	for _, peer := range peers {
		pairID, _, _, _ := pairIDForPlayers(viewer, peer)
		outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
			InviterPlayerID: viewer, RedeemerPlayerID: peer, TargetRelationEpoch: 1,
			TokenID: "shared-token-" + itoa(peer), TokenGeneration: 1, OperationID: "shared-activate-" + itoa(peer),
		}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "shared-invoke-" + itoa(peer), ParticipantStep: "ACTIVATE"}, redisFriendHash("shared-"+itoa(peer)))
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("peer=%d outcome=%+v err=%v", peer, outcome, err)
		}
	}
	hook := &pairBatchRedisHook{}
	repositories.authority.client.AddHook(hook)
	if _, err := repositories.Pair.BatchGetPairs(ctx, viewer, peers); err != nil {
		t.Fatal(err)
	}
	// Both requested Pairs share one authority bucket, so the complete batch
	// snapshot is exactly one Redis script in one physical round trip.
	wantCommands := uint64(1)
	if hook.roundTrips.Load() != 1 || hook.commands.Load() != wantCommands {
		t.Fatalf("roundTrips=%d commands=%d wantCommands=%d", hook.roundTrips.Load(), hook.commands.Load(), wantCommands)
	}
}

func TestRedisPairBatchMetricsAccountEveryQueuedEnvelopeOnFailure(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	const viewer = uint64(750)
	var peers []uint64
	seenBuckets := map[uint32]struct{}{}
	var corruptGroup friendRedisGroup
	for peer := uint64(751); peer < 10_000 && len(peers) < 3; peer++ {
		pairID, _, _, err := pairIDForPlayers(viewer, peer)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seenBuckets[group.ref.BucketID]; exists {
			continue
		}
		seenBuckets[group.ref.BucketID] = struct{}{}
		peers = append(peers, peer)
		if len(peers) == 2 {
			corruptGroup = group
		}
	}
	if len(peers) != 3 {
		t.Fatal("could not find three distinct Pair buckets")
	}
	if err := repositories.authority.client.Set(ctx, corruptGroup.meta, "corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Pair.BatchGetPairs(ctx, viewer, peers); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("batch err=%v want ErrFriendReconcile", err)
	}
	metrics := repositories.EnvelopeReadMetrics()
	if metrics.AtomicReadTotal != 3 || metrics.AbsentTotal != 2 || metrics.PresentTotal != 0 ||
		metrics.ReconcileTotal != 1 || metrics.TechnicalFailureTotal != 0 ||
		metrics.AtomicReadTotal != metrics.PresentTotal+metrics.AbsentTotal+metrics.ReconcileTotal+metrics.TechnicalFailureTotal {
		t.Fatalf("batch failure metrics=%+v", metrics)
	}
}

func TestRedisPairBatchCorruptionMatchesSingleReadFailClosed(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		corrupt func(context.Context, *RedisFriendshipRepositories, string) error
	}{
		{name: "missing canonical field", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, key string) error {
			return repositories.authority.client.HDel(ctx, key, "relation:epoch").Err()
		}},
		{name: "wrong canonical key type", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, key string) error {
			if err := repositories.authority.client.Del(ctx, key).Err(); err != nil {
				return err
			}
			return repositories.authority.client.Set(ctx, key, "corrupt", 0).Err()
		}},
		{name: "wrong bucket meta key type", corrupt: func(ctx context.Context, repositories *RedisFriendshipRepositories, _ string) error {
			pairID, _, _, err := pairIDForPlayers(801, 802)
			if err != nil {
				return err
			}
			group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
			if err != nil {
				return err
			}
			if err := repositories.authority.client.Del(ctx, group.meta).Err(); err != nil {
				return err
			}
			return repositories.authority.client.Set(ctx, group.meta, "corrupt", 0).Err()
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			const viewer, peer = uint64(801), uint64(802)
			pairID, key, _, _, err := repositories.Pair.recordKey(viewer, peer)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
				InviterPlayerID: viewer, RedeemerPlayerID: peer, TargetRelationEpoch: 1,
				TokenID: "corrupt-token", TokenGeneration: 1, OperationID: "corrupt-activate",
			}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "corrupt-invoke", ParticipantStep: "ACTIVATE"}, redisFriendHash("corrupt"))
			if err != nil || outcome.Status != StatusSucceeded {
				t.Fatalf("setup outcome=%+v err=%v", outcome, err)
			}
			if err := testCase.corrupt(ctx, repositories, key); err != nil {
				t.Fatal(err)
			}
			_, singleErr := repositories.Pair.GetPair(ctx, viewer, peer)
			_, batchErr := repositories.Pair.BatchGetPairs(ctx, viewer, []uint64{peer})
			if !errors.Is(singleErr, ErrFriendReconcile) || !errors.Is(batchErr, ErrFriendReconcile) {
				t.Fatalf("singleErr=%v batchErr=%v", singleErr, batchErr)
			}
		})
	}
}

func TestRedisFriendQuotaConcurrentHardLimit(t *testing.T) {
	const quotaLimit = 30
	repositories, server := newFriendRedisTestRepositories(t, quotaLimit)
	defer server.Close()
	ctx := context.Background()
	const attempts = 40
	var wg sync.WaitGroup
	results := make(chan Outcome, attempts)
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			reservation := QuotaReservation{ReservationID: "res-" + itoa(uint64(index)), WorkflowID: "wf-" + itoa(uint64(index)),
				RelationID: "relation-" + itoa(uint64(index)), TargetRelationEpoch: 1}
			outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 77, reservation,
				redisFriendPublic(77, "RESERVE", "reserve-"+itoa(uint64(index))), redisFriendHash("quota-"+itoa(uint64(index))), 10)
			if err != nil {
				errs <- err
				return
			}
			results <- outcome
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	snapshot, err := repositories.Quota.GetQuota(ctx, 77)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Occupancy() != quotaLimit || snapshot.OpenReservations != quotaLimit {
		t.Fatalf("quota exceeded: %+v", snapshot)
	}
	rejected := 0
	for outcome := range results {
		if outcome.Status == StatusRejected && outcome.Reason == ReasonFriendLimitReached {
			rejected++
		}
	}
	if rejected != attempts-quotaLimit {
		t.Fatalf("rejections=%d want=%d", rejected, attempts-quotaLimit)
	}
}

func TestRedisFriendCapacityCheckedBeforeCanonicalMutation(t *testing.T) {
	ctx := context.Background()
	quotaRepositories, quotaServer := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: 300, MaxRecordBytes: 1})
	defer quotaServer.Close()
	reservation := QuotaReservation{ReservationID: "capacity-res", WorkflowID: "capacity-wf", RelationID: "capacity-rel", TargetRelationEpoch: 1}
	if _, err := quotaRepositories.Quota.ReserveFriendSlot(ctx, 601, reservation,
		redisFriendPublic(601, "RESERVE", "capacity-quota"), redisFriendHash("capacity-quota"), 1); !errors.Is(err, ErrFriendAuthorityCapacity) {
		t.Fatalf("quota capacity error=%v", err)
	}
	quotaSnapshot, err := quotaRepositories.Quota.GetQuota(ctx, 601)
	if err != nil || quotaSnapshot.OpenReservations != 0 || len(quotaSnapshot.ActiveRelations) != 0 {
		t.Fatalf("quota mutated before capacity check: %+v err=%v", quotaSnapshot, err)
	}
	quotaGroup, err := quotaRepositories.authority.group(FriendQuotaAuthority, "601", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := quotaRepositories.authority.client.HExists(ctx, quotaGroup.record, "reservation:"+reservation.ReservationID+":state").Result(); err != nil || exists {
		t.Fatalf("quota reservation leaked on capacity: exists=%v err=%v", exists, err)
	}

	shareRepositories, shareServer := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: 300, MaxRecordBytes: 1})
	defer shareServer.Close()
	if _, _, err := shareRepositories.Share.GenerateShareToken(ctx, 602, "capacity-token",
		redisFriendPublic(602, "GENERATE_SHARE", "capacity-share"), redisFriendHash("capacity-share"), 1); !errors.Is(err, ErrFriendAuthorityCapacity) {
		t.Fatalf("share capacity error=%v", err)
	}
	issuer, err := shareRepositories.Share.GetIssuer(ctx, 602)
	if err != nil || issuer.Current.TokenID != "" {
		t.Fatalf("share mutated before capacity check: %+v err=%v", issuer, err)
	}

	pairRepositories, pairServer := newFriendRedisTestRepositories(t, 300)
	defer pairServer.Close()
	pairID, _, _, err := pairIDForPlayers(603, 604)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := pairRepositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 603, RedeemerPlayerID: 604, TargetRelationEpoch: 1, TokenID: "capacity-pair-token",
		TokenGeneration: 1, OperationID: "capacity-pair-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "capacity-pair-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("capacity-pair-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("pair setup: %+v %v", outcome, err)
	}
	pairRepositories.authority.limits.MaxRecordBytes = 1
	if _, err := pairRepositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(603, "BLOCK_PLAYER", "capacity-pair-block"),
		PayloadHash: redisFriendHash("capacity-pair-block"), CallerPlayerID: 603, PeerPlayerID: 604}); !errors.Is(err, ErrFriendAuthorityCapacity) {
		t.Fatalf("pair capacity error=%v", err)
	}
	pairSnapshot, err := pairRepositories.Pair.GetPair(ctx, 603, 604)
	if err != nil || pairSnapshot.Relation.Status != RelationStatusActive || pairSnapshot.HasActiveBlock() {
		t.Fatalf("pair mutated before capacity check: %+v err=%v", pairSnapshot, err)
	}
}

func TestRedisFriendPerItemAndScriptLimitsRejectBeforePrepare(t *testing.T) {
	cases := []struct {
		name   string
		limits FriendRedisLimits
		opLen  int
	}{
		{name: "source order", limits: FriendRedisLimits{MaxFriends: 300, MaxSourceOrderBytes: 128}, opLen: 1024},
		{name: "receipt", limits: FriendRedisLimits{MaxFriends: 300, MaxSingleReceiptBytes: 200}, opLen: 400},
		{name: "marker", limits: FriendRedisLimits{MaxFriends: 300, MaxAttemptBytes: 4095}, opLen: 1},
		{name: "argv", limits: FriendRedisLimits{MaxFriends: 300, MaxScriptInputBytes: 128}, opLen: 1024},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositoriesWithLimits(t, testCase.limits)
			defer server.Close()
			ctx := context.Background()
			low, high := uint64(1701+index*2), uint64(1702+index*2)
			pairID, _, _, err := pairIDForPlayers(low, high)
			if err != nil {
				t.Fatal(err)
			}
			identity := redisFriendPublic(low, "BLOCK_PLAYER", strings.Repeat("operation", testCase.opLen))
			payload := redisFriendHash(testCase.name)
			if _, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
				CallerPlayerID: low, PeerPlayerID: high}); !errors.Is(err, ErrFriendAuthorityCapacity) {
				t.Fatalf("limit error=%v, want capacity", err)
			}
			ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
			if err != nil {
				t.Fatal(err)
			}
			group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
			if err != nil {
				t.Fatal(err)
			}
			if exists, err := repositories.authority.client.Exists(ctx, group.record).Result(); err != nil || exists != 0 {
				t.Fatalf("limit rejection mutated pair record: exists=%d err=%v", exists, err)
			}
			if exists, err := repositories.authority.client.HExists(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || exists {
				t.Fatalf("limit rejection left Prepare cursor: exists=%v err=%v", exists, err)
			}
		})
	}
}

func TestRedisFriendMalformedMetaAndPartialMarkerFailClosed(t *testing.T) {
	ctx := context.Background()
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()

	reservation := QuotaReservation{ReservationID: "malformed-meta-res", WorkflowID: "malformed-meta-wf",
		RelationID: "malformed-meta-rel", TargetRelationEpoch: 1}
	identity := redisFriendPublic(1201, "RESERVE", "malformed-meta")
	payload := redisFriendHash("malformed-meta")
	group, err := repositories.authority.group(FriendQuotaAuthority, "1201", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HSet(ctx, group.meta,
		"schema_version", "friend-auth-v1", "authority_kind", string(FriendQuotaAuthority),
		"layout_version", group.ref.LayoutVersion, "bucket_id", group.ref.BucketID,
		"receipt_count", "not-a-counter", "receipt_bytes", "0", "pending_work_count", "0",
		"pending_work_bytes", "0").Err(); err != nil {
		t.Fatal(err)
	}
	malformedCI, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1201", identity, payload,
		"RESERVE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	malformedGroup, err := repositories.authority.group(FriendQuotaAuthority, "1201", malformedCI.digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1201, reservation, identity, payload, 1); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("malformed counter error=%v, want reconcile", err)
	}
	if exists, err := repositories.authority.client.Exists(ctx, malformedGroup.record).Result(); err != nil || exists != 0 {
		t.Fatalf("malformed counter created canonical record: exists=%d err=%v", exists, err)
	}
	if cursor, err := repositories.authority.client.HExists(ctx, malformedGroup.attempt, "cursor:"+malformedCI.digestHex+":state").Result(); err != nil {
		t.Fatal(err)
	} else if cursor {
		t.Fatal("malformed counter left a cursor")
	}

	markerRepositories, markerServer := newFriendRedisTestRepositories(t, 300)
	defer markerServer.Close()
	markerReservation := QuotaReservation{ReservationID: "marker-res", WorkflowID: "marker-wf", RelationID: "marker-rel", TargetRelationEpoch: 1}
	markerIdentity := redisFriendPublic(1202, "RESERVE", "marker-partial")
	markerPayload := redisFriendHash("marker-partial")
	if outcome, err := markerRepositories.Quota.ReserveFriendSlot(ctx, 1202, markerReservation, markerIdentity, markerPayload, 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("marker setup: %+v %v", outcome, err)
	}
	markerSnapshot, err := markerRepositories.Quota.GetQuota(ctx, 1202)
	if err != nil {
		t.Fatal(err)
	}
	markerCI, err := markerRepositories.authority.commitIdentity(FriendQuotaAuthority, "1202", markerIdentity, markerPayload,
		"RESERVE", markerReservation.ReservationID, markerReservation.WorkflowID, markerReservation.RelationID, "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	if markerSnapshot.RecordVersion != 1 {
		t.Fatalf("unexpected marker setup version=%d", markerSnapshot.RecordVersion)
	}
	markerGroup, err := markerRepositories.authority.group(FriendQuotaAuthority, "1202", markerCI.digest)
	if err != nil {
		t.Fatal(err)
	}
	markerAttemptID, err := markerRepositories.authority.client.HGet(ctx, markerGroup.attempt,
		"cursor:"+markerCI.digestHex+":attempt_id").Result()
	if err != nil {
		t.Fatal(err)
	}
	markerDigest := friendCommitMarkerDigest(markerCI.candidateHash, 1)
	if err := markerRepositories.authority.client.HDel(ctx, markerGroup.attempt, "commit_marker:"+markerDigest+":complete").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := markerRepositories.authority.resolveUnknown(ctx, markerGroup, FriendQuotaAuthority, "1202", markerCI, 1,
		markerAttemptID, friendAttemptDueMember(FriendQuotaAuthority, "1202", markerCI.digestHex, 1)); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("partial marker resolution=%v, want reconcile", err)
	}
}

func TestRedisFriendMalformedAttemptRejectedBeforeAuthorityMutation(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(1251, "RESERVE", "malformed-attempt")
	payload := redisFriendHash("malformed-attempt")
	reservation := QuotaReservation{ReservationID: "malformed-attempt-res", WorkflowID: "malformed-attempt-wf",
		RelationID: "malformed-attempt-rel", TargetRelationEpoch: 1}
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1251", identity, payload)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1251", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	partialField := "cursor:" + ci.digestHex + ":schema_version"
	if err := repositories.authority.client.HSet(ctx, group.attempt, partialField, "friend-commit-attempt-v1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1251, reservation, identity, payload, 1); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("malformed attempt error=%v, want reconcile", err)
	}
	if count, err := repositories.authority.client.HLen(ctx, group.attempt).Result(); err != nil || count != 1 {
		t.Fatalf("malformed attempt was partially rewritten: count=%d err=%v", count, err)
	}
	if exists, err := repositories.authority.client.Exists(ctx, group.record, group.meta, group.attemptDue).Result(); err != nil || exists != 0 {
		t.Fatalf("malformed attempt mutated authority keys: exists=%d err=%v", exists, err)
	}
}

func TestRedisFriendMalformedBucketMetadataRejectedBeforeAttempt(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(1252, "RESERVE", "malformed-bucket-meta")
	payload := redisFriendHash("malformed-bucket-meta")
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1252", identity, payload)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1252", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HSet(ctx, group.meta,
		"schema_version", "wrong-schema", "authority_kind", string(FriendQuotaAuthority),
		"layout_version", group.ref.LayoutVersion, "bucket_id", group.ref.BucketID,
		"receipt_count", 0, "receipt_bytes", 0, "pending_work_count", 0, "pending_work_bytes", 0,
		"attempt_count", 0, "attempt_bytes", 0, "last_updated_at_ms", 0).Err(); err != nil {
		t.Fatal(err)
	}
	reservation := QuotaReservation{ReservationID: "malformed-bucket-res", WorkflowID: "malformed-bucket-wf",
		RelationID: "malformed-bucket-rel", TargetRelationEpoch: 1}
	if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1252, reservation, identity, payload, 1); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("malformed bucket metadata error=%v, want reconcile", err)
	}
	if exists, err := repositories.authority.client.Exists(ctx, group.record, group.attempt, group.attemptDue).Result(); err != nil || exists != 0 {
		t.Fatalf("malformed bucket metadata mutated authority keys: exists=%d err=%v", exists, err)
	}
}

func TestRedisFriendCanonicalCorruptionFailsClosedButAbsentBootstrapSucceeds(t *testing.T) {
	t.Run("pair", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		pairID, _, _, err := pairIDForPlayers(1261, 1262)
		if err != nil {
			t.Fatal(err)
		}
		identity := CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "corrupt-pair", ParticipantStep: "ACTIVATE"}
		ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, redisFriendHash("corrupt-pair"))
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.HSet(ctx, group.record, "_meta:schema_version", "friend-pair-v1").Err(); err != nil {
			t.Fatal(err)
		}
		command := ActivateCommand{InviterPlayerID: 1261, RedeemerPlayerID: 1262, TargetRelationEpoch: 1,
			TokenID: "canonical-pair-token", TokenGeneration: 1, OperationID: "canonical-pair-activate"}
		if _, err := repositories.Pair.ActivateFriendRelation(ctx, command, identity, redisFriendHash("corrupt-pair")); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("corrupt pair error=%v", err)
		}
		if cursor, err := repositories.authority.client.HExists(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || cursor {
			t.Fatalf("corrupt pair wrote cursor: %v %v", cursor, err)
		}
		if err := repositories.authority.client.Del(ctx, group.record).Err(); err != nil {
			t.Fatal(err)
		}
		identity.ParticipantInvocationID = "absent-pair"
		if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, command, identity, redisFriendHash("absent-pair")); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("absent pair bootstrap: %+v %v", outcome, err)
		}
	})

	t.Run("quota", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		identity := redisFriendPublic(1271, "RESERVE", "canonical-quota-corrupt")
		payload := redisFriendHash("canonical-quota-corrupt")
		ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1271", identity, payload)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1271", ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.HSet(ctx, group.record, "_meta:schema_version", "friend-quota-v1").Err(); err != nil {
			t.Fatal(err)
		}
		reservation := QuotaReservation{ReservationID: "canonical-quota-res", WorkflowID: "canonical-quota-wf",
			RelationID: "canonical-quota-rel", TargetRelationEpoch: 1}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1271, reservation, identity, payload, 1); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("corrupt quota error=%v", err)
		}
		if cursor, err := repositories.authority.client.HExists(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || cursor {
			t.Fatalf("corrupt quota wrote cursor: %v %v", cursor, err)
		}
		if err := repositories.authority.client.Del(ctx, group.record).Err(); err != nil {
			t.Fatal(err)
		}
		identity.OperationID = "canonical-quota-absent"
		if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1271, reservation, identity, redisFriendHash("canonical-quota-absent"), 2); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("absent quota bootstrap: %+v %v", outcome, err)
		}
	})

	t.Run("share", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		identity := redisFriendPublic(1281, "GENERATE_SHARE", "canonical-share-corrupt")
		payload := redisFriendHash("canonical-share-corrupt")
		ci, err := repositories.authority.commitIdentity(FriendShareIssuerAuthority, "1281", identity, payload)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendShareIssuerAuthority, "1281", ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.HSet(ctx, group.record, "_meta:schema_version", "friend-share-issuer-v1").Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := repositories.Share.GenerateShareToken(ctx, 1281, "canonical-share-token", identity, payload, 1); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("corrupt share error=%v", err)
		}
		if cursor, err := repositories.authority.client.HExists(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || cursor {
			t.Fatalf("corrupt share wrote cursor: %v %v", cursor, err)
		}
		if err := repositories.authority.client.Del(ctx, group.record).Err(); err != nil {
			t.Fatal(err)
		}
		identity.OperationID = "canonical-share-absent"
		if token, outcome, err := repositories.Share.GenerateShareToken(ctx, 1281, "canonical-share-token", identity,
			redisFriendHash("canonical-share-absent"), 2); err != nil || outcome.Status != StatusSucceeded || token.TokenID == "" {
			t.Fatalf("absent share bootstrap: %+v %+v %v", token, outcome, err)
		}
	})
}

func TestRedisFriendCanonicalBucketMetaCorruptionFailsReadsWritesAndProofsClosed(t *testing.T) {
	t.Run("pair extra meta and canonical fields", func(t *testing.T) {
		for _, target := range []string{"meta", "record"} {
			t.Run(target, func(t *testing.T) {
				repositories, server := newFriendRedisTestRepositories(t, 300)
				defer server.Close()
				ctx := context.Background()
				pairID, _, _, err := pairIDForPlayers(1283, 1284)
				if err != nil {
					t.Fatal(err)
				}
				if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
					InviterPlayerID: 1283, RedeemerPlayerID: 1284, TargetRelationEpoch: 1, TokenID: "meta-pair-token",
					TokenGeneration: 1, OperationID: "meta-pair-activate",
				}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "meta-pair-activate", ParticipantStep: "ACTIVATE"},
					redisFriendHash("meta-pair-activate")); err != nil || outcome.Status != StatusSucceeded {
					t.Fatalf("activate: %+v %v", outcome, err)
				}
				group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
				if err != nil {
					t.Fatal(err)
				}
				version, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
				if err != nil {
					t.Fatal(err)
				}
				key := group.meta
				if target == "record" {
					key = group.record
				}
				if err := repositories.authority.client.HSet(ctx, key, "unexpected", "field").Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := repositories.Pair.GetPair(ctx, 1283, 1284); !errors.Is(err, ErrFriendReconcile) {
					t.Fatalf("corrupt %s read error=%v", target, err)
				}
				if _, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(1283, "BLOCK_PLAYER", "meta-pair-block"),
					PayloadHash: redisFriendHash("meta-pair-block"), CallerPlayerID: 1283, PeerPlayerID: 1284}); !errors.Is(err, ErrFriendReconcile) {
					t.Fatalf("corrupt %s write error=%v", target, err)
				}
				afterVersion, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
				if err != nil || afterVersion != version {
					t.Fatalf("corrupt %s changed Pair version before=%s after=%s err=%v", target, version, afterVersion, err)
				}
			})
		}
	})

	t.Run("quota existing record missing meta", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		reservation := QuotaReservation{ReservationID: "missing-meta-res", WorkflowID: "missing-meta-wf",
			RelationID: "missing-meta-rel", TargetRelationEpoch: 1}
		if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1285, reservation,
			redisFriendPublic(1285, "RESERVE", "missing-meta-reserve"), redisFriendHash("missing-meta-reserve"), 1); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("reserve: %+v %v", outcome, err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1285", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		version, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.Del(ctx, group.meta).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Quota.GetQuota(ctx, 1285); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("missing meta read error=%v", err)
		}
		if _, err := repositories.Quota.ReleaseFriendReservation(ctx, 1285, reservation.ReservationID,
			redisFriendPublic(1285, "RELEASE", "missing-meta-release"), redisFriendHash("missing-meta-release"), 2); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("missing meta write error=%v", err)
		}
		afterVersion, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
		if err != nil || afterVersion != version {
			t.Fatalf("missing meta changed Quota version before=%s after=%s err=%v", version, afterVersion, err)
		}
	})

	t.Run("share missing authority field and forged count", func(t *testing.T) {
		for _, corruption := range []string{"missing authority", "forged count"} {
			t.Run(corruption, func(t *testing.T) {
				repositories, server := newFriendRedisTestRepositories(t, 300)
				defer server.Close()
				ctx := context.Background()
				if _, outcome, err := repositories.Share.GenerateShareToken(ctx, 1286, "meta-share-token",
					redisFriendPublic(1286, "GENERATE_SHARE", "meta-share-generate"), redisFriendHash("meta-share-generate"), 1); err != nil || outcome.Status != StatusSucceeded {
					t.Fatalf("generate: %+v %v", outcome, err)
				}
				group, err := repositories.authority.group(FriendShareIssuerAuthority, "1286", [32]byte{})
				if err != nil {
					t.Fatal(err)
				}
				version, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
				if err != nil {
					t.Fatal(err)
				}
				if corruption == "missing authority" {
					err = repositories.authority.client.HDel(ctx, group.meta, "authority_kind").Err()
				} else {
					err = repositories.authority.client.HSet(ctx, group.meta, "receipt_count", "0").Err()
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := repositories.Share.GetIssuer(ctx, 1286); !errors.Is(err, ErrFriendReconcile) {
					t.Fatalf("%s read error=%v", corruption, err)
				}
				if _, _, err := repositories.Share.RotateShareToken(ctx, 1286, "meta-share-rotated",
					redisFriendPublic(1286, "ROTATE_SHARE", "meta-share-rotate"), redisFriendHash("meta-share-rotate"), 2); !errors.Is(err, ErrFriendReconcile) {
					t.Fatalf("%s write error=%v", corruption, err)
				}
				afterVersion, err := repositories.authority.client.HGet(ctx, group.record, "_meta:record_version").Result()
				if err != nil || afterVersion != version {
					t.Fatalf("%s changed Share version before=%s after=%s err=%v", corruption, version, afterVersion, err)
				}
			})
		}
	})

	t.Run("reactivation quota proof validates canonical meta", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		workflowID := "reactivation-proof-workflow"
		relationID, _, _, err := pairIDForPlayers(1287, 1288)
		if err != nil {
			t.Fatal(err)
		}
		reservationID, err := QuotaReservationID(workflowID, 1287, relationID)
		if err != nil {
			t.Fatal(err)
		}
		invocationID, err := stableID("farm-friend-participant-invocation-id-v1", workflowID, "RESERVE_FRIEND_SLOT:1287")
		if err != nil {
			t.Fatal(err)
		}
		identity := CommitIdentity{WorkAddressID: workflowID, ParticipantInvocationID: invocationID, ParticipantStep: "RESERVE_FRIEND_SLOT:1287"}
		payload := redisFriendHash("reactivation-proof-payload")
		reservation := QuotaReservation{ReservationID: reservationID, WorkflowID: workflowID, RelationID: relationID, TargetRelationEpoch: 1}
		if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1287, reservation, identity, payload, 1); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("reserve proof: %+v %v", outcome, err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1287", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.HSet(ctx, group.meta, "receipt_count", "0").Err(); err != nil {
			t.Fatal(err)
		}
		proof, err := repositories.Pair.quotaReactivationProof(ctx, 1287, reservationID, workflowID, relationID, 1, payload)
		if !errors.Is(err, ErrFriendReconcile) || proof != "" {
			t.Fatalf("corrupt quota proof=%q err=%v", proof, err)
		}
	})

	t.Run("absent non-create does not initialize authority or meta", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		group, err := repositories.authority.group(FriendQuotaAuthority, "1289", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Quota.ReleaseFriendReservation(ctx, 1289, "absent-release-reservation",
			redisFriendPublic(1289, "RELEASE", "absent-release"), redisFriendHash("absent-release"), 1); !errors.Is(err, ErrRedisContractViolation) {
			t.Fatalf("absent non-create error=%v", err)
		}
		for name, key := range map[string]string{"authority": group.record, "bucket meta": group.meta} {
			keyType, typeErr := repositories.authority.client.Type(ctx, key).Result()
			if typeErr != nil || keyType != "none" {
				t.Fatalf("absent non-create initialized %s type=%q err=%v", name, keyType, typeErr)
			}
		}
	})
}

func TestRedisFriendRuntimePartialMutationCannotResolveCommitted(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(1203, "RESERVE", "runtime-partial")
	payload := redisFriendHash("runtime-partial")
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1203", identity, payload,
		"RESERVE", "runtime-res", "runtime-wf", "runtime-rel", "1", "record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1203", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := friendAttemptID(FriendQuotaAuthority, "1203", ci.digestHex, 1)
	if err != nil {
		t.Fatal(err)
	}
	attemptDue := friendAttemptDueMember(FriendQuotaAuthority, "1203", ci.digestHex, 1)
	if _, err := repositories.authority.scripts.run(ctx, scriptFriendPrepare,
		[]string{group.record, group.attempt, group.attemptDue, group.receiptDue, group.meta}, group.tag, ci.digestHex, 1, attemptID,
		ci.candidateHash, attemptDue, repositories.authority.limits.AttemptRetentionMs, 0, "",
		string(FriendQuotaAuthority), group.ref.LayoutVersion, group.ref.BucketID, "1203",
		repositories.authority.limits.receiptCount(FriendQuotaAuthority), repositories.authority.limits.attemptAggregateBytes(FriendQuotaAuthority),
		friendAttemptCursorHardLimit, friendResolutionReserveBytes, 0, "1"); err != nil {
		t.Fatal(err)
	}
	// Simulate a Redis Lua runtime error after a canonical staging/apply write:
	// the record version/digest have advanced, but no receipt or marker exists.
	_, runtimeErr := repositories.authority.client.Eval(ctx,
		"redis.call('HSET', KEYS[1], '_meta:record_version', '1', '_meta:business_state_digest', 'runtime', 'partial', '1'); return redis.call('HGET', KEYS[2], 'wrongtype')",
		[]string{group.record, group.attemptDue}).Result()
	if runtimeErr == nil || !errors.Is(classifyFriendWriteError(runtimeErr), ErrRedisTimeoutUnknown) {
		t.Fatalf("runtime classification=%v", runtimeErr)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "1203", ci, 1, attemptID, attemptDue); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("runtime partial resolution=%v, want reconcile", err)
	}
	if exists, err := repositories.authority.client.HExists(ctx, group.receipt, ci.field).Result(); err != nil || exists {
		t.Fatalf("runtime partial created business receipt: exists=%v err=%v", exists, err)
	}
}

func TestRedisFriendExistingSourceOrderCorruptionFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		omit   string
		due    bool
		score  float64
		mutate string
	}{
		{name: "missing work_ref", omit: "work_ref", due: true},
		{name: "missing work record field", omit: "state", due: true},
		{name: "missing due member", due: false},
		{name: "wrong due score", due: true, score: 7},
		{name: "wrong immutable payload hash", due: true, mutate: "payload_hash"},
		{name: "extra manifest field", due: true, mutate: "extra"},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			low, high := uint64(1300+index*2), uint64(1301+index*2)
			pairID, _, _, err := pairIDForPlayers(low, high)
			if err != nil {
				t.Fatal(err)
			}
			identity := redisFriendPublic(low, "BLOCK_PLAYER", "source-corrupt-"+itoa(uint64(index)))
			payload := redisFriendHash("source-corrupt-" + itoa(uint64(index)))
			opDigest, err := pairOperationDigest(identity, pairID)
			if err != nil {
				t.Fatal(err)
			}
			resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, opDigest, "1")
			if err != nil {
				t.Fatal(err)
			}
			order, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "RESULT", pairID, opDigest, "1", identity.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
			if err != nil {
				t.Fatal(err)
			}
			group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
			if err != nil {
				t.Fatal(err)
			}
			seedFriendSourceOrder(t, ctx, repositories, group, pairID, order, testCase.omit, func() *float64 {
				if !testCase.due {
					return nil
				}
				score := testCase.score
				return &score
			}())
			if testCase.mutate == "payload_hash" {
				key, keyErr := repositories.authority.sourceOrderKey(group, order.workType, order.id)
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				if err := repositories.authority.client.HSet(ctx, key, "payload_hash", strings.Repeat("0", sha256.Size*2)).Err(); err != nil {
					t.Fatal(err)
				}
			} else if testCase.mutate == "extra" {
				key, keyErr := repositories.authority.sourceOrderKey(group, order.workType, order.id)
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				if err := repositories.authority.client.HSet(ctx, key, "unexpected", "field").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
				CallerPlayerID: low, PeerPlayerID: high}); !errors.Is(err, ErrFriendReconcile) {
				t.Fatalf("corrupt source order error=%v, want reconcile", err)
			}
			snapshot, err := repositories.Pair.GetPair(ctx, low, high)
			if testCase.due {
				if !errors.Is(err, ErrFriendReconcile) || !strings.Contains(err.Error(), "bucket discovery without meta") {
					t.Fatalf("discoverable orphan error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if snapshot.Exists() {
				t.Fatalf("source validation mutated Pair canonical state: %+v", snapshot)
			}
			if exists, cursorErr := repositories.authority.client.HExists(ctx, group.attempt,
				"cursor:"+ci.digestHex+":state").Result(); cursorErr != nil || exists {
				t.Fatalf("source validation wrote attempt cursor: exists=%v err=%v", exists, cursorErr)
			}
		})
	}
}

func TestRedisFriendExistingDispatchFieldsCorruptionFailsClosed(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(1411, 1412)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 1411, RedeemerPlayerID: 1412, TargetRelationEpoch: 1, TokenID: "dispatch-corrupt-token",
		TokenGeneration: 1, OperationID: "dispatch-corrupt-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "dispatch-corrupt-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("dispatch-corrupt-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate: %+v %v", outcome, err)
	}
	action := ActionAdmissionCommand{Identity: redisFriendPublic(1411, "CARE", "dispatch-corrupt-action"),
		PayloadHash: redisFriendHash("dispatch-corrupt-action"), VisitorPlayerID: 1411, OwnerPlayerID: 1412,
		TargetFarmID: 1412, Scope: ActionScopeCare, TargetBindingHash: redisFriendHash("dispatch-corrupt-binding"), PermitTTLMs: 60_000}
	if admitted, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); err != nil || admitted.Outcome.Status != StatusSucceeded {
		t.Fatalf("admit: %+v %v", admitted, err)
	}
	opDigest, err := workOperationDigest(action, pairID)
	if err != nil {
		t.Fatal(err)
	}
	ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, action.Identity, action.PayloadHash)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	dispatchID, err := ActionDispatchOrderID(pairID, opDigest, 1)
	if err != nil {
		t.Fatal(err)
	}
	dispatchKey, err := repositories.authority.sourceOrderKey(group, "FRIEND_ACTION_DISPATCH_ORDER", dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HSet(ctx, dispatchKey, "operation_id", "forged-operation").Err(); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HDel(ctx, group.receipt, ci.field).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.Del(ctx, group.attempt, group.resolution).Err(); err != nil {
		t.Fatal(err)
	}
	before, err := repositories.Pair.GetPair(ctx, 1411, 1412)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("forged dispatch field error=%v, want reconcile", err)
	}
	after, err := repositories.Pair.GetPair(ctx, 1411, 1412)
	if err != nil {
		t.Fatal(err)
	}
	if after.RecordVersion != before.RecordVersion || after.Relation != before.Relation {
		t.Fatalf("dispatch validation mutated canonical Pair: before=%+v after=%+v", before, after)
	}
}

func TestRedisFriendReceiptMissingRecoveryValidatesCompleteSourceOrderSchema(t *testing.T) {
	testCases := []struct {
		name       string
		target     string
		field      string
		value      string
		delete     bool
		extraField bool
	}{
		{name: "result missing schema", target: "result", field: "schema_version", delete: true},
		{name: "result wrong authority", target: "result", field: "canonical_source_authority_id", value: "forged"},
		{name: "result wrong bucket", target: "result", field: "bucket_id", value: "999999"},
		{name: "result typed payload", target: "result", field: "typed_payload", value: "TLP13:BAD"},
		{name: "result frozen state", target: "result", field: "state", value: "COMPLETED"},
		{name: "dispatch frozen version", target: "dispatch", field: "work_version", value: "2"},
		{name: "dispatch work binding", target: "dispatch", field: "work_ref", value: "wr1_forged"},
		{name: "dispatch payload hash", target: "dispatch", field: "payload_hash", value: strings.Repeat("0", sha256.Size*2)},
		{name: "dispatch extra field", target: "dispatch", field: "unexpected", value: "field", extraField: true},
	}
	for index, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			low, high := uint64(1450+index*2), uint64(1451+index*2)
			pairID, _, _, err := pairIDForPlayers(low, high)
			if err != nil {
				t.Fatal(err)
			}
			if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
				InviterPlayerID: low, RedeemerPlayerID: high, TargetRelationEpoch: 1, TokenID: "recovery-schema-token",
				TokenGeneration: 1, OperationID: "recovery-schema-activate",
			}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "recovery-schema-activate", ParticipantStep: "ACTIVATE"},
				redisFriendHash("recovery-schema-activate")); err != nil || outcome.Status != StatusSucceeded {
				t.Fatalf("activate: %+v %v", outcome, err)
			}
			action := ActionAdmissionCommand{Identity: redisFriendPublic(low, "CARE", "recovery-schema-action"),
				PayloadHash: redisFriendHash("recovery-schema-action"), VisitorPlayerID: low, OwnerPlayerID: high,
				TargetFarmID: high, Scope: ActionScopeCare, TargetBindingHash: redisFriendHash("recovery-schema-binding"), PermitTTLMs: 60_000}
			admitted, err := repositories.Pair.CommitFriendActionAdmission(ctx, action)
			if err != nil || admitted.Outcome.Status != StatusSucceeded {
				t.Fatalf("admit: %+v %v", admitted, err)
			}
			operationDigest, err := workOperationDigest(action, pairID)
			if err != nil {
				t.Fatal(err)
			}
			ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, action.Identity, action.PayloadHash)
			if err != nil {
				t.Fatal(err)
			}
			group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
			if err != nil {
				t.Fatal(err)
			}
			resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
			if err != nil {
				t.Fatal(err)
			}
			resultKey, err := repositories.authority.sourceOrderKey(group, "RESULT_PUBLISH_ORDER", resultID)
			if err != nil {
				t.Fatal(err)
			}
			dispatchKey, err := repositories.authority.sourceOrderKey(group, "FRIEND_ACTION_DISPATCH_ORDER", admitted.DispatchOrderID)
			if err != nil {
				t.Fatal(err)
			}
			if err := repositories.authority.client.HDel(ctx, group.receipt, ci.field).Err(); err != nil {
				t.Fatal(err)
			}
			targetKey := resultKey
			if testCase.target == "dispatch" {
				targetKey = dispatchKey
			}
			if testCase.delete {
				err = repositories.authority.client.HDel(ctx, targetKey, testCase.field).Err()
			} else {
				err = repositories.authority.client.HSet(ctx, targetKey, testCase.field, testCase.value).Err()
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := repositories.Pair.GetPair(ctx, low, high)
			if err != nil {
				t.Fatal(err)
			}
			recovered, recoveryErr := repositories.Pair.GetFriendActionAdmission(ctx, action)
			if !errors.Is(recoveryErr, ErrFriendReconcile) || recovered.Permit.PermitID != "" {
				t.Fatalf("recovery=%+v err=%v, want reconcile without permit", recovered, recoveryErr)
			}
			after, err := repositories.Pair.GetPair(ctx, low, high)
			if err != nil {
				t.Fatal(err)
			}
			if after.RecordVersion != before.RecordVersion || after.Relation != before.Relation || after.Barrier != before.Barrier {
				t.Fatalf("recovery corruption mutated Pair: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestRedisFriendCandidateHashConflictsAfterDeterministicNotCommitted(t *testing.T) {
	runActionCase := func(t *testing.T, suffix string, binding []byte, ttl int64) {
		t.Helper()
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		pairID, _, _, err := pairIDForPlayers(1501, 1502)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
			InviterPlayerID: 1501, RedeemerPlayerID: 1502, TargetRelationEpoch: 1, TokenID: "candidate-token",
			TokenGeneration: 1, OperationID: "candidate-activate-" + suffix,
		}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "candidate-activate-" + suffix, ParticipantStep: "ACTIVATE"}, redisFriendHash("candidate-activate-"+suffix)); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("activate: %+v %v", outcome, err)
		}
		repositories.authority.limits.MaxPairRecordBytes = 1
		identity := redisFriendPublic(1501, "CARE", "candidate-"+suffix)
		payload := redisFriendHash("candidate-payload-" + suffix)
		first := ActionAdmissionCommand{Identity: identity, PayloadHash: payload, VisitorPlayerID: 1501, OwnerPlayerID: 1502,
			TargetFarmID: 1502, Scope: ActionScopeCare, TargetBindingHash: redisFriendHash("binding-a"), PermitTTLMs: 60_000}
		if _, err := repositories.Pair.CommitFriendActionAdmission(ctx, first); !errors.Is(err, ErrFriendAuthorityCapacity) {
			t.Fatalf("first deterministic capacity error=%v", err)
		}
		second := first
		second.TargetBindingHash = binding
		second.PermitTTLMs = ttl
		if _, err := repositories.Pair.CommitFriendActionAdmission(ctx, second); !errors.Is(err, ErrIdempotentConflict) {
			t.Fatalf("changed candidate after NOT_COMMITTED error=%v", err)
		}
	}
	runActionCase(t, "binding", redisFriendHash("binding-b"), 60_000)
	runActionCase(t, "ttl", redisFriendHash("binding-a"), 30_000)

	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(1503, "RESERVE", "candidate-version")
	payload := redisFriendHash("candidate-version")
	oldFields := []string{"RESERVE", "candidate-res", "candidate-wf", "candidate-rel", "1", "record_version", "0", "authority_time", "REDIS_SERVER_TIME"}
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1503", identity, payload, oldFields...)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1503", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := friendAttemptID(FriendQuotaAuthority, "1503", ci.digestHex, 1)
	if err != nil {
		t.Fatal(err)
	}
	attemptDue := friendAttemptDueMember(FriendQuotaAuthority, "1503", ci.digestHex, 1)
	if _, err := repositories.authority.scripts.run(ctx, scriptFriendPrepare,
		[]string{group.record, group.attempt, group.attemptDue, group.receiptDue, group.meta}, group.tag, ci.digestHex, 1, attemptID,
		ci.candidateHash, attemptDue, repositories.authority.limits.AttemptRetentionMs, 0, "",
		string(FriendQuotaAuthority), group.ref.LayoutVersion, group.ref.BucketID, "1503",
		repositories.authority.limits.receiptCount(FriendQuotaAuthority), repositories.authority.limits.attemptAggregateBytes(FriendQuotaAuthority),
		friendAttemptCursorHardLimit, friendResolutionReserveBytes, 0, "1"); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.fenceKnownNotCommitted(ctx, group, FriendQuotaAuthority, "1503", ci, 1, attemptID, attemptDue); err != nil {
		t.Fatalf("precommit fence=%v", err)
	}
	changedFields := append([]string(nil), oldFields...)
	changedFields[6] = "1"
	if _, err := repositories.authority.commit(ctx, FriendQuotaAuthority, "1503", identity, payload, changedFields,
		scriptQuotaReserve, []string{group.record, group.receipt, group.attempt, group.attemptDue, group.receiptDue, group.meta}, func(_ friendCommitIdentity, _ uint64, _ string, _ string, _ string) []interface{} {
			return nil
		}); !errors.Is(err, ErrIdempotentConflict) {
		t.Fatalf("changed expected record version after NOT_COMMITTED error=%v", err)
	}
}

func TestRedisFriendReleaseFenceMarkerRequiresFenceAndDueIndex(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	reservation := QuotaReservation{ReservationID: "marker-fence-res", WorkflowID: "marker-fence-wf", RelationID: "marker-fence-rel", TargetRelationEpoch: 1}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1601, reservation, redisFriendPublic(1601, "RESERVE", "marker-fence-reserve"), redisFriendHash("marker-fence-reserve"), 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Quota.FinalizeFriendSlot(ctx, 1601, reservation, redisFriendPublic(1601, "FINALIZE", "marker-fence-finalize"), redisFriendHash("marker-fence-finalize"), 2); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("finalize: %+v %v", outcome, err)
	}
	identity := CommitIdentity{WorkAddressID: "marker-fence-termination", ParticipantInvocationID: "marker-fence-invocation", ParticipantStep: "REMOVE"}
	payload := redisFriendHash("marker-fence-remove")
	snapshot, err := repositories.Quota.GetQuota(ctx, 1601)
	if err != nil {
		t.Fatal(err)
	}
	fenceID, err := QuotaReleaseFenceID(1601, reservation.RelationID, reservation.TargetRelationEpoch)
	if err != nil {
		t.Fatal(err)
	}
	fields := []string{"REMOVE", reservation.RelationID, itoa(reservation.TargetRelationEpoch), fenceID,
		identity.WorkAddressID, identity.ParticipantInvocationID, "max_friends", itoa(uint64(repositories.authority.limits.MaxFriends)),
		"record_version", itoa(snapshot.RecordVersion), "authority_time", "REDIS_SERVER_TIME"}
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1601", identity, payload, fields...)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Quota.RemoveActiveRelationIfEpochMatches(ctx, 1601, reservation.RelationID,
		reservation.TargetRelationEpoch, identity, payload, 3); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("remove: %+v %v", outcome, err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1601", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":attempt_id").Result()
	if err != nil {
		t.Fatal(err)
	}
	fenceMember := "fence:" + friendFenceDigestHex(fenceID)
	if err := repositories.authority.client.ZRem(ctx, group.receiptDue, fenceMember).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "1601", ci, 1, attemptID,
		friendAttemptDueMember(FriendQuotaAuthority, "1601", ci.digestHex, 1), group.record, group.receipt,
		group.attempt, group.attemptDue, group.receiptDue, group.meta, func() string {
			fenceDigest := friendDigestBytes(appendFriendLP(nil, fenceID))
			key, keyErr := repositories.authority.keys.Receipt(group.ref, "1601", fenceDigest)
			if keyErr != nil {
				t.Fatal(keyErr)
			}
			return string(key)
		}()); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("missing fence DueIndex resolution=%v, want reconcile", err)
	}
}

func TestRedisFriendReleaseFenceCapacityCheckedBeforeQuotaMutation(t *testing.T) {
	repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{
		MaxFriends:      300,
		MaxReceiptCount: 3,
	})
	defer server.Close()
	ctx := context.Background()
	reservation := QuotaReservation{ReservationID: "fence-capacity-res", WorkflowID: "fence-capacity-wf",
		RelationID: "fence-capacity-rel", TargetRelationEpoch: 1}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 605, reservation,
		redisFriendPublic(605, "RESERVE", "fence-capacity-reserve"), redisFriendHash("fence-capacity-reserve"), 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Quota.FinalizeFriendSlot(ctx, 605, reservation,
		redisFriendPublic(605, "FINALIZE", "fence-capacity-finalize"), redisFriendHash("fence-capacity-finalize"), 2); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("finalize: %+v %v", outcome, err)
	}
	removeIdentity := CommitIdentity{WorkAddressID: "termination-capacity", ParticipantInvocationID: "fence-capacity-remove", ParticipantStep: "REMOVE"}
	if _, err := repositories.Quota.RemoveActiveRelationIfEpochMatches(ctx, 605, reservation.RelationID, reservation.TargetRelationEpoch,
		removeIdentity, nil, 3); !errors.Is(err, ErrFriendAuthorityCapacity) {
		t.Fatalf("release-fence capacity error=%v", err)
	}
	snapshot, err := repositories.Quota.GetQuota(ctx, 605)
	if err != nil || snapshot.ActiveRelations[reservation.RelationID] != reservation.TargetRelationEpoch || snapshot.OpenReservations != 0 {
		t.Fatalf("quota changed before release-fence capacity check: %+v err=%v", snapshot, err)
	}
	fenceID, err := QuotaReleaseFenceID(605, reservation.RelationID, reservation.TargetRelationEpoch)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "605", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	fenceDigest := friendDigestBytes(appendFriendLP(nil, fenceID))
	fenceKey, err := repositories.authority.keys.Receipt(group.ref, "605", fenceDigest)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := repositories.authority.client.Exists(ctx, string(fenceKey)).Result(); err != nil || exists != 0 {
		t.Fatalf("release fence leaked on capacity: exists=%d err=%v", exists, err)
	}
}

func TestRedisFriendQuotaFinalizeEpochInterleaving(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	relationship := "relation-epoch-interleave"
	first := QuotaReservation{ReservationID: "epoch-res-1", WorkflowID: "epoch-wf-1", RelationID: relationship, TargetRelationEpoch: 1}
	second := QuotaReservation{ReservationID: "epoch-res-2", WorkflowID: "epoch-wf-2", RelationID: relationship, TargetRelationEpoch: 2}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 701, first, redisFriendPublic(701, "RESERVE", "epoch-reserve-1"), redisFriendHash("epoch-reserve-1"), 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve first: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 701, second, redisFriendPublic(701, "RESERVE", "epoch-reserve-2"), redisFriendHash("epoch-reserve-2"), 2); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve second: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Quota.FinalizeFriendSlot(ctx, 701, second, redisFriendPublic(701, "FINALIZE", "epoch-finalize-2"), redisFriendHash("epoch-finalize-2"), 3); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("finalize second: %+v %v", outcome, err)
	}
	oldFinalizeIdentity := redisFriendPublic(701, "FINALIZE", "epoch-finalize-1")
	if _, err := repositories.Quota.FinalizeFriendSlot(ctx, 701, first, oldFinalizeIdentity, redisFriendHash("epoch-finalize-1"), 4); !errors.Is(err, ErrFriendRecordVersionConflict) {
		t.Fatalf("old finalize error=%v", err)
	}
	oldCI, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "701", oldFinalizeIdentity, redisFriendHash("epoch-finalize-1"),
		"FINALIZE", first.ReservationID, first.WorkflowID, first.RelationID, "1")
	if err != nil {
		t.Fatal(err)
	}
	oldGroup, err := repositories.authority.group(FriendQuotaAuthority, "701", oldCI.digest)
	if err != nil {
		t.Fatal(err)
	}
	if state, stateErr := repositories.authority.client.HGet(ctx, oldGroup.attempt, "cursor:"+oldCI.digestHex+":state").Result(); stateErr != nil || state != "NOT_COMMITTED" {
		t.Fatalf("deterministic epoch conflict left OPEN attempt: state=%q err=%v", state, stateErr)
	}
	snapshot, err := repositories.Quota.GetQuota(ctx, 701)
	if err != nil || snapshot.ActiveRelations[relationship] != 2 || snapshot.OpenReservations != 1 {
		t.Fatalf("old finalize overwrote new epoch: %+v err=%v", snapshot, err)
	}
	if outcome, err := repositories.Quota.ReleaseFriendReservation(ctx, 701, first.ReservationID,
		redisFriendPublic(701, "RELEASE", "epoch-release-1"), redisFriendHash("epoch-release-1"), 5); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("release old reservation: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Quota.FinalizeFriendSlot(ctx, 701, second, redisFriendPublic(701, "FINALIZE", "epoch-finalize-replay"), redisFriendHash("epoch-finalize-replay"), 6); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("finalize replay: %+v %v", outcome, err)
	}
	snapshot, err = repositories.Quota.GetQuota(ctx, 701)
	if err != nil || snapshot.ActiveRelations[relationship] != 2 || snapshot.OpenReservations != 0 {
		t.Fatalf("finalize/release interleave result: %+v err=%v", snapshot, err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "701", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	logical := logicalBytesFromHash(t, ctx, repositories.authority.client, string(group.record))
	logicalText, err := repositories.authority.client.HGet(ctx, string(group.record), "_meta:logical_bytes").Result()
	if err != nil || logicalText != itoa(uint64(logical)) {
		t.Fatalf("logical byte counter=%q recomputed=%d err=%v", logicalText, logical, err)
	}
}

func TestRedisFriendAttemptResolutionReplayAndNoResend(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(801, "RESERVE", "attempt-resolution")
	payload := redisFriendHash("attempt-resolution")
	reservation := QuotaReservation{ReservationID: "resolution-res", WorkflowID: "resolution-wf", RelationID: "resolution-rel", TargetRelationEpoch: 1}
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "801", identity, payload,
		"RESERVE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "801", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := friendAttemptID(FriendQuotaAuthority, "801", ci.digestHex, 1)
	if err != nil {
		t.Fatal(err)
	}
	attemptDue := friendAttemptDueMember(FriendQuotaAuthority, "801", ci.digestHex, 1)
	if _, err := repositories.authority.scripts.run(ctx, scriptFriendPrepare,
		[]string{group.record, group.attempt, group.attemptDue, group.receiptDue, group.meta}, group.tag, ci.digestHex, 1, attemptID,
		ci.candidateHash, attemptDue, repositories.authority.limits.AttemptRetentionMs, 0, "",
		string(FriendQuotaAuthority), group.ref.LayoutVersion, group.ref.BucketID, "801",
		repositories.authority.limits.receiptCount(FriendQuotaAuthority), repositories.authority.limits.attemptAggregateBytes(FriendQuotaAuthority),
		friendAttemptCursorHardLimit, friendResolutionReserveBytes, 0, "1"); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.fenceKnownNotCommitted(ctx, group, FriendQuotaAuthority, "801", ci, 1, attemptID, attemptDue); err != nil {
		t.Fatalf("proven precommit fence=%v", err)
	}
	if exists, err := repositories.authority.client.HExists(ctx, group.receipt, ci.field).Result(); err != nil || exists {
		t.Fatalf("NOT_COMMITTED incorrectly required/wrote business receipt: %v %v", exists, err)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "801", ci, 1, attemptID, attemptDue); !errors.Is(err, ErrFriendNotCommitted) {
		t.Fatalf("resolution replay=%v", err)
	}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 801, reservation, identity, payload, 9); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("retry after fenced NOT_COMMITTED: %+v %v", outcome, err)
	}
	ordinalText, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":ordinal").Result()
	if err != nil || ordinalText != "2" {
		t.Fatalf("retry reused OPEN ordinal: ordinal=%q err=%v", ordinalText, err)
	}
	attemptID2, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":attempt_id").Result()
	if err != nil {
		t.Fatal(err)
	}
	attemptDue2 := friendAttemptDueMember(FriendQuotaAuthority, "801", ci.digestHex, 2)
	if raw, rawErr := repositories.authority.client.HGet(ctx, group.receipt, ci.field).Result(); rawErr != nil {
		t.Fatal(rawErr)
	} else if stored, parseErr := parseFriendReceipt([]byte(raw)); parseErr != nil || stored.CommittedAttemptID != attemptID2 ||
		stored.CandidateHash != ci.candidateHash || stored.AttemptOrdinal != 2 {
		t.Fatalf("receipt attempt binding=%+v err=%v", stored, parseErr)
	}
	wrongAttempt, wrongErr := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "801", ci, 2,
		"sid1_wrong_attempt", attemptDue2)
	if !errors.Is(wrongErr, ErrFriendReconcile) || wrongAttempt.Kind != "" {
		t.Fatalf("receipt committed by different attempt was accepted: %+v err=%v", wrongAttempt, wrongErr)
	}
	committed, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "801", ci, 2, attemptID2, attemptDue2)
	if err != nil || committed.Kind != "QUOTA_RESERVE" {
		t.Fatalf("COMMITTED resolution=%+v err=%v", committed, err)
	}
	committedReplay, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "801", ci, 2, attemptID2, attemptDue2)
	if err != nil || committedReplay.CommittedAtMs != committed.CommittedAtMs {
		t.Fatalf("COMMITTED resolution replay=%+v/%+v err=%v", committed, committedReplay, err)
	}
	if err := repositories.authority.client.HDel(ctx, group.receipt, ci.field).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "801", ci, 2, attemptID2, attemptDue2); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("COMMITTED resolution without business receipt=%v, want reconcile", err)
	}
	if !errors.Is(mapFriendScriptError("ATTEMPT_FENCED_NOT_COMMITTED"), ErrFriendNotCommitted) ||
		!errors.Is(mapFriendScriptError("ATTEMPT_FENCED_RECONCILE_REQUIRED"), ErrFriendReconcile) {
		t.Fatal("attempt fence result mapping is not deterministic")
	}
}

func TestRedisFriendUnknownWithoutAttemptProofRequiresReconcile(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(802, "RESERVE", "unknown-without-proof")
	payload := redisFriendHash("unknown-without-proof")
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "802", identity, payload,
		"RESERVE", "unknown-res", "unknown-wf", "unknown-rel", "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "802", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := friendAttemptID(FriendQuotaAuthority, "802", ci.digestHex, 1)
	if err != nil {
		t.Fatal(err)
	}
	attemptDue := friendAttemptDueMember(FriendQuotaAuthority, "802", ci.digestHex, 1)
	if _, err := repositories.authority.scripts.run(ctx, scriptFriendPrepare,
		[]string{group.record, group.attempt, group.attemptDue, group.receiptDue, group.meta}, group.tag, ci.digestHex, 1, attemptID,
		ci.candidateHash, attemptDue, repositories.authority.limits.AttemptRetentionMs, 0, "",
		string(FriendQuotaAuthority), group.ref.LayoutVersion, group.ref.BucketID, "802",
		repositories.authority.limits.receiptCount(FriendQuotaAuthority), repositories.authority.limits.attemptAggregateBytes(FriendQuotaAuthority),
		friendAttemptCursorHardLimit, friendResolutionReserveBytes, 0, "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "802", ci, 1, attemptID, attemptDue); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("unproved unchanged attempt resolution=%v, want reconcile", err)
	}
	if exists, err := repositories.authority.client.HExists(ctx, group.receipt, ci.field).Result(); err != nil || exists {
		t.Fatalf("unproved unknown created receipt: exists=%v err=%v", exists, err)
	}
	if state, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || state != "RECONCILE_REQUIRED" {
		t.Fatalf("unproved unknown state=%q err=%v", state, err)
	}
	if score, err := repositories.authority.client.ZScore(ctx, group.attemptDue, attemptDue).Result(); err != nil || score <= 0 {
		t.Fatalf("unproved unknown lost recovery discovery: score=%v err=%v", score, err)
	}
}

func TestRedisFriendCommittedResolutionSurvivesLaterVersionAndRestart(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	firstIdentity := redisFriendPublic(811, "RESERVE", "resolution-version-a")
	firstPayload := redisFriendHash("resolution-version-a")
	firstReservation := QuotaReservation{ReservationID: "resolution-version-res-a", WorkflowID: "resolution-version-wf-a",
		RelationID: "resolution-version-rel-a", TargetRelationEpoch: 1}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 811, firstReservation, firstIdentity, firstPayload, 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("first commit: %+v %v", outcome, err)
	}
	firstCI, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "811", firstIdentity, firstPayload,
		"RESERVE", firstReservation.ReservationID, firstReservation.WorkflowID, firstReservation.RelationID, "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	firstGroup, err := repositories.authority.group(FriendQuotaAuthority, "811", firstCI.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := repositories.authority.client.HGet(ctx, firstGroup.attempt, "cursor:"+firstCI.digestHex+":attempt_id").Result()
	if err != nil {
		t.Fatal(err)
	}
	secondReservation := QuotaReservation{ReservationID: "resolution-version-res-b", WorkflowID: "resolution-version-wf-b",
		RelationID: "resolution-version-rel-b", TargetRelationEpoch: 1}
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 811, secondReservation,
		redisFriendPublic(811, "RESERVE", "resolution-version-b"), redisFriendHash("resolution-version-b"), 2); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("later commit: %+v %v", outcome, err)
	}

	restartedClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer restartedClient.Close()
	restarted, err := NewRedisFriendshipRepositories(restartedClient, RedisFriendshipOptions{
		Profile: repositories.authority.keys.Profile(), Limits: repositories.authority.limits})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := restarted.authority.resolveUnknown(ctx, firstGroup, FriendQuotaAuthority, "811", firstCI, 1,
		attemptID, friendAttemptDueMember(FriendQuotaAuthority, "811", firstCI.digestHex, 1))
	if err != nil || resolved.Kind != "QUOTA_RESERVE" || resolved.CandidateHash != firstCI.candidateHash {
		t.Fatalf("resolve A after B/restart: %+v %v", resolved, err)
	}
	replayed, err := restarted.authority.resolveUnknown(ctx, firstGroup, FriendQuotaAuthority, "811", firstCI, 1,
		attemptID, friendAttemptDueMember(FriendQuotaAuthority, "811", firstCI.digestHex, 1))
	if err != nil || replayed.CommittedAtMs != resolved.CommittedAtMs {
		t.Fatalf("resolution replay after restart: %+v %v", replayed, err)
	}
}

func TestRedisFriendPairReplayBindsPayloadAndPriorCandidateAfterLaterVersion(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	identity := redisFriendPublic(815, "BLOCK_PLAYER", "pair-replay-exact")
	payload := redisFriendHash("pair-replay-exact")
	first, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
		CallerPlayerID: 815, PeerPlayerID: 816})
	if err != nil || first.Status != StatusSucceeded {
		t.Fatalf("first block: %+v %v", first, err)
	}
	if outcome, err := repositories.Pair.UnblockPlayer(ctx, PairMutationCommand{
		Identity: redisFriendPublic(815, "UNBLOCK_PLAYER", "pair-replay-later"), PayloadHash: redisFriendHash("pair-replay-later"),
		CallerPlayerID: 815, PeerPlayerID: 816}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("later unblock: %+v %v", outcome, err)
	}
	restartedClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer restartedClient.Close()
	restarted, err := NewRedisFriendshipRepositories(restartedClient, RedisFriendshipOptions{
		Profile: repositories.authority.keys.Profile(), Limits: repositories.authority.limits})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restarted.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
		CallerPlayerID: 815, PeerPlayerID: 816})
	if err != nil || replay.CommittedAtMs != first.CommittedAtMs || replay.CommittedEpoch != first.CommittedEpoch {
		t.Fatalf("prior candidate replay: %+v %v", replay, err)
	}
	if _, err := restarted.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity,
		PayloadHash: redisFriendHash("pair-replay-forged-payload"), CallerPlayerID: 815, PeerPlayerID: 816}); !errors.Is(err, ErrIdempotentConflict) {
		t.Fatalf("changed payload replay error=%v", err)
	}
	snapshot, err := restarted.Pair.GetPair(ctx, 815, 816)
	if err != nil || snapshot.LowBlocksHigh.Status != BlockStatusRemoved {
		t.Fatalf("replay inferred/mutated current state: %+v %v", snapshot, err)
	}
	ci, err := restarted.authority.commitIdentity(FriendPairAuthority, snapshot.PairID, identity, payload)
	if err != nil {
		t.Fatal(err)
	}
	group, err := restarted.authority.group(FriendPairAuthority, snapshot.PairID, ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	candidateHash, err := restarted.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":candidate_hash").Result()
	if err != nil {
		t.Fatal(err)
	}
	markerDigest := friendCommitMarkerDigest(candidateHash, 1)
	proofField := "commit_marker:" + markerDigest + ":proof"
	proof, err := restarted.authority.client.HGet(ctx, group.attempt, proofField).Result()
	if err != nil {
		t.Fatal(err)
	}
	corruptProof := strings.Replace(proof, candidateHash, strings.Repeat("0", sha256.Size*2), 1)
	if corruptProof == proof {
		t.Fatal("candidate hash was not present in marker proof")
	}
	if err := restarted.authority.client.HSet(ctx, group.attempt, proofField, corruptProof).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
		CallerPlayerID: 815, PeerPlayerID: 816}); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("corrupt marker replay error=%v, want reconcile", err)
	}
}

func TestRedisFriendCompletedMarkerCompactionAndAttemptCapacity(t *testing.T) {
	t.Run("completed marker compacts to replayable resolution", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{
			MaxFriends: 300, AttemptRetentionMs: 1})
		defer server.Close()
		ctx := context.Background()
		identity := redisFriendPublic(821, "BLOCK_PLAYER", "compact-marker")
		payload := redisFriendHash("compact-marker")
		outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
			CallerPlayerID: 821, PeerPlayerID: 822})
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("block: %+v %v", outcome, err)
		}
		pairID, _, _, err := pairIDForPlayers(821, 822)
		if err != nil {
			t.Fatal(err)
		}
		ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		candidateHash, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":candidate_hash").Result()
		if err != nil {
			t.Fatal(err)
		}
		markerDigest := friendCommitMarkerDigest(candidateHash, 1)
		beforeBytes, err := repositories.authority.client.HGet(ctx, group.meta, "attempt_bytes").Int64()
		if err != nil {
			t.Fatal(err)
		}
		server.FastForward(5 * time.Millisecond)
		if compacted, err := repositories.CompactCompletedAttemptMarkers(ctx, FriendPairAuthority, pairID, 1); err != nil || compacted != 1 {
			t.Fatalf("compact result=%d err=%v", compacted, err)
		}
		if exists, err := repositories.authority.client.HExists(ctx, group.attempt, "commit_marker:"+markerDigest+":proof").Result(); err != nil || exists {
			t.Fatalf("marker proof retained: %v %v", exists, err)
		}
		afterBytes, err := repositories.authority.client.HGet(ctx, group.meta, "attempt_bytes").Int64()
		if err != nil || afterBytes >= beforeBytes || afterBytes < 0 {
			t.Fatalf("attempt bytes before=%d after=%d err=%v", beforeBytes, afterBytes, err)
		}
		if due, err := repositories.authority.client.ZCard(ctx, group.attemptDue).Result(); err != nil || due != 0 {
			t.Fatalf("completed attempt discovery retained: due=%d err=%v", due, err)
		}
		restartedClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
		defer restartedClient.Close()
		restarted, err := NewRedisFriendshipRepositories(restartedClient, RedisFriendshipOptions{
			Profile: repositories.authority.keys.Profile(), Limits: repositories.authority.limits})
		if err != nil {
			t.Fatal(err)
		}
		replay, err := restarted.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
			CallerPlayerID: 821, PeerPlayerID: 822})
		if err != nil || replay.CommittedAtMs != outcome.CommittedAtMs {
			t.Fatalf("compacted replay: %+v %v", replay, err)
		}
	})

	t.Run("attempt identity count is bounded before canonical mutation", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{
			MaxFriends: 300, MaxReceiptCount: 1})
		defer server.Close()
		ctx := context.Background()
		first := QuotaReservation{ReservationID: "attempt-capacity-res-a", WorkflowID: "attempt-capacity-wf-a",
			RelationID: "attempt-capacity-rel-a", TargetRelationEpoch: 1}
		if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 831, first,
			redisFriendPublic(831, "RESERVE", "attempt-capacity-a"), redisFriendHash("attempt-capacity-a"), 1); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("first reserve: %+v %v", outcome, err)
		}
		before, err := repositories.Quota.GetQuota(ctx, 831)
		if err != nil {
			t.Fatal(err)
		}
		secondIdentity := redisFriendPublic(831, "RESERVE", "attempt-capacity-b")
		secondPayload := redisFriendHash("attempt-capacity-b")
		second := QuotaReservation{ReservationID: "attempt-capacity-res-b", WorkflowID: "attempt-capacity-wf-b",
			RelationID: "attempt-capacity-rel-b", TargetRelationEpoch: 1}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, 831, second, secondIdentity, secondPayload, 2); !errors.Is(err, ErrFriendAuthorityCapacity) {
			t.Fatalf("second attempt capacity error=%v", err)
		}
		after, err := repositories.Quota.GetQuota(ctx, 831)
		if err != nil || after.RecordVersion != before.RecordVersion || after.OpenReservations != before.OpenReservations {
			t.Fatalf("attempt capacity mutated quota: before=%+v after=%+v err=%v", before, after, err)
		}
		ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "831", secondIdentity, secondPayload)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "831", ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		if cursor, err := repositories.authority.client.HExists(ctx, group.attempt, "cursor:"+ci.digestHex+":state").Result(); err != nil || cursor {
			t.Fatalf("capacity rejection wrote cursor: %v %v", cursor, err)
		}
	})
}

func TestRedisFriendCompactionPersistsResolutionBeforeMarkerDeletionAtEveryStep(t *testing.T) {
	faults := []struct {
		name  string
		token string
	}{
		{name: "before resolution", token: "-- FRIEND_COMPACT_FAULT_BEFORE_RESOLUTION"},
		{name: "after resolution", token: "-- FRIEND_COMPACT_FAULT_AFTER_RESOLUTION"},
		{name: "after marker delete and counter", token: "-- FRIEND_COMPACT_FAULT_AFTER_MARKER_DELETE"},
		{name: "after cursor index", token: "-- FRIEND_COMPACT_FAULT_AFTER_CURSOR_INDEX"},
		{name: "after discovery index", token: "-- FRIEND_COMPACT_FAULT_AFTER_DISCOVERY_INDEX"},
	}
	for index, fault := range faults {
		t.Run(fault.name, func(t *testing.T) {
			repositories, server := newFriendRedisTestRepositories(t, 300)
			defer server.Close()
			ctx := context.Background()
			low, high := uint64(880+index*2), uint64(881+index*2)
			pairID, _, _, err := pairIDForPlayers(low, high)
			if err != nil {
				t.Fatal(err)
			}
			identity := redisFriendPublic(low, "BLOCK_PLAYER", "compact-fault-"+itoa(uint64(index)))
			payload := redisFriendHash("compact-fault-" + itoa(uint64(index)))
			if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
				CallerPlayerID: low, PeerPlayerID: high}); err != nil || outcome.Status != StatusSucceeded {
				t.Fatalf("block: %+v %v", outcome, err)
			}
			ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
			if err != nil {
				t.Fatal(err)
			}
			group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
			if err != nil {
				t.Fatal(err)
			}
			base := "cursor:" + ci.digestHex
			cursor, err := repositories.authority.readFields(ctx, group.attempt,
				base+":ordinal", base+":attempt_id", base+":candidate_hash", base+":state")
			if err != nil {
				t.Fatal(err)
			}
			ordinal, err := strconv.ParseUint(cursor[base+":ordinal"], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			markerDigest := friendCommitMarkerDigest(cursor[base+":candidate_hash"], ordinal)
			resolutionID, err := friendResolutionID(FriendPairAuthority, pairID, ci.digestHex, ordinal)
			if err != nil {
				t.Fatal(err)
			}
			member := friendAttemptDueMember(FriendPairAuthority, pairID, ci.digestHex, ordinal)
			keys := []string{group.attempt, group.resolution, group.attemptDue, group.receipt, group.meta, group.receiptDue, group.sourceDue}
			args := []interface{}{group.tag, ci.digestHex, ordinal, cursor[base+":attempt_id"], cursor[base+":candidate_hash"],
				markerDigest, "resolve:" + ci.digestHex + ":" + itoa(ordinal), resolutionID, member,
				string(FriendPairAuthority), group.ref.LayoutVersion, group.ref.BucketID}
			faulted := strings.Replace(friendCompactMarkerSource, fault.token, "error('injected compaction fault')\n"+fault.token, 1)
			if faulted == friendCompactMarkerSource {
				t.Fatalf("fault token %q not found", fault.token)
			}
			if _, err := repositories.authority.client.Eval(ctx, faulted, keys, args...).Result(); err == nil {
				t.Fatal("injected compaction fault unexpectedly succeeded")
			}
			markerProof, err := repositories.authority.client.HExists(ctx, group.attempt, "commit_marker:"+markerDigest+":proof").Result()
			if err != nil {
				t.Fatal(err)
			}
			markerComplete, err := repositories.authority.client.HExists(ctx, group.attempt, "commit_marker:"+markerDigest+":complete").Result()
			if err != nil {
				t.Fatal(err)
			}
			resolutionExists, err := repositories.authority.client.HExists(ctx, group.resolution,
				"resolve:"+ci.digestHex+":"+itoa(ordinal)).Result()
			if err != nil {
				t.Fatal(err)
			}
			if !resolutionExists && (!markerProof || !markerComplete) {
				t.Fatalf("unsafe committed cursor after %s: marker=%v/%v resolution=%v", fault.name, markerProof, markerComplete, resolutionExists)
			}
			if fault.name == "before resolution" && (!markerProof || !markerComplete || resolutionExists) {
				t.Fatalf("pre-resolution fault changed proof: marker=%v/%v resolution=%v", markerProof, markerComplete, resolutionExists)
			}
			replayed, err := repositories.authority.client.Eval(ctx, friendCompactMarkerSource, keys, args...).StringSlice()
			if err != nil || len(replayed) != 1 || replayed[0] != "COMPACTED" {
				t.Fatalf("compaction replay=%v err=%v", replayed, err)
			}
			markerProof, _ = repositories.authority.client.HExists(ctx, group.attempt, "commit_marker:"+markerDigest+":proof").Result()
			markerComplete, _ = repositories.authority.client.HExists(ctx, group.attempt, "commit_marker:"+markerDigest+":complete").Result()
			resolutionExists, _ = repositories.authority.client.HExists(ctx, group.resolution,
				"resolve:"+ci.digestHex+":"+itoa(ordinal)).Result()
			if markerProof || markerComplete || !resolutionExists {
				t.Fatalf("replay did not finish compaction: marker=%v/%v resolution=%v", markerProof, markerComplete, resolutionExists)
			}
			if _, err := repositories.authority.client.ZScore(ctx, group.attemptDue, member).Result(); !errors.Is(err, redis.Nil) {
				t.Fatalf("replay retained discovery member: %v", err)
			}
			state, err := repositories.authority.client.HGet(ctx, group.attempt, base+":state").Result()
			if err != nil || state != "COMMITTED" {
				t.Fatalf("cursor state=%q err=%v", state, err)
			}
		})
	}

	t.Run("conflicting existing resolution keeps marker", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: 300, AttemptRetentionMs: 1})
		defer server.Close()
		ctx := context.Background()
		identity := redisFriendPublic(899, "BLOCK_PLAYER", "compact-conflict")
		payload := redisFriendHash("compact-conflict")
		if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
			CallerPlayerID: 899, PeerPlayerID: 900}); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("block: %+v %v", outcome, err)
		}
		pairID, _, _, err := pairIDForPlayers(899, 900)
		if err != nil {
			t.Fatal(err)
		}
		ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := repositories.authority.client.HGet(ctx, group.attempt, "cursor:"+ci.digestHex+":candidate_hash").Result()
		if err != nil {
			t.Fatal(err)
		}
		markerDigest := friendCommitMarkerDigest(candidate, 1)
		field := "resolve:" + ci.digestHex + ":1"
		if err := repositories.authority.client.HSet(ctx, group.resolution, field,
			encodeFriendResolution("COMMITTED", "forged-attempt", candidate, 1)).Err(); err != nil {
			t.Fatal(err)
		}
		server.FastForward(5 * time.Millisecond)
		if compacted, err := repositories.CompactCompletedAttemptMarkers(ctx, FriendPairAuthority, pairID, 1); !errors.Is(err, ErrFriendReconcile) || compacted != 0 {
			t.Fatalf("conflicting resolution compacted=%d err=%v", compacted, err)
		}
		if exists, err := repositories.authority.client.HExists(ctx, group.attempt,
			"commit_marker:"+markerDigest+":proof").Result(); err != nil || !exists {
			t.Fatalf("conflicting resolution removed marker: exists=%v err=%v", exists, err)
		}
	})
}

func TestRedisFriendKeyAuthorityTagsAreSeparate(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "friend")
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewFriendshipKeyBuilder(DefaultFriendRedisProfile(namespace))
	if err != nil {
		t.Fatal(err)
	}
	pairID, _, _, err := pairIDForPlayers(11, 22)
	if err != nil {
		t.Fatal(err)
	}
	pairRef, err := builder.BucketFor(FriendPairAuthority, pairID)
	if err != nil {
		t.Fatal(err)
	}
	quotaRef, err := builder.BucketFor(FriendQuotaAuthority, "11")
	if err != nil {
		t.Fatal(err)
	}
	shareRef, err := builder.BucketFor(FriendShareIssuerAuthority, "11")
	if err != nil {
		t.Fatal(err)
	}
	pairTag, _ := builder.HashTag(pairRef)
	quotaTag, _ := builder.HashTag(quotaRef)
	shareTag, _ := builder.HashTag(shareRef)
	if pairTag == quotaTag || pairTag == shareTag || quotaTag == shareTag {
		t.Fatalf("authority tags unexpectedly shared: %q %q %q", pairTag, quotaTag, shareTag)
	}
	if strings.Contains(pairTag, "{friend}") || strings.Contains(quotaTag, "{friend}") || strings.Contains(shareTag, "{friend}") {
		t.Fatal("legacy global friend tag leaked into 33A keys")
	}
	pairRecord, err := builder.Record(pairRef, pairID)
	if err != nil {
		t.Fatal(err)
	}
	pairDue, err := builder.SourceDueIndex(pairRef)
	if err != nil {
		t.Fatal(err)
	}
	quotaRecord, err := builder.Record(quotaRef, "11")
	if err != nil {
		t.Fatal(err)
	}
	quotaDue, err := builder.ReceiptDueIndex(quotaRef)
	if err != nil {
		t.Fatal(err)
	}
	shareRecord, err := builder.Record(shareRef, "11")
	if err != nil {
		t.Fatal(err)
	}
	shareDue, err := builder.ReceiptDueIndex(shareRef)
	if err != nil {
		t.Fatal(err)
	}
	for authority, keys := range map[string][]rediskey.RedisKey{
		"pair":  {pairRecord, pairDue},
		"quota": {quotaRecord, quotaDue},
		"share": {shareRecord, shareDue},
	} {
		if rediskey.ClusterSlot(keys[0]) != rediskey.ClusterSlot(keys[1]) {
			t.Fatalf("%s authority keys are cross-slot: %d/%d", authority, rediskey.ClusterSlot(keys[0]), rediskey.ClusterSlot(keys[1]))
		}
	}
}

func TestRedisFriendLimitsRejectFrozenOverflow(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "friend")
	if err != nil {
		t.Fatal(err)
	}
	profile := DefaultFriendRedisProfile(namespace)
	profile.ReceiptPartitionCount = 1 << 16
	if _, err := NewFriendshipKeyBuilder(profile); !errors.Is(err, ErrInvalidFriendProfile) {
		t.Fatalf("partition count overflow error=%v", err)
	}
	checks := []FriendRedisLimits{
		{MaxFriends: friendQuotaOccupancyHard + 1},
		{MaxReceiptCount: friendPairReceiptCountHard + 1},
		{MaxQuotaRecordBytes: friendQuotaRecordHardLimit + 1},
		{MaxSingleReceiptBytes: friendReceiptHardLimit + 1},
		{MaxSourceOrderBytes: friendSourceOrderHardLimit + 1},
		{MaxAttemptBytes: friendAttemptHardLimit + 1},
		{MaxScriptInputBytes: friendScriptInputHardLimit + 1},
		{MaxPairPendingOrders: friendPairPendingCountHard + 1},
		{AttemptRetentionMs: 2, ReceiptRetentionMs: 1},
	}
	for _, limits := range checks {
		options := RedisFriendshipOptions{Profile: DefaultFriendRedisProfile(namespace), Limits: limits}
		if err := options.normalize(); err == nil {
			t.Fatalf("invalid frozen limit accepted: %+v", limits)
		}
	}
}

func TestRedisFriendSourceOrderTupleAndActionPermitRecovery(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()

	pairID, _, _, err := pairIDForPlayers(301, 302)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 301, RedeemerPlayerID: 302, TargetRelationEpoch: 1, TokenID: "source-token",
		TokenGeneration: 1, OperationID: "source-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "source-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("source-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate pair: %+v %v", outcome, err)
	}
	deleteIdentity := redisFriendPublic(301, "DELETE_FRIEND", "source-delete")
	deletePayload := redisFriendHash("source-delete")
	if outcome, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{Identity: deleteIdentity, PayloadHash: deletePayload,
		CallerPlayerID: 301, PeerPlayerID: 302}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("delete pair: %+v %v", outcome, err)
	}
	operationDigest, err := pairOperationDigest(deleteIdentity, pairID)
	if err != nil {
		t.Fatal(err)
	}
	ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, deleteIdentity, deletePayload,
		"DELETE", "301", "302")
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	terminationID := terminationOrderID(pairID, pairID, 1)
	assertFriendSourceOrder(t, ctx, repositories, group, "FRIEND_TERMINATION_ORDER", terminationID)
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		t.Fatal(err)
	}
	assertFriendSourceOrder(t, ctx, repositories, group, "RESULT_PUBLISH_ORDER", resultID)

	pairID, _, _, err = pairIDForPlayers(401, 402)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 401, RedeemerPlayerID: 402, TargetRelationEpoch: 1, TokenID: "action-token",
		TokenGeneration: 1, OperationID: "action-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "action-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("action-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate action pair: %+v %v", outcome, err)
	}
	action := ActionAdmissionCommand{Identity: redisFriendPublic(401, "CARE", "source-care"), PayloadHash: redisFriendHash("source-care"),
		VisitorPlayerID: 401, OwnerPlayerID: 402, TargetFarmID: 402, Scope: ActionScopeCare,
		TargetBindingHash: redisFriendHash("source-binding"), PermitTTLMs: 60_000}
	admitted, err := repositories.Pair.CommitFriendActionAdmission(ctx, action)
	if err != nil || admitted.Outcome.Status != StatusSucceeded {
		t.Fatalf("action admission: %+v %v", admitted, err)
	}
	operationDigest, err = workOperationDigest(action, pairID)
	if err != nil {
		t.Fatal(err)
	}
	ci, err = repositories.authority.commitIdentity(FriendPairAuthority, pairID, action.Identity, action.PayloadHash,
		"ACTION_ADMISSION", "401", "402", "402", "2")
	if err != nil {
		t.Fatal(err)
	}
	group, err = repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	values := assertFriendSourceOrder(t, ctx, repositories, group, "FRIEND_ACTION_DISPATCH_ORDER", admitted.DispatchOrderID)
	for field, want := range map[string]string{
		"permit_id": admitted.Permit.PermitID, "relation_id": admitted.Permit.RelationID,
		"relation_epoch": itoa(admitted.Permit.RelationEpoch), "visitor_player_id": "401", "owner_player_id": "402",
		"target_farm_id": "402", "operation_id": "source-care", "scope": "2",
		"permit_payload_hash": hex.EncodeToString(action.PayloadHash),
		"target_binding_hash": hex.EncodeToString(action.TargetBindingHash),
		"dispatch_order_id":   admitted.DispatchOrderID,
	} {
		if values[field] != want {
			t.Fatalf("dispatch permit field %s=%q want %q values=%v", field, values[field], want, values)
		}
	}
	for _, field := range []string{"issued_at_authority_ms", "expire_at_authority_ms", "execution_accepted_at_authority_ms"} {
		if values[field] == "" || values[field] == "0" {
			t.Fatalf("dispatch authority timestamp missing: %s=%v", field, values)
		}
	}
	actionResultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		t.Fatal(err)
	}
	resultValues := assertFriendSourceOrder(t, ctx, repositories, group, "RESULT_PUBLISH_ORDER", actionResultID)
	if resultValues["dispatch_order_id"] != admitted.DispatchOrderID {
		t.Fatalf("result order lost dispatch locator: %v", resultValues)
	}
	if len(operationDigest) != sha256.Size*2 || strings.ToLower(operationDigest) != operationDigest {
		t.Fatalf("operation digest is not root-scoped lowercase hex: %q", operationDigest)
	}
	if err := repositories.authority.client.HDel(ctx, group.receipt, ci.field).Err(); err != nil {
		t.Fatal(err)
	}
	recoveredAfterCompaction, err := repositories.Pair.GetFriendActionAdmission(ctx, action)
	if err != nil || recoveredAfterCompaction.DispatchOrderID != admitted.DispatchOrderID || recoveredAfterCompaction.Permit.PermitID != admitted.Permit.PermitID {
		t.Fatalf("dispatch recovery after receipt compaction: %+v/%v", recoveredAfterCompaction, err)
	}
	if outcome, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{
		Identity: redisFriendPublic(401, "DELETE_FRIEND", "source-care-delete"), PayloadHash: redisFriendHash("source-care-delete"),
		CallerPlayerID: 401, PeerPlayerID: 402,
	}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("delete after action admission: %+v/%v", outcome, err)
	}
	recoveredAfterDelete, err := repositories.Pair.GetFriendActionAdmission(ctx, action)
	if err != nil || recoveredAfterDelete.DispatchOrderID != admitted.DispatchOrderID || recoveredAfterDelete.Permit.PermitID != admitted.Permit.PermitID {
		t.Fatalf("dispatch recovery after relation epoch advanced: %+v/%v", recoveredAfterDelete, err)
	}
}

func TestRedisFriendSourceOrderLogicalBytesAndLifecycleCounters(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(351, 352)
	if err != nil {
		t.Fatal(err)
	}
	identity := redisFriendPublic(351, "BLOCK_PLAYER", "source-byte-accounting")
	payload := redisFriendHash("source-byte-accounting")
	operationDigest, err := pairOperationDigest(identity, pairID)
	if err != nil {
		t.Fatal(err)
	}
	resultID, err := stableID("farm-source-order-id-v1", "RESULT_PUBLISH_ORDER", "FRIEND_PAIR", pairID, operationDigest, "1")
	if err != nil {
		t.Fatal(err)
	}
	order, err := newFriendSourceOrder("RESULT_PUBLISH_ORDER", resultID, "RESULT", pairID, operationDigest, "1", identity.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes := friendSourceOrderFixedBytes
	for _, value := range []string{order.id, order.workType, order.blob, order.workRef, order.payloadHash, order.dueMS} {
		expectedBytes += friendSourceOrderFieldOverhead + len(value)
	}
	if got := friendSourceOrderLogicalBytes(order); got != expectedBytes || got <= len(order.blob)+len(order.id)+len(order.workRef) {
		t.Fatalf("source logical bytes=%d want=%d", got, expectedBytes)
	}
	if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: identity, PayloadHash: payload,
		CallerPlayerID: 351, PeerPlayerID: 352}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("block: %+v %v", outcome, err)
	}
	ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := repositories.authority.readFields(ctx, group.meta, "pending_work_count", "pending_work_bytes",
		"receipt_count", "attempt_count", "attempt_bytes")
	if err != nil {
		t.Fatal(err)
	}
	if meta["pending_work_count"] != "1" || meta["pending_work_bytes"] != strconv.Itoa(expectedBytes) ||
		meta["receipt_count"] != "1" || meta["attempt_count"] != "1" {
		t.Fatalf("lifecycle counters=%v expected order bytes=%d", meta, expectedBytes)
	}
	if attemptBytes, err := strconv.ParseInt(meta["attempt_bytes"], 10, 64); err != nil || attemptBytes <= 0 {
		t.Fatalf("attempt byte counter=%q err=%v", meta["attempt_bytes"], err)
	}
	if receipts, err := repositories.authority.client.ZCard(ctx, group.receiptDue).Result(); err != nil || receipts != 1 {
		t.Fatalf("receipt due count=%d err=%v", receipts, err)
	}
	if attempts, err := repositories.authority.client.ZCard(ctx, group.attemptDue).Result(); err != nil || attempts != 1 {
		t.Fatalf("attempt due count=%d err=%v", attempts, err)
	}
}

func TestRedisFriendRootScopedLocatorAndSafeTypedEncoding(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	identity := redisFriendPublic(501, "DELETE_FRIEND", "operation|with:delim")
	payload := redisFriendHash("payload")
	ci, err := repositories.authority.commitIdentity(FriendPairAuthority, "not-a-pair", identity, payload, "DELETE", "501", "502")
	if err == nil {
		t.Fatal("invalid pair root unexpectedly accepted")
	}
	pairID, _, _, err := pairIDForPlayers(501, 502)
	if err != nil {
		t.Fatal(err)
	}
	ci, err = repositories.authority.commitIdentity(FriendPairAuthority, pairID, identity, payload, "DELETE", "501", "502")
	if err != nil {
		t.Fatal(err)
	}
	preimage := appendFriendLP(nil, "501")
	preimage = appendFriendLP(preimage, identity.CommandType)
	preimage = appendFriendLP(preimage, identity.OperationID)
	preimage = appendFriendLP(preimage, string(FriendPairAuthority))
	preimage = appendFriendLP(preimage, pairID)
	want := sha256.Sum256(preimage)
	if ci.digestHex != hex.EncodeToString(want[:]) || ci.field != "op:"+hex.EncodeToString(want[:]) || len(ci.digestHex) != 64 {
		t.Fatalf("root locator mismatch: %+v want=%x", ci, want)
	}
	participant := CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "participant|id", ParticipantStep: "STEP"}
	participantCI, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, participant, payload, "X")
	if err != nil {
		t.Fatal(err)
	}
	participantPreimage := appendFriendLP(nil, "friend-participant-receipt-v1")
	participantPreimage = appendFriendLP(participantPreimage, participant.ParticipantInvocationID)
	participantWant := sha256.Sum256(participantPreimage)
	if participantCI.digestHex != hex.EncodeToString(participantWant[:]) {
		t.Fatalf("participant locator mismatch: %q want=%x", participantCI.digestHex, participantWant)
	}
	fields := []string{"a|b", "x:y", string([]byte{0x1f}), "unicode-朋友"}
	encoded := encodeFriendTyped(fields...)
	decoded, err := decodeFriendTyped([]string{encoded})
	if err != nil || len(decoded) != len(fields) {
		t.Fatalf("typed round trip decode=%v err=%v", decoded, err)
	}
	for i := range fields {
		if decoded[i] != fields[i] {
			t.Fatalf("typed field %d=%q want %q", i, decoded[i], fields[i])
		}
	}
	if err := classifyFriendWriteError(errors.New("ERR Error running script: runtime failure after mutation")); !errors.Is(err, ErrRedisTimeoutUnknown) {
		t.Fatalf("runtime error classification=%v", err)
	}
}

func TestRedisFriendActionAndReactivationBarrier(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(101, 202)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = repositories.Share.GenerateShareToken(ctx, 101, "initial-share", redisFriendPublic(101, "GENERATE_SHARE", "share-initial"), redisFriendHash("initial"), 1)
	if err != nil {
		t.Fatal(err)
	}
	activationIdentity := CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "activate-action", ParticipantStep: "ACTIVATE"}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 101, RedeemerPlayerID: 202, TargetRelationEpoch: 1, TokenID: "initial-share",
		TokenGeneration: 1, OperationID: "activate", NowMs: 2,
	}, activationIdentity, redisFriendHash("activation")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate: %+v %v", outcome, err)
	}
	action := ActionAdmissionCommand{Identity: redisFriendPublic(101, "CARE", "care-1"), PayloadHash: redisFriendHash("care"),
		VisitorPlayerID: 101, OwnerPlayerID: 202, TargetFarmID: 202, Scope: ActionScopeCare,
		TargetBindingHash: redisFriendHash("binding"), PermitTTLMs: 30_000}
	actionResult, err := repositories.Pair.CommitFriendActionAdmission(ctx, action)
	if err != nil || actionResult.Outcome.Status != StatusSucceeded || actionResult.DispatchOrderID == "" {
		t.Fatalf("action admission: %+v %v", actionResult, err)
	}
	if err := VerifyAdmittedPermit(actionResult.Permit); err != nil {
		t.Fatalf("permit: %+v", err)
	}
	recovered, err := repositories.Pair.GetFriendActionAdmission(ctx, action)
	if err != nil || recovered.DispatchOrderID != actionResult.DispatchOrderID || recovered.Permit.PermitID != actionResult.Permit.PermitID {
		t.Fatalf("action recovery: %+v %v", recovered, err)
	}

	deleted, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{Identity: redisFriendPublic(101, "DELETE_FRIEND", "delete-reactivate"),
		PayloadHash: redisFriendHash("delete-reactivate"), CallerPlayerID: 101, PeerPlayerID: 202, NowMs: 3})
	if err != nil || deleted.Status != StatusSucceeded {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	proof, err := repositories.Pair.GetBarrierProof(ctx, 101, 202)
	if !errors.Is(err, ErrBarrierProofMismatch) || proof.PairID != "" {
		t.Fatalf("pending proof: %+v %v", proof, err)
	}
	if outcome, err := repositories.Pair.CommitReactivationGenerationFloors(ctx, 101, 202, 2, 1, 1, 0, 4); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("floors: %+v %v", outcome, err)
	}
	proof, err = repositories.Pair.GetBarrierProof(ctx, 101, 202)
	if err != nil || proof.BarrierRevision != 1 {
		t.Fatalf("ready proof: %+v %v", proof, err)
	}
	rotated, rotateReceipt, outcome, err := repositories.Share.RotateForPairReactivation(ctx, 101, proof, "reactivation-share",
		redisFriendPublic(101, "ROTATE_REACTIVATION", "rotate-reactivation"), redisFriendHash("rotate-reactivation"), 5)
	if err != nil || outcome.Status != StatusSucceeded || rotateReceipt.PairID != pairID || rotated.TokenGeneration != 2 {
		t.Fatalf("reactivation rotate: %+v %+v %+v %v", rotated, rotateReceipt, outcome, err)
	}
	oldRotateReceipt := rotateReceipt
	issuerAfterRotate, err := repositories.Share.GetIssuer(ctx, 101)
	if err != nil {
		t.Fatal(err)
	}
	if storedRotate, ok := issuerAfterRotate.HasRotateReceiptFor(pairID, proof.BarrierRevision); !ok || storedRotate.ReceiptID != rotateReceipt.ReceiptID {
		t.Fatalf("rotate receipt recovery: %+v ok=%v", storedRotate, ok)
	}
	shareGroup, err := repositories.authority.group(FriendShareIssuerAuthority, "101", [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := repositories.authority.client.HExists(ctx, shareGroup.record, "rotate:"+pairID).Result(); err != nil || exists {
		t.Fatalf("rotate receipt leaked into canonical record: %v %v", exists, err)
	}
	for _, name := range mustHKeys(t, ctx, repositories.authority.client, string(shareGroup.record)) {
		if strings.HasPrefix(name, "rotate:") || strings.HasPrefix(name, "validation:") {
			t.Fatalf("unbounded receipt field leaked into issuer record: %q", name)
		}
	}
	if !findShareRotateReceipt(t, ctx, repositories, "101", pairID, rotateReceipt.ReceiptID) {
		t.Fatalf("bounded rotate receipt missing")
	}
	replacedToken, replacedReceipt, replacedOutcome, err := repositories.Share.RotateForPairReactivation(ctx, 101, proof, "reactivation-share-replaced",
		redisFriendPublic(101, "ROTATE_REACTIVATION", "rotate-reactivation-replaced"), redisFriendHash("rotate-reactivation-replaced"), 6)
	if err != nil || replacedOutcome.Status != StatusSucceeded || replacedToken.TokenGeneration != 3 {
		t.Fatalf("replacement rotate: %+v %+v %+v %v", replacedToken, replacedReceipt, replacedOutcome, err)
	}
	issuerAfterReplacement, err := repositories.Share.GetIssuer(ctx, 101)
	if err != nil {
		t.Fatal(err)
	}
	if current, ok := issuerAfterReplacement.HasRotateReceiptFor(pairID, proof.BarrierRevision); !ok || current.ReceiptID != replacedReceipt.ReceiptID || current.OperationID != replacedReceipt.OperationID {
		t.Fatalf("current rotate proof was not replaced: %+v ok=%v", current, ok)
	}
	wrongRotateReceipt := oldRotateReceipt
	wrongRotateReceipt.ReceiptID = "sid1_wrong_rotate_receipt"
	if rejected, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 101, RedeemerPlayerID: 202, TargetRelationEpoch: 3, TokenID: replacedToken.TokenID,
		TokenGeneration: replacedToken.TokenGeneration, RotateReceipt: wrongRotateReceipt, RotateReceiptPresent: true,
		OperationID: "activate-wrong-rotate-receipt", NowMs: 6,
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "activate-wrong-rotate-receipt", ParticipantStep: "ACTIVATE"}, redisFriendHash("wrong-rotate-receipt")); err != nil ||
		rejected.Status != StatusRejected || rejected.Reason != ReasonReactivationRotateRequired {
		t.Fatalf("arbitrary rotate receipt was accepted: %+v %v", rejected, err)
	}
	forgedOperation := replacedReceipt
	forgedOperation.OperationID = "forged-operation"
	if rejected, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 101, RedeemerPlayerID: 202, TargetRelationEpoch: 3, TokenID: replacedToken.TokenID,
		TokenGeneration: replacedToken.TokenGeneration, RotateReceipt: forgedOperation,
		OperationID: "activate-forged-rotate-operation", NowMs: 6,
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "activate-forged-rotate-operation", ParticipantStep: "ACTIVATE"}, redisFriendHash("forged-rotate-operation")); err != nil ||
		rejected.Status != StatusRejected || rejected.Reason != ReasonReactivationRotateRequired {
		t.Fatalf("forged rotate operation was accepted: %+v %v", rejected, err)
	}
	activationPayload := redisFriendHash("activation-again")
	reserveReactivationQuota := func(playerID uint64) {
		reservationID, reservationErr := QuotaReservationID(pairID, playerID, pairID)
		if reservationErr != nil {
			t.Fatal(reservationErr)
		}
		reservation := QuotaReservation{ReservationID: reservationID, WorkflowID: pairID,
			RelationID: pairID, TargetRelationEpoch: 3}
		outcome, reserveErr := repositories.Quota.ReserveFriendSlot(ctx, playerID, reservation,
			participantIdentity(pairID, "RESERVE_FRIEND_SLOT:"+itoa(playerID)), activationPayload, 6)
		if reserveErr != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("reactivation reserve player=%d: %+v %v", playerID, outcome, reserveErr)
		}
	}
	reserveReactivationQuota(101)
	if rejected, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 101, RedeemerPlayerID: 202, TargetRelationEpoch: 3, TokenID: replacedToken.TokenID,
		TokenGeneration: replacedToken.TokenGeneration, RotateReceipt: replacedReceipt,
		OperationID: "activate-missing-high-quota", NowMs: 6,
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "activate-missing-high-quota", ParticipantStep: "ACTIVATE"}, activationPayload); err != nil ||
		rejected.Status != StatusRejected {
		t.Fatalf("unilateral quota proof restored relation: %+v %v", rejected, err)
	}
	if stillDeleted, err := repositories.Pair.GetPair(ctx, 101, 202); err != nil || stillDeleted.Relation.Status != RelationStatusDeleted {
		t.Fatalf("unilateral proof changed pair: %+v %v", stillDeleted, err)
	}
	reserveReactivationQuota(202)
	activatedAgain, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 101, RedeemerPlayerID: 202, TargetRelationEpoch: 3, TokenID: replacedToken.TokenID,
		TokenGeneration: replacedToken.TokenGeneration, RotateReceipt: replacedReceipt, RotateReceiptPresent: false,
		OperationID: "activate-again", NowMs: 6,
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "activate-again", ParticipantStep: "ACTIVATE"}, activationPayload)
	if err != nil || activatedAgain.Status != StatusSucceeded {
		t.Fatalf("reactivate: %+v %v", activatedAgain, err)
	}
	snapshot, err := repositories.Pair.GetPair(ctx, 101, 202)
	if err != nil || snapshot.Relation.Status != RelationStatusActive || snapshot.Relation.RelationEpoch != 3 || snapshot.Barrier.State != BarrierConsumed {
		t.Fatalf("reactivated pair: %+v %v", snapshot, err)
	}
	blocked, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(101, "BLOCK_PLAYER", "block-1"),
		PayloadHash: redisFriendHash("block"), CallerPlayerID: 101, PeerPlayerID: 202, NowMs: 7})
	if err != nil || blocked.Status != StatusSucceeded || blocked.CommittedEpoch != 4 {
		t.Fatalf("block active pair: %+v %v", blocked, err)
	}
	unblocked, err := repositories.Pair.UnblockPlayer(ctx, PairMutationCommand{Identity: redisFriendPublic(101, "UNBLOCK_PLAYER", "unblock-1"),
		PayloadHash: redisFriendHash("unblock"), CallerPlayerID: 101, PeerPlayerID: 202, NowMs: 8})
	if err != nil || unblocked.Status != StatusSucceeded {
		t.Fatalf("unblock: %+v %v", unblocked, err)
	}
}

func TestRedisFriendNOSCRIPTReloadAndDisconnectClassification(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	if err := repositories.authority.client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	reservation := QuotaReservation{ReservationID: "noscript-res", WorkflowID: "noscript-wf", RelationID: "noscript-rel", TargetRelationEpoch: 1}
	outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 909, reservation, redisFriendPublic(909, "RESERVE", "noscript"), redisFriendHash("noscript"), 1)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("NOSCRIPT reload: %+v %v", outcome, err)
	}
	if err := classifyFriendWriteError(errors.New("redis: client is closed")); !errors.Is(err, ErrRedisUnavailableNotSent) {
		t.Fatalf("disconnect classification: %v", err)
	}
	if err := classifyFriendWriteError(errors.New("NOSCRIPT No matching script. Please use EVAL.")); !errors.Is(err, ErrRedisUnavailableNotSent) {
		t.Fatalf("NOSCRIPT classification: %v", err)
	}
	if err := classifyFriendWriteError(context.DeadlineExceeded); !errors.Is(err, ErrRedisTimeoutUnknown) {
		t.Fatalf("timeout classification: %v", err)
	}
}

func TestRedisFriendMalformedScriptReplyIsOutcomeUnknown(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	const malformed = "FriendMalformedReplyRegressionV1"
	repositories.authority.scripts.scripts[malformed] = redis.NewScript("return 'not-an-array'")
	if _, err := repositories.authority.scripts.run(context.Background(), malformed, nil); !errors.Is(err, ErrRedisTimeoutUnknown) {
		t.Fatalf("malformed post-EVAL reply classification=%v", err)
	}
}

func TestRedisFriendBlockUnblockAndOldEpochRead(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(707, 808)
	if err != nil {
		t.Fatal(err)
	}
	identity := CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "block-activate", ParticipantStep: "ACTIVATE"}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 707, RedeemerPlayerID: 808, TargetRelationEpoch: 1, TokenID: "block-token",
		TokenGeneration: 1, OperationID: "block-activate-op",
	}, identity, redisFriendHash("block-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate: %+v %v", outcome, err)
	}
	blocked, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{
		Identity: redisFriendPublic(707, "BLOCK_FRIEND", "block-1"), PayloadHash: redisFriendHash("block-1"),
		CallerPlayerID: 707, PeerPlayerID: 808,
	})
	if err != nil || blocked.Status != StatusSucceeded || blocked.CommittedEpoch != 2 {
		t.Fatalf("block: %+v %v", blocked, err)
	}
	snapshot, err := repositories.Pair.GetPair(ctx, 808, 707)
	if err != nil || snapshot.Relation.Status != RelationStatusDeleted || !snapshot.HasActiveBlock() || snapshot.Barrier.State != BarrierPending {
		t.Fatalf("blocked snapshot: %+v %v", snapshot, err)
	}
	changedAt := snapshot.LowBlocksHigh.CreatedAtMs
	versionAfterBlock := snapshot.RecordVersion
	unblocked, err := repositories.Pair.UnblockPlayer(ctx, PairMutationCommand{
		Identity: redisFriendPublic(707, "UNBLOCK_FRIEND", "unblock-1"), PayloadHash: redisFriendHash("unblock-1"),
		CallerPlayerID: 707, PeerPlayerID: 808,
	})
	if err != nil || unblocked.Status != StatusSucceeded {
		t.Fatalf("unblock: %+v %v", unblocked, err)
	}
	snapshot, err = repositories.Pair.GetPair(ctx, 707, 808)
	if err != nil || snapshot.HasActiveBlock() || snapshot.LowBlocksHigh.BlockEpoch != 2 || snapshot.LowBlocksHigh.RemovedAtMs == 0 ||
		snapshot.LowBlocksHigh.CreatedAtMs != changedAt || snapshot.RecordVersion <= versionAfterBlock {
		t.Fatalf("unblocked snapshot: %+v %v", snapshot, err)
	}
	versionAfterUnblock := snapshot.RecordVersion
	removedAt := snapshot.LowBlocksHigh.RemovedAtMs
	repeatedUnblock, err := repositories.Pair.UnblockPlayer(ctx, PairMutationCommand{
		Identity: redisFriendPublic(707, "UNBLOCK_FRIEND", "unblock-repeat"), PayloadHash: redisFriendHash("unblock-repeat"),
		CallerPlayerID: 707, PeerPlayerID: 808,
	})
	if err != nil || repeatedUnblock.Status != StatusNoChange || repeatedUnblock.Reason != ReasonAlreadyUnblocked {
		t.Fatalf("repeat unblock: %+v %v", repeatedUnblock, err)
	}
	snapshot, err = repositories.Pair.GetPair(ctx, 707, 808)
	if err != nil || snapshot.RecordVersion != versionAfterUnblock || snapshot.LowBlocksHigh.RemovedAtMs != removedAt || snapshot.LowBlocksHigh.CreatedAtMs != changedAt {
		t.Fatalf("repeat unblock changed record/timestamps: %+v %v", snapshot, err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 707, RedeemerPlayerID: 808, TargetRelationEpoch: 2, TokenID: "stale",
		TokenGeneration: 1, OperationID: "stale-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "stale-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("stale")); err != nil || outcome.Status != StatusRejected {
		t.Fatalf("old epoch activation: %+v %v", outcome, err)
	}
}

func TestRedisFriendCompleteTerminationRequiresFloorsAndQuotaCleanup(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	pairID, _, _, err := pairIDForPlayers(1001, 1002)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: 1001, RedeemerPlayerID: 1002, TargetRelationEpoch: 1, TokenID: "termination-token",
		TokenGeneration: 1, OperationID: "termination-activate",
	}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "termination-activate", ParticipantStep: "ACTIVATE"}, redisFriendHash("termination-activate")); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Pair.DeleteFriend(ctx, PairMutationCommand{Identity: redisFriendPublic(1001, "DELETE_FRIEND", "termination-delete"),
		PayloadHash: redisFriendHash("termination-delete"), CallerPlayerID: 1001, PeerPlayerID: 1002}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("delete: %+v %v", outcome, err)
	}
	snapshot, err := repositories.Pair.GetPair(ctx, 1001, 1002)
	if err != nil || snapshot.PendingTermination == nil {
		t.Fatalf("termination snapshot: %+v %v", snapshot, err)
	}
	orderID := snapshot.PendingTermination.OrderID
	if outcome, err := repositories.Pair.CommitReactivationGenerationFloors(ctx, 1001, 1002, 2,
		snapshot.PendingTermination.BarrierRevision, 5, 6, 4); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("floors: %+v %v", outcome, err)
	}
	if outcome, err := repositories.Pair.CompleteTerminationOrder(ctx, 1001, 1002, orderID, 5); err != nil ||
		outcome.Status != StatusNoChange || outcome.Reason != ReasonRelationCleanupPending {
		t.Fatalf("premature completion: %+v %v", outcome, err)
	}
	final, err := repositories.Pair.GetPair(ctx, 1001, 1002)
	if err != nil || final.PendingTermination == nil || final.PendingTermination.Completed ||
		final.PendingTermination.QuotaReleasedLow || final.PendingTermination.QuotaReleasedHigh {
		t.Fatalf("premature completion mutated cleanup state: %+v %v", final, err)
	}
}

func TestRedisFriendTerminationOrderCompletesAfterBarrierAndQuotaCleanup(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	service, err := NewDomainService(repositories.Pair, repositories.Quota, repositories.Share)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := repositories.Share.GenerateShareToken(ctx, 1101, "termination-service-token",
		redisFriendPublic(1101, "GENERATE_SHARE", "termination-service-generate"), redisFriendHash("termination-service-generate"), 1)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RedeemShare(ctx, RedeemShareCommand{RedeemerPlayerID: 1102, InviterPlayerID: 1101,
		TokenID: token.TokenID, TokenGeneration: token.TokenGeneration, OperationID: "termination-service-redeem",
		PayloadHash: redisFriendHash("termination-service-redeem"), NowMs: 2})
	if err != nil || result.Outcome.Status != StatusSucceeded {
		t.Fatalf("redeem = %+v/%v", result, err)
	}
	if outcome, err := service.DeleteFriend(ctx, 1101, 1102, "termination-service-delete",
		redisFriendHash("termination-service-delete"), 3); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("delete = %+v/%v", outcome, err)
	}
	if outcome, err := service.ExecuteTerminationOrder(ctx, 1101, 1102, 4); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("execute termination = %+v/%v", outcome, err)
	}
	pair, err := repositories.Pair.GetPair(ctx, 1101, 1102)
	if err != nil || pair.PendingTermination == nil || !pair.PendingTermination.Completed || pair.Barrier.State != BarrierReady {
		t.Fatalf("completed termination pair = %+v/%v", pair, err)
	}
	for _, playerID := range []uint64{1101, 1102} {
		quota, quotaErr := repositories.Quota.GetQuota(ctx, playerID)
		if quotaErr != nil || len(quota.ActiveRelations) != 0 || quota.OpenReservations != 0 {
			t.Fatalf("player %d quota cleanup = %+v/%v", playerID, quota, quotaErr)
		}
	}
}

func TestRedisFriendReplayFastPathRequiresCommitProof(t *testing.T) {
	t.Run("quota reserve", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		reservation := QuotaReservation{ReservationID: "replay-proof-res", WorkflowID: "replay-proof-wf",
			RelationID: "replay-proof-rel", TargetRelationEpoch: 1}
		identity := redisFriendPublic(1701, "RESERVE", "replay-proof-reserve")
		payload := redisFriendHash("replay-proof-reserve")
		if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1701, reservation, identity, payload, 1); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("reserve: %+v %v", outcome, err)
		}
		ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1701", identity, payload,
			"RESERVE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, "1",
			"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1701", ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		candidateHash, err := repositories.authority.client.HGet(ctx, group.attempt,
			"cursor:"+ci.digestHex+":candidate_hash").Result()
		if err != nil {
			t.Fatal(err)
		}
		markerDigest := friendCommitMarkerDigest(candidateHash, 1)
		if err := repositories.authority.client.HDel(ctx, group.attempt,
			"commit_marker:"+markerDigest+":complete", "commit_marker:"+markerDigest+":proof").Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1701, reservation, identity, payload, 2); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("replay without FCM2/FRS3 proof=%v, want reconcile", err)
		}
	})

	t.Run("action admission", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		pairID, _, _, err := pairIDForPlayers(1703, 1704)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, err := repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
			InviterPlayerID: 1703, RedeemerPlayerID: 1704, TargetRelationEpoch: 1, TokenID: "replay-proof-token",
			TokenGeneration: 1, OperationID: "replay-proof-activate",
		}, CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "replay-proof-activate", ParticipantStep: "ACTIVATE"},
			redisFriendHash("replay-proof-activate")); err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("activate: %+v %v", outcome, err)
		}
		action := ActionAdmissionCommand{Identity: redisFriendPublic(1703, "CARE", "replay-proof-action"),
			PayloadHash: redisFriendHash("replay-proof-action"), VisitorPlayerID: 1703, OwnerPlayerID: 1704,
			TargetFarmID: 1704, Scope: ActionScopeCare, TargetBindingHash: redisFriendHash("replay-proof-binding"), PermitTTLMs: 60_000}
		if admitted, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); err != nil || admitted.Outcome.Status != StatusSucceeded {
			t.Fatalf("admit: %+v %v", admitted, err)
		}
		ci, err := repositories.authority.commitIdentity(FriendPairAuthority, pairID, action.Identity, action.PayloadHash)
		if err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendPairAuthority, pairID, ci.digest)
		if err != nil {
			t.Fatal(err)
		}
		candidateHash, err := repositories.authority.client.HGet(ctx, group.attempt,
			"cursor:"+ci.digestHex+":candidate_hash").Result()
		if err != nil {
			t.Fatal(err)
		}
		markerDigest := friendCommitMarkerDigest(candidateHash, 1)
		if err := repositories.authority.client.HDel(ctx, group.attempt,
			"commit_marker:"+markerDigest+":complete", "commit_marker:"+markerDigest+":proof").Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Pair.CommitFriendActionAdmission(ctx, action); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("replay without FCM2/FRS3 proof=%v, want reconcile", err)
		}
	})
}

func TestRedisFriendCanonicalReadNumbersFailClosed(t *testing.T) {
	t.Run("quota business digest", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		reservation := QuotaReservation{ReservationID: "quota-digest-res", WorkflowID: "quota-digest-wf",
			RelationID: "quota-digest-rel", TargetRelationEpoch: 1}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1711, reservation,
			redisFriendPublic(1711, "RESERVE", "quota-digest"), redisFriendHash("quota-digest"), 1); err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1711", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		if err := repositories.authority.client.HSet(ctx, group.record, "_meta:business_state_digest",
			strings.Repeat("z", sha256.Size*2)).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := repositories.Quota.GetQuota(ctx, 1711); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("invalid quota digest=%v, want reconcile", err)
		}
	})

	t.Run("quota updated timestamp", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		reservation := QuotaReservation{ReservationID: "quota-time-res", WorkflowID: "quota-time-wf",
			RelationID: "quota-time-rel", TargetRelationEpoch: 1}
		if _, err := repositories.Quota.ReserveFriendSlot(ctx, 1712, reservation,
			redisFriendPublic(1712, "RESERVE", "quota-time"), redisFriendHash("quota-time"), 1); err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendQuotaAuthority, "1712", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		setFriendHashFieldKeepingLogicalBytes(t, ctx, repositories.authority.client, group.record,
			"_meta:updated_at_ms", "01")
		if _, err := repositories.Quota.GetQuota(ctx, 1712); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("non-canonical quota timestamp=%v, want reconcile", err)
		}
	})

	t.Run("share updated timestamp", func(t *testing.T) {
		repositories, server := newFriendRedisTestRepositories(t, 300)
		defer server.Close()
		ctx := context.Background()
		if _, _, err := repositories.Share.GenerateShareToken(ctx, 1713, "share-time-token",
			redisFriendPublic(1713, "GENERATE_SHARE", "share-time"), redisFriendHash("share-time"), 1); err != nil {
			t.Fatal(err)
		}
		group, err := repositories.authority.group(FriendShareIssuerAuthority, "1713", [32]byte{})
		if err != nil {
			t.Fatal(err)
		}
		updatedAt, err := repositories.authority.client.HGet(ctx, group.record, "_meta:updated_at_ms").Result()
		if err != nil {
			t.Fatal(err)
		}
		bad := "-" + strings.Repeat("1", maxFriendTestInt(1, len(updatedAt)-1))
		setFriendHashFieldKeepingLogicalBytes(t, ctx, repositories.authority.client, group.record,
			"_meta:updated_at_ms", bad)
		if _, err := repositories.Share.GetIssuer(ctx, 1713); !errors.Is(err, ErrFriendReconcile) {
			t.Fatalf("negative share timestamp=%v, want reconcile", err)
		}
	})
}

func TestRedisFriendKnownPairUnknownCanonicalFieldsFailsClosed(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	if outcome, err := repositories.Pair.BlockPlayer(ctx, PairMutationCommand{
		Identity: redisFriendPublic(1715, "BLOCK_PLAYER", "pair-unknown-fields"), PayloadHash: redisFriendHash("pair-unknown-fields"),
		CallerPlayerID: 1715, PeerPlayerID: 1716,
	}); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("block: %+v %v", outcome, err)
	}
	pairID, _, _, err := pairIDForPlayers(1715, 1716)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pairID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	fields := mustHKeys(t, ctx, repositories.authority.client, group.record)
	if len(fields) == 0 {
		t.Fatal("pair record unexpectedly empty before corruption")
	}
	if err := repositories.authority.client.HDel(ctx, group.record, fields...).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HSet(ctx, group.record, "unexpected", "field").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Pair.GetPair(ctx, 1715, 1716); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("known pair with unknown canonical field=%v, want reconcile", err)
	}
	if _, err := repositories.Pair.ValidatePairForAdd(ctx, 1715, 1716); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("corrupt pair became create-eligible: %v", err)
	}
}

func TestRedisFriendFRS3ReceiptEpochRequiresCanonicalUnsigned(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	reservation := QuotaReservation{ReservationID: "frs3-epoch-res", WorkflowID: "frs3-epoch-wf",
		RelationID: "frs3-epoch-rel", TargetRelationEpoch: 1}
	identity := redisFriendPublic(1717, "RESERVE", "frs3-epoch")
	payload := redisFriendHash("frs3-epoch")
	if outcome, err := repositories.Quota.ReserveFriendSlot(ctx, 1717, reservation, identity, payload, 1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve: %+v %v", outcome, err)
	}
	ci, err := repositories.authority.commitIdentity(FriendQuotaAuthority, "1717", identity, payload,
		"RESERVE", reservation.ReservationID, reservation.WorkflowID, reservation.RelationID, "1",
		"record_version", "0", "authority_time", "REDIS_SERVER_TIME")
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, "1717", ci.digest)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := repositories.authority.client.HGet(ctx, group.attempt,
		"cursor:"+ci.digestHex+":attempt_id").Result()
	if err != nil {
		t.Fatal(err)
	}
	attemptDue := friendAttemptDueMember(FriendQuotaAuthority, "1717", ci.digestHex, 1)
	if resolved, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "1717", ci, 1,
		attemptID, attemptDue); err != nil || resolved.Kind != "QUOTA_RESERVE" {
		t.Fatalf("compact resolution: %+v %v", resolved, err)
	}
	resolutionField := "resolve:" + ci.digestHex + ":1"
	resolution, err := repositories.authority.client.HGet(ctx, group.resolution, resolutionField).Result()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := repositories.authority.client.HGet(ctx, group.receipt, ci.field).Result()
	if err != nil {
		t.Fatal(err)
	}
	badResolution := rewriteFriendResolutionEpochForTest(t, resolution, "01")
	badReceipt := rewriteFriendReceiptEpochForTest(t, receipt, "01")
	if err := repositories.authority.client.HSet(ctx, group.resolution, resolutionField, badResolution).Err(); err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HSet(ctx, group.receipt, ci.field, badReceipt).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.authority.resolveUnknown(ctx, group, FriendQuotaAuthority, "1717", ci, 1,
		attemptID, attemptDue); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("FRS3/FR3 epoch 01 replay=%v, want reconcile", err)
	}
}
