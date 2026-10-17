package friend

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func sameShareBucketInviters(t *testing.T, repositories *RedisFriendshipRepositories) (uint64, uint64) {
	t.Helper()
	first := uint64(130_000_001)
	firstRef, err := repositories.authority.keys.BucketFor(FriendShareIssuerAuthority, fmt.Sprintf("%d", first))
	if err != nil {
		t.Fatal(err)
	}
	for candidate := first + 1; candidate < first+100_000; candidate++ {
		ref, refErr := repositories.authority.keys.BucketFor(FriendShareIssuerAuthority, fmt.Sprintf("%d", candidate))
		if refErr != nil {
			t.Fatal(refErr)
		}
		if ref.BucketID == firstRef.BucketID {
			return first, candidate
		}
	}
	t.Fatal("could not find two Share issuers in one bucket")
	return 0, 0
}

func TestRedisShareGetIssuerSkipsForeignAuthorityDueEntriesInSameBucket(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	first, second := sameShareBucketInviters(t, repositories)
	for _, inviter := range []uint64{first, second} {
		tokenID := fmt.Sprintf("same-bucket-token-%d", inviter)
		identity := redisFriendPublic(inviter, "GENERATE_SHARE", fmt.Sprintf("same-bucket-operation-%d", inviter))
		token, outcome, err := repositories.Share.GenerateShareToken(ctx, inviter, tokenID, identity,
			redisFriendHash(tokenID), 100)
		if err != nil || outcome.Status != StatusSucceeded || token.TokenID != tokenID {
			t.Fatalf("generate inviter=%d token=%+v outcome=%+v err=%v", inviter, token, outcome, err)
		}
	}
	for _, inviter := range []uint64{first, second} {
		snapshot, err := repositories.Share.GetIssuer(ctx, inviter)
		if err != nil || snapshot.InviterPlayerID != inviter || snapshot.Current.TokenID != fmt.Sprintf("same-bucket-token-%d", inviter) {
			t.Fatalf("issuer=%d snapshot=%+v err=%v", inviter, snapshot, err)
		}
	}
}

func TestRedisShareGetIssuerStillRejectsWrongAuthorityKindDueEntry(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	inviter := uint64(140_000_001)
	tokenID := "wrong-kind-token"
	identity := redisFriendPublic(inviter, "GENERATE_SHARE", "wrong-kind-operation")
	if _, outcome, err := repositories.Share.GenerateShareToken(ctx, inviter, tokenID, identity, redisFriendHash(tokenID), 100); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("generate outcome=%+v err=%v", outcome, err)
	}
	authorityID := fmt.Sprintf("%d", inviter)
	group, err := repositories.authority.group(FriendShareIssuerAuthority, authorityID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	foreignMember := friendReceiptDueMember(FriendQuotaAuthority, authorityID, "op:"+fmt.Sprintf("%064x", 1))
	if err := repositories.authority.client.ZAdd(ctx, group.receiptDue, redis.Z{Score: 100, Member: foreignMember}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Share.GetIssuer(ctx, inviter); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("wrong-kind due err=%v want ErrFriendReconcile", err)
	}
}
