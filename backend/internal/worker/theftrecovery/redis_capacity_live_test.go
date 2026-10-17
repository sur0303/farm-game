package theftrecovery

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestPairMixedTerminalLifecycleOver64DoesNotLeakCapacity(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	runPairMixedTerminalCapacity(t, client, "unit", "theftcapacity")
}

func TestPairMixedTerminalLifecycleOver64RealRedis(t *testing.T) {
	address := os.Getenv("FRIEND_REDIS_LIVE_ADDR")
	if address == "" {
		t.Skip("FRIEND_REDIS_LIVE_ADDR is not configured")
	}
	client := redis.NewClient(&redis.Options{Addr: address, Password: os.Getenv("FRIEND_REDIS_LIVE_PASSWORD")})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("real Redis ping: %v", err)
	}
	zone := "theftcap" + strconv.FormatInt(time.Now().UnixNano(), 10)
	runPairMixedTerminalCapacity(t, client, "live", zone)
	t.Log("REAL_REDIS_PASS: 80 mixed terminal Pair operations released all source-order count, bytes, and due members")
}

func runPairMixedTerminalCapacity(t *testing.T, client redis.UniversalClient, environment, zone string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	namespace, err := rediskey.NewNamespace(environment, zone)
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		var cursor uint64
		pattern := "classic-farm:" + environment + ":" + zone + ":*"
		for {
			keys, next, scanErr := client.Scan(cleanupCtx, cursor, pattern, 256).Result()
			if scanErr != nil {
				return
			}
			if len(keys) != 0 {
				_ = client.Del(cleanupCtx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
	const visitor, activeOwner = uint64(81001), uint64(82002)
	pairID, err := friend.RelationIDFor(visitor, activeOwner)
	if err != nil {
		t.Fatal(err)
	}
	activationHash := sha256.Sum256([]byte("capacity-activation"))
	activated, err := repositories.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: activeOwner, TargetRelationEpoch: 1,
		TokenID: "capacity-token", TokenGeneration: 1, OperationID: "capacity-activation",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "capacity-activation", ParticipantStep: "ACTIVATE"}, activationHash[:])
	if err != nil || activated.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", activated, err)
	}
	cycleID, err := workidentity.StableOpaqueID("capacity-cycle", "one")
	if err != nil {
		t.Fatal(err)
	}
	dispatches, err := NewFriendPairDispatchRepository(FriendPairDispatchRepositoryConfig{
		Repository: repositories, MaxBuckets: 1,
		TargetDecode: func(context.Context, friend.PairRecoverySourceOrder, friend.ActionPermitSnapshot) (FriendPairTheftTarget, error) {
			return FriendPairTheftTarget{PlotID: 3, CropCycleID: cycleID}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	leases, err := NewFriendPairWorkLeaseRepository(repositories, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	terminals := []string{"SUCCEEDED", "NO_CHANGE", "REJECTED"}
	for index := 0; index < 80; index++ {
		owner := activeOwner
		terminal := terminals[index%len(terminals)]
		operationID := fmt.Sprintf("capacity-operation-%02d", index)
		payloadHash := sha256.Sum256([]byte("payload-" + operationID))
		targetHash := sha256.Sum256([]byte("target-" + operationID))
		admission, err := repositories.Pair.CommitFriendActionAdmission(ctx, friend.ActionAdmissionCommand{
			Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
			PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
			Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash[:], PermitTTLMs: 1000, WorkflowTTLMs: 60_000,
		})
		if err != nil {
			t.Fatalf("operation %d admission: %v", index, err)
		}
		binding, found, err := repositories.LookupPairActionResult(ctx, visitor, owner, operationID)
		if err != nil || !found {
			t.Fatalf("operation %d binding=%+v found=%v err=%v", index, binding, found, err)
		}
		if admission.Outcome.Status != friend.StatusSucceeded || binding.AdmissionStatus != friend.StatusSucceeded {
			t.Fatalf("operation %d accepted admission=%+v binding=%+v", index, admission, binding)
		}
		order, err := dispatches.LoadByID(ctx, admission.DispatchOrderID)
		if err != nil {
			t.Fatalf("operation %d dispatch: %v", index, err)
		}
		claim, err := leases.Claim(ctx, ClaimRequest{Order: order, HolderID: "capacity-worker", BootID: "capacity-boot",
			LeaseToken: "capacity-token", RequestedTTL: 10 * time.Second})
		if err != nil || claim.Status != LeaseClaimed {
			t.Fatalf("operation %d claim=%+v err=%v", index, claim, err)
		}
		proof := sha256.Sum256([]byte("dispatch-proof-" + operationID))
		completed, err := leases.Complete(ctx, CompleteRequest{Order: order,
			Fence: LeaseFence{HolderID: "capacity-worker", BootID: "capacity-boot", LeaseToken: "capacity-token",
				LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion},
			Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order), ResultProofHash: proof[:]}})
		if err != nil || completed.Status != LeaseCompleted {
			t.Fatalf("operation %d dispatch completion=%+v err=%v", index, completed, err)
		}
		resultProof := sha256.Sum256([]byte("result-proof-" + operationID))
		if err := repositories.CompletePairActionResultOrder(ctx, binding, terminal, resultProof[:]); err != nil {
			t.Fatalf("operation %d result completion: %v", index, err)
		}
		metaKey, _ := repositories.PairBucketMetaKey(binding.BucketID)
		pending, err := client.HMGet(ctx, metaKey, "pending_work_count", "pending_work_bytes").Result()
		if err != nil || len(pending) != 2 || pending[0] != "0" || pending[1] != "0" {
			t.Fatalf("operation %d pending=%v err=%v", index, pending, err)
		}
		dueKey, _ := repositories.PairDueIndexKey(binding.BucketID)
		if due, err := client.ZCard(ctx, dueKey).Result(); err != nil || due != 0 {
			t.Fatalf("operation %d due=%d err=%v", index, due, err)
		}
	}
}
