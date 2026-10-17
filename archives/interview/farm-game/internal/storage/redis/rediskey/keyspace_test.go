package rediskey

import (
	"errors"
	"strings"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/workidentity"
)

func TestAuthorityKeyGoldenAndDomainSeparation(t *testing.T) {
	profile := testPhysicalProfile(t)
	resolver := goldenShardResolver{}
	keys, err := NewAuthorityKeys(profile, resolver)
	if err != nil {
		t.Fatal(err)
	}
	player, _ := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001})
	farm, _ := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 10001})
	worker, err := profile.AuthorityBucketForShard(
		resolver,
		actorrouting.DomainFarmWorker,
		42,
	)
	if err != nil {
		t.Fatal(err)
	}
	actorKey, err := keys.ActorRecord(player, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if want := RedisKey("{classic-farm:prod:z1:auth:player:v1:s:42}:actor:10001"); actorKey != want {
		t.Fatalf("actor key=%q want=%q", actorKey, want)
	}
	shardKey, _ := keys.ShardAuthority(player)
	dueKey, _ := keys.SourceDueIndex(player)
	scheduleDueKey, _ := keys.SourceScheduleDueIndex(player)
	receiptDueKey, _ := keys.ReceiptDueIndex(player)
	metaKey, _ := keys.BucketMeta(player)
	leaseKey, _ := keys.BucketScanLease(player)
	if err := keys.AssertSameSlot(actorKey, shardKey, dueKey, scheduleDueKey, receiptDueKey, metaKey, leaseKey); err != nil {
		t.Fatal(err)
	}
	farmKey, _ := keys.ActorRecord(farm, 10001)
	workerKey, err := keys.ShardAuthority(worker)
	if err != nil {
		t.Fatal(err)
	}
	if want := RedisKey("{classic-farm:prod:z1:auth:farm-worker:v1:s:42}:shard-authority"); workerKey != want {
		t.Fatalf("worker shard key=%q want=%q", workerKey, want)
	}
	if HashTag(workerKey) == HashTag(farmKey) {
		t.Fatal("FARM and FARM_WORKER shard-authority keyspaces merged")
	}
	if ClusterSlot(playerKey(t, keys, player)) == ClusterSlot(farmKey) && HashTag(actorKey) == HashTag(farmKey) {
		t.Fatal("PLAYER and FARM keyspaces merged")
	}
	if !errors.Is(keys.AssertSameSlot(actorKey, farmKey), ErrCrossSlot) {
		t.Fatal("cross-domain keys accepted for one Lua operation")
	}
	if _, err := profile.AuthorityBucketForActor(
		resolver,
		actorrouting.ActorAddress{Domain: actorrouting.DomainFarmWorker, ActorID: 10001},
	); !errors.Is(err, ErrInvalidBucketRef) {
		t.Fatalf("FARM_WORKER actor bucket accepted: %v", err)
	}
	if got, want := ClusterSlot(actorKey), uint16(122); got != want {
		t.Fatalf("authority slot golden changed: got=%d want=%d", got, want)
	}
}

func TestRunNamespacePreservesBucketSlotIsolation(t *testing.T) {
	resolver := goldenShardResolver{}
	production := testPhysicalProfile(t)
	runNamespace, err := NewNamespace("l2r2a1b2c3d4", "z1")
	if err != nil {
		t.Fatal(err)
	}
	runProfile := production
	runProfile.Namespace = runNamespace
	runKeys, err := NewAuthorityKeys(runProfile, resolver)
	if err != nil {
		t.Fatal(err)
	}

	playerRef, err := runProfile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001})
	if err != nil {
		t.Fatal(err)
	}
	farmRef, err := runProfile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002})
	if err != nil {
		t.Fatal(err)
	}
	playerRecord, _ := runKeys.ActorRecord(playerRef, 10001)
	playerAuthority, _ := runKeys.ShardAuthority(playerRef)
	farmRecord, _ := runKeys.ActorRecord(farmRef, 20002)

	if err := runKeys.AssertSameSlot(playerRecord, playerAuthority); err != nil {
		t.Fatalf("same authority bucket lost colocation: %v", err)
	}
	if HashTag(playerRecord) == HashTag(farmRecord) || ClusterSlot(playerRecord) == ClusterSlot(farmRecord) {
		t.Fatalf("different run buckets collapsed to one slot: player=%q farm=%q", playerRecord, farmRecord)
	}
	for _, key := range []RedisKey{playerRecord, playerAuthority, farmRecord} {
		if !strings.Contains(key.String(), "classic-farm:l2r2a1b2c3d4:z1:") {
			t.Fatalf("key escaped run namespace: %q", key)
		}
		if HashTag(key) == "l2r2a1b2c3d4" {
			t.Fatalf("run namespace replaced the business bucket hash tag: %q", key)
		}
	}

	productionKeys, err := NewAuthorityKeys(production, resolver)
	if err != nil {
		t.Fatal(err)
	}
	productionPlayerRef, _ := production.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001})
	productionFarmRef, _ := production.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002})
	productionPlayer, _ := productionKeys.ActorRecord(productionPlayerRef, 10001)
	productionFarm, _ := productionKeys.ActorRecord(productionFarmRef, 20002)
	if HashTag(productionPlayer) == HashTag(productionFarm) || HashTag(playerRecord) == HashTag(farmRecord) {
		t.Fatal("namespace change degraded bucket separation")
	}
}

func TestSourceOrderAndWorkflowGolden(t *testing.T) {
	profile := testPhysicalProfile(t)
	resolver := goldenShardResolver{}
	authority, _ := NewAuthorityKeys(profile, resolver)
	ref, _ := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002})
	orderID, err := workidentity.SourceOrderID("CARE_REWARD", "FARM", 20002, "fact-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	order, err := authority.SourceOrder(ref, "CARE_REWARD", orderID)
	if err != nil {
		t.Fatal(err)
	}
	due, _ := authority.SourceDueIndex(ref)
	scheduleDue, _ := authority.SourceScheduleDueIndex(ref)
	if err := authority.AssertSameSlot(order, due); err != nil {
		t.Fatal(err)
	}
	if err := authority.AssertSameSlot(order, due, scheduleDue); err != nil {
		t.Fatal(err)
	}

	workflowID, _ := workidentity.WorkflowID(10001, "PLANT", "op-0001")
	wfRef, err := profile.WorkflowBucket(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if wfRef.BucketID() != 65 {
		t.Fatalf("workflow bucket golden changed: %d", wfRef.BucketID())
	}
	wfKeys, _ := NewWorkflowKeys(profile)
	record, _ := wfKeys.Work(wfRef)
	wfDue, _ := wfKeys.DueIndex(wfRef)
	if err := wfKeys.AssertSameSlot(record, wfDue); err != nil {
		t.Fatal(err)
	}
	if want := RedisKey("{classic-farm:prod:z1:wf:v1:b:65}:work:" + workflowID); record != want {
		t.Fatalf("workflow key=%q want=%q", record, want)
	}
}

func TestRuntimeProjectionAreSeparateNamespaces(t *testing.T) {
	ns, _ := NewNamespace("staging", "z2")
	runtime, _ := NewRuntimeKeys(ns)
	projection, _ := NewProjectionKeys(ns)
	session, _ := runtime.Session(10001)
	navigation, _ := runtime.FriendNavigation(10001, 7)
	result, _ := projection.ResultDirectory(10001)
	opportunity, _ := projection.FarmOpportunity(20002)
	ticket, _ := runtime.ConnectionTicket(10001, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if session != "{classic-farm:staging:z2:rt:session:v1:p:10001}:authority" ||
		ticket != "{classic-farm:staging:z2:rt:session:v1:p:10001}:ticket:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" ||
		navigation != "classic-farm:staging:z2:rt:friend-nav:v1:10001:7" ||
		result != "classic-farm:staging:z2:proj:result:v2:10001" ||
		opportunity != "classic-farm:staging:z2:proj:farm-opportunity:v1:20002" {
		t.Fatalf("runtime/projection key drift: %q %q %q %q %q", session, ticket, navigation, result, opportunity)
	}
	if err := AssertSameSlot(session, ticket); err != nil {
		t.Fatalf("ticket/session atomic group not colocated: %v", err)
	}
	if HashTag(navigation) != string(navigation) || HashTag(result) != string(result) {
		t.Fatal("friend navigation/projection unexpectedly acquired a hash tag")
	}
}

func TestKeyBuildersRejectInjectionAndInvalidComponents(t *testing.T) {
	for _, tc := range []struct{ env, zone string }{
		{"prod}{evil", "z1"},
		{"prod", "z1:other"},
		{"Prod", "z1"},
	} {
		if _, err := NewNamespace(tc.env, tc.zone); !errors.Is(err, ErrInvalidNamespace) {
			t.Fatalf("namespace injection accepted: %+v err=%v", tc, err)
		}
	}
	profile := testPhysicalProfile(t)
	resolver := goldenShardResolver{}
	keys, _ := NewAuthorityKeys(profile, resolver)
	ref, _ := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 30003})
	if _, err := keys.SourceOrder(ref, "CARE}:{", "sid1_bad"); !errors.Is(err, ErrInvalidComponent) {
		t.Fatalf("unsafe order accepted: %v", err)
	}
	if _, err := keys.ActorRecord(ref, 0); !errors.Is(err, ErrInvalidComponent) {
		t.Fatalf("zero actor accepted: %v", err)
	}
	wrongRef, _ := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002})
	if _, err := keys.ActorRecord(wrongRef, 10001); !errors.Is(err, ErrInvalidBucketRef) {
		t.Fatalf("actor accepted a caller-supplied wrong bucket: %v", err)
	}
	if _, err := profile.AuthorityBucketForShard(resolver, actorrouting.DomainFarm, 1024); !errors.Is(err, ErrInvalidBucketRef) {
		t.Fatalf("out-of-range scan bucket accepted: %v", err)
	}
	runtime, _ := NewRuntimeKeys(profile.Namespace)
	if _, err := runtime.ConnectionTicket(10001, "{unsafe}"); !errors.Is(err, ErrInvalidComponent) {
		t.Fatalf("unsafe ticket hash accepted: %v", err)
	}
}

func TestWorkflowRecordRefIsBoundToWorkflowIdentity(t *testing.T) {
	profile := testPhysicalProfile(t)
	keys, _ := NewWorkflowKeys(profile)
	firstID, _ := workidentity.WorkflowID(10001, "PLANT", "op-1")
	secondID, _ := workidentity.WorkflowID(10001, "PLANT", "op-2")
	ref, _ := profile.WorkflowBucket(firstID)
	ref.workflowID = secondID // package-level fault injection; callers cannot access this field.
	if _, err := keys.Work(ref); !errors.Is(err, ErrInvalidBucketRef) {
		t.Fatalf("workflow record accepted a mismatched bucket identity: %v", err)
	}
	scanRef, _ := profile.WorkflowBucketAt(0)
	if _, err := keys.Work(scanRef); !errors.Is(err, ErrInvalidComponent) {
		t.Fatalf("scan-only bucket built a workflow record: %v", err)
	}
}

func TestRedisCRC16KnownVector(t *testing.T) {
	if got := ClusterSlot(RedisKey("123456789")); got != 12739 {
		t.Fatalf("CRC16-XMODEM vector changed: %d", got)
	}
	left := RedisKey("{user1000}.following")
	right := RedisKey("{user1000}.followers")
	if ClusterSlot(left) != ClusterSlot(right) || AssertSameSlot(left, right) != nil {
		t.Fatal("Redis hash tag colocation failed")
	}
}

func playerKey(t *testing.T, keys AuthorityKeys, ref AuthorityBucketRef) RedisKey {
	t.Helper()
	key, err := keys.ActorRecord(ref, 10001)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

type goldenShardResolver struct{}

func (goldenShardResolver) Profile() actorrouting.HashProfile {
	return actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608030001}
}

func (goldenShardResolver) ShardFor(_ actorrouting.RouteDomain, actorID uint64) (uint32, error) {
	switch actorID {
	case 10001:
		return 42, nil
	case 20002:
		return 7, nil
	case 30003:
		return 1, nil
	default:
		return 0, actorrouting.ErrInvalidAddress
	}
}

func (goldenShardResolver) ShardCount(domain actorrouting.RouteDomain) (uint32, error) {
	switch domain {
	case actorrouting.DomainPlayer, actorrouting.DomainFarm, actorrouting.DomainFarmWorker:
		return 1024, nil
	default:
		return 0, actorrouting.ErrInvalidAddress
	}
}

type invalidProfileShardResolver struct{ goldenShardResolver }

func (invalidProfileShardResolver) Profile() actorrouting.HashProfile {
	return actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2}
}

func TestAuthorityKeysRejectUnsupportedRoutingProfile(t *testing.T) {
	profile := testPhysicalProfile(t)
	resolver := invalidProfileShardResolver{}
	if _, err := NewAuthorityKeys(profile, resolver); !errors.Is(err, ErrInvalidPhysicalProfile) {
		t.Fatalf("NewAuthorityKeys accepted missing hash seed: %v", err)
	}
	if _, err := profile.AuthorityBucketForActor(resolver, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}); !errors.Is(err, ErrInvalidPhysicalProfile) {
		t.Fatalf("AuthorityBucketForActor accepted missing hash seed: %v", err)
	}
}

func testPhysicalProfile(t *testing.T) PhysicalProfile {
	t.Helper()
	ns, err := NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := PhysicalProfile{
		ProfileVersion:         "redis-physical-v1",
		Namespace:              ns,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    256,
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	return profile
}
