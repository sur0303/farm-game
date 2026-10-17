package authority

import (
	"strings"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// Script goldens freeze the exact Lua bytes behind every registered SHA-1.
// Any semantic change must land as a new script version, not an edit.
func TestScriptSHAGoldens(t *testing.T) {
	want := map[string]string{
		ScriptClaimShardAuthority:          "e59048955a296ed27c2442caa71ad00f919cc1d1",
		ScriptRenewShardAuthority:          "a43b8d6bd290e26b79ad8339a793ae56d24a62a1",
		ScriptTransferShardAuthority:       "7cdfba89a030b50df036d390339704c88b88cc2f",
		ScriptTakeoverExpiredAuthority:     "8de0d49e7682a1b94695d7d9ba0f0d8262e4ded5",
		ScriptUnassignShardAuthority:       "ab4ae3545e26dd97fa547f1942d8530523ad61f8",
		ScriptLoadAndClaimActorV5:          "b717c2b33bd81b57917c06ce9ad2ab53d9579bfc",
		ScriptLoadAndClaimActor:            "ecf088c389973dd2141e99fb68d7a88be1cb4ad5",
		ScriptCommitActorWithOrdersV12:     "6250da8c59b478d1c568c1e74914ffc0c2f2c6dd",
		"CommitActorMutationWithOrdersV13": "a8750a844a9e8625e2099cfb71f4b782c0367a1b",
		"CommitActorMutationWithOrdersV15": "60eed6818e873ab7277b882d933b190f57f30313",
		ScriptCommitActorWithOrdersExact:   "b4c1136d186d106181c6346248dc5d30fa89e10f",
		ScriptFenceAndClaimActor:           "bc61b73e8c15c86dbdbd6f36de1a2e7bc9e30f04",
		ScriptClaimWorkV5:                  "ef7b1ad783c344f32a9cfd9b3cd6466ce46bb49f",
		ScriptRenewWorkLease:               "e3057fed3f3105ce43aebf37358d8f1c7d214421",
		ScriptCompleteWorkV5:               "4973bb594b3d2f533f16ae9d9bb88348dee75e2e",
		ScriptClaimAndCompleteWorkV3:       "f425c5fe1068036a1a9e4ac700f5bf95f3f09de5",
		ScriptClaimAndCompleteWorkV4:       "6c97041b15d435e0cce0d0159e45c1aa7e83201d",
		ScriptRescheduleWorkV2:             "36c2c91c963b13f54050b2fdef42d19328cdb278",
		ScriptClaimBucketScanLease:         "52b2185c39bd02606502659d1c8ecb24532ffda0",
		ScriptRenewBucketScanLease:         "b5109fb01d9248ebfcabfd77a43d8bcdd95e37ba",
		ScriptPublishResultEntry:           "1a332a0463eed34671606d1e8cad54c861eeb486",
		ScriptClaimScheduledWork:           "83c5325aef8f271e1487a5e5d953de94a28cd0ce",
		ScriptAdvanceScheduledWork:         "a5543ed9ae97b692c44483652db8272a01785aae",
		ScriptRenewScheduledWork:           "b29202244dab208bcc5679382286c09de6bdd48c",
		ScriptCompleteScheduledWork:        "0a1e55114c91fe2621e1b487b5b15744f5c141d8",
		ScriptClaimScheduledWorkDual:       "18aa80cac7a4af8fba26eb1134752af0ce73ed2e",
		ScriptAdvanceScheduledWorkDual:     "cf1beab97a0fd7aedb6ad21d777e205841582c95",
		ScriptCompleteScheduledWorkDual:    "357bfb400643df3f707b7f71d37c3fb3cc3f4fc7",
		ScriptBackfillScheduleDueIndex:     "341915a52dfa13e69164c25d12e4feed881c9753",
		ScriptReconcilePendingWork:         "4c0ad846c043e61edbbf5466e0f056e357fd5a97",
		ScriptVerifySourceOrderV1:          "deb6686acc98db2eb3944546e85a61066b030856",
		ScriptVerifySourceOrderV2:          "f38cb98183b1fe6ac06d5af50fd5e77ac8e5998c",
	}
	registry, err := NewScriptRegistry(newTestClientOnly(t))
	if err != nil {
		t.Fatal(err)
	}
	for name, sha := range want {
		got, ok := registry.ScriptSHA(name)
		if !ok {
			t.Fatalf("script %s not registered", name)
		}
		if got != sha {
			t.Fatalf("script %s sha=%s want %s (script body changed: freeze a new version instead)", name, got, sha)
		}
	}
	if len(builtinScripts()) != len(want) {
		t.Fatalf("registered %d scripts, goldens pin %d", len(builtinScripts()), len(want))
	}
}

// Slot goldens prove the auth-v1 atomic group shares one byte-identical hash
// tag, that PLAYER/FARM domains never mix slots, and that the exact key bytes
// stay frozen across refactors.
func TestClaimAndCompleteWorkV3HasNoTransientLeaseWrite(t *testing.T) {
	for _, source := range []string{luaClaimAndCompleteWorkV3Src, luaClaimAndCompleteWorkV4Src} {
		for _, forbidden := range []string{"'state', 'REWARD_DISPATCHING'", "redis.call('ZADD', KEYS[2]"} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("direct completion retained transient mutation %q", forbidden)
			}
		}
	}
	if !strings.Contains(luaClaimAndCompleteWorkV3Src, "'state', 'COMPLETED'") ||
		!strings.Contains(luaClaimAndCompleteWorkV4Src, "'pending_work_bytes'") ||
		!strings.Contains(luaClaimAndCompleteWorkV2Src, "'state', 'REWARD_DISPATCHING'") {
		t.Fatal("V4/V3/V2 direct-completion boundaries are not distinguishable")
	}
}

func TestAuthorityKeySlotGoldens(t *testing.T) {
	calculator := testCalculator(t)
	profile := testProfile(t)
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		t.Fatal(err)
	}
	addr := actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	bucket, err := profile.AuthorityBucketForActor(calculator, addr)
	if err != nil {
		t.Fatal(err)
	}
	if bucket.VirtualShard() != 34 {
		t.Fatalf("player 10001 shard=%d want 34 (routing profile drift)", bucket.VirtualShard())
	}
	tag, err := keys.HashTag(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if tag != "classic-farm:test:z1:auth:player:v1:s:34" {
		t.Fatalf("hash tag=%q", tag)
	}

	workID, err := workidentity.StableOpaqueID("testwork", "CARE_REWARD", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if workID != "sid1_ii_qRLn_cVnykYOohTWz9k5FZIl94PM3_m4lFLFQxpU" {
		t.Fatalf("work id golden=%q", workID)
	}

	cases := []struct {
		name string
		key  rediskey.RedisKey
		want string
	}{
		{"shard-authority", mustKey(t, func() (rediskey.RedisKey, error) { return keys.ShardAuthority(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:shard-authority"},
		{"actor", mustKey(t, func() (rediskey.RedisKey, error) { return keys.ActorRecord(bucket, 10001) }), "{classic-farm:test:z1:auth:player:v1:s:34}:actor:10001"},
		{"due", mustKey(t, func() (rediskey.RedisKey, error) { return keys.SourceDueIndex(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:due"},
		{"schedule-due", mustKey(t, func() (rediskey.RedisKey, error) { return keys.SourceScheduleDueIndex(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:schedule-due"},
		{"receipt-due", mustKey(t, func() (rediskey.RedisKey, error) { return keys.ReceiptDueIndex(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:receipt-due"},
		{"bucket-meta", mustKey(t, func() (rediskey.RedisKey, error) { return keys.BucketMeta(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:bucket-meta"},
		{"scan-lease", mustKey(t, func() (rediskey.RedisKey, error) { return keys.BucketScanLease(bucket) }), "{classic-farm:test:z1:auth:player:v1:s:34}:scan-lease"},
		{"order", mustKey(t, func() (rediskey.RedisKey, error) { return keys.SourceOrder(bucket, "CARE_REWARD", workID) }), "{classic-farm:test:z1:auth:player:v1:s:34}:order:CARE_REWARD:sid1_ii_qRLn_cVnykYOohTWz9k5FZIl94PM3_m4lFLFQxpU"},
		{"schedule", mustKey(t, func() (rediskey.RedisKey, error) { return keys.SourceSchedule(bucket, "CARE_REWARD", workID) }), "{classic-farm:test:z1:auth:player:v1:s:34}:schedule:CARE_REWARD:sid1_ii_qRLn_cVnykYOohTWz9k5FZIl94PM3_m4lFLFQxpU"},
	}
	var all []rediskey.RedisKey
	for _, tc := range cases {
		if string(tc.key) != tc.want {
			t.Fatalf("%s key=%q want %q", tc.name, tc.key, tc.want)
		}
		if rediskey.ClusterSlot(tc.key) != 11817 {
			t.Fatalf("%s slot=%d want 11817", tc.name, rediskey.ClusterSlot(tc.key))
		}
		all = append(all, tc.key)
	}
	if err := rediskey.AssertSameSlot(all...); err != nil {
		t.Fatalf("authority group not same slot: %v", err)
	}

	// FARM domain must derive a distinct hash tag and slot.
	farmBucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 10001})
	if err != nil {
		t.Fatal(err)
	}
	farmActor, err := keys.ActorRecord(farmBucket, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if string(farmActor) != "{classic-farm:test:z1:auth:farm:v1:s:52}:actor:10001" {
		t.Fatalf("farm actor key=%q", farmActor)
	}
	if rediskey.ClusterSlot(farmActor) != 16255 {
		t.Fatalf("farm actor slot=%d want 16255", rediskey.ClusterSlot(farmActor))
	}
	if err := rediskey.AssertSameSlot(all[1], farmActor); err == nil {
		t.Fatal("player and farm actor keys must not share a hash tag")
	}
}

func mustKey(t *testing.T, build func() (rediskey.RedisKey, error)) rediskey.RedisKey {
	t.Helper()
	key, err := build()
	if err != nil {
		t.Fatal(err)
	}
	return key
}
