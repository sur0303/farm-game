package routeplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/holderidentity"
	model "farmgame/internal/routeplan"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

type legacyFenceForTest struct {
	HolderID           string `json:"holder_id"`
	HolderBootID       string `json:"holder_boot_id"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	WriterEpoch        uint64 `json:"writer_epoch"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
}

type legacyProofForTest struct {
	ExpectedHolderBootID string `json:"expected_holder_boot_id"`
	HolderUnavailable    bool   `json:"holder_unavailable"`
	LeaseExpired         bool   `json:"lease_expired"`
	Evidence             string `json:"evidence"`
}

type legacyCanonicalForTest struct {
	SchemaVersion       string             `json:"schema_version"`
	PlanID              string             `json:"plan_id"`
	TransitionID        string             `json:"transition_id"`
	Domain              string             `json:"domain"`
	VirtualShard        uint32             `json:"virtual_shard"`
	SourceHolderID      string             `json:"source_holder_id"`
	SourceBootID        string             `json:"source_boot_id"`
	SourceEndpoint      string             `json:"source_endpoint"`
	TargetHolderID      string             `json:"target_holder_id"`
	TargetBootID        string             `json:"target_boot_id"`
	TargetEndpoint      string             `json:"target_endpoint"`
	ExpectedSourceFence legacyFenceForTest `json:"expected_source_fence"`
	RequestedTTLms      int64              `json:"requested_ttl_ms"`
	Operation           string             `json:"operation"`
	Phase               string             `json:"phase"`
	Attempt             uint64             `json:"attempt"`
	Revision            uint64             `json:"revision"`
	CreatedAtMs         int64              `json:"created_at_ms"`
	UpdatedAtMs         int64              `json:"updated_at_ms"`
	StartedAtMs         int64              `json:"started_at_ms"`
	CompletedAtMs       int64              `json:"completed_at_ms"`
	ExpiresAtMs         int64              `json:"expires_at_ms"`
	ErrorCode           string             `json:"error_code"`
	TakeoverProof       legacyProofForTest `json:"takeover_proof"`
}

func modelLegacyCanonicalForTest(plan model.Plan) legacyCanonicalForTest {
	return legacyCanonicalForTest{SchemaVersion: model.LegacyPlanSchemaVersion, PlanID: plan.PlanID,
		TransitionID: plan.TransitionID, Domain: string(plan.Domain), VirtualShard: plan.VirtualShard,
		SourceHolderID: plan.SourceHolderID, SourceBootID: plan.SourceBootID, SourceEndpoint: plan.SourceEndpoint,
		TargetHolderID: plan.TargetHolderID, TargetBootID: plan.TargetBootID, TargetEndpoint: plan.TargetEndpoint,
		ExpectedSourceFence: legacyFenceForTest{HolderID: plan.ExpectedSourceFence.ExpectedHolderID,
			HolderBootID:       plan.ExpectedSourceFence.ExpectedHolderBootID,
			AssignmentRevision: plan.ExpectedSourceFence.ExpectedAssignmentRevision,
			WriterEpoch:        plan.ExpectedSourceFence.ExpectedWriterEpoch, LeaseEpoch: plan.ExpectedSourceFence.ExpectedLeaseEpoch},
		RequestedTTLms: plan.RequestedTTLms, Operation: string(plan.Operation), Phase: plan.Phase.String(),
		Attempt: plan.Attempt, Revision: plan.Revision, CreatedAtMs: plan.CreatedAtMs, UpdatedAtMs: plan.UpdatedAtMs,
		StartedAtMs: plan.StartedAtMs, CompletedAtMs: plan.CompletedAtMs, ExpiresAtMs: plan.ExpiresAtMs,
		ErrorCode: plan.ErrorCode, TakeoverProof: legacyProofForTest{ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable: plan.TakeoverProof.HolderUnavailable, LeaseExpired: plan.TakeoverProof.LeaseExpired,
			Evidence: plan.TakeoverProof.Evidence}}
}

func redisPlanFixture(now int64, id string) model.Plan {
	return model.Plan{
		PlanID: id, TransitionID: "transition-" + id, Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: "holder-a", SourceBootID: "boot-a", SourceEndpoint: "holder-a.internal:443",
		TargetHolderID: "holder-b", TargetBootID: "boot-b", TargetEndpoint: "holder-b.internal:443",
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 3, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5},
		RequestedTTLms:      30_000, Operation: model.OperationPlannedDrain, Phase: model.PhasePending, Revision: 1,
		CreatedAtMs: now, UpdatedAtMs: now, ExpiresAtMs: now + time.Hour.Milliseconds(),
	}
}

func redisPlanStoreFixture(t *testing.T, maxItems, maxBytes int) (*miniredis.Miniredis, *redis.Client, *PlanStore, Options) {
	t.Helper()
	server := miniredis.RunT(t)
	// Miniredis stores absolute expiry as a duration from its effective clock.
	// Freeze that clock so PEXPIRETIME has Redis's stable absolute semantics
	// while the exact shared/hash expiry invariant is under test.
	server.SetTime(time.Now())
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions(namespace)
	options.MaxItems = maxItems
	options.MaxBytes = maxBytes
	options.MaxRestartRead = maxItems
	options.OperationTimeout = time.Second
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewPlanStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	return server, client, store, options
}

func TestRedisPlanStoreReadsOldRetentionScoreAfterProductionHorizonIncrease(t *testing.T) {
	_, client, writer, options := redisPlanStoreFixture(t, 8, 1<<20)
	ctx := context.Background()
	plan := redisPlanFixture(options.Now().UnixMilli(), "old-retention-plan")
	if _, err := writer.CompareAndSwap(ctx, 0, plan); err != nil {
		t.Fatal(err)
	}
	readerOptions := options
	readerOptions.AuditRetention = 38 * 24 * time.Hour
	reader, err := NewPlanStore(client, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err := reader.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan != plan {
		t.Fatalf("rollout load found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	if records, err := reader.ListIncomplete(ctx); err != nil || len(records) != 1 || records[0].Plan != plan {
		t.Fatalf("rollout list=%+v err=%v", records, err)
	}
	member, _ := reader.Keys().EncodedPlanID(plan.PlanID)
	if score := client.ZScore(ctx, reader.Keys().Index().String(), member).Val(); score != float64(plan.ExpiresAtMs+readerOptions.AuditRetention.Milliseconds()) {
		t.Fatalf("old retention score not migrated: %v", score)
	}
	key, _ := reader.Keys().Plan(plan.PlanID)
	if expiry := client.PExpireTime(ctx, key.String()).Val().Milliseconds(); expiry != plan.ExpiresAtMs+readerOptions.AuditRetention.Milliseconds() {
		t.Fatalf("old shared TTL not migrated: %d", expiry)
	}
}

func TestRedisPlanStoreOwnerRetentionOutlivesScalePlanRecoveryHorizon(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.Now()
	server.SetTime(now)
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions(namespace)
	options.OperationTimeout = time.Second
	options.MaxItems = 8
	options.MaxBytes = 1 << 20
	options.MaxRecordBytes = 1 << 20
	options.AuditRetention = 38 * 24 * time.Hour
	options.Now = func() time.Time { return now }
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewPlanStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	plan := redisPlanFixture(now.UnixMilli(), "scaleplan-owned-child")
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	plan.OwnerScalePlanID = "scale-plan-owner"
	plan.OwnerRetainUntilMs = plan.ExpiresAtMs + 37*24*time.Hour.Milliseconds()
	result, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("CAS status=%s err=%v", result.Status, err)
	}
	key, _ := store.Keys().Plan(plan.PlanID)
	if got, want := server.TTL(key.String()), 38*24*time.Hour+time.Hour; got < want-time.Second || got > want {
		t.Fatalf("physical TTL=%v want within [%v,%v]", got, want-time.Second, want)
	}
	server.FastForward(37*24*time.Hour + time.Hour)
	loaded, found, err := store.Load(context.Background(), plan.PlanID)
	if err != nil || !found || loaded.Plan != plan {
		t.Fatalf("retained found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	server.FastForward(24*time.Hour + time.Millisecond)
	if _, found, err := store.Load(context.Background(), plan.PlanID); err != nil || found {
		t.Fatalf("expired found=%t err=%v", found, err)
	}
}

func TestRedisPlanStoreCASReplayCorruptCapacityAndRestart(t *testing.T) {
	server, client, store, options := redisPlanStoreFixture(t, 1, 1<<20)
	now := time.Now().UnixMilli()
	plan := redisPlanFixture(now, "plan-redis-1")
	result, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("create status=%s err=%v", result.Status, err)
	}
	replay, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("replay status=%s err=%v", replay.Status, err)
	}
	advanced := plan
	advanced.Phase = model.PhaseDraining
	advanced.Revision = 2
	advanced.UpdatedAtMs++
	if _, err := store.CompareAndSwap(context.Background(), 1, advanced); err != nil {
		t.Fatal(err)
	}
	backward := advanced
	backward.Phase = model.PhasePending
	backward.Revision = 3
	backward.UpdatedAtMs++
	if _, err := store.CompareAndSwap(context.Background(), 2, backward); !errors.Is(err, model.ErrPlanPhase) && !errors.Is(err, model.ErrPlanConflict) {
		t.Fatalf("backward phase err=%v", err)
	}
	second := redisPlanFixture(now, "plan-redis-2")
	if _, err := store.CompareAndSwap(context.Background(), 0, second); !errors.Is(err, model.ErrPlanCapacity) {
		t.Fatalf("capacity err=%v", err)
	}
	restarted, err := NewPlanStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	incomplete, err := restarted.ListIncomplete(context.Background())
	if err != nil || len(incomplete) != 1 || incomplete[0].Plan.PlanID != plan.PlanID {
		t.Fatalf("restart incomplete=%+v err=%v", incomplete, err)
	}
	key, err := store.Keys().Plan(plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), key.String(), planFieldChecksum, strings.Repeat("0", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := restarted.Load(context.Background(), plan.PlanID); !found || !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("corrupt load found=%t err=%v", found, err)
	}
	if strings.Contains(compareAndSwapPlanLua, "redis.call('KEYS'") || strings.Contains(compareAndSwapPlanLua, "redis.call('SCAN'") {
		t.Fatal("route-plan CAS script must not discover keys")
	}
	server.FastForward(26 * time.Hour)
	if _, found, err := restarted.Load(context.Background(), plan.PlanID); err != nil || found {
		t.Fatalf("ttl load found=%t err=%v", found, err)
	}
}

func TestRedisPlanStoreUsesOneHashSlotForEveryCASKey(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	if err := store.Keys().AssertSameSlot(store.Keys().Meta(), store.Keys().Index(), store.Keys().Restart(), store.Keys().Accounting()); err != nil {
		t.Fatal(err)
	}
}

func TestRedisPlanStoreDirectLoadUsesBoundedAtomicScript(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	plan := redisPlanFixture(time.Now().UnixMilli(), "plan-atomic-load")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	hook := &routePlanCommandHook{}
	client.AddHook(hook)
	if _, found, err := store.Load(context.Background(), plan.PlanID); err != nil || !found {
		t.Fatalf("load found=%v err=%v", found, err)
	}
	if commands := hook.snapshot(); !reflect.DeepEqual(commands, []string{"evalsha"}) {
		t.Fatalf("direct Load commands=%v", commands)
	}
	key, _ := store.Keys().Plan(plan.PlanID)
	if err := client.HSet(context.Background(), key.String(), planFieldJSON, strings.Repeat("x", model.MaxCanonicalPlanBytes+1)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(context.Background(), plan.PlanID); !found || !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("oversized load found=%v err=%v", found, err)
	}
	if err := client.Del(context.Background(), key.String()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(context.Background(), plan.PlanID); !found || !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("dangling index/accounting load found=%v err=%v", found, err)
	}
}

func TestRedisPlanStoreDurableStopAtomicallyRemovesRestartEntry(t *testing.T) {
	_, client, store, options := redisPlanStoreFixture(t, 4, 1<<20)
	ctx := context.Background()
	plan := redisPlanFixture(options.Now().UnixMilli(), "durable-stop-plan")
	plan.OwnerScalePlanID = "scale-owner"
	plan.OwnerRetainUntilMs = plan.ExpiresAtMs + options.AuditRetention.Milliseconds()
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	if result, err := store.CompareAndSwap(ctx, 0, plan); err != nil || result.Status != model.CASCommitted {
		t.Fatalf("create status=%s err=%v", result.Status, err)
	}
	issuer := controlfence.NewIssuer()
	command := model.DurableStopCommand{
		OwnerScalePlanID: "scale-owner", PlanID: plan.PlanID, ExpectedRevision: 1,
		ExpectedPhase: model.PhasePending, NowMs: plan.UpdatedAtMs + 1, Fence: issuer.Issue("stop-op", 1),
	}
	result, err := store.StopOwnedPlanForScaleTermination(ctx, command)
	if err != nil || result.Status != model.DurableStopStopped || result.Record.Plan.Phase != model.PhasePaused ||
		result.Record.Plan.ErrorCode != model.DurableStopErrorCode || result.Record.Plan.Revision != 2 {
		t.Fatalf("stop result=%+v err=%v", result, err)
	}
	if incomplete, err := store.ListIncomplete(ctx); err != nil || len(incomplete) != 0 {
		t.Fatalf("restart list=%+v err=%v", incomplete, err)
	}
	restart := client.LRange(ctx, store.Keys().Restart().String(), 0, -1).Val()
	if !reflect.DeepEqual(restart, []string{"route-plan-restart-v1"}) {
		t.Fatalf("restart key=%v", restart)
	}
	member, _ := store.Keys().EncodedPlanID(plan.PlanID)
	if got := client.HGet(ctx, store.Keys().Accounting().String(), member).Val(); got != strconv.Itoa(modelEnvelopeSize(result.Record.Envelope)) {
		t.Fatalf("accounting bytes=%q", got)
	}
	auditKey := store.keys.key("stop-audit:" + member)
	if audit, err := client.HGetAll(ctx, auditKey.String()).Result(); err != nil || !reflect.DeepEqual(audit, map[string]string{
		stopAuditFieldSchema:    stopAuditSchema,
		stopAuditFieldOwner:     "scale-owner",
		stopAuditFieldOperation: "stop-op",
		stopAuditFieldEpoch:     "1",
	}) {
		t.Fatalf("stop audit=%v err=%v", audit, err)
	}
	replay, err := store.StopOwnedPlanForScaleTermination(ctx, command)
	if err != nil || replay.Status != model.DurableStopAlreadyStopped || !replay.DurablyStopped("scale-owner") {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	wrongOwner := command
	wrongOwner.OwnerScalePlanID = "other-owner"
	notOwned, err := store.StopOwnedPlanForScaleTermination(ctx, wrongOwner)
	if err != nil || notOwned.Status != model.DurableStopNotOwned {
		t.Fatalf("wrong owner=%+v err=%v", notOwned, err)
	}
}

func TestRedisPlanStoreDurableStopCorruptionStaysUnsafeWithoutInference(t *testing.T) {
	_, client, store, options := redisPlanStoreFixture(t, 4, 1<<20)
	ctx := context.Background()
	plan := redisPlanFixture(options.Now().UnixMilli(), "durable-stop-corrupt")
	plan.OwnerScalePlanID = "scale-owner"
	plan.OwnerRetainUntilMs = plan.ExpiresAtMs + options.AuditRetention.Milliseconds()
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	if result, err := store.CompareAndSwap(ctx, 0, plan); err != nil || result.Status != model.CASCommitted {
		t.Fatalf("create status=%s err=%v", result.Status, err)
	}
	member, _ := store.Keys().EncodedPlanID(plan.PlanID)
	if err := client.LRem(ctx, store.Keys().Restart().String(), 0, member).Err(); err != nil {
		t.Fatal(err)
	}
	issuer := controlfence.NewIssuer()
	command := model.DurableStopCommand{
		OwnerScalePlanID: "scale-owner", PlanID: plan.PlanID, ExpectedRevision: 1,
		ExpectedPhase: model.PhasePending, NowMs: plan.UpdatedAtMs + 1, Fence: issuer.Issue("stop-corrupt", 1),
	}
	result, err := store.StopOwnedPlanForScaleTermination(ctx, command)
	if result.Status != model.DurableStopUnsafe || !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("corrupt stop result=%+v err=%v", result, err)
	}
	loaded, found, err := store.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan.Phase != model.PhasePending {
		t.Fatalf("corrupt stop changed plan found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	if audit := client.Exists(ctx, store.keys.key("stop-audit:"+member).String()).Val(); audit != 0 {
		t.Fatalf("corrupt stop wrote audit key exists=%d", audit)
	}
}

func TestRedisPlanStoreReadsCanonicalV1AndCASUpgradesToV2(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	ctx := context.Background()
	plan := redisPlanFixture(time.Now().UnixMilli(), "canonical-v1-upgrade")
	if _, err := store.CompareAndSwap(ctx, 0, plan); err != nil {
		t.Fatal(err)
	}
	legacyJSON, err := json.Marshal(modelLegacyCanonicalForTest(plan))
	if err != nil {
		t.Fatal(err)
	}
	legacyChecksum := sha256.Sum256(legacyJSON)
	checksum := hex.EncodeToString(legacyChecksum[:])
	key, _ := store.Keys().Plan(plan.PlanID)
	member, _ := store.Keys().EncodedPlanID(plan.PlanID)
	oldBytes := len(legacyJSON) + len(checksum) + 64
	if err := client.HSet(ctx, key.String(), planFieldJSON, string(legacyJSON), planFieldChecksum, checksum).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, store.Keys().Accounting().String(), member, strconv.Itoa(oldBytes)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, store.Keys().Meta().String(), "bytes", strconv.Itoa(oldBytes)).Err(); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan != plan {
		t.Fatalf("legacy load found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	advanced := plan
	advanced.Revision = 2
	advanced.Phase = model.PhaseDraining
	advanced.UpdatedAtMs++
	if _, err := store.CompareAndSwap(ctx, 1, advanced); err != nil {
		t.Fatalf("legacy CAS upgrade: %v", err)
	}
	canonical := client.HGet(ctx, key.String(), planFieldJSON).Val()
	if !strings.HasPrefix(canonical, `{"schema_version":"`+model.PlanSchemaVersion+`",`) {
		t.Fatalf("upgraded canonical=%s", canonical)
	}
}

func TestRedisPlanStoreReadsLegacyEightFieldPlanAndCASUpgradesIt(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	legacy := redisPlanFixture(now, "plan-legacy-v1")
	current := redisPlanFixture(now, "plan-current-v2")
	if _, err := store.CompareAndSwap(ctx, 0, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(ctx, 0, current); err != nil {
		t.Fatal(err)
	}
	legacyKey, err := store.Keys().Plan(legacy.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	// This is the exact physical shape written before the audit tuple was
	// split out. Reads remain read-only; the next successful CAS is the sole
	// migration point to the versioned 11-field physical record.
	if err := client.HDel(ctx, legacyKey.String(), planFieldAttempt, planFieldStarted, planFieldComplete).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, legacyKey.String(), planFieldSchema, planPhysicalV1).Err(); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.Load(ctx, legacy.PlanID)
	if err != nil || !found || !reflect.DeepEqual(loaded.Plan, legacy) {
		t.Fatalf("legacy Load found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	if fields := client.HLen(ctx, legacyKey.String()).Val(); fields != 8 {
		t.Fatalf("read mutated legacy hash fields=%d", fields)
	}
	incomplete, err := store.ListIncomplete(ctx)
	if err != nil || len(incomplete) != 2 {
		t.Fatalf("mixed v1/v2 ListIncomplete=%+v err=%v", incomplete, err)
	}
	advanced := legacy
	advanced.Revision = 2
	advanced.Phase = model.PhaseDraining
	advanced.UpdatedAtMs++
	result, err := store.CompareAndSwap(ctx, 1, advanced)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("legacy CAS upgrade status=%s err=%v", result.Status, err)
	}
	if fields := client.HLen(ctx, legacyKey.String()).Val(); fields != 11 {
		t.Fatalf("upgraded hash fields=%d", fields)
	}
	if schema := client.HGet(ctx, legacyKey.String(), planFieldSchema).Val(); schema != planPhysicalV2 {
		t.Fatalf("upgraded schema=%q", schema)
	}
	loaded, found, err = store.Load(ctx, legacy.PlanID)
	if err != nil || !found || !reflect.DeepEqual(loaded.Plan, advanced) {
		t.Fatalf("upgraded Load found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
}

func TestRedisPlanStoreRejectsNonCanonicalMetaIntegersWithoutWrite(t *testing.T) {
	for _, value := range []string{"01", "1.0", "1e0"} {
		t.Run(value, func(t *testing.T) {
			_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
			plan := redisPlanFixture(time.Now().UnixMilli(), "plan-meta-integer")
			if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(context.Background(), store.Keys().Meta().String(), "count", value).Err(); err != nil {
				t.Fatal(err)
			}
			before := redisPhysicalState(t, client, store, plan.PlanID)
			if _, err := store.CompareAndSwap(context.Background(), 0, plan); !errors.Is(err, model.ErrPlanCorrupt) {
				t.Fatalf("meta=%q err=%v", value, err)
			}
			after := redisPhysicalState(t, client, store, plan.PlanID)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("noncanonical meta mutated state\nbefore=%v\nafter=%v", before, after)
			}
		})
	}
}

func TestRedisPlanLuaEnforcesInitialAndAttemptAuditInvariants(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	ctx := context.Background()
	initial := redisPlanFixture(time.Now().UnixMilli(), "plan-lua-audit")
	invalidInitial := initial
	invalidInitial.Phase = model.PhaseDraining
	envelope, err := model.EncodePlan(invalidInitial)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := store.Keys().Plan(invalidInitial.PlanID)
	member, _ := store.Keys().EncodedPlanID(invalidInitial.PlanID)
	keys, args, err := store.reconcileInvocation(reconcileBatch{}, key.String(), "CAS", invalidInitial.PlanID, member, 0, envelope, invalidInitial.Phase, true, invalidInitial)
	if err != nil {
		t.Fatal(err)
	}
	if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
		t.Fatalf("invalid initial reply=%v err=%v", reply, err)
	}
	if _, err := store.CompareAndSwap(ctx, 0, initial); err != nil {
		t.Fatal(err)
	}
	jump := initial
	jump.Revision = 2
	jump.Phase = model.PhaseDraining
	jump.Attempt = 2
	jump.StartedAtMs = jump.UpdatedAtMs
	envelope, err = model.EncodePlan(jump)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.prepareReconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys, args, err = store.reconcileInvocation(batch, key.String(), "CAS", jump.PlanID, member, 1, envelope, jump.Phase, true, jump)
	if err != nil {
		t.Fatal(err)
	}
	if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
		t.Fatalf("attempt jump reply=%v err=%v", reply, err)
	}

	validAdvance := initial
	validAdvance.Revision = 2
	validAdvance.Phase = model.PhaseDraining
	validAdvance.Attempt = 1
	validAdvance.UpdatedAtMs++
	validAdvance.StartedAtMs = validAdvance.UpdatedAtMs
	for name, mutate := range map[string]func([]any){
		"updated regression": func(values []any) {
			values[13] = strconv.FormatInt(initial.UpdatedAtMs-1, 10)
		},
		"started after updated": func(values []any) {
			values[18] = strconv.FormatInt(validAdvance.UpdatedAtMs+1, 10)
		},
	} {
		t.Run(name, func(t *testing.T) {
			envelope, err := model.EncodePlan(validAdvance)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.prepareReconcile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			keys, args, err := store.reconcileInvocation(batch, key.String(), "CAS", validAdvance.PlanID, member, 1, envelope, validAdvance.Phase, true, validAdvance)
			if err != nil {
				t.Fatal(err)
			}
			mutate(args)
			before := redisPhysicalState(t, client, store, initial.PlanID)
			if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
				t.Fatalf("reply=%v err=%v", reply, err)
			}
			if after := redisPhysicalState(t, client, store, initial.PlanID); !reflect.DeepEqual(before, after) {
				t.Fatalf("invalid audit tuple mutated state\nbefore=%v\nafter=%v", before, after)
			}
		})
	}

	terminal := initial
	terminal.Revision = 2
	terminal.Phase = model.PhasePaused
	terminal.ErrorCode = "TEST_FAILURE"
	terminal.UpdatedAtMs++
	terminal.CompletedAtMs = terminal.UpdatedAtMs
	envelope, err = model.EncodePlan(terminal)
	if err != nil {
		t.Fatal(err)
	}
	batch, err = store.prepareReconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys, args, err = store.reconcileInvocation(batch, key.String(), "CAS", terminal.PlanID, member, 1, envelope, terminal.Phase, false, terminal)
	if err != nil {
		t.Fatal(err)
	}
	args[19] = strconv.FormatInt(terminal.CompletedAtMs+1, 10)
	before := redisPhysicalState(t, client, store, initial.PlanID)
	if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
		t.Fatalf("terminal completion reply=%v err=%v", reply, err)
	}
	if after := redisPhysicalState(t, client, store, initial.PlanID); !reflect.DeepEqual(before, after) {
		t.Fatalf("invalid terminal timestamp mutated state\nbefore=%v\nafter=%v", before, after)
	}

	if result, err := store.CompareAndSwap(ctx, 1, validAdvance); err != nil || result.Status != model.CASCommitted {
		t.Fatalf("valid advance status=%s err=%v", result.Status, err)
	}
	frozenRewrite := validAdvance
	frozenRewrite.Revision = 3
	frozenRewrite.UpdatedAtMs++
	frozenRewrite.TargetHolderID = "holder-c"
	envelope, err = model.EncodePlan(frozenRewrite)
	if err != nil {
		t.Fatal(err)
	}
	batch, err = store.prepareReconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys, args, err = store.reconcileInvocation(batch, key.String(), "CAS", frozenRewrite.PlanID, member, 2, envelope, frozenRewrite.Phase, true, frozenRewrite)
	if err != nil {
		t.Fatal(err)
	}
	before = redisPhysicalState(t, client, store, initial.PlanID)
	if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
		t.Fatalf("frozen rewrite reply=%v err=%v", reply, err)
	}
	if after := redisPhysicalState(t, client, store, initial.PlanID); !reflect.DeepEqual(before, after) {
		t.Fatalf("frozen rewrite mutated state\nbefore=%v\nafter=%v", before, after)
	}

	committedTerminal := validAdvance
	committedTerminal.Revision = 3
	committedTerminal.Phase = model.PhasePaused
	committedTerminal.ErrorCode = "TEST_FAILURE"
	committedTerminal.UpdatedAtMs++
	committedTerminal.CompletedAtMs = committedTerminal.UpdatedAtMs
	if result, err := store.CompareAndSwap(ctx, 2, committedTerminal); err != nil || result.Status != model.CASCommitted {
		t.Fatalf("terminal commit status=%s err=%v", result.Status, err)
	}
	terminalRewrite := committedTerminal
	terminalRewrite.Revision = 4
	terminalRewrite.ErrorCode = "REWRITTEN_FAILURE"
	terminalRewrite.UpdatedAtMs++
	terminalRewrite.CompletedAtMs = terminalRewrite.UpdatedAtMs
	envelope, err = model.EncodePlan(terminalRewrite)
	if err != nil {
		t.Fatal(err)
	}
	batch, err = store.prepareReconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys, args, err = store.reconcileInvocation(batch, key.String(), "CAS", terminalRewrite.PlanID, member, 3, envelope, terminalRewrite.Phase, false, terminalRewrite)
	if err != nil {
		t.Fatal(err)
	}
	before = redisPhysicalState(t, client, store, initial.PlanID)
	if reply, err := store.registry.run(ctx, keys, args...); err != nil || len(reply) != 1 || reply[0] != "REVISION_CONFLICT" {
		t.Fatalf("terminal rewrite reply=%v err=%v", reply, err)
	}
	if after := redisPhysicalState(t, client, store, initial.PlanID); !reflect.DeepEqual(before, after) {
		t.Fatalf("terminal rewrite mutated state\nbefore=%v\nafter=%v", before, after)
	}
}

type routePlanCommandHook struct {
	mu       sync.Mutex
	commands []string
	noRetry  []bool
}

func (*routePlanCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *routePlanCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		h.mu.Lock()
		h.commands = append(h.commands, strings.ToLower(command.Name()))
		h.noRetry = append(h.noRetry, command.NoRetry())
		h.mu.Unlock()
		return next(ctx, command)
	}
}
func (h *routePlanCommandHook) retrySnapshot() []bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]bool(nil), h.noRetry...)
}

type loseCommittedPlanCASResponseHook struct {
	mu       sync.Mutex
	armed    bool
	attempts int
}

func (*loseCommittedPlanCASResponseHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *loseCommittedPlanCASResponseHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		err := next(ctx, command)
		if err != nil || strings.ToLower(command.Name()) != "evalsha" || !command.NoRetry() {
			return err
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.attempts++
		if h.armed {
			h.armed = false
			return errors.New("lost route-plan CAS response after commit")
		}
		return nil
	}
}
func (*loseCommittedPlanCASResponseHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (*routePlanCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *routePlanCommandHook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...)
}

func TestRedisPlanStoreBoundsIndexBeforeReturningMembers(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	huge := strings.Repeat("a", 1<<20)
	if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{Score: 1, Member: huge}).Err(); err != nil {
		t.Fatal(err)
	}
	hook := &routePlanCommandHook{}
	client.AddHook(hook)
	if _, err := store.ListIncomplete(context.Background()); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("huge index member err=%v", err)
	}
	if commands := hook.snapshot(); !reflect.DeepEqual(commands, []string{"evalsha"}) {
		t.Fatalf("bounded index commands=%v", commands)
	}
	countAt := strings.Index(readPlanIndexLua, "redis.call('ZCARD'")
	memoryAt := strings.Index(readPlanIndexLua, "redis.pcall('MEMORY', 'USAGE', KEYS[1], 'SAMPLES', '0')")
	rangeAt := strings.Index(readPlanIndexLua, "redis.call('ZRANGE'")
	if countAt < 0 || memoryAt < 0 || rangeAt < 0 || countAt >= memoryAt || memoryAt >= rangeAt {
		t.Fatalf("index bound order count=%d memory=%d zrange=%d", countAt, memoryAt, rangeAt)
	}
	if !strings.Contains(readPlanIndexLua, "max_items > math.floor((max_safe_integer - physical_base) / physical_per_item)") {
		t.Fatal("index physical ceiling arithmetic is not checked")
	}
}

func TestRedisPlanStoreIndexMemorySamplingFailsClosedAboveFallbackCardinality(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	for index := 0; index < 6; index++ {
		member := fmt.Sprintf("%02x", index+1)
		if index == 5 {
			member = strings.Repeat("a", 1<<20)
		}
		if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{Score: float64(index + 1), Member: member}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ListIncomplete(context.Background()); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("unsupported exact sampling above fallback cardinality err=%v", err)
	}
}

func TestRedisPlanCASIsNoRetryAndCommitLossRemainsReconcileable(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	hook := &loseCommittedPlanCASResponseHook{armed: true}
	client.AddHook(hook)
	plan := redisPlanFixture(time.Now().UnixMilli(), "plan-commit-response-lost")
	if result, err := store.CompareAndSwap(context.Background(), 0, plan); err == nil || result.Status != model.CASUnknown {
		t.Fatalf("lost response result=%+v err=%v", result, err)
	}
	loaded, found, err := store.Load(context.Background(), plan.PlanID)
	if err != nil || !found || !reflect.DeepEqual(loaded.Plan, plan) {
		t.Fatalf("reconcile Load found=%t plan=%+v err=%v", found, loaded.Plan, err)
	}
	replay, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	hook.mu.Lock()
	attempts := hook.attempts
	hook.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("CAS attempts=%d want 2 (one commit plus one explicit replay)", attempts)
	}
	if got := client.HGet(context.Background(), store.Keys().Meta().String(), "count").Val(); got != "1" {
		t.Fatalf("commit-loss meta count=%q", got)
	}
}

func TestRedisPlanExpiredAccountingRequiresExactCanonicalRawText(t *testing.T) {
	for _, corrupt := range []string{"01", "1e3"} {
		t.Run(corrupt, func(t *testing.T) {
			_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
			store.options.PlanTTL = 72 * time.Hour
			clock := time.Now()
			store.options.Now = func() time.Time { return clock }
			plan := redisPlanFixture(clock.UnixMilli(), "plan-expired-accounting-"+corrupt)
			if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
				t.Fatal(err)
			}
			later := redisPlanFixture(clock.UnixMilli()+1, "plan-accounting-retention-tail-"+corrupt)
			later.ExpiresAtMs = clock.Add(48 * time.Hour).UnixMilli()
			if _, err := store.CompareAndSwap(context.Background(), 0, later); err != nil {
				t.Fatal(err)
			}
			clock = clock.Add(26 * time.Hour)
			batch, err := store.prepareReconcile(context.Background())
			if err != nil || len(batch.expired) != 1 {
				t.Fatalf("expired batch=%+v err=%v", batch, err)
			}
			keys, args, err := store.reconcileInvocation(batch, store.Keys().Meta().String(), "READ", "", "", 0, model.PlanEnvelope{}, model.PhaseUnspecified, false, model.Plan{})
			if err != nil {
				t.Fatal(err)
			}
			member := mustEncodedMember(t, store, plan.PlanID)
			if err := client.HSet(context.Background(), store.Keys().Accounting().String(), member, corrupt).Err(); err != nil {
				t.Fatal(err)
			}
			before := redisPhysicalState(t, client, store, plan.PlanID)
			reply, err := store.registry.run(context.Background(), keys, args...)
			if err != nil || len(reply) < 2 || reply[0] != "MALFORMED" || reply[1] != "EXPIRED_ACCOUNTING" {
				t.Fatalf("raw accounting=%q reply=%v err=%v", corrupt, reply, err)
			}
			if after := redisPhysicalState(t, client, store, plan.PlanID); !reflect.DeepEqual(before, after) {
				t.Fatalf("raw accounting mutation\nbefore=%v\nafter=%v", before, after)
			}
		})
	}
}

func TestRedisPlanStoreReconcilesMultiplePlanExpiryAndSharedTTL(t *testing.T) {
	_, client, store, options := redisPlanStoreFixture(t, 4, 1<<20)
	now := time.Now().UnixMilli()
	first := redisPlanFixture(now, "plan-a")
	second := redisPlanFixture(now+10, "plan-b")
	second.ExpiresAtMs = now + 2*time.Hour.Milliseconds()
	if _, err := store.CompareAndSwap(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	firstMember, _ := store.Keys().EncodedPlanID(first.PlanID)
	secondMember, _ := store.Keys().EncodedPlanID(second.PlanID)
	firstScore := client.ZScore(context.Background(), store.Keys().Index().String(), firstMember).Val()
	secondScore := client.ZScore(context.Background(), store.Keys().Index().String(), secondMember).Val()
	if firstScore != float64(first.ExpiresAtMs+options.AuditRetention.Milliseconds()) || secondScore != float64(second.ExpiresAtMs+options.AuditRetention.Milliseconds()) {
		t.Fatalf("expiry scores first=%v second=%v", firstScore, secondScore)
	}
	sharedExpiry := client.PExpireTime(context.Background(), store.Keys().Meta().String()).Val()
	for _, key := range []string{store.Keys().Index().String(), store.Keys().Restart().String(), store.Keys().Accounting().String()} {
		if expiry := client.PExpireTime(context.Background(), key).Val(); expiry != sharedExpiry {
			t.Fatalf("shared TTL mismatch key=%s expiry=%s shared=%s", key, expiry, sharedExpiry)
		}
	}
	for _, id := range []string{first.PlanID, second.PlanID} {
		key, _ := store.Keys().Plan(id)
		if expiry := client.PExpireTime(context.Background(), key.String()).Val(); expiry != sharedExpiry {
			t.Fatalf("plan TTL mismatch id=%s expiry=%s shared=%s", id, expiry, sharedExpiry)
		}
	}
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 2 || records[0].Plan.PlanID != first.PlanID || records[1].Plan.PlanID != second.PlanID {
		t.Fatalf("records=%+v err=%v", records, err)
	}
}

func TestRedisPlanStoreCorruptionIsZeroWrite(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(context.Context, *redis.Client, *PlanStore, model.Plan)
	}{
		{name: "same cardinality member replacement", corrupt: func(ctx context.Context, client *redis.Client, store *PlanStore, plan model.Plan) {
			member, _ := store.Keys().EncodedPlanID(plan.PlanID)
			score := client.ZScore(ctx, store.Keys().Index().String(), member).Val()
			client.ZRem(ctx, store.Keys().Index().String(), member)
			client.ZAdd(ctx, store.Keys().Index().String(), redis.Z{Score: score, Member: "706c616e2d7265706c61636564"})
		}},
		{name: "duplicate restart", corrupt: func(ctx context.Context, client *redis.Client, store *PlanStore, plan model.Plan) {
			member, _ := store.Keys().EncodedPlanID(plan.PlanID)
			client.RPush(ctx, store.Keys().Restart().String(), member)
		}},
		{name: "accounting bytes drift", corrupt: func(ctx context.Context, client *redis.Client, store *PlanStore, plan model.Plan) {
			member, _ := store.Keys().EncodedPlanID(plan.PlanID)
			client.HSet(ctx, store.Keys().Accounting().String(), member, "999")
		}},
		{name: "missing unexpired hash", corrupt: func(ctx context.Context, client *redis.Client, store *PlanStore, plan model.Plan) {
			key, _ := store.Keys().Plan(plan.PlanID)
			client.Del(ctx, key.String())
		}},
		{name: "one millisecond shared TTL drift", corrupt: func(ctx context.Context, client *redis.Client, store *PlanStore, _ model.Plan) {
			key := store.Keys().Accounting().String()
			expires := client.PExpireTime(ctx, key).Val().Milliseconds()
			client.PExpireAt(ctx, key, time.UnixMilli(expires+1))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
			plan := redisPlanFixture(time.Now().UnixMilli(), "plan-corrupt")
			if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
				t.Fatal(err)
			}
			test.corrupt(context.Background(), client, store, plan)
			before := redisPhysicalState(t, client, store, plan.PlanID)
			if _, err := store.CompareAndSwap(context.Background(), 0, plan); !errors.Is(err, model.ErrPlanCorrupt) {
				t.Fatalf("corrupt CAS err=%v", err)
			}
			after := redisPhysicalState(t, client, store, plan.PlanID)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("corrupt CAS wrote state\nbefore=%v\nafter=%v", before, after)
			}
		})
	}
}

func TestRedisPlanStoreMissingLogicalExpiredHashIsCorrupt(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	now := time.Now()
	store.options.Now = func() time.Time { return now }
	plan := redisPlanFixture(now.UnixMilli(), "plan-expired-missing")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	key, _ := store.Keys().Plan(plan.PlanID)
	if err := client.Del(context.Background(), key.String()).Err(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	before := redisPhysicalState(t, client, store, plan.PlanID)
	if _, err := store.ListIncomplete(context.Background()); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("missing indexed hash err=%v", err)
	}
	after := redisPhysicalState(t, client, store, plan.PlanID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("corrupt read mutated evidence\nbefore=%v\nafter=%v", before, after)
	}
}

func TestRedisPlanStoreRemovesLogicalExpiryFromRestartButReturnsAuditIncomplete(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	clock := time.Now()
	store.options.Now = func() time.Time { return clock }
	plan := redisPlanFixture(clock.UnixMilli(), "plan-logical-expired")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Hour)
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 1 || records[0].Plan.PlanID != plan.PlanID {
		t.Fatalf("audit incomplete=%v err=%v", records, err)
	}
	restart := client.LRange(context.Background(), store.Keys().Restart().String(), 0, -1).Val()
	if !reflect.DeepEqual(restart, []string{"route-plan-restart-v1"}) {
		t.Fatalf("logical expiry remains in restart=%v", restart)
	}
	if _, found, err := store.Load(context.Background(), plan.PlanID); err != nil || !found {
		t.Fatalf("audit record found=%v err=%v", found, err)
	}
}

func TestRedisPlanStoreSerializesConcurrentFullReconciliations(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	plans := []model.Plan{redisPlanFixture(now, "plan-concurrent-a"), redisPlanFixture(now, "plan-concurrent-b")}
	start := make(chan struct{})
	errorsByPlan := make([]error, len(plans))
	var wait sync.WaitGroup
	for index := range plans {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, errorsByPlan[index] = store.CompareAndSwap(context.Background(), 0, plans[index])
		}(index)
	}
	close(start)
	wait.Wait()
	for index, err := range errorsByPlan {
		if err != nil {
			t.Fatalf("plan=%s err=%v", plans[index].PlanID, err)
		}
	}
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != len(plans) {
		t.Fatalf("records=%v err=%v", records, err)
	}
}

type advanceExistingBeforeCreateCASHook struct {
	mu        sync.Mutex
	armed     bool
	store     *PlanStore
	current   model.Plan
	updateErr error
}

func (*advanceExistingBeforeCreateCASHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *advanceExistingBeforeCreateCASHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		args := command.Args()
		if strings.EqualFold(command.Name(), "evalsha") && len(args) > 1 {
			sha, _ := args[1].(string)
			h.mu.Lock()
			armed := h.armed && sha == h.store.registry.defs[scriptCompareAndSwapPlan].sha
			if armed {
				h.armed = false
			}
			current := h.current
			h.mu.Unlock()
			if armed {
				advanced := current.Clone()
				advanced.Revision++
				advanced.UpdatedAtMs++
				advanced.Attempt++
				_, updateErr := h.store.CompareAndSwap(ctx, current.Revision, advanced)
				h.mu.Lock()
				h.current = advanced
				h.updateErr = updateErr
				h.mu.Unlock()
			}
		}
		return next(ctx, command)
	}
}
func (*advanceExistingBeforeCreateCASHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRedisPlanStoreCreateRetriesConcurrentExistingPlanAdvance(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	existing := redisPlanFixture(now, "plan-existing-during-create")
	if _, err := store.CompareAndSwap(context.Background(), 0, existing); err != nil {
		t.Fatal(err)
	}
	hook := &advanceExistingBeforeCreateCASHook{armed: true, store: store, current: existing}
	client.AddHook(hook)
	candidate := redisPlanFixture(now, "plan-create-after-live-advance")
	// Force the shared audit tail to grow so this case continues to exercise
	// the bounded fallback. Same-tail foreground members use the O(1) create
	// script and have no stale full-index read to race with an existing update.
	candidate.ExpiresAtMs++
	result, err := store.CompareAndSwap(context.Background(), 0, candidate)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("create status=%s err=%v", result.Status, err)
	}
	hook.mu.Lock()
	updateErr := hook.updateErr
	hook.mu.Unlock()
	if updateErr != nil {
		t.Fatalf("concurrent existing update err=%v", updateErr)
	}
	advanced, found, err := store.Load(context.Background(), existing.PlanID)
	if err != nil || !found || advanced.Plan.Revision != existing.Revision+1 {
		t.Fatalf("advanced=%+v found=%t err=%v", advanced.Plan, found, err)
	}
}

func TestRedisPlanStoreListRetriesConcurrentExistingPlanAdvance(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	existing := redisPlanFixture(now, "plan-existing-during-list")
	if _, err := store.CompareAndSwap(context.Background(), 0, existing); err != nil {
		t.Fatal(err)
	}
	hook := &advanceExistingBeforeCreateCASHook{armed: true, store: store, current: existing}
	client.AddHook(hook)
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 1 || records[0].Plan.PlanID != existing.PlanID || records[0].Plan.Revision != existing.Revision+1 {
		t.Fatalf("records=%v err=%v", records, err)
	}
	hook.mu.Lock()
	updateErr := hook.updateErr
	hook.mu.Unlock()
	if updateErr != nil {
		t.Fatalf("concurrent existing update err=%v", updateErr)
	}
}

func TestRedisPlanStoreExistingCASDoesNotWaitForFullReconcileGate(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	plan := redisPlanFixture(now, "plan-existing-cas-hot-path")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	if err := store.acquireReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.releaseReconcile()

	advanced := plan.Clone()
	advanced.Revision++
	advanced.UpdatedAtMs++
	advanced.Attempt++
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result, err := store.CompareAndSwap(ctx, plan.Revision, advanced)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("existing CAS waited for full reconcile gate: status=%s err=%v", result.Status, err)
	}
}

func TestRedisPlanStoreSameTailCreateDoesNotWaitForFullReconcileGate(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	first := redisPlanFixture(now, "plan-create-hot-first")
	if _, err := store.CompareAndSwap(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if err := store.acquireReconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.releaseReconcile()

	sameTail := redisPlanFixture(now, "plan-create-hot-same-tail")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result, err := store.CompareAndSwap(ctx, 0, sameTail)
	if err != nil || result.Status != model.CASCommitted {
		t.Fatalf("same-tail create waited for reconcile gate: result=%+v err=%v", result, err)
	}

	laterTail := redisPlanFixture(now, "plan-create-needs-tail-extension")
	laterTail.ExpiresAtMs++
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer blockedCancel()
	if _, err := store.CompareAndSwap(blockedCtx, 0, laterTail); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("tail extension bypassed bounded proof: err=%v", err)
	}
}

func TestRedisPlanStoreExistingCASReplaysWithOriginalExpectedRevision(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	plan := redisPlanFixture(time.Now().UnixMilli(), "plan-existing-cas-replay")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	advanced := plan.Clone()
	advanced.Revision++
	advanced.UpdatedAtMs++
	advanced.Attempt++
	if result, err := store.CompareAndSwap(context.Background(), plan.Revision, advanced); err != nil || result.Status != model.CASCommitted {
		t.Fatalf("commit status=%s err=%v", result.Status, err)
	}
	if replay, err := store.CompareAndSwap(context.Background(), plan.Revision, advanced); err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("replay status=%s err=%v", replay.Status, err)
	}
}

func TestRedisPlanStoreListsPausedIndependentScalePlanChildForRecovery(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	now := time.Now().UnixMilli()
	plan := redisPlanFixture(now, "plan-paused-independent-scale-child")
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	plan.OwnerScalePlanID = "scale-plan-owner"
	plan.OwnerRetainUntilMs = plan.ExpiresAtMs + time.Hour.Milliseconds()
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	paused := plan.Clone()
	paused.Phase = model.PhasePaused
	paused.ErrorCode = "TIMEOUT"
	paused.Revision++
	paused.UpdatedAtMs++
	paused.CompletedAtMs = paused.UpdatedAtMs
	if _, err := store.CompareAndSwap(context.Background(), plan.Revision, paused); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 1 || records[0].Plan.PlanID != paused.PlanID {
		t.Fatalf("paused independent child missing from recovery: records=%v err=%v", records, err)
	}
}

func TestRedisPlanStoreValidatesAndRetainsPhysicallyExpiredEvidence(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	clock := time.Now()
	store.options.Now = func() time.Time { return clock }
	first := redisPlanFixture(clock.UnixMilli(), "plan-expired-first")
	second := redisPlanFixture(clock.UnixMilli()+1, "plan-expired-second")
	second.ExpiresAtMs = clock.Add(2 * time.Hour).UnixMilli()
	if _, err := store.CompareAndSwap(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	firstKey, _ := store.Keys().Plan(first.PlanID)
	clock = clock.Add(25*time.Hour + 30*time.Minute)
	if client.Exists(context.Background(), firstKey.String()).Val() != 1 {
		t.Fatal("shared retention did not retain the earlier physical hash")
	}
	originalChecksum := client.HGet(context.Background(), firstKey.String(), planFieldChecksum).Val()
	if err := client.HSet(context.Background(), firstKey.String(), planFieldChecksum, strings.Repeat("0", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	candidate := redisPlanFixture(clock.UnixMilli(), "plan-after-expiry")
	before := redisPhysicalState(t, client, store, first.PlanID)
	if _, err := store.CompareAndSwap(context.Background(), 0, candidate); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("corrupt expired hash err=%v", err)
	}
	after := redisPhysicalState(t, client, store, first.PlanID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("corrupt expired cleanup wrote state\nbefore=%v\nafter=%v", before, after)
	}
	if err := client.HSet(context.Background(), firstKey.String(), planFieldChecksum, originalChecksum).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, candidate); err != nil {
		t.Fatalf("exact expired reconcile err=%v", err)
	}
	if client.ZScore(context.Background(), store.Keys().Index().String(), mustEncodedMember(t, store, first.PlanID)).Err() != nil || client.Exists(context.Background(), firstKey.String()).Val() != 1 {
		t.Fatal("physically expired evidence was removed before shared tail")
	}
}

func TestRedisPlanStorePhysicallyExpiredCandidateIsImmutableAuditEvidence(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	store.options.PlanTTL = 72 * time.Hour
	clock := time.Now()
	store.options.Now = func() time.Time { return clock }
	expired := redisPlanFixture(clock.UnixMilli(), "plan-expired-candidate")
	later := redisPlanFixture(clock.UnixMilli()+1, "plan-later-retention-tail")
	later.ExpiresAtMs = clock.Add(48 * time.Hour).UnixMilli()
	if _, err := store.CompareAndSwap(context.Background(), 0, expired); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, later); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(26 * time.Hour)
	expiredKey, _ := store.Keys().Plan(expired.PlanID)
	if client.Exists(context.Background(), expiredKey.String()).Val() != 1 {
		t.Fatal("shared tail did not retain the physically expired audit hash")
	}
	before := redisPhysicalState(t, client, store, expired.PlanID)
	replacement := expired
	replacement.Revision = 2
	replacement.UpdatedAtMs = clock.UnixMilli()
	replacement.ExpiresAtMs = clock.Add(time.Hour).UnixMilli()
	if result, err := store.CompareAndSwap(context.Background(), 1, replacement); !errors.Is(err, model.ErrPlanConflict) || result.Status != model.CASRevisionConflict {
		t.Fatalf("expired replacement result=%+v err=%v", result, err)
	}
	after := redisPhysicalState(t, client, store, expired.PlanID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("expired replacement changed audit/accounting state\nbefore=%v\nafter=%v", before, after)
	}
	replay, err := store.CompareAndSwap(context.Background(), 1, expired)
	if err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("exact expired replay=%+v err=%v", replay, err)
	}
	if afterReplay := redisPhysicalState(t, client, store, expired.PlanID); !reflect.DeepEqual(before, afterReplay) {
		t.Fatalf("exact expired replay wrote state\nbefore=%v\nafter=%v", before, afterReplay)
	}
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 1 || records[0].Plan.PlanID != later.PlanID {
		t.Fatalf("post-audit cleanup records=%v err=%v", records, err)
	}
	if got := client.HGet(context.Background(), store.Keys().Meta().String(), "count").Val(); got != "2" {
		t.Fatalf("post-audit meta count=%q", got)
	}
}

func TestRedisPlanStoreReplayDoesNotPartiallyApplyExpiredCleanup(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	store.options.PlanTTL = 72 * time.Hour
	clock := time.Now()
	store.options.Now = func() time.Time { return clock }
	first := redisPlanFixture(clock.UnixMilli(), "plan-expired-before-replay")
	second := redisPlanFixture(clock.UnixMilli()+1, "plan-replayed-after-expiry")
	second.ExpiresAtMs = clock.Add(48 * time.Hour).UnixMilli()
	if _, err := store.CompareAndSwap(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(26 * time.Hour)
	replay, err := store.CompareAndSwap(context.Background(), 0, second)
	if err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if got := client.HGet(context.Background(), store.Keys().Meta().String(), "count").Val(); got != "2" {
		t.Fatalf("replay partially cleaned meta count=%q", got)
	}
	if got := client.ZCard(context.Background(), store.Keys().Index().String()).Val(); got != 2 {
		t.Fatalf("replay partially cleaned index count=%d", got)
	}
	records, err := store.ListIncomplete(context.Background())
	if err != nil || len(records) != 1 || records[0].Plan.PlanID != second.PlanID {
		t.Fatalf("post-replay cleanup records=%v err=%v", records, err)
	}
	if got := client.HGet(context.Background(), store.Keys().Meta().String(), "count").Val(); got != "2" {
		t.Fatalf("retained evidence meta count=%q", got)
	}
}

func TestRedisPlanStoreMissingTailHashIsCorrupt(t *testing.T) {
	_, client, store, _ := redisPlanStoreFixture(t, 8, 1<<20)
	clock := time.Now()
	store.options.Now = func() time.Time { return clock }
	first := redisPlanFixture(clock.UnixMilli(), "plan-retained-earlier-tail")
	second := redisPlanFixture(clock.UnixMilli()+1, "plan-missing-latest-tail")
	second.ExpiresAtMs = clock.Add(2 * time.Hour).UnixMilli()
	if _, err := store.CompareAndSwap(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, second); err != nil {
		t.Fatal(err)
	}
	secondKey, _ := store.Keys().Plan(second.PlanID)
	if err := client.Del(context.Background(), secondKey.String()).Err(); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(3 * time.Hour)
	before := redisPhysicalState(t, client, store, second.PlanID)
	if _, err := store.ListIncomplete(context.Background()); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("missing tail hash err=%v", err)
	}
	after := redisPhysicalState(t, client, store, second.PlanID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("corrupt cleanup mutated evidence\nbefore=%v\nafter=%v", before, after)
	}
}

func TestRedisPlanStoreFreezesAuditFieldsAndTerminalRecords(t *testing.T) {
	_, _, store, _ := redisPlanStoreFixture(t, 4, 1<<20)
	now := time.Now().UnixMilli()
	plan := redisPlanFixture(now, "plan-audit")
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}

	advanced := plan
	advanced.Revision = 2
	advanced.UpdatedAtMs++
	advanced.StartedAtMs = advanced.UpdatedAtMs
	advanced.Attempt = 1
	if _, err := store.CompareAndSwap(context.Background(), 1, advanced); err != nil {
		t.Fatalf("legal audit advance err=%v", err)
	}
	for name, mutate := range map[string]func(*model.Plan){
		"attempt regression": func(p *model.Plan) { p.Attempt = 0 },
		"target rewrite":     func(p *model.Plan) { p.TargetHolderID = "holder-c" },
		"expiry rewrite":     func(p *model.Plan) { p.ExpiresAtMs++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := advanced
			candidate.Revision = 3
			candidate.UpdatedAtMs++
			mutate(&candidate)
			if _, err := store.CompareAndSwap(context.Background(), 2, candidate); !errors.Is(err, model.ErrPlanConflict) {
				t.Fatalf("err=%v", err)
			}
		})
	}

	paused := advanced
	paused.Revision = 3
	paused.UpdatedAtMs++
	paused.CompletedAtMs = paused.UpdatedAtMs
	paused.Phase = model.PhasePaused
	paused.ErrorCode = model.ErrorAuthorityCommittedSnapshotPending
	if _, err := store.CompareAndSwap(context.Background(), 2, paused); err != nil {
		t.Fatal(err)
	}
	replay, err := store.CompareAndSwap(context.Background(), 3, paused)
	if err != nil || replay.Status != model.CASAlreadyCommitted {
		t.Fatalf("replay status=%v err=%v", replay.Status, err)
	}
	changed := paused
	changed.Revision = 4
	changed.UpdatedAtMs++
	changed.CompletedAtMs = changed.UpdatedAtMs
	changed.ErrorCode = "DIFFERENT"
	if _, err := store.CompareAndSwap(context.Background(), 3, changed); !errors.Is(err, model.ErrPlanConflict) {
		t.Fatalf("terminal rewrite err=%v", err)
	}
}

func TestRedisPlanStoreEnforcesConfiguredLogicalPlanTTLBeforeWrites(t *testing.T) {
	_, client, store, options := redisPlanStoreFixture(t, 4, 1<<20)
	now := time.Now().UnixMilli()
	tooLong := redisPlanFixture(now, "plan-too-long")
	tooLong.ExpiresAtMs = now + options.PlanTTL.Milliseconds() + 1
	if _, err := store.CompareAndSwap(context.Background(), 0, tooLong); !errors.Is(err, model.ErrInvalidPlan) {
		t.Fatalf("candidate TTL err=%v", err)
	}
	if client.Exists(context.Background(), store.Keys().Meta().String(), store.Keys().Index().String(), store.Keys().Restart().String(), store.Keys().Accounting().String()).Val() != 0 {
		t.Fatal("overlong candidate wrote shared state")
	}

	valid := redisPlanFixture(now, "plan-existing-too-long")
	if _, err := store.CompareAndSwap(context.Background(), 0, valid); err != nil {
		t.Fatal(err)
	}
	before := redisPhysicalState(t, client, store, valid.PlanID)
	stricter := options
	stricter.PlanTTL = 30 * time.Minute
	restarted, err := NewPlanStore(client, stricter)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := restarted.Load(context.Background(), valid.PlanID); !found || !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("direct stored TTL found=%t err=%v", found, err)
	}
	if _, err := restarted.ListIncomplete(context.Background()); !errors.Is(err, model.ErrPlanCorrupt) {
		t.Fatalf("stored TTL err=%v", err)
	}
	if after := redisPhysicalState(t, client, store, valid.PlanID); !reflect.DeepEqual(before, after) {
		t.Fatalf("stored overlong plan caused writes\nbefore=%v\nafter=%v", before, after)
	}
}

func redisPhysicalState(t *testing.T, client *redis.Client, store *PlanStore, id string) map[string]any {
	t.Helper()
	ctx := context.Background()
	key, _ := store.Keys().Plan(id)
	expiries := make(map[string]int64)
	for _, current := range []string{store.Keys().Meta().String(), store.Keys().Index().String(), store.Keys().Restart().String(), store.Keys().Accounting().String(), key.String()} {
		expiries[current] = client.PExpireTime(ctx, current).Val().Milliseconds()
	}
	return map[string]any{
		"meta":       client.HGetAll(ctx, store.Keys().Meta().String()).Val(),
		"index":      fmt.Sprint(client.ZRangeWithScores(ctx, store.Keys().Index().String(), 0, -1).Val()),
		"restart":    client.LRange(ctx, store.Keys().Restart().String(), 0, -1).Val(),
		"accounting": client.HGetAll(ctx, store.Keys().Accounting().String()).Val(),
		"plan":       client.HGetAll(ctx, key.String()).Val(),
		"expiries":   expiries,
	}
}

func mustEncodedMember(t *testing.T, store *PlanStore, id string) string {
	t.Helper()
	member, err := store.Keys().EncodedPlanID(id)
	if err != nil {
		t.Fatal(err)
	}
	return member
}
