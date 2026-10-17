package authority

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"

	"github.com/redis/go-redis/v9"
)

type redisDispatchCounter struct {
	single   atomic.Uint64
	pipeline atomic.Uint64
}

func (c *redisDispatchCounter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *redisDispatchCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.single.Add(1)
		return next(ctx, cmd)
	}
}
func (c *redisDispatchCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.pipeline.Add(1)
		return next(ctx, cmds)
	}
}

type evalReplyCapture struct {
	sha   string
	mu    sync.Mutex
	reply []string
}

func (c *evalReplyCapture) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *evalReplyCapture) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (c *evalReplyCapture) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		err := next(ctx, command)
		args := command.Args()
		if err != nil || len(args) < 2 || strings.ToLower(command.Name()) != "evalsha" || fmt.Sprint(args[1]) != c.sha {
			return err
		}
		cmd, ok := command.(*redis.Cmd)
		if !ok {
			return err
		}
		value, resultErr := cmd.Result()
		if resultErr != nil {
			return err
		}
		reply, conversionErr := scriptStrings(value)
		if conversionErr != nil {
			return err
		}
		c.mu.Lock()
		c.reply = append(c.reply[:0], reply...)
		c.mu.Unlock()
		return err
	}
}

func (c *evalReplyCapture) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.reply...)
}

func replyPayloadBytes(reply []string) int {
	total := 0
	for _, value := range reply {
		total += len(value)
	}
	return total
}

func exactTestReceiptValueBytes(t *testing.T, req authoritystore.AuthorityCommitRequest) int {
	t.Helper()
	keyIdentity, err := keyIdentityLP(req.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	statusToken, err := terminalStatusToken(req.OperationResult.Status)
	if err != nil {
		t.Fatal(err)
	}
	return 64 + 6*4 + len(keyIdentity) + len(req.Receipt.ReceiptID) + len(statusToken) +
		len(req.OperationResult.ErrorCode) + len(req.OperationResult.Payload) +
		len(strconv.FormatInt(testNow.UnixMilli(), 10))
}

func storeWithReceiptLimit(t *testing.T, rig *testRig, maxReceiptBytes int) *Store {
	t.Helper()
	store, err := NewStore(rig.client, rig.store.Registry(), Options{
		Profile:              testProfile(t),
		Resolver:             testCalculator(t),
		MaxReceiptTotalBytes: maxReceiptBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func reclaimWithStore(t *testing.T, rig *testRig, store *Store, claim string) authoritystore.AuthoritySnapshot {
	t.Helper()
	result, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress:           rig.addr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        rig.fence,
		ClaimOperationID:       "claim-op-" + claim,
		NewWriterClaimID:       claim,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load status=%s", result.Status)
	}
	return result.Snapshot
}

func TestLoadAndClaimReturnsValidatedSnapshotInSingleRedisCommandAfterScriptWarmup(t *testing.T) {
	rig := newTestRig(t)
	first := rig.load(t, "single-command-first")
	commit, err := rig.store.Commit(context.Background(), rig.commitReq("single-command-first", first.RecordVersion,
		"single-command-receipt", authoritystore.ModelState{"record": []byte("committed")}))
	if err != nil || commit.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", commit, err)
	}
	// Warm the versioned LoadAndClaim script once; the measured call must then
	// be one EVALSHA. Model and receipt values are returned from the same Lua
	// snapshot that already validated their exact bytes and hash cardinality.
	warm := reclaimWithStore(t, rig, rig.store, "single-command-warm")
	counter := &redisDispatchCounter{}
	rig.client.AddHook(counter)
	got := reclaimWithStore(t, rig, rig.store, "single-command-measured")
	if single, pipeline := counter.single.Load(), counter.pipeline.Load(); single != 1 || pipeline != 0 {
		t.Fatalf("LoadAndClaim dispatches single=%d pipeline=%d, want one EVALSHA", single, pipeline)
	}
	if got.RecordVersion != warm.RecordVersion || got.PhysicalUsage != warm.PhysicalUsage ||
		!reflect.DeepEqual(got.ModelFields, warm.ModelFields) {
		t.Fatalf("measured snapshot=%+v warm=%+v", got, warm)
	}
}

func TestOrdinaryCommitUsesV13WithoutExactOutputAcrossModelSizes(t *testing.T) {
	methods := map[string]func(context.Context, *Store, authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error){
		"commit": func(ctx context.Context, store *Store, request authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
			return store.Commit(ctx, request)
		},
		"audited": func(ctx context.Context, store *Store, request authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
			return store.CommitAuditedClaim(ctx, request)
		},
	}
	for method, commit := range methods {
		for _, size := range []int{1, 64 << 10} {
			t.Run(fmt.Sprintf("%s_model_%d", method, size), func(t *testing.T) {
				rig := newTestRig(t)
				loaded := rig.load(t, "ordinary-v13")
				request := rig.commitReq("ordinary-v13", loaded.RecordVersion, fmt.Sprintf("ordinary-v13-%s-%d", method, size),
					authoritystore.ModelState{"record": bytes.Repeat([]byte{'x'}, size)})
				request.AttachedOrders = []authoritystore.AttachedOrder{dueOrder(t, "CARE_REWARD", fmt.Sprintf("ordinary-v13-%s-%d", method, size), testNow.UnixMilli())}

				exactSHA, ok := rig.store.Registry().ScriptSHA(ScriptCommitActorWithOrdersExact)
				if !ok || scriptCached(t, rig.client, exactSHA) {
					t.Fatalf("exact V14 script unexpectedly cached before ordinary commit: sha=%s found=%t", exactSHA, ok)
				}
				result, err := commit(context.Background(), rig.store, request)
				if err != nil || result.Status != authoritystore.CommitCommitted || result.CommitStamp != nil {
					t.Fatalf("ordinary commit=%+v err=%v", result, err)
				}
				if _, explicit := result.ExplicitCommit(request); explicit {
					t.Fatal("ordinary commit exposed exact output")
				}
				if scriptCached(t, rig.client, exactSHA) {
					t.Fatal("ordinary commit selected exact V14")
				}
				ordinarySHA, ok := rig.store.Registry().ScriptSHA(ScriptCommitActorWithOrders)
				if !ok || !scriptCached(t, rig.client, ordinarySHA) {
					t.Fatalf("ordinary V13 was not selected: sha=%s found=%t", ordinarySHA, ok)
				}
			})
		}
	}
}

func TestProjectPhysicalCommitIsPure(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "pure-projection")
	request := rig.commitReq("pure-projection", loaded.RecordVersion, "pure-projection",
		authoritystore.ModelState{"record": bytes.Repeat([]byte{'p'}, 32<<10)})
	request.AttachedOrders = []authoritystore.AttachedOrder{dueOrder(t, "CARE_REWARD", "pure-projection", testNow.UnixMilli())}
	beforeAdvances, beforeAdvanceBytes, beforeCommitBytes, beforeOrderBytes, beforeReleases := sourceordermetrics.Snapshot()
	for i := 0; i < 10; i++ {
		usage, err := rig.store.ProjectPhysicalCommit(request)
		if err != nil || usage.CommitBytes == 0 || usage.ReceiptBytes == 0 {
			t.Fatalf("projection %d=%+v err=%v", i, usage, err)
		}
	}
	afterAdvances, afterAdvanceBytes, afterCommitBytes, afterOrderBytes, afterReleases := sourceordermetrics.Snapshot()
	if beforeAdvances != afterAdvances || beforeAdvanceBytes != afterAdvanceBytes || beforeCommitBytes != afterCommitBytes ||
		beforeOrderBytes != afterOrderBytes || beforeReleases != afterReleases {
		t.Fatalf("pure projection mutated metrics: before=(%d,%d,%d,%d,%d) after=(%d,%d,%d,%d,%d)",
			beforeAdvances, beforeAdvanceBytes, beforeCommitBytes, beforeOrderBytes, beforeReleases,
			afterAdvances, afterAdvanceBytes, afterCommitBytes, afterOrderBytes, afterReleases)
	}
}

func TestCommitExplicitStampUsesSameRedisCommitOutputAndNeverReplay(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "explicit-commit-stamp")
	request := rig.commitReq("explicit-commit-stamp", loaded.RecordVersion, "explicit-commit-stamp",
		authoritystore.ModelState{"record": []byte("committed")})
	request.AttachedOrders = []authoritystore.AttachedOrder{dueOrder(t, "CARE_REWARD", "explicit-commit-stamp", testNow.UnixMilli())}
	result, err := rig.store.CommitExact(context.Background(), request)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", result, err)
	}
	output, explicit := result.ExplicitCommit(request)
	if !explicit || output.RecordVersion() != loaded.RecordVersion+1 || output.CommittedAtMs() != testNow.UnixMilli() {
		t.Fatalf("explicit output=%+v valid=%v", output, explicit)
	}

	replay, err := rig.store.Commit(context.Background(), request)
	if err != nil || replay.Status != authoritystore.CommitAlreadyCommitted || replay.CommitStamp != nil {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, explicit := replay.ExplicitCommit(request); explicit {
		t.Fatal("ALREADY_COMMITTED returned an explicit commit output")
	}
}

func TestCommitAuditedClaimSkipsHistoricalReceiptReads(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "audited-claim")
	version := loaded.RecordVersion
	for i := 0; i < 8; i++ {
		result, err := rig.store.Commit(context.Background(), rig.commitReq("audited-claim", version,
			fmt.Sprintf("audit-history-%d", i), authoritystore.ModelState{"record": []byte(fmt.Sprintf("v%d", i))}))
		if err != nil || result.Status != authoritystore.CommitCommitted {
			t.Fatalf("history commit %d=%+v err=%v", i, result, err)
		}
		version = result.Snapshot.RecordVersion
	}
	beforeFull := rig.server.CommandCount()
	full, err := rig.store.Commit(context.Background(), rig.commitReq("audited-claim", version,
		"audit-history-full", authoritystore.ModelState{"record": []byte("full")}))
	if err != nil || full.Status != authoritystore.CommitCommitted {
		t.Fatalf("full commit=%+v err=%v", full, err)
	}
	fullCalls := rig.server.CommandCount() - beforeFull
	hotState := authoritystore.ModelState{"record": []byte("hot")}
	hotMutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
		authoritystore.ModelLayoutV1, hotState, hotState, nil)
	if err != nil {
		t.Fatal(err)
	}
	hotReq := rig.commitReq("audited-claim", full.Snapshot.RecordVersion, "audit-history-hot", hotState)
	hotReq.ModelMutation = hotMutation
	beforeHot := rig.server.CommandCount()
	hot, err := rig.store.CommitAuditedClaim(context.Background(), hotReq)
	if err != nil || hot.Status != authoritystore.CommitCommitted {
		t.Fatalf("audited commit=%+v err=%v", hot, err)
	}
	hotCalls := rig.server.CommandCount() - beforeHot
	if fullCalls-hotCalls < 8 {
		t.Fatalf("historical receipt scan not removed: full_calls=%d audited_calls=%d", fullCalls, hotCalls)
	}
	if hotCalls != 12 {
		t.Fatalf("audited commit Redis command budget=%d want 12 (V15 skips the current-model HGET)", hotCalls)
	}
	if hot.Snapshot.PhysicalUsage.ReceiptItems != full.Snapshot.PhysicalUsage.ReceiptItems+1 {
		t.Fatalf("audited usage=%+v full=%+v", hot.Snapshot.PhysicalUsage, full.Snapshot.PhysicalUsage)
	}
}

func TestCommitSoftFlushOmitsFreshModelStateButReplayRehydrates(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "soft-flush-result")
	state := cloneModelState(loaded.ModelFields)
	state["record"] = bytes.Repeat([]byte("f"), 8<<10)
	upserts := authoritystore.ModelState{"record": state["record"]}
	mutation, err := authoritystore.BuildActorModelMutationBorrowed(
		authoritystore.ModelMutationIntentUpdate, authoritystore.ModelLayoutV1,
		state, upserts, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := rig.commitReq("soft-flush-result", loaded.RecordVersion, "soft-flush-result-op", state)
	req.ModelMutation = mutation

	committed, err := rig.store.CommitSoftFlush(context.Background(), req)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("soft flush=%+v err=%v", committed, err)
	}
	if committed.Snapshot.RecordVersion != loaded.RecordVersion+1 ||
		committed.Snapshot.BusinessStateDigest != mutation.ResultingBusinessStateDigest ||
		!committed.Snapshot.PhysicalUsage.Trustworthy {
		t.Fatalf("soft flush metadata=%+v", committed.Snapshot)
	}
	if committed.Snapshot.ModelFields != nil {
		t.Fatalf("fresh soft flush returned redundant model state bytes=%d", authoritystore.ModelStateSerializedBytes(committed.Snapshot.ModelFields))
	}

	replayed, err := rig.store.CommitSoftFlush(context.Background(), req)
	if err != nil || replayed.Status != authoritystore.CommitAlreadyCommitted {
		t.Fatalf("soft flush replay=%+v err=%v", replayed, err)
	}
	if !reflect.DeepEqual(replayed.Snapshot.ModelFields, state) {
		t.Fatalf("replay did not return authoritative model state: fields=%d", len(replayed.Snapshot.ModelFields))
	}
}

func TestLoadAndClaimV6CommandCountIsConstantButManifestResponseIsLinear(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "v6-budget")
	loadSHA, ok := rig.store.Registry().ScriptSHA(ScriptLoadAndClaimActor)
	if !ok {
		t.Fatal("LoadAndClaim V6 is not registered")
	}
	capture := &evalReplyCapture{sha: loadSHA}
	rig.client.AddHook(capture)
	request := authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "claim-op-v6-budget", NewWriterClaimID: "v6-budget",
	}

	beforeEmpty := rig.server.CommandCount()
	empty, err := rig.store.LoadAndClaim(context.Background(), request)
	if err != nil || empty.Status != authoritystore.LoadAndClaimAlreadyClaimed {
		t.Fatalf("empty-history same claim=%+v err=%v", empty, err)
	}
	emptyCalls := rig.server.CommandCount() - beforeEmpty
	emptyReply := capture.snapshot()
	if len(emptyReply) < 17 || emptyReply[11] != "" {
		t.Fatalf("empty-history raw reply=%q", emptyReply)
	}

	const history = 8
	version := loaded.RecordVersion
	for i := 0; i < history; i++ {
		result, err := rig.store.CommitAuditedClaim(context.Background(), rig.commitReq("v6-budget", version,
			fmt.Sprintf("v6-budget-history-%d", i), cloneModelState(loaded.ModelFields)))
		if err != nil || result.Status != authoritystore.CommitCommitted {
			t.Fatalf("history commit %d=%+v err=%v", i, result, err)
		}
		version = result.Snapshot.RecordVersion
	}

	beforeHistory := rig.server.CommandCount()
	result, err := rig.store.LoadAndClaim(context.Background(), request)
	if err != nil || result.Status != authoritystore.LoadAndClaimAlreadyClaimed {
		t.Fatalf("history same claim=%+v err=%v", result, err)
	}
	historyCalls := rig.server.CommandCount() - beforeHistory
	historyReply := capture.snapshot()
	if emptyCalls != 6 || historyCalls != emptyCalls {
		t.Fatalf("same-claim Redis commands empty=%d history=%d want constant 6", emptyCalls, historyCalls)
	}
	if len(historyReply) < 17 || len(historyReply[11]) != history*(4+64) {
		t.Fatalf("history receipt manifest bytes=%d want=%d reply_fields=%d", len(historyReply[11]), history*(4+64), len(historyReply))
	}
	if growth := replyPayloadBytes(historyReply) - replyPayloadBytes(emptyReply); growth < len(historyReply[11]) {
		t.Fatalf("response payload growth=%d smaller than monolithic manifest=%d", growth, len(historyReply[11]))
	}
	if result.Snapshot.RecordVersion != version || result.Snapshot.PhysicalUsage.ReceiptItems != history {
		t.Fatalf("same-claim snapshot=%+v", result.Snapshot)
	}
}

func TestAuditedLuaBranchesDoNotIterateReceiptHistory(t *testing.T) {
	if strings.Contains(luaCommitActorWithOrdersSrc, "if audited_claim == '1' then") {
		t.Fatal("Commit V13 retained the V12 audited model-integrity shortcut")
	}
	for name, tc := range map[string]struct {
		source string
		guards []string
	}{
		"load-v6": {
			source: luaLoadAndClaimActorSrc,
			guards: []string{
				"if not same_claim then receipt_fields = lp_fields(meta[13]) end",
				"if not same_claim then\n  local seen_receipt = {}",
				"redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + model_count + receipt_count + resolution_count",
			},
		},
		"commit-v13": {
			source: luaCommitActorWithOrdersSrc,
			guards: []string{
				"if audited_claim == '0' then receipt_fields = lp_fields(meta[5] or '') end",
				"if audited_claim == '0' then\n  local seen_receipt = {}",
				"redis.call('HLEN', KEYS[2]) ~= fixed_meta_fields + #model_fields + receipt_count + resolution_count",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, guard := range tc.guards {
				if !strings.Contains(tc.source, guard) {
					t.Fatalf("script missing audited-history boundary %q", guard)
				}
			}
		})
	}
}

func sameClaimResponseMeta(history int) snapshotMeta {
	state := authoritystore.ModelState{"record": []byte("state")}
	var receiptManifest []byte
	for i := 0; i < history; i++ {
		receiptManifest = appendLP(receiptManifest, fmt.Sprintf("%064x", i+1))
	}
	receiptBytes := 0
	if history > 0 {
		// Same-claim V6 decoding trusts this atomically maintained aggregate and
		// deliberately does not include historical receipt values in preloadedValues.
		receiptBytes = history
	}
	return snapshotMeta{
		recordVersion: "1", digest: digestHex(businessStateDigest(state)),
		modelManifest: manifestLP(state), writerClaimID: "max-history-claim", writerEpoch: "1",
		modelItems: "1", modelBytes: strconv.Itoa(len("record") + len("state")),
		receiptItems: strconv.Itoa(history), receiptBytes: strconv.Itoa(receiptBytes),
		receiptManifest: string(receiptManifest), resolutionItems: "0", schemaVersion: SchemaVersion,
		authorityLayout: "v1", limitProfile: "limit-v0.1", modelLayout: authoritystore.ModelLayoutV1.String(),
		preloaded: true, preloadedReceipts: false,
		preloadedHashLen: int64(actorRecordFixedMetaFields + 1 + history),
		preloadedValues:  []interface{}{"state"},
	}
}

func TestLoadAndClaimV6MaxHistoryResponseAndAllocationBoundary(t *testing.T) {
	rig := newTestRig(t)
	maxHistory := rig.store.opts.Limits.MaxReceipts
	emptyMeta := sameClaimResponseMeta(0)
	maxMeta := sameClaimResponseMeta(maxHistory)
	if got, want := len(maxMeta.receiptManifest), maxHistory*(4+64); got != want {
		t.Fatalf("max-history manifest bytes=%d want=%d", got, want)
	}

	decode := func(meta snapshotMeta) error {
		_, err := rig.store.snapshotFromMeta(context.Background(), rig.bucket, rig.addr, meta)
		return err
	}
	if err := decode(emptyMeta); err != nil {
		t.Fatalf("empty-history response decode: %v", err)
	}
	if err := decode(maxMeta); err != nil {
		t.Fatalf("max-history response decode: %v", err)
	}

	var allocationErr error
	emptyAllocs := testing.AllocsPerRun(3, func() { allocationErr = decode(emptyMeta) })
	if allocationErr != nil {
		t.Fatalf("empty-history allocation decode: %v", allocationErr)
	}
	maxAllocs := testing.AllocsPerRun(3, func() { allocationErr = decode(maxMeta) })
	if allocationErr != nil {
		t.Fatalf("max-history allocation decode: %v", allocationErr)
	}
	if maxAllocs <= emptyAllocs+float64(maxHistory) {
		t.Fatalf("history allocation cost not exposed: empty=%.0f max=%.0f receipts=%d", emptyAllocs, maxAllocs, maxHistory)
	}

	overMeta := sameClaimResponseMeta(maxHistory + 1)
	if err := decode(overMeta); !errors.Is(err, errMalformedBlob) {
		t.Fatalf("over-max history decode err=%v", err)
	}
}

func TestCommitReturnsExactPhysicalUsageAndNearReceiptBoundary(t *testing.T) {
	t.Run("valid_next_commit_is_not_rejected_by_accumulated_estimates", func(t *testing.T) {
		rig := newTestRig(t)
		loaded := rig.load(t, "claim-1")
		firstReq := rig.commitReq("claim-1", loaded.RecordVersion, "boundary-first",
			authoritystore.ModelState{"record": []byte("first")})
		firstProjection, err := rig.store.ProjectPhysicalCommit(firstReq)
		if err != nil {
			t.Fatal(err)
		}
		first, err := rig.store.Commit(context.Background(), firstReq)
		if err != nil || first.Status != authoritystore.CommitCommitted {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		digest, err := firstReq.IdempotencyKey.OperationDigest()
		if err != nil {
			t.Fatal(err)
		}
		actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
		if err != nil {
			t.Fatal(err)
		}
		storedReceipt, err := rig.client.HGet(context.Background(), actorKey.String(), "receipt:op:"+digest).Result()
		if err != nil {
			t.Fatal(err)
		}
		usage := first.Snapshot.PhysicalUsage
		if err := usage.Validate(first.Snapshot.RecordVersion); err != nil ||
			usage.ModelBytes != len("record")+len("first") || usage.ReceiptItems != 1 ||
			usage.ReceiptBytes != len(storedReceipt) || usage.ReceiptBytes != exactTestReceiptValueBytes(t, firstReq) {
			t.Fatalf("committed usage=%+v stored_receipt_bytes=%d err=%v", usage, len(storedReceipt), err)
		}
		if firstProjection.ReceiptBytes <= usage.ReceiptBytes {
			t.Fatalf("test requires a conservative projection: projected=%d exact=%d", firstProjection.ReceiptBytes, usage.ReceiptBytes)
		}

		secondReq := rig.commitReq("claim-2", first.Snapshot.RecordVersion, "boundary-second",
			authoritystore.ModelState{"record": []byte("second")})
		secondProjection, err := rig.store.ProjectPhysicalCommit(secondReq)
		if err != nil {
			t.Fatal(err)
		}
		limit := usage.ReceiptBytes + secondProjection.ReceiptBytes
		if firstProjection.ReceiptBytes+secondProjection.ReceiptBytes <= limit {
			t.Fatal("estimated first usage would not exercise the historical false rejection")
		}
		narrow := storeWithReceiptLimit(t, rig, limit)
		reclaimed := reclaimWithStore(t, rig, narrow, "claim-2")
		if reclaimed.PhysicalUsage != usage {
			t.Fatalf("reloaded exact usage=%+v want %+v", reclaimed.PhysicalUsage, usage)
		}
		secondReq.ExpectedWriterClaimID = "claim-2"
		second, err := narrow.Commit(context.Background(), secondReq)
		if err != nil || second.Status != authoritystore.CommitCommitted {
			t.Fatalf("near-boundary second=%+v err=%v limit=%d", second, err, limit)
		}
		if err := second.Snapshot.PhysicalUsage.Validate(second.Snapshot.RecordVersion); err != nil ||
			second.Snapshot.PhysicalUsage.ReceiptBytes > limit {
			t.Fatalf("second exact usage=%+v err=%v limit=%d", second.Snapshot.PhysicalUsage, err, limit)
		}
	})

	t.Run("one_byte_over_exact_limit_is_not_accepted", func(t *testing.T) {
		rig := newTestRig(t)
		loaded := rig.load(t, "claim-1")
		firstReq := rig.commitReq("claim-1", loaded.RecordVersion, "reject-first",
			authoritystore.ModelState{"record": []byte("first")})
		first, err := rig.store.Commit(context.Background(), firstReq)
		if err != nil || first.Status != authoritystore.CommitCommitted {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		secondReq := rig.commitReq("claim-2", first.Snapshot.RecordVersion, "reject-second",
			authoritystore.ModelState{"record": []byte("second")})
		limit := first.Snapshot.PhysicalUsage.ReceiptBytes + exactTestReceiptValueBytes(t, secondReq) - 1
		narrow := storeWithReceiptLimit(t, rig, limit)
		reclaimWithStore(t, rig, narrow, "claim-2")
		secondReq.ExpectedWriterClaimID = "claim-2"
		if _, err := narrow.Commit(context.Background(), secondReq); !errors.Is(err, authoritystore.ErrCapacityExceeded) {
			t.Fatalf("one-byte-over commit err=%v limit=%d", err, limit)
		}
		after := reclaimWithStore(t, rig, narrow, "claim-3")
		if after.RecordVersion != first.Snapshot.RecordVersion || after.PhysicalUsage != first.Snapshot.PhysicalUsage {
			t.Fatalf("rejected commit changed snapshot before=%+v after=%+v", first.Snapshot, after)
		}
	})
}

func actorRecordHash(t *testing.T, rig *testRig, actorKey string) map[string]string {
	t.Helper()
	values, err := rig.client.HGetAll(context.Background(), actorKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	return values
}

func assertMalformedCommitLeavesActorUnchanged(
	t *testing.T,
	rig *testRig,
	actorKey string,
	before map[string]string,
	req authoritystore.AuthorityCommitRequest,
) {
	t.Helper()
	result, err := rig.store.Commit(context.Background(), req)
	if !errors.Is(err, errMalformedBlob) || result.Status == authoritystore.CommitCommitted {
		t.Fatalf("malformed commit=%+v err=%v", result, err)
	}
	after := actorRecordHash(t, rig, actorKey)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("malformed commit changed actor hash\nbefore=%q\nafter=%q", before, after)
	}
}

func TestLoadAndClaimV6SameClaimRejectsBoundedModelAndCardinalityCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string)
	}{
		{
			name: "same_length_model_value",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "model:initial", "other").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unknown_hash_field",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "unknown", "field").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			rig.load(t, "v6-corrupt")
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String())
			before := actorRecordHash(t, rig, actorKey.String())
			result, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
				ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
				ClaimOperationID: "claim-op-v6-corrupt", NewWriterClaimID: "v6-corrupt",
			})
			if !errors.Is(err, errMalformedBlob) || result.Status == authoritystore.LoadAndClaimAlreadyClaimed {
				t.Fatalf("same-claim corrupt load=%+v err=%v", result, err)
			}
			if after := actorRecordHash(t, rig, actorKey.String()); !reflect.DeepEqual(after, before) {
				t.Fatalf("corrupt same-claim load changed actor hash\nbefore=%q\nafter=%q", before, after)
			}
		})
	}
}

func TestCommitV13AuditedClaimRejectsBoundedModelAndCardinalityCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string)
	}{
		{
			name: "unknown_hash_field",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "unknown", "field").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "malformed_business_digest",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:business_state_digest", "bad").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := loadState(t, rig, "v13-corrupt", authoritystore.ModelState{
				"profile": []byte("stable"), "record": []byte("old"),
			})
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String())
			before := actorRecordHash(t, rig, actorKey.String())

			candidate := cloneModelState(loaded.ModelFields)
			candidate["record"] = []byte("next")
			mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
				authoritystore.ModelLayoutV1, candidate,
				authoritystore.ModelState{"record": candidate["record"]}, nil)
			if err != nil {
				t.Fatal(err)
			}
			req := rig.commitReq("v13-corrupt", loaded.RecordVersion, "v13-corrupt-"+tc.name, candidate)
			req.ModelMutation = mutation
			result, err := rig.store.CommitAuditedClaim(context.Background(), req)
			if !errors.Is(err, errMalformedBlob) || result.Status == authoritystore.CommitCommitted {
				t.Fatalf("audited corrupt commit=%+v err=%v", result, err)
			}
			if after := actorRecordHash(t, rig, actorKey.String()); !reflect.DeepEqual(after, before) {
				t.Fatalf("corrupt audited commit changed actor hash\nbefore=%q\nafter=%q", before, after)
			}
		})
	}
}

func TestCommitRejectsRogueReceiptBalancedByForgedModelCount(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	rogueDigest := strings.Repeat("a", 64)
	if err := rig.client.HSet(context.Background(), actorKey.String(),
		"receipt:op:"+rogueDigest, "rogue",
		"_meta:model_field_count", "2").Err(); err != nil {
		t.Fatal(err)
	}
	before := actorRecordHash(t, rig, actorKey.String())

	order := dueOrder(t, "CARE_REWARD", "rogue-balanced", testNow.Add(time.Hour).UnixMilli())
	req := rig.commitReq("claim-1", loaded.RecordVersion, "rogue-balanced",
		authoritystore.ModelState{"record": []byte("must-not-commit")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}
	operationDigest, err := req.IdempotencyKey.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	orderKey, err := rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}

	assertMalformedCommitLeavesActorUnchanged(t, rig, actorKey.String(), before, req)
	if exists, err := rig.client.HExists(context.Background(), actorKey.String(), "receipt:op:"+operationDigest).Result(); err != nil || exists {
		t.Fatalf("new receipt exists=%v err=%v", exists, err)
	}
	if exists, err := rig.client.Exists(context.Background(), orderKey.String()).Result(); err != nil || exists != 0 {
		t.Fatalf("new order exists=%d err=%v", exists, err)
	}
	load, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "claim-op-rogue-check", NewWriterClaimID: "claim-rogue-check",
	})
	if !errors.Is(err, errMalformedBlob) || load.Status == authoritystore.LoadAndClaimClaimed {
		t.Fatalf("rogue load=%+v err=%v", load, err)
	}
}

func TestLoadAndCommitRejectMalformedModelManifestAggregates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string, exact authoritystore.PhysicalUsage)
	}{
		{
			name: "manifest_points_to_missing_field",
			mutate: func(t *testing.T, rig *testRig, actorKey string, _ authoritystore.PhysicalUsage) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey,
					"_meta:model_manifest", string(appendLP(nil, "missing"))).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "model_bytes_forged",
			mutate: func(t *testing.T, rig *testRig, actorKey string, exact authoritystore.PhysicalUsage) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey,
					"_meta:model_total_bytes", exact.ModelBytes+1).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "manifest_count_mismatch",
			mutate: func(t *testing.T, rig *testRig, actorKey string, _ authoritystore.PhysicalUsage) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:model_manifest", "").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "claim-1")
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String(), loaded.PhysicalUsage)
			before := actorRecordHash(t, rig, actorKey.String())
			req := rig.commitReq("claim-1", loaded.RecordVersion, "bad-model-"+tc.name,
				authoritystore.ModelState{"record": []byte("must-not-commit")})
			assertMalformedCommitLeavesActorUnchanged(t, rig, actorKey.String(), before, req)

			load, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
				ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
				ClaimOperationID: "claim-op-bad-model-" + tc.name,
				NewWriterClaimID: "claim-bad-model-" + tc.name,
			})
			if !errors.Is(err, errMalformedBlob) || load.Status == authoritystore.LoadAndClaimClaimed {
				t.Fatalf("malformed load=%+v err=%v", load, err)
			}
			if after := actorRecordHash(t, rig, actorKey.String()); !reflect.DeepEqual(after, before) {
				t.Fatalf("malformed load changed actor hash\nbefore=%q\nafter=%q", before, after)
			}
		})
	}
}

func TestCommitRejectsIllegalOrDuplicateStoredModelFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string)
	}{
		{
			name: "illegal_name",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HDel(context.Background(), actorKey, "model:initial").Err(); err != nil {
					t.Fatal(err)
				}
				if err := rig.client.HSet(context.Background(), actorKey,
					"model:Bad", "state",
					"_meta:model_manifest", string(appendLP(nil, "Bad")),
					"_meta:model_total_bytes", len("Bad")+len("state")).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "duplicate_name",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				manifest := appendLP(nil, "initial")
				manifest = appendLP(manifest, "initial")
				if err := rig.client.HSet(context.Background(), actorKey,
					"model:hidden", "state",
					"_meta:model_manifest", string(manifest),
					"_meta:model_field_count", "2",
					"_meta:model_total_bytes", 2*(len("initial")+len("state"))).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "claim-1")
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String())
			before := actorRecordHash(t, rig, actorKey.String())
			req := rig.commitReq("claim-1", loaded.RecordVersion, "bad-model-name-"+tc.name,
				authoritystore.ModelState{"record": []byte("must-not-commit")})
			assertMalformedCommitLeavesActorUnchanged(t, rig, actorKey.String(), before, req)
		})
	}
}

func TestCommitLuaUsesOnlyBoundedManifestReads(t *testing.T) {
	upper := strings.ToUpper(luaCommitActorWithOrdersSrc)
	for _, command := range []string{"HGETALL", "HSCAN", "SCAN", "KEYS"} {
		if strings.Contains(upper, "REDIS.CALL('"+command+"'") {
			t.Fatalf("commit Lua contains forbidden unbounded command %s", command)
		}
	}
	for _, boundedGuard := range []string{
		"#meta[8] > max_fields * 68",
		"#meta[5] > max_receipts * 68",
		"for _, stored_field in ipairs(model_fields)",
		"for _, stored_digest in ipairs(receipt_fields)",
		"candidate_count ~= #new_fields",
		"local actual_value = resulting_values[field]",
		"actual_value ~= ARGV[candidate_base + i - 1]",
	} {
		if !strings.Contains(luaCommitActorWithOrdersSrc, boundedGuard) {
			t.Fatalf("commit Lua missing bounded manifest guard %q", boundedGuard)
		}
	}
}

func assertCorruptRecordCannotLoadOrCommit(t *testing.T, rig *testRig, version uint64, claim string) {
	t.Helper()
	load, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress:           rig.addr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        rig.fence,
		ClaimOperationID:       "claim-op-corrupt-check",
		NewWriterClaimID:       "claim-corrupt-check",
	})
	if err == nil || load.Status == authoritystore.LoadAndClaimClaimed || load.Status == authoritystore.LoadAndClaimAlreadyClaimed {
		t.Fatalf("corrupt load=%+v err=%v", load, err)
	}
	req := rig.commitReq(claim, version, "corrupt-capacity-bypass",
		authoritystore.ModelState{"record": []byte("must-not-commit")})
	commit, err := rig.store.Commit(context.Background(), req)
	if err == nil || commit.Status == authoritystore.CommitCommitted {
		t.Fatalf("corrupt commit=%+v err=%v", commit, err)
	}
}

func TestLoadRejectsMalformedOrInconsistentReceiptCounters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string)
	}{
		{
			name: "missing_receipt_manifest",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HDel(context.Background(), actorKey, "_meta:receipt_manifest").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "malformed_receipt_count",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:receipt_count", "not-a-number").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "zero_count_nonzero_bytes",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:receipt_bytes", "1").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "receipt_field_with_zero_counters",
			mutate: func(t *testing.T, rig *testRig, actorKey string) {
				t.Helper()
				rogueDigest := strings.Repeat("a", 64)
				if err := rig.client.HSet(context.Background(), actorKey, "receipt:op:"+rogueDigest, "rogue").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "claim-1")
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String())
			assertCorruptRecordCannotLoadOrCommit(t, rig, loaded.RecordVersion, "claim-1")
		})
	}
}

func TestLoadRejectsReceiptManifestAndAggregateMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, rig *testRig, actorKey string, exact authoritystore.PhysicalUsage)
	}{
		{
			name: "manifest_points_to_missing_receipt",
			mutate: func(t *testing.T, rig *testRig, actorKey string, _ authoritystore.PhysicalUsage) {
				t.Helper()
				wrongManifest := string(appendLP(nil, strings.Repeat("b", 64)))
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:receipt_manifest", wrongManifest).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "receipt_bytes_understated",
			mutate: func(t *testing.T, rig *testRig, actorKey string, exact authoritystore.PhysicalUsage) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey, "_meta:receipt_bytes", exact.ReceiptBytes-1).Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "existing_receipt_hidden_by_zero_counters",
			mutate: func(t *testing.T, rig *testRig, actorKey string, _ authoritystore.PhysicalUsage) {
				t.Helper()
				if err := rig.client.HSet(context.Background(), actorKey,
					"_meta:receipt_count", "0",
					"_meta:receipt_bytes", "0",
					"_meta:receipt_manifest", "").Err(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "claim-1")
			req := rig.commitReq("claim-1", loaded.RecordVersion, "manifest-source",
				authoritystore.ModelState{"record": []byte("committed")})
			committed, err := rig.store.Commit(context.Background(), req)
			if err != nil || committed.Status != authoritystore.CommitCommitted {
				t.Fatalf("commit=%+v err=%v", committed, err)
			}
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, rig, actorKey.String(), committed.Snapshot.PhysicalUsage)
			assertCorruptRecordCannotLoadOrCommit(t, rig, committed.Snapshot.RecordVersion, "claim-1")
		})
	}
}

func TestCommitAcceptsFriendPairRootOnFarmActor(t *testing.T) {
	rig := newTestRig(t)
	profile := testProfile(t)
	calculator := testCalculator(t)
	farmAddr := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002}
	farmBucket, err := profile.AuthorityBucketForActor(calculator, farmAddr)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := rig.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "rm-boot-1",
		},
		TransitionID:           "transition-farm-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 farmAddr.Domain,
		VirtualShard:           farmBucket.VirtualShard(),
		NewHolderID:            "farm-pod-1",
		NewHolderBootID:        "boot-1",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("farm shard claim=%s", claim.Status)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           claim.Authority.HolderID,
		ExpectedHolderBootID:       claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        claim.Authority.WriterEpoch,
		ExpectedLeaseEpoch:         claim.Authority.LeaseEpoch,
	}
	load, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress:           farmAddr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        fence,
		ClaimOperationID:       "claim-op-f1",
		NewWriterClaimID:       "claim-f1",
		InitialModelFields:     authoritystore.ModelState{"plots": []byte("dry")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if load.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("farm load=%s", load.Status)
	}
	if usage := load.Snapshot.PhysicalUsage; usage.Validate(load.Snapshot.RecordVersion) != nil || usage.ModelBytes != len("plots")+len("dry") || usage.ReceiptItems != 0 || usage.ReceiptBytes != 0 {
		t.Fatalf("farm load physical usage=%+v", usage)
	}

	// A friend CARE operation is rooted at the canonical pair yet commits its
	// first authoritative ruling into the target FarmActor.
	pairRoot := authoritystore.OperationRootRef{Kind: authoritystore.RootKindFriendPair, CanonicalID: "sid1_pair_canonical_10001_20002"}
	key := authoritystore.IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "WATER_CROP", OperationID: "care-op-1", Root: pairRoot}
	hash := authoritystore.PayloadHash{1, 2, 3}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("ok:care-op-1")}
	req := authoritystore.AuthorityCommitRequest{
		CommitRequestID:        "commit-care-op-1",
		ActorAddress:           farmAddr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        fence,
		ExpectedWriterClaimID:  "claim-f1",
		ExpectedRecordVersion:  load.Snapshot.RecordVersion,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         authoritystore.ModelState{"plots": []byte("watered")},
		Receipt:                authoritystore.AuthorityReceipt{ReceiptID: "receipt-care-op-1", IdempotencyKey: key, CanonicalPayloadHash: hash, Outcome: outcome},
		OperationResult:        outcome,
	}
	first, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != authoritystore.CommitCommitted {
		t.Fatalf("friend-pair root commit=%s", first.Status)
	}
	projected, err := rig.store.ProjectPhysicalCommit(req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != authoritystore.CommitAlreadyCommitted || string(replay.Outcome.Payload) != "ok:care-op-1" {
		t.Fatalf("replay=%+v", replay)
	}
	if usage := replay.Snapshot.PhysicalUsage; usage.Validate(replay.Snapshot.RecordVersion) != nil ||
		usage.ModelBytes != projected.ModelBytes || usage.ReceiptItems != projected.ReceiptItems || usage.ReceiptBytes > projected.ReceiptBytes {
		t.Fatalf("Redis committed physical usage=%+v projected=%+v", usage, projected)
	}
	conflicted := req
	conflicted.CanonicalPayloadHash[0] ^= 0xff
	conflicted.Receipt.CanonicalPayloadHash = conflicted.CanonicalPayloadHash
	conflict, err := rig.store.Commit(context.Background(), conflicted)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != authoritystore.CommitIdempotencyConflict {
		t.Fatalf("conflict=%s", conflict.Status)
	}
	lookup, err := rig.store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress: farmAddr, IdempotencyKey: key, CanonicalPayloadHash: hash,
	})
	if err != nil || lookup.Status != authoritystore.IdempotencyFoundMatch || lookup.ReceiptRef != "receipt-care-op-1" {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}
}

func TestRootScopedDigestIsolation(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")

	base := authoritystore.IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "PLANT_CROP", OperationID: "shared-op", Root: testRootPlayer10001}
	other := base
	other.Root = authoritystore.OperationRootRef{Kind: authoritystore.RootKindFriendPair, CanonicalID: "sid1_pair_canonical_10001_20002"}
	digestA, err := base.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	digestB, err := other.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if digestA == digestB {
		t.Fatal("same operation_id under different roots must produce different digests")
	}

	// Both roots commit independently under the same operation_id on the same actor.
	reqA := rig.commitReq("claim-1", loaded.RecordVersion, "shared-op", authoritystore.ModelState{"x": []byte("a")})
	if _, err := rig.store.Commit(context.Background(), reqA); err != nil {
		t.Fatal(err)
	}
	reqB := rig.commitReq("claim-1", loaded.RecordVersion+1, "shared-op", authoritystore.ModelState{"x": []byte("b")})
	reqB.IdempotencyKey = other
	reqB.Receipt.IdempotencyKey = other
	committedB, err := rig.store.Commit(context.Background(), reqB)
	if err != nil {
		t.Fatal(err)
	}
	if committedB.Status != authoritystore.CommitCommitted {
		t.Fatalf("second root commit=%s", committedB.Status)
	}
	replayA, err := rig.store.Commit(context.Background(), reqA)
	if err != nil {
		t.Fatal(err)
	}
	if replayA.Status != authoritystore.CommitAlreadyCommitted || string(replayA.Outcome.Payload) != "ok:shared-op" {
		t.Fatalf("replayA=%+v", replayA)
	}
}

func TestCommitRejectsEmptyRoot(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "no-root", authoritystore.ModelState{"x": []byte("y")})
	req.IdempotencyKey.Root = authoritystore.OperationRootRef{}
	req.Receipt.IdempotencyKey = req.IdempotencyKey
	if _, err := rig.store.Commit(context.Background(), req); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("empty root err=%v", err)
	}
}

func TestLoadAndClaimIsIdempotentAndFencesOldClaim(t *testing.T) {
	rig := newTestRig(t)
	req := authoritystore.LoadAndClaimRequest{
		ActorAddress:           rig.addr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        rig.fence,
		ClaimOperationID:       "claim-op-1",
		NewWriterClaimID:       "claim-1",
		InitialModelFields:     authoritystore.ModelState{"profile": []byte("v1")},
	}
	first, err := rig.store.LoadAndClaim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != authoritystore.LoadAndClaimClaimed || first.Snapshot.RecordVersion != 1 ||
		first.Snapshot.WriterClaimID != "claim-1" {
		t.Fatalf("first=%+v", first)
	}
	if string(first.Snapshot.ModelFields["profile"]) != "v1" {
		t.Fatalf("initial model not loaded: %+v", first.Snapshot.ModelFields)
	}

	// Idempotent replay (CLAIM_UNKNOWN recovery): same op id + same claim.
	again, err := rig.store.LoadAndClaim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != authoritystore.LoadAndClaimAlreadyClaimed ||
		string(again.Snapshot.ModelFields["profile"]) != "v1" {
		t.Fatalf("again=%+v", again)
	}

	// Same op id with a different claim id: ambiguous, reconcile.
	conflicted := req
	conflicted.NewWriterClaimID = "claim-other"
	got, err := rig.store.LoadAndClaim(context.Background(), conflicted)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != authoritystore.LoadAndClaimReconcileRequired {
		t.Fatalf("conflicted=%s", got.Status)
	}

	// New activation reclaims and must not reinitialize state.
	nextReq := req
	nextReq.ClaimOperationID = "claim-op-2"
	nextReq.NewWriterClaimID = "claim-2"
	nextReq.InitialModelFields = authoritystore.ModelState{"profile": []byte("must-not-reinitialize")}
	next, err := rig.store.LoadAndClaim(context.Background(), nextReq)
	if err != nil {
		t.Fatal(err)
	}
	if next.Status != authoritystore.LoadAndClaimClaimed || next.Snapshot.WriterClaimID != "claim-2" ||
		string(next.Snapshot.ModelFields["profile"]) != "v1" {
		t.Fatalf("next=%+v", next)
	}

	// The fenced old writer can no longer commit.
	oldCommit := rig.commitReq("claim-1", next.Snapshot.RecordVersion, "op-old",
		authoritystore.ModelState{"profile": []byte("illegal")})
	commit, err := rig.store.Commit(context.Background(), oldCommit)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Status != authoritystore.CommitWriterClaimMismatch {
		t.Fatalf("old writer commit status=%s", commit.Status)
	}
}

func TestCommitChecksReceiptBeforeRecordVersion(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "plant-op-1",
		authoritystore.ModelState{"plots": []byte("planted")})
	req.AttachedOrders = []authoritystore.AttachedOrder{dueOrder(t, "CARE_REWARD", "plant-op-1", 123)}

	first, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != authoritystore.CommitCommitted || first.Snapshot.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("first=%+v", first)
	}
	if string(first.Snapshot.ModelFields["plots"]) != "planted" {
		t.Fatalf("candidate snapshot missing: %+v", first.Snapshot)
	}

	// Retry with the same payload but the stale expected version: the durable
	// receipt must win over the version check.
	duplicate, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Status != authoritystore.CommitAlreadyCommitted ||
		string(duplicate.Outcome.Payload) != "ok:plant-op-1" {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	if duplicate.Snapshot.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("duplicate snapshot=%+v", duplicate.Snapshot)
	}

	// Same operation id, different payload: hard conflict.
	conflict := rig.commitReq("claim-1", loaded.RecordVersion+1, "plant-op-1",
		authoritystore.ModelState{"plots": []byte("other")})
	conflict.CanonicalPayloadHash[0] ^= 0xff
	conflict.Receipt.CanonicalPayloadHash = conflict.CanonicalPayloadHash
	got, err := rig.store.Commit(context.Background(), conflict)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != authoritystore.CommitIdempotencyConflict {
		t.Fatalf("conflict=%s", got.Status)
	}

	// A genuinely new operation with a stale version is a CAS conflict.
	stale := rig.commitReq("claim-1", loaded.RecordVersion, "plant-op-2",
		authoritystore.ModelState{"plots": []byte("v2")})
	cas, err := rig.store.Commit(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if cas.Status != authoritystore.CommitRecordVersionConflict {
		t.Fatalf("cas=%s", cas.Status)
	}
}

func TestCommitEnforcesAllFences(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	cases := []struct {
		name   string
		mutate func(*authoritystore.AuthorityCommitRequest)
		want   authoritystore.AuthorityCommitStatus
	}{
		{"holder", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedHolderID = "other" }, authoritystore.CommitWriterEpochMismatch},
		{"boot", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedHolderBootID = "other" }, authoritystore.CommitWriterEpochMismatch},
		{"assignment", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedAssignmentRevision++ }, authoritystore.CommitWriterEpochMismatch},
		{"writer_epoch", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedWriterEpoch++ }, authoritystore.CommitWriterEpochMismatch},
		{"lease_epoch", func(r *authoritystore.AuthorityCommitRequest) { r.ShardWriteFence.ExpectedLeaseEpoch++ }, authoritystore.CommitWriterEpochMismatch},
		{"claim", func(r *authoritystore.AuthorityCommitRequest) { r.ExpectedWriterClaimID = "old-claim" }, authoritystore.CommitWriterClaimMismatch},
		{"record_version", func(r *authoritystore.AuthorityCommitRequest) { r.ExpectedRecordVersion++ }, authoritystore.CommitRecordVersionConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := rig.commitReq("claim-1", loaded.RecordVersion, "op-"+tc.name,
				authoritystore.ModelState{"economy": []byte(tc.name)})
			tc.mutate(&req)
			got, err := rig.store.Commit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.want {
				t.Fatalf("status=%s want %s", got.Status, tc.want)
			}
		})
	}
}

func TestDeterministicNoChangeIsDurableTerminal(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "care-noop",
		authoritystore.ModelState{"plots": []byte("unchanged")})
	req.OperationResult = authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalNoChange}
	req.Receipt.Outcome = req.OperationResult

	first, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != authoritystore.CommitCommitted || first.Outcome.Status != authoritystore.TerminalNoChange {
		t.Fatalf("first=%+v", first)
	}
	if first.Snapshot.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("record version must advance on NO_CHANGE commit: %+v", first.Snapshot)
	}
	duplicate, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Status != authoritystore.CommitAlreadyCommitted ||
		duplicate.Outcome.Status != authoritystore.TerminalNoChange {
		t.Fatalf("duplicate=%+v", duplicate)
	}
}

func TestConcurrentCommitProducesOneEffect(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "race-op",
		authoritystore.ModelState{"economy": []byte("race")})

	const workers = 8
	results := make([]authoritystore.CommitResult, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = rig.store.Commit(context.Background(), req)
		}(i)
	}
	wg.Wait()
	committed := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d err=%v", i, errs[i])
		}
		switch results[i].Status {
		case authoritystore.CommitCommitted:
			committed++
		case authoritystore.CommitAlreadyCommitted:
		default:
			t.Fatalf("worker %d status=%s", i, results[i].Status)
		}
	}
	if committed != 1 {
		t.Fatalf("committed=%d want exactly 1", committed)
	}
}

func TestFullStateReplaceDropsStaleModelFields(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "replace-op",
		authoritystore.ModelState{"economy": []byte("only")})
	if _, err := rig.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	// Reload through a fresh claim and confirm "initial" is gone.
	snapshot, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := snapshot.ModelFields["initial"]; exists {
		t.Fatalf("stale field survived full-state replace: %+v", snapshot.ModelFields)
	}
	if string(snapshot.ModelFields["economy"]) != "only" {
		t.Fatalf("candidate field missing: %+v", snapshot.ModelFields)
	}
}

func TestCapacityFailureDoesNotPartiallyCommit(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")

	// Model byte limit: force a tiny limit through a fresh store.
	small, err := NewStore(rig.client, rig.store.Registry(), Options{
		Profile:  testProfile(t),
		Resolver: testCalculator(t),
		Limits: authoritystore.Limits{
			MaxModelFields: 2, MaxModelBytes: 8, MaxReceipts: 16,
			MaxResolutionReceipts: 4, MaxOrders: 8, MaxOutbox: 8, MaxCommitBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := rig.commitReq("claim-1", loaded.RecordVersion, "too-big",
		authoritystore.ModelState{"economy": []byte("0123456789")})
	if _, err := small.Commit(context.Background(), req); !errors.Is(err, authoritystore.ErrCapacityExceeded) {
		t.Fatalf("model bytes err=%v", err)
	}

	commitLimits := authoritystore.DefaultLimits()
	mutation, _, err := authoritystore.NormalizeActorModelMutation(req.ActorAddress, req.CandidateState, req.ModelMutation)
	if err != nil {
		t.Fatal(err)
	}
	commitLimits.MaxCommitBytes = redisCommitArgvBytes(req, mutation) - 1
	commitLimited, err := NewStore(rig.client, rig.store.Registry(), Options{
		Profile: testProfile(t), Resolver: testCalculator(t), Limits: commitLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	caps, err := commitLimited.PhysicalCapabilities()
	if err != nil || caps.MaxCommitBytes != commitLimits.MaxCommitBytes || caps.MaxSingleReceiptBytes != maxReceiptValueBytes {
		t.Fatalf("physical capabilities=%+v err=%v", caps, err)
	}
	if _, err := commitLimited.Commit(context.Background(), req); !errors.Is(err, authoritystore.ErrCapacityExceeded) {
		t.Fatalf("serialized commit bytes err=%v", err)
	}

	// Too many orders in one commit (hard cap 4).
	many := rig.commitReq("claim-1", loaded.RecordVersion, "many-orders",
		authoritystore.ModelState{"economy": []byte("x")})
	for i := 0; i < 5; i++ {
		many.AttachedOrders = append(many.AttachedOrders, dueOrder(t, "CARE_REWARD", "many-"+string(rune('a'+i)), 1))
	}
	if _, err := rig.store.Commit(context.Background(), many); !errors.Is(err, authoritystore.ErrCapacityExceeded) {
		t.Fatalf("orders err=%v", err)
	}

	// Nothing may have been written by the rejected commits.
	lookup, err := rig.store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress:         rig.addr,
		IdempotencyKey:       req.IdempotencyKey,
		CanonicalPayloadHash: req.CanonicalPayloadHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Status != authoritystore.IdempotencyNotFound {
		t.Fatalf("receipt partially committed: %+v", lookup)
	}
	snapshot, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RecordVersion != loaded.RecordVersion || string(snapshot.ModelFields["initial"]) != "state" {
		t.Fatalf("state partially committed: %+v", snapshot)
	}
}

func TestCommitWithOrdersAttachesDueIndexAtomically(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	order := dueOrder(t, "CARE_REWARD", "order-op", testNow.Add(10_000_000).UnixMilli())
	req := rig.commitReq("claim-1", loaded.RecordVersion, "order-op",
		authoritystore.ModelState{"economy": []byte("with-order")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}

	if _, err := rig.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	// Work record exists in the same slot with the due score in the DueIndex.
	orderKey, err := rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	state, err := rig.client.HGet(context.Background(), orderKey.String(), "state").Result()
	if err != nil || state != "PENDING" {
		t.Fatalf("order state=%q err=%v", state, err)
	}
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	workRef, err := workRefOf(order)
	if err != nil {
		t.Fatal(err)
	}
	score, err := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()
	if err != nil {
		t.Fatal(err)
	}
	if int64(score) != order.DueAtMs {
		t.Fatalf("due score=%v want %d", score, order.DueAtMs)
	}

	// A conflicting work attachment under the same work id aborts the whole commit.
	loadedNow, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil {
		t.Fatal(err)
	}
	conflictReq := rig.commitReq("claim-1", loadedNow.RecordVersion, "order-op-2",
		authoritystore.ModelState{"economy": []byte("changed")})
	conflicted := order
	conflicted.Payload = []byte("different-payload")
	conflictReq.AttachedOrders = []authoritystore.AttachedOrder{conflicted}
	if _, err := rig.store.Commit(context.Background(), conflictReq); !errors.Is(err, authoritystore.ErrWorkConflict) {
		t.Fatalf("work conflict err=%v", err)
	}
	after, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil {
		t.Fatal(err)
	}
	if after.RecordVersion != loadedNow.RecordVersion || string(after.ModelFields["economy"]) != "with-order" {
		t.Fatalf("conflicting order partially committed: %+v", after)
	}
}

func TestLookupIdempotencyOutcome(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-1")
	req := rig.commitReq("claim-1", loaded.RecordVersion, "lookup-op",
		authoritystore.ModelState{"economy": []byte("x")})
	if _, err := rig.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	found, err := rig.store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress:         rig.addr,
		IdempotencyKey:       req.IdempotencyKey,
		CanonicalPayloadHash: req.CanonicalPayloadHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if found.Status != authoritystore.IdempotencyFoundMatch ||
		string(found.Outcome.Payload) != "ok:lookup-op" || found.ReceiptRef != "receipt-lookup-op" {
		t.Fatalf("found=%+v", found)
	}

	conflictHash := req.CanonicalPayloadHash
	conflictHash[0] ^= 0xff
	conflict, err := rig.store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress:         rig.addr,
		IdempotencyKey:       req.IdempotencyKey,
		CanonicalPayloadHash: conflictHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != authoritystore.IdempotencyHashConflict || conflict.ReceiptRef != "receipt-lookup-op" {
		t.Fatalf("conflict=%+v", conflict)
	}

	missing := req
	missing.IdempotencyKey.OperationID = "never-seen"
	notFound, err := rig.store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress:         rig.addr,
		IdempotencyKey:       missing.IdempotencyKey,
		CanonicalPayloadHash: missing.CanonicalPayloadHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if notFound.Status != authoritystore.IdempotencyNotFound {
		t.Fatalf("notFound=%+v", notFound)
	}
}

func TestGenericCommitChargesUnchanged8KiBCandidateAtExactMaxCommitBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		limitOffset int
		wantErr     bool
	}{
		{name: "exact_limit", limitOffset: 0},
		{name: "one_byte_below", limitOffset: -1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "claim-sparse-boundary")
			keep := bytes.Repeat([]byte("k"), 8<<10)
			first := rig.commitReq("claim-sparse-boundary", loaded.RecordVersion, "seed-two-fields",
				authoritystore.ModelState{"keep": keep, "dirty": []byte("a")})
			committed, err := rig.store.Commit(context.Background(), first)
			if err != nil || committed.Status != authoritystore.CommitCommitted {
				t.Fatalf("seed commit=%+v err=%v", committed, err)
			}

			candidate := authoritystore.ModelState{"keep": keep, "dirty": []byte("b")}
			mutation, err := authoritystore.BuildActorModelMutation(
				authoritystore.ModelMutationIntentUpdate, authoritystore.ModelLayoutV1,
				candidate, authoritystore.ModelState{"dirty": []byte("b")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			req := rig.commitReq("claim-sparse-boundary", committed.Snapshot.RecordVersion, "sparse-dirty", candidate)
			req.ModelMutation = mutation
			charged := redisCommitArgvBytes(req, mutation)
			emptyCandidateValues := req
			emptyCandidateValues.CandidateState = authoritystore.ModelState{"keep": nil, "dirty": nil}
			if delta := charged - redisCommitArgvBytes(emptyCandidateValues, mutation); delta != len(keep)+len("b") {
				t.Fatalf("V13 candidate value charge=%d want=%d", delta, len(keep)+len("b"))
			}

			limits := authoritystore.DefaultLimits()
			limits.MaxCommitBytes = charged + tc.limitOffset
			limited, err := NewStore(rig.client, rig.store.Registry(), Options{
				Profile: testProfile(t), Resolver: testCalculator(t), Limits: limits,
			})
			if err != nil {
				t.Fatal(err)
			}
			projection, err := limited.ProjectPhysicalCommit(req)
			if err != nil || projection.CommitBytes != charged {
				t.Fatalf("projection=%+v err=%v charged=%d", projection, err, charged)
			}
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			before := actorRecordHash(t, rig, actorKey.String())
			result, err := limited.Commit(context.Background(), req)
			if tc.wantErr {
				if !errors.Is(err, authoritystore.ErrCapacityExceeded) || result.Status == authoritystore.CommitCommitted {
					t.Fatalf("below-boundary commit=%+v err=%v", result, err)
				}
				if after := actorRecordHash(t, rig, actorKey.String()); !reflect.DeepEqual(after, before) {
					t.Fatal("below-boundary rejection changed actor record")
				}
				return
			}
			if err != nil || result.Status != authoritystore.CommitCommitted {
				t.Fatalf("exact-boundary commit=%+v err=%v", result, err)
			}
			if string(result.Snapshot.ModelFields["dirty"]) != "b" || !bytes.Equal(result.Snapshot.ModelFields["keep"], keep) {
				t.Fatal("exact-boundary sparse merge lost candidate fields")
			}
		})
	}
}

func TestAuditedCommitOmitsUnchangedCandidateBytes(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-audited-sparse")
	keep := bytes.Repeat([]byte("k"), 8<<10)
	first := rig.commitReq("claim-audited-sparse", loaded.RecordVersion, "seed-audited-two-fields",
		authoritystore.ModelState{"keep": keep, "dirty": []byte("a")})
	committed, err := rig.store.Commit(context.Background(), first)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("seed commit=%+v err=%v", committed, err)
	}

	candidate := authoritystore.ModelState{"keep": keep, "dirty": []byte("b")}
	mutation, err := authoritystore.BuildActorModelMutation(
		authoritystore.ModelMutationIntentUpdate, authoritystore.ModelLayoutV1,
		candidate, authoritystore.ModelState{"dirty": []byte("b")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := rig.commitReq("claim-audited-sparse", committed.Snapshot.RecordVersion, "audited-sparse-dirty", candidate)
	req.ModelMutation = mutation
	genericCharge := redisCommitArgvBytes(req, mutation)
	auditedCharge := redisCommitArgvBytesForMode(req, mutation, true)
	if delta := genericCharge - auditedCharge; delta != len(keep) {
		t.Fatalf("audited unchanged candidate saving=%d want=%d", delta, len(keep))
	}

	limits := authoritystore.DefaultLimits()
	limits.MaxCommitBytes = auditedCharge
	limited, err := NewStore(rig.client, rig.store.Registry(), Options{
		Profile: testProfile(t), Resolver: testCalculator(t), Limits: limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := limited.CommitAuditedClaim(context.Background(), req)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("audited sparse commit=%+v err=%v", result, err)
	}
	if string(result.Snapshot.ModelFields["dirty"]) != "b" || !bytes.Equal(result.Snapshot.ModelFields["keep"], keep) {
		t.Fatal("audited sparse merge lost candidate fields")
	}
}
