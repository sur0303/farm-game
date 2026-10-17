package carerecovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type forbiddenTheftAdvancer struct{ calls int }

func (a *forbiddenTheftAdvancer) CreateIfAbsentOrContinue(context.Context, theftrecovery.AdvanceRequest) (theftrecovery.AdvanceResult, error) {
	a.calls++
	return theftrecovery.AdvanceResult{}, errors.New("legacy CARE reached Theft advancer")
}

func (a *forbiddenTheftAdvancer) ReceiptFirst(context.Context, theftrecovery.ReceiptRequest) (theftrecovery.AdvanceResult, error) {
	a.calls++
	return theftrecovery.AdvanceResult{}, errors.New("legacy CARE reached Theft receipt")
}

type forbiddenTheftResultProcessor struct{ calls int }

func (p *forbiddenTheftResultProcessor) ProcessResultOrder(context.Context, theftrecovery.DispatchCandidate) (bool, error) {
	p.calls++
	return false, errors.New("legacy CARE reached Theft result processor")
}

type pairCareInspector struct {
	response *logicrpcpb.InspectFarmOperationResponse
	err      error
	calls    int
}

func (i *pairCareInspector) InspectFarmOperation(context.Context, *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error) {
	i.calls++
	return i.response, i.err
}

type pairCareFixture struct {
	client     *redis.Client
	repos      *friend.RedisFriendshipRepositories
	dispatch   *theftrecovery.FriendPairDispatchRepository
	leases     theftrecovery.WorkLeaseRepository
	candidates []theftrecovery.DispatchCandidate
	decoder    *int
}

func newPairCareFixture(t *testing.T) *pairCareFixture {
	return newPairCareFixtureWithRejectedAdmission(t, false)
}

func newPairCareFixtureWithRejectedAdmission(t *testing.T, rejected bool) *pairCareFixture {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.UnixMilli(1_800_100_000_000))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, err := rediskey.NewNamespace("test", "pair-care-recovery")
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repos, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	const visitor, owner = uint64(81001), uint64(82002)
	pairID, _ := friend.RelationIDFor(visitor, owner)
	activationHash := sha256.Sum256([]byte("pair-care-activation"))
	activated, err := repos.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "pair-care-token", TokenGeneration: 1, OperationID: "pair-care-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "pair-care-activate", ParticipantStep: "ACTIVATE"}, activationHash[:])
	if err != nil || activated.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", activated, err)
	}
	if rejected {
		blockHash := sha256.Sum256([]byte("pair-care-block"))
		blocked, blockErr := repos.Pair.BlockPlayer(context.Background(), friend.PairMutationCommand{
			Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "BLOCK_PLAYER", OperationID: "pair-care-block"},
			PayloadHash: blockHash[:], CallerPlayerID: visitor, PeerPlayerID: owner,
		})
		if blockErr != nil || blocked.Status != friend.StatusSucceeded {
			t.Fatalf("block=%+v err=%v", blocked, blockErr)
		}
	}
	payloadHash := sha256.Sum256([]byte("pair-care-payload"))
	targetHash := sha256.Sum256([]byte("pair-care-target"))
	admission, err := repos.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "CARE", OperationID: "pair-care-operation"},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeCare, TargetBindingHash: targetHash[:], PermitTTLMs: 60_000,
	})
	wantAdmission := friend.StatusSucceeded
	if rejected {
		wantAdmission = friend.StatusRejected
	}
	if err != nil || admission.Outcome.Status != wantAdmission {
		t.Fatalf("admission=%+v want=%v err=%v", admission, wantAdmission, err)
	}
	decoderCalls := 0
	dispatch, err := theftrecovery.NewFriendPairDispatchRepository(theftrecovery.FriendPairDispatchRepositoryConfig{
		Repository: repos,
		TargetDecode: func(context.Context, friend.PairRecoverySourceOrder, friend.ActionPermitSnapshot) (theftrecovery.FriendPairTheftTarget, error) {
			decoderCalls++
			return theftrecovery.FriendPairTheftTarget{}, errors.New("CARE reached Theft target decoder")
		},
		MaxBuckets: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := dispatch.Discover(context.Background(), theftrecovery.DiscoveryRequest{Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	var candidates []theftrecovery.DispatchCandidate
	for _, candidate := range page.Candidates {
		ref, decodeErr := workidentity.DecodeWorkRefV1(candidate.Member)
		if decodeErr != nil || (ref.WorkType != theftrecovery.FriendActionDispatchOrder && ref.WorkType != theftrecovery.ResultPublishOrder) {
			continue
		}
		scope, scopeErr := dispatch.ActionScope(context.Background(), candidate)
		if errors.Is(scopeErr, theftrecovery.ErrNotTheft) {
			continue
		}
		if scopeErr != nil {
			t.Fatalf("classify candidate %q: %v", candidate.Member, scopeErr)
		}
		if scope == friend.ActionScopeCare {
			candidates = append(candidates, candidate)
		}
	}
	wantCandidates := 2
	if rejected {
		wantCandidates = 1
	}
	if len(candidates) != wantCandidates {
		t.Fatalf("CARE candidates=%+v want=%d", candidates, wantCandidates)
	}
	leases, err := theftrecovery.NewFriendPairWorkLeaseRepository(repos, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &pairCareFixture{client: client, repos: repos, dispatch: dispatch, leases: leases,
		candidates: candidates, decoder: &decoderCalls}
}

func newPairCareProcessorForTest(t *testing.T, fixture *pairCareFixture, inspector *pairCareInspector, holder string) *PairCareProcessor {
	t.Helper()
	processor, err := NewPairCareProcessor(PairCareProcessorConfig{
		Pair: fixture.repos, Dispatch: fixture.dispatch, Leases: fixture.leases, Farm: inspector,
		HolderID: holder, BootID: holder + "-boot", LeaseToken: holder + "-lease", LeaseTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func TestPairCareProcessorConvergesDispatchAndResultWithoutTheftTargetLookup(t *testing.T) {
	fixture := newPairCareFixture(t)
	inspector := &pairCareInspector{response: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
	}}
	processor := newPairCareProcessorForTest(t, fixture, inspector, "care-owner-1")
	for _, candidate := range fixture.candidates {
		completed, err := processor.ProcessCareOrder(context.Background(), candidate)
		if err != nil || !completed {
			t.Fatalf("process %q completed=%v err=%v", candidate.Member, completed, err)
		}
	}
	if *fixture.decoder != 0 || inspector.calls != 2 {
		t.Fatalf("Theft decoder=%d Farm inspections=%d", *fixture.decoder, inspector.calls)
	}
	for _, candidate := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		state, err := fixture.client.HGet(context.Background(), key, "state").Result()
		if err != nil {
			t.Fatal(err)
		}
		want := "COMPLETED"
		if ref.WorkType == theftrecovery.ResultPublishOrder {
			want = "SUCCEEDED"
		}
		if state != want {
			t.Fatalf("%s state=%q want=%q", ref.WorkType, state, want)
		}
		due, _ := fixture.repos.PairDueIndexKey(candidate.BucketID)
		if _, err := fixture.client.ZScore(context.Background(), due, candidate.Member).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("completed CARE member remains due: %v", err)
		}
	}
}

func TestLegacyCareDueUpgradeRoutesOnceToCareAndDoesNotSpinLeaseRetryOrLastError(t *testing.T) {
	// The fixture commits both CARE source orders before the new worker and its
	// scope router are constructed. This is the upgrade shape of an already
	// persisted legacy Pair DueIndex residue.
	fixture := newPairCareFixture(t)
	ctx := context.Background()
	inspector := &pairCareInspector{response: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
	}}
	careProcessor := newPairCareProcessorForTest(t, fixture, inspector, "care-upgrade-owner")
	theftAdvancer := &forbiddenTheftAdvancer{}
	theftResults := &forbiddenTheftResultProcessor{}
	worker, err := theftrecovery.New(theftrecovery.Config{
		PairRepository: fixture.dispatch, Leases: fixture.leases, Advancer: theftAdvancer,
		ResultProcessor: theftResults, CareProcessor: careProcessor,
		HolderID: "shared-upgrade-worker", BootID: "shared-upgrade-boot", LeaseToken: "shared-upgrade-lease",
		LeaseTTL: time.Second, OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CareCompleted != 2 || stats.InvalidOrders != 0 || stats.Retried != 0 || stats.Errors != 0 ||
		theftAdvancer.calls != 0 || theftResults.calls != 0 || *fixture.decoder != 0 {
		t.Fatalf("upgrade stats=%+v theft_advancer=%d theft_results=%d decoder=%d", stats,
			theftAdvancer.calls, theftResults.calls, *fixture.decoder)
	}

	after := make(map[string]map[string]string, len(fixture.candidates))
	for _, candidate := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		fields, readErr := fixture.client.HGetAll(ctx, key).Result()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if fields["retry_count"] != "0" {
			t.Fatalf("legacy CARE retry_count drift for %s: %+v", ref.WorkType, fields)
		}
		if ref.WorkType == theftrecovery.FriendActionDispatchOrder && fields["lease_epoch"] != "1" {
			t.Fatalf("CARE owner did not use exactly one dispatch lease: %+v", fields)
		}
		if ref.WorkType == theftrecovery.ResultPublishOrder && (fields["lease_epoch"] != "0" || fields["lease_holder_id"] != "" || fields["lease_token_hash"] != "") {
			t.Fatalf("CARE result unexpectedly acquired a WorkLease: %+v", fields)
		}
		recoveryKey, _ := fixture.repos.PairRecoveryKey(candidate.BucketID, ref.WorkID)
		if lastError := fixture.client.HGet(ctx, recoveryKey, "last_error_class").Val(); lastError != "" {
			t.Fatalf("legacy CARE persisted last_error_class=%q", lastError)
		}
		after[candidate.Member] = fields
	}

	// Repeated post-upgrade scans see no due members and cannot reacquire the
	// completed dispatch lease or increment retry/version again.
	for round := 0; round < 5; round++ {
		repeat, runErr := worker.RunOnce(ctx)
		if runErr != nil || repeat.Discovered != 0 || repeat.InvalidOrders != 0 || repeat.Retried != 0 {
			t.Fatalf("repeat round %d stats=%+v err=%v", round+1, repeat, runErr)
		}
	}
	for _, candidate := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		fields, _ := fixture.client.HGetAll(ctx, key).Result()
		if !reflect.DeepEqual(fields, after[candidate.Member]) {
			t.Fatalf("legacy CARE order changed during repeated scans: %q", candidate.Member)
		}
	}
}

func TestPairCareRejectedAdmissionCompletesFromValidatedPairReceiptWithoutFarmLookup(t *testing.T) {
	fixture := newPairCareFixtureWithRejectedAdmission(t, true)
	inspector := &pairCareInspector{err: errors.New("Farm inspector must not be called for rejected Pair admission")}
	processor := newPairCareProcessorForTest(t, fixture, inspector, "care-rejected-owner")
	candidate := fixture.candidates[0]
	ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
	binding, readErr := fixture.repos.ReadPairActionResultOrder(context.Background(), candidate.BucketID, ref.WorkType, ref.WorkID)
	if readErr != nil {
		t.Fatalf("read rejected Pair binding: %v", readErr)
	}
	if binding.AdmissionStatus != friend.StatusRejected || binding.AdmissionReason != friend.ReasonVisitNotPermitted ||
		binding.DispatchOrderID != "" || binding.WorkflowDeadlineAtMs != 0 {
		t.Fatalf("rejected Pair binding shape=%+v", binding)
	}
	completed, err := processor.ProcessCareOrder(context.Background(), candidate)
	if err != nil || !completed {
		t.Fatalf("rejected Pair result completed=%v err=%v", completed, err)
	}
	if inspector.calls != 0 || *fixture.decoder != 0 {
		t.Fatalf("rejected Pair used Farm/Theft path inspector=%d decoder=%d", inspector.calls, *fixture.decoder)
	}
	key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
	if state := fixture.client.HGet(context.Background(), key, "state").Val(); state != "REJECTED" {
		t.Fatalf("rejected Pair result state=%q", state)
	}
	due, _ := fixture.repos.PairDueIndexKey(candidate.BucketID)
	if _, err := fixture.client.ZScore(context.Background(), due, candidate.Member).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("rejected Pair result remains due: %v", err)
	}
}

func TestAcceptedCareResultMissingDispatchFieldFailsClosedAndRemainsDue(t *testing.T) {
	fixture := newPairCareFixture(t)
	ctx := context.Background()
	var candidate theftrecovery.DispatchCandidate
	for _, item := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(item.Member)
		if ref.WorkType == theftrecovery.ResultPublishOrder {
			candidate = item
			break
		}
	}
	if candidate.Member == "" {
		t.Fatal("accepted CARE result candidate missing")
	}
	ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
	key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
	if err := fixture.client.HDel(ctx, key, "dispatch_order_id").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repos.ReadPairActionResultOrder(ctx, candidate.BucketID, ref.WorkType, ref.WorkID); !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("corrupt accepted result error=%v, want ErrFriendReconcile", err)
	}
	inspector := &pairCareInspector{response: &logicrpcpb.InspectFarmOperationResponse{Status: commonpb.CommandStatus_SUCCEEDED}}
	processor := newPairCareProcessorForTest(t, fixture, inspector, "care-corrupt-owner")
	if completed, err := processor.ProcessCareOrder(ctx, candidate); completed || !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("corrupt accepted result completed=%v err=%v", completed, err)
	}
	fields, _ := fixture.client.HGetAll(ctx, key).Result()
	if fields["state"] != "PENDING" || fields["retry_count"] != "0" || fields["lease_epoch"] != "0" || inspector.calls != 0 {
		t.Fatalf("corrupt accepted result mutated or inspected Farm: fields=%+v calls=%d", fields, inspector.calls)
	}
	due, _ := fixture.repos.PairDueIndexKey(candidate.BucketID)
	if _, err := fixture.client.ZScore(ctx, due, candidate.Member).Result(); err != nil {
		t.Fatalf("corrupt accepted result no longer due: %v", err)
	}
}

func TestSharedPairWorkerRoutesRejectedCareAndLeavesOtherPairWorkNotOwned(t *testing.T) {
	fixture := newPairCareFixtureWithRejectedAdmission(t, true)
	inspector := &pairCareInspector{err: errors.New("Farm inspector must not be called for rejected Pair admission")}
	careProcessor := newPairCareProcessorForTest(t, fixture, inspector, "care-rejected-shared-owner")
	theftAdvancer := &forbiddenTheftAdvancer{}
	theftResults := &forbiddenTheftResultProcessor{}
	worker, err := theftrecovery.New(theftrecovery.Config{
		PairRepository: fixture.dispatch, Leases: fixture.leases, Advancer: theftAdvancer,
		ResultProcessor: theftResults, CareProcessor: careProcessor,
		HolderID: "shared-rejected-worker", BootID: "shared-rejected-boot", LeaseToken: "shared-rejected-lease",
		LeaseTTL: time.Second, OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.CareCompleted != 1 || stats.InvalidOrders != 0 || stats.Errors != 0 || stats.NotOwned < 1 || stats.NonTheft < 1 ||
		inspector.calls != 0 || theftAdvancer.calls != 0 || theftResults.calls != 0 || *fixture.decoder != 0 {
		t.Fatalf("shared rejected stats=%+v inspector=%d theft_advancer=%d theft_results=%d decoder=%d", stats,
			inspector.calls, theftAdvancer.calls, theftResults.calls, *fixture.decoder)
	}
}

func TestPairCareProcessorRestartLeavesUncommittedFarmWorkUntouchedThenConverges(t *testing.T) {
	fixture := newPairCareFixture(t)
	ctx := context.Background()
	before := make(map[string]map[string]string, len(fixture.candidates))
	for _, candidate := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		fields, err := fixture.client.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		before[candidate.Member] = fields
	}
	missing := &pairCareInspector{response: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND,
	}}
	first := newPairCareProcessorForTest(t, fixture, missing, "care-before-restart")
	for _, candidate := range fixture.candidates {
		completed, err := first.ProcessCareOrder(ctx, candidate)
		if err != nil || completed {
			t.Fatalf("missing Farm receipt completed=%v err=%v", completed, err)
		}
	}
	for _, candidate := range fixture.candidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repos.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		fields, _ := fixture.client.HGetAll(ctx, key).Result()
		if !reflect.DeepEqual(fields, before[candidate.Member]) {
			t.Fatalf("uncommitted CARE order mutated before restart: %q", candidate.Member)
		}
	}

	terminal := &pairCareInspector{response: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_NO_CHANGE, ErrorCode: commonpb.ErrorCode_OK,
	}}
	restarted := newPairCareProcessorForTest(t, fixture, terminal, "care-after-restart")
	for _, candidate := range fixture.candidates {
		completed, err := restarted.ProcessCareOrder(ctx, candidate)
		if err != nil || !completed {
			t.Fatalf("restart process %q completed=%v err=%v", candidate.Member, completed, err)
		}
	}
	if *fixture.decoder != 0 {
		t.Fatalf("CARE called Theft target decoder %d times", *fixture.decoder)
	}
}
