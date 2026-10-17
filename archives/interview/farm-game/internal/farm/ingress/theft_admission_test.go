package ingress

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	frienddomain "farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type theftAdmissionFake struct {
	active       bool
	commitCalls  int
	getCalls     int
	loseResponse bool
	stored       map[string]frienddomain.ActionAdmissionOutcome
	lastCommand  frienddomain.ActionAdmissionCommand
}

type concurrentTheftDispatchFake struct {
	mu    sync.Mutex
	calls []string
}

type pairReadBarrier struct {
	arrived        atomic.Int32
	release        chan struct{}
	once           sync.Once
	prepareArrived atomic.Int32
	prepareRelease chan struct{}
	prepareOnce    sync.Once
}

type pairReadBarrierHook struct {
	barrier     *pairReadBarrier
	used        atomic.Bool
	prepareUsed atomic.Bool
}

func (h *pairReadBarrierHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *pairReadBarrierHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if err == nil && cmd.Name() == "hmget" && len(args) >= 2 &&
			strings.Contains(fmt.Sprint(args[1]), ":friend-auth:pair:") && h.used.CompareAndSwap(false, true) {
			if h.barrier.arrived.Add(1) == 2 {
				h.barrier.once.Do(func() { close(h.barrier.release) })
			}
			select {
			case <-h.barrier.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return err
		}
		if h.used.Load() && (cmd.Name() == "evalsha" || cmd.Name() == "eval") && h.prepareUsed.CompareAndSwap(false, true) {
			if h.barrier.prepareArrived.Add(1) == 2 {
				h.barrier.prepareOnce.Do(func() { close(h.barrier.prepareRelease) })
			}
			select {
			case <-h.barrier.prepareRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}
}
func (h *pairReadBarrierHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (d *concurrentTheftDispatchFake) DispatchTheft(_ context.Context, orderID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, orderID)
	return nil
}

func TestTheftAdmissionTwoServicesRefreshConcurrentPairRecordVersion(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 64})
	clientB := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 64})
	t.Cleanup(func() { _ = client.Close(); _ = clientB.Close() })
	namespace, err := rediskey.NewNamespace("test", "theft-pair-race")
	if err != nil {
		t.Fatal(err)
	}
	profile := frienddomain.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 8, 8, 8
	repositories, err := frienddomain.NewRedisFriendshipRepositories(client, frienddomain.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	repositoriesB, err := frienddomain.NewRedisFriendshipRepositories(clientB, frienddomain.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	pairID, err := frienddomain.RelationIDFor(10, 9)
	if err != nil {
		t.Fatal(err)
	}
	activateHash := sha256.Sum256([]byte("theft-pair-race-activate"))
	activated, err := repositories.Pair.ActivateFriendRelation(context.Background(), frienddomain.ActivateCommand{
		InviterPlayerID: 10, RedeemerPlayerID: 9, TargetRelationEpoch: 1, TokenID: "theft-pair-race-token",
		TokenGeneration: 1, OperationID: "theft-pair-race-activate",
	}, frienddomain.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "theft-pair-race-activate", ParticipantStep: "ACTIVATE"}, activateHash[:])
	if err != nil || activated.Status != frienddomain.StatusSucceeded {
		t.Fatalf("activate outcome=%+v err=%v", activated, err)
	}
	readBarrier := &pairReadBarrier{release: make(chan struct{}), prepareRelease: make(chan struct{})}
	client.AddHook(&pairReadBarrierHook{barrier: readBarrier})
	clientB.AddHook(&pairReadBarrierHook{barrier: readBarrier})
	serviceA := newTheftAdmissionTestService(repositories.Pair, &concurrentTheftDispatchFake{})
	serviceB := newTheftAdmissionTestService(repositoriesB.Pair, &concurrentTheftDispatchFake{})
	start := make(chan struct{})
	errorsByOperation := make(chan error, 2)
	requests := make([]*logicrpcpb.ExecuteCommandRequest, 2)
	for index := range requests {
		requests[index], _ = theftRequestForTest(t, fmt.Sprintf("theft-pair-race-%02d", index), 0, fmt.Sprintf("cycle-%02d", index))
	}
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		index := index
		service := serviceA
		if index%2 == 1 {
			service = serviceB
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := service.ExecuteCommand(testCtx, requests[index])
			if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
				errorsByOperation <- fmt.Errorf("operation %d response=%v err=%v", index, response, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errorsByOperation)
	for err := range errorsByOperation {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	// The same public identity with a different canonical payload remains a
	// conflict even though record-version-only refresh is allowed.
	first, _ := theftRequestForTest(t, "theft-pair-payload-conflict", 0, "cycle-conflict")
	if response, err := serviceA.ExecuteCommand(context.Background(), first); err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("first conflict seed response=%v err=%v", response, err)
	}
	changed, _ := theftRequestForTest(t, "theft-pair-payload-conflict", 1, "cycle-conflict")
	response, err := serviceB.ExecuteCommand(context.Background(), changed)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_REJECTED ||
		response.GetBusiness().Error.GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("payload conflict response=%v err=%v", response, err)
	}
}

type blockingPairAdmissionRepository struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingPairAdmissionRepository) CommitFriendActionAdmission(ctx context.Context, command frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error) {
	if command.OwnerPlayerID == 9 {
		r.once.Do(func() { close(r.started) })
		select {
		case <-r.release:
		case <-ctx.Done():
			return frienddomain.ActionAdmissionOutcome{}, ctx.Err()
		}
	}
	return fakeTheftAdmission(command), nil
}

func (r *blockingPairAdmissionRepository) GetFriendActionAdmission(context.Context, frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error) {
	return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrOutcomeNotFound
}

func TestTheftAdmissionDifferentPairsAreNotGloballySerialized(t *testing.T) {
	repository := &blockingPairAdmissionRepository{started: make(chan struct{}), release: make(chan struct{})}
	service := NewService(nil, nil)
	service.SetTheftPairRepository(repository)
	command := func(owner uint64) frienddomain.ActionAdmissionCommand {
		return frienddomain.ActionAdmissionCommand{Identity: frienddomain.CommitIdentity{SubjectPlayerID: 10,
			CommandType: "THEFT", OperationID: fmt.Sprintf("different-pair-%d", owner)}, PayloadHash: []byte("payload"),
			VisitorPlayerID: 10, OwnerPlayerID: owner, TargetFarmID: owner, Scope: frienddomain.ActionScopeTheft,
			TargetBindingHash: []byte("target"), NowMs: 100, PermitTTLMs: 10, WorkflowTTLMs: 100}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := service.commitOrRecoverTheftAdmission(context.Background(), command(9))
		firstDone <- err
	}()
	<-repository.started
	secondDone := make(chan error, 1)
	go func() {
		_, err := service.commitOrRecoverTheftAdmission(context.Background(), command(8))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated Pair admission was serialized behind a blocked Pair")
	}
	close(repository.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func (f *theftAdmissionFake) CommitFriendActionAdmission(_ context.Context, command frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error) {
	f.commitCalls++
	f.lastCommand = command
	if prior, ok := f.stored[command.Identity.Key()]; ok {
		if string(prior.Outcome.PayloadHash) != string(command.PayloadHash) {
			return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrIdempotentConflict
		}
		if f.loseResponse {
			return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrRedisTimeoutUnknown
		}
		return prior, nil
	}
	if !f.active {
		outcome := frienddomain.ActionAdmissionOutcome{Outcome: frienddomain.Outcome{
			Status:      frienddomain.StatusRejected,
			Reason:      frienddomain.ReasonVisitNotPermitted,
			Identity:    command.Identity,
			PayloadHash: append([]byte(nil), command.PayloadHash...),
		}}
		f.stored[command.Identity.Key()] = outcome
		return outcome, nil
	}
	outcome := fakeTheftAdmission(command)
	f.stored[command.Identity.Key()] = outcome
	if f.loseResponse {
		return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrRedisTimeoutUnknown
	}
	return outcome, nil
}

func (f *theftAdmissionFake) GetFriendActionAdmission(_ context.Context, command frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error) {
	f.getCalls++
	prior, ok := f.stored[command.Identity.Key()]
	if !ok {
		return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrOutcomeNotFound
	}
	if string(prior.Outcome.PayloadHash) != string(command.PayloadHash) {
		return frienddomain.ActionAdmissionOutcome{}, frienddomain.ErrIdempotentConflict
	}
	return prior, nil
}

func fakeTheftAdmission(command frienddomain.ActionAdmissionCommand) frienddomain.ActionAdmissionOutcome {
	return frienddomain.ActionAdmissionOutcome{
		Outcome: frienddomain.Outcome{
			Status:        frienddomain.StatusSucceeded,
			Identity:      command.Identity,
			PayloadHash:   append([]byte(nil), command.PayloadHash...),
			CommittedAtMs: command.NowMs,
		},
		Permit: frienddomain.ActionPermitSnapshot{
			PermitID:                       "permit-" + command.Identity.OperationID,
			RelationID:                     "relation-1",
			RelationEpoch:                  7,
			VisitorPlayerID:                command.VisitorPlayerID,
			OwnerPlayerID:                  command.OwnerPlayerID,
			TargetFarmID:                   command.TargetFarmID,
			OperationID:                    command.Identity.OperationID,
			Scope:                          frienddomain.ActionScopeTheft,
			PayloadHash:                    append([]byte(nil), command.PayloadHash...),
			TargetBindingHash:              append([]byte(nil), command.TargetBindingHash...),
			IssuedAtAuthorityMs:            command.NowMs,
			ExpireAtAuthorityMs:            command.NowMs + frienddomain.DefaultActionPermitTTL.Milliseconds(),
			ExecutionAcceptedAtAuthorityMs: command.NowMs,
		},
		DispatchOrderID:      "dispatch-" + command.Identity.OperationID,
		WorkflowDeadlineAtMs: command.NowMs + command.WorkflowTTLMs,
	}
}

type theftDispatchFake struct {
	calls     []string
	returnErr error
}

func (d *theftDispatchFake) DispatchTheft(_ context.Context, orderID string) error {
	d.calls = append(d.calls, orderID)
	return d.returnErr
}

func newTheftAdmissionTestService(repo TheftAdmissionRepository, dispatch TheftDispatchPort) *Service {
	service := NewService(nil, nil)
	service.SetTheftPairRepository(repo)
	service.SetTheftDispatchPort(dispatch)
	service.now = func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	return service
}

func theftRequestForTest(t *testing.T, operationID string, plot uint32, cycle string) (*logicrpcpb.ExecuteCommandRequest, *logicrpcpb.DispatchContext) {
	t.Helper()
	command := typedCommand(t, operationID, "request-"+operationID, commonpb.CommandType_THEFT,
		&publicpb.TheftRequest{PlotId: plot, TargetFarmId: 9, CropCycleId: cycle})
	trusted, dispatch := gateSideForTest(t, 10, "connection-theft", command)
	return &logicrpcpb.ExecuteCommandRequest{Command: command, Trusted: trusted, Dispatch: dispatch}, dispatch
}

func TestTheftActionAdmissionResponseLossUsesGetAndDispatchesPersistedOrder(t *testing.T) {
	repo := &theftAdmissionFake{active: true, loseResponse: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	dispatch := &theftDispatchFake{}
	service := newTheftAdmissionTestService(repo, dispatch)
	req, _ := theftRequestForTest(t, "theft-lost-response", 0, "cycle-1")

	response, err := service.ExecuteCommand(context.Background(), req)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("response=%v err=%v", response, err)
	}
	if response.GetBusiness().RetryAfterMs != 50 {
		t.Fatalf("theft initial retry_after_ms=%d, want 50", response.GetBusiness().RetryAfterMs)
	}
	if repo.commitCalls != 1 || repo.getCalls != 1 {
		t.Fatalf("commit/get calls=%d/%d", repo.commitCalls, repo.getCalls)
	}
	if len(dispatch.calls) != 1 || dispatch.calls[0] != "dispatch-theft-lost-response" {
		t.Fatalf("dispatch calls=%v", dispatch.calls)
	}
}

func TestTheftAdmissionReplayAndPayloadConflictNeverIssuePermit(t *testing.T) {
	repo := &theftAdmissionFake{active: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	dispatch := &theftDispatchFake{}
	service := newTheftAdmissionTestService(repo, dispatch)
	first, _ := theftRequestForTest(t, "theft-replay", 0, "cycle-1")
	if response, err := service.ExecuteCommand(context.Background(), first); err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("first=%v err=%v", response, err)
	}
	replay, _ := theftRequestForTest(t, "theft-replay", 0, "cycle-1")
	if response, err := service.ExecuteCommand(context.Background(), replay); err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("replay=%v err=%v", response, err)
	}
	conflict, _ := theftRequestForTest(t, "theft-replay", 1, "cycle-1")
	response, err := service.ExecuteCommand(context.Background(), conflict)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_REJECTED ||
		response.GetBusiness().Error.GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflict=%v err=%v", response, err)
	}
	if repo.commitCalls != 3 || repo.getCalls != 0 {
		t.Fatalf("commit/get calls=%d/%d", repo.commitCalls, repo.getCalls)
	}
}

func TestTheftAdmissionBeforeAndAfterFriendDelete(t *testing.T) {
	repo := &theftAdmissionFake{active: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	dispatch := &theftDispatchFake{}
	service := newTheftAdmissionTestService(repo, dispatch)
	before, _ := theftRequestForTest(t, "theft-before-delete", 0, "cycle-1")
	if response, err := service.ExecuteCommand(context.Background(), before); err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("before delete=%v err=%v", response, err)
	}
	repo.active = false
	after, _ := theftRequestForTest(t, "theft-after-delete", 0, "cycle-1")
	response, err := service.ExecuteCommand(context.Background(), after)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_REJECTED ||
		response.GetBusiness().Error.GetErrorCode() != commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID {
		t.Fatalf("after delete=%v err=%v", response, err)
	}
	acceptedReplay, _ := theftRequestForTest(t, "theft-before-delete", 0, "cycle-1")
	response, err = service.ExecuteCommand(context.Background(), acceptedReplay)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("accepted replay after delete=%v err=%v", response, err)
	}
}

func TestTheftDispatchLossStillReturnsProcessing(t *testing.T) {
	repo := &theftAdmissionFake{active: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	dispatch := &theftDispatchFake{returnErr: errors.New("worker temporarily unavailable")}
	service := newTheftAdmissionTestService(repo, dispatch)
	req, _ := theftRequestForTest(t, "theft-dispatch-loss", 0, "cycle-1")
	response, err := service.ExecuteCommand(context.Background(), req)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("response=%v err=%v", response, err)
	}
	if len(dispatch.calls) != 1 {
		t.Fatalf("dispatch calls=%v", dispatch.calls)
	}
}

func TestTheftLegacyIsClosedByDefaultAndMissingDependenciesFailClosed(t *testing.T) {
	service := NewService(nil, nil)
	service.now = func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	req, _ := theftRequestForTest(t, "theft-legacy-off", 0, "cycle-1")
	response, err := service.ExecuteCommand(context.Background(), req)
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_UNAVAILABLE {
		t.Fatalf("legacy default response=%v err=%v", response, err)
	}
	if service.TheftIngressReady() {
		t.Fatal("missing Pair/Dispatch dependencies reported ready")
	}
}

func TestTheftRequiresCompleteSubscriptionFence(t *testing.T) {
	repo := &theftAdmissionFake{active: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	service := newTheftAdmissionTestService(repo, &theftDispatchFake{})
	req, _ := theftRequestForTest(t, "theft-fence", 0, "cycle-1")
	req.Trusted.FarmContext.AdmittedSubscriptionId = ""
	response, err := service.ExecuteCommand(context.Background(), req)
	if err == nil || response != nil || repo.commitCalls != 0 {
		t.Fatalf("incomplete fence response=%v err=%v commitCalls=%d", response, err, repo.commitCalls)
	}
}

func TestTheftResultPayloadDoesNotContainServerComputedFields(t *testing.T) {
	repo := &theftAdmissionFake{active: true, stored: make(map[string]frienddomain.ActionAdmissionOutcome)}
	service := newTheftAdmissionTestService(repo, &theftDispatchFake{})
	req, _ := theftRequestForTest(t, "theft-payload-shape", 0, "cycle-1")
	response, err := service.ExecuteCommand(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var result publicpb.TheftResult
	if err := proto.Unmarshal(response.GetBusiness().ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.TargetFarmId != 9 || result.PlotId != 0 || result.CropCycleId != "cycle-1" ||
		result.CommittedFarmVersion != 0 || result.SettlementId != "" {
		t.Fatalf("unexpected result target=%d plot=%d cycle=%q farm_version=%d settlement=%q",
			result.TargetFarmId, result.PlotId, result.CropCycleId, result.CommittedFarmVersion, result.SettlementId)
	}
}
