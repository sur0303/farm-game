package ingress

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	taskpb "farmgame/api/gen/task"
	"farmgame/internal/cropcatalog"
	playeractor "farmgame/internal/player/actor"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/protobuf/proto"
)

type commitThenTimeoutStore struct {
	base     *blobstore.MemoryStore
	failNext atomic.Bool
}

func newCommitThenTimeoutStore() *commitThenTimeoutStore {
	return &commitThenTimeoutStore{base: blobstore.NewMemoryStore()}
}

func (s *commitThenTimeoutStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	return s.base.Get(ctx, id)
}

func (s *commitThenTimeoutStore) Insert(ctx context.Context, id uint64, data []byte) error {
	return s.base.Insert(ctx, id, data)
}

func (s *commitThenTimeoutStore) CAS(ctx context.Context, id uint64, version uint64, data []byte) error {
	if !s.failNext.CompareAndSwap(true, false) {
		return s.base.CAS(ctx, id, version, data)
	}
	if err := s.base.CAS(ctx, id, version, data); err != nil {
		return err
	}
	return blobstore.ErrTimeoutUnknown
}

// failFirstCASStore injects exactly one legacy blobstore CAS conflict before
// delegating, exercising the Registry's bounded CAS-conflict reload/retry path.
type failFirstCASStore struct {
	base     *blobstore.MemoryStore
	failNext atomic.Bool
}

func newFailFirstCASStore() *failFirstCASStore {
	return &failFirstCASStore{base: blobstore.NewMemoryStore()}
}

func (s *failFirstCASStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	return s.base.Get(ctx, id)
}

func (s *failFirstCASStore) Insert(ctx context.Context, id uint64, data []byte) error {
	return s.base.Insert(ctx, id, data)
}

func (s *failFirstCASStore) CAS(ctx context.Context, id uint64, version uint64, data []byte) error {
	if !s.failNext.CompareAndSwap(true, false) {
		return s.base.CAS(ctx, id, version, data)
	}
	return blobstore.ErrCASConflict
}

// bootstrapTestNowMs is the frozen trusted clock for bootstrap tests
// (2023-11-15 06:13:20+08). Keeping it deterministic makes the derived
// DailyPeriodID stable across test runs.
const bootstrapTestNowMs = int64(1_700_000_000_000)

func TestBuildWelcomeMailDeliveryOrderKeepsStableIdentity(t *testing.T) {
	first, entry, err := buildWelcomeMailDeliveryOrder(9, bootstrapTestNowMs)
	if err != nil {
		t.Fatal(err)
	}
	if first.OrderID == "" || first.OrderType == "" || len(first.Payload) == 0 {
		t.Fatalf("welcome order=%+v", first)
	}
	if entry.TitleSnapshot != "欢迎来到经典农场" || entry.RecipientPlayerID != 9 {
		t.Fatalf("welcome entry=%+v", entry)
	}
	second, _, err := buildWelcomeMailDeliveryOrder(9, bootstrapTestNowMs)
	if err != nil {
		t.Fatal(err)
	}
	if first.OrderID != second.OrderID {
		t.Fatalf("welcome order id must be stable: %q vs %q", first.OrderID, second.OrderID)
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	return newTestServiceWithTaskWiring(t, playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return bootstrapTestNowMs })
}

// newTestServiceWithTaskWiring builds a bootstrap-wired Service over a fresh
// legacy MemoryStore registry. nowMs is the injected trusted clock captured
// once per GET_PLAYER_BOOTSTRAP request.
func newTestServiceWithTaskWiring(t *testing.T, repository playertask.Repository, timezone string, nowMs func() int64) *Service {
	t.Helper()
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	// Legacy MemoryStore cannot physically commit PlayerRecord and terminal
	// outcome together. Existing domain tests opt into the compatibility path;
	// production Service constructors keep this disabled and fail closed.
	service.allowLegacyTaskClaim = true
	if err := service.SetBootstrapTaskDomain(repository, timezone, nowMs); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-9",
	}); err != nil {
		t.Fatal(err)
	}
	return service
}

// findDailyTask locates the daily instance of the given kind inside the frozen
// three-slot TaskDomainState.
func findDailyTask(t *testing.T, state *playertask.DomainState, kind playertask.DailyKind) *playertask.Instance {
	t.Helper()
	for i := range state.DailyTasks {
		if state.DailyTasks[i].Kind == kind {
			return &state.DailyTasks[i]
		}
	}
	t.Fatalf("daily task kind %s not present in %+v", kind, state)
	return nil
}

// playerDispatch 构造 Gate 按 Catalog 为玩家域命令生成的 DispatchContext
// (ACTOR_AFFINITY / PLAYER / session.player_id)。
func playerDispatch(playerID uint64) *logicrpcpb.DispatchContext {
	return &logicrpcpb.DispatchContext{
		RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
		ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: playerID},
	}
}

func playerCommandEnvelope(t *testing.T, operationID string, commandType commonpb.CommandType, payload proto.Message) *commonpb.ClientCommandEnvelope {
	t.Helper()
	raw, err := proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: "r-" + operationID,
		CommandType: commandType, Payload: raw,
	}
}

func (s *Service) executeForTest(t *testing.T, ctx context.Context, command *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	dispatch := playerDispatch(9)
	if command != nil && command.CommandType == commonpb.CommandType_GET_OPERATION_RESULT {
		dispatch = &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB}
	}
	response, err := s.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
		Command:  command,
		Trusted:  &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true},
		Dispatch: dispatch,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetBusiness()
}

// TestUncomposedOperationReadRejectedBeforeBusiness 钉死只读 reader 的
// fail-closed 边界,不得伪造业务成功,也不产生 Actor 副作用。
func TestUncomposedOperationReadRejectedBeforeBusiness(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	commandType := commonpb.CommandType_GET_OPERATION_RESULT
	payload := &publicpb.GetOperationResultRequest{OriginalOperationId: "buy-1"}
	outcome := service.executeForTest(t, ctx, playerCommandEnvelope(t, "uncomposed-operation-read", commandType, payload))
	if outcome.Status != commonpb.CommandStatus_REJECTED || outcome.Error == nil ||
		outcome.Error.ErrorCode != commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED {
		t.Fatalf("uncomposed %s outcome=%v", commandType, outcome)
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.RecentOperationReceipts) != 0 || record.EconomyVersion != 1 {
		t.Fatalf("disabled command mutated actor: receipts=%d economy_version=%d",
			len(record.RecentOperationReceipts), record.EconomyVersion)
	}
}

// TestGetPlayerBootstrapReturnsAuthoritativeView 钉死 28 §3.7
// (PLAYER-BOOTSTRAP-AUTHORITY-A2):Bootstrap 返回首屏只读权威视图(用户信息
// + 红点);每日登录由 Ensure 写入;player_state_revision 取 Read 版本;
// player_id 只来自 TrustedCommandContext;同日重复不写 Redis、不推进 revision。
func TestGetPlayerBootstrapReturnsAuthoritativeView(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	execute := func(command *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, command)
	}

	// 先做一次真实写(BUY),让 Bootstrap 读到被业务命令推进后的权威状态。
	buy := execute(playerCommandEnvelope(t, "buy-bootstrap", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 2, ExpectedOfferVersion: 1}))
	if buy.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("buy=%v", buy)
	}
	var bought playerpb.EconomyCommandResult
	if err := proto.Unmarshal(buy.ResultPayload, &bought); err != nil {
		t.Fatal(err)
	}

	bootstrap := execute(playerCommandEnvelope(t, "bootstrap-1", commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		&publicpb.GetPlayerBootstrapRequest{}))
	if bootstrap.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", bootstrap)
	}
	if len(bootstrap.ResultPayload) == 0 {
		t.Fatal("bootstrap result payload must not be empty")
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(bootstrap.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.Profile.GetPlayerId() != 9 {
		t.Errorf("profile.player_id must come from the trusted context, got %d", result.Profile.GetPlayerId())
	}
	if result.Profile.GetDisplayName() != playeractor.DefaultDisplayName {
		t.Errorf("profile.display_name=%q want authority default %q", result.Profile.GetDisplayName(), playeractor.DefaultDisplayName)
	}
	if result.Wallet.GetCoin() != bought.PrivateState.GetCoin() {
		t.Errorf("wallet.coin=%d want %d", result.Wallet.GetCoin(), bought.PrivateState.GetCoin())
	}
	// 28 §3.7:player_state_revision 取 Read 到的权威版本,而非 EconomyVersion。
	// Ensure 已写当日登录事实,Bootstrap 只读,记录版本停在 BUY 后的 3,
	// EconomyVersion 为 2。
	if result.PlayerStateRevision != bought.PrivateState.GetEconomyVersion()+1 {
		t.Errorf("player_state_revision=%d want committed record version %d (economy_version=%d unchanged)",
			result.PlayerStateRevision, bought.PrivateState.GetEconomyVersion()+1, bought.PrivateState.GetEconomyVersion())
	}
	// Ensure 已完成 EnsureDailyPeriod + 本周期登录一次事实:登录任务可领取。
	bootstrapRecord, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapState, err := playertask.DomainStateFromRecord(bootstrapRecord)
	if err != nil {
		t.Fatal(err)
	}
	if login := findDailyTask(t, &bootstrapState, playertask.DailyLogin); login.State != playertask.StateClaimable {
		t.Errorf("login task state=%s want CLAIMABLE after ensure", login.State)
	}
	if result.TrustedServerNowMs <= 0 {
		t.Error("trusted_server_now_ms must carry authoritative server time")
	}
	if result.Progression == nil || result.FeatureSummary == nil || result.Inventory == nil {
		t.Errorf("bootstrap view must include progression/feature_summary/inventory: %+v", &result)
	}
	if tasks := result.GetFeatureSummary().GetTasks(); tasks == nil || len(tasks.GetDailyTasks()) != playertask.DailySlotCount || tasks.GetTaskStateVersion() == 0 {
		t.Errorf("bootstrap task projection must come from TaskDomain ListView: %+v", tasks)
	}
	if compendium := result.GetFeatureSummary().GetCompendium(); compendium == nil || compendium.GetHasNewUnlock() {
		t.Errorf("bootstrap compendium red-point summary invalid: %+v", compendium)
	}
	if pet := result.GetFeatureSummary().GetPet(); pet == nil || pet.GetGuardEffective() || pet.GetActiveGuardPetId() != 0 {
		t.Errorf("bootstrap pet red-point summary invalid: %+v", pet)
	}
	seedCount := int64(0)
	for _, item := range result.Inventory.GetItems() {
		if item.GetItemId() == playeractor.ItemCarrotSeed {
			seedCount = item.GetCount()
		}
	}
	if seedCount != 2 {
		t.Errorf("inventory carrot_seed=%d want 2", seedCount)
	}

	// 只读语义:重复安全查询不得推进 revision。
	replay := execute(playerCommandEnvelope(t, "bootstrap-2", commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		&publicpb.GetPlayerBootstrapRequest{}))
	if replay.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap replay=%v", replay)
	}
	var replayResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(replay.ResultPayload, &replayResult); err != nil {
		t.Fatal(err)
	}
	if replayResult.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("read-only bootstrap advanced revision: %d -> %d",
			result.PlayerStateRevision, replayResult.PlayerStateRevision)
	}

	// Bootstrap 不得写入任何幂等 receipt(NO_DURABLE_OUTCOME):用相同
	// operation_id 发起 BUY 必须按新命令正常执行,而非命中幂等回放/冲突。
	buyAfter := execute(playerCommandEnvelope(t, "bootstrap-1", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 1, ExpectedOfferVersion: 1}))
	if buyAfter.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("buy after bootstrap must execute as a fresh command: %v", buyAfter)
	}
}

// TestGetPlayerBootstrapMissingRecordFailsClosed:未初始化的玩家记录不得被
// Bootstrap 隐式创建——记录创建唯一入口是登录期 EnsureOwnerInitialized(26 §3.7)。
// 未命中记录的 Bootstrap 必须继续 fail closed,而不是退化为建账或空视图。
func TestGetPlayerBootstrapMissingRecordFailsClosed(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return bootstrapTestNowMs }); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	request := &logicrpcpb.ExecuteCommandRequest{
		Command: playerCommandEnvelope(t, "bootstrap-missing", commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
			&publicpb.GetPlayerBootstrapRequest{}),
		Trusted:  &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 77, ConnectionId: "c"}},
		Dispatch: playerDispatch(77),
	}
	response, err := service.ExecuteCommand(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	outcome := response.GetBusiness()
	if outcome.Status != commonpb.CommandStatus_UNAVAILABLE ||
		outcome.Error == nil ||
		outcome.Error.ErrorCode != commonpb.ErrorCode_ERR_STORE_UNAVAILABLE {
		t.Fatalf("missing record must fail closed with UNAVAILABLE/ERR_STORE_UNAVAILABLE: %v", outcome)
	}
	// 再次查询必须仍然失败——证明第一次查询没有隐式建账。
	response, err = service.ExecuteCommand(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBusiness().Status != commonpb.CommandStatus_UNAVAILABLE {
		t.Fatalf("read-only bootstrap must not auto-create the record: %v", response.GetBusiness())
	}
}

// TestGetPlayerBootstrapSameDayFirstAndDuplicate 钉死首页只读:Ensure 已写入
// 当日登录事实后,Bootstrap 投影 CLAIMABLE 红点但不提交;同日重复查询不推进
// 任务/记录版本,也不写任何 receipt。
func TestGetPlayerBootstrapSameDayFirstAndDuplicate(t *testing.T) {
	var now atomic.Int64
	now.Store(bootstrapTestNowMs)
	service := newTestServiceWithTaskWiring(t, playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, now.Load)
	ctx := context.Background()
	execute := func(operationID string) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, playerCommandEnvelope(t, operationID, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	}

	first := execute("bootstrap-first")
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first bootstrap=%v", first)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.TrustedServerNowMs != bootstrapTestNowMs {
		t.Errorf("trusted_server_now_ms=%d want the single captured clock %d", result.TrustedServerNowMs, bootstrapTestNowMs)
	}
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	wantPeriod := playertask.DailyPeriodID(bootstrapTestNowMs, loc)
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetEconomyVersion() != 1 {
		t.Errorf("bootstrap must not touch economy: economy_version=%d want 1", record.GetEconomyVersion())
	}
	if len(record.GetRecentOperationReceipts()) != 0 {
		t.Errorf("bootstrap must not write receipts: %d", len(record.GetRecentOperationReceipts()))
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.DailyPeriodID != wantPeriod {
		t.Errorf("daily_period_id=%q want %q", state.DailyPeriodID, wantPeriod)
	}
	if login := findDailyTask(t, &state, playertask.DailyLogin); login.State != playertask.StateClaimable {
		t.Errorf("login state=%s want CLAIMABLE after ensure", login.State)
	}

	duplicate := execute("bootstrap-duplicate")
	if duplicate.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("duplicate bootstrap=%v", duplicate)
	}
	var duplicateResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(duplicate.ResultPayload, &duplicateResult); err != nil {
		t.Fatal(err)
	}
	if duplicateResult.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("same-day duplicate advanced revision: %d -> %d", result.PlayerStateRevision, duplicateResult.PlayerStateRevision)
	}
	after, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	afterState, err := playertask.DomainStateFromRecord(after)
	if err != nil {
		t.Fatal(err)
	}
	if afterState.TaskStateVersion != state.TaskStateVersion {
		t.Errorf("same-day duplicate advanced TaskStateVersion: %d -> %d", state.TaskStateVersion, afterState.TaskStateVersion)
	}
}

// TestGetPlayerBootstrapCrossDayDoesNotPersistPeriod 钉死首页只读:跨日
// Bootstrap 可以投影新周期红点,但不得 CAS 切换 DailyPeriod;惰性周期切换
// 留在 GET_TASK_LIST。
func TestGetPlayerBootstrapCrossDayDoesNotPersistPeriod(t *testing.T) {
	day1 := bootstrapTestNowMs
	day2 := bootstrapTestNowMs + 24*60*60*1000
	var now atomic.Int64
	now.Store(day1)
	service := newTestServiceWithTaskWiring(t, playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, now.Load)
	ctx := context.Background()
	executeBootstrap := func(operationID string) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, playerCommandEnvelope(t, operationID, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	}
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	day1Period := playertask.DailyPeriodID(day1, loc)
	day2Period := playertask.DailyPeriodID(day2, loc)
	if day1Period == day2Period {
		t.Fatalf("test days must map to distinct periods: %q", day1Period)
	}

	first := executeBootstrap("bootstrap-day1")
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("day1 bootstrap=%v", first)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}

	now.Store(day2)
	second := executeBootstrap("bootstrap-day2")
	if second.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("day2 bootstrap=%v", second)
	}
	var next publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(second.ResultPayload, &next); err != nil {
		t.Fatal(err)
	}
	if next.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("cross-day bootstrap must not persist: %d -> %d", result.PlayerStateRevision, next.PlayerStateRevision)
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.DailyPeriodID != day1Period {
		t.Errorf("daily_period_id=%q want %q after read-only day2 bootstrap", state.DailyPeriodID, day1Period)
	}

	list := service.executeForTest(t, ctx, playerCommandEnvelope(t, "task-list-day2", commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{}))
	if list.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("day2 task list=%v", list)
	}
	record, version, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if version <= next.PlayerStateRevision {
		t.Errorf("GET_TASK_LIST must persist cross-day period: bootstrap=%d list=%d", next.PlayerStateRevision, version)
	}
	state, err = playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.DailyPeriodID != day2Period {
		t.Errorf("daily_period_id=%q want %q after GET_TASK_LIST", state.DailyPeriodID, day2Period)
	}
	login := findDailyTask(t, &state, playertask.DailyLogin)
	if login.State != playertask.StateClaimable {
		t.Errorf("login state=%s want CLAIMABLE on day2", login.State)
	}
	if login.PeriodID != day2Period {
		t.Errorf("login period_id=%q want %q on day2", login.PeriodID, day2Period)
	}
}

// TestGetPlayerBootstrapDoesNotCASAfterEnsure 钉死首页只读:Ensure 已提交登录
// 事实后,Bootstrap 走 Read,注入的 CAS 冲突不得影响首页成功返回。
func TestGetPlayerBootstrapDoesNotCASAfterEnsure(t *testing.T) {
	store := newFailFirstCASStore()
	registry := playeractor.NewRegistry(store, 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	ctx := context.Background()
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return bootstrapTestNowMs }); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-9",
	}); err != nil {
		t.Fatal(err)
	}
	_, ensureVersion, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	store.failNext.Store(true)
	outcome := service.executeForTest(t, ctx, playerCommandEnvelope(t, "bootstrap-cas", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if outcome.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("read-only bootstrap=%v", outcome)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(outcome.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.PlayerStateRevision != ensureVersion {
		t.Errorf("player_state_revision=%d want ensure version %d", result.PlayerStateRevision, ensureVersion)
	}
	if store.failNext.Load() != true {
		t.Fatal("bootstrap must not consume a CAS attempt")
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if login := findDailyTask(t, &state, playertask.DailyLogin); login.State != playertask.StateClaimable {
		t.Errorf("login state=%s want CLAIMABLE after ensure", login.State)
	}
}

// TestGetPlayerBootstrapResponseLossRequeryIsStable 钉死 28 §3.7 的安全重查语义:
// 响应丢失后,客户端以同一 operation_id(RetrySafeQuery)或全新 operation_id
// 重查都必须返回同一 revision——登录一次事实不会因重查被重复提交。
func TestGetPlayerBootstrapResponseLossRequeryIsStable(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	execute := func(operationID string) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, playerCommandEnvelope(t, operationID, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	}
	first := execute("bootstrap-loss")
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first bootstrap=%v", first)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}

	sameID := execute("bootstrap-loss")
	if sameID.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("same-id requery=%v", sameID)
	}
	var sameResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(sameID.ResultPayload, &sameResult); err != nil {
		t.Fatal(err)
	}
	if sameResult.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("same-id requery advanced revision: %d -> %d", result.PlayerStateRevision, sameResult.PlayerStateRevision)
	}

	newID := execute("bootstrap-loss-2")
	if newID.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("new-id requery=%v", newID)
	}
	var newResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(newID.ResultPayload, &newResult); err != nil {
		t.Fatal(err)
	}
	if newResult.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("new-id requery advanced revision: %d -> %d", result.PlayerStateRevision, newResult.PlayerStateRevision)
	}
}

// TestGetPlayerBootstrapCapturesTrustedNowOnce 钉死 28 §3.7 的单次可信时间捕获:
// trusted_server_now_ms 与红点投影必须共用同一次注入时钟,不允许各调用一次
// 当前时间。
func TestGetPlayerBootstrapCapturesTrustedNowOnce(t *testing.T) {
	var calls atomic.Int64
	// 注入时钟:每次调用返回值单调 +1,便于区分"捕获一次"与"多次调用"。
	inject := func() int64 { return bootstrapTestNowMs + calls.Add(1) }
	service := newTestServiceWithTaskWiring(t, playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, inject)
	ctx := context.Background()
	before := calls.Load()
	outcome := service.executeForTest(t, ctx, playerCommandEnvelope(t, "bootstrap-single-now", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if outcome.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", outcome)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(outcome.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	wantCaptured := bootstrapTestNowMs + before + 1
	if result.TrustedServerNowMs != wantCaptured {
		t.Errorf("trusted_server_now_ms=%d want the single captured %d (clock invoked %d times)", result.TrustedServerNowMs, wantCaptured, calls.Load())
	}
	if got := calls.Load(); got != before+1 {
		t.Errorf("injected clock invoked %d extra times during bootstrap, want exactly 1", got-before)
	}
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	// 持久化 period 来自 Ensure;Bootstrap 只读,trusted_server_now_ms 是本请求捕获。
	// 同日 +1ms 仍落在同一 DailyPeriodID。
	if want := playertask.DailyPeriodID(result.TrustedServerNowMs, loc); state.DailyPeriodID != want {
		t.Errorf("daily_period_id=%q want %q from the same captured now", state.DailyPeriodID, want)
	}
}

// TestGetPlayerBootstrapDoesNotTimeoutOnStoreCAS 钉死首页只读:Ensure 之后
// Bootstrap 走 Read,apply-then-timeout 的 CAS 故障不得让首页变成 PROCESSING。
func TestGetPlayerBootstrapDoesNotTimeoutOnStoreCAS(t *testing.T) {
	store := newCommitThenTimeoutStore()
	registry := playeractor.NewRegistry(store, 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	ctx := context.Background()
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return bootstrapTestNowMs }); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-timeout",
	}); err != nil {
		t.Fatal(err)
	}
	_, ensureVersion, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	store.failNext.Store(true)
	first := service.executeForTest(t, ctx, playerCommandEnvelope(t, "bootstrap-committed-timeout", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("read-only bootstrap=%v", first)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.PlayerStateRevision != ensureVersion {
		t.Errorf("player_state_revision=%d want ensure version %d", result.PlayerStateRevision, ensureVersion)
	}
	if store.failNext.Load() != true {
		t.Fatal("bootstrap must not consume a CAS attempt")
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if login := findDailyTask(t, &state, playertask.DailyLogin); login.State != playertask.StateClaimable {
		t.Errorf("login state=%s want CLAIMABLE after ensure", login.State)
	}
	if len(record.GetRecentOperationReceipts()) != 0 {
		t.Errorf("bootstrap must not write receipts: %d", len(record.GetRecentOperationReceipts()))
	}
}

// TestGetPlayerBootstrapTaskChangeAdvancesRevisionWhileEconomyUnchanged 钉死
// 首页只读:Ensure 写入 TaskDomain 后 revision 停在 Ensure 版本,Bootstrap 同日
// 重复查询不推进,也不改 EconomyVersion。
func TestGetPlayerBootstrapTaskChangeAdvancesRevisionWhileEconomyUnchanged(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	execute := func(operationID string) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, playerCommandEnvelope(t, operationID, commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	}
	// Ensure 已把当日登录事实写到 v2(经济版本仍为 1)。Bootstrap 同日只读。
	first := execute("bootstrap-taskchange")
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first bootstrap=%v", first)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.PlayerStateRevision != 2 {
		t.Errorf("player_state_revision=%d want 2 after first task-domain commit", result.PlayerStateRevision)
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetEconomyVersion() != 1 {
		t.Errorf("economy_version=%d want 1 (unchanged)", record.GetEconomyVersion())
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.TaskStateVersion == 0 {
		t.Error("TaskStateVersion must advance when the task domain changes")
	}
	// 无 TaskDomain 变化(同日重复):revision 不再推进。
	second := execute("bootstrap-taskchange-2")
	if second.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("second bootstrap=%v", second)
	}
	var again publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(second.ResultPayload, &again); err != nil {
		t.Fatal(err)
	}
	if again.PlayerStateRevision != result.PlayerStateRevision {
		t.Errorf("same-day replay advanced revision: %d -> %d", result.PlayerStateRevision, again.PlayerStateRevision)
	}
}

func TestEnsureOwnerInitializedRecordsDailyLoginSoBootstrapIsNoChange(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	record, version, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("ensure login facts version=%d want 2", version)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if login := findDailyTask(t, &state, playertask.DailyLogin); login.State != playertask.StateClaimable {
		t.Fatalf("login state=%s want CLAIMABLE after EnsureOwnerInitialized", login.State)
	}
	if record.GetWelcomeMailVersion() != 0 {
		t.Fatalf("ensure must not attach welcome mail, welcome_mail_version=%d", record.GetWelcomeMailVersion())
	}
	bootstrap := service.executeForTest(t, ctx, playerCommandEnvelope(t, "bootstrap-after-ensure", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if bootstrap.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", bootstrap)
	}
	var result publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(bootstrap.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.PlayerStateRevision != version {
		t.Fatalf("bootstrap advanced revision after ensure: ensure=%d bootstrap=%d", version, result.PlayerStateRevision)
	}
	if result.GetFeatureSummary().GetMail() == nil {
		t.Fatal("bootstrap mail summary must stay present without a blocking GetSummary")
	}
}

// TestGetPlayerBootstrapFailsClosedWithoutTaskDomain 钉死 28 §3.7 的 Fail Closed
// 边界:未组合 TaskDomain 时 GET_PLAYER_BOOTSTRAP 必须明确拒绝,绝不退回
// EconomyVersion 旧视图,也不产生任何记录提交。
func TestGetPlayerBootstrapFailsClosedWithoutTaskDomain(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-9",
	}); err != nil {
		t.Fatal(err)
	}
	outcome := service.executeForTest(t, context.Background(), playerCommandEnvelope(t, "bootstrap-uncomposed", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if outcome.Status != commonpb.CommandStatus_REJECTED || outcome.Error == nil ||
		outcome.Error.ErrorCode != commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED {
		t.Fatalf("bootstrap without TaskDomain must fail closed with ERR_COMMAND_UNIMPLEMENTED: %v", outcome)
	}
	record, _, err := service.registry.Read(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetEconomyVersion() != 1 || len(record.GetRecentOperationReceipts()) != 0 {
		t.Fatalf("uncomposed bootstrap must not mutate the record: economy=%d receipts=%d",
			record.GetEconomyVersion(), len(record.GetRecentOperationReceipts()))
	}
}

// TestFirstHopRejectsMalformedAndForgedRoute 钉死 29 §5:畸形/未知字段 payload
// 与 Gate 路由重算不一致都在业务接受前 fail closed。
func TestFirstHopRejectsMalformedAndForgedRoute(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	// 未知字段:field1 varint 1 + 未知 field 15。CanonicalBytes 拒绝 → ERR_DECODE。
	unknownField := &commonpb.ClientCommandEnvelope{
		OperationId: "buy-unknown-field", RequestId: "r-buy-unknown-field",
		CommandType: commonpb.CommandType_BUY, Payload: []byte{0x08, 0x01, 0x78, 0x07},
	}
	outcome := service.executeForTest(t, ctx, unknownField)
	if outcome.Status != commonpb.CommandStatus_REJECTED || outcome.Error == nil ||
		outcome.Error.ErrorCode != commonpb.ErrorCode_ERR_DECODE {
		t.Fatalf("unknown-field payload outcome=%v", outcome)
	}
	// Gate 路由与 FirstHop 重算不一致:RPC 层 fail closed,不产生业务结果。
	forged := playerCommandEnvelope(t, "buy-forged-route", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 1, ExpectedOfferVersion: 1})
	_, err := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
		Command: forged,
		Trusted: &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true},
		Dispatch: &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 10},
		},
	})
	if err == nil {
		t.Fatal("forged actor route must fail closed at RPC layer")
	}
	record, _, readErr := service.registry.Read(ctx, 9)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if record.Wallet.GetCoin() != playeractor.DefaultInitialCoin || len(record.RecentOperationReceipts) != 0 {
		t.Fatalf("fail-closed attempts mutated actor: %+v", record.Wallet)
	}
}

func TestBuySellAndOperationIdempotency(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	execute := func(command *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, command)
	}
	buy := playerCommandEnvelope(t, "buy-1", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 2, ExpectedOfferVersion: 1})
	first := execute(buy)
	second := execute(buy)
	if first.Status != commonpb.CommandStatus_SUCCEEDED || second.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("buy statuses: %s %s", first.Status, second.Status)
	}
	var bought playerpb.EconomyCommandResult
	if err := proto.Unmarshal(second.ResultPayload, &bought); err != nil {
		t.Fatal(err)
	}
	if bought.PrivateState.Coin != 80 || inventoryCount(bought.PrivateState, playeractor.ItemCarrotSeed) != 2 {
		t.Fatalf("duplicate buy changed state: %+v", bought.PrivateState)
	}
	conflict := execute(playerCommandEnvelope(t, "buy-1", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 3, ExpectedOfferVersion: 1}))
	if conflict.Status != commonpb.CommandStatus_REJECTED || conflict.Error == nil || conflict.Error.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflicting buy=%v", conflict)
	}

	grant, err := service.GrantItem(ctx, &logicrpcpb.GrantItemRequest{PlayerId: 9, OperationId: "harvest-1", ItemId: playeractor.ItemCarrot, Count: 3, PayloadHash: []byte("harvest-1")})
	if err != nil || grant.PrivateState == nil {
		t.Fatalf("grant=%v err=%v", grant, err)
	}
	sold := execute(playerCommandEnvelope(t, "sell-1", commonpb.CommandType_SELL,
		&publicpb.SellRequest{ItemId: playeractor.ItemCarrot, Count: 1}))
	if sold.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("sell=%v", sold)
	}
	var result playerpb.EconomyCommandResult
	if err := proto.Unmarshal(sold.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if result.PrivateState.Coin != 95 || inventoryCount(result.PrivateState, playeractor.ItemCarrot) != 2 {
		t.Fatalf("sell state=%+v", result.PrivateState)
	}
}

func TestSellCountBoundaryRejectsAtomicallyAndSupportsSequentialChunks(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	executeSell := func(operationID string, count int64) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		return service.executeForTest(t, ctx, playerCommandEnvelope(t, operationID, commonpb.CommandType_SELL,
			&publicpb.SellRequest{ItemId: playeractor.ItemCarrot, Count: count}))
	}

	grant, err := service.GrantItem(ctx, &logicrpcpb.GrantItemRequest{
		PlayerId: 9, OperationId: "harvest-large-stack", ItemId: playeractor.ItemCarrot,
		Count: 1000, PayloadHash: []byte("harvest-large-stack"),
	})
	if err != nil || inventoryCount(grant.GetPrivateState(), playeractor.ItemCarrot) != 1000 {
		t.Fatalf("grant=%v err=%v", grant, err)
	}

	// The public command boundary is intentionally 999. A caller sending the
	// whole 1000 stack must receive a deterministic rejection with no partial
	// inventory/coin mutation; H5 is responsible for bounded sequential chunks.
	oversized := executeSell("sell-oversized", 1000)
	if oversized.Status != commonpb.CommandStatus_REJECTED || oversized.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM ||
		oversized.GetError().GetErrorMessage() != "item cannot be sold" {
		t.Fatalf("oversized sell=%v", oversized)
	}
	record, _, err := service.registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if got := playeractor.ItemCount(record, playeractor.ItemCarrot); got != 1000 || record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin {
		t.Fatalf("oversized sell partially mutated authority: count=%d coin=%d", got, record.GetWallet().GetCoin())
	}

	first := executeSell("sell-chunk-999", 999)
	last := executeSell("sell-chunk-1", 1)
	if first.Status != commonpb.CommandStatus_SUCCEEDED || last.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("chunk statuses first=%v last=%v", first, last)
	}
	var result playerpb.EconomyCommandResult
	if err := proto.Unmarshal(last.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	if got := inventoryCount(result.PrivateState, playeractor.ItemCarrot); got != 0 {
		t.Fatalf("remaining carrots=%d want 0", got)
	}
	definition, ok := cropcatalog.ByProduceItemID(playeractor.ItemCarrot)
	if !ok {
		t.Fatal("carrot definition missing")
	}
	wantCoin := playeractor.DefaultInitialCoin + 1000*definition.SellPrice
	if result.PrivateState.GetCoin() != wantCoin {
		t.Fatalf("coin=%d want %d", result.PrivateState.GetCoin(), wantCoin)
	}
}

func TestBuyCommittedButTimeoutReturnsProcessingThenReplayWithoutDoubleMutation(t *testing.T) {
	store := newCommitThenTimeoutStore()
	registry := playeractor.NewRegistry(store, 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	ctx := context.Background()
	if _, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-timeout",
	}); err != nil {
		t.Fatal(err)
	}

	buy := playerCommandEnvelope(t, "buy-timeout-committed", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 1, ExpectedOfferVersion: 1})
	execute := func(cmd *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
		t.Helper()
		response, executeErr := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
			Command: cmd, Trusted: &logicrpcpb.TrustedCommandContext{
				Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true,
			},
			Dispatch: playerDispatch(9),
		})
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		return response.GetBusiness()
	}

	store.failNext.Store(true)
	first := execute(buy)
	if first.Status != commonpb.CommandStatus_PROCESSING || first.Error == nil || first.Error.ErrorCode != commonpb.ErrorCode_ERR_TIMEOUT {
		t.Fatalf("ambiguous buy=%v", first)
	}
	// 弱网重试保留原 operation_id + 原语义 payload:命中同一权威 hash 的持久
	// receipt,返回原终态,不重复扣款。GET_OPERATION_RESULT 的独立 WorkRecord
	// 查询不参与这条 PlayerActor 写路径。
	replay := execute(buy)
	if replay.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("replay=%v", replay)
	}
	record, _, err := registry.Read(ctx, 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.Wallet.GetCoin() != 90 || inventoryCount(playeractor.PrivateState(record), playeractor.ItemCarrotSeed) != 1 {
		t.Fatalf("buy applied more than once: wallet=%v inventory=%v", record.Wallet, record.Inventory)
	}
	// 同 operation_id 携带不同语义 payload:权威 hash 不同 → IDEMPOTENCY_CONFLICT。
	conflict := execute(playerCommandEnvelope(t, "buy-timeout-committed", commonpb.CommandType_BUY,
		&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 2, ExpectedOfferVersion: 1}))
	if conflict.Status != commonpb.CommandStatus_REJECTED || conflict.Error == nil ||
		conflict.Error.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflicting retry=%v", conflict)
	}
}

func TestBuySupportsEveryCatalogCrop(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for _, crop := range cropcatalog.All() {
		response, err := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
			Command: playerCommandEnvelope(t, "buy-crop-"+crop.Name, commonpb.CommandType_BUY,
				&publicpb.BuyRequest{OfferId: crop.OfferID, Count: 1, ExpectedOfferVersion: 1}),
			Trusted: &logicrpcpb.TrustedCommandContext{
				Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true,
			},
			Dispatch: playerDispatch(9),
		})
		if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("buy %s response=%v err=%v", crop.Name, response, err)
		}
		var result playerpb.EconomyCommandResult
		if err := proto.Unmarshal(response.GetBusiness().ResultPayload, &result); err != nil {
			t.Fatal(err)
		}
		if result.ItemId != crop.SeedItemID || inventoryCount(result.PrivateState, crop.SeedItemID) != 1 {
			t.Fatalf("buy %s result=%v inventory=%d", crop.Name, result.ItemId, inventoryCount(result.PrivateState, crop.SeedItemID))
		}
	}
}

func TestReservationLifecycleIsIdempotent(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	rejected, err := service.ReserveItem(ctx, &logicrpcpb.ReserveItemRequest{PlayerId: 9, ReservationId: "plant-0", ItemId: playeractor.ItemCarrotSeed, Count: 1, PayloadHash: []byte("plant-0")})
	if err != nil || rejected.Status != commonpb.CommandStatus_REJECTED {
		t.Fatalf("reserve without seed=%v err=%v", rejected, err)
	}
	buyPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 1, ExpectedOfferVersion: 1})
	if response, err := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{OperationId: "buy-for-plant", RequestId: "r", CommandType: commonpb.CommandType_BUY, Payload: buyPayload},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true,
		},
		Dispatch: playerDispatch(9),
	}); err != nil || response.GetBusiness().Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("buy=%v err=%v", response, err)
	}
	reserved, err := service.ReserveItem(ctx, &logicrpcpb.ReserveItemRequest{PlayerId: 9, ReservationId: "plant-1", ItemId: playeractor.ItemCarrotSeed, Count: 1, PayloadHash: []byte("plant-1")})
	if err != nil || reserved.Status != commonpb.CommandStatus_SUCCEEDED || inventoryCount(reserved.PrivateState, playeractor.ItemCarrotSeed) != 0 {
		t.Fatalf("reserve=%v err=%v", reserved, err)
	}
	repeated, err := service.ReserveItem(ctx, &logicrpcpb.ReserveItemRequest{PlayerId: 9, ReservationId: "plant-1", ItemId: playeractor.ItemCarrotSeed, Count: 1, PayloadHash: []byte("plant-1")})
	if err != nil || repeated.Status != commonpb.CommandStatus_NO_CHANGE {
		t.Fatalf("repeat reserve=%v err=%v", repeated, err)
	}
	conflicting, err := service.ReserveItem(ctx, &logicrpcpb.ReserveItemRequest{
		PlayerId: 9, ReservationId: "plant-1", ItemId: playeractor.ItemCarrotSeed,
		Count: 1, PayloadHash: []byte("different-plant"),
	})
	if err != nil || conflicting.Status != commonpb.CommandStatus_REJECTED || conflicting.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflicting reserve=%v err=%v", conflicting, err)
	}
	committed, err := service.CommitReservation(ctx, &logicrpcpb.CommitReservationRequest{PlayerId: 9, ReservationId: "plant-1"})
	if err != nil || committed.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("commit=%v err=%v", committed, err)
	}
}

func TestHarvestTaskProgressAccruesWhileClaimTaskStaysDisabled(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for index := 0; index < 3; index++ {
		operationID := "task-harvest-" + string(rune('a'+index))
		response, err := service.ApplyReward(ctx, &logicrpcpb.ApplyRewardRequest{
			PlayerId: 9, OperationId: operationID, ItemId: playeractor.ItemCarrot, ItemCount: 1,
			ExpDelta: 1, CodexCropDefinitionId: 1, PayloadHash: []byte(operationID),
		})
		if err != nil || response.Status != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("apply harvest reward=%v err=%v", response, err)
		}
	}
	state, err := service.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil || len(state.PrivateState.Tasks) < 1 {
		t.Fatalf("private state tasks=%v err=%v", state, err)
	}
	var harvestTask *taskpb.TaskInstance
	for _, task := range state.PrivateState.Tasks {
		if task != nil && task.TaskDefinitionId == 1 {
			harvestTask = task
		}
	}
	if harvestTask == nil || harvestTask.Progress != 3 || harvestTask.State != taskpb.TaskState_CLAIMABLE {
		t.Fatalf("harvest task before claim=%v", harvestTask)
	}
	// CLAIM_TASK 只识别 TaskDomain instance_id；legacy TaskInstance 不能
	// 伪装成当前权威实例，必须留下可恢复的 NOT_FOUND 终态。
	outcome := service.executeForTest(t, ctx, playerCommandEnvelope(t, "claim-task-1", commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: "task-instance-1"}))
	if outcome.Status != commonpb.CommandStatus_REJECTED || outcome.Error == nil ||
		outcome.Error.ErrorCode != commonpb.ErrorCode_ERR_TASK_INSTANCE_NOT_FOUND {
		t.Fatalf("unknown TaskDomain claim outcome=%v", outcome)
	}
	state, err = service.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range state.PrivateState.Tasks {
		if task != nil && task.TaskDefinitionId == 1 && task.State != taskpb.TaskState_CLAIMABLE {
			t.Fatalf("disabled claim must not mutate task=%v", task)
		}
	}
}

func TestTaskListClaimReplayReclaimAndConflict(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	bootstrap := service.executeForTest(t, ctx, playerCommandEnvelope(t, "", commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		&publicpb.GetPlayerBootstrapRequest{}))
	if bootstrap.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", bootstrap)
	}

	listEnvelope := playerCommandEnvelope(t, "", commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{})
	list := service.executeForTest(t, ctx, listEnvelope)
	if list.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("task list=%v", list)
	}
	var listResult publicpb.GetTaskListResult
	if err := proto.Unmarshal(list.GetResultPayload(), &listResult); err != nil {
		t.Fatal(err)
	}
	if len(listResult.GetDailyTasks()) != playertask.DailySlotCount || listResult.GetSummary().GetClaimableCount() < 1 || listResult.GetNextResetAtMs() == 0 {
		t.Fatalf("task list result=%v", &listResult)
	}
	loginInstance := listResult.GetDailyTasks()[0].GetProgress().GetInstanceId()
	visitInstance := listResult.GetDailyTasks()[1].GetProgress().GetInstanceId()

	claimCommand := playerCommandEnvelope(t, "claim-login", commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: loginInstance})
	claimed := service.executeForTest(t, ctx, claimCommand)
	if claimed.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("claim=%v", claimed)
	}
	var claimResult publicpb.ClaimTaskResult
	if err := proto.Unmarshal(claimed.GetResultPayload(), &claimResult); err != nil {
		t.Fatal(err)
	}
	if claimResult.GetClaimedTaskInstanceId() != loginInstance || claimResult.GetGrantedReward().GetExp() != 1 ||
		claimResult.GetPrivateState().GetExp() != 1 {
		t.Fatalf("claim result=%v", &claimResult)
	}

	replay := service.executeForTest(t, ctx, claimCommand)
	if replay.GetStatus() != commonpb.CommandStatus_SUCCEEDED || !bytes.Equal(replay.GetResultPayload(), claimed.GetResultPayload()) {
		t.Fatalf("claim replay=%v", replay)
	}
	reclaim := service.executeForTest(t, ctx, playerCommandEnvelope(t, "claim-login-again", commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: loginInstance}))
	if reclaim.GetStatus() != commonpb.CommandStatus_NO_CHANGE || reclaim.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_TASK_ALREADY_CLAIMED {
		t.Fatalf("reclaim=%v", reclaim)
	}
	conflict := service.executeForTest(t, ctx, playerCommandEnvelope(t, "claim-login", commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: visitInstance}))
	if conflict.GetStatus() != commonpb.CommandStatus_REJECTED || conflict.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("claim conflict=%v", conflict)
	}
}

func TestTaskListLazyCrossDayResetAndOldInstanceExpires(t *testing.T) {
	var now atomic.Int64
	now.Store(bootstrapTestNowMs)
	service := newTestServiceWithTaskWiring(t, playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, now.Load)
	ctx := context.Background()

	getList := func() *publicpb.GetTaskListResult {
		t.Helper()
		outcome := service.executeForTest(t, ctx, playerCommandEnvelope(t, "", commonpb.CommandType_GET_TASK_LIST,
			&publicpb.GetTaskListRequest{}))
		if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("task list=%v", outcome)
		}
		var result publicpb.GetTaskListResult
		if err := proto.Unmarshal(outcome.GetResultPayload(), &result); err != nil {
			t.Fatal(err)
		}
		return &result
	}

	dayOne := getList()
	oldPeriod := dayOne.GetSummary().GetDailyPeriodId()
	oldLogin := dayOne.GetDailyTasks()[0].GetProgress().GetInstanceId()
	oldVersion := dayOne.GetSummary().GetTaskStateVersion()
	now.Add((24 * time.Hour).Milliseconds())
	dayTwo := getList()
	newLogin := dayTwo.GetDailyTasks()[0].GetProgress().GetInstanceId()
	if dayTwo.GetSummary().GetDailyPeriodId() == oldPeriod || newLogin == oldLogin ||
		dayTwo.GetSummary().GetTaskStateVersion() <= oldVersion ||
		dayTwo.GetDailyTasks()[0].GetProgress().GetState() != publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMABLE {
		t.Fatalf("lazy reset day1=%v day2=%v", dayOne.GetSummary(), dayTwo.GetSummary())
	}

	oldClaim := playerCommandEnvelope(t, "claim-expired-login", commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: oldLogin})
	expired := service.executeForTest(t, ctx, oldClaim)
	if expired.GetStatus() != commonpb.CommandStatus_REJECTED ||
		expired.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_TASK_INSTANCE_EXPIRED {
		t.Fatalf("old period claim=%v", expired)
	}
	// A lost terminal response is recovered by replaying the exact operation;
	// the stored rejection bytes are returned and the new period stays intact.
	replay := service.executeForTest(t, ctx, oldClaim)
	if replay.GetStatus() != commonpb.CommandStatus_REJECTED ||
		replay.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_TASK_INSTANCE_EXPIRED ||
		!bytes.Equal(replay.GetResultPayload(), expired.GetResultPayload()) {
		t.Fatalf("expired claim replay=%v", replay)
	}
	final := getList()
	if final.GetDailyTasks()[0].GetProgress().GetInstanceId() != newLogin ||
		final.GetDailyTasks()[0].GetProgress().GetState() != publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMABLE {
		t.Fatalf("expired claim mutated current period=%v", final)
	}
}

func TestConfirmFriendVisitRequiresReadyFriendFenceAndPersistsNoChange(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if got := service.executeForTest(t, ctx, playerCommandEnvelope(t, "", commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		&publicpb.GetPlayerBootstrapRequest{})); got.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", got)
	}
	execute := func(operationID string, target uint64, farmContext *logicrpcpb.FarmAdmissionContext) *logicrpcpb.BusinessCommandOutcome {
		command := playerCommandEnvelope(t, operationID, commonpb.CommandType_CONFIRM_FRIEND_VISIT,
			&publicpb.ConfirmFriendVisitRequest{TargetFarmId: target})
		response, err := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
			Command: command,
			Trusted: &logicrpcpb.TrustedCommandContext{
				Session: &logicrpcpb.SessionPermitSnapshot{
					PlayerId: 9, ConnectionId: "c", ConnectionGeneration: 1,
					GatewayId: "gate-a", GatewayBootId: "boot-a", ActiveSessionEpoch: 1, SessionRouteRevision: 1,
				},
				RecoveryCompleteAtAdmission: true, FarmContext: farmContext,
			},
			Dispatch: playerDispatch(9),
		})
		if err != nil {
			t.Fatal(err)
		}
		return response.GetBusiness()
	}
	friendFence := &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: 10, AdmittedSubscriptionId: "sub-ready-friend"}
	confirmed := execute("visit-1", 10, friendFence)
	if confirmed.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("confirm=%v", confirmed)
	}
	var result publicpb.ConfirmFriendVisitResult
	if err := proto.Unmarshal(confirmed.GetResultPayload(), &result); err != nil {
		t.Fatal(err)
	}
	if result.GetConfirmedTaskInstanceId() == "" || result.GetSummary().GetClaimableCount() < 2 {
		t.Fatalf("confirm result=%v", &result)
	}
	again := execute("visit-2", 10, friendFence)
	if again.GetStatus() != commonpb.CommandStatus_NO_CHANGE || !bytes.Equal(again.GetResultPayload(), confirmed.GetResultPayload()) {
		// The payload is rebuilt at a new operation but authority summary and
		// confirmed instance are identical; deterministic bytes must match.
		t.Fatalf("second confirm=%v", again)
	}
}

func TestGrantReceiptRejectsDifferentPayload(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	first, err := service.GrantItem(ctx, &logicrpcpb.GrantItemRequest{
		PlayerId: 9, OperationId: "grant-1", ItemId: playeractor.ItemCarrot,
		Count: 1, PayloadHash: []byte("grant-one"),
	})
	if err != nil || first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first grant=%v err=%v", first, err)
	}
	conflict, err := service.GrantItem(ctx, &logicrpcpb.GrantItemRequest{
		PlayerId: 9, OperationId: "grant-1", ItemId: playeractor.ItemCarrot,
		Count: 2, PayloadHash: []byte("grant-two"),
	})
	if err != nil || conflict.Status != commonpb.CommandStatus_REJECTED || conflict.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflicting grant=%v err=%v", conflict, err)
	}
	state, err := service.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil || inventoryCount(state.PrivateState, playeractor.ItemCarrot) != 1 {
		t.Fatalf("state after conflict=%v err=%v", state, err)
	}
}

func TestApplyRewardAtomicallyUpdatesAllEconomyFields(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	request := &logicrpcpb.ApplyRewardRequest{
		PlayerId: 9, OperationId: "care-reward",
		ItemId: playeractor.ItemCarrot, ItemCount: 1,
		CoinDelta: 2, ExpDelta: 3, CodexCropDefinitionId: 1,
		PayloadHash: []byte("care-reward-payload"),
	}
	first, err := service.ApplyReward(ctx, request)
	if err != nil || first.Status != commonpb.CommandStatus_SUCCEEDED ||
		first.PrivateState.Coin != 102 || first.PrivateState.Exp != 3 ||
		inventoryCount(first.PrivateState, playeractor.ItemCarrot) != 1 ||
		len(first.PrivateState.CodexEntries) != 1 ||
		first.PrivateState.CodexEntries[0].CropDefinitionId != 1 ||
		first.PrivateState.CodexEntries[0].SourceOperationId != request.OperationId {
		t.Fatalf("first reward=%v err=%v", first, err)
	}
	replayed, err := service.ApplyReward(ctx, request)
	if err != nil || replayed.Status != commonpb.CommandStatus_NO_CHANGE ||
		replayed.PrivateState.EconomyVersion != first.PrivateState.EconomyVersion ||
		len(replayed.PrivateState.CodexEntries) != 1 {
		t.Fatalf("reward replay=%v err=%v", replayed, err)
	}
	conflict := proto.Clone(request).(*logicrpcpb.ApplyRewardRequest)
	conflict.PayloadHash = []byte("different-reward")
	conflict.CoinDelta = 99
	rejected, err := service.ApplyReward(ctx, conflict)
	if err != nil || rejected.Status != commonpb.CommandStatus_REJECTED ||
		rejected.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("reward conflict=%v err=%v", rejected, err)
	}
	state, err := service.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil || state.PrivateState.Coin != 102 ||
		inventoryCount(state.PrivateState, playeractor.ItemCarrot) != 1 ||
		len(state.PrivateState.CodexEntries) != 1 {
		t.Fatalf("state after conflict=%v err=%v", state, err)
	}
}

func inventoryCount(state *playerpb.PrivateState, itemID int32) int64 {
	for _, item := range state.Inventory {
		if item != nil && item.ItemId == itemID {
			return item.Count
		}
	}
	return 0
}

func TestMissingOperationResultRemainsUnknown(t *testing.T) {
	result := &logicrpcpb.BusinessCommandOutcome{}
	applyError(result, &domainError{
		code:    commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND,
		message: "operation result not found",
	})
	if result.Status != commonpb.CommandStatus_UNAVAILABLE {
		t.Fatalf("missing historical result status=%v, want UNAVAILABLE", result.Status)
	}
}

func TestPlayerTimeoutUnknownReturnsProcessing(t *testing.T) {
	result := &logicrpcpb.BusinessCommandOutcome{}
	applyError(result, blobstore.ErrTimeoutUnknown)
	if result.Status != commonpb.CommandStatus_PROCESSING || result.Error.ErrorCode != commonpb.ErrorCode_ERR_TIMEOUT {
		t.Fatalf("timeout unknown result=%v", result)
	}
}

func TestReceiptCapacityDoesNotSilentlyEvictLiveIdempotencyKeys(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	rec := &playerpb.PlayerRecord{}
	for i := 0; i < maxRecentReceipts; i++ {
		rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
			OperationId: "live", CommittedAtMs: nowMs,
		})
	}
	err := ensureReceiptCapacity(rec, nowMs)
	var domain *domainError
	if !errors.As(err, &domain) || domain.code != commonpb.ErrorCode_ERR_RATE_LIMITED {
		t.Fatalf("full live receipt window err=%v", err)
	}

	rec.RecentOperationReceipts[0].CommittedAtMs = nowMs - operationReceiptRetention.Milliseconds() - 1
	if err := ensureReceiptCapacity(rec, nowMs); err != nil {
		t.Fatalf("expired receipt should free capacity: %v", err)
	}
}

func TestReservationCapacityKeepsPreparedAndPrunesOnlyExpiredTerminalEntries(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	rec := &playerpb.PlayerRecord{}
	for i := 0; i < maxItemReservations; i++ {
		rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
			ReservationId: "prepared", State: playerpb.ReservationState_PREPARED,
			UpdatedAtMs: nowMs - operationReceiptRetention.Milliseconds() - 1,
		})
	}
	err := ensureReservationCapacity(rec, nowMs)
	var domain *domainError
	if !errors.As(err, &domain) || domain.code != commonpb.ErrorCode_ERR_RATE_LIMITED {
		t.Fatalf("prepared reservations must retain capacity: %v", err)
	}

	rec.ItemReservations[0].State = playerpb.ReservationState_COMMITTED
	if err := ensureReservationCapacity(rec, nowMs); err != nil {
		t.Fatalf("expired terminal reservation should free capacity: %v", err)
	}
	pruneExpiredReservations(rec, nowMs)
	if len(rec.ItemReservations) != maxItemReservations-1 {
		t.Fatalf("reservation prune size=%d", len(rec.ItemReservations))
	}
}

func TestPublicSid1OperationIsNotParticipantArchive(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	oldMs := nowMs - operationReceiptRetention.Milliseconds() - 1
	rec := &playerpb.PlayerRecord{}
	AppendOperationReceiptAt(rec, "sid1_public_operation_id", []byte("public-hash"), []byte("public-result"), commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, oldMs)
	participantInvocation := "sid1_participant_invocation"
	AppendParticipantOperationReceiptAt(rec, participantInvocation, make([]byte, 32), []byte("participant-result"), commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, oldMs)
	legacyInvocation := "sid1_legacy_participant_invocation"
	legacyResult, err := proto.Marshal(&contractspb.ReserveSeedResult{PlayerRecordVersion: 9})
	if err != nil {
		t.Fatal(err)
	}
	AppendOperationReceiptAt(rec, legacyInvocation, make([]byte, 32), legacyResult, commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, oldMs)

	CompactParticipantOperationReceipts(rec, nowMs)
	public := FindOperationReceipt(rec, "sid1_public_operation_id")
	if public == nil || public.GetCommittedAtMs() != oldMs {
		t.Fatalf("public sid1 receipt was classified as Participant archive: %+v", public)
	}
	participant := FindParticipantOperationReceipt(rec, participantInvocation)
	if participant == nil || participant.GetCommittedAtMs() != 0 {
		t.Fatalf("Participant receipt was not archived explicitly: %+v", participant)
	}
	// A raw legacy-looking entry is deliberately not recoverable as a
	// Participant receipt: the frozen proto has no kind field, so accepting it
	// would let a legal public operation_id collide with Participant identity.
	if legacy := FindParticipantOperationReceipt(rec, legacyInvocation); legacy != nil {
		t.Fatalf("ambiguous raw receipt was classified as Participant: %+v", legacy)
	}
	// The public receipt may later leave the ordinary hot window, but it must
	// never be retained solely because its caller-chosen operation_id starts
	// with the StableOpaqueID display prefix.
	pruneReceiptCarrier(rec, nowMs)
	if FindOperationReceipt(rec, "sid1_public_operation_id") != nil {
		t.Fatal("expired public receipt was misclassified as durable Participant receipt")
	}
	if FindOperationReceipt(rec, legacyInvocation) != nil {
		t.Fatal("ambiguous raw receipt was retained by Participant compaction")
	}
}

func TestParticipantRecordCapacityBoundaries(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	rec := &playerpb.PlayerRecord{}
	hash := make([]byte, 32)
	estimate, err := EstimateParticipantRecordCapacity(rec, "boundary-invocation", hash, 128)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckParticipantRecordCapacity(estimate); err != nil {
		t.Fatalf("valid nested record estimate rejected: %v", err)
	}
	invalid := estimate
	invalid.ReceiptItems = 1025
	if err := CheckParticipantRecordCapacity(invalid); !errors.Is(err, ErrParticipantReceiptCapacity) {
		t.Fatalf("nested receipt item overrun err=%v", err)
	}
	invalid = estimate
	invalid.SingleReceiptBytes = invalid.ReceiptBytes + 1
	if err := CheckParticipantRecordCapacity(invalid); !errors.Is(err, ErrParticipantReceiptCapacity) {
		t.Fatalf("inconsistent nested receipt accounting err=%v", err)
	}
	if ParticipantReceiptItemCount(rec) != 0 {
		t.Fatalf("detached capacity estimate mutated source record at %d", nowMs)
	}
}

// TestEnsureOwnerInitializedAttachesHomeBootstrap asserts the login response
// carries the same home projection as GET_PLAYER_BOOTSTRAP, keyed to the final
// committed version, without a second registry.Read or extra CAS.
func TestEnsureOwnerInitializedAttachesHomeBootstrap(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	resp, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-9",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Owner == nil {
		t.Fatal("owner missing")
	}
	view := resp.GetHomeBootstrap()
	if view == nil {
		t.Fatal("home_bootstrap must be attached by the login Ensure path")
	}
	if view.PlayerStateRevision != resp.Owner.RecordVersion {
		t.Fatalf("projection revision=%d must equal final Ensure version=%d", view.PlayerStateRevision, resp.Owner.RecordVersion)
	}
	if view.GetProfile().GetPlayerId() != resp.Owner.OwnerId {
		t.Fatalf("projection profile player=%d want owner=%d", view.GetProfile().GetPlayerId(), resp.Owner.OwnerId)
	}
	features := view.GetFeatureSummary()
	if features == nil || features.GetTasks() == nil {
		t.Fatal("task red-point summary missing from login projection")
	}
	if features.GetMail() == nil {
		t.Fatal("mail summary must be attached (fail-soft fallback at minimum)")
	}
	// The projection must equal the RecoveryRequery command's view for the same
	// player: identical content, identical revision.
	bootstrap := service.executeForTest(t, ctx, playerCommandEnvelope(t, "bootstrap-compare", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{}))
	if bootstrap.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%v", bootstrap)
	}
	var commandView publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(bootstrap.ResultPayload, &commandView); err != nil {
		t.Fatal(err)
	}
	if commandView.PlayerStateRevision != view.PlayerStateRevision {
		t.Fatalf("command view revision=%d login view revision=%d", commandView.PlayerStateRevision, view.PlayerStateRevision)
	}
	if !proto.Equal(&commandView, view) {
		t.Fatalf("login-attached projection differs from GET_PLAYER_BOOTSTRAP:\ncommand=%v\nlogin=%v", &commandView, view)
	}
}

// TestEnsureOwnerInitializedOmitsProjectionWithoutTaskDomain pins the
// degradation contract: without a TaskDomain the login succeeds and simply
// omits home_bootstrap, so old WSS clients fall back to GET_PLAYER_BOOTSTRAP.
func TestEnsureOwnerInitializedOmitsProjectionWithoutTaskDomain(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	resp, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "bootstrap-9",
	})
	if err != nil {
		t.Fatalf("login must not fail without TaskDomain: %v", err)
	}
	if resp.Owner == nil || resp.Owner.OwnerId != 9 {
		t.Fatalf("owner=%v", resp.Owner)
	}
	if resp.GetHomeBootstrap() != nil {
		t.Fatal("home_bootstrap must be omitted when the projection is unavailable")
	}
}
