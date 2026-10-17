// Package ingress exposes the PlayerActor command and participant boundaries.
package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/trusted"
	playeractor "farmgame/internal/player/actor"
	playercompendium "farmgame/internal/player/compendium"
	"farmgame/internal/player/mail"
	playerpet "farmgame/internal/player/pet"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/shopcatalog"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/taskcatalog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	operationReceiptRetention = 7 * 24 * time.Hour
	maxRecentReceipts         = 1024
	maxItemReservations       = 1024
	// PlayerOperationReceipt has no frozen kind field. This namespace is an
	// internal storage key, not a public operation_id: the control-byte
	// delimiters are outside the legal printable opaque operation-id shape.
	participantReceiptNamespace = "\x00PI1-A1-PARTICIPANT-RECEIPT-V1\x00"
	// welcomeMailVersionV1 is a permanent PlayerRecord marker for the one P0
	// welcome-mail DeliveryOrder. GET_PLAYER_BOOTSTRAP is a home-page read
	// and must not attach it. HTTP EnsureOwnerInitialized writes daily login
	// facts and must not attach it, so fixture/login Ensure can replay
	// without a SourceOrder payload-hash fence. Bumping the template
	// requires a new version.
	welcomeMailVersionV1        uint32 = 1
	welcomeMailRetentionMs             = int64(30 * 24 * time.Hour / time.Millisecond)
	bootstrapMailSummaryTimeout        = 50 * time.Millisecond
	// maxTradeCount is the `max_trade_quantity` of 32 §2.2: one Buy or Sell
	// carries a single item type in a bounded quantity, never a cart.
	maxTradeCount = 999
)

// ErrParticipantReceiptCapacity is the stable admission sentinel used by the
// Participant adapter. It is intentionally distinct from the legacy domain
// error so a Participant RPC cannot leak a raw Go error as gRPC Unknown.
var ErrParticipantReceiptCapacity = errors.New("player ingress: participant receipt capacity exhausted")

type Service struct {
	logicrpcpb.UnimplementedLogicServiceServer
	logicrpcpb.UnimplementedBootstrapServiceServer
	logicrpcpb.UnimplementedPlayerParticipantServiceServer
	registry              *playeractor.Registry
	operationResultReader OperationResultLookup
	publicProfileModels   authoritystore.ModelFieldBatchReader
	// taskDomain projects GET_PLAYER_BOOTSTRAP task red-dots from the stored
	// record (28 §3.7). Without it the command fails closed and never falls
	// back to a legacy EconomyVersion view. Daily period / login facts are
	// written by HTTP EnsureOwnerInitialized and GET_TASK_LIST, not Bootstrap.
	taskDomain        *playertask.Domain
	compendiumCatalog playercompendium.CatalogProvider
	compendiumLimits  playercompendium.LimitProfile
	mailReads         mail.MailReadRepository
	mailEntries       mail.MailEntryRepository
	petCatalog        playerpet.Provider
	nowMs             func() int64
	// allowLegacyTaskClaim is a test-only compatibility seam for legacy
	// MemoryStore registries that cannot physically commit PlayerRecord and
	// terminal outcome together. Production constructors keep this disabled.
	allowLegacyTaskClaim bool
}

func NewService(registry *playeractor.Registry) *Service {
	return &Service{registry: registry, nowMs: playeractor.NowMs, compendiumLimits: playercompendium.DefaultLimitsV1(), petCatalog: playerpet.MustNewP0Repository()}
}

// NewServiceWithOperationResultReader is the composition form used by a
// player-svr that already owns the generic WorkRepository. The reader is a
// read-only port; it never claims a work lease or invokes the workflow engine.
func NewServiceWithOperationResultReader(registry *playeractor.Registry, reader OperationResultLookup) *Service {
	return &Service{registry: registry, operationResultReader: reader, nowMs: playeractor.NowMs, compendiumLimits: playercompendium.DefaultLimitsV1(), petCatalog: playerpet.MustNewP0Repository()}
}

func (s *Service) SetMailRepositories(read mail.MailReadRepository, entries mail.MailEntryRepository) error {
	if s == nil {
		return errors.New("player ingress: nil service")
	}
	if read == nil {
		return errors.New("player ingress: mail read repository is not configured")
	}
	if entries == nil {
		return errors.New("player ingress: mail entry repository is not configured")
	}
	s.mailReads = read
	s.mailEntries = entries
	return nil
}

func (s *Service) SetPetCatalogProvider(provider playerpet.Provider) error {
	if s == nil {
		return errors.New("player ingress: nil service")
	}
	if provider == nil {
		return errors.New("player ingress: pet catalog provider is not configured")
	}
	if err := playerpet.ValidateCatalog(provider.Current()); err != nil {
		return err
	}
	s.petCatalog = provider
	return nil
}

func (s *Service) SetCompendiumCatalogProvider(provider playercompendium.CatalogProvider, limits playercompendium.LimitProfile) error {
	if s == nil {
		return errors.New("player ingress: nil service")
	}
	if provider == nil {
		return errors.New("player ingress: compendium catalog provider is not configured")
	}
	if err := limits.Validate(); err != nil {
		return err
	}
	snapshot, err := provider.Snapshot()
	if err != nil {
		return err
	}
	if err := playercompendium.ValidateCatalog(snapshot, limits); err != nil {
		return err
	}
	s.compendiumCatalog = provider
	s.compendiumLimits = limits
	return nil
}

// SetOperationResultReader supplies the existing generic WorkRecord source.
// It is intentionally a small composition seam so PO1 can wire the
// repository and W31 placement resolver without changing command roots.
// SetPublicProfileModelReader enables mailbox-free, pipelined profile reads
// from the compact Player Authority model. Nil retains the legacy fallback.
func (s *Service) SetPublicProfileModelReader(reader authoritystore.ModelFieldBatchReader) {
	if s != nil {
		s.publicProfileModels = reader
	}
}

func (s *Service) SetOperationResultReader(reader OperationResultLookup) {
	if s != nil {
		s.operationResultReader = reader
	}
}

// SetOperationResultSource constructs the standard WorkRepository-backed
// reader for final composition. No durable result projection is created.
func (s *Service) SetOperationResultSource(config OperationResultReaderConfig) error {
	reader, err := NewOperationResultReader(config)
	if err != nil {
		return err
	}
	s.SetOperationResultReader(reader)
	return nil
}

// SetBootstrapTaskDomain wires the immutable TaskDomain used by
// GET_PLAYER_BOOTSTRAP (28 §3.7, PLAYER-BOOTSTRAP-AUTHORITY-A2). gameTimezone
// defaults to the frozen P0 timezone; a nil repository or an unknown timezone
// fails closed at composition time so the command never runs on a partial
// TaskDomain. nowMs is the trusted clock used for the single per-request
// capture; when nil the PlayerActor real clock is used.
func (s *Service) SetBootstrapTaskDomain(taskRepository playertask.Repository, gameTimezone string, nowMs func() int64) error {
	if s == nil {
		return errors.New("player ingress: nil service")
	}
	if taskRepository == nil {
		return errors.New("player ingress: TaskConfigRepository is not configured")
	}
	domain, err := playertask.NewDomain(taskRepository, gameTimezone)
	if err != nil {
		return err
	}
	s.taskDomain = domain
	if nowMs != nil {
		s.nowMs = nowMs
	}
	return nil
}

func (s *Service) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req == nil || req.Command == nil || req.Trusted == nil {
		return nil, status.Error(codes.InvalidArgument, "command and trusted context required")
	}
	result := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED}
	// 28 §3.6 + 29 §5:FirstHop 独立解码 raw payload、复核 TrustedContext 与
	// DispatchContext,并生成唯一权威 CanonicalPayloadHash。disabled 命令与
	// 畸形 payload 在业务接受前明确拒绝,不进入 Actor。
	verified, verifyErr := trusted.ValidateCommand(req.Command, req.Trusted, req.Dispatch)
	if verifyErr != nil {
		switch {
		case errors.Is(verifyErr, trusted.ErrCommandDisabled):
			rejectOutcome(result, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "命令已登记但尚未开放")
			return businessResponse(result), nil
		case errors.Is(verifyErr, trusted.ErrPayloadInvalid):
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, verifyErr.Error())
			return businessResponse(result), nil
		default:
			return nil, status.Error(codes.Unauthenticated, verifyErr.Error())
		}
	}
	cmd := req.Command
	if cmd.CommandType == commonpb.CommandType_GET_OPERATION_RESULT {
		request, ok := verified.Decoded.(*publicpb.GetOperationResultRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "GET_OPERATION_RESULT payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeOperationResult(ctx, req.Trusted.Session.PlayerId, request)
	}
	if cmd.CommandType == commonpb.CommandType_GET_PLAYER_BOOTSTRAP {
		if _, ok := verified.Decoded.(*publicpb.GetPlayerBootstrapRequest); !ok {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "GET_PLAYER_BOOTSTRAP payload type mismatch")
			return businessResponse(result), nil
		}
		// player_id 只允许来自 TrustedCommandContext(26 §3.7):RouteKey 由
		// catalog RouteKeyExtractor 从可信会话字段提取,payload 不携带任何主体。
		return s.executePlayerBootstrap(ctx, verified.RouteKey, result)
	}
	if cmd.CommandType == commonpb.CommandType_GET_TASK_LIST {
		if _, ok := verified.Decoded.(*publicpb.GetTaskListRequest); !ok {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "GET_TASK_LIST payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeTaskList(ctx, verified.RouteKey, result)
	}
	if cmd.CommandType == commonpb.CommandType_GET_COMPENDIUM {
		request, ok := verified.Decoded.(*publicpb.GetCompendiumRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "GET_COMPENDIUM payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeCompendiumGet(ctx, verified.RouteKey, request, result)
	}
	if cmd.CommandType == commonpb.CommandType_ACK_COMPENDIUM_SEEN {
		request, ok := verified.Decoded.(*publicpb.AckCompendiumSeenRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "ACK_COMPENDIUM_SEEN payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeCompendiumAck(ctx, verified.RouteKey, request, result)
	}
	if cmd.CommandType == commonpb.CommandType_GET_PET_STATE {
		request, ok := verified.Decoded.(*publicpb.GetPetStateRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "GET_PET_STATE payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executePetState(ctx, verified.RouteKey, result)
	}
	switch cmd.CommandType {
	case commonpb.CommandType_GET_MAIL_LIST, commonpb.CommandType_GET_MAIL_DETAIL,
		commonpb.CommandType_MARK_MAIL_READ, commonpb.CommandType_DELETE_MAIL:
		return s.executeMailCommand(ctx, req.Trusted.Session.PlayerId, cmd, verified.Decoded, result)
	case commonpb.CommandType_CLAIM_MAIL_ATTACHMENT:
		request, ok := verified.Decoded.(*publicpb.ClaimMailAttachmentRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "CLAIM_MAIL_ATTACHMENT payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeMailClaim(ctx, req.Trusted.Session.PlayerId, cmd.OperationId, verified.PayloadHash, request)
	case commonpb.CommandType_PET_UNLOCK_BY_COIN,
		commonpb.CommandType_PET_FEED,
		commonpb.CommandType_PET_SWITCH:
		return s.executePetMutationCommand(ctx, verified.RouteKey, cmd.CommandType, cmd.OperationId,
			verified.Decoded, verified.PayloadHash, result)
	}
	if cmd.CommandType == commonpb.CommandType_CLAIM_TASK {
		request, ok := verified.Decoded.(*publicpb.ClaimTaskRequest)
		if !ok || request == nil {
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, "CLAIM_TASK payload type mismatch")
			return businessResponse(result), nil
		}
		return s.executeTaskClaim(ctx, verified.RouteKey, cmd.OperationId, verified.PayloadHash, request, result)
	}
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	payloadHash := verified.PayloadHash

	value, err := s.registry.Submit(ctx, verified.RouteKey, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if receipt := findReceipt(rec, cmd.OperationId); receipt != nil {
			// 同 EffectiveIdempotencyKey + 相同权威 hash = 同一业务意图重试;
			// 不同 hash = IDEMPOTENCY_CONFLICT(28 §3.5)。
			if !bytes.Equal(receipt.PayloadHash, payloadHash) {
				return nil, false, &domainError{code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, message: "operation_id reused with different payload"}
			}
			return receipt, false, nil
		}
		switch cmd.CommandType {
		case commonpb.CommandType_BUY:
			return buy(rec, cmd.OperationId, verified.Decoded.(*publicpb.BuyRequest), payloadHash)
		case commonpb.CommandType_SELL:
			return sell(rec, cmd.OperationId, verified.Decoded.(*publicpb.SellRequest), payloadHash)
		case commonpb.CommandType_CONFIRM_FRIEND_VISIT:
			request := verified.Decoded.(*publicpb.ConfirmFriendVisitRequest)
			farmContext := req.Trusted.GetFarmContext()
			if farmContext == nil || !req.Trusted.GetRecoveryCompleteAtAdmission() ||
				farmContext.GetAdmittedFarmId() != request.GetTargetFarmId() ||
				request.GetTargetFarmId() == 0 || request.GetTargetFarmId() == verified.RouteKey {
				return nil, false, &domainError{code: commonpb.ErrorCode_ERR_NOT_IN_FARM, message: "friend visit confirmation requires the current Ready friend subscription"}
			}
			witnessID := farmContext.GetAdmittedSubscriptionId()
			if witnessID == "" {
				return nil, false, &domainError{code: commonpb.ErrorCode_ERR_NOT_IN_FARM, message: "friend visit subscription fence is missing"}
			}
			return s.confirmFriendVisit(rec, cmd.OperationId, request, payloadHash, witnessID, s.trustedNowMs())
		default:
			return nil, false, &domainError{code: commonpb.ErrorCode_ERR_UNKNOWN_COMMAND, message: "command is not routed to player-svr"}
		}
	})
	if err != nil {
		applyError(result, err)
		return businessResponse(result), nil
	}
	switch got := value.(type) {
	case *playerpb.PlayerOperationReceipt:
		result.Status = got.Status
		if got.ErrorCode != commonpb.ErrorCode_OK {
			result.Error = &logicrpcpb.ErrorDetail{ErrorCode: got.ErrorCode}
		}
		result.ResultPayload = append([]byte(nil), got.ResultPayload...)
	case *playerpb.EconomyCommandResult:
		result.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(got)
		if err != nil {
			applyError(result, err)
		}
	default:
		applyError(result, errors.New("player command returned an invalid result"))
	}
	return businessResponse(result), nil
}

// businessResponse 把业务结果包装进合同 29 §9.1 的 oneof business 分支。
func businessResponse(outcome *logicrpcpb.BusinessCommandOutcome) *logicrpcpb.ExecuteCommandResponse {
	return &logicrpcpb.ExecuteCommandResponse{
		Outcome: &logicrpcpb.ExecuteCommandResponse_Business{Business: outcome},
	}
}

func rejectOutcome(outcome *logicrpcpb.BusinessCommandOutcome, code commonpb.ErrorCode, message string) {
	outcome.Status = commonpb.CommandStatus_REJECTED
	outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: code, ErrorMessage: message}
}

type ownerLoginTurnValue struct {
	record           *playerpb.PlayerRecord
	committedVersion uint64
}

// executePlayerBootstrap 实现 28 §3.7 的 GET_PLAYER_BOOTSTRAP。
// 首页只读:用户信息 + 红点。每日登录由 HTTP EnsureOwnerInitialized 写入;
// 欢迎信/任务周期不在本命令提交。player_state_revision 取 Read 到的权威
// 版本。邮件红点是读:短超时 GetSummary,有邮件才亮;没有就空摘要。
func (s *Service) executePlayerBootstrap(ctx context.Context, playerID uint64, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if s.taskDomain == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "TaskDomain is not composed; GET_PLAYER_BOOTSTRAP is not ready")
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	rec, version, err := s.registry.ReadRoutedCurrent(ctx, playerID)
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	bootstrapResult, err := s.projectPlayerBootstrapView(rec, nowMs)
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	bootstrapResult.PlayerStateRevision = version
	s.attachBootstrapMailSummary(playerID, rec, bootstrapResult)
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(bootstrapResult)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

// projectPlayerBootstrapView builds the home-page view from the stored record.
// ListView may lazily align a local DomainState copy for red-dots; that copy
// is discarded. This command never CAS / AttachOrder.
func (s *Service) projectPlayerBootstrapView(rec *playerpb.PlayerRecord, nowMs int64) (*publicpb.GetPlayerBootstrapResult, error) {
	state, err := playertask.DomainStateFromRecord(rec)
	if err != nil {
		return nil, err
	}
	listView, err := s.taskDomain.ListView(&state, nowMs)
	if err != nil {
		return nil, err
	}
	compendiumState, err := playercompendium.StateFromPlayerRecord(rec, s.compendiumLimits)
	if err != nil {
		return nil, err
	}
	petState, err := playerpet.StateFromPlayerRecord(rec)
	if err != nil {
		return nil, err
	}
	view := buildPlayerBootstrapResult(rec, &listView, nowMs)
	view.FeatureSummary.Compendium = publicCompendiumSummary(compendiumState)
	view.FeatureSummary.Pet = publicPetSummary(petState, nowMs)
	return view, nil
}

// applyOwnerLoginFacts writes EnsureDailyPeriod / RecordLogin. HTTP Ensure
// and GET_TASK_LIST own this mutation; GET_PLAYER_BOOTSTRAP must not.
func (s *Service) applyOwnerLoginFacts(rec *playerpb.PlayerRecord, nowMs int64) (*ownerLoginTurnValue, bool, []authoritystore.AttachedOrder, error) {
	state, err := playertask.DomainStateFromRecord(rec)
	if err != nil {
		return nil, false, nil, err
	}
	changed, err := s.taskDomain.EnsureDailyPeriod(&state, nowMs)
	if err != nil {
		return nil, false, nil, err
	}
	login, err := s.taskDomain.RecordLogin(&state, nowMs)
	if err != nil {
		return nil, false, nil, err
	}
	changed = changed || login.StateChanged
	beforeListVersion := state.TaskStateVersion
	_, err = s.taskDomain.ListView(&state, nowMs)
	if err != nil {
		return nil, false, nil, err
	}
	changed = changed || state.TaskStateVersion != beforeListVersion
	if changed {
		if err := playertask.ApplyDomainStateToRecord(rec, &state); err != nil {
			return nil, false, nil, err
		}
	}
	return &ownerLoginTurnValue{record: rec}, changed, nil, nil
}

func (s *Service) ownerLoginFactsNeeded(rec *playerpb.PlayerRecord, nowMs int64) bool {
	if rec == nil {
		return true
	}
	if s == nil || s.taskDomain == nil {
		return false
	}
	state, err := playertask.DomainStateFromRecord(rec)
	if err != nil {
		return true
	}
	changed, err := s.taskDomain.EnsureDailyPeriod(&state, nowMs)
	if err != nil || changed {
		return true
	}
	login, err := s.taskDomain.RecordLogin(&state, nowMs)
	if err != nil {
		return true
	}
	return login.StateChanged
}

func (s *Service) commitOwnerLoginFacts(ctx context.Context, playerID uint64, nowMs int64) (*ownerLoginTurnValue, error) {
	result, err := s.registry.SubmitDurableCommand(ctx, playerID, func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
		value, changed, orders, turnErr := s.applyOwnerLoginFacts(rec, nowMs)
		if turnErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, turnErr
		}
		return value, changed, authoritystore.StoredOperationOutcome{
			Status: authoritystore.TerminalSucceeded,
		}, orders, nil
	})
	if errors.Is(err, playeractor.ErrParticipantReceiptAuthorityUnavailable) {
		result, err = s.registry.SubmitVersioned(ctx, playerID, func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
			value, changed, _, turnErr := s.applyOwnerLoginFacts(rec, nowMs)
			return value, changed, turnErr
		})
	}
	if err != nil {
		return nil, err
	}
	turnValue, ok := result.Value.(*ownerLoginTurnValue)
	if !ok || turnValue == nil {
		return nil, errors.New("player login facts returned an invalid result")
	}
	turnValue.committedVersion = result.CommittedRecordVersion
	return turnValue, nil
}

func (s *Service) attachBootstrapMailSummary(playerID uint64, rec *playerpb.PlayerRecord, view *publicpb.GetPlayerBootstrapResult) {
	if view == nil {
		return
	}
	if view.FeatureSummary == nil {
		view.FeatureSummary = &publicpb.PlayerFeatureSummaryView{}
	}
	view.FeatureSummary.Mail = bootstrapMailSummaryFallback(rec)
	if s == nil || s.mailReads == nil {
		return
	}
	summaryCtx, cancel := context.WithTimeout(context.Background(), bootstrapMailSummaryTimeout)
	defer cancel()
	summary, err := s.mailReads.GetSummary(summaryCtx, playerID)
	if err == nil {
		view.FeatureSummary.Mail = mailSummaryView(summary)
	}
}

func bootstrapMailSummaryFallback(rec *playerpb.PlayerRecord) *publicpb.MailboxSummaryView {
	view := &publicpb.MailboxSummaryView{}
	if rec != nil && rec.GetWelcomeMailVersion() >= welcomeMailVersionV1 {
		view.UnreadCount = 1
		view.ClaimableAttachmentCount = 1
		view.HasRedPoint = true
	}
	return view
}

// trustedNowMs captures the injected clock once per request. A malformed
// injected clock falls back to the Actor's real clock so bootstrap never
// blocks on a broken composition.
func (s *Service) trustedNowMs() int64 {
	if s != nil && s.nowMs != nil {
		if now := s.nowMs(); now > 0 {
			return now
		}
	}
	return playeractor.NowMs()
}

// executeTaskList enters the PlayerActor, writes trusted-turn login and lazy
// period alignment (home GET_PLAYER_BOOTSTRAP no longer does), and returns
// the complete fixed-size authority list. A query has no durable operation
// receipt.
func (s *Service) executeTaskList(ctx context.Context, playerID uint64, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if s.taskDomain == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "TaskDomain is not composed; GET_TASK_LIST is not ready")
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	value, err := s.registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		state, err := playertask.DomainStateFromRecord(rec)
		if err != nil {
			return nil, false, err
		}
		login, err := s.taskDomain.RecordLogin(&state, nowMs)
		if err != nil {
			return nil, false, err
		}
		view, err := s.taskDomain.ListView(&state, nowMs)
		if err != nil {
			return nil, false, err
		}
		if login.StateChanged {
			if err := playertask.ApplyDomainStateToRecord(rec, &state); err != nil {
				return nil, false, err
			}
		}
		return playertask.PublicListResult(view), login.StateChanged, nil
	})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	list, ok := value.(*publicpb.GetTaskListResult)
	if !ok {
		applyError(outcome, errors.New("task list returned an invalid result"))
		return businessResponse(outcome), nil
	}
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(list)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

func buildPlayerBootstrapResult(rec *playerpb.PlayerRecord, taskView *playertask.ListView, nowMs int64) *publicpb.GetPlayerBootstrapResult {
	displayName, _ := playeractor.PublicProfileValues(rec)
	stacks := rec.GetInventory().GetStacks()
	items := make([]*economypb.ItemStack, 0, len(stacks))
	for _, stack := range stacks {
		if stack == nil {
			continue
		}
		items = append(items, &economypb.ItemStack{ItemId: stack.GetItemId(), Count: stack.GetCount()})
	}
	featureSummary := &publicpb.PlayerFeatureSummaryView{}
	if taskView != nil {
		featureSummary.Tasks = playertask.PublicSummaryFromListView(*taskView)
	}
	return &publicpb.GetPlayerBootstrapResult{
		// PlayerStateRevision 由 executePlayerBootstrap 用 Read 版本填充。
		Profile: &publicpb.PlayerPrivateProfileView{
			PlayerId: rec.GetPlayerId(), DisplayName: displayName,
		},
		Progression: &publicpb.PlayerProgressionView{
			Level: rec.GetProgression().GetLevel(),
			Exp:   rec.GetProgression().GetExp(),
		},
		Wallet:             &publicpb.WalletView{Coin: rec.GetWallet().GetCoin()},
		Inventory:          &publicpb.InventoryView{Items: items},
		FeatureSummary:     featureSummary,
		TrustedServerNowMs: nowMs,
	}
}

func publicPetSummary(state playerpet.State, nowMs int64) *publicpb.PetSummaryView {
	summary := &publicpb.PetSummaryView{
		PetStateVersion: state.PetStateVersion,
		GuardUntilMs:    state.GuardUntilMs,
		GuardEffective:  state.GuardEffective(nowMs),
	}
	if state.ActiveGuardPetID != 0 {
		active := uint32(state.ActiveGuardPetID)
		summary.ActiveGuardPetId = &active
	}
	return summary
}

func (s *Service) executeOperationResult(ctx context.Context, subjectPlayerID uint64, request *publicpb.GetOperationResultRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	outcome := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED}
	if s == nil || s.operationResultReader == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "OperationResult reader is not composed")
		return businessResponse(outcome), nil
	}
	lookup, err := s.operationResultReader.Lookup(ctx, subjectPlayerID, request)
	if err != nil {
		switch {
		case errors.Is(err, ErrOperationResultUnsupportedCommand), errors.Is(err, ErrOperationResultReaderUnavailable):
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, err.Error())
		case errors.Is(err, ErrOperationResultInvalidRequest), errors.Is(err, ErrOperationResultMalformedOperationID), errors.Is(err, ErrOperationResultInvalidLocator):
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, err.Error())
		default:
			applyError(outcome, err)
		}
		return businessResponse(outcome), nil
	}
	if lookup == nil {
		applyError(outcome, ErrOperationResultInvalidRecord)
		return businessResponse(outcome), nil
	}
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(lookup)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

func (s *Service) claimTask(rec *playerpb.PlayerRecord, operationID string, req *publicpb.ClaimTaskRequest, payloadHash []byte, nowMs int64) (any, bool, error) {
	if s == nil || s.taskDomain == nil {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, message: "TaskDomain is not composed"}
	}
	candidate, ok := proto.Clone(rec).(*playerpb.PlayerRecord)
	if !ok || candidate == nil {
		return nil, false, errors.New("failed to clone PlayerRecord for task claim")
	}
	if err := ensureReceiptCapacity(candidate, nowMs); err != nil {
		return nil, false, err
	}
	state, err := playertask.DomainStateFromRecord(candidate)
	if err != nil {
		return nil, false, err
	}
	claim, err := s.taskDomain.Claim(&state, req.GetTaskInstanceId(), nowMs)
	if err != nil {
		return nil, false, err
	}

	commandStatus := commonpb.CommandStatus_REJECTED
	errorCode := commonpb.ErrorCode_OK
	switch claim.Status {
	case playertask.ClaimSucceeded:
		if claim.Reward.PetUnlock != nil {
			if err := s.applyTaskPetUnlock(candidate, claim.Reward.PetUnlock); err != nil {
				return nil, false, err
			}
		}
		if claim.Reward.Coin > 0 && candidate.GetWallet().GetCoin() > math.MaxInt64-claim.Reward.Coin {
			return nil, false, errors.New("task coin reward overflow")
		}
		if claim.Reward.Exp > 0 && candidate.GetProgression().GetExp() > math.MaxInt64-claim.Reward.Exp {
			return nil, false, errors.New("task exp reward overflow")
		}
		candidate.Wallet.Coin += claim.Reward.Coin
		candidate.Progression.Exp += claim.Reward.Exp
		for _, item := range claim.Reward.Items {
			if !playeractor.AddItem(candidate, item.ItemID, item.Count) {
				return nil, false, errors.New("task item reward cannot be applied")
			}
		}
		candidate.Progression.Level = int32(candidate.Progression.Exp/20) + 1
		if candidate.Progression.Level > state.HighestLevel {
			// Claim already bumped task_state_version once for this Actor turn.
			// P0's only growth metric is harvest, so recording the monotonic
			// level indicator cannot create another transition in this config.
			state.HighestLevel = candidate.Progression.Level
		}
		candidate.EconomyVersion++
		commandStatus = commonpb.CommandStatus_SUCCEEDED
	case playertask.ClaimAlreadyClaimed:
		commandStatus = commonpb.CommandStatus_NO_CHANGE
		errorCode = commonpb.ErrorCode_ERR_TASK_ALREADY_CLAIMED
	case playertask.ClaimNotCompleted:
		errorCode = commonpb.ErrorCode_ERR_TASK_NOT_CLAIMABLE
	case playertask.ClaimInstanceExpired:
		errorCode = commonpb.ErrorCode_ERR_TASK_INSTANCE_EXPIRED
	case playertask.ClaimInstanceNotFound:
		errorCode = commonpb.ErrorCode_ERR_TASK_INSTANCE_NOT_FOUND
	default:
		return nil, false, errors.New("task claim returned an unspecified terminal")
	}
	if claim.StateChanged {
		if err := playertask.ApplyDomainStateToRecord(candidate, &state); err != nil {
			return nil, false, err
		}
	}
	view, err := s.taskDomain.ListView(&state, nowMs)
	if err != nil {
		return nil, false, err
	}
	result := &publicpb.ClaimTaskResult{
		PrivateState: playeractor.PrivateState(candidate), Summary: playertask.PublicSummaryFromListView(view),
		GrantedReward: playertask.PublicReward(claim.Reward), NextGrowthTask: playertask.PublicProgress(claim.NextGrowth),
		ClaimedTaskInstanceId: req.GetTaskInstanceId(), TrustedServerNowMs: nowMs,
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, false, err
	}
	AppendOperationReceiptAt(candidate, operationID, payloadHash, payload, commandStatus, errorCode, nowMs)
	proto.Reset(rec)
	proto.Merge(rec, candidate)
	return findReceipt(rec, operationID), true, nil
}

func (s *Service) applyTaskPetUnlock(candidate *playerpb.PlayerRecord, grant *playertask.PetUnlockGrant) error {
	if candidate == nil || grant == nil {
		return errors.New("task pet unlock requires a candidate and grant")
	}
	if s == nil || s.petCatalog == nil {
		return &domainError{
			code:    commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED,
			message: "pet catalog is not composed",
		}
	}
	state, err := playerpet.StateFromPlayerRecord(candidate)
	if err != nil {
		return err
	}
	decision := playerpet.ApplyTaskUnlockGrant(state, s.petCatalog, playerpet.TaskUnlockGrant{
		PetID:                  grant.PetID,
		PetConfigVersion:       grant.PetConfigVersion,
		GrantDefinitionVersion: grant.GrantDefinitionVersion,
	})
	switch decision.Status {
	case playerpet.StatusSucceeded:
		if !decision.Changed {
			return errors.New("task pet unlock succeeded without changing state")
		}
		return playerpet.ApplyStateToPlayerRecord(candidate, decision.Candidate)
	case playerpet.StatusNoChange:
		if decision.Changed {
			return errors.New("task pet unlock no-change result changed state")
		}
		return nil
	case playerpet.StatusRejected:
		return &domainError{
			code:    commonpb.ErrorCode_ERR_INTERNAL,
			message: fmt.Sprintf("task pet unlock rejected: %s", decision.ErrorCode),
		}
	default:
		return errors.New("task pet unlock returned an unspecified terminal")
	}
}

func taskClaimTerminalStatus(statusValue commonpb.CommandStatus) (authoritystore.TerminalStatus, error) {
	switch statusValue {
	case commonpb.CommandStatus_SUCCEEDED:
		return authoritystore.TerminalSucceeded, nil
	case commonpb.CommandStatus_NO_CHANGE:
		return authoritystore.TerminalNoChange, nil
	case commonpb.CommandStatus_REJECTED:
		return authoritystore.TerminalRejected, nil
	default:
		return authoritystore.TerminalUnknown, errors.New("task claim returned an invalid terminal status")
	}
}

func (s *Service) executeTaskClaim(
	ctx context.Context,
	playerID uint64,
	operationID string,
	payloadHash []byte,
	req *publicpb.ClaimTaskRequest,
	outcome *logicrpcpb.BusinessCommandOutcome,
) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s == nil || s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	nowMs := s.trustedNowMs()
	claimTurn := func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if receipt := findReceipt(rec, operationID); receipt != nil {
			if !bytes.Equal(receipt.GetPayloadHash(), payloadHash) {
				return nil, false, &domainError{
					code:    commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT,
					message: "operation_id reused with different payload",
				}
			}
			return receipt, false, nil
		}
		return s.claimTask(rec, operationID, req, payloadHash, nowMs)
	}
	durableTurn := func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
		value, changed, err := claimTurn(rec)
		if err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
		receipt, ok := value.(*playerpb.PlayerOperationReceipt)
		if !ok || receipt == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("task claim returned an invalid operation receipt")
		}
		if !changed {
			return receipt, false, authoritystore.StoredOperationOutcome{}, nil, nil
		}
		terminal, err := taskClaimTerminalStatus(receipt.GetStatus())
		if err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
		storedErrorCode := ""
		if receipt.GetErrorCode() != commonpb.ErrorCode_OK {
			storedErrorCode = receipt.GetErrorCode().String()
		}
		return receipt, true, authoritystore.StoredOperationOutcome{
			Status:    terminal,
			ErrorCode: storedErrorCode,
			Payload:   append([]byte(nil), receipt.GetResultPayload()...),
		}, nil, nil
	}
	submit, err := s.registry.SubmitDurableCommand(ctx, playerID, durableTurn)
	if errors.Is(err, playeractor.ErrParticipantReceiptAuthorityUnavailable) && s.allowLegacyTaskClaim {
		var legacyValue any
		legacyValue, err = s.registry.Submit(ctx, playerID, claimTurn)
		if err == nil {
			submit.Value = legacyValue
		}
	}
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	receipt, ok := submit.Value.(*playerpb.PlayerOperationReceipt)
	if !ok || receipt == nil {
		applyError(outcome, errors.New("task claim returned an invalid operation receipt"))
		return businessResponse(outcome), nil
	}
	outcome.Status = receipt.GetStatus()
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK {
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: receipt.GetErrorCode()}
	}
	outcome.ResultPayload = append([]byte(nil), receipt.GetResultPayload()...)
	return businessResponse(outcome), nil
}

func buildWelcomeMailDeliveryOrder(playerID uint64, nowMs int64) (authoritystore.AttachedOrder, mail.MailEntry, error) {
	if playerID == 0 || nowMs <= 0 {
		return authoritystore.AttachedOrder{}, mail.MailEntry{}, errors.New("welcome mail requires player_id and trusted time")
	}
	canonicalPlayer := strconv.FormatUint(playerID, 10)
	deliveryID := fmt.Sprintf("WELCOME:%s:%d", canonicalPlayer, welcomeMailVersionV1)
	orderID, err := identity.SourceOrderID(
		identity.WorkTypeMailDeliveryOrder,
		identity.SourceKindPlayerActor,
		canonicalPlayer,
		deliveryID,
		uint64(welcomeMailVersionV1),
	)
	if err != nil {
		return authoritystore.AttachedOrder{}, mail.MailEntry{}, err
	}
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   playerID,
		DeliveryID:          deliveryID,
		SourceType:          mail.SourceTypeSystem,
		SourceID:            "WELCOME_V1",
		TitleSnapshot:       "欢迎来到经典农场",
		BodySnapshot:        "欢迎回来，领取这份种子开始经营你的农场吧！",
		Reward:              mail.RewardBundle{Items: []mail.ItemGrant{{ItemID: playeractor.ItemCarrotSeed, Count: 3}}},
		TemplateVersion:     "welcome-v1",
		RewardConfigVersion: "welcome-reward-v1",
		CreatedAtMs:         nowMs,
		VisibleAtMs:         nowMs,
		RetentionMs:         welcomeMailRetentionMs,
	}, mail.LimitsV1())
	if err != nil {
		return authoritystore.AttachedOrder{}, mail.MailEntry{}, err
	}
	payload, err := mail.EncodeMailEntry(entry, mail.LimitsV1())
	if err != nil {
		return authoritystore.AttachedOrder{}, mail.MailEntry{}, err
	}
	payloadHash := authoritystore.PayloadHash(sha256.Sum256(payload))
	return authoritystore.AttachedOrder{
		OrderID:         orderID,
		OrderType:       string(identity.WorkTypeMailDeliveryOrder),
		Payload:         payload,
		PayloadHash:     payloadHash,
		DueAtMs:         nowMs,
		SubjectPlayerID: playerID,
	}, entry, nil
}

func (s *Service) confirmFriendVisit(rec *playerpb.PlayerRecord, operationID string, req *publicpb.ConfirmFriendVisitRequest, payloadHash []byte, witnessID string, nowMs int64) (any, bool, error) {
	if s == nil || s.taskDomain == nil {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, message: "TaskDomain is not composed"}
	}
	candidate, ok := proto.Clone(rec).(*playerpb.PlayerRecord)
	if !ok || candidate == nil {
		return nil, false, errors.New("failed to clone PlayerRecord for friend visit")
	}
	if err := ensureReceiptCapacity(candidate, nowMs); err != nil {
		return nil, false, err
	}
	state, err := playertask.DomainStateFromRecord(candidate)
	if err != nil {
		return nil, false, err
	}
	fact, err := s.taskDomain.RecordFriendVisit(&state, witnessID, nowMs)
	if err != nil {
		return nil, false, err
	}
	if fact.StateChanged {
		if err := playertask.ApplyDomainStateToRecord(candidate, &state); err != nil {
			return nil, false, err
		}
	}
	view, err := s.taskDomain.ListView(&state, nowMs)
	if err != nil {
		return nil, false, err
	}
	result := &publicpb.ConfirmFriendVisitResult{
		Summary: playertask.PublicSummaryFromListView(view), ConfirmedTaskInstanceId: fact.InstanceID,
		TrustedServerNowMs: nowMs,
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, false, err
	}
	commandStatus := commonpb.CommandStatus_SUCCEEDED
	if fact.Status == playertask.FactNoChange {
		commandStatus = commonpb.CommandStatus_NO_CHANGE
	}
	AppendOperationReceiptAt(candidate, operationID, payloadHash, payload, commandStatus, commonpb.ErrorCode_OK, nowMs)
	proto.Reset(rec)
	proto.Merge(rec, candidate)
	return findReceipt(rec, operationID), true, nil
}

// buy 消费 FirstHop 已解码的 publicpb.BuyRequest(28 §6.1);不再解码 raw
// payload,也不读取旧信封兼容字段。
func buy(rec *playerpb.PlayerRecord, operationID string, req *publicpb.BuyRequest, payloadHash []byte) (any, bool, error) {
	offer, ok := shopcatalog.ByOfferID(req.OfferId)
	if !ok || req.Count <= 0 || req.Count > maxTradeCount {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_OFFER_NOT_FOUND, message: "offer is unavailable"}
	}
	// 32 §2.3:期望版本与权威不一致时不成交,让客户端刷新后用新 operation 重来。
	if req.ExpectedOfferVersion != offer.OfferVersion {
		return nil, false, &domainError{
			code:    commonpb.ErrorCode_ERR_OFFER_PRICE_CHANGED,
			message: "商店价格已更新,请刷新后重试",
		}
	}
	if offer.BuyPrice > math.MaxInt64/int64(maxTradeCount) {
		return nil, false, errors.New("offer price exceeds the checked-arithmetic bound")
	}
	cost := int64(req.Count) * offer.BuyPrice
	if rec.Wallet.Coin < cost {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_INSUFFICIENT_COIN, message: "金币不足"}
	}
	if err := ensureReceiptCapacity(rec, playeractor.NowMs()); err != nil {
		return nil, false, err
	}
	rec.Wallet.Coin -= cost
	if !playeractor.AddItem(rec, offer.ItemID, int64(req.Count)) {
		return nil, false, errors.New("failed to add purchased item")
	}
	if offer.Kind == shopcatalog.KindCropSeed {
		playeractor.AdvanceTask(rec, taskcatalog.EventBuyOffer, int64(req.Count), playeractor.NowMs())
	}
	rec.EconomyVersion++
	payload := &playerpb.EconomyCommandResult{
		PrivateState: playeractor.PrivateState(rec),
		CoinDelta:    -cost,
		ItemId:       offer.ItemID,
		ItemDelta:    int64(req.Count),
		OfferVersion: offer.OfferVersion,
		UnitPrice:    offer.BuyPrice,
	}
	appendReceipt(rec, operationID, payloadHash, payload)
	return payload, true, nil
}

func sell(rec *playerpb.PlayerRecord, operationID string, req *publicpb.SellRequest, payloadHash []byte) (any, bool, error) {
	definition, ok := cropcatalog.ByProduceItemID(req.ItemId)
	if !ok || req.Count <= 0 || req.Count > maxTradeCount {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, message: "item cannot be sold"}
	}
	if err := ensureReceiptCapacity(rec, playeractor.NowMs()); err != nil {
		return nil, false, err
	}
	if !playeractor.AddItem(rec, req.ItemId, -req.Count) {
		return nil, false, &domainError{code: commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, message: definition.Name + "数量不足"}
	}
	coin := req.Count * definition.SellPrice
	rec.Wallet.Coin += coin
	rec.Progression.Exp += req.Count * 2
	rec.Progression.Level = int32(rec.Progression.Exp/20) + 1
	// Selling is currently an economy fact but does not have a P0 task yet.
	rec.EconomyVersion++
	payload := &playerpb.EconomyCommandResult{
		PrivateState: playeractor.PrivateState(rec),
		CoinDelta:    coin,
		ItemId:       req.ItemId,
		ItemDelta:    -req.Count,
	}
	appendReceipt(rec, operationID, payloadHash, payload)
	return payload, true, nil
}

func appendReceipt(rec *playerpb.PlayerRecord, operationID string, payloadHash []byte, payload proto.Message) {
	nowMs := playeractor.NowMs()
	data, _ := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	AppendOperationReceiptAt(rec, operationID, payloadHash, data,
		commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, nowMs)
}

// FindOperationReceipt exposes the existing PlayerRecord receipt carrier to
// typed Participant adapters. The caller must still compare the canonical
// payload hash before treating it as a replay.
func FindOperationReceipt(rec *playerpb.PlayerRecord, operationID string) *playerpb.PlayerOperationReceipt {
	return findReceipt(rec, operationID)
}

// ParticipantReceiptOperationKey returns the explicit internal namespace key
// used in PlayerRecord.OperationId for a Participant receipt. It prevents a
// public operation_id such as "sid1_..." from being classified as a durable
// Participant archive entry. The RPC identity remains the invocationID; this
// is only the frozen proto's storage-key escape hatch because its receipt
// message cannot be extended with a kind field.
func ParticipantReceiptOperationKey(invocationID string) string {
	if invocationID == "" {
		return ""
	}
	return participantReceiptNamespace + invocationID
}

// FindParticipantOperationReceipt resolves the explicit internal Participant
// namespace key. Generic/public receipt lookup must continue to use
// FindOperationReceipt and the caller's original operation_id.
func FindParticipantOperationReceipt(rec *playerpb.PlayerRecord, invocationID string) *playerpb.PlayerOperationReceipt {
	if rec == nil || invocationID == "" {
		return nil
	}
	return findReceipt(rec, ParticipantReceiptOperationKey(invocationID))
}

// ParticipantReceiptBytes returns the nested protobuf bytes occupied by the
// PlayerRecord receipt carrier. It is deliberately not Authority receipt
// accounting: outer aggregate usage comes from the concrete Store snapshot.
func ParticipantReceiptBytes(rec *playerpb.PlayerRecord) int {
	if rec == nil {
		return 0
	}
	total := 0
	for _, receipt := range rec.GetRecentOperationReceipts() {
		if receipt != nil {
			total += proto.Size(receipt)
		}
	}
	return total
}

// ParticipantReceiptItemCount returns the number of non-nil receipt entries
// that the next Participant terminal commit would occupy. It is kept beside
// the byte accounting so the admission path cannot check only bytes while the
// authority/record item limit is already full.
func ParticipantReceiptItemCount(rec *playerpb.PlayerRecord) int {
	if rec == nil {
		return 0
	}
	return receiptCount(rec)
}

// ParticipantReceiptItemCountAfterRetention counts the detached candidate
// after applying the same retention pruning used by the Actor turn. The
// caller's live record is never modified; this is admission accounting only.
func ParticipantReceiptItemCountAfterRetention(rec *playerpb.PlayerRecord, nowMs int64) int {
	if rec == nil {
		return 0
	}
	candidate, ok := proto.Clone(rec).(*playerpb.PlayerRecord)
	if !ok {
		return 0
	}
	pruneReceiptCarrier(candidate, nowMs)
	return receiptCount(candidate)
}

// ParticipantCapacityEstimate describes only the detached nested PlayerRecord
// candidate. The concrete Authority Store separately projects the outer
// receipt/result and complete commit charge from the candidate ModelBytes.
type ParticipantCapacityEstimate struct {
	ReceiptItems       int
	ModelBytes         int
	ReceiptBytes       int
	SingleReceiptBytes int
}

// EstimateParticipantRecordCapacity builds a detached worst-case candidate
// for the next terminal Participant receipt. The actual Actor turn repeats
// this calculation against its freshly loaded record, while the admission
// copy lets the server reject an already-too-large model without entering
// business code. maxResultBytes is a command-spec bound, never a caller
// supplied size.
func EstimateParticipantRecordCapacity(rec *playerpb.PlayerRecord, invocationID string, payloadHash []byte, maxResultBytes int) (ParticipantCapacityEstimate, error) {
	return EstimateParticipantRecordCapacityAt(rec, invocationID, payloadHash, maxResultBytes, playeractor.NowMs())
}

// EstimateParticipantRecordCapacityAt is the trusted-time variant used by
// Participant admission. It constructs a detached candidate and applies the
// exact retention pruning rule before appending the worst-case terminal
// receipt, so expired public receipts do not falsely consume capacity.
func EstimateParticipantRecordCapacityAt(rec *playerpb.PlayerRecord, invocationID string, payloadHash []byte, maxResultBytes int, nowMs int64) (ParticipantCapacityEstimate, error) {
	if rec == nil || invocationID == "" || len(payloadHash) != 32 || maxResultBytes <= 0 {
		return ParticipantCapacityEstimate{}, fmt.Errorf("%w: participant capacity estimate inputs", ErrParticipantReceiptCapacity)
	}
	candidate, ok := proto.Clone(rec).(*playerpb.PlayerRecord)
	if !ok {
		return ParticipantCapacityEstimate{}, fmt.Errorf("%w: participant capacity record clone", ErrParticipantReceiptCapacity)
	}
	pruneReceiptCarrier(candidate, nowMs)
	receipt := &playerpb.PlayerOperationReceipt{
		OperationId:   ParticipantReceiptOperationKey(invocationID),
		Status:        commonpb.CommandStatus_SUCCEEDED,
		ErrorCode:     commonpb.ErrorCode_OK,
		ResultPayload: make([]byte, maxResultBytes),
		CommittedAtMs: 1,
		PayloadHash:   append([]byte(nil), payloadHash...),
	}
	candidate.RecentOperationReceipts = append(candidate.RecentOperationReceipts, receipt)
	modelBytes := len("record") + proto.Size(candidate)
	return ParticipantCapacityEstimate{
		ReceiptItems:       ParticipantReceiptItemCount(candidate),
		ModelBytes:         modelBytes,
		ReceiptBytes:       ParticipantReceiptBytes(candidate),
		SingleReceiptBytes: proto.Size(receipt),
	}, nil
}

// CheckParticipantRecordCapacity validates only the nested PlayerRecord
// candidate/carrier. Store MaxReceiptItems/MaxReceiptBytes are intentionally
// absent: they govern the outer Authority receipt collection and are checked
// by PlayerActor's concrete Store-backed capacity guard.
func CheckParticipantRecordCapacity(estimate ParticipantCapacityEstimate) error {
	if estimate.ReceiptItems <= 0 || estimate.ModelBytes <= len("record") || estimate.ReceiptBytes <= 0 ||
		estimate.SingleReceiptBytes <= 0 || estimate.SingleReceiptBytes > estimate.ReceiptBytes ||
		estimate.ReceiptBytes > estimate.ModelBytes-len("record") {
		return fmt.Errorf("%w: participant record capacity estimate is invalid", ErrParticipantReceiptCapacity)
	}
	if estimate.ReceiptItems > maxRecentReceipts {
		return fmt.Errorf("%w: participant receipt item capacity exhausted", ErrParticipantReceiptCapacity)
	}
	return nil
}

// EnsureOperationReceiptCapacity shares the bounded receipt retention guard
// with legacy player commands.
func EnsureOperationReceiptCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	return ensureReceiptCapacity(rec, nowMs)
}

// EnsureParticipantOperationReceiptCapacity applies the Participant-specific
// retention rule: old hot receipts are compacted to durable tombstones, never
// deleted. The same PlayerRecord repeated field is the P0 physical carrier;
// an archive marker is CommittedAtMs==0 while the internal namespaced
// operation key, payload_hash, result_payload, status/error and the typed
// result's record version remain.
func EnsureParticipantOperationReceiptCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	pruneReceiptCarrier(rec, nowMs)
	if receiptCount(rec) >= maxRecentReceipts {
		return fmt.Errorf("%w: %v", ErrParticipantReceiptCapacity, (&domainError{
			code:    commonpb.ErrorCode_ERR_RATE_LIMITED,
			message: "participant receipt archive capacity is full; terminal keys are retained",
		}))
	}
	return nil
}

// CompactParticipantOperationReceipts evicts only the hot timestamp/index
// aspect of old Participant receipts. It is a deterministic, durable archive
// operation, not deletion: FindParticipantOperationReceipt continues to
// resolve the invocation after process restart and after the seven-day hot
// window.
func CompactParticipantOperationReceipts(rec *playerpb.PlayerRecord, nowMs int64) {
	if rec == nil {
		return
	}
	cutoffMs := nowMs - operationReceiptRetention.Milliseconds()
	for _, receipt := range rec.RecentOperationReceipts {
		if isParticipantReceipt(receipt) && receipt.GetCommittedAtMs() != 0 && receipt.GetCommittedAtMs() < cutoffMs {
			receipt.CommittedAtMs = 0
		}
	}
}

// AppendOperationReceiptAt stores a legacy/public result in the existing
// PlayerRecord receipt carrier. Participant adapters use the namespaced
// AppendParticipantOperationReceiptAt seam below so a public operation_id
// cannot be confused with a Participant archive key.
func AppendOperationReceiptAt(rec *playerpb.PlayerRecord, operationID string, payloadHash, resultPayload []byte, statusValue commonpb.CommandStatus, errorCode commonpb.ErrorCode, nowMs int64) {
	pruneReceiptCarrier(rec, nowMs)
	rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
		OperationId:   operationID,
		Status:        statusValue,
		ErrorCode:     errorCode,
		ResultPayload: append([]byte(nil), resultPayload...),
		CommittedAtMs: nowMs,
		PayloadHash:   append([]byte(nil), payloadHash...),
	})
}

// AppendParticipantOperationReceiptAt is the Participant-only append seam.
// Unlike the legacy append path it never drops a terminal Participant key
// when compacting the seven-day hot set.
func AppendParticipantOperationReceiptAt(rec *playerpb.PlayerRecord, operationID string, payloadHash, resultPayload []byte, statusValue commonpb.CommandStatus, errorCode commonpb.ErrorCode, nowMs int64) {
	pruneReceiptCarrier(rec, nowMs)
	rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
		OperationId:   ParticipantReceiptOperationKey(operationID),
		Status:        statusValue,
		ErrorCode:     errorCode,
		ResultPayload: append([]byte(nil), resultPayload...),
		CommittedAtMs: nowMs,
		PayloadHash:   append([]byte(nil), payloadHash...),
	})
}

func ensureReceiptCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	pruneReceiptCarrier(rec, nowMs)
	if receiptCount(rec) >= maxRecentReceipts {
		return &domainError{
			code:    commonpb.ErrorCode_ERR_RATE_LIMITED,
			message: "operation receipt retention window is full; retry after older receipts expire",
		}
	}
	return nil
}

func pruneReceiptCarrier(rec *playerpb.PlayerRecord, nowMs int64) {
	if rec == nil {
		return
	}
	CompactParticipantOperationReceipts(rec, nowMs)
	cutoffMs := nowMs - operationReceiptRetention.Milliseconds()
	kept := rec.RecentOperationReceipts[:0]
	for _, receipt := range rec.RecentOperationReceipts {
		if receipt == nil {
			continue
		}
		if isParticipantReceipt(receipt) || receipt.GetCommittedAtMs() >= cutoffMs {
			kept = append(kept, receipt)
		}
	}
	rec.RecentOperationReceipts = kept
}

func receiptCount(rec *playerpb.PlayerRecord) int {
	if rec == nil {
		return 0
	}
	count := 0
	for _, receipt := range rec.RecentOperationReceipts {
		if receipt != nil {
			count++
		}
	}
	return count
}

func isParticipantReceipt(receipt *playerpb.PlayerOperationReceipt) bool {
	// Do not infer kind from the public operation_id / sid1_ StableOpaqueID
	// prefix. Public operation IDs are opaque and may legally begin with that
	// text. Only the explicit internal namespace is an archive Participant
	// marker; pruning never turns a public receipt into a durable tombstone.
	// There is intentionally no structural fallback. The frozen receipt has no
	// kind field, so a legal public operation_id with the same hash/result shape
	// is indistinguishable from an old raw Participant key. New writes use the
	// explicit namespace and old ambiguous records fail closed as public data.
	return receipt != nil && strings.HasPrefix(receipt.GetOperationId(), participantReceiptNamespace)
}

func ensureReservationCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	cutoffMs := nowMs - operationReceiptRetention.Milliseconds()
	active := 0
	for _, reservation := range rec.ItemReservations {
		if reservation == nil {
			continue
		}
		if reservation.State == playerpb.ReservationState_PREPARED || reservation.UpdatedAtMs >= cutoffMs {
			active++
		}
	}
	if active >= maxItemReservations {
		return &domainError{
			code:    commonpb.ErrorCode_ERR_RATE_LIMITED,
			message: "item reservation retention window is full; retry after older reservations expire",
		}
	}
	return nil
}

func pruneExpiredReservations(rec *playerpb.PlayerRecord, nowMs int64) {
	cutoffMs := nowMs - operationReceiptRetention.Milliseconds()
	kept := rec.ItemReservations[:0]
	for _, reservation := range rec.ItemReservations {
		if reservation != nil &&
			(reservation.State == playerpb.ReservationState_PREPARED || reservation.UpdatedAtMs >= cutoffMs) {
			kept = append(kept, reservation)
		}
	}
	rec.ItemReservations = kept
}

func findReceipt(rec *playerpb.PlayerRecord, operationID string) *playerpb.PlayerOperationReceipt {
	for i := len(rec.RecentOperationReceipts) - 1; i >= 0; i-- {
		if rec.RecentOperationReceipts[i] != nil && rec.RecentOperationReceipts[i].OperationId == operationID {
			return rec.RecentOperationReceipts[i]
		}
	}
	return nil
}

type domainError struct {
	code    commonpb.ErrorCode
	message string
}

func (e *domainError) Error() string { return e.message }

func applyError(outcome *logicrpcpb.BusinessCommandOutcome, err error) {
	outcome.Status = commonpb.CommandStatus_UNAVAILABLE
	code := commonpb.ErrorCode_ERR_INTERNAL
	var domain *domainError
	if errors.As(err, &domain) {
		// Absence from the bounded hot receipt window is not proof that the
		// original write was never accepted. Keep the client journal/polling
		// alive instead of converting an unknown outcome into a false rejection.
		if domain.code != commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND {
			outcome.Status = commonpb.CommandStatus_REJECTED
		}
		code = domain.code
	} else if errors.Is(err, blobstore.ErrCASConflict) {
		code = commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT
	} else if errors.Is(err, blobstore.ErrTimeoutUnknown) {
		outcome.Status = commonpb.CommandStatus_PROCESSING
		code = commonpb.ErrorCode_ERR_TIMEOUT
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = commonpb.ErrorCode_ERR_TIMEOUT
	} else if errors.Is(err, blobstore.ErrUnavailable) || errors.Is(err, blobstore.ErrNotFound) {
		code = commonpb.ErrorCode_ERR_STORE_UNAVAILABLE
	} else if errors.Is(err, activation.ErrActivationThrottled) || errors.Is(err, activation.ErrTooManyPendingLoads) ||
		errors.Is(err, activation.ErrLoadBackoff) || errors.Is(err, activation.ErrActorBusy) {
		code = commonpb.ErrorCode_ERR_SERVER_BUSY
	} else if errors.Is(err, ErrOperationResultRepositoryUnavailable) {
		code = commonpb.ErrorCode_ERR_STORE_UNAVAILABLE
	}
	outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: code, ErrorMessage: err.Error()}
}

func (s *Service) EnsureOwnerInitialized(ctx context.Context, req *logicrpcpb.EnsureOwnerInitializedRequest) (*logicrpcpb.EnsureOwnerInitializedResponse, error) {
	if req == nil || req.PlayerId == 0 || strings.TrimSpace(req.BootstrapId) == "" {
		return nil, status.Error(codes.InvalidArgument, "player_id and bootstrap_id required")
	}
	rec, version, err := s.registry.EnsureWithProfile(ctx, req.PlayerId, req.BootstrapId, req.DisplayName, req.AvatarId)
	if err != nil {
		return nil, ensureOwnerError(err)
	}
	var homeBootstrap *publicpb.GetPlayerBootstrapResult
	if s.taskDomain != nil {
		nowMs := s.trustedNowMs()
		if s.ownerLoginFactsNeeded(rec, nowMs) {
			turnValue, commitErr := s.commitOwnerLoginFacts(ctx, req.PlayerId, nowMs)
			if commitErr != nil {
				return nil, ensureOwnerError(commitErr)
			}
			if turnValue != nil {
				if turnValue.record != nil {
					rec = turnValue.record
				}
				if turnValue.committedVersion != 0 {
					version = turnValue.committedVersion
				}
			}
		}
		// 登录附带首页投影:直接用刚才的最终 rec/version 构建,不再 registry.Read、
		// 不再新增 CAS。投影或邮件摘要降级失败时省略可选字段,登录本身不得失败;
		// 旧 WSS 客户端仍以 GET_PLAYER_BOOTSTRAP 作为 RecoveryRequery。
		view, projectErr := s.projectPlayerBootstrapView(rec, nowMs)
		if projectErr == nil {
			view.PlayerStateRevision = version
			s.attachBootstrapMailSummary(req.PlayerId, rec, view)
			homeBootstrap = view
		}
	}
	return &logicrpcpb.EnsureOwnerInitializedResponse{
		Owner: &logicrpcpb.OwnerReady{
			OwnerId: rec.PlayerId, RecordVersion: version,
		},
		HomeBootstrap: homeBootstrap,
	}, nil
}

func ensureOwnerError(err error) error {
	if errors.Is(err, playeractor.ErrPublicProfileConflict) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, activation.ErrActivationThrottled) || errors.Is(err, activation.ErrTooManyPendingLoads) ||
		errors.Is(err, activation.ErrLoadBackoff) || errors.Is(err, activation.ErrActorBusy) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

func commandError(code commonpb.ErrorCode, format string, args ...any) error {
	return &domainError{code: code, message: fmt.Sprintf(format, args...)}
}
