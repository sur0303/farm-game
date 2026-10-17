package ingress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Service) petCatalogSnapshot() (playerpet.CatalogSnapshot, error) {
	if s == nil || s.petCatalog == nil {
		return playerpet.CatalogSnapshot{}, &domainError{
			code:    commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED,
			message: "pet catalog is not composed",
		}
	}
	snapshot := s.petCatalog.Current()
	if err := playerpet.ValidateCatalog(snapshot); err != nil {
		return playerpet.CatalogSnapshot{}, fmt.Errorf("pet catalog is invalid: %w", err)
	}
	return snapshot, nil
}

func (s *Service) executePetState(ctx context.Context, playerID uint64, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s == nil || s.registry == nil {
		return nil, statusErrorUnavailable("player actor registry unavailable")
	}
	catalog, err := s.petCatalogSnapshot()
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	value, err := s.registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		state, err := playerpet.StateFromPlayerRecord(rec)
		if err != nil {
			return nil, false, err
		}
		return &publicpb.GetPetStateResult{
			Page: buildPetPage(catalog, state, nowMs),
		}, false, nil
	})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	result, ok := value.(*publicpb.GetPetStateResult)
	if !ok || result == nil {
		applyError(outcome, errors.New("pet state returned an invalid result"))
		return businessResponse(outcome), nil
	}
	outcome.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		applyError(outcome, err)
	}
	return businessResponse(outcome), nil
}

func (s *Service) executePetMutationCommand(
	ctx context.Context,
	playerID uint64,
	commandType commonpb.CommandType,
	operationID string,
	decoded proto.Message,
	payloadHash []byte,
	outcome *logicrpcpb.BusinessCommandOutcome,
) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s == nil || s.registry == nil {
		return nil, statusErrorUnavailable("player actor registry unavailable")
	}
	catalog, err := s.petCatalogSnapshot()
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	result, err := s.registry.SubmitDurableCommand(ctx, playerID,
		func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
			if receipt := findReceipt(rec, operationID); receipt != nil {
				if !bytes.Equal(receipt.GetPayloadHash(), payloadHash) {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil,
						&domainError{code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, message: "operation_id reused with different payload"}
				}
				orders, rebuildErr := s.rebuildPetGuardSyncOrders(rec, playerID, commandType, operationID, receipt)
				return receipt, false, authoritystore.StoredOperationOutcome{}, orders, rebuildErr
			}
			return s.petMutationTurn(rec, playerID, commandType, operationID, decoded, payloadHash, catalog, nowMs)
		})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	receipt, ok := result.Value.(*playerpb.PlayerOperationReceipt)
	if !ok || receipt == nil {
		applyError(outcome, errors.New("pet command returned an invalid operation receipt"))
		return businessResponse(outcome), nil
	}
	if s.bestEffortGuardSync != nil {
		for _, order := range result.ProducedOrders {
			if err := s.bestEffortGuardSync.Dispatch(ctx, order); err != nil {
				// The Player mutation and receipt already exist in resident memory.
				// UNAVAILABLE tells the client to retry the same operation_id; the
				// replay path above rebuilds the same GuardSync order from current
				// Player state and repairs the Farm snapshot without a Redis worker.
				outcome.Status = commonpb.CommandStatus_UNAVAILABLE
				outcome.Error = &logicrpcpb.ErrorDetail{
					ErrorCode:    commonpb.ErrorCode_ERR_STORE_UNAVAILABLE,
					ErrorMessage: "pet mutation accepted in memory; guard synchronization requires same-operation retry: " + err.Error(),
				}
				return businessResponse(outcome), nil
			}
		}
	}
	outcome.Status = receipt.GetStatus()
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK {
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: receipt.GetErrorCode()}
	}
	outcome.ResultPayload = append([]byte(nil), receipt.GetResultPayload()...)
	return businessResponse(outcome), nil
}

func (s *Service) petMutationTurn(
	rec *playerpb.PlayerRecord,
	playerID uint64,
	commandType commonpb.CommandType,
	operationID string,
	decoded proto.Message,
	payloadHash []byte,
	catalog playerpet.CatalogSnapshot,
	nowMs int64,
) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
	state, err := playerpet.StateFromPlayerRecord(rec)
	if err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}

	var (
		status     playerpet.ActionStatus
		petError   playerpet.ErrorCode
		candidate  = state
		coinDelta  int64
		itemID     int32
		itemDelta  int64
		guardOrder *authoritystore.AttachedOrder
		result     proto.Message
	)
	switch commandType {
	case commonpb.CommandType_PET_UNLOCK_BY_COIN:
		request, ok := decoded.(*publicpb.PetUnlockByCoinRequest)
		if !ok || request == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("PET_UNLOCK_BY_COIN payload type mismatch")
		}
		decision := playerpet.UnlockByCoin(state, catalog, playerpet.UnlockByCoinRequest{
			OperationID:              operationID,
			PetID:                    int32(request.GetPetId()),
			ExpectedPetConfigVersion: request.GetExpectedPetConfigVersion(),
			ExpectedPriceVersion:     request.GetExpectedPriceVersion(),
		}, rec.GetWallet().GetCoin())
		status, petError, candidate = decision.Status, decision.ErrorCode, state
		if status == playerpet.StatusSucceeded {
			candidate = decision.Candidate
			coinDelta = -decision.Fee.Amount
			if rec.GetWallet() == nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("wallet is missing for pet unlock")
			}
			rec.Wallet.Coin -= decision.Fee.Amount
			rec.EconomyVersion++
		}
	case commonpb.CommandType_PET_FEED:
		request, ok := decoded.(*publicpb.PetFeedRequest)
		if !ok || request == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("PET_FEED payload type mismatch")
		}
		itemID = catalog.FeedRule.DogFoodItemID
		decision := playerpet.Feed(state, catalog, playerpet.FeedRequest{
			OperationID:                    operationID,
			Quantity:                       int64(request.GetQuantity()),
			ExpectedFeedRuleVersion:        request.GetExpectedFeedRuleVersion(),
			ExpectedActivePetConfigVersion: request.GetExpectedActivePetConfigVersion(),
			ExpectedGuardRuleVersion:       request.GetExpectedGuardRuleVersion(),
		}, nowMs, playeractor.ItemCount(rec, itemID))
		status, petError, candidate = decision.Status, decision.ErrorCode, state
		if status == playerpet.StatusSucceeded {
			candidate = decision.Candidate
			itemDelta = -decision.Consumption.Quantity
			if !playeractor.AddItem(rec, decision.Consumption.ItemID, -decision.Consumption.Quantity) {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("dog food could not be consumed")
			}
			rec.EconomyVersion++
			order, orderErr := buildPetGuardSyncOrder(playerID, operationID, nowMs, decision.GuardSync)
			err = orderErr
			if err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			guardOrder = &order
		}
	case commonpb.CommandType_PET_SWITCH:
		request, ok := decoded.(*publicpb.PetSwitchRequest)
		if !ok || request == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("PET_SWITCH payload type mismatch")
		}
		decision := playerpet.Switch(state, catalog, playerpet.SwitchRequest{
			OperationID:              operationID,
			TargetPetID:              int32(request.GetTargetPetId()),
			ExpectedPetConfigVersion: request.GetExpectedPetConfigVersion(),
			ExpectedGuardRuleVersion: request.GetExpectedGuardRuleVersion(),
		})
		status, petError, candidate = decision.Status, decision.ErrorCode, state
		if status == playerpet.StatusSucceeded {
			candidate = decision.Candidate
			order, orderErr := buildPetGuardSyncOrder(playerID, operationID, nowMs, decision.GuardSync)
			err = orderErr
			if err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			guardOrder = &order
		}
	default:
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("unsupported pet command")
	}

	if status != playerpet.StatusSucceeded && status != playerpet.StatusNoChange && status != playerpet.StatusRejected {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("pet domain returned an unspecified terminal status")
	}
	if err := EnsureOperationReceiptCapacity(rec, nowMs); err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	if status == playerpet.StatusSucceeded {
		if err := playerpet.ApplyStateToPlayerRecord(rec, candidate); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
	}
	mutation := buildPetMutationView(candidate, nowMs, coinDelta, itemID, itemDelta, guardOrder)
	switch commandType {
	case commonpb.CommandType_PET_UNLOCK_BY_COIN:
		result = &publicpb.PetUnlockByCoinResult{Mutation: mutation}
	case commonpb.CommandType_PET_FEED:
		result = &publicpb.PetFeedResult{Mutation: mutation}
	case commonpb.CommandType_PET_SWITCH:
		result = &publicpb.PetSwitchResult{Mutation: mutation}
	default:
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("unsupported pet result")
	}
	resultPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	publicStatus, publicError := petPublicOutcome(status, petError)
	storedErrorCode := ""
	if publicError != commonpb.ErrorCode_OK && publicError != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		storedErrorCode = publicError.String()
	}
	AppendOperationReceiptAt(rec, operationID, payloadHash, resultPayload, publicStatus, publicError, nowMs)
	receipt := findReceipt(rec, operationID)
	if receipt == nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("pet operation receipt was not appended")
	}
	terminalStatus, err := petTerminalStatus(status)
	if err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	return receipt, true, authoritystore.StoredOperationOutcome{
		Status:    terminalStatus,
		ErrorCode: storedErrorCode,
		Payload:   resultPayload,
	}, orderSlice(guardOrder), nil
}

func orderSlice(order *authoritystore.AttachedOrder) []authoritystore.AttachedOrder {
	if order == nil {
		return nil
	}
	return []authoritystore.AttachedOrder{*order}
}

func petTerminalStatus(status playerpet.ActionStatus) (authoritystore.TerminalStatus, error) {
	switch status {
	case playerpet.StatusSucceeded:
		return authoritystore.TerminalSucceeded, nil
	case playerpet.StatusNoChange:
		return authoritystore.TerminalNoChange, nil
	case playerpet.StatusRejected:
		return authoritystore.TerminalRejected, nil
	default:
		return authoritystore.TerminalUnknown, errors.New("pet domain returned an invalid terminal status")
	}
}

func petPublicOutcome(status playerpet.ActionStatus, petError playerpet.ErrorCode) (commonpb.CommandStatus, commonpb.ErrorCode) {
	switch status {
	case playerpet.StatusSucceeded:
		return commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK
	case playerpet.StatusNoChange:
		return commonpb.CommandStatus_NO_CHANGE, commonpb.ErrorCode_OK
	case playerpet.StatusRejected:
		return commonpb.CommandStatus_REJECTED, petPublicErrorCode(petError)
	default:
		return commonpb.CommandStatus_UNAVAILABLE, commonpb.ErrorCode_ERR_INTERNAL
	}
}

func petPublicErrorCode(code playerpet.ErrorCode) commonpb.ErrorCode {
	switch code {
	case playerpet.ErrPetAlreadyUnlocked, playerpet.ErrPetAlreadyActive:
		return commonpb.ErrorCode_OK
	case playerpet.ErrPetNotFound, playerpet.ErrPetNotAvailable:
		return commonpb.ErrorCode_ERR_OFFER_NOT_FOUND
	case playerpet.ErrPetNotUnlocked:
		return commonpb.ErrorCode_ERR_PERMISSION_DENIED
	case playerpet.ErrPetConfigVersionChanged:
		return commonpb.ErrorCode_ERR_VERSION_CONFLICT
	case playerpet.ErrPetPriceChanged, playerpet.ErrPetFeedRuleChanged, playerpet.ErrPetGuardRuleChanged,
		playerpet.ErrPetFeedLimitExceeded:
		return commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH
	case playerpet.ErrInsufficientCoin:
		return commonpb.ErrorCode_ERR_INSUFFICIENT_COIN
	case playerpet.ErrInsufficientDogFood:
		return commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	default:
		return commonpb.ErrorCode_ERR_INTERNAL
	}
}

func buildPetMutationView(state playerpet.State, nowMs, coinDelta int64, itemID int32, itemDelta int64, order *authoritystore.AttachedOrder) *publicpb.PetMutationView {
	view := &publicpb.PetMutationView{
		ResultingState:         buildPetStateView(state, nowMs),
		GuardSyncRequired:      order != nil,
		GuardVersionAtDecision: state.GuardVersion,
		CoinDelta:              coinDelta,
		ItemId:                 itemID,
		ItemDelta:              itemDelta,
		TrustedServerNowMs:     nowMs,
	}
	if order != nil {
		orderID := order.OrderID
		view.GuardSyncOrderId = &orderID
	}
	return view
}

func buildPetPage(catalog playerpet.CatalogSnapshot, state playerpet.State, nowMs int64) *publicpb.PetPageView {
	guardSyncStatus := publicpb.GuardSyncStatus_GUARD_SYNC_NOT_REQUIRED
	if state.ActiveGuardPetID != 0 {
		guardSyncStatus = publicpb.GuardSyncStatus_GUARD_SYNC_PENDING
	}
	pets := make([]*publicpb.PetCatalogItemView, 0, len(catalog.Pets))
	for _, definition := range catalog.Pets {
		displayState := publicpb.PetDisplayState_PET_LOCKED
		if state.Owns(definition.PetID) {
			switch {
			case state.ActiveGuardPetID == definition.PetID && state.GuardEffective(nowMs):
				displayState = publicpb.PetDisplayState_PET_GUARDING
			case state.ActiveGuardPetID == definition.PetID:
				displayState = publicpb.PetDisplayState_PET_SELECTED_INACTIVE
			default:
				displayState = publicpb.PetDisplayState_PET_UNLOCKED_NOT_SELECTED
			}
		}
		item := &publicpb.PetCatalogItemView{
			PetId:               uint32(definition.PetID),
			PetConfigVersion:    definition.PetConfigVersion,
			DisplayDefinitionId: definition.DisplayDefinitionID,
			State:               displayState,
			CoinUnlockEnabled:   definition.UnlockOffer.CoinUnlockEnabled,
			GuardRuleVersion:    definition.GuardRule.GuardRuleVersion,
			TaskUnlockHint:      false,
		}
		if definition.UnlockOffer.CoinUnlockEnabled {
			price := definition.UnlockOffer.CoinPrice
			priceVersion := definition.UnlockOffer.PriceVersion
			item.CoinPrice = &price
			item.PriceVersion = &priceVersion
		}
		pets = append(pets, item)
	}
	return &publicpb.PetPageView{
		TrustedServerNowMs:    nowMs,
		PetCatalogVersion:     catalog.CatalogVersion,
		State:                 buildPetStateView(state, nowMs),
		GuardSyncStatus:       guardSyncStatus,
		FeedRuleVersion:       catalog.FeedRule.FeedRuleVersion,
		DogFoodItemId:         catalog.FeedRule.DogFoodItemID,
		DurationPerFoodMs:     catalog.FeedRule.DurationPerFoodMs,
		MaxGuardHorizonMs:     catalog.FeedRule.MaxGuardHorizonMs,
		MaxQuantityPerCommand: catalog.FeedRule.MaxQuantityPerCommand,
		Pets:                  pets,
	}
}

func buildPetStateView(state playerpet.State, nowMs int64) *publicpb.PetStateView {
	view := &publicpb.PetStateView{
		PetStateVersion: state.PetStateVersion,
		OwnedPetIds:     make([]uint32, 0, len(state.OwnedPetIDs)),
		GuardUntilMs:    state.GuardUntilMs,
		GuardEffective:  state.GuardEffective(nowMs),
		GuardVersion:    state.GuardVersion,
	}
	for _, petID := range state.OwnedPetIDs {
		view.OwnedPetIds = append(view.OwnedPetIds, uint32(petID))
	}
	if state.ActiveGuardPetID != 0 {
		active := uint32(state.ActiveGuardPetID)
		view.ActiveGuardPetId = &active
	}
	return view
}

func (s *Service) rebuildPetGuardSyncOrders(rec *playerpb.PlayerRecord, playerID uint64, commandType commonpb.CommandType, operationID string, receipt *playerpb.PlayerOperationReceipt) ([]authoritystore.AttachedOrder, error) {
	if s == nil || s.bestEffortGuardSync == nil || receipt == nil || receipt.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		return nil, nil
	}
	if commandType != commonpb.CommandType_PET_FEED && commandType != commonpb.CommandType_PET_SWITCH {
		return nil, nil
	}
	state, err := playerpet.StateFromPlayerRecord(rec)
	if err != nil {
		return nil, err
	}
	sync, ok := playerpet.BuildGuardSyncInput(state)
	if !ok {
		return nil, errors.New("successful pet receipt cannot rebuild current guard synchronization")
	}
	if receipt.GetCommittedAtMs() <= 0 {
		return nil, errors.New("successful pet receipt is missing trusted commit time")
	}
	order, err := buildPetGuardSyncOrder(playerID, operationID, receipt.GetCommittedAtMs(), sync)
	if err != nil {
		return nil, err
	}
	return []authoritystore.AttachedOrder{order}, nil
}

func buildPetGuardSyncOrder(playerID uint64, operationID string, dueAtMs int64, sync playerpet.GuardSyncInput) (authoritystore.AttachedOrder, error) {
	if sync.GuardVersion == 0 || sync.GuardVersion != sync.Snapshot.GuardVersion ||
		sync.Snapshot.ActiveGuardPetID <= 0 || sync.SnapshotHash != sync.Snapshot.Hash() ||
		operationID == "" || dueAtMs <= 0 {
		return authoritystore.AttachedOrder{}, errors.New("invalid pet guard sync input")
	}
	orderID, err := identity.SourceOrderID(
		identity.WorkTypeGuardSyncOrder,
		identity.SourceKindPlayerActor,
		strconv.FormatUint(playerID, 10),
		"PET_GUARD_SLOT",
		sync.GuardVersion,
	)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	activePetID := uint32(sync.Snapshot.ActiveGuardPetID)
	rule := sync.Snapshot.Rule
	payload := &contractspb.ApplyGuardSnapshotPayload{
		OrderId:        orderID,
		SourcePlayerId: playerID,
		TargetFarmId:   playerID,
		GuardSnapshot: &contractspb.GuardSnapshot{
			SchemaVersion:    uint32(sync.Snapshot.SchemaVersion),
			GuardVersion:     sync.Snapshot.GuardVersion,
			ActiveGuardPetId: &activePetID,
			GuardUntilMs:     sync.Snapshot.GuardUntilMs,
			GuardRule: &contractspb.GuardRuleSnapshot{
				PetConfigVersion:       rule.PetConfigVersion,
				GuardRuleVersion:       rule.GuardRuleVersion,
				TriggerProbabilityBp:   rule.TriggerProbabilityBp,
				InterceptPolicy:        contractspb.GuardInterceptPolicy(rule.InterceptPolicy),
				RandomAlgorithmVersion: rule.RandomAlgorithmVersion,
				RandomKeyVersion:       rule.RandomKeyVersion,
			},
			SnapshotHash: append([]byte(nil), sync.SnapshotHash[:]...),
		},
		SnapshotHash: append([]byte(nil), sync.SnapshotHash[:]...),
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	canonicalHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	var payloadHash authoritystore.PayloadHash
	copy(payloadHash[:], canonicalHash[:])
	return authoritystore.AttachedOrder{
		OrderID:         orderID,
		OrderType:       string(identity.WorkTypeGuardSyncOrder),
		Payload:         raw,
		PayloadHash:     payloadHash,
		DueAtMs:         dueAtMs,
		OperationID:     operationID,
		SubjectPlayerID: playerID,
	}, nil
}

// statusErrorUnavailable is kept local so pet ingress does not need to expose
// the gRPC status construction details through the pure pet package.
func statusErrorUnavailable(message string) error {
	return status.Error(codes.Unavailable, message)
}
