package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strconv"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playermail "farmgame/internal/player/mail"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const mailClaimReceiptNamespace = "farm-mail-claim-receipt-v1"

type mailClaimTurnResult struct {
	payload    []byte
	status     commonpb.CommandStatus
	errorCode  commonpb.ErrorCode
	projection *playermail.ClaimProjectionOrder
}

func (s *Service) executeMailClaim(ctx context.Context, playerID uint64, operationID string, payloadHash []byte, request *publicpb.ClaimMailAttachmentRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	outcome := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED}
	if s == nil || s.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if s.mailReads == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "PlayerMail read repository is not composed")
		return businessResponse(outcome), nil
	}
	if request == nil || request.GetMailId() == "" {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "mail_id is required")
		return businessResponse(outcome), nil
	}
	// Operation terminal is authoritative over the current mailbox view. A
	// successful claim may be followed by projection delay, expiry or physical
	// mail cleanup; replaying the same operation must still return the original
	// terminal bytes instead of being rewritten as NOT_FOUND/EXPIRED.
	if replay, found, err := s.lookupMailClaimReplay(ctx, playerID, operationID, payloadHash); err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	} else if found {
		return replay, nil
	}
	nowMs := s.trustedNowMs()
	entry, state, err := s.mailReads.GetEntry(ctx, playerID, request.GetMailId())
	if err != nil {
		if errors.Is(err, playermail.ErrMailNotFound) {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_NOT_FOUND, "mail not found")
			return businessResponse(outcome), nil
		}
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	if !entry.HasAttachment() {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_HAS_NO_ATTACHMENT, "mail has no attachment")
		return businessResponse(outcome), nil
	}
	if state.DeletedAtMs > 0 || state.AutoEvicted || state.ExpiredAtMs > 0 {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_NOT_FOUND, "mail is no longer visible")
		return businessResponse(outcome), nil
	}
	if nowMs >= entry.ClaimEndAtMs {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_CLAIM_EXPIRED, "mail claim window ended")
		return businessResponse(outcome), nil
	}
	material, err := (playermail.ClaimMaterialIssuer{Issuer: playermail.ClaimMaterialIssuerIngress}).Issue(entry, nowMs)
	if err != nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_CLAIM_MATERIAL_INVALID, err.Error())
		return businessResponse(outcome), nil
	}

	submit, err := s.registry.SubmitDurableCommand(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
		if prior := findReceipt(record, operationID); prior != nil {
			if !equalBytes(prior.GetPayloadHash(), payloadHash) {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, &domainError{code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, message: "operation_id reused with different payload"}
			}
			return &mailClaimTurnResult{payload: append([]byte(nil), prior.GetResultPayload()...), status: prior.GetStatus(), errorCode: prior.GetErrorCode()}, false, authoritystore.StoredOperationOutcome{}, nil, nil
		}

		turnNowMs := s.trustedNowMs()
		claimState, stateErr := playermail.ClaimStateFromPlayerRecord(record)
		if stateErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, stateErr
		}
		if existing, claimed := claimState.ReceiptFor(material.MailID); claimed {
			statusValue := commonpb.CommandStatus_NO_CHANGE
			errorValue := commonpb.ErrorCode_ERR_MAIL_ALREADY_CLAIMED
			if existing.FirstOperationID == operationID {
				statusValue, errorValue = commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK
			}
			return durableMailClaimReceipt(record, operationID, payloadHash, existing.RewardResult, statusValue, errorValue, turnNowMs)
		}
		if turnNowMs >= material.ClaimEndAtMs {
			return durableMailClaimRejection(record, operationID, payloadHash, commonpb.ErrorCode_ERR_MAIL_CLAIM_EXPIRED, material.MailID, turnNowMs)
		}
		if validateErr := material.ValidateForTurn(playerID, turnNowMs); validateErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, validateErr
		}
		candidate, ok := proto.Clone(record).(*playerpb.PlayerRecord)
		if !ok || candidate == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, errors.New("failed to clone PlayerRecord for mail claim")
		}
		if err := ensureReceiptCapacity(candidate, turnNowMs); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
		if err := applyMailReward(candidate, material.Reward); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
		candidate.EconomyVersion++
		result := &publicpb.ClaimMailAttachmentResult{
			MailId:        material.MailID,
			PrivateState:  playeractor.PrivateState(candidate),
			GrantedReward: publicMailReward(material.Reward),
			ClaimedAtMs:   turnNowMs,
		}
		resultPayload, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(result)
		if marshalErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, marshalErr
		}
		hashRaw, decodeErr := hex.DecodeString(material.ClaimMaterialHash)
		if decodeErr != nil || len(hashRaw) != sha256.Size {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, playermail.ErrInvalidClaimMaterial
		}
		claimReceipt := playermail.ClaimReceipt{
			MailID: material.MailID, FirstOperationID: operationID, MailVersion: material.MailVersion,
			ClaimMaterialHash: hashRaw, RewardResult: resultPayload,
			ClaimDecisionAtMs: turnNowMs, ClaimedAtMs: turnNowMs,
			ReceiptRetainUntilMs: mailClaimRetainUntil(material.ClaimEndAtMs),
		}
		nextClaimState, changed, stateErr := claimState.RecordClaim(claimReceipt)
		if stateErr != nil || !changed {
			if stateErr == nil {
				stateErr = errors.New("mail claim state did not advance")
			}
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, stateErr
		}
		if err := playermail.ApplyClaimStateToPlayerRecord(candidate, nextClaimState); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
		}
		AppendOperationReceiptAt(candidate, operationID, payloadHash, resultPayload, commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_OK, turnNowMs)

		order, projection, orderErr := buildMailClaimProjectionOrder(playerID, material, nextClaimState.ProjectionDirtyGeneration, turnNowMs)
		if orderErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, nil, orderErr
		}
		proto.Reset(record)
		proto.Merge(record, candidate)
		stored := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: append([]byte(nil), resultPayload...)}
		return &mailClaimTurnResult{payload: resultPayload, status: commonpb.CommandStatus_SUCCEEDED, errorCode: commonpb.ErrorCode_OK, projection: &projection}, true, stored, []authoritystore.AttachedOrder{order}, nil
	})
	if err != nil {
		applyError(outcome, err)
		return businessResponse(outcome), nil
	}
	turnResult, ok := submit.Value.(*mailClaimTurnResult)
	if !ok || turnResult == nil {
		applyError(outcome, errors.New("mail claim returned an invalid result"))
		return businessResponse(outcome), nil
	}
	outcome.Status = turnResult.status
	if turnResult.errorCode != commonpb.ErrorCode_OK {
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: turnResult.errorCode}
	}
	outcome.ResultPayload = append([]byte(nil), turnResult.payload...)
	// Projection is deliberately outside the Actor mailbox and cannot roll
	// back the already committed asset grant.  The attached source order is
	// the recovery anchor when this fast path fails or its response is lost.
	if turnResult.projection != nil && s.mailEntries != nil {
		_, _ = s.mailEntries.ApplyMailClaimProjection(ctx, *turnResult.projection)
	}
	return businessResponse(outcome), nil
}

func (s *Service) lookupMailClaimReplay(ctx context.Context, playerID uint64, operationID string, payloadHash []byte) (*logicrpcpb.ExecuteCommandResponse, bool, error) {
	record, _, err := s.registry.Read(ctx, playerID)
	if err != nil {
		return nil, false, err
	}
	prior := findReceipt(record, operationID)
	if prior == nil {
		return nil, false, nil
	}
	if !equalBytes(prior.GetPayloadHash(), payloadHash) {
		outcome := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_REJECTED}
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, ErrorMessage: "operation_id reused with different payload"}
		return businessResponse(outcome), true, nil
	}
	outcome := &logicrpcpb.BusinessCommandOutcome{
		Status:        prior.GetStatus(),
		ResultPayload: append([]byte(nil), prior.GetResultPayload()...),
	}
	if prior.GetErrorCode() != commonpb.ErrorCode_OK {
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: prior.GetErrorCode()}
	}
	return businessResponse(outcome), true, nil
}

func durableMailClaimReceipt(record *playerpb.PlayerRecord, operationID string, payloadHash, resultPayload []byte, commandStatus commonpb.CommandStatus, errorCode commonpb.ErrorCode, nowMs int64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
	if err := ensureReceiptCapacity(record, nowMs); err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	AppendOperationReceiptAt(record, operationID, payloadHash, resultPayload, commandStatus, errorCode, nowMs)
	terminal := authoritystore.TerminalNoChange
	if commandStatus == commonpb.CommandStatus_SUCCEEDED {
		terminal = authoritystore.TerminalSucceeded
	}
	return &mailClaimTurnResult{payload: append([]byte(nil), resultPayload...), status: commandStatus, errorCode: errorCode}, true,
		authoritystore.StoredOperationOutcome{Status: terminal, ErrorCode: errorCode.String(), Payload: append([]byte(nil), resultPayload...)}, nil, nil
}

func durableMailClaimRejection(record *playerpb.PlayerRecord, operationID string, payloadHash []byte, errorCode commonpb.ErrorCode, mailID string, nowMs int64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
	resultPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.ClaimMailAttachmentResult{MailId: mailID, ClaimedAtMs: nowMs})
	if err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	if err := ensureReceiptCapacity(record, nowMs); err != nil {
		return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
	}
	AppendOperationReceiptAt(record, operationID, payloadHash, resultPayload, commonpb.CommandStatus_REJECTED, errorCode, nowMs)
	return &mailClaimTurnResult{payload: resultPayload, status: commonpb.CommandStatus_REJECTED, errorCode: errorCode}, true,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalRejected, ErrorCode: errorCode.String(), Payload: resultPayload}, nil, nil
}

func buildMailClaimProjectionOrder(playerID uint64, material playermail.TrustedMailClaimMaterial, generation uint64, nowMs int64) (authoritystore.AttachedOrder, playermail.ClaimProjectionOrder, error) {
	canonicalPlayer := strconv.FormatUint(playerID, 10)
	orderID, err := identity.SourceOrderID(identity.WorkTypeMailClaimProjectionOrder, identity.SourceKindPlayerActor, canonicalPlayer, material.MailID, generation)
	if err != nil {
		return authoritystore.AttachedOrder{}, playermail.ClaimProjectionOrder{}, err
	}
	receiptRef, err := workidentity.StableOpaqueID(mailClaimReceiptNamespace, canonicalPlayer, material.MailID)
	if err != nil {
		return authoritystore.AttachedOrder{}, playermail.ClaimProjectionOrder{}, err
	}
	payloadMessage := &contractspb.ApplyMailClaimProjectionPayload{
		RecipientPlayerId: playerID, MailId: material.MailID, ClaimReceiptRef: receiptRef,
		ClaimMaterialHash: material.ClaimMaterialHash, ClaimedAtMs: nowMs,
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payloadMessage)
	if err != nil {
		return authoritystore.AttachedOrder{}, playermail.ClaimProjectionOrder{}, err
	}
	payloadHash := sha256.Sum256(payload)
	projection := playermail.ClaimProjectionOrder{
		RecipientPlayerID: playerID, WorkID: orderID, WorkVersion: 1, MailID: material.MailID,
		ClaimReceiptRef: receiptRef, ClaimMaterialHash: material.ClaimMaterialHash, ClaimedAtMs: nowMs,
	}
	return authoritystore.AttachedOrder{OrderID: orderID, OrderType: string(identity.WorkTypeMailClaimProjectionOrder), Payload: payload, PayloadHash: payloadHash, DueAtMs: nowMs, SubjectPlayerID: playerID}, projection, nil
}

func applyMailReward(record *playerpb.PlayerRecord, reward playermail.RewardBundle) error {
	if record == nil || record.GetWallet() == nil || record.GetInventory() == nil || record.GetProgression() == nil {
		return errors.New("mail reward requires a normalized PlayerRecord")
	}
	if reward.Coin > 0 && record.GetWallet().GetCoin() > math.MaxInt64-reward.Coin {
		return errors.New("mail coin reward overflow")
	}
	if reward.Exp > 0 && record.GetProgression().GetExp() > math.MaxInt64-reward.Exp {
		return errors.New("mail experience reward overflow")
	}
	record.Wallet.Coin += reward.Coin
	record.Progression.Exp += reward.Exp
	for _, grant := range reward.Items {
		if !playeractor.AddItem(record, grant.ItemID, grant.Count) {
			return errors.New("mail item reward cannot be applied")
		}
	}
	record.Progression.Level = int32(record.Progression.Exp/20) + 1
	return nil
}

func mailClaimRetainUntil(claimEndAtMs int64) int64 {
	retention := playermail.LimitsV1().ClaimMaterialRetainAfterClaimEndMs
	if claimEndAtMs <= 0 || retention <= 0 || claimEndAtMs > math.MaxInt64-retention {
		return math.MaxInt64
	}
	return claimEndAtMs + retention
}

func publicMailReward(reward playermail.RewardBundle) *publicpb.MailRewardView {
	view := &publicpb.MailRewardView{Coin: reward.Coin, Exp: reward.Exp}
	for _, grant := range reward.Items {
		view.Items = append(view.Items, &publicpb.MailRewardItemView{ItemId: grant.ItemID, Count: grant.Count})
	}
	return view
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
