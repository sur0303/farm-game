package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/player/mail"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const mailPageSize = 6

// executeMailCommand keeps ordinary mailbox reads and mutations on the
// PlayerMailDomain ports. When an attachment projection has not caught up,
// list/detail/delete may perform one bounded PlayerAuthority read to avoid
// presenting an already-claimed attachment as claimable.
func (s *Service) executeMailCommand(ctx context.Context, playerID uint64, cmd *commonpb.ClientCommandEnvelope, decoded proto.Message, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	switch cmd.CommandType {
	case commonpb.CommandType_GET_MAIL_LIST:
		request, ok := decoded.(*publicpb.GetMailListRequest)
		if !ok || request == nil {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "GET_MAIL_LIST payload type mismatch")
			return businessResponse(outcome), nil
		}
		return s.executeMailList(ctx, playerID, request, outcome)
	case commonpb.CommandType_GET_MAIL_DETAIL:
		request, ok := decoded.(*publicpb.GetMailDetailRequest)
		if !ok || request == nil {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "GET_MAIL_DETAIL payload type mismatch")
			return businessResponse(outcome), nil
		}
		return s.executeMailDetail(ctx, playerID, request, outcome)
	case commonpb.CommandType_MARK_MAIL_READ:
		request, ok := decoded.(*publicpb.MarkMailReadRequest)
		if !ok || request == nil {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "MARK_MAIL_READ payload type mismatch")
			return businessResponse(outcome), nil
		}
		return s.executeMarkMailRead(ctx, playerID, cmd, request, outcome)
	case commonpb.CommandType_DELETE_MAIL:
		request, ok := decoded.(*publicpb.DeleteMailRequest)
		if !ok || request == nil {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "DELETE_MAIL payload type mismatch")
			return businessResponse(outcome), nil
		}
		return s.executeDeleteMail(ctx, playerID, cmd, request, outcome)
	default:
		applyError(outcome, fmt.Errorf("mail command %s is not handled by PlayerMailDomain", cmd.CommandType))
		return businessResponse(outcome), nil
	}
}

func (s *Service) executeMailList(ctx context.Context, playerID uint64, request *publicpb.GetMailListRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.mailReads == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "mail read repository is not composed")
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	page, err := s.mailReads.ListPage(ctx, playerID, request.GetOpaqueCursor(), nowMs)
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	if len(page.Items) > mailPageSize {
		applyError(outcome, fmt.Errorf("mail repository returned %d items, page size is %d", len(page.Items), mailPageSize))
		return businessResponse(outcome), nil
	}
	overlaidClaims, err := s.overlayAuthoritativeMailClaims(ctx, playerID, page.Items)
	if err != nil {
		applyMailAuthorityReadError(outcome, err)
		return businessResponse(outcome), nil
	}
	summary, err := s.mailReads.GetSummary(ctx, playerID)
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	summary = overlayMailSummaryClaims(summary, overlaidClaims)

	result := &publicpb.GetMailListResult{
		Mails:              make([]*publicpb.MailListItemView, 0, len(page.Items)),
		Summary:            mailSummaryView(summary),
		TrustedServerNowMs: nowMs,
	}
	for _, item := range page.Items {
		result.Mails = append(result.Mails, mailListItemView(item, nowMs))
	}
	if page.NextCursor != "" {
		result.NextCursor = proto.String(page.NextCursor)
	}
	outcome.Status = commonpb.CommandStatus_SUCCEEDED
	marshalMailResult(outcome, result)
	return businessResponse(outcome), nil
}

func (s *Service) executeMailDetail(ctx context.Context, playerID uint64, request *publicpb.GetMailDetailRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.mailReads == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "mail read repository is not composed")
		return businessResponse(outcome), nil
	}
	if !validateMailID(outcome, request.GetMailId()) {
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	entry, state, err := s.mailReads.GetEntry(ctx, playerID, request.GetMailId())
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	if state.DeletedAtMs > 0 || state.AutoEvicted {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_NOT_FOUND, "mail not found")
		return businessResponse(outcome), nil
	}
	authoritativeClaimOverlay := int64(0)
	if entry.HasAttachment() && !state.ClaimedProjection && s.registry != nil {
		claimState, claimErr := s.readAuthoritativeMailClaimState(ctx, playerID)
		if claimErr != nil {
			applyMailAuthorityReadError(outcome, claimErr)
			return businessResponse(outcome), nil
		}
		switch claimState.ClaimStatus(entry.MailID) {
		case mail.ClaimStatusClaimed:
			state.ClaimedProjection = true
			authoritativeClaimOverlay = 1
		case mail.ClaimStatusUnclaimed:
		case mail.ClaimStatusUnknown:
			applyMailAuthorityReadError(outcome, mail.ErrInvalidClaimState)
			return businessResponse(outcome), nil
		}
	}
	summary, err := s.mailReads.GetSummary(ctx, playerID)
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	summary = overlayMailSummaryClaims(summary, authoritativeClaimOverlay)
	outcome.Status = commonpb.CommandStatus_SUCCEEDED
	marshalMailResult(outcome, &publicpb.GetMailDetailResult{
		Mail:               mailDetailView(entry, state, nowMs),
		Summary:            mailSummaryView(summary),
		TrustedServerNowMs: nowMs,
	})
	return businessResponse(outcome), nil
}

func (s *Service) executeMarkMailRead(ctx context.Context, playerID uint64, cmd *commonpb.ClientCommandEnvelope, request *publicpb.MarkMailReadRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.mailEntries == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "mail entry repository is not composed")
		return businessResponse(outcome), nil
	}
	if !validateMailID(outcome, request.GetMailId()) {
		return businessResponse(outcome), nil
	}
	mutationID := mailMutationID(commonpb.CommandType_MARK_MAIL_READ, cmd, request.GetMailId())
	result, err := s.mailEntries.MarkMailRead(ctx, playerID, request.GetMailId(), mutationID)
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	applyMailMutationResult(outcome, result, request.GetMailId(), false)
	return businessResponse(outcome), nil
}

func (s *Service) executeDeleteMail(ctx context.Context, playerID uint64, cmd *commonpb.ClientCommandEnvelope, request *publicpb.DeleteMailRequest, outcome *logicrpcpb.BusinessCommandOutcome) (*logicrpcpb.ExecuteCommandResponse, error) {
	if s.mailReads == nil || s.mailEntries == nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "mail repositories are not composed")
		return businessResponse(outcome), nil
	}
	if !validateMailID(outcome, request.GetMailId()) {
		return businessResponse(outcome), nil
	}
	nowMs := s.trustedNowMs()
	entry, state, err := s.mailReads.GetEntry(ctx, playerID, request.GetMailId())
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	authorization := mail.AllowDelete()
	if state.DeletedAtMs == 0 && !state.AutoEvicted && entry.HasAttachment() && !state.ClaimedProjection && nowMs < entry.ClaimEndAtMs {
		if s.registry == nil {
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_HAS_UNCLAIMED_ATTACHMENT, "mail has an unclaimed attachment")
			return businessResponse(outcome), nil
		}
		claimState, claimErr := s.readAuthoritativeMailClaimState(ctx, playerID)
		if claimErr != nil {
			applyMailAuthorityReadError(outcome, claimErr)
			return businessResponse(outcome), nil
		}
		switch claimState.ClaimStatus(entry.MailID) {
		case mail.ClaimStatusClaimed:
			proof, proofErr := mailDeleteClaimProof(playerID, entry.MailID, claimState)
			if proofErr != nil {
				applyMailAuthorityReadError(outcome, proofErr)
				return businessResponse(outcome), nil
			}
			authorization = mail.AllowDeleteWithProof(proof)
		case mail.ClaimStatusUnclaimed:
			rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_HAS_UNCLAIMED_ATTACHMENT, "mail has an unclaimed attachment")
			return businessResponse(outcome), nil
		case mail.ClaimStatusUnknown:
			applyMailAuthorityReadError(outcome, mail.ErrInvalidClaimState)
			return businessResponse(outcome), nil
		}
	}
	mutationID := mailMutationID(commonpb.CommandType_DELETE_MAIL, cmd, request.GetMailId())
	result, err := s.mailEntries.DeleteMail(ctx, playerID, request.GetMailId(), mutationID, authorization)
	if err != nil {
		applyMailRepositoryError(outcome, err)
		return businessResponse(outcome), nil
	}
	applyMailMutationResult(outcome, result, request.GetMailId(), true)
	return businessResponse(outcome), nil
}

func (s *Service) overlayAuthoritativeMailClaims(ctx context.Context, playerID uint64, items []mail.ListItem) (int64, error) {
	if s.registry == nil {
		return 0, nil
	}
	needsAuthority := false
	for i := range items {
		if items[i].HasAttachment && !items[i].ClaimedProjection {
			needsAuthority = true
			break
		}
	}
	if !needsAuthority {
		return 0, nil
	}
	claimState, err := s.readAuthoritativeMailClaimState(ctx, playerID)
	if err != nil {
		return 0, err
	}
	var overlaid int64
	for i := range items {
		if !items[i].HasAttachment || items[i].ClaimedProjection {
			continue
		}
		switch claimState.ClaimStatus(items[i].MailID) {
		case mail.ClaimStatusClaimed:
			items[i].ClaimedProjection = true
			overlaid++
		case mail.ClaimStatusUnclaimed:
		case mail.ClaimStatusUnknown:
			return 0, mail.ErrInvalidClaimState
		}
	}
	return overlaid, nil
}

// overlayMailSummaryClaims corrects the rebuildable mailbox counter view
// with the bounded authoritative receipts already read for this response.
// It never increases counters and therefore cannot manufacture a red point.
// A later projection commit converges the stored summary to the same value.
func overlayMailSummaryClaims(summary mail.MailboxSummary, overlaidClaims int64) mail.MailboxSummary {
	if overlaidClaims <= 0 || summary.ClaimableCount <= 0 {
		return summary
	}
	if overlaidClaims > summary.ClaimableCount {
		overlaidClaims = summary.ClaimableCount
	}
	summary.ClaimableCount -= overlaidClaims
	return summary
}

func (s *Service) readAuthoritativeMailClaimState(ctx context.Context, playerID uint64) (mail.ClaimState, error) {
	if s.registry == nil {
		return mail.ClaimState{}, errors.New("mail claim authority is not composed")
	}
	record, _, err := s.registry.Read(ctx, playerID)
	if err != nil {
		return mail.ClaimState{}, err
	}
	return mail.ClaimStateFromPlayerRecord(record)
}

func mailDeleteClaimProof(playerID uint64, mailID string, state mail.ClaimState) (mail.ClaimProof, error) {
	receipt, ok := state.ReceiptFor(mailID)
	if !ok {
		return mail.ClaimProof{}, mail.ErrInvalidClaimState
	}
	canonicalPlayer := strconv.FormatUint(playerID, 10)
	receiptRef, err := workidentity.StableOpaqueID(mailClaimReceiptNamespace, canonicalPlayer, mailID)
	if err != nil {
		return mail.ClaimProof{}, err
	}
	proof := mail.ClaimProof{
		MailID: mailID, Claimed: true,
		ClaimMaterialHash: hex.EncodeToString(receipt.ClaimMaterialHash),
		ReceiptRef:        receiptRef,
	}
	if err := proof.Validate(); err != nil {
		return mail.ClaimProof{}, err
	}
	return proof, nil
}

func applyMailAuthorityReadError(outcome *logicrpcpb.BusinessCommandOutcome, err error) {
	outcome.Status = commonpb.CommandStatus_UNAVAILABLE
	outcome.Error = &logicrpcpb.ErrorDetail{
		ErrorCode:    commonpb.ErrorCode_ERR_STORE_UNAVAILABLE,
		ErrorMessage: fmt.Sprintf("mail claim authority is temporarily unavailable: %v", err),
	}
}

func validateMailID(outcome *logicrpcpb.BusinessCommandOutcome, mailID string) bool {
	if err := mail.ValidateMailID(mailID); err != nil {
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, "mail_id is invalid")
		return false
	}
	return true
}

func mailMutationID(commandType commonpb.CommandType, cmd *commonpb.ClientCommandEnvelope, mailID string) string {
	seed := cmd.OperationId
	if seed == "" {
		seed = cmd.RequestId
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("mail-mutation-v1\x00%d\x00%s\x00%s", commandType, mailID, seed)))
	return "mail-mutation-" + hex.EncodeToString(digest[:])
}

func applyMailMutationResult(outcome *logicrpcpb.BusinessCommandOutcome, result mail.MutationResult, mailID string, deleted bool) {
	switch result.Status {
	case mail.MutationReadMarked:
		if deleted {
			applyError(outcome, errors.New("mail delete returned READ_MARKED"))
			return
		}
		outcome.Status = commonpb.CommandStatus_SUCCEEDED
		marshalMailResult(outcome, &publicpb.MarkMailReadResult{MailId: mailID, Summary: mailSummaryView(result.Summary)})
	case mail.MutationDeleted:
		if !deleted {
			applyError(outcome, errors.New("mail read returned DELETED"))
			return
		}
		outcome.Status = commonpb.CommandStatus_SUCCEEDED
		marshalMailResult(outcome, &publicpb.DeleteMailResult{MailId: mailID, Summary: mailSummaryView(result.Summary)})
	case mail.MutationNoChange:
		outcome.Status = commonpb.CommandStatus_NO_CHANGE
		if deleted {
			marshalMailResult(outcome, &publicpb.DeleteMailResult{MailId: mailID, Summary: mailSummaryView(result.Summary)})
		} else {
			marshalMailResult(outcome, &publicpb.MarkMailReadResult{MailId: mailID, Summary: mailSummaryView(result.Summary)})
		}
	case mail.MutationMailNotFound:
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_NOT_FOUND, "mail not found")
	case mail.MutationRejectUnclaimed:
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_HAS_UNCLAIMED_ATTACHMENT, "mail has an unclaimed attachment")
	case mail.MutationUnavailableNotSent:
		outcome.Status = commonpb.CommandStatus_UNAVAILABLE
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_STORE_UNAVAILABLE, ErrorMessage: "mail mutation was not sent"}
	case mail.MutationTimeoutUnknown:
		outcome.Status = commonpb.CommandStatus_UNAVAILABLE
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT, ErrorMessage: "mail mutation outcome is unknown"}
	default:
		applyError(outcome, fmt.Errorf("unknown mail mutation status %q", result.Status))
	}
}

func applyMailRepositoryError(outcome *logicrpcpb.BusinessCommandOutcome, err error) {
	switch {
	case errors.Is(err, mail.ErrMailNotFound):
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_MAIL_NOT_FOUND, "mail not found")
	case errors.Is(err, mail.ErrMalformedSortMember), errors.Is(err, mail.ErrInvalidIdentity), errors.Is(err, mail.ErrInvalidRequest):
		rejectOutcome(outcome, commonpb.ErrorCode_ERR_DECODE, err.Error())
	default:
		applyError(outcome, err)
	}
}

func marshalMailResult(outcome *logicrpcpb.BusinessCommandOutcome, result proto.Message) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		applyError(outcome, err)
		return
	}
	outcome.ResultPayload = payload
}

func mailSummaryView(summary mail.MailboxSummary) *publicpb.MailboxSummaryView {
	return &publicpb.MailboxSummaryView{
		MailboxRevision:          summary.MailboxRevision,
		UnreadCount:              summary.UnreadCount,
		ClaimableAttachmentCount: summary.ClaimableCount,
		HasRedPoint:              summary.UnreadCount > 0 || summary.ClaimableCount > 0,
	}
}

func mailListItemView(item mail.ListItem, nowMs int64) *publicpb.MailListItemView {
	return &publicpb.MailListItemView{
		MailId:          item.MailID,
		Title:           item.Title,
		CreatedAtMs:     item.CreatedAtMs,
		ExpireAtMs:      item.ExpireAtMs,
		Read:            item.Read,
		HasAttachment:   item.HasAttachment,
		AttachmentState: mailAttachmentState(item.HasAttachment, item.ClaimedProjection, nowMs, item.ExpireAtMs),
	}
}

func mailDetailView(entry mail.MailEntry, state mail.MailUserState, nowMs int64) *publicpb.MailDetailView {
	view := &publicpb.MailDetailView{
		MailId:          entry.MailID,
		Title:           entry.TitleSnapshot,
		Body:            entry.BodySnapshot,
		CreatedAtMs:     entry.CreatedAtMs,
		ExpireAtMs:      entry.ExpireAtMs,
		ClaimEndAtMs:    entry.ClaimEndAtMs,
		Read:            state.ReadAtMs > 0,
		AttachmentState: mailAttachmentState(entry.HasAttachment(), state.ClaimedProjection, nowMs, entry.ClaimEndAtMs),
	}
	if entry.HasAttachment() {
		view.Attachment = mailRewardView(entry.Reward)
	}
	return view
}

func mailRewardView(reward mail.RewardBundle) *publicpb.MailRewardView {
	view := &publicpb.MailRewardView{Coin: reward.Coin, Exp: reward.Exp, Items: make([]*publicpb.MailRewardItemView, 0, len(reward.Items))}
	for _, item := range reward.Items {
		view.Items = append(view.Items, &publicpb.MailRewardItemView{ItemId: item.ItemID, Count: item.Count})
	}
	return view
}

func mailAttachmentState(hasAttachment, claimed bool, nowMs, claimEndAtMs int64) publicpb.MailAttachmentState {
	if !hasAttachment {
		return publicpb.MailAttachmentState_MAIL_NO_ATTACHMENT
	}
	if claimed {
		return publicpb.MailAttachmentState_MAIL_ATTACHMENT_CLAIMED
	}
	if claimEndAtMs > 0 && nowMs < claimEndAtMs {
		return publicpb.MailAttachmentState_MAIL_ATTACHMENT_CLAIMABLE
	}
	return publicpb.MailAttachmentState_MAIL_ATTACHMENT_UNKNOWN
}
