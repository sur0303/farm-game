package auth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	friendpb "farmgame/api/gen/friend"
	"farmgame/internal/friend"
	"farmgame/internal/friendshipservice"
)

const maxFriendshipJSONBodyBytes int64 = 4 << 10
const maxFriendshipShareTokenBytes = 256
const maxFriendshipCursorBytes = 1024
const friendOpportunityHintTargetQuery = "hint_target"

var (
	ErrShareLinkInvalid = errors.New("share link is invalid or expired")
	ErrShareSelfInvite  = errors.New("cannot open your own share link")
	ErrFriendLimit      = errors.New("friend limit reached")
)

type ShareCreateResponse struct {
	ShareToken string `json:"share_token"`
	AcceptMode string `json:"accept_mode"`
}

type ShareResolveResponse struct {
	InviterPlayerId string `json:"inviter_player_id"`
	InviterLabel    string `json:"inviter_label"`
	AvatarID        uint32 `json:"avatar_id"`
	Level           uint32 `json:"level"`
	AcceptMode      string `json:"accept_mode"`
}

type ShareAcceptRequest struct {
	ShareToken string `json:"share_token"`
}

type ShareAcceptResponse struct {
	TargetOwnerId string `json:"target_owner_id"`
	InviterLabel  string `json:"inviter_label"`
	AvatarID      uint32 `json:"avatar_id"`
	Level         uint32 `json:"level"`
	RelationEpoch string `json:"relation_epoch"`
	Changed       bool   `json:"changed"`
}

type HTTPIdentityResolver interface {
	AuthenticateHTTPSession(context.Context, string) (*SessionBinding, error)
}

type ShareHandler struct {
	identities       HTTPIdentityResolver
	notifier         FriendRosterNotifier
	friendshipClient friendshipservice.Client
}

// NewFriendshipShareHandler constructs the HTTP/session adapter for every
// FriendshipService location mode. It never allocates relationship authority.
func NewFriendshipShareHandler(identities HTTPIdentityResolver, client friendshipservice.Client, friendLimit int) (*ShareHandler, error) {
	if identities == nil || client == nil || friendLimit <= 0 {
		return nil, friendshipservice.ErrInvalidArgument
	}
	return &ShareHandler{identities: identities, friendshipClient: client}, nil
}

type FriendRosterNotifier interface {
	NotifyFriendRosterChanged(playerID, counterpartPlayerID, relationEpoch uint64, mutation friendpb.FriendMutationKind) int
}

func (h *ShareHandler) SetFriendRosterNotifier(notifier FriendRosterNotifier) {
	h.notifier = notifier
}

func (h *ShareHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/share/create":
		h.handleCreate(w, r)
	case "/api/v1/share/resolve":
		h.handleResolve(w, r)
	case "/api/v1/share/accept":
		h.handleAccept(w, r)
	case "/api/v1/friends":
		h.handleListFriends(w, r)
	case "/api/v1/friends/opportunity":
		h.handleFriendOpportunity(w, r)
	case "/api/v1/friends/revoke":
		h.handleRevokeFriend(w, r)
	default:
		writeJSON(w, http.StatusNotFound, httpError{Error: "unknown share endpoint"})
	}
}

func (h *ShareHandler) identity(w http.ResponseWriter, r *http.Request) (*SessionBinding, bool) {
	if h.identities == nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthenticated", ErrorCode: "ERR_UNAUTHENTICATED"})
		return nil, false
	}
	cookie, err := r.Cookie(HTTPSessionCookieName)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthenticated", ErrorCode: "ERR_UNAUTHENTICATED"})
		return nil, false
	}
	binding, err := h.identities.AuthenticateHTTPSession(r.Context(), cookie.Value)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, httpError{Error: "unauthenticated", ErrorCode: "ERR_UNAUTHENTICATED"})
		return nil, false
	}
	return binding, true
}

func decodeFriendshipJSON(w http.ResponseWriter, r *http.Request, target any) error {
	if r == nil || r.Body == nil || target == nil {
		return errors.New("friendship request body required")
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxFriendshipJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (h *ShareHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	binding, ok := h.identity(w, r)
	if !ok {
		return
	}
	result, err := h.friendshipClient.CreateShare(r.Context(), friendshipservice.CreateShareInput{SubjectPlayerID: binding.PlayerID})
	if err != nil {
		log.Printf("share create failed player_id=%d err=%v", binding.PlayerID, err)
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "failed to create share token"})
		return
	}
	writeJSON(w, http.StatusOK, ShareCreateResponse{ShareToken: result.ShareToken, AcceptMode: result.AcceptMode})
}

func (h *ShareHandler) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" || len(token) > maxFriendshipShareTokenBytes {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "missing token"})
		return
	}
	result, err := h.friendshipClient.ResolveShare(r.Context(), friendshipservice.ResolveShareInput{ShareToken: token})
	if err != nil {
		if errors.Is(err, friendshipservice.ErrShareLinkInvalid) {
			writeJSON(w, http.StatusNotFound, httpError{Error: ErrShareLinkInvalid.Error(), ErrorCode: "ERR_SHARE_LINK_INVALID"})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "inviter profile unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, ShareResolveResponse{InviterPlayerId: strconv.FormatUint(result.InviterPlayerID, 10),
		InviterLabel: result.InviterProfile.GetDisplayName(), AvatarID: result.InviterProfile.GetAvatarId(),
		Level: result.InviterProfile.GetLevel(), AcceptMode: result.AcceptMode})
}

func (h *ShareHandler) handleAccept(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	binding, ok := h.identity(w, r)
	if !ok {
		return
	}
	var request ShareAcceptRequest
	if err := decodeFriendshipJSON(w, r, &request); err != nil || strings.TrimSpace(request.ShareToken) == "" ||
		len(strings.TrimSpace(request.ShareToken)) > maxFriendshipShareTokenBytes {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "bad request body"})
		return
	}
	result, err := h.friendshipClient.RedeemShare(r.Context(), friendshipservice.RedeemShareInput{
		RedeemerPlayerID: binding.PlayerID, ShareToken: request.ShareToken,
	})
	if err != nil {
		switch {
		case errors.Is(err, friendshipservice.ErrShareSelfInvite):
			writeJSON(w, http.StatusBadRequest, httpError{Error: ErrShareSelfInvite.Error(), ErrorCode: "ERR_SHARE_LINK_INVALID"})
		case errors.Is(err, friendshipservice.ErrShareLinkInvalid):
			writeJSON(w, http.StatusBadRequest, httpError{Error: ErrShareLinkInvalid.Error(), ErrorCode: "ERR_SHARE_LINK_INVALID"})
		case errors.Is(err, friend.ErrIdempotentConflict):
			writeJSON(w, http.StatusConflict, httpError{Error: err.Error(), ErrorCode: "ERR_IDEMPOTENCY_CONFLICT"})
		default:
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend authority unavailable"})
		}
		return
	}
	if result.Outcome.Status == friend.StatusRejected {
		if result.Outcome.Reason == friend.ReasonFriendLimitReached {
			writeJSON(w, http.StatusConflict, httpError{Error: ErrFriendLimit.Error(), ErrorCode: "ERR_FRIEND_LIMIT_REACHED"})
		} else {
			writeJSON(w, http.StatusConflict, httpError{Error: string(result.Outcome.Reason), ErrorCode: "ERR_FRIEND_PERMIT_INVALID"})
		}
		return
	}
	if result.Outcome.Status != friend.StatusSucceeded && result.Outcome.Status != friend.StatusNoChange {
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend authority unavailable"})
		return
	}
	if result.Pair.Changed && h.notifier != nil {
		h.notifier.NotifyFriendRosterChanged(binding.PlayerID, result.InviterPlayerID, result.Pair.RelationEpoch, friendpb.FriendMutationKind_ACCEPT_SHARE)
		h.notifier.NotifyFriendRosterChanged(result.InviterPlayerID, binding.PlayerID, result.Pair.RelationEpoch, friendpb.FriendMutationKind_ACCEPT_SHARE)
	}
	writeJSON(w, http.StatusOK, ShareAcceptResponse{
		TargetOwnerId: strconv.FormatUint(result.InviterPlayerID, 10),
		InviterLabel:  result.InviterProfile.GetDisplayName(),
		AvatarID:      result.InviterProfile.GetAvatarId(),
		Level:         result.InviterProfile.GetLevel(),
		RelationEpoch: strconv.FormatUint(result.Pair.RelationEpoch, 10),
		Changed:       result.Pair.Changed,
	})
}

type FriendTheftTargetResponse struct {
	PlotID      uint32 `json:"plot_id"`
	CropCycleID string `json:"crop_cycle_id"`
}

type FriendCardResponse struct {
	OwnerID              string                      `json:"owner_id"`
	DisplayName          string                      `json:"display_name"`
	AvatarID             uint32                      `json:"avatar_id"`
	Level                uint32                      `json:"level"`
	RelationID           string                      `json:"relation_id"`
	RelationEpoch        string                      `json:"relation_epoch"`
	ActionMask           uint32                      `json:"action_mask"`
	OpportunityStatus    string                      `json:"opportunity_status"`
	OpportunityRevision  string                      `json:"opportunity_revision"`
	TheftEligibleTargets []FriendTheftTargetResponse `json:"theft_eligible_targets"`
	ValidatedAtMs        int64                       `json:"validated_at_ms"`
}

type FriendListResponse struct {
	Friends        []FriendCardResponse `json:"friends"`
	NextCursor     string               `json:"next_cursor,omitempty"`
	RosterRevision string               `json:"roster_revision,omitempty"`
	ValidatedAtMs  int64                `json:"validated_at_ms,omitempty"`
}

func friendCardResponse(card friend.FriendCard) FriendCardResponse {
	status := "READY"
	if card.OpportunityStatus == friend.OpportunityReadUnknown {
		status = "UNKNOWN"
	}
	targets := make([]FriendTheftTargetResponse, 0, len(card.EligibleTheftTargets))
	for _, target := range card.EligibleTheftTargets {
		targets = append(targets, FriendTheftTargetResponse{PlotID: target.PlotID, CropCycleID: target.CropCycleID})
	}
	return FriendCardResponse{
		OwnerID: strconv.FormatUint(card.FriendPlayerID, 10), DisplayName: card.PublicProfile.GetDisplayName(),
		AvatarID: card.PublicProfile.GetAvatarId(), Level: card.PublicProfile.GetLevel(), RelationID: card.RelationID,
		RelationEpoch: strconv.FormatUint(card.RelationEpoch, 10), ActionMask: uint32(card.Action), OpportunityStatus: status,
		OpportunityRevision: strconv.FormatUint(card.OpportunityRevision, 10), TheftEligibleTargets: targets, ValidatedAtMs: card.ValidatedAtMs,
	}
}

func friendListResponse(page friend.FriendPage) FriendListResponse {
	response := FriendListResponse{Friends: make([]FriendCardResponse, 0, len(page.Friends)), NextCursor: page.NextCursor,
		RosterRevision: strconv.FormatUint(page.RosterRevision, 10), ValidatedAtMs: page.ValidatedAtMs}
	for _, card := range page.Friends {
		response.Friends = append(response.Friends, friendCardResponse(card))
	}
	return response
}

type FriendOpportunityResponse struct {
	Status string              `json:"status"`
	Friend *FriendCardResponse `json:"friend,omitempty"`
}

type FriendRevokeRequest struct {
	FriendOwnerID FlexibleUint64 `json:"friend_owner_id"`
	OperationID   string         `json:"operation_id"`
}

type FlexibleUint64 uint64

func (value *FlexibleUint64) UnmarshalJSON(data []byte) error {
	parsed, err := parseUint64JSON(data)
	if err != nil {
		return err
	}
	*value = FlexibleUint64(parsed)
	return nil
}

func (h *ShareHandler) handleListFriends(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	binding, ok := h.identity(w, r)
	if !ok {
		return
	}
	pageSize := friend.DefaultPageSize
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if len(cursor) > maxFriendshipCursorBytes {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "invalid friend cursor"})
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page_size")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 || parsed > friend.MaxPageSize {
			writeJSON(w, http.StatusBadRequest, httpError{Error: "invalid friend page_size"})
			return
		}
		pageSize = parsed
	}
	result, err := h.friendshipClient.GetFriendList(r.Context(), friendshipservice.GetFriendListInput{
		ViewerPlayerID: binding.PlayerID, Cursor: cursor, PageSize: pageSize,
	})
	if err != nil {
		if errors.Is(err, friend.ErrInvalidCursor) || errors.Is(err, friend.ErrRosterChanged) {
			writeJSON(w, http.StatusConflict, httpError{Error: "friend roster changed", ErrorCode: "ERR_FRIEND_ROSTER_CHANGED"})
		} else {
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend roster unavailable"})
		}
		return
	}
	writeJSON(w, http.StatusOK, friendListResponse(result.Page))
}
func (h *ShareHandler) handleFriendOpportunity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	binding, ok := h.identity(w, r)
	if !ok {
		return
	}
	hintedTargets, err := parseFriendOpportunityHintTargets(r, binding.PlayerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "invalid friend opportunity hint target"})
		return
	}
	result, err := h.friendshipClient.GetNextFriendOpportunity(r.Context(),
		friendshipservice.GetNextFriendOpportunityInput{ViewerPlayerID: binding.PlayerID, HintedTargets: hintedTargets})
	if err != nil {
		// Keep the failure classified instead of collapsing every backend
		// outcome into one 503: capacity gates answer 429 + Retry-After,
		// deadline shows its own code, and only plain unavailability stays 503.
		var httpStatus int
		errorCode := "FRIEND_OPPORTUNITY_UNAVAILABLE"
		message := "friend opportunity unavailable"
		switch {
		case errors.Is(err, friendshipservice.ErrCapacity):
			httpStatus = http.StatusTooManyRequests
			errorCode = "FRIEND_OPPORTUNITY_BUSY"
			message = "friend opportunity busy, retry later"
			w.Header().Set("Retry-After", "1")
		case errors.Is(err, context.DeadlineExceeded):
			httpStatus = http.StatusServiceUnavailable
			errorCode = "FRIEND_OPPORTUNITY_TIMEOUT"
			message = "friend opportunity timed out"
			w.Header().Set("Retry-After", "1")
		default:
			httpStatus = http.StatusServiceUnavailable
		}
		writeJSON(w, httpStatus, httpError{Error: message, ErrorCode: errorCode})
		return
	}
	switch result.Status {
	case friendshipservice.NextOpportunityFound:
		if result.Friend == nil {
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend opportunity unavailable"})
			return
		}
		card := friendCardResponse(*result.Friend)
		writeJSON(w, http.StatusOK, FriendOpportunityResponse{Status: "FOUND", Friend: &card})
	case friendshipservice.NextOpportunityNone:
		writeJSON(w, http.StatusOK, FriendOpportunityResponse{Status: "NONE"})
	case friendshipservice.NextOpportunityUnknown:
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend opportunity temporarily unavailable", ErrorCode: "FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend opportunity unavailable"})
	}
}

func parseFriendOpportunityHintTargets(r *http.Request, viewer uint64) ([]friendshipservice.FriendOpportunityHintedTarget, error) {
	if r == nil || viewer == 0 {
		return nil, errors.New("invalid opportunity request")
	}
	values := r.URL.Query()[friendOpportunityHintTargetQuery]
	if len(values) > friendshipservice.MaxFriendOpportunityHintedTargets {
		return nil, errors.New("too many opportunity hints")
	}
	targets := make([]friendshipservice.FriendOpportunityHintedTarget, 0, len(values))
	for _, value := range values {
		ownerText, revisionText, ok := strings.Cut(value, ":")
		if !ok || ownerText == "" || revisionText == "" || strings.Contains(revisionText, ":") {
			return nil, errors.New("malformed opportunity hint")
		}
		ownerID, ownerErr := strconv.ParseUint(ownerText, 10, 64)
		revision, revisionErr := strconv.ParseUint(revisionText, 10, 64)
		if ownerErr != nil || revisionErr != nil || ownerID == 0 || ownerID == viewer || revision == 0 {
			return nil, errors.New("malformed opportunity hint")
		}
		targets = append(targets, friendshipservice.FriendOpportunityHintedTarget{
			OwnerID: ownerID, OpportunityRevision: revision,
		})
	}
	return targets, nil
}
func (h *ShareHandler) handleRevokeFriend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, httpError{Error: "method not allowed"})
		return
	}
	binding, ok := h.identity(w, r)
	if !ok {
		return
	}
	var request FriendRevokeRequest
	if err := decodeFriendshipJSON(w, r, &request); err != nil ||
		!friend.ValidPair(binding.PlayerID, uint64(request.FriendOwnerID)) {
		writeJSON(w, http.StatusBadRequest, httpError{Error: "invalid friend pair"})
		return
	}
	friendID := uint64(request.FriendOwnerID)
	result, err := h.friendshipClient.DeleteFriend(r.Context(), friendshipservice.PairMutationInput{
		CallerPlayerID: binding.PlayerID,
		PeerPlayerID:   friendID,
		OperationID:    strings.TrimSpace(request.OperationID),
	})
	if err != nil {
		switch {
		case errors.Is(err, friendshipservice.ErrNotFriend):
			writeJSON(w, http.StatusNotFound, httpError{Error: "friend relationship not found"})
		case errors.Is(err, friend.ErrIdempotentConflict):
			writeJSON(w, http.StatusConflict, httpError{Error: err.Error(), ErrorCode: "ERR_FRIEND_PERMIT_INVALID"})
		default:
			writeJSON(w, http.StatusServiceUnavailable, httpError{Error: "friend authority unavailable"})
		}
		return
	}
	if result.Pair.Changed && h.notifier != nil {
		h.notifier.NotifyFriendRosterChanged(binding.PlayerID, friendID, result.Pair.RelationEpoch, friendpb.FriendMutationKind_REVOKE_RELATION)
		h.notifier.NotifyFriendRosterChanged(friendID, binding.PlayerID, result.Pair.RelationEpoch, friendpb.FriendMutationKind_REVOKE_RELATION)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"removed":        result.Pair.Changed,
		"relation_epoch": strconv.FormatUint(result.Pair.RelationEpoch, 10),
	})
}
