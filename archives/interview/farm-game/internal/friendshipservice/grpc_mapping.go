package friendshipservice

import (
	"fmt"

	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
	"farmgame/internal/publicprofile"
)

func outcomeToRPC(value Outcome) *friendshiprpcpb.Outcome {
	result := &friendshiprpcpb.Outcome{Status: outcomeStatusToRPC(value.Status), Reason: string(value.Reason),
		CommittedEpoch: value.CommittedEpoch, Detail: value.Detail,
		CanonicalPayloadHash:   append([]byte(nil), value.PayloadHash...),
		CommittedAtAuthorityMs: value.CommittedAtMs}
	if value.Identity.Valid() {
		result.Identity = identityToRPC(value.Identity)
	}
	return result
}

func outcomeFromRPC(value *friendshiprpcpb.Outcome) (Outcome, error) {
	if value == nil {
		return Outcome{}, fmt.Errorf("%w: missing outcome", ErrMalformedResponse)
	}
	status, err := outcomeStatusFromRPC(value.GetStatus())
	if err != nil {
		return Outcome{}, err
	}
	reason := friend.ReasonCode(value.GetReason())
	if !validReason(reason) {
		return Outcome{}, fmt.Errorf("%w: outcome reason", ErrMalformedResponse)
	}
	var identity friend.CommitIdentity
	if value.GetIdentity() != nil {
		identity, err = identityFromRPC(value.GetIdentity())
		if err != nil {
			return Outcome{}, err
		}
	}
	return Outcome{Status: status, Reason: reason, Identity: identity, CommittedEpoch: value.GetCommittedEpoch(),
		CommittedAtMs: value.GetCommittedAtAuthorityMs(), Detail: value.GetDetail(),
		PayloadHash: append([]byte(nil), value.GetCanonicalPayloadHash()...)}, nil
}

func validReason(value friend.ReasonCode) bool {
	switch value {
	case friend.ReasonNone, friend.ReasonNotFriends, friend.ReasonAlreadyFriends, friend.ReasonAlreadyBlocked,
		friend.ReasonAlreadyUnblocked, friend.ReasonBlocked, friend.ReasonSelfAdd, friend.ReasonFriendLimitReached,
		friend.ReasonShareTokenExpired, friend.ReasonShareTokenRevoked, friend.ReasonShareTokenStaleGeneration,
		friend.ReasonShareTokenPurposeMismatch, friend.ReasonShareTokenPredatesRelationDel,
		friend.ReasonReactivationRotateRequired, friend.ReasonRelationReactivationPending,
		friend.ReasonRelationCleanupPending, friend.ReasonDomainEpochConflict, friend.ReasonBarrierRevisionMismatch,
		friend.ReasonVisitNotPermitted, friend.ReasonRelationAlreadyTerminated:
		return true
	default:
		return false
	}
}

func outcomeStatusToRPC(value friend.StatusClass) friendshiprpcpb.OutcomeStatus {
	switch value {
	case friend.StatusSucceeded:
		return friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_SUCCEEDED
	case friend.StatusRejected:
		return friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_REJECTED
	case friend.StatusNoChange:
		return friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_NO_CHANGE
	default:
		return friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_UNSPECIFIED
	}
}

func outcomeStatusFromRPC(value friendshiprpcpb.OutcomeStatus) (friend.StatusClass, error) {
	switch value {
	case friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_SUCCEEDED:
		return friend.StatusSucceeded, nil
	case friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_REJECTED:
		return friend.StatusRejected, nil
	case friendshiprpcpb.OutcomeStatus_OUTCOME_STATUS_NO_CHANGE:
		return friend.StatusNoChange, nil
	default:
		return friend.StatusUnspecified, fmt.Errorf("%w: outcome status", ErrMalformedResponse)
	}
}

func relationStatusToRPC(value friend.RelationStatus) friendshiprpcpb.RelationStatus {
	switch value {
	case friend.RelationStatusNone:
		return friendshiprpcpb.RelationStatus_RELATION_STATUS_NONE
	case friend.RelationStatusActive:
		return friendshiprpcpb.RelationStatus_RELATION_STATUS_ACTIVE
	case friend.RelationStatusDeleted:
		return friendshiprpcpb.RelationStatus_RELATION_STATUS_DELETED
	default:
		return friendshiprpcpb.RelationStatus_RELATION_STATUS_UNSPECIFIED
	}
}

func relationStatusFromRPC(value friendshiprpcpb.RelationStatus) (friend.RelationStatus, error) {
	switch value {
	case friendshiprpcpb.RelationStatus_RELATION_STATUS_NONE:
		return friend.RelationStatusNone, nil
	case friendshiprpcpb.RelationStatus_RELATION_STATUS_ACTIVE:
		return friend.RelationStatusActive, nil
	case friendshiprpcpb.RelationStatus_RELATION_STATUS_DELETED:
		return friend.RelationStatusDeleted, nil
	default:
		return friend.RelationStatusNone, fmt.Errorf("%w: relation status", ErrMalformedResponse)
	}
}

func pairToRPC(value PairState) *friendshiprpcpb.PairState {
	return &friendshiprpcpb.PairState{RelationId: value.RelationID, RelationEpoch: value.RelationEpoch,
		Status: relationStatusToRPC(value.Status), Changed: value.Changed}
}

func pairFromRPC(value *friendshiprpcpb.PairState) (PairState, error) {
	if value == nil {
		return PairState{}, fmt.Errorf("%w: missing pair", ErrMalformedResponse)
	}
	status, err := relationStatusFromRPC(value.GetStatus())
	if err != nil {
		return PairState{}, err
	}
	result := PairState{RelationID: value.GetRelationId(), RelationEpoch: value.GetRelationEpoch(), Status: status, Changed: value.GetChanged()}
	if status == friend.RelationStatusNone && (result.RelationID != "" || result.RelationEpoch != 0) {
		return PairState{}, fmt.Errorf("%w: NONE pair has facts", ErrMalformedResponse)
	}
	if status != friend.RelationStatusNone && (result.RelationID == "" || result.RelationEpoch == 0) {
		return PairState{}, fmt.Errorf("%w: incomplete pair", ErrMalformedResponse)
	}
	return result, nil
}

func profileToRPC(value *publicpb.PublicProfileView) *friendshiprpcpb.PublicProfile {
	if value == nil {
		return nil
	}
	return &friendshiprpcpb.PublicProfile{PlayerId: value.GetPlayerId(), DisplayName: value.GetDisplayName(),
		AvatarId: value.GetAvatarId(), Level: value.GetLevel()}
}

func profileFromRPC(value *friendshiprpcpb.PublicProfile, expected uint64) (*publicpb.PublicProfileView, error) {
	if value == nil {
		return nil, fmt.Errorf("%w: public profile", ErrMalformedResponse)
	}
	result := &publicpb.PublicProfileView{PlayerId: value.GetPlayerId(), DisplayName: value.GetDisplayName(),
		AvatarId: value.GetAvatarId(), Level: value.GetLevel()}
	if err := publicprofile.Validate(result, expected); err != nil {
		return nil, fmt.Errorf("%w: public profile: %v", ErrMalformedResponse, err)
	}
	return result, nil
}

func cardToRPC(value friend.FriendCard) *friendshiprpcpb.FriendCard {
	result := &friendshiprpcpb.FriendCard{OwnerPlayerId: value.FriendPlayerID, PublicProfile: profileToRPC(value.PublicProfile),
		RelationId: value.RelationID, RelationEpoch: value.RelationEpoch, ActionMask: uint32(value.Action),
		OpportunityRevision: value.OpportunityRevision, ValidatedAtMs: value.ValidatedAtMs}
	if value.OpportunityStatus == friend.OpportunityReadReady {
		result.OpportunityStatus = friendshiprpcpb.OpportunityStatus_OPPORTUNITY_STATUS_READY
	} else {
		result.OpportunityStatus = friendshiprpcpb.OpportunityStatus_OPPORTUNITY_STATUS_UNKNOWN
	}
	for _, target := range value.EligibleTheftTargets {
		result.EligibleTheftTargets = append(result.EligibleTheftTargets,
			&friendshiprpcpb.FriendTheftTarget{PlotId: target.PlotID, CropCycleId: target.CropCycleID})
	}
	return result
}

func cardFromRPC(value *friendshiprpcpb.FriendCard) (friend.FriendCard, error) {
	if value == nil || value.GetOwnerPlayerId() == 0 || value.GetRelationId() == "" || value.GetRelationEpoch() == 0 || value.GetValidatedAtMs() <= 0 {
		return friend.FriendCard{}, fmt.Errorf("%w: friend card identity", ErrMalformedResponse)
	}
	profile, err := profileFromRPC(value.GetPublicProfile(), value.GetOwnerPlayerId())
	if err != nil {
		return friend.FriendCard{}, err
	}
	result := friend.FriendCard{FriendPlayerID: value.GetOwnerPlayerId(), PublicProfile: profile,
		RelationID: value.GetRelationId(), RelationEpoch: value.GetRelationEpoch(), Action: friend.ActionMask(value.GetActionMask()),
		OpportunityRevision: value.GetOpportunityRevision(), ValidatedAtMs: value.GetValidatedAtMs()}
	knownActions := uint32(friend.ActionCanSteal | friend.ActionCanWater | friend.ActionCanRemovePest | friend.ActionCanVisit)
	if value.GetActionMask() & ^knownActions != 0 {
		return friend.FriendCard{}, fmt.Errorf("%w: action mask", ErrMalformedResponse)
	}
	switch value.GetOpportunityStatus() {
	case friendshiprpcpb.OpportunityStatus_OPPORTUNITY_STATUS_READY:
		result.OpportunityStatus = friend.OpportunityReadReady
	case friendshiprpcpb.OpportunityStatus_OPPORTUNITY_STATUS_UNKNOWN:
		result.OpportunityStatus = friend.OpportunityReadUnknown
	default:
		return friend.FriendCard{}, fmt.Errorf("%w: opportunity status", ErrMalformedResponse)
	}
	for _, target := range value.GetEligibleTheftTargets() {
		if target == nil || target.GetPlotId() == 0 || target.GetCropCycleId() == "" {
			return friend.FriendCard{}, fmt.Errorf("%w: theft target", ErrMalformedResponse)
		}
		result.EligibleTheftTargets = append(result.EligibleTheftTargets,
			friend.FriendTheftTarget{PlotID: target.GetPlotId(), CropCycleID: target.GetCropCycleId()})
	}
	if result.OpportunityStatus != friend.OpportunityReadReady && len(result.EligibleTheftTargets) != 0 {
		return friend.FriendCard{}, fmt.Errorf("%w: UNKNOWN card has targets", ErrMalformedResponse)
	}
	if result.OpportunityStatus == friend.OpportunityReadReady &&
		(((result.Action & friend.ActionCanSteal) != 0) != (len(result.EligibleTheftTargets) > 0)) {
		return friend.FriendCard{}, fmt.Errorf("%w: theft action/target mismatch", ErrMalformedResponse)
	}
	return result, nil
}

func actionScopeToRPC(value friend.ActionScope) friendshiprpcpb.ActionScope {
	switch value {
	case friend.ActionScopeVisitFarm:
		return friendshiprpcpb.ActionScope_ACTION_SCOPE_VISIT_FARM
	case friend.ActionScopeCare:
		return friendshiprpcpb.ActionScope_ACTION_SCOPE_CARE
	case friend.ActionScopeTheft:
		return friendshiprpcpb.ActionScope_ACTION_SCOPE_THEFT
	default:
		return friendshiprpcpb.ActionScope_ACTION_SCOPE_UNSPECIFIED
	}
}

func actionScopeFromRPC(value friendshiprpcpb.ActionScope) (friend.ActionScope, error) {
	switch value {
	case friendshiprpcpb.ActionScope_ACTION_SCOPE_VISIT_FARM:
		return friend.ActionScopeVisitFarm, nil
	case friendshiprpcpb.ActionScope_ACTION_SCOPE_CARE:
		return friend.ActionScopeCare, nil
	case friendshiprpcpb.ActionScope_ACTION_SCOPE_THEFT:
		return friend.ActionScopeTheft, nil
	default:
		return friend.ActionScopeUnspecified, fmt.Errorf("%w: action scope", ErrMalformedResponse)
	}
}

func identityToRPC(value friend.CommitIdentity) *friendshiprpcpb.CommitIdentity {
	return &friendshiprpcpb.CommitIdentity{SubjectPlayerId: value.SubjectPlayerID, CommandType: value.CommandType,
		OperationId: value.OperationID, WorkAddressId: value.WorkAddressID,
		ParticipantInvocationId: value.ParticipantInvocationID, ParticipantStep: value.ParticipantStep}
}

func identityFromRPC(value *friendshiprpcpb.CommitIdentity) (friend.CommitIdentity, error) {
	if value == nil {
		return friend.CommitIdentity{}, fmt.Errorf("%w: commit identity", ErrMalformedResponse)
	}
	result := friend.CommitIdentity{SubjectPlayerID: value.GetSubjectPlayerId(), CommandType: value.GetCommandType(),
		OperationID: value.GetOperationId(), WorkAddressID: value.GetWorkAddressId(),
		ParticipantInvocationID: value.GetParticipantInvocationId(), ParticipantStep: value.GetParticipantStep()}
	if !result.Valid() {
		return friend.CommitIdentity{}, fmt.Errorf("%w: commit identity shape", ErrMalformedResponse)
	}
	return result, nil
}

func admissionCommandToRPC(value friend.ActionAdmissionCommand) *friendshiprpcpb.CommitFriendActionAdmissionRequest {
	return &friendshiprpcpb.CommitFriendActionAdmissionRequest{Identity: identityToRPC(value.Identity),
		CanonicalPayloadHash: append([]byte(nil), value.PayloadHash...), VisitorPlayerId: value.VisitorPlayerID,
		OwnerPlayerId: value.OwnerPlayerID, TargetFarmId: value.TargetFarmID, ActionScope: actionScopeToRPC(value.Scope),
		TargetBindingHash: append([]byte(nil), value.TargetBindingHash...), AuthorityNowMs: value.NowMs,
		PermitTtlMs: value.PermitTTLMs, WorkflowTtlMs: value.WorkflowTTLMs}
}

func admissionCommandFromRPC(value *friendshiprpcpb.CommitFriendActionAdmissionRequest) (friend.ActionAdmissionCommand, error) {
	if value == nil {
		return friend.ActionAdmissionCommand{}, fmt.Errorf("%w: admission request", ErrInvalidArgument)
	}
	identity, err := identityFromRPC(value.GetIdentity())
	if err != nil {
		return friend.ActionAdmissionCommand{}, fmt.Errorf("%w: admission identity: %v", ErrInvalidArgument, err)
	}
	scope, err := actionScopeFromRPC(value.GetActionScope())
	if err != nil || (scope != friend.ActionScopeCare && scope != friend.ActionScopeTheft) {
		return friend.ActionAdmissionCommand{}, fmt.Errorf("%w: admission scope", ErrInvalidArgument)
	}
	result := friend.ActionAdmissionCommand{Identity: identity, PayloadHash: append([]byte(nil), value.GetCanonicalPayloadHash()...),
		VisitorPlayerID: value.GetVisitorPlayerId(), OwnerPlayerID: value.GetOwnerPlayerId(), TargetFarmID: value.GetTargetFarmId(),
		Scope: scope, TargetBindingHash: append([]byte(nil), value.GetTargetBindingHash()...), NowMs: value.GetAuthorityNowMs(),
		PermitTTLMs: value.GetPermitTtlMs(), WorkflowTTLMs: value.GetWorkflowTtlMs()}
	if result.VisitorPlayerID == 0 || result.OwnerPlayerID == 0 || result.TargetFarmID == 0 ||
		len(result.PayloadHash) == 0 || len(result.TargetBindingHash) == 0 {
		return friend.ActionAdmissionCommand{}, fmt.Errorf("%w: admission fields", ErrInvalidArgument)
	}
	return result, nil
}

func actionPermitToRPC(value friend.ActionPermitSnapshot) *friendshiprpcpb.ActionPermit {
	return &friendshiprpcpb.ActionPermit{PermitId: value.PermitID, RelationId: value.RelationID, RelationEpoch: value.RelationEpoch,
		VisitorPlayerId: value.VisitorPlayerID, OwnerPlayerId: value.OwnerPlayerID, TargetFarmId: value.TargetFarmID,
		OperationId: value.OperationID, ActionScope: actionScopeToRPC(value.Scope),
		CanonicalPayloadHash: append([]byte(nil), value.PayloadHash...), TargetBindingHash: append([]byte(nil), value.TargetBindingHash...),
		IssuedAtAuthorityMs: value.IssuedAtAuthorityMs, ExpireAtAuthorityMs: value.ExpireAtAuthorityMs,
		ExecutionAcceptedAtAuthorityMs: value.ExecutionAcceptedAtAuthorityMs}
}

func actionPermitFromRPC(value *friendshiprpcpb.ActionPermit) (friend.ActionPermitSnapshot, error) {
	if value == nil {
		return friend.ActionPermitSnapshot{}, fmt.Errorf("%w: missing action permit", ErrMalformedResponse)
	}
	scope, err := actionScopeFromRPC(value.GetActionScope())
	if err != nil {
		return friend.ActionPermitSnapshot{}, err
	}
	result := friend.ActionPermitSnapshot{PermitID: value.GetPermitId(), RelationID: value.GetRelationId(), RelationEpoch: value.GetRelationEpoch(),
		VisitorPlayerID: value.GetVisitorPlayerId(), OwnerPlayerID: value.GetOwnerPlayerId(), TargetFarmID: value.GetTargetFarmId(),
		OperationID: value.GetOperationId(), Scope: scope, PayloadHash: append([]byte(nil), value.GetCanonicalPayloadHash()...),
		TargetBindingHash: append([]byte(nil), value.GetTargetBindingHash()...), IssuedAtAuthorityMs: value.GetIssuedAtAuthorityMs(),
		ExpireAtAuthorityMs: value.GetExpireAtAuthorityMs(), ExecutionAcceptedAtAuthorityMs: value.GetExecutionAcceptedAtAuthorityMs()}
	if err := friend.VerifyAdmittedPermit(result); err != nil {
		return friend.ActionPermitSnapshot{}, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	return result, nil
}
