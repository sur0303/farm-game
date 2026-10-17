package ingress

import (
	"context"
	"time"

	"farmgame/internal/friend"
)

// friendActionAdmissionStub always admits CARE/THEFT so Authority-registry
// friend CARE can run without a live Pair repository.
type friendActionAdmissionStub struct{}

func (friendActionAdmissionStub) CommitFriendActionAdmission(_ context.Context, cmd friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return succeedFriendActionAdmission(cmd)
}

func (friendActionAdmissionStub) GetFriendActionAdmission(_ context.Context, cmd friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return succeedFriendActionAdmission(cmd)
}

func succeedFriendActionAdmission(cmd friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	relationID, err := friend.RelationIDFor(cmd.VisitorPlayerID, cmd.OwnerPlayerID)
	if err != nil {
		return friend.ActionAdmissionOutcome{}, err
	}
	nowMs := cmd.NowMs
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}
	ttl := cmd.PermitTTLMs
	if ttl <= 0 {
		ttl = friend.DefaultActionPermitTTL.Milliseconds()
	}
	return friend.ActionAdmissionOutcome{
		Outcome: friend.Outcome{
			Status:         friend.StatusSucceeded,
			Identity:       cmd.Identity,
			PayloadHash:    append([]byte(nil), cmd.PayloadHash...),
			CommittedAtMs:  nowMs,
			CommittedEpoch: 1,
		},
		Permit: friend.ActionPermitSnapshot{
			PermitID:                       "permit-" + cmd.Identity.OperationID,
			RelationID:                     relationID,
			RelationEpoch:                  1,
			VisitorPlayerID:                cmd.VisitorPlayerID,
			OwnerPlayerID:                  cmd.OwnerPlayerID,
			TargetFarmID:                   cmd.TargetFarmID,
			OperationID:                    cmd.Identity.OperationID,
			Scope:                          cmd.Scope,
			PayloadHash:                    append([]byte(nil), cmd.PayloadHash...),
			TargetBindingHash:              append([]byte(nil), cmd.TargetBindingHash...),
			IssuedAtAuthorityMs:            nowMs,
			ExpireAtAuthorityMs:            nowMs + ttl,
			ExecutionAcceptedAtAuthorityMs: nowMs,
		},
	}, nil
}
