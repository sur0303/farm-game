package actor

import (
	"errors"

	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
)

// PublicProfileProjection derives the bounded public view from one immutable
// PlayerRecord snapshot. Readers of legacy record-only model state reuse this
// function so compatibility cannot drift from the write-side projection.
func PublicProfileProjection(record *playerpb.PlayerRecord) (*publicpb.PublicProfileView, error) {
	if record == nil || record.GetPlayerId() == 0 {
		return nil, errors.New("player actor: public profile projection source is incomplete")
	}
	displayName, avatarID := PublicProfileValues(record)
	level := record.GetProgression().GetLevel()
	if level < 0 {
		return nil, errors.New("player actor: public profile level is malformed")
	}
	if level == 0 {
		level = 1
	}
	return &publicpb.PublicProfileView{
		PlayerId: record.GetPlayerId(), DisplayName: displayName, AvatarId: avatarID, Level: uint32(level),
	}, nil
}

func buildPlayerAuthorityModelState(record *playerpb.PlayerRecord) (authoritystore.ModelState, error) {
	recordBytes, err := marshal(record)
	if err != nil {
		return nil, err
	}
	profile, err := PublicProfileProjection(record)
	if err != nil {
		return nil, err
	}
	profileBytes, err := publicprofile.MarshalProjection(profile)
	if err != nil {
		return nil, err
	}
	return authoritystore.ModelState{
		authority.RecordModelField: recordBytes,
		publicprofile.ModelField:   profileBytes,
	}, nil
}
