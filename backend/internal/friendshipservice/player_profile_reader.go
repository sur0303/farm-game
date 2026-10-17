package friendshipservice

import (
	"context"
	"errors"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/publicprofile"
)

type playerProfileService interface {
	GetPublicProfile(context.Context, *logicrpcpb.GetPublicProfileRequest) (*logicrpcpb.GetPublicProfileResponse, error)
	BatchGetPublicProfiles(context.Context, *logicrpcpb.BatchGetPublicProfilesRequest) (*logicrpcpb.BatchGetPublicProfilesResponse, error)
}

// LocalPlayerProfileReader invokes player-svr's narrow public projection
// directly; it never dispatches a Friendship command through PlayerActor.
type LocalPlayerProfileReader struct{ service playerProfileService }

func NewLocalPlayerProfileReader(service playerProfileService) (*LocalPlayerProfileReader, error) {
	if service == nil {
		return nil, ErrInvalidArgument
	}
	return &LocalPlayerProfileReader{service: service}, nil
}

func (r *LocalPlayerProfileReader) Read(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	if r == nil || r.service == nil || playerID == 0 {
		return nil, ErrInvalidArgument
	}
	response, err := r.service.GetPublicProfile(ctx, &logicrpcpb.GetPublicProfileRequest{PlayerId: playerID})
	if err != nil {
		return nil, err
	}
	if response == nil || publicprofile.Validate(response.GetPublicProfile(), playerID) != nil {
		return nil, ErrMalformedResponse
	}
	return response.GetPublicProfile(), nil
}

func (r *LocalPlayerProfileReader) BatchGetPublicProfiles(ctx context.Context, playerIDs []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	if r == nil || r.service == nil || len(playerIDs) == 0 || len(playerIDs) > publicprofile.MaxBatchProfiles {
		return nil, ErrInvalidArgument
	}
	response, err := r.service.BatchGetPublicProfiles(ctx,
		&logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: append([]uint64(nil), playerIDs...)})
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.GetPublicProfiles()) != len(playerIDs) {
		return nil, errors.New("friendship service: public profile batch cardinality mismatch")
	}
	result := make(map[uint64]*publicpb.PublicProfileView, len(playerIDs))
	for index, playerID := range playerIDs {
		profile := response.GetPublicProfiles()[index]
		if _, duplicate := result[playerID]; duplicate || publicprofile.Validate(profile, playerID) != nil {
			return nil, ErrMalformedResponse
		}
		result[playerID] = profile
	}
	return result, nil
}
