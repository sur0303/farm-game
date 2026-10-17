package publicprofile

import (
	"context"
	"errors"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
)

// DirectReader uses the fixed PLAYER_ADDR composition.
type DirectReader struct {
	client logicrpcpb.PlayerParticipantServiceClient
}

func NewDirectReader(client logicrpcpb.PlayerParticipantServiceClient) *DirectReader {
	if client == nil {
		return nil
	}
	return &DirectReader{client: client}
}

func (r *DirectReader) BatchGetPublicProfiles(ctx context.Context, playerIDs []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	if r == nil || r.client == nil || len(playerIDs) == 0 || len(playerIDs) > MaxBatchProfiles {
		return nil, errors.New("public profile direct batch reader unavailable")
	}
	response, err := r.client.BatchGetPublicProfiles(ctx, &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: append([]uint64(nil), playerIDs...)})
	if err != nil {
		return nil, err
	}
	if response == nil || len(response.PublicProfiles) != len(playerIDs) {
		return nil, errors.New("public profile direct batch cardinality mismatch")
	}
	result := make(map[uint64]*publicpb.PublicProfileView, len(playerIDs))
	for i, playerID := range playerIDs {
		profile, validateErr := cloneValidated(response.PublicProfiles[i], playerID)
		if validateErr != nil {
			return nil, validateErr
		}
		result[playerID] = profile
	}
	return result, nil
}

func (r *DirectReader) Read(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	if r == nil || r.client == nil || playerID == 0 {
		return nil, errors.New("public profile direct reader unavailable")
	}
	response, err := r.client.GetPublicProfile(ctx, &logicrpcpb.GetPublicProfileRequest{PlayerId: playerID})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("public profile direct reader returned nil response")
	}
	return cloneValidated(response.GetPublicProfile(), playerID)
}
