package ingress

import (
	"context"
	"errors"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/publicprofile"
)

type PlayerParticipant interface {
	ReserveItem(context.Context, *logicrpcpb.ReserveItemRequest) (*logicrpcpb.ReservationResponse, error)
	CommitReservation(context.Context, *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error)
	ReleaseReservation(context.Context, *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error)
	GrantItem(context.Context, *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error)
	ApplyReward(context.Context, *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error)
	GetPrivateState(context.Context, *logicrpcpb.GetPrivateStateRequest) (*logicrpcpb.GetPrivateStateResponse, error)
}

type publicProfileParticipant interface {
	GetPublicProfile(context.Context, *logicrpcpb.GetPublicProfileRequest) (*logicrpcpb.GetPublicProfileResponse, error)
}

type participantProfileReader struct{ participant publicProfileParticipant }

func (r participantProfileReader) Read(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	response, err := r.participant.GetPublicProfile(ctx, &logicrpcpb.GetPublicProfileRequest{PlayerId: playerID})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("farm ingress: nil public profile response")
	}
	profile := response.GetPublicProfile()
	if err := publicprofile.Validate(profile, playerID); err != nil {
		return nil, err
	}
	return &publicpb.PublicProfileView{
		PlayerId: profile.GetPlayerId(), DisplayName: profile.GetDisplayName(),
		AvatarId: profile.GetAvatarId(), Level: profile.GetLevel(),
	}, nil
}

type grpcPlayerParticipant struct {
	client logicrpcpb.PlayerParticipantServiceClient
}

func NewGRPCPlayerParticipant(client logicrpcpb.PlayerParticipantServiceClient) PlayerParticipant {
	if client == nil {
		return nil
	}
	return &grpcPlayerParticipant{client: client}
}

func (p *grpcPlayerParticipant) ReserveItem(ctx context.Context, req *logicrpcpb.ReserveItemRequest) (*logicrpcpb.ReservationResponse, error) {
	return p.client.ReserveItem(ctx, req)
}

func (p *grpcPlayerParticipant) CommitReservation(ctx context.Context, req *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	return p.client.CommitReservation(ctx, req)
}

func (p *grpcPlayerParticipant) ReleaseReservation(ctx context.Context, req *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	return p.client.ReleaseReservation(ctx, req)
}

func (p *grpcPlayerParticipant) GrantItem(ctx context.Context, req *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error) {
	return p.client.GrantItem(ctx, req)
}

func (p *grpcPlayerParticipant) ApplyReward(ctx context.Context, req *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error) {
	return p.client.ApplyReward(ctx, req)
}

func (p *grpcPlayerParticipant) GetPrivateState(ctx context.Context, req *logicrpcpb.GetPrivateStateRequest) (*logicrpcpb.GetPrivateStateResponse, error) {
	return p.client.GetPrivateState(ctx, req)
}

func (p *grpcPlayerParticipant) GetPublicProfile(ctx context.Context, req *logicrpcpb.GetPublicProfileRequest) (*logicrpcpb.GetPublicProfileResponse, error) {
	return p.client.GetPublicProfile(ctx, req)
}
