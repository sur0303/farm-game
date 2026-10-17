package publicprofile

import (
	"context"
	"testing"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/grpc"
)

type fakeParticipantClient struct {
	logicrpcpb.PlayerParticipantServiceClient
	response      *logicrpcpb.GetPublicProfileResponse
	batchResponse *logicrpcpb.BatchGetPublicProfilesResponse
}

func (c fakeParticipantClient) GetPublicProfile(context.Context, *logicrpcpb.GetPublicProfileRequest, ...grpc.CallOption) (*logicrpcpb.GetPublicProfileResponse, error) {
	return c.response, nil
}

func (c fakeParticipantClient) BatchGetPublicProfiles(context.Context, *logicrpcpb.BatchGetPublicProfilesRequest, ...grpc.CallOption) (*logicrpcpb.BatchGetPublicProfilesResponse, error) {
	return c.batchResponse, nil
}

func TestDirectReaderUsesOneBatchRPCAndValidatesOrder(t *testing.T) {
	reader := NewDirectReader(fakeParticipantClient{batchResponse: &logicrpcpb.BatchGetPublicProfilesResponse{PublicProfiles: []*publicpb.PublicProfileView{
		{PlayerId: 7, DisplayName: "seven", AvatarId: 1, Level: 2},
		{PlayerId: 8, DisplayName: "eight", AvatarId: 2, Level: 3},
	}}})
	profiles, err := reader.BatchGetPublicProfiles(context.Background(), []uint64{7, 8})
	if err != nil || profiles[7].GetDisplayName() != "seven" || profiles[8].GetDisplayName() != "eight" {
		t.Fatalf("profiles=%v err=%v", profiles, err)
	}
}

func TestDirectReaderValidatesTheSamePublicProjection(t *testing.T) {
	reader := NewDirectReader(fakeParticipantClient{response: &logicrpcpb.GetPublicProfileResponse{
		PublicProfile: &publicpb.PublicProfileView{PlayerId: 7, DisplayName: "farmer", AvatarId: 3, Level: 4},
	}})
	profile, err := reader.Read(context.Background(), 7)
	if err != nil || profile.GetDisplayName() != "farmer" || profile.GetAvatarId() != 3 || profile.GetLevel() != 4 {
		t.Fatalf("profile=%v err=%v", profile, err)
	}
	bad := NewDirectReader(fakeParticipantClient{response: &logicrpcpb.GetPublicProfileResponse{
		PublicProfile: &publicpb.PublicProfileView{PlayerId: 8, DisplayName: "farmer", AvatarId: 1, Level: 1},
	}})
	if _, err := bad.Read(context.Background(), 7); err == nil {
		t.Fatal("expected player_id mismatch")
	}
}
