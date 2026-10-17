package ingress

import (
	"context"
	"testing"

	economypb "farmgame/api/gen/economy"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	playeractor "farmgame/internal/player/actor"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type profileModelBatchReader struct {
	rows    map[uint64][]byte
	records map[uint64][]byte
	v2      map[uint64]authoritystore.ModelState
	calls   int
}

func (r *profileModelBatchReader) BatchReadModelFields(_ context.Context, domain actorrouting.RouteDomain, ids []uint64, fields []string) ([]authoritystore.ModelFieldReadResult, error) {
	r.calls++
	if domain != actorrouting.DomainPlayer || len(fields) == 0 {
		return nil, authoritystore.ErrInvalidRequest
	}
	result := make([]authoritystore.ModelFieldReadResult, len(ids))
	for index, id := range ids {
		state := authoritystore.ModelState{}
		for _, field := range fields {
			switch field {
			case publicprofile.ModelField:
				if data, ok := r.rows[id]; ok {
					state[field] = append([]byte(nil), data...)
				}
			case "record":
				if data, ok := r.records[id]; ok {
					state[field] = append([]byte(nil), data...)
				}
			case "profile", "progression":
				if data, ok := r.v2[id][field]; ok {
					state[field] = append([]byte(nil), data...)
				}
			default:
				return nil, authoritystore.ErrInvalidRequest
			}
		}
		result[index] = authoritystore.ModelFieldReadResult{ActorID: id, Found: true, RecordVersion: 3, ModelLayout: authoritystore.ModelLayoutV1, ModelFields: state}
	}
	return result, nil
}

func TestGetPublicProfileUsesCompactModelWithoutLocalActorOwnership(t *testing.T) {
	projection, err := publicprofile.MarshalProjection(&publicpb.PublicProfileView{
		PlayerId: 96, DisplayName: "remote_farmer", AvatarId: 6, Level: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &profileModelBatchReader{rows: map[uint64][]byte{96: projection}}
	service := NewService(nil)
	service.SetPublicProfileModelReader(reader)

	response, err := service.GetPublicProfile(
		context.Background(),
		&logicrpcpb.GetPublicProfileRequest{PlayerId: 96},
	)
	if err != nil || reader.calls != 1 {
		t.Fatalf("response=%v calls=%d err=%v", response, reader.calls, err)
	}
	profile := response.GetPublicProfile()
	if profile.GetPlayerId() != 96 || profile.GetDisplayName() != "remote_farmer" ||
		profile.GetAvatarId() != 6 || profile.GetLevel() != 12 {
		t.Fatalf("profile=%v", profile)
	}
}

func TestBatchGetPublicProfilesNeverFallsBackFromV2ToRecord(t *testing.T) {
	reader := &profileModelBatchReader{rows: map[uint64][]byte{}, records: map[uint64][]byte{94: []byte("must-not-read")}}
	service := NewService(nil)
	service.SetPublicProfileModelReader(modelLayoutReader{profileModelBatchReader: reader, layout: authoritystore.ModelLayoutV2})
	_, err := service.BatchGetPublicProfiles(context.Background(), &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{94}})
	if status.Code(err) != codes.DataLoss || reader.calls != 1 {
		t.Fatalf("status=%v calls=%d err=%v", status.Code(err), reader.calls, err)
	}
}

func TestBatchGetPublicProfilesBuildsV2ViewFromProfileAndProgression(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 95, Progression: &economypb.Progression{Level: 11}}
	if err := playeractor.ProvisionPublicProfile(record, "v2_farmer", 5); err != nil {
		t.Fatal(err)
	}
	state, err := playeractormodelv2.Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	reader := &profileModelBatchReader{v2: map[uint64]authoritystore.ModelState{95: state}, records: map[uint64][]byte{95: []byte("must-not-read")}}
	service := NewService(nil)
	service.SetPublicProfileModelReader(modelLayoutReader{profileModelBatchReader: reader, layout: authoritystore.ModelLayoutV2})
	response, err := service.BatchGetPublicProfiles(context.Background(), &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{95}})
	if err != nil || reader.calls != 1 || len(response.GetPublicProfiles()) != 1 {
		t.Fatalf("response=%v calls=%d err=%v", response, reader.calls, err)
	}
	profile := response.PublicProfiles[0]
	if profile.GetPlayerId() != 95 || profile.GetDisplayName() != "v2_farmer" || profile.GetAvatarId() != 5 || profile.GetLevel() != 11 {
		t.Fatalf("profile=%v", profile)
	}
}

type modelLayoutReader struct {
	*profileModelBatchReader
	layout authoritystore.ModelLayoutVersion
}

func (r modelLayoutReader) BatchReadModelFields(ctx context.Context, domain actorrouting.RouteDomain, ids []uint64, fields []string) ([]authoritystore.ModelFieldReadResult, error) {
	rows, err := r.profileModelBatchReader.BatchReadModelFields(ctx, domain, ids, fields)
	for index := range rows {
		rows[index].ModelLayout = r.layout
	}
	return rows, err
}

func TestBatchGetPublicProfilesUsesCompactModelWithoutRegistry(t *testing.T) {
	first, err := publicprofile.MarshalProjection(&publicpb.PublicProfileView{PlayerId: 92, DisplayName: "farmer_two", AvatarId: 2, Level: 7})
	if err != nil {
		t.Fatal(err)
	}
	second, err := publicprofile.MarshalProjection(&publicpb.PublicProfileView{PlayerId: 91, DisplayName: "farmer_one", AvatarId: 1, Level: 4})
	if err != nil {
		t.Fatal(err)
	}
	reader := &profileModelBatchReader{rows: map[uint64][]byte{92: first, 91: second}}
	service := NewService(nil)
	service.SetPublicProfileModelReader(reader)
	response, err := service.BatchGetPublicProfiles(context.Background(), &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{92, 91}})
	if err != nil || reader.calls != 1 || len(response.GetPublicProfiles()) != 2 || response.PublicProfiles[0].GetPlayerId() != 92 || response.PublicProfiles[1].GetLevel() != 4 {
		t.Fatalf("response=%v calls=%d err=%v", response, reader.calls, err)
	}
}

func TestBatchGetPublicProfilesReadsLegacyRecordDirectlyWithoutRegistry(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 93, Progression: &economypb.Progression{Level: 8}}
	if err := playeractor.ProvisionPublicProfile(record, "legacy_farmer", 3); err != nil {
		t.Fatal(err)
	}
	recordBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	reader := &profileModelBatchReader{rows: map[uint64][]byte{}, records: map[uint64][]byte{93: recordBytes}}
	service := NewService(nil)
	service.SetPublicProfileModelReader(reader)
	response, err := service.BatchGetPublicProfiles(context.Background(), &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{93}})
	if err != nil || reader.calls != 2 || len(response.GetPublicProfiles()) != 1 ||
		response.PublicProfiles[0].GetDisplayName() != "legacy_farmer" || response.PublicProfiles[0].GetLevel() != 8 {
		t.Fatalf("response=%v calls=%d err=%v", response, reader.calls, err)
	}
}

func TestEnsureAndGetPublicProfileAreAuthorityOnly(t *testing.T) {
	registry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	service := NewService(registry)
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 91, BootstrapId: "web-bootstrap", DisplayName: "farmer_one", AvatarId: 1,
	}); err != nil {
		t.Fatal(err)
	}
	profile, err := service.GetPublicProfile(context.Background(), &logicrpcpb.GetPublicProfileRequest{PlayerId: 91})
	if err != nil {
		t.Fatal(err)
	}
	if got := profile.GetPublicProfile(); got.GetPlayerId() != 91 || got.GetDisplayName() != "farmer_one" || got.GetAvatarId() != 1 || got.GetLevel() != 1 {
		t.Fatalf("profile=%v", got)
	}
	_, err = service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 91, BootstrapId: "same-bootstrap", DisplayName: "other_name", AvatarId: 9,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflict status=%v err=%v", status.Code(err), err)
	}
	profile, err = service.GetPublicProfile(context.Background(), &logicrpcpb.GetPublicProfileRequest{PlayerId: 91})
	if err != nil || profile.GetPublicProfile().GetDisplayName() != "farmer_one" || profile.GetPublicProfile().GetAvatarId() != 1 {
		t.Fatalf("profile changed after conflict: %v err=%v", profile, err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 92, BootstrapId: "web-bootstrap-92", DisplayName: "farmer_two", AvatarId: 2,
	}); err != nil {
		t.Fatal(err)
	}
	batch, err := service.BatchGetPublicProfiles(context.Background(), &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: []uint64{92, 91}})
	if err != nil || len(batch.GetPublicProfiles()) != 2 || batch.PublicProfiles[0].GetPlayerId() != 92 ||
		batch.PublicProfiles[0].GetDisplayName() != "farmer_two" || batch.PublicProfiles[1].GetPlayerId() != 91 {
		t.Fatalf("batch=%v err=%v", batch, err)
	}
}
