package publicprofile

import (
	"errors"
	"testing"

	publicpb "farmgame/api/gen/public"
)

func TestValidateRejectsIdentityAndIncompleteProfiles(t *testing.T) {
	valid := &publicpb.PublicProfileView{PlayerId: 9, DisplayName: "农场主", AvatarId: 1, Level: 1}
	if err := Validate(valid, 9); err != nil {
		t.Fatal(err)
	}
	if err := Validate(valid, 10); !errors.Is(err, ErrPlayerIDMismatch) {
		t.Fatalf("mismatch err=%v", err)
	}
	for _, value := range []*publicpb.PublicProfileView{
		{PlayerId: 9, AvatarId: 1, Level: 1},
		{PlayerId: 9, DisplayName: "x", Level: 1},
		{PlayerId: 9, DisplayName: "x", AvatarId: 1},
	} {
		if err := Validate(value, 9); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("profile=%v err=%v", value, err)
		}
	}
}
