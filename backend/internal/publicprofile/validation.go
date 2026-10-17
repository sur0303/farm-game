package publicprofile

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	publicpb "farmgame/api/gen/public"
)

var (
	ErrInvalidProfile   = errors.New("public profile is malformed")
	ErrPlayerIDMismatch = errors.New("public profile player_id mismatch")
)

// Validate checks the server-owned profile contract and the identity binding
// expected by the caller. It rejects incomplete data instead of synthesizing
// a display name from an owner id.
func Validate(profile *publicpb.PublicProfileView, expectedPlayerID uint64) error {
	if profile == nil || profile.GetPlayerId() == 0 || profile.GetPlayerId() != expectedPlayerID {
		return ErrPlayerIDMismatch
	}
	name := strings.TrimSpace(profile.GetDisplayName())
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 128 {
		return fmt.Errorf("%w: display_name", ErrInvalidProfile)
	}
	if profile.GetAvatarId() <= 0 {
		return fmt.Errorf("%w: avatar_id", ErrInvalidProfile)
	}
	if profile.GetLevel() <= 0 {
		return fmt.Errorf("%w: level", ErrInvalidProfile)
	}
	return nil
}

func cloneValidated(profile *publicpb.PublicProfileView, expectedPlayerID uint64) (*publicpb.PublicProfileView, error) {
	if err := Validate(profile, expectedPlayerID); err != nil {
		return nil, err
	}
	return &publicpb.PublicProfileView{
		PlayerId: profile.GetPlayerId(), DisplayName: strings.TrimSpace(profile.GetDisplayName()),
		AvatarId: profile.GetAvatarId(), Level: profile.GetLevel(),
	}, nil
}
