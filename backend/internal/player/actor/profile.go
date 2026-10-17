package actor

import (
	"errors"
	"strings"

	playerpb "farmgame/api/gen/player"

	"google.golang.org/protobuf/proto"
)

const (
	DefaultDisplayName        = "农场主"
	DefaultAvatarID    uint32 = 1
)

// ErrPublicProfileConflict is returned when a bootstrap caller attempts to
// bind a different non-empty display name to an already provisioned player.
// The profile is an authority-owned identity fact, not a client-side label.
var ErrPublicProfileConflict = errors.New("player actor: public profile display name conflict")

// ProvisionPublicProfile installs the durable profile defaults and, when this
// is the first Web bootstrap, binds the supplied normalized display name.
// Empty request values never overwrite an existing value. Avatar changes are
// likewise only accepted while the stored avatar is still absent, which keeps
// repeated login/bootstrap calls idempotent.
func ProvisionPublicProfile(record *playerpb.PlayerRecord, displayName string, avatarID uint32) error {
	if record == nil || record.PlayerId == 0 {
		return errors.New("player actor: player profile requires player record")
	}
	displayName = strings.TrimSpace(displayName)
	profile := record.PublicProfile
	if profile == nil {
		profile = &playerpb.PlayerPublicProfile{}
		record.PublicProfile = profile
	}
	if strings.TrimSpace(profile.DisplayName) == "" {
		if displayName == "" {
			profile.DisplayName = DefaultDisplayName
		} else {
			profile.DisplayName = displayName
		}
	} else if displayName != "" && strings.TrimSpace(profile.DisplayName) != displayName {
		return ErrPublicProfileConflict
	} else {
		profile.DisplayName = strings.TrimSpace(profile.DisplayName)
	}
	if profile.AvatarId == 0 {
		if avatarID == 0 {
			profile.AvatarId = DefaultAvatarID
		} else {
			profile.AvatarId = avatarID
		}
	}
	return nil
}

// publicProfileNeedsWrite reports whether ProvisionPublicProfile would change
// durable fields. Login/bootstrap uses this to skip a second LoadAndClaim when
// ensureAuthority already linearized the profile.
func publicProfileNeedsWrite(record *playerpb.PlayerRecord, displayName string, avatarID uint32) (bool, error) {
	if record == nil {
		return false, errors.New("player actor: player profile requires player record")
	}
	clone := proto.Clone(record).(*playerpb.PlayerRecord)
	before := clone.GetPublicProfile()
	beforeName, beforeAvatar := "", uint32(0)
	if before != nil {
		beforeName, beforeAvatar = before.GetDisplayName(), before.GetAvatarId()
	}
	if err := ProvisionPublicProfile(clone, displayName, avatarID); err != nil {
		return false, err
	}
	after := clone.GetPublicProfile()
	return before == nil || beforeName != after.GetDisplayName() || beforeAvatar != after.GetAvatarId(), nil
}

// PublicProfileValues returns the safe effective values for a record. It does
// not mutate the record, so read paths cannot accidentally create a write.
func PublicProfileValues(record *playerpb.PlayerRecord) (displayName string, avatarID uint32) {
	if record != nil && record.PublicProfile != nil {
		displayName = strings.TrimSpace(record.PublicProfile.DisplayName)
		avatarID = record.PublicProfile.AvatarId
	}
	if displayName == "" {
		displayName = DefaultDisplayName
	}
	if avatarID == 0 {
		avatarID = DefaultAvatarID
	}
	return displayName, avatarID
}
