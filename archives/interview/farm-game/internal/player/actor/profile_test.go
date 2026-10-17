package actor

import (
	"errors"
	"testing"

	playerpb "farmgame/api/gen/player"
)

func TestProvisionPublicProfileDefaultsAndBindsOnlyOnce(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 17}
	if err := ProvisionPublicProfile(record, "", 0); err != nil {
		t.Fatal(err)
	}
	if record.GetPublicProfile().GetDisplayName() != DefaultDisplayName || record.GetPublicProfile().GetAvatarId() != DefaultAvatarID {
		t.Fatalf("defaults=%v", record.GetPublicProfile())
	}
	if err := ProvisionPublicProfile(record, "different", 9); !errors.Is(err, ErrPublicProfileConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	if record.GetPublicProfile().GetDisplayName() != DefaultDisplayName || record.GetPublicProfile().GetAvatarId() != DefaultAvatarID {
		t.Fatalf("conflict changed profile=%v", record.GetPublicProfile())
	}
	if err := ProvisionPublicProfile(record, "", 9); err != nil {
		t.Fatal(err)
	}
	if record.GetPublicProfile().GetAvatarId() != DefaultAvatarID {
		t.Fatalf("empty request overwrote avatar=%v", record.GetPublicProfile())
	}
}

func TestPublicProfileNeedsWriteMatchesProvision(t *testing.T) {
	empty := &playerpb.PlayerRecord{PlayerId: 19}
	needs, err := publicProfileNeedsWrite(empty, "farmer_one", 2)
	if err != nil || !needs {
		t.Fatalf("empty needs=%v err=%v", needs, err)
	}
	if err := ProvisionPublicProfile(empty, "farmer_one", 2); err != nil {
		t.Fatal(err)
	}
	needs, err = publicProfileNeedsWrite(empty, "farmer_one", 2)
	if err != nil || needs {
		t.Fatalf("bound needs=%v err=%v", needs, err)
	}
	needs, err = publicProfileNeedsWrite(empty, "", 0)
	if err != nil || needs {
		t.Fatalf("repeat login needs=%v err=%v", needs, err)
	}
	_, err = publicProfileNeedsWrite(empty, "other", 2)
	if !errors.Is(err, ErrPublicProfileConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	missingAvatar := &playerpb.PlayerRecord{
		PlayerId:      20,
		PublicProfile: &playerpb.PlayerPublicProfile{DisplayName: "farmer_two"},
	}
	needs, err = publicProfileNeedsWrite(missingAvatar, "", 0)
	if err != nil || !needs {
		t.Fatalf("missing avatar needs=%v err=%v", needs, err)
	}
}

func TestProvisionPublicProfileUsesWebNameOnUnprovisionedRecord(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 18}
	if err := ProvisionPublicProfile(record, "farmer_one", 1); err != nil {
		t.Fatal(err)
	}
	if record.GetPublicProfile().GetDisplayName() != "farmer_one" || record.GetPublicProfile().GetAvatarId() != 1 {
		t.Fatalf("profile=%v", record.GetPublicProfile())
	}
}
