package loadtest

import (
	"testing"

	"farmgame/internal/socialprofile"
)

func TestDefaultFriendOpportunityABProfile(t *testing.T) {
	profile, evidence := DefaultFriendOpportunityABProfile()
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	if profile.FriendCounts[len(profile.FriendCounts)-1] != socialprofile.Current().EffectiveMaxFriends || profile.SameFarmViewers != 200 {
		t.Fatalf("capacity edges are not frozen: %+v", profile)
	}
	if evidence.Tool == "" || len(evidence.RequiredEvidence) < 5 || evidence.PassCriteria == "" {
		t.Fatalf("evidence contract is incomplete: %+v", evidence)
	}
}
