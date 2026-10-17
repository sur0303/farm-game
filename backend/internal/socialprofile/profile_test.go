package socialprofile

import (
	"testing"
	"time"
)

func TestCurrentProfileMatchesConfirmedI0(t *testing.T) {
	profile := Current()
	if profile.Version != "social-product-profile-v1" || profile.FriendLimitProfile != "friend-limit-v1" ||
		profile.EffectiveMaxFriends != 300 || profile.ShareTTLProfile != "share-token-ttl-v1" ||
		profile.ShareTokenTTL != 7*24*time.Hour || profile.ShareProjectionRetention != 24*time.Hour ||
		profile.ShareRedemptionClock != "REDIS_SERVER_TIME" {
		t.Fatalf("profile = %+v", profile)
	}
}

func TestResolveEnvironmentDefaultsAndBoundOverrides(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	}
	defaults, err := ResolveEnvironment(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if defaults.FriendLimitValue != "300" || defaults.ShareTokenTTLValue != "168h" {
		t.Fatalf("defaults = %+v", defaults)
	}
	profilesOnly, err := ResolveEnvironment(lookup(map[string]string{
		FriendLimitProfileEnv: "friend-limit-v1", ShareTokenTTLProfileEnv: "share-token-ttl-v1",
	}))
	if err != nil || profilesOnly != defaults {
		t.Fatalf("profile-only assembly = %+v, err=%v", profilesOnly, err)
	}
	bound, err := ResolveEnvironment(lookup(map[string]string{
		FriendLimitProfileEnv: "friend-limit-v1", FriendLimitEnv: "300",
		ShareTokenTTLProfileEnv: "share-token-ttl-v1", ShareTokenTTLEnv: "7d",
	}))
	if err != nil || bound != defaults {
		t.Fatalf("bound = %+v, err=%v", bound, err)
	}
}

func TestResolveEnvironmentFailsClosed(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	}
	tests := []map[string]string{
		{FriendLimitEnv: "300"},
		{FriendLimitProfileEnv: "friend-limit-v2"},
		{FriendLimitProfileEnv: "friend-limit-v1", FriendLimitEnv: "200"},
		{ShareTokenTTLEnv: "168h"},
		{ShareTokenTTLProfileEnv: "share-token-ttl-v2"},
		{ShareTokenTTLProfileEnv: "share-token-ttl-v1", ShareTokenTTLEnv: "24h"},
	}
	for _, values := range tests {
		if _, err := ResolveEnvironment(lookup(values)); err == nil {
			t.Errorf("values=%v err=%v", values, err)
		}
	}
}
