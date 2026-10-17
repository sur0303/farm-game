// Package socialprofile owns the versioned product decisions shared by the
// Friendship authority, the HTTP compatibility path, deployment assembly and
// verification tooling.
package socialprofile

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	FriendLimitProfileEnv   = "FRIEND_LIMIT_PROFILE"
	FriendLimitEnv          = "FRIEND_LIMIT"
	ShareTokenTTLProfileEnv = "SHARE_TOKEN_TTL_PROFILE"
	ShareTokenTTLEnv        = "SHARE_TOKEN_TTL"
)

//go:embed social-product-profile-v1.json
var manifestJSON []byte

type manifest struct {
	Schema          string `json:"schema"`
	ProfileVersion  string `json:"profile_version"`
	DecisionStatus  string `json:"decision_status"`
	SourceOwnership struct {
		SingleBackendSource               bool   `json:"single_backend_source"`
		RuntimePackage                    string `json:"runtime_package"`
		EnvironmentBinding                string `json:"environment_binding"`
		UnversionedNumericOverrideAllowed bool   `json:"unversioned_numeric_override_allowed"`
	} `json:"source_ownership"`
	FriendLimit struct {
		Profile             string `json:"profile"`
		EffectiveMaxFriends int    `json:"effective_max_friends"`
		ContractError       string `json:"contract_error"`
		AuthorityReason     string `json:"authority_reason"`
		HTTPErrorCode       string `json:"http_error_code"`
	} `json:"friend_limit"`
	ShareTokenTTL struct {
		Profile                          string `json:"profile"`
		DefaultTTL                       string `json:"default_ttl"`
		DefaultTTLMs                     int64  `json:"default_ttl_ms"`
		ProjectionRetentionAfterExpiryMs int64  `json:"projection_retention_after_expiry_ms"`
		RedemptionClock                  string `json:"redemption_clock"`
		ValidWhen                        string `json:"valid_when"`
		ExpiryError                      string `json:"expiry_error"`
	} `json:"share_token_ttl"`
	PublicEntrypoints struct {
		HTTPSPrimary      []string `json:"https_primary"`
		WSSPrimary        []string `json:"wss_primary"`
		WSSSocialDisabled []string `json:"wss_social_disabled"`
		BlockUnblock      struct {
			Status            string `json:"status"`
			H5Entry           bool   `json:"h5_entry"`
			HTTPRoute         bool   `json:"http_route"`
			WSSEnabled        bool   `json:"wss_enabled"`
			ShowcaseR1Blocker bool   `json:"showcase_r1_blocker"`
			F05Implemented    bool   `json:"f05_implemented"`
		} `json:"block_unblock"`
	} `json:"public_entrypoints"`
	FinalEntry string `json:"final_entry"`
}

// Profile is the immutable runtime view of the embedded product profile.
type Profile struct {
	Version                  string
	FriendLimitProfile       string
	EffectiveMaxFriends      int
	ShareTTLProfile          string
	ShareTokenTTL            time.Duration
	ShareProjectionRetention time.Duration
	ShareRedemptionClock     string
}

var current = mustLoad()

func mustLoad() Profile {
	var decoded manifest
	if err := json.Unmarshal(manifestJSON, &decoded); err != nil {
		panic(fmt.Sprintf("decode social product profile: %v", err))
	}
	if decoded.Schema != "farm.social-product-profile/v1" ||
		decoded.ProfileVersion == "" || decoded.DecisionStatus != "I0_USER_CONFIRMED" ||
		!decoded.SourceOwnership.SingleBackendSource || decoded.SourceOwnership.RuntimePackage != "internal/socialprofile" ||
		decoded.SourceOwnership.EnvironmentBinding != "cmd/farm-gate/main.go" || decoded.SourceOwnership.UnversionedNumericOverrideAllowed ||
		decoded.FriendLimit.Profile != "friend-limit-v1" || decoded.FriendLimit.EffectiveMaxFriends != 300 ||
		decoded.FriendLimit.ContractError != "ERR_FRIEND_LIMIT_REACHED" || decoded.FriendLimit.AuthorityReason != "FRIEND_LIMIT_REACHED" ||
		decoded.FriendLimit.HTTPErrorCode != "ERR_FRIEND_LIMIT_REACHED" ||
		decoded.ShareTokenTTL.Profile != "share-token-ttl-v1" || decoded.ShareTokenTTL.DefaultTTL != "7d" ||
		decoded.ShareTokenTTL.DefaultTTLMs != int64(7*24*time.Hour/time.Millisecond) ||
		decoded.ShareTokenTTL.ProjectionRetentionAfterExpiryMs != int64(24*time.Hour/time.Millisecond) ||
		decoded.ShareTokenTTL.RedemptionClock != "REDIS_SERVER_TIME" ||
		decoded.ShareTokenTTL.ValidWhen != "now_ms < expires_at_ms" || decoded.ShareTokenTTL.ExpiryError != "SHARE_TOKEN_EXPIRED" ||
		!slices.Equal(decoded.PublicEntrypoints.HTTPSPrimary, []string{
			"POST /api/v1/share/create", "POST /api/v1/share/accept", "GET /api/v1/friends",
			"POST /api/v1/friends/revoke", "GET /api/v1/friends/opportunity",
		}) ||
		!slices.Equal(decoded.PublicEntrypoints.WSSPrimary, []string{"JOIN_FARM", "CARE", "THEFT"}) ||
		!slices.Equal(decoded.PublicEntrypoints.WSSSocialDisabled, []string{
			"GENERATE_SHARE_TOKEN", "REDEEM_SHARE", "GET_FRIEND_LIST", "DELETE_FRIEND",
			"BLOCK_PLAYER", "UNBLOCK_PLAYER", "GET_NEXT_FRIEND_OPPORTUNITY",
		}) ||
		decoded.PublicEntrypoints.BlockUnblock.Status != "DEFERRED_NOT_EXPOSED" ||
		decoded.PublicEntrypoints.BlockUnblock.H5Entry || decoded.PublicEntrypoints.BlockUnblock.HTTPRoute ||
		decoded.PublicEntrypoints.BlockUnblock.WSSEnabled || decoded.PublicEntrypoints.BlockUnblock.ShowcaseR1Blocker ||
		decoded.PublicEntrypoints.BlockUnblock.F05Implemented || decoded.FinalEntry == "" {
		panic("social product profile violates the confirmed I0 contract")
	}
	return Profile{
		Version: decoded.ProfileVersion, FriendLimitProfile: decoded.FriendLimit.Profile,
		EffectiveMaxFriends:      decoded.FriendLimit.EffectiveMaxFriends,
		ShareTTLProfile:          decoded.ShareTokenTTL.Profile,
		ShareTokenTTL:            time.Duration(decoded.ShareTokenTTL.DefaultTTLMs) * time.Millisecond,
		ShareProjectionRetention: time.Duration(decoded.ShareTokenTTL.ProjectionRetentionAfterExpiryMs) * time.Millisecond,
		ShareRedemptionClock:     decoded.ShareTokenTTL.RedemptionClock,
	}
}

// Current returns the immutable profile loaded from the machine-readable
// source embedded in this package.
func Current() Profile { return current }

// Environment is the validated farm-gate environment projection. Values are
// canonicalized before the legacy gateway config parser sees them.
type Environment struct {
	FriendLimitProfile string
	FriendLimitValue   string
	ShareTTLProfile    string
	ShareTokenTTLValue string
}

// ResolveEnvironment applies the profile when no override is present. A
// legacy value override is accepted only when its profile variable is also
// explicit and the value agrees with that versioned profile.
func ResolveEnvironment(lookup func(string) (string, bool)) (Environment, error) {
	if lookup == nil {
		return Environment{}, errors.New("social profile environment lookup is required")
	}
	friendProfileValue, friendProfileSet := explicit(lookup, FriendLimitProfileEnv)
	friendLimitValue, friendLimitSet := explicit(lookup, FriendLimitEnv)
	shareProfileValue, shareProfileSet := explicit(lookup, ShareTokenTTLProfileEnv)
	shareTTLValue, shareTTLSet := explicit(lookup, ShareTokenTTLEnv)

	if friendLimitSet && !friendProfileSet {
		return Environment{}, fmt.Errorf("%s requires explicit %s=%s", FriendLimitEnv, FriendLimitProfileEnv, current.FriendLimitProfile)
	}
	if shareTTLSet && !shareProfileSet {
		return Environment{}, fmt.Errorf("%s requires explicit %s=%s", ShareTokenTTLEnv, ShareTokenTTLProfileEnv, current.ShareTTLProfile)
	}
	if friendProfileSet && friendProfileValue != current.FriendLimitProfile {
		return Environment{}, fmt.Errorf("unsupported %s %q", FriendLimitProfileEnv, friendProfileValue)
	}
	if shareProfileSet && shareProfileValue != current.ShareTTLProfile {
		return Environment{}, fmt.Errorf("unsupported %s %q", ShareTokenTTLProfileEnv, shareProfileValue)
	}
	if friendLimitSet {
		limit, err := strconv.Atoi(friendLimitValue)
		if err != nil || limit != current.EffectiveMaxFriends {
			return Environment{}, fmt.Errorf("%s=%q is inconsistent with %s=%s", FriendLimitEnv, friendLimitValue, FriendLimitProfileEnv, current.FriendLimitProfile)
		}
	}
	if shareTTLSet {
		ttl, err := parseTTL(shareTTLValue)
		if err != nil || ttl != current.ShareTokenTTL {
			return Environment{}, fmt.Errorf("%s=%q is inconsistent with %s=%s", ShareTokenTTLEnv, shareTTLValue, ShareTokenTTLProfileEnv, current.ShareTTLProfile)
		}
	}

	return Environment{
		FriendLimitProfile: current.FriendLimitProfile,
		FriendLimitValue:   strconv.Itoa(current.EffectiveMaxFriends),
		ShareTTLProfile:    current.ShareTTLProfile,
		ShareTokenTTLValue: canonicalDuration(current.ShareTokenTTL),
	}, nil
}

func explicit(lookup func(string) (string, bool), key string) (string, bool) {
	value, ok := lookup(key)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(value), true
}

func parseTTL(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 32)
		if err != nil || days <= 0 {
			return 0, errors.New("TTL days must be positive")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(value)
}

func canonicalDuration(value time.Duration) string {
	if value > 0 && value%time.Hour == 0 {
		return strconv.FormatInt(int64(value/time.Hour), 10) + "h"
	}
	return value.String()
}

// BindProcessEnvironment validates explicit overrides and installs canonical
// values for gatewayapp's existing config loader. It must run before Run.
func BindProcessEnvironment() (Profile, error) {
	resolved, err := ResolveEnvironment(os.LookupEnv)
	if err != nil {
		return Profile{}, err
	}
	for key, value := range map[string]string{
		FriendLimitProfileEnv:   resolved.FriendLimitProfile,
		FriendLimitEnv:          resolved.FriendLimitValue,
		ShareTokenTTLProfileEnv: resolved.ShareTTLProfile,
		ShareTokenTTLEnv:        resolved.ShareTokenTTLValue,
	} {
		if err := os.Setenv(key, value); err != nil {
			return Profile{}, fmt.Errorf("set %s: %w", key, err)
		}
	}
	return current, nil
}
