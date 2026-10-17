package fieldv2

import (
	"fmt"
	"strconv"
)

const (
	PlayerConversionEnabledEnv = "ACTOR_MODEL_FIELD_V2_PLAYER_CONVERSION_ENABLED"
	PlayerCohortPermyriadEnv   = "ACTOR_MODEL_FIELD_V2_PLAYER_COHORT_PERMYRIAD"
	FarmConversionEnabledEnv   = "ACTOR_MODEL_FIELD_V2_FARM_CONVERSION_ENABLED"
	FarmCohortPermyriadEnv     = "ACTOR_MODEL_FIELD_V2_FARM_COHORT_PERMYRIAD"
)

// ParseConversionPolicy reads the bounded runtime conversion gate. Complete
// absence is the checked-in default (both domains disabled, zero cohort).
// Once a variable is explicitly present, empty/whitespace/alternate boolean
// spellings and out-of-range cohorts are configuration errors; startup must
// not silently substitute a permissive value.
func ParseConversionPolicy(lookup func(string) (string, bool)) (ConversionPolicy, error) {
	if lookup == nil {
		return ConversionPolicy{}, fmt.Errorf("%w: nil configuration lookup", ErrMalformed)
	}
	player, err := parseDomainPolicy(lookup, PlayerConversionEnabledEnv, PlayerCohortPermyriadEnv)
	if err != nil {
		return ConversionPolicy{}, err
	}
	farm, err := parseDomainPolicy(lookup, FarmConversionEnabledEnv, FarmCohortPermyriadEnv)
	if err != nil {
		return ConversionPolicy{}, err
	}
	return ConversionPolicy{Player: player, Farm: farm}, nil
}

// ProductionConversionPolicy is the player-svr / farm-svr gate. Production
// always writes sparse v2 fields. Env vars are still parsed so a typo fails
// startup; ENABLED=false no longer keeps the full-record v1 write path.
func ProductionConversionPolicy(lookup func(string) (string, bool)) (ConversionPolicy, error) {
	if _, err := ParseConversionPolicy(lookup); err != nil {
		return ConversionPolicy{}, err
	}
	return FullConversionPolicy(), nil
}

func parseDomainPolicy(lookup func(string) (string, bool), enabledName, cohortName string) (DomainPolicy, error) {
	var policy DomainPolicy
	if raw, present := lookup(enabledName); present {
		switch raw {
		case "true":
			policy.ConversionEnabled = true
		case "false":
		default:
			return DomainPolicy{}, fmt.Errorf("%w: %s must be true or false", ErrMalformed, enabledName)
		}
	}
	cohortPresent := false
	if raw, present := lookup(cohortName); present {
		cohortPresent = true
		if raw == "" {
			return DomainPolicy{}, fmt.Errorf("%w: %s is empty", ErrMalformed, cohortName)
		}
		parsed, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || parsed > uint64(CohortBasis) {
			return DomainPolicy{}, fmt.Errorf("%w: %s must be 0..%d", ErrMalformed, cohortName, CohortBasis)
		}
		policy.CohortPermyriad = uint16(parsed)
	}
	if policy.ConversionEnabled && (!cohortPresent || policy.CohortPermyriad == 0) {
		return DomainPolicy{}, fmt.Errorf("%w: enabled %s requires a non-zero %s", ErrMalformed, enabledName, cohortName)
	}
	return policy, nil
}
