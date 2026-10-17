// Package permitsecret owns process-startup validation for the signing secret
// shared by FarmGate and permit-verifying backend services.
package permitsecret

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

const (
	// MinimumBytes is the production minimum for PERMIT_SECRET.
	MinimumBytes = 32

	SecretEnvironment                    = "PERMIT_SECRET"
	InsecureDevelopmentSwitchEnvironment = "PERMIT_SECRET_ALLOW_INSECURE_DEVELOPMENT"
	ActorAuthorityModeEnvironment        = "ACTOR_AUTHORITY_MODE"
)

// Policy describes why a startup secret was accepted. It is safe to log; it
// never contains secret bytes or a digest of them.
type Policy string

const (
	PolicyProductionDefault   Policy = "production-default"
	PolicyAuthority           Policy = "authority"
	PolicyExplicitLegacy      Policy = "explicit-legacy"
	PolicyInsecureDevelopment Policy = "insecure-development"
)

// Config is an immutable validated startup result.
type Config struct {
	secret              []byte
	policy              Policy
	insecureDevelopment bool
	developmentSwitchOn bool
}

func (c Config) Secret() []byte { return append([]byte(nil), c.secret...) }

func (c Config) Policy() Policy { return c.policy }

func (c Config) InsecureDevelopment() bool { return c.insecureDevelopment }

func (c Config) DevelopmentSwitchEnabled() bool { return c.developmentSwitchOn }

var knownDevelopmentLiterals = [][]byte{
	[]byte("farm-game-dev-secret"),
	[]byte("farm-game-dev-secret-do-not-use-in-prod"),
	[]byte("local-secret-do-not-use-in-prod"),
	[]byte("multigate-local-secret"),
	[]byte("stage5-permit-secret"),
	[]byte("s2-process-secret"),
}

// Load applies the same environment policy in every process that signs or
// verifies a shared permit. An unset actor mode is treated as production-safe
// for secret validation even though actor persistence retains its historical
// implicit legacy default. Insecure material therefore requires either an
// explicitly selected legacy/rollback mode or the clearly named development
// switch. Authority mode never permits the escape hatch.
func Load(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("permit secret: environment reader is required")
	}

	actorMode := strings.ToLower(strings.TrimSpace(getenv(ActorAuthorityModeEnvironment)))
	switch actorMode {
	case "", "authority", "legacy", "rollback":
	default:
		return Config{}, fmt.Errorf("permit secret: %s must be authority, legacy, or rollback", ActorAuthorityModeEnvironment)
	}

	developmentSwitch, err := parseSwitch(getenv(InsecureDevelopmentSwitchEnvironment))
	if err != nil {
		return Config{}, err
	}
	if actorMode == "authority" && developmentSwitch {
		return Config{}, fmt.Errorf("permit secret: %s is forbidden in authority mode", InsecureDevelopmentSwitchEnvironment)
	}

	raw := []byte(getenv(SecretEnvironment))
	if len(raw) == 0 {
		return Config{}, fmt.Errorf("permit secret: %s is required", SecretEnvironment)
	}

	policy := PolicyProductionDefault
	if actorMode == "authority" {
		policy = PolicyAuthority
	} else if actorMode == "legacy" || actorMode == "rollback" {
		policy = PolicyExplicitLegacy
	} else if developmentSwitch {
		policy = PolicyInsecureDevelopment
	}

	strictErr := ValidateProduction(raw)
	allowInsecure := policy == PolicyExplicitLegacy || policy == PolicyInsecureDevelopment
	if strictErr != nil && !allowInsecure {
		return Config{}, strictErr
	}

	return Config{
		secret:              append([]byte(nil), raw...),
		policy:              policy,
		insecureDevelopment: strictErr != nil || developmentSwitch,
		developmentSwitchOn: developmentSwitch,
	}, nil
}

// ValidateProduction checks only properties that can be established locally:
// byte length, exact known literals, and whitespace padding. Entropy still has
// to come from the deployment secret manager or a CSPRNG.
func ValidateProduction(secret []byte) error {
	if err := ValidateKeyMaterial(SecretEnvironment, secret); err != nil {
		return err
	}
	if !bytes.Equal(secret, bytes.TrimSpace(secret)) {
		return fmt.Errorf("permit secret: %s must not use surrounding whitespace as key material", SecretEnvironment)
	}
	return nil
}

// ValidateKeyMaterial applies the production length and known-literal checks
// to decoded binary signing keys such as HTTP Cookie KeyRing entries. It does
// not trim or otherwise reinterpret the key bytes.
func ValidateKeyMaterial(name string, secret []byte) error {
	if strings.TrimSpace(name) == "" {
		name = "signing key"
	}
	if len(secret) < MinimumBytes {
		return fmt.Errorf("permit secret: %s must contain at least %d bytes", name, MinimumBytes)
	}
	for _, known := range knownDevelopmentLiterals {
		if bytes.Equal(secret, known) {
			return fmt.Errorf("permit secret: %s is a known development literal", name)
		}
	}
	return nil
}

func parseSwitch(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "0", "false":
		return false, nil
	case "1", "true":
		return true, nil
	default:
		return false, fmt.Errorf("permit secret: %s must be 0/1 or false/true", InsecureDevelopmentSwitchEnvironment)
	}
}
