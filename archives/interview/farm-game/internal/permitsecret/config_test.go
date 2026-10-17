package permitsecret_test

import (
	"errors"
	"testing"
	"time"

	"farmgame/internal/gateway/session"
	"farmgame/internal/permitsecret"
)

func environment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestAuthorityPermitSecretConfigurationFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		secret string
	}{
		{name: "missing"},
		{name: "short", secret: "too-short"},
		{name: "known public default", secret: "farm-game-dev-secret"},
		{name: "known long development default", secret: "farm-game-dev-secret-do-not-use-in-prod"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := permitsecret.Load(environment(map[string]string{
				permitsecret.ActorAuthorityModeEnvironment: "authority",
				permitsecret.SecretEnvironment:             test.secret,
			}))
			if err == nil {
				t.Fatal("invalid authority permit secret was accepted")
			}
		})
	}
}

func TestAuthorityPermitSecretAcceptsValidMaterial(t *testing.T) {
	config, err := permitsecret.Load(environment(map[string]string{
		permitsecret.ActorAuthorityModeEnvironment: "authority",
		permitsecret.SecretEnvironment:             "9dB!7vQ2#sL4@xN8%kR6^mT1&cW5*zP3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.Policy() != permitsecret.PolicyAuthority || config.InsecureDevelopment() {
		t.Fatalf("authority policy=%s insecure=%t", config.Policy(), config.InsecureDevelopment())
	}
}

func TestInsecureDevelopmentRequiresAnExplicitNonAuthorityChoice(t *testing.T) {
	config, err := permitsecret.Load(environment(map[string]string{
		permitsecret.SecretEnvironment:                    "local-demo-only",
		permitsecret.InsecureDevelopmentSwitchEnvironment: "1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.Policy() != permitsecret.PolicyInsecureDevelopment || !config.InsecureDevelopment() || !config.DevelopmentSwitchEnabled() {
		t.Fatalf("development policy=%s insecure=%t switch=%t", config.Policy(), config.InsecureDevelopment(), config.DevelopmentSwitchEnabled())
	}

	if _, err := permitsecret.Load(environment(map[string]string{
		permitsecret.ActorAuthorityModeEnvironment:        "authority",
		permitsecret.SecretEnvironment:                    "local-demo-only",
		permitsecret.InsecureDevelopmentSwitchEnvironment: "1",
	})); err == nil {
		t.Fatal("authority mode accepted the insecure development switch")
	}
}

func TestExplicitLegacyModeMakesInsecureAcceptanceVisible(t *testing.T) {
	config, err := permitsecret.Load(environment(map[string]string{
		permitsecret.ActorAuthorityModeEnvironment: "legacy",
		permitsecret.SecretEnvironment:             "legacy-demo-only",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.Policy() != permitsecret.PolicyExplicitLegacy || !config.InsecureDevelopment() {
		t.Fatalf("legacy policy=%s insecure=%t", config.Policy(), config.InsecureDevelopment())
	}
}

func TestGateAndFarmVerifierSecretMismatchRejectsPermit(t *testing.T) {
	gate, err := permitsecret.Load(environment(map[string]string{
		permitsecret.ActorAuthorityModeEnvironment: "authority",
		permitsecret.SecretEnvironment:             "G7!pQ2@vL9#xR4$mN8%tW3^kC6&zS1*e",
	}))
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := permitsecret.Load(environment(map[string]string{
		permitsecret.ActorAuthorityModeEnvironment: "authority",
		permitsecret.SecretEnvironment:             "V4@rM8!qT2#nK7$xP5%cL9^wZ3&sD6*f",
	}))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	permit := &session.Permit{
		PlayerID: 1, SessionID: "session", AuthorityEpoch: 1, ActiveSessionEpoch: 2,
		GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "connection",
		ConnectionGeneration: 1, RouteRevision: 1, RequestID: "request",
		Action: session.ActionReadOnly, ExpiresAt: now.Add(time.Minute),
	}
	permit.Sign(gate.Secret())
	if err := permit.Validate(now, verifier.Secret(), 1, 2); !errors.Is(err, session.ErrPermitInvalid) {
		t.Fatalf("mismatched verifier returned %v, want ErrPermitInvalid", err)
	}
}
