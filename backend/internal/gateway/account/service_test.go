package account

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newFastTestService(t *testing.T) *Service {
	t.Helper()
	service, err := NewService(NewMemoryStore(20_000), Config{
		PBKDF2Iterations: 1_000,
		MaxFailures:      3,
		FailureWindow:    time.Minute,
		LockDuration:     time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestRegisterAndAuthenticatePersistentIdentity(t *testing.T) {
	service := newFastTestService(t)
	account, err := service.Register(context.Background(), " Farmer_One ", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if account.Username != "farmer_one" || account.PlayerID != 20_001 {
		t.Fatalf("account=%+v", account)
	}
	playerID, err := service.Authenticate(context.Background(), "FARMER_ONE", "correct horse battery")
	if err != nil || playerID != account.PlayerID {
		t.Fatalf("authenticate=(%d,%v)", playerID, err)
	}
	if _, err := service.Register(context.Background(), "farmer_one", "another password"); !errors.Is(err, ErrAccountExists) {
		t.Fatalf("duplicate err=%v", err)
	}
}

func TestInvalidCredentialIsUniformAndRateLimited(t *testing.T) {
	service := newFastTestService(t)
	if _, err := service.Register(context.Background(), "farmer_two", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if _, err := service.Authenticate(context.Background(), "farmer_two", "wrong password"); !errors.Is(err, ErrInvalidCredential) {
			t.Fatalf("failure %d err=%v", index, err)
		}
	}
	if _, err := service.Authenticate(context.Background(), "farmer_two", "correct horse battery"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("locked err=%v", err)
	}
	if _, err := service.Authenticate(context.Background(), "missing_user", "wrong password"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("missing user leaked err=%v", err)
	}
}

func TestCredentialValidation(t *testing.T) {
	if _, err := NormalizeUsername("ab"); !errors.Is(err, ErrInvalidUsername) {
		t.Fatalf("short username err=%v", err)
	}
	if _, err := NormalizeUsername("bad-name"); !errors.Is(err, ErrInvalidUsername) {
		t.Fatalf("punctuated username err=%v", err)
	}
	if err := ValidatePassword("short"); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("short password err=%v", err)
	}
}

func TestChangePasswordAndDisable(t *testing.T) {
	service := newFastTestService(t)
	created, err := service.Register(context.Background(), "farmer_security", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	playerID, err := service.ChangePassword(context.Background(), "farmer_security", "correct horse battery", "new correct horse")
	if err != nil || playerID != created.PlayerID {
		t.Fatalf("change password=(%d,%v)", playerID, err)
	}
	if _, err := service.Authenticate(context.Background(), "farmer_security", "correct horse battery"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("old password err=%v", err)
	}
	if _, err := service.Authenticate(context.Background(), "farmer_security", "new correct horse"); err != nil {
		t.Fatal(err)
	}
	disabledID, err := service.Disable(context.Background(), "farmer_security")
	if err != nil || disabledID != created.PlayerID {
		t.Fatalf("disable=(%d,%v)", disabledID, err)
	}
	if _, err := service.Authenticate(context.Background(), "farmer_security", "new correct horse"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("disabled login err=%v", err)
	}
	if _, err := service.Disable(context.Background(), "farmer_security"); err != nil {
		t.Fatalf("idempotent disable err=%v", err)
	}
}

func TestSuccessfulLoginProgressivelyUpgradesPasswordKDF(t *testing.T) {
	store := NewMemoryStore(40_000)
	legacy, err := NewService(store, Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	created, err := legacy.Register(context.Background(), "upgrade_me", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	before := created.PasswordVerifier
	current, err := NewService(store, Config{PBKDF2Iterations: 2_000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.Authenticate(context.Background(), "upgrade_me", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	upgraded, err := store.Get(context.Background(), "upgrade_me")
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.PasswordVerifier == before {
		t.Fatal("successful login did not upgrade the historical verifier")
	}
	if iterations, ok := verifierIterations(upgraded.PasswordVerifier); !ok || iterations != 2_000 {
		t.Fatalf("upgraded verifier=%q", upgraded.PasswordVerifier)
	}
	if upgraded.PasswordKDFVersion != "pbkdf2-sha256" ||
		upgraded.PasswordKDFParams != "iterations=2000;key_bytes=32" ||
		upgraded.PasswordChangedAt.Before(upgraded.CreatedAt) {
		t.Fatalf("credential metadata=%+v", upgraded)
	}
}

func TestDisabledAccountUsesUniformCredentialFailure(t *testing.T) {
	store := NewMemoryStore(50_000)
	service, err := NewService(store, Config{PBKDF2Iterations: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Register(context.Background(), "disabled_user", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	credential := store.accounts["disabled_user"]
	credential.AccountStatus = AccountStatusDisabled
	store.accounts["disabled_user"] = credential
	store.mu.Unlock()
	if _, err := service.Authenticate(context.Background(), "disabled_user", "correct horse battery"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("disabled account leaked status: %v", err)
	}
}
