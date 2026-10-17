// Package account implements the standalone Web username/password boundary.
//
// It is deliberately separate from auth.Service: account verifies a human
// credential and resolves it to a stable player_id; auth.Service then creates
// the normal session epoch, owner bootstrap and one-time WebSocket ticket.
package account

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"crypto/pbkdf2"
)

var (
	ErrAccountExists     = errors.New("account: username already exists")
	ErrAccountNotFound   = errors.New("account: username not found")
	ErrInvalidCredential = errors.New("account: invalid username or password")
	ErrRateLimited       = errors.New("account: too many failed attempts")
	ErrInvalidUsername   = errors.New("account: username must be 3-24 letters, digits, or underscores")
	ErrInvalidPassword   = errors.New("account: password must contain 8-128 characters")
	ErrSamePassword      = errors.New("account: new password must differ from current password")
	ErrAccountConflict   = errors.New("account: credential changed concurrently")
)

const (
	DefaultPBKDF2Iterations = 600_000
	passwordSaltBytes       = 16
	passwordKeyBytes        = 32
	AccountStatusActive     = "ACTIVE"
	AccountStatusDisabled   = "DISABLED"
)

type Credential struct {
	Username           string
	PlayerID           uint64
	PasswordVerifier   string
	PasswordKDFVersion string
	PasswordKDFParams  string
	AccountStatus      string
	CreatedAt          time.Time
	PasswordChangedAt  time.Time
}

type Store interface {
	Create(ctx context.Context, username, passwordVerifier string, now time.Time) (*Credential, error)
	Get(ctx context.Context, username string) (*Credential, error)
	UpdatePasswordVerifier(ctx context.Context, username, expectedVerifier, nextVerifier string, changedAt time.Time) (bool, error)
	UpdateAccountStatus(ctx context.Context, username, status string) error
}

type Config struct {
	PBKDF2Iterations int
	MaxFailures      int
	FailureWindow    time.Duration
	LockDuration     time.Duration
}

type failureState struct {
	count       int
	windowStart time.Time
	lockedUntil time.Time
}

type Service struct {
	store              Store
	pbkdf2Iterations   int
	maxFailures        int
	failureWindow      time.Duration
	lockDuration       time.Duration
	dummyVerifier      string
	now                func() time.Time
	mu                 sync.Mutex
	failuresByUsername map[string]failureState
}

func NewService(store Store, cfg Config) (*Service, error) {
	if store == nil {
		return nil, errors.New("account: store required")
	}
	if cfg.PBKDF2Iterations <= 0 {
		cfg.PBKDF2Iterations = DefaultPBKDF2Iterations
	}
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 5
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 5 * time.Minute
	}
	if cfg.LockDuration <= 0 {
		cfg.LockDuration = time.Minute
	}
	s := &Service{
		store: store, pbkdf2Iterations: cfg.PBKDF2Iterations,
		maxFailures: cfg.MaxFailures, failureWindow: cfg.FailureWindow,
		lockDuration: cfg.LockDuration, now: time.Now,
		failuresByUsername: make(map[string]failureState),
	}
	// A fixed dummy record equalizes most of the missing-user and bad-password
	// paths without storing or logging the submitted password.
	dummy, err := s.hashPassword("not-a-real-account-password", make([]byte, passwordSaltBytes))
	if err != nil {
		return nil, err
	}
	s.dummyVerifier = dummy
	return s, nil
}

func (s *Service) Register(ctx context.Context, username, password string) (*Credential, error) {
	normalized, err := NormalizeUsername(username)
	if err != nil {
		return nil, err
	}
	if err := ValidatePassword(password); err != nil {
		return nil, err
	}
	salt := make([]byte, passwordSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("account: create salt: %w", err)
	}
	verifier, err := s.hashPassword(password, salt)
	if err != nil {
		return nil, err
	}
	return s.store.Create(ctx, normalized, verifier, s.now())
}

// ChangePassword verifies the current password, CAS-updates the verifier, and
// returns the player_id so callers can unconditionally revoke every session.
func (s *Service) ChangePassword(ctx context.Context, username, currentPassword, newPassword string) (uint64, error) {
	normalized, err := NormalizeUsername(username)
	if err != nil {
		return 0, ErrInvalidCredential
	}
	if err := ValidatePassword(currentPassword); err != nil {
		return 0, ErrInvalidCredential
	}
	if err := ValidatePassword(newPassword); err != nil {
		return 0, err
	}
	if currentPassword == newPassword {
		return 0, ErrSamePassword
	}
	now := s.now()
	if s.isLocked(normalized, now) {
		return 0, ErrRateLimited
	}
	credential, getErr := s.store.Get(ctx, normalized)
	verifier := s.dummyVerifier
	if getErr == nil && credential != nil {
		verifier = credential.PasswordVerifier
	} else if getErr != nil && !errors.Is(getErr, ErrAccountNotFound) {
		return 0, getErr
	}
	matches := s.verifyPassword(currentPassword, verifier)
	if getErr != nil || credential == nil || !matches || credential.PlayerID == 0 ||
		credential.AccountStatus != AccountStatusActive {
		s.recordFailure(normalized, now)
		return 0, ErrInvalidCredential
	}
	salt := make([]byte, passwordSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return 0, fmt.Errorf("account: create salt: %w", err)
	}
	nextVerifier, err := s.hashPassword(newPassword, salt)
	if err != nil {
		return 0, err
	}
	updated, err := s.store.UpdatePasswordVerifier(ctx, normalized, credential.PasswordVerifier, nextVerifier, now)
	if err != nil {
		return 0, err
	}
	if !updated {
		return 0, ErrAccountConflict
	}
	s.clearFailures(normalized)
	return credential.PlayerID, nil
}

// Disable marks the account DISABLED. It is idempotent for an already-disabled
// username and returns the player_id so callers can revoke every session.
func (s *Service) Disable(ctx context.Context, username string) (uint64, error) {
	normalized, err := NormalizeUsername(username)
	if err != nil {
		return 0, ErrInvalidUsername
	}
	credential, err := s.store.Get(ctx, normalized)
	if err != nil {
		return 0, err
	}
	if credential.AccountStatus != AccountStatusDisabled {
		if err := s.store.UpdateAccountStatus(ctx, normalized, AccountStatusDisabled); err != nil {
			return 0, err
		}
	}
	return credential.PlayerID, nil
}

func (s *Service) Authenticate(ctx context.Context, username, password string) (uint64, error) {
	normalized, err := NormalizeUsername(username)
	if err != nil {
		return 0, ErrInvalidCredential
	}
	if err := ValidatePassword(password); err != nil {
		return 0, ErrInvalidCredential
	}
	now := s.now()
	if s.isLocked(normalized, now) {
		return 0, ErrRateLimited
	}

	credential, getErr := s.store.Get(ctx, normalized)
	verifier := s.dummyVerifier
	if getErr == nil && credential != nil {
		verifier = credential.PasswordVerifier
	} else if getErr != nil && !errors.Is(getErr, ErrAccountNotFound) {
		return 0, getErr
	}
	matches := s.verifyPassword(password, verifier)
	if getErr != nil || credential == nil || !matches || credential.PlayerID == 0 ||
		credential.AccountStatus != AccountStatusActive {
		s.recordFailure(normalized, now)
		return 0, ErrInvalidCredential
	}
	s.clearFailures(normalized)
	if iterations, ok := verifierIterations(credential.PasswordVerifier); ok && iterations < s.pbkdf2Iterations {
		// Successful authentication is the only safe time to upgrade a weak
		// historical verifier. CAS prevents two concurrent logins from
		// overwriting each other's salt; an unavailable upgrade does not turn a
		// correct password into a failed login.
		salt := make([]byte, passwordSaltBytes)
		if _, err := rand.Read(salt); err == nil {
			if upgraded, err := s.hashPassword(password, salt); err == nil {
				_, _ = s.store.UpdatePasswordVerifier(ctx, normalized,
					credential.PasswordVerifier, upgraded, now)
			}
		}
	}
	return credential.PlayerID, nil
}

func NormalizeUsername(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) < 3 || len(value) > 24 {
		return "", ErrInvalidUsername
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return "", ErrInvalidUsername
		}
	}
	return value, nil
}

func ValidatePassword(value string) error {
	length := utf8.RuneCountInString(value)
	if length < 8 || length > 128 || len(value) > 512 {
		return ErrInvalidPassword
	}
	return nil
}

func (s *Service) hashPassword(password string, salt []byte) (string, error) {
	key, err := pbkdf2.Key(sha256.New, password, salt, s.pbkdf2Iterations, passwordKeyBytes)
	if err != nil {
		return "", fmt.Errorf("account: derive password verifier: %w", err)
	}
	encode := base64.RawURLEncoding.EncodeToString
	return fmt.Sprintf("pbkdf2-sha256$%d$%s$%s", s.pbkdf2Iterations, encode(salt), encode(key)), nil
}

func (s *Service) verifyPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" {
		return false
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 1 || iterations > 10_000_000 {
		return false
	}
	salt, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(salt) < 8 {
		return false
	}
	expected, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(expected) != passwordKeyBytes {
		return false
	}
	actual, err := pbkdf2.Key(sha256.New, password, salt, iterations, len(expected))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(actual, expected) == 1
}

func verifierIterations(encoded string) (int, bool) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" {
		return 0, false
	}
	iterations, err := strconv.Atoi(parts[1])
	return iterations, err == nil && iterations > 0
}

func verifierMetadata(encoded string) (version, params string) {
	iterations, ok := verifierIterations(encoded)
	if !ok {
		return "", ""
	}
	return "pbkdf2-sha256", fmt.Sprintf("iterations=%d;key_bytes=%d", iterations, passwordKeyBytes)
}

func (s *Service) isLocked(username string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.failuresByUsername[username]
	if !ok {
		return false
	}
	if now.Before(state.lockedUntil) {
		return true
	}
	if now.Sub(state.windowStart) > s.failureWindow {
		delete(s.failuresByUsername, username)
	}
	return false
}

func (s *Service) recordFailure(username string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.failuresByUsername[username]
	if state.windowStart.IsZero() || now.Sub(state.windowStart) > s.failureWindow {
		state = failureState{windowStart: now}
	}
	state.count++
	if state.count >= s.maxFailures {
		state.lockedUntil = now.Add(s.lockDuration)
	}
	s.failuresByUsername[username] = state
}

func (s *Service) clearFailures(username string) {
	s.mu.Lock()
	delete(s.failuresByUsername, username)
	s.mu.Unlock()
}
