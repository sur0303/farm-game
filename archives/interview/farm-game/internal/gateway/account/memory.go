package account

import (
	"context"
	"sync"
	"time"
)

type MemoryStore struct {
	mu       sync.Mutex
	nextID   uint64
	accounts map[string]Credential
}

func NewMemoryStore(playerIDBase uint64) *MemoryStore {
	if playerIDBase == 0 {
		playerIDBase = 1_000_000
	}
	return &MemoryStore{nextID: playerIDBase, accounts: make(map[string]Credential)}
}

func (m *MemoryStore) Create(_ context.Context, username, passwordVerifier string, now time.Time) (*Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.accounts[username]; exists {
		return nil, ErrAccountExists
	}
	m.nextID++
	kdfVersion, kdfParams := verifierMetadata(passwordVerifier)
	credential := Credential{
		Username: username, PlayerID: m.nextID,
		PasswordVerifier: passwordVerifier, PasswordKDFVersion: kdfVersion,
		PasswordKDFParams: kdfParams, AccountStatus: AccountStatusActive,
		CreatedAt: now, PasswordChangedAt: now,
	}
	m.accounts[username] = credential
	copy := credential
	return &copy, nil
}

func (m *MemoryStore) UpdatePasswordVerifier(_ context.Context, username, expectedVerifier, nextVerifier string, changedAt time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	credential, ok := m.accounts[username]
	if !ok {
		return false, ErrAccountNotFound
	}
	if credential.PasswordVerifier != expectedVerifier {
		return false, nil
	}
	credential.PasswordVerifier = nextVerifier
	credential.PasswordKDFVersion, credential.PasswordKDFParams = verifierMetadata(nextVerifier)
	credential.PasswordChangedAt = changedAt
	m.accounts[username] = credential
	return true, nil
}

func (m *MemoryStore) Get(_ context.Context, username string) (*Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	credential, ok := m.accounts[username]
	if !ok {
		return nil, ErrAccountNotFound
	}
	copy := credential
	return &copy, nil
}

func (m *MemoryStore) UpdateAccountStatus(_ context.Context, username, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	credential, ok := m.accounts[username]
	if !ok {
		return ErrAccountNotFound
	}
	credential.AccountStatus = status
	m.accounts[username] = credential
	return nil
}
