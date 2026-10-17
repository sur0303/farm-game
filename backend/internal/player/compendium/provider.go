package compendium

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// CatalogProvider is the narrow read-side seam for the immutable compendium
// catalog. Implementations must return a detached snapshot on every call.
type CatalogProvider interface {
	Snapshot() (CatalogSnapshot, error)
}

// StaticCatalogProvider serves one validated catalog version. The stored
// snapshot is never exposed directly, so the provider is safe to share.
type StaticCatalogProvider struct {
	snapshot CatalogSnapshot
}

var _ CatalogProvider = (*StaticCatalogProvider)(nil)

// NewStaticCatalogProvider validates and retains a detached catalog snapshot.
// With no explicit profile, DefaultLimitsV1 is used.
func NewStaticCatalogProvider(snapshot CatalogSnapshot, limits ...LimitProfile) (*StaticCatalogProvider, error) {
	activeLimits, err := resolveCatalogProviderLimits(limits)
	if err != nil {
		return nil, err
	}

	candidate := snapshot.Clone()
	if err := ValidateCatalog(candidate, activeLimits); err != nil {
		return nil, err
	}
	return &StaticCatalogProvider{snapshot: candidate}, nil
}

// Snapshot returns a deep copy of the validated catalog.
func (p *StaticCatalogProvider) Snapshot() (CatalogSnapshot, error) {
	if p == nil {
		return CatalogSnapshot{}, errors.New("compendium: nil static catalog provider")
	}
	return p.snapshot.Clone(), nil
}

// AtomicCatalogProvider publishes complete catalog versions atomically. A
// reader observes either the previous or the newly published version, never a
// partially updated slice graph.
type AtomicCatalogProvider struct {
	limits   LimitProfile
	snapshot atomic.Pointer[CatalogSnapshot]
}

var _ CatalogProvider = (*AtomicCatalogProvider)(nil)

// NewAtomicCatalogProvider validates the initial snapshot before making it
// visible. With no explicit profile, DefaultLimitsV1 is used.
func NewAtomicCatalogProvider(snapshot CatalogSnapshot, limits ...LimitProfile) (*AtomicCatalogProvider, error) {
	activeLimits, err := resolveCatalogProviderLimits(limits)
	if err != nil {
		return nil, err
	}

	candidate := snapshot.Clone()
	if err := ValidateCatalog(candidate, activeLimits); err != nil {
		return nil, err
	}

	provider := &AtomicCatalogProvider{limits: activeLimits}
	provider.snapshot.Store(&candidate)
	return provider, nil
}

// Snapshot returns a deep copy of the currently published catalog version.
func (p *AtomicCatalogProvider) Snapshot() (CatalogSnapshot, error) {
	if p == nil {
		return CatalogSnapshot{}, errors.New("compendium: nil atomic catalog provider")
	}
	current := p.snapshot.Load()
	if current == nil {
		return CatalogSnapshot{}, errors.New("compendium: atomic catalog provider has no snapshot")
	}
	return current.Clone(), nil
}

// Publish validates a detached candidate before atomically replacing the
// current version. A rejected candidate leaves the previous version intact.
func (p *AtomicCatalogProvider) Publish(snapshot CatalogSnapshot) error {
	if p == nil {
		return errors.New("compendium: nil atomic catalog provider")
	}

	candidate := snapshot.Clone()
	if err := ValidateCatalog(candidate, p.limits); err != nil {
		return err
	}
	p.snapshot.Store(&candidate)
	return nil
}

// Store is an alias for Publish for callers that model the provider as an
// atomic value.
func (p *AtomicCatalogProvider) Store(snapshot CatalogSnapshot) error {
	return p.Publish(snapshot)
}

func resolveCatalogProviderLimits(limits []LimitProfile) (LimitProfile, error) {
	if len(limits) > 1 {
		return LimitProfile{}, fmt.Errorf("compendium: expected at most one limit profile, got %d", len(limits))
	}
	if len(limits) == 0 {
		return DefaultLimitsV1(), nil
	}
	if err := limits[0].Validate(); err != nil {
		return LimitProfile{}, err
	}
	return limits[0], nil
}
