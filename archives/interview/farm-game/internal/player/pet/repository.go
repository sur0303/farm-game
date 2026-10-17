package pet

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrCatalogRepositoryUnavailable is returned when a nil repository is
	// asked to publish a catalog.
	ErrCatalogRepositoryUnavailable = errors.New("pet catalog: repository unavailable")
	// ErrCatalogVersionNotMonotonic prevents a hot update from moving the
	// active catalog backwards or changing an existing catalog version.
	ErrCatalogVersionNotMonotonic = errors.New("pet catalog: catalog version is not monotonic")
	// ErrHistoricalDefinitionConflict prevents an already published
	// {pet_id, pet_config_version} from being changed in place.
	ErrHistoricalDefinitionConflict = errors.New("pet catalog: historical definition conflict")
)

// Provider is the read-only seam used by pet domain callers. Current returns
// the active immutable snapshot; DefinitionAt resolves an exact definition
// version, including definitions retained after a later catalog update.
type Provider interface {
	Current() CatalogSnapshot
	DefinitionAt(petID int32, petConfigVersion uint64) (Definition, bool)
}

// Repository is the versioned catalog seam. Publish validates the complete
// candidate before it can become Current and appends its definitions to the
// historical lookup set.
type Repository interface {
	Provider
	Publish(CatalogSnapshot) error
}

// These aliases make the catalog-specific names available to composition
// code without creating a second, subtly different interface.
type CatalogProvider = Provider
type CatalogRepository = Repository
type PetCatalogProvider = Provider

type definitionKey struct {
	petID            int32
	petConfigVersion uint64
}

// MemoryRepository is a concurrency-safe in-memory implementation of
// Repository. It is intended for process composition and tests; a durable
// config loader can implement the same Provider/Repository seam.
//
// history is append-only by {pet_id, pet_config_version}. A later catalog may
// omit a pet or mark its coin offer unavailable, but it can never rewrite the
// definition used by an already-issued task reward or guard snapshot.
type MemoryRepository struct {
	mu         sync.RWMutex
	current    CatalogSnapshot
	hasCurrent bool
	history    map[definitionKey]Definition
}

// NewMemoryRepository returns an empty repository. The first Publish must
// still pass ValidateCatalog.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{history: make(map[definitionKey]Definition)}
}

// NewMemoryCatalogRepository is a descriptive constructor alias.
func NewMemoryCatalogRepository() *MemoryRepository { return NewMemoryRepository() }

// NewCatalogRepository is kept as the short composition-root constructor.
func NewCatalogRepository() *MemoryRepository { return NewMemoryRepository() }

// Current returns a deep copy of the active snapshot. Mutating the returned
// Pets slice cannot alter repository state.
func (r *MemoryRepository) Current() CatalogSnapshot {
	if r == nil {
		return CatalogSnapshot{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.hasCurrent {
		return CatalogSnapshot{}
	}
	return r.current.Clone()
}

// DefinitionAt returns a value copy of one exact historical definition.
func (r *MemoryRepository) DefinitionAt(petID int32, petConfigVersion uint64) (Definition, bool) {
	if r == nil || petID <= 0 || petConfigVersion == 0 {
		return Definition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.history[definitionKey{petID: petID, petConfigVersion: petConfigVersion}]
	return def, ok
}

// Publish validates candidate before locking and before changing any
// repository state. A failed validation or historical conflict leaves the
// previous Current and history untouched.
func (r *MemoryRepository) Publish(candidate CatalogSnapshot) error {
	if r == nil {
		return ErrCatalogRepositoryUnavailable
	}
	snapshot := candidate.Clone()
	if err := ValidateCatalog(snapshot); err != nil {
		return fmt.Errorf("pet catalog publish: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.history == nil {
		r.history = make(map[definitionKey]Definition)
	}
	if r.hasCurrent {
		switch {
		case snapshot.CatalogVersion < r.current.CatalogVersion:
			return fmt.Errorf("%w: current=%d candidate=%d", ErrCatalogVersionNotMonotonic, r.current.CatalogVersion, snapshot.CatalogVersion)
		case snapshot.CatalogVersion == r.current.CatalogVersion:
			if catalogSnapshotsEqual(snapshot, r.current) {
				return nil
			}
			return fmt.Errorf("%w: catalog_version %d already published", ErrCatalogVersionNotMonotonic, snapshot.CatalogVersion)
		}
	}
	for _, def := range snapshot.Pets {
		key := definitionKey{petID: def.PetID, petConfigVersion: def.PetConfigVersion}
		if previous, exists := r.history[key]; exists && previous != def {
			return fmt.Errorf("%w: pet_id=%d pet_config_version=%d", ErrHistoricalDefinitionConflict, def.PetID, def.PetConfigVersion)
		}
	}

	for _, def := range snapshot.Pets {
		r.history[definitionKey{petID: def.PetID, petConfigVersion: def.PetConfigVersion}] = def
	}
	r.current = snapshot
	r.hasCurrent = true
	return nil
}

// PublishCatalog is an explicit alias for callers that prefer the operation
// name to include the resource being updated.
func (r *MemoryRepository) PublishCatalog(candidate CatalogSnapshot) error {
	return r.Publish(candidate)
}

func catalogSnapshotsEqual(a, b CatalogSnapshot) bool {
	if a.CatalogVersion != b.CatalogVersion || a.FeedRule != b.FeedRule || len(a.Pets) != len(b.Pets) {
		return false
	}
	for i := range a.Pets {
		if a.Pets[i] != b.Pets[i] {
			return false
		}
	}
	return true
}

const (
	P0CatalogVersion         uint64 = 1
	P0FeedRuleVersion        uint64 = 1
	P0DogPetID               int32  = 1
	P0CatPetID               int32  = 2
	P0DogDisplayDefinitionID int32  = 1001
	P0CatDisplayDefinitionID int32  = 1002
)

const p0HourMs int64 = 3_600_000

// DefaultP0Catalog returns a fresh default catalog. It contains no secret
// random material: guard rules carry only public version references, while
// secret-key handling remains outside this domain package.
func DefaultP0Catalog() CatalogSnapshot {
	return CatalogSnapshot{
		CatalogVersion: P0CatalogVersion,
		FeedRule: FeedRule{
			FeedRuleVersion:       P0FeedRuleVersion,
			DogFoodItemID:         P0DogFoodItemID,
			DurationPerFoodMs:     8 * p0HourMs,
			MaxGuardHorizonMs:     24 * p0HourMs,
			MaxQuantityPerCommand: 3,
		},
		Pets: []Definition{
			{
				PetID:               P0DogPetID,
				PetConfigVersion:    1,
				DisplayDefinitionID: P0DogDisplayDefinitionID,
				UnlockOffer:         UnlockOffer{CoinUnlockEnabled: true, CoinPrice: 100, PriceVersion: 1},
				GuardRule: GuardRule{
					GuardRuleVersion:       1,
					TriggerProbabilityBp:   5_000,
					InterceptPolicy:        InterceptPolicyFullGross,
					RandomAlgorithmVersion: 1,
					RandomKeyVersion:       1,
				},
			},
			{
				PetID:               P0CatPetID,
				PetConfigVersion:    1,
				DisplayDefinitionID: P0CatDisplayDefinitionID,
				UnlockOffer:         UnlockOffer{CoinUnlockEnabled: true, CoinPrice: 200, PriceVersion: 1},
				GuardRule: GuardRule{
					GuardRuleVersion:       1,
					TriggerProbabilityBp:   2_500,
					InterceptPolicy:        InterceptPolicyFullGross,
					RandomAlgorithmVersion: 1,
					RandomKeyVersion:       1,
				},
			},
		},
	}
}

// DefaultCatalog is the concise public name for the P0 default.
func DefaultCatalog() CatalogSnapshot { return DefaultP0Catalog() }

// P0Catalog is an equivalent constructor for composition code that names the
// product tier explicitly.
func P0Catalog() CatalogSnapshot { return DefaultP0Catalog() }

// P0Repository is the default in-memory repository implementation.
// PetCatalogRepository is the named in-memory implementation used by the
// default composition root.
type PetCatalogRepository = MemoryRepository

// NewPetCatalogRepository returns an empty versioned repository.
func NewPetCatalogRepository() *PetCatalogRepository { return NewMemoryRepository() }

type P0Repository = MemoryRepository

// NewP0Repository publishes the validated default catalog.
func NewP0Repository() (*P0Repository, error) {
	repo := NewMemoryRepository()
	if err := repo.Publish(DefaultP0Catalog()); err != nil {
		return nil, err
	}
	return repo, nil
}

// MustNewP0Repository is for composition roots where a malformed built-in
// catalog is a startup failure.
func MustNewP0Repository() *P0Repository {
	repo, err := NewP0Repository()
	if err != nil {
		panic(err)
	}
	return repo
}
