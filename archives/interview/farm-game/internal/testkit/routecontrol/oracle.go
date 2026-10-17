package routecontrol

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// OracleSpec reserves a future observation oracle without executing it or
// assigning a business result.
type OracleSpec struct {
	ID                   string        `json:"id"`
	Description          string        `json:"description"`
	MinimumEvidenceLevel EvidenceLevel `json:"minimum_evidence_level"`
	RequiresAttestation  bool          `json:"requires_attestation"`
}

// OracleRegistry stores future oracle metadata only.
type OracleRegistry struct {
	mu    sync.Mutex
	items map[string]OracleSpec
}

// NewOracleRegistry returns an empty future-oracle registry.
func NewOracleRegistry() *OracleRegistry {
	return &OracleRegistry{items: make(map[string]OracleSpec)}
}

// Register adds metadata for a future oracle. No oracle callback is accepted,
// so registration cannot adjudicate route behavior.
func (r *OracleRegistry) Register(spec OracleSpec) error {
	if r == nil {
		return errors.New("oracle registry is nil")
	}
	if strings.TrimSpace(spec.ID) == "" || strings.TrimSpace(spec.Description) == "" {
		return fmt.Errorf("%w: id and description are required", ErrInvalidOracle)
	}
	if !safeToken(spec.ID) {
		return fmt.Errorf("%w: id must be a safe token", ErrInvalidOracle)
	}
	if !spec.MinimumEvidenceLevel.Valid() {
		return fmt.Errorf("%w: minimum evidence level is invalid", ErrInvalidOracle)
	}
	if spec.MinimumEvidenceLevel.Rank() >= EvidenceE3.Rank() && !spec.RequiresAttestation {
		return fmt.Errorf("%w: E3/E4 oracle must require attestation", ErrInvalidOracle)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[spec.ID]; exists {
		return fmt.Errorf("%w: duplicate id %q", ErrInvalidOracle, spec.ID)
	}
	r.items[spec.ID] = spec
	return nil
}

// RegisterFuture is an explicit alias for callers documenting deferred
// oracle implementation.
func (r *OracleRegistry) RegisterFuture(spec OracleSpec) error {
	return r.Register(spec)
}

// Lookup returns one registered specification.
func (r *OracleRegistry) Lookup(id string) (OracleSpec, bool) {
	if r == nil {
		return OracleSpec{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	spec, ok := r.items[id]
	return spec, ok
}

// List returns registered specifications sorted by ID.
func (r *OracleRegistry) List() []OracleSpec {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]OracleSpec, 0, len(r.items))
	for _, spec := range r.items {
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
