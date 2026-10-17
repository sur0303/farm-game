package verifiedholder

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
)

var (
	ErrInvalidWorkloadSnapshot = errors.New("verifiedholder: invalid workload snapshot")
	ErrStaleWorkloadSnapshot   = errors.New("verifiedholder: stale workload snapshot")
	ErrHolderIdentityConflict  = errors.New("verifiedholder: holder identity conflict")
	ErrHolderKeyRebind         = errors.New("verifiedholder: same holder boot cannot rebind commit proof key")
	ErrHolderKeyHistoryFull    = errors.New("verifiedholder: holder boot key history is full")
)

const maxHolderBootKeyHistory = 4096

// WorkloadKey is the complete logical identity of one managed StatefulSet.
// Workload UID is deliberately not part of the key because a recreated
// StatefulSet must atomically replace the old incarnation under this key.
type WorkloadKey struct {
	Realm     string
	Domain    actorrouting.RouteDomain
	Namespace string
	Kind      string
	Name      string
}

func (k WorkloadKey) String() string {
	return strings.Join([]string{k.Realm, string(k.Domain), k.Namespace, k.Kind, k.Name}, "\x1f")
}

func (k WorkloadKey) Validate() error {
	if strings.TrimSpace(k.Realm) == "" || strings.TrimSpace(k.Namespace) == "" || strings.TrimSpace(k.Name) == "" || k.Kind != "StatefulSet" {
		return fmt.Errorf("%w: incomplete workload key", ErrInvalidWorkloadSnapshot)
	}
	if k.Domain != actorrouting.DomainPlayer && k.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: unsupported route domain", ErrInvalidWorkloadSnapshot)
	}
	return nil
}

type LifecycleState string

const (
	LifecycleWarming        LifecycleState = "WARMING"
	LifecycleReadyZeroShard LifecycleState = "READY_ZERO_SHARD"
	LifecycleServing        LifecycleState = "SERVING"
	LifecycleDraining       LifecycleState = "DRAINING"
)

func (s LifecycleState) valid() bool {
	switch s {
	case LifecycleWarming, LifecycleReadyZeroShard, LifecycleServing, LifecycleDraining:
		return true
	default:
		return false
	}
}

// Holder is a rebuildable, attested runtime view. It is not a Lease, an
// Assignment, or a writer capability.
type Holder struct {
	WorkloadKey           WorkloadKey
	WorkloadUID           string
	Domain                actorrouting.RouteDomain
	HolderID              string
	BootID                string
	PodName               string
	PodUID                string
	Ordinal               int32
	LogicEndpoint         string
	ControlEndpoint       string
	TLSDNSIdentity        string
	Lifecycle             LifecycleState
	ProcessReady          bool
	Draining              bool
	ServingShardCount     uint32
	InflightTurnCount     uint32
	ObservedRouteRevision uint64
	CapacityWeight        uint32
	CommitProofKey        holderkey.Publication
	AttestationHash       string
	VerifiedAt            time.Time
	VerifiedUntil         time.Time
}

func (h Holder) Validate() error {
	if err := h.WorkloadKey.Validate(); err != nil {
		return err
	}
	if h.Domain != h.WorkloadKey.Domain || h.WorkloadUID == "" || h.HolderID == "" || h.BootID == "" || h.PodName == "" || h.PodUID == "" || h.Ordinal < 0 {
		return fmt.Errorf("%w: incomplete holder identity", ErrInvalidWorkloadSnapshot)
	}
	if h.LogicEndpoint == "" || h.ControlEndpoint == "" || h.TLSDNSIdentity == "" || h.AttestationHash == "" || !h.Lifecycle.valid() {
		return fmt.Errorf("%w: incomplete verified runtime", ErrInvalidWorkloadSnapshot)
	}
	if h.CommitProofKey.KeyID != "" || len(h.CommitProofKey.PublicKey) != 0 {
		if err := h.CommitProofKey.Validate(); err != nil {
			return fmt.Errorf("%w: commit proof public key", ErrInvalidWorkloadSnapshot)
		}
	}
	if h.VerifiedAt.IsZero() || !h.VerifiedUntil.After(h.VerifiedAt) {
		return fmt.Errorf("%w: invalid verification interval", ErrInvalidWorkloadSnapshot)
	}
	if h.Draining != (h.Lifecycle == LifecycleDraining) {
		return fmt.Errorf("%w: draining lifecycle mismatch", ErrInvalidWorkloadSnapshot)
	}
	if h.Lifecycle == LifecycleReadyZeroShard && (!h.ProcessReady || h.ServingShardCount != 0 || h.InflightTurnCount != 0) {
		return fmt.Errorf("%w: invalid zero-shard runtime", ErrInvalidWorkloadSnapshot)
	}
	// SERVING describes observed ownership, not admission for new placement.
	// A healthy Holder reconciling a route snapshot can keep serving shards
	// while ProcessReady is temporarily false. ProcessAdmitted deliberately
	// retains the stricter ProcessReady check below.
	if h.Lifecycle == LifecycleServing && h.ServingShardCount == 0 {
		return fmt.Errorf("%w: invalid serving runtime", ErrInvalidWorkloadSnapshot)
	}
	return nil
}

func (h Holder) ProcessAdmitted() bool {
	if h.Validate() != nil {
		return false
	}
	return h.ProcessReady && !h.Draining && (h.Lifecycle == LifecycleReadyZeroShard || h.Lifecycle == LifecycleServing)
}

func (h Holder) Eligible(now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	if !h.VerifiedUntil.After(now) || now.Before(h.VerifiedAt) {
		return false
	}
	return h.ProcessAdmitted()
}

// WorkloadSnapshot is replaced atomically. DiscoveryRevision is generated by
// this controller process; Kubernetes resourceVersion remains opaque evidence
// and is never ordered numerically or lexicographically.
type WorkloadSnapshot struct {
	Key                     WorkloadKey
	WorkloadUID             string
	KubeGeneration          int64
	KubeResourceVersion     string
	KubeListResourceVersion string
	DiscoveryRevision       uint64
	CapturedAt              time.Time
	Holders                 []Holder
}

func (s WorkloadSnapshot) Validate() error {
	if err := s.Key.Validate(); err != nil {
		return err
	}
	if s.WorkloadUID == "" || s.KubeGeneration < 0 || s.KubeResourceVersion == "" || s.KubeListResourceVersion == "" || s.DiscoveryRevision == 0 || s.CapturedAt.IsZero() {
		return fmt.Errorf("%w: incomplete workload observation", ErrInvalidWorkloadSnapshot)
	}
	seen := make(map[string]struct{}, len(s.Holders))
	for _, holder := range s.Holders {
		if err := holder.Validate(); err != nil {
			return err
		}
		if holder.WorkloadKey != s.Key || holder.WorkloadUID != s.WorkloadUID {
			return fmt.Errorf("%w: holder workload mismatch", ErrInvalidWorkloadSnapshot)
		}
		key := holderKey(holder.Domain, holder.HolderID)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: duplicate holder", ErrInvalidWorkloadSnapshot)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type Store struct {
	mu          sync.RWMutex
	now         func() time.Time
	workloads   map[string]WorkloadSnapshot
	holderOwner map[string]string
	// bootKeys is a bounded continuity tombstone. Revocation clears the routed
	// publication but the same holder/boot may only reappear with its original
	// key; changing keys requires a new process boot identity.
	bootKeys map[string]holderkey.Publication
}

func NewStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, workloads: make(map[string]WorkloadSnapshot), holderOwner: make(map[string]string), bootKeys: make(map[string]holderkey.Publication)}
}

func (s *Store) ReplaceWorkload(next WorkloadSnapshot) error {
	if s == nil {
		return ErrInvalidWorkloadSnapshot
	}
	if err := next.Validate(); err != nil {
		return err
	}
	next = cloneSnapshot(next)
	workloadKey := next.Key.String()

	s.mu.Lock()
	defer s.mu.Unlock()
	if current, found := s.workloads[workloadKey]; found && next.DiscoveryRevision <= current.DiscoveryRevision {
		return fmt.Errorf("%w: current=%d next=%d", ErrStaleWorkloadSnapshot, current.DiscoveryRevision, next.DiscoveryRevision)
	}
	for _, holder := range next.Holders {
		key := holderKey(holder.Domain, holder.HolderID)
		if owner, found := s.holderOwner[key]; found && owner != workloadKey {
			return fmt.Errorf("%w: %s belongs to %s", ErrHolderIdentityConflict, key, owner)
		}
		if holder.CommitProofKey.KeyID == "" && len(holder.CommitProofKey.PublicKey) == 0 {
			continue
		}
		bootKey := holderBootKey(holder.Domain, holder.HolderID, holder.BootID)
		if previous, found := s.bootKeys[bootKey]; found {
			if previous.KeyID != holder.CommitProofKey.KeyID || !bytes.Equal(previous.PublicKey, holder.CommitProofKey.PublicKey) {
				return fmt.Errorf("%w: %s", ErrHolderKeyRebind, bootKey)
			}
		} else if len(s.bootKeys) >= maxHolderBootKeyHistory {
			return ErrHolderKeyHistoryFull
		}
	}
	if current, found := s.workloads[workloadKey]; found {
		for _, holder := range current.Holders {
			delete(s.holderOwner, holderKey(holder.Domain, holder.HolderID))
		}
	}
	for _, holder := range next.Holders {
		s.holderOwner[holderKey(holder.Domain, holder.HolderID)] = workloadKey
		if holder.CommitProofKey.KeyID != "" || len(holder.CommitProofKey.PublicKey) != 0 {
			s.bootKeys[holderBootKey(holder.Domain, holder.HolderID, holder.BootID)] = holder.CommitProofKey.Clone()
		}
	}
	s.workloads[workloadKey] = next
	return nil
}

func (s *Store) Snapshot(key WorkloadKey) (WorkloadSnapshot, bool) {
	if s == nil {
		return WorkloadSnapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, found := s.workloads[key.String()]
	return cloneSnapshot(value), found
}

func (s *Store) ListDomain(domain actorrouting.RouteDomain) []Holder {
	if s == nil {
		return nil
	}
	verified := s.ListVerifiedDomain(domain)
	now := s.now()
	result := verified[:0]
	for _, holder := range verified {
		if holder.Eligible(now) {
			result = append(result, holder)
		}
	}
	return result
}

// ListVerifiedDomain includes fresh WARMING generations as well as ordinary
// placement-eligible holders. It is a control-plane discovery surface only;
// callers must apply their own narrow lifecycle admission before routing work.
func (s *Store) ListVerifiedDomain(domain actorrouting.RouteDomain) []Holder {
	if s == nil {
		return nil
	}
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Holder, 0)
	for _, workload := range s.workloads {
		for _, holder := range workload.Holders {
			if holder.Domain == domain && holder.Validate() == nil &&
				!now.Before(holder.VerifiedAt) && holder.VerifiedUntil.After(now) {
				copy := holder
				copy.CommitProofKey = holder.CommitProofKey.Clone()
				result = append(result, copy)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Ordinal != result[j].Ordinal {
			return result[i].Ordinal < result[j].Ordinal
		}
		if result[i].HolderID != result[j].HolderID {
			return result[i].HolderID < result[j].HolderID
		}
		return result[i].BootID < result[j].BootID
	})
	return result
}

func holderKey(domain actorrouting.RouteDomain, holderID string) string {
	return string(domain) + "\x00" + holderID
}

func holderBootKey(domain actorrouting.RouteDomain, holderID, bootID string) string {
	return string(domain) + "\x00" + holderID + "\x00" + bootID
}

func cloneSnapshot(value WorkloadSnapshot) WorkloadSnapshot {
	value.Holders = append([]Holder(nil), value.Holders...)
	for index := range value.Holders {
		value.Holders[index].CommitProofKey = value.Holders[index].CommitProofKey.Clone()
	}
	return value
}
