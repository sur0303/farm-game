// Package actorrouting defines the stable Actor -> Virtual Shard -> Holder
// contract. It is deliberately independent from gRPC and any service mesh.
//
// Shape follows 27 §5.7: RouteDomain + ActorAddress + RouteSnapshot
// (DomainSnapshot / ShardRoute / HolderRef). HashProfile v2 byte encoding is
// frozen; changing version, seed, realm, or shard count is a routing migration.
package actorrouting

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"farmgame/internal/holderkey"

	"github.com/cespare/xxhash/v2"
)

// RouteDomain is the authoritative routing namespace. FarmWorker owns only the
// right to scan a FARM virtual shard; it never grants FarmActor write authority.
type RouteDomain string

const (
	DomainPlayer     RouteDomain = "PLAYER"
	DomainFarm       RouteDomain = "FARM"
	DomainFarmWorker RouteDomain = "FARM_WORKER"
)

// Kind is a compatibility alias for RouteDomain used by existing call sites.
type Kind = RouteDomain

const (
	KindPlayer     = DomainPlayer
	KindFarm       = DomainFarm
	KindFarmWorker = DomainFarmWorker
)

type HolderState string
type ShardRouteState string

const (
	HolderStateActive    HolderState = "ACTIVE"
	HolderStateDraining  HolderState = "DRAINING"
	HolderStateAssigning HolderState = "ASSIGNING"

	ShardStateActive    ShardRouteState = "ACTIVE"
	ShardStateDraining  ShardRouteState = "DRAINING"
	ShardStateAssigning ShardRouteState = "ASSIGNING"

	// HashVersionXXH64V2 is the first production routing hash profile. Version
	// 1 was a pre-contract SHA-256 prototype without a seed and cannot be
	// silently loaded into the auth-v1 persistence layout.
	HashVersionXXH64V2 uint32 = 2
	hashInputMagic            = "CFRT"

	// MaxCommitProofPublishedKeys bounds all key-ID/public-key validation maps
	// independently of the enclosing snapshot byte limit.
	MaxCommitProofPublishedKeys = 4096
)

var (
	ErrInvalidAddress             = errors.New("actor route: invalid address")
	ErrNoAssignment               = errors.New("actor route: no shard assignment")
	ErrLeaseExpired               = errors.New("actor route: holder lease expired")
	ErrStaleRoute                 = errors.New("actor route: stale route")
	ErrShardDraining              = errors.New("actor route: shard is not active")
	ErrInvalidSnapshot            = errors.New("actor route: invalid snapshot")
	ErrStaleRedirect              = errors.New("actor route: stale redirect revision")
	ErrTooManyRedirects           = errors.New("actor route: too many redirects")
	ErrInvalidRedirect            = errors.New("actor route: invalid redirect")
	ErrUnsupportedHashProfile     = errors.New("actor route: unsupported hash profile")
	ErrHashMigrationRequired      = errors.New("actor route: hash layout migration required")
	ErrSnapshotRevisionConflict   = errors.New("actor route: same revision has different snapshot")
	ErrSnapshotRevisionGap        = errors.New("actor route: snapshot revision gap")
	ErrSnapshotGenerationRollback = errors.New("actor route: snapshot generation rollback")
	ErrSnapshotHolderGeneration   = errors.New("actor route: holder identity changed without generation progression")
	ErrRedirectReloadRequired     = errors.New("actor route: redirect requires full snapshot reload")
	ErrHolderNamespaceConflict    = errors.New("actor route: holder namespace shared across domains")
	ErrContextRequired            = errors.New("actor route: context is required")
	// ErrTransportGenerationTransition identifies a local data-plane window
	// where the resolved route is valid but its pre-dialed transport generation
	// has not converged yet. It is deliberately distinct from ErrStaleRoute:
	// callers may retry before any backend RPC has been submitted.
	ErrTransportGenerationTransition = errors.New("actor route: transport generation transition")
)

// HashProfile is part of the persisted routing layout, not a local tuning
// option. All callers in the same realm must receive the same pair from one
// atomically installed RouteSnapshot.
type HashProfile struct {
	Version uint32
	Seed    uint64
}

// Validate rejects profiles this binary cannot execute. A future hash version
// must add a new implementation and an explicit data migration before support.
func (p HashProfile) Validate() error {
	if p.Version != HashVersionXXH64V2 || p.Seed == 0 {
		return ErrUnsupportedHashProfile
	}
	return nil
}

// ActorAddress is the contract address: (domain, actor_id).
type ActorAddress struct {
	Domain  RouteDomain
	ActorID uint64
}

// Address is the data-plane address used by callers and gRPC metadata.
// Realm comes from the loaded RouteSnapshot; Kind maps to RouteDomain.
type Address struct {
	Realm string
	Kind  Kind
	ID    uint64
}

func (a Address) ActorAddress() ActorAddress {
	return ActorAddress{Domain: RouteDomain(a.Kind), ActorID: a.ID}
}

// HolderRef is one physical holder process referenced by ShardRoute.HolderIndex.
type HolderRef struct {
	InstanceID string      `json:"instance_id"`
	BootID     string      `json:"boot_id,omitempty"`
	Endpoint   string      `json:"endpoint"`
	State      HolderState `json:"state"`
	Weight     uint32      `json:"weight"`

	// CommitProofKeyID and CommitProofPublicKeyBase64 are the sole public
	// verification key for this exact holder process boot. They are installed
	// and retired with the complete RouteSnapshot generation. Private key
	// material is never represented by the route model.
	CommitProofKeyID           string `json:"commit_proof_key_id,omitempty"`
	CommitProofPublicKeyBase64 string `json:"commit_proof_public_key_base64,omitempty"`
}

// CommitProofPublication returns the exact public key carried by this holder.
// A completely absent pair is a valid mixed-rollout state and returns found
// false; a partial or non-canonical pair fails closed.
func (h HolderRef) CommitProofPublication() (holderkey.Publication, bool, error) {
	if h.CommitProofKeyID == "" && h.CommitProofPublicKeyBase64 == "" {
		return holderkey.Publication{}, false, nil
	}
	if h.CommitProofKeyID == "" || h.CommitProofPublicKeyBase64 == "" {
		return holderkey.Publication{}, false, holderkey.ErrInvalidPublication
	}
	publication, err := holderkey.FromEncoded(h.CommitProofKeyID, h.CommitProofPublicKeyBase64)
	if err != nil {
		return holderkey.Publication{}, false, err
	}
	return publication, true, nil
}

// SetCommitProofPublication copies a validated public key onto the HolderRef.
func (h *HolderRef) SetCommitProofPublication(publication holderkey.Publication) error {
	if h == nil {
		return holderkey.ErrInvalidPublication
	}
	encoded, err := publication.EncodedPublicKey()
	if err != nil {
		return err
	}
	h.CommitProofKeyID = publication.KeyID
	h.CommitProofPublicKeyBase64 = encoded
	return nil
}

func (h *HolderRef) ClearCommitProofPublication() {
	if h == nil {
		return
	}
	h.CommitProofKeyID = ""
	h.CommitProofPublicKeyBase64 = ""
}

// ShardRoute is the assignment for virtual shard index i in DomainSnapshot.Routes.
type ShardRoute struct {
	HolderIndex         int             `json:"holder_index"`
	State               ShardRouteState `json:"state"`
	ShardRevision       uint64          `json:"shard_revision"`
	ExpectedWriterEpoch uint64          `json:"expected_writer_epoch"`
	ExpectedLeaseEpoch  uint64          `json:"expected_lease_epoch,omitempty"`
	LeaseUntilMs        int64           `json:"lease_expire_at_ms,omitempty"`
}

// DomainSnapshot holds one domain's shard table. Routes[i] covers shard i.
type DomainSnapshot struct {
	ShardCount uint32       `json:"shard_count"`
	Routes     []ShardRoute `json:"routes"`
}

// RouteSnapshot is the JSON control-plane snapshot (not a proto).
type RouteSnapshot struct {
	Realm          string         `json:"realm"`
	GlobalRevision uint64         `json:"global_revision"`
	HashVersion    uint32         `json:"hash_version"`
	HashSeed       uint64         `json:"hash_seed"`
	Player         DomainSnapshot `json:"player"`
	Farm           DomainSnapshot `json:"farm"`
	// FarmWorker is the independent FARM_WORKER route domain. It reuses the
	// FARM virtual-shard number space and the same farm_id -> shard algorithm,
	// but its holders, leases, writer epochs, and key space are distinct from
	// FarmActor. Zero-valued FarmWorker means the snapshot predates the
	// FARM_WORKER domain and carries no worker assignment.
	FarmWorker DomainSnapshot `json:"-"`
	Holders    []HolderRef    `json:"holders"`
}

// ShardRouteSnapshot is the small coherent view needed by hot-path authority
// admission. It intentionally contains one route and one HolderRef rather
// than the complete RouteSnapshot. The complete snapshot remains available to
// control-plane consumers; data-plane fence checks should use LookupShard.
type ShardRouteSnapshot struct {
	Realm          string
	GlobalRevision uint64
	HashVersion    uint32
	HashSeed       uint64
	Domain         RouteDomain
	Shard          uint32
	ShardCount     uint32
	Route          ShardRoute
	Holder         HolderRef
}

func (s RouteSnapshot) MarshalJSON() ([]byte, error) {
	type wire struct {
		Realm          string          `json:"realm"`
		GlobalRevision uint64          `json:"global_revision"`
		HashVersion    uint32          `json:"hash_version"`
		HashSeed       uint64          `json:"hash_seed"`
		Player         DomainSnapshot  `json:"player"`
		Farm           DomainSnapshot  `json:"farm"`
		FarmWorker     *DomainSnapshot `json:"farm_worker,omitempty"`
		Holders        []HolderRef     `json:"holders"`
	}
	var farmWorker *DomainSnapshot
	if s.FarmWorker.ShardCount != 0 || len(s.FarmWorker.Routes) != 0 {
		value := s.FarmWorker
		farmWorker = &value
	}
	return json.Marshal(wire{Realm: s.Realm, GlobalRevision: s.GlobalRevision, HashVersion: s.HashVersion, HashSeed: s.HashSeed,
		Player: s.Player, Farm: s.Farm, FarmWorker: farmWorker, Holders: s.Holders})
}

func (s *RouteSnapshot) UnmarshalJSON(data []byte) error {
	type wire struct {
		Realm          string          `json:"realm"`
		GlobalRevision uint64          `json:"global_revision"`
		HashVersion    uint32          `json:"hash_version"`
		HashSeed       uint64          `json:"hash_seed"`
		Player         DomainSnapshot  `json:"player"`
		Farm           DomainSnapshot  `json:"farm"`
		FarmWorker     *DomainSnapshot `json:"farm_worker,omitempty"`
		Holders        []HolderRef     `json:"holders"`
	}
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if s == nil {
		return errors.New("actor route: nil RouteSnapshot receiver")
	}
	*s = RouteSnapshot{Realm: value.Realm, GlobalRevision: value.GlobalRevision, HashVersion: value.HashVersion, HashSeed: value.HashSeed,
		Player: value.Player, Farm: value.Farm, Holders: value.Holders}
	if value.FarmWorker != nil {
		s.FarmWorker = *value.FarmWorker
	}
	return nil
}

// domains returns every domain declared in the snapshot in validation order.
func (s RouteSnapshot) domains() []RouteDomain {
	domains := []RouteDomain{DomainPlayer, DomainFarm}
	if s.FarmWorker.ShardCount != 0 || len(s.FarmWorker.Routes) != 0 {
		domains = append(domains, DomainFarmWorker)
	}
	return domains
}

// Assignment is the thin resolve projection used by data-plane callers.
// HolderPod mirrors HolderRef.InstanceID; WriterEpoch mirrors ExpectedWriterEpoch.
type Assignment struct {
	Kind               Kind
	ShardID            uint32
	HolderPod          string
	HolderBootID       string
	Endpoint           string
	ShardRevision      uint64
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
	LeaseUntilMs       int64
	SnapshotRevision   uint64
	HolderState        HolderState
	ShardState         ShardRouteState
}

// Calculator maps ActorAddress -> virtual shard under a frozen HashVersion.
type Calculator struct {
	realm        string
	profile      HashProfile
	playerShards uint32
	farmShards   uint32
	playerPrefix []byte
	farmPrefix   []byte
}

func NewCalculator(realm string, profile HashProfile, playerShards, farmShards uint32) (*Calculator, error) {
	trimmedRealm := strings.TrimSpace(realm)
	if realm != trimmedRealm || realm == "" || !utf8.ValidString(realm) || len([]byte(realm)) > int(^uint16(0)) || playerShards == 0 || farmShards == 0 {
		return nil, ErrInvalidSnapshot
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	calculator := &Calculator{
		realm:        realm,
		profile:      profile,
		playerShards: playerShards,
		farmShards:   farmShards,
	}
	calculator.playerPrefix = calculator.canonicalPrefix(1)
	calculator.farmPrefix = calculator.canonicalPrefix(2)
	return calculator, nil
}

// ShardFor hashes a canonical versioned route key with seeded XXH64.
// HashVersionXXH64V2 byte layout (must not change without a new version):
//
//	ASCII "CFRT"
//	+ big_endian(hash_version:u32)
//	+ big_endian(len(realm):u16) + realm
//	+ domain byte (PLAYER=1, FARM=2)
//	+ big_endian(actor_id:u64)
//
// FARM and FARM_WORKER share the same farm virtual-shard count and farm_id
// hash input so a worker scans exactly the FARM shard of the farms it owns.
// Their holder/lease tables remain independent because they are different
// RouteSnapshot domains; route-table namespace, not hash input, is the fence.
//
// HashSeed initializes XXH64 and is intentionally not duplicated in the key.
func (c *Calculator) ShardFor(domain RouteDomain, actorID uint64) (uint32, error) {
	if c == nil || actorID == 0 {
		return 0, ErrInvalidAddress
	}
	count, prefix, err := c.domainConfig(domain)
	if err != nil {
		return 0, err
	}
	var actor [8]byte
	binary.BigEndian.PutUint64(actor[:], actorID)
	h := xxhash.NewWithSeed(c.profile.Seed)
	_, _ = h.Write(prefix)
	_, _ = h.Write(actor[:])
	return uint32(h.Sum64() % uint64(count)), nil
}

func (c *Calculator) canonicalPrefix(domainByte byte) []byte {
	prefix := make([]byte, 0, len(hashInputMagic)+4+2+len(c.realm)+1)
	prefix = append(prefix, hashInputMagic...)
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], c.profile.Version)
	prefix = append(prefix, encoded[:]...)
	binary.BigEndian.PutUint16(encoded[:2], uint16(len([]byte(c.realm))))
	prefix = append(prefix, encoded[:2]...)
	prefix = append(prefix, c.realm...)
	prefix = append(prefix, domainByte)
	return prefix
}

func (c *Calculator) domainConfig(domain RouteDomain) (uint32, []byte, error) {
	switch domain {
	case DomainPlayer:
		return c.playerShards, c.playerPrefix, nil
	case DomainFarm, DomainFarmWorker:
		return c.farmShards, c.farmPrefix, nil
	default:
		return 0, nil, ErrInvalidAddress
	}
}

func (c *Calculator) canonicalInput(domain RouteDomain, actorID uint64) ([]byte, error) {
	if c == nil || actorID == 0 {
		return nil, ErrInvalidAddress
	}
	_, prefix, err := c.domainConfig(domain)
	if err != nil {
		return nil, err
	}
	key := make([]byte, len(prefix), len(prefix)+8)
	copy(key, prefix)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], actorID)
	key = append(key, encoded[:]...)
	return key, nil
}

// Profile exposes the immutable profile used by routing and auth-v1 bucket
// derivation. It is useful for persistence claim validation and probes.
func (c *Calculator) Profile() HashProfile {
	if c == nil {
		return HashProfile{}
	}
	return c.profile
}

// ShardCount exposes the immutable domain size from the same calculator used
// by ShardFor. Persistence bucket scanners use it to reject forged/out-of-range
// auth-v1 bucket references instead of copying a count into local config.
func (c *Calculator) ShardCount(domain RouteDomain) (uint32, error) {
	if c == nil {
		return 0, ErrInvalidAddress
	}
	count, _, err := c.domainConfig(domain)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Calculator) Shard(address Address) (uint32, error) {
	if c == nil || address.Realm != c.realm {
		return 0, ErrInvalidAddress
	}
	return c.ShardFor(RouteDomain(address.Kind), address.ID)
}

type Resolver interface {
	Resolve(context.Context, Address) (Assignment, error)
}

// LeaseCheckMode controls which layer owns the final liveness decision for a
// resolved Assignment. Snapshot mode preserves the historical local deadline
// check. AuthorityDelegated mode treats LeaseUntilMs as a bootstrap/diagnostic
// hint and delegates final lease admission to the Holder LeaseGuard and Redis
// Authority fence; it is the mode used when deadline refreshes no longer
// publish full Assignment revisions.
type LeaseCheckMode string

const (
	LeaseCheckSnapshot           LeaseCheckMode = "snapshot"
	LeaseCheckAuthorityDelegated LeaseCheckMode = "authority-delegated"
)

type ResolverOptions struct {
	LeaseCheckMode LeaseCheckMode
}

func (o ResolverOptions) normalized() (ResolverOptions, error) {
	if o.LeaseCheckMode == "" {
		o.LeaseCheckMode = LeaseCheckSnapshot
	}
	switch o.LeaseCheckMode {
	case LeaseCheckSnapshot, LeaseCheckAuthorityDelegated:
		return o, nil
	default:
		return ResolverOptions{}, fmt.Errorf("%w: unsupported lease check mode %q", ErrInvalidSnapshot, o.LeaseCheckMode)
	}
}

// routeTable is an immutable resolve projection. Readers load the pointer via
// atomic.Value; Replace swaps the whole table so Resolve never sees a torn snapshot.
type routeTable struct {
	calculator *Calculator
	snapshot   RouteSnapshot
	// shard -> assignment projection per domain (fail-closed coverage at build).
	assignments map[RouteDomain][]Assignment
}

func buildRouteTable(snapshot RouteSnapshot) (*routeTable, error) {
	if err := validateRouteSnapshot(snapshot); err != nil {
		return nil, err
	}
	calculator, err := NewCalculator(
		snapshot.Realm,
		HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount,
		snapshot.Farm.ShardCount,
	)
	if err != nil {
		return nil, err
	}
	assignments := make(map[RouteDomain][]Assignment, len(snapshot.domains()))
	for _, domain := range snapshot.domains() {
		ds := snapshot.domain(domain)
		byShard := make([]Assignment, ds.ShardCount)
		for shardID := uint32(0); shardID < ds.ShardCount; shardID++ {
			route := ds.Routes[shardID]
			holder := snapshot.Holders[route.HolderIndex]
			byShard[shardID] = Assignment{
				Kind:               Kind(domain),
				ShardID:            shardID,
				HolderPod:          holder.InstanceID,
				HolderBootID:       holder.BootID,
				Endpoint:           holder.Endpoint,
				ShardRevision:      route.ShardRevision,
				AssignmentRevision: route.ShardRevision,
				WriterEpoch:        route.ExpectedWriterEpoch,
				LeaseEpoch:         route.ExpectedLeaseEpoch,
				LeaseUntilMs:       route.LeaseUntilMs,
				SnapshotRevision:   snapshot.GlobalRevision,
				HolderState:        holder.State,
				ShardState:         route.State,
			}
		}
		assignments[domain] = byShard
	}
	return &routeTable{calculator: calculator, snapshot: cloneRouteSnapshot(snapshot), assignments: assignments}, nil
}

// StaticResolver is a Resolver backed by an atomically swappable route table.
// Prefer constructing via StaticAssignmentProvider when a control-plane source
// is needed; NewStaticResolver / LoadStaticResolver remain the data-plane entry.
type StaticResolver struct {
	installMu      sync.Mutex
	table          atomic.Value // *routeTable
	metrics        atomic.Pointer[Metrics]
	leaseCheckMode LeaseCheckMode

	// beforeInstall is a package-private deterministic concurrency-test seam.
	// It runs under installMu immediately before the cancellation linearization
	// check and table.Store; production constructors leave it nil.
	beforeInstall func()
}

func NewStaticResolver(snapshot RouteSnapshot) (*StaticResolver, error) {
	return NewStaticResolverWithOptions(snapshot, ResolverOptions{LeaseCheckMode: LeaseCheckSnapshot})
}

func NewStaticResolverWithOptions(snapshot RouteSnapshot, options ResolverOptions) (*StaticResolver, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	t, err := buildRouteTable(snapshot)
	if err != nil {
		return nil, err
	}
	r := &StaticResolver{leaseCheckMode: options.LeaseCheckMode}
	r.table.Store(t)
	return r, nil
}

// SetMetrics attaches Batch 2.6 route metrics. Nil clears. On attach, syncs
// snapshot_revision / snapshot_age from the current table.
func (r *StaticResolver) SetMetrics(m *Metrics) {
	if r == nil {
		return
	}
	r.metrics.Store(m)
	if m == nil {
		return
	}
	if t := r.load(); t != nil {
		m.NoteSnapshot(t.snapshot.GlobalRevision, time.Now())
	}
}

// NewStaticResolverFromProvider loads the provider's current snapshot into a StaticResolver.
func NewStaticResolverFromProvider(ctx context.Context, provider AssignmentProvider) (*StaticResolver, error) {
	return NewStaticResolverFromProviderWithOptions(ctx, provider, ResolverOptions{LeaseCheckMode: LeaseCheckSnapshot})
}

func NewStaticResolverFromProviderWithOptions(ctx context.Context, provider AssignmentProvider, options ResolverOptions) (*StaticResolver, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: provider required", ErrInvalidSnapshot)
	}
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot, err := provider.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	// Provider return is not a cancellation linearization point: it may have
	// completed concurrently with cancellation. Recheck before starting the
	// potentially expensive table build and once more before returning it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolver, err := NewStaticResolverWithOptions(snapshot, options)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return resolver, nil
}

// Replace atomically swaps the resolve table after validating the snapshot.
// On validation failure the previous table remains in service.
func (r *StaticResolver) Replace(snapshot RouteSnapshot) error {
	return r.ReplaceContext(context.Background(), snapshot)
}

// ReplaceContext atomically swaps the resolve table after validation. Its
// cancellation check is made under installMu immediately before table.Store,
// so a cancellation observed while validation/build is in progress leaves the
// table and metrics revision unchanged.
func (r *StaticResolver) ReplaceContext(ctx context.Context, snapshot RouteSnapshot) error {
	return r.replaceContext(ctx, snapshot, false)
}

// replaceTrustedProviderSnapshot installs a complete snapshot obtained from
// the manager-owned dynamic provider. Unlike public Replace, this reload path
// may converge a stale ASSIGNING route to the provider's current ACTIVE route
// without replaying transient transition metadata.
func (r *StaticResolver) replaceTrustedProviderSnapshot(ctx context.Context, snapshot RouteSnapshot) error {
	return r.replaceContext(ctx, snapshot, true)
}

func (r *StaticResolver) replaceContext(ctx context.Context, snapshot RouteSnapshot, trustedProvider bool) error {
	if r == nil {
		return ErrNoAssignment
	}
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.installMu.Lock()
	defer r.installMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRouteSnapshot(snapshot); err != nil {
		return err
	}
	if current := r.load(); current != nil {
		if !sameRoutingLayout(current.snapshot, snapshot) {
			return ErrHashMigrationRequired
		}
		// Identical replacement is an idempotent no-op. Cancellation was checked
		// under installMu above, so this preserves the install linearization rule.
		if reflect.DeepEqual(current.snapshot, snapshot) {
			return nil
		}
		var err error
		if trustedProvider {
			err = validateTrustedProviderSnapshotReload(current.snapshot, snapshot)
		} else {
			err = ValidateRouteSnapshotUpdate(current.snapshot, snapshot)
		}
		if err != nil {
			return err
		}
	}
	t, err := buildRouteTable(snapshot)
	if err != nil {
		return err
	}
	if r.beforeInstall != nil {
		r.beforeInstall()
	}
	// This is the cancellation/install linearization boundary. Both it and the
	// store are serialized with every other replacement by installMu.
	if err := ctx.Err(); err != nil {
		return err
	}
	r.table.Store(t)
	if m := r.metrics.Load(); m != nil {
		m.NoteSnapshot(snapshot.GlobalRevision, time.Now())
	}
	return nil
}

func sameRoutingLayout(left, right RouteSnapshot) bool {
	return left.Realm == right.Realm &&
		left.HashVersion == right.HashVersion &&
		left.HashSeed == right.HashSeed &&
		left.Player.ShardCount == right.Player.ShardCount &&
		left.Farm.ShardCount == right.Farm.ShardCount &&
		left.FarmWorker.ShardCount == right.FarmWorker.ShardCount
}

func (r *StaticResolver) load() *routeTable {
	if r == nil {
		return nil
	}
	v := r.table.Load()
	if v == nil {
		return nil
	}
	return v.(*routeTable)
}

func (s RouteSnapshot) domain(domain RouteDomain) DomainSnapshot {
	switch domain {
	case DomainPlayer:
		return s.Player
	case DomainFarm:
		return s.Farm
	case DomainFarmWorker:
		return s.FarmWorker
	default:
		return DomainSnapshot{}
	}
}

func validateRouteSnapshot(snapshot RouteSnapshot) error {
	if strings.TrimSpace(snapshot.Realm) == "" || snapshot.GlobalRevision == 0 {
		return ErrInvalidSnapshot
	}
	if err := (HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed}).Validate(); err != nil {
		return fmt.Errorf("%w: %w: %v", ErrInvalidSnapshot, ErrHashMigrationRequired, err)
	}
	if len(snapshot.Holders) == 0 {
		return ErrInvalidSnapshot
	}
	type publishedKeyOwner struct {
		index    int
		identity string
	}
	publishedKeyOwners := make(map[string]publishedKeyOwner)
	publishedKeys := 0
	for i, holder := range snapshot.Holders {
		if strings.TrimSpace(holder.InstanceID) == "" || strings.TrimSpace(holder.Endpoint) == "" {
			return fmt.Errorf("%w: holders[%d] missing instance_id/endpoint", ErrInvalidSnapshot, i)
		}
		if !validHolderState(holder.State) {
			return fmt.Errorf("%w: holders[%d] invalid state %q", ErrInvalidSnapshot, i, holder.State)
		}
		publication, found, err := holder.CommitProofPublication()
		if err != nil {
			return fmt.Errorf("%w: holders[%d] commit proof key: %v", ErrInvalidSnapshot, i, err)
		}
		if found {
			identity := holder.InstanceID + "\x00" + holder.BootID
			if owner, duplicate := publishedKeyOwners[publication.KeyID]; duplicate {
				if owner.identity != identity {
					return fmt.Errorf("%w: commit proof key ID is shared by holders[%d] and holders[%d]", ErrInvalidSnapshot, owner.index, i)
				}
				// One process generation may appear in multiple immutable HolderRef
				// entries while snapshots converge. Count and authenticate the exact
				// holder/boot identity once, not once per array index.
				continue
			}
			publishedKeys++
			if publishedKeys > MaxCommitProofPublishedKeys {
				return fmt.Errorf("%w: commit proof key publication limit", ErrInvalidSnapshot)
			}
			publishedKeyOwners[publication.KeyID] = publishedKeyOwner{index: i, identity: identity}
		}
	}
	for _, domain := range snapshot.domains() {
		ds := snapshot.domain(domain)
		if ds.ShardCount == 0 || uint32(len(ds.Routes)) != ds.ShardCount {
			return fmt.Errorf("%w: %s routes length must equal shard_count", ErrInvalidSnapshot, domain)
		}
		for shardID := uint32(0); shardID < ds.ShardCount; shardID++ {
			route := ds.Routes[shardID]
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				return fmt.Errorf("%w: %s shard %d holder_index out of range", ErrInvalidSnapshot, domain, shardID)
			}
			if route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 {
				return fmt.Errorf("%w: %s shard %d revision/epoch required", ErrInvalidSnapshot, domain, shardID)
			}
			if !validShardState(route.State) {
				return fmt.Errorf("%w: %s shard %d invalid state %q", ErrInvalidSnapshot, domain, shardID, route.State)
			}
			if route.State == ShardStateActive {
				holder := snapshot.Holders[route.HolderIndex]
				if strings.TrimSpace(holder.BootID) == "" || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
					return fmt.Errorf("%w: %s shard %d active authority assignment requires holder boot, lease epoch, and positive lease deadline", ErrInvalidSnapshot, domain, shardID)
				}
			}
		}
	}
	if snapshot.FarmWorker.ShardCount != 0 && snapshot.FarmWorker.ShardCount != snapshot.Farm.ShardCount {
		return fmt.Errorf("%w: FARM_WORKER shard_count=%d must equal FARM shard_count=%d", ErrInvalidSnapshot, snapshot.FarmWorker.ShardCount, snapshot.Farm.ShardCount)
	}
	if err := validateHolderNamespaces(snapshot); err != nil {
		return err
	}
	return nil
}

// ValidateRouteSnapshotUpdate validates a complete old-to-new replacement.
// GlobalRevision is only the outer ordering key: every domain/shard generation
// carried by a higher revision must also be monotonic. ACTIVE -> DRAINING is
// the sole same-generation fail-closed transition; returning to ACTIVE still
// requires a complete Authority generation advance or the sealed authority
// activation path. This is intentionally shared by StaticResolver,
// DynamicAssignmentProvider, and RouteAssignmentManager so no consumer can
// install a higher global revision containing an older holder/writer/lease
// generation.
func ValidateRouteSnapshotUpdate(previous, next RouteSnapshot) error {
	return validateRouteSnapshotUpdate(previous, next, false, false)
}

// ValidateWorkerHandoffDrainingSnapshotUpdate validates the one fail-closed
// pre-transfer publication used by FARM_WORKER planned handoff. Exactly one
// FARM_WORKER route may move ACTIVE -> DRAINING at the same Authority
// generation; holder identity, fence tuple, and every other route remain
// subject to the normal complete-snapshot checks.
func ValidateWorkerHandoffDrainingSnapshotUpdate(previous, next RouteSnapshot) error {
	if err := validateRouteSnapshotUpdate(previous, next, false, false); err != nil {
		return err
	}
	if transitions := countWorkerHandoffDrainingTransitions(previous, next); transitions != 1 {
		return fmt.Errorf(
			"%w: worker handoff draining count=%d",
			ErrSnapshotGenerationRollback,
			transitions,
		)
	}
	return nil
}

// ValidateWorkerHandoffActivationSnapshotUpdate validates the fenced
// generation-changing half of FARM_WORKER planned handoff. Exactly one route
// must move DRAINING -> ACTIVE; the generic validator still requires complete
// assignment/writer/lease progression when holder identity changes.
func ValidateWorkerHandoffActivationSnapshotUpdate(
	previous,
	next RouteSnapshot,
) error {
	if err := validateRouteSnapshotUpdate(
		previous,
		next,
		false,
		false,
	); err != nil {
		return err
	}
	transitions := 0
	for shardID := uint32(0); shardID < previous.FarmWorker.ShardCount; shardID++ {
		oldRoute := previous.FarmWorker.Routes[shardID]
		newRoute := next.FarmWorker.Routes[shardID]
		if oldRoute.State == ShardStateDraining &&
			newRoute.State == ShardStateActive {
			oldHolder := previous.Holders[oldRoute.HolderIndex]
			newHolder := next.Holders[newRoute.HolderIndex]
			if newRoute.ShardRevision <= oldRoute.ShardRevision ||
				newRoute.ExpectedWriterEpoch <= oldRoute.ExpectedWriterEpoch ||
				newRoute.ExpectedLeaseEpoch <= oldRoute.ExpectedLeaseEpoch ||
				(oldHolder.InstanceID == newHolder.InstanceID &&
					oldHolder.BootID == newHolder.BootID) {
				return fmt.Errorf(
					"%w: FARM_WORKER shard %d activation lacks complete generation and holder advance",
					ErrSnapshotGenerationRollback,
					shardID,
				)
			}
			transitions++
		}
	}
	if transitions != 1 {
		return fmt.Errorf(
			"%w: worker handoff activation count=%d",
			ErrSnapshotGenerationRollback,
			transitions,
		)
	}
	return nil
}

// ValidateAuthoritySnapshotUpdate validates the one exceptional publication
// made after a RouteAssignmentManager authority commit: exactly one legal
// initial placeholder or committed Drain ASSIGNING -> ACTIVE transition. It
// returns no token or capability; only the separately-created dynamic write
// handle can request this publication mode.
func ValidateAuthoritySnapshotUpdate(previous, next RouteSnapshot) error {
	if err := validateRouteSnapshotUpdate(previous, next, true, true); err != nil {
		return err
	}
	if activations := countSameGenerationActivations(previous, next); activations != 1 {
		return fmt.Errorf("%w: authority activation count=%d", ErrSnapshotGenerationRollback, activations)
	}
	return nil
}

// ValidateAuthorityWaveSnapshotUpdate allows one complete ScalePlan wave to
// change many owned routes in a single GlobalRevision. Ordinary single-shard
// authority publication still uses ValidateAuthoritySnapshotUpdate.
func ValidateAuthorityWaveSnapshotUpdate(previous, next RouteSnapshot) error {
	return validateRouteSnapshotUpdate(previous, next, true, true)
}

// validateTrustedProviderSnapshotReload validates a complete current snapshot
// from the trusted read capability. A resolver may have missed one or several
// publications, so every legal ASSIGNING -> ACTIVE transition is accepted;
// all layout, global revision, generation, holder, deadline, and shape checks
// remain enforced by validateRouteSnapshotUpdate.
func validateTrustedProviderSnapshotReload(previous, next RouteSnapshot) error {
	return validateRouteSnapshotUpdate(previous, next, true, true)
}

func countSameGenerationActivations(previous, next RouteSnapshot) int {
	activations := 0
	for _, domain := range previous.domains() {
		before := previous.domain(domain)
		after := next.domain(domain)
		for shardID := uint32(0); shardID < before.ShardCount; shardID++ {
			oldRoute, newRoute := before.Routes[shardID], after.Routes[shardID]
			sameGeneration := newRoute.ShardRevision == oldRoute.ShardRevision &&
				newRoute.ExpectedWriterEpoch == oldRoute.ExpectedWriterEpoch &&
				newRoute.ExpectedLeaseEpoch == oldRoute.ExpectedLeaseEpoch
			if sameGeneration && oldRoute.State == ShardStateAssigning && newRoute.State == ShardStateActive {
				activations++
			}
		}
	}
	return activations
}

func countWorkerHandoffDrainingTransitions(previous, next RouteSnapshot) int {
	if previous.FarmWorker.ShardCount == 0 ||
		previous.FarmWorker.ShardCount != next.FarmWorker.ShardCount {
		return 0
	}
	transitions := 0
	for shardID := uint32(0); shardID < previous.FarmWorker.ShardCount; shardID++ {
		oldRoute := previous.FarmWorker.Routes[shardID]
		newRoute := next.FarmWorker.Routes[shardID]
		sameGeneration := newRoute.ShardRevision == oldRoute.ShardRevision &&
			newRoute.ExpectedWriterEpoch == oldRoute.ExpectedWriterEpoch &&
			newRoute.ExpectedLeaseEpoch == oldRoute.ExpectedLeaseEpoch
		if sameGeneration &&
			oldRoute.State == ShardStateActive &&
			newRoute.State == ShardStateDraining {
			transitions++
		}
	}
	return transitions
}

func validateRouteSnapshotUpdate(
	previous,
	next RouteSnapshot,
	allowInactiveBootstrap,
	allowSameGenerationActivation bool,
) error {
	if err := validateRouteSnapshot(next); err != nil {
		return err
	}
	if err := validateRouteSnapshot(previous); err != nil {
		return err
	}
	if !sameRoutingLayout(previous, next) {
		return ErrHashMigrationRequired
	}
	switch {
	case next.GlobalRevision < previous.GlobalRevision:
		return ErrStaleRoute
	case next.GlobalRevision == previous.GlobalRevision:
		if reflect.DeepEqual(previous, next) {
			return nil
		}
		return ErrSnapshotRevisionConflict
	}
	for _, domain := range previous.domains() {
		before := previous.domain(domain)
		after := next.domain(domain)
		for shardID := uint32(0); shardID < before.ShardCount; shardID++ {
			oldRoute := before.Routes[shardID]
			newRoute := after.Routes[shardID]
			sameGeneration := newRoute.ShardRevision == oldRoute.ShardRevision &&
				newRoute.ExpectedWriterEpoch == oldRoute.ExpectedWriterEpoch &&
				newRoute.ExpectedLeaseEpoch == oldRoute.ExpectedLeaseEpoch
			if newRoute.ShardRevision < oldRoute.ShardRevision ||
				newRoute.ExpectedWriterEpoch < oldRoute.ExpectedWriterEpoch ||
				newRoute.ExpectedLeaseEpoch < oldRoute.ExpectedLeaseEpoch {
				return fmt.Errorf("%w: %s shard %d old=(assignment=%d writer=%d lease=%d) new=(assignment=%d writer=%d lease=%d)",
					ErrSnapshotGenerationRollback, domain, shardID,
					oldRoute.ShardRevision, oldRoute.ExpectedWriterEpoch, oldRoute.ExpectedLeaseEpoch,
					newRoute.ShardRevision, newRoute.ExpectedWriterEpoch, newRoute.ExpectedLeaseEpoch)
			}
			if sameGeneration && oldRoute.State != newRoute.State {
				initialBootstrap := allowInactiveBootstrap && isInitialRoutePlaceholder(oldRoute) && newRoute.State == ShardStateActive
				activation := allowSameGenerationActivation && oldRoute.State == ShardStateAssigning && !isInitialRoutePlaceholder(oldRoute) && newRoute.State == ShardStateActive
				draining := oldRoute.State == ShardStateActive &&
					newRoute.State == ShardStateDraining
				if !initialBootstrap && !activation && !draining {
					return fmt.Errorf("%w: %s shard %d state %s -> %s without generation progression", ErrSnapshotGenerationRollback, domain, shardID, oldRoute.State, newRoute.State)
				}
			}
			oldHolder := previous.Holders[oldRoute.HolderIndex]
			newHolder := next.Holders[newRoute.HolderIndex]
			if oldHolder.InstanceID == newHolder.InstanceID && oldHolder.BootID == newHolder.BootID &&
				oldHolder.CommitProofKeyID != "" && newHolder.CommitProofKeyID != "" &&
				(oldHolder.CommitProofKeyID != newHolder.CommitProofKeyID || oldHolder.CommitProofPublicKeyBase64 != newHolder.CommitProofPublicKeyBase64) {
				return fmt.Errorf("%w: %s shard %d commit proof key changed within holder boot", ErrSnapshotHolderGeneration, domain, shardID)
			}
			if oldHolder.InstanceID != newHolder.InstanceID || oldHolder.BootID != newHolder.BootID {
				if newRoute.ShardRevision <= oldRoute.ShardRevision ||
					newRoute.ExpectedWriterEpoch <= oldRoute.ExpectedWriterEpoch ||
					newRoute.ExpectedLeaseEpoch <= oldRoute.ExpectedLeaseEpoch {
					initialBootstrap := allowInactiveBootstrap && isInitialRoutePlaceholder(oldRoute) && newRoute.State == ShardStateActive
					activation := allowSameGenerationActivation && oldRoute.State == ShardStateAssigning && !isInitialRoutePlaceholder(oldRoute) && newRoute.State == ShardStateActive
					if initialBootstrap || activation {
						continue
					}
					return fmt.Errorf("%w: %s shard %d holder %s/%s -> %s/%s without complete generation advance",
						ErrSnapshotHolderGeneration, domain, shardID,
						oldHolder.InstanceID, oldHolder.BootID, newHolder.InstanceID, newHolder.BootID)
				}
			}
			if sameGeneration && newRoute.LeaseUntilMs < oldRoute.LeaseUntilMs {
				return fmt.Errorf("%w: %s shard %d lease deadline %d -> %d", ErrSnapshotGenerationRollback, domain, shardID, oldRoute.LeaseUntilMs, newRoute.LeaseUntilMs)
			}
		}
	}
	return nil
}

func isInitialRoutePlaceholder(route ShardRoute) bool {
	return route.State == ShardStateAssigning && route.ShardRevision == 1 && route.ExpectedWriterEpoch == 1 && route.ExpectedLeaseEpoch == 1
}

func validateHolderNamespaces(snapshot RouteSnapshot) error {
	owner := make(map[int]RouteDomain)
	for _, domain := range snapshot.domains() {
		ds := snapshot.domain(domain)
		for _, route := range ds.Routes {
			if previous, shared := owner[route.HolderIndex]; shared && previous != domain {
				return fmt.Errorf("%w: holder index %d is referenced by both %s and %s", ErrHolderNamespaceConflict, route.HolderIndex, previous, domain)
			}
			owner[route.HolderIndex] = domain
		}
	}
	return nil
}

func validHolderState(state HolderState) bool {
	switch state {
	case HolderStateActive, HolderStateDraining, HolderStateAssigning:
		return true
	default:
		return false
	}
}

func validShardState(state ShardRouteState) bool {
	switch state {
	case ShardStateActive, ShardStateDraining, ShardStateAssigning:
		return true
	default:
		return false
	}
}

func LoadStaticResolver(path string) (*StaticResolver, error) {
	provider, err := LoadStaticAssignmentProvider(path)
	if err != nil {
		return nil, err
	}
	return NewStaticResolverFromProvider(context.Background(), provider)
}

// ParseRouteSnapshotJSON loads only the contract RouteSnapshot. Legacy flat
// Assignment JSON and snapshots without HashSeed require an explicit offline
// routing/persistence migration; they must never be silently reinterpreted.
func ParseRouteSnapshotJSON(data []byte) (RouteSnapshot, error) {
	var probe struct {
		HashVersion    uint32          `json:"hash_version"`
		HashSeed       uint64          `json:"hash_seed"`
		Player         json.RawMessage `json:"player"`
		Assignments    json.RawMessage `json:"assignments"`
		RoutingVersion uint32          `json:"routing_version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return RouteSnapshot{}, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if len(probe.Assignments) > 0 || probe.RoutingVersion > 0 {
		return RouteSnapshot{}, ErrHashMigrationRequired
	}
	if len(probe.Player) > 0 || probe.HashVersion > 0 || probe.HashSeed > 0 {
		var snapshot RouteSnapshot
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return RouteSnapshot{}, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
		}
		if snapshot.HashSeed == 0 || snapshot.HashVersion != HashVersionXXH64V2 {
			return RouteSnapshot{}, ErrHashMigrationRequired
		}
		return snapshot, nil
	}
	return RouteSnapshot{}, fmt.Errorf("%w: unrecognized snapshot shape", ErrInvalidSnapshot)
}

func (r *StaticResolver) Realm() string {
	t := r.load()
	if t == nil || t.calculator == nil {
		return ""
	}
	return t.calculator.realm
}

func (r *StaticResolver) Snapshot() RouteSnapshot {
	t := r.load()
	if t == nil {
		return RouteSnapshot{}
	}
	return cloneRouteSnapshot(t.snapshot)
}

// CurrentRevision returns the revision of the resolver's immutable route
// table without cloning the complete RouteSnapshot.
func (r *StaticResolver) CurrentRevision() uint64 {
	t := r.load()
	if t == nil {
		return 0
	}
	return t.snapshot.GlobalRevision
}

// CurrentRevisionForHolder returns one coherent resolver revision together
// with the number of shards assigned to a holder generation in that same
// immutable table. It avoids copying all routes merely to report holder
// readiness and serving-shard counts.
func (r *StaticResolver) CurrentRevisionForHolder(domain RouteDomain, holderID, bootID string) (uint64, int) {
	t := r.load()
	if t == nil || holderID == "" || bootID == "" {
		return 0, 0
	}
	assignments, ok := t.assignments[domain]
	if !ok {
		return 0, 0
	}
	count := 0
	for i := range assignments {
		assignment := assignments[i]
		if assignment.HolderPod == holderID && assignment.HolderBootID == bootID {
			count++
		}
	}
	return t.snapshot.GlobalRevision, count
}

// CloneRouteSnapshot returns a deep copy suitable for control-plane handoff
// code. RouteSnapshot remains the single shared model; callers cannot mutate
// the resolver's atomic table through the returned value.
func CloneRouteSnapshot(snapshot RouteSnapshot) RouteSnapshot {
	return cloneRouteSnapshot(snapshot)
}

func (r *StaticResolver) Endpoints() []string {
	t := r.load()
	if t == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var endpoints []string
	for _, holder := range t.snapshot.Holders {
		if _, ok := seen[holder.Endpoint]; ok {
			continue
		}
		seen[holder.Endpoint] = struct{}{}
		endpoints = append(endpoints, holder.Endpoint)
	}
	return endpoints
}

func (r *StaticResolver) EndpointsFor(domain Kind) []string {
	t := r.load()
	if t == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var endpoints []string
	for _, assignment := range t.assignments[RouteDomain(domain)] {
		if _, ok := seen[assignment.Endpoint]; ok {
			continue
		}
		seen[assignment.Endpoint] = struct{}{}
		endpoints = append(endpoints, assignment.Endpoint)
	}
	return endpoints
}

func (r *StaticResolver) HolderEpochResolver(domain Kind, holderPod string) func(uint64) (uint64, error) {
	return func(id uint64) (uint64, error) {
		// This compatibility callback has no context parameter. Do not
		// substitute context.Background: cancellation could be lost before
		// route admission. Use HolderEpochResolverWithContext for new callers.
		return 0, ErrContextRequired
	}
}

// HolderEpochResolverWithContext is the cancellation-aware replacement for
// the legacy holder epoch callback.
func (r *StaticResolver) HolderEpochResolverWithContext(ctx context.Context, domain Kind, holderPod string) func(uint64) (uint64, error) {
	return func(id uint64) (uint64, error) {
		if ctx == nil {
			return 0, ErrContextRequired
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		address := Address{Realm: r.Realm(), Kind: domain, ID: id}
		assignment, err := r.resolve(ctx, address)
		if err == nil && assignment.HolderPod != holderPod {
			err = fmt.Errorf("%w: shard belongs to %s", ErrStaleRoute, assignment.HolderPod)
		}
		if err == nil {
			err = ctx.Err()
		}
		if m := r.metrics.Load(); m != nil {
			m.ObserveResolve(RouteDomain(domain), err)
		}
		if err != nil {
			return 0, err
		}
		return assignment.WriterEpoch, nil
	}
}

// ResolveActor is the thin contract API: domain + actorID -> holder/shard/epoch.
func (r *StaticResolver) ResolveActor(ctx context.Context, domain RouteDomain, actorID uint64) (Assignment, error) {
	return r.Resolve(ctx, Address{Realm: r.Realm(), Kind: Kind(domain), ID: actorID})
}

// LookupRoute returns one coherent route+holder projection from a single
// atomically loaded table. Unlike Resolve it does not decide active/lease
// admission; cryptographic validators need to check those facts together with
// the public key from this same generation.
func (r *StaticResolver) LookupRoute(ctx context.Context, address Address) (ShardRouteSnapshot, error) {
	if ctx == nil {
		return ShardRouteSnapshot{}, ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return ShardRouteSnapshot{}, err
	}
	table := r.load()
	if table == nil || table.calculator == nil {
		return ShardRouteSnapshot{}, ErrNoAssignment
	}
	shard, err := table.calculator.Shard(address)
	if err != nil {
		return ShardRouteSnapshot{}, err
	}
	domain := RouteDomain(address.Kind)
	domainSnapshot := table.snapshot.domain(domain)
	if shard >= domainSnapshot.ShardCount || int(shard) >= len(domainSnapshot.Routes) {
		return ShardRouteSnapshot{}, ErrNoAssignment
	}
	route := domainSnapshot.Routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(table.snapshot.Holders) {
		return ShardRouteSnapshot{}, ErrInvalidSnapshot
	}
	if err := ctx.Err(); err != nil {
		return ShardRouteSnapshot{}, err
	}
	return ShardRouteSnapshot{
		Realm: table.snapshot.Realm, GlobalRevision: table.snapshot.GlobalRevision,
		HashVersion: table.snapshot.HashVersion, HashSeed: table.snapshot.HashSeed,
		Domain: domain, Shard: shard, ShardCount: domainSnapshot.ShardCount,
		Route: route, Holder: table.snapshot.Holders[route.HolderIndex],
	}, nil
}

func (r *StaticResolver) Resolve(ctx context.Context, address Address) (Assignment, error) {
	domain := RouteDomain(address.Kind)
	assignment, err := r.resolve(ctx, address)
	if m := r.metrics.Load(); m != nil {
		m.ObserveResolve(domain, err)
	}
	return assignment, err
}

func (r *StaticResolver) resolve(ctx context.Context, address Address) (Assignment, error) {
	if ctx == nil {
		return Assignment{}, ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return Assignment{}, err
	}
	t := r.load()
	if t == nil || t.calculator == nil {
		return Assignment{}, ErrNoAssignment
	}
	shard, err := t.calculator.Shard(address)
	if err != nil {
		return Assignment{}, err
	}
	byShard := t.assignments[RouteDomain(address.Kind)]
	if int(shard) >= len(byShard) {
		return Assignment{}, ErrNoAssignment
	}
	assignment := byShard[shard]
	if assignment.ShardState != ShardStateActive || assignment.HolderState != HolderStateActive {
		return Assignment{}, ErrShardDraining
	}
	if r.leaseCheckMode == LeaseCheckSnapshot && (assignment.LeaseUntilMs <= 0 || time.Now().UnixMilli() >= assignment.LeaseUntilMs) {
		return Assignment{}, ErrLeaseExpired
	}
	if err := ctx.Err(); err != nil {
		return Assignment{}, err
	}
	return assignment, nil
}

// ActorRouteClient is a thin data-plane facade. StaticResolver already performs
// one hash and one immutable shard-table lookup, so a high-cardinality
// actor_id -> Assignment cache would add stale-route and memory risk without
// removing meaningful work. Holder connection pools remain keyed by endpoint.
type ActorRouteClient struct {
	resolver Resolver
}

func NewActorRouteClient(resolver Resolver, _ time.Duration) (*ActorRouteClient, error) {
	if resolver == nil {
		return nil, errors.New("actor route: resolver required")
	}
	return &ActorRouteClient{resolver: resolver}, nil
}

func (c *ActorRouteClient) Resolve(ctx context.Context, address Address) (Assignment, error) {
	return c.resolver.Resolve(ctx, address)
}

func (c *ActorRouteClient) Invalidate(Address, uint64) {
	// No per-actor cache exists. RedirectApplier installs the corrected shard
	// route atomically; the next Resolve observes it immediately.
}
