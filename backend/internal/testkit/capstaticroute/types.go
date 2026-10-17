package capstaticroute

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
)

const (
	ReceiptSchema = "farm-local-static-route-init-receipt.v2"
	IntentSchema  = "farm-local-static-route-init-intent.v1"
	LedgerSchema  = "farm-local-static-route-init-exact-ledger.v2"
)

var (
	ErrInvalidConfig    = errors.New("cap static route: invalid configuration")
	ErrIncomplete       = errors.New("cap static route: authority initialization incomplete")
	ErrIdentityConflict = errors.New("cap static route: existing authority identity conflict")
	stableSegment       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	noncePattern        = regexp.MustCompile(`^[a-z0-9]{20,40}$`)
)

type Holder struct {
	ID       string `json:"holder_id"`
	BootID   string `json:"holder_boot_id"`
	Endpoint string `json:"endpoint"`
	Domain   string `json:"domain"`
}

type Config struct {
	RunID                 string
	ProfileID             string
	Generation            uint64
	ControllerNonce       string
	Realm                 string
	HashSeed              uint64
	ShardCount            uint32
	LeaseTTL              time.Duration
	MinimumRemainingLease time.Duration
	Profile               rediskey.PhysicalProfile
	Holders               []Holder
	RouteManagerID        string
	RouteManagerBootID    string
	Now                   func() time.Time
}

func (c Config) Validate() error {
	if !stableSegment.MatchString(c.RunID) || !stableSegment.MatchString(c.ProfileID) || c.Generation == 0 ||
		!noncePattern.MatchString(c.ControllerNonce) || strings.TrimSpace(c.Realm) != c.Realm || c.Realm == "" || c.HashSeed == 0 {
		return ErrInvalidConfig
	}
	if c.ShardCount != 4 && c.ShardCount != 1024 {
		return fmt.Errorf("%w: shard count must be 4 or 1024", ErrInvalidConfig)
	}
	if c.LeaseTTL < time.Second || c.LeaseTTL > time.Minute || c.MinimumRemainingLease <= 0 || c.MinimumRemainingLease >= c.LeaseTTL {
		return fmt.Errorf("%w: unsafe lease budget", ErrInvalidConfig)
	}
	if err := c.Profile.Validate(); err != nil {
		return fmt.Errorf("%w: Redis profile: %v", ErrInvalidConfig, err)
	}
	if c.Profile.Namespace.Environment == "" || c.Profile.Namespace.Zone == "" {
		return fmt.Errorf("%w: run namespace", ErrInvalidConfig)
	}
	if len(c.Holders) != 4 {
		return fmt.Errorf("%w: exactly four holders required", ErrInvalidConfig)
	}
	seenID := map[string]bool{}
	seenBoot := map[string]bool{}
	for index, holder := range c.Holders {
		wantDomain := "PLAYER"
		if index >= 2 {
			wantDomain = "FARM"
		}
		if holder.Domain != wantDomain || !stableSegment.MatchString(holder.ID) ||
			strings.TrimSpace(holder.BootID) != holder.BootID || holder.BootID == "" || len(holder.BootID) > 256 ||
			strings.TrimSpace(holder.Endpoint) != holder.Endpoint || holder.Endpoint == "" || len(holder.Endpoint) > 253 ||
			strings.ContainsAny(holder.Endpoint, "{} /\\\t\r\n") || seenID[holder.ID] || seenBoot[holder.BootID] {
			return fmt.Errorf("%w: holder %d", ErrInvalidConfig, index)
		}
		wantBoot := fmt.Sprintf("%s-%s-g%d-%s", holder.ID, c.ProfileID, c.Generation, c.ControllerNonce)
		if holder.BootID != wantBoot {
			return fmt.Errorf("%w: holder %q boot does not match generation", ErrInvalidConfig, holder.ID)
		}
		seenID[holder.ID] = true
		seenBoot[holder.BootID] = true
	}
	if strings.TrimSpace(c.RouteManagerID) != c.RouteManagerID || c.RouteManagerID == "" || len(c.RouteManagerID) > 256 ||
		strings.TrimSpace(c.RouteManagerBootID) != c.RouteManagerBootID || c.RouteManagerBootID == "" || len(c.RouteManagerBootID) > 256 {
		return fmt.Errorf("%w: route manager identity", ErrInvalidConfig)
	}
	return nil
}

func (c Config) clock() func() time.Time {
	if c.Now != nil {
		return c.Now
	}
	return time.Now
}

func (c Config) Calculator() (*actorrouting.Calculator, error) {
	return actorrouting.NewCalculator(c.Realm, actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: c.HashSeed}, c.ShardCount, c.ShardCount)
}

func (c Config) HolderIndex(domain actorrouting.RouteDomain, shard uint32) (int, error) {
	if shard >= c.ShardCount {
		return 0, ErrInvalidConfig
	}
	switch domain {
	case actorrouting.DomainPlayer:
		return int(shard % 2), nil
	case actorrouting.DomainFarm:
		return 2 + int(shard%2), nil
	default:
		return 0, ErrInvalidConfig
	}
}

func (c Config) TransitionID(domain actorrouting.RouteDomain, shard uint32) string {
	holder, err := c.holderFor(domain, shard)
	if err != nil {
		return ""
	}
	input := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%d\x00%s\x00%s", c.RunID, c.ProfileID, c.Generation, domain, shard, holder.ID, holder.BootID)
	digest := sha256.Sum256([]byte(input))
	return "capinit-" + hex.EncodeToString(digest[:16])
}

func (c Config) PublicIntent() Intent {
	holders := make([]Holder, len(c.Holders))
	copy(holders, c.Holders)
	return Intent{
		SchemaVersion: IntentSchema, RunID: c.RunID, ProfileID: c.ProfileID, Generation: c.Generation,
		Realm: c.Realm, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: c.HashSeed,
		ShardCount: c.ShardCount, LeaseTTLms: c.LeaseTTL.Milliseconds(), MinimumRemainingLeaseMs: c.MinimumRemainingLease.Milliseconds(),
		NamespaceEnvironment: c.Profile.Namespace.Environment, NamespaceZone: c.Profile.Namespace.Zone,
		Holders: holders, CompanyRedisContacted: false, ActiveBindingAuthorized: false,
		CapacityConstantEligible: false, Supports30mDAU: false, BindingScope: "LOCAL_FUNCTIONAL_ONLY", Activatable: false,
	}
}

type Intent struct {
	SchemaVersion            string   `json:"schema_version"`
	RunID                    string   `json:"run_id"`
	ProfileID                string   `json:"profile_id"`
	Generation               uint64   `json:"generation"`
	Realm                    string   `json:"realm"`
	HashVersion              uint32   `json:"hash_version"`
	HashSeed                 uint64   `json:"hash_seed"`
	ShardCount               uint32   `json:"shard_count"`
	LeaseTTLms               int64    `json:"lease_ttl_ms"`
	MinimumRemainingLeaseMs  int64    `json:"minimum_remaining_lease_ms"`
	NamespaceEnvironment     string   `json:"namespace_environment"`
	NamespaceZone            string   `json:"namespace_zone"`
	Holders                  []Holder `json:"holders"`
	CompanyRedisContacted    bool     `json:"company_redis_contacted"`
	ActiveBindingAuthorized  bool     `json:"active_binding_authorized"`
	CapacityConstantEligible bool     `json:"capacity_constant_eligible"`
	Supports30mDAU           bool     `json:"supports_30m_dau"`
	BindingScope             string   `json:"binding_scope"`
	Activatable              bool     `json:"activatable"`
}

type Ledger struct {
	SchemaVersion            string         `json:"schema_version"`
	RunID                    string         `json:"run_id"`
	ProfileID                string         `json:"profile_id"`
	Generation               uint64         `json:"generation"`
	RouteSnapshotSHA256      string         `json:"route_snapshot_sha256"`
	ExpectedRecordCount      int            `json:"expected_record_count"`
	Records                  []LedgerRecord `json:"records"`
	Complete                 bool           `json:"complete"`
	BootstrapMaterializable  bool           `json:"bootstrap_materializable"`
	Activatable              bool           `json:"activatable"`
	CompanyRedisContacted    bool           `json:"company_redis_contacted"`
	ActiveBindingAuthorized  bool           `json:"active_binding_authorized"`
	CapacityConstantEligible bool           `json:"capacity_constant_eligible"`
	Supports30mDAU           bool           `json:"supports_30m_dau"`
	BindingScope             string         `json:"binding_scope"`
}

type LedgerRecord struct {
	Domain                 actorrouting.RouteDomain `json:"domain"`
	VirtualShard           uint32                   `json:"virtual_shard"`
	RedisKey               string                   `json:"redis_key"`
	AuthorityLayoutVersion string                   `json:"authority_layout_version"`
	HashVersion            uint32                   `json:"hash_version"`
	HashSeed               uint64                   `json:"hash_seed"`
	HolderID               string                   `json:"holder_id"`
	HolderBootID           string                   `json:"holder_boot_id"`
	AssignmentRevision     uint64                   `json:"assignment_revision"`
	WriterEpoch            uint64                   `json:"writer_epoch"`
	LeaseEpoch             uint64                   `json:"lease_epoch"`
	LeaseState             string                   `json:"lease_state"`
	LeaseUntilMs           int64                    `json:"lease_until_ms"`
	TransitionID           string                   `json:"transition_id"`
	TransitionAtMs         int64                    `json:"transition_at_ms"`
	TransitionDigest       string                   `json:"transition_digest"`
	RecordSHA256           string                   `json:"record_sha256"`
}

type Counters struct {
	ExistingExact int `json:"existing_exact"`
	ClaimApplied  int `json:"claim_applied"`
	ClaimReplay   int `json:"claim_already_applied"`
	RenewApplied  int `json:"renew_applied"`
	RenewReplay   int `json:"renew_already_applied"`
	Verified      int `json:"verified"`
}

type Receipt struct {
	SchemaVersion                      string   `json:"schema_version"`
	Status                             string   `json:"status"`
	RunID                              string   `json:"run_id"`
	ProfileID                          string   `json:"profile_id"`
	Generation                         uint64   `json:"generation"`
	ManagerWorkingRevision             uint64   `json:"manager_working_revision"`
	ControllerNonceSHA256              string   `json:"controller_nonce_sha256"`
	Realm                              string   `json:"realm"`
	HashVersion                        uint32   `json:"hash_version"`
	HashSeed                           uint64   `json:"hash_seed"`
	PlayerShardCount                   uint32   `json:"player_shard_count"`
	FarmShardCount                     uint32   `json:"farm_shard_count"`
	Holders                            []Holder `json:"holders"`
	Counters                           Counters `json:"counters"`
	RequestedTTLms                     int64    `json:"requested_ttl_ms"`
	MinimumRemainingLeaseMs            int64    `json:"minimum_remaining_lease_ms"`
	MinimumLeaseUntilMs                int64    `json:"minimum_lease_until_ms"`
	MaximumLeaseUntilMs                int64    `json:"maximum_lease_until_ms"`
	RouteSnapshotSHA256                string   `json:"route_snapshot_sha256,omitempty"`
	ExactLedgerSHA256                  string   `json:"exact_ledger_sha256,omitempty"`
	DurableSnapshotCommitted           bool     `json:"durable_snapshot_committed"`
	DurableSnapshotRevision            uint64   `json:"durable_snapshot_revision,omitempty"`
	DurableSnapshotChecksumSHA256      string   `json:"durable_snapshot_checksum_sha256,omitempty"`
	DurableSnapshotTopologyFingerprint string   `json:"durable_snapshot_topology_fingerprint,omitempty"`
	NamespaceSHA256                    string   `json:"namespace_sha256"`
	Complete                           bool     `json:"complete"`
	BootstrapMaterializable            bool     `json:"bootstrap_materializable"`
	Activatable                        bool     `json:"activatable"`
	CompanyRedisContacted              bool     `json:"company_redis_contacted"`
	ActiveBindingAuthorized            bool     `json:"active_binding_authorized"`
	CapacityConstantEligible           bool     `json:"capacity_constant_eligible"`
	Supports30mDAU                     bool     `json:"supports_30m_dau"`
	BindingScope                       string   `json:"binding_scope"`
	FormalL2Authorized                 bool     `json:"formal_l2_authorized"`
	FailureClass                       string   `json:"failure_class,omitempty"`
}

func sha256Text(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
