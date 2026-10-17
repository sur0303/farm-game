package loadbot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"farmgame/internal/identity"
)

const (
	FixtureManifestSchemaVersion = "mixed-farm-bot/fixture-manifest/v1"
	CleanupManifestSchemaVersion = "mixed-farm-bot/cleanup/v1"
)

type FixtureAccount struct {
	AccountID     string `json:"account_id"`
	PlayerID      uint64 `json:"player_id"`
	CredentialRef string `json:"credential_ref,omitempty"`
}

type FixturePlayer struct {
	PlayerID  uint64  `json:"player_id"`
	AccountID string  `json:"account_id"`
	Profile   Profile `json:"profile"`
}

type FixtureFarm struct {
	FarmID        uint64 `json:"farm_id"`
	OwnerPlayerID uint64 `json:"owner_player_id"`
	PlotCount     int    `json:"plot_count"`
}

// CanonicalPair is the fixture-side representation of one canonical
// FriendRelation. PairID and RelationID intentionally carry the same identity
// so a renderer cannot silently replace the relation root with a direction.
type CanonicalPair struct {
	PairID       string `json:"pair_id"`
	RelationID   string `json:"relation_id"`
	LowPlayerID  uint64 `json:"low_player_id"`
	HighPlayerID uint64 `json:"high_player_id"`
}

type CanonicalFriendRelation = CanonicalPair

func NewCanonicalPair(playerA, playerB uint64) (CanonicalPair, error) {
	if playerA == 0 || playerB == 0 || playerA == playerB {
		return CanonicalPair{}, errors.New("canonical friend relation requires two distinct non-zero players")
	}
	low, high := playerA, playerB
	if low > high {
		low, high = high, low
	}
	relationID, err := identity.FriendRelationID(low, high)
	if err != nil {
		return CanonicalPair{}, fmt.Errorf("derive canonical friend relation: %w", err)
	}
	return CanonicalPair{PairID: relationID, RelationID: relationID, LowPlayerID: low, HighPlayerID: high}, nil
}

type PairParticipants struct {
	LowPlayerID  uint64 `json:"low_player_id"`
	HighPlayerID uint64 `json:"high_player_id"`
}

// FixtureIndex contains both directions of every logical relationship. The
// friend index is explicitly represented in both directions because a fixture
// must not rely on eventual index repair to discover a social opportunity.
type FixtureIndex struct {
	AccountToPlayer    map[string]uint64           `json:"account_to_player"`
	PlayerToAccount    map[uint64]string           `json:"player_to_account"`
	PlayerToFarm       map[uint64]uint64           `json:"player_to_farm"`
	FarmToPlayer       map[uint64]uint64           `json:"farm_to_player"`
	FriendIndexForward map[uint64][]uint64         `json:"friend_index_forward"`
	FriendIndexReverse map[uint64][]uint64         `json:"friend_index_reverse"`
	PairByID           map[string]PairParticipants `json:"pair_by_id"`
}

// BidirectionalIndex is a descriptive alias for callers that want the
// contract name in their own code.
type BidirectionalIndex = FixtureIndex

type HotspotPreparation struct {
	HotspotID            string   `json:"hotspot_id"`
	FarmID               uint64   `json:"farm_id"`
	RequestedViewerCount int      `json:"requested_viewer_count"`
	ViewerCount          int      `json:"viewer_count"`
	ViewerPlayerIDs      []uint64 `json:"viewer_player_ids,omitempty"`
	Ready                bool     `json:"ready"`
}

type OpportunityPreparation struct {
	OpportunityID       string `json:"opportunity_id"`
	ViewerPlayerID      uint64 `json:"viewer_player_id"`
	TargetOwnerPlayerID uint64 `json:"target_owner_player_id"`
	FarmID              uint64 `json:"farm_id"`
	PairID              string `json:"pair_id"`
	RequiredAction      Action `json:"required_action"`
	Ready               bool   `json:"ready"`
}

type FixtureManifest struct {
	SchemaVersion  string                   `json:"schema_version"`
	RunID          string                   `json:"run_id"`
	FixtureID      string                   `json:"fixture_id"`
	Seed           int64                    `json:"seed"`
	Accounts       []FixtureAccount         `json:"accounts"`
	Players        []FixturePlayer          `json:"players"`
	Farms          []FixtureFarm            `json:"farms"`
	CanonicalPairs []CanonicalPair          `json:"canonical_pairs"`
	Index          FixtureIndex             `json:"index"`
	Hotspots       []HotspotPreparation     `json:"hotspots"`
	Opportunities  []OpportunityPreparation `json:"opportunities"`
	Cleanup        CleanupManifest          `json:"cleanup"`
	Fingerprint    string                   `json:"fingerprint"`
}

type CleanupResource string

const (
	CleanupAccount        CleanupResource = "account"
	CleanupPlayer         CleanupResource = "player"
	CleanupFarm           CleanupResource = "farm"
	CleanupFriendRelation CleanupResource = "canonical_friend_relation"
	CleanupIndex          CleanupResource = "friend_index"
	CleanupHotspot        CleanupResource = "hotspot"
	CleanupOpportunity    CleanupResource = "opportunity"
)

type CleanupItem struct {
	Resource CleanupResource `json:"resource"`
	Key      string          `json:"key"`
}

type CleanupManifest struct {
	SchemaVersion string        `json:"schema_version"`
	Scope         string        `json:"scope"`
	Exact         bool          `json:"exact"`
	Items         []CleanupItem `json:"items"`
}

func BuildFixtureManifest(spec RunSpec, plan DryRunPlan) (FixtureManifest, error) {
	if err := spec.Validate(); err != nil {
		return FixtureManifest{}, err
	}
	if err := ValidatePlan(plan); err != nil {
		return FixtureManifest{}, err
	}
	if plan.RunID != spec.RunID || plan.Seed != spec.Workload.Seed {
		return FixtureManifest{}, errors.New("fixture manifest plan identity does not match run spec")
	}
	manifest := FixtureManifest{
		SchemaVersion: FixtureManifestSchemaVersion,
		RunID:         spec.RunID,
		FixtureID:     spec.FixtureID,
		Seed:          spec.Workload.Seed,
		Accounts:      make([]FixtureAccount, 0, len(plan.Players)),
		Players:       make([]FixturePlayer, 0, len(plan.Players)),
		Farms:         make([]FixtureFarm, 0, len(plan.Players)),
	}
	const playerBase uint64 = 9_000_000_000
	const farmBase uint64 = 9_100_000_000
	runSlug := slugIdentifier(spec.RunID)
	for _, planned := range plan.Players {
		playerID := playerBase + uint64(planned.PlayerOrdinal)
		farmID := farmBase + uint64(planned.PlayerOrdinal)
		accountID := fmt.Sprintf("%s-account-%04d", runSlug, planned.PlayerOrdinal)
		manifest.Accounts = append(manifest.Accounts, FixtureAccount{
			AccountID: accountID, PlayerID: playerID,
			CredentialRef: accountID + "-credential-ref",
		})
		manifest.Players = append(manifest.Players, FixturePlayer{
			PlayerID: playerID, AccountID: accountID, Profile: planned.Profile,
		})
		manifest.Farms = append(manifest.Farms, FixtureFarm{
			FarmID: farmID, OwnerPlayerID: playerID, PlotCount: 6,
		})
	}
	for index := 0; index+1 < len(manifest.Players); index++ {
		pair, err := NewCanonicalPair(manifest.Players[index].PlayerID, manifest.Players[index+1].PlayerID)
		if err != nil {
			return FixtureManifest{}, err
		}
		manifest.CanonicalPairs = append(manifest.CanonicalPairs, pair)
	}
	if len(manifest.Farms) > 0 {
		viewers := make([]uint64, 0, len(manifest.Players))
		for _, player := range manifest.Players {
			viewers = append(viewers, player.PlayerID)
		}
		for _, count := range spec.Workload.HotFarmViewerCounts {
			preparedViewers := append([]uint64(nil), viewers...)
			if count < len(preparedViewers) {
				preparedViewers = preparedViewers[:count]
			}
			manifest.Hotspots = append(manifest.Hotspots, HotspotPreparation{
				HotspotID:            fmt.Sprintf("%s-hotspot-%04d", runSlug, count),
				FarmID:               manifest.Farms[0].FarmID,
				RequestedViewerCount: count,
				ViewerCount:          len(preparedViewers),
				ViewerPlayerIDs:      preparedViewers, Ready: len(preparedViewers) == count,
			})
		}
	}
	for index, pair := range manifest.CanonicalPairs {
		manifest.Opportunities = append(manifest.Opportunities, OpportunityPreparation{
			OpportunityID:  fmt.Sprintf("%s-opportunity-%04d", runSlug, index+1),
			ViewerPlayerID: pair.LowPlayerID, TargetOwnerPlayerID: pair.HighPlayerID,
			FarmID: farmIDForOwner(manifest.Farms, pair.HighPlayerID), PairID: pair.PairID,
			RequiredAction: ActionTheft, Ready: true,
		})
	}
	manifest.Index = buildFixtureIndex(manifest)
	manifest.Cleanup = buildCleanupManifest(manifest)
	return manifest.WithFingerprint()
}

func BuildFixtureManifestFromPlan(spec RunSpec, plan DryRunPlan) (FixtureManifest, error) {
	return BuildFixtureManifest(spec, plan)
}

func NewFixtureManifest(spec RunSpec, plan DryRunPlan) (FixtureManifest, error) {
	return BuildFixtureManifest(spec, plan)
}

func (manifest FixtureManifest) Canonical() FixtureManifest {
	result := manifest
	result.Accounts = append([]FixtureAccount(nil), manifest.Accounts...)
	result.Players = append([]FixturePlayer(nil), manifest.Players...)
	result.Farms = append([]FixtureFarm(nil), manifest.Farms...)
	result.CanonicalPairs = append([]CanonicalPair(nil), manifest.CanonicalPairs...)
	result.Hotspots = append([]HotspotPreparation(nil), manifest.Hotspots...)
	result.Opportunities = append([]OpportunityPreparation(nil), manifest.Opportunities...)
	sort.Slice(result.Accounts, func(left, right int) bool { return result.Accounts[left].AccountID < result.Accounts[right].AccountID })
	sort.Slice(result.Players, func(left, right int) bool { return result.Players[left].PlayerID < result.Players[right].PlayerID })
	sort.Slice(result.Farms, func(left, right int) bool { return result.Farms[left].FarmID < result.Farms[right].FarmID })
	sort.Slice(result.CanonicalPairs, func(left, right int) bool {
		return result.CanonicalPairs[left].PairID < result.CanonicalPairs[right].PairID
	})
	sort.Slice(result.Hotspots, func(left, right int) bool { return result.Hotspots[left].HotspotID < result.Hotspots[right].HotspotID })
	sort.Slice(result.Opportunities, func(left, right int) bool {
		return result.Opportunities[left].OpportunityID < result.Opportunities[right].OpportunityID
	})
	result.Cleanup.Items = append([]CleanupItem(nil), manifest.Cleanup.Items...)
	sort.Slice(result.Cleanup.Items, func(left, right int) bool { return result.Cleanup.Items[left].Key < result.Cleanup.Items[right].Key })
	return result
}

func (manifest FixtureManifest) validateStructure() error {
	if manifest.SchemaVersion != FixtureManifestSchemaVersion {
		return fmt.Errorf("fixture manifest schema_version must be %q", FixtureManifestSchemaVersion)
	}
	if err := validateScopedIdentifier("run_id", manifest.RunID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("fixture_id", manifest.FixtureID); err != nil {
		return err
	}
	if len(manifest.Accounts) == 0 || len(manifest.Players) == 0 || len(manifest.Farms) == 0 {
		return errors.New("fixture manifest requires accounts, players, and farms")
	}
	accounts := make(map[string]FixtureAccount, len(manifest.Accounts))
	players := make(map[uint64]FixturePlayer, len(manifest.Players))
	farms := make(map[uint64]FixtureFarm, len(manifest.Farms))
	for _, account := range manifest.Accounts {
		if err := validateScopedIdentifier("account_id", account.AccountID); err != nil {
			return err
		}
		if account.PlayerID == 0 || account.CredentialRef == "" || strings.ContainsAny(account.CredentialRef, " \t\r\n") {
			return fmt.Errorf("account %q has invalid player or credential reference", account.AccountID)
		}
		if _, exists := accounts[account.AccountID]; exists {
			return fmt.Errorf("duplicate account %q", account.AccountID)
		}
		accounts[account.AccountID] = account
	}
	for _, player := range manifest.Players {
		if player.PlayerID == 0 || player.AccountID == "" {
			return errors.New("fixture player has missing identity")
		}
		if err := player.Profile.Validate(); err != nil {
			return err
		}
		if _, exists := players[player.PlayerID]; exists {
			return fmt.Errorf("duplicate player %d", player.PlayerID)
		}
		account, exists := accounts[player.AccountID]
		if !exists || account.PlayerID != player.PlayerID {
			return fmt.Errorf("player %d is not bound to its account", player.PlayerID)
		}
		players[player.PlayerID] = player
	}
	for _, account := range accounts {
		if _, exists := players[account.PlayerID]; !exists {
			return fmt.Errorf("account %q points to an unknown player", account.AccountID)
		}
	}
	for _, farm := range manifest.Farms {
		if farm.FarmID == 0 || farm.OwnerPlayerID == 0 || farm.PlotCount <= 0 {
			return fmt.Errorf("farm %d has invalid identity or plot count", farm.FarmID)
		}
		if _, exists := players[farm.OwnerPlayerID]; !exists {
			return fmt.Errorf("farm %d has unknown owner %d", farm.FarmID, farm.OwnerPlayerID)
		}
		if _, exists := farms[farm.FarmID]; exists {
			return fmt.Errorf("duplicate farm %d", farm.FarmID)
		}
		for _, existing := range farms {
			if existing.OwnerPlayerID == farm.OwnerPlayerID {
				return fmt.Errorf("player %d owns more than one fixture farm", farm.OwnerPlayerID)
			}
		}
		farms[farm.FarmID] = farm
	}
	pairs := make(map[string]CanonicalPair, len(manifest.CanonicalPairs))
	for _, pair := range manifest.CanonicalPairs {
		if pair.LowPlayerID == 0 || pair.LowPlayerID >= pair.HighPlayerID {
			return fmt.Errorf("pair %q is not in low/high canonical order", pair.PairID)
		}
		expected, err := NewCanonicalPair(pair.LowPlayerID, pair.HighPlayerID)
		if err != nil {
			return err
		}
		if pair.PairID == "" || pair.PairID != expected.PairID || pair.RelationID != expected.RelationID {
			return fmt.Errorf("pair %q does not match canonical FriendRelation identity", pair.PairID)
		}
		if _, ok := players[pair.LowPlayerID]; !ok {
			return fmt.Errorf("pair %q references unknown low player", pair.PairID)
		}
		if _, ok := players[pair.HighPlayerID]; !ok {
			return fmt.Errorf("pair %q references unknown high player", pair.PairID)
		}
		if _, exists := pairs[pair.PairID]; exists {
			return fmt.Errorf("duplicate canonical FriendRelation %q", pair.PairID)
		}
		pairs[pair.PairID] = pair
	}
	expectedIndex := buildFixtureIndex(manifest)
	if !reflect.DeepEqual(manifest.Index, expectedIndex) {
		return errors.New("fixture bidirectional indexes do not match canonical entities")
	}
	hotspotIDs := make(map[string]struct{}, len(manifest.Hotspots))
	for _, hotspot := range manifest.Hotspots {
		if err := validateScopedIdentifier("hotspot_id", hotspot.HotspotID); err != nil {
			return err
		}
		if hotspot.RequestedViewerCount <= 0 || hotspot.RequestedViewerCount > 1_000_000 {
			return fmt.Errorf("hotspot %q has invalid requested_viewer_count", hotspot.HotspotID)
		}
		if hotspot.ViewerCount <= 0 || hotspot.ViewerCount > hotspot.RequestedViewerCount {
			return fmt.Errorf("hotspot %q has invalid viewer_count", hotspot.HotspotID)
		}
		if _, exists := farms[hotspot.FarmID]; !exists {
			return fmt.Errorf("hotspot %q references unknown farm", hotspot.HotspotID)
		}
		if _, exists := hotspotIDs[hotspot.HotspotID]; exists {
			return fmt.Errorf("duplicate hotspot %q", hotspot.HotspotID)
		}
		hotspotIDs[hotspot.HotspotID] = struct{}{}
		seenViewers := make(map[uint64]struct{}, len(hotspot.ViewerPlayerIDs))
		for _, playerID := range hotspot.ViewerPlayerIDs {
			if _, exists := players[playerID]; !exists {
				return fmt.Errorf("hotspot %q references unknown viewer %d", hotspot.HotspotID, playerID)
			}
			if _, exists := seenViewers[playerID]; exists {
				return fmt.Errorf("hotspot %q repeats viewer %d", hotspot.HotspotID, playerID)
			}
			seenViewers[playerID] = struct{}{}
		}
		if hotspot.ViewerCount != len(hotspot.ViewerPlayerIDs) {
			return fmt.Errorf("hotspot %q viewer_count does not match prepared viewer list", hotspot.HotspotID)
		}
		if hotspot.Ready != (hotspot.ViewerCount == hotspot.RequestedViewerCount) {
			return fmt.Errorf("hotspot %q ready does not match requested/effective viewer counts", hotspot.HotspotID)
		}
	}
	opportunityIDs := make(map[string]struct{}, len(manifest.Opportunities))
	for _, opportunity := range manifest.Opportunities {
		if err := validateScopedIdentifier("opportunity_id", opportunity.OpportunityID); err != nil {
			return err
		}
		if opportunity.ViewerPlayerID == 0 || opportunity.TargetOwnerPlayerID == 0 || opportunity.ViewerPlayerID == opportunity.TargetOwnerPlayerID {
			return fmt.Errorf("opportunity %q has invalid viewer/owner pair", opportunity.OpportunityID)
		}
		if _, exists := players[opportunity.ViewerPlayerID]; !exists {
			return fmt.Errorf("opportunity %q references unknown viewer", opportunity.OpportunityID)
		}
		targetFarm, exists := farms[opportunity.FarmID]
		if !exists || targetFarm.OwnerPlayerID != opportunity.TargetOwnerPlayerID {
			return fmt.Errorf("opportunity %q does not point to the target owner's farm", opportunity.OpportunityID)
		}
		pair, exists := pairs[opportunity.PairID]
		if !exists || !((pair.LowPlayerID == opportunity.ViewerPlayerID && pair.HighPlayerID == opportunity.TargetOwnerPlayerID) ||
			(pair.HighPlayerID == opportunity.ViewerPlayerID && pair.LowPlayerID == opportunity.TargetOwnerPlayerID)) {
			return fmt.Errorf("opportunity %q is not backed by its canonical FriendRelation", opportunity.OpportunityID)
		}
		if opportunity.RequiredAction != ActionCare && opportunity.RequiredAction != ActionTheft {
			return fmt.Errorf("opportunity %q has unsupported action %q", opportunity.OpportunityID, opportunity.RequiredAction)
		}
		if _, exists := opportunityIDs[opportunity.OpportunityID]; exists {
			return fmt.Errorf("duplicate opportunity %q", opportunity.OpportunityID)
		}
		opportunityIDs[opportunity.OpportunityID] = struct{}{}
	}
	if err := manifest.Cleanup.Validate(); err != nil {
		return err
	}
	expectedCleanup := buildCleanupManifest(manifest)
	if !reflect.DeepEqual(manifest.Cleanup, expectedCleanup) {
		return errors.New("fixture cleanup list is not the exact run-scoped entity/index set")
	}
	return nil
}

func (manifest FixtureManifest) WithDerivedIndexes() (FixtureManifest, error) {
	manifest.Index = buildFixtureIndex(manifest)
	manifest.Cleanup = buildCleanupManifest(manifest)
	return manifest.WithFingerprint()
}

func RebuildFixtureIndexes(manifest FixtureManifest) (FixtureManifest, error) {
	return manifest.WithDerivedIndexes()
}

func (manifest FixtureManifest) WithFingerprint() (FixtureManifest, error) {
	manifest = manifest.Canonical()
	if err := manifest.validateStructure(); err != nil {
		return FixtureManifest{}, err
	}
	fingerprint, err := manifest.computeFingerprint()
	if err != nil {
		return FixtureManifest{}, err
	}
	manifest.Fingerprint = fingerprint
	return manifest, nil
}

func (manifest FixtureManifest) validateWithoutFingerprint() error {
	copy := manifest
	copy.Fingerprint = "fixture-fingerprint-placeholder"
	copy.Cleanup = buildCleanupManifest(copy)
	// Validate the structural portions without recursing through fingerprint.
	if copy.SchemaVersion != FixtureManifestSchemaVersion || copy.RunID == "" || copy.FixtureID == "" {
		return errors.New("fixture manifest identity is incomplete")
	}
	return validateFixtureStructure(copy)
}

func validateFixtureStructure(manifest FixtureManifest) error {
	fingerprint := manifest.Fingerprint
	manifest.Fingerprint = strings.Repeat("0", 64)
	// The public Validate path performs the full fingerprint check. This helper
	// is intentionally only used while creating a fingerprint.
	_ = fingerprint
	return validateFixtureStructureNoFingerprint(manifest)
}

func validateFixtureStructureNoFingerprint(manifest FixtureManifest) error {
	// Keep creation validation aligned with Validate without recursive
	// fingerprint checking.
	if manifest.SchemaVersion != FixtureManifestSchemaVersion {
		return fmt.Errorf("fixture manifest schema_version must be %q", FixtureManifestSchemaVersion)
	}
	if err := validateScopedIdentifier("run_id", manifest.RunID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("fixture_id", manifest.FixtureID); err != nil {
		return err
	}
	if len(manifest.Accounts) == 0 || len(manifest.Players) == 0 || len(manifest.Farms) == 0 {
		return errors.New("fixture manifest requires accounts, players, and farms")
	}
	return nil
}

func (manifest FixtureManifest) computeFingerprint() (string, error) {
	copy := manifest.Canonical()
	copy.Fingerprint = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", fmt.Errorf("encode fixture manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func buildFixtureIndex(manifest FixtureManifest) FixtureIndex {
	index := FixtureIndex{
		AccountToPlayer:    make(map[string]uint64, len(manifest.Accounts)),
		PlayerToAccount:    make(map[uint64]string, len(manifest.Players)),
		PlayerToFarm:       make(map[uint64]uint64, len(manifest.Players)),
		FarmToPlayer:       make(map[uint64]uint64, len(manifest.Farms)),
		FriendIndexForward: make(map[uint64][]uint64),
		FriendIndexReverse: make(map[uint64][]uint64),
		PairByID:           make(map[string]PairParticipants, len(manifest.CanonicalPairs)),
	}
	for _, account := range manifest.Accounts {
		index.AccountToPlayer[account.AccountID] = account.PlayerID
		index.PlayerToAccount[account.PlayerID] = account.AccountID
	}
	for _, farm := range manifest.Farms {
		index.PlayerToFarm[farm.OwnerPlayerID] = farm.FarmID
		index.FarmToPlayer[farm.FarmID] = farm.OwnerPlayerID
	}
	for _, pair := range manifest.CanonicalPairs {
		index.PairByID[pair.PairID] = PairParticipants{LowPlayerID: pair.LowPlayerID, HighPlayerID: pair.HighPlayerID}
		index.FriendIndexForward[pair.LowPlayerID] = append(index.FriendIndexForward[pair.LowPlayerID], pair.HighPlayerID)
		index.FriendIndexForward[pair.HighPlayerID] = append(index.FriendIndexForward[pair.HighPlayerID], pair.LowPlayerID)
		index.FriendIndexReverse[pair.HighPlayerID] = append(index.FriendIndexReverse[pair.HighPlayerID], pair.LowPlayerID)
		index.FriendIndexReverse[pair.LowPlayerID] = append(index.FriendIndexReverse[pair.LowPlayerID], pair.HighPlayerID)
	}
	for _, friends := range []map[uint64][]uint64{index.FriendIndexForward, index.FriendIndexReverse} {
		for playerID := range friends {
			sort.Slice(friends[playerID], func(left, right int) bool { return friends[playerID][left] < friends[playerID][right] })
		}
	}
	return index
}

func farmIDForOwner(farms []FixtureFarm, owner uint64) uint64 {
	for _, farm := range farms {
		if farm.OwnerPlayerID == owner {
			return farm.FarmID
		}
	}
	return 0
}

func buildCleanupManifest(manifest FixtureManifest) CleanupManifest {
	scope := fmt.Sprintf("run/%s/fixture/%s", slugIdentifier(manifest.RunID), slugIdentifier(manifest.FixtureID))
	items := make([]CleanupItem, 0)
	add := func(resource CleanupResource, key string) {
		items = append(items, CleanupItem{Resource: resource, Key: scope + "/" + string(resource) + "/" + key})
	}
	for _, account := range manifest.Accounts {
		add(CleanupAccount, account.AccountID)
	}
	for _, player := range manifest.Players {
		add(CleanupPlayer, fmt.Sprintf("%d", player.PlayerID))
	}
	for _, farm := range manifest.Farms {
		add(CleanupFarm, fmt.Sprintf("%d", farm.FarmID))
	}
	for _, pair := range manifest.CanonicalPairs {
		add(CleanupFriendRelation, pair.PairID)
	}
	for account, player := range manifest.Index.AccountToPlayer {
		add(CleanupIndex, fmt.Sprintf("account-to-player/%s", account))
		add(CleanupIndex, fmt.Sprintf("player-to-account/%d", player))
	}
	for pairID := range manifest.Index.PairByID {
		add(CleanupIndex, fmt.Sprintf("pair-by-id/%s", pairID))
	}
	for player, farm := range manifest.Index.PlayerToFarm {
		add(CleanupIndex, fmt.Sprintf("player-to-farm/%d", player))
		add(CleanupIndex, fmt.Sprintf("farm-to-player/%d", farm))
	}
	for player, friends := range manifest.Index.FriendIndexForward {
		for _, friend := range friends {
			add(CleanupIndex, fmt.Sprintf("friend-forward/%d/%d", player, friend))
		}
	}
	for player, friends := range manifest.Index.FriendIndexReverse {
		for _, friend := range friends {
			add(CleanupIndex, fmt.Sprintf("friend-reverse/%d/%d", player, friend))
		}
	}
	for _, hotspot := range manifest.Hotspots {
		add(CleanupHotspot, hotspot.HotspotID)
	}
	for _, opportunity := range manifest.Opportunities {
		add(CleanupOpportunity, opportunity.OpportunityID)
	}
	sort.Slice(items, func(left, right int) bool { return items[left].Key < items[right].Key })
	return CleanupManifest{SchemaVersion: CleanupManifestSchemaVersion, Scope: scope, Exact: true, Items: items}
}

func (cleanup CleanupManifest) Validate() error {
	if cleanup.SchemaVersion != CleanupManifestSchemaVersion {
		return fmt.Errorf("cleanup schema_version must be %q", CleanupManifestSchemaVersion)
	}
	if !strings.HasPrefix(cleanup.Scope, "run/") || !strings.Contains(cleanup.Scope, "/fixture/") {
		return errors.New("cleanup scope must be run/?/fixture/?")
	}
	if !cleanup.Exact || len(cleanup.Items) == 0 {
		return errors.New("cleanup must be a non-empty exact list")
	}
	seen := make(map[string]struct{}, len(cleanup.Items))
	for index, item := range cleanup.Items {
		if item.Resource == "" || item.Key == "" || strings.ContainsAny(item.Key, "*?[]{}") ||
			strings.Contains(item.Key, "..") || strings.ContainsAny(item.Key, " \t\r\n") {
			return fmt.Errorf("cleanup item %d is not an exact safe key", index)
		}
		if !strings.HasPrefix(item.Key, cleanup.Scope+"/") {
			return fmt.Errorf("cleanup item %q escapes its run scope", item.Key)
		}
		if index > 0 && cleanup.Items[index-1].Key >= item.Key {
			return errors.New("cleanup items must be strictly sorted and unique")
		}
		if _, exists := seen[item.Key]; exists {
			return fmt.Errorf("duplicate cleanup key %q", item.Key)
		}
		seen[item.Key] = struct{}{}
		switch item.Resource {
		case CleanupAccount, CleanupPlayer, CleanupFarm, CleanupFriendRelation, CleanupIndex, CleanupHotspot, CleanupOpportunity:
		default:
			return fmt.Errorf("unknown cleanup resource %q", item.Resource)
		}
	}
	return nil
}

func (cleanup CleanupManifest) Keys() []string {
	result := make([]string, 0, len(cleanup.Items))
	for _, item := range cleanup.Items {
		result = append(result, item.Key)
	}
	return result
}
func (manifest FixtureManifest) Validate() error {
	if err := manifest.validateStructure(); err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Fingerprint) == "" {
		return errors.New("fixture manifest fingerprint is required")
	}
	expectedFingerprint, err := manifest.computeFingerprint()
	if err != nil {
		return err
	}
	if manifest.Fingerprint != expectedFingerprint {
		return errors.New("fixture manifest fingerprint mismatch")
	}
	return nil
}
