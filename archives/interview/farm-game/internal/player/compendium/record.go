package compendium

import (
	"fmt"
	"sort"

	playerpb "farmgame/api/gen/player"
)

// StateFromPlayerRecord is the only PlayerRecord <-> compendium domain
// mapping. Keeping it in the domain package prevents Harvest and the public
// read/Ack ingress from inventing subtly different migration rules.
//
// Legacy records (entries written before revision/provenance fields existed)
// are upgraded deterministically in memory. Historical provenance remains
// unknown; source_operation_id is audit data and is deliberately not reused
// as source_activity_id. Legacy entries are treated as already seen so a
// rollout does not create a false red dot for every historical plant.
func StateFromPlayerRecord(rec *playerpb.PlayerRecord, limits LimitProfile) (State, error) {
	if rec == nil {
		return State{}, fmt.Errorf("compendium: nil player record")
	}
	if err := limits.Validate(); err != nil {
		return State{}, fmt.Errorf("compendium: invalid limits: %v", err)
	}

	type indexed struct {
		entry *playerpb.CodexEntry
		index int
	}
	entries := make([]indexed, 0, len(rec.GetCodexEntries()))
	allLegacy := rec.GetCompendiumRevision() == 0 && rec.GetCompendiumSeenRevision() == 0
	for i, entry := range rec.GetCodexEntries() {
		if entry == nil {
			return State{}, fmt.Errorf("compendium: nil unlock fact")
		}
		if entry.GetCropDefinitionId() <= 0 {
			return State{}, fmt.Errorf("compendium: invalid crop_definition_id %d", entry.GetCropDefinitionId())
		}
		if entry.GetUnlockRevision() != 0 || entry.GetSourceActivityId() != "" ||
			entry.GetCatalogVersionAtUnlock() != 0 || entry.GetDefinitionVersionAtUnlock() != 0 {
			allLegacy = false
		}
		entries = append(entries, indexed{entry: entry, index: i})
	}

	legacyRevision := make(map[int32]uint64, len(entries))
	if allLegacy && len(entries) > 0 {
		byFirstHarvest := append([]indexed(nil), entries...)
		sort.Slice(byFirstHarvest, func(i, j int) bool {
			left, right := byFirstHarvest[i].entry, byFirstHarvest[j].entry
			if left.GetFirstHarvestedAtMs() != right.GetFirstHarvestedAtMs() {
				return left.GetFirstHarvestedAtMs() < right.GetFirstHarvestedAtMs()
			}
			if left.GetCropDefinitionId() != right.GetCropDefinitionId() {
				return left.GetCropDefinitionId() < right.GetCropDefinitionId()
			}
			return byFirstHarvest[i].index < byFirstHarvest[j].index
		})
		for i, item := range byFirstHarvest {
			legacyRevision[item.entry.GetCropDefinitionId()] = uint64(i + 1)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.GetCropDefinitionId() < entries[j].entry.GetCropDefinitionId()
	})
	facts := make([]UnlockFact, 0, len(entries))
	seenCrop := make(map[int32]struct{}, len(entries))
	seenRevision := make(map[uint64]struct{}, len(entries))
	maxRevision := rec.GetCompendiumRevision()
	for _, item := range entries {
		entry := item.entry
		if _, duplicate := seenCrop[entry.GetCropDefinitionId()]; duplicate {
			return State{}, fmt.Errorf("compendium: duplicate crop_definition_id %d", entry.GetCropDefinitionId())
		}
		seenCrop[entry.GetCropDefinitionId()] = struct{}{}
		unlockRevision := entry.GetUnlockRevision()
		if allLegacy {
			unlockRevision = legacyRevision[entry.GetCropDefinitionId()]
		}
		if unlockRevision == 0 {
			return State{}, fmt.Errorf("compendium: zero unlock_revision for crop %d", entry.GetCropDefinitionId())
		}
		if _, duplicate := seenRevision[unlockRevision]; duplicate {
			return State{}, fmt.Errorf("compendium: duplicate unlock_revision %d", unlockRevision)
		}
		seenRevision[unlockRevision] = struct{}{}
		if unlockRevision > maxRevision {
			maxRevision = unlockRevision
		}
		facts = append(facts, UnlockFact{
			CropDefinitionID:          uint32(entry.GetCropDefinitionId()),
			UnlockRevision:            unlockRevision,
			SourceActivityID:          entry.GetSourceActivityId(),
			CatalogVersionAtUnlock:    entry.GetCatalogVersionAtUnlock(),
			DefinitionVersionAtUnlock: entry.GetDefinitionVersionAtUnlock(),
		})
	}
	if !allLegacy {
		if rec.GetCompendiumRevision() == 0 {
			return State{}, fmt.Errorf("compendium: missing authoritative revision")
		}
		if maxRevision != rec.GetCompendiumRevision() {
			return State{}, fmt.Errorf("compendium: fact revision %d exceeds authoritative revision %d", maxRevision, rec.GetCompendiumRevision())
		}
		if len(seenRevision) != int(rec.GetCompendiumRevision()) {
			return State{}, fmt.Errorf("compendium: unlock revisions are not contiguous through %d", rec.GetCompendiumRevision())
		}
		for revision := uint64(1); revision <= rec.GetCompendiumRevision(); revision++ {
			if _, ok := seenRevision[revision]; !ok {
				return State{}, fmt.Errorf("compendium: missing unlock_revision %d", revision)
			}
		}
	}
	if allLegacy {
		maxRevision = uint64(len(facts))
	}
	seen := rec.GetCompendiumSeenRevision()
	if allLegacy {
		seen = maxRevision
	}
	if seen > maxRevision {
		return State{}, fmt.Errorf("compendium: seen_revision %d exceeds revision %d", seen, maxRevision)
	}
	if len(facts) > int(limits.MaxUnlockedEntries) {
		return State{}, fmt.Errorf("compendium: %d facts exceed limit %d", len(facts), limits.MaxUnlockedEntries)
	}
	state := State{Facts: facts, Revision: maxRevision, SeenRevision: seen}
	if EstimateStateBytes(state) > uint64(limits.MaxCompendiumStateBytes) {
		return State{}, fmt.Errorf("compendium: state exceeds byte limit %d", limits.MaxCompendiumStateBytes)
	}
	return state, nil
}

// ApplyStateToPlayerRecord writes a complete candidate, preserving the
// audit-only harvest fields of existing facts. The authoritative set and both
// revision watermarks move together in one PlayerActor commit.
func ApplyStateToPlayerRecord(rec *playerpb.PlayerRecord, state State, operationID string, nowMs int64) error {
	if rec == nil {
		return fmt.Errorf("compendium: nil player record")
	}
	existing := make(map[uint32]*playerpb.CodexEntry, len(rec.GetCodexEntries()))
	for _, entry := range rec.GetCodexEntries() {
		if entry != nil && entry.GetCropDefinitionId() > 0 {
			existing[uint32(entry.GetCropDefinitionId())] = entry
		}
	}
	entries := make([]*playerpb.CodexEntry, 0, len(state.Facts))
	for _, fact := range state.Facts {
		if fact.CropDefinitionID == 0 || uint64(fact.CropDefinitionID) > uint64(^uint32(0)>>1) {
			return fmt.Errorf("compendium: crop id %d does not fit player record", fact.CropDefinitionID)
		}
		firstHarvestedAt, sourceOperationID := nowMs, operationID
		if old := existing[fact.CropDefinitionID]; old != nil {
			firstHarvestedAt, sourceOperationID = old.GetFirstHarvestedAtMs(), old.GetSourceOperationId()
		}
		entries = append(entries, &playerpb.CodexEntry{
			CropDefinitionId:          int32(fact.CropDefinitionID),
			FirstHarvestedAtMs:        firstHarvestedAt,
			SourceOperationId:         sourceOperationID,
			UnlockRevision:            fact.UnlockRevision,
			SourceActivityId:          fact.SourceActivityID,
			CatalogVersionAtUnlock:    fact.CatalogVersionAtUnlock,
			DefinitionVersionAtUnlock: fact.DefinitionVersionAtUnlock,
		})
	}
	rec.CodexEntries = entries
	rec.CompendiumRevision = state.Revision
	rec.CompendiumSeenRevision = state.SeenRevision
	return nil
}
