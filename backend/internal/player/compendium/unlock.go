package compendium

// UnlockSnapshot is the player-domain mirror of the CompendiumUnlockSnapshot
// frozen at Plant time and persisted with the CropCycle (docs/design/32
// §3.3, docs/design/37 §3.1). The Farm domain already carries the wire-side
// copy; the integrator maps HarvestDecision.Compendium into this struct
// field-by-field. Activity end, catalog hot updates, retries and Actor
// migration must never re-evaluate it.
type UnlockSnapshot struct {
	CropDefinitionID         uint32
	UnlockEligible           bool
	PlantedByPlayerID        uint64
	ActivityID               string
	CatalogVersionAtPlant    uint64
	DefinitionVersionAtPlant uint64
}

// UnlockCommand is the deterministic adjudication input of one Harvest
// reward commit, restricted to the compendium slice (docs/design/37 §3.1).
type UnlockCommand struct {
	// PlayerID is the current PlayerActor owner.
	PlayerID uint64
	// OwnerPlayerID is HarvestDecision.owner_player_id.
	OwnerPlayerID uint64
	// CropDefinitionID is HarvestDecision.crop_definition_id.
	CropDefinitionID uint32
	// Snapshot is the frozen snapshot copied unchanged from the CropCycle.
	Snapshot UnlockSnapshot
}

// UnlockDelta is the original per-operation result embedded in the Harvest
// ParticipantReceipt and StoredOperationOutcome (docs/design/37 §3.2). It
// must be persisted verbatim: a retry returns the original true/false, entry
// ID and revision of its own first commit and never recomputes them from the
// current set.
type UnlockDelta struct {
	NewlyUnlocked    bool
	CropDefinitionID uint32 // only set when NewlyUnlocked
	RevisionAfter    uint64
}

// UnlockOutcome is the result of DecideUnlock.
type UnlockOutcome struct {
	Status ActionStatus
	Err    *Error
	// Candidate carries the post-transition state iff Status ==
	// StatusSucceeded; the input state is never modified.
	Candidate State
	// Delta is always populated, including NO_CHANGE: the integrator
	// persists it with the Harvest outcome either way.
	Delta UnlockDelta
}

// DecideUnlock adjudicates the compendium slice of one successful Harvest
// (docs/design/37 §3.1). The single P0 unlock condition is "planted by
// myself on my own farm, and successfully harvested by myself":
//
//	snapshot.unlock_eligible == true
//	snapshot.crop_definition_id == HarvestDecision.crop_definition_id
//	snapshot.planted_by_player_id == HarvestDecision.owner_player_id
//	HarvestDecision.owner_player_id == current PlayerActor.player_id
//
// Stolen crops, friend care rewards, mail attachments and merely-held legacy
// seeds all fail this check and produce a NO_CHANGE delta with
// newly_unlocked=false — the harvest reward itself is unaffected (it is
// settled outside this package). A repeated harvest of an already-unlocked
// plant is likewise NO_CHANGE: the set is idempotent and the revision does
// not grow.
//
// On a genuine first unlock the transition checks the LimitProfile first
// and fails closed (REJECTED / WORK_CAPACITY_EXCEEDED) instead of silently
// skipping the permanent membership (docs/design/37 §5.4).
func DecideUnlock(state State, cmd UnlockCommand, limits LimitProfile) UnlockOutcome {
	noop := func() UnlockOutcome {
		return UnlockOutcome{
			Status: StatusNoChange,
			Delta:  UnlockDelta{NewlyUnlocked: false, RevisionAfter: state.Revision},
		}
	}

	if cmd.PlayerID == 0 || cmd.OwnerPlayerID == 0 || cmd.CropDefinitionID == 0 {
		return UnlockOutcome{Status: StatusRejected,
			Err: reject(ErrInvalidArgument, "player/owner/crop ids must be non-zero")}
	}
	snap := cmd.Snapshot
	if !snap.UnlockEligible ||
		snap.CropDefinitionID != cmd.CropDefinitionID ||
		snap.PlantedByPlayerID != cmd.OwnerPlayerID ||
		cmd.OwnerPlayerID != cmd.PlayerID {
		// Not the unique P0 unlock condition (docs/design/37 §3.1 table):
		// stolen crops, care rewards, mail grants, other-player plants and
		// non-collectible definitions never unlock.
		return noop()
	}
	if _, ok := state.findFact(cmd.CropDefinitionID); ok {
		// Already a permanent member: idempotent NO_CHANGE, no revision
		// growth, reward still flows outside this package.
		return noop()
	}

	// First unlock: enforce the capacity fence before mutating anything.
	if err := limits.Validate(); err != nil {
		return UnlockOutcome{Status: StatusRejected, Err: err}
	}
	if uint32(len(state.Facts))+1 > limits.MaxUnlockedEntries {
		return UnlockOutcome{Status: StatusRejected,
			Err: reject(ErrWorkCapacityExceeded,
				"unlock of crop %d would exceed max_unlocked_entries %d",
				cmd.CropDefinitionID, limits.MaxUnlockedEntries)}
	}
	candidate := state.Clone()
	candidate.Facts = insertFact(candidate.Facts, UnlockFact{
		CropDefinitionID:          cmd.CropDefinitionID,
		UnlockRevision:            state.Revision + 1,
		SourceActivityID:          snap.ActivityID,
		CatalogVersionAtUnlock:    snap.CatalogVersionAtPlant,
		DefinitionVersionAtUnlock: snap.DefinitionVersionAtPlant,
	})
	candidate.Revision = state.Revision + 1
	if EstimateStateBytes(candidate) > uint64(limits.MaxCompendiumStateBytes) {
		return UnlockOutcome{Status: StatusRejected,
			Err: reject(ErrWorkCapacityExceeded,
				"unlock of crop %d would exceed max_compendium_state_bytes %d",
				cmd.CropDefinitionID, limits.MaxCompendiumStateBytes)}
	}

	return UnlockOutcome{
		Status:    StatusSucceeded,
		Candidate: candidate,
		Delta: UnlockDelta{
			NewlyUnlocked:    true,
			CropDefinitionID: cmd.CropDefinitionID,
			RevisionAfter:    candidate.Revision,
		},
	}
}
