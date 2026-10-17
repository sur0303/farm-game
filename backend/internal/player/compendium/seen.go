package compendium

// SeenOutcome is the result of AckSeen.
type SeenOutcome struct {
	Status ActionStatus
	Err    *Error
	// Candidate carries the post-transition state iff Status ==
	// StatusSucceeded; the input state is never modified.
	Candidate State
	// SeenAfter mirrors AckCompendiumSeenResult.compendium_seen_revision_after.
	SeenAfter uint64
	// CurrentRevision mirrors AckCompendiumSeenResult.current_compendium_revision.
	CurrentRevision uint64
}

// AckSeen applies the monotonic seen fence of ACK_COMPENDIUM_SEEN
// (docs/design/37 §5.2, docs/design/28 §5.9):
//
//	seen_after = max(current_seen_revision,
//	                 min(observed_revision, current_compendium_revision))
//
// A late Ack(5) after a concurrent unlock to revision 6 advances the
// watermark only to 5 and leaves the revision-6 red dot intact; an old or
// repeated Ack never regresses the watermark and returns NO_CHANGE /
// COMPENDIUM_SEEN_ALREADY_ADVANCED. This is a low-risk monotonic UI Ack: it
// enters no long-term operation journal and a lost Ack at worst repeats the
// red dot.
func AckSeen(state State, observedRevision uint64) SeenOutcome {
	seenAfter := state.SeenRevision
	if observedRevision > seenAfter {
		seenAfter = observedRevision
	}
	if seenAfter > state.Revision {
		seenAfter = state.Revision
	}
	if seenAfter == state.SeenRevision {
		return SeenOutcome{
			Status:          StatusNoChange,
			Err:             reject(ErrSeenAlreadyAdvanced, "observed_revision %d does not advance seen_revision %d", observedRevision, state.SeenRevision),
			SeenAfter:       state.SeenRevision,
			CurrentRevision: state.Revision,
		}
	}
	candidate := state.Clone()
	candidate.SeenRevision = seenAfter
	return SeenOutcome{
		Status:          StatusSucceeded,
		Candidate:       candidate,
		SeenAfter:       seenAfter,
		CurrentRevision: state.Revision,
	}
}
