package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// CanonicalGuardSnapshotHash covers schema_version + guard_version +
// active_guard_pet_id + guard_until_ms + rule of the canonical encoding,
// never the hash itself (design 36 §6.2). It is deterministic and free of
// any unstable JSON/protobuf default serialization.
func CanonicalGuardSnapshotHash(snapshot GuardSnapshot) string {
	// This is the same explicit fixed-order big-endian layout used by the
	// Player pet domain. Keeping the Farm copy here avoids hashing unstable
	// protobuf/JSON encodings while preserving byte-identical source/order/
	// target snapshot hashes.
	var canonical bytes.Buffer
	write := func(value any) { _ = binary.Write(&canonical, binary.BigEndian, value) }
	write(snapshot.SchemaVersion)
	write(snapshot.GuardVersion)
	write(int32(snapshot.ActiveGuardPetID))
	write(snapshot.GuardUntilMs)
	write(snapshot.Rule.PetConfigVersion)
	write(snapshot.Rule.GuardRuleVersion)
	write(snapshot.Rule.TriggerProbabilityBP)
	write(uint32(snapshot.Rule.InterceptPolicy))
	write(snapshot.Rule.RandomAlgorithmVersion)
	write(snapshot.Rule.RandomKeyVersion)
	sum := sha256.Sum256(canonical.Bytes())
	return hex.EncodeToString(sum[:])
}

// ApplyGuardSnapshot applies an incoming GuardSync snapshot monotonically
// (design 36 §6.4):
//
//	higher guard_version            -> install, APPLIED
//	same version, same hash         -> idempotent replay, no state change
//	lower guard_version             -> SUPERSEDED, never overwrite
//	same version, different hash    -> INVARIANT_VIOLATION, keep current
//
// AppearanceChanged tells the caller whether the public guard pet appearance
// changed (a real Switch): only then may the caller advance the public
// farm_version and emit one FarmMetaDelta; a hidden Feed install only
// advances record_version. This function never produces a FarmChange by
// itself.
func ApplyGuardSnapshot(state FarmState, sourcePlayerID, targetFarmID uint64, incoming GuardSnapshot) (CandidateState, GuardApplyDecision, *Error) {
	if sourcePlayerID != state.OwnerPlayerID || targetFarmID != state.FarmID {
		return CandidateState{}, GuardApplyDecision{}, reject(CodeGuardOwnerMismatch,
			"source player %d / target farm %d do not match owner %d / farm %d",
			sourcePlayerID, targetFarmID, state.OwnerPlayerID, state.FarmID)
	}
	if incoming.SnapshotHash == "" {
		incoming.SnapshotHash = CanonicalGuardSnapshotHash(incoming)
	}
	current := state.Guard
	decision := GuardApplyDecision{
		AppliedGuardVersion: current.GuardVersion,
		AppliedSnapshotHash: current.SnapshotHash,
	}
	switch {
	case incoming.GuardVersion < current.GuardVersion:
		decision.Outcome = GuardApplySuperseded
		return CandidateState{}, decision, nil
	case incoming.GuardVersion == current.GuardVersion && current.GuardVersion != 0:
		if current.SnapshotHash == incoming.SnapshotHash {
			decision.Outcome = GuardApplyReplayed
			return CandidateState{}, decision, nil
		}
		decision.Outcome = GuardApplyInvariantViolation
		return CandidateState{}, decision, reject(CodeInvalidArgument,
			"guard version %d has conflicting snapshot hashes %q vs %q",
			incoming.GuardVersion, current.SnapshotHash, incoming.SnapshotHash)
	}

	candidate := state.Clone()
	candidate.Guard = incoming
	decision.Outcome = GuardApplyApplied
	decision.AppliedGuardVersion = incoming.GuardVersion
	decision.AppliedSnapshotHash = incoming.SnapshotHash
	decision.AppearanceChanged = current.ActiveGuardPetID != incoming.ActiveGuardPetID
	return CandidateState{State: candidate}, decision, nil
}
