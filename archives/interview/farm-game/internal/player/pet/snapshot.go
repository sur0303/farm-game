package pet

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

// GuardSnapshotSchemaVersion is the schema of GuardSnapshot. Bump only via a
// new contract version.
const GuardSnapshotSchemaVersion uint64 = 1

// GuardSnapshot is the immutable, versioned copy installed into FarmActor
// for theft adjudication (docs/design/36 §6.2). It is a complete target
// snapshot, never a delta; FarmActor decides solely from it plus its trusted
// clock and never queries PlayerActor or hot pet config during a
// TheftAttempt.
//
// The snapshot deliberately carries no owner field: the farm_id of the
// receiving FarmActor already identifies the owner. It also carries no
// mutable PlayerState (no wallet, no inventory, no owned set).
type GuardSnapshot struct {
	SchemaVersion    uint64
	GuardVersion     uint64
	ActiveGuardPetID int32 // 0 is only legal while no pet has ever been selected
	GuardUntilMs     int64
	Rule             GuardRuleSnapshot
}

// Hash returns the canonical content hash of the snapshot
// (docs/design/36 §6.2 snapshot_hash). It covers
// schema_version + guard_version + active_guard_pet_id + guard_until_ms +
// rule, encoded as an explicit fixed-order big-endian binary layout. It does
// not cover itself, and it never hashes unstable JSON or default Protobuf
// bytes.
func (s GuardSnapshot) Hash() [32]byte {
	var buf bytes.Buffer
	buf.Grow(128)
	writeU64 := func(v uint64) { _ = binary.Write(&buf, binary.BigEndian, v) }
	writeU32 := func(v uint32) { _ = binary.Write(&buf, binary.BigEndian, v) }
	writeI64 := func(v int64) { _ = binary.Write(&buf, binary.BigEndian, v) }
	writeI32 := func(v int32) { _ = binary.Write(&buf, binary.BigEndian, v) }

	writeU64(s.SchemaVersion)
	writeU64(s.GuardVersion)
	writeI32(s.ActiveGuardPetID)
	writeI64(s.GuardUntilMs)
	writeU64(s.Rule.PetConfigVersion)
	writeU64(s.Rule.GuardRuleVersion)
	writeU32(s.Rule.TriggerProbabilityBp)
	writeU32(uint32(s.Rule.InterceptPolicy))
	writeU64(s.Rule.RandomAlgorithmVersion)
	writeU64(s.Rule.RandomKeyVersion)
	return sha256.Sum256(buf.Bytes())
}

// GuardSyncInput is the immutable payload the PlayerActor Authority layer
// uses to build the GuardSyncOrder (docs/design/36 §6.3). It carries the
// complete snapshot decided in the same turn as the PetState change; the
// order identity (StableOpaqueID over GUARD_SYNC_ORDER / PLAYER_ACTOR /
// player_id / PET_GUARD_SLOT / guard_version), target farm resolution,
// DueIndex insert and delivery all belong to the Actor/Workflow layer.
type GuardSyncInput struct {
	GuardVersion uint64
	Snapshot     GuardSnapshot
	SnapshotHash [32]byte
}

// BuildGuardSyncInput derives the GuardSync input from a state that has just
// committed a feed or a real switch. It returns ok=false when the state has
// never produced a guard version (unlock alone does not sync).
//
// The returned SnapshotHash is computed once here; the Authority layer must
// embed exactly these bytes into both PetState persistence and the
// GuardSyncOrder so that acceptance item 24 (byte-identical canonical hash
// between PetState rule snapshot and the order) holds by construction.
func BuildGuardSyncInput(s State) (GuardSyncInput, bool) {
	if s.GuardVersion == 0 || s.GuardRuleSnapshot == nil {
		return GuardSyncInput{}, false
	}
	snap := GuardSnapshot{
		SchemaVersion:    GuardSnapshotSchemaVersion,
		GuardVersion:     s.GuardVersion,
		ActiveGuardPetID: s.ActiveGuardPetID,
		GuardUntilMs:     s.GuardUntilMs,
		Rule:             *s.GuardRuleSnapshot,
	}
	return GuardSyncInput{
		GuardVersion: s.GuardVersion,
		Snapshot:     snap,
		SnapshotHash: snap.Hash(),
	}, true
}
