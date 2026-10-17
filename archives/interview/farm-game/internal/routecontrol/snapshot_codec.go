package routecontrol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"farmgame/internal/actorrouting"
)

const (
	// SnapshotSchemaVersion is the durable envelope schema. A schema change
	// requires an explicit migration; it must not be silently decoded as v1.
	SnapshotSchemaVersion = "route-snapshot-v1"

	topologyFieldLimit = uint32(^uint32(0))
)

var (
	ErrSnapshotEnvelopeMalformed = errors.New("routecontrol: malformed snapshot envelope")
	ErrSnapshotSchemaMismatch    = errors.New("routecontrol: snapshot schema mismatch")
	ErrSnapshotChecksumMismatch  = errors.New("routecontrol: snapshot checksum mismatch")
	ErrSnapshotTopologyMismatch  = errors.New("routecontrol: snapshot topology fingerprint mismatch")
)

// StoredRouteSnapshot is the validated durable projection of one complete
// RouteSnapshot. CommittedAtMs is metadata and is deliberately excluded from
// EnvelopeEqual so an exact replay does not create a new revision.
type StoredRouteSnapshot struct {
	SchemaVersion       string
	Realm               string
	Revision            uint64
	CanonicalJSON       []byte
	ChecksumSHA256      string
	TopologyFingerprint string
	CommittedAtMs       int64
	Snapshot            actorrouting.RouteSnapshot
}

// SnapshotEnvelopeFields is the fixed-field representation used by Redis
// adapters. It keeps decoding independent from Redis hash layout.
type SnapshotEnvelopeFields struct {
	SchemaVersion       string
	Realm               string
	Revision            string
	CanonicalJSON       []byte
	ChecksumSHA256      string
	TopologyFingerprint string
	CommittedAtMs       string
}

// CanonicalizeRouteSnapshot validates the complete route shape using the
// existing routing model, then marshals that validated model exactly once.
func CanonicalizeRouteSnapshot(snapshot actorrouting.RouteSnapshot) (StoredRouteSnapshot, error) {
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: %v", ErrSnapshotEnvelopeMalformed, err)
	}
	canonicalJSON, err := json.Marshal(snapshot)
	if err != nil {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: canonical JSON: %v", ErrSnapshotEnvelopeMalformed, err)
	}
	return storedFromCanonical(snapshot, canonicalJSON, 0)
}

// EncodeRouteSnapshot is an alias kept for callers that use codec terminology.
func EncodeRouteSnapshot(snapshot actorrouting.RouteSnapshot) (StoredRouteSnapshot, error) {
	return CanonicalizeRouteSnapshot(snapshot)
}

// DecodeStoredRouteSnapshot verifies every envelope field, including exact
// canonical JSON bytes. Whitespace/key-order variants are rejected.
func DecodeStoredRouteSnapshot(fields SnapshotEnvelopeFields) (StoredRouteSnapshot, error) {
	if fields.SchemaVersion != SnapshotSchemaVersion {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: got %q", ErrSnapshotSchemaMismatch, fields.SchemaVersion)
	}
	revision, err := strconv.ParseUint(fields.Revision, 10, 64)
	if err != nil || fields.Revision == "" || strconv.FormatUint(revision, 10) != fields.Revision {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: revision %q", ErrSnapshotEnvelopeMalformed, fields.Revision)
	}
	committedAtMs, err := strconv.ParseInt(fields.CommittedAtMs, 10, 64)
	if err != nil || committedAtMs < 0 || strconv.FormatInt(committedAtMs, 10) != fields.CommittedAtMs {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: committed_at_ms %q", ErrSnapshotEnvelopeMalformed, fields.CommittedAtMs)
	}
	snapshot, err := actorrouting.ParseRouteSnapshotJSON(fields.CanonicalJSON)
	if err != nil {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: route JSON: %v", ErrSnapshotEnvelopeMalformed, err)
	}
	if snapshot.GlobalRevision != revision {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: envelope revision=%d snapshot revision=%d", ErrSnapshotEnvelopeMalformed, revision, snapshot.GlobalRevision)
	}
	if snapshot.Realm != fields.Realm {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: envelope realm=%q snapshot realm=%q", ErrSnapshotEnvelopeMalformed, fields.Realm, snapshot.Realm)
	}
	canonical, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return StoredRouteSnapshot{}, err
	}
	if string(canonical.CanonicalJSON) != string(fields.CanonicalJSON) {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: non-canonical JSON", ErrSnapshotEnvelopeMalformed)
	}
	if fields.ChecksumSHA256 != canonical.ChecksumSHA256 {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: got %q want %q", ErrSnapshotChecksumMismatch, fields.ChecksumSHA256, canonical.ChecksumSHA256)
	}
	if fields.TopologyFingerprint != canonical.TopologyFingerprint {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: got %q want %q", ErrSnapshotTopologyMismatch, fields.TopologyFingerprint, canonical.TopologyFingerprint)
	}
	canonical.CommittedAtMs = committedAtMs
	return canonical, nil
}

func snapshotEnvelopeFields(stored StoredRouteSnapshot) SnapshotEnvelopeFields {
	return SnapshotEnvelopeFields{
		SchemaVersion:       stored.SchemaVersion,
		Realm:               stored.Realm,
		Revision:            strconv.FormatUint(stored.Revision, 10),
		CanonicalJSON:       append([]byte(nil), stored.CanonicalJSON...),
		ChecksumSHA256:      stored.ChecksumSHA256,
		TopologyFingerprint: stored.TopologyFingerprint,
		CommittedAtMs:       strconv.FormatInt(stored.CommittedAtMs, 10),
	}
}

func (s StoredRouteSnapshot) EnvelopeFields() SnapshotEnvelopeFields {
	return snapshotEnvelopeFields(s)
}

func validateStoredRouteSnapshot(stored StoredRouteSnapshot) error {
	decoded, err := DecodeStoredRouteSnapshot(snapshotEnvelopeFields(stored))
	if err != nil {
		return err
	}
	if decoded.CommittedAtMs != stored.CommittedAtMs ||
		!decoded.EnvelopeEqual(stored) ||
		!reflect.DeepEqual(decoded.Snapshot, stored.Snapshot) {
		return fmt.Errorf("%w: stored projection differs from its envelope", ErrSnapshotEnvelopeMalformed)
	}
	return nil
}

// TopologyBytes returns the versioned, length-prefixed topology material.
// Numeric values are fixed-width big-endian fields; variable text fields use
// LP(u32be byte length || UTF-8 bytes). Holder and assignment details are
// intentionally absent.
func TopologyBytes(snapshot actorrouting.RouteSnapshot) ([]byte, error) {
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotEnvelopeMalformed, err)
	}
	out := make([]byte, 0, 64+len(snapshot.Realm))
	var err error
	if out, err = appendTopologyLP(out, SnapshotSchemaVersion); err != nil {
		return nil, err
	}
	if out, err = appendTopologyLP(out, snapshot.Realm); err != nil {
		return nil, err
	}
	out = binary.BigEndian.AppendUint32(out, snapshot.HashVersion)
	out = binary.BigEndian.AppendUint64(out, snapshot.HashSeed)
	out = binary.BigEndian.AppendUint32(out, snapshot.Player.ShardCount)
	out = binary.BigEndian.AppendUint32(out, snapshot.Farm.ShardCount)
	if snapshot.FarmWorker.ShardCount != 0 || len(snapshot.FarmWorker.Routes) != 0 {
		out = binary.BigEndian.AppendUint32(out, snapshot.FarmWorker.ShardCount)
	}
	return out, nil
}

// TopologyFingerprint returns the lowercase SHA-256 of TopologyBytes.
func TopologyFingerprint(snapshot actorrouting.RouteSnapshot) (string, error) {
	topology, err := TopologyBytes(snapshot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(topology)
	return hex.EncodeToString(sum[:]), nil
}

func storedFromCanonical(snapshot actorrouting.RouteSnapshot, canonicalJSON []byte, committedAtMs int64) (StoredRouteSnapshot, error) {
	if committedAtMs < 0 {
		return StoredRouteSnapshot{}, fmt.Errorf("%w: negative committed_at_ms", ErrSnapshotEnvelopeMalformed)
	}
	checksum := sha256.Sum256(canonicalJSON)
	topology, err := TopologyFingerprint(snapshot)
	if err != nil {
		return StoredRouteSnapshot{}, err
	}
	return StoredRouteSnapshot{
		SchemaVersion:       SnapshotSchemaVersion,
		Realm:               snapshot.Realm,
		Revision:            snapshot.GlobalRevision,
		CanonicalJSON:       append([]byte(nil), canonicalJSON...),
		ChecksumSHA256:      hex.EncodeToString(checksum[:]),
		TopologyFingerprint: topology,
		CommittedAtMs:       committedAtMs,
		Snapshot:            actorrouting.CloneRouteSnapshot(snapshot),
	}, nil
}

func appendTopologyLP(dst []byte, value string) ([]byte, error) {
	if uint64(len([]byte(value))) > uint64(topologyFieldLimit) {
		return nil, fmt.Errorf("%w: topology field too large", ErrSnapshotEnvelopeMalformed)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len([]byte(value))))
	dst = append(dst, value...)
	return dst, nil
}

func (s StoredRouteSnapshot) EnvelopeEqual(other StoredRouteSnapshot) bool {
	return s.SchemaVersion == other.SchemaVersion &&
		s.Realm == other.Realm &&
		s.Revision == other.Revision &&
		s.ChecksumSHA256 == other.ChecksumSHA256 &&
		s.TopologyFingerprint == other.TopologyFingerprint &&
		string(s.CanonicalJSON) == string(other.CanonicalJSON)
}

func (s StoredRouteSnapshot) Clone() StoredRouteSnapshot {
	s.CanonicalJSON = append([]byte(nil), s.CanonicalJSON...)
	s.Snapshot = actorrouting.CloneRouteSnapshot(s.Snapshot)
	return s
}
