package routecontrol

import (
	"encoding/hex"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
)

func TestSnapshotCodecCanonicalJSONAndTopologyGolden(t *testing.T) {
	stored, err := CanonicalizeRouteSnapshot(testSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"realm":"realm-a","global_revision":1,"hash_version":2,"hash_seed":202608030001,"player":{"shard_count":1,"routes":[{"holder_index":0,"state":"ACTIVE","shard_revision":1,"expected_writer_epoch":1,"expected_lease_epoch":1,"lease_expire_at_ms":1000}]},"farm":{"shard_count":1,"routes":[{"holder_index":1,"state":"ACTIVE","shard_revision":1,"expected_writer_epoch":1,"expected_lease_epoch":1,"lease_expire_at_ms":1000}]},"holders":[{"instance_id":"player-1","boot_id":"boot-player","endpoint":"player:1","state":"ACTIVE","weight":1},{"instance_id":"farm-1","boot_id":"boot-farm","endpoint":"farm:1","state":"ACTIVE","weight":1}]}`
	if string(stored.CanonicalJSON) != wantJSON {
		t.Fatalf("canonical JSON drift:\n got %s\nwant %s", stored.CanonicalJSON, wantJSON)
	}
	const wantChecksum = "ca50f035b4d08017999ce880bf7ed8334b258d5b3a0476a6b34e87c49150e90c"
	if stored.ChecksumSHA256 != wantChecksum {
		t.Fatalf("checksum=%s want=%s", stored.ChecksumSHA256, wantChecksum)
	}
	topology, err := TopologyBytes(testSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	const wantTopologyHex = "00000011726f7574652d736e617073686f742d7631000000077265616c6d2d61000000020000002f2c6131310000000100000001"
	if got := hex.EncodeToString(topology); got != wantTopologyHex {
		t.Fatalf("topology=%s want=%s", got, wantTopologyHex)
	}
	if got, err := TopologyFingerprint(testSnapshot(2)); err != nil || got != stored.TopologyFingerprint {
		t.Fatalf("revision changed topology got=%s err=%v want=%s", got, err, stored.TopologyFingerprint)
	}
	moved := testSnapshot(1)
	moved.Holders[0].Endpoint = "player:other"
	moved.Player.Routes[0].LeaseUntilMs = 2000
	moved.GlobalRevision = 2
	movedStored, err := CanonicalizeRouteSnapshot(moved)
	if err != nil {
		t.Fatal(err)
	}
	if movedStored.TopologyFingerprint != stored.TopologyFingerprint {
		t.Fatal("holder/lease changes changed topology fingerprint")
	}
}

func TestDecodeStoredRouteSnapshotFailsClosed(t *testing.T) {
	stored, err := CanonicalizeRouteSnapshot(testSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	fields := SnapshotEnvelopeFields{
		SchemaVersion:       stored.SchemaVersion,
		Realm:               stored.Realm,
		Revision:            "1",
		CanonicalJSON:       stored.CanonicalJSON,
		ChecksumSHA256:      stored.ChecksumSHA256,
		TopologyFingerprint: stored.TopologyFingerprint,
		CommittedAtMs:       "100",
	}
	for name, mutate := range map[string]func(*SnapshotEnvelopeFields){
		"schema":             func(f *SnapshotEnvelopeFields) { f.SchemaVersion = "route-snapshot-v0" },
		"checksum":           func(f *SnapshotEnvelopeFields) { f.ChecksumSHA256 = "00" + f.ChecksumSHA256[2:] },
		"topology":           func(f *SnapshotEnvelopeFields) { f.TopologyFingerprint = "00" + f.TopologyFingerprint[2:] },
		"non-canonical-json": func(f *SnapshotEnvelopeFields) { f.CanonicalJSON = append([]byte(" "), f.CanonicalJSON...) },
		"revision":           func(f *SnapshotEnvelopeFields) { f.Revision = "2" },
		"missing-time":       func(f *SnapshotEnvelopeFields) { f.CommittedAtMs = "" },
	} {
		t.Run(name, func(t *testing.T) {
			copyFields := fields
			copyFields.CanonicalJSON = append([]byte(nil), fields.CanonicalJSON...)
			mutate(&copyFields)
			if _, err := DecodeStoredRouteSnapshot(copyFields); err == nil {
				t.Fatal("corrupt envelope accepted")
			}
		})
	}
	if _, err := DecodeStoredRouteSnapshot(fields); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	for name, mutate := range map[string]func(*SnapshotEnvelopeFields){
		"leading-zero-revision":      func(f *SnapshotEnvelopeFields) { f.Revision = "01" },
		"leading-zero-committed-at":  func(f *SnapshotEnvelopeFields) { f.CommittedAtMs = "01" },
		"negative-zero-committed-at": func(f *SnapshotEnvelopeFields) { f.CommittedAtMs = "-0" },
	} {
		t.Run(name, func(t *testing.T) {
			copyFields := fields
			mutate(&copyFields)
			if _, err := DecodeStoredRouteSnapshot(copyFields); !errors.Is(err, ErrSnapshotEnvelopeMalformed) {
				t.Fatalf("non-canonical decimal accepted: %v", err)
			}
		})
	}
	if _, err := CanonicalizeRouteSnapshot(actorrouting.RouteSnapshot{}); !errors.Is(err, ErrSnapshotEnvelopeMalformed) {
		t.Fatalf("invalid snapshot error=%v", err)
	}
}
