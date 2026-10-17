package transport

import (
	"fmt"
	"strconv"
	"strings"

	routecontrolpb "farmgame/api/gen/routecontrol"
	core "farmgame/internal/routecontrol"

	"google.golang.org/protobuf/proto"
)

func storedToEnvelope(stored core.StoredRouteSnapshot, controllerID, boot, realm string, maxBytes int) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	if _, err := core.DecodeStoredRouteSnapshot(stored.EnvelopeFields()); err != nil {
		return nil, fmt.Errorf("%w: stored snapshot", ErrEnvelopeMalformed)
	}
	if len(stored.CanonicalJSON) > maxBytes {
		return nil, fmt.Errorf("%w: canonical JSON size", ErrEnvelopeCapacity)
	}
	if !validRequiredIdentity(controllerID) || !validRequiredIdentity(boot) {
		return nil, invalidConfiguration("controller identity and boot are required")
	}
	if realm != "" && stored.Realm != realm {
		return nil, fmt.Errorf("%w: server realm", ErrEnvelopeRealmMismatch)
	}
	envelope := &routecontrolpb.RouteSnapshotEnvelope{
		Schema:        stored.SchemaVersion,
		Realm:         stored.Realm,
		Revision:      stored.Revision,
		CanonicalJson: append([]byte(nil), stored.CanonicalJSON...),
		Checksum:      stored.ChecksumSHA256,
		Topology:      stored.TopologyFingerprint,
		CommittedAt:   stored.CommittedAtMs,
		ControllerId:  strings.TrimSpace(controllerID),
		Boot:          strings.TrimSpace(boot),
	}
	if proto.Size(envelope) > maxBytes {
		return nil, fmt.Errorf("%w: protobuf message size", ErrEnvelopeCapacity)
	}
	return envelope, nil
}

func decodeEnvelope(envelope *routecontrolpb.RouteSnapshotEnvelope, maxBytes int) (core.StoredRouteSnapshot, error) {
	if envelope == nil {
		return core.StoredRouteSnapshot{}, ErrEnvelopeMalformed
	}
	if proto.Size(envelope) > maxBytes || len(envelope.GetCanonicalJson()) > maxBytes {
		return core.StoredRouteSnapshot{}, ErrEnvelopeCapacity
	}
	if strings.TrimSpace(envelope.GetControllerId()) == "" || strings.TrimSpace(envelope.GetBoot()) == "" {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: controller identity or boot missing", ErrEnvelopeMalformed)
	}
	stored, err := core.DecodeStoredRouteSnapshot(core.SnapshotEnvelopeFields{
		SchemaVersion:       envelope.GetSchema(),
		Realm:               envelope.GetRealm(),
		Revision:            strconv.FormatUint(envelope.GetRevision(), 10),
		CanonicalJSON:       append([]byte(nil), envelope.GetCanonicalJson()...),
		ChecksumSHA256:      envelope.GetChecksum(),
		TopologyFingerprint: envelope.GetTopology(),
		CommittedAtMs:       strconv.FormatInt(envelope.GetCommittedAt(), 10),
	})
	if err != nil {
		return core.StoredRouteSnapshot{}, fmt.Errorf("%w: %w", ErrEnvelopeMalformed, err)
	}
	return stored, nil
}
