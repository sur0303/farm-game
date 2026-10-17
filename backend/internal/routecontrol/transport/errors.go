package transport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrTLSRequired                = errors.New("routecontrol transport: mutual TLS is required")
	ErrTLSConfig                  = errors.New("routecontrol transport: invalid TLS configuration")
	ErrPeerUnauthenticated        = errors.New("routecontrol transport: authenticated TLS peer required")
	ErrPeerIdentityNotAllowed     = errors.New("routecontrol transport: peer identity is not allowed")
	ErrEnvelopeMalformed          = errors.New("routecontrol transport: malformed route snapshot envelope")
	ErrEnvelopeControllerMismatch = errors.New("routecontrol transport: controller identity mismatch")
	ErrEnvelopeBootDrift          = errors.New("routecontrol transport: controller boot drift")
	ErrEnvelopeBootRetired        = errors.New("routecontrol transport: retired controller boot returned")
	ErrEnvelopeRealmMismatch      = errors.New("routecontrol transport: realm mismatch")
	ErrEnvelopeTopologyDrift      = errors.New("routecontrol transport: topology drift")
	ErrEnvelopeCapacity           = errors.New("routecontrol transport: route snapshot envelope exceeds size limit")
	ErrBootRolloverOverlap        = errors.New("routecontrol transport: controller boot rollover overlaps an active operation")
	ErrBootRolloverRollback       = errors.New("routecontrol transport: controller boot rollover revision rollback")
	ErrBootRolloverMismatch       = errors.New("routecontrol transport: equal-revision controller boot rollover mismatch")
	ErrBootRolloverContract       = errors.New("routecontrol transport: invalid controller boot rollover contract")
	ErrRetiredBootCapacity        = errors.New("routecontrol transport: retired controller boot capacity exhausted")
	ErrClientClosed               = errors.New("routecontrol transport: client is closed")
	ErrClientConcurrency          = errors.New("routecontrol transport: client concurrency limit exceeded")
	ErrWatchBackpressure          = errors.New("routecontrol transport: client watch queue is full")
	ErrSnapshotUnavailable        = errors.New("routecontrol transport: snapshot is not available")
	ErrServerConfiguration        = errors.New("routecontrol transport: invalid server configuration")
	ErrTLSIdentityMismatch        = errors.New("routecontrol transport: TLS server identity mismatch")
)

const (
	defaultTransportMessageBytes = 4 << 20
	defaultClientConcurrency     = 8
	defaultClientWatchBuffer     = 8
	defaultClientCallTimeout     = 5 * time.Second
	defaultRetiredBootLimit      = 8
	maxRetiredBootLimit          = 64
	defaultServerSendTimeout     = 5 * time.Second
	serverSendCancelGrace        = 100 * time.Millisecond
)

type ServerOptions struct {
	ControllerID          string
	Boot                  string
	Realm                 string
	AllowedClientIdentity []string
	MaxMessageBytes       int
	SendTimeout           time.Duration
}

type GRPCServerOptions struct {
	MaxMessageBytes  int
	SendTimeout      time.Duration
	MethodAuthorizer *MethodAuthorizer
}

type ClientOptions struct {
	ExpectedControllerID   string
	ExpectedRealm          string
	ExpectedTopology       string
	ServerName             string
	ExpectedServerIdentity string
	MaxMessageBytes        int
	MaxConcurrent          int
	WatchBuffer            int
	CallTimeout            time.Duration
	RetiredBootLimit       int
}

type normalizedClientOptions struct {
	expectedControllerID   string
	expectedRealm          string
	expectedTopology       string
	expectedServerIdentity string
	maxMessageBytes        int
	maxConcurrent          int
	watchBuffer            int
	callTimeout            time.Duration
	retiredBootLimit       int
}

func (o ClientOptions) normalize() normalizedClientOptions {
	result := normalizedClientOptions{
		expectedControllerID:   strings.TrimSpace(o.ExpectedControllerID),
		expectedRealm:          strings.TrimSpace(o.ExpectedRealm),
		expectedTopology:       strings.TrimSpace(o.ExpectedTopology),
		expectedServerIdentity: strings.TrimSpace(o.ExpectedServerIdentity),
		maxMessageBytes:        o.MaxMessageBytes,
		maxConcurrent:          o.MaxConcurrent,
		watchBuffer:            o.WatchBuffer,
		callTimeout:            o.CallTimeout,
		retiredBootLimit:       o.RetiredBootLimit,
	}
	if result.maxMessageBytes <= 0 {
		result.maxMessageBytes = defaultTransportMessageBytes
	}
	if result.maxConcurrent <= 0 {
		result.maxConcurrent = defaultClientConcurrency
	}
	if result.watchBuffer <= 0 {
		result.watchBuffer = defaultClientWatchBuffer
	}
	if result.callTimeout <= 0 {
		result.callTimeout = defaultClientCallTimeout
	}
	if result.retiredBootLimit == 0 {
		result.retiredBootLimit = defaultRetiredBootLimit
	}
	return result
}

func normalizedMaxMessageBytes(value int) int {
	if value <= 0 {
		return defaultTransportMessageBytes
	}
	return value
}

func normalizedSendTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return defaultServerSendTimeout
	}
	return value
}

func validRequiredIdentity(value string) bool {
	return strings.TrimSpace(value) != ""
}

func (o ClientOptions) validate() (normalizedClientOptions, error) {
	normalized := o.normalize()
	if normalized.expectedControllerID == "" {
		return normalized, fmt.Errorf("%w: expected controller id required", ErrEnvelopeControllerMismatch)
	}
	if normalized.retiredBootLimit < 1 || normalized.retiredBootLimit > maxRetiredBootLimit {
		return normalized, fmt.Errorf("%w: limit=%d must be within [1,%d]", ErrRetiredBootCapacity, normalized.retiredBootLimit, maxRetiredBootLimit)
	}
	if strings.TrimSpace(o.ServerName) == "" || strings.TrimSpace(o.ServerName) != o.ServerName {
		return normalized, fmt.Errorf("%w: ServerName is required", ErrTLSConfig)
	}
	if normalized.expectedServerIdentity == "" ||
		strings.TrimSpace(o.ExpectedServerIdentity) != o.ExpectedServerIdentity {
		return normalized, fmt.Errorf("%w: expected TLS server identity required", ErrTLSIdentityMismatch)
	}
	serverIdentity, err := canonicalDNSIdentity(o.ServerName)
	if err != nil {
		return normalized, fmt.Errorf("%w: ServerName: %v", ErrTLSConfig, err)
	}
	expectedIdentity, err := canonicalPeerIdentity(normalized.expectedServerIdentity)
	if err != nil {
		return normalized, fmt.Errorf("%w: expected TLS server identity: %v", ErrTLSIdentityMismatch, err)
	}
	if expectedIdentity != serverIdentity {
		return normalized, fmt.Errorf("%w: ServerName=%q expected=%q", ErrTLSIdentityMismatch, serverIdentity, expectedIdentity)
	}
	normalized.expectedServerIdentity = expectedIdentity
	return normalized, nil
}

func transportStatusError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "route snapshot request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "route snapshot request deadline exceeded")
	case errors.Is(err, ErrPeerUnauthenticated), errors.Is(err, ErrPeerIdentityNotAllowed):
		return status.Error(codes.Unauthenticated, "route snapshot peer authentication failed")
	case errors.Is(err, ErrEnvelopeControllerMismatch),
		errors.Is(err, ErrEnvelopeBootDrift),
		errors.Is(err, ErrEnvelopeBootRetired),
		errors.Is(err, ErrEnvelopeRealmMismatch),
		errors.Is(err, ErrEnvelopeTopologyDrift),
		errors.Is(err, ErrBootRolloverOverlap),
		errors.Is(err, ErrBootRolloverRollback),
		errors.Is(err, ErrBootRolloverMismatch),
		errors.Is(err, ErrBootRolloverContract),
		errors.Is(err, ErrRetiredBootCapacity),
		errors.Is(err, ErrEnvelopeMalformed),
		errors.Is(err, ErrTLSIdentityMismatch),
		errors.Is(err, core.ErrSnapshotDurableMissing),
		errors.Is(err, core.ErrSnapshotDurableRollback),
		errors.Is(err, core.ErrSnapshotDurableConflict),
		errors.Is(err, core.ErrSnapshotDurableContract),
		errors.Is(err, core.ErrSnapshotEnvelopeMalformed),
		errors.Is(err, core.ErrSnapshotSchemaMismatch),
		errors.Is(err, core.ErrSnapshotChecksumMismatch),
		errors.Is(err, core.ErrSnapshotTopologyMismatch),
		errors.Is(err, actorrouting.ErrInvalidSnapshot),
		errors.Is(err, actorrouting.ErrStaleRoute),
		errors.Is(err, actorrouting.ErrSnapshotRevisionConflict),
		errors.Is(err, actorrouting.ErrSnapshotGenerationRollback),
		errors.Is(err, actorrouting.ErrSnapshotHolderGeneration),
		errors.Is(err, actorrouting.ErrHashMigrationRequired),
		errors.Is(err, actorrouting.ErrHolderNamespaceConflict):
		return status.Error(codes.FailedPrecondition, "route snapshot envelope rejected")
	case errors.Is(err, ErrEnvelopeCapacity),
		errors.Is(err, core.ErrSnapshotCapacity),
		errors.Is(err, ErrWatchBackpressure),
		errors.Is(err, core.ErrSnapshotWatchClientCapacity):
		return status.Error(codes.ResourceExhausted, "route snapshot transport capacity exceeded")
	case errors.Is(err, ErrSnapshotUnavailable):
		return status.Error(codes.FailedPrecondition, "route snapshot unavailable")
	case errors.Is(err, core.ErrSnapshotHubClosed):
		return status.Error(codes.Unavailable, "route snapshot watch hub closed")
	case errors.Is(err, core.ErrSnapshotWatchSlowConsumer):
		return status.Error(codes.ResourceExhausted, "route snapshot watch consumer too slow")
	default:
		return status.Error(codes.Internal, "route snapshot transport failure")
	}
}

func invalidConfiguration(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrServerConfiguration, fmt.Sprintf(format, args...))
}

func isGRPCMessageCapacityError(err error) bool {
	if err == nil || status.Code(err) != codes.ResourceExhausted {
		return false
	}
	message := strings.ToLower(status.Convert(err).Message())
	return strings.Contains(message, "message") &&
		(strings.Contains(message, "larger") ||
			strings.Contains(message, "maximum") ||
			strings.Contains(message, "max") ||
			strings.Contains(message, "size"))
}

func isGRPCContractError(err error) bool {
	return err != nil && status.Code(err) == codes.FailedPrecondition
}
