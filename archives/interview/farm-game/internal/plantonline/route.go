// Package plantonline contains the small process-composition adapter for the
// frozen SC1 Plant workflow.  It owns routing/client selection and result
// projection only; the scenario and participant packages remain the business
// authorities.
package plantonline

import (
	"context"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/scenario/plant"
	"farmgame/internal/workflow"

	"google.golang.org/grpc"
)

// RouteFence is the physical route proof injected into PC1.  It resolves the
// current assignment and compares every route field carried by the request;
// it also requires the local holder identity, so a process cannot accept a
// request for a different physical holder merely because its endpoint is
// reachable.
type RouteFence struct {
	Resolver actorrouting.Resolver
	Realm    string
	Domain   actorrouting.RouteDomain
	HolderID string
	BootID   string
}

// NewRouteFence constructs the fail-closed route proof used by a typed
// Participant server.
func NewRouteFence(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, holderID, bootID string) (*RouteFence, error) {
	if resolver == nil || realm == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || holderID == "" || bootID == "" {
		return nil, fmt.Errorf("plantonline: route resolver, realm, domain, and local holder identity are required")
	}
	return &RouteFence{Resolver: resolver, Realm: realm, Domain: domain, HolderID: holderID, BootID: bootID}, nil
}

var _ contracts.ParticipantRouteFenceVerifier = (*RouteFence)(nil)

// VerifyRoute proves the complete current assignment outside the Actor
// mailbox.  The injected resolver is the existing RouteSnapshot path; no
// Redis lookup or second route calculator is introduced here.
func (f *RouteFence) VerifyRoute(ctx context.Context, call *contracts.ParticipantCall) error {
	if f == nil || f.Resolver == nil || call == nil || call.RouteHeader == nil || call.RouteHeader.GetActorId() == 0 {
		return fmt.Errorf("plantonline: incomplete route proof")
	}
	wantDomain, err := routeDomain(call.Spec.Command)
	if err != nil || wantDomain != f.Domain {
		return fmt.Errorf("plantonline: Participant route domain is not held by this service")
	}
	if call.RouteHeader.GetRouteDomain() != protoRouteDomain(wantDomain) {
		return fmt.Errorf("plantonline: Participant route header domain mismatch")
	}
	assignment, err := f.Resolver.Resolve(ctx, actorrouting.Address{Realm: f.Realm, Kind: wantDomain, ID: call.RouteHeader.GetActorId()})
	if err != nil {
		return err
	}
	if assignment.HolderPod != f.HolderID || assignment.HolderBootID != f.BootID {
		return fmt.Errorf("plantonline: local holder is not the current assignment")
	}
	if !sameAssignment(call.RouteHeader, assignment) {
		return fmt.Errorf("plantonline: stale or mismatched physical route proof")
	}
	if revision := call.RouteHeader.GetSourceSnapshotRevision(); revision != 0 && revision != assignment.SnapshotRevision {
		return fmt.Errorf("plantonline: route snapshot revision mismatch")
	}
	return nil
}

// CoordinatorRouteProvider builds the exact route header consumed by the
// existing Plant RPC adapter.  A later client-side assignment comparison
// rejects a stale header before the RPC is sent.
func CoordinatorRouteProvider(resolver actorrouting.Resolver, realm string) plant.RouteProvider {
	return routeProvider(resolver, realm)
}

// routeProvider is kept as a concrete helper because the scenario's
// workflow.StepCall type is intentionally not re-exported through this
// composition package.
func routeProvider(resolver actorrouting.Resolver, realm string) plant.RouteProvider {
	return func(ctx context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || realm == "" || actorID == 0 {
			return nil, fmt.Errorf("plantonline: route resolver is unavailable")
		}
		domain, protoDomain, err := routeDomainPair(command)
		if err != nil {
			return nil, err
		}
		assignment, err := resolver.Resolve(ctx, actorrouting.Address{Realm: realm, Kind: domain, ID: actorID})
		if err != nil {
			return nil, err
		}
		if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
			return nil, fmt.Errorf("plantonline: incomplete route assignment")
		}
		leaseUntilMs := uint64(assignment.LeaseUntilMs)
		return &logicrpcpb.ActorRouteHeader{
			RouteDomain:          protoDomain,
			ActorId:              actorID,
			VirtualShard:         assignment.ShardID,
			ShardRevision:        assignment.ShardRevision,
			ExpectedHolderId:     assignment.HolderPod,
			ExpectedHolderBootId: assignment.HolderBootID,
			ExpectedWriterEpoch:  assignment.WriterEpoch,
			ExpectedLeaseEpoch:   assignment.LeaseEpoch,
			ExpectedLeaseUntilMs: &leaseUntilMs,
		}, nil
	}
}

// routedCoordinatorClient selects the existing generated typed Participant
// client from the current assignment and rechecks the header immediately
// before dispatch.  A stale assignment returns to the generic Engine, which
// persists the same invocation and lets the existing bounded recovery route
// retry it; this adapter never forwards through a coordinator proxy.
type routedCoordinatorClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]plant.CoordinatorParticipantClient
}

func newRoutedCoordinatorClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]plant.CoordinatorParticipantClient) (*routedCoordinatorClient, error) {
	if resolver == nil || realm == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || len(clients) == 0 {
		return nil, fmt.Errorf("plantonline: typed Participant route client configuration is incomplete")
	}
	copyClients := make(map[string]plant.CoordinatorParticipantClient, len(clients))
	for endpoint, client := range clients {
		if endpoint != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("plantonline: typed Participant route clients are empty")
	}
	return &routedCoordinatorClient{resolver: resolver, realm: realm, domain: domain, clients: copyClients}, nil
}

// NewRoutedCoordinatorParticipantClient is the production typed-client
// adapter shared by composition roots that need one direct Participant call
// outside the generic Plant scenario. It preserves the same route metadata
// and assignment recheck as the Plant coordinator.
func NewRoutedCoordinatorParticipantClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]plant.CoordinatorParticipantClient) (plant.CoordinatorParticipantClient, error) {
	return newRoutedCoordinatorClient(resolver, realm, domain, clients)
}

func (c *routedCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || request == nil || request.GetRouteHeader() == nil || request.GetRouteHeader().GetActorId() == 0 {
		return nil, fmt.Errorf("plantonline: typed Participant route request is incomplete")
	}
	header := request.GetRouteHeader()
	wantDomain, protoDomain, err := routeDomainPair(request.GetParticipantCommand())
	if err != nil || wantDomain != c.domain || header.GetRouteDomain() != protoDomain {
		return nil, fmt.Errorf("plantonline: typed Participant route-domain separation")
	}
	address := actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()}
	assignment, err := c.resolver.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: typed Participant route header is stale", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: typed Participant endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return client.ExecuteStep(actorrouting.WithRoute(ctx, address, assignment), request, options...)
}

// LocalCoordinatorClient adapts an already-registered typed Participant
// server for the farm-svr local Farm authority. It is a transport adapter,
// not a second service implementation.
type LocalCoordinatorClient struct {
	Server interface {
		ExecuteStep(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)
	}
}

func (c LocalCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c.Server == nil {
		return nil, fmt.Errorf("plantonline: local Participant server is unavailable")
	}
	return c.Server.ExecuteStep(ctx, request)
}

func routeDomain(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, error) {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION:
		return actorrouting.DomainPlayer, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		return actorrouting.DomainFarm, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION:
		return actorrouting.DomainPlayer, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
		return actorrouting.DomainFarm, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		return actorrouting.DomainFarm, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD:
		return actorrouting.DomainPlayer, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT:
		return actorrouting.DomainFarm, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		return actorrouting.DomainPlayer, nil
	default:
		return "", fmt.Errorf("plantonline: unsupported Participant command %s", command.String())
	}
}

func routeDomainPair(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, logicrpcpb.RouteDomain, error) {
	domain, err := routeDomain(command)
	if err != nil {
		return "", logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED, err
	}
	return domain, protoRouteDomain(domain), nil
}

func protoRouteDomain(domain actorrouting.RouteDomain) logicrpcpb.RouteDomain {
	if domain == actorrouting.DomainPlayer {
		return logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	}
	return logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
}

func sameAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil &&
		header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		compatibleLeaseProof(header, assignment) &&
		(header.GetSourceSnapshotRevision() == 0 || header.GetSourceSnapshotRevision() == assignment.SnapshotRevision)
}

func compatibleLeaseProof(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	hasEpoch := header.GetExpectedLeaseEpoch() != 0
	hasDeadline := header.ExpectedLeaseUntilMs != nil
	if !hasEpoch && !hasDeadline {
		return true
	}
	return hasEpoch && hasDeadline && header.GetExpectedLeaseUntilMs() != 0 &&
		assignment.LeaseEpoch != 0 && assignment.LeaseUntilMs > 0 &&
		header.GetExpectedLeaseEpoch() == assignment.LeaseEpoch &&
		uint64(assignment.LeaseUntilMs) >= header.GetExpectedLeaseUntilMs()
}
