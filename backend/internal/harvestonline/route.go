package harvestonline

import (
	"context"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"

	"google.golang.org/grpc"
)

// RouteFence is the Harvest Participant route proof. It compares the full
// current assignment and local holder identity before a Participant mailbox
// can accept the call.
type RouteFence struct {
	Resolver actorrouting.Resolver
	Realm    string
	Domain   actorrouting.RouteDomain
	HolderID string
	BootID   string
}

func NewRouteFence(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, holderID, bootID string) (*RouteFence, error) {
	if resolver == nil || realm == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || holderID == "" || bootID == "" {
		return nil, fmt.Errorf("%w: route resolver, realm, domain, and local holder identity are required", ErrInvalidConfig)
	}
	return &RouteFence{Resolver: resolver, Realm: realm, Domain: domain, HolderID: holderID, BootID: bootID}, nil
}

var _ contracts.ParticipantRouteFenceVerifier = (*RouteFence)(nil)

func (f *RouteFence) VerifyRoute(ctx context.Context, call *contracts.ParticipantCall) error {
	if f == nil || f.Resolver == nil || call == nil || call.RouteHeader == nil || call.RouteHeader.GetActorId() == 0 {
		return fmt.Errorf("%w: incomplete route proof", ErrInvalidConfig)
	}
	wantDomain, err := routeDomain(call.Spec.Command)
	if err != nil || wantDomain != f.Domain {
		return fmt.Errorf("%w: Participant route domain is not held by this service", ErrInvalidConfig)
	}
	if call.RouteHeader.GetRouteDomain() != protoRouteDomain(wantDomain) {
		return fmt.Errorf("%w: Participant route header domain mismatch", ErrInvalidConfig)
	}
	assignment, err := f.Resolver.Resolve(ctx, actorrouting.Address{Realm: f.Realm, Kind: wantDomain, ID: call.RouteHeader.GetActorId()})
	if err != nil {
		return err
	}
	if assignment.HolderPod != f.HolderID || assignment.HolderBootID != f.BootID || !sameAssignment(call.RouteHeader, assignment) {
		return fmt.Errorf("%w: stale or mismatched physical route proof", actorrouting.ErrStaleRoute)
	}
	return nil
}

// CoordinatorRouteProvider resolves the actor assignment at each retry. It
// never changes the WorkAddress, WorkVersion, InvocationID, or payload.
func CoordinatorRouteProvider(resolver actorrouting.Resolver, realm string) harvest.RouteProvider {
	return func(ctx context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || realm == "" || actorID == 0 {
			return nil, fmt.Errorf("%w: route resolver is unavailable", ErrInvalidConfig)
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
			return nil, fmt.Errorf("%w: incomplete route assignment", ErrInvalidConfig)
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

type routedCoordinatorClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]harvest.CoordinatorParticipantClient
}

func newRoutedCoordinatorClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]harvest.CoordinatorParticipantClient) (*routedCoordinatorClient, error) {
	if resolver == nil || realm == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || len(clients) == 0 {
		return nil, fmt.Errorf("%w: typed Participant route client configuration is incomplete", ErrInvalidConfig)
	}
	copyClients := make(map[string]harvest.CoordinatorParticipantClient, len(clients))
	for endpoint, client := range clients {
		if endpoint != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("%w: typed Participant route clients are empty", ErrInvalidConfig)
	}
	return &routedCoordinatorClient{resolver: resolver, realm: realm, domain: domain, clients: copyClients}, nil
}

func (c *routedCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || request == nil || request.GetRouteHeader() == nil || request.GetRouteHeader().GetActorId() == 0 {
		return nil, fmt.Errorf("%w: typed Participant route request is incomplete", ErrInvalidConfig)
	}
	header := request.GetRouteHeader()
	wantDomain, protoDomain, err := routeDomainPair(request.GetParticipantCommand())
	if err != nil || wantDomain != c.domain || header.GetRouteDomain() != protoDomain {
		return nil, fmt.Errorf("%w: typed Participant route-domain separation", ErrInvalidConfig)
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

type LocalCoordinatorClient struct {
	Server interface {
		ExecuteStep(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)
	}
}

func (c LocalCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c.Server == nil {
		return nil, fmt.Errorf("%w: local Participant server is unavailable", ErrInvalidConfig)
	}
	return c.Server.ExecuteStep(ctx, request)
}

func routeDomain(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, error) {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		return actorrouting.DomainFarm, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		return actorrouting.DomainPlayer, nil
	default:
		return "", fmt.Errorf("%w: unsupported Participant command %s", ErrInvalidConfig, command.String())
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
		compatibleLeaseProof(header, assignment)
}

func compatibleLeaseProof(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && actorrouting.CompatibleLeaseProof(
		header.GetExpectedLeaseEpoch(), header.ExpectedLeaseUntilMs, assignment, true,
	)
}
