// Package gateverification implements the internal, read-only Gate evidence
// transport. It owns no assignment source and exposes no mutation capability.
package gateverification

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	gateverificationpb "farmgame/api/gen/gateverification"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/netendpoint"
	"farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	MaxPeers            = 64
	MaxRealmBytes       = 256
	MaxIdentityBytes    = 256
	MaxEndpointBytes    = 512
	defaultMessageBytes = 64 << 10
	defaultCallTimeout  = 5 * time.Second
)

var (
	ErrInvalidConfiguration = errors.New("gate verification: invalid configuration")
	ErrUnavailable          = errors.New("gate verification: unavailable")
	ErrClosed               = errors.New("gate verification: closed")
)

type ShardObserver interface {
	ObserveShard(context.Context, string, actorrouting.RouteDomain, uint32) (authoritycomposition.ShardObservation, error)
}

type Service struct {
	gateverificationpb.UnimplementedGateVerificationServiceServer
	observer       ShardObserver
	now            func() time.Time
	leaseCheckMode actorrouting.LeaseCheckMode
}

func NewService(observer ShardObserver) (*Service, error) {
	return NewServiceWithOptions(observer, ServiceOptions{})
}

type ServiceOptions struct {
	Now            func() time.Time
	LeaseCheckMode actorrouting.LeaseCheckMode
}

func NewServiceWithOptions(observer ShardObserver, options ServiceOptions) (*Service, error) {
	if observer == nil {
		return nil, ErrInvalidConfiguration
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	leaseCheckMode := options.LeaseCheckMode
	if leaseCheckMode == "" {
		leaseCheckMode = actorrouting.LeaseCheckSnapshot
		if delegated, ok := observer.(interface{ LeaseAuthorityDelegated() bool }); ok && delegated.LeaseAuthorityDelegated() {
			leaseCheckMode = actorrouting.LeaseCheckAuthorityDelegated
		}
	}
	if leaseCheckMode != actorrouting.LeaseCheckSnapshot && leaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return nil, ErrInvalidConfiguration
	}
	return &Service{observer: observer, now: now, leaseCheckMode: leaseCheckMode}, nil
}

func (s *Service) ObserveShard(ctx context.Context, request *gateverificationpb.ObserveShardRequest) (*gateverificationpb.ObserveShardResponse, error) {
	if s == nil || s.observer == nil || s.now == nil || ctx == nil {
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	if request == nil || !bounded(request.GetRealm(), MaxRealmBytes) {
		return nil, status.Error(codes.InvalidArgument, "invalid shard observation request")
	}
	domain := actorrouting.RouteDomain(request.GetDomain())
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return nil, status.Error(codes.InvalidArgument, "invalid shard observation request")
	}
	observed, err := s.observer.ObserveShard(ctx, request.GetRealm(), domain, request.GetShard())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	if observed.Realm != request.GetRealm() || observed.Domain != domain || observed.VirtualShard != request.GetShard() ||
		!bounded(observed.HolderID, MaxIdentityBytes) || !bounded(observed.HolderBootID, MaxIdentityBytes) ||
		netendpoint.Validate(observed.LogicEndpoint, MaxEndpointBytes) != nil || observed.State != actorrouting.ShardStateActive ||
		observed.AssignmentRevision == 0 || observed.WriterEpoch == 0 || observed.LeaseEpoch == 0 ||
		observed.LeaseUntilMs <= 0 ||
		(s.leaseCheckMode == actorrouting.LeaseCheckSnapshot && observed.LeaseUntilMs <= s.now().UnixMilli()) || observed.SnapshotRevision == 0 {
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	return &gateverificationpb.ObserveShardResponse{
		Realm: observed.Realm, Domain: string(observed.Domain), Shard: observed.VirtualShard,
		HolderId: observed.HolderID, HolderBootId: observed.HolderBootID, LogicEndpoint: observed.LogicEndpoint,
		State: string(observed.State), AssignmentRevision: observed.AssignmentRevision, WriterEpoch: observed.WriterEpoch,
		LeaseEpoch: observed.LeaseEpoch, LeaseUntilMs: observed.LeaseUntilMs, SnapshotRevision: observed.SnapshotRevision,
	}, nil
}

func bounded(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value
}

type Server struct {
	server *transport.SealedGRPCServer
}

func NewServer(observer ShardObserver, tlsConfig *tls.Config, allowedClientIdentities []string, maxMessageBytes int) (*Server, error) {
	service, err := NewService(observer)
	if err != nil {
		return nil, err
	}
	authorizer, err := transport.NewExactDNSMethodAuthorizer(map[string][]string{
		gateverificationpb.GateVerificationService_ObserveShard_FullMethodName: append([]string(nil), allowedClientIdentities...),
	})
	if err != nil {
		return nil, err
	}
	if maxMessageBytes <= 0 {
		maxMessageBytes = defaultMessageBytes
	}
	sealed, err := transport.NewHolderOnlySealedGRPCServer(tlsConfig, transport.GRPCServerOptions{
		MaxMessageBytes: maxMessageBytes, MethodAuthorizer: authorizer,
	}, func(registrar grpc.ServiceRegistrar) {
		gateverificationpb.RegisterGateVerificationServiceServer(registrar, service)
	})
	if err != nil {
		return nil, err
	}
	return &Server{server: sealed}, nil
}

func (s *Server) Serve(listener net.Listener) error {
	if s == nil || s.server == nil {
		return ErrInvalidConfiguration
	}
	return s.server.Serve(listener)
}

func (s *Server) GracefulStop() {
	if s != nil && s.server != nil {
		s.server.GracefulStop()
	}
}

func (s *Server) Stop() {
	if s != nil && s.server != nil {
		s.server.Stop()
	}
}

type Peer struct {
	Name             string `json:"name"`
	Address          string `json:"address"`
	ServerName       string `json:"server_name"`
	ExpectedIdentity string `json:"expected_identity"`
}

type ClientOptions struct {
	CallTimeout     time.Duration
	MaxMessageBytes int
	Now             func() time.Time
	LeaseCheckMode  actorrouting.LeaseCheckMode
}

type configuredPeer struct {
	peer Peer
	tls  *tls.Config
}

type PeerObservation struct {
	Peer        Peer
	Observation authoritycomposition.ShardObservation
}

// ClientFleet keeps an explicit one-to-one direct peer list. It creates no
// connection at startup, but validates every address and TLS identity then.
type ClientFleet struct {
	peers          []configuredPeer
	timeout        time.Duration
	maxMessage     int
	now            func() time.Time
	leaseCheckMode actorrouting.LeaseCheckMode
	root           context.Context
	cancel         context.CancelFunc

	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
}

func NewClientFleet(peers []Peer, tlsConfig *tls.Config, options ClientOptions) (*ClientFleet, error) {
	if len(peers) == 0 || len(peers) > MaxPeers || tlsConfig == nil {
		return nil, ErrInvalidConfiguration
	}
	timeout := options.CallTimeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	if timeout > 30*time.Second {
		return nil, ErrInvalidConfiguration
	}
	maxMessage := options.MaxMessageBytes
	if maxMessage <= 0 {
		maxMessage = defaultMessageBytes
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	leaseCheckMode := options.LeaseCheckMode
	if leaseCheckMode == "" {
		leaseCheckMode = actorrouting.LeaseCheckSnapshot
	}
	if leaseCheckMode != actorrouting.LeaseCheckSnapshot && leaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return nil, ErrInvalidConfiguration
	}
	configured := make([]configuredPeer, 0, len(peers))
	seenNames := make(map[string]struct{}, len(peers))
	seenAddresses := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if !bounded(peer.Name, MaxIdentityBytes) {
			return nil, ErrInvalidConfiguration
		}
		canonicalAddress, err := netendpoint.Canonical(peer.Address, MaxEndpointBytes)
		if err != nil {
			return nil, ErrInvalidConfiguration
		}
		if _, exists := seenNames[peer.Name]; exists {
			return nil, ErrInvalidConfiguration
		}
		if _, exists := seenAddresses[canonicalAddress]; exists {
			return nil, ErrInvalidConfiguration
		}
		boundTLS, err := transport.BindExactServerIdentity(tlsConfig, peer.ServerName, peer.ExpectedIdentity)
		if err != nil {
			return nil, ErrInvalidConfiguration
		}
		seenNames[peer.Name] = struct{}{}
		seenAddresses[canonicalAddress] = struct{}{}
		peer.Address = canonicalAddress
		configured = append(configured, configuredPeer{peer: peer, tls: boundTLS})
	}
	root, cancel := context.WithCancel(context.Background())
	return &ClientFleet{peers: configured, timeout: timeout, maxMessage: maxMessage, now: now, leaseCheckMode: leaseCheckMode, root: root, cancel: cancel, closeDone: make(chan struct{})}, nil
}

func (c *ClientFleet) ObserveShard(ctx context.Context, realm string, domain actorrouting.RouteDomain, shard uint32) ([]PeerObservation, error) {
	if c == nil || c.now == nil || ctx == nil ||
		!bounded(realm, MaxRealmBytes) ||
		(domain != actorrouting.DomainPlayer &&
			domain != actorrouting.DomainFarm &&
			domain != actorrouting.DomainFarmWorker) {
		return nil, ErrInvalidConfiguration
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	results := make([]PeerObservation, 0, len(c.peers))
	for _, peer := range c.peers {
		observation, err := c.observePeer(ctx, peer, realm, domain, shard)
		if err != nil {
			return nil, ErrUnavailable
		}
		results = append(results, PeerObservation{Peer: peer.peer, Observation: observation})
	}
	return results, nil
}

func (c *ClientFleet) observePeer(ctx context.Context, peer configuredPeer, realm string, domain actorrouting.RouteDomain, shard uint32) (authoritycomposition.ShardObservation, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	stop := context.AfterFunc(c.root, cancel)
	defer func() {
		stop()
		cancel()
	}()
	conn, err := grpc.DialContext(callCtx, peer.peer.Address,
		grpc.WithTransportCredentials(credentials.NewTLS(peer.tls.Clone())), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.maxMessage), grpc.MaxCallSendMsgSize(c.maxMessage)))
	if err != nil {
		return authoritycomposition.ShardObservation{}, err
	}
	defer conn.Close()
	response, err := gateverificationpb.NewGateVerificationServiceClient(conn).ObserveShard(callCtx, &gateverificationpb.ObserveShardRequest{
		Realm: realm, Domain: string(domain), Shard: shard,
	})
	if err != nil || response == nil || !bounded(response.GetRealm(), MaxRealmBytes) ||
		!bounded(response.GetHolderId(), MaxIdentityBytes) || !bounded(response.GetHolderBootId(), MaxIdentityBytes) ||
		netendpoint.Validate(response.GetLogicEndpoint(), MaxEndpointBytes) != nil || response.GetRealm() != realm ||
		actorrouting.RouteDomain(response.GetDomain()) != domain || response.GetShard() != shard ||
		actorrouting.ShardRouteState(response.GetState()) != actorrouting.ShardStateActive || response.GetAssignmentRevision() == 0 ||
		response.GetWriterEpoch() == 0 || response.GetLeaseEpoch() == 0 || response.GetLeaseUntilMs() <= 0 ||
		(c.leaseCheckMode == actorrouting.LeaseCheckSnapshot && response.GetLeaseUntilMs() <= c.now().UnixMilli()) || response.GetSnapshotRevision() == 0 {
		return authoritycomposition.ShardObservation{}, ErrUnavailable
	}
	return authoritycomposition.ShardObservation{
		Realm: response.GetRealm(), Domain: actorrouting.RouteDomain(response.GetDomain()), VirtualShard: response.GetShard(),
		HolderID: response.GetHolderId(), HolderBootID: response.GetHolderBootId(), LogicEndpoint: response.GetLogicEndpoint(),
		State: actorrouting.ShardRouteState(response.GetState()), AssignmentRevision: response.GetAssignmentRevision(),
		WriterEpoch: response.GetWriterEpoch(), LeaseEpoch: response.GetLeaseEpoch(), LeaseUntilMs: response.GetLeaseUntilMs(),
		SnapshotRevision: response.GetSnapshotRevision(),
	}, nil
}

// Close cancels every lazy dial/RPC, joins all in-flight calls, and is safe to
// invoke concurrently or repeatedly.
func (c *ClientFleet) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.wg.Wait()
		close(c.closeDone)
	})
	<-c.closeDone
	return nil
}

func (c *ClientFleet) String() string {
	if c == nil {
		return "gate-verification-fleet(closed)"
	}
	return fmt.Sprintf("gate-verification-fleet(peers=%d)", len(c.peers))
}
