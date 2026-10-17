package routecontrol

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	routecontrolpb "farmgame/api/gen/routecontrol"
	routetransport "farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ForwardTLSConfig struct {
	ListenAddress         string
	ServerCertFile        string
	ServerKeyFile         string
	ClientCAFile          string
	AllowedClientIdentity string
	UpstreamTarget        string
	UpstreamServerName    string
	UpstreamClientCert    string
	UpstreamClientKey     string
	UpstreamServerCA      string
}

type TLSConnectionFact struct {
	Direction          string `json:"direction"`
	PeerIdentity       string `json:"peer_identity"`
	CertificateSHA256  string `json:"certificate_sha256"`
	TLSVersion         uint16 `json:"tls_version"`
	VerifiedChainCount int    `json:"verified_chain_count"`
}

type TLSFailureFact struct {
	Direction            string `json:"direction"`
	Class                string `json:"class"`
	TLSVersion           uint16 `json:"tls_version"`
	PeerCertificateCount int    `json:"peer_certificate_count"`
	VerifiedChainCount   int    `json:"verified_chain_count"`
}

type SnapshotProxyEvent struct {
	Sequence       uint64 `json:"sequence"`
	Method         string `json:"method"`
	Revision       uint64 `json:"revision"`
	ControllerID   string `json:"controller_id"`
	Boot           string `json:"boot"`
	EnvelopeSHA256 string `json:"envelope_sha256"`
	Held           bool   `json:"held,omitempty"`
	Released       bool   `json:"released,omitempty"`
	UpstreamOK     bool   `json:"upstream_ok"`
	ErrorCode      string `json:"error_code,omitempty"`
	WallTime       string `json:"wall_time"`
}

type SnapshotProxyCounters struct {
	UnaryRPCs      uint64 `json:"unary_rpcs"`
	WatchRPCs      uint64 `json:"watch_rpcs"`
	StreamMessages uint64 `json:"stream_messages"`
}

// SnapshotForwardProxy terminates two strict mTLS connections and forwards
// only complete production envelopes. Its hold operation retains the exact
// protobuf message received upstream; no field mutation API exists.
type SnapshotForwardProxy struct {
	routecontrolpb.UnimplementedRouteSnapshotServiceServer

	mu            sync.Mutex
	listener      net.Listener
	server        *grpc.Server
	connMu        sync.Mutex
	conn          *grpc.ClientConn
	client        routecontrolpb.RouteSnapshotServiceClient
	dialUpstream  func() (*grpc.ClientConn, error)
	upstreamStale bool
	events        []SnapshotProxyEvent
	tlsFacts      []TLSConnectionFact
	tlsFailures   []TLSFailureFact
	next          uint64
	counters      SnapshotProxyCounters
	hold          snapshotHold
}

type snapshotHold struct {
	armed    bool
	revision uint64
	held     bool
	heldCh   chan struct{}
	release  chan struct{}
}

func StartSnapshotForwardProxy(cfg ForwardTLSConfig) (*SnapshotForwardProxy, error) {
	serverTLS, upstreamTLS, err := loadForwardTLS(cfg)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenOrLoopback(cfg.ListenAddress))
	if err != nil {
		return nil, err
	}
	proxy := &SnapshotForwardProxy{listener: listener}
	serverTLS.VerifyConnection = chainedVerify(serverTLS.VerifyConnection, func(state tls.ConnectionState) error {
		fact, verifyErr := exactTLSFact(state, cfg.AllowedClientIdentity, "downstream")
		if verifyErr == nil {
			proxy.recordTLS(fact)
		} else {
			proxy.recordTLSFailure(tlsFailureFact(state, cfg.AllowedClientIdentity, "downstream"))
		}
		return verifyErr
	})
	upstreamTLS.VerifyConnection = chainedVerify(upstreamTLS.VerifyConnection, func(state tls.ConnectionState) error {
		fact, verifyErr := exactTLSFact(state, cfg.UpstreamServerName, "upstream")
		if verifyErr == nil {
			proxy.recordTLS(fact)
		} else {
			proxy.recordTLSFailure(tlsFailureFact(state, cfg.UpstreamServerName, "upstream"))
		}
		return verifyErr
	})
	proxy.dialUpstream = func() (*grpc.ClientConn, error) {
		return grpc.NewClient(cfg.UpstreamTarget,
			grpc.WithTransportCredentials(credentials.NewTLS(upstreamTLS.Clone())),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20), grpc.WaitForReady(true)))
	}
	conn, err := proxy.dialUpstream()
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	proxy.conn = conn
	proxy.client = routecontrolpb.NewRouteSnapshotServiceClient(conn)
	proxy.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	routecontrolpb.RegisterRouteSnapshotServiceServer(proxy.server, proxy)
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *SnapshotForwardProxy) Address() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *SnapshotForwardProxy) GetSnapshot(ctx context.Context, request *routecontrolpb.GetSnapshotRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	p.mu.Lock()
	p.counters.UnaryRPCs++
	p.mu.Unlock()
	client, clientErr := p.snapshotClient()
	if clientErr != nil {
		p.recordSnapshot("GetSnapshot", nil, false, false, clientErr)
		return nil, clientErr
	}
	envelope, err := client.GetSnapshot(ctx, request)
	if status.Code(err) == codes.Unavailable {
		p.markSnapshotUpstreamStale()
	}
	p.recordSnapshot("GetSnapshot", envelope, false, false, err)
	return envelope, err
}

func (p *SnapshotForwardProxy) GetCurrentRevision(ctx context.Context, request *routecontrolpb.GetCurrentRevisionRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	p.mu.Lock()
	p.counters.UnaryRPCs++
	p.mu.Unlock()
	client, clientErr := p.snapshotClient()
	if clientErr != nil {
		p.recordSnapshot("GetCurrentRevision", nil, false, false, clientErr)
		return nil, clientErr
	}
	envelope, err := client.GetCurrentRevision(ctx, request)
	if status.Code(err) == codes.Unavailable {
		p.markSnapshotUpstreamStale()
	}
	p.recordSnapshot("GetCurrentRevision", envelope, false, false, err)
	return envelope, err
}

func (p *SnapshotForwardProxy) WatchFrom(request *routecontrolpb.WatchFromRequest, downstream grpc.ServerStreamingServer[routecontrolpb.RouteSnapshotEnvelope]) error {
	p.mu.Lock()
	p.counters.WatchRPCs++
	p.mu.Unlock()
	client, clientErr := p.snapshotClient()
	if clientErr != nil {
		return clientErr
	}
	upstream, err := client.WatchFrom(downstream.Context(), request)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			p.markSnapshotUpstreamStale()
		}
		return err
	}
	for {
		envelope, recvErr := upstream.Recv()
		if recvErr != nil {
			p.markSnapshotUpstreamStale()
			return recvErr
		}
		p.mu.Lock()
		p.counters.StreamMessages++
		hold := p.hold.armed && !p.hold.held && (p.hold.revision == 0 || p.hold.revision == envelope.GetRevision())
		var release <-chan struct{}
		if hold {
			p.hold.held = true
			close(p.hold.heldCh)
			release = p.hold.release
		}
		p.mu.Unlock()
		p.recordSnapshot("WatchFrom", envelope, hold, false, nil)
		if hold {
			select {
			case <-release:
				p.recordSnapshot("WatchFrom", envelope, false, true, nil)
			case <-downstream.Context().Done():
				return downstream.Context().Err()
			}
		}
		if err := downstream.Send(envelope); err != nil {
			return err
		}
	}
}

func (p *SnapshotForwardProxy) ArmHold(revision uint64) error {
	if p == nil {
		return errors.New("snapshot proxy is nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hold.armed {
		return errors.New("snapshot hold is already armed")
	}
	p.hold = snapshotHold{armed: true, revision: revision, heldCh: make(chan struct{}), release: make(chan struct{})}
	return nil
}

func (p *SnapshotForwardProxy) WaitHeld(ctx context.Context) error {
	if p == nil || ctx == nil {
		return errors.New("snapshot hold wait requires context")
	}
	p.mu.Lock()
	if !p.hold.armed {
		p.mu.Unlock()
		return errors.New("snapshot hold is not armed")
	}
	held := p.hold.heldCh
	p.mu.Unlock()
	select {
	case <-held:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *SnapshotForwardProxy) ReleaseHeld() error {
	if p == nil {
		return errors.New("snapshot proxy is nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hold.armed || !p.hold.held {
		return errors.New("snapshot envelope is not held")
	}
	close(p.hold.release)
	p.hold.armed = false
	return nil
}

func (p *SnapshotForwardProxy) snapshotClient() (routecontrolpb.RouteSnapshotServiceClient, error) {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.upstreamStale {
		if p.dialUpstream == nil {
			return nil, errors.New("snapshot proxy upstream dialer is unavailable")
		}
		conn, err := p.dialUpstream()
		if err != nil {
			return nil, err
		}
		old := p.conn
		p.conn = conn
		p.client = routecontrolpb.NewRouteSnapshotServiceClient(conn)
		p.upstreamStale = false
		if old != nil {
			_ = old.Close()
		}
	}
	if p.client == nil {
		return nil, errors.New("snapshot proxy upstream client is unavailable")
	}
	return p.client, nil
}

func (p *SnapshotForwardProxy) markSnapshotUpstreamStale() {
	p.connMu.Lock()
	p.upstreamStale = true
	p.connMu.Unlock()
}

func (p *SnapshotForwardProxy) Counters() SnapshotProxyCounters {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counters
}

func (p *SnapshotForwardProxy) Events() []SnapshotProxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]SnapshotProxyEvent(nil), p.events...)
}

func (p *SnapshotForwardProxy) TLSFacts() []TLSConnectionFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSConnectionFact(nil), p.tlsFacts...)
}

func (p *SnapshotForwardProxy) TLSFailures() []TLSFailureFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSFailureFact(nil), p.tlsFailures...)
}

func (p *SnapshotForwardProxy) recordSnapshot(method string, envelope *routecontrolpb.RouteSnapshotEnvelope, held, released bool, rpcErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	event := SnapshotProxyEvent{Sequence: p.next, Method: method, Held: held, Released: released, UpstreamOK: rpcErr == nil, WallTime: time.Now().UTC().Format(time.RFC3339Nano)}
	if rpcErr != nil {
		event.ErrorCode = status.Code(rpcErr).String()
	}
	if envelope != nil {
		event.Revision = envelope.GetRevision()
		event.ControllerID = envelope.GetControllerId()
		event.Boot = envelope.GetBoot()
		event.EnvelopeSHA256 = protoDigest(envelope)
	}
	p.events = append(p.events, event)
}

func (p *SnapshotForwardProxy) recordTLS(fact TLSConnectionFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFacts {
		if existing == fact {
			return
		}
	}
	p.tlsFacts = append(p.tlsFacts, fact)
}

func (p *SnapshotForwardProxy) recordTLSFailure(fact TLSFailureFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFailures {
		if existing == fact {
			return
		}
	}
	p.tlsFailures = append(p.tlsFailures, fact)
}

func (p *SnapshotForwardProxy) Close() error {
	if p == nil {
		return nil
	}
	if p.server != nil {
		p.server.Stop()
	}
	var errs []error
	p.connMu.Lock()
	conn := p.conn
	p.conn = nil
	p.client = nil
	p.connMu.Unlock()
	if conn != nil {
		errs = append(errs, conn.Close())
	}
	if p.listener != nil {
		if err := p.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type HolderProxyEvent struct {
	Sequence       uint64 `json:"sequence"`
	Method         string `json:"method"`
	Domain         string `json:"domain"`
	Shard          uint32 `json:"shard"`
	RequestSHA256  string `json:"request_sha256"`
	ResponseSHA256 string `json:"response_sha256,omitempty"`
	Held           bool   `json:"held,omitempty"`
	UpstreamOK     bool   `json:"upstream_ok"`
	WallTime       string `json:"wall_time"`
}

// HolderForwardProxy counts a callback only after a successful response from
// the real production Holder. Stop can be held strictly before forwarding.
type HolderForwardProxy struct {
	holdercontrolpb.UnimplementedHolderControlServiceServer

	mu          sync.Mutex
	listener    net.Listener
	server      *grpc.Server
	conn        *grpc.ClientConn
	client      holdercontrolpb.HolderControlServiceClient
	events      []HolderProxyEvent
	tlsFacts    []TLSConnectionFact
	tlsFailures []TLSFailureFact
	next        uint64
	holdStop    holderStopHold
}

type holderStopHold struct {
	armed   bool
	domain  string
	shard   uint32
	held    bool
	heldCh  chan struct{}
	release chan struct{}
}

func StartHolderForwardProxy(cfg ForwardTLSConfig) (*HolderForwardProxy, error) {
	serverTLS, upstreamTLS, err := loadForwardTLS(cfg)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenOrLoopback(cfg.ListenAddress))
	if err != nil {
		return nil, err
	}
	proxy := &HolderForwardProxy{listener: listener}
	serverTLS.VerifyConnection = chainedVerify(serverTLS.VerifyConnection, func(state tls.ConnectionState) error {
		fact, verifyErr := exactTLSFact(state, cfg.AllowedClientIdentity, "downstream")
		if verifyErr == nil {
			proxy.recordTLS(fact)
		} else {
			proxy.recordTLSFailure(tlsFailureFact(state, cfg.AllowedClientIdentity, "downstream"))
		}
		return verifyErr
	})
	upstreamTLS.VerifyConnection = chainedVerify(upstreamTLS.VerifyConnection, func(state tls.ConnectionState) error {
		fact, verifyErr := exactTLSFact(state, cfg.UpstreamServerName, "upstream")
		if verifyErr == nil {
			proxy.recordTLS(fact)
		} else {
			proxy.recordTLSFailure(tlsFailureFact(state, cfg.UpstreamServerName, "upstream"))
		}
		return verifyErr
	})
	conn, err := grpc.NewClient(cfg.UpstreamTarget,
		grpc.WithTransportCredentials(credentials.NewTLS(upstreamTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20), grpc.MaxCallSendMsgSize(4<<20)))
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	proxy.conn = conn
	proxy.client = holdercontrolpb.NewHolderControlServiceClient(conn)
	proxy.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
	holdercontrolpb.RegisterHolderControlServiceServer(proxy.server, proxy)
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *HolderForwardProxy) Address() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *HolderForwardProxy) StopNewActivationAndTurns(ctx context.Context, request *holdercontrolpb.StopNewActivationAndTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	held, release := p.maybeHoldStop(request.GetDomain(), request.GetShard())
	p.recordHolder("Stop", request.GetDomain(), request.GetShard(), request, nil, held, false)
	if held {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	response, err := p.client.StopNewActivationAndTurns(ctx, request)
	p.recordHolder("Stop", request.GetDomain(), request.GetShard(), request, response, false, err == nil)
	return response, err
}

func (p *HolderForwardProxy) WaitCurrentTurns(ctx context.Context, request *holdercontrolpb.WaitCurrentTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	response, err := p.client.WaitCurrentTurns(ctx, request)
	p.recordHolder("Wait", request.GetDomain(), request.GetShard(), request, response, false, err == nil)
	return response, err
}

func (p *HolderForwardProxy) ConfirmFactsCommitted(ctx context.Context, request *holdercontrolpb.ConfirmFactsCommittedRequest) (*holdercontrolpb.HolderControlResponse, error) {
	response, err := p.client.ConfirmFactsCommitted(ctx, request)
	p.recordHolder("Facts", request.GetDomain(), request.GetShard(), request, response, false, err == nil)
	return response, err
}

func (p *HolderForwardProxy) PrepareShardGeneration(ctx context.Context, request *holdercontrolpb.PrepareShardGenerationRequest) (*holdercontrolpb.HolderControlResponse, error) {
	response, err := p.client.PrepareShardGeneration(ctx, request)
	p.recordHolder("Prepare", request.GetDomain(), request.GetShard(), request, response, false, err == nil)
	return response, err
}

func (p *HolderForwardProxy) GetShardStatus(ctx context.Context, request *holdercontrolpb.GetShardStatusRequest) (*holdercontrolpb.GetShardStatusResponse, error) {
	response, err := p.client.GetShardStatus(ctx, request)
	p.recordHolder("Status", request.GetDomain(), request.GetShard(), request, response, false, err == nil)
	return response, err
}

func (p *HolderForwardProxy) ArmStopHold(domain string, shard uint32) error {
	if p == nil || (domain != "PLAYER" && domain != "FARM") {
		return errors.New("invalid Holder Stop hold")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.holdStop.armed {
		return errors.New("Holder Stop hold is already armed")
	}
	p.holdStop = holderStopHold{armed: true, domain: domain, shard: shard, heldCh: make(chan struct{}), release: make(chan struct{})}
	return nil
}

func (p *HolderForwardProxy) WaitStopHeld(ctx context.Context) error {
	if p == nil || ctx == nil {
		return errors.New("Holder Stop wait requires context")
	}
	p.mu.Lock()
	if !p.holdStop.armed {
		p.mu.Unlock()
		return errors.New("Holder Stop hold is not armed")
	}
	held := p.holdStop.heldCh
	p.mu.Unlock()
	select {
	case <-held:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *HolderForwardProxy) ReleaseStop() error {
	if p == nil {
		return errors.New("Holder proxy is nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.holdStop.armed || !p.holdStop.held {
		return errors.New("Holder Stop is not held")
	}
	close(p.holdStop.release)
	p.holdStop.armed = false
	return nil
}

func (p *HolderForwardProxy) maybeHoldStop(domain string, shard uint32) (bool, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.holdStop.armed || p.holdStop.held || p.holdStop.domain != domain || p.holdStop.shard != shard {
		return false, nil
	}
	p.holdStop.held = true
	close(p.holdStop.heldCh)
	return true, p.holdStop.release
}

func (p *HolderForwardProxy) Events() []HolderProxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]HolderProxyEvent(nil), p.events...)
}

func (p *HolderForwardProxy) TLSFacts() []TLSConnectionFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSConnectionFact(nil), p.tlsFacts...)
}

func (p *HolderForwardProxy) TLSFailures() []TLSFailureFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSFailureFact(nil), p.tlsFailures...)
}

func (p *HolderForwardProxy) recordHolder(method, domain string, shard uint32, request, response proto.Message, held, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	p.events = append(p.events, HolderProxyEvent{
		Sequence: p.next, Method: method, Domain: domain, Shard: shard,
		RequestSHA256: protoDigest(request), ResponseSHA256: protoDigest(response), Held: held,
		UpstreamOK: ok, WallTime: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (p *HolderForwardProxy) recordTLS(fact TLSConnectionFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFacts {
		if existing == fact {
			return
		}
	}
	p.tlsFacts = append(p.tlsFacts, fact)
}

func (p *HolderForwardProxy) recordTLSFailure(fact TLSFailureFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFailures {
		if existing == fact {
			return
		}
	}
	p.tlsFailures = append(p.tlsFailures, fact)
}

func (p *HolderForwardProxy) Close() error {
	if p == nil {
		return nil
	}
	if p.server != nil {
		p.server.Stop()
	}
	var errs []error
	if p.conn != nil {
		errs = append(errs, p.conn.Close())
	}
	if p.listener != nil {
		if err := p.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func loadForwardTLS(cfg ForwardTLSConfig) (*tls.Config, *tls.Config, error) {
	values := []string{cfg.ServerCertFile, cfg.ServerKeyFile, cfg.ClientCAFile, cfg.AllowedClientIdentity, cfg.UpstreamTarget, cfg.UpstreamServerName, cfg.UpstreamClientCert, cfg.UpstreamClientKey, cfg.UpstreamServerCA}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return nil, nil, errors.New("forward proxy requires complete TLS and endpoint configuration")
		}
	}
	serverTLS, err := routetransport.LoadServerTLSConfig(cfg.ServerCertFile, cfg.ServerKeyFile, cfg.ClientCAFile)
	if err != nil {
		return nil, nil, err
	}
	upstreamTLS, err := routetransport.LoadClientTLSConfig(cfg.UpstreamClientCert, cfg.UpstreamClientKey, cfg.UpstreamServerCA)
	if err != nil {
		return nil, nil, err
	}
	upstreamTLS, err = routetransport.BindExactServerIdentity(upstreamTLS, cfg.UpstreamServerName, cfg.UpstreamServerName)
	if err != nil {
		return nil, nil, err
	}
	return serverTLS, upstreamTLS, nil
}

func listenOrLoopback(value string) string {
	if strings.TrimSpace(value) == "" {
		return "127.0.0.1:0"
	}
	return value
}

func chainedVerify(previous func(tls.ConnectionState) error, next func(tls.ConnectionState) error) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if previous != nil {
			if err := previous(state); err != nil {
				return err
			}
		}
		return next(state)
	}
}

func exactTLSFact(state tls.ConnectionState, expected, direction string) (TLSConnectionFact, error) {
	// VerifyConnection runs before crypto/tls publishes HandshakeComplete.
	// TLS 1.3 plus the single verified chain and exact leaf are the valid
	// callback-time predicates; a forwarded RPC proves completion afterward.
	if state.Version != tls.VersionTLS13 || len(state.VerifiedChains) != 1 || len(state.PeerCertificates) != 1 {
		return TLSConnectionFact{}, errors.New("forward proxy requires one verified TLS 1.3 peer leaf")
	}
	leaf := state.PeerCertificates[0]
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != expected || len(leaf.URIs) != 0 || len(leaf.IPAddresses) != 0 || leaf.Subject.CommonName != "" {
		return TLSConnectionFact{}, fmt.Errorf("forward proxy peer identity mismatch: want %s", expected)
	}
	digest := sha256.Sum256(leaf.Raw)
	return TLSConnectionFact{
		Direction: direction, PeerIdentity: expected, CertificateSHA256: hex.EncodeToString(digest[:]),
		TLSVersion: state.Version, VerifiedChainCount: len(state.VerifiedChains),
	}, nil
}

func tlsFailureFact(state tls.ConnectionState, expected, direction string) TLSFailureFact {
	class := "unknown"
	switch {
	case state.Version != tls.VersionTLS13:
		class = "tls-version"
	case len(state.VerifiedChains) != 1:
		class = "verified-chain-count"
	case len(state.PeerCertificates) != 1:
		class = "peer-certificate-count"
	default:
		leaf := state.PeerCertificates[0]
		switch {
		case len(leaf.DNSNames) != 1:
			class = "dns-san-count"
		case leaf.DNSNames[0] != expected:
			class = "dns-identity"
		case len(leaf.URIs) != 0 || len(leaf.IPAddresses) != 0:
			class = "alternate-identity"
		case leaf.Subject.CommonName != "":
			class = "common-name"
		}
	}
	return TLSFailureFact{Direction: direction, Class: class, TLSVersion: state.Version,
		PeerCertificateCount: len(state.PeerCertificates), VerifiedChainCount: len(state.VerifiedChains)}
}

func protoDigest(message proto.Message) string {
	if message == nil {
		return ""
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
