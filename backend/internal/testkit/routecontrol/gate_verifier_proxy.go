package routecontrol

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	gateverificationpb "farmgame/api/gen/gateverification"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GateVerifierFault names the owned ObserveShard reply-mutation modes used by S4.
type GateVerifierFault string

const (
	GateFaultNone          GateVerifierFault = ""
	GateFaultStaleRevision GateVerifierFault = "stale"
	GateFaultUnavailable   GateVerifierFault = "unavailable"
	GateFaultPending       GateVerifierFault = "pending"
	GateFaultUnknown       GateVerifierFault = "unknown"
	GateFaultMissingReplay GateVerifierFault = "missing-replay"
)

// GateVerifierProxyEvent records one authenticated ObserveShard forward without
// exposing raw Redis values or secrets.
type GateVerifierProxyEvent struct {
	Sequence              uint64 `json:"sequence"`
	PeerName              string `json:"peer_name,omitempty"`
	Fault                 string `json:"fault,omitempty"`
	UpstreamOK            bool   `json:"upstream_ok"`
	Mutated               bool   `json:"mutated,omitempty"`
	SnapshotRevision      uint64 `json:"snapshot_revision,omitempty"`
	AssignmentRevision    uint64 `json:"assignment_revision,omitempty"`
	WriterEpoch           uint64 `json:"writer_epoch,omitempty"`
	LeaseEpoch            uint64 `json:"lease_epoch,omitempty"`
	UpstreamResponseSHA   string `json:"upstream_response_sha256,omitempty"`
	DownstreamResponseSHA string `json:"downstream_response_sha256,omitempty"`
	Identical             bool   `json:"identical,omitempty"`
	ErrorCode             string `json:"error_code,omitempty"`
	WallTime              string `json:"wall_time"`
}

// GateVerifierForwardProxy terminates controller→Gate verifier mTLS, forwards
// ObserveShard to a real production Gate, and can apply one owned reply fault.
type GateVerifierForwardProxy struct {
	gateverificationpb.UnimplementedGateVerificationServiceServer

	mu            sync.Mutex
	peerName      string
	listener      net.Listener
	server        *grpc.Server
	connMu        sync.Mutex
	conn          *grpc.ClientConn
	client        gateverificationpb.GateVerificationServiceClient
	dialUpstream  func() (*grpc.ClientConn, error)
	upstreamStale bool
	fault         GateVerifierFault
	events        []GateVerifierProxyEvent
	tlsFacts      []TLSConnectionFact
	tlsFailures   []TLSFailureFact
	next          uint64
}

func StartGateVerifierForwardProxy(peerName string, cfg ForwardTLSConfig) (*GateVerifierForwardProxy, error) {
	if strings.TrimSpace(peerName) == "" {
		return nil, errors.New("gate verifier proxy requires a peer name")
	}
	serverTLS, upstreamTLS, err := loadForwardTLS(cfg)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenOrLoopback(cfg.ListenAddress))
	if err != nil {
		return nil, err
	}
	proxy := &GateVerifierForwardProxy{peerName: peerName, listener: listener}
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
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10), grpc.WaitForReady(true)))
	}
	proxy.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10))
	gateverificationpb.RegisterGateVerificationServiceServer(proxy.server, proxy)
	go func() { _ = proxy.server.Serve(listener) }()
	return proxy, nil
}

func (p *GateVerifierForwardProxy) Address() string {
	if p == nil || p.listener == nil {
		return ""
	}
	return p.listener.Addr().String()
}

func (p *GateVerifierForwardProxy) PeerName() string {
	if p == nil {
		return ""
	}
	return p.peerName
}

func (p *GateVerifierForwardProxy) ArmFault(fault GateVerifierFault) error {
	if p == nil {
		return errors.New("gate verifier proxy is nil")
	}
	switch fault {
	case GateFaultStaleRevision, GateFaultUnavailable, GateFaultPending, GateFaultUnknown, GateFaultMissingReplay:
	default:
		return errors.New("unsupported gate verifier fault")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fault != GateFaultNone {
		return errors.New("gate verifier fault is already armed")
	}
	p.fault = fault
	return nil
}

func (p *GateVerifierForwardProxy) ClearFault() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fault = GateFaultNone
}

func (p *GateVerifierForwardProxy) Fault() GateVerifierFault {
	if p == nil {
		return GateFaultNone
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fault
}

func (p *GateVerifierForwardProxy) ObserveShard(ctx context.Context, request *gateverificationpb.ObserveShardRequest) (*gateverificationpb.ObserveShardResponse, error) {
	if p == nil {
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	p.mu.Lock()
	fault := p.fault
	p.mu.Unlock()
	if fault == GateFaultUnavailable {
		p.recordEvent(fault, nil, nil, false, true, status.Error(codes.Unavailable, "gate observation unavailable"))
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	client, err := p.verifierClient()
	if err != nil {
		p.recordEvent(fault, nil, nil, false, false, err)
		return nil, status.Error(codes.Unavailable, "gate observation unavailable")
	}
	upstream, err := client.ObserveShard(ctx, request)
	if err != nil {
		p.recordEvent(fault, nil, nil, false, false, err)
		return nil, err
	}
	downstream := proto.Clone(upstream).(*gateverificationpb.ObserveShardResponse)
	mutated := false
	switch fault {
	case GateFaultStaleRevision:
		if downstream.GetSnapshotRevision() > 1 {
			downstream.SnapshotRevision = downstream.GetSnapshotRevision() - 1
			mutated = true
		}
	case GateFaultPending:
		downstream.State = "ASSIGNING"
		mutated = true
	case GateFaultUnknown:
		downstream.WriterEpoch = 0
		mutated = true
	case GateFaultMissingReplay:
		if downstream.GetAssignmentRevision() > 0 {
			downstream.AssignmentRevision = downstream.GetAssignmentRevision() - 1
			mutated = true
		}
		if downstream.GetHolderBootId() != "" {
			downstream.HolderBootId = downstream.GetHolderBootId() + "-missing-replay"
			mutated = true
		}
	}
	identical := !mutated && protoDigest(upstream) != "" && protoDigest(upstream) == protoDigest(downstream)
	p.recordEvent(fault, upstream, downstream, identical, mutated, nil)
	return downstream, nil
}

func (p *GateVerifierForwardProxy) verifierClient() (gateverificationpb.GateVerificationServiceClient, error) {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.client != nil && !p.upstreamStale {
		return p.client, nil
	}
	if p.dialUpstream == nil {
		return nil, errors.New("gate verifier proxy upstream dialer is unavailable")
	}
	conn, err := p.dialUpstream()
	if err != nil {
		return nil, err
	}
	old := p.conn
	p.conn = conn
	p.client = gateverificationpb.NewGateVerificationServiceClient(conn)
	p.upstreamStale = false
	if old != nil {
		_ = old.Close()
	}
	return p.client, nil
}

func (p *GateVerifierForwardProxy) Events() []GateVerifierProxyEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]GateVerifierProxyEvent(nil), p.events...)
}

func (p *GateVerifierForwardProxy) TLSFacts() []TLSConnectionFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSConnectionFact(nil), p.tlsFacts...)
}

func (p *GateVerifierForwardProxy) TLSFailures() []TLSFailureFact {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TLSFailureFact(nil), p.tlsFailures...)
}

func (p *GateVerifierForwardProxy) recordEvent(fault GateVerifierFault, upstream, downstream *gateverificationpb.ObserveShardResponse, identical, mutated bool, rpcErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	event := GateVerifierProxyEvent{
		Sequence: p.next, PeerName: p.peerName, Fault: string(fault),
		UpstreamOK: rpcErr == nil, Mutated: mutated, Identical: identical,
		WallTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if rpcErr != nil {
		event.ErrorCode = status.Code(rpcErr).String()
	}
	if upstream != nil {
		event.UpstreamResponseSHA = protoDigest(upstream)
		event.SnapshotRevision = upstream.GetSnapshotRevision()
		event.AssignmentRevision = upstream.GetAssignmentRevision()
		event.WriterEpoch = upstream.GetWriterEpoch()
		event.LeaseEpoch = upstream.GetLeaseEpoch()
	}
	if downstream != nil {
		event.DownstreamResponseSHA = protoDigest(downstream)
		if mutated {
			event.SnapshotRevision = downstream.GetSnapshotRevision()
			event.AssignmentRevision = downstream.GetAssignmentRevision()
			event.WriterEpoch = downstream.GetWriterEpoch()
			event.LeaseEpoch = downstream.GetLeaseEpoch()
		}
	}
	p.events = append(p.events, event)
}

func (p *GateVerifierForwardProxy) recordTLS(fact TLSConnectionFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFacts {
		if existing == fact {
			return
		}
	}
	p.tlsFacts = append(p.tlsFacts, fact)
}

func (p *GateVerifierForwardProxy) recordTLSFailure(fact TLSFailureFact) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.tlsFailures {
		if existing == fact {
			return
		}
	}
	p.tlsFailures = append(p.tlsFailures, fact)
}

func (p *GateVerifierForwardProxy) Close() error {
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
