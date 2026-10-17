package gatewayapp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/holdercontrol"
	"farmgame/internal/routecontrol/transport"
	"google.golang.org/grpc"
)

const (
	maxHolderProcessMessageBytes = 4 << 20
	maxHolderProcessTimeout      = 30 * time.Second
)

var subjectAlternativeNameOID = asn1.ObjectIdentifier{2, 5, 29, 17}

// BoundedHolderTimeout and BoundedHolderMessageBytes are command-root config
// helpers. They preserve a safe fallback and prevent deployment input from
// removing the frozen Holder operation/close/message ceilings.
func BoundedHolderTimeout(value, fallback time.Duration) time.Duration {
	if fallback <= 0 {
		fallback = 5 * time.Second
	}
	if fallback > maxHolderProcessTimeout {
		fallback = maxHolderProcessTimeout
	}
	if value <= 0 {
		return fallback
	}
	if value > maxHolderProcessTimeout {
		return maxHolderProcessTimeout
	}
	return value
}

func BoundedHolderMessageBytes(value, fallback int) int {
	if fallback <= 0 {
		fallback = maxHolderProcessMessageBytes
	}
	if fallback > maxHolderProcessMessageBytes {
		fallback = maxHolderProcessMessageBytes
	}
	if value <= 0 {
		return fallback
	}
	if value > maxHolderProcessMessageBytes {
		return maxHolderProcessMessageBytes
	}
	return value
}

// HolderRuntimeProcessStatus derives only lifecycle admission flags and keeps
// the caller's real serving facts, revision, capacity, and resource summary.
// Runtime fatal and explicit shutdown are draining conditions, allowing the
// next bounded Agent heartbeat/close to durably publish DRAINING/DRAINED.
// A transient assignment outage is different: the data plane fails closed via
// ProcessReady, but it must not irreversibly translate Controller unavailability
// into a planned Holder drain. The Agent retains its existing durable SERVING
// record and may renew it after assignment recovery within the Holder lease.
func HolderRuntimeProcessStatus(status holdercontrol.ProcessStatus, runtimeHealthy, assignmentHealthy, shutdownDraining bool) holdercontrol.ProcessStatus {
	status.ProcessReady = runtimeHealthy && assignmentHealthy && !shutdownDraining
	status.Draining = shutdownDraining || !runtimeHealthy
	return status
}

// HolderSnapshotRevisionEvidence decides which applied RouteSnapshot revision a
// Holder may publish as its own evidence. A revision that already removed every
// local shard is withheld while the process still serves one: HolderRegistry
// only accepts SERVING -> READY_ZERO_SHARD together with a strictly newer
// revision, and the emptying revision is usually the last one a scale-in
// publishes. Reporting it before the local shards are actually gone therefore
// spends the only proof the drained Holder has, and scale-down never completes.
// Zero means "no new evidence this observation": the Agent keeps the durable
// revision it already published. A Holder that still wants shards reports the
// applied revision immediately, so migration freshness is unaffected.
func HolderSnapshotRevisionEvidence(appliedRevision uint64, servingShardCount uint32, desiredShardCount int) uint64 {
	if servingShardCount > 0 && desiredShardCount == 0 {
		return 0
	}
	return appliedRevision
}

type grpcStopper interface {
	GracefulStop()
	Stop()
}

// BoundedGRPCGracefulStop ensures a stuck RPC cannot prevent later Holder,
// Pump, runtime, and client cleanup. Stop is the forced fallback.
func BoundedGRPCGracefulStop(ctx context.Context, server grpcStopper) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		server.Stop()
		// grpc.Server.Stop normally releases GracefulStop immediately. Do not
		// add a second unbounded wait here: later Holder/Pump/runtime cleanup
		// must continue even if a test seam or future server is non-cooperative.
		return ctx.Err()
	}
}

// HolderProcessOptions is the minimal RT2I construction seam for one domain
// holder. The command root supplies the real registry, ProcessStatus source,
// and already-built LocalService; this package supplies only the sealed
// transport lifecycle.
type HolderProcessOptions struct {
	ListenAddr          string
	ServerCertFile      string
	ServerKeyFile       string
	ClientCAFile        string
	ControllerIdentity  string
	LocalService        *holdercontrol.LocalService
	Registry            holdercontrol.HolderRegistryPort
	ProcessStatus       holdercontrol.ProcessStatusSource
	StartupReadyTimeout time.Duration
	AgentOptions        holdercontrol.HolderAgentOptions
	MaxMessageBytes     int

	// Listen and Serve are minimal lifecycle test seams. Production leaves
	// them nil and receives net.Listen plus SealedGRPCServer.Serve.
	Listen func(string, string) (net.Listener, error)
	Serve  func(*transport.SealedGRPCServer, net.Listener) error
}

type acceptObservedListener struct {
	net.Listener
	accepted chan struct{}
	once     sync.Once
}

func (l *acceptObservedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return conn, err
}

// HolderProcess owns exactly one sealed HolderControl server and one
// HolderAgent. It deliberately exposes no grpc.ServiceRegistrar.
type HolderProcess struct {
	Server   *transport.SealedGRPCServer
	Listener net.Listener
	Agent    *holdercontrol.HolderAgent

	serveListener net.Listener
	serve         func(*transport.SealedGRPCServer, net.Listener) error
	processStatus holdercontrol.ProcessStatusSource
	accepted      chan struct{}
	serverReady   atomic.Bool
	stopping      atomic.Bool
	started       atomic.Bool
	agentStarted  atomic.Bool
	serveDone     chan struct{}

	mu                  sync.Mutex
	terminalErr         error
	startMu             sync.Mutex
	failOnce            sync.Once
	failStarted         atomic.Bool
	failDone            chan struct{}
	closeLimit          time.Duration
	startupReadyLimit   time.Duration
	allowInitialServing bool
}

func NewHolderProcess(opts HolderProcessOptions) (*HolderProcess, error) {
	if opts.LocalService == nil || opts.Registry == nil || opts.ProcessStatus == nil {
		return nil, fmt.Errorf("holder process: local service, registry, and process status are required")
	}
	if opts.ListenAddr == "" || opts.ControllerIdentity == "" {
		return nil, fmt.Errorf("holder process: listen address and controller identity are required")
	}
	tlsConfig, err := transport.LoadServerTLSConfig(opts.ServerCertFile, opts.ServerKeyFile, opts.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("holder process: load TLS files: %w", err)
	}
	if err := validateHolderCertificateIdentity(tlsConfig.Certificates, opts.AgentOptions.TLSDNSIdentity); err != nil {
		return nil, fmt.Errorf("holder process: server certificate identity: %w", err)
	}
	rules := holdercontrol.HolderControlMethodACL([]string{opts.ControllerIdentity})
	if err := holdercontrol.ValidateHolderControlMethodACL(rules); err != nil {
		return nil, fmt.Errorf("holder process: validate exact Holder ACL: %w", err)
	}
	authorizer, err := transport.NewMethodAuthorizer(rules)
	if err != nil {
		return nil, fmt.Errorf("holder process: Holder method authorizer: %w", err)
	}
	server, err := transport.NewHolderOnlySealedGRPCServer(tlsConfig, transport.GRPCServerOptions{
		MaxMessageBytes:  BoundedHolderMessageBytes(opts.MaxMessageBytes, maxHolderProcessMessageBytes),
		MethodAuthorizer: authorizer,
	}, func(registrar grpc.ServiceRegistrar) {
		// The generated Holder registration is construction-only. The returned
		// sealed server does not retain the Registrar capability.
		holdercontrolpb.RegisterHolderControlServiceServer(registrar, opts.LocalService)
	})
	if err != nil {
		return nil, fmt.Errorf("holder process: construct sealed Holder server: %w", err)
	}
	listen := opts.Listen
	if listen == nil {
		listen = net.Listen
	}
	listener, err := listen("tcp", opts.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("holder process: listen %s: %w", opts.ListenAddr, err)
	}
	agentOptions := opts.AgentOptions
	agentOptions.HeartbeatInterval = BoundedHolderTimeout(agentOptions.HeartbeatInterval, 5*time.Second)
	agentOptions.HeartbeatTimeout = BoundedHolderTimeout(agentOptions.HeartbeatTimeout, 2*time.Second)
	agentOptions.CloseTimeout = BoundedHolderTimeout(agentOptions.CloseTimeout, 10*time.Second)
	agentOptions.ClosePollInterval = BoundedHolderTimeout(agentOptions.ClosePollInterval, 100*time.Millisecond)
	agentOptions.Registry = opts.Registry
	process := &HolderProcess{
		Server: server, Listener: listener, Agent: nil,
		accepted: make(chan struct{}), serveDone: make(chan struct{}), failDone: make(chan struct{}),
		serve: opts.Serve, processStatus: opts.ProcessStatus, closeLimit: agentOptions.CloseTimeout,
		startupReadyLimit:   BoundedHolderTimeout(opts.StartupReadyTimeout, 20*time.Second),
		allowInitialServing: agentOptions.AllowInitialServing,
	}
	if process.serve == nil {
		process.serve = func(server *transport.SealedGRPCServer, listener net.Listener) error {
			return server.Serve(listener)
		}
	}
	process.serveListener = &acceptObservedListener{Listener: listener, accepted: process.accepted}
	agentOptions.ProcessStatus = holdercontrol.ProcessStatusSourceFunc(process.agentProcessStatus)
	agentOptions.ServerReady = func() bool {
		return process.serverReady.Load() && process.TerminalError() == nil
	}
	agent, err := holdercontrol.NewHolderAgent(agentOptions)
	if err != nil {
		_ = listener.Close()
		server.Stop()
		return nil, fmt.Errorf("holder process: construct HolderAgent: %w", err)
	}
	process.Agent = agent
	return process, nil
}

// agentProcessStatus preserves the real runtime facts while preventing any
// Agent operation from translating a failed/not-started Holder server into a
// ready registration or heartbeat.
func (p *HolderProcess) agentProcessStatus(ctx context.Context) (holdercontrol.ProcessStatus, error) {
	if p == nil || p.processStatus == nil {
		return holdercontrol.ProcessStatus{}, errors.New("holder process: process status source is required")
	}
	status, err := p.processStatus.Status(ctx)
	if err != nil {
		return holdercontrol.ProcessStatus{}, err
	}
	if !p.serverReady.Load() || p.TerminalError() != nil {
		status.ProcessReady = false
		status.Draining = true
	}
	return status, nil
}

// Start preserves the original one-call lifecycle while allowing command roots
// to start the control server before their remote assignment becomes healthy.
// That split is required for a cold generation: RouteController must be able to
// verify the exact WARMING boot before it can publish the first ASSIGNING route.
func (p *HolderProcess) Start(ctx context.Context) error {
	if err := p.StartControlServer(ctx); err != nil {
		return err
	}
	return p.StartAgent(ctx)
}

// StartControlServer proves the sealed HolderControl server has entered a
// successful Accept, but deliberately performs no durable Holder registration.
// During this interval GetRuntimeIdentity may expose only the exact WARMING,
// zero-shard process generation discovered from Kubernetes.
func (p *HolderProcess) StartControlServer(ctx context.Context) error {
	if p == nil || p.Server == nil || p.Listener == nil || p.Agent == nil || p.serveListener == nil {
		return fmt.Errorf("holder process: incomplete process")
	}
	if ctx == nil {
		return fmt.Errorf("holder process: context is required")
	}
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.started.Load() {
		if err := p.TerminalError(); err != nil {
			return fmt.Errorf("holder process: prior startup or Serve terminal: %w", err)
		}
		if !p.serverReady.Load() {
			return errors.New("holder process: already stopped")
		}
		return nil
	}
	p.started.Store(true)
	go p.serveOwner()

	probeCtx, cancelProbe := context.WithTimeout(ctx, BoundedHolderTimeout(p.closeLimit, 2*time.Second))
	probeErr := p.probeServe(probeCtx)
	cancelProbe()
	if probeErr != nil {
		p.recordTerminal(probeErr)
		p.stopping.Store(true)
		p.serverReady.Store(false)
		p.Server.Stop()
		_ = p.Listener.Close()
		if !waitHolderProcessDone(p.serveDone, p.closeLimit) {
			return fmt.Errorf("holder process: Holder server did not stop after startup failure: %w", probeErr)
		}
		return fmt.Errorf("holder process: Holder server did not start: %w", probeErr)
	}
	select {
	case <-p.serveDone:
		return fmt.Errorf("holder process: Holder server exited during startup: %w", p.TerminalError())
	default:
	}
	p.serverReady.Store(true)
	select {
	case <-p.serveDone:
		p.serverReady.Store(false)
		return fmt.Errorf("holder process: Holder server exited during startup: %w", p.TerminalError())
	default:
	}
	return nil
}

// StartAgent waits until remote assignment and the actor runtime are healthy,
// then starts the durable HolderAgent registration/heartbeat lifecycle. The
// control server must already be serving so a cold target can be verified and
// prepared without falsely advertising data-plane readiness.
func (p *HolderProcess) StartAgent(ctx context.Context) error {
	if p == nil || p.Server == nil || p.Listener == nil || p.Agent == nil || p.processStatus == nil {
		return fmt.Errorf("holder process: incomplete process")
	}
	if ctx == nil {
		return fmt.Errorf("holder process: context is required")
	}
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if !p.started.Load() || !p.serverReady.Load() {
		return errors.New("holder process: control server is not ready")
	}
	if p.agentStarted.Load() {
		if err := p.TerminalError(); err != nil {
			return fmt.Errorf("holder process: prior startup or Serve terminal: %w", err)
		}
		status := p.Agent.Status()
		if status.State != holdercontrol.AgentStateRunning || status.Unhealthy {
			return errors.New("holder process: HolderAgent is not running")
		}
		return nil
	}
	if err := p.waitProcessReadyBeforeRegistration(ctx); err != nil {
		p.recordTerminal(err)
		p.stopping.Store(true)
		p.serverReady.Store(false)
		p.Server.Stop()
		_ = p.Listener.Close()
		if !waitHolderProcessDone(p.serveDone, p.closeLimit) {
			return fmt.Errorf("holder process: Holder server did not stop after process readiness failure: %w", err)
		}
		return fmt.Errorf("holder process: process not ready before Agent registration: %w", err)
	}
	if err := p.Agent.Start(ctx); err != nil {
		p.recordTerminal(err)
		p.stopping.Store(true)
		p.serverReady.Store(false)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), p.closeLimit)
		_ = BoundedGRPCGracefulStop(stopCtx, p.Server)
		stopCancel()
		_ = p.Listener.Close()
		if !waitHolderProcessDone(p.serveDone, p.closeLimit) {
			return fmt.Errorf("holder process: Holder server did not stop after Agent startup failure: %w", err)
		}
		return fmt.Errorf("holder process: start HolderAgent after server ready: %w", err)
	}
	p.agentStarted.Store(true)
	return nil
}

// waitProcessReadyBeforeRegistration prevents a transient cold-start state
// from publishing WARMING and then durably draining the immutable boot when
// the Agent's mandatory initial heartbeat observes not-ready. The Holder
// server is already bound for peer pre-dial, but no registry mutation occurs
// until the exact runtime/assignment status can pass that initial heartbeat.
func (p *HolderProcess) waitProcessReadyBeforeRegistration(parent context.Context) error {
	if p == nil || p.processStatus == nil || parent == nil {
		return errors.New("holder process: process readiness inputs are required")
	}
	ctx, cancel := context.WithTimeout(parent, p.startupReadyLimit)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		status, err := p.agentProcessStatus(ctx)
		if err == nil && status.ProcessReady && !status.Draining &&
			(status.ServingShardCount == 0 || p.allowInitialServing) {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-p.serveDone:
			if terminal := p.TerminalError(); terminal != nil {
				return terminal
			}
			return errors.New("holder process: Holder server exited before process readiness")
		case <-ctx.Done():
			if lastErr != nil {
				return errors.Join(ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitHolderProcessDone(done <-chan struct{}, timeout time.Duration) bool {
	if done == nil {
		return true
	}
	timer := time.NewTimer(BoundedHolderTimeout(timeout, 2*time.Second))
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (p *HolderProcess) serveOwner() {
	err := p.serve(p.Server, p.serveListener)
	p.serverReady.Store(false)
	if !p.stopping.Load() {
		if err == nil {
			err = errors.New("Holder server exited without error")
		}
		p.recordTerminal(err)
		p.failClosed(err)
	}
	close(p.serveDone)
}

func (p *HolderProcess) probeServe(ctx context.Context) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, p.Listener.Addr().Network(), holderProbeAddress(p.Listener.Addr()))
	if err != nil {
		select {
		case <-p.serveDone:
			if terminal := p.TerminalError(); terminal != nil {
				return terminal
			}
		default:
		}
		return err
	}
	_ = conn.Close()
	select {
	case <-p.accepted:
		return nil
	case <-p.serveDone:
		if terminal := p.TerminalError(); terminal != nil {
			return terminal
		}
		return errors.New("Holder server exited before Accept")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func holderProbeAddress(address net.Addr) string {
	if tcp, ok := address.(*net.TCPAddr); ok {
		host := tcp.IP.String()
		if tcp.IP == nil || tcp.IP.IsUnspecified() {
			if tcp.IP != nil && tcp.IP.To4() == nil {
				host = "::1"
			} else {
				host = "127.0.0.1"
			}
		}
		return net.JoinHostPort(host, fmt.Sprintf("%d", tcp.Port))
	}
	return address.String()
}

func (p *HolderProcess) failClosed(err error) {
	if p == nil {
		return
	}
	p.serverReady.Store(false)
	p.recordTerminal(err)
	if p.Agent == nil {
		return
	}
	p.failOnce.Do(func() {
		p.failStarted.Store(true)
		go func() {
			defer close(p.failDone)
			ctx, cancel := context.WithTimeout(context.Background(), p.closeLimit)
			defer cancel()
			_ = p.Agent.Close(ctx)
		}()
	})
}

// FailClosed is used by the assignment terminal monitor. It retains the
// terminal cause locally, clears process readiness, and transfers draining
// and registry cleanup to HolderAgent.Close.
func (p *HolderProcess) FailClosed(err error) {
	if p == nil || err == nil {
		return
	}
	p.failClosed(err)
}

func (p *HolderProcess) recordTerminal(err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	if p.terminalErr == nil {
		p.terminalErr = err
	}
	p.mu.Unlock()
}

func (p *HolderProcess) TerminalError() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminalErr
}

func (p *HolderProcess) Ready() bool {
	if p == nil || !p.serverReady.Load() || p.TerminalError() != nil || p.Agent == nil {
		return false
	}
	status := p.Agent.Status()
	return status.State == holdercontrol.AgentStateRunning && !status.Unhealthy
}

// GracefulStop stops the sealed Holder server with a bounded fallback. The
// Agent should normally be closed by the command root first.
func (p *HolderProcess) GracefulStop(ctx context.Context) error {
	if p == nil || p.Server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.startMu.Lock()
	defer p.startMu.Unlock()
	p.stopping.Store(true)
	p.serverReady.Store(false)
	var err error
	if p.started.Load() {
		err = BoundedGRPCGracefulStop(ctx, p.Server)
		_ = p.Listener.Close()
		select {
		case <-p.serveDone:
		case <-ctx.Done():
			p.Server.Stop()
			if err == nil {
				err = ctx.Err()
			}
		}
	} else {
		p.Server.Stop()
		_ = p.Listener.Close()
	}
	if p.failStarted.Load() && !waitHolderProcessDone(p.failDone, p.closeLimit) && err == nil {
		err = errors.New("holder process: timed out joining HolderAgent fail-closed owner")
	}
	return err
}

func validateHolderCertificateIdentity(certificates []tls.Certificate, identity string) error {
	if !strictHolderDNSIdentity(identity) {
		return errors.New("Agent TLS DNS identity must be canonical ASCII DNS without wildcard")
	}
	if len(certificates) != 1 || len(certificates[0].Certificate) == 0 {
		return errors.New("exactly one server certificate chain is required")
	}
	leaf, err := x509.ParseCertificate(certificates[0].Certificate[0])
	if err != nil {
		return errors.New("server leaf certificate cannot be parsed")
	}
	if certificates[0].Leaf != nil && !certificates[0].Leaf.Equal(leaf) {
		return errors.New("server parsed leaf does not match the verified key pair")
	}
	if leaf.Subject.CommonName != "" || len(leaf.DNSNames) != 1 || len(leaf.EmailAddresses) != 0 ||
		len(leaf.IPAddresses) != 0 || len(leaf.URIs) != 0 {
		return errors.New("server leaf must contain only one DNS SAN and no CN/email/IP/URI identity")
	}
	rawDNS, err := rawSingleDNSSAN(leaf)
	if err != nil {
		return err
	}
	if rawDNS != leaf.DNSNames[0] || !strictHolderDNSIdentity(rawDNS) || rawDNS != identity {
		return errors.New("server leaf DNS SAN must exactly equal Agent TLS DNS identity")
	}
	return nil
}

func strictHolderDNSIdentity(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || strings.ToLower(value) != value ||
		strings.HasSuffix(value, ".") || strings.Contains(value, "*") || len(value) > 253 {
		return false
	}
	for _, b := range []byte(value) {
		if b >= 0x80 {
			return false
		}
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, b := range []byte(label) {
			if (b < 'a' || b > 'z') && (b < '0' || b > '9') && b != '-' {
				return false
			}
		}
	}
	return true
}

func rawSingleDNSSAN(leaf *x509.Certificate) (string, error) {
	if leaf == nil {
		return "", errors.New("parsed leaf certificate is required")
	}
	var san []byte
	for _, extension := range leaf.Extensions {
		if extension.Id.Equal(subjectAlternativeNameOID) {
			if san != nil {
				return "", errors.New("multiple subjectAltName extensions are forbidden")
			}
			san = extension.Value
		}
	}
	if san == nil {
		return "", errors.New("subjectAltName extension is required")
	}
	var sequence asn1.RawValue
	rest, err := asn1.Unmarshal(san, &sequence)
	if err != nil || len(rest) != 0 || sequence.Class != asn1.ClassUniversal || sequence.Tag != asn1.TagSequence || !sequence.IsCompound {
		return "", errors.New("invalid raw subjectAltName extension")
	}
	names := sequence.Bytes
	var dns string
	count := 0
	for len(names) != 0 {
		var name asn1.RawValue
		next, parseErr := asn1.Unmarshal(names, &name)
		if parseErr != nil || len(next) >= len(names) {
			return "", errors.New("invalid raw subjectAltName entry")
		}
		if name.Class != asn1.ClassContextSpecific || name.Tag != 2 || name.IsCompound {
			return "", errors.New("only one DNS subjectAltName is allowed")
		}
		count++
		dns = string(name.Bytes)
		names = next
	}
	if count != 1 {
		return "", errors.New("exactly one DNS subjectAltName is required")
	}
	return dns, nil
}
