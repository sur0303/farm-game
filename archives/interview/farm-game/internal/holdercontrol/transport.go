package holdercontrol

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrHolderTLSConfig      = errors.New("holdercontrol: invalid TLS configuration")
	ErrHolderTLSIdentity    = errors.New("holdercontrol: TLS server identity mismatch")
	ErrHolderClientClosed   = errors.New("holdercontrol: client is closed")
	ErrHolderRPCUnavailable = errors.New("holdercontrol: client RPC unavailable")
	ErrHolderMethodACL      = errors.New("holdercontrol: invalid method ACL")
	ErrHolderBudget         = errors.New("holdercontrol: budget is outside the allowed range")
)

const (
	defaultHolderControlMaxMessageBytes = 1 << 20
	defaultHolderControlDialTimeout     = 5 * time.Second
	defaultHolderControlRPCTimeout      = 5 * time.Second
	defaultHolderControlCloseTimeout    = 5 * time.Second
	minHolderControlMessageBytes        = 1 << 10
	maxHolderControlMessageBytes        = 4 << 20
	minHolderControlTimeout             = time.Millisecond
	maxHolderControlTimeout             = 30 * time.Second
)

type HolderControlDialOptions struct {
	ServerName             string
	ExpectedServerIdentity string
	MaxMessageBytes        int
	DialTimeout            time.Duration
	RPCTimeout             time.Duration
}

type HolderControlClient struct {
	conn                   *grpc.ClientConn
	rpc                    holdercontrolpb.HolderControlServiceClient
	verifiedServerIdentity string
	rpcTimeout             time.Duration

	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// DialHolderControl is the only production constructor. It accepts PEM
// material, constructs TLS 1.3 mTLS credentials, and requires the explicit
// DNS ServerName to equal the expected peer identity.
func DialHolderControl(ctx context.Context, target string, certPEM, keyPEM, caPEM []byte, options ...HolderControlDialOptions) (*HolderControlClient, error) {
	if ctx == nil {
		return nil, ErrHolderRPCUnavailable
	}
	if strings.TrimSpace(target) == "" || strings.TrimSpace(target) != target {
		return nil, fmt.Errorf("%w: target", ErrHolderTLSConfig)
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("%w: one dial options value is allowed", ErrHolderTLSConfig)
	}
	var opts HolderControlDialOptions
	if len(options) == 1 {
		opts = options[0]
	}
	var err error
	opts, err = normalizeDialOptions(opts)
	if err != nil {
		return nil, err
	}
	if err := validateHolderServerIdentity(opts.ServerName, opts.ExpectedServerIdentity); err != nil {
		return nil, err
	}
	tlsConfig, err := transport.NewClientTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHolderTLSConfig, err)
	}
	boundTLS, err := transport.BindExactServerIdentity(tlsConfig, opts.ServerName, opts.ExpectedServerIdentity)
	if err != nil {
		return nil, fmt.Errorf("%w: exact verified TLS leaf required", ErrHolderTLSIdentity)
	}
	if err := validateHolderClientTLSConfig(boundTLS); err != nil {
		return nil, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, opts.DialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		target,
		grpc.WithTransportCredentials(credentials.NewTLS(boundTLS)),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(opts.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(opts.MaxMessageBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	return &HolderControlClient{
		conn: conn, rpc: holdercontrolpb.NewHolderControlServiceClient(conn),
		// DialContext is blocking: this proof is populated only after the exact
		// leaf verifier completed a TLS 1.3 verified-chain handshake.
		verifiedServerIdentity: opts.ExpectedServerIdentity, rpcTimeout: opts.RPCTimeout,
	}, nil
}

func normalizeDialOptions(opts HolderControlDialOptions) (HolderControlDialOptions, error) {
	if opts.MaxMessageBytes < 0 {
		return HolderControlDialOptions{}, fmt.Errorf("%w: max message bytes", ErrHolderBudget)
	}
	if opts.MaxMessageBytes == 0 {
		opts.MaxMessageBytes = defaultHolderControlMaxMessageBytes
	}
	if opts.MaxMessageBytes < minHolderControlMessageBytes || opts.MaxMessageBytes > maxHolderControlMessageBytes {
		return HolderControlDialOptions{}, fmt.Errorf("%w: max message bytes must be in [%d,%d]", ErrHolderBudget, minHolderControlMessageBytes, maxHolderControlMessageBytes)
	}
	if opts.DialTimeout < 0 || opts.RPCTimeout < 0 {
		return HolderControlDialOptions{}, fmt.Errorf("%w: timeout", ErrHolderBudget)
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = defaultHolderControlDialTimeout
	}
	if opts.RPCTimeout == 0 {
		opts.RPCTimeout = defaultHolderControlRPCTimeout
	}
	if err := validateHolderControlTimeout("dial timeout", opts.DialTimeout); err != nil {
		return HolderControlDialOptions{}, err
	}
	if err := validateHolderControlTimeout("rpc timeout", opts.RPCTimeout); err != nil {
		return HolderControlDialOptions{}, err
	}
	return opts, nil
}

func validateHolderServerIdentity(serverName, expected string) error {
	if strings.TrimSpace(serverName) != serverName || strings.TrimSpace(expected) != expected ||
		serverName == "" || expected == "" || serverName != expected {
		return fmt.Errorf("%w: ServerName and ExpectedServerIdentity must be identical", ErrHolderTLSIdentity)
	}
	if _, err := canonicalHolderDNSIdentity(serverName); err != nil {
		return fmt.Errorf("%w: DNS identity required", ErrHolderTLSIdentity)
	}
	if _, err := canonicalHolderDNSIdentity(expected); err != nil {
		return fmt.Errorf("%w: DNS identity required", ErrHolderTLSIdentity)
	}
	return nil
}

func canonicalHolderDNSIdentity(value string) (string, error) {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 253 ||
		net.ParseIP(value) != nil || strings.ContainsAny(value, ":/\\*_") ||
		value != strings.ToLower(value) {
		return "", fmt.Errorf("%w: invalid DNS identity", ErrHolderTLSIdentity)
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("%w: invalid DNS identity", ErrHolderTLSIdentity)
		}
		for i := 0; i < len(label); i++ {
			value := label[i]
			if (value < 'a' || value > 'z') && (value < '0' || value > '9') && value != '-' {
				return "", fmt.Errorf("%w: invalid DNS identity", ErrHolderTLSIdentity)
			}
		}
	}
	return value, nil
}

func validateHolderControlTimeout(name string, value time.Duration) error {
	if value < minHolderControlTimeout || value > maxHolderControlTimeout {
		return fmt.Errorf("%w: %s must be in [%s,%s]", ErrHolderBudget, name, minHolderControlTimeout, maxHolderControlTimeout)
	}
	return nil
}

func validateHolderClientTLSConfig(config *tls.Config) error {
	if config == nil || config.InsecureSkipVerify || config.MinVersion < tls.VersionTLS13 ||
		len(config.Certificates) == 0 || config.RootCAs == nil || config.ServerName == "" {
		return ErrHolderTLSConfig
	}
	return nil
}

func (c *HolderControlClient) Identity() string {
	if c == nil {
		return ""
	}
	return c.verifiedServerIdentity
}

func (c *HolderControlClient) Close() error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultHolderControlCloseTimeout)
	defer cancel()
	return c.CloseContext(ctx)
}

// CloseContext permanently closes the client before waiting for the
// underlying connection to finish closing. A caller timeout only stops the
// wait; the asynchronous close continues to completion.
func (c *HolderControlClient) CloseContext(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return ErrHolderRPCUnavailable
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.closeDone = make(chan struct{})
		conn := c.conn
		closeDone := c.closeDone
		c.mu.Unlock()
		go func() {
			var err error
			if conn != nil {
				err = conn.Close()
			}
			c.mu.Lock()
			c.closeErr = err
			c.mu.Unlock()
			close(closeDone)
		}()
	})

	c.mu.Lock()
	closeDone := c.closeDone
	c.mu.Unlock()
	if closeDone == nil {
		return nil
	}
	select {
	case <-closeDone:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *HolderControlClient) rpcContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if c == nil || ctx == nil {
		return nil, nil, ErrHolderRPCUnavailable
	}
	c.mu.Lock()
	closed := c.closed
	timeout := c.rpcTimeout
	rpc := c.rpc
	c.mu.Unlock()
	if closed {
		return nil, nil, ErrHolderClientClosed
	}
	if rpc == nil {
		return nil, nil, ErrHolderRPCUnavailable
	}
	if err := validateHolderControlTimeout("rpc timeout", timeout); err != nil {
		return nil, nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	return callCtx, cancel, nil
}

func (c *HolderControlClient) StopNewActivationAndTurns(ctx context.Context, req *holdercontrolpb.StopNewActivationAndTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.StopNewActivationAndTurns(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) WaitCurrentTurns(ctx context.Context, req *holdercontrolpb.WaitCurrentTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.WaitCurrentTurns(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) ConfirmFactsCommitted(ctx context.Context, req *holdercontrolpb.ConfirmFactsCommittedRequest) (*holdercontrolpb.HolderControlResponse, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.ConfirmFactsCommitted(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) PrepareShardGeneration(ctx context.Context, req *holdercontrolpb.PrepareShardGenerationRequest) (*holdercontrolpb.HolderControlResponse, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.PrepareShardGeneration(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) GetShardStatus(ctx context.Context, req *holdercontrolpb.GetShardStatusRequest) (*holdercontrolpb.GetShardStatusResponse, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.GetShardStatus(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) GetRuntimeIdentity(ctx context.Context, req *holdercontrolpb.GetRuntimeIdentityRequest) (*holdercontrolpb.RuntimeIdentity, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.GetRuntimeIdentity(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

func (c *HolderControlClient) ConfirmZeroShard(ctx context.Context, req *holdercontrolpb.ConfirmZeroShardRequest) (*holdercontrolpb.RuntimeIdentity, error) {
	callCtx, cancel, err := c.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.ConfirmZeroShard(callCtx, req)
	if err == nil {
		err = callCtx.Err()
	}
	return response, err
}

var holderControlFullMethods = [...]string{
	holdercontrolpb.HolderControlService_StopNewActivationAndTurns_FullMethodName,
	holdercontrolpb.HolderControlService_WaitCurrentTurns_FullMethodName,
	holdercontrolpb.HolderControlService_ConfirmFactsCommitted_FullMethodName,
	holdercontrolpb.HolderControlService_PrepareShardGeneration_FullMethodName,
	holdercontrolpb.HolderControlService_GetShardStatus_FullMethodName,
	holdercontrolpb.HolderControlService_GetRuntimeIdentity_FullMethodName,
	holdercontrolpb.HolderControlService_ConfirmZeroShard_FullMethodName,
}

func HolderControlFullMethodNames() []string {
	return append([]string(nil), holderControlFullMethods[:]...)
}

// HolderControlMethodACL returns exact full-method rules for one or more
// already canonical peer identities. It never creates wildcard/prefix rules.
func HolderControlMethodACL(identities []string) map[string][]string {
	copied := append([]string(nil), identities...)
	rules := make(map[string][]string, len(holderControlFullMethods))
	for _, method := range holderControlFullMethods {
		rules[method] = append([]string(nil), copied...)
	}
	return rules
}

func ValidateHolderControlMethodACL(rules map[string][]string) error {
	if len(rules) != len(holderControlFullMethods) {
		return ErrHolderMethodACL
	}
	for _, method := range holderControlFullMethods {
		identities, ok := rules[method]
		if !ok || len(identities) == 0 {
			return fmt.Errorf("%w: missing %s", ErrHolderMethodACL, method)
		}
		seen := make(map[string]struct{}, len(identities))
		for _, identity := range identities {
			canonical, err := canonicalHolderDNSIdentity(identity)
			if err != nil || canonical != identity {
				return fmt.Errorf("%w: non-canonical identity", ErrHolderMethodACL)
			}
			if _, exists := seen[identity]; exists {
				return fmt.Errorf("%w: duplicate identity", ErrHolderMethodACL)
			}
			seen[identity] = struct{}{}
		}
	}
	return nil
}
