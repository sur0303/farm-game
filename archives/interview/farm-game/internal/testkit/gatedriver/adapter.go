// Package gatedriver is the farm-gate service adapter for the capacity
// driver. One capacity operation is one real session establishment: HTTPS
// login, WebSocket dial, ClientHello, and a ServerHello whose identity must
// match the login exactly. It is test-only infrastructure and contains no
// product business semantics.
package gatedriver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"farmgame/internal/testkit/capdriver"
	"farmgame/internal/testkit/gateclient"
)

// SessionIdentityMismatchGate counts ServerHello frames that carried an
// identity other than the one the login issued. A non-zero value is a
// correctness breach, not a load failure.
const SessionIdentityMismatchGate = "session_identity_mismatch_total"

// Config binds one adapter to one loopback farm-gate deployment.
type Config struct {
	Gate gateclient.Config
	// PlayerBase is the first load-test player id. Each arrival derives a
	// distinct player from its sequence so concurrent sessions never collide
	// on one identity.
	PlayerBase uint64
}

func (c Config) validate() error {
	if err := c.Gate.Validate(); err != nil {
		return err
	}
	attached, err := sameHostNetworks()
	if err != nil {
		return fmt.Errorf("gatedriver: read local networks: %w", err)
	}
	if err := requireSameHost(c.Gate.HTTPBaseURL, attached); err != nil {
		return err
	}
	if err := requireSameHost(c.Gate.WSURL, attached); err != nil {
		return err
	}
	if c.PlayerBase == 0 {
		return errors.New("gatedriver: player base is required")
	}
	if len(c.Gate.LoadSecret) == 0 {
		return errors.New("gatedriver: load secret is required")
	}
	return nil
}

// requireSameHost keeps the driver pointed at a deployment on this machine.
// What the harness needs is that no arrival crosses a router, and the address
// text is poor evidence of that in both directions: a loopback port forwarder
// reaches anywhere, while a container bridge address reaches nothing but this
// host. So the endpoint qualifies on how the kernel would route it, and
// attached is the set of networks that carry no gateway hop.
func requireSameHost(raw string, attached []*net.IPNet) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("gatedriver: parse %q: %w", raw, err)
	}
	host := parsed.Hostname()
	if host == "localhost" {
		return nil
	}
	address := net.ParseIP(host)
	if address == nil {
		return fmt.Errorf("gatedriver: %q does not name an address on this host", raw)
	}
	if address.IsLoopback() {
		return nil
	}
	for _, network := range attached {
		if network.Contains(address) {
			return nil
		}
	}
	return fmt.Errorf("gatedriver: %q is not an address on this host", raw)
}

// LoadSecretFromEnv reads the load-test signing secret from the environment so
// it never appears in a reviewed config file.
func LoadSecretFromEnv(name string) ([]byte, error) {
	secret := strings.TrimSpace(os.Getenv(name))
	if secret == "" {
		return nil, fmt.Errorf("gatedriver: %s is empty", name)
	}
	return []byte(secret), nil
}

// Adapter executes gate operations for capdriver.
type Adapter struct {
	config   Config
	sessions atomic.Uint64
	encoded  prometheus.Counter
	decoded  prometheus.Counter
	sockets  *socketMetrics
}

func New(config Config) (*Adapter, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	adapter := &Adapter{
		config: config,
		encoded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driver_frame_encode_total",
			Help: "Protocol frames encoded and sent by the load generator.",
		}),
		decoded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driver_frame_decode_total",
			Help: "Protocol frames received and decoded by the load generator.",
		}),
		sockets: newSocketMetrics(),
	}
	adapter.config.Gate.Frames = adapter
	adapter.config.Gate.HTTPClient = adapter.sockets.httpClient()
	adapter.config.Gate.NetDial = adapter.sockets.dial
	return adapter, nil
}

func (a *Adapter) ObserveFrameEncoded() { a.encoded.Inc() }

func (a *Adapter) ObserveFrameDecoded() { a.decoded.Inc() }

// Describe and Collect let the driver publish these counters on its own
// /metrics endpoint, which is where the collector reads load generator work.
func (a *Adapter) Describe(into chan<- *prometheus.Desc) {
	a.encoded.Describe(into)
	a.decoded.Describe(into)
	a.sockets.Describe(into)
}

func (a *Adapter) Collect(into chan<- prometheus.Metric) {
	a.encoded.Collect(into)
	a.decoded.Collect(into)
	a.sockets.Collect(into)
}

// Execute performs one session establishment and closes it. The connection is
// not held open: this adapter measures the arrival cost of a new session,
// which is the unit the login and handshake capacity chapters need.
func (a *Adapter) Execute(
	ctx context.Context,
	request capdriver.OperationRequest,
) capdriver.Result {
	gates := map[string]int{SessionIdentityMismatchGate: 0}
	playerID := a.config.PlayerBase + request.Sequence
	login, err := a.config.Gate.Login(ctx, playerID, nil)
	if err != nil {
		return a.fail(err, gates)
	}
	conn, _, err := a.config.Gate.Connect(ctx, login)
	if err != nil {
		return a.fail(err, gates)
	}
	defer conn.Close()
	a.sessions.Add(1)
	return capdriver.Result{Status: capdriver.StatusCompleted, QueueDepth: 0, Correctness: gates}
}

// fail records the socket-level half of a failure before mapping it onto a
// driver error class. Everything the socket could see is already counted by
// the instrumented connection; what reaches here is the protocol's view, where
// a stream that simply ended means the peer left mid-session.
func (a *Adapter) fail(err error, gates map[string]int) capdriver.Result {
	a.sockets.observeUnexpectedClose(err)
	return classify(err, gates)
}

// Sessions reports how many handshakes completed, so a qualification run can
// reconcile the adapter against the scheduler's own launched count.
func (a *Adapter) Sessions() uint64 {
	return a.sessions.Load()
}

// classify maps a failure onto the driver's allowlisted error classes. An
// identity mismatch stops the run as a correctness breach; a deliberate
// server-side rejection is capacity; anything else is an unavailable
// dependency.
func classify(err error, gates map[string]int) capdriver.Result {
	switch {
	case errors.Is(err, gateclient.ErrIdentityMismatch):
		gates[SessionIdentityMismatchGate] = 1
		return capdriver.Result{
			Status: capdriver.StatusFailed, ErrorClass: "CORRECTNESS_STOP",
			QueueDepth: 0, Correctness: gates,
		}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return capdriver.Result{
			Status: capdriver.StatusTimeout, ErrorClass: "REQUEST_TIMEOUT",
			QueueDepth: 0, Correctness: gates,
		}
	case capacityRejection(err):
		return capdriver.Result{
			Status: capdriver.StatusRejected, ErrorClass: "CAPACITY_REJECTED",
			QueueDepth: 0, Correctness: gates,
		}
	default:
		return capdriver.Result{
			Status: capdriver.StatusFailed, ErrorClass: "DEPENDENCY_UNAVAILABLE",
			QueueDepth: 0, Correctness: gates,
		}
	}
}

// capacityRejection is true when the deployment deliberately shed the arrival.
func capacityRejection(err error) bool {
	var status *gateclient.LoginStatusError
	if !errors.As(err, &status) {
		return false
	}
	switch status.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return true
	default:
		return false
	}
}
