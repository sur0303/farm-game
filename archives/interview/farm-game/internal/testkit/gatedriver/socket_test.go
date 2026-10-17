package gatedriver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"farmgame/internal/testkit/capdriver"
	"farmgame/internal/testkit/gateclient"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// A run stops on the slope of driver_socket_error_total, so the counter has to
// separate a generator whose own sockets are failing from a service that is
// answering with a rejection. Counting the rejection would blame the generator
// for a measurement that is exactly what the run went looking for.
func TestSocketErrorCountsTransportFailuresNotServiceAnswers(t *testing.T) {
	for name, testCase := range map[string]struct {
		configure    func(t *testing.T) Config
		wantStatus   capdriver.Status
		wantErrClass string
		wantFailures float64
		wantTraffic  bool
	}{
		"dial refused": {
			configure:    unreachableGateConfig,
			wantStatus:   capdriver.StatusFailed,
			wantErrClass: "DEPENDENCY_UNAVAILABLE",
			wantFailures: 1,
		},
		"peer vanishes mid handshake": {
			configure: func(t *testing.T) Config {
				stub := newGateStub(t)
				stub.abortBeforeHello = true
				return stub.config()
			},
			wantStatus:   capdriver.StatusFailed,
			wantErrClass: "DEPENDENCY_UNAVAILABLE",
			wantFailures: 1,
			wantTraffic:  true,
		},
		"capacity rejection": {
			configure: func(t *testing.T) Config {
				stub := newGateStub(t)
				stub.loginStatus = http.StatusTooManyRequests
				return stub.config()
			},
			wantStatus:   capdriver.StatusRejected,
			wantErrClass: "CAPACITY_REJECTED",
			wantFailures: 0,
			wantTraffic:  true,
		},
		"service error response": {
			configure: func(t *testing.T) Config {
				stub := newGateStub(t)
				stub.loginStatus = http.StatusInternalServerError
				return stub.config()
			},
			wantStatus:   capdriver.StatusFailed,
			wantErrClass: "DEPENDENCY_UNAVAILABLE",
			wantFailures: 0,
			wantTraffic:  true,
		},
		"borrowed session": {
			configure: func(t *testing.T) Config {
				stub := newGateStub(t)
				stub.helloSession = "session-from-a-previous-login"
				return stub.config()
			},
			wantStatus:   capdriver.StatusFailed,
			wantErrClass: "CORRECTNESS_STOP",
			wantFailures: 0,
			wantTraffic:  true,
		},
		"completed session": {
			configure: func(t *testing.T) Config {
				return newGateStub(t).config()
			},
			wantStatus:   capdriver.StatusCompleted,
			wantFailures: 0,
			wantTraffic:  true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			adapter, handler := newDriver(t, testCase.configure(t))
			result := adapter.Execute(context.Background(), operation(1))
			if result.Status != testCase.wantStatus || result.ErrorClass != testCase.wantErrClass {
				t.Fatalf("result=%+v", result)
			}
			exposed := scrapeDriver(t, handler)
			if got := metricValue(t, exposed, "driver_socket_error_total"); got != testCase.wantFailures {
				t.Fatalf("driver_socket_error_total=%v want %v", got, testCase.wantFailures)
			}
			received := metricValue(t, exposed, "driver_network_rx_bytes_total")
			sent := metricValue(t, exposed, "driver_network_tx_bytes_total")
			if traffic := received > 0 && sent > 0; traffic != testCase.wantTraffic {
				t.Fatalf("rx=%v tx=%v want traffic=%v", received, sent, testCase.wantTraffic)
			}
		})
	}
}

// The byte counters answer whether the generator is still putting load on the
// wire, so they have to move with every session rather than only at the end.
func TestNetworkByteCountersAdvanceWithTraffic(t *testing.T) {
	adapter, handler := newDriver(t, newGateStub(t).config())
	var previousReceived, previousSent float64
	for sequence := uint64(1); sequence <= 3; sequence++ {
		if result := adapter.Execute(context.Background(), operation(sequence)); result.Status != capdriver.StatusCompleted {
			t.Fatalf("sequence %d result=%+v", sequence, result)
		}
		exposed := scrapeDriver(t, handler)
		received := metricValue(t, exposed, "driver_network_rx_bytes_total")
		sent := metricValue(t, exposed, "driver_network_tx_bytes_total")
		if received <= previousReceived || sent <= previousSent {
			t.Fatalf("sequence %d rx=%v (was %v) tx=%v (was %v)",
				sequence, received, previousReceived, sent, previousSent)
		}
		if failures := metricValue(t, exposed, "driver_socket_error_total"); failures != 0 {
			t.Fatalf("sequence %d reported %v socket errors on healthy sessions", sequence, failures)
		}
		previousReceived, previousSent = received, sent
	}
}

// The socket layer sees every failed operation but cannot see intent, so it
// counts a broken socket and stays silent on the three outcomes that are the
// generator's own doing: a stream that ended, a socket it closed itself, and a
// deadline it set.
func TestSocketLayerCountsBrokenSocketsOnly(t *testing.T) {
	for name, testCase := range map[string]struct {
		err  error
		want float64
	}{
		"connection refused":          {&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, 1},
		"connection reset":            {&net.OpError{Op: "read", Err: syscall.ECONNRESET}, 1},
		"broken pipe":                 {&net.OpError{Op: "write", Err: syscall.EPIPE}, 1},
		"host unreachable":            {&net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH}, 1},
		"end of stream":               {io.EOF, 0},
		"generator closed the socket": {net.ErrClosed, 0},
		"read deadline expired":       {&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, 0},
		"operation cancelled":         {context.Canceled, 0},
		"operation completed":         {nil, 0},
	} {
		t.Run(name, func(t *testing.T) {
			metrics := newSocketMetrics()
			metrics.observeFailure(testCase.err)
			if got := collected(t, metrics, "driver_socket_error_total"); got != testCase.want {
				t.Fatalf("driver_socket_error_total=%v want %v for %v", got, testCase.want, testCase.err)
			}
		})
	}
}

// The protocol layer adds the one failure the socket reads as an ordinary end
// of stream, and nothing else: a service that answered, however it answered,
// is not a broken socket, and a socket error already counted below must not be
// counted twice.
func TestProtocolLayerCountsAnUnexpectedCloseOnly(t *testing.T) {
	for name, testCase := range map[string]struct {
		err  error
		want float64
	}{
		"peer vanished mid frame":           {&websocket.CloseError{Code: websocket.CloseAbnormalClosure}, 1},
		"stream ended before the reply":     {io.EOF, 1},
		"negotiated close":                  {&websocket.CloseError{Code: websocket.CloseNormalClosure}, 0},
		"service shed the login":            {&gateclient.LoginStatusError{StatusCode: http.StatusTooManyRequests}, 0},
		"service returned another identity": {gateclient.ErrIdentityMismatch, 0},
		"socket error already counted":      {&net.OpError{Op: "read", Err: syscall.ECONNRESET}, 0},
		"session completed":                 {nil, 0},
	} {
		t.Run(name, func(t *testing.T) {
			metrics := newSocketMetrics()
			metrics.observeUnexpectedClose(testCase.err)
			if got := collected(t, metrics, "driver_socket_error_total"); got != testCase.want {
				t.Fatalf("driver_socket_error_total=%v want %v for %v", got, testCase.want, testCase.err)
			}
		})
	}
}

// unreachableGateConfig points the driver at a loopback port nothing listens
// on, which is the one dial failure a test can produce deterministically.
func unreachableGateConfig(t *testing.T) Config {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return Config{
		Gate: gateclient.Config{
			HTTPBaseURL: "http://" + address,
			WSURL:       "ws://" + address + "/ws",
			LoadSecret:  []byte("secret"),
			ClientBuild: "cap-gate/1",
		},
		PlayerBase: playerBase,
	}
}

func newDriver(t *testing.T, config Config) (*Adapter, http.Handler) {
	t.Helper()
	adapter, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := capdriver.NewServer(capdriver.Config{
		BinarySHA256: strings.Repeat("a", 64), ConfigSHA256: strings.Repeat("b", 64),
		Service: capdriver.ServiceGate, Profile: "gate-qualification",
		MaximumInflight: 4, Ready: true,
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, server.Handler()
}

func scrapeDriver(t *testing.T, handler http.Handler) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics code=%d", recorder.Code)
	}
	return recorder.Body.String()
}

func collected(t *testing.T, metrics *socketMetrics, name string) float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics)
	return metricValue(t, scrapeDriver(t, promhttp.HandlerFor(registry, promhttp.HandlerOpts{})), name)
}

func metricValue(t *testing.T, exposed string, name string) float64 {
	t.Helper()
	line := metricLine(exposed, name)
	if line == "" {
		t.Fatalf("%s is not exposed on the driver scrape endpoint", name)
	}
	value, err := strconv.ParseFloat(strings.TrimPrefix(line, name+" "), 64)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return value
}
