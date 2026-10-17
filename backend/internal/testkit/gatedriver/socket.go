package gatedriver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
)

// socketMetrics instruments every socket the load generator opens. A capacity
// run stops on the slope of driver_socket_error_total, so the counter must
// rise only when the generator's own transport breaks: a service that answers
// with a rejection is a measurement, not a broken socket.
type socketMetrics struct {
	dialer   net.Dialer
	failures prometheus.Counter
	received prometheus.Counter
	sent     prometheus.Counter
}

func newSocketMetrics() *socketMetrics {
	return &socketMetrics{
		// The default transport's dial settings, so measuring the sockets
		// cannot change the arrival cost being measured.
		dialer: net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second},
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driver_socket_error_total",
			Help: "Socket-level failures in the load generator: dial, read, write, and unexpected close.",
		}),
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driver_network_rx_bytes_total",
			Help: "Bytes the load generator read off its sockets.",
		}),
		sent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "driver_network_tx_bytes_total",
			Help: "Bytes the load generator wrote to its sockets.",
		}),
	}
}

func (m *socketMetrics) Describe(into chan<- *prometheus.Desc) {
	m.failures.Describe(into)
	m.received.Describe(into)
	m.sent.Describe(into)
}

func (m *socketMetrics) Collect(into chan<- prometheus.Metric) {
	m.failures.Collect(into)
	m.received.Collect(into)
	m.sent.Collect(into)
}

// httpClient carries the login request. Only the dial is replaced: the default
// transport's pooling and timeouts stay exactly as an uninstrumented driver
// had them, so the load profile does not change with the instrumentation.
func (m *socketMetrics) httpClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = m.dial
	return &http.Client{Transport: transport}
}

// dial opens every socket the generator uses, the login and the WebSocket
// alike, so a refused connection is counted once and every byte that later
// crosses the connection is counted where it meets the wire.
func (m *socketMetrics) dial(ctx context.Context, network string, address string) (net.Conn, error) {
	conn, err := m.dialer.DialContext(ctx, network, address)
	if err != nil {
		m.observeFailure(err)
		return nil, err
	}
	return &countedConn{Conn: conn, metrics: m}, nil
}

// countedConn measures one socket. It sits below TLS and below the frame
// codec, so the byte counts are wire bytes rather than payload bytes.
type countedConn struct {
	net.Conn
	metrics *socketMetrics
}

func (c *countedConn) Read(buffer []byte) (int, error) {
	count, err := c.Conn.Read(buffer)
	if count > 0 {
		c.metrics.received.Add(float64(count))
	}
	c.metrics.observeFailure(err)
	return count, err
}

func (c *countedConn) Write(buffer []byte) (int, error) {
	count, err := c.Conn.Write(buffer)
	if count > 0 {
		c.metrics.sent.Add(float64(count))
	}
	c.metrics.observeFailure(err)
	return count, err
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.metrics.observeFailure(err)
	return err
}

// observeFailure counts a failed socket operation. Three outcomes are
// deliberately not counted. An ordinary end of stream is left to
// observeUnexpectedClose, because only the protocol layer knows whether a
// frame was still owed. A socket the generator closed itself is its own
// bookkeeping racing the close. A cancelled or expired deadline is the
// generator giving up on a slow service, and counting it would let a saturated
// service trip the harness's load generator stop condition, which exists to
// catch the opposite failure.
func (m *socketMetrics) observeFailure(err error) {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) || timedOut(err) {
		return
	}
	m.failures.Inc()
}

// observeUnexpectedClose counts the one socket failure the connection itself
// cannot recognise: the peer vanished while the generator was still waiting
// for a frame. To the socket that is an ordinary end of stream; to the
// handshake it is a broken session. A close the service performs after
// answering carries a close code and is the service's decision, so it is not
// counted here.
func (m *socketMetrics) observeUnexpectedClose(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		m.failures.Inc()
	}
}

func timedOut(err error) bool {
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}
