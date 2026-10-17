package farmbotclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	wireprotocol "farmgame/internal/gateway/protocol"
)

const (
	// ProtocolVersion is the only FarmFrame version accepted by this client.
	ProtocolVersion = wireprotocol.ProtocolVersion

	// FarmSessionCookieName is the HTTP session cookie issued by login.
	FarmSessionCookieName = "farm_session"

	defaultTimeout         = 10 * time.Second
	defaultMaxResponseBody = 1 << 20
	defaultMaxFrameBytes   = wireprotocol.DefaultMaxFrameSize
)

// Config contains the endpoints and bounded transport settings used by a
// Client. TLSCA and TLSCAPEM are PEM-encoded CA certificates; TLSCAPath is a
// convenience for reading them from a file. TLSCA and TLSCAPEM are combined.
type Config struct {
	HTTPBaseURL string
	WSURL       string

	TLSCA     []byte
	TLSCAPEM  []byte
	TLSCAPath string

	Timeout     time.Duration
	ClientBuild string

	// IdleTimeout bounds how long the background reader waits for any inbound
	// frame, including the server WebSocket ping. It must stay above the gate
	// ping period or a healthy idle session would be torn down as dead. A
	// non-positive value selects the default.
	IdleTimeout time.Duration

	// TrackFarmView makes each session materialize the farm state from its
	// join snapshot and the realtime stream. A bot that acts repeatedly needs
	// it; a bot that performs one action and disconnects does not, and pays
	// the decode cost for nothing.
	TrackFarmView bool

	// MaxIdleConnsPerHost bounds the pooled login connections kept for the
	// gate. Every bot logs in over HTTP before it opens its WebSocket, and at
	// the transport default of two idle connections all but two of those
	// sockets are closed on return; a run of any size then spends its local
	// port range on TIME_WAIT rather than on bots. A non-positive value keeps
	// the transport default.
	MaxIdleConnsPerHost int

	// OperationResultPoll overrides how long the client waits before asking
	// again whether an async operation settled.
	//
	// By default the server's own retry hint is honoured, which is what a real
	// client does. That hint is a quarter of a second, so an async command can
	// never be measured as faster than that however quickly the server
	// actually finished, and the whole range below it is invisible. A
	// measurement run can shorten the wait to see the server instead of the
	// hint, at the cost of more lookup traffic than a real client would send.
	// A non-positive value honours the server.
	OperationResultPoll time.Duration

	// MaxResponseBodyBytes bounds every login response body. A non-positive
	// value selects a conservative default.
	MaxResponseBodyBytes int64
	// MaxFrameBytes bounds one serialized FarmFrame in either direction.
	// A non-positive value selects the protocol package default.
	MaxFrameBytes int

	// HTTPClient is optional. When nil, the client builds one with Timeout and
	// the configured CA pool. A supplied client is used as-is.
	HTTPClient *http.Client
}

func (c Config) normalized() (Config, error) {
	if err := validateEndpoint(c.HTTPBaseURL, "http", "https"); err != nil {
		return Config{}, fmt.Errorf("invalid HTTPBaseURL: %w", err)
	}
	if err := validateEndpoint(c.WSURL, "ws", "wss"); err != nil {
		return Config{}, fmt.Errorf("invalid WSURL: %w", err)
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxResponseBodyBytes <= 0 {
		c.MaxResponseBodyBytes = defaultMaxResponseBody
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = defaultMaxFrameBytes
	}
	if strings.TrimSpace(c.ClientBuild) == "" {
		c.ClientBuild = "farmbotclient/1"
	}
	return c, nil
}

func validateEndpoint(raw string, schemes ...string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("endpoint is empty")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return errors.New("endpoint must be an absolute URL without userinfo")
	}
	for _, scheme := range schemes {
		if strings.EqualFold(u.Scheme, scheme) {
			return nil
		}
	}
	return fmt.Errorf("unsupported scheme %q", u.Scheme)
}

func (c Config) tlsConfig() (*tls.Config, error) {
	var pemBytes []byte
	pemBytes = append(pemBytes, c.TLSCA...)
	pemBytes = append(pemBytes, c.TLSCAPEM...)
	if strings.TrimSpace(c.TLSCAPath) != "" {
		filePEM, err := os.ReadFile(c.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pemBytes = append(pemBytes, filePEM...)
	}
	if len(pemBytes) == 0 {
		return nil, nil
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("TLS CA contains no certificate")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}

func (c Config) httpClient() (*http.Client, error) {
	if c.HTTPClient != nil {
		return c.HTTPClient, nil
	}
	tlsConfig, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Client{Timeout: c.Timeout}, nil
	}
	transport = transport.Clone()
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	if c.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = c.MaxIdleConnsPerHost
		if transport.MaxIdleConns < c.MaxIdleConnsPerHost {
			transport.MaxIdleConns = c.MaxIdleConnsPerHost
		}
	}
	return &http.Client{Timeout: c.Timeout, Transport: transport}, nil
}
