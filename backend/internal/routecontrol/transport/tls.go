package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// LoadServerTLSConfig loads a strict mTLS server configuration. The client
// certificate is verified by the supplied CA before gRPC handlers run.
func LoadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("%w: server certificate", ErrTLSConfig)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: server key", ErrTLSConfig)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("%w: client CA", ErrTLSConfig)
	}
	return NewServerTLSConfig(certPEM, keyPEM, caPEM)
}

// NewServerTLSConfig constructs a strict mTLS server configuration from PEM
// bytes. It never enables an insecure or one-way TLS fallback.
func NewServerTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: server certificate/key", ErrTLSConfig)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: client CA", ErrTLSConfig)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		NextProtos:   []string{"h2"},
	}, nil
}

// LoadClientTLSConfig loads a strict mTLS client configuration.
func LoadClientTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("%w: client certificate", ErrTLSConfig)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: client key", ErrTLSConfig)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("%w: server CA", ErrTLSConfig)
	}
	return NewClientTLSConfig(certPEM, keyPEM, caPEM)
}

// NewClientTLSConfig constructs a strict mTLS client configuration. ServerName
// is intentionally left to the caller because it is deployment-specific.
func NewClientTLSConfig(certPEM, keyPEM, caPEM []byte) (*tls.Config, error) {
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: client certificate/key", ErrTLSConfig)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: server CA", ErrTLSConfig)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		NextProtos:   []string{"h2"},
	}, nil
}

// BindExactServerIdentity returns a clone that requires TLS 1.3, ordinary DNS
// chain/hostname verification, and exactly one canonical DNS SAN on the
// verified leaf. CN, IP, URI, multiple-SAN, and intermediate identities never
// satisfy this verifier.
func BindExactServerIdentity(config *tls.Config, serverName, expectedIdentity string) (*tls.Config, error) {
	clone, err := cloneTLSConfig(config)
	if err != nil {
		return nil, err
	}
	serverIdentity, err := canonicalDNSIdentity(serverName)
	if err != nil {
		return nil, fmt.Errorf("%w: ServerName", ErrTLSConfig)
	}
	expected, err := canonicalDNSIdentity(expectedIdentity)
	if err != nil || expected != serverIdentity {
		return nil, fmt.Errorf("%w: exact DNS leaf identity required", ErrTLSIdentityMismatch)
	}
	previous := clone.VerifyConnection
	clone.ServerName = serverIdentity
	clone.VerifyConnection = func(state tls.ConnectionState) error {
		if state.Version < tls.VersionTLS13 || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
			return ErrPeerUnauthenticated
		}
		identity, identityErr := exactDNSCertificateIdentity(state.VerifiedChains[0][0])
		if identityErr != nil || identity != expected {
			return ErrTLSIdentityMismatch
		}
		if previous != nil {
			return previous(state)
		}
		return nil
	}
	return clone, nil
}

func validateServerTLSConfig(config *tls.Config) error {
	if config == nil {
		return ErrTLSRequired
	}
	if config.InsecureSkipVerify {
		return fmt.Errorf("%w: InsecureSkipVerify", ErrTLSConfig)
	}
	if len(config.Certificates) == 0 {
		return fmt.Errorf("%w: server certificate required", ErrTLSConfig)
	}
	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		return fmt.Errorf("%w: RequireAndVerifyClientCert required", ErrTLSConfig)
	}
	if config.ClientCAs == nil {
		return fmt.Errorf("%w: client CA required", ErrTLSConfig)
	}
	if config.MinVersion < tls.VersionTLS13 {
		return fmt.Errorf("%w: TLS 1.3 or newer required", ErrTLSConfig)
	}
	return nil
}

func validateClientTLSConfig(config *tls.Config) error {
	if config == nil {
		return ErrTLSRequired
	}
	if config.InsecureSkipVerify {
		return fmt.Errorf("%w: InsecureSkipVerify", ErrTLSConfig)
	}
	if len(config.Certificates) == 0 {
		return fmt.Errorf("%w: client certificate required", ErrTLSConfig)
	}
	if config.RootCAs == nil {
		return fmt.Errorf("%w: server CA required", ErrTLSConfig)
	}
	if config.MinVersion < tls.VersionTLS13 {
		return fmt.Errorf("%w: TLS 1.3 or newer required", ErrTLSConfig)
	}
	return nil
}

func validateTLSConfigForPeer(config *tls.Config) error {
	if config == nil {
		return ErrTLSRequired
	}
	if config.InsecureSkipVerify {
		return fmt.Errorf("%w: InsecureSkipVerify", ErrTLSConfig)
	}
	if config.MinVersion != 0 && config.MinVersion < tls.VersionTLS13 {
		return fmt.Errorf("%w: TLS 1.3 or newer required", ErrTLSConfig)
	}
	return nil
}

func cloneTLSConfig(config *tls.Config) (*tls.Config, error) {
	if err := validateTLSConfigForPeer(config); err != nil {
		return nil, err
	}
	clone := config.Clone()
	if clone.MinVersion == 0 {
		clone.MinVersion = tls.VersionTLS13
	}
	if err := validateClientTLSConfig(clone); err != nil {
		return nil, err
	}
	return clone, nil
}

func requireTLSPaths(certFile, keyFile, caFile string) error {
	for name, value := range map[string]string{
		"certificate": certFile,
		"key":         keyFile,
		"CA":          caFile,
	} {
		if strings.TrimSpace(value) == "" {
			return errors.New("routecontrol transport: " + name + " path is required")
		}
	}
	return nil
}
