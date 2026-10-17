package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strings"
	"unicode"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var ErrCertificateIdentityAmbiguous = errors.New("routecontrol transport: certificate identity is ambiguous")

var subjectAltNameOID = asn1.ObjectIdentifier{2, 5, 29, 17}

// VerifiedPeerIdentity returns the canonical identity from a completed TLS 1.3
// handshake and its first verified peer certificate chain. It deliberately
// accepts only one URI SAN or one DNS SAN through certificateIdentity; CN and
// all ambiguous SAN combinations remain rejected.
func VerifiedPeerIdentity(ctx context.Context) (string, error) {
	certificate, err := verifiedPeerLeaf(ctx)
	if err != nil {
		return "", err
	}
	identity, err := certificateIdentity(certificate)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrPeerUnauthenticated, err)
	}
	return identity, nil
}

// VerifiedExactDNSPeerIdentity is the Gate-specific client authorization
// path. Generic Holder/SPIFFE authorization continues to use
// VerifiedPeerIdentity and may accept its one canonical URI SAN contract.
func VerifiedExactDNSPeerIdentity(ctx context.Context) (string, error) {
	certificate, err := verifiedPeerLeaf(ctx)
	if err != nil {
		return "", err
	}
	identity, err := exactDNSCertificateIdentity(certificate)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrPeerUnauthenticated, err)
	}
	return identity, nil
}

func verifiedPeerLeaf(ctx context.Context) (*x509.Certificate, error) {
	if isNilValue(ctx) {
		return nil, ErrPeerUnauthenticated
	}
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil || p.AuthInfo == nil {
		return nil, ErrPeerUnauthenticated
	}
	var info credentials.TLSInfo
	switch typed := p.AuthInfo.(type) {
	case credentials.TLSInfo:
		info = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return nil, ErrPeerUnauthenticated
		}
		info = *typed
	default:
		return nil, ErrPeerUnauthenticated
	}
	if !info.State.HandshakeComplete ||
		info.State.Version < tls.VersionTLS13 ||
		len(info.State.VerifiedChains) == 0 ||
		len(info.State.VerifiedChains[0]) == 0 {
		return nil, ErrPeerUnauthenticated
	}
	return info.State.VerifiedChains[0][0], nil
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func certificateIdentity(certificate *x509.Certificate) (string, error) {
	if certificate == nil {
		return "", ErrPeerUnauthenticated
	}
	if len(certificate.IPAddresses) > 0 {
		return "", fmt.Errorf("%w: IP SAN identities are unsupported", ErrCertificateIdentityAmbiguous)
	}
	if len(certificate.EmailAddresses) > 0 {
		return "", fmt.Errorf("%w: email SAN identities are unsupported", ErrCertificateIdentityAmbiguous)
	}
	raw, err := rawSubjectAltName(certificate.Extensions)
	if err != nil {
		return "", err
	}
	switch raw.tag {
	case 2:
		if len(certificate.URIs) != 0 || len(certificate.DNSNames) != 1 ||
			string(raw.value) != certificate.DNSNames[0] {
			return "", certificateIdentityError("raw dNSName does not match Go-parsed SAN values")
		}
		return canonicalDNSIdentity(certificate.DNSNames[0])
	case 6:
		if len(certificate.DNSNames) != 0 || len(certificate.URIs) != 1 ||
			certificate.URIs[0] == nil || string(raw.value) != certificate.URIs[0].String() {
			return "", certificateIdentityError("raw URI does not match Go-parsed SAN values")
		}
		return canonicalURIIdentity(certificate.URIs[0].String())
	}
	return "", certificateIdentityError("unsupported raw SAN tag %d", raw.tag)
}

// exactDNSCertificateIdentity is the DNS-only leaf verifier used by strict
// server bindings and Gate-specific authorization. The raw SAN bytes must be
// one already-canonical lowercase dNSName; generic peer identity parsing keeps
// its separate URI-capable contract.
func exactDNSCertificateIdentity(certificate *x509.Certificate) (string, error) {
	if certificate == nil || len(certificate.IPAddresses) != 0 || len(certificate.EmailAddresses) != 0 || len(certificate.URIs) != 0 {
		return "", certificateIdentityError("exact DNS leaf required")
	}
	raw, err := rawSubjectAltName(certificate.Extensions)
	if err != nil || raw.tag != 2 || len(certificate.DNSNames) != 1 || string(raw.value) != certificate.DNSNames[0] {
		return "", certificateIdentityError("exact raw dNSName required")
	}
	identity, err := canonicalDNSIdentity(certificate.DNSNames[0])
	if err != nil || identity != certificate.DNSNames[0] || identity != string(raw.value) {
		return "", certificateIdentityError("canonical raw dNSName required")
	}
	return identity, nil
}

type rawSubjectAltNameValue struct {
	tag   uint64
	value []byte
}

type derElement struct {
	class       int
	tag         uint64
	constructed bool
	value       []byte
}

func rawSubjectAltName(extensions []pkix.Extension) (rawSubjectAltNameValue, error) {
	var extension *pkix.Extension
	for index := range extensions {
		if !extensions[index].Id.Equal(subjectAltNameOID) {
			continue
		}
		if extension != nil {
			return rawSubjectAltNameValue{}, certificateIdentityError("duplicate subjectAltName extensions")
		}
		extension = &extensions[index]
	}
	if extension == nil {
		return rawSubjectAltNameValue{}, certificateIdentityError("subjectAltName extension is required")
	}

	sequence, rest, err := parseDERElement(extension.Value)
	if err != nil || len(rest) != 0 {
		return rawSubjectAltNameValue{}, certificateIdentityError("subjectAltName extension is not a single DER value")
	}
	if sequence.class != asn1.ClassUniversal || sequence.tag != asn1.TagSequence || !sequence.constructed {
		return rawSubjectAltNameValue{}, certificateIdentityError("subjectAltName must be a constructed SEQUENCE")
	}

	var name rawSubjectAltNameValue
	remaining := sequence.value
	count := 0
	for len(remaining) > 0 {
		element, next, parseErr := parseDERElement(remaining)
		if parseErr != nil {
			return rawSubjectAltNameValue{}, certificateIdentityError("malformed GeneralName: %v", parseErr)
		}
		remaining = next
		count++
		if count != 1 {
			return rawSubjectAltNameValue{}, certificateIdentityError("GeneralNames must contain exactly one value")
		}
		if element.class != asn1.ClassContextSpecific || element.constructed {
			return rawSubjectAltNameValue{}, certificateIdentityError("GeneralName must be a primitive context-specific value")
		}
		if element.tag != 2 && element.tag != 6 {
			return rawSubjectAltNameValue{}, certificateIdentityError("unsupported GeneralName tag %d", element.tag)
		}
		if len(element.value) == 0 {
			return rawSubjectAltNameValue{}, certificateIdentityError("GeneralName value must be non-empty")
		}
		for _, value := range element.value {
			if value > 0x7f {
				return rawSubjectAltNameValue{}, certificateIdentityError("GeneralName value is not IA5")
			}
		}
		name = rawSubjectAltNameValue{tag: element.tag, value: element.value}
	}
	if count != 1 {
		return rawSubjectAltNameValue{}, certificateIdentityError("GeneralNames must contain exactly one value")
	}
	return name, nil
}

func parseDERElement(input []byte) (derElement, []byte, error) {
	if len(input) < 2 {
		return derElement{}, nil, errors.New("truncated DER header")
	}
	tagByte := input[0]
	element := derElement{
		class:       int(tagByte >> 6),
		constructed: tagByte&0x20 != 0,
	}
	offset := 1
	element.tag = uint64(tagByte & 0x1f)
	if element.tag == 0x1f {
		element.tag = 0
		for {
			if offset >= len(input) {
				return derElement{}, nil, errors.New("truncated high-tag number")
			}
			value := input[offset]
			offset++
			if element.tag > (^uint64(0) >> 7) {
				return derElement{}, nil, errors.New("high-tag number overflows")
			}
			element.tag = (element.tag << 7) | uint64(value&0x7f)
			if value&0x80 == 0 {
				break
			}
		}
		if element.tag < 31 {
			return derElement{}, nil, errors.New("non-minimal high-tag number")
		}
	}
	if offset >= len(input) {
		return derElement{}, nil, errors.New("truncated DER length")
	}

	lengthByte := input[offset]
	offset++
	var length uint64
	if lengthByte&0x80 == 0 {
		length = uint64(lengthByte)
	} else {
		byteCount := int(lengthByte & 0x7f)
		if byteCount == 0 {
			return derElement{}, nil, errors.New("indefinite DER length")
		}
		if byteCount > 8 || byteCount > len(input)-offset {
			return derElement{}, nil, errors.New("truncated DER length")
		}
		if input[offset] == 0 {
			return derElement{}, nil, errors.New("non-minimal DER length")
		}
		for _, value := range input[offset : offset+byteCount] {
			if length > (^uint64(0) >> 8) {
				return derElement{}, nil, errors.New("DER length overflows")
			}
			length = (length << 8) | uint64(value)
		}
		if length < 128 {
			return derElement{}, nil, errors.New("long-form DER length is not minimal")
		}
		offset += byteCount
	}
	if length > uint64(len(input)-offset) {
		return derElement{}, nil, errors.New("DER value is truncated")
	}
	end := offset + int(length)
	return derElement{
		class:       element.class,
		tag:         element.tag,
		constructed: element.constructed,
		value:       input[offset:end],
	}, input[end:], nil
}

func certificateIdentityError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCertificateIdentityAmbiguous, fmt.Sprintf(format, args...))
}

func canonicalPeerIdentity(value string) (string, error) {
	if strings.TrimSpace(value) != value || value == "" {
		return "", fmt.Errorf("%w: identity must be non-empty and already trimmed", ErrPeerIdentityNotAllowed)
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", fmt.Errorf("%w: identity contains whitespace/control characters", ErrPeerIdentityNotAllowed)
		}
	}
	if isIPLiteral(value) {
		return "", fmt.Errorf("%w: IP literal identities are unsupported", ErrPeerIdentityNotAllowed)
	}
	if strings.Contains(value, ":") {
		return canonicalURIIdentity(value)
	}
	return canonicalDNSIdentity(value)
}

func canonicalURIIdentity(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || !parsed.IsAbs() || parsed.String() != value {
		return "", fmt.Errorf("%w: invalid URI SAN %q", ErrPeerIdentityNotAllowed, value)
	}
	return value, nil
}

func canonicalDNSIdentity(value string) (string, error) {
	if strings.TrimSpace(value) != value || value == "" {
		return "", fmt.Errorf("%w: invalid DNS identity %q", ErrPeerIdentityNotAllowed, value)
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", fmt.Errorf("%w: invalid DNS identity %q", ErrPeerIdentityNotAllowed, value)
		}
	}
	if isIPLiteral(value) {
		return "", fmt.Errorf("%w: IP literal identities are unsupported", ErrPeerIdentityNotAllowed)
	}
	lower := strings.ToLower(value)
	if len(lower) > 253 || strings.HasPrefix(lower, ".") || strings.HasSuffix(lower, ".") ||
		strings.Contains(lower, "..") || strings.Contains(lower, "*") {
		return "", fmt.Errorf("%w: invalid DNS identity %q", ErrPeerIdentityNotAllowed, value)
	}
	for _, label := range strings.Split(lower, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("%w: invalid DNS identity %q", ErrPeerIdentityNotAllowed, value)
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return "", fmt.Errorf("%w: invalid DNS identity %q", ErrPeerIdentityNotAllowed, value)
			}
		}
	}
	return lower, nil
}

func isIPLiteral(value string) bool {
	candidate := value
	if strings.HasPrefix(candidate, "[") && strings.HasSuffix(candidate, "]") {
		candidate = candidate[1 : len(candidate)-1]
	}
	return net.ParseIP(candidate) != nil
}

func normalizeAllowedClientIdentities(values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: at least one allowed client identity is required", ErrPeerIdentityNotAllowed)
	}
	allowed := make(map[string]struct{}, len(values))
	for _, value := range values {
		identity, err := canonicalPeerIdentity(value)
		if err != nil {
			return nil, err
		}
		if _, exists := allowed[identity]; exists {
			return nil, fmt.Errorf("%w: duplicate identity %q", ErrPeerIdentityNotAllowed, identity)
		}
		allowed[identity] = struct{}{}
	}
	return allowed, nil
}

// ValidateAllowedClientIdentities validates a construction-time mTLS
// allowlist and returns its canonical set. The returned map is owned by the
// caller and is safe to mutate after validation.
func ValidateAllowedClientIdentities(values []string) (map[string]struct{}, error) {
	allowed, err := normalizeAllowedClientIdentities(values)
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(allowed))
	for identity := range allowed {
		result[identity] = struct{}{}
	}
	return result, nil
}
