package transport

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net/url"
	"testing"
)

func TestCertificateIdentityRejectsUnsupportedAndMalformedRawSAN(t *testing.T) {
	validDNS := &x509.Certificate{DNSNames: []string{"client.test"}}
	validURI := &x509.Certificate{URIs: []*url.URL{{Scheme: "spiffe", Host: "farm.test", Path: "/client"}}}
	tests := []struct {
		name string
		cert *x509.Certificate
	}{
		{
			name: "otherName",
			cert: &x509.Certificate{
				DNSNames:   []string{"client.test"},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(0, []byte{1}, false))},
			},
		},
		{
			name: "directoryName",
			cert: &x509.Certificate{
				DNSNames:   []string{"client.test"},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(4, []byte{1}, false))},
			},
		},
		{
			name: "registeredID",
			cert: &x509.Certificate{
				DNSNames:   []string{"client.test"},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(8, []byte{0x2a, 0x03, 0x04}, false))},
			},
		},
		{
			name: "mixed",
			cert: &x509.Certificate{
				DNSNames: []string{"client.test"},
				URIs:     validURI.URIs,
				Extensions: []pkix.Extension{testSANExtension(
					testDERContextName(2, []byte("client.test"), false),
					testDERContextName(6, []byte("spiffe://farm.test/client"), false),
				)},
			},
		},
		{
			name: "constructed DNS",
			cert: &x509.Certificate{
				DNSNames:   []string{"client.test"},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(2, []byte("client.test"), true))},
			},
		},
		{
			name: "empty DNS",
			cert: &x509.Certificate{
				DNSNames:   []string{""},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(2, nil, false))},
			},
		},
		{
			name: "duplicate SAN extension",
			cert: &x509.Certificate{
				DNSNames: []string{"client.test"},
				Extensions: []pkix.Extension{
					testSANExtension(testDERContextName(2, []byte("client.test"), false)),
					testSANExtension(testDERContextName(2, []byte("client.test"), false)),
				},
			},
		},
		{
			name: "trailing extension bytes",
			cert: &x509.Certificate{
				DNSNames: []string{"client.test"},
				Extensions: []pkix.Extension{{
					Id:    subjectAltNameOID,
					Value: append(testDERSequence(testDERContextName(2, []byte("client.test"), false)), 0),
				}},
			},
		},
		{
			name: "truncated GeneralName",
			cert: &x509.Certificate{
				DNSNames: []string{"client.test"},
				Extensions: []pkix.Extension{{
					Id:    subjectAltNameOID,
					Value: []byte{0x30, 0x03, 0x82, 0x02, 'a'},
				}},
			},
		},
		{
			name: "non-minimal length",
			cert: &x509.Certificate{
				DNSNames: []string{"client.test"},
				Extensions: []pkix.Extension{{
					Id:    subjectAltNameOID,
					Value: []byte{0x30, 0x81, 0x02, 0x82, 0x00},
				}},
			},
		},
		{
			name: "raw value mismatch",
			cert: &x509.Certificate{
				DNSNames:   []string{"client.test"},
				Extensions: []pkix.Extension{testSANExtension(testDERContextName(2, []byte("other.test"), false))},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := certificateIdentity(test.cert); !errors.Is(err, ErrCertificateIdentityAmbiguous) {
				t.Fatalf("identity error=%v want raw SAN rejection", err)
			}
		})
	}

	if identity, err := certificateIdentity(testCertificateWithSAN(validDNS)); err != nil || identity != "client.test" {
		t.Fatalf("valid DNS identity=%q err=%v", identity, err)
	}
	if identity, err := certificateIdentity(testCertificateWithSAN(validURI)); err != nil || identity != "spiffe://farm.test/client" {
		t.Fatalf("valid URI identity=%q err=%v", identity, err)
	}
}

func TestParseDERElementRejectsHighTagLengthBoundary(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{name: "high tag ends at input end", input: []byte{0x9f, 0x1f}},
		{name: "continued high tag ends at input end", input: []byte{0x9f, 0x81}},
		{name: "continued high tag then length ends at input end", input: []byte{0x9f, 0x81, 0x00}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("parseDERElement panicked: %v", recovered)
				}
			}()
			if _, _, err := parseDERElement(test.input); err == nil {
				t.Fatal("malformed high-tag DER unexpectedly parsed")
			}
		})
	}
}

func FuzzParseDERElementHighTagNeverPanics(f *testing.F) {
	for _, seed := range [][]byte{
		{0x9f, 0x1f},
		{0x9f, 0x1f, 0x00},
		{0x9f, 0x81},
		{0x9f, 0x81, 0x00},
		{0x30, 0x03, 0x9f, 0x1f, 0x00},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("parseDERElement panicked for %x: %v", input, recovered)
			}
		}()
		_, _, _ = parseDERElement(input)
	})
}

func testCertificateWithSAN(certificate *x509.Certificate) *x509.Certificate {
	if certificate == nil {
		return nil
	}
	clone := *certificate
	if len(certificate.Extensions) != 0 {
		return &clone
	}
	var names [][]byte
	for _, value := range certificate.EmailAddresses {
		names = append(names, testDERContextName(1, []byte(value), false))
	}
	for _, value := range certificate.DNSNames {
		names = append(names, testDERContextName(2, []byte(value), false))
	}
	for _, value := range certificate.IPAddresses {
		names = append(names, testDERContextName(7, []byte(value), false))
	}
	for _, value := range certificate.URIs {
		if value == nil {
			names = append(names, testDERContextName(6, nil, false))
			continue
		}
		names = append(names, testDERContextName(6, []byte(value.String()), false))
	}
	if len(names) == 0 {
		return &clone
	}
	clone.Extensions = []pkix.Extension{testSANExtension(names...)}
	return &clone
}

func testSANExtension(names ...[]byte) pkix.Extension {
	return pkix.Extension{Id: subjectAltNameOID, Value: testDERSequence(names...)}
}

func testDERSequence(elements ...[]byte) []byte {
	var content []byte
	for _, element := range elements {
		content = append(content, element...)
	}
	return testDERElement(0, 16, true, content)
}

func testDERContextName(tag uint64, value []byte, constructed bool) []byte {
	return testDERElement(2, tag, constructed, value)
}

func testDERElement(class int, tag uint64, constructed bool, value []byte) []byte {
	header := []byte{byte(class << 6)}
	if constructed {
		header[0] |= 0x20
	}
	if tag < 31 {
		header[0] |= byte(tag)
	} else {
		header[0] |= 0x1f
		var encoded []byte
		for tag > 0 {
			encoded = append([]byte{byte(tag & 0x7f)}, encoded...)
			tag >>= 7
		}
		for index := 0; index < len(encoded)-1; index++ {
			encoded[index] |= 0x80
		}
		header = append(header, encoded...)
	}
	if len(value) < 128 {
		return append(append(header, byte(len(value))), value...)
	}
	length := len(value)
	var encoded []byte
	for length > 0 {
		encoded = append([]byte{byte(length)}, encoded...)
		length >>= 8
	}
	header = append(header, byte(0x80|len(encoded)))
	header = append(header, encoded...)
	return append(header, value...)
}
