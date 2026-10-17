package routecontrol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type CertificateFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
	DNSName  string
	Fact     CertificateFact
}

type CertificateFact struct {
	TrustDomain      string `json:"trust_domain"`
	Usage            string `json:"usage"`
	DNSName          string `json:"dns_name"`
	Serial           string `json:"serial"`
	Issuer           string `json:"issuer"`
	NotBefore        string `json:"not_before"`
	NotAfter         string `json:"not_after"`
	CertificateDER   string `json:"certificate_der_sha256"`
	PublicKeySHA256  string `json:"public_key_sha256"`
	CertificateCount int    `json:"certificate_count"`
}

type TrustDomainSpec struct {
	Name    string
	Servers []string
	Clients []string
}

type TrustBundle struct {
	Root    string
	Domains map[string]map[string]CertificateFiles
	CAs     map[string]string
}

// GenerateTrustBundle creates independent ephemeral CAs and single-DNS-SAN
// leaves below a private root. Private material is returned only as paths and
// is never represented by CertificateFact.
func GenerateTrustBundle(root, challenge string, specs []TrustDomainSpec, now time.Time) (*TrustBundle, error) {
	if strings.TrimSpace(root) == "" || !validChallenge(challenge) || len(specs) == 0 {
		return nil, fmt.Errorf("%w: invalid trust bundle options", ErrInvalidEvidence)
	}
	if now.IsZero() {
		now = time.Now()
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, err
	}
	bundle := &TrustBundle{Root: root, Domains: make(map[string]map[string]CertificateFiles), CAs: make(map[string]string)}
	seenDomains := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		if !safeToken(spec.Name) {
			return nil, fmt.Errorf("%w: invalid trust domain", ErrInvalidEvidence)
		}
		if _, duplicate := seenDomains[spec.Name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate trust domain", ErrInvalidEvidence)
		}
		seenDomains[spec.Name] = struct{}{}
		dir := filepath.Join(root, spec.Name)
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		caCert, caKey, caDER, err := newCertificateAuthority(spec.Name+"-"+challenge, now)
		if err != nil {
			return nil, err
		}
		caPath := filepath.Join(dir, "ca.crt")
		if err := writePrivateFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})); err != nil {
			return nil, err
		}
		bundle.CAs[spec.Name] = caPath
		bundle.Domains[spec.Name] = make(map[string]CertificateFiles)
		leaves := make([]struct {
			name  string
			usage string
		}, 0, len(spec.Servers)+len(spec.Clients))
		for _, name := range spec.Servers {
			leaves = append(leaves, struct{ name, usage string }{name, "server"})
		}
		for _, name := range spec.Clients {
			leaves = append(leaves, struct{ name, usage string }{name, "client"})
		}
		for _, leaf := range leaves {
			if err := validateTrustDNSName(leaf.name, challenge); err != nil {
				return nil, err
			}
			if _, duplicate := bundle.Domains[spec.Name][leaf.name]; duplicate {
				return nil, fmt.Errorf("%w: duplicate certificate identity", ErrInvalidEvidence)
			}
			certDER, key, certificate, err := newLeafCertificate(caCert, caKey, leaf.name, leaf.usage, now)
			if err != nil {
				return nil, err
			}
			keyDER, err := x509.MarshalPKCS8PrivateKey(key)
			if err != nil {
				return nil, err
			}
			base := safeCertificateFilename(leaf.name)
			certPath := filepath.Join(dir, base+".crt")
			keyPath := filepath.Join(dir, base+".key")
			if err := writePrivateFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})); err != nil {
				return nil, err
			}
			if err := writePrivateFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})); err != nil {
				return nil, err
			}
			publicDER, err := x509.MarshalPKIXPublicKey(certificate.PublicKey)
			if err != nil {
				return nil, err
			}
			certDigest := sha256.Sum256(certDER)
			publicDigest := sha256.Sum256(publicDER)
			bundle.Domains[spec.Name][leaf.name] = CertificateFiles{
				CertFile: certPath, KeyFile: keyPath, CAFile: caPath, DNSName: leaf.name,
				Fact: CertificateFact{
					TrustDomain: spec.Name, Usage: leaf.usage, DNSName: leaf.name,
					Serial: certificate.SerialNumber.Text(16), Issuer: certificate.Issuer.CommonName,
					NotBefore:      certificate.NotBefore.UTC().Format(time.RFC3339Nano),
					NotAfter:       certificate.NotAfter.UTC().Format(time.RFC3339Nano),
					CertificateDER: hex.EncodeToString(certDigest[:]), PublicKeySHA256: hex.EncodeToString(publicDigest[:]),
					CertificateCount: 1,
				},
			}
		}
	}
	return bundle, nil
}

func (b *TrustBundle) Leaf(domain, dnsName string) (CertificateFiles, error) {
	if b == nil || b.Domains[domain] == nil {
		return CertificateFiles{}, errors.New("trust domain is unavailable")
	}
	leaf, ok := b.Domains[domain][dnsName]
	if !ok {
		return CertificateFiles{}, errors.New("certificate identity is unavailable")
	}
	return leaf, nil
}

func (b *TrustBundle) Facts() []CertificateFact {
	if b == nil {
		return nil
	}
	var facts []CertificateFact
	for _, leaves := range b.Domains {
		for _, leaf := range leaves {
			facts = append(facts, leaf.Fact)
		}
	}
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].TrustDomain == facts[j].TrustDomain {
			return facts[i].DNSName < facts[j].DNSName
		}
		return facts[i].TrustDomain < facts[j].TrustDomain
	})
	return facts
}

func newCertificateAuthority(commonName string, now time.Time) (*x509.Certificate, ed25519.PrivateKey, []byte, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: now.Add(-time.Minute).UTC(), NotAfter: now.Add(24 * time.Hour).UTC(),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:           x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		SignatureAlgorithm: x509.PureEd25519,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return nil, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	return certificate, private, der, err
}

func newLeafCertificate(ca *x509.Certificate, caKey ed25519.PrivateKey, dnsName, usage string, now time.Time) ([]byte, ed25519.PrivateKey, *x509.Certificate, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, nil, err
	}
	extended := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if usage == "server" {
		extended = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{}, DNSNames: []string{dnsName},
		NotBefore: now.Add(-time.Minute).UTC(), NotAfter: now.Add(12 * time.Hour).UTC(),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: extended,
		BasicConstraintsValid: true, SignatureAlgorithm: x509.PureEd25519,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	certificate, err := x509.ParseCertificate(der)
	return der, private, certificate, err
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil || serial.Sign() == 0 {
		return nil, errors.New("generate non-zero certificate serial")
	}
	return serial, nil
}

func validateTrustDNSName(name, challenge string) error {
	if name == "" || !strings.Contains(name, challenge) || name != strings.ToLower(name) || len(name) > 253 || strings.ContainsAny(name, ":/\\*_ ") {
		return fmt.Errorf("%w: invalid challenge-bound DNS identity", ErrInvalidEvidence)
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("%w: invalid DNS label", ErrInvalidEvidence)
		}
		for _, value := range label {
			if value != '-' && (value < 'a' || value > 'z') && (value < '0' || value > '9') {
				return fmt.Errorf("%w: invalid DNS character", ErrInvalidEvidence)
			}
		}
	}
	return nil
}

func safeCertificateFilename(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func writePrivateFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || written != len(data) {
		return errors.Join(writeErr, ioErrShortWrite(written, len(data)), syncErr, closeErr)
	}
	return errors.Join(syncErr, closeErr)
}

func ioErrShortWrite(written, expected int) error {
	if written == expected {
		return nil
	}
	return fmt.Errorf("short write %d/%d", written, expected)
}
