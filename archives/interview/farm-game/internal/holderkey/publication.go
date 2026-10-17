// Package holderkey defines the public, boot-scoped key publication shared by
// HolderControl, verified holder discovery, and RouteSnapshot. It contains no
// private-key serialization or configuration surface.
package holderkey

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	MaxKeyIDBytes       = 96
	EncodedPublicKeyLen = 44
	keyIDPrefix         = "ed25519-sha256-"
)

var ErrInvalidPublication = errors.New("holderkey: invalid public key publication")

// Publication is the sole key material allowed to cross a holder-process
// boundary. PublicKey is copied by constructors and accessors.
type Publication struct {
	KeyID     string
	PublicKey ed25519.PublicKey
}

// New derives the immutable key ID from the Ed25519 public key. A key ID is
// therefore not an operator-selected alias that could be rebound to another
// key during a holder boot.
func New(public ed25519.PublicKey) (Publication, error) {
	if len(public) != ed25519.PublicKeySize {
		return Publication{}, ErrInvalidPublication
	}
	digest := sha256.Sum256(public)
	publication := Publication{
		KeyID:     keyIDPrefix + hex.EncodeToString(digest[:]),
		PublicKey: append(ed25519.PublicKey(nil), public...),
	}
	return publication, publication.Validate()
}

// FromEncoded parses the canonical RouteSnapshot/JSON representation.
func FromEncoded(keyID, encoded string) (Publication, error) {
	if len(encoded) != EncodedPublicKeyLen {
		return Publication{}, ErrInvalidPublication
	}
	public, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(public) != ed25519.PublicKeySize {
		return Publication{}, ErrInvalidPublication
	}
	publication := Publication{KeyID: keyID, PublicKey: ed25519.PublicKey(public)}
	if err := publication.Validate(); err != nil {
		return Publication{}, err
	}
	publication.PublicKey = append(ed25519.PublicKey(nil), publication.PublicKey...)
	return publication, nil
}

func (p Publication) Validate() error {
	if len(p.PublicKey) != ed25519.PublicKeySize || len(p.KeyID) > MaxKeyIDBytes ||
		!strings.HasPrefix(p.KeyID, keyIDPrefix) {
		return ErrInvalidPublication
	}
	digest := sha256.Sum256(p.PublicKey)
	if p.KeyID != keyIDPrefix+hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%w: key ID does not bind public key", ErrInvalidPublication)
	}
	return nil
}

func (p Publication) EncodedPublicKey() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(p.PublicKey), nil
}

func (p Publication) Clone() Publication {
	p.PublicKey = append(ed25519.PublicKey(nil), p.PublicKey...)
	return p
}

func (p Publication) Equal(other Publication) bool {
	if p.KeyID != other.KeyID || len(p.PublicKey) != len(other.PublicKey) {
		return false
	}
	for index := range p.PublicKey {
		if p.PublicKey[index] != other.PublicKey[index] {
			return false
		}
	}
	return true
}
