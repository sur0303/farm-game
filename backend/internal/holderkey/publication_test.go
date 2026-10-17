package holderkey

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestPublicationCanonicalRoundTripAndBinding(t *testing.T) {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := New(public)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := publication.EncodedPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := FromEncoded(publication.KeyID, encoded)
	if err != nil || !decoded.Equal(publication) {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FromEncoded(publication.KeyID, mustEncoded(t, other)); err == nil {
		t.Fatal("key ID was rebound to a different public key")
	}
}

func mustEncoded(t *testing.T, public ed25519.PublicKey) string {
	t.Helper()
	publication, err := New(public)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := publication.EncodedPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
