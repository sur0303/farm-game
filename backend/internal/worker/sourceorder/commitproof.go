package sourceorder

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"farmgame/internal/holderkey"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

const CommitProofSchemaVersion uint32 = 2

var (
	ErrCommitProofInvalid           = errors.New("sourceorder: invalid commit proof")
	ErrCommitProofMissing           = errors.New("sourceorder: commit proof missing")
	ErrCommitProofSignature         = errors.New("sourceorder: commit proof signature invalid")
	ErrCommitProofStale             = errors.New("sourceorder: commit proof stale")
	ErrCommitProofRoute             = errors.New("sourceorder: commit proof source route invalid")
	ErrCommitProofBypassUnavailable = errors.New("sourceorder: commit proof Redis bypass unavailable")
)

// CommitProofStatement is the smallest source-actor statement needed before a
// destination could consider avoiding a SourceOrder read. It binds the exact
// source bucket and actor, payload, operation, source record version and the
// complete source-holder generation/lease fence at the atomic actor commit.
//
// The statement is intentionally not authority by itself. A destination must
// reconstruct CommitProofExpected from a validated Participant call, resolve
// KeyID under the exact holder/boot identity, and verify the current source
// route generation. Only explicit prefer mode may then bypass source Redis.
type CommitProofStatement struct {
	Work                    workidentity.WorkAddress
	WorkVersion             uint64
	OperationID             string
	SubjectPlayerID         uint64
	PayloadHash             [sha256.Size]byte
	DueAtMs                 int64
	SourceRealm             string
	AuthorityLayoutVersion  string
	SourceHolderID          string
	SourceHolderBootID      string
	AssignmentRevision      uint64
	WriterEpoch             uint64
	LeaseEpoch              uint64
	LeaseUntilMs            int64
	SourceRecordVersion     uint64
	CommittedAtMs           int64
	ExpiresAtMs             int64
	DestinationRouteDomain  workidentity.RouteDomain
	DestinationActorID      uint64
	DestinationStep         string
	DestinationInvocationID string
}

// CommitProofExpected contains only values independently reconstructed by the
// destination from its validated Participant call and configured realm. Source
// holder/fence/time values are signed attestations and are separately checked
// against the local current source route and bounded keyring.
type CommitProofExpected struct {
	Work                    workidentity.WorkAddress
	WorkVersion             uint64
	OperationID             string
	SubjectPlayerID         uint64
	PayloadHash             [sha256.Size]byte
	SourceRealm             string
	AuthorityLayoutVersion  string
	DestinationRouteDomain  workidentity.RouteDomain
	DestinationActorID      uint64
	DestinationStep         string
	DestinationInvocationID string
}

// CommitProof carries a canonical Ed25519 signature. KeyID is signed and must
// be resolved under the exact holder/boot identity; a self-asserted public key
// is never part of the proof.
type CommitProof struct {
	Statement CommitProofStatement
	KeyID     string
	Signature []byte
}

type CommitProofSigner struct {
	realm    string
	holderID string
	bootID   string
	keyID    string
	private  ed25519.PrivateKey
}

type CommitProofVerifier struct {
	realm    string
	holderID string
	bootID   string
	keyID    string
	public   ed25519.PublicKey
}

// String and GoString prevent generic structured/debug logging from rendering
// the signer's unexported private byte slice. Identity and key ID are available
// separately where public diagnostics are explicitly needed.
func (*CommitProofSigner) String() string   { return "sourceorder.CommitProofSigner{private=REDACTED}" }
func (*CommitProofSigner) GoString() string { return "sourceorder.CommitProofSigner{private=REDACTED}" }

func NewCommitProofSigner(realm, holderID, bootID, keyID string, private ed25519.PrivateKey) (*CommitProofSigner, error) {
	if !validProofToken(realm, 128) || !validProofToken(holderID, 128) || !validProofToken(bootID, 128) || !validProofToken(keyID, 128) || len(private) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: signer identity/key", ErrCommitProofInvalid)
	}
	return &CommitProofSigner{realm: realm, holderID: holderID, bootID: bootID, keyID: keyID, private: append(ed25519.PrivateKey(nil), private...)}, nil
}

// NewEphemeralHolderBootSigner generates one non-exportable-by-API Ed25519
// private key and binds it to the exact process holder/boot identity. Only the
// returned public Publication may cross HolderControl or RouteSnapshot.
func NewEphemeralHolderBootSigner(realm, holderID, bootID string) (*CommitProofSigner, holderkey.Publication, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, holderkey.Publication{}, fmt.Errorf("%w: generate holder boot key", ErrCommitProofInvalid)
	}
	publication, err := holderkey.New(public)
	if err != nil {
		return nil, holderkey.Publication{}, fmt.Errorf("%w: public key publication", ErrCommitProofInvalid)
	}
	signer, err := NewCommitProofSigner(realm, holderID, bootID, publication.KeyID, private)
	if err != nil {
		return nil, holderkey.Publication{}, err
	}
	return signer, publication, nil
}

func (s *CommitProofSigner) MatchesPublication(publication holderkey.Publication) bool {
	if s == nil || len(s.private) != ed25519.PrivateKeySize || publication.Validate() != nil || s.keyID != publication.KeyID {
		return false
	}
	public, ok := s.private.Public().(ed25519.PublicKey)
	if !ok {
		return false
	}
	return publication.Equal(holderkey.Publication{KeyID: publication.KeyID, PublicKey: public})
}

func (s *CommitProofSigner) SourceIdentity() (realm, holderID, bootID string) {
	if s == nil {
		return "", "", ""
	}
	return s.realm, s.holderID, s.bootID
}

func NewCommitProofVerifier(realm, holderID, bootID, keyID string, public ed25519.PublicKey) (*CommitProofVerifier, error) {
	if !validProofToken(realm, 128) || !validProofToken(holderID, 128) || !validProofToken(bootID, 128) || !validProofToken(keyID, 128) || len(public) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: verifier identity/key", ErrCommitProofInvalid)
	}
	return &CommitProofVerifier{realm: realm, holderID: holderID, bootID: bootID, keyID: keyID, public: append(ed25519.PublicKey(nil), public...)}, nil
}

func (s *CommitProofSigner) Sign(statement CommitProofStatement) (CommitProof, error) {
	if s == nil || len(s.private) != ed25519.PrivateKeySize || statement.SourceRealm != s.realm || statement.SourceHolderID != s.holderID || statement.SourceHolderBootID != s.bootID {
		return CommitProof{}, fmt.Errorf("%w: signer/source holder mismatch", ErrCommitProofInvalid)
	}
	canonical, err := CanonicalCommitProofBytes(statement, s.keyID)
	if err != nil {
		return CommitProof{}, err
	}
	return CommitProof{Statement: statement, KeyID: s.keyID, Signature: ed25519.Sign(s.private, canonical)}, nil
}

// Verify requires an independently constructed expected statement. It rejects
// a valid signature over any different work/source/fence scalar and never
// treats the proof's self-asserted holder or KeyID as trust configuration.
func (v *CommitProofVerifier) Verify(proof CommitProof, expected CommitProofStatement) error {
	if v == nil || len(v.public) != ed25519.PublicKeySize || proof.KeyID != v.keyID ||
		proof.Statement.SourceRealm != v.realm || proof.Statement.SourceHolderID != v.holderID || proof.Statement.SourceHolderBootID != v.bootID ||
		proof.Statement != expected || len(proof.Signature) != ed25519.SignatureSize {
		return ErrCommitProofSignature
	}
	canonical, err := CanonicalCommitProofBytes(proof.Statement, proof.KeyID)
	if err != nil {
		return err
	}
	if !ed25519.Verify(v.public, canonical, proof.Signature) {
		return ErrCommitProofSignature
	}
	return nil
}

// CanonicalCommitProofBytes is a versioned, length-framed preimage. Integer
// fields use fixed-width big-endian encoding and every text field is framed,
// so concatenation and field-boundary ambiguities are impossible.
func CanonicalCommitProofBytes(statement CommitProofStatement, keyID string) ([]byte, error) {
	if err := validateCommitProofStatement(statement); err != nil {
		return nil, err
	}
	if !validProofToken(keyID, 128) {
		return nil, fmt.Errorf("%w: key id", ErrCommitProofInvalid)
	}
	var out bytes.Buffer
	writeUint32(&out, CommitProofSchemaVersion)
	writeLP(&out, []byte("SOURCE_ORDER_COMMIT_PROOF"))
	writeLP(&out, []byte(keyID))
	writeLP(&out, []byte(statement.Work.WorkID))
	writeLP(&out, []byte(statement.Work.WorkType))
	writeUint32(&out, uint32(statement.Work.PlacementKind))
	writeUint32(&out, statement.Work.BucketVersion)
	writeUint32(&out, statement.Work.BucketID)
	writeUint32(&out, uint32(statement.Work.SourceRouteDomain))
	writeUint64(&out, statement.Work.SourceActorID)
	writeUint64(&out, statement.WorkVersion)
	writeLP(&out, []byte(statement.OperationID))
	writeUint64(&out, statement.SubjectPlayerID)
	writeLP(&out, statement.PayloadHash[:])
	writeInt64(&out, statement.DueAtMs)
	writeLP(&out, []byte(statement.SourceRealm))
	writeLP(&out, []byte(statement.AuthorityLayoutVersion))
	writeLP(&out, []byte(statement.SourceHolderID))
	writeLP(&out, []byte(statement.SourceHolderBootID))
	writeUint64(&out, statement.AssignmentRevision)
	writeUint64(&out, statement.WriterEpoch)
	writeUint64(&out, statement.LeaseEpoch)
	writeInt64(&out, statement.LeaseUntilMs)
	writeUint64(&out, statement.SourceRecordVersion)
	writeInt64(&out, statement.CommittedAtMs)
	writeInt64(&out, statement.ExpiresAtMs)
	writeUint32(&out, uint32(statement.DestinationRouteDomain))
	writeUint64(&out, statement.DestinationActorID)
	writeLP(&out, []byte(statement.DestinationStep))
	writeLP(&out, []byte(statement.DestinationInvocationID))
	return out.Bytes(), nil
}

func validateCommitProofStatement(statement CommitProofStatement) error {
	if err := AcceptsDirect(statement.Work); err != nil {
		return fmt.Errorf("%w: work address: %v", ErrCommitProofInvalid, err)
	}
	if statement.Work.SourceRouteDomain != workidentity.RouteDomainFarm ||
		(statement.Work.WorkType != string(identity.WorkTypeCareReward) && statement.Work.WorkType != string(identity.WorkTypeHarvest)) ||
		statement.DestinationRouteDomain != workidentity.RouteDomainPlayer ||
		statement.DestinationActorID != statement.SubjectPlayerID {
		return fmt.Errorf("%w: CARE/HARVEST route binding", ErrCommitProofInvalid)
	}
	if (statement.Work.WorkType == string(identity.WorkTypeCareReward) && statement.DestinationStep != string(identity.StepApplyCareReward)) ||
		(statement.Work.WorkType == string(identity.WorkTypeHarvest) && statement.DestinationStep != string(identity.StepGrantHarvestReward)) {
		return fmt.Errorf("%w: CARE/HARVEST step binding", ErrCommitProofInvalid)
	}
	if statement.WorkVersion != 1 || !validProofToken(statement.OperationID, 128) || statement.SubjectPlayerID == 0 ||
		statement.PayloadHash == ([sha256.Size]byte{}) || statement.DueAtMs <= 0 ||
		!validProofToken(statement.SourceRealm, 128) || !validProofToken(statement.AuthorityLayoutVersion, 64) ||
		!validProofToken(statement.SourceHolderID, 128) || !validProofToken(statement.SourceHolderBootID, 128) ||
		statement.AssignmentRevision == 0 || statement.WriterEpoch == 0 || statement.LeaseEpoch == 0 ||
		statement.SourceRecordVersion == 0 || statement.CommittedAtMs <= 0 || statement.ExpiresAtMs <= statement.CommittedAtMs ||
		statement.LeaseUntilMs < statement.ExpiresAtMs || statement.DestinationActorID == 0 ||
		!validProofToken(statement.DestinationStep, 64) ||
		!validProofToken(statement.DestinationInvocationID, 128) {
		return fmt.Errorf("%w: statement fields", ErrCommitProofInvalid)
	}
	return nil
}

func validProofText(value string, max int) bool {
	return value != "" && len(value) <= max && utf8.ValidString(value) && strings.TrimSpace(value) == value
}

func validProofToken(value string, max int) bool {
	if !validProofText(value, max) {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func writeLP(out *bytes.Buffer, value []byte) {
	writeUint32(out, uint32(len(value)))
	_, _ = out.Write(value)
}

func writeUint32(out *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = out.Write(encoded[:])
}

func writeUint64(out *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = out.Write(encoded[:])
}

func writeInt64(out *bytes.Buffer, value int64) { writeUint64(out, uint64(value)) }
