package sourceorder

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/workidentity"
)

const (
	MaxCommitProofVerificationKeys = 32
	// MaxCommitProofKeyringJSONBytes bounds parser work and every string
	// allocation before JSON decoding. It is large enough for 32 maximum-size
	// identity entries while remaining an operator-config, not a data channel.
	MaxCommitProofKeyringJSONBytes = 64 * 1024
)

type CommitProofVerificationKey struct {
	SourceRealm  string
	HolderID     string
	HolderBootID string
	KeyID        string
	PublicKey    ed25519.PublicKey
	NotBeforeMs  int64
	NotAfterMs   int64
}

type commitProofKeyIdentity struct {
	realm, holder, boot, key string
}

// CommitProofKeyring is immutable after construction and has a hard entry
// bound. Rotation is additive: install overlapping old/new KeyIDs with bounded
// validity windows, then remove the old entry after its last possible proof
// expiry.
type CommitProofKeyring struct {
	keys   map[commitProofKeyIdentity]CommitProofVerificationKey
	maxTTL time.Duration
}

func NewCommitProofKeyring(entries []CommitProofVerificationKey, maxTTL time.Duration) (*CommitProofKeyring, error) {
	if len(entries) == 0 || len(entries) > MaxCommitProofVerificationKeys || maxTTL < time.Millisecond ||
		maxTTL > time.Minute || maxTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: keyring size/ttl", ErrCommitProofInvalid)
	}
	keys := make(map[commitProofKeyIdentity]CommitProofVerificationKey, len(entries))
	for _, entry := range entries {
		if !validProofToken(entry.SourceRealm, 128) || !validProofToken(entry.HolderID, 128) ||
			!validProofToken(entry.HolderBootID, 128) || !validProofToken(entry.KeyID, 128) ||
			len(entry.PublicKey) != ed25519.PublicKeySize || entry.NotBeforeMs <= 0 || entry.NotAfterMs <= entry.NotBeforeMs {
			return nil, fmt.Errorf("%w: keyring entry", ErrCommitProofInvalid)
		}
		identity := commitProofKeyIdentity{entry.SourceRealm, entry.HolderID, entry.HolderBootID, entry.KeyID}
		if _, duplicate := keys[identity]; duplicate {
			return nil, fmt.Errorf("%w: duplicate keyring entry", ErrCommitProofInvalid)
		}
		entry.PublicKey = append(ed25519.PublicKey(nil), entry.PublicKey...)
		keys[identity] = entry
	}
	return &CommitProofKeyring{keys: keys, maxTTL: maxTTL}, nil
}

func (k *CommitProofKeyring) Verify(proof CommitProof, expected CommitProofExpected, now time.Time) error {
	if k == nil || len(k.keys) == 0 || now.IsZero() {
		return fmt.Errorf("%w: keyring unavailable", ErrCommitProofSignature)
	}
	statement := proof.Statement
	if !commitProofMatchesExpected(statement, expected) {
		return ErrCommitProofSignature
	}
	if err := k.verifyFreshness(statement, now); err != nil {
		return err
	}
	entry, ok := k.keys[commitProofKeyIdentity{statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, proof.KeyID}]
	if !ok || statement.CommittedAtMs < entry.NotBeforeMs || statement.ExpiresAtMs > entry.NotAfterMs {
		return ErrCommitProofSignature
	}
	verifier, err := NewCommitProofVerifier(entry.SourceRealm, entry.HolderID, entry.HolderBootID, entry.KeyID, entry.PublicKey)
	if err != nil {
		return err
	}
	return verifier.Verify(proof, statement)
}

func (k *CommitProofKeyring) verifyFreshness(statement CommitProofStatement, now time.Time) error {
	if k == nil {
		return ErrCommitProofStale
	}
	return verifyCommitProofFreshness(statement, now, k.maxTTL)
}

func commitProofMatchesExpected(statement CommitProofStatement, expected CommitProofExpected) bool {
	return statement.Work == expected.Work && statement.WorkVersion == expected.WorkVersion &&
		statement.OperationID == expected.OperationID && statement.SubjectPlayerID == expected.SubjectPlayerID &&
		statement.PayloadHash == expected.PayloadHash && statement.SourceRealm == expected.SourceRealm &&
		statement.AuthorityLayoutVersion == expected.AuthorityLayoutVersion &&
		statement.DestinationRouteDomain == expected.DestinationRouteDomain &&
		statement.DestinationActorID == expected.DestinationActorID && statement.DestinationStep == expected.DestinationStep &&
		statement.DestinationInvocationID == expected.DestinationInvocationID
}

func verifyCommitProofFreshness(statement CommitProofStatement, now time.Time, maxTTL time.Duration) error {
	if now.IsZero() || maxTTL < time.Millisecond || maxTTL > time.Minute {
		return ErrCommitProofStale
	}
	nowMs := now.UnixMilli()
	if nowMs < statement.CommittedAtMs || nowMs < statement.DueAtMs || nowMs >= statement.ExpiresAtMs ||
		statement.ExpiresAtMs-statement.CommittedAtMs > maxTTL.Milliseconds() || statement.LeaseUntilMs < statement.ExpiresAtMs {
		return ErrCommitProofStale
	}
	return nil
}

// CommitProofRouteVerifier proves the signed source generation against a
// current local route snapshot. It must not consult SourceOrder Redis.
type CommitProofRouteVerifier interface {
	VerifyCommitProofRoute(context.Context, CommitProofStatement) error
}

// CommitProofValidator is the fail-closed composition of cryptographic key
// lookup and current source-route verification. Keeping the checks together
// prevents a future caller from treating signature verification alone as
// authority. It does not decide whether Redis may be bypassed.
type CommitProofValidator struct {
	keyring      *CommitProofKeyring
	route        CommitProofRouteVerifier
	routeKeyring *RouteCommitProofKeyring
	now          func() time.Time
}

func NewCommitProofValidator(keyring *CommitProofKeyring, route CommitProofRouteVerifier, now func() time.Time) (*CommitProofValidator, error) {
	if keyring == nil || len(keyring.keys) == 0 || route == nil {
		return nil, fmt.Errorf("%w: validator configuration", ErrCommitProofInvalid)
	}
	if now == nil {
		now = time.Now
	}
	return &CommitProofValidator{keyring: keyring, route: route, now: now}, nil
}

// RouteCommitProofKeyring resolves the exact public key and source generation
// from one immutable RouteSnapshot table. Key install, rotation, revocation,
// and holder movement therefore have the same atomic visibility as routing.
// It contains no independent mutable key map.
type RouteCommitProofKeyring struct {
	routes CommitProofRouteLookup
	realm  string
	maxTTL time.Duration
}

func NewRouteCommitProofKeyring(routes CommitProofRouteLookup, realm string, maxTTL time.Duration) (*RouteCommitProofKeyring, error) {
	if routes == nil || !validProofToken(realm, 128) || maxTTL < time.Millisecond || maxTTL > time.Minute || maxTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("%w: route keyring configuration", ErrCommitProofInvalid)
	}
	return &RouteCommitProofKeyring{routes: routes, realm: realm, maxTTL: maxTTL}, nil
}

func NewRouteCommitProofValidator(routes CommitProofRouteLookup, realm string, maxTTL time.Duration, now func() time.Time) (*CommitProofValidator, error) {
	keyring, err := NewRouteCommitProofKeyring(routes, realm, maxTTL)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &CommitProofValidator{routeKeyring: keyring, now: now}, nil
}

func (k *RouteCommitProofKeyring) Verify(ctx context.Context, proof CommitProof, expected CommitProofExpected, now func() time.Time) error {
	if k == nil || k.routes == nil || ctx == nil || now == nil || proof.Statement.SourceRealm != k.realm ||
		!commitProofMatchesExpected(proof.Statement, expected) {
		return ErrCommitProofSignature
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verifyCommitProofFreshness(proof.Statement, now(), k.maxTTL); err != nil {
		return err
	}
	statement := proof.Statement
	if err := validateCommitProofStatement(statement); err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	domain, err := routeDomainToActor(statement.Work.SourceRouteDomain)
	if err != nil {
		return ErrCommitProofRoute
	}
	view, err := k.routes.LookupRoute(ctx, actorrouting.Address{Realm: k.realm, Kind: domain, ID: statement.Work.SourceActorID})
	if err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	if view.Realm != k.realm || view.Domain != domain || view.Shard != statement.Work.BucketID ||
		view.Route.State != actorrouting.ShardStateActive || view.Holder.State != actorrouting.HolderStateActive ||
		view.Holder.InstanceID != statement.SourceHolderID || view.Holder.BootID != statement.SourceHolderBootID ||
		view.Route.ShardRevision != statement.AssignmentRevision || view.Route.ExpectedWriterEpoch != statement.WriterEpoch ||
		view.Route.ExpectedLeaseEpoch != statement.LeaseEpoch || view.Route.LeaseUntilMs < statement.LeaseUntilMs ||
		view.Route.LeaseUntilMs < statement.ExpiresAtMs {
		return ErrCommitProofRoute
	}
	publication, found, err := view.Holder.CommitProofPublication()
	if err != nil || !found || proof.KeyID != publication.KeyID {
		return ErrCommitProofSignature
	}
	verifier, err := NewCommitProofVerifier(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID,
		publication.KeyID, publication.PublicKey)
	if err != nil {
		return err
	}
	if err := verifier.Verify(proof, statement); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return verifyCommitProofFreshness(statement, now(), k.maxTTL)
}

func (v *CommitProofValidator) Verify(ctx context.Context, proof CommitProof, expected CommitProofExpected) error {
	if v == nil || v.now == nil || ctx == nil {
		return fmt.Errorf("%w: validator unavailable", ErrCommitProofInvalid)
	}
	if v.routeKeyring != nil {
		return v.routeKeyring.Verify(ctx, proof, expected, v.now)
	}
	if v.keyring == nil || v.route == nil {
		return fmt.Errorf("%w: validator unavailable", ErrCommitProofInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := v.keyring.Verify(proof, expected, v.now()); err != nil {
		return err
	}
	if err := v.route.VerifyCommitProofRoute(ctx, proof.Statement); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Route resolution may block until the proof expires. Re-sample the clock
	// after the exact generation check so a proof cannot pass on stale
	// pre-resolution freshness.
	return v.keyring.verifyFreshness(proof.Statement, v.now())
}

type LocalCommitProofRouteVerifier struct {
	resolver actorrouting.Resolver
	realm    string
}

func NewLocalCommitProofRouteVerifier(resolver actorrouting.Resolver, realm string) (*LocalCommitProofRouteVerifier, error) {
	if resolver == nil || !validProofToken(realm, 128) {
		return nil, fmt.Errorf("%w: local route verifier", ErrCommitProofInvalid)
	}
	return &LocalCommitProofRouteVerifier{resolver: resolver, realm: realm}, nil
}

func (v *LocalCommitProofRouteVerifier) VerifyCommitProofRoute(ctx context.Context, statement CommitProofStatement) error {
	if v == nil || v.resolver == nil || ctx == nil || statement.SourceRealm != v.realm {
		return ErrCommitProofRoute
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCommitProofStatement(statement); err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	domain, err := routeDomainToActor(statement.Work.SourceRouteDomain)
	if err != nil {
		return ErrCommitProofRoute
	}
	assignment, err := v.resolver.Resolve(ctx, actorrouting.Address{Realm: v.realm, Kind: domain, ID: statement.Work.SourceActorID})
	if err != nil {
		return errors.Join(ErrCommitProofRoute, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if assignment.Kind != domain || assignment.HolderState != actorrouting.HolderStateActive || assignment.ShardState != actorrouting.ShardStateActive ||
		assignment.ShardID != statement.Work.BucketID || assignment.HolderPod != statement.SourceHolderID ||
		assignment.HolderBootID != statement.SourceHolderBootID || assignment.ShardRevision != statement.AssignmentRevision ||
		assignment.AssignmentRevision != statement.AssignmentRevision || assignment.WriterEpoch != statement.WriterEpoch ||
		assignment.LeaseEpoch != statement.LeaseEpoch || assignment.LeaseUntilMs < statement.LeaseUntilMs ||
		assignment.LeaseUntilMs < statement.ExpiresAtMs {
		return ErrCommitProofRoute
	}
	return nil
}

func routeDomainToActor(domain workidentity.RouteDomain) (actorrouting.RouteDomain, error) {
	switch domain {
	case workidentity.RouteDomainPlayer:
		return actorrouting.DomainPlayer, nil
	case workidentity.RouteDomainFarm:
		return actorrouting.DomainFarm, nil
	default:
		return "", ErrCommitProofRoute
	}
}

type commitProofKeyJSON struct {
	SourceRealm     string `json:"source_realm"`
	HolderID        string `json:"holder_id"`
	HolderBootID    string `json:"holder_boot_id"`
	KeyID           string `json:"key_id"`
	PublicKeyBase64 string `json:"public_key_base64"`
	NotBeforeMs     int64  `json:"not_before_ms"`
	NotAfterMs      int64  `json:"not_after_ms"`
}

// ParseCommitProofPublicKeysJSON parses the bounded operator-owned keyring.
// Unknown fields and trailing JSON are rejected so misspelled identity fields
// cannot silently produce a weaker trust entry.
func ParseCommitProofPublicKeysJSON(raw string, maxTTL time.Duration) (*CommitProofKeyring, error) {
	if len(raw) > MaxCommitProofKeyringJSONBytes {
		return nil, fmt.Errorf("%w: keyring json exceeds byte limit", ErrCommitProofInvalid)
	}
	if strings.TrimSpace(raw) == "" {
		return nil, ErrCommitProofMissing
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('[') {
		return nil, fmt.Errorf("%w: keyring json array", ErrCommitProofInvalid)
	}
	entries := make([]CommitProofVerificationKey, 0, MaxCommitProofVerificationKeys)
	for decoder.More() {
		// Check before decoding the next object. Decoding directly into a slice
		// would allocate from attacker-controlled array cardinality first.
		if len(entries) >= MaxCommitProofVerificationKeys {
			return nil, fmt.Errorf("%w: keyring entry limit", ErrCommitProofInvalid)
		}
		var input commitProofKeyJSON
		if err := decoder.Decode(&input); err != nil {
			return nil, fmt.Errorf("%w: keyring json: %v", ErrCommitProofInvalid, err)
		}
		if len(input.PublicKeyBase64) != base64.StdEncoding.EncodedLen(ed25519.PublicKeySize) {
			return nil, fmt.Errorf("%w: keyring public key", ErrCommitProofInvalid)
		}
		public, err := base64.StdEncoding.Strict().DecodeString(input.PublicKeyBase64)
		if err != nil || len(public) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("%w: keyring public key", ErrCommitProofInvalid)
		}
		entries = append(entries, CommitProofVerificationKey{SourceRealm: input.SourceRealm, HolderID: input.HolderID,
			HolderBootID: input.HolderBootID, KeyID: input.KeyID, PublicKey: ed25519.PublicKey(public),
			NotBeforeMs: input.NotBeforeMs, NotAfterMs: input.NotAfterMs})
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return nil, fmt.Errorf("%w: keyring json closing array", ErrCommitProofInvalid)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing keyring json", ErrCommitProofInvalid)
	}
	return NewCommitProofKeyring(entries, maxTTL)
}

func DecodeCommitProofPrivateKey(raw string) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: private key encoding", ErrCommitProofInvalid)
	}
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		return append(ed25519.PrivateKey(nil), decoded...), nil
	default:
		return nil, fmt.Errorf("%w: private key size", ErrCommitProofInvalid)
	}
}
