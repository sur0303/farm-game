package holdercontrol

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/actorrouting"
)

const (
	MaxDomainBytes      = 16
	MaxHolderIDBytes    = 128
	MaxBootIDBytes      = 128
	MaxPreparedBytes    = 512
	MaxGenerationFuture = 24 * time.Hour
)

var (
	ErrInvalidRequest     = errors.New("holdercontrol: invalid request")
	ErrAuthorization      = errors.New("holdercontrol: authorization unavailable")
	ErrShardPhaseOrder    = errors.New("holdercontrol: invalid shard control phase")
	ErrGenerationConflict = errors.New("holdercontrol: conflicting prepared generation")
	ErrRuntimeUnavailable = errors.New("holdercontrol: shard runtime unavailable")
	ErrFactsPrecondition  = errors.New("holdercontrol: facts barrier precondition not met")
	ErrPreparedNotFound   = errors.New("holdercontrol: prepared membership not found")
	ErrPreparedExpired    = errors.New("holdercontrol: prepared membership expired")
	ErrPreparedOverflow   = errors.New("holdercontrol: prepared membership capacity exceeded")
	ErrPreparedBusy       = errors.New("holdercontrol: prepared membership is committing")
	ErrPreparedIdentity   = errors.New("holdercontrol: prepared membership identity mismatch")
)

// AuthorityGeneration is the exact route generation accepted by Prepare.
// It intentionally contains no WriterClaim, action token, or secret.
type AuthorityGeneration struct {
	HolderID           string
	HolderBootID       string
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
	LeaseUntilMs       int64
}

func (g AuthorityGeneration) Equal(other AuthorityGeneration) bool {
	return g == other
}

func (g AuthorityGeneration) Validate() error {
	return g.validate()
}

func (g AuthorityGeneration) ValidateAt(now time.Time) error {
	if err := g.validate(); err != nil {
		return err
	}
	if g.LeaseUntilMs <= now.UnixMilli() {
		return fmt.Errorf("%w: generation lease is expired", ErrPreparedExpired)
	}
	if g.LeaseUntilMs > now.Add(MaxGenerationFuture).UnixMilli() {
		return fmt.Errorf("%w: generation lease is too far in the future", ErrInvalidRequest)
	}
	return nil
}

func (g AuthorityGeneration) validate() error {
	if err := validateBoundString("holder_id", g.HolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := validateBoundString("holder_boot_id", g.HolderBootID, MaxBootIDBytes); err != nil {
		return err
	}
	if g.AssignmentRevision == 0 || g.WriterEpoch == 0 || g.LeaseEpoch == 0 || g.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: complete authority generation required", ErrInvalidRequest)
	}
	return nil
}

func decodeGeneration(value *holdercontrolpb.AuthorityGeneration) (AuthorityGeneration, error) {
	if value == nil {
		return AuthorityGeneration{}, fmt.Errorf("%w: generation required", ErrInvalidRequest)
	}
	generation := AuthorityGeneration{
		HolderID:           value.GetHolderId(),
		HolderBootID:       value.GetHolderBootId(),
		AssignmentRevision: value.GetAssignmentRevision(),
		WriterEpoch:        value.GetWriterEpoch(),
		LeaseEpoch:         value.GetLeaseEpoch(),
		LeaseUntilMs:       value.GetLeaseUntilMs(),
	}
	if err := generation.validate(); err != nil {
		return AuthorityGeneration{}, err
	}
	return generation, nil
}

func DecodeAuthorityGeneration(value *holdercontrolpb.AuthorityGeneration) (AuthorityGeneration, error) {
	return decodeGeneration(value)
}

func encodeGeneration(value AuthorityGeneration) *holdercontrolpb.AuthorityGeneration {
	return &holdercontrolpb.AuthorityGeneration{
		HolderId:           value.HolderID,
		HolderBootId:       value.HolderBootID,
		AssignmentRevision: value.AssignmentRevision,
		WriterEpoch:        value.WriterEpoch,
		LeaseEpoch:         value.LeaseEpoch,
		LeaseUntilMs:       value.LeaseUntilMs,
	}
}

func EncodeAuthorityGeneration(value AuthorityGeneration) (*holdercontrolpb.AuthorityGeneration, error) {
	if err := value.validate(); err != nil {
		return nil, err
	}
	return encodeGeneration(value), nil
}

func decodeDomain(value string) (actorrouting.RouteDomain, error) {
	if err := validateBoundString("domain", value, MaxDomainBytes); err != nil {
		return "", err
	}
	domain := actorrouting.RouteDomain(value)
	if domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm {
		return "", fmt.Errorf("%w: unsupported domain %q", ErrInvalidRequest, value)
	}
	return domain, nil
}

func ParseDomain(value string) (actorrouting.RouteDomain, error) {
	return decodeDomain(value)
}

func validateBoundString(name, value string, maxBytes int) error {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must be non-empty, trimmed UTF-8", ErrInvalidRequest, name)
	}
	if len([]byte(value)) > maxBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrInvalidRequest, name, maxBytes)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalidRequest, name)
		}
	}
	return nil
}

func validateHolderIdentity(holderID, bootID string) error {
	if err := validateBoundString("holder_id", holderID, MaxHolderIDBytes); err != nil {
		return err
	}
	return validateBoundString("holder_boot_id", bootID, MaxBootIDBytes)
}

func validateShard(shard uint32, count uint32) error {
	if count == 0 || shard > count-1 {
		return fmt.Errorf("%w: shard %d is outside configured range", ErrInvalidRequest, shard)
	}
	return nil
}
