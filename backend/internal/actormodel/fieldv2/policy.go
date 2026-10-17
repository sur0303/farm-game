package fieldv2

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"farmgame/internal/actorrouting"
)

const CohortBasis = uint16(10_000)

type DomainPolicy struct {
	ConversionEnabled bool
	CohortPermyriad   uint16
}

type ConversionPolicy struct {
	Player DomainPolicy
	Farm   DomainPolicy
}

// DefaultConversionPolicy is the parser's absent-env value: both domains
// disabled. Production binaries always apply FullConversionPolicy. Unit tests
// that construct a Registry without a policy stay v1.
func DefaultConversionPolicy() ConversionPolicy { return ConversionPolicy{} }

// FullConversionPolicy converts every Player and Farm v1 row on the next
// load. Already-v2 rows stay v2. Command results are unchanged; only the
// physical HASH field layout becomes sparse upserts.
func FullConversionPolicy() ConversionPolicy {
	enabled := DomainPolicy{ConversionEnabled: true, CohortPermyriad: CohortBasis}
	return ConversionPolicy{Player: enabled, Farm: enabled}
}

func (p ConversionPolicy) Eligible(domain actorrouting.RouteDomain, actorID uint64) (bool, error) {
	if actorID == 0 {
		return false, fmt.Errorf("%w: actor id", ErrMalformed)
	}
	policy := p.Player
	if domain == actorrouting.DomainFarm {
		policy = p.Farm
	} else if domain != actorrouting.DomainPlayer {
		return false, fmt.Errorf("%w: domain", ErrMalformed)
	}
	if !policy.ConversionEnabled || policy.CohortPermyriad == 0 {
		return false, nil
	}
	if policy.CohortPermyriad > CohortBasis {
		return false, fmt.Errorf("%w: cohort", ErrMalformed)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("actor-model-field-v2-cohort-v1\x00"))
	_, _ = h.Write([]byte(domain))
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], actorID)
	_, _ = h.Write(id[:])
	sum := h.Sum(nil)
	return binary.BigEndian.Uint16(sum[:2])%CohortBasis < policy.CohortPermyriad, nil
}

// WriteLayout applies policy only to a v1 record. A v2 record always remains
// v2, including after conversion is disabled.
func (p ConversionPolicy) WriteLayout(domain actorrouting.RouteDomain, actorID uint64, current uint8) (uint8, error) {
	switch current {
	case 2:
		return 2, nil
	case 1:
		eligible, err := p.Eligible(domain, actorID)
		if err != nil || !eligible {
			return 1, err
		}
		return 2, nil
	default:
		return 0, fmt.Errorf("%w: model layout", ErrMalformed)
	}
}
