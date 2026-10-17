// Package workidentity implements the stable identities shared by workflow,
// authority storage, workers and test tools. The byte-level format is frozen
// by design contract 31; routing hash seeds must never be used here.
package workidentity

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	stableIDPrefix = "sid1_"
	stableIDBytes  = sha256.Size
)

var ErrInvalidIdentity = errors.New("workidentity: invalid identity input")

// StableOpaqueID returns sid1_ + Base64URL_NoPadding(SHA-256(LP(fields...))).
// LP is uint32 big-endian byte length followed by the UTF-8 bytes.
func StableOpaqueID(namespace string, fields ...string) (string, error) {
	if namespace == "" || !utf8.ValidString(namespace) {
		return "", fmt.Errorf("%w: namespace", ErrInvalidIdentity)
	}
	preimage, err := appendLP(nil, namespace)
	if err != nil {
		return "", err
	}
	for i, field := range fields {
		if !utf8.ValidString(field) {
			return "", fmt.Errorf("%w: field %d is not UTF-8", ErrInvalidIdentity, i)
		}
		preimage, err = appendLP(preimage, field)
		if err != nil {
			return "", err
		}
	}
	digest := sha256.Sum256(preimage)
	return stableIDPrefix + base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

// ValidateStableOpaqueID rejects truncated, padded or non-canonical IDs.
func ValidateStableOpaqueID(id string) error {
	if !strings.HasPrefix(id, stableIDPrefix) {
		return fmt.Errorf("%w: stable id prefix", ErrInvalidIdentity)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(id, stableIDPrefix))
	if err != nil || len(raw) != stableIDBytes || stableIDPrefix+base64.RawURLEncoding.EncodeToString(raw) != id {
		return fmt.Errorf("%w: stable id encoding", ErrInvalidIdentity)
	}
	return nil
}

// OperationDigest is the lowercase SHA-256 digest used to locate an
// IdempotencyKey(subject_player_id, command_type, operation_id).
func OperationDigest(subjectPlayerID uint64, commandType, operationID string) (string, error) {
	if subjectPlayerID == 0 || !validStableName(commandType) || operationID == "" || !utf8.ValidString(operationID) {
		return "", fmt.Errorf("%w: operation digest fields", ErrInvalidIdentity)
	}
	preimage, _ := appendLP(nil, strconv.FormatUint(subjectPlayerID, 10))
	preimage, _ = appendLP(preimage, commandType)
	preimage, err := appendLP(preimage, operationID)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(preimage)
	return hex.EncodeToString(digest[:]), nil
}

func WorkflowID(subjectPlayerID uint64, commandType, operationID string) (string, error) {
	if subjectPlayerID == 0 || !validStableName(commandType) || operationID == "" {
		return "", fmt.Errorf("%w: workflow id fields", ErrInvalidIdentity)
	}
	return StableOpaqueID("farm-workflow-id-v1", strconv.FormatUint(subjectPlayerID, 10), commandType, operationID)
}

func SourceOrderID(orderType, sourceRouteDomain string, sourceActorID uint64, sourceFactID string, sourceFactVersion uint64) (string, error) {
	if !validStableName(orderType) || !validStableName(sourceRouteDomain) || sourceActorID == 0 || sourceFactID == "" || sourceFactVersion == 0 {
		return "", fmt.Errorf("%w: source order id fields", ErrInvalidIdentity)
	}
	return StableOpaqueID("farm-source-order-id-v1", orderType, sourceRouteDomain,
		strconv.FormatUint(sourceActorID, 10), sourceFactID, strconv.FormatUint(sourceFactVersion, 10))
}

func ParticipantInvocationID(workType, workID, participantStep string, stepInstance uint64) (string, error) {
	if !validStableName(workType) || ValidateStableOpaqueID(workID) != nil || !validStableName(participantStep) || stepInstance == 0 {
		return "", fmt.Errorf("%w: participant invocation id fields", ErrInvalidIdentity)
	}
	return StableOpaqueID("farm-participant-invocation-v1", workType, workID, participantStep, strconv.FormatUint(stepInstance, 10))
}

func UnlockCoinReservationID(workflowID string) (string, error) {
	return derivedID("farm-unlock-coin-reservation-id-v1", workflowID)
}

func SeedReservationID(workflowID string) (string, error) {
	return derivedID("farm-seed-reservation-id-v1", workflowID)
}

func CropCycleID(workflowID string) (string, error) {
	return derivedID("farm-crop-cycle-id-v1", workflowID)
}

func TheftSettlementID(workflowID string) (string, error) {
	return derivedID("farm-theft-settlement-id-v1", workflowID)
}

func CareIncidentID(cropCycleID, careType string, ordinal uint64) (string, error) {
	if ValidateStableOpaqueID(cropCycleID) != nil || !validStableName(careType) || ordinal == 0 {
		return "", fmt.Errorf("%w: care incident id fields", ErrInvalidIdentity)
	}
	return StableOpaqueID("farm-care-incident-id-v1", cropCycleID, careType, strconv.FormatUint(ordinal, 10))
}

func TheftAttemptID(targetFarmID uint64, cropCycleID string, visitorPlayerID uint64) (string, error) {
	if targetFarmID == 0 || visitorPlayerID == 0 || ValidateStableOpaqueID(cropCycleID) != nil {
		return "", fmt.Errorf("%w: theft attempt id fields", ErrInvalidIdentity)
	}
	return StableOpaqueID("farm-theft-attempt-id-v1", strconv.FormatUint(targetFarmID, 10), cropCycleID,
		strconv.FormatUint(visitorPlayerID, 10))
}

func derivedID(namespace, parentID string) (string, error) {
	if err := ValidateStableOpaqueID(parentID); err != nil {
		return "", fmt.Errorf("%w: parent stable id", ErrInvalidIdentity)
	}
	return StableOpaqueID(namespace, parentID)
}

func appendLP(dst []byte, value string) ([]byte, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: LP field too large", ErrInvalidIdentity)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func validStableName(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
