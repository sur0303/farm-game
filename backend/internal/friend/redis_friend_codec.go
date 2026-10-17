package friend

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	friendReceiptVersion = "FR3"
	friendReceiptLegacy  = "FR2"
	friendReceiptSep     = byte(0x1f)
	friendTypedVersion   = "TLP1"
)

var (
	ErrFriendReceiptContract       = errors.New("friend: redis receipt contract violation")
	ErrFriendAttemptConflict       = errors.New("friend: commit attempt conflict")
	ErrFriendRecordVersionConflict = errors.New("friend: redis record version conflict")
	ErrFriendNotCommitted          = errors.New("friend: commit attempt not committed")
	ErrFriendReconcile             = errors.New("friend: commit attempt reconcile required")
)

// friendIdentityMaterial gives the non-Actor port identity a stable physical
// root.  SO0's domain CommitIdentity predates the shared five-field identity
// type, so the adapter scopes it with the concrete 33A authority ID here.  A
// future port revision can replace this encoding without changing keys.
func friendIdentityMaterial(kind FriendshipAuthorityKind, authorityID string, identity CommitIdentity) ([]byte, error) {
	if err := validateFriendAuthorityID(kind, authorityID); err != nil {
		return nil, err
	}
	if !identity.Valid() {
		return nil, fmt.Errorf("%w: invalid commit identity", ErrInvalidCommand)
	}
	fields := []string{"friend-identity-v1", string(kind), authorityID}
	if identity.IsPublic() {
		if !validFriendStableName(identity.CommandType) || identity.OperationID == "" || !utf8.ValidString(identity.OperationID) {
			return nil, fmt.Errorf("%w: public identity", ErrInvalidCommand)
		}
		fields = append(fields, "PUBLIC", itoa(identity.SubjectPlayerID), identity.CommandType, identity.OperationID)
	} else {
		fields = append(fields, "WORKFLOW_PARTICIPANT", identity.WorkAddressID,
			identity.ParticipantInvocationID, identity.ParticipantStep)
	}
	var out []byte
	for _, field := range fields {
		if !utf8.ValidString(field) {
			return nil, fmt.Errorf("%w: identity utf8", ErrInvalidCommand)
		}
		out = appendFriendLP(out, field)
	}
	return out, nil
}

type friendCommitIdentity struct {
	material      []byte
	digest        [32]byte
	digestHex     string
	field         string
	prefix        []byte
	candidateHash string
}

// friendOperationLocatorDigest is the frozen root-scoped lowercase-hex
// locator from contract 31 §8.1.  It is deliberately kept local to this
// adapter because the common identity package has no FRIEND_SHARE_ISSUER root
// enum yet; the framing and formula are the shared contract, not a second
// StableOpaqueID scheme.
func friendOperationLocatorDigest(identity CommitIdentity, rootKind FriendshipAuthorityKind, rootID string) ([32]byte, error) {
	if !identity.IsPublic() || !identity.Valid() || rootID == "" || !validFriendStableName(identity.CommandType) {
		return [32]byte{}, fmt.Errorf("%w: operation locator", ErrInvalidCommand)
	}
	if err := validateFriendAuthorityID(rootKind, rootID); err != nil {
		return [32]byte{}, fmt.Errorf("%w: operation root", ErrInvalidCommand)
	}
	preimage := appendFriendLP(nil, itoa(identity.SubjectPlayerID))
	preimage = appendFriendLP(preimage, identity.CommandType)
	preimage = appendFriendLP(preimage, identity.OperationID)
	preimage = appendFriendLP(preimage, string(rootKind))
	preimage = appendFriendLP(preimage, rootID)
	return sha256.Sum256(preimage), nil
}

func friendParticipantLocatorDigest(identity CommitIdentity) ([32]byte, error) {
	if identity.IsPublic() || !identity.Valid() || identity.ParticipantInvocationID == "" {
		return [32]byte{}, fmt.Errorf("%w: participant locator", ErrInvalidCommand)
	}
	preimage := appendFriendLP(nil, "friend-participant-receipt-v1")
	preimage = appendFriendLP(preimage, identity.ParticipantInvocationID)
	return sha256.Sum256(preimage), nil
}

func newFriendCommitIdentity(kind FriendshipAuthorityKind, authorityID string, identity CommitIdentity, payloadHash []byte, candidateFields ...string) (friendCommitIdentity, error) {
	material, err := friendIdentityMaterial(kind, authorityID, identity)
	if err != nil {
		return friendCommitIdentity{}, err
	}
	var digest [32]byte
	if identity.IsPublic() {
		digest, err = friendOperationLocatorDigest(identity, kind, authorityID)
	} else {
		digest, err = friendParticipantLocatorDigest(identity)
	}
	if err != nil {
		return friendCommitIdentity{}, err
	}
	digestHex := hex.EncodeToString(digest[:])
	fieldPrefix := "participant:"
	if identity.IsPublic() {
		fieldPrefix = "op:"
	}
	prefix := appendFriendLP(nil, string(material))
	prefix = appendFriendLP(prefix, string(payloadHash))
	candidate := appendFriendLP(nil, string(material))
	candidate = appendFriendLP(candidate, string(payloadHash))
	for _, field := range candidateFields {
		candidate = appendFriendLP(candidate, field)
	}
	candidateHash := sha256.Sum256(candidate)
	return friendCommitIdentity{
		material:      material,
		digest:        digest,
		digestHex:     digestHex,
		field:         fieldPrefix + digestHex,
		prefix:        prefix,
		candidateHash: hex.EncodeToString(candidateHash[:]),
	}, nil
}

func appendFriendLPBytes(dst []byte, value []byte) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(value)))
	dst = append(dst, n[:]...)
	return append(dst, value...)
}

type friendReceiptValue struct {
	IdentityPrefix     []byte
	Kind               string
	Status             StatusClass
	Reason             ReasonCode
	Epoch              uint64
	Detail             string
	CommittedAtMs      int64
	Typed              []string
	CommittedAttemptID string
	CandidateHash      string
	AttemptOrdinal     uint64
}

func parseFriendReceipt(blob []byte) (friendReceiptValue, error) {
	identity, rest, err := readFriendLP(blob)
	if err != nil {
		return friendReceiptValue{}, err
	}
	payload, rest, err := readFriendLP(rest)
	if err != nil {
		return friendReceiptValue{}, err
	}
	if len(rest) == 0 || (!bytes.HasPrefix(rest, []byte(friendReceiptVersion)) &&
		!bytes.HasPrefix(rest, []byte(friendReceiptLegacy))) {
		return friendReceiptValue{}, fmt.Errorf("%w: version", ErrFriendReceiptContract)
	}
	version := friendReceiptVersion
	partCount := 10
	if bytes.HasPrefix(rest, []byte(friendReceiptLegacy)) {
		version = friendReceiptLegacy
		partCount = 7
	}
	body := rest[len(version):]
	parts := make([][]byte, 0, partCount)
	for i := 0; i < partCount; i++ {
		var part []byte
		part, body, err = readFriendTextLP(body)
		if err != nil {
			return friendReceiptValue{}, err
		}
		parts = append(parts, part)
	}
	if len(body) != 0 {
		return friendReceiptValue{}, fmt.Errorf("%w: trailing receipt bytes", ErrFriendReceiptContract)
	}
	status, err := parseFriendStatus(string(parts[1]))
	if err != nil {
		return friendReceiptValue{}, err
	}
	epoch, err := strconv.ParseUint(string(parts[3]), 10, 64)
	if err != nil {
		return friendReceiptValue{}, fmt.Errorf("%w: epoch", ErrFriendReceiptContract)
	}
	committedAt, err := strconv.ParseInt(string(parts[5]), 10, 64)
	if err != nil {
		return friendReceiptValue{}, fmt.Errorf("%w: time", ErrFriendReceiptContract)
	}
	var committedAttemptID, candidateHash string
	var attemptOrdinal uint64
	if partCount == 10 {
		committedAttemptID = string(parts[7])
		candidateHash = string(parts[8])
		attemptOrdinal, err = strconv.ParseUint(string(parts[9]), 10, 64)
		if err != nil || committedAttemptID == "" || len(candidateHash) != sha256.Size*2 {
			return friendReceiptValue{}, fmt.Errorf("%w: committed attempt", ErrFriendReceiptContract)
		}
	}
	typed, err := decodeFriendTypedPayload(string(parts[6]))
	if err != nil {
		return friendReceiptValue{}, err
	}
	return friendReceiptValue{
		IdentityPrefix: append(append([]byte(nil), identity...), appendFriendLP(nil, string(payload))...),
		Kind:           string(parts[0]), Status: status, Reason: ReasonCode(parts[2]),
		Epoch: epoch, Detail: string(parts[4]), CommittedAtMs: committedAt,
		Typed: func() []string {
			if len(typed) == 0 {
				return nil
			}
			return append([]string{"RAW1"}, typed...)
		}(),
		CommittedAttemptID: committedAttemptID, CandidateHash: candidateHash, AttemptOrdinal: attemptOrdinal,
	}, nil
}

func friendReceiptPrefixMatches(blob []byte, prefix []byte) bool {
	return len(blob) >= len(prefix) && bytes.Equal(blob[:len(prefix)], prefix)
}

func readFriendLP(blob []byte) ([]byte, []byte, error) {
	if len(blob) < 4 {
		return nil, nil, fmt.Errorf("%w: short LP", ErrFriendReceiptContract)
	}
	n := binary.BigEndian.Uint32(blob[:4])
	if uint64(n) > uint64(len(blob)-4) {
		return nil, nil, fmt.Errorf("%w: LP length", ErrFriendReceiptContract)
	}
	return append([]byte(nil), blob[4:4+n]...), blob[4+n:], nil
}

// readFriendTextLP is the textual length-prefix format used inside Lua
// receipts and typed payloads.  Unlike the old RAW1 delimiter format it is
// safe for valid IDs and payload fields containing '|', SEP, or ':' bytes.
func readFriendTextLP(blob []byte) ([]byte, []byte, error) {
	colon := bytes.IndexByte(blob, ':')
	if colon <= 0 {
		return nil, nil, fmt.Errorf("%w: text LP prefix", ErrFriendReceiptContract)
	}
	n, err := strconv.Atoi(string(blob[:colon]))
	if err != nil || n < 0 || n > len(blob)-colon-1 {
		return nil, nil, fmt.Errorf("%w: text LP length", ErrFriendReceiptContract)
	}
	start := colon + 1
	return append([]byte(nil), blob[start:start+n]...), blob[start+n:], nil
}

func parseFriendStatus(value string) (StatusClass, error) {
	switch value {
	case "SUCCEEDED":
		return StatusSucceeded, nil
	case "REJECTED":
		return StatusRejected, nil
	case "NO_CHANGE":
		return StatusNoChange, nil
	default:
		return StatusUnspecified, fmt.Errorf("%w: status %q", ErrFriendReceiptContract, value)
	}
}

func friendStatusToken(status StatusClass) string {
	switch status {
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusRejected:
		return "REJECTED"
	case StatusNoChange:
		return "NO_CHANGE"
	default:
		return "UNKNOWN"
	}
}

func encodeFriendTyped(fields ...string) string {
	var builder strings.Builder
	builder.WriteString(friendTypedVersion)
	for _, field := range fields {
		builder.WriteString(strconv.Itoa(len([]byte(field))))
		builder.WriteByte(':')
		builder.WriteString(field)
	}
	return builder.String()
}

func decodeFriendTyped(fields []string) ([]string, error) {
	if len(fields) > 0 && fields[0] == "RAW1" {
		return append([]string(nil), fields[1:]...), nil
	}
	if len(fields) == 1 && strings.HasPrefix(fields[0], friendTypedVersion) {
		return decodeFriendTypedPayload(fields[0])
	}
	out := make([]string, len(fields))
	for i, field := range fields {
		decoded, err := base64.RawURLEncoding.DecodeString(field)
		if err != nil {
			return nil, fmt.Errorf("%w: typed field %d", ErrFriendReceiptContract, i)
		}
		out[i] = string(decoded)
	}
	return out, nil
}

func decodeFriendTypedPayload(payload string) ([]string, error) {
	if payload == "" {
		return nil, nil
	}
	if !strings.HasPrefix(payload, friendTypedVersion) {
		return nil, fmt.Errorf("%w: typed version", ErrFriendReceiptContract)
	}
	rest := []byte(payload[len(friendTypedVersion):])
	out := make([]string, 0, 8)
	for len(rest) > 0 {
		field, tail, err := readFriendTextLP(rest)
		if err != nil {
			return nil, err
		}
		out = append(out, string(field))
		rest = tail
	}
	return out, nil
}

func outcomeFromFriendReceipt(value friendReceiptValue, identity CommitIdentity, payloadHash []byte) Outcome {
	return Outcome{
		Status: value.Status, Reason: value.Reason, Identity: identity,
		PayloadHash: append([]byte(nil), payloadHash...), CommittedAtMs: value.CommittedAtMs,
		CommittedEpoch: value.Epoch, Detail: value.Detail,
	}
}

func parseFriendToken(fields []string) (ShareToken, error) {
	decoded, err := decodeFriendTyped(fields)
	if err != nil || len(decoded) != 7 {
		return ShareToken{}, fmt.Errorf("%w: token fields", ErrFriendReceiptContract)
	}
	generation, err := strconv.ParseUint(decoded[1], 10, 64)
	if err != nil {
		return ShareToken{}, err
	}
	issued, err := strconv.ParseInt(decoded[2], 10, 64)
	if err != nil {
		return ShareToken{}, err
	}
	expires, err := strconv.ParseInt(decoded[3], 10, 64)
	if err != nil {
		return ShareToken{}, err
	}
	status, err := strconv.ParseUint(decoded[4], 10, 8)
	if err != nil {
		return ShareToken{}, err
	}
	purpose, err := strconv.ParseUint(decoded[5], 10, 8)
	if err != nil {
		return ShareToken{}, err
	}
	format, err := strconv.ParseUint(decoded[6], 10, 32)
	if err != nil {
		return ShareToken{}, err
	}
	return ShareToken{TokenID: decoded[0], TokenGeneration: generation, IssuedAtMs: issued,
		ExpiresAtMs: expires, Status: TokenStatus(status), Purpose: SharePurpose(purpose), FormatVersion: uint32(format)}, nil
}

func parseFriendValidation(fields []string) (ValidationReceipt, error) {
	decoded, err := decodeFriendTyped(fields)
	if err != nil || len(decoded) != 7 {
		return ValidationReceipt{}, fmt.Errorf("%w: validation fields", ErrFriendReceiptContract)
	}
	generation, err := strconv.ParseUint(decoded[2], 10, 64)
	if err != nil {
		return ValidationReceipt{}, err
	}
	inviter, err := strconv.ParseUint(decoded[3], 10, 64)
	if err != nil {
		return ValidationReceipt{}, err
	}
	redeemer, err := strconv.ParseUint(decoded[4], 10, 64)
	if err != nil {
		return ValidationReceipt{}, err
	}
	committed, err := strconv.ParseInt(decoded[5], 10, 64)
	if err != nil {
		return ValidationReceipt{}, err
	}
	return ValidationReceipt{ReceiptID: decoded[0], WorkflowID: decoded[1], TokenID: decoded[6],
		TokenGeneration: generation, InviterPlayerID: inviter, RedeemerPlayerID: redeemer, CommittedAtMs: committed}, nil
}

func parseFriendRotate(fields []string) (RotateReceipt, error) {
	decoded, err := decodeFriendTyped(fields)
	if err != nil || (len(decoded) != 8 && len(decoded) != 9 && len(decoded) != 11 && len(decoded) != 15 && len(decoded) != 16 && len(decoded) != 18) {
		return RotateReceipt{}, fmt.Errorf("%w: rotate fields", ErrFriendReceiptContract)
	}
	withBinding := len(decoded) == 18
	if len(decoded) == 15 || len(decoded) == 16 || withBinding {
		decoded = decoded[7:]
	}
	terminated, err := strconv.ParseUint(decoded[2], 10, 64)
	if err != nil {
		return RotateReceipt{}, err
	}
	revision, err := strconv.ParseUint(decoded[3], 10, 64)
	if err != nil {
		return RotateReceipt{}, err
	}
	generation, err := strconv.ParseUint(decoded[5], 10, 64)
	if err != nil {
		return RotateReceipt{}, err
	}
	committed, err := strconv.ParseInt(decoded[6], 10, 64)
	if err != nil {
		return RotateReceipt{}, err
	}
	var lowFloor, highFloor uint64
	if len(decoded) == 9 || len(decoded) == 11 {
		lowFloor, err = strconv.ParseUint(decoded[7], 10, 64)
		if err != nil {
			return RotateReceipt{}, err
		}
		highFloor, err = strconv.ParseUint(decoded[8], 10, 64)
		if err != nil {
			return RotateReceipt{}, err
		}
	}
	var operationID string
	var payloadHash []byte
	if len(decoded) == 11 {
		operationID = decoded[9]
		if operationID == "" {
			return RotateReceipt{}, fmt.Errorf("%w: rotate operation id", ErrFriendReceiptContract)
		}
		payloadHash, err = hex.DecodeString(decoded[10])
		if err != nil || len(payloadHash) != sha256.Size {
			return RotateReceipt{}, fmt.Errorf("%w: rotate payload hash", ErrFriendReceiptContract)
		}
	}
	return RotateReceipt{ReceiptID: decoded[0], PairID: decoded[1], TerminatedRelationEpoch: terminated,
		BarrierRevision: revision, TokenID: decoded[4], TokenGeneration: generation, CommittedAtMs: committed,
		LowShareGenerationFloor: lowFloor, HighShareGenerationFloor: highFloor,
		OperationID: operationID, PayloadHash: payloadHash}, nil
}

func parseFriendAction(fields []string) (ActionPermitSnapshot, string, int64, error) {
	decoded, err := decodeFriendTyped(fields)
	if err != nil || len(decoded) != 15 {
		return ActionPermitSnapshot{}, "", 0, fmt.Errorf("%w: action fields", ErrFriendReceiptContract)
	}
	parseU := func(index int) (uint64, error) { return strconv.ParseUint(decoded[index], 10, 64) }
	epoch, err := parseU(2)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	visitor, err := parseU(3)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	owner, err := parseU(4)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	target, err := parseU(5)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	scope, err := parseU(7)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	issued, err := strconv.ParseInt(decoded[10], 10, 64)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	expires, err := strconv.ParseInt(decoded[11], 10, 64)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	accepted, err := strconv.ParseInt(decoded[12], 10, 64)
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	workflowDeadline, err := strconv.ParseInt(decoded[14], 10, 64)
	if err != nil || workflowDeadline <= expires {
		return ActionPermitSnapshot{}, "", 0, fmt.Errorf("%w: workflow deadline", ErrFriendReceiptContract)
	}
	payload, err := hex.DecodeString(decoded[8])
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	binding, err := hex.DecodeString(decoded[9])
	if err != nil {
		return ActionPermitSnapshot{}, "", 0, err
	}
	return ActionPermitSnapshot{PermitID: decoded[0], RelationID: decoded[1], RelationEpoch: epoch,
		VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: target, OperationID: decoded[6],
		Scope: ActionScope(scope), PayloadHash: payload, TargetBindingHash: binding,
		IssuedAtAuthorityMs: issued, ExpireAtAuthorityMs: expires,
		ExecutionAcceptedAtAuthorityMs: accepted}, decoded[13], workflowDeadline, nil
}

func encodeFriendResolution(status string, attemptID, candidateHash string, atMs int64) string {
	marker := strings.Repeat("0", sha256.Size*2)
	parts := []string{status, attemptID, candidateHash, "resolution", "1", strconv.FormatInt(atMs, 10), marker, "", "", "0"}
	var builder strings.Builder
	builder.WriteString("FRS3")
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func parseFriendResolution(value string) (string, error) {
	evidence, err := parseFriendResolutionEvidence(value)
	if err != nil {
		return "", err
	}
	return evidence.Status, nil
}

type friendResolutionEvidence struct {
	Status        string
	AttemptID     string
	CandidateHash string
	ResolutionID  string
	Ordinal       uint64
	ResolvedAtMs  int64
	MarkerDigest  string
	ReceiptField  string
	ReceiptKind   string
	ReceiptEpoch  uint64
}

func parseFriendResolutionEvidence(value string) (friendResolutionEvidence, error) {
	if !strings.HasPrefix(value, "FRS3") {
		return friendResolutionEvidence{}, fmt.Errorf("%w: resolution", ErrFriendReceiptContract)
	}
	rest := []byte(value[len("FRS3"):])
	parts := make([][]byte, 0, 10)
	for len(parts) < 10 {
		part, tail, err := readFriendTextLP(rest)
		if err != nil {
			return friendResolutionEvidence{}, err
		}
		parts = append(parts, part)
		rest = tail
	}
	if len(rest) != 0 || (string(parts[0]) != "COMMITTED" && string(parts[0]) != "NOT_COMMITTED" && string(parts[0]) != "RECONCILE_REQUIRED") ||
		string(parts[1]) == "" || !validFriendHex(string(parts[2]), sha256.Size) || string(parts[3]) == "" || string(parts[4]) == "0" ||
		!validFriendHex(string(parts[6]), sha256.Size) {
		return friendResolutionEvidence{}, fmt.Errorf("%w: resolution evidence", ErrFriendReceiptContract)
	}
	ordinal, ordinalErr := strconv.ParseUint(string(parts[4]), 10, 64)
	resolvedAt, timeErr := strconv.ParseInt(string(parts[5]), 10, 64)
	receiptEpoch, epochErr := strconv.ParseUint(string(parts[9]), 10, 64)
	if ordinalErr != nil || ordinal == 0 || strconv.FormatUint(ordinal, 10) != string(parts[4]) ||
		timeErr != nil || resolvedAt < 0 || strconv.FormatInt(resolvedAt, 10) != string(parts[5]) ||
		epochErr != nil || strconv.FormatUint(receiptEpoch, 10) != string(parts[9]) {
		return friendResolutionEvidence{}, fmt.Errorf("%w: resolution numeric evidence", ErrFriendReceiptContract)
	}
	if string(parts[0]) == "COMMITTED" && (len(parts[7]) == 0 || len(parts[8]) == 0) {
		return friendResolutionEvidence{}, fmt.Errorf("%w: committed resolution evidence", ErrFriendReceiptContract)
	}
	if string(parts[0]) != "COMMITTED" && (len(parts[7]) != 0 || len(parts[8]) != 0 || string(parts[9]) != "0") {
		return friendResolutionEvidence{}, fmt.Errorf("%w: non-committed resolution evidence", ErrFriendReceiptContract)
	}
	return friendResolutionEvidence{Status: string(parts[0]), AttemptID: string(parts[1]), CandidateHash: string(parts[2]),
		ResolutionID: string(parts[3]), Ordinal: ordinal, ResolvedAtMs: resolvedAt, MarkerDigest: string(parts[6]),
		ReceiptField: string(parts[7]), ReceiptKind: string(parts[8]), ReceiptEpoch: receiptEpoch}, nil
}

func parseFriendCommitMarkerProof(value string) (map[string]string, error) {
	if len(value) == 0 || len(value) > friendAttemptHardLimit {
		return nil, fmt.Errorf("%w: marker proof size", ErrFriendReceiptContract)
	}
	fields := make(map[string]string)
	rest := []byte(value)
	for len(rest) > 0 {
		tag, tail, err := readFriendTextLP(rest)
		if err != nil {
			return nil, fmt.Errorf("%w: marker tag", ErrFriendReceiptContract)
		}
		fieldValue, next, err := readFriendTextLP(tail)
		if err != nil {
			return nil, fmt.Errorf("%w: marker value", ErrFriendReceiptContract)
		}
		name := string(tag)
		if name == "" {
			return nil, fmt.Errorf("%w: empty marker tag", ErrFriendReceiptContract)
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("%w: duplicate marker tag", ErrFriendReceiptContract)
		}
		fields[name] = string(fieldValue)
		rest = next
	}
	fixed := []string{"v", "d", "c", "a", "o", "e", "rv", "rd", "rb", "rf", "rp", "rk", "rs", "re",
		"rm", "rx", "oc", "fc", "fb", "pc", "pb", "xf", "xv", "xm", "xx", "ly", "bu", "au"}
	for _, tag := range fixed {
		if _, ok := fields[tag]; !ok {
			return nil, fmt.Errorf("%w: missing marker tag %s", ErrFriendReceiptContract, tag)
		}
	}
	orderCount, err := strconv.ParseUint(fields["oc"], 10, 64)
	if err != nil || orderCount > 2 {
		return nil, fmt.Errorf("%w: marker order count", ErrFriendReceiptContract)
	}
	expected := make(map[string]struct{}, len(fixed)+int(orderCount)*6)
	for _, tag := range fixed {
		expected[tag] = struct{}{}
	}
	for i := uint64(1); i <= orderCount; i++ {
		for _, suffix := range []string{"i", "t", "b", "z", "r", "p", "n"} {
			tag := "o" + strconv.FormatUint(i, 10) + suffix
			expected[tag] = struct{}{}
			if _, ok := fields[tag]; !ok {
				return nil, fmt.Errorf("%w: missing marker order tag %s", ErrFriendReceiptContract, tag)
			}
		}
	}
	if len(fields) != len(expected) {
		return nil, fmt.Errorf("%w: marker proof manifest", ErrFriendReceiptContract)
	}
	for tag := range fields {
		if _, ok := expected[tag]; !ok {
			return nil, fmt.Errorf("%w: unexpected marker tag %s", ErrFriendReceiptContract, tag)
		}
	}
	return fields, nil
}
