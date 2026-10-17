package authority

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"farmgame/internal/storage/authoritystore"
)

var errMalformedBlob = errors.New("redisauthority: malformed stored blob")

func hexEncode(b []byte) string { return hex.EncodeToString(b) }

func sha256Hex(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write(appendLP(nil, part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// appendLP appends one length-prefixed field (4-byte big-endian length).
func appendLP(dst []byte, value string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	dst = append(dst, size[:]...)
	return append(dst, value...)
}

// parseLP decodes consecutive length-prefixed fields.
func parseLP(blob []byte) ([]string, error) {
	var out []string
	i := 0
	for i < len(blob) {
		if i+4 > len(blob) {
			return nil, errMalformedBlob
		}
		length := int(binary.BigEndian.Uint32(blob[i:]))
		i += 4
		if length < 0 || i+length > len(blob) {
			return nil, errMalformedBlob
		}
		out = append(out, string(blob[i:i+length]))
		i += length
	}
	return out, nil
}

// businessStateDigest mirrors authoritystore's digest over a full model state:
// sorted field names, 4-byte name length + name + 8-byte value length + value.
// It must stay deterministic inside this adapter because FenceAndClaim compares
// the stored digest against the caller's Before snapshot digest.
func businessStateDigest(state authoritystore.ModelState) authoritystore.PayloadHash {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	var size [8]byte
	for _, key := range keys {
		binary.BigEndian.PutUint32(size[:4], uint32(len(key)))
		_, _ = h.Write(size[:4])
		_, _ = h.Write([]byte(key))
		value := state[key]
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(value)
	}
	var out authoritystore.PayloadHash
	copy(out[:], h.Sum(nil))
	return out
}

func digestHex(d authoritystore.PayloadHash) string { return hex.EncodeToString(d[:]) }

func parseDigestHex(value string) (authoritystore.PayloadHash, error) {
	var out authoritystore.PayloadHash
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != sha256.Size {
		return out, fmt.Errorf("%w: payload hash", errMalformedBlob)
	}
	copy(out[:], raw)
	return out, nil
}

// manifestLP builds the deterministic model manifest: sorted names, LP joined.
func manifestLP(state authoritystore.ModelState) string {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []byte
	for _, key := range keys {
		out = appendLP(out, key)
	}
	return string(out)
}

func namesManifestLP(names []string) string {
	var out []byte
	for _, key := range names {
		out = appendLP(out, key)
	}
	return string(out)
}

func parseManifest(blob string) ([]string, error) { return parseLP([]byte(blob)) }

func terminalStatusToken(status authoritystore.TerminalStatus) (string, error) {
	switch status {
	case authoritystore.TerminalSucceeded:
		return "SUCCEEDED", nil
	case authoritystore.TerminalRejected:
		return "REJECTED", nil
	case authoritystore.TerminalNoChange:
		return "NO_CHANGE", nil
	default:
		return "", fmt.Errorf("%w: operation result is not terminal", authoritystore.ErrInvalidRequest)
	}
}

func parseTerminalStatus(token string) (authoritystore.TerminalStatus, error) {
	switch token {
	case "SUCCEEDED":
		return authoritystore.TerminalSucceeded, nil
	case "REJECTED":
		return authoritystore.TerminalRejected, nil
	case "NO_CHANGE":
		return authoritystore.TerminalNoChange, nil
	default:
		return authoritystore.TerminalUnknown, fmt.Errorf("%w: terminal status %q", errMalformedBlob, token)
	}
}

// keyIdentityLP encodes the full EffectiveIdempotencyKey as one opaque LP
// blob: LP(subject) || LP(command) || LP(operation_id) || LP(root_kind) ||
// LP(canonical_root_id). The digest only locates a receipt; this blob is what
// Lua and Go compare before declaring replay vs conflict.
func keyIdentityLP(key authoritystore.IdempotencyKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", fmt.Errorf("%w: idempotency key: %v", authoritystore.ErrInvalidRequest, err)
	}
	subject := strconv.FormatUint(key.SubjectPlayerID, 10)
	var out []byte
	out = appendLP(out, subject)
	out = appendLP(out, key.StableCommandTypeName)
	out = appendLP(out, key.OperationID)
	out = appendLP(out, string(key.Root.Kind))
	out = appendLP(out, key.Root.CanonicalID)
	return string(out), nil
}

func parseKeyIdentity(blob string) (authoritystore.IdempotencyKey, error) {
	var out authoritystore.IdempotencyKey
	fields, err := parseLP([]byte(blob))
	if err != nil || len(fields) < 5 {
		return out, errMalformedBlob
	}
	if out.SubjectPlayerID, err = strconv.ParseUint(fields[0], 10, 64); err != nil {
		return out, fmt.Errorf("%w: subject", errMalformedBlob)
	}
	out.StableCommandTypeName = fields[1]
	out.OperationID = fields[2]
	out.Root = authoritystore.OperationRootRef{Kind: authoritystore.RootAuthorityKind(fields[3]), CanonicalID: fields[4]}
	if err := out.Validate(); err != nil {
		return authoritystore.IdempotencyKey{}, fmt.Errorf("%w: stored key identity", errMalformedBlob)
	}
	return out, nil
}

// receipt value = payload_hash(64 hex) || LP(key_identity) || LP(receipt_id) ||
// LP(terminal_status) || LP(error_code) || LP(outcome_payload) ||
// LP(first_seen_at_ms).
type decodedReceipt struct {
	Key            authoritystore.IdempotencyKey
	ReceiptID      string
	Status         authoritystore.TerminalStatus
	ErrorCode      string
	OutcomePayload []byte
	FirstSeenAtMs  int64
}

func parseReceiptValue(value string) (decodedReceipt, error) {
	var out decodedReceipt
	if len(value) < 64 {
		return out, errMalformedBlob
	}
	if _, err := parseDigestHex(value[:64]); err != nil {
		return out, err
	}
	fields, err := parseLP([]byte(value[64:]))
	if err != nil || len(fields) != 6 {
		return out, errMalformedBlob
	}
	if out.Key, err = parseKeyIdentity(fields[0]); err != nil {
		return out, err
	}
	out.ReceiptID = fields[1]
	if out.ReceiptID == "" {
		return decodedReceipt{}, fmt.Errorf("%w: receipt id", errMalformedBlob)
	}
	if out.Status, err = parseTerminalStatus(fields[2]); err != nil {
		return out, err
	}
	out.ErrorCode = fields[3]
	out.OutcomePayload = []byte(fields[4])
	out.FirstSeenAtMs, err = strconv.ParseInt(fields[5], 10, 64)
	if err != nil || out.FirstSeenAtMs <= 0 {
		return decodedReceipt{}, fmt.Errorf("%w: receipt timestamp", errMalformedBlob)
	}
	return out, nil
}

func (r decodedReceipt) outcome() authoritystore.StoredOperationOutcome {
	return authoritystore.StoredOperationOutcome{
		Status:    r.Status,
		ErrorCode: r.ErrorCode,
		Payload:   append([]byte(nil), r.OutcomePayload...),
	}
}

// resolution value = request_digest(64 hex) || LP fields in fenceResolutionOrder.
const fenceResolutionFieldCount = 16

type decodedResolution struct {
	Status             authoritystore.FenceResolutionStatus
	ResolveOperationID string
	OldWriterClaimID   string
	NewWriterClaimID   string
	OriginalDigest     string
	OriginalHash       string
	OriginalKey        authoritystore.IdempotencyKey
	BeforeVersion      uint64
	BeforeDigest       string
	CutVersion         uint64
	CutDigest          string
	OutStatus          string
	OutErrorCode       string
	OutPayload         string
	OriginalReceiptID  string
	CreatedAtMs        int64
}

func parseResolutionValue(value string) (decodedResolution, error) {
	var out decodedResolution
	if len(value) < 64 {
		return out, errMalformedBlob
	}
	fields, err := parseLP([]byte(value[64:]))
	if err != nil || len(fields) < fenceResolutionFieldCount {
		return out, errMalformedBlob
	}
	out.ResolveOperationID = fields[0]
	out.OldWriterClaimID = fields[1]
	out.NewWriterClaimID = fields[2]
	out.OriginalDigest = fields[3]
	out.OriginalHash = fields[4]
	if out.OriginalKey, err = parseKeyIdentity(fields[5]); err != nil {
		return out, err
	}
	if out.BeforeVersion, err = strconv.ParseUint(fields[6], 10, 64); err != nil {
		return out, fmt.Errorf("%w: before version", errMalformedBlob)
	}
	out.BeforeDigest = fields[7]
	if out.CutVersion, err = strconv.ParseUint(fields[8], 10, 64); err != nil {
		return out, fmt.Errorf("%w: cut version", errMalformedBlob)
	}
	out.CutDigest = fields[9]
	switch fields[10] {
	case "COMMITTED":
		out.Status = authoritystore.FenceResolutionCommitted
	case "NOT_COMMITTED":
		out.Status = authoritystore.FenceResolutionNotCommitted
	case "RECONCILE_REQUIRED":
		out.Status = authoritystore.FenceResolutionReconcileRequired
	default:
		return out, fmt.Errorf("%w: resolution status %q", errMalformedBlob, fields[10])
	}
	out.OutStatus = fields[11]
	out.OutErrorCode = fields[12]
	out.OutPayload = fields[13]
	out.OriginalReceiptID = fields[14]
	if ms, err := strconv.ParseInt(fields[15], 10, 64); err == nil {
		out.CreatedAtMs = ms
	}
	return out, nil
}

// resolutionRequestDigest binds a FenceAndClaim retry to its exact request so a
// replay with different parameters is RECONCILE_REQUIRED, never silently merged.
func resolutionRequestDigest(req authoritystore.FenceAndClaimRequest, originalDigest string) string {
	return sha256Hex(
		req.ResolveOperationID,
		string(req.ActorAddress.Domain)+":"+strconv.FormatUint(req.ActorAddress.ActorID, 10),
		req.OldWriterClaimID,
		req.NewWriterClaimID,
		originalDigest,
		digestHex(req.CanonicalPayloadHash),
		strconv.FormatUint(req.BeforeRecordVersion, 10),
		digestHex(req.BeforeBusinessStateDigest),
	)
}

// transitionIntentDigest binds a shard transition replay to the full intent.
func transitionIntentDigest(kind string, layout string, domain string, shard uint32, fence authoritystore.ShardWriteFence, newHolderID, newHolderBootID string, requestedTTLms int64) string {
	return authoritystore.ShardAuthorityTransitionIntentDigest(kind, layout, domain, shard, fence, newHolderID, newHolderBootID, requestedTTLms)
}

// resultEntryValue encodes a ResultDirectory entry as LP fields:
// command_type, operation_digest, root_kind, root_id, source_ref,
// final_status_class, minimal_outcome.
type ResultEntry struct {
	CommandType      string
	OperationDigest  string
	RootKind         string
	RootID           string
	SourceRef        string
	FinalStatusClass string
	MinimalOutcome   []byte
}

func encodeResultEntry(entry ResultEntry) string {
	var out []byte
	out = appendLP(out, entry.CommandType)
	out = appendLP(out, entry.OperationDigest)
	out = appendLP(out, entry.RootKind)
	out = appendLP(out, entry.RootID)
	out = appendLP(out, entry.SourceRef)
	out = appendLP(out, entry.FinalStatusClass)
	out = appendLP(out, string(entry.MinimalOutcome))
	return string(out)
}

func parseResultEntry(value string) (ResultEntry, error) {
	var out ResultEntry
	fields, err := parseLP([]byte(value))
	if err != nil || len(fields) < 7 {
		return out, errMalformedBlob
	}
	out.CommandType = fields[0]
	out.OperationDigest = fields[1]
	out.RootKind = fields[2]
	out.RootID = fields[3]
	out.SourceRef = fields[4]
	out.FinalStatusClass = fields[5]
	out.MinimalOutcome = []byte(fields[6])
	return out, nil
}

func asString(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

// scriptStrings converts a Lua array reply to strings, failing closed on
// unexpected element types.
func scriptStrings(reply interface{}) ([]string, error) {
	items, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: reply is not an array", errMalformedBlob)
	}
	out := make([]string, len(items))
	for i, item := range items {
		s, ok := asString(item)
		if !ok {
			return nil, fmt.Errorf("%w: reply element %d is not a string", errMalformedBlob, i)
		}
		out[i] = s
	}
	return out, nil
}
