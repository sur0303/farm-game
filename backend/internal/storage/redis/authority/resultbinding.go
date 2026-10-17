package authority

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
)

const resultBindingVersion = "result-binding-v1"

// ResultBinding is the self-contained identity and canonical-payload binding
// stored inside ResultEntry.MinimalOutcome. The ResultDirectory hash key still
// uses the frozen operation digest; these fields make a projection hit safe to
// validate without treating the projection as the operation authority.
type ResultBinding struct {
	SubjectPlayerID uint64
	CommandType     string
	OperationID     string
	PayloadHash     []byte
	Outcome         []byte
}

func EncodeResultBinding(binding ResultBinding) ([]byte, error) {
	if binding.SubjectPlayerID == 0 || binding.OperationID == "" || len(binding.OperationID) > 64 ||
		len(binding.PayloadHash) != sha256.Size || len(binding.Outcome) == 0 {
		return nil, fmt.Errorf("%w: result binding", errMalformedBlob)
	}
	if err := validateStableID("command_type", binding.CommandType); err != nil {
		return nil, err
	}
	fields := []string{resultBindingVersion, strconv.FormatUint(binding.SubjectPlayerID, 10), binding.CommandType,
		binding.OperationID, hex.EncodeToString(binding.PayloadHash), string(binding.Outcome)}
	var encoded []byte
	for _, field := range fields {
		encoded = appendLP(encoded, field)
	}
	checksum := sha256.Sum256(encoded)
	encoded = appendLP(encoded, hex.EncodeToString(checksum[:]))
	return encoded, nil
}

func DecodeResultBinding(encoded []byte) (ResultBinding, error) {
	var binding ResultBinding
	fields, err := parseLP(encoded)
	if err != nil || len(fields) != 7 || fields[0] != resultBindingVersion {
		return binding, errMalformedBlob
	}
	var body []byte
	for _, field := range fields[:6] {
		body = appendLP(body, field)
	}
	checksum := sha256.Sum256(body)
	if fields[6] != hex.EncodeToString(checksum[:]) {
		return binding, errMalformedBlob
	}
	subject, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || subject == 0 || strconv.FormatUint(subject, 10) != fields[1] {
		return binding, errMalformedBlob
	}
	if err := validateStableID("command_type", fields[2]); err != nil || fields[3] == "" || len(fields[3]) > 64 {
		return binding, errMalformedBlob
	}
	payloadHash, err := hex.DecodeString(fields[4])
	if err != nil || len(payloadHash) != sha256.Size || len(fields[5]) == 0 {
		return binding, errMalformedBlob
	}
	return ResultBinding{SubjectPlayerID: subject, CommandType: fields[2], OperationID: fields[3],
		PayloadHash: payloadHash, Outcome: []byte(fields[5])}, nil
}
