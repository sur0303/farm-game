package workflow

import (
	"errors"
	"strings"

	"farmgame/internal/workidentity"
)

// WorkKeys addresses the generic work store. It is exported so that a reader
// outside this package, such as a correctness audit, reaches the same records
// the store writes instead of rebuilding the key strings and drifting from
// them silently.
type WorkKeys struct {
	prefix string
}

func NewWorkKeys(prefix string) (WorkKeys, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return WorkKeys{}, errors.New("workflow: work store prefix required")
	}
	return WorkKeys{prefix: prefix}, nil
}

func (k WorkKeys) base() string { return k.prefix + ":{work-v1}" }

// KeyPrefix is the text every key of this store opens with, for a reader that
// has to address the store as a whole rather than one record in it.
func (k WorkKeys) KeyPrefix() string { return k.base() }

// Due is the index of records waiting to run, scored by their due time.
func (k WorkKeys) Due() string { return k.base() + ":due" }

// RecordByRef addresses a record by the reference used as a Due member, so a
// reader can walk the index straight into the records it names.
func (k WorkKeys) RecordByRef(ref string) string { return k.base() + ":record:" + ref }

func (k WorkKeys) Record(workType, workID string) (string, error) {
	ref, err := workidentity.EncodeWorkRefV1(workType, workID)
	if err != nil {
		return "", err
	}
	return k.RecordByRef(ref), nil
}
