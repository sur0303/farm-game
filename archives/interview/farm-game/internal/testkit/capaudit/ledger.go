package capaudit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// LedgerSchema is the only ledger shape this audit accepts.
const LedgerSchema = "cap-actor-ledger.v1"

// Ledger names the actors a capacity run can touch.
//
// Redis has no actor index, and at full scale a sweep of the keyspace is out of
// the question, so an audit that reads per-actor state needs to be told which
// actors to read. The load generator already decides this: it derives each
// identity as a base plus the arrival number, and uses the same id for a player
// and that player's farm. So the ledger is a range, not a list of millions of
// ids, and it is authoritative by construction rather than by collection.
//
// It carries the run id because auditing one run against another run's ledger
// would read the wrong actors and find them clean.
type Ledger struct {
	SchemaVersion string `json:"schema_version"`
	RunID         string `json:"run_id"`
	// ActorBase is the first identity the generator uses.
	ActorBase uint64 `json:"actor_base"`
	// ActorCount is how many consecutive identities it may use.
	ActorCount uint64 `json:"actor_count"`
}

func LoadLedger(path string, expectedRunID string) (Ledger, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Ledger{}, fmt.Errorf("read actor ledger: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var ledger Ledger
	if err := decoder.Decode(&ledger); err != nil {
		return Ledger{}, fmt.Errorf("decode actor ledger: %w", err)
	}
	if err := ledger.validate(); err != nil {
		return Ledger{}, err
	}
	if expectedRunID != "" && ledger.RunID != expectedRunID {
		return Ledger{}, fmt.Errorf("actor ledger belongs to run %s, not %s", ledger.RunID, expectedRunID)
	}
	return ledger, nil
}

func (l Ledger) validate() error {
	if l.SchemaVersion != LedgerSchema {
		return fmt.Errorf("unsupported actor ledger schema %q", l.SchemaVersion)
	}
	if l.RunID == "" {
		return errors.New("actor ledger must name the run it belongs to")
	}
	if l.ActorBase == 0 || l.ActorCount == 0 {
		return errors.New("actor ledger must cover at least one identity")
	}
	return nil
}

// Actors calls visit for every identity in the run, stopping at the first
// error.
func (l Ledger) Actors(visit func(actorID uint64) error) error {
	for offset := uint64(0); offset < l.ActorCount; offset++ {
		if err := visit(l.ActorBase + offset); err != nil {
			return err
		}
	}
	return nil
}
