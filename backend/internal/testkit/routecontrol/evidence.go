package routecontrol

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// SchemaVersion identifies the JSON/NDJSON shape emitted by this package.
	SchemaVersion = "rt2q-route-control-evidence/v1"

	// Evidence levels are ordered by the strength of their provenance.
	EvidenceE1 EvidenceLevel = "E1"
	EvidenceE2 EvidenceLevel = "E2"
	EvidenceE3 EvidenceLevel = "E3"
	EvidenceE4 EvidenceLevel = "E4"

	// Short aliases keep call sites readable while retaining the typed values.
	E1 = EvidenceE1
	E2 = EvidenceE2
	E3 = EvidenceE3
	E4 = EvidenceE4
)

var (
	ErrInvalidEvidence            = errors.New("routecontrol: invalid evidence")
	ErrEvidenceEscalation         = errors.New("routecontrol: evidence escalation rejected")
	ErrAttestationRequired        = errors.New("routecontrol: real attestation required")
	ErrInvalidAttestation         = errors.New("routecontrol: invalid attestation")
	ErrInvalidManifest            = errors.New("routecontrol: invalid manifest")
	ErrArtifactMismatch           = errors.New("routecontrol: artifact digest mismatch")
	ErrKeyResidue                 = errors.New("routecontrol: exact key remains after cleanup")
	ErrProcessShutdownTimed       = errors.New("routecontrol: process shutdown timed out")
	ErrInvalidOracle              = errors.New("routecontrol: invalid oracle registration")
	ErrTrustedVerifierRequired    = errors.New("routecontrol: trusted verifier capability required")
	ErrRunAlreadyExists           = errors.New("routecontrol: run output already exists")
	ErrUnsafePath                 = errors.New("routecontrol: unsafe output path")
	ErrDurableEvidenceUnavailable = errors.New("routecontrol: durable evidence unavailable")
	// ErrCapability is the short capability-gate alias used by unsupported
	// platform tests and callers that do not need the evidence-specific name.
	ErrCapability                 = ErrDurableEvidenceUnavailable
	ErrInvalidProcessWriter       = errors.New("routecontrol: process output writer must be an *os.File or nil")
	ErrProcessGroupUnavailable    = errors.New("routecontrol: process-group identity capability unavailable")
	ErrProcessGroupIdentityReused = errors.New("routecontrol: original process-group identity was reused")
	ErrTimelinePoisoned           = errors.New("routecontrol: timeline stream is permanently poisoned")
	ErrGitCommandTimeout          = errors.New("routecontrol: git command timed out")
	ErrGitCommandFailed           = errors.New("routecontrol: git command failed")
	ErrGitDetached                = errors.New("routecontrol: detached HEAD")
	ErrCleanupInProgress          = errors.New("routecontrol: exact-key cleanup already in progress")
	ErrExactKeyRegistryClosed     = errors.New("routecontrol: exact-key registry is closed")
)

// SyntheticE1Commit is explicit non-git provenance for the local self-check.
// It is valid only for synthetic E1 evidence and can never support escalation.
const SyntheticE1Commit = "synthetic-e1"

// EvidenceLevel is the declared strength of an observation set.
type EvidenceLevel string

// Rank returns the ordered rank of a level. Invalid levels return zero.
func (l EvidenceLevel) Rank() int {
	switch l {
	case EvidenceE1:
		return 1
	case EvidenceE2:
		return 2
	case EvidenceE3:
		return 3
	case EvidenceE4:
		return 4
	default:
		return 0
	}
}

// Valid reports whether l is one of E1 through E4.
func (l EvidenceLevel) Valid() bool {
	return l.Rank() != 0
}

// EvidenceStatus is the status of the evidence record itself. It is not a
// route-control or business decision.
type EvidenceStatus string

const (
	StatusPass         EvidenceStatus = "PASS"
	StatusFail         EvidenceStatus = "FAIL"
	StatusInconclusive EvidenceStatus = "INCONCLUSIVE"
)

func (s EvidenceStatus) valid() bool {
	switch s {
	case StatusPass, StatusFail, StatusInconclusive:
		return true
	default:
		return false
	}
}

// SourceKind identifies the strongest source that directly produced a record.
// A lower source kind cannot be relabeled as a higher evidence level.
type SourceKind string

const (
	SourceSelfCheck           SourceKind = "local-self-check"
	SourceFixture             SourceKind = "controlled-fixture"
	SourceRealProcess         SourceKind = "real-process"
	SourceExternalAttestation SourceKind = "external-attestation"
)

func (s SourceKind) maxLevel() EvidenceLevel {
	switch s {
	case SourceSelfCheck:
		return EvidenceE1
	case SourceFixture:
		return EvidenceE2
	case SourceRealProcess:
		return EvidenceE3
	case SourceExternalAttestation:
		return EvidenceE4
	default:
		return ""
	}
}

// Provenance is required on manifests and timeline records. RecordedAt is an
// RFC3339 timestamp so records remain portable across test runners.
type Provenance struct {
	TaskID        string        `json:"task_id"`
	RunID         string        `json:"run_id"`
	BaseCommit    string        `json:"base_commit"`
	HeadCommit    string        `json:"head_commit"`
	Branch        string        `json:"branch"`
	Worktree      string        `json:"worktree"`
	Collector     string        `json:"collector"`
	SourceKind    SourceKind    `json:"source_kind"`
	DeclaredLevel EvidenceLevel `json:"declared_level"`
	RecordedAt    string        `json:"recorded_at"`
	Synthetic     bool          `json:"synthetic"`
}

// Redacted returns provenance with free-text fields sanitized for evidence
// output. The same transformation is used for manifest top-level bindings.
func (p Provenance) Redacted(redactor *Redactor) Provenance {
	return Provenance{
		TaskID:        redactor.Redact(p.TaskID),
		RunID:         redactor.Redact(p.RunID),
		BaseCommit:    redactor.Redact(p.BaseCommit),
		HeadCommit:    redactor.Redact(p.HeadCommit),
		Branch:        redactor.Redact(p.Branch),
		Worktree:      redactor.Redact(p.Worktree),
		Collector:     redactor.Redact(p.Collector),
		SourceKind:    p.SourceKind,
		DeclaredLevel: p.DeclaredLevel,
		RecordedAt:    p.RecordedAt,
		Synthetic:     p.Synthetic,
	}
}

// ValidateFor validates that p can directly support level.
func (p Provenance) ValidateFor(level EvidenceLevel) error {
	if !level.Valid() {
		return fmt.Errorf("%w: unknown level %q", ErrInvalidEvidence, level)
	}
	required := map[string]string{
		"task_id":     p.TaskID,
		"run_id":      p.RunID,
		"base_commit": p.BaseCommit,
		"head_commit": p.HeadCommit,
		"branch":      p.Branch,
		"worktree":    p.Worktree,
		"collector":   p.Collector,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: provenance %s is required", ErrInvalidEvidence, name)
		}
	}
	if p.DeclaredLevel != level {
		return fmt.Errorf("%w: provenance declares %s for requested %s", ErrEvidenceEscalation, p.DeclaredLevel, level)
	}
	if p.SourceKind.maxLevel().Rank() < level.Rank() {
		return fmt.Errorf("%w: source %s cannot support %s", ErrEvidenceEscalation, p.SourceKind, level)
	}
	if _, err := time.Parse(time.RFC3339Nano, p.RecordedAt); err != nil {
		return fmt.Errorf("%w: provenance recorded_at: %v", ErrInvalidEvidence, err)
	}
	if p.Synthetic {
		if level != EvidenceE1 || p.SourceKind != SourceSelfCheck ||
			p.BaseCommit != SyntheticE1Commit || p.HeadCommit != SyntheticE1Commit {
			return fmt.Errorf("%w: synthetic provenance is limited to E1 self-check", ErrEvidenceEscalation)
		}
	} else {
		if !isCommitToken(p.BaseCommit) || !isCommitToken(p.HeadCommit) {
			return fmt.Errorf("%w: provenance commits must be 7-64 hex characters", ErrInvalidEvidence)
		}
		if err := validateBoundGitHead(p); err != nil {
			return err
		}
	}
	return nil
}

// Attestation is an externally supplied witness reference. The scaffold never
// fabricates one and never treats a local self-check as an attestation.
type Attestation struct {
	ID             string        `json:"id"`
	Issuer         string        `json:"issuer"`
	Level          EvidenceLevel `json:"level"`
	ArtifactSHA256 string        `json:"artifact_sha256"`
	Real           bool          `json:"real"`
	Verified       bool          `json:"verified"`
}

// ValidateFor checks the minimum requirements for a real attestation at level.
func (a Attestation) ValidateFor(level EvidenceLevel) error {
	if !level.Valid() {
		return fmt.Errorf("%w: unknown level %q", ErrInvalidAttestation, level)
	}
	if strings.TrimSpace(a.ID) == "" || strings.TrimSpace(a.Issuer) == "" {
		return fmt.Errorf("%w: id and issuer are required", ErrInvalidAttestation)
	}
	if a.Level.Rank() < level.Rank() {
		return fmt.Errorf("%w: attestation level %s is below %s", ErrEvidenceEscalation, a.Level, level)
	}
	if !validSHA256(a.ArtifactSHA256) {
		return fmt.Errorf("%w: artifact_sha256 must be 64 lowercase hex characters", ErrInvalidAttestation)
	}
	if !a.Real || !a.Verified {
		return ErrAttestationRequired
	}
	return nil
}

// Claim records only evidence status and its provenance. It deliberately has
// no route-control outcome fields or adjudication callback.
type Claim struct {
	Level       EvidenceLevel  `json:"level"`
	Status      EvidenceStatus `json:"status"`
	Reason      string         `json:"reason"`
	Provenance  Provenance     `json:"provenance"`
	Attestation *Attestation   `json:"attestation,omitempty"`
}

// Validate enforces provenance monotonicity and the E3/E4 attestation gate.
func (c Claim) Validate() error {
	if !c.Level.Valid() {
		return fmt.Errorf("%w: unknown level %q", ErrInvalidEvidence, c.Level)
	}
	if !c.Status.valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidEvidence, c.Status)
	}
	if strings.TrimSpace(c.Reason) == "" {
		return fmt.Errorf("%w: reason is required", ErrInvalidEvidence)
	}
	if err := c.Provenance.ValidateFor(c.Level); err != nil {
		return err
	}

	requiresAttestation := c.Level.Rank() >= EvidenceE3.Rank()
	if requiresAttestation && c.Status == StatusPass {
		return ErrTrustedVerifierRequired
	}
	if c.Attestation != nil {
		if err := c.Attestation.ValidateFor(c.Level); err != nil {
			if c.Status == StatusInconclusive && errors.Is(err, ErrAttestationRequired) {
				return nil
			}
			return err
		}
	}
	if requiresAttestation && c.Attestation == nil {
		if c.Status == StatusInconclusive {
			return nil
		}
		return ErrAttestationRequired
	}
	if requiresAttestation && c.Attestation != nil {
		if err := c.Attestation.ValidateFor(c.Level); err != nil {
			if c.Status == StatusInconclusive && errors.Is(err, ErrAttestationRequired) {
				return nil
			}
			return err
		}
	}
	return nil
}

// CanPass is a structural gate only. It does not evaluate route behavior.
func (c Claim) CanPass() bool {
	if c.Level.Rank() >= EvidenceE3.Rank() {
		return false
	}
	return c.Status == StatusPass && c.Validate() == nil
}

func isCommitToken(value string) bool {
	if len(value) < 7 || len(value) > 64 {
		return false
	}
	allZero := true
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
			if r != '0' {
				allZero = false
			}
		case r >= 'a' && r <= 'f':
			allZero = false
		case r >= 'A' && r <= 'F':
			allZero = false
		default:
			return false
		}
	}
	return !allZero
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return false
	}
	return strings.ToLower(value) == value
}
