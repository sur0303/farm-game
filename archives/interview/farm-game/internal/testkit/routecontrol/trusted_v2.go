package routecontrol

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TrustedSchemaVersion is deliberately separate from the Q0 v1 scaffold.
// The v1 Claim.CanPass gate remains permanently closed for E3/E4; trusted v2
// verdicts are derived by VerifyTrustedRun from immutable typed observations.
const TrustedSchemaVersion = "rt2q-route-control-evidence/v2"

const TrustedOracleVersion = "rt2q-derived-oracle/2.0.0"

var trustedArtifactNames = []string{
	"provenance.json",
	"binaries.json",
	"configs.json",
	"processes.ndjson",
	"tls-peers.ndjson",
	"snapshots.ndjson",
	"plans.ndjson",
	"authority.ndjson",
	"holders.ndjson",
	"gate-observations.ndjson",
	"rpc-faults.ndjson",
	"redis-faults.ndjson",
	"business.ndjson",
	"routectl.ndjson",
	"cleanup.ndjson",
	"oracle.json",
}

var trustedNDJSONNames = map[string]struct{}{
	"processes.ndjson":         {},
	"tls-peers.ndjson":         {},
	"snapshots.ndjson":         {},
	"plans.ndjson":             {},
	"authority.ndjson":         {},
	"holders.ndjson":           {},
	"gate-observations.ndjson": {},
	"rpc-faults.ndjson":        {},
	"redis-faults.ndjson":      {},
	"business.ndjson":          {},
	"routectl.ndjson":          {},
	"cleanup.ndjson":           {},
}

// GitBinding is the exact source identity required by trusted evidence.
type GitBinding struct {
	HeadCommit string `json:"head_commit"`
	TreeHash   string `json:"tree_hash"`
	Detached   bool   `json:"detached"`
	Clean      bool   `json:"clean"`
	Modified   bool   `json:"modified"`
}

// TrustedBindings are independently recomputable set/digest bindings.
type TrustedBindings struct {
	ConfigSetSHA256      string `json:"config_set_sha256"`
	ProcessSetSHA256     string `json:"process_set_sha256"`
	TLSPeerSetSHA256     string `json:"tls_peer_set_sha256"`
	GatePeerSetSHA256    string `json:"gate_peer_set_sha256"`
	RedisKeyLedgerSHA256 string `json:"redis_key_ledger_sha256"`
	OracleSHA256         string `json:"oracle_sha256"`
	OracleVersion        string `json:"oracle_version"`
}

// ExternalSignature is E4 reviewer material. There is no trusted boolean:
// verification is an Ed25519 operation over ManifestSHA256.
type ExternalSignature struct {
	Issuer         string `json:"issuer"`
	KeyID          string `json:"key_id"`
	Algorithm      string `json:"algorithm"`
	PublicKey      string `json:"public_key_base64"`
	Signature      string `json:"signature_base64"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// TrustedManifest is the v2 run root. It never accepts a caller-provided pass
// flag; Status is checked against a newly derived OracleResult during read.
type TrustedManifest struct {
	Schema        string             `json:"schema"`
	TaskID        string             `json:"task_id"`
	RunID         string             `json:"run_id"`
	Challenge     string             `json:"challenge"`
	EvidenceLevel EvidenceLevel      `json:"evidence_level"`
	Runtime       string             `json:"runtime"`
	RedisMode     string             `json:"redis_mode"`
	Scenarios     []string           `json:"scenarios"`
	Status        EvidenceStatus     `json:"status"`
	Reason        string             `json:"reason"`
	Git           GitBinding         `json:"git"`
	Bindings      TrustedBindings    `json:"bindings"`
	Artifacts     []Artifact         `json:"artifacts"`
	Signature     *ExternalSignature `json:"external_signature,omitempty"`
}

// TrustedProvenance is written as provenance.json before any runtime fact.
type TrustedProvenance struct {
	Schema          string        `json:"schema"`
	TaskID          string        `json:"task_id"`
	RunID           string        `json:"run_id"`
	Challenge       string        `json:"challenge"`
	EvidenceLevel   EvidenceLevel `json:"evidence_level"`
	Runtime         string        `json:"runtime"`
	RedisMode       string        `json:"redis_mode"`
	Scenarios       []string      `json:"scenarios"`
	Git             GitBinding    `json:"git"`
	Collector       string        `json:"collector"`
	CollectorSHA256 string        `json:"collector_sha256"`
	StartedAt       string        `json:"started_at"`
	Keyspace        string        `json:"keyspace_fingerprint"`
	Source          SourceKind    `json:"source_kind"`
}

// TrustedRecord is one member of the run-global hash chain. Sequence is
// global across all NDJSON files, so moving a valid line between artifacts or
// splicing independently generated streams is detected by offline verify.
type TrustedRecord struct {
	Schema        string          `json:"schema"`
	RunID         string          `json:"run_id"`
	Challenge     string          `json:"challenge"`
	ScenarioID    string          `json:"scenario_id"`
	Stream        string          `json:"stream"`
	Kind          string          `json:"kind"`
	Sequence      uint64          `json:"sequence"`
	WallTime      string          `json:"wall_time"`
	MonotonicNano int64           `json:"monotonic_ns"`
	Observer      ProcessIdentity `json:"observer"`
	PreviousHash  string          `json:"previous_hash"`
	Payload       json.RawMessage `json:"payload"`
	CurrentHash   string          `json:"current_hash"`
}

type ProcessIdentity struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	PID           int    `json:"pid,omitempty"`
	ContainerID   string `json:"container_id,omitempty"`
	BinarySHA256  string `json:"binary_sha256"`
	StartIdentity string `json:"start_identity"`
}

// OracleClause is the complete stable output of one derived predicate.
type OracleClause struct {
	ID      string         `json:"id"`
	Status  EvidenceStatus `json:"status"`
	Reason  string         `json:"reason"`
	Records []uint64       `json:"record_sequences,omitempty"`
}

type OracleResult struct {
	Schema        string         `json:"schema"`
	OracleVersion string         `json:"oracle_version"`
	OracleSHA256  string         `json:"oracle_sha256"`
	RunID         string         `json:"run_id"`
	Challenge     string         `json:"challenge"`
	Status        EvidenceStatus `json:"status"`
	Reason        string         `json:"reason"`
	Clauses       []OracleClause `json:"clauses"`
	RecordCount   uint64         `json:"record_count"`
	ChainHead     string         `json:"chain_head"`
}

// TrustedRunOptions contains only run identity/configuration, never a result.
type TrustedRunOptions struct {
	Root            string
	TaskID          string
	RunID           string
	Challenge       string
	Level           EvidenceLevel
	Runtime         string
	RedisMode       string
	Scenarios       []string
	Git             GitBinding
	Collector       string
	CollectorSHA256 string
	Keyspace        string
	Now             func() time.Time
	Redactor        *Redactor
}

// TrustedRunWriter owns all original file and directory capabilities until
// manifest installation. It is safe for concurrent observations.
type TrustedRunWriter struct {
	mu        sync.Mutex
	root      secureRunDir
	opts      TrustedRunOptions
	files     map[string]*os.File
	sequence  uint64
	chainHead string
	started   time.Time
	now       func() time.Time
	redactor  *Redactor
	closed    bool
	poison    error
}

func NewTrustedRunWriter(options TrustedRunOptions) (*TrustedRunWriter, error) {
	if err := validateTrustedRunOptions(options); err != nil {
		return nil, err
	}
	runDir, err := createRunDirectory(options.Root)
	if err != nil {
		return nil, err
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	redactor := options.Redactor
	if redactor == nil {
		redactor = NewRedactor()
	}
	w := &TrustedRunWriter{
		root: runDir, opts: options, files: make(map[string]*os.File, len(trustedArtifactNames)),
		started: now(), now: now, redactor: redactor,
	}
	created := make([]string, 0, len(trustedArtifactNames))
	for _, name := range trustedArtifactNames {
		file, createErr := runDir.createFile(name)
		if createErr != nil {
			_ = w.closeFilesLocked()
			_ = runDir.cleanup(created)
			return nil, fmt.Errorf("create trusted artifact %s: %w", name, createErr)
		}
		w.files[name] = file
		created = append(created, name)
	}
	provenance := TrustedProvenance{
		Schema: TrustedSchemaVersion, TaskID: options.TaskID, RunID: options.RunID,
		Challenge: options.Challenge, EvidenceLevel: options.Level, Runtime: options.Runtime,
		RedisMode: options.RedisMode, Scenarios: append([]string(nil), options.Scenarios...),
		Git: options.Git, Collector: options.Collector, CollectorSHA256: options.CollectorSHA256,
		StartedAt: w.started.UTC().Format(time.RFC3339Nano), Keyspace: options.Keyspace,
		Source: SourceRealProcess,
	}
	if options.Level == EvidenceE4 {
		provenance.Source = SourceExternalAttestation
	}
	if err := w.writeJSONLocked("provenance.json", provenance); err != nil {
		_ = w.closeFilesLocked()
		_ = runDir.cleanup(created)
		return nil, err
	}
	return w, nil
}

func validateTrustedRunOptions(options TrustedRunOptions) error {
	if strings.TrimSpace(options.Root) == "" || !safeToken(options.TaskID) || !safeToken(options.RunID) ||
		!validChallenge(options.Challenge) || (options.Level != EvidenceE3 && options.Level != EvidenceE4) ||
		(options.Runtime != "process" && options.Runtime != "container") ||
		(options.RedisMode != "isolated" && options.RedisMode != "company") || len(options.Scenarios) == 0 ||
		strings.TrimSpace(options.Collector) == "" || !validSHA256(options.CollectorSHA256) ||
		strings.TrimSpace(options.Keyspace) == "" {
		return fmt.Errorf("%w: invalid trusted run options", ErrInvalidEvidence)
	}
	if err := validateGitBinding(options.Git); err != nil {
		return err
	}
	want := append([]string(nil), options.Scenarios...)
	sort.Strings(want)
	for index, scenario := range want {
		if !validScenario(scenario) || (index > 0 && want[index-1] == scenario) {
			return fmt.Errorf("%w: invalid scenario set", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateGitBinding(binding GitBinding) error {
	if !isCommitToken(binding.HeadCommit) || !isCommitToken(binding.TreeHash) ||
		!binding.Detached || !binding.Clean || binding.Modified {
		return fmt.Errorf("%w: trusted evidence requires exact clean detached Git HEAD/tree", ErrInvalidEvidence)
	}
	return nil
}

func validChallenge(value string) bool {
	if len(value) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16 && value == strings.ToLower(value) && value != strings.Repeat("0", 32)
}

func validScenario(value string) bool {
	if value == "S7" || value == "deadline-refresh" {
		return true
	}
	return len(value) == 2 && value[0] == 'S' && value[1] >= '0' && value[1] <= '6'
}

func NewRunChallenge() (string, error) {
	var nonce [16]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

// Observe appends a typed fact. The caller chooses a required artifact stream,
// but cannot supply sequence, timestamps, hashes, status, or verdict fields.
func (w *TrustedRunWriter) Observe(stream, scenario, kind string, observer ProcessIdentity, payload any) error {
	if _, ok := trustedNDJSONNames[stream]; !ok || !validScenario(scenario) || !safeToken(kind) {
		return fmt.Errorf("%w: invalid trusted observation routing", ErrInvalidEvidence)
	}
	if err := validateProcessIdentity(observer); err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode trusted observation payload: %w", err)
	}
	raw = []byte(w.redactor.Redact(string(raw)))
	if !json.Valid(raw) {
		return fmt.Errorf("%w: redaction invalidated observation JSON", ErrInvalidEvidence)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("trusted run writer is closed")
	}
	if w.poison != nil {
		return fmt.Errorf("%w: %v", ErrTimelinePoisoned, w.poison)
	}
	w.sequence++
	now := w.now()
	record := TrustedRecord{
		Schema: TrustedSchemaVersion, RunID: w.opts.RunID, Challenge: w.opts.Challenge,
		ScenarioID: scenario, Stream: stream, Kind: kind, Sequence: w.sequence,
		WallTime: now.UTC().Format(time.RFC3339Nano), MonotonicNano: now.Sub(w.started).Nanoseconds(),
		Observer: observer, PreviousHash: w.chainHead, Payload: raw,
	}
	if record.Sequence == 1 {
		record.PreviousHash = strings.Repeat("0", 64)
	}
	record.CurrentHash, err = trustedRecordHash(record)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	file := w.files[stream]
	if file == nil {
		return w.poisonLocked(fmt.Errorf("trusted stream %s is unavailable", stream))
	}
	written, writeErr := timelineWrite(file, encoded)
	if writeErr != nil || written != len(encoded) {
		return w.poisonLocked(errors.Join(writeErr, io.ErrShortWrite))
	}
	w.chainHead = record.CurrentHash
	return nil
}

func validateProcessIdentity(identity ProcessIdentity) error {
	if !safeToken(identity.Name) || !safeToken(identity.Role) || !validSHA256(identity.BinarySHA256) ||
		strings.TrimSpace(identity.StartIdentity) == "" || (identity.PID <= 1 && identity.ContainerID == "") ||
		(identity.PID > 1 && identity.ContainerID != "") {
		return fmt.Errorf("%w: invalid observer process identity", ErrInvalidEvidence)
	}
	if identity.ContainerID != "" && !isCommitToken(identity.ContainerID) {
		return fmt.Errorf("%w: invalid observer container identity", ErrInvalidEvidence)
	}
	return nil
}

func trustedRecordHash(record TrustedRecord) (string, error) {
	record.CurrentHash = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (w *TrustedRunWriter) WriteJSON(name string, value any) error {
	if name != "binaries.json" && name != "configs.json" {
		return fmt.Errorf("%w: JSON artifact %s is not caller-writable", ErrInvalidEvidence, name)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.poison != nil {
		return errors.New("trusted run writer is unavailable")
	}
	return w.writeJSONLocked(name, value)
}

func (w *TrustedRunWriter) writeJSONLocked(name string, value any) error {
	file := w.files[name]
	if file == nil {
		return fmt.Errorf("%w: trusted artifact %s unavailable", ErrInvalidEvidence, name)
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	redacted := []byte(w.redactor.Redact(encoded.String()))
	if !json.Valid(redacted) {
		return fmt.Errorf("%w: redaction invalidated %s", ErrInvalidEvidence, name)
	}
	if info, err := file.Stat(); err != nil || info.Size() != 0 {
		return fmt.Errorf("%w: trusted JSON artifact %s is not empty", ErrRunAlreadyExists, name)
	}
	written, err := timelineWrite(file, redacted)
	if err != nil || written != len(redacted) {
		return w.poisonLocked(errors.Join(err, io.ErrShortWrite))
	}
	return nil
}

func (w *TrustedRunWriter) poisonLocked(err error) error {
	if err == nil {
		err = ErrTimelinePoisoned
	}
	if w.poison == nil {
		w.poison = err
	} else {
		w.poison = errors.Join(w.poison, err)
	}
	return fmt.Errorf("%w: %v", ErrTimelinePoisoned, err)
}

// Finalize derives the oracle from the held files, writes oracle.json, hashes
// every regular artifact, and installs run-manifest.json. A manifest status is
// never accepted as input.
func (w *TrustedRunWriter) Finalize(bindings TrustedBindings, signature *ExternalSignature) (OracleResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return OracleResult{}, errors.New("trusted run writer is closed")
	}
	defer func() {
		w.closed = true
		_ = w.closeFilesLocked()
		_ = w.root.close()
	}()
	if w.poison != nil {
		return OracleResult{}, fmt.Errorf("%w: %v", ErrTimelinePoisoned, w.poison)
	}
	for name, file := range w.files {
		if name == "oracle.json" {
			continue
		}
		if err := timelineSync(file); err != nil {
			return OracleResult{}, fmt.Errorf("sync %s: %w", name, err)
		}
	}
	records, err := readTrustedRecordsFromOpenFiles(w.files, w.opts.RunID, w.opts.Challenge)
	if err != nil {
		return OracleResult{}, err
	}
	oracle := deriveOracle(w.opts.RunID, w.opts.Challenge, w.opts.Scenarios, records, bindings.OracleSHA256)
	bindings.OracleVersion = TrustedOracleVersion
	if err := w.writeJSONLocked("oracle.json", oracle); err != nil {
		return OracleResult{}, err
	}
	artifacts := make([]Artifact, 0, len(trustedArtifactNames))
	for _, name := range trustedArtifactNames {
		file := w.files[name]
		artifact, hashErr := computeOpenArtifact(file, name, name)
		if hashErr != nil {
			return OracleResult{}, fmt.Errorf("hash %s: %w", name, hashErr)
		}
		artifacts = append(artifacts, artifact)
	}
	manifest := TrustedManifest{
		Schema: TrustedSchemaVersion, TaskID: w.opts.TaskID, RunID: w.opts.RunID,
		Challenge: w.opts.Challenge, EvidenceLevel: w.opts.Level, Runtime: w.opts.Runtime,
		RedisMode: w.opts.RedisMode, Scenarios: append([]string(nil), w.opts.Scenarios...),
		Status: oracle.Status, Reason: oracle.Reason, Git: w.opts.Git, Bindings: bindings,
		Artifacts: artifacts, Signature: signature,
	}
	if w.opts.Level == EvidenceE4 && signature == nil && manifest.Status == StatusPass {
		manifest.Status = StatusInconclusive
		manifest.Reason = "independent E4 manifest signature is required"
	}
	if err := validateTrustedManifest(manifest, false); err != nil {
		return OracleResult{}, err
	}
	encoded, err := encodeIndented(manifest)
	if err != nil {
		return OracleResult{}, err
	}
	if err := w.root.createManifest("run-manifest.json", encoded); err != nil {
		return OracleResult{}, err
	}
	return oracle, nil
}

func (w *TrustedRunWriter) closeFilesLocked() error {
	var joined error
	for _, name := range trustedArtifactNames {
		if file := w.files[name]; file != nil {
			joined = errors.Join(joined, file.Close())
			w.files[name] = nil
		}
	}
	return joined
}

func encodeIndented(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func readTrustedRecordsFromOpenFiles(files map[string]*os.File, runID, challenge string) ([]TrustedRecord, error) {
	var records []TrustedRecord
	for _, name := range trustedArtifactNames {
		if _, ok := trustedNDJSONNames[name]; !ok {
			continue
		}
		file := files[name]
		if file == nil {
			return nil, fmt.Errorf("trusted stream %s is closed", name)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		for scanner.Scan() {
			var record TrustedRecord
			if err := strictJSON(scanner.Bytes(), &record); err != nil {
				return nil, fmt.Errorf("decode %s: %w", name, err)
			}
			if record.Stream != name || record.RunID != runID || record.Challenge != challenge {
				return nil, fmt.Errorf("%w: record binding mismatch in %s", ErrInvalidEvidence, name)
			}
			records = append(records, record)
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		_, _ = file.Seek(0, io.SeekEnd)
	}
	if err := validateTrustedChain(records, runID, challenge); err != nil {
		return nil, err
	}
	return records, nil
}

func validateTrustedChain(records []TrustedRecord, runID, challenge string) error {
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	previous := strings.Repeat("0", 64)
	var previousMono int64 = -1
	for index, record := range records {
		if record.Schema != TrustedSchemaVersion || record.RunID != runID || record.Challenge != challenge ||
			record.Sequence != uint64(index+1) || record.PreviousHash != previous || !validScenario(record.ScenarioID) ||
			!safeToken(record.Kind) || !json.Valid(record.Payload) || record.MonotonicNano < previousMono {
			return fmt.Errorf("%w: invalid trusted record sequence %d", ErrInvalidEvidence, record.Sequence)
		}
		if _, err := time.Parse(time.RFC3339Nano, record.WallTime); err != nil {
			return fmt.Errorf("%w: invalid record wall time", ErrInvalidEvidence)
		}
		if err := validateProcessIdentity(record.Observer); err != nil {
			return err
		}
		digest, err := trustedRecordHash(record)
		if err != nil || digest != record.CurrentHash {
			return fmt.Errorf("%w: trusted record hash mismatch at %d", ErrArtifactMismatch, record.Sequence)
		}
		previous = digest
		previousMono = record.MonotonicNano
	}
	return nil
}

func strictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

// VerifyTrustedRun is the offline trusted verifier used by the stable verify
// CLI. It reopens through secure descriptor-relative APIs, enumerates every
// regular file, rehashes the exact manifest set, validates the global chain,
// and derives a new oracle result.
func VerifyTrustedRun(root, expectedHead string) (TrustedManifest, OracleResult, error) {
	rootDir, err := openSecureRoot(root)
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	defer rootDir.close()
	manifestFile, err := rootDir.openRegular("run-manifest.json")
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	raw, err := io.ReadAll(manifestFile)
	_ = manifestFile.Close()
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	var manifest TrustedManifest
	if err := strictJSON(raw, &manifest); err != nil {
		return TrustedManifest{}, OracleResult{}, fmt.Errorf("decode trusted manifest: %w", err)
	}
	if manifest.Git.HeadCommit != strings.ToLower(expectedHead) {
		return TrustedManifest{}, OracleResult{}, fmt.Errorf("%w: manifest HEAD %s does not match expected %s", ErrInvalidEvidence, manifest.Git.HeadCommit, expectedHead)
	}
	if err := validateTrustedManifest(manifest, true); err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	if err := validateExactArtifactSet(root, manifest.Artifacts); err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	for _, artifact := range manifest.Artifacts {
		file, openErr := rootDir.openRegular(artifact.Path)
		if openErr != nil {
			return TrustedManifest{}, OracleResult{}, openErr
		}
		digest, size, hashErr := hashRegularFile(file)
		if hashErr != nil || digest != artifact.SHA256 || size != artifact.Bytes {
			return TrustedManifest{}, OracleResult{}, fmt.Errorf("%w: %s", ErrArtifactMismatch, artifact.Path)
		}
	}
	records, err := readTrustedRecordsFromRoot(rootDir, manifest)
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	derived := deriveOracle(manifest.RunID, manifest.Challenge, manifest.Scenarios, records, manifest.Bindings.OracleSHA256)
	oracleFile, err := rootDir.openRegular("oracle.json")
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	oracleRaw, err := io.ReadAll(oracleFile)
	_ = oracleFile.Close()
	if err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	var stored OracleResult
	if err := strictJSON(oracleRaw, &stored); err != nil {
		return TrustedManifest{}, OracleResult{}, err
	}
	derivedRaw, _ := json.Marshal(derived)
	storedRaw, _ := json.Marshal(stored)
	if !bytes.Equal(derivedRaw, storedRaw) {
		return TrustedManifest{}, OracleResult{}, fmt.Errorf("%w: stored oracle is not derived from typed records", ErrArtifactMismatch)
	}
	wantStatus, wantReason := derived.Status, derived.Reason
	if manifest.EvidenceLevel == EvidenceE4 && manifest.Signature == nil && wantStatus == StatusPass {
		wantStatus, wantReason = StatusInconclusive, "independent E4 manifest signature is required"
	}
	if manifest.Status != wantStatus || manifest.Reason != wantReason {
		return TrustedManifest{}, OracleResult{}, fmt.Errorf("%w: manifest verdict differs from derived oracle", ErrInvalidManifest)
	}
	if manifest.Signature != nil {
		if err := verifyExternalSignature(manifest); err != nil {
			return TrustedManifest{}, OracleResult{}, err
		}
	}
	return manifest, derived, nil
}

func validateTrustedManifest(manifest TrustedManifest, requireSignatureCheck bool) error {
	if manifest.Schema != TrustedSchemaVersion || !safeToken(manifest.TaskID) || !safeToken(manifest.RunID) ||
		!validChallenge(manifest.Challenge) || (manifest.EvidenceLevel != EvidenceE3 && manifest.EvidenceLevel != EvidenceE4) ||
		(manifest.Runtime != "process" && manifest.Runtime != "container") ||
		(manifest.RedisMode != "isolated" && manifest.RedisMode != "company") ||
		!manifest.Status.valid() || strings.TrimSpace(manifest.Reason) == "" || len(manifest.Scenarios) == 0 {
		return fmt.Errorf("%w: invalid trusted manifest fields", ErrInvalidManifest)
	}
	if err := validateGitBinding(manifest.Git); err != nil {
		return err
	}
	if manifest.Bindings.OracleVersion != TrustedOracleVersion || !validSHA256(manifest.Bindings.OracleSHA256) ||
		!validSHA256(manifest.Bindings.ConfigSetSHA256) || !validSHA256(manifest.Bindings.ProcessSetSHA256) ||
		!validSHA256(manifest.Bindings.TLSPeerSetSHA256) || !validSHA256(manifest.Bindings.GatePeerSetSHA256) ||
		!validSHA256(manifest.Bindings.RedisKeyLedgerSHA256) {
		return fmt.Errorf("%w: invalid trusted digest bindings", ErrInvalidManifest)
	}
	if len(manifest.Artifacts) != len(trustedArtifactNames) {
		return fmt.Errorf("%w: trusted artifact count", ErrInvalidManifest)
	}
	for _, scenario := range manifest.Scenarios {
		if !validScenario(scenario) {
			return fmt.Errorf("%w: invalid trusted scenario", ErrInvalidManifest)
		}
	}
	if manifest.EvidenceLevel == EvidenceE3 && manifest.Signature != nil {
		return fmt.Errorf("%w: E3 must not be promoted by an external signature", ErrInvalidManifest)
	}
	if requireSignatureCheck && manifest.Signature != nil {
		return verifyExternalSignature(manifest)
	}
	return nil
}

func validateExactArtifactSet(root string, artifacts []Artifact) error {
	want := make(map[string]Artifact, len(artifacts))
	for _, artifact := range artifacts {
		if err := validateArtifact(artifact); err != nil {
			return err
		}
		if _, exists := want[artifact.Path]; exists {
			return fmt.Errorf("%w: duplicate trusted artifact", ErrInvalidManifest)
		}
		want[artifact.Path] = artifact
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: nested or linked trusted artifact %s", ErrUnsafePath, entry.Name())
		}
		if entry.Name() == "run-manifest.json" {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: non-regular trusted artifact %s", ErrUnsafePath, entry.Name())
		}
		if _, ok := want[entry.Name()]; !ok {
			return fmt.Errorf("%w: unhashed regular artifact %s", ErrInvalidManifest, entry.Name())
		}
		seen[entry.Name()] = struct{}{}
	}
	if len(seen) != len(want) {
		return fmt.Errorf("%w: missing trusted artifact", ErrArtifactMismatch)
	}
	return nil
}

func readTrustedRecordsFromRoot(root secureRoot, manifest TrustedManifest) ([]TrustedRecord, error) {
	files := make(map[string]*os.File, len(trustedNDJSONNames))
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	for name := range trustedNDJSONNames {
		file, err := root.openRegular(name)
		if err != nil {
			return nil, err
		}
		files[name] = file
	}
	return readTrustedRecordsFromOpenFiles(files, manifest.RunID, manifest.Challenge)
}

// ExternalSignatureDigest returns the digest an independent E4 reviewer signs.
// The signature field itself is excluded, avoiding circular input.
func ExternalSignatureDigest(manifest TrustedManifest) (string, error) {
	manifest.Signature = nil
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func verifyExternalSignature(manifest TrustedManifest) error {
	signature := manifest.Signature
	if signature == nil || signature.Algorithm != "ed25519" || !safeToken(signature.Issuer) ||
		!safeToken(signature.KeyID) || !validSHA256(signature.ManifestSHA256) {
		return ErrAttestationRequired
	}
	digest, err := ExternalSignatureDigest(manifest)
	if err != nil || digest != signature.ManifestSHA256 {
		return fmt.Errorf("%w: E4 manifest digest mismatch", ErrInvalidAttestation)
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(signature.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: E4 public key", ErrInvalidAttestation)
	}
	signatureBytes, err := base64.StdEncoding.Strict().DecodeString(signature.Signature)
	if err != nil || len(signatureBytes) != ed25519.SignatureSize ||
		!ed25519.Verify(ed25519.PublicKey(publicKey), []byte(digest), signatureBytes) {
		return fmt.Errorf("%w: E4 signature verification", ErrInvalidAttestation)
	}
	return nil
}

func readFactMap(record TrustedRecord) (map[string]any, error) {
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(record.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		return nil, fmt.Errorf("%w: %s payload", ErrInvalidEvidence, record.Kind)
	}
	return fields, nil
}

type factIndex struct {
	byScenario map[string]map[string][]TrustedRecord
}

func indexFacts(records []TrustedRecord) factIndex {
	index := factIndex{byScenario: make(map[string]map[string][]TrustedRecord)}
	for _, record := range records {
		if index.byScenario[record.ScenarioID] == nil {
			index.byScenario[record.ScenarioID] = make(map[string][]TrustedRecord)
		}
		index.byScenario[record.ScenarioID][record.Kind] = append(index.byScenario[record.ScenarioID][record.Kind], record)
	}
	return index
}

func deriveOracle(runID, challenge string, scenarios []string, records []TrustedRecord, oracleSHA string) OracleResult {
	result := OracleResult{
		Schema: TrustedSchemaVersion, OracleVersion: TrustedOracleVersion, OracleSHA256: oracleSHA,
		RunID: runID, Challenge: challenge, RecordCount: uint64(len(records)), Status: StatusPass,
	}
	if len(records) != 0 {
		result.ChainHead = records[len(records)-1].CurrentHash
	} else {
		result.ChainHead = strings.Repeat("0", 64)
	}
	index := indexFacts(records)
	ordered := append([]string(nil), scenarios...)
	sort.Strings(ordered)
	for _, scenario := range ordered {
		result.Clauses = append(result.Clauses, scenarioClauses(scenario, index)...)
	}
	result.Clauses = append(result.Clauses, cleanupClauses(index, ordered)...)
	for _, clause := range result.Clauses {
		result.Status = AggregateStatus(result.Status, clause.Status)
	}
	if len(result.Clauses) == 0 {
		result.Status = StatusInconclusive
		result.Reason = "no mandatory oracle clauses were selected"
	} else {
		result.Reason = "all mandatory trusted clauses passed"
		if result.Status == StatusFail {
			result.Reason = "one or more trusted clauses failed"
		} else if result.Status == StatusInconclusive {
			result.Reason = "one or more trusted clauses are inconclusive"
		}
	}
	return result
}

func scenarioClauses(scenario string, index factIndex) []OracleClause {
	switch scenario {
	case "S0":
		return []OracleClause{
			minimumCountClause(index, scenario, "s0.authenticated_watch", "snapshot_installed", 7),
			fieldCountClause(index, scenario, "s0.hot_resolve", "business_resolve", "ok", "true", 100),
			exactFieldClause(index, scenario, "s0.zero_remote_hot_path", "hot_path_counters", map[string]string{"rpc_delta": "0", "stream_delta": "0"}),
		}
	case "S1":
		return []OracleClause{
			exactFieldClause(index, scenario, "s1.new_boot", "controller_rollover", map[string]string{"new_boot": "true", "stop_first": "true"}),
			fieldCountClause(index, scenario, "s1.consumer_resync", "consumer_rollover", "healthy", "true", 7),
		}
	case "S2":
		return []OracleClause{
			// PENDING is submitted before the held Stop can be observed, and ACTIVE
			// is an intentionally short durable CAS immediately followed by
			// SUCCEEDED. The external production API cannot deterministically sample
			// either transient state without perturbing execution. Require every
			// externally observable ordered phase plus the final SUCCEEDED record;
			// exact callbacks and Authority transfer are proved independently below.
			phaseClause(index, scenario, "s2.plan_monotonic", "plan_phase", []string{"DRAINING", "ASSIGNING", "PREPARING", "SUCCEEDED"}),
			exactCountClause(index, scenario, "s2.stop_once", "holder_callback_stop", 1),
			exactCountClause(index, scenario, "s2.wait_once", "holder_callback_wait", 1),
			exactCountClause(index, scenario, "s2.facts_once", "holder_callback_facts", 1),
			exactCountClause(index, scenario, "s2.prepare_once", "holder_callback_prepare", 1),
			exactCountClause(index, scenario, "s2.authority_once", "authority_transfer", 1),
		}
	case "S3":
		return []OracleClause{
			exactCountClause(index, scenario, "s3.transfer_eval_once", "transfer_evalsha_forwarded", 1),
			exactCountClause(index, scenario, "s3.reply_dropped_once", "transfer_reply_dropped", 1),
			exactFieldClause(index, scenario, "s3.pending_four_truth", "pending_projection", map[string]string{"authority_target": "true", "snapshot_source": "true", "plan_assigning": "true", "authority_attempted": "true"}),
			exactCountClause(index, scenario, "s3.stop_once", "holder_callback_stop", 1),
			exactCountClause(index, scenario, "s3.wait_once", "holder_callback_wait", 1),
			exactCountClause(index, scenario, "s3.facts_once", "holder_callback_facts", 1),
			exactCountClause(index, scenario, "s3.prepare_once", "holder_callback_prepare", 1),
			exactCountClause(index, scenario, "s3.authority_once", "authority_transfer", 1),
			exactCountClause(index, scenario, "s3.no_paused_or_failed", "plan_paused_or_failed", 0),
			fieldCountClause(index, scenario, "s3.succeeded", "plan_phase", "phase", "SUCCEEDED", 1),
		}
	case "S4":
		return []OracleClause{
			fieldSetClause(index, scenario, "s4.negative_verify", "verify_negative", "case", []string{"pending", "stale", "unavailable", "unknown", "missing-replay"}, "nonzero", "true"),
			exactFieldClause(index, scenario, "s4.identical_release", "snapshot_release", map[string]string{"identical": "true"}),
			exactFieldClause(index, scenario, "s4.final_verify_zero", "verify_final", map[string]string{"zero": "true", "checks": "11"}),
		}
	case "S5":
		return []OracleClause{
			exactFieldClause(index, scenario, "s5.reject_live", "takeover_live_rejected", map[string]string{"rejected": "true"}),
			exactFieldClause(index, scenario, "s5.redis_expiry", "redis_expiry_proof", map[string]string{"expired": "true"}),
			exactCountClause(index, scenario, "s5.takeover_once", "authority_transfer", 1),
			exactFieldClause(index, scenario, "s5.old_generation_fenced", "old_generation_request", map[string]string{"fenced": "true", "durable_unchanged": "true"}),
		}
	case "S6":
		return []OracleClause{
			fieldSetClause(index, scenario, "s6.three_operations", "business_receipt", "operation", []string{"T1", "T2", "T3"}, "count", "1"),
			exactFieldClause(index, scenario, "s6.t2_response_lost", "business_response_lost", map[string]string{"operation": "T2", "committed": "true"}),
			exactFieldClause(index, scenario, "s6.t2_replay", "business_replay", map[string]string{"operation": "T2", "same_receipt": "true", "duplicate_reward": "false"}),
			exactFieldClause(index, scenario, "s6.rejoin", "subscription_rejoin", map[string]string{"reason": "SUBSCRIPTION_LEASE_LOST", "fresh_attempt": "true", "fresh_fence": "true"}),
			exactFieldClause(index, scenario, "s6.four_truth", "business_reconcile", map[string]string{"authority": "true", "snapshot": "true", "holder": "true", "plan": "true"}),
		}
	case "S7":
		return []OracleClause{
			exactFieldClause(index, scenario, "s7.bounded_canary", "company_canary", map[string]string{"bootstrap": "true", "watch": "true", "player_move": "true", "farm_move": "true", "verify_zero": "true", "faults": "false", "business": "false"}),
		}
	case "deadline-refresh":
		return []OracleClause{
			exactFieldClause(index, scenario, "deadline.projected_complete", "deadline_refresh_projected", map[string]string{"changed_shards": "4", "one_complete_snapshot_per_revision": "true", "generation_unchanged": "true"}),
			exactFieldClause(index, scenario, "deadline.seven_consumers", "consumer_metrics_applied", map[string]string{"consumer_count": "7"}),
			fieldCountClause(index, scenario, "deadline.business_after_old_horizon", "business_resolve", "ok", "true", 100),
			exactFieldClause(index, scenario, "deadline.long_lived_local_hot_path", "long_lived_route", map[string]string{"old_deadline_crossed": "true", "business_resolves": "100", "consumer_count": "7", "hot_path_rpc_delta": "0", "holder_callback_delta": "0", "authority_transfer_delta": "0"}),
		}
	default:
		return []OracleClause{{ID: "scenario.invalid", Status: StatusFail, Reason: "unknown scenario"}}
	}
}

func cleanupClauses(index factIndex, scenarios []string) []OracleClause {
	var records []TrustedRecord
	for _, scenario := range scenarios {
		records = append(records, index.byScenario[scenario]["cleanup_key"]...)
	}
	residue := false
	for _, record := range records {
		fields, err := readFactMap(record)
		if err != nil || factString(fields["exists"]) != "0" || factString(fields["deleted"]) != "true" {
			residue = true
		}
	}
	clauses := []OracleClause{{ID: "cleanup.exact_keys", Status: StatusPass, Reason: "every registered exact key was deleted and verified", Records: sequences(records)}}
	if len(records) == 0 {
		clauses[0].Status, clauses[0].Reason = StatusInconclusive, "no exact-key cleanup records"
	} else if residue {
		clauses[0].Status, clauses[0].Reason = StatusFail, "cleanup residue or malformed exact-key result"
	}
	process := minimumCountClause(index, scenarios[len(scenarios)-1], "cleanup.process_residue", "cleanup_processes", 1)
	process.ID = "cleanup.process_residue"
	if process.Status == StatusPass {
		for _, scenario := range scenarios {
			for _, record := range index.byScenario[scenario]["cleanup_processes"] {
				fields, err := readFactMap(record)
				if err != nil || factString(fields["residue"]) != "false" {
					process.Status, process.Reason = StatusFail, "process or container residue remains"
				}
			}
		}
	}
	clauses = append(clauses, process)
	return clauses
}

func exactCountClause(index factIndex, scenario, id, kind string, want int) OracleClause {
	records := index.byScenario[scenario][kind]
	clause := OracleClause{ID: id, Status: StatusPass, Reason: fmt.Sprintf("%s count=%d", kind, want), Records: sequences(records)}
	if len(records) != want {
		clause.Status = StatusFail
		clause.Reason = fmt.Sprintf("%s count=%d want=%d", kind, len(records), want)
	}
	return clause
}

func minimumCountClause(index factIndex, scenario, id, kind string, minimum int) OracleClause {
	records := index.byScenario[scenario][kind]
	clause := OracleClause{ID: id, Status: StatusPass, Reason: fmt.Sprintf("%s count=%d", kind, len(records)), Records: sequences(records)}
	if len(records) < minimum {
		clause.Status = StatusInconclusive
		clause.Reason = fmt.Sprintf("%s count=%d minimum=%d", kind, len(records), minimum)
	}
	return clause
}

func exactFieldClause(index factIndex, scenario, id, kind string, fields map[string]string) OracleClause {
	records := index.byScenario[scenario][kind]
	clause := OracleClause{ID: id, Status: StatusInconclusive, Reason: "required trusted observation is absent", Records: sequences(records)}
	if len(records) != 1 {
		if len(records) > 1 {
			clause.Status, clause.Reason = StatusFail, "duplicate singleton trusted observation"
		}
		return clause
	}
	facts, err := readFactMap(records[0])
	if err != nil {
		clause.Status, clause.Reason = StatusFail, err.Error()
		return clause
	}
	for name, want := range fields {
		if got := factString(facts[name]); got != want {
			clause.Status, clause.Reason = StatusFail, fmt.Sprintf("%s=%q want=%q", name, got, want)
			return clause
		}
	}
	clause.Status, clause.Reason = StatusPass, "trusted fields match"
	return clause
}

func fieldCountClause(index factIndex, scenario, id, kind, field, value string, minimum int) OracleClause {
	records := index.byScenario[scenario][kind]
	matched := 0
	malformed := false
	for _, record := range records {
		facts, err := readFactMap(record)
		if err != nil {
			malformed = true
			continue
		}
		if factString(facts[field]) == value {
			matched++
		}
	}
	clause := OracleClause{ID: id, Status: StatusPass, Reason: fmt.Sprintf("%s matched=%d", kind, matched), Records: sequences(records)}
	if malformed {
		clause.Status, clause.Reason = StatusFail, "malformed trusted observation"
	} else if matched < minimum {
		clause.Status, clause.Reason = StatusInconclusive, fmt.Sprintf("%s matched=%d minimum=%d", kind, matched, minimum)
	}
	return clause
}

func fieldSetClause(index factIndex, scenario, id, kind, key string, want []string, checkKey, checkValue string) OracleClause {
	records := index.byScenario[scenario][kind]
	seen := make(map[string]int, len(want))
	for _, record := range records {
		facts, err := readFactMap(record)
		if err != nil || factString(facts[checkKey]) != checkValue {
			return OracleClause{ID: id, Status: StatusFail, Reason: "malformed or contradictory set observation", Records: sequences(records)}
		}
		seen[factString(facts[key])]++
	}
	for _, value := range want {
		if seen[value] != 1 {
			status := StatusFail
			if seen[value] == 0 {
				status = StatusInconclusive
			}
			return OracleClause{ID: id, Status: status, Reason: fmt.Sprintf("%s=%s count=%d want=1", key, value, seen[value]), Records: sequences(records)}
		}
	}
	if len(seen) != len(want) {
		return OracleClause{ID: id, Status: StatusFail, Reason: "unexpected set member", Records: sequences(records)}
	}
	return OracleClause{ID: id, Status: StatusPass, Reason: "exact trusted set matched", Records: sequences(records)}
}

// planPhaseRank is the production RoutePlan total order. Required milestones
// may omit intermediate ranks (for example ACTIVE between PREPARING and
// SUCCEEDED) when a poll samples sparsely; observing those intermediates must
// remain monotonic, not fail the clause.
var planPhaseRank = map[string]int{
	"PENDING": 1, "DRAINING": 2, "ASSIGNING": 3, "PREPARING": 4, "ACTIVE": 5, "SUCCEEDED": 6,
}

func phaseClause(index factIndex, scenario, id, kind string, required []string) OracleClause {
	records := index.byScenario[scenario][kind]
	position := 0
	lastRevision := uint64(0)
	for _, record := range records {
		facts, err := readFactMap(record)
		if err != nil {
			return OracleClause{ID: id, Status: StatusFail, Reason: "malformed phase observation", Records: sequences(records)}
		}
		phase := factString(facts["phase"])
		revision, err := strconv.ParseUint(factString(facts["revision"]), 10, 64)
		if err != nil || revision <= lastRevision {
			return OracleClause{ID: id, Status: StatusFail, Reason: "Plan revisions are not strictly increasing", Records: sequences(records)}
		}
		lastRevision = revision
		if position < len(required) && phase == required[position] {
			position++
			continue
		}
		if position > 0 && phase == required[position-1] {
			continue
		}
		if position > 0 && position < len(required) {
			prevRank, prevOK := planPhaseRank[required[position-1]]
			nextRank, nextOK := planPhaseRank[required[position]]
			rank, ok := planPhaseRank[phase]
			if prevOK && nextOK && ok && rank > prevRank && rank < nextRank {
				continue
			}
		}
		return OracleClause{ID: id, Status: StatusFail, Reason: "Plan phase order is not monotonic", Records: sequences(records)}
	}
	if position != len(required) {
		return OracleClause{ID: id, Status: StatusInconclusive, Reason: fmt.Sprintf("observed %d/%d required Plan phases", position, len(required)), Records: sequences(records)}
	}
	return OracleClause{ID: id, Status: StatusPass, Reason: "Plan phases and revisions are monotonic", Records: sequences(records)}
}

func sequences(records []TrustedRecord) []uint64 {
	result := make([]uint64, 0, len(records))
	for _, record := range records {
		result = append(result, record.Sequence)
	}
	return result
}

func factString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

// AggregateStatus implements the stable fail-dominant verdict lattice.
func AggregateStatus(left, right EvidenceStatus) EvidenceStatus {
	if left == StatusFail || right == StatusFail {
		return StatusFail
	}
	if left == StatusInconclusive || right == StatusInconclusive {
		return StatusInconclusive
	}
	return StatusPass
}

// SHA256File hashes an already-resolved regular executable or image archive.
func SHA256File(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	return hashRegularFile(file)
}

// SHA256CanonicalSet hashes sorted, length-delimited non-secret values.
func SHA256CanonicalSet(domain string, values []string) string {
	values = append([]string(nil), values...)
	sort.Strings(values)
	digest := sha256.New()
	writeSetFrame(digest, domain)
	for _, value := range values {
		writeSetFrame(digest, value)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func writeSetFrame(writer io.Writer, value string) {
	_, _ = io.WriteString(writer, strconv.Itoa(len(value)))
	_, _ = io.WriteString(writer, ":")
	_, _ = io.WriteString(writer, value)
}

// CurrentTrustedGitBinding resolves exact detached/clean state. Untracked
// files count as modified; the artifact root must therefore live elsewhere.
func CurrentTrustedGitBinding(worktree, expectedHead string) (GitBinding, error) {
	head, err := CurrentGitHead(worktree)
	if err != nil {
		return GitBinding{}, err
	}
	if head != strings.ToLower(expectedHead) {
		return GitBinding{}, fmt.Errorf("%w: Git HEAD %s does not match expected %s", ErrInvalidEvidence, head, expectedHead)
	}
	branch, err := CurrentGitBranch(worktree)
	if err != nil {
		return GitBinding{}, err
	}
	treeRaw, err := runGitCommand(worktree, "rev-parse", "--verify", "HEAD^{tree}")
	if err != nil {
		return GitBinding{}, err
	}
	tree := strings.TrimSpace(string(treeRaw))
	status, err := runGitCommand(worktree, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return GitBinding{}, err
	}
	binding := GitBinding{HeadCommit: head, TreeHash: strings.ToLower(tree), Detached: branch == "detached", Clean: len(status) == 0, Modified: len(status) != 0}
	if err := validateGitBinding(binding); err != nil {
		return GitBinding{}, err
	}
	return binding, nil
}

// TrustedArtifactNames returns a copy of the exact v2 artifact allowlist.
func TrustedArtifactNames() []string {
	return append([]string(nil), trustedArtifactNames...)
}

// TrustedManifestPath returns the stable manifest location.
func TrustedManifestPath(root string) string {
	return filepath.Join(root, "run-manifest.json")
}
