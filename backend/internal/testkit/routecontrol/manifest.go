package routecontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Artifact is a relative evidence file and its content digest. Raw secrets
// and external key material must never be encoded into this record.
type Artifact struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Manifest is the required run-level record. Validate must succeed before it
// is written or used as evidence input.
type Manifest struct {
	Schema        string         `json:"schema"`
	TaskID        string         `json:"task_id"`
	RunID         string         `json:"run_id"`
	BaseCommit    string         `json:"base_commit"`
	HeadCommit    string         `json:"head_commit"`
	Branch        string         `json:"branch"`
	Worktree      string         `json:"worktree"`
	EvidenceLevel EvidenceLevel  `json:"evidence_level"`
	Status        EvidenceStatus `json:"status"`
	Reason        string         `json:"reason"`
	Provenance    Provenance     `json:"provenance"`
	Attestation   *Attestation   `json:"attestation,omitempty"`
	Artifacts     []Artifact     `json:"artifacts"`
}

// Redacted returns a manifest whose free-text fields use the supplied
// redactor. Top-level provenance fields are transformed together so binding
// checks remain exact after sanitization.
func (m Manifest) Redacted(redactor *Redactor) Manifest {
	if redactor == nil {
		redactor = NewRedactor()
	}
	// A value receiver only copies the slice header. Clone Artifacts before
	// redacting so the caller's backing array remains untouched.
	m.Artifacts = append([]Artifact(nil), m.Artifacts...)
	m.TaskID = redactor.Redact(m.TaskID)
	m.RunID = redactor.Redact(m.RunID)
	m.BaseCommit = redactor.Redact(m.BaseCommit)
	m.HeadCommit = redactor.Redact(m.HeadCommit)
	m.Branch = redactor.Redact(m.Branch)
	m.Worktree = redactor.Redact(m.Worktree)
	m.Reason = redactor.Redact(m.Reason)
	m.Provenance = m.Provenance.Redacted(redactor)
	for index := range m.Artifacts {
		m.Artifacts[index].Name = redactor.Redact(m.Artifacts[index].Name)
		m.Artifacts[index].Path = redactor.Redact(m.Artifacts[index].Path)
	}
	if m.Attestation != nil {
		attestation := *m.Attestation
		attestation.ID = redactor.Redact(attestation.ID)
		attestation.Issuer = redactor.Redact(attestation.Issuer)
		m.Attestation = &attestation
	}
	return m
}

// Claim converts the manifest's status fields into the anti-escalation claim
// checked by the package.
func (m Manifest) Claim() Claim {
	return Claim{
		Level:       m.EvidenceLevel,
		Status:      m.Status,
		Reason:      m.Reason,
		Provenance:  m.Provenance,
		Attestation: m.Attestation,
	}
}

// Validate checks required fields, provenance, status, and artifact metadata.
func (m Manifest) Validate() error {
	return m.validate(NewRedactor(), true)
}

// ValidateWithRedactor rejects manifests whose free-text fields would leak
// values known to redactor.
func (m Manifest) ValidateWithRedactor(redactor *Redactor) error {
	return m.validate(redactor, true)
}

func (m Manifest) validate(redactor *Redactor, requireRedacted bool) error {
	redacted := m.Redacted(redactor)
	if requireRedacted && !sameManifestBindings(m, redacted) {
		return fmt.Errorf("%w: manifest contains unredacted free text", ErrInvalidManifest)
	}
	if m.Schema != SchemaVersion {
		return fmt.Errorf("%w: schema must be %q", ErrInvalidManifest, SchemaVersion)
	}
	required := map[string]string{
		"task_id":     m.TaskID,
		"run_id":      m.RunID,
		"base_commit": m.BaseCommit,
		"head_commit": m.HeadCommit,
		"branch":      m.Branch,
		"worktree":    m.Worktree,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidManifest, name)
		}
	}
	if !safeToken(m.TaskID) || !safeToken(m.RunID) || !safeToken(m.Branch) {
		return fmt.Errorf("%w: task_id, run_id, and branch must be non-empty safe tokens", ErrInvalidManifest)
	}
	claim := m.Claim()
	if err := claim.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if m.TaskID != m.Provenance.TaskID ||
		m.RunID != m.Provenance.RunID ||
		m.BaseCommit != m.Provenance.BaseCommit ||
		m.HeadCommit != m.Provenance.HeadCommit ||
		m.Branch != m.Provenance.Branch ||
		m.Worktree != m.Provenance.Worktree {
		return fmt.Errorf("%w: top-level provenance bindings do not match", ErrInvalidManifest)
	}
	if len(m.Artifacts) == 0 {
		return fmt.Errorf("%w: at least one artifact is required", ErrInvalidManifest)
	}

	seenNames := make(map[string]struct{}, len(m.Artifacts))
	seenPaths := make(map[string]struct{}, len(m.Artifacts))
	for index, artifact := range m.Artifacts {
		if err := validateArtifact(artifact); err != nil {
			return fmt.Errorf("%w: artifacts[%d]: %v", ErrInvalidManifest, index, err)
		}
		if _, ok := seenNames[artifact.Name]; ok {
			return fmt.Errorf("%w: duplicate artifact name %q", ErrInvalidManifest, artifact.Name)
		}
		if _, ok := seenPaths[artifact.Path]; ok {
			return fmt.Errorf("%w: duplicate artifact path %q", ErrInvalidManifest, artifact.Path)
		}
		seenNames[artifact.Name] = struct{}{}
		seenPaths[artifact.Path] = struct{}{}
	}
	return nil
}

// ComputeArtifact hashes one relative file without retaining its contents.
func ComputeArtifact(root, relativePath, name string) (Artifact, error) {
	if err := validateArtifactPath(relativePath); err != nil {
		return Artifact{}, err
	}
	if !validArtifactName(name) {
		return Artifact{}, errors.New("artifact name is required and must be a safe token")
	}
	rootDir, err := openSecureRoot(root)
	if err != nil {
		return Artifact{}, err
	}
	defer rootDir.close()
	file, err := rootDir.openRegular(relativePath)
	if err != nil {
		return Artifact{}, err
	}
	digest, size, err := hashRegularFile(file)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{Name: name, Path: filepath.ToSlash(relativePath), Bytes: size, SHA256: digest}, nil
}

// ValidateArtifacts re-hashes each manifest artifact relative to root.
func (m Manifest) ValidateArtifacts(root string) error {
	rootDir, err := openSecureRoot(root)
	if err != nil {
		return err
	}
	defer rootDir.close()
	if err := m.Validate(); err != nil {
		return err
	}
	return m.hashArtifactsWithRoot(rootDir)
}

func (m Manifest) hashArtifactsWithRoot(rootDir secureRoot) error {
	for _, artifact := range m.Artifacts {
		file, err := rootDir.openRegular(artifact.Path)
		if err != nil {
			return wrapArtifactHashError(artifact.Path, err)
		}
		digest, size, err := hashRegularFile(file)
		if err != nil {
			return wrapArtifactHashError(artifact.Path, err)
		}
		if digest != artifact.SHA256 || size != artifact.Bytes {
			return fmt.Errorf("%w: %s got sha256=%s bytes=%d want sha256=%s bytes=%d",
				ErrArtifactMismatch, artifact.Path, digest, size, artifact.SHA256, artifact.Bytes)
		}
	}
	return nil
}

func wrapArtifactHashError(path string, err error) error {
	if errors.Is(err, ErrUnsafePath) {
		return fmt.Errorf("%w: %s: %v", ErrUnsafePath, path, err)
	}
	return fmt.Errorf("%w: %s: %v", ErrArtifactMismatch, path, err)
}

// WriteManifest validates, re-hashes artifacts below root, and atomically
// creates a new private-by-default JSON manifest below the same root.
func WriteManifest(root, path string, manifest Manifest) error {
	return WriteManifestWithRedactor(root, path, manifest, NewRedactor())
}

// WriteManifestWithRedactor sanitizes free text, re-hashes artifacts below
// root, and atomically creates a new manifest without overwriting an existing
// file or following a symlink.
func WriteManifestWithRedactor(root, path string, manifest Manifest, redactor *Redactor) error {
	if redactor == nil {
		redactor = NewRedactor()
	}
	if err := manifest.validate(redactor, false); err != nil {
		return err
	}
	redacted := manifest.Redacted(redactor)
	if !sameArtifacts(manifest.Artifacts, redacted.Artifacts) {
		return fmt.Errorf("%w: artifact name/path metadata must not be redacted", ErrInvalidManifest)
	}
	rootDir, err := openSecureRoot(root)
	if err != nil {
		return err
	}
	defer rootDir.close()
	if err := manifest.hashArtifactsWithRoot(rootDir); err != nil {
		return err
	}
	manifest = redacted
	if err := manifest.validate(NewRedactor(), true); err != nil {
		return err
	}
	relativePath, err := relativePathWithinRoot(rootDir.path(), path)
	if err != nil {
		return err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(manifest); err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	return rootDir.createManifest(relativePath, encoded.Bytes())
}

// ReadManifest decodes a strict JSON manifest and re-hashes every artifact
// below root before returning it.
func ReadManifest(root, path string) (Manifest, error) {
	return ReadManifestWithRedactor(root, path, NewRedactor())
}

// ReadManifestWithRedactor rejects unredacted free text, validates provenance,
// and re-hashes every artifact below root.
func ReadManifestWithRedactor(root, path string, redactor *Redactor) (Manifest, error) {
	if redactor == nil {
		redactor = NewRedactor()
	}
	rootDir, err := openSecureRoot(root)
	if err != nil {
		return Manifest{}, err
	}
	defer rootDir.close()
	relativePath, err := relativePathWithinRoot(rootDir.path(), path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	file, err := rootDir.openRegular(relativePath)
	if err != nil {
		return Manifest{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, errors.New("decode manifest: trailing JSON value")
		}
		return Manifest{}, fmt.Errorf("decode manifest trailing data: %w", err)
	}
	if err := manifest.ValidateWithRedactor(redactor); err != nil {
		return Manifest{}, err
	}
	if err := manifest.hashArtifactsWithRoot(rootDir); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateArtifact(artifact Artifact) error {
	if !validArtifactName(artifact.Name) {
		return errors.New("name is required and must be a safe token")
	}
	if err := validateArtifactPath(artifact.Path); err != nil {
		return err
	}
	if artifact.Bytes < 0 {
		return errors.New("bytes cannot be negative")
	}
	if !validSHA256(artifact.SHA256) {
		return errors.New("sha256 must be 64 lowercase hex characters")
	}
	return nil
}

func validArtifactName(value string) bool {
	return safeToken(value) || value == "<redacted>"
}

func validateArtifactPath(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("path is required")
	}
	if filepath.IsAbs(value) || strings.ContainsRune(value, 0) {
		return errors.New("path must be relative and NUL-free")
	}
	if strings.Contains(value, "\\") {
		return errors.New("path must use slash separators")
	}
	clean := filepath.Clean(filepath.FromSlash(value))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("path must stay below the artifact root")
	}
	return nil
}

func hashRegularFile(file *os.File) (string, int64, error) {
	if file == nil {
		return "", 0, fmt.Errorf("%w: opened file is nil", ErrArtifactMismatch)
	}
	defer file.Close()
	return hashOpenRegularFile(file)
}

func computeOpenArtifact(file *os.File, name, path string) (Artifact, error) {
	if !validArtifactName(name) {
		return Artifact{}, fmt.Errorf("%w: artifact name is invalid", ErrInvalidManifest)
	}
	if err := validateArtifactPath(path); err != nil {
		return Artifact{}, fmt.Errorf("%w: artifact path: %v", ErrInvalidManifest, err)
	}
	if file == nil {
		return Artifact{}, fmt.Errorf("%w: opened file is nil", ErrArtifactMismatch)
	}
	if err := timelineSync(file); err != nil {
		return Artifact{}, fmt.Errorf("sync artifact: %w", err)
	}
	digest, size, err := hashOpenRegularFile(file)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{Name: name, Path: filepath.ToSlash(path), Bytes: size, SHA256: digest}, nil
}

// hashOpenRegularFile hashes an already-held regular-file capability without
// closing it. The original offset is restored to EOF for subsequent appends.
func hashOpenRegularFile(file *os.File) (string, int64, error) {
	if file == nil {
		return "", 0, fmt.Errorf("%w: opened file is nil", ErrArtifactMismatch)
	}
	info, err := timelineStat(file)
	if err != nil {
		return "", 0, err
	}
	if info == nil {
		return "", 0, errors.New("stat artifact returned nil info")
	}
	if !info.Mode().IsRegular() {
		return "", 0, fmt.Errorf("%w: opened file is not regular", ErrArtifactMismatch)
	}
	if _, err := timelineSeek(file, 0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("seek artifact start: %w", err)
	}
	hash := sha256.New()
	size, err := timelineCopy(hash, file)
	if err != nil {
		return "", 0, fmt.Errorf("hash artifact: %w", err)
	}
	if size != info.Size() {
		return "", 0, fmt.Errorf("hash artifact size changed: got %d want %d", size, info.Size())
	}
	if _, err := timelineSeek(file, 0, io.SeekEnd); err != nil {
		return "", 0, fmt.Errorf("seek artifact end: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func sameManifestBindings(left, right Manifest) bool {
	return left.TaskID == right.TaskID &&
		left.RunID == right.RunID &&
		left.BaseCommit == right.BaseCommit &&
		left.HeadCommit == right.HeadCommit &&
		left.Branch == right.Branch &&
		left.Worktree == right.Worktree &&
		left.Reason == right.Reason &&
		left.Provenance == right.Provenance &&
		sameArtifacts(left.Artifacts, right.Artifacts) &&
		sameAttestation(left.Attestation, right.Attestation)
}

func sameArtifacts(left, right []Artifact) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameAttestation(left, right *Attestation) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func safeToken(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || strings.ContainsRune("._:/-", r) {
			continue
		}
		return false
	}
	return true
}
