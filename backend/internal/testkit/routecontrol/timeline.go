package routecontrol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TimelineStream is one independent observation stream.
type TimelineStream string

const (
	StreamRevision  TimelineStream = "revision"
	StreamAuthority TimelineStream = "authority"
	StreamHolder    TimelineStream = "holder"
	StreamFault     TimelineStream = "fault"
)

// RevisionObservation records route revision facts without deciding whether a
// route is correct.
type RevisionObservation struct {
	Domain   string `json:"domain"`
	Shard    string `json:"shard"`
	Revision uint64 `json:"revision"`
	Source   string `json:"source"`
}

// AuthorityObservation records an observed authority transition.
type AuthorityObservation struct {
	Operation    string `json:"operation"`
	TransitionID string `json:"transition_id"`
	State        string `json:"state"`
	Revision     uint64 `json:"revision"`
}

// HolderObservation records an observed holder identity tuple.
type HolderObservation struct {
	HolderID    string `json:"holder_id"`
	BootID      string `json:"boot_id"`
	State       string `json:"state"`
	WriterEpoch uint64 `json:"writer_epoch"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
}

// FaultPhase identifies where an injected fault was observed. It is only
// recorded; no pass/fail decision is derived from a fault phase.
type FaultPhase string

const (
	FaultBeforeSend   FaultPhase = "before-send"
	FaultInFlight     FaultPhase = "in-flight"
	FaultAfterApply   FaultPhase = "after-apply"
	FaultResponseLost FaultPhase = "response-lost"
	FaultRecovered    FaultPhase = "recovered"
	FaultShutdown     FaultPhase = "shutdown"
)

// FaultObservation contains no verdict field by design.
type FaultObservation struct {
	Name   string     `json:"name"`
	Action string     `json:"action"`
	Phase  FaultPhase `json:"phase"`
	Note   string     `json:"note,omitempty"`
}

// TimelineOptions controls timestamp and redaction behavior.
type TimelineOptions struct {
	Provenance Provenance
	Now        func() time.Time
	Redactor   *Redactor
}

// Timeline writes four separate NDJSON files with independent sequence
// counters. A Timeline is safe for concurrent record calls.
type Timeline struct {
	mu         sync.Mutex
	dir        string
	runDir     secureRunDir
	provenance Provenance
	now        func() time.Time
	redactor   *Redactor
	files      map[TimelineStream]*os.File
	paths      map[TimelineStream]string
	sequences  map[TimelineStream]uint64
	closed     bool
	poison     error
}

var streamOrder = []TimelineStream{StreamRevision, StreamAuthority, StreamHolder, StreamFault}

var (
	timelineWrite = func(file *os.File, data []byte) (int, error) {
		return file.Write(data)
	}
	timelineSync = func(file *os.File) error {
		return file.Sync()
	}
	timelineSeek = func(file *os.File, offset int64, whence int) (int64, error) {
		return file.Seek(offset, whence)
	}
	timelineStat = func(file *os.File) (os.FileInfo, error) {
		return file.Stat()
	}
	timelineCopy = io.Copy
)

// NewTimeline creates revision.ndjson, authority.ndjson, holder.ndjson, and
// fault.ndjson below dir.
func NewTimeline(dir string, provenance Provenance) (*Timeline, error) {
	return NewTimelineWithOptions(dir, TimelineOptions{Provenance: provenance})
}

// NewTimelineWithOptions creates a timeline with deterministic test seams.
func NewTimelineWithOptions(dir string, options TimelineOptions) (*Timeline, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("timeline directory is required")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	redactor := options.Redactor
	if redactor == nil {
		redactor = NewRedactor()
	}
	provenance := options.Provenance.Redacted(redactor)
	if err := provenance.ValidateFor(provenance.DeclaredLevel); err != nil {
		return nil, err
	}
	runDir, err := createRunDirectory(dir)
	if err != nil {
		return nil, err
	}
	timeline := &Timeline{
		dir:        runDir.path(),
		runDir:     runDir,
		provenance: provenance,
		now:        now,
		redactor:   redactor,
		files:      make(map[TimelineStream]*os.File, len(streamOrder)),
		paths:      make(map[TimelineStream]string, len(streamOrder)),
		sequences:  make(map[TimelineStream]uint64, len(streamOrder)),
	}
	createdNames := make([]string, 0, len(streamOrder))
	for _, stream := range streamOrder {
		name := string(stream) + ".ndjson"
		file, err := runDir.createFile(name)
		if err != nil {
			_ = timeline.closeFiles(true)
			_ = runDir.cleanup(createdNames)
			if errors.Is(err, os.ErrExist) {
				return nil, ErrRunAlreadyExists
			}
			return nil, fmt.Errorf("create %s timeline: %w", stream, err)
		}
		timeline.files[stream] = file
		timeline.paths[stream] = filepath.Join(runDir.path(), name)
		createdNames = append(createdNames, name)
	}
	return timeline, nil
}

// Paths returns the four stream paths in deterministic order.
func (t *Timeline) Paths() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	paths := make([]string, 0, len(streamOrder))
	for _, stream := range streamOrder {
		paths = append(paths, t.paths[stream])
	}
	return paths
}

// RecordRevision appends one revision observation.
func (t *Timeline) RecordRevision(observation RevisionObservation) error {
	if strings.TrimSpace(observation.Domain) == "" ||
		strings.TrimSpace(observation.Shard) == "" ||
		strings.TrimSpace(observation.Source) == "" {
		return errors.New("revision observation requires domain, shard, and source")
	}
	observation.Domain = t.redactor.Redact(observation.Domain)
	observation.Shard = t.redactor.Redact(observation.Shard)
	observation.Source = t.redactor.Redact(observation.Source)
	return t.record(StreamRevision, observation)
}

// RecordAuthority appends one authority observation.
func (t *Timeline) RecordAuthority(observation AuthorityObservation) error {
	if strings.TrimSpace(observation.Operation) == "" ||
		strings.TrimSpace(observation.TransitionID) == "" ||
		strings.TrimSpace(observation.State) == "" {
		return errors.New("authority observation requires operation, transition_id, and state")
	}
	observation.Operation = t.redactor.Redact(observation.Operation)
	observation.TransitionID = t.redactor.Redact(observation.TransitionID)
	observation.State = t.redactor.Redact(observation.State)
	return t.record(StreamAuthority, observation)
}

// RecordHolder appends one holder observation.
func (t *Timeline) RecordHolder(observation HolderObservation) error {
	if strings.TrimSpace(observation.HolderID) == "" ||
		strings.TrimSpace(observation.BootID) == "" ||
		strings.TrimSpace(observation.State) == "" {
		return errors.New("holder observation requires holder_id, boot_id, and state")
	}
	observation.HolderID = t.redactor.Redact(observation.HolderID)
	observation.BootID = t.redactor.Redact(observation.BootID)
	observation.State = t.redactor.Redact(observation.State)
	return t.record(StreamHolder, observation)
}

// RecordFault appends a fault phase observation. It never computes a verdict.
func (t *Timeline) RecordFault(observation FaultObservation) error {
	if strings.TrimSpace(observation.Name) == "" ||
		strings.TrimSpace(observation.Action) == "" ||
		!validFaultPhase(observation.Phase) {
		return errors.New("fault observation requires name, action, and a known phase")
	}
	observation.Name = t.redactor.Redact(observation.Name)
	observation.Action = t.redactor.Redact(observation.Action)
	observation.Note = t.redactor.Redact(observation.Note)
	return t.record(StreamFault, observation)
}

// Flush syncs all open stream files.
func (t *Timeline) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("timeline is closed")
	}
	if t.poison != nil {
		return t.poisonErrorLocked()
	}
	var joined error
	for _, stream := range streamOrder {
		file := t.files[stream]
		if file == nil {
			err := fmt.Errorf("%s: timeline file is closed", stream)
			t.poisonLocked(err)
			joined = errors.Join(joined, err)
			continue
		}
		if err := timelineSync(file); err != nil {
			wrapped := fmt.Errorf("%s: %w", stream, err)
			t.poisonLocked(wrapped)
			joined = errors.Join(joined, wrapped)
		}
	}
	return errors.Join(joined, t.poisonErrorLocked())
}

// Close flushes and closes all stream files. It is idempotent.
func (t *Timeline) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	return t.finishLocked(true)
}

// FinalizeManifest hashes the original open timeline files, writes an
// exclusive run-manifest.json through the held run-directory capability, and
// only then closes all capabilities. Callers must provide manifest metadata
// without Artifacts; the finalizer owns the bound artifact list.
func (t *Timeline) FinalizeManifest(manifest Manifest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("timeline is closed")
	}

	var joined error
	if t.poison != nil {
		joined = errors.Join(joined, t.poisonErrorLocked())
	} else if len(manifest.Artifacts) != 0 {
		joined = errors.Join(joined, fmt.Errorf("%w: finalization owns timeline artifacts", ErrInvalidManifest))
	} else {
		final := manifest.Redacted(t.redactor)
		if final.Provenance != t.provenance ||
			final.TaskID != t.provenance.TaskID ||
			final.RunID != t.provenance.RunID ||
			final.BaseCommit != t.provenance.BaseCommit ||
			final.HeadCommit != t.provenance.HeadCommit ||
			final.Branch != t.provenance.Branch ||
			final.Worktree != t.provenance.Worktree {
			joined = errors.Join(joined, fmt.Errorf("%w: final manifest provenance does not match timeline", ErrInvalidManifest))
		} else {
			artifacts := make([]Artifact, 0, len(streamOrder))
			for _, stream := range streamOrder {
				file := t.files[stream]
				if file == nil {
					joined = errors.Join(joined, fmt.Errorf("%s: timeline file is closed", stream))
					continue
				}
				artifact, err := computeOpenArtifact(file, string(stream)+".ndjson", string(stream)+".ndjson")
				if err != nil {
					wrapped := fmt.Errorf("%s: %w", stream, err)
					t.poisonLocked(wrapped)
					joined = errors.Join(joined, wrapped)
					continue
				}
				artifacts = append(artifacts, artifact)
			}
			final.Artifacts = artifacts
			if joined == nil && t.poison == nil {
				if err := final.validate(NewRedactor(), true); err != nil {
					joined = errors.Join(joined, err)
				} else {
					var encoded strings.Builder
					encoder := json.NewEncoder(&encoded)
					encoder.SetIndent("", "  ")
					encoder.SetEscapeHTML(false)
					if err := encoder.Encode(final); err != nil {
						joined = errors.Join(joined, fmt.Errorf("encode final manifest: %w", err))
					} else if err := t.runDir.createManifest("run-manifest.json", []byte(encoded.String())); err != nil {
						joined = errors.Join(joined, err)
					}
				}
			}
		}
	}
	if t.poison != nil {
		joined = errors.Join(joined, t.poisonErrorLocked())
	}
	joined = errors.Join(joined, t.finishLocked(false))
	return joined
}

type timelineEnvelope struct {
	Schema      string         `json:"schema"`
	Stream      TimelineStream `json:"stream"`
	Sequence    uint64         `json:"sequence"`
	RecordedAt  string         `json:"recorded_at"`
	Provenance  Provenance     `json:"provenance"`
	Observation any            `json:"observation"`
}

func (t *Timeline) record(stream TimelineStream, observation any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("timeline is closed")
	}
	if t.poison != nil {
		return t.poisonErrorLocked()
	}
	t.sequences[stream]++
	now := t.now().UTC().Format(time.RFC3339Nano)
	envelope := timelineEnvelope{
		Schema:      SchemaVersion,
		Stream:      stream,
		Sequence:    t.sequences[stream],
		RecordedAt:  now,
		Provenance:  t.provenance,
		Observation: observation,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode %s timeline: %w", stream, err)
	}
	encoded = append(encoded, '\n')
	file := t.files[stream]
	if file == nil {
		err := fmt.Errorf("%s: timeline file is closed", stream)
		t.poisonLocked(err)
		return err
	}
	written, err := timelineWrite(file, encoded)
	if err != nil {
		wrapped := fmt.Errorf("write %s timeline: %w", stream, err)
		t.poisonLocked(wrapped)
		return wrapped
	}
	if written != len(encoded) {
		err := fmt.Errorf("write %s timeline: %w (%d/%d bytes)", stream, io.ErrShortWrite, written, len(encoded))
		t.poisonLocked(err)
		return err
	}
	return nil
}

func (t *Timeline) closeFiles(syncFiles bool) error {
	var joined error
	for _, stream := range streamOrder {
		file := t.files[stream]
		if file == nil {
			continue
		}
		if syncFiles {
			if err := timelineSync(file); err != nil {
				wrapped := fmt.Errorf("%s sync: %w", stream, err)
				t.poisonLocked(wrapped)
				joined = errors.Join(joined, wrapped)
			}
		}
		if err := file.Close(); err != nil {
			joined = errors.Join(joined, fmt.Errorf("%s close: %w", stream, err))
		}
		t.files[stream] = nil
	}
	return joined
}

func (t *Timeline) finishLocked(syncFiles bool) error {
	t.closed = true
	return errors.Join(t.closeFiles(syncFiles), t.runDir.close(), t.poisonErrorLocked())
}

func (t *Timeline) poisonLocked(err error) {
	if err == nil {
		return
	}
	if t.poison == nil {
		t.poison = err
		return
	}
	t.poison = errors.Join(t.poison, err)
}

func (t *Timeline) poisonErrorLocked() error {
	if t.poison == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrTimelinePoisoned, t.poison)
}

func validFaultPhase(phase FaultPhase) bool {
	switch phase {
	case FaultBeforeSend, FaultInFlight, FaultAfterApply, FaultResponseLost, FaultRecovered, FaultShutdown:
		return true
	default:
		return false
	}
}
