package demoorchestrator

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"farmgame/internal/demokit"
)

const (
	// DefaultReplayMaxFileBytes bounds every replay input opened by the loader.
	DefaultReplayMaxFileBytes int64 = 4 << 20
	// DefaultReplayMaxTotalBytes bounds all files consumed across a replay root.
	DefaultReplayMaxTotalBytes int64 = 32 << 20
	// DefaultReplayMaxEvents bounds decoded event and plan records.
	DefaultReplayMaxEvents = 100000
	// DefaultReplayMaxBundles bounds candidate bundles retained during discovery.
	DefaultReplayMaxBundles = 256
)

const replayDirectoryReadBatch = 64

// ReplayLimits bounds untrusted replay evidence before it reaches the store.
type ReplayLimits struct {
	MaxFileBytes  int64
	MaxTotalBytes int64
	MaxEvents     int
	MaxBundles    int
}

// DefaultReplayLimits returns the conservative limits used by ReadReplay.
func DefaultReplayLimits() ReplayLimits {
	return ReplayLimits{
		MaxFileBytes:  DefaultReplayMaxFileBytes,
		MaxTotalBytes: DefaultReplayMaxTotalBytes,
		MaxEvents:     DefaultReplayMaxEvents,
		MaxBundles:    DefaultReplayMaxBundles,
	}
}

func (limits ReplayLimits) validate() error {
	if limits.MaxFileBytes <= 0 {
		return errors.New("replay max file bytes must be positive")
	}
	if limits.MaxTotalBytes <= 0 {
		return errors.New("replay max total bytes must be positive")
	}
	if limits.MaxEvents <= 0 {
		return errors.New("replay max events must be positive")
	}
	if limits.MaxBundles <= 0 {
		return errors.New("replay max bundles must be positive")
	}
	maxInt := int64(^uint(0) >> 1)
	if limits.MaxFileBytes >= maxInt {
		return errors.New("replay max file bytes is too large")
	}
	return nil
}

type replayBudget struct {
	limits     ReplayLimits
	totalBytes int64
	events     int
	bundles    int
}

func (budget *replayBudget) claimEvent(path string, lineNumber int) error {
	if budget.events >= budget.limits.MaxEvents {
		return fmt.Errorf(
			"replay event limit %d exceeded at %s line %d",
			budget.limits.MaxEvents,
			filepath.Base(path),
			lineNumber,
		)
	}
	budget.events++
	return nil
}

func (budget *replayBudget) claimBundle(path string) error {
	if budget.bundles >= budget.limits.MaxBundles {
		return fmt.Errorf("replay bundle limit %d exceeded at %q", budget.limits.MaxBundles, path)
	}
	budget.bundles++
	return nil
}

type boundedReplayInput struct {
	file   *os.File
	reader *io.LimitedReader
	path   string
	limit  int64
}

func (budget *replayBudget) openInput(path string) (*boundedReplayInput, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("replay input is not a regular file: %q", path)
	}
	if info.Size() > budget.limits.MaxFileBytes {
		return nil, fmt.Errorf(
			"replay input %q exceeds per-file limit of %d bytes",
			path,
			budget.limits.MaxFileBytes,
		)
	}
	if info.Size() > budget.limits.MaxTotalBytes-budget.totalBytes {
		return nil, fmt.Errorf(
			"replay input total exceeds %d bytes while opening %q",
			budget.limits.MaxTotalBytes,
			path,
		)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &boundedReplayInput{
		file:   file,
		reader: &io.LimitedReader{R: file, N: budget.limits.MaxFileBytes + 1},
		path:   path,
		limit:  budget.limits.MaxFileBytes,
	}, nil
}

func (input *boundedReplayInput) closeAndAccount(budget *replayBudget) error {
	closeErr := input.file.Close()
	consumed := input.limit + 1 - input.reader.N
	if consumed > input.limit {
		return fmt.Errorf(
			"replay input %q exceeds per-file limit of %d bytes",
			input.path,
			input.limit,
		)
	}
	if consumed > budget.limits.MaxTotalBytes-budget.totalBytes {
		return fmt.Errorf(
			"replay input total exceeds %d bytes while reading %q",
			budget.limits.MaxTotalBytes,
			input.path,
		)
	}
	budget.totalBytes += consumed
	if closeErr != nil {
		return fmt.Errorf("close replay input %q: %w", input.path, closeErr)
	}
	return nil
}

// EvidenceReader is the read-only surface consumed by the HTTP controller.
// Implementations may be backed by a live append-only store or a finite
// replay-loaded store, but neither implementation exposes event mutation via
// HTTP.
type EvidenceReader interface {
	Mode() demokit.EvidenceMode
	RunViews() []demokit.RunView
	RunView(runID string) (demokit.RunView, bool)
	Page(runID string, afterSeq uint64, limit int) EventPage
	Subscribe(runID string, afterSeq uint64) Subscription
	Stats() StoreStats
}

// LiveEvidence is the in-process LIVE adapter. Trusted in-process producers
// append to Store; the HTTP layer receives only the EvidenceReader methods.
type LiveEvidence struct {
	store *EventStore
}

// NewLiveEvidence wraps a store, creating a default bounded store if nil is
// supplied.
func NewLiveEvidence(store *EventStore) *LiveEvidence {
	if store == nil {
		store = NewEventStore(DefaultEventCapacity)
	}
	return &LiveEvidence{store: store}
}

// Store returns the append-only store for an in-process evidence producer.
func (evidence *LiveEvidence) Store() *EventStore {
	return evidence.store
}

// Append is intentionally a Go-library method for an in-process producer; no
// HTTP route calls it.
func (evidence *LiveEvidence) Append(event demokit.Event) (demokit.Event, error) {
	return evidence.store.Append(event)
}

func (evidence *LiveEvidence) Mode() demokit.EvidenceMode {
	return evidence.store.Mode()
}

func (evidence *LiveEvidence) RunViews() []demokit.RunView {
	return evidence.store.RunViews()
}

func (evidence *LiveEvidence) RunView(runID string) (demokit.RunView, bool) {
	return evidence.store.RunView(runID)
}

func (evidence *LiveEvidence) Page(runID string, afterSeq uint64, limit int) EventPage {
	return evidence.store.Page(runID, afterSeq, limit)
}

func (evidence *LiveEvidence) Subscribe(runID string, afterSeq uint64) Subscription {
	return evidence.store.Subscribe(runID, afterSeq)
}

func (evidence *LiveEvidence) Stats() StoreStats {
	return evidence.store.Stats()
}

// ReplayEvidence is a finite, read-only projection of one evidence bundle or
// a directory containing multiple bundle subdirectories.
type ReplayEvidence struct {
	store     *EventStore
	directory string
}

// NewReplayEvidence loads events.ndjson when present. For the current
// plan-only evidence format it also understands action-plan.ndjson and
// produces explicitly inert plan.step observations, preserving PLAN_ONLY
// semantics in the RunView.
func NewReplayEvidence(directory string, capacity int) (*ReplayEvidence, error) {
	return NewReplayEvidenceWithLimits(directory, capacity, DefaultReplayLimits())
}

// NewReplayEvidenceWithLimits loads replay evidence under explicit safety
// limits. Files are streamed directly into the bounded store.
func NewReplayEvidenceWithLimits(
	directory string,
	capacity int,
	limits ReplayLimits,
) (*ReplayEvidence, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("replay evidence directory is required")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve replay evidence directory: %w", err)
	}
	if err := requireDirectory(absolute); err != nil {
		return nil, err
	}
	store, err := NewEventStoreChecked(capacity, demokit.EvidenceModeReplay)
	if err != nil {
		return nil, err
	}
	budget := &replayBudget{limits: limits}
	if err := loadReplayRoot(absolute, store, budget); err != nil {
		return nil, err
	}
	if len(store.RunViews()) == 0 {
		return nil, fmt.Errorf("no evidence bundle found below %q", absolute)
	}
	return &ReplayEvidence{store: store, directory: absolute}, nil
}

// ReadReplay is the default-capacity constructor used by callers that do not
// need to tune the bounded replay window.
func ReadReplay(directory string) (*ReplayEvidence, error) {
	return NewReplayEvidence(directory, DefaultEventCapacity)
}

// Directory returns the normalized source directory for diagnostics.
func (evidence *ReplayEvidence) Directory() string {
	return evidence.directory
}

func (evidence *ReplayEvidence) Mode() demokit.EvidenceMode {
	return evidence.store.Mode()
}

func (evidence *ReplayEvidence) RunViews() []demokit.RunView {
	return evidence.store.RunViews()
}

func (evidence *ReplayEvidence) RunView(runID string) (demokit.RunView, bool) {
	return evidence.store.RunView(runID)
}

func (evidence *ReplayEvidence) Page(runID string, afterSeq uint64, limit int) EventPage {
	return evidence.store.Page(runID, afterSeq, limit)
}

func (evidence *ReplayEvidence) Subscribe(runID string, afterSeq uint64) Subscription {
	subscription := evidence.store.Subscribe(runID, afterSeq)
	// A replay has no live suffix. Keep the race-free Initial prefix and close
	// the channel before returning so an SSE client receives it and completes.
	subscription.Close()
	return subscription
}

func (evidence *ReplayEvidence) Stats() StoreStats {
	return evidence.store.Stats()
}

type replayManifest struct {
	FormatVersion string    `json:"format_version"`
	RunID         string    `json:"run_id"`
	ScenarioID    string    `json:"scenario_id"`
	SourceRef     string    `json:"source_ref"`
	CreatedAt     time.Time `json:"created_at"`
	Execution     string    `json:"execution"`
}

type replayVerdict struct {
	RunID string `json:"run_id"`
}

type replayScenario struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type replayActionPlanLine struct {
	ScenarioID string `json:"scenario_id"`
	StepID     string `json:"step_id"`
	Action     string `json:"action"`
	Expected   string `json:"expected"`
	Executable bool   `json:"executable"`
}

func requireDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect replay evidence directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("replay evidence path is not a regular directory: %q", path)
	}
	return nil
}

func loadReplayRoot(root string, store *EventStore, budget *replayBudget) error {
	if isBundleDirectory(root) {
		if err := budget.claimBundle(root); err != nil {
			return err
		}
		return loadReplayBundle(root, store, budget)
	}

	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open replay evidence root: %w", err)
	}
	defer directory.Close()

	bundles := make([]string, 0)
	for {
		entries, readErr := directory.ReadDir(replayDirectoryReadBatch)
		for _, entry := range entries {
			if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			path := filepath.Join(root, entry.Name())
			if !isBundleDirectory(path) {
				continue
			}
			if err := budget.claimBundle(path); err != nil {
				return err
			}
			bundles = append(bundles, path)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read replay evidence root: %w", readErr)
		}
	}
	if len(bundles) == 0 {
		return fmt.Errorf("no replay bundle files found below %q", root)
	}
	sort.Strings(bundles)
	for _, bundle := range bundles {
		if err := loadReplayBundle(bundle, store, budget); err != nil {
			return err
		}
	}
	return nil
}

func isBundleDirectory(path string) bool {
	for _, name := range []string{
		"run-manifest.json",
		"verdict.json",
		"events.ndjson",
		"action-plan.ndjson",
	} {
		info, err := os.Lstat(filepath.Join(path, name))
		if err == nil && info.Mode().IsRegular() {
			return true
		}
	}
	return false
}

func loadReplayBundle(directory string, store *EventStore, budget *replayBudget) error {
	manifest, manifestErr := readReplayManifest(directory, budget)
	if manifestErr != nil && !errors.Is(manifestErr, os.ErrNotExist) {
		return manifestErr
	}
	verdict, verdictErr := readReplayVerdict(directory, budget)
	if verdictErr != nil && !errors.Is(verdictErr, os.ErrNotExist) {
		return verdictErr
	}
	scenario, scenarioErr := readReplayScenario(directory, budget)
	if scenarioErr != nil && !errors.Is(scenarioErr, os.ErrNotExist) {
		return scenarioErr
	}

	manifestRunID := strings.TrimSpace(manifest.RunID)
	verdictRunID := strings.TrimSpace(verdict.RunID)
	if manifestRunID != "" && verdictRunID != "" && manifestRunID != verdictRunID {
		return fmt.Errorf(
			"replay bundle %q has conflicting manifest run_id %q and verdict run_id %q",
			directory,
			manifestRunID,
			verdictRunID,
		)
	}
	runID := manifestRunID
	if runID == "" {
		runID = verdictRunID
	}
	metadata := demokit.RunView{
		RunID:         runID,
		ScenarioID:    manifest.ScenarioID,
		ScenarioTitle: scenario.Title,
		SourceRef:     manifest.SourceRef,
		Mode:          demokit.EvidenceModeReplay,
		Execution:     manifest.Execution,
		Status:        "read_only",
	}
	if metadata.Execution == "" && manifestRunID != "" {
		metadata.Execution = "REPLAY"
	}
	if !manifest.CreatedAt.IsZero() {
		metadata.StartedAtMs = manifest.CreatedAt.UnixMilli()
	}

	eventsPath := filepath.Join(directory, "events.ndjson")
	eventsExist, err := regularReplayInputExists(eventsPath)
	if err != nil {
		return err
	}
	if eventsExist {
		return streamReplayEvents(eventsPath, store, metadata, runID, budget)
	}

	planPath := filepath.Join(directory, "action-plan.ndjson")
	planExists, err := regularReplayInputExists(planPath)
	if err != nil {
		return err
	}
	if planExists {
		if runID == "" {
			return fmt.Errorf("plan-only replay bundle %q has no run_id", directory)
		}
		metadata.RunID = runID
		if metadata.Execution == "" {
			metadata.Execution = "PLAN_ONLY"
		}
		if err := store.RegisterRun(metadata); err != nil {
			return err
		}
		return streamReplayPlan(planPath, store, metadata, budget)
	}

	if runID == "" {
		return fmt.Errorf("replay bundle %q has no manifest, verdict, or event identity", directory)
	}
	metadata.RunID = runID
	return store.RegisterRun(metadata)
}

func regularReplayInputExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect replay input %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("replay input is not a regular file: %q", path)
	}
	return true, nil
}

func streamReplayEvents(
	path string,
	store *EventStore,
	metadata demokit.RunView,
	expectedRunID string,
	budget *replayBudget,
) error {
	runID := strings.TrimSpace(expectedRunID)
	registered := false
	register := func() error {
		if registered {
			return nil
		}
		if runID == "" {
			return errors.New("replay run_id is required")
		}
		metadata.RunID = runID
		if err := store.RegisterRun(metadata); err != nil {
			return err
		}
		registered = true
		return nil
	}
	if runID != "" {
		if err := register(); err != nil {
			return err
		}
	}

	err := scanReplayNDJSON(path, budget, func(line []byte, lineNumber int) error {
		var event demokit.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("decode %s line %d: %w", filepath.Base(path), lineNumber, err)
		}
		event.RunID = strings.TrimSpace(event.RunID)
		if event.RunID == "" {
			event.RunID = runID
		}
		if runID == "" {
			runID = event.RunID
			if runID == "" {
				return fmt.Errorf("%s line %d has no run_id", filepath.Base(path), lineNumber)
			}
			if err := register(); err != nil {
				return err
			}
		}
		if event.RunID != runID {
			return fmt.Errorf(
				"%s line %d run_id %q does not match bundle run_id %q",
				filepath.Base(path),
				lineNumber,
				event.RunID,
				runID,
			)
		}
		if event.TsMs == 0 {
			event.TsMs = metadata.StartedAtMs
		}
		if err := budget.claimEvent(path, lineNumber); err != nil {
			return err
		}
		if _, err := store.Append(event); err != nil {
			return fmt.Errorf("append replay event from %s line %d: %w", filepath.Base(path), lineNumber, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if runID == "" {
		return fmt.Errorf("replay events file %q has no run_id", path)
	}
	return register()
}

func streamReplayPlan(
	path string,
	store *EventStore,
	metadata demokit.RunView,
	budget *replayBudget,
) error {
	return scanReplayNDJSON(path, budget, func(line []byte, lineNumber int) error {
		var plan replayActionPlanLine
		if err := json.Unmarshal(line, &plan); err != nil {
			return fmt.Errorf("decode %s line %d: %w", filepath.Base(path), lineNumber, err)
		}
		if err := budget.claimEvent(path, lineNumber); err != nil {
			return err
		}
		event := demokit.Event{
			TsMs:    metadata.StartedAtMs,
			RunID:   metadata.RunID,
			Kind:    demokit.KindPlanStep,
			Phase:   "PLAN_ONLY",
			Message: plan.Action,
			Fields: map[string]any{
				"scenario_id": plan.ScenarioID,
				"step_id":     plan.StepID,
				"expected":    plan.Expected,
				"executable":  plan.Executable,
			},
		}
		if _, err := store.Append(event); err != nil {
			return fmt.Errorf("append replay plan from %s line %d: %w", filepath.Base(path), lineNumber, err)
		}
		return nil
	})
}

func scanReplayNDJSON(
	path string,
	budget *replayBudget,
	consume func([]byte, int) error,
) error {
	input, err := budget.openInput(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}

	maxTokenSize := int(budget.limits.MaxFileBytes)
	initialBufferSize := 64 << 10
	if maxTokenSize < initialBufferSize {
		initialBufferSize = maxTokenSize
	}
	scanner := bufio.NewScanner(input.reader)
	scanner.Buffer(make([]byte, initialBufferSize), maxTokenSize)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := consume([]byte(line), lineNumber); err != nil {
			_ = input.file.Close()
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		_ = input.file.Close()
		return fmt.Errorf("read %s: %w", filepath.Base(path), err)
	}
	return input.closeAndAccount(budget)
}

func readReplayManifest(directory string, budget *replayBudget) (replayManifest, error) {
	var manifest replayManifest
	if err := readJSONFile(filepath.Join(directory, "run-manifest.json"), &manifest, budget); err != nil {
		return replayManifest{}, err
	}
	return manifest, nil
}

func readReplayVerdict(directory string, budget *replayBudget) (replayVerdict, error) {
	var verdict replayVerdict
	if err := readJSONFile(filepath.Join(directory, "verdict.json"), &verdict, budget); err != nil {
		return replayVerdict{}, err
	}
	return verdict, nil
}

func readReplayScenario(directory string, budget *replayBudget) (replayScenario, error) {
	var scenario replayScenario
	if err := readJSONFile(filepath.Join(directory, "scenario-contract.json"), &scenario, budget); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return replayScenario{}, err
		}
		if err := readJSONFile(filepath.Join(directory, "scenario.json"), &scenario, budget); err != nil {
			return replayScenario{}, err
		}
	}
	return scenario, nil
}

func readJSONFile(path string, destination any, budget *replayBudget) error {
	input, err := budget.openInput(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(input.reader)
	if err := decoder.Decode(destination); err != nil {
		_ = input.file.Close()
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = input.file.Close()
		if err == nil {
			return fmt.Errorf("decode %s: trailing JSON value", filepath.Base(path))
		}
		return fmt.Errorf("decode %s trailing data: %w", filepath.Base(path), err)
	}
	return input.closeAndAccount(budget)
}
