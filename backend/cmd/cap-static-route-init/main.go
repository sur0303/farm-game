package main

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	"farmgame/internal/testkit/capstaticroute"

	"github.com/redis/go-redis/v9"
)

const identitySchema = "farm-local-static-route-init-identity.v1"

type options struct {
	applyLocalProfileB bool
	validateBundle     bool
	requireFresh       bool
	runID              string
	profileID          string
	generation         uint64
	shardCount         uint
	namespaceEnv       string
	namespaceZone      string
	redisURLFile       string
	identityFile       string
	outputFile         string
	ledgerFile         string
	receiptFile        string
	intentFile         string
	leaseTTL           time.Duration
	minimumRemaining   time.Duration
	timeout            time.Duration
}

type runtimeIdentity struct {
	SchemaVersion   string `json:"schema_version"`
	RunID           string `json:"run_id"`
	ProfileID       string `json:"profile_id"`
	Generation      uint64 `json:"generation"`
	ControllerNonce string `json:"controller_nonce"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("cap-static-route-init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts options
	fs.BoolVar(&opts.applyLocalProfileB, "apply-local-profile-b", false, "contact only an allowlisted local Redis Profile B")
	fs.BoolVar(&opts.validateBundle, "validate-bundle", false, "strictly validate route, ledger and receipt without Redis")
	fs.BoolVar(&opts.requireFresh, "require-fresh", false, "require the validated bundle to retain its startup lease budget")
	fs.StringVar(&opts.runID, "run-id", "", "stable local run ID; generated when absent")
	fs.StringVar(&opts.profileID, "profile-id", "functional", "local placement profile")
	fs.Uint64Var(&opts.generation, "generation", 1, "controller generation")
	fs.UintVar(&opts.shardCount, "shard-count", 1024, "4 test shards or frozen 1024 shards per domain")
	fs.StringVar(&opts.namespaceEnv, "namespace-environment", "", "ordinary Redis namespace environment")
	fs.StringVar(&opts.namespaceZone, "namespace-zone", "z1", "ordinary Redis namespace zone")
	fs.StringVar(&opts.redisURLFile, "redis-url-file", "", "0600 file containing an allowlisted local Redis URL")
	fs.StringVar(&opts.identityFile, "identity-file", "", "0600 runtime identity file; created once and reused")
	fs.StringVar(&opts.outputFile, "route-out", "", "final complete RouteSnapshot output")
	fs.StringVar(&opts.ledgerFile, "ledger-out", "", "final exact per-shard authority ledger output")
	fs.StringVar(&opts.receiptFile, "receipt-out", "", "non-sensitive receipt commit output")
	fs.StringVar(&opts.intentFile, "intent-out", "", "optional dry-run intent output")
	fs.DurationVar(&opts.leaseTTL, "lease-ttl", 60*time.Second, "requested initial shard authority TTL")
	fs.DurationVar(&opts.minimumRemaining, "minimum-remaining-lease", 30*time.Second, "minimum final startup lease budget")
	fs.DurationVar(&opts.timeout, "timeout", 5*time.Minute, "bounded whole initialization timeout")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return errors.New("cap-static-route-init: invalid arguments")
	}
	if opts.shardCount > 1024 || opts.timeout < time.Second || opts.timeout > 10*time.Minute {
		return errors.New("cap-static-route-init: bounded arguments invalid")
	}
	if opts.validateBundle {
		return validateExistingBundle(opts, stdout)
	}
	if opts.requireFresh {
		return errors.New("cap-static-route-init: --require-fresh requires --validate-bundle")
	}

	identity, err := loadOrCreateIdentity(opts, !opts.applyLocalProfileB)
	if err != nil {
		return err
	}
	opts.runID = identity.RunID
	if opts.namespaceEnv == "" {
		digest := sha256.Sum256([]byte(opts.runID))
		opts.namespaceEnv = "l2r2" + hex.EncodeToString(digest[:6])
	}
	cfg, err := buildConfig(opts, identity)
	if err != nil {
		return err
	}
	if !opts.applyLocalProfileB {
		intent := cfg.PublicIntent()
		if opts.intentFile != "" {
			if err := atomicJSON(opts.intentFile, intent, 0o600); err != nil {
				return err
			}
		}
		return json.NewEncoder(stdout).Encode(intent)
	}
	if opts.redisURLFile == "" || opts.outputFile == "" || opts.ledgerFile == "" || opts.receiptFile == "" || opts.identityFile == "" {
		return errors.New("cap-static-route-init: apply mode requires Redis, identity, route, ledger and receipt files")
	}
	if err := validateDistinctPaths(opts.redisURLFile, opts.identityFile, opts.outputFile, opts.ledgerFile, opts.receiptFile); err != nil {
		return err
	}
	if err := archiveExistingOutputs(opts.outputFile, opts.ledgerFile, opts.receiptFile); err != nil {
		return err
	}

	rawURL, err := readProtectedValue(opts.redisURLFile)
	if err != nil {
		_ = atomicJSON(opts.receiptFile, capstaticroute.FailureReceipt(cfg, "LOCAL_REDIS_INPUT_REJECTED"), 0o640)
		return err
	}
	redisOptions, err := parseLocalRedisURL(rawURL)
	if err != nil {
		_ = atomicJSON(opts.receiptFile, capstaticroute.FailureReceipt(cfg, "NON_LOCAL_REDIS_REJECTED"), 0o640)
		return err
	}
	client := redis.NewClient(redisOptions)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = atomicJSON(opts.receiptFile, capstaticroute.FailureReceipt(cfg, "LOCAL_REDIS_UNAVAILABLE"), 0o640)
		return errors.New("cap-static-route-init: local Redis unavailable")
	}
	calculator, err := cfg.Calculator()
	if err != nil {
		return err
	}
	registry, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		return err
	}
	if err := registry.EnsureLoaded(ctx); err != nil {
		_ = atomicJSON(opts.receiptFile, capstaticroute.FailureReceipt(cfg, "SCRIPT_WARMUP_FAILED"), 0o640)
		return errors.New("cap-static-route-init: script warmup failed")
	}
	store, err := redisauthority.NewStore(client, registry, redisauthority.Options{
		Profile: cfg.Profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		return err
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(cfg.Profile, calculator)
	readerOptions.OperationTimeout = 2 * time.Second
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		return err
	}
	result, initErr := (&capstaticroute.Initializer{Store: store, Reader: reader, Config: cfg}).Run(ctx)
	if initErr != nil {
		if err := atomicJSON(opts.receiptFile, result.Receipt, 0o640); err != nil {
			return err
		}
		return errors.New("cap-static-route-init: initialization rejected")
	}
	if !result.Receipt.Complete || !result.Receipt.BootstrapMaterializable || result.Receipt.Activatable {
		return errors.New("cap-static-route-init: incomplete or product-activatable result")
	}
	receiptBytes, err := capstaticroute.MarshalReceipt(result.Receipt)
	if err != nil || capstaticroute.ValidateBundle(result.Bytes, result.LedgerBytes, receiptBytes, time.Now()) != nil {
		return errors.New("cap-static-route-init: final bundle self-validation failed")
	}
	durable, durableSnapshot, err := commitDurableSnapshot(ctx, client, cfg.Profile.Namespace, result.Snapshot)
	if err != nil {
		result.Receipt.Status = "REJECTED_DURABLE_SNAPSHOT_NOT_PROVEN"
		result.Receipt.Complete = false
		result.Receipt.BootstrapMaterializable = false
		result.Receipt.Activatable = false
		result.Receipt.FailureClass = "DURABLE_SNAPSHOT_NOT_PROVEN"
		_ = atomicJSON(opts.receiptFile, result.Receipt, 0o640)
		return errors.New("cap-static-route-init: durable route snapshot bootstrap rejected")
	}
	result.Snapshot = durableSnapshot
	result.Bytes, result.Receipt.RouteSnapshotSHA256, err = capstaticroute.MarshalSnapshot(durableSnapshot)
	if err != nil {
		return errors.New("cap-static-route-init: durable snapshot marshal failed")
	}
	result.Ledger.RouteSnapshotSHA256 = result.Receipt.RouteSnapshotSHA256
	result.LedgerBytes, result.Receipt.ExactLedgerSHA256, err = capstaticroute.MarshalLedger(result.Ledger)
	if err != nil {
		return errors.New("cap-static-route-init: durable ledger marshal failed")
	}
	result.Receipt.DurableSnapshotCommitted = true
	result.Receipt.DurableSnapshotRevision = durable.Revision
	result.Receipt.DurableSnapshotChecksumSHA256 = durable.ChecksumSHA256
	result.Receipt.DurableSnapshotTopologyFingerprint = durable.TopologyFingerprint
	receiptBytes, err = capstaticroute.MarshalReceipt(result.Receipt)
	if err != nil || capstaticroute.ValidateBundle(result.Bytes, result.LedgerBytes, receiptBytes, time.Now()) != nil {
		return errors.New("cap-static-route-init: durable bundle self-validation failed")
	}
	// Route and ledger are immutable data files. Receipt is renamed last and is
	// the only commit marker; any preceding write failure removes staged finals.
	if err := atomicBytes(opts.ledgerFile, result.LedgerBytes, 0o600); err != nil {
		return err
	}
	if err := atomicBytes(opts.outputFile, result.Bytes, 0o600); err != nil {
		_ = os.Remove(opts.ledgerFile)
		return err
	}
	if err := atomicBytes(opts.receiptFile, receiptBytes, 0o640); err != nil {
		_ = os.Remove(opts.outputFile)
		_ = os.Remove(opts.ledgerFile)
		return err
	}
	return json.NewEncoder(stdout).Encode(result.Receipt)
}

func commitDurableSnapshot(ctx context.Context, client redis.UniversalClient, namespace rediskey.Namespace, snapshot actorrouting.RouteSnapshot) (routecontrol.StoredRouteSnapshot, actorrouting.RouteSnapshot, error) {
	if ctx == nil || client == nil {
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, errors.New("cap-static-route-init: durable bootstrap inputs invalid")
	}
	opts := redisroutecontrol.DefaultOptions(namespace)
	opts.OperationTimeout = 2 * time.Second
	opts.MaxSnapshotBytes = 4 << 20
	store, err := redisroutecontrol.NewSnapshotStore(client, opts)
	if err != nil {
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, err
	}
	if err := store.EnsureLoaded(ctx); err != nil {
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, err
	}
	current, found, err := store.LoadCurrent(ctx)
	if err != nil {
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, err
	}
	candidate := actorrouting.CloneRouteSnapshot(snapshot)
	if found && !reflect.DeepEqual(current.Snapshot, candidate) {
		candidate.GlobalRevision = current.Revision + 1
		if err := actorrouting.ValidateRouteSnapshotUpdate(current.Snapshot, candidate); err != nil {
			return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, err
		}
	}
	expected, err := routecontrol.CanonicalizeRouteSnapshot(candidate)
	if err != nil {
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, err
	}
	issuer := controlfence.NewIssuer()
	token := issuer.Issue(fmt.Sprintf("cap-static-route-init:%s:%s:r%d", namespace.Environment, namespace.Zone, candidate.GlobalRevision), candidate.GlobalRevision)
	var commitErr error
	if found {
		_, commitErr = store.CompareAndSwap(ctx, token, current.Revision, candidate)
	} else {
		_, commitErr = store.Bootstrap(ctx, token, candidate)
	}
	committed, committedFound, loadErr := store.LoadCurrent(ctx)
	if loadErr != nil || !committedFound || !committed.EnvelopeEqual(expected) {
		if loadErr != nil {
			return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, loadErr
		}
		if commitErr != nil {
			return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, commitErr
		}
		return routecontrol.StoredRouteSnapshot{}, actorrouting.RouteSnapshot{}, errors.New("cap-static-route-init: durable snapshot differs")
	}
	return committed, actorrouting.CloneRouteSnapshot(committed.Snapshot), nil
}

func validateExistingBundle(opts options, stdout io.Writer) error {
	if opts.applyLocalProfileB || opts.outputFile == "" || opts.ledgerFile == "" || opts.receiptFile == "" {
		return errors.New("cap-static-route-init: validator requires only route, ledger and receipt files")
	}
	routeBytes, err := readBoundedRegular(opts.outputFile, 4<<20)
	if err != nil {
		return errors.New("cap-static-route-init: route input rejected")
	}
	ledgerBytes, err := readBoundedRegular(opts.ledgerFile, 16<<20)
	if err != nil {
		return errors.New("cap-static-route-init: ledger input rejected")
	}
	receiptBytes, err := readBoundedRegular(opts.receiptFile, 1<<20)
	if err != nil {
		return errors.New("cap-static-route-init: receipt input rejected")
	}
	if opts.requireFresh {
		err = capstaticroute.ValidateBundle(routeBytes, ledgerBytes, receiptBytes, time.Now())
	} else {
		err = capstaticroute.ValidateBundleEvidence(routeBytes, ledgerBytes, receiptBytes)
	}
	if err != nil {
		return errors.New("cap-static-route-init: bundle validation rejected")
	}
	return json.NewEncoder(stdout).Encode(map[string]any{
		"schema_version": "farm-local-static-route-init-validation.v1",
		"status":         "EXACT_BUNDLE_VALID", "freshness_required": opts.requireFresh,
		"company_redis_contacted": false, "active_binding_authorized": false,
		"capacity_constant_eligible": false, "supports_30m_dau": false,
	})
}

func buildConfig(opts options, identity runtimeIdentity) (capstaticroute.Config, error) {
	namespace, err := rediskey.NewNamespace(opts.namespaceEnv, opts.namespaceZone)
	if err != nil {
		return capstaticroute.Config{}, err
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	boot := func(id string) string {
		return fmt.Sprintf("%s-%s-g%d-%s", id, opts.profileID, opts.generation, identity.ControllerNonce)
	}
	cfg := capstaticroute.Config{
		RunID: opts.runID, ProfileID: opts.profileID, Generation: opts.generation, ControllerNonce: identity.ControllerNonce,
		Realm: "classic-farm-local-l2-r2", HashSeed: 202608030001, ShardCount: uint32(opts.shardCount),
		LeaseTTL: opts.leaseTTL, MinimumRemainingLease: opts.minimumRemaining, Profile: profile,
		Holders: []capstaticroute.Holder{
			{ID: "player-svr-0", BootID: boot("player-svr-0"), Endpoint: "player-svr-0.player-svr-headless.farm-l2-r2.svc.cluster.local:9100", Domain: "PLAYER"},
			{ID: "player-svr-1", BootID: boot("player-svr-1"), Endpoint: "player-svr-1.player-svr-headless.farm-l2-r2.svc.cluster.local:9100", Domain: "PLAYER"},
			{ID: "farm-svr-0", BootID: boot("farm-svr-0"), Endpoint: "farm-svr-0.farm-svr-headless.farm-l2-r2.svc.cluster.local:9090", Domain: "FARM"},
			{ID: "farm-svr-1", BootID: boot("farm-svr-1"), Endpoint: "farm-svr-1.farm-svr-headless.farm-l2-r2.svc.cluster.local:9090", Domain: "FARM"},
		},
		RouteManagerID:     "cap-static-route-init",
		RouteManagerBootID: fmt.Sprintf("cap-static-route-init-%s-g%d-%s", opts.runID, opts.generation, identity.ControllerNonce),
	}
	return cfg, cfg.Validate()
}

func loadOrCreateIdentity(opts options, dryRun bool) (runtimeIdentity, error) {
	if opts.identityFile != "" {
		if data, err := readProtectedFile(opts.identityFile, 8192); err == nil {
			var value runtimeIdentity
			if json.Unmarshal(data, &value) != nil || value.SchemaVersion != identitySchema ||
				(opts.runID != "" && value.RunID != opts.runID) || value.ProfileID != opts.profileID || value.Generation != opts.generation {
				return runtimeIdentity{}, errors.New("cap-static-route-init: identity file mismatch")
			}
			return value, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return runtimeIdentity{}, err
		}
	}
	raw := make([]byte, 20)
	if _, err := cryptorand.Read(raw); err != nil {
		return runtimeIdentity{}, err
	}
	runID := opts.runID
	if runID == "" {
		runID = "capr2-" + hex.EncodeToString(raw[:8])
	}
	value := runtimeIdentity{SchemaVersion: identitySchema, RunID: runID, ProfileID: opts.profileID, Generation: opts.generation, ControllerNonce: hex.EncodeToString(raw[8:])}
	if !dryRun {
		if opts.identityFile == "" {
			return runtimeIdentity{}, errors.New("cap-static-route-init: apply requires identity file")
		}
		if err := atomicJSON(opts.identityFile, value, 0o600); err != nil {
			return runtimeIdentity{}, err
		}
	}
	return value, nil
}

func readProtectedValue(path string) (string, error) {
	data, err := readProtectedFile(path, 4096)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(data))
	if value == "" || strings.ContainsAny(value, "\r\n\t ") {
		return "", errors.New("cap-static-route-init: protected input malformed")
	}
	return value, nil
}

func validateDistinctPaths(paths ...string) error {
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		absolute = filepath.Clean(absolute)
		if seen[absolute] {
			return errors.New("cap-static-route-init: input and output paths must be distinct")
		}
		seen[absolute] = true
	}
	return nil
}

func archiveExistingOutputs(routePath, ledgerPath, receiptPath string) error {
	type artifact struct {
		name    string
		path    string
		maximum int64
		data    []byte
	}
	artifacts := []artifact{
		{name: "route-snapshot.json", path: routePath, maximum: 4 << 20},
		{name: "exact-ledger.json", path: ledgerPath, maximum: 16 << 20},
		{name: "receipt.json", path: receiptPath, maximum: 1 << 20},
	}
	found := false
	digest := sha256.New()
	for index := range artifacts {
		data, err := readBoundedRegular(artifacts[index].path, artifacts[index].maximum)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.New("cap-static-route-init: existing output cannot be archived")
		}
		found = true
		artifacts[index].data = data
		_, _ = digest.Write([]byte(artifacts[index].name))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(data)
	}
	if !found {
		return nil
	}
	archiveRoot := filepath.Join(filepath.Dir(receiptPath), "attempt-history")
	if err := os.MkdirAll(archiveRoot, 0o700); err != nil {
		return err
	}
	archivePath := filepath.Join(archiveRoot, hex.EncodeToString(digest.Sum(nil)))
	if _, err := os.Stat(archivePath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.MkdirTemp(archiveRoot, ".attempt-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	if err := os.Chmod(temporary, 0o700); err != nil {
		return err
	}
	for _, artifact := range artifacts {
		if artifact.data == nil {
			continue
		}
		if err := atomicBytes(filepath.Join(temporary, artifact.name), artifact.data, 0o600); err != nil {
			return err
		}
	}
	if err := syncDirectory(temporary); err != nil {
		return err
	}
	if err := os.Rename(temporary, archivePath); err != nil {
		if _, statErr := os.Stat(archivePath); statErr == nil {
			return nil
		}
		return err
	}
	return syncDirectory(archiveRoot)
}

func readBoundedRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("cap-static-route-init: bounded input rejected")
	}
	return os.ReadFile(path)
}

func readProtectedFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > maximum {
		return nil, errors.New("cap-static-route-init: protected input rejected")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func parseLocalRedisURL(raw string) (*redis.Options, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "redis" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("cap-static-route-init: only credential-free local Redis is allowed")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber <= 0 || portNumber > 65535 {
		return nil, errors.New("cap-static-route-init: local Redis address rejected")
	}
	loopback := host == "127.0.0.1" || host == "localhost" || host == "::1"
	clusterLocal := host == "farm-local-redis.farm-l2-r2-deps.svc.cluster.local" && port == "6379"
	if !loopback && !clusterLocal {
		return nil, errors.New("cap-static-route-init: non-local Redis host rejected")
	}
	if parsed.Path != "" && parsed.Path != "/0" {
		return nil, errors.New("cap-static-route-init: only local Redis DB 0 is allowed")
	}
	options, err := redis.ParseURL(raw)
	if err != nil {
		return nil, errors.New("cap-static-route-init: Redis URL rejected")
	}
	return options, nil
}

func atomicJSON(path string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return atomicBytes(path, append(data, '\n'), mode)
}

func atomicBytes(path string, data []byte, mode os.FileMode) error {
	if path == "" {
		return errors.New("cap-static-route-init: output path required")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".cap-static-route-init-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
