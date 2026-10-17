// Command routectl is the production manual RoutePlan client. It has no
// Redis, Authority, Snapshot-store, or holder mutation dependency.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/routecontrol/transport"
	model "farmgame/internal/routeplan"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var (
	errUsage             = errors.New("routectl: invalid command or arguments")
	errVerificationMatch = errors.New("routectl: verification mismatch")
)

const (
	defaultDialTimeout = 5 * time.Second
	defaultRPCTimeout  = 10 * time.Second
	defaultMaxMessage  = 4 << 20
	minimumTimeout     = time.Millisecond
	maximumTimeout     = 30 * time.Second
	maximumTargetBytes = 320
	maximumTTLms       = int64((24 * time.Hour) / time.Millisecond)
)

type globalConfig struct {
	target                 string
	serverName             string
	expectedServerIdentity string
	certFile               string
	keyFile                string
	caFile                 string
	dialTimeout            time.Duration
	rpcTimeout             time.Duration
	maxMessageBytes        int
}

type fenceFlags struct {
	holderID           string
	holderBootID       string
	assignmentRevision uint64
	writerEpoch        uint64
	leaseEpoch         uint64
}

type redactedFence struct {
	HolderID           string `json:"holder_id"`
	HolderBootID       string `json:"holder_boot_id"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	WriterEpoch        uint64 `json:"writer_epoch"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
}

type redactedGeneration struct {
	HolderID                  string `json:"holder_id"`
	BootID                    string `json:"boot_id"`
	PodUID                    string `json:"pod_uid,omitempty"`
	WorkloadUID               string `json:"workload_uid,omitempty"`
	Ordinal                   int32  `json:"ordinal,omitempty"`
	DeploymentIdentityPresent bool   `json:"deployment_identity_present"`
}

type redactedPlan struct {
	PlanID                 string             `json:"plan_id"`
	TransitionID           string             `json:"transition_id"`
	Domain                 string             `json:"domain"`
	Shard                  uint32             `json:"shard"`
	SourceHolderID         string             `json:"source_holder_id"`
	SourceBootID           string             `json:"source_boot_id"`
	SourceEndpoint         string             `json:"source_endpoint"`
	TargetHolderID         string             `json:"target_holder_id"`
	TargetBootID           string             `json:"target_boot_id"`
	TargetEndpoint         string             `json:"target_endpoint"`
	ExpectedSourceFence    redactedFence      `json:"expected_source_fence"`
	Operation              string             `json:"operation"`
	Phase                  string             `json:"phase"`
	Attempt                uint64             `json:"attempt"`
	Revision               uint64             `json:"revision"`
	CreatedAtMs            int64              `json:"created_at_ms"`
	UpdatedAtMs            int64              `json:"updated_at_ms"`
	StartedAtMs            int64              `json:"started_at_ms"`
	CompletedAtMs          int64              `json:"completed_at_ms"`
	ExpiresAtMs            int64              `json:"expires_at_ms"`
	ErrorCode              string             `json:"error_code"`
	ChecksumSHA256         string             `json:"checksum_sha256"`
	RequestedTTLms         int64              `json:"requested_ttl_ms"`
	ProofHolderUnavailable bool               `json:"proof_holder_unavailable"`
	ProofLeaseExpired      bool               `json:"proof_lease_expired"`
	SourceGeneration       redactedGeneration `json:"source_generation"`
	TargetGeneration       redactedGeneration `json:"target_generation"`
	OwnerScalePlanID       string             `json:"owner_scale_plan_id,omitempty"`
	OwnerRetainUntilMs     int64              `json:"owner_retain_until_ms,omitempty"`
}

type redactedAuthority struct {
	Domain             string        `json:"domain"`
	Shard              uint32        `json:"shard"`
	State              string        `json:"state"`
	Fence              redactedFence `json:"fence"`
	LeaseUntilMs       int64         `json:"lease_until_ms"`
	LastTransitionID   string        `json:"last_transition_id"`
	LastTransitionAtMs int64         `json:"last_transition_at_ms"`
}

type redactedHolder struct {
	Domain                    string `json:"domain"`
	HolderID                  string `json:"holder_id"`
	BootID                    string `json:"boot_id"`
	LogicEndpoint             string `json:"logic_endpoint"`
	HolderControlEndpoint     string `json:"holder_control_endpoint"`
	TLSDNSIdentity            string `json:"tls_dns_identity"`
	State                     string `json:"state"`
	ProcessReady              bool   `json:"process_ready"`
	Draining                  bool   `json:"draining"`
	ServingShardCount         uint32 `json:"serving_shard_count"`
	SnapshotRevision          uint64 `json:"snapshot_revision"`
	HeartbeatAtMs             int64  `json:"heartbeat_at_ms"`
	LeaseUntilMs              int64  `json:"lease_until_ms"`
	Sequence                  uint64 `json:"sequence"`
	CapacityWeight            uint32 `json:"capacity_weight"`
	PodUID                    string `json:"pod_uid,omitempty"`
	WorkloadUID               string `json:"workload_uid,omitempty"`
	Ordinal                   int32  `json:"ordinal,omitempty"`
	DeploymentIdentityPresent bool   `json:"deployment_identity_present"`
}

type statusOutput struct {
	Command          string         `json:"command"`
	ControllerID     string         `json:"controller_id"`
	Boot             string         `json:"boot"`
	Realm            string         `json:"realm"`
	SnapshotRevision uint64         `json:"snapshot_revision"`
	Ready            bool           `json:"ready"`
	IncompletePlans  []redactedPlan `json:"incomplete_plans"`
}

type planOutput struct {
	Command string       `json:"command"`
	Plan    redactedPlan `json:"plan"`
}

type snapshotOutput struct {
	Command             string          `json:"command"`
	Schema              string          `json:"schema"`
	Realm               string          `json:"realm"`
	Revision            uint64          `json:"revision"`
	CanonicalJSON       json.RawMessage `json:"canonical_json"`
	ChecksumSHA256      string          `json:"checksum_sha256"`
	TopologyFingerprint string          `json:"topology_fingerprint"`
	CommittedAtMs       int64           `json:"committed_at_ms"`
}

type holdersOutput struct {
	Command string           `json:"command"`
	Domain  string           `json:"domain"`
	Holders []redactedHolder `json:"holders"`
}

type shardOutput struct {
	Command          string             `json:"command"`
	Domain           string             `json:"domain"`
	Shard            uint32             `json:"shard"`
	SnapshotState    string             `json:"snapshot_state"`
	SnapshotRevision uint64             `json:"snapshot_revision"`
	Authority        *redactedAuthority `json:"authority,omitempty"`
	RelatedPlans     []redactedPlan     `json:"related_plans"`
}

type verifyCheckOutput struct {
	Name      string `json:"name"`
	OK        bool   `json:"ok"`
	ErrorCode string `json:"error_code"`
}

type verifyOutput struct {
	Command    string              `json:"command"`
	Consistent bool                `json:"consistent"`
	Checks     []verifyCheckOutput `json:"checks"`
	Plan       redactedPlan        `json:"plan"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		// Do not print RPC errors: they may contain endpoints, evidence, or
		// transport details. The JSON response, when applicable, was already
		// emitted before returning a non-zero result.
		if os.Getenv("ROUTECTL_REDACTED_ERRORS") == "1" {
			_ = writeRedactedError(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func writeRedactedError(output io.Writer, err error) error {
	if output == nil || err == nil {
		return errUsage
	}
	code := status.Code(err)
	if code == codes.OK {
		code = codes.Unknown
	}
	return json.NewEncoder(output).Encode(struct {
		GRPCCode string `json:"grpc_code"`
	}{
		GRPCCode: code.String(),
	})
}

func run(args []string, output io.Writer) error {
	global, command, commandArgs, err := parseGlobal(args)
	if err != nil {
		return err
	}
	if err := global.validate(); err != nil {
		return err
	}
	conn, err := dialRoutePlan(global)
	if err != nil {
		return err
	}
	defer conn.Close()
	if command == "scaleplan" {
		return runScalePlanOverConn(global, commandArgs, conn, output)
	}
	client := routeplanpb.NewRoutePlanServiceClient(conn)
	return runCommand(global, command, commandArgs, client, output)
}

func parseGlobal(args []string) (globalConfig, string, []string, error) {
	fs := flag.NewFlagSet("routectl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg := globalConfig{}
	fs.StringVar(&cfg.target, "target", os.Getenv("ROUTE_CONTROLLER_TARGET"), "route-controller host:port")
	fs.StringVar(&cfg.serverName, "server-name", os.Getenv("ROUTE_CONTROLLER_SERVER_NAME"), "canonical TLS DNS identity")
	fs.StringVar(&cfg.expectedServerIdentity, "expected-server-identity", os.Getenv("ROUTE_CONTROLLER_EXPECTED_SERVER_IDENTITY"), "expected canonical TLS identity")
	fs.StringVar(&cfg.certFile, "tls-cert", os.Getenv("ROUTECTL_TLS_CERT"), "client TLS certificate path")
	fs.StringVar(&cfg.keyFile, "tls-key", os.Getenv("ROUTECTL_TLS_KEY"), "client TLS key path")
	fs.StringVar(&cfg.caFile, "tls-ca", os.Getenv("ROUTECTL_TLS_CA"), "server CA certificate path")
	fs.DurationVar(&cfg.dialTimeout, "dial-timeout", durationEnv("ROUTECTL_DIAL_TIMEOUT", defaultDialTimeout), "bounded TLS dial timeout")
	fs.DurationVar(&cfg.rpcTimeout, "rpc-timeout", durationEnv("ROUTECTL_RPC_TIMEOUT", defaultRPCTimeout), "bounded RPC timeout")
	fs.IntVar(&cfg.maxMessageBytes, "max-message-bytes", intEnv("ROUTECTL_MAX_MESSAGE_BYTES", defaultMaxMessage), "bounded gRPC message size")
	if err := fs.Parse(args); err != nil {
		return globalConfig{}, "", nil, errUsage
	}
	remaining := fs.Args()
	if len(remaining) == 0 {
		return globalConfig{}, "", nil, errUsage
	}
	if cfg.expectedServerIdentity == "" {
		cfg.expectedServerIdentity = cfg.serverName
	}
	return cfg, remaining[0], remaining[1:], nil
}

func (c globalConfig) validate() error {
	if strings.TrimSpace(c.target) != c.target || c.target == "" || strings.TrimSpace(c.serverName) != c.serverName || c.serverName == "" ||
		strings.TrimSpace(c.certFile) != c.certFile || c.certFile == "" || strings.TrimSpace(c.keyFile) != c.keyFile || c.keyFile == "" ||
		strings.TrimSpace(c.caFile) != c.caFile || c.caFile == "" {
		return errUsage
	}
	if c.expectedServerIdentity == "" {
		c.expectedServerIdentity = c.serverName
	}
	if c.expectedServerIdentity != c.serverName || !validCanonicalDNS(c.serverName) || !validCanonicalTarget(c.target) ||
		c.dialTimeout < minimumTimeout || c.dialTimeout > maximumTimeout || c.rpcTimeout < minimumTimeout || c.rpcTimeout > maximumTimeout ||
		c.maxMessageBytes < 1024 || c.maxMessageBytes > 4<<20 {
		return errUsage
	}
	return nil
}

func dialRoutePlan(cfg globalConfig) (*grpc.ClientConn, error) {
	tlsConfig, err := transport.LoadClientTLSConfig(cfg.certFile, cfg.keyFile, cfg.caFile)
	if err != nil {
		return nil, errUsage
	}
	tlsConfig = tlsConfig.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.ServerName = cfg.serverName
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		return verifyRouteControllerConnection(state, cfg.expectedServerIdentity)
	}
	dialCtx, cancel := context.WithTimeout(context.Background(), cfg.dialTimeout)
	defer cancel()
	return grpc.DialContext(dialCtx, cfg.target,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.maxMessageBytes), grpc.MaxCallSendMsgSize(cfg.maxMessageBytes)))
}

func runCommand(cfg globalConfig, command string, args []string, client routeplanpb.RoutePlanServiceClient, output io.Writer) error {
	switch command {
	case "status":
		fs := commandFlagSet(command)
		if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
			return errUsage
		}
		response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.GetStatusResponse, error) {
			return client.GetStatus(ctx, &routeplanpb.GetStatusRequest{})
		})
		if err != nil || response == nil {
			return errUsage
		}
		return writeJSON(output, statusOutput{Command: command, ControllerID: response.GetControllerId(), Boot: response.GetBoot(), Realm: response.GetRealm(), SnapshotRevision: response.GetSnapshotRevision(), Ready: response.GetReady(), IncompletePlans: plansRedacted(response.GetIncompletePlans())})
	case "plan":
		fs := commandFlagSet(command)
		planID := fs.String("plan-id", "", "exact RoutePlan ID")
		if err := fs.Parse(args); err != nil || fs.NArg() != 0 || model.ValidatePlanID(*planID) != nil {
			return errUsage
		}
		response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.GetPlanResponse, error) {
			return client.GetPlan(ctx, &routeplanpb.GetPlanRequest{PlanId: *planID})
		})
		if err != nil || response.GetPlan() == nil || response.GetPlan().GetPlanId() != *planID {
			return errUsage
		}
		return writeJSON(output, planOutput{Command: command, Plan: redactPlan(response.GetPlan())})
	case "snapshot":
		fs := commandFlagSet(command)
		if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
			return errUsage
		}
		response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.GetSnapshotResponse, error) {
			return client.GetSnapshot(ctx, &routeplanpb.GetSnapshotRequest{})
		})
		if err != nil || response == nil || !json.Valid(response.GetCanonicalJson()) {
			return errUsage
		}
		return writeJSON(output, snapshotOutput{Command: command, Schema: response.GetSchema(), Realm: response.GetRealm(), Revision: response.GetRevision(), CanonicalJSON: json.RawMessage(response.GetCanonicalJson()), ChecksumSHA256: response.GetChecksumSha256(), TopologyFingerprint: response.GetTopologyFingerprint(), CommittedAtMs: response.GetCommittedAtMs()})
	case "holders":
		fs := commandFlagSet(command)
		domain := fs.String("domain", "", "PLAYER, FARM, or FARM_WORKER")
		if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validObservedDomain(*domain) {
			return errUsage
		}
		response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.GetHoldersResponse, error) {
			return client.GetHolders(ctx, &routeplanpb.GetHoldersRequest{Domain: *domain})
		})
		if err != nil {
			return errUsage
		}
		holders := make([]redactedHolder, 0, len(response.GetHolders()))
		for _, holder := range response.GetHolders() {
			holders = append(holders, redactHolder(holder))
		}
		return writeJSON(output, holdersOutput{Command: command, Domain: *domain, Holders: holders})
	case "shard":
		fs := commandFlagSet(command)
		domain := fs.String("domain", "", "PLAYER, FARM, or FARM_WORKER")
		shard := registerShardFlag(fs)
		if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validObservedDomain(*domain) || !shard.set {
			return errUsage
		}
		response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.GetShardResponse, error) {
			return client.GetShard(ctx, &routeplanpb.GetShardRequest{Domain: *domain, Shard: shard.value})
		})
		if err != nil {
			return errUsage
		}
		if !validShardResponse(*domain, shard.value, response) {
			return errVerificationMatch
		}
		var authority *redactedAuthority
		if response.GetAuthority() != nil {
			value := redactAuthority(response.GetAuthority())
			authority = &value
		}
		return writeJSON(output, shardOutput{Command: command, Domain: response.GetDomain(), Shard: response.GetShard(), SnapshotState: response.GetSnapshotState(), SnapshotRevision: response.GetSnapshotRevision(), Authority: authority, RelatedPlans: plansRedacted(response.GetRelatedPlans())})
	case "move-shard":
		return runMove(cfg, args, client, output)
	case "drain-holder":
		return runDrain(cfg, args, client, output)
	case "takeover-after-expiry":
		return runTakeover(cfg, args, client, output)
	case "verify":
		return runVerify(cfg, args, client, output)
	default:
		return errUsage
	}
}

func runMove(cfg globalConfig, args []string, client routeplanpb.RoutePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("move-shard")
	request, fence, shard := moveFlags(fs)
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !shard.set || !validMoveRequest(request, fence) {
		return errUsage
	}
	request.Shard = shard.value
	request.ExpectedSourceFence = fenceProto(fence)
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.MoveShardResponse, error) {
		return client.MoveShard(ctx, request)
	})
	if err != nil {
		return err
	}
	if response.GetPlan() == nil {
		return errUsage
	}
	if err := writeJSON(output, struct {
		Command string       `json:"command"`
		Plan    redactedPlan `json:"plan"`
	}{Command: "move-shard", Plan: redactPlan(response.GetPlan())}); err != nil {
		return err
	}
	return requireSucceeded(response.GetPlan())
}

func runDrain(cfg globalConfig, args []string, client routeplanpb.RoutePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("drain-holder")
	planPrefix := fs.String("plan-id-prefix", "", "stable plan ID prefix")
	transitionPrefix := fs.String("transition-id-prefix", "", "stable transition ID prefix")
	domain := fs.String("domain", "", "PLAYER, FARM, or FARM_WORKER")
	sourceHolder := fs.String("source-holder-id", "", "source holder ID")
	sourceBoot := fs.String("source-boot-id", "", "source holder boot ID")
	targetHolder := fs.String("target-holder-id", "", "target holder ID")
	targetBoot := fs.String("target-boot-id", "", "target holder boot ID")
	targetEndpoint := fs.String("target-endpoint", "", "target internal endpoint")
	ttl := fs.Int64("requested-ttl-ms", 0, "requested authority TTL in milliseconds")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !validPlanIDPrefix(*planPrefix) || *transitionPrefix == "" || !validDomain(*domain) || *sourceHolder == "" || *sourceBoot == "" || *targetHolder == "" || *targetBoot == "" || *targetEndpoint == "" || !validRequestedTTL(*ttl) {
		return errUsage
	}
	request := &routeplanpb.DrainHolderRequest{PlanIdPrefix: *planPrefix, TransitionIdPrefix: *transitionPrefix, Domain: *domain, SourceHolderId: *sourceHolder, SourceBootId: *sourceBoot, TargetHolderId: *targetHolder, TargetBootId: *targetBoot, TargetEndpoint: *targetEndpoint, RequestedTtlMs: *ttl}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.DrainHolderResponse, error) {
		return client.DrainHolder(ctx, request)
	})
	if err != nil || response == nil || len(response.GetPlans()) == 0 {
		return errUsage
	}
	plans := plansRedacted(response.GetPlans())
	if err := writeJSON(output, struct {
		Command string         `json:"command"`
		Plans   []redactedPlan `json:"plans"`
	}{Command: "drain-holder", Plans: plans}); err != nil {
		return err
	}
	for _, plan := range response.GetPlans() {
		if err := requireSucceeded(plan); err != nil {
			return err
		}
	}
	return nil
}

func runTakeover(cfg globalConfig, args []string, client routeplanpb.RoutePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("takeover-after-expiry")
	planID := fs.String("plan-id", "", "stable plan ID")
	transitionID := fs.String("transition-id", "", "stable transition ID")
	domain := fs.String("domain", "", "PLAYER, FARM, or FARM_WORKER")
	shard := registerShardFlag(fs)
	oldHolder := fs.String("old-holder-id", "", "old holder ID")
	oldBoot := fs.String("old-boot-id", "", "old holder boot ID")
	targetHolder := fs.String("target-holder-id", "", "target holder ID")
	targetBoot := fs.String("target-boot-id", "", "target holder boot ID")
	targetEndpoint := fs.String("target-endpoint", "", "target internal endpoint")
	holderUnavailable := fs.Bool("holder-unavailable", false, "explicit old-holder unavailability proof")
	leaseExpired := fs.Bool("lease-expired", false, "explicit authority lease-expiry proof")
	evidence := fs.String("evidence", "", "non-empty operator evidence; never returned by route-controller")
	ttl := fs.Int64("requested-ttl-ms", 0, "requested authority TTL in milliseconds")
	fence := registerFenceFlags(fs)
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !shard.set || model.ValidatePlanID(*planID) != nil || *transitionID == "" || !validDomain(*domain) || *oldHolder == "" || *oldBoot == "" || *targetHolder == "" || *targetBoot == "" || *targetEndpoint == "" || !validRequestedTTL(*ttl) || (!*holderUnavailable && !*leaseExpired) || *evidence == "" || !validFence(fence) {
		return errUsage
	}
	request := &routeplanpb.TakeoverAfterExpiryRequest{PlanId: *planID, TransitionId: *transitionID, Domain: *domain, Shard: shard.value, OldHolderId: *oldHolder, OldBootId: *oldBoot, TargetHolderId: *targetHolder, TargetBootId: *targetBoot, TargetEndpoint: *targetEndpoint, HolderUnavailable: *holderUnavailable, LeaseExpired: *leaseExpired, Evidence: *evidence, RequestedTtlMs: *ttl, ExpectedSourceFence: fenceProto(fence)}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.TakeoverAfterExpiryResponse, error) {
		return client.TakeoverAfterExpiry(ctx, request)
	})
	if err != nil {
		return err
	}
	if response.GetPlan() == nil {
		return errUsage
	}
	if err := writeJSON(output, struct {
		Command string       `json:"command"`
		Plan    redactedPlan `json:"plan"`
	}{Command: "takeover-after-expiry", Plan: redactPlan(response.GetPlan())}); err != nil {
		return err
	}
	return requireSucceeded(response.GetPlan())
}

func runVerify(cfg globalConfig, args []string, client routeplanpb.RoutePlanServiceClient, output io.Writer) error {
	fs := commandFlagSet("verify")
	planID := fs.String("plan-id", "", "stable plan ID")
	domain := fs.String("domain", "", "PLAYER, FARM, or FARM_WORKER")
	shard := registerShardFlag(fs)
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || model.ValidatePlanID(*planID) != nil || !validDomain(*domain) || !shard.set {
		return errUsage
	}
	response, err := callWithTimeout(cfg, func(ctx context.Context) (*routeplanpb.VerifyResponse, error) {
		return client.Verify(ctx, &routeplanpb.VerifyRequest{PlanId: *planID, Domain: *domain, Shard: shard.value})
	})
	if err != nil || response.GetPlan() == nil {
		return errUsage
	}
	checks := make([]verifyCheckOutput, 0, len(response.GetChecks()))
	for _, check := range response.GetChecks() {
		checks = append(checks, verifyCheckOutput{Name: check.GetName(), OK: check.GetOk(), ErrorCode: check.GetErrorCode()})
	}
	if err := writeJSON(output, verifyOutput{Command: "verify", Consistent: response.GetConsistent(), Checks: checks, Plan: redactPlan(response.GetPlan())}); err != nil {
		return err
	}
	if !validVerifyResponse(*planID, *domain, shard.value, response) {
		return errVerificationMatch
	}
	return nil
}

func moveFlags(fs *flag.FlagSet) (*routeplanpb.MoveShardRequest, *fenceFlags, *shardFlagValue) {
	request := &routeplanpb.MoveShardRequest{}
	fs.StringVar(&request.PlanId, "plan-id", "", "stable plan ID")
	fs.StringVar(&request.TransitionId, "transition-id", "", "stable transition ID")
	fs.StringVar(&request.Domain, "domain", "", "PLAYER, FARM, or FARM_WORKER")
	shard := registerShardFlag(fs)
	fs.StringVar(&request.SourceHolderId, "source-holder-id", "", "source holder ID")
	fs.StringVar(&request.SourceBootId, "source-boot-id", "", "source holder boot ID")
	fs.StringVar(&request.SourceEndpoint, "source-endpoint", "", "source internal endpoint")
	fs.StringVar(&request.TargetHolderId, "target-holder-id", "", "target holder ID")
	fs.StringVar(&request.TargetBootId, "target-boot-id", "", "target holder boot ID")
	fs.StringVar(&request.TargetEndpoint, "target-endpoint", "", "target internal endpoint")
	fs.Int64Var(&request.RequestedTtlMs, "requested-ttl-ms", 0, "requested authority TTL in milliseconds")
	fence := registerFenceFlags(fs)
	return request, fence, shard
}

func validMoveRequest(request *routeplanpb.MoveShardRequest, fence *fenceFlags) bool {
	return model.ValidatePlanID(request.GetPlanId()) == nil && request.GetTransitionId() != "" && validDomain(request.GetDomain()) && request.GetSourceHolderId() != "" && request.GetSourceBootId() != "" && request.GetSourceEndpoint() != "" && request.GetTargetHolderId() != "" && request.GetTargetBootId() != "" && request.GetTargetEndpoint() != "" && validRequestedTTL(request.GetRequestedTtlMs()) && validFence(fence)
}

func validPlanIDPrefix(value string) bool {
	// DrainHolder appends the domain and an unsigned shard decimal before any
	// RPC side effect. Reserve the longest current domain and uint32 suffix so
	// every generated exact ID satisfies the shared RoutePlan key grammar.
	if value == "" || len(value)+1+len("FARM_WORKER")+1+10 > model.MaxPlanIDBytes {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

// registerFenceFlags returns a pointer so flag.FlagSet mutations land in the
// same storage validated after Parse. Returning fenceFlags by value copied an
// empty struct while the flag bindings kept updating the discarded original.
func registerFenceFlags(fs *flag.FlagSet) *fenceFlags {
	fence := &fenceFlags{}
	fs.StringVar(&fence.holderID, "expected-holder-id", "", "expected source holder ID")
	fs.StringVar(&fence.holderBootID, "expected-holder-boot-id", "", "expected source holder boot ID")
	fs.Uint64Var(&fence.assignmentRevision, "expected-assignment-revision", 0, "expected assignment revision")
	fs.Uint64Var(&fence.writerEpoch, "expected-writer-epoch", 0, "expected writer epoch")
	fs.Uint64Var(&fence.leaseEpoch, "expected-lease-epoch", 0, "expected lease epoch")
	return fence
}

func validFence(fence *fenceFlags) bool {
	return fence != nil && fence.holderID != "" && fence.holderBootID != "" && fence.assignmentRevision > 0 && fence.writerEpoch > 0 && fence.leaseEpoch > 0
}

func fenceProto(fence *fenceFlags) *routeplanpb.AuthorityFence {
	if fence == nil {
		return nil
	}
	return &routeplanpb.AuthorityFence{HolderId: fence.holderID, HolderBootId: fence.holderBootID, AssignmentRevision: fence.assignmentRevision, WriterEpoch: fence.writerEpoch, LeaseEpoch: fence.leaseEpoch}
}

func commandFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

type shardFlagValue struct {
	value uint32
	set   bool
}

func (s *shardFlagValue) String() string { return strconv.FormatUint(uint64(s.value), 10) }

func (s *shardFlagValue) Set(value string) error {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return errUsage
	}
	s.value = uint32(parsed)
	s.set = true
	return nil
}

func registerShardFlag(fs *flag.FlagSet) *shardFlagValue {
	value := &shardFlagValue{}
	fs.Var(value, "shard", "virtual shard")
	return value
}

func callWithTimeout[T any](cfg globalConfig, call func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.rpcTimeout)
	defer cancel()
	return call(ctx)
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func requireSucceeded(plan *routeplanpb.Plan) error {
	if plan == nil || plan.GetPhase() != routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED {
		return errUsage
	}
	return nil
}

func validVerifyResponse(planID, domain string, shard uint32, response *routeplanpb.VerifyResponse) bool {
	if response == nil || !validVerifyPlan(planID, domain, shard, response.GetPlan()) {
		return false
	}
	required := map[string]string{
		"authority_active_lease": "OK", "authority_tuple": "AUTHORITY_TUPLE", "snapshot_read": "OK",
		"snapshot_tuple": "SNAPSHOT_TUPLE", "target_holder": "OK", "gate_resolution": "GATE_RESOLUTION",
		"transition": "TRANSITION", "recovery": "RECOVERY", "tombstone": "TOMBSTONE", "plan_state": "PLAN_STATE",
		"final_freshness": "FINAL_FRESHNESS",
	}
	seen := make(map[string]struct{}, len(required))
	for _, check := range response.GetChecks() {
		if check == nil {
			return false
		}
		expectedCode, ok := required[check.GetName()]
		if !ok {
			return false
		}
		if _, duplicate := seen[check.GetName()]; duplicate || !check.GetOk() || check.GetErrorCode() != expectedCode {
			return false
		}
		seen[check.GetName()] = struct{}{}
	}
	for name := range required {
		if _, present := seen[name]; !present {
			return false
		}
	}
	return response.GetConsistent() && response.GetPlan().GetPhase() == routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED
}

func validVerifyPlan(planID, domain string, shard uint32, plan *routeplanpb.Plan) bool {
	if plan == nil || plan.GetPlanId() != planID || plan.GetDomain() != domain || plan.GetShard() != shard ||
		!validResponseOpaque(plan.GetPlanId(), 128) || !validResponseOpaque(plan.GetTransitionId(), 128) ||
		!validResponseOpaque(plan.GetSourceHolderId(), 128) || !validResponseOpaque(plan.GetSourceBootId(), 128) || !validResponseOpaque(plan.GetSourceEndpoint(), 512) ||
		!validResponseOpaque(plan.GetTargetHolderId(), 128) || !validResponseOpaque(plan.GetTargetBootId(), 128) || !validResponseOpaque(plan.GetTargetEndpoint(), 512) ||
		(plan.GetSourceHolderId() == plan.GetTargetHolderId() && plan.GetSourceBootId() == plan.GetTargetBootId()) ||
		plan.GetRevision() == 0 || !validRequestedTTL(plan.GetRequestedTtlMs()) || !validLowerSHA256(plan.GetChecksumSha256()) {
		return false
	}
	fence := plan.GetExpectedSourceFence()
	if fence == nil || fence.GetHolderId() != plan.GetSourceHolderId() || fence.GetHolderBootId() != plan.GetSourceBootId() ||
		fence.GetAssignmentRevision() == 0 || fence.GetWriterEpoch() == 0 || fence.GetLeaseEpoch() == 0 {
		return false
	}
	switch plan.GetOperation() {
	case routeplanpb.OperationKind_OPERATION_KIND_PLANNED_DRAIN:
		if plan.GetTakeoverHolderUnavailable() || plan.GetTakeoverLeaseExpired() {
			return false
		}
	case routeplanpb.OperationKind_OPERATION_KIND_CRASH_TAKEOVER:
		if !plan.GetTakeoverHolderUnavailable() && !plan.GetTakeoverLeaseExpired() {
			return false
		}
	default:
		return false
	}
	terminal := false
	switch plan.GetPhase() {
	case routeplanpb.PlanPhase_PLAN_PHASE_PENDING, routeplanpb.PlanPhase_PLAN_PHASE_DRAINING,
		routeplanpb.PlanPhase_PLAN_PHASE_ASSIGNING, routeplanpb.PlanPhase_PLAN_PHASE_PREPARING,
		routeplanpb.PlanPhase_PLAN_PHASE_ACTIVE:
	case routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED, routeplanpb.PlanPhase_PLAN_PHASE_FAILED, routeplanpb.PlanPhase_PLAN_PHASE_PAUSED:
		terminal = true
	default:
		return false
	}
	created, updated, started, completed, expires := plan.GetCreatedAtMs(), plan.GetUpdatedAtMs(), plan.GetStartedAtMs(), plan.GetCompletedAtMs(), plan.GetExpiresAtMs()
	if created <= 0 || updated < created || expires <= created ||
		(started != 0 && (started < created || started > updated)) ||
		(completed != 0 && (completed < created || completed < updated)) {
		return false
	}
	if terminal {
		if completed != updated || (plan.GetPhase() == routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED && plan.GetErrorCode() != "") ||
			((plan.GetPhase() == routeplanpb.PlanPhase_PLAN_PHASE_FAILED || plan.GetPhase() == routeplanpb.PlanPhase_PLAN_PHASE_PAUSED) && !validResponseOpaque(plan.GetErrorCode(), 128)) {
			return false
		}
	} else if completed != 0 || plan.GetErrorCode() != "" || expires <= updated {
		return false
	}
	return true
}

func validResponseOpaque(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) || unicode.IsSpace(current) {
			return false
		}
	}
	return true
}

func validLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for index := 0; index < len(value); index++ {
		if (value[index] < '0' || value[index] > '9') && (value[index] < 'a' || value[index] > 'f') {
			return false
		}
	}
	return true
}

func validShardResponse(domain string, shard uint32, response *routeplanpb.GetShardResponse) bool {
	if response == nil || response.GetDomain() != domain || response.GetShard() != shard {
		return false
	}
	if authority := response.GetAuthority(); authority != nil &&
		(authority.GetDomain() != domain || authority.GetShard() != shard) {
		return false
	}
	for _, plan := range response.GetRelatedPlans() {
		if plan == nil || plan.GetDomain() != domain || plan.GetShard() != shard {
			return false
		}
	}
	return true
}

func plansRedacted(plans []*routeplanpb.Plan) []redactedPlan {
	result := make([]redactedPlan, 0, len(plans))
	for _, plan := range plans {
		if plan != nil {
			result = append(result, redactPlan(plan))
		}
	}
	return result
}

func redactPlan(plan *routeplanpb.Plan) redactedPlan {
	if plan == nil {
		return redactedPlan{}
	}
	return redactedPlan{PlanID: plan.GetPlanId(), TransitionID: plan.GetTransitionId(), Domain: plan.GetDomain(), Shard: plan.GetShard(), SourceHolderID: plan.GetSourceHolderId(), SourceBootID: plan.GetSourceBootId(), SourceEndpoint: plan.GetSourceEndpoint(), TargetHolderID: plan.GetTargetHolderId(), TargetBootID: plan.GetTargetBootId(), TargetEndpoint: plan.GetTargetEndpoint(), ExpectedSourceFence: redactFence(plan.GetExpectedSourceFence()), Operation: operationName(plan.GetOperation()), Phase: phaseName(plan.GetPhase()), Attempt: plan.GetAttempt(), Revision: plan.GetRevision(), CreatedAtMs: plan.GetCreatedAtMs(), UpdatedAtMs: plan.GetUpdatedAtMs(), StartedAtMs: plan.GetStartedAtMs(), CompletedAtMs: plan.GetCompletedAtMs(), ExpiresAtMs: plan.GetExpiresAtMs(), ErrorCode: plan.GetErrorCode(), ChecksumSHA256: plan.GetChecksumSha256(), RequestedTTLms: plan.GetRequestedTtlMs(), ProofHolderUnavailable: plan.GetTakeoverHolderUnavailable(), ProofLeaseExpired: plan.GetTakeoverLeaseExpired(), SourceGeneration: redactGeneration(plan.GetSourceGeneration()), TargetGeneration: redactGeneration(plan.GetTargetGeneration()), OwnerScalePlanID: plan.GetOwnerScalePlanId(), OwnerRetainUntilMs: plan.GetOwnerRetainUntilMs()}
}

func redactGeneration(value *routeplanpb.HolderGeneration) redactedGeneration {
	if value == nil {
		return redactedGeneration{}
	}
	return redactedGeneration{HolderID: value.GetHolderId(), BootID: value.GetBootId(), PodUID: value.GetPodUid(),
		WorkloadUID: value.GetWorkloadUid(), Ordinal: value.GetOrdinal(), DeploymentIdentityPresent: value.GetDeploymentIdentityPresent()}
}

func redactFence(fence *routeplanpb.AuthorityFence) redactedFence {
	if fence == nil {
		return redactedFence{}
	}
	return redactedFence{HolderID: fence.GetHolderId(), HolderBootID: fence.GetHolderBootId(), AssignmentRevision: fence.GetAssignmentRevision(), WriterEpoch: fence.GetWriterEpoch(), LeaseEpoch: fence.GetLeaseEpoch()}
}

func redactAuthority(value *routeplanpb.AuthorityView) redactedAuthority {
	return redactedAuthority{Domain: value.GetDomain(), Shard: value.GetShard(), State: value.GetState(), Fence: redactFence(value.GetFence()), LeaseUntilMs: value.GetLeaseUntilMs(), LastTransitionID: value.GetLastTransitionId(), LastTransitionAtMs: value.GetLastTransitionAtMs()}
}

func redactHolder(value *routeplanpb.HolderView) redactedHolder {
	return redactedHolder{
		Domain: value.GetDomain(), HolderID: value.GetHolderId(), BootID: value.GetBootId(),
		LogicEndpoint: value.GetLogicEndpoint(), HolderControlEndpoint: value.GetHolderControlEndpoint(),
		TLSDNSIdentity: value.GetTlsDnsIdentity(),
		State:          value.GetState(), ProcessReady: value.GetProcessReady(), Draining: value.GetDraining(),
		ServingShardCount: value.GetServingShardCount(), SnapshotRevision: value.GetSnapshotRevision(),
		HeartbeatAtMs: value.GetHeartbeatAtMs(), LeaseUntilMs: value.GetLeaseUntilMs(),
		Sequence: value.GetSequence(), CapacityWeight: value.GetCapacityWeight(),
		PodUID: value.GetPodUid(), WorkloadUID: value.GetWorkloadUid(), Ordinal: value.GetOrdinal(),
		DeploymentIdentityPresent: value.GetDeploymentIdentityPresent(),
	}
}

func operationName(value routeplanpb.OperationKind) string {
	switch value {
	case routeplanpb.OperationKind_OPERATION_KIND_PLANNED_DRAIN:
		return "planned-drain"
	case routeplanpb.OperationKind_OPERATION_KIND_CRASH_TAKEOVER:
		return "crash-takeover"
	default:
		return "unspecified"
	}
}

func phaseName(value routeplanpb.PlanPhase) string {
	return map[routeplanpb.PlanPhase]string{routeplanpb.PlanPhase_PLAN_PHASE_PENDING: "PENDING", routeplanpb.PlanPhase_PLAN_PHASE_DRAINING: "DRAINING", routeplanpb.PlanPhase_PLAN_PHASE_ASSIGNING: "ASSIGNING", routeplanpb.PlanPhase_PLAN_PHASE_PREPARING: "PREPARING", routeplanpb.PlanPhase_PLAN_PHASE_ACTIVE: "ACTIVE", routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED: "SUCCEEDED", routeplanpb.PlanPhase_PLAN_PHASE_FAILED: "FAILED", routeplanpb.PlanPhase_PLAN_PHASE_PAUSED: "PAUSED"}[value]
}

func validDomain(value string) bool {
	return value == "PLAYER" || value == "FARM" || value == "FARM_WORKER"
}

func validObservedDomain(value string) bool {
	return validDomain(value)
}

func validCanonicalDNS(value string) bool {
	if value == "" || value != strings.ToLower(value) || net.ParseIP(value) != nil || strings.ContainsAny(value, ":/\\*_ ") {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := 0; index < len(label); index++ {
			value := label[index]
			if value != '-' && (value < 'a' || value > 'z') && (value < '0' || value > '9') {
				return false
			}
		}
	}
	return len(value) <= 253
}

func validCanonicalTarget(value string) bool {
	if value == "" || len(value) > maximumTargetBytes || strings.TrimSpace(value) != value {
		return false
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" {
		return false
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 || strconv.FormatUint(parsedPort, 10) != port {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		if strings.Contains(host, "%") || ip.String() != host {
			return false
		}
	} else if !validCanonicalDNS(host) {
		return false
	}
	return net.JoinHostPort(host, port) == value
}

func validRequestedTTL(value int64) bool { return value >= 1 && value <= maximumTTLms }

var subjectAltNameOID = asn1.ObjectIdentifier{2, 5, 29, 17}

func verifyRouteControllerConnection(state tls.ConnectionState, expected string) error {
	// HandshakeComplete is false while crypto/tls is still inside VerifyConnection
	// (observed on Go 1.26). Fail closed on verified TLS 1.3 chain + exact leaf
	// identity instead of the callback-phase completion flag.
	if state.Version < tls.VersionTLS13 || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return errUsage
	}
	leaf := state.VerifiedChains[0][0]
	if leaf == nil || leaf.Subject.CommonName != "" || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != expected ||
		len(leaf.IPAddresses) != 0 || len(leaf.EmailAddresses) != 0 || len(leaf.URIs) != 0 {
		return errUsage
	}
	if len(state.PeerCertificates) > 0 && state.PeerCertificates[0] != leaf {
		return errUsage
	}
	rawDNS, err := exactRawDNSSubjectAltName(leaf.Extensions)
	if err != nil || rawDNS != expected || !validCanonicalDNS(rawDNS) {
		return errUsage
	}
	return nil
}

func exactRawDNSSubjectAltName(extensions []pkix.Extension) (string, error) {
	var encoded []byte
	for _, extension := range extensions {
		if !extension.Id.Equal(subjectAltNameOID) {
			continue
		}
		if encoded != nil {
			return "", errUsage
		}
		encoded = extension.Value
	}
	if encoded == nil {
		return "", errUsage
	}
	var sequence asn1.RawValue
	rest, err := asn1.Unmarshal(encoded, &sequence)
	if err != nil || len(rest) != 0 || sequence.Class != asn1.ClassUniversal || sequence.Tag != asn1.TagSequence || !sequence.IsCompound {
		return "", errUsage
	}
	var name asn1.RawValue
	rest, err = asn1.Unmarshal(sequence.Bytes, &name)
	if err != nil || len(rest) != 0 || name.Class != asn1.ClassContextSpecific || name.Tag != 2 || name.IsCompound || len(name.Bytes) == 0 {
		return "", errUsage
	}
	for _, value := range name.Bytes {
		if value > 0x7f {
			return "", errUsage
		}
	}
	return string(name.Bytes), nil
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func intEnv(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
