// Command resp-trace-proxy is a test-only transparent Redis RESP proxy used to
// measure complete business-command windows. It persists only allowlisted
// command/category aggregates and byte counts; Redis arguments, keys,
// credentials, identities, payloads, and raw replies are never written.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxRESPFrameBytes = 32 << 20
	maxRESPDepth      = 32
)

var expectedBusinessCommands = []string{"BUY", "SELL", "CLAIM_TASK", "PLANT", "HARVEST", "WATER", "THEFT"}

type config struct {
	listen, control, upstream, output                          string
	trainRun, businessCommit, evidenceHead, h5Commit, toolHead string
}

type requestClass struct {
	ActorCommit   int
	WorkflowWrite int
	Orders        int
	DueMutations  int
	Receipts      int
	Category      string
}

type traceRow struct {
	Command             string         `json:"command"`
	WindowID            uint64         `json:"window_id"`
	StartedAt           string         `json:"started_at"`
	FinishedAt          string         `json:"finished_at"`
	Status              string         `json:"status"`
	ActorCommit         int64          `json:"actor_commit"`
	WorkflowWrites      int64          `json:"workflow_writes"`
	Orders              int64          `json:"orders"`
	Due                 int64          `json:"due"`
	Receipts            int64          `json:"receipts"`
	RedisOps            int64          `json:"redis_ops"`
	RequestBytes        int64          `json:"request_bytes"`
	ResponseBytes       int64          `json:"response_bytes"`
	TotalBytes          int64          `json:"total_bytes"`
	RedisErrors         int64          `json:"redis_errors"`
	NoScriptRecovered   int64          `json:"noscript_recovered"`
	UnansweredRequests  int64          `json:"unanswered_requests"`
	RedisCommands       map[string]int `json:"redis_commands"`
	RedisErrorCodes     map[string]int `json:"redis_error_codes"`
	PhysicalCategories  map[string]int `json:"physical_categories"`
	OperationBindingSHA string         `json:"operation_binding_sha256"`
}

type capturedEvent struct {
	request, response []byte
	connectionID      uint64
	command           string
	class             requestClass
}

type traceWindow struct {
	mu          sync.Mutex
	row         traceRow
	operationID []byte
	events      []capturedEvent
	overflow    bool
}

type pendingRequest struct {
	window       *traceWindow
	request      []byte
	connectionID uint64
	command      string
	class        requestClass
}

type proxy struct {
	cfg config

	mu      sync.Mutex
	active  *traceWindow
	windows []*traceWindow
	nextID  uint64
	frozen  bool

	pending atomic.Int64
	connID  atomic.Uint64
}

type finalReport struct {
	SchemaVersion   string          `json:"schema_version"`
	TrainRunID      string          `json:"train_run_id"`
	BusinessCommit  string          `json:"backend_business_commit"`
	EvidenceHead    string          `json:"backend_evidence_head"`
	H5Commit        string          `json:"h5_commit"`
	ToolHead        string          `json:"tool_head"`
	Measurement     string          `json:"measurement_source"`
	IdentityPolicy  string          `json:"identity_policy"`
	Rows            []traceRow      `json:"rows"`
	Checks          map[string]bool `json:"checks"`
	AggregateSHA256 string          `json:"aggregate_sha256"`
	Verdict         string          `json:"verdict"`
	RequiredGap     string          `json:"required_gap"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:0", "loopback RESP listen address")
	flag.StringVar(&cfg.control, "control-listen", "127.0.0.1:0", "loopback HTTP control address")
	flag.StringVar(&cfg.upstream, "upstream", "", "upstream Redis host:port")
	flag.StringVar(&cfg.output, "output", "", "redacted aggregate JSON output")
	flag.StringVar(&cfg.trainRun, "train-run", "", "bound train run ID")
	flag.StringVar(&cfg.businessCommit, "business-commit", "", "bound Backend Business Commit")
	flag.StringVar(&cfg.evidenceHead, "evidence-head", "", "bound Backend Evidence Head")
	flag.StringVar(&cfg.h5Commit, "h5-commit", "", "bound H5 Commit")
	flag.StringVar(&cfg.toolHead, "tool-head", "", "trace tool source Head")
	flag.Parse()
	if err := validateConfig(cfg); err != nil {
		log.Fatal(err)
	}
	p := &proxy{cfg: cfg}
	respListener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}
	defer respListener.Close()
	controlListener, err := net.Listen("tcp", cfg.control)
	if err != nil {
		log.Fatal(err)
	}
	defer controlListener.Close()

	server := &http.Server{Handler: p.controlHandler(), ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if serveErr := server.Serve(controlListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("control server: %v", serveErr)
		}
	}()
	go p.accept(respListener)
	fmt.Printf("RESP_TRACE_READY redis=%s control=%s\n", respListener.Addr(), controlListener.Addr())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func validateConfig(cfg config) error {
	for name, value := range map[string]string{
		"upstream": cfg.upstream, "output": cfg.output, "train-run": cfg.trainRun,
		"business-commit": cfg.businessCommit, "evidence-head": cfg.evidenceHead,
		"h5-commit": cfg.h5Commit, "tool-head": cfg.toolHead,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{"business-commit": cfg.businessCommit, "evidence-head": cfg.evidenceHead, "h5-commit": cfg.h5Commit, "tool-head": cfg.toolHead} {
		if len(value) != 40 || strings.Trim(value, "0123456789abcdef") != "" {
			return fmt.Errorf("%s must be a lowercase 40-character SHA", name)
		}
	}
	for _, address := range []string{cfg.listen, cfg.control} {
		host, _, err := net.SplitHostPort(address)
		if err != nil || (host != "127.0.0.1" && host != "localhost") {
			return fmt.Errorf("listeners must be loopback: %s", address)
		}
	}
	return nil
}

func (p *proxy) accept(listener net.Listener) {
	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		go p.serveConnection(client, p.connID.Add(1))
	}
}

func (p *proxy) serveConnection(client net.Conn, connectionID uint64) {
	defer client.Close()
	upstream, err := net.DialTimeout("tcp", p.cfg.upstream, 5*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	queue := make(chan pendingRequest, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		reader := bufio.NewReader(upstream)
		for request := range queue {
			response, readErr := readRESPFrame(reader)
			if readErr != nil {
				p.recordUnanswered(request)
				return
			}
			if _, writeErr := client.Write(response); writeErr != nil {
				p.recordResponse(request, response)
				return
			}
			p.recordResponse(request, response)
		}
	}()

	reader := bufio.NewReader(client)
	for {
		requestFrame, readErr := readRESPFrame(reader)
		if readErr != nil {
			close(queue)
			<-done
			return
		}
		command, class := classifyRequest(requestFrame)
		request := pendingRequest{window: p.currentWindow(), request: requestFrame, connectionID: connectionID, command: command, class: class}
		p.pending.Add(1)
		queue <- request
		if _, writeErr := upstream.Write(requestFrame); writeErr != nil {
			_ = upstream.Close()
			close(queue)
			<-done
			return
		}
	}
}

func (p *proxy) currentWindow() *traceWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}

func (p *proxy) recordResponse(request pendingRequest, response []byte) {
	defer p.pending.Add(-1)
	if request.window == nil {
		return
	}
	request.window.mu.Lock()
	defer request.window.mu.Unlock()
	if request.window.overflow {
		return
	}
	capturedBytes := len(request.request) + len(response)
	for _, event := range request.window.events {
		capturedBytes += len(event.request) + len(event.response)
	}
	if capturedBytes > maxRESPFrameBytes {
		request.window.overflow = true
		request.window.events = nil
		return
	}
	request.window.events = append(request.window.events, capturedEvent{
		request: append([]byte(nil), request.request...), response: append([]byte(nil), response...),
		connectionID: request.connectionID, command: request.command, class: request.class,
	})
}

func (p *proxy) recordUnanswered(request pendingRequest) {
	defer p.pending.Add(-1)
	if request.window == nil {
		return
	}
	request.window.mu.Lock()
	defer request.window.mu.Unlock()
	request.window.row.UnansweredRequests++
}

func allowCommand(command string) string {
	switch command {
	case "EVAL", "EVALSHA", "GET", "SET", "HGET", "HMGET", "HSET", "HLEN", "HEXISTS", "DEL", "EXISTS", "ZADD", "ZREM", "ZCARD", "ZRANGE", "ZRANGEBYSCORE", "ZCOUNT", "TIME", "PTTL", "PEXPIRE", "PING", "MULTI", "EXEC", "WATCH", "UNWATCH":
		return command
	default:
		return "OTHER"
	}
}

func (p *proxy) controlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/begin", p.begin)
	mux.HandleFunc("/bind", p.bind)
	mux.HandleFunc("/end", p.end)
	mux.HandleFunc("/freeze", p.freeze)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

type markerRequest struct {
	Command     string `json:"command"`
	Status      string `json:"status"`
	OperationID string `json:"operation_id"`
}

func decodeMarker(w http.ResponseWriter, r *http.Request) (markerRequest, bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return markerRequest{}, false
	}
	defer r.Body.Close()
	var request markerRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid marker", http.StatusBadRequest)
		return markerRequest{}, false
	}
	request.Command = strings.ToUpper(strings.TrimSpace(request.Command))
	if !contains(expectedBusinessCommands, request.Command) {
		http.Error(w, "unsupported command", http.StatusBadRequest)
		return markerRequest{}, false
	}
	return request, true
}

func (p *proxy) begin(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeMarker(w, r)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frozen || p.active != nil {
		http.Error(w, "trace is frozen or active", http.StatusConflict)
		return
	}
	p.nextID++
	window := &traceWindow{row: traceRow{
		Command: request.Command, WindowID: p.nextID, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Status: "ACTIVE",
		RedisCommands: make(map[string]int), PhysicalCategories: make(map[string]int),
	}}
	p.active = window
	p.windows = append(p.windows, window)
	writeJSON(w, map[string]any{"status": "ACTIVE", "window_id": window.row.WindowID})
}

func (p *proxy) bind(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeMarker(w, r)
	if !ok {
		return
	}
	if len(request.OperationID) < 8 || len(request.OperationID) > 256 || strings.ContainsAny(request.OperationID, "\r\n\x00") {
		http.Error(w, "invalid operation identity", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active == nil || p.active.row.Command != request.Command {
		http.Error(w, "active command mismatch", http.StatusConflict)
		return
	}
	p.active.mu.Lock()
	defer p.active.mu.Unlock()
	if len(p.active.operationID) != 0 && !bytes.Equal(p.active.operationID, []byte(request.OperationID)) {
		http.Error(w, "operation identity already bound", http.StatusConflict)
		return
	}
	p.active.operationID = append([]byte(nil), request.OperationID...)
	writeJSON(w, map[string]string{"status": "BOUND"})
}

func (p *proxy) end(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeMarker(w, r)
	if !ok {
		return
	}
	status := strings.ToUpper(strings.TrimSpace(request.Status))
	if status != "PASS" && status != "FAIL" {
		http.Error(w, "status must be PASS or FAIL", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active == nil || p.active.row.Command != request.Command {
		http.Error(w, "active command mismatch", http.StatusConflict)
		return
	}
	p.active.mu.Lock()
	if len(p.active.operationID) == 0 {
		p.active.mu.Unlock()
		http.Error(w, "operation identity not bound", http.StatusConflict)
		return
	}
	p.active.row.Status = status
	p.active.row.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	p.active.mu.Unlock()
	p.active = nil
	writeJSON(w, map[string]string{"status": status})
}

func (p *proxy) freeze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	p.mu.Lock()
	if p.active != nil || p.frozen {
		p.mu.Unlock()
		http.Error(w, "trace active or already frozen", http.StatusConflict)
		return
	}
	p.frozen = true
	p.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for p.pending.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	report := p.buildReport()
	if err := writeReport(p.cfg.output, report); err != nil {
		http.Error(w, "write report", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"verdict": report.Verdict, "aggregate_sha256": report.AggregateSHA256})
}

func (p *proxy) buildReport() finalReport {
	p.mu.Lock()
	windows := append([]*traceWindow(nil), p.windows...)
	p.mu.Unlock()
	rows := make([]traceRow, 0, len(windows))
	seen := make(map[string]int)
	allPass, bytesPositive, commitsPositive, noTransportErrors, operationBound := true, true, true, true, true
	for _, window := range windows {
		row := aggregateWindow(window)
		rows = append(rows, row)
		seen[row.Command]++
		allPass = allPass && row.Status == "PASS"
		bytesPositive = bytesPositive && row.RedisOps > 0 && row.TotalBytes == row.RequestBytes+row.ResponseBytes && row.TotalBytes > 0
		commitsPositive = commitsPositive && row.ActorCommit > 0 && row.Receipts == row.ActorCommit
		noTransportErrors = noTransportErrors && row.RedisErrors == 0 && row.UnansweredRequests == 0
		operationBound = operationBound && len(row.OperationBindingSHA) == 64
	}
	exactCommands := len(rows) == len(expectedBusinessCommands)
	for _, command := range expectedBusinessCommands {
		exactCommands = exactCommands && seen[command] == 1
	}
	checks := map[string]bool{
		"exact_command_set":                      exactCommands,
		"all_browser_windows_pass":               allPass,
		"all_rows_have_redis_transport":          bytesPositive,
		"all_rows_have_actor_commit_and_receipt": commitsPositive,
		"no_redis_error_or_unanswered_request":   noTransportErrors && p.pending.Load() == 0,
		"every_window_bound_to_operation":        operationBound,
		"causal_filter_excludes_background":      operationBound,
		"raw_redis_arguments_persisted":          false,
		"raw_identity_persisted":                 false,
	}
	verdict := "PASS"
	for name, value := range checks {
		if strings.HasPrefix(name, "raw_") {
			if value {
				verdict = "FAIL"
			}
		} else if !value {
			verdict = "FAIL"
		}
	}
	aggregateInput, _ := json.Marshal(rows)
	digest := sha256.Sum256(aggregateInput)
	return finalReport{
		SchemaVersion: "cap-redis-business-amplification-trace-v1", TrainRunID: p.cfg.trainRun,
		BusinessCommit: p.cfg.businessCommit, EvidenceHead: p.cfg.evidenceHead, H5Commit: p.cfg.h5Commit, ToolHead: p.cfg.toolHead,
		Measurement:    "TRANSPARENT_RESP_PROXY_OPERATION_BOUND_REAL_H5_END_TO_END_TRACE",
		IdentityPolicy: "DOMAIN_SEPARATED_OPERATION_HASH_AND_ALLOWLISTED_AGGREGATES_ONLY_NO_RAW_RESP_KEYS_ARGUMENTS_PAYLOADS_OR_IDENTITIES",
		Rows:           rows, Checks: checks, AggregateSHA256: hex.EncodeToString(digest[:]), Verdict: verdict,
		RequiredGap: map[bool]string{true: "CLOSED", false: "ACTUAL_END_TO_END_BUSINESS_TRACE"}[verdict == "PASS"],
	}
}

func aggregateWindow(window *traceWindow) traceRow {
	window.mu.Lock()
	row := window.row
	operationID := append([]byte(nil), window.operationID...)
	events := append([]capturedEvent(nil), window.events...)
	overflow := window.overflow
	window.mu.Unlock()
	row.RedisCommands = make(map[string]int)
	row.RedisErrorCodes = make(map[string]int)
	row.PhysicalCategories = make(map[string]int)
	if overflow || len(operationID) == 0 {
		row.UnansweredRequests++
		return row
	}
	binding := sha256.Sum256(append([]byte("cap-redis-business-operation-binding-v1\x00"), operationID...))
	row.OperationBindingSHA = hex.EncodeToString(binding[:])
	selected := make([]bool, len(events))
	aliases := make(map[string][]byte)
	for index, event := range events {
		if bytes.Contains(event.request, operationID) {
			selected[index] = true
			collectCausalAliases(event.request, event.class, aliases)
		}
	}
	for changed := true; changed; {
		changed = false
		for index, event := range events {
			if selected[index] {
				continue
			}
			matched := false
			for _, alias := range aliases {
				if bytes.Contains(event.request, alias) {
					matched = true
					break
				}
			}
			if matched {
				selected[index] = true
				collectCausalAliases(event.request, event.class, aliases)
				changed = true
			}
		}
	}
	noscriptPending := make(map[string]int64)
	for index, event := range events {
		if !selected[index] {
			continue
		}
		row.RedisOps++
		row.RequestBytes += int64(len(event.request))
		row.ResponseBytes += int64(len(event.response))
		row.TotalBytes += int64(len(event.request) + len(event.response))
		row.ActorCommit += int64(event.class.ActorCommit)
		row.WorkflowWrites += int64(event.class.WorkflowWrite)
		row.Orders += int64(event.class.Orders)
		row.Due += int64(event.class.DueMutations)
		row.Receipts += int64(event.class.Receipts)
		if len(event.response) != 0 && event.response[0] == '-' {
			code := classifyRedisError(event.response)
			if code == "NOSCRIPT" {
				noscriptPending[scriptDigest(event.request)]++
			} else {
				row.RedisErrors++
				row.RedisErrorCodes[code]++
			}
		} else if event.command == "EVAL" && noscriptPending[scriptDigest(event.request)] > 0 {
			noscriptPending[scriptDigest(event.request)]--
			row.NoScriptRecovered++
			row.RedisErrorCodes["NOSCRIPT_RECOVERED"]++
		}
		row.RedisCommands[allowCommand(event.command)]++
		row.PhysicalCategories[event.class.Category]++
	}
	for _, pending := range noscriptPending {
		if pending > 0 {
			row.RedisErrors += pending
			row.RedisErrorCodes["NOSCRIPT_UNRESOLVED"] += int(pending)
		}
	}
	return row
}

func scriptDigest(frame []byte) string {
	values, err := decodeCommand(frame)
	if err != nil || len(values) < 2 {
		return "INVALID"
	}
	switch strings.ToUpper(string(values[0])) {
	case "EVALSHA":
		return strings.ToLower(string(values[1]))
	case "EVAL":
		digest := sha1.Sum(values[1])
		return hex.EncodeToString(digest[:])
	default:
		return "INVALID"
	}
}

func collectCausalAliases(frame []byte, class requestClass, aliases map[string][]byte) {
	if class.WorkflowWrite > 0 {
		for _, token := range stableOpaqueTokens(frame) {
			digest := sha256.Sum256(token)
			aliases[hex.EncodeToString(digest[:])] = token
		}
	}
	values, err := decodeCommand(frame)
	if err != nil || len(values) == 0 {
		return
	}
	for _, key := range commandKeys(strings.ToUpper(string(values[0])), values) {
		if bytes.Contains(key, []byte("}:order:")) || bytes.Contains(key, []byte(":workflow:work:")) ||
			bytes.Contains(key, []byte("}:work:")) {
			digest := sha256.Sum256(key)
			aliases[hex.EncodeToString(digest[:])] = append([]byte(nil), key...)
		}
	}
}

func stableOpaqueTokens(frame []byte) [][]byte {
	var tokens [][]byte
	for offset := 0; offset+5 < len(frame); offset++ {
		if !bytes.Equal(frame[offset:offset+5], []byte("sid1_")) {
			continue
		}
		end := offset + 5
		for end < len(frame) && ((frame[end] >= 'a' && frame[end] <= 'z') || (frame[end] >= 'A' && frame[end] <= 'Z') ||
			(frame[end] >= '0' && frame[end] <= '9') || frame[end] == '_' || frame[end] == '-') {
			end++
		}
		if end-offset >= 20 && end-offset <= 256 {
			tokens = append(tokens, append([]byte(nil), frame[offset:end]...))
			offset = end - 1
		}
	}
	return tokens
}

func classifyRedisError(response []byte) string {
	upper := bytes.ToUpper(response)
	switch {
	case bytes.Contains(upper, []byte("NOSCRIPT")):
		return "NOSCRIPT"
	case bytes.Contains(upper, []byte("WRONGTYPE")):
		return "WRONGTYPE"
	case bytes.Contains(upper, []byte("CROSSSLOT")):
		return "CROSSSLOT"
	case bytes.Contains(upper, []byte("UNKNOWN COMMAND")):
		return "UNKNOWN_COMMAND"
	case bytes.Contains(upper, []byte("NOAUTH")):
		return "NOAUTH"
	case bytes.Contains(upper, []byte("NOPERM")):
		return "NOPERM"
	default:
		return "OTHER_REDACTED"
	}
}

func writeReport(path string, report finalReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func classifyRequest(frame []byte) (string, requestClass) {
	values, err := decodeCommand(frame)
	if err != nil || len(values) == 0 {
		return "OTHER", requestClass{Category: "other"}
	}
	command := strings.ToUpper(string(values[0]))
	class := requestClass{Category: "other"}
	keys := commandKeys(command, values)
	if isActorCommit(keys) {
		class.ActorCommit = 1
		class.Receipts = 1
		class.Category = "actor_commit"
		for _, key := range keys {
			if bytes.Contains(key, []byte("}:order:")) {
				class.Orders++
			}
		}
		class.DueMutations += class.Orders
		return command, class
	}
	if isWorkflowRequest(command, values, keys) {
		class.WorkflowWrite = 1
		class.Category = "workflow_write"
		for _, key := range keys {
			if bytes.HasSuffix(key, []byte(":due")) || bytes.Contains(key, []byte(":due:")) {
				class.DueMutations = 1
				break
			}
		}
		return command, class
	}
	for _, key := range keys {
		switch {
		case bytes.Contains(key, []byte(":friend")) || bytes.Contains(key, []byte(":pair:")):
			class.Category = "friend_authority"
		case bytes.Contains(key, []byte(":auth:")):
			class.Category = "actor_authority_other"
		case bytes.Contains(key, []byte(":session")) || bytes.Contains(key, []byte(":gate")):
			class.Category = "runtime_authority"
		}
	}
	return command, class
}

func commandKeys(command string, values [][]byte) [][]byte {
	if (command == "EVAL" || command == "EVALSHA") && len(values) >= 3 {
		n, err := strconv.Atoi(string(values[2]))
		if err == nil && n >= 0 && 3+n <= len(values) {
			return values[3 : 3+n]
		}
	}
	if len(values) > 1 {
		return values[1:]
	}
	return nil
}

func isActorCommit(keys [][]byte) bool {
	return len(keys) >= 5 && bytes.HasSuffix(keys[0], []byte(":shard-authority")) &&
		bytes.Contains(keys[1], []byte("}:actor:")) && bytes.HasSuffix(keys[2], []byte(":due")) &&
		bytes.HasSuffix(keys[3], []byte(":receipt-due")) && bytes.HasSuffix(keys[4], []byte(":bucket-meta"))
}

func isWorkflowRequest(command string, values, keys [][]byte) bool {
	if command != "EVAL" && command != "EVALSHA" {
		return false
	}
	workflowKey := false
	for _, key := range keys {
		if bytes.Contains(key, []byte(":workflow:")) || bytes.Contains(key, []byte(":wf:")) || bytes.Contains(key, []byte(":work:")) {
			workflowKey = true
		}
	}
	if !workflowKey {
		return false
	}
	if command == "EVAL" && len(values) > 1 {
		script := bytes.ToUpper(values[1])
		return bytes.Contains(script, []byte("HSET")) || bytes.Contains(script, []byte("ZADD")) ||
			bytes.Contains(script, []byte("ZREM")) || bytes.Contains(script, []byte("DEL"))
	}
	return true
}

func readRESPFrame(reader *bufio.Reader) ([]byte, error) {
	var output bytes.Buffer
	if err := readRESPInto(reader, &output, 0); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func readRESPInto(reader *bufio.Reader, output *bytes.Buffer, depth int) error {
	if depth > maxRESPDepth || output.Len() > maxRESPFrameBytes {
		return errors.New("RESP frame limit")
	}
	prefix, err := reader.ReadByte()
	if err != nil {
		return err
	}
	output.WriteByte(prefix)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	output.Write(line)
	trimmed := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
	switch prefix {
	case '$', '!', '=':
		length, parseErr := strconv.Atoi(trimmed)
		if parseErr != nil {
			return parseErr
		}
		if length < 0 {
			return nil
		}
		if length > maxRESPFrameBytes-output.Len()-2 {
			return errors.New("RESP bulk limit")
		}
		_, err = io.CopyN(output, reader, int64(length+2))
		return err
	case '*', '~', '>':
		count, parseErr := strconv.Atoi(trimmed)
		if parseErr != nil || count < 0 {
			return parseErr
		}
		for i := 0; i < count; i++ {
			if err := readRESPInto(reader, output, depth+1); err != nil {
				return err
			}
		}
	case '%', '|':
		count, parseErr := strconv.Atoi(trimmed)
		if parseErr != nil || count < 0 {
			return parseErr
		}
		for i := 0; i < count*2; i++ {
			if err := readRESPInto(reader, output, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeCommand(frame []byte) ([][]byte, error) {
	if len(frame) == 0 || frame[0] != '*' {
		return nil, errors.New("not RESP array")
	}
	index := bytes.Index(frame, []byte("\r\n"))
	if index < 0 {
		return nil, errors.New("array line")
	}
	count, err := strconv.Atoi(string(frame[1:index]))
	if err != nil || count < 0 {
		return nil, errors.New("array count")
	}
	position := index + 2
	values := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		if position >= len(frame) || frame[position] != '$' {
			return nil, errors.New("non-bulk command argument")
		}
		lineEnd := bytes.Index(frame[position:], []byte("\r\n"))
		if lineEnd < 0 {
			return nil, errors.New("bulk line")
		}
		lineEnd += position
		length, parseErr := strconv.Atoi(string(frame[position+1 : lineEnd]))
		if parseErr != nil || length < 0 {
			return nil, errors.New("bulk length")
		}
		start := lineEnd + 2
		end := start + length
		if end+2 > len(frame) || frame[end] != '\r' || frame[end+1] != '\n' {
			return nil, errors.New("bulk boundary")
		}
		values = append(values, frame[start:end])
		position = end + 2
	}
	return values, nil
}

func stableMapKeys(values map[string]int) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
