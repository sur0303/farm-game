package routecontrol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RESPProxy is the sole Redis boundary used by trusted runs. It records exact
// mutating keys in private memory before forwarding and exposes only
// challenge-bound digests to evidence collectors.
type RESPProxy struct {
	mu        sync.Mutex
	challenge string
	backend   redis.Options
	listeners map[string]net.Listener
	addresses map[string]string
	conns     map[net.Conn]struct{}
	ledger    map[string]*RedisKeyLedgerEntry
	events    []RedisAuditEvent
	nextConn  uint64
	nextEvent uint64
	fault     *RedisReplyLossFault
	faultWait chan struct{}
	closed    bool
}

type RedisKeyLedgerEntry struct {
	KeyDigest    string   `json:"key_digest"`
	Commands     []string `json:"commands"`
	FirstEvent   uint64   `json:"first_event"`
	MutationSeen bool     `json:"mutation_seen"`
	rawKey       string
}

type RedisAuditEvent struct {
	Sequence       uint64   `json:"sequence"`
	Listener       string   `json:"listener"`
	Connection     uint64   `json:"connection"`
	Command        string   `json:"command"`
	Class          string   `json:"class"`
	ScriptSHA      string   `json:"script_sha,omitempty"`
	KeyDigests     []string `json:"key_digests,omitempty"`
	RequestSHA256  string   `json:"request_sha256"`
	ResponseSHA256 string   `json:"response_sha256,omitempty"`
	Forwarded      bool     `json:"forwarded"`
	ReplyDropped   bool     `json:"reply_dropped,omitempty"`
	Rejected       bool     `json:"rejected,omitempty"`
	WallTime       string   `json:"wall_time"`
}

type RedisReplyLossFault struct {
	Listener       string `json:"listener"`
	KeyDigest      string `json:"key_digest"`
	ScriptSHA      string `json:"script_sha,omitempty"`
	Armed          bool   `json:"armed"`
	Triggered      bool   `json:"triggered"`
	TriggerEvent   uint64 `json:"trigger_event,omitempty"`
	BlockAfterDrop bool   `json:"block_after_drop"`
	Blocked        bool   `json:"blocked"`
	rawKey         string
}

type RedisCleanupObservation struct {
	KeyDigest   string `json:"key_digest"`
	Commands    string `json:"commands"`
	DeleteReply int64  `json:"delete_reply"`
	ExistsReply int64  `json:"exists_reply"`
}

var redisReadCommands = stringSet(
	"PING", "ECHO", "HELLO", "AUTH", "SELECT", "QUIT", "RESET",
	"CLIENT", "COMMAND", "INFO", "ROLE", "TIME", "DBSIZE", "LASTSAVE",
	"GET", "MGET", "GETRANGE", "STRLEN", "EXISTS", "TYPE", "TTL", "PTTL",
	"EXPIRETIME", "PEXPIRETIME", "HGET", "HMGET", "HGETALL", "HEXISTS", "HLEN", "HSTRLEN", "HKEYS", "HVALS", "HRANDFIELD",
	"LINDEX", "LLEN", "LRANGE", "SCARD", "SISMEMBER", "SMISMEMBER", "SMEMBERS", "SRANDMEMBER",
	"ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANGE", "ZRANGEBYSCORE", "ZRANK", "ZREVRANK", "ZSCORE", "ZMSCORE",
	"XINFO", "XLEN", "XRANGE", "XREVRANGE", "XPENDING", "PFCOUNT", "BITCOUNT", "GETBIT", "BITPOS",
	"SCRIPT", "PUBSUB",
)

var redisSingleKeyMutations = stringSet(
	"SET", "SETNX", "SETEX", "PSETEX", "GETSET", "GETDEL", "GETEX", "SETRANGE", "APPEND", "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
	"HSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
	"LPUSH", "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "LSET", "LTRIM", "LREM", "LINSERT",
	"SADD", "SREM", "SPOP", "SMOVE",
	"ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX",
	"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TOUCH",
	"XADD", "XDEL", "XTRIM", "XACK", "XCLAIM", "XAUTOCLAIM", "XGROUP",
	"SETBIT", "BITFIELD", "PFADD", "GEOADD",
)

// Transaction markers carry no keys of their own. Queued mutations still pass
// through classifyRedisRequest individually so the exact-key ledger remains
// complete; MULTI/EXEC/DISCARD/UNWATCH must not be fail-closed as unclassified.
var redisTransactionCommands = stringSet("MULTI", "EXEC", "DISCARD", "UNWATCH")

// NewRESPProxy parses the private backend URL but never exposes it in facts.
func NewRESPProxy(rawBackendURL, challenge string) (*RESPProxy, error) {
	if !validChallenge(challenge) {
		return nil, fmt.Errorf("%w: invalid Redis proxy challenge", ErrInvalidEvidence)
	}
	opts, err := redis.ParseURL(strings.TrimSpace(rawBackendURL))
	if err != nil {
		return nil, fmt.Errorf("parse Redis backend: %w", err)
	}
	if strings.TrimSpace(opts.Addr) == "" {
		return nil, errors.New("Redis backend address is required")
	}
	return &RESPProxy{
		challenge: challenge, backend: *opts, listeners: make(map[string]net.Listener),
		addresses: make(map[string]string), conns: make(map[net.Conn]struct{}),
		ledger: make(map[string]*RedisKeyLedgerEntry),
	}, nil
}

// Listen creates one independently faultable endpoint. Labels are evidence,
// so they must be stable non-secret tokens.
func (p *RESPProxy) Listen(label string) (string, error) {
	if p == nil || !safeToken(label) {
		return "", fmt.Errorf("%w: invalid Redis listener label", ErrInvalidEvidence)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = listener.Close()
		return "", errors.New("Redis proxy is closed")
	}
	if _, exists := p.listeners[label]; exists {
		p.mu.Unlock()
		_ = listener.Close()
		return "", errors.New("duplicate Redis listener label")
	}
	p.listeners[label] = listener
	p.addresses[label] = listener.Addr().String()
	p.mu.Unlock()
	go p.accept(label, listener)
	return p.proxyURL(listener.Addr().String()), nil
}

func (p *RESPProxy) proxyURL(address string) string {
	value := &url.URL{Scheme: "redis", Host: address}
	if p.backend.Username != "" || p.backend.Password != "" {
		value.User = url.UserPassword(p.backend.Username, p.backend.Password)
	}
	if p.backend.DB != 0 {
		value.Path = "/" + strconv.Itoa(p.backend.DB)
	}
	return value.String()
}

func (p *RESPProxy) accept(label string, listener net.Listener) {
	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = client.Close()
			return
		}
		p.nextConn++
		connection := p.nextConn
		p.conns[client] = struct{}{}
		p.mu.Unlock()
		go p.serve(label, connection, client)
	}
}

func (p *RESPProxy) dialBackend(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if p.backend.TLSConfig != nil {
		config := p.backend.TLSConfig.Clone()
		return tls.DialWithDialer(dialer, "tcp", p.backend.Addr, config)
	}
	return dialer.DialContext(ctx, "tcp", p.backend.Addr)
}

func (p *RESPProxy) serve(label string, connection uint64, client net.Conn) {
	defer func() {
		_ = client.Close()
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()
	backend, err := p.dialBackend(context.Background())
	if err != nil {
		_, _ = io.WriteString(client, "-ERR RT2Q Redis backend unavailable\r\n")
		return
	}
	defer backend.Close()
	clientReader := bufio.NewReader(client)
	backendReader := bufio.NewReader(backend)
	for {
		request, args, err := readRESPRequest(clientReader)
		if err != nil {
			return
		}
		command, class, scriptSHA, keys, classifyErr := classifyRedisRequest(args)
		if classifyErr != nil {
			p.recordRedisEvent(label, connection, command, "rejected", scriptSHA, nil, request, nil, false, false, true)
			_, _ = io.WriteString(client, "-ERR RT2Q rejected unclassified Redis command\r\n")
			continue
		}
		if p.listenerBlocked(label) {
			p.recordRedisEvent(label, connection, command, "fault-blocked", scriptSHA, keys, request, nil, false, false, true)
			return
		}
		var event uint64
		if class == "mutation" {
			event = p.registerMutation(label, connection, command, scriptSHA, keys, request)
		}
		if _, err := backend.Write(request); err != nil {
			return
		}
		response, err := readRESPFrame(backendReader)
		if err != nil {
			return
		}
		drop := p.shouldDropReply(label, command, scriptSHA, keys, event, response)
		if class != "mutation" {
			p.recordRedisEvent(label, connection, command, class, scriptSHA, keys, request, response, true, drop, false)
		} else {
			p.completeMutationEvent(event, response, drop)
		}
		if drop {
			return
		}
		if _, err := client.Write(response); err != nil {
			return
		}
	}
}

// ArmReplyLoss forwards and fully reads one matching EVALSHA response, drops
// it, then optionally blocks later traffic from that named listener.
func (p *RESPProxy) ArmReplyLoss(listener, rawKey, scriptSHA string, blockAfterDrop bool) error {
	if p == nil || strings.TrimSpace(rawKey) == "" {
		return errors.New("Redis reply-loss fault requires an exact key")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.listeners[listener]; !ok {
		return errors.New("Redis reply-loss listener is unavailable")
	}
	p.faultWait = make(chan struct{})
	p.fault = &RedisReplyLossFault{
		Listener: listener, KeyDigest: p.keyDigest(rawKey), ScriptSHA: strings.ToLower(strings.TrimSpace(scriptSHA)),
		Armed: true, BlockAfterDrop: blockAfterDrop, rawKey: rawKey,
	}
	return nil
}

// ClearReplyLoss disarms the reply-loss fault and unblocks the named listener so
// a replacement controller can use the audited Redis boundary again.
func (p *RESPProxy) ClearReplyLoss() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fault = nil
	p.faultWait = nil
}

// WaitFaultTriggered waits until the armed reply-loss fault drops its one
// matching successful EVALSHA reply. It does not sleep for correctness.
func (p *RESPProxy) WaitFaultTriggered(ctx context.Context) error {
	if p == nil || ctx == nil {
		return errors.New("Redis reply-loss wait requires proxy and context")
	}
	p.mu.Lock()
	ch := p.faultWait
	triggered := p.fault != nil && p.fault.Triggered
	p.mu.Unlock()
	if triggered {
		return nil
	}
	if ch == nil {
		return errors.New("Redis reply-loss fault is not armed")
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *RESPProxy) listenerBlocked(label string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fault != nil && p.fault.Listener == label && p.fault.Blocked
}

func (p *RESPProxy) shouldDropReply(label, command, scriptSHA string, keys []string, event uint64, response []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	fault := p.fault
	if fault == nil || !fault.Armed || fault.Triggered || label != fault.Listener || command != "EVALSHA" {
		return false
	}
	if fault.ScriptSHA != "" && !strings.EqualFold(fault.ScriptSHA, scriptSHA) {
		return false
	}
	matched := false
	for _, key := range keys {
		if key == fault.rawKey {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	// Drop only a successful Transfer APPLIED reply. NOSCRIPT / error replies
	// and ALREADY_APPLIED must remain visible to the production client.
	if !redisReplyIndicatesTransferApplied(response) {
		return false
	}
	fault.Triggered = true
	fault.TriggerEvent = event
	fault.Blocked = fault.BlockAfterDrop
	if p.faultWait != nil {
		select {
		case <-p.faultWait:
		default:
			close(p.faultWait)
		}
	}
	return true
}

// redisReplyIndicatesTransferApplied recognizes the Transfer Lua APPLIED status
// bulk string without parsing Redis values into trusted evidence.
func redisReplyIndicatesTransferApplied(response []byte) bool {
	return bytes.Contains(response, []byte("$7\r\nAPPLIED\r\n"))
}

func (p *RESPProxy) registerMutation(label string, connection uint64, command, scriptSHA string, keys []string, request []byte) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextEvent++
	event := p.nextEvent
	digests := make([]string, 0, len(keys))
	for _, key := range keys {
		digest := p.keyDigest(key)
		digests = append(digests, digest)
		entry := p.ledger[key]
		if entry == nil {
			entry = &RedisKeyLedgerEntry{KeyDigest: digest, FirstEvent: event, MutationSeen: true, rawKey: key}
			p.ledger[key] = entry
		}
		if !contains(entry.Commands, command) {
			entry.Commands = append(entry.Commands, command)
			sort.Strings(entry.Commands)
		}
	}
	p.events = append(p.events, RedisAuditEvent{
		Sequence: event, Listener: label, Connection: connection, Command: command, Class: "mutation", ScriptSHA: scriptSHA,
		KeyDigests: digests, RequestSHA256: digestBytes(request), Forwarded: true, WallTime: time.Now().UTC().Format(time.RFC3339Nano),
	})
	return event
}

func (p *RESPProxy) completeMutationEvent(sequence uint64, response []byte, dropped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for index := range p.events {
		if p.events[index].Sequence == sequence {
			p.events[index].ResponseSHA256 = digestBytes(response)
			p.events[index].ReplyDropped = dropped
			return
		}
	}
}

func (p *RESPProxy) recordRedisEvent(label string, connection uint64, command, class, scriptSHA string, keys []string, request, response []byte, forwarded, dropped, rejected bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextEvent++
	digests := make([]string, 0, len(keys))
	for _, key := range keys {
		digests = append(digests, p.keyDigest(key))
	}
	p.events = append(p.events, RedisAuditEvent{
		Sequence: p.nextEvent, Listener: label, Connection: connection, Command: command, Class: class,
		ScriptSHA: scriptSHA, KeyDigests: digests, RequestSHA256: digestBytes(request), ResponseSHA256: digestBytes(response),
		Forwarded: forwarded, ReplyDropped: dropped, Rejected: rejected, WallTime: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (p *RESPProxy) keyDigest(raw string) string {
	digest := sha256.Sum256([]byte("rt2q-redis-key\x00" + p.challenge + "\x00" + raw))
	return hex.EncodeToString(digest[:])
}

func (p *RESPProxy) Ledger() ([]RedisKeyLedgerEntry, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := make([]RedisKeyLedgerEntry, 0, len(p.ledger))
	for _, private := range p.ledger {
		entries = append(entries, RedisKeyLedgerEntry{
			KeyDigest: private.KeyDigest, Commands: append([]string(nil), private.Commands...),
			FirstEvent: private.FirstEvent, MutationSeen: private.MutationSeen,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].KeyDigest < entries[j].KeyDigest })
	return entries, canonicalDigest(entries)
}

func (p *RESPProxy) Events() []RedisAuditEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]RedisAuditEvent(nil), p.events...)
}

func (p *RESPProxy) Fault() *RedisReplyLossFault {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fault == nil {
		return nil
	}
	copy := *p.fault
	copy.rawKey = ""
	return &copy
}

// Cleanup uses a dedicated already-audited connection and issues one exact
// DEL then EXISTS for every private ledger key. Discovery commands and flushes
// are neither needed nor accepted.
func (p *RESPProxy) Cleanup(ctx context.Context, listenerLabel string) ([]RedisCleanupObservation, error) {
	if p == nil || ctx == nil {
		return nil, errors.New("Redis cleanup requires proxy and context")
	}
	p.mu.Lock()
	address, listenerFound := p.addresses[listenerLabel]
	keys := make([]string, 0, len(p.ledger))
	for key := range p.ledger {
		keys = append(keys, key)
	}
	p.mu.Unlock()
	if !listenerFound || p.listenerBlocked(listenerLabel) {
		return nil, errors.New("Redis cleanup requires an available audited listener")
	}
	sort.Slice(keys, func(i, j int) bool { return p.keyDigest(keys[i]) < p.keyDigest(keys[j]) })
	options, err := redis.ParseURL(p.proxyURL(address))
	if err != nil {
		return nil, errors.New("create audited Redis cleanup client")
	}
	client := redis.NewClient(options)
	defer client.Close()
	observations := make([]RedisCleanupObservation, 0, len(keys))
	for _, key := range keys {
		deleted, err := client.Del(ctx, key).Result()
		if err != nil {
			return observations, fmt.Errorf("delete ledger key %s: %w", p.keyDigest(key), err)
		}
		exists, err := client.Exists(ctx, key).Result()
		if err != nil {
			return observations, fmt.Errorf("exists ledger key %s: %w", p.keyDigest(key), err)
		}
		observation := RedisCleanupObservation{KeyDigest: p.keyDigest(key), Commands: "DEL,EXISTS", DeleteReply: deleted, ExistsReply: exists}
		observations = append(observations, observation)
		if exists != 0 {
			return observations, fmt.Errorf("Redis cleanup residue for key %s", observation.KeyDigest)
		}
	}
	return observations, nil
}

func (p *RESPProxy) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	listeners := make([]net.Listener, 0, len(p.listeners))
	for _, listener := range p.listeners {
		listeners = append(listeners, listener)
	}
	connections := make([]net.Conn, 0, len(p.conns))
	for connection := range p.conns {
		connections = append(connections, connection)
	}
	p.mu.Unlock()
	var errs []error
	for _, listener := range listeners {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func classifyRedisRequest(args [][]byte) (command, class, scriptSHA string, keys []string, err error) {
	if len(args) == 0 {
		return "", "", "", nil, errors.New("empty Redis request")
	}
	command = strings.ToUpper(string(args[0]))
	if _, ok := redisReadCommands[command]; ok {
		if command == "SCRIPT" && (len(args) < 2 || strings.ToUpper(string(args[1])) != "LOAD" && strings.ToUpper(string(args[1])) != "EXISTS") {
			return command, "", "", nil, errors.New("unsafe SCRIPT subcommand")
		}
		return command, "read", "", redisReadKeys(command, args), nil
	}
	if _, ok := redisTransactionCommands[command]; ok {
		return command, "transaction", "", nil, nil
	}
	if command == "WATCH" {
		if len(args) < 2 {
			return command, "", "", nil, errors.New("WATCH requires keys")
		}
		keys := make([]string, 0, len(args)-1)
		for _, key := range args[1:] {
			keys = append(keys, string(key))
		}
		return command, "mutation", "", keys, nil
	}
	if command == "EVAL" || command == "EVALSHA" {
		if len(args) < 3 {
			return command, "", "", nil, errors.New("invalid eval request")
		}
		if command == "EVALSHA" {
			scriptSHA = strings.ToLower(string(args[1]))
		} else {
			digest := sha256.Sum256(args[1])
			scriptSHA = hex.EncodeToString(digest[:])
		}
		count, parseErr := strconv.Atoi(string(args[2]))
		if parseErr != nil || count <= 0 || len(args) < 3+count {
			return command, "", scriptSHA, nil, errors.New("eval must declare one or more exact keys")
		}
		for _, key := range args[3 : 3+count] {
			keys = append(keys, string(key))
		}
		return command, "mutation", scriptSHA, keys, nil
	}
	if command == "DEL" || command == "UNLINK" || command == "MSET" || command == "MSETNX" {
		start, step := 1, 1
		if command == "MSET" || command == "MSETNX" {
			step = 2
		}
		for index := start; index < len(args); index += step {
			keys = append(keys, string(args[index]))
		}
		if len(keys) == 0 {
			return command, "", "", nil, errors.New("mutation requires keys")
		}
		return command, "mutation", "", keys, nil
	}
	if command == "RENAME" || command == "RENAMENX" || command == "COPY" || command == "SMOVE" || command == "RPOPLPUSH" || command == "LMOVE" || command == "BLMOVE" {
		if len(args) < 3 {
			return command, "", "", nil, errors.New("multi-key mutation requires keys")
		}
		return command, "mutation", "", []string{string(args[1]), string(args[2])}, nil
	}
	if _, ok := redisSingleKeyMutations[command]; ok {
		if len(args) < 2 {
			return command, "", "", nil, errors.New("mutation requires a key")
		}
		return command, "mutation", "", []string{string(args[1])}, nil
	}
	return command, "", "", nil, fmt.Errorf("unclassified Redis command %s", command)
}

func redisReadKeys(command string, args [][]byte) []string {
	switch command {
	case "GET", "GETRANGE", "STRLEN", "TYPE", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME", "HGET", "HMGET", "HGETALL", "HEXISTS", "HLEN", "HSTRLEN", "HKEYS", "HVALS", "HRANDFIELD", "LINDEX", "LLEN", "LRANGE", "SCARD", "SISMEMBER", "SMISMEMBER", "SMEMBERS", "SRANDMEMBER", "ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANGE", "ZRANGEBYSCORE", "ZRANK", "ZREVRANK", "ZSCORE", "ZMSCORE", "XINFO", "XLEN", "XRANGE", "XREVRANGE", "XPENDING", "PFCOUNT", "BITCOUNT", "GETBIT", "BITPOS":
		if len(args) > 1 {
			return []string{string(args[1])}
		}
	case "MGET", "EXISTS":
		keys := make([]string, 0, len(args)-1)
		for _, key := range args[1:] {
			keys = append(keys, string(key))
		}
		return keys
	}
	return nil
}

func readRESPRequest(reader *bufio.Reader) ([]byte, [][]byte, error) {
	frame, err := readRESPFrame(reader)
	if err != nil {
		return nil, nil, err
	}
	args, err := parseRESPArray(frame)
	return frame, args, err
}

func readRESPFrame(reader *bufio.Reader) ([]byte, error) {
	var output bytes.Buffer
	if err := copyRESPFrame(reader, &output); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func copyRESPFrame(reader *bufio.Reader, output *bytes.Buffer) error {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return errors.New("invalid RESP line")
	}
	_, _ = output.Write(line)
	prefix := line[0]
	value := strings.TrimSuffix(strings.TrimSuffix(string(line[1:]), "\n"), "\r")
	switch prefix {
	case '+', '-', ':', '_', '#', ',', '(', '!':
		if prefix == '!' {
			length, parseErr := strconv.Atoi(value)
			if parseErr != nil || length < 0 {
				return errors.New("invalid RESP blob error")
			}
			return copyRESPBytes(reader, output, length+2)
		}
		return nil
	case '$', '=':
		length, parseErr := strconv.Atoi(value)
		if parseErr != nil || length < -1 {
			return errors.New("invalid RESP bulk length")
		}
		if length == -1 {
			return nil
		}
		return copyRESPBytes(reader, output, length+2)
	case '*', '~', '>':
		count, parseErr := strconv.Atoi(value)
		if parseErr != nil || count < -1 {
			return errors.New("invalid RESP aggregate length")
		}
		for index := 0; index < count; index++ {
			if err := copyRESPFrame(reader, output); err != nil {
				return err
			}
		}
		return nil
	case '%', '|':
		count, parseErr := strconv.Atoi(value)
		if parseErr != nil || count < -1 {
			return errors.New("invalid RESP map length")
		}
		for index := 0; index < count*2; index++ {
			if err := copyRESPFrame(reader, output); err != nil {
				return err
			}
		}
		if prefix == '|' {
			return copyRESPFrame(reader, output)
		}
		return nil
	default:
		return errors.New("unsupported RESP frame")
	}
}

func copyRESPBytes(reader *bufio.Reader, output *bytes.Buffer, length int) error {
	if length < 2 {
		return errors.New("invalid RESP byte length")
	}
	buffer := make([]byte, length)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return err
	}
	if buffer[length-2] != '\r' || buffer[length-1] != '\n' {
		return errors.New("invalid RESP bulk terminator")
	}
	_, _ = output.Write(buffer)
	return nil
}

func parseRESPArray(frame []byte) ([][]byte, error) {
	reader := bufio.NewReader(bytes.NewReader(frame))
	line, err := reader.ReadString('\n')
	if err != nil || len(line) < 3 || line[0] != '*' {
		return nil, errors.New("Redis request must be a RESP array")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count <= 0 {
		return nil, errors.New("invalid Redis request array")
	}
	args := make([][]byte, 0, count)
	for index := 0; index < count; index++ {
		bulk, readErr := reader.ReadString('\n')
		if readErr != nil || len(bulk) < 3 || bulk[0] != '$' {
			return nil, errors.New("Redis request arguments must be bulk strings")
		}
		length, parseErr := strconv.Atoi(strings.TrimSpace(bulk[1:]))
		if parseErr != nil || length < 0 {
			return nil, errors.New("invalid Redis request argument length")
		}
		value := make([]byte, length+2)
		if _, readErr := io.ReadFull(reader, value); readErr != nil || value[length] != '\r' || value[length+1] != '\n' {
			return nil, errors.New("invalid Redis request argument")
		}
		args = append(args, append([]byte(nil), value[:length]...))
	}
	if reader.Buffered() != 0 {
		return nil, errors.New("trailing Redis request bytes")
	}
	return args, nil
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func digestBytes(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func canonicalDigest(value any) string {
	encoded := fmt.Sprintf("%#v", value)
	digest := sha256.Sum256([]byte(encoded))
	return hex.EncodeToString(digest[:])
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
