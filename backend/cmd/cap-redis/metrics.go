package main

import (
	"context"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type wireCounters struct {
	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
	dials      atomic.Uint64
}

type countingConn struct {
	net.Conn
	counters *wireCounters
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.counters.readBytes.Add(uint64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.counters.writeBytes.Add(uint64(n))
	return n, err
}

type metricSnapshot struct {
	ReadBytes  uint64
	WriteBytes uint64
	Dials      uint64
	Commands   uint64
	Pipelines  uint64
	Items      uint64
	ByCommand  map[string]uint64
}

type redisMetricHook struct {
	wire      wireCounters
	mu        sync.Mutex
	commands  uint64
	pipelines uint64
	items     uint64
	byCommand map[string]uint64
}

func newRedisMetricHook() *redisMetricHook {
	return &redisMetricHook{byCommand: make(map[string]uint64)}
}

func (h *redisMetricHook) countingDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	h.wire.dials.Add(1)
	return &countingConn{Conn: conn, counters: &h.wire}, nil
}

func (h *redisMetricHook) DialHook(next redis.DialHook) redis.DialHook {
	// Options.Dialer is installed before NewClient so MinIdleConns created by
	// the pool are observable too. Do not wrap a second time here.
	return next
}

func (h *redisMetricHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.record([]redis.Cmder{cmd}, false)
		return err
	}
}

func (h *redisMetricHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		h.record(cmds, true)
		return err
	}
}

func (h *redisMetricHook) record(cmds []redis.Cmder, pipeline bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if pipeline {
		h.pipelines++
		h.items += uint64(len(cmds))
	}
	for _, cmd := range cmds {
		name := strings.ToUpper(strings.TrimSpace(cmd.Name()))
		if name == "" {
			name = "UNKNOWN"
		}
		h.commands++
		h.byCommand[name]++
	}
}

func (h *redisMetricHook) snapshot() metricSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	byCommand := make(map[string]uint64, len(h.byCommand))
	for k, v := range h.byCommand {
		byCommand[k] = v
	}
	return metricSnapshot{
		ReadBytes: h.wire.readBytes.Load(), WriteBytes: h.wire.writeBytes.Load(), Dials: h.wire.dials.Load(),
		Commands: h.commands, Pipelines: h.pipelines, Items: h.items, ByCommand: byCommand,
	}
}

func diffMetrics(after, before metricSnapshot) metricSnapshot {
	out := metricSnapshot{
		ReadBytes: after.ReadBytes - before.ReadBytes, WriteBytes: after.WriteBytes - before.WriteBytes,
		Dials: after.Dials - before.Dials, Commands: after.Commands - before.Commands,
		Pipelines: after.Pipelines - before.Pipelines, Items: after.Items - before.Items,
		ByCommand: make(map[string]uint64),
	}
	for name, count := range after.ByCommand {
		if delta := count - before.ByCommand[name]; delta != 0 {
			out.ByCommand[name] = delta
		}
	}
	return out
}

func sortedCommands(values map[string]uint64) []commandCount {
	out := make([]commandCount, 0, len(values))
	for name, count := range values {
		out = append(out, commandCount{Name: name, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func percentile(values []float64) percentileSet {
	if len(values) == 0 {
		return percentileSet{}
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	pick := func(q float64) float64 {
		// Nearest-rank percentile: ceil(q*N)-1. This keeps an exact 50/45/4/1
		// fixture at P50/P95/P99 instead of crossing into the next bucket.
		index := int(math.Ceil(q*float64(len(copyValues)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(copyValues) {
			index = len(copyValues) - 1
		}
		return copyValues[index]
	}
	return percentileSet{P50: pick(.50), P95: pick(.95), P99: pick(.99), Min: copyValues[0], Max: copyValues[len(copyValues)-1]}
}

func durationsUS(values []time.Duration) percentileSet {
	converted := make([]float64, len(values))
	for i, value := range values {
		converted[i] = float64(value.Nanoseconds()) / 1000
	}
	return percentile(converted)
}

func ints(values []int) percentileSet {
	converted := make([]float64, len(values))
	for i, value := range values {
		converted[i] = float64(value)
	}
	return percentile(converted)
}

func parseRedisCPU(info string) float64 {
	var total float64
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_cpu_sys:") || strings.HasPrefix(line, "used_cpu_user:") {
			value, _ := strconv.ParseFloat(strings.TrimSpace(strings.SplitN(line, ":", 2)[1]), 64)
			total += value
		}
	}
	return total
}

func slowLogDurations(logs []redis.SlowLog) ([]time.Duration, []string) {
	var durations []time.Duration
	set := make(map[string]struct{})
	for _, entry := range logs {
		if len(entry.Args) == 0 {
			continue
		}
		name := strings.ToUpper(entry.Args[0])
		if name != "EVAL" && name != "EVALSHA" {
			continue
		}
		durations = append(durations, entry.Duration)
		label := name
		if len(entry.Args) > 1 {
			value := entry.Args[1]
			if len(value) > 40 {
				value = value[:40]
			}
			label += ":" + value
		}
		set[label] = struct{}{}
	}
	commands := make([]string, 0, len(set))
	for command := range set {
		commands = append(commands, command)
	}
	sort.Strings(commands)
	return durations, commands
}
