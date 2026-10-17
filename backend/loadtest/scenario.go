// Package loadtest 是 W5B 既有压测工具(cmd/lease-stress、cmd/ws-stress、
// scripts/test-multigate-drain)的扩展接缝层:它只负责声明场景参数、
// 生成既有二进制的命令行和所需证据清单,不实现第二套 Harness,
// 不运行真实压测。没有真实运行数据 + 抓取的 metrics/json 证据时,
// 任何场景不得标记 CAPACITY_PASS(证据等级定义见 docs/design/72 §16.5)。
package loadtest

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// LeaseStressParams 映射 cmd/lease-stress 的全部关键 flag。
// 零值字段不生成对应 flag(沿用二进制默认值)。
type LeaseStressParams struct {
	GateHTTP        string
	GateWS          string
	MetricsURL      string
	LoadTestSecret  string
	BootstrapPrefix string
	Viewers         int
	JoinConcurrency int
	Duration        time.Duration
	StepTimeout     time.Duration
	ActionInterval  time.Duration
}

// Validate 做边界校验;不校验真实连通性(那是真实运行的职责)。
func (p LeaseStressParams) Validate() error {
	if p.Viewers < 0 {
		return errors.New("loadtest: viewers must be >= 0")
	}
	if p.Viewers > 199 {
		// 合同 26 §HC-ACCESS-36:1 owner + 199 viewers = 200 上限。
		return fmt.Errorf("loadtest: viewers=%d exceeds the 199-viewer contract cap", p.Viewers)
	}
	if p.JoinConcurrency < 0 {
		return errors.New("loadtest: join-concurrency must be >= 0")
	}
	if p.Duration < 0 || p.StepTimeout < 0 || p.ActionInterval < 0 {
		return errors.New("loadtest: durations must be >= 0")
	}
	return nil
}

// Args 生成 cmd/lease-stress 的命令行参数(不含二进制名)。
func (p LeaseStressParams) Args() []string {
	var args []string
	appendStr := func(name, value string) {
		if value != "" {
			args = append(args, "-"+name, value)
		}
	}
	appendInt := func(name string, value int) {
		if value != 0 {
			args = append(args, "-"+name, strconv.Itoa(value))
		}
	}
	appendDur := func(name string, value time.Duration) {
		if value != 0 {
			args = append(args, "-"+name, value.String())
		}
	}
	appendStr("gate-http", p.GateHTTP)
	appendStr("gate-ws", p.GateWS)
	appendStr("metrics-url", p.MetricsURL)
	appendStr("load-test-secret", p.LoadTestSecret)
	appendStr("bootstrap-prefix", p.BootstrapPrefix)
	appendInt("viewers", p.Viewers)
	appendInt("join-concurrency", p.JoinConcurrency)
	appendDur("duration", p.Duration)
	appendDur("step-timeout", p.StepTimeout)
	appendDur("action-interval", p.ActionInterval)
	return args
}

// WSStressParams 映射 cmd/ws-stress 的全部 flag。
type WSStressParams struct {
	HTTPURL        string
	WSURL          string
	Conns          int
	Rate           int
	Duration       time.Duration
	PlayerStart    uint64
	Scenario       string // login|join|plant|hot|reconnect
	SharedFarm     uint64
	HotLoops       int
	HotMixed       bool
	ReconnectLoops int
	JSONOut        string
}

// Validate 校验 scenario 枚举与数值边界。
func (p WSStressParams) Validate() error {
	switch p.Scenario {
	case "", "login", "join", "plant", "hot", "reconnect":
	default:
		return fmt.Errorf("loadtest: unknown ws-stress scenario %q", p.Scenario)
	}
	if p.Conns < 0 || p.Rate < 0 || p.HotLoops < 0 || p.ReconnectLoops < 0 {
		return errors.New("loadtest: numeric fields must be >= 0")
	}
	if p.Duration < 0 {
		return errors.New("loadtest: duration must be >= 0")
	}
	return nil
}

// Args 生成 cmd/ws-stress 的命令行参数(不含二进制名)。
func (p WSStressParams) Args() []string {
	var args []string
	appendStr := func(name, value string) {
		if value != "" {
			args = append(args, "-"+name, value)
		}
	}
	appendInt := func(name string, value int) {
		if value != 0 {
			args = append(args, "-"+name, strconv.Itoa(value))
		}
	}
	appendStr("http-url", p.HTTPURL)
	appendStr("ws-url", p.WSURL)
	appendInt("conns", p.Conns)
	appendInt("rate", p.Rate)
	if p.Duration != 0 {
		args = append(args, "-duration", p.Duration.String())
	}
	if p.PlayerStart != 0 {
		args = append(args, "-player-start", strconv.FormatUint(p.PlayerStart, 10))
	}
	appendStr("scenario", p.Scenario)
	if p.SharedFarm != 0 {
		args = append(args, "-shared-farm", strconv.FormatUint(p.SharedFarm, 10))
	}
	appendInt("hot-loops", p.HotLoops)
	if p.HotMixed {
		args = append(args, "-hot-mixed")
	}
	appendInt("reconnect-loops", p.ReconnectLoops)
	appendStr("json-out", p.JSONOut)
	return args
}

// DrainScriptEnv 映射 scripts/test-multigate-drain 的环境变量旋钮。
type DrainScriptEnv struct {
	RedisConfig string // FARM_REDIS_CONFIG
	KeepTmp     bool   // MULTIGATE_KEEP_TMP=1
}

// Env 返回 "KEY=VALUE" 形态的环境变量对。
func (e DrainScriptEnv) Env() []string {
	var env []string
	if e.RedisConfig != "" {
		env = append(env, "FARM_REDIS_CONFIG="+e.RedisConfig)
	}
	if e.KeepTmp {
		env = append(env, "MULTIGATE_KEEP_TMP=1")
	}
	return env
}
