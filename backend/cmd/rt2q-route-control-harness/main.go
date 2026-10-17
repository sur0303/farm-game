// Command rt2q-route-control-harness executes and independently verifies the
// trusted RT2Q Final evidence contract.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"farmgame/internal/testkit/routecontrol"
)

const taskID = "RT2Q-ROUTE-CONTROL-FINAL"

const (
	exitPass         = 0
	exitFail         = 1
	exitInconclusive = 2
	exitUsage        = 64
)

type runConfig struct {
	level          routecontrol.EvidenceLevel
	expectedHead   string
	runtime        string
	redisMode      string
	redisURLEnv    string
	scenario       string
	artifactRoot   string
	overallTimeout time.Duration
	worktree       string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, output, errorOutput io.Writer) int {
	if len(args) == 0 {
		writeUsage(errorOutput)
		return exitUsage
	}
	switch args[0] {
	case "run":
		cfg, err := parseRun(args[1:], errorOutput)
		if err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintln(errorOutput, "invalid run arguments")
			}
			return exitUsage
		}
		manifest, oracle, runErr := executeRun(cfg)
		status := manifest.Status
		reason := manifest.Reason
		if status == "" {
			status = routecontrol.StatusInconclusive
			reason = "trusted runtime unavailable before manifest finalization"
		}
		if runErr != nil && status == routecontrol.StatusPass {
			status = routecontrol.StatusFail
			reason = "runtime contradicted a derived PASS"
		}
		fmt.Fprintf(output, "%s %s: %s\n", cfg.level, status, reason)
		if oracle.RunID != "" {
			fmt.Fprintf(output, "run=%s records=%d\n", cfg.artifactRoot, oracle.RecordCount)
		}
		return statusExit(status)
	case "verify":
		expectedHead, root, err := parseVerify(args[1:], errorOutput)
		if err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintln(errorOutput, "invalid verify arguments")
			}
			return exitUsage
		}
		manifest, oracle, err := routecontrol.VerifyTrustedRun(root, expectedHead)
		if err != nil {
			fmt.Fprintln(output, "VERIFY FAIL: trusted artifact verification failed")
			return exitFail
		}
		fmt.Fprintf(output, "VERIFY %s: %s\n", manifest.Status, manifest.Reason)
		fmt.Fprintf(output, "run=%s records=%d chain=%s\n", manifest.RunID, oracle.RecordCount, oracle.ChainHead)
		return statusExit(manifest.Status)
	case "help", "-h", "--help":
		writeUsage(errorOutput)
		return exitUsage
	default:
		writeUsage(errorOutput)
		return exitUsage
	}
}

func parseRun(args []string, errorOutput io.Writer) (runConfig, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(errorOutput)
	level := fs.String("level", "", "E3 or E4")
	expectedHead := fs.String("expected-head", "", "exact clean detached Git HEAD")
	runtimeName := fs.String("runtime", "", "process or container")
	redisMode := fs.String("redis-mode", "", "isolated or company")
	redisURLEnv := fs.String("redis-url-env", "RT2Q_COMPANY_REDIS_URL", "company Redis URL environment variable name")
	scenario := fs.String("scenario", "", "all, S0..S7, deadline-refresh, or company-canary")
	artifactRoot := fs.String("artifact-root", "", "new private evidence directory")
	overallTimeout := fs.Duration("overall-timeout", 0, "whole-run timeout")
	worktree := fs.String("worktree", ".", "Git worktree")
	if err := fs.Parse(args); err != nil {
		return runConfig{}, err
	}
	cfg := runConfig{
		level: routecontrol.EvidenceLevel(strings.ToUpper(strings.TrimSpace(*level))), expectedHead: strings.ToLower(strings.TrimSpace(*expectedHead)),
		runtime: strings.TrimSpace(*runtimeName), redisMode: strings.TrimSpace(*redisMode), redisURLEnv: strings.TrimSpace(*redisURLEnv),
		scenario: strings.TrimSpace(*scenario), artifactRoot: strings.TrimSpace(*artifactRoot), overallTimeout: *overallTimeout,
		worktree: strings.TrimSpace(*worktree),
	}
	if fs.NArg() != 0 || (cfg.level != routecontrol.EvidenceE3 && cfg.level != routecontrol.EvidenceE4) || len(cfg.expectedHead) != 40 ||
		(cfg.runtime != "process" && cfg.runtime != "container") || (cfg.redisMode != "isolated" && cfg.redisMode != "company") ||
		cfg.artifactRoot == "" || cfg.overallTimeout <= 0 || cfg.overallTimeout > 30*time.Minute || cfg.worktree == "" ||
		!validScenarioFlag(cfg.scenario) || (cfg.redisMode == "company" && cfg.scenario != "company-canary" && cfg.scenario != "S7") ||
		(cfg.redisMode == "isolated" && (cfg.scenario == "company-canary" || cfg.scenario == "S7")) ||
		(cfg.redisMode == "company" && cfg.scenario == "deadline-refresh") ||
		(cfg.level == routecontrol.EvidenceE3 && cfg.runtime != "process") || (cfg.level == routecontrol.EvidenceE4 && cfg.runtime != "container") ||
		(cfg.redisMode == "company" && !validEnvironmentName(cfg.redisURLEnv)) {
		return runConfig{}, errors.New("invalid trusted run configuration")
	}
	return cfg, nil
}

func parseVerify(args []string, errorOutput io.Writer) (string, string, error) {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(errorOutput)
	expectedHead := fs.String("expected-head", "", "expected exact Git HEAD")
	root := fs.String("run", "", "trusted run directory")
	if err := fs.Parse(args); err != nil {
		return "", "", err
	}
	head := strings.ToLower(strings.TrimSpace(*expectedHead))
	directory := strings.TrimSpace(*root)
	if fs.NArg() != 0 || len(head) != 40 || directory == "" {
		return "", "", errors.New("expected-head and run are required")
	}
	return head, directory, nil
}

func validScenarioFlag(value string) bool {
	if value == "all" || value == "company-canary" || value == "deadline-refresh" {
		return true
	}
	return len(value) == 2 && value[0] == 'S' && value[1] >= '0' && value[1] <= '7'
}

func validEnvironmentName(value string) bool {
	if value == "" || value[0] < 'A' || value[0] > 'Z' {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' {
			continue
		}
		return false
	}
	return true
}

func statusExit(status routecontrol.EvidenceStatus) int {
	switch status {
	case routecontrol.StatusPass:
		return exitPass
	case routecontrol.StatusFail:
		return exitFail
	default:
		return exitInconclusive
	}
}

func writeUsage(destination io.Writer) {
	fmt.Fprintln(destination, "Usage:")
	fmt.Fprintln(destination, "  rt2q-route-control-harness run --level E3|E4 --expected-head SHA --runtime process|container --redis-mode isolated|company --scenario all|S0..S7|deadline-refresh|company-canary --artifact-root DIR --overall-timeout DURATION")
	fmt.Fprintln(destination, "  rt2q-route-control-harness verify --expected-head SHA --run DIR")
}
