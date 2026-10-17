package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "SESSION_ROUTE_C2_ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		redisURL   string
		outputPath string
		runID      string
		mode       string
		iterations int
	)
	flag.StringVar(&redisURL, "redis-url", os.Getenv("SESSION_ROUTE_C2_REDIS_URL"), "isolated ephemeral Redis URL")
	flag.StringVar(&outputPath, "output", "", "evidence JSON output path")
	flag.StringVar(&runID, "run-id", "", "unique run id")
	flag.StringVar(&mode, "mode", "smoke", "smoke|full")
	flag.IntVar(&iterations, "iterations", 1, "timed iterations per capacity cell")
	flag.Parse()

	if redisURL == "" || outputPath == "" || runID == "" {
		return errors.New("redis-url, output, and run-id are required")
	}
	if mode != "smoke" && mode != "full" {
		return errors.New("mode must be smoke or full")
	}
	if iterations <= 0 {
		return errors.New("iterations must be positive")
	}

	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("redis URL: %w", err)
	}
	options.Protocol = 2
	hook := newRedisMetricHook()
	options.Dialer = hook.countingDialer
	client := redis.NewClient(options)
	client.AddHook(hook)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	deps := newProbeDeps(client, hook, runID)
	report, err := runProbe(ctx, deps, mode, iterations, runID)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outputPath, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("SESSION_ROUTE_C2_RUN=%s GO_NO_GO=%s CORRECTNESS=%v CELLS=%d TOPOLOGY=%s CLUSTER=%s\n",
		report.RunID, report.GoNoGo, report.CorrectnessOK, len(report.Cells), report.TopologyLabel, report.ClusterVerification)
	if !report.CorrectnessOK {
		return errors.New("correctness invariants failed")
	}
	return nil
}
