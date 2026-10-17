package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"farmgame/internal/loadtest/friendopportunity"
)

type options struct {
	config    friendopportunity.RunConfig
	duration  time.Duration
	efficient time.Duration
	outputDir string
	matrix    bool
	variant   string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	opts := parseFlags()
	opts.config.DurationMS = opts.duration.Milliseconds()
	opts.config.EfficientFallbackIntervalMS = opts.efficient.Milliseconds()
	if err := opts.config.Validate(); err != nil {
		return err
	}
	if opts.matrix {
		for _, config := range friendopportunity.MatrixConfigs(opts.config) {
			if err := runCell(filepath.Join(opts.outputDir, friendopportunity.CellID(config)), config, opts.variant); err != nil {
				return err
			}
		}
		fmt.Printf("synthetic matrix complete: cells=%d out=%s\n",
			len(friendopportunity.MatrixConfigs(opts.config)), opts.outputDir)
		return nil
	}
	return runCell(opts.outputDir, opts.config, opts.variant)
}

func parseFlags() options {
	c := friendopportunity.DefaultConfig()
	var opts options
	opts.config = c
	flag.Int64Var(&opts.config.Seed, "seed", c.Seed, "fixed master seed")
	flag.IntVar(&opts.config.FriendCount, "friends", c.FriendCount, "friend count: 20, 100, or 300")
	flag.IntVar(&opts.config.PlotCount, "plots", c.PlotCount, "plots per farm: 5, 12, or 24")
	flag.IntVar(&opts.config.OpportunityDensityPercent, "density", c.OpportunityDensityPercent, "opportunity density percent: 0, 5, 20, or 50")
	flag.Float64Var(&opts.config.OnlineRatePercent, "online-rate", c.OnlineRatePercent, "online rate percent: 1.9, 5, or 10")
	expiry := flag.String("expiry", string(c.ExpiryPattern), "expiry pattern: uniform or bursty")
	flag.Float64Var(&opts.config.ArrivalRatePerSecond, "arrival-rate", c.ArrivalRatePerSecond, "open-loop session arrivals per second")
	flag.DurationVar(&opts.duration, "duration", time.Duration(c.DurationMS)*time.Millisecond, "run horizon")
	flag.DurationVar(&opts.efficient, "efficient-fallback", time.Duration(c.EfficientFallbackIntervalMS)*time.Millisecond, "B2 fallback interval")
	flag.IntVar(&opts.config.PullPageSize, "pull-page-size", c.PullPageSize, "friends per fallback pull page")
	flag.IntVar(&opts.config.GateBatchSize, "gate-batch-size", c.GateBatchSize, "online hint targets per gate batch")
	flag.IntVar(&opts.config.WorkerConcurrency, "workers", c.WorkerConcurrency, "synthetic request workers")
	flag.IntVar(&opts.config.MaxInFlight, "max-in-flight", c.MaxInFlight, "synthetic admission cap; zero is unlimited")
	flag.StringVar(&opts.outputDir, "out-dir", "/tmp/friend-opportunity-realtime-ab", "artifact output directory")
	flag.BoolVar(&opts.matrix, "matrix", false, "run all 216 fixed dimension cells")
	flag.StringVar(&opts.variant, "variant", "all", "all, A, B1, or B2")
	flag.Parse()
	opts.config.ExpiryPattern = friendopportunity.ExpiryPattern(*expiry)
	return opts
}

func runCell(outputDir string, c friendopportunity.RunConfig, selection string) error {
	trace, err := friendopportunity.GenerateTrace(c)
	if err != nil {
		return err
	}
	results, err := selectResults(c, trace, selection)
	if err != nil {
		return err
	}
	if err := friendopportunity.WriteArtifacts(outputDir, c, trace, results); err != nil {
		return err
	}
	summary := struct {
		Synthetic   bool                          `json:"synthetic"`
		Cell        string                        `json:"cell"`
		OutputDir   string                        `json:"output_dir"`
		TraceHashes friendopportunity.TraceHashes `json:"trace_hashes"`
		Results     []friendopportunity.RunResult `json:"results"`
	}{true, friendopportunity.CellID(c), outputDir, trace.Hashes(), results}
	data, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func selectResults(c friendopportunity.RunConfig, trace friendopportunity.Trace, selection string) ([]friendopportunity.RunResult, error) {
	switch strings.ToUpper(strings.TrimSpace(selection)) {
	case "ALL":
		return friendopportunity.RunAll(c, trace)
	case "A", "PULL":
		return oneResult(c, trace, friendopportunity.VariantPull)
	case "B1", "SCHEDULERSAFE":
		return oneResult(c, trace, friendopportunity.VariantSchedulerSafe)
	case "B2", "SCHEDULEREFFICIENT":
		return oneResult(c, trace, friendopportunity.VariantSchedulerEfficient)
	default:
		return nil, fmt.Errorf("friend-opportunity-bench: unknown variant %q (want all, A, B1, or B2)", selection)
	}
}

func oneResult(c friendopportunity.RunConfig, trace friendopportunity.Trace, variant friendopportunity.Variant) ([]friendopportunity.RunResult, error) {
	result, err := friendopportunity.RunVariant(c, trace, variant)
	if err != nil {
		return nil, err
	}
	return []friendopportunity.RunResult{result}, nil
}
