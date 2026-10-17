// mixed-farm-bot generates a deterministic, non-networked workload recipe.
//
// It is intentionally dry-run only. A later execution adapter will be reviewed
// separately and must explicitly opt into real HTTP/WSS traffic.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"farmgame/internal/loadbot"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mixed-farm-bot:", err)
		os.Exit(1)
	}
}

func run(arguments []string, stdout io.Writer) error {
	if len(arguments) == 0 || arguments[0] != "dry-run" {
		return errors.New("usage: mixed-farm-bot dry-run [--players N] [--actions N] [--seed N] [--duration D] [--run-id ID] [--fixture-id ID] [--environment ENV] [--namespace NS]")
	}
	flags := flag.NewFlagSet("dry-run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	players := flags.Int("players", 20, "bounded number of synthetic players")
	actions := flags.Int("actions", 8, "bounded actions per synthetic session")
	seed := flags.Int64("seed", 0, "override deterministic seed; zero keeps default")
	duration := flags.Duration("duration", 0, "override synthetic run duration")
	runID := flags.String("run-id", "", "override synthetic run identity")
	fixtureID := flags.String("fixture-id", "", "override run-scoped fixture identity")
	environment := flags.String("environment", string(loadbot.EnvironmentDryRun), "safe allowlisted environment")
	namespace := flags.String("namespace", "", "run-scoped namespace; empty derives from run id")
	if err := flags.Parse(arguments[1:]); err != nil {
		return err
	}
	config := loadbot.DefaultConfig()
	if *seed != 0 {
		config.Seed = *seed
	}
	if *duration != 0 {
		config.Duration = *duration
	}
	if strings.TrimSpace(*runID) != "" {
		config.RunID = strings.TrimSpace(*runID)
	}
	plan, err := loadbot.BuildDryRun(config, *players, *actions)
	if err != nil {
		return err
	}
	if err := loadbot.ValidatePlan(plan); err != nil {
		return err
	}
	spec, err := loadbot.NewRunSpec(config, *fixtureID, *environment, *namespace)
	if err != nil {
		return err
	}
	run, err := loadbot.NewRun(spec, time.Unix(0, 0).UTC())
	if err != nil {
		return err
	}
	manifest, err := loadbot.BuildFixtureManifest(spec, plan)
	if err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		GeneratedAt     time.Time               `json:"generated_at"`
		RunSpec         loadbot.RunSpec         `json:"run_spec"`
		Run             loadbot.Run             `json:"run"`
		Plan            loadbot.DryRunPlan      `json:"plan"`
		FixtureManifest loadbot.FixtureManifest `json:"fixture_manifest"`
	}{
		GeneratedAt: time.Now().UTC(), RunSpec: spec, Run: run,
		Plan: plan, FixtureManifest: manifest,
	})
}
