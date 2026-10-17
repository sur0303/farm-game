// Command actor-layout-inspect samples the existing Redis auth-v1 hashes using
// read-only commands. It never constructs an Actor Registry and therefore
// cannot Claim, activate, Commit, reconcile, repair, or migrate an Actor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type cliConfig struct {
	RedisURL      string
	Namespace     string
	SamplePlayers int
	SampleFarms   int
	Out           string
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("actor-layout-inspect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := cliConfig{}
	flags.StringVar(&config.RedisURL, "redis-url", "", "Redis URL (required; never emitted)")
	flags.StringVar(&config.Namespace, "namespace", "", "Redis namespace as <environment>:<zone>")
	flags.IntVar(&config.SamplePlayers, "sample-players", 0, "maximum Player auth-v1 records to sample")
	flags.IntVar(&config.SampleFarms, "sample-farms", 0, "maximum Farm auth-v1 records to sample")
	flags.StringVar(&config.Out, "out", "-", "JSON output path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if config.RedisURL == "" {
		_, _ = fmt.Fprintln(stderr, "actor-layout-inspect: --redis-url is required")
		return 1
	}
	options, err := redis.ParseURL(config.RedisURL)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "actor-layout-inspect: --redis-url is invalid")
		return 1
	}
	client := redis.NewClient(options)
	inspector, err := newInspector(goRedisReader{client: client}, inspectConfig{
		Namespace: config.Namespace, SamplePlayers: config.SamplePlayers, SampleFarms: config.SampleFarms,
	})
	if err != nil {
		_ = client.Close()
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_, _ = fmt.Fprintf(stderr, "actor-layout-inspect: Redis connection unavailable: %v\n", redactRedisError(err, config.RedisURL, options.Password))
		return 1
	}
	result := inspector.inspect(ctx)
	redactReportSecrets(&result, config.RedisURL, options.Password)
	if err := writeReport(config.Out, stdout, result); err != nil {
		_, _ = fmt.Fprintf(stderr, "actor-layout-inspect: write output: %v\n", err)
		return 1
	}
	return result.ExitCode
}

func writeReport(path string, stdout io.Writer, value report) error {
	w := stdout
	var closeOutput func() error
	if path != "-" {
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		w = file
		closeOutput = file.Close
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		if closeOutput != nil {
			_ = closeOutput()
		}
		return err
	}
	if closeOutput != nil {
		return closeOutput()
	}
	return nil
}

func redactRedisError(err error, secrets ...string) error {
	if err == nil {
		return err
	}
	return errors.New(redactSecrets(err.Error(), secrets...))
}

func redactReportSecrets(value *report, secrets ...string) {
	if value == nil {
		return
	}
	for index := range value.Failures {
		value.Failures[index].Error = redactSecrets(value.Failures[index].Error, secrets...)
	}
}

func redactSecrets(value string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "<redacted-secret>")
		}
	}
	return value
}
