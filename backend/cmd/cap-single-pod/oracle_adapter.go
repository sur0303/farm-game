package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type oracleGateMapping struct {
	Name      string            `json:"name"`
	Source    string            `json:"source"`
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels,omitempty"`
	Value     string            `json:"value"`
	Aggregate string            `json:"aggregate"`
	Mode      string            `json:"mode"`
}

type oracleAdapterConfig struct {
	SchemaVersion string              `json:"schema_version"`
	Listen        string              `json:"listen"`
	OraclePath    string              `json:"oracle_path"`
	Sources       []prometheusSource  `json:"sources"`
	Gates         []oracleGateMapping `json:"gates"`
}

type oracleAdapter struct {
	config       oracleAdapterConfig
	client       *http.Client
	mu           sync.Mutex
	baseline     map[string]float64
	binarySHA256 string
	configSHA256 string
}

func serveOracleCommand(arguments []string) {
	flags := flag.NewFlagSet("serve-oracle", flag.ExitOnError)
	configPath := flags.String("config", "", "absolute correctness oracle adapter config")
	_ = flags.Parse(arguments)
	if *configPath == "" || !filepath.IsAbs(*configPath) {
		fatal("absolute oracle config required")
	}
	config, err := loadOracleAdapterConfig(*configPath)
	if err != nil {
		fatal(err.Error())
	}
	binaryPath, err := os.Executable()
	if err != nil {
		fatal("oracle binary identity failed")
	}
	binarySHA, err := exactFileSHA256(binaryPath)
	if err != nil {
		fatal("oracle binary identity failed")
	}
	configSHA, err := exactFileSHA256(*configPath)
	if err != nil {
		fatal("oracle config identity failed")
	}
	adapter := &oracleAdapter{config: config, client: adapterHTTPClient(), baseline: make(map[string]float64), binarySHA256: binarySHA, configSHA256: configSHA}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := adapter.arm(ctx); err != nil {
		cancel()
		fatal("oracle baseline unavailable")
	}
	cancel()
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		fatal("oracle listen failed")
	}
	mux := http.NewServeMux()
	mux.HandleFunc(config.OraclePath, adapter.handleOracle)
	mux.HandleFunc("/health", adapter.handleHealth)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("oracle server failed")
	}
}

func loadOracleAdapterConfig(path string) (oracleAdapterConfig, error) {
	var config oracleAdapterConfig
	if err := loadJSONStrict(path, &config); err != nil {
		return config, err
	}
	if config.SchemaVersion != "cap-single-pod-l2.correctness-oracle-adapter.v0.1" {
		return config, errors.New("oracle adapter schema")
	}
	if err := validateLoopbackListen(config.Listen); err != nil {
		return config, err
	}
	if config.OraclePath == "" || config.OraclePath == "/health" || !strings.HasPrefix(config.OraclePath, "/") || strings.Contains(config.OraclePath, "?") {
		return config, errors.New("oracle path")
	}
	mappings := make([]metricMapping, 0, len(config.Gates))
	seen := map[string]bool{}
	for _, gate := range config.Gates {
		if gate.Name == "" || seen[gate.Name] || (gate.Mode != "COUNTER_DELTA" && gate.Mode != "GAUGE_CURRENT") {
			return config, fmt.Errorf("oracle gate invalid: %s", gate.Name)
		}
		seen[gate.Name] = true
		mappings = append(mappings, gate.metricMapping())
	}
	if len(config.Gates) == 0 {
		return config, errors.New("oracle gates empty")
	}
	if err := validateMappings(config.Sources, mappings); err != nil {
		return config, err
	}
	return config, nil
}

func (gate oracleGateMapping) metricMapping() metricMapping {
	return metricMapping{LogicalName: gate.Name, Source: gate.Source, Metric: gate.Metric, Labels: gate.Labels, Value: gate.Value, Aggregate: gate.Aggregate}
}

func (adapter *oracleAdapter) arm(ctx context.Context) error {
	values, err := adapter.scrape(ctx)
	if err != nil {
		return err
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	for _, gate := range adapter.config.Gates {
		if gate.Mode == "COUNTER_DELTA" {
			adapter.baseline[gate.Name] = values[gate.Name]
		}
	}
	return nil
}

func (adapter *oracleAdapter) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(adapterHealth{SchemaVersion: "cap-single-pod-l2.adapter-health.v0.1", Kind: "oracle", BinarySHA256: adapter.binarySHA256, ConfigSHA256: adapter.configSHA256})
}

func (adapter *oracleAdapter) handleOracle(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	result := adapter.observe(ctx)
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(result)
}

func (adapter *oracleAdapter) observe(ctx context.Context) oracleSnapshot {
	values, err := adapter.scrape(ctx)
	if err != nil {
		return oracleSnapshot{ObservationComplete: false, Gates: map[string]int64{}, ErrorCode: "ORACLE_SOURCE_FAILED"}
	}
	result := oracleSnapshot{ObservationComplete: true, Gates: make(map[string]int64, len(adapter.config.Gates))}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	for _, gate := range adapter.config.Gates {
		value := values[gate.Name]
		if gate.Mode == "COUNTER_DELTA" {
			baseline, found := adapter.baseline[gate.Name]
			if !found || value < baseline {
				return oracleSnapshot{ObservationComplete: false, Gates: result.Gates, ErrorCode: "ORACLE_COUNTER_RESET"}
			}
			value -= baseline
		}
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value > math.MaxInt64 {
			return oracleSnapshot{ObservationComplete: false, Gates: result.Gates, ErrorCode: "ORACLE_VALUE_INVALID"}
		}
		result.Gates[gate.Name] = int64(value)
	}
	return result
}

func (adapter *oracleAdapter) scrape(ctx context.Context) (map[string]float64, error) {
	snapshot, err := scrapePrometheusSources(ctx, adapter.client, adapter.config.Sources)
	if err != nil {
		return nil, err
	}
	values := make(map[string]float64, len(adapter.config.Gates))
	for _, gate := range adapter.config.Gates {
		value, mapErr := mappedMetricValue(snapshot, gate.metricMapping())
		if mapErr != nil {
			return nil, mapErr
		}
		values[gate.Name] = value
	}
	return values, nil
}
