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
	"strconv"
	"strings"
	"sync"
	"time"
)

type cgroupProbeConfig struct {
	Name                string  `json:"name"`
	Path                string  `json:"path"`
	ExpectedCPUQuota    float64 `json:"expected_cpu_quota"`
	ExpectedMemoryBytes int64   `json:"expected_memory_bytes"`
	PIDFile             string  `json:"pid_file,omitempty"`
}

type identityFileConfig struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type collectorAdapterConfig struct {
	SchemaVersion string `json:"schema_version"`
	Listen        string `json:"listen"`
	SnapshotPath  string `json:"snapshot_path"`
	// HostStatPath is the kernel CPU accounting file, /proc/stat unless a test
	// points it elsewhere.
	HostStatPath        string               `json:"host_stat_path,omitempty"`
	AllowedCgroupRoot   string               `json:"allowed_cgroup_root"`
	Sources             []prometheusSource   `json:"sources"`
	Mappings            []metricMapping      `json:"mappings"`
	MeasuredService     cgroupProbeConfig    `json:"measured_service"`
	Driver              cgroupProbeConfig    `json:"driver"`
	Observer            cgroupProbeConfig    `json:"observer"`
	Dependencies        []cgroupProbeConfig  `json:"dependencies"`
	IdentityFiles       []identityFileConfig `json:"identity_files"`
	PrimaryQueueMetric  string               `json:"primary_queue_metric"`
	PrimaryBytesMetric  string               `json:"primary_queue_bytes_metric"`
	PrimaryAgeMetric    string               `json:"primary_queue_age_metric"`
	RedisPoolWaitMetric string               `json:"redis_pool_wait_metric"`
}

// The logical names a run maps the measured service's own process gauges onto.
// Every Go service in this repository publishes these three through the client
// library's default collectors, so a run gets descriptor pressure and restart
// evidence from the same scrape it already takes, without needing to read another
// user's procfs.
const (
	serviceOpenFDsMetric   = "service_process_open_fds"
	serviceMaxFDsMetric    = "service_process_max_fds"
	serviceStartTimeMetric = "service_process_start_time_seconds"
)

type cgroupReading struct {
	CPUPercent float64
	// CPUSeconds is the CPU this cgroup consumed since the previous reading. The
	// host sampler subtracts the run's own consumption from the host's, so it needs
	// the raw seconds rather than the percentage of a quota.
	CPUSeconds  float64
	MemoryBytes int64
	MemoryPct   float64
	FDOpen      int64
	FDPercent   float64
	// FDKnown records whether this process's descriptors could be counted at all.
	// Counting them means reading /proc/<pid>/fd, which the kernel only shows to
	// the process's own user, so an observer running beside a container it does
	// not own gets nothing. That is a missing reading, not a reading of zero, and
	// it must not take the cgroup's CPU and memory down with it.
	FDKnown    bool
	OOMKills   float64
	MainPID    int
	LimitDrift bool
}

type cpuPrevious struct {
	UsageUsec int64
	At        time.Time
}

type collectorAdapter struct {
	config       collectorAdapterConfig
	client       *http.Client
	host         *hostSampler
	mu           sync.Mutex
	previous     map[string]cpuPrevious
	binarySHA256 string
	configSHA256 string
}

func serveCollectorCommand(arguments []string) {
	flags := flag.NewFlagSet("serve-collector", flag.ExitOnError)
	configPath := flags.String("config", "", "absolute collector adapter config")
	_ = flags.Parse(arguments)
	if *configPath == "" || !filepath.IsAbs(*configPath) {
		fatal("absolute collector config required")
	}
	config, err := loadCollectorAdapterConfig(*configPath)
	if err != nil {
		fatal(err.Error())
	}
	binaryPath, err := os.Executable()
	if err != nil {
		fatal("collector binary identity failed")
	}
	binarySHA, err := exactFileSHA256(binaryPath)
	if err != nil {
		fatal("collector binary identity failed")
	}
	configSHA, err := exactFileSHA256(*configPath)
	if err != nil {
		fatal("collector config identity failed")
	}
	adapter := &collectorAdapter{config: config, client: adapterHTTPClient(), host: newHostSampler(config.HostStatPath), previous: make(map[string]cpuPrevious), binarySHA256: binarySHA, configSHA256: configSHA}
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		fatal("collector listen failed")
	}
	mux := http.NewServeMux()
	mux.HandleFunc(config.SnapshotPath, adapter.handleSnapshot)
	mux.HandleFunc("/health", adapter.handleHealth)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("collector server failed")
	}
}

func loadCollectorAdapterConfig(path string) (collectorAdapterConfig, error) {
	var config collectorAdapterConfig
	if err := loadJSONStrict(path, &config); err != nil {
		return config, err
	}
	if config.SchemaVersion != "cap-single-pod-l2.collector-adapter.v0.1" {
		return config, errors.New("collector adapter schema")
	}
	if err := validateLoopbackListen(config.Listen); err != nil {
		return config, err
	}
	if config.SnapshotPath == "" || config.SnapshotPath == "/health" || !strings.HasPrefix(config.SnapshotPath, "/") || strings.Contains(config.SnapshotPath, "?") {
		return config, errors.New("collector snapshot path")
	}
	if !filepath.IsAbs(config.AllowedCgroupRoot) {
		return config, errors.New("collector cgroup root")
	}
	if err := validateMappings(config.Sources, config.Mappings); err != nil {
		return config, err
	}
	for _, probe := range append([]cgroupProbeConfig{config.MeasuredService, config.Driver, config.Observer}, config.Dependencies...) {
		if err := validateCgroupProbe(config.AllowedCgroupRoot, probe); err != nil {
			return config, err
		}
	}
	for _, identity := range config.IdentityFiles {
		if !filepath.IsAbs(identity.Path) || !sha256Pattern.MatchString(identity.SHA256) {
			return config, errors.New("collector identity file")
		}
	}
	return config, nil
}

func validateLoopbackListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("adapter listen address")
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("adapter must listen on loopback")
	}
	return nil
}

func validateCgroupProbe(root string, probe cgroupProbeConfig) error {
	if probe.Name == "" || !filepath.IsAbs(probe.Path) || probe.ExpectedCPUQuota <= 0 || probe.ExpectedMemoryBytes <= 0 {
		return fmt.Errorf("cgroup probe invalid: %s", probe.Name)
	}
	root = filepath.Clean(root)
	path := filepath.Clean(probe.Path)
	if path != root && !strings.HasPrefix(path, root+string(os.PathSeparator)) {
		return fmt.Errorf("cgroup probe escapes root: %s", probe.Name)
	}
	if probe.PIDFile != "" && !filepath.IsAbs(probe.PIDFile) {
		return fmt.Errorf("pid file invalid: %s", probe.Name)
	}
	return nil
}

func (adapter *collectorAdapter) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(adapterHealth{SchemaVersion: "cap-single-pod-l2.adapter-health.v0.1", Kind: "collector", BinarySHA256: adapter.binarySHA256, ConfigSHA256: adapter.configSHA256})
}

func (adapter *collectorAdapter) handleSnapshot(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	snapshot := adapter.collect(ctx)
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(snapshot)
}

func (adapter *collectorAdapter) collect(ctx context.Context) resourceSnapshot {
	snapshot := resourceSnapshot{MetricValues: map[string]float64{}}
	prometheusValues, promErr := adapter.collectPrometheus(ctx)
	for name, value := range prometheusValues {
		snapshot.MetricValues[name] = value
	}
	service, serviceErr := adapter.readCgroup(adapter.config.MeasuredService)
	driver, driverErr := adapter.readCgroup(adapter.config.Driver)
	observer, observerErr := adapter.readCgroup(adapter.config.Observer)
	dependencies, dependencyErr := adapter.readDependencies()
	fdUnavailable, restartUnavailable := false, false
	if serviceErr == nil {
		snapshot.ServiceCPUPercent = service.CPUPercent
		snapshot.ServiceRSSBytes = service.MemoryBytes
		snapshot.ServiceMemoryPercent = service.MemoryPct
		snapshot.MetricValues["service_cpu_percent"] = service.CPUPercent
		snapshot.MetricValues["service_rss_bytes"] = float64(service.MemoryBytes)
		snapshot.MetricValues["service_memory_percent_of_limit"] = service.MemoryPct
		snapshot.MetricValues["measured_service_memory_percent_of_limit"] = service.MemoryPct
		snapshot.MetricValues["measured_service_oom_kill_total"] = service.OOMKills

		// The service counts its own descriptors and publishes them, which is both
		// cheaper and readable across users, so it is preferred over procfs. Procfs
		// remains the fallback for a bare process the run owns.
		open, openFound := snapshot.MetricValues[serviceOpenFDsMetric]
		limit, limitFound := snapshot.MetricValues[serviceMaxFDsMetric]
		switch {
		case openFound && limitFound && limit > 0:
			snapshot.ServiceFDOpen = int64(open)
			snapshot.MetricValues["service_fd_open"] = open
			snapshot.MetricValues["measured_service_fd_percent_of_limit"] = 100 * open / limit
		case service.FDKnown:
			snapshot.ServiceFDOpen = service.FDOpen
			snapshot.MetricValues["service_fd_open"] = float64(service.FDOpen)
			snapshot.MetricValues["measured_service_fd_percent_of_limit"] = service.FDPercent
		default:
			fdUnavailable = true
		}

		// A restarted process reports a new start time, which survives a pid the
		// observer cannot see. A changed main pid says the same thing when the run
		// owns the process directly.
		if startedAt, found := snapshot.MetricValues[serviceStartTimeMetric]; found {
			snapshot.MetricValues["unexpected_container_restart_total"] =
				float64(adapter.host.observeRestartAt(adapter.config.MeasuredService.Name, startedAt))
		} else if service.MainPID > 0 {
			snapshot.MetricValues["unexpected_container_restart_total"] =
				float64(adapter.host.observeRestart(adapter.config.MeasuredService.Name, service.MainPID))
		} else {
			restartUnavailable = true
		}
	}
	if driverErr == nil {
		snapshot.DriverCPUPercent = driver.CPUPercent
		snapshot.DriverRSSBytes = driver.MemoryBytes
		snapshot.DriverFDOpen = driver.FDOpen
		snapshot.MetricValues["driver_cpu_percent"] = driver.CPUPercent
		snapshot.MetricValues["driver_rss_bytes"] = float64(driver.MemoryBytes)
		snapshot.MetricValues["driver_fd_open"] = float64(driver.FDOpen)
	}
	if observerErr == nil {
		snapshot.MetricValues["observer_cpu_percent"] = observer.CPUPercent
		snapshot.MetricValues["observer_rss_bytes"] = float64(observer.MemoryBytes)
	}
	if dependencyErr == nil {
		snapshot.DependencyCPUPercent = dependencies.CPUPercent
		snapshot.DependencyMemoryPercent = dependencies.MemoryPct
	}
	if service.LimitDrift || driver.LimitDrift || observer.LimitDrift || dependencies.LimitDrift {
		snapshot.MetricValues["cgroup_limit_drift"] = 1
	} else {
		snapshot.MetricValues["cgroup_limit_drift"] = 0
	}
	identityDrift := 0.0
	for _, identity := range adapter.config.IdentityFiles {
		digest, err := exactFileSHA256(identity.Path)
		if err != nil || digest != identity.SHA256 {
			identityDrift = 1
			break
		}
	}
	snapshot.MetricValues["identity_drift"] = identityDrift
	// Host facts are only meaningful once every cgroup the run owns has been read,
	// because the background figure is what the host did minus what the run did.
	hostErr := errors.New("run cgroups incomplete")
	if serviceErr == nil && driverErr == nil && observerErr == nil && dependencyErr == nil {
		var facts hostFacts
		facts, hostErr = adapter.host.sample(service.CPUSeconds + driver.CPUSeconds + observer.CPUSeconds + dependencies.CPUSeconds)
		if hostErr == nil {
			snapshot.MetricValues["host_cpu_steal_percent"] = facts.StealPercent
			snapshot.MetricValues["host_background_cpu_percent"] = facts.BackgroundPercent
			snapshot.MetricValues["host_clock_step_ms"] = facts.ClockStepMS
		}
	}
	snapshot.PrimaryQueueItems = snapshot.MetricValues[adapter.config.PrimaryQueueMetric]
	snapshot.PrimaryQueueBytes = snapshot.MetricValues[adapter.config.PrimaryBytesMetric]
	snapshot.PrimaryQueueOldestSeconds = snapshot.MetricValues[adapter.config.PrimaryAgeMetric]
	snapshot.RedisPoolWaitSeconds = snapshot.MetricValues[adapter.config.RedisPoolWaitMetric]
	if promErr != nil || serviceErr != nil || driverErr != nil || observerErr != nil || dependencyErr != nil ||
		hostErr != nil || fdUnavailable || restartUnavailable {
		snapshot.ScrapeErrorCode = "COLLECTOR_SOURCE_FAILED"
		missing := []string{}
		if fdUnavailable {
			missing = append(missing, "service_fd_open", "measured_service_fd_percent_of_limit")
		}
		if restartUnavailable {
			missing = append(missing, "unexpected_container_restart_total")
		}
		if promErr != nil {
			for _, mapping := range adapter.config.Mappings {
				missing = append(missing, mapping.LogicalName)
			}
		}
		if hostErr != nil {
			missing = append(missing, "host_cpu_steal_percent", "host_background_cpu_percent", "host_clock_step_ms")
		}
		if serviceErr != nil {
			missing = append(missing, "service_cpu_percent", "service_rss_bytes", "service_fd_open",
				"measured_service_fd_percent_of_limit", "measured_service_oom_kill_total", "unexpected_container_restart_total")
		}
		if driverErr != nil {
			missing = append(missing, "driver_cpu_percent", "driver_rss_bytes", "driver_fd_open")
		}
		if observerErr != nil {
			missing = append(missing, "observer_cpu_percent", "observer_rss_bytes")
		}
		snapshot.MissingMetrics = uniqueSorted(missing)
	}
	return snapshot
}

func (adapter *collectorAdapter) collectPrometheus(ctx context.Context) (map[string]float64, error) {
	snapshot, err := scrapePrometheusSources(ctx, adapter.client, adapter.config.Sources)
	if err != nil {
		return nil, err
	}
	values := map[string]float64{}
	failed := false
	for _, mapping := range adapter.config.Mappings {
		value, mapErr := mappedMetricValue(snapshot, mapping)
		if mapErr != nil {
			failed = true
			continue
		}
		values[mapping.LogicalName] = value
	}
	if failed {
		return values, errors.New("one or more metric mappings unavailable")
	}
	return values, nil
}

func (adapter *collectorAdapter) readDependencies() (cgroupReading, error) {
	if len(adapter.config.Dependencies) == 0 {
		return cgroupReading{}, nil
	}
	result := cgroupReading{}
	for _, probe := range adapter.config.Dependencies {
		reading, err := adapter.readCgroup(probe)
		if err != nil {
			return cgroupReading{}, err
		}
		result.CPUPercent += reading.CPUPercent
		if reading.MemoryPct > result.MemoryPct {
			result.MemoryPct = reading.MemoryPct
		}
		result.LimitDrift = result.LimitDrift || reading.LimitDrift
	}
	return result, nil
}

func (adapter *collectorAdapter) readCgroup(probe cgroupProbeConfig) (cgroupReading, error) {
	usage, err := readCPUUsageUsec(filepath.Join(probe.Path, "cpu.stat"))
	if err != nil {
		return cgroupReading{}, err
	}
	memory, err := readIntFile(filepath.Join(probe.Path, "memory.current"))
	if err != nil {
		return cgroupReading{}, err
	}
	cpuLimit, err := readCPUQuota(filepath.Join(probe.Path, "cpu.max"))
	if err != nil {
		return cgroupReading{}, err
	}
	memoryLimit, err := readIntFile(filepath.Join(probe.Path, "memory.max"))
	if err != nil {
		return cgroupReading{}, err
	}
	oomKills, err := readOOMKillTotal(probe.Path)
	if err != nil {
		return cgroupReading{}, err
	}
	now := time.Now()
	cpuPercent, cpuSeconds := 0.0, 0.0
	adapter.mu.Lock()
	previous, found := adapter.previous[probe.Name]
	adapter.previous[probe.Name] = cpuPrevious{UsageUsec: usage, At: now}
	adapter.mu.Unlock()
	if found && usage >= previous.UsageUsec && now.After(previous.At) {
		cpuSeconds = float64(usage-previous.UsageUsec) / 1e6
		cpuPercent = 100 * cpuSeconds / (now.Sub(previous.At).Seconds() * probe.ExpectedCPUQuota)
	}
	// A pid file is how a bare process names itself. A container has none, so the
	// process is found through the cgroup that the run already declares. Neither
	// the pid nor its descriptors are required: cross-user procfs is unreadable in
	// a real cluster, and the signals that need them are also published by the
	// service itself, so the caller decides which source it got and reports the
	// signal missing when it got neither.
	pid := 0
	if probe.PIDFile != "" {
		pid, _ = readPIDFile(probe.PIDFile)
	} else {
		pid, _ = mainPIDFromCgroup(probe.Path)
	}
	fdOpen, fdPercent, fdKnown := 0.0, 0.0, false
	if pid > 0 {
		open, openErr := countOpenFDs(pid)
		limit, limitErr := readFDLimit(pid)
		if openErr == nil && limitErr == nil && limit > 0 {
			fdOpen, fdPercent, fdKnown = open, 100*open/limit, true
		}
	}
	return cgroupReading{
		CPUPercent: cpuPercent, CPUSeconds: cpuSeconds,
		MemoryBytes: memory, MemoryPct: 100 * float64(memory) / float64(probe.ExpectedMemoryBytes),
		FDOpen: int64(fdOpen), FDPercent: fdPercent, FDKnown: fdKnown,
		OOMKills: oomKills, MainPID: pid,
		LimitDrift: math.Abs(cpuLimit-probe.ExpectedCPUQuota) > 1e-9 || memoryLimit != probe.ExpectedMemoryBytes,
	}, nil
}

func readCPUUsageUsec(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			return strconv.ParseInt(fields[1], 10, 64)
		}
	}
	return 0, errors.New("usage_usec absent")
}
func readCPUQuota(path string) (float64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 2 || fields[0] == "max" {
		return 0, errors.New("finite cpu.max required")
	}
	quota, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, err
	}
	period, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || period <= 0 {
		return 0, errors.New("cpu.max period")
	}
	return quota / period, nil
}
func readIntFile(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
}
func readPIDFile(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0, errors.New("pid file")
	}
	return pid, nil
}
