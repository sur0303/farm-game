package main

// The facts in this file decide whether a capacity number means anything. If the
// host was stealing CPU, if the clock jumped, if the container restarted mid-run
// or was OOM-killed, then the run measured the machine's weather rather than the
// service. Nothing else in the system is positioned to see them: the service
// cannot report its own OOM kill, and the host cannot be asked over a loopback
// scrape. The collector already holds every cgroup handle the run declares, so it
// is the one process that can subtract what the run owns from what the host did.

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// linuxClockTicksPerSecond is the unit /proc/stat counts in. The kernel exports
// those fields in USER_HZ, which the Linux ABI fixes at 100 whatever the kernel's
// internal tick rate is.
const linuxClockTicksPerSecond = 100.0

// hostCPUTotals is /proc/stat's first line, in USER_HZ ticks.
type hostCPUTotals struct {
	Busy  float64
	Steal float64
	Total float64
}

type hostFacts struct {
	StealPercent      float64
	BackgroundPercent float64
	ClockStepMS       float64
}

type hostSampler struct {
	// statPath is the kernel's CPU accounting file. Tests point it at a fixture.
	statPath string

	mu sync.Mutex

	established bool
	previousCPU hostCPUTotals
	// previousWall carries no monotonic reading, so subtracting two of them
	// measures the wall clock. previousReading keeps its monotonic reading, so
	// subtracting two of those measures elapsed time the clock cannot move.
	previousWall      time.Time
	previousReading   time.Time
	observedMainPID   map[string]int
	observedStartTime map[string]float64
	restarts          map[string]int64
}

func newHostSampler(statPath string) *hostSampler {
	if statPath == "" {
		statPath = "/proc/stat"
	}
	return &hostSampler{
		statPath:          statPath,
		observedMainPID:   map[string]int{},
		observedStartTime: map[string]float64{},
		restarts:          map[string]int64{},
	}
}

// sample reports what the host did since the previous call, minus the CPU the run
// itself declares. runCPUSeconds is the CPU consumed in this interval by every
// cgroup the run owns: the measured service, the driver, the observer and the
// dependencies. Whatever busy time is left over belongs to something the run does
// not control, which is exactly what host_background_cpu_percent must show.
//
// The first call has nothing to subtract from and reports zeroes, the same way
// the existing cgroup probes report a zero first CPU percent. A run samples for at
// least sixty seconds, and the collector is scraped before the run starts, so the
// establishing sample is never one the verdict rests on.
func (sampler *hostSampler) sample(runCPUSeconds float64) (hostFacts, error) {
	totals, err := readHostCPUTotals(sampler.statPath)
	if err != nil {
		return hostFacts{}, err
	}
	reading := time.Now()
	wall := reading.Round(0)

	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	previousCPU, previousWall, previousReading := sampler.previousCPU, sampler.previousWall, sampler.previousReading
	established := sampler.established
	sampler.previousCPU, sampler.previousWall, sampler.previousReading, sampler.established = totals, wall, reading, true
	if !established {
		return hostFacts{}, nil
	}

	deltaTotal := totals.Total - previousCPU.Total
	if deltaTotal <= 0 {
		return hostFacts{}, errors.New("host cpu counter did not advance")
	}
	deltaBusy := totals.Busy - previousCPU.Busy
	background := deltaBusy - runCPUSeconds*linuxClockTicksPerSecond
	if background < 0 {
		// Sampling the host and the cgroups is not atomic, so a cgroup can appear to
		// have used marginally more than the host did. That is measurement skew, not
		// negative background load.
		background = 0
	}
	facts := hostFacts{
		StealPercent:      100 * (totals.Steal - previousCPU.Steal) / deltaTotal,
		BackgroundPercent: 100 * background / deltaTotal,
		ClockStepMS:       clockStepMS(wall.Sub(previousWall), reading.Sub(previousReading)),
	}
	return facts, nil
}

// observeRestart reports how many times the process inside a cgroup has been
// replaced. A container that restarts keeps its pod slice but starts a new main
// process, so a changed main PID is a restart the run did not ask for.
func (sampler *hostSampler) observeRestart(name string, mainPID int) int64 {
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	previous, seen := sampler.observedMainPID[name]
	if mainPID > 0 {
		sampler.observedMainPID[name] = mainPID
		if seen && previous > 0 && previous != mainPID {
			sampler.restarts[name]++
		}
	}
	return sampler.restarts[name]
}

// observeRestartAt reports the same count from the process's own start time. A
// container that restarts inside its pod slice reports a later start time, and
// unlike a pid this is visible to an observer that does not own the process.
// Start times are compared with a one-second tolerance because the value is
// derived from boot time and the clock, so a disciplined clock can nudge it
// without anything having restarted.
func (sampler *hostSampler) observeRestartAt(name string, startedAt float64) int64 {
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	previous, seen := sampler.observedStartTime[name]
	if startedAt > 0 {
		sampler.observedStartTime[name] = startedAt
		if seen && math.Abs(startedAt-previous) > 1 {
			sampler.restarts[name]++
		}
	}
	return sampler.restarts[name]
}

// clockStepMS is how far wall time moved that monotonic time did not. A machine
// whose clock is disciplined smoothly reports a fraction of a millisecond; a
// machine that stepped reports the step.
func clockStepMS(wall, mono time.Duration) float64 {
	difference := wall - mono
	if difference < 0 {
		difference = -difference
	}
	return float64(difference.Nanoseconds()) / 1e6
}

func readHostCPUTotals(path string) (hostCPUTotals, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return hostCPUTotals{}, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 9 || fields[0] != "cpu" {
			continue
		}
		values := make([]float64, 0, len(fields)-1)
		for _, field := range fields[1:] {
			value, parseErr := strconv.ParseFloat(field, 64)
			if parseErr != nil {
				return hostCPUTotals{}, errors.New("host cpu field")
			}
			values = append(values, value)
		}
		// user nice system idle iowait irq softirq steal [guest guest_nice]
		total := 0.0
		for _, value := range values {
			total += value
		}
		idle := values[3] + values[4]
		return hostCPUTotals{Busy: total - idle, Steal: values[7], Total: total}, nil
	}
	return hostCPUTotals{}, errors.New("host cpu totals absent")
}

// readOOMKillTotal reports how many times the kernel killed something in this
// cgroup. A single OOM kill invalidates a capacity number outright, so an
// unreadable memory.events is an error rather than a zero.
func readOOMKillTotal(cgroupPath string) (float64, error) {
	raw, err := os.ReadFile(filepath.Join(cgroupPath, "memory.events"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "oom_kill" {
			return strconv.ParseFloat(fields[1], 64)
		}
	}
	return 0, errors.New("oom_kill absent")
}

// mainPIDFromCgroup finds the first process in a cgroup, descending into children
// because a Kubernetes pod slice holds its processes in per-container cgroups
// rather than at the slice itself.
func mainPIDFromCgroup(cgroupPath string) (int, error) {
	raw, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if pid, convErr := strconv.Atoi(strings.TrimSpace(line)); convErr == nil && pid > 0 {
			return pid, nil
		}
	}
	entries, err := os.ReadDir(cgroupPath)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if pid, childErr := mainPIDFromCgroup(filepath.Join(cgroupPath, entry.Name())); childErr == nil {
			return pid, nil
		}
	}
	return 0, errors.New("cgroup holds no process")
}

// readFDLimit reports the soft open-file limit of a process, which is the ceiling
// the service will actually hit.
func readFDLimit(pid int) (float64, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "limits"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "Max open files") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Max open files"))
		if len(fields) == 0 {
			break
		}
		if fields[0] == "unlimited" {
			return 0, errors.New("fd limit unbounded")
		}
		limit, parseErr := strconv.ParseFloat(fields[0], 64)
		if parseErr != nil || limit <= 0 {
			return 0, errors.New("fd limit unreadable")
		}
		return limit, nil
	}
	return 0, errors.New("fd limit absent")
}

func countOpenFDs(pid int) (float64, error) {
	entries, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(pid), "fd"))
	if err != nil {
		return 0, err
	}
	return float64(len(entries)), nil
}
