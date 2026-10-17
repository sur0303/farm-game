package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeStat(t *testing.T, path string, user, nice, system, idle, iowait, irq, softirq, steal int) {
	t.Helper()
	line := fmt.Sprintf("cpu  %d %d %d %d %d %d %d %d\ncpu0 1 1 1 1 1 1 1 1\n",
		user, nice, system, idle, iowait, irq, softirq, steal)
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
}

func millis(value int) time.Duration {
	return time.Duration(value) * time.Millisecond
}

func TestFirstHostSampleEstablishesTheBaselineRatherThanReportingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	writeStat(t, path, 100, 0, 100, 800, 0, 0, 0, 0)
	sampler := newHostSampler(path)
	facts, err := sampler.sample(0)
	if err != nil {
		t.Fatal(err)
	}
	if facts != (hostFacts{}) {
		t.Fatalf("the establishing sample invented a reading: %+v", facts)
	}
}

func TestStealAndBackgroundAreWhatTheHostDidMinusWhatTheRunDid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	writeStat(t, path, 100, 0, 100, 800, 0, 0, 0, 0)
	sampler := newHostSampler(path)
	if _, err := sampler.sample(0); err != nil {
		t.Fatal(err)
	}
	// One hundred ticks pass. Sixty of them are busy, ten of which were stolen. The
	// run's own cgroups account for a quarter of a second, which is twenty-five
	// ticks, so thirty-five ticks of busy time belong to something else.
	writeStat(t, path, 130, 0, 120, 840, 0, 0, 0, 10)
	facts, err := sampler.sample(.25)
	if err != nil {
		t.Fatal(err)
	}
	if facts.StealPercent != 10 {
		t.Fatalf("steal=%v want 10", facts.StealPercent)
	}
	if facts.BackgroundPercent != 35 {
		t.Fatalf("background=%v want 35", facts.BackgroundPercent)
	}
}

// A cgroup can look marginally busier than the host because the two are not read
// at the same instant. That is skew, and reporting it as negative background load
// would let a run claim the host was quieter than idle.
func TestBackgroundNeverGoesNegativeOnSamplingSkew(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	writeStat(t, path, 100, 0, 100, 800, 0, 0, 0, 0)
	sampler := newHostSampler(path)
	if _, err := sampler.sample(0); err != nil {
		t.Fatal(err)
	}
	writeStat(t, path, 110, 0, 100, 890, 0, 0, 0, 0)
	facts, err := sampler.sample(5)
	if err != nil {
		t.Fatal(err)
	}
	if facts.BackgroundPercent != 0 {
		t.Fatalf("background=%v want 0", facts.BackgroundPercent)
	}
}

func TestAStalledHostCounterIsAnErrorRatherThanAZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	writeStat(t, path, 100, 0, 100, 800, 0, 0, 0, 0)
	sampler := newHostSampler(path)
	if _, err := sampler.sample(0); err != nil {
		t.Fatal(err)
	}
	if _, err := sampler.sample(0); err == nil {
		t.Fatal("a host whose CPU counter never moved was reported as observed")
	}
}

func TestClockStepIsWallTimeMonotonicTimeDidNotAccountFor(t *testing.T) {
	for _, test := range []struct {
		name             string
		wallMS, monoMS   int
		expectedStepMSAt float64
	}{
		{"disciplined clock", 1000, 1000, 0},
		{"clock jumped forward", 1400, 1000, 400},
		{"clock jumped back", 600, 1000, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := clockStepMS(millis(test.wallMS), millis(test.monoMS))
			if got != test.expectedStepMSAt {
				t.Fatalf("step=%v want %v", got, test.expectedStepMSAt)
			}
		})
	}
}

func TestARestartIsSeenWhenTheProcessInsideACgroupIsReplaced(t *testing.T) {
	sampler := newHostSampler(filepath.Join(t.TempDir(), "stat"))
	if count := sampler.observeRestart("gate", 4001); count != 0 {
		t.Fatalf("first sighting counted as a restart: %d", count)
	}
	if count := sampler.observeRestart("gate", 4001); count != 0 {
		t.Fatalf("a steady process counted as a restart: %d", count)
	}
	if count := sampler.observeRestart("gate", 4002); count != 1 {
		t.Fatalf("a replaced process was not counted: %d", count)
	}
	if count := sampler.observeRestart("driver", 9001); count != 0 {
		t.Fatalf("another cgroup inherited the count: %d", count)
	}
}

func TestOOMKillsAreReadFromTheCgroupAndAbsenceIsAnError(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "memory.events"),
		[]byte("low 0\nhigh 0\nmax 12\noom 3\noom_kill 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	kills, err := readOOMKillTotal(directory)
	if err != nil || kills != 2 {
		t.Fatalf("kills=%v err=%v", kills, err)
	}
	empty := t.TempDir()
	if _, err := readOOMKillTotal(empty); err == nil {
		t.Fatal("a cgroup with no memory.events was reported as having no kills")
	}
}

// A Kubernetes pod slice holds no processes itself; they live in the per-container
// cgroups beneath it. A probe pointed at the slice must still find the process.
func TestTheMainProcessIsFoundBeneathAPodSlice(t *testing.T) {
	slice := t.TempDir()
	if err := os.WriteFile(filepath.Join(slice, "cgroup.procs"), []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	container := filepath.Join(slice, "cri-containerd-abc.scope")
	if err := os.MkdirAll(container, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(container, "cgroup.procs"), []byte("7321\n7322\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pid, err := mainPIDFromCgroup(slice)
	if err != nil || pid != 7321 {
		t.Fatalf("pid=%d err=%v", pid, err)
	}
}

func TestAnEmptyCgroupYieldsNoProcess(t *testing.T) {
	slice := t.TempDir()
	if err := os.WriteFile(filepath.Join(slice, "cgroup.procs"), []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := mainPIDFromCgroup(slice); err == nil {
		t.Fatal("an empty cgroup produced a process")
	}
}

func TestHostCPUTotalsRejectAFileWithoutTheAggregateLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	if err := os.WriteFile(path, []byte("cpu0 1 2 3 4 5 6 7 8\nintr 9\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readHostCPUTotals(path); err == nil {
		t.Fatal("per-CPU lines were mistaken for the aggregate")
	}
}
