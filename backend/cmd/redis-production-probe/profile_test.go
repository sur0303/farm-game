package main

import "testing"

func TestProbeProfileValidation(t *testing.T) {
	valid := probeProfile{
		ProfileVersion:          "redis-physical-probe-v1",
		AuthorityLayoutVersion:  "v1",
		WorkflowLayoutVersion:   "v1",
		WorkflowHashVersion:     1,
		WorkflowBucketCandidate: 256,
		WaitReplicas:            1,
		WaitTimeoutMS:           1000,
		OverallTimeoutMS:        30000,
		ProbeKeyTTLMS:           120000,
		LuaScriptSizeCandidates: []int{1024, 8192, 32768},
	}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.WorkflowBucketCandidate = 100
	if invalid.validate() == nil {
		t.Fatal("non-power-of-two workflow bucket count accepted")
	}
	invalid = valid
	invalid.LuaScriptSizeCandidates = []int{8192, 1024}
	if invalid.validate() == nil {
		t.Fatal("unordered Lua candidates accepted")
	}
}

func TestLuaScriptOfSize(t *testing.T) {
	for _, size := range []int{64, 1024, 8192} {
		script := luaScriptOfSize(size)
		if len(script) != size {
			t.Fatalf("size=%d got=%d", size, len(script))
		}
	}
}
