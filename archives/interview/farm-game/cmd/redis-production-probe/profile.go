package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type probeProfile struct {
	ProfileVersion          string `json:"profile_version"`
	AuthorityLayoutVersion  string `json:"authority_layout_version"`
	WorkflowLayoutVersion   string `json:"workflow_layout_version"`
	WorkflowHashVersion     uint32 `json:"workflow_hash_version"`
	WorkflowBucketCandidate uint32 `json:"workflow_bucket_count_candidate"`
	WaitReplicas            int    `json:"wait_replicas"`
	WaitTimeoutMS           int    `json:"wait_timeout_ms"`
	OverallTimeoutMS        int    `json:"overall_timeout_ms"`
	ProbeKeyTTLMS           int    `json:"probe_key_ttl_ms"`
	LuaScriptSizeCandidates []int  `json:"lua_script_size_candidates"`
}

func loadProbeProfile(path string) (probeProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return probeProfile{}, err
	}
	var profile probeProfile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return probeProfile{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return probeProfile{}, errors.New("probe profile contains trailing JSON values")
	}
	if err := profile.validate(); err != nil {
		return probeProfile{}, err
	}
	return profile, nil
}

func (p probeProfile) validate() error {
	if p.ProfileVersion == "" || p.AuthorityLayoutVersion != "v1" || p.WorkflowLayoutVersion != "v1" ||
		p.WorkflowHashVersion != 1 || p.WorkflowBucketCandidate == 0 || p.WorkflowBucketCandidate&(p.WorkflowBucketCandidate-1) != 0 ||
		p.WaitReplicas < 0 || p.WaitTimeoutMS <= 0 || p.OverallTimeoutMS < p.WaitTimeoutMS ||
		p.ProbeKeyTTLMS < p.OverallTimeoutMS ||
		len(p.LuaScriptSizeCandidates) == 0 {
		return errors.New("invalid W31-A probe profile")
	}
	seen := make(map[int]struct{}, len(p.LuaScriptSizeCandidates))
	previous := 0
	for _, size := range p.LuaScriptSizeCandidates {
		if size < 64 || size > 1024*1024 || size <= previous {
			return fmt.Errorf("invalid Lua script size candidate %d", size)
		}
		if _, duplicate := seen[size]; duplicate {
			return fmt.Errorf("duplicate Lua script size candidate %d", size)
		}
		seen[size] = struct{}{}
		previous = size
	}
	return nil
}
