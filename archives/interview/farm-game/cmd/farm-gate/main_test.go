package main

import (
	"testing"

	"farmgame/internal/authoritycomposition"
	"farmgame/internal/storage/redis/rediskey"
)

func setGateTupleEnvironment(t *testing.T) {
	t.Helper()
	for name, value := range map[string]string{
		rediskey.RedisNamespaceEnvironmentEnv: "prod",
		rediskey.RedisNamespaceZoneEnv:        "z1",
		rediskey.RedisPhysicalProfileEnv:      "redis-physical-v1",
		rediskey.RedisAuthorityLayoutEnv:      "v1",
		rediskey.RedisWorkflowLayoutEnv:       "v1",
		rediskey.RedisWorkflowHashEnv:         "1",
		rediskey.RedisWorkflowBucketCountEnv:  "256",
	} {
		t.Setenv(name, value)
	}
}

func TestFarmGateRemoteRequiresCompleteTuple(t *testing.T) {
	setGateTupleEnvironment(t)
	tuple, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch)
	if err != nil || tuple == nil {
		t.Fatalf("tuple=%v err=%v", tuple, err)
	}
	t.Setenv(rediskey.RedisWorkflowLayoutEnv, "")
	if _, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch); err == nil {
		t.Fatal("remote Gate accepted an incomplete tuple")
	}
	if tuple, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeStatic); err != nil || tuple != nil {
		t.Fatalf("static Gate tuple=%v err=%v", tuple, err)
	}
}
