package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/player/mail"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/mailmaintenance"

	"github.com/redis/go-redis/v9"
)

func buildMailMaintenance(redisClient redis.UniversalClient, profile rediskey.PhysicalProfile, owner, bootID string) (*mailmaintenance.Worker, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("mail maintenance requires PlayerMail Redis")
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("mail maintenance Redis keyspace tuple: %w", err)
	}
	bucketCount, err := mailMaintenanceInt("PLAYER_MAIL_BUCKET_COUNT", int(redisplayermail.DefaultBucketCount), 1)
	if err != nil {
		return nil, err
	}
	if bucketCount > mailmaintenance.MaxBucketCount {
		return nil, fmt.Errorf("mail maintenance PLAYER_MAIL_BUCKET_COUNT exceeds %d", mailmaintenance.MaxBucketCount)
	}
	workerIndex, err := mailMaintenanceInt("MAIL_MAINTENANCE_WORKER_INDEX", 0, 0)
	if err != nil {
		return nil, err
	}
	workerCount, err := mailMaintenanceInt("MAIL_MAINTENANCE_WORKER_COUNT", 1, 1)
	if err != nil {
		return nil, err
	}
	if uint64(workerIndex) > uint64(^uint32(0)) || uint64(workerCount) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("mail maintenance worker index/count exceed uint32")
	}
	bucketsPerRound, err := mailMaintenanceInt("MAIL_MAINTENANCE_BUCKETS_PER_ROUND", mailmaintenance.DefaultBucketsPerRound, 1)
	if err != nil {
		return nil, err
	}
	batchLimit, err := mailMaintenanceInt("MAIL_MAINTENANCE_BATCH_LIMIT", mailmaintenance.DefaultBatchLimit, 1)
	if err != nil {
		return nil, err
	}
	maxConcurrent, err := mailMaintenanceInt("MAIL_MAINTENANCE_MAX_CONCURRENT", mailmaintenance.DefaultMaxConcurrent, 1)
	if err != nil {
		return nil, err
	}
	leaseTTL, err := mailMaintenanceEnvDuration("MAIL_MAINTENANCE_SCAN_LEASE", mailmaintenance.DefaultLeaseTTL)
	if err != nil {
		return nil, err
	}
	safetyMargin, err := mailMaintenanceEnvDuration("MAIL_MAINTENANCE_SCAN_LEASE_SAFETY_MARGIN", mailmaintenance.DefaultSafetyMargin)
	if err != nil {
		return nil, err
	}
	operationTimeout, err := mailMaintenanceEnvDuration("MAIL_MAINTENANCE_OPERATION_TIMEOUT", mailmaintenance.DefaultOperationTimeout)
	if err != nil {
		return nil, err
	}
	roundTimeout, err := mailMaintenanceEnvDuration("MAIL_MAINTENANCE_ROUND_TIMEOUT", mailmaintenance.DefaultRoundTimeout)
	if err != nil {
		return nil, err
	}

	mailStore, err := redisplayermail.NewStore(redisClient, redisplayermail.Profile{
		Namespace: profile.Namespace, BucketCount: uint32(bucketCount),
	}, mail.LimitsV1())
	if err != nil {
		return nil, fmt.Errorf("mail maintenance repository: %w", err)
	}
	loadCtx, loadCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = mailStore.EnsureLoaded(loadCtx)
	loadCancel()
	if err != nil {
		return nil, fmt.Errorf("mail maintenance projection scripts: %w", err)
	}
	return mailmaintenance.NewWorker(mailmaintenance.Config{
		Repository: mailStore, OwnerToken: fmt.Sprintf("%s:%s:%d/%d", owner, bootID, workerIndex, workerCount),
		BucketCount: uint32(bucketCount), WorkerIndex: uint32(workerIndex), WorkerCount: uint32(workerCount),
		BucketsPerRound: bucketsPerRound, BatchLimit: batchLimit, MaxConcurrent: maxConcurrent,
		LeaseTTL: leaseTTL, SafetyMargin: safetyMargin, OperationTimeout: operationTimeout,
		RoundTimeout: roundTimeout,
	})
}

func mailMaintenanceInt(key string, fallback, minimum int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < minimum {
		return 0, fmt.Errorf("mail maintenance %s must be an integer >= %d", key, minimum)
	}
	return value, nil
}

func mailMaintenanceEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("mail maintenance %s must be a positive duration", key)
	}
	return value, nil
}
