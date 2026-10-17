package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"

	"github.com/redis/go-redis/v9"
)

const (
	authorityLayoutVersion = "v1"
	scanCountHint          = int64(128)
	maxOrderKeysPerBucket  = 10_000
)

var expectedModelFields = map[string][]string{
	"player": {"record", "public_profile", "profile", "economy", "inventory", "progression", "task", "pet", "compendium", "mail_claim_state"},
	"farm":   {"record", "farm_meta", "plots", "guard", "activity_summary", "opportunity_header", "opportunity_facts"},
}

type redisReader interface {
	Scan(context.Context, uint64, string, int64) ([]string, uint64, error)
	HGetAll(context.Context, string) (map[string]string, error)
}

type goRedisReader struct{ client redis.UniversalClient }

func (r goRedisReader) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return r.client.Scan(ctx, cursor, match, count).Result()
}

func (r goRedisReader) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return r.client.HGetAll(ctx, key).Result()
}

type inspectConfig struct {
	Namespace     string
	SamplePlayers int
	SampleFarms   int
	Now           func() time.Time
}

type report struct {
	SchemaVersion string                  `json:"schema_version"`
	GeneratedAt   string                  `json:"generated_at"`
	Status        string                  `json:"status"`
	ExitCode      int                     `json:"exit_code"`
	ExitSemantics string                  `json:"exit_semantics"`
	Source        sourceReport            `json:"source"`
	Domains       map[string]domainReport `json:"domains"`
	Failures      []sampleFailure         `json:"failures"`
}

type sourceReport struct {
	Kind                   string `json:"kind"`
	Namespace              string `json:"namespace"`
	AuthorityLayoutVersion string `json:"authority_layout_version"`
	Selection              string `json:"selection"`
	Representative         bool   `json:"representative"`
	RedisURLRedacted       bool   `json:"redis_url_redacted"`
}

type domainReport struct {
	Requested                      int                         `json:"requested"`
	SelectedKeys                   int                         `json:"selected_keys"`
	Sampled                        int                         `json:"sampled"`
	FailureCount                   int                         `json:"failure_count"`
	RedisActorHashLogicalBytes     distribution                `json:"redis_actor_hash_logical_bytes"`
	ActorRecordSerializedBytes     distribution                `json:"actor_record_serialized_bytes"`
	ModelStateBytes                distribution                `json:"model_state_bytes"`
	ModelFields                    map[string]modelFieldReport `json:"model_fields"`
	ReceiptBytesPerActor           distribution                `json:"receipt_bytes_per_actor"`
	ReceiptBytesPerItem            distribution                `json:"receipt_bytes_per_item"`
	ResolutionReceiptBytesPerActor distribution                `json:"resolution_receipt_bytes_per_actor"`
	OrderBytesPerActor             distribution                `json:"order_bytes_per_actor"`
	OrderBytesPerItem              distribution                `json:"order_bytes_per_item"`
	OrderPayloadBytesPerItem       distribution                `json:"order_payload_bytes_per_item"`
	ReceiptItems                   int                         `json:"receipt_items"`
	ResolutionReceiptItems         int                         `json:"resolution_receipt_items"`
	OrderItems                     int                         `json:"order_items"`
	ModelLayouts                   map[string]int              `json:"model_layouts"`
}

type modelFieldReport struct {
	PresentSamples int          `json:"present_samples"`
	MissingSamples int          `json:"missing_samples"`
	LogicalBytes   distribution `json:"logical_bytes"`
}

type distribution struct {
	Count int `json:"count"`
	P50   int `json:"p50"`
	P95   int `json:"p95"`
	P99   int `json:"p99"`
	Max   int `json:"max"`
}

type sampleFailure struct {
	Domain  string `json:"domain"`
	ActorID uint64 `json:"actor_id,omitempty"`
	Key     string `json:"key,omitempty"`
	Stage   string `json:"stage"`
	Error   string `json:"error"`
}

type actorSample struct {
	domain                 string
	actorID                uint64
	bucketPrefix           string
	redisActorHashBytes    int
	actorRecordBytes       int
	modelStateBytes        int
	modelFields            map[string]int
	modelLayout            string
	receiptBytes           int
	receiptItemBytes       []int
	resolutionReceiptBytes int
	resolutionReceiptItems int
	orderBytes             int
	orderItemBytes         []int
	orderPayloadBytes      []int
}

type inspector struct {
	reader redisReader
	config inspectConfig
}

func newInspector(reader redisReader, config inspectConfig) (*inspector, error) {
	if reader == nil {
		return nil, errors.New("actor-layout-inspect: Redis reader is required")
	}
	if _, _, err := parseNamespace(config.Namespace); err != nil {
		return nil, err
	}
	if config.SamplePlayers < 0 || config.SampleFarms < 0 || config.SamplePlayers+config.SampleFarms == 0 {
		return nil, errors.New("actor-layout-inspect: at least one positive sample count is required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &inspector{reader: reader, config: config}, nil
}

func (i *inspector) inspect(ctx context.Context) report {
	report := report{
		SchemaVersion: "actor-layout-v1-v2-candidate/v2",
		GeneratedAt:   i.config.Now().UTC().Format(time.RFC3339Nano),
		Status:        "complete",
		ExitCode:      0,
		ExitSemantics: "0=complete requested sample; 2=partial (output written with itemized failures); 1=fatal configuration/connection/output error",
		Source: sourceReport{
			Kind: "redis_live_sample", Namespace: i.config.Namespace,
			AuthorityLayoutVersion: authorityLayoutVersion,
			Selection:              "bounded Redis SCAN discovery followed by HGETALL of selected v1 Actor/SourceOrder hashes",
			Representative:         false, RedisURLRedacted: true,
		},
		Domains: map[string]domainReport{},
	}

	samplesByDomain := make(map[string][]*actorSample, 2)
	for _, request := range []struct {
		domain string
		count  int
	}{{"player", i.config.SamplePlayers}, {"farm", i.config.SampleFarms}} {
		samples, selected, failures := i.inspectActors(ctx, request.domain, request.count)
		samplesByDomain[request.domain] = samples
		report.Failures = append(report.Failures, failures...)
		report.Domains[request.domain] = summarizeDomain(request.domain, request.count, selected, samples, len(failures))
	}

	orderFailures := i.inspectOrders(ctx, samplesByDomain)
	report.Failures = append(report.Failures, orderFailures...)
	// Orders are discovered after the Actor summaries, so rebuild the immutable
	// output summaries once all read-only sampling is complete.
	for domain, samples := range samplesByDomain {
		current := report.Domains[domain]
		current = summarizeDomain(domain, current.Requested, current.SelectedKeys, samples, current.FailureCount)
		report.Domains[domain] = current
	}
	for _, failure := range orderFailures {
		domain := report.Domains[failure.Domain]
		domain.FailureCount++
		report.Domains[failure.Domain] = domain
	}

	if len(report.Failures) > 0 || report.Domains["player"].Sampled < i.config.SamplePlayers || report.Domains["farm"].Sampled < i.config.SampleFarms {
		report.Status = "partial"
		report.ExitCode = 2
	}
	return report
}

func (i *inspector) inspectActors(ctx context.Context, domain string, requested int) ([]*actorSample, int, []sampleFailure) {
	if requested == 0 {
		return nil, 0, nil
	}
	pattern := actorKeyPattern(i.config.Namespace, domain)
	keys, _, err := i.scanUntil(ctx, pattern, requested)
	if err != nil {
		return nil, len(keys), []sampleFailure{{Domain: domain, Stage: "actor_scan", Error: err.Error()}}
	}
	sort.Strings(keys)
	failures := make([]sampleFailure, 0)
	if len(keys) < requested {
		failures = append(failures, sampleFailure{Domain: domain, Stage: "actor_discovery", Error: fmt.Sprintf("requested %d records, discovered %d", requested, len(keys))})
	}
	samples := make([]*actorSample, 0, len(keys))
	for _, key := range keys {
		actorID, bucketPrefix, parseErr := parseActorKey(key)
		if parseErr != nil {
			failures = append(failures, sampleFailure{Domain: domain, Key: key, Stage: "actor_key", Error: parseErr.Error()})
			continue
		}
		fields, readErr := i.reader.HGetAll(ctx, key)
		if readErr != nil {
			failures = append(failures, sampleFailure{Domain: domain, ActorID: actorID, Key: key, Stage: "actor_hash", Error: readErr.Error()})
			continue
		}
		sample, analyzeErr := analyzeActor(domain, actorID, bucketPrefix, fields)
		if analyzeErr != nil {
			failures = append(failures, sampleFailure{Domain: domain, ActorID: actorID, Key: key, Stage: "actor_decode", Error: analyzeErr.Error()})
			continue
		}
		samples = append(samples, sample)
	}
	return samples, len(keys), failures
}

func (i *inspector) inspectOrders(ctx context.Context, samplesByDomain map[string][]*actorSample) []sampleFailure {
	type bucketActors struct {
		domain  string
		byActor map[uint64]*actorSample
	}
	buckets := make(map[string]bucketActors)
	for domain, samples := range samplesByDomain {
		for _, sample := range samples {
			bucket := buckets[sample.bucketPrefix]
			bucket.domain = domain
			if bucket.byActor == nil {
				bucket.byActor = make(map[uint64]*actorSample)
			}
			bucket.byActor[sample.actorID] = sample
			buckets[sample.bucketPrefix] = bucket
		}
	}

	failures := make([]sampleFailure, 0)
	prefixes := make([]string, 0, len(buckets))
	for prefix := range buckets {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	for _, prefix := range prefixes {
		bucket := buckets[prefix]
		keys, complete, err := i.scanUntil(ctx, prefix+":order:*", maxOrderKeysPerBucket)
		if err != nil {
			failures = append(failures, sampleFailure{Domain: bucket.domain, Key: prefix, Stage: "order_scan", Error: err.Error()})
			continue
		}
		if !complete {
			failures = append(failures, sampleFailure{Domain: bucket.domain, Key: prefix, Stage: "order_scan", Error: fmt.Sprintf("bucket order scan exceeded %d keys", maxOrderKeysPerBucket)})
		}
		for _, key := range keys {
			fields, readErr := i.reader.HGetAll(ctx, key)
			if readErr != nil {
				failures = append(failures, sampleFailure{Domain: bucket.domain, Key: key, Stage: "order_hash", Error: readErr.Error()})
				continue
			}
			actorID, parseErr := strconv.ParseUint(fields["canonical_source_authority_id"], 10, 64)
			if parseErr != nil || actorID == 0 {
				failures = append(failures, sampleFailure{Domain: bucket.domain, Key: key, Stage: "order_decode", Error: "missing canonical_source_authority_id"})
				continue
			}
			sample := bucket.byActor[actorID]
			if sample == nil {
				continue
			}
			logicalBytes := hashLogicalBytes(fields)
			sample.orderBytes += logicalBytes
			sample.orderItemBytes = append(sample.orderItemBytes, logicalBytes)
			sample.orderPayloadBytes = append(sample.orderPayloadBytes, len(fields["typed_payload"]))
		}
	}
	return failures
}

func (i *inspector) scanUntil(ctx context.Context, pattern string, limit int) ([]string, bool, error) {
	keys := make([]string, 0, limit)
	seen := make(map[string]struct{}, limit)
	var cursor uint64
	for {
		batch, next, err := i.reader.Scan(ctx, cursor, pattern, scanCountHint)
		if err != nil {
			return keys, false, err
		}
		for index, key := range batch {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
			if len(keys) == limit {
				// SCAN count is only a hint and one response may contain more
				// than limit. Unexamined keys in this batch mean the scan was
				// truncated even when Redis returned cursor zero.
				return keys, next == 0 && index == len(batch)-1, nil
			}
		}
		if next == 0 {
			return keys, true, nil
		}
		cursor = next
	}
}

func analyzeActor(domain string, actorID uint64, bucketPrefix string, fields map[string]string) (*actorSample, error) {
	if len(fields) == 0 {
		return nil, errors.New("actor hash is empty")
	}
	layout := fields["_meta:model_layout_version"]
	if layout == "" {
		layout = "1"
	}
	if layout != "1" && layout != "2" {
		return nil, errors.New("model layout is unknown")
	}
	record := fields["model:record"]
	if layout == "1" && record == "" {
		return nil, errors.New("v1 model:record is missing")
	}
	sample := &actorSample{domain: domain, actorID: actorID, bucketPrefix: bucketPrefix, modelFields: make(map[string]int), modelLayout: "v" + layout}
	sample.redisActorHashBytes = hashLogicalBytes(fields)
	sample.actorRecordBytes = len(record)
	for name, value := range fields {
		switch {
		case strings.HasPrefix(name, "model:"):
			field := strings.TrimPrefix(name, "model:")
			logicalBytes := len(field) + len(value)
			sample.modelFields[field] = logicalBytes
			sample.modelStateBytes += logicalBytes
		case strings.HasPrefix(name, "receipt:op:"):
			sample.receiptBytes += len(value)
			sample.receiptItemBytes = append(sample.receiptItemBytes, len(value))
		case strings.HasPrefix(name, "receipt:resolve:"):
			sample.resolutionReceiptBytes += len(value)
			sample.resolutionReceiptItems++
		}
	}
	if layout == "2" {
		if _, mixed := sample.modelFields["record"]; mixed {
			return nil, errors.New("mixed-invalid v2 retains model:record")
		}
		routeDomain := actorrouting.RouteDomain(strings.ToUpper(domain))
		complete := make(map[string][]byte, len(sample.modelFields))
		for name := range sample.modelFields {
			complete[name] = []byte(fields["model:"+name])
		}
		if err := fieldv2.ValidateExactFields(routeDomain, complete); err != nil {
			return nil, fmt.Errorf("partial or malformed v2 truth set: %w", err)
		}
	}
	return sample, nil
}

func summarizeDomain(domain string, requested, selected int, samples []*actorSample, failures int) domainReport {
	result := domainReport{Requested: requested, SelectedKeys: selected, Sampled: len(samples), FailureCount: failures, ModelFields: map[string]modelFieldReport{}, ModelLayouts: map[string]int{"v1": 0, "v2": 0}}
	var redisActorHash, actorRecord, modelState, receiptActor, receiptItem, resolutionActor, orderActor, orderItem, orderPayload []int
	fieldValues := make(map[string][]int)
	for _, sample := range samples {
		result.ModelLayouts[sample.modelLayout]++
		redisActorHash = append(redisActorHash, sample.redisActorHashBytes)
		actorRecord = append(actorRecord, sample.actorRecordBytes)
		modelState = append(modelState, sample.modelStateBytes)
		receiptActor = append(receiptActor, sample.receiptBytes)
		receiptItem = append(receiptItem, sample.receiptItemBytes...)
		result.ReceiptItems += len(sample.receiptItemBytes)
		resolutionActor = append(resolutionActor, sample.resolutionReceiptBytes)
		result.ResolutionReceiptItems += sample.resolutionReceiptItems
		orderActor = append(orderActor, sample.orderBytes)
		orderItem = append(orderItem, sample.orderItemBytes...)
		orderPayload = append(orderPayload, sample.orderPayloadBytes...)
		result.OrderItems += len(sample.orderItemBytes)
		for name, size := range sample.modelFields {
			fieldValues[name] = append(fieldValues[name], size)
		}
	}
	for _, name := range expectedModelFields[domain] {
		if _, exists := fieldValues[name]; !exists {
			fieldValues[name] = nil
		}
	}
	result.RedisActorHashLogicalBytes = summarize(redisActorHash)
	result.ActorRecordSerializedBytes = summarize(actorRecord)
	result.ModelStateBytes = summarize(modelState)
	result.ReceiptBytesPerActor = summarize(receiptActor)
	result.ReceiptBytesPerItem = summarize(receiptItem)
	result.ResolutionReceiptBytesPerActor = summarize(resolutionActor)
	result.OrderBytesPerActor = summarize(orderActor)
	result.OrderBytesPerItem = summarize(orderItem)
	result.OrderPayloadBytesPerItem = summarize(orderPayload)
	for name, values := range fieldValues {
		result.ModelFields[name] = modelFieldReport{PresentSamples: len(values), MissingSamples: len(samples) - len(values), LogicalBytes: summarize(values)}
	}
	return result
}

func summarize(values []int) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	ordered := append([]int(nil), values...)
	sort.Ints(ordered)
	percentile := func(p float64) int {
		index := int(math.Ceil(p*float64(len(ordered)))) - 1
		if index < 0 {
			index = 0
		}
		return ordered[index]
	}
	return distribution{Count: len(ordered), P50: percentile(.50), P95: percentile(.95), P99: percentile(.99), Max: ordered[len(ordered)-1]}
}

func hashLogicalBytes(fields map[string]string) int {
	total := 0
	for name, value := range fields {
		total += len(name) + len(value)
	}
	return total
}

func actorKeyPattern(namespace, domain string) string {
	environment, zone, _ := parseNamespace(namespace)
	return fmt.Sprintf("{classic-farm:%s:%s:auth:%s:%s:s:*}:actor:*", environment, zone, domain, authorityLayoutVersion)
}

func parseActorKey(key string) (uint64, string, error) {
	marker := ":actor:"
	index := strings.LastIndex(key, marker)
	if index <= 0 {
		return 0, "", errors.New("not an auth-v1 Actor key")
	}
	actorID, err := strconv.ParseUint(key[index+len(marker):], 10, 64)
	if err != nil || actorID == 0 {
		return 0, "", errors.New("actor id is malformed")
	}
	return actorID, key[:index], nil
}

func parseNamespace(namespace string) (string, string, error) {
	parts := strings.Split(namespace, ":")
	if len(parts) != 2 || !validLowerSegment(parts[0]) || !validLowerSegment(parts[1]) {
		return "", "", errors.New("actor-layout-inspect: namespace must be <environment>:<zone> using lowercase safe segments")
	}
	return parts[0], parts[1], nil
}

func validLowerSegment(value string) bool {
	if len(value) == 0 || len(value) > 32 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}
