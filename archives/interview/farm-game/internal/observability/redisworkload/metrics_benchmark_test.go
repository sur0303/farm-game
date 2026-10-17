package redisworkload

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func BenchmarkAttachDouble(b *testing.B) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	expected, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		metrics, err := AttachFromEnvironment(client, registry, "true")
		if err != nil {
			b.Fatal(err)
		}
		if metrics != expected {
			b.Fatalf("double attach returned collectors %p and %p", expected, metrics)
		}
	}
	b.StopTimer()
	if got := client.hookCount(); got != 1 {
		b.Fatalf("double attach added %d hooks want=1", got)
	}
}

func BenchmarkAttachConcurrent(b *testing.B) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	expected, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			metrics, err := AttachFromEnvironment(client, registry, "true")
			if err != nil {
				b.Error(err)
				return
			}
			if metrics != expected {
				b.Errorf("concurrent attach returned collectors %p and %p", expected, metrics)
				return
			}
		}
	})
	b.StopTimer()
	if got := client.hookCount(); got != 1 {
		b.Fatalf("concurrent attach added %d hooks want=1", got)
	}
}

func BenchmarkBoundedCardinality(b *testing.B) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	metrics, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		b.Fatal(err)
	}
	inputs := make([]string, 1024)
	for index := range inputs {
		inputs[index] = fmt.Sprintf("secret-user-component-%d", index)
	}
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "get", "secret-key")
	hook := metrics.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	index := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		attributed := WithComponent(ctx, inputs[index&(len(inputs)-1)])
		if err := hook(attributed, cmd); err != nil {
			b.Fatal(err)
		}
		index++
	}
	b.StopTimer()
	families, err := registry.Gather()
	if err != nil {
		b.Fatal(err)
	}
	series := 0
	for _, family := range families {
		series += len(family.GetMetric())
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "component" && label.GetValue() != ComponentUnspecified {
					b.Fatalf("untrusted component escaped normalization: %q", label.GetValue())
				}
			}
		}
	}
	b.ReportMetric(float64(series), "bounded_series")
}

func BenchmarkDisabledAttributionAllocations(b *testing.B) {
	wasActive := attributionActive.Swap(false)
	b.Cleanup(func() { attributionActive.Store(wasActive) })
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if got := WithComponent(ctx, ComponentActorLoad); got != ctx {
			b.Fatal("disabled attribution changed the context")
		}
	}
}
