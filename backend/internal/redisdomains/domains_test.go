package redisdomains

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func redisURL(server *miniredis.Miniredis) string {
	return "redis://" + server.Addr() + "/0"
}

func TestLegacyFallbackUsesDistinctClients(t *testing.T) {
	server := miniredis.RunT(t)
	clients, err := OpenFromEnv(context.Background(), env(map[string]string{
		LegacyURLEnv: redisURL(server),
	}), SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.Close()
	if clients.Authority == clients.Runtime || clients.Authority == clients.Projection || clients.Runtime == clients.Projection {
		t.Fatal("selected domains must use distinct redis clients and pools")
	}
	for _, client := range []redis.UniversalClient{clients.Authority, clients.Runtime, clients.Projection} {
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClusterSeedsOverrideLegacyURL(t *testing.T) {
	urls, err := Resolve(env(map[string]string{
		LegacyURLEnv:        "redis://legacy:6379/0",
		AuthorityClusterEnv: "redis://redis-a:6379/0, redis://redis-b:6379/0,redis://redis-c:6379/0",
	}), SelectAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls.authority.clusterURLs) != 3 || urls.authority.singleURL != "redis://legacy:6379/0" {
		t.Fatalf("resolved endpoint=%+v", urls.authority)
	}
}

func TestLegacyClusterSeedsFallbackToEverySelectedDomain(t *testing.T) {
	urls, err := Resolve(env(map[string]string{
		LegacyClusterURLEnv: "redis://redis-a:6379/0,redis://redis-b:6379/0,redis://redis-c:6379/0",
	}), SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []endpoint{urls.authority, urls.runtime, urls.projection} {
		if len(endpoint.clusterURLs) != 3 || endpoint.singleURL != "" {
			t.Fatalf("cluster fallback endpoint=%+v", endpoint)
		}
	}
}

func TestDomainClusterOverridesGenericCluster(t *testing.T) {
	urls, err := Resolve(env(map[string]string{
		LegacyClusterURLEnv: "redis://generic-a:6379/0,redis://generic-b:6379/0",
		RuntimeClusterEnv:   "redis://runtime-a:6379/0,redis://runtime-b:6379/0,redis://runtime-c:6379/0",
	}), SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls.authority.clusterURLs) != 2 || len(urls.projection.clusterURLs) != 2 || len(urls.runtime.clusterURLs) != 3 {
		t.Fatalf("domain cluster override endpoints=%+v", urls)
	}
	if urls.runtime.clusterURLs[0] != "redis://runtime-a:6379/0" {
		t.Fatalf("runtime cluster seeds=%v", urls.runtime.clusterURLs)
	}
}

func TestClusterEndpointRejectsLogicalDatabase(t *testing.T) {
	_, err := defaultEndpointFactory(endpoint{clusterURLs: []string{"redis://redis-a:6379/2"}})
	if err == nil || !strings.Contains(err.Error(), "database 0") {
		t.Fatalf("err=%v, want database-0 rejection", err)
	}
}

func TestExplicitDomainsAreIsolated(t *testing.T) {
	authority := miniredis.RunT(t)
	runtime := miniredis.RunT(t)
	projection := miniredis.RunT(t)
	clients, err := OpenFromEnv(context.Background(), env(map[string]string{
		AuthorityURLEnv: redisURL(authority), RuntimeURLEnv: redisURL(runtime), ProjectionURLEnv: redisURL(projection),
	}), SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.Close()
	ctx := context.Background()
	if err := clients.Authority.Set(ctx, "domain-key", "authority", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := clients.Runtime.Set(ctx, "domain-key", "runtime", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := clients.Projection.Set(ctx, "domain-key", "projection", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got, _ := authority.Get("domain-key"); got != "authority" {
		t.Fatalf("authority value=%q", got)
	}
	if got, _ := runtime.Get("domain-key"); got != "runtime" {
		t.Fatalf("runtime value=%q", got)
	}
	if got, _ := projection.Get("domain-key"); got != "projection" {
		t.Fatalf("projection value=%q", got)
	}
}

func TestMissingSelectedDomainFailsBeforeOpen(t *testing.T) {
	_, err := Resolve(env(map[string]string{AuthorityURLEnv: "redis://127.0.0.1:1/0"}), SelectAuthority|SelectRuntime)
	if err == nil || !strings.Contains(err.Error(), RuntimeURLEnv) {
		t.Fatalf("error=%v", err)
	}
}

func TestClusterFlagSynthesizesSingleSeedWhenClusterURLsAbsent(t *testing.T) {
	urls, err := Resolve(env(map[string]string{
		LegacyURLEnv: "redis://redis-a:6379/0",
		ClusterEnv:   "1",
	}), SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []endpoint{urls.authority, urls.runtime, urls.projection} {
		if len(endpoint.clusterURLs) != 1 || endpoint.clusterURLs[0] != "redis://redis-a:6379/0" {
			t.Fatalf("cluster flag seed=%+v", endpoint)
		}
	}
}

func TestOpenEndpointClusterFlagSynthesizesSingleSeed(t *testing.T) {
	ep := endpointFrom("redis://redis-a:6379/0", "", env(map[string]string{ClusterEnv: "1"}))
	if len(ep.clusterURLs) != 1 || ep.clusterURLs[0] != "redis://redis-a:6379/0" {
		t.Fatalf("OpenEndpoint cluster flag seed=%+v", ep)
	}
	explicit := endpointFrom("redis://legacy:6379/0", "redis://redis-a:6379/0,redis://redis-b:6379/0", env(map[string]string{ClusterEnv: "1"}))
	if len(explicit.clusterURLs) != 2 {
		t.Fatalf("explicit CLUSTER_URLS must win: %+v", explicit)
	}
}

func TestClusterURLsWinOverClusterFlag(t *testing.T) {
	urls, err := Resolve(env(map[string]string{
		LegacyURLEnv:        "redis://legacy:6379/0",
		LegacyClusterURLEnv: "redis://redis-a:6379/0,redis://redis-b:6379/0",
		ClusterEnv:          "1",
	}), SelectAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls.authority.clusterURLs) != 2 {
		t.Fatalf("CLUSTER_URLS must win over FARM_REDIS_CLUSTER=1: %+v", urls.authority)
	}
}

func TestMalformedURLDoesNotLeakCredentials(t *testing.T) {
	secret := "do-not-log-this"
	urls, err := Resolve(env(map[string]string{AuthorityURLEnv: "redis://user:" + secret + "%zz@127.0.0.1:6379/0"}), SelectAuthority)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(context.Background(), urls, SelectAuthority)
	if err == nil {
		t.Fatal("expected malformed URL failure")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "user:") {
		t.Fatalf("error leaked credentials: %v", err)
	}
}

func TestPartialFailureClosesCreatedClients(t *testing.T) {
	server := miniredis.RunT(t)
	urls, err := Resolve(env(map[string]string{
		AuthorityURLEnv: redisURL(server), RuntimeURLEnv: "redis://127.0.0.1:1/0",
	}), SelectAuthority|SelectRuntime)
	if err != nil {
		t.Fatal(err)
	}
	var created []*redis.Client
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = openWithFactory(ctx, urls, SelectAuthority|SelectRuntime, func(options *redis.Options) *redis.Client {
		client := redis.NewClient(options)
		created = append(created, client)
		return client
	})
	if err == nil || len(created) != 2 {
		t.Fatalf("err=%v created=%d", err, len(created))
	}
	for index, client := range created {
		if err := client.Ping(context.Background()).Err(); !strings.Contains(err.Error(), "client is closed") {
			t.Fatalf("client %d was not closed: %v", index, err)
		}
	}
}

func TestSelectionOmitsUnusedDomainsAndCloseIsIdempotent(t *testing.T) {
	server := miniredis.RunT(t)
	clients, err := OpenFromEnv(context.Background(), env(map[string]string{
		AuthorityURLEnv: redisURL(server),
	}), SelectAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if clients.Authority == nil || clients.Runtime != nil || clients.Projection != nil {
		t.Fatalf("unexpected clients: %+v", clients)
	}
	if err := clients.Close(); err != nil {
		t.Fatal(err)
	}
	if err := clients.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClusterEnabled(t *testing.T) {
	if ClusterEnabled(nil) || ClusterEnabled(env(nil)) || ClusterEnabled(env(map[string]string{ClusterEnv: "0"})) {
		t.Fatal("cluster must stay off by default")
	}
	if !ClusterEnabled(env(map[string]string{ClusterEnv: "1"})) || !ClusterEnabled(env(map[string]string{ClusterEnv: "true"})) {
		t.Fatal("FARM_REDIS_CLUSTER=1 must enable cluster clients")
	}
}

func TestSingleClusterSeedOpensClusterClient(t *testing.T) {
	client, err := defaultEndpointFactory(endpoint{clusterURLs: []string{"redis://127.0.0.1:6379/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, ok := client.(*redis.ClusterClient); !ok {
		t.Fatalf("got %T, want ClusterClient", client)
	}
}
