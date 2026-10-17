// Package redisdomains owns Redis endpoint resolution and client lifecycle for
// the three bounded storage domains used by the farm services.
package redisdomains

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

const (
	LegacyURLEnv         = "FARM_REDIS_URL"
	AuthorityURLEnv      = "FARM_AUTHORITY_REDIS_URL"
	RuntimeURLEnv        = "FARM_RUNTIME_REDIS_URL"
	ProjectionURLEnv     = "FARM_PROJECTION_REDIS_URL"
	LegacyClusterURLEnv  = "FARM_REDIS_CLUSTER_URLS"
	AuthorityClusterEnv  = "FARM_AUTHORITY_REDIS_CLUSTER_URLS"
	RuntimeClusterEnv    = "FARM_RUNTIME_REDIS_CLUSTER_URLS"
	ProjectionClusterEnv = "FARM_PROJECTION_REDIS_CLUSTER_URLS"
	// ClusterEnv is the FO/r93 compatibility switch. Production config should
	// set *_REDIS_CLUSTER_URLS; FARM_REDIS_CLUSTER=1 only synthesizes a single
	// seed from the matching standalone URL so existing UC YAML keeps working.
	ClusterEnv = "FARM_REDIS_CLUSTER"
)

// Domain is deliberately closed: adding a storage domain requires an explicit
// code and deployment change instead of accepting arbitrary label values.
type Domain uint8

const (
	Authority Domain = iota + 1
	Runtime
	Projection
)

func (d Domain) String() string {
	switch d {
	case Authority:
		return "authority"
	case Runtime:
		return "runtime"
	case Projection:
		return "projection"
	default:
		return "unknown"
	}
}

// Selection identifies the domains needed by one process.
type Selection uint8

const (
	SelectAuthority Selection = 1 << iota
	SelectRuntime
	SelectProjection
	SelectAll = SelectAuthority | SelectRuntime | SelectProjection
)

func (s Selection) Has(domain Domain) bool {
	switch domain {
	case Authority:
		return s&SelectAuthority != 0
	case Runtime:
		return s&SelectRuntime != 0
	case Projection:
		return s&SelectProjection != 0
	default:
		return false
	}
}

// endpoint is deliberately private. Keeping credentials and seed URLs behind
// this type prevents callers from accidentally logging them and lets the
// same process use either one-node Redis or a Redis Cluster without changing
// its storage-domain code.
type endpoint struct {
	singleURL   string
	clusterURLs []string
}

func (e endpoint) empty() bool {
	return strings.TrimSpace(e.singleURL) == "" && len(e.clusterURLs) == 0
}

// URLs contains resolved endpoints. Values remain private so callers cannot
// accidentally log credentials; clients are obtained from Open.
type URLs struct {
	authority  endpoint
	runtime    endpoint
	projection endpoint
}

func (u URLs) forDomain(domain Domain) endpoint {
	switch domain {
	case Authority:
		return u.authority
	case Runtime:
		return u.runtime
	case Projection:
		return u.projection
	default:
		return endpoint{}
	}
}

// Resolve reads endpoint configuration without opening network connections.
// Every domain independently falls back only to FARM_REDIS_URL.
func Resolve(getenv func(string) string, selected Selection) (URLs, error) {
	if getenv == nil {
		return URLs{}, errors.New("redisdomains: getenv is required")
	}
	if selected == 0 || selected&^SelectAll != 0 {
		return URLs{}, errors.New("redisdomains: invalid domain selection")
	}
	legacySingle := strings.TrimSpace(getenv(LegacyURLEnv))
	legacyCluster := splitSeeds(getenv(LegacyClusterURLEnv))
	resolve := func(domain Domain, singleEnv, clusterEnv string) (endpoint, error) {
		single := strings.TrimSpace(getenv(singleEnv))
		if single == "" {
			single = legacySingle
		}
		seeds := splitSeeds(getenv(clusterEnv))
		if len(seeds) == 0 {
			seeds = append([]string(nil), legacyCluster...)
		}
		if len(seeds) == 0 && ClusterEnabled(getenv) && single != "" {
			seeds = []string{single}
		}
		// A cluster seed list is authoritative when present. This permits a
		// staged rollout where FARM_REDIS_URL remains set for older binaries.
		if len(seeds) == 0 && selected.Has(domain) && single == "" {
			return endpoint{}, fmt.Errorf("redisdomains: %s Redis endpoint is required (%s/%s or %s/%s)", domain, singleEnv, clusterEnv, LegacyURLEnv, LegacyClusterURLEnv)
		}
		return endpoint{singleURL: single, clusterURLs: seeds}, nil
	}
	var out URLs
	var err error
	if out.authority, err = resolve(Authority, AuthorityURLEnv, AuthorityClusterEnv); err != nil {
		return URLs{}, err
	}
	if out.runtime, err = resolve(Runtime, RuntimeURLEnv, RuntimeClusterEnv); err != nil {
		return URLs{}, err
	}
	if out.projection, err = resolve(Projection, ProjectionURLEnv, ProjectionClusterEnv); err != nil {
		return URLs{}, err
	}
	return out, nil
}

func splitSeeds(raw string) []string {
	parts := strings.Split(raw, ",")
	seeds := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			seeds = append(seeds, value)
		}
	}
	return seeds
}

// ClusterEnabled reports whether getenv asks for Redis Cluster clients via the
// compatibility flag. Standalone remains the default so unit tests against
// miniredis stay unchanged. Explicit *_REDIS_CLUSTER_URLS always win.
func ClusterEnabled(getenv func(string) string) bool {
	if getenv == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(getenv(ClusterEnv))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

// Clients contains independent go-redis clients and pools. Each field may be
// either *redis.Client or *redis.ClusterClient behind UniversalClient. A nil
// field means that the process did not select that domain.
type Clients struct {
	Authority  redis.UniversalClient
	Runtime    redis.UniversalClient
	Projection redis.UniversalClient

	closeOnce sync.Once
	closeErr  error
}

func (c *Clients) Client(domain Domain) redis.UniversalClient {
	if c == nil {
		return nil
	}
	switch domain {
	case Authority:
		return c.Authority
	case Runtime:
		return c.Runtime
	case Projection:
		return c.Projection
	default:
		return nil
	}
}

type clientFactory func(*redis.Options) *redis.Client

type endpointFactory func(endpoint) (redis.UniversalClient, error)

// Open creates and validates one independent client per selected domain.
// Parse and Ping failures are intentionally secret-safe and never include the
// raw endpoint or wrapped errors that might contain credentials.
func Open(ctx context.Context, urls URLs, selected Selection) (*Clients, error) {
	return openWithEndpointFactory(ctx, urls, selected, defaultEndpointFactory)
}

// OpenFromEnv combines Resolve and Open.
func OpenFromEnv(ctx context.Context, getenv func(string) string, selected Selection) (*Clients, error) {
	urls, err := Resolve(getenv, selected)
	if err != nil {
		return nil, err
	}
	return Open(ctx, urls, selected)
}

func openWithFactory(ctx context.Context, urls URLs, selected Selection, factory clientFactory) (*Clients, error) {
	if factory == nil {
		return nil, errors.New("redisdomains: client factory is required")
	}
	return openWithEndpointFactory(ctx, urls, selected, func(ep endpoint) (redis.UniversalClient, error) {
		if len(ep.clusterURLs) != 0 {
			return nil, errors.New("redisdomains: test client factory does not support cluster endpoints")
		}
		options, err := parseSingleURL(ep.singleURL)
		if err != nil {
			return nil, err
		}
		return factory(options), nil
	})
}

func openWithEndpointFactory(ctx context.Context, urls URLs, selected Selection, factory endpointFactory) (*Clients, error) {
	if ctx == nil {
		return nil, errors.New("redisdomains: context is required")
	}
	if factory == nil {
		return nil, errors.New("redisdomains: client factory is required")
	}
	if selected == 0 || selected&^SelectAll != 0 {
		return nil, errors.New("redisdomains: invalid domain selection")
	}
	clients := &Clients{}
	for _, domain := range []Domain{Authority, Runtime, Projection} {
		if !selected.Has(domain) {
			continue
		}
		ep := urls.forDomain(domain)
		if ep.empty() {
			_ = clients.Close()
			return nil, fmt.Errorf("redisdomains: %s Redis endpoint is required", domain)
		}
		client, err := factory(ep)
		if err != nil {
			_ = clients.Close()
			return nil, fmt.Errorf("redisdomains: invalid %s Redis endpoint", domain)
		}
		setClient(clients, domain, client)
		if err := client.Ping(ctx).Err(); err != nil {
			_ = clients.Close()
			return nil, fmt.Errorf("redisdomains: %s Redis is unavailable", domain)
		}
	}
	return clients, nil
}

func setClient(clients *Clients, domain Domain, client redis.UniversalClient) {
	switch domain {
	case Authority:
		clients.Authority = client
	case Runtime:
		clients.Runtime = client
	case Projection:
		clients.Projection = client
	}
}

func parseSingleURL(raw string) (*redis.Options, error) {
	options, err := redis.ParseURL(strings.TrimSpace(raw))
	if err != nil || options == nil || strings.TrimSpace(options.Addr) == "" {
		return nil, errors.New("invalid Redis URL")
	}
	return options, nil
}

func defaultEndpointFactory(ep endpoint) (redis.UniversalClient, error) {
	if len(ep.clusterURLs) == 0 {
		options, err := parseSingleURL(ep.singleURL)
		if err != nil {
			return nil, err
		}
		return redis.NewClient(options), nil
	}
	cluster := redis.ClusterOptions{}
	for index, raw := range ep.clusterURLs {
		options, err := parseSingleURL(raw)
		if err != nil {
			return nil, err
		}
		if options.DB != 0 {
			return nil, errors.New("Redis Cluster URLs must use database 0")
		}
		if index == 0 {
			cluster.Username = options.Username
			cluster.Password = options.Password
			cluster.Protocol = options.Protocol
			cluster.TLSConfig = options.TLSConfig
			cluster.DialTimeout = options.DialTimeout
			cluster.ReadTimeout = options.ReadTimeout
			cluster.WriteTimeout = options.WriteTimeout
			cluster.ContextTimeoutEnabled = options.ContextTimeoutEnabled
			cluster.MaxRetries = options.MaxRetries
			cluster.MinRetryBackoff = options.MinRetryBackoff
			cluster.MaxRetryBackoff = options.MaxRetryBackoff
			cluster.PoolSize = options.PoolSize
			cluster.PoolTimeout = options.PoolTimeout
			cluster.MinIdleConns = options.MinIdleConns
			cluster.MaxIdleConns = options.MaxIdleConns
			cluster.MaxActiveConns = options.MaxActiveConns
			cluster.ConnMaxIdleTime = options.ConnMaxIdleTime
			cluster.ConnMaxLifetime = options.ConnMaxLifetime
			cluster.ReadBufferSize = options.ReadBufferSize
			cluster.WriteBufferSize = options.WriteBufferSize
			cluster.DisableIdentity = options.DisableIdentity
		}
		cluster.Addrs = append(cluster.Addrs, options.Addr)
	}
	return redis.NewClusterClient(&cluster), nil
}

func endpointFrom(singleURL, clusterURLs string, getenv func(string) string) endpoint {
	single := strings.TrimSpace(singleURL)
	seeds := splitSeeds(clusterURLs)
	if len(seeds) == 0 && ClusterEnabled(getenv) && single != "" {
		seeds = []string{single}
	}
	return endpoint{singleURL: single, clusterURLs: seeds}
}

// OpenEndpoint is used by the small control-plane processes which select one
// Redis domain instead of all three. clusterURLs is a comma-separated list of
// redis:// seed URLs. When empty, FARM_REDIS_CLUSTER=1 still treats singleURL
// as one Cluster seed so port-forward bootstrap keeps following MOVED/ASK.
func OpenEndpoint(ctx context.Context, singleURL, clusterURLs string) (redis.UniversalClient, error) {
	if ctx == nil {
		return nil, errors.New("redisdomains: context is required")
	}
	ep := endpointFrom(singleURL, clusterURLs, os.Getenv)
	if ep.empty() {
		return nil, errors.New("redisdomains: Redis endpoint is required")
	}
	client, err := defaultEndpointFactory(ep)
	if err != nil {
		return nil, errors.New("redisdomains: invalid Redis endpoint")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, errors.New("redisdomains: Redis is unavailable")
	}
	return client, nil
}

// Dial opens one Redis client. cluster=true treats rawURL as a single Cluster
// seed so callers that still pass FARM_REDIS_CLUSTER=1 follow MOVED/ASK.
func Dial(ctx context.Context, rawURL string, cluster bool) (redis.UniversalClient, error) {
	clusterURLs := ""
	if cluster {
		clusterURLs = strings.TrimSpace(rawURL)
	}
	return OpenEndpoint(ctx, rawURL, clusterURLs)
}

// Close is idempotent and attempts to close every selected domain.
func (c *Clients) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		var errs []error
		for _, client := range []redis.UniversalClient{c.Authority, c.Runtime, c.Projection} {
			if client != nil {
				errs = append(errs, client.Close())
			}
		}
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}
