// Command multigate-proxy is a verification-only readiness-aware reverse
// proxy. It models the Kubernetes Service boundary in local multi-Gate drain
// tests: a draining Gate returns /readyz=503, so new HTTP and WebSocket
// requests are sent to the next ready instance.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	upstreams := parseUpstreams(env("MULTIGATE_UPSTREAMS", "http://127.0.0.1:18380,http://127.0.0.1:18480"))
	healthClient := &http.Client{Timeout: 300 * time.Millisecond}
	proxy := &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			target := firstReady(request.Context(), healthClient, upstreams)
			request.URL.Scheme = target.Scheme
			request.URL.Host = target.Host
			request.Host = target.Host
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("proxy upstream error: %v", err)
			http.Error(w, "no ready farm-gate", http.StatusServiceUnavailable)
		},
		FlushInterval: -1,
	}
	server := &http.Server{
		Addr:              env("MULTIGATE_PROXY_LISTEN", "127.0.0.1:18580"),
		Handler:           proxy,
		ReadHeaderTimeout: 5 * time.Second,
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("multi-gate verification proxy listening on %s upstreams=%v", server.Addr, upstreams)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func parseUpstreams(raw string) []*url.URL {
	var values []*url.URL
	for _, item := range strings.Split(raw, ",") {
		value, err := url.Parse(strings.TrimSpace(item))
		if err != nil || value.Scheme == "" || value.Host == "" {
			log.Fatalf("invalid MULTIGATE_UPSTREAMS entry %q", item)
		}
		values = append(values, value)
	}
	if len(values) < 2 {
		log.Fatal("MULTIGATE_UPSTREAMS requires at least two Gate instances")
	}
	return values
}

func firstReady(ctx context.Context, client *http.Client, upstreams []*url.URL) *url.URL {
	for _, upstream := range upstreams {
		readyURL := *upstream
		readyURL.Path = "/readyz"
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, readyURL.String(), nil)
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return upstream
			}
		}
	}
	// ReverseProxy will surface the real connection failure/503. Never route
	// to a random later entry because that would hide total unavailability.
	return upstreams[0]
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
