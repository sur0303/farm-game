// Package grpcmetrics 记录内部 gRPC 服务的每方法调用次数与延迟。
// internal_rpc_qps 的观测来源:一次公网命令经 farm-svr/player-svr 的
// Participant RPC 放大(如 CommitReservation/GrantItem/InspectFarmOperation)都会
// 在这里计数,配合公网 command QPS 即可算出 rpc_amplification(command)。
package grpcmetrics

import (
	"context"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics 是 gRPC 服务端观测集合,仅使用有界标签(service/method/code)。
type Metrics struct {
	Calls    *prometheus.CounterVec   // service, method, code
	Latency  *prometheus.HistogramVec // service, method
	InFlight *prometheus.GaugeVec     // service, method

	ClientCalls    *prometheus.CounterVec   // service, method, target, code
	ClientLatency  *prometheus.HistogramVec // service, method, target
	ClientInFlight *prometheus.GaugeVec     // service, method, target
}

// New 注册并返回 Metrics。reg 为 nil 时使用 DefaultRegisterer。
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		Calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "calls_total",
			Help: "Internal gRPC server calls by service, method and status code.",
		}, []string{"service", "method", "code"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "server_seconds",
			Help:    "Internal gRPC server handler latency.",
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"service", "method"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "server_in_flight",
			Help: "Internal gRPC server handlers currently executing.",
		}, []string{"service", "method"}),
		ClientCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "client_calls_total",
			Help: "Internal gRPC client calls by service, method, target, and status code.",
		}, []string{"service", "method", "target", "code"}),
		ClientLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "client_seconds",
			Help:    "Internal gRPC client round-trip latency.",
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"service", "method", "target"}),
		ClientInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "rpc", Name: "client_in_flight",
			Help: "Internal gRPC client calls currently executing.",
		}, []string{"service", "method", "target"}),
	}
	reg.MustRegister(m.Calls, m.Latency, m.InFlight, m.ClientCalls, m.ClientLatency, m.ClientInFlight)
	return m
}

// UnaryClientInterceptor records the caller-side status and latency. target is
// the bounded configured backend endpoint, not a user/client IP.
func (m *Metrics) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, fullMethod string, req, resp any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		service, method := splitFullMethod(fullMethod)
		target := clientTarget(cc)
		inFlight := m.ClientInFlight.WithLabelValues(service, method, target)
		inFlight.Inc()
		defer inFlight.Dec()
		start := time.Now()
		err := invoker(ctx, fullMethod, req, resp, cc, opts...)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		m.ClientCalls.WithLabelValues(service, method, target, code.String()).Inc()
		m.ClientLatency.WithLabelValues(service, method, target).Observe(time.Since(start).Seconds())
		return err
	}
}

// UnaryServerInterceptor 返回记录次数与延迟的 unary 拦截器。
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		service, method := splitFullMethod(info.FullMethod)
		inFlight := m.InFlight.WithLabelValues(service, method)
		inFlight.Inc()
		defer inFlight.Dec()
		start := time.Now()
		resp, err := handler(ctx, req)
		code := codes.OK
		if st, ok := status.FromError(err); ok {
			code = st.Code()
		}
		m.Calls.WithLabelValues(service, method, code.String()).Inc()
		m.Latency.WithLabelValues(service, method).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// splitFullMethod 将 "/service/Method" 拆为 ("service","Method")。
func splitFullMethod(fullMethod string) (service, method string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	if index := strings.LastIndex(fullMethod, "/"); index >= 0 {
		return fullMethod[:index], fullMethod[index+1:]
	}
	return fullMethod, ""
}

func clientTarget(cc *grpc.ClientConn) string {
	if cc == nil {
		return "unknown"
	}
	target := cc.Target()
	if parsed, err := url.Parse(target); err == nil && parsed.Host == "" {
		if endpoint := parsed.Path; endpoint != "" {
			target = endpoint
		}
	}
	if host, _, err := net.SplitHostPort(target); err == nil && host != "" {
		return host
	}
	if target == "" {
		return "unknown"
	}
	return target
}
