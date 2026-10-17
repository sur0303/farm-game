package friendshipservice

import "github.com/prometheus/client_golang/prometheus"

type PrometheusShadowRecorder struct {
	comparisons *prometheus.CounterVec
}

func NewPrometheusShadowRecorder(registerer prometheus.Registerer) (*PrometheusShadowRecorder, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "friendship", Name: "shadow_comparison_total",
		Help: "Bounded FriendshipService shadow comparisons by method and result class.",
	}, []string{"method", "result"})
	if err := registerer.Register(counter); err != nil {
		if existing, ok := err.(prometheus.AlreadyRegisteredError); ok {
			counter = existing.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return nil, err
		}
	}
	return &PrometheusShadowRecorder{comparisons: counter}, nil
}

func (r *PrometheusShadowRecorder) Record(method string, result ShadowResult) {
	if r == nil || r.comparisons == nil {
		return
	}
	switch method {
	case "create_share", "resolve_share", "redeem_share", "get_friend_list", "delete_friend", "block_friend",
		"unblock_friend", "get_next_friend_opportunity", "authorize_visit", "commit_friend_action_admission",
		"get_friend_action_admission":
	default:
		method = "unknown"
	}
	switch result {
	case ShadowEqual, ShadowMismatch, ShadowMalformed, ShadowUnavailable:
	default:
		result = ShadowMalformed
	}
	r.comparisons.WithLabelValues(method, string(result)).Inc()
}
