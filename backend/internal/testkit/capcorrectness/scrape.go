package capcorrectness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// ScrapeProducer reads one gate from a service that counts the violation
// itself. The service keeps its own naming; this producer maps it onto the
// contract name the oracle reads.
//
// A gate counter must be registered at service start and left unlabelled, so
// that "nothing has gone wrong yet" reads as an explicit zero. A counter that
// only appears after its first increment would be missing for the whole of a
// healthy run, which this producer reports as absence and which therefore
// costs the run.
type ScrapeProducer struct {
	client   *http.Client
	endpoint string
	metric   string
	labels   map[string]string
}

func NewScrapeProducer(client *http.Client, endpoint string, metric string, labels map[string]string) (*ScrapeProducer, error) {
	if client == nil {
		return nil, errors.New("capcorrectness: scrape client is required")
	}
	if strings.TrimSpace(metric) == "" {
		return nil, errors.New("capcorrectness: scrape metric is required")
	}
	if err := requireLoopback(endpoint); err != nil {
		return nil, err
	}
	return &ScrapeProducer{client: client, endpoint: endpoint, metric: metric, labels: labels}, nil
}

// requireLoopback keeps the exporter reading only what runs beside it. A gate
// scraped across the network could be answered by a different deployment than
// the one under test.
func requireLoopback(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" {
		return fmt.Errorf("capcorrectness: %q is not a plain loopback endpoint", endpoint)
	}
	switch host := parsed.Hostname(); host {
	case "localhost", "127.0.0.1", "::1":
		return nil
	default:
		return fmt.Errorf("capcorrectness: %q is not a loopback endpoint", endpoint)
	}
}

func (p *ScrapeProducer) Observe(ctx context.Context) (float64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint, nil)
	if err != nil {
		return 0, err
	}
	response, err := p.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("capcorrectness: %s returned %d", p.metric, response.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return 0, err
	}
	family, found := families[p.metric]
	if !found {
		return 0, fmt.Errorf("capcorrectness: %s is not exported", p.metric)
	}
	return sumMatching(family, p.labels)
}

// sumMatching folds every series that carries the requested labels. A gate that
// a service reports per shard or per domain is still one invariant, so the run
// reads the total rather than a series that happens to be first.
func sumMatching(family *dto.MetricFamily, labels map[string]string) (float64, error) {
	total := 0.0
	matched := 0
	for _, metric := range family.GetMetric() {
		if !hasLabels(metric, labels) {
			continue
		}
		value, err := sampleValue(metric)
		if err != nil {
			return 0, fmt.Errorf("capcorrectness: %s: %w", family.GetName(), err)
		}
		total += value
		matched++
	}
	if matched == 0 {
		return 0, fmt.Errorf("capcorrectness: %s has no series for %v", family.GetName(), labels)
	}
	return total, nil
}

func hasLabels(metric *dto.Metric, labels map[string]string) bool {
	for name, want := range labels {
		found := false
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == name && pair.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sampleValue(metric *dto.Metric) (float64, error) {
	switch {
	case metric.GetCounter() != nil:
		return metric.GetCounter().GetValue(), nil
	case metric.GetGauge() != nil:
		return metric.GetGauge().GetValue(), nil
	case metric.GetUntyped() != nil:
		return metric.GetUntyped().GetValue(), nil
	default:
		return 0, errors.New("series is not a counter or gauge")
	}
}
