package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type RecordRuleProbeConfig struct {
	ReadUrl         url.URL
	Interval        time.Duration
	Timeout         time.Duration
	Lookback        time.Duration
	RuleCount       int
	HttpBearerToken string
}

var (
	recordingRuleLatencyTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "recording_rule_latency_time",
			Help:    "end to end recording latency",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 40, 60, 120, 180, 240, 300},
		},
		[]string{"probe"},
	)

	recordingRuleLatencySummary = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "recording_rule_latency_time_quantile",
			Help:       "end to end recording latency quantile",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001, 0.999: 0.0001},
		},
		[]string{"probe"},
	)

	recordingRuleFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "recording_rule_failures",
			Help: "counter for recording failures",
		},
		[]string{"probe"},
	)
)

func RecordRuleProbeLoop(ctx context.Context, config RecordRuleProbeConfig) {

	interval := int64(config.Interval / time.Second)

	for {

		a := time.Duration(rand.Int63n(int64(interval*1000))) * time.Millisecond
		select {

		case <-ctx.Done():
			return

		case <-time.After(a):
			pctx, cancel := context.WithTimeout(ctx, config.Timeout)
			defer cancel()
			probename := "record_rule_prober"
			probeExecutionsTotal.WithLabelValues(probename).Inc()
			if err := RecordRuleProbe(pctx, config); err != nil {
				probeErrorsTotal.WithLabelValues(probename).Inc()
			}
		}

	}

}

func RecordRuleProbe(pctx context.Context, config RecordRuleProbeConfig) error {

	var wg sync.WaitGroup

	rule_count := config.RuleCount
	metricsToQuery := []string{}
	generated := make(map[int]bool)
	rand.Seed(time.Now().UnixNano())

	numGenerated := 0

	for numGenerated < 20 {

		x := rand.Intn(rule_count) + 1

		if !generated[x] {
			generated[x] = true
			numGenerated++
		}

	}
	for k, _ := range generated {

		s := strconv.Itoa(k)

		metricsToQuery = append(metricsToQuery, "record:"+s)
	}

	for _, m := range metricsToQuery {
		wg.Add(1)
		go func(metric string) {
			defer wg.Done()
			RecordRuleQueryAndRecord(pctx, metric, config)
		}(m)
	}

	wg.Wait()
	return nil

}

func RecordRuleQueryAndRecord(ctx context.Context, metric string, config RecordRuleProbeConfig) error {

	Url := config.ReadUrl
	look_back := int64(config.Lookback / time.Minute)
	params := url.Values{}

	query := metric + "{}[" + strconv.Itoa(int(look_back)) + "m]"
	// fmt.Printf("query: %v\n", query)
	Url.Path = Url.Path + "/api/v1/query"
	params.Set("query", query)
	Url.RawQuery = params.Encode()

	resp, err := pipelineProbeDo(ctx, "GET", Url.String(), nil, config.HttpBearerToken)

	if err != nil {
		return fmt.Errorf("erros while querying %v\n%s", Url.String())
	}

	var response QueryResponse

	// fmt.Printf("response: %+v\n", response)

	if err := json.Unmarshal(resp, &response); err != nil {
		return err
	}

	maxLatency := 300.0
	var latency float64

	latency = maxLatency

	if (len(response.Data.Result) > 0) && len(response.Data.Result[0].Values) > 0 {

		PipelineProbeSeries := response.Data.Result
		values := PipelineProbeSeries[0].Values
		lastSeenTimeStamp, _ := values[len(values)-1][0].(float64)
		now := (float64(time.Now().UnixNano()) / float64(time.Millisecond)) / (1000.0)
		latency = float64(now - lastSeenTimeStamp)
		// fmt.Printf("lastSeen %v, now %v, latency %v\n", lastSeenTimeStamp, now, latency)

	}

	if latency >= maxLatency {
		recordingRuleFailures.WithLabelValues("record_rule_prober").Inc()
	} else {

		recordingRuleLatencyTime.WithLabelValues("record_rule_prober").Observe(latency)
		recordingRuleLatencySummary.WithLabelValues("record_rule_prober").Observe(latency)
	}

	return err

}
