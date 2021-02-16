package metrics

import (
	"encoding/json"
        "fmt"
	"io/ioutil"
	"log"
        "math"
	"net/http"
	"net/url"
        "strings"
        "strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promauto"
)

// queries map with cardinality as key aa well as value should be cardinality
var qmap  = map[int]string{10: "max_over_time(count(avg({series_id=~\"[0-9]{1,1}\", __name__ =~\"avalanche_metric_mmmmm_._I\",C}) by(series_id))[T:S])", 100: "max_over_time(count(avg({series_id=~\"[0-9]{1,2}\", __name__ =~\"avalanche_metric_mmmmm_._I\",C}) by(series_id))[T:S])", 1000: "max_over_time(count(avg({series_id=~\"[0-9]{1,3}\", __name__ =~\"avalanche_metric_mmmmm_._I\",C}) by(series_id))[T:S])", 10000: "max_over_time(count(avg({series_id=~\"[0-9]{1,3}\", __name__ =~\"avalanche_metric_mmmmm_._[0-9]{1,1}\",C}) by(series_id))[T:S])", 100000: "max_over_time(count(avg({series_id=~\"[0-9]{1,3}\", __name__ =~\"avalanche_metric_mmmmm_._[0-9]{1,2}\",C}) by(series_id))[T:S])", 500000: "max_over_time(count(avg({series_id=~\"[0-9]{1,3}\", __name__ =~\"avalanche_metric_mmmmm_._[0-9]{1,3}\",C}) by(series_id))[T:S])"}


var (
	tstep =  map[string]string{"2h": "10s", "24h": "1m", "7d":"10m", "30d":"10m"}
	tdis = map[string]float64{"2h": 0.5, "24h": 0.35, "7d": 0.1, "30d": 0.05}
	cdis = map[int]int{10: 200, 100: 200, 1000: 50, 10000: 10, 100000: 10, 500000: 5}

	queryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "promql_query_total",
			Help: "The total number of query generated",
		},
		[]string{"group"},
	)

	queryAccuracy = promauto.NewCounterVec(
                prometheus.CounterOpts{
                        Name: "promql_query_value_match",
                        Help: "The total number of query returned expected valuess",
                },
                []string{"group"},
        )

	queryFailures = promauto.NewCounterVec(
                prometheus.CounterOpts{
                        Name: "promql_query_failures",
                        Help: "The total number of query failures",
                },
                []string{"group"},
        )

	queryLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "store_query_request_durations",
			Help:       "Store requests latencies in seconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001},
		},
		[]string{"group"},
	)
)

type ConfigRead struct {
	URL             url.URL
	RequestInterval time.Duration
	Size         int
	RequestCount int
	Tenant       string
	ConstLabels  []string
}

// Client for the remote write requests.
type ReadClient struct {
	client  *http.Client
	timeout time.Duration
	config  ConfigRead
}
type Response struct {
	Status string
	Data *data
}
type data struct {
	ResultType string
	Result []*series
}
type series struct {
	Metric map[string]string
	Value [2]interface{}
}


// Make size queries after every RequestInterval RequestCount times.
func Query(config ConfigRead) {
        // N*M means M queries with N cardinality
	var rt http.RoundTripper = &http.Transport{}
	rt = &cortexTenantRoundTripper{tenant: config.Tenant, rt: rt}
	httpClient := &http.Client{Transport: rt}
	c := ReadClient{
		client:  httpClient,
		timeout: time.Minute,
		config:  config,
	}

	for i, label := range config.ConstLabels {
                lkv := strings.Split(label,"=")
                config.ConstLabels[i] = fmt.Sprintf("%s=\"%s\"", lkv[0], lkv[1])
        }

	ticker := time.NewTicker(config.RequestInterval)
	quit := make(chan struct{})

	for {
		select {
			case <- ticker.C:
				runQueryBatch(config, c)
			case <- quit:
				ticker.Stop()
			return
		}
	}
	close(quit)


}

func runQueryBatch(config ConfigRead, c ReadClient) {

	var wg sync.WaitGroup
	queries := generateQueries(config.Size, config.ConstLabels)

        for q, group := range queries {

		queryTotal.WithLabelValues(group).Inc()
		wg.Add(1)
                go func(q string, group string, c ReadClient) {
			defer wg.Done()
			timer := prometheus.NewTimer(queryLatency.WithLabelValues(group))
                        defer timer.ObserveDuration()

                        bytes := do(q, c)
                        var data Response
                        json.Unmarshal(bytes, &data)
                        expectedValue := strings.Split(group, ":")[0]
                        if data.Data != nil && len(data.Data.Result) > 0 && len(data.Data.Result[0].Value) > 0 {
				fmt.Printf("%s gave %s, expected %s \n", q, data.Data.Result[0].Value[1], expectedValue)
                                if data.Data.Result[0].Value[1] == expectedValue {
					queryAccuracy.WithLabelValues(group).Inc()
                                }
                        } else {
                                queryFailures.WithLabelValues(group).Inc()
                        }
		} (q, group, c)

		wg.Wait()
	}
}

// Make a HTTP Get query and return result 
func do(query string, c ReadClient) []byte {

	u := c.config.URL
	q := u.Query()
	q.Set("query",query)
	u.RawQuery = q.Encode()
	resp, _  := c.client.Get(u.String())
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	return bodyBytes
}

// Generate query map of given size with query is  key and value is cardinality:timeRange:step 
func generateQueries(size int, labels []string) map[string]string {
        log.Printf("Generating queries \n")
        timestep := tstep
        total := 0
        timestep = tstep

	list := make(map[string]string )
        for t, s := range timestep {
                for k, v := range cdis {
                        q := qmap[k]
                        q = strings.Replace(q, "T", t, 1)
                        q = strings.Replace(q, "S", s, 1)
			q = strings.Replace(q, "C", strings.Join(labels, ","), 1)
                        num := int(math.Max(1.0 , (float64)(v*size/475) * tdis[t]))
                        for i := 0; i < num; i++ {
                                query := strings.Replace(q, "I", strconv.Itoa(i), 1)
				list[query] =  fmt.Sprintf("%d:%s:%s", k, t, s)
                        }
                        total += num
                }
        }
	return list
}
