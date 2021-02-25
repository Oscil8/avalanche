package metrics

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/prompb"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type PipelineProbeConfig struct {
	ReadUrl  url.URL
	WriteUrl url.URL
	Interval time.Duration
	Timeout  time.Duration
}

type QueryResponse struct {
	Status    string
	Data      pipelineProbeData
	Error     string
	ErrorType string
}

type pipelineProbeData struct {
	ResultType string
	Result     []*PipelineProbeSeries
}

type PipelineProbeSeries struct {
	Metric map[string]string
	Values [][2]interface{}
}

// const maxErrMsgLen = 256

var PipelineProbeMetrics []string = []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"}

var (
	probeExecutionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prober_executions_total",
			Help: "counter for prober executions",
		},
		[]string{"probe"},
	)

	probeErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prober_errors_total",
			Help: "counter for prober execution errors",
		},
		[]string{"probe"},
	)

	pipelineLatencyTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pipeline_latency_time",
			Help:    "end to end piepline latency",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 40, 60, 120, 180, 240, 300},
		},
		[]string{"probe"},
	)

	pipelineLatencySummary = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "pipeline_latency_time_quantile",
			Help:       "end to end piepline latency quantile",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001, 0.999: 0.0001},
		},
		[]string{"probe"},
	)

	pipelineFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pipeline_failures",
			Help: "counter for pipeline failures",
		},
		[]string{"probe"},
	)
)

func convert_to_prom(PipelineProbeMetrics []string) []prompb.TimeSeries {

	var promseries []prompb.TimeSeries

	for i := range PipelineProbeMetrics {

		// time.Sleep(100 * time.Millisecond)

		// fmt.Printf("%v\n", PipelineProbeMetrics[i])

		currentTime := time.Now().UnixNano() / int64(time.Millisecond)

		sample := &prompb.Sample{
			Value:     float64(1.0),
			Timestamp: currentTime,
		}

		// fmt.Printf("%+v\n", *sample)

		label1 := &prompb.Label{
			Name:  "__name__",
			Value: PipelineProbeMetrics[i],
		}

		label2 := &prompb.Label{
			Name:  "isProber",
			Value: "true",
		}

		samples := make([]prompb.Sample, 1)
		samples[0] = *sample

		labels := make([]prompb.Label, 2)
		labels[0] = *label1
		labels[1] = *label2

		timeseries := &prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		}
		// fmt.Printf("%+v\n", timeseries)
		promseries = append(promseries, *timeseries)
	}
	// fmt.Printf("%+v\n", promseries)
	return promseries

}

func createPayload(promseries []prompb.TimeSeries) ([]byte, error) {

	// fmt.Printf("%+v\n", promseries)
	writeRequest := &prompb.WriteRequest{
		Timeseries: promseries,
	}

	// Convert the struct to a slice of bytes and then compress it.
	message, err := proto.Marshal(writeRequest)
	if err != nil {
		return nil, err
	}
	compressed := snappy.Encode(nil, message)

	return compressed, nil
}

func createWriteRequest(ctx context.Context, url string, payload []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		url,
		bytes.NewReader(payload),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Add("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")

	if err != nil {
		return nil, err
	}
	// fmt.Printf("request %+v\n", req)
	return req, nil
}

func remote_write(req *http.Request) error {

	var httpClient = &http.Client{
		Timeout: time.Second * 30,
	}
	resp, err := httpClient.Do(req)

	// fmt.Printf("%+v %+v\n", resp, err)

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 == 5 {
		return err
	}

	return err

}

func publish(ctx context.Context, PipelineProbeMetrics []string, config PipelineProbeConfig) error {

	promseries := convert_to_prom(PipelineProbeMetrics)
	payload, _ := createPayload(promseries)
	write_url := config.WriteUrl.String()
	req, _ := createWriteRequest(ctx, write_url, payload)
	return remote_write(req)

}

func queryAndRecord(ctx context.Context, metric string, config PipelineProbeConfig) error {

	Url := config.ReadUrl
	params := url.Values{}

	query := metric + "{isProber=\"true\"}[5m]"
	params.Set("query", query)
	Url.RawQuery = params.Encode()
	resp, err := pipelineProbeDo(ctx, "GET", Url.String(), nil)

	if err != nil {
		return fmt.Errorf("erros while querying %v\n%s", Url.String())
	}

	var response QueryResponse
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

	if latency > maxLatency {
		pipelineFailures.WithLabelValues("pipeline_prober").Inc()
	} else {

		pipelineLatencyTime.WithLabelValues("pipeline_prober").Observe(latency)
		pipelineLatencySummary.WithLabelValues("pipeline_prober").Observe(latency)
	}

	return err

}

func pipelineProbeDo(ctx context.Context, method string, url string, body io.Reader) ([]byte, error) {

	// fmt.Printf("Hi!!!!\n")

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	// fmt.Printf("Request: %v\n", url)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("response %v\n", resp)
	defer resp.Body.Close()

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return d, fmt.Errorf("reuqest failed with htpp %s", resp.Status)
	}

	return d, nil

}

func PipelineProbe(ctx context.Context, config PipelineProbeConfig) {

	ProbeLoop(ctx, PipelineProbeMetrics, config)

}

func ProbeLoop(ctx context.Context, PipelineProbeMetrics []string, config PipelineProbeConfig) {

	interval := int64(config.Interval / time.Second)

	for {

		a := time.Duration(rand.Int63n(int64(interval*1000))) * time.Millisecond
		select {

		case <-ctx.Done():
			return

		case <-time.After(a):
			pctx, cancel := context.WithTimeout(ctx, config.Timeout)
			defer cancel()
			probename := "pipeline_prober"
			probeExecutionsTotal.WithLabelValues(probename).Inc()
			if err := Probe(pctx, PipelineProbeMetrics, config); err != nil {
				probeErrorsTotal.WithLabelValues(probename).Inc()
			}
		}

	}

}

func Probe(pctx context.Context, PipelineProbeMetrics []string, config PipelineProbeConfig) error {

	var wg sync.WaitGroup
	wg.Add(1)
	go func(PipelineProbeMetrics []string) {

		defer wg.Done()
		err := publish(pctx, PipelineProbeMetrics, config)
		if err != nil {
			glog.Fatalf("failed to publish PipelineProbeMetrics: %s", err)
		}
	}(PipelineProbeMetrics)

	for _, m := range PipelineProbeMetrics {
		wg.Add(1)
		go func(metric string) {
			defer wg.Done()
			queryAndRecord(pctx, metric, config)
		}(m)
	}

	wg.Wait()
	return nil

}
