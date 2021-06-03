package metrics

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/open-fresh/avalanche/pkg/download"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	"github.com/open-fresh/avalanche/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/tcnksm/go-httpstat"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
)

const maxErrMsgLen = 256

var (
	userAgent = "avalanche"

	writeTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "write_request_total",
			Help: "The total number of write requests",
		},
	)

	writeFailures = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "write_request_failures",
			Help: "The total number of write failures",
		},
	)

	writeResponseCodes = promauto.NewCounterVec(
                prometheus.CounterOpts{
                        Name: "write_request_responses",
                        Help: "response codes of write requests",
                },
		[]string{"code"},
        )

	writeLatency = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name:       "write_request_durations",
			Help:       "Write requests latencies in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001, 0.999: 0.0001},
		},
	)
	writeLatencyTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "write_request_latency",
			Help:    "write request latency histogram",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 20, 40, 60, 300},
		},
	)
	samplesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "write_samples_total",
			Help: "The total number of sample ingested",
		},
	)
)

func init() {
	prometheus.MustRegister(writeLatency)
	//prometheus.MustRegister(writeLatencyTime)
}

// ConfigWrite for the remote write requests.
type ConfigWrite struct {
	URL             url.URL
	RequestInterval time.Duration
	BatchSize,
	RequestCount int
	UpdateNotify chan struct{}
	PprofURLs    []*url.URL
	Tenant       string
}

// Client for the remote write requests.
type Client struct {
	client  *http.Client
	timeout time.Duration
	config  ConfigWrite
}

// SendRemoteWrite initializes a http client and
// sends metrics to a prometheus compatible remote endpoint.
func SendRemoteWrite(config ConfigWrite) error {
	var rt http.RoundTripper = &http.Transport{
		MaxIdleConns: 1000,
		MaxIdleConnsPerHost: 1000,
	}
	rt = &cortexTenantRoundTripper{tenant: config.Tenant, rt: rt}
	httpClient := &http.Client{Transport: otelhttp.NewTransport(rt)}

	c := Client{
		client:  httpClient,
		timeout: time.Minute,
		config:  config,
	}
	return c.write()
}

// Add the tenant ID header required by Cortex
func (rt *cortexTenantRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = cloneRequest(req)
	req.Header.Set("X-Scope-OrgID", rt.tenant)
	return rt.rt.RoundTrip(req)
}

type cortexTenantRoundTripper struct {
	tenant string
	rt     http.RoundTripper
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request) *http.Request {
	// Shallow copy of the struct.
	r2 := new(http.Request)
	*r2 = *r
	// Deep copy of the Header.
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	return r2
}

func (c *Client) write() error {

	tss, err := collectMetrics()
	if err != nil {
		return err
	}

	var (
		totalTime       time.Duration
		totalSamplesExp = len(tss) * c.config.RequestCount
		totalSamplesAct int
		mtx             sync.Mutex
		wgMetrics       sync.WaitGroup
		wgPprof         sync.WaitGroup
		merr            = &errors.MultiError{}
	)

	log.Printf("Sending:  %v timeseries, %v samples, %v timeseries per request, %v delay between requests\n", len(tss), c.config.RequestCount, c.config.BatchSize, c.config.RequestInterval)
	ticker := time.NewTicker(c.config.RequestInterval)
	defer ticker.Stop()
	for ii := 0; ii < c.config.RequestCount; ii++ {
		// Download the pprofs during half of the iteration to get avarege readings.
		// Do that only when it is not set to take profiles at a given interval.
		if len(c.config.PprofURLs) > 0 && ii == c.config.RequestCount/2 {
			wgPprof.Add(1)
			go func() {
				download.URLs(c.config.PprofURLs, time.Now().Format("2-Jan-2006-15:04:05"))
				wgPprof.Done()
			}()
		}
		<-ticker.C
		select {
		case <-c.config.UpdateNotify:
			log.Println("updating remote write metrics")
			tss, err = collectMetrics()
			if err != nil {
				merr.Add(err)
			}
		default:
			tss = updateTimetamps(tss)
		}

		start := time.Now()
		for i := 0; i < len(tss); i += c.config.BatchSize {
			wgMetrics.Add(1)
			go func(i int) {
				defer wgMetrics.Done()
				end := i + c.config.BatchSize
				if end > len(tss) {
					end = len(tss)
				}
				req := &prompb.WriteRequest{
					Timeseries: tss[i:end],
				}
				start := time.Now()
				writeTotal.Inc()
				timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
					us := v * 1000
					writeLatency.Observe(us)
				}))
				defer timer.ObserveDuration()

				err := c.Store(context.TODO(), req)
				if err != nil {
					writeFailures.Inc()
					merr.Add(err)
					return
				}
				writeLatencyTime.Observe(time.Since(start).Seconds())
				mtx.Lock()
				totalSamplesAct += len(tss[i:end])
				samplesTotal.Add(float64(len(tss[i:end])))
				mtx.Unlock()

			}(i)
		}
		wgMetrics.Wait()
		totalTime += time.Since(start)
		if merr.Count() > 20 {
			merr.Add(fmt.Errorf("too many errors"))
		}
	}
	wgPprof.Wait()
	if c.config.RequestCount*len(tss) != totalSamplesAct {
		merr.Add(fmt.Errorf("total samples mismatch, exp:%v , act:%v", totalSamplesExp, totalSamplesAct))
	}
	log.Printf("Total request time: %v ; Total samples: %v; Samples/sec: %v\n", totalTime.Round(time.Second), totalSamplesAct, int(float64(totalSamplesAct)/totalTime.Seconds()))
	return merr.Err()
}

func updateTimetamps(tss []prompb.TimeSeries) []prompb.TimeSeries {
	t := int64(model.Now())
	for i := range tss {
		tss[i].Samples[0].Timestamp = t
	}
	return tss
}

func collectMetrics() ([]prompb.TimeSeries, error) {
	metricsMux.Lock()
	defer metricsMux.Unlock()
	metricFamilies, err := promRegistry.Gather()
	if err != nil {
		return nil, err
	}
	return ToTimeSeriesSlice(metricFamilies), nil
}

// ToTimeSeriesSlice converts a slice of metricFamilies containing samples into a slice of TimeSeries
func ToTimeSeriesSlice(metricFamilies []*dto.MetricFamily) []prompb.TimeSeries {
	tss := make([]prompb.TimeSeries, 0, len(metricFamilies)*10)
	timestamp := int64(model.Now()) // Not using metric.TimestampMs because it is (always?) nil. Is this right?

	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.Metric {
			labels := prompbLabels(*metricFamily.Name, metric.Label)
			ts := prompb.TimeSeries{
				Labels: labels,
			}
			switch *metricFamily.Type {
			case dto.MetricType_COUNTER:
				ts.Samples = []prompb.Sample{{
					Value:     *metric.Counter.Value,
					Timestamp: timestamp,
				}}
			case dto.MetricType_GAUGE:
				ts.Samples = []prompb.Sample{{
					Value:     *metric.Gauge.Value,
					Timestamp: timestamp,
				}}
			}
			tss = append(tss, ts)
		}
	}

	return tss
}

func prompbLabels(name string, label []*dto.LabelPair) []prompb.Label {
	ret := make([]prompb.Label, 0, len(label)+1)
	ret = append(ret, prompb.Label{
		Name:  model.MetricNameLabel,
		Value: name,
	})
	for _, pair := range label {
		ret = append(ret, prompb.Label{
			Name:  *pair.Name,
			Value: *pair.Value,
		})
	}
	sort.Slice(ret, func(i int, j int) bool {
		return ret[i].Name < ret[j].Name
	})
	return ret
}

// Store sends a batch of samples to the HTTP endpoint.
func (c *Client) Store(ctx context.Context, req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	ctx = baggage.ContextWithValues(ctx,
		attribute.String("username", "donuts"),
	)

	tr := otel.Tracer("avalanche/client")
	ctx, span := tr.Start(ctx, "publish", trace.WithAttributes(semconv.PeerServiceKey.String("StoreService")))
	defer span.End()

	ctx = httptrace.WithClientTrace(ctx, otelhttptrace.NewClientTrace(ctx))


	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.config.URL.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	var result httpstat.Result
	ctx = httpstat.WithHTTPStat(httpReq.Context(), &result)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	writeResponseCodes.WithLabelValues(strconv.Itoa(httpResp.StatusCode)).Inc()
	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
		fmt.Printf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}

	result.End(time.Now())

	// Show results
	log.Printf("%+v", result)
	return err
}
