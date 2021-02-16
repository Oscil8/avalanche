package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/open-fresh/avalanche/metrics"
	"github.com/open-fresh/avalanche/pkg/download"
        "github.com/prometheus/client_golang/prometheus/promhttp"	
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	metricCount         = kingpin.Flag("metric-count", "Number of metrics to serve.").Default("500").Int()
	labelCount          = kingpin.Flag("label-count", "Number of labels per-metric.").Default("10").Int()
	seriesCount         = kingpin.Flag("series-count", "Number of series per-metric.").Default("10").Int()
	metricLength        = kingpin.Flag("metricname-length", "Modify length of metric names.").Default("5").Int()
	labelLength         = kingpin.Flag("labelname-length", "Modify length of label names.").Default("5").Int()
	constLabels         = kingpin.Flag("const-label", "Constant label to add to every metric. Format is labelName=labelValue. Flag can be specified multiple times.").Strings()
	valueInterval       = kingpin.Flag("value-interval", "Change series values every {interval} seconds.").Default("30").Int()
	labelInterval       = kingpin.Flag("series-interval", "Change series_id label values every {interval} seconds.").Default("60").Int()
	metricInterval      = kingpin.Flag("metric-interval", "Change __name__ label values every {interval} seconds.").Default("120").Int()
	port                = kingpin.Flag("port", "Port to serve at").Default("9001").Int()
	remoteURL           = kingpin.Flag("remote-url", "URL to send samples via remote_write API.").URL()
	remoteReadURL           = kingpin.Flag("remote-read-url", "URL to read samples via remote_write API.").URL()
	remotePprofURLs     = kingpin.Flag("remote-pprof-urls", "a list of urls to download pprofs during the remote write: --remote-pprof-urls=http://127.0.0.1:10902/debug/pprof/heap --remote-pprof-urls=http://127.0.0.1:10902/debug/pprof/profile").URLList()
	remotePprofInterval = kingpin.Flag("remote-pprof-interval", "how often to download pprof profiles.When not provided it will download a profile once before the end of the test.").Duration()
	remoteBatchSize     = kingpin.Flag("remote-batch-size", "how many samples to send with each remote_write API request.").Default("2000").Int()
	remoteRequestCount  = kingpin.Flag("remote-requests-count", "how many requests to send in total to the remote_write API.").Default("100").Int()
	remoteReqsInterval  = kingpin.Flag("remote-write-interval", "delay between each remote write request.").Default("100ms").Duration()
	remoteReadBatchSize     = kingpin.Flag("remote-read-batch-size", "how many queries to make with each remote_read API request batch.").Default("50").Int()
	remoteReadRequestCount  = kingpin.Flag("remote-read-requests-count", "how many batch of read requests to make in total to the remote_query API.").Default("100").Int()
	remoteReadReqsInterval  = kingpin.Flag("remote-read-interval", "delay between each batch of remote query requests.").Default("30s").Duration()
	remoteTenant        = kingpin.Flag("remote-tenant", "Tenant ID to include in remote_write send").Default("0").String()

)

func Serve() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()
}

func Query() {
	go func() {
			readConfig := metrics.ConfigRead{
			URL:             **remoteReadURL,
			RequestInterval: *remoteReadReqsInterval,
			Size:            *remoteReadBatchSize,
			RequestCount:    *remoteReadRequestCount,
			Tenant:          *remoteTenant,
			ConstLabels:	 *constLabels,
	}
		metrics.Query(readConfig)
	}()
}

func main() {
	kingpin.Version("0.3")
	log.SetFlags(log.Ltime | log.Lshortfile) // Show file name and line in logs.
	kingpin.CommandLine.Help = "avalanche - metrics test server"
	kingpin.Parse()

	stop := make(chan struct{})
	defer close(stop)
	updateNotify, err := metrics.RunMetrics(*metricCount, *labelCount, *seriesCount, *metricLength, *labelLength, *valueInterval, *labelInterval, *metricInterval, *constLabels, stop)
	if err != nil {
		log.Fatal(err)
	}

	// Start Prometheus Exposing metrics
	Serve()
	
	// Start Query thread
	Query()

	if *remoteURL != nil {
		if (**remoteURL).Host == "" || (**remoteURL).Scheme == "" {
			log.Fatal("remote host and scheme can't be empty")
		}
		if *remoteBatchSize <= 0 {
			log.Fatal("remote send batch size should be more than zero")
		}

		config := metrics.ConfigWrite{
			URL:             **remoteURL,
			RequestInterval: *remoteReqsInterval,
			BatchSize:       *remoteBatchSize,
			RequestCount:    *remoteRequestCount,
			UpdateNotify:    updateNotify,
			Tenant:          *remoteTenant,
		}

		// Collect Pprof during the write only if not collecting within a regular interval.
		if *remotePprofInterval == 0 {
			config.PprofURLs = *remotePprofURLs
		}

		var (
			wg   sync.WaitGroup
			done = make(chan struct{})
		)
		if *remotePprofInterval > 0 {
			if len(*remotePprofURLs) == 0 {
				log.Fatal("remote profiling interval specified wihout any remote pprof urls")
			}
			rand.Seed(time.Now().UnixNano())
			suffix := rand.Intn(1000)
			go func() {
				ticker := time.NewTicker(*remotePprofInterval)
				var dur time.Duration
			loop:
				for {
					select {
					case <-ticker.C:
						select {
						case <-done: // Prevents a panic when calling wg.Add(1) after calling wg.Wait().
							break loop
						default:
						}
						dur += *remotePprofInterval
						wg.Add(1)
						download.URLs(*remotePprofURLs, strconv.Itoa(suffix)+"-"+dur.String())
						wg.Done()
					}
				}
			}()

		}
		// First cut: just send the metrics once then exit
		err := metrics.SendRemoteWrite(config)
		if err != nil {
			log.Fatal(err)
		}
		if *remotePprofInterval > 0 {
			done <- struct{}{}
			wg.Wait()
		}
		return
	}

	
	fmt.Printf("Serving ur metrics at localhost:%v/metrics\n", *port)
	err = metrics.ServeMetrics(*port)
	if err != nil {
		log.Fatal(err)
	}
}


