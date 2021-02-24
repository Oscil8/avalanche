# avalanche

Avalanche serves a text-based [Prometheus metrics](https://prometheus.io/docs/instrumenting/exposition_formats/) endpoint for load testing [Prometheus](https://prometheus.io/) and possibly other [OpenMetrics](https://github.com/OpenObservability/OpenMetrics) consumers.

Avalanche also supports load testing for services accepting data via Prometheus remote_write API such as [Thanos](https://github.com/improbable-eng/thanos), [Cortex](https://github.com/weaveworks/cortex), [M3DB](https://m3db.github.io/m3/integrations/prometheus/), [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics/) and other services [listed here](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage).

Metric names and unique series change over time to simulate series churn.

Checkout the [blog post](https://blog.freshtracks.io/load-testing-prometheus-metric-ingestion-5b878711711c).

## configuration flags 
```bash 
avalanche --help
```

## run Docker image

```bash
docker run quay.io/freshtracks.io/avalanche --help
```

## Endpoints

Two endpoints are available :
* `port/metrics` - metrics endpoint
* `2112/stats` - stats endpoint
* `port/health` - healthcheck endpoint

## build and run go binary
### Do Go setup before this
```bash
mkdir -p $GOROOT/github.com/open-fresh
cd $GOROOT/github.com/open-fresh
git clone https://github.com/mnottheone/avalanche.git
go install github.com/open-fresh/avalanche/cmd/...
cmd --help

## Example 
cmd --remote-url=http://host:port/api/v1/remote_write --remote-read-url=http://host:port/api/v1/query --metric-count=10 --label-count=1 --series-count=100 --value-interval=10 --series-interval=360000 --metric-interval=360000 --remote-requests-count=100000 --remote-write-interval=1000ms --const-label=instance=local --remote-read-batch-size=50 --remote-read-requests-count=500 --remote-read-interval=10s

```
