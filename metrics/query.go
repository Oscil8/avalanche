package metrics

import (
        "fmt"
        "math"
        "strings"
        "strconv"

	"github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promauto"
)

// queries map with cardinality as key aa well as value should be cardinality
var qmap  = map[int]string{10: "avg_over_time(count(avg(avalanche_metric_mmmmm_._I{series_id=~\".\"}) by(series_id))[T:S])", 100: "avg_over_time(count(avg(avalanche_metric_mmmmm_._I{series_id=~\".\"}) by(series_id))[T:S])", 1000: "avg_over_time(count(avg(avalanche_metric_mmmmm_._I{series_id=~\".\"}) by(series_id))[T:S])", 10000: "avg_over_time(count(avg({series_id=~\".\", __name__ =~\"avalanche_metric_mmmmm_._.\"}) by(series_id))[T:S])", 100000: "avg_over_time(count(avg({series_id=~\".\", __name__ =~\"avalanche_metric_mmmmm_._..\"}) by(series_id))[T:S])", 500000: "avg_over_time(count(avg({series_id=~\".\", __name__ =~\"avalanche_metric_mmmmm_._...\"}) by(series_id))[T:S])"}


var tstep =  map[string]string{"2h": "10s", "24h": "1m", "7d":"10m", "30d":"10m"}
var tdis = map[string]float64{"2h": 0.5, "24h": 0.35, "7d": 0.1, "30d": 0.05}
var cdis = map[int]int{10: 200, 100: 200, 1000: 50, 10000: 10, 100000: 10, 500000: 5}

var queryTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "promql_query_total",
	Help: "The total number of query generated",
})

func Query(mode string, Total int) {
        // N*M means M queries with N cardinality
        //if 3 != len(os.Args)  {
        //        fmt.Println("Usage:", os.Args[0], "MODE", "Total")
        //        return
        //}
        //mode := os.Args[1]
        //Total, _ := strconv.Atoi(os.Args[2])
        //Total := 100
        fmt.Printf("Generating queries \n")
        timestep := tstep
        total := 0
        if mode == "adhoc" {
                // choose a random key from tstep map
                timestep = tstep
        }
        for t, s := range timestep {
                for k, v := range cdis {
                        q := qmap[k]
                        q = strings.Replace(q, "T", t, 1)
                        q = strings.Replace(q, "S", s, 1)
                        num := int(math.Max(1.0 , (float64)(v*Total/475) * tdis[t]))
                        fmt.Printf("\n\n###  (%d X %d) [%s:%s]\n", num, k, t, s)
                        for i := 0; i < num; i++ {
                                query := strings.Replace(q, "I", strconv.Itoa(i), 1)
                                queryTotal.Inc()
				fmt.Println(query)
                        }
                        total += num
                }
        }
        fmt.Printf(" Total %d", total)
}
