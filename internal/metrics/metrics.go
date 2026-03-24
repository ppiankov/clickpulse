package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// Scrape health — per node
	Up = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_up",
		Help: "1 if ClickHouse is reachable, 0 otherwise",
	}, []string{"node"})
	ScrapeDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_scrape_duration_seconds",
		Help: "Time taken to collect metrics",
	}, []string{"node"})
	ScrapeErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_scrape_errors_total",
		Help: "Cumulative scrape error count",
	}, []string{"node"})
)

func init() {
	prometheus.MustRegister(Up, ScrapeDuration, ScrapeErrors)
}
