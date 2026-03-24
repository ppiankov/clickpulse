package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// Scrape health
	Up = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_up",
		Help: "1 if ClickHouse is reachable, 0 otherwise",
	})
	ScrapeDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_scrape_duration_seconds",
		Help: "Time taken to collect metrics",
	})
	ScrapeErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_scrape_errors_total",
		Help: "Cumulative scrape error count",
	})
)

func init() {
	prometheus.MustRegister(Up, ScrapeDuration, ScrapeErrors)
}
