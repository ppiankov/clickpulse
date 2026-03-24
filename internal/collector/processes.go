package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeQueries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_active_queries",
		Help: "Number of currently executing queries",
	})
	queriesByType = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_queries_by_type",
		Help: "Active queries grouped by type (SELECT, INSERT, ALTER, etc)",
	}, []string{"type"})
	slowQueries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_slow_queries",
		Help: "Number of currently running slow queries",
	})
	longestQuery = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_longest_query_seconds",
		Help: "Duration of the longest running query in seconds",
	})
	queryMemory = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_query_memory_bytes",
		Help: "Total memory used by all running queries",
	})
	waitingQueries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_waiting_queries",
		Help: "Number of queries waiting for locks or resources",
	})
)

func init() {
	prometheus.MustRegister(activeQueries, queriesByType, slowQueries, longestQuery, queryMemory, waitingQueries)
}

// Processes collects metrics from system.processes.
type Processes struct {
	slowThreshold time.Duration
}

// NewProcesses creates a processes collector with the given slow query threshold.
func NewProcesses(slowThreshold time.Duration) *Processes {
	return &Processes{slowThreshold: slowThreshold}
}

func (p *Processes) Name() string { return "processes" }

func (p *Processes) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			query_kind,
			elapsed,
			memory_usage,
			is_initial_query
		FROM system.processes
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var total, slow, waiting int
	var maxElapsed float64
	var totalMem int64
	typeCounts := make(map[string]int)

	for rows.Next() {
		var queryKind string
		var elapsed float64
		var memUsage int64
		var isInitial uint8
		if err := rows.Scan(&queryKind, &elapsed, &memUsage, &isInitial); err != nil {
			return err
		}

		total++
		totalMem += memUsage

		if queryKind == "" {
			queryKind = "other"
		}
		typeCounts[queryKind]++

		if elapsed > maxElapsed {
			maxElapsed = elapsed
		}
		if elapsed > p.slowThreshold.Seconds() {
			slow++
		}
		if isInitial == 0 {
			waiting++
		}
	}

	activeQueries.Set(float64(total))
	slowQueries.Set(float64(slow))
	longestQuery.Set(maxElapsed)
	queryMemory.Set(float64(totalMem))
	waitingQueries.Set(float64(waiting))

	queriesByType.Reset()
	for kind, count := range typeCounts {
		queriesByType.WithLabelValues(kind).Set(float64(count))
	}

	return rows.Err()
}
