package collector

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryRegressions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_query_regressions",
		Help: "Number of queries whose mean time regressed above threshold since last poll",
	}, []string{"node"})
	queryMeanTimeChangeRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_query_mean_time_change_ratio",
		Help: "Ratio of current mean query time to previous mean (>1 = slower)",
	}, []string{"node", "query_hash"})
	queryCallsDelta = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_query_calls_delta",
		Help: "Change in call count since last poll",
	}, []string{"node", "query_hash"})
)

func init() {
	prometheus.MustRegister(queryRegressions, queryMeanTimeChangeRatio, queryCallsDelta)
}

type queryStats struct {
	calls    int64
	meanTime float64
}

// QueryLog detects query regressions by comparing mean times across poll cycles.
type QueryLog struct {
	threshold float64
	limit     int
	mu        sync.Mutex
	prev      map[string]map[string]queryStats // keyed by node, then query_hash
}

// NewQueryLog creates a query log collector.
func NewQueryLog(threshold float64, limit int) *QueryLog {
	return &QueryLog{
		threshold: threshold,
		limit:     limit,
		prev:      make(map[string]map[string]queryStats),
	}
}

func (q *QueryLog) Name() string { return "querylog" }

func (q *QueryLog) Collect(querier Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := querier.QueryContext(ctx, `
		SELECT
			normalized_query_hash,
			count()          AS calls,
			avg(query_duration_ms) / 1000.0 AS mean_seconds
		FROM system.query_log
		WHERE type = 'QueryFinish'
		  AND event_date = today()
		  AND query_kind = 'Select'
		GROUP BY normalized_query_hash
		ORDER BY mean_seconds DESC
		LIMIT ?
	`, q.limit)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	current := make(map[string]queryStats)
	for rows.Next() {
		var hash string
		var calls int64
		var meanSec float64
		if err := rows.Scan(&hash, &calls, &meanSec); err != nil {
			return err
		}
		current[hash] = queryStats{calls: calls, meanTime: meanSec}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	prevNode := q.prev[node]
	regressionCount := 0

	for hash, cur := range current {
		if prevNode == nil {
			continue
		}
		prev, ok := prevNode[hash]
		if !ok || prev.meanTime == 0 {
			continue
		}

		ratio := cur.meanTime / prev.meanTime
		queryMeanTimeChangeRatio.WithLabelValues(node, hash).Set(ratio)
		queryCallsDelta.WithLabelValues(node, hash).Set(float64(cur.calls - prev.calls))

		if ratio >= q.threshold {
			regressionCount++
		}
	}

	queryRegressions.WithLabelValues(node).Set(float64(regressionCount))
	q.prev[node] = current

	return nil
}
