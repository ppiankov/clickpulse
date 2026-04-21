package collector

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	eventInsertedRows                = "InsertedRows"
	eventSelectedRows                = "SelectedRows"
	eventReplicatedPartFailedFetches = "ReplicatedPartFailedFetches"
	eventReplicatedPartChecksFailed  = "ReplicatedPartChecksFailed"
	eventReplicatedDataLoss          = "ReplicatedDataLoss"
)

var (
	connections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_connections",
		Help: "Current number of TCP connections",
	}, []string{"node"})
	memoryTracking = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_memory_tracking_bytes",
		Help: "Current memory tracked by the server",
	}, []string{"node"})
	backgroundPoolTasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_background_pool_tasks",
		Help: "Active tasks in the background merge/mutate pool",
	}, []string{"node"})
	insertedRowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_inserted_rows_total",
		Help: "Cumulative rows inserted (from system.events)",
	}, []string{"node"})
	selectedRowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_selected_rows_total",
		Help: "Cumulative rows read by SELECT (from system.events)",
	}, []string{"node"})
	replicatedPartFailedFetchesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_part_failed_fetches_total",
		Help: "Cumulative failed replicated part fetches (from system.events)",
	}, []string{"node"})
	replicatedPartChecksFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_part_checks_failed_total",
		Help: "Cumulative failed replicated part checks (from system.events)",
	}, []string{"node"})
	replicatedDataLossTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_replicated_data_loss_total",
		Help: "Cumulative replicated data loss events (from system.events)",
	}, []string{"node"})
)

func init() {
	prometheus.MustRegister(
		connections,
		memoryTracking,
		backgroundPoolTasks,
		insertedRowsTotal,
		selectedRowsTotal,
		replicatedPartFailedFetchesTotal,
		replicatedPartChecksFailedTotal,
		replicatedDataLossTotal,
	)
}

// Server collects metrics from system.metrics and system.events.
type Server struct {
	eventCounters *cumulativeEventCounters
}

func NewServer() *Server {
	return &Server{
		eventCounters: newCumulativeEventCounters(systemEventCounters),
	}
}

func (s *Server) Name() string { return "server" }

func (s *Server) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metricsMap := map[string]*prometheus.GaugeVec{
		"TCPConnection":                        connections,
		"MemoryTracking":                       memoryTracking,
		"BackgroundMergesAndMutationsPoolTask": backgroundPoolTasks,
	}

	rows, err := q.QueryContext(ctx, `
		SELECT metric, value
		FROM system.metrics
		WHERE metric IN ('TCPConnection', 'MemoryTracking', 'BackgroundMergesAndMutationsPoolTask')
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var metric string
		var value int64
		if err := rows.Scan(&metric, &value); err != nil {
			return err
		}
		if g, ok := metricsMap[metric]; ok {
			g.WithLabelValues(node).Set(float64(value))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	rows2, err := q.QueryContext(ctx, `
		SELECT event, value
		FROM system.events
		WHERE event IN (
			'InsertedRows',
			'SelectedRows',
			'ReplicatedPartFailedFetches',
			'ReplicatedPartChecksFailed',
			'ReplicatedDataLoss'
		)
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows2.Close() }()

	eventCounters := s.eventCounters
	if eventCounters == nil {
		eventCounters = newCumulativeEventCounters(systemEventCounters)
		s.eventCounters = eventCounters
	}

	for rows2.Next() {
		var event string
		var value uint64
		if err := rows2.Scan(&event, &value); err != nil {
			return err
		}
		eventCounters.Observe(node, event, value)
	}

	return rows2.Err()
}

var systemEventCounters = map[string]*prometheus.CounterVec{
	eventInsertedRows:                insertedRowsTotal,
	eventSelectedRows:                selectedRowsTotal,
	eventReplicatedPartFailedFetches: replicatedPartFailedFetchesTotal,
	eventReplicatedPartChecksFailed:  replicatedPartChecksFailedTotal,
	eventReplicatedDataLoss:          replicatedDataLossTotal,
}

type cumulativeEventCounters struct {
	mu       sync.Mutex
	counters map[string]*prometheus.CounterVec
	previous map[string]map[string]uint64
}

func newCumulativeEventCounters(counters map[string]*prometheus.CounterVec) *cumulativeEventCounters {
	return &cumulativeEventCounters{
		counters: counters,
		previous: make(map[string]map[string]uint64),
	}
}

func (c *cumulativeEventCounters) Observe(node, event string, value uint64) {
	counter, ok := c.counters[event]
	if !ok {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	nodeEvents, ok := c.previous[node]
	if !ok {
		nodeEvents = make(map[string]uint64)
		c.previous[node] = nodeEvents
	}

	previous, seen := nodeEvents[event]
	nodeEvents[event] = value
	if !seen || value <= previous {
		return
	}

	counter.WithLabelValues(node).Add(float64(value - previous))
}
