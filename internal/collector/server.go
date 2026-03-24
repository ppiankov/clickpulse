package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	connections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_connections",
		Help: "Current number of TCP connections",
	})
	memoryTracking = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_memory_tracking_bytes",
		Help: "Current memory tracked by the server",
	})
	backgroundPoolTasks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_background_pool_tasks",
		Help: "Active tasks in the background merge/mutate pool",
	})
	insertedRowsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_inserted_rows_total",
		Help: "Cumulative rows inserted (from system.events)",
	})
	selectedRowsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_selected_rows_total",
		Help: "Cumulative rows read by SELECT (from system.events)",
	})
)

func init() {
	prometheus.MustRegister(connections, memoryTracking, backgroundPoolTasks, insertedRowsTotal, selectedRowsTotal)
}

// Server collects metrics from system.metrics and system.events.
type Server struct{}

func NewServer() *Server { return &Server{} }

func (s *Server) Name() string { return "server" }

func (s *Server) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// system.metrics — current gauges
	metricsMap := map[string]*prometheus.Gauge{
		"TCPConnection":                        &connections,
		"MemoryTracking":                       &memoryTracking,
		"BackgroundMergesAndMutationsPoolTask": &backgroundPoolTasks,
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
			(*g).Set(float64(value))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// system.events — cumulative counters
	eventsMap := map[string]*prometheus.Gauge{
		"InsertedRows": &insertedRowsTotal,
		"SelectedRows": &selectedRowsTotal,
	}

	rows2, err := q.QueryContext(ctx, `
		SELECT event, value
		FROM system.events
		WHERE event IN ('InsertedRows', 'SelectedRows')
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows2.Close() }()

	for rows2.Next() {
		var event string
		var value uint64
		if err := rows2.Scan(&event, &value); err != nil {
			return err
		}
		if g, ok := eventsMap[event]; ok {
			(*g).Set(float64(value))
		}
	}

	return rows2.Err()
}
