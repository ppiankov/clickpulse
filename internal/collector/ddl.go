package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ddlQueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_ddl_queue_size",
		Help: "Number of entries in the distributed DDL queue",
	})
	ddlQueueStuck = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_ddl_queue_stuck",
		Help: "Number of DDL entries that appear stuck (not finished, older than threshold)",
	})
	ddlOldestEntry = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_ddl_oldest_entry_seconds",
		Help: "Age of the oldest pending DDL entry in seconds",
	})
)

func init() {
	prometheus.MustRegister(ddlQueueSize, ddlQueueStuck, ddlOldestEntry)
}

const ddlStuckThreshold = 5 * time.Minute

// DDL collects metrics from system.distributed_ddl_queue.
type DDL struct{}

func NewDDL() *DDL { return &DDL{} }

func (d *DDL) Name() string { return "ddl" }

func (d *DDL) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if the table exists (only available in clustered setups).
	var exists int
	if err := q.QueryRowContext(ctx, `
		SELECT count()
		FROM system.tables
		WHERE database = 'system' AND name = 'distributed_ddl_queue'
	`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil // Not a cluster setup
	}

	rows, err := q.QueryContext(ctx, `
		SELECT
			status,
			initiator,
			entry_time
		FROM system.distributed_ddl_queue
		WHERE status != 'Finished'
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var total, stuck int
	var oldestAge float64
	now := time.Now()

	for rows.Next() {
		var status, initiator string
		var entryTime time.Time
		if err := rows.Scan(&status, &initiator, &entryTime); err != nil {
			return err
		}

		total++
		age := now.Sub(entryTime).Seconds()
		if age > oldestAge {
			oldestAge = age
		}
		if age > ddlStuckThreshold.Seconds() {
			stuck++
		}
	}

	ddlQueueSize.Set(float64(total))
	ddlQueueStuck.Set(float64(stuck))
	ddlOldestEntry.Set(oldestAge)

	return rows.Err()
}
