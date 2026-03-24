package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	replicaQueueSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replica_queue_size",
		Help: "Replication queue size per table",
	}, []string{"database", "table"})
	replicaInsertsInQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_replica_inserts_in_queue",
		Help: "Total insert operations waiting in replication queues",
	})
	replicaLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_replica_lag_seconds",
		Help: "Maximum absolute delay across all replicated tables",
	})
	replicaReadonly = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replica_readonly",
		Help: "1 if the replica is in read-only mode",
	}, []string{"database", "table"})
	replicasTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_replicas_total",
		Help: "Total number of replicated tables",
	})
)

func init() {
	prometheus.MustRegister(replicaQueueSize, replicaInsertsInQueue, replicaLag, replicaReadonly, replicasTotal)
}

// Replication collects metrics from system.replicas.
type Replication struct{}

func NewReplication() *Replication { return &Replication{} }

func (r *Replication) Name() string { return "replication" }

func (r *Replication) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := q.QueryContext(ctx, `
		SELECT
			database,
			table,
			queue_size,
			inserts_in_queue,
			absolute_delay,
			is_readonly
		FROM system.replicas
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var total int
	var totalInserts int64
	var maxLag float64

	replicaQueueSize.Reset()
	replicaReadonly.Reset()

	for rows.Next() {
		var database, table string
		var queueSize, insertsInQueue int64
		var absDelay uint64
		var isReadonly uint8
		if err := rows.Scan(&database, &table, &queueSize, &insertsInQueue, &absDelay, &isReadonly); err != nil {
			return err
		}

		total++
		totalInserts += insertsInQueue

		lag := float64(absDelay)
		if lag > maxLag {
			maxLag = lag
		}

		replicaQueueSize.WithLabelValues(database, table).Set(float64(queueSize))
		replicaReadonly.WithLabelValues(database, table).Set(float64(isReadonly))
	}

	replicasTotal.Set(float64(total))
	replicaInsertsInQueue.Set(float64(totalInserts))
	replicaLag.Set(maxLag)

	return rows.Err()
}
