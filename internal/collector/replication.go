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
	}, []string{"node", "database", "table"})
	replicaInsertsInQueue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replica_inserts_in_queue",
		Help: "Total insert operations waiting in replication queues",
	}, []string{"node"})
	replicaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replica_lag_seconds",
		Help: "Maximum absolute delay across all replicated tables",
	}, []string{"node"})
	replicaReadonly = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replica_readonly",
		Help: "1 if the replica is in read-only mode",
	}, []string{"node", "database", "table"})
	replicasTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replicas_total",
		Help: "Total number of replicated tables",
	}, []string{"node"})
)

type replicaTableKey struct {
	database string
	table    string
}

type replicaTableMetrics struct {
	queueSize  int64
	isReadonly uint8
}

func init() {
	prometheus.MustRegister(replicaQueueSize, replicaInsertsInQueue, replicaLag, replicaReadonly, replicasTotal)
}

// Replication collects metrics from system.replicas.
type Replication struct {
	tables seriesTracker[replicaTableKey]
}

func NewReplication() *Replication { return &Replication{} }

func (r *Replication) Name() string { return "replication" }

func (r *Replication) Collect(q Querier, node string) error {
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
	currentTables := make(map[replicaTableKey]replicaTableMetrics)

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

		currentTables[replicaTableKey{database: database, table: table}] = replicaTableMetrics{
			queueSize:  queueSize,
			isReadonly: isReadonly,
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	replicasTotal.WithLabelValues(node).Set(float64(total))
	replicaInsertsInQueue.WithLabelValues(node).Set(float64(totalInserts))
	replicaLag.WithLabelValues(node).Set(maxLag)
	r.recordReplicaTables(node, currentTables)

	return nil
}

func (r *Replication) recordReplicaTables(node string, currentTables map[replicaTableKey]replicaTableMetrics) {
	current := make(map[replicaTableKey]struct{}, len(currentTables))
	for key, metrics := range currentTables {
		replicaQueueSize.WithLabelValues(node, key.database, key.table).Set(float64(metrics.queueSize))
		replicaReadonly.WithLabelValues(node, key.database, key.table).Set(float64(metrics.isReadonly))
		current[key] = struct{}{}
	}

	r.tables.Prune(node, current, func(key replicaTableKey) {
		replicaQueueSize.DeleteLabelValues(node, key.database, key.table)
		replicaReadonly.DeleteLabelValues(node, key.database, key.table)
	})
}
