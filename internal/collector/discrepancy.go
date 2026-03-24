package collector

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	replicationMissingParts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_missing_parts",
		Help: "Parts present on one replica but absent on another",
	}, []string{"node", "database", "table", "replica"})
	replicationUnreplicatedTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_unreplicated_tables",
		Help: "MergeTree tables without Replicated engine in a clustered setup",
	}, []string{"node", "database", "table"})
	replicationOrphanTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_orphan_tables",
		Help: "Tables that exist on one node but not on peer replicas",
	}, []string{"node", "database", "table", "host"})
	replicationLeaderlessTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_leaderless_tables",
		Help: "Replicated tables with no active leader",
	}, []string{"node", "database", "table"})
	replicationPartCountDiff = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_part_count_diff",
		Help: "Max part count difference across replicas for same table",
	}, []string{"node", "database", "table"})
)

func init() {
	prometheus.MustRegister(
		replicationMissingParts,
		replicationUnreplicatedTables,
		replicationOrphanTables,
		replicationLeaderlessTables,
		replicationPartCountDiff,
	)
}

// Discrepancy detects cross-replica replication inconsistencies.
type Discrepancy struct{}

func NewDiscrepancy() *Discrepancy { return &Discrepancy{} }

func (d *Discrepancy) Name() string { return "discrepancy" }

func (d *Discrepancy) Collect(q Querier, node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var clusterCount int
	if err := q.QueryRowContext(ctx, "SELECT count(DISTINCT cluster) FROM system.clusters").Scan(&clusterCount); err != nil {
		return err
	}
	if clusterCount == 0 {
		return nil
	}

	if err := d.collectLeaderless(ctx, q, node); err != nil {
		return err
	}
	if err := d.collectUnreplicated(ctx, q, node); err != nil {
		return err
	}
	if err := d.collectPartCountDiff(ctx, q, node); err != nil {
		return err
	}

	return nil
}

func (d *Discrepancy) collectLeaderless(ctx context.Context, q Querier, node string) error {
	rows, err := q.QueryContext(ctx, `
		SELECT database, table
		FROM system.replicas
		GROUP BY database, table
		HAVING max(is_leader) = 0
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			return err
		}
		replicationLeaderlessTables.WithLabelValues(node, database, table).Set(1)
	}
	return rows.Err()
}

func (d *Discrepancy) collectUnreplicated(ctx context.Context, q Querier, node string) error {
	rows, err := q.QueryContext(ctx, `
		SELECT database, name
		FROM system.tables
		WHERE engine LIKE '%MergeTree%'
		  AND engine NOT LIKE 'Replicated%'
		  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			return err
		}
		replicationUnreplicatedTables.WithLabelValues(node, database, table).Set(1)
	}
	return rows.Err()
}

func (d *Discrepancy) collectPartCountDiff(ctx context.Context, q Querier, node string) error {
	rows, err := q.QueryContext(ctx, `
		SELECT
			database,
			table,
			max(active_replicas) AS max_parts,
			min(active_replicas) AS min_parts
		FROM (
			SELECT
				database,
				table,
				replica_name,
				toUInt64(parts_to_check) + toUInt64(queue_size) AS active_replicas
			FROM system.replicas
		)
		GROUP BY database, table
		HAVING max_parts - min_parts > 0
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var database, table string
		var maxParts, minParts uint64
		if err := rows.Scan(&database, &table, &maxParts, &minParts); err != nil {
			return err
		}
		replicationPartCountDiff.WithLabelValues(node, database, table).Set(float64(maxParts - minParts))
	}
	return rows.Err()
}
