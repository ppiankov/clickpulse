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
	}, []string{"database", "table", "replica"})
	replicationUnreplicatedTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_unreplicated_tables",
		Help: "MergeTree tables without Replicated engine in a clustered setup",
	}, []string{"database", "table"})
	replicationOrphanTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_orphan_tables",
		Help: "Tables that exist on one node but not on peer replicas",
	}, []string{"database", "table", "host"})
	replicationLeaderlessTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_leaderless_tables",
		Help: "Replicated tables with no active leader",
	}, []string{"database", "table"})
	replicationPartCountDiff = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_replication_part_count_diff",
		Help: "Max part count difference across replicas for same table",
	}, []string{"database", "table"})
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
// On standalone (non-clustered) setups this is a safe no-op.
type Discrepancy struct{}

func NewDiscrepancy() *Discrepancy { return &Discrepancy{} }

func (d *Discrepancy) Name() string { return "discrepancy" }

func (d *Discrepancy) Collect(q Querier) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Check if this is a clustered setup.
	var clusterCount int
	if err := q.QueryRowContext(ctx, "SELECT count(DISTINCT cluster) FROM system.clusters").Scan(&clusterCount); err != nil {
		return err
	}
	if clusterCount == 0 {
		return nil // standalone — nothing to check
	}

	if err := d.collectLeaderless(ctx, q); err != nil {
		return err
	}
	if err := d.collectUnreplicated(ctx, q); err != nil {
		return err
	}
	if err := d.collectPartCountDiff(ctx, q); err != nil {
		return err
	}

	return nil
}

// collectLeaderless finds replicated tables where is_leader=0 for all replicas
// visible to this node (no active leader from this node's perspective).
func (d *Discrepancy) collectLeaderless(ctx context.Context, q Querier) error {
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

	replicationLeaderlessTables.Reset()
	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			return err
		}
		replicationLeaderlessTables.WithLabelValues(database, table).Set(1)
	}
	return rows.Err()
}

// collectUnreplicated finds MergeTree tables that are NOT Replicated* in a cluster.
func (d *Discrepancy) collectUnreplicated(ctx context.Context, q Querier) error {
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

	replicationUnreplicatedTables.Reset()
	for rows.Next() {
		var database, table string
		if err := rows.Scan(&database, &table); err != nil {
			return err
		}
		replicationUnreplicatedTables.WithLabelValues(database, table).Set(1)
	}
	return rows.Err()
}

// collectPartCountDiff compares active part counts across replicas for each table.
func (d *Discrepancy) collectPartCountDiff(ctx context.Context, q Querier) error {
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

	replicationPartCountDiff.Reset()
	for rows.Next() {
		var database, table string
		var maxParts, minParts uint64
		if err := rows.Scan(&database, &table, &maxParts, &minParts); err != nil {
			return err
		}
		replicationPartCountDiff.WithLabelValues(database, table).Set(float64(maxParts - minParts))
	}
	return rows.Err()
}
