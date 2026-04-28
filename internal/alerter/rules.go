package alerter

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CheckAndFire runs all built-in alert rules against the database
// and fires alerts for any that trigger.
func CheckAndFire(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	if !a.Enabled() {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	checkMergeBacklog(ctx, db, a, node)
	checkStuckMutations(ctx, db, a, node)
	checkPartExplosion(ctx, db, a, node)
	checkReplicaLag(ctx, db, a, node)
	checkKeeperLeader(ctx, db, a, node)
	checkOrphanReplicatedTables(ctx, db, a, node)
	checkUnreplicatedTables(ctx, db, a, node)
	checkPartCountDiff(ctx, db, a, node)
}

const (
	mergeBacklogThreshold  = 50
	partCountThreshold     = 300 // per partition
	replicaLagThreshold    = 30  // seconds
	partCountDiffThreshold = 100 // cross-replica part count skew; distinct from per-partition part explosion
	alertableReplicaLag    = "max(if(queue_size > 0 OR (log_max_index > 0 AND log_pointer <= log_max_index), absolute_delay, 0))"
)

func checkMergeBacklog(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count() FROM system.merges").Scan(&count); err != nil {
		return
	}
	if count >= mergeBacklogThreshold {
		a.Fire(ctx, Alert{
			Name:    "merge_backlog",
			Host:    node,
			Message: fmt.Sprintf("merge backlog high (%d active merges)", count),
		})
	}
}

func checkStuckMutations(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count() FROM system.mutations
		WHERE is_done = 0 AND create_time < now() - INTERVAL 10 MINUTE
	`).Scan(&count); err != nil {
		return
	}
	if count > 0 {
		a.Fire(ctx, Alert{
			Name:    "stuck_mutations",
			Host:    node,
			Message: fmt.Sprintf("%d stuck mutation(s) (>10min)", count),
		})
	}
}

func checkPartExplosion(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	var maxParts int64
	var database, table, partition string
	if err := db.QueryRowContext(ctx, `
		SELECT database, table, partition, count() AS c
		FROM system.parts
		WHERE active = 1
		GROUP BY database, table, partition
		ORDER BY c DESC
		LIMIT 1
	`).Scan(&database, &table, &partition, &maxParts); err != nil {
		return
	}
	if maxParts >= partCountThreshold {
		a.Fire(ctx, Alert{
			Name:    "part_explosion",
			Host:    node,
			Message: fmt.Sprintf("partition %s.%s/%s has %d parts", database, table, partition, maxParts),
		})
	}
}

func checkReplicaLag(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	var maxLag float64
	if err := db.QueryRowContext(ctx, "SELECT "+alertableReplicaLag+" FROM system.replicas").Scan(&maxLag); err != nil {
		return
	}
	if maxLag >= replicaLagThreshold {
		a.Fire(ctx, Alert{
			Name:    "replica_lag",
			Host:    node,
			Message: fmt.Sprintf("replica lag %.0fs (threshold %ds)", maxLag, replicaLagThreshold),
		})
	}
}

func checkKeeperLeader(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	// Check if keeper_info exists.
	var exists int
	if err := db.QueryRowContext(ctx, `
		SELECT count() FROM system.tables
		WHERE database = 'system' AND name = 'keeper_info'
	`).Scan(&exists); err != nil || exists == 0 {
		return
	}

	var isLeader uint8
	if err := db.QueryRowContext(ctx, `
		SELECT is_leader FROM system.keeper_info LIMIT 1
	`).Scan(&isLeader); err != nil {
		return
	}
	if isLeader == 0 {
		a.Fire(ctx, Alert{
			Name:    "keeper_leader_loss",
			Host:    node,
			Message: "this node lost Keeper leadership",
		})
	}
}

func clusterIsStandalone(ctx context.Context, db *sql.DB) bool {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(DISTINCT cluster) FROM system.clusters").Scan(&count); err != nil {
		return true
	}
	return count == 0
}

func checkOrphanReplicatedTables(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	if clusterIsStandalone(ctx, db) {
		return
	}
	var count int
	var database, table string
	// total_replicas = 1 means this node holds the only replica — orphan.
	if err := db.QueryRowContext(ctx, `
		SELECT count(), any(database), any(table)
		FROM system.replicas
		WHERE total_replicas = 1
	`).Scan(&count, &database, &table); err != nil || count == 0 {
		return
	}
	a.Fire(ctx, Alert{
		Name:    "orphan_replicated_table",
		Host:    node,
		Message: fmt.Sprintf("%d orphan Replicated table(s) with total_replicas=1 (e.g. %s.%s)", count, database, table),
	})
}

func checkUnreplicatedTables(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	if clusterIsStandalone(ctx, db) {
		return
	}
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT count()
		FROM system.tables
		WHERE engine LIKE '%MergeTree%'
		  AND engine NOT LIKE 'Replicated%'
		  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	`).Scan(&count); err != nil || count == 0 {
		return
	}
	a.Fire(ctx, Alert{
		Name:    "unreplicated_table",
		Host:    node,
		Message: fmt.Sprintf("%d plain MergeTree table(s) in clustered setup", count),
	})
}

func checkPartCountDiff(ctx context.Context, db *sql.DB, a *Alerter, node string) {
	if clusterIsStandalone(ctx, db) {
		return
	}
	var database, table string
	var maxDiff uint64
	if err := db.QueryRowContext(ctx, `
		SELECT database, table, max(active_replicas) - min(active_replicas) AS diff
		FROM (
			SELECT database, table, replica_name,
			       toUInt64(parts_to_check) + toUInt64(queue_size) AS active_replicas
			FROM system.replicas
		)
		GROUP BY database, table
		ORDER BY diff DESC
		LIMIT 1
	`).Scan(&database, &table, &maxDiff); err != nil {
		return
	}
	if maxDiff > partCountDiffThreshold {
		a.Fire(ctx, Alert{
			Name:    "part_count_diff",
			Host:    node,
			Message: fmt.Sprintf("replica part count diff %d for %s.%s (threshold %d)", maxDiff, database, table, partCountDiffThreshold),
		})
	}
}
