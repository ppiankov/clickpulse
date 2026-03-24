package alerter

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CheckAndFire runs all built-in alert rules against the database
// and fires alerts for any that trigger.
func CheckAndFire(ctx context.Context, db *sql.DB, a *Alerter) {
	if !a.Enabled() {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	checkMergeBacklog(ctx, db, a)
	checkStuckMutations(ctx, db, a)
	checkPartExplosion(ctx, db, a)
	checkReplicaLag(ctx, db, a)
	checkKeeperLeader(ctx, db, a)
}

const (
	mergeBacklogThreshold = 50
	partCountThreshold    = 300 // per partition
	replicaLagThreshold   = 30  // seconds
)

func checkMergeBacklog(ctx context.Context, db *sql.DB, a *Alerter) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count() FROM system.merges").Scan(&count); err != nil {
		return
	}
	if count >= mergeBacklogThreshold {
		a.Fire(ctx, Alert{
			Name:    "merge_backlog",
			Message: fmt.Sprintf("clickpulse: merge backlog high (%d active merges)", count),
		})
	}
}

func checkStuckMutations(ctx context.Context, db *sql.DB, a *Alerter) {
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
			Message: fmt.Sprintf("clickpulse: %d stuck mutation(s) (>10min)", count),
		})
	}
}

func checkPartExplosion(ctx context.Context, db *sql.DB, a *Alerter) {
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
			Message: fmt.Sprintf("clickpulse: partition %s.%s/%s has %d parts", database, table, partition, maxParts),
		})
	}
}

func checkReplicaLag(ctx context.Context, db *sql.DB, a *Alerter) {
	var maxLag float64
	if err := db.QueryRowContext(ctx, `
		SELECT max(absolute_delay) FROM system.replicas
	`).Scan(&maxLag); err != nil {
		return
	}
	if maxLag >= replicaLagThreshold {
		a.Fire(ctx, Alert{
			Name:    "replica_lag",
			Message: fmt.Sprintf("clickpulse: replica lag %.0fs (threshold %ds)", maxLag, replicaLagThreshold),
		})
	}
}

func checkKeeperLeader(ctx context.Context, db *sql.DB, a *Alerter) {
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
			Message: "clickpulse: this node lost Keeper leadership",
		})
	}
}
