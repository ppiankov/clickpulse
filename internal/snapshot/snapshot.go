package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Snapshot holds a point-in-time health summary of the ClickHouse cluster.
type Snapshot struct {
	Version        string
	Uptime         time.Duration
	ActiveQueries  int
	SlowQueries    int
	ActiveMerges   int
	MergeBytesPS   float64
	ReplicaLag     float64
	ReadonlyTables int
	TotalParts     int64
	KeeperOK       bool
}

// Take queries ClickHouse once and returns a health snapshot.
func Take(ctx context.Context, db *sql.DB) (*Snapshot, error) {
	s := &Snapshot{}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Version + uptime
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&s.Version); err != nil {
		return nil, fmt.Errorf("version: %w", err)
	}
	var uptimeSec uint64
	if err := db.QueryRowContext(ctx, "SELECT uptime()").Scan(&uptimeSec); err != nil {
		return nil, fmt.Errorf("uptime: %w", err)
	}
	s.Uptime = time.Duration(uptimeSec) * time.Second

	// Active/slow queries
	if err := db.QueryRowContext(ctx, `
		SELECT count(), countIf(elapsed > 5) FROM system.processes
	`).Scan(&s.ActiveQueries, &s.SlowQueries); err != nil {
		return nil, fmt.Errorf("processes: %w", err)
	}

	// Merges
	if err := db.QueryRowContext(ctx, `
		SELECT count(), sum(bytes_read_uncompressed / if(elapsed > 0, elapsed, 1))
		FROM system.merges
	`).Scan(&s.ActiveMerges, &s.MergeBytesPS); err != nil {
		return nil, fmt.Errorf("merges: %w", err)
	}

	// Replication
	row := db.QueryRowContext(ctx, `
		SELECT
			max(absolute_delay),
			countIf(is_readonly = 1)
		FROM system.replicas
	`)
	var lag sql.NullFloat64
	var readonly sql.NullInt64
	if err := row.Scan(&lag, &readonly); err != nil {
		// No replicas is fine — just leave defaults.
		s.ReplicaLag = 0
		s.ReadonlyTables = 0
	} else {
		if lag.Valid {
			s.ReplicaLag = lag.Float64
		}
		if readonly.Valid {
			s.ReadonlyTables = int(readonly.Int64)
		}
	}

	// Parts
	if err := db.QueryRowContext(ctx, `
		SELECT count() FROM system.parts WHERE active = 1
	`).Scan(&s.TotalParts); err != nil {
		return nil, fmt.Errorf("parts: %w", err)
	}

	// Keeper
	var keeperCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count() FROM system.zookeeper WHERE path = '/'
	`).Scan(&keeperCount); err != nil {
		s.KeeperOK = false
	} else {
		s.KeeperOK = true
	}

	return s, nil
}
