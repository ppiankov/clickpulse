package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Result holds the outcome of a single diagnostic check.
type Result struct {
	Name   string
	Passed bool
	Detail string
}

// systemTables are the tables clickpulse needs SELECT access to.
var systemTables = []string{
	"system.replicas",
	"system.parts",
	"system.processes",
	"system.mutations",
	"system.merges",
	"system.query_log",
	"system.disks",
	"system.tables",
	"system.clusters",
}

// Run executes all diagnostic checks and returns results.
func Run(ctx context.Context, db *sql.DB) []Result {
	var results []Result
	results = append(results, checkConnectivity(ctx, db))
	results = append(results, checkVersion(ctx, db))
	results = append(results, checkSystemTables(ctx, db)...)
	results = append(results, checkKeeper(ctx, db))
	return results
}

func checkConnectivity(ctx context.Context, db *sql.DB) Result {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return Result{Name: "connectivity", Passed: false, Detail: err.Error()}
	}
	return Result{Name: "connectivity", Passed: true, Detail: "ok"}
}

func checkVersion(ctx context.Context, db *sql.DB) Result {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return Result{Name: "version", Passed: false, Detail: err.Error()}
	}
	return Result{Name: "version", Passed: true, Detail: version}
}

func checkSystemTables(ctx context.Context, db *sql.DB) []Result {
	results := make([]Result, 0, len(systemTables))
	for _, table := range systemTables {
		r := checkTableAccess(ctx, db, table)
		results = append(results, r)
	}
	return results
}

func checkTableAccess(ctx context.Context, db *sql.DB, table string) Result {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	name := fmt.Sprintf("select %s", table)
	// Use a LIMIT 0 query to test access without reading data.
	query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", table)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return Result{Name: name, Passed: false, Detail: err.Error()}
	}
	return Result{Name: name, Passed: true, Detail: "ok"}
}

func checkKeeper(ctx context.Context, db *sql.DB) Result {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// system.zookeeper requires a path filter; check the root.
	var count int
	err := db.QueryRowContext(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&count)
	if err != nil {
		return Result{Name: "keeper", Passed: false, Detail: err.Error()}
	}
	return Result{Name: "keeper", Passed: true, Detail: fmt.Sprintf("%d root znodes", count)}
}
