//go:build integration

package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	clickHouseImage          = "clickhouse/clickhouse-server:24.3"
	clickHouseDSNEnv         = "CLICKPULSE_TEST_DSN"
	clickHouseNativePort     = "9000/tcp"
	clickHouseReadyTimeout   = 90 * time.Second
	clickHouseCommandTimeout = 30 * time.Second
	clickHousePollInterval   = 500 * time.Millisecond
	clickHouseQueryTimeout   = 10 * time.Second
	partsTablePrefix         = "clickpulse_test_parts"
)

var (
	collectorIntegrationDB      *sql.DB
	collectorIntegrationCleanup func()
	collectorIntegrationErr     error
)

func TestMain(m *testing.M) {
	collectorIntegrationDB, collectorIntegrationCleanup, collectorIntegrationErr = setupIntegrationDB(context.Background(), "collector")

	code := m.Run()

	if collectorIntegrationDB != nil {
		_ = collectorIntegrationDB.Close()
	}
	if collectorIntegrationCleanup != nil {
		collectorIntegrationCleanup()
	}

	os.Exit(code)
}

func TestProcessesCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewProcesses(5*time.Second))
}

func TestMergesCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewMerges())
}

func TestMutationsCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewMutations())
}

func TestReplicationCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewReplication())
}

func TestPartsCollector(t *testing.T) {
	t.Parallel()

	db := requireIntegrationDB(t)
	tableName := fmt.Sprintf("%s_%d", partsTablePrefix, os.Getpid())
	ctx, cancel := context.WithTimeout(context.Background(), clickHouseQueryTimeout)
	defer cancel()

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.ExecContext(ctx, dropQuery); err != nil {
		t.Fatalf("drop parts table: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), clickHouseQueryTimeout)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, dropQuery)
	})

	createQuery := fmt.Sprintf(
		"CREATE TABLE %s (id UInt64) ENGINE = MergeTree ORDER BY id",
		tableName,
	)
	if _, err := db.ExecContext(ctx, createQuery); err != nil {
		t.Fatalf("create parts table: %v", err)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (id) VALUES (1), (2), (3)", tableName)
	if _, err := db.ExecContext(ctx, insertQuery); err != nil {
		t.Fatalf("insert parts rows: %v", err)
	}

	runCollector(t, NewParts())
}

func TestDisksCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewDisks())
}

func TestServerCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewServer())
}

func TestQueryLogCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewQueryLog(2.0, 50))
}

func TestDiscrepancyCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewDiscrepancy())
}

func TestKeeperCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewKeeper())
}

func TestDDLCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewDDL())
}

func TestDictionariesCollector(t *testing.T) {
	t.Parallel()

	runCollector(t, NewDictionaries())
}

func runCollector(t *testing.T, c Collector) {
	t.Helper()

	db := requireIntegrationDB(t)
	if err := c.Collect(db); err != nil {
		t.Fatalf("%s collect: %v", c.Name(), err)
	}
}

func requireIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()

	if collectorIntegrationErr != nil {
		t.Skipf("integration ClickHouse unavailable: %v", collectorIntegrationErr)
	}

	return collectorIntegrationDB
}

func setupIntegrationDB(ctx context.Context, name string) (*sql.DB, func(), error) {
	if dsn := os.Getenv(clickHouseDSNEnv); dsn != "" {
		db, err := openClickHouse(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return db, func() {}, nil
	}

	return setupDockerClickHouse(ctx, name)
}

func setupDockerClickHouse(ctx context.Context, name string) (*sql.DB, func(), error) {
	containerName := fmt.Sprintf("clickpulse-%s-%d-%d", name, os.Getpid(), time.Now().UnixNano())

	if _, err := runDocker(ctx, "run", "-d", "--rm", "--name", containerName, "-p", "127.0.0.1::9000", clickHouseImage); err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		_, _ = runDocker(context.Background(), "rm", "-f", containerName)
	}

	port, err := dockerHostPort(ctx, containerName, clickHouseNativePort)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	dsn := fmt.Sprintf(
		"clickhouse://default@127.0.0.1:%s/default?secure=false&dial_timeout=10s&read_timeout=30s",
		port,
	)

	db, err := openClickHouse(ctx, dsn)
	if err != nil {
		logs, logsErr := runDocker(context.Background(), "logs", containerName)
		cleanup()
		if logsErr == nil && logs != "" {
			return nil, nil, fmt.Errorf("wait for ClickHouse: %w: %s", err, logs)
		}
		return nil, nil, err
	}

	return db, cleanup, nil
}

func openClickHouse(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}

	if err := waitForClickHouse(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func waitForClickHouse(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(clickHouseReadyTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		queryCtx, cancel := context.WithTimeout(ctx, clickHouseQueryTimeout)

		if err := db.PingContext(queryCtx); err == nil {
			var uptimeSeconds uint64
			err = db.QueryRowContext(queryCtx, "SELECT uptime()").Scan(&uptimeSeconds)
			if err == nil && uptimeSeconds > 0 {
				cancel()
				return nil
			}
			if err == nil {
				err = errors.New("ClickHouse uptime is still zero")
			}
			lastErr = err
		} else {
			lastErr = err
		}

		cancel()
		time.Sleep(clickHousePollInterval)
	}

	if lastErr == nil {
		lastErr = errors.New("timed out waiting for ClickHouse")
	}

	return lastErr
}

func dockerHostPort(ctx context.Context, containerName, containerPort string) (string, error) {
	format := fmt.Sprintf("{{(index (index .NetworkSettings.Ports %q) 0).HostPort}}", containerPort)
	output, err := runDocker(ctx, "inspect", "--format", format, containerName)
	if err != nil {
		return "", err
	}
	if output == "" {
		return "", fmt.Errorf("docker inspect returned empty host port for %s", containerPort)
	}
	return output, nil
}

func runDocker(ctx context.Context, args ...string) (string, error) {
	commandCtx, cancel := context.WithTimeout(ctx, clickHouseCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(commandCtx, "docker", args...)
	output, err := cmd.CombinedOutput()
	trimmed := strings.TrimSpace(string(output))
	if err != nil {
		if trimmed == "" {
			return "", fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
		}
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, trimmed)
	}
	return trimmed, nil
}
