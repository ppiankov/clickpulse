//go:build integration

package doctor

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
)

var (
	doctorIntegrationDB      *sql.DB
	doctorIntegrationCleanup func()
	doctorIntegrationErr     error
)

func TestMain(m *testing.M) {
	doctorIntegrationDB, doctorIntegrationCleanup, doctorIntegrationErr = setupIntegrationDB(context.Background(), "doctor")

	code := m.Run()

	if doctorIntegrationDB != nil {
		_ = doctorIntegrationDB.Close()
	}
	if doctorIntegrationCleanup != nil {
		doctorIntegrationCleanup()
	}

	os.Exit(code)
}

func TestDoctorRun(t *testing.T) {
	t.Parallel()

	db := requireIntegrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), clickHouseQueryTimeout)
	defer cancel()

	results := Run(ctx, db)
	if len(results) == 0 {
		t.Fatal("doctor returned no results")
	}

	for _, result := range results {
		if result.Name == "keeper" {
			continue
		}
		if !result.Passed {
			t.Fatalf("doctor check %q failed: %s", result.Name, result.Detail)
		}
	}
}

func requireIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()

	if doctorIntegrationErr != nil {
		t.Skipf("integration ClickHouse unavailable: %v", doctorIntegrationErr)
	}

	return doctorIntegrationDB
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
