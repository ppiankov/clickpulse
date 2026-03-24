# clickpulse

A heartbeat monitor for ClickHouse — polls system tables, exposes Prometheus metrics, fires alerts.

## Install

```bash
brew install ppiankov/tap/clickpulse
```

Or from source:

```bash
go install github.com/ppiankov/clickpulse/cmd/clickpulse@latest
```

## Commands

### clickpulse serve

Start the metrics exporter. Connects to ClickHouse, runs 12 collectors on a poll loop, exposes `/metrics` and `/healthz`.

**Environment variables:** `CLICKHOUSE_DSN` (required), `METRICS_PORT` (default 9188), `POLL_INTERVAL` (default 5s), `SLOW_QUERY_THRESHOLD` (default 5s), `REGRESSION_THRESHOLD` (default 2.0), `STMT_LIMIT` (default 50), `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `ALERT_WEBHOOK_URL`, `ALERT_COOLDOWN`, `GRAFANA_URL`, `GRAFANA_TOKEN`, `GRAFANA_DASHBOARD_UID`.

**Exit codes:**
- 0: clean shutdown
- 1: startup error (bad config, connection refused)

### clickpulse status

One-shot cluster health summary.

```bash
clickpulse status --format json
```

**Flags:**
- `--format json` — structured JSON output

**JSON output:**

```json
{
  "version": "24.3.1",
  "uptime_seconds": 86400,
  "active_queries": 5,
  "slow_queries": 0,
  "active_merges": 2,
  "merge_bytes_ps": 1048576,
  "replica_lag": 0,
  "readonly_tables": 0,
  "total_parts": 1200,
  "keeper_ok": true
}
```

**Exit codes:**
- 0: success
- 1: error (connection refused, query failed)

### clickpulse doctor

Diagnose ClickHouse connectivity and permissions.

```bash
clickpulse doctor --format json
```

**Flags:**
- `--format json` — structured JSON output per ANCC doctor convention

**JSON output:**

```json
{
  "status": "healthy",
  "checks": [
    { "name": "connectivity", "status": "pass", "message": "ok" },
    { "name": "version", "status": "pass", "message": "24.3.1" },
    { "name": "select system.replicas", "status": "pass", "message": "ok" },
    { "name": "keeper", "status": "fail", "message": "connection refused" }
  ]
}
```

`status` is `healthy` when all checks pass, `degraded` when any check fails.

**Exit codes:**
- 0: all checks pass
- 1: one or more checks failed

### clickpulse init

Print a default `.env` configuration with all supported environment variables and sensible defaults.

```bash
clickpulse init > .env
```

**Exit codes:**
- 0: success

### clickpulse version

Print the version string.

**Exit codes:**
- 0: success

## What this does NOT do

- **Does not manage ClickHouse** — clickpulse observes, it does not execute DDL, ALTER, or SYSTEM commands
- **Does not store time-series data** — metrics are exposed via Prometheus scrape endpoint; clickpulse owns no storage
- **Does not replace Prometheus or Grafana** — it is a collector, not a monitoring platform; dashboards and alerting rules live in your stack
- **Does not modify ClickHouse configuration** — it reads system tables with SELECT only; no writes, no config changes
- **Does not own cluster topology** — it monitors whatever DSN it is pointed at; cluster discovery is not its job

## Parsing examples

```bash
# Check if ClickHouse is reachable
clickpulse doctor --format json | jq -r '.status'

# Get replica lag
clickpulse status --format json | jq '.replica_lag'

# List failing doctor checks
clickpulse doctor --format json | jq '.checks[] | select(.status == "fail") | .name'

# Get version
clickpulse status --format json | jq -r '.version'

# Generate default config
clickpulse init > .env
```

---

This tool follows the [Agent-Native CLI Convention](https://ancc.dev). Validate with: `ancc validate .`
