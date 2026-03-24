[![CI](https://github.com/ppiankov/clickpulse/actions/workflows/ci.yml/badge.svg)](https://github.com/ppiankov/clickpulse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

# clickpulse

A heartbeat monitor for ClickHouse — polls system tables, exposes Prometheus metrics on `/metrics`.

## What clickpulse is

- A lightweight sidecar that connects to ClickHouse and exposes 40+ Prometheus-compatible metrics
- A poll-based exporter for queries, merges, mutations, replication, parts health, Keeper state, distributed DDL, dictionaries, disk usage, and query regression detection
- Compatible with ClickHouse 22.x+ (auto-detects version for correct system table schemas)
- Ships with a Grafana dashboard and a Helm chart with ServiceMonitor
- Zero config beyond a DSN — sensible defaults for everything

## What clickpulse is NOT

- Not a replacement for `clickhouse-exporter` or built-in Prometheus endpoints — clickpulse computes operational signals (merge pressure, part count explosions, mutation backlogs) that raw counters miss
- Not a query profiler — it captures top-N queries by time and memory, not full query plans or flame graphs
- Not a cluster manager — it reads system tables, it never writes to ClickHouse or modifies settings
- Not an alerting engine — pair it with Alertmanager or Grafana alerts for thresholds

## Philosophy

Observe, don't interfere. clickpulse opens a read-only window into ClickHouse's own system tables. It adds no extensions, modifies no data, and uses minimal resources. The metrics tell you what's happening; you decide what to do about it.

## ClickHouse prerequisites

### Required: a monitoring user

Create a dedicated user with read access to system tables:

```sql
CREATE USER clickpulse IDENTIFIED BY 'your-secure-password';
GRANT SELECT ON system.* TO clickpulse;
```

### System tables used

| System table | Collector | Notes |
|---|---|---|
| `system.processes` | Queries/activity | Active queries, memory, elapsed time |
| `system.query_log` | Query regression | Mean time deltas, call counts |
| `system.merges` | Merge pressure | Active merges, bytes/sec, part count |
| `system.mutations` | Mutations | Stuck mutations, parts remaining |
| `system.replicas` | Replication | Queue size, lag, delay |
| `system.parts` | Parts health | Part count per partition, sizes |
| `system.disks` | Disk/storage | Free space, total, usage ratio |
| `system.dictionaries` | Dictionaries | Load status, staleness |
| `system.distributed_ddl_queue` | Distributed DDL | Stuck operations |
| `system.zookeeper` / Keeper API | Keeper health | Latency, ephemeral nodes, leader |
| `system.metrics` | Server metrics | ClickHouse internal counters |
| `system.events` | Server events | Cumulative event counters |

### Connection string

clickpulse connects using a ClickHouse DSN:

```
clickhouse://clickpulse:password@localhost:9000/default?secure=false
```

For production, use `secure=true` for TLS.

## Quick start

```bash
# Build
make build

# Run
export CLICKHOUSE_DSN="clickhouse://clickpulse@localhost:9000/default"
./bin/clickpulse serve

# Docker
docker build -t clickpulse:dev .
docker run -e CLICKHOUSE_DSN="clickhouse://clickpulse@localhost:9000/default" -p 9188:9188 clickpulse:dev
```

Metrics at `http://localhost:9188/metrics`, health check at `/healthz`.

### Helm (Kubernetes)

```bash
helm upgrade --install clickpulse charts/clickpulse/ -f clickpulse-values.yaml -n clickpulse-system
```

Minimal `clickpulse-values.yaml`:

```yaml
targets:
  - name: prod
    dsn: "clickhouse://clickpulse@clickhouse:9000/default?secure=true"

serviceMonitor:
  enabled: true
  labels:
    release: prometheus-operator

prometheusRule:
  enabled: true
  labels:
    release: prometheus-operator
```

### systemd

```bash
sudo cp bin/clickpulse /usr/local/bin/
sudo cp deploy/clickpulse.service /etc/systemd/system/
sudo mkdir -p /etc/clickpulse
sudo cp deploy/clickpulse.env.example /etc/clickpulse/clickpulse.env
sudo chmod 600 /etc/clickpulse/clickpulse.env
# Edit CLICKHOUSE_DSN in /etc/clickpulse/clickpulse.env, then:
sudo systemctl daemon-reload
sudo systemctl enable --now clickpulse
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_DSN` or `DATABASE_URL` | *(required)* | ClickHouse connection string |
| `METRICS_PORT` | `9188` | Port for the HTTP metrics server |
| `POLL_INTERVAL` | `5s` | How often to collect metrics |
| `SLOW_QUERY_THRESHOLD` | `5s` | Duration after which a query is counted as slow |
| `REGRESSION_THRESHOLD` | `2.0` | Mean time ratio above which a query is flagged as regressed |
| `STMT_LIMIT` | `50` | Number of top queries to track per dimension |
| `TELEGRAM_BOT_TOKEN` | *(disabled)* | Telegram bot token for alerts |
| `TELEGRAM_CHAT_ID` | *(disabled)* | Telegram chat ID for alerts |
| `ALERT_WEBHOOK_URL` | *(disabled)* | Slack or generic webhook URL for alerts |
| `ALERT_COOLDOWN` | `5m` | Minimum interval between repeated alerts |
| `GRAFANA_URL` | *(disabled)* | Grafana base URL for anomaly annotations |
| `GRAFANA_TOKEN` | *(disabled)* | Grafana service account token |
| `GRAFANA_DASHBOARD_UID` | *(optional)* | Scope annotations to a specific dashboard |

## Architecture

```
cmd/clickpulse/main.go              CLI entry point (delegates to internal/cli)
internal/
  cli/                               Cobra commands: serve, version, status, doctor
  config/                            Environment-based configuration
  collector/                         Poll loop + collectors
    processes.go                     system.processes (active queries, memory, elapsed)
    merges.go                        system.merges (merge pressure, bytes/sec)
    mutations.go                     system.mutations (stuck mutations, parts remaining)
    replication.go                   system.replicas (queue size, lag, delay)
    parts.go                         system.parts (part count per partition, sizes)
    regression.go                    system.query_log (mean time deltas, stateful)
    storage.go                       system.disks (free space, usage ratio)
    dictionaries.go                  system.dictionaries (load status, staleness)
    ddl.go                           system.distributed_ddl_queue (stuck DDL)
    server.go                        system.metrics + system.events
    querier.go                       Interface for testability
  keeper/                            ClickHouse Keeper / ZooKeeper health
  metrics/                           Prometheus metric definitions
  snapshot/                          Point-in-time cluster snapshot
  doctor/                            Connectivity and permission diagnostics
  alerter/                           Telegram, webhook alerting
  annotator/                         Grafana anomaly annotations
charts/clickpulse/                   Helm chart with ServiceMonitor + PrometheusRule
grafana/
  clickpulse-dashboard.json          Importable Grafana dashboard
deploy/
  clickpulse.service                 systemd unit file
  clickpulse.env.example             Environment file template
```

## Known limitations

- Query fingerprints are truncated to 80 characters
- No support for multiple ClickHouse clusters in a single process
- Query regression detection requires `system.query_log` (enabled by default)
- Keeper metrics require either ClickHouse Keeper or ZooKeeper access
- Part count metrics are per-partition, not per-table (by design — partition-level is where problems show)

## Roadmap

- [ ] Core scaffold and CLI (serve, version, status, doctor)
- [ ] Processes collector (system.processes)
- [ ] Merge pressure collector (system.merges)
- [ ] Mutations collector (system.mutations)
- [ ] Replication collector (system.replicas)
- [ ] Parts health collector (system.parts)
- [ ] Query regression detection (system.query_log)
- [ ] Disk/storage collector (system.disks)
- [ ] Keeper health collector
- [ ] Distributed DDL collector
- [ ] Dictionaries collector
- [ ] Server metrics/events collector
- [ ] Grafana dashboard
- [ ] Helm chart with ServiceMonitor + PrometheusRule
- [ ] Built-in alerting (Telegram, Slack)
- [ ] Anomaly annotations

## License

[MIT](LICENSE)
