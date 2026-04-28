# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.4.0] - 2026-04-28

- Add PrometheusRule alerts for all four replication-discrepancy gauges: `ClickHouseOrphanReplicatedTable` (critical, 10m), `ClickHouseUnreplicatedTable` (warning, 30m), `ClickHouseLeaderlessReplicatedTable` (critical, 5m), `ClickHousePartCountDiff` (warning, 15m, threshold 100).
- Add built-in alerter equivalents in `internal/alerter/rules.go`: `checkOrphanReplicatedTables`, `checkUnreplicatedTables`, `checkPartCountDiff` — all no-ops on standalone nodes.

## [0.3.0] - 2026-04-21

- Add `system.replication_queue` metrics and split replication backlog alerts from missing-part failures.

## [0.2.9] - 2026-04-21

- Add table-scoped replica lag metrics and make the Helm replica lag alert match lag and pending work on the same table.

## [0.2.8] - 2026-04-21

- Require pending replica queue work before the Helm replica lag alert fires.

## [0.2.7] - 2026-04-21

- Fix replica lag alerts so idle replicated tables with old `absolute_delay` values do not page unless there is queued work or an uncopied replication log entry.

## [0.2.6] - 2026-04-21

- Prune stale table-scoped Prometheus gauge series after successful scrapes.
- Add Kafka, object storage, Keeper memory rejection, query memory limit, and guardrail rejection alert metrics.

## [0.2.5] - 2026-04-21

- Fix replica sync alerts so cumulative ClickHouse error counters only alert on recent increases.
- Add replicated part fetch/check failure and replicated data loss counters.
- Fix mutation parts remaining gauge so it no longer accumulates across scrapes.
