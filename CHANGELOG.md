# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
