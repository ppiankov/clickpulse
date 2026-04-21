# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.2.6] - 2026-04-21

- Prune stale table-scoped Prometheus gauge series after successful scrapes.
- Add Kafka, object storage, Keeper memory rejection, query memory limit, and guardrail rejection alert metrics.

## [0.2.5] - 2026-04-21

- Fix replica sync alerts so cumulative ClickHouse error counters only alert on recent increases.
- Add replicated part fetch/check failure and replicated data loss counters.
- Fix mutation parts remaining gauge so it no longer accumulates across scrapes.
