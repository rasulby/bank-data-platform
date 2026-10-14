# Project Plan: Bank Data Platform

## 1. Goal

Build a portfolio-grade data platform that mirrors a typical retail-bank
stack: an operational database, batch and streaming ingestion, a lake and a
warehouse, orchestrated and tested transformations, and a consumer-facing
dashboard.

## 2. Consumer

**Transaction monitoring analyst** (risk / operations). Needs, per day:
number of card transactions, decline rate, volume by merchant category, and
number of transactions matching suspicious patterns.
Serving output: a Grafana dashboard.

## 3. Source

Synthetic operational PostgreSQL database (customers, accounts, cards,
merchants, transactions) produced by Python generators. No real personal data.

## 4. Components and roles

| Component | Role | Phase |
|---|---|---|
| PostgreSQL | Source OLTP system | 1 |
| Python generators | Seed data, historical data, live stream | 1 |
| Python ingestion | Extract by logical date, write Parquet | 1 |
| MinIO | Raw object storage (Parquet) | 1 |
| ClickHouse | Warehouse: raw, staging, curated | 1 |
| dbt | Staging and curated models, data tests | 1 |
| Apache Airflow | Orchestration | 1 |
| Grafana | Serving / dashboard | 1 |
| Apache Kafka | Broker; connectivity proof only in Phase 1 | 1 / 2 |
| GitHub Actions | CI | 1 |
| Debezium | CDC from PostgreSQL | 2 (ADR-0001) |
| Spark Structured Streaming | Streaming processing | 2 |
| Apache Iceberg | Lakehouse tables | 2 |
| Great Expectations | Data quality | 2 |

## 5. Data layers

- **raw**: Parquet in MinIO, partitioned by logical date
  (`transactions/ds=YYYY-MM-DD/`), mirrored into ClickHouse raw tables.
- **staging**: typed, renamed, deduplicated (dbt).
- **curated**: simplified dimensional model, one fact and one dimension (dbt).
- **serving**: view/table read by the Grafana dashboard.

## 6. Repository layout

    ingestion/       generators and extract/load code
    orchestration/   Airflow DAGs
    transformation/  dbt project
    serving/         Grafana provisioning
    infra/           service configuration (Postgres, Debezium, ...)
    tests/           smoke and unit tests
    docs/            plan, decisions (ADRs), evidence

## 7. Decisions

- [ADR-0001](decisions/0001-batch-first-phase-1.md): batch-first Phase 1

## 8. Changelog

- 2026-10-14: Initial plan. Phase 1 scoped as a batch pipeline; streaming
  moved to Phase 2 (ADR-0001).
