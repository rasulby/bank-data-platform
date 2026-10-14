# Bank Data Platform

A portfolio data engineering project that simulates the data side of a retail
bank: card transactions flow from an operational PostgreSQL database through
ingestion, object storage, a warehouse and transformations to a dashboard.
All data is synthetic.

> Status: Phase 1 (walking skeleton) in progress.
> See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md).

## Prerequisites

- Docker and Docker Compose v2
- Python 3.11+
- GNU Make

## Quick start (infrastructure only)

    cp .env.example .env
    make up
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements/generator.txt
    make seed

The full "Run the pipeline" section is added as the pipeline is built.
