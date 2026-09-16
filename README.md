# Incremental CDC Databricks Pipeline

Production-Grade Incremental Change Data Capture (CDC) Pipeline using Databricks, Delta Lake, and Apache Spark.

## Overview
This repository contains an end-to-end incremental CDC (Change Data Capture) data engineering pipeline built on Databricks Lakehouse architecture. The pipeline captures, processes, and merges transactional CDC event streams (inserts, updates, deletes) into Delta tables using Medallion Architecture (Bronze -> Silver -> Gold).

## Tech Stack
- **Cloud Lakehouse Platform:** Databricks
- **Storage & Table Format:** Delta Lake (Delta Change Data Feed / CDF)
- **Processing Engine:** Apache Spark / PySpark / Spark Structured Streaming
- **Data Ingestion:** Databricks Auto Loader (`cloudFiles`) / Kafka / Debezium CDC
- **Governance & Security:** Unity Catalog
- **Orchestration:** Databricks Workflows (Multi-task Jobs) / Delta Live Tables (DLT)
- **Data Modeling:** SCD Type 1 (Upserts/MERGE) & SCD Type 2 (Historical tracking)
- **CI/CD & Quality:** Databricks CLI / GitHub Actions / Great Expectations / Delta Expectations

## Architecture
- **Bronze Layer (Raw Ingestion):** Ingests CDC logs/events append-only with metadata (operation type, timestamp, sequence/offset).
- **Silver Layer (Cleaned & Conformed CDC):** Deduplicates, applies schema validation, and handles CDC operations via `MERGE INTO` or Delta Change Data Feed for downstream consumers.
- **Gold Layer (Business Aggregates & Analytics):** Curated business dimensions, metrics, and star-schema models for BI, reporting, and downstream consumption.

## Key Features
- **Idempotent Ingestion & Deduplication:** Guarantees exactly-once processing semantics using Spark checkpoints and sequence IDs.
- **Change Data Feed (CDF):** Captures row-level changes (insert, update_preimage, update_postimage, delete) for auditing and downstream streaming consumers.
- **Automated Schema Evolution:** Resilient handling of incoming schema variations.
- **SCD Management:** Automated Slowly Changing Dimensions Type 1 and Type 2 implementations.
