# Cryptocurrency Market Analytics
# Phase 2 — Bronze and Silver Data Dictionary

## 1. Project Overview

This document defines the data models for the Bronze and Silver layers
of the Cryptocurrency Market Analytics Lakehouse.

The pipeline uses Apache Spark and Databricks.

The primary source is the CoinGecko API.

The pipeline supports:

- Full historical loads
- Incremental loads
- Parameterized backfills
- Explicit schema enforcement
- Idempotent processing
- Schema drift handling
- Data quality validation
- Audit logging

No Spark schema inference is used.

All production reads use explicitly defined Spark schemas.

---

# 2. Naming Conventions

| Convention | Description |
|---|---|
| Bronze | Raw source data with ingestion metadata |
| Silver | Cleaned and structured analytical data |
| FULL | Historical/full load |
| INCREMENTAL | Daily incremental load |
| batch_id | Identifies the logical processing batch |
| run_id | Identifies one execution of the pipeline |
| load_timestamp | Timestamp when the record was ingested or processed |
| observation_timestamp | Timestamp represented by the source market observation |

All timestamps are stored as UTC.

---

# 3. Bronze Layer

The Bronze layer preserves the source data with minimal transformation.

The Bronze layer must retain the original API response so that source data
can be reprocessed if the Silver transformation logic changes.

Bronze ingestion uses an explicit Spark schema.

Schema inference is NOT used.

---

## 3.1 bronze_coin_market_raw

### Purpose

Stores raw CoinGecko market-chart API responses for cryptocurrency coins.

Both FULL and INCREMENTAL loads are stored in this table.

### Business Key

The deterministic `record_hash` identifies the raw source record.

### Primary/Unique Key

`record_hash`

### Columns

| Column | Data Type | Nullable | Key | Description |
|---|---|---:|---|---|
| record_hash | STRING | NO | Primary/Unique | SHA-256 hash used for deterministic identification of a raw record |
| batch_id | STRING | NO | Business | Identifies the logical full or incremental batch |
| run_id | STRING | NO | Business | Identifies one pipeline execution |
| load_type | STRING | NO | | FULL or INCREMENTAL |
| source | STRING | NO | | Source system, e.g. CoinGecko |
| source_endpoint | STRING | NO | | API endpoint used to obtain the data |
| coin_id | STRING | NO | Business | CoinGecko cryptocurrency identifier |
| source_file | STRING | NO | | Name/path of the raw input file |
| raw_response | STRING | NO | | Original raw API response stored as text |
| load_timestamp | TIMESTAMP | NO | | UTC timestamp when the raw record was ingested |

### Constraints

- `record_hash` must not be NULL.
- `batch_id` must not be NULL.
- `run_id` must not be NULL.
- `coin_id` must not be NULL.
- `raw_response` must not be NULL.
- `load_timestamp` must not be NULL.
- `load_type` must be either `FULL` or `INCREMENTAL`.
- Raw source content must not be modified during Bronze ingestion.
- Duplicate raw records must be identified using `record_hash`.

---

# 4. Silver Layer

The Silver layer contains cleaned, validated, typed, and deduplicated
cryptocurrency market observations.

The Silver layer converts the raw CoinGecko arrays into one row per
cryptocurrency observation timestamp.

---

## 4.1 silver_market_observation

### Purpose

Stores structured cryptocurrency market observations after parsing and
validation of the Bronze data.

### Business Key

The business key is:

`coin_id + observation_timestamp`

This combination uniquely identifies a market observation for a coin.

### Primary/Unique Key

`coin_id + observation_timestamp`

### Columns

| Column | Data Type | Nullable | Key | Description |
|---|---|---:|---|---|
| coin_id | STRING | NO | Business Key | CoinGecko cryptocurrency identifier |
| observation_timestamp | TIMESTAMP | NO | Business Key | UTC timestamp of the market observation |
| price_usd | DOUBLE | NO | | Cryptocurrency price in USD |
| market_cap_usd | DOUBLE | NO | | Cryptocurrency market capitalization in USD |
| total_volume_usd | DOUBLE | NO | | Total trading volume in USD |
| load_timestamp | TIMESTAMP | NO | | UTC timestamp when the record was processed into Silver |
| load_type | STRING | NO | | FULL or INCREMENTAL |
| batch_id | STRING | NO | | Logical processing batch |
| run_id | STRING | NO | | Pipeline execution identifier |

### Constraints

- `coin_id` must not be NULL.
- `observation_timestamp` must not be NULL.
- `price_usd` must not be NULL.
- `market_cap_usd` must not be NULL.
- `total_volume_usd` must not be NULL.
- `load_timestamp` must not be NULL.
- `price_usd >= 0`.
- `market_cap_usd >= 0`.
- `total_volume_usd >= 0`.
- `load_type` must be either `FULL` or `INCREMENTAL`.
- Duplicate observations are not allowed for the same:
  `coin_id + observation_timestamp`.

### Idempotency Rule

Silver processing must use MERGE INTO using:

`coin_id + observation_timestamp`

as the matching condition.

If the same source data is processed again, a duplicate Silver record
must not be created.

---

# 5. Silver Quarantine

## 5.1 silver_quarantine

### Purpose

Stores records that fail Silver validation or cannot be safely converted
to the required data types.

Invalid records must not cause the entire pipeline to fail.

### Columns

| Column | Data Type | Nullable | Key | Description |
|---|---|---:|---|---|
| batch_id | STRING | NO | | Logical processing batch |
| run_id | STRING | NO | | Pipeline execution identifier |
| source | STRING | NO | | Original data source |
| coin_id | STRING | YES | | Cryptocurrency identifier if available |
| raw_record | STRING | NO | | Original problematic record |
| error_reason | STRING | NO | | Reason why the record failed validation |
| load_timestamp | TIMESTAMP | NO | | UTC timestamp when the record was quarantined |

### Constraints

- Invalid records must be redirected to quarantine.
- The pipeline must continue processing valid records.
- `error_reason` must clearly identify the validation or conversion problem.
- `load_timestamp` must always be populated.

---

# 6. Pipeline Execution Logs

## 6.1 pipeline_execution_logs

### Purpose

Stores execution information for every Raw-to-Bronze and
Bronze-to-Silver pipeline run.

This table provides the required audit trail.

### Primary Key

`run_id + layer + file_processed`

### Columns

| Column | Data Type | Nullable | Key | Description |
|---|---|---:|---|---|
| run_id | STRING | NO | Primary/Business | Unique identifier for one execution |
| batch_id | STRING | NO | Business | Logical batch being processed |
| layer | STRING | NO | | Processing layer, e.g. Raw-to-Bronze or Bronze-to-Silver |
| load_type | STRING | NO | | FULL or INCREMENTAL |
| parameter | STRING | YES | | Backfill date, input path, or other execution parameter |
| file_processed | STRING | NO | Business | File or table processed |
| execution_start | TIMESTAMP | NO | | Pipeline start timestamp |
| execution_end | TIMESTAMP | YES | | Pipeline completion timestamp |
| status | STRING | NO | | SUCCESS or FAILURE |
| rows_inserted | LONG | NO | | Number of records inserted |
| rows_updated | LONG | NO | | Number of records updated |
| error_message | STRING | YES | | Error details when execution fails |

### Constraints

- Every processed file/table must generate an audit record.
- `execution_start` must always be populated.
- `execution_end` must be populated after processing finishes.
- `status` must be `SUCCESS` or `FAILURE`.
- `rows_inserted` must never be NULL.
- `rows_updated` must never be NULL.
- Failed executions must record an `error_message`.
- Both FULL and INCREMENTAL loads must be logged.
- Raw-to-Bronze processing must be logged.
- Bronze-to-Silver processing must be logged.

---

# 7. Data Quality Rules

The following validation rules are applied during Silver processing.

## Required Fields

The following fields cannot be NULL:

- coin_id
- observation_timestamp
- price_usd
- market_cap_usd
- total_volume_usd
- load_timestamp

## Numeric Validation

The following values must be greater than or equal to zero:

- price_usd
- market_cap_usd
- total_volume_usd

## Timestamp Validation

- observation_timestamp must be a valid UTC timestamp.
- load_timestamp must be a valid UTC timestamp.

## Duplicate Validation

Duplicate records are identified using:

`coin_id + observation_timestamp`

Only one Silver record may exist for each business key.

## Schema Validation

Records that cannot be converted to the required data types are
quarantined rather than causing the complete batch to fail.

---

# 8. Schema Drift Policy

The Bronze layer preserves the original source response.

Therefore, newly added source fields are not immediately lost.

The Silver layer uses an explicit schema.

If the source introduces an unexpected field, the raw Bronze data remains
available for future processing.

If a source field changes to an incompatible data type, the affected
record is quarantined and the remaining valid records continue processing.

Schema changes must be reviewed before modifying the Silver schema.

---

# 9. Idempotency

The pipeline must be safe to execute more than once.

For Silver:

`coin_id + observation_timestamp`

is the business key.

Silver processing uses Delta Lake MERGE INTO to perform upsert operations.

Reprocessing the same raw data must not create duplicate Silver records.

Bronze records are identified using a deterministic `record_hash`.

---

# 10. Load Timestamp

Every Bronze and Silver record must contain:

`load_timestamp`

This represents the UTC timestamp when the record was ingested or
processed by the pipeline.

`load_timestamp` is different from:

`observation_timestamp`

where:

- `observation_timestamp` = when the cryptocurrency market observation
  occurred.
- `load_timestamp` = when the pipeline processed the record.

---

# 11. Full vs Incremental Loads

## Full Load

The full load contains historical cryptocurrency market observations
for the selected 250 cryptocurrencies.

It is used to initially populate the Lakehouse.

## Incremental Load

The incremental load contains new/recent observations for the same
cryptocurrencies.

The incremental process is parameterized using a run date.

Example:

`--run-date 2026-10-06`

The run date determines the historical API range and output batch.

Incremental data uses the same observation structure as the full load so
that both can be processed by the same Bronze and Silver data models.

---

# 12. Parameterization

The pipeline must not depend on a hardcoded "today" value.

The processing date/batch/input path must be supplied as a parameter.

This allows:

- Standard daily processing
- Historical backfills
- Reprocessing
- Testing

Example:

`2026-10-06`

and

`2026-10-07`

must be treated as separate logical batches.

---

# 13. Data Lineage

Source:

CoinGecko API

↓

Raw JSON files

↓

Bronze

`bronze_coin_market_raw`

↓

Silver parsing and validation

↓

`silver_market_observation`

Invalid records:

↓

`silver_quarantine`

Execution information:

↓

`pipeline_execution_logs`