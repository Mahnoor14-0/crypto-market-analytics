# crypto-market-analytics
Cryptocurrency Market Analytics Pipeline using Apache Spark, Databricks and Power BI


## Bronze and Silver Data Pipeline

### Project Overview

This project uses Apache Spark (PySpark) and Delta Lake in Databricks to ingest, validate, and prepare cryptocurrency market data for downstream analytics and Power BI reporting.

The data pipeline follows a layered architecture:

**JSON Source Files → Bronze Layer → Silver Layer → Analytics / Power BI**

### Bronze Layer

**Notebook:** `01_bronze_pipeline_final`  
**Runner notebook:** `00_run_bronze`  
**Delta table:** `workspace.default.bronze_coin_market_raw`

The Bronze layer:

- Reads cryptocurrency market data from JSON files using an explicit schema.
- Extracts price, market capitalization, and trading volume observations.
- Preserves source-file information and load timestamps.
- Validates source structure and observation alignment.
- Quarantines malformed or invalid data.
- Uses Delta MERGE to prevent duplicate business keys during reprocessing.

**Business key:** `coin_id`, `observation_timestamp`

### Silver Layer

**Notebook:** `02_silver_pipeline_final`  
**Runner notebook:** `00_run_silver`  
**Delta table:** `workspace.default.silver_market_observation`

The Silver layer:

- Reads observations from the Bronze Delta table.
- Rejects missing cryptocurrency IDs or observation timestamps.
- Rejects null, NaN, or negative market values.
- Stores rejected observations with rejection reasons.
- Uses Delta MERGE to insert new observations and update existing ones.
- Records pipeline execution results.

**Business key:** `coin_id`, `observation_timestamp`

### Data Models

**Bronze columns**

| Column | Data Type |
|---|---|
| coin_id | STRING |
| source_file | STRING |
| timestamp_ms | BIGINT |
| price_usd | DOUBLE |
| market_cap_usd | DOUBLE |
| total_volume_usd | DOUBLE |
| load_timestamp | TIMESTAMP |
| observation_timestamp | TIMESTAMP |

**Silver columns**

| Column | Data Type |
|---|---|
| coin_id | STRING |
| observation_timestamp | TIMESTAMP |
| price_usd | DOUBLE |
| market_cap_usd | DOUBLE |
| total_volume_usd | DOUBLE |
| source_file | STRING |
| load_timestamp | TIMESTAMP |

Both layers use the composite business key `(coin_id, observation_timestamp)`. The `load_timestamp` in Silver is carried forward from Bronze.

### Running the Pipelines

Run the notebooks in the following order:

1. `00_run_bronze`
2. `00_run_silver`

Each runner executes its corresponding final pipeline notebook.

The pipelines support these parameters:

| Parameter | Description |
|---|---|
| `load_type` | `full_load` or `incremental_load` |
| `processing_date` | Optional date in `YYYY-MM-DD` format |
| `source_folder` | Optional source-folder selection |

For Silver, an empty date and folder selection processes all available Bronze observations. A supplied `processing_date` filters by observation date, while `source_folder` filters the Bronze source-file path. The Silver `load_type` parameter is recorded for execution tracking; it does not independently change filtering behavior.

### Data Quality and Quarantine

Invalid data is retained for investigation rather than silently discarded.

Relevant tables include:

- `workspace.default.bronze_file_quarantine`
- `workspace.default.silver_quarantine`

Quarantine records provide information about rejected data and the reason for rejection.

### Execution Logging

Pipeline executions are recorded in:

`workspace.default.pipeline_execution_logs`

The log stores:

- Run ID and pipeline layer
- Target table
- Load type, processing date, and source folder
- Execution start and end timestamps
- SUCCESS or FAILED status
- Rows read, inserted, updated, and quarantined
- Error messages for logged failures

### Validation Results

The tested dataset produced:

| Metric | Result |
|---|---:|
| Bronze observations | 551,996 |
| Silver observations | 551,991 |
| Quarantined Silver observations | 5 |
| Unique cryptocurrencies | 250 |
| Duplicate Silver business keys | 0 |

Reprocessing the same Silver observations did not increase the Silver record count, confirming repeat-safe loading for the tested dataset.

### Technologies

- Python
- Apache Spark / PySpark
- Databricks
- Delta Lake
- GitHub
- Power BI (downstream analytics and visualization) 