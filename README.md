# Snowflake & Hadoop ETL Portfolio — Sample Project

A depersonalized demonstration of data pipeline patterns spanning **Snowflake**, **Hive/Hadoop**, **S3**, **PySpark**, and **Luigi** orchestration. Each sample shows a distinct ingestion, transformation, or export technique — no duplicated patterns.

---

## Pipeline Architecture

```
┌─────────────┐     ┌─────────┐     ┌─────────────┐     ┌──────────────┐
│  Hive/HDFS  │────▶│   S3    │────▶│  Snowflake  │────▶│  Delivery /  │
│  (Parquet)  │ gz  │ Staging │ COPY│  Warehouse  │     │   Reports    │
└─────────────┘     └─────────┘     └──────┬──────┘     └──────────────┘
       ▲                                   │
       │                                   │ UNLOAD
┌──────┴──────┐                     ┌──────▼──────┐
│   PySpark   │                     │  S3 Export  │
│  (Luigi)    │                     │  (.csv.gz)  │
└─────────────┘                     └─────────────┘
```

### Data Flow Summary

| Flow | Source | Destination | Method | Sample |
|------|--------|-------------|--------|--------|
| **Ingest** | Hive query | Snowflake table | `hive -e` → gzip → s3cmd → `COPY INTO` | [etl/hive_to_snowflake.sh](etl/hive_to_snowflake.sh) |
| **Export** | Snowflake query | S3 file | `COPY INTO 's3://...'` with SINGLE/OVERWRITE | [etl/snowflake_to_s3.sh](etl/snowflake_to_s3.sh) |
| **Distribute** | Snowflake query | HDFS/NFS | snowsql → split → gzip → NFS mount | [etl/snowflake_to_hdfs.sh](etl/snowflake_to_hdfs.sh) |
| **Remote ETL** | Hive + JARs | External tables | SSH gateway → Hive DDL + UDF JARs + LATERAL VIEW | [etl/hive_external_tables.sh](etl/hive_external_tables.sh) |
| **Orchestrate** | Raw files | Aggregated Parquet | Luigi → PySpark → custom UDFs | [pipeline/tasks/etl_task.py](pipeline/tasks/etl_task.py) |

---

## Folder Structure

```
snowflake-jupyter-project/
├── sql/                          # Snowflake SQL patterns
│   ├── global_functions.sql      #   UDFs: haversine, grid distance, proximity scoring
│   │                             #   Stored procedures: JS-based validation
│   ├── campaigns/
│   │   ├── campaign_delivery.sql #   CTAS, CROSS JOIN grid expansion, window functions,
│   │   │                         #   ROLLUP aggregation, UNION ALL audience merge
│   │   └── campaign_performance.sql  CTEs, EXISTS/NOT EXISTS, conditional CASE agg
│   ├── visitation/
│   │   └── location_matching.sql #   UPDATE...FROM, correlated subqueries, ALTER TABLE,
│   │                             #   TRUNCATE + reload, COALESCE/GREATEST null handling
│   ├── household/
│   │   └── household_graph.sql   #   Multi-pass UPDATE joins, RLIKE regex validation,
│   │                             #   UNION in CTE, ROW_NUMBER() ID generation
│   └── procedures/
│       └── address_parsing.sql   #   REGEXP_REPLACE chains, TRANSLATE, SPLIT_PART,
│                                 #   classification via multi-condition CASE + RLIKE
├── etl/                          # Shell-based data movement
│   ├── hive_to_snowflake.sh      #   Full pipeline: Hive→gzip→S3→COPY INTO Snowflake
│   ├── snowflake_to_s3.sh        #   COPY INTO s3:// unload with SINGLE/OVERWRITE/HEADER
│   ├── snowflake_to_hdfs.sh      #   snowsql → split → gzip to distributed filesystem
│   └── hive_external_tables.sh   #   SSH gateway, Parquet tables, UDF JARs, LATERAL VIEW
├── pipeline/                     # Containerized PySpark + Luigi
│   ├── Dockerfile                #   Multi-stage build: system deps → pip → code
│   ├── requirements.txt          #   snowflake, pandas, luigi, pyspark, scipy
│   ├── setup.py                  #   Egg packaging for Spark distribution
│   └── tasks/
│       ├── luigi.conf            #   Scheduler, state persistence, task history
│       └── etl_task.py           #   Luigi Task: idempotency check, Spark SQL, partitioned write
└── notebooks/
    └── data_export.sh            #   Generic query→gzip export utility
```

---

## Technologies Demonstrated

### Snowflake
- **DDL**: `CREATE TABLE AS`, transient tables, clustering, `ALTER TABLE`
- **DML**: `INSERT INTO...SELECT`, `UPDATE...SET...FROM` (join-update), `TRUNCATE`
- **UDFs**: Scalar functions (haversine, grid math, scoring)
- **Stored Procedures**: JavaScript with `snowflake.createStatement()`, parameterized binds
- **COPY INTO**: Bulk load from S3 stages; unload to S3 with compression/headers
- **File Formats**: CSV, TSV, GZIP, Parquet; custom delimiters, null handling, error skip
- **Stages**: Internal named stages, S3 external stages with credentials
- **Query Patterns**: CTEs, window functions (`ROW_NUMBER`, `MAX OVER`), `ROLLUP`, `EXISTS`/`NOT EXISTS`, `UNION ALL`, conditional `CASE` aggregation

### Hive / Hadoop
- External tables with `STORED AS PARQUET` + HDFS `LOCATION`
- Custom UDF JARs: `ADD JAR` + `CREATE TEMPORARY FUNCTION`
- `LATERAL VIEW` for table-generating functions
- `INSERT OVERWRITE` for partition replacement

### Data Movement
- **Hive → S3 → Snowflake**: pipe + gzip + s3cmd + COPY INTO
- **Snowflake → S3**: `COPY INTO 's3://...'` with `SINGLE=TRUE`, `OVERWRITE=TRUE`
- **Snowflake → HDFS/NFS**: snowsql CLI output → split → gzip → NFS mount
- **SSH Gateway**: Remote Hive/Spark execution through gateway host

### Orchestration & Automation
- **Luigi**: Task DAGs, `complete()` idempotency, egg distribution to Spark
- **PySpark**: `SparkSession`, custom JARs, `spark.sql()`, partitioned Parquet writes
- **Docker**: Containerized pipeline with multi-stage build
- **Shell**: Bash orchestration with Slack webhooks, error capture, S3 staging lifecycle
- **snowsql CLI**: Formatted output modes (`tsv`, `friendly=False`), `exit_on_error`

### String & Geospatial Processing
- Address normalization: `REGEXP_REPLACE` chains, `TRANSLATE`, `SPLIT_PART`
- Classification: multi-condition `CASE` with `RLIKE` regex
- Geospatial: Haversine distance, grid-cell encoding, radius intersection, offset generation
- Postal matching: ZIP+4+2 composite codes, `RLIKE` validation

---

## How to Use These Samples

These are **reference patterns**, not a runnable project. Each file demonstrates specific techniques:

1. **SQL files** — Run individual statements in a Snowflake worksheet to see each pattern
2. **Shell scripts** — Adapt environment variables and execute; they show the full data-movement lifecycle
3. **Pipeline** — `docker build -t sample-etl . && docker run sample-etl` (requires Spark cluster access)
4. **Luigi tasks** — `luigi --module etl_task RunPipeline --run-date 2020-01-15 --local-scheduler`
