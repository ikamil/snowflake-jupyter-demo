#!/usr/bin/env bash
# =============================================================================
# Hive External Table + Remote Execution (sample)
# Demonstrates: SSH gateway pattern for remote Hive/Spark execution,
#   Parquet external tables, Hive UDF JAR loading, LATERAL VIEW
# =============================================================================

set -euo pipefail

GATEWAY_HOST="$HADOOP_GATEWAY"
HDFS_BASE="$HDFS_ROOT/user/$USER/hive"

# --- Create Hive external table over Parquet files ---
ssh "$GATEWAY_HOST" << 'REMOTE_EOF'
hive -e "
    CREATE EXTERNAL TABLE IF NOT EXISTS analytics.device_observations (
        device_id       STRING,
        grid_cell_id    BIGINT,
        observation_ts  BIGINT,
        source_type     STRING,
        ip_address      STRING
    )
    STORED AS PARQUET
    LOCATION '${HDFS_BASE}/device_observations/';
"
REMOTE_EOF

# --- Load custom UDF JARs and use LATERAL VIEW ---
ssh "$GATEWAY_HOST" << 'REMOTE_EOF'
hive -e "
    ADD JAR /opt/jars/location_api.jar;
    ADD JAR /opt/jars/custom_udfs.jar;

    CREATE TEMPORARY FUNCTION resolve_location AS 'com.example.udf.ResolveLocation';
    CREATE TEMPORARY FUNCTION ip_to_long       AS 'com.example.udf.IpToLong';

    SELECT
        d.device_id,
        d.observation_ts,
        loc.parcel_id,
        loc.distance_m
    FROM analytics.device_observations d
        LATERAL VIEW resolve_location(d.grid_cell_id, reference_table, 100.0, true) loc
    WHERE d.source_type = 'primary'
    LIMIT 100;
"
REMOTE_EOF
