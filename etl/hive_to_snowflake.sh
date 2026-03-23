#!/usr/bin/env bash
# =============================================================================
# Hive → S3 → Snowflake Pipeline (sample)
# Demonstrates: multi-hop data transfer, gzip streaming, S3 staging,
#   COPY INTO with file format, Slack webhook notifications, error handling
# =============================================================================

set -euo pipefail

# --- Configuration (from environment or secrets manager) ---
SNOW_CMD="snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -w $SNOWFLAKE_WAREHOUSE -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA"
S3_CMD="s3cmd --access_key=$AWS_ACCESS_KEY --secret_key=$AWS_SECRET_KEY"
S3_BUCKET="$S3_STAGING_BUCKET"
SLACK_WEBHOOK="$SLACK_WEBHOOK_URL"

# --- Slack notification helper ---
function notify() {
    local text="$(date '+%Y-%m-%d %H:%M:%S'): $1"
    local header="${2:-ETL Pipeline}"
    local emoji=":information_source:"

    shopt -s nocasematch
    if [[ ! ( $1 == *"on_error"* ) && ( $1 == *"error"* || $1 == *"exception"* ) ]]; then
        emoji=":exclamation:"
        header="$header Error"
    else
        header="$header Progress"
    fi

    local txt="${text//$'\n'/}"
    txt="${txt//$'\"'/}"
    local json="{\"username\": \"$header\", \"text\": \"$txt\", \"icon_emoji\": \"$emoji\"}"
    curl -s -X POST --data-urlencode "payload=$json" "$SLACK_WEBHOOK" > /dev/null
}

# --- Hive → gzip → S3 → Snowflake transfer ---
function hive_to_snowflake() {
    local hive_query="$1"
    local target_table="$2"
    local uxd=$(date -u "+%s")
    local staging_table="hive_transfer_$uxd"
    local s3_dir="s3://$S3_BUCKET/staging/etl/tmp/$staging_table/"
    local snow_fmt="file_format = (TYPE = CSV FIELD_DELIMITER = '\t' COMPRESSION=GZIP) ON_ERROR = 'CONTINUE'"

    notify "Starting Hive → Snowflake transfer for $target_table"

    # Clean staging area
    $S3_CMD rm "${s3_dir}*" 2>/dev/null || true

    # Stream Hive output → gzip → S3
    hive --database analytics -e "$hive_query" | gzip | $S3_CMD put - "${s3_dir}data.tsv.gz"

    # Create target table in Snowflake
    $SNOW_CMD -q "CREATE OR REPLACE TRANSIENT TABLE $target_table (
        device_id VARCHAR(100), campaign_id INT, event_date DATE,
        metadata VARCHAR(50), record_count INT
    ) CLUSTER BY (device_id)"

    # Load from S3 stage into Snowflake
    local src="@\"${SNOWFLAKE_DB}\".\"PUBLIC\".\"S3_STAGE\"/staging/etl/tmp/${staging_table}/data.tsv.gz"
    $SNOW_CMD -q "COPY INTO $target_table FROM '$src' $snow_fmt"

    # Cleanup S3 staging
    $S3_CMD rm "${s3_dir}*"

    notify "Completed transfer: $target_table"
}

# --- S3 → Snowflake (when data is already staged) ---
function s3_to_snowflake() {
    local s3_path="$1"
    local target_table="$2"
    local snow_fmt="file_format = (TYPE = CSV FIELD_DELIMITER = '\t' COMPRESSION=GZIP) ON_ERROR = 'CONTINUE'"
    $SNOW_CMD -q "COPY INTO $target_table FROM '$s3_path' $snow_fmt"
}

# --- Main execution ---
notify "Pipeline started"

hive_to_snowflake \
    "SELECT device_id, campaign_id, event_date, source, count(*) cnt
     FROM events.device_observations
     WHERE event_date = '2020-01-15'
     GROUP BY device_id, campaign_id, event_date, source" \
    "staging.daily_observations"

notify "Pipeline finished"
