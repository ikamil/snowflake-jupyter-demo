#!/usr/bin/env bash
# =============================================================================
# Snowflake → S3 Export (sample)
# Demonstrates: COPY INTO s3:// unload, SINGLE file mode, OVERWRITE,
#   header inclusion, gzip compression, Jenkins-style parameterization
# =============================================================================

set -euo pipefail

SNOW_CMD="snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -w $SNOWFLAKE_WAREHOUSE -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA"
SNOW_CREDS="CREDENTIALS = (AWS_KEY_ID = '$AWS_ACCESS_KEY' AWS_SECRET_KEY = '$AWS_SECRET_KEY')"

EXPORT_DATE="${TEMPLATE_DATE:-$(date '+%Y-%m-%d')}"
S3_DEST="s3://${S3_EXPORT_BUCKET}/exports/${EXPORT_DATE}/"

# Unload query result to S3 as a single gzipped CSV
$SNOW_CMD -q "
COPY INTO '${S3_DEST}audience_export.csv.gz'
FROM (
    SELECT device_id, segment, region, score
    FROM delivery.campaign_audience
    WHERE export_date = '$EXPORT_DATE'
)
${SNOW_CREDS}
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    COMPRESSION = GZIP
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    NULL_IF = ('NULL', '')
)
SINGLE = TRUE
OVERWRITE = TRUE
HEADER = TRUE
MAX_FILE_SIZE = 5368709120;
"

echo "Export complete: ${S3_DEST}audience_export.csv.gz"
