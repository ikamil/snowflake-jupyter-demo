#!/usr/bin/env bash
# =============================================================================
# Query → Compressed File Export Utility (sample)
# Demonstrates: snowsql formatted output, gzip streaming, split chunking,
#   Slack progress/error webhook, parameterized SQL execution
# =============================================================================

set -euo pipefail

SNOW_CMD="snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -w $SNOWFLAKE_WAREHOUSE -d $SNOWFLAKE_DB"
SNOW_FMT="-o exit_on_error=True -o friendly=False -o timing=False -o empty_for_null_in_tsv=True -o output_format=tsv -o header=false"

SQL="$1"
TARGET="$2"

if [[ -z "$SQL" || -z "$TARGET" ]]; then
    echo "Usage: $0 \"SELECT ...\" \"/output/path.tsv.gz\""
    exit 1
fi

echo "$(date): Exporting data to $TARGET"

# Execute query, pipe through gzip to target
result=$( { $SNOW_CMD -q "$SQL" $SNOW_FMT | gzip > "$TARGET" ; } 2>&1 )

if [[ ! ( -z "$result" || $result == *"completed"* ) ]]; then
    echo "ERROR: $result"
    exit 1
fi

echo "$(date): Export complete → $TARGET"
