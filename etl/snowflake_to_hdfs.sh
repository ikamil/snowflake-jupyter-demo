#!/usr/bin/env bash
# =============================================================================
# Snowflake → Distributed Filesystem (sample)
# Demonstrates: snowsql piped output, split for chunking large files,
#   gzip streaming, header preservation across splits, NFS mount targets
# =============================================================================

set -euo pipefail

TASK="SF_TO_DFS"
SNOW_CMD="snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -w $SNOWFLAKE_WAREHOUSE -d $SNOWFLAKE_DB"
SNOW_FMT="-o exit_on_error=True -o friendly=False -o timing=False -o empty_for_null_in_tsv=True -o output_format=tsv"

SQL="$1"
TARGET="$2"
LINES_PER_CHUNK=10000000000

if [[ -z "$SQL" || -z "$TARGET" ]]; then
    echo "Usage: $0 \"SQL Query\" \"/path/to/output.tsv.gz\""
    exit 1
fi

# Stream query → preserve header across splits → gzip each chunk
$SNOW_CMD -q "$SQL" $SNOW_FMT \
    | { read header && \
        sed "1~$((${LINES_PER_CHUNK}-1)) s/^/${header}\n/g" \
        | split -l $LINES_PER_CHUNK --numeric-suffixes=1 --filter "gzip > $TARGET" - _ ; }

echo "Export complete: $TARGET"
