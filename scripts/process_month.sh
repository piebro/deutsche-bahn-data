#!/bin/bash

# Process one month of Deutsche Bahn data: download the target month's raw
# data (plus adjacent months for cross-midnight trains) from Hugging Face,
# run the monthly release script, and upload the resulting parquet back.
#
# Usage: scripts/process_month.sh YEAR MONTH
# Example: scripts/process_month.sh 2025 7

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 YEAR MONTH"
    exit 1
fi

REPO_ID="piebro/deutsche-bahn-data"

YEAR="$1"
MONTH_NO_ZERO=$((10#$2))
MONTH_PADDED=$(printf "%02d" "$MONTH_NO_ZERO")

# Adjacent months are needed because trains cross midnight boundaries.
MONTH_BEFORE=$(date -d "$YEAR-$MONTH_PADDED-01 - 1 month" +"%m")
YEAR_BEFORE=$(date -d "$YEAR-$MONTH_PADDED-01 - 1 month" +"%Y")
MONTH_AFTER=$(date -d "$YEAR-$MONTH_PADDED-01 + 1 month" +"%m")
YEAR_AFTER=$(date -d "$YEAR-$MONTH_PADDED-01 + 1 month" +"%Y")
MONTH_BEFORE_NO_ZERO=$((10#$MONTH_BEFORE))
MONTH_AFTER_NO_ZERO=$((10#$MONTH_AFTER))

echo "=== Processing $YEAR-$MONTH_PADDED ==="
echo "Downloading raw data: $YEAR_BEFORE-$MONTH_BEFORE_NO_ZERO, $YEAR-$MONTH_NO_ZERO, $YEAR_AFTER-$MONTH_AFTER_NO_ZERO"

uv run --with huggingface_hub hf download "$REPO_ID" \
    --repo-type=dataset \
    --include "raw_data/year=$YEAR_BEFORE/month=$MONTH_BEFORE_NO_ZERO/*" \
    --include "raw_data/year=$YEAR/month=$MONTH_NO_ZERO/*" \
    --include "raw_data/year=$YEAR_AFTER/month=$MONTH_AFTER_NO_ZERO/*" \
    --local-dir .

echo "Running monthly release script..."
uv run python scripts/create_monthly_data_release.py "$YEAR" "$MONTH_NO_ZERO"

DATA_FILE="monthly_processed_data/data-$YEAR-$MONTH_PADDED.parquet"

echo "Uploading $DATA_FILE..."
uv run --with huggingface_hub hf upload "$REPO_ID" "$DATA_FILE" "$DATA_FILE" \
    --repo-type=dataset \
    --commit-message="Monthly data release for $YEAR-$MONTH_PADDED - $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

echo "=== Done $YEAR-$MONTH_PADDED ==="
