#!/bin/bash

# Process one month of Deutsche Bahn data: download the target month's raw
# data (plus adjacent days for cross-midnight trains) from Hugging Face,
# parse the XML, build the monthly release, and upload all resulting parquet files.
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

# Adjacent boundary days are needed because trains cross midnight.
MONTH_BEFORE=$(date -d "$YEAR-$MONTH_PADDED-01 - 1 month" +"%m")
YEAR_BEFORE=$(date -d "$YEAR-$MONTH_PADDED-01 - 1 month" +"%Y")
LAST_DAY_BEFORE=$(date -d "$YEAR-$MONTH_PADDED-01 - 1 day" +"%-d")
MONTH_AFTER=$(date -d "$YEAR-$MONTH_PADDED-01 + 1 month" +"%m")
YEAR_AFTER=$(date -d "$YEAR-$MONTH_PADDED-01 + 1 month" +"%Y")
MONTH_BEFORE_NO_ZERO=$((10#$MONTH_BEFORE))
MONTH_AFTER_NO_ZERO=$((10#$MONTH_AFTER))

echo "=== Processing $YEAR-$MONTH_PADDED ==="
echo "Downloading raw data for the target month and adjacent boundary days"

uv run --with "huggingface_hub[cli]" hf download "$REPO_ID" \
    --repo-type=dataset \
    --include "raw_data/year=$YEAR_BEFORE/month=$MONTH_BEFORE_NO_ZERO/day=$LAST_DAY_BEFORE/*" \
    --include "raw_data/year=$YEAR/month=$MONTH_NO_ZERO/*" \
    --include "raw_data/year=$YEAR_AFTER/month=$MONTH_AFTER_NO_ZERO/day=1/*" \
    --local-dir .

echo "Parsing monthly XML data..."
uv run python scripts/parse_monthly_xml.py "$YEAR" "$MONTH_NO_ZERO"

echo "Building monthly stop data..."
uv run python scripts/create_monthly_data_release.py "$YEAR" "$MONTH_NO_ZERO"

PLAN_FILE="monthly_processed_data_plan/data-$YEAR-$MONTH_PADDED.parquet"
CHANGE_FILE="monthly_processed_data_change/data-$YEAR-$MONTH_PADDED.parquet"
DATA_FILE="monthly_processed_data/data-$YEAR-$MONTH_PADDED.parquet"

echo "Uploading monthly parsed and processed data..."
uv run --with "huggingface_hub[cli]" hf upload "$REPO_ID" . . \
    --repo-type=dataset \
    --include "$PLAN_FILE" \
    --include "$CHANGE_FILE" \
    --include "$DATA_FILE" \
    --commit-message="Monthly data release for $YEAR-$MONTH_PADDED - $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

echo "=== Done $YEAR-$MONTH_PADDED ==="
