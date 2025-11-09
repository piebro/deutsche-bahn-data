#!/bin/bash

# Exit on error, undefined variables, and pipeline failures
set -euo pipefail

REPO_ID="piebro/deutsche-bahn-data"
DATA_DIR="./raw_data"

echo "Uploading Deutsche Bahn data to Hugging Face..."
echo "Repository: $REPO_ID"
echo "Data directory: $DATA_DIR"
echo "---"

if [ -d "$DATA_DIR" ]; then
    echo "Uploading data directory..."

    uv run --with huggingface_hub hf upload "$REPO_ID" "$DATA_DIR" "raw_data" \
        --repo-type=dataset \
        --commit-message="Update Deutsche Bahn data - $(date -u +"%Y-%m-%d %H:%M:%S UTC")"

    echo "Finished uploading data"
    echo "---"
else
    echo "Error: Data directory $DATA_DIR does not exist"
    exit 1
fi

echo "Upload complete!"
