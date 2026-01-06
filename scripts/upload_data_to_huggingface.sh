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

    MAX_RETRIES=3
    RETRY_DELAY=30

    for attempt in $(seq 1 $MAX_RETRIES); do
        echo "Upload attempt $attempt of $MAX_RETRIES..."

        if uv run --with huggingface_hub hf upload "$REPO_ID" "$DATA_DIR" "raw_data" \
            --repo-type=dataset \
            --commit-message="Update Deutsche Bahn data - $(date -u +"%Y-%m-%d %H:%M:%S UTC")"; then
            echo "Finished uploading data"
            echo "---"
            break
        else
            if [ $attempt -lt $MAX_RETRIES ]; then
                echo "Upload failed. Retrying in $RETRY_DELAY seconds..."
                sleep $RETRY_DELAY
                RETRY_DELAY=$((RETRY_DELAY * 2))
            else
                echo "Upload failed after $MAX_RETRIES attempts"
                exit 1
            fi
        fi
    done
else
    echo "Error: Data directory $DATA_DIR does not exist"
    exit 1
fi

echo "Upload complete!"
