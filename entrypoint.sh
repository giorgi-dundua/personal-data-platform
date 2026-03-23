#!/bin/bash
set -e

echo "🚀 Container Starting..."

# 1. Decode Google Credentials
# Check if the secret exists in the environment
if [ -n "$GOOGLE_CREDENTIALS_BASE64" ]; then
    echo "🔑 Decoding Google Credentials..."
    mkdir -p .secrets
    # Decode the Base64 string back into the JSON file
    echo "$GOOGLE_CREDENTIALS_BASE64" | base64 -d > .secrets/personal-data-platform.json
fi

# 2. Pull Data from Google Drive
if [ -f ".secrets/personal-data-platform.json" ]; then
    echo "☁️ Syncing State from Google Drive..."
    # This downloads registry.db and merged_daily_metrics.csv
    python -m scripts.sync_state pull
else
    echo "⚠️ No credentials found. Skipping Cloud Sync (Demo Mode only)."
fi

# 3. Run the Main Command (Streamlit)
echo "🎨 Starting Dashboard..."
# "$@" executes whatever command was passed to docker run (or CMD in Dockerfile)
exec "$@"