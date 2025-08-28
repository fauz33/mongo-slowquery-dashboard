#!/bin/bash

# Simple MongoDB CurrentOp Collection Script
# Quick script to collect db.currentOp() output for analysis

# Configuration
MONGO_HOST="${1:-localhost:27017}"
OUTPUT_FILE="${2:-currentop_$(date '+%Y%m%d_%H%M%S').json}"

echo "🔍 Collecting MongoDB currentOp data..."
echo "📍 Host: $MONGO_HOST"
echo "📄 Output: $OUTPUT_FILE"
echo

# Check if mongosh or mongo is available
if command -v mongosh &> /dev/null; then
    MONGO_CLIENT="mongosh"
elif command -v mongo &> /dev/null; then
    MONGO_CLIENT="mongo"
else
    echo "❌ Error: Neither mongosh nor mongo client found!"
    echo "   Please install MongoDB shell"
    exit 1
fi

# Create output directory if needed
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
if [[ ! -d "$OUTPUT_DIR" ]]; then
    mkdir -p "$OUTPUT_DIR"
fi

# Execute currentOp command
echo "🚀 Executing db.currentOp()..."
$MONGO_CLIENT "mongodb://$MONGO_HOST/admin" --eval "printjson(db.currentOp())" --quiet > "$OUTPUT_FILE" 2>&1

# Check if successful
if [[ $? -eq 0 ]] && [[ -s "$OUTPUT_FILE" ]]; then
    FILE_SIZE=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null)
    OPERATION_COUNT=$(grep -o '"opid"' "$OUTPUT_FILE" | wc -l | tr -d ' ')
    
    echo "✅ Success! CurrentOp data collected"
    echo "   📊 File size: ${FILE_SIZE} bytes"
    echo "   🔢 Operations found: ${OPERATION_COUNT}"
    echo "   📂 Saved to: $OUTPUT_FILE"
    echo
    echo "💡 You can now upload this file to the CurrentOp Analyzer web interface"
else
    echo "❌ Error: Failed to collect currentOp data"
    if [[ -f "$OUTPUT_FILE" ]]; then
        echo "   Error details:"
        cat "$OUTPUT_FILE"
        rm "$OUTPUT_FILE"
    fi
    exit 1
fi