#!/bin/bash
# Usage: ./scripts/generate-test-csv.sh <count> <output>
COUNT="${1:-100000}"
OUT="${2:-/tmp/test.csv}"
echo "externalId,name,value,category,eventTs" > "$OUT"
for ((i=1; i<=COUNT; i++)); do
  echo "EXT-$(printf '%08d' $i),Name $i,$(echo "scale=4; $i*3.14/100" | bc),CAT$(($i%5+1)),2024-01-01T10:00:00Z"
done >> "$OUT"
echo "Generated $COUNT records → $OUT ($(du -sh "$OUT" | cut -f1))"
