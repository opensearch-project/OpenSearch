#!/bin/bash
set -e
HOST="localhost:9200"
BATCH_DOCS=10000
BATCH_LINES=$((BATCH_DOCS * 2))

echo "=== Step 1: Generate 1M docs ==="
python3 ReferenceTesting/generate_data.py > /tmp/bench_data.ndjson
TOTAL=$(wc -l < /tmp/bench_data.ndjson)
echo "Done: $TOTAL lines ($((TOTAL/2)) docs)"

echo ""
echo "=== Step 2: Create single index ==="
curl -s -X DELETE "$HOST/bench" --max-time 10 > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench" --max-time 10 -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created bench index"

echo ""
echo "=== Step 3: Ingest 1M docs in batches of $BATCH_DOCS ==="
sed 's/"_index": "ecommerce"/"_index": "bench"/g' /tmp/bench_data.ndjson > /tmp/bench_ingest.ndjson
OFF=1
COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/bench_ingest.ndjson > /tmp/b.ndjson
  N=$(curl -s --max-time 60 -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
  sleep 1
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 4: Verify count ==="
sleep 2
curl -s --max-time 10 "$HOST/bench/_count" | python3 -c "import sys,json;print(f'  Count: {json.load(sys.stdin)[\"count\"]}')"

QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "=========================================="
echo "  BEFORE star tree upgrade"
echo "=========================================="
# Clear caches
curl -s --max-time 10 -X POST "$HOST/_cache/clear" > /dev/null 2>&1
for i in 1 2 3 4 5; do
  T=$(curl -s --max-time 30 -X POST "$HOST/bench/_search?request_cache=false" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Step 5: Force merge + flush before upgrade ==="
echo "  (This commits all data and empties the translog so the upgrade is fast)"
curl -s --max-time 300 -X POST "$HOST/bench/_forcemerge?max_num_segments=1" > /dev/null
curl -s --max-time 30 -X POST "$HOST/bench/_flush?force=true" > /dev/null
echo "  Done"

echo ""
echo "=== Step 6: Upgrade to star tree ==="
curl -s --max-time 300 -X POST "$HOST/bench/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "bench_star_tree",
    "ordered_dimensions": [
      {"name": "customer_gender"},
      {"name": "currency"},
      {"name": "day_of_week"},
      {"name": "order_date"}
    ],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}'
echo ""

echo ""
echo "=========================================="
echo "  AFTER star tree upgrade"
echo "=========================================="
sleep 2
curl -s --max-time 10 -X POST "$HOST/_cache/clear" > /dev/null 2>&1
for i in 1 2 3 4 5; do
  T=$(curl -s --max-time 30 -X POST "$HOST/bench/_search?request_cache=false" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Done ==="
