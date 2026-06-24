#!/bin/bash
# 1M doc cold aggregation query comparison: with star tree (upgrade API) vs without
set -e
HOST="localhost:9200"
BATCH_LINES=100000

echo "=============================================="
echo "  1M Doc Cold Aggregation Query Benchmark"
echo "=============================================="

# Step 1: Create both indices
echo ""
echo "=== Step 1: Create indices ==="
curl -s -X DELETE "$HOST/bench_no_st" > /dev/null 2>&1 || true
curl -s -X DELETE "$HOST/bench_st" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench_no_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
curl -s -X PUT "$HOST/bench_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created bench_no_st and bench_st"

# Step 2: Generate 1M docs
echo ""
echo "=== Step 2: Generate 1M docs ==="
python3 ReferenceTesting/gen_1m.py > /tmp/bench_1m_data.ndjson
echo "Lines: $(wc -l < /tmp/bench_1m_data.ndjson)"

# Step 3: Ingest into both indices
echo ""
echo "=== Step 3: Ingest into bench_no_st ==="
sed 's/"_index":"ecommerce"/"_index":"bench_no_st"/g; s/"_index": "ecommerce"/"_index": "bench_no_st"/g' \
  /tmp/bench_1m_data.ndjson > /tmp/no_st_data.ndjson
TOTAL=$(wc -l < /tmp/no_st_data.ndjson)
OFF=1; COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/no_st_data.ndjson > /tmp/_b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N)); printf "  %d docs\r" $COUNT; OFF=$((END + 1))
done
echo "  bench_no_st: $COUNT docs"

echo ""
echo "=== Step 4: Ingest into bench_st ==="
sed 's/"_index":"ecommerce"/"_index":"bench_st"/g; s/"_index": "ecommerce"/"_index": "bench_st"/g' \
  /tmp/bench_1m_data.ndjson > /tmp/st_data.ndjson
OFF=1; COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/st_data.ndjson > /tmp/_b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N)); printf "  %d docs\r" $COUNT; OFF=$((END + 1))
done
echo "  bench_st: $COUNT docs"

# Step 5: Force merge bench_no_st
echo ""
echo "=== Step 5: Force merge bench_no_st ==="
curl -s -X POST "$HOST/bench_no_st/_forcemerge?max_num_segments=1" > /dev/null
echo "Done"

# Step 6: Upgrade bench_st
echo ""
echo "=== Step 6: Upgrade bench_st to star tree ==="
UPGRADE_START=$(python3 -c "import time; print(time.time())")
curl -s -X POST "$HOST/bench_st/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
}' > /tmp/_upgrade_resp.json
UPGRADE_END=$(python3 -c "import time; print(time.time())")
echo "Upgrade response:"
python3 -c "import json; print(json.dumps(json.load(open('/tmp/_upgrade_resp.json')), indent=2))"
python3 -c "print(f'Upgrade time: {$UPGRADE_END - $UPGRADE_START:.1f}s')"

sleep 2

# Step 7: Verify
echo ""
echo "=== Step 7: Verify doc counts ==="
echo -n "  bench_no_st: "; curl -s "$HOST/bench_no_st/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])"
echo -n "  bench_st:    "; curl -s "$HOST/bench_st/_count" | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])"


# Step 8: Cold aggregation queries
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "=============================================="
echo "  COLD AGGREGATION QUERY BENCHMARK (1M docs)"
echo "=============================================="

# Clear caches before each run
echo ""
echo "--- WITHOUT star tree (bench_no_st) ---"
for i in 1 2 3 4 5; do
  curl -s -X POST "$HOST/bench_no_st/_cache/clear" > /dev/null
  TOOK=$(curl -s -X POST "$HOST/bench_no_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json; print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${TOOK}ms"
done

echo ""
echo "--- WITH star tree (bench_st) ---"
for i in 1 2 3 4 5; do
  curl -s -X POST "$HOST/bench_st/_cache/clear" > /dev/null
  TOOK=$(curl -s -X POST "$HOST/bench_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json; print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${TOOK}ms"
done

echo ""
echo "=== Done ==="
