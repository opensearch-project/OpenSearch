#!/bin/bash
set -e
HOST="localhost:9200"
BATCH_DOCS=50000
BATCH_LINES=$((BATCH_DOCS * 2))

echo "=== Step 1: Generate 1M docs ==="
python3 ReferenceTesting/generate_data.py > /tmp/bench_1m.ndjson
echo "Done: $(wc -l < /tmp/bench_1m.ndjson) lines"

echo ""
echo "=== Step 2: Create indexes ==="
curl -s -X DELETE "$HOST/bench_no_st" > /dev/null 2>&1 || true
curl -s -X DELETE "$HOST/bench_st" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench_no_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
curl -s -X PUT "$HOST/bench_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created bench_no_st and bench_st"

echo ""
echo "=== Step 3: Ingest 1M docs into bench_no_st ==="
sed 's/"_index": "ecommerce"/"_index": "bench_no_st"/g' /tmp/bench_1m.ndjson > /tmp/no_st.ndjson
TOTAL=$(wc -l < /tmp/no_st.ndjson)
OFF=1
COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/no_st.ndjson > /tmp/b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 4: Ingest 1M docs into bench_st ==="
sed 's/"_index": "ecommerce"/"_index": "bench_st"/g' /tmp/bench_1m.ndjson > /tmp/st.ndjson
OFF=1
COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/st.ndjson > /tmp/b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 5: Force merge both to 1 segment ==="
curl -s -X POST "$HOST/bench_no_st/_forcemerge?max_num_segments=1" > /dev/null
echo "  bench_no_st merged"
curl -s -X POST "$HOST/bench_st/_forcemerge?max_num_segments=1" > /dev/null
echo "  bench_st merged"

echo ""
echo "=== Step 6: Upgrade bench_st to star tree ==="
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
}'
echo ""
sleep 2

echo ""
echo "=== Step 7: Verify doc counts ==="
echo -n "  bench_no_st: "
curl -s "$HOST/bench_no_st/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"
echo -n "  bench_st:    "
curl -s "$HOST/bench_st/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "=========================================="
echo "  BENCHMARK: 1M docs aggregation query"
echo "=========================================="
echo ""
echo "--- WITHOUT star tree (bench_no_st) ---"
for i in 1 2 3 4 5; do
  T=$(curl -s -X POST "$HOST/bench_no_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "--- WITH star tree (bench_st) ---"
for i in 1 2 3 4 5; do
  T=$(curl -s -X POST "$HOST/bench_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Done ==="
