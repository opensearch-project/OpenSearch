#!/bin/bash
# Star Tree Upgrade Benchmark — compares query times with and without star tree

set -e
HOST="localhost:9200"

echo "=== Generating 10000 dummy documents ==="
python3 ReferenceTesting/generate_data.py > /tmp/benchmark_data.ndjson
echo "Generated $(wc -l < /tmp/benchmark_data.ndjson) lines"

echo ""
echo "=== Creating index WITHOUT star tree: ecommerce_no_st ==="
curl -s -X DELETE "$HOST/ecommerce_no_st" 2>/dev/null || true
curl -s -X PUT "$HOST/ecommerce_no_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json
echo ""

echo "=== Creating index WITH star tree upgrade: ecommerce_st ==="
curl -s -X DELETE "$HOST/ecommerce_st" 2>/dev/null || true
curl -s -X PUT "$HOST/ecommerce_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json
echo ""

echo "=== Ingesting 10000 docs into both indexes ==="
# Ingest into no_st
sed 's/"_index":"ecommerce"/"_index":"ecommerce_no_st"/g' /tmp/benchmark_data.ndjson > /tmp/data_no_st.ndjson
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
  --data-binary @/tmp/data_no_st.ndjson | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  no_st: errors={d[\"errors\"]}, items={len(d[\"items\"])}')"

# Ingest into st
sed 's/"_index":"ecommerce"/"_index":"ecommerce_st"/g' /tmp/benchmark_data.ndjson > /tmp/data_st.ndjson
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
  --data-binary @/tmp/data_st.ndjson | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  st: errors={d[\"errors\"]}, items={len(d[\"items\"])}')"

echo ""
echo "=== Force merge both to 1 segment ==="
curl -s -X POST "$HOST/ecommerce_no_st/_forcemerge?max_num_segments=1" > /dev/null
curl -s -X POST "$HOST/ecommerce_st/_forcemerge?max_num_segments=1" > /dev/null
echo "Done"

echo ""
echo "=== Upgrading ecommerce_st to star tree ==="
curl -s -X POST "$HOST/ecommerce_st/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecommerce_star_tree",
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

# Wait for refresh
sleep 2

QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"total_revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "=== Running aggregation query 5 times on each index ==="
echo ""
echo "--- WITHOUT star tree (ecommerce_no_st) ---"
for i in 1 2 3 4 5; do
  TOOK=$(curl -s -X POST "$HOST/ecommerce_no_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json; print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${TOOK}ms"
done

echo ""
echo "--- WITH star tree (ecommerce_st) ---"
for i in 1 2 3 4 5; do
  TOOK=$(curl -s -X POST "$HOST/ecommerce_st/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json; print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${TOOK}ms"
done

echo ""
echo "=== Done ==="
