#!/bin/bash
# Star Tree Build Time Comparison
# Approach 1: Upgrade API (ingest → upgrade API → measure build time)
# Approach 2: Traditional Reindex (ingest without ST → create new index with ST → reindex → measure build time)
#
# Requires: OpenSearch running on localhost:9200 with star tree upgrade feature

set -e
HOST="localhost:9200"
BATCH_DOCS=50000
BATCH_LINES=$((BATCH_DOCS * 2))
RESP="/tmp/_build_time_resp.json"

STAR_TREE_CONFIG='{
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
}'

bulk_ingest() {
  local INDEX=$1
  local SOURCE=$2
  local TOTAL
  TOTAL=$(wc -l < "$SOURCE")
  local OFF=1
  local COUNT=0
  while [ $OFF -le $TOTAL ]; do
    local END=$((OFF + BATCH_LINES - 1))
    sed -n "${OFF},${END}p" "$SOURCE" > /tmp/_build_batch.ndjson
    local N
    N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
      --data-binary @/tmp/_build_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
    COUNT=$((COUNT + N))
    printf "  %d docs\r" $COUNT
    OFF=$((END + 1))
  done
  echo "  Total: $COUNT docs ingested into $INDEX"
}

echo "=============================================="
echo "  Star Tree BUILD TIME Comparison Benchmark"
echo "=============================================="
echo ""

# --- Generate data ---
echo "=== Step 1: Generate docs ==="
python3 ReferenceTesting/generate_data.py > /tmp/bench_build.ndjson
DOC_COUNT=$(python3 -c "import math; print(sum(1 for i, l in enumerate(open('/tmp/bench_build.ndjson')) if i % 2 == 1))")
echo "Generated $DOC_COUNT documents"

# =============================================
# APPROACH 1: Upgrade API
# =============================================
echo ""
echo "=============================================="
echo "  APPROACH 1: Upgrade API"
echo "=============================================="

echo ""
echo "--- Creating index: bench_upgrade ---"
curl -s -X DELETE "$HOST/bench_upgrade" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench_upgrade" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "  Created"

echo ""
echo "--- Ingesting docs into bench_upgrade ---"
sed 's/"_index":"ecommerce"/"_index":"bench_upgrade"/g; s/"_index": "ecommerce"/"_index": "bench_upgrade"/g' \
  /tmp/bench_build.ndjson > /tmp/build_upgrade.ndjson
bulk_ingest "bench_upgrade" "/tmp/build_upgrade.ndjson"

echo ""
echo "--- Refreshing ---"
curl -s -X POST "$HOST/bench_upgrade/_refresh" > /dev/null

echo ""
echo "--- Calling upgrade API (timed) ---"
UPGRADE_START=$(python3 -c "import time; print(time.time())")

curl -s -X POST "$HOST/bench_upgrade/_star_tree/upgrade" -H 'Content-Type: application/json' \
  -d "{\"star_tree\": $STAR_TREE_CONFIG}" > $RESP

UPGRADE_END=$(python3 -c "import time; print(time.time())")
UPGRADE_TIME=$(python3 -c "print(f'{$UPGRADE_END - $UPGRADE_START:.3f}')")
echo "  Upgrade API response:"
python3 -c "import json; print(json.dumps(json.load(open('$RESP')), indent=2))"
echo ""
echo "  >>> UPGRADE API build time: ${UPGRADE_TIME}s"

# Verify doc count
echo ""
echo "--- Verifying bench_upgrade doc count ---"
curl -s "$HOST/bench_upgrade/_count" > $RESP
python3 -c "import json; print(f'  Doc count: {json.load(open(\"$RESP\"))[\"count\"]}')"

# =============================================
# APPROACH 2: Traditional Reindex
# =============================================
echo ""
echo "=============================================="
echo "  APPROACH 2: Traditional Reindex"
echo "=============================================="
echo ""
echo "--- NOTE: Requires server restart with star tree settings ---"
echo "--- Please restart OpenSearch now, then press Enter to continue ---"
read -r

echo ""
echo "--- Waiting for cluster to be ready ---"
until curl -s "$HOST/_cluster/health" > /dev/null 2>&1; do
  sleep 2
  echo "  Waiting..."
done
echo "  Cluster is ready"

echo ""
echo "--- Creating source index: bench_reindex_src (no star tree) ---"
curl -s -X DELETE "$HOST/bench_reindex_src" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench_reindex_src" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "  Created"

echo ""
echo "--- Ingesting docs into bench_reindex_src ---"
sed 's/"_index":"ecommerce"/"_index":"bench_reindex_src"/g; s/"_index": "ecommerce"/"_index": "bench_reindex_src"/g' \
  /tmp/bench_build.ndjson > /tmp/build_reindex_src.ndjson
bulk_ingest "bench_reindex_src" "/tmp/build_reindex_src.ndjson"

echo ""
echo "--- Refreshing source ---"
curl -s -X POST "$HOST/bench_reindex_src/_refresh" > /dev/null

echo ""
echo "--- Creating target index: bench_reindex_dst (WITH star tree at creation) ---"
curl -s -X DELETE "$HOST/bench_reindex_dst" > /dev/null 2>&1 || true

cat <<'MAPPING_EOF' > /tmp/bench_st_mappings.json
{
  "settings": {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index.composite_index": true,
    "index.append_only.enabled": true
  },
  "mappings": {
    "composite": {
      "bench_star_tree": {
        "type": "star_tree",
        "config": {
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
      }
    },
    "properties": {
      "category": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "currency": { "type": "keyword" },
      "customer_birth_date": { "type": "date" },
      "customer_first_name": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "customer_full_name": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "customer_gender": { "type": "keyword" },
      "customer_id": { "type": "keyword" },
      "customer_last_name": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "customer_phone": { "type": "keyword" },
      "day_of_week": { "type": "keyword" },
      "day_of_week_i": { "type": "integer" },
      "email": { "type": "keyword" },
      "manufacturer": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "order_date": { "type": "date" },
      "order_id": { "type": "keyword" },
      "sku": { "type": "keyword" },
      "taxful_total_price": { "type": "half_float" },
      "taxless_total_price": { "type": "half_float" },
      "total_quantity": { "type": "integer" },
      "total_unique_products": { "type": "integer" },
      "type": { "type": "keyword" },
      "user": { "type": "keyword" }
    }
  }
}
MAPPING_EOF

curl -s -X PUT "$HOST/bench_reindex_dst" -H 'Content-Type: application/json' \
  -d @/tmp/bench_st_mappings.json > $RESP
echo "  Created with star tree config:"
python3 -c "import json; print(json.dumps(json.load(open('$RESP')), indent=2))"

echo ""
echo "--- Reindexing bench_reindex_src → bench_reindex_dst (timed) ---"
REINDEX_START=$(python3 -c "import time; print(time.time())")

curl -s -X POST "$HOST/_reindex?wait_for_completion=true" -H 'Content-Type: application/json' -d '{
  "source": { "index": "bench_reindex_src" },
  "dest": { "index": "bench_reindex_dst" }
}' > $RESP

REINDEX_END=$(python3 -c "import time; print(time.time())")
REINDEX_TIME=$(python3 -c "print(f'{$REINDEX_END - $REINDEX_START:.3f}')")
echo "  Reindex response:"
python3 -c "import json; d=json.load(open('$RESP')); print(f'  took={d.get(\"took\",\"?\")}ms, created={d.get(\"created\",\"?\")}, failures={len(d.get(\"failures\",[]))}')"

echo ""
echo "--- Force merging bench_reindex_dst to 1 segment (timed) ---"
MERGE_START=$(python3 -c "import time; print(time.time())")

curl -s -X POST "$HOST/bench_reindex_dst/_forcemerge?max_num_segments=1" > /dev/null

MERGE_END=$(python3 -c "import time; print(time.time())")
MERGE_TIME=$(python3 -c "print(f'{$MERGE_END - $MERGE_START:.3f}')")
echo "  Force merge time: ${MERGE_TIME}s"

REINDEX_TOTAL=$(python3 -c "print(f'{$MERGE_END - $REINDEX_START:.3f}')")
echo ""
echo "  >>> REINDEX approach total time: ${REINDEX_TOTAL}s"
echo "      (reindex: ${REINDEX_TIME}s + force merge: ${MERGE_TIME}s)"

# Verify doc count
echo ""
echo "--- Verifying bench_reindex_dst doc count ---"
curl -s "$HOST/bench_reindex_dst/_count" > $RESP
python3 -c "import json; print(f'  Doc count: {json.load(open(\"$RESP\"))[\"count\"]}')"

# =============================================
# SUMMARY
# =============================================
echo ""
echo "=============================================="
echo "  RESULTS SUMMARY ($DOC_COUNT documents)"
echo "=============================================="
echo ""
echo "  Upgrade API build time:     ${UPGRADE_TIME}s"
echo "  Reindex approach total:     ${REINDEX_TOTAL}s"
echo "    - Reindex time:           ${REINDEX_TIME}s"
echo "    - Force merge time:       ${MERGE_TIME}s"
echo ""
SPEEDUP=$(python3 -c "
u = $UPGRADE_TIME
r = $REINDEX_TOTAL
if u > 0:
    print(f'  Reindex/Upgrade ratio: {r/u:.2f}x')
else:
    print('  (upgrade was instant)')
")
echo "$SPEEDUP"
echo ""

# Cleanup temp files
rm -f $RESP /tmp/_build_batch.ndjson /tmp/build_upgrade.ndjson /tmp/build_reindex_src.ndjson /tmp/bench_st_mappings.json

echo "=== Done ==="
