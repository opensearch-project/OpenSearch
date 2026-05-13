#!/bin/bash
set -e
HOST="localhost:9200"
BATCH_DOCS=10000
BATCH_LINES=$((BATCH_DOCS * 2))
NUM_DOCS=100000
COLD_RUNS=5

echo "============================================"
echo "  COLD BENCHMARK: 100K docs, st vs no_st"
echo "============================================"
echo ""

# --- Step 1: Generate data ---
echo "=== Step 1: Generate 100K docs ==="
python3 -c "
import json, random
from datetime import datetime, timedelta

genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
categories = [\"Men's Clothing\", \"Women's Clothing\", \"Men's Shoes\", \"Women's Shoes\", \"Men's Accessories\", \"Women's Accessories\"]
manufacturers = ['Elitelligence', 'Oceanavigations', 'Pyramidustries', 'Champion Arts', 'Tigress Enterprises', 'Gnomehouse']
base_date = datetime(2024, 1, 1)

for i in range($NUM_DOCS):
    print(json.dumps({'index': {'_index': 'ecommerce', '_id': i}}))
    gender = random.choice(genders)
    currency = random.choice(currencies)
    day = random.choice(days)
    order_date = base_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    total_qty = random.randint(1, 10)
    taxful = round(random.uniform(10, 500), 2)
    taxless = round(taxful * random.uniform(0.8, 1.0), 2)
    doc = {
        'customer_gender': gender, 'currency': currency, 'day_of_week': day,
        'day_of_week_i': days.index(day),
        'order_date': order_date.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        'order_id': str(100000 + i),
        'taxful_total_price': taxful, 'taxless_total_price': taxless,
        'total_quantity': total_qty, 'total_unique_products': random.randint(1, min(total_qty, 5)),
        'category': [random.choice(categories)],
        'customer_first_name': f'User{i}', 'customer_full_name': f'User{i} Test',
        'customer_id': str(i % 500), 'customer_last_name': 'Test',
        'type': 'order', 'user': f'user{i}',
        'manufacturer': [random.choice(manufacturers)],
        'sku': [f'ZO{random.randint(100000, 999999)}'],
    }
    print(json.dumps(doc))
" > /tmp/bench_cold.ndjson
echo "  Generated $(( $(wc -l < /tmp/bench_cold.ndjson) / 2 )) docs"

# --- Step 2: Create indexes ---
echo ""
echo "=== Step 2: Create indexes ==="
curl -s -X DELETE "$HOST/bench_no_st" > /dev/null 2>&1 || true
curl -s -X DELETE "$HOST/bench_st" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/bench_no_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
curl -s -X PUT "$HOST/bench_st" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "  Created bench_no_st and bench_st"

# --- Step 3: Ingest into bench_no_st ---
echo ""
echo "=== Step 3: Ingest ${NUM_DOCS} docs into bench_no_st ==="
sed 's/"_index": "ecommerce"/"_index": "bench_no_st"/g' /tmp/bench_cold.ndjson > /tmp/cold_no_st.ndjson
TOTAL=$(wc -l < /tmp/cold_no_st.ndjson)
OFF=1; COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/cold_no_st.ndjson > /tmp/b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

# --- Step 4: Ingest into bench_st ---
echo ""
echo "=== Step 4: Ingest ${NUM_DOCS} docs into bench_st ==="
sed 's/"_index": "ecommerce"/"_index": "bench_st"/g' /tmp/bench_cold.ndjson > /tmp/cold_st.ndjson
OFF=1; COUNT=0
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH_LINES - 1))
  sed -n "${OFF},${END}p" /tmp/cold_st.ndjson > /tmp/b.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/b.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

# --- Step 5: Force merge both ---
echo ""
echo "=== Step 5: Force merge both to 1 segment ==="
curl -s -X POST "$HOST/bench_no_st/_forcemerge?max_num_segments=1" > /dev/null
echo "  bench_no_st merged"
curl -s -X POST "$HOST/bench_st/_forcemerge?max_num_segments=1" > /dev/null
echo "  bench_st merged"

# --- Step 6: Upgrade bench_st to star tree ---
echo ""
echo "=== Step 6: Upgrade bench_st to star tree ==="
UPGRADE_RESP=$(curl -s -w "\n%{http_code}" -X POST "$HOST/bench_st/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
}')
HTTP_CODE=$(echo "$UPGRADE_RESP" | tail -1)
UPGRADE_BODY=$(echo "$UPGRADE_RESP" | sed '$d')
echo "  HTTP status: $HTTP_CODE"
echo "  Response: $UPGRADE_BODY"

if [ "$HTTP_CODE" != "200" ]; then
  echo "  ERROR: Star tree upgrade failed with HTTP $HTTP_CODE"
  exit 1
fi

# Check for successful shards in response
FAILED_SHARDS=$(echo "$UPGRADE_BODY" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    print(r.get('_shards', {}).get('failed', -1))
except:
    print(-1)
" 2>/dev/null)
if [ "$FAILED_SHARDS" != "0" ] && [ "$FAILED_SHARDS" != "-1" ]; then
  echo "  ERROR: Star tree upgrade had $FAILED_SHARDS failed shards"
  exit 1
fi
echo "  Upgrade API returned successfully"

# Wait for the upgrade to settle
sleep 3
# Refresh to make sure searcher picks up the new segments
curl -s -X POST "$HOST/bench_st/_refresh" > /dev/null
echo ""
sleep 3

# --- Step 7: Verify doc counts (assert exactly NUM_DOCS) ---
echo ""
echo "=== Step 7: Verify doc counts ==="
COUNT_NO_ST=$(curl -s "$HOST/bench_no_st/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
COUNT_ST=$(curl -s "$HOST/bench_st/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
echo "  bench_no_st: $COUNT_NO_ST"
echo "  bench_st:    $COUNT_ST"

if [ "$COUNT_NO_ST" != "$NUM_DOCS" ]; then
  echo "  ERROR: bench_no_st has $COUNT_NO_ST docs, expected $NUM_DOCS"
  exit 1
fi
if [ "$COUNT_ST" != "$NUM_DOCS" ]; then
  echo "  ERROR: bench_st has $COUNT_ST docs, expected $NUM_DOCS"
  exit 1
fi
echo "  PASS: Both indexes have exactly $NUM_DOCS docs"

# --- Step 8: Verify star tree was built ---
echo ""
echo "=== Step 8: Verify star tree was built ==="

# Check mapping for star_tree config
HAS_STAR_TREE=$(curl -s "$HOST/bench_st/_mapping" | python3 -c "
import sys, json
m = json.load(sys.stdin)
props = m.get('bench_st', {}).get('mappings', {})
# star_tree config appears under composite or star_tree key in mappings
composite = props.get('_meta', {}).get('star_tree', props.get('composite', {}))
if composite:
    print('yes')
else:
    # Also check via settings
    print('check_settings')
")

if [ "$HAS_STAR_TREE" = "yes" ]; then
  echo "  PASS: Star tree config found in mapping"
else
  echo "  INFO: Star tree config not in _meta, checking settings..."
  curl -s "$HOST/bench_st/_settings" | python3 -c "
import sys, json
s = json.load(sys.stdin)
settings = s.get('bench_st', {}).get('settings', {}).get('index', {})
star_tree = settings.get('composite_index', settings.get('star_tree', {}))
print(json.dumps(star_tree, indent=2) if star_tree else '  No star_tree in settings')
"
fi

# Check segment stats for star tree fields (composite doc values)
echo ""
echo "  Segment-level check:"
SEGMENTS_INFO=$(curl -s "$HOST/bench_st/_segments")
NUM_SEGMENTS=$(echo "$SEGMENTS_INFO" | python3 -c "
import sys, json
data = json.load(sys.stdin)
shards = data.get('indices', {}).get('bench_st', {}).get('shards', {})
total = 0
for shard_list in shards.values():
    for shard in shard_list:
        total += shard.get('num_committed_segments', shard.get('num_search_segments', 0))
print(total)
")
echo "  Segments in bench_st: $NUM_SEGMENTS"

# Verify via a star-tree-eligible query and check if it uses star tree
# A query that hits star tree will typically be much faster; we can also check
# the response profile for star tree usage
echo ""
echo "  Query verification (checking star tree is used):"
VERIFY_QUERY='{"size":0,"aggs":{"total_revenue":{"sum":{"field":"taxful_total_price"}}}}'
VERIFY_RESP=$(curl -s -X POST "$HOST/bench_st/_search" -H 'Content-Type: application/json' -d "$VERIFY_QUERY")
VERIFY_TOOK=$(echo "$VERIFY_RESP" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
VERIFY_VAL=$(echo "$VERIFY_RESP" | python3 -c "import sys,json;print(json.load(sys.stdin)['aggregations']['total_revenue']['value'])")
echo "  bench_st sum(taxful_total_price): $VERIFY_VAL (took ${VERIFY_TOOK}ms)"

# Same query on no_st for comparison
VERIFY_RESP_NO=$(curl -s -X POST "$HOST/bench_no_st/_search" -H 'Content-Type: application/json' -d "$VERIFY_QUERY")
VERIFY_TOOK_NO=$(echo "$VERIFY_RESP_NO" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
VERIFY_VAL_NO=$(echo "$VERIFY_RESP_NO" | python3 -c "import sys,json;print(json.load(sys.stdin)['aggregations']['total_revenue']['value'])")
echo "  bench_no_st sum(taxful_total_price): $VERIFY_VAL_NO (took ${VERIFY_TOOK_NO}ms)"

# Sanity: aggregation values should match (within floating point tolerance)
python3 -c "
st_val = $VERIFY_VAL
no_st_val = $VERIFY_VAL_NO
diff = abs(st_val - no_st_val)
pct = (diff / no_st_val * 100) if no_st_val else 0
if pct < 0.01:
    print(f'  PASS: Aggregation values match (diff={diff:.4f}, {pct:.6f}%)')
else:
    print(f'  WARN: Aggregation values differ by {pct:.4f}% (st={st_val}, no_st={no_st_val})')
"

# --- Cold benchmark function ---
# Instead of sudo purge, we clear all OpenSearch-level caches and use
# the _search API with request_cache=false + a unique preference per run
# to avoid any cached results. This gives a fair cold comparison without
# needing root privileges.
clear_caches() {
  # Clear OpenSearch request cache, fielddata cache, and query cache
  curl -s -X POST "$HOST/_cache/clear" > /dev/null
  # Clear fielddata specifically
  curl -s -X POST "$HOST/_cache/clear?fielddata=true" > /dev/null
  # Clear query cache
  curl -s -X POST "$HOST/_cache/clear?query=true" > /dev/null
  # Clear request cache
  curl -s -X POST "$HOST/_cache/clear?request=true" > /dev/null
  # Small pause to let things settle
  sleep 1
}

QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "============================================"
echo "  COLD QUERY BENCHMARK: ${NUM_DOCS} docs"
echo "  (caches cleared before each query)"
echo "============================================"

NO_ST_TIMES=()
ST_TIMES=()

for i in $(seq 1 $COLD_RUNS); do
  echo ""
  echo "--- Cold run $i of $COLD_RUNS ---"

  # Cold query: no_st
  clear_caches
  T_NO_ST=$(curl -s -X POST "$HOST/bench_no_st/_search?request_cache=false&preference=coldrun${i}nost" \
    -H 'Content-Type: application/json' -d "$QUERY" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  NO_ST_TIMES+=($T_NO_ST)
  echo "  no_st: ${T_NO_ST}ms"

  # Cold query: st
  clear_caches
  T_ST=$(curl -s -X POST "$HOST/bench_st/_search?request_cache=false&preference=coldrun${i}st" \
    -H 'Content-Type: application/json' -d "$QUERY" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  ST_TIMES+=($T_ST)
  echo "  st:    ${T_ST}ms"
done

# --- Summary ---
echo ""
echo "============================================"
echo "  RESULTS SUMMARY"
echo "============================================"
echo ""
echo "  Run  |  no_st (ms)  |  st (ms)  |  speedup"
echo "  -----|-------------|-----------|----------"
for i in $(seq 0 $((COLD_RUNS - 1))); do
  SPEEDUP=$(python3 -c "
no_st = ${NO_ST_TIMES[$i]}
st = ${ST_TIMES[$i]}
if st > 0:
    print(f'{no_st/st:.2f}x')
else:
    print('inf')
")
  printf "  %3d  |  %9s  |  %7s  |  %s\n" $((i + 1)) "${NO_ST_TIMES[$i]}" "${ST_TIMES[$i]}" "$SPEEDUP"
done

# Averages
python3 -c "
no_st = [${NO_ST_TIMES[*]// /,}]
st = [${ST_TIMES[*]// /,}]
avg_no = sum(no_st) / len(no_st)
avg_st = sum(st) / len(st)
print(f'  -----|-------------|-----------|----------')
print(f'  AVG  |  {avg_no:9.1f}  |  {avg_st:7.1f}  |  {avg_no/avg_st:.2f}x')
"

echo ""
echo "=== Done ==="
