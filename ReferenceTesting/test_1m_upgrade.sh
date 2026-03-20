#!/bin/bash
# Test per-segment star tree upgrade with 1M ecommerce docs — cold comparison
set -e
HOST="localhost:9200"

echo "=============================================="
echo "  1M Doc Per-Segment Star Tree Upgrade Test"
echo "  (Cold comparison — caches cleared each run)"
echo "=============================================="

echo ""
echo "=== Step 1: Generate 1M docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_1m.ndjson', 'w') as f:
    for i in range(1000000):
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({'index': {'_index': 'ecom_1m'}}) + '\n')
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': random.choice(days),
            'order_date': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10),
            'day_of_week_i': random.randint(0, 6),
            'customer_id': 'cust_' + str(random.randint(1,5000)),
            'order_id': 'ord_' + str(i),
            'type': 'order',
            'user': 'user_' + str(random.randint(1,2000))
        }) + '\n')
print('Generated 1,000,000 docs')
"

echo ""
echo "=== Step 2: Create index ==="
curl -s -X DELETE "$HOST/ecom_1m" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/ecom_1m" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created ecom_1m"

echo ""
echo "=== Step 3: Ingest 1M docs ==="
TOTAL=$(wc -l < /tmp/test_1m.ndjson)
OFF=1
COUNT=0
BATCH=100000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_1m.ndjson > /tmp/_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 4: Refresh ==="
curl -s -X POST "$HOST/ecom_1m/_refresh" > /dev/null

echo ""
echo "=== Step 5: Verify doc count ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_1m/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 6: Segment info before upgrade ==="
curl -s "$HOST/ecom_1m/_segments" | python3 -c "
import sys,json
d = json.load(sys.stdin)
for idx_name, idx in d['indices'].items():
    for shard_id, shards in idx['shards'].items():
        for shard in shards:
            segs = shard['segments']
            print(f'  {len(segs)} segments:')
            for name, seg in segs.items():
                print(f'    {name}: {seg[\"num_docs\"]} docs, compound={seg[\"compound\"]}')
"

QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'

echo ""
echo "=== Step 7: Cold aggregation BEFORE upgrade ==="
echo "--- Before upgrade (cold — caches cleared each run) ---"
for i in 1 2 3 4 5; do
  curl -s -X POST "$HOST/ecom_1m/_cache/clear" > /dev/null
  sleep 1
  T=$(curl -s -X POST "$HOST/ecom_1m/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Step 8: Upgrade to star tree (TIMED) ==="
START=$(python3 -c "import time; print(time.time())")

RESP=$(curl -s -X POST "$HOST/ecom_1m/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
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

END=$(python3 -c "import time; print(time.time())")
UPGRADE_TIME=$(python3 -c "print(f'{$END - $START:.3f}')")
echo "  Response: $RESP"
echo "  >>> Upgrade time: ${UPGRADE_TIME}s"

echo ""
echo "=== Step 9: Verify doc count after upgrade ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_1m/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 10: Cold aggregation AFTER upgrade ==="
echo "--- After upgrade (cold — caches cleared each run) ---"
for i in 1 2 3 4 5; do
  curl -s -X POST "$HOST/ecom_1m/_cache/clear" > /dev/null
  sleep 1
  T=$(curl -s -X POST "$HOST/ecom_1m/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=============================================="
echo "  SUMMARY"
echo "=============================================="
echo "  Docs: 1,000,000"
echo "  Upgrade time: ${UPGRADE_TIME}s"
echo "=============================================="
