#!/bin/bash
# Test per-segment star tree upgrade with 100k ecommerce docs
set -e
HOST="localhost:9200"

echo "=== Step 1: Generate 100k docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_100k.ndjson', 'w') as f:
    for i in range(100000):
        f.write(json.dumps({'index': {'_index': 'ecom_upgrade'}}) + '\n')
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
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
            'customer_id': f'cust_{random.randint(1,1000)}',
            'order_id': f'ord_{i}',
            'type': 'order',
            'user': f'user_{random.randint(1,500)}'
        }) + '\n')
print('Generated 100000 docs')
"

echo ""
echo "=== Step 2: Create index ==="
curl -s -X DELETE "$HOST/ecom_upgrade" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/ecom_upgrade" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created ecom_upgrade"

echo ""
echo "=== Step 3: Ingest 100k docs ==="
TOTAL=$(wc -l < /tmp/test_100k.ndjson)
OFF=1
COUNT=0
BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_100k.ndjson > /tmp/_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 4: Refresh ==="
curl -s -X POST "$HOST/ecom_upgrade/_refresh" > /dev/null

echo ""
echo "=== Step 5: Verify doc count before upgrade ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_upgrade/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 6: Run aggregation BEFORE upgrade ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_price":{"avg":{"field":"taxful_total_price"}},"max_price":{"max":{"field":"taxful_total_price"}},"min_price":{"min":{"field":"taxful_total_price"}}}}}}'
echo "--- Before upgrade ---"
for i in 1 2 3; do
  T=$(curl -s -X POST "$HOST/ecom_upgrade/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Step 7: Upgrade to star tree (TIMED) ==="
START=$(python3 -c "import time; print(time.time())")

curl -s -X POST "$HOST/ecom_upgrade/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
}'

END=$(python3 -c "import time; print(time.time())")
UPGRADE_TIME=$(python3 -c "print(f'{$END - $START:.3f}')")
echo ""
echo "  >>> Upgrade time: ${UPGRADE_TIME}s"

echo ""
echo "=== Step 8: Verify doc count after upgrade ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_upgrade/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 9: Verify star tree files exist ==="
echo "  Segment files:"
ls -la build/testclusters/runTask-0/data/nodes/0/indices/*/0/index/ 2>/dev/null | grep -E "\.ci"

echo ""
echo "=== Step 10: Run aggregation AFTER upgrade ==="
echo "--- After upgrade ---"
for i in 1 2 3; do
  T=$(curl -s -X POST "$HOST/ecom_upgrade/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "import sys,json;print(json.load(sys.stdin)['took'])")
  echo "  Run $i: ${T}ms"
done

echo ""
echo "=== Done ==="
