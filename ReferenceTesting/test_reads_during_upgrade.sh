#!/bin/bash
# Test reads remain available during the ~25s upgrade window
set -e
HOST="localhost:9200"
IDX="ecom_read_test"

echo "=============================================="
echo "  Reads During Upgrade Test (1M docs)"
echo "=============================================="

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

echo "1. Create + ingest 1M docs..."
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/read_test.ndjson', 'w') as f:
    for i in range(1000000):
        dt = base + datetime.timedelta(hours=random.randint(0, 8760))
        f.write(json.dumps({'index': {'_index': '$IDX'}}) + '\n')
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': random.choice(days),
            'order_date': dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10)
        }) + '\n')
"

TOTAL=$(wc -l < /tmp/read_test.ndjson)
OFF=1; COUNT=0; BATCH=100000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/read_test.ndjson > /tmp/_read_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_read_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Ingested: $COUNT docs"
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

echo ""
echo "2. Fire upgrade in background..."
curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [
      {"name": "customer_gender"},
      {"name": "currency"},
      {"name": "day_of_week"},
      {"name": "order_date"}
    ],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "value_count", "min", "max"]},
      {"name": "taxless_total_price", "stats": ["sum", "value_count", "min", "max"]},
      {"name": "total_quantity", "stats": ["sum", "value_count", "min", "max"]},
      {"name": "total_unique_products", "stats": ["sum", "value_count"]}
    ]
  }
}' > /tmp/upgrade_result.json 2>&1 &
UPGRADE_PID=$!
UPGRADE_START=$(python3 -c "import time; print(time.time())")

echo "   Upgrade PID: $UPGRADE_PID"
echo ""
echo "3. Hammering reads while upgrade runs..."

READ_OK=0
READ_FAIL=0
READ_ERRORS=""

while kill -0 $UPGRADE_PID 2>/dev/null; do
  # _count
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" "$HOST/$IDX/_count")
  if [ "$HTTP" = "200" ]; then
    READ_OK=$((READ_OK + 1))
  else
    READ_FAIL=$((READ_FAIL + 1))
    READ_ERRORS="$READ_ERRORS count=$HTTP"
  fi

  # sum agg
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$HOST/$IDX/_search?size=0" \
    -H 'Content-Type: application/json' \
    -d '{"aggs":{"s":{"sum":{"field":"taxful_total_price"}}}}')
  if [ "$HTTP" = "200" ]; then
    READ_OK=$((READ_OK + 1))
  else
    READ_FAIL=$((READ_FAIL + 1))
    READ_ERRORS="$READ_ERRORS agg=$HTTP"
  fi

  # terms agg
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$HOST/$IDX/_search?size=0" \
    -H 'Content-Type: application/json' \
    -d '{"aggs":{"g":{"terms":{"field":"customer_gender"}}}}')
  if [ "$HTTP" = "200" ]; then
    READ_OK=$((READ_OK + 1))
  else
    READ_FAIL=$((READ_FAIL + 1))
    READ_ERRORS="$READ_ERRORS terms=$HTTP"
  fi

  sleep 0.1
done

wait $UPGRADE_PID
UPGRADE_END=$(python3 -c "import time; print(time.time())")
UPGRADE_TIME=$(python3 -c "print(f'{$UPGRADE_END - $UPGRADE_START:.1f}')")

echo ""
echo "=============================================="
echo "  RESULTS"
echo "=============================================="
echo "  Upgrade time: ${UPGRADE_TIME}s"
echo "  Upgrade result: $(cat /tmp/upgrade_result.json)"
echo "  Reads succeeded: $READ_OK"
echo "  Reads failed:    $READ_FAIL"
if [ -n "$READ_ERRORS" ]; then
  echo "  Errors: $READ_ERRORS"
fi
echo ""

if [ "$READ_FAIL" = "0" ] && [ "$READ_OK" -gt "0" ]; then
  echo "  PASS: All $READ_OK reads succeeded during ${UPGRADE_TIME}s upgrade"
else
  echo "  FAIL: $READ_FAIL reads failed out of $((READ_OK + READ_FAIL))"
fi

# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null
rm -f /tmp/upgrade_result.json
