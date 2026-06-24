#!/bin/bash
# Test that reads and writes remain available throughout the upgrade
# Strategy: 1M docs → start background readers + writers → upgrade → verify
set -e
HOST="localhost:9200"
IDX="test_rw_upgrade"

echo "=============================================="
echo "  Read/Write Availability During Upgrade Test"
echo "=============================================="

curl -s -X DELETE "$HOST/$IDX" > /dev/null 2>&1 || true

echo ""
echo "=== Step 1: Create index + ingest 1M docs ==="
curl -s -X PUT "$HOST/$IDX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/rw_test.ndjson', 'w') as f:
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

TOTAL=$(wc -l < /tmp/rw_test.ndjson)
OFF=1; COUNT=0; BATCH=100000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/rw_test.ndjson > /tmp/_rw_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_rw_batch.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Ingested: $COUNT docs"
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null

INITIAL_COUNT=$(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
echo "  Initial doc count: $INITIAL_COUNT"

echo ""
echo "=== Step 2: Start background readers ==="
# Reader: continuously runs _count and a sum aggregation, logs failures
rm -f /tmp/rw_read_log.txt
(
  read_ok=0
  read_fail=0
  while [ -f /tmp/rw_upgrade_running ]; do
    # Test 1: _count
    RESP=$(curl -s -w "\n%{http_code}" "$HOST/$IDX/_count" 2>/dev/null)
    HTTP=$(echo "$RESP" | tail -1)
    if [ "$HTTP" = "200" ]; then
      read_ok=$((read_ok + 1))
    else
      read_fail=$((read_fail + 1))
      echo "READ_FAIL count http=$HTTP" >> /tmp/rw_read_log.txt
    fi

    # Test 2: sum aggregation
    RESP=$(curl -s -w "\n%{http_code}" -X POST "$HOST/$IDX/_search?size=0" \
      -H 'Content-Type: application/json' \
      -d '{"aggs":{"s":{"sum":{"field":"taxful_total_price"}}}}' 2>/dev/null)
    HTTP=$(echo "$RESP" | tail -1)
    if [ "$HTTP" = "200" ]; then
      read_ok=$((read_ok + 1))
    else
      read_fail=$((read_fail + 1))
      echo "READ_FAIL agg http=$HTTP" >> /tmp/rw_read_log.txt
    fi

    sleep 0.2
  done
  echo "$read_ok" > /tmp/rw_read_ok.txt
  echo "$read_fail" > /tmp/rw_read_fail.txt
) &
READER_PID=$!

echo ""
echo "=== Step 3: Start background writers ==="
# Writer: continuously indexes small batches of docs with a known marker
rm -f /tmp/rw_write_log.txt
(
  write_ok=0
  write_fail=0
  batch_num=0
  while [ -f /tmp/rw_upgrade_running ]; do
    batch_num=$((batch_num + 1))
    RESP=$(curl -s -w "\n%{http_code}" -X POST "$HOST/$IDX/_bulk" \
      -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"customer_gender":"WRITE_TEST","currency":"TEST","day_of_week":"Monday","taxful_total_price":1.0,"taxless_total_price":1.0,"total_quantity":1,"total_unique_products":1}
{"index":{}}
{"customer_gender":"WRITE_TEST","currency":"TEST","day_of_week":"Monday","taxful_total_price":1.0,"taxless_total_price":1.0,"total_quantity":1,"total_unique_products":1}
' 2>/dev/null)
    HTTP=$(echo "$RESP" | tail -1)
    if [ "$HTTP" = "200" ]; then
      # Check for errors in bulk response
      BODY=$(echo "$RESP" | head -n -1)
      ERRORS=$(echo "$BODY" | python3 -c "import sys,json;print(json.load(sys.stdin).get('errors',True))" 2>/dev/null)
      if [ "$ERRORS" = "False" ]; then
        write_ok=$((write_ok + 2))
      else
        write_fail=$((write_fail + 2))
        echo "WRITE_FAIL bulk_errors batch=$batch_num" >> /tmp/rw_write_log.txt
      fi
    else
      write_fail=$((write_fail + 2))
      echo "WRITE_FAIL http=$HTTP batch=$batch_num" >> /tmp/rw_write_log.txt
    fi

    sleep 0.5
  done
  echo "$write_ok" > /tmp/rw_write_ok.txt
  echo "$write_fail" > /tmp/rw_write_fail.txt
) &
WRITER_PID=$!

# Signal that upgrade is running
touch /tmp/rw_upgrade_running
sleep 5
echo "  Background processes running for 5s before upgrade..."

echo ""
echo "=== Step 4: Run upgrade (readers + writers active) ==="
START=$(python3 -c "import time; print(time.time())")

UPGRADE_RESP=$(curl -s -X POST "$HOST/$IDX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecom_star_tree",
    "ordered_dimensions": [
      {"name": "customer_gender"},
      {"name": "currency"},
      {"name": "day_of_week"}
    ],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "value_count", "min", "max"]},
      {"name": "total_quantity", "stats": ["sum", "value_count"]}
    ]
  }
}')

END=$(python3 -c "import time; print(time.time())")
UPGRADE_TIME=$(python3 -c "print(f'{$END - $START:.3f}')")
echo "  Upgrade time: ${UPGRADE_TIME}s"
echo "  Response: $UPGRADE_RESP"

# Let readers/writers run a bit more after upgrade
sleep 5
echo "  Background processes ran for 5s after upgrade..."

# Stop background processes
rm -f /tmp/rw_upgrade_running
sleep 2
wait $READER_PID 2>/dev/null || true
wait $WRITER_PID 2>/dev/null || true

echo ""
echo "=== Step 5: Results ==="

READ_OK=$(cat /tmp/rw_read_ok.txt 2>/dev/null || echo "0")
READ_FAIL=$(cat /tmp/rw_read_fail.txt 2>/dev/null || echo "0")
WRITE_OK=$(cat /tmp/rw_write_ok.txt 2>/dev/null || echo "0")
WRITE_FAIL=$(cat /tmp/rw_write_fail.txt 2>/dev/null || echo "0")

echo "  Reads:  $READ_OK succeeded, $READ_FAIL failed"
echo "  Writes: $WRITE_OK succeeded, $WRITE_FAIL failed"

if [ -f /tmp/rw_read_log.txt ]; then
  echo "  Read failures:"
  cat /tmp/rw_read_log.txt | head -10
fi
if [ -f /tmp/rw_write_log.txt ]; then
  echo "  Write failures:"
  cat /tmp/rw_write_log.txt | head -10
fi

# Refresh and check that written docs are visible
curl -s -X POST "$HOST/$IDX/_refresh" > /dev/null
FINAL_COUNT=$(curl -s "$HOST/$IDX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
WRITE_TEST_COUNT=$(curl -s -X POST "$HOST/$IDX/_search?size=0" -H 'Content-Type: application/json' -d '{
  "query": {"term": {"customer_gender": "WRITE_TEST"}}
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['hits']['total']['value'])")

echo ""
echo "  Initial docs: $INITIAL_COUNT"
echo "  Final docs:   $FINAL_COUNT"
echo "  WRITE_TEST docs found: $WRITE_TEST_COUNT (expected: $WRITE_OK)"
echo "  New docs added during upgrade: $((FINAL_COUNT - INITIAL_COUNT))"

echo ""
echo "=== VERIFICATION ==="
PASS=0; FAIL=0

if [ "$READ_FAIL" = "0" ]; then
  echo "  PASS: Zero read failures"
  PASS=$((PASS + 1))
else
  echo "  FAIL: $READ_FAIL read failures"
  FAIL=$((FAIL + 1))
fi

if [ "$WRITE_FAIL" = "0" ]; then
  echo "  PASS: Zero write failures"
  PASS=$((PASS + 1))
else
  echo "  FAIL: $WRITE_FAIL write failures"
  FAIL=$((FAIL + 1))
fi

if [ "$WRITE_TEST_COUNT" = "$WRITE_OK" ]; then
  echo "  PASS: All written docs visible ($WRITE_TEST_COUNT)"
  PASS=$((PASS + 1))
else
  echo "  FAIL: Written docs mismatch (visible=$WRITE_TEST_COUNT, written=$WRITE_OK)"
  FAIL=$((FAIL + 1))
fi

UPGRADE_SUCCESS=$(echo "$UPGRADE_RESP" | python3 -c "import sys,json;r=json.load(sys.stdin);print(r.get('_shards',{}).get('failed',1))")
if [ "$UPGRADE_SUCCESS" = "0" ]; then
  echo "  PASS: Upgrade succeeded"
  PASS=$((PASS + 1))
else
  echo "  FAIL: Upgrade had failures"
  FAIL=$((FAIL + 1))
fi

echo ""
echo "=== RESULTS: $PASS passed, $FAIL failed ==="

# Cleanup
curl -s -X DELETE "$HOST/$IDX" > /dev/null
rm -f /tmp/rw_read_log.txt /tmp/rw_write_log.txt /tmp/rw_read_ok.txt /tmp/rw_read_fail.txt /tmp/rw_write_ok.txt /tmp/rw_write_fail.txt /tmp/rw_upgrade_running
