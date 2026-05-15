#!/bin/bash
# Thorough test: verify reads return CORRECT values during upgrade with deletes
# Compares exact doc_count and sum values against pre-upgrade baseline
set -e
HOST="localhost:9200"
INDEX="ecom_correctness_test"

echo "=== Setup: Create index, ingest 100k docs with explicit IDs, delete 5000 ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/correctness.ndjson', 'w') as f:
    for i in range(100000):
        f.write(json.dumps({'index': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
        f.write(json.dumps({
            'customer_gender': random.choice(genders),
            'currency': random.choice(currencies),
            'day_of_week': 'Monday',
            'order_date': '2024-01-01T00:00:00Z',
            'taxful_total_price': round(random.uniform(5, 500), 2),
            'taxless_total_price': round(random.uniform(4, 450), 2),
            'total_quantity': random.randint(1, 20),
            'total_unique_products': random.randint(1, 10),
            'day_of_week_i': random.randint(0, 6),
            'customer_id': 'cust_1',
            'order_id': 'ord_' + str(i),
            'type': 'order',
            'user': 'user_1'
        }) + '\n')
"
TOTAL=$(wc -l < /tmp/correctness.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/correctness.ndjson > /tmp/_batch_c.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_batch_c.ndjson > /dev/null
  OFF=$((END + 1))
done
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

# Delete 5000 docs
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(100000), 5000)
with open('/tmp/correctness_del.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/correctness_del.ndjson > /dev/null
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null
curl -s -X POST "$HOST/$INDEX/_refresh" > /dev/null

echo ""
echo "=== Baseline: capture exact values BEFORE upgrade ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BASELINE=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
BASELINE_COUNT=$(curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")

python3 -c "
import json
d = json.loads('$BASELINE')
print(f'  Doc count: $BASELINE_COUNT')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: doc_count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
" 2>/dev/null || echo "  (parse error — checking raw)"

# Save baseline values for comparison
EXPECTED_MALE_COUNT=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);print([b for b in d['aggregations']['by_gender']['buckets'] if b['key']=='MALE'][0]['doc_count'])")
EXPECTED_FEMALE_COUNT=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);print([b for b in d['aggregations']['by_gender']['buckets'] if b['key']=='FEMALE'][0]['doc_count'])")
EXPECTED_MALE_REV=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);print([b for b in d['aggregations']['by_gender']['buckets'] if b['key']=='MALE'][0]['revenue']['value'])")
EXPECTED_FEMALE_REV=$(echo "$BASELINE" | python3 -c "import sys,json;d=json.load(sys.stdin);print([b for b in d['aggregations']['by_gender']['buckets'] if b['key']=='FEMALE'][0]['revenue']['value'])")

echo ""
echo "  Expected MALE: count=$EXPECTED_MALE_COUNT, revenue=$EXPECTED_MALE_REV"
echo "  Expected FEMALE: count=$EXPECTED_FEMALE_COUNT, revenue=$EXPECTED_FEMALE_REV"
echo "  Expected total doc count: $BASELINE_COUNT"

echo ""
echo "=== Starting background reads with value comparison ==="
LOGFILE="/tmp/correctness_reads.log"
> "$LOGFILE"

(
  while true; do
    RESULT=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY" 2>&1)
    python3 -c "
import sys, json
try:
    d = json.loads('''$RESULT''')
    buckets = {b['key']: b for b in d['aggregations']['by_gender']['buckets']}
    male_count = buckets.get('MALE', {}).get('doc_count', 0)
    female_count = buckets.get('FEMALE', {}).get('doc_count', 0)
    male_rev = buckets.get('MALE', {}).get('revenue', {}).get('value', 0)
    female_rev = buckets.get('FEMALE', {}).get('revenue', {}).get('value', 0)
    total = male_count + female_count
    
    match = (male_count == $EXPECTED_MALE_COUNT and 
             female_count == $EXPECTED_FEMALE_COUNT and
             abs(male_rev - $EXPECTED_MALE_REV) < 0.01 and
             abs(female_rev - $EXPECTED_FEMALE_REV) < 0.01)
    
    status = 'MATCH' if match else 'MISMATCH'
    print(f'{status} total={total} male={male_count} female={female_count}')
except Exception as e:
    print(f'ERROR {e}')
" >> "$LOGFILE" 2>&1
    sleep 0.15
  done
) &
READER_PID=$!

sleep 1

echo ""
echo "=== Triggering upgrade ==="
UPGRADE_RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "test_st",
    "ordered_dimensions": [{"name": "customer_gender"}, {"name": "currency"}],
    "metrics": [{"name": "taxful_total_price", "stats": ["sum", "value_count"]}]
  }
}')
echo "  Result: $UPGRADE_RESULT"

sleep 2
kill $READER_PID 2>/dev/null || true
wait $READER_PID 2>/dev/null || true

echo ""
echo "=== Results ==="
TOTAL_READS=$(wc -l < "$LOGFILE")
MATCH_READS=$(grep -c "^MATCH" "$LOGFILE" || echo 0)
MISMATCH_READS=$(grep -c "^MISMATCH" "$LOGFILE" || echo 0)
ERROR_READS=$(grep -c "^ERROR" "$LOGFILE" || echo 0)

echo "  Total reads: $TOTAL_READS"
echo "  Correct (MATCH): $MATCH_READS"
echo "  Wrong (MISMATCH): $MISMATCH_READS"
echo "  Errors: $ERROR_READS"

if [ "$MISMATCH_READS" != "0" ]; then
  echo ""
  echo "  !!! MISMATCHES FOUND !!!"
  grep "^MISMATCH" "$LOGFILE" | head -5
fi

if [ "$ERROR_READS" != "0" ]; then
  echo ""
  echo "  !!! ERRORS FOUND !!!"
  grep "^ERROR" "$LOGFILE" | head -5
fi

echo ""
echo "=== Post-upgrade check ==="
echo -n "  Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"
POST=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$POST" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: doc_count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Done ==="
