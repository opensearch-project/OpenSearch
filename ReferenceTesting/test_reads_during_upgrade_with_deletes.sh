#!/bin/bash
# Test reads DURING upgrade on an index with prior deletes
set -e
HOST="localhost:9200"
INDEX="ecom_reads_del_test"

echo "=== Setup: Create index, ingest 100k docs, delete 5000 ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

# Generate and ingest
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/reads_del.ndjson', 'w') as f:
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
# Bulk ingest in batches
TOTAL=$(wc -l < /tmp/reads_del.ndjson)
OFF=1; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/reads_del.ndjson > /tmp/_batch_rd.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_batch_rd.ndjson > /dev/null
  OFF=$((END + 1))
done
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

# Delete 5000 docs
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(100000), 5000)
with open('/tmp/reads_del_bulk.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/reads_del_bulk.ndjson > /dev/null
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

echo -n "Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Starting background read loop ==="
# Background loop: send search requests every 200ms during upgrade
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"}}}}'
LOGFILE="/tmp/reads_during_upgrade_del.log"
> "$LOGFILE"

(
  while true; do
    RESULT=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY" 2>&1)
    STATUS=$(echo "$RESULT" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    shards=d.get('_shards',{})
    failed=shards.get('failed',0)
    took=d.get('took','?')
    buckets=len(d.get('aggregations',{}).get('by_gender',{}).get('buckets',[]))
    print(f'OK took={took}ms buckets={buckets} failed_shards={failed}')
except:
    print(f'ERROR: {sys.stdin.read()[:100]}')
" 2>&1)
    echo "$(date +%H:%M:%S.%N) $STATUS" >> "$LOGFILE"
    sleep 0.2
  done
) &
READER_PID=$!

echo "Reader PID: $READER_PID"
sleep 1  # let a few reads happen before upgrade

echo ""
echo "=== Triggering upgrade ==="
UPGRADE_RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "test_st",
    "ordered_dimensions": [{"name": "customer_gender"}, {"name": "currency"}],
    "metrics": [{"name": "taxful_total_price", "stats": ["sum", "value_count"]}]
  }
}')
echo "Upgrade result: $UPGRADE_RESULT"

sleep 1  # let a few more reads happen after upgrade
kill $READER_PID 2>/dev/null || true
wait $READER_PID 2>/dev/null || true

echo ""
echo "=== Read results during upgrade ==="
TOTAL_READS=$(wc -l < "$LOGFILE")
OK_READS=$(grep -c "^.*OK" "$LOGFILE" || echo 0)
ERROR_READS=$(grep -c "^.*ERROR" "$LOGFILE" || echo 0)
ZERO_BUCKET_READS=$(grep -c "buckets=0" "$LOGFILE" || echo 0)
FAILED_SHARD_READS=$(grep -v "failed_shards=0" "$LOGFILE" | grep -c "failed_shards" || echo 0)

echo "  Total reads: $TOTAL_READS"
echo "  Successful (OK): $OK_READS"
echo "  Errors: $ERROR_READS"
echo "  Zero-bucket responses: $ZERO_BUCKET_READS"
echo "  Failed-shard responses: $FAILED_SHARD_READS"

echo ""
echo "=== Sample log entries ==="
head -3 "$LOGFILE"
echo "..."
tail -3 "$LOGFILE"

echo ""
echo "=== Post-upgrade verification ==="
echo -n "Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Done ==="
