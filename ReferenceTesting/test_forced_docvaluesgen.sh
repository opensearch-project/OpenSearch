#!/bin/bash
# Forces segments with docValuesGen != -1 by:
# 1. Ingesting in batches with flush between each → creates committed segments
# 2. Deleting docs from those committed segments → creates doc values updates (docValuesGen != -1)
set -e
HOST="localhost:9200"
INDEX="ecom_forced_dvgen"

echo "=== Setup: Force docValuesGen != -1 topology ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null

# Generate 100k docs
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/forced_dvgen.ndjson', 'w') as f:
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

# Ingest in 5 batches of 20k with flush after each → 5 committed segments
echo "  Ingesting in 5 batches with flush between each..."
for BATCH_NUM in 1 2 3 4 5; do
  START_LINE=$(( (BATCH_NUM - 1) * 40000 + 1 ))
  END_LINE=$(( BATCH_NUM * 40000 ))
  sed -n "${START_LINE},${END_LINE}p" /tmp/forced_dvgen.ndjson > /tmp/_fdv_batch.ndjson
  curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/_fdv_batch.ndjson > /dev/null
  curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null
  echo "    Batch $BATCH_NUM: flushed"
done

echo ""
echo "=== Segments BEFORE delete ==="
curl -s "$HOST/$INDEX/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: docs={seg[\"num_docs\"]}, del={seg.get(\"deleted_docs\",0)}')
"

echo ""
echo "=== Deleting 5000 docs (from already-committed segments → docValuesGen != -1) ==="
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(100000), 5000)
with open('/tmp/forced_dvgen_del.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
"
curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/forced_dvgen_del.ndjson > /dev/null
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

echo ""
echo "=== Segments AFTER delete (should show deleted_docs > 0 on existing segments) ==="
curl -s "$HOST/$INDEX/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                print(f'  {seg_name}: docs={seg[\"num_docs\"]}, del={seg.get(\"deleted_docs\",0)}')
"

echo ""
echo "=== Doc count ==="
curl -s -X POST "$HOST/$INDEX/_refresh" > /dev/null
echo -n "  "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Baseline aggregation ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BASELINE=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BASELINE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Upgrade (TIMED) ==="
START=$(python3 -c "import time; print(time.time())")
RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "test_st",
    "ordered_dimensions": [{"name": "customer_gender"}, {"name": "currency"}],
    "metrics": [{"name": "taxful_total_price", "stats": ["sum", "value_count"]}]
  }
}')
END=$(python3 -c "import time; print(time.time())")
echo "  Result: $RESULT"
echo "  Time: $(python3 -c "print(f'{$END - $START:.3f}')")s"

echo ""
echo "=== Post-upgrade doc count ==="
echo -n "  "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Post-upgrade aggregation ==="
POST=$(curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$POST" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
buckets = d['aggregations']['by_gender']['buckets']
print(f'  buckets: {len(buckets)}')
for b in buckets:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Correctness check ==="
python3 -c "
import json
before = json.loads('$BASELINE')
after = json.loads('$(echo "$POST" | tr "'" " ")')
bb = {b['key']: b for b in before['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
if not ab:
    print('  FAIL: post-upgrade returned empty buckets!')
else:
    all_match = True
    for key in bb:
        if key not in ab:
            print(f'  FAIL: missing key {key}')
            all_match = False
        elif bb[key]['doc_count'] != ab[key]['doc_count']:
            print(f'  FAIL: {key} count mismatch: {bb[key][\"doc_count\"]} vs {ab[key][\"doc_count\"]}')
            all_match = False
    if all_match:
        print('  PASS: all values match')
"

echo ""
echo "=== Done ==="
