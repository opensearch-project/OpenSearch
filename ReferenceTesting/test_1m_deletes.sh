#!/bin/bash
# Test 1M docs with deletes — measures parallel star tree build time
set -e
HOST="localhost:9200"
INDEX="ecom_1m_del"

echo "=== Step 1: Generate 1M docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_1m_del.ndjson', 'w') as f:
    for i in range(1000000):
        f.write(json.dumps({'index': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
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
            'customer_id': 'cust_' + str(random.randint(1,1000)),
            'order_id': 'ord_' + str(i),
            'type': 'order',
            'user': 'user_' + str(random.randint(1,500))
        }) + '\n')
print('Generated 1,000,000 docs')
"

echo ""
echo "=== Step 2: Create index ==="
curl -s -X DELETE "$HOST/$INDEX" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/$INDEX" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created $INDEX"

echo ""
echo "=== Step 3: Ingest 1M docs ==="
TOTAL=$(wc -l < /tmp/test_1m_del.ndjson)
OFF=1; COUNT=0; BATCH=50000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_1m_del.ndjson > /tmp/_batch_1m.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_1m.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total: $COUNT docs"

echo ""
echo "=== Step 4: Flush ==="
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

echo ""
echo "=== Step 5: Delete 50,000 docs ==="
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(1000000), 50000)
with open('/tmp/test_1m_del_bulk.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': '$INDEX', '_id': str(i)}}) + '\n')
print('Generated 50,000 delete operations')
"
# Bulk delete in batches
OFF=1; DEL_COUNT=0; DEL_BATCH=10000
DEL_TOTAL=$(wc -l < /tmp/test_1m_del_bulk.ndjson)
while [ $OFF -le $DEL_TOTAL ]; do
  END=$((OFF + DEL_BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_1m_del_bulk.ndjson > /tmp/_del_batch.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_del_batch.ndjson | python3 -c "import sys,json;d=json.load(sys.stdin);print(sum(1 for i in d['items'] if i['delete']['result']=='deleted'))")
  DEL_COUNT=$((DEL_COUNT + N))
  printf "  %d deleted\r" $DEL_COUNT
  OFF=$((END + 1))
done
echo "  Total deleted: $DEL_COUNT"

echo ""
echo "=== Step 6: Flush ==="
curl -s -X POST "$HOST/$INDEX/_flush?force=true" > /dev/null

echo ""
echo "=== Step 7: Verify ==="
echo -n "  Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"
echo "  Segments:"
curl -s "$HOST/$INDEX/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            total_docs = 0
            total_del = 0
            seg_count = 0
            for seg_name, seg in shard['segments'].items():
                total_docs += seg['num_docs']
                total_del += seg.get('deleted_docs', 0)
                seg_count += 1
            print(f'    {seg_count} segments, {total_docs} live docs, {total_del} deleted docs')
"

echo ""
echo "=== Step 8: Upgrade (TIMED) ==="
START=$(python3 -c "import time; print(time.time())")
RESULT=$(curl -s -X POST "$HOST/$INDEX/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "  Response: $RESULT"
echo "  >>> Upgrade time: ${UPGRADE_TIME}s"

echo ""
echo "=== Step 9: Post-upgrade verify ==="
echo -n "  Doc count: "
curl -s "$HOST/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 10: Aggregation ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
curl -s -X POST "$HOST/$INDEX/_search" -H 'Content-Type: application/json' -d "$QUERY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  terminated_early: {d.get(\"terminated_early\", False)}')
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Done ==="
