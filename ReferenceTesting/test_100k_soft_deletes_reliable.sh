#!/bin/bash
# Reliable test for star tree upgrade with soft deletes on original segments.
# Uses _delete_by_query which applies soft-delete DV updates directly to segments.
set -e
HOST="localhost:9200"

echo "=== Step 1: Create index ==="
curl -s -X DELETE "$HOST/ecom_sd_test" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/ecom_sd_test" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created ecom_sd_test"

echo ""
echo "=== Step 2: Generate and ingest 100k docs ==="
python3 -c "
import json, random, datetime
random.seed(42)
genders = ['MALE', 'FEMALE']
currencies = ['EUR', 'USD', 'GBP']
days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
base = datetime.datetime(2024, 1, 1)
with open('/tmp/test_sd.ndjson', 'w') as f:
    for i in range(100000):
        f.write(json.dumps({'index': {'_index': 'ecom_sd_test', '_id': str(i)}}) + '\n')
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
            'day_of_week_i': random.randint(0, 6)
        }) + '\n')
"
TOTAL=$(wc -l < /tmp/test_sd.ndjson)
OFF=1; COUNT=0; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_sd.ndjson > /tmp/_batch_sd.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_sd.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total ingested: $COUNT docs"

echo ""
echo "=== Step 3: Flush to commit segments ==="
curl -s -X POST "$HOST/ecom_sd_test/_flush?force=true" > /dev/null

echo ""
echo "=== Step 4: Delete 5000 docs via _delete_by_query ==="
# This applies soft-delete DV updates directly to the segments containing these docs
DEL_RESULT=$(curl -s -X POST "$HOST/ecom_sd_test/_delete_by_query" -H 'Content-Type: application/json' -d '{
  "query": { "range": { "total_quantity": { "lte": 2 } } }
}')
echo "  Deleted: $(echo $DEL_RESULT | python3 -c "import sys,json;print(json.load(sys.stdin).get('deleted', 0))")"

echo ""
echo "=== Step 5: Refresh to apply DV updates ==="
curl -s -X POST "$HOST/ecom_sd_test/_refresh" > /dev/null

echo ""
echo "=== Step 6: Flush to persist DV updates ==="
curl -s -X POST "$HOST/ecom_sd_test/_flush?force=true" > /dev/null

echo ""
echo "=== Step 7: Verify segments have soft deletes ==="
curl -s "$HOST/ecom_sd_test/_segments" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for idx_name, idx_data in data['indices'].items():
    for shard_id, shards in idx_data['shards'].items():
        for shard in shards:
            for seg_name, seg in shard['segments'].items():
                del_docs = seg.get('deleted_docs', 0)
                marker = ' *** HAS DELETES ***' if del_docs > 0 else ''
                print(f'  {seg_name}: {seg[\"num_docs\"]} docs, {del_docs} deleted{marker}')
"

echo ""
echo "=== Step 8: Doc count ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_sd_test/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 9: Aggregation BEFORE upgrade (baseline) ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}}}}}}'
BEFORE=$(curl -s -X POST "$HOST/ecom_sd_test/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Step 10: UPGRADE ==="
START=$(python3 -c "import time; print(time.time())")
RESULT=$(curl -s -X POST "$HOST/ecom_sd_test/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "  Response: $RESULT"
echo "  Upgrade time: $(python3 -c "print(f'{$END - $START:.3f}')")s"
echo "$RESULT" | python3 -c "
import sys, json; d = json.load(sys.stdin)
if d['_shards']['failed'] > 0: print('  *** UPGRADE FAILED ***'); sys.exit(1)
else: print('  *** UPGRADE SUCCEEDED ***')
"

echo ""
echo "=== Step 11: Aggregation AFTER upgrade ==="
AFTER=$(curl -s -X POST "$HOST/ecom_sd_test/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}')
"

echo ""
echo "=== Step 12: COMPARE ==="
python3 -c "
import json, sys
before = json.loads('''$BEFORE''')
after = json.loads('''$AFTER''')
bb = {b['key']: b for b in before['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in bb:
    if key not in ab:
        print(f'  {key}: MISSING in after'); all_pass = False; continue
    b = bb[key]; a = ab[key]
    checks = [
        ('doc_count', b['doc_count'], a['doc_count']),
        ('revenue', b['revenue']['value'], a['revenue']['value']),
    ]
    for name, bv, av in checks:
        match = abs(bv - av) < 0.01 if isinstance(bv, float) else bv == av
        status = 'PASS' if match else 'FAIL'
        if not match: all_pass = False
        print(f'  {key}.{name}: before={bv}, after={av} → {status}')
print()
if all_pass: print('  *** ALL CHECKS PASSED ***')
else: print('  *** SOME CHECKS FAILED ***'); sys.exit(1)
"
