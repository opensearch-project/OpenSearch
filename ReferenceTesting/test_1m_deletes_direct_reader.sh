#!/bin/bash
# Test star tree upgrade with 1M docs INCLUDING DELETES
# Validates the StarTreeDirectReader approach for soft-delete segments
set -e
HOST="localhost:9200"

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
        f.write(json.dumps({'index': {'_index': 'ecom_1m_del', '_id': str(i)}}) + '\n')
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
print('Generated 1000000 docs with _id')
"

echo ""
echo "=== Step 2: Create index ==="
curl -s -X DELETE "$HOST/ecom_1m_del" > /dev/null 2>&1 || true
curl -s -X PUT "$HOST/ecom_1m_del" -H 'Content-Type: application/json' \
  -d @ReferenceTesting/ecommerce-field_mappings.json > /dev/null
echo "Created ecom_1m_del"

echo ""
echo "=== Step 3: Ingest 1M docs ==="
TOTAL=$(wc -l < /tmp/test_1m_del.ndjson)
OFF=1; COUNT=0; BATCH=20000
while [ $OFF -le $TOTAL ]; do
  END=$((OFF + BATCH - 1))
  sed -n "${OFF},${END}p" /tmp/test_1m_del.ndjson > /tmp/_batch_1m_del.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_1m_del.ndjson | python3 -c "import sys,json;print(len(json.load(sys.stdin)['items']))")
  COUNT=$((COUNT + N))
  printf "  %d docs\r" $COUNT
  OFF=$((END + 1))
done
echo "  Total ingested: $COUNT docs"

echo ""
echo "=== Step 4: Refresh + flush ==="
curl -s -X POST "$HOST/ecom_1m_del/_refresh" > /dev/null
curl -s -X POST "$HOST/ecom_1m_del/_flush?force=true" > /dev/null

echo ""
echo "=== Step 5: DELETE 50000 docs (creates delGen != -1) ==="
python3 -c "
import json, random
random.seed(99)
ids = random.sample(range(1000000), 50000)
with open('/tmp/test_1m_del_bulk.ndjson', 'w') as f:
    for i in ids:
        f.write(json.dumps({'delete': {'_index': 'ecom_1m_del', '_id': str(i)}}) + '\n')
print('Generated 50000 delete operations')
"

# Bulk delete in batches
DEL_TOTAL=$(wc -l < /tmp/test_1m_del_bulk.ndjson)
DEL_OFF=1; DEL_COUNT=0; DEL_BATCH=10000
while [ $DEL_OFF -le $DEL_TOTAL ]; do
  DEL_END=$((DEL_OFF + DEL_BATCH - 1))
  sed -n "${DEL_OFF},${DEL_END}p" /tmp/test_1m_del_bulk.ndjson > /tmp/_batch_1m_del_del.ndjson
  N=$(curl -s -X POST "$HOST/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @/tmp/_batch_1m_del_del.ndjson | python3 -c "import sys,json;d=json.load(sys.stdin);print(sum(1 for i in d['items'] if i['delete']['result']=='deleted'))")
  DEL_COUNT=$((DEL_COUNT + N))
  printf "  Deleted %d docs\r" $DEL_COUNT
  DEL_OFF=$((DEL_END + 1))
done
echo "  Total deleted: $DEL_COUNT docs"

echo ""
echo "=== Step 6: Flush (commits deletes) ==="
curl -s -X POST "$HOST/ecom_1m_del/_flush?force=true" > /dev/null

echo ""
echo "=== Step 7: Doc count before upgrade ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_1m_del/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 8: Segment info (look for deleted_docs > 0) ==="
curl -s "$HOST/ecom_1m_del/_segments" | python3 -c "
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
echo "=== Step 9: Aggregation BEFORE upgrade (baseline) ==="
QUERY='{"size":0,"aggs":{"by_gender":{"terms":{"field":"customer_gender"},"aggs":{"revenue":{"sum":{"field":"taxful_total_price"}},"avg_qty":{"avg":{"field":"total_quantity"}}}}}}'
BEFORE=$(curl -s -X POST "$HOST/ecom_1m_del/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$BEFORE" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}, avg_qty={b[\"avg_qty\"][\"value\"]:.4f}')
"

echo ""
echo "=== Step 10: UPGRADE (segments have deletes — critical test) ==="
START=$(python3 -c "import time; print(time.time())")
RESULT=$(curl -s -X POST "$HOST/ecom_1m_del/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
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
echo "=== Step 11: Doc count after upgrade ==="
echo -n "  Doc count: "
curl -s "$HOST/ecom_1m_del/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"

echo ""
echo "=== Step 12: Aggregation AFTER upgrade ==="
AFTER=$(curl -s -X POST "$HOST/ecom_1m_del/_search" -H 'Content-Type: application/json' -d "$QUERY")
echo "$AFTER" | python3 -c "
import sys, json; d = json.load(sys.stdin)
for b in d['aggregations']['by_gender']['buckets']:
    print(f'  {b[\"key\"]}: count={b[\"doc_count\"]}, revenue={b[\"revenue\"][\"value\"]:.2f}, avg_qty={b[\"avg_qty\"][\"value\"]:.4f}')
"

echo ""
echo "=== Step 13: COMPARE before vs after ==="
python3 -c "
import json, sys
before = json.loads('''$BEFORE''')
after = json.loads('''$AFTER''')
bb = {b['key']: b for b in before['aggregations']['by_gender']['buckets']}
ab = {b['key']: b for b in after['aggregations']['by_gender']['buckets']}
all_pass = True
for key in bb:
    b = bb[key]; a = ab[key]
    checks = [
        ('doc_count', b['doc_count'], a['doc_count']),
        ('revenue', b['revenue']['value'], a['revenue']['value']),
        ('avg_qty', b['avg_qty']['value'], a['avg_qty']['value']),
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

echo ""
echo "=== Done ==="
